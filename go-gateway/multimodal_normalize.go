package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"
)

// canonicalImageStamp is an unexported in-memory sentinel type stamped onto validated inline
// image maps. Because encoding/json cannot unmarshal into an unexported struct type, untrusted
// HTTP JSON payloads can never forge this marker to bypass MIME sniffing or byte budgets.
type canonicalImageStamp struct{}

const canonicalImageSentinelKey = "_canonical"

func isCanonicalizedImageBlock(b map[string]any) bool {
	if b == nil {
		return false
	}
	_, ok := b[canonicalImageSentinelKey].(canonicalImageStamp)
	return ok
}

type validatedInlineImageMeta struct {
	mime         string
	decodedBytes int
	digest       string
}

var (
	validatedInlineCacheMu sync.RWMutex
	validatedInlineCache   = make(map[[32]byte]validatedInlineImageMeta)
)

const maxValidatedInlineCacheEntries = 512

func hashStringSHA256(s string) [32]byte {
	h := sha256.New()
	_, _ = io.WriteString(h, s)
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func lookupValidatedHistoricalImage(rawB64, fieldPath string) (map[string]any, bool, error) {
	clean := cleanBase64Whitespace(rawB64)
	if clean == "" {
		return nil, false, nil
	}
	if MaxImageBytes > 0 {
		maxCleanLen := ((MaxImageBytes + 2) / 3) * 4
		if len(clean) > maxCleanLen+8 {
			return nil, true, newMMError(http.StatusRequestEntityTooLarge, fieldPath, fmt.Sprintf("image exceeds maximum decoded size of %d bytes", MaxImageBytes))
		}
	}
	key := hashStringSHA256(clean)
	validatedInlineCacheMu.RLock()
	meta, ok := validatedInlineCache[key]
	validatedInlineCacheMu.RUnlock()
	if !ok {
		return nil, false, nil
	}
	if MaxImageBytes > 0 && meta.decodedBytes > MaxImageBytes {
		return nil, true, newMMError(http.StatusRequestEntityTooLarge, fieldPath, fmt.Sprintf("image size (%d bytes) exceeds %d byte limit", meta.decodedBytes, MaxImageBytes))
	}
	return map[string]any{
		"type":                    "image",
		"mimeType":                meta.mime,
		"data":                    clean,
		"decodedBytes":            meta.decodedBytes,
		"digest":                  meta.digest,
		canonicalImageSentinelKey: canonicalImageStamp{},
	}, true, nil
}

func recordValidatedInlineImage(cleanB64 string, meta validatedInlineImageMeta) {
	if cleanB64 == "" || meta.mime == "" || meta.decodedBytes <= 0 || meta.digest == "" {
		return
	}
	key := hashStringSHA256(cleanB64)
	validatedInlineCacheMu.Lock()
	if len(validatedInlineCache) >= maxValidatedInlineCacheEntries {
		for k := range validatedInlineCache {
			delete(validatedInlineCache, k)
			if len(validatedInlineCache) < maxValidatedInlineCacheEntries*3/4 {
				break
			}
		}
	}
	validatedInlineCache[key] = meta
	validatedInlineCacheMu.Unlock()
}

// MultimodalValidationError represents a client-facing 400/413 multimodal input error
// with the exact field path that failed Stage 1-3 validation.
type MultimodalValidationError struct {
	StatusCode int
	FieldPath  string
	Message    string
}

func (e *MultimodalValidationError) Error() string {
	if e == nil {
		return ""
	}
	if e.FieldPath != "" {
		return fmt.Sprintf("%s: %s", e.FieldPath, e.Message)
	}
	return e.Message
}

func newMMError(status int, fieldPath, msg string) error {
	if status <= 0 {
		status = http.StatusBadRequest
	}
	return &MultimodalValidationError{
		StatusCode: status,
		FieldPath:  fieldPath,
		Message:    msg,
	}
}

// sniffAllowedImageMime strictly inspects the first 16 decoded bytes and ignores any
// client-declared MIME type or HTTP Content-Type header.
func sniffAllowedImageMime(raw []byte) (string, bool) {
	if len(raw) == 0 {
		return "", false
	}
	head := raw
	if len(head) > 16 {
		head = raw[:16]
	}
	switch {
	case len(head) >= 8 && bytes.Equal(head[:8], []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'}):
		return "image/png", true
	case len(head) >= 3 && head[0] == 0xff && head[1] == 0xd8 && head[2] == 0xff:
		return "image/jpeg", true
	case len(head) >= 6 && (string(head[:6]) == "GIF87a" || string(head[:6]) == "GIF89a"):
		return "image/gif", true
	case len(head) >= 12 && string(head[:4]) == "RIFF" && string(head[8:12]) == "WEBP":
		return "image/webp", true
	default:
		return "", false
	}
}

func cleanBase64Whitespace(s string) string {
	s = strings.TrimSpace(s)
	if !strings.ContainsAny(s, " \t\r\n") {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case ' ', '\t', '\r', '\n':
			continue
		default:
			b.WriteByte(s[i])
		}
	}
	return b.String()
}

func decodeBoundedBase64(rawB64 string, maxBytes, overLimitStatus int, fieldPath, kind string) ([]byte, string, error) {
	clean := cleanBase64Whitespace(rawB64)
	if clean == "" {
		return nil, "", newMMError(http.StatusBadRequest, fieldPath, kind+" base64 payload is empty")
	}
	if maxBytes > 0 {
		// Maximum clean base64 length for maxBytes with standard padding plus tolerance.
		maxCleanLen := ((maxBytes + 2) / 3) * 4
		if len(clean) > maxCleanLen+8 {
			return nil, "", newMMError(overLimitStatus, fieldPath, fmt.Sprintf("%s exceeds maximum decoded size of %d bytes", kind, maxBytes))
		}
	}
	decoded, err := base64.StdEncoding.DecodeString(clean)
	if err != nil && !strings.ContainsRune(clean, '=') {
		decoded, err = base64.RawStdEncoding.DecodeString(clean)
	}
	if err != nil {
		return nil, "", newMMError(http.StatusBadRequest, fieldPath, fmt.Sprintf("invalid base64 %s payload", kind))
	}
	if len(decoded) == 0 {
		return nil, "", newMMError(http.StatusBadRequest, fieldPath, kind+" payload decoded to 0 bytes")
	}
	if maxBytes > 0 && len(decoded) > maxBytes {
		return nil, "", newMMError(overLimitStatus, fieldPath, fmt.Sprintf("%s decoded size (%d bytes) exceeds %d byte limit", kind, len(decoded), maxBytes))
	}
	cleanOut := base64.StdEncoding.EncodeToString(decoded)
	if cleanOut == clean {
		// Reuse caller's already-canonical base64 string without retaining the newly allocated copy.
		cleanOut = clean
	}
	return decoded, cleanOut, nil
}

func inlineImageDigest(mime string, raw []byte) string {
	h := sha256.New()
	_, _ = io.WriteString(h, mime)
	_, _ = h.Write([]byte{':'})
	_, _ = io.WriteString(h, strconv.Itoa(len(raw)))
	_, _ = h.Write([]byte{':'})
	_, _ = h.Write(raw)
	return hex.EncodeToString(h.Sum(nil))[:16]
}

func buildCanonicalInlineImage(raw []byte, cleanB64, fieldPath string) (map[string]any, error) {
	if MaxImageBytes > 0 && len(raw) > MaxImageBytes {
		return nil, newMMError(http.StatusRequestEntityTooLarge, fieldPath, fmt.Sprintf("image size (%d bytes) exceeds %d byte limit", len(raw), MaxImageBytes))
	}
	sniffed, ok := sniffAllowedImageMime(raw)
	if !ok {
		return nil, newMMError(http.StatusBadRequest, fieldPath, "unsupported or malformed image bytes; only PNG, JPEG, WEBP, and GIF magic headers are supported")
	}
	if cleanB64 == "" {
		cleanB64 = base64.StdEncoding.EncodeToString(raw)
	}
	dig := inlineImageDigest(sniffed, raw)
	recordValidatedInlineImage(cleanB64, validatedInlineImageMeta{
		mime:         sniffed,
		decodedBytes: len(raw),
		digest:       dig,
	})
	return map[string]any{
		"type":                    "image",
		"mimeType":                sniffed,
		"data":                    cleanB64,
		"decodedBytes":            len(raw),
		"digest":                  dig,
		canonicalImageSentinelKey: canonicalImageStamp{},
	}, nil
}

func decodeAndCanonicalizeImageB64(rawB64, fieldPath string, isHistorical bool) (map[string]any, error) {
	if isHistorical {
		if cachedBlk, handled, hErr := lookupValidatedHistoricalImage(rawB64, fieldPath); handled {
			return cachedBlk, hErr
		}
	}
	rawBytes, cleanB64, err := decodeBoundedBase64(rawB64, MaxImageBytes, http.StatusRequestEntityTooLarge, fieldPath, "image")
	if err != nil {
		return nil, err
	}
	return buildCanonicalInlineImage(rawBytes, cleanB64, fieldPath)
}

func sanitizeACPImageBlock(img map[string]any) map[string]any {
	mimeStr, _ := img["mimeType"].(string)
	dataStr, _ := img["data"].(string)
	if mimeStr == "" {
		mimeStr = "image/png"
	}
	return map[string]any{
		"type":     "image",
		"mimeType": mimeStr,
		"data":     dataStr,
	}
}

func normalizeMimeBase(mime string) string {
	m := strings.ToLower(strings.TrimSpace(mime))
	if idx := strings.IndexByte(m, ';'); idx >= 0 {
		m = strings.TrimSpace(m[:idx])
	}
	return m
}

func isSupportedTextMIME(mime string) bool {
	m := normalizeMimeBase(mime)
	if m == "" {
		return false
	}
	if strings.HasPrefix(m, "text/") {
		return true
	}
	switch m {
	case "application/json", "application/xml", "application/yaml", "application/x-yaml", "application/csv", "application/x-ndjson":
		return true
	}
	if strings.HasPrefix(m, "application/") && (strings.HasSuffix(m, "+json") || strings.HasSuffix(m, "+xml")) {
		return true
	}
	return false
}

var allowedTextExtensions = map[string]bool{
	".txt":      true,
	".md":       true,
	".markdown": true,
	".json":     true,
	".jsonl":    true,
	".ndjson":   true,
	".csv":      true,
	".tsv":      true,
	".yaml":     true,
	".yml":      true,
	".xml":      true,
	".log":      true,
	".ini":      true,
	".toml":     true,
	".py":       true,
	".go":       true,
	".js":       true,
	".ts":       true,
	".tsx":      true,
	".jsx":      true,
	".html":     true,
	".css":      true,
	".sql":      true,
	".sh":       true,
}

func isAllowedTextExtension(filename string) bool {
	ext := strings.ToLower(path.Ext(strings.TrimSpace(filename)))
	return allowedTextExtensions[ext]
}

func unpackUTF8TextBytes(raw []byte, label, fieldPath string) (map[string]any, error) {
	maxBytes := MaxTextFileBytes
	if maxBytes <= 0 {
		maxBytes = 2 * 1024 * 1024
	}
	if len(raw) > maxBytes {
		return nil, newMMError(http.StatusBadRequest, fieldPath, fmt.Sprintf("text file (%d bytes) exceeds %d byte limit", len(raw), maxBytes))
	}
	if bytes.Contains(raw, []byte{0}) || !utf8.Valid(raw) {
		return nil, newMMError(http.StatusBadRequest, fieldPath, "file attachment is not valid UTF-8 text")
	}
	name := strings.TrimSpace(label)
	if name == "" {
		name = "attachment.txt"
	}
	return map[string]any{
		"type": "text",
		"text": fmt.Sprintf("[File: %s]\n%s", name, string(raw)),
	}, nil
}

func rejectUnsupportedMedia(mime, fieldPath string) error {
	m := normalizeMimeBase(mime)
	if m == "" {
		return newMMError(http.StatusBadRequest, fieldPath, "missing or unsupported media attachment")
	}
	return newMMError(http.StatusBadRequest, fieldPath, fmt.Sprintf("unsupported attachment media_type %q; only raster images (PNG/JPEG/WEBP/GIF) and UTF-8 text files (<=2MB) are supported", m))
}

func hasPrefixFold(s, pfx string) bool {
	return len(s) >= len(pfx) && strings.EqualFold(s[:len(pfx)], pfx)
}

func parseDataURI(rawURL string) (mime string, isBase64 bool, payload string, ok bool) {
	if !hasPrefixFold(rawURL, "data:") {
		return "", false, "", false
	}
	comma := strings.IndexByte(rawURL, ',')
	if comma < 5 {
		return "", false, "", false
	}
	header := rawURL[5:comma]
	payload = rawURL[comma+1:]
	parts := strings.Split(header, ";")
	mime = normalizeMimeBase(parts[0])
	if mime == "" {
		mime = "text/plain"
	}
	for _, p := range parts[1:] {
		if strings.EqualFold(strings.TrimSpace(p), "base64") {
			isBase64 = true
		}
	}
	return mime, isBase64, payload, true
}

func isPublicNetipAddr(addr netip.Addr) bool {
	if !addr.IsValid() || addr.Zone() != "" {
		return false
	}
	addr = addr.Unmap()
	if !addr.IsValid() || !addr.IsGlobalUnicast() {
		return false
	}
	if addr.IsLoopback() || addr.IsPrivate() || addr.IsLinkLocalUnicast() || addr.IsLinkLocalMulticast() || addr.IsMulticast() || addr.IsUnspecified() || addr.IsInterfaceLocalMulticast() {
		return false
	}
	for _, pfx := range blockedIPPrefixes {
		if pfx.Contains(addr) {
			return false
		}
	}
	return true
}

var blockedIPPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("169.254.0.0/16"),
	netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("::1/128"),
	netip.MustParsePrefix("::/96"),
	netip.MustParsePrefix("::ffff:0:0/96"),
	netip.MustParsePrefix("::ffff:0:0:0/96"),
	netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("64:ff9b:1::/48"),
	netip.MustParsePrefix("100::/64"),
	netip.MustParsePrefix("2001::/32"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("fc00::/7"),
	netip.MustParsePrefix("fe80::/10"),
	netip.MustParsePrefix("fec0::/10"),
}

func validateRemoteImageURLSyntax(rawURL, fieldPath string) (string, error) {
	uStr := strings.TrimSpace(rawURL)
	if uStr == "" {
		return "", newMMError(http.StatusBadRequest, fieldPath, "empty image URL")
	}
	if hasPrefixFold(uStr, "gs://") || hasPrefixFold(uStr, "files/") {
		return "", newMMError(http.StatusBadRequest, fieldPath, "hosted Gemini Files / gs:// references are not supported; pass inline base64 or a public http(s) image URL")
	}
	if !hasPrefixFold(uStr, "http://") && !hasPrefixFold(uStr, "https://") {
		return "", newMMError(http.StatusBadRequest, fieldPath, fmt.Sprintf("unsupported URL scheme in %q; only http(s) and data: URIs are allowed", truncateRunes(uStr, 64)))
	}
	if !FetchRemoteImages {
		return "", newMMError(http.StatusBadRequest, fieldPath, "remote image URLs are disabled (AGY_FETCH_IMAGES=false); send inline data:image/...;base64,... instead")
	}
	parsed, err := url.Parse(uStr)
	if err != nil || parsed.Host == "" {
		return "", newMMError(http.StatusBadRequest, fieldPath, "malformed remote image URL")
	}
	if strings.EqualFold(parsed.Hostname(), "generativelanguage.googleapis.com") && strings.Contains(strings.ToLower(parsed.Path), "/files/") {
		return "", newMMError(http.StatusBadRequest, fieldPath, "hosted Gemini Files / gs:// references are not supported; pass inline base64 or a public http(s) image URL")
	}
	if parsed.User != nil {
		return "", newMMError(http.StatusBadRequest, fieldPath, "credentials inside remote image URL are not permitted")
	}
	host := parsed.Hostname()
	if host == "" {
		return "", newMMError(http.StatusBadRequest, fieldPath, "missing hostname in remote image URL")
	}
	if strings.EqualFold(host, "localhost") || strings.HasSuffix(strings.ToLower(host), ".localhost") {
		return "", newMMError(http.StatusBadRequest, fieldPath, "loopback image URL rejected")
	}
	if port := parsed.Port(); port != "" && port != "80" && port != "443" {
		return "", newMMError(http.StatusBadRequest, fieldPath, fmt.Sprintf("port %q is not allowed for remote image URLs (only 80/443)", port))
	}
	if addr, errA := netip.ParseAddr(host); errA == nil && !isPublicNetipAddr(addr) {
		return "", newMMError(http.StatusBadRequest, fieldPath, fmt.Sprintf("non-public remote image IP %q rejected", host))
	}
	return uStr, nil
}

// extractSystemTextStrict converts system/developer prompt fields to plain text while rejecting
// any image, file, or binary payload inside system/developer prompts with HTTP 400.
func extractSystemTextStrict(sys any, fieldPath string) (string, error) {
	switch v := sys.(type) {
	case nil:
		return "", nil
	case string:
		return v, nil
	case map[string]any:
		if parts, ok := v["parts"].([]any); ok {
			return extractSystemTextStrict(parts, fieldPath+".parts")
		}
		if t, ok := v["text"].(string); ok {
			bType, _ := v["type"].(string)
			if bType == "" || bType == "text" || bType == "input_text" || bType == "output_text" {
				return t, nil
			}
		}
		return "", newMMError(http.StatusBadRequest, fieldPath, "system/developer prompts only support plain text")
	case []any:
		var parts []string
		for i, item := range v {
			itemPath := fmt.Sprintf("%s[%d]", fieldPath, i)
			switch b := item.(type) {
			case string:
				parts = append(parts, b)
			case map[string]any:
				if mapField(b, "inlineData", "inline_data") != nil || mapField(b, "fileData", "file_data") != nil {
					return "", newMMError(http.StatusBadRequest, itemPath, "system/developer prompts cannot contain binary or file attachments")
				}
				bType, _ := b["type"].(string)
				switch bType {
				case "", "text", "input_text", "output_text":
					t, okT := b["text"].(string)
					if !okT {
						return "", newMMError(http.StatusBadRequest, itemPath, "system/developer text block must contain a string text field")
					}
					parts = append(parts, t)
				default:
					return "", newMMError(http.StatusBadRequest, itemPath, fmt.Sprintf("system/developer prompt cannot contain %q block", bType))
				}
			default:
				return "", newMMError(http.StatusBadRequest, itemPath, "invalid item in system/developer prompt")
			}
		}
		return strings.Join(parts, "\n"), nil
	default:
		return "", newMMError(http.StatusBadRequest, fieldPath, "invalid type for system/developer prompt")
	}
}

func normalizeDataURIBlock(rawURI, filename, fieldPath string, allowTextFile bool) (map[string]any, error) {
	return normalizeDataURIBlockWithHistory(rawURI, filename, fieldPath, allowTextFile, false)
}

func normalizeDataURIBlockWithHistory(rawURI, filename, fieldPath string, allowTextFile, isHistorical bool) (map[string]any, error) {
	mime, isB64, payload, ok := parseDataURI(rawURI)
	if !ok {
		return nil, newMMError(http.StatusBadRequest, fieldPath, "malformed data: URI")
	}
	if mime == "image/svg+xml" {
		return nil, newMMError(http.StatusBadRequest, fieldPath, "image/svg+xml attachments are not supported")
	}
	if strings.HasPrefix(mime, "image/") {
		if !isB64 {
			return nil, newMMError(http.StatusBadRequest, fieldPath, "image data: URI must use ;base64 encoding")
		}
		return decodeAndCanonicalizeImageB64(payload, fieldPath, isHistorical)
	}
	if allowTextFile && isSupportedTextMIME(mime) {
		var raw []byte
		if isB64 {
			var err error
			raw, _, err = decodeBoundedBase64(payload, MaxTextFileBytes, http.StatusBadRequest, fieldPath, "text file")
			if err != nil {
				return nil, err
			}
		} else {
			unesc, err := url.PathUnescape(payload)
			if err != nil {
				return nil, newMMError(http.StatusBadRequest, fieldPath, "invalid percent-encoding in text data: URI")
			}
			raw = []byte(unesc)
		}
		label := strings.TrimSpace(filename)
		if label == "" {
			label = mime
		}
		return unpackUTF8TextBytes(raw, label, fieldPath)
	}
	return nil, rejectUnsupportedMedia(mime, fieldPath)
}

func normalizeURLOrDataImageBlock(rawURL, fieldPath string, allowTextDataURI bool) (map[string]any, error) {
	return normalizeURLOrDataImageBlockWithHistory(rawURL, fieldPath, allowTextDataURI, false)
}

func normalizeURLOrDataImageBlockWithHistory(rawURL, fieldPath string, allowTextDataURI, isHistorical bool) (map[string]any, error) {
	uStr := strings.TrimSpace(rawURL)
	if hasPrefixFold(uStr, "data:") {
		return normalizeDataURIBlockWithHistory(uStr, "", fieldPath, allowTextDataURI, isHistorical)
	}
	cleanURL, err := validateRemoteImageURLSyntax(uStr, fieldPath)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"type": "image_ref",
		"url":  cleanURL,
	}, nil
}

func normalizeAnthropicFileOrDocBlock(b map[string]any, fieldPath string) (map[string]any, error) {
	bType, _ := b["type"].(string)
	if bType == "document" {
		src, _ := b["source"].(map[string]any)
		if src == nil {
			return nil, newMMError(http.StatusBadRequest, fieldPath+".source", "document block missing source")
		}
		sType, _ := src["type"].(string)
		mime, _ := src["media_type"].(string)
		title, _ := b["title"].(string)
		switch sType {
		case "text":
			if mime != "" && !isSupportedTextMIME(mime) {
				return nil, rejectUnsupportedMedia(mime, fieldPath+".source.media_type")
			}
			txt, _ := src["data"].(string)
			return unpackUTF8TextBytes([]byte(txt), title, fieldPath+".source.data")
		case "base64":
			if !isSupportedTextMIME(mime) {
				return nil, rejectUnsupportedMedia(mime, fieldPath+".source.media_type")
			}
			dataStr, _ := src["data"].(string)
			raw, _, err := decodeBoundedBase64(dataStr, MaxTextFileBytes, http.StatusBadRequest, fieldPath+".source.data", "text file")
			if err != nil {
				return nil, err
			}
			return unpackUTF8TextBytes(raw, title, fieldPath+".source.data")
		case "file":
			return nil, newMMError(http.StatusBadRequest, fieldPath+".source", "hosted file_id references are not supported; inline the UTF-8 text or image")
		default:
			return nil, rejectUnsupportedMedia(mime, fieldPath+".source")
		}
	}

	// OpenAI Chat ("file") or Responses ("input_file")
	fileMap := b
	subPath := fieldPath
	if sub, ok := b["file"].(map[string]any); ok {
		fileMap = sub
		subPath = fieldPath + ".file"
	}
	if fid, _ := fileMap["file_id"].(string); strings.TrimSpace(fid) != "" {
		return nil, newMMError(http.StatusBadRequest, subPath+".file_id", "hosted file_id references are not supported")
	}
	if fid, _ := b["file_id"].(string); strings.TrimSpace(fid) != "" {
		return nil, newMMError(http.StatusBadRequest, fieldPath+".file_id", "hosted file_id references are not supported")
	}
	if fURL, _ := fileMap["file_url"].(string); strings.TrimSpace(fURL) != "" {
		return nil, newMMError(http.StatusBadRequest, subPath+".file_url", "arbitrary file_url attachments are not supported; send inline UTF-8 text or image URL blocks")
	}
	if fURL, _ := b["file_url"].(string); strings.TrimSpace(fURL) != "" {
		return nil, newMMError(http.StatusBadRequest, fieldPath+".file_url", "arbitrary file_url attachments are not supported; send inline UTF-8 text or image URL blocks")
	}
	filename, _ := fileMap["filename"].(string)
	if filename == "" {
		filename, _ = b["filename"].(string)
	}
	fileData, _ := fileMap["file_data"].(string)
	if fileData == "" {
		fileData, _ = b["file_data"].(string)
	}
	fileData = strings.TrimSpace(fileData)
	if fileData == "" {
		return nil, newMMError(http.StatusBadRequest, subPath, "file attachment missing file_data")
	}
	if hasPrefixFold(fileData, "data:") {
		return normalizeDataURIBlock(fileData, filename, subPath+".file_data", true)
	}
	if !isAllowedTextExtension(filename) {
		return nil, newMMError(http.StatusBadRequest, subPath+".filename", fmt.Sprintf("unsupported file extension for %q; only UTF-8 text/code extensions or image/text data: URIs are accepted", filename))
	}
	raw, _, err := decodeBoundedBase64(fileData, MaxTextFileBytes, http.StatusBadRequest, subPath+".file_data", "text file")
	if err != nil {
		return nil, err
	}
	return unpackUTF8TextBytes(raw, filename, subPath+".file_data")
}

func normalizeContentBlock(raw any, fieldPath string) (map[string]any, error) {
	return normalizeContentBlockWithHistory(raw, fieldPath, false)
}

func normalizeContentBlockWithHistory(raw any, fieldPath string, isHistorical bool) (map[string]any, error) {
	b, ok := raw.(map[string]any)
	if !ok {
		if s, okS := raw.(string); okS {
			return map[string]any{"type": "text", "text": s}, nil
		}
		return nil, newMMError(http.StatusBadRequest, fieldPath, "content block must be an object")
	}
	if b["cache_control"] != nil {
		cleaned := make(map[string]any, len(b)-1)
		for k, v := range b {
			if k != "cache_control" {
				cleaned[k] = v
			}
		}
		b = cleaned
	}
	bType, _ := b["type"].(string)
	switch bType {
	case "text", "input_text", "output_text":
		return b, nil
	case "refusal":
		if ref, okR := b["refusal"].(string); okR {
			return map[string]any{"type": "text", "text": ref}, nil
		}
		return b, nil
	case "thinking", "redacted_thinking", "tool_use":
		return b, nil
	case "tool_result":
		if bc, exists := b["content"]; exists && bc != nil {
			normChild, err := normalizeContentValueWithHistory(bc, fieldPath+".content", isHistorical)
			if err != nil {
				return nil, err
			}
			cloned := make(map[string]any, len(b))
			for k, v := range b {
				cloned[k] = v
			}
			cloned["content"] = normChild
			return cloned, nil
		}
		return b, nil
	case "image":
		// Only trust the canonical fast-path when stamped by our unexported in-memory sentinel.
		if isCanonicalizedImageBlock(b) {
			if nBytes, okB := toInt(b["decodedBytes"]); okB && MaxImageBytes > 0 && nBytes > MaxImageBytes {
				return nil, newMMError(http.StatusRequestEntityTooLarge, fieldPath, fmt.Sprintf("image size (%d bytes) exceeds %d byte limit", nBytes, MaxImageBytes))
			}
			return b, nil
		}
		if b["decodedBytes"] != nil || b["digest"] != nil || b[canonicalImageSentinelKey] != nil {
			cleaned := make(map[string]any, len(b))
			for k, v := range b {
				if k == "decodedBytes" || k == "digest" || k == canonicalImageSentinelKey {
					continue
				}
				cleaned[k] = v
			}
			b = cleaned
		}
		if dataStr, okD := b["data"].(string); okD && b["source"] == nil {
			if rawMime, _ := b["mimeType"].(string); rawMime != "" {
				if strings.EqualFold(normalizeMimeBase(rawMime), "image/svg+xml") {
					return nil, newMMError(http.StatusBadRequest, fieldPath+".mimeType", "image/svg+xml attachments are not supported")
				}
				if !hasPrefixFold(strings.TrimSpace(rawMime), "image/") {
					return nil, rejectUnsupportedMedia(rawMime, fieldPath+".mimeType")
				}
			}
			if rawMime, _ := b["media_type"].(string); rawMime != "" {
				if strings.EqualFold(normalizeMimeBase(rawMime), "image/svg+xml") {
					return nil, newMMError(http.StatusBadRequest, fieldPath+".media_type", "image/svg+xml attachments are not supported")
				}
				if !hasPrefixFold(strings.TrimSpace(rawMime), "image/") {
					return nil, rejectUnsupportedMedia(rawMime, fieldPath+".media_type")
				}
			}
			return decodeAndCanonicalizeImageB64(dataStr, fieldPath+".data", isHistorical)
		}
		src, _ := b["source"].(map[string]any)
		if src == nil {
			return nil, newMMError(http.StatusBadRequest, fieldPath+".source", "image block missing source")
		}
		sType, _ := src["type"].(string)
		switch sType {
		case "base64":
			mime, _ := src["media_type"].(string)
			if strings.EqualFold(normalizeMimeBase(mime), "image/svg+xml") {
				return nil, newMMError(http.StatusBadRequest, fieldPath+".source.media_type", "image/svg+xml attachments are not supported")
			}
			if mime != "" && !hasPrefixFold(strings.TrimSpace(mime), "image/") {
				return nil, rejectUnsupportedMedia(mime, fieldPath+".source.media_type")
			}
			dataStr, _ := src["data"].(string)
			return decodeAndCanonicalizeImageB64(dataStr, fieldPath+".source.data", isHistorical)
		case "url":
			uStr, _ := src["url"].(string)
			return normalizeURLOrDataImageBlockWithHistory(uStr, fieldPath+".source.url", false, isHistorical)
		case "file":
			return nil, newMMError(http.StatusBadRequest, fieldPath+".source", "hosted file_id references are not supported in image blocks")
		default:
			return nil, newMMError(http.StatusBadRequest, fieldPath+".source.type", fmt.Sprintf("unsupported image source type %q", sType))
		}
	case "image_ref":
		uStr, _ := b["url"].(string)
		return normalizeURLOrDataImageBlockWithHistory(uStr, fieldPath+".url", false, isHistorical)
	case "image_url", "input_image":
		var uStr string
		switch u := b["image_url"].(type) {
		case string:
			uStr = u
		case map[string]any:
			uStr, _ = u["url"].(string)
		}
		if uStr == "" {
			uStr, _ = b["url"].(string)
		}
		if uStr == "" {
			if fid, _ := b["file_id"].(string); strings.TrimSpace(fid) != "" {
				return nil, newMMError(http.StatusBadRequest, fieldPath+".file_id", "hosted file_id references are not supported")
			}
			return nil, newMMError(http.StatusBadRequest, fieldPath, bType+" block missing image URL")
		}
		return normalizeURLOrDataImageBlockWithHistory(uStr, fieldPath, false, isHistorical)
	case "file", "input_file", "document":
		return normalizeAnthropicFileOrDocBlock(b, fieldPath)
	case "input_audio", "audio", "audio_url", "video", "video_url":
		return nil, newMMError(http.StatusBadRequest, fieldPath, fmt.Sprintf("unsupported media block type %q; audio and video are not supported", bType))
	default:
		if b["file_id"] != nil {
			return nil, newMMError(http.StatusBadRequest, fieldPath+".file_id", "hosted file_id references are not supported")
		}
		if b["file_data"] != nil {
			return normalizeAnthropicFileOrDocBlock(b, fieldPath)
		}
		if b["audio"] != nil || b["video"] != nil || b["input_audio"] != nil {
			return nil, newMMError(http.StatusBadRequest, fieldPath, "audio and video attachments are not supported")
		}
		if inl := mapField(b, "inlineData", "inline_data"); inl != nil {
			return normalizeGeminiInlineAttachmentWithHistory(inl, fieldPath+".inlineData", isHistorical)
		}
		if fd := mapField(b, "fileData", "file_data"); fd != nil {
			return normalizeGeminiFileAttachment(fd, fieldPath+".fileData")
		}
		return b, nil
	}
}

func normalizeContentValue(val any, fieldPath string) (any, error) {
	return normalizeContentValueWithHistory(val, fieldPath, false)
}

func normalizeContentValueWithHistory(val any, fieldPath string, isHistorical bool) (any, error) {
	switch v := val.(type) {
	case nil:
		return nil, nil
	case string:
		return v, nil
	case map[string]any:
		nb, err := normalizeContentBlockWithHistory(v, fieldPath, isHistorical)
		if err != nil {
			return nil, err
		}
		return []any{nb}, nil
	case []any:
		out := make([]any, 0, len(v))
		for i, item := range v {
			nb, err := normalizeContentBlockWithHistory(item, fmt.Sprintf("%s[%d]", fieldPath, i), isHistorical)
			if err != nil {
				return nil, err
			}
			out = append(out, nb)
		}
		return out, nil
	case []map[string]any:
		out := make([]any, 0, len(v))
		for i, item := range v {
			nb, err := normalizeContentBlockWithHistory(item, fmt.Sprintf("%s[%d]", fieldPath, i), isHistorical)
			if err != nil {
				return nil, err
			}
			out = append(out, nb)
		}
		return out, nil
	default:
		return val, nil
	}
}

func normalizeGeminiInlineAttachment(inl map[string]any, fieldPath string) (map[string]any, error) {
	return normalizeGeminiInlineAttachmentWithHistory(inl, fieldPath, false)
}

func normalizeGeminiInlineAttachmentWithHistory(inl map[string]any, fieldPath string, isHistorical bool) (map[string]any, error) {
	mime := strings.TrimSpace(stringField(inl, "mimeType", "mime_type"))
	data := strings.TrimSpace(stringField(inl, "data", ""))
	lowMime := normalizeMimeBase(mime)
	if data == "" {
		return nil, newMMError(http.StatusBadRequest, fieldPath+".data", "inlineData missing data")
	}
	if lowMime == "image/svg+xml" {
		return nil, newMMError(http.StatusBadRequest, fieldPath+".mimeType", "image/svg+xml attachments are not supported")
	}
	if strings.HasPrefix(lowMime, "image/") {
		return decodeAndCanonicalizeImageB64(data, fieldPath+".data", isHistorical)
	}
	if isSupportedTextMIME(lowMime) {
		raw, _, err := decodeBoundedBase64(data, MaxTextFileBytes, http.StatusBadRequest, fieldPath+".data", "text file")
		if err != nil {
			return nil, err
		}
		return unpackUTF8TextBytes(raw, "inline ("+lowMime+")", fieldPath+".data")
	}
	return nil, rejectUnsupportedMedia(lowMime, fieldPath+".mimeType")
}

func normalizeGeminiFileAttachment(fd map[string]any, fieldPath string) (map[string]any, error) {
	uri := strings.TrimSpace(stringField(fd, "fileUri", "file_uri"))
	mime := strings.TrimSpace(stringField(fd, "mimeType", "mime_type"))
	if uri == "" {
		return nil, newMMError(http.StatusBadRequest, fieldPath+".fileUri", "fileData missing fileUri")
	}
	if hasPrefixFold(uri, "data:") {
		return normalizeDataURIBlock(uri, "", fieldPath+".fileUri", true)
	}
	if mime != "" {
		lowMime := normalizeMimeBase(mime)
		if lowMime == "image/svg+xml" || !strings.HasPrefix(lowMime, "image/") {
			return nil, rejectUnsupportedMedia(lowMime, fieldPath+".mimeType")
		}
	}
	cleanURL, err := validateRemoteImageURLSyntax(uri, fieldPath+".fileUri")
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"type": "image_ref",
		"url":  cleanURL,
	}, nil
}

// CanonicalizeAndValidateMessages runs Stage 1 validation & canonicalization across all 4 protocols.
func CanonicalizeAndValidateMessages(body map[string]any, messages []map[string]any, protocol string) error {
	if body != nil {
		if sys, hasSys := body["system"]; hasSys && sys != nil {
			sysStr, err := extractSystemTextStrict(sys, "system")
			if err != nil {
				return err
			}
			body["system"] = sysStr
		}
	}
	turnStartIdx := latestTurnStartIndex(messages)
	for i, m := range messages {
		if m == nil {
			continue
		}
		delete(m, "cache_control")
		msgPath := fmt.Sprintf("messages[%d]", i)
		if m["audio"] != nil {
			return newMMError(http.StatusBadRequest, msgPath+".audio", "audio message inputs are not supported")
		}
		if c, hasC := m["content"]; hasC && c != nil {
			normContent, err := normalizeContentValueWithHistory(c, msgPath+".content", i < turnStartIdx)
			if err != nil {
				return err
			}
			m["content"] = normContent
		}
	}
	return nil
}

func walkMessageMediaBlocks(m map[string]any, visit func(b map[string]any, inToolResult bool)) {
	if m == nil {
		return
	}
	var walkArr func(val any, inTR bool)
	walkArr = func(val any, inTR bool) {
		switch arr := val.(type) {
		case []any:
			for _, item := range arr {
				if bm, ok := item.(map[string]any); ok {
					bType, _ := bm["type"].(string)
					if bType == "tool_result" {
						walkArr(bm["content"], true)
						continue
					}
					visit(bm, inTR)
				}
			}
		case []map[string]any:
			for _, bm := range arr {
				bType, _ := bm["type"].(string)
				if bType == "tool_result" {
					walkArr(bm["content"], true)
					continue
				}
				visit(bm, inTR)
			}
		case map[string]any:
			bType, _ := arr["type"].(string)
			if bType == "tool_result" {
				walkArr(arr["content"], true)
				return
			}
			visit(arr, inTR)
		}
	}
	walkArr(m["content"], false)
}

func inlineImageDecodedBytes(b map[string]any) int {
	if isCanonicalizedImageBlock(b) {
		if n, ok := toInt(b["decodedBytes"]); ok && n > 0 {
			return n
		}
	} else if n, ok := b["decodedBytes"].(int); ok && n > 0 {
		// Go-native int only (never produced by encoding/json unmarshal of untrusted HTTP JSON,
		// and normalizeContentBlock strips untrusted decodedBytes on Stage 1 anyway).
		return n
	}
	if dataStr, ok := b["data"].(string); ok && dataStr != "" {
		clean := cleanBase64Whitespace(dataStr)
		return (len(clean) * 3) / 4
	}
	if src, ok := b["source"].(map[string]any); ok {
		if dataStr, okD := src["data"].(string); okD && dataStr != "" {
			clean := cleanBase64Whitespace(dataStr)
			return (len(clean) * 3) / 4
		}
	}
	return 0
}

func inlineImageSummaryKB(b map[string]any) int {
	kb := inlineImageDecodedBytes(b) / 1024
	if kb < 1 {
		kb = 1
	}
	return kb
}

func imageFingerprintTokens(m map[string]any) []string {
	var tokens []string
	walkMessageMediaBlocks(m, func(b map[string]any, _ bool) {
		bType, _ := b["type"].(string)
		switch bType {
		case "image":
			if isCanonicalizedImageBlock(b) {
				if dig, ok := b["digest"].(string); ok && dig != "" {
					tokens = append(tokens, "img:"+dig)
					return
				}
			}
			if dataStr, ok := b["data"].(string); ok && dataStr != "" {
				tokens = append(tokens, "img:"+inlineImageDigest("image", []byte(dataStr)))
				return
			}
			if src, ok := b["source"].(map[string]any); ok {
				if dataStr, okD := src["data"].(string); okD && dataStr != "" {
					tokens = append(tokens, "img:"+inlineImageDigest("image", []byte(dataStr)))
				}
			}
		case "image_ref":
			if u, ok := b["url"].(string); ok && u != "" {
				tokens = append(tokens, "imgurl:"+shortSha256(u))
			}
		case "image_url", "input_image":
			var uStr string
			switch u := b["image_url"].(type) {
			case string:
				uStr = u
			case map[string]any:
				uStr, _ = u["url"].(string)
			}
			if uStr == "" {
				uStr, _ = b["url"].(string)
			}
			if uStr != "" {
				tokens = append(tokens, "imgurl:"+shortSha256(uStr))
			}
		}
	})
	return tokens
}

func latestTurnStartIndex(messages []map[string]any) int {
	for i := len(messages) - 1; i >= 0; i-- {
		if role, _ := messages[i]["role"].(string); role == "assistant" {
			if i+1 < len(messages) {
				return i + 1
			}
			return len(messages)
		}
	}
	return 0
}

func countMessageImagesAndBytes(m map[string]any) (count int, bytesSum int) {
	walkMessageMediaBlocks(m, func(b map[string]any, _ bool) {
		bType, _ := b["type"].(string)
		switch bType {
		case "image":
			count++
			bytesSum += inlineImageDecodedBytes(b)
		case "image_ref", "image_url", "input_image":
			count++
		}
	})
	return count, bytesSum
}

// validateLatestTurnImageBudget enforces model-family MaxImages (400) and prompt byte budget
// (413) on the current turn while returning total image count across the request for count_tokens.
func validateLatestTurnImageBudget(messages []map[string]any, m *ModelInfo) (latestImgs, totalImgs, latestBytes int, err error) {
	maxImgs := effectiveModelMaxImages(m)
	modelName := DefaultModelID
	if m != nil && m.ID != "" {
		modelName = m.ID
	}
	start := latestTurnStartIndex(messages)
	for i, msg := range messages {
		cnt, bSum := countMessageImagesAndBytes(msg)
		totalImgs += cnt
		if i >= start {
			latestImgs += cnt
			latestBytes += bSum
		}
	}
	if maxImgs > 0 && latestImgs > maxImgs {
		return latestImgs, totalImgs, latestBytes, newMMError(
			http.StatusBadRequest,
			"messages",
			fmt.Sprintf("latest turn has %d image(s), exceeding limit of %d for model %s", latestImgs, maxImgs, modelName),
		)
	}
	if MaxPromptImageBytes > 0 && latestBytes > MaxPromptImageBytes {
		return latestImgs, totalImgs, latestBytes, newMMError(
			http.StatusRequestEntityTooLarge,
			"messages",
			fmt.Sprintf("latest turn inline images (%d decoded bytes) exceed %d byte prompt limit", latestBytes, MaxPromptImageBytes),
		)
	}
	return latestImgs, totalImgs, latestBytes, nil
}
