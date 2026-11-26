package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	remoteImageCacheMaxBytes = 64 * 1024 * 1024
	remoteImageCacheTTL      = 15 * time.Minute
	remoteImageFetchTimeout  = 10 * time.Second
	remoteImageMaxRedirects  = 3
)

type cachedRemoteImage struct {
	key       string
	seq       uint64
	mime      string
	b64       string
	rawBytes  int
	digest    string
	expiresAt time.Time
}

type cacheOrderEntry struct {
	key string
	seq uint64
}

type imageInflight struct {
	done chan struct{}
	res  cachedRemoteImage
	err  error
}

// RemoteImageFetcher resolves remote http(s) images lazily with a bounded 64MB/15m
// content-addressed cache, singleflight deduplication, socket-level SSRF control,
// and mandatory 16-byte magic sniffing.
type RemoteImageFetcher struct {
	client *http.Client

	cacheMu    sync.Mutex
	cache      map[string]*cachedRemoteImage
	cacheOrder []cacheOrderEntry
	cacheBytes int
	nextSeq    uint64

	sfMu     sync.Mutex
	inflight map[string]*imageInflight
}

func safePublicSocketControl(network, address string, c syscall.RawConn) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("invalid remote image socket address %q", address)
	}
	if port != "80" && port != "443" {
		return fmt.Errorf("remote image port %q blocked by SSRF policy", port)
	}
	addr, err := netip.ParseAddr(host)
	if err != nil || !isPublicNetipAddr(addr) {
		return fmt.Errorf("non-public IP %q blocked by SSRF policy", host)
	}
	return nil
}

func newRemoteImageFetcher(dialControl func(network, address string, c syscall.RawConn) error) *RemoteImageFetcher {
	if dialControl == nil {
		dialControl = safePublicSocketControl
	}
	dialer := &net.Dialer{
		Timeout:   8 * time.Second,
		KeepAlive: 30 * time.Second,
		Control:   dialControl,
	}
	tr := &http.Transport{
		Proxy:                 nil, // Never inherit host proxy env vars that might reach internal services.
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          16,
		IdleConnTimeout:       30 * time.Second,
		TLSHandshakeTimeout:   8 * time.Second,
		ResponseHeaderTimeout: 8 * time.Second,
	}
	cli := &http.Client{
		Transport: tr,
		Timeout:   remoteImageFetchTimeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= remoteImageMaxRedirects {
				return errors.New("exceeded maximum of 3 HTTP redirects")
			}
			if _, err := validateRemoteImageURLSyntax(req.URL.String(), "image_url"); err != nil {
				return err
			}
			return nil
		},
	}
	return &RemoteImageFetcher{
		client:   cli,
		cache:    make(map[string]*cachedRemoteImage),
		inflight: make(map[string]*imageInflight),
	}
}

var GlobalImageFetcher = newRemoteImageFetcher(nil)

func urlCacheKey(rawURL string) string {
	sum := sha256.Sum256([]byte(rawURL))
	return hex.EncodeToString(sum[:])
}

func (f *RemoteImageFetcher) getCached(key string, now time.Time) (cachedRemoteImage, bool) {
	f.cacheMu.Lock()
	defer f.cacheMu.Unlock()
	ent, ok := f.cache[key]
	if !ok {
		return cachedRemoteImage{}, false
	}
	if !ent.expiresAt.After(now) {
		f.deleteLocked(key)
		return cachedRemoteImage{}, false
	}
	return *ent, true
}

func (f *RemoteImageFetcher) deleteLocked(key string) {
	ent, ok := f.cache[key]
	if !ok {
		return
	}
	f.cacheBytes -= len(ent.b64) + ent.rawBytes
	if f.cacheBytes < 0 {
		f.cacheBytes = 0
	}
	delete(f.cache, key)
}

func (f *RemoteImageFetcher) putCached(key string, item cachedRemoteImage, now time.Time) {
	cost := len(item.b64) + item.rawBytes
	if cost <= 0 || cost > remoteImageCacheMaxBytes/2 {
		return
	}
	f.cacheMu.Lock()
	defer f.cacheMu.Unlock()
	// Sweep expired entries lazily
	for k, ent := range f.cache {
		if !ent.expiresAt.After(now) {
			f.deleteLocked(k)
		}
	}
	if _, exists := f.cache[key]; exists {
		f.deleteLocked(key)
	}
	for f.cacheBytes+cost > remoteImageCacheMaxBytes && len(f.cacheOrder) > 0 {
		oldest := f.cacheOrder[0]
		f.cacheOrder = f.cacheOrder[1:]
		if cur, ok := f.cache[oldest.key]; ok && cur.seq == oldest.seq {
			f.deleteLocked(oldest.key)
		}
	}
	if len(f.cacheOrder) > 4096 {
		// Compact dead tombstones whose sequence was superseded or expired.
		compacted := make([]cacheOrderEntry, 0, len(f.cache))
		for _, entry := range f.cacheOrder {
			if cur, ok := f.cache[entry.key]; ok && cur.seq == entry.seq {
				compacted = append(compacted, entry)
			}
		}
		f.cacheOrder = compacted
	}
	f.nextSeq++
	item.key = key
	item.seq = f.nextSeq
	item.expiresAt = now.Add(remoteImageCacheTTL)
	copyItem := item
	f.cache[key] = &copyItem
	f.cacheOrder = append(f.cacheOrder, cacheOrderEntry{key: key, seq: item.seq})
	f.cacheBytes += cost
}

func (f *RemoteImageFetcher) Fetch(ctx context.Context, rawURL, fieldPath string) (cachedRemoteImage, error) {
	cleanURL, err := validateRemoteImageURLSyntax(rawURL, fieldPath)
	if err != nil {
		return cachedRemoteImage{}, err
	}
	key := urlCacheKey(cleanURL)
	now := time.Now()
	if hit, ok := f.getCached(key, now); ok {
		return hit, nil
	}

	f.sfMu.Lock()
	if call, ok := f.inflight[key]; ok {
		f.sfMu.Unlock()
		select {
		case <-call.done:
			return call.res, call.err
		case <-ctx.Done():
			return cachedRemoteImage{}, newMMError(http.StatusBadRequest, fieldPath, "request canceled while fetching remote image")
		}
	}
	call := &imageInflight{done: make(chan struct{})}
	f.inflight[key] = call
	f.sfMu.Unlock()

	go func() {
		fetchCtx, cancel := context.WithTimeout(context.Background(), remoteImageFetchTimeout)
		defer cancel()
		res, fetchErr := f.fetchUncached(fetchCtx, cleanURL, fieldPath)
		if fetchErr == nil {
			f.putCached(key, res, time.Now())
		}
		call.res = res
		call.err = fetchErr
		close(call.done)
		f.sfMu.Lock()
		if f.inflight[key] == call {
			delete(f.inflight, key)
		}
		f.sfMu.Unlock()
	}()

	select {
	case <-call.done:
		return call.res, call.err
	case <-ctx.Done():
		return cachedRemoteImage{}, newMMError(http.StatusBadRequest, fieldPath, "request canceled while fetching remote image")
	}
}

func (f *RemoteImageFetcher) fetchUncached(ctx context.Context, cleanURL, fieldPath string) (cachedRemoteImage, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, cleanURL, nil)
	if err != nil {
		return cachedRemoteImage{}, newMMError(http.StatusBadRequest, fieldPath, "failed to construct remote image request")
	}
	req.Header.Set("Accept", "image/png, image/jpeg, image/webp, image/gif")
	req.Header.Set("User-Agent", "agy-acp-gateway/1.0")

	resp, err := f.client.Do(req)
	if err != nil {
		var mmErr *MultimodalValidationError
		if errors.As(err, &mmErr) {
			return cachedRemoteImage{}, mmErr
		}
		return cachedRemoteImage{}, newMMError(http.StatusBadRequest, fieldPath, "failed to fetch remote image URL: "+redactURLSecretsForError(err.Error()))
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return cachedRemoteImage{}, newMMError(http.StatusBadRequest, fieldPath, fmt.Sprintf("remote image URL returned HTTP %d", resp.StatusCode))
	}
	limit := MaxImageBytes
	if limit <= 0 {
		limit = 10 * 1024 * 1024
	}
	if resp.ContentLength > int64(limit) {
		return cachedRemoteImage{}, newMMError(http.StatusRequestEntityTooLarge, fieldPath, fmt.Sprintf("remote image Content-Length (%d bytes) exceeds %d byte limit", resp.ContentLength, limit))
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, int64(limit)+1))
	if err != nil {
		return cachedRemoteImage{}, newMMError(http.StatusBadRequest, fieldPath, "failed reading remote image body: "+redactURLSecretsForError(err.Error()))
	}
	if len(raw) == 0 {
		return cachedRemoteImage{}, newMMError(http.StatusBadRequest, fieldPath, "remote image body is empty")
	}
	if len(raw) > limit {
		return cachedRemoteImage{}, newMMError(http.StatusRequestEntityTooLarge, fieldPath, fmt.Sprintf("remote image exceeds %d byte limit", limit))
	}
	mime, ok := sniffAllowedImageMime(raw)
	if !ok {
		return cachedRemoteImage{}, newMMError(http.StatusBadRequest, fieldPath, "remote image payload failed PNG/JPEG/WEBP/GIF magic header verification")
	}
	return cachedRemoteImage{
		mime:     mime,
		b64:      base64.StdEncoding.EncodeToString(raw),
		rawBytes: len(raw),
		digest:   inlineImageDigest(mime, raw),
	}, nil
}

type mediaItemMeta struct {
	msgIdx int
	seq    int
	block  map[string]any
}

type resolvedMediaSlot struct {
	keep       bool
	anchorNum  int
	historyMsg string
	acpBlock   map[string]any
	rawBytes   int
}

func collectMessageMediaMeta(msgIdx int, m map[string]any) []mediaItemMeta {
	var out []mediaItemMeta
	seq := 0
	walkMessageMediaBlocks(m, func(b map[string]any, _ bool) {
		bType, _ := b["type"].(string)
		switch bType {
		case "image", "image_ref", "image_url", "input_image":
			out = append(out, mediaItemMeta{msgIdx: msgIdx, seq: seq, block: b})
			seq++
		}
	})
	return out
}

func resolveInlineOrRemoteBlock(ctx context.Context, fetcher *RemoteImageFetcher, b map[string]any, fieldPath string) (acpBlock map[string]any, rawBytes int, err error) {
	bType, _ := b["type"].(string)
	switch bType {
	case "image":
		if isCanonicalizedImageBlock(b) {
			bytesLen := inlineImageDecodedBytes(b)
			if MaxImageBytes > 0 && bytesLen > MaxImageBytes {
				return nil, 0, newMMError(http.StatusRequestEntityTooLarge, fieldPath, fmt.Sprintf("image (%d bytes) exceeds %d byte limit", bytesLen, MaxImageBytes))
			}
			return sanitizeACPImageBlock(b), bytesLen, nil
		}
		if src, ok := b["source"].(map[string]any); ok && b["data"] == nil {
			sType, _ := src["type"].(string)
			if sType == "base64" {
				if rawMime, _ := src["media_type"].(string); rawMime != "" {
					if strings.EqualFold(normalizeMimeBase(rawMime), "image/svg+xml") {
						return nil, 0, newMMError(http.StatusBadRequest, fieldPath+".source.media_type", "image/svg+xml attachments are not supported")
					}
					if !hasPrefixFold(strings.TrimSpace(rawMime), "image/") {
						return nil, 0, rejectUnsupportedMedia(rawMime, fieldPath+".source.media_type")
					}
				}
				dataStr, _ := src["data"].(string)
				canon, cErr := decodeAndCanonicalizeImageB64(dataStr, fieldPath, false)
				if cErr != nil {
					return nil, 0, cErr
				}
				return sanitizeACPImageBlock(canon), inlineImageDecodedBytes(canon), nil
			}
		}
		if dataStr, okD := b["data"].(string); okD {
			if rawMime, _ := b["mimeType"].(string); rawMime != "" {
				if strings.EqualFold(normalizeMimeBase(rawMime), "image/svg+xml") {
					return nil, 0, newMMError(http.StatusBadRequest, fieldPath+".mimeType", "image/svg+xml attachments are not supported")
				}
				if !hasPrefixFold(strings.TrimSpace(rawMime), "image/") {
					return nil, 0, rejectUnsupportedMedia(rawMime, fieldPath+".mimeType")
				}
			}
			canon, cErr := decodeAndCanonicalizeImageB64(dataStr, fieldPath, false)
			if cErr != nil {
				return nil, 0, cErr
			}
			return sanitizeACPImageBlock(canon), inlineImageDecodedBytes(canon), nil
		}
		return nil, 0, newMMError(http.StatusBadRequest, fieldPath, "image block missing base64 data")
	case "image_ref":
		uStr, _ := b["url"].(string)
		img, fErr := fetcher.Fetch(ctx, uStr, fieldPath)
		if fErr != nil {
			return nil, 0, fErr
		}
		return map[string]any{"type": "image", "mimeType": img.mime, "data": img.b64}, img.rawBytes, nil
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
		norm, nErr := normalizeURLOrDataImageBlock(uStr, fieldPath, false)
		if nErr != nil {
			return nil, 0, nErr
		}
		if norm["type"] == "image" {
			return sanitizeACPImageBlock(norm), inlineImageDecodedBytes(norm), nil
		}
		uClean, _ := norm["url"].(string)
		img, fErr := fetcher.Fetch(ctx, uClean, fieldPath)
		if fErr != nil {
			return nil, 0, fErr
		}
		return map[string]any{"type": "image", "mimeType": img.mime, "data": img.b64}, img.rawBytes, nil
	default:
		return nil, 0, nil
	}
}

func resolveToolResultImages(ctx context.Context, fetcher *RemoteImageFetcher, matched []ToolResultInput, model *ModelInfo) ([]ToolResultInput, error) {
	if fetcher == nil {
		fetcher = GlobalImageFetcher
	}
	maxImgs := effectiveModelMaxImages(model)
	maxBytes := MaxPromptImageBytes
	if maxBytes <= 0 {
		maxBytes = 32 * 1024 * 1024
	}
	totalImgs := 0
	totalBytes := 0
	out := make([]ToolResultInput, len(matched))
	for i, tr := range matched {
		out[i] = tr
		if len(tr.Images) == 0 {
			continue
		}
		resolvedImgs := make([]map[string]any, 0, len(tr.Images))
		for j, img := range tr.Images {
			totalImgs++
			if maxImgs > 0 && totalImgs > maxImgs {
				return nil, newMMError(http.StatusBadRequest, fmt.Sprintf("tool_result[%d].images[%d]", i, j), fmt.Sprintf("tool result images (%d) exceed model limit of %d", totalImgs, maxImgs))
			}
			acpBlk, bLen, err := resolveInlineOrRemoteBlock(ctx, fetcher, img, fmt.Sprintf("tool_result[%d].images[%d]", i, j))
			if err != nil {
				return nil, err
			}
			totalBytes += bLen
			if totalBytes > maxBytes {
				return nil, newMMError(http.StatusRequestEntityTooLarge, fmt.Sprintf("tool_result[%d].images[%d]", i, j), fmt.Sprintf("tool result total image payload (%d bytes) exceeds %d byte prompt limit", totalBytes, maxBytes))
			}
			if acpBlk != nil {
				resolvedImgs = append(resolvedImgs, acpBlk)
			}
		}
		out[i].Images = resolvedImgs
	}
	return out, nil
}

func resolveAndBuildPromptBlocks(ctx context.Context, fetcher *RemoteImageFetcher, unseenMessages []map[string]any, isRollover bool, allMessages []map[string]any, maxChars int, model *ModelInfo) ([]map[string]any, error) {
	if maxChars <= 0 {
		maxChars = MaxTranscriptChar
	}
	if fetcher == nil {
		fetcher = GlobalImageFetcher
	}
	if ctx == nil {
		ctx = context.Background()
	}
	maxImgs := effectiveModelMaxImages(model)
	maxBytes := MaxPromptImageBytes
	if maxBytes <= 0 {
		maxBytes = 32 * 1024 * 1024
	}

	resolveWindow := func(historyMsgs, currentMsgs []map[string]any) (histImgs, curImgs []map[string]any, notesByMsg map[int][]string, err error) {
		notesByMsg = make(map[int][]string)
		keptCount := 0
		keptBytes := 0

		// 1. Current turn is strictly enforced (no silent dropping; 400/413 on failure or overflow).
		for mi, m := range currentMsgs {
			metas := collectMessageMediaMeta(mi, m)
			for _, mm := range metas {
				if maxImgs > 0 && keptCount+1 > maxImgs {
					return nil, nil, nil, newMMError(http.StatusBadRequest, "messages", fmt.Sprintf("current turn exceeds model limit of %d images", maxImgs))
				}
				acpBlk, bLen, rErr := resolveInlineOrRemoteBlock(ctx, fetcher, mm.block, "messages")
				if rErr != nil {
					return nil, nil, nil, rErr
				}
				if keptBytes+bLen > maxBytes {
					return nil, nil, nil, newMMError(http.StatusRequestEntityTooLarge, "messages", fmt.Sprintf("current turn images (%d bytes) exceed %d byte prompt limit", keptBytes+bLen, maxBytes))
				}
				keptCount++
				keptBytes += bLen
				if acpBlk != nil {
					curImgs = append(curImgs, acpBlk)
				}
			}
		}

		if len(historyMsgs) == 0 {
			return nil, curImgs, notesByMsg, nil
		}

		// 2. Walk history newest-to-oldest within remaining count & byte budget.
		msgSlots := make([][]resolvedMediaSlot, len(historyMsgs))
		for i := len(historyMsgs) - 1; i >= 0; i-- {
			metas := collectMessageMediaMeta(i, historyMsgs[i])
			slots := make([]resolvedMediaSlot, len(metas))
			for j := len(metas) - 1; j >= 0; j-- {
				if (maxImgs > 0 && keptCount >= maxImgs) || keptBytes >= maxBytes {
					slots[j] = resolvedMediaSlot{historyMsg: "[Earlier image omitted due to prompt image budget]"}
					continue
				}
				acpBlk, bLen, rErr := resolveInlineOrRemoteBlock(ctx, fetcher, metas[j].block, fmt.Sprintf("messages[%d]", i))
				if rErr != nil {
					if ctx.Err() != nil {
						return nil, nil, nil, ctx.Err()
					}
					// If an earlier remote image expired during rollover, keep the rollover alive with a note.
					if metas[j].block["type"] == "image_ref" || metas[j].block["type"] == "image_url" || metas[j].block["type"] == "input_image" {
						slots[j] = resolvedMediaSlot{historyMsg: "[Earlier remote image unavailable during rollover]"}
						continue
					}
					slots[j] = resolvedMediaSlot{historyMsg: "[Earlier image omitted due to prompt image budget]"}
					continue
				}
				if keptBytes+bLen > maxBytes {
					slots[j] = resolvedMediaSlot{historyMsg: "[Earlier image omitted due to prompt image budget]"}
					continue
				}
				keptCount++
				keptBytes += bLen
				slots[j] = resolvedMediaSlot{keep: true, acpBlock: acpBlk, rawBytes: bLen}
			}
			msgSlots[i] = slots
		}

		// 3. Assign chronological [Image #k] anchors for kept historical images.
		anchorIdx := 0
		for i := 0; i < len(historyMsgs); i++ {
			for j := range msgSlots[i] {
				sl := &msgSlots[i][j]
				if sl.keep && sl.acpBlock != nil {
					anchorIdx++
					sl.anchorNum = anchorIdx
					kb := sl.rawBytes / 1024
					if kb < 1 {
						kb = 1
					}
					mimeStr, _ := sl.acpBlock["mimeType"].(string)
					if mimeStr == "" {
						mimeStr = "image"
					}
					sl.historyMsg = fmt.Sprintf("[Image #%d (%s, ~%dKB)]", anchorIdx, mimeStr, kb)
					histImgs = append(histImgs, sl.acpBlock)
				}
				if sl.historyMsg != "" {
					notesByMsg[i] = append(notesByMsg[i], sl.historyMsg)
				}
			}
		}
		return histImgs, curImgs, notesByMsg, nil
	}

	if isRollover && len(allMessages) > 1 {
		var cleanAll []map[string]any
		for _, msg := range allMessages {
			if isFailedErrorTurnMessage(msg) {
				if len(cleanAll) > 0 && cleanAll[len(cleanAll)-1]["role"] == "user" && !messageHasToolResult(cleanAll[len(cleanAll)-1]) {
					cleanAll = cleanAll[:len(cleanAll)-1]
				}
				continue
			}
			cleanAll = append(cleanAll, msg)
		}
		if len(cleanAll) == 0 {
			cleanAll = allMessages
		}

		curStart := rolloverCurrentTurnStart(cleanAll)
		historyMsgs := cleanAll[:curStart]
		currentMsgs := cleanAll[curStart:]

		histImgs, curImgs, notesByMsg, err := resolveWindow(historyMsgs, currentMsgs)
		if err != nil {
			return nil, err
		}

		var historyLines []string
		for i, m := range historyMsgs {
			txt := trimBlankOuterLines(formatSingleMessageTextWithMediaNotes(m, notesByMsg[i]))
			if txt != "" {
				role, _ := m["role"].(string)
				if role == "" {
					role = "user"
				}
				historyLines = append(historyLines, fmt.Sprintf("[%s]\n%s", strings.ToUpper(role), txt))
			}
		}
		transcriptText := strings.Join(historyLines, "\n\n")
		if len(transcriptText) > maxChars {
			headChars := 220000
			if headChars > maxChars*15/100 {
				headChars = maxChars * 15 / 100
			}
			tailChars := maxChars - headChars
			headEnd := headChars
			if headEnd > len(transcriptText) {
				headEnd = len(transcriptText)
			}
			for headEnd > 0 && headEnd < len(transcriptText) && !utf8.RuneStart(transcriptText[headEnd]) {
				headEnd--
			}
			tailStart := len(transcriptText) - tailChars
			if tailStart < headEnd {
				tailStart = headEnd
			}
			for tailStart < len(transcriptText) && !utf8.RuneStart(transcriptText[tailStart]) {
				tailStart++
			}
			trimmed := tailStart - headEnd
			transcriptText = transcriptText[:headEnd] +
				fmt.Sprintf("\n\n[... %d chars of intermediate tool outputs trimmed to stay within the model context window ...]\n\n", trimmed) +
				transcriptText[tailStart:]
		}

		var curTexts []string
		for _, m := range currentMsgs {
			role, _ := m["role"].(string)
			t := trimBlankOuterLines(formatSingleMessageText(m))
			if t != "" {
				if len(currentMsgs) > 1 && role != "" {
					curTexts = append(curTexts, fmt.Sprintf("[%s]\n%s", strings.ToUpper(role), t))
				} else {
					curTexts = append(curTexts, t)
				}
			}
		}
		lastText := strings.Join(curTexts, "\n\n")

		var out []map[string]any
		out = append(out, map[string]any{
			"type": "resource",
			"resource": map[string]any{
				"uri":      "context://host/conversation-history.txt",
				"mimeType": "text/plain",
				"text":     transcriptText,
			},
		})
		out = append(out, histImgs...)
		out = append(out, curImgs...)
		out = append(out, map[string]any{
			"type": "text",
			"text": "[Conversation History Restored — All " + McpBridgeName + " MCP Tools Active]\n" + lastText,
		})
		return out, nil
	}

	curStart := latestTurnStartIndex(unseenMessages)
	if curStart > len(unseenMessages) {
		curStart = len(unseenMessages)
	}
	unseenHist := unseenMessages[:curStart]
	unseenCur := unseenMessages[curStart:]
	histImgs, curImgs, notesByMsg, err := resolveWindow(unseenHist, unseenCur)
	if err != nil {
		return nil, err
	}

	var nonEmpty []struct {
		Role string
		Text string
	}
	for i, m := range unseenMessages {
		role, _ := m["role"].(string)
		var t string
		if i < curStart {
			t = trimBlankOuterLines(formatSingleMessageTextWithMediaNotes(m, notesByMsg[i]))
		} else {
			t = trimBlankOuterLines(formatSingleMessageText(m))
		}
		if t != "" {
			nonEmpty = append(nonEmpty, struct {
				Role string
				Text string
			}{Role: role, Text: t})
		}
	}
	var texts []string
	for _, item := range nonEmpty {
		if len(nonEmpty) > 1 {
			texts = append(texts, fmt.Sprintf("[%s]\n%s", strings.ToUpper(item.Role), item.Text))
		} else {
			texts = append(texts, item.Text)
		}
	}
	joined := strings.Join(texts, "\n\n")
	if joined == "" {
		joined = "Continue."
	}
	var out []map[string]any
	out = append(out, histImgs...)
	out = append(out, curImgs...)
	out = append(out, map[string]any{"type": "text", "text": joined})
	return out, nil
}

func trimBlankOuterLines(s string) string {
	if strings.TrimSpace(s) == "" {
		return ""
	}
	s = strings.TrimLeft(s, "\r\n")
	return strings.TrimRightFunc(s, unicode.IsSpace)
}

func messageHasToolResult(m map[string]any) bool {
	if m == nil {
		return false
	}
	switch arr := m["content"].(type) {
	case []any:
		for _, it := range arr {
			if bm, ok := it.(map[string]any); ok && bm["type"] == "tool_result" {
				return true
			}
		}
	case []map[string]any:
		for _, bm := range arr {
			if bm["type"] == "tool_result" {
				return true
			}
		}
	}
	return false
}

// rolloverCurrentTurnStart keeps parallel tool_result or multi-part media messages in the current
// turn strict (even when followed by a user text message), while treating abandoned text-only
// user prompts before a new prompt as history.
func rolloverCurrentTurnStart(cleanAll []map[string]any) int {
	if len(cleanAll) == 0 {
		return 0
	}
	curStart := len(cleanAll) - 1
	for curStart > 0 {
		prev := cleanAll[curStart-1]
		if role, _ := prev["role"].(string); role == "assistant" {
			break
		}
		prevIsTR := messageHasToolResult(prev)
		prevImgs, _ := countMessageImagesAndBytes(prev)
		if prevIsTR || prevImgs > 0 {
			curStart--
			continue
		}
		break
	}
	return curStart
}
