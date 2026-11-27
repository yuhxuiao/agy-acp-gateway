package main

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"
)

const (
	maxPDFStreamDecompressedBytes = 8 * 1024 * 1024
	maxPDFTotalDecompressedBytes  = 16 * 1024 * 1024
	maxPDFExtractedRunes          = 100000
	maxPDFParsedStreams           = 2048
	maxPDFCMapRangeEntries        = 4096
	maxPDFCMapTotalEntries        = 65536
	maxPDFArrayDepth              = 32
	maxPDFContentTokens           = 500000
	minPDFMeaningfulRunes         = 20
)

type pdfCMap struct {
	byHex map[string]string // uppercase hex key (2 or 4 hex chars) -> UTF-8 string
	lens  []int             // distinct byte lengths present (sorted descending: e.g. 2, 1)
}

func (cm *pdfCMap) add(src []byte, dst string) bool {
	if len(src) == 0 || len(src) > 4 || dst == "" {
		return false
	}
	if cm.byHex == nil {
		cm.byHex = make(map[string]string)
	}
	if len(cm.byHex) >= maxPDFCMapTotalEntries {
		return false
	}
	k := strings.ToUpper(hex.EncodeToString(src))
	cm.byHex[k] = dst
	return true
}

func (cm *pdfCMap) finalize() {
	if cm == nil || len(cm.byHex) == 0 {
		return
	}
	hasLen := map[int]bool{}
	for k := range cm.byHex {
		hasLen[len(k)/2] = true
	}
	cm.lens = cm.lens[:0]
	for l := 4; l >= 1; l-- {
		if hasLen[l] {
			cm.lens = append(cm.lens, l)
		}
	}
}

func (cm *pdfCMap) decode(raw []byte) (string, bool) {
	if cm == nil || len(cm.byHex) == 0 || len(raw) == 0 {
		return "", false
	}
	var sb strings.Builder
	matched := 0
	for i := 0; i < len(raw); {
		found := false
		for _, l := range cm.lens {
			if i+l > len(raw) {
				continue
			}
			k := strings.ToUpper(hex.EncodeToString(raw[i : i+l]))
			if mapped, ok := cm.byHex[k]; ok {
				sb.WriteString(mapped)
				i += l
				matched++
				found = true
				break
			}
		}
		if !found {
			// Skip single unmatched byte; if it's printable ASCII we'll only use CMap when majority matched.
			b := raw[i]
			if b >= 0x20 && b <= 0x7e {
				sb.WriteByte(b)
			}
			i++
		}
	}
	return sb.String(), matched > 0
}

func decodePDFUTF16BE(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	if len(b)%2 == 1 {
		b = append(append([]byte(nil), b...), 0)
	}
	u16 := make([]uint16, 0, len(b)/2)
	for i := 0; i+1 < len(b); i += 2 {
		u16 = append(u16, binary.BigEndian.Uint16(b[i:i+2]))
	}
	return string(utf16.Decode(u16))
}

func parsePDFHexToken(s string) ([]byte, bool) {
	s = strings.TrimSpace(s)
	if len(s) < 2 || s[0] != '<' || s[len(s)-1] != '>' {
		return nil, false
	}
	inner := s[1 : len(s)-1]
	var clean []byte
	clean = make([]byte, 0, len(inner)+1)
	for i := 0; i < len(inner); i++ {
		c := inner[i]
		if (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F') {
			clean = append(clean, c)
		}
	}
	if len(clean) == 0 {
		return nil, false
	}
	if len(clean)%2 == 1 {
		clean = append(clean, '0')
	}
	out := make([]byte, len(clean)/2)
	if _, err := hex.Decode(out, clean); err != nil {
		return nil, false
	}
	return out, true
}

func bytesToBigUint(b []byte) uint64 {
	var v uint64
	for _, x := range b {
		v = (v << 8) | uint64(x)
	}
	return v
}

func bigUintToBytes(v uint64, width int) []byte {
	out := make([]byte, width)
	for i := width - 1; i >= 0; i-- {
		out[i] = byte(v & 0xff)
		v >>= 8
	}
	return out
}

func parseCMapStream(data []byte) *pdfCMap {
	if !bytes.Contains(data, []byte("beginbfchar")) && !bytes.Contains(data, []byte("beginbfrange")) {
		return nil
	}
	cm := &pdfCMap{byHex: make(map[string]string)}
	text := string(data)

	// Parse beginbfchar ... endbfchar blocks.
	pos := 0
	for {
		idx := strings.Index(text[pos:], "beginbfchar")
		if idx < 0 {
			break
		}
		blockStart := pos + idx + len("beginbfchar")
		endIdx := strings.Index(text[blockStart:], "endbfchar")
		if endIdx < 0 {
			break
		}
		block := text[blockStart : blockStart+endIdx]
		pos = blockStart + endIdx + len("endbfchar")

		hexToks := scanHexTokens(block)
		for i := 0; i+1 < len(hexToks); i += 2 {
			src, ok1 := parsePDFHexToken(hexToks[i])
			dst, ok2 := parsePDFHexToken(hexToks[i+1])
			if ok1 && ok2 {
				cm.add(src, decodePDFUTF16BE(dst))
			}
		}
	}

	// Parse beginbfrange ... endbfrange blocks.
	pos = 0
	for {
		idx := strings.Index(text[pos:], "beginbfrange")
		if idx < 0 {
			break
		}
		blockStart := pos + idx + len("beginbfrange")
		endIdx := strings.Index(text[blockStart:], "endbfrange")
		if endIdx < 0 {
			break
		}
		block := text[blockStart : blockStart+endIdx]
		pos = blockStart + endIdx + len("endbfrange")

		toks := scanCMapRangeTokens(block)
		for i := 0; i < len(toks); {
			if i+2 >= len(toks) {
				break
			}
			srcStart, ok1 := parsePDFHexToken(toks[i])
			srcEnd, ok2 := parsePDFHexToken(toks[i+1])
			if !ok1 || !ok2 || len(srcStart) == 0 || len(srcStart) != len(srcEnd) || len(srcStart) > 4 {
				i++
				continue
			}
			startVal := bytesToBigUint(srcStart)
			endVal := bytesToBigUint(srcEnd)
			if endVal < startVal {
				i += 3
				continue
			}
			span := endVal - startVal + 1
			if span > maxPDFCMapRangeEntries {
				span = maxPDFCMapRangeEntries
			}
			third := toks[i+2]
			if third == "[" {
				j := i + 3
				var offset uint64
				for j < len(toks) && toks[j] != "]" {
					if offset < span {
						if dst, okD := parsePDFHexToken(toks[j]); okD {
							if !cm.add(bigUintToBytes(startVal+offset, len(srcStart)), decodePDFUTF16BE(dst)) {
								break
							}
						}
					}
					offset++
					j++
				}
				if j < len(toks) && toks[j] == "]" {
					j++
				}
				i = j
				continue
			}
			if dstBase, okD := parsePDFHexToken(third); okD && len(dstBase) >= 2 && len(dstBase) <= 4 {
				baseVal := bytesToBigUint(dstBase)
				for offset := uint64(0); offset < span; offset++ {
					if !cm.add(
						bigUintToBytes(startVal+offset, len(srcStart)),
						decodePDFUTF16BE(bigUintToBytes(baseVal+offset, len(dstBase))),
					) {
						break
					}
				}
			}
			i += 3
			if len(cm.byHex) >= maxPDFCMapTotalEntries {
				break
			}
		}
	}

	if len(cm.byHex) == 0 {
		return nil
	}
	cm.finalize()
	return cm
}

func scanHexTokens(s string) []string {
	var out []string
	for i := 0; i < len(s); i++ {
		if s[i] == '<' && (i+1 >= len(s) || s[i+1] != '<') {
			end := strings.IndexByte(s[i+1:], '>')
			if end < 0 {
				break
			}
			out = append(out, s[i:i+1+end+1])
			i += end + 1
		}
	}
	return out
}

func scanCMapRangeTokens(s string) []string {
	var out []string
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '<':
			if i+1 < len(s) && s[i+1] == '<' {
				i++
				continue
			}
			end := strings.IndexByte(s[i+1:], '>')
			if end < 0 {
				return out
			}
			out = append(out, s[i:i+1+end+1])
			i += end + 1
		case '[':
			out = append(out, "[")
		case ']':
			out = append(out, "]")
		}
	}
	return out
}

func mergePDFCMaps(cmaps []*pdfCMap) *pdfCMap {
	if len(cmaps) == 0 {
		return nil
	}
	merged := &pdfCMap{byHex: make(map[string]string)}
	for _, cm := range cmaps {
		if cm == nil {
			continue
		}
		for k, v := range cm.byHex {
			if len(merged.byHex) >= maxPDFCMapTotalEntries {
				break
			}
			if _, exists := merged.byHex[k]; !exists {
				merged.byHex[k] = v
			}
		}
		if len(merged.byHex) >= maxPDFCMapTotalEntries {
			break
		}
	}
	if len(merged.byHex) == 0 {
		return nil
	}
	merged.finalize()
	return merged
}

// pdfObjectStream holds a parsed stream dictionary and raw stream payload from an indirect object.
type pdfObjectStream struct {
	objNum int
	dict   []byte
	raw    []byte
}

func isPDFWhitespace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\r' || b == '\n' || b == '\f' || b == 0
}

func isPDFDelimiter(b byte) bool {
	switch b {
	case '(', ')', '<', '>', '[', ']', '{', '}', '/', '%':
		return true
	}
	return false
}

// extractPDFStreams locates indirect object dictionaries and stream bodies safely without regex backtracking.
func extractPDFStreams(raw []byte) (streams []pdfObjectStream, fontToUnicode map[string]int, fontObjToUnicode map[int]int) {
	fontToUnicode = make(map[string]int)
	fontObjToUnicode = make(map[int]int)

	// Pass 1: find /ToUnicode N M R references inside font dicts and font resource maps (/F1 N M R).
	n := len(raw)
	pos := 0
	for pos < n && len(streams) < maxPDFParsedStreams {
		idx := bytes.Index(raw[pos:], []byte("stream"))
		if idx < 0 {
			break
		}
		sPos := pos + idx
		// Ensure "stream" is a standalone keyword (not "endstream" or part of a name).
		if sPos > 0 && !isPDFWhitespace(raw[sPos-1]) && raw[sPos-1] != '>' {
			pos = sPos + 6
			continue
		}
		after := sPos + 6
		if after >= n {
			break
		}
		// Per PDF spec, stream keyword is followed by CRLF or LF (some generators use CR).
		if raw[after] == '\r' {
			after++
			if after < n && raw[after] == '\n' {
				after++
			}
		} else if raw[after] == '\n' {
			after++
		} else {
			pos = sPos + 6
			continue
		}

		// Extract preceding dictionary (bounded lookback 8 KiB).
		dictStart := sPos - 8192
		if dictStart < 0 {
			dictStart = 0
		}
		prefix := raw[dictStart:sPos]
		var dict []byte
		if dOpen := bytes.LastIndex(prefix, []byte("<<")); dOpen >= 0 {
			dict = prefix[dOpen:]
		}
		objNum := parsePrecedingObjNum(prefix)

		// Honor explicit positive /Length if valid and followed near-immediately by endstream.
		streamEnd := -1
		if declaredLen, okLen := parseDirectDictLength(dict); okLen && declaredLen >= 0 && declaredLen <= n-after {
			tailStart := after + declaredLen
			tailLimit := tailStart + 16
			if tailLimit > n {
				tailLimit = n
			}
			if relEnd := bytes.Index(raw[tailStart:tailLimit], []byte("endstream")); relEnd >= 0 {
				streamEnd = after + declaredLen
				pos = tailStart + relEnd + len("endstream")
			}
		}
		if streamEnd < 0 {
			relEnd := bytes.Index(raw[after:], []byte("endstream"))
			if relEnd < 0 {
				break
			}
			streamEnd = after + relEnd
			pos = streamEnd + len("endstream")
		}
		body := raw[after:streamEnd]
		body = bytes.TrimRight(body, "\r\n")
		streams = append(streams, pdfObjectStream{
			objNum: objNum,
			dict:   dict,
			raw:    body,
		})
	}

	// Scan indirect object headers for /ToUnicode and /Font mappings to improve CMap accuracy.
	parseFontMappings(raw, fontToUnicode, fontObjToUnicode)
	return streams, fontToUnicode, fontObjToUnicode
}

func parsePrecedingObjNum(prefix []byte) int {
	// Look for the last "<num> <gen> obj" inside prefix.
	idx := bytes.LastIndex(prefix, []byte(" obj"))
	if idx < 0 {
		return 0
	}
	i := idx - 1
	for i >= 0 && isPDFWhitespace(prefix[i]) {
		i--
	}
	// Skip generation number digits.
	genEnd := i + 1
	for i >= 0 && prefix[i] >= '0' && prefix[i] <= '9' {
		i--
	}
	if i+1 == genEnd {
		return 0
	}
	for i >= 0 && isPDFWhitespace(prefix[i]) {
		i--
	}
	numEnd := i + 1
	for i >= 0 && prefix[i] >= '0' && prefix[i] <= '9' {
		i--
	}
	if i+1 == numEnd {
		return 0
	}
	num, err := strconv.Atoi(string(prefix[i+1 : numEnd]))
	if err != nil || num <= 0 {
		return 0
	}
	return num
}

func parseDirectDictLength(dict []byte) (int, bool) {
	idx := bytes.Index(dict, []byte("/Length"))
	if idx < 0 {
		return 0, false
	}
	p := idx + len("/Length")
	for p < len(dict) && isPDFWhitespace(dict[p]) {
		p++
	}
	start := p
	for p < len(dict) && dict[p] >= '0' && dict[p] <= '9' {
		p++
	}
	if p == start {
		return 0, false
	}
	// Make sure it is NOT an indirect reference ("/Length 12 0 R").
	q := p
	for q < len(dict) && isPDFWhitespace(dict[q]) {
		q++
	}
	if q < len(dict) && dict[q] >= '0' && dict[q] <= '9' {
		for q < len(dict) && dict[q] >= '0' && dict[q] <= '9' {
			q++
		}
		for q < len(dict) && isPDFWhitespace(dict[q]) {
			q++
		}
		if q < len(dict) && dict[q] == 'R' {
			return 0, false
		}
	}
	v, err := strconv.Atoi(string(dict[start:p]))
	if err != nil || v < 0 {
		return 0, false
	}
	return v, true
}

func parseIndirectRef(b []byte) (objNum int, ok bool) {
	p := 0
	for p < len(b) && isPDFWhitespace(b[p]) {
		p++
	}
	s1 := p
	for p < len(b) && b[p] >= '0' && b[p] <= '9' {
		p++
	}
	if p == s1 {
		return 0, false
	}
	n1, err := strconv.Atoi(string(b[s1:p]))
	if err != nil || n1 <= 0 {
		return 0, false
	}
	for p < len(b) && isPDFWhitespace(b[p]) {
		p++
	}
	s2 := p
	for p < len(b) && b[p] >= '0' && b[p] <= '9' {
		p++
	}
	if p == s2 {
		return 0, false
	}
	for p < len(b) && isPDFWhitespace(b[p]) {
		p++
	}
	if p < len(b) && b[p] == 'R' {
		return n1, true
	}
	return 0, false
}

func parseFontMappings(raw []byte, fontNameToObj map[string]int, fontObjToUnicode map[int]int) {
	// Find /ToUnicode <N> 0 R occurrences and tie to enclosing obj num.
	pos := 0
	for pos < len(raw) {
		idx := bytes.Index(raw[pos:], []byte("/ToUnicode"))
		if idx < 0 {
			break
		}
		at := pos + idx
		tailStart := at + len("/ToUnicode")
		tailEnd := tailStart + 32
		if tailEnd > len(raw) {
			tailEnd = len(raw)
		}
		if tuObj, ok := parseIndirectRef(raw[tailStart:tailEnd]); ok {
			lookStart := at - 2048
			if lookStart < 0 {
				lookStart = 0
			}
			if owner := parsePrecedingObjNum(raw[lookStart:at]); owner > 0 {
				fontObjToUnicode[owner] = tuObj
			}
		}
		pos = tailStart
	}

	// Find /Font << /F1 5 0 R ... >> blocks.
	pos = 0
	for pos < len(raw) {
		idx := bytes.Index(raw[pos:], []byte("/Font"))
		if idx < 0 {
			break
		}
		at := pos + idx + len("/Font")
		for at < len(raw) && isPDFWhitespace(raw[at]) {
			at++
		}
		if at+2 <= len(raw) && raw[at] == '<' && raw[at+1] == '<' {
			endLim := at + 2048
			if endLim > len(raw) {
				endLim = len(raw)
			}
			if closeIdx := bytes.Index(raw[at+2:endLim], []byte(">>")); closeIdx >= 0 {
				block := raw[at+2 : at+2+closeIdx]
				parseFontDictEntries(block, fontNameToObj)
			}
		}
		pos = at
	}
}

func parseFontDictEntries(block []byte, out map[string]int) {
	i := 0
	for i < len(block) {
		if block[i] != '/' {
			i++
			continue
		}
		nameStart := i + 1
		j := nameStart
		for j < len(block) && !isPDFWhitespace(block[j]) && !isPDFDelimiter(block[j]) {
			j++
		}
		name := string(block[nameStart:j])
		if name != "" {
			end := j + 32
			if end > len(block) {
				end = len(block)
			}
			if refObj, ok := parseIndirectRef(block[j:end]); ok {
				out[name] = refObj
			}
		}
		i = j
	}
}

func shouldSkipPDFStreamDict(dict []byte) bool {
	if len(dict) == 0 {
		return false
	}
	if bytes.Contains(dict, []byte("/Subtype /Image")) || bytes.Contains(dict, []byte("/Subtype/Image")) {
		return true
	}
	if bytes.Contains(dict, []byte("/Type /XRef")) || bytes.Contains(dict, []byte("/Type/XRef")) {
		return true
	}
	if bytes.Contains(dict, []byte("/Type /ObjStm")) || bytes.Contains(dict, []byte("/Type/ObjStm")) {
		return true
	}
	if bytes.Contains(dict, []byte("/Subtype /Type1C")) || bytes.Contains(dict, []byte("/Subtype/Type1C")) ||
		bytes.Contains(dict, []byte("/Subtype /CIDFontType0C")) || bytes.Contains(dict, []byte("/Subtype/CIDFontType0C")) {
		return true
	}
	return false
}

func decodePDFStreamData(dict, raw []byte, remainingTotal int64) ([]byte, int64, bool) {
	if len(raw) == 0 || remainingTotal <= 0 {
		return nil, 0, false
	}
	hasFilter := bytes.Contains(dict, []byte("/Filter"))
	if !hasFilter {
		// Uncompressed stream; cap to remainingTotal & stream cap.
		limit := int64(maxPDFStreamDecompressedBytes)
		if remainingTotal < limit {
			limit = remainingTotal
		}
		if int64(len(raw)) > limit {
			return raw[:limit], limit, true
		}
		return raw, int64(len(raw)), true
	}

	// Support /FlateDecode or /Fl; skip image/unsupported filters cleanly.
	isFlate := bytes.Contains(dict, []byte("/FlateDecode")) || bytes.Contains(dict, []byte("/Fl ")) || bytes.Contains(dict, []byte("/Fl]")) || bytes.Contains(dict, []byte("/Fl\n")) || bytes.Contains(dict, []byte("/Fl\r"))
	if !isFlate {
		return nil, 0, false
	}
	if bytes.Contains(dict, []byte("/DCTDecode")) || bytes.Contains(dict, []byte("/JPXDecode")) || bytes.Contains(dict, []byte("/CCITTFaxDecode")) || bytes.Contains(dict, []byte("/JBIG2Decode")) {
		return nil, 0, false
	}

	capBytes := int64(maxPDFStreamDecompressedBytes)
	if remainingTotal < capBytes {
		capBytes = remainingTotal
	}
	zr, err := zlib.NewReader(bytes.NewReader(raw))
	if err != nil {
		return nil, 0, false
	}
	defer zr.Close()
	out, readErr := io.ReadAll(io.LimitReader(zr, capBytes+1))
	if len(out) == 0 && readErr != nil {
		return nil, 0, false
	}
	if int64(len(out)) > capBytes {
		out = out[:capBytes]
	}
	return out, int64(len(out)), true
}

// WinAnsiEncoding table for 0x80..0x9F (CP1252).
var winAnsi80To9F = [32]rune{
	0x20AC, 0x0081, 0x201A, 0x0192, 0x201E, 0x2026, 0x2020, 0x2021,
	0x02C6, 0x2030, 0x0160, 0x2039, 0x0152, 0x008D, 0x017D, 0x008F,
	0x0090, 0x2018, 0x2019, 0x201C, 0x201D, 0x2022, 0x2013, 0x2014,
	0x02DC, 0x2122, 0x0161, 0x203A, 0x0153, 0x009D, 0x017E, 0x0178,
}

func decodePDFRawStringBytes(raw []byte, isHex bool, activeCMap, fallbackCMap *pdfCMap) string {
	if len(raw) == 0 {
		return ""
	}
	// UTF-16BE BOM (FE FF)
	if len(raw) >= 2 && raw[0] == 0xFE && raw[1] == 0xFF {
		return decodePDFUTF16BE(raw[2:])
	}
	// Try active font's ToUnicode CMap first, then merged fallback CMap.
	if activeCMap != nil {
		if s, ok := activeCMap.decode(raw); ok {
			return s
		}
	}
	if fallbackCMap != nil && (isHex || hasNonASCIIOrControl(raw)) {
		if s, ok := fallbackCMap.decode(raw); ok {
			return s
		}
	}
	// Fallback: if hex string looks like UTF-16BE without BOM (even length, alternating 0x00 + ASCII).
	if isHex && len(raw) >= 2 && len(raw)%2 == 0 && looksLikeUTF16BE(raw) {
		return decodePDFUTF16BE(raw)
	}
	if utf8.Valid(raw) && !bytes.ContainsRune(raw, 0) {
		return string(raw)
	}
	// WinAnsi / Latin-1 fallback.
	var sb strings.Builder
	sb.Grow(len(raw))
	for _, b := range raw {
		switch {
		case b >= 0x80 && b <= 0x9F:
			sb.WriteRune(winAnsi80To9F[b-0x80])
		case b >= 0x20 || b == '\t' || b == '\n' || b == '\r':
			sb.WriteRune(rune(b))
		}
	}
	return sb.String()
}

func hasNonASCIIOrControl(b []byte) bool {
	for _, x := range b {
		if x < 0x20 || x > 0x7e {
			return true
		}
	}
	return false
}

func looksLikeUTF16BE(b []byte) bool {
	if len(b) < 4 || len(b)%2 != 0 {
		return false
	}
	zeroHi := 0
	pairs := len(b) / 2
	for i := 0; i < len(b); i += 2 {
		if b[i] == 0 && b[i+1] >= 0x20 && b[i+1] <= 0x7e {
			zeroHi++
		}
	}
	return zeroHi*2 >= pairs
}

type pdfTokenKind int

const (
	pdfTokWord pdfTokenKind = iota
	pdfTokName
	pdfTokNumber
	pdfTokString
	pdfTokHexString
	pdfTokArray
)

type pdfToken struct {
	kind  pdfTokenKind
	text  string
	bytes []byte
	num   float64
	arr   []pdfToken
}

func parsePDFLiteralString(data []byte, start int) ([]byte, int) {
	// start points at '('
	var out []byte
	depth := 1
	i := start + 1
	for i < len(data) && depth > 0 {
		c := data[i]
		if c == '\\' {
			i++
			if i >= len(data) {
				break
			}
			esc := data[i]
			switch esc {
			case 'n':
				out = append(out, '\n')
				i++
			case 'r':
				out = append(out, '\r')
				i++
			case 't':
				out = append(out, '\t')
				i++
			case 'b':
				out = append(out, '\b')
				i++
			case 'f':
				out = append(out, '\f')
				i++
			case '(', ')', '\\':
				out = append(out, esc)
				i++
			case '\r':
				i++
				if i < len(data) && data[i] == '\n' {
					i++
				}
			case '\n':
				i++
			default:
				if esc >= '0' && esc <= '7' {
					val := int(esc - '0')
					i++
					for k := 0; k < 2 && i < len(data) && data[i] >= '0' && data[i] <= '7'; k++ {
						val = val*8 + int(data[i]-'0')
						i++
					}
					out = append(out, byte(val&0xff))
				} else {
					out = append(out, esc)
					i++
				}
			}
			continue
		}
		if c == '(' {
			depth++
			out = append(out, c)
			i++
			continue
		}
		if c == ')' {
			depth--
			if depth > 0 {
				out = append(out, c)
			}
			i++
			continue
		}
		out = append(out, c)
		i++
	}
	return out, i
}

func parsePDFHexStringBytes(data []byte, start int) ([]byte, int) {
	// start points at '<'
	i := start + 1
	var hexBuf []byte
	for i < len(data) && data[i] != '>' {
		c := data[i]
		if (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F') {
			hexBuf = append(hexBuf, c)
		}
		i++
	}
	if i < len(data) && data[i] == '>' {
		i++
	}
	if len(hexBuf)%2 == 1 {
		hexBuf = append(hexBuf, '0')
	}
	out := make([]byte, len(hexBuf)/2)
	_, _ = hex.Decode(out, hexBuf)
	return out, i
}

func tokenizePDFContent(data []byte) []pdfToken {
	var toks []pdfToken
	i := 0
	n := len(data)
	totalToks := 0
	var readToken func(depth int) (pdfToken, bool)
	readToken = func(depth int) (pdfToken, bool) {
		if totalToks >= maxPDFContentTokens {
			return pdfToken{}, false
		}
		for i < n {
			c := data[i]
			if isPDFWhitespace(c) {
				i++
				continue
			}
			if c == '%' {
				for i < n && data[i] != '\n' && data[i] != '\r' {
					i++
				}
				continue
			}
			break
		}
		if i >= n {
			return pdfToken{}, false
		}
		totalToks++
		c := data[i]
		switch {
		case c == '(':
			b, next := parsePDFLiteralString(data, i)
			i = next
			return pdfToken{kind: pdfTokString, bytes: b}, true
		case c == ')' || c == '{' || c == '}':
			i++
			return pdfToken{kind: pdfTokWord, text: string(c)}, true
		case c == '<':
			if i+1 < n && data[i+1] == '<' {
				i += 2
				return pdfToken{kind: pdfTokWord, text: "<<"}, true
			}
			b, next := parsePDFHexStringBytes(data, i)
			i = next
			return pdfToken{kind: pdfTokHexString, bytes: b}, true
		case c == '>':
			if i+1 < n && data[i+1] == '>' {
				i += 2
				return pdfToken{kind: pdfTokWord, text: ">>"}, true
			}
			i++
			return pdfToken{kind: pdfTokWord, text: ">"}, true
		case c == '[':
			i++
			if depth >= maxPDFArrayDepth {
				return pdfToken{kind: pdfTokWord, text: "["}, true
			}
			var arr []pdfToken
			for i < n && totalToks < maxPDFContentTokens {
				for i < n && isPDFWhitespace(data[i]) {
					i++
				}
				if i >= n {
					break
				}
				if data[i] == ']' {
					i++
					break
				}
				if data[i] == '%' {
					for i < n && data[i] != '\n' && data[i] != '\r' {
						i++
					}
					continue
				}
				sub, ok := readToken(depth + 1)
				if !ok {
					break
				}
				arr = append(arr, sub)
			}
			return pdfToken{kind: pdfTokArray, arr: arr}, true
		case c == ']':
			i++
			return pdfToken{kind: pdfTokWord, text: "]"}, true
		case c == '/':
			i++
			start := i
			for i < n && !isPDFWhitespace(data[i]) && !isPDFDelimiter(data[i]) {
				i++
			}
			return pdfToken{kind: pdfTokName, text: string(data[start:i])}, true
		case c == '\'' || c == '"':
			i++
			return pdfToken{kind: pdfTokWord, text: string(c)}, true
		default:
			start := i
			for i < n && !isPDFWhitespace(data[i]) && !isPDFDelimiter(data[i]) {
				i++
			}
			if i == start {
				i++
				return pdfToken{kind: pdfTokWord, text: string(c)}, true
			}
			w := string(data[start:i])
			if f, err := strconv.ParseFloat(w, 64); err == nil {
				return pdfToken{kind: pdfTokNumber, text: w, num: f}, true
			}
			return pdfToken{kind: pdfTokWord, text: w}, true
		}
	}

	for totalToks < maxPDFContentTokens {
		tok, ok := readToken(0)
		if !ok {
			break
		}
		toks = append(toks, tok)
	}
	return toks
}

func extractTextFromContentStream(content []byte, fontCMapByName map[string]*pdfCMap, fallbackCMap *pdfCMap, sb *strings.Builder, runeCount *int) {
	if !bytes.Contains(content, []byte("BT")) {
		return
	}
	toks := tokenizePDFContent(content)
	inText := false
	var activeCMap *pdfCMap
	var stack []pdfToken
	lastLineEndedWithSpace := true

	appendStr := func(s string) {
		if s == "" || *runeCount >= maxPDFExtractedRunes {
			return
		}
		for _, r := range s {
			if *runeCount >= maxPDFExtractedRunes {
				return
			}
			if r == 0 {
				continue
			}
			if r == '\r' {
				continue
			}
			sb.WriteRune(r)
			*runeCount++
			lastLineEndedWithSpace = unicode.IsSpace(r)
		}
	}

	emitNewline := func() {
		if sb.Len() > 0 && *runeCount < maxPDFExtractedRunes {
			s := sb.String()
			if len(s) > 0 && s[len(s)-1] != '\n' {
				sb.WriteByte('\n')
				*runeCount++
				lastLineEndedWithSpace = true
			}
		}
	}

	emitSpace := func() {
		if !lastLineEndedWithSpace && sb.Len() > 0 && *runeCount < maxPDFExtractedRunes {
			sb.WriteByte(' ')
			*runeCount++
			lastLineEndedWithSpace = true
		}
	}

	showStringTok := func(tok pdfToken) {
		switch tok.kind {
		case pdfTokString:
			appendStr(decodePDFRawStringBytes(tok.bytes, false, activeCMap, fallbackCMap))
		case pdfTokHexString:
			appendStr(decodePDFRawStringBytes(tok.bytes, true, activeCMap, fallbackCMap))
		}
	}

	var lastTmY float64
	hasTmY := false

	for _, tok := range toks {
		if *runeCount >= maxPDFExtractedRunes {
			return
		}
		if tok.kind != pdfTokWord {
			stack = append(stack, tok)
			if len(stack) > 64 {
				stack = stack[len(stack)-64:]
			}
			continue
		}
		switch tok.text {
		case "BT":
			inText = true
			hasTmY = false
			stack = stack[:0]
		case "ET":
			if inText {
				emitNewline()
			}
			inText = false
			stack = stack[:0]
		case "Tf":
			if len(stack) >= 2 && stack[len(stack)-2].kind == pdfTokName {
				fontName := stack[len(stack)-2].text
				if cm, ok := fontCMapByName[fontName]; ok {
					activeCMap = cm
				} else {
					activeCMap = nil
				}
			}
			stack = stack[:0]
		case "Tj":
			if inText && len(stack) >= 1 {
				showStringTok(stack[len(stack)-1])
			}
			stack = stack[:0]
		case "TJ":
			if inText && len(stack) >= 1 && stack[len(stack)-1].kind == pdfTokArray {
				for _, elem := range stack[len(stack)-1].arr {
					switch elem.kind {
					case pdfTokString, pdfTokHexString:
						showStringTok(elem)
					case pdfTokNumber:
						if elem.num <= -120 {
							emitSpace()
						}
					}
				}
			}
			stack = stack[:0]
		case "'":
			if inText && len(stack) >= 1 {
				emitNewline()
				showStringTok(stack[len(stack)-1])
			}
			stack = stack[:0]
		case "\"":
			if inText && len(stack) >= 1 {
				emitNewline()
				showStringTok(stack[len(stack)-1])
			}
			stack = stack[:0]
		case "T*":
			if inText {
				emitNewline()
			}
			stack = stack[:0]
		case "Td", "TD":
			if inText && len(stack) >= 2 {
				dy := stack[len(stack)-1].num
				dx := stack[len(stack)-2].num
				if dy != 0 {
					emitNewline()
				} else if dx > 0 {
					emitSpace()
				}
			}
			stack = stack[:0]
		case "Tm":
			if inText && len(stack) >= 6 {
				y := stack[len(stack)-1].num
				if hasTmY && (y-lastTmY > 2 || lastTmY-y > 2) {
					emitNewline()
				} else if hasTmY {
					emitSpace()
				}
				lastTmY = y
				hasTmY = true
			}
			stack = stack[:0]
		default:
			stack = stack[:0]
		}
	}
}

func countNonWhitespaceRunes(s string) int {
	n := 0
	for _, r := range s {
		if !unicode.IsSpace(r) && unicode.IsPrint(r) {
			n++
		}
	}
	return n
}

func cleanExtractedPDFText(s string) string {
	lines := strings.Split(s, "\n")
	out := make([]string, 0, len(lines))
	blankStreak := 0
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			blankStreak++
			if blankStreak <= 1 && len(out) > 0 {
				out = append(out, "")
			}
			continue
		}
		blankStreak = 0
		out = append(out, trimmed)
	}
	return strings.TrimSpace(strings.Join(out, "\n"))
}

// ExtractPDFText extracts plain text from a PDF document using pure-Go stdlib parsing
// (FlateDecode with strict decompression-bomb caps, BT..ET Tj/TJ/'/" operators, UTF-16BE BOM,
// and /ToUnicode CMap bfchar/bfrange decoding). Returns extracted text and count of meaningful
// non-whitespace printable runes.
func ExtractPDFText(raw []byte) (string, int, error) {
	if len(raw) < 5 || !bytes.HasPrefix(raw, []byte("%PDF-")) {
		return "", 0, errors.New("invalid PDF header (missing %PDF- signature)")
	}
	streams, fontNameToObj, fontObjToUnicode := extractPDFStreams(raw)
	if len(streams) == 0 {
		return "", 0, nil
	}

	remainingTotal := int64(maxPDFTotalDecompressedBytes)
	type decodedStream struct {
		objNum int
		data   []byte
	}
	var contentStreams []decodedStream
	cmapsByObj := make(map[int]*pdfCMap)
	var allCMaps []*pdfCMap

	for _, st := range streams {
		if remainingTotal <= 0 {
			break
		}
		if shouldSkipPDFStreamDict(st.dict) {
			continue
		}
		decoded, used, ok := decodePDFStreamData(st.dict, st.raw, remainingTotal)
		if !ok || len(decoded) == 0 {
			continue
		}
		remainingTotal -= used

		if cm := parseCMapStream(decoded); cm != nil {
			if st.objNum > 0 {
				cmapsByObj[st.objNum] = cm
			}
			allCMaps = append(allCMaps, cm)
			continue
		}
		if bytes.Contains(decoded, []byte("BT")) {
			contentStreams = append(contentStreams, decodedStream{objNum: st.objNum, data: decoded})
		}
	}

	fontCMapByName := make(map[string]*pdfCMap)
	for fontName, fontObj := range fontNameToObj {
		if tuObj, ok := fontObjToUnicode[fontObj]; ok {
			if cm, okC := cmapsByObj[tuObj]; okC {
				fontCMapByName[fontName] = cm
			}
		}
	}
	fallbackCMap := mergePDFCMaps(allCMaps)

	var sb strings.Builder
	runeCount := 0
	for _, cs := range contentStreams {
		if runeCount >= maxPDFExtractedRunes {
			break
		}
		extractTextFromContentStream(cs.data, fontCMapByName, fallbackCMap, &sb, &runeCount)
	}

	cleaned := cleanExtractedPDFText(sb.String())
	return cleaned, countNonWhitespaceRunes(cleaned), nil
}
