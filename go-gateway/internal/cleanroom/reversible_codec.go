package cleanroom

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"

	gwcfg "agy-acp-gateway-go/internal/config"
)

const (
	reversibleSegPrefix = "wsseg_"
	reversibleTagPrefix = "wstag_"
	reversibleTokenLen  = 14 // 6-byte prefix + 8 lowercase hex chars
	reversibleMaxHold   = reversibleTokenLen - 1
)

// ToolSeedItem supplies tool schema text to pre-seed a ReversibleSessionCodec.
type ToolSeedItem struct {
	Description string
	Parameters  map[string]any
}

var (
	vendorWrapperTagRe = regexp.MustCompile(
		`<(/?)\s*((?i:` +
			`claude_[a-zA-Z0-9_-]*` +
			`|dsh_[a-zA-Z0-9_-]*` +
			`|deepseek_[a-zA-Z0-9_-]*` +
			`|codex_[a-zA-Z0-9_-]*` +
			`|cursor_[a-zA-Z0-9_-]*` +
			`|windsurf_[a-zA-Z0-9_-]*` +
			`|cline_[a-zA-Z0-9_-]*` +
			`|roo_[a-zA-Z0-9_-]*` +
			`|kilo_[a-zA-Z0-9_-]*` +
			`|opencode_[a-zA-Z0-9_-]*` +
			`|openclaw_[a-zA-Z0-9_-]*` +
			`|clawdbot_[a-zA-Z0-9_-]*` +
			`|augment_[a-zA-Z0-9_-]*` +
			`|anthropic_[a-zA-Z0-9_-]*` +
			`|t3_code_[a-zA-Z0-9_-]*` +
			`|environment_details` +
			`))\b([^>]*)>`,
	)

	sensitivePathPhraseRe = regexp.MustCompile(
		`(?i)DeepSeek[\s_-]+Harness` +
			`|Claude[\s_-]+Code` +
			`|Claude[\s_-]+Agent[\s_-]+SDK` +
			`|OpenAI[\s_-]+Codex` +
			`|Codex[\s_-]+CLI` +
			`|Anthropic` +
			`|OpenClaw` +
			`|ClawdBot` +
			`|Roo[\s_-]+Code` +
			`|Kilo[\s_-]+Code` +
			`|Augment[\s_-]+Code` +
			`|PI-Desktop` +
			`|@[a-z0-9._-]+/(?:[a-z0-9._-]*coding[_-]agent[a-z0-9._-]*|pi-[a-z0-9._-]+)` +
			`|\b(?:[a-z0-9][a-z0-9._-]*[_-])?coding[_-]agent(?:[_-][a-z0-9._-]+)?\b`,
	)

	toolVendorTokenReplacements = []textReplacement{
		{regexp.MustCompile(`(?i)deepseek[_-]+harness`), "ws_harness"},
		{regexp.MustCompile(`(?i)DeepSeekHarness`), "WsHarness"},
		{regexp.MustCompile(`(?i)claude[_-]+code`), "ws_agent"},
		{regexp.MustCompile(`(?i)ClaudeCode`), "WsAgent"},
		{regexp.MustCompile(`(?i)codex[_-]+cli`), "ws_cli"},
		{regexp.MustCompile(`(?i)open[_-]?claw`), "ws_claw"},
		{regexp.MustCompile(`(?i)clawd[_-]?bot`), "ws_bot"},
		{regexp.MustCompile(`(?i)roo[_-]+code`), "ws_roo"},
		{regexp.MustCompile(`(?i)kilo[_-]+code`), "ws_kilo"},
		{regexp.MustCompile(`(?i)augment[_-]+code`), "ws_augment"},
		{regexp.MustCompile(`(?i)pi[_-]+coding[_-]+agent`), "ws_coding_agent"},
		{regexp.MustCompile(`(?i)pi[_-]+desktop`), "ws_desktop"},
		{regexp.MustCompile(`(?i)^(__+)[a-z0-9-]+([_-]+deferred[_-]+placeholder__*)$`), "${1}ws${2}"},
		{regexp.MustCompile(`(?i)^dsh([_-])`), "ws${1}"},
		{regexp.MustCompile(`(?i)([_-])dsh([_-])`), "${1}ws${2}"},
		{regexp.MustCompile(`(?i)([_-])dsh$`), "${1}ws"},
		{regexp.MustCompile(`(?i)^claude([_-])`), "ws${1}"},
		{regexp.MustCompile(`(?i)([_-])claude([_-])`), "${1}ws${2}"},
		{regexp.MustCompile(`(?i)^anthropic([_-])`), "provider${1}"},
		{regexp.MustCompile(`(?i)([_-])anthropic([_-])`), "${1}provider${2}"},
		{regexp.MustCompile(`(?i)^codex([_-])`), "ws${1}"},
		{regexp.MustCompile(`(?i)([_-])codex([_-])`), "${1}ws${2}"},
		{regexp.MustCompile(`(?i)^pi([_-])`), "ws${1}"},
		{regexp.MustCompile(`(?i)([_-])pi([_-])`), "${1}ws${2}"},
		{regexp.MustCompile(`(?i)([_-])pi$`), "${1}ws"},
	}
)

func hasToolVendorStem(s string) bool {
	for i := 0; i < len(s); i++ {
		switch s[i] | 0x20 {
		case 'd':
			rem := s[i:]
			if hasPrefixFoldASCII(rem, "dsh") || hasPrefixFoldASCII(rem, "deepseek") || hasPrefixFoldASCII(rem, "deferred") {
				return true
			}
		case 'c':
			rem := s[i:]
			if hasPrefixFoldASCII(rem, "claude") || hasPrefixFoldASCII(rem, "codex") || hasPrefixFoldASCII(rem, "claw") {
				return true
			}
		case 'a':
			rem := s[i:]
			if hasPrefixFoldASCII(rem, "anthropic") || hasPrefixFoldASCII(rem, "augment") {
				return true
			}
		case 'o':
			if hasPrefixFoldASCII(s[i:], "open") {
				return true
			}
		case 'r':
			if hasPrefixFoldASCII(s[i:], "roo") {
				return true
			}
		case 'k':
			if hasPrefixFoldASCII(s[i:], "kilo") {
				return true
			}
		case 'p':
			rem := s[i:]
			if hasPrefixFoldASCII(rem, "pi_") || hasPrefixFoldASCII(rem, "pi-") ||
				(len(rem) == 2 && i > 0 && (s[i-1] == '_' || s[i-1] == '-') && hasPrefixFoldASCII(rem, "pi")) {
				return true
			}
		}
	}
	return false
}

// NeutralizeToolIdentifier rewrites third-party agent/vendor tokens inside client tool identifiers
// while keeping internal bridge and standard tool names intact.
func NeutralizeToolIdentifier(name string) string {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" || gwcfg.IsInternalBridgeTool(trimmed) || !hasToolVendorStem(trimmed) {
		return trimmed
	}
	out := trimmed
	for _, item := range toolVendorTokenReplacements {
		out = item.re.ReplaceAllString(out, item.repl)
	}
	return out
}

const maxSeededToolFps = 64

// ReversibleSessionCodec maintains a deterministic per-session bidirectional mapping between
// sensitive client path segments / vendor XML wrapper tags and fixed 14-byte neutral tokens
// (wsseg_<8hex>, wstag_<8hex>).
type ReversibleSessionCodec struct {
	mu            sync.RWMutex
	key           []byte
	origToAlias   map[string]string
	aliasToOrig   map[string]string
	seededToolFps map[string]struct{}
	empty         atomic.Bool
	historySeeded atomic.Bool
}

// NewReversibleSessionCodec creates an isolated reversible token codec keyed by tenantKey.
func NewReversibleSessionCodec(tenantKey string) *ReversibleSessionCodec {
	if tenantKey == "" {
		tenantKey = "default-session"
	}
	c := &ReversibleSessionCodec{
		key:         []byte("agy-reversible-v1:" + tenantKey),
		origToAlias: make(map[string]string),
		aliasToOrig: make(map[string]string),
	}
	c.empty.Store(true)
	return c
}

// SetTenantKey updates the HMAC key seed for subsequent alias generation.
// When the key actually changes, memoized history/tool seeding flags reset so aliases re-seed cleanly.
func (c *ReversibleSessionCodec) SetTenantKey(tenantKey string) {
	if c == nil || tenantKey == "" {
		return
	}
	newKey := "agy-reversible-v1:" + tenantKey
	c.mu.Lock()
	if string(c.key) != newKey {
		c.key = []byte(newKey)
		c.historySeeded.Store(false)
		c.seededToolFps = nil
	}
	c.mu.Unlock()
}

// IsHistorySeeded reports whether conversation history has already been seeded for this codec.
func (c *ReversibleSessionCodec) IsHistorySeeded() bool {
	if c == nil {
		return true
	}
	return c.historySeeded.Load()
}

// IsEmpty reports whether any reversible alias entries have been registered yet.
func (c *ReversibleSessionCodec) IsEmpty() bool {
	if c == nil {
		return true
	}
	return c.empty.Load()
}

// Count returns the number of registered reversible aliases.
func (c *ReversibleSessionCodec) Count() int {
	if c == nil || c.empty.Load() {
		return 0
	}
	c.mu.RLock()
	n := len(c.aliasToOrig)
	c.mu.RUnlock()
	return n
}

func (c *ReversibleSessionCodec) getOrCreateAlias(prefix, orig string) string {
	if orig == "" {
		return ""
	}
	scopedKey := prefix + orig
	c.mu.RLock()
	if existing, ok := c.origToAlias[scopedKey]; ok {
		c.mu.RUnlock()
		return existing
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.origToAlias[scopedKey]; ok {
		return existing
	}
	for attempt := 0; ; attempt++ {
		mac := hmac.New(sha256.New, c.key)
		mac.Write([]byte(scopedKey))
		if attempt > 0 {
			mac.Write([]byte{0})
			mac.Write([]byte{byte(attempt)})
		}
		sum := mac.Sum(nil)
		cand := prefix + hex.EncodeToString(sum[:4])
		if prevOrig, taken := c.aliasToOrig[cand]; !taken || prevOrig == orig {
			c.origToAlias[scopedKey] = cand
			c.aliasToOrig[cand] = orig
			c.empty.Store(false)
			return cand
		}
	}
}

// Lookup returns the original string registered for alias, if any.
func (c *ReversibleSessionCodec) Lookup(alias string) (string, bool) {
	if c == nil || len(alias) != reversibleTokenLen || c.empty.Load() {
		return "", false
	}
	c.mu.RLock()
	orig, ok := c.aliasToOrig[alias]
	c.mu.RUnlock()
	return orig, ok
}

func hasSensitivePathPhraseStem(s string) bool {
	for i := 0; i < len(s); i++ {
		switch s[i] | 0x20 {
		case 'd':
			if hasPrefixFoldASCII(s[i:], "deepseek") {
				return true
			}
		case 'c':
			rem := s[i:]
			if hasPrefixFoldASCII(rem, "claude") || hasPrefixFoldASCII(rem, "codex") || hasPrefixFoldASCII(rem, "clawdbot") ||
				hasPrefixFoldASCII(rem, "coding-agent") || hasPrefixFoldASCII(rem, "coding_agent") {
				return true
			}
		case 'a':
			rem := s[i:]
			if hasPrefixFoldASCII(rem, "anthropic") || hasPrefixFoldASCII(rem, "augment") {
				return true
			}
		case 'o':
			if hasPrefixFoldASCII(s[i:], "openclaw") {
				return true
			}
		case 'r':
			if hasPrefixFoldASCII(s[i:], "roo") {
				return true
			}
		case 'k':
			if hasPrefixFoldASCII(s[i:], "kilo") {
				return true
			}
		case 'p':
			if hasPrefixFoldASCII(s[i:], "pi-") || hasPrefixFoldASCII(s[i:], "pi_desktop") {
				return true
			}
		}
	}
	return false
}

// EncodeOutboundText aliases vendor XML tags (wstag_<8hex>) and path phrases (wsseg_<8hex>).
func (c *ReversibleSessionCodec) EncodeOutboundText(s string) string {
	if c == nil || s == "" {
		return s
	}
	if strings.IndexByte(s, '<') >= 0 && vendorWrapperTagRe.MatchString(s) {
		s = vendorWrapperTagRe.ReplaceAllStringFunc(s, func(full string) string {
			sub := vendorWrapperTagRe.FindStringSubmatch(full)
			if len(sub) != 4 {
				return full
			}
			alias := c.getOrCreateAlias(reversibleTagPrefix, sub[2])
			return "<" + sub[1] + alias + sub[3] + ">"
		})
	}
	if hasSensitivePathPhraseStem(s) && sensitivePathPhraseRe.MatchString(s) {
		s = sensitivePathPhraseRe.ReplaceAllStringFunc(s, func(phrase string) string {
			return c.getOrCreateAlias(reversibleSegPrefix, phrase)
		})
	}
	return s
}

func (c *ReversibleSessionCodec) seedOutboundTextReadOnly(s string) {
	if c == nil || s == "" {
		return
	}
	if strings.IndexByte(s, '<') >= 0 && vendorWrapperTagRe.MatchString(s) {
		for _, sub := range vendorWrapperTagRe.FindAllStringSubmatch(s, -1) {
			if len(sub) == 4 {
				_ = c.getOrCreateAlias(reversibleTagPrefix, sub[2])
			}
		}
	}
	if hasSensitivePathPhraseStem(s) && sensitivePathPhraseRe.MatchString(s) {
		for _, phrase := range sensitivePathPhraseRe.FindAllString(s, -1) {
			_ = c.getOrCreateAlias(reversibleSegPrefix, phrase)
		}
	}
}

func (c *ReversibleSessionCodec) seedValueReadOnly(v any) {
	if c == nil || v == nil {
		return
	}
	switch x := v.(type) {
	case string:
		c.seedOutboundTextReadOnly(x)
	case []string:
		for _, elem := range x {
			c.seedOutboundTextReadOnly(elem)
		}
	case map[string]any:
		for k, val := range x {
			c.seedOutboundTextReadOnly(k)
			c.seedValueReadOnly(val)
		}
	case []any:
		for _, elem := range x {
			c.seedValueReadOnly(elem)
		}
	}
}

// EncodePromptBlocks returns a shallow copy of ACP prompt blocks with text encoded by the codec.
func (c *ReversibleSessionCodec) EncodePromptBlocks(blocks []map[string]any) []map[string]any {
	if c == nil || len(blocks) == 0 {
		return blocks
	}
	out := make([]map[string]any, len(blocks))
	for i, blk := range blocks {
		cp := make(map[string]any, len(blk))
		for k, v := range blk {
			cp[k] = v
		}
		if txt, ok := cp["text"].(string); ok && txt != "" {
			cp["text"] = c.EncodeOutboundText(txt)
		}
		if res, ok := cp["resource"].(map[string]any); ok && res != nil {
			resCp := make(map[string]any, len(res))
			for rk, rv := range res {
				resCp[rk] = rv
			}
			if rt, okT := resCp["text"].(string); okT && rt != "" {
				resCp["text"] = c.EncodeOutboundText(rt)
			}
			cp["resource"] = resCp
		}
		out[i] = cp
	}
	return out
}

// EncodeValue recursively encodes nested strings/maps/slices with reversible tokens.
func (c *ReversibleSessionCodec) EncodeValue(v any) any {
	if c == nil || v == nil {
		return v
	}
	switch x := v.(type) {
	case string:
		return c.EncodeOutboundText(x)
	case []string:
		out := make([]string, len(x))
		for i, elem := range x {
			out[i] = c.EncodeOutboundText(elem)
		}
		return out
	case map[string]any:
		return c.EncodeMap(x)
	case []any:
		out := make([]any, len(x))
		for i, elem := range x {
			out[i] = c.EncodeValue(elem)
		}
		return out
	default:
		return v
	}
}

// EncodeMap recursively encodes keys and values of a map.
func (c *ReversibleSessionCodec) EncodeMap(m map[string]any) map[string]any {
	if c == nil || m == nil {
		return m
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[c.EncodeOutboundText(k)] = c.EncodeValue(v)
	}
	return out
}

// IsToolsFPSeeded reports whether tool catalog fingerprint fp has already been seeded on this codec.
func (c *ReversibleSessionCodec) IsToolsFPSeeded(fp string) bool {
	if c == nil || fp == "" {
		return false
	}
	c.mu.RLock()
	_, ok := c.seededToolFps[fp]
	c.mu.RUnlock()
	return ok
}

// SeedTools registers reversible aliases from normalized tool descriptions and parameter schemas.
func (c *ReversibleSessionCodec) SeedTools(items []ToolSeedItem) {
	if c == nil || len(items) == 0 {
		return
	}
	for _, t := range items {
		if t.Description != "" {
			c.seedOutboundTextReadOnly(t.Description)
		}
		if t.Parameters != nil {
			c.seedValueReadOnly(t.Parameters)
		}
	}
}

// SeedToolsOnce registers reversible aliases from items at most once per catalog fingerprint fp
// (memoized inside the codec in a bounded set cleared whenever SetTenantKey changes).
func (c *ReversibleSessionCodec) SeedToolsOnce(fp string, items []ToolSeedItem) {
	if c == nil || len(items) == 0 {
		return
	}
	if fp != "" && c.IsToolsFPSeeded(fp) {
		return
	}
	c.SeedTools(items)
	if fp == "" {
		return
	}
	c.mu.Lock()
	if c.seededToolFps == nil {
		c.seededToolFps = make(map[string]struct{}, 4)
	} else if len(c.seededToolFps) >= maxSeededToolFps {
		c.seededToolFps = make(map[string]struct{}, 4)
	}
	c.seededToolFps[fp] = struct{}{}
	c.mu.Unlock()
}

// SeedConversationStrings registers reversible aliases from historical system/message strings.
func (c *ReversibleSessionCodec) SeedConversationStrings(sysText string, messageTexts []string, once bool) {
	if c == nil {
		return
	}
	if once {
		if c.historySeeded.Swap(true) {
			return
		}
	} else {
		c.historySeeded.Store(true)
	}
	if sysText != "" {
		_ = c.EncodeOutboundText(sysText)
	}
	for _, txt := range messageTexts {
		if txt != "" {
			_ = c.EncodeOutboundText(txt)
		}
	}
}

// MarkHistorySeeded marks history seeding complete so subsequent Once calls no-op.
func (c *ReversibleSessionCodec) MarkHistorySeeded() {
	if c != nil {
		c.historySeeded.Store(true)
	}
}

func isLowerHexByte(b byte) bool {
	return (b >= '0' && b <= '9') || (b >= 'a' && b <= 'f')
}

func hasReversiblePrefixAt(s string, pos int) bool {
	if pos+6 > len(s) {
		return false
	}
	p := s[pos : pos+6]
	return p == reversibleSegPrefix || p == reversibleTagPrefix
}

func isCompleteReversibleTokenAt(s string, pos int) bool {
	if pos+reversibleTokenLen > len(s) || !hasReversiblePrefixAt(s, pos) {
		return false
	}
	for i := pos + 6; i < pos+reversibleTokenLen; i++ {
		if !isLowerHexByte(s[i]) {
			return false
		}
	}
	return true
}

func isPartialReversiblePrefix(tail string) bool {
	n := len(tail)
	if n == 0 || n >= reversibleTokenLen {
		return false
	}
	if n <= 6 {
		return strings.HasPrefix(reversibleSegPrefix, tail) || strings.HasPrefix(reversibleTagPrefix, tail)
	}
	p := tail[:6]
	if p != reversibleSegPrefix && p != reversibleTagPrefix {
		return false
	}
	for i := 6; i < n; i++ {
		if !isLowerHexByte(tail[i]) {
			return false
		}
	}
	return true
}

// RestoreText replaces registered wsseg_<8hex> / wstag_<8hex> tokens in s with originals.
func (c *ReversibleSessionCodec) RestoreText(s string) string {
	if c == nil || c.empty.Load() || len(s) < reversibleTokenLen {
		return s
	}
	if !strings.Contains(s, "ws") {
		return s
	}
	var b strings.Builder
	changed := false
	last := 0
	for i := 0; i <= len(s)-reversibleTokenLen; {
		if s[i] == 'w' && isCompleteReversibleTokenAt(s, i) {
			tok := s[i : i+reversibleTokenLen]
			if orig, ok := c.Lookup(tok); ok {
				if !changed {
					b.Grow(len(s) + 32)
					changed = true
				}
				b.WriteString(s[last:i])
				b.WriteString(orig)
				i += reversibleTokenLen
				last = i
				continue
			}
		}
		i++
	}
	if !changed {
		return s
	}
	b.WriteString(s[last:])
	return b.String()
}

// RestoreValue recursively restores reversible tokens inside decoded tool call JSON structures.
func (c *ReversibleSessionCodec) RestoreValue(v any) any {
	if c == nil || c.empty.Load() || v == nil {
		return v
	}
	switch x := v.(type) {
	case string:
		return c.RestoreText(x)
	case []string:
		out := make([]string, len(x))
		for i, elem := range x {
			out[i] = c.RestoreText(elem)
		}
		return out
	case map[string]any:
		return c.RestoreMap(x)
	case []any:
		out := make([]any, len(x))
		for i, elem := range x {
			out[i] = c.RestoreValue(elem)
		}
		return out
	default:
		return v
	}
}

// RestoreMap recursively restores reversible tokens in map keys and values.
func (c *ReversibleSessionCodec) RestoreMap(m map[string]any) map[string]any {
	if c == nil || c.empty.Load() || m == nil {
		return m
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[c.RestoreText(k)] = c.RestoreValue(v)
	}
	return out
}

// StreamRestorer restores wsseg_<8hex> / wstag_<8hex> tokens across streaming chunk boundaries.
type StreamRestorer struct {
	codec   *ReversibleSessionCodec
	pending string
}

// NewStreamRestorer constructs a streaming restorer bound to c (may be nil).
func (c *ReversibleSessionCodec) NewStreamRestorer() *StreamRestorer {
	return &StreamRestorer{codec: c}
}

// Push feeds a streaming text chunk and returns the safe restored prefix.
func (r *StreamRestorer) Push(chunk string) string {
	if r == nil || chunk == "" {
		return chunk
	}
	if r.codec == nil || r.codec.IsEmpty() {
		if r.pending != "" {
			out := r.pending + chunk
			r.pending = ""
			return out
		}
		return chunk
	}
	buf := r.pending + chunk
	r.pending = ""

	maxHold := reversibleMaxHold
	if len(buf) < maxHold {
		maxHold = len(buf)
	}
	holdLen := 0
	for k := maxHold; k >= 1; k-- {
		if isPartialReversiblePrefix(buf[len(buf)-k:]) {
			holdLen = k
			break
		}
	}
	safe := buf[:len(buf)-holdLen]
	r.pending = buf[len(buf)-holdLen:]
	if safe == "" {
		return ""
	}
	return r.codec.RestoreText(safe)
}

// Flush drains any held trailing prefix bytes at end-of-stream.
func (r *StreamRestorer) Flush() string {
	if r == nil || r.pending == "" {
		return ""
	}
	tail := r.pending
	r.pending = ""
	if r.codec != nil && !r.codec.IsEmpty() {
		return r.codec.RestoreText(tail)
	}
	return tail
}
