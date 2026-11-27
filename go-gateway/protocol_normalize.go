package main

import (
	"agy-acp-gateway-go/internal/acp"
	gwcfg "agy-acp-gateway-go/internal/config"
	"agy-acp-gateway-go/internal/protocol"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"
)

var (
	claudeWebOnlyTools = map[string]bool{
		"WebSearch":    true,
		"NotebookEdit": true,
	}
	billingHeaderRe     = regexp.MustCompile(`(?im)^x-anthropic-billing-header:[^\r\n]*$`)
	claudeCliIdentRe    = regexp.MustCompile(`(?i)You are Claude Code,\s*Anthropic's official CLI for Claude[^\r\n]*`)
	claudeSdkIdentRe    = regexp.MustCompile(`(?i)You are a Claude agent,\s*built on Anthropic's Claude Agent SDK[^\r\n]*`)
	claudeVendorNamesRe = regexp.MustCompile(`(?i)Claude Code|Claude Agent SDK|Anthropic|Claude`)
	sysReminderDumpRe   = regexp.MustCompile(`(?is)<system-reminder>\s*Codebase and user instructions are shown below.*?</system-reminder>`)
	inlineBase64ImgRe   = regexp.MustCompile(`data:image/[\w/+.-]+;base64,[A-Za-z0-9+/=]{500,}`)
	cwdPatterns         = []*regexp.Regexp{
		regexp.MustCompile(`(?i)Primary working directory:\s*([^\r\n"\\]+)`),
		regexp.MustCompile(`(?i)Working directory:\s*([^\r\n"\\]+)`),
		regexp.MustCompile(`(?i)WORKSPACE_DIR[:=\s]+([^\r\n"\\]+)`),
		regexp.MustCompile(`(?is)<cwd>\s*([^\r\n<]+?)\s*</cwd>`),
	}
	softErrBlockedRe = acp.SoftErrBlockedRe
	softErrLicenseRe = acp.SoftErrLicenseRe
)

func isAccountQuotaOrBillingExhausted(detail string) bool {
	return acp.IsAccountQuotaOrBillingExhausted(detail)
}

func isUpstreamPolicyOrSafetyBlock(text string, inErrorContext bool) bool {
	return acp.IsUpstreamPolicyOrSafetyBlock(text, inErrorContext)
}

func extractRetryAfterHeaderValue(msg string) string {
	return acp.ExtractRetryAfterHeaderValue(msg)
}

func unmarshalJSONPreserveNumbers(data []byte, dst any) error {
	return gwcfg.UnmarshalJSONPreserveNumbers(data, dst)
}

type SoftError = acp.SoftError

func DetectAntigravitySoftError(text string) *SoftError {
	return acp.DetectAntigravitySoftError(text, TripTosCircuitBreaker)
}

func classifyUpstreamError(err error) (statusCode int, errType string, retryAfter bool, msg string) {
	if err == nil {
		return http.StatusOK, "", false, ""
	}
	safeMsg := redactSecretsInString(err.Error())
	var switchErr *ModelSwitchError
	if errors.As(err, &switchErr) {
		if strings.Contains(safeMsg, "-32602") {
			return http.StatusBadRequest, "invalid_request_error", false, safeMsg
		}
		return http.StatusBadGateway, "api_error", false, safeMsg
	}
	var mmErr *MultimodalValidationError
	if errors.As(err, &mmErr) && mmErr != nil {
		code := mmErr.StatusCode
		if code <= 0 {
			code = http.StatusBadRequest
		}
		return code, "invalid_request_error", false, redactSecretsInString(mmErr.Error())
	}
	var softErr *SoftError
	if errors.As(err, &softErr) && softErr != nil {
		retry := (softErr.StatusCode == 429 || softErr.StatusCode == 503 || softErr.StatusCode == 529) &&
			softErr.ErrorType != "tos_circuit_breaker" && softErr.ErrorType != "verification_required"
		return softErr.StatusCode, softErr.ErrorType, retry, redactSecretsInString(softErr.Message)
	}
	if isVerificationRequiredMessage(safeMsg) {
		return http.StatusServiceUnavailable, "verification_required", false, safeMsg
	}
	if softErrLicenseRe.MatchString(safeMsg) {
		return http.StatusForbidden, "permission_error", false, safeMsg
	}
	if IsTosViolationError(safeMsg) {
		TripTosCircuitBreaker(safeMsg)
		return http.StatusServiceUnavailable, "tos_circuit_breaker", false, "Antigravity ToS circuit breaker tripped: " + safeMsg
	}
	if isUpstreamPolicyOrSafetyBlock(safeMsg, true) {
		return http.StatusBadRequest, "invalid_request_error", false, safeMsg
	}
	low := strings.ToLower(safeMsg)
	hasRateLimitMarker := strings.Contains(low, "429") || strings.Contains(low, "rate limit") ||
		strings.Contains(low, "rate_limit") || strings.Contains(low, "quota") || strings.Contains(low, "resource_exhausted")
	if strings.Contains(low, "invalid_argument") || strings.Contains(low, "failed_precondition") ||
		strings.Contains(low, "code 400") || strings.Contains(low, "status 400") ||
		strings.Contains(low, "400 bad request") ||
		(!hasRateLimitMarker && (strings.Contains(low, "thinking budget") ||
			strings.Contains(low, "prompt is too long") || strings.Contains(low, "context length exceeded") ||
			strings.Contains(low, "exceeds the maximum number of tokens"))) {
		return http.StatusBadRequest, "invalid_request_error", false, safeMsg
	}
	if (strings.Contains(low, "model_capacity_exhausted") || strings.Contains(low, "no capacity available") ||
		strings.Contains(low, "service unavailable") || strings.Contains(low, "code 503") ||
		strings.Contains(low, "status 503") || strings.Contains(low, "overloaded")) && !hasRateLimitMarker {
		return 529, "overloaded_error", true, safeMsg
	}
	if hasRateLimitMarker {
		return http.StatusTooManyRequests, "rate_limit_error", true, safeMsg
	}
	if strings.Contains(low, "timed out") || strings.Contains(low, "not running") || strings.Contains(low, "exited") || strings.Contains(low, "unavailable") {
		return http.StatusServiceUnavailable, "api_error", true, safeMsg
	}
	return http.StatusInternalServerError, "api_error", false, safeMsg
}

func toInt(v any) (int, bool) {
	return protocol.ToInt(v)
}

func stripClaudeAttributionAndBoilerplate(s string) string {
	return PruneClientSystemPrompt(s)
}

func stripRedundantSystemReminders(s string) string {
	if s == "" {
		return ""
	}
	return SanitizeOutboundText(sysReminderDumpRe.ReplaceAllString(s, ""))
}

func normalizeSystemText(sys any) string {
	if sys == nil {
		return ""
	}
	return PruneClientSystemPrompt(rawSystemText(sys))
}

// rawSystemText joins a system field (string or content blocks) without pruning.
func rawSystemText(sys any) string {
	switch v := sys.(type) {
	case nil:
		return ""
	case string:
		return v
	case []any:
		var parts []string
		for _, item := range v {
			switch b := item.(type) {
			case string:
				parts = append(parts, b)
			case map[string]any:
				if t, ok := b["text"].(string); ok {
					parts = append(parts, t)
				} else if raw, err := json.Marshal(b); err == nil {
					parts = append(parts, string(raw))
				}
			}
		}
		return strings.Join(parts, "\n")
	default:
		if raw, err := json.Marshal(v); err == nil {
			return string(raw)
		}
		return ""
	}
}

func extractCwdFromSystem(sysText string, messages []map[string]any) string {
	// Fast path: cwdPatterns[0] has highest precedence; if sysText already yields a valid directory
	// for cwdPatterns[0], we never need to format/sanitize messages[:4].
	if sysText != "" {
		if m := cwdPatterns[0].FindStringSubmatch(sysText); len(m) > 1 {
			cand := strings.TrimSpace(m[1])
			if st, err := os.Stat(cand); err == nil && st.IsDir() {
				return cand
			}
		}
	}
	limit := len(messages)
	if limit > 4 {
		limit = 4
	}
	var initParts []string
	for _, m := range messages[:limit] {
		if t := strings.TrimSpace(formatSingleMessageText(m)); t != "" {
			initParts = append(initParts, t)
		}
	}
	combined := sysText + "\n" + strings.Join(initParts, "\n")
	for _, pat := range cwdPatterns {
		m := pat.FindStringSubmatch(combined)
		if len(m) > 1 {
			cand := strings.TrimSpace(m[1])
			if st, err := os.Stat(cand); err == nil && st.IsDir() {
				return cand
			}
		}
	}
	return DefaultCwd
}

// parseThoughtLevel: medium is the full-quota 1M Gemini tier ("high" can 429 on bursts);
// explicit -low/-high suffix or reasoning_effort is honoured (see models.go).
func parseThoughtLevel(modelStr string, body map[string]any) string {
	return ResolveRequestedModel(modelStr, body).Tier
}

func normalizeDisplayModel(reqModel string, body map[string]any) string {
	return ResolveRequestedModel(reqModel, body).Model.ID
}

func sanitizeSchemaNode(node any) any {
	switch v := node.(type) {
	case []any:
		out := make([]any, len(v))
		for i, elem := range v {
			out[i] = sanitizeSchemaNode(elem)
		}
		return out
	case map[string]any:
		out := make(map[string]any)
		for k, val := range v {
			switch k {
			case "pattern", "prefixItems", "$schema", "additionalProperties", "propertyNames", "unevaluatedProperties", "const", "default", "examples", "title":
				continue
			case "$ref":
				out["type"] = "object"
				continue
			case "anyOf", "oneOf", "allOf":
				if arr, ok := val.([]any); ok && len(arr) > 0 {
					var picked any
					for _, sub := range arr {
						if sm, okM := sub.(map[string]any); okM && sm["type"] != "null" {
							picked = sub
							break
						}
					}
					if picked == nil {
						picked = arr[0]
					}
					if pm, okP := sanitizeSchemaNode(picked).(map[string]any); okP {
						for pk, pv := range pm {
							out[pk] = pv
						}
					}
				}
				continue
			case "type":
				if tArr, okA := val.([]any); okA {
					nonNull := "string"
					for _, tItem := range tArr {
						if ts, okS := tItem.(string); okS && ts != "null" {
							nonNull = ts
							break
						}
					}
					out["type"] = nonNull
					continue
				}
			case "required":
				switch rArr := val.(type) {
				case []any:
					var reqs []string
					seenReq := make(map[string]bool, len(rArr))
					for _, item := range rArr {
						if rs, okS := item.(string); okS && !seenReq[rs] {
							seenReq[rs] = true
							reqs = append(reqs, rs)
						}
					}
					sort.Strings(reqs)
					out["required"] = reqs
					continue
				case []string:
					var reqs []string
					seenReq := make(map[string]bool, len(rArr))
					for _, rs := range rArr {
						if !seenReq[rs] {
							seenReq[rs] = true
							reqs = append(reqs, rs)
						}
					}
					sort.Strings(reqs)
					out["required"] = reqs
					continue
				}
			}
			out[k] = sanitizeSchemaNode(val)
		}
		if out["type"] == "array" && out["items"] == nil {
			out["items"] = map[string]any{"type": "string"}
		}
		if desc, ok := out["description"].(string); ok {
			d := strings.TrimSpace(SanitizeOutboundText(desc))
			if len(d) > 200 {
				d = truncateRunes(d, 200)
			}
			out["description"] = d
		}
		return out
	default:
		return node
	}
}

const maxSanitizedToolsCacheEntries = 64

type sanitizedToolsCacheKey struct {
	hash     [32]byte
	wafStamp wafStateKey
}

var sanitizedToolsCache = struct {
	mu      sync.RWMutex
	entries map[sanitizedToolsCacheKey][]ToolDef
	order   []sanitizedToolsCacheKey
}{
	entries: make(map[sanitizedToolsCacheKey][]ToolDef, maxSanitizedToolsCacheEntries),
}

func hashRawToolsSlice(arr []any) ([32]byte, bool) {
	h := sha256.New()
	enc := json.NewEncoder(h)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(arr); err != nil {
		return [32]byte{}, false
	}
	var sum [32]byte
	h.Sum(sum[:0])
	return sum, true
}

func sanitizeToolsCatalog(rawTools any) []ToolDef {
	arr, ok := rawTools.([]any)
	if !ok || len(arr) == 0 {
		return nil
	}
	var cacheKey sanitizedToolsCacheKey
	cacheable := false
	if sum, okH := hashRawToolsSlice(arr); okH {
		cacheKey = sanitizedToolsCacheKey{
			hash:     sum,
			wafStamp: currentWafStateKey(),
		}
		cacheable = true
		sanitizedToolsCache.mu.RLock()
		if cached, hit := sanitizedToolsCache.entries[cacheKey]; hit {
			sanitizedToolsCache.mu.RUnlock()
			return append([]ToolDef(nil), cached...)
		}
		sanitizedToolsCache.mu.RUnlock()
	}

	seen := make(map[string]bool)
	seenAlias := make(map[string]bool)
	var catalog []ToolDef

	for _, item := range arr {
		t, ok := item.(map[string]any)
		if !ok {
			continue
		}
		name, _ := t["name"].(string)
		if name == "" {
			if fn, okF := t["function"].(map[string]any); okF {
				name, _ = fn["name"].(string)
			}
		}
		name = strings.TrimSpace(name)
		if name == "" || seen[name] || claudeWebOnlyTools[name] {
			continue
		}
		tType, _ := t["type"].(string)
		if tType != "" && tType != "custom" && tType != "function" && t["input_schema"] == nil && t["parameters"] == nil {
			continue
		}
		seen[name] = true

		rawSchema := t["input_schema"]
		if rawSchema == nil {
			rawSchema = t["parameters"]
		}
		if rawSchema == nil {
			if fn, okF := t["function"].(map[string]any); okF {
				rawSchema = fn["parameters"]
			}
		}
		if rawSchema == nil {
			rawSchema = map[string]any{"type": "object", "properties": map[string]any{}}
		}
		sanitized, _ := sanitizeSchemaNode(rawSchema).(map[string]any)
		if sanitized == nil {
			sanitized = map[string]any{"type": "object", "properties": map[string]any{}}
		}
		props, _ := sanitized["properties"].(map[string]any)
		if props == nil {
			props = make(map[string]any)
		}
		delete(props, "toolSummary")

		var reqList []string
		reqSeen := make(map[string]bool)
		appendReq := func(rk string) {
			if rk != "toolSummary" && !reqSeen[rk] {
				if _, hasProp := props[rk]; hasProp {
					reqSeen[rk] = true
					reqList = append(reqList, rk)
				}
			}
		}
		switch rawReq := sanitized["required"].(type) {
		case []string:
			for _, rk := range rawReq {
				appendReq(rk)
			}
		case []any:
			for _, rItem := range rawReq {
				if rk, okS := rItem.(string); okS {
					appendReq(rk)
				}
			}
		}
		sort.Strings(reqList)

		cleanSchema := map[string]any{
			"type":       "object",
			"properties": props,
		}
		if len(reqList) > 0 {
			cleanSchema["required"] = reqList
		}

		rawDesc, _ := t["description"].(string)
		if rawDesc == "" {
			if fn, okF := t["function"].(map[string]any); okF {
				rawDesc, _ = fn["description"].(string)
			}
		}
		rawDesc = strings.TrimSpace(SanitizeOutboundText(rawDesc))
		if idx := strings.IndexByte(rawDesc, '\n'); idx >= 0 {
			rawDesc = strings.TrimSpace(rawDesc[:idx])
		}
		if len(rawDesc) > 300 {
			rawDesc = truncateRunes(rawDesc, 300)
		}
		neutralName := NeutralizeToolIdentifier(name)
		outboundAlias := neutralName
		if outboundAlias == "" {
			outboundAlias = "ws_tool"
		}
		if seenAlias[outboundAlias] {
			outboundAlias = outboundAlias + "_" + shortSha256(name)[:4]
		}
		seenAlias[outboundAlias] = true

		if rawDesc == "" {
			rawDesc = "Execute host tool " + outboundAlias
		}

		catalog = append(catalog, ToolDef{
			Name:          name,
			OutboundAlias: outboundAlias,
			McpName:       protocol.McpToolNameFromNeutral(name, neutralName),
			Description:   rawDesc,
			Parameters:    cleanSchema,
		})
	}

	if len(catalog) > 0 {
		if !seen[AwaitToolName] {
			catalog = append(catalog, ToolDef{
				Name:        AwaitToolName,
				McpName:     mcpToolName(AwaitToolName),
				Description: "Wait for the result of a harness tool call that reported it is still running.",
				Parameters: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"call_id": map[string]any{"type": "string", "description": "The call id from the still-running notice."},
					},
					"required": []string{"call_id"},
				},
			})
		}
		if !seen[CallToolName] {
			catalog = append(catalog, ToolDef{
				Name:        CallToolName,
				McpName:     mcpToolName(CallToolName),
				Description: "Invoke any harness tool by exact name when its direct tool declaration is absent.",
				Parameters: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"name":      map[string]any{"type": "string", "description": "Exact host tool name."},
						"arguments": map[string]any{"type": "object", "description": "Arguments object."},
					},
					"required": []string{"name"},
				},
			})
		}
	}
	if cacheable {
		stored := append([]ToolDef(nil), catalog...)
		sanitizedToolsCache.mu.Lock()
		if _, exists := sanitizedToolsCache.entries[cacheKey]; !exists {
			if len(sanitizedToolsCache.order) >= maxSanitizedToolsCacheEntries {
				evict := sanitizedToolsCache.order[0]
				sanitizedToolsCache.order = sanitizedToolsCache.order[1:]
				delete(sanitizedToolsCache.entries, evict)
			}
			sanitizedToolsCache.order = append(sanitizedToolsCache.order, cacheKey)
		}
		sanitizedToolsCache.entries[cacheKey] = stored
		sanitizedToolsCache.mu.Unlock()
	}
	return catalog
}

func extractMediaDescriptor(b map[string]any) (map[string]any, bool) {
	if b == nil {
		return nil, false
	}
	bType, _ := b["type"].(string)
	switch bType {
	case "image":
		if dataStr, _ := b["data"].(string); dataStr != "" {
			mimeStr, _ := b["mimeType"].(string)
			if mimeStr == "" {
				mimeStr, _ = b["media_type"].(string)
			}
			if mimeStr == "" {
				mimeStr = "image/png"
			}
			return map[string]any{
				"type":     "image",
				"mimeType": mimeStr,
				"data":     dataStr,
			}, true
		}
		if src, okS := b["source"].(map[string]any); okS && src["type"] == "base64" {
			dataStr, _ := src["data"].(string)
			mimeStr, _ := src["media_type"].(string)
			if mimeStr == "" {
				mimeStr = "image/png"
			}
			if dataStr != "" {
				return map[string]any{
					"type":     "image",
					"mimeType": mimeStr,
					"data":     dataStr,
				}, true
			}
		}
	case "image_ref":
		if uStr, _ := b["url"].(string); strings.TrimSpace(uStr) != "" {
			return map[string]any{
				"type": "image_ref",
				"url":  strings.TrimSpace(uStr),
			}, true
		}
	case "image_url", "input_image":
		var urlStr string
		switch u := b["image_url"].(type) {
		case string:
			urlStr = u
		case map[string]any:
			urlStr, _ = u["url"].(string)
		}
		if urlStr == "" {
			urlStr, _ = b["url"].(string)
		}
		if strings.HasPrefix(urlStr, "data:") {
			if comma := strings.IndexByte(urlStr, ','); comma > 5 {
				hdr := urlStr[5:comma]
				dataPart := urlStr[comma+1:]
				mimeStr := strings.Split(hdr, ";")[0]
				if mimeStr == "" {
					mimeStr = "image/png"
				}
				return map[string]any{
					"type":     "image",
					"mimeType": mimeStr,
					"data":     dataPart,
				}, true
			}
		} else if strings.TrimSpace(urlStr) != "" {
			return map[string]any{
				"type": "image_ref",
				"url":  strings.TrimSpace(urlStr),
			}, true
		}
	}
	return nil, false
}

func extractImageBlocksFromMessage(m map[string]any) []map[string]any {
	if m == nil {
		return nil
	}
	var imgs []map[string]any
	walkMessageMediaBlocks(m, func(b map[string]any, _ bool) {
		if desc, ok := extractMediaDescriptor(b); ok {
			imgs = append(imgs, desc)
		}
	})
	return imgs
}

func formatMediaPlaceholder(b map[string]any) string {
	bType, _ := b["type"].(string)
	if bType == "image" {
		mimeStr, _ := b["mimeType"].(string)
		if mimeStr == "" {
			if src, okS := b["source"].(map[string]any); okS {
				mimeStr, _ = src["media_type"].(string)
			}
		}
		if mimeStr == "" {
			mimeStr = "image"
		}
		kb := inlineImageSummaryKB(b)
		return fmt.Sprintf("[Image attachment (%s, ~%dKB binary omitted in text history)]", mimeStr, kb)
	}
	return "[Remote image attachment omitted in text history]"
}

func formatToolResultContentAsTextWithNotes(content any, consumeNote func(b map[string]any) string) string {
	switch v := content.(type) {
	case string:
		return SanitizeOutboundText(inlineBase64ImgRe.ReplaceAllString(v, "[base64 image omitted]"))
	case []any:
		var parts []string
		for _, item := range v {
			c, ok := item.(map[string]any)
			if !ok {
				continue
			}
			cType, _ := c["type"].(string)
			switch cType {
			case "text", "input_text", "output_text":
				t, _ := c["text"].(string)
				parts = append(parts, stripRedundantSystemReminders(t))
			case "refusal":
				t, _ := c["refusal"].(string)
				if t == "" {
					t, _ = c["text"].(string)
				}
				parts = append(parts, stripRedundantSystemReminders(t))
			case "image", "image_ref", "image_url", "input_image":
				if consumeNote != nil {
					if note := consumeNote(c); note != "" {
						parts = append(parts, note)
					}
				} else {
					parts = append(parts, formatMediaPlaceholder(c))
				}
			case "tool_result":
				parts = append(parts, formatToolResultBlockText(c, consumeNote, nil))
			default:
				if raw, err := json.Marshal(SanitizeStructuredValue(c)); err == nil {
					parts = append(parts, string(raw))
				}
			}
		}
		return strings.Join(parts, "\n")
	case []map[string]any:
		asAny := make([]any, 0, len(v))
		for _, item := range v {
			asAny = append(asAny, item)
		}
		return formatToolResultContentAsTextWithNotes(asAny, consumeNote)
	case map[string]any:
		return formatToolResultContentAsTextWithNotes([]any{v}, consumeNote)
	default:
		if raw, err := json.Marshal(SanitizeStructuredValue(v)); err == nil {
			return string(raw)
		}
		return ""
	}
}

func formatToolResultContentAsText(content any) string {
	return formatToolResultContentAsTextWithNotes(content, nil)
}

type hibernatedCallMetaCtxKey struct{}

func withHibernatedCallMeta(ctx context.Context, meta map[string]HibernatedCallMeta) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(meta) == 0 {
		return ctx
	}
	return context.WithValue(ctx, hibernatedCallMetaCtxKey{}, meta)
}

func hibernatedCallMetaFromContext(ctx context.Context) map[string]HibernatedCallMeta {
	if ctx == nil {
		return nil
	}
	meta, _ := ctx.Value(hibernatedCallMetaCtxKey{}).(map[string]HibernatedCallMeta)
	return meta
}

func toolResultBlockIsError(b map[string]any) bool {
	if b == nil {
		return false
	}
	if v, ok := b["is_error"].(bool); ok {
		return v
	}
	if s, ok := b["is_error"].(string); ok {
		return strings.EqualFold(strings.TrimSpace(s), "true")
	}
	return false
}

func formatToolResultHeaderAndText(id string, isErr bool, c string, hibMeta map[string]HibernatedCallMeta) string {
	if hibMeta != nil && id != "" {
		if meta, ok := hibMeta[id]; ok && (meta.ToolName != "" || meta.ArgSummary != "" || meta.Seq > 0) {
			seqPart := ""
			if meta.Seq > 0 {
				seqPart = fmt.Sprintf(" #%d", meta.Seq)
			}
			label := NeutralizeToolIdentifier(meta.ToolName)
			if meta.ArgSummary != "" {
				if label != "" {
					label += " (" + meta.ArgSummary + ")"
				} else {
					label = meta.ArgSummary
				}
			}
			attrs := make([]string, 0, 3)
			if label != "" {
				attrs = append(attrs, label)
			}
			attrs = append(attrs, "id="+id)
			if isErr {
				attrs = append(attrs, "status=error")
			}
			return fmt.Sprintf("[Tool Result%s (%s)]: %s", seqPart, strings.Join(attrs, ", "), c)
		}
	}
	if isErr {
		if id != "" {
			return fmt.Sprintf("[Tool Result (%s, status=error)]: %s", id, c)
		}
		return fmt.Sprintf("[Tool Result (status=error)]: %s", c)
	}
	if id != "" {
		return fmt.Sprintf("[Tool Result (%s)]: %s", id, c)
	}
	return c
}

func formatToolResultBlockText(b map[string]any, consumeNote func(map[string]any) string, hibMeta map[string]HibernatedCallMeta) string {
	id, _ := b["tool_use_id"].(string)
	isErr := toolResultBlockIsError(b)
	c := formatToolResultContentAsTextWithNotes(b["content"], consumeNote)
	return formatToolResultHeaderAndText(id, isErr, c, hibMeta)
}

func formatSingleMessageTextWithMediaNotes(m map[string]any, notes []string) string {
	return formatSingleMessageTextWithHibernatedMeta(m, notes, nil)
}

func formatSingleMessageTextWithHibernatedMeta(m map[string]any, notes []string, hibMeta map[string]HibernatedCallMeta) string {
	if m == nil {
		return ""
	}
	noteIdx := 0
	nextHistoryNote := func(_ map[string]any) string {
		if notes == nil {
			return ""
		}
		if noteIdx < len(notes) {
			s := notes[noteIdx]
			noteIdx++
			return s
		}
		return ""
	}
	var consumeToolNote func(b map[string]any) string
	if notes != nil {
		consumeToolNote = nextHistoryNote
	}

	formatBlocks := func(blocks []any) string {
		var parts []string
		for _, item := range blocks {
			b, ok := item.(map[string]any)
			if !ok {
				continue
			}
			bType, _ := b["type"].(string)
			switch bType {
			case "thinking", "redacted_thinking":
				continue
			case "image", "image_ref", "image_url", "input_image":
				if note := nextHistoryNote(b); note != "" {
					parts = append(parts, note)
				}
				continue
			case "media_relay":
				if isCanonicalMediaRelayBlock(b) {
					fname, _ := b["filename"].(string)
					mimeStr, _ := b["mimeType"].(string)
					if fname == "" {
						fname = "attachment"
					}
					if mimeStr != "" {
						parts = append(parts, fmt.Sprintf("[Attached media: %s (%s)]", fname, mimeStr))
					} else {
						parts = append(parts, fmt.Sprintf("[Attached media: %s]", fname))
					}
				}
				continue
			case "text", "input_text", "output_text":
				t, _ := b["text"].(string)
				if s := stripRedundantSystemReminders(t); s != "" {
					parts = append(parts, s)
				}
			case "refusal":
				t, _ := b["refusal"].(string)
				if t == "" {
					t, _ = b["text"].(string)
				}
				if s := stripRedundantSystemReminders(t); s != "" {
					parts = append(parts, s)
				}
			case "tool_result":
				parts = append(parts, formatToolResultBlockText(b, consumeToolNote, hibMeta))
			case "tool_use":
				name, _ := b["name"].(string)
				id, _ := b["id"].(string)
				inBytes, _ := json.Marshal(SanitizeStructuredValue(b["input"]))
				parts = append(parts, fmt.Sprintf("[Tool Call (%s, id=%s)]: %s", NeutralizeToolIdentifier(name), id, string(inBytes)))
			default:
				if raw, err := json.Marshal(SanitizeStructuredValue(b)); err == nil {
					parts = append(parts, string(raw))
				}
			}
		}
		return strings.Join(parts, "\n")
	}

	switch content := m["content"].(type) {
	case string:
		return stripRedundantSystemReminders(content)
	case []any:
		return formatBlocks(content)
	case []map[string]any:
		asAny := make([]any, 0, len(content))
		for _, item := range content {
			asAny = append(asAny, item)
		}
		return formatBlocks(asAny)
	case map[string]any:
		return formatBlocks([]any{content})
	default:
		if raw, err := json.Marshal(SanitizeStructuredValue(content)); err == nil {
			return string(raw)
		}
		return ""
	}
}

func formatSingleMessageText(m map[string]any) string {
	return formatSingleMessageTextWithMediaNotes(m, nil)
}

func extractToolResultTextAndMedia(val any) (string, []map[string]any) {
	var parts []string
	var imgs []map[string]any
	var walk func(v any)
	walk = func(v any) {
		switch x := v.(type) {
		case nil:
			return
		case string:
			if x != "" {
				parts = append(parts, x)
			}
		case []any:
			for _, sub := range x {
				walk(sub)
			}
		case []map[string]any:
			for _, sub := range x {
				walk(sub)
			}
		case map[string]any:
			ct, _ := x["type"].(string)
			switch ct {
			case "text", "input_text", "output_text":
				if ts, _ := x["text"].(string); ts != "" {
					parts = append(parts, ts)
				}
			case "refusal":
				ts, _ := x["refusal"].(string)
				if ts == "" {
					ts, _ = x["text"].(string)
				}
				if ts != "" {
					parts = append(parts, ts)
				}
			case "image", "image_ref", "image_url", "input_image":
				if desc, ok := extractMediaDescriptor(x); ok {
					imgs = append(imgs, desc)
				}
			case "tool_result":
				walk(x["content"])
			default:
				if raw, err := json.Marshal(x); err == nil {
					parts = append(parts, string(raw))
				}
			}
		default:
			if raw, err := json.Marshal(x); err == nil {
				parts = append(parts, string(raw))
			}
		}
	}
	walk(val)
	return strings.Join(parts, "\n"), imgs
}

func formatAttachedUserSteeringText(extra string) string {
	clean := stripRedundantSystemReminders(extra)
	if strings.TrimSpace(clean) == "" {
		return ""
	}
	trimmed := trimBlankOuterLines(extra)
	if strings.HasPrefix(clean, "<workspace-context>") && strings.HasSuffix(clean, "</workspace-context>") {
		return trimmed
	}
	return "[User message]\n" + trimmed
}

func attachTrailingUserInputsToToolResult(dst *ToolResultInput, extraText string, extraImgs []map[string]any) {
	if dst == nil {
		return
	}
	formatted := formatAttachedUserSteeringText(extraText)
	if formatted != "" {
		if dst.Content == "" {
			dst.Content = formatted
		} else {
			dst.Content += "\n\n" + formatted
		}
		if dst.TrailingUserText == "" {
			dst.TrailingUserText = extraText
		} else {
			dst.TrailingUserText += "\n\n" + extraText
		}
	}
	if len(extraImgs) > 0 {
		dst.Images = append(dst.Images, extraImgs...)
		dst.TrailingUserImages = append(dst.TrailingUserImages, extraImgs...)
	}
}

func extractToolResultsFromMessages(messages []map[string]any) []ToolResultInput {
	if len(messages) == 0 {
		return nil
	}
	start := latestTurnStartIndex(messages)
	var results []ToolResultInput
	var extraTexts []string
	var extraImgs []map[string]any

	collectBlockMap := func(b map[string]any) {
		if b == nil {
			return
		}
		bType, _ := b["type"].(string)
		switch bType {
		case "tool_result":
			tuID, _ := b["tool_use_id"].(string)
			if tuID == "" {
				return
			}
			txt, toolImgs := extractToolResultTextAndMedia(b["content"])
			isErr := toolResultBlockIsError(b)
			results = append(results, ToolResultInput{
				ToolCallID: tuID,
				Content:    txt,
				Images:     toolImgs,
				IsError:    isErr,
			})
		case "thinking", "redacted_thinking", "tool_use":
			return
		case "text", "input_text", "output_text":
			if t, _ := b["text"].(string); strings.TrimSpace(stripRedundantSystemReminders(t)) != "" {
				extraTexts = append(extraTexts, t)
			}
		case "refusal":
			t, _ := b["refusal"].(string)
			if t == "" {
				t, _ = b["text"].(string)
			}
			if strings.TrimSpace(stripRedundantSystemReminders(t)) != "" {
				extraTexts = append(extraTexts, t)
			}
		default:
			if desc, okD := extractMediaDescriptor(b); okD {
				extraImgs = append(extraImgs, desc)
			} else if raw, err := json.Marshal(b); err == nil && len(raw) > 2 {
				extraTexts = append(extraTexts, string(raw))
			}
		}
	}

	for i := start; i < len(messages); i++ {
		msg := messages[i]
		if msg == nil {
			continue
		}
		role, _ := msg["role"].(string)
		if role == "assistant" {
			continue
		}
		if role == "tool" {
			tcID, _ := msg["tool_call_id"].(string)
			if tcID != "" {
				txt, toolImgs := extractToolResultTextAndMedia(msg["content"])
				results = append(results, ToolResultInput{
					ToolCallID: tcID,
					Content:    txt,
					Images:     toolImgs,
				})
			}
			continue
		}
		switch content := msg["content"].(type) {
		case string:
			if strings.TrimSpace(stripRedundantSystemReminders(content)) != "" {
				extraTexts = append(extraTexts, content)
			}
		case []any:
			for _, item := range content {
				switch x := item.(type) {
				case map[string]any:
					collectBlockMap(x)
				case string:
					if strings.TrimSpace(stripRedundantSystemReminders(x)) != "" {
						extraTexts = append(extraTexts, x)
					}
				}
			}
		case []map[string]any:
			for _, item := range content {
				collectBlockMap(item)
			}
		case map[string]any:
			collectBlockMap(content)
		}
	}

	if len(results) == 0 {
		return nil
	}
	if len(extraTexts) > 0 || len(extraImgs) > 0 {
		attachTrailingUserInputsToToolResult(&results[len(results)-1], strings.Join(extraTexts, "\n\n"), extraImgs)
	}
	return results
}

var volatileFirstTurnTagRe = regexp.MustCompile(`(?s)<(?:system-reminder|workspace-context|environment_details)>.*?</(?:system-reminder|workspace-context|environment_details)>`)

func computeFirstUserFingerprint(messages []map[string]any) string {
	if len(messages) == 0 {
		return ""
	}
	firstAsst := -1
	for i, m := range messages {
		if m["role"] == "assistant" {
			firstAsst = i
			break
		}
	}
	limit := firstAsst
	if limit < 0 || limit > 2 {
		limit = 2
		if limit > len(messages) {
			limit = len(messages)
		}
	}
	var parts []string
	for _, m := range messages[:limit] {
		role, _ := m["role"].(string)
		rawTxt := strings.TrimSpace(formatSingleMessageText(m))
		coreTxt := strings.TrimSpace(volatileFirstTurnTagRe.ReplaceAllString(rawTxt, ""))
		imgs := imageFingerprintTokens(m)
		effTxt := rawTxt
		if coreTxt != "" || len(imgs) > 0 {
			effTxt = coreTxt
		}
		seg := role + ":" + effTxt
		if len(imgs) > 0 {
			seg += "|media:" + strings.Join(imgs, ",")
		}
		parts = append(parts, seg)
	}
	return shortSha256(strings.Join(parts, "||"))
}

// compactionHeadChars bounds where a compaction instruction may appear: it must open the final
// user text block, so a conversation that merely quotes the instruction is not treated as one.
const compactionHeadChars = 600

func lastMessageHasToolResult(messages []map[string]any) bool {
	if len(messages) == 0 {
		return false
	}
	last := messages[len(messages)-1]
	if last == nil {
		return false
	}
	if role, _ := last["role"].(string); role == "tool" {
		return true
	}
	hasBlockToolResult := func(b map[string]any) bool {
		if b == nil {
			return false
		}
		t, _ := b["type"].(string)
		return t == "tool_result" || t == "function_call_output"
	}
	switch c := last["content"].(type) {
	case []any:
		for _, item := range c {
			if b, ok := item.(map[string]any); ok && hasBlockToolResult(b) {
				return true
			}
		}
	case []map[string]any:
		for _, b := range c {
			if hasBlockToolResult(b) {
				return true
			}
		}
	case map[string]any:
		return hasBlockToolResult(c)
	}
	return false
}

func isCompactionSummaryRequest(messages []map[string]any) bool {
	return isCompactionSummaryRequestWithSystem("", messages)
}

func matchesCompactionInstructionBlock(text string, hasToolResult bool) bool {
	head := strings.TrimSpace(text)
	if head == "" {
		return false
	}
	if len(head) > compactionHeadChars {
		head = head[:compactionHeadChars]
	}
	head = strings.ToLower(head)
	if hasToolResult {
		// When a user turn also carries tool_result blocks, require start-of-block / start-of-line
		// anchoring for known client compaction templates so quoted prose discussing compaction
		// alongside tool_result never triggers a false positive.
		for _, line := range strings.Split(head, "\n") {
			trimmed := strings.TrimSpace(line)
			if trimmed == "" {
				continue
			}
			if strings.HasPrefix(trimmed, "your task is to create a detailed summary of the conversation") ||
				strings.HasPrefix(trimmed, "create a detailed summary of the conversation") ||
				strings.HasPrefix(trimmed, "you are performing a context checkpoint compaction") ||
				strings.HasPrefix(trimmed, "provide a detailed prompt for continuing our conversation above") ||
				strings.HasPrefix(trimmed, `<explicit_instructions type="summarize_task">`) ||
				strings.HasPrefix(trimmed, "your task is to summarize this conversation") {
				return true
			}
		}
		return false
	}
	if strings.Contains(head, "create a detailed summary of the conversation") ||
		strings.Contains(head, "you are performing a context checkpoint compaction") ||
		strings.Contains(head, "provide a detailed prompt for continuing our conversation above") ||
		strings.Contains(head, `<explicit_instructions type="summarize_task">`) ||
		strings.Contains(head, "your task is to summarize this conversation") {
		return true
	}
	if strings.Contains(head, "summarize this conversation") &&
		(strings.Contains(head, "context checkpoint") ||
			strings.Contains(head, "handoff") ||
			strings.Contains(head, "compaction") ||
			strings.Contains(head, "continuing our conversation")) {
		return true
	}
	return false
}

func isCompactionSummaryRequestWithSystem(sysText string, messages []map[string]any) bool {
	if len(messages) == 0 {
		return false
	}
	last := messages[len(messages)-1]
	if last == nil {
		return false
	}
	if role, _ := last["role"].(string); role == "tool" {
		return false
	}
	if sysText != "" && hasSystemLeadingBlockPrefixFold(sysText, "you are performing a context checkpoint compaction") {
		return true
	}
	hasToolResult := lastMessageHasToolResult(messages)
	// lastUserTextBlocks reads ONLY sibling type=="text" / "input_text" blocks on role=="user",
	// never tool_result.content — so quoted compaction text inside tool_result never matches.
	blocks := lastUserTextBlocks(messages)
	for i := len(blocks) - 1; i >= 0; i-- {
		if matchesCompactionInstructionBlock(blocks[i], hasToolResult) {
			return true
		}
	}
	return false
}

const postCompactionHeadChars = 256

// isPostCompactionContinuation reports whether messages[0] opens with a known client
// post-compaction continuation summary preamble (explicit sid: dual-locked with rollover_compaction).
func isPostCompactionContinuation(messages []map[string]any) bool {
	if len(messages) == 0 || messages[0] == nil {
		return false
	}
	if role, _ := messages[0]["role"].(string); role != "user" {
		return false
	}
	raw := strings.TrimSpace(volatileFirstTurnTagRe.ReplaceAllString(extractMessagePlainTextForAnchor(messages[0]), ""))
	if raw == "" {
		return false
	}
	head := raw
	if len(head) > postCompactionHeadChars {
		head = head[:postCompactionHeadChars]
	}
	head = strings.ToLower(strings.TrimSpace(head))
	return strings.HasPrefix(head, "this session is being continued from a previous conversation") ||
		strings.HasPrefix(head, "the conversation history before this point was compacted into the following summary:") ||
		strings.HasPrefix(head, "another language model started to solve this problem and produced a summary") ||
		strings.HasPrefix(head, "[context compaction]") ||
		strings.HasPrefix(head, "this task is being continued from a previous conversation")
}

const (
	anchorMinBytes    = 48
	anchorMaxBytes    = 112
	anchorMaxTotal    = 8
	anchorMinDistinct = 16
	maxCorpusBytes    = 2 * 1024 * 1024
)

type sessionContentAnchors struct {
	Anchors      []string
	PrimaryCount int // leading Anchors[0:PrimaryCount] come from messages[0]
}

func normalizeAnchorText(raw string) string {
	if raw == "" {
		return ""
	}
	if len(raw) > maxCorpusBytes {
		end := maxCorpusBytes
		for end > 0 && !utf8.RuneStart(raw[end]) {
			end--
		}
		raw = raw[:end]
	}
	var b strings.Builder
	b.Grow(min(len(raw), 4096))
	prevSpace := true
	for _, r := range raw {
		if unicode.IsSpace(r) {
			if !prevSpace {
				b.WriteByte(' ')
				prevSpace = true
			}
			continue
		}
		if r >= 'A' && r <= 'Z' {
			b.WriteByte(byte(r + ('a' - 'A')))
		} else {
			b.WriteRune(unicode.ToLower(r))
		}
		prevSpace = false
	}
	out := b.String()
	return strings.TrimSuffix(out, " ")
}

func containsSensitiveAnchorPattern(norm string) bool {
	for _, pat := range []string{
		"begin private key",
		"begin rsa private key",
		"begin openssh private key",
		"bearer ",
		"sk-",
		"aiza",
		"ghp_",
		"gho_",
		"xoxb-",
		"xoxp-",
		"api_key",
		"apikey",
		"password",
		"secret",
		"token=",
	} {
		if strings.Contains(norm, pat) {
			return true
		}
	}
	return false
}

func hasSufficientAnchorEntropy(norm string) bool {
	if len(norm) < anchorMinBytes {
		return false
	}
	seen := make(map[rune]struct{}, anchorMinDistinct)
	for _, r := range norm {
		seen[r] = struct{}{}
		if len(seen) >= anchorMinDistinct {
			return true
		}
	}
	return false
}

func trimUTF8PrefixBytes(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	cut := maxBytes
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return strings.TrimSpace(s[:cut])
}

func trimUTF8SuffixBytes(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	start := len(s) - maxBytes
	for start < len(s) && !utf8.RuneStart(s[start]) {
		start++
	}
	return strings.TrimSpace(s[start:])
}

func sliceAnchorWindow(norm string, fromEnd bool) string {
	norm = strings.TrimSpace(norm)
	if len(norm) < anchorMinBytes {
		return ""
	}
	var cand string
	if fromEnd {
		cand = trimUTF8SuffixBytes(norm, anchorMaxBytes)
	} else {
		cand = trimUTF8PrefixBytes(norm, anchorMaxBytes)
	}
	if len(cand) < anchorMinBytes || containsSensitiveAnchorPattern(cand) || !hasSufficientAnchorEntropy(cand) {
		return ""
	}
	return cand
}

func extractMessagePlainTextForAnchor(m map[string]any) string {
	if m == nil {
		return ""
	}
	role, _ := m["role"].(string)
	if role != "user" && role != "assistant" {
		return ""
	}
	var parts []string
	collectBlock := func(b map[string]any) {
		if b == nil {
			return
		}
		t, _ := b["type"].(string)
		if t == "text" || t == "input_text" || t == "output_text" {
			if txt, _ := b["text"].(string); strings.TrimSpace(txt) != "" {
				parts = append(parts, txt)
			}
		}
	}
	switch c := m["content"].(type) {
	case string:
		if strings.TrimSpace(c) != "" {
			parts = append(parts, c)
		}
	case []any:
		for _, item := range c {
			if b, ok := item.(map[string]any); ok {
				collectBlock(b)
			}
		}
	case []map[string]any:
		for _, b := range c {
			collectBlock(b)
		}
	}
	return strings.Join(parts, "\n")
}

// extractSessionContentAnchors builds up to 8 lightweight, non-persisted normalized text anchors
// sampled across messages[0] (including <summary> inner body for 2nd/3rd Pi compactions), early
// turns, midpoint turns, and pre-tail turns so Pi's keepRecentTokens tail truncation does not
// break unique pre-compaction target matching.
func extractSessionContentAnchors(messages []map[string]any, lastAssistantText string) sessionContentAnchors {
	var out sessionContentAnchors
	if len(messages) == 0 {
		return out
	}
	seen := make(map[string]bool, anchorMaxTotal)
	addAnchor := func(a string, primary bool) bool {
		if a == "" || seen[a] || len(out.Anchors) >= anchorMaxTotal {
			return false
		}
		seen[a] = true
		out.Anchors = append(out.Anchors, a)
		if primary {
			out.PrimaryCount++
		}
		return true
	}

	msgNorm := func(idx int) string {
		if idx < 0 || idx >= len(messages) {
			return ""
		}
		raw := extractMessagePlainTextForAnchor(messages[idx])
		if raw == "" {
			return ""
		}
		raw = volatileFirstTurnTagRe.ReplaceAllString(raw, "")
		return normalizeAnchorText(raw)
	}

	// Primary anchors from messages[0]
	if norm0 := msgNorm(0); norm0 != "" {
		if sIdx := strings.Index(norm0, "<summary>"); sIdx >= 0 {
			inner := norm0[sIdx+len("<summary>"):]
			if eIdx := strings.Index(inner, "</summary>"); eIdx >= 0 {
				inner = inner[:eIdx]
			}
			inner = strings.TrimSpace(inner)
			addAnchor(sliceAnchorWindow(inner, false), true)
			if len(inner) > 260 {
				midStart := len(inner) / 2
				for midStart < len(inner) && !utf8.RuneStart(inner[midStart]) {
					midStart++
				}
				addAnchor(sliceAnchorWindow(inner[midStart:], false), true)
			}
		} else {
			addAnchor(sliceAnchorWindow(norm0, false), true)
			if len(norm0) > 180 {
				addAnchor(sliceAnchorWindow(norm0, true), true)
			}
		}
	}

	// Early turns: outward scan up to 32 messages (up to 2 anchors) so tool-heavy turns still yield text anchors.
	earlyAdded := 0
	for i := 1; i < len(messages) && i <= 32 && earlyAdded < 2 && len(out.Anchors) < anchorMaxTotal; i++ {
		if a := sliceAnchorWindow(msgNorm(i), false); addAnchor(a, false) {
			earlyAdded++
		}
	}

	// Midpoint turns around len/2 (outside keepRecentTokens, outward up to ±16, up to 2 anchors).
	if len(messages) >= 4 && len(out.Anchors) < anchorMaxTotal {
		mid := len(messages) / 2
		midAdded := 0
		for d := 0; d <= 16 && midAdded < 2 && len(out.Anchors) < anchorMaxTotal; d++ {
			for _, idx := range []int{mid + d, mid - d} {
				if idx <= 0 || idx >= len(messages) || (d == 0 && idx != mid) {
					continue
				}
				if a := sliceAnchorWindow(msgNorm(idx), false); addAnchor(a, false) {
					midAdded++
					if midAdded >= 2 {
						break
					}
				}
			}
		}
	}

	// Pre-tail turns: scan backward from len-2 down to max(1, len-33) (up to 2 anchors).
	tailAdded := 0
	minTail := len(messages) - 33
	if minTail < 1 {
		minTail = 1
	}
	for i := len(messages) - 2; i >= minTail && tailAdded < 2 && len(out.Anchors) < anchorMaxTotal; i-- {
		if a := sliceAnchorWindow(msgNorm(i), false); addAnchor(a, false) {
			tailAdded++
		}
	}

	// Fallback to lastAssistantText when anchors are still sparse
	if len(out.Anchors) < 4 && strings.TrimSpace(lastAssistantText) != "" {
		normAsst := normalizeAnchorText(volatileFirstTurnTagRe.ReplaceAllString(lastAssistantText, ""))
		addAnchor(sliceAnchorWindow(normAsst, false), false)
	}
	return out
}

// allMessageTextBlocks returns all non-empty text blocks in a message joined by "\n"
// with system reminders removed.
func allMessageTextBlocks(msg map[string]any) string {
	if msg == nil {
		return ""
	}
	switch c := msg["content"].(type) {
	case string:
		return strings.TrimSpace(anySystemReminderRe.ReplaceAllString(c, ""))
	case []any:
		var parts []string
		for _, elem := range c {
			b, ok := elem.(map[string]any)
			if !ok {
				continue
			}
			if t, _ := b["type"].(string); t == "text" || t == "input_text" || t == "output_text" {
				raw, _ := b["text"].(string)
				if text := strings.TrimSpace(anySystemReminderRe.ReplaceAllString(raw, "")); text != "" {
					parts = append(parts, text)
				}
			}
		}
		return strings.Join(parts, "\n")
	case []map[string]any:
		var parts []string
		for _, b := range c {
			if t, _ := b["type"].(string); t == "text" || t == "input_text" || t == "output_text" {
				raw, _ := b["text"].(string)
				if text := strings.TrimSpace(anySystemReminderRe.ReplaceAllString(raw, "")); text != "" {
					parts = append(parts, text)
				}
			}
		}
		return strings.Join(parts, "\n")
	}
	return ""
}

// extractCompactionSearchCorpus extracts the normalized search corpus from a compaction request,
// preferring <previous-summary> before <conversation> so large >2MB round-2 compactions never
// truncate previous-summary primary anchors, and slicing strictly on the lowered buffer to avoid
// Unicode case-folding byte-offset panics.
func extractCompactionSearchCorpus(messages []map[string]any) string {
	if len(messages) == 0 {
		return ""
	}
	last := messages[len(messages)-1]
	tailRaw := ""
	if role, _ := last["role"].(string); role == "user" {
		tailRaw = allMessageTextBlocks(last)
	}
	if tailRaw != "" {
		low := strings.ToLower(tailRaw)
		var spans []string
		extractTag := func(openTag, closeTag string) {
			pos := 0
			for pos < len(low) {
				o := strings.Index(low[pos:], openTag)
				if o < 0 {
					break
				}
				start := pos + o + len(openTag)
				if c := strings.Index(low[start:], closeTag); c >= 0 {
					spans = append(spans, low[start:start+c])
					pos = start + c + len(closeTag)
				} else {
					spans = append(spans, low[start:])
					break
				}
			}
		}
		extractTag("<previous-summary>", "</previous-summary>")
		extractTag("<conversation>", "</conversation>")
		if len(spans) > 0 {
			return normalizeAnchorText(strings.Join(spans, "\n"))
		}
	}
	if len(messages) == 1 {
		return normalizeAnchorText(tailRaw)
	}
	// Multi-message compaction without inline tags: include earlier messages (starting with messages[0]
	// so primary anchors are never truncated by maxCorpusBytes) plus the tail prompt.
	var b strings.Builder
	for i := 0; i < len(messages)-1 && b.Len() < maxCorpusBytes; i++ {
		if txt := allMessageTextBlocks(messages[i]); txt != "" {
			if b.Len() > 0 {
				b.WriteByte('\n')
			}
			b.WriteString(txt)
		}
	}
	if tailRaw != "" && b.Len() < maxCorpusBytes {
		if b.Len() > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(tailRaw)
	}
	return normalizeAnchorText(b.String())
}

// lastUserTextBlocks returns all non-empty text/input_text blocks of a trailing user message
// with system reminders removed, strictly ignoring tool_result/function_call_output blocks.
func lastUserTextBlocks(messages []map[string]any) []string {
	if len(messages) == 0 {
		return nil
	}
	last := messages[len(messages)-1]
	if role, _ := last["role"].(string); role != "user" {
		return nil
	}
	var out []string
	switch c := last["content"].(type) {
	case string:
		if text := strings.TrimSpace(anySystemReminderRe.ReplaceAllString(c, "")); text != "" {
			out = append(out, text)
		}
	case []any:
		for _, item := range c {
			b, ok := item.(map[string]any)
			if !ok {
				continue
			}
			if t, _ := b["type"].(string); t == "text" || t == "input_text" {
				raw, _ := b["text"].(string)
				if text := strings.TrimSpace(anySystemReminderRe.ReplaceAllString(raw, "")); text != "" {
					out = append(out, text)
				}
			}
		}
	case []map[string]any:
		for _, b := range c {
			if t, _ := b["type"].(string); t == "text" || t == "input_text" {
				raw, _ := b["text"].(string)
				if text := strings.TrimSpace(anySystemReminderRe.ReplaceAllString(raw, "")); text != "" {
					out = append(out, text)
				}
			}
		}
	}
	return out
}

// extractCompactionTailText scans sibling text/input_text blocks of a trailing role=="user" message
// (never tool_result.content), locates the last block matching a compaction instruction, and joins
// that block through the end of the message's text blocks (preserving trailing focus directives while
// never re-injecting pre-existing user prose before the compaction block).
func extractCompactionTailText(messages []map[string]any) string {
	blocks := lastUserTextBlocks(messages)
	if len(blocks) == 0 {
		return ""
	}
	hasToolResult := lastMessageHasToolResult(messages)
	for i := len(blocks) - 1; i >= 0; i-- {
		if matchesCompactionInstructionBlock(blocks[i], hasToolResult) {
			return strings.Join(blocks[i:], "\n\n")
		}
	}
	return ""
}

// lastUserTextBlock returns the final non-empty text block of a trailing user message with
// system reminders removed, or "" when the conversation does not end with user text.
func lastUserTextBlock(messages []map[string]any) string {
	blocks := lastUserTextBlocks(messages)
	if len(blocks) == 0 {
		return ""
	}
	return blocks[len(blocks)-1]
}

func isFailedErrorTurnMessage(m map[string]any) bool {
	role, _ := m["role"].(string)
	if role != "assistant" {
		return false
	}
	txt := strings.TrimSpace(formatSingleMessageText(m))
	if strings.HasPrefix(txt, "⚠ Agent execution terminated due to error") {
		return true
	}
	if _, ok := parseDynamicTerminalErrorLine(txt, ""); ok {
		return true
	}
	return isStatusMarker(txt) && DetectAntigravitySoftError(txt) != nil
}

func buildPromptBlocks(unseenMessages []map[string]any, isRollover bool, allMessages []map[string]any, maxChars int) []map[string]any {
	blocks, _ := resolveAndBuildPromptBlocks(context.Background(), GlobalImageFetcher, unseenMessages, isRollover, allMessages, maxChars, nil)
	return blocks
}

func estimateConversationChars(sysText string, messages []map[string]any) int {
	total := len(sysText)
	for _, m := range messages {
		total += len(formatSingleMessageText(m))
		if total >= 16000 {
			return total
		}
	}
	return total
}

func isBackgroundUtilityCall(sysText string, tools []ToolDef, body map[string]any) bool {
	return isBackgroundUtilityCallWithContext(nil, sysText, tools, nil, body)
}

func isBackgroundUtilityCallWithContext(r *http.Request, sysText string, tools []ToolDef, messages []map[string]any, body map[string]any) bool {
	if maxTok, ok := toInt(body["max_tokens"]); ok && maxTok == 1 {
		return true
	}
	if sug, spin := detectClientUtilityProbe(messages); sug || spin {
		return true
	}
	if isStandaloneCompactionSummary(sysText, tools, messages) {
		return false
	}
	if len(messages) >= 3 || estimateConversationChars(sysText, messages) >= 16000 {
		return false
	}
	if len(tools) == 0 && !strings.Contains(sysText, "Primary working directory") && isClaudeCodeClient(r, body) {
		return true
	}
	return false
}

const (
	suggestionProbeMarker = "[SUGGESTION MODE:"
	spinnerProbeMarker    = "Describe your most recent action in 3-5 words"
	// Claude Code's spinner probe is about 1-2K chars; longer blocks are conversation content.
	spinnerProbeMaxChars = 4000

	// Ephemeral sidecar classifiers sent by clients with the parent's explicit session ID
	// and 0 tools; isolating them into util: prevents them from evicting warm interactive workers.
	permissionClassifierPrefix = "You are a security monitor for autonomous AI coding agents."
	sessionNamingPrefix        = "You are naming a coding session so the user can pick it out of a long list of sessions."
	dshSessionTitlePrefix      = "Create a concise title for an AI coding-assistant session"

	// Pi standalone bypass compaction summary prompt (single-turn, 0 tools, normal sid:/fp: key,
	// immediately hibernated after completion rather than isolated into util:).
	piCompactionSummaryPrefix = "You are a context summarization assistant."
)

// hasSystemLeadingBlockPrefix checks whether sysText starts with prefix after trimming,
// or whether any of the first 4 non-empty "\n\n"-separated blocks starts with prefix
// (handling /v1/responses instructions + "\n\n" + developer messages).
func hasSystemLeadingBlockPrefix(sysText, prefix string) bool {
	if prefix == "" {
		return false
	}
	trimmed := strings.TrimSpace(sysText)
	if trimmed == "" {
		return false
	}
	if strings.HasPrefix(trimmed, prefix) {
		return true
	}
	rest := trimmed
	seen := 0
	for rest != "" && seen < 4 {
		var blk string
		if idx := strings.Index(rest, "\n\n"); idx >= 0 {
			blk = strings.TrimSpace(rest[:idx])
			rest = strings.TrimSpace(rest[idx+2:])
		} else {
			blk = strings.TrimSpace(rest)
			rest = ""
		}
		if blk == "" {
			continue
		}
		seen++
		if strings.HasPrefix(blk, prefix) {
			return true
		}
	}
	return false
}

func hasSystemLeadingBlockPrefixFold(sysText, lowerPrefix string) bool {
	if lowerPrefix == "" {
		return false
	}
	trimmed := strings.TrimSpace(sysText)
	if trimmed == "" {
		return false
	}
	if strings.HasPrefix(strings.ToLower(trimmed), lowerPrefix) {
		return true
	}
	rest := trimmed
	seen := 0
	for rest != "" && seen < 4 {
		var blk string
		if idx := strings.Index(rest, "\n\n"); idx >= 0 {
			blk = strings.TrimSpace(rest[:idx])
			rest = strings.TrimSpace(rest[idx+2:])
		} else {
			blk = strings.TrimSpace(rest)
			rest = ""
		}
		if blk == "" {
			continue
		}
		seen++
		if strings.HasPrefix(strings.ToLower(blk), lowerPrefix) {
			return true
		}
	}
	lineRest := trimmed
	lineSeen := 0
	for lineRest != "" && lineSeen < 4 {
		var line string
		if idx := strings.IndexByte(lineRest, '\n'); idx >= 0 {
			line = strings.TrimSpace(lineRest[:idx])
			lineRest = lineRest[idx+1:]
		} else {
			line = strings.TrimSpace(lineRest)
			lineRest = ""
		}
		if line == "" {
			continue
		}
		lineSeen++
		if strings.HasPrefix(strings.ToLower(line), lowerPrefix) {
			return true
		}
	}
	return false
}

// isStandaloneCompactionSummary reports whether a 0-tool single-user-message request is a
// standalone Pi context compaction summary turn (initial, update with <previous-summary>,
// turn-prefix, or branch summary). Intentionally excluded from isEphemeralBackgroundClassifier
// so it keeps normal sid:/fp: admission and DB persistence while hibernating immediately upon
// clean completion.
func isStandaloneCompactionSummary(sysText string, tools []ToolDef, messages []map[string]any) bool {
	if len(tools) > 0 || len(messages) != 1 || messagesContainToolBlocks(messages) {
		return false
	}
	if !hasSystemLeadingBlockPrefixFold(sysText, strings.ToLower(piCompactionSummaryPrefix)) {
		return false
	}
	if role, _ := messages[0]["role"].(string); role != "user" {
		return false
	}
	text := strings.TrimSpace(allMessageTextBlocks(messages[0]))
	if text == "" {
		return false
	}
	head := text
	if len(head) > 1024 {
		head = head[:1024]
	}
	lowHead := strings.ToLower(head)
	if strings.HasPrefix(lowHead, "<conversation>") || strings.HasPrefix(lowHead, "<previous-summary>") ||
		strings.Contains(lowHead, "<conversation>") || strings.Contains(lowHead, "<previous-summary>") {
		return true
	}
	tail := text
	if len(tail) > 4096 {
		tail = tail[len(tail)-4096:]
	}
	lowTail := strings.ToLower(tail)
	return strings.Contains(lowTail, "</conversation>") ||
		strings.Contains(lowTail, "</previous-summary>") ||
		strings.Contains(lowTail, "create a structured context checkpoint summary") ||
		strings.Contains(lowTail, "update the existing structured summary") ||
		strings.Contains(lowTail, "update the previous summary")
}

// isSessionNamingUtility reports whether a 0-tool short request is a background session-title
// generator whose implicit thinking tier can safely be capped to "low".
// Deliberately excludes permissionClassifierPrefix so security monitors retain their tier.
func isSessionNamingUtility(sysText string, tools []ToolDef, messages []map[string]any) bool {
	if len(tools) > 0 || len(messages) == 0 || len(messages) > 4 || messagesContainToolBlocks(messages) {
		return false
	}
	return hasSystemLeadingBlockPrefix(sysText, sessionNamingPrefix) ||
		hasSystemLeadingBlockPrefix(sysText, dshSessionTitlePrefix)
}

// isEphemeralBackgroundClassifier reports whether a 0-tool short request is a known client
// sidecar classifier (e.g. automatic-mode permission review or session-title naming) that
// carries the parent conversation's explicit session ID despite being single-turn and disposable.
func isEphemeralBackgroundClassifier(sysText string, tools []ToolDef, messages []map[string]any) bool {
	if len(tools) > 0 || len(messages) == 0 || len(messages) > 4 || messagesContainToolBlocks(messages) {
		return false
	}
	return hasSystemLeadingBlockPrefix(sysText, permissionClassifierPrefix) ||
		hasSystemLeadingBlockPrefix(sysText, sessionNamingPrefix) ||
		hasSystemLeadingBlockPrefix(sysText, dshSessionTitlePrefix)
}

var anySystemReminderRe = regexp.MustCompile(`(?s)<system-reminder>.*?</system-reminder>`)

// detectClientUtilityProbe recognises Claude Code's prompt-suggestion and spinner-summary probes.
// Only the final non-empty text block of a trailing user message counts and it must START with
// the marker, so a conversation that quotes these strings anywhere else is never answered locally.
// The spinner probe puts recent context first and its instruction on the final line, so for it
// the final line of a short block may also carry the marker.
func detectClientUtilityProbe(messages []map[string]any) (suggestion, spinner bool) {
	text := lastUserTextBlock(messages)
	spinner = strings.HasPrefix(text, spinnerProbeMarker)
	if !spinner && len(text) <= spinnerProbeMaxChars {
		lastLine := text
		if i := strings.LastIndexByte(text, '\n'); i >= 0 {
			lastLine = strings.TrimSpace(text[i+1:])
		}
		spinner = strings.HasPrefix(lastLine, spinnerProbeMarker)
	}
	return strings.HasPrefix(text, suggestionProbeMarker), spinner
}

// isClaudeCodeClient reports whether the request carries Claude Code's (or the Agent SDK's)
// identity. A bare max_tokens=1 request is only treated as a quota probe for these clients;
// for anyone else it is a legitimate one-token request and goes to the model.
func isClaudeCodeClient(r *http.Request, body map[string]any) bool {
	sys := rawSystemText(body["system"])
	if claudeCliIdentRe.MatchString(sys) || claudeSdkIdentRe.MatchString(sys) {
		return true
	}
	if r != nil && strings.HasPrefix(strings.ToLower(r.Header.Get("User-Agent")), "claude-cli/") {
		return true
	}
	return false
}

var userIDSessionUUIDRe = regexp.MustCompile(`(?i)session_([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})`)

func extractExplicitSessionAndAgent(r *http.Request, body map[string]any) (string, string) {
	var explicit, agentID string
	if r != nil {
		for _, hdr := range []string{
			"x-claude-code-session-id",
			"x-pi-session-id",
			"x-session-id",
			"session-id",
			"session_id",
			"thread-id",
			"x-conversation-id",
			"conversation_id",
			"openai-conversation-id",
			"x-client-session-id",
		} {
			if v := strings.TrimSpace(r.Header.Get(hdr)); v != "" {
				explicit = v
				break
			}
		}
		for _, ahdr := range []string{"x-claude-code-agent-id", "x-agent-id", "x-subagent-id"} {
			if v := strings.TrimSpace(r.Header.Get(ahdr)); v != "" {
				agentID = v
				break
			}
		}
	}
	if explicit == "" {
		if pck, ok := body["prompt_cache_key"].(string); ok && strings.TrimSpace(pck) != "" {
			explicit = strings.TrimSpace(pck)
		}
	}
	if meta, ok := body["metadata"].(map[string]any); ok {
		if explicit == "" {
			if s, okS := meta["session_id"].(string); okS && strings.TrimSpace(s) != "" {
				explicit = strings.TrimSpace(s)
			} else if c, okC := meta["conversation_id"].(string); okC && strings.TrimSpace(c) != "" {
				explicit = strings.TrimSpace(c)
			}
		}
		if agentID == "" {
			if a, okA := meta["agent_id"].(string); okA && strings.TrimSpace(a) != "" {
				agentID = strings.TrimSpace(a)
			}
		}
		if explicit == "" {
			if uidStr, okU := meta["user_id"].(string); okU && strings.TrimSpace(uidStr) != "" {
				uidStr = strings.TrimSpace(uidStr)
				if strings.HasPrefix(uidStr, "{") {
					var parsedUID map[string]any
					if json.Unmarshal([]byte(uidStr), &parsedUID) == nil {
						if sid, okS := parsedUID["session_id"].(string); okS && strings.TrimSpace(sid) != "" {
							explicit = strings.TrimSpace(sid)
						}
						if agentID == "" {
							if aid, okA := parsedUID["agent_id"].(string); okA && strings.TrimSpace(aid) != "" {
								agentID = strings.TrimSpace(aid)
							}
						}
					}
				}
				if explicit == "" {
					if m := userIDSessionUUIDRe.FindStringSubmatch(uidStr); len(m) > 1 {
						explicit = m[1]
					}
				}
			}
		}
	}
	if explicit == "" {
		if usrStr, okU := body["user"].(string); okU && strings.TrimSpace(usrStr) != "" {
			if m := userIDSessionUUIDRe.FindStringSubmatch(strings.TrimSpace(usrStr)); len(m) > 1 {
				explicit = m[1]
			}
		}
	}
	return explicit, agentID
}

func messagesContainToolBlocks(messages []map[string]any) bool {
	for _, m := range messages {
		if role, _ := m["role"].(string); role == "tool" {
			return true
		}
		if arr, ok := m["content"].([]any); ok {
			for _, item := range arr {
				if bm, okM := item.(map[string]any); okM {
					if t, _ := bm["type"].(string); t == "tool_use" || t == "tool_result" {
						return true
					}
				}
			}
		}
	}
	return false
}

func deriveConversationKey(r *http.Request, body map[string]any, sysText string, tools []ToolDef, messages []map[string]any) string {
	return deriveConversationKeyWithFirstFp(r, body, sysText, tools, messages, "")
}

func deriveConversationKeyWithFirstFp(r *http.Request, body map[string]any, sysText string, tools []ToolDef, messages []map[string]any, firstUserFp string) string {
	utilKeyFromPrompt := func() string {
		var firstMsgBytes []byte
		if len(messages) > 0 {
			firstMsgBytes, _ = json.Marshal(messages[0])
		}
		return "util:" + shortSha256(truncateRunes(sysText, 512)+"||"+truncateRunes(string(firstMsgBytes), 512))
	}
	if r != nil {
		reqClass := strings.ToLower(strings.TrimSpace(r.Header.Get("x-claude-code-request-class")))
		if reqClass == "utility" || reqClass == "background" || reqClass == "title" || reqClass == "suggestion" {
			return utilKeyFromPrompt()
		}
	}
	if maxTok, ok := toInt(body["max_tokens"]); ok && maxTok == 1 && !messagesContainToolBlocks(messages) {
		return utilKeyFromPrompt()
	}

	firstFp := firstUserFp
	if firstFp == "" {
		firstFp = computeFirstUserFingerprint(messages)
	}
	if isEphemeralBackgroundClassifier(sysText, tools, messages) {
		return "util:" + shortSha256(truncateRunes(sysText, 256)+"||"+firstFp+"||"+shortSha256(lastUserTextBlock(messages)))
	}
	if len(firstFp) > 8 {
		firstFp = firstFp[:8]
	}

	explicit, agentID := extractExplicitSessionAndAgent(r, body)
	if explicit != "" {
		if agentID != "" {
			return fmt.Sprintf("sid:%s:ag:%s:u:%s", explicit, agentID, firstFp)
		}
		return fmt.Sprintf("sid:%s:u:%s", explicit, firstFp)
	}

	if isBackgroundUtilityCallWithContext(r, sysText, tools, messages, body) && !messagesContainToolBlocks(messages) {
		return utilKeyFromPrompt()
	}

	sysPrefix := sysText
	if len(sysPrefix) > 4096 {
		sysPrefix = sysPrefix[:4096]
	}
	if agentID != "" {
		return fmt.Sprintf("fp:%s:ag:%s:u:%s", shortSha256(sysPrefix), agentID, firstFp)
	}
	return fmt.Sprintf("fp:%s:u:%s", shortSha256(sysPrefix), firstFp)
}

type TurnToolChoice struct {
	Mode               string
	Allowed            map[string]bool
	AllowedOrdered     []string
	RestrictiveAllowed bool // true when originating from type=="allowed_tools" (must fail closed if 0 declared tools match)
}

type TurnPromptDirective struct {
	ToolChoice       TurnToolChoice
	StructuredOutput string
}

type turnDirectiveContextKey struct{}

func withTurnDirective(ctx context.Context, dir TurnPromptDirective) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, turnDirectiveContextKey{}, dir)
}

func turnDirectiveFromContext(ctx context.Context) TurnPromptDirective {
	if ctx == nil {
		return TurnPromptDirective{}
	}
	dir, _ := ctx.Value(turnDirectiveContextKey{}).(TurnPromptDirective)
	return dir
}

type standaloneCompactionContextKey struct{}

func withStandaloneCompaction(ctx context.Context, v bool) context.Context {
	if !v {
		return ctx
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, standaloneCompactionContextKey{}, true)
}

func isStandaloneCompactionFromContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v, _ := ctx.Value(standaloneCompactionContextKey{}).(bool)
	return v
}

func (d TurnPromptDirective) IsEmpty() bool {
	return (d.ToolChoice.Mode == "" || d.ToolChoice.Mode == "auto") && len(d.ToolChoice.Allowed) == 0 && d.StructuredOutput == ""
}

func (d TurnPromptDirective) FingerprintToken() string {
	if d.IsEmpty() {
		return ""
	}
	return d.ToolChoice.Mode + "|" + strings.Join(d.ToolChoice.AllowedOrdered, ",") + "|" + d.StructuredOutput
}

func parseClientToolChoice(raw any) TurnToolChoice {
	if raw == nil {
		return TurnToolChoice{}
	}
	addAllowed := func(tc *TurnToolChoice, name string) {
		n := strings.TrimSpace(name)
		if n == "" {
			return
		}
		if tc.Allowed == nil {
			tc.Allowed = make(map[string]bool)
		}
		if !tc.Allowed[n] {
			tc.Allowed[n] = true
			tc.AllowedOrdered = append(tc.AllowedOrdered, n)
		}
	}
	switch v := raw.(type) {
	case string:
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "none":
			return TurnToolChoice{Mode: "none"}
		case "any", "required":
			return TurnToolChoice{Mode: "required"}
		default:
			return TurnToolChoice{}
		}
	case map[string]any:
		typ := strings.ToLower(strings.TrimSpace(stringField(v, "type", "")))
		switch typ {
		case "none":
			return TurnToolChoice{Mode: "none"}
		case "auto":
			return TurnToolChoice{}
		case "any", "required":
			return TurnToolChoice{Mode: "required"}
		case "tool", "function", "custom":
			var tc TurnToolChoice
			tc.Mode = "tool"
			name := strings.TrimSpace(stringField(v, "name", ""))
			if name == "" {
				if fn, ok := v["function"].(map[string]any); ok {
					name = strings.TrimSpace(stringField(fn, "name", ""))
				}
			}
			if name == "" {
				if cu, ok := v["custom"].(map[string]any); ok {
					name = strings.TrimSpace(stringField(cu, "name", ""))
				}
			}
			addAllowed(&tc, name)
			if len(tc.AllowedOrdered) == 0 {
				return TurnToolChoice{Mode: "required"}
			}
			return tc
		case "allowed_tools":
			var tc TurnToolChoice
			mode := strings.ToLower(strings.TrimSpace(stringField(v, "mode", "")))
			if mode == "none" {
				return TurnToolChoice{Mode: "none"}
			}
			collectToolEntry := func(item any) {
				if tm, okM := item.(map[string]any); okM {
					n := strings.TrimSpace(stringField(tm, "name", ""))
					if n == "" {
						if fn, okF := tm["function"].(map[string]any); okF {
							n = strings.TrimSpace(stringField(fn, "name", ""))
						}
					}
					addAllowed(&tc, n)
				} else if s, okS := item.(string); okS {
					addAllowed(&tc, s)
				}
			}
			switch arr := v["tools"].(type) {
			case []any:
				for _, item := range arr {
					collectToolEntry(item)
				}
			case []string:
				for _, s := range arr {
					addAllowed(&tc, s)
				}
			case []map[string]any:
				for _, tm := range arr {
					collectToolEntry(tm)
				}
			}
			// Fail closed (OpenAI Responses & Gemini spec): allowed_tools with 0 resolved tools
			// must NEVER fall open to allow-all or required-without-filter.
			if len(tc.AllowedOrdered) == 0 {
				return TurnToolChoice{Mode: "none"}
			}
			sort.Strings(tc.AllowedOrdered)
			tc.RestrictiveAllowed = true
			if mode == "required" || mode == "any" {
				if len(tc.AllowedOrdered) == 1 {
					tc.Mode = "tool"
				} else {
					tc.Mode = "required"
				}
			} else {
				tc.Mode = "allowed"
			}
			return tc
		}
	}
	return TurnToolChoice{}
}

// reconcileToolChoiceWithCatalog intersects an allowed_tools allowlist against the turn's declared
// tool catalog, failing closed to TurnToolChoice{Mode: "none"} whenever 0 declared tools match.
func reconcileToolChoiceWithCatalog(tc TurnToolChoice, declared []ToolDef) TurnToolChoice {
	if !tc.RestrictiveAllowed {
		return tc
	}
	if len(tc.AllowedOrdered) == 0 || len(declared) == 0 {
		return TurnToolChoice{Mode: "none"}
	}
	declaredByName := make(map[string]string, len(declared)*2)
	for _, t := range declared {
		if isInternalBridgeTool(t.Name) {
			continue
		}
		if t.Name != "" {
			declaredByName[t.Name] = t.Name
		}
		if out := t.OutboundName(); out != "" {
			declaredByName[out] = t.Name
		}
	}
	if len(declaredByName) == 0 {
		return TurnToolChoice{Mode: "none"}
	}
	matched := TurnToolChoice{
		Mode:               tc.Mode,
		RestrictiveAllowed: true,
	}
	for _, name := range tc.AllowedOrdered {
		canonical, ok := declaredByName[name]
		if !ok {
			continue
		}
		if matched.Allowed == nil {
			matched.Allowed = make(map[string]bool)
		}
		if !matched.Allowed[canonical] {
			matched.Allowed[canonical] = true
			matched.AllowedOrdered = append(matched.AllowedOrdered, canonical)
		}
	}
	if len(matched.AllowedOrdered) == 0 {
		return TurnToolChoice{Mode: "none"}
	}
	sort.Strings(matched.AllowedOrdered)
	if matched.Mode == "required" && len(matched.AllowedOrdered) == 1 {
		matched.Mode = "tool"
	}
	return matched
}

func parseClientToolChoiceWithDeclared(raw any, declared []ToolDef) TurnToolChoice {
	tc := parseClientToolChoice(raw)
	if tc.RestrictiveAllowed {
		return reconcileToolChoiceWithCatalog(tc, declared)
	}
	return tc
}

func sanitizeStructuredOutputSchema(node any) any {
	switch v := node.(type) {
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = sanitizeStructuredOutputSchema(item)
		}
		return out
	case []map[string]any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = sanitizeStructuredOutputSchema(item)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, val := range v {
			switch k {
			case "enum", "const", "required":
				out[k] = val
			case "properties", "$defs", "definitions", "patternProperties", "dependentSchemas":
				if subMap, okM := val.(map[string]any); okM {
					cleanSub := make(map[string]any, len(subMap))
					for propKey, propSchema := range subMap {
						cleanSub[propKey] = sanitizeStructuredOutputSchema(propSchema)
					}
					out[k] = cleanSub
				} else {
					out[k] = sanitizeStructuredOutputSchema(val)
				}
			case "description", "title":
				if s, okS := val.(string); okS {
					out[k] = strings.TrimSpace(SanitizeOutboundText(s))
				} else {
					out[k] = sanitizeStructuredOutputSchema(val)
				}
			default:
				out[k] = sanitizeStructuredOutputSchema(val)
			}
		}
		return out
	default:
		return node
	}
}

func formatStructuredOutputSchemaHint(fmtMap map[string]any) string {
	if fmtMap == nil {
		return ""
	}
	fType := strings.ToLower(strings.TrimSpace(stringField(fmtMap, "type", "")))
	switch fType {
	case "json_object":
		return "[Structured Output Requirement: Respond with a single valid raw JSON object only (no markdown code fences, no preamble, and no trailing explanation).]"
	case "json_schema":
		var schema any
		if js, ok := fmtMap["json_schema"].(map[string]any); ok {
			schema = js["schema"]
			if schema == nil {
				schema = js["json_schema"]
			}
		}
		if schema == nil {
			schema = fmtMap["schema"]
		}
		if schema != nil {
			if sb, err := json.Marshal(sanitizeStructuredOutputSchema(schema)); err == nil && len(sb) > 0 && string(sb) != "null" {
				return "[Structured Output Requirement: Respond ONLY with valid raw JSON (no markdown code fences, no preamble, and no trailing explanation) strictly conforming to this JSON Schema: " + string(sb) + "]"
			}
		}
		return "[Structured Output Requirement: Respond ONLY with valid raw JSON (no markdown code fences, no preamble, and no trailing explanation).]"
	}
	return ""
}

func extractStructuredOutputInstruction(body map[string]any) string {
	if body == nil {
		return ""
	}
	// 1. Anthropic /v1/messages: output_config.format or output_format
	if oc, ok := body["output_config"].(map[string]any); ok {
		if fmtMap, okF := oc["format"].(map[string]any); okF {
			if h := formatStructuredOutputSchemaHint(fmtMap); h != "" {
				return h
			}
		}
	}
	if of, ok := body["output_format"].(map[string]any); ok {
		if h := formatStructuredOutputSchemaHint(of); h != "" {
			return h
		}
	}
	// 2. OpenAI /v1/chat/completions: response_format
	switch rf := body["response_format"].(type) {
	case string:
		if strings.EqualFold(strings.TrimSpace(rf), "json_object") {
			return "[Structured Output Requirement: Respond with a single valid raw JSON object only (no markdown code fences, no preamble, and no trailing explanation).]"
		}
	case map[string]any:
		if h := formatStructuredOutputSchemaHint(rf); h != "" {
			return h
		}
	}
	// 3. OpenAI /v1/responses: text.format
	if tm, ok := body["text"].(map[string]any); ok {
		if fmtMap, okF := tm["format"].(map[string]any); okF {
			if h := formatStructuredOutputSchemaHint(fmtMap); h != "" {
				return h
			}
		}
	}
	return ""
}

func extractTurnPromptDirective(body map[string]any) TurnPromptDirective {
	if body == nil {
		return TurnPromptDirective{}
	}
	tc := parseClientToolChoice(body["tool_choice"])
	if tc.RestrictiveAllowed {
		if rawTools, hasTools := body["tools"]; hasTools {
			tc = reconcileToolChoiceWithCatalog(tc, sanitizeToolsCatalog(rawTools))
		}
	}
	return TurnPromptDirective{
		ToolChoice:       tc,
		StructuredOutput: extractStructuredOutputInstruction(body),
	}
}

func buildTurnDirectivePromptTail(dir TurnPromptDirective, toolsCatalog []ToolDef) string {
	if dir.ToolChoice.RestrictiveAllowed {
		dir.ToolChoice = reconcileToolChoiceWithCatalog(dir.ToolChoice, toolsCatalog)
	}
	var parts []string
	bridgeSrv := sessionBridgeServerName(toolsCatalog)
	byHost := make(map[string]ToolDef, len(toolsCatalog))
	for _, t := range toolsCatalog {
		byHost[t.Name] = t
	}
	formatToolRef := func(hostName string) string {
		if td, ok := byHost[hostName]; ok && td.McpName != "" {
			return fmt.Sprintf("`%s` (`mcp_%s_%s`)", td.OutboundName(), bridgeSrv, td.McpName)
		}
		return fmt.Sprintf("`%s`", NeutralizeToolIdentifier(hostName))
	}

	switch dir.ToolChoice.Mode {
	case "none":
		if len(toolsCatalog) > 0 {
			parts = append(parts, "[Turn Constraint: tool_choice=none — Do NOT invoke any tools on this turn; respond directly in assistant text.]")
		}
	case "tool":
		if len(dir.ToolChoice.AllowedOrdered) > 0 {
			name := dir.ToolChoice.AllowedOrdered[0]
			parts = append(parts, fmt.Sprintf("[Turn Constraint: tool_choice forces tool %s — You MUST invoke ONLY %s right now with arguments matching its schema, and do not call any other tool.]", formatToolRef(name), formatToolRef(name)))
		}
	case "required":
		if len(dir.ToolChoice.AllowedOrdered) > 0 {
			refs := make([]string, len(dir.ToolChoice.AllowedOrdered))
			for i, n := range dir.ToolChoice.AllowedOrdered {
				refs[i] = formatToolRef(n)
			}
			parts = append(parts, fmt.Sprintf("[Turn Constraint: tool_choice=required — You MUST invoke at least one of these allowed tools on this turn: %s.]", strings.Join(refs, ", ")))
		} else if len(toolsCatalog) > 0 {
			parts = append(parts, "[Turn Constraint: tool_choice=required — You MUST invoke at least one declared workspace tool on this turn before answering.]")
		}
	case "allowed":
		if len(dir.ToolChoice.AllowedOrdered) > 0 {
			refs := make([]string, len(dir.ToolChoice.AllowedOrdered))
			for i, n := range dir.ToolChoice.AllowedOrdered {
				refs[i] = formatToolRef(n)
			}
			parts = append(parts, fmt.Sprintf("[Turn Constraint: Only these tools are permitted on this turn: %s.]", strings.Join(refs, ", ")))
		}
	}
	if dir.StructuredOutput != "" {
		parts = append(parts, dir.StructuredOutput)
	}
	return strings.Join(parts, "\n")
}

func appendTurnDirectiveToPromptBlocks(blocks []map[string]any, tail string) []map[string]any {
	if tail == "" {
		return blocks
	}
	out := make([]map[string]any, len(blocks))
	copy(out, blocks)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i]["type"] == "text" {
			cp := make(map[string]any, len(out[i]))
			for k, v := range out[i] {
				cp[k] = v
			}
			prev, _ := cp["text"].(string)
			if strings.TrimSpace(prev) == "" {
				cp["text"] = tail
			} else {
				cp["text"] = prev + "\n\n" + tail
			}
			out[i] = cp
			return out
		}
	}
	return append(out, map[string]any{"type": "text", "text": tail})
}
