package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	claudeWebOnlyTools = map[string]bool{
		"WebSearch":    true,
		"WebFetch":     true,
		"ToolSearch":   true,
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
	}
	softErrBlockedRe = regexp.MustCompile(`(?i)^\s*The prompt could not be submitted|Generative AI Prohibited Use policy|prompt contains sensitive words`)
	softErrVerifyRe  = regexp.MustCompile(`(?im)^\s*⚠?\s*Verification Required\s*$|Verify your account to continue`)
	softErrTermRe    = regexp.MustCompile(`(?im)^\s*⚠\s*Agent execution terminated due to error`)
)

type SoftError struct {
	StatusCode int
	ErrorType  string
	Message    string
}

func (e *SoftError) Error() string { return e.Message }

func DetectAntigravitySoftError(text string) *SoftError {
	if text == "" {
		return nil
	}
	head := text
	if len(head) > 600 {
		head = head[:600]
	}
	if tosSoftRe.MatchString(text) {
		TripTosCircuitBreaker(head)
		return &SoftError{StatusCode: http.StatusServiceUnavailable, ErrorType: "tos_circuit_breaker", Message: "Antigravity ToS circuit breaker tripped: " + strings.TrimSpace(head)}
	}
	if softErrBlockedRe.MatchString(head) {
		return &SoftError{StatusCode: 400, ErrorType: "invalid_request_error", Message: "Blocked by Antigravity prompt filter: " + strings.TrimSpace(head)}
	}
	if softErrVerifyRe.MatchString(head) {
		return &SoftError{StatusCode: 503, ErrorType: "api_error", Message: "Google account verification required: " + strings.TrimSpace(head)}
	}
	if softErrTermRe.MatchString(head) {
		return &SoftError{StatusCode: 429, ErrorType: "rate_limit_error", Message: "Antigravity upstream rate limit or transient failure: " + strings.TrimSpace(head)}
	}
	return nil
}

func classifyUpstreamError(err error) (statusCode int, errType string, retryAfter bool, msg string) {
	if err == nil {
		return http.StatusOK, "", false, ""
	}
	var softErr *SoftError
	if errors.As(err, &softErr) && softErr != nil {
		retry := (softErr.StatusCode == 429 || softErr.StatusCode == 503 || softErr.StatusCode == 529) && softErr.ErrorType != "tos_circuit_breaker"
		return softErr.StatusCode, softErr.ErrorType, retry, softErr.Message
	}
	if IsTosViolationError(err.Error()) {
		TripTosCircuitBreaker(err.Error())
		return http.StatusServiceUnavailable, "tos_circuit_breaker", false, "Antigravity ToS circuit breaker tripped: " + err.Error()
	}
	low := strings.ToLower(err.Error())
	if strings.Contains(low, "429") || strings.Contains(low, "rate limit") || strings.Contains(low, "quota") || strings.Contains(low, "overloaded") || strings.Contains(low, "resource_exhausted") {
		return 529, "overloaded_error", true, err.Error()
	}
	if strings.Contains(low, "timed out") || strings.Contains(low, "not running") || strings.Contains(low, "exited") || strings.Contains(low, "unavailable") {
		return http.StatusServiceUnavailable, "api_error", true, err.Error()
	}
	return http.StatusInternalServerError, "api_error", false, err.Error()
}

func toInt(v any) (int, bool) {
	switch x := v.(type) {
	case float64:
		return int(x), true
	case int:
		return x, true
	case int64:
		return int(x), true
	case json.Number:
		i, err := x.Int64()
		return int(i), err == nil
	}
	return 0, false
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
	switch v := sys.(type) {
	case string:
		return PruneClientSystemPrompt(v)
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
		return PruneClientSystemPrompt(strings.Join(parts, "\n"))
	default:
		if raw, err := json.Marshal(v); err == nil {
			return PruneClientSystemPrompt(string(raw))
		}
		return ""
	}
}

func extractCwdFromSystem(sysText string, messages []map[string]any) string {
	limit := len(messages)
	if limit > 4 {
		limit = 4
	}
	initBytes, _ := json.Marshal(messages[:limit])
	combined := sysText + "\n" + string(initBytes)
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
			}
			out[k] = sanitizeSchemaNode(val)
		}
		if out["type"] == "array" && out["items"] == nil {
			out["items"] = map[string]any{"type": "string"}
		}
		if desc, ok := out["description"].(string); ok {
			d := SanitizeOutboundText(desc)
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

func sanitizeToolsCatalog(rawTools any) []ToolDef {
	arr, ok := rawTools.([]any)
	if !ok || len(arr) == 0 {
		return nil
	}
	seen := make(map[string]bool)
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
		if rawReq, okR := sanitized["required"].([]any); okR {
			reqSeen := make(map[string]bool)
			for _, rItem := range rawReq {
				if rk, okS := rItem.(string); okS && rk != "toolSummary" && !reqSeen[rk] {
					if _, hasProp := props[rk]; hasProp {
						reqSeen[rk] = true
						reqList = append(reqList, rk)
					}
				}
			}
		}

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
		rawDesc = SanitizeOutboundText(rawDesc)
		if idx := strings.IndexByte(rawDesc, '\n'); idx >= 0 {
			rawDesc = rawDesc[:idx]
		}
		if len(rawDesc) > 300 {
			rawDesc = truncateRunes(rawDesc, 300)
		}
		if rawDesc == "" {
			rawDesc = "Execute host tool " + name
		}

		catalog = append(catalog, ToolDef{
			Name:        name,
			McpName:     mcpToolName(name),
			Description: rawDesc,
			Parameters:  cleanSchema,
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
	return catalog
}

func extractImageBlocksFromMessage(m map[string]any) []map[string]any {
	arr, ok := m["content"].([]any)
	if !ok {
		return nil
	}
	var imgs []map[string]any
	for _, item := range arr {
		b, okB := item.(map[string]any)
		if !okB {
			continue
		}
		bType, _ := b["type"].(string)
		if bType == "image" {
			if src, okS := b["source"].(map[string]any); okS && src["type"] == "base64" {
				dataStr, _ := src["data"].(string)
				mimeStr, _ := src["media_type"].(string)
				if mimeStr == "" {
					mimeStr = "image/png"
				}
				if dataStr != "" {
					imgs = append(imgs, map[string]any{
						"type":     "image",
						"mimeType": mimeStr,
						"data":     dataStr,
					})
				}
			}
		} else if bType == "image_url" || bType == "input_image" {
			var urlStr string
			switch u := b["image_url"].(type) {
			case string:
				urlStr = u
			case map[string]any:
				urlStr, _ = u["url"].(string)
			}
			if strings.HasPrefix(urlStr, "data:") {
				if comma := strings.IndexByte(urlStr, ','); comma > 5 {
					hdr := urlStr[5:comma]
					dataPart := urlStr[comma+1:]
					mimeStr := strings.Split(hdr, ";")[0]
					if mimeStr == "" {
						mimeStr = "image/png"
					}
					imgs = append(imgs, map[string]any{
						"type":     "image",
						"mimeType": mimeStr,
						"data":     dataPart,
					})
				}
			}
		}
	}
	return imgs
}

func formatToolResultContentAsText(content any) string {
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
			if cType == "text" {
				t, _ := c["text"].(string)
				parts = append(parts, stripRedundantSystemReminders(t))
			} else if cType == "image" {
				src, _ := c["source"].(map[string]any)
				dataStr, _ := src["data"].(string)
				mimeStr, _ := src["media_type"].(string)
				if mimeStr == "" {
					mimeStr = "image"
				}
				kb := (len(dataStr) * 3) / 4 / 1024
				parts = append(parts, fmt.Sprintf("[Image attachment (%s, ~%dKB binary omitted in text history)]", mimeStr, kb))
			} else if raw, err := json.Marshal(SanitizeStructuredValue(c)); err == nil {
				parts = append(parts, string(raw))
			}
		}
		return strings.Join(parts, "\n")
	default:
		if raw, err := json.Marshal(SanitizeStructuredValue(v)); err == nil {
			return string(raw)
		}
		return ""
	}
}

func formatSingleMessageText(m map[string]any) string {
	switch content := m["content"].(type) {
	case string:
		return stripRedundantSystemReminders(content)
	case []any:
		var parts []string
		for _, item := range content {
			b, ok := item.(map[string]any)
			if !ok {
				continue
			}
			bType, _ := b["type"].(string)
			switch bType {
			case "thinking", "redacted_thinking", "image", "image_url", "input_image":
				continue
			case "text", "input_text", "output_text":
				t, _ := b["text"].(string)
				if s := stripRedundantSystemReminders(t); s != "" {
					parts = append(parts, s)
				}
			case "tool_result":
				id, _ := b["tool_use_id"].(string)
				c := formatToolResultContentAsText(b["content"])
				parts = append(parts, fmt.Sprintf("[Tool Result (%s)]: %s", id, c))
			case "tool_use":
				name, _ := b["name"].(string)
				id, _ := b["id"].(string)
				inBytes, _ := json.Marshal(SanitizeStructuredValue(b["input"]))
				parts = append(parts, fmt.Sprintf("[Tool Call (%s, id=%s)]: %s", name, id, string(inBytes)))
			default:
				if raw, err := json.Marshal(SanitizeStructuredValue(b)); err == nil {
					parts = append(parts, string(raw))
				}
			}
		}
		return strings.Join(parts, "\n")
	default:
		if raw, err := json.Marshal(SanitizeStructuredValue(content)); err == nil {
			return string(raw)
		}
		return ""
	}
}

func extractToolResultsFromMessages(messages []map[string]any) []ToolResultInput {
	var results []ToolResultInput
	for i := len(messages) - 1; i >= 0; i-- {
		msg := messages[i]
		role, _ := msg["role"].(string)
		if role == "assistant" {
			break
		}
		if role == "tool" {
			tcID, _ := msg["tool_call_id"].(string)
			if tcID != "" {
				var txt string
				if s, ok := msg["content"].(string); ok {
					txt = s
				} else if raw, err := json.Marshal(msg["content"]); err == nil {
					txt = string(raw)
				}
				results = append(results, ToolResultInput{
					ToolCallID: tcID,
					Content:    txt,
				})
			}
		} else if arr, ok := msg["content"].([]any); ok {
			trailingImgs := extractImageBlocksFromMessage(msg)
			for _, item := range arr {
				b, okB := item.(map[string]any)
				if !okB {
					continue
				}
				if b["type"] == "tool_result" {
					tuID, _ := b["tool_use_id"].(string)
					if tuID == "" {
						continue
					}
					var txt string
					var toolImgs []map[string]any
					switch bc := b["content"].(type) {
					case string:
						txt = bc
					case []any:
						var tParts []string
						for _, sub := range bc {
							cm, okC := sub.(map[string]any)
							if !okC {
								continue
							}
							ct, _ := cm["type"].(string)
							if ct == "text" {
								ts, _ := cm["text"].(string)
								tParts = append(tParts, ts)
							} else if ct == "image" {
								if src, okS := cm["source"].(map[string]any); okS && src["type"] == "base64" {
									dStr, _ := src["data"].(string)
									mStr, _ := src["media_type"].(string)
									if mStr == "" {
										mStr = "image/png"
									}
									if dStr != "" {
										toolImgs = append(toolImgs, map[string]any{
											"type":     "image",
											"mimeType": mStr,
											"data":     dStr,
										})
									}
								}
							} else if raw, err := json.Marshal(cm); err == nil {
								tParts = append(tParts, string(raw))
							}
						}
						txt = strings.Join(tParts, "\n")
					default:
						if raw, err := json.Marshal(bc); err == nil {
							txt = string(raw)
						}
					}
					isErr, _ := b["is_error"].(bool)
					results = append(results, ToolResultInput{
						ToolCallID: tuID,
						Content:    txt,
						Images:     append(toolImgs, trailingImgs...),
						IsError:    isErr,
					})
				}
			}
		}
	}
	return results
}

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
	if limit < 0 {
		limit = 2
		if limit > len(messages) {
			limit = len(messages)
		}
	}
	var parts []string
	for _, m := range messages[:limit] {
		role, _ := m["role"].(string)
		parts = append(parts, role+":"+strings.TrimSpace(formatSingleMessageText(m)))
	}
	return shortSha256(strings.Join(parts, "||"))
}

func isCompactionSummaryRequest(messages []map[string]any) bool {
	if len(messages) == 0 {
		return false
	}
	last := messages[len(messages)-1]
	role, _ := last["role"].(string)
	if role != "user" {
		return false
	}
	txt := strings.ToLower(formatSingleMessageText(last))
	return strings.Contains(txt, "create a detailed summary of the conversation") ||
		strings.Contains(txt, "summarize this conversation") ||
		(strings.Contains(txt, "primary request and intent") && strings.Contains(txt, "<summary>"))
}

func isFailedErrorTurnMessage(m map[string]any) bool {
	role, _ := m["role"].(string)
	if role != "assistant" {
		return false
	}
	txt := strings.TrimSpace(formatSingleMessageText(m))
	return strings.HasPrefix(txt, "⚠ Agent execution terminated due to error")
}

func buildPromptBlocks(unseenMessages []map[string]any, isRollover bool, allMessages []map[string]any, maxChars int) []map[string]any {
	if maxChars <= 0 {
		maxChars = MaxTranscriptChar
	}
	var recentImgs []map[string]any
	for _, m := range unseenMessages {
		recentImgs = append(recentImgs, extractImageBlocksFromMessage(m)...)
	}
	if len(recentImgs) > 10 {
		recentImgs = recentImgs[len(recentImgs)-10:]
	}

	if isRollover && len(allMessages) > 1 {
		var cleanAll []map[string]any
		for _, msg := range allMessages {
			if isFailedErrorTurnMessage(msg) {
				if len(cleanAll) > 0 && cleanAll[len(cleanAll)-1]["role"] == "user" {
					cleanAll = cleanAll[:len(cleanAll)-1]
				}
				continue
			}
			cleanAll = append(cleanAll, msg)
		}
		if len(cleanAll) == 0 {
			cleanAll = allMessages
		}

		var historyLines []string
		for _, m := range cleanAll[:len(cleanAll)-1] {
			txt := strings.TrimSpace(formatSingleMessageText(m))
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
			trimmed := len(transcriptText) - maxChars
			transcriptText = transcriptText[:headChars] +
				fmt.Sprintf("\n\n[... %d chars of intermediate tool outputs trimmed to stay within the model context window ...]\n\n", trimmed) +
				transcriptText[len(transcriptText)-tailChars:]
		}
		lastUser := cleanAll[len(cleanAll)-1]
		lastText := formatSingleMessageText(lastUser)

		var out []map[string]any
		out = append(out, map[string]any{
			"type": "resource",
			"resource": map[string]any{
				"uri":      "context://host/conversation-history.txt",
				"mimeType": "text/plain",
				"text":     transcriptText,
			},
		})
		out = append(out, recentImgs...)
		out = append(out, map[string]any{
			"type": "text",
			"text": "[Conversation History Restored — All dsh-bridge MCP Tools Active]\n" + lastText,
		})
		return out
	}

	var nonEmpty []struct {
		Role string
		Text string
	}
	for _, m := range unseenMessages {
		role, _ := m["role"].(string)
		t := strings.TrimSpace(formatSingleMessageText(m))
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
	out = append(out, recentImgs...)
	out = append(out, map[string]any{"type": "text", "text": joined})
	return out
}

func isBackgroundUtilityCall(sysText string, tools []ToolDef, body map[string]any) bool {
	if maxTok, ok := toInt(body["max_tokens"]); ok && maxTok == 1 {
		return true
	}
	if len(tools) == 0 && !strings.Contains(sysText, "Primary working directory") {
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
			"x-session-id",
			"session-id",
			"thread-id",
			"x-conversation-id",
		} {
			if v := strings.TrimSpace(r.Header.Get(hdr)); v != "" {
				explicit = v
				break
			}
		}
		agentID = strings.TrimSpace(r.Header.Get("x-claude-code-agent-id"))
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
	return explicit, agentID
}

func deriveConversationKey(r *http.Request, body map[string]any, sysText string, tools []ToolDef, messages []map[string]any) string {
	if r != nil {
		reqClass := strings.ToLower(strings.TrimSpace(r.Header.Get("x-claude-code-request-class")))
		if reqClass == "utility" || reqClass == "background" || reqClass == "title" || reqClass == "suggestion" {
			var firstMsgBytes []byte
			if len(messages) > 0 {
				firstMsgBytes, _ = json.Marshal(messages[0])
			}
			return "util:" + shortSha256(truncateRunes(sysText, 512)+"||"+truncateRunes(string(firstMsgBytes), 512))
		}
	}
	if isBackgroundUtilityCall(sysText, tools, body) {
		var firstMsgBytes []byte
		if len(messages) > 0 {
			firstMsgBytes, _ = json.Marshal(messages[0])
		}
		s1 := sysText
		if len(s1) > 512 {
			s1 = s1[:512]
		}
		s2 := string(firstMsgBytes)
		if len(s2) > 512 {
			s2 = s2[:512]
		}
		return "util:" + shortSha256(s1+"||"+s2)
	}

	firstFp := computeFirstUserFingerprint(messages)
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
	sysPrefix := sysText
	if len(sysPrefix) > 4096 {
		sysPrefix = sysPrefix[:4096]
	}
	if agentID != "" {
		return fmt.Sprintf("fp:%s:ag:%s:u:%s", shortSha256(sysPrefix), agentID, firstFp)
	}
	return fmt.Sprintf("fp:%s:u:%s", shortSha256(sysPrefix), firstFp)
}

type GatewayServer struct {
	stopReaper    chan struct{}
	reaperDone    chan struct{}
	closeOnce     sync.Once
	workerAdmitMu sync.Mutex
	mu            sync.RWMutex
	sessions      map[string]*AcpProjectSession
	tenants       map[string]*AcpProjectSession
	reqCounter    atomic.Int64
}

func NewGatewayServer() *GatewayServer {
	GlobalStore.mu.Lock()
	hasDB := GlobalStore.db != nil
	GlobalStore.mu.Unlock()
	if !hasDB && GatewayStateDB != "" {
		GlobalStore.Init(GatewayStateDB)
	}
	gs := &GatewayServer{
		stopReaper: make(chan struct{}), reaperDone: make(chan struct{}),
		sessions: make(map[string]*AcpProjectSession),
		tenants:  make(map[string]*AcpProjectSession),
	}
	if restored := GlobalStore.RestoreDetachedSessions(gs); restored > 0 {
		LogEvent("STATE_STORE_RESTORE", fmt.Sprintf("Restored %d detached sessions from gateway.db", restored), map[string]any{
			"db": GatewayStateDB,
		})
	}
	go func() {
		defer close(gs.reaperDone)
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-gs.stopReaper:
				return
			case <-ticker.C:
				gs.reapIdleWorkers()
			}
		}
	}()
	return gs
}

func (gs *GatewayServer) Close() {
	if gs.stopReaper != nil {
		gs.closeOnce.Do(func() { close(gs.stopReaper) })
		<-gs.reaperDone
	}
}

func (gs *GatewayServer) countLiveWorkers() int {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	cnt := 0
	for _, s := range gs.sessions {
		s.mu.Lock()
		if s.SessionID != "" || s.spawning || s.closing {
			cnt++
		}
		s.mu.Unlock()
	}
	return cnt
}

// Lock order: workerAdmitMu -> gs.mu -> s.mu. Never enter with s.mu held.
func (gs *GatewayServer) reserveWorkerSlot(s *AcpProjectSession) error {
	gs.workerAdmitMu.Lock()
	defer gs.workerAdmitMu.Unlock()
	gs.enforceWorkerCap(s)
	if gs.countLiveWorkers() >= MaxLiveWorkers {
		return &SoftError{StatusCode: 529, ErrorType: "overloaded_error", Message: "All live worker slots are busy; retry shortly"}
	}
	s.mu.Lock()
	s.spawning = true
	s.mu.Unlock()
	return nil
}

func (gs *GatewayServer) enforceWorkerCap(exclude *AcpProjectSession) {
	if gs.countLiveWorkers() < MaxLiveWorkers {
		return
	}
	gs.mu.RLock()
	var live []*AcpProjectSession
	for _, s := range gs.sessions {
		s.mu.Lock()
		hasSid := s.SessionID != ""
		s.mu.Unlock()
		if hasSid && s != exclude && s.IsAvailableForNewTurn() {
			live = append(live, s)
		}
	}
	gs.mu.RUnlock()

	sort.Slice(live, func(i, j int) bool {
		aUtil := strings.HasPrefix(live[i].ConvKey, "util:")
		bUtil := strings.HasPrefix(live[j].ConvKey, "util:")
		if aUtil != bUtil {
			return aUtil
		}
		live[i].mu.Lock()
		ti := live[i].LastUsedAt
		live[i].mu.Unlock()
		live[j].mu.Lock()
		tj := live[j].LastUsedAt
		live[j].mu.Unlock()
		return ti.Before(tj)
	})

	for gs.countLiveWorkers() >= MaxLiveWorkers && len(live) > 0 {
		victim := live[0]
		live = live[1:]
		victim.HibernateWorker(fmt.Sprintf("lru_cap_%d", MaxLiveWorkers), false)
	}
}

func (gs *GatewayServer) reapIdleWorkers() {
	now := time.Now()
	anyBusy := GlobalEngine.IsBooting()

	gs.mu.Lock()
	var toHibernate []struct {
		sess   *AcpProjectSession
		reason string
		force  bool
	}
	for k, s := range gs.sessions {
		s.mu.Lock()
		if s.IsBusy || s.spawning || s.closing {
			anyBusy = true
		}
		idleDur := now.Sub(s.LastUsedAt)
		sid := s.SessionID
		busy := s.IsBusy || s.spawning || s.closing
		parkedCnt := len(s.ParkedCalls)
		onlyStruct := parkedCnt > 0
		for _, p := range s.ParkedCalls {
			if p.Call.Name != "StructuredOutput" {
				onlyStruct = false
				break
			}
		}
		s.mu.Unlock()

		if sid == "" {
			if !busy && idleDur >= 7200*time.Second {
				delete(gs.sessions, k)
				delete(gs.tenants, s.TenantKey)
			}
			continue
		}

		if !s.IsAvailableForNewTurn() {
			if (onlyStruct && idleDur >= 5*time.Second) || (!busy && idleDur >= ParkedTtl) {
				reason := fmt.Sprintf("reap_orphan_parked_%ds", int(idleDur.Seconds()))
				if onlyStruct {
					reason = "reap_structured_output"
				}
				toHibernate = append(toHibernate, struct {
					sess   *AcpProjectSession
					reason string
					force  bool
				}{s, reason, true})
			}
			continue
		}

		ttl := IdleWorkerTtl
		if strings.HasPrefix(s.ConvKey, "util:") {
			ttl = UtilWorkerTtl
		}
		if idleDur >= ttl {
			toHibernate = append(toHibernate, struct {
				sess   *AcpProjectSession
				reason string
				force  bool
			}{s, fmt.Sprintf("idle_%ds", int(idleDur.Seconds())), false})
		}
	}
	gs.mu.Unlock()

	for _, item := range toHibernate {
		item.sess.HibernateWorker(item.reason, item.force)
	}

	if !anyBusy && GlobalEngine.IsAlive() && gs.countLiveWorkers() == 0 {
		GlobalEngine.StopEngine()
		LogEvent("ACP_ENGINE_IDLE_STOP", "Stopped shared agy-acp-900k engine (0 active sessions, 0 MB Go child RAM)", nil)
	}
}

// Remove only the mutable history suffix; never infer identity from content.
func explicitConversationIdentity(key string) string {
	if !strings.HasPrefix(key, "sid:") {
		return ""
	}
	if i := strings.LastIndex(key, ":u:"); i > 4 {
		return key[:i]
	}
	return ""
}

func sameConversationIdentity(a, b string) bool {
	if a == b {
		return true
	}
	id := explicitConversationIdentity(a)
	return id != "" && id == explicitConversationIdentity(b)
}

func (gs *GatewayServer) getOrCreateAvailableSession(baseKey, cwd, sysText string, historyFp ...string) *AcpProjectSession {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	// Parked compaction updates the persisted HistoryFingerprint, not ConvKey.
	// Only a unique FULL fingerprint within the same explicit identity can reuse
	// that canonical session. No key mutation, duplicate rows or worker aliases.
	if gs.sessions[baseKey] == nil && explicitConversationIdentity(baseKey) != "" && len(historyFp) > 0 && historyFp[0] != "" {
		var candidate *AcpProjectSession
		ambiguous := false
		for key, s := range gs.sessions {
			if !sameConversationIdentity(key, baseKey) {
				continue
			}
			s.mu.Lock()
			matches := s.HistoryFingerprint == historyFp[0] && s.MessageCount > 0 && s.Cwd == cwd && s.SystemText == sysText
			s.mu.Unlock()
			if matches {
				if candidate != nil {
					ambiguous = true
					break
				}
				candidate = s
			}
		}
		if candidate != nil && !ambiguous && candidate.IsAvailableForNewTurn() {
			LogEvent("SESSION_CONTINUITY", "Reused same-identity session after history key changed", map[string]any{"reason": "full_history_fingerprint_match"})
			return candidate
		}
	}

	for slot := 0; slot < 8; slot++ {
		slotKey := baseKey
		if slot > 0 {
			slotKey = fmt.Sprintf("%s#%d", baseKey, slot+1)
		}
		existing, ok := gs.sessions[slotKey]
		if !ok {
			created := NewAcpProjectSession(slotKey, cwd, sysText)
			created.onBeforeSpawn = gs.reserveWorkerSlot
			gs.sessions[slotKey] = created
			gs.tenants[created.TenantKey] = created
			return created
		}
		if existing.IsAvailableForNewTurn() {
			return existing
		}
	}
	fallbackKey := fmt.Sprintf("%s#%s", baseKey, randomHex(2))
	fallback := NewAcpProjectSession(fallbackKey, cwd, sysText)
	fallback.onBeforeSpawn = gs.reserveWorkerSlot
	gs.sessions[fallbackKey] = fallback
	gs.tenants[fallback.TenantKey] = fallback
	return fallback
}

func (gs *GatewayServer) findSessionWithParkedToolResult(baseKey string, messages []map[string]any) *AcpProjectSession {
	if len(messages) == 0 {
		return nil
	}
	trs := extractToolResultsFromMessages(messages)
	if len(trs) == 0 {
		return nil
	}
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	var candidate *AcpProjectSession
	for key, s := range gs.sessions {
		if !sameConversationIdentity(key, baseKey) {
			continue
		}
		s.mu.Lock()
		matched := false
		for _, tr := range trs {
			if _, ok := s.ParkedCalls[tr.ToolCallID]; ok {
				matched = true
				break
			}
		}
		s.mu.Unlock()
		if matched {
			if candidate != nil {
				return nil
			}
			candidate = s
		}
	}
	return candidate
}

func (gs *GatewayServer) HandleMessagesRequest(r *http.Request, body map[string]any, messages []map[string]any, onDelta func(DeltaChunk)) (TurnResult, error) {
	ctx := r.Context()
	sysText := normalizeSystemText(body["system"])
	cwd := extractCwdFromSystem(sysText, messages)
	toolsCatalog := sanitizeToolsCatalog(body["tools"])
	modelStr, _ := body["model"].(string)
	sel := ResolveRequestedModel(modelStr, body)
	thoughtLevel := sel.Tier

	compactionHdr := ""
	compactedHdr := ""
	reqClass := ""
	if r != nil {
		compactionHdr = strings.ToLower(strings.TrimSpace(r.Header.Get("x-claude-code-compaction")))
		compactedHdr = strings.ToLower(strings.TrimSpace(r.Header.Get("x-claude-code-context-compacted")))
		reqClass = strings.ToLower(strings.TrimSpace(r.Header.Get("x-claude-code-request-class")))
	}
	isExplicitCompaction := compactionHdr == "1" || compactionHdr == "true" || reqClass == "compaction"
	isExplicitCompacted := compactedHdr == "1" || compactedHdr == "true"

	baseKey := deriveConversationKey(r, body, sysText, toolsCatalog, messages)
	if parkedSess := gs.findSessionWithParkedToolResult(baseKey, messages); parkedSess != nil {
		parkedSess.queueMu.Lock()
		defer parkedSess.queueMu.Unlock()
		if isExplicitCompacted {
			parkedSess.mu.Lock()
			parkedSess.ForceRolloverNext = true
			parkedSess.mu.Unlock()
		}
		return parkedSess.RunTurn(ctx, messages, toolsCatalog, thoughtLevel, onDelta, &sel)
	}

	if isCompactionSummaryRequest(messages) || isExplicitCompaction {
		// Mark all sibling sessions of this Claude session ID with ForceRolloverNext = true
		// so that when the compacted message list (with any pending tool_result) arrives next,
		// the old uncompacted 400K-800K session is guaranteed to roll over cleanly!
		sidPrefix := baseKey
		if idx := strings.Index(sidPrefix, ":u:"); idx > 0 {
			sidPrefix = sidPrefix[:idx+3]
		}
		gs.mu.RLock()
		for k, sib := range gs.sessions {
			if strings.HasPrefix(k, sidPrefix) {
				sib.mu.Lock()
				sib.ForceRolloverNext = true
				sib.mu.Unlock()
			}
		}
		gs.mu.RUnlock()
	}
	if len(messages) > 0 {
		lastBytes, _ := json.Marshal(messages[len(messages)-1])
		if strings.Contains(string(lastBytes), "Automated prompt-cache keep-alive ping") {
			gs.mu.RLock()
			existing := gs.sessions[baseKey]
			gs.mu.RUnlock()
			if existing != nil {
				return existing.RunKeepalivePing(onDelta), nil
			}
		}
	}

	sess := gs.getOrCreateAvailableSession(baseKey, cwd, sysText, computeFirstUserFingerprint(messages))
	sess.queueMu.Lock()
	defer sess.queueMu.Unlock()
	if isExplicitCompacted {
		sess.mu.Lock()
		sess.ForceRolloverNext = true
		sess.mu.Unlock()
	}
	return sess.RunTurn(ctx, messages, toolsCatalog, thoughtLevel, onDelta, &sel)
}

func (gs *GatewayServer) GetStats() map[string]any {
	gs.mu.RLock()
	defer gs.mu.RUnlock()

	var projects []map[string]any
	liveWorkers := 0
	for key, s := range gs.sessions {
		s.mu.Lock()
		if s.SessionID != "" {
			liveWorkers++
		}
		if s.SessionID == "" && !s.IsBusy {
			s.mu.Unlock()
			continue
		}
		turnsCopy := append([]TurnHistoryEntry(nil), s.TurnHistory...)
		projects = append(projects, map[string]any{
			"convKey":             key,
			"cwd":                 s.Cwd,
			"sessionId":           s.SessionID,
			"model":               s.ModelID,
			"thoughtLevel":        s.ThoughtLevel,
			"messageCount":        s.MessageCount,
			"sessionReuses":       s.SessionReuses,
			"compactionRollovers": s.CompactionRollovers,
			"createdAtMs":         s.CreatedAt.UnixMilli(),
			"lastUsedAt":          s.LastUsedAt.UTC().Format(time.RFC3339),
			"turns":               turnsCopy,
		})
		s.mu.Unlock()
	}

	sort.Slice(projects, func(i, j int) bool {
		ci, _ := projects[i]["createdAtMs"].(int64)
		cj, _ := projects[j]["createdAtMs"].(int64)
		if ci != cj {
			return ci < cj
		}
		ki, _ := projects[i]["convKey"].(string)
		kj, _ := projects[j]["convKey"].(string)
		return ki < kj
	})

	gogc := os.Getenv("GOGC")
	if gogc == "" {
		gogc = "25"
	}
	gomem := os.Getenv("GOMEMLIMIT")
	if gomem == "" {
		gomem = "64MiB"
	}

	tosTripped, tosReason := IsTosTripped()

	return map[string]any{
		"activeProjects":            len(projects),
		"liveWorkers":               liveWorkers,
		"maxLiveWorkers":            MaxLiveWorkers,
		"idleWorkerTtlMs":           int(IdleWorkerTtl.Milliseconds()),
		"parkedTtlMs":               int(ParkedTtl.Milliseconds()),
		"binary":                    PatchedAgyBin,
		"engine":                    "Go Native Single-Binary Gateway",
		"contextWindowMax":          ModelOrDefault(DefaultModelID).ContextWindow,
		"internalAutoCheckpoint":    "disabled (delegated to host Claude Code)",
		"defaultModel":              DefaultModelID,
		"models":                    AgyModels,
		"gogc":                      gogc,
		"gomemlimit":                gomem,
		"tosTripped":                tosTripped,
		"tosTrippedReason":          tosReason,
		"argonDegradationDetected":  argonDegradationDetectedTotal.Load(),
		"argonDegradationRecovered": argonDegradationRecoveredTotal.Load(),
		"logFile":                   GatewayLogFile,
		"tmpLogFile":                TmpLogFile,
		"projects":                  projects,
	}
}

func (gs *GatewayServer) CollectDashboardTelemetry(gwSids map[string]bool) ([]map[string]any, []map[string]any) {
	gs.mu.RLock()
	defer gs.mu.RUnlock()

	var liveSessions []map[string]any
	var gatewayTurns []map[string]any

	for key, s := range gs.sessions {
		s.mu.Lock()
		if s.SessionID != "" {
			gwSids[strings.ToLower(s.SessionID)] = true
		}
		var lastTurn *TurnHistoryEntry
		if len(s.TurnHistory) > 0 {
			lt := s.TurnHistory[len(s.TurnHistory)-1]
			lastTurn = &lt
		}
		ctxTokens := s.LastCachedReadTokens
		if ctxTokens == 0 && lastTurn != nil && lastTurn.Usage != nil {
			ctxTokens = lastTurn.Usage.InputTokens + lastTurn.Usage.CachedReadTokens
		}
		if s.SessionID != "" || s.IsBusy {
			status := "IDLE"
			if len(s.ParkedCalls) > 0 {
				status = "PARKED"
			} else if s.IsBusy {
				status = "BUSY"
			}
			var parkedTools []string
			for _, p := range s.ParkedCalls {
				parkedTools = append(parkedTools, p.Call.Name)
			}
			lastHit := "N/A"
			if lastTurn != nil {
				lastHit = lastTurn.CacheHitRate
			}
			sm := ModelOrDefault(s.ModelID)
			liveSessions = append(liveSessions, map[string]any{
				"convKey":             key,
				"sessionId":           s.SessionID,
				"model":               sm.ID,
				"modelLabel":          sm.Label,
				"thoughtLevel":        s.ThoughtLevel,
				"contextWindow":       sm.ContextWindow,
				"cwd":                 s.Cwd,
				"status":              status,
				"parkedTools":         parkedTools,
				"messageCount":        s.MessageCount,
				"sessionReuses":       s.SessionReuses,
				"compactionRollovers": s.CompactionRollovers,
				"contextTokens":       ctxTokens,
				"contextPct":          roundTo(float64(ctxTokens)/float64(sm.ContextWindow)*100.0, 2),
				"createdAtMs":         s.CreatedAt.UnixMilli(),
				"lastUsedAt":          s.LastUsedAt.UTC().Format(time.RFC3339),
				"turnsCount":          len(s.TurnHistory),
				"lastHitRate":         lastHit,
			})
		}

		for _, t := range s.TurnHistory {
			if t.SessionID != "" {
				gwSids[strings.ToLower(t.SessionID)] = true
			}
			inT := 0
			crT := 0
			rawIn := 0
			rawCr := 0
			outT := 0
			if t.Usage != nil {
				inT = t.Usage.InputTokens
				crT = t.Usage.CachedReadTokens
				rawIn = t.Usage.RawUncachedInputTokens
				if rawIn == 0 {
					rawIn = inT
				}
				rawCr = t.Usage.RawTpuCachedTokens
				if rawCr == 0 {
					rawCr = crT
				}
				outT = t.Usage.OutputTokens
			}
			tm := ModelOrDefault(t.Model)
			stepCost := PriceUsage(tm, rawIn, rawCr, outT, 0, 0).CostUsd
			gatewayTurns = append(gatewayTurns, map[string]any{
				"model":            tm.ID,
				"modelLabel":       tm.Label,
				"thoughtLevel":     t.ThoughtLevel,
				"convKey":          key,
				"sessionId":        t.SessionID,
				"turnIndex":        t.TurnIndex,
				"completedAtMs":    t.CompletedAtMs,
				"ts":               t.Ts,
				"mode":             t.Mode,
				"stopReason":       t.StopReason,
				"elapsedMs":        t.ElapsedMs,
				"ttftMs":           t.TtftMs,
				"tps":              t.Tps,
				"hasThinking":      t.HasThinking,
				"thinkingTokens":   t.ThinkingTokens,
				"inputTokens":      inT,
				"cachedReadTokens": crT,
				"outputTokens":     outT,
				"cacheHitRate":     t.CacheHitRate,
				"costUsd":          roundTo(stepCost, 4),
			})
		}
		s.mu.Unlock()
	}

	// Sort liveSessions deterministically by creation time (then convKey) so cards never jump around on refresh
	sort.Slice(liveSessions, func(i, j int) bool {
		ci, _ := liveSessions[i]["createdAtMs"].(int64)
		cj, _ := liveSessions[j]["createdAtMs"].(int64)
		if ci != cj {
			return ci < cj
		}
		ki, _ := liveSessions[i]["convKey"].(string)
		kj, _ := liveSessions[j]["convKey"].(string)
		return ki < kj
	})

	// Sort gatewayTurns chronologically across all sessions (newest completed turn at the top)
	sort.Slice(gatewayTurns, func(i, j int) bool {
		ti, _ := gatewayTurns[i]["completedAtMs"].(int64)
		tj, _ := gatewayTurns[j]["completedAtMs"].(int64)
		if ti != tj {
			return ti > tj
		}
		ki, _ := gatewayTurns[i]["convKey"].(string)
		kj, _ := gatewayTurns[j]["convKey"].(string)
		if ki != kj {
			return ki < kj
		}
		idi, _ := gatewayTurns[i]["turnIndex"].(int)
		idj, _ := gatewayTurns[j]["turnIndex"].(int)
		return idi > idj
	})
	if len(gatewayTurns) > 60 {
		gatewayTurns = gatewayTurns[:60]
	}
	return liveSessions, gatewayTurns
}

func sendSseEvent(w io.Writer, flusher http.Flusher, evName string, data any) {
	if rw, ok := w.(http.ResponseWriter); ok {
		_ = http.NewResponseController(rw).SetWriteDeadline(time.Now().Add(30 * time.Second))
	}
	b, _ := json.Marshal(data)
	_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", evName, string(b))
	if flusher != nil {
		flusher.Flush()
	}
}

func isAllowedLoopbackHost(hostHdr string) bool {
	h := strings.ToLower(hostHdr)
	if h == "" || strings.TrimSpace(h) != h {
		return false
	}
	if strings.Contains(h, ":") && h != "::1" && h != "[::1]" {
		host, port, err := net.SplitHostPort(h)
		if err != nil {
			return false
		}
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return false
		}
		h = host
	}
	return h == "127.0.0.1" || h == "localhost" || h == "::1" || h == "[::1]"
}

func applyCORSAndSecurityGuards(w http.ResponseWriter, r *http.Request) bool {
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin != "" && AllowedCorsOrigins[origin] {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Vary", "Origin")
	}
	if !authEnabled() {
		if !isAllowedLoopbackHost(r.Host) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"type":"error","error":{"type":"permission_error","message":"non-loopback Host header rejected on unauthenticated gateway"}}`))
			return false
		}
		if origin != "" && !AllowedCorsOrigins[origin] {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"type":"error","error":{"type":"permission_error","message":"cross-origin browser request rejected on unauthenticated loopback gateway"}}`))
			return false
		}
	}
	if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/") {
		ct := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Type")))
		if (!authEnabled() && !strings.HasPrefix(ct, "application/json")) || (ct != "" && !strings.HasPrefix(ct, "application/json")) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnsupportedMediaType)
			_, _ = w.Write([]byte(`{"type":"error","error":{"type":"invalid_request_error","message":"Content-Type must be application/json"}}`))
			return false
		}
	}
	return true
}

func estimateCountTokens(rawBody []byte) (int, error) {
	if len(rawBody) == 0 {
		return 0, errors.New("empty request body")
	}
	var body map[string]any
	if err := json.Unmarshal(rawBody, &body); err != nil || body == nil {
		return 0, errors.New("invalid JSON body")
	}
	charCount := len(normalizeSystemText(body["system"]))
	imgCount := 0
	if rawMsgs, ok := body["messages"].([]any); ok {
		for _, rm := range rawMsgs {
			if m, okM := rm.(map[string]any); okM {
				charCount += len(formatSingleMessageText(m))
				imgCount += len(extractImageBlocksFromMessage(m))
			}
		}
	}
	if rawTools, ok := body["tools"].([]any); ok {
		if b, err := json.Marshal(rawTools); err == nil {
			charCount += len(b)
		}
	}
	est := int(math.Ceil(float64(charCount)/4.0)) + imgCount*1600
	if est < 1 {
		est = 1
	}
	return est, nil
}

func (gs *GatewayServer) GatewayOnlyHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		if p == "/" || p == "/dashboard" || p == "/api/dashboard" || p == "/api/quota" || p == "/metrics" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"Not found on gateway API port"}`))
			return
		}
		gs.ServeHTTP(w, r)
	})
}

func (gs *GatewayServer) DashboardOnlyHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		if strings.HasPrefix(p, "/v1/") || strings.HasPrefix(p, "/mcp/") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"Not found on dashboard port"}`))
			return
		}
		gs.ServeHTTP(w, r)
	})
}

func (gs *GatewayServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	reqID := gs.reqCounter.Add(1)
	path := r.URL.Path

	if r.Method == http.MethodOptions {
		origin := strings.TrimSpace(r.Header.Get("Origin"))
		if origin != "" && AllowedCorsOrigins[origin] {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, x-api-key, anthropic-version, x-claude-code-session-id")
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// Single-Port Multi-Tenant MCP Router
	if strings.HasPrefix(path, "/mcp/") {
		tenantKey := strings.TrimPrefix(path, "/mcp/")
		gs.mu.RLock()
		sess := gs.tenants[tenantKey]
		gs.mu.RUnlock()
		if sess == nil {
			if McpTrace {
				LogEvent("MCP_404", "unknown tenant "+tenantKey, map[string]any{"method": r.Method})
			}
			w.WriteHeader(http.StatusNotFound)
			return
		}
		sess.HandleMcpHTTP(w, r)
		return
	}

	// Unauthenticated liveness probe (docker healthcheck)
	if r.Method == http.MethodGet && path == "/healthz" {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
		return
	}

	if !applyCORSAndSecurityGuards(w, r) {
		return
	}

	if path == "/" || path == "/dashboard" || path == "/api/dashboard" || path == "/api/quota" || path == "/health" || path == "/v1/health" || path == "/metrics" {
		if !checkDashboardAuth(w, r) {
			return
		}
	} else if strings.HasPrefix(path, "/v1/") {
		if !checkAPIAuth(w, r) {
			return
		}
	}

	if r.Method == http.MethodGet && (path == "/" || path == "/dashboard") {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		_, _ = w.Write([]byte(RenderDashboardHTML()))
		return
	}

	if r.Method == http.MethodGet && path == "/api/dashboard" {
		force := r.URL.Query().Get("refresh") == "1"
		payload := BuildDashboardPayload(gs, force)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		_ = json.NewEncoder(w).Encode(payload)
		return
	}

	if r.Method == http.MethodGet && path == "/api/quota" {
		force := r.URL.Query().Get("refresh") == "1"
		payload := BuildDashboardPayload(gs, force)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"now":     payload["now"],
			"gateway": payload["gateway"],
			"quota":   payload["quota"],
			"windows": payload["windows"],
		})
		return
	}

	if r.Method == http.MethodGet && (path == "/health" || path == "/v1/health") {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(gs.GetStats())
		return
	}

	if r.Method == http.MethodGet && path == "/metrics" {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = w.Write([]byte(RenderPrometheusMetrics(gs)))
		return
	}

	if r.Method == http.MethodGet && path == "/v1/models" {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"object": "list",
			"data":   ListModelsForAPI(),
		})
		return
	}

	if r.Method == http.MethodPost && path == "/v1/messages/count_tokens" {
		r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			var maxBytesErr *http.MaxBytesError
			if errors.As(err, &maxBytesErr) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusRequestEntityTooLarge)
				_, _ = w.Write([]byte(`{"type":"error","error":{"type":"invalid_request_error","message":"Request body exceeds maximum size"}}`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"type":"error","error":{"type":"invalid_request_error","message":"Failed to read request body"}}`))
			return
		}
		est, err := estimateCountTokens(raw)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"type":"error","error":{"type":"invalid_request_error","message":"Malformed JSON in count_tokens request"}}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"input_tokens": est})
		return
	}

	if r.Method == http.MethodPost && (path == "/v1/messages" || path == "/v1/chat/completions" || path == "/v1/responses") {
		if tripped, reason := IsTosTripped(); tripped {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"type": "error",
				"error": map[string]any{
					"type":    "tos_circuit_breaker",
					"message": "Gateway halted by ToS circuit breaker (delete runtime/tos-tripped to reset): " + reason,
				},
			})
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)
		rawBody, err := io.ReadAll(r.Body)
		if err != nil {
			var maxBytesErr *http.MaxBytesError
			w.Header().Set("Content-Type", "application/json")
			if errors.As(err, &maxBytesErr) {
				w.WriteHeader(http.StatusRequestEntityTooLarge)
				_, _ = w.Write([]byte(`{"type":"error","error":{"type":"invalid_request_error","message":"Request body exceeds maximum size"}}`))
			} else {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"type":"error","error":{"type":"invalid_request_error","message":"Failed to read request body"}}`))
			}
			return
		}
		var body map[string]any
		if len(rawBody) == 0 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"type":"error","error":{"type":"invalid_request_error","message":"Empty JSON request body"}}`))
			return
		}
		if err := json.Unmarshal(rawBody, &body); err != nil || body == nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"type":  "error",
				"error": map[string]any{"type": "invalid_request_error", "message": "Malformed JSON request body: " + fmt.Sprint(err)},
			})
			return
		}

		customToolNames := make(map[string]bool)
		if path == "/v1/responses" {
			var sysParts []string
			if inst, ok := body["instructions"].(string); ok && inst != "" {
				sysParts = append(sysParts, inst)
			}
			var items []any
			if sIn, ok := body["input"].(string); ok {
				items = []any{map[string]any{"type": "message", "role": "user", "content": sIn}}
			} else if arrIn, ok := body["input"].([]any); ok {
				items = arrIn
			}
			var convMsgs []any
			for _, rawIt := range items {
				it, ok := rawIt.(map[string]any)
				if !ok {
					continue
				}
				t, _ := it["type"].(string)
				if t == "" && it["role"] != nil {
					t = "message"
				}
				switch t {
				case "message":
					role, _ := it["role"].(string)
					if role == "system" || role == "developer" {
						if cs, okS := it["content"].(string); okS {
							sysParts = append(sysParts, cs)
						} else if b, errM := json.Marshal(it["content"]); errM == nil {
							sysParts = append(sysParts, string(b))
						}
					} else {
						rNorm := "user"
						if role == "assistant" {
							rNorm = "assistant"
						}
						convMsgs = append(convMsgs, map[string]any{"role": rNorm, "content": it["content"]})
					}
				case "function_call", "custom_tool_call":
					cid, _ := it["call_id"].(string)
					if cid == "" {
						cid, _ = it["id"].(string)
					}
					if cid == "" {
						cid = "call_" + randomHex(4)
					}
					parsedArgs := make(map[string]any)
					if t == "custom_tool_call" {
						inStr, _ := it["input"].(string)
						parsedArgs["input"] = inStr
					} else if argStr, okA := it["arguments"].(string); okA {
						_ = json.Unmarshal([]byte(argStr), &parsedArgs)
					} else if argMap, okM := it["arguments"].(map[string]any); okM {
						parsedArgs = argMap
					}
					tName, _ := it["name"].(string)
					convMsgs = append(convMsgs, map[string]any{
						"role": "assistant",
						"content": []any{
							map[string]any{"type": "tool_use", "id": cid, "name": tName, "input": parsedArgs},
						},
					})
				case "function_call_output", "custom_tool_call_output":
					cid, _ := it["call_id"].(string)
					outStr, okS := it["output"].(string)
					if !okS {
						b, _ := json.Marshal(it["output"])
						outStr = string(b)
					}
					convMsgs = append(convMsgs, map[string]any{
						"role": "user",
						"content": []any{
							map[string]any{"type": "tool_result", "tool_use_id": cid, "content": outStr},
						},
					})
				}
			}
			if len(sysParts) > 0 && body["system"] == nil {
				body["system"] = strings.Join(sysParts, "\n\n")
			}
			body["messages"] = convMsgs
			if rawTools, okT := body["tools"].([]any); okT {
				var normTools []any
				for _, rt := range rawTools {
					if tm, okM := rt.(map[string]any); okM && tm["type"] == "custom" {
						tName, _ := tm["name"].(string)
						tDesc, _ := tm["description"].(string)
						if tName != "" {
							customToolNames[tName] = true
							normTools = append(normTools, map[string]any{
								"name":        tName,
								"description": tDesc + "\nPass free-form text in arguments.input.",
								"parameters": map[string]any{
									"type":       "object",
									"properties": map[string]any{"input": map[string]any{"type": "string"}},
									"required":   []string{"input"},
								},
							})
							continue
						}
					}
					normTools = append(normTools, rt)
				}
				body["tools"] = normTools
			}
		}

		if path == "/v1/chat/completions" {
			if rawMsgs, ok := body["messages"].([]any); ok {
				var sysParts []string
				var convMsgs []any
				for _, rm := range rawMsgs {
					m, okM := rm.(map[string]any)
					if !okM {
						continue
					}
					role, _ := m["role"].(string)
					if role == "system" || role == "developer" {
						if cs, okS := m["content"].(string); okS {
							sysParts = append(sysParts, cs)
						} else if b, errM := json.Marshal(m["content"]); errM == nil {
							sysParts = append(sysParts, string(b))
						}
						continue
					}
					if role == "tool" {
						tcID, _ := m["tool_call_id"].(string)
						outStr, okS := m["content"].(string)
						if !okS {
							b, _ := json.Marshal(m["content"])
							outStr = string(b)
						}
						convMsgs = append(convMsgs, map[string]any{
							"role": "user",
							"content": []any{
								map[string]any{"type": "tool_result", "tool_use_id": tcID, "content": outStr},
							},
						})
					} else if role == "assistant" {
						if tcs, okTC := m["tool_calls"].([]any); okTC && len(tcs) > 0 {
							var blocks []any
							if cs, okS := m["content"].(string); okS && cs != "" {
								blocks = append(blocks, map[string]any{"type": "text", "text": cs})
							}
							for _, rtc := range tcs {
								tc, okT := rtc.(map[string]any)
								if !okT {
									continue
								}
								fn, _ := tc["function"].(map[string]any)
								fName, _ := fn["name"].(string)
								fArgsStr, _ := fn["arguments"].(string)
								parsedArgs := make(map[string]any)
								_ = json.Unmarshal([]byte(fArgsStr), &parsedArgs)
								idStr, _ := tc["id"].(string)
								if idStr == "" {
									idStr = "call_" + randomHex(4)
								}
								blocks = append(blocks, map[string]any{
									"type":  "tool_use",
									"id":    idStr,
									"name":  fName,
									"input": parsedArgs,
								})
							}
							convMsgs = append(convMsgs, map[string]any{"role": "assistant", "content": blocks})
							continue
						}
						convMsgs = append(convMsgs, m)
					} else {
						convMsgs = append(convMsgs, m)
					}
				}
				if body["system"] == nil && len(sysParts) > 0 {
					body["system"] = strings.Join(sysParts, "\n")
				}
				body["messages"] = convMsgs
			}
		}

		var messages []map[string]any
		if rawMsgs, ok := body["messages"].([]any); ok {
			for _, rm := range rawMsgs {
				if m, okM := rm.(map[string]any); okM {
					messages = append(messages, m)
				}
			}
		}

		reqModel, _ := body["model"].(string)
		displayModel := normalizeDisplayModel(reqModel, body)
		msgID := "msg_" + randomHex(12)
		isStream, _ := body["stream"].(bool)
		maxTok, _ := toInt(body["max_tokens"])
		var toolsLen int
		if ta, ok := body["tools"].([]any); ok {
			toolsLen = len(ta)
		}

		reqLogMeta := map[string]any{
			"requestedModel":  reqModel,
			"displayModel":    displayModel,
			"stream":          isStream,
			"maxTokens":       maxTok,
			"messagesCount":   len(messages),
			"toolsCount":      toolsLen,
			"sessionIdHeader": r.Header.Get("x-claude-code-session-id"),
		}
		if LogHeaders {
			ccHeaders := make(map[string]string)
			for k, vals := range r.Header {
				lk := strings.ToLower(k)
				if strings.HasPrefix(lk, "x-claude-code-") || lk == "x-session-id" || lk == "session-id" || lk == "thread-id" || lk == "x-conversation-id" {
					ccHeaders[lk] = strings.Join(vals, ",")
				}
			}
			reqLogMeta["hintHeaders"] = ccHeaders
			if meta, ok := body["metadata"].(map[string]any); ok {
				reqLogMeta["metadata"] = meta
			}
		}
		LogEvent("HTTP_REQ", fmt.Sprintf("REQ #%d %s %s", reqID, r.Method, path), reqLogMeta)

		var lastMsgStr string
		if len(messages) > 0 {
			b, _ := json.Marshal(messages[len(messages)-1])
			lastMsgStr = string(b)
		}
		isSuggestion := strings.Contains(lastMsgStr, "[SUGGESTION MODE:")
		isSpinner := strings.Contains(lastMsgStr, "Describe your most recent action in 3-5 words")
		if maxTok == 1 || isSuggestion || isSpinner {
			fastReply := "OK"
			if isSuggestion {
				fastReply = ""
			} else if isSpinner {
				fastReply = "Analyzing project files and changes"
			}
			LogEvent("HTTP_FASTPATH", fmt.Sprintf("REQ #%d Fast-responding to utility probe", reqID), nil)
			if isStream && path == "/v1/messages" {
				w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
				w.Header().Set("Cache-Control", "no-cache, no-transform")
				flusher, _ := w.(http.Flusher)
				sendSseEvent(w, flusher, "message_start", map[string]any{
					"type": "message_start",
					"message": map[string]any{
						"id": msgID, "type": "message", "role": "assistant", "model": displayModel,
						"content": []any{}, "stop_reason": nil, "stop_sequence": nil,
						"usage": map[string]any{"input_tokens": 10, "cache_creation_input_tokens": 0, "cache_read_input_tokens": 0, "output_tokens": 1},
					},
				})
				sendSseEvent(w, flusher, "content_block_start", map[string]any{"type": "content_block_start", "index": 0, "content_block": map[string]any{"type": "text", "text": ""}})
				sendSseEvent(w, flusher, "content_block_delta", map[string]any{"type": "content_block_delta", "index": 0, "delta": map[string]any{"type": "text_delta", "text": fastReply}})
				sendSseEvent(w, flusher, "content_block_stop", map[string]any{"type": "content_block_stop", "index": 0})
				sendSseEvent(w, flusher, "message_delta", map[string]any{
					"type":  "message_delta",
					"delta": map[string]any{"stop_reason": "end_turn", "stop_sequence": nil},
					"usage": map[string]any{"output_tokens": 1},
				})
				sendSseEvent(w, flusher, "message_stop", map[string]any{"type": "message_stop"})
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id": msgID, "type": "message", "role": "assistant", "model": displayModel,
				"content":     []map[string]any{{"type": "text", "text": fastReply}},
				"stop_reason": "end_turn", "stop_sequence": nil,
				"usage": map[string]any{"input_tokens": 10, "cache_creation_input_tokens": 0, "cache_read_input_tokens": 0, "output_tokens": 1},
			})
			return
		}

		// 1. Streaming Anthropic /v1/messages
		if isStream && path == "/v1/messages" {
			w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
			w.Header().Set("Cache-Control", "no-cache, no-transform")
			w.Header().Set("Connection", "keep-alive")
			w.Header().Set("X-Accel-Buffering", "no")
			flusher, _ := w.(http.Flusher)

			var sseMu sync.Mutex
			emit := func(ev string, data any) {
				sseMu.Lock()
				sendSseEvent(w, flusher, ev, data)
				sseMu.Unlock()
			}

			emit("message_start", map[string]any{
				"type": "message_start",
				"message": map[string]any{
					"id": msgID, "type": "message", "role": "assistant", "model": displayModel,
					"content": []any{}, "stop_reason": nil, "stop_sequence": nil,
					"usage": map[string]any{"input_tokens": 0, "cache_creation_input_tokens": 0, "cache_read_input_tokens": 0, "output_tokens": 1},
				},
			})

			curBlockType := ""
			nextIdx := 0
			closeBlock := func() {
				if curBlockType == "thinking" {
					emit("content_block_delta", map[string]any{
						"type": "content_block_delta", "index": nextIdx - 1,
						"delta": map[string]any{"type": "signature_delta", "signature": "agy-gemini-4-argon-signature"},
					})
					emit("content_block_stop", map[string]any{"type": "content_block_stop", "index": nextIdx - 1})
				} else if curBlockType == "text" {
					emit("content_block_stop", map[string]any{"type": "content_block_stop", "index": nextIdx - 1})
				}
				curBlockType = ""
			}

			onDelta := func(chunk DeltaChunk) {
				if chunk.Type == "thinking" && chunk.Thinking != "" {
					if curBlockType != "thinking" {
						closeBlock()
						curBlockType = "thinking"
						idx := nextIdx
						nextIdx++
						emit("content_block_start", map[string]any{
							"type": "content_block_start", "index": idx,
							"content_block": map[string]any{"type": "thinking", "thinking": ""},
						})
					}
					emit("content_block_delta", map[string]any{
						"type": "content_block_delta", "index": nextIdx - 1,
						"delta": map[string]any{"type": "thinking_delta", "thinking": chunk.Thinking},
					})
				} else if chunk.Type == "text" && chunk.Text != "" {
					if curBlockType != "text" {
						closeBlock()
						curBlockType = "text"
						idx := nextIdx
						nextIdx++
						emit("content_block_start", map[string]any{
							"type": "content_block_start", "index": idx,
							"content_block": map[string]any{"type": "text", "text": ""},
						})
					}
					emit("content_block_delta", map[string]any{
						"type": "content_block_delta", "index": nextIdx - 1,
						"delta": map[string]any{"type": "text_delta", "text": chunk.Text},
					})
				}
			}

			donePing := make(chan struct{})
			go func() {
				tk := time.NewTicker(10 * time.Second)
				defer tk.Stop()
				for {
					select {
					case <-donePing:
						return
					case <-tk.C:
						emit("ping", map[string]any{"type": "ping"})
					}
				}
			}()

			res, err := gs.HandleMessagesRequest(r, body, messages, onDelta)
			close(donePing)
			closeBlock()
			if err == nil {
				if softErr := DetectAntigravitySoftError(res.Text); softErr != nil {
					err = softErr
				}
			}
			if err != nil {
				_, errType, _, errMsg := classifyUpstreamError(err)
				LogEvent("HTTP_ERR", fmt.Sprintf("REQ #%d Failed: %v", reqID, err), nil)
				emit("error", map[string]any{
					"type":  "error",
					"error": map[string]any{"type": errType, "message": errMsg},
				})
				return
			}

			if len(res.ToolCalls) == 0 && nextIdx == 0 {
				idx := nextIdx
				nextIdx++
				emit("content_block_start", map[string]any{"type": "content_block_start", "index": idx, "content_block": map[string]any{"type": "text", "text": ""}})
				emit("content_block_delta", map[string]any{"type": "content_block_delta", "index": idx, "delta": map[string]any{"type": "text_delta", "text": res.Text}})
				emit("content_block_stop", map[string]any{"type": "content_block_stop", "index": idx})
			}

			for _, tc := range res.ToolCalls {
				idx := nextIdx
				nextIdx++
				inJson, _ := json.Marshal(tc.Input)
				emit("content_block_start", map[string]any{
					"type": "content_block_start", "index": idx,
					"content_block": map[string]any{"type": "tool_use", "id": tc.ID, "name": tc.Name, "input": map[string]any{}},
				})
				emit("content_block_delta", map[string]any{
					"type": "content_block_delta", "index": idx,
					"delta": map[string]any{"type": "input_json_delta", "partial_json": string(inJson)},
				})
				emit("content_block_stop", map[string]any{"type": "content_block_stop", "index": idx})
			}

			inTok := 0
			crTok := 0
			outTok := 16
			if res.Usage != nil {
				inTok = res.Usage.InputTokens
				crTok = res.Usage.CachedReadTokens
				outTok = res.Usage.OutputTokens
			}
			stopReason := "end_turn"
			if len(res.ToolCalls) > 0 {
				stopReason = "tool_use"
			}
			emit("message_delta", map[string]any{
				"type":  "message_delta",
				"delta": map[string]any{"stop_reason": stopReason, "stop_sequence": nil},
				"usage": map[string]any{
					"input_tokens":                inTok,
					"cache_creation_input_tokens": 0,
					"cache_read_input_tokens":     crTok,
					"output_tokens":               outTok,
				},
			})
			emit("message_stop", map[string]any{"type": "message_stop"})
			return
		}

		// 2. Streaming OpenAI /v1/chat/completions
		if isStream && path == "/v1/chat/completions" {
			w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
			w.Header().Set("Cache-Control", "no-cache, no-transform")
			w.Header().Set("Connection", "keep-alive")
			w.Header().Set("X-Accel-Buffering", "no")
			flusher, _ := w.(http.Flusher)

			createdTs := time.Now().Unix()
			cmplID := "chatcmpl-" + msgID
			var sseMu sync.Mutex
			sendChunk := func(delta map[string]any, finish any, usage map[string]any) {
				sseMu.Lock()
				defer sseMu.Unlock()
				payload := map[string]any{
					"id":      cmplID,
					"object":  "chat.completion.chunk",
					"created": createdTs,
					"model":   displayModel,
					"choices": []map[string]any{{"index": 0, "delta": delta, "finish_reason": finish}},
				}
				if usage != nil {
					payload["usage"] = usage
				}
				b, _ := json.Marshal(payload)
				_, _ = fmt.Fprintf(w, "data: %s\n\n", string(b))
				if flusher != nil {
					flusher.Flush()
				}
			}

			sendChunk(map[string]any{"role": "assistant", "content": ""}, nil, nil)
			doneChatPing := make(chan struct{})
			go func() {
				tk := time.NewTicker(15 * time.Second)
				defer tk.Stop()
				for {
					select {
					case <-doneChatPing:
						return
					case <-tk.C:
						sseMu.Lock()
						_, _ = fmt.Fprint(w, ": ping\n\n")
						if flusher != nil {
							flusher.Flush()
						}
						sseMu.Unlock()
					}
				}
			}()
			onDelta := func(c DeltaChunk) {
				if c.Type == "thinking" && c.Thinking != "" {
					sendChunk(map[string]any{"reasoning_content": c.Thinking}, nil, nil)
				} else if c.Type == "text" && c.Text != "" {
					sendChunk(map[string]any{"content": c.Text}, nil, nil)
				}
			}
			res, err := gs.HandleMessagesRequest(r, body, messages, onDelta)
			close(doneChatPing)
			if err == nil {
				if softErr := DetectAntigravitySoftError(res.Text); softErr != nil {
					err = softErr
				}
			}
			if err != nil {
				_, errType, _, errMsg := classifyUpstreamError(err)
				LogEvent("HTTP_ERR", fmt.Sprintf("REQ #%d Failed: %v", reqID, err), nil)
				sseMu.Lock()
				b, _ := json.Marshal(map[string]any{
					"error": map[string]any{"type": errType, "message": errMsg},
				})
				_, _ = fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", string(b))
				if flusher != nil {
					flusher.Flush()
				}
				sseMu.Unlock()
				return
			}
			for i, tc := range res.ToolCalls {
				inJson, _ := json.Marshal(tc.Input)
				sendChunk(map[string]any{
					"tool_calls": []map[string]any{
						{
							"index": i, "id": tc.ID, "type": "function",
							"function": map[string]any{"name": tc.Name, "arguments": string(inJson)},
						},
					},
				}, nil, nil)
			}
			inTok, crTok, outTok := 0, 0, 16
			if res.Usage != nil {
				inTok, crTok, outTok = res.Usage.InputTokens, res.Usage.CachedReadTokens, res.Usage.OutputTokens
			}
			fin := "stop"
			if len(res.ToolCalls) > 0 {
				fin = "tool_calls"
			}
			sendChunk(map[string]any{}, fin, map[string]any{
				"prompt_tokens":         inTok + crTok,
				"completion_tokens":     outTok,
				"total_tokens":          inTok + crTok + outTok,
				"prompt_tokens_details": map[string]any{"cached_tokens": crTok},
			})
			_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
			if flusher != nil {
				flusher.Flush()
			}
			return
		}

		// 3. Streaming OpenAI /v1/responses
		if isStream && path == "/v1/responses" {
			w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
			w.Header().Set("Cache-Control", "no-cache, no-transform")
			w.Header().Set("Connection", "keep-alive")
			w.Header().Set("X-Accel-Buffering", "no")
			flusher, _ := w.(http.Flusher)

			seqNum := 0
			respID := "resp_" + msgID[4:]
			createdTs := time.Now().Unix()
			buildRespObj := func(st string, output []any, usage any) map[string]any {
				return map[string]any{
					"id": respID, "object": "response", "created_at": createdTs,
					"status": st, "model": displayModel, "output": output,
					"error": nil, "incomplete_details": nil, "usage": usage,
					"parallel_tool_calls": true, "tool_choice": "auto", "store": false,
				}
			}
			var sseMu sync.Mutex
			sendEv := func(evName string, fields map[string]any) {
				sseMu.Lock()
				defer sseMu.Unlock()
				data := map[string]any{"type": evName, "sequence_number": seqNum}
				seqNum++
				for k, v := range fields {
					data[k] = v
				}
				b, _ := json.Marshal(data)
				_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", evName, string(b))
				if flusher != nil {
					flusher.Flush()
				}
			}

			sendEv("response.created", map[string]any{"response": buildRespObj("in_progress", []any{}, nil)})
			sendEv("response.in_progress", map[string]any{"response": buildRespObj("in_progress", []any{}, nil)})

			msgOpen := false
			outMsgID := "msg_" + randomHex(12)
			var textChunks []string
			onDelta := func(c DeltaChunk) {
				if c.Type == "text" && c.Text != "" {
					if !msgOpen {
						msgOpen = true
						sendEv("response.output_item.added", map[string]any{
							"output_index": 0,
							"item":         map[string]any{"type": "message", "id": outMsgID, "status": "in_progress", "role": "assistant", "content": []any{}},
						})
						sendEv("response.content_part.added", map[string]any{
							"item_id": outMsgID, "output_index": 0, "content_index": 0,
							"part": map[string]any{"type": "output_text", "text": "", "annotations": []any{}},
						})
					}
					textChunks = append(textChunks, c.Text)
					sendEv("response.output_text.delta", map[string]any{
						"item_id": outMsgID, "output_index": 0, "content_index": 0, "delta": c.Text, "logprobs": []any{},
					})
				}
			}

			res, err := gs.HandleMessagesRequest(r, body, messages, onDelta)
			if err == nil {
				if softErr := DetectAntigravitySoftError(res.Text); softErr != nil {
					err = softErr
				}
			}
			if err != nil {
				_, errType, _, errMsg := classifyUpstreamError(err)
				LogEvent("HTTP_ERR", fmt.Sprintf("REQ #%d Failed: %v", reqID, err), nil)
				failedObj := buildRespObj("failed", []any{}, nil)
				failedObj["error"] = map[string]any{"code": errType, "message": errMsg}
				sendEv("response.failed", map[string]any{"response": failedObj})
				return
			}
			var outItems []any
			if msgOpen {
				fullTxt := strings.Join(textChunks, "")
				part := map[string]any{"type": "output_text", "text": fullTxt, "annotations": []any{}}
				item := map[string]any{"type": "message", "id": outMsgID, "status": "completed", "role": "assistant", "content": []any{part}}
				sendEv("response.output_text.done", map[string]any{"item_id": outMsgID, "output_index": 0, "content_index": 0, "text": fullTxt, "logprobs": []any{}})
				sendEv("response.content_part.done", map[string]any{"item_id": outMsgID, "output_index": 0, "content_index": 0, "part": part})
				sendEv("response.output_item.done", map[string]any{"output_index": 0, "item": item})
				outItems = append(outItems, item)
			}
			for _, tc := range res.ToolCalls {
				idx := len(outItems)
				if customToolNames[tc.Name] {
					rawIn, _ := tc.Input["input"].(string)
					item := map[string]any{"type": "custom_tool_call", "id": "ctc_" + tc.ID[len(tc.ID)-12:], "call_id": tc.ID, "name": tc.Name, "input": rawIn, "status": "completed"}
					sendEv("response.output_item.added", map[string]any{"output_index": idx, "item": map[string]any{"type": "custom_tool_call", "id": item["id"], "call_id": tc.ID, "name": tc.Name, "input": "", "status": "in_progress"}})
					sendEv("response.custom_tool_call_input.delta", map[string]any{"item_id": item["id"], "output_index": idx, "delta": rawIn})
					sendEv("response.custom_tool_call_input.done", map[string]any{"item_id": item["id"], "output_index": idx, "input": rawIn})
					sendEv("response.output_item.done", map[string]any{"output_index": idx, "item": item})
					outItems = append(outItems, item)
				} else {
					inJson, _ := json.Marshal(tc.Input)
					argsStr := string(inJson)
					item := map[string]any{"type": "function_call", "id": "fc_" + tc.ID[len(tc.ID)-12:], "call_id": tc.ID, "name": tc.Name, "arguments": argsStr, "status": "completed"}
					sendEv("response.output_item.added", map[string]any{"output_index": idx, "item": map[string]any{"type": "function_call", "id": item["id"], "call_id": tc.ID, "name": tc.Name, "arguments": "", "status": "in_progress"}})
					sendEv("response.function_call_arguments.delta", map[string]any{"item_id": item["id"], "output_index": idx, "delta": argsStr})
					sendEv("response.function_call_arguments.done", map[string]any{"item_id": item["id"], "output_index": idx, "arguments": argsStr})
					sendEv("response.output_item.done", map[string]any{"output_index": idx, "item": item})
					outItems = append(outItems, item)
				}
			}
			inTok, crTok, outTok := 0, 0, 16
			if res.Usage != nil {
				inTok, crTok, outTok = res.Usage.InputTokens, res.Usage.CachedReadTokens, res.Usage.OutputTokens
			}
			sendEv("response.completed", map[string]any{
				"response": buildRespObj("completed", outItems, map[string]any{
					"input_tokens":          inTok + crTok,
					"input_tokens_details":  map[string]any{"cached_tokens": crTok},
					"output_tokens":         outTok,
					"output_tokens_details": map[string]any{"reasoning_tokens": 0},
					"total_tokens":          inTok + crTok + outTok,
				}),
			})
			return
		}

		// 4. Non-streaming responses
		res, err := gs.HandleMessagesRequest(r, body, messages, nil)
		if err == nil {
			if softErr := DetectAntigravitySoftError(res.Text); softErr != nil {
				err = softErr
			}
		}
		if err != nil {
			statusCode, errType, retryAfter, errMsg := classifyUpstreamError(err)
			LogEvent("HTTP_ERR", fmt.Sprintf("REQ #%d Failed: %v", reqID, err), nil)
			w.Header().Set("Content-Type", "application/json")
			if retryAfter {
				w.Header().Set("Retry-After", "5")
			}
			w.WriteHeader(statusCode)
			_ = json.NewEncoder(w).Encode(map[string]any{"type": "error", "error": map[string]any{"type": errType, "message": errMsg}})
			return
		}

		inTok, crTok, outTok := 0, 0, 16
		if res.Usage != nil {
			inTok, crTok, outTok = res.Usage.InputTokens, res.Usage.CachedReadTokens, res.Usage.OutputTokens
		}

		if path == "/v1/responses" {
			var outItems []any
			if res.Text != "" || len(res.ToolCalls) == 0 {
				outItems = append(outItems, map[string]any{
					"type": "message", "id": msgID, "status": "completed", "role": "assistant",
					"content": []map[string]any{{"type": "output_text", "text": res.Text, "annotations": []any{}}},
				})
			}
			for _, tc := range res.ToolCalls {
				if customToolNames[tc.Name] {
					rawIn, _ := tc.Input["input"].(string)
					outItems = append(outItems, map[string]any{"type": "custom_tool_call", "id": "ctc_" + tc.ID[len(tc.ID)-12:], "call_id": tc.ID, "name": tc.Name, "input": rawIn, "status": "completed"})
				} else {
					inJson, _ := json.Marshal(tc.Input)
					outItems = append(outItems, map[string]any{"type": "function_call", "id": "fc_" + tc.ID[len(tc.ID)-12:], "call_id": tc.ID, "name": tc.Name, "arguments": string(inJson), "status": "completed"})
				}
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id": "resp_" + msgID[4:], "object": "response", "created_at": time.Now().Unix(),
				"status": "completed", "model": displayModel, "output": outItems, "output_text": res.Text,
				"usage": map[string]any{
					"input_tokens":          inTok + crTok,
					"input_tokens_details":  map[string]any{"cached_tokens": crTok},
					"output_tokens":         outTok,
					"output_tokens_details": map[string]any{"reasoning_tokens": 0},
					"total_tokens":          inTok + crTok + outTok,
				},
			})
			return
		}

		if path == "/v1/chat/completions" {
			var msgObj map[string]any
			fin := "stop"
			if len(res.ToolCalls) > 0 {
				fin = "tool_calls"
				var tcs []map[string]any
				for _, tc := range res.ToolCalls {
					inJson, _ := json.Marshal(tc.Input)
					tcs = append(tcs, map[string]any{
						"id": tc.ID, "type": "function",
						"function": map[string]any{"name": tc.Name, "arguments": string(inJson)},
					})
				}
				var contentVal any
				if res.Text != "" {
					contentVal = res.Text
				}
				msgObj = map[string]any{"role": "assistant", "content": contentVal, "tool_calls": tcs}
			} else {
				msgObj = map[string]any{"role": "assistant", "content": res.Text}
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id": "chatcmpl-" + msgID, "object": "chat.completion", "created": time.Now().Unix(),
				"model":   displayModel,
				"choices": []map[string]any{{"index": 0, "message": msgObj, "finish_reason": fin}},
				"usage": map[string]any{
					"prompt_tokens":         inTok + crTok,
					"completion_tokens":     outTok,
					"total_tokens":          inTok + crTok + outTok,
					"prompt_tokens_details": map[string]any{"cached_tokens": crTok},
				},
			})
			return
		}

		var contentBlocks []map[string]any
		if res.Thinking != "" {
			contentBlocks = append(contentBlocks, map[string]any{
				"type": "thinking", "thinking": res.Thinking, "signature": "agy-gemini-4-argon-signature",
			})
		}
		if res.Text != "" {
			contentBlocks = append(contentBlocks, map[string]any{"type": "text", "text": res.Text})
		}
		for _, tc := range res.ToolCalls {
			contentBlocks = append(contentBlocks, map[string]any{
				"type": "tool_use", "id": tc.ID, "name": tc.Name, "input": tc.Input,
			})
		}
		if len(contentBlocks) == 0 {
			contentBlocks = append(contentBlocks, map[string]any{"type": "text", "text": ""})
		}
		stopReason := "end_turn"
		if len(res.ToolCalls) > 0 {
			stopReason = "tool_use"
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": msgID, "type": "message", "role": "assistant", "model": displayModel,
			"content": contentBlocks, "stop_reason": stopReason, "stop_sequence": nil,
			"usage": map[string]any{
				"input_tokens":                inTok,
				"cache_creation_input_tokens": 0,
				"cache_read_input_tokens":     crTok,
				"output_tokens":               outTok,
			},
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotFound)
	_, _ = w.Write([]byte(`{"error":"Not found"}`))
}

func main() {
	// container first-run: make sure the default workspace and log dir exist
	_ = os.MkdirAll(DefaultCwd, 0755)
	_ = os.MkdirAll(filepath.Dir(GatewayLogFile), 0700)
	if os.Getenv("GOGC") == "" {
		debug.SetGCPercent(25)
	}
	if os.Getenv("GOMEMLIMIT") == "" {
		debug.SetMemoryLimit(64 * 1024 * 1024)
	}
	if authConfigErr != nil {
		LogEvent("AUTH_CONFIG_ERR", authConfigErr.Error(), nil)
	}
	gw := NewGatewayServer()

	// Pre-warm SQLite index asynchronously so the first dashboard poll is instant, then release temp buffers to OS
	go func() {
		gwSids := getKnownGatewaySids()
		convDir := ConversationsDir()
		if GlobalStore.IsReady() {
			GlobalStore.SyncUsageStepsFromDir(convDir, gwSids)
		} else if entries, err := os.ReadDir(convDir); err == nil {
			for _, e := range entries {
				if !e.IsDir() && strings.HasSuffix(e.Name(), ".db") {
					GlobalDBCache.GetSteps(convDir+"/"+e.Name(), "WSL", gwSids)
				}
			}
		}
		runtime.GC()
		debug.FreeOSMemory()
	}()

	srv18890 := gatewayHTTPServer(fmt.Sprintf("%s:%d", ListenHost, GatewayPort), gw.GatewayOnlyHandler())
	srv18891 := gatewayHTTPServer(fmt.Sprintf("%s:%d", ListenHost, DashboardPort), gw.DashboardOnlyHandler())

	go func() {
		var modelIDs []string
		for _, m := range AgyModels {
			modelIDs = append(modelIDs, m.ID)
		}
		LogEvent("GATEWAY_READY", fmt.Sprintf("Go gateway listening on http://%s:%d & :%d", ListenHost, GatewayPort, DashboardPort), map[string]any{
			"engine":         "Go native",
			"defaultModel":   DefaultModelID,
			"models":         modelIDs,
			"authEnabled":    authEnabled(),
			"maxLiveWorkers": MaxLiveWorkers,
			"pureApiHome":    PureApiHome,
			"logFile":        GatewayLogFile,
		})
		if err := srv18890.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "18890 listen error: %v\n", err)
			os.Exit(1)
		}
	}()

	go func() {
		if err := srv18891.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "18891 listen error: %v\n", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := drainHTTPServers(ctx, srv18890, srv18891); err != nil {
		LogEvent("HTTP_DRAIN_TIMEOUT", "Grace period expired; remaining connections closed", nil)
	}
	gw.Close()
	GlobalEngine.StopEngine()
}
