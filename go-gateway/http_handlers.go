package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"agy-acp-gateway-go/internal/protocol"
)

var (
	sseHeartbeatInterval = 10 * time.Second
	sseWriteTimeout      = 30 * time.Second
)

func setSseWriteDeadline(w io.Writer) {
	if rw, ok := w.(http.ResponseWriter); ok {
		_ = http.NewResponseController(rw).SetWriteDeadline(time.Now().Add(sseWriteTimeout))
	}
}

func clearSseWriteDeadline(w io.Writer) {
	if rw, ok := w.(http.ResponseWriter); ok {
		_ = http.NewResponseController(rw).SetWriteDeadline(time.Time{})
	}
}

func sendSseEvent(w io.Writer, flusher http.Flusher, evName string, data any) {
	setSseWriteDeadline(w)
	b, _ := json.Marshal(data)
	_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", evName, string(b))
	if flusher != nil {
		flusher.Flush()
	}
}

func sendSseComment(w io.Writer, flusher http.Flusher, comment string) {
	setSseWriteDeadline(w)
	_, _ = fmt.Fprintf(w, ": %s\n\n", comment)
	if flusher != nil {
		flusher.Flush()
	}
}

func setAgyTurnHeaders(w http.ResponseWriter, u *TurnUsage) {
	setAgyTurnHeadersOn(w.Header(), u)
}

func setAgyTurnHeadersOn(h http.Header, u *TurnUsage) {
	if h == nil || u == nil {
		return
	}
	if u.ServedModel != "" {
		h.Set("X-Agy-Served-Model", u.ServedModel)
	}
	if u.CacheWarning != "" {
		h.Set("X-Agy-Cache-Warning", u.CacheWarning)
	} else {
		h.Del("X-Agy-Cache-Warning")
	}
}

func effectiveReasoningTokens(res TurnResult, outTok int) int {
	thTok := 0
	if res.Usage != nil && res.Usage.ThinkingTokens > 0 {
		thTok = res.Usage.ThinkingTokens
	} else if len(res.Thinking) > 0 {
		thTok = int(math.Round(float64(len(res.Thinking)) / 3.2))
		if thTok < 1 {
			thTok = 1
		}
	}
	if thTok > outTok {
		thTok = outTok
	}
	if thTok < 0 {
		thTok = 0
	}
	return thTok
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

func isDataPlaneCORSPath(path string) bool {
	return strings.HasPrefix(path, "/v1/") || strings.HasPrefix(path, "/v1beta/")
}

func isDirectLoopbackSameOrigin(r *http.Request) bool {
	if r == nil || len(r.Header.Values("Origin")) > 1 {
		return false
	}
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin == "" {
		return false
	}
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return false
	}
	wantScheme := "http"
	if r.TLS != nil {
		wantScheme = "https"
	}
	if !strings.EqualFold(u.Scheme, wantScheme) {
		return false
	}
	reqHost := strings.TrimSpace(r.Host)
	return isAllowedLoopbackHost(reqHost) && strings.EqualFold(reqHost, u.Host)
}

func applyCORSAndSecurityGuards(w http.ResponseWriter, r *http.Request) bool {
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	isDataPlane := isDataPlaneCORSPath(r.URL.Path)
	if isDataPlane && origin != "" && len(r.Header.Values("Origin")) == 1 && AllowedCorsOrigins[origin] {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Expose-Headers", "X-Agy-Served-Model, X-Agy-Cache-Warning, X-Agy-Account, X-Agy-Group, X-Agy-Route-Mode, X-Agy-Failover-Count")
		w.Header().Set("Vary", "Origin")
	}
	isV1Beta := strings.HasPrefix(r.URL.Path, "/v1beta/")
	if !requestAuthActive(r.URL.Path) {
		if !isAllowedLoopbackHost(r.Host) {
			if isV1Beta {
				writeGeminiError(w, http.StatusForbidden, "non-loopback Host header rejected on unauthenticated gateway")
				return false
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"type":"error","error":{"type":"permission_error","message":"non-loopback Host header rejected on unauthenticated gateway"}}`))
			return false
		}
		allowedDataPlaneCORS := isDataPlane && len(r.Header.Values("Origin")) == 1 && AllowedCorsOrigins[origin]
		if origin != "" && !allowedDataPlaneCORS && !isDirectLoopbackSameOrigin(r) {
			if isV1Beta {
				writeGeminiError(w, http.StatusForbidden, "cross-origin browser request rejected on unauthenticated loopback gateway")
				return false
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"type":"error","error":{"type":"permission_error","message":"cross-origin browser request rejected on unauthenticated loopback gateway"}}`))
			return false
		}
	}
	if r.Method == http.MethodPost && (strings.HasPrefix(r.URL.Path, "/v1/") || isV1Beta) {
		ct := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Type")))
		if (!dataPlaneAuthEnabled() && !strings.HasPrefix(ct, "application/json")) || (ct != "" && !strings.HasPrefix(ct, "application/json")) {
			if isV1Beta {
				writeGeminiError(w, http.StatusUnsupportedMediaType, "Content-Type must be application/json")
				return false
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnsupportedMediaType)
			_, _ = w.Write([]byte(`{"type":"error","error":{"type":"invalid_request_error","message":"Content-Type must be application/json"}}`))
			return false
		}
	}
	return true
}

func writeMultimodalOrBadRequest(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	var mmErr *MultimodalValidationError
	if errors.As(err, &mmErr) && mmErr != nil && mmErr.StatusCode > 0 {
		status = mmErr.StatusCode
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"type":  "error",
		"error": map[string]any{"type": "invalid_request_error", "message": err.Error()},
	})
}

func estimateCountTokens(rawBody []byte) (int, error) {
	if len(rawBody) == 0 {
		return 0, errors.New("empty request body")
	}
	var body map[string]any
	if err := unmarshalJSONPreserveNumbers(rawBody, &body); err != nil || body == nil {
		return 0, errors.New("Malformed JSON in count_tokens request")
	}
	return estimateCountTokensFromBody(body)
}

func estimateCountTokensFromBody(body map[string]any) (int, error) {
	if body == nil {
		return 0, errors.New("Malformed JSON in count_tokens request")
	}
	var messages []map[string]any
	if rawMsgs, ok := body["messages"].([]any); ok {
		for _, rm := range rawMsgs {
			if m, okM := rm.(map[string]any); okM {
				messages = append(messages, m)
			}
		}
	}
	if err := CanonicalizeAndValidateMessages(body, messages, "anthropic"); err != nil {
		return 0, err
	}
	reqModel, _ := body["model"].(string)
	sel := ResolveRequestedModel(reqModel, body)
	_, imgCount, _, err := validateLatestTurnImageBudget(messages, sel.Model)
	if err != nil {
		return 0, err
	}
	if maxImgs := effectiveModelMaxImages(sel.Model); maxImgs > 0 && imgCount > maxImgs {
		imgCount = maxImgs
	}

	charCount := len(normalizeSystemText(body["system"]))
	for _, m := range messages {
		charCount += len(formatSingleMessageText(m))
	}
	if rawTools, ok := body["tools"].([]any); ok {
		if b, err := json.Marshal(rawTools); err == nil {
			charCount += len(b)
		}
	}
	if soInst := extractStructuredOutputInstruction(body); soInst != "" {
		charCount += len(soInst)
	}
	est := int(math.Ceil(float64(charCount)/4.0)) + imgCount*1600
	if est < 1 {
		est = 1
	}
	return est, nil
}

// isDashboardAuthPath reports whether path belongs to the admin plane guarded by checkDashboardAuth.
func isDashboardAuthPath(path string) bool {
	switch path {
	case "/", "/dashboard", "/api/dashboard", "/api/quota", "/api/upgrade", "/api/upgrade/approve",
		"/api/accounts", "/api/client-keys", "/health", "/v1/health", "/metrics":
		return true
	}
	return strings.HasPrefix(path, "/api/accounts/") || strings.HasPrefix(path, "/api/client-keys/")
}

// requestAuthActive reports whether the plane serving path enforces credentials; unauthenticated
// planes additionally get loopback Host / same-origin hardening in applyCORSAndSecurityGuards.
func requestAuthActive(path string) bool {
	if isDashboardAuthPath(path) {
		return dashboardAuthEnabled()
	}
	if strings.HasPrefix(path, "/v1/") || strings.HasPrefix(path, "/v1beta/") {
		return dataPlaneAuthEnabled()
	}
	return dashboardAuthEnabled() && dataPlaneAuthEnabled()
}

func (gs *GatewayServer) GatewayOnlyHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		if p == "/" || p == "/dashboard" || strings.HasPrefix(p, "/dashboard/") ||
			p == "/api" || strings.HasPrefix(p, "/api/") ||
			p == "/api/upgrade" || p == "/api/upgrade/approve" ||
			p == "/metrics" {
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
		if strings.HasPrefix(p, "/v1/") || strings.HasPrefix(p, "/v1beta/") || strings.HasPrefix(p, "/mcp/") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"Not found on dashboard port"}`))
			return
		}
		gs.ServeHTTP(w, r)
	})
}

const allowedCorsRequestHeaders = "Content-Type, Authorization, x-api-key, x-goog-api-key, x-goog-user-project, x-agy-account, x-agy-group, x-agy-requested-with, anthropic-version, anthropic-beta, anthropic-dangerous-direct-browser-access, OpenAI-Beta, OpenAI-Organization, OpenAI-Project, x-claude-code-session-id, x-pi-session-id, x-session-id, session-id, session_id, thread-id, x-conversation-id, conversation_id, openai-conversation-id, x-client-session-id, x-claude-code-agent-id, x-agent-id, x-subagent-id, x-claude-code-compaction, x-claude-code-context-compacted, x-claude-code-request-class"

func shortItemID(prefix, id string) string {
	clean := strings.TrimSpace(id)
	if clean == "" {
		return prefix + randomHex(6)
	}
	if len(clean) > 12 {
		clean = clean[len(clean)-12:]
	}
	return prefix + clean
}

func respParallelToolCalls(body map[string]any) bool {
	if body != nil {
		if b, ok := body["parallel_tool_calls"].(bool); ok {
			return b
		}
	}
	return true
}

func respToolChoiceEcho(body map[string]any) any {
	if body != nil && body["tool_choice"] != nil {
		return body["tool_choice"]
	}
	return "auto"
}

func hasNonEmptyThreadField(body map[string]any) bool {
	if body == nil {
		return false
	}
	raw, exists := body["thread"]
	if !exists || raw == nil {
		return false
	}
	if s, ok := raw.(string); ok {
		return strings.TrimSpace(s) != ""
	}
	return true
}

func extractNonEmptyPreviousResponseID(body map[string]any) (string, bool) {
	if body == nil {
		return "", false
	}
	raw, exists := body["previous_response_id"]
	if !exists || raw == nil {
		return "", false
	}
	if s, ok := raw.(string); ok {
		trimmed := strings.TrimSpace(s)
		return trimmed, trimmed != ""
	}
	return fmt.Sprint(raw), true
}

func contentValueToBlockSlice(content any) []any {
	switch c := content.(type) {
	case nil:
		return nil
	case string:
		if c == "" {
			return nil
		}
		return []any{map[string]any{"type": "text", "text": c}}
	case []any:
		out := make([]any, len(c))
		copy(out, c)
		return out
	case []map[string]any:
		out := make([]any, len(c))
		for i, item := range c {
			out[i] = item
		}
		return out
	case map[string]any:
		return []any{c}
	default:
		if b, err := json.Marshal(c); err == nil {
			return []any{map[string]any{"type": "text", "text": string(b)}}
		}
		return nil
	}
}

func appendChatAssistantBlocks(convMsgs []any, blocks []any, rawMsg map[string]any) []any {
	if len(convMsgs) > 0 {
		if last, okL := convMsgs[len(convMsgs)-1].(map[string]any); okL && last["role"] == "assistant" {
			prevBlocks := contentValueToBlockSlice(last["content"])
			last["content"] = append(prevBlocks, blocks...)
			if rawMsg != nil && rawMsg["audio"] != nil {
				last["audio"] = rawMsg["audio"]
			}
			return convMsgs
		}
	}
	if rawMsg != nil && len(blocks) == 0 {
		content := ""
		if s, ok := rawMsg["content"].(string); ok && s != "" {
			content = s
		}
		newMsg := map[string]any{"role": "assistant", "content": content}
		if rawMsg["audio"] != nil {
			newMsg["audio"] = rawMsg["audio"]
		}
		return append(convMsgs, newMsg)
	}
	newMsg := map[string]any{"role": "assistant", "content": blocks}
	if rawMsg != nil && rawMsg["audio"] != nil {
		newMsg["audio"] = rawMsg["audio"]
	}
	return append(convMsgs, newMsg)
}

func isRecognizedContentPartMap(ov map[string]any) bool {
	if ov == nil {
		return false
	}
	if mapField(ov, "inlineData", "inline_data") != nil || mapField(ov, "fileData", "file_data") != nil {
		return true
	}
	bType, _ := ov["type"].(string)
	switch bType {
	case "text", "input_text", "output_text":
		_, ok := ov["text"].(string)
		return ok
	case "image_url", "input_image":
		return ov["image_url"] != nil
	case "image":
		return ov["source"] != nil || ov["data"] != nil
	case "image_ref":
		_, ok := ov["url"].(string)
		return ok
	case "input_audio":
		return ov["input_audio"] != nil
	case "audio_url":
		return ov["audio_url"] != nil
	case "video_url":
		return ov["video_url"] != nil
	case "document":
		_, ok := ov["source"].(map[string]any)
		return ok
	case "input_file":
		return ov["file_data"] != nil || ov["file_id"] != nil
	}
	return false
}

func normalizeFunctionOutputContent(outVal any) any {
	if outVal == nil {
		return ""
	}
	switch ov := outVal.(type) {
	case string, []any, []map[string]any:
		return ov
	case map[string]any:
		if isRecognizedContentPartMap(ov) {
			return ov
		}
		if b, err := json.Marshal(ov); err == nil {
			return string(b)
		}
		return fmt.Sprint(ov)
	default:
		if b, err := json.Marshal(ov); err == nil {
			return string(b)
		}
		return fmt.Sprint(ov)
	}
}

func (gs *GatewayServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	reqID := gs.reqCounter.Add(1)
	path := r.URL.Path
	if r.Method == http.MethodPost && (strings.HasPrefix(path, "/v1/") || strings.HasPrefix(path, "/v1beta/")) {
		r = withHttpRequestStart(r, time.Now())
	}

	if r.Method == http.MethodOptions {
		origin := strings.TrimSpace(r.Header.Get("Origin"))
		if isDataPlaneCORSPath(path) && origin != "" && len(r.Header.Values("Origin")) == 1 && AllowedCorsOrigins[origin] {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", allowedCorsRequestHeaders)
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// Single-Port Multi-Tenant MCP Router
	if strings.HasPrefix(path, "/mcp/") {
		if !isLoopbackRemoteAddr(r.RemoteAddr) {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		tenantKey := strings.TrimPrefix(path, "/mcp/")
		gs.mu.RLock()
		sess := gs.tenants[tenantKey]
		gs.mu.RUnlock()
		if sess == nil {
			if McpTrace {
				LogEvent("MCP_404", "unknown tenant on /mcp/[REDACTED]", map[string]any{"method": r.Method})
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

	if isDashboardAuthPath(path) {
		if !checkDashboardAuth(w, r) {
			return
		}
	} else if strings.HasPrefix(path, "/v1/") || strings.HasPrefix(path, "/v1beta/") {
		if !checkAPIAuth(w, r) {
			return
		}
		if !enforceClientKeyModelAllowlist(w, r) {
			return
		}
		var handled bool
		var finishRoute func()
		r, w, finishRoute, handled = GlobalAccountManager.TryRouteAPIRequestWithFinish(w, r)
		if finishRoute != nil {
			defer finishRoute()
		}
		if handled {
			return
		}
	}

	if path == "/api/accounts" || strings.HasPrefix(path, "/api/accounts/") {
		HandleAccountsAPI(w, r)
		return
	}

	if path == "/api/client-keys" || strings.HasPrefix(path, "/api/client-keys/") {
		HandleClientKeysAPI(w, r)
		return
	}

	if r.Method == http.MethodGet && (path == "/" || path == "/dashboard") {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
		w.Header().Set("Pragma", "no-cache")
		_, _ = w.Write([]byte(RenderDashboardHTML()))
		return
	}

	if r.Method == http.MethodGet && path == "/api/dashboard" {
		force := r.URL.Query().Get("refresh") == "1"
		localOnly := r.URL.Query().Get("local_only") == "1" || strings.TrimSpace(r.Header.Get("X-Agy-Federated-Hop")) != ""
		var payload map[string]any
		if localOnly {
			payload = BuildDashboardPayload(gs, force)
		} else {
			payload = BuildFederatedDashboardPayload(gs, force)
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
		_ = json.NewEncoder(w).Encode(payload)
		return
	}

	if r.Method == http.MethodGet && path == "/api/upgrade" {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		_ = json.NewEncoder(w).Encode(BuildUpgradePayload())
		return
	}

	if r.Method == http.MethodPost && path == "/api/upgrade/approve" {
		HandleUpgradeApprove(w, r)
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
		modelsList, boundAccID := GlobalAccountManager.FilterModelsForRequest(r, ListModelsForAPI())
		if boundAccID != "" {
			w.Header().Set("X-Agy-Account", boundAccID)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"object": "list",
			"data":   modelsList,
		})
		return
	}

	if strings.HasPrefix(path, "/v1beta/") {
		gs.handleGeminiV1Beta(w, r, reqID)
		return
	}

	if r.Method == http.MethodPost && path == "/v1/messages/count_tokens" {
		var raw []byte
		var err error
		if preRead, ok := preReadBodyFromContext(r); ok {
			raw = preRead
		} else {
			r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)
			raw, err = io.ReadAll(r.Body)
		}
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
		if len(raw) == 0 {
			writeMultimodalOrBadRequest(w, errors.New("empty request body"))
			return
		}
		var parsedBody map[string]any
		if err := unmarshalJSONPreserveNumbers(raw, &parsedBody); err != nil || parsedBody == nil {
			writeMultimodalOrBadRequest(w, errors.New("Malformed JSON in count_tokens request"))
			return
		}
		raw = nil
		if hasNonEmptyThreadField(parsedBody) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"type": "error",
				"error": map[string]any{
					"type":    "thread_not_found",
					"message": "Message thread storage is not supported on this gateway; please resend full message history without thread.",
				},
			})
			return
		}
		est, err := estimateCountTokensFromBody(parsedBody)
		if err != nil {
			writeMultimodalOrBadRequest(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"input_tokens": est})
		return
	}

	if r.Method == http.MethodPost && (path == "/v1/messages" || path == "/v1/chat/completions" || path == "/v1/responses") {
		if tripped, reason := IsTosTripped(); tripped {
			recordPrimaryDirectOutcome(r, http.StatusServiceUnavailable, "tos_circuit_breaker", "", reason)
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
		var rawBody []byte
		var err error
		if preRead, ok := preReadBodyFromContext(r); ok {
			rawBody = preRead
		} else {
			r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)
			rawBody, err = io.ReadAll(r.Body)
		}
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
		if err := unmarshalJSONPreserveNumbers(rawBody, &body); err != nil || body == nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"type":  "error",
				"error": map[string]any{"type": "invalid_request_error", "message": "Malformed JSON request body: " + fmt.Sprint(err)},
			})
			return
		}
		rawBody = nil

		if path == "/v1/messages" && hasNonEmptyThreadField(body) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"type": "error",
				"error": map[string]any{
					"type":    "thread_not_found",
					"message": "Message thread storage is not supported on this gateway; please resend full message history without thread.",
				},
			})
			return
		}
		if path == "/v1/responses" {
			if prevID, hasPrev := extractNonEmptyPreviousResponseID(body); hasPrev {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"type": "error",
					"error": map[string]any{
						"type":    "invalid_request_error",
						"code":    "previous_response_not_found",
						"param":   "previous_response_id",
						"message": fmt.Sprintf("Previous response with id %q not found (gateway responses use store=false; send full conversation input).", prevID),
					},
				})
				return
			}
		}

		customToolNames := make(map[string]bool)
		if path == "/v1/responses" {
			var sysParts []string
			if rawInst, hasInst := body["instructions"]; hasInst && rawInst != nil {
				instTxt, instErr := extractSystemTextStrict(rawInst, "instructions")
				if instErr != nil {
					writeMultimodalOrBadRequest(w, instErr)
					return
				}
				if instTxt != "" {
					sysParts = append(sysParts, instTxt)
				}
			}
			var items []any
			if rawIn, hasIn := body["input"]; hasIn && rawIn != nil {
				if sIn, ok := rawIn.(string); ok {
					items = []any{map[string]any{"type": "message", "role": "user", "content": sIn}}
				} else if arrIn, ok := rawIn.([]any); ok {
					items = arrIn
				} else {
					writeMultimodalOrBadRequest(w, errors.New("input must be a string or an array of input items"))
					return
				}
			}
			seenConvItem := false
			var convMsgs []any
			lastWasToolOutputs := false
			appendRespAssistantBlocks := func(blocks []any) {
				if len(convMsgs) > 0 {
					if last, ok := convMsgs[len(convMsgs)-1].(map[string]any); ok && last["role"] == "assistant" {
						prevBlocks := contentValueToBlockSlice(last["content"])
						last["content"] = append(prevBlocks, blocks...)
						lastWasToolOutputs = false
						return
					}
				}
				convMsgs = append(convMsgs, map[string]any{"role": "assistant", "content": blocks})
				lastWasToolOutputs = false
			}
			appendRespToolOutput := func(trBlock map[string]any) {
				if len(convMsgs) > 0 && lastWasToolOutputs {
					if last, ok := convMsgs[len(convMsgs)-1].(map[string]any); ok && last["role"] == "user" {
						if prevArr, okA := last["content"].([]any); okA {
							last["content"] = append(prevArr, trBlock)
							return
						}
					}
				}
				convMsgs = append(convMsgs, map[string]any{"role": "user", "content": []any{trBlock}})
				lastWasToolOutputs = true
			}
			for i, rawIt := range items {
				if len(convMsgs) > 0 {
					seenConvItem = true
				}
				it, ok := rawIt.(map[string]any)
				if !ok {
					writeMultimodalOrBadRequest(w, fmt.Errorf("input[%d] must be an object", i))
					return
				}
				t, _ := it["type"].(string)
				if t == "" && it["role"] != nil {
					t = "message"
				}
				switch t {
				case "message":
					role, _ := it["role"].(string)
					if !seenConvItem && (role == "system" || role == "developer") {
						sysTxt, sysErr := extractSystemTextStrict(it["content"], fmt.Sprintf("input[%d].content", i))
						if sysErr != nil {
							writeMultimodalOrBadRequest(w, sysErr)
							return
						}
						if sysTxt != "" {
							sysParts = append(sysParts, sysTxt)
						}
					} else if role == "assistant" {
						blocks := contentValueToBlockSlice(it["content"])
						if len(blocks) > 0 {
							appendRespAssistantBlocks(blocks)
						} else {
							appendRespAssistantBlocks([]any{map[string]any{"type": "text", "text": ""}})
						}
						seenConvItem = true
					} else {
						convMsgs = append(convMsgs, map[string]any{"role": "user", "content": it["content"]})
						lastWasToolOutputs = false
						seenConvItem = true
					}
				case "reasoning":
					seenConvItem = true
					continue
				case "function_call", "custom_tool_call":
					seenConvItem = true
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
					} else if argStr, okA := it["arguments"].(string); okA && strings.TrimSpace(argStr) != "" {
						_ = unmarshalJSONPreserveNumbers([]byte(argStr), &parsedArgs)
					} else if argMap, okM := it["arguments"].(map[string]any); okM {
						parsedArgs = argMap
					}
					tName, _ := it["name"].(string)
					appendRespAssistantBlocks([]any{
						map[string]any{"type": "tool_use", "id": cid, "name": tName, "input": parsedArgs},
					})
				case "function_call_output", "custom_tool_call_output":
					seenConvItem = true
					cid, _ := it["call_id"].(string)
					if cid == "" {
						cid, _ = it["id"].(string)
					}
					outVal := normalizeFunctionOutputContent(it["output"])
					appendRespToolOutput(map[string]any{
						"type":        "tool_result",
						"tool_use_id": cid,
						"content":     outVal,
					})
				default:
					writeMultimodalOrBadRequest(w, newMMError(http.StatusBadRequest, fmt.Sprintf("input[%d].type", i), fmt.Sprintf("unsupported input item type %q", t)))
					return
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
							firstLine := strings.TrimSpace(tDesc)
							if idx := strings.IndexByte(firstLine, '\n'); idx >= 0 {
								firstLine = strings.TrimSpace(firstLine[:idx])
							}
							const customHint = "Pass free-form text in arguments.input."
							desc := customHint
							if firstLine != "" {
								if len(firstLine)+len(customHint)+3 > 300 {
									firstLine = truncateRunes(firstLine, 300-len(customHint)-4)
								}
								desc = firstLine + " (" + customHint + ")"
							}
							normTools = append(normTools, map[string]any{
								"name":        tName,
								"description": desc,
								"parameters": map[string]any{
									"type": "object",
									"properties": map[string]any{
										"input": map[string]any{
											"type":        "string",
											"description": "Raw free-form text input for this custom tool.",
										},
									},
									"required": []string{"input"},
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
				lastWasToolOutputs := false
				appendChatAssistant := func(blocks []any, rawMsg map[string]any) {
					convMsgs = appendChatAssistantBlocks(convMsgs, blocks, rawMsg)
					lastWasToolOutputs = false
				}
				for i, rm := range rawMsgs {
					m, okM := rm.(map[string]any)
					if !okM {
						continue
					}
					role, _ := m["role"].(string)
					if role == "system" || role == "developer" {
						if len(convMsgs) == 0 {
							sysTxt, sysErr := extractSystemTextStrict(m["content"], fmt.Sprintf("messages[%d].content", i))
							if sysErr != nil {
								writeMultimodalOrBadRequest(w, sysErr)
								return
							}
							if sysTxt != "" {
								sysParts = append(sysParts, sysTxt)
							}
							continue
						}
						cloned := make(map[string]any, len(m))
						for k, v := range m {
							cloned[k] = v
						}
						cloned["role"] = "user"
						m = cloned
						role = "user"
					}
					if role == "tool" {
						tcID, _ := m["tool_call_id"].(string)
						if tcID == "" {
							tcID, _ = m["id"].(string)
						}
						outVal := normalizeFunctionOutputContent(m["content"])
						trBlock := map[string]any{"type": "tool_result", "tool_use_id": tcID, "content": outVal}
						if len(convMsgs) > 0 && lastWasToolOutputs {
							if last, okL := convMsgs[len(convMsgs)-1].(map[string]any); okL && last["role"] == "user" {
								if prevArr, okA := last["content"].([]any); okA {
									last["content"] = append(prevArr, trBlock)
									continue
								}
							}
						}
						convMsgs = append(convMsgs, map[string]any{
							"role":    "user",
							"content": []any{trBlock},
						})
						lastWasToolOutputs = true
					} else if role == "assistant" {
						var blocks []any
						blocks = append(blocks, contentValueToBlockSlice(m["content"])...)
						if tcs, okTC := m["tool_calls"].([]any); okTC && len(tcs) > 0 {
							for _, rtc := range tcs {
								tc, okT := rtc.(map[string]any)
								if !okT {
									continue
								}
								fn, _ := tc["function"].(map[string]any)
								fName, _ := fn["name"].(string)
								fArgsStr, _ := fn["arguments"].(string)
								parsedArgs := make(map[string]any)
								if strings.TrimSpace(fArgsStr) != "" {
									_ = unmarshalJSONPreserveNumbers([]byte(fArgsStr), &parsedArgs)
								}
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
							appendChatAssistant(blocks, m)
							continue
						}
						appendChatAssistant(blocks, m)
					} else {
						convMsgs = append(convMsgs, m)
						lastWasToolOutputs = false
					}
				}
				if body["system"] == nil && len(sysParts) > 0 {
					body["system"] = strings.Join(sysParts, "\n")
				}
				body["messages"] = convMsgs
			}
		}

		if body["max_tokens"] == nil {
			if mct, ok := toInt(body["max_completion_tokens"]); ok && mct > 0 {
				body["max_tokens"] = mct
			} else if mot, ok := toInt(body["max_output_tokens"]); ok && mot > 0 {
				body["max_tokens"] = mot
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

		if err := CanonicalizeAndValidateMessages(body, messages, path); err != nil {
			writeMultimodalOrBadRequest(w, err)
			return
		}

		reqModel, _ := body["model"].(string)
		selModel := ResolveRequestedModel(reqModel, body)
		if _, _, _, err := validateLatestTurnImageBudget(messages, selModel.Model); err != nil {
			writeMultimodalOrBadRequest(w, err)
			return
		}
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

		// Local fast replies exist only for Claude Code's own background probes, which only use
		// the Anthropic endpoint; OpenAI-protocol requests always reach the model.
		isSuggestion, isSpinner := false, false
		isQuotaProbe := false
		if path == "/v1/messages" {
			isSuggestion, isSpinner = detectClientUtilityProbe(messages)
			isQuotaProbe = maxTok == 1 && isClaudeCodeClient(r, body)
		}
		if isQuotaProbe || isSuggestion || isSpinner {
			markRoutedSkipHealth(r)
			fastReply := "OK"
			if isSuggestion {
				fastReply = ""
			} else if isSpinner {
				fastReply = "Analyzing project files and changes"
			}
			LogEvent("HTTP_FASTPATH", fmt.Sprintf("REQ #%d Fast-responding to utility probe", reqID), nil)
			if isStream {
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
			stream := protocol.NewSSEStreamSession(w, protocol.SSEStreamOptions{
				HeartbeatInterval: sseHeartbeatInterval,
				WriteTimeout:      sseWriteTimeout,
			})
			defer stream.ClearDeadline()

			msgStarted := false
			ensureMsgStartLocked := func(sw *protocol.StreamWriter, inT, crT int) {
				if msgStarted {
					return
				}
				msgStarted = true
				sw.WriteSSEEvent("message_start", map[string]any{
					"type": "message_start",
					"message": map[string]any{
						"id": msgID, "type": "message", "role": "assistant", "model": displayModel,
						"content": []any{}, "stop_reason": nil, "stop_sequence": nil,
						"usage": map[string]any{"input_tokens": inT, "cache_creation_input_tokens": 0, "cache_read_input_tokens": crT, "output_tokens": 0},
					},
				})
			}
			emit := func(ev string, data any) {
				stream.WriteSSEEvent(ev, data)
			}
			emitContent := func(ev string, data any) {
				stream.DoLocked(func(sw *protocol.StreamWriter) {
					ensureMsgStartLocked(sw, 0, 0)
					sw.WriteSSEEvent(ev, data)
				})
			}

			curBlockType := ""
			nextIdx := 0
			closeBlock := func() {
				if curBlockType == "thinking" {
					emitContent("content_block_delta", map[string]any{
						"type": "content_block_delta", "index": nextIdx - 1,
						"delta": map[string]any{"type": "signature_delta", "signature": "agy-gemini-4-argon-signature"},
					})
					emitContent("content_block_stop", map[string]any{"type": "content_block_stop", "index": nextIdx - 1})
				} else if curBlockType == "text" {
					emitContent("content_block_stop", map[string]any{"type": "content_block_stop", "index": nextIdx - 1})
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
						emitContent("content_block_start", map[string]any{
							"type": "content_block_start", "index": idx,
							"content_block": map[string]any{"type": "thinking", "thinking": ""},
						})
					}
					emitContent("content_block_delta", map[string]any{
						"type": "content_block_delta", "index": nextIdx - 1,
						"delta": map[string]any{"type": "thinking_delta", "thinking": chunk.Thinking},
					})
				} else if chunk.Type == "text" && chunk.Text != "" {
					if curBlockType != "text" {
						closeBlock()
						curBlockType = "text"
						idx := nextIdx
						nextIdx++
						emitContent("content_block_start", map[string]any{
							"type": "content_block_start", "index": idx,
							"content_block": map[string]any{"type": "text", "text": ""},
						})
					}
					emitContent("content_block_delta", map[string]any{
						"type": "content_block_delta", "index": nextIdx - 1,
						"delta": map[string]any{"type": "text_delta", "text": chunk.Text},
					})
				}
			}

			// Segmented SSE heartbeats: W3C comment before message_start, event: ping after.
			stream.StartHeartbeat(func(sw *protocol.StreamWriter) {
				if msgStarted {
					sw.WriteSSEEvent("ping", map[string]any{"type": "ping"})
				} else {
					sw.WriteRaw(": ping\n\n")
				}
			})
			defer stream.StopHeartbeat()

			r = withSessionWarnHook(r, func(warn string) {
				stream.SetPreHeader("X-Agy-Cache-Warning", warn)
			})
			res, err := gs.HandleMessagesRequest(r, body, messages, onDelta)
			stream.StopHeartbeat()
			closeBlock()
			if err == nil {
				if softErr := DetectAntigravitySoftError(res.Text); softErr != nil {
					err = softErr
				}
			}
			if err != nil {
				statusCode, errType, retryAfter, errMsg := classifyUpstreamError(err)
				raHdr := ""
				if retryAfter {
					raHdr = extractRetryAfterHeaderValue(errMsg)
				}
				recordPrimaryDirectOutcome(r, statusCode, errType, raHdr, errMsg)
				LogEvent("HTTP_ERR", fmt.Sprintf("REQ #%d Failed: %v", reqID, err), nil)
				stream.DoLocked(func(sw *protocol.StreamWriter) {
					if !sw.HeadersCommitted() {
						var extra map[string]string
						if retryAfter {
							extra = map[string]string{"Retry-After": raHdr}
						}
						sw.CommitErrorStatus(statusCode, extra, "")
					}
					sw.WriteSSEEvent("error", map[string]any{
						"type":  "error",
						"error": map[string]any{"type": errType, "message": errMsg},
					})
				})
				return
			}

			inTok, crTok, outTok := res.Usage.ClientTokens()
			stream.DoLocked(func(sw *protocol.StreamWriter) {
				if !sw.HeadersCommitted() {
					setAgyTurnHeadersOn(sw.Header(), res.Usage)
				}
				ensureMsgStartLocked(sw, inTok, crTok)
			})

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

			stopReason := "end_turn"
			if len(res.ToolCalls) > 0 {
				stopReason = "tool_use"
			} else if res.StopReason == "max_tokens" {
				stopReason = "max_tokens"
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
			stream := protocol.NewSSEStreamSession(w, protocol.SSEStreamOptions{
				HeartbeatInterval: sseHeartbeatInterval,
				WriteTimeout:      sseWriteTimeout,
			})
			defer stream.ClearDeadline()

			createdTs := time.Now().Unix()
			cmplID := "chatcmpl-" + msgID
			roleSent := false
			writeChunkLocked := func(sw *protocol.StreamWriter, delta map[string]any, finish any, usage map[string]any) {
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
				sw.WriteSSEData(payload)
			}
			sendChunk := func(delta map[string]any, finish any, usage map[string]any) {
				stream.DoLocked(func(sw *protocol.StreamWriter) {
					if !roleSent {
						roleSent = true
						writeChunkLocked(sw, map[string]any{"role": "assistant", "content": ""}, nil, nil)
					}
					writeChunkLocked(sw, delta, finish, usage)
				})
			}

			stream.StartHeartbeat(func(sw *protocol.StreamWriter) {
				sw.WriteRaw(": ping\n\n")
			})
			defer stream.StopHeartbeat()
			streamedChatText := false
			onDelta := func(c DeltaChunk) {
				if c.Type == "thinking" && c.Thinking != "" {
					sendChunk(map[string]any{"reasoning_content": c.Thinking}, nil, nil)
				} else if c.Type == "text" && c.Text != "" {
					streamedChatText = true
					sendChunk(map[string]any{"content": c.Text}, nil, nil)
				}
			}
			r = withSessionWarnHook(r, func(warn string) {
				stream.SetPreHeader("X-Agy-Cache-Warning", warn)
			})
			res, err := gs.HandleMessagesRequest(r, body, messages, onDelta)
			stream.StopHeartbeat()
			if err == nil {
				if softErr := DetectAntigravitySoftError(res.Text); softErr != nil {
					err = softErr
				}
			}
			if err != nil {
				statusCode, errType, retryAfter, errMsg := classifyUpstreamError(err)
				raHdr := ""
				if retryAfter {
					raHdr = extractRetryAfterHeaderValue(errMsg)
				}
				recordPrimaryDirectOutcome(r, statusCode, errType, raHdr, errMsg)
				LogEvent("HTTP_ERR", fmt.Sprintf("REQ #%d Failed: %v", reqID, err), nil)
				stream.DoLocked(func(sw *protocol.StreamWriter) {
					if !sw.HeadersCommitted() {
						var extra map[string]string
						if retryAfter {
							extra = map[string]string{"Retry-After": raHdr}
						}
						sw.CommitErrorStatus(statusCode, extra, "")
					}
					b, _ := json.Marshal(map[string]any{
						"error": map[string]any{"type": errType, "message": errMsg},
					})
					sw.WriteRaw(fmt.Sprintf("data: %s\n\ndata: [DONE]\n\n", string(b)))
				})
				return
			}
			stream.ApplyPreHeaders(func(h http.Header) {
				setAgyTurnHeadersOn(h, res.Usage)
			})
			if !streamedChatText && len(res.ToolCalls) == 0 && res.Text != "" {
				sendChunk(map[string]any{"content": res.Text}, nil, nil)
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
			inTok, crTok, outTok := res.Usage.ClientTokens()
			thTok := effectiveReasoningTokens(res, outTok)
			fin := "stop"
			if len(res.ToolCalls) > 0 {
				fin = "tool_calls"
			} else if res.StopReason == "max_tokens" {
				fin = "length"
			}
			sendChunk(map[string]any{}, fin, map[string]any{
				"prompt_tokens":             inTok + crTok,
				"completion_tokens":         outTok,
				"total_tokens":              inTok + crTok + outTok,
				"prompt_tokens_details":     map[string]any{"cached_tokens": crTok},
				"completion_tokens_details": map[string]any{"reasoning_tokens": thTok},
			})
			stream.WriteRaw("data: [DONE]\n\n")
			return
		}

		// 3. Streaming OpenAI /v1/responses
		if isStream && path == "/v1/responses" {
			stream := protocol.NewSSEStreamSession(w, protocol.SSEStreamOptions{
				DelayHeaders:      true,
				HeartbeatInterval: sseHeartbeatInterval,
				WriteTimeout:      sseWriteTimeout,
			})
			defer stream.ClearDeadline()

			seqNum := 0
			respID := "resp_" + msgID[4:]
			createdTs := time.Now().Unix()
			parallelTC := respParallelToolCalls(body)
			toolChoiceEcho := respToolChoiceEcho(body)
			buildRespObj := func(st string, output []any, usage any) map[string]any {
				return map[string]any{
					"id": respID, "object": "response", "created_at": createdTs,
					"status": st, "model": displayModel, "output": output,
					"error": nil, "incomplete_details": nil, "usage": usage,
					"parallel_tool_calls": parallelTC, "tool_choice": toolChoiceEcho, "store": false,
				}
			}
			respStarted := false
			writeEvLocked := func(sw *protocol.StreamWriter, evName string, fields map[string]any) {
				data := map[string]any{"type": evName, "sequence_number": seqNum}
				seqNum++
				for k, v := range fields {
					data[k] = v
				}
				sw.WriteSSEEvent(evName, data)
			}
			ensureRespStartedLocked := func(sw *protocol.StreamWriter) {
				if respStarted {
					return
				}
				respStarted = true
				writeEvLocked(sw, "response.created", map[string]any{"response": buildRespObj("in_progress", []any{}, nil)})
				writeEvLocked(sw, "response.in_progress", map[string]any{"response": buildRespObj("in_progress", []any{}, nil)})
			}
			sendEv := func(evName string, fields map[string]any) {
				stream.DoLocked(func(sw *protocol.StreamWriter) {
					ensureRespStartedLocked(sw)
					writeEvLocked(sw, evName, fields)
				})
			}

			stream.StartHeartbeat(func(sw *protocol.StreamWriter) {
				sw.WriteRaw(": ping\n\n")
			})
			defer stream.StopHeartbeat()

			nextOutIdx := 0
			rsOpen := false
			rsClosed := false
			rsOutIdx := -1
			outRsID := "rs_" + randomHex(12)
			var thoughtChunks []string

			msgOpen := false
			msgClosed := false
			msgOutIdx := -1
			outMsgID := "msg_" + randomHex(12)
			var textChunks []string
			var outItems []any

			closeReasoningIfOpen := func() {
				if !rsOpen || rsClosed {
					return
				}
				rsClosed = true
				fullThk := strings.Join(thoughtChunks, "")
				sumPart := map[string]any{"type": "summary_text", "text": fullThk}
				rsItem := map[string]any{"type": "reasoning", "id": outRsID, "status": "completed", "summary": []any{sumPart}}
				sendEv("response.reasoning_summary_text.done", map[string]any{
					"item_id": outRsID, "output_index": rsOutIdx, "summary_index": 0, "text": fullThk,
				})
				sendEv("response.reasoning_summary_part.done", map[string]any{
					"item_id": outRsID, "output_index": rsOutIdx, "summary_index": 0, "part": sumPart,
				})
				sendEv("response.output_item.done", map[string]any{
					"output_index": rsOutIdx, "item": rsItem,
				})
				outItems = append(outItems, rsItem)
			}

			onDelta := func(c DeltaChunk) {
				if c.Type == "thinking" && c.Thinking != "" {
					if !msgOpen && !rsClosed {
						if !rsOpen {
							rsOpen = true
							rsOutIdx = nextOutIdx
							nextOutIdx++
							sendEv("response.output_item.added", map[string]any{
								"output_index": rsOutIdx,
								"item":         map[string]any{"type": "reasoning", "id": outRsID, "status": "in_progress", "summary": []any{}},
							})
							sendEv("response.reasoning_summary_part.added", map[string]any{
								"item_id": outRsID, "output_index": rsOutIdx, "summary_index": 0,
								"part": map[string]any{"type": "summary_text", "text": ""},
							})
						}
						thoughtChunks = append(thoughtChunks, c.Thinking)
						sendEv("response.reasoning_summary_text.delta", map[string]any{
							"item_id": outRsID, "output_index": rsOutIdx, "summary_index": 0, "delta": c.Thinking,
						})
					}
				} else if c.Type == "text" && c.Text != "" {
					closeReasoningIfOpen()
					if !msgOpen {
						msgOpen = true
						msgOutIdx = nextOutIdx
						nextOutIdx++
						sendEv("response.output_item.added", map[string]any{
							"output_index": msgOutIdx,
							"item":         map[string]any{"type": "message", "id": outMsgID, "status": "in_progress", "role": "assistant", "content": []any{}},
						})
						sendEv("response.content_part.added", map[string]any{
							"item_id": outMsgID, "output_index": msgOutIdx, "content_index": 0,
							"part": map[string]any{"type": "output_text", "text": "", "annotations": []any{}},
						})
					}
					textChunks = append(textChunks, c.Text)
					sendEv("response.output_text.delta", map[string]any{
						"item_id": outMsgID, "output_index": msgOutIdx, "content_index": 0, "delta": c.Text, "logprobs": []any{},
					})
				}
			}

			r = withSessionWarnHook(r, func(warn string) {
				stream.SetPreHeader("X-Agy-Cache-Warning", warn)
			})
			res, err := gs.HandleMessagesRequest(r, body, messages, onDelta)
			stream.StopHeartbeat()
			if err == nil {
				if softErr := DetectAntigravitySoftError(res.Text); softErr != nil {
					err = softErr
				}
			}
			if err != nil {
				statusCode, errType, retryAfter, errMsg := classifyUpstreamError(err)
				raHdr := ""
				if retryAfter {
					raHdr = extractRetryAfterHeaderValue(errMsg)
				}
				recordPrimaryDirectOutcome(r, statusCode, errType, raHdr, errMsg)
				LogEvent("HTTP_ERR", fmt.Sprintf("REQ #%d Failed: %v", reqID, err), nil)
				preCommitted := false
				stream.DoLocked(func(sw *protocol.StreamWriter) {
					if !sw.HeadersCommitted() {
						var extra map[string]string
						if retryAfter {
							extra = map[string]string{"Retry-After": extractRetryAfterHeaderValue(errMsg)}
						}
						sw.CommitErrorStatus(statusCode, extra, "application/json")
						b, _ := json.Marshal(map[string]any{
							"type":  "error",
							"error": map[string]any{"type": errType, "code": errType, "message": errMsg},
						})
						sw.WriteRaw(string(b) + "\n")
						preCommitted = true
					}
				})
				if preCommitted {
					return
				}
				failedObj := buildRespObj("failed", []any{}, nil)
				failedObj["error"] = map[string]any{"code": errType, "message": errMsg}
				sendEv("response.failed", map[string]any{"response": failedObj})
				return
			}
			stream.ApplyPreHeaders(func(h http.Header) {
				setAgyTurnHeadersOn(h, res.Usage)
			})
			if !rsOpen && res.Thinking != "" && !msgOpen {
				rsOpen = true
				rsOutIdx = nextOutIdx
				nextOutIdx++
				thoughtChunks = append(thoughtChunks, res.Thinking)
				sendEv("response.output_item.added", map[string]any{
					"output_index": rsOutIdx,
					"item":         map[string]any{"type": "reasoning", "id": outRsID, "status": "in_progress", "summary": []any{}},
				})
				sendEv("response.reasoning_summary_part.added", map[string]any{
					"item_id": outRsID, "output_index": rsOutIdx, "summary_index": 0,
					"part": map[string]any{"type": "summary_text", "text": ""},
				})
				sendEv("response.reasoning_summary_text.delta", map[string]any{
					"item_id": outRsID, "output_index": rsOutIdx, "summary_index": 0, "delta": res.Thinking,
				})
			}
			closeReasoningIfOpen()
			isTruncated := len(res.ToolCalls) == 0 && res.StopReason == "max_tokens"
			if !msgOpen && (res.Text != "" || len(res.ToolCalls) == 0) {
				msgOpen = true
				msgOutIdx = nextOutIdx
				nextOutIdx++
				if res.Text != "" {
					textChunks = append(textChunks, res.Text)
				}
				sendEv("response.output_item.added", map[string]any{
					"output_index": msgOutIdx,
					"item":         map[string]any{"type": "message", "id": outMsgID, "status": "in_progress", "role": "assistant", "content": []any{}},
				})
				sendEv("response.content_part.added", map[string]any{
					"item_id": outMsgID, "output_index": msgOutIdx, "content_index": 0,
					"part": map[string]any{"type": "output_text", "text": "", "annotations": []any{}},
				})
				if res.Text != "" {
					sendEv("response.output_text.delta", map[string]any{
						"item_id": outMsgID, "output_index": msgOutIdx, "content_index": 0, "delta": res.Text, "logprobs": []any{},
					})
				}
			}
			if msgOpen && !msgClosed {
				msgClosed = true
				fullTxt := strings.Join(textChunks, "")
				msgItemStatus := "completed"
				if isTruncated {
					msgItemStatus = "incomplete"
				}
				part := map[string]any{"type": "output_text", "text": fullTxt, "annotations": []any{}}
				item := map[string]any{"type": "message", "id": outMsgID, "status": msgItemStatus, "role": "assistant", "content": []any{part}}
				sendEv("response.output_text.done", map[string]any{"item_id": outMsgID, "output_index": msgOutIdx, "content_index": 0, "text": fullTxt, "logprobs": []any{}})
				sendEv("response.content_part.done", map[string]any{"item_id": outMsgID, "output_index": msgOutIdx, "content_index": 0, "part": part})
				sendEv("response.output_item.done", map[string]any{"output_index": msgOutIdx, "item": item})
				outItems = append(outItems, item)
			}
			for _, tc := range res.ToolCalls {
				idx := nextOutIdx
				nextOutIdx++
				if customToolNames[tc.Name] {
					rawIn, _ := tc.Input["input"].(string)
					item := map[string]any{"type": "custom_tool_call", "id": shortItemID("ctc_", tc.ID), "call_id": tc.ID, "name": tc.Name, "input": rawIn, "status": "completed"}
					sendEv("response.output_item.added", map[string]any{"output_index": idx, "item": map[string]any{"type": "custom_tool_call", "id": item["id"], "call_id": tc.ID, "name": tc.Name, "input": "", "status": "in_progress"}})
					sendEv("response.custom_tool_call_input.delta", map[string]any{"item_id": item["id"], "output_index": idx, "delta": rawIn})
					sendEv("response.custom_tool_call_input.done", map[string]any{"item_id": item["id"], "output_index": idx, "input": rawIn})
					sendEv("response.output_item.done", map[string]any{"output_index": idx, "item": item})
					outItems = append(outItems, item)
				} else {
					inJson, _ := json.Marshal(tc.Input)
					argsStr := string(inJson)
					item := map[string]any{"type": "function_call", "id": shortItemID("fc_", tc.ID), "call_id": tc.ID, "name": tc.Name, "arguments": argsStr, "status": "completed"}
					sendEv("response.output_item.added", map[string]any{"output_index": idx, "item": map[string]any{"type": "function_call", "id": item["id"], "call_id": tc.ID, "name": tc.Name, "arguments": "", "status": "in_progress"}})
					sendEv("response.function_call_arguments.delta", map[string]any{"item_id": item["id"], "output_index": idx, "delta": argsStr})
					sendEv("response.function_call_arguments.done", map[string]any{"item_id": item["id"], "output_index": idx, "arguments": argsStr})
					sendEv("response.output_item.done", map[string]any{"output_index": idx, "item": item})
					outItems = append(outItems, item)
				}
			}
			inTok, crTok, outTok := res.Usage.ClientTokens()
			thTok := effectiveReasoningTokens(res, outTok)
			usageObj := map[string]any{
				"input_tokens":          inTok + crTok,
				"input_tokens_details":  map[string]any{"cached_tokens": crTok},
				"output_tokens":         outTok,
				"output_tokens_details": map[string]any{"reasoning_tokens": thTok},
				"total_tokens":          inTok + crTok + outTok,
			}
			if isTruncated {
				incObj := buildRespObj("incomplete", outItems, usageObj)
				incObj["incomplete_details"] = map[string]any{"reason": "max_output_tokens"}
				sendEv("response.incomplete", map[string]any{"response": incObj})
			} else {
				sendEv("response.completed", map[string]any{"response": buildRespObj("completed", outItems, usageObj)})
			}
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
			raHdr := ""
			if retryAfter {
				raHdr = extractRetryAfterHeaderValue(errMsg)
			}
			recordPrimaryDirectOutcome(r, statusCode, errType, raHdr, errMsg)
			LogEvent("HTTP_ERR", fmt.Sprintf("REQ #%d Failed: %v", reqID, err), nil)
			w.Header().Set("Content-Type", "application/json")
			if retryAfter {
				w.Header().Set("Retry-After", extractRetryAfterHeaderValue(errMsg))
			}
			w.WriteHeader(statusCode)
			_ = json.NewEncoder(w).Encode(map[string]any{"type": "error", "error": map[string]any{"type": errType, "message": errMsg}})
			return
		}

		inTok, crTok, outTok := res.Usage.ClientTokens()
		thTok := effectiveReasoningTokens(res, outTok)
		// body "model" keeps the requested name for client compatibility
		setAgyTurnHeaders(w, res.Usage)

		if path == "/v1/responses" {
			var outItems []any
			isTruncated := len(res.ToolCalls) == 0 && res.StopReason == "max_tokens"
			if res.Thinking != "" {
				outItems = append(outItems, map[string]any{
					"type": "reasoning", "id": "rs_" + randomHex(12), "status": "completed",
					"summary": []map[string]any{{"type": "summary_text", "text": res.Thinking}},
				})
			}
			if res.Text != "" || len(res.ToolCalls) == 0 {
				msgItemStatus := "completed"
				if isTruncated {
					msgItemStatus = "incomplete"
				}
				outItems = append(outItems, map[string]any{
					"type": "message", "id": msgID, "status": msgItemStatus, "role": "assistant",
					"content": []map[string]any{{"type": "output_text", "text": res.Text, "annotations": []any{}}},
				})
			}
			for _, tc := range res.ToolCalls {
				if customToolNames[tc.Name] {
					rawIn, _ := tc.Input["input"].(string)
					outItems = append(outItems, map[string]any{"type": "custom_tool_call", "id": shortItemID("ctc_", tc.ID), "call_id": tc.ID, "name": tc.Name, "input": rawIn, "status": "completed"})
				} else {
					inJson, _ := json.Marshal(tc.Input)
					outItems = append(outItems, map[string]any{"type": "function_call", "id": shortItemID("fc_", tc.ID), "call_id": tc.ID, "name": tc.Name, "arguments": string(inJson), "status": "completed"})
				}
			}
			respStatus := "completed"
			var incDetails any
			if isTruncated {
				respStatus = "incomplete"
				incDetails = map[string]any{"reason": "max_output_tokens"}
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id": "resp_" + msgID[4:], "object": "response", "created_at": time.Now().Unix(),
				"status": respStatus, "error": nil, "incomplete_details": incDetails,
				"model": displayModel, "output": outItems, "output_text": res.Text,
				"parallel_tool_calls": respParallelToolCalls(body),
				"tool_choice":         respToolChoiceEcho(body),
				"store":               false,
				"usage": map[string]any{
					"input_tokens":          inTok + crTok,
					"input_tokens_details":  map[string]any{"cached_tokens": crTok},
					"output_tokens":         outTok,
					"output_tokens_details": map[string]any{"reasoning_tokens": thTok},
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
				if res.StopReason == "max_tokens" {
					fin = "length"
				}
				msgObj = map[string]any{"role": "assistant", "content": res.Text}
			}
			if res.Thinking != "" {
				msgObj["reasoning_content"] = res.Thinking
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id": "chatcmpl-" + msgID, "object": "chat.completion", "created": time.Now().Unix(),
				"model":   displayModel,
				"choices": []map[string]any{{"index": 0, "message": msgObj, "finish_reason": fin}},
				"usage": map[string]any{
					"prompt_tokens":             inTok + crTok,
					"completion_tokens":         outTok,
					"total_tokens":              inTok + crTok + outTok,
					"prompt_tokens_details":     map[string]any{"cached_tokens": crTok},
					"completion_tokens_details": map[string]any{"reasoning_tokens": thTok},
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
		} else if res.StopReason == "max_tokens" {
			stopReason = "max_tokens"
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
