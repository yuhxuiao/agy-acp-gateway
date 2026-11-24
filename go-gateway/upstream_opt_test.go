package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPureApiHomeSeedingAndClientIdentity(t *testing.T) {
	origHome := PureApiHome
	tmpHome := t.TempDir()
	PureApiHome = tmpHome
	defer func() { PureApiHome = origHome }()

	EnsurePureApiHome()

	projBytes, err := os.ReadFile(filepath.Join(tmpHome, "config", "projects", "default-cli-project.json"))
	if err != nil {
		t.Fatalf("default-cli-project.json missing: %v", err)
	}
	var proj map[string]any
	if err := json.Unmarshal(projBytes, &proj); err != nil || proj["id"] != "default-cli-project" {
		t.Fatalf("unexpected default-cli-project.json: %s", string(projBytes))
	}

	onbPath := filepath.Join(tmpHome, "antigravity-cli", "cache", "onboarding.json")
	onbBytes, err := os.ReadFile(onbPath)
	if err != nil {
		t.Fatalf("onboarding.json missing: %v", err)
	}
	var onb map[string]any
	if err := json.Unmarshal(onbBytes, &onb); err != nil || onb["onboardingComplete"] != true || onb["consumerOnboardingComplete"] != true {
		t.Fatalf("unexpected onboarding.json: %s", string(onbBytes))
	}

	// Regression: literal JSON null in onboarding.json must not nil-map panic on re-seed
	if err := os.WriteFile(onbPath, []byte("null\n"), 0600); err != nil {
		t.Fatal(err)
	}
	seedOnboardingFile(onbPath)
	onbBytes2, err := os.ReadFile(onbPath)
	if err != nil || !strings.Contains(string(onbBytes2), `"onboardingComplete":true`) {
		t.Fatalf("seedOnboardingFile failed after literal null: %v %s", err, string(onbBytes2))
	}

	// Regression: C-drive guard catches non-existent nested targets and Windows drive letters
	for _, cPath := range []string{
		"/mnt/c",
		"/mnt/c/Users/nonexistent-dir/cache/onboarding.json",
		`C:\Users\example\onboarding.json`,
	} {
		if !isWindowsCDrivePath(cPath) {
			t.Fatalf("expected isWindowsCDrivePath(%q)=true", cPath)
		}
	}

	for _, pluginName := range []string{"ws-bridge", "dsh-bridge"} {
		mdBytes, err := os.ReadFile(filepath.Join(tmpHome, "config", "plugins", pluginName, "agents", pluginName+".md"))
		if err != nil {
			t.Fatalf("%s.md missing: %v", pluginName, err)
		}
		md := string(mdBytes)
		for _, want := range []string{"<identity>", "search_web", "read_url_content", "Google Deepmind team"} {
			if !strings.Contains(md, want) {
				t.Fatalf("%s.md missing %q:\n%s", pluginName, want, md)
			}
		}
	}

	baseURL, cleanup := startTestGateway(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]any{
		"model":      "gemini-3.8-flash",
		"max_tokens": 64,
		"messages":   []map[string]any{{"role": "user", "content": "[ECHO_CLIENT_INFO] hi"}},
	})
	resp, err := http.Post(baseURL+"/v1/messages", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	out := string(raw)
	if !strings.Contains(out, "client=antigravity/1.5.10") && !strings.Contains(out, "client=antigravity/1.5.9") {
		t.Fatalf("expected clientInfo antigravity/1.5.10 (or 1.5.9), got: %s", out)
	}
	if !strings.Contains(out, "--override_user_agent_name|antigravity") {
		t.Fatalf("expected --override_user_agent_name antigravity spawn arg, got: %s", out)
	}
}

func TestStructuredToolPermissionGate(t *testing.T) {
	sess := NewAcpProjectSession("perm-unit", "/tmp", "")
	bashMcp := mcpToolName("Bash")
	sess.McpToHostMap = map[string]string{
		bashMcp:                    "Bash",
		mcpToolName(CallToolName):  CallToolName,
		mcpToolName(AwaitToolName): AwaitToolName,
	}

	// 1. Reject when no prompt is in flight or when turn was cancelled (activeTurn == nil && no parked calls)
	if sess.ownsToolCall(map[string]any{"name": "search_web"}) {
		t.Fatal("expected rejection when PromptInFlight is false")
	}
	sess.PromptInFlight = true
	if sess.ownsToolCall(map[string]any{"name": "search_web"}) {
		t.Fatal("expected rejection when PromptInFlight is true but activeTurn and parked calls are empty")
	}
	sess.activeTurn = &activeTurnState{}

	// 1b. Reject identity-less kind="other" calls even with empty rawInput (rawInput is model-controlled)
	if sess.ownsToolCall(map[string]any{"toolCallId": "empty-other", "kind": "other", "rawInput": map[string]any{}}) {
		t.Fatal("identity-less kind=other tool call must be rejected")
	}

	// 2. Allow built-in web tools
	for _, webTool := range []string{"search_web", "read_url_content"} {
		if !sess.ownsToolCall(map[string]any{"toolCallId": "web-" + webTool, "name": webTool, "kind": "search"}) {
			t.Fatalf("expected %s to be allowed", webTool)
		}
	}

	// 3. Allow agy 1.5.8+ name-only MCP format on BOTH ws-bridge and legacy dsh-bridge
	for _, bridgeName := range []string{"ws-bridge", "dsh-bridge"} {
		prefix := "mcp_" + bridgeName + "_"
		if !sess.ownsToolCall(map[string]any{"toolCallId": "mcp-direct-" + bridgeName, "name": prefix + bashMcp}) {
			t.Fatalf("expected direct %s<tool> to be allowed", prefix)
		}
		sess.handleSessionUpdate(map[string]any{
			"sessionUpdate": "tool_call",
			"toolCallId":    "mcp-prereported-" + bridgeName,
			"name":          prefix + bashMcp,
			"kind":          "other",
		})
		if !sess.ownsToolCall(map[string]any{
			"toolCallId": "mcp-prereported-" + bridgeName,
			"title":      "Run Bash",
			"kind":       "other",
			"rawInput":   map[string]any{"command": "pwd"},
		}) {
			t.Fatalf("expected pre-reported %s<tool> to be allowed when request_permission omits name", prefix)
		}
	}

	// 4. Reject spoofed native tool calls carrying ws-bridge / dsh-bridge / search_web / mcpName in rawInput or title
	if sess.ownsToolCall(map[string]any{
		"toolCallId": "spoof-1",
		"name":       "run_command",
		"kind":       "execute",
		"title":      "ws-bridge dsh-bridge search_web " + bashMcp,
		"rawInput":   map[string]any{"CommandLine": "echo ws-bridge dsh-bridge search_web read_url_content " + bashMcp},
	}) {
		t.Fatal("spoofed run_command must be rejected")
	}

	// 5. Reject unoffered MCP tools on ws-bridge and dsh-bridge
	for _, unoffered := range []string{
		"mcp_ws-bridge_ws_NotInCatalog_123456789abc",
		"mcp_dsh-bridge_dsh_NotInCatalog_123456789abc",
	} {
		if sess.ownsToolCall(map[string]any{
			"toolCallId": "unoffered-" + unoffered,
			"name":       unoffered,
		}) {
			t.Fatalf("unoffered bridge MCP tool %q must be rejected", unoffered)
		}
	}

	// 6. Reject conflicting session/update vs request_permission metadata (including cross-bridge mismatch)
	sess.handleSessionUpdate(map[string]any{
		"sessionUpdate": "tool_call",
		"toolCallId":    "conflict-1",
		"name":          "run_command",
		"kind":          "execute",
	})
	if sess.ownsToolCall(map[string]any{
		"toolCallId":    "conflict-1",
		"mcpServerName": "ws-bridge",
		"mcpToolName":   bashMcp,
	}) {
		t.Fatal("conflicting tool identity across update and permission request must be rejected")
	}
	if sess.ownsToolCall(map[string]any{
		"toolCallId":    "conflict-cross-bridge",
		"name":          "mcp_ws-bridge_" + bashMcp,
		"mcpServerName": "dsh-bridge",
		"mcpToolName":   bashMcp,
	}) {
		t.Fatal("cross-bridge mismatch between name prefix and mcpServerName must be rejected")
	}

	// 7. End-to-end permission handling via fakeacp (allow_once preference + spoof rejection)
	baseURL, cleanup := startTestGateway(t)
	defer cleanup()
	tools := []map[string]any{{"name": "Bash", "description": "Run bash", "input_schema": map[string]any{"type": "object"}}}

	for _, tc := range []struct {
		marker string
		sid    string
		want   string
	}{
		{"[FAKE_PERM_NAMEONLY]", "perm-e2e-ok", "PERM_NAMEONLY=selected:opt_allow_once"},
		{"[FAKE_PERM_SPOOF]", "perm-e2e-spoof", "PERM_SPOOF=selected:opt_reject_once"},
	} {
		b, _ := json.Marshal(map[string]any{
			"model": "gemini-3.8-flash", "max_tokens": 64, "tools": tools,
			"messages": []map[string]any{{"role": "user", "content": tc.marker}},
		})
		req, _ := http.NewRequest("POST", baseURL+"/v1/messages", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("x-claude-code-session-id", tc.sid)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if !strings.Contains(string(raw), tc.want) {
			t.Fatalf("%s: want %q in %s", tc.marker, tc.want, string(raw))
		}
	}
}

func TestMaxTokensExhaustionAcrossFourProtocols(t *testing.T) {
	baseURL, cleanup := startTestGateway(t)
	defer cleanup()

	// 1. Anthropic /v1/messages (non-stream & stream)
	for i, stream := range []bool{false, true} {
		b, _ := json.Marshal(map[string]any{
			"model": "gemini-3.8-flash", "max_tokens": 64, "stream": stream,
			"messages": []map[string]any{{"role": "user", "content": "[FAKE_MAX_TOKENS_ALWAYS] anthropic-" + string(rune('0'+i))}},
		})
		resp, err := http.Post(baseURL+"/v1/messages", "application/json", bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		s := string(raw)
		if !strings.Contains(s, "SEG1_SEG2_SEG3_SEG4_") && !strings.Contains(s, "SEG4_") {
			t.Fatalf("anthropic stream=%v missing 4 continuation segments: %s", stream, s)
		}
		if !strings.Contains(s, `"stop_reason":"max_tokens"`) {
			t.Fatalf("anthropic stream=%v expected stop_reason=max_tokens, got: %s", stream, s)
		}
	}

	// 2. OpenAI /v1/chat/completions (non-stream & stream)
	for i, stream := range []bool{false, true} {
		b, _ := json.Marshal(map[string]any{
			"model": "gemini-3.8-flash", "stream": stream,
			"messages": []map[string]any{{"role": "user", "content": "[FAKE_MAX_TOKENS_ALWAYS] chat-" + string(rune('0'+i))}},
		})
		resp, err := http.Post(baseURL+"/v1/chat/completions", "application/json", bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if !strings.Contains(string(raw), `"finish_reason":"length"`) {
			t.Fatalf("chat/completions stream=%v expected finish_reason=length, got: %s", stream, string(raw))
		}
	}

	// 3. OpenAI /v1/responses (non-stream & stream)
	for i, stream := range []bool{false, true} {
		b, _ := json.Marshal(map[string]any{
			"model": "gemini-3.8-flash", "stream": stream, "input": "[FAKE_MAX_TOKENS_ALWAYS] resp-" + string(rune('0'+i)),
		})
		resp, err := http.Post(baseURL+"/v1/responses", "application/json", bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		s := string(raw)
		if !strings.Contains(s, `"status":"incomplete"`) || !strings.Contains(s, `"max_output_tokens"`) {
			t.Fatalf("responses stream=%v expected status=incomplete & max_output_tokens, got: %s", stream, s)
		}
		if stream && !strings.Contains(s, "event: response.incomplete") {
			t.Fatalf("responses stream=true expected event: response.incomplete, got: %s", s)
		}
	}

	// 4. Google Gemini /v1beta (non-stream & stream SSE)
	for i, stream := range []bool{false, true} {
		path := "/v1beta/models/gemini-3.8-flash:generateContent"
		if stream {
			path = "/v1beta/models/gemini-3.8-flash:streamGenerateContent?alt=sse"
		}
		b, _ := json.Marshal(map[string]any{
			"contents": []map[string]any{{"role": "user", "parts": []map[string]any{{"text": "[FAKE_MAX_TOKENS_ALWAYS] gemini-" + string(rune('0'+i))}}}},
		})
		resp, err := http.Post(baseURL+path, "application/json", bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if !strings.Contains(string(raw), `"finishReason":"MAX_TOKENS"`) {
			t.Fatalf("gemini v1beta stream=%v expected finishReason=MAX_TOKENS, got: %s", stream, string(raw))
		}
	}
}

func TestResponsesReasoningItemsAndReasoningTokens(t *testing.T) {
	baseURL, cleanup := startTestGateway(t)
	defer cleanup()

	// 1. Streaming /v1/responses with [FAKE_THINK]
	bStream, _ := json.Marshal(map[string]any{
		"model": "gemini-3.8-flash", "stream": true, "input": "[FAKE_THINK] explain",
	})
	respS, err := http.Post(baseURL+"/v1/responses", "application/json", bytes.NewReader(bStream))
	if err != nil {
		t.Fatal(err)
	}
	rawS, _ := io.ReadAll(respS.Body)
	respS.Body.Close()
	s := string(rawS)
	for _, want := range []string{
		"event: response.reasoning_summary_part.added",
		"event: response.reasoning_summary_text.delta",
		"event: response.reasoning_summary_text.done",
		`"type":"reasoning"`,
		`"reasoning_tokens":7`,
		"event: response.completed",
	} {
		if !strings.Contains(s, want) {
			t.Fatalf("streaming /v1/responses missing %q:\n%s", want, s)
		}
	}

	// 2. Non-streaming /v1/responses with [FAKE_THINK]
	bJSON, _ := json.Marshal(map[string]any{
		"model": "gemini-3.8-flash", "stream": false, "input": "[FAKE_THINK] explain",
	})
	respJ, err := http.Post(baseURL+"/v1/responses", "application/json", bytes.NewReader(bJSON))
	if err != nil {
		t.Fatal(err)
	}
	var parsed map[string]any
	_ = json.NewDecoder(respJ.Body).Decode(&parsed)
	respJ.Body.Close()
	outputs, _ := parsed["output"].([]any)
	if len(outputs) < 2 {
		t.Fatalf("expected reasoning + message items in output, got: %+v", parsed["output"])
	}
	firstItem, _ := outputs[0].(map[string]any)
	secondItem, _ := outputs[1].(map[string]any)
	if firstItem["type"] != "reasoning" || secondItem["type"] != "message" {
		t.Fatalf("expected output[0]=reasoning and output[1]=message, got %+v", outputs)
	}
	usage, _ := parsed["usage"].(map[string]any)
	outDetails, _ := usage["output_tokens_details"].(map[string]any)
	if rt, _ := toInt(outDetails["reasoning_tokens"]); rt != 7 {
		t.Fatalf("expected reasoning_tokens=7, got %+v", usage)
	}
}

func TestClassifyUpstreamErrorDirectRpcAndRedaction(t *testing.T) {
	code, typ, retry, msg := classifyUpstreamError(errors.New(`{"code":-32603,"message":"400 INVALID_ARGUMENT: thinking budget is not supported; key=Bearer ya29.secret_token_value"}`))
	if code != 400 || typ != "invalid_request_error" || retry {
		t.Fatalf("expected 400 invalid_request_error retry=false, got %d %s %v", code, typ, retry)
	}
	if strings.Contains(msg, "secret_token_value") {
		t.Fatalf("expected Bearer token redaction in error message, got %q", msg)
	}

	code, typ, retry, _ = classifyUpstreamError(errors.New(`{"code":-32603,"message":"429 RESOURCE_EXHAUSTED: quota exceeded"}`))
	if code != 429 || typ != "rate_limit_error" || !retry {
		t.Fatalf("expected 429 rate_limit_error retry=true, got %d %s %v", code, typ, retry)
	}

	code, typ, retry, _ = classifyUpstreamError(errors.New(`{"code":-32603,"message":"429 RESOURCE_EXHAUSTED: request exceeds the maximum number of tokens per minute"}`))
	if code != 429 || typ != "rate_limit_error" || !retry {
		t.Fatalf("expected TPM 429 to remain 429 rate_limit_error retry=true, got %d %s %v", code, typ, retry)
	}

	code, typ, retry, _ = classifyUpstreamError(errors.New(`{"code":-32603,"message":"503 MODEL_CAPACITY_EXHAUSTED: server overloaded"}`))
	if code != 529 || typ != "overloaded_error" || !retry {
		t.Fatalf("expected 529 overloaded_error retry=true, got %d %s %v", code, typ, retry)
	}

	baseURL, cleanup := startTestGateway(t)
	defer cleanup()
	for _, tc := range []struct {
		marker string
		want   int
	}{
		{"[FAKE_RPC_400]", 400},
		{"[FAKE_RPC_429]", 429},
		{"[FAKE_RPC_529]", 529},
	} {
		b, _ := json.Marshal(map[string]any{
			"model": "gemini-3.8-flash", "max_tokens": 32,
			"messages": []map[string]any{{"role": "user", "content": tc.marker}},
		})
		resp, err := http.Post(baseURL+"/v1/messages", "application/json", bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != tc.want {
			t.Fatalf("%s: status=%d, want %d", tc.marker, resp.StatusCode, tc.want)
		}
	}
}
