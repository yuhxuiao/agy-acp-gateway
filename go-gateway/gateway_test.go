package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

var fakeAcpBin string

func TestMain(m *testing.M) {
	tmpDir, err := os.MkdirTemp("", "agw-test-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	fakeAcpBin = filepath.Join(tmpDir, "fakeacp")
	goBin := "go"
	if _, err := exec.LookPath(goBin); err != nil {
		if home, err2 := os.UserHomeDir(); err2 == nil {
			candidate := filepath.Join(home, ".local", "go", "bin", "go")
			if _, err3 := os.Stat(candidate); err3 == nil {
				goBin = candidate
			}
		}
	}
	cmd := exec.Command(goBin, "build", "-o", fakeAcpBin, "./testdata/fakeacp")
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := cmd.CombinedOutput(); err != nil {
		panic(fmt.Sprintf("failed to build fakeacp: %v: %s", err, string(out)))
	}

	os.Setenv("AGY_ACP_BIN", fakeAcpBin)
	os.Setenv("AGY_PURE_API_HOME", filepath.Join(tmpDir, "pure-api-home"))
	os.Setenv("AGY_GW_LOG", filepath.Join(tmpDir, "gateway.log"))
	os.Setenv("AGY_STATE_DB", filepath.Join(tmpDir, "gateway.db"))
	os.Setenv("AGY_TOS_TRIPPED_FILE", filepath.Join(tmpDir, "tos-tripped"))
	os.Setenv("AGY_GW_TMPLOG", "")
	os.Setenv("AGY_ACP_RECORD", filepath.Join(tmpDir, "recordings"))
	initConfig()

	code := m.Run()
	GlobalEngine.StopEngine()
	os.Exit(code)
}

func TestTruncateRunesUTF8Safety(t *testing.T) {
	in := "你好世界Hello"
	got := truncateRunes(in, 3)
	if got != "你好世" {
		t.Fatalf("expected 你好世, got %q", got)
	}
	if !utf8.ValidString(got) {
		t.Fatalf("invalid UTF-8 after truncateRunes: %q", got)
	}
}

func TestSanitizeSchemaAndToolsCatalog(t *testing.T) {
	rawTools := []any{
		map[string]any{
			"name":        "Bash",
			"description": "Run bash in Claude Code\nExtra line",
			"input_schema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"command":     map[string]any{"type": "string"},
					"timeout":     map[string]any{"type": "number"},
					"toolSummary": map[string]any{"type": "string"},
				},
				"required": []any{"command", "toolSummary", "nonexistent"},
			},
		},
		map[string]any{
			"name": "WebSearch",
		},
	}
	cat := sanitizeToolsCatalog(rawTools)
	if len(cat) != 3 { // Bash + dsh_await_result + dsh_call_tool (WebSearch filtered)
		t.Fatalf("expected 3 tools in catalog, got %d", len(cat))
	}
	if cat[0].Name != "Bash" {
		t.Fatalf("expected Bash first, got %s", cat[0].Name)
	}
	if strings.Contains(cat[0].Description, "Claude Code") || strings.Contains(cat[0].Description, "\n") {
		t.Fatalf("vendor name or newline not stripped: %q", cat[0].Description)
	}
	props, _ := cat[0].Parameters["properties"].(map[string]any)
	if _, hasSummary := props["toolSummary"]; hasSummary {
		t.Fatalf("toolSummary should be stripped from properties")
	}
}

func TestResolveRequestedModelAndThinkingTier(t *testing.T) {
	cases := []struct {
		model    string
		body     map[string]any
		wantID   string
		wantTier string
	}{
		{"claude-sonnet-4-5", nil, "gemini-4-argon", "medium"},
		{"gemini-4-argon-high", nil, "gemini-4-argon", "high"},
		{"claude-opus-5-5", map[string]any{"thinking": map[string]any{"type": "enabled", "budget_tokens": 32000}}, "claude-opus-5-5", "high"},
		{"claude-sonnet-5-5", map[string]any{"thinking": map[string]any{"type": "enabled", "budget_tokens": 2048}}, "claude-sonnet-5-5", "low"},
		{"gemini-3.8-flash", map[string]any{"reasoning_effort": "xhigh"}, "gemini-3.8-flash", "high"},
		{"gpt-oss-120b", map[string]any{"reasoning_effort": "high"}, "gpt-oss-120b-medium", "medium"},
	}
	for _, tc := range cases {
		sel := ResolveRequestedModel(tc.model, tc.body)
		if sel.Model.ID != tc.wantID || sel.Tier != tc.wantTier {
			t.Errorf("model=%q: got (%s, %s), want (%s, %s)", tc.model, sel.Model.ID, sel.Tier, tc.wantID, tc.wantTier)
		}
	}
}

func TestPriceUsageTiers(t *testing.T) {
	argon := GetModel("gemini-4-argon")
	// 1.0x tier (<= 256K)
	p1 := PriceUsage(argon, 100000, 100000, 1000, 0, 0)
	if p1.TierMultiplier != 1.0 {
		t.Fatalf("expected 1.0x at 200K for Argon, got %v", p1.TierMultiplier)
	}
	// 1.3x tier (256K ~ 512K)
	p2 := PriceUsage(argon, 100000, 200000, 1000, 0, 0)
	if p2.TierMultiplier != 1.3 {
		t.Fatalf("expected 1.3x at 300K for Argon, got %v", p2.TierMultiplier)
	}
	// 1.8x tier (> 512K)
	p3 := PriceUsage(argon, 100000, 500000, 1000, 0, 0)
	if p3.TierMultiplier != 1.8 {
		t.Fatalf("expected 1.8x at 600K for Argon, got %v", p3.TierMultiplier)
	}

	opus := GetModel("claude-opus-5-5")
	// <= 200K: uncached write $6.25/M, cached read $0.50/M, output $25/M
	c1 := PriceUsage(opus, 100000, 50000, 1000, 0, 0)
	wantC1 := (100000*6.25 + 50000*0.50 + 1000*25.0) / 1e6
	if math.Abs(c1.CostUsd-wantC1) > 1e-6 {
		t.Fatalf("opus <=200K cost mismatch: got %v, want %v", c1.CostUsd, wantC1)
	}
	// > 200K: uncached write 2x ($12.50/M), cached read 1x ($0.50/M), output 1.5x ($37.50/M)
	c2 := PriceUsage(opus, 100000, 200000, 1000, 0, 0)
	wantC2 := (100000*12.50 + 200000*0.50 + 1000*37.50) / 1e6
	if math.Abs(c2.CostUsd-wantC2) > 1e-6 {
		t.Fatalf("opus >200K cost mismatch: got %v, want %v", c2.CostUsd, wantC2)
	}
}

func TestBuildPromptBlocksAndTruncation(t *testing.T) {
	msgs := []map[string]any{
		{"role": "user", "content": strings.Repeat("A", 5000)},
		{"role": "assistant", "content": "⚠ Agent execution terminated due to error."},
		{"role": "user", "content": strings.Repeat("B", 5000)},
		{"role": "assistant", "content": "OK"},
		{"role": "user", "content": "Final question"},
	}
	blocks := buildPromptBlocks(msgs[4:], true, msgs, 3000)
	if len(blocks) != 2 {
		t.Fatalf("expected 2 blocks (resource + text), got %d", len(blocks))
	}
	res, _ := blocks[0]["resource"].(map[string]any)
	txt, _ := res["text"].(string)
	if !strings.Contains(txt, "trimmed to stay within the model context window") {
		t.Fatalf("expected truncation marker in transcript")
	}
	if strings.Contains(txt, "Agent execution terminated") {
		t.Fatalf("failed error turn should have been filtered from rollover transcript")
	}
}

func TestExtractToolResultsAndCompactionDetection(t *testing.T) {
	msgs := []map[string]any{
		{"role": "user", "content": "First user message"},
		{"role": "assistant", "content": []any{map[string]any{"type": "tool_use", "id": "call_123", "name": "Bash"}}},
		{"role": "user", "content": []any{map[string]any{"type": "tool_result", "tool_use_id": "call_123", "content": "OUT_123"}}},
	}
	trs := extractToolResultsFromMessages(msgs)
	if len(trs) != 1 || trs[0].ToolCallID != "call_123" || trs[0].Content != "OUT_123" {
		t.Fatalf("unexpected tool results: %+v", trs)
	}
	fp := computeFirstUserFingerprint(msgs)
	if fp == "" {
		t.Fatalf("expected non-empty first user fingerprint")
	}

	compMsgs := append(msgs, map[string]any{
		"role":    "user",
		"content": "Your task is to create a detailed summary of the conversation so far.",
	})
	if !isCompactionSummaryRequest(compMsgs) {
		t.Fatalf("expected isCompactionSummaryRequest=true")
	}
	if se := DetectAntigravitySoftError("⚠ Agent execution terminated due to error."); se == nil || se.StatusCode != 429 {
		t.Fatalf("expected soft error 429, got %+v", se)
	}
}

func TestBuildAppendSystemPromptDeterminism(t *testing.T) {
	sess := NewAcpProjectSession("test", "/tmp", "You are Claude Code, Anthropic's official CLI.")
	tools := sanitizeToolsCatalog([]any{
		map[string]any{
			"name": "Edit",
			"input_schema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"z_last":  map[string]any{"type": "string"},
					"a_first": map[string]any{"type": "string"},
					"m_mid":   map[string]any{"type": "string"},
				},
			},
		},
	})
	p1 := sess.buildAppendSystemPrompt(tools)
	for i := 0; i < 20; i++ {
		if p2 := sess.buildAppendSystemPrompt(tools); p1 != p2 {
			t.Fatalf("buildAppendSystemPrompt is non-deterministic:\n%s\nvs\n%s", p1, p2)
		}
	}
	if !strings.Contains(p1, "keys: a_first, m_mid, z_last") {
		t.Fatalf("expected sorted tool keys in append prompt, got: %s", p1)
	}
}

func startTestGateway(t *testing.T) (string, func()) {
	t.Helper()
	gs := NewGatewayServer()
	t.Cleanup(gs.Close)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	GatewayPort = ln.Addr().(*net.TCPAddr).Port
	srv := &http.Server{Handler: gs}
	go srv.Serve(ln)
	baseURL := fmt.Sprintf("http://127.0.0.1:%d", GatewayPort)
	return baseURL, func() {
		_ = srv.Close()
		GlobalEngine.StopEngine()
	}
}

func TestIntegrationThreeProtocolsAndToolLoop(t *testing.T) {
	baseURL, cleanup := startTestGateway(t)
	defer cleanup()

	// 1. Anthropic /v1/messages non-stream + tool_use park & resume
	msgs := []map[string]any{
		{"role": "user", "content": "[FAKE_TOOL_CALL] [ECHO_SYS_APPEND] Run bash"},
	}
	tools := []map[string]any{
		{
			"name":        "Bash",
			"description": "Execute bash",
			"input_schema": map[string]any{
				"type":       "object",
				"properties": map[string]any{"command": map[string]any{"type": "string"}},
			},
		},
	}
	body1, _ := json.Marshal(map[string]any{
		"model":      "claude-sonnet-5-5",
		"max_tokens": 1024,
		"system":     "Primary working directory: /tmp",
		"tools":      tools,
		"messages":   msgs,
	})
	req1, _ := http.NewRequest("POST", baseURL+"/v1/messages", bytes.NewReader(body1))
	req1.Header.Set("Content-Type", "application/json")
	req1.Header.Set("x-claude-code-session-id", "golden-anth-1")
	resp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatal(err)
	}
	var d1 map[string]any
	_ = json.NewDecoder(resp1.Body).Decode(&d1)
	resp1.Body.Close()
	if d1["stop_reason"] != "tool_use" {
		t.Fatalf("expected stop_reason=tool_use, got %v (%+v)", d1["stop_reason"], d1)
	}
	content1, _ := d1["content"].([]any)
	var toolUseID string
	for _, c := range content1 {
		if cm, ok := c.(map[string]any); ok && cm["type"] == "tool_use" {
			toolUseID, _ = cm["id"].(string)
		}
	}
	if toolUseID == "" {
		t.Fatalf("missing tool_use id in %+v", d1)
	}

	// Resume parked tool call
	msgs = append(msgs,
		map[string]any{"role": "assistant", "content": content1},
		map[string]any{"role": "user", "content": []map[string]any{{"type": "tool_result", "tool_use_id": toolUseID, "content": "HOST_EXEC_RESULT_42"}}},
	)
	body2, _ := json.Marshal(map[string]any{
		"model":      "claude-sonnet-5-5",
		"max_tokens": 1024,
		"system":     "Primary working directory: /tmp",
		"tools":      tools,
		"messages":   msgs,
	})
	req2, _ := http.NewRequest("POST", baseURL+"/v1/messages", bytes.NewReader(body2))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("x-claude-code-session-id", "golden-anth-1")
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	var d2 map[string]any
	_ = json.NewDecoder(resp2.Body).Decode(&d2)
	resp2.Body.Close()
	if d2["stop_reason"] != "end_turn" {
		t.Fatalf("expected stop_reason=end_turn on resume, got %v", d2["stop_reason"])
	}
	rawD2, _ := json.Marshal(d2["content"])
	if !strings.Contains(string(rawD2), "TOOL_DONE:HOST_EXEC_RESULT_42") {
		t.Fatalf("expected tool result echoed in final reply, got %s", string(rawD2))
	}

	// 2. Anthropic /v1/messages streaming SSE with thinking
	bodyStream, _ := json.Marshal(map[string]any{
		"model":      "gemini-4-argon-high",
		"max_tokens": 512,
		"stream":     true,
		"messages":   []map[string]any{{"role": "user", "content": "[FAKE_THINK] Hello stream"}},
	})
	reqS, _ := http.NewRequest("POST", baseURL+"/v1/messages", bytes.NewReader(bodyStream))
	reqS.Header.Set("Content-Type", "application/json")
	respS, err := http.DefaultClient.Do(reqS)
	if err != nil {
		t.Fatal(err)
	}
	sseBytes, _ := io.ReadAll(respS.Body)
	respS.Body.Close()
	sseStr := string(sseBytes)
	if !strings.Contains(sseStr, `"thinking_delta"`) || !strings.Contains(sseStr, `"text_delta"`) || !strings.Contains(sseStr, `event: message_stop`) {
		t.Fatalf("unexpected Anthropic SSE stream:\n%s", sseStr)
	}

	// 3. OpenAI /v1/chat/completions (non-stream & stream)
	oaBody, _ := json.Marshal(map[string]any{
		"model":    "gpt-oss-120b",
		"messages": []map[string]any{{"role": "user", "content": "Hello OpenAI"}},
	})
	respOA, err := http.Post(baseURL+"/v1/chat/completions", "application/json", bytes.NewReader(oaBody))
	if err != nil {
		t.Fatal(err)
	}
	var dOA map[string]any
	_ = json.NewDecoder(respOA.Body).Decode(&dOA)
	respOA.Body.Close()
	if dOA["object"] != "chat.completion" {
		t.Fatalf("expected chat.completion, got %+v", dOA)
	}

	// 4. OpenAI /v1/responses (non-stream & stream)
	respBody, _ := json.Marshal(map[string]any{
		"model":  "gemini-3.8-flash",
		"stream": true,
		"input":  "Hello Responses",
	})
	respR, err := http.Post(baseURL+"/v1/responses", "application/json", bytes.NewReader(respBody))
	if err != nil {
		t.Fatal(err)
	}
	rBytes, _ := io.ReadAll(respR.Body)
	respR.Body.Close()
	if !strings.Contains(string(rBytes), "response.completed") {
		t.Fatalf("expected response.completed in /v1/responses stream, got:\n%s", string(rBytes))
	}

	// 5. Auto-continue on max_tokens
	acBody, _ := json.Marshal(map[string]any{
		"model":      "gemini-3.8-flash",
		"max_tokens": 512,
		"messages":   []map[string]any{{"role": "user", "content": "[FAKE_MAX_TOKENS] Long output"}},
	})
	respAC, err := http.Post(baseURL+"/v1/messages", "application/json", bytes.NewReader(acBody))
	if err != nil {
		t.Fatal(err)
	}
	var dAC map[string]any
	_ = json.NewDecoder(respAC.Body).Decode(&dAC)
	respAC.Body.Close()
	rawAC, _ := json.Marshal(dAC["content"])
	if !strings.Contains(string(rawAC), "PART1_FAKE_OK") {
		t.Fatalf("expected auto-continued text PART1_FAKE_OK, got %s", string(rawAC))
	}

	// Verify ACP NDJSON recording was written
	if AcpRecordDir != "" {
		if st, err := os.Stat(filepath.Join(AcpRecordDir, "acp-traffic.ndjson")); err != nil || st.Size() == 0 {
			t.Fatalf("expected non-empty acp-traffic.ndjson recording, err=%v", err)
		}
	}
}

func TestLatestStepFastPathMatchesFull(t *testing.T) {
	convDir := os.Getenv("AGY_TEST_CONV_DIR")
	if convDir == "" {
		t.Skip("AGY_TEST_CONV_DIR not set")
	}
	entries, err := os.ReadDir(convDir)
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".db") {
			continue
		}
		dbPath := filepath.Join(convDir, e.Name())
		fast := ReadLatestStepFromSQLiteFast(dbPath)
		full := ReadModelStepsFromSQLite(dbPath, "WSL", nil)
		if len(full) == 0 {
			continue
		}
		lastFull := full[len(full)-1]
		if fast == nil {
			t.Fatalf("%s: fast returned nil, full had %d steps (last idx=%d)", e.Name(), len(full), lastFull.StepIdx)
		}
		if fast.StepIdx != lastFull.StepIdx || fast.RawIn != lastFull.RawIn || fast.RawCr != lastFull.RawCr || fast.OutTok != lastFull.OutTok || fast.ModelID != lastFull.ModelID {
			t.Fatalf("%s mismatch: fast=%+v vs full=%+v", e.Name(), *fast, lastFull)
		}
		checked++
	}
	t.Logf("Verified %d conversation databases: fast SQL path 100%% matches full parser", checked)
}

func BenchmarkLatestStepLookup(b *testing.B) {
	dbPath := os.Getenv("AGY_BENCH_DB")
	if dbPath == "" {
		b.Skip("AGY_BENCH_DB not set")
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if step := ReadLatestStepFromSQLiteFast(dbPath); step == nil {
			b.Fatal("expected non-nil step")
		}
	}
}

func TestStage1DeriveConversationKeyAndCountTokens(t *testing.T) {
	msgs := []map[string]any{{"role": "user", "content": "Hello project"}}
	tools := []ToolDef{{Name: "Bash", McpName: "dsh_Bash_123"}}

	// 1. metadata.user_id JSON string (survives GPT-Load proxy forwarding)
	r1 := newLoopbackRequest("POST", "/v1/messages", nil)
	body1 := map[string]any{
		"metadata": map[string]any{
			"user_id": `{"device_id":"dev1","session_id":"uuid-from-user-id-1234"}`,
		},
	}
	k1 := deriveConversationKey(r1, body1, "Primary working directory: /tmp", tools, msgs)
	if !strings.HasPrefix(k1, "sid:uuid-from-user-id-1234:u:") {
		t.Fatalf("expected sid from metadata.user_id JSON, got %s", k1)
	}

	// 2. Subagent isolation via x-claude-code-agent-id
	r2 := newLoopbackRequest("POST", "/v1/messages", nil)
	r2.Header.Set("x-claude-code-session-id", "parent-sess-1")
	r2.Header.Set("x-claude-code-agent-id", "subagent-alpha")
	k2 := deriveConversationKey(r2, nil, "Primary working directory: /tmp", tools, msgs)
	if !strings.HasPrefix(k2, "sid:parent-sess-1:ag:subagent-alpha:u:") {
		t.Fatalf("expected agent-id isolated key, got %s", k2)
	}

	// 3. count_tokens excludes raw Base64 image bytes and counts images at 1600 tokens each
	bigBase64 := strings.Repeat("A", 400000) // 400 KB base64 (~100K raw bytes/4 if naive!)
	ctBody, _ := json.Marshal(map[string]any{
		"system": "System instructions",
		"messages": []map[string]any{
			{
				"role": "user",
				"content": []map[string]any{
					{"type": "text", "text": "Describe this screenshot"},
					{
						"type": "image",
						"source": map[string]any{
							"type":       "base64",
							"media_type": "image/png",
							"data":       bigBase64,
						},
					},
				},
			},
		},
	})
	est, err := estimateCountTokens(ctBody)
	if err != nil {
		t.Fatal(err)
	}
	if est < 1600 || est > 2000 {
		t.Fatalf("expected count_tokens around ~1612 (excluding 400KB base64), got %d", est)
	}
}

func TestStage1SecurityPortSeparationAndBodyValidation(t *testing.T) {
	gs := NewGatewayServer()
	t.Cleanup(gs.Close)
	gwHandler := gs.GatewayOnlyHandler()
	dashHandler := gs.DashboardOnlyHandler()

	// 1. Port separation: /dashboard blocked on gateway port, /v1/messages blocked on dashboard port
	rec := httptest.NewRecorder()
	gwHandler.ServeHTTP(rec, newLoopbackRequest("GET", "/dashboard", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for /dashboard on gateway port, got %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	dashHandler.ServeHTTP(rec, newLoopbackRequest("POST", "/v1/messages", strings.NewReader(`{}`)))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for /v1/messages on dashboard port, got %d", rec.Code)
	}

	// 2. Non-loopback Host header rejected when auth disabled
	rec = httptest.NewRecorder()
	reqHost := newLoopbackRequest("GET", "/v1/models", nil)
	reqHost.Host = "evil.attacker.com:18890"
	gwHandler.ServeHTTP(rec, reqHost)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for non-loopback Host header, got %d", rec.Code)
	}

	// 3. Cross-origin Origin header rejected when auth disabled
	rec = httptest.NewRecorder()
	reqOrigin := newLoopbackRequest("POST", "/v1/messages", strings.NewReader(`{}`))
	reqOrigin.Header.Set("Content-Type", "application/json")
	reqOrigin.Header.Set("Origin", "https://evil.example.com")
	gwHandler.ServeHTTP(rec, reqOrigin)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for cross-origin POST, got %d", rec.Code)
	}

	// 4. Missing/wrong Content-Type rejected with 415
	rec = httptest.NewRecorder()
	reqCT := newLoopbackRequest("POST", "/v1/messages", strings.NewReader(`{}`))
	reqCT.Header.Set("Content-Type", "text/plain")
	gwHandler.ServeHTTP(rec, reqCT)
	if rec.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("expected 415 for text/plain POST, got %d", rec.Code)
	}

	// 5. Malformed JSON rejected with 400
	rec = httptest.NewRecorder()
	reqBadJSON := newLoopbackRequest("POST", "/v1/messages", strings.NewReader(`{bad json`))
	reqBadJSON.Header.Set("Content-Type", "application/json")
	gwHandler.ServeHTTP(rec, reqBadJSON)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for malformed JSON, got %d", rec.Code)
	}

	// 6. Oversized body rejected with 413
	origMax := MaxBodyBytes
	MaxBodyBytes = 128
	defer func() { MaxBodyBytes = origMax }()
	rec = httptest.NewRecorder()
	reqBig := newLoopbackRequest("POST", "/v1/messages", strings.NewReader(`{"messages":[{"role":"user","content":"`+strings.Repeat("X", 512)+`"}]}`))
	reqBig.Header.Set("Content-Type", "application/json")
	gwHandler.ServeHTTP(rec, reqBig)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 for oversized body, got %d", rec.Code)
	}

	// 7. /health does not leak bridgeUrl
	rec = httptest.NewRecorder()
	gwHandler.ServeHTTP(rec, newLoopbackRequest("GET", "/health", nil))
	if strings.Contains(rec.Body.String(), "bridgeUrl") {
		t.Fatalf("/health should not expose bridgeUrl: %s", rec.Body.String())
	}
}

func TestStage1ErrorSemanticsCancelResumeAndLateTools(t *testing.T) {
	baseURL, cleanup := startTestGateway(t)
	defer cleanup()

	// 1. max_tokens auto-continue with MaxConcurrentPrompts=1 (no self-deadlock!)
	origSem := promptSem
	promptSem = make(chan struct{}, 1)
	defer func() { promptSem = origSem }()

	acBody, _ := json.Marshal(map[string]any{
		"model":      "gemini-3.8-flash",
		"max_tokens": 512,
		"messages":   []map[string]any{{"role": "user", "content": "[FAKE_MAX_TOKENS] single-slot test"}},
	})
	respAC, err := http.Post(baseURL+"/v1/messages", "application/json", bytes.NewReader(acBody))
	if err != nil {
		t.Fatal(err)
	}
	var dAC map[string]any
	_ = json.NewDecoder(respAC.Body).Decode(&dAC)
	respAC.Body.Close()
	rawAC, _ := json.Marshal(dAC["content"])
	if !strings.Contains(string(rawAC), "PART1_FAKE_OK") {
		t.Fatalf("expected PART1_FAKE_OK with 1 semaphore slot, got %s", string(rawAC))
	}

	// 2. Non-stream soft 429 returns 429 + Retry-After: 5
	errBody, _ := json.Marshal(map[string]any{
		"model":      "gemini-4-argon",
		"max_tokens": 256,
		"messages":   []map[string]any{{"role": "user", "content": "[FAKE_SOFT_429]"}},
	})
	resp429, err := http.Post(baseURL+"/v1/messages", "application/json", bytes.NewReader(errBody))
	if err != nil {
		t.Fatal(err)
	}
	resp429.Body.Close()
	if resp429.StatusCode != 429 || resp429.Header.Get("Retry-After") != "5" {
		t.Fatalf("expected 429 with Retry-After=5, got status=%d retry-after=%q", resp429.StatusCode, resp429.Header.Get("Retry-After"))
	}

	// 3. Streaming RPC error emits event: error on /v1/messages
	streamErrBody, _ := json.Marshal(map[string]any{
		"model":      "gemini-4-argon",
		"max_tokens": 256,
		"stream":     true,
		"messages":   []map[string]any{{"role": "user", "content": "[FAKE_RPC_ERROR]"}},
	})
	respSE, err := http.Post(baseURL+"/v1/messages", "application/json", bytes.NewReader(streamErrBody))
	if err != nil {
		t.Fatal(err)
	}
	seBytes, _ := io.ReadAll(respSE.Body)
	respSE.Body.Close()
	if !strings.Contains(string(seBytes), "event: error") || !strings.Contains(string(seBytes), "simulated upstream RPC failure") {
		t.Fatalf("expected event: error with simulated upstream RPC failure, got:\n%s", string(seBytes))
	}

	// 4. Client disconnect sends session/cancel and preserves session for next turn
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	slowBody, _ := json.Marshal(map[string]any{
		"model":      "gemini-4-argon",
		"max_tokens": 256,
		"system":     "Primary working directory: /tmp",
		"tools": []map[string]any{
			{"name": "Bash", "input_schema": map[string]any{"type": "object"}},
		},
		"messages": []map[string]any{{"role": "user", "content": "[FAKE_SLOW] cancel me"}},
	})
	reqSlow, _ := http.NewRequestWithContext(ctx, "POST", baseURL+"/v1/messages", bytes.NewReader(slowBody))
	reqSlow.Header.Set("Content-Type", "application/json")
	reqSlow.Header.Set("x-claude-code-session-id", "cancel-test-1")
	_, _ = http.DefaultClient.Do(reqSlow)
	time.Sleep(80 * time.Millisecond)

	// 5. Late second parallel tool call arriving after 80ms batch window is delivered on resume
	tools := []map[string]any{
		{
			"name":        "Bash",
			"description": "Execute bash",
			"input_schema": map[string]any{
				"type":       "object",
				"properties": map[string]any{"command": map[string]any{"type": "string"}},
			},
		},
	}
	lateMsgs := []map[string]any{
		{"role": "user", "content": "[FAKE_TOOL_CALL] [FAKE_LATE_SECOND_TOOL] run two tools"},
	}
	lateBody1, _ := json.Marshal(map[string]any{
		"model": "gemini-4-argon", "max_tokens": 512,
		"system": "Primary working directory: /tmp", "tools": tools, "messages": lateMsgs,
	})
	reqL1, _ := http.NewRequest("POST", baseURL+"/v1/messages", bytes.NewReader(lateBody1))
	reqL1.Header.Set("Content-Type", "application/json")
	reqL1.Header.Set("x-claude-code-session-id", "late-tool-sess")
	respL1, err := http.DefaultClient.Do(reqL1)
	if err != nil {
		t.Fatal(err)
	}
	var dL1 map[string]any
	_ = json.NewDecoder(respL1.Body).Decode(&dL1)
	respL1.Body.Close()
	cL1, _ := dL1["content"].([]any)
	var firstToolID string
	for _, c := range cL1 {
		if cm, ok := c.(map[string]any); ok && cm["type"] == "tool_use" {
			firstToolID, _ = cm["id"].(string)
		}
	}
	if firstToolID == "" {
		t.Fatalf("expected 1st tool_use id, got %+v", dL1)
	}
	// Wait 120ms so fakeacp's 2nd tool call arrives after the 80ms batch flush
	time.Sleep(120 * time.Millisecond)

	// Send result for 1st tool -> gateway should now deliver the late 2nd tool call as tool_use!
	lateMsgs = append(lateMsgs,
		map[string]any{"role": "assistant", "content": cL1},
		map[string]any{"role": "user", "content": []map[string]any{{"type": "tool_result", "tool_use_id": firstToolID, "content": "RES_1"}}},
	)
	lateBody2, _ := json.Marshal(map[string]any{
		"model": "gemini-4-argon", "max_tokens": 512,
		"system": "Primary working directory: /tmp", "tools": tools, "messages": lateMsgs,
	})
	reqL2, _ := http.NewRequest("POST", baseURL+"/v1/messages", bytes.NewReader(lateBody2))
	reqL2.Header.Set("Content-Type", "application/json")
	reqL2.Header.Set("x-claude-code-session-id", "late-tool-sess")
	respL2, err := http.DefaultClient.Do(reqL2)
	if err != nil {
		t.Fatal(err)
	}
	var dL2 map[string]any
	_ = json.NewDecoder(respL2.Body).Decode(&dL2)
	respL2.Body.Close()
	if dL2["stop_reason"] != "tool_use" {
		t.Fatalf("expected late 2nd tool call to produce stop_reason=tool_use, got %v (%+v)", dL2["stop_reason"], dL2)
	}
	cL2, _ := dL2["content"].([]any)
	var secondToolID string
	for _, c := range cL2 {
		if cm, ok := c.(map[string]any); ok && cm["type"] == "tool_use" {
			secondToolID, _ = cm["id"].(string)
		}
	}
	// Send result for 2nd tool -> turn finishes with end_turn!
	lateMsgs = append(lateMsgs,
		map[string]any{"role": "assistant", "content": cL2},
		map[string]any{"role": "user", "content": []map[string]any{{"type": "tool_result", "tool_use_id": secondToolID, "content": "RES_2"}}},
	)
	lateBody3, _ := json.Marshal(map[string]any{
		"model": "gemini-4-argon", "max_tokens": 512,
		"system": "Primary working directory: /tmp", "tools": tools, "messages": lateMsgs,
	})
	reqL3, _ := http.NewRequest("POST", baseURL+"/v1/messages", bytes.NewReader(lateBody3))
	reqL3.Header.Set("Content-Type", "application/json")
	reqL3.Header.Set("x-claude-code-session-id", "late-tool-sess")
	respL3, err := http.DefaultClient.Do(reqL3)
	if err != nil {
		t.Fatal(err)
	}
	var dL3 map[string]any
	_ = json.NewDecoder(respL3.Body).Decode(&dL3)
	respL3.Body.Close()
	if dL3["stop_reason"] != "end_turn" {
		t.Fatalf("expected end_turn after 2nd tool result, got %v", dL3["stop_reason"])
	}
}

func TestStage2PersistentStateAndRestartWarmResume(t *testing.T) {
	stateDB := filepath.Join(t.TempDir(), "persist-gateway.db")
	GlobalStore.Init(stateDB)
	defer GlobalStore.Init(GatewayStateDB)

	// Instance 1: run a turn with session ID "persist-sess-1"
	gs1 := NewGatewayServer()
	t.Cleanup(gs1.Close)
	ln1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	GatewayPort = ln1.Addr().(*net.TCPAddr).Port
	srv1 := &http.Server{Handler: gs1}
	go srv1.Serve(ln1)
	url1 := fmt.Sprintf("http://127.0.0.1:%d", GatewayPort)

	tools := []map[string]any{
		{"name": "Bash", "input_schema": map[string]any{"type": "object"}},
	}
	msgs := []map[string]any{
		{"role": "user", "content": "Turn 1 before gateway restart"},
	}
	b1, _ := json.Marshal(map[string]any{
		"model": "gemini-4-argon", "max_tokens": 256,
		"system": "Primary working directory: /tmp", "tools": tools, "messages": msgs,
	})
	req1, _ := http.NewRequest("POST", url1+"/v1/messages", bytes.NewReader(b1))
	req1.Header.Set("Content-Type", "application/json")
	req1.Header.Set("x-claude-code-session-id", "persist-sess-1")
	resp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatal(err)
	}
	resp1.Body.Close()
	_ = srv1.Close()

	// Instance 2 (simulating gateway restart with the same gateway.db):
	// Should restore "persist-sess-1" as Detached and wake via session/resume (mode=resumed_delta)
	gs2 := NewGatewayServer()
	t.Cleanup(gs2.Close)
	ln2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	GatewayPort = ln2.Addr().(*net.TCPAddr).Port
	srv2 := &http.Server{Handler: gs2}
	go srv2.Serve(ln2)
	defer srv2.Close()
	url2 := fmt.Sprintf("http://127.0.0.1:%d", GatewayPort)

	msgs = append(msgs,
		map[string]any{"role": "assistant", "content": "FAKE_OK"},
		map[string]any{"role": "user", "content": "Turn 2 after gateway restart"},
	)
	b2, _ := json.Marshal(map[string]any{
		"model": "gemini-4-argon", "max_tokens": 256,
		"system": "Primary working directory: /tmp", "tools": tools, "messages": msgs,
	})
	req2, _ := http.NewRequest("POST", url2+"/v1/messages", bytes.NewReader(b2))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("x-claude-code-session-id", "persist-sess-1")
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()

	stats := gs2.GetStats()
	projs, _ := stats["projects"].([]map[string]any)
	foundResumed := false
	for _, p := range projs {
		ck, _ := p["convKey"].(string)
		if strings.HasPrefix(ck, "sid:persist-sess-1:") {
			turns, _ := p["turns"].([]TurnHistoryEntry)
			for _, tr := range turns {
				if tr.Mode == "resumed_delta" {
					foundResumed = true
				}
			}
		}
	}
	if !foundResumed {
		t.Fatalf("expected restored session to wake with mode=resumed_delta, stats=%+v", projs)
	}
}

func TestStage3SlowClientDoesNotBlockOtherSessions(t *testing.T) {
	baseURL, cleanup := startTestGateway(t)
	defer cleanup()

	// Session A: slow streaming consumer (blocks inside onDelta via activeTurnState.deltaCh decoupling)
	sessA := NewAcpProjectSession("sid:slow-a:u:11111111", "/tmp", "Primary working directory: /tmp")
	sessB := NewAcpProjectSession("sid:fast-b:u:22222222", "/tmp", "Primary working directory: /tmp")

	blockCh := make(chan struct{})
	startedSlow := make(chan struct{}, 1)
	doneA := make(chan error, 1)
	go func() {
		_, err := sessA.RunTurn(
			context.Background(),
			[]map[string]any{{"role": "user", "content": "[FAKE_THINK] Slow stream A"}},
			nil,
			"medium",
			func(c DeltaChunk) {
				select {
				case startedSlow <- struct{}{}:
				default:
				}
				<-blockCh // Simulate slow HTTP writer blocking onDelta consumer goroutine
			},
			nil,
		)
		doneA <- err
	}()

	// Wait until sessA's onDelta is blocked
	select {
	case <-startedSlow:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for slow session A to receive first chunk")
	}

	// While sessA's onDelta is blocked, sessB must complete its turn via readStdoutLoop without stalling!
	t0 := time.Now()
	resB, errB := sessB.RunTurn(
		context.Background(),
		[]map[string]any{{"role": "user", "content": "Fast request B"}},
		nil,
		"medium",
		nil,
		nil,
	)
	if errB != nil || !strings.Contains(resB.Text, "FAKE_OK") {
		t.Fatalf("expected fast session B to succeed while session A is blocked, err=%v text=%q", errB, resB.Text)
	}
	if time.Since(t0) > 1500*time.Millisecond {
		t.Fatalf("session B took too long (%v) while session A was blocked", time.Since(t0))
	}

	// Unblock sessA and verify it finishes cleanly
	close(blockCh)
	if errA := <-doneA; errA != nil {
		t.Fatalf("expected session A to finish cleanly after unblocking, got %v", errA)
	}

	// Also verify HTTP streaming on baseURL still works end-to-end
	resp, err := http.Post(baseURL+"/v1/messages", "application/json", strings.NewReader(`{"model":"gemini-3.8-flash","stream":true,"messages":[{"role":"user","content":"Hello"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
}

func TestStage5MetricsLogRotationAndQuotaSingleflight(t *testing.T) {
	gs := NewGatewayServer()
	t.Cleanup(gs.Close)
	dashHandler := gs.DashboardOnlyHandler()

	// 1. /metrics on dashboard port returns Prometheus format
	rec := httptest.NewRecorder()
	dashHandler.ServeHTTP(rec, newLoopbackRequest("GET", "/metrics", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "agy_gateway_live_workers") {
		t.Fatalf("unexpected /metrics response: code=%d body=%s", rec.Code, rec.Body.String())
	}

	// 2. Log rotation rotates gateway.log -> gateway.log.1 when LogMaxBytes exceeded
	tmpLogDir := t.TempDir()
	origLogFile := GatewayLogFile
	origMaxBytes := LogMaxBytes
	GatewayLogFile = filepath.Join(tmpLogDir, "gateway.log")
	LogMaxBytes = 256
	defer func() {
		GatewayLogFile = origLogFile
		LogMaxBytes = origMaxBytes
	}()
	for i := 0; i < 6; i++ {
		LogEvent("ROTATE_TEST", strings.Repeat("Z", 80), map[string]any{"idx": i})
	}
	if _, err := os.Stat(GatewayLogFile + ".1"); err != nil {
		t.Fatalf("expected rotated log file %s.1 to exist: %v", GatewayLogFile, err)
	}
}

func TestStage7OutboundSanitizerAndTosCircuitBreaker(t *testing.T) {
	// 1. Verify Stage A: dsh-bridge.md native Antigravity Agent identity + non-adversarial buildAppendSystemPrompt
	EnsurePureApiHome()
	mdBytes, err := os.ReadFile(filepath.Join(PureApiHome, "config", "plugins", "dsh-bridge", "agents", "dsh-bridge.md"))
	if err != nil {
		t.Fatal(err)
	}
	mdStr := string(mdBytes)
	if !strings.Contains(mdStr, "You are Antigravity Agent, a powerful agentic AI coding assistant designed by the Google engineering team.") {
		t.Fatalf("expected native Antigravity Agent identity in dsh-bridge.md, got:\n%s", mdStr)
	}

	rawClaudeSys := strings.Join([]string{
		"x-anthropic-billing-header: cc_version=2.1.273; cch=9f8e7d",
		"",
		"You are Claude Code, Anthropic's official CLI for Claude, running on the Claude Agent SDK.",
		"",
		"IMPORTANT: Assist with authorized security testing, defensive security, CTF challenges, and educational contexts. Refuse requests for destructive techniques, DoS attacks, mass targeting, supply chain compromise, or detection evasion for malicious purposes. Dual-use security tools (C2 frameworks, credential testing, exploit development) require clear authorization context.",
		"",
		"# Environment",
		"Primary working directory: /home/developer/projects/agy-acp-gateway",
		"Claude Code is available as a CLI. See https://claude.ai/code/docs and https://github.com/anthropics/claude-code for details. Fast mode for Claude Code uses Claude Opus.",
		"",
		"# Project instructions",
		"Always check `/home/developer/.claude/CLAUDE.md` and `~/.codex/config.toml` and invoke skill `claude-design` or `ccb-diagnose` when appropriate. Never leak OpenClaw or OpenAI Codex or PI-Desktop brand names.",
	}, "\n")

	prunedSys := normalizeSystemText(rawClaudeSys)
	sess := NewAcpProjectSession("test-stage7", "/home/developer/projects/agy-acp-gateway", prunedSys)
	tools := sanitizeToolsCatalog([]any{
		map[string]any{
			"name":        "AskUserQuestion",
			"description": "Invoke Claude Code's AskUserQuestion in OpenClaw or OpenAI Codex",
			"input_schema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"question": map[string]any{"type": "string", "description": "Prompt for Claude Code user"},
				},
			},
		},
	})
	appendPrompt := sess.buildAppendSystemPrompt(tools)

	userMsg := map[string]any{
		"role":    "user",
		"content": "<system-reminder>\nClaude Code attached this context automatically from /home/developer/.claude/CLAUDE.md and ~/.codex/config.toml using skill claude-design.\nupdate-config: Use this skill to configure the Claude Code harness.\n</system-reminder>\nPlease check the repository.",
	}
	cleanedUserMsg := formatSingleMessageText(userMsg)

	combinedOutbound := appendPrompt + "\n" + cleanedUserMsg + "\n" + tools[0].Description
	forbiddenSubstrings := []string{
		"Claude Code",
		"Claude Agent SDK",
		"Anthropic's official CLI",
		"claude.ai/code",
		"github.com/anthropics/claude-code",
		"C2 frameworks",
		"exploit development",
		"OpenClaw",
		"OpenAI Codex",
		"PI-Desktop",
		"<system-reminder>",
		"</system-reminder>",
		"built-in Antigravity",
		"x-anthropic-billing-header",
	}
	for _, bad := range forbiddenSubstrings {
		if strings.Contains(combinedOutbound, bad) {
			t.Fatalf("outbound cleanroom leaked forbidden substring %q in:\n%s", bad, combinedOutbound)
		}
	}

	requiredPreserved := []string{
		"Primary working directory: /home/developer/projects/agy-acp-gateway",
		"/home/developer/.claude/CLAUDE.md",
		"~/.codex/config.toml",
		"claude-design",
		"ccb-diagnose",
		"<workspace-context>",
		"</workspace-context>",
		"The workspace IDE attached this context automatically",
	}
	for _, keep := range requiredPreserved {
		if !strings.Contains(combinedOutbound, keep) {
			t.Fatalf("outbound cleanroom failed to preserve required token %q in:\n%s", keep, combinedOutbound)
		}
	}

	// 2. Verify 100x byte-for-byte determinism of SanitizeOutboundText & PruneClientSystemPrompt
	for i := 0; i < 100; i++ {
		if got := PruneClientSystemPrompt(rawClaudeSys); got != prunedSys {
			t.Fatalf("PruneClientSystemPrompt non-deterministic on iteration %d", i)
		}
		if got := formatSingleMessageText(userMsg); got != cleanedUserMsg {
			t.Fatalf("SanitizeOutboundText non-deterministic on iteration %d", i)
		}
	}

	// 3. Verify 1m: ToS 403 circuit breaker (RPC error & soft error + persistence in runtime/tos-tripped)
	tosFile := filepath.Join(t.TempDir(), "tos-tripped")
	origTosFile := TosTrippedFile
	TosTrippedFile = tosFile
	defer func() {
		_ = os.Remove(tosFile)
		TosTrippedFile = origTosFile
	}()

	baseURL, cleanup := startTestGateway(t)
	defer cleanup()

	// Trigger ToS RPC error via fakeacp
	tosBody, _ := json.Marshal(map[string]any{
		"model":      "gemini-3.8-flash",
		"max_tokens": 128,
		"messages":   []map[string]any{{"role": "user", "content": "[FAKE_TOS_RPC_ERROR]"}},
	})
	respTos, err := http.Post(baseURL+"/v1/messages", "application/json", bytes.NewReader(tosBody))
	if err != nil {
		t.Fatal(err)
	}
	var dTos map[string]any
	_ = json.NewDecoder(respTos.Body).Decode(&dTos)
	respTos.Body.Close()
	if respTos.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 on ToS RPC error, got %d (%+v)", respTos.StatusCode, dTos)
	}
	if _, err := os.Stat(tosFile); err != nil {
		t.Fatalf("expected %s file to be created on ToS trip: %v", tosFile, err)
	}

	// Subsequent normal requests must be immediately rejected with 503 while tos-tripped exists
	okBody, _ := json.Marshal(map[string]any{
		"model":      "gemini-3.8-flash",
		"max_tokens": 128,
		"messages":   []map[string]any{{"role": "user", "content": "Normal hello"}},
	})
	respBlocked, err := http.Post(baseURL+"/v1/messages", "application/json", bytes.NewReader(okBody))
	if err != nil {
		t.Fatal(err)
	}
	var dBlocked map[string]any
	_ = json.NewDecoder(respBlocked.Body).Decode(&dBlocked)
	respBlocked.Body.Close()
	if respBlocked.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected subsequent request to be blocked with 503, got %d", respBlocked.StatusCode)
	}
	errObj, _ := dBlocked["error"].(map[string]any)
	if errObj["type"] != "tos_circuit_breaker" {
		t.Fatalf("expected error.type=tos_circuit_breaker, got %+v", dBlocked)
	}

	// Simulate gateway restart while tos-tripped file still exists -> still tripped!
	gsRestarted := NewGatewayServer()
	t.Cleanup(gsRestarted.Close)
	stats := gsRestarted.GetStats()
	if stats["tosTripped"] != true {
		t.Fatalf("expected tosTripped=true after gateway restart, got %+v", stats["tosTripped"])
	}

	// Remove tos-tripped file manually -> circuit breaker resets and requests succeed again!
	_ = os.Remove(tosFile)
	respRecovered, err := http.Post(baseURL+"/v1/messages", "application/json", bytes.NewReader(okBody))
	if err != nil {
		t.Fatal(err)
	}
	respRecovered.Body.Close()
	if respRecovered.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK after removing tos-tripped file, got %d", respRecovered.StatusCode)
	}

	// 4. Trigger ToS soft error text via fakeacp -> also trips circuit breaker and writes tos-tripped!
	tosSoftBody, _ := json.Marshal(map[string]any{
		"model":      "gemini-3.8-flash",
		"max_tokens": 128,
		"messages":   []map[string]any{{"role": "user", "content": "[FAKE_TOS_SOFT_ERROR]"}},
	})
	respSoftTos, err := http.Post(baseURL+"/v1/messages", "application/json", bytes.NewReader(tosSoftBody))
	if err != nil {
		t.Fatal(err)
	}
	respSoftTos.Body.Close()
	if respSoftTos.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 on ToS soft error, got %d", respSoftTos.StatusCode)
	}
	if _, err := os.Stat(tosFile); err != nil {
		t.Fatalf("expected %s file to be created on ToS soft error: %v", tosFile, err)
	}
	_ = os.Remove(tosFile)
}

func newLoopbackRequest(method, target string, body io.Reader) *http.Request {
	r := httptest.NewRequest(method, target, body)
	r.Host = "127.0.0.1:18892"
	return r
}
