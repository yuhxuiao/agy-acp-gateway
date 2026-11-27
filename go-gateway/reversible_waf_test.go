package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStep1LegacyDshBridgeHibernatedWakeupCompatibility(t *testing.T) {
	baseURL, cleanup := startTestGateway(t)
	defer cleanup()

	readTool := map[string]any{
		"name":         "Read",
		"description":  "Read a file",
		"input_schema": map[string]any{"type": "object", "properties": map[string]any{"file_path": map[string]any{"type": "string"}}},
	}
	writeTool := map[string]any{
		"name":         "Write",
		"description":  "Write a file",
		"input_schema": map[string]any{"type": "object", "properties": map[string]any{"file_path": map[string]any{"type": "string"}}},
	}

	// 1. New session uses ws-bridge and --agent ws-bridge
	bodyNew, _ := json.Marshal(map[string]any{
		"model":      "gemini-3.8-flash",
		"max_tokens": 128,
		"tools":      []map[string]any{readTool},
		"messages":   []map[string]any{{"role": "user", "content": "[ECHO_CLIENT_INFO] [ECHO_TOOLS] hello"}},
	})
	reqNew, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/messages", bytes.NewReader(bodyNew))
	reqNew.Header.Set("Content-Type", "application/json")
	reqNew.Header.Set("x-session-id", "step1-fresh-ws")
	respNew, err := http.DefaultClient.Do(reqNew)
	if err != nil {
		t.Fatal(err)
	}
	rawNew, _ := io.ReadAll(respNew.Body)
	respNew.Body.Close()
	outNew := string(rawNew)
	if !strings.Contains(outNew, "--agent|ws-bridge") {
		t.Fatalf("expected --agent ws-bridge in spawn args, got: %s", outNew)
	}
	if !strings.Contains(outNew, "ws_Read_") || strings.Contains(outNew, "dsh_Read_") {
		t.Fatalf("expected new session to expose ws_Read_ MCP tool, got: %s", outNew)
	}

	// 2. Simulate a pre-upgrade hibernated dsh-bridge session restored from gateway.db, then add a new tool!
	legacyReadMcp := "dsh_Read_" + shortSha256("Read")[:12]
	legacyCat := sanitizeToolsCatalog([]any{readTool})
	legacyCat[0].McpName = legacyReadMcp

	user1 := map[string]any{"role": "user", "content": "turn one"}
	legacySess := NewAcpProjectSession("sess:step1-legacy-dsh", DefaultCwd, "")
	legacySess.HibernatedSessionID = "fake-hibernated-dsh-sid"
	legacySess.ModelID = "gemini-3.8-flash"
	legacySess.ThoughtLevel = "low"
	legacySess.InitialToolsCatalog = append([]ToolDef(nil), legacyCat...)
	legacySess.ToolsCatalog = append([]ToolDef(nil), legacyCat...)
	legacySess.DeclaredMcpTools = map[string]bool{"Read": true}
	legacySess.McpToHostMap = map[string]string{legacyReadMcp: "Read"}
	legacySess.ToolFingerprint = toolCatalogFingerprint(legacyCat)
	legacySess.MessageCount = 2
	legacySess.lastTurnAssistant = true
	legacySess.HistoryFingerprint = computeFirstUserFingerprint([]map[string]any{user1})

	if legacySess.bridgeServerName() != "dsh-bridge" {
		t.Fatalf("expected woken pre-upgrade session to keep bridgeServerName()=dsh-bridge, got %q", legacySess.bridgeServerName())
	}

	// Simulate post-upgrade consumeToolCatalogLocked when a deferred tool (Write) is added on turn 2:
	legacySess.mu.Lock()
	notice, added, _, _ := legacySess.consumeToolCatalogLocked(sanitizeToolsCatalog([]any{readTool, writeTool}))
	resolvedRead, okRead := resolveAllowedHostTool(legacySess.McpToHostMap, "mcp_dsh-bridge_"+legacyReadMcp)
	resolvedWrite, okWrite := allowedHostToolName(legacySess.McpToHostMap, "Write", legacySess.ToolsCatalog)
	legacySess.mu.Unlock()

	if len(added) != 1 || added[0] != "Write" {
		t.Fatalf("expected Write to be added in-place, got added=%v", added)
	}
	if !strings.Contains(notice, "mcp_dsh-bridge_dsh_dsh_call_tool_") {
		t.Fatalf("expected legacy session tool delta notice to reference dsh-bridge dsh_call_tool helper, got:\n%s", notice)
	}
	if !okRead || resolvedRead != "Read" {
		t.Fatalf("expected original mcp_dsh-bridge_%s to remain authorized after tool addition, got ok=%v name=%q", legacyReadMcp, okRead, resolvedRead)
	}
	if !okWrite || resolvedWrite != "Write" {
		t.Fatalf("expected newly added Write to be callable via dsh_call_tool, got ok=%v name=%q", okWrite, resolvedWrite)
	}
}

func TestStep2ThreeTierSemanticIntegrationVsUserPersonas(t *testing.T) {
	// Tier 1: Unknown future third-party agent harness self-attributions & host UI shells MUST be pruned cleanly
	unknownHarnessSys := strings.Join([]string{
		"You are an autonomous coding agent powered by Zeta Harness.",
		"You are NovaCode, Acme Corp's official CLI for Nova models.",
		"You are interacting with the user through the HyperCode desktop GUI.",
		"Primary working directory: /workspace/demo",
		"Always run unit tests with -race before returning.",
	}, "\n")
	pruned := PruneClientSystemPrompt(unknownHarnessSys)
	for _, banned := range []string{"Zeta Harness", "NovaCode", "Acme Corp", "HyperCode desktop GUI"} {
		if strings.Contains(pruned, banned) {
			t.Fatalf("expected unknown harness attribution %q to be merged/pruned into ACP identity, got:\n%s", banned, pruned)
		}
	}
	for _, want := range []string{"Primary working directory: /workspace/demo", "Always run unit tests with -race before returning."} {
		if !strings.Contains(pruned, want) {
			t.Fatalf("expected workspace context & project rule %q to survive, got:\n%s", want, pruned)
		}
	}

	// Tier 3: User-defined custom personas (without product/vendor attribution) & ordinary English words MUST survive 100% intact
	userPersonaSys := strings.Join([]string{
		"You are a senior Go backend architect focused on low-latency distributed systems.",
		"You are a helpful coding assistant specialized in Rust and WebAssembly.",
		"You are Alice, a Staff Security Engineer reviewing pull requests.",
		"Move the cursor to line 42 and inspect the op-amp bias voltage.",
	}, "\n")
	prunedPersona := PruneClientSystemPrompt(userPersonaSys)
	for _, want := range []string{
		"You are a senior Go backend architect focused on low-latency distributed systems.",
		"You are a helpful coding assistant specialized in Rust and WebAssembly.",
		"You are Alice, a Staff Security Engineer reviewing pull requests.",
		"Move the cursor to line 42 and inspect the op-amp bias voltage.",
	} {
		if !strings.Contains(prunedPersona, want) {
			t.Fatalf("expected custom user persona / technical prose %q to be 100%% preserved, got:\n%s", want, prunedPersona)
		}
	}
}

func TestStep3ReversibleCodecChunkSplitsAndEndToEndToolRoundtrip(t *testing.T) {
	// 1. Unit test NeutralizeToolIdentifier
	for in, want := range map[string]string{
		"dsh_present":           "ws_present",
		"DSH_Artifact":          "ws_Artifact",
		"mcp__claude_code__run": "mcp__ws_agent__run",
		"claude_browser":        "ws_browser",
		"Read":                  "Read",
		"Bash":                  "Bash",
	} {
		if got := NeutralizeToolIdentifier(in); got != want {
			t.Fatalf("NeutralizeToolIdentifier(%q) = %q, want %q", in, got, want)
		}
	}

	// 2. Unit test ReversibleSessionCodec + StreamRestorer across ALL 1..13-byte split boundaries
	codec := NewReversibleSessionCodec("tenant-secret-key-123")
	raw := "<dsh_artifact>Check /Applications/DeepSeek Harness.app/Contents/MacOS/dsh and ws literal wsseg_zzzzzzzz wstag_12</dsh_artifact>"
	san := SanitizeOutboundText(raw)
	encoded := codec.EncodeOutboundText(san)
	if strings.Contains(encoded, "DeepSeek Harness") || strings.Contains(encoded, "dsh_artifact") {
		t.Fatalf("expected vendor path & XML tag to be reversibly encoded, got: %s", encoded)
	}
	if codec.Count() != 2 {
		t.Fatalf("expected exactly 2 reversible mappings (1 tag + 1 path phrase), got %d", codec.Count())
	}

	for step := 1; step <= 14; step++ {
		sr := codec.NewStreamRestorer()
		var out strings.Builder
		for i := 0; i < len(encoded); i += step {
			end := i + step
			if end > len(encoded) {
				end = len(encoded)
			}
			out.WriteString(sr.Push(encoded[i:end]))
		}
		out.WriteString(sr.Flush())
		if out.String() != raw {
			t.Fatalf("StreamRestorer chunk step=%d mismatch:\ngot:  %q\nwant: %q", step, out.String(), raw)
		}
	}

	// Empty codec must hold 0 bytes even on partial "wsseg_" prefix
	emptyCodec := NewReversibleSessionCodec("empty")
	emptySr := emptyCodec.NewStreamRestorer()
	if got := emptySr.Push("wsseg_"); got != "wsseg_" {
		t.Fatalf("expected empty codec StreamRestorer to pass through immediately with 0 buffering, got %q", got)
	}

	// 3. End-to-end streaming SSE + vendor-aliased MCP tool call + tool_result sanitization roundtrip
	baseURL, cleanup := startTestGateway(t)
	defer cleanup()

	tools := []map[string]any{
		{
			"name":         "Read",
			"description":  "Read file in Claude Code",
			"input_schema": map[string]any{"type": "object", "properties": map[string]any{"file_path": map[string]any{"type": "string"}}},
		},
		{
			"name":         "dsh_present",
			"description":  "Present DeepSeek Harness artifact",
			"input_schema": map[string]any{"type": "object", "properties": map[string]any{"path": map[string]any{"type": "string"}}},
		},
	}
	sys := "You are an agentic coding assistant built by DeepSeek Harness.\nApp bundle: /Applications/DeepSeek Harness.app/Contents/Resources"
	userMsg := "<system-reminder>Claude Code opened <dsh_artifact>/Applications/DeepSeek Harness.app/Contents/MacOS/dsh</dsh_artifact></system-reminder>\n[FAKE_REVERSIBLE_ROUNDTRIP] [FAKE_CALL_VENDOR_ALIAS] go"

	req1Body, _ := json.Marshal(map[string]any{
		"model":      "gemini-3.8-flash",
		"max_tokens": 256,
		"stream":     true,
		"system":     sys,
		"tools":      tools,
		"messages":   []map[string]any{{"role": "user", "content": userMsg}},
	})
	req1, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/messages", bytes.NewReader(req1Body))
	req1.Header.Set("Content-Type", "application/json")
	req1.Header.Set("x-session-id", "step3-e2e-roundtrip")
	resp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatal(err)
	}
	sse1Bytes, _ := io.ReadAll(resp1.Body)
	resp1.Body.Close()
	if resp1.StatusCode != 200 {
		t.Fatalf("turn 1 status=%d body=%s", resp1.StatusCode, string(sse1Bytes))
	}

	var toolUseID, toolUseName, inputJSON string
	for _, line := range strings.Split(string(sse1Bytes), "\n") {
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var ev map[string]any
		if json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &ev) != nil {
			continue
		}
		switch ev["type"] {
		case "content_block_start":
			if cb, ok := ev["content_block"].(map[string]any); ok && cb["type"] == "tool_use" {
				toolUseID, _ = cb["id"].(string)
				toolUseName, _ = cb["name"].(string)
			}
		case "content_block_delta":
			if d, ok := ev["delta"].(map[string]any); ok && d["type"] == "input_json_delta" {
				part, _ := d["partial_json"].(string)
				inputJSON += part
			}
		}
	}
	if toolUseName != "dsh_present" {
		t.Fatalf("expected outbound alias ws_present to be restored to client tool name dsh_present, got %q (sse=%s)", toolUseName, string(sse1Bytes))
	}
	var parsedInput map[string]any
	if err := json.Unmarshal([]byte(inputJSON), &parsedInput); err != nil {
		t.Fatalf("invalid tool_use input JSON %q: %v", inputJSON, err)
	}
	if gotPath, _ := parsedInput["path"].(string); gotPath != "/Applications/DeepSeek Harness.app/Contents/MacOS/dsh" {
		t.Fatalf("expected tool_use path argument to be reversibly restored, got %q", gotPath)
	}

	// Send tool_result containing <system-reminder> + vendor strings + spaced vendor path
	msgs2 := []map[string]any{
		{"role": "user", "content": userMsg},
		{"role": "assistant", "content": []map[string]any{
			{"type": "tool_use", "id": toolUseID, "name": toolUseName, "input": parsedInput},
		}},
		{"role": "user", "content": []map[string]any{
			{
				"type":        "tool_result",
				"tool_use_id": toolUseID,
				"content":     "<system-reminder>Claude Code read /Applications/DeepSeek Harness.app/Contents/MacOS/dsh</system-reminder>\nBINARY_OK",
			},
		}},
	}
	req2Body, _ := json.Marshal(map[string]any{
		"model":      "gemini-3.8-flash",
		"max_tokens": 256,
		"stream":     true,
		"system":     sys,
		"tools":      tools,
		"messages":   msgs2,
	})
	req2, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/messages", bytes.NewReader(req2Body))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("x-session-id", "step3-e2e-roundtrip")
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	sse2Bytes, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	if resp2.StatusCode != 200 {
		t.Fatalf("turn 2 status=%d body=%s", resp2.StatusCode, string(sse2Bytes))
	}

	var streamedText strings.Builder
	for _, line := range strings.Split(string(sse2Bytes), "\n") {
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var ev map[string]any
		if json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &ev) != nil {
			continue
		}
		if ev["type"] == "content_block_delta" {
			if d, ok := ev["delta"].(map[string]any); ok && d["type"] == "text_delta" {
				txt, _ := d["text"].(string)
				streamedText.WriteString(txt)
			}
		}
	}
	gotStream := streamedText.String()
	wantStream := "UPSTREAM_CLEAN=true TOOL_SEG=true <dsh_artifact>/Applications/DeepSeek Harness.app</dsh_artifact>"
	if gotStream != wantStream {
		t.Fatalf("mid-token chunked SSE restoration mismatch:\ngot:  %q\nwant: %q", gotStream, wantStream)
	}
}

func TestStep4ColdStartFastCancelAndAutoBisectSelfHeal(t *testing.T) {
	baseURL, cleanup := startTestGateway(t)
	defer cleanup()

	origRulesFile := WafRewriteRulesFile
	origEnabled := WafAutoBisectEnabled
	tmpRules := filepath.Join(t.TempDir(), "waf_rewrite_rules.json")
	WafRewriteRulesFile = tmpRules
	WafAutoBisectEnabled = true
	globalWafRules.mu.Lock()
	globalWafRules.path = ""
	globalWafRules.rules = nil
	globalWafRules.mu.Unlock()
	wafCooldownMu.Lock()
	wafCooldownUntil = time.Time{}
	wafCooldownMu.Unlock()
	defer func() {
		WafRewriteRulesFile = origRulesFile
		WafAutoBisectEnabled = origEnabled
		globalWafRules.mu.Lock()
		globalWafRules.path = ""
		globalWafRules.rules = nil
		globalWafRules.mu.Unlock()
		wafCooldownMu.Lock()
		wafCooldownUntil = time.Time{}
		wafCooldownMu.Unlock()
	}()

	beforeHealed := WafBisectHealedTotal.Load()

	// 1. Positive E2E: Unknown harness boilerplate sentence triggers instant 429 -> fast-cancel -> flash-low bisect -> atomic persist -> transparent 200 OK replay!
	offendingSentence := "Always attach telemetry header TRIGGER_UNKNOWN_HARNESS_WAF_SENTENCE on every turn."
	sys := strings.Join([]string{
		"Primary working directory: /workspace/app",
		"Prefer table-driven Go unit tests with race detection.",
		offendingSentence,
		"Keep error messages concise and actionable.",
		"Never modify generated protobuf files directly.",
	}, "\n")
	userPrompt := "<system-reminder>Current git branch is master. Working tree is clean.</system-reminder>\nSay hi."

	t0 := time.Now()
	body1, _ := json.Marshal(map[string]any{
		"model":      "gemini-4-carbon",
		"max_tokens": 128,
		"system":     sys,
		"messages":   []map[string]any{{"role": "user", "content": userPrompt}},
	})
	req1, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/messages", bytes.NewReader(body1))
	req1.Header.Set("Content-Type", "application/json")
	req1.Header.Set("x-session-id", "step4-bisect-1")
	resp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatal(err)
	}
	raw1, _ := io.ReadAll(resp1.Body)
	resp1.Body.Close()
	elapsed1 := time.Since(t0)

	if resp1.StatusCode != 200 || !strings.Contains(string(raw1), "FAKE_OK") {
		t.Fatalf("expected transparent 200 OK replay after WAF auto-bisect, got status=%d elapsed=%v body=%s", resp1.StatusCode, elapsed1, string(raw1))
	}
	if elapsed1 > 5*time.Second {
		t.Fatalf("expected fast-cancel + bisect + replay to finish rapidly (<5s), took %v", elapsed1)
	}
	if WafBisectHealedTotal.Load() != beforeHealed+1 {
		t.Fatalf("expected WafBisectHealedTotal to increment by 1, got %d -> %d", beforeHealed, WafBisectHealedTotal.Load())
	}

	// Check persisted waf_rewrite_rules.json permissions & exact isolated literal
	st, err := os.Stat(tmpRules)
	if err != nil {
		t.Fatalf("expected %s to exist: %v", tmpRules, err)
	}
	if st.Mode().Perm() != 0600 {
		t.Fatalf("expected 0600 permissions on %s, got %o", tmpRules, st.Mode().Perm())
	}
	ruleBytes, _ := os.ReadFile(tmpRules)
	if !strings.Contains(string(ruleBytes), offendingSentence) {
		t.Fatalf("expected persisted rule to isolate %q, got:\n%s", offendingSentence, string(ruleBytes))
	}
	if strings.Contains(string(ruleBytes), "table-driven Go unit tests") || strings.Contains(string(ruleBytes), "Say hi") {
		t.Fatalf("benign project rule or user prompt must NEVER be persisted in waf_rewrite_rules.json:\n%s", string(ruleBytes))
	}

	// 2. Second cold-start request with the exact same offending sentence succeeds immediately with 0 extra bisects!
	req2, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/messages", bytes.NewReader(body1))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("x-session-id", "step4-bisect-2")
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	raw2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	if resp2.StatusCode != 200 || !strings.Contains(string(raw2), "FAKE_OK") {
		t.Fatalf("expected second request to be scrubbed immediately via hot-reloaded rule, got status=%d body=%s", resp2.StatusCode, string(raw2))
	}
	if WafBisectHealedTotal.Load() != beforeHealed+1 {
		t.Fatalf("expected 0 extra bisects on second request, got %d", WafBisectHealedTotal.Load())
	}

	// 3. Negative Test A: Trigger phrase inside ordinary user message prose is NEVER bisected or persisted!
	userOnlyBody, _ := json.Marshal(map[string]any{
		"model":      "gemini-3.8-flash",
		"max_tokens": 64,
		"system":     "Primary working directory: /workspace/app\nKeep answers concise and factual.",
		"messages":   []map[string]any{{"role": "user", "content": "Why did TRIGGER_UNKNOWN_HARNESS_WAF_SENTENCE fail yesterday?"}},
	})
	reqUser, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/messages", bytes.NewReader(userOnlyBody))
	reqUser.Header.Set("Content-Type", "application/json")
	reqUser.Header.Set("x-session-id", "step4-user-prose-only")
	respUser, err := http.DefaultClient.Do(reqUser)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.ReadAll(respUser.Body)
	respUser.Body.Close()
	if respUser.StatusCode != 429 {
		t.Fatalf("expected 429 when trigger is in user chat prose (non-bisectable), got %d", respUser.StatusCode)
	}
	ruleBytesAfterUser, _ := os.ReadFile(tmpRules)
	if strings.Contains(string(ruleBytesAfterUser), "yesterday") {
		t.Fatalf("user chat prose MUST NEVER be written to waf_rewrite_rules.json:\n%s", string(ruleBytesAfterUser))
	}

	// 4. Negative Test B: Genuine upstream throttle (Cleanroom Probe 1 also 429s) aborts after 1 probe and enters cooldown!
	sentinel := filepath.Join(DefaultCwd, ".fake_global_429")
	if err := os.WriteFile(sentinel, []byte("1"), 0600); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(sentinel)

	globalBody, _ := json.Marshal(map[string]any{
		"model":      "gemini-3.8-flash",
		"max_tokens": 64,
		"system":     "Primary working directory: /workspace/app\nAlways validate inputs carefully.\nReturn structured JSON when asked.",
		"messages":   []map[string]any{{"role": "user", "content": "Hello"}},
	})
	reqGlobal, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/messages", bytes.NewReader(globalBody))
	reqGlobal.Header.Set("Content-Type", "application/json")
	reqGlobal.Header.Set("x-session-id", "step4-genuine-429")
	respGlobal, err := http.DefaultClient.Do(reqGlobal)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.ReadAll(respGlobal.Body)
	respGlobal.Body.Close()
	if respGlobal.StatusCode != 429 {
		t.Fatalf("expected 429 on genuine upstream throttle, got %d", respGlobal.StatusCode)
	}
	if !isWafBisectInCooldown() {
		t.Fatal("expected WAF auto-bisect cooldown to activate when Cleanroom Probe 1 also returns 429")
	}
}

func TestReviewRemediation_ThinkFlush_HydratedSeed_BisectWithEncodedPath(t *testing.T) {
	baseURL, cleanup := startTestGateway(t)
	defer cleanup()

	// 1. Review Finding #1: thinking chunk ending in 'w' (partial reversible prefix) must flush BEFORE text starts
	// on both /v1/messages (no trailing 2nd thinking block) and /v1/responses (no dropped 'w').
	sysWithPath := "App bundle: \"/Applications/DeepSeek Harness.app/Contents/MacOS/dsh\""
	msgBody, _ := json.Marshal(map[string]any{
		"model":      "gemini-3.8-flash",
		"max_tokens": 128,
		"stream":     true,
		"system":     sysWithPath,
		"messages":   []map[string]any{{"role": "user", "content": "[FAKE_THINK_TRAILING_W] go"}},
	})
	reqAnthro, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/messages", bytes.NewReader(msgBody))
	reqAnthro.Header.Set("Content-Type", "application/json")
	reqAnthro.Header.Set("x-session-id", "rev-think-anthro")
	respAnthro, err := http.DefaultClient.Do(reqAnthro)
	if err != nil {
		t.Fatal(err)
	}
	sseAnthro, _ := io.ReadAll(respAnthro.Body)
	respAnthro.Body.Close()

	var blockTypes []string
	var thinkOut, textOut strings.Builder
	for _, line := range strings.Split(string(sseAnthro), "\n") {
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var ev map[string]any
		if json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &ev) != nil {
			continue
		}
		switch ev["type"] {
		case "content_block_start":
			if cb, ok := ev["content_block"].(map[string]any); ok {
				bt, _ := cb["type"].(string)
				blockTypes = append(blockTypes, bt)
			}
		case "content_block_delta":
			if d, ok := ev["delta"].(map[string]any); ok {
				if d["type"] == "thinking_delta" {
					s, _ := d["thinking"].(string)
					thinkOut.WriteString(s)
				} else if d["type"] == "text_delta" {
					s, _ := d["text"].(string)
					textOut.WriteString(s)
				}
			}
		}
	}
	if strings.Join(blockTypes, ",") != "thinking,text" {
		t.Fatalf("expected exact block order thinking,text without trailing thinking block, got %v (sse=%s)", blockTypes, string(sseAnthro))
	}
	if thinkOut.String() != "Let's check how" || textOut.String() != "Answer ready" {
		t.Fatalf("expected thinking=%q text=%q, got thinking=%q text=%q", "Let's check how", "Answer ready", thinkOut.String(), textOut.String())
	}

	respBody, _ := json.Marshal(map[string]any{
		"model":        "gemini-3.8-flash",
		"stream":       true,
		"instructions": sysWithPath,
		"input":        "[FAKE_THINK_TRAILING_W] go",
	})
	reqResp, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/responses", bytes.NewReader(respBody))
	reqResp.Header.Set("Content-Type", "application/json")
	reqResp.Header.Set("x-session-id", "rev-think-responses")
	respResp, err := http.DefaultClient.Do(reqResp)
	if err != nil {
		t.Fatal(err)
	}
	sseResp, _ := io.ReadAll(respResp.Body)
	respResp.Body.Close()
	if !strings.Contains(string(sseResp), "Let's check how") {
		t.Fatalf("expected /v1/responses reasoning stream to preserve trailing 'w', got:\n%s", string(sseResp))
	}

	// 2. Review Finding #2: Gateway restart / hydrated session with BOTH SystemText wsseg_* AND history wstag_*
	// must seed history wstag_* even though reloadHibernatedSession encodes SystemText first.
	readTool := map[string]any{
		"name":         "Read",
		"description":  "Read a file",
		"input_schema": map[string]any{"type": "object", "properties": map[string]any{"file_path": map[string]any{"type": "string"}}},
	}
	turn1User := "<dsh_artifact>first turn payload</dsh_artifact>"
	turn1Body, _ := json.Marshal(map[string]any{
		"model":      "gemini-3.8-flash",
		"max_tokens": 64,
		"system":     sysWithPath,
		"tools":      []map[string]any{readTool},
		"messages":   []map[string]any{{"role": "user", "content": turn1User}},
	})
	reqT1, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/messages", bytes.NewReader(turn1Body))
	reqT1.Header.Set("Content-Type", "application/json")
	reqT1.Header.Set("x-session-id", "rev-hydrate-seed")
	respT1, err := http.DefaultClient.Do(reqT1)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.ReadAll(respT1.Body)
	respT1.Body.Close()

	var targetSess *AcpProjectSession
	for _, s := range listTestSessions() {
		if strings.Contains(s.ConvKey, "rev-hydrate-seed") {
			targetSess = s
		}
	}
	if targetSess == nil {
		t.Fatal("session rev-hydrate-seed not found")
	}
	// Compute the deterministic wstag_* token for "dsh_artifact" on this session's tenant key,
	// then hibernate & wipe in-memory Codec to simulate a fresh gateway restart hydration!
	probeCodec := NewReversibleSessionCodec(targetSess.TenantKey)
	encodedTagSample := probeCodec.EncodeOutboundText("<dsh_artifact>x</dsh_artifact>")
	wstagTok := strings.TrimSuffix(strings.TrimPrefix(encodedTagSample, "<"), ">x</"+strings.TrimPrefix(strings.Split(encodedTagSample, ">")[0], "<")+">")
	targetSess.HibernateWorker("test_simulate_restart", false)
	targetSess.mu.Lock()
	targetSess.Codec = NewReversibleSessionCodec(targetSess.TenantKey)
	targetSess.mu.Unlock()

	turn2Msgs := []map[string]any{
		{"role": "user", "content": turn1User},
		{"role": "assistant", "content": "FAKE_OK"},
		{"role": "user", "content": "[FAKE_EMIT_TOKEN:" + wstagTok + "] second turn"},
	}
	turn2Body, _ := json.Marshal(map[string]any{
		"model":      "gemini-3.8-flash",
		"max_tokens": 64,
		"system":     sysWithPath,
		"tools":      []map[string]any{readTool},
		"messages":   turn2Msgs,
	})
	reqT2, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/messages", bytes.NewReader(turn2Body))
	reqT2.Header.Set("Content-Type", "application/json")
	reqT2.Header.Set("x-session-id", "rev-hydrate-seed")
	respT2, err := http.DefaultClient.Do(reqT2)
	if err != nil {
		t.Fatal(err)
	}
	rawT2, _ := io.ReadAll(respT2.Body)
	respT2.Body.Close()
	var parsedT2 struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	_ = json.Unmarshal(rawT2, &parsedT2)
	if len(parsedT2.Content) == 0 || !strings.Contains(parsedT2.Content[0].Text, "<dsh_artifact>ok</dsh_artifact>") {
		t.Fatalf("expected hydrated session to seed history wstag_* before reloadHibernatedSession, got: %s", string(rawT2))
	}

	// 3. Review Finding #3: WAF auto-bisect probes must run Codec.EncodeOutboundText so a sentence with a
	// reversibly-encoded vendor path does NOT false-trigger 429 during bisect probes!
	origRulesFile := WafRewriteRulesFile
	tmpRules := filepath.Join(t.TempDir(), "waf_rules_encoded_path.json")
	WafRewriteRulesFile = tmpRules
	globalWafRules.mu.Lock()
	globalWafRules.path = ""
	globalWafRules.rules = nil
	globalWafRules.mu.Unlock()
	wafCooldownMu.Lock()
	wafCooldownUntil = time.Time{}
	wafCooldownMu.Unlock()
	defer func() {
		WafRewriteRulesFile = origRulesFile
		globalWafRules.mu.Lock()
		globalWafRules.path = ""
		globalWafRules.rules = nil
		globalWafRules.mu.Unlock()
	}()

	altOffender := "Attach internal harness telemetry TRIGGER_UNKNOWN_HARNESS_WAF_ALT on cold start."
	mixedSys := strings.Join([]string{
		`Bundle location: "/Applications/DeepSeek Harness.app/Contents/MacOS/dsh" (WAF_IF_RAW_DSH_PATH).`,
		altOffender,
		"Use context-aware structured logging.",
	}, "\n")
	bisectBody, _ := json.Marshal(map[string]any{
		"model":      "gemini-3.8-flash",
		"max_tokens": 64,
		"system":     mixedSys,
		"messages":   []map[string]any{{"role": "user", "content": "Say hello"}},
	})
	reqBisect, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/messages", bytes.NewReader(bisectBody))
	reqBisect.Header.Set("Content-Type", "application/json")
	reqBisect.Header.Set("x-session-id", "rev-bisect-encoded-path")
	respBisect, err := http.DefaultClient.Do(reqBisect)
	if err != nil {
		t.Fatal(err)
	}
	rawBisect, _ := io.ReadAll(respBisect.Body)
	respBisect.Body.Close()
	if respBisect.StatusCode != 200 || !strings.Contains(string(rawBisect), "FAKE_OK") {
		t.Fatalf("expected bisect with encoded sensitive path sentence to self-heal and replay 200 OK, got status=%d body=%s", respBisect.StatusCode, string(rawBisect))
	}
	persisted, _ := os.ReadFile(tmpRules)
	if !strings.Contains(string(persisted), altOffender) || strings.Contains(string(persisted), "WAF_IF_RAW_DSH_PATH") || strings.Contains(string(persisted), "wsseg_") {
		t.Fatalf("expected persisted rule to contain ONLY %q (and no path sentence or wsseg_ token), got:\n%s", altOffender, string(persisted))
	}
}
