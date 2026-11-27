package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// R5-BUG-01: parked_mcp_resume must keep SystemTail (parkedToolNotice + tailDirective) out of
// irreversible Layer-A SanitizeOutboundText so Structured Output JSON Schemas and dynamic tool
// parameter schemas preserve vendor-named keys/enums while tr.Content still gets Layer-A cleaned.
func TestRound5Bug01ParkedResumePreservesStructuredOutputAndDynamicToolSchemas(t *testing.T) {
	baseURL, cleanup := startTestGateway(t)
	defer cleanup()

	convID := "r5-bug01-schema-preserve"
	turn1Tools := []map[string]any{
		{
			"type": "function",
			"function": map[string]any{
				"name":        "Bash",
				"description": "Run shell command",
				"parameters": map[string]any{
					"type":       "object",
					"properties": map[string]any{"command": map[string]any{"type": "string"}},
				},
			},
		},
	}
	turn1Msgs := []map[string]any{
		{"role": "user", "content": "[FAKE_TOOL_CALL] inspect vendor schema"},
	}
	b1, _ := json.Marshal(map[string]any{
		"model":    "gemini-3.8-flash",
		"tools":    turn1Tools,
		"messages": turn1Msgs,
	})
	req1, _ := http.NewRequest("POST", baseURL+"/v1/chat/completions", bytes.NewReader(b1))
	req1.Header.Set("Content-Type", "application/json")
	req1.Header.Set("x-session-id", convID)
	resp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatal(err)
	}
	var d1 map[string]any
	_ = json.NewDecoder(resp1.Body).Decode(&d1)
	resp1.Body.Close()

	choices1, _ := d1["choices"].([]any)
	if len(choices1) == 0 {
		t.Fatalf("turn 1 missing choices: %+v", d1)
	}
	msg1, _ := choices1[0].(map[string]any)["message"].(map[string]any)
	tcs1, _ := msg1["tool_calls"].([]any)
	if len(tcs1) == 0 {
		t.Fatalf("expected parked tool_calls on turn 1, got %+v", d1)
	}
	callID, _ := tcs1[0].(map[string]any)["id"].(string)
	if callID == "" {
		t.Fatalf("missing tool_call id: %+v", tcs1[0])
	}

	// Turn 2 (parked_mcp_resume): add dynamic tool with "anthropic_mode" property + Structured Output
	// json_schema containing "claude_vendor" and enum ["Claude", "Codex", "DSH"].
	turn2Tools := append(append([]map[string]any{}, turn1Tools...), map[string]any{
		"type": "function",
		"function": map[string]any{
			"name":        "DeployConfig",
			"description": "Deploy configuration",
			"parameters": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"anthropic_mode": map[string]any{
						"type": "string",
						"enum": []any{"Claude", "Codex", "Anthropic"},
					},
				},
				"required": []any{"anthropic_mode"},
			},
		},
	})
	turn2Msgs := []map[string]any{
		turn1Msgs[0],
		msg1,
		{
			"role":         "tool",
			"tool_call_id": callID,
			"content":      "<system-reminder>Claude Code internal reminder</system-reminder>\nProse from Claude tool output",
		},
	}
	b2, _ := json.Marshal(map[string]any{
		"model":    "gemini-3.8-flash",
		"tools":    turn2Tools,
		"messages": turn2Msgs,
		"response_format": map[string]any{
			"type": "json_schema",
			"json_schema": map[string]any{
				"name":   "vendor_report",
				"strict": true,
				"schema": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"claude_vendor": map[string]any{
							"type":        "string",
							"description": "Used by Claude Code CLI",
							"enum":        []any{"Claude", "Codex", "DSH"},
						},
					},
					"required": []any{"claude_vendor"},
				},
			},
		},
	})
	req2, _ := http.NewRequest("POST", baseURL+"/v1/chat/completions", bytes.NewReader(b2))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("x-session-id", convID)
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	var d2 map[string]any
	_ = json.NewDecoder(resp2.Body).Decode(&d2)
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("turn 2 failed with %d: %+v", resp2.StatusCode, d2)
	}
	choices2, _ := d2["choices"].([]any)
	reply2, _ := choices2[0].(map[string]any)["message"].(map[string]any)["content"].(string)
	if !strings.HasPrefix(reply2, "TOOL_DONE:") {
		t.Fatalf("expected TOOL_DONE reply from parked_mcp_resume, got %q", reply2)
	}

	// 1. Layer-A still cleaned tr.Content (stripped <system-reminder> and sanitized prose "Claude" -> "the assistant").
	if strings.Contains(reply2, "Claude Code internal reminder") || !strings.Contains(reply2, "Prose from the assistant tool output") {
		t.Fatalf("expected tr.Content Layer-A sanitization preserved, got %q", reply2)
	}
	// 2. Structured Output JSON Schema & dynamic [Host tool update] schema keys/enums were NOT corrupted by Layer-A!
	for _, mustKeep := range []string{`"claude_vendor"`, `"Claude"`, `"Codex"`, `"DSH"`, `"anthropic_mode"`, `"Anthropic"`} {
		if !strings.Contains(reply2, mustKeep) {
			t.Fatalf("schema token %s was corrupted during parked_mcp_resume: %q", mustKeep, reply2)
		}
	}
	if strings.Contains(reply2, "Used by Claude Code CLI") {
		t.Fatalf("expected schema description field to still be cleanroom-sanitized, got %q", reply2)
	}

	// 3. Direct unit check on formatParkedToolResultText for empty Content ("OK" vs "" fallback) + reversible codec.
	codec := NewReversibleSessionCodec("r5-bug01-unit")
	sysTail := `[Structured Output Requirement: JSON Schema: {"properties":{"claude_vendor":{"enum":["Claude","Codex","Anthropic"]}},"required":["claude_vendor"]}]`
	liveFormatted := formatParkedToolResultText(ToolResultInput{ToolCallID: "c1", Content: "", SystemTail: sysTail}, codec, "OK")
	if !strings.HasPrefix(liveFormatted, "OK\n\n[Structured Output Requirement:") {
		t.Fatalf("expected live empty tool result to prefix OK before SystemTail, got %q", liveFormatted)
	}
	if !strings.Contains(liveFormatted, `"claude_vendor"`) || !strings.Contains(liveFormatted, `"Claude"`) || !strings.Contains(liveFormatted, `"Codex"`) {
		t.Fatalf("formatParkedToolResultText corrupted vendor schema keys/enums: %q", liveFormatted)
	}
	if strings.Contains(liveFormatted, `"Anthropic"`) {
		t.Fatalf("expected sensitivePathPhraseRe word Anthropic encoded as wsseg_* outbound, got %q", liveFormatted)
	}
	if restored := codec.RestoreText(liveFormatted); !strings.Contains(restored, `"Anthropic"`) {
		t.Fatalf("expected reversible codec to restore Anthropic cleanly, got %q", restored)
	}
	newPromptFormatted := formatParkedToolResultText(ToolResultInput{ToolCallID: "c1", Content: "", SystemTail: sysTail}, codec, "")
	if strings.HasPrefix(newPromptFormatted, "OK") || !strings.HasPrefix(newPromptFormatted, "[Structured Output Requirement:") {
		t.Fatalf("expected emptyFallback=\"\" to omit OK prefix on runNewPromptResume path, got %q", newPromptFormatted)
	}
}

// R5-BUG-02: expireDetachedTurn must serialize through s.queueSem and both expireDetachedTurn &
// AbortDetachedTurn must hold s.IsBusy = true across settleDetachedTurn so a superseding turn can
// never race with CancelActiveTurn / HibernateWorker.
func TestRound5Bug02ExpireAndAbortDetachedTurnSerializeOnQueueSemAndBusyFlag(t *testing.T) {
	oldGrace, oldMin := PrefillCancelGrace, PrefillCancelMinElapsed
	PrefillCancelGrace = 10 * time.Second
	PrefillCancelMinElapsed = 0
	defer func() {
		PrefillCancelGrace = oldGrace
		PrefillCancelMinElapsed = oldMin
	}()

	s := NewAcpProjectSession("sid:r5-bug02", t.TempDir(), "")
	turn1 := newActiveTurnState(nil, func() {})
	turn1.createdAt = time.Now().Add(-500 * time.Millisecond)
	s.mu.Lock()
	s.SessionID = "00000000-0000-4000-8000-000000005002"
	s.MessageCount = 4
	s.activeTurn = turn1
	s.mu.Unlock()

	if !s.tryStartSoftDetach(turn1, "fp_r5_bug02_1", s.SessionID, "sticky_delta", "", time.Now().Add(-500*time.Millisecond), time.Now().Add(-500*time.Millisecond), 4, "u1", "h1", false) {
		t.Fatal("expected tryStartSoftDetach to succeed")
	}
	s.mu.Lock()
	det := s.detachedTurn
	s.mu.Unlock()
	if det == nil {
		t.Fatal("expected detachedTurn to be populated")
	}
	if det.graceTimer != nil {
		det.graceTimer.Stop()
	}

	beforeExp := softDetachExpiredTotal.Load()
	beforeSup := softDetachSupersededTotal.Load()

	// Hold queueSem (as gateway_pool.go:930 does before AbortDetachedTurn).
	if err := s.acquireTurnQueue(context.Background()); err != nil {
		t.Fatal(err)
	}

	expireDone := make(chan struct{})
	go func() {
		s.expireDetachedTurn(det)
		close(expireDone)
	}()

	// expireDetachedTurn MUST block on queueSem while caller holds it.
	select {
	case <-expireDone:
		s.releaseTurnQueue()
		t.Fatal("expireDetachedTurn bypassed queueSem while turn queue was held")
	case <-time.After(45 * time.Millisecond):
	}

	// AbortDetachedTurn runs under the caller's queueSem without deadlocking and clears s.detachedTurn.
	s.AbortDetachedTurn("superseded_by_new_request")
	if softDetachSupersededTotal.Load() != beforeSup+1 {
		s.releaseTurnQueue()
		t.Fatalf("expected softDetachSupersededTotal=%d, got %d", beforeSup+1, softDetachSupersededTotal.Load())
	}

	// Simulate R2 starting its new activeTurn before releasing queueSem (or right after).
	turn2 := newActiveTurnState(nil, func() {})
	s.mu.Lock()
	s.activeTurn = turn2
	s.IsBusy = true
	s.mu.Unlock()

	if s.IsAvailableForNewTurn() {
		s.releaseTurnQueue()
		t.Fatal("session must not report IsAvailableForNewTurn while IsBusy is true")
	}

	// Release queueSem: queued expireDetachedTurn wakes, sees s.detachedTurn != det, and exits as a no-op!
	s.releaseTurnQueue()
	select {
	case <-expireDone:
	case <-time.After(time.Second):
		t.Fatal("expireDetachedTurn did not finish after queueSem release")
	}

	if gotExp := softDetachExpiredTotal.Load(); gotExp != beforeExp {
		t.Fatalf("expected expireDetachedTurn to no-op after AbortDetachedTurn (exp=%d, want %d)", gotExp, beforeExp)
	}
	s.mu.Lock()
	stillTurn2 := s.activeTurn == turn2
	s.activeTurn = nil
	s.IsBusy = false
	s.mu.Unlock()
	if !stillTurn2 {
		t.Fatal("queued expireDetachedTurn clobbered subsequent activeTurn")
	}

	// Also verify unobstructed expireDetachedTurn acquires/releases queueSem and resets IsBusy = false.
	turn3 := newActiveTurnState(nil, func() {})
	turn3.createdAt = time.Now().Add(-500 * time.Millisecond)
	turn3.mu.Lock()
	turn3.resolved = true
	turn3.upstreamDone = true
	turn3.mu.Unlock()
	turn3.resolveChan <- TurnResult{StopReason: "end_turn", Text: "unclaimed"}
	s.mu.Lock()
	s.SessionID = "00000000-0000-4000-8000-000000005003"
	s.activeTurn = turn3
	s.mu.Unlock()
	if !s.tryStartSoftDetach(turn3, "fp_r5_bug02_2", s.SessionID, "sticky_delta", "", time.Now().Add(-500*time.Millisecond), time.Now().Add(-500*time.Millisecond), 4, "u1", "h2", false) {
		t.Fatal("expected second tryStartSoftDetach to succeed")
	}
	s.mu.Lock()
	det2 := s.detachedTurn
	s.mu.Unlock()
	if det2.graceTimer != nil {
		det2.graceTimer.Stop()
	}
	s.expireDetachedTurn(det2)
	if softDetachExpiredTotal.Load() != beforeExp+1 {
		t.Fatalf("expected softDetachExpiredTotal incremented to %d", beforeExp+1)
	}
	ctxQuick, cancelQuick := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelQuick()
	if err := s.acquireTurnQueue(ctxQuick); err != nil {
		t.Fatalf("expireDetachedTurn leaked queueSem: %v", err)
	}
	s.releaseTurnQueue()
	if !s.IsAvailableForNewTurn() {
		t.Fatal("expected session available for new turn after expireDetachedTurn settled")
	}
}

// R5-BUG-03: authorizeToolCallPermission must allow ws_await_result / dsh_await_result to collect
// an already-parked/relayed tool result even when the resuming turn sets tool_choice: "none" (or
// omits tools), while still blocking ws_call_tool, regular host tools, and built-in web tools.
func TestRound5Bug03AuthorizeToolCallPermissionAllowsAwaitResultUnderToolChoiceNone(t *testing.T) {
	cat := sanitizeToolsCatalog([]any{
		map[string]any{
			"name":        "Read",
			"description": "Read file",
			"input_schema": map[string]any{
				"type":       "object",
				"properties": map[string]any{"file_path": map[string]any{"type": "string"}},
			},
		},
	})
	s := NewAcpProjectSession("sid:r5-bug03", t.TempDir(), "")
	s.SessionID = "00000000-0000-4000-8000-000000005004"
	s.PromptInFlight = true
	s.activeTurn = newActiveTurnState(nil, func() {})
	s.InitialToolsCatalog = append([]ToolDef(nil), cat...)
	s.ToolsCatalog = append([]ToolDef(nil), cat...)
	s.DeclaredMcpTools = make(map[string]bool, len(cat))
	for _, td := range cat {
		s.DeclaredMcpTools[td.Name] = true
		s.McpToHostMap[td.McpName] = td.Name
	}

	awaitMcp := mcpToolName(AwaitToolName)
	callMcpName := mcpToolName(CallToolName)
	readMcp := mcpToolName("Read")

	resCh := make(chan map[string]any, 1)
	resCh <- map[string]any{"content": []map[string]any{{"type": "text", "text": "RELAYED_RESULT_OK"}}}
	s.RelayedCalls = map[string]*ParkedMcpCall{
		"acp_relayed_1": {
			Call:       ToolCallItem{ID: "acp_relayed_1", Name: "Read"},
			SessionID:  s.SessionID,
			ResultChan: resCh,
		},
	}
	s.activeTurnToolChoice = TurnToolChoice{Mode: "none"}

	// 1. ws_await_result allowed while RelayedCalls is non-empty under Mode == "none".
	if allow, why := s.authorizeToolCallPermission(map[string]any{
		"toolCallId": "perm-await-1",
		"name":       "mcp_" + McpBridgeName + "_" + awaitMcp,
	}); !allow || why != "ws_bridge_internal_await" {
		t.Fatalf("expected ws_await_result allowed under tool_choice=none with relayed call, got allow=%v why=%q", allow, why)
	}

	// 2. ws_call_tool, Read, and search_web MUST still be rejected with tool_choice_none!
	for _, blocked := range []map[string]any{
		{"toolCallId": "perm-call-1", "name": "mcp_" + McpBridgeName + "_" + callMcpName},
		{"toolCallId": "perm-read-1", "name": "mcp_" + McpBridgeName + "_" + readMcp},
		{"toolCallId": "perm-web-1", "name": "search_web"},
	} {
		if allow, why := s.authorizeToolCallPermission(blocked); allow || why != "tool_choice_none" {
			t.Fatalf("expected %v rejected with tool_choice_none, got allow=%v why=%q", blocked["name"], allow, why)
		}
	}

	// 3. Even if resuming client omitted tools (consumeToolCatalogLocked(nil)), ws_await_result stays mapped while RelayedCalls > 0.
	s.mu.Lock()
	_, _, _, _ = s.consumeToolCatalogLocked(nil)
	s.mu.Unlock()
	if allow, why := s.authorizeToolCallPermission(map[string]any{
		"toolCallId": "perm-await-empty-cat",
		"name":       "mcp_" + McpBridgeName + "_" + awaitMcp,
	}); !allow || why != "ws_bridge_internal_await" {
		t.Fatalf("expected ws_await_result still allowed when tools omitted with active RelayedCalls, got allow=%v why=%q", allow, why)
	}
	gotRes := s.executeMcpToolCall(context.Background(), AwaitToolName, map[string]any{"call_id": "acp_relayed_1"})
	if isErr, _ := gotRes["isError"].(bool); isErr {
		t.Fatalf("expected executeMcpToolCall(ws_await_result) to return relayed result, got %+v", gotRes)
	}

	// 4. Once RelayedCalls and ParkedCalls are both empty, ws_await_result is rejected under Mode == "none".
	s.mu.Lock()
	s.McpToHostMap[awaitMcp] = AwaitToolName
	s.mu.Unlock()
	if allow, why := s.authorizeToolCallPermission(map[string]any{
		"toolCallId": "perm-await-after-drain",
		"name":       "mcp_" + McpBridgeName + "_" + awaitMcp,
	}); allow || why != "tool_choice_none" {
		t.Fatalf("expected ws_await_result rejected once no parked/relayed calls remain, got allow=%v why=%q", allow, why)
	}

	// 5. Legacy dsh-bridge + dsh_await_result parity with ParkedCalls > 0.
	legAwait := fmt.Sprintf("dsh_%s_%s", LegacyAwaitToolName, shortSha256(LegacyAwaitToolName)[:12])
	sLeg := NewAcpProjectSession("sid:r5-bug03-leg", t.TempDir(), "")
	sLeg.PromptInFlight = true
	sLeg.McpToHostMap[legAwait] = LegacyAwaitToolName
	sLeg.ParkedCalls["acp_parked_leg"] = &ParkedMcpCall{Call: ToolCallItem{ID: "acp_parked_leg", Name: "Bash"}}
	sLeg.activeTurnToolChoice = TurnToolChoice{Mode: "none"}
	if allow, why := sLeg.authorizeToolCallPermission(map[string]any{
		"toolCallId": "perm-leg-await",
		"name":       "mcp_" + LegacyMcpBridgeName + "_" + legAwait,
	}); !allow || why != "ws_bridge_internal_await" {
		t.Fatalf("expected legacy dsh_await_result allowed under tool_choice=none, got allow=%v why=%q", allow, why)
	}
}

// R5-BUG-04: Switching canonical model on a parked_mcp_resume turn must roll over to a fresh
// session pinned to the newly requested model (Decision U8), whereas tier-only adjustments on the
// same canonical model stay warm on parked_mcp_resume.
func TestRound5Bug04ParkedResumeModelSwitchForcesCleanRollover(t *testing.T) {
	gs := NewGatewayServer()
	t.Cleanup(gs.Close)
	srv := httptest.NewServer(gs)
	defer func() {
		srv.Close()
		GlobalEngine.StopEngine()
	}()
	// Sync GatewayPort to httptest listener so MCP callbacks reach gs.
	var port int
	_, _ = fmt.Sscanf(strings.TrimPrefix(srv.URL, "http://127.0.0.1:"), "%d", &port)
	GatewayPort = port

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

	// Part A: Switch gemini-3.8-flash -> gemini-4-carbon on parked_mcp_resume -> rollover_model_switch!
	convSwitch := "r5-bug04-model-switch"
	msgs1 := []map[string]any{{"role": "user", "content": "[FAKE_TOOL_CALL] run tool on flash"}}
	b1, _ := json.Marshal(map[string]any{
		"model":      "gemini-3.8-flash",
		"max_tokens": 512,
		"tools":      tools,
		"messages":   msgs1,
	})
	r1, _ := http.NewRequest("POST", srv.URL+"/v1/messages", bytes.NewReader(b1))
	r1.Header.Set("Content-Type", "application/json")
	r1.Header.Set("x-claude-code-session-id", convSwitch)
	resp1, err := http.DefaultClient.Do(r1)
	if err != nil {
		t.Fatal(err)
	}
	var d1 map[string]any
	_ = json.NewDecoder(resp1.Body).Decode(&d1)
	resp1.Body.Close()
	if d1["stop_reason"] != "tool_use" {
		t.Fatalf("expected tool_use on turn 1, got %+v", d1)
	}
	c1, _ := d1["content"].([]any)
	var tuID string
	for _, blk := range c1 {
		if bm, ok := blk.(map[string]any); ok && bm["type"] == "tool_use" {
			tuID, _ = bm["id"].(string)
		}
	}
	var sessSwitch *AcpProjectSession
	gs.mu.RLock()
	for _, s := range gs.sessions {
		if strings.Contains(s.ConvKey, convSwitch) {
			sessSwitch = s
			break
		}
	}
	gs.mu.RUnlock()
	if sessSwitch == nil {
		t.Fatal("could not locate parked session for convSwitch")
	}
	sessSwitch.mu.Lock()
	oldSid := sessSwitch.SessionID
	oldModel := sessSwitch.ModelID
	sessSwitch.mu.Unlock()
	if oldSid == "" || oldModel != "gemini-3.8-flash" {
		t.Fatalf("expected parked session on gemini-3.8-flash, got sid=%q model=%q", oldSid, oldModel)
	}

	msgs2 := append(append([]map[string]any{}, msgs1...),
		map[string]any{"role": "assistant", "content": c1},
		map[string]any{
			"role": "user",
			"content": []any{
				map[string]any{"type": "tool_result", "tool_use_id": tuID, "content": "SWITCHED_TOOL_OUTPUT_777"},
				map[string]any{"type": "text", "text": "[FAKE_ZERO_50K] [ECHO_PROMPT] [ECHO_RELAY_META] summarize on carbon"},
			},
		},
	)
	b2, _ := json.Marshal(map[string]any{
		"model":      "gemini-4-carbon",
		"max_tokens": 512,
		"tools":      tools,
		"messages":   msgs2,
	})
	r2, _ := http.NewRequest("POST", srv.URL+"/v1/messages", bytes.NewReader(b2))
	r2.Header.Set("Content-Type", "application/json")
	r2.Header.Set("x-claude-code-session-id", convSwitch)
	resp2, err := http.DefaultClient.Do(r2)
	if err != nil {
		t.Fatal(err)
	}
	var d2 map[string]any
	_ = json.NewDecoder(resp2.Body).Decode(&d2)
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("turn 2 model switch failed (%d): %+v", resp2.StatusCode, d2)
	}
	if d2["model"] != "gemini-4-carbon" {
		t.Fatalf("expected response model=gemini-4-carbon, got %v", d2["model"])
	}
	var replyText string
	for _, blk := range d2["content"].([]any) {
		if bm, ok := blk.(map[string]any); ok && bm["type"] == "text" {
			replyText, _ = bm["text"].(string)
		}
	}
	// Verify fakeacp actually ran on gemini-4-carbon (NOT TOOL_DONE from old parked flash session!)
	// and replayed full history including tool_use + SWITCHED_TOOL_OUTPUT_777.
	if !strings.Contains(replyText, "FAKE_OK model=gemini-4-carbon") || strings.HasPrefix(replyText, "TOOL_DONE:") {
		t.Fatalf("expected fresh session pinned to gemini-4-carbon, got %q", replyText)
	}
	if !strings.Contains(replyText, "SWITCHED_TOOL_OUTPUT_777") {
		t.Fatalf("expected full history replay on model-switch rollover to include tool_result, got %q", replyText)
	}
	sessSwitch.mu.Lock()
	newSid := sessSwitch.SessionID
	rolls := sessSwitch.CompactionRollovers
	curModel := sessSwitch.ModelID
	var lastHist TurnHistoryEntry
	if len(sessSwitch.TurnHistory) > 0 {
		lastHist = sessSwitch.TurnHistory[len(sessSwitch.TurnHistory)-1]
	}
	sessSwitch.mu.Unlock()
	if newSid == "" || newSid == oldSid || rolls != 1 || curModel != "gemini-4-carbon" {
		t.Fatalf("unexpected post-rollover state: newSid=%q oldSid=%q rolls=%d model=%q", newSid, oldSid, rolls, curModel)
	}
	if lastHist.Mode != "compaction_rollover" || lastHist.CacheMiss != "rollover_model_switch" {
		t.Fatalf("expected mode=compaction_rollover & cacheMiss=rollover_model_switch, got mode=%q miss=%q", lastHist.Mode, lastHist.CacheMiss)
	}

	// Part B: Same canonical model tier tweak (gemini-4-carbon-low -> gemini-4-carbon-high) stays warm!
	convSame := "r5-bug04-same-model-tier"
	bSame1, _ := json.Marshal(map[string]any{
		"model":      "gemini-4-carbon-low",
		"max_tokens": 512,
		"tools":      tools,
		"messages":   []map[string]any{{"role": "user", "content": "[FAKE_TOOL_CALL] warm tier check"}},
	})
	rs1, _ := http.NewRequest("POST", srv.URL+"/v1/messages", bytes.NewReader(bSame1))
	rs1.Header.Set("Content-Type", "application/json")
	rs1.Header.Set("x-claude-code-session-id", convSame)
	respS1, err := http.DefaultClient.Do(rs1)
	if err != nil {
		t.Fatal(err)
	}
	var ds1 map[string]any
	_ = json.NewDecoder(respS1.Body).Decode(&ds1)
	respS1.Body.Close()
	cs1, _ := ds1["content"].([]any)
	var tuSame string
	for _, blk := range cs1 {
		if bm, ok := blk.(map[string]any); ok && bm["type"] == "tool_use" {
			tuSame, _ = bm["id"].(string)
		}
	}
	bSame2, _ := json.Marshal(map[string]any{
		"model":      "gemini-4-carbon-high",
		"max_tokens": 512,
		"tools":      tools,
		"messages": []map[string]any{
			{"role": "user", "content": "[FAKE_TOOL_CALL] warm tier check"},
			{"role": "assistant", "content": cs1},
			{"role": "user", "content": []any{map[string]any{"type": "tool_result", "tool_use_id": tuSame, "content": "WARM_TIER_OK"}}},
		},
	})
	rs2, _ := http.NewRequest("POST", srv.URL+"/v1/messages", bytes.NewReader(bSame2))
	rs2.Header.Set("Content-Type", "application/json")
	rs2.Header.Set("x-claude-code-session-id", convSame)
	respS2, err := http.DefaultClient.Do(rs2)
	if err != nil {
		t.Fatal(err)
	}
	var ds2 map[string]any
	_ = json.NewDecoder(respS2.Body).Decode(&ds2)
	respS2.Body.Close()
	var sameReply string
	for _, blk := range ds2["content"].([]any) {
		if bm, ok := blk.(map[string]any); ok && bm["type"] == "text" {
			sameReply, _ = bm["text"].(string)
		}
	}
	if !strings.HasPrefix(sameReply, "TOOL_DONE:WARM_TIER_OK") {
		t.Fatalf("expected warm parked_mcp_resume on same-model tier tweak, got %q", sameReply)
	}
}

// R5-BUG-06 + Cross-Round 1a: StateStore.buildSessionFromRowLocked and redactLogJSON must use
// unmarshalJSONPreserveNumbers so >53-bit integers and 0.0/1.0 float tokens survive SQLite restore,
// ReversibleSessionCodec tools/list encoding, diffToolCatalogs, and log redaction verbatim.
func TestRound5Bug06SQLiteRestoreAndLogRedactionPreserveJSONNumbers(t *testing.T) {
	prevDB := GatewayStateDB
	t.Cleanup(func() {
		initConfig()
		InitGlobalStore(prevDB)
	})
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "r5_bug06.db")
	t.Setenv("AGY_STATE_DB", dbPath)
	t.Setenv("AGY_PURE_API_HOME", filepath.Join(tmp, "pure"))
	t.Setenv("AGY_GW_LOG", filepath.Join(tmp, "gw.log"))
	initConfig()
	InitGlobalStore(dbPath)

	const rawToolsJSON = `[{
		"name": "QueryShard",
		"description": "Query /Applications/Anthropic.app shard table",
		"input_schema": {
			"type": "object",
			"properties": {
				"shard_id": {
					"type": "integer",
					"default": 9007199254740993,
					"enum": [9007199254740993, 9007199254740995]
				},
				"ratio": {
					"type": "number",
					"minimum": 0.0,
					"maximum": 1.0,
					"default": 0.0
				}
			},
			"required": ["shard_id"]
		}
	}]`
	var rawTools any
	if err := unmarshalJSONPreserveNumbers([]byte(rawToolsJSON), &rawTools); err != nil {
		t.Fatal(err)
	}
	origCat := sanitizeToolsCatalog(rawTools)
	origFp := toolCatalogFingerprint(origCat)

	s := NewAcpProjectSession("sid:r5-bug06:u:1", tmp, "")
	s.HibernatedSessionID = "00000000-0000-4000-8000-000000005006"
	s.MessageCount = 4
	s.InitialToolsCatalog = append([]ToolDef(nil), origCat...)
	s.ToolsCatalog = append([]ToolDef(nil), origCat...)
	s.ToolFingerprint = origFp
	s.DeclaredMcpTools = make(map[string]bool, len(origCat))
	for _, td := range origCat {
		s.DeclaredMcpTools[td.Name] = true
		s.McpToHostMap[td.McpName] = td.Name
	}
	s.Codec.SeedFromToolsCatalog(origCat)
	GlobalStore.UpsertSession(s)

	restored := GlobalStore.RestoreSingleSession(s.ConvKey)
	if restored == nil {
		t.Fatal("RestoreSingleSession returned nil")
	}
	if gotFp := toolCatalogFingerprint(restored.ToolsCatalog); gotFp != origFp {
		t.Fatalf("restored ToolCatalog fingerprint mismatch: got %s want %s", gotFp, origFp)
	}
	if gotInitFp := toolCatalogFingerprint(restored.InitialToolsCatalog); gotInitFp != origFp {
		t.Fatalf("restored InitialToolsCatalog fingerprint mismatch: got %s want %s", gotInitFp, origFp)
	}
	added, removed, changed := diffToolCatalogs(restored.ToolsCatalog, origCat)
	if len(added) != 0 || len(removed) != 0 || len(changed) != 0 {
		t.Fatalf("expected zero spurious tool catalog diff after SQLite restore, got added=%v removed=%v changed=%v", added, removed, changed)
	}

	// Verify MCP tools/list on the restored session emits exact 9007199254740993 & 0.0 alongside ReversibleSessionCodec!
	mcpReqBody := `{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}`
	mcpReq := httptest.NewRequest("POST", "http://127.0.0.1:18890/mcp/"+restored.TenantKey, strings.NewReader(mcpReqBody))
	mcpReq.RemoteAddr = "127.0.0.1:54321"
	mcpReq.Header.Set("Authorization", "Bearer "+restored.TenantKey)
	rec := httptest.NewRecorder()
	restored.HandleMcpHTTP(rec, mcpReq)
	mcpOut := rec.Body.String()
	if rec.Code != http.StatusOK {
		t.Fatalf("MCP tools/list failed (%d): %s", rec.Code, mcpOut)
	}
	for _, wantTok := range []string{"9007199254740993", "9007199254740995", `"minimum":0.0`, `"maximum":1.0`, "wsseg_"} {
		if !strings.Contains(mcpOut, wantTok) {
			t.Fatalf("MCP tools/list output missing %s: %s", wantTok, mcpOut)
		}
	}
	if strings.Contains(mcpOut, "9007199254740992") || strings.Contains(mcpOut, "Anthropic") {
		t.Fatalf("MCP tools/list rounded >53-bit int or leaked raw vendor path: %s", mcpOut)
	}

	// Verify redactLogJSON preserves >53-bit json.Number while redacting secrets.
	redacted, err := redactLogJSON(map[string]any{
		"api_key":  "sk-super-secret",
		"trace_id": json.Number("9007199254740993"),
		"ratio":    json.Number("0.0"),
	})
	if err != nil {
		t.Fatal(err)
	}
	redStr := string(redacted)
	if !strings.Contains(redStr, `"[REDACTED]"`) || strings.Contains(redStr, "sk-super-secret") {
		t.Fatalf("redactLogJSON failed to redact api_key: %s", redStr)
	}
	if !strings.Contains(redStr, "9007199254740993") || strings.Contains(redStr, "9007199254740992") {
		t.Fatalf("redactLogJSON lost >53-bit integer precision: %s", redStr)
	}
}

// R5-BUG-07: (1) A hibernated session entering RunTurn with a known rollover condition must skip
// session/resume and go straight to openFreshSession; (2) reloadHibernatedSession failing with
// *ModelSwitchError must preserve HibernatedSessionID & MessageCount without falling back to session/new.
func TestRound5Bug07HibernatedRolloverSkipsResumeAndModelSwitchErrorPreservesHibernatedState(t *testing.T) {
	prevDB := GatewayStateDB
	t.Cleanup(func() {
		GlobalEngine.StopEngine()
		initConfig()
		InitGlobalStore(prevDB)
	})
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "r5_bug07.db")
	logPath := filepath.Join(tmp, "r5_bug07.log")
	t.Setenv("AGY_STATE_DB", dbPath)
	t.Setenv("AGY_PURE_API_HOME", filepath.Join(tmp, "pure"))
	t.Setenv("AGY_GW_LOG", logPath)
	initConfig()
	InitGlobalStore(dbPath)
	GlobalEngine.StopEngine()

	gs := NewGatewayServer()
	t.Cleanup(gs.Close)
	srv := httptest.NewServer(gs)
	defer srv.Close()
	var port int
	_, _ = fmt.Sscanf(strings.TrimPrefix(srv.URL, "http://127.0.0.1:"), "%d", &port)
	GatewayPort = port

	hibTools := []map[string]any{
		{
			"name":        "Read",
			"description": "Read file",
			"input_schema": map[string]any{
				"type":       "object",
				"properties": map[string]any{"path": map[string]any{"type": "string"}},
			},
		},
	}
	postAnth := func(convID, model string, msgs []map[string]any, compacted bool) (int, map[string]any) {
		b, _ := json.Marshal(map[string]any{
			"model":      model,
			"max_tokens": 256,
			"system":     "Primary working directory: /tmp",
			"tools":      hibTools,
			"messages":   msgs,
		})
		req, _ := http.NewRequest("POST", srv.URL+"/v1/messages", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("x-claude-code-session-id", convID)
		if compacted {
			req.Header.Set("x-claude-code-context-compacted", "1")
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}

	// --- Part A: Hibernated session with compacted messages skips session/resume ---
	convA := "r5-bug07-hib-rollover"
	msgA1 := map[string]any{"role": "user", "content": "turn 1 hello [ECHO_RELAY_META]"}
	st1, d1 := postAnth(convA, "gemini-4-carbon", []map[string]any{msgA1}, false)
	if st1 != 200 {
		t.Fatalf("Part A turn 1 status=%d body=%+v", st1, d1)
	}
	st2, d2 := postAnth(convA, "gemini-4-carbon", []map[string]any{
		msgA1,
		{"role": "assistant", "content": d1["content"]},
		{"role": "user", "content": "turn 2 follow up"},
	}, false)
	if st2 != 200 {
		t.Fatalf("Part A turn 2 status=%d body=%+v", st2, d2)
	}

	var sessA *AcpProjectSession
	gs.mu.RLock()
	for _, s := range gs.sessions {
		if strings.Contains(s.ConvKey, convA) {
			sessA = s
			break
		}
	}
	gs.mu.RUnlock()
	if sessA == nil {
		t.Fatal("missing sessA")
	}
	sessA.mu.Lock()
	hibSidA := sessA.SessionID
	msgCntA := sessA.MessageCount
	sessA.mu.Unlock()
	if hibSidA == "" || msgCntA != 4 {
		t.Fatalf("expected live sessA with MessageCount=4, got sid=%q msgCnt=%d", hibSidA, msgCntA)
	}
	if !sessA.HibernateWorker("idle_test", false) {
		t.Fatal("expected sessA HibernateWorker to succeed")
	}

	logBeforeA, _ := os.ReadFile(logPath)
	st3, d3 := postAnth(convA, "gemini-4-carbon", []map[string]any{msgA1}, false)
	if st3 != 200 {
		t.Fatalf("Part A turn 3 status=%d body=%+v", st3, d3)
	}
	logAfterA, _ := os.ReadFile(logPath)
	deltaLogA := string(logAfterA[len(logBeforeA):])
	if strings.Contains(deltaLogA, "[SESSION_RESUME]") || strings.Contains(deltaLogA, "[SESSION_LOAD]") {
		t.Fatalf("hibernated session with known rollover wasted a session/resume spawn:\n%s", deltaLogA)
	}
	if strings.Count(deltaLogA, "[SESSION_NEW]") != 1 || !strings.Contains(deltaLogA, "[COMPACTION_ROLLOVER]") || !strings.Contains(deltaLogA, `"oldSessionId":"`+hibSidA+`"`) {
		t.Fatalf("expected 1 [SESSION_NEW] and [COMPACTION_ROLLOVER] referencing %s, got:\n%s", hibSidA, deltaLogA)
	}
	sessA.mu.Lock()
	rollsA := sessA.CompactionRollovers
	lastModeA := sessA.TurnHistory[len(sessA.TurnHistory)-1].Mode
	sessA.mu.Unlock()
	if rollsA != 1 || lastModeA != "compaction_rollover" {
		t.Fatalf("expected CompactionRollovers=1 & mode=compaction_rollover, got rolls=%d mode=%q", rollsA, lastModeA)
	}

	// --- Part B: Hibernated session hitting *ModelSwitchError preserves HibernatedSessionID & MessageCount ---
	t.Setenv("FAKE_ACP_REJECT_MODEL", "claude-opus-5-5")
	GlobalEngine.StopEngine()

	convB := "r5-bug07-msw-preserve"
	sb1, db1 := postAnth(convB, "gemini-4-carbon", []map[string]any{{"role": "user", "content": "b1"}}, false)
	if sb1 != 200 {
		t.Fatalf("Part B turn 1 failed: %d %+v", sb1, db1)
	}
	msgsB2 := []map[string]any{
		{"role": "user", "content": "b1"},
		{"role": "assistant", "content": db1["content"]},
		{"role": "user", "content": "b2"},
	}
	sb2, db2 := postAnth(convB, "gemini-4-carbon", msgsB2, false)
	if sb2 != 200 {
		t.Fatalf("Part B turn 2 failed: %d %+v", sb2, db2)
	}

	var sessB *AcpProjectSession
	gs.mu.RLock()
	for _, s := range gs.sessions {
		if strings.Contains(s.ConvKey, convB) {
			sessB = s
			break
		}
	}
	gs.mu.RUnlock()
	if sessB == nil {
		t.Fatal("missing sessB")
	}
	sessB.mu.Lock()
	hibSidB := sessB.SessionID
	sessB.mu.Unlock()
	if !sessB.HibernateWorker("idle_test", false) {
		t.Fatal("expected sessB HibernateWorker to succeed")
	}

	msgsB3 := append(append([]map[string]any{}, msgsB2...),
		map[string]any{"role": "assistant", "content": db2["content"]},
		map[string]any{"role": "user", "content": "b3 [ECHO_RELAY_META]"},
	)
	logBeforeB, _ := os.ReadFile(logPath)
	sb3, db3 := postAnth(convB, "claude-opus-5-5", msgsB3, false)
	if sb3 != http.StatusBadGateway {
		t.Fatalf("expected 502 on rejected model switch during wake, got %d (%+v)", sb3, db3)
	}
	logAfterB, _ := os.ReadFile(logPath)
	deltaLogB := string(logAfterB[len(logBeforeB):])
	if strings.Contains(deltaLogB, "[SESSION_NEW]") {
		t.Fatalf("*ModelSwitchError during wake must NOT fall back to openFreshSession:\n%s", deltaLogB)
	}
	sessB.mu.Lock()
	gotHibB := sessB.HibernatedSessionID
	gotMsgCntB := sessB.MessageCount
	gotModelB := sessB.ModelID
	sessB.mu.Unlock()
	if gotHibB != hibSidB || gotMsgCntB != 4 || gotModelB != "gemini-4-carbon" {
		t.Fatalf("hibernated session state wiped on *ModelSwitchError: hib=%q (want %q) msgCnt=%d (want 4) model=%q", gotHibB, hibSidB, gotMsgCntB, gotModelB)
	}

	// Subsequent valid request on gemini-4-carbon resumes hibSidB cleanly!
	sb4, db4 := postAnth(convB, "gemini-4-carbon", msgsB3, false)
	if sb4 != 200 {
		t.Fatalf("Part B recovery turn 4 failed: %d %+v", sb4, db4)
	}
	var textB4 string
	for _, blk := range db4["content"].([]any) {
		if bm, ok := blk.(map[string]any); ok && bm["type"] == "text" {
			textB4, _ = bm["text"].(string)
		}
	}
	if !strings.Contains(textB4, "sid="+hibSidB) || !strings.Contains(textB4, "turn=3") || !strings.Contains(textB4, "resumes=2") {
		t.Fatalf("expected clean resume of %s after transient *ModelSwitchError, got %q", hibSidB, textB4)
	}
	sessB.mu.Lock()
	lastModeB := sessB.TurnHistory[len(sessB.TurnHistory)-1].Mode
	sessB.mu.Unlock()
	if lastModeB != "resumed_delta" {
		t.Fatalf("expected mode=resumed_delta on recovery turn, got %q", lastModeB)
	}
}

// R5-BUG-08: POST /v1/responses (stream: true) must emit response.created (seq 0) and
// response.in_progress (seq 1) before response.failed (seq 2) when upstream fails AFTER a `: ping`
// heartbeat has already committed HTTP 200 SSE headers, while still returning JSON 4xx/5xx pre-commit.
func TestRound5Bug08ResponsesStreamingEmitsCreatedBeforeFailedAfterHeartbeatPing(t *testing.T) {
	origBeat := sseHeartbeatInterval
	sseHeartbeatInterval = 25 * time.Millisecond
	defer func() { sseHeartbeatInterval = origBeat }()

	baseURL, cleanup := startTestGateway(t)
	defer cleanup()

	// 1. Post-ping error: [FAKE_TTFT_300MS] lets 25ms `: ping` commit SSE headers before [FAKE_SOFT_429].
	reqBody, _ := json.Marshal(map[string]any{
		"model":  "gemini-3.8-flash",
		"stream": true,
		"input":  "[FAKE_TTFT_300MS] [FAKE_SOFT_429] slow upstream failure",
	})
	resp, err := http.Post(baseURL+"/v1/responses", "application/json", bytes.NewReader(reqBody))
	if err != nil {
		t.Fatal(err)
	}
	rawBytes, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	rawSSE := string(rawBytes)

	if resp.StatusCode != http.StatusOK || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("expected HTTP 200 text/event-stream after heartbeat ping, got %d %q:\n%s", resp.StatusCode, resp.Header.Get("Content-Type"), rawSSE)
	}
	if !strings.Contains(rawSSE, ": ping") {
		t.Fatalf("expected SSE heartbeat `: ping` before failure, got:\n%s", rawSSE)
	}

	var evTypes []string
	var seqNums []int
	for _, block := range strings.Split(rawSSE, "\n\n") {
		var evName, dataLine string
		for _, line := range strings.Split(strings.TrimSpace(block), "\n") {
			if strings.HasPrefix(line, "event: ") {
				evName = strings.TrimPrefix(line, "event: ")
			} else if strings.HasPrefix(line, "data: ") {
				dataLine = strings.TrimPrefix(line, "data: ")
			}
		}
		if evName == "" || dataLine == "" {
			continue
		}
		var payload struct {
			Type           string `json:"type"`
			SequenceNumber int    `json:"sequence_number"`
			Response       struct {
				Status string `json:"status"`
			} `json:"response"`
		}
		if err := json.Unmarshal([]byte(dataLine), &payload); err != nil {
			t.Fatalf("unmarshal SSE event %q failed: %v (%s)", evName, err, dataLine)
		}
		evTypes = append(evTypes, evName)
		seqNums = append(seqNums, payload.SequenceNumber)
		if evName == "response.failed" && payload.Response.Status != "failed" {
			t.Fatalf("expected response.failed status=failed, got %q", payload.Response.Status)
		}
	}
	wantEvents := []string{"response.created", "response.in_progress", "response.failed"}
	if len(evTypes) != len(wantEvents) {
		t.Fatalf("expected SSE events %v, got %v\nraw:\n%s", wantEvents, evTypes, rawSSE)
	}
	for i, w := range wantEvents {
		if evTypes[i] != w || seqNums[i] != i {
			t.Fatalf("event[%d] mismatch: got (%s, seq=%d), want (%s, seq=%d)", i, evTypes[i], seqNums[i], w, i)
		}
	}

	// 2. Pre-ping error: with normal 15s heartbeat, instant [FAKE_RPC_429] returns HTTP 429 JSON.
	sseHeartbeatInterval = 15 * time.Second
	fastErrBody, _ := json.Marshal(map[string]any{
		"model":  "gemini-3.8-flash",
		"stream": true,
		"input":  "[FAKE_RPC_429] instant error",
	})
	respFast, err := http.Post(baseURL+"/v1/responses", "application/json", bytes.NewReader(fastErrBody))
	if err != nil {
		t.Fatal(err)
	}
	respFast.Body.Close()
	if respFast.StatusCode != http.StatusTooManyRequests || !strings.HasPrefix(respFast.Header.Get("Content-Type"), "application/json") {
		t.Fatalf("expected pre-commit 429 application/json, got %d %q", respFast.StatusCode, respFast.Header.Get("Content-Type"))
	}
}

// Cross-Round Hardening (Hunt 1c): ARCH-OPT-03 8-day cold/active split + BUG-TEL-03 steady-state
// annotation + BUG-TEL-04 ServedVariantCounts across BuildDashboardPayload windows["allTime"] & ["24h"].
func TestRound5CrossRoundAllTimeRollupPreservesSteadyAndServedVariants(t *testing.T) {
	prevDB := GatewayStateDB
	t.Cleanup(func() {
		initConfig()
		InitGlobalStore(prevDB)
	})
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "r5_rollup.db")
	t.Setenv("AGY_STATE_DB", dbPath)
	t.Setenv("AGY_PURE_API_HOME", filepath.Join(tmp, "pure"))
	t.Setenv("AGY_GW_LOG", filepath.Join(tmp, "gw.log"))
	initConfig()
	InitGlobalStore(dbPath)

	sid := "aaaaaaaa-bbbb-4000-8000-000000000509"
	s := NewAcpProjectSession("sid:r5-rollup:u:1", tmp, "")
	s.SessionID = sid
	s.MessageCount = 6
	GlobalStore.UpsertSession(s)

	now := time.Now().UTC()
	insertStep := func(idx int, ts time.Time, modelID, modelLabel, fam, qg, tier, served string, rawIn, rawCr, outTok int, costUsd float64) {
		GlobalStore.mu.Lock()
		_, err := GlobalStore.db.Exec(`
			INSERT OR REPLACE INTO usage_steps (
				session_id, step_idx, source_cat, ts_sec, ttft_ms, stream_ms, elapsed_ms, tps, decode_tps,
				model_enum, model_id, model_label, model_family, model_tier, quota_group,
				raw_in, raw_cr, aligned_in, aligned_cr, out_tok, vis_out_tok, th_tok,
				has_thinking, cw_tok, tier_mult, in_cost_usd, cr_cost_usd, out_cost_usd,
				th_cost_usd, cost_usd, base1x_cost_usd, nocache_cost_usd, saved_usd, is_dsh_bridge, served_model
			) VALUES (?, ?, 'CLAUDE_GW', ?, 400, 200, 600, 50.0, 50.0, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 1.0, ?, 0.0, 0.0, 0.0, ?, ?, ?, 0.0, 1, ?)`,
			sid, idx, ts.Unix(), modelID, modelLabel, fam, tier, qg,
			rawIn, rawCr, rawIn, rawCr, outTok, outTok, costUsd, costUsd, costUsd, costUsd, served,
		)
		GlobalStore.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	}

	// Cold historical steps (< now - 8d):
	// Step 1: cold start on gemini-4-carbon (not steady)
	insertStep(1, now.Add(-12*24*time.Hour), "gemini-4-carbon", "Gemini 4 Carbon", "gemini", "gemini", "medium", "gemini-4-carbon", 20000, 0, 100, 0.04)
	// Step 2: warm hit on gemini-4-carbon with served variant mismatch #1 (steady + hit)
	insertStep(2, now.Add(-11*24*time.Hour), "gemini-4-carbon", "Gemini 4 Carbon", "gemini", "gemini", "medium", "gemini-4-carbon-exp", 2000, 22000, 120, 0.01)
	// Step 3: warm miss on gemini-4-carbon (steady + miss -> MissResendUsd > 0)
	insertStep(3, now.Add(-10*24*time.Hour), "gemini-4-carbon", "Gemini 4 Carbon", "gemini", "gemini", "medium", "gemini-4-carbon", 25000, 0, 110, 0.05)

	// Active recent steps (>= now - 24h) continuing the SAME session across the 8-day split boundary:
	// Step 4: warm hit on gemini-4-carbon with served variant mismatch #2 (steady across 8d boundary + hit)
	insertStep(4, now.Add(-2*time.Hour), "gemini-4-carbon", "Gemini 4 Carbon", "gemini", "gemini", "medium", "gemini-4-carbon-v2", 1500, 30000, 90, 0.01)
	// Step 5: in-session model_switch to gemini-3.8-flash (excluded from steady denominator per BUG-TEL-03!)
	insertStep(5, now.Add(-90*time.Minute), "gemini-3.8-flash", "Gemini 3.8 Flash", "gemini", "gemini", "medium", "gemini-3.8-flash", 32000, 0, 80, 0.02)
	// Step 6: warm hit on gemini-3.8-flash (steady + hit)
	insertStep(6, now.Add(-60*time.Minute), "gemini-3.8-flash", "Gemini 3.8 Flash", "gemini", "gemini", "medium", "gemini-3.8-flash", 1000, 33000, 85, 0.005)

	// Re-initialize GlobalStore on dbPath so coldSteps (< now-8d) and activeSteps (>= now-8d) are split cleanly in memory.
	InitGlobalStore(dbPath)

	gs := NewGatewayServer()
	defer gs.Close()
	payload := BuildDashboardPayload(gs, false)
	windows, ok := payload["windows"].(map[string]WindowAggregate)
	if !ok {
		t.Fatalf("unexpected windows type: %T", payload["windows"])
	}

	findModel := func(w WindowAggregate, modelID string) *ModelAggregate {
		for i := range w.Models {
			if w.Models[i].ModelID == modelID {
				return &w.Models[i]
			}
		}
		return nil
	}

	allTime := windows["allTime"]
	if allTime.StepsCount != 6 {
		t.Fatalf("expected allTime StepsCount=6, got %d", allTime.StepsCount)
	}
	mCarbonAll := findModel(allTime, "gemini-4-carbon")
	if mCarbonAll == nil {
		t.Fatal("missing gemini-4-carbon in allTime")
	}
	if mCarbonAll.Steps != 4 || mCarbonAll.SteadySteps != 3 || mCarbonAll.SteadyHits != 2 || mCarbonAll.SteadyHitRate != 66.7 {
		t.Fatalf("unexpected allTime carbon steady metrics: steps=%d steady=%d hits=%d rate=%.1f",
			mCarbonAll.Steps, mCarbonAll.SteadySteps, mCarbonAll.SteadyHits, mCarbonAll.SteadyHitRate)
	}
	if mCarbonAll.MissResendUsd <= 0 {
		t.Fatalf("expected positive MissResendUsd on allTime carbon from Step 3 miss, got %f", mCarbonAll.MissResendUsd)
	}
	if mCarbonAll.ServedMismatch != 2 || mCarbonAll.ServedVariantCounts["gemini-4-carbon-exp"] != 1 || mCarbonAll.ServedVariantCounts["gemini-4-carbon-v2"] != 1 {
		t.Fatalf("unexpected allTime ServedVariantCounts: mismatch=%d counts=%+v", mCarbonAll.ServedMismatch, mCarbonAll.ServedVariantCounts)
	}

	mFlashAll := findModel(allTime, "gemini-3.8-flash")
	if mFlashAll == nil || mFlashAll.Steps != 2 || mFlashAll.SteadySteps != 1 || mFlashAll.SteadyHits != 1 || mFlashAll.SteadyHitRate != 100.0 {
		t.Fatalf("expected model_switch Step 5 excluded from flash steady denominator: %+v", mFlashAll)
	}

	// Verify rolling5h window recognizes Step 4 as steady because annotateSteadySteps ran before window slicing!
	w5h := windows["rolling5h"]
	mCarbon5h := findModel(w5h, "gemini-4-carbon")
	if mCarbon5h == nil || mCarbon5h.Steps != 1 || mCarbon5h.SteadySteps != 1 || mCarbon5h.SteadyHits != 1 {
		t.Fatalf("expected rolling5h carbon Step 4 annotated steady across 8d boundary, got %+v", mCarbon5h)
	}
}

// Cross-Round Hardening (Hunts 1b & 1d): turnClaimed / queueSem release across error/cancel/panic
// paths, and multi-turn tool_choice transitions (auto -> tool -> none -> required -> auto) keeping
// InitialToolsCatalog and McpToHostMap intact without busting prefix caches.
func TestRound5CrossRoundTurnClaimedAndMultiTurnToolChoiceTransitions(t *testing.T) {
	baseURL, cleanup := startTestGateway(t)
	defer cleanup()

	// Part 1: Verify turnClaimed and queueSem never leak on Stage-2 SSRF error, cancelled ctx, or recovered panic.
	gs := NewGatewayServer()
	defer gs.Close()

	// 1a. Stage-2 loopback image SSRF rejection through HandleMessagesRequest.
	ssrfMsgs := []map[string]any{
		{"role": "user", "content": []any{map[string]any{"type": "image_ref", "url": "http://127.0.0.1/ssrf.png"}}},
	}
	ssrfBody := map[string]any{"model": "gemini-3.8-flash", "system": "Primary working directory: /tmp"}
	ssrfReq := httptest.NewRequest("POST", "http://127.0.0.1:18890/v1/messages", nil)
	ssrfReq.Header.Set("x-session-id", "r5-claim-ssrf")
	if _, err := gs.HandleMessagesRequest(ssrfReq, ssrfBody, ssrfMsgs, nil); err == nil {
		t.Fatal("expected SSRF error from HandleMessagesRequest")
	}
	gs.mu.RLock()
	var ssrfSess *AcpProjectSession
	for _, s := range gs.sessions {
		if strings.Contains(s.ConvKey, "r5-claim-ssrf") {
			ssrfSess = s
		}
	}
	gs.mu.RUnlock()
	if ssrfSess == nil || !ssrfSess.IsAvailableForNewTurn() {
		t.Fatalf("expected ssrfSess available with 0 leaked turn claims: %+v", ssrfSess)
	}

	// 1b. Pre-cancelled context before acquireTurnQueue while another caller holds queueSem.
	if err := ssrfSess.acquireTurnQueue(context.Background()); err != nil {
		t.Fatal(err)
	}
	cancCtx, cancel := context.WithCancel(context.Background())
	cancel()
	cancReq := httptest.NewRequest("POST", "http://127.0.0.1:18890/v1/messages", nil).WithContext(cancCtx)
	cancReq.Header.Set("x-session-id", "r5-claim-ssrf")
	_, cancErr := gs.HandleMessagesRequest(cancReq, ssrfBody, []map[string]any{{"role": "user", "content": "hi"}}, nil)
	ssrfSess.releaseTurnQueue()
	if cancErr == nil || !ssrfSess.IsAvailableForNewTurn() {
		t.Fatalf("expected cancelled request to release turnClaimed cleanly (err=%v avail=%v)", cancErr, ssrfSess.IsAvailableForNewTurn())
	}

	// 1c. Panic inside a claimed+queued critical section releases both queueSem and turnClaimed via defers.
	func() {
		defer func() { _ = recover() }()
		claimed := gs.getOrCreateClaimedSession("sid:r5-panic", t.TempDir(), "", "fp1")
		defer claimed.releaseTurnClaim()
		if err := claimed.acquireTurnQueue(context.Background()); err != nil {
			t.Fatal(err)
		}
		defer claimed.releaseTurnQueue()
		panic("simulated turn panic")
	}()
	gs.mu.RLock()
	panicSess := gs.sessions["sid:r5-panic"]
	gs.mu.RUnlock()
	if panicSess == nil || !panicSess.IsAvailableForNewTurn() {
		t.Fatal("expected panicSess available after deferred releaseTurnQueue + releaseTurnClaim")
	}

	// Part 2: Multi-turn tool_choice transitions across 4 turns on one sticky conversation.
	convTC := "r5-multiturn-toolchoice"
	oaiTools := []map[string]any{
		{
			"type": "function",
			"function": map[string]any{
				"name":        "Read",
				"description": "Read file",
				"parameters":  map[string]any{"type": "object", "properties": map[string]any{"path": map[string]any{"type": "string"}}},
			},
		},
		{
			"type": "function",
			"function": map[string]any{
				"name":        "Bash",
				"description": "Run command",
				"parameters":  map[string]any{"type": "object", "properties": map[string]any{"cmd": map[string]any{"type": "string"}}},
			},
		},
	}
	steps := []struct {
		toolChoice any
		wantSubstr string
	}{
		{"auto", ""},
		{map[string]any{"type": "function", "function": map[string]any{"name": "Read"}}, "tool_choice forces tool `Read`"},
		{"none", "tool_choice=none"},
		{"required", "tool_choice=required"},
		{"auto", ""},
	}

	var history []map[string]any
	sidRe := regexp.MustCompile(`sid=(00000000-0000-4000-8000-[0-9]{12})`)
	var stickySid string
	for idx, st := range steps {
		history = append(history, map[string]any{
			"role":    "user",
			"content": fmt.Sprintf("step %d [ECHO_PROMPT] [ECHO_TOOLS] [ECHO_RELAY_META]", idx+1),
		})
		b, _ := json.Marshal(map[string]any{
			"model":       "gemini-3.8-flash",
			"tools":       oaiTools,
			"tool_choice": st.toolChoice,
			"messages":    history,
		})
		req, _ := http.NewRequest("POST", baseURL+"/v1/chat/completions", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("x-session-id", convTC)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		var d map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&d)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("step %d failed (%d): %+v", idx+1, resp.StatusCode, d)
		}
		reply, _ := d["choices"].([]any)[0].(map[string]any)["message"].(map[string]any)["content"].(string)
		m := sidRe.FindStringSubmatch(reply)
		if len(m) != 2 {
			t.Fatalf("step %d missing sid in reply: %q", idx+1, reply)
		}
		if stickySid == "" {
			stickySid = m[1]
		} else if m[1] != stickySid {
			t.Fatalf("step %d busted session continuity: got sid=%s want %s", idx+1, m[1], stickySid)
		}
		if !strings.Contains(reply, "ws_Read_") || !strings.Contains(reply, "ws_Bash_") {
			t.Fatalf("step %d lost MCP tools across tool_choice transition: %q", idx+1, reply)
		}
		if st.wantSubstr != "" && !strings.Contains(reply, st.wantSubstr) {
			t.Fatalf("step %d missing expected directive %q in prompt: %q", idx+1, st.wantSubstr, reply)
		}
		history = append(history, map[string]any{"role": "assistant", "content": reply})
	}
}
