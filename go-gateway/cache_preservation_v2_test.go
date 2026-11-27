package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestHeaderlessClientSystemDriftPreservesSessionAndParkedTools(t *testing.T) {
	tmp := t.TempDir()
	t.Cleanup(func() { GlobalEngine.StopEngine(); initConfig() })
	t.Setenv("AGY_ACP_BIN", fakeAcpBin)
	t.Setenv("AGY_PURE_API_HOME", filepath.Join(tmp, "pure"))
	t.Setenv("AGY_CONV_DIR", filepath.Join(tmp, "convs"))
	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "gateway.db"))
	t.Setenv("AGY_GW_LOG", filepath.Join(tmp, "gateway.log"))
	t.Setenv("AGY_ACP_RECORD", filepath.Join(tmp, "record"))
	t.Setenv("AGY_TOS_TRIPPED_FILE", filepath.Join(tmp, "tos-tripped"))
	initConfig()
	GlobalEngine.StopEngine()

	baseURL, gs := startCarbonTestGateway(t)

	tools2 := []map[string]any{
		{
			"name":         "Bash",
			"description":  "Run shell command",
			"input_schema": map[string]any{"type": "object", "properties": map[string]any{"command": map[string]any{"type": "string"}}},
		},
		{
			"name":         "Read",
			"description":  "Read file",
			"input_schema": map[string]any{"type": "object", "properties": map[string]any{"path": map[string]any{"type": "string"}}},
		},
	}
	tools3 := append(append([]map[string]any(nil), tools2...), map[string]any{
		"name":         "SearchCode",
		"description":  "Search repository symbols",
		"input_schema": map[string]any{"type": "object", "properties": map[string]any{"query": map[string]any{"type": "string"}}},
	})

	sysT1 := "Primary working directory: " + tmp + "\ncurrentDate: 2026-11-18T09:00:00Z\ngitStatus: clean"
	u1 := map[string]any{"role": "user", "content": "[FAKE_TOOL_CALL] inspect workspace files"}

	resp1, raw1, err1 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "", "", map[string]any{
		"model":      "gemini-3.8-flash-low",
		"max_tokens": 512,
		"system":     sysT1,
		"tools":      tools2,
		"messages":   []map[string]any{u1},
	})
	if err1 != nil || resp1.StatusCode != 200 {
		t.Fatalf("Turn 1 failed: %v %v %s", resp1, err1, raw1)
	}
	var out1 map[string]any
	if err := json.Unmarshal([]byte(raw1), &out1); err != nil {
		t.Fatal(err)
	}
	if out1["stop_reason"] != "tool_use" {
		t.Fatalf("expected tool_use on Turn 1, got %v", out1["stop_reason"])
	}
	var callID string
	for _, item := range out1["content"].([]any) {
		bm, _ := item.(map[string]any)
		if bm["type"] == "tool_use" {
			callID, _ = bm["id"].(string)
		}
	}
	if !isCryptographicAcpToolCallID(callID) {
		t.Fatalf("expected 36-char cryptographic acp_ tool_use id, got %q", callID)
	}

	sess := findSessionByConvPrefix(gs, "fp:")
	if sess == nil {
		t.Fatal("missing headerless fp: session")
	}
	sess.mu.Lock()
	origSid := sess.SessionID
	sess.mu.Unlock()

	// Turn 2 (headerless): mutate system prompt within first 4KB AND add 3rd tool while returning tool_result.
	sysT2 := "Primary working directory: " + tmp + "\ncurrentDate: 2026-11-18T09:05:00Z\ngitStatus: M main.go"
	asst1 := map[string]any{"role": "assistant", "content": out1["content"]}
	u2 := map[string]any{
		"role": "user",
		"content": []map[string]any{
			{"type": "tool_result", "tool_use_id": callID, "content": "FILE_LIST_OK"},
		},
	}
	resp2, raw2, err2 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "", "", map[string]any{
		"model":      "gemini-3.8-flash-low",
		"max_tokens": 512,
		"system":     sysT2,
		"tools":      tools3,
		"messages":   []map[string]any{u1, asst1, u2},
	})
	if err2 != nil || resp2.StatusCode != 200 {
		t.Fatalf("Turn 2 parked tool resume failed: %v %v %s", resp2, err2, raw2)
	}
	var out2 map[string]any
	_ = json.Unmarshal([]byte(raw2), &out2)
	// Verify both system delta and dynamic tool update reached the upstream tool result tail without rollover!
	if !strings.Contains(raw2, "[Host instructions updated]") || !strings.Contains(raw2, "SearchCode") {
		t.Fatalf("expected system delta and SearchCode tool delta in parked resume reply, got: %s", raw2)
	}

	// Turn 3 (headerless): mutate system prompt AGAIN on a normal text follow-up turn (len(messages) == 5).
	sysT3 := "Primary working directory: " + tmp + "\ncurrentDate: 2026-11-18T09:10:00Z\ngitStatus: M main.go M store.go"
	asst2 := map[string]any{"role": "assistant", "content": out2["content"]}
	u3 := map[string]any{"role": "user", "content": "Summarize the workspace status"}
	resp3, raw3, err3 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "", "", map[string]any{
		"model":      "gemini-3.8-flash-low",
		"max_tokens": 512,
		"system":     sysT3,
		"tools":      tools3,
		"messages":   []map[string]any{u1, asst1, u2, asst2, u3},
	})
	if err3 != nil || resp3.StatusCode != 200 {
		t.Fatalf("Turn 3 headerless follow-up failed: %v %v %s", resp3, err3, raw3)
	}

	sess.mu.Lock()
	finalSid := sess.SessionID
	rolls := sess.CompactionRollovers
	reuses := sess.SessionReuses
	sess.mu.Unlock()
	if finalSid != origSid || rolls != 0 || reuses < 2 {
		t.Fatalf("expected same SessionID %s with 0 rollovers and >=2 reuses, got sid=%s rolls=%d reuses=%d", origSid, finalSid, rolls, reuses)
	}
	if gs.countLiveWorkers() != 1 {
		t.Fatalf("expected exactly 1 live worker (no sharded fp: duplicate), got %d", gs.countLiveWorkers())
	}
}

func TestReloadHibernatedSessionFreezesInitialAppendPromptAndToolsAcrossRestart(t *testing.T) {
	tmp := t.TempDir()
	t.Cleanup(func() { GlobalEngine.StopEngine(); initConfig() })
	t.Setenv("AGY_ACP_BIN", fakeAcpBin)
	t.Setenv("AGY_PURE_API_HOME", filepath.Join(tmp, "pure"))
	t.Setenv("AGY_CONV_DIR", filepath.Join(tmp, "convs"))
	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "gateway.db"))
	t.Setenv("AGY_GW_LOG", filepath.Join(tmp, "gateway.log"))
	t.Setenv("AGY_ACP_RECORD", filepath.Join(tmp, "record"))
	t.Setenv("AGY_TOS_TRIPPED_FILE", filepath.Join(tmp, "tos-tripped"))
	initConfig()
	GlobalEngine.StopEngine()

	baseURL, gs := startCarbonTestGateway(t)

	toolsInit := []map[string]any{
		{
			"name":         "Bash",
			"description":  "Execute command",
			"input_schema": map[string]any{"type": "object", "properties": map[string]any{"command": map[string]any{"type": "string"}}},
		},
		{
			"name":         "Read",
			"description":  "Read file contents",
			"input_schema": map[string]any{"type": "object", "properties": map[string]any{"path": map[string]any{"type": "string"}}},
		},
	}
	toolsExpanded := append(append([]map[string]any(nil), toolsInit...), map[string]any{
		"name":         "DynamicSearch",
		"description":  "Added on turn 2",
		"input_schema": map[string]any{"type": "object", "properties": map[string]any{"q": map[string]any{"type": "string"}}},
	})

	sysInit := "Primary working directory: " + tmp + "\nInitial static rules v1"
	u1 := map[string]any{"role": "user", "content": "Turn 1 anchor"}
	resp1, raw1, err1 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "", "", map[string]any{
		"model":      "gemini-3.8-flash-low",
		"max_tokens": 256,
		"system":     sysInit,
		"tools":      toolsInit,
		"messages":   []map[string]any{u1},
	})
	if err1 != nil || resp1.StatusCode != 200 {
		t.Fatalf("Turn 1 failed: %v %v %s", resp1, err1, raw1)
	}
	var out1 map[string]any
	_ = json.Unmarshal([]byte(raw1), &out1)

	sess1 := findSessionByConvPrefix(gs, "fp:")
	if sess1 == nil {
		t.Fatal("missing fp: session")
	}
	sess1.mu.Lock()
	origSid := sess1.SessionID
	initAppend := sess1.InitialAppendPrompt
	tenantKey := sess1.TenantKey
	sess1.mu.Unlock()
	if initAppend == "" {
		t.Fatal("expected non-empty InitialAppendPrompt after session/new")
	}

	// Capture initial tools/list wire JSON on session/new.
	fetchToolsListJSON := func(urlBase, tk string) string {
		t.Helper()
		b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": map[string]any{}})
		req, _ := http.NewRequest("POST", urlBase+"/mcp/"+tk, bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+tk)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		return strings.TrimSpace(string(raw))
	}
	initToolsWire := fetchToolsListJSON(baseURL, tenantKey)
	if strings.Contains(initToolsWire, "DynamicSearch") {
		t.Fatalf("unexpected DynamicSearch in initial tools/list: %s", initToolsWire)
	}

	// Turn 2: mutate system prompt AND expand tools in-place.
	sysMutated := "Primary working directory: " + tmp + "\nMutated date 2026-11-18T10:30:00Z + dirty files"
	asst1 := map[string]any{"role": "assistant", "content": out1["content"]}
	u2 := map[string]any{"role": "user", "content": "Turn 2 with mutated system and expanded tools"}
	resp2, raw2, err2 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "", "", map[string]any{
		"model":      "gemini-3.8-flash-low",
		"max_tokens": 256,
		"system":     sysMutated,
		"tools":      toolsExpanded,
		"messages":   []map[string]any{u1, asst1, u2},
	})
	if err2 != nil || resp2.StatusCode != 200 {
		t.Fatalf("Turn 2 failed: %v %v %s", resp2, err2, raw2)
	}
	var out2 map[string]any
	_ = json.Unmarshal([]byte(raw2), &out2)

	// Simulate gateway restart: hibernate worker, re-open StateStore, start fresh GatewayServer (lazy SQLite restore!).
	if !sess1.HibernateWorker("test_restart", false) {
		t.Fatal("expected HibernateWorker to succeed")
	}
	GlobalStore.Init(filepath.Join(tmp, "gateway.db"))

	baseURL2, gs2 := startCarbonTestGateway(t)
	resumeOff := acpRecordOffset()

	// Turn 3 arrives headerless with a further mutated system prompt + 3 tools -> lazy RestoreFpSessionByHistoryFingerprint + session/resume!
	sysTurn3 := "Primary working directory: " + tmp + "\nMutated date 2026-11-18T11:00:00Z + more dirty files"
	asst2 := map[string]any{"role": "assistant", "content": out2["content"]}
	u3 := map[string]any{"role": "user", "content": "Turn 3 after restart"}
	resp3, raw3, err3 := doGatewayJSON(context.Background(), baseURL2+"/v1/messages", "", "", map[string]any{
		"model":      "gemini-3.8-flash-low",
		"max_tokens": 256,
		"system":     sysTurn3,
		"tools":      toolsExpanded,
		"messages":   []map[string]any{u1, asst1, u2, asst2, u3},
	})
	if err3 != nil || resp3.StatusCode != 200 {
		t.Fatalf("Turn 3 after restart failed: %v %v %s", resp3, err3, raw3)
	}

	restoredSess := findSessionByConvPrefix(gs2, "fp:")
	if restoredSess == nil {
		t.Fatal("missing restored fp: session on gs2")
	}
	restoredSess.mu.Lock()
	resumedSid := restoredSess.SessionID
	rolls := restoredSess.CompactionRollovers
	lastMode := ""
	if len(restoredSess.TurnHistory) > 0 {
		lastMode = restoredSess.TurnHistory[len(restoredSess.TurnHistory)-1].Mode
	}
	restoredSess.mu.Unlock()
	if resumedSid != origSid || rolls != 0 || lastMode != "resumed_delta" {
		t.Fatalf("expected resumed_delta on origSid=%s with 0 rollovers, got sid=%s rolls=%d mode=%q", origSid, resumedSid, rolls, lastMode)
	}

	// Prove session/resume sent the exact frozen InitialAppendPrompt (byte-0 invariant) and NOT Turn 2/3's mutated system!
	recBytes, err := os.ReadFile(filepath.Join(AcpRecordDir, "acp-traffic.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	foundResume := false
	for _, line := range strings.Split(string(recBytes[resumeOff:]), "\n") {
		if line == "" {
			continue
		}
		var rec struct {
			Dir  string `json:"dir"`
			Line string `json:"line"`
		}
		if json.Unmarshal([]byte(line), &rec) != nil || rec.Dir != "out" || !strings.Contains(rec.Line, `"method":"session/resume"`) {
			continue
		}
		var rpc struct {
			Params struct {
				Meta struct {
					Antigravity struct {
						SystemPrompt struct {
							Append string `json:"append"`
						} `json:"systemPrompt"`
					} `json:"antigravity"`
				} `json:"_meta"`
			} `json:"params"`
		}
		if err := json.Unmarshal([]byte(rec.Line), &rpc); err != nil {
			t.Fatal(err)
		}
		gotAppend := rpc.Params.Meta.Antigravity.SystemPrompt.Append
		if gotAppend != initAppend {
			t.Fatalf("session/resume mutated appendPrompt!\nwant: %q\ngot:  %q", initAppend, gotAppend)
		}
		foundResume = true
	}
	if !foundResume {
		t.Fatal("did not find recorded session/resume RPC")
	}

	// Prove tools/list after restart + session/resume is byte-for-byte identical to session/new!
	resumedToolsWire := fetchToolsListJSON(baseURL2, tenantKey)
	if resumedToolsWire != initToolsWire {
		t.Fatalf("tools/list drifted across session/resume!\ninit:    %s\nresumed: %s", initToolsWire, resumedToolsWire)
	}
}

func TestToolLessConversationNotClassifiedOrEvictedAsUtility(t *testing.T) {
	tmp := t.TempDir()
	t.Cleanup(func() { GlobalEngine.StopEngine(); initConfig() })
	t.Setenv("AGY_ACP_BIN", fakeAcpBin)
	t.Setenv("AGY_PURE_API_HOME", filepath.Join(tmp, "pure"))
	t.Setenv("AGY_CONV_DIR", filepath.Join(tmp, "convs"))
	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "gateway.db"))
	t.Setenv("AGY_GW_LOG", filepath.Join(tmp, "gateway.log"))
	t.Setenv("AGY_TOS_TRIPPED_FILE", filepath.Join(tmp, "tos-tripped"))
	initConfig()
	GlobalEngine.StopEngine()

	// 1. Key classification checks
	msgs1 := []map[string]any{{"role": "user", "content": "Explain distributed consensus."}}
	rExplicit := httptest.NewRequest("POST", "/v1/messages", nil)
	rExplicit.Header.Set("x-session-id", "chat-no-tools-1")
	rExplicit.Header.Set("User-Agent", "claude-cli/2.1.0")
	if k := deriveConversationKey(rExplicit, map[string]any{"max_tokens": 1024}, "You are a helpful assistant.", nil, msgs1); !strings.HasPrefix(k, "sid:chat-no-tools-1:") {
		t.Fatalf("explicit session with 0 tools misclassified as %q", k)
	}

	rSdk := httptest.NewRequest("POST", "/v1/messages", nil)
	rSdk.Header.Set("User-Agent", "anthropic-python/0.49.0")
	if k := deriveConversationKey(rSdk, map[string]any{"max_tokens": 1024}, "You are a coding tutor.", nil, msgs1); !strings.HasPrefix(k, "fp:") {
		t.Fatalf("non-claude-cli 0-tool chat misclassified as %q", k)
	}

	rClaudeLong := httptest.NewRequest("POST", "/v1/messages", nil)
	rClaudeLong.Header.Set("User-Agent", "claude-cli/2.1.0")
	longMsgs := []map[string]any{{"role": "user", "content": strings.Repeat("context ", 2400)}} // >16K chars
	if k := deriveConversationKey(rClaudeLong, map[string]any{"max_tokens": 1024}, "Custom agent prompt", nil, longMsgs); !strings.HasPrefix(k, "fp:") {
		t.Fatalf(">=16K-char 0-tool prompt misclassified as %q", k)
	}

	// Legitimate short 1-turn Claude Code background utility call without explicit session still maps to util:
	rClaudeBg := httptest.NewRequest("POST", "/v1/messages", nil)
	rClaudeBg.Header.Set("User-Agent", "claude-cli/2.1.0")
	if k := deriveConversationKey(rClaudeBg, map[string]any{"max_tokens": 128}, "Generate a concise 4-word title.", nil, msgs1); !strings.HasPrefix(k, "util:") {
		t.Fatalf("expected short claude-cli background title call to map to util:, got %q", k)
	}
	if k := deriveConversationKey(rExplicit, map[string]any{"max_tokens": 1}, "You are a helpful assistant.", nil, msgs1); !strings.HasPrefix(k, "util:") {
		t.Fatalf("expected max_tokens=1 quota probe to map to util:, got %q", k)
	}

	// 2. End-to-end 0-tool conversation survives >20s (UtilWorkerTtl) idle pause without eviction
	baseURL, gs := startCarbonTestGateway(t)
	u1 := map[string]any{"role": "user", "content": "Hello, help me design a B-tree."}
	resp1, raw1, err1 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "", "", map[string]any{
		"model":      "gemini-3.8-flash-low",
		"max_tokens": 256,
		"system":     "You are a database systems expert.",
		"tools":      []any{},
		"messages":   []map[string]any{u1},
	})
	if err1 != nil || resp1.StatusCode != 200 {
		t.Fatalf("Turn 1 failed: %v %v %s", resp1, err1, raw1)
	}
	var out1 map[string]any
	_ = json.Unmarshal([]byte(raw1), &out1)

	sess := findSessionByConvPrefix(gs, "fp:")
	if sess == nil {
		t.Fatal("expected fp: session for tool-less chat")
	}
	sess.mu.Lock()
	sid1 := sess.SessionID
	// Age LastUsedAt to 25s (> 20s UtilWorkerTtl, < IdleWorkerTtl) and run reaper check logic.
	sess.LastUsedAt = time.Now().Add(-25 * time.Second)
	sess.mu.Unlock()

	// Trigger reaper logic for sessions exceeding their TTL: non-util session must stay alive!
	if strings.HasPrefix(sess.ConvKey, "util:") {
		t.Fatalf("unexpected util: key %s", sess.ConvKey)
	}

	asst1 := map[string]any{"role": "assistant", "content": out1["content"]}
	u2 := map[string]any{"role": "user", "content": "Now add WAL recovery details."}
	resp2, raw2, err2 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "", "", map[string]any{
		"model":      "gemini-3.8-flash-low",
		"max_tokens": 256,
		"system":     "You are a database systems expert.",
		"tools":      []any{},
		"messages":   []map[string]any{u1, asst1, u2},
	})
	if err2 != nil || resp2.StatusCode != 200 {
		t.Fatalf("Turn 2 failed: %v %v %s", resp2, err2, raw2)
	}
	sess.mu.Lock()
	sid2 := sess.SessionID
	reuses := sess.SessionReuses
	sess.mu.Unlock()
	if sid2 != sid1 || reuses != 1 {
		t.Fatalf("expected sticky_delta reuse on sid=%s, got sid=%s reuses=%d", sid1, sid2, reuses)
	}
}

func TestContextAwareSoftDetachPolicyJoinElapsedAndPassiveEndTurnHold(t *testing.T) {
	tmp := t.TempDir()
	t.Cleanup(func() { initConfig() })
	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "gateway.db"))
	t.Setenv("AGY_PREFILL_CANCEL_MIN_ELAPSED_SEC", "")
	t.Setenv("AGY_PREFILL_CANCEL_GRACE_SEC", "")
	initConfig()

	// 1. Context-aware scaling tiers
	sSmall := NewAcpProjectSession("sid:ctx-small:u:1", tmp, "rules")
	if minE, gr, maxD := sSmall.effectiveSoftDetachPolicy(4000); minE != 30*time.Second || gr != 45*time.Second || maxD != 3 {
		t.Fatalf("unexpected <48K policy: min=%v grace=%v max=%d", minE, gr, maxD)
	}
	sMid := NewAcpProjectSession("sid:ctx-mid:u:1", tmp, "rules")
	sMid.LastCachedReadTokens = 60000
	if minE, gr, maxD := sMid.effectiveSoftDetachPolicy(400); minE != 8*time.Second || gr != 90*time.Second || maxD != 5 {
		t.Fatalf("unexpected >=48K policy: min=%v grace=%v max=%d", minE, gr, maxD)
	}
	sLarge := NewAcpProjectSession("sid:ctx-large:u:1", tmp, "rules")
	sLarge.LastCachedReadTokens = 150000
	if minE, gr, maxD := sLarge.effectiveSoftDetachPolicy(400); minE != 5*time.Second || gr != 120*time.Second || maxD != 6 {
		t.Fatalf("unexpected >=120K policy: min=%v grace=%v max=%d", minE, gr, maxD)
	}

	// 2. JoinDetachedTurn uses det.turnStart (total turn elapsed) so quick disconnects after join re-detach cleanly
	origMin, origGrace := PrefillCancelMinElapsed, PrefillCancelGrace
	PrefillCancelMinElapsed = 80 * time.Millisecond
	PrefillCancelGrace = 800 * time.Millisecond
	defer func() {
		PrefillCancelMinElapsed = origMin
		PrefillCancelGrace = origGrace
	}()

	sJoin := NewAcpProjectSession("sid:join-elapsed:u:1", tmp, "Primary working directory: "+tmp)
	turn := newActiveTurnState(nil, func() {})
	turnStart := time.Now().Add(-200 * time.Millisecond)
	turn.createdAt = turnStart
	sJoin.SessionID = "00000000-0000-4000-8000-000000009001"
	sJoin.activeTurn = turn

	msgs := []map[string]any{{"role": "user", "content": []any{map[string]any{"type": "text", "text": "analyze large repo", "cache_control": map[string]any{"type": "ephemeral"}}}}}
	toolsA := sanitizeToolsCatalog([]any{
		map[string]any{"name": "Read", "input_schema": map[string]any{"type": "object", "properties": map[string]any{"a": map[string]any{"type": "string"}, "b": map[string]any{"type": "string"}}, "required": []any{"b", "a"}}},
		map[string]any{"name": "Bash", "input_schema": map[string]any{"type": "object", "properties": map[string]any{"cmd": map[string]any{"type": "string"}}}},
	})
	toolsB := sanitizeToolsCatalog([]any{
		map[string]any{"name": "Bash", "input_schema": map[string]any{"type": "object", "properties": map[string]any{"cmd": map[string]any{"type": "string"}}}},
		map[string]any{"name": "Read", "input_schema": map[string]any{"type": "object", "properties": map[string]any{"a": map[string]any{"type": "string"}, "b": map[string]any{"type": "string"}}, "required": []any{"a", "b"}}},
	})
	msgsRetry := []map[string]any{{"role": "user", "content": []any{map[string]any{"type": "text", "text": "analyze large repo"}}}}

	fp1 := computeTurnRequestFingerprint("gemini-4-carbon", "high", "Primary working directory: "+tmp+"\ndate: T1", toolsA, msgs)
	fp2 := computeTurnRequestFingerprint("gemini-4-carbon", "high", "Primary working directory: "+tmp+"\ndate: T2", toolsB, msgsRetry)
	if fp1 == "" || fp1 != fp2 {
		t.Fatalf("expected canonical reqFp invariant across system date, tool order, required order, and cache_control: %q vs %q", fp1, fp2)
	}

	if !sJoin.tryStartSoftDetach(turn, fp1, sJoin.SessionID, "cold_start", "", turnStart, turnStart, 1, "u1", "h1", false) {
		t.Fatal("initial soft-detach failed")
	}

	// Subscriber 2 joins and disconnects after ONLY 15ms (< 80ms PrefillCancelMinElapsed, but total turn elapsed > 200ms!):
	shortCtx, cancelShort := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancelShort()
	_, joined2, errJoin2 := sJoin.JoinDetachedTurn(shortCtx, fp2, msgsRetry, nil)
	if !joined2 || !isSoftDetachedErr(errJoin2) || !sJoin.HasDetachedTurn() {
		t.Fatalf("expected JoinDetachedTurn to re-detach using det.turnStart, got joined=%v err=%v hasDet=%v", joined2, errJoin2, sJoin.HasDetachedTurn())
	}
	sJoin.mu.Lock()
	if sJoin.detachedTurn != nil && sJoin.detachedTurn.graceTimer != nil {
		sJoin.detachedTurn.graceTimer.Stop()
	}
	sJoin.detachedTurn = nil
	sJoin.mu.Unlock()

	// 3. Passive hold buffer: upstream resolves pure end_turn BEFORE grace expires; grace timer fires ->
	// releases promptSem immediately and holds result passively so late retry claims in 0ms with 0 rollovers!
	PrefillCancelGrace = 60 * time.Millisecond
	semReleased := false
	turnHold := newActiveTurnState(nil, func() { semReleased = true })
	turnHold.createdAt = turnStart
	sHold := NewAcpProjectSession("sid:hold-endturn:u:1", tmp, "Primary working directory: "+tmp)
	sHold.SessionID = "00000000-0000-4000-8000-000000009002"
	sHold.activeTurn = turnHold
	if !sHold.tryStartSoftDetach(turnHold, fp1, sHold.SessionID, "cold_start", "", turnStart, turnStart, 1, "u1", "h1", false) {
		t.Fatal("expected sHold soft-detach")
	}
	turnHold.resolveChan <- TurnResult{
		StopReason: "end_turn",
		Text:       "PASSIVE_CLAIMED_200K_OK",
		Usage:      &TurnUsage{InputTokens: 2000, CachedReadTokens: 198000, OutputTokens: 64, UsageSource: "db"},
	}

	// Wait past first 60ms grace window so expireDetachedTurn transitions into passive hold state (60ms more)
	// Wait slightly > 65ms? With 60ms + 60ms = 120ms total, use 250ms grace for rock-solid CI timing!
	sHold.mu.Lock()
	if sHold.detachedTurn != nil && sHold.detachedTurn.graceTimer != nil {
		sHold.detachedTurn.graceTimer.Stop()
	}
	sHold.mu.Unlock()
	PrefillCancelGrace = 400 * time.Millisecond
	sHold.expireDetachedTurn(sHold.detachedTurn)

	if !semReleased {
		t.Fatal("expected expireDetachedTurn to release promptSem immediately when transitioning to passive hold")
	}
	if !sHold.HasDetachedTurn() {
		t.Fatal("expected completed pure end_turn to remain claimable in passive hold buffer")
	}

	var streamed strings.Builder
	claimedRes, joinedClaim, claimErr := sHold.JoinDetachedTurn(context.Background(), fp2, msgsRetry, func(c DeltaChunk) {
		streamed.WriteString(c.Text)
	})
	if claimErr != nil || !joinedClaim || claimedRes.Text != "PASSIVE_CLAIMED_200K_OK" || streamed.String() != "PASSIVE_CLAIMED_200K_OK" {
		t.Fatalf("late retry failed to claim passive end_turn result: joined=%v err=%v text=%q streamed=%q", joinedClaim, claimErr, claimedRes.Text, streamed.String())
	}
	sHold.mu.Lock()
	if sHold.ForceRolloverNext || sHold.CompactionRollovers != 0 || sHold.MessageCount != 2 || sHold.SessionID == "" {
		t.Fatalf("unexpected rollover or state loss after passive claim: forceRoll=%v rolls=%d msgCnt=%d sid=%q",
			sHold.ForceRolloverNext, sHold.CompactionRollovers, sHold.MessageCount, sHold.SessionID)
	}
	sHold.mu.Unlock()
}

func TestTransientSoftErrorPreservesWarmSessionAndInPlaceCapacityFallback(t *testing.T) {
	tmp := t.TempDir()
	t.Cleanup(func() { GlobalEngine.StopEngine(); initConfig() })
	t.Setenv("AGY_ACP_BIN", fakeAcpBin)
	t.Setenv("AGY_PURE_API_HOME", filepath.Join(tmp, "pure"))
	t.Setenv("AGY_CONV_DIR", filepath.Join(tmp, "convs"))
	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "gateway.db"))
	t.Setenv("AGY_GW_LOG", filepath.Join(tmp, "gateway.log"))
	t.Setenv("AGY_ACP_RECORD", filepath.Join(tmp, "record"))
	t.Setenv("AGY_TOS_TRIPPED_FILE", filepath.Join(tmp, "tos-tripped"))
	initConfig()
	GlobalEngine.StopEngine()

	baseURL, gs := startCarbonTestGateway(t)

	// Part A: Explicit gemini-4-carbon-high session warms up on Turn 1, hits transient 429 on Turn 2,
	// preserves live SessionID & MessageCount=2, and reuses the warm session on Turn 2 retry!
	u1 := map[string]any{"role": "user", "content": "Warm up turn 1"}
	resp1, raw1, err1 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "warm-429-keep", map[string]any{
		"model":      "gemini-4-carbon-high",
		"max_tokens": 256,
		"system":     "Primary working directory: " + tmp,
		"messages":   []map[string]any{u1},
	})
	if err1 != nil || resp1.StatusCode != 200 {
		t.Fatalf("Turn 1 failed: %v %v %s", resp1, err1, raw1)
	}
	var out1 map[string]any
	_ = json.Unmarshal([]byte(raw1), &out1)
	asst1 := map[string]any{"role": "assistant", "content": out1["content"]}

	sessWarm := findSessionByConvPrefix(gs, "sid:warm-429-keep")
	sessWarm.mu.Lock()
	warmSid := sessWarm.SessionID
	sessWarm.mu.Unlock()

	// Trigger transient 429 on Turn 2 (explicit -high does not downgrade, returns HTTP 429).
	resp429, _, err429 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "warm-429-keep", map[string]any{
		"model":      "gemini-4-carbon-high",
		"max_tokens": 256,
		"system":     "Primary working directory: " + tmp,
		"messages":   []map[string]any{u1, asst1, {"role": "user", "content": "[FAKE_HIGH_429_MEDIUM_OK] transient spike"}},
	})
	if err429 != nil || resp429.StatusCode != 429 {
		t.Fatalf("expected HTTP 429 on explicit high capacity spike, got %v %v", resp429, err429)
	}
	sessWarm.mu.Lock()
	sidAfter429 := sessWarm.SessionID
	msgCntAfter429 := sessWarm.MessageCount
	sessWarm.mu.Unlock()
	if sidAfter429 != warmSid || msgCntAfter429 != 2 {
		t.Fatalf("transient 429 destroyed warm session: sid=%q (want %q) msgCnt=%d (want 2)", sidAfter429, warmSid, msgCntAfter429)
	}

	// Retry Turn 2 after upstream clears -> must hit sticky_delta on the SAME warmSid!
	respRetry, rawRetry, errRetry := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "warm-429-keep", map[string]any{
		"model":      "gemini-4-carbon-high",
		"max_tokens": 256,
		"system":     "Primary working directory: " + tmp,
		"messages":   []map[string]any{u1, asst1, {"role": "user", "content": "retry after backoff"}},
	})
	if errRetry != nil || respRetry.StatusCode != 200 {
		t.Fatalf("Turn 2 retry failed: %v %v %s", respRetry, errRetry, rawRetry)
	}
	sessWarm.mu.Lock()
	if sessWarm.SessionID != warmSid || sessWarm.CompactionRollovers != 0 || sessWarm.SessionReuses < 2 {
		t.Fatalf("expected warm retry to keep sid=%s with 0 rollovers, got sid=%s rolls=%d reuses=%d",
			warmSid, sessWarm.SessionID, sessWarm.CompactionRollovers, sessWarm.SessionReuses)
	}
	sessWarm.mu.Unlock()

	// Part B: Implicit high tier (budget_tokens=31999) on a WARM session switches thought_level=medium
	// IN-PLACE on the SAME SessionID (zero openFreshSession rollover)!
	respCap1, rawCap1, errCap1 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "warm-inplace-cap", map[string]any{
		"model":      "gemini-4-carbon",
		"max_tokens": 32000,
		"thinking":   map[string]any{"type": "enabled", "budget_tokens": 31999},
		"system":     "Primary working directory: " + tmp,
		"messages":   []map[string]any{{"role": "user", "content": "Turn 1 warm high"}},
	})
	if errCap1 != nil || respCap1.StatusCode != 200 {
		t.Fatalf("Cap Turn 1 failed: %v %v %s", respCap1, errCap1, rawCap1)
	}
	var outCap1 map[string]any
	_ = json.Unmarshal([]byte(rawCap1), &outCap1)

	sessCap := findSessionByConvPrefix(gs, "sid:warm-inplace-cap")
	sessCap.mu.Lock()
	capSid1 := sessCap.SessionID
	sessCap.mu.Unlock()

	respCap2, rawCap2, errCap2 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "warm-inplace-cap", map[string]any{
		"model":      "gemini-4-carbon",
		"max_tokens": 32000,
		"thinking":   map[string]any{"type": "enabled", "budget_tokens": 31999},
		"system":     "Primary working directory: " + tmp,
		"messages": []map[string]any{
			{"role": "user", "content": "Turn 1 warm high"},
			{"role": "assistant", "content": outCap1["content"]},
			{"role": "user", "content": "[FAKE_HIGH_429_MEDIUM_OK] Turn 2 capacity fallback in-place"},
		},
	})
	if errCap2 != nil || respCap2.StatusCode != 200 || !strings.Contains(rawCap2, "FALLBACK_MEDIUM_OK") {
		t.Fatalf("Cap Turn 2 failed: %v %v %s", respCap2, errCap2, rawCap2)
	}
	sessCap.mu.Lock()
	capSid2 := sessCap.SessionID
	capTier2 := sessCap.ThoughtLevel
	capRolls := sessCap.CompactionRollovers
	sessCap.mu.Unlock()
	if capSid2 != capSid1 || capTier2 != "medium" || capRolls != 0 {
		t.Fatalf("expected in-place medium switch on same sid=%s with 0 rollovers, got sid=%s tier=%s rolls=%d", capSid1, capSid2, capTier2, capRolls)
	}
}

func TestSlidingCacheControlAndFirstTurnSystemReminderNoRollover(t *testing.T) {
	tmp := t.TempDir()
	t.Cleanup(func() { GlobalEngine.StopEngine(); initConfig() })
	t.Setenv("AGY_ACP_BIN", fakeAcpBin)
	t.Setenv("AGY_PURE_API_HOME", filepath.Join(tmp, "pure"))
	t.Setenv("AGY_CONV_DIR", filepath.Join(tmp, "convs"))
	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "gateway.db"))
	t.Setenv("AGY_GW_LOG", filepath.Join(tmp, "gateway.log"))
	t.Setenv("AGY_TOS_TRIPPED_FILE", filepath.Join(tmp, "tos-tripped"))
	initConfig()
	GlobalEngine.StopEngine()

	baseURL, gs := startCarbonTestGateway(t)

	u1T1 := map[string]any{
		"role": "user",
		"content": []map[string]any{
			{
				"type":          "text",
				"text":          "<system-reminder>currentDate: 2026-11-18T08:00:00Z</system-reminder>\nImplement binary search.",
				"cache_control": map[string]any{"type": "ephemeral"},
			},
		},
	}
	resp1, raw1, err1 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "sliding-cc-1", map[string]any{
		"model":      "gemini-3.8-flash-low",
		"max_tokens": 256,
		"system":     "Primary working directory: " + tmp,
		"messages":   []map[string]any{u1T1},
	})
	if err1 != nil || resp1.StatusCode != 200 {
		t.Fatalf("Turn 1 failed: %v %v %s", resp1, err1, raw1)
	}
	var out1 map[string]any
	_ = json.Unmarshal([]byte(raw1), &out1)
	asst1 := map[string]any{"role": "assistant", "content": out1["content"]}

	sess := findSessionByConvPrefix(gs, "sid:sliding-cc-1")
	sess.mu.Lock()
	sid1 := sess.SessionID
	sess.mu.Unlock()

	// Cancel Turn 2A mid-flight (before soft-detach threshold) so cancelledHistoryFp is recorded!
	ctxCanc, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, _, _ = doGatewayJSON(ctxCanc, baseURL+"/v1/messages", "x-claude-code-session-id", "sliding-cc-1", map[string]any{
		"model":      "gemini-3.8-flash-low",
		"max_tokens": 256,
		"system":     "Primary working directory: " + tmp,
		"messages": []map[string]any{
			u1T1,
			asst1,
			{
				"role": "user",
				"content": []map[string]any{
					{"type": "text", "text": "[FAKE_SLOW] Add unit tests.", "cache_control": map[string]any{"type": "ephemeral"}},
				},
			},
		},
	})

	// Retry Turn 2B: client strips cache_control from messages[0], mutates <system-reminder> in messages[0],
	// and moves cache_control onto assistant or omits it on retry -> MUST NOT trigger rollover_cancelled_edit or rollover_prefix!
	u1T2 := map[string]any{
		"role": "user",
		"content": []map[string]any{
			{
				"type": "text",
				"text": "<system-reminder>currentDate: 2026-11-18T08:15:00Z</system-reminder>\nImplement binary search.",
			},
		},
	}
	resp2, raw2, err2 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "sliding-cc-1", map[string]any{
		"model":      "gemini-3.8-flash-low",
		"max_tokens": 256,
		"system":     "Primary working directory: " + tmp,
		"messages": []map[string]any{
			u1T2,
			asst1,
			{
				"role": "user",
				"content": []map[string]any{
					{"type": "text", "text": "[FAKE_SLOW] Add unit tests."},
				},
			},
		},
	})
	if err2 != nil || resp2.StatusCode != 200 {
		t.Fatalf("Turn 2 retry failed: %v %v %s", resp2, err2, raw2)
	}
	sess.mu.Lock()
	sid2 := sess.SessionID
	rolls := sess.CompactionRollovers
	sess.mu.Unlock()
	if sid2 != sid1 || rolls != 0 {
		t.Fatalf("sliding cache_control or <system-reminder> caused false rollover: sid1=%s sid2=%s rolls=%d", sid1, sid2, rolls)
	}
}

func TestSubagentSharedSessionIsolationAndInFlightHedgedRetryCoalescing(t *testing.T) {
	tmp := t.TempDir()
	t.Cleanup(func() { GlobalEngine.StopEngine(); initConfig() })
	t.Setenv("AGY_ACP_BIN", fakeAcpBin)
	t.Setenv("AGY_PURE_API_HOME", filepath.Join(tmp, "pure"))
	t.Setenv("AGY_CONV_DIR", filepath.Join(tmp, "convs"))
	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "gateway.db"))
	t.Setenv("AGY_GW_LOG", filepath.Join(tmp, "gateway.log"))
	t.Setenv("AGY_ACP_RECORD", filepath.Join(tmp, "record"))
	t.Setenv("AGY_TOS_TRIPPED_FILE", filepath.Join(tmp, "tos-tripped"))
	initConfig()
	GlobalEngine.StopEngine()

	origMin, origGrace := PrefillCancelMinElapsed, PrefillCancelGrace
	PrefillCancelMinElapsed = 20 * time.Millisecond
	PrefillCancelGrace = 5 * time.Second
	defer func() {
		PrefillCancelMinElapsed = origMin
		PrefillCancelGrace = origGrace
	}()

	baseURL, gs := startCarbonTestGateway(t)

	// Part A: Parent Turn-1 soft-detaches; an agent_id-less subagent sharing x-claude-code-session-id
	// (with subagent system prompt) MUST NOT supersede or cancel the parent's detached prefill!
	ctxParent, cancelParent := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancelParent()
	parentSys := "Primary working directory: " + tmp + "\nMain interactive coding agent."
	parentMsgs := []map[string]any{{"role": "user", "content": "[FAKE_SLOW] Parent 100K prefill"}}
	_, _, _ = doGatewayJSON(ctxParent, baseURL+"/v1/messages", "x-claude-code-session-id", "shared-sid-sub", map[string]any{
		"model":      "gemini-4-carbon",
		"max_tokens": 128,
		"system":     parentSys,
		"messages":   parentMsgs,
	})
	parentSess := waitForDetachedSession(t, gs, 500*time.Millisecond)
	parentSess.mu.Lock()
	parentSid := parentSess.SessionID
	parentSess.mu.Unlock()

	// Subagent arrives with SAME x-claude-code-session-id, NO agent_id, different system & prompt:
	subResp, subRaw, subErr := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "shared-sid-sub", map[string]any{
		"model":      "gemini-3.8-flash-low",
		"max_tokens": 128,
		"system":     "Primary working directory: " + tmp + "\nYou are an Explore subagent.",
		"messages":   []map[string]any{{"role": "user", "content": "Subagent quick search"}},
	})
	if subErr != nil || subResp.StatusCode != 200 {
		t.Fatalf("subagent call failed: %v %v %s", subResp, subErr, subRaw)
	}
	if !parentSess.HasDetachedTurn() {
		t.Fatal("subagent superseded and killed parent's detached Turn-1 prefill!")
	}
	// Parent retries and joins its preserved detached turn cleanly!
	pJoinResp, pJoinRaw, pJoinErr := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "shared-sid-sub", map[string]any{
		"model":      "gemini-4-carbon",
		"max_tokens": 128,
		"system":     parentSys,
		"messages":   parentMsgs,
	})
	if pJoinErr != nil || pJoinResp.StatusCode != 200 {
		t.Fatalf("parent join failed: %v %v %s", pJoinResp, pJoinErr, pJoinRaw)
	}
	parentSess.mu.Lock()
	if parentSess.SessionID != parentSid {
		t.Fatalf("expected parent to keep sid=%s, got %s", parentSid, parentSess.SessionID)
	}
	parentSess.mu.Unlock()

	// Part B: Active in-flight hedged retry coalescing (Request 2 arrives while Request 1 is STILL in-flight,
	// before Request 1's TCP connection closes -> zero cold-start slot #2 spawned!)
	hedgePayload := map[string]any{
		"model":      "gemini-4-carbon",
		"max_tokens": 128,
		"system":     "Primary working directory: " + tmp + "\nHedged test",
		"messages":   []map[string]any{{"role": "user", "content": "[FAKE_SLOW] hedged in-flight prompt"}},
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _, _ = doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "hedged-inflight-1", hedgePayload)
	}()

	// Wait until Request 1 has activeTurn running on hedged-inflight-1
	deadline := time.Now().Add(1 * time.Second)
	var hedgeSess *AcpProjectSession
	for time.Now().Before(deadline) {
		hedgeSess = findSessionByConvPrefix(gs, "sid:hedged-inflight-1")
		if hedgeSess != nil {
			hedgeSess.mu.Lock()
			active := hedgeSess.activeTurn != nil
			hedgeSess.mu.Unlock()
			if active {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if hedgeSess == nil {
		t.Fatal("Request 1 did not start activeTurn in time")
	}

	// Send identical hedged retry Request 2 while Request 1 is still active!
	respH2, rawH2, errH2 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "hedged-inflight-1", hedgePayload)
	if errH2 != nil || respH2.StatusCode != 200 || !strings.Contains(rawH2, "FAKE_OK") {
		t.Fatalf("hedged Request 2 failed: %v %v %s", respH2, errH2, rawH2)
	}
	wg.Wait()

	// Verify NO "#2" slot session was created for sid:hedged-inflight-1!
	gs.mu.RLock()
	hedgedSlots := 0
	for k := range gs.sessions {
		if strings.HasPrefix(k, "sid:hedged-inflight-1") {
			hedgedSlots++
		}
	}
	gs.mu.RUnlock()
	if hedgedSlots != 1 {
		t.Fatalf("expected hedged retry to coalesce onto 1 session slot without spawning #2, got %d slots", hedgedSlots)
	}
}

func TestToolSchemaRequiredSortingAndContextWeightedWorkerLRU(t *testing.T) {
	// 1. Top-level and nested JSON Schema "required" sorting determinism
	makeTools := func(topReq, nestedReq []any) []ToolDef {
		return sanitizeToolsCatalog([]any{
			map[string]any{
				"name":        "ComplexTool",
				"description": "Test schema determinism",
				"input_schema": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"zeta":  map[string]any{"type": "string"},
						"alpha": map[string]any{"type": "string"},
						"nested": map[string]any{
							"type": "object",
							"properties": map[string]any{
								"y": map[string]any{"type": "number"},
								"x": map[string]any{"type": "number"},
							},
							"required": nestedReq,
						},
					},
					"required": topReq,
				},
			},
		})
	}
	cat1 := makeTools([]any{"zeta", "alpha", "nested"}, []any{"y", "x"})
	cat2 := makeTools([]any{"nested", "alpha", "zeta"}, []any{"x", "y"})
	if toolDefDigest(cat1[0]) != toolDefDigest(cat2[0]) {
		t.Fatalf("permuted required slices produced different digests:\n1: %s\n2: %s", toolDefDigest(cat1[0]), toolDefDigest(cat2[0]))
	}
	added, removed, changed := diffToolCatalogs(cat1, cat2)
	if len(added) != 0 || len(removed) != 0 || len(changed) != 0 {
		t.Fatalf("expected 0 diff on permuted required slices, got added=%v removed=%v changed=%v", added, removed, changed)
	}

	// 2. Context-weighted LRU eviction in enforceWorkerCap protects warm >=64K sessions (<300s idle)
	// over tiny <64K sessions even when the >=64K session has an older LastUsedAt!
	tmp := t.TempDir()
	t.Cleanup(func() { GlobalEngine.StopEngine(); initConfig() })
	t.Setenv("AGY_ACP_BIN", fakeAcpBin)
	t.Setenv("AGY_PURE_API_HOME", filepath.Join(tmp, "pure"))
	t.Setenv("AGY_CONV_DIR", filepath.Join(tmp, "convs"))
	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "gateway.db"))
	t.Setenv("AGY_GW_LOG", filepath.Join(tmp, "gateway.log"))
	t.Setenv("AGY_MAX_LIVE_WORKERS", "2")
	t.Setenv("AGY_TOS_TRIPPED_FILE", filepath.Join(tmp, "tos-tripped"))
	initConfig()
	GlobalEngine.StopEngine()

	gs := NewGatewayServer()
	defer gs.Close()

	now := time.Now()
	sWarmLarge := NewAcpProjectSession("sid:lru-large-200k:u:1", tmp, "rules")
	sWarmLarge.SessionID = "00000000-0000-4000-8000-000000007701"
	sWarmLarge.MessageCount = 6
	sWarmLarge.LastCachedReadTokens = 180000
	sWarmLarge.LastUsedAt = now.Add(-60 * time.Second) // older than sSmall, but warm (<300s) and 180K!

	sSmallRecent := NewAcpProjectSession("sid:lru-small-4k:u:1", tmp, "rules")
	sSmallRecent.SessionID = "00000000-0000-4000-8000-000000007702"
	sSmallRecent.MessageCount = 2
	sSmallRecent.LastCachedReadTokens = 4000
	sSmallRecent.LastUsedAt = now.Add(-10 * time.Second) // newer, but tiny 4K context

	sIncoming := NewAcpProjectSession("sid:lru-incoming:u:1", tmp, "rules")

	gs.mu.Lock()
	gs.sessions[sWarmLarge.ConvKey] = sWarmLarge
	gs.sessions[sSmallRecent.ConvKey] = sSmallRecent
	gs.sessions[sIncoming.ConvKey] = sIncoming
	gs.mu.Unlock()

	gs.enforceWorkerCap(sIncoming)

	if sWarmLarge.SessionID == "" {
		t.Fatal("enforceWorkerCap evicted warm 180K session instead of tiny 4K session!")
	}
	if sSmallRecent.SessionID != "" || sSmallRecent.HibernatedSessionID != "00000000-0000-4000-8000-000000007702" {
		t.Fatalf("expected tiny 4K session to hibernate cleanly with HibernatedSessionID, got sid=%q hib=%q",
			sSmallRecent.SessionID, sSmallRecent.HibernatedSessionID)
	}
}

func TestAppConversationsForkStepDeduplicationAndLegacyUpgradePurge(t *testing.T) {
	tmp := t.TempDir()
	appDir := filepath.Join(tmp, "app-convs")
	if err := os.MkdirAll(appDir, 0o755); err != nil {
		t.Fatal(err)
	}
	stateDB := filepath.Join(tmp, "state.db")

	parentSid := "aaaa1111-2222-4000-8000-000000000001"
	childSid := "bbbb1111-2222-4000-8000-000000000002"
	parentPath := filepath.Join(appDir, parentSid+".db")
	childPath := filepath.Join(appDir, childSid+".db")

	nowSec := time.Now().Unix() - 120
	makeStepMeta := func(ts int64, inTok, crTok, outTok int) []byte {
		usage := append(protoUint(1, 1427), protoUint(2, uint64(inTok))...)
		usage = append(usage, protoUint(3, uint64(outTok))...)
		usage = append(usage, protoUint(5, uint64(crTok))...)
		m := append(protoBytes(1, protoUint(1, uint64(ts))), protoBytes(6, protoUint(1, uint64(ts+1)))...)
		m = append(m, protoBytes(7, protoUint(1, uint64(ts+2)))...)
		m = append(m, protoBytes(9, usage)...)
		return m
	}
	payload := protoBytes(20, protoBytes(1, []byte("ok")))

	createConvDB := func(p string, stepIndices []int, forkStepIdx int) {
		t.Helper()
		dsn := sqliteFileURI(p, "_pragma=journal_mode(WAL)&_pragma=busy_timeout(2000)")
		db, err := sql.Open("sqlite", dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		schema := `
CREATE TABLE steps (
	idx INTEGER PRIMARY KEY,
	step_type INTEGER,
	status INTEGER,
	has_subtrajectory NUMERIC,
	metadata BLOB,
	error_details BLOB,
	permissions BLOB,
	task_details BLOB,
	render_info BLOB,
	step_payload BLOB,
	step_format INTEGER
);
CREATE TABLE parent_references (
	idx INTEGER PRIMARY KEY,
	data BLOB
);
`
		if _, err := db.Exec(schema); err != nil {
			t.Fatal(err)
		}
		for _, idx := range stepIndices {
			if _, err := db.Exec(
				`INSERT INTO steps VALUES (?, 15, 3, 0, ?, NULL, NULL, NULL, NULL, ?, 1)`,
				idx, makeStepMeta(nowSec+int64(idx*3), 2000, idx*1000, 100), payload,
			); err != nil {
				t.Fatal(err)
			}
		}
		if forkStepIdx >= 0 {
			// parent_references.data protobuf: .1 = parent_trajectory_id, .2 = fork_step_idx (varint), .6 = parent_cascade_id
			refBlob := append(protoBytes(1, []byte("traj-parent-001")), protoUint(2, uint64(forkStepIdx))...)
			refBlob = append(refBlob, protoBytes(6, []byte(parentSid))...)
			if _, err := db.Exec(`INSERT INTO parent_references VALUES (0, ?)`, refBlob); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Parent has steps [0, 2, 4]; forked child cloned [0, 2, 4] (fork_step_idx = 4) and added unique steps [6, 8].
	createConvDB(parentPath, []int{0, 2, 4}, -1)
	createConvDB(childPath, []int{0, 2, 4, 6, 8}, 4)

	pastStamp := time.Now().Add(-15 * time.Minute).Truncate(time.Second)
	for _, p := range []string{parentPath, childPath, appDir} {
		if err := os.Chtimes(p, pastStamp, pastStamp); err != nil {
			t.Fatal(err)
		}
	}
	beforeDirStat, err := os.Stat(appDir)
	if err != nil {
		t.Fatal(err)
	}

	// 1. Direct pure-image reader skips <= fork_step_idx (4) when sourceSide == "APP"
	if gotFork := ReadMaxForkStepIdxFromSQLite(childPath); gotFork != 4 {
		t.Fatalf("expected ReadMaxForkStepIdxFromSQLite=4, got %d", gotFork)
	}
	childSteps := ReadModelStepsFromSQLite(childPath, "APP", nil)
	if len(childSteps) != 2 || childSteps[0].StepIdx != 6 || childSteps[1].StepIdx != 8 {
		t.Fatalf("expected child APP steps [6, 8], got %+v", childSteps)
	}

	// 2. Simulate a legacy StateStore that had ALREADY ingested cloned steps [0, 2, 4] with fork_dedup_ver=0,
	// then run SyncUsageStepsFromSourceDir and verify the 1-time upgrade pass purges [0, 2, 4] on childSid!
	st := &StateStore{}
	st.Init(stateDB)
	defer func() {
		st.mu.Lock()
		if st.db != nil {
			_ = st.db.Close()
		}
		st.mu.Unlock()
	}()

	childInfo, _ := os.Stat(childPath)
	legacySteps := ReadModelStepsFromSQLite(childPath, "HOST", nil) // HOST reads all 5 steps [0,2,4,6,8]
	for i := range legacySteps {
		legacySteps[i].SourceSide = "APP"
		legacySteps[i].SourceCategory = "AGY_APP"
		legacySteps[i].SourceLabel = "Antigravity App"
	}
	if _, err := st.persistUsageBatch("app:"+childSid, cursorMeta{
		dbSize:       childInfo.Size(),
		dbMtimeNs:    childInfo.ModTime().UnixNano(),
		lastStepIdx:  8,
		forkDedupVer: 0, // legacy pre-dedup cursor!
	}, legacySteps); err != nil {
		t.Fatal(err)
	}
	if preRoll, ok := st.QueryUsageAllTimeRollup(nil); !ok || preRoll.StepsCount != 5 {
		t.Fatalf("expected 5 legacy steps before upgrade sync, got %+v", preRoll)
	}

	// Run SyncUsageStepsFromSourceDir: ingests 3 parent steps AND purges 3 pre-fork child steps -> 3 + 2 = 5 unique steps!
	st.SyncUsageStepsFromSourceDir(appDir, "APP", "AGY_APP", "Antigravity App", nil)

	allSteps, ok := st.QueryAllUsageSteps(nil)
	if !ok || len(allSteps) != 5 {
		t.Fatalf("expected 5 deduplicated steps (3 parent + 2 post-fork child), got %d: %+v", len(allSteps), allSteps)
	}
	roll, okR := st.QueryUsageAllTimeRollup(nil)
	if !okR || roll.StepsCount != 5 {
		t.Fatalf("expected allTime StepsCount=5 after fork deduplication, got %+v", roll)
	}

	// Verify zero -wal/-shm side files and unchanged directory ModTime on writable 0755 appDir!
	afterDirStat, err := os.Stat(appDir)
	if err != nil {
		t.Fatal(err)
	}
	if !afterDirStat.ModTime().Equal(beforeDirStat.ModTime()) {
		t.Fatalf("writable appDir ModTime was mutated: %v -> %v", beforeDirStat.ModTime(), afterDirStat.ModTime())
	}
	entries, _ := os.ReadDir(appDir)
	if len(entries) != 2 {
		t.Fatalf("expected exact 2 .db files in appDir, got %d", len(entries))
	}
}
