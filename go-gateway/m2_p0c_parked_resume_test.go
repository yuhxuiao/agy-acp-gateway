package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func setupM2IsolatedEnv(t *testing.T) {
	t.Helper()
	tmp := t.TempDir()
	prevDB := GatewayStateDB
	origPort := GatewayPort
	if GlobalEngine != nil {
		GlobalEngine.StopEngine()
	}
	t.Cleanup(func() {
		if GlobalEngine != nil {
			GlobalEngine.StopEngine()
		}
		initConfig()
		InitGlobalStore(prevDB)
		GatewayPort = origPort
	})
	t.Setenv("AGY_ACP_BIN", fakeAcpBin)
	t.Setenv("AGY_PURE_API_HOME", filepath.Join(tmp, "pure"))
	t.Setenv("AGY_CONV_DIR", filepath.Join(tmp, "convs"))
	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "gateway.db"))
	t.Setenv("AGY_GW_LOG", filepath.Join(tmp, "gateway.log"))
	t.Setenv("AGY_TOS_TRIPPED_FILE", filepath.Join(tmp, "tos-tripped"))
	t.Setenv("AGY_ACP_RECORD", filepath.Join(tmp, "recordings"))
	initConfig()
	InitGlobalStore(GatewayStateDB)
}

func m2FindSession(gs *GatewayServer, sub string) *AcpProjectSession {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	for k, s := range gs.sessions {
		if strings.Contains(k, sub) {
			return s
		}
	}
	return nil
}

func m2PostMessagesWithSystem(t *testing.T, baseURL, sessionHdr, model, sys string, tools []map[string]any, msgs []map[string]any) (int, map[string]any, []byte) {
	t.Helper()
	if model == "" {
		model = "gemini-4-carbon"
	}
	payload := map[string]any{
		"model":      model,
		"max_tokens": 512,
		"messages":   msgs,
	}
	if sys != "" {
		payload["system"] = sys
	}
	if tools != nil {
		payload["tools"] = tools
	}
	rawBody, _ := json.Marshal(payload)
	req, err := http.NewRequest("POST", baseURL+"/v1/messages", bytes.NewReader(rawBody))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if sessionHdr != "" {
		req.Header.Set("x-claude-code-session-id", sessionHdr)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	outBytes, _ := io.ReadAll(resp.Body)
	var parsed map[string]any
	_ = json.Unmarshal(outBytes, &parsed)
	return resp.StatusCode, parsed, outBytes
}

func m2PostMessages(t *testing.T, baseURL, sessionHdr, model string, tools []map[string]any, msgs []map[string]any) (int, map[string]any, []byte) {
	t.Helper()
	return m2PostMessagesWithSystem(t, baseURL, sessionHdr, model, "", tools, msgs)
}

func m2DefaultBashTools() []map[string]any {
	return []map[string]any{
		{
			"name":         "Bash",
			"description":  "Run shell command",
			"input_schema": map[string]any{"type": "object", "properties": map[string]any{"command": map[string]any{"type": "string"}}},
		},
	}
}

func TestHotParkedInterruptInPlaceResumeNoRollover(t *testing.T) {
	setupM2IsolatedEnv(t)
	t.Setenv("AGY_TOOL_RELAY_MS", "2000")
	t.Setenv("AGY_MAX_TOOL_RELAYS", "6")
	t.Setenv("AGY_PARKED_TTL_SEC", "1800")

	baseURL, gs := startCarbonTestGateway(t)
	tools := m2DefaultBashTools()

	t.Run("stripped assistant tool_use resumes in-place on warm session with zero rollover", func(t *testing.T) {
		u0 := map[string]any{"role": "user", "content": "hello"}
		a0 := map[string]any{"role": "assistant", "content": "hi"}
		u1 := map[string]any{"role": "user", "content": "[FAKE_TOOL_CALL] run bash"}

		code1, r1, raw1 := m2PostMessages(t, baseURL, "m2-hot-strip", "", tools, []map[string]any{u0, a0, u1})
		if code1 != 200 || r1["stop_reason"] != "tool_use" {
			t.Fatalf("turn 1 expected tool_use, got code=%d body=%s", code1, string(raw1))
		}
		sess := m2FindSession(gs, "m2-hot-strip")
		if sess == nil {
			t.Fatal("expected session for m2-hot-strip")
		}
		sess.mu.Lock()
		sid1 := sess.SessionID
		sess.orphanCapStreak = 2
		msgCnt1 := sess.MessageCount
		sess.mu.Unlock()
		if sid1 == "" || msgCnt1 != 4 {
			t.Fatalf("unexpected post-turn1 state: sid=%q msgCnt=%d", sid1, msgCnt1)
		}

		// Client aborts tool call, strips unanswered assistant(tool_use), and sends u2 at len=4 == MessageCount.
		u2 := map[string]any{"role": "user", "content": "NEW_INSTRUCTION [ECHO_PROMPT]"}
		code2, r2, raw2 := m2PostMessages(t, baseURL, "m2-hot-strip", "", tools, []map[string]any{u0, a0, u1, u2})
		if code2 != 200 {
			t.Fatalf("turn 2 expected 200, got %d: %s", code2, string(raw2))
		}
		content2, _ := r2["content"].([]any)
		if len(content2) == 0 {
			t.Fatalf("empty turn 2 content: %s", string(raw2))
		}
		txt2, _ := content2[0].(map[string]any)["text"].(string)
		if !strings.Contains(txt2, "TOOL_DONE:") || !strings.Contains(txt2, "NEW_INSTRUCTION") {
			t.Fatalf("expected TOOL_DONE + NEW_INSTRUCTION in-place resume reply, got: %s", txt2)
		}
		if strings.Contains(txt2, "Conversation History Restored") {
			t.Fatalf("unexpected cold rollover replay in reply: %s", txt2)
		}

		sess.mu.Lock()
		sid2 := sess.SessionID
		rolls2 := sess.CompactionRollovers
		streak2 := sess.orphanCapStreak
		msgCnt2 := sess.MessageCount
		parked2 := len(sess.ParkedCalls)
		sess.mu.Unlock()
		if sid2 != sid1 || rolls2 != 0 || streak2 != 0 || msgCnt2 != 5 || parked2 != 0 {
			t.Fatalf("expected warm in-place state (sid=%q rolls=0 streak=0 msgCnt=5 parked=0), got sid=%q rolls=%d streak=%d msgCnt=%d parked=%d",
				sid1, sid2, rolls2, streak2, msgCnt2, parked2)
		}

		// Turn 3 sticky follow-up stays on sid1 (turn=2 in fakeacp).
		a2 := map[string]any{"role": "assistant", "content": content2}
		u3 := map[string]any{"role": "user", "content": "FOLLOW_UP_STICKY"}
		code3, r3, raw3 := m2PostMessages(t, baseURL, "m2-hot-strip", "", tools, []map[string]any{u0, a0, u1, u2, a2, u3})
		if code3 != 200 {
			t.Fatalf("turn 3 expected 200, got %d: %s", code3, string(raw3))
		}
		c3, _ := r3["content"].([]any)
		txt3, _ := c3[0].(map[string]any)["text"].(string)
		if !strings.Contains(txt3, "turn=2") {
			t.Fatalf("expected sticky turn=2 on same session after hot interrupt, got: %s", txt3)
		}
		sess.mu.Lock()
		sid3 := sess.SessionID
		rolls3 := sess.CompactionRollovers
		sess.mu.Unlock()
		if sid3 != sid1 || rolls3 != 0 {
			t.Fatalf("expected sid=%q rolls=0 after turn 3, got sid=%q rolls=%d", sid1, sid3, rolls3)
		}
	})

	t.Run("mid-relay interrupt via ws_await_result preserves session and avoids cancelled abort", func(t *testing.T) {
		t.Setenv("AGY_TOOL_RELAY_MS", "120")
		u0 := map[string]any{"role": "user", "content": "hello"}
		a0 := map[string]any{"role": "assistant", "content": "hi"}
		u1 := map[string]any{"role": "user", "content": "[FAKE_TOOL_CALL] [FAKE_FOLLOW_RELAYS] run long relay"}

		code1, r1, raw1 := m2PostMessages(t, baseURL, "m2-mid-relay", "", tools, []map[string]any{u0, a0, u1})
		if code1 != 200 || r1["stop_reason"] != "tool_use" {
			t.Fatalf("turn 1 expected tool_use, got code=%d body=%s", code1, string(raw1))
		}
		sess := m2FindSession(gs, "m2-mid-relay")
		if sess == nil {
			t.Fatal("expected session for m2-mid-relay")
		}
		sess.mu.Lock()
		sid1 := sess.SessionID
		sess.mu.Unlock()

		// Wait until at least 1 relay hop has fired so fakeacp is in its ws_await_result relay loop.
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			sess.mu.Lock()
			hops := sess.orphanRelayHops
			sess.mu.Unlock()
			if hops >= 1 {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}

		u2 := map[string]any{"role": "user", "content": "MID_RELAY_ABORT [ECHO_PROMPT]"}
		code2, r2, raw2 := m2PostMessages(t, baseURL, "m2-mid-relay", "", tools, []map[string]any{u0, a0, u1, u2})
		if code2 != 200 {
			t.Fatalf("turn 2 expected 200, got %d: %s", code2, string(raw2))
		}
		c2, _ := r2["content"].([]any)
		txt2, _ := c2[0].(map[string]any)["text"].(string)
		if !strings.Contains(txt2, "TOOL_DONE:") || !strings.Contains(txt2, "MID_RELAY_ABORT") {
			t.Fatalf("expected TOOL_DONE + MID_RELAY_ABORT without cancelled abort, got: %s", txt2)
		}
		sess.mu.Lock()
		sid2 := sess.SessionID
		rolls2 := sess.CompactionRollovers
		sess.mu.Unlock()
		if sid2 != sid1 || rolls2 != 0 {
			t.Fatalf("expected sid=%q rolls=0, got sid=%q rolls=%d", sid1, sid2, rolls2)
		}
	})

	t.Run("PromptInFlight false fallback uses fallbackPromptBlocks and clears parked state", func(t *testing.T) {
		u0 := map[string]any{"role": "user", "content": "warm start"}
		code1, _, raw1 := m2PostMessages(t, baseURL, "m2-fallback-blocks", "", tools, []map[string]any{u0})
		if code1 != 200 {
			t.Fatalf("turn 1 failed: %d %s", code1, string(raw1))
		}
		sess := m2FindSession(gs, "m2-fallback-blocks")
		if sess == nil {
			t.Fatal("expected session for m2-fallback-blocks")
		}
		sess.mu.Lock()
		sid1 := sess.SessionID
		sess.orphanCapStreak = 2
		sess.ParkedCalls["acp_fb_1"] = &ParkedMcpCall{
			Call:       ToolCallItem{ID: "acp_fb_1", Name: "Bash"},
			SessionID:  sid1,
			ResultChan: make(chan map[string]any, 1),
			Delivered:  true,
		}
		sess.PendingSiblingCalls = []*ParkedMcpCall{
			{Call: ToolCallItem{ID: "acp_fb_sib"}, SessionID: sid1, ResultChan: make(chan map[string]any, 1)},
		}
		sess.LastDeliveredCallIDs = []string{"acp_fb_1"}
		sess.ParkedSince = time.Now()
		sess.PromptInFlight = false
		sess.mu.Unlock()

		var fdNs atomic.Int64
		msgs := []map[string]any{
			u0,
			{"role": "assistant", "content": "ok"},
			{"role": "user", "content": "FALLBACK_USER_PROMPT"},
		}
		fallbackBlocks := []map[string]any{
			{"type": "text", "text": "SYS_DELTA_FALLBACK\n\nFALLBACK_USER_PROMPT [ECHO_PROMPT]\n\nFALLBACK_TAIL_DIRECTIVE"},
		}
		abortBlocks := []map[string]any{
			{"type": "text", "text": "Host user interrupted the pending tool execution"},
		}
		res, err := sess.resumeParkedTurnInternal(context.Background(), nil, msgs, nil, time.Now(), &fdNs, abortBlocks, fallbackBlocks)
		if err != nil {
			t.Fatalf("resumeParkedTurnInternal fallback failed: %v", err)
		}
		if !strings.Contains(res.Text, "FALLBACK_USER_PROMPT") || !strings.Contains(res.Text, "SYS_DELTA_FALLBACK") || !strings.Contains(res.Text, "FALLBACK_TAIL_DIRECTIVE") {
			t.Fatalf("expected fallbackPromptBlocks content preserved, got: %s", res.Text)
		}
		sess.mu.Lock()
		leftParked := len(sess.ParkedCalls)
		leftSibs := len(sess.PendingSiblingCalls)
		leftDelivered := len(sess.LastDeliveredCallIDs)
		parkedSinceZero := sess.ParkedSince.IsZero()
		streak := sess.orphanCapStreak
		sid2 := sess.SessionID
		sess.mu.Unlock()
		if sid2 != sid1 || leftParked != 0 || leftSibs != 0 || leftDelivered != 0 || !parkedSinceZero || streak != 0 {
			t.Fatalf("expected clean fallback state, got sid=%q parked=%d sibs=%d delivered=%d zeroSince=%v streak=%d",
				sid2, leftParked, leftSibs, leftDelivered, parkedSinceZero, streak)
		}
	})
}

func TestHibernatedParkedToolAbortedNoFalseCompaction(t *testing.T) {
	setupM2IsolatedEnv(t)
	t.Setenv("AGY_TOOL_RELAY_MS", "120")
	t.Setenv("AGY_MAX_TOOL_RELAYS", "1")
	t.Setenv("AGY_PARKED_TTL_SEC", "1800")

	baseURL, gs := startCarbonTestGateway(t)
	tools := m2DefaultBashTools()

	u0 := map[string]any{"role": "user", "content": "hello"}
	a0 := map[string]any{"role": "assistant", "content": "hi"}
	u1 := map[string]any{"role": "user", "content": "[FAKE_TOOL_CALL] [FAKE_FOLLOW_RELAYS] run slow tool"}

	code1, r1, raw1 := m2PostMessages(t, baseURL, "m2-hib-abort", "", tools, []map[string]any{u0, a0, u1})
	if code1 != 200 || r1["stop_reason"] != "tool_use" {
		t.Fatalf("turn 1 expected tool_use, got code=%d body=%s", code1, string(raw1))
	}
	sess := m2FindSession(gs, "m2-hib-abort")
	if sess == nil {
		t.Fatal("expected session for m2-hib-abort")
	}
	sess.mu.Lock()
	origSid := sess.SessionID
	sess.mu.Unlock()

	deadline := time.Now().Add(3 * time.Second)
	var curSid, hibSid string
	var hibParkedCnt, streak int
	for time.Now().Before(deadline) {
		sess.mu.Lock()
		curSid = sess.SessionID
		hibSid = sess.HibernatedSessionID
		hibParkedCnt = len(sess.HibernatedParkedCallIDs)
		streak = sess.orphanCapStreak
		sess.mu.Unlock()
		if curSid == "" && hibSid == origSid && hibParkedCnt == 1 && streak == 1 {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if curSid != "" || hibSid != origSid || hibParkedCnt != 1 || streak != 1 {
		t.Fatalf("expected hibernated parked session with streak=1, got curSid=%q hibSid=%q hibParked=%d streak=%d",
			curSid, hibSid, hibParkedCnt, streak)
	}

	// Client aborts tool, strips assistant(tool_use), sends u2 at len=4 == MessageCount.
	u2 := map[string]any{"role": "user", "content": "AFTER_HIBERNATE_NEW_PROMPT [ECHO_PROMPT] [ECHO_RELAY_META]"}
	code2, r2, raw2 := m2PostMessages(t, baseURL, "m2-hib-abort", "", tools, []map[string]any{u0, a0, u1, u2})
	if code2 != 200 {
		t.Fatalf("turn 2 expected 200, got %d: %s", code2, string(raw2))
	}
	c2, _ := r2["content"].([]any)
	txt2, _ := c2[0].(map[string]any)["text"].(string)
	if !strings.Contains(txt2, "resumes=1") || !strings.Contains(txt2, "sid="+origSid) || !strings.Contains(txt2, "AFTER_HIBERNATE_NEW_PROMPT") {
		t.Fatalf("expected session/resume on origSid=%q with new prompt, got: %s", origSid, txt2)
	}
	if strings.Contains(txt2, "Conversation History Restored") {
		t.Fatalf("false compaction rollover occurred after hibernated-parked abort: %s", txt2)
	}

	sess.mu.Lock()
	sid2 := sess.SessionID
	rolls2 := sess.CompactionRollovers
	hibParkedAfter := len(sess.HibernatedParkedCallIDs)
	msgCnt2 := sess.MessageCount
	streak2 := sess.orphanCapStreak
	sess.mu.Unlock()
	if sid2 != origSid || rolls2 != 0 || hibParkedAfter != 0 || msgCnt2 != 5 || streak2 != 0 {
		t.Fatalf("expected resumed warm state (sid=%q rolls=0 hibParked=0 msgCnt=5 streak=0), got sid=%q rolls=%d hibParked=%d msgCnt=%d streak=%d",
			origSid, sid2, rolls2, hibParkedAfter, msgCnt2, streak2)
	}

	// Verify real compaction (changed first user message) STILL rolls over even with HibernatedParkedCallIDs set.
	sess.HibernateWorker("reap_orphan_parked_relay_cap", true)
	sess.mu.Lock()
	sess.HibernatedParkedCallIDs = map[string]bool{"acp_dummy_hib": true}
	sess.mu.Unlock()
	compactedMsgs := []map[string]any{
		{"role": "user", "content": "COMPACTED_SUMMARY_REPLACED_FIRST_USER"},
		a0, u1, u2,
		{"role": "assistant", "content": c2},
		{"role": "user", "content": "post compaction turn"},
	}
	res3, err3 := sess.RunTurn(context.Background(), compactedMsgs, nil, "medium", nil, nil)
	if err3 != nil || res3.StopReason == "" {
		t.Fatalf("turn 3 compaction rollover failed: err=%v res=%+v", err3, res3)
	}
	sess.mu.Lock()
	sid3 := sess.SessionID
	rolls3 := sess.CompactionRollovers
	sess.mu.Unlock()
	if sid3 == "" || sid3 == origSid || rolls3 != 1 {
		t.Fatalf("expected real compaction (fpChanged) to roll over to new session, got sid3=%q orig=%q rolls=%d", sid3, origSid, rolls3)
	}
}

func TestOrphanCapStreakCircuitBreaker(t *testing.T) {
	setupM2IsolatedEnv(t)
	t.Setenv("AGY_TOOL_RELAY_MS", "120")
	t.Setenv("AGY_MAX_TOOL_RELAYS", "2")
	t.Setenv("AGY_PARKED_TTL_SEC", "1800")

	baseURL, gs := startCarbonTestGateway(t)
	tools := m2DefaultBashTools()

	// 1. Trigger orphan relay cap once via fakeacp -> orphanCapStreak == 1.
	u1 := map[string]any{"role": "user", "content": "[FAKE_TOOL_CALL] [FAKE_FOLLOW_RELAYS] orphan one"}
	code1, r1, raw1 := m2PostMessages(t, baseURL, "m2-streak-1", "", tools, []map[string]any{u1})
	if code1 != 200 || r1["stop_reason"] != "tool_use" {
		t.Fatalf("turn 1 expected tool_use, got %d %s", code1, string(raw1))
	}
	sess := m2FindSession(gs, "m2-streak-1")
	if sess == nil {
		t.Fatal("expected session for m2-streak-1")
	}
	sess.mu.Lock()
	origSid := sess.SessionID
	sess.mu.Unlock()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		sess.mu.Lock()
		curSid := sess.SessionID
		streak := sess.orphanCapStreak
		sess.mu.Unlock()
		if curSid == "" && streak == 1 {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	sess.mu.Lock()
	if sess.SessionID != "" || sess.orphanCapStreak != 1 {
		sess.mu.Unlock()
		t.Fatalf("expected hibernated with orphanCapStreak=1, got sid=%q streak=%d", sess.SessionID, sess.orphanCapStreak)
	}
	sess.mu.Unlock()

	// 2. Wake session on late tool_result -> resets orphanCapStreak to 0.
	c1, _ := r1["content"].([]any)
	tc1, _ := c1[0].(map[string]any)
	callID1, _ := tc1["id"].(string)
	code2, _, raw2 := m2PostMessages(t, baseURL, "m2-streak-1", "", tools, []map[string]any{
		u1,
		{"role": "assistant", "content": c1},
		{"role": "user", "content": []map[string]any{{"type": "tool_result", "tool_use_id": callID1, "content": "OK"}}},
	})
	if code2 != 200 {
		t.Fatalf("late tool_result wake failed: %d %s", code2, string(raw2))
	}
	sess.mu.Lock()
	if sess.SessionID != origSid || sess.orphanCapStreak != 0 {
		sess.mu.Unlock()
		t.Fatalf("expected late tool_result wake to reset orphanCapStreak=0 on %q, got sid=%q streak=%d", origSid, sess.SessionID, sess.orphanCapStreak)
	}
	sess.mu.Unlock()

	// 3. With orphanCapStreak >= 1 and AGY_MAX_TOOL_RELAYS=2, awaitParkedEntry drops maxRelays=0 and hibernates on 1st timer fire (0 relays).
	sUnit := NewAcpProjectSession("sid:m2-await-breaker:u:1", "/tmp", "")
	sUnit.SessionID = "sid-breaker-1"
	sUnit.MessageCount = 2
	sUnit.orphanCapStreak = 1
	entry := &ParkedMcpCall{
		Call:       ToolCallItem{ID: "acp_break_1", Name: "Bash"},
		SessionID:  "sid-breaker-1",
		ResultChan: make(chan map[string]any, 1),
		ParkedAt:   time.Now(),
	}
	sUnit.ParkedCalls["acp_break_1"] = entry
	res := sUnit.awaitParkedEntry(context.Background(), entry)
	resContent, _ := res["content"].([]map[string]any)
	if len(resContent) == 0 || !strings.Contains(resContent[0]["text"].(string), "Session hibernated.") {
		t.Fatalf("expected immediate hibernation on 1st interval when orphanCapStreak=1, got: %+v", res)
	}
	sUnit.mu.Lock()
	if sUnit.orphanCapStreak != 2 || sUnit.orphanRelayHops != 0 {
		sUnit.mu.Unlock()
		t.Fatalf("expected orphanCapStreak=2 after 2nd reap, got streak=%d hops=%d", sUnit.orphanCapStreak, sUnit.orphanRelayHops)
	}
	sUnit.mu.Unlock()

	// 4. AGY_MAX_TOOL_RELAYS=-1 (unlimited) stays unlimited even when orphanCapStreak >= 1.
	t.Setenv("AGY_MAX_TOOL_RELAYS", "-1")
	sUnlim := NewAcpProjectSession("sid:m2-await-unlim:u:1", "/tmp", "")
	sUnlim.SessionID = "sid-unlim-1"
	sUnlim.MessageCount = 2
	sUnlim.orphanCapStreak = 3
	entryUnlim := &ParkedMcpCall{
		Call:       ToolCallItem{ID: "acp_unlim_1", Name: "Bash"},
		SessionID:  "sid-unlim-1",
		ResultChan: make(chan map[string]any, 1),
		ParkedAt:   time.Now(),
	}
	sUnlim.ParkedCalls["acp_unlim_1"] = entryUnlim
	resUnlim := sUnlim.awaitParkedEntry(context.Background(), entryUnlim)
	resUnlimContent, _ := resUnlim["content"].([]map[string]any)
	if len(resUnlimContent) == 0 || !strings.Contains(resUnlimContent[0]["text"].(string), "still running in the host harness") {
		t.Fatalf("expected unlimited (-1) to relay despite orphanCapStreak=3, got: %+v", resUnlim)
	}
	sUnlim.mu.Lock()
	if sUnlim.SessionID == "" || sUnlim.orphanCapStreak != 3 {
		sUnlim.mu.Unlock()
		t.Fatalf("expected unlimited session to stay live, got sid=%q streak=%d", sUnlim.SessionID, sUnlim.orphanCapStreak)
	}
	sUnlim.mu.Unlock()

	// 5. Verify clean finalizeCompletedTurn and openFreshSession both reset orphanCapStreak to 0.
	sFin := NewAcpProjectSession("sid:m2-finalize-reset:u:1", "/tmp", "")
	sFin.SessionID = "sid-fin-1"
	sFin.orphanCapStreak = 4
	_, err := sFin.finalizeCompletedTurn(context.Background(), TurnResult{StopReason: "end_turn", Text: "ok"}, time.Now(), 0, []map[string]any{{"role": "user", "content": "hi"}}, "fp1", false, "sid-fin-1", "sticky_delta", "")
	if err != nil {
		t.Fatalf("finalizeCompletedTurn failed: %v", err)
	}
	sFin.mu.Lock()
	if sFin.orphanCapStreak != 0 {
		sFin.mu.Unlock()
		t.Fatalf("expected finalizeCompletedTurn to reset orphanCapStreak=0, got %d", sFin.orphanCapStreak)
	}
	sFin.mu.Unlock()

	if err := sess.openFreshSession(nil, "medium"); err != nil {
		t.Fatalf("openFreshSession failed: %v", err)
	}
	sess.mu.Lock()
	sess.orphanCapStreak = 5
	sess.mu.Unlock()
	if err := sess.openFreshSession(nil, "medium"); err != nil {
		t.Fatalf("second openFreshSession failed: %v", err)
	}
	sess.mu.Lock()
	if sess.orphanCapStreak != 0 {
		sess.mu.Unlock()
		t.Fatalf("expected openFreshSession to reset orphanCapStreak=0, got %d", sess.orphanCapStreak)
	}
	sess.mu.Unlock()
}

func TestEnforceWorkerCapDoesNotEvictFreshParkedAtZeroRelays(t *testing.T) {
	setupM2IsolatedEnv(t)
	t.Setenv("AGY_MAX_LIVE_WORKERS", "1")
	t.Setenv("AGY_MAX_TOOL_RELAYS", "0")
	t.Setenv("AGY_PARKED_TTL_SEC", "1800")
	initConfig()

	gs := NewGatewayServer()
	defer gs.Close()

	// 1. AGY_MAX_TOOL_RELAYS=0: fresh parked session with orphanRelayHops=0 must NOT be evicted.
	sParked := NewAcpProjectSession("sid:cap-zero-fresh:u:1", "/tmp", "")
	sParked.SessionID = "live-parked-0"
	sParked.MessageCount = 2
	sParked.LastUsedAt = time.Now()
	sParked.orphanRelayHops = 0
	sParked.ParkedCalls["call_1"] = &ParkedMcpCall{
		Call:       ToolCallItem{ID: "call_1", Name: "Bash"},
		SessionID:  "live-parked-0",
		ResultChan: make(chan map[string]any, 1),
	}
	incoming := NewAcpProjectSession("sid:cap-incoming:u:1", "/tmp", "")

	gs.mu.Lock()
	gs.sessions[sParked.ConvKey] = sParked
	gs.sessions[incoming.ConvKey] = incoming
	gs.mu.Unlock()

	gs.enforceWorkerCap(incoming)
	sParked.mu.Lock()
	if sParked.SessionID != "live-parked-0" {
		sParked.mu.Unlock()
		t.Fatalf("fresh parked worker with orphanRelayHops=0 was prematurely evicted at AGY_MAX_TOOL_RELAYS=0 (sid=%q)", sParked.SessionID)
	}
	// Once orphanRelayHops >= 1, enforceWorkerCap evicts it under pressure.
	sParked.orphanRelayHops = 1
	sParked.mu.Unlock()

	gs.enforceWorkerCap(incoming)
	sParked.mu.Lock()
	if sParked.SessionID != "" || sParked.HibernatedSessionID != "live-parked-0" || sParked.orphanCapStreak != 1 {
		sParked.mu.Unlock()
		t.Fatalf("expected parked worker with orphanRelayHops=1 to be hibernated with streak=1, got sid=%q hib=%q streak=%d",
			sParked.SessionID, sParked.HibernatedSessionID, sParked.orphanCapStreak)
	}
	sParked.mu.Unlock()

	// 2. AGY_MAX_TOOL_RELAYS=4 with orphanCapStreak=1: fresh parked (hops=0) stays live; hops=1 is evicted.
	t.Setenv("AGY_MAX_TOOL_RELAYS", "4")
	initConfig()
	sStreak := NewAcpProjectSession("sid:cap-streak:u:1", "/tmp", "")
	sStreak.SessionID = "live-parked-streak"
	sStreak.MessageCount = 2
	sStreak.LastUsedAt = time.Now()
	sStreak.orphanCapStreak = 1
	sStreak.orphanRelayHops = 0
	sStreak.ParkedCalls["call_2"] = &ParkedMcpCall{
		Call:       ToolCallItem{ID: "call_2", Name: "Bash"},
		SessionID:  "live-parked-streak",
		ResultChan: make(chan map[string]any, 1),
	}
	gs.mu.Lock()
	gs.sessions[sStreak.ConvKey] = sStreak
	gs.mu.Unlock()

	gs.enforceWorkerCap(incoming)
	sStreak.mu.Lock()
	if sStreak.SessionID != "live-parked-streak" {
		sStreak.mu.Unlock()
		t.Fatalf("fresh parked worker with orphanCapStreak=1 & hops=0 prematurely evicted (sid=%q)", sStreak.SessionID)
	}
	// Verify reapIdleWorkers also does not reap hops=0 even if lastOrphanRelayAt is old.
	sStreak.lastOrphanRelayAt = time.Now().Add(-120 * time.Second)
	sStreak.mu.Unlock()

	gs.reapIdleWorkers()
	sStreak.mu.Lock()
	if sStreak.SessionID != "live-parked-streak" {
		sStreak.mu.Unlock()
		t.Fatalf("reapIdleWorkers evicted hops=0 parked worker prematurely (sid=%q)", sStreak.SessionID)
	}
	sStreak.orphanRelayHops = 1
	sStreak.mu.Unlock()

	gs.reapIdleWorkers()
	sStreak.mu.Lock()
	if sStreak.SessionID != "" || sStreak.HibernatedSessionID != "live-parked-streak" || sStreak.orphanCapStreak != 2 {
		sStreak.mu.Unlock()
		t.Fatalf("expected reapIdleWorkers to evict orphanCapStreak=1 & hops=1, got sid=%q hib=%q streak=%d",
			sStreak.SessionID, sStreak.HibernatedSessionID, sStreak.orphanCapStreak)
	}
	sStreak.mu.Unlock()
}

func TestResumeParkedTurnTimedTransientWarmProtection(t *testing.T) {
	setupM2IsolatedEnv(t)

	// Protect global ToS breaker state across [FAKE_TOS_SOFT_ERROR] subtest.
	oldFile := TosTrippedFile
	tosMu.Lock()
	oldMem, oldReason, oldPersisted := tosTrippedMem.Load(), tosReasonMem, tosPersisted
	tosTrippedMem.Store(false)
	tosReasonMem = ""
	tosPersisted = false
	tosMu.Unlock()
	t.Cleanup(func() {
		if TosTrippedFile != "" {
			_ = os.Remove(TosTrippedFile)
		}
		TosTrippedFile = oldFile
		tosMu.Lock()
		tosTrippedMem.Store(oldMem)
		tosReasonMem = oldReason
		tosPersisted = oldPersisted
		tosMu.Unlock()
	})

	baseURL, gs := startCarbonTestGateway(t)

	// Part A: runNewPromptResume (PromptInFlight == false) via fakeacp on tNum >= 2.
	u0 := map[string]any{"role": "user", "content": "warm baseline"}
	code1, _, raw1 := m2PostMessages(t, baseURL, "m2-warm-resume", "", m2DefaultBashTools(), []map[string]any{u0})
	if code1 != 200 {
		t.Fatalf("baseline turn failed: %d %s", code1, string(raw1))
	}
	sess := m2FindSession(gs, "m2-warm-resume")
	if sess == nil {
		t.Fatal("expected session for m2-warm-resume")
	}
	sess.mu.Lock()
	origSid := sess.SessionID
	origMsgCnt := sess.MessageCount
	sess.mu.Unlock()
	if origSid == "" || origMsgCnt != 2 {
		t.Fatalf("unexpected baseline state: sid=%q msgCnt=%d", origSid, origMsgCnt)
	}

	// 1. Transient 429 & 529 via runNewPromptResume must preserve warm SessionID & MessageCount.
	for _, marker := range []string{"[FAKE_SOFT_429]", "[FAKE_AGY159_RAW_529]"} {
		sess.mu.Lock()
		sess.ParkedCalls["acp_warm_"+marker] = &ParkedMcpCall{
			Call:       ToolCallItem{ID: "acp_warm_" + marker, Name: "Bash"},
			SessionID:  origSid,
			ResultChan: make(chan map[string]any, 1),
		}
		sess.PromptInFlight = false
		sess.mu.Unlock()

		matched := []ToolResultInput{{ToolCallID: "acp_warm_" + marker, Content: marker}}
		res, err := sess.resumeParkedTurn(context.Background(), matched, []map[string]any{
			u0,
			{"role": "assistant", "content": "tool"},
			{"role": "user", "content": "tool_result"},
		}, nil)
		if err != nil {
			t.Fatalf("expected soft-error TurnResult without rpc error for %s, got err=%v", marker, err)
		}
		if se := DetectAntigravitySoftError(res.Text); se == nil {
			t.Fatalf("expected DetectAntigravitySoftError != nil for %s, got text=%q", marker, res.Text)
		}
		sess.mu.Lock()
		curSid := sess.SessionID
		curMsgCnt := sess.MessageCount
		sess.mu.Unlock()
		if curSid != origSid || curMsgCnt != origMsgCnt {
			t.Fatalf("transient %s destroyed warm parked session: sid=%q (want %q) msgCnt=%d (want %d)",
				marker, curSid, origSid, curMsgCnt, origMsgCnt)
		}
	}

	// 2. Non-transient 400 and FailedNotice must hibernate on parked_mcp_resume.
	sess.mu.Lock()
	sess.ParkedCalls["acp_400"] = &ParkedMcpCall{
		Call:       ToolCallItem{ID: "acp_400", Name: "Bash"},
		SessionID:  origSid,
		ResultChan: make(chan map[string]any, 1),
	}
	sess.PromptInFlight = false
	sess.mu.Unlock()
	res400, err400 := sess.resumeParkedTurn(context.Background(), []ToolResultInput{{ToolCallID: "acp_400", Content: "[FAKE_AGY159_RAW_400]"}}, []map[string]any{u0}, nil)
	if err400 != nil {
		t.Fatalf("unexpected error for 400 notice: %v", err400)
	}
	if se := DetectAntigravitySoftError(res400.Text); se == nil || se.StatusCode != 400 {
		t.Fatalf("expected 400 soft error, got %+v text=%q", se, res400.Text)
	}
	sess.mu.Lock()
	if sess.SessionID != "" {
		sess.mu.Unlock()
		t.Fatalf("expected 400 non-transient error to hibernate session, got sid=%q", sess.SessionID)
	}
	sess.mu.Unlock()

	// Re-open fresh session to test FailedNotice & ToS hibernation.
	if err := sess.openFreshSession(nil, "medium"); err != nil {
		t.Fatalf("openFreshSession failed: %v", err)
	}
	sess.mu.Lock()
	sess.MessageCount = 2
	sidBeforeLate := sess.SessionID
	sess.ParkedCalls["acp_late_429"] = &ParkedMcpCall{
		Call:       ToolCallItem{ID: "acp_late_429", Name: "Bash"},
		SessionID:  sidBeforeLate,
		ResultChan: make(chan map[string]any, 1),
	}
	sess.mu.Unlock()
	_, errLate := sess.resumeParkedTurn(context.Background(), []ToolResultInput{{ToolCallID: "acp_late_429", Content: "[FAKE_AGY159_LATE_429]"}}, []map[string]any{u0}, nil)
	if errLate == nil {
		t.Fatal("expected error from FailedNotice on [FAKE_AGY159_LATE_429]")
	}
	sess.mu.Lock()
	if sess.SessionID != "" {
		sess.mu.Unlock()
		t.Fatalf("expected FailedNotice on parked_mcp_resume to hibernate session, got sid=%q", sess.SessionID)
	}
	sess.mu.Unlock()

	// ToS soft error must hibernate too (never transient warm).
	if err := sess.openFreshSession(nil, "medium"); err != nil {
		t.Fatalf("openFreshSession failed: %v", err)
	}
	sess.mu.Lock()
	sess.MessageCount = 2
	sidBeforeTos := sess.SessionID
	sess.ParkedCalls["acp_tos"] = &ParkedMcpCall{
		Call:       ToolCallItem{ID: "acp_tos", Name: "Bash"},
		SessionID:  sidBeforeTos,
		ResultChan: make(chan map[string]any, 1),
	}
	sess.mu.Unlock()
	resTos, errTos := sess.resumeParkedTurn(context.Background(), []ToolResultInput{{ToolCallID: "acp_tos", Content: "[FAKE_TOS_SOFT_ERROR]"}}, []map[string]any{u0}, nil)
	if errTos != nil {
		t.Fatalf("unexpected err for ToS soft error: %v", errTos)
	}
	if se := DetectAntigravitySoftError(resTos.Text); se == nil || se.ErrorType != "tos_circuit_breaker" {
		t.Fatalf("expected tos_circuit_breaker soft error, got %+v text=%q", se, resTos.Text)
	}
	sess.mu.Lock()
	if sess.SessionID != "" {
		sess.mu.Unlock()
		t.Fatalf("expected ToS error to hibernate session, got sid=%q", sess.SessionID)
	}
	sess.mu.Unlock()

	// Clear global ToS breaker (both memory and marker file) before Part B active select tests.
	if TosTrippedFile != "" {
		_ = os.Remove(TosTrippedFile)
	}
	tosMu.Lock()
	tosTrippedMem.Store(false)
	tosReasonMem = ""
	tosPersisted = false
	tosMu.Unlock()

	// Part B: active select branch (PromptInFlight == true) preserves warm session on 503 AND
	// propagates isCompactionReq, orphanCapStreak=0, and recordRecentCompletedTurn on clean completion.
	sAct := NewAcpProjectSession("sid:m2-active-select:u:1", "/tmp", "")
	sAct.SessionID = "sid-active-warm"
	sAct.MessageCount = 2
	sAct.PromptInFlight = true
	parked503 := &ParkedMcpCall{
		Call:       ToolCallItem{ID: "acp_act_503", Name: "Bash"},
		SessionID:  "sid-active-warm",
		ResultChan: make(chan map[string]any, 1),
	}
	sAct.ParkedCalls["acp_act_503"] = parked503

	go func() {
		<-parked503.ResultChan
		sAct.mu.Lock()
		turn := sAct.activeTurn
		sAct.mu.Unlock()
		turn.resolveChan <- TurnResult{
			StopReason: "end_turn",
			Text:       "⚠ Agent execution terminated due to error: UNAVAILABLE (code 503): No capacity available for model gemini-4-carbon",
		}
	}()

	res503, err503 := sAct.resumeParkedTurn(context.Background(), []ToolResultInput{{ToolCallID: "acp_act_503", Content: "ok"}}, []map[string]any{u0}, nil)
	if err503 != nil {
		t.Fatalf("active select 503 failed: %v", err503)
	}
	if se := DetectAntigravitySoftError(res503.Text); se == nil || se.StatusCode != 529 {
		t.Fatalf("expected 529 SoftError from UNAVAILABLE 503, got %+v text=%q", se, res503.Text)
	}
	sAct.mu.Lock()
	if sAct.SessionID != "sid-active-warm" || sAct.MessageCount != 2 {
		sAct.mu.Unlock()
		t.Fatalf("active select 503 should preserve warm session, got sid=%q msgCnt=%d", sAct.SessionID, sAct.MessageCount)
	}

	// Clean completion via active select propagates isCompactionReq, resets orphanCapStreak=0, and records recent completed turn.
	parkedOk := &ParkedMcpCall{
		Call:       ToolCallItem{ID: "acp_act_ok", Name: "Bash"},
		SessionID:  "sid-active-warm",
		ResultChan: make(chan map[string]any, 1),
	}
	sAct.ParkedCalls["acp_act_ok"] = parkedOk
	sAct.orphanCapStreak = 3
	sAct.coalesceHandoff = true
	sAct.mu.Unlock()

	go func() {
		<-parkedOk.ResultChan
		sAct.mu.Lock()
		turn := sAct.activeTurn
		sAct.mu.Unlock()
		turn.resolveChan <- TurnResult{
			StopReason: "end_turn",
			Text:       "COMPLETED_COMPACTION_SUMMARY",
		}
	}()

	metaCtx := withRequestFingerprint(context.Background(), "req-fp-active-compaction")
	metaCtx = withTurnDetachMeta(metaCtx, &turnDetachMeta{
		sid:             "sid-active-warm",
		mode:            "parked_mcp_resume",
		turnStart:       time.Now(),
		msgCount:        3,
		firstUserFp:     "fp-first-user",
		isCompactionReq: true,
	})
	msgsOk := []map[string]any{u0, {"role": "assistant", "content": "a"}, {"role": "user", "content": "u"}}
	resOk, errOk := sAct.resumeParkedTurn(metaCtx, []ToolResultInput{{ToolCallID: "acp_act_ok", Content: "ok"}}, msgsOk, nil)
	if errOk != nil || !strings.Contains(resOk.Text, "COMPLETED_COMPACTION_SUMMARY") {
		t.Fatalf("active select clean completion failed: res=%+v err=%v", resOk, errOk)
	}
	sAct.mu.Lock()
	if !sAct.ForceRolloverNext || sAct.ForceRolloverReason != "rollover_compaction" || sAct.orphanCapStreak != 0 || sAct.MessageCount != 4 || sAct.HistoryFingerprint != "fp-first-user" {
		sAct.mu.Unlock()
		t.Fatalf("unexpected active select finalization state: force=%v why=%q streak=%d msgCnt=%d fp=%q",
			sAct.ForceRolloverNext, sAct.ForceRolloverReason, sAct.orphanCapStreak, sAct.MessageCount, sAct.HistoryFingerprint)
	}
	sAct.mu.Unlock()
	replayed, okReplay := sAct.tryReplayRecentCompletedTurn("req-fp-active-compaction", nil)
	if !okReplay || replayed.Text != "COMPLETED_COMPACTION_SUMMARY" {
		t.Fatalf("expected recordRecentCompletedTurn replay on parked_mcp_resume, got ok=%v res=%+v", okReplay, replayed)
	}
}

func TestHotParkedPlannerSiblingBatchInterruptDeliversPromptToAwaitedCall(t *testing.T) {
	setupM2IsolatedEnv(t)

	sess := NewAcpProjectSession("sid:m2-sib-hot:u:1", "/tmp", "")
	sid := "00000000-0000-4000-8000-000000009901"
	sess.SessionID = sid
	sess.ModelID = "gemini-4-carbon"
	sess.ThoughtLevel = "high"
	sess.MessageCount = 4
	sess.lastTurnAssistant = true
	sess.HistoryFingerprint = computeFirstUserFingerprint([]map[string]any{
		{"role": "user", "content": "hello"},
		{"role": "assistant", "content": "hi"},
		{"role": "user", "content": "run two tools"},
	})
	readMcp := mcpToolName("Read")
	writeMcp := mcpToolName("Write")
	sess.DeclaredMcpTools = map[string]bool{"Read": true, "Write": true}
	sess.McpToHostMap = map[string]string{readMcp: "Read", writeMcp: "Write"}
	sess.ToolsCatalog = []ToolDef{{Name: "Read", McpName: readMcp}, {Name: "Write", McpName: writeMcp}}
	sess.ToolFingerprint = toolCatalogFingerprint(sess.ToolsCatalog)

	dbPath := filepath.Join(ConversationsDir(), sid+".db")
	writeTestPlannerStepSQLite(t, dbPath, 1, buildTestPlannerStepPayload([][3]string{
		{"call_1", readMcp, `{"file_path":"/a.txt"}`},
		{"call_2", writeMcp, `{"file_path":"/b.txt","content":"hi"}`},
	}))

	turn1 := newActiveTurnState(nil, func() {})
	sess.mu.Lock()
	sess.PromptInFlight = true
	sess.activeTurn = turn1
	sess.mu.Unlock()

	awaitedGot := make(chan map[string]any, 1)
	go func() {
		// Real PlannerResponse sibling batch: call #1 awaits in executeMcpToolCall while call #2 is
		// pre-dispatched into ParkedCalls + PendingSiblingCalls + LastDeliveredCallIDs[1].
		r1 := sess.executeMcpToolCall(context.Background(), "Read", map[string]any{"file_path": "/a.txt"})
		awaitedGot <- r1
		// Simulate upstream receiving the interrupt abortContentBlocks and finishing the active turn.
		sess.mu.Lock()
		sess.PromptInFlight = false
		cur := sess.activeTurn
		sess.activeTurn = nil
		sess.mu.Unlock()
		if cur != nil {
			cur.mu.Lock()
			cur.resolved = true
			cur.mu.Unlock()
			cur.closeDelta()
			cur.resolveChan <- TurnResult{
				StopReason: "end_turn",
				Text:       "SIBLING_INTERRUPTED_OK:" + fmt.Sprint(r1["content"]),
			}
		}
	}()

	var res1 TurnResult
	select {
	case res1 = <-turn1.resolveChan:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for PlannerResponse batched tool_use turn")
	}
	if res1.StopReason != "tool_use" || len(res1.ToolCalls) != 2 {
		t.Fatalf("expected 2 batched sibling tool calls, got %+v", res1)
	}

	sess.mu.Lock()
	if len(sess.ParkedCalls) != 2 || len(sess.PendingSiblingCalls) != 1 || len(sess.LastDeliveredCallIDs) != 2 {
		sess.mu.Unlock()
		t.Fatalf("expected 2 ParkedCalls, 1 PendingSiblingCalls, 2 LastDeliveredCallIDs; got %d/%d/%d",
			len(sess.ParkedCalls), len(sess.PendingSiblingCalls), len(sess.LastDeliveredCallIDs))
	}
	pendingSibID := sess.PendingSiblingCalls[0].Call.ID
	awaitedID := res1.ToolCalls[0].ID
	if pendingSibID == awaitedID {
		sess.mu.Unlock()
		t.Fatalf("expected pending sibling ID (%q) != awaited ID (%q)", pendingSibID, awaitedID)
	}
	pendingSibChan := sess.PendingSiblingCalls[0].ResultChan
	sess.mu.Unlock()

	// Client aborts both tools and sends updated user prompt.
	msgs2 := []map[string]any{
		{"role": "user", "content": "hello"},
		{"role": "assistant", "content": "hi"},
		{"role": "user", "content": "run two tools"},
		{"role": "user", "content": "CRITICAL_UPDATED_USER_PROMPT"},
	}
	res2, err2 := sess.RunTurn(context.Background(), msgs2, sess.ToolsCatalog, "high", nil, nil)
	if err2 != nil {
		t.Fatalf("RunTurn hot-parked sibling interrupt failed: %v", err2)
	}
	gotAwaited := <-awaitedGot
	awaitedTxt := fmt.Sprint(gotAwaited["content"])
	if !strings.Contains(awaitedTxt, "CRITICAL_UPDATED_USER_PROMPT") {
		t.Fatalf("actively awaited call did NOT receive updated user prompt; got: %s", awaitedTxt)
	}
	if !strings.Contains(res2.Text, "CRITICAL_UPDATED_USER_PROMPT") {
		t.Fatalf("expected final turn output to reflect awaited call's prompt, got: %s", res2.Text)
	}
	// Verify the un-awaited PendingSiblingCalls entry only received the generic stub, NOT abortContentBlocks.
	select {
	case sibMsg := <-pendingSibChan:
		if strings.Contains(fmt.Sprint(sibMsg["content"]), "CRITICAL_UPDATED_USER_PROMPT") {
			t.Fatalf("un-awaited PendingSiblingCalls entry stole abortContentBlocks: %+v", sibMsg)
		}
	default:
		t.Fatal("expected un-awaited PendingSiblingCalls entry to receive short interrupt notice")
	}

	sess.mu.Lock()
	if sess.SessionID != sid || sess.CompactionRollovers != 0 || len(sess.ParkedCalls) != 0 || len(sess.PendingSiblingCalls) != 0 {
		sess.mu.Unlock()
		t.Fatalf("unexpected post-interrupt state: sid=%q rolls=%d parked=%d sibs=%d",
			sess.SessionID, sess.CompactionRollovers, len(sess.ParkedCalls), len(sess.PendingSiblingCalls))
	}
	sess.mu.Unlock()
}

func TestTurn1StrippedAssistantHotAndHibernatedNoRollover(t *testing.T) {
	setupM2IsolatedEnv(t)
	t.Setenv("AGY_TOOL_RELAY_MS", "2000")
	t.Setenv("AGY_MAX_TOOL_RELAYS", "6")
	t.Setenv("AGY_PARKED_TTL_SEC", "1800")

	baseURL, gs := startCarbonTestGateway(t)
	tools := m2DefaultBashTools()

	t.Run("hot-parked turn-1 [u0] -> strip a0 -> [u0, u1] with SystemText drift -> turn 3 sticky", func(t *testing.T) {
		u0 := map[string]any{"role": "user", "content": "[FAKE_TOOL_CALL] run bash"}
		code1, r1, raw1 := m2PostMessagesWithSystem(t, baseURL, "m2-turn1-hot", "", "SYS_V1", tools, []map[string]any{u0})
		if code1 != 200 || r1["stop_reason"] != "tool_use" {
			t.Fatalf("turn 1 expected tool_use, got %d %s", code1, string(raw1))
		}
		sess := m2FindSession(gs, "m2-turn1-hot")
		if sess == nil {
			t.Fatal("expected session for m2-turn1-hot")
		}
		sess.mu.Lock()
		sid1 := sess.SessionID
		msgCnt1 := sess.MessageCount
		sess.mu.Unlock()
		if sid1 == "" || msgCnt1 != 2 {
			t.Fatalf("expected sid!=empty and msgCnt=2, got sid=%q msgCnt=%d", sid1, msgCnt1)
		}

		// Turn 2: strip a0 and send [u0, u1] WITH SystemText drift (SYS_V1 -> SYS_V2).
		u1 := map[string]any{"role": "user", "content": "TURN1_HOT_NEW [ECHO_PROMPT]"}
		code2, r2, raw2 := m2PostMessagesWithSystem(t, baseURL, "m2-turn1-hot", "", "SYS_V2", tools, []map[string]any{u0, u1})
		if code2 != 200 {
			t.Fatalf("turn 2 expected 200, got %d %s", code2, string(raw2))
		}
		c2, _ := r2["content"].([]any)
		txt2, _ := c2[0].(map[string]any)["text"].(string)
		if !strings.Contains(txt2, "TOOL_DONE:") || !strings.Contains(txt2, "TURN1_HOT_NEW") {
			t.Fatalf("expected TOOL_DONE + TURN1_HOT_NEW in-place resume, got: %s", txt2)
		}
		if strings.Contains(txt2, "Conversation History Restored") {
			t.Fatalf("unexpected rollover replay on Turn 2: %s", txt2)
		}
		sess.mu.Lock()
		sid2 := sess.SessionID
		rolls2 := sess.CompactionRollovers
		msgCnt2 := sess.MessageCount
		sess.mu.Unlock()
		if sid2 != sid1 || rolls2 != 0 || msgCnt2 != 3 {
			t.Fatalf("expected warm state after Turn 2 (sid=%q rolls=0 msgCnt=3), got sid=%q rolls=%d msgCnt=%d", sid1, sid2, rolls2, msgCnt2)
		}

		// Turn 3: [u0, u1, a1, u2] MUST stay on exact same live session (turn=2, 0 rollovers).
		a1 := map[string]any{"role": "assistant", "content": c2}
		u2 := map[string]any{"role": "user", "content": "TURN3_STICKY"}
		code3, r3, raw3 := m2PostMessagesWithSystem(t, baseURL, "m2-turn1-hot", "", "SYS_V2", tools, []map[string]any{u0, u1, a1, u2})
		if code3 != 200 {
			t.Fatalf("turn 3 expected 200, got %d %s", code3, string(raw3))
		}
		c3, _ := r3["content"].([]any)
		txt3, _ := c3[0].(map[string]any)["text"].(string)
		if !strings.Contains(txt3, "turn=2") {
			t.Fatalf("expected Turn 3 sticky turn=2 on same session, got: %s", txt3)
		}
		sess.mu.Lock()
		if sess.SessionID != sid1 || sess.CompactionRollovers != 0 {
			sess.mu.Unlock()
			t.Fatalf("expected Turn 3 sid=%q rolls=0, got sid=%q rolls=%d", sid1, sess.SessionID, sess.CompactionRollovers)
		}
		sess.mu.Unlock()
		if cnt := countMatchingSessions(gs, "m2-turn1-hot"); cnt != 1 {
			t.Fatalf("expected exactly 1 session (no duplicate cold-start), got %d", cnt)
		}
	})

	t.Run("hibernated-parked turn-1 [u0] -> strip a0 -> [u0, u1] with SystemText drift -> turn 3 sticky", func(t *testing.T) {
		t.Setenv("AGY_TOOL_RELAY_MS", "120")
		t.Setenv("AGY_MAX_TOOL_RELAYS", "1")

		u0 := map[string]any{"role": "user", "content": "[FAKE_TOOL_CALL] [FAKE_FOLLOW_RELAYS] slow turn1 tool"}
		code1, r1, raw1 := m2PostMessagesWithSystem(t, baseURL, "m2-turn1-hib", "", "HIB_SYS_V1", tools, []map[string]any{u0})
		if code1 != 200 || r1["stop_reason"] != "tool_use" {
			t.Fatalf("turn 1 expected tool_use, got %d %s", code1, string(raw1))
		}
		sess := m2FindSession(gs, "m2-turn1-hib")
		if sess == nil {
			t.Fatal("expected session for m2-turn1-hib")
		}
		sess.mu.Lock()
		origSid := sess.SessionID
		sess.mu.Unlock()

		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			sess.mu.Lock()
			done := sess.SessionID == "" && sess.HibernatedSessionID == origSid && len(sess.HibernatedParkedCallIDs) == 1
			sess.mu.Unlock()
			if done {
				break
			}
			time.Sleep(15 * time.Millisecond)
		}
		sess.mu.Lock()
		if sess.SessionID != "" || sess.HibernatedSessionID != origSid || len(sess.HibernatedParkedCallIDs) != 1 {
			sess.mu.Unlock()
			t.Fatalf("expected hibernated parked session, got sid=%q hib=%q hibParked=%d",
				sess.SessionID, sess.HibernatedSessionID, len(sess.HibernatedParkedCallIDs))
		}
		sess.mu.Unlock()

		// Turn 2: strip a0 and send [u0, u1] WITH SystemText drift.
		u1 := map[string]any{"role": "user", "content": "TURN1_HIB_NEW [ECHO_PROMPT] [ECHO_RELAY_META]"}
		code2, r2, raw2 := m2PostMessagesWithSystem(t, baseURL, "m2-turn1-hib", "", "HIB_SYS_V2", tools, []map[string]any{u0, u1})
		if code2 != 200 {
			t.Fatalf("turn 2 expected 200, got %d %s", code2, string(raw2))
		}
		c2, _ := r2["content"].([]any)
		txt2, _ := c2[0].(map[string]any)["text"].(string)
		if !strings.Contains(txt2, "resumes=1") || !strings.Contains(txt2, "sid="+origSid) || !strings.Contains(txt2, "TURN1_HIB_NEW") {
			t.Fatalf("expected session/resume on origSid=%q with TURN1_HIB_NEW, got: %s", origSid, txt2)
		}
		if strings.Contains(txt2, "Conversation History Restored") {
			t.Fatalf("unexpected compaction rollover on Turn 2: %s", txt2)
		}
		sess.mu.Lock()
		if sess.SessionID != origSid || sess.CompactionRollovers != 0 || sess.MessageCount != 3 {
			sess.mu.Unlock()
			t.Fatalf("expected resumed state (sid=%q rolls=0 msgCnt=3), got sid=%q rolls=%d msgCnt=%d",
				origSid, sess.SessionID, sess.CompactionRollovers, sess.MessageCount)
		}
		sess.mu.Unlock()

		// Turn 3: [u0, u1, a1, u2] stays on exact same resumed session (turn=2, 0 rollovers).
		a1 := map[string]any{"role": "assistant", "content": c2}
		u2 := map[string]any{"role": "user", "content": "TURN3_HIB_STICKY"}
		code3, r3, raw3 := m2PostMessagesWithSystem(t, baseURL, "m2-turn1-hib", "", "HIB_SYS_V2", tools, []map[string]any{u0, u1, a1, u2})
		if code3 != 200 {
			t.Fatalf("turn 3 expected 200, got %d %s", code3, string(raw3))
		}
		c3, _ := r3["content"].([]any)
		txt3, _ := c3[0].(map[string]any)["text"].(string)
		if !strings.Contains(txt3, "turn=3") || strings.Contains(txt3, "Conversation History Restored") {
			t.Fatalf("expected Turn 3 sticky turn=3 on resumed session (1=hibernated, 2=resumed, 3=sticky), got: %s", txt3)
		}
		sess.mu.Lock()
		if sess.SessionID != origSid || sess.CompactionRollovers != 0 {
			sess.mu.Unlock()
			t.Fatalf("expected Turn 3 sid=%q rolls=0, got sid=%q rolls=%d", origSid, sess.SessionID, sess.CompactionRollovers)
		}
		sess.mu.Unlock()
		if cnt := countMatchingSessions(gs, "m2-turn1-hib"); cnt != 1 {
			t.Fatalf("expected exactly 1 session (no duplicate cold-start), got %d", cnt)
		}
	})
}

func countMatchingSessions(gs *GatewayServer, sub string) int {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	cnt := 0
	for k := range gs.sessions {
		if strings.Contains(k, sub) {
			cnt++
		}
	}
	return cnt
}

func TestHotParkedMultiStepToolLoopStrippedAssistantNoRollover(t *testing.T) {
	setupM2IsolatedEnv(t)
	t.Setenv("AGY_TOOL_RELAY_MS", "2000")
	t.Setenv("AGY_MAX_TOOL_RELAYS", "6")
	t.Setenv("AGY_PARKED_TTL_SEC", "1800")

	baseURL, gs := startCarbonTestGateway(t)
	tools := m2DefaultBashTools()

	// Multi-step tool loop: history already contains [u0, a0(tool_use_1), u1(tool_result_1)],
	// and upstream ACP parks a1(tool_use_2) on step 2 (tNum==1 in fakeacp).
	u0 := map[string]any{"role": "user", "content": "step 1"}
	a0 := map[string]any{
		"role": "assistant",
		"content": []map[string]any{
			{"type": "tool_use", "id": "call_step1_done", "name": "Bash", "input": map[string]any{"command": "echo step1"}},
		},
	}
	u1 := map[string]any{
		"role": "user",
		"content": []map[string]any{
			{"type": "tool_result", "tool_use_id": "call_step1_done", "content": "step1 ok"},
			{"type": "text", "text": "[FAKE_TOOL_CALL] step 2"},
		},
	}
	code1, r1, raw1 := m2PostMessages(t, baseURL, "m2-multistep-strip", "", tools, []map[string]any{u0, a0, u1})
	if code1 != 200 || r1["stop_reason"] != "tool_use" {
		t.Fatalf("step 2 expected tool_use, got %d %s", code1, string(raw1))
	}
	sess := m2FindSession(gs, "m2-multistep-strip")
	if sess == nil {
		t.Fatal("expected session for m2-multistep-strip")
	}
	sess.mu.Lock()
	origSid := sess.SessionID
	if origSid == "" || sess.MessageCount != 4 || !sess.lastTurnAssistant {
		sess.mu.Unlock()
		t.Fatalf("unexpected step 2 state: sid=%q msgCnt=%d lastAsst=%v", origSid, sess.MessageCount, sess.lastTurnAssistant)
	}
	sess.mu.Unlock()

	// Step 3: Client aborts tool_use_2, strips unanswered a1, and sends u2 after earlier u1(tool_result_1).
	u2 := map[string]any{"role": "user", "content": "ABORT_STEP2_NEW_PROMPT [ECHO_PROMPT]"}
	code3, r3, raw3 := m2PostMessages(t, baseURL, "m2-multistep-strip", "", tools, []map[string]any{u0, a0, u1, u2})
	if code3 != 200 {
		t.Fatalf("step 3 expected 200, got %d %s", code3, string(raw3))
	}
	c3, _ := r3["content"].([]any)
	txt3, _ := c3[0].(map[string]any)["text"].(string)
	if !strings.Contains(txt3, "TOOL_DONE:") || !strings.Contains(txt3, "ABORT_STEP2_NEW_PROMPT") {
		t.Fatalf("expected multi-step hot interrupt in-place resume, got: %s", txt3)
	}
	if strings.Contains(txt3, "Conversation History Restored") {
		t.Fatalf("false rollover on multi-step stripped assistant: %s", txt3)
	}
	sess.mu.Lock()
	if sess.SessionID != origSid || sess.CompactionRollovers != 0 || sess.MessageCount != 5 {
		sess.mu.Unlock()
		t.Fatalf("expected same sid=%q rolls=0 msgCnt=5, got sid=%q rolls=%d msgCnt=%d",
			origSid, sess.SessionID, sess.CompactionRollovers, sess.MessageCount)
	}
	sess.mu.Unlock()
}

func TestAwaitParkedEntryWaitLiveRegistersRelayedCalls(t *testing.T) {
	setupM2IsolatedEnv(t)
	t.Setenv("AGY_TOOL_RELAY_MS", "20")
	t.Setenv("AGY_MAX_TOOL_RELAYS", "4")
	t.Setenv("AGY_PARKED_TTL_SEC", "1800")
	initConfig()

	sess := NewAcpProjectSession("sid:m2-waitlive-relay:u:1", "/tmp", "")
	sess.SessionID = "sid-waitlive-1"
	sess.MessageCount = 2
	sess.orphanCapStreak = 1 // clamps maxRelays=0 -> capReached=true on 1st timer
	sess.turnClaimed = true  // causes HibernateIfSession to yield (false)

	entry := &ParkedMcpCall{
		Call:       ToolCallItem{ID: "acp_waitlive_1", Name: "Bash"},
		SessionID:  "sid-waitlive-1",
		ResultChan: make(chan map[string]any, 1),
		ParkedAt:   time.Now(),
	}
	sess.ParkedCalls["acp_waitlive_1"] = entry

	res := sess.awaitParkedEntry(context.Background(), entry)
	resTxt := fmt.Sprint(res["content"])
	if !strings.Contains(resTxt, "ws_await_result") {
		t.Fatalf("expected waitLive timeout to return ws_await_result prompt, got: %+v", res)
	}
	sess.mu.Lock()
	relayed := sess.RelayedCalls["acp_waitlive_1"]
	sess.mu.Unlock()
	if relayed != entry {
		t.Fatalf("expected waitLive branch to register entry in s.RelayedCalls, got %v", relayed)
	}

	// Simulate resumeParkedTurnInternal removing ParkedCalls[callID] and delivering abort result.
	sess.mu.Lock()
	delete(sess.ParkedCalls, "acp_waitlive_1")
	sess.mu.Unlock()
	entry.ResultChan <- map[string]any{
		"content": []map[string]any{{"type": "text", "text": "DELIVERED_AFTER_WAITLIVE"}},
		"isError": true,
	}

	got := sess.awaitParkedCall(context.Background(), "acp_waitlive_1")
	if !strings.Contains(fmt.Sprint(got["content"]), "DELIVERED_AFTER_WAITLIVE") {
		t.Fatalf("expected ws_await_result to drain ResultChan after ParkedCalls removal, got: %+v", got)
	}
}

func TestM2ChallengeCoverageBranches(t *testing.T) {
	setupM2IsolatedEnv(t)

	t.Run("finalizeCompletedTurn hibernates 429/503/529 when hasRemainingParked is true", func(t *testing.T) {
		for _, msg := range []string{
			"⚠ Agent execution terminated due to error: RESOURCE_EXHAUSTED (code 429): quota",
			"⚠ Agent execution terminated due to error: UNAVAILABLE (code 503): No capacity",
		} {
			s := NewAcpProjectSession("sid:m2-rem-parked:u:1", "/tmp", "")
			s.SessionID = "sid-rem-parked"
			s.MessageCount = 2
			s.ParkedCalls["acp_still_parked"] = &ParkedMcpCall{
				Call:       ToolCallItem{ID: "acp_still_parked", Name: "Bash"},
				SessionID:  "sid-rem-parked",
				ResultChan: make(chan map[string]any, 1),
			}
			_, err := s.finalizeCompletedTurn(context.Background(), TurnResult{StopReason: "end_turn", Text: msg}, time.Now(), 0, []map[string]any{{"role": "user", "content": "hi"}}, "fp", false, "sid-rem-parked", "parked_mcp_resume", "")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			s.mu.Lock()
			sidAfter := s.SessionID
			s.mu.Unlock()
			if sidAfter != "" {
				t.Fatalf("expected SessionID==\"\" (hibernated) when hasRemainingParked==true on soft error %q, got %q", msg, sidAfter)
			}
		}
	})

	t.Run("canHotInterrupt rolls over cleanly on toolCatalogNeedsRollover and ForceRolloverNext", func(t *testing.T) {
		baseURL, gs := startCarbonTestGateway(t)
		_ = baseURL
		// 1. toolCatalogNeedsRollover (hasBridge == false with non-empty toolsCatalog)
		sTool := gs.getOrCreateClaimedSession("sid:m2-tool-roll:u:1", "/tmp", "")
		defer sTool.releaseTurnClaim()
		if err := sTool.openFreshSession(nil, "medium"); err != nil {
			t.Fatalf("openFreshSession failed: %v", err)
		}
		sTool.mu.Lock()
		oldSid1 := sTool.SessionID
		sTool.MessageCount = 2
		sTool.lastTurnAssistant = true
		sTool.HistoryFingerprint = computeFirstUserFingerprint([]map[string]any{{"role": "user", "content": "hi"}})
		sTool.PromptInFlight = true
		sTool.DeclaredMcpTools = nil
		sTool.ParkedCalls["acp_tr_1"] = &ParkedMcpCall{
			Call: ToolCallItem{ID: "acp_tr_1", Name: "Bash"}, SessionID: oldSid1, ResultChan: make(chan map[string]any, 1),
		}
		sTool.mu.Unlock()

		msgs := []map[string]any{{"role": "user", "content": "hi"}, {"role": "user", "content": "use tools"}}
		newCat := []ToolDef{{Name: "Bash", McpName: mcpToolName("Bash")}}
		res1, err1 := sTool.RunTurn(context.Background(), msgs, newCat, "medium", nil, nil)
		if err1 != nil || res1.StopReason == "" {
			t.Fatalf("RunTurn tool rollover failed: err=%v res=%+v", err1, res1)
		}
		sTool.mu.Lock()
		if sTool.SessionID == "" || sTool.SessionID == oldSid1 || sTool.CompactionRollovers != 1 {
			sTool.mu.Unlock()
			t.Fatalf("expected toolCatalogNeedsRollover to roll over from %q, got sid=%q rolls=%d",
				oldSid1, sTool.SessionID, sTool.CompactionRollovers)
		}
		sTool.mu.Unlock()

		// 2. Pre-set ForceRolloverNext
		sForce := gs.getOrCreateClaimedSession("sid:m2-force-roll:u:1", "/tmp", "")
		defer sForce.releaseTurnClaim()
		if err := sForce.openFreshSession(newCat, "medium"); err != nil {
			t.Fatalf("openFreshSession failed: %v", err)
		}
		sForce.mu.Lock()
		oldSid2 := sForce.SessionID
		sForce.MessageCount = 2
		sForce.lastTurnAssistant = true
		sForce.HistoryFingerprint = computeFirstUserFingerprint([]map[string]any{{"role": "user", "content": "hi"}})
		sForce.PromptInFlight = true
		sForce.ForceRolloverNext = true
		sForce.ForceRolloverReason = "rollover_compaction"
		sForce.ParkedCalls["acp_fr_1"] = &ParkedMcpCall{
			Call: ToolCallItem{ID: "acp_fr_1", Name: "Bash"}, SessionID: oldSid2, ResultChan: make(chan map[string]any, 1),
		}
		sForce.mu.Unlock()

		res2, err2 := sForce.RunTurn(context.Background(), msgs, newCat, "medium", nil, nil)
		if err2 != nil || res2.StopReason == "" {
			t.Fatalf("RunTurn ForceRolloverNext failed: err=%v res=%+v", err2, res2)
		}
		sForce.mu.Lock()
		if sForce.SessionID == "" || sForce.SessionID == oldSid2 || sForce.CompactionRollovers != 1 {
			sForce.mu.Unlock()
			t.Fatalf("expected ForceRolloverNext to roll over from %q, got sid=%q rolls=%d",
				oldSid2, sForce.SessionID, sForce.CompactionRollovers)
		}
		sForce.mu.Unlock()
	})

	t.Run("runNewPromptResume ctx.Err() rolls back MessageCount, lastTurnAssistant, cancelledHistoryFp, and RelayedCalls", func(t *testing.T) {
		_, gs := startCarbonTestGateway(t)
		s := gs.getOrCreateClaimedSession("sid:m2-cancel-rollback:u:1", "/tmp", "")
		defer s.releaseTurnClaim()
		if err := s.openFreshSession(nil, "medium"); err != nil {
			t.Fatalf("openFreshSession failed: %v", err)
		}
		s.mu.Lock()
		origSid := s.SessionID
		s.MessageCount = 2
		s.lastTurnAssistant = true
		s.PromptInFlight = false
		s.ParkedCalls["acp_canc_1"] = &ParkedMcpCall{
			Call: ToolCallItem{ID: "acp_canc_1", Name: "Bash"}, SessionID: origSid, ResultChan: make(chan map[string]any, 1),
		}
		s.RelayedCalls = map[string]*ParkedMcpCall{"acp_canc_1": s.ParkedCalls["acp_canc_1"]}
		s.mu.Unlock()

		msgs := []map[string]any{
			{"role": "user", "content": "u0"},
			{"role": "assistant", "content": "a0"},
			{"role": "user", "content": "u1"},
		}
		cancCtx, cancel := context.WithCancel(context.Background())
		cancel()
		var fdNs atomic.Int64
		_, err := s.resumeParkedTurnInternal(cancCtx, nil, msgs, nil, time.Now(), &fdNs, nil, []map[string]any{{"type": "text", "text": "u1"}})
		if err == nil {
			t.Fatal("expected context.Canceled error from runNewPromptResume")
		}
		wantCancFp := computeMessagesSliceFingerprint(msgs)
		s.mu.Lock()
		if s.MessageCount != len(msgs) || s.lastTurnAssistant || s.cancelledHistoryFp != wantCancFp || s.RelayedCalls != nil {
			s.mu.Unlock()
			t.Fatalf("expected ctx.Err() rollback (msgCnt=%d lastAsst=false cancFp=%q relayed=nil), got msgCnt=%d lastAsst=%v cancFp=%q relayed=%v",
				len(msgs), wantCancFp, s.MessageCount, s.lastTurnAssistant, s.cancelledHistoryFp, s.RelayedCalls)
		}
		s.mu.Unlock()
	})
}

func buildOrphanBreakerFakeAcp(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "main.go")
	binPath := filepath.Join(dir, "fakeacp-orphan")

	const src = `package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
)

type rpcMsg struct {
	JSONRPC string          ` + "`json:\"jsonrpc\"`" + `
	ID      any             ` + "`json:\"id,omitempty\"`" + `
	Method  string          ` + "`json:\"method,omitempty\"`" + `
	Params  json.RawMessage ` + "`json:\"params,omitempty\"`" + `
	Result  any             ` + "`json:\"result,omitempty\"`" + `
	Error   any             ` + "`json:\"error,omitempty\"`" + `
}

type sessState struct {
	ID       string
	McpURL   string
	McpAuth  string
	McpTools []string
}

var (
	outMu    sync.Mutex
	sessMu   sync.Mutex
	sessions = map[string]*sessState{}
	seq      int
)

func send(v any) {
	b, _ := json.Marshal(v)
	outMu.Lock()
	os.Stdout.Write(append(b, '\n'))
	outMu.Unlock()
}

func sendUpdate(sid string, upd map[string]any) {
	b, _ := json.Marshal(map[string]any{"sessionId": sid, "update": upd})
	send(rpcMsg{JSONRPC: "2.0", Method: "session/update", Params: b})
}

func callMcp(s *sessState, method string, params any) map[string]any {
	if s.McpURL == "" {
		return nil
	}
	body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	req, err := http.NewRequest("POST", s.McpURL, bytes.NewReader(body))
	if err != nil {
		return nil
	}
	req.Header.Set("Content-Type", "application/json")
	if s.McpAuth != "" {
		req.Header.Set("Authorization", s.McpAuth)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	lines := strings.Split(string(raw), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if strings.HasPrefix(line, "data:") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		}
		if strings.HasPrefix(line, "{") {
			var parsed map[string]any
			if json.Unmarshal([]byte(line), &parsed) == nil {
				if r, ok := parsed["result"].(map[string]any); ok {
					return r
				}
			}
		}
	}
	return nil
}

func extractMcp(s *sessState, params map[string]any) {
	if srvs, ok := params["mcpServers"].([]any); ok && len(srvs) > 0 {
		if srv, ok := srvs[0].(map[string]any); ok {
			s.McpURL, _ = srv["url"].(string)
			if hdrs, ok := srv["headers"].([]any); ok {
				for _, h := range hdrs {
					if hm, ok := h.(map[string]any); ok && hm["name"] == "Authorization" {
						s.McpAuth, _ = hm["value"].(string)
					}
				}
			}
		}
	}
	if s.McpURL != "" {
		_ = callMcp(s, "initialize", map[string]any{"protocolVersion": "2025-11-25"})
		res := callMcp(s, "tools/list", map[string]any{})
		if tools, ok := res["tools"].([]any); ok {
			s.McpTools = nil
			for _, tl := range tools {
				if tm, ok := tl.(map[string]any); ok {
					if n, ok := tm["name"].(string); ok {
						s.McpTools = append(s.McpTools, n)
					}
				}
			}
		}
	}
}

func findMcpTool(s *sessState, hostName string) string {
	prefixes := []string{"ws_" + hostName + "_", "dsh_" + hostName + "_"}
	if hostName == "ws_await_result" {
		prefixes = []string{"ws_ws_await_result_", "dsh_dsh_await_result_"}
	}
	for _, p := range prefixes {
		for _, n := range s.McpTools {
			if strings.HasPrefix(n, p) {
				return n
			}
		}
	}
	if len(s.McpTools) > 0 {
		return s.McpTools[0]
	}
	return "ws_" + hostName + "_000000000000"
}

func extractToolText(r map[string]any) (string, bool) {
	if r == nil {
		return "", false
	}
	isErr, _ := r["isError"].(bool)
	if arr, ok := r["content"].([]any); ok && len(arr) > 0 {
		if cm, ok := arr[0].(map[string]any); ok {
			s, _ := cm["text"].(string)
			return s, isErr
		}
	}
	return "", isErr
}

func main() {
	for _, a := range os.Args[1:] {
		if a == "--version" || a == "-v" {
			fmt.Println("1.5.10")
			return
		}
	}
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 0, 1024*1024), 16*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var msg rpcMsg
		if json.Unmarshal(line, &msg) != nil {
			continue
		}
		var params map[string]any
		_ = json.Unmarshal(msg.Params, &params)
		switch msg.Method {
		case "initialize":
			send(rpcMsg{
				JSONRPC: "2.0",
				ID:      msg.ID,
				Result: map[string]any{
					"protocolVersion": 1,
					"agentCapabilities": map[string]any{
						"loadSession": true,
						"sessionCapabilities": map[string]any{
							"resume": map[string]any{},
							"close":  map[string]any{},
							"load":   map[string]any{},
							"list":   map[string]any{},
						},
						"mcpCapabilities": map[string]any{"http": true},
					},
				},
			})
		case "session/new":
			sessMu.Lock()
			seq++
			sid := fmt.Sprintf("22222222-2222-4222-8222-%012d", seq)
			s := &sessState{ID: sid}
			sessions[sid] = s
			sessMu.Unlock()
			extractMcp(s, params)
			send(rpcMsg{JSONRPC: "2.0", ID: msg.ID, Result: map[string]any{"sessionId": sid}})
		case "session/resume", "session/load":
			sid, _ := params["sessionId"].(string)
			sessMu.Lock()
			s := sessions[sid]
			if s == nil {
				s = &sessState{ID: sid}
				sessions[sid] = s
			}
			sessMu.Unlock()
			extractMcp(s, params)
			send(rpcMsg{JSONRPC: "2.0", ID: msg.ID, Result: map[string]any{"sessionId": sid}})
		case "session/set_config_option":
			cid, _ := params["configId"].(string)
			val, _ := params["value"].(string)
			send(rpcMsg{JSONRPC: "2.0", ID: msg.ID, Result: map[string]any{
				"configOptions": []map[string]any{{"id": cid, "currentValue": val}},
			}})
		case "session/cancel", "session/close":
			if msg.ID != nil {
				send(rpcMsg{JSONRPC: "2.0", ID: msg.ID, Result: map[string]any{}})
			}
		case "session/prompt":
			sid, _ := params["sessionId"].(string)
			sessMu.Lock()
			s := sessions[sid]
			if s == nil {
				s = &sessState{ID: sid}
				sessions[sid] = s
			}
			sessMu.Unlock()
			var promptText string
			if blocks, ok := params["prompt"].([]any); ok {
				for _, b := range blocks {
					if bm, ok := b.(map[string]any); ok {
						if txt, ok := bm["text"].(string); ok {
							promptText += txt + "\n"
						}
					}
				}
			}
			go func(reqID any, sess *sessState, txt string) {
				if strings.Contains(txt, "[PARK_ORPHAN_TOOL]") {
					for {
						mcpName := findMcpTool(sess, "Bash")
						res := callMcp(sess, "tools/call", map[string]any{
							"name":      mcpName,
							"arguments": map[string]any{"command": "echo ORPHAN_PARK"},
						})
						toolOut, isErr := extractToolText(res)
						for hop := 1; hop <= 20 && !isErr && strings.Contains(toolOut, "\"call_id\":\""); hop++ {
							idx := strings.Index(toolOut, "\"call_id\":\"")
							rest := toolOut[idx+len("\"call_id\":\""):]
							end := strings.IndexByte(rest, '"')
							if end <= 0 {
								break
							}
							cid := rest[:end]
							res = callMcp(sess, "tools/call", map[string]any{
								"name":      findMcpTool(sess, "ws_await_result"),
								"arguments": map[string]any{"call_id": cid},
							})
							toolOut, isErr = extractToolText(res)
						}
						if isErr && strings.Contains(toolOut, "hibernated") {
							send(rpcMsg{JSONRPC: "2.0", ID: reqID, Result: map[string]any{"stopReason": "cancelled"}})
							return
						}
						if isErr && strings.Contains(toolOut, "[HOT_PARK_AGAIN]") {
							continue
						}
						sendUpdate(sess.ID, map[string]any{
							"sessionUpdate": "agent_message_chunk",
							"content":       map[string]any{"type": "text", "text": "TOOL_DONE:" + toolOut},
						})
						send(rpcMsg{
							JSONRPC: "2.0",
							ID:      reqID,
							Result: map[string]any{
								"stopReason": "end_turn",
								"usage":      map[string]any{"inputTokens": 240, "outputTokens": 24, "cachedReadTokens": 128},
							},
						})
						return
					}
				}
				sendUpdate(sess.ID, map[string]any{
					"sessionUpdate": "agent_message_chunk",
					"content":       map[string]any{"type": "text", "text": "CLEAN_END_TURN"},
				})
				send(rpcMsg{
					JSONRPC: "2.0",
					ID:      reqID,
					Result: map[string]any{
						"stopReason": "end_turn",
						"usage":      map[string]any{"inputTokens": 240, "outputTokens": 24, "cachedReadTokens": 128},
					},
				})
			}(msg.ID, s, promptText)
		}
	}
}
`
	if err := os.WriteFile(srcPath, []byte(src), 0600); err != nil {
		t.Fatalf("write fakeacp-orphan source: %v", err)
	}
	goBin := "go"
	if _, err := exec.LookPath(goBin); err != nil {
		if home, err2 := os.UserHomeDir(); err2 == nil {
			candidate := filepath.Join(home, ".local", "go", "bin", "go")
			if _, err3 := os.Stat(candidate); err3 == nil {
				goBin = candidate
			}
		}
	}
	cmd := exec.Command(goBin, "build", "-o", binPath, srcPath)
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build fakeacp-orphan failed: %v: %s", err, string(out))
	}
	return binPath
}

func waitSessionHibernated(t *testing.T, sess *AcpProjectSession, wantStreak int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		sess.mu.Lock()
		curSid := sess.SessionID
		closing := sess.closing
		busy := sess.IsBusy
		claimed := sess.turnClaimed
		streak := sess.orphanCapStreak
		sess.mu.Unlock()
		if curSid == "" && !closing && !busy && !claimed && streak == wantStreak {
			if logBytes, err := os.ReadFile(GatewayLogFile); err == nil && strings.Count(string(logBytes), "[ACP_HIBERNATE]") >= wantStreak {
				return
			}
		}
		time.Sleep(15 * time.Millisecond)
	}
	sess.mu.Lock()
	curSid := sess.SessionID
	closing := sess.closing
	busy := sess.IsBusy
	claimed := sess.turnClaimed
	streak := sess.orphanCapStreak
	hibSid := sess.HibernatedSessionID
	sess.mu.Unlock()
	t.Fatalf("timed out waiting for hibernation with orphanCapStreak=%d (got sid=%q hibSid=%q closing=%v busy=%v claimed=%v streak=%d)",
		wantStreak, curSid, hibSid, closing, busy, claimed, streak)
}

func TestOrphanCapStreakEndToEndTwoTurnBreaker(t *testing.T) {
	orphanAcpBin := buildOrphanBreakerFakeAcp(t)
	tools := m2DefaultBashTools()

	runTwoTurnOrphanCase := func(t *testing.T, sessionHdr string, stripAsst bool) {
		t.Helper()
		setupM2IsolatedEnv(t)
		t.Setenv("AGY_ACP_BIN", orphanAcpBin)
		t.Setenv("AGY_TOOL_RELAY_MS", "120")
		t.Setenv("AGY_MAX_TOOL_RELAYS", "1")
		t.Setenv("AGY_PARKED_TTL_SEC", "1800")
		initConfig()

		baseURL, gs := startCarbonTestGateway(t)

		// Turn 1: park an orphan tool -> 1 relay hop -> MCP_TOOL_RELAY_CAP (maxToolRelays=1) -> orphanCapStreak=1.
		u0 := map[string]any{"role": "user", "content": "[PARK_ORPHAN_TOOL] turn 1"}
		code1, r1, raw1 := m2PostMessages(t, baseURL, sessionHdr, "", tools, []map[string]any{u0})
		if code1 != 200 || r1["stop_reason"] != "tool_use" {
			t.Fatalf("turn 1 expected tool_use, got %d %s", code1, string(raw1))
		}
		sess := m2FindSession(gs, sessionHdr)
		if sess == nil {
			t.Fatalf("expected session for %s", sessionHdr)
		}
		sess.mu.Lock()
		origSid := sess.SessionID
		sess.mu.Unlock()

		waitSessionHibernated(t, sess, 1, 3*time.Second)

		logAfterT1, err := os.ReadFile(GatewayLogFile)
		if err != nil {
			t.Fatalf("read gateway log after turn 1: %v", err)
		}
		if !strings.Contains(string(logAfterT1), "[MCP_TOOL_RELAY_CAP]") || !strings.Contains(string(logAfterT1), `"maxToolRelays":1`) {
			t.Fatalf("expected turn 1 MCP_TOOL_RELAY_CAP with maxToolRelays=1, log:\n%s", string(logAfterT1))
		}
		offsetBeforeT2 := len(logAfterT1)

		// Turn 2: resume hibernated session WITHOUT tool_result (either [u0, a0, u1] or [u0, u1] strippedHibernatedAsst)
		// and park a 2nd orphan tool. finalizeCompletedTurn("tool_use") & strippedHibernatedAsst MUST keep orphanCapStreak == 1.
		var turn2Msgs []map[string]any
		u1 := map[string]any{"role": "user", "content": "[PARK_ORPHAN_TOOL] turn 2"}
		if stripAsst {
			turn2Msgs = []map[string]any{u0, u1}
		} else {
			a0 := map[string]any{"role": "assistant", "content": "acknowledged"}
			turn2Msgs = []map[string]any{u0, a0, u1}
		}
		code2, r2, raw2 := m2PostMessages(t, baseURL, sessionHdr, "", tools, turn2Msgs)
		if code2 != 200 || r2["stop_reason"] != "tool_use" {
			t.Fatalf("turn 2 expected tool_use, got %d %s", code2, string(raw2))
		}
		sess.mu.Lock()
		sid2 := sess.SessionID
		rolls2 := sess.CompactionRollovers
		streakAfterT2Resp := sess.orphanCapStreak
		sess.mu.Unlock()
		if sid2 != origSid || rolls2 != 0 || streakAfterT2Resp != 1 {
			t.Fatalf("expected turn 2 resumed on origSid=%q with rolls=0 and orphanCapStreak=1 after tool_use, got sid=%q rolls=%d streak=%d",
				origSid, sid2, rolls2, streakAfterT2Resp)
		}

		// Turn 2 awaits parked tool -> orphanCapStreak==1 forces maxToolRelays=0 -> immediate cap (0 relay hops) -> streak=2.
		waitSessionHibernated(t, sess, 2, 3*time.Second)

		logAll, err := os.ReadFile(GatewayLogFile)
		if err != nil {
			t.Fatalf("read gateway log after turn 2: %v", err)
		}
		turn2Log := string(logAll[offsetBeforeT2:])
		if !strings.Contains(turn2Log, "[SESSION_RESUME]") || !strings.Contains(turn2Log, origSid) {
			t.Fatalf("expected SESSION_RESUME for %s in turn 2 log, got:\n%s", origSid, turn2Log)
		}
		if strings.Contains(turn2Log, "[MCP_TOOL_RELAY] ") {
			t.Fatalf("expected 0 MCP_TOOL_RELAY hops in turn 2 when orphanCapStreak>=1, got:\n%s", turn2Log)
		}
		if !strings.Contains(turn2Log, "[MCP_TOOL_RELAY_CAP]") || !strings.Contains(turn2Log, `"maxToolRelays":0`) || !strings.Contains(turn2Log, `"orphanRelayHops":0`) {
			t.Fatalf("expected turn 2 MCP_TOOL_RELAY_CAP with maxToolRelays=0 and orphanRelayHops=0, got:\n%s", turn2Log)
		}

		// Turn 3: subsequent clean end_turn MUST reset orphanCapStreak back to 0.
		c2, _ := r2["content"].([]any)
		a1 := map[string]any{"role": "assistant", "content": c2}
		u2 := map[string]any{"role": "user", "content": "clean end turn recovery"}
		turn3Msgs := append(append([]map[string]any{}, turn2Msgs...), a1, u2)
		code3, r3, raw3 := m2PostMessages(t, baseURL, sessionHdr, "", tools, turn3Msgs)
		if code3 != 200 || r3["stop_reason"] != "end_turn" {
			t.Fatalf("turn 3 expected end_turn, got %d %s", code3, string(raw3))
		}
		c3, _ := r3["content"].([]any)
		txt3, _ := c3[0].(map[string]any)["text"].(string)
		if !strings.Contains(txt3, "CLEAN_END_TURN") {
			t.Fatalf("expected CLEAN_END_TURN reply, got: %s", txt3)
		}
		sess.mu.Lock()
		sid3 := sess.SessionID
		rolls3 := sess.CompactionRollovers
		streak3 := sess.orphanCapStreak
		sess.mu.Unlock()
		if sid3 != origSid || rolls3 != 0 || streak3 != 0 {
			t.Fatalf("expected clean end_turn to stay on origSid=%q with rolls=0 and reset orphanCapStreak=0, got sid=%q rolls=%d streak=%d",
				origSid, sid3, rolls3, streak3)
		}
	}

	t.Run("keep assistant [u0,a0,u1] consecutive orphan triggers maxToolRelays=0 then end_turn resets", func(t *testing.T) {
		runTwoTurnOrphanCase(t, "m2-e2e-breaker-keep", false)
	})

	t.Run("strippedHibernatedAsst [u0,u1] consecutive orphan triggers maxToolRelays=0 then end_turn resets", func(t *testing.T) {
		runTwoTurnOrphanCase(t, "m2-e2e-breaker-strip", true)
	})

	t.Run("hot-parked interrupt that parks another tool keeps orphanCapStreak intact and fires maxToolRelays=0", func(t *testing.T) {
		setupM2IsolatedEnv(t)
		t.Setenv("AGY_ACP_BIN", orphanAcpBin)
		t.Setenv("AGY_TOOL_RELAY_MS", "400")
		t.Setenv("AGY_MAX_TOOL_RELAYS", "1")
		t.Setenv("AGY_PARKED_TTL_SEC", "1800")
		initConfig()

		baseURL, gs := startCarbonTestGateway(t)
		hdr := "m2-e2e-breaker-hot-int"

		// Turn 1: park tool -> relay cap -> orphanCapStreak=1.
		u0 := map[string]any{"role": "user", "content": "[PARK_ORPHAN_TOOL] hot int turn 1"}
		code1, r1, raw1 := m2PostMessages(t, baseURL, hdr, "", tools, []map[string]any{u0})
		if code1 != 200 || r1["stop_reason"] != "tool_use" {
			t.Fatalf("turn 1 expected tool_use, got %d %s", code1, string(raw1))
		}
		sess := m2FindSession(gs, hdr)
		if sess == nil {
			t.Fatalf("expected session for %s", hdr)
		}
		sess.mu.Lock()
		origSid := sess.SessionID
		sess.mu.Unlock()
		waitSessionHibernated(t, sess, 1, 3*time.Second)

		logBeforeT2, err := os.ReadFile(GatewayLogFile)
		if err != nil {
			t.Fatalf("read gateway log: %v", err)
		}
		offsetBeforeT2 := len(logBeforeT2)

		// Turn 2: wake session on [u0, u1] and park a tool (orphanCapStreak remains 1).
		u1 := map[string]any{"role": "user", "content": "[PARK_ORPHAN_TOOL] hot int turn 2"}
		code2, r2, raw2 := m2PostMessages(t, baseURL, hdr, "", tools, []map[string]any{u0, u1})
		if code2 != 200 || r2["stop_reason"] != "tool_use" {
			t.Fatalf("turn 2 expected tool_use, got %d %s", code2, string(raw2))
		}

		// Turn 3 (immediate hot-parked interrupt before 400ms timer fires):
		// sends [HOT_PARK_AGAIN] which parks yet another tool -> must keep orphanCapStreak=1 -> maxToolRelays=0!
		u2 := map[string]any{"role": "user", "content": "[HOT_PARK_AGAIN] interrupt and park again"}
		code3, r3, raw3 := m2PostMessages(t, baseURL, hdr, "", tools, []map[string]any{u0, u1, u2})
		if code3 != 200 || r3["stop_reason"] != "tool_use" {
			t.Fatalf("turn 3 expected tool_use, got %d %s", code3, string(raw3))
		}
		sess.mu.Lock()
		sid3 := sess.SessionID
		rolls3 := sess.CompactionRollovers
		streak3 := sess.orphanCapStreak
		sess.mu.Unlock()
		if sid3 != origSid || rolls3 != 0 || streak3 != 1 {
			t.Fatalf("expected hot-parked interrupt parking another tool to keep origSid=%q rolls=0 orphanCapStreak=1, got sid=%q rolls=%d streak=%d",
				origSid, sid3, rolls3, streak3)
		}

		waitSessionHibernated(t, sess, 2, 3*time.Second)

		logAll, err := os.ReadFile(GatewayLogFile)
		if err != nil {
			t.Fatalf("read gateway log after turn 3: %v", err)
		}
		tailLog := string(logAll[offsetBeforeT2:])
		if !strings.Contains(tailLog, "[PARKED_MCP_INTERRUPT_RESUME]") {
			t.Fatalf("expected PARKED_MCP_INTERRUPT_RESUME in log, got:\n%s", tailLog)
		}
		if strings.Contains(tailLog, "[MCP_TOOL_RELAY] ") {
			t.Fatalf("expected 0 MCP_TOOL_RELAY hops when orphanCapStreak>=1, got:\n%s", tailLog)
		}
		if !strings.Contains(tailLog, "[MCP_TOOL_RELAY_CAP]") || !strings.Contains(tailLog, `"maxToolRelays":0`) {
			t.Fatalf("expected MCP_TOOL_RELAY_CAP with maxToolRelays=0, got:\n%s", tailLog)
		}
	})
}
