package main

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestOrphanToolRelayCapAndResumeRecovery(t *testing.T) {
	t.Setenv("AGY_PARKED_TTL_SEC", "")
	t.Setenv("AGY_MAX_TOOL_RELAYS", "")
	if got := time.Duration(envInt("AGY_PARKED_TTL_SEC", 300)) * time.Second; got != 300*time.Second {
		t.Fatalf("expected default ParkedTtl 300s, got %v", got)
	}
	if got := envIntAllowSigned("AGY_MAX_TOOL_RELAYS", 4); got != 4 {
		t.Fatalf("expected default MaxToolRelays 4, got %d", got)
	}
	t.Setenv("AGY_MAX_TOOL_RELAYS", "0")
	if got := envIntAllowSigned("AGY_MAX_TOOL_RELAYS", 4); got != 0 {
		t.Fatalf("expected AGY_MAX_TOOL_RELAYS=0 to parse as 0, got %d", got)
	}
	t.Setenv("AGY_MAX_TOOL_RELAYS", "-1")
	if got := envIntAllowSigned("AGY_MAX_TOOL_RELAYS", 4); got != -1 {
		t.Fatalf("expected AGY_MAX_TOOL_RELAYS=-1 to parse as -1, got %d", got)
	}
	t.Setenv("AGY_MAX_TOOL_RELAYS", "")

	t.Setenv("AGY_TOOL_RELAY_MS", "70")
	t.Setenv("AGY_MAX_TOOL_RELAYS", "2")
	// Simulate an existing deployment whose .env still carries legacy AGY_PARKED_TTL_SEC=1800:
	// MaxToolRelays must still stop orphan relay loops after 2 hops.
	t.Setenv("AGY_PARKED_TTL_SEC", "1800")

	baseURL, cleanup := startTestGateway(t)
	defer cleanup()

	tools := []map[string]any{
		{
			"name":         "Bash",
			"description":  "Run shell command",
			"input_schema": map[string]any{"type": "object", "properties": map[string]any{"command": map[string]any{"type": "string"}}},
		},
	}

	postMessages := func(sessionHdr string, msgs []map[string]any) map[string]any {
		t.Helper()
		rawBody, _ := json.Marshal(map[string]any{
			"model":      "gemini-4-carbon",
			"max_tokens": 512,
			"tools":      tools,
			"messages":   msgs,
		})
		req, err := http.NewRequest("POST", baseURL+"/v1/messages", bytes.NewReader(rawBody))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("x-session-id", sessionHdr)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		outBytes, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			t.Fatalf("HTTP %d: %s", resp.StatusCode, string(outBytes))
		}
		var parsed map[string]any
		if err := json.Unmarshal(outBytes, &parsed); err != nil {
			t.Fatalf("invalid json: %v (%s)", err, string(outBytes))
		}
		return parsed
	}

	extractToolUse := func(resp map[string]any) (string, map[string]any) {
		t.Helper()
		content, _ := resp["content"].([]any)
		for _, item := range content {
			m, _ := item.(map[string]any)
			if m["type"] == "tool_use" {
				id, _ := m["id"].(string)
				return id, map[string]any{"role": "assistant", "content": content}
			}
		}
		t.Fatalf("missing tool_use block in %+v", resp)
		return "", nil
	}

	findConvSession := func(sub string) *AcpProjectSession {
		for _, s := range listTestSessions() {
			if strings.Contains(s.ConvKey, sub) {
				return s
			}
		}
		return nil
	}

	t.Run("caps dsh_await_result loop at 2 hops and resumes cleanly on late tool_result", func(t *testing.T) {
		beforeCaps := orphanRelayCapsTotal.Load()
		u1 := map[string]any{"role": "user", "content": "[FAKE_TOOL_CALL] [FAKE_FOLLOW_RELAYS] run check"}
		r1 := postMessages("relay-cap-normal", []map[string]any{u1})
		if r1["stop_reason"] != "tool_use" {
			t.Fatalf("expected tool_use, got %+v", r1)
		}
		callID, assistantMsg := extractToolUse(r1)
		sess := findConvSession("relay-cap-normal")
		if sess == nil {
			t.Fatal("live session not found")
		}
		sess.mu.Lock()
		origSid := sess.SessionID
		sess.mu.Unlock()
		if origSid == "" {
			t.Fatal("expected non-empty SessionID")
		}

		// Wait past 3 * 70ms = 210ms without returning tool_result: hop 1 (70ms) and hop 2 (140ms)
		// relay normally; hop 3 (210ms) hits MaxToolRelays=2 and hibernates while preserving HibernatedSessionID.
		deadline := time.Now().Add(2 * time.Second)
		var hibSid, curSid string
		for time.Now().Before(deadline) {
			sess.mu.Lock()
			curSid = sess.SessionID
			hibSid = sess.HibernatedSessionID
			sess.mu.Unlock()
			if curSid == "" && hibSid != "" && sess.IsAvailableForNewTurn() {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if curSid != "" || hibSid != origSid || !sess.IsAvailableForNewTurn() {
			t.Fatalf("expected available hibernated session preserving %s, got curSid=%q hibSid=%q avail=%v", origSid, curSid, hibSid, sess.IsAvailableForNewTurn())
		}
		if orphanRelayCapsTotal.Load() <= beforeCaps {
			t.Fatalf("expected orphanRelayCapsTotal increment above %d", beforeCaps)
		}

		// Client returns late with tool_result: gateway must wake origSid via session/resume (resumed_delta)
		// rather than cold-starting a new session, and fakeacp must report exactly 2 relay follows.
		u2 := map[string]any{
			"role": "user",
			"content": []map[string]any{
				{
					"type":        "tool_result",
					"tool_use_id": callID,
					"content":     "LATE_TOOL_OUTPUT_OK [ECHO_RELAY_META] [ECHO_PROMPT]",
				},
			},
		}
		r2 := postMessages("relay-cap-normal", []map[string]any{u1, assistantMsg, u2})
		content, _ := r2["content"].([]any)
		if len(content) == 0 {
			t.Fatalf("empty turn 2 content: %+v", r2)
		}
		txt, _ := content[0].(map[string]any)["text"].(string)
		if !strings.Contains(txt, "sid="+origSid) || !strings.Contains(txt, "resumes=1") || !strings.Contains(txt, "relay_follows=2") {
			t.Fatalf("expected resumed origSid with resumes=1 and relay_follows=2, got: %s", txt)
		}
		if !strings.Contains(txt, "LATE_TOOL_OUTPUT_OK") || strings.Contains(txt, "Conversation History Restored") {
			t.Fatalf("expected delta tool_result without cold rollover replay, got: %s", txt)
		}
	})

	t.Run("caps orphan loop even when model switches to a new host tool mid-relay", func(t *testing.T) {
		u1 := map[string]any{"role": "user", "content": "[FAKE_TOOL_CALL] [FAKE_FOLLOW_RELAYS] [FAKE_ROGUE_NEW_TOOL] run rogue"}
		r1 := postMessages("relay-cap-rogue", []map[string]any{u1})
		if r1["stop_reason"] != "tool_use" {
			t.Fatalf("expected tool_use, got %+v", r1)
		}
		sess := findConvSession("relay-cap-rogue")
		if sess == nil {
			t.Fatal("live rogue session not found")
		}
		sess.mu.Lock()
		origSid := sess.SessionID
		sess.mu.Unlock()

		deadline := time.Now().Add(2 * time.Second)
		var curSid, hibSid string
		for time.Now().Before(deadline) {
			sess.mu.Lock()
			curSid = sess.SessionID
			hibSid = sess.HibernatedSessionID
			sess.mu.Unlock()
			if curSid == "" && hibSid != "" {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if curSid != "" || hibSid != origSid {
			t.Fatalf("rogue tool switch bypassed session orphanRelayHops cap: curSid=%q hibSid=%q want=%q", curSid, hibSid, origSid)
		}
	})

	t.Run("rolls over instead of dropping user prompt when client strips abandoned tool_use at equal message count", func(t *testing.T) {
		u0 := map[string]any{"role": "user", "content": "hello"}
		a0 := map[string]any{"role": "assistant", "content": "hi"}
		u1 := map[string]any{"role": "user", "content": "[FAKE_TOOL_CALL] [FAKE_FOLLOW_RELAYS] run abandoned"}
		// Note: fakeacp triggers [FAKE_TOOL_CALL] on tNum == 1, so open fresh session with [u0, a0, u1] (len=3 -> MessageCount=4).
		r1 := postMessages("relay-abandon-strip", []map[string]any{u0, a0, u1})
		if r1["stop_reason"] != "tool_use" {
			t.Fatalf("expected tool_use, got %+v", r1)
		}
		// Client hits Ctrl+C, strips unanswered assistant(tool_use), and sends [u0, a0, u1, u2] (len=4 == MessageCount!).
		u2 := map[string]any{"role": "user", "content": "NEW_QUESTION_AFTER_ABORT [ECHO_PROMPT]"}
		r2 := postMessages("relay-abandon-strip", []map[string]any{u0, a0, u1, u2})
		content, _ := r2["content"].([]any)
		if len(content) == 0 {
			t.Fatalf("empty turn 2 content: %+v", r2)
		}
		txt, _ := content[0].(map[string]any)["text"].(string)
		if !strings.Contains(txt, "NEW_QUESTION_AFTER_ABORT") {
			t.Fatalf("abandoned tool_use strip swallowed new user message: %s", txt)
		}
	})
}

func TestOfficialQuotaWindowOpeningStepSkewAndIdleGuard(t *testing.T) {
	// Exact timestamps from 2026-11-15 audit:
	// - Local gemini-5h: opening step 3061 at ts_sec=1794703146 ($0.2531), resetTime=2026-11-15T05:39:07Z (reset-5h = 1794703147, delta = -1s)
	// - Prod gemini-weekly: opening step at ts_sec=1794614183 ($0.40), reset-7d = 1794614184 (delta = -1s)
	now := time.Unix(1794703400, 0).UTC()
	readAt := time.Unix(1794703350, 0).UTC().Format(time.RFC3339)
	reset5h := "2026-11-15T05:39:07Z" // 1794721147 -> start5h = 1794703147
	resetWk := time.Unix(1794614184+7*86400, 0).UTC().Format(time.RFC3339)

	snap := QuotaSnapshot{
		Status:    "ok",
		UpdatedAt: readAt,
		Groups: []QuotaGroup{
			{
				Name: "Gemini models",
				Buckets: []QuotaBucket{
					{Window: "5h", RemainingFraction: 0.99829, UsedFraction: 0.00171, ResetTime: reset5h},
					{Window: "weekly", RemainingFraction: 0.964, UsedFraction: 0.036, ResetTime: resetWk},
				},
			},
			{
				Name: "Claude models",
				Buckets: []QuotaBucket{
					{Window: "5h", RemainingFraction: 0.95, UsedFraction: 0.05, ResetTime: reset5h},
					{Window: "weekly", RemainingFraction: 0.90, UsedFraction: 0.10, ResetTime: resetWk},
				},
			},
		},
	}

	steps := []ParsedModelStep{
		// Previous ended 5h window step (finished 35s before start5h=1794703147): must NOT leak into 5h, but IS in weekly.
		{TsSec: 1794703107, ElapsedMs: 5000, StepIdx: 1, CostUsd: 0.90, QuotaGroup: "gemini", SourceCategory: "wsl_gateway", ModelID: "gemini-4-carbon"},
		// Prod weekly opening step with -1s skew (1794614183 vs 1794614184): must be included in weekly.
		{TsSec: 1794614183, ElapsedMs: 1200, StepIdx: 2, CostUsd: 0.40, QuotaGroup: "gemini", SourceCategory: "wsl_gateway", ModelID: "gemini-4-carbon"},
		// Local 5h opening step 3061 with -1s skew (1794703146 vs 1794703147): must be included in 5h & weekly.
		{TsSec: 1794703146, ElapsedMs: 0, StepIdx: 3, CostUsd: 0.2531, QuotaGroup: "gemini", SourceCategory: "wsl_gateway", ModelID: "gemini-4-carbon"},
		// Follow-up step 3062 inside 5h window:
		{TsSec: 1794703210, ElapsedMs: 3200, StepIdx: 4, CostUsd: 0.0560, QuotaGroup: "gemini", SourceCategory: "wsl_gateway", ModelID: "gemini-4-carbon"},
		// Long 90s thinking Claude 3p step that started 88s before reset5h-5h and finished at reset5h-5h+2s: must be included in tp.Window5h!
		{TsSec: 1794703147 - 88, ElapsedMs: 90000, StepIdx: 5, CostUsd: 1.25, QuotaGroup: "3p", SourceCategory: "wsl_gateway", ModelID: "claude-opus-5-5"},
	}

	q, w := buildQuotaView(snap, steps, now)
	if q.Deduction == nil {
		t.Fatal("missing Gemini deduction")
	}
	// Gemini 5h must include step 3 ($0.2531) + step 4 ($0.0560) = $0.31, excluding step 1 ($0.90).
	if math.Abs(q.Deduction.LocalTotal5hUsd-0.31) > 1e-6 {
		t.Fatalf("expected LocalTotal5hUsd=0.31, got %.4f", q.Deduction.LocalTotal5hUsd)
	}
	// Gemini weekly must include step 1 ($0.90) + step 2 ($0.40) + step 3 ($0.2531) + step 4 ($0.0560) = $1.61.
	if math.Abs(q.Deduction.LocalTotalWeeklyUsd-1.61) > 1e-6 {
		t.Fatalf("expected LocalTotalWeeklyUsd=1.61, got %.4f", q.Deduction.LocalTotalWeeklyUsd)
	}
	// Third-party 5h must include the 90s long-thinking opening step ($1.25).
	if q.ThirdPartyPool == nil || q.ThirdPartyPool.Window5h.Steps != 1 || math.Abs(q.ThirdPartyPool.Window5h.LocalCostUsd-1.25) > 1e-6 {
		t.Fatalf("expected ThirdPartyPool.Window5h steps=1 cost=1.25, got %+v", q.ThirdPartyPool.Window5h)
	}
	// Global window5h includes Gemini steps 3,4 + 3p step 5 = 3 steps (excluding step 1).
	if w["window5h"].StepsCount != 3 {
		t.Fatalf("expected window5h StepsCount=3, got %d", w["window5h"].StepsCount)
	}

	// Idle guard: when Gemini 5h is 0% used and reset == readAt + 5h, window5h must report Idle=true and 0 steps
	// even if a step occurred 2s before readAt.
	idleRead := time.Unix(1794703350, 0).UTC()
	idleSnap := QuotaSnapshot{
		Status:    "ok",
		UpdatedAt: idleRead.Format(time.RFC3339),
		Groups:    quotaTestGroups(true, false, 1.0, idleRead.Add(5*time.Hour).Format(time.RFC3339), idleRead.Add(7*24*time.Hour).Format(time.RFC3339)),
	}
	nearSteps := []ParsedModelStep{
		{TsSec: idleRead.Unix() - 2, ElapsedMs: 1000, StepIdx: 1, CostUsd: 0.50, QuotaGroup: "3p", ModelID: "claude-sonnet-5"},
	}
	_, idleW := buildQuotaView(idleSnap, nearSteps, now)
	if !idleW["window5h"].Idle || idleW["window5h"].StepsCount != 0 {
		t.Fatalf("idle official window must stay at 0 steps, got idle=%v steps=%d", idleW["window5h"].Idle, idleW["window5h"].StepsCount)
	}

	// Dual-pool isolation & post-readAt cache-TTL step inclusion:
	// Gemini is idle at readAt, then a brand-new $0.01 Flash step occurs 10s AFTER readAt while Claude 3p is active.
	dualSnap := QuotaSnapshot{
		Status:    "ok",
		UpdatedAt: idleRead.Format(time.RFC3339),
		Groups: []QuotaGroup{
			{
				Name: "Gemini models",
				Buckets: []QuotaBucket{
					{Window: "5h", RemainingFraction: 1.0, UsedFraction: 0.0, ResetTime: idleRead.Add(5 * time.Hour).Format(time.RFC3339)},
				},
			},
			{
				Name: "Claude models",
				Buckets: []QuotaBucket{
					{Window: "5h", RemainingFraction: 0.9999, UsedFraction: 0.0001, ResetTime: idleRead.Add(5 * time.Hour).Format(time.RFC3339)},
				},
			},
		},
	}
	dualSteps := []ParsedModelStep{
		// Ended Gemini step before readAt -> excluded.
		{TsSec: idleRead.Unix() - 30, ElapsedMs: 1000, StepIdx: 1, CostUsd: 0.80, QuotaGroup: "gemini", ModelID: "gemini-4-carbon"},
		// New Gemini Flash step strictly after readAt (within 3m /usage cache TTL) -> included!
		{TsSec: idleRead.Unix() + 10, ElapsedMs: 500, StepIdx: 2, CostUsd: 0.02, QuotaGroup: "gemini", ModelID: "gemini-3-flash-agent"},
		// Micro-fraction (0.01% = 0.0001 > 1e-6) Claude opening step with -2s skew -> not idle, included!
		{TsSec: idleRead.Unix() - 2, ElapsedMs: 1500, StepIdx: 3, CostUsd: 0.05, QuotaGroup: "3p", ModelID: "claude-sonnet-5"},
	}
	dq, dw := buildQuotaView(dualSnap, dualSteps, now)
	if math.Abs(dq.Deduction.LocalTotal5hUsd-0.02) > 1e-6 {
		t.Fatalf("expected post-readAt Gemini step ($0.02) included and pre-readAt ($0.80) excluded, got %.4f", dq.Deduction.LocalTotal5hUsd)
	}
	if dq.ThirdPartyPool.Window5h.Idle || dq.ThirdPartyPool.Window5h.Steps != 1 {
		t.Fatalf("micro-usage 3p bucket must not be idle and must keep opening step: %+v", dq.ThirdPartyPool.Window5h)
	}
	if dw["window5h"].Idle || dw["window5h"].StepsCount != 2 {
		t.Fatalf("global window5h must sum active/post-read steps across both pools (2 steps), got idle=%v steps=%d", dw["window5h"].Idle, dw["window5h"].StepsCount)
	}
}

func TestNormalizeTurnUsagePerTurnStepsInPrompt(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("AGY_CONV_DIR", dir)
	sid := "11111111-2222-4333-8444-555555555555"
	dbPath := filepath.Join(dir, sid+".db")

	st := &StateStore{}
	st.Init(filepath.Join(dir, "gateway.db"))
	t.Cleanup(func() {
		st.mu.Lock()
		if st.db != nil {
			_ = st.db.Close()
		}
		st.mu.Unlock()
	})

	s := NewAcpProjectSession("sid:steps-check", "/tmp", "")
	s.SessionID = sid
	s.ModelID = "gemini-4-carbon"
	s.ThoughtLevel = "high"

	// Turn 1 (PromptStepCount=1, 1 generation): StepsInPrompt must be 1.
	s.PromptStepCount = 1
	writeTestGenMetadataSQLite(t, dbPath, 1, buildTestGenMetadataBlobServed("gemini-4-carbon", 1427, 2000, 120, 50000, 40, 80, 52000, 786432, 1.2, 0.8))
	u1 := s.normalizeTurnUsage(nil, true, 0, 0)
	if u1.StepsInPrompt != 1 || u1.Generations != 1 || u1.UsageSource != "db" {
		t.Fatalf("turn 1 unexpected usage: %+v", u1)
	}

	// Turn 2 via parked_mcp_resume (PromptStepCount=5, 1 new generation): StepsInPrompt must be 1, NOT cumulative 5!
	s.PromptStepCount = 5
	writeTestGenMetadataSQLite(t, dbPath, 2, buildTestGenMetadataBlobServed("gemini-4-carbon", 1427, 1500, 90, 52000, 30, 60, 53500, 786432, 1.0, 0.6))
	u2 := s.normalizeTurnUsage(nil, true, 0, 0)
	if u2.StepsInPrompt != 1 || u2.Generations != 1 {
		t.Fatalf("turn 2 single-gen parked resume must report StepsInPrompt=1, got %+v", u2)
	}

	// Turn 3 via parked_mcp_resume (PromptStepCount=6, 3 upstream generations in one HTTP turn): StepsInPrompt must be 3.
	s.PromptStepCount = 6
	for idx := 3; idx <= 5; idx++ {
		writeTestGenMetadataSQLite(t, dbPath, idx, buildTestGenMetadataBlobServed("gemini-4-carbon", 1427, 1000, 80, 54000, 20, 60, 55000, 786432, 0.9, 0.5))
	}
	u3 := s.normalizeTurnUsage(nil, false, 0, 0)
	if u3.StepsInPrompt != 3 || u3.Generations != 3 {
		t.Fatalf("turn 3 multi-gen must report StepsInPrompt=3, got %+v", u3)
	}
	st.InsertTurn("sid:steps-check", TurnHistoryEntry{
		TurnIndex:     3,
		CompletedAtMs: time.Now().UnixMilli(),
		SessionID:     sid,
		Mode:          "parked_mcp_resume",
		Model:         "gemini-4-carbon",
		ThoughtLevel:  "high",
		StopReason:    "end_turn",
		Usage:         u3,
		CacheHitRate:  "98.00%",
	})

	// Turn 4 split batch with 0 new generations (UsageSource="none"): StepsInPrompt must be 0 in both struct and SQLite turns table.
	s.PromptStepCount = 7
	u4 := s.normalizeTurnUsage(nil, true, 0, 0)
	if u4.UsageSource != "none" || u4.StepsInPrompt != 0 {
		t.Fatalf("turn 4 split batch must report UsageSource=none StepsInPrompt=0, got %+v", u4)
	}
	st.InsertTurn("sid:steps-check", TurnHistoryEntry{
		TurnIndex:     4,
		CompletedAtMs: time.Now().UnixMilli(),
		SessionID:     sid,
		Mode:          "parked_mcp_resume",
		Model:         "gemini-4-carbon",
		ThoughtLevel:  "high",
		StopReason:    "tool_use",
		Usage:         u4,
		CacheHitRate:  "N/A",
	})
	var storedSteps int
	var storedSrc string
	if err := st.db.QueryRow(`SELECT steps_in_prompt, usage_source FROM turns WHERE conv_key = ? AND turn_idx = 4`, "sid:steps-check").Scan(&storedSteps, &storedSrc); err != nil {
		t.Fatalf("query turn 4 failed: %v", err)
	}
	if storedSteps != 0 || storedSrc != "none" {
		t.Fatalf("expected stored steps_in_prompt=0 usage_source=none, got steps=%d src=%q", storedSteps, storedSrc)
	}

	// Turn 5 ACP cumulative fallback (no SQLite DB for session): must still divide cumulative tokens by PromptStepCount=6.
	sAcp := NewAcpProjectSession("sid:acp-fallback", "/tmp", "")
	sAcp.SessionID = "99999999-9999-4999-8999-999999999999"
	sAcp.PromptStepCount = 6
	u5 := sAcp.normalizeTurnUsage(map[string]any{
		"inputTokens":      6000,
		"cachedReadTokens": 120000,
		"outputTokens":     150,
	}, false, 0, 0)
	if u5.UsageSource != "acp" || u5.InputTokens != 1000 || u5.CachedReadTokens != 20000 || u5.StepsInPrompt != 6 {
		t.Fatalf("ACP cumulative fallback broken: %+v", u5)
	}

	// Verify RestoreDetachedSessions backfills Generations from steps_in_prompt and /metrics exposes orphan_relay_caps_total.
	s.HibernatedSessionID = sid
	s.SessionID = ""
	s.MessageCount = 4
	s.TotalTurns = 4
	st.UpsertSession(s)
	gs := &GatewayServer{sessions: map[string]*AcpProjectSession{}, tenants: map[string]*AcpProjectSession{}}
	if n := st.RestoreDetachedSessions(gs); n != 1 {
		t.Fatalf("expected 1 restored session, got %d", n)
	}
	restored := gs.sessions["sid:steps-check"]
	var foundGen3 bool
	for _, th := range restored.TurnHistory {
		if th.TurnIndex == 3 && th.Usage != nil && th.Usage.Generations == 3 {
			foundGen3 = true
		}
	}
	if !foundGen3 {
		t.Fatalf("RestoreDetachedSessions did not backfill Generations=3: %+v", restored.TurnHistory)
	}
	metrics := RenderPrometheusMetrics(gs)
	if !strings.Contains(metrics, "agy_gateway_orphan_relay_caps_total") {
		t.Fatalf("missing agy_gateway_orphan_relay_caps_total in /metrics:\n%s", metrics)
	}
}
