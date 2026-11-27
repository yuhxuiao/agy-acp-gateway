package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestCarbonMediumChronicZeroCacheArmsLowTierFallback(t *testing.T) {
	setupM2IsolatedEnv(t)

	// 1. tierRank & nextFallbackTier contracts
	if !(tierRank("low") < tierRank("medium") && tierRank("medium") < tierRank("high") && tierRank("high") < tierRank("xhigh")) {
		t.Fatalf("expected strictly monotonic tierRank low<medium<high<xhigh, got %d,%d,%d,%d",
			tierRank("low"), tierRank("medium"), tierRank("high"), tierRank("xhigh"))
	}
	for _, tc := range []struct {
		model string
		tier  string
		want  string
	}{
		{"gemini-4-carbon", "xhigh", "medium"},
		{"gemini-4-carbon", "high", "medium"},
		{"gemini-4-carbon", "medium", "low"},
		{"gemini-4-carbon", "low", ""},
		{"gemini-4-hydrogen", "high", "medium"},
		{"gemini-4-hydrogen", "medium", ""},
		{"gemini-4-argon", "medium", ""},
	} {
		if got := nextFallbackTier(tc.model, tc.tier); got != tc.want {
			t.Fatalf("nextFallbackTier(%q, %q) = %q, want %q", tc.model, tc.tier, got, tc.want)
		}
	}

	// 2. Carbon medium chronic slow zero-cache misses arm capacityFallbackTier="low"
	sess := NewAcpProjectSession("sid:carbon-medium", DefaultCwd, "")
	sess.SessionID = "sid-med-1"
	sess.ModelID = "gemini-4-carbon"
	sess.ThoughtLevel = "medium"

	sess.logTurnStats("cold_start", 14000, TurnResult{
		StopReason: "end_turn",
		TtftMs:     12000,
		Usage: &TurnUsage{
			InputTokens: 60000, CachedReadTokens: 0,
			ContextInputTokens: 60000, ContextCachedTokens: 0, RawUncachedInputTokens: 60000,
			OutputTokens: 200, StepsInPrompt: 1, UsageSource: "db",
		},
	})
	if sess.capacityFallbackSid != "" || sess.capacityFallbackTier != "" {
		t.Fatalf("cold_start must not arm fallback: sid=%q tier=%q", sess.capacityFallbackSid, sess.capacityFallbackTier)
	}

	for i, ctxTok := range []int{62000, 68000} {
		sess.logTurnStats("sticky_delta", 22000, TurnResult{
			StopReason: "end_turn",
			TtftMs:     18000,
			Usage: &TurnUsage{
				InputTokens: ctxTok, CachedReadTokens: 0,
				ContextInputTokens: ctxTok, ContextCachedTokens: 0, RawUncachedInputTokens: ctxTok,
				OutputTokens: 180, StepsInPrompt: 1, UsageSource: "db",
			},
		})
		if i == 0 && sess.capacityFallbackSid != "" {
			t.Fatalf("single zero-cache turn should not arm fallback yet")
		}
	}
	if sess.capacityFallbackSid != "sid-med-1" || sess.capacityFallbackTier != "low" {
		t.Fatalf("expected carbon-medium to arm low fallback, got sid=%q tier=%q", sess.capacityFallbackSid, sess.capacityFallbackTier)
	}
	logRaw, _ := os.ReadFile(GatewayLogFile)
	if !strings.Contains(string(logRaw), `"HIGH_TIER_CACHE_DEGRADE_FALLBACK"`) && !strings.Contains(string(logRaw), `[HIGH_TIER_CACHE_DEGRADE_FALLBACK]`) {
		t.Fatalf("expected HIGH_TIER_CACHE_DEGRADE_FALLBACK log entry, got:\n%s", string(logRaw))
	}
	if !strings.Contains(string(logRaw), `"toTier":"low"`) {
		t.Fatalf("expected toTier=low in HIGH_TIER_CACHE_DEGRADE_FALLBACK log, got:\n%s", string(logRaw))
	}

	// 3. Two-step escalation high -> medium -> low
	sessEsc := NewAcpProjectSession("sid:carbon-esc", DefaultCwd, "")
	sessEsc.SessionID = "sid-esc-1"
	sessEsc.ModelID = "gemini-4-carbon"
	sessEsc.ThoughtLevel = "high"
	for i := 0; i < 2; i++ {
		sessEsc.logTurnStats("sticky_delta", 20000, TurnResult{
			StopReason: "end_turn",
			TtftMs:     15000,
			Usage: &TurnUsage{
				InputTokens: 55000, ContextInputTokens: 55000, RawUncachedInputTokens: 55000,
				OutputTokens: 150, StepsInPrompt: 1, UsageSource: "db",
			},
		})
	}
	if sessEsc.capacityFallbackSid != "sid-esc-1" || sessEsc.capacityFallbackTier != "medium" {
		t.Fatalf("expected step 1 escalation to medium, got sid=%q tier=%q", sessEsc.capacityFallbackSid, sessEsc.capacityFallbackTier)
	}
	sessEsc.ThoughtLevel = "medium"
	for i := 0; i < 2; i++ {
		sessEsc.logTurnStats("sticky_delta", 20000, TurnResult{
			StopReason: "end_turn",
			TtftMs:     15000,
			Usage: &TurnUsage{
				InputTokens: 55000, ContextInputTokens: 55000, RawUncachedInputTokens: 55000,
				OutputTokens: 150, StepsInPrompt: 1, UsageSource: "db",
			},
		})
	}
	if sessEsc.capacityFallbackSid != "sid-esc-1" || sessEsc.capacityFallbackTier != "low" {
		t.Fatalf("expected step 2 escalation medium->low on same sid, got sid=%q tier=%q", sessEsc.capacityFallbackSid, sessEsc.capacityFallbackTier)
	}
	// Low tier must never arm further
	sessEsc.ThoughtLevel = "low"
	for i := 0; i < 3; i++ {
		sessEsc.logTurnStats("sticky_delta", 20000, TurnResult{
			StopReason: "end_turn",
			TtftMs:     15000,
			Usage: &TurnUsage{
				InputTokens: 55000, ContextInputTokens: 55000, RawUncachedInputTokens: 55000,
				OutputTokens: 150, StepsInPrompt: 1, UsageSource: "db",
			},
		})
	}
	if sessEsc.highTierZeroCacheStreak != 0 || sessEsc.ModelID != "gemini-4-carbon" {
		t.Fatalf("expected low tier streak=0 and U8 ModelID pinned, got streak=%d model=%q", sessEsc.highTierZeroCacheStreak, sessEsc.ModelID)
	}

	// 4. applyModelSelection sticky clamping & ExplicitTier clear + hibernateWorker keepResumeState preservation
	sess.MessageCount = 2
	sess.HibernateWorker("test_idle", false)
	if sess.capacityFallbackSid != "sid-med-1" || sess.capacityFallbackTier != "low" {
		t.Fatalf("hibernateWorker(keepResumeState=true) must preserve capacityFallbackSid/Tier, got %q/%q",
			sess.capacityFallbackSid, sess.capacityFallbackTier)
	}
	// Verify forced hibernate (!keepResumeState) clears capacityFallbackSid/Tier
	sessReset := NewAcpProjectSession("sid:carbon-reset", DefaultCwd, "")
	sessReset.SessionID = "sid-reset-1"
	sessReset.MessageCount = 2
	sessReset.capacityFallbackSid = "sid-reset-1"
	sessReset.capacityFallbackTier = "low"
	sessReset.HibernateWorker("force_reset", true)
	if sessReset.capacityFallbackSid != "" || sessReset.capacityFallbackTier != "" {
		t.Fatalf("hibernateWorker(keepResumeState=false) must clear capacityFallbackSid/Tier, got %q/%q",
			sessReset.capacityFallbackSid, sessReset.capacityFallbackTier)
	}
	if err := sess.applyModelSelection(ModelSelection{Model: ModelOrDefault("gemini-4-carbon"), Tier: "high", ExplicitTier: true}); err != nil {
		t.Fatalf("explicit tier applyModelSelection: %v", err)
	}
	if sess.capacityFallbackSid != "" || sess.capacityFallbackTier != "" || sess.ThoughtLevel != "high" {
		t.Fatalf("ExplicitTier=true must clear fallback and apply high, got sid=%q tier=%q level=%q",
			sess.capacityFallbackSid, sess.capacityFallbackTier, sess.ThoughtLevel)
	}

	// 5. End-to-end HTTP warm reactive 429 medium->low & high->medium->low in-place (0 rollovers)
	baseURL, gs := startCarbonTestGateway(t)

	// Warm session on implicit medium (no thinking block)
	u1 := map[string]any{"role": "user", "content": "warm carbon medium"}
	resp1, raw1, err1 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "m3-med-low-429", map[string]any{
		"model":      "gemini-4-carbon",
		"max_tokens": 128,
		"messages":   []map[string]any{u1},
	})
	if err1 != nil || resp1.StatusCode != 200 {
		t.Fatalf("Turn 1 failed: %v %v %s", resp1, err1, raw1)
	}
	var out1 map[string]any
	_ = json.Unmarshal([]byte(raw1), &out1)
	asst1 := map[string]any{"role": "assistant", "content": out1["content"]}

	sessLive := findSessionByConvPrefix(gs, "sid:m3-med-low-429")
	sessLive.mu.Lock()
	origSid := sessLive.SessionID
	sessLive.mu.Unlock()

	resp2, raw2, err2 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "m3-med-low-429", map[string]any{
		"model":      "gemini-4-carbon",
		"max_tokens": 128,
		"messages":   []map[string]any{u1, asst1, {"role": "user", "content": "[FAKE_MEDIUM_429_LOW_OK] step to low"}},
	})
	if err2 != nil || resp2.StatusCode != 200 || !strings.Contains(raw2, "FALLBACK_LOW_OK") {
		t.Fatalf("expected warm medium->low in-place fallback, got status=%v err=%v body=%s", resp2, err2, raw2)
	}
	sessLive.mu.Lock()
	gotSid, gotModel, gotTier, gotFbTier, gotRolls := sessLive.SessionID, sessLive.ModelID, sessLive.ThoughtLevel, sessLive.capacityFallbackTier, sessLive.CompactionRollovers
	sessLive.mu.Unlock()
	if gotSid != origSid || gotModel != "gemini-4-carbon" || gotTier != "low" || gotFbTier != "low" || gotRolls != 0 {
		t.Fatalf("expected same sid=%s model=gemini-4-carbon tier=low fbTier=low rolls=0, got sid=%s model=%s tier=%s fbTier=%s rolls=%d",
			origSid, gotSid, gotModel, gotTier, gotFbTier, gotRolls)
	}

	// Follow-up turn without ExplicitTier stays sticky on low even if client sends implicit medium
	resp3, raw3, err3 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "m3-med-low-429", map[string]any{
		"model":      "gemini-4-carbon",
		"max_tokens": 128,
		"messages": []map[string]any{
			u1, asst1,
			{"role": "user", "content": "[FAKE_MEDIUM_429_LOW_OK] step to low"},
			{"role": "assistant", "content": "FALLBACK_LOW_OK"},
			{"role": "user", "content": "confirm tier"},
		},
	})
	if err3 != nil || resp3.StatusCode != 200 || !strings.Contains(raw3, "tier=low") {
		t.Fatalf("expected sticky low on follow-up turn, got %v %v %s", resp3, err3, raw3)
	}

	// Two-step high -> medium -> low on warm session
	hu1 := map[string]any{"role": "user", "content": "warm high"}
	hResp1, hRaw1, hErr1 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "m3-high-med-low", map[string]any{
		"model":      "gemini-4-carbon",
		"max_tokens": 32000,
		"thinking":   map[string]any{"type": "enabled", "budget_tokens": 31999},
		"messages":   []map[string]any{hu1},
	})
	if hErr1 != nil || hResp1.StatusCode != 200 {
		t.Fatalf("high Turn 1 failed: %v %v %s", hResp1, hErr1, hRaw1)
	}
	var hOut1 map[string]any
	_ = json.Unmarshal([]byte(hRaw1), &hOut1)
	hSess := findSessionByConvPrefix(gs, "sid:m3-high-med-low")
	hSess.mu.Lock()
	hOrigSid := hSess.SessionID
	hSess.mu.Unlock()

	hResp2, hRaw2, hErr2 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "m3-high-med-low", map[string]any{
		"model":      "gemini-4-carbon",
		"max_tokens": 32000,
		"thinking":   map[string]any{"type": "enabled", "budget_tokens": 31999},
		"messages":   []map[string]any{hu1, {"role": "assistant", "content": hOut1["content"]}, {"role": "user", "content": "[FAKE_MEDIUM_429_LOW_OK] two-step"}},
	})
	if hErr2 != nil || hResp2.StatusCode != 200 || !strings.Contains(hRaw2, "FALLBACK_LOW_OK") {
		t.Fatalf("expected two-step high->medium->low recovery, got %v %v %s", hResp2, hErr2, hRaw2)
	}
	hSess.mu.Lock()
	if hSess.SessionID != hOrigSid || hSess.ThoughtLevel != "low" || hSess.capacityFallbackTier != "low" || hSess.CompactionRollovers != 0 {
		t.Fatalf("expected same sid=%s tier=low fbTier=low rolls=0, got sid=%s tier=%s fbTier=%s rolls=%d",
			hOrigSid, hSess.SessionID, hSess.ThoughtLevel, hSess.capacityFallbackTier, hSess.CompactionRollovers)
	}
	hSess.mu.Unlock()
}

func TestParkedMcpResumeInPlaceTierSwitch(t *testing.T) {
	tools := m2DefaultBashTools()

	t.Run("explicit same-model tier switch on parked_mcp_resume", func(t *testing.T) {
		setupM2IsolatedEnv(t)
		t.Setenv("AGY_TOOL_RELAY_MS", "2000")
		t.Setenv("AGY_MAX_TOOL_RELAYS", "6")
		baseURL, gs := startCarbonTestGateway(t)

		u1 := map[string]any{"role": "user", "content": "[FAKE_TOOL_CALL] park at high"}
		st1, d1, raw1 := m2PostMessages(t, baseURL, "m3-park-explicit", "gemini-4-carbon-high", tools, []map[string]any{u1})
		if st1 != 200 {
			t.Fatalf("Turn 1 status=%d body=%s", st1, string(raw1))
		}
		cs1, _ := d1["content"].([]any)
		var callID string
		for _, blk := range cs1 {
			if bm, ok := blk.(map[string]any); ok && bm["type"] == "tool_use" {
				callID, _ = bm["id"].(string)
			}
		}
		if callID == "" {
			t.Fatalf("expected tool_use block, got %s", string(raw1))
		}
		sess := m2FindSession(gs, "m3-park-explicit")
		sess.mu.Lock()
		origSid := sess.SessionID
		sess.mu.Unlock()
		off := acpRecordOffset()

		// Turn 2: explicit tier switch to low while resuming parked tool
		u2 := map[string]any{"role": "user", "content": []any{map[string]any{"type": "tool_result", "tool_use_id": callID, "content": "LOW_TIER_RESULT"}}}
		st2, _, raw2 := m2PostMessages(t, baseURL, "m3-park-explicit", "gemini-4-carbon-low", tools, []map[string]any{u1, {"role": "assistant", "content": cs1}, u2})
		if st2 != 200 || !strings.Contains(string(raw2), "TOOL_DONE:LOW_TIER_RESULT") {
			t.Fatalf("Turn 2 expected TOOL_DONE:LOW_TIER_RESULT, got %d %s", st2, string(raw2))
		}
		if countRecordedSessionRPCContains(off, origSid, "session/set_config_option", `"value":"low"`) < 1 {
			t.Fatalf("expected session/set_config_option value=low during parked_mcp_resume")
		}
		sess.mu.Lock()
		lastHist := sess.TurnHistory[len(sess.TurnHistory)-1]
		// Turn 2 resumes the in-flight prompt started at high (agy applies set_config_option to the
		// NEXT session/prompt); after end_turn completes, sess.ThoughtLevel promotes to low.
		if sess.SessionID != origSid || sess.ThoughtLevel != "low" || sess.PendingThoughtLevel != "" || sess.CompactionRollovers != 0 || lastHist.Mode != "parked_mcp_resume" || lastHist.ThoughtLevel != "high" {
			t.Fatalf("unexpected session state after parked tier switch: sid=%s/%s tier=%s pending=%q rolls=%d mode=%s histTier=%s",
				sess.SessionID, origSid, sess.ThoughtLevel, sess.PendingThoughtLevel, sess.CompactionRollovers, lastHist.Mode, lastHist.ThoughtLevel)
		}
		sess.mu.Unlock()

		// Turn 3 verifies fakeacp session also reflects tier=low and records thoughtLevel=low
		st3, _, raw3 := m2PostMessages(t, baseURL, "m3-park-explicit", "gemini-4-carbon-low", tools, []map[string]any{
			u1, {"role": "assistant", "content": cs1}, u2, {"role": "assistant", "content": "TOOL_DONE:LOW_TIER_RESULT"},
			{"role": "user", "content": "confirm tier"},
		})
		if st3 != 200 || !strings.Contains(string(raw3), "tier=low") {
			t.Fatalf("Turn 3 expected tier=low, got %d %s", st3, string(raw3))
		}
		sess.mu.Lock()
		hist3 := sess.TurnHistory[len(sess.TurnHistory)-1]
		if hist3.ThoughtLevel != "low" || sess.ThoughtLevel != "low" || sess.PendingThoughtLevel != "" {
			t.Fatalf("Turn 3 expected histTier=low sessTier=low pending='', got hist=%s sess=%s pending=%q",
				hist3.ThoughtLevel, sess.ThoughtLevel, sess.PendingThoughtLevel)
		}
		sess.mu.Unlock()
	})

	t.Run("auto capacityFallbackTier switches in-place on parked_mcp_resume", func(t *testing.T) {
		setupM2IsolatedEnv(t)
		t.Setenv("AGY_TOOL_RELAY_MS", "2000")
		t.Setenv("AGY_MAX_TOOL_RELAYS", "6")
		baseURL, gs := startCarbonTestGateway(t)

		u1 := map[string]any{"role": "user", "content": "[FAKE_TOOL_CALL] park implicit high"}
		resp1, raw1, err1 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "m3-park-auto", map[string]any{
			"model":      "gemini-4-carbon",
			"max_tokens": 32000,
			"thinking":   map[string]any{"type": "enabled", "budget_tokens": 31999},
			"tools":      tools,
			"messages":   []map[string]any{u1},
		})
		if err1 != nil || resp1.StatusCode != 200 {
			t.Fatalf("Turn 1 failed: %v %v %s", resp1, err1, raw1)
		}
		var d1 map[string]any
		_ = json.Unmarshal([]byte(raw1), &d1)
		cs1, _ := d1["content"].([]any)
		var callID string
		for _, blk := range cs1 {
			if bm, ok := blk.(map[string]any); ok && bm["type"] == "tool_use" {
				callID, _ = bm["id"].(string)
			}
		}
		sess := m2FindSession(gs, "m3-park-auto")
		sess.mu.Lock()
		origSid := sess.SessionID
		sess.capacityFallbackSid = origSid
		sess.capacityFallbackTier = "medium"
		sess.mu.Unlock()

		u2 := map[string]any{"role": "user", "content": []any{map[string]any{"type": "tool_result", "tool_use_id": callID, "content": "AUTO_MED"}}}
		resp2, raw2, err2 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "m3-park-auto", map[string]any{
			"model":      "gemini-4-carbon",
			"max_tokens": 32000,
			"thinking":   map[string]any{"type": "enabled", "budget_tokens": 31999},
			"tools":      tools,
			"messages":   []map[string]any{u1, {"role": "assistant", "content": cs1}, u2},
		})
		if err2 != nil || resp2.StatusCode != 200 || !strings.Contains(raw2, "TOOL_DONE:AUTO_MED") {
			t.Fatalf("Turn 2 failed: %v %v %s", resp2, err2, raw2)
		}
		sess.mu.Lock()
		if sess.SessionID != origSid || sess.ThoughtLevel != "medium" || sess.CompactionRollovers != 0 {
			t.Fatalf("expected auto capacityFallbackTier=medium applied in-place, got sid=%s tier=%s rolls=%d",
				sess.SessionID, sess.ThoughtLevel, sess.CompactionRollovers)
		}
		sess.mu.Unlock()
	})

	t.Run("FAKE_ACP_REJECT_TIER graceful fallback for implicit vs fail-closed for explicit", func(t *testing.T) {
		setupM2IsolatedEnv(t)
		t.Setenv("AGY_TOOL_RELAY_MS", "2000")
		t.Setenv("AGY_MAX_TOOL_RELAYS", "6")
		t.Setenv("FAKE_ACP_REJECT_TIER", "low")
		baseURL, gs := startCarbonTestGateway(t)

		// Implicit tier downgrade failure logs warning, preserves ModelID/ThoughtLevel, and completes TOOL_DONE
		u1 := map[string]any{"role": "user", "content": "[FAKE_TOOL_CALL] reject low"}
		resp1, raw1, err1 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "m3-park-reject-implicit", map[string]any{
			"model":      "gemini-4-carbon",
			"max_tokens": 32000,
			"thinking":   map[string]any{"type": "enabled", "budget_tokens": 31999},
			"tools":      tools,
			"messages":   []map[string]any{u1},
		})
		if err1 != nil || resp1.StatusCode != 200 {
			t.Fatalf("Turn 1 failed: %v %v %s", resp1, err1, raw1)
		}
		var d1 map[string]any
		_ = json.Unmarshal([]byte(raw1), &d1)
		cs1, _ := d1["content"].([]any)
		var callID string
		for _, blk := range cs1 {
			if bm, ok := blk.(map[string]any); ok && bm["type"] == "tool_use" {
				callID, _ = bm["id"].(string)
			}
		}
		sess := m2FindSession(gs, "m3-park-reject-implicit")
		sess.mu.Lock()
		origSid := sess.SessionID
		sess.capacityFallbackSid = origSid
		sess.capacityFallbackTier = "low"
		sess.mu.Unlock()

		u2 := map[string]any{"role": "user", "content": []any{map[string]any{"type": "tool_result", "tool_use_id": callID, "content": "GRACEFUL_OK"}}}
		resp2, raw2, err2 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "m3-park-reject-implicit", map[string]any{
			"model":      "gemini-4-carbon",
			"max_tokens": 32000,
			"thinking":   map[string]any{"type": "enabled", "budget_tokens": 31999},
			"tools":      tools,
			"messages":   []map[string]any{u1, {"role": "assistant", "content": cs1}, u2},
		})
		if err2 != nil || resp2.StatusCode != 200 || !strings.Contains(raw2, "TOOL_DONE:GRACEFUL_OK") {
			t.Fatalf("expected implicit parked tier switch failure to continue gracefully, got %v %v %s", resp2, err2, raw2)
		}
		sess.mu.Lock()
		if sess.SessionID != origSid || sess.ModelID != "gemini-4-carbon" || sess.ThoughtLevel != "high" || sess.CompactionRollovers != 0 {
			t.Fatalf("expected ModelID=gemini-4-carbon & ThoughtLevel=high restored, got sid=%s model=%q tier=%q rolls=%d",
				sess.SessionID, sess.ModelID, sess.ThoughtLevel, sess.CompactionRollovers)
		}
		sess.mu.Unlock()
		logBytes, _ := os.ReadFile(GatewayLogFile)
		if !strings.Contains(string(logBytes), "PARKED_TIER_SWITCH_SKIPPED") {
			t.Fatalf("expected PARKED_TIER_SWITCH_SKIPPED warning in log")
		}

		// Explicit tier failure on parked_mcp_resume fails closed, preserves ModelID/ThoughtLevel,
		// and a subsequent model switch triggers model_switch_parked COMPACTION_ROLLOVER (U8).
		eu1 := map[string]any{"role": "user", "content": "[FAKE_TOOL_CALL] explicit reject low"}
		est1, ed1, eraw1 := m2PostMessages(t, baseURL, "m3-park-reject-explicit", "gemini-4-carbon-high", tools, []map[string]any{eu1})
		if est1 != 200 {
			t.Fatalf("explicit Turn 1 status=%d body=%s", est1, string(eraw1))
		}
		ecs1, _ := ed1["content"].([]any)
		var eCallID string
		for _, blk := range ecs1 {
			if bm, ok := blk.(map[string]any); ok && bm["type"] == "tool_use" {
				eCallID, _ = bm["id"].(string)
			}
		}
		esess := m2FindSession(gs, "m3-park-reject-explicit")
		esess.mu.Lock()
		eOrigSid := esess.SessionID
		esess.mu.Unlock()

		logOffBefore := len(logBytes)
		eu2 := map[string]any{"role": "user", "content": []any{map[string]any{"type": "tool_result", "tool_use_id": eCallID, "content": "SHOULD_FAIL"}}}
		est2, _, eraw2 := m2PostMessages(t, baseURL, "m3-park-reject-explicit", "gemini-4-carbon-low", tools, []map[string]any{eu1, {"role": "assistant", "content": ecs1}, eu2})
		if est2 != 400 {
			t.Fatalf("expected explicit tier switch rejection to fail closed with HTTP 400, got %d body=%s", est2, string(eraw2))
		}
		esess.mu.Lock()
		if esess.SessionID != eOrigSid || esess.ModelID != "gemini-4-carbon" || esess.ThoughtLevel != "high" || len(esess.ParkedCalls) != 1 {
			t.Fatalf("expected parked ModelID=gemini-4-carbon & ThoughtLevel=high & ParkedCalls preserved after 400, got sid=%s/%s model=%q tier=%q parked=%d",
				esess.SessionID, eOrigSid, esess.ModelID, esess.ThoughtLevel, len(esess.ParkedCalls))
		}
		esess.mu.Unlock()
		logAfterT2, _ := os.ReadFile(GatewayLogFile)
		if !strings.Contains(string(logAfterT2[logOffBefore:]), "MODEL_SWITCH_FAILED") {
			t.Fatalf("expected MODEL_SWITCH_FAILED in log after Turn 2")
		}

		// Turn 3: switching canonical model while parked must trigger model_switch_parked COMPACTION_ROLLOVER
		logOffT3 := len(logAfterT2)
		est3, _, eraw3 := m2PostMessages(t, baseURL, "m3-park-reject-explicit", "gemini-4-hydrogen-high", tools, []map[string]any{eu1, {"role": "assistant", "content": ecs1}, eu2})
		if est3 != 200 || !strings.Contains(string(eraw3), "model=gemini-4-hydrogen") {
			t.Fatalf("Turn 3 model switch rollover expected 200 on gemini-4-hydrogen, got %d body=%s", est3, string(eraw3))
		}
		esess.mu.Lock()
		if esess.SessionID == "" || esess.SessionID == eOrigSid || esess.ModelID != "gemini-4-hydrogen" || esess.ThoughtLevel != "high" || esess.CompactionRollovers < 1 {
			t.Fatalf("expected COMPACTION_ROLLOVER to new sid (old=%s) on gemini-4-hydrogen/high with rolls>=1, got sid=%s model=%q tier=%q rolls=%d",
				eOrigSid, esess.SessionID, esess.ModelID, esess.ThoughtLevel, esess.CompactionRollovers)
		}
		esess.mu.Unlock()
		logAfterT3, _ := os.ReadFile(GatewayLogFile)
		deltaT3 := string(logAfterT3[logOffT3:])
		if !strings.Contains(deltaT3, "COMPACTION_ROLLOVER") || !strings.Contains(deltaT3, "rollover_model_switch") {
			t.Fatalf("expected COMPACTION_ROLLOVER (rollover_model_switch) in Turn 3 log, got:\n%s", deltaT3)
		}
	})

	t.Run("canHotInterrupt switches tier in-place without rollover", func(t *testing.T) {
		setupM2IsolatedEnv(t)
		t.Setenv("AGY_TOOL_RELAY_MS", "2000")
		t.Setenv("AGY_MAX_TOOL_RELAYS", "6")
		baseURL, gs := startCarbonTestGateway(t)

		u1 := map[string]any{"role": "user", "content": "[FAKE_TOOL_CALL] park then hot interrupt"}
		resp1, raw1, err1 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "m3-hot-interrupt-tier", map[string]any{
			"model":      "gemini-4-carbon",
			"max_tokens": 32000,
			"thinking":   map[string]any{"type": "enabled", "budget_tokens": 31999},
			"tools":      tools,
			"messages":   []map[string]any{u1},
		})
		if err1 != nil || resp1.StatusCode != 200 {
			t.Fatalf("Turn 1 failed: %v %v %s", resp1, err1, raw1)
		}
		sess := m2FindSession(gs, "m3-hot-interrupt-tier")
		sess.mu.Lock()
		origSid := sess.SessionID
		sess.capacityFallbackSid = origSid
		sess.capacityFallbackTier = "medium"
		sess.mu.Unlock()

		// Stripped assistant tool_use + new user prompt -> hot interrupt with implicit high clamped to medium
		resp2, raw2, err2 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "m3-hot-interrupt-tier", map[string]any{
			"model":      "gemini-4-carbon",
			"max_tokens": 32000,
			"thinking":   map[string]any{"type": "enabled", "budget_tokens": 31999},
			"tools":      tools,
			"messages":   []map[string]any{u1, {"role": "user", "content": "stop tool and answer directly"}},
		})
		if err2 != nil || resp2.StatusCode != 200 {
			t.Fatalf("hot interrupt Turn 2 failed: %v %v %s", resp2, err2, raw2)
		}
		sess.mu.Lock()
		if sess.SessionID != origSid || sess.ThoughtLevel != "medium" || sess.CompactionRollovers != 0 {
			t.Fatalf("expected hot interrupt in-place tier=medium on sid=%s rolls=0, got sid=%s tier=%s rolls=%d",
				origSid, sess.SessionID, sess.ThoughtLevel, sess.CompactionRollovers)
		}
		sess.mu.Unlock()
	})
}

func TestCarbon4xCostWeightedLRUEviction(t *testing.T) {
	setupM2IsolatedEnv(t)

	// 1. LruProtectionWeight checks
	var nilModel *ModelInfo
	if got := nilModel.LruProtectionWeight(); got != 0.5 {
		t.Fatalf("nil ModelInfo LruProtectionWeight = %v, want 0.5", got)
	}
	if got := GetModel("").LruProtectionWeight(); got != 0.5 {
		t.Fatalf("GetModel(\"\").LruProtectionWeight() = %v, want 0.5", got)
	}
	if got := GetModel("nonexistent-model").LruProtectionWeight(); got != 0.5 {
		t.Fatalf("GetModel(\"nonexistent-model\").LruProtectionWeight() = %v, want 0.5", got)
	}
	if got := GetModel("gemini-4-carbon").LruProtectionWeight(); got != 4.0 {
		t.Fatalf("GetModel(\"gemini-4-carbon\").LruProtectionWeight() = %v, want 4.0", got)
	}
	for _, tc := range []struct {
		id   string
		want float64
	}{
		{"gemini-4-carbon", 4.0},
		{"gemini-4-neon", 2.0},
		{"gemini-4-argon", 4.0},
		{"gemini-4-hydrogen", 1.0},
		{"gemini-3.8-flash", 0.5},
		{"claude-opus-5-5", 8.0},
		{"claude-sonnet-5-5", 4.0},
	} {
		if got := ModelOrDefault(tc.id).LruProtectionWeight(); got != tc.want {
			t.Fatalf("ModelOrDefault(%q).LruProtectionWeight() = %v, want %v", tc.id, got, tc.want)
		}
	}
	if got := RetiredModels[0].LruProtectionWeight(); got != 0.5 {
		t.Fatalf("RetiredModels[0].LruProtectionWeight() = %v, want 0.5", got)
	}

	// 2. Strict weak ordering check on compareLruWorkerCandidates
	now := time.Now()
	makeCand := func(key, model string, tok int, age time.Duration) lruWorkerCandidate {
		w := ModelOrDefault(model).LruProtectionWeight()
		wt := float64(tok) * w
		warm := age < 300*time.Second
		prot := warm && wt >= 64000
		score := 0.0
		if warm {
			m := age.Minutes()
			if m < 1.0 {
				m = 1.0
			}
			score = wt / m
		}
		return lruWorkerCandidate{
			isUtil:        strings.HasPrefix(key, "util:"),
			lastUsed:      now.Add(-age),
			ctxTok:        tok,
			weightedTok:   wt,
			protectedWarm: prot,
			cacheWarm:     warm,
			costScore:     score,
			convKey:       key,
		}
	}
	matrix := []lruWorkerCandidate{
		makeCand("util:1", "gemini-4-carbon", 200000, 5*time.Second),
		makeCand("sid:carbon-expired", "gemini-4-carbon", 200000, 400*time.Second),
		makeCand("sid:flash-unprot", "gemini-3.8-flash", 100000, 10*time.Second),
		makeCand("sid:carbon-prot-50k", "gemini-4-carbon", 50000, 120*time.Second),
		makeCand("sid:flash-prot-150k", "gemini-3.8-flash", 150000, 10*time.Second),
		makeCand("sid:hydrogen-150k", "gemini-4-hydrogen", 150000, 10*time.Second),
		makeCand("sid:carbon-100k-old", "gemini-4-carbon", 100000, 120*time.Second),
		makeCand("sid:carbon-100k-new", "gemini-4-carbon", 100000, 10*time.Second),
		makeCand("sid:carbon-100k-tieA", "gemini-4-carbon", 100000, 30*time.Second),
		makeCand("sid:carbon-100k-tieB", "gemini-4-carbon", 100000, 30*time.Second),
		makeCand("sid:zero-old", "gemini-3.8-flash", 0, 60*time.Second),
		makeCand("sid:zero-new", "gemini-3.8-flash", 0, 10*time.Second),
	}
	for i, a := range matrix {
		if compareLruWorkerCandidates(a, a) {
			t.Fatalf("irreflexivity violated at %d", i)
		}
		for j, b := range matrix {
			if compareLruWorkerCandidates(a, b) && compareLruWorkerCandidates(b, a) {
				t.Fatalf("asymmetry violated for (%d, %d)", i, j)
			}
			for k, c := range matrix {
				if compareLruWorkerCandidates(a, b) && compareLruWorkerCandidates(b, c) && !compareLruWorkerCandidates(a, c) {
					t.Fatalf("transitivity violated for (%d, %d, %d)", i, j, k)
				}
			}
		}
	}

	// 3. Carbon 50K (t-120s) survives over Flash 100K (t-10s), and util: is evicted first
	t.Setenv("AGY_MAX_LIVE_WORKERS", "2")
	initConfig()
	gs := NewGatewayServer()
	t.Cleanup(gs.Close)

	seed := func(key, sid, model string, tok int, lastUsed time.Time) *AcpProjectSession {
		s := NewAcpProjectSession(key, DefaultCwd, "")
		s.SessionID = sid
		s.ModelID = model
		s.LastCachedReadTokens = tok
		s.LastUsedAt = lastUsed
		s.MessageCount = 2
		gs.mu.Lock()
		gs.sessions[key] = s
		gs.mu.Unlock()
		return s
	}

	sCarbon := seed("sid:carbon-50k", "sid-c50", "gemini-4-carbon", 50000, now.Add(-120*time.Second))
	sFlash := seed("sid:flash-100k", "sid-f100", "gemini-3.8-flash", 100000, now.Add(-10*time.Second))
	sIncoming := NewAcpProjectSession("sid:incoming", DefaultCwd, "")
	gs.mu.Lock()
	gs.sessions["sid:incoming"] = sIncoming
	gs.mu.Unlock()

	gs.enforceWorkerCap(sIncoming)
	sCarbon.mu.Lock()
	cSid := sCarbon.SessionID
	sCarbon.mu.Unlock()
	sFlash.mu.Lock()
	fSid, fHib := sFlash.SessionID, sFlash.HibernatedSessionID
	sFlash.mu.Unlock()
	if cSid != "sid-c50" || fSid != "" || fHib != "sid-f100" {
		t.Fatalf("expected Carbon 50K to survive and Flash 100K to hibernate, got carbon=%q flash=%q(hib=%q)", cSid, fSid, fHib)
	}

	// util: worker is evicted before Flash 4K even if util has larger tokens
	sUtil := seed("util:title", "sid-util", "gemini-4-carbon", 200000, now.Add(-5*time.Second))
	sSmall := seed("sid:flash-4k", "sid-f4", "gemini-3.8-flash", 4000, now.Add(-60*time.Second))
	// Current live count is 3 (sCarbon, sUtil, sSmall) >= MaxLiveWorkers(2) -> evicts 2 down to 1
	// Wait: let's set MaxLiveWorkers=3 so only 1 victim (sUtil) is evicted!
	MaxLiveWorkers = 3
	gs.enforceWorkerCap(sIncoming)
	sUtil.mu.Lock()
	uSid := sUtil.SessionID
	sUtil.mu.Unlock()
	sSmall.mu.Lock()
	smSid := sSmall.SessionID
	sSmall.mu.Unlock()
	if uSid != "" || smSid != "sid-f4" {
		t.Fatalf("expected util: evicted before Flash 4K, got util=%q small=%q", uSid, smSid)
	}

	// 4. Concurrent -race check on enforceWorkerCap
	MaxLiveWorkers = 2
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for it := 0; it < 20; it++ {
				sCarbon.mu.Lock()
				sCarbon.LastUsedAt = time.Now()
				sCarbon.mu.Unlock()
				gs.enforceWorkerCap(sIncoming)
			}
		}(g)
	}
	wg.Wait()
}

func TestTransient32603BackoffRetryPreservesWarmSession(t *testing.T) {
	setupM2IsolatedEnv(t)
	origBackoffs := rpcRetryBackoffs
	rpcRetryBackoffs = []time.Duration{10 * time.Millisecond, 20 * time.Millisecond}
	t.Cleanup(func() { rpcRetryBackoffs = origBackoffs })

	// 1. Error classification truth table
	retryableCases := []error{
		&acpRPCError{Raw: `{"code":-32603,"message":"503 MODEL_CAPACITY_EXHAUSTED: transient upstream rpc failure"}`},
		&acpRPCError{Raw: `{"code":-32603,"message":"Internal error"}`},
		&acpRPCError{Raw: `{"code":-32603,"message":"429 RESOURCE_EXHAUSTED: rate limit exceeded"}`},
	}
	for _, err := range retryableCases {
		if !isRetryableAcpRpcError(err) || !isTransientWarmRpcError(err) {
			t.Fatalf("expected retryable & transient warm for %v", err)
		}
	}
	nonRetryableCases := []error{
		&acpRPCError{Raw: `{"code":-32603,"message":"400 INVALID_ARGUMENT: thinking budget is not supported"}`},
		&acpRPCError{Raw: `{"code":-32603,"message":"400 Bad payload"}`},
		&acpRPCError{Raw: `{"code":-32603,"message":"429 RESOURCE_EXHAUSTED: Individual quota reached for model"}`},
		&acpRPCError{Raw: `{"code":-32603,"message":"401 UNAUTHENTICATED: invalid_api_key"}`},
		&acpRPCError{Raw: `{"code":403,"message":"PERMISSION_DENIED: license #3501"}`},
		&acpRPCError{Raw: `{"code":403,"message":"PERMISSION_DENIED: violation of Terms of Service"}`},
		&acpRPCError{Raw: `{"code":-32000,"message":"simulated upstream RPC failure"}`},
		&acpRPCError{Raw: `{"code":-32602,"message":"Invalid params"}`},
		errors.New("shared ACP engine not running"),
		errors.New("shared ACP engine exited"),
		io.EOF,
		syscall.EPIPE,
		errors.New("ACP RPC session/prompt timed out after 3600s"),
		context.Canceled,
		&wafFastCancelledError{Detail: "rate limit"},
	}
	for _, err := range nonRetryableCases {
		if isRetryableAcpRpcError(err) || isTransientWarmRpcError(err) {
			t.Fatalf("expected non-retryable & non-transient for %v", err)
		}
	}

	baseURL, gs := startCarbonTestGateway(t)

	// Subtest A: 1st -32603 -> retry 2nd succeeds without openFreshSession/session/close
	t.Run("transient -32603 ONCE recovers in-session", func(t *testing.T) {
		u1 := map[string]any{"role": "user", "content": "warm up 32603 once"}
		r1, raw1, e1 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "m3-32603-once", map[string]any{
			"model": "gemini-4-carbon", "max_tokens": 128, "messages": []map[string]any{u1},
		})
		if e1 != nil || r1.StatusCode != 200 {
			t.Fatalf("Turn 1 failed: %v %v %s", r1, e1, raw1)
		}
		var d1 map[string]any
		_ = json.Unmarshal([]byte(raw1), &d1)
		sess := findSessionByConvPrefix(gs, "sid:m3-32603-once")
		sess.mu.Lock()
		origSid := sess.SessionID
		sess.mu.Unlock()
		off := acpRecordOffset()

		r2, raw2, e2 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "m3-32603-once", map[string]any{
			"model": "gemini-4-carbon", "max_tokens": 128,
			"messages": []map[string]any{u1, {"role": "assistant", "content": d1["content"]}, {"role": "user", "content": "[FAKE_RPC_32603_ONCE] retry me"}},
		})
		if e2 != nil || r2.StatusCode != 200 || !strings.Contains(raw2, "FAKE_OK") {
			t.Fatalf("expected recovery after -32603 once, got %v %v %s", r2, e2, raw2)
		}
		if pCnt := countRecordedSessionRPCs(off, origSid, "session/prompt"); pCnt != 2 {
			t.Fatalf("expected 2 session/prompt calls on %s, got %d", origSid, pCnt)
		}
		if cCnt := countRecordedSessionRPCs(off, origSid, "session/close"); cCnt != 0 {
			t.Fatalf("expected 0 session/close calls, got %d", cCnt)
		}
		sess.mu.Lock()
		lastHist := sess.TurnHistory[len(sess.TurnHistory)-1]
		if sess.SessionID != origSid || sess.CompactionRollovers != 0 || lastHist.RetryCount < 1 {
			t.Fatalf("expected same sid=%s rolls=0 retryCount>=1, got sid=%s rolls=%d retry=%d",
				origSid, sess.SessionID, sess.CompactionRollovers, lastHist.RetryCount)
		}
		sess.mu.Unlock()
	})

	// Subtest B: Persistent [FAKE_RPC_529] retries 2x (total 3 prompts), returns 529, preserves warm session
	t.Run("persistent 529 preserves warm session without prompt_rpc_error hibernate", func(t *testing.T) {
		u1 := map[string]any{"role": "user", "content": "warm up 529"}
		r1, raw1, e1 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "m3-529-preserve", map[string]any{
			"model": "gemini-4-carbon", "max_tokens": 128, "messages": []map[string]any{u1},
		})
		if e1 != nil || r1.StatusCode != 200 {
			t.Fatalf("Turn 1 failed: %v %v %s", r1, e1, raw1)
		}
		var d1 map[string]any
		_ = json.Unmarshal([]byte(raw1), &d1)
		asst1 := map[string]any{"role": "assistant", "content": d1["content"]}
		sess := findSessionByConvPrefix(gs, "sid:m3-529-preserve")
		sess.mu.Lock()
		origSid := sess.SessionID
		origFp := sess.HistoryFingerprint
		sess.mu.Unlock()
		off := acpRecordOffset()
		logBefore, _ := os.ReadFile(GatewayLogFile)

		r2, raw2, e2 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "m3-529-preserve", map[string]any{
			"model": "gemini-4-carbon", "max_tokens": 128,
			"messages": []map[string]any{u1, asst1, {"role": "user", "content": "[FAKE_RPC_529] overloaded"}},
		})
		if e2 != nil || r2.StatusCode != 529 {
			t.Fatalf("expected HTTP 529, got %v %v %s", r2, e2, raw2)
		}
		if pCnt := countRecordedSessionRPCs(off, origSid, "session/prompt"); pCnt != 3 {
			t.Fatalf("expected 3 session/prompt calls (1+2 retries), got %d", pCnt)
		}
		sess.mu.Lock()
		if sess.SessionID != origSid || sess.MessageCount != 2 || sess.HistoryFingerprint != origFp {
			t.Fatalf("expected warm session preserved (sid=%s msgCnt=2 fp=%s), got sid=%s msgCnt=%d fp=%s",
				origSid, origFp, sess.SessionID, sess.MessageCount, sess.HistoryFingerprint)
		}
		sess.mu.Unlock()
		logAfter, _ := os.ReadFile(GatewayLogFile)
		deltaLog := string(logAfter[len(logBefore):])
		if strings.Contains(deltaLog, `"reason":"prompt_rpc_error"`) {
			t.Fatalf("unexpected prompt_rpc_error hibernate in log:\n%s", deltaLog)
		}

		// Turn 3 recovers via sticky_delta on origSid with 0 rollovers
		r3, raw3, e3 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "m3-529-preserve", map[string]any{
			"model": "gemini-4-carbon", "max_tokens": 128,
			"messages": []map[string]any{u1, asst1, {"role": "user", "content": "recover after 529"}},
		})
		if e3 != nil || r3.StatusCode != 200 || !strings.Contains(raw3, "FAKE_OK") {
			t.Fatalf("Turn 3 recovery failed: %v %v %s", r3, e3, raw3)
		}
		sess.mu.Lock()
		lastHist := sess.TurnHistory[len(sess.TurnHistory)-1]
		if sess.SessionID != origSid || sess.CompactionRollovers != 0 || lastHist.Mode != "sticky_delta" {
			t.Fatalf("expected sticky_delta on origSid=%s rolls=0, got sid=%s rolls=%d mode=%s",
				origSid, sess.SessionID, sess.CompactionRollovers, lastHist.Mode)
		}
		sess.mu.Unlock()
	})

	// Subtest C: Non-retryable 400 executes once and hibernates
	t.Run("non-retryable 400 executes once and hibernates", func(t *testing.T) {
		u1 := map[string]any{"role": "user", "content": "warm up 400"}
		r1, raw1, e1 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "m3-400-hib", map[string]any{
			"model": "gemini-4-carbon", "max_tokens": 128, "messages": []map[string]any{u1},
		})
		if e1 != nil || r1.StatusCode != 200 {
			t.Fatalf("Turn 1 failed: %v %v %s", r1, e1, raw1)
		}
		var d1 map[string]any
		_ = json.Unmarshal([]byte(raw1), &d1)
		sess := findSessionByConvPrefix(gs, "sid:m3-400-hib")
		sess.mu.Lock()
		origSid := sess.SessionID
		sess.mu.Unlock()
		off := acpRecordOffset()

		r2, raw2, e2 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "m3-400-hib", map[string]any{
			"model": "gemini-4-carbon", "max_tokens": 128,
			"messages": []map[string]any{u1, {"role": "assistant", "content": d1["content"]}, {"role": "user", "content": "[FAKE_RPC_400] bad arg"}},
		})
		if e2 != nil || r2.StatusCode != 400 {
			t.Fatalf("expected HTTP 400, got %v %v %s", r2, e2, raw2)
		}
		if pCnt := countRecordedSessionRPCs(off, origSid, "session/prompt"); pCnt != 1 {
			t.Fatalf("expected 1 session/prompt call on 400, got %d", pCnt)
		}
		sess.mu.Lock()
		if sess.SessionID != "" {
			t.Fatalf("expected session hibernated after 400, got sid=%s", sess.SessionID)
		}
		sess.mu.Unlock()
	})

	// Subtest D: Context cancellation during backoff aborts without advancing MessageCount
	t.Run("ctx cancel during backoff does not advance MessageCount", func(t *testing.T) {
		rpcRetryBackoffs = []time.Duration{250 * time.Millisecond, 250 * time.Millisecond}
		t.Cleanup(func() { rpcRetryBackoffs = []time.Duration{10 * time.Millisecond, 20 * time.Millisecond} })

		u1 := map[string]any{"role": "user", "content": "warm up cancel backoff"}
		r1, raw1, e1 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "m3-32603-cancel", map[string]any{
			"model": "gemini-4-carbon", "max_tokens": 128, "messages": []map[string]any{u1},
		})
		if e1 != nil || r1.StatusCode != 200 {
			t.Fatalf("Turn 1 failed: %v %v %s", r1, e1, raw1)
		}
		var d1 map[string]any
		_ = json.Unmarshal([]byte(raw1), &d1)
		sess := findSessionByConvPrefix(gs, "sid:m3-32603-cancel")
		sess.mu.Lock()
		origSid := sess.SessionID
		sess.mu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
		defer cancel()
		_, turnErr := sess.RunTurn(ctx, []map[string]any{
			u1, {"role": "assistant", "content": d1["content"]}, {"role": "user", "content": "[FAKE_RPC_32603_ALWAYS] cancel me"},
		}, nil, "medium", nil, &ModelSelection{Model: ModelOrDefault("gemini-4-carbon"), Tier: "medium"})
		var retryAbort *rpcRetryAbortedError
		if !errors.As(turnErr, &retryAbort) || !errors.Is(turnErr, context.DeadlineExceeded) {
			t.Fatalf("expected rpcRetryAbortedError wrapping DeadlineExceeded, got %v", turnErr)
		}
		sess.mu.Lock()
		if sess.SessionID != origSid || sess.MessageCount != 2 {
			t.Fatalf("expected sid=%s and MessageCount=2 preserved on backoff cancel, got sid=%s msgCnt=%d",
				origSid, sess.SessionID, sess.MessageCount)
		}
		sess.mu.Unlock()
	})
}

func TestPrefillCancelGrace90Preserves120KFloor(t *testing.T) {
	setupM2IsolatedEnv(t)

	// 1. AGY_PREFILL_CANCEL_GRACE_SEC=90 preserves >=120K -> 120s floor and passiveHold=120s
	t.Setenv("AGY_PREFILL_CANCEL_MIN_ELAPSED_SEC", "")
	t.Setenv("AGY_PREFILL_CANCEL_GRACE_SEC", "90")
	initConfig()

	sSmall := NewAcpProjectSession("sid:grace90-small", DefaultCwd, "")
	if _, gr, _ := sSmall.effectiveSoftDetachPolicy(4000); gr != 90*time.Second {
		t.Fatalf("<48K grace under 90s env = %v, want 90s", gr)
	}
	sMid := NewAcpProjectSession("sid:grace90-mid", DefaultCwd, "")
	sMid.LastCachedReadTokens = 60000
	if _, gr, _ := sMid.effectiveSoftDetachPolicy(400); gr != 90*time.Second {
		t.Fatalf(">=48K grace under 90s env = %v, want 90s", gr)
	}
	sLarge := NewAcpProjectSession("sid:grace90-large", DefaultCwd, "")
	sLarge.LastCachedReadTokens = 150000
	if _, gr, _ := sLarge.effectiveSoftDetachPolicy(400); gr != 120*time.Second {
		t.Fatalf(">=120K grace under 90s env = %v, want 120s", gr)
	}

	// expireDetachedTurn passiveHold under AGY_PREFILL_CANCEL_GRACE_SEC=90 logs holdSec:120
	turn := newActiveTurnState(nil, func() {})
	sHold := NewAcpProjectSession("sid:grace90-hold", DefaultCwd, "")
	sHold.SessionID = "sid-grace90-hold"
	sHold.activeTurn = turn
	turnStart := time.Now().Add(-40 * time.Second)
	if !sHold.tryStartSoftDetach(turn, "fp-grace90-123456", sHold.SessionID, "cold_start", "", turnStart, turnStart, 1, "u1", "h1", false) {
		t.Fatal("expected soft detach start")
	}
	sHold.mu.Lock()
	det := sHold.detachedTurn
	if det != nil && det.graceTimer != nil {
		det.graceTimer.Stop()
	}
	sHold.mu.Unlock()
	turn.resolveChan <- TurnResult{
		StopReason: "end_turn",
		Text:       "DONE",
		Usage:      &TurnUsage{InputTokens: 2000, CachedReadTokens: 20000, OutputTokens: 32, UsageSource: "db"},
	}
	sHold.expireDetachedTurn(det)
	sHold.mu.Lock()
	if sHold.detachedTurn != nil && sHold.detachedTurn.graceTimer != nil {
		sHold.detachedTurn.graceTimer.Stop()
	}
	sHold.detachedTurn = nil
	sHold.mu.Unlock()

	logBytes, _ := os.ReadFile(GatewayLogFile)
	if !strings.Contains(string(logBytes), `"holdSec":120`) {
		t.Fatalf("expected PREFILL_SOFT_DETACH_HOLD with holdSec:120 under grace=90, got:\n%s", string(logBytes))
	}

	// 2. Larger override (180s) is never clamped down
	t.Setenv("AGY_PREFILL_CANCEL_GRACE_SEC", "180")
	initConfig()
	if _, gr, _ := sLarge.effectiveSoftDetachPolicy(400); gr != 180*time.Second {
		t.Fatalf(">=120K grace under 180s env = %v, want 180s", gr)
	}

	// 3. Small test overrides (<45s) remain exact
	for _, sec := range []struct {
		env  string
		want time.Duration
	}{
		{"30", 30 * time.Second},
		{"10", 10 * time.Second},
	} {
		t.Setenv("AGY_PREFILL_CANCEL_GRACE_SEC", sec.env)
		initConfig()
		if _, gr, _ := sSmall.effectiveSoftDetachPolicy(4000); gr != sec.want {
			t.Fatalf("<48K grace under %ss env = %v, want %v", sec.env, gr, sec.want)
		}
		if _, gr, _ := sLarge.effectiveSoftDetachPolicy(400); gr != sec.want {
			t.Fatalf(">=120K grace under %ss env = %v, want %v", sec.env, gr, sec.want)
		}
	}

	// 4. PrefillCancelGrace = 0 disables soft detach
	PrefillCancelGrace = 0
	turnZero := newActiveTurnState(nil, func() {})
	if sLarge.tryStartSoftDetachWithChars(turnZero, "fp-zero-123456", "sid-zero", "cold_start", "", turnStart, turnStart, 1, "u1", "h1", false, 600000) {
		t.Fatal("expected PrefillCancelGrace=0 to disable soft detach")
	}

	_ = filepath.Base(GatewayLogFile)
}

func TestCarbonWarmReactive429TierRejectionPreservesModelAndDB(t *testing.T) {
	setupM2IsolatedEnv(t)
	t.Setenv("FAKE_ACP_REJECT_TIER", "low")
	baseURL, gs := startCarbonTestGateway(t)

	// Turn 1: warm gemini-4-carbon at high tier
	u1 := map[string]any{"role": "user", "content": "warm high before 429 reject low"}
	r1, raw1, e1 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "m3-warm-429-reject-low", map[string]any{
		"model":      "gemini-4-carbon",
		"max_tokens": 32000,
		"thinking":   map[string]any{"type": "enabled", "budget_tokens": 31999},
		"messages":   []map[string]any{u1},
	})
	if e1 != nil || r1.StatusCode != 200 {
		t.Fatalf("Turn 1 failed: %v %v %s", r1, e1, raw1)
	}
	var d1 map[string]any
	_ = json.Unmarshal([]byte(raw1), &d1)
	asst1 := map[string]any{"role": "assistant", "content": d1["content"]}
	sess := findSessionByConvPrefix(gs, "sid:m3-warm-429-reject-low")
	sess.mu.Lock()
	origSid := sess.SessionID
	convKey := sess.ConvKey
	if sess.ModelID != "gemini-4-carbon" || sess.ThoughtLevel != "high" {
		t.Fatalf("Turn 1 unexpected model/tier: %q/%q", sess.ModelID, sess.ThoughtLevel)
	}
	sess.mu.Unlock()

	logBefore, _ := os.ReadFile(GatewayLogFile)

	// Turn 2: [FAKE_MEDIUM_429_LOW_OK] triggers high->medium (429), then medium->low fails (FAKE_ACP_REJECT_TIER=low)
	r2, raw2, e2 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "m3-warm-429-reject-low", map[string]any{
		"model":      "gemini-4-carbon",
		"max_tokens": 32000,
		"thinking":   map[string]any{"type": "enabled", "budget_tokens": 31999},
		"messages":   []map[string]any{u1, asst1, {"role": "user", "content": "[FAKE_MEDIUM_429_LOW_OK] fail low switch"}},
	})
	if e2 != nil || r2.StatusCode != 429 {
		t.Fatalf("expected HTTP 429 when medium->low rejected, got %v %v %s", r2, e2, raw2)
	}
	sess.mu.Lock()
	if sess.SessionID != origSid || sess.ModelID != "gemini-4-carbon" || sess.ThoughtLevel != "medium" {
		t.Fatalf("expected warm session state preserved at gemini-4-carbon/medium on sid=%s, got sid=%s model=%q tier=%q",
			origSid, sess.SessionID, sess.ModelID, sess.ThoughtLevel)
	}
	sess.mu.Unlock()

	logAfter, _ := os.ReadFile(GatewayLogFile)
	deltaLog := string(logAfter[len(logBefore):])
	if !strings.Contains(deltaLog, "WARM_CAPACITY_TIER_SWITCH_SKIPPED") {
		t.Fatalf("expected WARM_CAPACITY_TIER_SWITCH_SKIPPED in log, got:\n%s", deltaLog)
	}
	if strings.Contains(deltaLog, `"model":""`) || strings.Contains(deltaLog, `"thoughtLevel":""`) {
		t.Fatalf("log must not contain empty model/thoughtLevel:\n%s", deltaLog)
	}

	var turnModel, turnTier string
	if err := GlobalStore.db.QueryRow(`SELECT model_id, thought_level FROM turns WHERE conv_key = ? ORDER BY turn_idx DESC LIMIT 1`, convKey).Scan(&turnModel, &turnTier); err != nil {
		t.Fatalf("query turns table: %v", err)
	}
	if turnModel != "gemini-4-carbon" || turnTier != "medium" {
		t.Fatalf("turns table expected model_id=gemini-4-carbon thought_level=medium, got %q/%q", turnModel, turnTier)
	}
	var convModel, convTier string
	if err := GlobalStore.db.QueryRow(`SELECT model_id, thought_level FROM conversations WHERE conv_key = ?`, convKey).Scan(&convModel, &convTier); err != nil {
		t.Fatalf("query conversations table: %v", err)
	}
	if convModel != "gemini-4-carbon" || convTier != "medium" {
		t.Fatalf("conversations table expected model_id=gemini-4-carbon thought_level=medium, got %q/%q", convModel, convTier)
	}

	// Turn 3: subsequent turn recovers cleanly on same session with 0 rollovers
	r3, raw3, e3 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "m3-warm-429-reject-low", map[string]any{
		"model":      "gemini-4-carbon",
		"max_tokens": 32000,
		"thinking":   map[string]any{"type": "enabled", "budget_tokens": 31999},
		"messages":   []map[string]any{u1, asst1, {"role": "user", "content": "recover after tier reject"}},
	})
	if e3 != nil || r3.StatusCode != 200 || !strings.Contains(raw3, "FAKE_OK") {
		t.Fatalf("Turn 3 recovery failed: %v %v %s", r3, e3, raw3)
	}
	sess.mu.Lock()
	if sess.SessionID != origSid || sess.CompactionRollovers != 0 {
		t.Fatalf("expected same sid=%s rolls=0 on recovery, got sid=%s rolls=%d", origSid, sess.SessionID, sess.CompactionRollovers)
	}
	sess.mu.Unlock()
}

func TestCarbonLRUFailModelOrTierSwitchEvictsFlashNotCarbon(t *testing.T) {
	if got := GetModel("").LruProtectionWeight(); got != 0.5 {
		t.Fatalf("GetModel(\"\").LruProtectionWeight() = %v, want 0.5", got)
	}
	if got := GetModel("nonexistent-model").LruProtectionWeight(); got != 0.5 {
		t.Fatalf("GetModel(\"nonexistent-model\").LruProtectionWeight() = %v, want 0.5", got)
	}
	if got := GetModel("gemini-4-carbon").LruProtectionWeight(); got != 4.0 {
		t.Fatalf("GetModel(\"gemini-4-carbon\").LruProtectionWeight() = %v, want 4.0", got)
	}

	t.Run("Probe 1b same-model tier reject preserves Flash ModelID and evicts Flash over Carbon 50K", func(t *testing.T) {
		setupM2IsolatedEnv(t)
		t.Setenv("AGY_MAX_LIVE_WORKERS", "2")
		t.Setenv("FAKE_ACP_REJECT_TIER", "low")
		initConfig()
		baseURL, gs := startCarbonTestGateway(t)

		// 1. Seed warm Carbon 50K (gemini-4-carbon-high)
		cU1 := map[string]any{"role": "user", "content": "[FAKE_INPUT_TOKENS:2000][FAKE_CACHE_READ:50000] seed carbon 50k"}
		cR1, cRaw1, cE1 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "p1b-carbon", map[string]any{
			"model": "gemini-4-carbon-high", "max_tokens": 128, "messages": []map[string]any{cU1},
		})
		if cE1 != nil || cR1.StatusCode != 200 {
			t.Fatalf("Carbon Turn 1 failed: %v %v %s", cR1, cE1, cRaw1)
		}
		var cD1 map[string]any
		_ = json.Unmarshal([]byte(cRaw1), &cD1)
		carbonSess := findSessionByConvPrefix(gs, "sid:p1b-carbon")
		carbonSess.mu.Lock()
		carbonOrigSid := carbonSess.SessionID
		carbonSess.mu.Unlock()

		time.Sleep(25 * time.Millisecond)

		// 2. Seed newer Flash 50K (gemini-3.8-flash-high)
		fU1 := map[string]any{"role": "user", "content": "[FAKE_INPUT_TOKENS:2000][FAKE_CACHE_READ:50000] seed flash 50k"}
		fR1, fRaw1, fE1 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "p1b-flash", map[string]any{
			"model": "gemini-3.8-flash-high", "max_tokens": 128, "messages": []map[string]any{fU1},
		})
		if fE1 != nil || fR1.StatusCode != 200 {
			t.Fatalf("Flash Turn 1 failed: %v %v %s", fR1, fE1, fRaw1)
		}
		var fD1 map[string]any
		_ = json.Unmarshal([]byte(fRaw1), &fD1)
		flashSess := findSessionByConvPrefix(gs, "sid:p1b-flash")
		flashSess.mu.Lock()
		flashOrigSid := flashSess.SessionID
		flashSess.mu.Unlock()

		// 3. Non-parked turn on Flash requests gemini-3.8-flash-low (rejected >=400 at L3281)
		fR2, fRaw2, fE2 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "p1b-flash", map[string]any{
			"model": "gemini-3.8-flash-low", "max_tokens": 128,
			"messages": []map[string]any{fU1, {"role": "assistant", "content": fD1["content"]}, {"role": "user", "content": "reject low"}},
		})
		if fE2 != nil || fR2.StatusCode < 400 {
			t.Fatalf("expected HTTP >=400 on rejected same-model tier switch, got %v %v %s", fR2, fE2, fRaw2)
		}
		flashSess.mu.Lock()
		if flashSess.SessionID != flashOrigSid || flashSess.ModelID != "gemini-3.8-flash" || flashSess.ThoughtLevel != "high" {
			t.Fatalf("expected Flash state restored to gemini-3.8-flash/high on sid=%s, got sid=%s model=%q tier=%q",
				flashOrigSid, flashSess.SessionID, flashSess.ModelID, flashSess.ThoughtLevel)
		}
		flashSess.mu.Unlock()

		// 4. Trigger 3rd worker (gemini-4-argon) -> Flash 50K must be evicted, Carbon 50K survives
		aR1, aRaw1, aE1 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "p1b-argon", map[string]any{
			"model": "gemini-4-argon", "max_tokens": 128, "messages": []map[string]any{{"role": "user", "content": "3rd worker"}},
		})
		if aE1 != nil || aR1.StatusCode != 200 {
			t.Fatalf("3rd worker failed: %v %v %s", aR1, aE1, aRaw1)
		}

		flashSess.mu.Lock()
		fSidAfter := flashSess.SessionID
		flashSess.mu.Unlock()
		carbonSess.mu.Lock()
		cSidAfter := carbonSess.SessionID
		carbonSess.mu.Unlock()
		if fSidAfter != "" || cSidAfter != carbonOrigSid {
			t.Fatalf("expected Flash evicted (sid=\"\") and Carbon alive (sid=%s), got flashSid=%q carbonSid=%q",
				carbonOrigSid, fSidAfter, cSidAfter)
		}

		off := acpRecordOffset()
		cR2, cRaw2, cE2 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "p1b-carbon", map[string]any{
			"model": "gemini-4-carbon-high", "max_tokens": 128,
			"messages": []map[string]any{cU1, {"role": "assistant", "content": cD1["content"]}, {"role": "user", "content": "follow up carbon"}},
		})
		if cE2 != nil || cR2.StatusCode != 200 {
			t.Fatalf("Carbon follow-up failed: %v %v %s", cR2, cE2, cRaw2)
		}
		if resCnt := countRecordedSessionRPCs(off, carbonOrigSid, "session/resume"); resCnt != 0 {
			t.Fatalf("expected 0 session/resume on surviving Carbon, got %d", resCnt)
		}
		carbonSess.mu.Lock()
		lastHist := carbonSess.TurnHistory[len(carbonSess.TurnHistory)-1]
		carbonSess.mu.Unlock()
		if lastHist.Mode != "sticky_delta" {
			t.Fatalf("expected Carbon follow-up mode=sticky_delta, got %q", lastHist.Mode)
		}
		logRaw, _ := os.ReadFile(GatewayLogFile)
		if !strings.Contains(string(logRaw), `"reason":"lru_cap_2"`) {
			t.Fatalf("expected lru_cap_2 eviction in log, got:\n%s", string(logRaw))
		}
	})

	t.Run("Probe 1c cross-model reject clears Flash ModelID and GetModel empty weight evicts Flash over Carbon 50K", func(t *testing.T) {
		setupM2IsolatedEnv(t)
		t.Setenv("AGY_MAX_LIVE_WORKERS", "2")
		t.Setenv("FAKE_ACP_REJECT_MODEL", "gemini-4-hydrogen")
		initConfig()
		baseURL, gs := startCarbonTestGateway(t)

		// 1. Seed warm Carbon 50K
		cU1 := map[string]any{"role": "user", "content": "[FAKE_INPUT_TOKENS:2000][FAKE_CACHE_READ:50000] seed carbon 50k"}
		cR1, cRaw1, cE1 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "p1c-carbon", map[string]any{
			"model": "gemini-4-carbon-high", "max_tokens": 128, "messages": []map[string]any{cU1},
		})
		if cE1 != nil || cR1.StatusCode != 200 {
			t.Fatalf("Carbon Turn 1 failed: %v %v %s", cR1, cE1, cRaw1)
		}
		var cD1 map[string]any
		_ = json.Unmarshal([]byte(cRaw1), &cD1)
		carbonSess := findSessionByConvPrefix(gs, "sid:p1c-carbon")
		carbonSess.mu.Lock()
		carbonOrigSid := carbonSess.SessionID
		carbonSess.mu.Unlock()

		time.Sleep(25 * time.Millisecond)

		// 2. Seed newer Flash 50K (gemini-3.8-flash-high)
		fU1 := map[string]any{"role": "user", "content": "[FAKE_INPUT_TOKENS:2000][FAKE_CACHE_READ:50000] seed flash 50k"}
		fR1, fRaw1, fE1 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "p1c-flash", map[string]any{
			"model": "gemini-3.8-flash-high", "max_tokens": 128, "messages": []map[string]any{fU1},
		})
		if fE1 != nil || fR1.StatusCode != 200 {
			t.Fatalf("Flash Turn 1 failed: %v %v %s", fR1, fE1, fRaw1)
		}
		var fD1 map[string]any
		_ = json.Unmarshal([]byte(fRaw1), &fD1)
		flashSess := findSessionByConvPrefix(gs, "sid:p1c-flash")

		// 3. Cross-model switch to rejected gemini-4-hydrogen-high (leaves ModelID == "" per TestReviewLiveModelSwitchMustBeConfirmed)
		fR2, fRaw2, fE2 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "p1c-flash", map[string]any{
			"model": "gemini-4-hydrogen-high", "max_tokens": 128,
			"messages": []map[string]any{fU1, {"role": "assistant", "content": fD1["content"]}, {"role": "user", "content": "reject hydrogen"}},
		})
		if fE2 != nil || fR2.StatusCode < 400 {
			t.Fatalf("expected HTTP >=400 on rejected cross-model switch, got %v %v %s", fR2, fE2, fRaw2)
		}
		flashSess.mu.Lock()
		if flashSess.ModelID != "" {
			t.Fatalf("expected Flash ModelID cleared on failed cross-model switch, got %q", flashSess.ModelID)
		}
		flashSess.mu.Unlock()

		// 4. Trigger 3rd worker (gemini-4-argon) -> Flash (0.5x via GetModel("")) must be evicted, Carbon survives
		aR1, aRaw1, aE1 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "p1c-argon", map[string]any{
			"model": "gemini-4-argon", "max_tokens": 128, "messages": []map[string]any{{"role": "user", "content": "3rd worker"}},
		})
		if aE1 != nil || aR1.StatusCode != 200 {
			t.Fatalf("3rd worker failed: %v %v %s", aR1, aE1, aRaw1)
		}

		flashSess.mu.Lock()
		fSidAfter := flashSess.SessionID
		flashSess.mu.Unlock()
		carbonSess.mu.Lock()
		cSidAfter := carbonSess.SessionID
		carbonSess.mu.Unlock()
		if fSidAfter != "" || cSidAfter != carbonOrigSid {
			t.Fatalf("expected Flash evicted (sid=\"\") and Carbon alive (sid=%s), got flashSid=%q carbonSid=%q",
				carbonOrigSid, fSidAfter, cSidAfter)
		}

		off := acpRecordOffset()
		cR2, cRaw2, cE2 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "p1c-carbon", map[string]any{
			"model": "gemini-4-carbon-high", "max_tokens": 128,
			"messages": []map[string]any{cU1, {"role": "assistant", "content": cD1["content"]}, {"role": "user", "content": "follow up carbon"}},
		})
		if cE2 != nil || cR2.StatusCode != 200 {
			t.Fatalf("Carbon follow-up failed: %v %v %s", cR2, cE2, cRaw2)
		}
		if resCnt := countRecordedSessionRPCs(off, carbonOrigSid, "session/resume"); resCnt != 0 {
			t.Fatalf("expected 0 session/resume on surviving Carbon, got %d", resCnt)
		}
		carbonSess.mu.Lock()
		lastHist := carbonSess.TurnHistory[len(carbonSess.TurnHistory)-1]
		carbonSess.mu.Unlock()
		if lastHist.Mode != "sticky_delta" {
			t.Fatalf("expected Carbon follow-up mode=sticky_delta, got %q", lastHist.Mode)
		}
		logRaw, _ := os.ReadFile(GatewayLogFile)
		if !strings.Contains(string(logRaw), `"reason":"lru_cap_2"`) {
			t.Fatalf("expected lru_cap_2 eviction in log, got:\n%s", string(logRaw))
		}
	})

	t.Run("stickyFallback implicit tier rejection degrades gracefully on live and hibernated wake", func(t *testing.T) {
		setupM2IsolatedEnv(t)
		t.Setenv("FAKE_ACP_REJECT_TIER", "low")
		baseURL, gs := startCarbonTestGateway(t)

		u1 := map[string]any{"role": "user", "content": "warm medium"}
		r1, raw1, e1 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "p1-sticky-fb", map[string]any{
			"model": "gemini-4-carbon", "max_tokens": 128, "messages": []map[string]any{u1},
		})
		if e1 != nil || r1.StatusCode != 200 {
			t.Fatalf("Turn 1 failed: %v %v %s", r1, e1, raw1)
		}
		var d1 map[string]any
		_ = json.Unmarshal([]byte(raw1), &d1)
		sess := findSessionByConvPrefix(gs, "sid:p1-sticky-fb")
		sess.mu.Lock()
		origSid := sess.SessionID
		sess.capacityFallbackSid = origSid
		sess.capacityFallbackTier = "low"
		sess.mu.Unlock()

		// Live session: implicit request has stickyFallback=true -> low rejected -> skips and runs on medium
		r2, raw2, e2 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "p1-sticky-fb", map[string]any{
			"model": "gemini-4-carbon", "max_tokens": 128,
			"messages": []map[string]any{u1, {"role": "assistant", "content": d1["content"]}, {"role": "user", "content": "live sticky skip"}},
		})
		if e2 != nil || r2.StatusCode != 200 || !strings.Contains(raw2, "tier=medium") {
			t.Fatalf("expected live stickyFallback to degrade gracefully to medium, got %v %v %s", r2, e2, raw2)
		}
		var d2 map[string]any
		_ = json.Unmarshal([]byte(raw2), &d2)

		// Hibernated wake: re-arm capacityFallbackTier="low", hibernate, then implicit request wakes cleanly on medium
		if !sess.HibernateWorker("test_sticky_hib", false) {
			t.Fatal("expected hibernate to succeed")
		}
		sess.mu.Lock()
		sess.capacityFallbackSid = origSid
		sess.capacityFallbackTier = "low"
		sess.mu.Unlock()

		r3, raw3, e3 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "p1-sticky-fb", map[string]any{
			"model": "gemini-4-carbon", "max_tokens": 128,
			"messages": []map[string]any{
				u1, {"role": "assistant", "content": d1["content"]},
				{"role": "user", "content": "live sticky skip"}, {"role": "assistant", "content": d2["content"]},
				{"role": "user", "content": "hib sticky skip"},
			},
		})
		if e3 != nil || r3.StatusCode != 200 || !strings.Contains(raw3, "tier=medium") {
			t.Fatalf("expected hibernated stickyFallback to degrade gracefully to medium, got %v %v %s", r3, e3, raw3)
		}
		sess.mu.Lock()
		if sess.SessionID != origSid || sess.ModelID != "gemini-4-carbon" || sess.ThoughtLevel != "medium" || sess.capacityFallbackSid != "" {
			t.Fatalf("expected clean state on origSid=%s gemini-4-carbon/medium, got sid=%s model=%q tier=%q fb=%q",
				origSid, sess.SessionID, sess.ModelID, sess.ThoughtLevel, sess.capacityFallbackSid)
		}
		sess.mu.Unlock()
	})
}
