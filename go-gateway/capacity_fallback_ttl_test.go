package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	gwcfg "agy-acp-gateway-go/internal/config"
)

func TestNextFallbackTier(t *testing.T) {
	cases := []struct {
		model string
		tier  string
		want  string
	}{
		{"gemini-4-neon", "xhigh", "high"},
		{"gemini-4-neon", "high", "medium"},
		{"gemini-4-neon", "medium", ""},
		{"gemini-4-hydrogen", "xhigh", "high"},
		{"gemini-4-hydrogen", "high", "medium"},
		{"gemini-4-hydrogen", "medium", ""},
		{"gemini-4-carbon", "xhigh", "medium"},
		{"gemini-4-carbon", "high", "medium"},
		{"gemini-4-carbon", "medium", "low"},
		{"gemini-4-carbon", "low", ""},
		{"gemini-4-argon", "high", "medium"},
		{"gemini-4-argon", "medium", ""},
		{"gemini-3.8-flash", "high", "medium"},
		{"gemini-3.8-flash", "medium", ""},
	}
	for _, tc := range cases {
		if got := nextFallbackTier(tc.model, tc.tier); got != tc.want {
			t.Fatalf("nextFallbackTier(%q, %q) = %q, want %q", tc.model, tc.tier, got, tc.want)
		}
	}
}

func TestCapacityFallbackConfigParsingAndZeroOptOut(t *testing.T) {
	origTTL, origMax := CapacityFallbackTTL, CapacityFallbackMaxTTL
	origNow := capacityFallbackNow
	t.Cleanup(func() {
		CapacityFallbackTTL = origTTL
		CapacityFallbackMaxTTL = origMax
		capacityFallbackNow = origNow
	})

	// Default values when env unset
	t.Setenv("AGY_CAPACITY_FALLBACK_TTL_SEC", "")
	t.Setenv("AGY_CAPACITY_FALLBACK_MAX_TTL_SEC", "")
	snap := gwcfg.LoadFromEnv()
	if snap.CapacityFallbackTTL != 300*time.Second || snap.CapacityFallbackMaxTTL != 1800*time.Second {
		t.Fatalf("unexpected defaults: ttl=%v max=%v", snap.CapacityFallbackTTL, snap.CapacityFallbackMaxTTL)
	}

	// Negative TTL falls back to default 300s; max < base clamps to base
	t.Setenv("AGY_CAPACITY_FALLBACK_TTL_SEC", "-10")
	t.Setenv("AGY_CAPACITY_FALLBACK_MAX_TTL_SEC", "60")
	snap = gwcfg.LoadFromEnv()
	if snap.CapacityFallbackTTL != 300*time.Second || snap.CapacityFallbackMaxTTL != 300*time.Second {
		t.Fatalf("unexpected clamp/fallback: ttl=%v max=%v", snap.CapacityFallbackTTL, snap.CapacityFallbackMaxTTL)
	}

	// Explicit 0 disables expiry (opt-out)
	t.Setenv("AGY_CAPACITY_FALLBACK_TTL_SEC", "0")
	t.Setenv("AGY_CAPACITY_FALLBACK_MAX_TTL_SEC", "1800")
	snap = gwcfg.LoadFromEnv()
	if snap.CapacityFallbackTTL != 0 {
		t.Fatalf("expected AGY_CAPACITY_FALLBACK_TTL_SEC=0 -> 0, got %v", snap.CapacityFallbackTTL)
	}

	CapacityFallbackTTL = 0
	CapacityFallbackMaxTTL = 30 * time.Minute
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	capacityFallbackNow = func() time.Time { return now }

	sess := NewAcpProjectSession("sid:cap-optout", DefaultCwd, "")
	sess.SessionID = "sid-optout-1"
	sess.ModelID = "gemini-4-carbon"
	sess.ThoughtLevel = "medium"
	sess.LastUsedAt = now.Add(-10 * time.Minute)

	sess.mu.Lock()
	strikes, cd := sess.armCapacityFallbackLocked("sid-optout-1", "medium", "capacity_429", now)
	untilZero := sess.capacityFallbackUntil.IsZero()
	clamped, sticky, probe, _ := sess.evaluateCapacityFallbackLocked(ModelSelection{
		Model: GetModel("gemini-4-carbon"), Tier: "high",
	}, "sid-optout-1", "gemini-4-carbon", now.Add(24*time.Hour))
	sess.mu.Unlock()

	if strikes != 1 || cd != 0 || !untilZero {
		t.Fatalf("expected strikes=1 cd=0 zero until on TTL=0 opt-out, got strikes=%d cd=%v zero=%v", strikes, cd, untilZero)
	}
	if clamped != "medium" || !sticky || probe {
		t.Fatalf("expected TTL=0 fallback never expires, got clamped=%q sticky=%v probe=%v", clamped, sticky, probe)
	}
}

func TestCapacityFallbackZeroUntilNeverExpires(t *testing.T) {
	origTTL, origMax := CapacityFallbackTTL, CapacityFallbackMaxTTL
	t.Cleanup(func() {
		CapacityFallbackTTL = origTTL
		CapacityFallbackMaxTTL = origMax
	})
	CapacityFallbackTTL = 5 * time.Minute
	CapacityFallbackMaxTTL = 30 * time.Minute

	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	sess := NewAcpProjectSession("sid:cap-legacy", DefaultCwd, "")
	sess.SessionID = "sid-legacy-1"
	sess.ModelID = "gemini-4-carbon"
	sess.ThoughtLevel = "low"
	sess.LastUsedAt = now.Add(-2 * time.Hour)
	// Legacy direct field assignment leaving capacityFallbackUntil zero
	sess.capacityFallbackSid = "sid-legacy-1"
	sess.capacityFallbackTier = "low"

	sess.mu.Lock()
	clamped, sticky, probe, probeFrom := sess.evaluateCapacityFallbackLocked(ModelSelection{
		Model: GetModel("gemini-4-carbon"), Tier: "high",
	}, "sid-legacy-1", "gemini-4-carbon", now.Add(5*time.Hour))
	tier, remSec, _, _ := sess.capacityFallbackTelemetryLocked(now.Add(5 * time.Hour))
	sess.mu.Unlock()

	if clamped != "low" || !sticky || probe || probeFrom != "" {
		t.Fatalf("legacy zero capacityFallbackUntil must never expire, got clamped=%q sticky=%v probe=%v from=%q", clamped, sticky, probe, probeFrom)
	}
	if tier != "low" || remSec != 0 {
		t.Fatalf("telemetry for legacy non-expiring fallback: got tier=%q rem=%d, want low/0", tier, remSec)
	}
}

func TestCapacityFallbackTTLExpiryBackoffAndReset(t *testing.T) {
	setupM2IsolatedEnv(t)
	origTTL, origMax := CapacityFallbackTTL, CapacityFallbackMaxTTL
	origNow := capacityFallbackNow
	t.Cleanup(func() {
		CapacityFallbackTTL = origTTL
		CapacityFallbackMaxTTL = origMax
		capacityFallbackNow = origNow
	})

	CapacityFallbackTTL = 5 * time.Minute
	CapacityFallbackMaxTTL = 30 * time.Minute
	curNow := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	capacityFallbackNow = func() time.Time { return curNow }

	sess := NewAcpProjectSession("sid:cap-backoff", DefaultCwd, "")
	sess.SessionID = "sid-bo-1"
	sess.ModelID = "gemini-4-neon"
	sess.ThoughtLevel = "high"
	sess.LastUsedAt = curNow.Add(-5 * time.Minute) // cold boundary ready

	selXHigh := ModelSelection{Model: GetModel("gemini-4-neon"), Tier: "xhigh"}

	// Strike 1: 5m -> Strike 2: 10m -> Strike 3: 20m -> Strike 4: cap 30m -> Strike 5: cap 30m
	wantCooldowns := []time.Duration{
		5 * time.Minute,
		10 * time.Minute,
		20 * time.Minute,
		30 * time.Minute,
		30 * time.Minute,
	}
	for i, wantCD := range wantCooldowns {
		sess.mu.Lock()
		strikes, cd := sess.armCapacityFallbackLocked("sid-bo-1", "high", "capacity_429", curNow)
		if strikes != i+1 || cd != wantCD {
			sess.mu.Unlock()
			t.Fatalf("strike %d: got strikes=%d cd=%v, want %d/%v", i+1, strikes, cd, i+1, wantCD)
		}
		// Telemetry reports active tier & remaining seconds
		telTier, telRem, telStrikes, telReason := sess.capacityFallbackTelemetryLocked(curNow.Add(30 * time.Second))
		if telTier != "high" || telRem != int((wantCD-30*time.Second).Seconds()) || telStrikes != i+1 || telReason != "capacity_429" {
			sess.mu.Unlock()
			t.Fatalf("strike %d telemetry mismatch: tier=%q rem=%d strikes=%d reason=%q", i+1, telTier, telRem, telStrikes, telReason)
		}
		// 1 second before expiry: still clamped
		clamped, sticky, probe, _ := sess.evaluateCapacityFallbackLocked(selXHigh, "sid-bo-1", "gemini-4-neon", curNow.Add(wantCD-time.Second))
		if clamped != "high" || !sticky || probe {
			sess.mu.Unlock()
			t.Fatalf("strike %d pre-expiry: got clamped=%q sticky=%v probe=%v", i+1, clamped, sticky, probe)
		}
		// At expiry: probes recovery while preserving strikes!
		curNow = curNow.Add(wantCD)
		clamped, sticky, probe, fromTier := sess.evaluateCapacityFallbackLocked(selXHigh, "sid-bo-1", "gemini-4-neon", curNow)
		probing := sess.capacityFallbackProbing
		preservedStrikes := sess.capacityFallbackStrikes
		sess.mu.Unlock()

		if clamped != "xhigh" || sticky || !probe || fromTier != "high" || !probing || preservedStrikes != i+1 {
			t.Fatalf("strike %d at expiry: clamped=%q sticky=%v probe=%v from=%q probing=%v strikes=%d",
				i+1, clamped, sticky, probe, fromTier, probing, preservedStrikes)
		}
	}

	// Clean probed turn completes -> clears probing & resets strikes to 0 and logs CAPACITY_FALLBACK_RECOVERED
	sess.mu.Lock()
	sess.ThoughtLevel = "xhigh"
	sess.mu.Unlock()
	sess.logTurnStats("sticky_delta", 1200, TurnResult{
		StopReason: "end_turn",
		Text:       "ok",
		TtftMs:     400,
		Usage: &TurnUsage{
			InputTokens: 1000, CachedReadTokens: 40000, ContextInputTokens: 1000, ContextCachedTokens: 40000,
			OutputTokens: 120, StepsInPrompt: 1, UsageSource: "db",
		},
	})

	sess.mu.Lock()
	if sess.capacityFallbackProbing || sess.capacityFallbackStrikes != 0 || sess.capacityFallbackStrikeSid != "" {
		sess.mu.Unlock()
		t.Fatalf("expected clean probed turn to reset probing & strikes, got probing=%v strikes=%d strikeSid=%q",
			sess.capacityFallbackProbing, sess.capacityFallbackStrikes, sess.capacityFallbackStrikeSid)
	}
	sess.mu.Unlock()

	logRaw, _ := os.ReadFile(GatewayLogFile)
	if !strings.Contains(string(logRaw), "CAPACITY_FALLBACK_RECOVERED") {
		t.Fatalf("expected CAPACITY_FALLBACK_RECOVERED event in log, got:\n%s", string(logRaw))
	}

	// Carbon low -> medium step-up recovery before probing high
	cSess := NewAcpProjectSession("sid:cap-carbon-stepup", DefaultCwd, "")
	cSess.SessionID = "sid-c-1"
	cSess.ModelID = "gemini-4-carbon"
	cSess.ThoughtLevel = "low"
	cSess.LastUsedAt = curNow.Add(-5 * time.Minute)
	cSelHigh := ModelSelection{Model: GetModel("gemini-4-carbon"), Tier: "high"}

	cSess.mu.Lock()
	cSess.armCapacityFallbackLocked("sid-c-1", "low", "capacity_429", curNow)
	curNow = curNow.Add(5 * time.Minute)
	clamped1, sticky1, probe1, from1 := cSess.evaluateCapacityFallbackLocked(cSelHigh, "sid-c-1", "gemini-4-carbon", curNow)
	tierAfterStep1 := cSess.capacityFallbackTier
	untilAfterStep1 := cSess.capacityFallbackUntil
	cSess.mu.Unlock()

	if clamped1 != "medium" || !sticky1 || !probe1 || from1 != "low" || tierAfterStep1 != "medium" || !untilAfterStep1.Equal(curNow.Add(5*time.Minute)) {
		t.Fatalf("Carbon low->medium step-up failed: clamped=%q sticky=%v probe=%v from=%q fbTier=%q until=%v",
			clamped1, sticky1, probe1, from1, tierAfterStep1, untilAfterStep1)
	}

	// After second baseTTL expires, Carbon medium steps up to high
	curNow = curNow.Add(5 * time.Minute)
	cSess.mu.Lock()
	clamped2, sticky2, probe2, from2 := cSess.evaluateCapacityFallbackLocked(cSelHigh, "sid-c-1", "gemini-4-carbon", curNow)
	cSess.mu.Unlock()
	if clamped2 != "high" || sticky2 || !probe2 || from2 != "medium" {
		t.Fatalf("Carbon medium->high step-up failed: clamped=%q sticky=%v probe=%v from=%q", clamped2, sticky2, probe2, from2)
	}
}

func TestCapacityFallbackHotCacheBoundaryGating(t *testing.T) {
	origTTL, origMax := CapacityFallbackTTL, CapacityFallbackMaxTTL
	t.Cleanup(func() {
		CapacityFallbackTTL = origTTL
		CapacityFallbackMaxTTL = origMax
	})
	CapacityFallbackTTL = 5 * time.Minute
	CapacityFallbackMaxTTL = 30 * time.Minute

	t0 := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	selHigh := ModelSelection{Model: GetModel("gemini-4-carbon"), Tier: "high"}

	makeHotSession := func() *AcpProjectSession {
		s := NewAcpProjectSession("sid:cap-hot", DefaultCwd, "")
		s.SessionID = "sid-hot-1"
		s.ModelID = "gemini-4-carbon"
		s.ThoughtLevel = "medium"
		s.LastCachedReadTokens = 120000
		s.LastReportedContextUsed = 120000
		s.ZeroCacheStreak = 0
		s.LastUsedAt = t0.Add(4*time.Minute + 40*time.Second) // active burst (20s idle at expiry)
		s.mu.Lock()
		s.armCapacityFallbackLocked("sid-hot-1", "medium", "capacity_429", t0)
		s.mu.Unlock()
		return s
	}

	// 1. Hot large-context active burst: expired at t0+5m MUST defer probe
	s := makeHotSession()
	s.mu.Lock()
	clamped, sticky, probe, _ := s.evaluateCapacityFallbackLocked(selHigh, "sid-hot-1", "gemini-4-carbon", t0.Add(5*time.Minute))
	s.mu.Unlock()
	if clamped != "medium" || !sticky || probe {
		t.Fatalf("expected hot large-context burst to defer probe, got clamped=%q sticky=%v probe=%v", clamped, sticky, probe)
	}

	// 2. PromptInFlight == true NEVER probes even if idle >= 3m, smallCtx, ZeroCacheStreak > 0, and defer cap passed
	sInFlight := makeHotSession()
	sInFlight.PromptInFlight = true
	sInFlight.LastCachedReadTokens = 1000
	sInFlight.LastReportedContextUsed = 1000
	sInFlight.ZeroCacheStreak = 3
	sInFlight.mu.Lock()
	clamped, sticky, probe, _ = sInFlight.evaluateCapacityFallbackLocked(selHigh, "sid-hot-1", "gemini-4-carbon", t0.Add(20*time.Minute))
	sInFlight.mu.Unlock()
	if clamped != "medium" || !sticky || probe {
		t.Fatalf("PromptInFlight=true must NEVER probe, got clamped=%q sticky=%v probe=%v", clamped, sticky, probe)
	}

	// 3a. Cold idle gap >= 3m unblocks probe
	sIdle := makeHotSession()
	sIdle.LastUsedAt = t0.Add(2 * time.Minute) // 3m01s idle at t0+5m01s
	sIdle.mu.Lock()
	clamped, sticky, probe, from := sIdle.evaluateCapacityFallbackLocked(selHigh, "sid-hot-1", "gemini-4-carbon", t0.Add(5*time.Minute+time.Second))
	sIdle.mu.Unlock()
	if clamped != "high" || sticky || !probe || from != "medium" {
		t.Fatalf("idle >= 3m should unblock probe, got clamped=%q sticky=%v probe=%v from=%q", clamped, sticky, probe, from)
	}

	// 3b. Waking from hibernation unblocks probe
	sHib := makeHotSession()
	sHib.SessionID = ""
	sHib.HibernatedSessionID = "sid-hot-1"
	sHib.mu.Lock()
	clamped, sticky, probe, from = sHib.evaluateCapacityFallbackLocked(selHigh, "sid-hot-1", "gemini-4-carbon", t0.Add(5*time.Minute))
	sHib.mu.Unlock()
	if clamped != "high" || sticky || !probe || from != "medium" {
		t.Fatalf("hibernated wake should unblock probe, got clamped=%q sticky=%v probe=%v from=%q", clamped, sticky, probe, from)
	}

	// 3c. Small context (< 48K) unblocks probe
	sSmall := makeHotSession()
	sSmall.LastCachedReadTokens = 20000
	sSmall.LastReportedContextUsed = 32000
	sSmall.mu.Lock()
	clamped, sticky, probe, from = sSmall.evaluateCapacityFallbackLocked(selHigh, "sid-hot-1", "gemini-4-carbon", t0.Add(5*time.Minute))
	sSmall.mu.Unlock()
	if clamped != "high" || sticky || !probe || from != "medium" {
		t.Fatalf("small context (<48K) should unblock probe, got clamped=%q sticky=%v probe=%v from=%q", clamped, sticky, probe, from)
	}

	// 3d. Lower tier missing cache (ZeroCacheStreak > 0) unblocks probe
	sMiss := makeHotSession()
	sMiss.ZeroCacheStreak = 1
	sMiss.mu.Lock()
	clamped, sticky, probe, from = sMiss.evaluateCapacityFallbackLocked(selHigh, "sid-hot-1", "gemini-4-carbon", t0.Add(5*time.Minute))
	sMiss.mu.Unlock()
	if clamped != "high" || sticky || !probe || from != "medium" {
		t.Fatalf("ZeroCacheStreak > 0 should unblock probe, got clamped=%q sticky=%v probe=%v from=%q", clamped, sticky, probe, from)
	}

	// 3e. Hard max-defer cap reached (now - until >= CapacityFallbackTTL) unblocks probe even in continuous burst
	sCap := makeHotSession()
	sCap.LastUsedAt = t0.Add(9*time.Minute + 50*time.Second) // still only 10s idle at t0+10m
	sCap.mu.Lock()
	clamped, sticky, probe, from = sCap.evaluateCapacityFallbackLocked(selHigh, "sid-hot-1", "gemini-4-carbon", t0.Add(10*time.Minute))
	sCap.mu.Unlock()
	if clamped != "high" || sticky || !probe || from != "medium" {
		t.Fatalf("defer cap reached should unblock probe, got clamped=%q sticky=%v probe=%v from=%q", clamped, sticky, probe, from)
	}
}

func TestCapacityFallbackExplicitTierPreventsZeroCacheDegrade(t *testing.T) {
	setupM2IsolatedEnv(t)
	origTTL, origMax := CapacityFallbackTTL, CapacityFallbackMaxTTL
	origNow := capacityFallbackNow
	t.Cleanup(func() {
		CapacityFallbackTTL = origTTL
		CapacityFallbackMaxTTL = origMax
		capacityFallbackNow = origNow
	})
	CapacityFallbackTTL = 5 * time.Minute
	CapacityFallbackMaxTTL = 30 * time.Minute
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	capacityFallbackNow = func() time.Time { return now }

	sess := NewAcpProjectSession("sid:cap-explicit-guard", DefaultCwd, "")
	sess.SessionID = "sid-exp-1"
	sess.ModelID = "gemini-4-carbon"
	sess.ThoughtLevel = "high"
	sess.lastTurnExplicitTier = true

	makeZeroTurn := func(ctxTok int) TurnResult {
		return TurnResult{
			StopReason: "end_turn",
			TtftMs:     18000,
			Usage: &TurnUsage{
				InputTokens: ctxTok, CachedReadTokens: 0,
				ContextInputTokens: ctxTok, ContextCachedTokens: 0, RawUncachedInputTokens: ctxTok,
				OutputTokens: 150, StepsInPrompt: 1, UsageSource: "db",
			},
		}
	}

	sess.logTurnStats("cold_start", 20000, makeZeroTurn(60000))
	for _, ctxTok := range []int{64000, 68000, 72000} {
		sess.logTurnStats("sticky_delta", 24000, makeZeroTurn(ctxTok))
	}
	sess.mu.Lock()
	if sess.capacityFallbackSid != "" || sess.capacityFallbackTier != "" {
		sess.mu.Unlock()
		t.Fatalf("lastTurnExplicitTier=true must prevent HIGH_TIER_CACHE_DEGRADE_FALLBACK, got sid=%q tier=%q",
			sess.capacityFallbackSid, sess.capacityFallbackTier)
	}
	// Flip back to implicit tier -> 2 slow zero-cache misses now arm fallback with TTL & reason
	sess.lastTurnExplicitTier = false
	sess.mu.Unlock()

	sess.logTurnStats("sticky_delta", 24000, makeZeroTurn(76000))
	sess.logTurnStats("sticky_delta", 24000, makeZeroTurn(80000))

	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.capacityFallbackSid != "sid-exp-1" || sess.capacityFallbackTier != "medium" ||
		sess.capacityFallbackReason != "zero_cache_degrade" || sess.capacityFallbackStrikes != 1 ||
		!sess.capacityFallbackUntil.Equal(now.Add(5*time.Minute)) {
		t.Fatalf("expected implicit slow zero-cache misses to arm fallback with TTL, got sid=%q tier=%q reason=%q strikes=%d until=%v",
			sess.capacityFallbackSid, sess.capacityFallbackTier, sess.capacityFallbackReason, sess.capacityFallbackStrikes, sess.capacityFallbackUntil)
	}
}

func TestFingerprintThinkingHardening(t *testing.T) {
	baseMsgs := []map[string]any{
		{"role": "user", "content": "Explain the architecture"},
		{
			"role": "assistant",
			"content": []any{
				map[string]any{"type": "text", "text": "Here is the summary."},
			},
		},
		{"role": "user", "content": "Continue with step 2"},
	}

	withThinkingAny := []map[string]any{
		{"role": "user", "content": "Explain the architecture"},
		{
			"role":              "assistant",
			"reasoning_content": "internal chain of thought v1",
			"reasoning":         map[string]any{"summary": "short"},
			"content": []any{
				map[string]any{"type": "thinking", "thinking": "step 1 thought", "signature": "sig-aaa"},
				map[string]any{"type": "redacted_thinking", "data": "opaque-123"},
				map[string]any{"type": "text", "text": "Here is the summary.", "cache_control": map[string]any{"type": "ephemeral"}},
			},
		},
		{"role": "user", "content": "Continue with step 2"},
	}

	withAlteredThinkingTyped := []map[string]any{
		{"role": "user", "content": "Explain the architecture"},
		{
			"role":              "assistant",
			"reasoning_content": "",
			"reasoning":         nil,
			"content": []map[string]any{
				{"type": "thinking", "thinking": "COMPLETELY DIFFERENT THOUGHT", "signature": "sig-zzz"},
				{"type": "text", "text": "Here is the summary."},
			},
		},
		{"role": "user", "content": "Continue with step 2"},
	}

	editedTextMsgs := []map[string]any{
		{"role": "user", "content": "Explain the architecture"},
		{
			"role": "assistant",
			"content": []any{
				map[string]any{"type": "text", "text": "Edited visible text!"},
			},
		},
		{"role": "user", "content": "Continue with step 2"},
	}

	// 1. computeMessagesSliceFingerprint & cancelledHistoryDiverged
	hBase := computeMessagesSliceFingerprint(baseMsgs)
	hWith := computeMessagesSliceFingerprint(withThinkingAny)
	hAltered := computeMessagesSliceFingerprint(withAlteredThinkingTyped)
	hEdited := computeMessagesSliceFingerprint(editedTextMsgs)

	if hBase == "" || hBase != hWith || hBase != hAltered {
		t.Fatalf("computeMessagesSliceFingerprint must ignore thinking/redacted_thinking/reasoning_content/reasoning: base=%s with=%s altered=%s", hBase, hWith, hAltered)
	}
	if hBase == hEdited {
		t.Fatal("computeMessagesSliceFingerprint must change when visible text changes")
	}
	if cancelledHistoryDiverged(withThinkingAny, len(baseMsgs), false, hBase) {
		t.Fatal("cancelledHistoryDiverged must return false when only thinking/reasoning blocks differ")
	}
	if cancelledHistoryDiverged(withAlteredThinkingTyped, len(baseMsgs), false, hBase) {
		t.Fatal("cancelledHistoryDiverged must return false when typed thinking/reasoning fields differ")
	}
	if !cancelledHistoryDiverged(editedTextMsgs, len(baseMsgs), false, hBase) {
		t.Fatal("cancelledHistoryDiverged must return true when visible message text changed")
	}

	// 2. computeTurnRequestFingerprintPrecomputed & computeTurnRequestFingerprintWithDirective
	tools := []ToolDef{{Name: "Read"}}
	_, digests := toolCatalogFingerprintAndDigests(tools)
	reqBase := computeTurnRequestFingerprintPrecomputed("gemini-4-carbon", "high", "/tmp", digests, baseMsgs, "")
	reqWith := computeTurnRequestFingerprintPrecomputed("gemini-4-carbon", "high", "/tmp", digests, withThinkingAny, "")
	reqAltered := computeTurnRequestFingerprintPrecomputed("gemini-4-carbon", "high", "/tmp", digests, withAlteredThinkingTyped, "")
	reqEdited := computeTurnRequestFingerprintPrecomputed("gemini-4-carbon", "high", "/tmp", digests, editedTextMsgs, "")

	if reqBase == "" || reqBase != reqWith || reqBase != reqAltered {
		t.Fatalf("computeTurnRequestFingerprintPrecomputed must ignore thinking/reasoning fields: base=%s with=%s altered=%s", reqBase, reqWith, reqAltered)
	}
	if reqBase == reqEdited {
		t.Fatal("computeTurnRequestFingerprintPrecomputed must change when visible text changes")
	}

	dirBase := computeTurnRequestFingerprintWithDirective("gemini-4-carbon", "high", "Primary working directory: /tmp", tools, baseMsgs, "auto")
	dirWith := computeTurnRequestFingerprintWithDirective("gemini-4-carbon", "high", "Primary working directory: /tmp", tools, withThinkingAny, "auto")
	if dirBase != dirWith {
		t.Fatalf("computeTurnRequestFingerprintWithDirective mismatch: base=%s with=%s", dirBase, dirWith)
	}
}

func TestCapacityFallbackDetachedStandaloneCompactionExpires(t *testing.T) {
	setupM2IsolatedEnv(t)
	sess := NewAcpProjectSession("sid:standalone-compact-detach", DefaultCwd, "")
	sess.SessionID = "sid-compact-1"
	sess.MessageCount = 6

	ctx := withStandaloneCompaction(context.Background(), true)
	if !isStandaloneCompactionFromContext(ctx) {
		t.Fatal("expected standalone compaction context flag to be true")
	}
	turn := newActiveTurnState(nil, nil)
	det := &detachedTurnState{
		reqFp:                  "fp-standalone-compact-1234",
		turn:                   turn,
		sid:                    "sid-compact-1",
		mode:                   "cold_start",
		turnStart:              time.Now().Add(-10 * time.Second),
		msgCount:               6,
		isCompactionReq:        true,
		isStandaloneCompaction: true,
	}
	sess.mu.Lock()
	sess.activeTurn = turn
	sess.detachedTurn = det
	sess.mu.Unlock()

	sess.settleDetachedTurn(det, "prefill_grace_expired", "PREFILL_SOFT_DETACH_EXPIRED")

	sess.mu.Lock()
	sidAfter := sess.SessionID
	hibAfter := sess.HibernatedSessionID
	msgAfter := sess.MessageCount
	sess.mu.Unlock()

	if sidAfter != "" || hibAfter != "" || msgAfter != 0 {
		t.Fatalf("expired standalone compaction detached turn must force-hibernate immediately, got sid=%q hib=%q msgCount=%d", sidAfter, hibAfter, msgAfter)
	}
}

func TestCapacityFallbackPrometheusMetrics(t *testing.T) {
	origTTL, origMax := CapacityFallbackTTL, CapacityFallbackMaxTTL
	t.Cleanup(func() {
		CapacityFallbackTTL = origTTL
		CapacityFallbackMaxTTL = origMax
	})
	CapacityFallbackTTL = 5 * time.Minute
	CapacityFallbackMaxTTL = 30 * time.Minute

	gs := NewGatewayServer()
	defer gs.Close()

	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)

	sLiveActive := NewAcpProjectSession("sid:prom-active", DefaultCwd, "")
	sLiveActive.SessionID = "sid-prom-1"
	sLiveActive.ModelID = "gemini-4-carbon"
	sLiveActive.ThoughtLevel = "medium"

	sHibFallback := NewAcpProjectSession("sid:prom-hib", DefaultCwd, "")
	sHibFallback.HibernatedSessionID = "sid-prom-2"
	sHibFallback.ModelID = "gemini-4-carbon"
	sHibFallback.ThoughtLevel = "low"

	sLiveNormal := NewAcpProjectSession("sid:prom-normal", DefaultCwd, "")
	sLiveNormal.SessionID = "sid-prom-3"
	sLiveNormal.ModelID = "gemini-4-neon"
	sLiveNormal.ThoughtLevel = "xhigh"

	evBefore := capacityFallbackEventsTotal.Load()
	prBefore := capacityFallbackProbesTotal.Load()

	sLiveActive.mu.Lock()
	sLiveActive.armCapacityFallbackLocked("sid-prom-1", "medium", "capacity_429", now)
	sLiveActive.mu.Unlock()

	sHibFallback.mu.Lock()
	sHibFallback.armCapacityFallbackLocked("sid-prom-2", "low", "zero_cache_degrade", now)
	// Expire and probe sHibFallback (waking from hibernation -> unblocked)
	sHibFallback.evaluateCapacityFallbackLocked(ModelSelection{
		Model: GetModel("gemini-4-carbon"), Tier: "high",
	}, "sid-prom-2", "gemini-4-carbon", now.Add(5*time.Minute))
	sHibFallback.mu.Unlock()

	gs.mu.Lock()
	gs.sessions[sLiveActive.ConvKey] = sLiveActive
	gs.sessions[sHibFallback.ConvKey] = sHibFallback
	gs.sessions[sLiveNormal.ConvKey] = sLiveNormal
	gs.mu.Unlock()

	prom := RenderPrometheusMetrics(gs)

	wantLines := []string{
		"agy_gateway_capacity_fallback_active_sessions 1",
		fmt.Sprintf("agy_gateway_capacity_fallback_events_total %d", evBefore+2),
		fmt.Sprintf("agy_gateway_capacity_fallback_probes_total %d", prBefore+1),
	}
	for _, want := range wantLines {
		if !strings.Contains(prom, want) {
			t.Fatalf("RenderPrometheusMetrics missing %q\n---\n%s", want, prom)
		}
	}
}
