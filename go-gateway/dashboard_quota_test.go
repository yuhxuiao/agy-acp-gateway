package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func quotaTestGroups(gemini, claude bool, gemRem float64, reset5h, resetWk string) []QuotaGroup {
	var gs []QuotaGroup
	if gemini {
		gs = append(gs, QuotaGroup{Name: "Gemini models", Buckets: []QuotaBucket{
			{Window: "5h", RemainingFraction: gemRem, UsedFraction: 1 - gemRem, ResetTime: reset5h},
			{Window: "weekly", RemainingFraction: gemRem, UsedFraction: 1 - gemRem, ResetTime: resetWk},
		}})
	}
	if claude {
		gs = append(gs, QuotaGroup{Name: "Claude models", Buckets: []QuotaBucket{
			{Window: "5h", RemainingFraction: 0.5, UsedFraction: 0.5, ResetTime: reset5h},
			{Window: "weekly", RemainingFraction: 0.75, UsedFraction: 0.25, ResetTime: resetWk},
		}})
	}
	return gs
}

func assertUnknownGeminiNumbers(t *testing.T, q QuotaSnapshot) {
	t.Helper()
	d := q.Deduction
	if d.PoolUsed5hUsd != nil || d.PoolRem5hUsd != nil || d.PoolUsedWeeklyUsd != nil || d.PoolRemWeeklyUsd != nil {
		t.Fatalf("unknown Gemini pool must not report dollars: %+v", d)
	}
	if d.Rem5hTokensAt99PctCache != nil || d.Rem5hTokensAt0PctCache != nil || d.RemWeeklyTokensAt99PctCache != nil || d.RemWeeklyTokensAt0PctCache != nil {
		t.Fatalf("unknown Gemini pool must not report runnable tokens: %+v", d)
	}
}

func TestDashboardQuotaStates(t *testing.T) {
	now := time.Date(2026, 11, 11, 8, 0, 0, 0, time.UTC)
	fresh := now.Add(-2 * time.Minute).Format(time.RFC3339)
	r5 := now.Add(3 * time.Hour).Format(time.RFC3339)
	rw := now.Add(4 * 24 * time.Hour).Format(time.RFC3339)

	t.Run("error without cache", func(t *testing.T) {
		q, _ := buildQuotaView(QuotaSnapshot{Status: "error", UpdatedAt: fresh, Error: "exit status 1", Groups: []QuotaGroup{}}, nil, now)
		if q.GeminiPool.State != QuotaStateError || q.ThirdPartyPool.State != QuotaStateError || q.ThirdPartyPool.Available {
			t.Fatalf("states: gem=%+v tp=%s", q.GeminiPool, q.ThirdPartyPool.State)
		}
		if q.GeminiPool.Error == "" {
			t.Fatal("error text must reach the dashboard")
		}
		assertUnknownGeminiNumbers(t, q)
		if q.ThirdPartyPool.Window5h.RemainingFraction != nil || q.ThirdPartyPool.WindowWeekly.UsedFraction != nil {
			t.Fatal("unknown third-party pool must report null fractions")
		}
		raw, _ := json.Marshal(q)
		if !strings.Contains(string(raw), `"poolRem5hUsd":null`) || !strings.Contains(string(raw), `"remainingFraction":null`) {
			t.Fatalf("unknown values must serialise as null: %s", raw)
		}
	})

	t.Run("stale after failed refresh", func(t *testing.T) {
		snap := QuotaSnapshot{Status: "stale", UpdatedAt: now.Add(-40 * time.Minute).Format(time.RFC3339), Error: "timeout", Groups: quotaTestGroups(true, true, 0.8, r5, rw)}
		q, _ := buildQuotaView(snap, nil, now)
		if q.GeminiPool.State != QuotaStateStale || q.ThirdPartyPool.State != QuotaStateStale {
			t.Fatalf("states: gem=%s tp=%s", q.GeminiPool.State, q.ThirdPartyPool.State)
		}
		if q.GeminiPool.UpdatedAt != snap.UpdatedAt {
			t.Fatalf("stale state must carry the reading time, got %q", q.GeminiPool.UpdatedAt)
		}
		if q.Deduction.PoolRem5hUsd == nil || *q.Deduction.PoolRem5hUsd != 160 {
			t.Fatalf("stale reading keeps its last numbers: %+v", q.Deduction.PoolRem5hUsd)
		}
	})

	t.Run("stale by age", func(t *testing.T) {
		snap := QuotaSnapshot{Status: "ok", UpdatedAt: now.Add(-45 * time.Minute).Format(time.RFC3339), Groups: quotaTestGroups(true, true, 0.8, r5, rw)}
		q, _ := buildQuotaView(snap, nil, now)
		if q.GeminiPool.State != QuotaStateStale {
			t.Fatalf("45-minute-old reading must be stale, got %s", q.GeminiPool.State)
		}
	})

	t.Run("empty groups", func(t *testing.T) {
		q, _ := buildQuotaView(QuotaSnapshot{Status: "ok", UpdatedAt: fresh, Groups: []QuotaGroup{}}, nil, now)
		if q.GeminiPool.State != QuotaStateNotEnabled || q.ThirdPartyPool.State != QuotaStateNotEnabled {
			t.Fatalf("states: gem=%s tp=%s", q.GeminiPool.State, q.ThirdPartyPool.State)
		}
		assertUnknownGeminiNumbers(t, q)
	})

	t.Run("one family missing", func(t *testing.T) {
		q, _ := buildQuotaView(QuotaSnapshot{Status: "ok", UpdatedAt: fresh, Groups: quotaTestGroups(false, true, 1, r5, rw)}, nil, now)
		if q.GeminiPool.State != QuotaStateNotEnabled {
			t.Fatalf("Gemini group absent must be not_enabled (never borrow the Claude group), got %s", q.GeminiPool.State)
		}
		assertUnknownGeminiNumbers(t, q)
		if q.ThirdPartyPool.State != QuotaStateKnown || q.ThirdPartyPool.Window5h.RemainingFraction == nil || *q.ThirdPartyPool.Window5h.RemainingFraction != 0.5 {
			t.Fatalf("Claude pool must stay known: %+v", q.ThirdPartyPool)
		}

		q, _ = buildQuotaView(QuotaSnapshot{Status: "ok", UpdatedAt: fresh, Groups: quotaTestGroups(true, false, 1, r5, rw)}, nil, now)
		if q.GeminiPool.State != QuotaStateKnown || q.ThirdPartyPool.State != QuotaStateNotEnabled || q.ThirdPartyPool.Available {
			t.Fatalf("states: gem=%s tp=%s", q.GeminiPool.State, q.ThirdPartyPool.State)
		}
		if q.ThirdPartyPool.Window5h.RemainingFraction != nil {
			t.Fatal("missing Claude group must report null fractions, not 100%")
		}
	})

	t.Run("normal full pool", func(t *testing.T) {
		q, w := buildQuotaView(QuotaSnapshot{Status: "ok", UpdatedAt: fresh, Groups: quotaTestGroups(true, true, 1, r5, rw)}, nil, now)
		if q.GeminiPool.State != QuotaStateKnown || q.ThirdPartyPool.State != QuotaStateKnown {
			t.Fatalf("states: gem=%s tp=%s", q.GeminiPool.State, q.ThirdPartyPool.State)
		}
		d := q.Deduction
		if d.PoolRem5hUsd == nil || *d.PoolRem5hUsd != DefaultGeminiPricing.Cap5hUsd || d.PoolUsed5hUsd == nil || *d.PoolUsed5hUsd != 0 {
			t.Fatalf("full pool dollars: rem=%v used=%v", d.PoolRem5hUsd, d.PoolUsed5hUsd)
		}
		if d.Rem5hTokensAt99PctCache == nil || *d.Rem5hTokensAt99PctCache <= 0 {
			t.Fatal("known pool must report runnable tokens")
		}
		if w["window5h"].Idle {
			t.Fatal("a window resetting in 3h is running, not idle")
		}
	})
}

func TestDashboardQuotaIdleWindowAndRolling(t *testing.T) {
	now := time.Date(2026, 11, 11, 6, 30, 0, 0, time.UTC)
	readAt := time.Date(2026, 11, 11, 6, 24, 7, 0, time.UTC)
	snap := QuotaSnapshot{Status: "ok", UpdatedAt: readAt.Format(time.RFC3339), Groups: quotaTestGroups(true, false, 1,
		readAt.Add(5*time.Hour).Format(time.RFC3339), readAt.Add(7*24*time.Hour+2*time.Minute).Format(time.RFC3339))}
	// F1 sample: a $1.50 Argon step at 04:32Z belongs to the previous (ended) 5h window.
	steps := []ParsedModelStep{
		{TsSec: time.Date(2026, 11, 11, 4, 32, 0, 0, time.UTC).Unix(), StepIdx: 1, CostUsd: 1.5, QuotaGroup: "gemini", ModelID: "gemini-4-argon"},
		{TsSec: now.Add(-8 * time.Hour).Unix(), StepIdx: 2, CostUsd: 2, QuotaGroup: "gemini", ModelID: "gemini-4-argon"},
	}
	_, w := buildQuotaView(snap, steps, now)
	if !w["window5h"].Idle || !w["windowWeekly"].Idle {
		t.Fatalf("0%% used with reset = read + window must be idle: 5h=%v wk=%v", w["window5h"].Idle, w["windowWeekly"].Idle)
	}
	if w["window5h"].StepsCount != 0 {
		t.Fatalf("idle official window has no steps, got %d", w["window5h"].StepsCount)
	}
	if got := w["rolling5h"]; got.StepsCount != 1 || got.CostUsd != 1.5 {
		t.Fatalf("rolling 5h must sum the last 5 hours: steps=%d cost=%v", got.StepsCount, got.CostUsd)
	}
	if got := w["rolling7d"]; got.StepsCount != 2 {
		t.Fatalf("rolling 7d steps=%d", got.StepsCount)
	}

	used := snap
	used.Groups = quotaTestGroups(true, false, 0.9, readAt.Add(5*time.Hour).Format(time.RFC3339), readAt.Add(7*24*time.Hour).Format(time.RFC3339))
	if _, w := buildQuotaView(used, nil, now); w["window5h"].Idle {
		t.Fatal("a window with usage is never idle")
	}
	mid := snap
	mid.Groups = quotaTestGroups(true, false, 1, readAt.Add(2*time.Hour).Format(time.RFC3339), readAt.Add(3*24*time.Hour).Format(time.RFC3339))
	if _, w := buildQuotaView(mid, nil, now); w["window5h"].Idle {
		t.Fatal("0% used but resetting in 2h is a running window")
	}
}

// Drives the real /usage reader: first read succeeds, a later refresh fails (stale), and a failure
// with no cached reading is an error.
func TestDashboardQuotaFetchMarksStaleAndError(t *testing.T) {
	quotaMu.Lock()
	saved, savedAt := cachedQuota, cachedQuotaAt
	cachedQuota = nil
	quotaMu.Unlock()
	t.Cleanup(func() {
		quotaMu.Lock()
		cachedQuota, cachedQuotaAt = saved, savedAt
		quotaMu.Unlock()
	})

	dir := t.TempDir()
	okBin := filepath.Join(dir, "agy-ok")
	failBin := filepath.Join(dir, "agy-fail")
	reset := time.Now().Add(3 * time.Hour).UTC().Format(time.RFC3339)
	payload := `{"command":{"data":{"groups":[{"name":"Gemini models","buckets":[{"window":"5h","remaining_fraction":0.75,"reset_time":"` + reset + `"}]}]}}}`
	if err := os.WriteFile(okBin, []byte("#!/bin/sh\necho '"+payload+"'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(failBin, []byte("#!/bin/sh\nexit 3\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	t.Setenv("AGY_QUOTA_BIN", failBin)
	if snap := fetchOfficialQuotaDo(); snap.Status != "error" {
		t.Fatalf("failure without cache must be error, got %q", snap.Status)
	}

	t.Setenv("AGY_QUOTA_BIN", okBin)
	first := fetchOfficialQuotaDo()
	if first.Status != "ok" || len(first.Groups) != 1 {
		t.Fatalf("first read: %+v", first)
	}

	t.Setenv("AGY_QUOTA_BIN", failBin)
	second := fetchOfficialQuotaDo()
	if second.Status != "stale" || second.Error == "" || second.LastErrorAt == "" || second.UpdatedAt != first.UpdatedAt {
		t.Fatalf("refresh failure after a good read must be stale with the old time: %+v", second)
	}
	q, _ := buildQuotaView(second, nil, time.Now())
	if q.GeminiPool.State != QuotaStateStale || q.Deduction.PoolRem5hUsd == nil {
		t.Fatalf("stale view: %+v %+v", q.GeminiPool, q.Deduction.PoolRem5hUsd)
	}
}

func TestCanaryRoutesSurfacedWithoutCarbonRollover(t *testing.T) {
	// Carbon canary servedModel must NOT trigger Argon-style auto-rollover degradation,
	// while surfacing ServedVariants and canaryRoutes for the UI.
	gmCarbonCanary := ParseGenMetadataBlob(buildTestGenMetadataBlobServed("gemini-4-carbon-exp", 1427, 4000, 500, 200000, 100, 400, 204000, 983040, 3.0, 4.0))
	if rep := EvaluateArgonDegradation(gmCarbonCanary, "gemini-4-carbon"); rep == nil || rep.Degraded {
		t.Fatalf("Carbon canary servedModel must stay non-degraded (UI hint only): %+v", rep)
	}
	if !isServedModelMismatch("gemini-4-carbon", "high", "gemini-4-carbon-exp") {
		t.Fatal("expected Carbon canary servedModel to flag servedMismatch for UI display")
	}

	agg := AggregateSteps([]ParsedModelStep{
		{SessionID: "s1", StepIdx: 1, ModelID: "gemini-4-carbon", ModelLabel: "Gemini 4 Carbon", ModelTier: "high", ServedModel: "gemini-4-carbon-exp", RawIn: 4000, RawCr: 200000, OutTok: 500, ElapsedMs: 7000},
		{SessionID: "s1", StepIdx: 2, ModelID: "gemini-4-carbon", ModelLabel: "Gemini 4 Carbon", ModelTier: "high", ServedModel: "gemini-4-carbon", RawIn: 2000, RawCr: 204000, OutTok: 300, ElapsedMs: 4000},
	})
	if len(agg.Models) != 1 || agg.Models[0].ServedMismatch != 1 || len(agg.Models[0].ServedVariants) != 1 || agg.Models[0].ServedVariants[0] != "gemini-4-carbon-exp" {
		t.Fatalf("unexpected model aggregate variants: %+v", agg.Models)
	}
	routes := summarizeCanaryRoutes([]map[string]any{
		{"model": "gemini-4-carbon", "modelLabel": "Gemini 4 Carbon", "servedModel": "gemini-4-carbon-exp", "servedMismatch": true},
	}, agg)
	if len(routes) != 1 || routes[0]["servedModel"] != "gemini-4-carbon-exp" || routes[0]["live"] != true || routes[0]["steps"] != 1 {
		t.Fatalf("unexpected canaryRoutes: %+v", routes)
	}
}

func TestDeferredToolCatalogPersistsAcrossGatewayRestart(t *testing.T) {
	tmp := t.TempDir()
	dbFile := tmp + "/state.db"
	st := &StateStore{}
	st.Init(dbFile)
	t.Cleanup(func() {
		st.mu.Lock()
		if st.db != nil {
			_ = st.db.Close()
		}
		st.mu.Unlock()
	})

	initTools := sanitizeToolsCatalog([]any{
		map[string]any{"name": "Read", "description": "Read file", "input_schema": map[string]any{"type": "object"}},
	})
	midTools := sanitizeToolsCatalog([]any{
		map[string]any{"name": "Read", "description": "Read file", "input_schema": map[string]any{"type": "object"}},
		map[string]any{"name": "Write", "description": "Write file", "input_schema": map[string]any{"type": "object"}},
	})
	postRestartTools := sanitizeToolsCatalog([]any{
		map[string]any{"name": "Read", "description": "Read file", "input_schema": map[string]any{"type": "object"}},
		map[string]any{"name": "Write", "description": "Write file", "input_schema": map[string]any{"type": "object"}},
		map[string]any{"name": "Bash", "description": "Run shell", "input_schema": map[string]any{"type": "object"}},
	})

	orig := NewAcpProjectSession("sid:restart-cache-keep", tmp, "")
	orig.SessionID = "00000000-0000-4000-8000-000000009999"
	orig.ModelID = "gemini-4-carbon"
	orig.ThoughtLevel = "high"
	orig.MessageCount = 12
	orig.InitialToolsCatalog = append([]ToolDef(nil), initTools...)
	orig.DeclaredMcpTools = map[string]bool{"Read": true}
	for _, td := range initTools {
		orig.McpToHostMap[td.McpName] = td.Name
	}
	orig.ToolsCatalog = append([]ToolDef(nil), initTools...)
	orig.ToolFingerprint = toolCatalogFingerprint(initTools)
	if notice, _, _, _ := orig.consumeToolCatalogLocked(midTools); notice == "" {
		t.Fatal("expected mid-session deferred tool delta notice")
	}
	st.UpsertSession(orig)

	gs := &GatewayServer{
		sessions: make(map[string]*AcpProjectSession),
		tenants:  make(map[string]*AcpProjectSession),
	}
	if n := st.RestoreDetachedSessions(gs); n != 1 {
		t.Fatalf("expected 1 restored session, got %d", n)
	}
	restored := gs.sessions["sid:restart-cache-keep"]
	if restored == nil {
		t.Fatal("restored session missing")
	}
	// InitialToolsCatalog must stay locked to the 1 initial tool (Read + internal helpers) while ToolsCatalog has Write.
	if len(restored.InitialToolsCatalog) != len(initTools) || len(restored.ToolsCatalog) != len(midTools) {
		t.Fatalf("catalogs not restored accurately: init=%d (want %d), cur=%d (want %d)",
			len(restored.InitialToolsCatalog), len(initTools), len(restored.ToolsCatalog), len(midTools))
	}
	if toolCatalogNeedsRollover(len(restored.DeclaredMcpTools) > 0, restored.ToolsCatalog, restored.ToolFingerprint, postRestartTools) {
		t.Fatal("restored session with MCP bridge must NOT roll over when another deferred tool is added after restart")
	}
	notice, added, _, _ := restored.consumeToolCatalogLocked(postRestartTools)
	if len(added) != 1 || added[0] != "Bash" || notice == "" {
		t.Fatalf("expected incremental delta for Bash only, got added=%v notice=%q", added, notice)
	}
}

func TestPrometheusMetricsMonotonicBeyond150TurnRingBuffer(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("AGY_STATE_DB", tmp+"/prom.db")
	InitGlobalStore(tmp + "/prom.db")
	t.Cleanup(func() { InitGlobalStore(GatewayStateDB) })

	gs := &GatewayServer{
		sessions: make(map[string]*AcpProjectSession),
		tenants:  make(map[string]*AcpProjectSession),
	}
	s := NewAcpProjectSession("sid:long-prom", tmp, "")
	s.SessionID = "00000000-0000-4000-8000-000000008888"
	s.ModelID = "gemini-4-carbon"
	s.CompactionRollovers = 2
	s.SessionReuses = 164
	s.MessageCount = 330
	GlobalStore.UpsertSession(s)

	for i := 1; i <= 165; i++ {
		entry := TurnHistoryEntry{
			TurnIndex:     i,
			CompletedAtMs: time.Now().UnixMilli() + int64(i),
			SessionID:     s.SessionID,
			Mode:          "sticky_delta",
			Model:         "gemini-4-carbon",
			ThoughtLevel:  "high",
			StopReason:    "end_turn",
			Usage: &TurnUsage{
				InputTokens:      1000,
				CachedReadTokens: 50000,
				OutputTokens:     200,
				UsageSource:      "db",
			},
		}
		GlobalStore.InsertTurn(s.ConvKey, entry)
		s.TurnHistory = append(s.TurnHistory, entry)
		if len(s.TurnHistory) > 150 {
			s.TurnHistory = s.TurnHistory[len(s.TurnHistory)-150:]
		}
	}
	gs.sessions[s.ConvKey] = s

	prom := RenderPrometheusMetrics(gs)
	wantTurnLine := `agy_gateway_turns_total{model="gemini-4-carbon",mode="sticky_delta",stop_reason="end_turn",usage_source="db"} 165`
	wantInLine := `agy_gateway_tokens_total{model="gemini-4-carbon",kind="input"} 165000`
	if !strings.Contains(prom, wantTurnLine) || !strings.Contains(prom, wantInLine) {
		t.Fatalf("expected monotonic 165-turn totals in Prometheus output, got:\n%s", prom)
	}
}

func TestDepletedClaudePoolKeepsCalibratedCapsAndRolling5hSteps(t *testing.T) {
	now := time.Date(2026, 11, 12, 3, 55, 0, 0, time.UTC)
	fresh := now.Add(-5 * time.Minute).Format(time.RFC3339)
	wkReset := now.Add(2 * time.Hour).Format(time.RFC3339)

	// Reproduces live /usage when 3p-weekly hits 0% remaining: upstream omits 3p-5h resetTime ("").
	snap := QuotaSnapshot{
		Status:    "ok",
		UpdatedAt: fresh,
		Groups: []QuotaGroup{
			{
				Name: "Gemini Models",
				Buckets: []QuotaBucket{
					{Window: "5h", RemainingFraction: 0.80, UsedFraction: 0.20, ResetTime: now.Add(75 * time.Minute).Format(time.RFC3339)},
					{Window: "weekly", RemainingFraction: 0.15, UsedFraction: 0.85, ResetTime: wkReset},
				},
			},
			{
				Name: "Claude and GPT models",
				Buckets: []QuotaBucket{
					{Window: "5h", RemainingFraction: 0.12, UsedFraction: 0.88, ResetTime: ""},
					{Window: "weekly", RemainingFraction: 0, UsedFraction: 1.0, ResetTime: wkReset},
				},
			},
		},
	}
	steps := []ParsedModelStep{
		{
			TsSec:      now.Add(-90 * time.Minute).Unix(),
			StepIdx:    1,
			CostUsd:    22.0,
			QuotaGroup: "3p",
			ModelID:    "claude-opus-5-5",
		},
	}

	q, _ := buildQuotaView(snap, steps, now)
	tp := q.ThirdPartyPool
	if tp == nil || tp.State != QuotaStateKnown {
		t.Fatalf("expected known ThirdPartyPool, got %+v", tp)
	}
	if tp.Window5h.Steps != 1 || tp.Window5h.LocalCostUsd != 22.0 {
		t.Fatalf("empty 5h resetTime with active usage must keep rolling 5h steps, got steps=%d cost=%v", tp.Window5h.Steps, tp.Window5h.LocalCostUsd)
	}
	if tp.Window5h.ImpliedCapUsd == nil || *tp.Window5h.ImpliedCapUsd != 25.0 {
		t.Fatalf("expected implied 5h cap $25.00, got %v", tp.Window5h.ImpliedCapUsd)
	}
	if tp.Cap5hUsd != 25.0 || tp.CapWeeklyUsd != 50.0 {
		t.Fatalf("expected calibrated caps $25/$50, got %v/%v", tp.Cap5hUsd, tp.CapWeeklyUsd)
	}
	if tp.PoolUsed5hUsd == nil || *tp.PoolUsed5hUsd != 22.0 || tp.PoolUsedWeeklyUsd == nil || *tp.PoolUsedWeeklyUsd != 50.0 {
		t.Fatalf("unexpected pool used dollars: 5h=%v wk=%v", tp.PoolUsed5hUsd, tp.PoolUsedWeeklyUsd)
	}
	if tp.EffectiveRemUsd == nil || *tp.EffectiveRemUsd != 0 || tp.Bottleneck != "weekly" {
		t.Fatalf("weekly-exhausted pool must cap effective remaining at $0 (bottleneck=weekly), got rem=%v b=%q", tp.EffectiveRemUsd, tp.Bottleneck)
	}
	if tp.RemEffectiveTokensAt99PctCache == nil || *tp.RemEffectiveTokensAt99PctCache != 0 {
		t.Fatalf("weekly-exhausted pool must report 0 runnable tokens, got %v", tp.RemEffectiveTokensAt99PctCache)
	}

	// Gemini weekly ($120 remaining) < Gemini 5h ($160 remaining) -> effective cap is weekly ($120).
	if q.Deduction.EffectiveRemUsd == nil || *q.Deduction.EffectiveRemUsd != 120.0 || q.Deduction.Bottleneck != "weekly" {
		t.Fatalf("expected Gemini effective cap $120 (weekly bottleneck), got rem=%v b=%q", q.Deduction.EffectiveRemUsd, q.Deduction.Bottleneck)
	}

	// Even on another host with zero local steps, calibrated pool used/cap numbers stay intact (never $0 / --).
	qRemote, _ := buildQuotaView(snap, nil, now)
	tpRemote := qRemote.ThirdPartyPool
	if tpRemote.PoolUsed5hUsd == nil || *tpRemote.PoolUsed5hUsd != 22.0 || tpRemote.PoolUsedWeeklyUsd == nil || *tpRemote.PoolUsedWeeklyUsd != 50.0 {
		t.Fatalf("remote host without local DB steps must still show official pool dollars: 5h=%v wk=%v", tpRemote.PoolUsed5hUsd, tpRemote.PoolUsedWeeklyUsd)
	}
}

func TestLegacyUsageStepsRepricedToCalibratedWeights(t *testing.T) {
	tmp := t.TempDir()
	st := &StateStore{}
	st.Init(filepath.Join(tmp, "legacy.db"))
	t.Cleanup(func() {
		st.mu.Lock()
		if st.db != nil {
			_ = st.db.Close()
		}
		st.mu.Unlock()
	})

	// Insert a pre-2026-11-11 Opus step carrying legacy 2x long-ctx multiplier and 2054-token TPU tail.
	legacy := ParsedModelStep{
		SessionID:      "00000000-0000-4000-8000-000000007777",
		StepIdx:        2,
		SourceCategory: "CLAUDE_GW",
		TsSec:          1794128703,
		TtftMs:         1800,
		ElapsedMs:      5000,
		Tps:            40.0,
		ModelEnum:      1402, // claude-opus-5-5 high
		ModelID:        "claude-opus-5-5",
		ModelLabel:     "Claude Opus 5.5",
		ModelFamily:    "claude",
		ModelTier:      "high",
		QuotaGroup:     "3p",
		RawIn:          10000,
		RawCr:          250000,
		AlignedIn:      12054, // legacy TPU tail artifact
		AlignedCr:      247946,
		OutTok:         1000,
		VisOutTok:      800,
		ThTok:          200,
		HasThinking:    true,
		TierMultiplier: 2.0,  // legacy bogus long-ctx multiplier
		CostUsd:        0.58, // inflated legacy cost
	}
	if _, err := st.persistUsageBatch(legacy.SessionID, cursorMeta{lastStepIdx: 2}, []ParsedModelStep{legacy}); err != nil {
		t.Fatalf("persistUsageBatch: %v", err)
	}

	got, ok := st.QueryAllUsageSteps(nil)
	if !ok || len(got) != 1 {
		t.Fatalf("expected 1 step, got ok=%v len=%d", ok, len(got))
	}
	s := got[0]
	if s.AlignedIn != 10000 || s.AlignedCr != 250000 || s.TierMultiplier != 1.0 {
		t.Fatalf("legacy artifacts not normalized: alignedIn=%d alignedCr=%d mult=%v", s.AlignedIn, s.AlignedCr, s.TierMultiplier)
	}
	// Calibrated warm Opus 5.5: 10K * $5/M ($0.05) + 250K * $0.20/M ($0.05) + 1K * $20/M ($0.02) = $0.12
	if s.CostUsd < 0.1199 || s.CostUsd > 0.1201 {
		t.Fatalf("expected calibrated cost $0.12, got %v", s.CostUsd)
	}
}

func TestRound4TelemetrySteadyStateCanaryVariantsAndMissIdleMath(t *testing.T) {
	prevDB := GatewayStateDB
	t.Cleanup(func() { InitGlobalStore(prevDB) })
	tmp := t.TempDir()
	dbFile := filepath.Join(tmp, "r4.db")
	InitGlobalStore(dbFile)

	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	nowSec := now.Unix()

	// BUG-TEL-02: Step 1 starts at t=1000, runs 180s (ends 1180). Step 2 starts at t=1310 (130s idle < 300s,
	// despite start-to-start 310s >= 300s) -> must be "zero", NOT "gap_ttl". Step 3 starts at t=1630 after
	// Step 2's 10s run (ends 1320, idle 310s >= 300s) -> must be "gap_ttl".
	missSteps := []ParsedModelStep{
		{SessionID: "sid-long-1", StepIdx: 1, TsSec: 1000, ElapsedMs: 180000, ModelID: "gemini-4-carbon", RawIn: 50000, RawCr: 0, OutTok: 400},
		{SessionID: "sid-long-1", StepIdx: 2, TsSec: 1310, ElapsedMs: 10000, ModelID: "gemini-4-carbon", RawIn: 55000, RawCr: 0, OutTok: 300},
		{SessionID: "sid-long-1", StepIdx: 3, TsSec: 1630, ElapsedMs: 5000, ModelID: "gemini-4-carbon", RawIn: 58000, RawCr: 0, OutTok: 200},
	}
	misses := computeDbStepCacheMisses(missSteps, nil)
	if len(misses) != 3 || misses[0] != "cold_start" || misses[1] != "zero" || misses[2] != "gap_ttl" {
		t.Fatalf("unexpected step cache misses (BUG-TEL-02): %v", misses)
	}

	// BUG-TEL-03 & BUG-TEL-04:
	// - Session A starts 6h ago (outside rolling5h) on Carbon, continues 1h ago on Carbon (warm hit inside rolling5h),
	//   switches in-session to Opus 40m ago (cold model_switch prefix), and continues on Opus 20m ago (warm hit).
	// - Session B hits two Argon canary variants in rolling5h: 4 steps on -b and 1 step on -x.
	// - Session ColdIdx2 is a cold-start session whose first generation step has StepIdx=2 (after idx=0 init & idx=1 user input)
	//   and hits a small shared boilerplate prefix (RawCr=2048, RawIn=86000) -> must NOT be marked steady or charge MissResendUsd.
	rawSteps := []ParsedModelStep{
		{SessionID: "sess-a", StepIdx: 1, TsSec: nowSec - 6*3600, ModelEnum: 1427, ModelID: "gemini-4-carbon", RawIn: 50000, RawCr: 0, OutTok: 500, IsDshBridge: true},
		{SessionID: "sess-a", StepIdx: 2, TsSec: nowSec - 3600, ModelEnum: 1427, ModelID: "gemini-4-carbon", RawIn: 2000, RawCr: 50000, OutTok: 400, IsDshBridge: true},
		{SessionID: "sess-a", StepIdx: 3, TsSec: nowSec - 2400, ModelEnum: 1401, ModelID: "claude-opus-5-5", RawIn: 54000, RawCr: 0, OutTok: 600, IsDshBridge: true},
		{SessionID: "sess-a", StepIdx: 4, TsSec: nowSec - 1200, ModelEnum: 1401, ModelID: "claude-opus-5-5", RawIn: 1500, RawCr: 54000, OutTok: 500, IsDshBridge: true},
		{SessionID: "sess-b", StepIdx: 1, TsSec: nowSec - 1800, ModelEnum: 1425, ModelID: "gemini-4-argon", ServedModel: "gemini-4-argon-b", RawIn: 20000, RawCr: 0, OutTok: 200, IsDshBridge: true},
		{SessionID: "sess-b", StepIdx: 2, TsSec: nowSec - 1700, ModelEnum: 1425, ModelID: "gemini-4-argon", ServedModel: "gemini-4-argon-b", RawIn: 1000, RawCr: 20000, OutTok: 200, IsDshBridge: true},
		{SessionID: "sess-b", StepIdx: 3, TsSec: nowSec - 1600, ModelEnum: 1425, ModelID: "gemini-4-argon", ServedModel: "gemini-4-argon-b", RawIn: 1000, RawCr: 21000, OutTok: 200, IsDshBridge: true},
		{SessionID: "sess-b", StepIdx: 4, TsSec: nowSec - 1500, ModelEnum: 1425, ModelID: "gemini-4-argon", ServedModel: "gemini-4-argon-b", RawIn: 1000, RawCr: 22000, OutTok: 200, IsDshBridge: true},
		{SessionID: "sess-b", StepIdx: 5, TsSec: nowSec - 1400, ModelEnum: 1425, ModelID: "gemini-4-argon", ServedModel: "gemini-4-argon-x", RawIn: 1000, RawCr: 23000, OutTok: 200, IsDshBridge: true},
		{SessionID: "sess-cold-idx2", StepIdx: 2, TsSec: nowSec - 900, ModelEnum: 1319, ModelID: "gemini-3.8-flash", RawIn: 86000, RawCr: 2048, OutTok: 300, IsDshBridge: true},
	}
	for i := range rawSteps {
		normalizeScannedUsageStep(&rawSteps[i])
	}
	if _, err := GlobalStore.persistUsageBatch("sess-a", cursorMeta{lastStepIdx: 4}, rawSteps[:4]); err != nil {
		t.Fatal(err)
	}
	if _, err := GlobalStore.persistUsageBatch("sess-b", cursorMeta{lastStepIdx: 5}, rawSteps[4:9]); err != nil {
		t.Fatal(err)
	}
	if _, err := GlobalStore.persistUsageBatch("sess-cold-idx2", cursorMeta{lastStepIdx: 2}, rawSteps[9:]); err != nil {
		t.Fatal(err)
	}

	snap := QuotaSnapshot{
		Status:    "ok",
		UpdatedAt: now.Add(-1 * time.Minute).Format(time.RFC3339),
		Groups:    quotaTestGroups(true, true, 0.85, now.Add(2*time.Hour).Format(time.RFC3339), now.Add(4*24*time.Hour).Format(time.RFC3339)),
	}
	q, wins := buildQuotaView(snap, rawSteps, now)

	// Verify BUG-TEL-05 Gemini local step counts & gemKnown guard
	if q.Deduction.LocalSteps5h != 7 || q.Deduction.LocalStepsWeekly != 8 {
		t.Fatalf("expected Gemini local steps 5h=7 weekly=8, got 5h=%d weekly=%d", q.Deduction.LocalSteps5h, q.Deduction.LocalStepsWeekly)
	}
	qNoGem, _ := buildQuotaView(QuotaSnapshot{Status: "ok", UpdatedAt: snap.UpdatedAt, Groups: quotaTestGroups(false, true, 1, now.Add(2*time.Hour).Format(time.RFC3339), now.Add(4*24*time.Hour).Format(time.RFC3339))}, rawSteps, now)
	if qNoGem.Deduction.LocalSteps5h != 0 || qNoGem.Deduction.LocalTotal5hUsd != 0 {
		t.Fatalf("unknown/disabled Gemini pool must not accumulate local steps/dollars: %+v", qNoGem.Deduction)
	}

	// Verify BUG-TEL-03 in rolling5h
	r5 := wins["rolling5h"]
	byMod := map[string]ModelAggregate{}
	for _, m := range r5.Models {
		byMod[m.ModelID] = m
	}
	carb := byMod["gemini-4-carbon"]
	if carb.Steps != 1 || carb.SteadySteps != 1 || carb.SteadyHits != 1 || carb.SteadyHitRate != 100.0 {
		t.Fatalf("expected warm Carbon continuation inside rolling5h to have SteadySteps=1 SteadyHitRate=100%%, got %+v", carb)
	}
	opus := byMod["claude-opus-5-5"]
	if opus.Steps != 2 || opus.SteadySteps != 1 || opus.SteadyHits != 1 || opus.SteadyHitRate != 100.0 || opus.MissResendUsd != 0 {
		t.Fatalf("expected in-session model_switch cold prefix excluded from Opus SteadySteps/MissResendUsd, got %+v", opus)
	}
	flashCold := byMod["gemini-3.8-flash"]
	if flashCold.Steps != 1 || flashCold.SteadySteps != 0 || flashCold.MissResendUsd != 0 {
		t.Fatalf("expected cold-start StepIdx=2 with boilerplate RawCr=2048 to have SteadySteps=0 MissResendUsd=0, got %+v", flashCold)
	}

	// Verify allTime matches SQL rollup (Round 2 ARCH-OPT-03 parity)
	roll, ok := GlobalStore.QueryUsageAllTimeRollup(map[string]bool{"sess-a": true, "sess-b": true, "sess-cold-idx2": true})
	allAgg := wins["allTime"]
	if !ok || allAgg.StepsCount != roll.StepsCount || allAgg.CostUsd != roll.CostUsd || allAgg.SavedUsd != roll.SavedUsd {
		t.Fatalf("allTime window vs SQL rollup mismatch: agg=(steps=%d cost=%v saved=%v) roll=%+v",
			allAgg.StepsCount, allAgg.CostUsd, allAgg.SavedUsd, roll)
	}

	// Verify BUG-TEL-04 per-variant canary step counts
	routes := summarizeCanaryRoutes(nil, r5)
	routeSteps := map[string]int{}
	for _, rt := range routes {
		sv, _ := rt["servedModel"].(string)
		cnt, _ := rt["steps"].(int)
		routeSteps[sv] = cnt
	}
	if routeSteps["gemini-4-argon-b"] != 4 || routeSteps["gemini-4-argon-x"] != 1 {
		t.Fatalf("expected per-variant canary counts b=4 x=1, got %v", routeSteps)
	}
}

func TestFetchOfficialQuotaRefreshesAfterResetTimePasses(t *testing.T) {
	quotaMu.Lock()
	saved, savedAt, savedAttempt := cachedQuota, cachedQuotaAt, lastQuotaAttemptAt
	quotaMu.Unlock()
	savedLastTurn := lastGatewayTurn.Load()
	t.Cleanup(func() {
		quotaMu.Lock()
		cachedQuota, cachedQuotaAt, lastQuotaAttemptAt = saved, savedAt, savedAttempt
		quotaMu.Unlock()
		lastGatewayTurn.Store(savedLastTurn)
	})

	dir := t.TempDir()
	okBin := filepath.Join(dir, "agy-post-reset")
	newReset := time.Now().Add(5 * time.Hour).UTC().Format(time.RFC3339)
	payload := `{"command":{"data":{"groups":[{"name":"Gemini models","buckets":[{"window":"5h","remaining_fraction":1.0,"reset_time":"` + newReset + `"}]}]}}}`
	if err := os.WriteFile(okBin, []byte("#!/bin/sh\necho '"+payload+"'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGY_QUOTA_BIN", okBin)

	// Seed an exhausted bucket whose resetTime passed 10s ago; cache is 45s old (<3m, <15m) and no new turns arrived.
	pastReset := time.Now().Add(-10 * time.Second).UTC().Format(time.RFC3339)
	quotaMu.Lock()
	cachedQuota = &QuotaSnapshot{
		Status:    "ok",
		UpdatedAt: time.Now().Add(-45 * time.Second).UTC().Format(time.RFC3339),
		Groups: []QuotaGroup{{
			Name:    "Gemini models",
			Buckets: []QuotaBucket{{Window: "5h", RemainingFraction: 0, UsedFraction: 1, ResetTime: pastReset}},
		}},
	}
	cachedQuotaAt = time.Now().Add(-45 * time.Second)
	lastQuotaAttemptAt = cachedQuotaAt
	quotaMu.Unlock()
	lastGatewayTurn.Store(0)

	_ = FetchOfficialQuota(false)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		quotaMu.Lock()
		cur := cachedQuota
		quotaMu.Unlock()
		if cur != nil && len(cur.Groups) == 1 && len(cur.Groups[0].Buckets) == 1 && cur.Groups[0].Buckets[0].RemainingFraction == 1.0 {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("expected FetchOfficialQuota(false) to background-refresh within 3s once resetTime passed")
}

func waitQuotaRefreshIdle(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		quotaFlightMu.Lock()
		waitCh := quotaFlightWait
		quotaFlightMu.Unlock()
		if !quotaRefreshing.Load() && waitCh == nil {
			return
		}
		if waitCh != nil {
			select {
			case <-waitCh:
			case <-time.After(15 * time.Millisecond):
			}
		} else {
			time.Sleep(15 * time.Millisecond)
		}
	}
	t.Fatal("timed out waiting for background official quota refresh to finish")
}

func TestQuotaCmdEnvUsesPureApiHome(t *testing.T) {
	origPure := PureApiHome
	t.Cleanup(func() { PureApiHome = origPure })

	setupProfile := func(t *testing.T, pure string) {
		t.Helper()
		cliDir := filepath.Join(pure, "antigravity-cli")
		if err := os.MkdirAll(cliDir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(cliDir, "antigravity-oauth-token"), []byte("tok-valid-placeholder-value"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	extractEnvVal := func(env []string, key string) string {
		pfx := key + "="
		for _, kv := range env {
			if strings.HasPrefix(kv, pfx) {
				return strings.TrimPrefix(kv, pfx)
			}
		}
		return ""
	}

	tmp := t.TempDir()
	pure := filepath.Join(tmp, "pure-api-home")
	setupProfile(t, pure)
	PureApiHome = pure

	env := quotaCmdEnv([]string{"HOME=/Users/other", "PATH=/usr/bin", "AGY_API_KEYS=secret-should-strip"})
	wantHome := filepath.Join(tmp, "quota-home-pure-api-home")
	gotHome := extractEnvVal(env, "HOME")
	if gotHome != wantHome {
		t.Fatalf("expected isolated HOME=%q, got %q", wantHome, gotHome)
	}
	if extractEnvVal(env, "AGY_API_KEYS") != "" {
		t.Fatalf("expected AGY_API_KEYS to be stripped from leaf /usage subprocess env")
	}
	target, err := os.Readlink(filepath.Join(wantHome, ".gemini"))
	if err != nil || target != pure {
		t.Fatalf("expected .gemini symlink -> %q, got %q (err=%v)", pure, target, err)
	}

	t.Run("sibling profiles do not collide", func(t *testing.T) {
		sibTmp := t.TempDir()
		profA := filepath.Join(sibTmp, "account-a")
		profB := filepath.Join(sibTmp, "account-b")
		setupProfile(t, profA)
		setupProfile(t, profB)

		PureApiHome = profA
		homeA := extractEnvVal(quotaCmdEnv([]string{"HOME=/Users/other"}), "HOME")
		PureApiHome = profB
		homeB := extractEnvVal(quotaCmdEnv([]string{"HOME=/Users/other"}), "HOME")

		wantA := filepath.Join(sibTmp, "quota-home-account-a")
		wantB := filepath.Join(sibTmp, "quota-home-account-b")
		if homeA != wantA || homeB != wantB || homeA == homeB {
			t.Fatalf("expected distinct per-profile quota homes (%q, %q), got (%q, %q)", wantA, wantB, homeA, homeB)
		}
		if gotA, err := os.Readlink(filepath.Join(homeA, ".gemini")); err != nil || gotA != profA {
			t.Fatalf("account-a symlink mutated after account-b run: got %q (err=%v), want %q", gotA, err, profA)
		}
		if gotB, err := os.Readlink(filepath.Join(homeB, ".gemini")); err != nil || gotB != profB {
			t.Fatalf("account-b symlink mismatch: got %q (err=%v), want %q", gotB, err, profB)
		}
	})

	t.Run("relative PureApiHome resolves to absolute symlink", func(t *testing.T) {
		relTmp := t.TempDir()
		t.Chdir(relTmp)
		wd, err := os.Getwd()
		if err != nil {
			t.Fatal(err)
		}
		relPure := filepath.Join("runtime", "pure-api-home")
		absPure := filepath.Join(wd, relPure)
		setupProfile(t, absPure)

		PureApiHome = relPure
		gotRelHome := extractEnvVal(quotaCmdEnv([]string{"HOME=/Users/other"}), "HOME")
		wantRelHome := filepath.Join(wd, "runtime", "quota-home-pure-api-home")
		if !filepath.IsAbs(gotRelHome) || gotRelHome != wantRelHome {
			t.Fatalf("expected absolute quota home %q, got %q", wantRelHome, gotRelHome)
		}
		linkTarget, err := os.Readlink(filepath.Join(gotRelHome, ".gemini"))
		if err != nil || !filepath.IsAbs(linkTarget) || linkTarget != absPure {
			t.Fatalf("expected absolute .gemini symlink -> %q, got %q (err=%v)", absPure, linkTarget, err)
		}
		if !fileExists(filepath.Join(gotRelHome, ".gemini", "antigravity-cli", "antigravity-oauth-token")) {
			t.Fatalf("expected token reachable via .gemini symlink under %q", gotRelHome)
		}
	})

	t.Run("child worker .gemini PureApiHome uses parent directly without extra symlink", func(t *testing.T) {
		childRoot := filepath.Join(t.TempDir(), "acct-2")
		dotGem := filepath.Join(childRoot, ".gemini")
		setupProfile(t, dotGem)
		PureApiHome = dotGem
		gotChildHome := extractEnvVal(quotaCmdEnv([]string{"HOME=/Users/other"}), "HOME")
		if gotChildHome != childRoot {
			t.Fatalf("expected child .gemini parent %q directly, got %q", childRoot, gotChildHome)
		}
	})
}

func TestFetchOfficialQuotaColdNonBlocking(t *testing.T) {
	waitQuotaRefreshIdle(t)
	quotaMu.Lock()
	savedCache, savedAt, savedErr, savedAttempt, savedSteps := cachedQuota, cachedQuotaAt, lastQuotaErr, lastQuotaAttemptAt, quotaBackoffSteps
	cachedQuota, cachedQuotaAt, lastQuotaErr, lastQuotaAttemptAt, quotaBackoffSteps = nil, time.Time{}, nil, time.Time{}, 0
	quotaMu.Unlock()
	t.Cleanup(func() {
		quotaMu.Lock()
		cachedQuota, cachedQuotaAt, lastQuotaErr, lastQuotaAttemptAt, quotaBackoffSteps = savedCache, savedAt, savedErr, savedAttempt, savedSteps
		quotaMu.Unlock()
	})

	dir := t.TempDir()
	slowBin := filepath.Join(dir, "agy-slow")
	if err := os.WriteFile(slowBin, []byte("#!/bin/sh\nsleep 1\nexit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGY_QUOTA_BIN", slowBin)
	t.Cleanup(func() { waitQuotaRefreshIdle(t) })

	start := time.Now()
	snap := FetchOfficialQuota(false)
	elapsed := time.Since(start)
	if elapsed >= 400*time.Millisecond {
		t.Fatalf("FetchOfficialQuota(false) blocked for %v on cold cache; want immediate return", elapsed)
	}
	if snap.Status != QuotaStateLoading {
		t.Fatalf("expected status=%q on cold non-blocking fetch, got %+v", QuotaStateLoading, snap)
	}
	qView, _ := buildQuotaView(snap, nil, time.Now())
	if qView.GeminiPool.State != QuotaStateLoading || qView.ThirdPartyPool.State != QuotaStateLoading {
		t.Fatalf("expected loading pool states, got gem=%s tp=%s", qView.GeminiPool.State, qView.ThirdPartyPool.State)
	}
}

func TestFetchOfficialQuotaColdInvalidJSONPersistsError(t *testing.T) {
	waitQuotaRefreshIdle(t)
	quotaMu.Lock()
	savedCache, savedAt, savedErr, savedAttempt, savedSteps := cachedQuota, cachedQuotaAt, lastQuotaErr, lastQuotaAttemptAt, quotaBackoffSteps
	cachedQuota, cachedQuotaAt, lastQuotaErr, lastQuotaAttemptAt, quotaBackoffSteps = nil, time.Time{}, nil, time.Time{}, 0
	quotaMu.Unlock()
	t.Cleanup(func() {
		quotaMu.Lock()
		cachedQuota, cachedQuotaAt, lastQuotaErr, lastQuotaAttemptAt, quotaBackoffSteps = savedCache, savedAt, savedErr, savedAttempt, savedSteps
		quotaMu.Unlock()
	})

	dir := t.TempDir()
	countFile := filepath.Join(dir, "calls.txt")
	badBin := filepath.Join(dir, "agy-bad-json")
	script := "#!/bin/sh\necho x >> " + countFile + "\necho 'not-valid-json'\nexit 0\n"
	if err := os.WriteFile(badBin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGY_QUOTA_BIN", badBin)
	t.Cleanup(func() { waitQuotaRefreshIdle(t) })

	snap := FetchOfficialQuota(false)
	if snap.Status != QuotaStateLoading {
		t.Fatalf("expected initial status=%q, got %+v", QuotaStateLoading, snap)
	}

	waitQuotaRefreshIdle(t)

	for i := 0; i < 2; i++ {
		snap = FetchOfficialQuota(false)
		if snap.Status != QuotaStateError {
			t.Fatalf("poll %d: expected persisted error status=%q, got %+v", i+1, QuotaStateError, snap)
		}
		if !strings.Contains(snap.Error, "Unparseable") {
			t.Fatalf("poll %d: expected unparseable error detail, got %q", i+1, snap.Error)
		}
	}

	rawCalls, err := os.ReadFile(countFile)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(string(rawCalls), "x"); got != 1 {
		t.Fatalf("expected backoff cooldown to throttle retries to 1 invocation, got %d", got)
	}
}

func TestFetchOfficialQuotaOrphanGrandchildPipeCleanupOnTimeout(t *testing.T) {
	waitQuotaRefreshIdle(t)
	prevTimeout := officialQuotaTimeout
	officialQuotaTimeout = 300 * time.Millisecond
	quotaMu.Lock()
	savedCache, savedAt, savedErr, savedAttempt, savedSteps := cachedQuota, cachedQuotaAt, lastQuotaErr, lastQuotaAttemptAt, quotaBackoffSteps
	cachedQuota, cachedQuotaAt, lastQuotaErr, lastQuotaAttemptAt, quotaBackoffSteps = nil, time.Time{}, nil, time.Time{}, 0
	quotaMu.Unlock()
	t.Cleanup(func() {
		officialQuotaTimeout = prevTimeout
		waitQuotaRefreshIdle(t)
		quotaMu.Lock()
		cachedQuota, cachedQuotaAt, lastQuotaErr, lastQuotaAttemptAt, quotaBackoffSteps = savedCache, savedAt, savedErr, savedAttempt, savedSteps
		quotaMu.Unlock()
	})

	dir := t.TempDir()
	orphanBin := filepath.Join(dir, "agy-orphan-pipe")
	// Spawns a background grandchild holding stdout open for 30s!
	script := "#!/bin/sh\n(sleep 30) &\nsleep 30\n"
	if err := os.WriteFile(orphanBin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGY_QUOTA_BIN", orphanBin)

	start := time.Now()
	snap := fetchOfficialQuotaDo()
	elapsed := time.Since(start)
	if elapsed > 1800*time.Millisecond {
		t.Fatalf("fetchOfficialQuotaDo hung for %v waiting on orphaned grandchild stdout pipe; want <1.8s", elapsed)
	}
	if snap.Status != "error" || !strings.Contains(strings.ToLower(snap.Error), "timeout") {
		t.Fatalf("expected timeout error snapshot, got %+v", snap)
	}
}

func TestBuildQuotaViewColdLoadingSuppressesUltra20xAndProDetection(t *testing.T) {
	waitQuotaRefreshIdle(t)
	resetCreditsTierCacheForTest()
	origPure := PureApiHome
	quotaMu.Lock()
	savedCache, savedAt, savedErr, savedAttempt, savedSteps := cachedQuota, cachedQuotaAt, lastQuotaErr, lastQuotaAttemptAt, quotaBackoffSteps
	cachedQuota, cachedQuotaAt, lastQuotaErr, lastQuotaAttemptAt, quotaBackoffSteps = nil, time.Time{}, nil, time.Time{}, 0
	quotaMu.Unlock()
	t.Cleanup(func() {
		waitQuotaRefreshIdle(t)
		resetCreditsTierCacheForTest()
		PureApiHome = origPure
		quotaMu.Lock()
		cachedQuota, cachedQuotaAt, lastQuotaErr, lastQuotaAttemptAt, quotaBackoffSteps = savedCache, savedAt, savedErr, savedAttempt, savedSteps
		quotaMu.Unlock()
	})

	PureApiHome = t.TempDir()
	now := time.Date(2026, 11, 24, 8, 0, 0, 0, time.UTC)
	r5 := now.Add(3 * time.Hour).Format(time.RFC3339)
	rw := now.Add(5 * 24 * time.Hour).Format(time.RFC3339)

	// 1. Cold/loading and not_enabled empty snapshots MUST NOT stamp "Ultra 20x"
	for _, st := range []string{QuotaStateLoading, QuotaStateNotEnabled, QuotaStateError} {
		q, _ := buildQuotaView(QuotaSnapshot{Status: st, UpdatedAt: now.Format(time.RFC3339), Groups: []QuotaGroup{}}, nil, now)
		if q.PlanTier != "" || q.PlanLabel != "" {
			t.Fatalf("status=%q with empty groups must not stamp default PlanTier/PlanLabel, got tier=%q label=%q", st, q.PlanTier, q.PlanLabel)
		}
	}

	// 2. 2-group Pro detection via SummaryDescription -> Pro caps ($10/$35 Gemini, $2.50/$5.00 3P)
	proSummarySnap := QuotaSnapshot{
		Status:             "ok",
		UpdatedAt:          now.Format(time.RFC3339),
		SummaryDescription: "Google AI Pro subscription usage",
		Groups:             quotaTestGroups(true, true, 0.5, r5, rw),
	}
	qSum, _ := buildQuotaView(proSummarySnap, nil, now)
	if qSum.PlanTier != "pro" || qSum.PlanLabel != "AI Pro" {
		t.Fatalf("expected Pro detection via SummaryDescription, got tier=%q label=%q", qSum.PlanTier, qSum.PlanLabel)
	}
	if qSum.Deduction.Cap5hUsd != 10.0 || qSum.Deduction.CapWeeklyUsd != 35.0 || qSum.ThirdPartyPool.Cap5hUsd != 2.5 || qSum.ThirdPartyPool.CapWeeklyUsd != 5.0 {
		t.Fatalf("expected Pro caps ($10/$35, $2.50/$5.00), got gem=%+v tp=%+v", qSum.Deduction, qSum.ThirdPartyPool)
	}

	// 3. 2-group Pro detection via authoritative `/credits` plan_name ("Google AI Pro"), cached across calls
	dir := t.TempDir()
	creditsCountFile := filepath.Join(dir, "credits-calls.txt")
	mockBin := filepath.Join(dir, "agy-credits-pro")
	script := "#!/bin/sh\n" +
		"if [ \"$2\" = \"/credits\" ]; then\n" +
		"  echo 1 >> '" + creditsCountFile + "'\n" +
		"  echo '{\"command\":{\"data\":{\"plan_name\":\"Google AI Pro\"}}}'\n" +
		"  exit 0\n" +
		"fi\n" +
		"echo '{\"command\":{\"data\":{\"groups\":[{\"name\":\"Gemini Models\",\"buckets\":[{\"window\":\"5h\",\"remaining_fraction\":0.4,\"reset_time\":\"" + r5 + "\"},{\"window\":\"weekly\",\"remaining_fraction\":0.8,\"reset_time\":\"" + rw + "\"}]},{\"name\":\"Claude and GPT models\",\"buckets\":[{\"window\":\"5h\",\"remaining_fraction\":0.5,\"reset_time\":\"" + r5 + "\"},{\"window\":\"weekly\",\"remaining_fraction\":0.6,\"reset_time\":\"" + rw + "\"}]}]}}}'\n"
	if err := os.WriteFile(mockBin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGY_QUOTA_BIN", mockBin)

	snap1 := fetchOfficialQuotaDo()
	qCred, _ := buildQuotaView(snap1, nil, now)
	if qCred.PlanTier != "pro" || qCred.PlanLabel != "AI Pro" {
		t.Fatalf("expected Pro detection via /credits plan_name, got tier=%q label=%q", qCred.PlanTier, qCred.PlanLabel)
	}
	if qCred.Deduction.Cap5hUsd != 10.0 || qCred.Deduction.CapWeeklyUsd != 35.0 || qCred.ThirdPartyPool.Cap5hUsd != 2.5 || qCred.ThirdPartyPool.CapWeeklyUsd != 5.0 {
		t.Fatalf("expected Pro caps from /credits, got gem=%+v tp=%+v", qCred.Deduction, qCred.ThirdPartyPool)
	}
	// Second /usage fetch must reuse cached /credits tier without re-invoking /credits!
	_ = fetchOfficialQuotaDo()
	rawCredCalls, _ := os.ReadFile(creditsCountFile)
	if cnt := strings.Count(string(rawCredCalls), "1"); cnt != 1 {
		t.Fatalf("expected /credits to be cached after 1 invocation, got %d", cnt)
	}

	// 4. 2-group Pro detection via AGY_ACCOUNT_PLAN_TIER_HINT env var (including cold loading snapshot)
	resetCreditsTierCacheForTest()
	t.Setenv("AGY_ACCOUNT_PLAN_TIER_HINT", "pro")
	qHintCold, _ := buildQuotaView(QuotaSnapshot{Status: QuotaStateLoading, UpdatedAt: now.Format(time.RFC3339), Groups: []QuotaGroup{}}, nil, now)
	if qHintCold.PlanTier != "pro" || qHintCold.PlanLabel != "AI Pro" {
		t.Fatalf("expected AGY_ACCOUNT_PLAN_TIER_HINT=pro on cold snapshot, got tier=%q label=%q", qHintCold.PlanTier, qHintCold.PlanLabel)
	}
	qHint2Grp, _ := buildQuotaView(QuotaSnapshot{Status: "ok", UpdatedAt: now.Format(time.RFC3339), Groups: quotaTestGroups(true, true, 0.5, r5, rw)}, nil, now)
	if qHint2Grp.PlanTier != "pro" || qHint2Grp.Deduction.Cap5hUsd != 10.0 || qHint2Grp.ThirdPartyPool.Cap5hUsd != 2.5 {
		t.Fatalf("expected AGY_ACCOUNT_PLAN_TIER_HINT=pro to drive Pro caps on ambiguous 2-group snapshot, got %+v", qHint2Grp)
	}
}

func TestFetchOfficialQuotaSkipsDisabledAndCountOnlyBuckets(t *testing.T) {
	waitQuotaRefreshIdle(t)
	resetCreditsTierCacheForTest()
	quotaMu.Lock()
	savedCache, savedAt, savedErr, savedAttempt, savedSteps := cachedQuota, cachedQuotaAt, lastQuotaErr, lastQuotaAttemptAt, quotaBackoffSteps
	cachedQuota, cachedQuotaAt, lastQuotaErr, lastQuotaAttemptAt, quotaBackoffSteps = nil, time.Time{}, nil, time.Time{}, 0
	quotaMu.Unlock()
	t.Cleanup(func() {
		waitQuotaRefreshIdle(t)
		resetCreditsTierCacheForTest()
		quotaMu.Lock()
		cachedQuota, cachedQuotaAt, lastQuotaErr, lastQuotaAttemptAt, quotaBackoffSteps = savedCache, savedAt, savedErr, savedAttempt, savedSteps
		quotaMu.Unlock()
	})

	dir := t.TempDir()
	mockBin := filepath.Join(dir, "agy-buckets")
	reset := time.Now().Add(4 * time.Hour).UTC().Format(time.RFC3339)
	payload := `{"command":{"data":{"description":"Google AI Pro","groups":[{"name":"Gemini Models","buckets":[` +
		`{"id":"b-disabled","name":"Disabled 5h","window":"5h","disabled":true,"remaining_fraction":0.0,"reset_time":"` + reset + `"},` +
		`{"id":"b-count","name":"Count-only 5h","window":"5h","remaining_amount":42,"reset_time":"` + reset + `"},` +
		`{"id":"b-weekly","name":"Weekly","window":"weekly","remaining_fraction":0.75,"reset_time":"` + reset + `"}` +
		`]}]}}}`
	if err := os.WriteFile(mockBin, []byte("#!/bin/sh\necho '"+payload+"'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGY_QUOTA_BIN", mockBin)

	snap := fetchOfficialQuotaDo()
	if snap.Status != "ok" || len(snap.Groups) != 1 {
		t.Fatalf("expected ok snapshot with 1 group, got %+v", snap)
	}
	bkts := snap.Groups[0].Buckets
	if len(bkts) != 1 || bkts[0].ID != "b-weekly" || bkts[0].RemainingFraction != 0.75 {
		t.Fatalf("expected disabled & count-only buckets to be skipped (not fabricated as 100%% used), got %+v", bkts)
	}
}

func TestFetchOfficialQuotaCreditsUpgradeURIProEvenWithCarbonInDescription(t *testing.T) {
	waitQuotaRefreshIdle(t)
	resetCreditsTierCacheForTest()
	quotaMu.Lock()
	savedCache, savedAt, savedErr, savedAttempt, savedSteps := cachedQuota, cachedQuotaAt, lastQuotaErr, lastQuotaAttemptAt, quotaBackoffSteps
	cachedQuota, cachedQuotaAt, lastQuotaErr, lastQuotaAttemptAt, quotaBackoffSteps = nil, time.Time{}, nil, time.Time{}, 0
	quotaMu.Unlock()
	t.Cleanup(func() {
		waitQuotaRefreshIdle(t)
		resetCreditsTierCacheForTest()
		quotaMu.Lock()
		cachedQuota, cachedQuotaAt, lastQuotaErr, lastQuotaAttemptAt, quotaBackoffSteps = savedCache, savedAt, savedErr, savedAttempt, savedSteps
		quotaMu.Unlock()
	})

	now := time.Now().UTC()
	r5 := now.Add(3 * time.Hour).Format(time.RFC3339)
	rw := now.Add(6 * 24 * time.Hour).Format(time.RFC3339)
	usageWithCarbon := `{"command":{"data":{"groups":[{"name":"Gemini Models","description":"Models within this group: Gemini 4 Neon, Gemini 4 Carbon, Gemini 4 Argon, Gemini 3.8 Flash","buckets":[{"id":"gemini-5h","window":"5h","remaining_fraction":0.68,"reset_time":"` + r5 + `"},{"id":"gemini-weekly","window":"weekly","remaining_fraction":0.91,"reset_time":"` + rw + `"}]},{"name":"the assistant and GPT models","buckets":[{"id":"3p-weekly","window":"weekly","remaining_fraction":0.5,"reset_time":"` + rw + `"}]}]}}}`

	// 1. Live Google AI Pro shape: Gemini Models description includes Carbon, /credits returns upgrade_uri.
	// Even with a stale AGY_ACCOUNT_PLAN_TIER_HINT=ultra, live /credits upgrade_uri must self-heal to Pro.
	dir := t.TempDir()
	proBin := filepath.Join(dir, "agy-pro-upgrade-uri")
	proScript := "#!/bin/sh\nif [ \"$2\" = \"/credits\" ]; then\n  echo '{\"status\":\"SUCCESS\",\"command\":{\"name\":\"credits\",\"data\":{\"remaining_credits\":0,\"upgrade_uri\":\"https://antigravity.google/g1-upgrade\"}}}'\n  exit 0\nfi\necho '" + usageWithCarbon + "'\n"
	if err := os.WriteFile(proBin, []byte(proScript), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGY_QUOTA_BIN", proBin)
	t.Setenv("AGY_ACCOUNT_PLAN_TIER_HINT", "ultra")
	proSnap := fetchOfficialQuotaDo()
	proView, _ := buildQuotaView(proSnap, nil, now)
	if proView.PlanTier != "pro" || proView.PlanLabel != "AI Pro" {
		t.Fatalf("expected Pro via /credits upgrade_uri despite Carbon in Gemini Models description, got tier=%q label=%q", proView.PlanTier, proView.PlanLabel)
	}
	if proView.Deduction.Cap5hUsd != 10.0 || proView.Deduction.CapWeeklyUsd != 35.0 || proView.ThirdPartyPool.Cap5hUsd != 2.5 || proView.ThirdPartyPool.CapWeeklyUsd != 5.0 {
		t.Fatalf("expected Pro caps ($10/$35 & $2.50/$5.00), got gem=%+v tp=%+v", proView.Deduction, proView.ThirdPartyPool)
	}

	// 2. Live Google AI Ultra 20x shape: /credits returns SUCCESS with no upgrade_uri.
	resetCreditsTierCacheForTest()
	ultraBin := filepath.Join(dir, "agy-ultra-no-upgrade-uri")
	ultraScript := "#!/bin/sh\nif [ \"$2\" = \"/credits\" ]; then\n  echo '{\"status\":\"SUCCESS\",\"command\":{\"name\":\"credits\",\"data\":{\"remaining_credits\":0}}}'\n  exit 0\nfi\necho '" + usageWithCarbon + "'\n"
	if err := os.WriteFile(ultraBin, []byte(ultraScript), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGY_QUOTA_BIN", ultraBin)
	ultraSnap := fetchOfficialQuotaDo()
	ultraView, _ := buildQuotaView(ultraSnap, nil, now)
	if ultraView.PlanTier != "ultra" || ultraView.PlanLabel != "Ultra 20x" {
		t.Fatalf("expected Ultra 20x via /credits with empty upgrade_uri, got tier=%q label=%q", ultraView.PlanTier, ultraView.PlanLabel)
	}
	if ultraView.Deduction.Cap5hUsd != 200.0 || ultraView.Deduction.CapWeeklyUsd != 800.0 {
		t.Fatalf("expected Ultra caps ($200/$800), got %+v", ultraView.Deduction)
	}
}

func TestFetchOfficialQuotaFastAbortsInteractiveAuthPrompt(t *testing.T) {
	waitQuotaRefreshIdle(t)
	resetCreditsTierCacheForTest()
	quotaMu.Lock()
	savedCache, savedAt, savedErr, savedAttempt, savedSteps := cachedQuota, cachedQuotaAt, lastQuotaErr, lastQuotaAttemptAt, quotaBackoffSteps
	cachedQuota, cachedQuotaAt, lastQuotaErr, lastQuotaAttemptAt, quotaBackoffSteps = nil, time.Time{}, nil, time.Time{}, 0
	quotaMu.Unlock()
	origTimeout := officialQuotaTimeout
	officialQuotaTimeout = 10 * time.Second
	t.Cleanup(func() {
		officialQuotaTimeout = origTimeout
		waitQuotaRefreshIdle(t)
		resetCreditsTierCacheForTest()
		quotaMu.Lock()
		cachedQuota, cachedQuotaAt, lastQuotaErr, lastQuotaAttemptAt, quotaBackoffSteps = savedCache, savedAt, savedErr, savedAttempt, savedSteps
		quotaMu.Unlock()
	})

	dir := t.TempDir()
	mockBin := filepath.Join(dir, "agy-auth-prompt")
	script := "#!/bin/sh\necho 'Authentication required. Please visit the URL to log in:'\necho '  https://accounts.google.com/o/oauth2/auth?state=secret'\necho 'Waiting for authentication (timeout 60s)...'\nsleep 30\n"
	if err := os.WriteFile(mockBin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGY_QUOTA_BIN", mockBin)

	t0 := time.Now()
	snap := fetchOfficialQuotaDo()
	elapsed := time.Since(t0)
	if elapsed > 3*time.Second {
		t.Fatalf("expected fast abort on interactive auth prompt (<3s), took %v", elapsed)
	}
	if snap.Status != "error" || !strings.Contains(snap.Error, "Authentication required") {
		t.Fatalf("expected clean Authentication required error, got %+v", snap)
	}
	if strings.Contains(snap.Error, "oauth2") || strings.Contains(snap.Error, "secret") {
		t.Fatalf("expected OAuth URL to be excluded from error string, got %q", snap.Error)
	}
	if !isAuthErrorMessage(snap.Error) {
		t.Fatalf("expected isAuthErrorMessage(%q) == true", snap.Error)
	}
}
