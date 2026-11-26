package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"agy-acp-gateway-go/internal/acp"
)

// --- 1. Green-Zone Monitor-Level Balancing for round-robin / weighted-rr + preferWarmNoTools ---

func TestV2319_GreenMonitorBalancing_RR_WRR_AntiHerdAndWarmNoTools(t *testing.T) {
	mkAcc := func(id string, weight int) AccountProfile {
		return AccountProfile{
			ID:           id,
			Label:        id,
			Enabled:      true,
			Weight:       weight,
			PlanTierHint: "ultra",
		}
	}

	t.Run("RR eliminates 558/284/146 drift and uses inFlight*25 anti-thundering-herd", func(t *testing.T) {
		scored := []candidateScore{
			{acc: mkAcc("u1", 1), monitorZone: monitorZoneGreen, monitorCalls: 558, monitorHardCap: 4800, remQuota: 0.9, configOrd: 0},
			{acc: mkAcc("u2", 1), monitorZone: monitorZoneGreen, monitorCalls: 284, monitorHardCap: 4800, remQuota: 0.9, configOrd: 1},
			{acc: mkAcc("u3", 1), monitorZone: monitorZoneGreen, monitorCalls: 146, monitorHardCap: 4800, remQuota: 0.9, configOrd: 2},
		}
		rr := map[string]uint64{}
		swrr := map[string]map[string]*swrrEntry{}

		chosen, mode := selectAccountByStrategyWithMonitor(RouteStrategyRoundRobin, "", "pool", "gemini-4-carbon", scored, rr, swrr, true, false)
		if chosen.ID != "u3" || mode != "pool-rr" {
			t.Fatalf("expected u3 (lowest monitorCalls=146), got %s (%s)", chosen.ID, mode)
		}

		// Simulate 6 concurrent Turn-1 requests arriving at u3 before monitorCalls updates:
		// effUtil(u3) = (146 + 6*25)/4800 = 0.0617; effUtil(u2) = 284/4800 = 0.0592 -> u2 now wins/shares!
		scored[2].inFlight = 6
		seen := map[string]int{}
		for i := 0; i < 6; i++ {
			c, _ := selectAccountByStrategyWithMonitor(RouteStrategyRoundRobin, "", "pool", "gemini-4-carbon", scored, rr, swrr, true, false)
			seen[c.ID]++
		}
		if seen["u2"] == 0 || seen["u3"] == 0 || seen["u1"] != 0 {
			t.Fatalf("expected anti-thundering-herd to balance between u2 & u3 while excluding drifted u1, got %+v", seen)
		}
	})

	t.Run("RR keeps normal rotation when effUtil spread < 3%", func(t *testing.T) {
		scored := []candidateScore{
			{acc: mkAcc("u1", 1), monitorZone: monitorZoneGreen, monitorCalls: 100, monitorHardCap: 4800, remQuota: 0.9, configOrd: 0},
			{acc: mkAcc("u2", 1), monitorZone: monitorZoneGreen, monitorCalls: 150, monitorHardCap: 4800, remQuota: 0.9, configOrd: 1},
			{acc: mkAcc("u3", 1), monitorZone: monitorZoneGreen, monitorCalls: 200, monitorHardCap: 4800, remQuota: 0.9, configOrd: 2},
		}
		rr := map[string]uint64{}
		swrr := map[string]map[string]*swrrEntry{}
		var got []string
		for i := 0; i < 3; i++ {
			c, _ := selectAccountByStrategyWithMonitor(RouteStrategyRoundRobin, "", "pool", "gemini-4-carbon", scored, rr, swrr, true, false)
			got = append(got, c.ID)
		}
		if strings.Join(got, ",") != "u1,u2,u3" {
			t.Fatalf("expected normal RR order u1,u2,u3 when spread < 3%%, got %v", got)
		}
	})

	t.Run("WRR normalizes effUtil by EffectiveWeight", func(t *testing.T) {
		// u1 has weight=2 and 600 calls; u2 has weight=1 and 300 calls -> proportional, normal SWRR (2:1)!
		proportional := []candidateScore{
			{acc: mkAcc("u1", 2), monitorZone: monitorZoneGreen, monitorCalls: 600, monitorHardCap: 4800, remQuota: 0.9, configOrd: 0},
			{acc: mkAcc("u2", 1), monitorZone: monitorZoneGreen, monitorCalls: 300, monitorHardCap: 4800, remQuota: 0.9, configOrd: 1},
		}
		rr := map[string]uint64{}
		swrr := map[string]map[string]*swrrEntry{}
		counts := map[string]int{}
		for i := 0; i < 6; i++ {
			c, _ := selectAccountByStrategyWithMonitor(RouteStrategyWeightedRR, "", "pool", "gemini-4-carbon", proportional, rr, swrr, true, false)
			counts[c.ID]++
		}
		if counts["u1"] != 4 || counts["u2"] != 2 {
			t.Fatalf("expected proportional 2:1 SWRR split (4/2), got %+v", counts)
		}

		// Now u1 drifts far above its 2x share (1600 vs 200) -> narrows to u2.
		drifted := []candidateScore{
			{acc: mkAcc("u1", 2), monitorZone: monitorZoneGreen, monitorCalls: 1600, monitorHardCap: 4800, remQuota: 0.9, configOrd: 0},
			{acc: mkAcc("u2", 1), monitorZone: monitorZoneGreen, monitorCalls: 200, monitorHardCap: 4800, remQuota: 0.9, configOrd: 1},
		}
		c, _ := selectAccountByStrategyWithMonitor(RouteStrategyWeightedRR, "", "pool", "gemini-4-carbon", drifted, rr, swrr, true, false)
		if c.ID != "u2" {
			t.Fatalf("expected WRR to narrow to u2 when u1 drifts above weighted share, got %s", c.ID)
		}
	})

	t.Run("RR and WRR honor preferWarmNoTools for zero-tool requests", func(t *testing.T) {
		scored := []candidateScore{
			{acc: mkAcc("green1", 1), monitorZone: monitorZoneGreen, monitorCalls: 100, monitorHardCap: 4800, remQuota: 0.9, configOrd: 0},
			{acc: mkAcc("yellow1", 1), monitorZone: monitorZoneYellow, monitorCalls: 3000, monitorHardCap: 4800, remQuota: 0.7, configOrd: 1},
			{acc: mkAcc("red1", 1), monitorZone: monitorZoneRed, monitorCalls: 4000, monitorHardCap: 4800, remQuota: 0.6, configOrd: 2},
		}
		for _, strat := range []string{RouteStrategyRoundRobin, RouteStrategyWeightedRR} {
			rr := map[string]uint64{}
			swrr := map[string]map[string]*swrrEntry{}
			for i := 0; i < 4; i++ {
				c, _ := selectAccountByStrategyWithMonitor(strat, "", "pool", "gemini-4-carbon", scored, rr, swrr, false, true)
				if c.ID == "green1" {
					t.Fatalf("strategy=%s preferWarmNoTools must prefer healthy Yellow/Red over Green, got green1", strat)
				}
			}
		}
	})
}

// --- 2. Safety Bug Fix: isAccountHardBlocked with Custom Primary Account ID ---

func TestV2319_IsAccountHardBlockedCustomPrimaryID(t *testing.T) {
	oldFile := TosTrippedFile
	t.Cleanup(func() {
		resetTosBreakerForTest()
		TosTrippedFile = oldFile
	})
	TosTrippedFile = filepath.Join(t.TempDir(), "tos-tripped")
	resetTosBreakerForTest()

	am := NewAccountManager()
	am.accounts = []AccountProfile{
		{ID: "tr20x", Label: "Custom Primary", Primary: true, Enabled: true},
		{ID: "acc2", Label: "Secondary", Primary: false, Enabled: true},
	}

	if blocked, _, _, _ := am.isAccountHardBlocked("tr20x", time.Now()); blocked {
		t.Fatal("expected tr20x unblocked before ToS trip")
	}

	// Trip ToS circuit breaker.
	TripTosCircuitBreaker("This service has been disabled in this account for violation of Terms of Service.")

	blocked, state, _, _ := am.isAccountHardBlocked("tr20x", time.Now())
	if !blocked || state != "tos_tripped" {
		t.Fatalf("custom primary ID tr20x bypassed IsTosTripped! got blocked=%v state=%q", blocked, state)
	}
	// Non-primary account must NOT be marked tos_tripped from primary's breaker file.
	if b2, st2, _, _ := am.isAccountHardBlocked("acc2", time.Now()); b2 || st2 != "" {
		t.Fatalf("non-primary acc2 should not be blocked by primary ToS breaker, got blocked=%v state=%q", b2, st2)
	}
}

// --- 3. Quota Burn Multiplier Anomaly Detector (Single & Federated) ---

func TestV2319_QuotaBurnAnomalyDetectorAndFederation(t *testing.T) {
	fPtr := func(v float64) *float64 { return &v }

	// Normal spend (< 2.2x): no anomaly
	if got := detectQuotaBurnAnomaly(fPtr(12.0), 8.0, 0.5, 200.0, false, "1.5.18"); got != nil {
		t.Fatalf("expected nil for normal 1.4x ratio, got %+v", got)
	}
	// Warn multiplier anomaly (2.2 <= ratio < 3.0)
	warn := detectQuotaBurnAnomaly(fPtr(12.5), 4.5, 0.5, 200.0, false, "1.5.18")
	if warn == nil || warn.Kind != "multiplier_anomaly" || warn.Severity != "warn" || warn.Ratio != 2.5 || warn.AgyVersion != "1.5.18" {
		t.Fatalf("unexpected warn anomaly: %+v", warn)
	}
	// Critical multiplier anomaly (ratio >= 3.0)
	crit := detectQuotaBurnAnomaly(fPtr(25.0), 5.0, 1.0, 200.0, false, "1.5.18")
	if crit == nil || crit.Kind != "multiplier_anomaly" || crit.Severity != "critical" || crit.Ratio < 4.0 {
		t.Fatalf("unexpected critical anomaly: %+v", crit)
	}
	// Unattributed burn (expected < minExp=4.0 on Ultra $200 cap, poolUsed >= max(6, 8) = $8)
	unatt := detectQuotaBurnAnomaly(fPtr(10.0), 1.0, 0.2, 200.0, false, "1.5.18")
	if unatt == nil || unatt.Kind != "unattributed_burn" || unatt.Severity != "warn" {
		t.Fatalf("unexpected unattributed_burn anomaly: %+v", unatt)
	}
	// Idle window must never trigger anomaly
	if got := detectQuotaBurnAnomaly(fPtr(25.0), 5.0, 1.0, 200.0, true, "1.5.18"); got != nil {
		t.Fatalf("expected nil on idle window, got %+v", got)
	}

	// Verify debounced log suppression (10m window)
	burnAnomalyLogMu.Lock()
	burnAnomalyLogLast = map[string]time.Time{}
	burnAnomalyLogMu.Unlock()
	t0 := time.Now()
	logQuotaBurnAnomalyDebounced("tr20x", "5h", crit, t0)
	logQuotaBurnAnomalyDebounced("tr20x", "5h", crit, t0.Add(2*time.Minute))
	burnAnomalyLogMu.Lock()
	tsAfter := burnAnomalyLogLast["tr20x|5h|multiplier_anomaly|critical"]
	burnAnomalyLogMu.Unlock()
	if !tsAfter.Equal(t0) {
		t.Fatalf("expected debounced log timestamp to stay at %v, got %v", t0, tsAfter)
	}

	// Verify attachQuotaBurnAnomalies + federated aggregation sorting (critical before warn)
	qSelf := QuotaSnapshot{
		Status:     "ok",
		UpdatedAt:  t0.UTC().Format(time.RFC3339),
		GeminiPool: &PoolQuotaState{State: QuotaStateKnown},
		Groups: []QuotaGroup{{
			Name: "Gemini Models",
			Buckets: []QuotaBucket{
				{Window: "5h", UsedFraction: 0.0625, RemainingFraction: 0.9375, ResetTime: t0.Add(3 * time.Hour).UTC().Format(time.RFC3339)},
			},
		}},
		Deduction: &QuotaDeduction{
			Cap5hUsd:        200.0,
			CapWeeklyUsd:    800.0,
			PoolUsed5hUsd:   fPtr(12.5),
			LocalTotal5hUsd: 4.5,
			MonitorEst5hUsd: 0.5,
			LocalSteps5h:    10,
		},
	}
	attachQuotaBurnAnomalies(&qSelf, "primary", "1.5.18", t0)
	if qSelf.Deduction.BurnAnomaly5h == nil || qSelf.Deduction.BurnAnomaly5h.Severity != "warn" {
		t.Fatalf("expected attached warn anomaly on 5h, got %+v", qSelf.Deduction.BurnAnomaly5h)
	}
	items := append(
		collectQuotaBurnAnomalies("primary", "Primary", qSelf),
		map[string]any{"accountId": "acc2", "accountLabel": "Acc 2", "window": "5h", "kind": "multiplier_anomaly", "severity": "critical", "ratio": 4.2},
	)
	sortBurnAnomalies(items)
	if items[0]["severity"] != "critical" || items[0]["accountId"] != "acc2" {
		t.Fatalf("expected critical anomaly sorted first, got %+v", items)
	}
}

// --- 4. MonitorBudgetSnapshot BurnRatePerHr + ETA & AccountHealthView Mirror ---

func TestV2319_MonitorBudgetBurnRateAndETA(t *testing.T) {
	resetMonitorBudgetsForTest()
	resetMonitorTripsForTest()
	now := time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)

	// Record 90 calls in the last 15m -> 360 calls/hr.
	for i := 0; i < 9; i++ {
		RecordMonitorToolDispatchAt("acc_rate", 10, now.Add(-time.Duration(i+1)*time.Minute))
	}
	acc := AccountProfile{ID: "acc_rate", Enabled: true, PlanTierHint: "ultra"}
	snap := EvaluateMonitorBudget(acc, now)
	if snap.BurnRatePerHr < 350 || snap.BurnRatePerHr > 370 {
		t.Fatalf("expected burnRatePerHr ~360, got %.1f", snap.BurnRatePerHr)
	}
	if snap.EtaSoftCapSec <= 0 || snap.EtaDrainCapSec <= snap.EtaSoftCapSec {
		t.Fatalf("expected positive EtaSoftCapSec < EtaDrainCapSec in Green zone, got soft=%d drain=%d", snap.EtaSoftCapSec, snap.EtaDrainCapSec)
	}

	// Verify AccountHealthView mirrors burn rate & ETAs.
	hv := buildSingleHealthViewFromRuntimeWithMonitor(acc, true, 0, 0, nil, false, "", snap, now)
	if hv.MonitorBurnRatePerHr != snap.BurnRatePerHr || hv.MonitorEtaSoftCapSec != snap.EtaSoftCapSec || hv.MonitorEtaDrainCapSec != snap.EtaDrainCapSec {
		t.Fatalf("AccountHealthView did not mirror monitor burn rate/ETA: hv=%+v snap=%+v", hv, snap)
	}

	// Verify 30m fallback when last 15m is quiet.
	resetMonitorBudgetsForTest()
	RecordMonitorToolDispatchAt("acc_rate30", 60, now.Add(-20*time.Minute))
	snap30 := EvaluateMonitorBudget(AccountProfile{ID: "acc_rate30", Enabled: true, PlanTierHint: "ultra"}, now)
	if snap30.BurnRatePerHr < 115 || snap30.BurnRatePerHr > 125 {
		t.Fatalf("expected 30m fallback burnRatePerHr ~120, got %.1f", snap30.BurnRatePerHr)
	}
}

// --- 5. Cgroup RAM Budget Parsing & PoolStats ---

func TestV2319_CgroupMemoryLimitParsingAndPoolBudget(t *testing.T) {
	// Cgroup v2 explicit bytes (3200 MiB)
	if got := parseHostMemLimitMB("3355443200\n", "", ""); got != 3200 {
		t.Fatalf("cgroup v2 expected 3200 MB, got %d", got)
	}
	// Cgroup v2 "max" falls back to cgroup v1 (4096 MiB)
	if got := parseHostMemLimitMB("max\n", "4294967296\n", ""); got != 4096 {
		t.Fatalf("cgroup v1 fallback expected 4096 MB, got %d", got)
	}
	// Cgroup v1 unlimited sentinel falls back to /proc/meminfo MemTotal (16384 MiB)
	if got := parseHostMemLimitMB("max\n", "9223372036854771712\n", "MemTotal:       16777216 kB\nMemFree: 1000 kB\n"); got != 16384 {
		t.Fatalf("/proc/meminfo fallback expected 16384 MB, got %d", got)
	}

	totalW, estMB, limitMB := computePoolWorkerMemoryBudget([]AccountProfile{
		{ID: "a", Enabled: true, MaxLiveWorkers: 8},
		{ID: "b", Enabled: true, MaxLiveWorkers: 4},
		{ID: "c", Enabled: false, MaxLiveWorkers: 10},
	})
	if totalW != 12 || estMB != 12*260 {
		t.Fatalf("expected totalMaxLiveWorkers=12 estPeakRssMB=%d, got total=%d est=%d", 12*260, totalW, estMB)
	}
	if limitMB <= 0 {
		t.Fatalf("expected positive detected hostMemLimitMB on Linux, got %d", limitMB)
	}
}

// --- 6. Anti-Duplicate-Read Prompt & Smart Green Long-Tool Relay ---

func TestV2319_PromptAntiDupReadAndSmartGreenToolRelay(t *testing.T) {
	s := NewAcpProjectSession("sid:v2319-prompt", t.TempDir(), "")
	prompt := s.buildAppendSystemPromptWithText([]ToolDef{{Name: "Read", McpName: "ws_Read_1234", Parameters: map[string]any{}}}, "")
	if !strings.Contains(prompt, "Do not re-`Read` unchanged files") || !strings.Contains(prompt, "`Grep` or `Glob`") {
		t.Fatalf("expected anti-duplicate-Read guidance in append prompt, got: %s", prompt)
	}

	resetMonitorBudgetsForTest()
	resetMonitorTripsForTest()
	t.Setenv("AGY_TOOL_RELAY_MS", "")
	t.Setenv("AGY_TOOL_RELAY_SEC", "")

	// Green <50%: small/read tool stays 55s; Bash >=32K ctx capped at 80s (<90s KV cliff); Bash <32K ctx widens to 135s.
	if d := toolRelayIntervalForTool(0, "Read", 64000); d != 55*time.Second {
		t.Fatalf("expected Read in Green to stay 55s, got %v", d)
	}
	if d := toolRelayIntervalForTool(0, "Bash", 64000); d != 80*time.Second {
		t.Fatalf("expected Bash with ctxTokens>=32K in Green to cap at 80s, got %v", d)
	}
	if d := toolRelayIntervalForTool(0, "Bash", 8000); d != 135*time.Second {
		t.Fatalf("expected Bash with ctxTokens<32K in Green to widen to 135s, got %v", d)
	}
	// Explicit env override is always respected.
	t.Setenv("AGY_TOOL_RELAY_SEC", "42")
	if d := toolRelayIntervalForTool(0, "Bash", 8000); d != 42*time.Second {
		t.Fatalf("expected explicit AGY_TOOL_RELAY_SEC=42 override, got %v", d)
	}
}

// --- 7. BeginShutdown Unblocks Idle HotParked MCP Without Killing Active SSE + 503 Guard ---

type stubEngine struct {
	alive       atomic.Bool
	stopCalls   atomic.Int32
	cancelCalls atomic.Int32
	rpcMu       sync.Mutex
	rpcMethods  []string
	rpcHandler  func(method string, params any) (json.RawMessage, error)
	binInfo     map[string]any
}

func (e *stubEngine) EnsureStarted() error { e.alive.Store(true); return nil }
func (e *stubEngine) StopEngine()          { e.alive.Store(false); e.stopCalls.Add(1) }
func (e *stubEngine) IsAlive() bool        { return e.alive.Load() }
func (e *stubEngine) IsBooting() bool      { return false }
func (e *stubEngine) CurrentGen() uint64   { return 1 }
func (e *stubEngine) Capabilities() acp.Capabilities {
	return acp.Capabilities{McpHTTP: true, Resume: true, Load: true}
}
func (e *stubEngine) RPC(method string, params any, _ time.Duration) (json.RawMessage, error) {
	e.rpcMu.Lock()
	e.rpcMethods = append(e.rpcMethods, method)
	h := e.rpcHandler
	e.rpcMu.Unlock()
	if h != nil {
		return h(method, params)
	}
	return json.RawMessage(`{}`), nil
}
func (e *stubEngine) RegisterListener(string, acp.SessionListener) {}
func (e *stubEngine) UnregisterSession(string)                     {}
func (e *stubEngine) CloseSession(string) error                    { return nil }
func (e *stubEngine) CancelSession(string)                         { e.cancelCalls.Add(1) }
func (e *stubEngine) EnqueueControlReply(map[string]any)           {}
func (e *stubEngine) BinaryInfo() map[string]any {
	if e.binInfo != nil {
		return e.binInfo
	}
	return map[string]any{"alive": e.alive.Load()}
}

func TestV2319_BeginShutdownUnblocksIdleParkedWithoutKillingActiveSSE(t *testing.T) {
	eng := &stubEngine{}
	eng.alive.Store(true)
	gs := NewGatewayServerWithDeps(eng, nil)
	defer gs.Close()

	// Session 1: Idle HotParked MCP tool call (!IsBusy, activeTurn == nil)
	idleSess := NewAcpProjectSession("sid:idle-parked", t.TempDir(), "")
	idleSess.Engine = eng
	idleSess.SessionID = "11111111-1111-4000-8000-000000000101"
	idleSess.MessageCount = 2
	idleCh := make(chan map[string]any, 1)
	idleSess.ParkedCalls["call_idle"] = &ParkedMcpCall{
		Call:       ToolCallItem{ID: "call_idle", Name: "Bash"},
		SessionID:  idleSess.SessionID,
		ResultChan: idleCh,
		ParkedAt:   time.Now(),
	}

	// Session 2: Active streaming SSE turn (IsBusy == true, activeTurn != nil)
	activeSess := NewAcpProjectSession("sid:active-sse", t.TempDir(), "")
	activeSess.Engine = eng
	activeSess.SessionID = "22222222-2222-4000-8000-000000000202"
	activeSess.IsBusy = true
	activeSess.activeTurn = newActiveTurnState(nil, nil)
	activeCh := make(chan map[string]any, 1)
	activeSess.ParkedCalls["call_active"] = &ParkedMcpCall{
		Call:       ToolCallItem{ID: "call_active", Name: "Read"},
		SessionID:  activeSess.SessionID,
		ResultChan: activeCh,
		ParkedAt:   time.Now(),
	}

	gs.mu.Lock()
	gs.sessions[idleSess.ConvKey] = idleSess
	gs.sessions[activeSess.ConvKey] = activeSess
	gs.mu.Unlock()

	unblocked := gs.BeginShutdown()
	if unblocked != 1 {
		t.Fatalf("expected exactly 1 idle parked session unblocked, got %d", unblocked)
	}
	select {
	case res := <-idleCh:
		if isErr, _ := res["isError"].(bool); !isErr {
			t.Fatalf("expected error payload unblocking idle parked MCP call, got %+v", res)
		}
	default:
		t.Fatal("idle parked MCP ResultChan was not unblocked by BeginShutdown")
	}
	select {
	case res := <-activeCh:
		t.Fatalf("BeginShutdown MUST NOT disturb active streaming SSE session, got %+v", res)
	default:
	}
	if idleSess.HibernatedSessionID != "11111111-1111-4000-8000-000000000101" {
		t.Fatalf("expected idle parked session demoted to HibernatedSessionID for resume, got %q", idleSess.HibernatedSessionID)
	}
	if eng.cancelCalls.Load() != 1 {
		t.Fatalf("expected 1 CancelSession for idle parked session, got %d", eng.cancelCalls.Load())
	}

	// Non-health endpoints return 503 during shutdown while /healthz still returns 200.
	recAPI := httptest.NewRecorder()
	gs.ServeHTTP(recAPI, httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{}`)))
	if recAPI.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for /v1/messages during shutdown, got %d", recAPI.Code)
	}
	recHealth := httptest.NewRecorder()
	gs.ServeHTTP(recHealth, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if recHealth.Code != http.StatusOK {
		t.Fatalf("expected 200 for /healthz during shutdown, got %d", recHealth.Code)
	}
}

func TestV2319_MonitorTripWatcherCleanStop(t *testing.T) {
	am := NewAccountManager()
	am.startMonitorTripWatcherWithInterval(20 * time.Millisecond)
	done := make(chan struct{})
	go func() {
		am.stopMonitorTripWatcher()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("stopMonitorTripWatcher hung")
	}
}

// --- 8. AGY_ENGINE_IDLE_GRACE_SEC ---

func TestV2319_EngineIdleGraceSec(t *testing.T) {
	eng := &stubEngine{}
	eng.alive.Store(true)
	gs := NewGatewayServerWithDeps(eng, nil)
	defer gs.Close()

	// Set explicit 180s grace period.
	t.Setenv("AGY_ENGINE_IDLE_GRACE_SEC", "180")

	// First reap sees 0 live workers: starts grace timer, engine MUST stay alive!
	gs.reapIdleWorkers()
	if !eng.IsAlive() {
		t.Fatal("reapIdleWorkers stopped engine immediately despite AGY_ENGINE_IDLE_GRACE_SEC=180")
	}
	// Second immediate reap still inside grace window: engine MUST stay alive!
	gs.reapIdleWorkers()
	if !eng.IsAlive() {
		t.Fatal("reapIdleWorkers stopped engine before 180s grace expired")
	}
	// Simulate 181s elapsed -> engine now stops cleanly.
	gs.engineIdleSinceNs.Store(time.Now().Add(-181 * time.Second).UnixNano())
	gs.reapIdleWorkers()
	if eng.IsAlive() {
		t.Fatal("expected reapIdleWorkers to stop engine after 180s grace elapsed")
	}
}

// --- 9. AGY_ACP_LOG_MAX_MB Size Budget with >=24h Floor ---

func TestV2319_AcpLogMaxMBWith24hProtection(t *testing.T) {
	prevPure := PureApiHome
	prevKeep := AcpLogKeepDays
	prevConvKeep := ConvKeepDays
	prevMaxMB := AcpLogMaxMB
	t.Cleanup(func() {
		PureApiHome = prevPure
		AcpLogKeepDays = prevKeep
		ConvKeepDays = prevConvKeep
		AcpLogMaxMB = prevMaxMB
	})

	tmp := t.TempDir()
	PureApiHome = tmp
	AcpLogKeepDays = 0
	ConvKeepDays = 0
	AcpLogMaxMB = 1 // 1 MB budget

	logDir := filepath.Join(tmp, "antigravity-cli", "log")
	if err := os.MkdirAll(logDir, 0700); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	writeLog := func(name string, sizeBytes int, age time.Duration) string {
		p := filepath.Join(logDir, name)
		if err := os.WriteFile(p, make([]byte, sizeBytes), 0600); err != nil {
			t.Fatal(err)
		}
		mt := now.Add(-age)
		if err := os.Chtimes(p, mt, mt); err != nil {
			t.Fatal(err)
		}
		return p
	}

	// Oldest (72h, 800KB), middle (30h, 800KB), and recent (2h, 800KB) -> total 2.4MB > 1MB cap.
	pOld := writeLog("acp-worker-20261123_100000-1.log", 800*1024, 72*time.Hour)
	pMid := writeLog("acp-worker-20261125_060000-2.log", 800*1024, 30*time.Hour)
	pFresh := writeLog("acp-worker-20261126_100000-3.log", 800*1024, 2*time.Hour)

	gs := &GatewayServer{}
	cleaned := gs.CleanupExpiredAcpLogs(now)
	if cleaned != 2 {
		t.Fatalf("expected 2 old logs pruned to get under 1MB cap, got %d", cleaned)
	}
	if _, err := os.Stat(pOld); !os.IsNotExist(err) {
		t.Fatal("expected 72h oldest log to be removed")
	}
	if _, err := os.Stat(pMid); !os.IsNotExist(err) {
		t.Fatal("expected 30h log to be removed to satisfy 1MB budget")
	}
	if _, err := os.Stat(pFresh); err != nil {
		t.Fatalf("2h fresh log MUST be protected by 24h floor, stat err=%v", err)
	}
}

// --- 10. SQLite Indexes, 2h Boot Restore vs 7d Lazy Restore, Compaction & getKnownGatewaySids Short-Circuit ---

func TestV2319_SQLiteIndexes_2hBootVs7dLazy_CompactionAndLogShortCircuit(t *testing.T) {
	prevDB := GatewayStateDB
	prevLog := GatewayLogFile
	t.Cleanup(func() {
		initConfig()
		InitGlobalStore(prevDB)
		GatewayLogFile = prevLog
	})

	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "v2319.db")
	logPath := filepath.Join(tmp, "gateway.log")
	// Write a log-only session ID to prove getKnownGatewaySids short-circuits when GlobalStore is ready.
	logOnlySid := "99999999-9999-4000-8000-000000000999"
	if err := os.WriteFile(logPath, []byte(`{"sessionId":"`+logOnlySid+`"}`+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGY_STATE_DB", dbPath)
	t.Setenv("AGY_GW_LOG", logPath)
	initConfig()
	InitGlobalStore(dbPath)

	// 1. Verify all 5 new indexes exist in sqlite_master.
	wantIndexes := []string{"idx_conv_sid", "idx_conv_hist_fp", "idx_conv_updated", "idx_turns_conv_id", "idx_turns_sid"}
	GlobalStore.mu.Lock()
	for _, idx := range wantIndexes {
		var name string
		err := GlobalStore.db.QueryRow(`SELECT name FROM sqlite_master WHERE type='index' AND name=?`, idx).Scan(&name)
		if err != nil || name != idx {
			GlobalStore.mu.Unlock()
			t.Fatalf("expected SQLite index %s to exist, err=%v", idx, err)
		}
	}
	GlobalStore.mu.Unlock()

	// 2. Verify getKnownGatewaySids short-circuits to GlobalStore (never reads gateway.log).
	gwSidMu.Lock()
	knownGwSidsLogPath = ""
	logGwSids = map[string]bool{}
	gwSidMu.Unlock()
	if sids := getKnownGatewaySids(); sids[logOnlySid] {
		t.Fatalf("expected getKnownGatewaySids to short-circuit when GlobalStore.IsReady() without reading gateway.log")
	}

	// 3. Verify 2h boot RestoreDetachedSessions vs 7d lazy RestoreSingleSession + CompactExpiredData.
	now := time.Now()
	insertConv := func(key, sid string, age time.Duration) {
		snap := SessionSnapshot{
			ConvKey:          key,
			SessionID:        sid,
			TenantKey:        "tk-" + key,
			Cwd:              tmp,
			ModelID:          "gemini-4-carbon",
			ThoughtLevel:     "medium",
			ToolFingerprint:  "fp",
			InitToolsJSON:    `[{"name":"Read"}]`,
			CurToolsJSON:     `[{"name":"Read"}]`,
			SystemText:       strings.Repeat("sys", 100),
			InitSystemText:   strings.Repeat("init", 100),
			InitAppendPrompt: strings.Repeat("app", 100),
			MsgCount:         2,
			HistoryFp:        "hfp-" + key,
			State:            "Detached",
			CreatedAtMs:      now.Add(-age).UnixMilli(),
			UpdatedAtMs:      now.Add(-age).UnixMilli(),
		}
		GlobalStore.UpsertSessionSnapshot(snap)
	}
	insertConv("sid:recent-30m", "11111111-1111-4000-8000-000000000011", 30*time.Minute)
	insertConv("sid:older-6h", "22222222-2222-4000-8000-000000000022", 6*time.Hour)
	insertConv("sid:expired-20d", "33333333-3333-4000-8000-000000000033", 20*24*time.Hour)

	gs := &GatewayServer{
		sessions: make(map[string]*AcpProjectSession),
		tenants:  make(map[string]*AcpProjectSession),
	}
	restored := GlobalStore.RestoreDetachedSessions(gs)
	if restored != 1 || gs.sessions["sid:recent-30m"] == nil || gs.sessions["sid:older-6h"] != nil {
		t.Fatalf("expected boot restore to load only 2h window (1 session), got restored=%d keys=%v", restored, len(gs.sessions))
	}
	// 6h session MUST still restore lazily via RestoreSingleSession (7d window)!
	lazy := GlobalStore.RestoreSingleSession("sid:older-6h")
	if lazy == nil || lazy.HibernatedSessionID != "22222222-2222-4000-8000-000000000022" {
		t.Fatalf("expected 6h session to lazy-restore within 7d window, got %+v", lazy)
	}

	// CompactExpiredData clears blobs on >14d rows while keeping recent rows intact.
	compacted, err := GlobalStore.CompactExpiredData(now, 0)
	if err != nil || compacted != 1 {
		t.Fatalf("expected 1 compacted expired conversation (>14d), got compacted=%d err=%v", compacted, err)
	}
	var expSys, recentSys string
	GlobalStore.mu.Lock()
	_ = GlobalStore.db.QueryRow(`SELECT system_text FROM conversations WHERE conv_key = ?`, "sid:expired-20d").Scan(&expSys)
	_ = GlobalStore.db.QueryRow(`SELECT system_text FROM conversations WHERE conv_key = ?`, "sid:recent-30m").Scan(&recentSys)
	GlobalStore.mu.Unlock()
	if expSys != "" || recentSys == "" {
		t.Fatalf("unexpected compaction result: expSys=%q recentSysLen=%d", expSys, len(recentSys))
	}
}

// --- 11. Skip Redundant set_config_option RPC & Typed acpUpdatePayload Fast-Path ---

func TestV2319_SkipRedundantSetConfigOptionAndTypedUpdateFastPath(t *testing.T) {
	// Case A: session/new already returns matching model & thought_level -> ZERO set_config_option RPCs!
	engMatch := &stubEngine{
		rpcHandler: func(method string, _ any) (json.RawMessage, error) {
			if method == "session/new" {
				return json.RawMessage(`{
					"sessionId": "44444444-4444-4000-8000-000000000044",
					"configOptions": [
						{"id": "model", "currentValue": "gemini-4-carbon"},
						{"id": "thought_level", "currentValue": "medium"}
					]
				}`), nil
			}
			return json.RawMessage(`{}`), nil
		},
	}
	engMatch.alive.Store(true)
	s1 := NewAcpProjectSession("sid:skip-cfg-all", t.TempDir(), "")
	s1.Engine = engMatch
	s1.ModelID = "gemini-4-carbon"
	if err := s1.openFreshSession(nil, "medium"); err != nil {
		t.Fatalf("openFreshSession failed: %v", err)
	}
	for _, m := range engMatch.rpcMethods {
		if m == "session/set_config_option" {
			t.Fatalf("expected 0 set_config_option RPCs when initial configOptions match, got %v", engMatch.rpcMethods)
		}
	}

	// Case B: model differs, model ack already sets thought_level=high -> only 1 set_config_option RPC!
	engOne := &stubEngine{
		rpcHandler: func(method string, _ any) (json.RawMessage, error) {
			switch method {
			case "session/new":
				return json.RawMessage(`{
					"sessionId": "55555555-5555-4000-8000-000000000055",
					"configOptions": [
						{"id": "model", "currentValue": "gemini-4-carbon"},
						{"id": "thought_level", "currentValue": "medium"}
					]
				}`), nil
			case "session/set_config_option":
				return json.RawMessage(`{
					"configOptions": [
						{"id": "model", "currentValue": "gemini-3.8-flash"},
						{"id": "thought_level", "currentValue": "high"}
					]
				}`), nil
			}
			return json.RawMessage(`{}`), nil
		},
	}
	engOne.alive.Store(true)
	s2 := NewAcpProjectSession("sid:skip-cfg-tier", t.TempDir(), "")
	s2.Engine = engOne
	s2.ModelID = "gemini-3.8-flash"
	if err := s2.openFreshSession(nil, "high"); err != nil {
		t.Fatalf("openFreshSession failed: %v", err)
	}
	setCnt := 0
	for _, m := range engOne.rpcMethods {
		if m == "session/set_config_option" {
			setCnt++
		}
	}
	if setCnt != 1 {
		t.Fatalf("expected exactly 1 set_config_option RPC (tier skipped via model ack), got %d (%v)", setCnt, engOne.rpcMethods)
	}

	// Typed acpUpdatePayload fast-path for agent_message_chunk & agent_thought_chunk + generic fallback.
	eng := newSharedAcpEngine()
	sess := NewAcpProjectSession("sid:fast-update", t.TempDir(), "")
	turn := newActiveTurnState(nil, nil)
	sess.activeTurn = turn
	eng.RegisterListener("sid-fast", sess)

	dispatchAcpSessionUpdate(eng, json.RawMessage(`{"sessionId":"sid-fast","update":{"sessionUpdate":"agent_thought_chunk","content":{"type":"text","text":"plan "}}}`))
	dispatchAcpSessionUpdate(eng, json.RawMessage(`{"sessionId":"sid-fast","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"hello"}}}`))
	dispatchAcpSessionUpdate(eng, json.RawMessage(`{"sessionId":"sid-fast","update":{"sessionUpdate":"usage_update","used":12345,"size":1048576}}`))

	turn.mu.Lock()
	gotText := strings.Join(turn.textChunks, "")
	gotThought := strings.Join(turn.thoughtChks, "")
	turn.mu.Unlock()
	if gotText != "hello" || gotThought != "plan " {
		t.Fatalf("typed update fast-path mismatch: text=%q thought=%q", gotText, gotThought)
	}
	if sess.LastReportedContextUsed != 12345 || sess.LastReportedContextSize != 1048576 {
		t.Fatalf("generic fallback usage_update mismatch: used=%d size=%d", sess.LastReportedContextUsed, sess.LastReportedContextSize)
	}
}

// --- 12. Cached Negative IsTosTripped + Gateway Binary Version Info ---

func TestV2319_CachedNegativeIsTosTrippedAndGatewayBinaryStatus(t *testing.T) {
	oldFile := TosTrippedFile
	t.Cleanup(func() {
		resetTosBreakerForTest()
		TosTrippedFile = oldFile
	})
	TosTrippedFile = filepath.Join(t.TempDir(), "tos-tripped")
	resetTosBreakerForTest()

	if tripped, _ := IsTosTripped(); tripped {
		t.Fatal("expected initial IsTosTripped==false")
	}
	if tosNegCacheUntilNs.Load() == 0 {
		t.Fatal("expected negative cache TTL to be populated")
	}
	// Writing marker directly on disk is cached for 1s until TTL expires or resetTosBreakerForTest/TripTosCircuitBreaker runs.
	if err := os.WriteFile(TosTrippedFile, []byte(`{"tripped":true,"reason":"disk"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if tripped, _ := IsTosTripped(); tripped {
		t.Fatal("expected cached negative fast-path within 1s TTL")
	}
	resetTosNegativeCache()
	if tripped, reason := IsTosTripped(); !tripped || reason != "disk" {
		t.Fatalf("expected IsTosTripped==true after resetting negative cache, got %v %q", tripped, reason)
	}
	resetTosBreakerForTest()
	if tripped, _ := IsTosTripped(); tripped {
		t.Fatal("expected resetTosBreakerForTest to clear breaker cleanly")
	}

	// Verify resolveGatewayBinaryStatus returns agyVersion & engineAlive from BinaryInfo with zero RPC.
	fakeScript := writeMockAcpScript(t, t.TempDir(), "agy-mock", "#!/bin/sh\necho 'agy version 1.5.18'\n")
	eng := &stubEngine{
		binInfo: map[string]any{
			"alive":   true,
			"running": fakeScript,
			"current": fakeScript,
		},
	}
	eng.alive.Store(true)
	ver, alive, pending := resolveGatewayBinaryStatus(eng)
	if ver != "1.5.18" || !alive || pending {
		t.Fatalf("unexpected resolveGatewayBinaryStatus: ver=%q alive=%v pending=%v", ver, alive, pending)
	}
}

// --- 13. Verifier Regression Tests (Bugs #1–#4 from board #80) ---

func TestV2319_VerifierRegressions_RollupBoundary_HotCompaction_StaleOpts_PendingSiblings(t *testing.T) {
	t.Run("Bug1: Cold+Active rollup has zero gap and zero overlap when cutoff advances within 1h cache TTL", func(t *testing.T) {
		prevDB := GatewayStateDB
		t.Cleanup(func() {
			initConfig()
			InitGlobalStore(prevDB)
		})
		tmp := t.TempDir()
		dbPath := filepath.Join(tmp, "rollup_boundary.db")
		t.Setenv("AGY_STATE_DB", dbPath)
		initConfig()
		InitGlobalStore(dbPath)

		t0 := time.Now().Unix()
		cutoff0 := t0 - activeUsageStepsWindowSec
		insertStep := func(idx int, tsSec int64) {
			GlobalStore.mu.Lock()
			_, err := GlobalStore.db.Exec(`
				INSERT OR REPLACE INTO usage_steps (
					session_id, step_idx, source_cat, ts_sec, ttft_ms, stream_ms, elapsed_ms, tps, decode_tps,
					model_enum, model_id, model_label, model_family, model_tier, quota_group,
					raw_in, raw_cr, aligned_in, aligned_cr, out_tok, vis_out_tok, th_tok,
					has_thinking, cw_tok, tier_mult, in_cost_usd, cr_cost_usd, out_cost_usd,
					th_cost_usd, cost_usd, base1x_cost_usd, nocache_cost_usd, saved_usd, is_dsh_bridge, served_model
				) VALUES ('aaaaaaaa-aaaa-4000-8000-0000000000aa', ?, 'CLAUDE_GW', ?, 300, 100, 400, 50.0, 50.0, 0,
					'gemini-4-carbon', 'Gemini 4 Carbon', 'gemini', 'medium', 'gemini',
					1000, 2000, 1000, 2000, 100, 100, 0, 0, 0, 1.0, 0.01, 0.01, 0.01, 0.0, 0.03, 0.03, 0.05, 0.02, 1, 'gemini-4-carbon')`,
				idx, tsSec,
			)
			GlobalStore.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
		}
		// Step 1 strictly cold (< cutoff0), Step 2 in [cutoff0, cutoff0+1800) boundary zone, Step 3 recent.
		insertStep(1, cutoff0-3600)
		insertStep(2, cutoff0+900)
		insertStep(3, t0-60)

		// First call caches cold rollup at cutoff0.
		cold0, _, used0, ok0 := GlobalStore.QueryColdWindowRollup(nil, cutoff0)
		if !ok0 || used0 != cutoff0 || cold0.StepsCount != 1 {
			t.Fatalf("expected cold0=1 at used0=%d, got ok=%v used=%d steps=%d", cutoff0, ok0, used0, cold0.StepsCount)
		}
		// Advance cutoff by +1800s (still within 3600s cache TTL): cold rollup hits cache at cutoff0,
		// so returned cutoffUsed MUST be cutoff0 and active query >= cutoffUsed MUST include Step 2 & 3!
		cold1, _, used1, ok1 := GlobalStore.QueryColdWindowRollup(nil, cutoff0+1800)
		if !ok1 || used1 != cutoff0 || cold1.StepsCount != 1 {
			t.Fatalf("expected cached cold1=1 with cutoffUsed=%d, got ok=%v used=%d steps=%d", cutoff0, ok1, used1, cold1.StepsCount)
		}
		active, okAct := GlobalStore.QueryActiveUsageStepsSince(nil, used1)
		if !okAct || len(active) != 2 {
			t.Fatalf("expected 2 active steps (Steps 2 & 3) with zero gap/overlap, got ok=%v len=%d", okAct, len(active))
		}
		merged := mergeWindowAggregates([]WindowAggregate{cold1, AggregateSteps(active)})
		if merged.StepsCount != 3 {
			t.Fatalf("expected merged allTime StepsCount=3, got %d", merged.StepsCount)
		}
	})

	t.Run("Bug2: CompactExpiredData preserves conversations still active in conversation_hot", func(t *testing.T) {
		prevDB := GatewayStateDB
		t.Cleanup(func() {
			initConfig()
			InitGlobalStore(prevDB)
		})
		tmp := t.TempDir()
		dbPath := filepath.Join(tmp, "compact_hot.db")
		t.Setenv("AGY_STATE_DB", dbPath)
		initConfig()
		InitGlobalStore(dbPath)

		now := time.Now()
		oldMs := now.Add(-20 * 24 * time.Hour).UnixMilli()
		hotMs := now.Add(-10 * time.Minute).UnixMilli()

		GlobalStore.UpsertSessionSnapshot(SessionSnapshot{
			ConvKey:          "sid:long-lived-active",
			SessionID:        "bbbbbbbb-bbbb-4000-8000-0000000000bb",
			TenantKey:        "tk-hot",
			Cwd:              tmp,
			ModelID:          "gemini-4-carbon",
			ThoughtLevel:     "medium",
			ToolFingerprint:  "fp",
			InitToolsJSON:    `[{"name":"Read"}]`,
			CurToolsJSON:     `[{"name":"Read"}]`,
			SystemText:       "keep-me-system",
			InitSystemText:   "keep-me-init",
			InitAppendPrompt: "keep-me-append",
			MsgCount:         5,
			HistoryFp:        "hfp",
			State:            "LiveIdle",
			CreatedAtMs:      oldMs,
			UpdatedAtMs:      oldMs,
		})
		// Simulate ongoing hot turns updating conversation_hot.updated_at_ms without rewriting conversations.updated_at_ms.
		GlobalStore.mu.Lock()
		_, err := GlobalStore.db.Exec(`
			INSERT OR REPLACE INTO conversation_hot (conv_key, agy_session_id, msg_count, history_fp, state, updated_at_ms)
			VALUES ('sid:long-lived-active', 'bbbbbbbb-bbbb-4000-8000-0000000000bb', 6, 'hfp2', 'LiveIdle', ?)`, hotMs)
		GlobalStore.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}

		compacted, err := GlobalStore.CompactExpiredData(now, 14)
		if err != nil || compacted != 0 {
			t.Fatalf("expected 0 compacted rows for hot-active conversation, got compacted=%d err=%v", compacted, err)
		}
		var sysText string
		GlobalStore.mu.Lock()
		_ = GlobalStore.db.QueryRow(`SELECT system_text FROM conversations WHERE conv_key = 'sid:long-lived-active'`).Scan(&sysText)
		GlobalStore.mu.Unlock()
		if sysText != "keep-me-system" {
			t.Fatalf("CompactExpiredData wiped active conversation_hot blobs! system_text=%q", sysText)
		}
	})

	t.Run("Bug3: pinSessionConfigWithInitial does not skip thought_level using stale pre-model-switch curOpts", func(t *testing.T) {
		var setCalls []string
		eng := &stubEngine{
			rpcHandler: func(method string, params any) (json.RawMessage, error) {
				if method == "session/new" {
					return json.RawMessage(`{
						"sessionId": "cccccccc-cccc-4000-8000-0000000000cc",
						"configOptions": [
							{"id": "model", "currentValue": "gemini-4-carbon"},
							{"id": "thought_level", "currentValue": "medium"}
						]
					}`), nil
				}
				if method == "session/set_config_option" {
					pm, _ := params.(map[string]any)
					cid, _ := pm["configId"].(string)
					val, _ := pm["value"].(string)
					setCalls = append(setCalls, cid+"="+val)
					if cid == "model" {
						// Model switch ack returns ONLY model (no thought_level) -> MUST NOT reuse stale pre-switch thought_level!
						return json.RawMessage(`{"configOptions":[{"id":"model","currentValue":"` + val + `"}]}`), nil
					}
					return json.RawMessage(`{"configOptions":[{"id":"` + cid + `","currentValue":"` + val + `"}]}`), nil
				}
				return json.RawMessage(`{}`), nil
			},
		}
		eng.alive.Store(true)
		s := NewAcpProjectSession("sid:stale-tier-check", t.TempDir(), "")
		s.Engine = eng
		s.ModelID = "gemini-3.8-flash"
		if err := s.openFreshSession(nil, "medium"); err != nil {
			t.Fatalf("openFreshSession failed: %v", err)
		}
		if strings.Join(setCalls, ",") != "model=gemini-3.8-flash,thought_level=medium" {
			t.Fatalf("expected both model and thought_level RPCs when model ack omits thought_level, got %v", setCalls)
		}
	})

	t.Run("Bug4: BeginShutdown drains PendingSiblingCalls ResultChan", func(t *testing.T) {
		eng := &stubEngine{}
		eng.alive.Store(true)
		gs := NewGatewayServerWithDeps(eng, nil)
		defer gs.Close()

		sess := NewAcpProjectSession("sid:pending-sib-only", t.TempDir(), "")
		sess.Engine = eng
		sess.SessionID = "dddddddd-dddd-4000-8000-0000000000dd"
		sess.MessageCount = 2
		sibCh := make(chan map[string]any, 1)
		sess.PendingSiblingCalls = []*ParkedMcpCall{{
			Call:       ToolCallItem{ID: "call_sib_1", Name: "Read"},
			SessionID:  sess.SessionID,
			ResultChan: sibCh,
			ParkedAt:   time.Now(),
		}}

		gs.mu.Lock()
		gs.sessions[sess.ConvKey] = sess
		gs.mu.Unlock()

		if n := gs.BeginShutdown(); n != 1 {
			t.Fatalf("expected 1 session unblocked with PendingSiblingCalls, got %d", n)
		}
		select {
		case res := <-sibCh:
			if isErr, _ := res["isError"].(bool); !isErr {
				t.Fatalf("expected error payload on PendingSiblingCalls ResultChan, got %+v", res)
			}
		default:
			t.Fatal("BeginShutdown did not signal PendingSiblingCalls ResultChan")
		}
		if !sess.HibernatedParkedCallIDs["call_sib_1"] {
			t.Fatalf("expected call_sib_1 recorded in HibernatedParkedCallIDs, got %+v", sess.HibernatedParkedCallIDs)
		}
	})
}
