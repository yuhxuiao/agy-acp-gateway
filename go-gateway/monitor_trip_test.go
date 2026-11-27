package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	tripLineFail      = `I1126 13:05:22.118163    3000 runner.go:957] Monitor sir_flash_lite FINISHED: triggered=false, reason="Failed: LLM call failed: modelAPI.Generate failed: transport error (ResourceExhausted, HTTP 429): You have exhausted your capacity on this model. Your quota will reset after 2h29m49s.: RESOURCE_EXHAUSTED (code 429)", duration=12ms`
	tripLineTransient = `I1126 12:53:11.222913    2128 runner.go:957] Monitor sir_flash_lite FINISHED: triggered=false, reason="Failed: LLM call failed: modelAPI.Generate failed: transport error (ResourceExhausted, HTTP 429): You have exhausted your capacity on this model. Your quota will reset after 0s.: RESOURCE_EXHAUSTED (code 429)", duration=12ms`
	tripLineOK        = `I1126 13:03:34.549345     863 runner.go:957] Monitor sir_flash_lite FINISHED: triggered=false, reason="", duration=536.805123ms`
)

func newTripTestAccount() *monitorTripAccount {
	return &monitorTripAccount{files: make(map[string]*monitorTripFile)}
}

func TestMonitorTripParseResetDuration(t *testing.T) {
	cases := map[string]time.Duration{
		"reset after 2h29m49s.":   2*time.Hour + 29*time.Minute + 49*time.Second,
		"reset after 2h29m49.5s":  2*time.Hour + 29*time.Minute + 49*time.Second + 500*time.Millisecond,
		"reset after 125.4s":      125*time.Second + 400*time.Millisecond,
		"reset after 1d2h":        26 * time.Hour,
		"reset after 1d2h3m4.25s": 26*time.Hour + 3*time.Minute + 4*time.Second + 250*time.Millisecond,
		"reset after 1h50m16s":    time.Hour + 50*time.Minute + 16*time.Second,
		"reset after 9m5s":        9*time.Minute + 5*time.Second,
		"reset after 0s.":         0,
	}
	for in, want := range cases {
		got, ok := parseMonitorTripResetDuration([]byte(in))
		if !ok || got != want {
			t.Fatalf("%q: got %v ok=%v want %v", in, got, ok, want)
		}
	}
	if _, ok := parseMonitorTripResetDuration([]byte("no reset info")); ok {
		t.Fatal("expected no match")
	}
	if _, ok := parseMonitorTripResetDuration([]byte("reset after ")); ok {
		t.Fatal("expected empty duration not to match")
	}
}

func TestMonitorTripApplyLineHysteresisSubsecondAndStrictOK(t *testing.T) {
	loc := time.Local
	lineTime := time.Date(2026, time.November, 26, 13, 5, 22, 118163000, loc)
	now := lineTime.Add(time.Minute)

	a := newTripTestAccount()
	// 1. Transient 0s 429 ignored.
	a.applyLine([]byte(tripLineTransient), 2026, time.November, loc, now)
	if !a.failAt.IsZero() {
		t.Fatal("transient 0s 429 must be ignored")
	}

	// 2. Unparseable glog header (mid-line fragment) must NEVER fall back to now.
	a.applyLine([]byte(`Monitor sir_flash_lite FINISHED: triggered=false, reason="Failed: RESOURCE_EXHAUSTED reset after 2h0m0s", duration=12ms`), 2026, time.November, loc, now)
	if !a.failAt.IsZero() {
		t.Fatal("unparseable glog header must be ignored, never synthesized with now")
	}

	// 3. Real 429 lockout parses sub-second microsecond precision.
	a.applyLine([]byte(tripLineFail), 2026, time.November, loc, now)
	if a.failAt.Nanosecond() != 118163000 || !isMonitorTripped(a.until, a.failAt, a.okAt, now) {
		t.Fatalf("expected tripped with microsecond failAt, got failAt=%v until=%v", a.failAt, a.until)
	}

	// 4. Earlier OK line (13:03) does not clear 13:05 failure.
	a.applyLine([]byte(tripLineOK), 2026, time.November, loc, now)
	if !isMonitorTripped(a.until, a.failAt, a.okAt, now) {
		t.Fatal("earlier OK line must not clear trip")
	}

	// 5. Same-second / +2s in-flight OK must NOT clear trip (15s hysteresis prevents flapping).
	a.applyLine([]byte(`I1126 13:05:22.900000     863 runner.go:957] Monitor sir_flash_lite FINISHED: triggered=false, reason="", duration=900ms`), 2026, time.November, loc, now)
	a.applyLine([]byte(`I1126 13:05:24.500000     863 runner.go:957] Monitor sir_flash_lite FINISHED: triggered=false, reason="", duration=2400ms`), 2026, time.November, loc, now)
	if !isMonitorTripped(a.until, a.failAt, a.okAt, now) {
		t.Fatalf("concurrent in-flight OK (+2.4s < 15s hysteresis) must NOT clear trip: failAt=%v okAt=%v", a.failAt, a.okAt)
	}

	// 6. Non-Failed error/timeout line (non-empty reason with triggered=false) must NOT update okAt or clear trip.
	a.applyLine([]byte(`I1126 13:08:00.000000     863 runner.go:957] Monitor sir_flash_lite FINISHED: triggered=false, reason="context deadline exceeded", duration=10s`), 2026, time.November, loc, now)
	if !isMonitorTripped(a.until, a.failAt, a.okAt, now) {
		t.Fatalf("non-empty error reason line must NOT clear trip: okAt=%v", a.okAt)
	}

	// 7. Uppercase RESOURCE_EXHAUSTED (without camel-case ResourceExhausted) also triggers trip.
	a2 := newTripTestAccount()
	a2.applyLine([]byte(`I1126 13:05:22.500000    3000 runner.go:957] Monitor sir_flash_lite FINISHED: triggered=false, reason="Failed: RESOURCE_EXHAUSTED reset after 150.5s", duration=10ms`), 2026, time.November, loc, now)
	if !isMonitorTripped(a2.until, a2.failAt, a2.okAt, now) {
		t.Fatal("uppercase RESOURCE_EXHAUSTED with fractional reset must trip")
	}

	// 8. Explicit healthy OK >= 15s after failAt clears the trip.
	a.applyLine([]byte(`I1126 13:05:38.000000     863 runner.go:957] Monitor sir_flash_lite FINISHED: triggered=false, reason="", duration=400ms`), 2026, time.November, loc, now)
	if isMonitorTripped(a.until, a.failAt, a.okAt, now) {
		t.Fatalf("explicit healthy OK >=15s after failAt must clear trip: failAt=%v okAt=%v", a.failAt, a.okAt)
	}

	// 9. Legitimate triggered=true verdict (without error keywords) >= 15s after failAt also clears.
	a2.applyLine([]byte(`I1126 13:10:00.000000     863 runner.go:957] Monitor sir_flash_lite FINISHED: triggered=true, reason="shell command flagged", duration=300ms`), 2026, time.November, loc, now)
	if isMonitorTripped(a2.until, a2.failAt, a2.okAt, now) {
		t.Fatal("healthy triggered=true verdict >=15s after failAt must clear trip")
	}
}

func TestMonitorTripMidLineSeekSafetyAndStartupWindow(t *testing.T) {
	resetMonitorTripsForTest()
	t.Cleanup(resetMonitorTripsForTest)
	dir := t.TempDir()
	now := time.Now()

	// Create a >512KB file where an expired 429 sits right across the 512KB tail cut boundary,
	// so start = size - monitorTripInitTail lands mid-line on the expired 429!
	oldTs := now.Add(-4 * time.Hour)
	expiredFail := fmt.Sprintf("I%02d%02d %02d:%02d:%02d.123456    1 runner.go:957] Monitor sir_flash_lite FINISHED: triggered=false, reason=\"Failed: ResourceExhausted reset after 2h0m0s\", duration=12ms\n",
		int(oldTs.Month()), oldTs.Day(), oldTs.Hour(), oldTs.Minute(), oldTs.Second())
	padLine := strings.Repeat("A", 100) + "\n"
	var b strings.Builder
	b.WriteString(strings.Repeat(padLine, 500))
	b.WriteString(expiredFail)
	// Place exactly ~monitorTripInitTail - 20 bytes after the start of expiredFail so tail seek cuts mid-line.
	for b.Len() < monitorTripInitTail+500*len(padLine)+20 {
		b.WriteString(padLine)
	}
	name := "acp-worker-" + oldTs.Format("20060102_150405") + "-1.log"
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	// Set mtime to 2 hours ago (inside 6h startup window, outside legacy 10m window).
	twoHrsAgo := now.Add(-2 * time.Hour)
	if err := os.Chtimes(p, twoHrsAgo, twoHrsAgo); err != nil {
		t.Fatal(err)
	}
	scanMonitorTripAccount("acc-mid", dir, now)
	if !MonitorTripUntil("acc-mid", now).IsZero() {
		t.Fatal("mid-line tail cut of expired 429 must NOT create a phantom trip")
	}

	// Also verify 6h startup window recovers a real active lockout whose log stopped updating 90m ago.
	dir2 := t.TempDir()
	ts90m := now.Add(-90 * time.Minute)
	activeFail := fmt.Sprintf("I%02d%02d %02d:%02d:%02d.000000    1 runner.go:957] Monitor sir_flash_lite FINISHED: triggered=false, reason=\"Failed: ResourceExhausted reset after 3h0m0s\", duration=12ms\n",
		int(ts90m.Month()), ts90m.Day(), ts90m.Hour(), ts90m.Minute(), ts90m.Second())
	p2 := filepath.Join(dir2, "acp-worker-"+ts90m.Format("20060102_150405")+"-2.log")
	if err := os.WriteFile(p2, []byte(activeFail), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(p2, ts90m, ts90m); err != nil {
		t.Fatal(err)
	}
	scanMonitorTripAccount("acc-6h", dir2, now)
	if until := MonitorTripUntil("acc-6h", now); until.IsZero() || until.Before(now.Add(time.Hour)) {
		t.Fatalf("6h startup scan must recover active lockout from 90m-old quiet worker log, got until=%v", until)
	}
}

func TestMonitorTripPersistenceAndClearAndPrune(t *testing.T) {
	resetMonitorTripsForTest()
	t.Cleanup(resetMonitorTripsForTest)
	stateDir := t.TempDir()
	logDir := t.TempDir()
	LoadMonitorStateFromDir(stateDir)

	now := time.Now()
	ts := now.Add(-30 * time.Second)
	fail := fmt.Sprintf("I%02d%02d %02d:%02d:%02d.250000    1 runner.go:957] Monitor sir_flash_lite FINISHED: triggered=false, reason=\"Failed: ResourceExhausted HTTP 429 Your quota will reset after 2h0m0s.\", duration=10ms\n",
		int(ts.Month()), ts.Day(), ts.Hour(), ts.Minute(), ts.Second())
	p := filepath.Join(logDir, "acp-worker-"+now.Format("20060102_150405")+"-1.log")
	if err := os.WriteFile(p, []byte(fail), 0o600); err != nil {
		t.Fatal(err)
	}

	// Seed 1200 real-time calls so trip enter triggers auto-calibration (< 4800 and >= 500) and persists both!
	RecordMonitorToolDispatchAt("acc-p", 1200, now)
	scanMonitorTripAccount("acc-p", logDir, now)
	until1 := MonitorTripUntil("acc-p", now)
	if until1.IsZero() {
		t.Fatal("expected acc-p to be tripped")
	}

	// Verify monitor-trips.json exists and survives a simulated restart.
	if _, err := os.Stat(filepath.Join(stateDir, monitorTripsFileName)); err != nil {
		t.Fatalf("expected monitor-trips.json persisted: %v", err)
	}
	resetMonitorTripsForTest()
	if !MonitorTripUntil("acc-p", now).IsZero() {
		t.Fatal("expected clean state after reset")
	}
	LoadMonitorStateFromDir(stateDir)
	if until2 := MonitorTripUntil("acc-p", now); !until2.Equal(until1) {
		t.Fatalf("expected persisted trip restored across restart: got %v want %v", until2, until1)
	}
	snap := EvaluateMonitorBudget(AccountProfile{ID: "acc-p"}, now)
	if snap.HardCap != 1200 || snap.SoftCap != 825 || snap.DrainCap != 1063 {
		t.Fatalf("expected persisted calibrated caps 825/1063/1200 (balanced ratios), got %+v", snap)
	}

	// ClearMonitorTrip clears trip without re-reading old log lines.
	ClearMonitorTrip("acc-p")
	if !MonitorTripUntil("acc-p", now).IsZero() {
		t.Fatal("ClearMonitorTrip must clear active trip")
	}

	// PruneMonitorTripAccounts removes disabled/deleted accounts.
	setMonitorTripForTest("acc-disabled", now.Add(time.Hour))
	PruneMonitorTripAccounts(map[string]bool{"acc-p": true})
	if !MonitorTripUntil("acc-disabled", now).IsZero() {
		t.Fatal("PruneMonitorTripAccounts must remove disabled account trips")
	}
}

func TestMonitorTripScanDirIncremental(t *testing.T) {
	resetMonitorTripsForTest()
	t.Cleanup(resetMonitorTripsForTest)
	dir := t.TempDir()
	now := time.Now()
	ts := now.Add(-30 * time.Second)
	fail := fmt.Sprintf("I%02d%02d %02d:%02d:%02d.000000    1 runner.go:957] Monitor sir_flash_lite FINISHED: triggered=false, reason=\"Failed: ResourceExhausted HTTP 429 Your quota will reset after 1h10m0s.\", duration=1ms\n",
		int(ts.Month()), ts.Day(), ts.Hour(), ts.Minute(), ts.Second())
	name := "acp-worker-" + now.Format("20060102_150405") + "-1.log"
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte("noise line\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	scanMonitorTripAccount("acc-x", dir, now)
	if !MonitorTripUntil("acc-x", now).IsZero() {
		t.Fatal("no failure yet, must be healthy")
	}
	f, _ := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0o600)
	_, _ = f.WriteString(fail)
	_ = f.Close()
	scanMonitorTripAccount("acc-x", dir, now.Add(time.Second))
	until := MonitorTripUntil("acc-x", now.Add(time.Second))
	if until.IsZero() || until.Before(now.Add(time.Hour)) {
		t.Fatalf("expected tripped with ~1h10m reset, got %v", until)
	}
	snap := MonitorTripSnapshot(now.Add(time.Second))
	if _, ok := snap["acc-x"]; !ok {
		t.Fatalf("snapshot missing tripped account: %v", snap)
	}
	if !MonitorTripUntil("acc-x", until.Add(time.Second)).IsZero() {
		t.Fatal("trip must expire at reset time")
	}
}

func newTripRoutingManager(t *testing.T) *AccountManager {
	t.Helper()
	waitQuotaRefreshIdle(t)
	quotaMu.Lock()
	savedQ, savedQAt, savedQErr, savedQAttempt, savedQBackoff := cachedQuota, cachedQuotaAt, lastQuotaErr, lastQuotaAttemptAt, quotaBackoffSteps
	cachedQuota = nil
	cachedQuotaAt = time.Time{}
	lastQuotaErr = nil
	lastQuotaAttemptAt = time.Time{}
	quotaBackoffSteps = 0
	quotaMu.Unlock()
	resetTosBreakerForTest()
	resetMonitorTripsForTest()

	strictTrue := true
	am := &AccountManager{
		accounts: []AccountProfile{
			{ID: "a1", Label: "A1", Enabled: true, Priority: 1, MaxLiveWorkers: 4, SchedState: SchedStateActive, AllowedModels: []string{"gemini-4-neon"}},
			{ID: "a2", Label: "A2", Enabled: true, Priority: 2, MaxLiveWorkers: 4, SchedState: SchedStateActive, AllowedModels: []string{"gemini-4-neon"}},
			{ID: "ultra", Label: "Ultra", Enabled: true, Primary: true, Priority: 3, MaxLiveWorkers: 16, SchedState: SchedStateActive, AllowedModels: []string{"gemini-4-neon"}},
		},
		routing: AccountPoolConfig{
			Strategy:             RouteStrategyFillFirst,
			StickyTTLSec:         1800,
			StrictU8NoMidTurnHop: &strictTrue,
		},
		stickyLRU: newStickySessionLRU(),
		workers:   make(map[string]*accountWorkerProc),
	}
	prevDB := GatewayStateDB
	tmpDir := t.TempDir()
	am.baseDataDir = tmpDir
	t.Setenv("AGY_STATE_DB", filepath.Join(tmpDir, "gateway.db"))
	t.Setenv("AGY_TOS_TRIPPED_FILE", filepath.Join(tmpDir, "tos-tripped"))
	t.Setenv("AGY_PURE_API_HOME", filepath.Join(tmpDir, "primary-pure"))
	t.Cleanup(func() {
		waitQuotaRefreshIdle(t)
		quotaMu.Lock()
		cachedQuota, cachedQuotaAt, lastQuotaErr, lastQuotaAttemptAt, quotaBackoffSteps = savedQ, savedQAt, savedQErr, savedQAttempt, savedQBackoff
		quotaMu.Unlock()
		resetTosBreakerForTest()
		resetMonitorTripsForTest()
		initConfig()
		InitGlobalStore(prevDB)
	})
	initConfig()
	resetTosBreakerForTest()
	for _, id := range []string{"a1", "a2", "ultra"} {
		var tokPath string
		if id == "ultra" {
			tokPath = filepath.Join(PureApiHome, "antigravity-cli", "antigravity-oauth-token")
		} else {
			tokPath = filepath.Join(tmpDir, "accounts", id, ".gemini", "antigravity-cli", "antigravity-oauth-token")
		}
		_ = os.MkdirAll(filepath.Dir(tokPath), 0o700)
		_ = os.WriteFile(tokPath, []byte("dummy-oauth-token-fixture-32bytes"), 0o600)
	}
	resetMonitorTripsForTest()
	return am
}

func TestMonitorTripRoutingShiftsNewAndPinnedSessions(t *testing.T) {
	am := newTripRoutingManager(t)
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	now := time.Now()

	first := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:conv-1", HasTools: true})
	if first.DenyErr != nil || first.Account.ID != "a1" {
		t.Fatalf("expected fill-first a1, got %q err=%v", first.Account.ID, first.DenyErr)
	}
	// Seed a second sticky session on a1 BEFORE tripping so AGY_MONITOR_TRIP_HOP=0 tests a genuine sticky hit!
	pinSeed := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:conv-pin", HasTools: true})
	if pinSeed.DenyErr != nil || pinSeed.Account.ID != "a1" {
		t.Fatalf("expected conv-pin seeded on a1, got %q err=%v", pinSeed.Account.ID, pinSeed.DenyErr)
	}
	nonContSeed := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:conv-noncont", HasTools: true})
	if nonContSeed.DenyErr != nil || nonContSeed.Account.ID != "a1" {
		t.Fatalf("expected conv-noncont seeded on a1, got %q err=%v", nonContSeed.Account.ID, nonContSeed.DenyErr)
	}

	var hopCancelledAcc, hopCancelledSK string
	am.setMonitorTripHopCancelHookForTest(func(accountID, sessionKey string, _ []string, _ string) {
		hopCancelledAcc = accountID
		hopCancelledSK = sessionKey
	})

	setMonitorTripForTest("a1", now.Add(2*time.Hour))

	// New tool session skips the tripped account.
	n := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:conv-new", HasTools: true})
	if n.DenyErr != nil || n.Account.ID != "a2" {
		t.Fatalf("new session must avoid tripped a1 and land on a2, got %q err=%v", n.Account.ID, n.DenyErr)
	}

	// Pinned continuation is re-routed AND triggers old parked-session cancellation!
	c := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:conv-1", IsContinuation: true, HasTools: true})
	if c.DenyErr != nil || c.Account.ID == "a1" || c.IsStickyHit {
		t.Fatalf("pinned continuation must hop off tripped a1, got %q sticky=%v err=%v", c.Account.ID, c.IsStickyHit, c.DenyErr)
	}
	if hopCancelledAcc != "a1" || hopCancelledSK != "hdr:conv-1" {
		t.Fatalf("MONITOR_TRIP_HOP must cancel old parked session on a1, got acc=%q sk=%q", hopCancelledAcc, hopCancelledSK)
	}

	// Hop disabled -> continuation stays pinned (legacy U8 behaviour) with REAL sticky hit assertion!
	t.Setenv("AGY_MONITOR_TRIP_HOP", "0")
	initConfig()
	hop := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:conv-pin", IsContinuation: true, HasTools: true})
	if hop.DenyErr != nil || hop.Account.ID != "a1" || !hop.IsStickyHit {
		t.Fatalf("hop disabled must keep sticky pin on a1, got acc=%q sticky=%v err=%v", hop.Account.ID, hop.IsStickyHit, hop.DenyErr)
	}

	// Non-continuation sticky hit with AGY_MONITOR_TRIP_HOP=0 must NOT bypass pool-monitor-tripped (line 2193 guard).
	setMonitorTripForTest("a2", now.Add(90*time.Minute))
	setMonitorTripForTest("ultra", now.Add(3*time.Hour))
	nonCont := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:conv-noncont", IsContinuation: false, HasTools: true})
	if nonCont.DenyErr == nil || nonCont.StatusCode != http.StatusTooManyRequests || nonCont.RouteMode != "pool-monitor-tripped" {
		t.Fatalf("non-continuation sticky hit on tripped account must not bypass 429, got acc=%q mode=%q status=%d err=%v",
			nonCont.Account.ID, nonCont.RouteMode, nonCont.StatusCode, nonCont.DenyErr)
	}

	t.Setenv("AGY_MONITOR_TRIP_HOP", "1")
	initConfig()

	// All accounts tripped -> tool-bearing request returns 429 + earliest Retry-After.
	d := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:conv-all", HasTools: true})
	if d.DenyErr == nil || d.StatusCode != http.StatusTooManyRequests || d.RouteMode != "pool-monitor-tripped" {
		t.Fatalf("expected pool-monitor-tripped 429, got acc=%q status=%d mode=%q err=%v", d.Account.ID, d.StatusCode, d.RouteMode, d.DenyErr)
	}
	if d.RetryAfterSec < 89*60 || d.RetryAfterSec > 91*60 {
		t.Fatalf("Retry-After should be the earliest reset (~90m), got %ds", d.RetryAfterSec)
	}

	// Zero-tool request is EXEMPT even when ALL accounts are monitor-tripped!
	noTools := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:conv-text", HasTools: false})
	if noTools.DenyErr != nil {
		t.Fatalf("zero-tool request must NEVER be blocked by monitor trips, got err=%v", noTools.DenyErr)
	}

	// Explicit account routing honors MonitorTripUntil for tool-bearing requests, exempts zero-tool requests!
	expReq := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	expReq.Header.Set("X-Agy-Account", "a1")
	expTool := am.ResolveTargetAccountEx(expReq, "gemini-4-neon", ResolveOptions{HasTools: true})
	if expTool.DenyErr == nil || expTool.StatusCode != http.StatusTooManyRequests || !strings.HasSuffix(expTool.RouteMode, "-monitor-tripped") {
		t.Fatalf("explicit tool request on tripped account must return 429, got status=%d mode=%q err=%v", expTool.StatusCode, expTool.RouteMode, expTool.DenyErr)
	}
	if expTool.RetryAfterSec < 119*60 || expTool.RetryAfterSec > 121*60 {
		t.Fatalf("explicit tripped Retry-After must match a1's ~2h reset, got %ds", expTool.RetryAfterSec)
	}
	expText := am.ResolveTargetAccountEx(expReq, "gemini-4-neon", ResolveOptions{HasTools: false})
	if expText.DenyErr != nil || expText.Account.ID != "a1" {
		t.Fatalf("explicit zero-tool request on tripped a1 must be allowed, got acc=%q err=%v", expText.Account.ID, expText.DenyErr)
	}

	// Feature kill switch.
	t.Setenv("AGY_MONITOR_TRIP_ROUTING", "0")
	initConfig()
	off := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:conv-off", HasTools: true})
	if off.DenyErr != nil {
		t.Fatalf("routing disabled must ignore trips, got err=%v", off.DenyErr)
	}
}

func TestMonitorTripDenialPrecedenceAndManualLockedRetryAfter(t *testing.T) {
	am := newTripRoutingManager(t)
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	now := time.Now()

	// 1. Denial precedence: a1 & ultra monitor-tripped (2.5h), a2 in short 15s 529 cooldown -> must return pool-cooldown (15s)!
	setMonitorTripForTest("a1", now.Add(150*time.Minute))
	setMonitorTripForTest("ultra", now.Add(180*time.Minute))
	am.RecordAccountOutcome("a2", "gemini-4-neon", 529, "overloaded_error", "15", "busy")

	pick := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:prec-1", HasTools: true})
	if pick.DenyErr == nil || pick.RouteMode != "pool-cooldown" || pick.RetryAfterSec > 20 || pick.RetryAfterSec < 1 {
		t.Fatalf("expected pool-cooldown (~15s) to beat 2.5h monitor trip, got mode=%q retry=%ds err=%v", pick.RouteMode, pick.RetryAfterSec, pick.DenyErr)
	}

	// 2. Reverse case: a1 tripped for 3m (180s), a2 cooling for 10m (600s) -> monitor trip recovers sooner -> pool-monitor-tripped!
	am.ClearAccountCooldown("a2")
	setMonitorTripForTest("a1", now.Add(180*time.Second))
	setMonitorTripForTest("ultra", now.Add(180*time.Minute))
	am.RecordAccountOutcome("a2", "gemini-4-neon", http.StatusTooManyRequests, "rate_limit_error", "600", "429")

	pick2 := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:prec-2", HasTools: true})
	if pick2.DenyErr == nil || pick2.RouteMode != "pool-monitor-tripped" || pick2.RetryAfterSec > 185 || pick2.RetryAfterSec < 170 {
		t.Fatalf("expected shorter 180s monitor trip to beat 600s cooldown, got mode=%q retry=%ds err=%v", pick2.RouteMode, pick2.RetryAfterSec, pick2.DenyErr)
	}

	// 3. pool-manual-locked-monitor must use effectiveActiveID's own tripUntil (~3h), NOT pool-wide minTripRetrySec (~3m)!
	manualFalse := false
	am.mu.Lock()
	am.routing.Strategy = RouteStrategyManualActive
	am.routing.ActiveAccountID = "ultra"
	am.routing.ManualAutoFailover = &manualFalse
	am.mu.Unlock()
	pickManual := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:manual-lock", HasTools: true})
	if pickManual.DenyErr == nil || pickManual.RouteMode != "pool-manual-locked-monitor" {
		t.Fatalf("expected pool-manual-locked-monitor, got mode=%q err=%v", pickManual.RouteMode, pickManual.DenyErr)
	}
	if pickManual.RetryAfterSec < 178*60 || pickManual.RetryAfterSec > 181*60 {
		t.Fatalf("expected RetryAfterSec from ultra's own ~3h reset, got %ds", pickManual.RetryAfterSec)
	}
}

func TestMonitorTripHealthViewRotateNextAndAPIs(t *testing.T) {
	am := newTripRoutingManager(t)
	now := time.Now()
	setMonitorTripForTest("a1", now.Add(2*time.Hour))
	RecordMonitorToolDispatchAt("a2", 3400, now) // yellow zone (balanced soft=3300, drain=4250)

	// HealthView reports monitor_tripped for a1 and excludes it from healthy count; a2 stays healthy with yellow zone.
	hvs := am.SnapshotHealthViews(am.accounts, nil, now)
	if hvs["a1"].State != "monitor_tripped" || !hvs["a1"].MonitorTripped || hvs["a1"].MonitorZone != monitorZoneRed || hvs["a1"].MonitorTripUntil == "" {
		t.Fatalf("expected a1 health monitor_tripped/red, got %+v", hvs["a1"])
	}
	if hvs["a2"].State != "healthy" || hvs["a2"].MonitorTripped || hvs["a2"].MonitorZone != monitorZoneYellow || hvs["a2"].MonitorCalls5h != 3400 {
		t.Fatalf("expected a2 health healthy/yellow/3400, got %+v", hvs["a2"])
	}

	// rotate_next must skip monitor-tripped a1 and land on a2 (not a1!).
	am.filePath = filepath.Join(am.baseDataDir, "accounts.json")
	am.mu.Lock()
	am.routing.ActiveAccountID = "ultra"
	am.mu.Unlock()
	res, err := am.PerformAccountAction("ultra", "rotate_next", true)
	if err != nil {
		t.Fatalf("rotate_next failed: %v", err)
	}
	if res["activeAccountId"] != "a2" {
		t.Fatalf("rotate_next must skip tripped a1 and pick a2, got %+v", res)
	}

	// ClearAccountCooldown clears monitor trip cleanly.
	am.ClearAccountCooldown("a1")
	if !MonitorTripUntil("a1", now).IsZero() {
		t.Fatal("ClearAccountCooldown must clear a1 monitor trip")
	}
	hvsAfter := am.SnapshotHealthViews(am.accounts, nil, now)
	if hvsAfter["a1"].State != "healthy" || hvsAfter["a1"].MonitorTripped {
		t.Fatalf("expected a1 healthy after ClearAccountCooldown, got %+v", hvsAfter["a1"])
	}

	// Verify /api/accounts exposes both monitorTrips and monitorBudgets in poolStats, and health has monitor fields.
	prevAM := GlobalAccountManager
	GlobalAccountManager = am
	t.Cleanup(func() { GlobalAccountManager = prevAM })
	setMonitorTripForTest("a1", now.Add(2*time.Hour))

	rec := httptest.NewRecorder()
	HandleAccountsAPI(rec, httptest.NewRequest(http.MethodGet, "/api/accounts", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/api/accounts status=%d body=%s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode /api/accounts: %v", err)
	}
	ps, _ := body["poolStats"].(map[string]any)
	if ps["healthyAccounts"] != float64(2) {
		t.Fatalf("expected healthyAccounts=2 (a1 tripped excluded), got %v", ps["healthyAccounts"])
	}
	mt, _ := ps["monitorTrips"].(map[string]any)
	if _, ok := mt["a1"]; !ok {
		t.Fatalf("expected monitorTrips[a1] in /api/accounts poolStats, got %+v", mt)
	}
	mb, _ := ps["monitorBudgets"].(map[string]any)
	if _, ok := mb["a2"]; !ok {
		t.Fatalf("expected monitorBudgets[a2] in /api/accounts poolStats, got %+v", mb)
	}
}

func TestMonitorTripSinglePrimaryDegradesOpen(t *testing.T) {
	strictTrue := true
	am := &AccountManager{
		accounts: []AccountProfile{
			{ID: "solo", Label: "Solo", Enabled: true, Primary: true, Priority: 1, MaxLiveWorkers: 16, SchedState: SchedStateActive, AllowedModels: []string{"gemini-4-neon"}},
		},
		routing:   AccountPoolConfig{Strategy: RouteStrategyFillFirst, StrictU8NoMidTurnHop: &strictTrue},
		stickyLRU: newStickySessionLRU(),
		workers:   make(map[string]*accountWorkerProc),
	}
	t.Cleanup(resetMonitorTripsForTest)
	setMonitorTripForTest("solo", time.Now().Add(time.Hour))
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	r := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:s", HasTools: true})
	if r.DenyErr != nil || r.Account.ID != "solo" {
		t.Fatalf("single-primary deployments must never self-lockout, got %q err=%v", r.Account.ID, r.DenyErr)
	}
}

func TestMonitorWindowRestartPersistenceZeroFalseRedAndVersion1Compat(t *testing.T) {
	resetMonitorTripsForTest()
	t.Cleanup(resetMonitorTripsForTest)

	stateDir := t.TempDir()
	logDir := t.TempDir()
	LoadMonitorStateFromDir(stateDir)

	// Window reset occurred at T0 (10m ago); next reset is T0 + 5h.
	now := time.Now().Truncate(time.Second)
	winStart := now.Add(-10 * time.Minute)
	winReset := winStart.Add(5 * time.Hour)
	winKey := winReset.UTC().Format(time.RFC3339)

	// Populate acp-worker log with 4,000 pre-reset lines (30m..2h ago) + 100 post-reset lines (2m ago).
	var sb strings.Builder
	preTs := winStart.Add(-30 * time.Minute)
	preLine := fmt.Sprintf("I%02d%02d %02d:%02d:%02d.000000    1 runner.go:957] Monitor sir_flash_lite FINISHED: triggered=false, reason=\"\", duration=200ms\n",
		int(preTs.Month()), preTs.Day(), preTs.Hour(), preTs.Minute(), preTs.Second())
	for i := 0; i < 4000; i++ {
		sb.WriteString(preLine)
	}
	postTs := winStart.Add(2 * time.Minute)
	postLine := fmt.Sprintf("I%02d%02d %02d:%02d:%02d.000000    1 runner.go:957] Monitor sir_flash_lite FINISHED: triggered=false, reason=\"\", duration=200ms\n",
		int(postTs.Month()), postTs.Day(), postTs.Hour(), postTs.Minute(), postTs.Second())
	for i := 0; i < 100; i++ {
		sb.WriteString(postLine)
	}
	logPath := filepath.Join(logDir, "acp-worker-"+now.Format("20060102_150405")+"-1.log")
	if err := os.WriteFile(logPath, []byte(sb.String()), 0o600); err != nil {
		t.Fatal(err)
	}

	// Before restart: gateway knew the active 5h window (windowStartAt = winStart, logCalls5h = 100).
	SyncMonitorBudgetCalls("u-restart", 100, winKey, now)

	// Verify Version: 2 file contains persisted Windows entry.
	raw, err := os.ReadFile(filepath.Join(stateDir, monitorTripsFileName))
	if err != nil {
		t.Fatalf("read persisted monitor-trips.json: %v", err)
	}
	var persisted persistedMonitorStateFile
	if err := json.Unmarshal(raw, &persisted); err != nil {
		t.Fatalf("unmarshal monitor-trips.json: %v", err)
	}
	if persisted.Version != 2 {
		t.Fatalf("expected persisted Version=2, got %d", persisted.Version)
	}
	if persisted.Windows["u-restart"].WindowKey != winKey || persisted.Windows["u-restart"].LogCalls5h != 100 {
		t.Fatalf("expected persisted window entry for u-restart, got %+v", persisted.Windows["u-restart"])
	}

	// Simulate restart: wipe in-memory state, run LoadMonitorStateFromDir BEFORE initial log scan,
	// and verify at t=0s (before /usage runs!) that calls5h == 100 (Green, ZERO 6s false-red!).
	resetMonitorTripsForTest()
	LoadMonitorStateFromDir(stateDir)
	scanMonitorTripAccount("u-restart", logDir, now)

	snap := EvaluateMonitorBudget(AccountProfile{ID: "u-restart", PlanTierHint: "ultra"}, now)
	if snap.Calls5h != 100 || snap.Zone != monitorZoneGreen {
		t.Fatalf("expected calls5h=100 green at t=0s after restart (4000 pre-reset lines excluded), got %+v", snap)
	}

	// Also test restart when gateway was down across a window reset (persisted windowResetAt < now):
	// windowStartAt anchors at old windowResetAt so pre-reset lines are STILL excluded!
	stateDir2 := t.TempDir()
	prevWinReset := winStart
	oldState := persistedMonitorStateFile{
		Version:   2,
		UpdatedAt: prevWinReset.Add(-5 * time.Minute).UTC().Format(time.RFC3339),
		Windows: map[string]persistedMonitorWindowEntry{
			"u-down": {
				WindowKey:     prevWinReset.UTC().Format(time.RFC3339),
				WindowStartAt: prevWinReset.Add(-5 * time.Hour).UTC().Format(time.RFC3339),
				WindowResetAt: prevWinReset.UTC().Format(time.RFC3339),
				LogCalls5h:    4200,
			},
		},
	}
	b2, _ := json.Marshal(oldState)
	if err := os.WriteFile(filepath.Join(stateDir2, monitorTripsFileName), b2, 0o600); err != nil {
		t.Fatal(err)
	}
	resetMonitorTripsForTest()
	LoadMonitorStateFromDir(stateDir2)
	scanMonitorTripAccount("u-down", logDir, now)
	snapDown := EvaluateMonitorBudget(AccountProfile{ID: "u-down", PlanTierHint: "ultra"}, now)
	if snapDown.Calls5h != 100 || snapDown.Zone != monitorZoneGreen {
		t.Fatalf("expected rolled-over persisted window to yield calls5h=100 green, got %+v", snapDown)
	}

	// Backward-compatible load of Version 1 monitor-trips.json (no Windows field).
	stateDirV1 := t.TempDir()
	v1Until := now.Add(90 * time.Minute).UTC().Format(time.RFC3339Nano)
	v1Fail := now.Add(-10 * time.Minute).UTC().Format(time.RFC3339Nano)
	v1Raw := fmt.Sprintf(`{"version":1,"updatedAt":%q,"trips":{"u-v1":{"until":%q,"failAt":%q}},"calibratedCaps":{"u-v1":{"hardCap":2400,"softCap":1440,"drainCap":1920}}}`,
		now.UTC().Format(time.RFC3339), v1Until, v1Fail)
	if err := os.WriteFile(filepath.Join(stateDirV1, monitorTripsFileName), []byte(v1Raw), 0o600); err != nil {
		t.Fatal(err)
	}
	resetMonitorTripsForTest()
	LoadMonitorStateFromDir(stateDirV1)
	if MonitorTripUntil("u-v1", now).IsZero() {
		t.Fatal("Version 1 monitor-trips.json trip must load cleanly")
	}
	snapV1 := EvaluateMonitorBudget(AccountProfile{ID: "u-v1"}, now)
	if snapV1.HardCap != 2400 || snapV1.Zone != monitorZoneRed || !snapV1.Tripped {
		t.Fatalf("Version 1 calibratedCaps/trip mismatch: %+v", snapV1)
	}
}

func TestMonitorTripStartupScanCapturesExpired429LiteResetAt(t *testing.T) {
	resetMonitorTripsForTest()
	t.Cleanup(resetMonitorTripsForTest)

	logDir := t.TempDir()
	now := time.Now().Truncate(time.Second)
	// Expired 429 occurred 2h10m ago with reset after 2h0m0s -> liteResetAt = now - 10m (already expired!).
	failTs := now.Add(-2*time.Hour - 10*time.Minute)
	liteResetAt := failTs.Add(2 * time.Hour) // now - 10m

	var sb strings.Builder
	// 4,000 pre-reset lines 30m before liteResetAt
	preTs := liteResetAt.Add(-30 * time.Minute)
	preLine := fmt.Sprintf("I%02d%02d %02d:%02d:%02d.000000    1 runner.go:957] Monitor sir_flash_lite FINISHED: triggered=false, reason=\"\", duration=200ms\n",
		int(preTs.Month()), preTs.Day(), preTs.Hour(), preTs.Minute(), preTs.Second())
	for i := 0; i < 4000; i++ {
		sb.WriteString(preLine)
	}
	// Real 429 line (already expired at now)
	sb.WriteString(fmt.Sprintf("I%02d%02d %02d:%02d:%02d.000000    1 runner.go:957] Monitor sir_flash_lite FINISHED: triggered=false, reason=\"Failed: ResourceExhausted HTTP 429 Your quota will reset after 2h0m0s.\", duration=10ms\n",
		int(failTs.Month()), failTs.Day(), failTs.Hour(), failTs.Minute(), failTs.Second()))
	// 50 post-reset lines after liteResetAt
	postTs := liteResetAt.Add(3 * time.Minute)
	postLine := fmt.Sprintf("I%02d%02d %02d:%02d:%02d.000000    1 runner.go:957] Monitor sir_flash_lite FINISHED: triggered=false, reason=\"\", duration=200ms\n",
		int(postTs.Month()), postTs.Day(), postTs.Hour(), postTs.Minute(), postTs.Second())
	for i := 0; i < 50; i++ {
		sb.WriteString(postLine)
	}

	p := filepath.Join(logDir, "acp-worker-"+now.Format("20060102_150405")+"-1.log")
	if err := os.WriteFile(p, []byte(sb.String()), 0o600); err != nil {
		t.Fatal(err)
	}

	scanMonitorTripAccount("u-exp", logDir, now)
	if !MonitorTripUntil("u-exp", now).IsZero() {
		t.Fatal("expired 429 must not report active trip")
	}
	snap := EvaluateMonitorBudget(AccountProfile{ID: "u-exp", PlanTierHint: "ultra"}, now)
	if snap.Calls5h != 50 || snap.Zone != monitorZoneGreen {
		t.Fatalf("expired 429 liteResetAt must filter pre-reset calls and return 50/green, got %+v", snap)
	}
}
