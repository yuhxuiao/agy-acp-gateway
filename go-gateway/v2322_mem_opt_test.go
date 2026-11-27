package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type v2322CloseTrackEngine struct {
	*stubEngine
	mu      sync.Mutex
	closed  []string
	onClose func(sid string)
}

func newV2322CloseTrackEngine(onClose func(sid string)) *v2322CloseTrackEngine {
	base := &stubEngine{}
	base.alive.Store(true)
	return &v2322CloseTrackEngine{
		stubEngine: base,
		onClose:    onClose,
	}
}

func (e *v2322CloseTrackEngine) CloseSession(sid string) error {
	e.mu.Lock()
	e.closed = append(e.closed, sid)
	cb := e.onClose
	e.mu.Unlock()
	if cb != nil {
		cb(sid)
	}
	return nil
}

func v2322SeedSession(gs *GatewayServer, convKey, sid string, status string, lastUsed time.Time, ctxTok int) *AcpProjectSession {
	s := NewAcpProjectSession(convKey, "", "sys-"+convKey)
	s.Engine = gs.Engine
	s.Store = gs.Store
	s.SessionID = sid
	s.engineGen = 1
	s.MessageCount = 4
	s.TotalTurns = 2
	s.LastUsedAt = lastUsed
	s.CreatedAt = lastUsed.Add(-time.Minute)
	s.LastCachedReadTokens = ctxTok
	s.InitialSystemText = "init-sys-" + convKey
	s.InitialAppendPrompt = "init-append-" + convKey
	s.InitialToolsCatalog = []ToolDef{{Name: "Read", McpName: "ws_Read_abc"}}
	s.ToolsCatalog = []ToolDef{{Name: "Read", McpName: "ws_Read_abc"}}
	s.ToolFingerprint = "fp-" + convKey
	s.HistoryFingerprint = "hist-" + convKey
	switch status {
	case "BUSY":
		s.IsBusy = true
		s.PromptInFlight = true
	case "PARKED":
		s.PromptInFlight = true
		s.ParkedSince = lastUsed
		s.ParkedCalls["call-"+convKey] = &ParkedMcpCall{
			Call:       ToolCallItem{ID: "call-" + convKey, Name: "Read"},
			ResultChan: make(chan map[string]any, 1),
		}
	}
	s.mu.Lock()
	s.syncPhaseLocked()
	s.mu.Unlock()
	gs.mu.Lock()
	gs.sessions[convKey] = s
	gs.tenants[s.TenantKey] = s
	gs.mu.Unlock()
	return s
}

func TestV2322_GlobalCrossAccountLRUHibernationAndInternalEndpoints(t *testing.T) {
	origAM := GlobalAccountManager
	origGlobalCap := GlobalMaxLiveWorkers
	origGlobalBoot := globalMaxLiveWorkersBoot
	origMaxW := MaxLiveWorkers
	origMaxBoot := maxLiveWorkersBoot
	origDB := GatewayStateDB
	t.Cleanup(func() {
		if GlobalAccountManager != nil {
			GlobalAccountManager.StopAll()
		}
		GlobalAccountManager = origAM
		GlobalMaxLiveWorkers = origGlobalCap
		globalMaxLiveWorkersBoot = origGlobalBoot
		MaxLiveWorkers = origMaxW
		maxLiveWorkersBoot = origMaxBoot
		initConfig()
		InitGlobalStore(origDB)
	})

	tmp := t.TempDir()
	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "primary.db"))
	t.Setenv("AGY_GW_LOG", filepath.Join(tmp, "gateway.log"))
	t.Setenv("AGY_API_KEYS", "")
	t.Setenv("AGY_GLOBAL_MAX_LIVE_WORKERS", "4")
	t.Setenv("AGY_MAX_LIVE_WORKERS", "8")
	t.Setenv("AGY_CHILD_ACCOUNT_WORKER", "")
	initConfig()

	// Verify per-instance cap is clamped to <= GlobalMaxLiveWorkers when global cap > 0.
	if got := effectiveMaxLiveWorkers(); got != 4 {
		t.Fatalf("expected effectiveMaxLiveWorkers clamped to GlobalMaxLiveWorkers=4, got %d", got)
	}
	// Verify auto-computed global cap formula.
	if got := autoGlobalMaxLiveWorkers(0); got != 8 {
		t.Fatalf("autoGlobalMaxLiveWorkers(0) = %d, want 8", got)
	}
	if got := autoGlobalMaxLiveWorkers(1500); got != 4 {
		t.Fatalf("autoGlobalMaxLiveWorkers(1500) = %d, want 4", got)
	}
	if got := autoGlobalMaxLiveWorkers(3200); got != 10 {
		t.Fatalf("autoGlobalMaxLiveWorkers(3200) = %d, want 10", got)
	}
	if got := autoGlobalMaxLiveWorkers(6144); got != 12 {
		t.Fatalf("autoGlobalMaxLiveWorkers(6144) = %d, want 12", got)
	}

	// Primary & child GatewayServers backed by mock ACP engines.
	primEng := newV2322CloseTrackEngine(nil)
	primGW := NewGatewayServerWithDeps(primEng, &StateStore{})
	defer primGW.Close()

	childEng := newV2322CloseTrackEngine(nil)
	childGW := NewGatewayServerWithDeps(childEng, &StateStore{})
	defer childGW.Close()

	childSrv := httptest.NewServer(childGW.DashboardOnlyHandler())
	defer childSrv.Close()
	parsedChild, err := url.Parse(childSrv.URL)
	if err != nil {
		t.Fatal(err)
	}
	childPort, err := strconv.Atoi(parsedChild.Port())
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	// Primary sessions: 1 BUSY, 1 fresh IDLE (20s), 1 old IDLE (200s), 1 PARKED (90s) -> 4 live
	pBusy := v2322SeedSession(primGW, "sid:p-busy", "00000000-0000-4000-8000-000000000101", "BUSY", now.Add(-5*time.Second), 20000)
	pIdleFresh := v2322SeedSession(primGW, "sid:p-idle-20s", "00000000-0000-4000-8000-000000000102", "IDLE", now.Add(-20*time.Second), 0)
	pIdleOld := v2322SeedSession(primGW, "sid:p-idle-200s", "00000000-0000-4000-8000-000000000103", "IDLE", now.Add(-200*time.Second), 0)
	pParked := v2322SeedSession(primGW, "sid:p-parked-90s", "00000000-0000-4000-8000-000000000104", "PARKED", now.Add(-90*time.Second), 10000)

	// Child sessions: 1 BUSY, 1 very old IDLE (400s), 1 medium IDLE (120s) -> 3 live
	cBusy := v2322SeedSession(childGW, "sid:c-busy", "00000000-0000-4000-8000-000000000201", "BUSY", now.Add(-5*time.Second), 30000)
	cIdleOldest := v2322SeedSession(childGW, "sid:c-idle-400s", "00000000-0000-4000-8000-000000000202", "IDLE", now.Add(-400*time.Second), 0)
	cIdleMed := v2322SeedSession(childGW, "sid:c-idle-120s", "00000000-0000-4000-8000-000000000203", "IDLE", now.Add(-120*time.Second), 0)

	// Verify GET /api/internal/worker-slots rejects non-loopback requests.
	reqNonLoop := httptest.NewRequest(http.MethodGet, "/api/internal/worker-slots", nil)
	reqNonLoop.Host = "127.0.0.1"
	reqNonLoop.RemoteAddr = "203.0.113.10:55555"
	rrNonLoop := httptest.NewRecorder()
	childGW.DashboardOnlyHandler().ServeHTTP(rrNonLoop, reqNonLoop)
	if rrNonLoop.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for non-loopback /api/internal/worker-slots, got %d", rrNonLoop.Code)
	}
	reqHibNonLoop := httptest.NewRequest(http.MethodPost, "/api/internal/hibernate-lru", strings.NewReader(`{"count":1}`))
	reqHibNonLoop.Host = "127.0.0.1"
	reqHibNonLoop.RemoteAddr = "203.0.113.10:55555"
	rrHibNonLoop := httptest.NewRecorder()
	childGW.DashboardOnlyHandler().ServeHTTP(rrHibNonLoop, reqHibNonLoop)
	if rrHibNonLoop.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for non-loopback /api/internal/hibernate-lru, got %d", rrHibNonLoop.Code)
	}

	// Verify loopback GET /api/internal/worker-slots returns accurate counts and candidates (no BUSY).
	respSlots, err := http.Get(childSrv.URL + "/api/internal/worker-slots")
	if err != nil {
		t.Fatalf("GET /api/internal/worker-slots failed: %v", err)
	}
	var childSlots WorkerSlotsSnapshot
	if err := json.NewDecoder(respSlots.Body).Decode(&childSlots); err != nil {
		t.Fatalf("decode worker-slots: %v", err)
	}
	_ = respSlots.Body.Close()
	if childSlots.LiveWorkers != 3 || childSlots.BusyWorkers != 1 || childSlots.IdleWorkers != 2 || childSlots.LogicalSessions != 3 {
		t.Fatalf("unexpected child worker-slots: %+v", childSlots)
	}
	if len(childSlots.Candidates) != 2 || childSlots.Candidates[0].ConvKey != "sid:c-idle-400s" || childSlots.Candidates[1].ConvKey != "sid:c-idle-120s" {
		t.Fatalf("expected child candidates ordered oldest-first without BUSY, got %+v", childSlots.Candidates)
	}

	// Disable real cgroup pressure for this cap-only test.
	restoreMem := setCgroupMemoryReaderForTest(func() CgroupMemoryStatus {
		return CgroupMemoryStatus{
			Version:         2,
			CurrentBytes:    1800 * 1024 * 1024,
			LimitBytes:      6144 * 1024 * 1024,
			AnonBytes:       1500 * 1024 * 1024,
			FileBytes:       300 * 1024 * 1024,
			WorkingSetBytes: 1600 * 1024 * 1024,
			UsagePct:        26.0,
			HasLiveUsage:    true,
		}
	})
	defer restoreMem()

	am := NewAccountManager()
	am.accounts = []AccountProfile{
		{ID: "primary", Label: "Primary", Primary: true, Enabled: true, MaxLiveWorkers: 8},
		{ID: "child-a", Label: "Child A", Primary: false, Enabled: true, MaxLiveWorkers: 8},
	}
	am.enabledAccountCount.Store(2)
	am.workers["child-a"] = &accountWorkerProc{id: "child-a", dashPort: childPort}
	am.setPrimaryGatewayForTest(primGW)
	GlobalAccountManager = am

	// Total live = 7, GlobalMaxLiveWorkers = 4 -> must evict 3 globally oldest IDLE sessions:
	// 1) child-a sid:c-idle-400s, 2) primary sid:p-idle-200s, 3) child-a sid:c-idle-120s.
	res := am.runGlobalMemoryGovernorTick(now)
	if res.TotalLiveWorkersBefore != 7 || res.TotalLiveWorkersAfter != 4 || res.HibernatedByCap != 3 {
		t.Fatalf("unexpected governor cap result: %+v", res)
	}

	checkLive := func(name string, s *AcpProjectSession, wantLive bool) {
		t.Helper()
		s.mu.Lock()
		sid := s.SessionID
		hibSid := s.HibernatedSessionID
		initSys := s.InitialSystemText
		initApp := s.InitialAppendPrompt
		histFp := s.HistoryFingerprint
		s.mu.Unlock()
		if wantLive && sid == "" {
			t.Fatalf("%s expected LIVE, got hibernated (hibSid=%q)", name, hibSid)
		}
		if !wantLive {
			if sid != "" || hibSid == "" {
				t.Fatalf("%s expected HIBERNATED with preserved HibernatedSessionID, got sid=%q hibSid=%q", name, sid, hibSid)
			}
			if initSys == "" || initApp == "" || histFp == "" {
				t.Fatalf("%s lost lossless resume metadata after hibernation", name)
			}
		}
	}

	checkLive("pBusy", pBusy, true)
	checkLive("cBusy", cBusy, true)
	checkLive("pIdleFresh", pIdleFresh, true)
	checkLive("pParked", pParked, true)
	checkLive("cIdleOldest", cIdleOldest, false)
	checkLive("pIdleOld", pIdleOld, false)
	checkLive("cIdleMed", cIdleMed, false)

	// Pre-spawn admission check when already at GlobalMaxLiveWorkers=4: evicts remaining IDLE (pIdleFresh) to make room!
	am.TryPreSpawnGlobalCapCheck("sid:incoming-new")
	checkLive("pIdleFresh after pre-spawn", pIdleFresh, false)
	checkLive("pBusy after pre-spawn", pBusy, true)
	checkLive("cBusy after pre-spawn", cBusy, true)

	// Background governor start/stop lifecycle must exit cleanly without hanging.
	am.startGlobalMemoryGovernorWithInterval(15 * time.Millisecond)
	time.Sleep(40 * time.Millisecond)
	stopped := make(chan struct{})
	go func() {
		am.stopGlobalMemoryGovernor()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("stopGlobalMemoryGovernor hung")
	}
}

func TestV2322_CgroupMemoryParserAndPressureGuard(t *testing.T) {
	origAM := GlobalAccountManager
	origGlobalCap := GlobalMaxLiveWorkers
	origGlobalBoot := globalMaxLiveWorkersBoot
	origWarn := MemPressureWarnPct
	origWarnBoot := memPressureWarnPctBoot
	origHigh := MemPressureHighPct
	origHighBoot := memPressureHighPctBoot
	origDB := GatewayStateDB
	t.Cleanup(func() {
		if GlobalAccountManager != nil {
			GlobalAccountManager.StopAll()
		}
		GlobalAccountManager = origAM
		GlobalMaxLiveWorkers = origGlobalCap
		globalMaxLiveWorkersBoot = origGlobalBoot
		MemPressureWarnPct = origWarn
		memPressureWarnPctBoot = origWarnBoot
		MemPressureHighPct = origHigh
		memPressureHighPctBoot = origHighBoot
		initConfig()
		InitGlobalStore(origDB)
	})

	// 1. Cgroup v2 memory.stat parser: workingSet = current - inactive_file (ignores reclaimable page cache)
	const gib = int64(1024 * 1024 * 1024)
	v2Stat := fmt.Sprintf("anon %d\nfile %d\nactive_file %d\ninactive_file %d\n",
		3500*1024*1024, 1500*1024*1024, 200*1024*1024, 1300*1024*1024)
	stV2 := parseCgroupMemoryStatus(
		strconv.FormatInt(5000*1024*1024, 10)+"\n",
		strconv.FormatInt(6*gib, 10)+"\n",
		v2Stat, "", "", "", "",
	)
	if stV2.Version != 2 || !stV2.HasLiveUsage {
		t.Fatalf("expected cgroup v2 live usage, got %+v", stV2)
	}
	wantWS := int64(5000-1300) * 1024 * 1024
	if stV2.WorkingSetBytes != wantWS || stV2.AnonBytes != 3500*1024*1024 || stV2.FileBytes != 1500*1024*1024 || stV2.LimitBytes != 6*gib {
		t.Fatalf("unexpected cgroup v2 parsed fields: %+v (wantWS=%d)", stV2, wantWS)
	}
	if stV2.UsagePct >= 70.0 || stV2.UsagePct < 55.0 {
		t.Fatalf("expected working-set UsagePct ~60.2%% (<70%% despite 81%% raw current), got %.2f%%", stV2.UsagePct)
	}

	// 2. Cgroup v2 without inactive_file falls back to anon + active_file
	stAnonFallback := parseCgroupMemoryStatus(
		strconv.FormatInt(4*gib, 10),
		strconv.FormatInt(6*gib, 10),
		fmt.Sprintf("anon %d\nactive_file %d\n", 3*gib, 512*1024*1024),
		"", "", "", "",
	)
	if stAnonFallback.WorkingSetBytes != 3*gib+512*1024*1024 {
		t.Fatalf("expected anon+active_file fallback, got %d", stAnonFallback.WorkingSetBytes)
	}

	// 3. Cgroup v1 fallback when v2 is absent
	stV1 := parseCgroupMemoryStatus(
		"", "max\n", "",
		strconv.FormatInt(3*gib, 10),
		strconv.FormatInt(4*gib, 10),
		fmt.Sprintf("total_rss %d\ntotal_cache %d\ntotal_inactive_file %d\n", 2*gib, gib, 512*1024*1024),
		"",
	)
	if stV1.Version != 1 || !stV1.HasLiveUsage || stV1.WorkingSetBytes != 3*gib-512*1024*1024 || stV1.AnonBytes != 2*gib {
		t.Fatalf("unexpected cgroup v1 parsed status: %+v", stV1)
	}

	// 4. Non-cgroup / macOS empty fallback
	stEmpty := parseCgroupMemoryStatus("", "", "", "", "", "", "")
	if stEmpty.HasLiveUsage || stEmpty.WorkingSetBytes != 0 || stEmpty.LimitBytes != 0 {
		t.Fatalf("expected safe zero fallback on non-cgroup host, got %+v", stEmpty)
	}

	tmp := t.TempDir()
	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "pressure.db"))
	t.Setenv("AGY_GW_LOG", filepath.Join(tmp, "pressure.log"))
	t.Setenv("AGY_GLOBAL_MAX_LIVE_WORKERS", "12")
	t.Setenv("AGY_MEM_PRESSURE_WARN_PCT", "70")
	t.Setenv("AGY_MEM_PRESSURE_HIGH_PCT", "80")
	initConfig()

	// 5. 70% WARN pressure: proactively hibernates IDLE >= 60s only (never IDLE <60s, PARKED, or BUSY)
	var memPct atomic.Int64 // stored as basis points (e.g. 7300 = 73.00%)
	memPct.Store(7300)
	restoreMem := setCgroupMemoryReaderForTest(func() CgroupMemoryStatus {
		pct := float64(memPct.Load()) / 100.0
		lim := int64(6 * gib)
		ws := int64(float64(lim) * pct / 100.0)
		return CgroupMemoryStatus{
			Version:         2,
			CurrentBytes:    ws + 200*1024*1024,
			LimitBytes:      lim,
			AnonBytes:       ws,
			FileBytes:       200 * 1024 * 1024,
			WorkingSetBytes: ws,
			UsagePct:        pct,
			HasLiveUsage:    true,
		}
	})
	defer restoreMem()

	eng := newV2322CloseTrackEngine(func(string) {
		// Each evicted worker simulates freeing ~5% of container working set.
		memPct.Add(-500)
	})
	gs := NewGatewayServerWithDeps(eng, &StateStore{})
	defer gs.Close()

	now := time.Now()
	sWarnOldIdle := v2322SeedSession(gs, "sid:warn-idle-90s", "00000000-0000-4000-8000-000000000301", "IDLE", now.Add(-90*time.Second), 0)
	sWarnYoungIdle := v2322SeedSession(gs, "sid:warn-idle-30s", "00000000-0000-4000-8000-000000000302", "IDLE", now.Add(-30*time.Second), 0)
	sWarnParked := v2322SeedSession(gs, "sid:warn-parked-90s", "00000000-0000-4000-8000-000000000303", "PARKED", now.Add(-90*time.Second), 0)
	sWarnBusy := v2322SeedSession(gs, "sid:warn-busy", "00000000-0000-4000-8000-000000000304", "BUSY", now.Add(-120*time.Second), 0)

	am := NewAccountManager()
	am.accounts = []AccountProfile{{ID: "primary", Label: "Primary", Primary: true, Enabled: true, MaxLiveWorkers: 12}}
	am.enabledAccountCount.Store(1)
	am.setPrimaryGatewayForTest(gs)
	GlobalAccountManager = am

	resWarn := am.runGlobalMemoryGovernorTick(now)
	if resWarn.HibernatedByMemWarn != 1 || resWarn.HibernatedByMemHigh != 0 {
		t.Fatalf("expected 1 warn hibernation at 73%%, got %+v", resWarn)
	}
	if sWarnOldIdle.SessionID != "" || sWarnOldIdle.HibernatedSessionID == "" {
		t.Fatalf("expected idle-90s to be hibernated with resume ID preserved")
	}
	if sWarnYoungIdle.SessionID == "" || sWarnParked.SessionID == "" || sWarnBusy.SessionID == "" {
		t.Fatalf("expected idle-30s, parked-90s, and busy to stay live under 73%% warn")
	}

	// 6. 80% HIGH pressure (84% -> drops 5% per close -> stops as soon as <75%):
	// Should evict IDLE >= 15s (sWarnYoungIdle at 30s + new idle-50s) and STOP before touching PARKED(90s)!
	sHighIdle50 := v2322SeedSession(gs, "sid:high-idle-50s", "00000000-0000-4000-8000-000000000305", "IDLE", now.Add(-50*time.Second), 0)
	sHighIdle10 := v2322SeedSession(gs, "sid:high-idle-10s", "00000000-0000-4000-8000-000000000306", "IDLE", now.Add(-10*time.Second), 0)
	sHighParked20 := v2322SeedSession(gs, "sid:high-parked-20s", "00000000-0000-4000-8000-000000000307", "PARKED", now.Add(-20*time.Second), 0)

	memPct.Store(8400) // 84% -> -5% = 79% -> -5% = 74% (<75%, stop!)
	resHigh1 := am.runGlobalMemoryGovernorTick(now)
	if resHigh1.HibernatedByMemHigh != 2 {
		t.Fatalf("expected exactly 2 high-pressure evictions (84%% -> 74%%), got %+v", resHigh1)
	}
	if sHighIdle50.SessionID != "" || sWarnYoungIdle.SessionID != "" {
		t.Fatalf("expected idle-50s and idle-30s evicted first under 84%% pressure")
	}
	if sWarnParked.SessionID == "" || sHighIdle10.SessionID == "" || sHighParked20.SessionID == "" || sWarnBusy.SessionID == "" {
		t.Fatalf("expected stop before evicting parked-90s / idle-10s / parked-20s / busy once <75%%")
	}

	// 7. 88% HIGH pressure with no IDLE >= 15s left -> escalates to PARKED >= 45s (sWarnParked),
	// preserving HibernatedParkedCallIDs & never evicting BUSY / parked-20s / idle-10s!
	memPct.Store(8800)
	resHigh2 := am.runGlobalMemoryGovernorTick(now)
	if resHigh2.HibernatedByMemHigh != 1 {
		t.Fatalf("expected 1 parked>=45s eviction at 88%%, got %+v", resHigh2)
	}
	if sWarnParked.SessionID != "" || sWarnParked.HibernatedSessionID == "" || !sWarnParked.HibernatedParkedCallIDs["call-sid:warn-parked-90s"] {
		t.Fatalf("expected parked-90s hibernated with HibernatedSessionID & HibernatedParkedCallIDs preserved, got sid=%q hib=%q parked=%v",
			sWarnParked.SessionID, sWarnParked.HibernatedSessionID, sWarnParked.HibernatedParkedCallIDs)
	}
	if sWarnBusy.SessionID == "" || sHighIdle10.SessionID == "" || sHighParked20.SessionID == "" {
		t.Fatalf("BUSY, idle-10s, and parked-20s must NEVER be evicted")
	}

	// 8. Verify /api/dashboard additive fields in both gateway & poolRouting.poolStats.
	dash := BuildFederatedDashboardPayload(gs, false)
	gwMap, _ := dash["gateway"].(map[string]any)
	for _, field := range []string{
		"globalLiveWorkers", "globalMaxLiveWorkers", "globalLogicalSessions", "globalHibernatedSessions",
		"cgroupWorkingSetBytes", "cgroupAnonBytes", "cgroupFileBytes", "cgroupLimitBytes",
	} {
		if _, ok := gwMap[field]; !ok {
			t.Fatalf("missing additive gateway field %q in /api/dashboard", field)
		}
	}
	pr, _ := dash["poolRouting"].(map[string]any)
	ps, _ := pr["poolStats"].(map[string]any)
	for _, field := range []string{
		"globalLiveWorkers", "globalMaxLiveWorkers", "globalLogicalSessions", "globalHibernatedSessions",
		"cgroupWorkingSetBytes", "cgroupAnonBytes", "cgroupFileBytes", "cgroupLimitBytes",
	} {
		if _, ok := ps[field]; !ok {
			t.Fatalf("missing additive poolRouting.poolStats field %q in /api/dashboard", field)
		}
	}
	if mapInt64(gwMap, "cgroupLimitBytes") != 6*gib {
		t.Fatalf("expected cgroupLimitBytes=%d, got %d", 6*gib, mapInt64(gwMap, "cgroupLimitBytes"))
	}
}

func TestV2322_HeapSlimming_ImageCacheDashboardStoreSessionAndPreReadBody(t *testing.T) {
	origDB := GatewayStateDB
	origImgMB := ImageCacheMaxMB
	origImgBoot := imageCacheMaxMBBoot
	origIdleTTL := dashCacheIdleTTL
	t.Cleanup(func() {
		dashStepsCacheMu.Lock()
		dashCacheIdleTTL = origIdleTTL
		dashStepsCacheMu.Unlock()
		ImageCacheMaxMB = origImgMB
		imageCacheMaxMBBoot = origImgBoot
		initConfig()
		InitGlobalStore(origDB)
	})

	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "memopt.db")
	t.Setenv("AGY_STATE_DB", dbPath)
	t.Setenv("AGY_GW_LOG", filepath.Join(tmp, "memopt.log"))
	t.Setenv("AGY_APP_CONV_DIR", "off")
	t.Setenv("AGY_IDLE_WORKER_TTL_SEC", "")
	t.Setenv("AGY_IMAGE_CACHE_MAX_MB", "")
	initConfig()

	// 1. Verify new default AGY_IDLE_WORKER_TTL_SEC = 180s and AGY_IMAGE_CACHE_MAX_MB = 16 MB.
	if IdleWorkerTtl != 180*time.Second {
		t.Fatalf("expected default IdleWorkerTtl=180s, got %v", IdleWorkerTtl)
	}
	if got := effectiveImageCacheMaxBytes(); got != 16*1024*1024 {
		t.Fatalf("expected default image cache cap 16 MiB, got %d", got)
	}

	// 2. Test configurable AGY_IMAGE_CACHE_MAX_MB eviction (1 MiB cap) & 0 disables cache.
	t.Setenv("AGY_IMAGE_CACHE_MAX_MB", "1")
	initConfig()
	if got := effectiveImageCacheMaxBytes(); got != 1*1024*1024 {
		t.Fatalf("expected 1 MiB image cache cap, got %d", got)
	}
	fetcher := newRemoteImageFetcher(nil)
	now := time.Now()
	makeItem := func(tag string, payloadLen int) cachedRemoteImage {
		return cachedRemoteImage{
			mime:     "image/png",
			b64:      strings.Repeat(tag, payloadLen/2),
			rawBytes: payloadLen / 2,
		}
	}
	// Each item costs 400 KiB (<= 512 KiB single-item limit); inserting 3 (1200 KiB > 1024 KiB) evicts k1!
	fetcher.putCached("k1", makeItem("a", 400*1024), now)
	fetcher.putCached("k2", makeItem("b", 400*1024), now.Add(time.Second))
	fetcher.putCached("k3", makeItem("c", 400*1024), now.Add(2*time.Second))
	if _, ok := fetcher.getCached("k1", now.Add(3*time.Second)); ok {
		t.Fatal("expected oldest image cache entry k1 evicted under 1 MiB cap")
	}
	if _, ok := fetcher.getCached("k2", now.Add(3*time.Second)); !ok {
		t.Fatal("expected k2 retained under 1 MiB cap")
	}
	if _, ok := fetcher.getCached("k3", now.Add(3*time.Second)); !ok {
		t.Fatal("expected k3 retained under 1 MiB cap")
	}
	if fetcher.cacheBytes > 1*1024*1024 {
		t.Fatalf("image cacheBytes %d exceeded 1 MiB cap", fetcher.cacheBytes)
	}

	t.Setenv("AGY_IMAGE_CACHE_MAX_MB", "0")
	initConfig()
	fetcherZero := newRemoteImageFetcher(nil)
	fetcherZero.putCached("k0", makeItem("z", 1024), now)
	if len(fetcherZero.cache) != 0 {
		t.Fatalf("expected AGY_IMAGE_CACHE_MAX_MB=0 to disable caching, got %d entries", len(fetcherZero.cache))
	}

	// 3. Test store.go readDB PRAGMA cache_size = -2000.
	st := &StateStore{}
	st.Init(filepath.Join(tmp, "read_pragma.db"))
	defer st.Close()
	if st.readDB == nil {
		t.Fatal("expected readDB initialized")
	}
	var cacheSize int
	if err := st.readDB.QueryRow("PRAGMA cache_size").Scan(&cacheSize); err != nil {
		t.Fatalf("PRAGMA cache_size query failed: %v", err)
	}
	if cacheSize != -2000 {
		t.Fatalf("expected readDB PRAGMA cache_size=-2000, got %d", cacheSize)
	}

	// 4. Test dashboard idle cache eviction (5m TTL) and transparent rebuild.
	nowSec := time.Now().Unix()
	step := ParsedModelStep{
		SessionID: "00000000-0000-4000-8000-000000004001",
		StepIdx:   1, TsSec: nowSec - 60,
		ModelEnum: 1319, ModelID: "gemini-3.8-flash",
		RawIn: 5000, RawCr: 45000, OutTok: 300, IsDshBridge: true,
	}
	if _, err := GlobalStore.persistUsageBatch(step.SessionID, cursorMeta{lastStepIdx: 1}, []ParsedModelStep{step}); err != nil {
		t.Fatal(err)
	}
	gs := NewGatewayServerWithDeps(newV2322CloseTrackEngine(nil), GlobalStore)
	defer gs.Close()

	p1 := BuildDashboardPayload(gs, false)
	dashStepsCacheMu.Lock()
	populated := len(dashCachedAnnotated) > 0
	lastAcc := dashCacheLastAccess
	dashStepsCacheMu.Unlock()
	if !populated {
		t.Fatal("expected dashCachedAnnotated populated after BuildDashboardPayload")
	}
	// Before 5m TTL: must NOT evict.
	if EvictStaleDashboardCache(lastAcc.Add(2 * time.Minute)) {
		t.Fatal("expected dashboard cache retained before 5m idle TTL")
	}
	// After 5m TTL: must evict all cached step/view slices.
	if !EvictStaleDashboardCache(lastAcc.Add(6 * time.Minute)) {
		t.Fatal("expected dashboard cache evicted after 5m idle TTL")
	}
	dashStepsCacheMu.Lock()
	cleared := dashCachedAnnotated == nil && dashCachedRecent == nil && dashViewWindows == nil
	dashStepsCacheMu.Unlock()
	if !cleared {
		t.Fatal("expected dashCachedAnnotated, dashCachedRecent, and dashViewWindows nil after idle eviction")
	}
	// Transparent rebuild on next BuildDashboardPayload returns matching allTime stepsCount.
	p2 := BuildDashboardPayload(gs, false)
	w1, _ := p1["windows"].(map[string]WindowAggregate)
	w2, _ := p2["windows"].(map[string]WindowAggregate)
	if w1["allTime"].StepsCount != w2["allTime"].StepsCount || w2["allTime"].StepsCount < 1 {
		t.Fatalf("expected transparent rebuild with matching allTime stepsCount: before=%d after=%d", w1["allTime"].StepsCount, w2["allTime"].StepsCount)
	}

	// 5. Test HibernateWorker trims transient working memory while keeping lossless resume fields.
	s := v2322SeedSession(gs, "sid:trim-test", "00000000-0000-4000-8000-000000004002", "IDLE", time.Now().Add(-300*time.Second), 50000)
	s.RawSystemText = strings.Repeat("RAW-SYSTEM-PROMPT-", 500)
	s.RecentToolSigs = []string{"sig1", "sig2", "sig3"}
	s.cachedInitToolsJSON = `[{"name":"Read"}]`
	s.cachedCurToolsJSON = `[{"name":"Read"}]`
	s.lastCompletedReqFp = "req-fp-1"
	s.lastCompletedResult = TurnResult{Text: strings.Repeat("huge-response-", 200)}
	s.TurnHistory = make([]TurnHistoryEntry, 40)
	for i := range s.TurnHistory {
		s.TurnHistory[i] = TurnHistoryEntry{TurnIndex: i + 1, SessionID: s.SessionID, Mode: "sticky_delta"}
	}
	if !s.HibernateWorker("idle_300s", false) {
		t.Fatal("expected HibernateWorker to succeed")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.RawSystemText != "" || s.RecentToolSigs != nil || s.cachedInitToolsJSON != "" || s.cachedCurToolsJSON != "" || s.lastCompletedResult.Text != "" {
		t.Fatalf("expected transient memory trimmed on hibernate: rawSys=%d sigs=%d initJSON=%d curJSON=%d lastRes=%d",
			len(s.RawSystemText), len(s.RecentToolSigs), len(s.cachedInitToolsJSON), len(s.cachedCurToolsJSON), len(s.lastCompletedResult.Text))
	}
	if len(s.TurnHistory) != hibernatedTurnHistoryMax || s.TurnHistory[0].TurnIndex != 40-hibernatedTurnHistoryMax+1 || s.TurnHistory[len(s.TurnHistory)-1].TurnIndex != 40 {
		t.Fatalf("expected TurnHistory capped to latest %d entries (25..40), got len=%d first=%d last=%d",
			hibernatedTurnHistoryMax, len(s.TurnHistory), s.TurnHistory[0].TurnIndex, s.TurnHistory[len(s.TurnHistory)-1].TurnIndex)
	}
	if s.HibernatedSessionID != "00000000-0000-4000-8000-000000004002" ||
		s.InitialSystemText != "init-sys-sid:trim-test" ||
		s.InitialAppendPrompt != "init-append-sid:trim-test" ||
		len(s.InitialToolsCatalog) != 1 ||
		s.ToolFingerprint != "fp-sid:trim-test" ||
		s.HistoryFingerprint != "hist-sid:trim-test" {
		t.Fatal("expected lossless session/resume metadata preserved verbatim after hibernate")
	}

	// 6. Test clearPreReadBody + releasableBytesReader release payload references cleanly.
	payload := bytes.Repeat([]byte("x"), 1024)
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", newReleasableBytesReader(payload))
	req = withPreReadBody(req, payload)
	if got, ok := preReadBodyFromContext(req); !ok || len(got) != 1024 {
		t.Fatalf("expected preReadBodyFromContext hit, got ok=%v len=%d", ok, len(got))
	}
	clearPreReadBody(req)
	if _, ok := preReadBodyFromContext(req); ok {
		t.Fatal("expected preReadBodyFromContext empty after clearPreReadBody")
	}
	if req.Body != http.NoBody || req.GetBody != nil {
		t.Fatal("expected req.Body=http.NoBody and req.GetBody=nil after clearPreReadBody")
	}
	rb := newReleasableBytesReader(payload)
	_ = rb.Close()
	buf := make([]byte, 8)
	if n, err := rb.Read(buf); n != 0 || err != io.EOF {
		t.Fatalf("expected EOF after releasableBytesReader.Close(), got n=%d err=%v", n, err)
	}
}

func TestDetectAgySemverNoPoisonOnFailureAndPathFallback(t *testing.T) {
	tmp := t.TempDir()
	verDir := filepath.Join(tmp, "1.5.16-8996aedf")
	if err := os.MkdirAll(verDir, 0o755); err != nil {
		t.Fatal(err)
	}
	failBin := filepath.Join(verDir, "agy-acp-900k")
	if err := os.WriteFile(failBin, []byte("#!/bin/sh\nexit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	linkPath := filepath.Join(tmp, "current-agy")
	if err := os.Symlink(failBin, linkPath); err != nil {
		t.Fatal(err)
	}
	// When --version fails/times out, detectAgySemver infers 1.5.16 from the versioned directory
	// and does NOT permanently poison semverCache with "1.5.10".
	if got := detectAgySemver(linkPath); got != "1.5.16" {
		t.Fatalf("expected directory-inferred semver 1.5.16 on --version failure, got %q", got)
	}

	// Non-versioned failing binary must fall back to "1.5.10" WITHOUT caching the failure.
	plainFail := filepath.Join(tmp, "plain-agy")
	if err := os.WriteFile(plainFail, []byte("#!/bin/sh\nexit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if got := detectAgySemver(plainFail); got != "1.5.10" {
		t.Fatalf("expected fallback 1.5.10 on unversioned failing binary, got %q", got)
	}
	semverCacheMu.Lock()
	for k, v := range semverCache {
		if strings.Contains(k, plainFail) && v == "1.5.10" {
			semverCacheMu.Unlock()
			t.Fatalf("failed detectAgySemver must not poison semverCache (key=%q val=%q)", k, v)
		}
	}
	semverCacheMu.Unlock()
}
