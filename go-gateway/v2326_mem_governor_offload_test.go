package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gwcfg "agy-acp-gateway-go/internal/config"
	gwstore "agy-acp-gateway-go/internal/store"
)

func v2326FindEnvValues(env []string, key string) []string {
	prefix := key + "="
	var out []string
	for _, kv := range env {
		if strings.HasPrefix(kv, prefix) {
			out = append(out, kv[len(prefix):])
		}
	}
	return out
}

func TestV2326_GlibcMallocEnvInjectionAndSafeDefaults(t *testing.T) {
	t.Setenv("AGY_CHILD_GOMEMLIMIT", "")
	t.Setenv("AGY_CHILD_GOGC", "")
	t.Setenv("AGY_CHILD_GATEWAY_GOMEMLIMIT", "")
	t.Setenv("GOGC", "")
	t.Setenv("AGY_IMAGE_CACHE_MAX_MB", "")

	snap := gwcfg.LoadFromEnv()
	if snap.ChildGoMemLimit != "150MiB" {
		t.Fatalf("ChildGoMemLimit = %q, want 150MiB", snap.ChildGoMemLimit)
	}
	if snap.ChildGoGC != "25" {
		t.Fatalf("ChildGoGC = %q, want 25", snap.ChildGoGC)
	}
	if snap.ChildGatewayGoMemLimit != "80MiB" {
		t.Fatalf("ChildGatewayGoMemLimit = %q, want 80MiB", snap.ChildGatewayGoMemLimit)
	}

	// 1. Default glibc trio injection with zero duplicates.
	baseEnv := []string{
		"PATH=/usr/bin",
		"GOGC=100",
		"GOMEMLIMIT=512MiB",
		"AGY_API_KEYS=sk-top-secret",
	}
	childEnv := buildAgySubprocessEnv(baseEnv, "/tmp/agy-home", "", "25", "150MiB", "")
	for k, want := range map[string]string{
		"MALLOC_ARENA_MAX":       "2",
		"MALLOC_TRIM_THRESHOLD_": "65536",
		"MALLOC_MMAP_THRESHOLD_": "65536",
		"GOGC":                   "25",
		"GOMEMLIMIT":             "150MiB",
	} {
		vals := v2326FindEnvValues(childEnv, k)
		if len(vals) != 1 || vals[0] != want {
			t.Fatalf("env %s = %v, want single [%s]", k, vals, want)
		}
	}
	if vals := v2326FindEnvValues(childEnv, "AGY_API_KEYS"); len(vals) != 0 {
		t.Fatalf("expected AGY_API_KEYS stripped from leaf agy subprocess, got %v", vals)
	}

	// 2. Explicit parent overrides for glibc trio must be honored without duplicate keys.
	overrideBase := []string{
		"PATH=/usr/bin",
		"MALLOC_ARENA_MAX=4",
		"MALLOC_TRIM_THRESHOLD_=131072",
		"MALLOC_MMAP_THRESHOLD_=262144",
	}
	overEnv := buildAgySubprocessEnv(overrideBase, "/tmp/agy-home", "", "25", "150MiB", "")
	for k, want := range map[string]string{
		"MALLOC_ARENA_MAX":       "4",
		"MALLOC_TRIM_THRESHOLD_": "131072",
		"MALLOC_MMAP_THRESHOLD_": "262144",
	} {
		vals := v2326FindEnvValues(overEnv, k)
		if len(vals) != 1 || vals[0] != want {
			t.Fatalf("override env %s = %v, want single [%s]", k, vals, want)
		}
	}

	// 3. buildAccountWorkerEnvEx passes thin child defaults (GOGC=25, AGY_IMAGE_CACHE_MAX_MB=4,
	//    GOMEMLIMIT=80MiB even when childMaxWorkers=16, plus dual-tier global vars & parent port).
	acc := AccountProfile{ID: "ultra-2", Label: "Ultra 2", Enabled: true, MaxLiveWorkers: 16}
	workerEnv := buildAccountWorkerEnvEx(acc, filepath.Join(t.TempDir(), "u2"), "", "80MiB", 28902, 28903, 16, 16, 8, 18, 18891, false)
	for k, want := range map[string]string{
		"GOGC":                              "25",
		"GOMEMLIMIT":                        "80MiB",
		"AGY_IMAGE_CACHE_MAX_MB":            "4",
		"AGY_MAX_LIVE_WORKERS":              "16",
		"AGY_GLOBAL_IDLE_TARGET":            "8",
		"AGY_GLOBAL_MAX_LIVE_WORKERS":       "8",
		"AGY_GLOBAL_BURST_MAX_LIVE_WORKERS": "18",
		"AGY_PARENT_DASH_PORT":              "18891",
	} {
		vals := v2326FindEnvValues(workerEnv, k)
		if len(vals) != 1 || vals[0] != want {
			t.Fatalf("worker env %s = %v, want single [%s]", k, vals, want)
		}
	}

	// Host explicit AGY_IMAGE_CACHE_MAX_MB must not be overwritten by default 4.
	t.Setenv("AGY_IMAGE_CACHE_MAX_MB", "12")
	workerEnvHost := buildAccountWorkerEnvEx(acc, filepath.Join(t.TempDir(), "u2b"), "", "80MiB", 28904, 28905, 12, 12, 8, 18, 18891, false)
	if vals := v2326FindEnvValues(workerEnvHost, "AGY_IMAGE_CACHE_MAX_MB"); len(vals) != 1 || vals[0] != "12" {
		t.Fatalf("expected host AGY_IMAGE_CACHE_MAX_MB=12 preserved, got %v", vals)
	}
}

func TestV2326_QuotaLiteDoesNotTouchDashboardCacheAndThinChildSlimming(t *testing.T) {
	origDB := GatewayStateDB
	origIdleTTL := dashCacheIdleTTL
	t.Cleanup(func() {
		dashStepsCacheMu.Lock()
		dashCacheIdleTTL = origIdleTTL
		dashStepsCacheMu.Unlock()
		resetAgyMonitorCacheForTest()
		resetPeerDashboardCacheForTest()
		initConfig()
		InitGlobalStore(origDB)
	})

	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "child.db")
	t.Setenv("AGY_CHILD_ACCOUNT_WORKER", "1")
	t.Setenv("AGY_STATE_DB", dbPath)
	t.Setenv("AGY_GW_LOG", filepath.Join(tmp, "child.log"))
	t.Setenv("AGY_APP_CONV_DIR", "off")
	t.Setenv("AGY_API_KEYS", "")
	initConfig()

	// 1. Child SQLite PRAGMA cache_size = -500 for write DB, read DB, and roDB!
	st := &StateStore{}
	st.Init(dbPath)
	defer st.Close()
	var writeCache, readCache, roCache int
	if err := st.db.QueryRow("PRAGMA cache_size").Scan(&writeCache); err != nil || writeCache != -500 {
		t.Fatalf("child writeDB PRAGMA cache_size = %d (err=%v), want -500", writeCache, err)
	}
	if err := st.readDB.QueryRow("PRAGMA cache_size").Scan(&readCache); err != nil || readCache != -500 {
		t.Fatalf("child readDB PRAGMA cache_size = %d (err=%v), want -500", readCache, err)
	}
	roDB, err := gwstore.OpenReadOnlySQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenReadOnlySQLite failed: %v", err)
	}
	defer roDB.Close()
	if err := roDB.QueryRow("PRAGMA cache_size").Scan(&roCache); err != nil || roCache != -500 {
		t.Fatalf("child roDB PRAGMA cache_size = %d (err=%v), want -500", roCache, err)
	}

	// 2. Seed dashboard cache + raw monitor events, then verify GET /api/internal/quota-lite
	//    NEVER refreshes dashCacheLastAccess and rejects non-loopback requests.
	nowSec := time.Now().Unix()
	step := ParsedModelStep{
		SessionID: "00000000-0000-4000-8000-000000009001",
		StepIdx:   1, TsSec: nowSec - 30,
		ModelEnum: 1319, ModelID: "gemini-3.8-flash",
		RawIn: 2000, RawCr: 18000, OutTok: 150, IsDshBridge: true,
	}
	if _, err := GlobalStore.persistUsageBatch(step.SessionID, cursorMeta{lastStepIdx: 1}, []ParsedModelStep{step}); err != nil {
		t.Fatal(err)
	}
	gs := NewGatewayServerWithDeps(newV2322CloseTrackEngine(nil), GlobalStore)
	defer gs.Close()

	// Seed a cached official quota so FetchOfficialQuota(false) returns immediately without spawning agy.
	quotaMu.Lock()
	origCachedQuota := cachedQuota
	origCachedQuotaAt := cachedQuotaAt
	cachedQuota = &QuotaSnapshot{
		Status:    "ok",
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
		PlanTier:  "ultra",
		Groups: []QuotaGroup{
			{Name: "Gemini Models", Buckets: []QuotaBucket{{Window: "5h", RemainingFraction: 0.85}}},
		},
	}
	cachedQuotaAt = time.Now()
	quotaMu.Unlock()
	t.Cleanup(func() {
		quotaMu.Lock()
		cachedQuota = origCachedQuota
		cachedQuotaAt = origCachedQuotaAt
		quotaMu.Unlock()
	})

	_ = BuildDashboardPayload(gs, false)
	oldAccess := time.Now().Add(-20 * time.Second)
	dashStepsCacheMu.Lock()
	dashCacheLastAccess = oldAccess
	dashStepsCacheMu.Unlock()

	agyMonitorCacheMu.Lock()
	agyMonitorCachedEvents = []agyMonitorEvent{{TsSec: nowSec - 10, Monitor: "sir_flash_lite", CostUsd: 0.01}}
	agyMonitorFilesByPath["/tmp/fake.log"] = &agyMonitorFileCache{events: append([]agyMonitorEvent(nil), agyMonitorCachedEvents...)}
	agyMonitorCacheMu.Unlock()

	// Non-loopback /api/internal/quota-lite -> 403
	reqNonLoop := httptest.NewRequest(http.MethodGet, "/api/internal/quota-lite", nil)
	reqNonLoop.RemoteAddr = "198.51.100.2:44321"
	rrNonLoop := httptest.NewRecorder()
	if !gs.HandleInternalV2326Routes(rrNonLoop, reqNonLoop) || rrNonLoop.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for non-loopback quota-lite, got code=%d", rrNonLoop.Code)
	}

	// Loopback server exposing HandleInternalV2326Routes + DashboardOnlyHandler.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if gs.HandleInternalV2326Routes(w, r) {
			return
		}
		gs.DashboardOnlyHandler().ServeHTTP(w, r)
	}))
	defer srv.Close()

	q, reachable, errMsg := fetchPeerQuotaLite(gwcfg.DashboardPeerConfig{ID: "child-1", BaseURL: srv.URL}, false, "")
	if !reachable || errMsg != "" || q == nil || q.Status != "ok" || q.PlanTier != "ultra" || len(q.Groups) != 1 {
		t.Fatalf("unexpected fetchPeerQuotaLite result: reach=%v err=%q q=%+v", reachable, errMsg, q)
	}
	dashStepsCacheMu.Lock()
	afterAccess := dashCacheLastAccess
	dashStepsCacheMu.Unlock()
	if !afterAccess.Equal(oldAccess) {
		t.Fatalf("GET /api/internal/quota-lite unexpectedly modified dashCacheLastAccess: before=%v after=%v", oldAccess, afterAccess)
	}

	// 3. Child worker EvictStaleDashboardCache expires at 30s (not 5m) and also evicts agyMonitorCachedEvents.
	if EvictStaleDashboardCache(oldAccess.Add(25 * time.Second)) {
		t.Fatal("expected child dashboard cache retained before 30s TTL")
	}
	if !EvictStaleDashboardCache(oldAccess.Add(35 * time.Second)) {
		t.Fatal("expected child dashboard cache evicted at 35s (>30s child TTL)")
	}
	agyMonitorCacheMu.Lock()
	rawMonLen := len(agyMonitorCachedEvents)
	rawFilesLen := len(agyMonitorFilesByPath)
	agyMonitorCacheMu.Unlock()
	if rawMonLen != 0 || rawFilesLen != 0 {
		t.Fatalf("expected agyMonitorCachedEvents & files cleared on stale eviction, got events=%d files=%d", rawMonLen, rawFilesLen)
	}

	// 4. Child worker reapIdleWorkers evicts persisted hibernated sessions (>10m) from gs.sessions,
	//    and GlobalStore.RestoreSingleSession lazily recovers them cleanly.
	sHib := NewAcpProjectSession("sid:child-lazy-1", "/tmp/work", "sys-child")
	sHib.Engine = gs.Engine
	sHib.Store = GlobalStore
	sHib.HibernatedSessionID = "00000000-0000-4000-8000-000000009099"
	sHib.MessageCount = 4
	sHib.LastUsedAt = time.Now().Add(-12 * time.Minute)
	sHib.CreatedAt = time.Now().Add(-20 * time.Minute)
	sHib.InitialSystemText = "sys-child"
	sHib.InitialAppendPrompt = "append-child"
	sHib.HistoryFingerprint = "hist-child-1"
	sHib.persistSession()
	GlobalStore.Flush()

	gs.mu.Lock()
	gs.sessions[sHib.ConvKey] = sHib
	gs.tenants[sHib.TenantKey] = sHib
	gs.mu.Unlock()

	gs.reapIdleWorkers()
	gs.mu.RLock()
	_, stillInMap := gs.sessions[sHib.ConvKey]
	gs.mu.RUnlock()
	if stillInMap {
		t.Fatal("expected persisted hibernated session >10m evicted from child gs.sessions")
	}
	restored := GlobalStore.RestoreSingleSession(sHib.ConvKey)
	if restored == nil || restored.HibernatedSessionID != sHib.HibernatedSessionID {
		t.Fatalf("expected RestoreSingleSession to recover evicted session, got %+v", restored)
	}
}

func TestV2326_DualTierBurstAdmission25ConcurrentAndWatermarkTieredLRU(t *testing.T) {
	origAM := GlobalAccountManager
	origIdle := GlobalIdleTarget
	origIdleBoot := globalIdleTargetBoot
	origBurst := GlobalBurstMaxLiveWorkers
	origBurstBoot := globalBurstMaxLiveWorkersBoot
	origGlobal := GlobalMaxLiveWorkers
	origGlobalBoot := globalMaxLiveWorkersBoot
	origMaxW := MaxLiveWorkers
	origMaxBoot := maxLiveWorkersBoot
	origPoll := globalAdmitPollInterval
	origWait := globalAdmitWaitTimeout
	origDB := GatewayStateDB
	t.Cleanup(func() {
		if GlobalAccountManager != nil {
			GlobalAccountManager.StopAll()
		}
		GlobalAccountManager = origAM
		GlobalIdleTarget = origIdle
		globalIdleTargetBoot = origIdleBoot
		GlobalBurstMaxLiveWorkers = origBurst
		globalBurstMaxLiveWorkersBoot = origBurstBoot
		GlobalMaxLiveWorkers = origGlobal
		globalMaxLiveWorkersBoot = origGlobalBoot
		MaxLiveWorkers = origMaxW
		maxLiveWorkersBoot = origMaxBoot
		globalAdmitPollInterval = origPoll
		globalAdmitWaitTimeout = origWait
		initConfig()
		InitGlobalStore(origDB)
	})

	// 1. Verify autoGlobalBurstMaxLiveWorkers(6144, 8) == 18 and single-account burst unlock.
	if got := autoGlobalBurstMaxLiveWorkers(6144, 8); got != 18 {
		t.Fatalf("autoGlobalBurstMaxLiveWorkers(6144, 8) = %d, want 18", got)
	}
	if got := autoGlobalBurstMaxLiveWorkers(2048, 8); got != 8 {
		t.Fatalf("autoGlobalBurstMaxLiveWorkers(2048, 8) = %d, want >= idleTarget(8)", got)
	}

	tmp := t.TempDir()
	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "admit.db"))
	t.Setenv("AGY_GW_LOG", filepath.Join(tmp, "admit.log"))
	t.Setenv("AGY_API_KEYS", "")
	t.Setenv("AGY_CHILD_ACCOUNT_WORKER", "")
	t.Setenv("AGY_GLOBAL_IDLE_TARGET", "8")
	t.Setenv("AGY_GLOBAL_BURST_MAX_LIVE_WORKERS", "18")
	t.Setenv("AGY_MAX_LIVE_WORKERS", "16")
	initConfig()

	if got := effectiveMaxLiveWorkers(); got != 16 {
		t.Fatalf("expected primary Ultra 16 burst slots unlocked when IdleTarget=8 BurstMax=18, got %d", got)
	}

	// Pin healthy cgroup memory (<60%) for concurrency stress test.
	restoreMem := setCgroupMemoryReaderForTest(func() CgroupMemoryStatus {
		return CgroupMemoryStatus{
			Version:         2,
			CurrentBytes:    2000 * 1024 * 1024,
			LimitBytes:      6144 * 1024 * 1024,
			WorkingSetBytes: 1800 * 1024 * 1024,
			UsagePct:        29.3,
			HasLiveUsage:    true,
		}
	})
	defer restoreMem()

	var distinctActive atomic.Int32
	eng := newV2322CloseTrackEngine(func(string) {
		distinctActive.Add(-1)
	})
	gs := NewGatewayServerWithDeps(eng, &StateStore{})
	defer gs.Close()

	am := NewAccountManager()
	am.accounts = []AccountProfile{
		{ID: "primary", Label: "Ultra Primary", Primary: true, Enabled: true, MaxLiveWorkers: 16},
		{ID: "child-1", Label: "Child 1", Primary: false, Enabled: true, MaxLiveWorkers: 12},
	}
	am.enabledAccountCount.Store(2)
	am.setPrimaryGatewayForTest(gs)
	GlobalAccountManager = am

	// Verify /api/accounts poolStats exposes dual-tier metrics while keeping globalMaxLiveWorkers compatible.
	recAcc := httptest.NewRecorder()
	HandleAccountsAPI(recAcc, httptest.NewRequest(http.MethodGet, "/api/accounts", nil))
	var accBody struct {
		PoolStats map[string]any `json:"poolStats"`
	}
	if err := json.Unmarshal(recAcc.Body.Bytes(), &accBody); err != nil {
		t.Fatalf("decode /api/accounts: %v", err)
	}
	if accBody.PoolStats["globalIdleTarget"] != float64(8) ||
		accBody.PoolStats["globalBurstMaxLiveWorkers"] != float64(18) ||
		accBody.PoolStats["globalMaxLiveWorkers"] != float64(8) {
		t.Fatalf("unexpected dual-tier poolStats: %+v", accBody.PoolStats)
	}

	// 2. 25-way concurrent burst admission stress test under -race (BurstMax = 18):
	//    Mix direct AcquireGlobalWorkerSlot calls and loopback POST /api/internal/global-admit + global-release.
	globalAdmitPollInterval = 10 * time.Millisecond
	globalAdmitWaitTimeout = 5 * time.Second

	admitSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if gs.HandleInternalV2326Routes(w, r) {
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer admitSrv.Close()
	parsedAdmit, _ := url.Parse(admitSrv.URL)
	admitPort, _ := strconv.Atoi(parsedAdmit.Port())

	var peakLive atomic.Int32
	var peakDistinct atomic.Int32
	var succeeded atomic.Int32
	var wg sync.WaitGroup
	const totalRequests = 25

	recordPeaks := func() {
		liveNow := int32(gs.countLiveWorkers())
		for {
			prev := peakLive.Load()
			if liveNow <= prev || peakLive.CompareAndSwap(prev, liveNow) {
				break
			}
		}
		distNow := distinctActive.Load()
		for {
			prev := peakDistinct.Load()
			if distNow <= prev || peakDistinct.CompareAndSwap(prev, distNow) {
				break
			}
		}
	}

	for i := 0; i < totalRequests; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			convKey := fmt.Sprintf("sid:burst-%02d", idx)
			sid := fmt.Sprintf("00000000-0000-4000-8000-00000000%04d", idx+1)
			var leaseID string
			var err error
			if idx%3 == 0 {
				leaseID, err = acquireGlobalWorkerSlotFromParent(context.Background(), admitPort, "", "child-1", convKey, "normal")
			} else {
				leaseID, err = am.AcquireGlobalWorkerSlot(context.Background(), "primary", "normal")
			}
			if err != nil {
				t.Errorf("request %d failed admission: %v", idx, err)
				return
			}
			if leaseID == "" {
				t.Errorf("request %d expected non-empty leaseID", idx)
				return
			}
			distinctActive.Add(1)
			recordPeaks()
			// Transition into a live BUSY session, then release spawn lease (mirroring reserveWorkerSlot).
			s := v2322SeedSession(gs, convKey, sid, "BUSY", time.Now(), 8000)
			if idx%3 == 0 {
				releaseGlobalWorkerSlotToParent(admitPort, "", leaseID)
			} else {
				am.ReleaseGlobalWorkerSlot(leaseID)
			}
			recordPeaks()

			// Hold BUSY briefly, then turn IDLE so queued callers can synchronously evict & wake cleanly.
			time.Sleep(time.Duration(25+(idx%5)*10) * time.Millisecond)
			s.mu.Lock()
			s.IsBusy = false
			s.PromptInFlight = false
			s.LastUsedAt = time.Now().Add(-30 * time.Second)
			s.syncPhaseLocked()
			s.mu.Unlock()
			am.NotifyGlobalWorkerSlotFreed()
			succeeded.Add(1)
		}(i)
	}
	wg.Wait()

	if succeeded.Load() != totalRequests {
		t.Fatalf("expected all %d burst requests to succeed, got %d", totalRequests, succeeded.Load())
	}
	if peak := peakLive.Load(); peak > 18 {
		t.Fatalf("concurrency burst exceeded physical live BurstMax=18: observed peak live=%d", peak)
	}
	if peak := peakDistinct.Load(); peak > 18 {
		t.Fatalf("concurrency burst exceeded live+unconverted-leases BurstMax=18: observed peak=%d", peak)
	}

	// 3. Bounded FIFO timeout test when all 18 slots are BUSY -> returns 529 cleanly without deadlock.
	gs2 := NewGatewayServerWithDeps(newV2322CloseTrackEngine(nil), &StateStore{})
	defer gs2.Close()
	am.setPrimaryGatewayForTest(gs2)
	for i := 0; i < 18; i++ {
		v2322SeedSession(gs2, fmt.Sprintf("sid:allbusy-%02d", i), fmt.Sprintf("00000000-0000-4000-8000-00000001%04d", i+1), "BUSY", time.Now(), 10000)
	}
	globalAdmitWaitTimeout = 80 * time.Millisecond
	_, errBusy := am.AcquireGlobalWorkerSlot(context.Background(), "primary", "normal")
	if errBusy == nil {
		t.Fatal("expected 529 error when all 18 burst slots remain BUSY")
	}
	if se, ok := errBusy.(*SoftError); !ok || se.StatusCode != 529 {
		t.Fatalf("expected SoftError 529, got %T %v", errBusy, errBusy)
	}

	// 4. Watermark-driven Tiered LRU verification:
	//    (a) Comfortable zone (live <= 8, cgroup < 60%): 0 evictions.
	gs3 := NewGatewayServerWithDeps(newV2322CloseTrackEngine(nil), &StateStore{})
	defer gs3.Close()
	am.setPrimaryGatewayForTest(gs3)
	now := time.Now()
	sMainLarge := v2322SeedSession(gs3, "sid:t4-main-120k", "00000000-0000-4000-8000-000000020001", "IDLE", now.Add(-120*time.Second), 120000)
	sCarbonMed := v2322SeedSession(gs3, "sid:t3-carbon-60k", "00000000-0000-4000-8000-000000020002", "IDLE", now.Add(-90*time.Second), 60000)
	sCarbonMed.ModelID = "gemini-4-carbon"
	sFlashMed := v2322SeedSession(gs3, "sid:t3-flash-60k", "00000000-0000-4000-8000-000000020003", "IDLE", now.Add(-90*time.Second), 60000)
	sFlashMed.ModelID = "gemini-3.8-flash"
	sSmallIdle := v2322SeedSession(gs3, "sid:t1-small-10k", "00000000-0000-4000-8000-000000020004", "IDLE", now.Add(-60*time.Second), 10000)
	sUtil := v2322SeedSession(gs3, "util:t0-one-shot", "00000000-0000-4000-8000-000000020005", "IDLE", now.Add(-10*time.Second), 5000)
	sParkedBash := v2322SeedSession(gs3, "sid:t2-parked-bash", "00000000-0000-4000-8000-000000020006", "PARKED", now.Add(-20*time.Second), 15000)
	sParkedBash.ParkedCalls["call-sid:t2-parked-bash"].Call.Name = "Bash"

	resComf := am.runGlobalMemoryGovernorTick(now)
	if resComf.HibernatedTotal != 0 {
		t.Fatalf("comfortable zone (live=6 <= IdleTarget=8, cgroup=29%%) must not evict, got %+v", resComf)
	}

	//    (b) Over IdleTarget (add 5 BUSY so live=11, excess=3): must evict T0 (sUtil), T1 (sSmallIdle), T2 (sParkedBash)
	//        while protecting T3 Carbon/Flash and T4 >=100K main session!
	for i := 0; i < 5; i++ {
		v2322SeedSession(gs3, fmt.Sprintf("sid:tier-busy-%d", i), fmt.Sprintf("00000000-0000-4000-8000-0000000201%02d", i), "BUSY", now.Add(-5*time.Second), 10000)
	}
	resExcess := am.runGlobalMemoryGovernorTick(now)
	if resExcess.HibernatedTotal != 3 || resExcess.HibernatedByElasticParked != 1 {
		t.Fatalf("expected exactly 3 tiered evictions (T0+T1+T2 elastic parked), got %+v", resExcess)
	}
	if sUtil.SessionID != "" || sSmallIdle.SessionID != "" || sParkedBash.SessionID != "" {
		t.Fatalf("expected T0/T1/T2 evicted: util=%q small=%q bash=%q", sUtil.SessionID, sSmallIdle.SessionID, sParkedBash.SessionID)
	}
	if sFlashMed.SessionID == "" || sCarbonMed.SessionID == "" || sMainLarge.SessionID == "" {
		t.Fatalf("expected T3 & T4 preserved after exact excess=3 eviction: flash=%q carbon=%q main=%q", sFlashMed.SessionID, sCarbonMed.SessionID, sMainLarge.SessionID)
	}
	//    (c) Add 1 more BUSY (live=9, excess=1): T3 Flash must be evicted BEFORE T3 Carbon (4x protection) and T4 Main!
	v2322SeedSession(gs3, "sid:tier-busy-extra", "00000000-0000-4000-8000-000000020199", "BUSY", now.Add(-5*time.Second), 10000)
	resT3 := am.runGlobalMemoryGovernorTick(now)
	if resT3.HibernatedTotal != 1 || sFlashMed.SessionID != "" || sCarbonMed.SessionID == "" || sMainLarge.SessionID == "" {
		t.Fatalf("expected T3 Flash evicted before Carbon 4x & Main 120K: res=%+v flash=%q carbon=%q main=%q", resT3, sFlashMed.SessionID, sCarbonMed.SessionID, sMainLarge.SessionID)
	}
}

type v2326OrderCheckEngine struct {
	*stubEngine
	mu              sync.Mutex
	events          []string
	watchChans      []chan map[string]any
	sawPrematureMsg bool
}

func (e *v2326OrderCheckEngine) checkPrematureLocked() {
	for _, ch := range e.watchChans {
		if len(ch) > 0 {
			e.sawPrematureMsg = true
		}
	}
}

func (e *v2326OrderCheckEngine) CancelSession(sid string) {
	e.mu.Lock()
	e.checkPrematureLocked()
	e.events = append(e.events, "cancel:"+sid)
	e.mu.Unlock()
	e.stubEngine.CancelSession(sid)
}

func (e *v2326OrderCheckEngine) CloseSession(sid string) error {
	e.mu.Lock()
	e.checkPrematureLocked()
	e.events = append(e.events, "close:"+sid)
	e.mu.Unlock()
	return e.stubEngine.CloseSession(sid)
}

func TestV2326_ElasticParkedOffloadOrderMetaPersistenceAndGeminiRebind(t *testing.T) {
	origDB := GatewayStateDB
	t.Cleanup(func() {
		initConfig()
		InitGlobalStore(origDB)
	})

	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "offload.db")
	t.Setenv("AGY_STATE_DB", dbPath)
	t.Setenv("AGY_GW_LOG", filepath.Join(tmp, "offload.log"))
	initConfig()

	ch1 := make(chan map[string]any, 1)
	ch2 := make(chan map[string]any, 1)
	ch3 := make(chan map[string]any, 1)

	base := &stubEngine{}
	base.alive.Store(true)
	ordEng := &v2326OrderCheckEngine{
		stubEngine: base,
		watchChans: []chan map[string]any{ch1, ch2, ch3},
	}

	sid := "00000000-0000-4000-8000-000000007777"
	s := NewAcpProjectSession("sid:offload-multi", "/tmp/proj", "sys-offload")
	s.Engine = ordEng
	s.Store = GlobalStore
	s.SessionID = sid
	s.engineGen = 1
	s.MessageCount = 4
	s.TotalTurns = 2
	s.PromptInFlight = true
	s.ParkedSince = time.Now().Add(-30 * time.Second)
	s.LastUsedAt = time.Now().Add(-30 * time.Second)
	s.InitialSystemText = "sys-offload"
	s.InitialAppendPrompt = "append-offload"
	s.HistoryFingerprint = "hist-offload"
	s.LastDeliveredCallIDs = []string{"acp_call_001", "acp_call_002"}
	s.ParkedCalls["acp_call_001"] = &ParkedMcpCall{
		Call:       ToolCallItem{ID: "acp_call_001", Name: "Read", Input: map[string]any{"file_path": "go-gateway/config.go"}},
		ResultChan: ch1,
		ParkedAt:   s.ParkedSince,
	}
	s.ParkedCalls["acp_call_002"] = &ParkedMcpCall{
		Call:       ToolCallItem{ID: "acp_call_002", Name: "Bash", Input: map[string]any{"command": "go test -race ./..."}},
		ResultChan: ch2,
		ParkedAt:   s.ParkedSince,
	}
	s.PendingSiblingCalls = []*ParkedMcpCall{
		{
			Call:       ToolCallItem{ID: "acp_call_003", Name: "Read", Input: map[string]any{"file_path": "go-gateway/store.go"}},
			ResultChan: ch3,
			ParkedAt:   s.ParkedSince,
		},
	}
	s.mu.Lock()
	s.syncPhaseLocked()
	s.mu.Unlock()

	// 1. Hibernate with elastic_parked_offload: CancelSession + CloseSession MUST run BEFORE ResultChan receives anything!
	if !s.HibernateIfSession(sid, "elastic_parked_offload") {
		t.Fatal("expected HibernateIfSession(elastic_parked_offload) to succeed")
	}
	ordEng.mu.Lock()
	sawPremature := ordEng.sawPrematureMsg
	events := append([]string(nil), ordEng.events...)
	ordEng.mu.Unlock()
	if sawPremature {
		t.Fatal("P0 violation: ResultChan received hibernation message BEFORE CancelSession/CloseSession completed")
	}
	if len(events) != 2 || events[0] != "cancel:"+sid || events[1] != "close:"+sid {
		t.Fatalf("expected [cancel:%s close:%s], got %v", sid, sid, events)
	}
	for i, ch := range []chan map[string]any{ch1, ch2, ch3} {
		select {
		case msg := <-ch:
			if isErr, _ := msg["isError"].(bool); !isErr {
				t.Fatalf("chan %d expected isError=true after CloseSession, got %+v", i+1, msg)
			}
		default:
			t.Fatalf("chan %d was not unblocked after CloseSession", i+1)
		}
	}

	// 2. Verify orphanCapStreak NOT incremented and HibernatedParkedCalls recorded all 3 siblings with exact Seq/ToolName/ArgSummary.
	s.mu.Lock()
	if s.orphanCapStreak != 0 {
		s.mu.Unlock()
		t.Fatalf("elastic_parked_offload must NOT increment orphanCapStreak, got %d", s.orphanCapStreak)
	}
	if s.HibernatedSessionID != sid || len(s.HibernatedParkedCalls) != 3 || len(s.HibernatedParkedCallIDs) != 3 {
		s.mu.Unlock()
		t.Fatalf("unexpected hibernated state: hibSid=%q calls=%+v ids=%+v", s.HibernatedSessionID, s.HibernatedParkedCalls, s.HibernatedParkedCallIDs)
	}
	m1 := s.HibernatedParkedCalls["acp_call_001"]
	m2 := s.HibernatedParkedCalls["acp_call_002"]
	m3 := s.HibernatedParkedCalls["acp_call_003"]
	s.mu.Unlock()
	if m1.Seq != 1 || m1.ToolName != "Read" || !strings.Contains(m1.ArgSummary, "config.go") {
		t.Fatalf("unexpected meta #1: %+v", m1)
	}
	if m2.Seq != 2 || m2.ToolName != "Bash" || !strings.Contains(m2.ArgSummary, "go test -race") {
		t.Fatalf("unexpected meta #2: %+v", m2)
	}
	if m3.Seq != 3 || m3.ToolName != "Read" || !strings.Contains(m3.ArgSummary, "store.go") {
		t.Fatalf("unexpected meta #3: %+v", m3)
	}

	// 3. Verify SQLite persistence & restart recovery of HibernatedParkedCalls in gateway.db.
	GlobalStore.Flush()
	stVerify := &StateStore{}
	stVerify.Init(dbPath)
	defer stVerify.Close()
	restored := stVerify.RestoreSingleSession(s.ConvKey)
	if restored == nil {
		t.Fatal("RestoreSingleSession returned nil after elastic_parked_offload persist")
	}
	if len(restored.HibernatedParkedCalls) != 3 || !restored.HibernatedParkedCallIDs["acp_call_002"] {
		t.Fatalf("expected HibernatedParkedCalls restored from gateway.db, got calls=%+v ids=%+v", restored.HibernatedParkedCalls, restored.HibernatedParkedCallIDs)
	}
	if restored.HibernatedParkedCalls["acp_call_002"].ToolName != "Bash" || restored.HibernatedParkedCalls["acp_call_002"].Seq != 2 {
		t.Fatalf("unexpected restored meta for acp_call_002: %+v", restored.HibernatedParkedCalls["acp_call_002"])
	}

	// 4. Verify resumed_delta tool result formatting preserves is_error (status=error) + #Seq ToolName (ArgSummary).
	toolResultMsg := map[string]any{
		"role": "user",
		"content": []any{
			map[string]any{"type": "tool_result", "tool_use_id": "acp_call_001", "content": "package main", "is_error": false},
			map[string]any{"type": "tool_result", "tool_use_id": "acp_call_002", "content": "FAIL: exit status 1", "is_error": true},
			map[string]any{"type": "tool_result", "tool_use_id": "acp_call_003", "content": "store code"},
		},
	}
	ctxWithMeta := withHibernatedCallMeta(context.Background(), restored.snapshotHibernatedParkedCalls())
	blocks, err := resolveAndBuildPromptBlocks(ctxWithMeta, GlobalImageFetcher, []map[string]any{toolResultMsg}, false, []map[string]any{toolResultMsg}, MaxTranscriptChar, ModelOrDefault("gemini-4-carbon"))
	if err != nil || len(blocks) == 0 {
		t.Fatalf("resolveAndBuildPromptBlocks failed: err=%v blocks=%+v", err, blocks)
	}
	promptText, _ := blocks[len(blocks)-1]["text"].(string)
	for _, wantSub := range []string{
		"[Tool Result #1 (Read (go-gateway/config.go), id=acp_call_001)]: package main",
		"[Tool Result #2 (Bash (go test -race ./...), id=acp_call_002, status=error)]: FAIL: exit status 1",
		"[Tool Result #3 (Read (go-gateway/store.go), id=acp_call_003)]: store code",
	} {
		if !strings.Contains(promptText, wantSub) {
			t.Fatalf("resumed_delta prompt missing %q\ngot:\n%s", wantSub, promptText)
		}
	}

	// 5. Verify MatchHibernatedParkedCallID deterministic secondary rebinding for Gemini /v1beta.
	used := map[string]bool{}
	idRead1 := restored.MatchHibernatedParkedCallID("Read", used)
	if idRead1 != "acp_call_001" {
		t.Fatalf("first Read match = %q, want acp_call_001", idRead1)
	}
	used[idRead1] = true
	idBash := restored.MatchHibernatedParkedCallID("Bash", used)
	if idBash != "acp_call_002" {
		t.Fatalf("Bash match = %q, want acp_call_002", idBash)
	}
	used[idBash] = true
	idRead2 := restored.MatchHibernatedParkedCallID("Read", used)
	if idRead2 != "acp_call_003" {
		t.Fatalf("second Read match = %q, want acp_call_003", idRead2)
	}
	used[idRead2] = true
	if extra := restored.MatchHibernatedParkedCallID("Read", used); extra != "" {
		t.Fatalf("expected empty match once all Read calls claimed, got %q", extra)
	}

	// 6. Verify Elastic Parked Offload eligibility matrix (15s hot protection, long-exec >=15s <64K, any >=45s <64K, >=64K only under high pressure).
	cases := []struct {
		name         string
		cand         WorkerSlotCandidate
		allowHighCtx bool
		want         bool
	}{
		{"hot_10s_bash_protected", WorkerSlotCandidate{Status: "PARKED", ParkedLongExec: true, ParkedForSec: 10, ContextTokens: 10000}, false, false},
		{"hot_10s_bash_high_still_protected", WorkerSlotCandidate{Status: "PARKED", ParkedLongExec: true, ParkedForSec: 10, ContextTokens: 10000}, true, false},
		{"long_exec_18s_small_ctx_eligible", WorkerSlotCandidate{Status: "PARKED", ParkedLongExec: true, ParkedForSec: 18, ContextTokens: 30000}, false, true},
		{"long_exec_18s_large_ctx_not_eligible", WorkerSlotCandidate{Status: "PARKED", ParkedLongExec: true, ParkedForSec: 18, ContextTokens: 80000}, false, false},
		{"read_20s_small_ctx_not_eligible", WorkerSlotCandidate{Status: "PARKED", ParkedLongExec: false, ParkedForSec: 20, ContextTokens: 10000}, false, false},
		{"read_46s_small_ctx_eligible_pre_relay", WorkerSlotCandidate{Status: "PARKED", ParkedLongExec: false, ParkedForSec: 46, ContextTokens: 50000}, false, true},
		{"read_46s_large_ctx_protected_below_high", WorkerSlotCandidate{Status: "PARKED", ParkedLongExec: false, ParkedForSec: 46, ContextTokens: 90000}, false, false},
		{"read_46s_large_ctx_eligible_at_high", WorkerSlotCandidate{Status: "PARKED", ParkedLongExec: false, ParkedForSec: 46, ContextTokens: 90000}, true, true},
	}
	for _, tc := range cases {
		if got := isElasticParkedOffloadEligible(tc.cand, tc.allowHighCtx); got != tc.want {
			t.Errorf("%s: isElasticParkedOffloadEligible = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestV2326_GlobalAdmitInternalRouteLoopbackAndJSON(t *testing.T) {
	origAM := GlobalAccountManager
	origBurst := GlobalBurstMaxLiveWorkers
	origBurstBoot := globalBurstMaxLiveWorkersBoot
	origIdle := GlobalIdleTarget
	origIdleBoot := globalIdleTargetBoot
	t.Cleanup(func() {
		GlobalAccountManager = origAM
		GlobalBurstMaxLiveWorkers = origBurst
		globalBurstMaxLiveWorkersBoot = origBurstBoot
		GlobalIdleTarget = origIdle
		globalIdleTargetBoot = origIdleBoot
		initConfig()
	})

	t.Setenv("AGY_GLOBAL_IDLE_TARGET", "2")
	t.Setenv("AGY_GLOBAL_BURST_MAX_LIVE_WORKERS", "2")
	initConfig()

	restoreMem := setCgroupMemoryReaderForTest(func() CgroupMemoryStatus {
		return CgroupMemoryStatus{Version: 2, LimitBytes: 6144 * 1024 * 1024, WorkingSetBytes: 1024 * 1024 * 1024, UsagePct: 16.6, HasLiveUsage: true}
	})
	defer restoreMem()

	gs := NewGatewayServerWithDeps(newV2322CloseTrackEngine(nil), &StateStore{})
	defer gs.Close()
	am := NewAccountManager()
	am.setPrimaryGatewayForTest(gs)
	GlobalAccountManager = am

	// Non-loopback rejected with 403.
	reqNonLoop := httptest.NewRequest(http.MethodPost, "/api/internal/global-admit", bytes.NewBufferString(`{"accountId":"acc2"}`))
	reqNonLoop.RemoteAddr = "203.0.113.5:12345"
	rrNonLoop := httptest.NewRecorder()
	if !gs.HandleInternalV2326Routes(rrNonLoop, reqNonLoop) || rrNonLoop.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for non-loopback global-admit, got %d", rrNonLoop.Code)
	}

	// Loopback acquire & release via HTTP.
	reqAdmit := httptest.NewRequest(http.MethodPost, "/api/internal/global-admit", bytes.NewBufferString(`{"accountId":"acc2","priorityHint":"normal"}`))
	reqAdmit.RemoteAddr = "127.0.0.1:43210"
	rrAdmit := httptest.NewRecorder()
	if !gs.HandleInternalV2326Routes(rrAdmit, reqAdmit) || rrAdmit.Code != http.StatusOK {
		t.Fatalf("expected 200 for loopback global-admit, got %d body=%s", rrAdmit.Code, rrAdmit.Body.String())
	}
	var admitResp GlobalAdmitResponse
	if err := json.Unmarshal(rrAdmit.Body.Bytes(), &admitResp); err != nil || !admitResp.OK || admitResp.LeaseID == "" {
		t.Fatalf("unexpected admit response: %+v err=%v", admitResp, err)
	}
	if am.ActiveGlobalSpawnLeases() != 1 {
		t.Fatalf("expected 1 active spawn lease, got %d", am.ActiveGlobalSpawnLeases())
	}

	reqRel := httptest.NewRequest(http.MethodPost, "/api/internal/global-release", bytes.NewBufferString(fmt.Sprintf(`{"leaseId":%q}`, admitResp.LeaseID)))
	reqRel.RemoteAddr = "127.0.0.1:43211"
	rrRel := httptest.NewRecorder()
	if !gs.HandleInternalV2326Routes(rrRel, reqRel) || rrRel.Code != http.StatusOK {
		t.Fatalf("expected 200 for loopback global-release, got %d", rrRel.Code)
	}
	if am.ActiveGlobalSpawnLeases() != 0 {
		t.Fatalf("expected 0 active spawn leases after release, got %d", am.ActiveGlobalSpawnLeases())
	}
}

type v2326BlockingCloseEngine struct {
	*stubEngine
	closeEntered chan struct{}
	releaseClose chan struct{}
	closeErr     error
	enteredOnce  sync.Once
}

func (e *v2326BlockingCloseEngine) CloseSession(sid string) error {
	e.enteredOnce.Do(func() {
		if e.closeEntered != nil {
			close(e.closeEntered)
		}
	})
	if e.releaseClose != nil {
		<-e.releaseClose
	}
	if e.closeErr != nil {
		return e.closeErr
	}
	return e.stubEngine.CloseSession(sid)
}

func TestV2326_P1_1_GlobalAdmitFIFOFairnessImageGenGuardAndChildTimeoutFallback(t *testing.T) {
	origAM := GlobalAccountManager
	origIdle := GlobalIdleTarget
	origIdleBoot := globalIdleTargetBoot
	origBurst := GlobalBurstMaxLiveWorkers
	origBurstBoot := globalBurstMaxLiveWorkersBoot
	origPoll := globalAdmitPollInterval
	origWait := globalAdmitWaitTimeout
	origHook := globalIdleImageGenStopHook
	t.Cleanup(func() {
		if GlobalAccountManager != nil && GlobalAccountManager != origAM {
			GlobalAccountManager.StopAll()
		}
		GlobalAccountManager = origAM
		GlobalIdleTarget = origIdle
		globalIdleTargetBoot = origIdleBoot
		GlobalBurstMaxLiveWorkers = origBurst
		globalBurstMaxLiveWorkersBoot = origBurstBoot
		globalAdmitPollInterval = origPoll
		globalAdmitWaitTimeout = origWait
		globalIdleImageGenStopHook = origHook
		initConfig()
	})

	t.Setenv("AGY_GLOBAL_IDLE_TARGET", "1")
	t.Setenv("AGY_GLOBAL_BURST_MAX_LIVE_WORKERS", "1")
	t.Setenv("AGY_API_KEYS", "")
	initConfig()

	var memPct atomic.Int64
	memPct.Store(30)
	restoreMem := setCgroupMemoryReaderForTest(func() CgroupMemoryStatus {
		pct := float64(memPct.Load())
		return CgroupMemoryStatus{
			Version:         2,
			LimitBytes:      6144 << 20,
			WorkingSetBytes: int64(pct / 100.0 * float64(6144<<20)),
			UsagePct:        pct,
			HasLiveUsage:    true,
		}
	})
	defer restoreMem()

	gs := NewGatewayServerWithDeps(newV2322CloseTrackEngine(nil), &StateStore{})
	defer gs.Close()
	am := NewAccountManager()
	am.setPrimaryGatewayForTest(gs)
	GlobalAccountManager = am

	// 1. Strict FIFO fairness: new arrivals MUST NOT cut ahead of waiting head goroutines.
	var hookCalls atomic.Int32
	globalIdleImageGenStopHook = func() bool {
		hookCalls.Add(1)
		return false
	}
	globalAdmitPollInterval = 200 * time.Millisecond
	globalAdmitWaitTimeout = 3 * time.Second

	busy := v2322SeedSession(gs, "sid:fifo-busy", "00000000-0000-4000-8000-00000000f101", "BUSY", time.Now(), 8000)

	waitForQueueLen := func(want int) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			am.admitStateMu.Lock()
			n := len(am.admitWaiters)
			am.admitStateMu.Unlock()
			if n >= want {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for admitWaiters >= %d", want)
	}

	var grantMu sync.Mutex
	var grantOrder []string
	var wg sync.WaitGroup
	startWaiter := func(name string) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lid, err := am.AcquireGlobalWorkerSlot(context.Background(), "primary", "normal")
			if err != nil {
				t.Errorf("waiter %s failed: %v", name, err)
				return
			}
			grantMu.Lock()
			grantOrder = append(grantOrder, name)
			grantMu.Unlock()
			time.Sleep(15 * time.Millisecond)
			am.ReleaseGlobalWorkerSlot(lid)
		}()
	}

	startWaiter("w1")
	waitForQueueLen(1)
	startWaiter("w2")
	waitForQueueLen(2)
	startWaiter("w3")
	waitForQueueLen(3)

	// Confirm that while saturated at burstMax=1 with low memory (30%), warm ws-imagegen was NOT killed.
	if calls := hookCalls.Load(); calls != 0 {
		t.Fatalf("expected 0 globalIdleImageGenStopHook calls when highMem=false, got %d", calls)
	}

	// Free the BUSY slot so waiters drain strictly in FIFO order: w1 -> w2 -> w3.
	busy.mu.Lock()
	busy.IsBusy = false
	busy.PromptInFlight = false
	busy.LastUsedAt = time.Now().Add(-30 * time.Second)
	busy.syncPhaseLocked()
	busy.mu.Unlock()
	am.NotifyGlobalWorkerSlotFreed()

	wg.Wait()
	if len(grantOrder) != 3 || grantOrder[0] != "w1" || grantOrder[1] != "w2" || grantOrder[2] != "w3" {
		t.Fatalf("FIFO fairness violated: got grant order %v, want [w1 w2 w3]", grantOrder)
	}

	// 2. When highMem == true (>=82%), AcquireGlobalWorkerSlot DOES invoke globalIdleImageGenStopHook once and grants after memory drops.
	hookCalls.Store(0)
	memPct.Store(86)
	globalIdleImageGenStopHook = func() bool {
		hookCalls.Add(1)
		memPct.Store(55)
		return true
	}
	gsEmpty := NewGatewayServerWithDeps(newV2322CloseTrackEngine(nil), &StateStore{})
	defer gsEmpty.Close()
	am.setPrimaryGatewayForTest(gsEmpty)
	lidHigh, errHigh := am.AcquireGlobalWorkerSlot(context.Background(), "primary", "normal")
	if errHigh != nil || lidHigh == "" {
		t.Fatalf("expected grant after highMem idle imagegen stop relieved pressure, got lid=%q err=%v", lidHigh, errHigh)
	}
	am.ReleaseGlobalWorkerSlot(lidHigh)
	if hookCalls.Load() != 1 {
		t.Fatalf("expected exactly 1 globalIdleImageGenStopHook call under highMem, got %d", hookCalls.Load())
	}

	// 3. P2-1: runGlobalMemoryGovernorTick under high pressure + overIdle calls hook at most ONCE per tick.
	hookCalls.Store(0)
	memPct.Store(86)
	globalIdleImageGenStopHook = func() bool {
		hookCalls.Add(1)
		return false
	}
	v2322SeedSession(gsEmpty, "sid:gov-busy-1", "00000000-0000-4000-8000-00000000f201", "BUSY", time.Now(), 8000)
	v2322SeedSession(gsEmpty, "sid:gov-busy-2", "00000000-0000-4000-8000-00000000f202", "BUSY", time.Now(), 8000)
	_ = am.runGlobalMemoryGovernorTick(time.Now())
	if got := hookCalls.Load(); got != 1 {
		t.Fatalf("expected runGlobalMemoryGovernorTick to invoke imagegen stop hook at most once per tick, got %d", got)
	}

	// 4. collectGlobalWorkerSlots child RPC timeout falls back to last known snapshot (or conservative >=1 when none).
	var slowMode atomic.Bool
	childSrv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if slowMode.Load() {
			time.Sleep(250 * time.Millisecond)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(WorkerSlotsSnapshot{LiveWorkers: 3, LogicalSessions: 4, HibernatedSessions: 1})
	}))
	defer childSrv1.Close()
	u1, _ := url.Parse(childSrv1.URL)
	p1, _ := strconv.Atoi(u1.Port())

	childSrv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(250 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer childSrv2.Close()
	u2, _ := url.Parse(childSrv2.URL)
	p2, _ := strconv.Atoi(u2.Port())

	amChild := NewAccountManager()
	amChild.setPrimaryGatewayForTest(NewGatewayServerWithDeps(newV2322CloseTrackEngine(nil), &StateStore{}))
	amChild.workers = map[string]*accountWorkerProc{
		"child-snap": {dashPort: p1},
	}
	// First call succeeds and caches LiveWorkers=3.
	live1, _, _, _ := amChild.collectGlobalWorkerSlots(time.Now(), 500*time.Millisecond, "", "")
	if live1 != 3 {
		t.Fatalf("expected initial child snapshot live=3, got %d", live1)
	}
	// Second call times out child-snap AND adds cold child-nosnap -> must count 3 + 1 = 4 (never 0).
	slowMode.Store(true)
	amChild.workers["child-nosnap"] = &accountWorkerProc{dashPort: p2}
	live2, _, _, _ := amChild.collectGlobalWorkerSlots(time.Now(), 40*time.Millisecond, "", "")
	if live2 != 4 {
		t.Fatalf("expected timeout fallback 3 (cached) + 1 (conservative) = 4, got %d", live2)
	}
}

func TestV2326_P1_2_3_4_5_ProbeGateClosingRaceCloseErrAndShutdownOrder(t *testing.T) {
	origAM := GlobalAccountManager
	origIdle := GlobalIdleTarget
	origIdleBoot := globalIdleTargetBoot
	origBurst := GlobalBurstMaxLiveWorkers
	origBurstBoot := globalBurstMaxLiveWorkersBoot
	origMaxW := MaxLiveWorkers
	origMaxProbes := MaxProbeWorkers
	t.Cleanup(func() {
		if GlobalAccountManager != nil && GlobalAccountManager != origAM {
			GlobalAccountManager.StopAll()
		}
		GlobalAccountManager = origAM
		GlobalIdleTarget = origIdle
		globalIdleTargetBoot = origIdleBoot
		GlobalBurstMaxLiveWorkers = origBurst
		globalBurstMaxLiveWorkersBoot = origBurstBoot
		MaxLiveWorkers = origMaxW
		MaxProbeWorkers = origMaxProbes
		initConfig()
	})

	t.Setenv("AGY_GLOBAL_IDLE_TARGET", "2")
	t.Setenv("AGY_GLOBAL_BURST_MAX_LIVE_WORKERS", "2")
	t.Setenv("AGY_MAX_LIVE_WORKERS", "4")
	t.Setenv("AGY_API_KEYS", "")
	initConfig()

	restoreMem := setCgroupMemoryReaderForTest(func() CgroupMemoryStatus {
		return CgroupMemoryStatus{Version: 2, LimitBytes: 6144 << 20, WorkingSetBytes: 1200 << 20, UsagePct: 19.5, HasLiveUsage: true}
	})
	defer restoreMem()

	// 1. P1-2: reserveProbeWorkerSlot MUST honor global BurstMax (non-preemptive "probe") and never leak global leases.
	gsProbe := NewGatewayServerWithDeps(newV2322CloseTrackEngine(nil), &StateStore{})
	defer gsProbe.Close()
	am := NewAccountManager()
	am.setPrimaryGatewayForTest(gsProbe)
	GlobalAccountManager = am
	MaxLiveWorkers = 4
	MaxProbeWorkers = 2

	w1 := v2322SeedSession(gsProbe, "sid:probe-warm-1", "00000000-0000-4000-8000-00000000a101", "IDLE", time.Now(), 80000)
	w1.ModelID = "gemini-4-carbon"
	w2 := v2322SeedSession(gsProbe, "sid:probe-warm-2", "00000000-0000-4000-8000-00000000a102", "IDLE", time.Now(), 80000)
	w2.ModelID = "gemini-4-carbon"

	probeSess := NewAcpProjectSession("util:waf-probe-global", "/tmp", "")
	probeSess.isWafBisectProbe = true
	t0 := time.Now()
	_, errProbe := gsProbe.reserveProbeWorkerSlot(probeSess)
	if se, ok := errProbe.(*SoftError); !ok || se.StatusCode != 529 {
		t.Fatalf("expected global probe gate 529 when BurstMax=2 filled by warm T3 workers, got %v", errProbe)
	}
	if time.Since(t0) > 500*time.Millisecond {
		t.Fatalf("expected non-preemptive probe to fail fast, took %v", time.Since(t0))
	}
	if am.ActiveGlobalSpawnLeases() != 0 {
		t.Fatalf("expected 0 leaked global leases after probe rejection, got %d", am.ActiveGlobalSpawnLeases())
	}

	// Free one slot -> probe succeeds, increments LiveWorkers, and cleans up cleanly.
	w2.mu.Lock()
	w2.SessionID = ""
	w2.syncPhaseLocked()
	w2.mu.Unlock()
	relProbe, errProbe2 := gsProbe.reserveProbeWorkerSlot(probeSess)
	if errProbe2 != nil {
		t.Fatalf("expected probe to succeed when 1/2 global burst slots used, got %v", errProbe2)
	}
	if am.ActiveGlobalSpawnLeases() != 0 {
		t.Fatalf("expected global spawn lease released after probe reserved, got %d", am.ActiveGlobalSpawnLeases())
	}
	if snap := gsProbe.CollectWorkerSlots(time.Now()); snap.LiveWorkers != 2 {
		t.Fatalf("expected CollectWorkerSlots LiveWorkers=2 (1 warm + 1 probe), got %d", snap.LiveWorkers)
	}
	relProbe()
	if snap := gsProbe.CollectWorkerSlots(time.Now()); snap.LiveWorkers != 1 {
		t.Fatalf("expected CollectWorkerSlots LiveWorkers=1 after probe release, got %d", snap.LiveWorkers)
	}

	// 2. P1-3: Concurrent tool_result arriving during hibernateWorker CloseSession (s.closing == true) MUST wait and resume hibernated session.
	base := &stubEngine{}
	base.alive.Store(true)
	blockEng := &v2326BlockingCloseEngine{
		stubEngine:   base,
		closeEntered: make(chan struct{}),
		releaseClose: make(chan struct{}),
	}
	gsClose := NewGatewayServerWithDeps(blockEng, &StateStore{})
	defer gsClose.Close()

	sid := "00000000-0000-4000-8000-00000000c301"
	sClosing := NewAcpProjectSession("sid:closing-race", "/tmp/proj", "sys-close")
	sClosing.Engine = blockEng
	sClosing.Store = gsClose.Store
	sClosing.SessionID = sid
	sClosing.engineGen = 1
	sClosing.MessageCount = 4
	sClosing.PromptInFlight = true
	sClosing.ParkedSince = time.Now().Add(-20 * time.Second)
	sClosing.LastUsedAt = time.Now().Add(-20 * time.Second)
	sClosing.ParkedCalls["acp_closing_001"] = &ParkedMcpCall{
		Call:       ToolCallItem{ID: "acp_closing_001", Name: "Bash", Input: map[string]any{"command": "sleep 20"}},
		ResultChan: make(chan map[string]any, 1),
		ParkedAt:   sClosing.ParkedSince,
	}
	sClosing.mu.Lock()
	sClosing.syncPhaseLocked()
	sClosing.mu.Unlock()
	gsClose.mu.Lock()
	gsClose.sessions[sClosing.ConvKey] = sClosing
	gsClose.tenants[sClosing.TenantKey] = sClosing
	gsClose.mu.Unlock()

	hibDone := make(chan bool, 1)
	go func() {
		hibDone <- sClosing.HibernateIfSession(sid, "elastic_parked_offload")
	}()
	<-blockEng.closeEntered

	toolResultMsgs := []map[string]any{
		{
			"role": "user",
			"content": []any{
				map[string]any{"type": "tool_result", "tool_use_id": "acp_closing_001", "content": "done", "is_error": "true"},
			},
		},
	}
	// Verify P2-3: string "true" for is_error is extracted properly.
	if trs := extractToolResultsFromMessages(toolResultMsgs); len(trs) != 1 || !trs[0].IsError {
		t.Fatalf("expected string is_error=\"true\" parsed as IsError=true, got %+v", trs)
	}

	foundCh := make(chan *AcpProjectSession, 1)
	go func() {
		foundCh <- gsClose.findSessionWithParkedToolResult("sid:closing-race", toolResultMsgs)
	}()
	select {
	case premature := <-foundCh:
		t.Fatalf("findSessionWithParkedToolResult returned prematurely during closing window: %+v", premature)
	case <-time.After(35 * time.Millisecond):
	}
	close(blockEng.releaseClose)
	if !<-hibDone {
		t.Fatal("expected HibernateIfSession to succeed after CloseSession unblocked")
	}
	found := <-foundCh
	if found != sClosing {
		t.Fatalf("expected findSessionWithParkedToolResult to recover closing->hibernated session, got %v", found)
	}
	found.releaseTurnClaim()
	if found.HibernatedSessionID != sid || !found.HibernatedParkedCallIDs["acp_closing_001"] {
		t.Fatalf("expected hibernated resume metadata intact, hibSid=%q ids=%+v", found.HibernatedSessionID, found.HibernatedParkedCallIDs)
	}

	// 3. P1-4: force && hasPendingParked with CloseSession error MUST stay hibernated (no dirty live rollback),
	//    and finishCloseLocked(restoreSid != "") MUST clear HibernatedParkedCalls.
	errEngBase := &stubEngine{}
	errEngBase.alive.Store(true)
	errEng := &v2326BlockingCloseEngine{
		stubEngine: errEngBase,
		closeErr:   fmt.Errorf("simulated close timeout"),
	}
	sErr := NewAcpProjectSession("sid:force-close-err", "/tmp/proj", "sys-err")
	sErr.Engine = errEng
	sErr.Store = gsClose.Store
	sErr.SessionID = "00000000-0000-4000-8000-00000000c401"
	sErr.engineGen = 1
	sErr.MessageCount = 4
	sErr.PromptInFlight = true
	sErr.ParkedSince = time.Now().Add(-30 * time.Second)
	sErr.ParkedCalls["acp_err_001"] = &ParkedMcpCall{
		Call:       ToolCallItem{ID: "acp_err_001", Name: "Bash"},
		ResultChan: make(chan map[string]any, 1),
		ParkedAt:   sErr.ParkedSince,
	}
	sErr.mu.Lock()
	sErr.syncPhaseLocked()
	sErr.mu.Unlock()
	if !sErr.HibernateIfSession(sErr.SessionID, "elastic_parked_offload") {
		t.Fatal("expected force+parked hibernation to stay disconnected/hibernated despite CloseSession error")
	}
	sErr.mu.Lock()
	if sErr.SessionID != "" || sErr.HibernatedSessionID != "00000000-0000-4000-8000-00000000c401" || !sErr.HibernatedParkedCallIDs["acp_err_001"] {
		sErr.mu.Unlock()
		t.Fatalf("expected clean hibernated state after force+parked CloseSession error: sid=%q hibSid=%q ids=%+v", sErr.SessionID, sErr.HibernatedSessionID, sErr.HibernatedParkedCallIDs)
	}
	// Verify FSM invariant: finishCloseLocked(restoreSid) always clears HibernatedParkedCalls.
	sErr.finishCloseLocked("00000000-0000-4000-8000-00000000c401")
	if len(sErr.HibernatedParkedCalls) != 0 || len(sErr.HibernatedParkedCallIDs) != 0 {
		sErr.mu.Unlock()
		t.Fatalf("finishCloseLocked(restoreSid) left orphan HibernatedParkedCalls: %+v %+v", sErr.HibernatedParkedCalls, sErr.HibernatedParkedCallIDs)
	}
	sErr.mu.Unlock()

	// 4. P1-5: BeginShutdown() MUST call eng.CancelSession BEFORE pushing error to ResultChan.
	shCh := make(chan map[string]any, 1)
	shBase := &stubEngine{}
	shBase.alive.Store(true)
	shEng := &v2326OrderCheckEngine{
		stubEngine: shBase,
		watchChans: []chan map[string]any{shCh},
	}
	gsShut := NewGatewayServerWithDeps(shEng, &StateStore{})
	defer gsShut.Close()
	sShut := NewAcpProjectSession("sid:shutdown-order", "/tmp/proj", "sys-shut")
	sShut.Engine = shEng
	sShut.Store = gsShut.Store
	sShut.SessionID = "00000000-0000-4000-8000-00000000c501"
	sShut.MessageCount = 2
	sShut.PromptInFlight = true
	sShut.ParkedCalls["acp_shut_001"] = &ParkedMcpCall{
		Call:       ToolCallItem{ID: "acp_shut_001", Name: "Read"},
		ResultChan: shCh,
		ParkedAt:   time.Now(),
	}
	gsShut.mu.Lock()
	gsShut.sessions[sShut.ConvKey] = sShut
	gsShut.mu.Unlock()
	if n := gsShut.BeginShutdown(); n != 1 {
		t.Fatalf("BeginShutdown unblocked = %d, want 1", n)
	}
	shEng.mu.Lock()
	sawPremature := shEng.sawPrematureMsg
	events := append([]string(nil), shEng.events...)
	shEng.mu.Unlock()
	if sawPremature {
		t.Fatal("BeginShutdown pushed to ResultChan BEFORE eng.CancelSession")
	}
	if len(events) != 1 || events[0] != "cancel:00000000-0000-4000-8000-00000000c501" {
		t.Fatalf("expected [cancel:00000000-0000-4000-8000-00000000c501], got %v", events)
	}
	select {
	case msg := <-shCh:
		if isErr, _ := msg["isError"].(bool); !isErr {
			t.Fatalf("expected isError=true shutdown message, got %+v", msg)
		}
	default:
		t.Fatal("expected BeginShutdown to unblock ResultChan after CancelSession")
	}

	// 5. P2-5: Single-account without global cap reports globalBurstMaxLiveWorkers == 0 (not faked as globalMaxLiveWorkers),
	//    and BuildFederatedDashboardPayload exposes globalIdleTarget & globalBurstMaxLiveWorkers.
	t.Setenv("AGY_GLOBAL_IDLE_TARGET", "0")
	t.Setenv("AGY_GLOBAL_MAX_LIVE_WORKERS", "0")
	t.Setenv("AGY_GLOBAL_BURST_MAX_LIVE_WORKERS", "0")
	t.Setenv("AGY_MAX_LIVE_WORKERS", "8")
	initConfig()
	amSingle := NewAccountManager()
	amSingle.accounts = []AccountProfile{{ID: "primary", Label: "Single", Primary: true, Enabled: true, MaxLiveWorkers: 8}}
	amSingle.enabledAccountCount.Store(1)
	GlobalAccountManager = amSingle
	recSingle := httptest.NewRecorder()
	HandleAccountsAPI(recSingle, httptest.NewRequest(http.MethodGet, "/api/accounts", nil))
	var singleBody struct {
		PoolStats map[string]any `json:"poolStats"`
	}
	if err := json.Unmarshal(recSingle.Body.Bytes(), &singleBody); err != nil {
		t.Fatalf("decode single /api/accounts: %v", err)
	}
	if singleBody.PoolStats["globalBurstMaxLiveWorkers"] != float64(0) || singleBody.PoolStats["globalIdleTarget"] != float64(0) {
		t.Fatalf("single-account unbounded mode must report globalBurstMaxLiveWorkers=0 & globalIdleTarget=0, got %+v", singleBody.PoolStats)
	}
}
