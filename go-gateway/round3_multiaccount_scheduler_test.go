package main

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gwcfg "agy-acp-gateway-go/internal/config"
)

func setupRound3AccountTestEnv(t *testing.T) (am *AccountManager, cleanup func()) {
	t.Helper()
	prevDB := GatewayStateDB
	waitQuotaRefreshIdle(t)
	quotaMu.Lock()
	prevQuota := cachedQuota
	prevQuotaAt := cachedQuotaAt
	prevQuotaErr := lastQuotaErr
	prevQuotaSteps := quotaBackoffSteps
	cachedQuota = nil
	cachedQuotaAt = time.Time{}
	lastQuotaErr = nil
	quotaBackoffSteps = 0
	quotaMu.Unlock()

	cleanup = func() {
		waitQuotaRefreshIdle(t)
		GlobalAccountManager.StopAll()
		GlobalAccountManager = NewAccountManager()
		resetPeerDashboardCacheForTest()
		quotaMu.Lock()
		cachedQuota = prevQuota
		cachedQuotaAt = prevQuotaAt
		lastQuotaErr = prevQuotaErr
		quotaBackoffSteps = prevQuotaSteps
		quotaMu.Unlock()
		initConfig()
		apiKeys = loadAPIKeys()
		InitGlobalStore(prevDB)
	}
	t.Cleanup(cleanup)

	tmpDir := t.TempDir()
	accFile := filepath.Join(tmpDir, "accounts.json")

	t.Setenv("AGY_STATE_DB", filepath.Join(tmpDir, "gateway.db"))
	t.Setenv("AGY_QUOTA_BIN", "/bin/false")
	t.Setenv("AGY_TOS_TRIPPED_FILE", filepath.Join(tmpDir, "tos-tripped"))
	t.Setenv("AGY_ACCOUNTS_FILE", accFile)
	t.Setenv("AGY_INSTANCE_ID", "primary")
	t.Setenv("AGY_INSTANCE_LABEL", "Primary Account")
	t.Setenv("AGY_API_KEYS", "master-r3-key")
	initConfig()
	apiKeys = loadAPIKeys()

	GlobalAccountManager.StopAll()
	GlobalAccountManager = NewAccountManager()
	GlobalAccountManager.Init()
	writeDummyOAuthToken := func(accID string) {
		p := GlobalAccountManager.AccountTokenPath(accID)
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			t.Fatalf("mkdir token dir for %s: %v", accID, err)
		}
		if err := os.WriteFile(p, []byte(`{"access_token":"unit-test-dummy-token-value","expiry_date":9999999999}`), 0o600); err != nil {
			t.Fatalf("write dummy token for %s: %v", accID, err)
		}
		GlobalAccountManager.InvalidateTokenCache(accID)
	}

	allModels := []string{"gemini-4-neon", "gemini-4-carbon", "gemini-3.8-flash", "claude-opus-5-5", "claude-sonnet-5-5"}
	for _, prof := range []AccountProfile{
		{ID: "primary", Label: "Primary", Primary: true, Enabled: true, ProxyPreset: "direct", MaxLiveWorkers: 8, Priority: 100, Weight: 1, AllowedModels: allModels},
		{ID: "acct-b", Label: "Account B", Enabled: true, ProxyPreset: "direct", MaxLiveWorkers: 4, Priority: 10, Weight: 2, AllowedModels: allModels},
		{ID: "acct-c", Label: "Account C", Enabled: true, ProxyPreset: "direct", MaxLiveWorkers: 4, Priority: 50, Weight: 5, AllowedModels: allModels},
	} {
		if _, err := GlobalAccountManager.UpsertAccount(prof); err != nil {
			t.Fatalf("upsert %s: %v", prof.ID, err)
		}
		if !prof.Primary {
			writeDummyOAuthToken(prof.ID)
		}
	}

	return GlobalAccountManager, cleanup
}

func makeOpenAITurn1Req(userPrompt string) *http.Request {
	body := `{"model":"gemini-4-neon","messages":[{"role":"system","content":"Shared IDE static system prompt"},{"role":"user","content":` + strconv.Quote(userPrompt) + `}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer master-r3-key")
	return req
}

func TestRound3_AllFiveRoutingStrategiesAndTurn1SystemPromptDistribution(t *testing.T) {
	am, _ := setupRound3AccountTestEnv(t)

	// 1. Round-Robin: OpenAI Turn-1 [system, user] must have isContinuation=false and distinct fp: hashes,
	// distributing across all 3 accounts instead of colliding on the shared system prompt.
	if _, err := am.UpdateRoutingConfig(AccountPoolConfig{Strategy: RouteStrategyRoundRobin}); err != nil {
		t.Fatalf("set round-robin: %v", err)
	}
	seenRR := map[string]int{}
	for i := 0; i < 6; i++ {
		req := makeOpenAITurn1Req("brand-new-question-" + strconv.Itoa(i))
		_, sk, isCont, isNonUp, invPath, _, mal, _, rErr := extractRequestRoutingHintsFull(req)
		if rErr != nil || mal || invPath || isNonUp {
			t.Fatalf("unexpected hint parse error on turn 1 #%d: err=%v mal=%v inv=%v", i, rErr, mal, invPath)
		}
		if isCont {
			t.Fatalf("OpenAI Turn-1 [system, user] must NOT be classified as continuation (got sk=%q)", sk)
		}
		pick := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: sk, IsContinuation: isCont})
		if pick.DenyErr != nil {
			t.Fatalf("RR pick #%d failed: %v", i, pick.DenyErr)
		}
		seenRR[pick.Account.ID]++
	}
	for _, id := range []string{"primary", "acct-b", "acct-c"} {
		if seenRR[id] != 2 {
			t.Fatalf("expected round-robin 2 hits per account across 6 Turn-1 requests, got %+v", seenRR)
		}
	}

	// 2. Fill-First (Priority): acct-b has priority 10 (< acct-c 50 < primary 100)
	if _, err := am.UpdateRoutingConfig(AccountPoolConfig{Strategy: RouteStrategyFillFirst}); err != nil {
		t.Fatalf("set fill-first: %v", err)
	}
	reqFF := makeOpenAITurn1Req("fill-first-check")
	_, skFF, isContFF, _, _, _, _, _, _ := extractRequestRoutingHintsFull(reqFF)
	pickFF := am.ResolveTargetAccountEx(reqFF, "gemini-4-neon", ResolveOptions{SessionKey: skFF, IsContinuation: isContFF})
	if pickFF.DenyErr != nil || pickFF.Account.ID != "acct-b" {
		t.Fatalf("expected fill-first to pick priority-10 acct-b, got %q err=%v", pickFF.Account.ID, pickFF.DenyErr)
	}

	// Verify transitive strict weak ordering on 2% discrete quota buckets
	cands := []candidateScore{
		{acc: AccountProfile{ID: "c1"}, priority: 10, remQuota: 0.804, inFlight: 1, configOrd: 2},
		{acc: AccountProfile{ID: "c2"}, priority: 10, remQuota: 0.818, inFlight: 0, configOrd: 1}, // same bucket 40, lower inFlight wins
		{acc: AccountProfile{ID: "c3"}, priority: 10, remQuota: 0.850, inFlight: 3, configOrd: 0}, // bucket 42 wins
	}
	cands = sortCandidatesFillFirst(cands)
	if cands[0].acc.ID != "c3" || cands[1].acc.ID != "c2" || cands[2].acc.ID != "c1" {
		t.Fatalf("unexpected fill-first bucket sort order: %s, %s, %s", cands[0].acc.ID, cands[1].acc.ID, cands[2].acc.ID)
	}

	// 3. Smooth Weighted Round-Robin (weights: primary=1, acct-b=2, acct-c=5 -> sum=8)
	if _, err := am.UpdateRoutingConfig(AccountPoolConfig{Strategy: RouteStrategyWeightedRR}); err != nil {
		t.Fatalf("set weighted-rr: %v", err)
	}
	am.ResetRoutingWeights()
	seenW := map[string]int{}
	for i := 0; i < 8; i++ {
		req := makeOpenAITurn1Req("wrr-req-" + strconv.Itoa(i))
		_, sk, _, _, _, _, _, _, _ := extractRequestRoutingHintsFull(req)
		pick := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: sk})
		if pick.DenyErr != nil {
			t.Fatalf("WRR pick #%d failed: %v", i, pick.DenyErr)
		}
		seenW[pick.Account.ID]++
	}
	if seenW["primary"] != 1 || seenW["acct-b"] != 2 || seenW["acct-c"] != 5 {
		t.Fatalf("expected exact SWRR distribution {primary:1, acct-b:2, acct-c:5}, got %+v", seenW)
	}

	// 4. Least-Load: hold inFlight on primary and acct-b -> acct-c has 0 load and wins
	if _, err := am.UpdateRoutingConfig(AccountPoolConfig{Strategy: RouteStrategyLeastLoad}); err != nil {
		t.Fatalf("set least-load: %v", err)
	}
	relP := am.AcquireAccountInFlight("primary")
	relB1 := am.AcquireAccountInFlight("acct-b")
	relB2 := am.AcquireAccountInFlight("acct-b")
	reqLL := makeOpenAITurn1Req("least-load-req")
	_, skLL, _, _, _, _, _, _, _ := extractRequestRoutingHintsFull(reqLL)
	pickLL := am.ResolveTargetAccountEx(reqLL, "gemini-4-neon", ResolveOptions{SessionKey: skLL})
	relB2()
	relB1()
	relP()
	if pickLL.DenyErr != nil || pickLL.Account.ID != "acct-c" {
		t.Fatalf("expected least-load to pick unloaded acct-c, got %q err=%v", pickLL.Account.ID, pickLL.DenyErr)
	}

	// 5. Manual-Active with auto-failover ON vs strict ManualAutoFailover=false
	autoTrue := true
	if _, err := am.UpdateRoutingConfig(AccountPoolConfig{
		Strategy:           RouteStrategyManualActive,
		ActiveAccountID:    "acct-b",
		ManualAutoFailover: &autoTrue,
	}); err != nil {
		t.Fatalf("set manual-active: %v", err)
	}
	am.RecordAccountOutcome("acct-b", "gemini-4-neon", http.StatusTooManyRequests, "rate_limit_error", "60", "429 quota")
	reqMA1 := makeOpenAITurn1Req("manual-failover-on")
	_, skMA1, _, _, _, _, _, _, _ := extractRequestRoutingHintsFull(reqMA1)
	pickMA1 := am.ResolveTargetAccountEx(reqMA1, "gemini-4-neon", ResolveOptions{SessionKey: skMA1})
	if pickMA1.DenyErr != nil || pickMA1.Account.ID != "acct-c" {
		t.Fatalf("expected manual-active with failover=true to fall back to priority-50 acct-c, got %q err=%v", pickMA1.Account.ID, pickMA1.DenyErr)
	}

	autoFalse := false
	if _, err := am.UpdateRoutingConfig(AccountPoolConfig{
		Strategy:           RouteStrategyManualActive,
		ActiveAccountID:    "acct-b",
		ManualAutoFailover: &autoFalse,
	}); err != nil {
		t.Fatalf("set strict manual-active: %v", err)
	}
	reqMA2 := makeOpenAITurn1Req("manual-failover-off")
	_, skMA2, _, _, _, _, _, _, _ := extractRequestRoutingHintsFull(reqMA2)
	pickMA2 := am.ResolveTargetAccountEx(reqMA2, "gemini-4-neon", ResolveOptions{SessionKey: skMA2})
	if pickMA2.DenyErr == nil || pickMA2.StatusCode != http.StatusTooManyRequests || pickMA2.RetryAfterSec <= 0 {
		t.Fatalf("expected strict manual-active (ManualAutoFailover=false) on cooling acct-b to fail closed with 429 + Retry-After, got %+v", pickMA2)
	}
	am.ClearAccountCooldown("acct-b")
}

func TestRound3_GroupsSuffixesCrossHeaderGuardAndDashboardKeyMasking(t *testing.T) {
	am, _ := setupRound3AccountTestEnv(t)

	// Assign acct-b and acct-c to group "vip-pool"
	for _, id := range []string{"acct-b", "acct-c"} {
		acc, _ := am.GetAccount(id)
		acc.Groups = []string{"vip-pool"}
		if _, err := am.UpsertAccount(acc); err != nil {
			t.Fatalf("assign %s to vip-pool: %v", id, err)
		}
	}
	if _, err := am.UpsertGroup(AccountGroup{
		ID:            "vip-pool",
		Label:         "VIP Channel Pool",
		Strategy:      RouteStrategyRoundRobin,
		BoundKeys:     []string{"sk-vip-pool-secret99"},
		AllowedModels: []string{"gemini-4-neon", "gemini-3.8-flash"},
	}); err != nil {
		t.Fatalf("upsert vip-pool: %v", err)
	}

	// Setting non-member primary as vip-pool active account must fail validation
	if _, err := am.PerformAccountActionEx(AccountActionRequest{
		AccountID: "primary",
		GroupID:   "vip-pool",
		Action:    "set_active",
	}); err == nil {
		t.Fatal("expected set_active of non-member primary on vip-pool to return error")
	}

	// Verify keyMatches accepts @acct:<id>, #acct:<id>, @group:<id>, #group:<id>, and group boundKey
	for _, k := range []string{
		"master-r3-key@acct:acct-b",
		"master-r3-key#acct:acct-c",
		"master-r3-key@group:vip-pool",
		"master-r3-key#group:vip-pool",
		"sk-vip-pool-secret99",
	} {
		if !keyMatches(k) {
			t.Fatalf("expected keyMatches(%q) == true", k)
		}
	}

	// Verify group routing restricts selection to {acct-b, acct-c} and enforces group AllowedModels
	reqGrp := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"gemini-4-neon","messages":[{"role":"user","content":"hi"}]}`))
	reqGrp.Header.Set("Authorization", "Bearer sk-vip-pool-secret99")
	pickGrp := am.ResolveTargetAccountEx(reqGrp, "gemini-4-neon", ResolveOptions{SessionKey: "fp:grp1"})
	if pickGrp.DenyErr != nil || (pickGrp.Account.ID != "acct-b" && pickGrp.Account.ID != "acct-c") || pickGrp.GroupID != "vip-pool" {
		t.Fatalf("expected vip-pool route to acct-b/c, got acc=%q grp=%q err=%v", pickGrp.Account.ID, pickGrp.GroupID, pickGrp.DenyErr)
	}

	pickDisallowed := am.ResolveTargetAccountEx(reqGrp, "claude-opus-5-5", ResolveOptions{SessionKey: "fp:grp2"})
	if pickDisallowed.DenyErr == nil || pickDisallowed.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 when requesting model outside group allowlist, got %+v", pickDisallowed)
	}

	// Cross-header conflict: X-Agy-Account: primary + X-Agy-Group: vip-pool -> 403
	reqConflict := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"gemini-4-neon","messages":[{"role":"user","content":"hi"}]}`))
	reqConflict.Header.Set("Authorization", "Bearer master-r3-key")
	reqConflict.Header.Set("X-Agy-Account", "primary")
	reqConflict.Header.Set("X-Agy-Group", "vip-pool")
	pickConflict := am.ResolveTargetAccountEx(reqConflict, "gemini-4-neon", ResolveOptions{})
	if pickConflict.DenyErr == nil || pickConflict.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 when X-Agy-Account is not a member of X-Agy-Group, got %+v", pickConflict)
	}

	// Verify /api/dashboard masks group boundKeys ("sk-vip-pool-secret99" must NEVER appear in plaintext)
	gs := NewGatewayServer()
	defer gs.Close()
	reqDash := httptest.NewRequest(http.MethodGet, "/api/dashboard?local_only=1", nil)
	reqDash.Header.Set("x-api-key", "master-r3-key")
	recDash := httptest.NewRecorder()
	gs.ServeHTTP(recDash, reqDash)
	if recDash.Code != http.StatusOK {
		t.Fatalf("expected 200 from /api/dashboard, got %d", recDash.Code)
	}
	if strings.Contains(recDash.Body.String(), "sk-vip-pool-secret99") {
		t.Fatalf("/api/dashboard leaked plaintext group boundKey: %s", recDash.Body.String())
	}
}

func mountMockChildWorker(t *testing.T, am *AccountManager, accountID string, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(handler)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse mock server url: %v", err)
	}
	_, portStr, err := net.SplitHostPort(u.Host)
	if err != nil {
		t.Fatalf("split host port: %v", err)
	}
	port, _ := strconv.Atoi(portStr)
	tr := &http.Transport{}
	proc := &accountWorkerProc{
		id:        accountID,
		gwPort:    port,
		dashPort:  port,
		transport: tr,
		done:      make(chan struct{}),
		readyCh:   make(chan struct{}),
		healthyAt: time.Now(),
	}
	proc.finishReady(nil)
	am.mu.Lock()
	am.workers[accountID] = proc
	am.mu.Unlock()
	t.Cleanup(func() {
		tr.CloseIdleConnections()
		srv.Close()
	})
	return srv
}

func TestRound3_PreStreamFirstTurnFailoverAndStrictU8ContinuationPinning(t *testing.T) {
	am, _ := setupRound3AccountTestEnv(t)

	// Put acct-b (priority 10) and acct-c (priority 50) into group "ha-grp" with fill-first
	for _, id := range []string{"acct-b", "acct-c"} {
		acc, _ := am.GetAccount(id)
		acc.Groups = []string{"ha-grp"}
		if _, err := am.UpsertAccount(acc); err != nil {
			t.Fatalf("upsert %s: %v", id, err)
		}
	}
	if _, err := am.UpsertGroup(AccountGroup{
		ID:       "ha-grp",
		Label:    "HA Failover Pool",
		Strategy: RouteStrategyFillFirst,
	}); err != nil {
		t.Fatalf("upsert ha-grp: %v", err)
	}

	var hitsB, hitsC atomic.Int32
	mountMockChildWorker(t, am, "acct-b", func(w http.ResponseWriter, r *http.Request) {
		hitsB.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "45")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"type":"error","error":{"type":"rate_limit_error","message":"429 rate limit on acct-b"}}`))
	})
	mountMockChildWorker(t, am, "acct-c", func(w http.ResponseWriter, r *http.Request) {
		hitsC.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"chatcmpl-ok","choices":[{"message":{"role":"assistant","content":"Hello from acct-c"}}]}`))
	})

	// 1. Brand-new Turn-1 request [system, user_1]: hits acct-b (429) -> pre-stream hops to acct-c (200 OK)!
	turn1Body := `{"model":"gemini-4-neon","messages":[{"role":"system","content":"Sys"},{"role":"user","content":"Hello world turn 1"}]}`
	req1 := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(turn1Body))
	req1.Header.Set("Content-Type", "application/json")
	req1.Header.Set("Authorization", "Bearer master-r3-key")
	req1.Header.Set("X-Agy-Group", "ha-grp")
	rec1 := httptest.NewRecorder()

	_, _, finish1, handled1 := am.TryRouteAPIRequestWithFinish(rec1, req1)
	finish1()
	if !handled1 || rec1.Code != http.StatusOK {
		t.Fatalf("expected Turn-1 pre-stream failover to succeed with 200 OK, got handled=%v code=%d body=%s", handled1, rec1.Code, rec1.Body.String())
	}
	if rec1.Header().Get("X-Agy-Account") != "acct-c" || rec1.Header().Get("X-Agy-Failover-Count") != "1" {
		t.Fatalf("expected X-Agy-Account=acct-c and X-Agy-Failover-Count=1, got acc=%q fo=%q", rec1.Header().Get("X-Agy-Account"), rec1.Header().Get("X-Agy-Failover-Count"))
	}
	if hitsB.Load() != 1 || hitsC.Load() != 1 {
		t.Fatalf("expected 1 attempt on acct-b and 1 attempt on acct-c, got B=%d C=%d", hitsB.Load(), hitsC.Load())
	}
	hvB := am.BuildAccountHealthView(AccountProfile{ID: "acct-b", Enabled: true}, true, 0, time.Now())
	if hvB.FailoverOutCount != 1 || hvB.Error429Count != 1 || hvB.CooldownSec < 30 {
		t.Fatalf("expected acct-b health telemetry to record failoverOut=1, err429=1, cooldown>=30s, got %+v", hvB)
	}

	// 2. Decision U8 Multi-Turn Continuation Pinning:
	// Clear acct-b cooldown so acct-b is healthy & higher priority, AND put acct-c into draining + 429 cooldown!
	am.ClearAccountCooldown("acct-b")
	if _, err := am.PerformAccountActionEx(AccountActionRequest{AccountID: "acct-c", Action: "drain"}); err != nil {
		t.Fatalf("drain acct-c: %v", err)
	}
	am.RecordAccountOutcome("acct-c", "gemini-4-neon", http.StatusTooManyRequests, "rate_limit_error", "30", "transient 429")

	// Turn-2 carries [system, user_1, assistant_1, user_2] with NO explicit session header (pure fp: continuity).
	turn2Body := `{"model":"gemini-4-neon","messages":[{"role":"system","content":"Sys"},{"role":"user","content":"Hello world turn 1"},{"role":"assistant","content":"Hello from acct-c"},{"role":"user","content":"Follow-up turn 2"}]}`
	req2 := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(turn2Body))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("Authorization", "Bearer master-r3-key")
	req2.Header.Set("X-Agy-Group", "ha-grp")
	rec2 := httptest.NewRecorder()

	_, _, finish2, handled2 := am.TryRouteAPIRequestWithFinish(rec2, req2)
	finish2()
	if !handled2 || rec2.Code != http.StatusOK {
		t.Fatalf("expected Turn-2 continuation on draining acct-c to stay pinned and return 200, got code=%d body=%s", rec2.Code, rec2.Body.String())
	}
	if rec2.Header().Get("X-Agy-Account") != "acct-c" || hitsB.Load() != 1 || hitsC.Load() != 2 {
		t.Fatalf("U8 VIOLATION: Turn-2 continuation must stay pinned to acct-c without touching acct-b; got acc=%q hitsB=%d hitsC=%d", rec2.Header().Get("X-Agy-Account"), hitsB.Load(), hitsC.Load())
	}

	// Hard-block acct-c with 401 auth_error -> Turn-3 continuation must fail closed (503) rather than hopping to acct-b!
	am.RecordAccountOutcome("acct-c", "gemini-4-neon", http.StatusUnauthorized, "authentication_error", "", "OAuth token revoked")
	req3 := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(turn2Body))
	req3.Header.Set("Content-Type", "application/json")
	req3.Header.Set("Authorization", "Bearer master-r3-key")
	req3.Header.Set("X-Agy-Group", "ha-grp")
	rec3 := httptest.NewRecorder()
	_, _, finish3, handled3 := am.TryRouteAPIRequestWithFinish(rec3, req3)
	finish3()
	if !handled3 || rec3.Code != http.StatusServiceUnavailable || hitsB.Load() != 1 {
		t.Fatalf("expected hard-blocked U8 continuation to return 503 without replaying to acct-b, got code=%d hitsB=%d", rec3.Code, hitsB.Load())
	}
}

func TestRound3_RotateDeadlockRegressionSecurityAndStreamTailIsolation(t *testing.T) {
	am, _ := setupRound3AccountTestEnv(t)

	// 1. P0 #1 Regression: rotate_next & drain_and_rotate must never self-deadlock on RWMutex + HasOAuthToken
	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		res1, err1 := am.PerformAccountActionEx(AccountActionRequest{
			Action:         "rotate_next",
			SwitchStrategy: true,
		})
		if err1 != nil || res1["activeAccountId"] != "acct-b" {
			t.Errorf("rotate_next #1 unexpected: res=%+v err=%v", res1, err1)
			return
		}
		res2, err2 := am.PerformAccountActionEx(AccountActionRequest{
			Action:         "drain_and_rotate",
			SwitchStrategy: true,
			DrainPrevious:  true,
		})
		if err2 != nil || res2["activeAccountId"] != "acct-c" || res2["drainedAccountId"] != "acct-b" {
			t.Errorf("drain_and_rotate #2 unexpected: res=%+v err=%v", res2, err2)
			return
		}
	}()
	select {
	case <-doneCh:
	case <-time.After(2 * time.Second):
		t.Fatal("DEADLOCK detected in PerformAccountActionEx rotate_next / drain_and_rotate")
	}
	accB, _ := am.GetAccount("acct-b")
	if accB.EffectiveSchedState() != SchedStateDraining {
		t.Fatalf("expected acct-b to be in draining state after drain_and_rotate, got %q", accB.EffectiveSchedState())
	}

	// 2. P2 #8a & Non-Upstream Isolation: 529 must NOT shorten an active 429 cooldown, and count_tokens must NOT clear it
	am.RecordAccountOutcome("primary", "gemini-3.8-flash", http.StatusTooManyRequests, "rate_limit_error", "120", "429 quota")
	am.RecordAccountOutcome("primary", "gemini-3.8-flash", 529, "overloaded_error", "", "529 overloaded")
	coolBefore, _, remSecBefore := am.isAccountFamilyCoolingDown("primary", "gemini-3.8-flash", time.Now())
	if !coolBefore || remSecBefore < 100 {
		t.Fatalf("expected 529 to preserve longer 120s 429 cooldown, got cooling=%v rem=%ds", coolBefore, remSecBefore)
	}

	gs := NewGatewayServer()
	defer gs.Close()
	reqCT := httptest.NewRequest(http.MethodPost, "/v1beta/models/gemini-3.8-flash:countTokens", strings.NewReader(`{"contents":[{"role":"user","parts":[{"text":"hello"}]}]}`))
	reqCT.Header.Set("Content-Type", "application/json")
	reqCT.Header.Set("x-goog-api-key", "master-r3-key")
	reqCT.Header.Set("X-Agy-Account", "primary")
	recCT := httptest.NewRecorder()
	gs.ServeHTTP(recCT, reqCT)
	if recCT.Code != http.StatusOK {
		t.Fatalf("expected 200 from :countTokens, got %d body=%s", recCT.Code, recCT.Body.String())
	}
	coolAfter, _, remSecAfter := am.isAccountFamilyCoolingDown("primary", "gemini-3.8-flash", time.Now())
	if !coolAfter || remSecAfter < 95 {
		t.Fatalf("non-upstream :countTokens 200 OK wiped active 429 cooldown: cooling=%v rem=%ds", coolAfter, remSecAfter)
	}

	// 3. Gemini Colon-Stuffing Model Allowlist Bypass Regression -> 400 invalid_model_path
	reqColon := httptest.NewRequest(http.MethodPost, "/v1beta/models/gemini-3.8-flash:claude-opus-5-5:generateContent", strings.NewReader(`{"contents":[{"parts":[{"text":"hi"}]}]}`))
	reqColon.Header.Set("Content-Type", "application/json")
	reqColon.Header.Set("x-goog-api-key", "master-r3-key")
	recColon := httptest.NewRecorder()
	gs.ServeHTTP(recColon, reqColon)
	if recColon.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 BadRequest on colon-stuffed Gemini path, got %d body=%s", recColon.Code, recColon.Body.String())
	}

	// 4. P1 #4 Regression: inspectStreamTailForUpstreamError false-positive immunity vs genuine SSE error frame
	benignSSE := []byte("data: {\"id\":\"1\",\"choices\":[{\"delta\":{\"content\":\"How to handle 429 RESOURCE_EXHAUSTED, rate_limit_error, 3501, and tos_circuit_breaker in production.\"}}]}\n\ndata: [DONE]\n\n")
	if failed, sc, et, _ := inspectStreamTailForUpstreamError(benignSSE); failed {
		t.Fatalf("inspectStreamTailForUpstreamError triggered false positive on normal model content: sc=%d type=%q", sc, et)
	}
	realErrSSE := []byte("event: message_start\ndata: {\"type\":\"message_start\"}\n\nevent: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"rate_limit_error\",\"message\":\"429 RESOURCE_EXHAUSTED upstream\"}}\n\n")
	if failed, sc, et, _ := inspectStreamTailForUpstreamError(realErrSSE); !failed || sc != http.StatusTooManyRequests || et != "rate_limit_error" {
		t.Fatalf("expected real SSE error frame to classify as 429 rate_limit_error, got failed=%v sc=%d et=%q", failed, sc, et)
	}

	// 5. Proxy Username-Only Secret Redaction & Prometheus Metrics Escaping
	redLog := gwcfg.RedactSecretsInString("dial http://secretUserToken@10.0.0.2:8080 and socks5://u1:pw2@10.0.0.3:1080 failed")
	if strings.Contains(redLog, "secretUserToken") || strings.Contains(redLog, "pw2") {
		t.Fatalf("RedactSecretsInString leaked proxy userinfo: %q", redLog)
	}
	if masked := maskProxyURL("http://onlyTokenCred@proxy.internal:8080"); strings.Contains(masked, "onlyTokenCred") {
		t.Fatalf("maskProxyURL leaked username-only credential: %q", masked)
	}
	reqProm := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	reqProm.Header.Set("x-api-key", "master-r3-key")
	recProm := httptest.NewRecorder()
	gs.ServeHTTP(recProm, reqProm)
	prom := recProm.Body.String()
	if !strings.Contains(prom, "agy_account_inflight{") || !strings.Contains(prom, "agy_account_failovers_total{") {
		t.Fatalf("expected agy_account_* metrics in /metrics output, got:\n%s", prom)
	}
	if got := promQuote("a\"b\\c\nd\r"); got != `"a\"b\\c\nd"` {
		t.Fatalf("unexpected promQuote output: %s", got)
	}
}

func TestRound3_UnauthenticatedLoopbackDashboardSameOriginMutations(t *testing.T) {
	oldKeys, oldErr := apiKeys, authConfigErr
	t.Cleanup(func() {
		GlobalAccountManager.StopAll()
		GlobalAccountManager = NewAccountManager()
		resetPeerDashboardCacheForTest()
		initConfig()
		apiKeys, authConfigErr = oldKeys, oldErr
	})

	tmp := t.TempDir()
	t.Setenv("AGY_API_KEYS", "")
	t.Setenv("AGY_API_KEYS_FILE", "")
	t.Setenv("AGY_CORS_ORIGINS", "")
	t.Setenv("AGY_ACCOUNTS_FILE", filepath.Join(tmp, "accounts.json"))
	t.Setenv("AGY_PURE_API_HOME", filepath.Join(tmp, "primary-home"))
	initConfig()
	apiKeys = loadAPIKeys()

	GlobalAccountManager.StopAll()
	GlobalAccountManager = NewAccountManager()
	GlobalAccountManager.Init()
	gs := NewGatewayServer()
	t.Cleanup(gs.Close)
	dash := gs.DashboardOnlyHandler()
	// 1. Same-origin POST /api/accounts from loopback dashboard port succeeds without AGY_API_KEYS.
	reqOK := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18891/api/accounts", strings.NewReader(`{"id":"loop-a","label":"Loop A","enabled":true,"priority":10,"weight":100}`))
	reqOK.RemoteAddr = "127.0.0.1:54321"
	reqOK.Host = "127.0.0.1:18891"
	reqOK.Header.Set("Content-Type", "application/json")
	reqOK.Header.Set("Origin", "http://127.0.0.1:18891")
	reqOK.Header.Set("Sec-Fetch-Site", "same-origin")
	reqOK.Header.Set("X-Agy-Requested-With", "dashboard")
	recOK := httptest.NewRecorder()
	dash.ServeHTTP(recOK, reqOK)
	if recOK.Code != http.StatusOK {
		t.Fatalf("expected 200 for same-origin loopback dashboard POST /api/accounts, got %d body=%s", recOK.Code, recOK.Body.String())
	}

	// 2. Cross-port loopback Origin (e.g. untrusted app on :3000) is still blocked with 403.
	reqCrossPort := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18891/api/accounts", strings.NewReader(`{"id":"loop-b","label":"Loop B"}`))
	reqCrossPort.RemoteAddr = "127.0.0.1:54322"
	reqCrossPort.Host = "127.0.0.1:18891"
	reqCrossPort.Header.Set("Content-Type", "application/json")
	reqCrossPort.Header.Set("Origin", "http://127.0.0.1:3000")
	reqCrossPort.Header.Set("X-Agy-Requested-With", "dashboard")
	recCrossPort := httptest.NewRecorder()
	dash.ServeHTTP(recCrossPort, reqCrossPort)
	if recCrossPort.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for cross-port loopback Origin, got %d body=%s", recCrossPort.Code, recCrossPort.Body.String())
	}

	// 3. X-Forwarded-Host spoofing on unauthenticated loopback cannot bypass Origin check.
	reqSpoof := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18891/api/accounts", strings.NewReader(`{"id":"loop-c","label":"Loop C"}`))
	reqSpoof.RemoteAddr = "127.0.0.1:54323"
	reqSpoof.Host = "127.0.0.1:18891"
	reqSpoof.Header.Set("Content-Type", "application/json")
	reqSpoof.Header.Set("Origin", "https://evil.example")
	reqSpoof.Header.Set("X-Forwarded-Host", "evil.example")
	reqSpoof.Header.Set("X-Agy-Requested-With", "dashboard")
	recSpoof := httptest.NewRecorder()
	dash.ServeHTTP(recSpoof, reqSpoof)
	if recSpoof.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for X-Forwarded-Host spoof on unauthenticated loopback, got %d body=%s", recSpoof.Code, recSpoof.Body.String())
	}

	// 4. AGY_CORS_ORIGINS must ONLY apply to data-plane (/v1/*, /v1beta/*), never :18891 admin endpoints.
	origCors := AllowedCorsOrigins
	AllowedCorsOrigins = map[string]bool{"https://app.example.com": true}
	defer func() { AllowedCorsOrigins = origCors }()

	gwOnly := gs.GatewayOnlyHandler()
	reqDataCORS := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:18890/v1/models", nil)
	reqDataCORS.RemoteAddr = "127.0.0.1:54324"
	reqDataCORS.Host = "127.0.0.1:18890"
	reqDataCORS.Header.Set("Origin", "https://app.example.com")
	recDataCORS := httptest.NewRecorder()
	gwOnly.ServeHTTP(recDataCORS, reqDataCORS)
	if recDataCORS.Code != http.StatusOK || recDataCORS.Header().Get("Access-Control-Allow-Origin") != "https://app.example.com" {
		t.Fatalf("expected 200 + CORS header on data-plane /v1/models, got %d hdr=%q", recDataCORS.Code, recDataCORS.Header().Get("Access-Control-Allow-Origin"))
	}

	for _, adminPath := range []string{"/api/accounts", "/api/dashboard", "/metrics"} {
		reqAdminCORS := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:18891"+adminPath, nil)
		reqAdminCORS.RemoteAddr = "127.0.0.1:54325"
		reqAdminCORS.Host = "127.0.0.1:18891"
		reqAdminCORS.Header.Set("Origin", "https://app.example.com")
		recAdminCORS := httptest.NewRecorder()
		dash.ServeHTTP(recAdminCORS, reqAdminCORS)
		if recAdminCORS.Code != http.StatusForbidden || recAdminCORS.Header().Get("Access-Control-Allow-Origin") != "" {
			t.Fatalf("expected 403 and empty CORS header on admin %s, got %d hdr=%q", adminPath, recAdminCORS.Code, recAdminCORS.Header().Get("Access-Control-Allow-Origin"))
		}
	}

	// 5. Duplicate Origin headers are rejected on both loopback and sameOrigin checks.
	reqDup := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18891/api/accounts", strings.NewReader(`{"id":"loop-dup","label":"Dup"}`))
	reqDup.RemoteAddr = "127.0.0.1:54326"
	reqDup.Host = "127.0.0.1:18891"
	reqDup.Header.Set("Content-Type", "application/json")
	reqDup.Header.Add("Origin", "http://127.0.0.1:18891")
	reqDup.Header.Add("Origin", "https://evil.example")
	reqDup.Header.Set("X-Agy-Requested-With", "dashboard")
	recDup := httptest.NewRecorder()
	dash.ServeHTTP(recDup, reqDup)
	if recDup.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for duplicate Origin headers, got %d", recDup.Code)
	}

	// 6. X-Pi-Session-Id is recognized for sticky multi-account session affinity.
	reqPi := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	reqPi.Header.Set("X-Pi-Session-Id", "pi-sess-42")
	if gotKey := extractSessionKeyFromHeaders(reqPi); gotKey != "hdr:pi-sess-42" {
		t.Fatalf("expected hdr:pi-sess-42 from X-Pi-Session-Id, got %q", gotKey)
	}
}

func TestRound3_SinglePrimaryDegradeOpenAfterLicense3501And200Recovery(t *testing.T) {
	oldKeys, oldErr := apiKeys, authConfigErr
	t.Cleanup(func() {
		GlobalAccountManager.StopAll()
		GlobalAccountManager = NewAccountManager()
		resetPeerDashboardCacheForTest()
		initConfig()
		apiKeys, authConfigErr = oldKeys, oldErr
	})

	tmp := t.TempDir()
	t.Setenv("AGY_API_KEYS", "")
	t.Setenv("AGY_ACCOUNTS_FILE", filepath.Join(tmp, "accounts.json"))
	t.Setenv("AGY_TOS_TRIPPED_FILE", filepath.Join(tmp, "tos-tripped"))
	initConfig()
	apiKeys = loadAPIKeys()

	am := NewAccountManager()
	am.Init()
	GlobalAccountManager = am

	// Record a #3501 license error + 529 overload on single-account primary
	am.RecordAccountOutcome("primary", "gemini-3.8-flash", http.StatusForbidden, "permission_error", "", "You do not have a valid license to use Gemini Code Assist. (#3501)")
	am.RecordAccountOutcome("primary", "gemini-3.8-flash", 529, "overloaded_error", "", "529 overloaded")

	hvErr := am.BuildAccountHealthView(AccountProfile{ID: "primary", Primary: true, Enabled: true}, true, 0, time.Now())
	if hvErr.State != "auth_error" {
		t.Fatalf("expected telemetry state=auth_error after #3501, got %+v", hvErr)
	}

	// Single-primary fast path AND full ResolveTargetAccountEx must NOT self-lockout with 503/429
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"gemini-3.8-flash","messages":[{"role":"user","content":"hi"}]}`))
	if id, fast := am.isSimpleSinglePrimary(req); !fast || id != "primary" {
		t.Fatalf("expected single-primary fast path to stay open after #3501, got id=%q fast=%v", id, fast)
	}
	pick := am.ResolveTargetAccountEx(req, "gemini-3.8-flash", ResolveOptions{SessionKey: "hdr:s1"})
	if pick.DenyErr != nil || pick.Account.ID != "primary" {
		t.Fatalf("expected ResolveTargetAccountEx to degrade-open on single-primary, got %+v", pick)
	}

	// A subsequent 200 OK upstream completion must clear authErrorUntil immediately
	am.RecordAccountOutcome("primary", "gemini-3.8-flash", http.StatusOK, "", "", "")
	hvOK := am.BuildAccountHealthView(AccountProfile{ID: "primary", Primary: true, Enabled: true}, true, 0, time.Now())
	if hvOK.State != "healthy" || hvOK.CooldownSec != 0 {
		t.Fatalf("expected 200 OK to clear auth_error and cooldown immediately, got %+v", hvOK)
	}
}

func TestRound3_PrimaryDisableActionUpsertPersistenceEngineStopAndRouting(t *testing.T) {
	am, _ := setupRound3AccountTestEnv(t)
	t.Cleanup(func() {
		if GlobalEngine != nil {
			GlobalEngine.StopEngine()
		}
	})

	// Bind primary + acct-b into a group and set primary as active account for both global and group.
	for _, id := range []string{"primary", "acct-b"} {
		acc, _ := am.GetAccount(id)
		acc.Groups = []string{"team-a"}
		if _, err := am.UpsertAccount(acc); err != nil {
			t.Fatalf("assign %s to team-a: %v", id, err)
		}
	}
	if _, err := am.UpsertGroup(AccountGroup{
		ID:              "team-a",
		Label:           "Team A",
		Strategy:        RouteStrategyManualActive,
		ActiveAccountID: "primary",
	}); err != nil {
		t.Fatalf("upsert team-a: %v", err)
	}
	if _, err := am.UpdateRoutingConfig(AccountPoolConfig{
		Strategy:        RouteStrategyManualActive,
		ActiveAccountID: "primary",
	}); err != nil {
		t.Fatalf("set primary active: %v", err)
	}

	// Pin a sticky session on primary and arm a sentinel stdinCloser on GlobalEngine to verify StopEngine().
	reqSeed := makeOpenAITurn1Req("sticky-on-primary")
	pickSeed := am.ResolveTargetAccountEx(reqSeed, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:prim-sticky"})
	if pickSeed.DenyErr != nil || pickSeed.Account.ID != "primary" {
		t.Fatalf("expected initial route to primary, got %+v", pickSeed)
	}

	armEngineSentinel := func() (stopped func() bool) {
		rPipe, wPipe, err := os.Pipe()
		if err != nil {
			t.Fatalf("os.Pipe: %v", err)
		}
		t.Cleanup(func() { _ = rPipe.Close(); _ = wPipe.Close() })
		GlobalEngine.mu.Lock()
		GlobalEngine.stdinCloser = wPipe
		GlobalEngine.mu.Unlock()
		return func() bool {
			GlobalEngine.mu.Lock()
			cleared := GlobalEngine.stdinCloser == nil
			GlobalEngine.mu.Unlock()
			return cleared
		}
	}

	// 1. Disable primary via PerformAccountActionEx("disable")
	wasStopped := armEngineSentinel()
	res, err := am.PerformAccountActionEx(AccountActionRequest{AccountID: "primary", Action: "disable"})
	if err != nil {
		t.Fatalf("disable primary via action failed: %v", err)
	}
	if res["schedState"] != SchedStateDisabled {
		t.Fatalf("expected schedState=disabled, got %+v", res)
	}
	if !wasStopped() {
		t.Fatal("expected disabling primary via action to invoke GlobalEngine.StopEngine()")
	}
	prim, ok := am.GetPrimaryAccount()
	if !ok || prim.Enabled || prim.EffectiveSchedState() != SchedStateDisabled {
		t.Fatalf("expected primary disabled in memory, got %+v", prim)
	}
	if gotActive := am.GetRoutingConfig().ActiveAccountID; gotActive == "primary" || gotActive == "" {
		t.Fatalf("expected global ActiveAccountID rotated away from primary, got %q", gotActive)
	}
	grp, _ := am.GetGroup("team-a")
	if grp.ActiveAccountID != "acct-b" {
		t.Fatalf("expected group team-a ActiveAccountID rotated to acct-b, got %q", grp.ActiveAccountID)
	}

	// 2. Verify GET /api/accounts reports primary running=false, enabled=false, schedState=disabled, health.state=disabled
	recAPI := httptest.NewRecorder()
	reqAPI := httptest.NewRequest(http.MethodGet, "/api/accounts", nil)
	HandleAccountsAPI(recAPI, reqAPI)
	if recAPI.Code != http.StatusOK {
		t.Fatalf("GET /api/accounts returned %d", recAPI.Code)
	}
	var apiPayload struct {
		Accounts []struct {
			ID         string            `json:"id"`
			Primary    bool              `json:"primary"`
			Enabled    bool              `json:"enabled"`
			Running    bool              `json:"running"`
			SchedState string            `json:"schedState"`
			Health     AccountHealthView `json:"health"`
		} `json:"accounts"`
	}
	if err := json.Unmarshal(recAPI.Body.Bytes(), &apiPayload); err != nil {
		t.Fatalf("decode /api/accounts: %v", err)
	}
	foundPrimAPI := false
	for _, a := range apiPayload.Accounts {
		if a.ID == "primary" {
			foundPrimAPI = true
			if a.Running || a.Enabled || a.SchedState != SchedStateDisabled || a.Health.State != "disabled" {
				t.Fatalf("unexpected disabled primary API view: %+v", a)
			}
		}
	}
	if !foundPrimAPI {
		t.Fatal("primary missing from /api/accounts")
	}

	// 3. Reload accounts.json from disk into a fresh AccountManager -> disabled primary MUST persist
	reloaded := NewAccountManager()
	reloaded.Init()
	primReloaded, ok := reloaded.GetPrimaryAccount()
	if !ok || primReloaded.Enabled || primReloaded.EffectiveSchedState() != SchedStateDisabled {
		t.Fatalf("disabled primary did not persist across reload: %+v", primReloaded)
	}
	// Verify !hasPrimary fallback does NOT override SchedStateDisabled
	noPrimNorm := normalizeAccountsList([]AccountProfile{
		{ID: "solo", Primary: false, Enabled: false, SchedState: SchedStateDisabled},
	}, 2)
	if len(noPrimNorm) != 1 || !noPrimNorm[0].Primary || noPrimNorm[0].Enabled || noPrimNorm[0].SchedState != SchedStateDisabled {
		t.Fatalf("expected !hasPrimary fallback to preserve disabled state, got %+v", noPrimNorm)
	}

	// 4. Routing routes cleanly to remaining enabled accounts, and explicit X-Agy-Account: primary returns 403
	for i := 0; i < 4; i++ {
		req := makeOpenAITurn1Req("after-primary-disabled-" + strconv.Itoa(i))
		pick := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "fp:post-disable-" + strconv.Itoa(i)})
		if pick.DenyErr != nil || pick.Account.ID == "primary" || pick.Account.ID == "" {
			t.Fatalf("expected request #%d to route to non-primary enabled account, got %+v", i, pick)
		}
	}
	reqExp := makeOpenAITurn1Req("explicit-disabled-primary")
	reqExp.Header.Set("X-Agy-Account", "primary")
	pickExp := am.ResolveTargetAccountEx(reqExp, "gemini-4-neon", ResolveOptions{})
	if pickExp.DenyErr == nil || pickExp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 for explicit X-Agy-Account: primary when disabled, got %+v", pickExp)
	}

	// 5. DeleteAccount still blocks deleting primary
	if err := am.DeleteAccount("primary"); err == nil {
		t.Fatal("expected DeleteAccount(primary) to remain blocked")
	}

	// 6. Re-enable primary, then disable via UpsertAccount -> stops engine & rotates ActiveAccountID
	if _, err := am.PerformAccountActionEx(AccountActionRequest{AccountID: "primary", Action: "enable"}); err != nil {
		t.Fatalf("re-enable primary: %v", err)
	}
	if _, err := am.UpdateRoutingConfig(AccountPoolConfig{Strategy: RouteStrategyManualActive, ActiveAccountID: "primary"}); err != nil {
		t.Fatalf("re-set primary active: %v", err)
	}
	wasStopped2 := armEngineSentinel()
	primUp, _ := am.GetPrimaryAccount()
	primUp.Enabled = false
	primUp.SchedState = SchedStateDisabled
	if _, err := am.UpsertAccount(primUp); err != nil {
		t.Fatalf("UpsertAccount disable primary: %v", err)
	}
	if !wasStopped2() {
		t.Fatal("expected disabling primary via UpsertAccount to invoke GlobalEngine.StopEngine()")
	}
	if gotActive := am.GetRoutingConfig().ActiveAccountID; gotActive == "primary" {
		t.Fatalf("expected UpsertAccount disable to rotate ActiveAccountID away from primary, got %q", gotActive)
	}
}

func TestRound3_BannedSuspendedStateMachinePrecedenceAndQuotaErrorWiring(t *testing.T) {
	am, _ := setupRound3AccountTestEnv(t)
	now := time.Now()

	// 1. Permanent ban/suspension on draining primary MUST surface "tos_tripped" (never disguised as "draining")
	if _, err := am.PerformAccountActionEx(AccountActionRequest{AccountID: "primary", Action: "drain"}); err != nil {
		t.Fatalf("drain primary: %v", err)
	}
	am.RecordAccountOutcome("primary", "gemini-4-neon", http.StatusForbidden, "permission_denied", "", "Your account has been suspended due to a Terms of Service violation")
	primDraining, _ := am.GetPrimaryAccount()
	hvBannedDraining := am.BuildAccountHealthView(primDraining, true, 0, now)
	if hvBannedDraining.State != "tos_tripped" || !strings.Contains(strings.ToLower(hvBannedDraining.CooldownReason), "suspended") {
		t.Fatalf("expected draining+banned primary to surface tos_tripped, got %+v", hvBannedDraining)
	}
	// Permanent ban must NOT expire after 30 minutes (unlike transient auth_error)
	hvFuture := am.BuildAccountHealthView(primDraining, true, 0, now.Add(2*time.Hour))
	if hvFuture.State != "tos_tripped" {
		t.Fatalf("permanent ban must not silently expire after 2h, got %+v", hvFuture)
	}

	// 2. Draining + active auth_error must surface "auth_error" (not "draining")
	if _, err := am.PerformAccountActionEx(AccountActionRequest{AccountID: "acct-b", Action: "drain"}); err != nil {
		t.Fatalf("drain acct-b: %v", err)
	}
	am.RecordAccountOutcome("acct-b", "gemini-4-neon", http.StatusUnauthorized, "authentication_error", "", "invalid_grant: login required, please re-authenticate")
	accB, _ := am.GetAccount("acct-b")
	hvAuthDraining := am.BuildAccountHealthView(accB, true, 0, now)
	if hvAuthDraining.State != "auth_error" {
		t.Fatalf("expected draining+auth_error acct-b to surface auth_error, got %+v", hvAuthDraining)
	}

	// 3. Disabled account MUST ALWAYS report state="disabled" even when tosTripped / primaryTripped / auth_error are set,
	// while preserving diagnostic LastError and CooldownReason!
	primDisabled := primDraining
	primDisabled.Enabled = false
	primDisabled.SchedState = SchedStateDisabled
	hvDisabled := buildSingleHealthViewFromRuntime(primDisabled, true, 0, 0, &accountHealthRuntime{
		tosTripped:  true,
		tosReason:   "Account has been suspended",
		lastError:   "Account has been suspended",
		lastErrorAt: now,
	}, true, "ToS file tripped", now)
	if hvDisabled.State != "disabled" {
		t.Fatalf("expected disabled account to stay state=disabled, got %+v", hvDisabled)
	}
	if hvDisabled.LastError == "" || hvDisabled.CooldownReason == "" {
		t.Fatalf("expected disabled account to preserve diagnostic LastError/CooldownReason, got %+v", hvDisabled)
	}

	// 4. Explicit ClearAccountCooldown / enable clears permanent tosTripped
	if _, err := am.PerformAccountActionEx(AccountActionRequest{AccountID: "primary", Action: "enable"}); err != nil {
		t.Fatalf("enable primary: %v", err)
	}
	primActive, _ := am.GetPrimaryAccount()
	if hvCleared := am.BuildAccountHealthView(primActive, true, 0, now); hvCleared.State != "healthy" {
		t.Fatalf("expected enable to clear tosTripped and return healthy, got %+v", hvCleared)
	}

	// 5. Quota sync (/usage) error wiring: ban & auth errors classify into health state; transient errors do NOT
	am.RecordQuotaSyncOutcome("primary", "timeout (15s) running /usage (check auth/proxy)")
	am.RecordQuotaSyncOutcome("primary", "exit status 1")
	if hv := am.BuildAccountHealthView(primActive, true, 0, now); hv.State != "healthy" {
		t.Fatalf("transient /usage timeout or generic exit 1 must not poison health state, got %+v", hv)
	}
	am.RecordQuotaSyncOutcome("primary", "exit status 1: Account has been disabled by administrator")
	if hv := am.BuildAccountHealthView(primActive, true, 0, now); hv.State != "tos_tripped" {
		t.Fatalf("expected /usage ban error to mark primary tos_tripped, got %+v", hv)
	}
	accC, _ := am.GetAccount("acct-c")
	am.RecordQuotaSyncOutcome("acct-c", "exit status 1: not logged in, login required")
	if hv := am.BuildAccountHealthView(accC, true, 0, now); hv.State != "auth_error" {
		t.Fatalf("expected /usage login required error to mark acct-c auth_error, got %+v", hv)
	}
}

func setupV2318FiveAccountPool(t *testing.T, proStandby bool) *AccountManager {
	t.Helper()
	prevDB := GatewayStateDB
	waitQuotaRefreshIdle(t)
	quotaMu.Lock()
	prevQuota, prevQuotaAt, prevQuotaErr, prevQuotaSteps := cachedQuota, cachedQuotaAt, lastQuotaErr, quotaBackoffSteps
	cachedQuota, cachedQuotaAt, lastQuotaErr, quotaBackoffSteps = nil, time.Time{}, nil, 0
	quotaMu.Unlock()
	resetTosBreakerForTest()
	resetMonitorTripsForTest()

	tmpDir := t.TempDir()
	accFile := filepath.Join(tmpDir, "accounts.json")
	// Register cleanup BEFORE t.Setenv so LIFO cleanup restores env vars before calling initConfig/loadAPIKeys.
	t.Cleanup(func() {
		waitQuotaRefreshIdle(t)
		GlobalAccountManager.StopAll()
		GlobalAccountManager = NewAccountManager()
		resetPeerDashboardCacheForTest()
		resetMonitorTripsForTest()
		resetTosBreakerForTest()
		quotaMu.Lock()
		cachedQuota, cachedQuotaAt, lastQuotaErr, quotaBackoffSteps = prevQuota, prevQuotaAt, prevQuotaErr, prevQuotaSteps
		quotaMu.Unlock()
		initConfig()
		apiKeys = loadAPIKeys()
		InitGlobalStore(prevDB)
	})

	t.Setenv("AGY_STATE_DB", filepath.Join(tmpDir, "gateway.db"))
	t.Setenv("AGY_QUOTA_BIN", "/bin/false")
	t.Setenv("AGY_TOS_TRIPPED_FILE", filepath.Join(tmpDir, "tos-tripped"))
	t.Setenv("AGY_ACCOUNTS_FILE", accFile)
	t.Setenv("AGY_INSTANCE_ID", "u1")
	t.Setenv("AGY_INSTANCE_LABEL", "Ultra 1")
	t.Setenv("AGY_API_KEYS", "master-v2318-key")
	t.Setenv("AGY_MONITOR_BUDGET_SPREAD", "1")
	initConfig()
	apiKeys = loadAPIKeys()

	GlobalAccountManager.StopAll()
	GlobalAccountManager = NewAccountManager()
	GlobalAccountManager.Init()

	proRole := AccountRolePrimary
	if proStandby {
		proRole = AccountRoleStandby
	}
	models := []string{"gemini-4-neon", "gemini-3.8-flash"}
	profiles := []AccountProfile{
		{ID: "u1", Label: "Ultra 1", Primary: true, Enabled: true, Role: AccountRolePrimary, PlanTierHint: "ultra", ProxyPreset: "direct", MaxLiveWorkers: 8, Priority: 10, Weight: 3, AllowedModels: models},
		{ID: "u2", Label: "Ultra 2", Enabled: true, Role: AccountRolePrimary, PlanTierHint: "ultra", ProxyPreset: "direct", MaxLiveWorkers: 8, Priority: 10, Weight: 3, AllowedModels: models},
		{ID: "u3", Label: "Ultra 3", Enabled: true, Role: AccountRolePrimary, PlanTierHint: "ultra", ProxyPreset: "direct", MaxLiveWorkers: 8, Priority: 10, Weight: 3, AllowedModels: models},
		{ID: "p1", Label: "Pro 1", Enabled: true, Role: proRole, PlanTierHint: "pro", ProxyPreset: "direct", MaxLiveWorkers: 4, Priority: 20, Weight: 1, AllowedModels: models},
		{ID: "p2", Label: "Pro 2", Enabled: true, Role: proRole, PlanTierHint: "pro", ProxyPreset: "direct", MaxLiveWorkers: 4, Priority: 20, Weight: 1, AllowedModels: models},
	}
	for _, prof := range profiles {
		if _, err := GlobalAccountManager.UpsertAccount(prof); err != nil {
			t.Fatalf("upsert %s: %v", prof.ID, err)
		}
		if !prof.Primary {
			p := GlobalAccountManager.AccountTokenPath(prof.ID)
			if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
				t.Fatalf("mkdir token dir %s: %v", prof.ID, err)
			}
			if err := os.WriteFile(p, []byte(`{"access_token":"unit-test-dummy-token-value","expiry_date":9999999999}`), 0o600); err != nil {
				t.Fatalf("write token %s: %v", prof.ID, err)
			}
			GlobalAccountManager.InvalidateTokenCache(prof.ID)
		}
	}
	return GlobalAccountManager
}

func TestV2318_StandbyRoleAcrossAllStrategiesAndSpillOnYellow(t *testing.T) {
	am := setupV2318FiveAccountPool(t, true)
	now := time.Now()
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	req.Header.Set("Authorization", "Bearer master-v2318-key")

	// 1. All 4 strategies keep 100% of tool & zero-tool traffic on the 3 Green primary Ultras; standby Pros get 0!
	for _, strat := range []string{RouteStrategyRoundRobin, RouteStrategyWeightedRR, RouteStrategyLeastLoad, RouteStrategyFillFirst} {
		if _, err := am.UpdateRoutingConfig(AccountPoolConfig{Strategy: strat}); err != nil {
			t.Fatalf("set strategy %s: %v", strat, err)
		}
		for i := 0; i < 12; i++ {
			pickTool := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{
				SessionKey: "hdr:stby-tool-" + strat + "-" + strconv.Itoa(i),
				HasTools:   true,
			})
			if pickTool.DenyErr != nil || pickTool.Account.IsStandby() {
				t.Fatalf("strategy=%s tool #%d must stay on primary Ultras, got %q (standby=%v) err=%v",
					strat, i, pickTool.Account.ID, pickTool.Account.IsStandby(), pickTool.DenyErr)
			}
			pickText := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{
				SessionKey: "hdr:stby-text-" + strat + "-" + strconv.Itoa(i),
				HasTools:   false,
			})
			if pickText.DenyErr != nil || pickText.Account.IsStandby() {
				t.Fatalf("strategy=%s zero-tool #%d must stay on primary Ultras, got %q (standby=%v) err=%v",
					strat, i, pickText.Account.ID, pickText.Account.IsStandby(), pickText.DenyErr)
			}
		}
	}

	// 2. Zero-tool warmHealthy preference stays WITHIN primaryPool (prefers Yellow u1 over Green u2/u3, never wakes standby Pros).
	if _, err := am.UpdateRoutingConfig(AccountPoolConfig{Strategy: RouteStrategyRoundRobin}); err != nil {
		t.Fatalf("set round-robin: %v", err)
	}
	RecordMonitorToolDispatchAt("u1", 3000, now) // u1 Yellow, u2/u3 Green
	pickWarm := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:stby-warm-text", HasTools: false})
	if pickWarm.DenyErr != nil || pickWarm.Account.ID != "u1" {
		t.Fatalf("zero-tool must prefer Yellow primary u1 within primaryPool, got %q err=%v", pickWarm.Account.ID, pickWarm.DenyErr)
	}

	// 3. All 3 Ultras enter Yellow -> monitorSpillOnYellow=true (default) spills tool traffic to standby Pros!
	RecordMonitorToolDispatchAt("u2", 3000, now)
	RecordMonitorToolDispatchAt("u3", 3000, now)
	pickSpill := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:stby-spill-y", HasTools: true})
	if pickSpill.DenyErr != nil || !pickSpill.Account.IsStandby() {
		t.Fatalf("all primary Ultras Yellow with monitorSpillOnYellow=true must spill to Green standby Pro, got %q err=%v", pickSpill.Account.ID, pickSpill.DenyErr)
	}
	// Zero-tool STILL stays on primaries even when all 3 Ultras are Yellow!
	pickTextAllYellow := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:stby-text-ally", HasTools: false})
	if pickTextAllYellow.DenyErr != nil || pickTextAllYellow.Account.IsStandby() {
		t.Fatalf("zero-tool must stay on primary Ultras even when all Ultras are Yellow, got %q err=%v", pickTextAllYellow.Account.ID, pickTextAllYellow.DenyErr)
	}

	// 4. monitorSpillOnYellow=false: stays on Yellow primary Ultras (0 spills to standby) until Ultras are Red/tripped!
	spillFalse := false
	if _, err := am.UpdateRoutingConfig(AccountPoolConfig{
		Strategy:             RouteStrategyRoundRobin,
		MonitorSpillOnYellow: &spillFalse,
	}); err != nil {
		t.Fatalf("disable spillOnYellow: %v", err)
	}
	for i := 0; i < 6; i++ {
		pickNoSpill := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:stby-nospill-" + strconv.Itoa(i), HasTools: true})
		if pickNoSpill.DenyErr != nil || pickNoSpill.Account.IsStandby() {
			t.Fatalf("monitorSpillOnYellow=false must keep Yellow tool traffic on primary Ultras, got %q err=%v", pickNoSpill.Account.ID, pickNoSpill.DenyErr)
		}
	}
	// Push all 3 Ultras to Red (>= DrainCap 3800) -> NOW spills to Green standby Pros!
	RecordMonitorToolDispatchAt("u1", 900, now) // 3900 Red
	RecordMonitorToolDispatchAt("u2", 900, now) // 3900 Red
	RecordMonitorToolDispatchAt("u3", 900, now) // 3900 Red
	pickRedSpill := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:stby-red-spill", HasTools: true})
	if pickRedSpill.DenyErr != nil || !pickRedSpill.Account.IsStandby() {
		t.Fatalf("all primary Ultras Red with monitorSpillOnYellow=false must spill to standby Pros, got %q err=%v", pickRedSpill.Account.ID, pickRedSpill.DenyErr)
	}

	// 5. Recovery snap-back: u1 resets to Green -> traffic immediately snaps back to primary Ultra u1!
	resetAccountMonitorBudget("u1", false)
	pickSnapBack := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:stby-snapback", HasTools: true})
	if pickSnapBack.DenyErr != nil || pickSnapBack.Account.ID != "u1" {
		t.Fatalf("after primary u1 recovers to Green, traffic must snap back to u1, got %q err=%v", pickSnapBack.Account.ID, pickSnapBack.DenyErr)
	}

	// 6. Explicit X-Agy-Account and manual-active remain unrestricted for standby accounts!
	expReq := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	expReq.Header.Set("Authorization", "Bearer master-v2318-key")
	expReq.Header.Set("X-Agy-Account", "p1")
	pickExp := am.ResolveTargetAccountEx(expReq, "gemini-4-neon", ResolveOptions{HasTools: true})
	if pickExp.DenyErr != nil || pickExp.Account.ID != "p1" {
		t.Fatalf("explicit X-Agy-Account=p1 must remain unrestricted on standby, got %q err=%v", pickExp.Account.ID, pickExp.DenyErr)
	}
	if _, err := am.UpdateRoutingConfig(AccountPoolConfig{Strategy: RouteStrategyManualActive, ActiveAccountID: "p1"}); err != nil {
		t.Fatalf("set manual-active p1: %v", err)
	}
	pickManual := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:stby-manual", HasTools: true})
	if pickManual.DenyErr != nil || pickManual.Account.ID != "p1" {
		t.Fatalf("manual-active activeAccountId=p1 must remain unrestricted on standby, got %q err=%v", pickManual.Account.ID, pickManual.DenyErr)
	}

	// 7. rotate_next / drain_and_rotate prefers non-standby primary accounts before falling back to standby!
	resetMonitorTripsForTest()
	if _, err := am.UpdateRoutingConfig(AccountPoolConfig{Strategy: RouteStrategyManualActive, ActiveAccountID: "u3"}); err != nil {
		t.Fatalf("set active u3: %v", err)
	}
	// Accounts list order is [u1, u2, u3, p1(standby), p2(standby)]; rotating from u3 must skip p1/p2 and land on u1!
	rotRes, err := am.PerformAccountAction("u3", "rotate_next", true)
	if err != nil {
		t.Fatalf("rotate_next from u3 failed: %v", err)
	}
	if rotRes["activeAccountId"] != "u1" {
		t.Fatalf("rotate_next must prefer primary u1 over standby p1/p2, got %+v", rotRes)
	}
}

func TestV2318_PriorityTieringAndGovernanceRoutingAPI(t *testing.T) {
	am := setupV2318FiveAccountPool(t, false) // all 5 accounts role="primary", Ultras P10, Pros P20
	now := time.Now()
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	req.Header.Set("Authorization", "Bearer master-v2318-key")

	tierTrue := true
	if _, err := am.UpdateRoutingConfig(AccountPoolConfig{
		Strategy:        RouteStrategyRoundRobin,
		PriorityTiering: &tierTrue,
	}); err != nil {
		t.Fatalf("enable priorityTiering: %v", err)
	}

	// 1. With priorityTiering=true on round-robin, 100% of Green traffic stays in P10 tier and distributes across u1/u2/u3.
	hits := map[string]int{}
	for i := 0; i < 9; i++ {
		pick := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{
			SessionKey: "hdr:pt-green-" + strconv.Itoa(i),
			HasTools:   true,
		})
		if pick.DenyErr != nil {
			t.Fatalf("priorityTiering RR #%d err=%v", i, pick.DenyErr)
		}
		hits[pick.Account.ID]++
	}
	if hits["p1"] != 0 || hits["p2"] != 0 || hits["u1"] != 3 || hits["u2"] != 3 || hits["u3"] != 3 {
		t.Fatalf("expected P10 tier exact RR {u1:3, u2:3, u3:3, p1:0, p2:0}, got %+v", hits)
	}

	// 2. Push all P10 Ultras to Yellow -> spills to Green P20 Pros (monitorSpillOnYellow defaults to true).
	RecordMonitorToolDispatchAt("u1", 3000, now)
	RecordMonitorToolDispatchAt("u2", 3000, now)
	RecordMonitorToolDispatchAt("u3", 3000, now)
	pickYellowSpill := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:pt-spill-y", HasTools: true})
	if pickYellowSpill.DenyErr != nil || (pickYellowSpill.Account.ID != "p1" && pickYellowSpill.Account.ID != "p2") {
		t.Fatalf("all P10 Yellow must spill to P20 Pros with monitorSpillOnYellow=true, got %q err=%v", pickYellowSpill.Account.ID, pickYellowSpill.DenyErr)
	}

	// 3. Recovery of u2 snaps traffic back to P10!
	resetAccountMonitorBudget("u2", false)
	pickBack := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:pt-back", HasTools: true})
	if pickBack.DenyErr != nil || pickBack.Account.ID != "u2" {
		t.Fatalf("recovered P10 u2 must reclaim traffic from P20, got %q err=%v", pickBack.Account.ID, pickBack.DenyErr)
	}

	// 4. Verify HTTP API round-trip: POST /api/accounts (role), POST /api/accounts/routing (3 governance booleans),
	//    GET /api/accounts exposes role + effective booleans, and monitorBudgetSpread=false overrides env AGY_MONITOR_BUDGET_SPREAD=1!
	postRoutingReq := httptest.NewRequest(http.MethodPost, "/api/accounts/routing", strings.NewReader(
		`{"strategy":"fill-first","priorityTiering":true,"monitorSpillOnYellow":false,"monitorBudgetSpread":false}`,
	))
	postRoutingReq.Header.Set("Content-Type", "application/json")
	postRoutingReq.Header.Set("X-Agy-Requested-With", "dashboard")
	recRoute := httptest.NewRecorder()
	HandleAccountsAPI(recRoute, postRoutingReq)
	if recRoute.Code != http.StatusOK {
		t.Fatalf("POST /api/accounts/routing status=%d body=%s", recRoute.Code, recRoute.Body.String())
	}
	var routeResp struct {
		OK      bool              `json:"ok"`
		Routing AccountPoolConfig `json:"routing"`
	}
	if err := json.Unmarshal(recRoute.Body.Bytes(), &routeResp); err != nil || !routeResp.OK {
		t.Fatalf("decode routing response: err=%v body=%s", err, recRoute.Body.String())
	}
	if !routeResp.Routing.PriorityTieringEnabled() || routeResp.Routing.MonitorSpillOnYellowEnabled() || routeResp.Routing.MonitorBudgetSpreadEnabled() {
		t.Fatalf("unexpected routing governance flags after POST: %+v", routeResp.Routing)
	}

	// Verify monitorBudgetSpread=false runtime override immediately disables equal-priority spread without restart!
	resetMonitorTripsForTest()
	RecordMonitorToolDispatchAt("u1", 960, now) // u1=20% Green, u2=0% Green (both P10, u1 configOrd=0 < u2 configOrd=1)
	pickNoSpread := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:pt-nospread", HasTools: true})
	if pickNoSpread.DenyErr != nil || pickNoSpread.Account.ID != "u1" {
		t.Fatalf("monitorBudgetSpread=false override must disable spread and keep fill-first configOrd u1, got %q err=%v", pickNoSpread.Account.ID, pickNoSpread.DenyErr)
	}

	// Update p2 role to standby via POST /api/accounts and verify GET /api/accounts exposes role + routing booleans.
	p2Acc, _ := am.GetAccount("p2")
	p2Acc.Role = AccountRoleStandby
	p2Body, _ := json.Marshal(p2Acc)
	postAccReq := httptest.NewRequest(http.MethodPost, "/api/accounts", strings.NewReader(string(p2Body)))
	postAccReq.Header.Set("Content-Type", "application/json")
	postAccReq.Header.Set("X-Agy-Requested-With", "dashboard")
	recAcc := httptest.NewRecorder()
	HandleAccountsAPI(recAcc, postAccReq)
	if recAcc.Code != http.StatusOK {
		t.Fatalf("POST /api/accounts role=standby status=%d body=%s", recAcc.Code, recAcc.Body.String())
	}

	recGet := httptest.NewRecorder()
	HandleAccountsAPI(recGet, httptest.NewRequest(http.MethodGet, "/api/accounts", nil))
	if recGet.Code != http.StatusOK {
		t.Fatalf("GET /api/accounts status=%d body=%s", recGet.Code, recGet.Body.String())
	}
	var getBody struct {
		OK       bool             `json:"ok"`
		Routing  map[string]any   `json:"routing"`
		Accounts []map[string]any `json:"accounts"`
	}
	if err := json.Unmarshal(recGet.Body.Bytes(), &getBody); err != nil || !getBody.OK {
		t.Fatalf("decode GET /api/accounts: err=%v", err)
	}
	if getBody.Routing["priorityTiering"] != true || getBody.Routing["monitorSpillOnYellow"] != false || getBody.Routing["monitorBudgetSpread"] != false {
		t.Fatalf("GET /api/accounts routing missing/wrong governance booleans: %+v", getBody.Routing)
	}
	foundP2Standby := false
	foundU1Primary := false
	for _, a := range getBody.Accounts {
		if a["id"] == "p2" && a["role"] == AccountRoleStandby {
			foundP2Standby = true
		}
		if a["id"] == "u1" && a["role"] == AccountRolePrimary {
			foundU1Primary = true
		}
	}
	if !foundP2Standby || !foundU1Primary {
		t.Fatalf("GET /api/accounts expected u1=primary and p2=standby roles, got %+v", getBody.Accounts)
	}

	// Verify accounts.json reload preserves role and all 3 routing governance flags across restart!
	reloaded := NewAccountManager()
	reloaded.filePath = am.filePath
	reloaded.mu.Lock()
	okLoad := reloaded.loadFromDiskLocked()
	reloaded.mu.Unlock()
	if !okLoad {
		t.Fatal("failed to reload persisted accounts.json")
	}
	rCfg := reloaded.GetRoutingConfig()
	if !rCfg.PriorityTieringEnabled() || rCfg.MonitorSpillOnYellowEnabled() || rCfg.MonitorBudgetSpreadEnabled() {
		t.Fatalf("reloaded routing config lost governance flags: %+v", rCfg)
	}
	p2Reloaded, ok := reloaded.GetAccount("p2")
	if !ok || !p2Reloaded.IsStandby() {
		t.Fatalf("reloaded p2 lost standby role: %+v", p2Reloaded)
	}
}
