package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
)

func TestV2327_ImageGenCooldownFamilyIsolation(t *testing.T) {
	// 1. Verify accountCooldownFamily splits gemini-3.1-flash-image (and -preview / models/ prefix) into "image_gen".
	cases := []struct {
		model string
		want  string
	}{
		{"gemini-3.1-flash-image", "image_gen"},
		{"gemini-3.1-flash-image-preview", "image_gen"},
		{"models/gemini-3.1-flash-image", "image_gen"},
		{"  GEMINI-3.1-FLASH-IMAGE-PREVIEW  ", "image_gen"},
		{"gemini-2.5-pro", "gemini"},
		{"gemini-3.1-pro-high", "gemini"},
		{"gemini-4-carbon", "gemini"},
		{"gemini-4-argon", "argon"},
	}
	for _, tc := range cases {
		if got := accountCooldownFamily(tc.model); got != tc.want {
			t.Fatalf("accountCooldownFamily(%q) = %q, want %q", tc.model, got, tc.want)
		}
	}

	origAM := GlobalAccountManager
	origDB := GatewayStateDB
	t.Cleanup(func() {
		GlobalAccountManager = origAM
		initConfig()
		InitGlobalStore(origDB)
	})

	tmp := t.TempDir()
	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "v2327_imgfam.db"))
	t.Setenv("AGY_GW_LOG", filepath.Join(tmp, "gateway.log"))
	t.Setenv("AGY_CHILD_ACCOUNT_WORKER", "")
	initConfig()

	am := NewAccountManager()
	am.accounts = []AccountProfile{
		{ID: "primary", Label: "Primary", Primary: true, Enabled: true, Priority: 10, AllowedModels: []string{"gemini-2.5-pro", imageGenServedModel}},
		{ID: "child-img", Label: "Child Img", Primary: false, Enabled: true, Priority: 20, AllowedModels: []string{"gemini-2.5-pro", imageGenServedModel}},
	}
	am.enabledAccountCount.Store(2)
	am.routing = AccountPoolConfig{Strategy: RouteStrategyFillFirst}.Normalized()
	am.workers["child-img"] = &accountWorkerProc{
		id:        "child-img",
		gwPort:    29901,
		dashPort:  29902,
		transport: &http.Transport{},
	}
	// Seed dummy OAuth file for child-img so HasOAuthToken returns true.
	am.baseDataDir = tmp
	if err := writeTestDummyOAuth(am, "child-img"); err != nil {
		t.Fatal(err)
	}
	GlobalAccountManager = am

	// 2. image_gen 429 must ONLY cool down "image_gen", NEVER Gemini text/coding models.
	am.RecordAccountOutcome("primary", imageGenServedModel, http.StatusTooManyRequests, "rate_limit_error", "120", "image hidden quota exhausted")
	now := time.Now()
	if cooling, _, _ := am.isAccountFamilyCoolingDown("primary", imageGenServedModel, now); !cooling {
		t.Fatal("expected image_gen family to be in cooldown after 429")
	}
	if cooling, _, _ := am.isAccountFamilyCoolingDown("primary", imageGenServedModel+"-preview", now); !cooling {
		t.Fatal("expected image_gen -preview alias to share image_gen cooldown")
	}
	if cooling, _, _ := am.isAccountFamilyCoolingDown("primary", "gemini-2.5-pro", now); cooling {
		t.Fatal("image_gen 429 must NOT cool down Gemini text model gemini-2.5-pro")
	}
	if cooling, _, _ := am.isAccountFamilyCoolingDown("primary", "gemini-4-carbon", now); cooling {
		t.Fatal("image_gen 429 must NOT cool down Gemini text model gemini-4-carbon")
	}

	// 3. Gemini text 200 must NEVER clear image_gen cooldown.
	am.RecordAccountOutcome("primary", "gemini-2.5-pro", http.StatusOK, "", "", "")
	if cooling, _, _ := am.isAccountFamilyCoolingDown("primary", imageGenServedModel, time.Now()); !cooling {
		t.Fatal("Gemini text 200 must NOT clear image_gen cooldown")
	}

	// 4. HealthView reports "image_gen" family cooldown without "gemini".
	hv := am.BuildAccountHealthView(am.accounts[0], true, 0, time.Now())
	if hv.FamilyCooldowns["image_gen"] <= 0 {
		t.Fatalf("expected image_gen in FamilyCooldowns, got %+v", hv.FamilyCooldowns)
	}
	if hv.FamilyCooldowns["gemini"] != 0 {
		t.Fatalf("expected gemini NOT in FamilyCooldowns, got %+v", hv.FamilyCooldowns)
	}

	// 5. Routing check: primary still receives Gemini text traffic, while image_gen routes to child-img.
	reqText := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	pickText := am.ResolveTargetAccountEx(reqText, "gemini-2.5-pro", ResolveOptions{})
	if pickText.DenyErr != nil || pickText.Account.ID != "primary" {
		t.Fatalf("expected Gemini text to still route to primary, got acc=%q err=%v", pickText.Account.ID, pickText.DenyErr)
	}
	reqImg := httptest.NewRequest(http.MethodPost, "/v1/images/generations", nil)
	pickImg := am.ResolveTargetAccountEx(reqImg, imageGenServedModel, ResolveOptions{})
	if pickImg.DenyErr != nil || pickImg.Account.ID != "child-img" {
		t.Fatalf("expected image_gen to avoid cooling primary and route to child-img, got acc=%q err=%v", pickImg.Account.ID, pickImg.DenyErr)
	}

	// 6. Gemini text 429 + image_gen 200 clears ONLY image_gen and leaves Gemini text cooldown intact.
	am.RecordAccountOutcome("primary", "gemini-2.5-pro", http.StatusTooManyRequests, "rate_limit_error", "90", "gemini text 429")
	am.RecordAccountOutcome("primary", imageGenServedModel, http.StatusOK, "", "", "")
	now = time.Now()
	if cooling, _, _ := am.isAccountFamilyCoolingDown("primary", imageGenServedModel, now); cooling {
		t.Fatal("image_gen 200 should clear image_gen cooldown")
	}
	if cooling, _, _ := am.isAccountFamilyCoolingDown("primary", "gemini-2.5-pro", now); !cooling {
		t.Fatal("image_gen 200 must NOT clear Gemini text cooldown")
	}
}

func writeTestDummyOAuth(am *AccountManager, accountID string) error {
	p := am.AccountTokenPath(accountID)
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return err
	}
	return os.WriteFile(p, []byte(`{"access_token":"tok","refresh_token":"ref"}`), 0o600)
}

func TestV2327_GlobalWorkerSlotErrorIsolationAndNoCascadeFailover(t *testing.T) {
	// 1. Direct helper checks
	hdr := make(http.Header)
	hdr.Set(internalGlobalSlotFullHeader, "1")
	if !isGlobalWorkerSlotError(529, "overloaded_error", "anything", hdr) {
		t.Fatal("expected X-Agy-Global-Slot-Full=1 header to match isGlobalWorkerSlotError")
	}
	if !isGlobalWorkerSlotError(529, "overloaded_error", "All global live worker slots are busy; retry shortly", nil) {
		t.Fatal("expected global busy message to match isGlobalWorkerSlotError")
	}
	if !isGlobalWorkerSlotError(529, "overloaded_error", "Global worker admission queue is full; retry shortly", nil) {
		t.Fatal("expected global admission queue message to match isGlobalWorkerSlotError")
	}
	if !isGlobalWorkerSlotError(http.StatusServiceUnavailable, "overloaded_error", "All global live worker slots are busy; retry shortly", nil) {
		t.Fatal("expected Gemini 503 global busy message to match isGlobalWorkerSlotError")
	}
	if isGlobalWorkerSlotError(529, "overloaded_error", "All live worker slots are busy; retry shortly", nil) {
		t.Fatal("local slot busy message must NOT match isGlobalWorkerSlotError")
	}
	if isGlobalWorkerSlotError(529, "overloaded_error", "Upstream model overloaded", nil) {
		t.Fatal("genuine upstream 529 must NOT match isGlobalWorkerSlotError")
	}

	// 2. copyCleanProxyResponseHeaders strips X-Agy-Global-Slot-Full & hop-by-hop headers.
	srcHdr := make(http.Header)
	srcHdr.Set("Content-Type", "application/json")
	srcHdr.Set("X-Agy-Global-Slot-Full", "1")
	srcHdr.Set("X-Agy-Local-Slot-Full", "1")
	srcHdr.Set("X-Agy-Slot-Usage", "4/4")
	srcHdr.Set("Connection", "X-Dynamic-Hop")
	srcHdr.Set("X-Dynamic-Hop", "secret")
	srcHdr.Set("X-Keep-Me", "ok")
	dstHdr := make(http.Header)
	copyCleanProxyResponseHeaders(dstHdr, srcHdr)
	if dstHdr.Get("X-Agy-Global-Slot-Full") != "" || dstHdr.Get("X-Agy-Local-Slot-Full") != "" || dstHdr.Get("X-Agy-Slot-Usage") != "" {
		t.Fatalf("internal slot headers must be stripped, got %+v", dstHdr)
	}
	if dstHdr.Get("X-Dynamic-Hop") != "" || dstHdr.Get("Connection") != "" {
		t.Fatalf("Connection-listed hop headers must be stripped, got %+v", dstHdr)
	}
	if dstHdr.Get("X-Keep-Me") != "ok" || dstHdr.Get("Content-Type") != "application/json" {
		t.Fatalf("normal headers must be preserved, got %+v", dstHdr)
	}

	// 3. shouldRetryChildPreStreamFailure returns false for global slot errors, true for upstream 529.
	if shouldRetryChildPreStreamFailure(529, "overloaded_error", "All global live worker slots are busy; retry shortly") {
		t.Fatal("shouldRetryChildPreStreamFailure must return false for global slot busy")
	}
	if shouldRetryChildPreStreamFailure(529, "overloaded_error", "Global worker admission queue is full; retry shortly") {
		t.Fatal("shouldRetryChildPreStreamFailure must return false for global queue full")
	}
	if !shouldRetryChildPreStreamFailure(529, "overloaded_error", "Upstream model capacity exhausted") {
		t.Fatal("shouldRetryChildPreStreamFailure must still return true for genuine upstream 529")
	}

	origAM := GlobalAccountManager
	origDB := GatewayStateDB
	t.Cleanup(func() {
		GlobalAccountManager = origAM
		initConfig()
		InitGlobalStore(origDB)
	})

	tmp := t.TempDir()
	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "v2327_bug01.db"))
	t.Setenv("AGY_GW_LOG", filepath.Join(tmp, "gateway.log"))
	t.Setenv("AGY_CHILD_ACCOUNT_WORKER", "")
	initConfig()

	// 4. recordPrimaryDirectOutcome must skip RecordAccountOutcome on global slot error.
	am := NewAccountManager()
	am.baseDataDir = tmp
	GlobalAccountManager = am
	primReq := withRoutedAccountMeta(httptest.NewRequest(http.MethodPost, "/v1/messages", nil), "primary", "gemini-2.5-pro", false)
	recordPrimaryDirectOutcome(primReq, 529, "overloaded_error", "5", "All global live worker slots are busy; retry shortly")
	if cooling, _, _ := am.isAccountFamilyCoolingDown("primary", "gemini-2.5-pro", time.Now()); cooling {
		t.Fatal("recordPrimaryDirectOutcome must NOT cool down primary on global slot error")
	}
	if am.HasActiveLocalSlotBusy("primary", time.Now()) {
		t.Fatal("recordPrimaryDirectOutcome must NOT mark local slot busy on global slot error")
	}

	// 5. End-to-end TryRouteAPIRequestWithFinish: pre-stream 529 global slot full from child-a
	// must NOT poison child-a cooldown and must NOT cascade-failover to child-b.
	var childAHits, childBHits atomic.Int32
	childAMode := atomic.Int32{} // 0 = pre-stream 529 w/ header, 1 = mid-stream SSE global slot error
	srvA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		childAHits.Add(1)
		if childAMode.Load() == 0 {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set(internalGlobalSlotFullHeader, "1")
			w.WriteHeader(529)
			_, _ = w.Write([]byte(`{"error":{"type":"overloaded_error","message":"All global live worker slots are busy; retry shortly"}}`))
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(": keepalive\n\n"))
		_, _ = w.Write([]byte("event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"All global live worker slots are busy; retry shortly\"}}\n\n"))
	}))
	defer srvA.Close()
	srvB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		childBHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srvB.Close()

	portOf := func(rawURL string) int {
		u, err := url.Parse(rawURL)
		if err != nil {
			t.Fatal(err)
		}
		p, err := strconv.Atoi(u.Port())
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	portA := portOf(srvA.URL)
	portB := portOf(srvB.URL)

	preStream := true
	am.accounts = []AccountProfile{
		{ID: "primary", Label: "Primary", Primary: true, Enabled: false, SchedState: SchedStateDisabled},
		{ID: "child-a", Label: "Child A", Primary: false, Enabled: true, Priority: 10, AllowedModels: []string{"gemini-4-carbon"}},
		{ID: "child-b", Label: "Child B", Primary: false, Enabled: true, Priority: 20, AllowedModels: []string{"gemini-4-carbon"}},
	}
	am.enabledAccountCount.Store(2)
	am.routing = AccountPoolConfig{
		Strategy:          RouteStrategyFillFirst,
		PreStreamFailover: &preStream,
		MaxFailoverHops:   2,
	}.Normalized()
	for _, item := range []struct {
		id   string
		port int
	}{{"child-a", portA}, {"child-b", portB}} {
		if err := writeTestDummyOAuth(am, item.id); err != nil {
			t.Fatal(err)
		}
		p := &accountWorkerProc{
			id:        item.id,
			gwPort:    item.port,
			dashPort:  item.port,
			transport: &http.Transport{},
			done:      make(chan struct{}),
			readyCh:   make(chan struct{}),
			healthyAt: time.Now(),
		}
		p.finishReady(nil)
		am.workers[item.id] = p
	}

	reqBody := []byte(`{"model":"gemini-4-carbon","messages":[{"role":"user","content":"hi"}]}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	_, _, finish, handled := am.TryRouteAPIRequestWithFinish(rr, req)
	finish()
	if !handled {
		t.Fatal("expected request to be handled by child-a")
	}
	if rr.Code != 529 {
		t.Fatalf("expected 529 response from child-a, got %d body=%s", rr.Code, rr.Body.String())
	}
	if rr.Header().Get(internalGlobalSlotFullHeader) != "" {
		t.Fatalf("expected X-Agy-Global-Slot-Full stripped from client response, got %q", rr.Header().Get(internalGlobalSlotFullHeader))
	}
	if childAHits.Load() != 1 || childBHits.Load() != 0 {
		t.Fatalf("global slot 529 must NOT cascade-failover to child-b (hits A=%d B=%d)", childAHits.Load(), childBHits.Load())
	}
	if cooling, _, _ := am.isAccountFamilyCoolingDown("child-a", "gemini-4-carbon", time.Now()); cooling {
		t.Fatal("child-a must NOT be cooled down after pre-stream global slot 529")
	}

	// 6. Mid-stream SSE global slot error must ALSO skip RecordAccountOutcome.
	childAMode.Store(1)
	reqSSE := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(reqBody))
	reqSSE.Header.Set("Content-Type", "application/json")
	rrSSE := httptest.NewRecorder()
	_, _, finishSSE, handledSSE := am.TryRouteAPIRequestWithFinish(rrSSE, reqSSE)
	finishSSE()
	if !handledSSE {
		t.Fatal("expected SSE request to be handled by child-a")
	}
	if cooling, _, _ := am.isAccountFamilyCoolingDown("child-a", "gemini-4-carbon", time.Now()); cooling {
		t.Fatal("child-a must NOT be cooled down after mid-stream SSE global slot error")
	}
}

func TestV2327_GlobalAdmitClientCancelAndClassify499(t *testing.T) {
	// 1. classifyUpstreamError maps context.Canceled (direct and wrapped) to 499 request_cancelled.
	for _, err := range []error{context.Canceled, fmt.Errorf("admit aborted: %w", context.Canceled)} {
		code, errType, retry, msg := classifyUpstreamError(err)
		if code != 499 || errType != "request_cancelled" || retry || !strings.Contains(strings.ToLower(msg), "cancelled") {
			t.Fatalf("classifyUpstreamError(%v) = (%d, %q, %v, %q), want (499, request_cancelled, false, ...)", err, code, errType, retry, msg)
		}
	}

	origAM := GlobalAccountManager
	origWait := globalAdmitWaitTimeout
	origPoll := globalAdmitPollInterval
	origDB := GatewayStateDB
	t.Cleanup(func() {
		GlobalAccountManager = origAM
		globalAdmitWaitTimeout = origWait
		globalAdmitPollInterval = origPoll
		initConfig()
		InitGlobalStore(origDB)
	})

	tmp := t.TempDir()
	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "v2327_bug02.db"))
	t.Setenv("AGY_GW_LOG", filepath.Join(tmp, "gateway.log"))
	t.Setenv("AGY_GLOBAL_BURST_MAX_LIVE_WORKERS", "1")
	t.Setenv("AGY_CHILD_ACCOUNT_WORKER", "")
	initConfig()

	globalAdmitWaitTimeout = 2 * time.Second
	globalAdmitPollInterval = 25 * time.Millisecond

	restoreMem := setCgroupMemoryReaderForTest(func() CgroupMemoryStatus {
		return CgroupMemoryStatus{Version: 2, HasLiveUsage: true, LimitBytes: 6 << 30, WorkingSetBytes: 2 << 30, UsagePct: 33.3}
	})
	defer restoreMem()

	eng := newV2322CloseTrackEngine(nil)
	gs := NewGatewayServerWithDeps(eng, &StateStore{})
	defer gs.Close()
	// Occupy the sole burst slot with a BUSY session so admission must wait.
	v2322SeedSession(gs, "sid:busy-holder", "00000000-0000-4000-8000-000000009001", "BUSY", time.Now(), 10000)

	am := NewAccountManager()
	am.accounts = []AccountProfile{{ID: "primary", Label: "Primary", Primary: true, Enabled: true, MaxLiveWorkers: 8}}
	am.enabledAccountCount.Store(1)
	am.setPrimaryGatewayForTest(gs)
	GlobalAccountManager = am

	// 2a. Pre-canceled context returns context.Canceled (never 529 SoftError).
	preCtx, preCancel := context.WithCancel(context.Background())
	preCancel()
	if _, err := am.AcquireGlobalWorkerSlotExcluding(preCtx, "primary", "sid:new-0", ""); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled on pre-canceled ctx, got %T %v", err, err)
	}

	// 2b. Head waiter canceled mid-wait AND non-head waiter canceled mid-wait both return context.Canceled.
	headCtx, cancelHead := context.WithCancel(context.Background())
	tailCtx, cancelTail := context.WithCancel(context.Background())
	headErrCh := make(chan error, 1)
	tailErrCh := make(chan error, 1)

	go func() {
		_, err := am.AcquireGlobalWorkerSlotExcluding(headCtx, "primary", "sid:head", "")
		headErrCh <- err
	}()
	// Wait until head waiter is enqueued.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		am.admitStateMu.Lock()
		n := len(am.admitWaiters)
		am.admitStateMu.Unlock()
		if n >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	go func() {
		_, err := am.AcquireGlobalWorkerSlotExcluding(tailCtx, "primary", "sid:tail", "")
		tailErrCh <- err
	}()
	for time.Now().Before(deadline) {
		am.admitStateMu.Lock()
		n := len(am.admitWaiters)
		am.admitStateMu.Unlock()
		if n >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Cancel non-head waiter first -> must exit with context.Canceled, not 529.
	cancelTail()
	select {
	case err := <-tailErrCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled for non-head waiter cancel, got %T %v", err, err)
		}
	case <-time.After(time.Second):
		t.Fatal("non-head waiter did not exit promptly on cancel")
	}

	// Cancel head waiter next -> must exit with context.Canceled, not 529.
	cancelHead()
	select {
	case err := <-headErrCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled for head waiter cancel, got %T %v", err, err)
		}
	case <-time.After(time.Second):
		t.Fatal("head waiter did not exit promptly on cancel")
	}

	// 2c. Genuine admission timeout (caller ctx still alive) still returns 529 SoftError.
	globalAdmitWaitTimeout = 80 * time.Millisecond
	_, errTimeout := am.AcquireGlobalWorkerSlotExcluding(context.Background(), "primary", "sid:real-timeout", "")
	var se *SoftError
	if !errors.As(errTimeout, &se) || se.StatusCode != 529 {
		t.Fatalf("expected 529 SoftError on genuine admission timeout, got %T %v", errTimeout, errTimeout)
	}

	// 3. acquireGlobalWorkerSlotFromParent returns context.Canceled on HTTP 499 or canceled ctx.
	parentSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(499)
	}))
	defer parentSrv.Close()
	u, _ := url.Parse(parentSrv.URL)
	pPort, _ := strconv.Atoi(u.Port())
	if _, err := acquireGlobalWorkerSlotFromParent(context.Background(), pPort, "", "child-a", "sid:x", ""); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled when parent returns 499, got %v", err)
	}
	if _, err := acquireGlobalWorkerSlotFromParent(preCtx, pPort, "", "child-a", "sid:x", ""); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled on pre-canceled ctx to parent, got %v", err)
	}
}

func TestV2327_DecoupledChildHealthzPollerSurvivesCallerCancel(t *testing.T) {
	// 1. First caller cancels after 30ms while child /healthz becomes 200 OK after 120ms.
	// The shared booting worker must NOT be killed, must NOT incur crash backoff, and waiter 2 must succeed!
	var ready atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" && ready.Load() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true}`))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	am := NewAccountManager()
	acc := AccountProfile{ID: "child-boot", Label: "Child Boot", Enabled: true}
	proc := &accountWorkerProc{
		id:        acc.ID,
		gwPort:    29911,
		dashPort:  0,
		transport: &http.Transport{},
		done:      make(chan struct{}),
		readyCh:   make(chan struct{}),
	}
	am.mu.Lock()
	am.workers[acc.ID] = proc
	am.mu.Unlock()

	var cleanupCalled atomic.Bool
	cleanupFailed := func(err error) {
		cleanupCalled.Store(true)
		am.mu.Lock()
		if am.workers[acc.ID] == proc {
			delete(am.workers, acc.ID)
		}
		proc.stopped = true
		proc.finishReady(err)
		am.mu.Unlock()
	}

	go am.pollAccountWorkerReady(acc, proc, srv.URL+"/healthz", 2*time.Second, cleanupFailed)

	waiter := func(ctx context.Context) error {
		select {
		case <-proc.readyCh:
			return proc.readyErr
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	ctx1, cancel1 := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel1()
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()

	err1Ch := make(chan error, 1)
	err2Ch := make(chan error, 1)
	go func() { err1Ch <- waiter(ctx1) }()
	go func() { err2Ch <- waiter(ctx2) }()

	// Waiter 1 cancels early.
	if err1 := <-err1Ch; !errors.Is(err1, context.DeadlineExceeded) {
		t.Fatalf("expected waiter 1 DeadlineExceeded, got %v", err1)
	}
	// Mark child /healthz ready 90ms after waiter 1 cancelled.
	time.Sleep(90 * time.Millisecond)
	ready.Store(true)

	// Waiter 2 MUST succeed!
	if err2 := <-err2Ch; err2 != nil {
		t.Fatalf("expected waiter 2 to succeed after waiter 1 cancelled, got %v", err2)
	}
	if cleanupCalled.Load() {
		t.Fatal("cleanupFailed must NOT be called when only a caller ctx cancels")
	}
	am.mu.RLock()
	stillRegistered := am.workers[acc.ID] == proc && !proc.stopped && !proc.healthyAt.IsZero()
	crashCount := am.crashStates[acc.ID].failures
	am.mu.RUnlock()
	if !stillRegistered || crashCount != 0 {
		t.Fatalf("expected worker healthy & zero crash backoff, registered=%v crashCount=%d", stillRegistered, crashCount)
	}
	running, gwP, _ := am.IsWorkerRunning(acc.ID)
	if !running || gwP != 29911 {
		t.Fatalf("expected IsWorkerRunning=true (port 29911) via transport without proc.proxy, got running=%v port=%d", running, gwP)
	}

	// 2. Early child exit (proc.done closed) MUST trigger cleanupFailed and unblock waiter immediately.
	procCrash := &accountWorkerProc{
		id:        "child-crash",
		gwPort:    29912,
		transport: &http.Transport{},
		done:      make(chan struct{}),
		readyCh:   make(chan struct{}),
	}
	am.mu.Lock()
	am.workers[procCrash.id] = procCrash
	am.mu.Unlock()
	var crashCleanup atomic.Bool
	go am.pollAccountWorkerReady(AccountProfile{ID: procCrash.id}, procCrash, "http://127.0.0.1:1/healthz", 2*time.Second, func(err error) {
		crashCleanup.Store(true)
		procCrash.finishReady(err)
	})
	close(procCrash.done)
	select {
	case <-procCrash.readyCh:
		if procCrash.readyErr == nil || !crashCleanup.Load() {
			t.Fatalf("expected cleanupFailed and error on early proc.done, err=%v cleanup=%v", procCrash.readyErr, crashCleanup.Load())
		}
	case <-time.After(time.Second):
		t.Fatal("poller did not abort promptly on proc.done")
	}

	// 3. Real boot timeout MUST trigger cleanupFailed and unblock waiter with timeout error.
	procSlow := &accountWorkerProc{
		id:        "child-slow",
		gwPort:    29913,
		transport: &http.Transport{},
		done:      make(chan struct{}),
		readyCh:   make(chan struct{}),
	}
	am.mu.Lock()
	am.workers[procSlow.id] = procSlow
	am.mu.Unlock()
	ready.Store(false)
	var timeoutCleanup atomic.Bool
	go am.pollAccountWorkerReady(AccountProfile{ID: procSlow.id}, procSlow, srv.URL+"/healthz", 120*time.Millisecond, func(err error) {
		timeoutCleanup.Store(true)
		procSlow.finishReady(err)
	})
	select {
	case <-procSlow.readyCh:
		if procSlow.readyErr == nil || !strings.Contains(procSlow.readyErr.Error(), "healthz timeout") || !timeoutCleanup.Load() {
			t.Fatalf("expected healthz timeout cleanup, err=%v cleanup=%v", procSlow.readyErr, timeoutCleanup.Load())
		}
	case <-time.After(time.Second):
		t.Fatal("poller did not enforce bootTimeout")
	}
}

func TestV2327_StreamingImageGenErrorClassificationAndNoWarmRefresh(t *testing.T) {
	// 1. extractJSONErrorEnvelope preserves string eo.Code into errStatus when eo.Type != "".
	rawImgErr := []byte(`{"error":{"type":"api_error","code":"image_generation_failed","message":"model produced no image artifact"}}`)
	isErr, et, es, em, ec := extractJSONErrorEnvelope(rawImgErr)
	if !isErr || et != "api_error" || es != "image_generation_failed" || em != "model produced no image artifact" || ec != 0 {
		t.Fatalf("extractJSONErrorEnvelope got (%v, %q, %q, %q, %d)", isErr, et, es, em, ec)
	}

	// 2. classifyStructuredStreamError maps timeout_error->504 and api_error/image_*->502.
	streamCases := []struct {
		errType, errStatus, errMsg string
		wantCode                   int
	}{
		{"timeout_error", "", "image generation timed out", http.StatusGatewayTimeout},
		{"api_error", "timeout_error", "deadline exceeded", http.StatusGatewayTimeout},
		{"api_error", "image_generation_failed", "no image", http.StatusBadGateway},
		{"error", "image_artifact_error", "corrupt png", http.StatusBadGateway},
		{"error", "image_read_failed", "read failed", http.StatusBadGateway},
		{"error", "session_init_failed", "session failed", http.StatusBadGateway},
		{"api_error", "", "generic upstream api failure", http.StatusBadGateway},
	}
	for _, tc := range streamCases {
		failed, sc, _, _ := classifyStructuredStreamError(tc.errType, tc.errStatus, tc.errMsg, 0)
		if !failed || sc != tc.wantCode {
			t.Fatalf("classifyStructuredStreamError(%q,%q,%q) = (%v,%d), want (true,%d)",
				tc.errType, tc.errStatus, tc.errMsg, failed, sc, tc.wantCode)
		}
	}

	// 3. inspectStreamTailForUpstreamError catches SSE heartbeat + imagegen error frame.
	sseTail := []byte(": keepalive\n\ndata: {\"error\":{\"type\":\"api_error\",\"code\":\"image_generation_failed\",\"message\":\"generation failed\"}}\n\ndata: [DONE]\n\n")
	failed, sc, outType, _ := inspectStreamTailForUpstreamError(sseTail)
	if !failed || sc != http.StatusBadGateway || outType != "api_error" {
		t.Fatalf("inspectStreamTailForUpstreamError got (%v,%d,%q), want (true,502,api_error)", failed, sc, outType)
	}

	// 4. End-to-end TryRouteAPIRequestWithFinish: streaming imagegen error after 200 SSE heartbeat
	// must NOT refresh st.lastImageGenAt and MUST cool down image_gen (not gemini).
	origAM := GlobalAccountManager
	origDB := GatewayStateDB
	t.Cleanup(func() {
		GlobalAccountManager = origAM
		initConfig()
		InitGlobalStore(origDB)
	})

	tmp := t.TempDir()
	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "v2327_bug05.db"))
	t.Setenv("AGY_GW_LOG", filepath.Join(tmp, "gateway.log"))
	t.Setenv("AGY_CHILD_ACCOUNT_WORKER", "")
	initConfig()

	childSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(": heartbeat\n\n"))
		_, _ = w.Write([]byte("data: {\"error\":{\"type\":\"api_error\",\"code\":\"image_generation_failed\",\"message\":\"image generation failed\"}}\n\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer childSrv.Close()
	u, _ := url.Parse(childSrv.URL)
	cPort, _ := strconv.Atoi(u.Port())

	am := NewAccountManager()
	am.baseDataDir = tmp
	am.accounts = []AccountProfile{
		{ID: "primary", Label: "Primary", Primary: true, Enabled: false, SchedState: SchedStateDisabled},
		{ID: "child-img", Label: "Child Img", Primary: false, Enabled: true, Priority: 10, AllowedModels: []string{imageGenServedModel, "gemini-2.5-pro"}},
	}
	am.enabledAccountCount.Store(1)
	am.routing = AccountPoolConfig{Strategy: RouteStrategyFillFirst}.Normalized()
	if err := writeTestDummyOAuth(am, "child-img"); err != nil {
		t.Fatal(err)
	}
	p := &accountWorkerProc{
		id:        "child-img",
		gwPort:    cPort,
		dashPort:  cPort,
		transport: &http.Transport{},
		done:      make(chan struct{}),
		readyCh:   make(chan struct{}),
		healthyAt: time.Now(),
	}
	p.finishReady(nil)
	am.workers["child-img"] = p
	GlobalAccountManager = am

	body := []byte(fmt.Sprintf(`{"model":%q,"stream":true,"messages":[{"role":"user","content":"draw a cat"}]}`, imageGenServedModel))
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	_, _, finish, handled := am.TryRouteAPIRequestWithFinish(rr, req)
	finish()
	if !handled {
		t.Fatal("expected imagegen chat request to be routed to child-img")
	}
	if !am.AccountLastImageGenAt("child-img").IsZero() {
		t.Fatalf("streaming imagegen error must NOT refresh lastImageGenAt, got %v", am.AccountLastImageGenAt("child-img"))
	}
	if cooling, _, _ := am.isAccountFamilyCoolingDown("child-img", imageGenServedModel, time.Now()); !cooling {
		t.Fatal("expected image_gen cooldown on child-img after 502 stream error")
	}
	if cooling, _, _ := am.isAccountFamilyCoolingDown("child-img", "gemini-2.5-pro", time.Now()); cooling {
		t.Fatal("image_gen 502 stream error must NOT cool down gemini text family")
	}
}

func TestV2327_GlobalSpawnLeaseTTLExpiryCgroupParserAndParallelEviction(t *testing.T) {
	// 1. globalSpawnLeaseTTL auto-expiry unblocks queued admission without manual release.
	origAM := GlobalAccountManager
	origTTL := globalSpawnLeaseTTL
	origWait := globalAdmitWaitTimeout
	origPoll := globalAdmitPollInterval
	origDB := GatewayStateDB
	t.Cleanup(func() {
		GlobalAccountManager = origAM
		globalSpawnLeaseTTL = origTTL
		globalAdmitWaitTimeout = origWait
		globalAdmitPollInterval = origPoll
		initConfig()
		InitGlobalStore(origDB)
	})

	tmp := t.TempDir()
	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "v2327_lease.db"))
	t.Setenv("AGY_GW_LOG", filepath.Join(tmp, "gateway.log"))
	t.Setenv("AGY_GLOBAL_BURST_MAX_LIVE_WORKERS", "1")
	t.Setenv("AGY_CHILD_ACCOUNT_WORKER", "")
	initConfig()

	globalSpawnLeaseTTL = 80 * time.Millisecond
	globalAdmitWaitTimeout = 2 * time.Second
	globalAdmitPollInterval = 20 * time.Millisecond

	restoreMem := setCgroupMemoryReaderForTest(func() CgroupMemoryStatus {
		return CgroupMemoryStatus{Version: 2, HasLiveUsage: true, LimitBytes: 6 << 30, WorkingSetBytes: 2 << 30, UsagePct: 33.3}
	})
	defer restoreMem()

	eng := newV2322CloseTrackEngine(nil)
	gs := NewGatewayServerWithDeps(eng, &StateStore{})
	defer gs.Close()

	am := NewAccountManager()
	am.accounts = []AccountProfile{{ID: "primary", Label: "Primary", Primary: true, Enabled: true, MaxLiveWorkers: 8}}
	am.enabledAccountCount.Store(1)
	am.setPrimaryGatewayForTest(gs)
	GlobalAccountManager = am

	lease1, err := am.AcquireGlobalWorkerSlotExcluding(context.Background(), "primary", "sid:l1", "")
	if err != nil || lease1 == "" {
		t.Fatalf("expected lease1 granted, got lease=%q err=%v", lease1, err)
	}
	if got := am.ActiveGlobalSpawnLeases(); got != 1 {
		t.Fatalf("expected 1 active spawn lease, got %d", got)
	}
	// Do NOT release lease1; lease2 must succeed automatically after lease1's 80ms TTL expires.
	t0 := time.Now()
	lease2, err := am.AcquireGlobalWorkerSlotExcluding(context.Background(), "primary", "sid:l2", "")
	elapsed := time.Since(t0)
	if err != nil || lease2 == "" || lease2 == lease1 {
		t.Fatalf("expected lease2 granted after lease1 TTL expiry, got lease=%q err=%v", lease2, err)
	}
	if elapsed < 50*time.Millisecond {
		t.Fatalf("expected lease2 to wait for lease1 TTL expiry, elapsed=%v", elapsed)
	}
	am.ReleaseGlobalWorkerSlot(lease2)
	if got := am.ActiveGlobalSpawnLeases(); got != 0 {
		t.Fatalf("expected 0 active spawn leases after release, got %d", got)
	}

	// 2. Zero-alloc cgroup v2/v1 parser edge cases (tabs, total_* precedence, MemTotal parsing).
	v1Stat := "rss 100\ntotal_rss\t2000000000\ncache 50\ntotal_cache 1000000000\nactive_file 10\ntotal_active_file 250000000\ninactive_file 5\ntotal_inactive_file 500000000\n"
	stV1 := parseCgroupMemoryStatus("", "max\n", "", "3000000000\n", "4000000000\n", v1Stat, "MemTotal:       8388608 kB\n")
	if stV1.Version != 1 || stV1.AnonBytes != 2000000000 || stV1.FileBytes != 1000000000 || stV1.ActiveFileBytes != 250000000 || stV1.InactiveFile != 500000000 || stV1.WorkingSetBytes != 2500000000 {
		t.Fatalf("unexpected zero-alloc cgroup v1 status: %+v", stV1)
	}
	if gotMem := parseProcMeminfoTotalBytes("MemFree: 100 kB\nMemTotal:\t4194304 kB\n"); gotMem != 4194304*1024 {
		t.Fatalf("parseProcMeminfoTotalBytes = %d, want %d", gotMem, 4194304*1024)
	}

	// 3. PERF-ADMIT-01: hibernateGlobalVictimBatch executes multi-account evictions in parallel.
	var inflight atomic.Int32
	var maxInflight atomic.Int32
	makeChildEvictSrv := func(accID string) (*httptest.Server, int) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/internal/hibernate-lru" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			cur := inflight.Add(1)
			for {
				prev := maxInflight.Load()
				if cur <= prev || maxInflight.CompareAndSwap(prev, cur) {
					break
				}
			}
			time.Sleep(60 * time.Millisecond)
			inflight.Add(-1)
			var req HibernateLRURequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(HibernateLRUResponse{
				Hibernated:         len(req.ConvKeys),
				HibernatedConvKeys: req.ConvKeys,
			})
		}))
		u, _ := url.Parse(srv.URL)
		p, _ := strconv.Atoi(u.Port())
		return srv, p
	}
	srv1, p1 := makeChildEvictSrv("c1")
	defer srv1.Close()
	srv2, p2 := makeChildEvictSrv("c2")
	defer srv2.Close()

	victims := []globalWorkerCandidate{
		{target: globalWorkerTarget{accountID: "c1", dashPort: p1}, cand: WorkerSlotCandidate{ConvKey: "sid:c1-a", Status: "IDLE"}},
		{target: globalWorkerTarget{accountID: "c2", dashPort: p2}, cand: WorkerSlotCandidate{ConvKey: "sid:c2-a", Status: "IDLE"}},
	}
	hibN, byAcc, evSet := am.hibernateGlobalVictimBatch(context.Background(), victims, false, "test_parallel")
	if hibN != 2 || byAcc["c1"] != 1 || byAcc["c2"] != 1 || !evSet["c1|sid:c1-a"] || !evSet["c2|sid:c2-a"] {
		t.Fatalf("unexpected parallel eviction result: hibN=%d byAcc=%+v evSet=%+v", hibN, byAcc, evSet)
	}
	if maxInflight.Load() < 2 {
		t.Fatalf("expected parallel multi-account eviction (maxInflight>=2), got %d", maxInflight.Load())
	}
}
