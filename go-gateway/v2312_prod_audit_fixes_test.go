package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"agy-acp-gateway-go/internal/acp"
	gwcfg "agy-acp-gateway-go/internal/config"
)

func extractFirstToolUse(t *testing.T, resp map[string]any) ToolCallItem {
	t.Helper()
	blocks, _ := resp["content"].([]any)
	for _, b := range blocks {
		bm, _ := b.(map[string]any)
		if bm != nil && bm["type"] == "tool_use" {
			id, _ := bm["id"].(string)
			name, _ := bm["name"].(string)
			inp, _ := bm["input"].(map[string]any)
			return ToolCallItem{ID: id, Name: name, Input: inp}
		}
	}
	t.Fatalf("no tool_use block found in response: %+v", resp)
	return ToolCallItem{}
}

func TestRPCWithActivityExtendsPastIdleAndFailsOnStallAndMaxDuration(t *testing.T) {
	t.Run("activity keeps RPC alive past idleTimeout until completion", func(t *testing.T) {
		eng := &SharedAcpEngine{}
		rPipe, wPipe, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		defer rPipe.Close()
		defer wPipe.Close()
		go func() { _, _ = io.Copy(io.Discard, rPipe) }()

		eng.mu.Lock()
		eng.stdin = bufio.NewWriter(wPipe)
		eng.mu.Unlock()

		start := time.Now()
		var actNs atomic.Int64
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		doneCh := make(chan error, 1)
		go func() {
			_, rpcErr := eng.RPCWithActivity(ctx, "session/prompt", map[string]any{"sessionId": "s1"}, 250*time.Millisecond, 3*time.Second, &actNs, start)
			doneCh <- rpcErr
		}()

		// Keep touching activity past 1.5x idleTimeout (~360ms), then deliver RPC result.
		for i := 0; i < 9; i++ {
			time.Sleep(40 * time.Millisecond)
			actNs.Store(int64(time.Since(start)))
		}
		resolved := false
		eng.pending.Range(func(k, v any) bool {
			ch := v.(chan rpcResult)
			ch <- rpcResult{Result: json.RawMessage(`{"stopReason":"end_turn"}`)}
			resolved = true
			return false
		})
		if !resolved {
			t.Fatal("expected pending RPC entry to still be alive after activity resets")
		}
		if rpcErr := <-doneCh; rpcErr != nil {
			t.Fatalf("expected clean completion after activity resets, got %v", rpcErr)
		}
	})

	t.Run("true silence triggers idle timeout and cleans pending entry", func(t *testing.T) {
		eng := &SharedAcpEngine{}
		rPipe, wPipe, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		defer rPipe.Close()
		defer wPipe.Close()
		go func() { _, _ = io.Copy(io.Discard, rPipe) }()
		eng.mu.Lock()
		eng.stdin = bufio.NewWriter(wPipe)
		eng.mu.Unlock()

		start := time.Now()
		var actNs atomic.Int64
		_, rpcErr := eng.RPCWithActivity(context.Background(), "session/prompt", map[string]any{"sessionId": "s2"}, 80*time.Millisecond, 2*time.Second, &actNs, start)
		if rpcErr == nil || !strings.Contains(rpcErr.Error(), "timed out after") || !strings.Contains(rpcErr.Error(), "idle") {
			t.Fatalf("expected idle timeout error, got %v", rpcErr)
		}
		if !isNonTransientOrFatalUpstreamErr(rpcErr) {
			t.Fatalf("idle timeout error must remain classified as non-transient: %v", rpcErr)
		}
		cnt := 0
		eng.pending.Range(func(_, _ any) bool { cnt++; return true })
		if cnt != 0 {
			t.Fatalf("expected pending map drained, got %d", cnt)
		}
	})

	t.Run("continuous activity still respects maxDuration ceiling", func(t *testing.T) {
		eng := &SharedAcpEngine{}
		rPipe, wPipe, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		defer rPipe.Close()
		defer wPipe.Close()
		go func() { _, _ = io.Copy(io.Discard, rPipe) }()
		eng.mu.Lock()
		eng.stdin = bufio.NewWriter(wPipe)
		eng.mu.Unlock()

		start := time.Now()
		var actNs atomic.Int64
		stopTouch := make(chan struct{})
		go func() {
			tk := time.NewTicker(10 * time.Millisecond)
			defer tk.Stop()
			for {
				select {
				case <-stopTouch:
					return
				case <-tk.C:
					actNs.Store(int64(time.Since(start)))
				}
			}
		}()
		_, rpcErr := eng.RPCWithActivity(context.Background(), "session/prompt", map[string]any{"sessionId": "s3"}, 250*time.Millisecond, 420*time.Millisecond, &actNs, start)
		close(stopTouch)
		if rpcErr == nil || !strings.Contains(rpcErr.Error(), "timed out after max duration") {
			t.Fatalf("expected max duration timeout error, got %v", rpcErr)
		}
	})
}

type parkedTimeoutMockEngine struct {
	mu       sync.Mutex
	closed   []string
	promptCh chan error
}

func (m *parkedTimeoutMockEngine) EnsureStarted() error                         { return nil }
func (m *parkedTimeoutMockEngine) StopEngine()                                  {}
func (m *parkedTimeoutMockEngine) IsAlive() bool                                { return true }
func (m *parkedTimeoutMockEngine) IsBooting() bool                              { return false }
func (m *parkedTimeoutMockEngine) CurrentGen() uint64                           { return 1 }
func (m *parkedTimeoutMockEngine) Capabilities() acp.Capabilities               { return acp.Capabilities{} }
func (m *parkedTimeoutMockEngine) BinaryInfo() map[string]any                   { return nil }
func (m *parkedTimeoutMockEngine) EnqueueControlReply(msg map[string]any)       {}
func (m *parkedTimeoutMockEngine) RegisterListener(string, acp.SessionListener) {}
func (m *parkedTimeoutMockEngine) UnregisterSession(string)                     {}
func (m *parkedTimeoutMockEngine) CancelSession(string)                         {}
func (m *parkedTimeoutMockEngine) CloseSession(sid string) error {
	m.mu.Lock()
	m.closed = append(m.closed, sid)
	m.mu.Unlock()
	return nil
}
func (m *parkedTimeoutMockEngine) RPC(method string, params any, timeout time.Duration) (json.RawMessage, error) {
	if method == "session/prompt" {
		err := <-m.promptCh
		return nil, err
	}
	return json.RawMessage(`{}`), nil
}

func TestParkedPromptRPCTimeoutCleansUpSession(t *testing.T) {
	mock := &parkedTimeoutMockEngine{promptCh: make(chan error, 1)}
	sess := &AcpProjectSession{
		ConvKey:          "sid:parked-timeout-test:u:1",
		SessionID:        "sid-parked-to",
		engineGen:        1,
		Engine:           mock,
		MessageCount:     4,
		ParkedCalls:      make(map[string]*ParkedMcpCall),
		RelayedCalls:     make(map[string]*ParkedMcpCall),
		McpToHostMap:     make(map[string]string),
		DeclaredMcpTools: make(map[string]bool),
	}
	parkedResCh := make(chan map[string]any, 1)
	sess.ParkedCalls["call-1"] = &ParkedMcpCall{
		Call:       ToolCallItem{ID: "call-1", Name: "Bash"},
		ResultChan: parkedResCh,
	}

	// Start executePromptOrPark, then immediately resolve origTurn as tool_use (simulating parked tool),
	// then fail the background session/prompt RPC (idle timeout while parked).
	doneTurn := make(chan struct{})
	go func() {
		_, _ = sess.executePromptOrPark(context.Background(), []map[string]any{{"type": "text", "text": "go"}}, nil, 0)
		close(doneTurn)
	}()

	var origTurn *activeTurnState
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		sess.mu.Lock()
		tr := sess.activeTurn
		sess.mu.Unlock()
		if tr != nil {
			origTurn = tr
			tr.mu.Lock()
			tr.resolved = true
			tr.mu.Unlock()
			select {
			case tr.resolveChan <- TurnResult{StopReason: "tool_use", ToolCalls: []ToolCallItem{{ID: "call-1", Name: "Bash"}}}:
			default:
			}
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	<-doneTurn

	// Now simulate the background session/prompt RPC timing out while parked (no HTTP turn in flight).
	mock.promptCh <- errors.New("ACP RPC session/prompt timed out after 30m0s idle")

	select {
	case msg := <-parkedResCh:
		if isErr, _ := msg["isError"].(bool); !isErr {
			t.Fatalf("expected parked tool channel unblocked with isError=true, got %+v", msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for parked tool ResultChan to unblock after parked prompt RPC error")
	}

	if origTurn != nil {
		select {
		case <-origTurn.bgDone:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for background executePromptOrPark goroutine to exit")
		}
	}

	mock.mu.Lock()
	nClosed := len(mock.closed)
	mock.mu.Unlock()
	sess.mu.Lock()
	sidNow := sess.SessionID
	sess.mu.Unlock()
	if nClosed == 0 || sidNow != "" {
		t.Fatalf("expected HibernateIfSession(prompt_rpc_error) to close wedged ACP session, closed=%d sid=%q", nClosed, sidNow)
	}
}

func TestSubagentStickyKeyIsolationAndLocalSlot529(t *testing.T) {
	t.Run("extractRequestRoutingHintsFull scopes subagents independently from parent", func(t *testing.T) {
		mkReq := func(hdrAgent, metaJSON string) *http.Request {
			body := `{"model":"gemini-4-neon","messages":[{"role":"user","content":"hi"}]`
			if metaJSON != "" {
				body += `,"metadata":` + metaJSON
			}
			body += `}`
			req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(body))
			req.Header.Set("X-Session-Id", "sess-shared-1")
			if hdrAgent != "" {
				req.Header.Set("x-agent-id", hdrAgent)
			}
			return req
		}

		_, kMain, _, _, _, _, _, _, _ := extractRequestRoutingHintsFull(mkReq("", ""))
		_, kSub1, _, _, _, _, _, _, _ := extractRequestRoutingHintsFull(mkReq("agent-a", ""))
		_, kSub2, _, _, _, _, _, _, _ := extractRequestRoutingHintsFull(mkReq("", `{"agent_id":"agent-b"}`))
		_, kSub3, _, _, _, _, _, _, _ := extractRequestRoutingHintsFull(mkReq("", `{"user_id":"{\"session_id\":\"sess-shared-1\",\"agent_id\":\"agent-c\"}"}`))

		if kMain != "hdr:sess-shared-1" {
			t.Fatalf("expected main sticky key hdr:sess-shared-1, got %q", kMain)
		}
		if kSub1 != "hdr:sess-shared-1:ag:agent-a" || kSub2 != "hdr:sess-shared-1:ag:agent-b" || kSub3 != "hdr:sess-shared-1:ag:agent-c" {
			t.Fatalf("unexpected subagent sticky keys: sub1=%q sub2=%q sub3=%q", kSub1, kSub2, kSub3)
		}
	})

	t.Run("local slot-full 529 skips upstream cooldown and slot hint guides Turn-1 without breaking sticky U8", func(t *testing.T) {
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
				{ID: "pro1", Label: "Pro-1", Enabled: true, Primary: false, Priority: 1, MaxLiveWorkers: 1, SchedState: SchedStateActive, AllowedModels: []string{"gemini-4-neon"}},
				{ID: "ultra", Label: "Ultra", Enabled: true, Primary: true, Priority: 2, MaxLiveWorkers: 16, SchedState: SchedStateActive, AllowedModels: []string{"gemini-4-neon"}},
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
		tmpDir := t.TempDir()
		am.baseDataDir = tmpDir
		t.Setenv("AGY_STATE_DB", filepath.Join(tmpDir, "gateway.db"))
		t.Setenv("AGY_TOS_TRIPPED_FILE", filepath.Join(tmpDir, "tos-tripped"))
		t.Setenv("AGY_PURE_API_HOME", filepath.Join(tmpDir, "primary-pure"))
		initConfig()
		resetTosBreakerForTest()
		for _, id := range []string{"pro1", "ultra"} {
			var tokPath string
			if id == "ultra" {
				tokPath = filepath.Join(PureApiHome, "antigravity-cli", "antigravity-oauth-token")
			} else {
				tokPath = filepath.Join(tmpDir, "accounts", id, ".gemini", "antigravity-cli", "antigravity-oauth-token")
			}
			_ = os.MkdirAll(filepath.Dir(tokPath), 0o700)
			_ = os.WriteFile(tokPath, []byte("dummy-oauth-token-fixture-32bytes"), 0o600)
		}

		hdr := make(http.Header)
		hdr.Set(internalLocalSlotFullHeader, "1")
		if !isLocalWorkerSlotError(529, "overloaded_error", "anything", hdr) {
			t.Fatal("expected header-tagged local slot error to match")
		}
		if !isLocalWorkerSlotError(529, "overloaded_error", "All live worker slots are busy; retry shortly", nil) {
			t.Fatal("expected message-matched local slot error to match")
		}
		if !isLocalWorkerSlotError(http.StatusTooManyRequests, "overloaded_error", "All live worker slots are busy; retry shortly", nil) {
			t.Fatal("expected Gemini v1beta remapped-429 local slot error to match")
		}
		if isLocalWorkerSlotError(529, "overloaded_error", "Upstream model overloaded", nil) {
			t.Fatal("genuine upstream 529 must NOT be classified as local worker slot error")
		}
		// Verify pck: + x-subagent-id also scopes subagents independently.
		reqPck := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gemini-4-neon","prompt_cache_key":"pck-main","input":"hi"}`))
		reqPck.Header.Set("x-subagent-id", "sub-9")
		_, kPck, _, _, _, _, _, _, _ := extractRequestRoutingHintsFull(reqPck)
		if kPck != "pck:pck-main:ag:sub-9" {
			t.Fatalf("expected pck subagent key pck:pck-main:ag:sub-9, got %q", kPck)
		}
		// Verify Primary in-process occupied slots callback feeds EffectiveOccupiedSlots symmetrically.
		am.SetPrimaryOccupiedSlotsFn(func() int { return 6 })
		if occ := am.EffectiveOccupiedSlots("ultra", 0, 16, time.Now()); occ != 6 {
			t.Fatalf("expected primary EffectiveOccupiedSlots=6 via callback, got %d", occ)
		}
		am.SetPrimaryOccupiedSlotsFn(nil)

		dummyReq := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
		res1 := am.ResolveTargetAccountEx(dummyReq, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:conv-pro1", DeferStickyCommit: true})
		if res1.DenyErr != nil || res1.Account.ID != "pro1" {
			t.Fatalf("expected Turn-1 fill-first pick pro1, got acc=%q err=%v", res1.Account.ID, res1.DenyErr)
		}
		res1.CommitSticky()

		// Capture fresh timestamp right before RecordLocalSlotHint so 5s TTL cannot expire under -race stalls.
		now := time.Now()
		// Child reports 1/1 occupied slot (hot-parked) -> no upstream 529 penalty!
		am.RecordLocalSlotHint("pro1", 1, false, now)
		hvs := am.SnapshotHealthViews(am.accounts, nil, now)
		if hvs["pro1"].Error529Count != 0 || hvs["pro1"].CooldownSec != 0 {
			t.Fatalf("local slot hint must not penalize upstream 529 health: %+v", hvs["pro1"])
		}

		// NEW Turn-1 for another conversation must skip full pro1 and land directly on ultra!
		resNew := am.ResolveTargetAccountEx(dummyReq, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:conv-new"})
		if resNew.DenyErr != nil || resNew.Account.ID != "ultra" {
			t.Fatalf("expected NEW Turn-1 to avoid 1/1 occupied pro1 and pick ultra, got acc=%q err=%v", resNew.Account.ID, resNew.DenyErr)
		}

		// CONTINUATION for conv-pro1 MUST still stick to pro1 (Decision U8 + reuses its own parked slot)!
		resCont := am.ResolveTargetAccountEx(dummyReq, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:conv-pro1", IsContinuation: true})
		if resCont.DenyErr != nil || resCont.Account.ID != "pro1" || !resCont.IsStickyHit {
			t.Fatalf("expected continuation to stay pinned to pro1 despite 1/1 occupied slot, got acc=%q sticky=%v err=%v", resCont.Account.ID, resCont.IsStickyHit, resCont.DenyErr)
		}
	})
}

func TestMaxConcurrentPromptsAutoScalingAndPrimarySync(t *testing.T) {
	prevDB := GatewayStateDB
	t.Cleanup(func() {
		GlobalAccountManager.primaryMaxLiveWorkers.Store(0)
		initConfig()
		InitGlobalStore(prevDB)
	})
	t.Setenv("AGY_MAX_LIVE_WORKERS", "16")
	t.Setenv("AGY_MAX_CONCURRENT_PROMPTS", "")
	snap := gwcfg.Reload()
	if snap.MaxConcurrentPromptsEnvSet {
		t.Fatal("expected MaxConcurrentPromptsEnvSet=false when env unset")
	}
	if snap.MaxConcurrentPrompts != 16 {
		t.Fatalf("expected MaxConcurrentPrompts auto-scaled to 16, got %d", snap.MaxConcurrentPrompts)
	}

	t.Setenv("AGY_MAX_CONCURRENT_PROMPTS", "5")
	snap2 := gwcfg.Reload()
	if !snap2.MaxConcurrentPromptsEnvSet || snap2.MaxConcurrentPrompts != 5 {
		t.Fatalf("expected explicit override 5, got envSet=%v val=%d", snap2.MaxConcurrentPromptsEnvSet, snap2.MaxConcurrentPrompts)
	}

	t.Setenv("AGY_MAX_LIVE_WORKERS", "8")
	t.Setenv("AGY_MAX_CONCURRENT_PROMPTS", "")
	initConfig()
	if MaxConcurrentPrompts != 8 || cap(promptSem) != 8 {
		t.Fatalf("expected boot default 8, got %d / cap=%d", MaxConcurrentPrompts, cap(promptSem))
	}
	GlobalAccountManager.primaryMaxLiveWorkers.Store(16)
	syncPrimaryPromptSemBeforeServe()
	if MaxConcurrentPrompts != 16 || cap(promptSem) != 16 {
		t.Fatalf("expected syncPrimaryPromptSemBeforeServe to resize to 16, got %d / cap=%d", MaxConcurrentPrompts, cap(promptSem))
	}
}

func TestCleanupExpiredConversationsBrainAndAcpLogs(t *testing.T) {
	prevDB := GatewayStateDB
	t.Cleanup(func() {
		initConfig()
		InitGlobalStore(prevDB)
	})
	tmp := t.TempDir()
	pureHome := filepath.Join(tmp, "pure-api-home")
	appRoot := filepath.Join(pureHome, "antigravity-cli")
	convDir := filepath.Join(appRoot, "conversations")
	brainDir := filepath.Join(appRoot, "brain")
	logDir := filepath.Join(appRoot, "log")
	for _, d := range []string{convDir, brainDir, logDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}

	t.Setenv("AGY_PURE_API_HOME", pureHome)
	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "gateway.db"))
	t.Setenv("AGY_APP_CONV_DIR", "off")
	t.Setenv("AGY_CONV_KEEP_DAYS", "7")
	t.Setenv("AGY_ACP_LOG_KEEP_DAYS", "3")
	initConfig()
	gs := NewGatewayServer()
	t.Cleanup(gs.Close)

	now := time.Now()
	oldTime := now.Add(-10 * 24 * time.Hour)
	freshTime := now.Add(-2 * time.Hour)

	sidExpired := "11111111-1111-4111-8111-111111111111"
	sidSymlink := "22222222-2222-4222-8222-222222222222"
	RegisterGatewaySessionID(sidExpired)
	RegisterGatewaySessionID(sidSymlink)

	for _, sid := range []string{sidExpired, sidSymlink} {
		pb := filepath.Join(convDir, sid+".pb")
		if err := os.WriteFile(pb, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		_ = os.Chtimes(pb, oldTime, oldTime)
	}
	expBrain := filepath.Join(brainDir, sidExpired)
	_ = os.MkdirAll(expBrain, 0o700)
	_ = os.WriteFile(filepath.Join(expBrain, "step.txt"), []byte("ok"), 0o600)

	outsideDir := filepath.Join(tmp, "outside-keep")
	_ = os.MkdirAll(outsideDir, 0o700)
	_ = os.WriteFile(filepath.Join(outsideDir, "precious.txt"), []byte("keep"), 0o600)
	_ = os.Symlink(outsideDir, filepath.Join(brainDir, sidSymlink))

	oldWorkerLog := filepath.Join(logDir, "acp-worker-20261110_010203-1234.log")
	oldFrontLog := filepath.Join(logDir, "acp-front-20261110_010203-1234.log")
	freshWorkerLog := filepath.Join(logDir, "acp-worker-20261125_010203-9999.log")
	cliLog := filepath.Join(logDir, "cli-20261110_010203.log")
	for _, p := range []string{oldWorkerLog, oldFrontLog, cliLog} {
		_ = os.WriteFile(p, []byte("log"), 0o600)
		_ = os.Chtimes(p, oldTime, oldTime)
	}
	_ = os.WriteFile(freshWorkerLog, []byte("live"), 0o600)
	_ = os.Chtimes(freshWorkerLog, freshTime, freshTime)

	// Safety guard check: when AGY_APP_CONV_DIR aliases AGY_CONV_DIR, BOTH cleanup functions MUST refuse to delete!
	t.Setenv("AGY_APP_CONV_DIR", convDir)
	initConfig()
	if got := gs.CleanupExpiredConversations(now); got != 0 {
		t.Fatalf("CleanupExpiredConversations MUST refuse when AGY_APP_CONV_DIR == AGY_CONV_DIR, got %d", got)
	}
	if got := gs.CleanupExpiredAcpLogs(now); got != 0 {
		t.Fatalf("CleanupExpiredAcpLogs MUST refuse when AGY_APP_CONV_DIR aliases PureApiHome, got %d", got)
	}
	t.Setenv("AGY_APP_CONV_DIR", "off")
	initConfig()

	cleanedConvs := gs.CleanupExpiredConversations(now)
	if cleanedConvs != 2 {
		t.Fatalf("expected 2 expired convs cleaned, got %d", cleanedConvs)
	}
	if _, err := os.Stat(expBrain); !os.IsNotExist(err) {
		t.Fatalf("expected expired regular brain dir removed, err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(outsideDir, "precious.txt")); err != nil {
		t.Fatalf("symlinked outside directory must NEVER be deleted: %v", err)
	}

	cleanedLogs := gs.CleanupExpiredAcpLogs(now)
	if cleanedLogs != 2 {
		t.Fatalf("expected 2 expired acp logs cleaned, got %d", cleanedLogs)
	}
	if _, err := os.Stat(freshWorkerLog); err != nil {
		t.Fatalf("fresh active acp-worker log must be preserved: %v", err)
	}
	if _, err := os.Stat(cliLog); err != nil {
		t.Fatalf("non-matching cli log must be preserved: %v", err)
	}
}

func TestFederatedPrometheusChildWorkerMetrics(t *testing.T) {
	// Serve real GatewayServer.GetStats() JSON from the mock child so /health schema drift is caught by tests!
	childGS := NewGatewayServerWithDeps(nil, &StateStore{})
	t.Cleanup(childGS.Close)
	liveSess := NewAcpProjectSession("sid:child-live:u:1", t.TempDir(), "sys")
	liveSess.SessionID = "sid-live-1"
	liveSess.ParkedCalls["call_1"] = &ParkedMcpCall{Call: ToolCallItem{ID: "call_1"}}
	hibSess := NewAcpProjectSession("sid:child-hib:u:1", t.TempDir(), "sys")
	hibSess.HibernatedSessionID = "sid-hib-1"
	childGS.mu.Lock()
	childGS.sessions[liveSess.ConvKey] = liveSess
	childGS.sessions[hibSess.ConvKey] = hibSess
	childGS.mu.Unlock()

	childSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(childGS.GetStats())
	}))
	defer childSrv.Close()

	portStr := strings.TrimPrefix(childSrv.URL, "http://127.0.0.1:")
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("parse child test port %q: %v", portStr, err)
	}

	origAM := GlobalAccountManager
	tmpBase := t.TempDir()
	acc4Tok := filepath.Join(tmpBase, "accounts", "acc4", ".gemini", "antigravity-cli", "antigravity-oauth-token")
	_ = os.MkdirAll(filepath.Dir(acc4Tok), 0o700)
	_ = os.WriteFile(acc4Tok, []byte("dummy-oauth-token-fixture-32bytes"), 0o600)
	readyCh := make(chan struct{})
	close(readyCh)
	am := &AccountManager{
		baseDataDir: tmpBase,
		accounts: []AccountProfile{
			{ID: "tr20x", Label: "Primary", Enabled: true, Primary: true, MaxLiveWorkers: 8},
			{ID: "acc4", Label: "Ultra", Enabled: true, Primary: false, MaxLiveWorkers: 16},
			{ID: "acc-down", Label: "Down", Enabled: true, Primary: false, MaxLiveWorkers: 1},
		},
		workers: map[string]*accountWorkerProc{
			"acc4": {id: "acc4", dashPort: port, gwPort: port, proxy: &httputil.ReverseProxy{}, done: make(chan struct{}), readyCh: readyCh, healthyAt: time.Now()},
		},
		stickyLRU: newStickySessionLRU(),
	}
	GlobalAccountManager = am
	fedMetricsMu.Lock()
	fedMetricsCache = nil
	fedMetricsAt = time.Time{}
	fedMetricsMu.Unlock()
	t.Cleanup(func() {
		GlobalAccountManager = origAM
		fedMetricsMu.Lock()
		fedMetricsCache = nil
		fedMetricsAt = time.Time{}
		fedMetricsMu.Unlock()
	})

	gs := NewGatewayServer()
	t.Cleanup(gs.Close)
	out := RenderPrometheusMetrics(gs)

	for _, want := range []string{
		`agy_account_worker_up{account="tr20x"} 1`,
		`agy_account_worker_up{account="acc4"} 1`,
		`agy_account_worker_up{account="acc-down"} 0`,
		`agy_account_live_workers{account="acc4"} 1`,
		`agy_account_hibernated_sessions{account="acc4"} 1`,
		`agy_account_parked_calls{account="acc4"} 1`,
		`agy_account_prompts_inflight{account="acc4"} `,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing expected federated metric %q in:\n%s", want, out)
		}
	}
	if strings.Count(out, "# TYPE agy_account_worker_up gauge") != 1 {
		t.Fatalf("duplicate # TYPE agy_account_worker_up header in:\n%s", out)
	}
}

func TestParkedTierDegradeBreakoutAndBlipImmunity(t *testing.T) {
	setupM2IsolatedEnv(t)
	origTTL := CapacityFallbackTTL
	CapacityFallbackTTL = 5 * time.Minute
	t.Cleanup(func() { CapacityFallbackTTL = origTTL })

	baseURL, gs := startCarbonTestGateway(t)
	hdrs := map[string]string{"x-session-id": "parked-tier-breakout"}
	tools := []map[string]any{{
		"name":        "Bash",
		"description": "Run shell command",
		"input_schema": map[string]any{
			"type":       "object",
			"properties": map[string]any{"command": map[string]any{"type": "string"}},
		},
	}}

	m1 := []map[string]any{{"role": "user", "content": "[FAKE_TOOL_CALL] run check"}}
	st1, res1, raw1 := postJSONWithHeaders(t, baseURL+"/v1/messages", hdrs, map[string]any{
		"model":      "gemini-4-carbon",
		"max_tokens": 1024,
		"tools":      tools,
		"messages":   m1,
	})
	if st1 != http.StatusOK || res1["stop_reason"] != "tool_use" {
		t.Fatalf("turn 1 failed: %d %s", st1, string(raw1))
	}
	c1 := extractFirstToolUse(t, res1)

	var sess *AcpProjectSession
	gs.mu.RLock()
	for _, s := range gs.sessions {
		sess = s
	}
	gs.mu.RUnlock()
	if sess == nil {
		t.Fatal("expected session")
	}

	// Simulate: HIGH_TIER_CACHE_DEGRADE_FALLBACK armed high->medium, PARKED_TIER_SWITCH_QUEUED set
	// PendingThoughtLevel="medium", capacityFallbackUntil ALREADY EXPIRED (Hazard B1!), and
	// sustained post-queue pendingTierZeroCacheStreak == 2.
	sess.mu.Lock()
	origSid := sess.SessionID
	sess.ThoughtLevel = "high"
	sess.PendingThoughtLevel = "medium"
	sess.capacityFallbackSid = origSid
	sess.capacityFallbackTier = "medium"
	sess.capacityFallbackUntil = time.Now().Add(-1 * time.Minute)
	sess.pendingTierZeroCacheStreak = 2
	sess.mu.Unlock()

	m2 := []map[string]any{
		m1[0],
		{"role": "assistant", "content": []any{map[string]any{"type": "tool_use", "id": c1.ID, "name": c1.Name, "input": c1.Input}}},
		{"role": "user", "content": []any{map[string]any{"type": "tool_result", "tool_use_id": c1.ID, "content": "done"}}},
	}
	st2, _, raw2 := postJSONWithHeaders(t, baseURL+"/v1/messages", hdrs, map[string]any{
		"model":      "gemini-4-carbon",
		"max_tokens": 1024,
		"tools":      tools,
		"messages":   m2,
	})
	if st2 != http.StatusOK {
		t.Fatalf("turn 2 breakout failed: %d %s", st2, string(raw2))
	}

	sess.mu.Lock()
	finalTier := sess.ThoughtLevel
	finalPending := sess.PendingThoughtLevel
	finalSid := sess.SessionID
	finalUntil := sess.capacityFallbackUntil
	lastTurn := sess.TurnHistory[len(sess.TurnHistory)-1]
	sess.mu.Unlock()

	if finalTier != "medium" || finalPending != "" {
		t.Fatalf("expected breakout to materialize ThoughtLevel=medium Pending='', got tier=%q pending=%q", finalTier, finalPending)
	}
	if finalSid != origSid {
		t.Fatalf("expected Option B resume on same SessionID=%q, got %q", origSid, finalSid)
	}
	if !finalUntil.After(time.Now().Add(4 * time.Minute)) {
		t.Fatalf("Hazard B1: expected refreshed full CapacityFallbackTTL cooldown, got %v", finalUntil)
	}
	if lastTurn.Mode != "resumed_delta" || lastTurn.ThoughtLevel != "medium" {
		t.Fatalf("expected resumed_delta on medium, got mode=%q tier=%q", lastTurn.Mode, lastTurn.ThoughtLevel)
	}

	// Subtest 2: Blip immunity — pendingTierZeroCacheStreak == 1 (even with stale ZeroCacheStreak == 2)
	// MUST stay on parked_mcp_resume without breaking out!
	t.Run("single post-queue miss stays parked (blip immunity)", func(t *testing.T) {
		hdrsBlip := map[string]string{"x-session-id": "parked-tier-blip-immunity"}
		stB1, resB1, rawB1 := postJSONWithHeaders(t, baseURL+"/v1/messages", hdrsBlip, map[string]any{
			"model":      "gemini-4-carbon",
			"max_tokens": 1024,
			"tools":      tools,
			"messages":   m1,
		})
		if stB1 != http.StatusOK || resB1["stop_reason"] != "tool_use" {
			t.Fatalf("blip turn 1 failed: %d %s", stB1, string(rawB1))
		}
		cb1 := extractFirstToolUse(t, resB1)
		var bSess *AcpProjectSession
		gs.mu.RLock()
		for k, s := range gs.sessions {
			if strings.Contains(k, "parked-tier-blip-immunity") {
				bSess = s
			}
		}
		gs.mu.RUnlock()
		bSess.mu.Lock()
		bSid := bSess.SessionID
		bSess.ThoughtLevel = "high"
		bSess.PendingThoughtLevel = "medium"
		bSess.capacityFallbackSid = bSid
		bSess.capacityFallbackTier = "medium"
		bSess.capacityFallbackUntil = time.Now().Add(5 * time.Minute)
		bSess.ZeroCacheStreak = 2
		bSess.pendingTierZeroCacheStreak = 1
		bSess.mu.Unlock()

		mB2 := []map[string]any{
			m1[0],
			{"role": "assistant", "content": []any{map[string]any{"type": "tool_use", "id": cb1.ID, "name": cb1.Name, "input": cb1.Input}}},
			{"role": "user", "content": []any{map[string]any{"type": "tool_result", "tool_use_id": cb1.ID, "content": "done"}}},
		}
		stB2, _, rawB2 := postJSONWithHeaders(t, baseURL+"/v1/messages", hdrsBlip, map[string]any{
			"model":      "gemini-4-carbon",
			"max_tokens": 1024,
			"tools":      tools,
			"messages":   mB2,
		})
		if stB2 != http.StatusOK {
			t.Fatalf("blip turn 2 failed: %d %s", stB2, string(rawB2))
		}
		bSess.mu.Lock()
		bLast := bSess.TurnHistory[len(bSess.TurnHistory)-1]
		bSess.mu.Unlock()
		if bLast.Mode != "parked_mcp_resume" {
			t.Fatalf("expected blip turn to stay on parked_mcp_resume without breaking out, got mode=%q", bLast.Mode)
		}
	})
}

func TestCompactionDetectionMultiBlockAnchoredAndCodex(t *testing.T) {
	// 1. Multi-block trailing user message with tool_result + compaction + trailing focus directive
	multi := []map[string]any{
		{"role": "user", "content": []any{
			map[string]any{"type": "tool_result", "tool_use_id": "t1", "content": "ok"},
			map[string]any{"type": "text", "text": "Your task is to create a detailed summary of the conversation so far."},
			map[string]any{"type": "text", "text": "Focus on auth refactor."},
		}},
	}
	if !isCompactionSummaryRequestWithSystem("", multi) {
		t.Fatal("expected multi-block compaction with trailing focus text to be detected")
	}
	tail := extractCompactionTailText(multi)
	if !strings.Contains(tail, "create a detailed summary") || !strings.Contains(tail, "Focus on auth refactor.") {
		t.Fatalf("expected extractCompactionTailText to join compaction + focus blocks, got %q", tail)
	}

	// 2. Quoted compaction phrase alongside tool_result must NOT false-positive!
	quoted := []map[string]any{
		{"role": "user", "content": []any{
			map[string]any{"type": "tool_result", "tool_use_id": "t1", "content": "file read"},
			map[string]any{"type": "text", "text": `Why does the code check "create a detailed summary of the conversation"?`},
		}},
	}
	if isCompactionSummaryRequestWithSystem("", quoted) {
		t.Fatal("quoted compaction phrase alongside tool_result must NOT match as compaction")
	}

	// 3. Codex sysText compaction ending with tool_result only (no trailing user text) MUST detect!
	codexOnlyTR := []map[string]any{
		{"role": "user", "content": []any{
			map[string]any{"type": "tool_result", "tool_use_id": "t1", "content": "done"},
		}},
	}
	if !isCompactionSummaryRequestWithSystem("You are performing a CONTEXT CHECKPOINT COMPACTION. Summarize everything.", codexOnlyTR) {
		t.Fatal("expected Codex sysText compaction with tool_result-only tail to match")
	}
	if tail := extractCompactionTailText(codexOnlyTR); tail != "" {
		t.Fatalf("expected empty extractCompactionTailText for tool_result-only tail, got %q", tail)
	}
}
