package main

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// ENG-01 + ARCH-OPT-01: sendRawJSON unlocks tosMu before pipe Write/Flush and control replies
// (replyPermissionOutcome / -32601) never block readStdoutLoop on stdinMu/stdinPipe.
func TestRound2ENG01SendRawJSONUnlocksTosMuAndAsyncControlReplies(t *testing.T) {
	oldFile := TosTrippedFile
	tosMu.Lock()
	oldMem, oldReason, oldPersisted := tosTrippedMem.Load(), tosReasonMem, tosPersisted
	tosTrippedMem.Store(false)
	tosReasonMem = ""
	tosPersisted = false
	tosMu.Unlock()
	defer func() {
		TosTrippedFile = oldFile
		tosMu.Lock()
		tosTrippedMem.Store(oldMem)
		tosReasonMem = oldReason
		tosPersisted = oldPersisted
		tosMu.Unlock()
	}()
	TosTrippedFile = filepath.Join(t.TempDir(), "tos-marker")

	rPipe, wPipe, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer rPipe.Close()
	defer wPipe.Close()

	// Fill OS pipe buffer using non-blocking fd check or bounded goroutine until a write blocks.
	eng := newSharedAcpEngine()
	done := make(chan struct{})
	defer close(done)
	replyCh := make(chan map[string]any, 256)
	eng.mu.Lock()
	eng.stdin = bufio.NewWriterSize(wPipe, 256)
	eng.exitDone = done
	eng.replyCh = replyCh
	eng.mu.Unlock()
	go eng.writeControlRepliesLoop(replyCh, done)

	// Hold stdinMu to simulate a stalled pipe Flush in sendRawJSON while testing enqueueControlReply & tosMu.
	eng.stdinMu.Lock()
	replyStart := time.Now()
	for i := 0; i < 32; i++ {
		eng.enqueueControlReply(map[string]any{
			"jsonrpc": "2.0",
			"id":      i + 1,
			"result":  map[string]any{"outcome": map[string]any{"outcome": "cancelled"}},
		})
	}
	if elapsed := time.Since(replyStart); elapsed > 50*time.Millisecond {
		eng.stdinMu.Unlock()
		t.Fatalf("enqueueControlReply blocked caller for %v while stdinMu was held", elapsed)
	}

	// Verify IsTosTripped (which acquires tosMu) does NOT block while stdinMu is held or sendRawJSON waits.
	tosDone := make(chan struct{})
	go func() {
		_, _ = IsTosTripped()
		close(tosDone)
	}()
	select {
	case <-tosDone:
	case <-time.After(200 * time.Millisecond):
		eng.stdinMu.Unlock()
		t.Fatal("IsTosTripped deadlocked on tosMu while stdinMu was held")
	}
	eng.stdinMu.Unlock()

	// Drain pipe in background and verify sendRawJSON writes exact JSON + newline without slice corruption.
	readDone := make(chan []string, 1)
	go func() {
		var lines []string
		sc := bufio.NewScanner(rPipe)
		for len(lines) < 33 && sc.Scan() {
			lines = append(lines, sc.Text())
		}
		readDone <- lines
	}()
	if err := eng.sendRawJSON(map[string]any{"jsonrpc": "2.0", "id": 999, "method": "session/cancel"}); err != nil {
		t.Fatalf("sendRawJSON failed: %v", err)
	}
	select {
	case lines := <-readDone:
		if len(lines) != 33 {
			t.Fatalf("expected 33 JSON-RPC lines on pipe, got %d", len(lines))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out draining async control replies + sendRawJSON")
	}
}

// ENG-02: pushDelta coalesces >1024 chunks up to 32 MiB with zero drops, and DetachSubscriber /
// AttachSubscriber remain O(1) non-blocking even when a departing subscriber's Flush() is wedged.
func TestRound2ENG02SubscriberDecoupledFromFlushAndCoalescesWithoutDrops(t *testing.T) {
	blockSub1 := make(chan struct{})
	sub1Entered := make(chan struct{}, 1)
	turn := newActiveTurnStateWithCodec(func(c DeltaChunk) {
		select {
		case sub1Entered <- struct{}{}:
		default:
		}
		<-blockSub1
	}, func() {}, nil)

	const totalChunks = 2500
	var wantBuilder strings.Builder
	for i := 0; i < totalChunks; i++ {
		frag := fmt.Sprintf("chunk_%04d|", i)
		wantBuilder.WriteString(frag)
		turn.pushDelta(DeltaChunk{Type: "text", Text: frag})
	}

	select {
	case <-sub1Entered:
	case <-time.After(time.Second):
		close(blockSub1)
		t.Fatal("subscriber 1 never received first delta")
	}

	// Subscriber 1 is now wedged inside cb(c). DetachSubscriber MUST complete promptly (<100ms).
	t0 := time.Now()
	turn.DetachSubscriber()
	if dt := time.Since(t0); dt > 150*time.Millisecond {
		close(blockSub1)
		t.Fatalf("DetachSubscriber blocked %v on wedged subscriber", dt)
	}

	// Close delta stream so restorer flushes all 2,500 chunks into replayedDeltas while sub1 is still wedged!
	turn.closeDelta()

	var gotMu sync.Mutex
	var gotBuilder strings.Builder
	tAttach := time.Now()
	turn.AttachSubscriber(func(c DeltaChunk) {
		if c.Type == "text" {
			gotMu.Lock()
			gotBuilder.WriteString(c.Text)
			gotMu.Unlock()
		}
	})
	if dt := time.Since(tAttach); dt > 100*time.Millisecond {
		close(blockSub1)
		t.Fatalf("AttachSubscriber blocked %v on wedged prior subscriber", dt)
	}
	turn.DrainAndDetachSubscriber(context.Background())
	close(blockSub1)

	gotMu.Lock()
	got := gotBuilder.String()
	gotMu.Unlock()
	want := wantBuilder.String()
	if got != want {
		t.Fatalf("delta stream dropped or reordered chunks: got len=%d want len=%d", len(got), len(want))
	}
}

// ENG-02 review item 1: 60ms slow onDelta + ctx cancel must release session queue/claim immediately
// while HandleMessagesRequest waits for in-flight onDelta before returning (zero ResponseWriter race).
func TestRound2ENG02SlowOnDeltaCancelNoResponseWriterUseAfterReturn(t *testing.T) {
	tmp := t.TempDir()
	t.Cleanup(func() { GlobalEngine.StopEngine(); initConfig() })
	t.Setenv("AGY_ACP_BIN", fakeAcpBin)
	t.Setenv("AGY_PURE_API_HOME", filepath.Join(tmp, "pure"))
	t.Setenv("AGY_CONV_DIR", filepath.Join(tmp, "convs"))
	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "gateway.db"))
	t.Setenv("AGY_GW_LOG", filepath.Join(tmp, "gateway.log"))
	t.Setenv("AGY_TOS_TRIPPED_FILE", filepath.Join(tmp, "tos-tripped"))
	t.Setenv("AGY_PREFILL_CANCEL_MIN_ELAPSED_SEC", "0.005")
	initConfig()
	PrefillCancelMinElapsed = 5 * time.Millisecond
	PrefillCancelGrace = 5 * time.Second
	GlobalEngine.StopEngine()

	gs := NewGatewayServer()
	defer gs.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Unguarded slice simulating http.ResponseWriter / pooled bufio.Writer mutated inside onDelta
	// and recycled by net/http immediately after HandleMessagesRequest returns!
	var unguardedWriterBuf []string
	var onceDelta sync.Once
	sessionReleasedEarly := make(chan bool, 1)

	onDelta := func(c DeltaChunk) {
		onceDelta.Do(func() {
			go func() {
				time.Sleep(15 * time.Millisecond)
				cancel()
				// Within 25ms (while onDelta is STILL sleeping in its 60ms write!), session queue & claim
				// MUST already be released so a retry can acquireTurnQueue immediately!
				time.Sleep(15 * time.Millisecond)
				var target *AcpProjectSession
				gs.mu.RLock()
				for _, s := range gs.sessions {
					target = s
				}
				gs.mu.RUnlock()
				released := false
				if target != nil {
					qCtx, qCancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
					if target.acquireTurnQueue(qCtx) == nil {
						target.mu.Lock()
						released = !target.turnClaimed
						target.mu.Unlock()
						target.releaseTurnQueue()
					}
					qCancel()
				}
				sessionReleasedEarly <- released
			}()
			time.Sleep(60 * time.Millisecond)
			unguardedWriterBuf = append(unguardedWriterBuf, c.Text)
		})
	}

	req := httptest.NewRequest("POST", "/v1/messages", nil).WithContext(ctx)
	req.Header.Set("x-claude-code-session-id", "eng02-slow-delta")
	body := map[string]any{"model": "gemini-3.8-flash", "system": "Primary working directory: " + tmp}
	_, _ = gs.HandleMessagesRequest(req, body, []map[string]any{{"role": "user", "content": "[FAKE_MAX_TOKENS_ALWAYS] slow delta race"}}, onDelta)

	// Immediately touch unguardedWriterBuf WITHOUT any mutex: if HandleMessagesRequest returned
	// while the 60ms onDelta was still executing, `go test -race` will catch this immediately!
	unguardedWriterBuf = append(unguardedWriterBuf, "RECYCLED_BY_SERVEHTTP")
	if len(unguardedWriterBuf) < 2 {
		t.Fatalf("expected onDelta write to finish before HandleMessagesRequest returned, got %v", unguardedWriterBuf)
	}
	if early := <-sessionReleasedEarly; !early {
		t.Fatal("expected session queueSem and turnClaimed to release in nanoseconds while onDelta was still in-flight")
	}
}

// ENG-03: editing a cancelled user turn (same message count & same first user message) must force
// rollover ("rollover_cancelled_edit") instead of sending "Continue.", and Turn-1 superseding must
// reclaim the live worker immediately.
func TestRound2ENG03CancelledTurnEditedHistoryForcesRolloverAndTurn1Supersede(t *testing.T) {
	tmp := t.TempDir()
	t.Cleanup(func() { GlobalEngine.StopEngine(); initConfig() })
	t.Setenv("AGY_ACP_BIN", fakeAcpBin)
	t.Setenv("AGY_PURE_API_HOME", filepath.Join(tmp, "pure"))
	t.Setenv("AGY_CONV_DIR", filepath.Join(tmp, "convs"))
	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "gateway.db"))
	t.Setenv("AGY_GW_LOG", filepath.Join(tmp, "gateway.log"))
	t.Setenv("AGY_TOS_TRIPPED_FILE", filepath.Join(tmp, "tos-tripped"))
	t.Setenv("AGY_PREFILL_CANCEL_MIN_ELAPSED_SEC", "60") // immediate cancel for part A
	initConfig()
	GlobalEngine.StopEngine()

	gs := NewGatewayServer()
	defer gs.Close()

	body := map[string]any{"model": "gemini-3.8-flash", "system": "Primary working directory: " + tmp}
	req1 := httptest.NewRequest("POST", "/v1/messages", nil)
	req1.Header.Set("x-claude-code-session-id", "eng03-sess")
	msgs1 := []map[string]any{{"role": "user", "content": "Turn one stable prefix"}}
	res1, err := gs.HandleMessagesRequest(req1, body, msgs1, nil)
	if err != nil {
		t.Fatalf("turn 1 failed: %v", err)
	}
	sid1 := res1.SessionID

	// Turn 2A: start slow prompt and cancel mid-flight -> MessageCount=3, lastTurnAssistant=false.
	ctxCancel, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	req2a := httptest.NewRequest("POST", "/v1/messages", nil).WithContext(ctxCancel)
	req2a.Header.Set("x-claude-code-session-id", "eng03-sess")
	msgs2Alpha := []map[string]any{
		{"role": "user", "content": "Turn one stable prefix"},
		{"role": "assistant", "content": res1.Text},
		{"role": "user", "content": "[FAKE_SLOW] Question ALPHA"},
	}
	if _, err := gs.HandleMessagesRequest(req2a, body, msgs2Alpha, nil); err == nil {
		t.Fatal("expected cancelled turn 2a to return context error")
	}

	// Turn 2B: user edits Question ALPHA -> Question BETA (same len=3, same firstUserFp!).
	// Must roll over onto a new SessionID instead of sending "Continue." to answer ALPHA!
	req2b := httptest.NewRequest("POST", "/v1/messages", nil)
	req2b.Header.Set("x-claude-code-session-id", "eng03-sess")
	msgs2Beta := []map[string]any{
		{"role": "user", "content": "Turn one stable prefix"},
		{"role": "assistant", "content": res1.Text},
		{"role": "user", "content": "Question BETA"},
	}
	res2b, err := gs.HandleMessagesRequest(req2b, body, msgs2Beta, nil)
	if err != nil {
		t.Fatalf("turn 2b failed: %v", err)
	}
	if res2b.SessionID == sid1 || res2b.Mode != "compaction_rollover" {
		t.Fatalf("edited cancelled turn did not roll over: sid=%s (old=%s) mode=%s", res2b.SessionID, sid1, res2b.Mode)
	}

	// Part B: Turn-1 soft-detach superseded by a new request must reclaim the Turn-1 worker immediately.
	PrefillCancelMinElapsed = 10 * time.Millisecond
	PrefillCancelGrace = 30 * time.Second
	ctxDet, cancelDet := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancelDet()
	reqDet := httptest.NewRequest("POST", "/v1/messages", nil).WithContext(ctxDet)
	reqDet.Header.Set("x-claude-code-session-id", "eng03-superseded-t1")
	_, errDet := gs.HandleMessagesRequest(reqDet, body, []map[string]any{{"role": "user", "content": "[FAKE_SLOW] turn 1 slow"}}, nil)
	if !isSoftDetachedErr(errDet) {
		t.Fatalf("expected Turn 1 soft-detach, got %v", errDet)
	}

	// Supersede Turn 1 with a different prompt on the same explicit session.
	reqSup := httptest.NewRequest("POST", "/v1/messages", nil)
	reqSup.Header.Set("x-claude-code-session-id", "eng03-superseded-t1")
	resSup, err := gs.HandleMessagesRequest(reqSup, body, []map[string]any{{"role": "user", "content": "turn 1 replacement"}}, nil)
	if err != nil || !strings.Contains(resSup.Text, "FAKE_OK") {
		t.Fatalf("superseding turn failed: err=%v text=%q", err, resSup.Text)
	}
	// Only 1 live worker should remain for eng03-superseded-t1 (plus 1 for eng03-sess = 2 total).
	if live := gs.countLiveWorkers(); live != 2 {
		t.Fatalf("expected exactly 2 live workers (no leaked Turn-1 superseded worker), got %d", live)
	}
}

// ENG-04: engine crash/stop demotes idle live sessions to HibernatedSessionID, increments engineExitsTotal,
// and openFreshSession NEVER sends session/close for HibernatedSessionID.
func TestRound2ENG04EngineRestartDemotionAndNeverCloseHibernatedSid(t *testing.T) {
	tmp := t.TempDir()
	t.Cleanup(func() { GlobalEngine.StopEngine(); initConfig() })
	t.Setenv("AGY_ACP_BIN", fakeAcpBin)
	t.Setenv("AGY_PURE_API_HOME", filepath.Join(tmp, "pure"))
	t.Setenv("AGY_CONV_DIR", filepath.Join(tmp, "convs"))
	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "gateway.db"))
	t.Setenv("AGY_GW_LOG", filepath.Join(tmp, "gateway.log"))
	t.Setenv("AGY_TOS_TRIPPED_FILE", filepath.Join(tmp, "tos-tripped"))
	initConfig()
	GlobalEngine.StopEngine()

	gs := NewGatewayServer()
	defer gs.Close()

	body := map[string]any{"model": "gemini-3.8-flash", "system": "Primary working directory: " + tmp}
	reqA := httptest.NewRequest("POST", "/v1/messages", nil)
	reqA.Header.Set("x-claude-code-session-id", "eng04-a")
	resA, err := gs.HandleMessagesRequest(reqA, body, []map[string]any{{"role": "user", "content": "hello A"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	sidA := resA.SessionID

	var sessA *AcpProjectSession
	gs.mu.RLock()
	for _, s := range gs.sessions {
		sessA = s
	}
	gs.mu.RUnlock()
	if sessA == nil {
		t.Fatal("missing session A")
	}

	// Simulate engine crash/restart while sessA is live & idle: StopEngine triggers readStdoutLoop exit.
	exitsBefore := engineExitsTotal.Load()
	GlobalEngine.StopEngine()
	if engineExitsTotal.Load() <= exitsBefore {
		t.Fatal("expected engineExitsTotal to increment on engine exit")
	}
	sessA.mu.Lock()
	liveAfterExit := sessA.SessionID
	hibAfterExit := sessA.HibernatedSessionID
	sessA.mu.Unlock()
	if liveAfterExit != "" || hibAfterExit != sidA {
		t.Fatalf("expected live session %s demoted to HibernatedSessionID after engine exit, got live=%q hib=%q", sidA, liveAfterExit, hibAfterExit)
	}

	// Boot a new engine generation where session sequence resets to 00000000-0000-4000-8000-000000000001 (= sidA!)
	// on Session B, then call openFreshSession on Session A (whose HibernatedSessionID == sidA).
	// Session A MUST NOT close Session B's live sidA!
	reqB := httptest.NewRequest("POST", "/v1/messages", nil)
	reqB.Header.Set("x-claude-code-session-id", "eng04-b")
	resB, err := gs.HandleMessagesRequest(reqB, body, []map[string]any{{"role": "user", "content": "hello B"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if resB.SessionID != sidA {
		t.Fatalf("expected restarted fakeacp to issue %s to Session B, got %s", sidA, resB.SessionID)
	}
	if err := sessA.openFreshSession(nil, "medium"); err != nil {
		t.Fatalf("sessA.openFreshSession failed: %v", err)
	}
	// Session B's sidA must still be alive and reusable sticky in fakeacp!
	resB2, err := gs.HandleMessagesRequest(reqB, body, []map[string]any{
		{"role": "user", "content": "hello B"},
		{"role": "assistant", "content": resB.Text},
		{"role": "user", "content": "turn 2 on B"},
	}, nil)
	if err != nil || !strings.Contains(resB2.Text, "turn=2") {
		t.Fatalf("Session B's live session was destroyed by Session A's openFreshSession: err=%v text=%q", err, resB2.Text)
	}

	// Review item 2b: if a session was busy during an engine crash and kept a non-empty SessionID
	// with a stale engineGen != GlobalEngine.CurrentGen(), ensureBooted must demote it to
	// HibernatedSessionID (when MessageCount > 0) and resume it on the current engine generation!
	staleSess := NewAcpProjectSession("sid:eng04-stale-gen:u:1", tmp, "")
	staleSess.onBeforeSpawn = gs.reserveWorkerSlot
	staleSess.SessionID = "dead-prev-gen-sid"
	staleSess.engineGen = 1 // CurrentGen() is now >= 2
	staleSess.MessageCount = 2
	if GlobalEngine.CurrentGen() <= 1 {
		t.Fatalf("expected GlobalEngine.CurrentGen() > 1 after restart, got %d", GlobalEngine.CurrentGen())
	}
	if err := staleSess.ensureBooted(nil, "medium"); err != nil {
		t.Fatalf("ensureBooted failed on stale engineGen session: %v", err)
	}
	staleSess.mu.Lock()
	reopenedSid := staleSess.SessionID
	reopenedGen := staleSess.engineGen
	staleSess.mu.Unlock()
	_, registeredOnNewEngine := GlobalEngine.sessionListeners.Load("dead-prev-gen-sid")
	if reopenedSid != "dead-prev-gen-sid" || !registeredOnNewEngine || reopenedGen != GlobalEngine.CurrentGen() {
		t.Fatalf("expected stale engineGen session to demote & session/resume on gen %d, got sid=%q registered=%v gen=%d",
			GlobalEngine.CurrentGen(), reopenedSid, registeredOnNewEngine, reopenedGen)
	}

	// And when MessageCount == 0, stale SessionID is discarded and openFreshSession spawns a fresh UUID.
	staleZero := NewAcpProjectSession("sid:eng04-stale-zero:u:1", tmp, "")
	staleZero.onBeforeSpawn = gs.reserveWorkerSlot
	staleZero.SessionID = "dead-zero-msg-sid"
	staleZero.engineGen = 1
	staleZero.MessageCount = 0
	if err := staleZero.ensureBooted(nil, "medium"); err != nil {
		t.Fatalf("ensureBooted failed on zero-msg stale engineGen session: %v", err)
	}
	staleZero.mu.Lock()
	freshZeroSid := staleZero.SessionID
	freshZeroGen := staleZero.engineGen
	staleZero.mu.Unlock()
	if freshZeroSid == "" || freshZeroSid == "dead-zero-msg-sid" || freshZeroGen != GlobalEngine.CurrentGen() {
		t.Fatalf("expected zero-msg stale engineGen session to spawn fresh SID on gen %d, got sid=%q gen=%d",
			GlobalEngine.CurrentGen(), freshZeroSid, freshZeroGen)
	}
}

// ENG-05: AUTO_CONTINUE (contDepth > 0) client disconnect cancels turnCtx immediately without
// leaking goroutines or promptSem slots.
func TestRound2ENG05AutoContinueClientDisconnectNoLeak(t *testing.T) {
	tmp := t.TempDir()
	t.Cleanup(func() { GlobalEngine.StopEngine(); initConfig() })
	t.Setenv("AGY_ACP_BIN", fakeAcpBin)
	t.Setenv("AGY_PURE_API_HOME", filepath.Join(tmp, "pure"))
	t.Setenv("AGY_CONV_DIR", filepath.Join(tmp, "convs"))
	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "gateway.db"))
	t.Setenv("AGY_GW_LOG", filepath.Join(tmp, "gateway.log"))
	t.Setenv("AGY_TOS_TRIPPED_FILE", filepath.Join(tmp, "tos-tripped"))
	t.Setenv("AGY_MAX_CONCURRENT_PROMPTS", "1")
	t.Setenv("AGY_PREFILL_CANCEL_MIN_ELAPSED_SEC", "60")
	initConfig()
	GlobalEngine.StopEngine()

	gs := NewGatewayServer()
	defer gs.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Hold promptSem as soon as SEG1_ arrives so hop 2 (contDepth=1) blocks on promptSem, then cancel ctx!
	var heldOnce sync.Once
	var capturedBgDone chan struct{}
	var capturedMu sync.Mutex
	releaseHeld := make(chan struct{})
	onDelta := func(c DeltaChunk) {
		if strings.Contains(c.Text, "SEG1_") {
			heldOnce.Do(func() {
				gs.mu.RLock()
				for _, s := range gs.sessions {
					s.mu.Lock()
					if s.activeTurn != nil {
						capturedMu.Lock()
						capturedBgDone = s.activeTurn.bgDone
						capturedMu.Unlock()
					}
					s.mu.Unlock()
				}
				gs.mu.RUnlock()
				go func() {
					promptSem <- struct{}{}
					cancel()
					time.Sleep(30 * time.Millisecond)
					<-promptSem
					close(releaseHeld)
				}()
			})
		}
	}

	req := httptest.NewRequest("POST", "/v1/messages", nil).WithContext(ctx)
	req.Header.Set("x-claude-code-session-id", "eng05-autocont")
	body := map[string]any{"model": "gemini-3.8-flash", "system": "Primary working directory: " + tmp}
	t0 := time.Now()
	_, err := gs.HandleMessagesRequest(req, body, []map[string]any{{"role": "user", "content": "[FAKE_MAX_TOKENS_ALWAYS] go"}}, onDelta)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled on AUTO_CONTINUE disconnect, got %v", err)
	}
	if elapsed := time.Since(t0); elapsed > 2*time.Second {
		t.Fatalf("AUTO_CONTINUE disconnect hung for %v", elapsed)
	}
	<-releaseHeld
	capturedMu.Lock()
	bgDone := capturedBgDone
	capturedMu.Unlock()
	if bgDone != nil {
		select {
		case <-bgDone:
		case <-time.After(time.Second):
			t.Fatal("AUTO_CONTINUE background goroutine leaked after client disconnect")
		}
	}
	if len(promptSem) != 0 {
		t.Fatalf("promptSem leaked: len=%d", len(promptSem))
	}
}

// ENG-06: Lazy RestoreSingleSession reloads 7d hibernated sessions evicted after 2h idle,
// enforces Risk Hotspot #2 duplicate-SID rejection, and UpsertSession guards monotonic counters.
func TestRound2ENG06LazySingleSessionRestoreAndMonotonicCounters(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "gateway.db")
	t.Cleanup(func() { initConfig() })
	t.Setenv("AGY_STATE_DB", dbPath)
	initConfig()

	gs := NewGatewayServer()
	defer gs.Close()

	convKey := "sid:eng06-owner:u:11223344"
	s := NewAcpProjectSession(convKey, tmp, "rules")
	origTenant := s.TenantKey
	s.HibernatedSessionID = "00000000-0000-4000-8000-000000000601"
	s.MessageCount = 4
	s.HistoryFingerprint = "fp-eng06"
	s.SessionReuses = 9
	s.CompactionRollovers = 4
	GlobalStore.UpsertSession(s)

	// Verify monotonic MAX(...) guard on UpsertSession: lower counters must not clobber persisted totals.
	sLow := NewAcpProjectSession(convKey, tmp, "rules")
	sLow.TenantKey = origTenant
	sLow.HibernatedSessionID = s.HibernatedSessionID
	sLow.MessageCount = 4
	sLow.HistoryFingerprint = "fp-eng06"
	sLow.SessionReuses = 1
	sLow.CompactionRollovers = 0
	GlobalStore.UpsertSession(sLow)

	// Evict from memory (gs.sessions is empty) and lazily reload via getOrCreateAvailableSession.
	reloaded := gs.getOrCreateAvailableSession(convKey, tmp, "rules")
	if reloaded.HibernatedSessionID != s.HibernatedSessionID || reloaded.TenantKey != origTenant {
		t.Fatalf("lazy reload lost HibernatedSessionID or TenantKey: hib=%q tenant=%q (want %q / %q)",
			reloaded.HibernatedSessionID, reloaded.TenantKey, s.HibernatedSessionID, origTenant)
	}
	if reloaded.SessionReuses != 9 || reloaded.CompactionRollovers != 4 {
		t.Fatalf("monotonic counter guard failed: reuses=%d rollovers=%d (want 9, 4)", reloaded.SessionReuses, reloaded.CompactionRollovers)
	}

	// Risk Hotspot #2: duplicate agy_session_id across two conv_keys must NEVER be auto-restored.
	dupSid := "00000000-0000-4000-8000-000000000699"
	for _, k := range []string{"sid:dup-a:u:1", "sid:dup-b:u:2"} {
		ds := NewAcpProjectSession(k, tmp, "rules")
		ds.HibernatedSessionID = dupSid
		ds.MessageCount = 2
		GlobalStore.UpsertSession(ds)
	}
	if got := GlobalStore.RestoreSingleSession("sid:dup-a:u:1"); got != nil {
		t.Fatal("Risk Hotspot #2 violated: ambiguous duplicate ACP session was lazily restored")
	}
}

// ENG-07: direct openFreshSession (!wasSpawning && prevLiveSid == "") and runWafAutoBisectProbe
// must enforce MaxLiveWorkers and return 529 instead of spawning unreserved workers.
func TestRound2ENG07OpenFreshSessionAndWafProbeEnforceMaxLiveWorkers(t *testing.T) {
	tmp := t.TempDir()
	t.Cleanup(func() { GlobalEngine.StopEngine(); initConfig() })
	t.Setenv("AGY_ACP_BIN", fakeAcpBin)
	t.Setenv("AGY_PURE_API_HOME", filepath.Join(tmp, "pure"))
	t.Setenv("AGY_CONV_DIR", filepath.Join(tmp, "convs"))
	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "gateway.db"))
	t.Setenv("AGY_GW_LOG", filepath.Join(tmp, "gateway.log"))
	t.Setenv("AGY_TOS_TRIPPED_FILE", filepath.Join(tmp, "tos-tripped"))
	t.Setenv("AGY_MAX_LIVE_WORKERS", "1")
	initConfig()
	GlobalEngine.StopEngine()
	if err := GlobalEngine.EnsureStarted(); err != nil {
		t.Fatal(err)
	}

	gs := NewGatewayServer()
	defer gs.Close()

	// Occupy the single worker slot with a busy session.
	busySess := gs.getOrCreateAvailableSession("sid:busy-holder:u:1", tmp, "")
	busySess.mu.Lock()
	busySess.SessionID = "busy-live-sid"
	busySess.IsBusy = true
	busySess.mu.Unlock()

	rejBefore := workerCapRejectionsTotal.Load()
	extraSess := gs.getOrCreateAvailableSession("sid:extra:u:2", tmp, "")
	err := extraSess.openFreshSession(nil, "low")
	var se *SoftError
	if !errors.As(err, &se) || se.StatusCode != 529 {
		t.Fatalf("expected 529 SoftError from direct openFreshSession at cap, got %v", err)
	}
	if _, _, probeErr := runWafAutoBisectProbe(context.Background(), "test rule", "Reply with OK."); !errors.As(probeErr, &se) || se.StatusCode != 529 {
		t.Fatalf("expected 529 SoftError from runWafAutoBisectProbe at cap, got %v", probeErr)
	}
	if workerCapRejectionsTotal.Load() < rejBefore+2 {
		t.Fatalf("expected workerCapRejectionsTotal +2, got %d -> %d", rejBefore, workerCapRejectionsTotal.Load())
	}
}

// ENG-08: CanSoftDetach stays open across upstreamDone before resultConsumed; unclaimed tool_use
// detached turn cleans up ParkedCalls & forces rollover; JoinDetachedTurn checks Argon on cold_start.
func TestRound2ENG08SoftDetachWindowUnclaimedToolUseAndArgonJoin(t *testing.T) {
	tmp := t.TempDir()
	t.Cleanup(func() { initConfig() })
	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "gateway.db"))
	t.Setenv("AGY_PREFILL_CANCEL_MIN_ELAPSED_SEC", "1")
	t.Setenv("AGY_PREFILL_CANCEL_GRACE_SEC", "30")
	initConfig()
	PrefillCancelMinElapsed = 5 * time.Millisecond

	s := NewAcpProjectSession("sid:eng08:u:1", tmp, "")
	turn := newActiveTurnState(nil, func() {})
	turn.createdAt = time.Now().Add(-50 * time.Millisecond)

	// Simulate normalizeTurnUsageAsync window: resolved=true & upstreamDone=true, resultConsumed=false.
	turn.mu.Lock()
	turn.resolved = true
	turn.upstreamDone = true
	turn.mu.Unlock()
	s.mu.Lock()
	s.SessionID = "00000000-0000-4000-8000-000000000801"
	s.activeTurn = turn
	s.ParkedCalls["acp_unclaimed_1"] = &ParkedMcpCall{
		Call:       ToolCallItem{ID: "acp_unclaimed_1", Name: "Bash"},
		SessionID:  s.SessionID,
		ResultChan: make(chan map[string]any, 1),
	}
	s.mu.Unlock()

	if !s.tryStartSoftDetach(turn, "fp_eng08_tool_use", s.SessionID, "cold_start", "", time.Now().Add(-50*time.Millisecond), time.Now().Add(-50*time.Millisecond), 1, "u1", "h1", false) {
		t.Fatal("tryStartSoftDetach rejected turn during upstreamDone..resultConsumed window")
	}

	// Resolve detached turn as tool_use and expire without a retry claiming it.
	turn.resolveChan <- TurnResult{
		StopReason: "tool_use",
		ToolCalls:  []ToolCallItem{{ID: "acp_unclaimed_1", Name: "Bash"}},
	}
	s.AbortDetachedTurn("prefill_grace_expired")

	s.mu.Lock()
	parkedLeft := len(s.ParkedCalls)
	forceRoll := s.ForceRolloverNext
	rollWhy := s.ForceRolloverReason
	hibLeft := s.HibernatedSessionID
	s.mu.Unlock()
	if parkedLeft != 0 || !forceRoll || rollWhy != "rollover_unclaimed_detached" || hibLeft != "" {
		t.Fatalf("unclaimed tool_use detached turn left dirty state: parked=%d forceRoll=%v why=%q hib=%q",
			parkedLeft, forceRoll, rollWhy, hibLeft)
	}

	// Verify JoinDetachedTurn runs Argon degradation detection even when mode == "cold_start".
	s2 := NewAcpProjectSession("sid:eng08-argon:u:1", tmp, "")
	s2.ModelID = "gemini-4-argon"
	t2 := newActiveTurnState(nil, func() {})
	t2.createdAt = time.Now().Add(-50 * time.Millisecond)
	s2.mu.Lock()
	s2.SessionID = "00000000-0000-4000-8000-000000000802"
	s2.activeTurn = t2
	s2.mu.Unlock()
	if !s2.tryStartSoftDetach(t2, "fp_eng08_argon_join", s2.SessionID, "cold_start", "", time.Now().Add(-50*time.Millisecond), time.Now().Add(-50*time.Millisecond), 1, "u1", "h1", false) {
		t.Fatal("expected soft-detach on s2")
	}
	t2.resolveChan <- TurnResult{
		StopReason: "end_turn",
		Text:       "degraded reply",
		Usage: &TurnUsage{
			InputTokens: 1000, OutputTokens: 50, UsageSource: "db",
			Degradation: &ArgonDegradationReport{Degraded: true, ServedModel: "gemini-4-argon-b"},
		},
	}
	_, joined, err := s2.JoinDetachedTurn(context.Background(), "fp_eng08_argon_join", []map[string]any{{"role": "user", "content": "hi"}}, nil)
	if err != nil || !joined {
		t.Fatalf("JoinDetachedTurn failed: joined=%v err=%v", joined, err)
	}
	s2.mu.Lock()
	argonRoll := s2.ForceRolloverNext && s2.ForceRolloverReason == "rollover_degraded"
	s2.mu.Unlock()
	if !argonRoll {
		t.Fatal("JoinDetachedTurn on cold_start missed Argon degradation ForceRolloverNext")
	}
}

// ENG-09: HandleMcpHTTP tools/call synchronizes keepalive goroutine via sseWg without ResponseWriter races.
func TestRound2ENG09McpHTTPKeepaliveWaitGroupRaceFree(t *testing.T) {
	s := NewAcpProjectSession("sid:eng09:u:1", "/tmp", "")
	mcpBash := mcpToolName("Bash")
	s.McpToHostMap[mcpBash] = "Bash"

	// Concurrent fast rejected & cancelled tools/call requests under -race.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			payload, _ := json.Marshal(map[string]any{
				"jsonrpc": "2.0",
				"id":      idx + 1,
				"method":  "tools/call",
				"params": map[string]any{
					"name":      "ws_unoffered_tool_000000000000",
					"arguments": map[string]any{},
					"_meta":     map[string]any{"progressToken": fmt.Sprintf("tok-%d", idx)},
				},
			})
			req := httptest.NewRequest("POST", "/mcp/"+s.TenantKey, bytes.NewReader(payload))
			req.Header.Set("Authorization", "Bearer "+s.TenantKey)
			rec := httptest.NewRecorder()
			s.HandleMcpHTTP(rec, req)
			if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "not available") {
				t.Errorf("unexpected MCP response: code=%d body=%s", rec.Code, rec.Body.String())
			}
		}(i)
	}
	wg.Wait()
}

// ENG-10: getOrCreateClaimedSession atomically claims slots so concurrent same-identity requests
// shard onto #2, and acquireTurnQueue honors context cancellation.
func TestRound2ENG10ConcurrentSameIdentityShardingAndContextQueue(t *testing.T) {
	gs := &GatewayServer{
		sessions: make(map[string]*AcpProjectSession),
		tenants:  make(map[string]*AcpProjectSession),
	}
	baseKey := "sid:eng10-owner:u:abcdef01"
	s1 := gs.getOrCreateClaimedSession(baseKey, "/tmp", "rules")
	s2 := gs.getOrCreateClaimedSession(baseKey, "/tmp", "rules")
	defer s1.releaseTurnClaim()
	defer s2.releaseTurnClaim()

	if s1 == s2 || s1.ConvKey != baseKey || s2.ConvKey != baseKey+"#2" {
		t.Fatalf("concurrent same-identity claims did not shard cleanly: s1=%s s2=%s", s1.ConvKey, s2.ConvKey)
	}

	// Context-aware per-session turn queue must abort promptly when caller disconnects while queued.
	if err := s1.acquireTurnQueue(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer s1.releaseTurnQueue()

	qCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	t0 := time.Now()
	err := s1.acquireTurnQueue(qCtx)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(t0) > 500*time.Millisecond {
		t.Fatalf("expected fast DeadlineExceeded from acquireTurnQueue, got err=%v elapsed=%v", err, time.Since(t0))
	}
}

// ENG-11: openSQLiteImage ignores stale-salt WAL frames and uncommitted WAL frames (dbSize == 0).
func TestRound2ENG11SQLiteImageWalSaltAndUncommittedFrames(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "00000000-0000-4000-8000-000000001111.db")
	db, err := sql.Open("sqlite", sqliteFileURI(dbPath, "_pragma=journal_mode(DELETE)"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE steps (idx INTEGER PRIMARY KEY, step_type INTEGER, step_status INTEGER, view_Mask BLOB, metadata BLOB, err_msg TEXT, a BLOB, b BLOB, c BLOB, step_payload BLOB)`); err != nil {
		t.Fatal(err)
	}
	// Build a minimal valid step_type=15 metadata blob (field 1 timestamp, field 4 model usage).
	stepMeta := buildMinimalStep15MetaBlob()
	if _, err := db.Exec(`INSERT INTO steps (idx, step_type, metadata) VALUES (1, 15, ?)`, stepMeta); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	baseData, err := os.ReadFile(dbPath)
	if err != nil || len(baseData) < 8192 {
		t.Fatalf("unexpected base db size: err=%v len=%d", err, len(baseData))
	}
	ps := int(binary.BigEndian.Uint16(baseData[16:18]))
	if ps == 1 {
		ps = 65536
	}

	// Construct synthetic -wal with:
	// Frame 1: matching salt, pgNo=2, dbSize=0 (UNCOMMITTED garbage page -> MUST BE IGNORED!)
	// Frame 2: mismatched salt, pgNo=2, dbSize=2 (STALE generation garbage page -> MUST STOP!)
	walHdr := make([]byte, 32)
	binary.BigEndian.PutUint32(walHdr[0:4], 0x377f0682)
	binary.BigEndian.PutUint32(walHdr[4:8], 3007000)
	binary.BigEndian.PutUint32(walHdr[8:12], uint32(ps))
	copy(walHdr[16:24], []byte{1, 2, 3, 4, 5, 6, 7, 8})

	makeFrame := func(pgNo, dbSize uint32, salt []byte, fill byte) []byte {
		f := make([]byte, 24+ps)
		binary.BigEndian.PutUint32(f[0:4], pgNo)
		binary.BigEndian.PutUint32(f[4:8], dbSize)
		copy(f[8:16], salt)
		for i := 24; i < len(f); i++ {
			f[i] = fill
		}
		return f
	}
	var wal bytes.Buffer
	wal.Write(walHdr)
	wal.Write(makeFrame(2, 0, []byte{1, 2, 3, 4, 5, 6, 7, 8}, 0xFF)) // uncommitted
	wal.Write(makeFrame(2, 2, []byte{9, 9, 9, 9, 9, 9, 9, 9}, 0xEE)) // stale salt
	if err := os.WriteFile(dbPath+"-wal", wal.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}

	steps := ReadModelStepsFromSQLite(dbPath, "WSL", map[string]bool{"00000000-0000-4000-8000-000000001111": true})
	if len(steps) != 1 || steps[0].StepIdx != 1 {
		t.Fatalf("uncommitted or stale-salt WAL frame corrupted pure-Go B-tree reader: got %+v", steps)
	}

	// Review item 6a: WAL with mismatched page size (ps*2) must be ignored completely.
	badPsHdr := append([]byte(nil), walHdr...)
	binary.BigEndian.PutUint32(badPsHdr[8:12], uint32(ps*2))
	var badPsWal bytes.Buffer
	badPsWal.Write(badPsHdr)
	badPsWal.Write(makeFrame(2, 2, []byte{1, 2, 3, 4, 5, 6, 7, 8}, 0xCC))
	if err := os.WriteFile(dbPath+"-wal", badPsWal.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	if steps2 := ReadModelStepsFromSQLite(dbPath, "WSL", map[string]bool{"00000000-0000-4000-8000-000000001111": true}); len(steps2) != 1 {
		t.Fatalf("mismatched page-size WAL corrupted pure-Go B-tree reader: got %+v", steps2)
	}

	// Review item 6b: committed WAL truncation (dbSizeAfter=2) must evict earlier committed pgNo > 2!
	var truncWal bytes.Buffer
	truncWal.Write(walHdr)
	truncWal.Write(makeFrame(5, 5, []byte{1, 2, 3, 4, 5, 6, 7, 8}, 0xAA))             // tx1 commits pg 5
	truncWal.Write(append([]byte(nil), append(make([]byte, 0, 24+ps), func() []byte { // tx2 commits pg 1 with dbSizeAfter=2
		f := make([]byte, 24+ps)
		binary.BigEndian.PutUint32(f[0:4], 1)
		binary.BigEndian.PutUint32(f[4:8], 2)
		copy(f[8:16], []byte{1, 2, 3, 4, 5, 6, 7, 8})
		copy(f[24:], baseData[:ps])
		return f
	}()...)...))
	if err := os.WriteFile(dbPath+"-wal", truncWal.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	img, errImg := openSQLiteImage(dbPath)
	if errImg != nil {
		t.Fatalf("openSQLiteImage failed: %v", errImg)
	}
	if img.HasWALPage(5) {
		t.Fatal("expected committed WAL truncation (dbSizeAfter=2) to evict stale walPages[5]")
	}
}

func appendProtoVarint(dst []byte, v uint64) []byte {
	for v >= 0x80 {
		dst = append(dst, byte(v)|0x80)
		v >>= 7
	}
	return append(dst, byte(v))
}

func appendProtoBytes(dst []byte, fieldNum uint64, payload []byte) []byte {
	dst = appendProtoVarint(dst, (fieldNum<<3)|2)
	dst = appendProtoVarint(dst, uint64(len(payload)))
	return append(dst, payload...)
}

func appendProtoVarintField(dst []byte, fieldNum, val uint64) []byte {
	dst = appendProtoVarint(dst, (fieldNum<<3)|0)
	return appendProtoVarint(dst, val)
}

func buildMinimalStep15MetaBlob() []byte {
	// ParseStepMetadataBlob expects field 9 (ModelUsageStats):
	//   .1 model enum (1319 = gemini-3.8-flash)
	//   .2 inputTokens, .3 outputTokens, .5 cachedReadTokens
	var usage []byte
	usage = appendProtoVarintField(usage, 1, 1319)
	usage = appendProtoVarintField(usage, 2, 500)
	usage = appendProtoVarintField(usage, 3, 50)
	usage = appendProtoVarintField(usage, 5, 2000)
	var ts []byte
	ts = appendProtoVarintField(ts, 1, uint64(time.Now().Unix()))
	var meta []byte
	meta = appendProtoBytes(meta, 1, ts)
	meta = appendProtoBytes(meta, 9, usage)
	return meta
}

// ENG-13: authorizeToolCallPermission accepts ws-bridge + ws_* without mcpToolName, refines
// kind=="other" -> specific kind, and merges prefixed/stripped bridge tool names while blocking spoofs.
func TestRound2ENG13BridgeToolPermissionIdentityRefinement(t *testing.T) {
	s := NewAcpProjectSession("sid:eng13:u:1", "/tmp", "")
	mcpBash := mcpToolName("Bash")
	s.mu.Lock()
	s.PromptInFlight = true
	s.activeTurn = newActiveTurnState(nil, func() {})
	s.McpToHostMap[mcpBash] = "Bash"
	s.mu.Unlock()

	// Case 1: serverName="ws-bridge" + name="ws_Bash_<hash>" without mcpToolName.
	if allow, why := s.authorizeToolCallPermission(map[string]any{
		"toolCallId":    "tc-1",
		"mcpServerName": McpBridgeName,
		"name":          mcpBash,
	}); !allow {
		t.Fatalf("expected ws-bridge + ws_* without mcpToolName to be allowed, rejected with %s", why)
	}

	// Case 2: session/update reports kind="other" + prefixed name, permission request refines kind="execute" + stripped name.
	s.recordReportedToolUpdate(map[string]any{
		"sessionUpdate": "tool_call",
		"toolCallId":    "tc-2",
		"kind":          "other",
		"name":          "mcp_" + McpBridgeName + "_" + mcpBash,
	})
	if allow, why := s.authorizeToolCallPermission(map[string]any{
		"toolCallId":    "tc-2",
		"kind":          "execute",
		"mcpServerName": McpBridgeName,
		"name":          mcpBash,
	}); !allow {
		t.Fatalf("expected kind=other refinement + bridge prefix stripping to be allowed, rejected with %s", why)
	}

	// Case 3: built-in local run_command spoof must STILL be rejected.
	if allow, _ := s.authorizeToolCallPermission(map[string]any{
		"toolCallId": "tc-spoof",
		"kind":       "execute",
		"name":       "run_command",
	}); allow {
		t.Fatal("unauthorized local run_command was allowed")
	}
}

// ENG-14 + ARCH-OPT-04: enforceWorkerCap second pass hibernates relay-capped parked slot workers
// while preserving HibernatedParkedCallIDs so delayed tool_result routes back to #2!
func TestRound2ENG14AndArchOpt04HibernatedSlotParkedCallRouting(t *testing.T) {
	tmp := t.TempDir()
	t.Cleanup(func() { GlobalEngine.StopEngine(); initConfig() })
	t.Setenv("AGY_ACP_BIN", fakeAcpBin)
	t.Setenv("AGY_PURE_API_HOME", filepath.Join(tmp, "pure"))
	t.Setenv("AGY_CONV_DIR", filepath.Join(tmp, "convs"))
	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "gateway.db"))
	t.Setenv("AGY_GW_LOG", filepath.Join(tmp, "gateway.log"))
	t.Setenv("AGY_TOS_TRIPPED_FILE", filepath.Join(tmp, "tos-tripped"))
	t.Setenv("AGY_MAX_LIVE_WORKERS", "2")
	t.Setenv("AGY_MAX_TOOL_RELAYS", "2")
	initConfig()
	GlobalEngine.StopEngine()

	gs := NewGatewayServer()
	defer gs.Close()

	// Slot 0 is actively busy; Slot #2 is idle-parked and has hit MaxToolRelays.
	s0 := NewAcpProjectSession("sid:eng14:u:001", tmp, "")
	s0.SessionID = "live-busy-0"
	s0.IsBusy = true
	gs.sessions[s0.ConvKey] = s0

	s2 := NewAcpProjectSession("sid:eng14:u:001#2", tmp, "")
	s2.SessionID = "00000000-0000-4000-8000-000000001402"
	s2.MessageCount = 2
	s2.orphanRelayHops = 2
	s2.LastUsedAt = time.Now().Add(-3 * time.Minute)
	s2.ParkedCalls["acp_slot2_call"] = &ParkedMcpCall{
		Call:       ToolCallItem{ID: "acp_slot2_call", Name: "Bash"},
		SessionID:  s2.SessionID,
		ResultChan: make(chan map[string]any, 1),
	}
	gs.sessions[s2.ConvKey] = s2

	// Requesting a worker slot for a 3rd session must trigger ARCH-OPT-04 second-pass LRU hibernation on s2!
	s3 := NewAcpProjectSession("sid:other:u:999", tmp, "")
	gs.sessions[s3.ConvKey] = s3
	if err := gs.reserveWorkerSlot(s3); err != nil {
		t.Fatalf("expected second-pass parked LRU hibernation to free a worker slot, got %v", err)
	}
	s3.mu.Lock()
	s3.spawning = false
	s3.mu.Unlock()

	s2.mu.Lock()
	hibSid := s2.HibernatedSessionID
	hasPreservedCall := s2.HibernatedParkedCallIDs["acp_slot2_call"]
	s2.mu.Unlock()
	if hibSid != "00000000-0000-4000-8000-000000001402" || !hasPreservedCall {
		t.Fatalf("slot #2 hibernation lost resume state: hib=%q hasCall=%v", hibSid, hasPreservedCall)
	}

	// Delayed tool_result from host must route back to s2 (#2), NOT cold-start!
	msgs := []map[string]any{
		{"role": "user", "content": []any{
			map[string]any{"type": "tool_result", "tool_use_id": "acp_slot2_call", "content": "delayed output"},
		}},
	}
	matched := gs.findSessionWithParkedToolResult("sid:eng14:u:changed", msgs)
	if matched != s2 {
		t.Fatalf("expected findSessionWithParkedToolResult to match hibernated slot #2, got %v", matched)
	}
	if matched.IsAvailableForNewTurn() {
		t.Fatal("expected findSessionWithParkedToolResult to atomically claim matched session")
	}
	matched.releaseTurnClaim()

	// Also verify headerless sharded slot ("fp:abc#2") routes parked tool_result back to #2!
	sFp2 := NewAcpProjectSession("fp:headerless123#2", tmp, "")
	sFp2.HibernatedSessionID = "00000000-0000-4000-8000-000000001409"
	sFp2.HibernatedParkedCallIDs = map[string]bool{"acp_fp_slot2": true}
	gs.sessions[sFp2.ConvKey] = sFp2
	fpMatched := gs.findSessionWithParkedToolResult("fp:headerless123", []map[string]any{
		{"role": "user", "content": []any{
			map[string]any{"type": "tool_result", "tool_use_id": "acp_fp_slot2", "content": "ok"},
		}},
	})
	if fpMatched != sFp2 {
		t.Fatalf("expected headerless slot #2 to match parked tool_result, got %v", fpMatched)
	}
	fpMatched.releaseTurnClaim()
}

// ENG-15: Keepalive fast-ack only matches short prefix pings without tool blocks, never hijacking user prompts.
func TestRound2ENG15KeepalivePingStrictPrefixMatch(t *testing.T) {
	if !isKeepalivePingRequest([]map[string]any{
		{"role": "user", "content": "Automated prompt-cache keep-alive ping."},
	}) {
		t.Fatal("legitimate keepalive ping was not matched")
	}
	for _, msg := range []map[string]any{
		{"role": "user", "content": "Please explain why `Automated prompt-cache keep-alive ping` appears in gateway_pool.go!"},
		{"role": "user", "content": "Automated prompt-cache keep-alive ping " + strings.Repeat("x", 220)},
		{"role": "assistant", "content": "Automated prompt-cache keep-alive ping"},
		{"role": "user", "content": []any{
			map[string]any{"type": "tool_result", "tool_use_id": "t1", "content": "Automated prompt-cache keep-alive ping"},
		}},
	} {
		if isKeepalivePingRequest([]map[string]any{msg}) {
			t.Fatalf("false-positive keepalive match on real message: %+v", msg)
		}
	}
}

// ENG-16: ensureProcessGroupStopped terminates surviving grandchildren in -pgid after leader exit.
func TestRound2ENG16StopEngineKillsProcessGroupGrandchildren(t *testing.T) {
	cmd := exec.Command("sh", "-c", "sleep 30 >/dev/null 2>&1 & exit 0")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	pgid := cmd.Process.Pid
	_ = cmd.Wait() // leader has exited, background `sleep 30` grandchild remains in pgid!
	defer func() { _ = syscall.Kill(-pgid, syscall.SIGKILL) }()

	if !processGroupAlive(pgid) {
		t.Skip("platform reaped background subshell immediately")
	}
	ensureProcessGroupStopped(pgid, 20*time.Millisecond)

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) && processGroupAlive(pgid) {
		time.Sleep(10 * time.Millisecond)
	}
	if processGroupAlive(pgid) {
		t.Fatalf("process group %d grandchild survived ensureProcessGroupStopped", pgid)
	}
}

// ENG-17: SQLite URI escaping for `#`/`%`/spaces, missing steps table -> (nil, true),
// 20ms coarse-mtime guard in SyncUsageStepsFromDir, and GlobalDBCache eviction + known_gateway_sids retention.
func TestRound2ENG17StoreCoarseMtimeSpecialPathAndCacheEvict(t *testing.T) {
	base := t.TempDir()
	weirdDir := filepath.Join(base, "dir #1 %20 space")
	if err := os.MkdirAll(weirdDir, 0755); err != nil {
		t.Fatal(err)
	}
	st := &StateStore{}
	st.Init(filepath.Join(weirdDir, "gateway#state.db"))
	if !st.IsReady() {
		t.Fatal("StateStore.Init failed on path with #, %, and spaces")
	}
	defer func() {
		st.mu.Lock()
		if st.db != nil {
			_ = st.db.Close()
		}
		st.mu.Unlock()
	}()

	// Empty SQLite DB without `steps` table must return (nil, true), not (nil, false).
	convDir := filepath.Join(weirdDir, "convs")
	if err := os.MkdirAll(convDir, 0755); err != nil {
		t.Fatal(err)
	}
	sidNoSteps := "00000000-0000-4000-8000-000000001701"
	noStepsPath := filepath.Join(convDir, sidNoSteps+".db")
	emptyDB, err := sql.Open("sqlite", sqliteFileURI(noStepsPath, "_pragma=journal_mode(DELETE)"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := emptyDB.Exec(`CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT); INSERT INTO meta VALUES ('init', '1')`); err != nil {
		t.Fatal(err)
	}
	_ = emptyDB.Close()

	if steps, ok := ReadModelStepsSQL(noStepsPath, "WSL", nil, -1, time.Now().Unix()); !ok || len(steps) != 0 {
		t.Fatalf("expected (nil, true) on DB without steps table, got steps=%v ok=%v", steps, ok)
	}

	// Freshly touched DB (<20ms old): first sync ingests immediately and leaves dbMtimeNs=0;
	// second sync >=25ms later stamps exact dbMtimeNs > 0!
	sidFresh := "00000000-0000-4000-8000-000000001702"
	freshPath := filepath.Join(convDir, sidFresh+".db")
	fDB, err := sql.Open("sqlite", sqliteFileURI(freshPath, "_pragma=journal_mode(DELETE)"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fDB.Exec(`CREATE TABLE steps (idx INTEGER PRIMARY KEY, step_type INTEGER, metadata BLOB, step_payload BLOB)`); err != nil {
		t.Fatal(err)
	}
	if _, err := fDB.Exec(`INSERT INTO steps (idx, step_type, metadata) VALUES (1, 15, ?)`, buildMinimalStep15MetaBlob()); err != nil {
		t.Fatal(err)
	}
	_ = fDB.Close()
	nowTouch := time.Now()
	_ = os.Chtimes(freshPath, nowTouch, nowTouch)

	if n := st.SyncUsageStepsFromDir(convDir, map[string]bool{sidFresh: true}); n != 1 {
		t.Fatalf("expected 1 step ingested immediately on fresh DB, got %d", n)
	}
	time.Sleep(30 * time.Millisecond)
	_ = st.SyncUsageStepsFromDir(convDir, map[string]bool{sidFresh: true})
	fInfo, _ := os.Stat(freshPath)
	if !st.IsCursorUpToDate(sidFresh, fInfo.Size(), fInfo.ModTime().UnixNano(), 0, 0) {
		t.Fatal("expected cursor mtime to be stamped after 20ms coarse-mtime window elapsed")
	}

	// Verify known_gateway_sids retains cleaned zero-turn SIDs and GlobalDBCache.Delete evicts cache entry.
	GlobalDBCache.mu.Lock()
	GlobalDBCache.entries[freshPath] = &dbCacheEntry{size: 123}
	GlobalDBCache.mu.Unlock()
	GlobalDBCache.Delete(freshPath)
	GlobalDBCache.mu.RLock()
	_, stillCached := GlobalDBCache.entries[freshPath]
	GlobalDBCache.mu.RUnlock()
	if stillCached {
		t.Fatal("GlobalDBCache.Delete did not evict entry")
	}

	zeroTurnSess := NewAcpProjectSession("sid:zero-turn:u:1", weirdDir, "")
	zeroTurnSess.HibernatedSessionID = "00000000-0000-4000-8000-000000001703"
	zeroTurnSess.MessageCount = 1
	st.UpsertSession(zeroTurnSess)
	st.ClearCleanedConversationSid(zeroTurnSess.HibernatedSessionID)
	known := make(map[string]bool)
	st.LoadKnownGatewaySids(known)
	if !known[zeroTurnSess.HibernatedSessionID] {
		t.Fatal("ClearCleanedConversationSid lost gateway SID ownership for zero-turn session")
	}
}

// PROTO-10: explicit-identity client ("sid:...") that aborts a parked tool_use and sends a new
// user turn without tool_result must immediately reclaim & roll over Slot 0 without slot sharding
// or double rollover on the subsequent turn.
func TestRound2PROTO10ExplicitIdentityAbortedParkedToolImmediateRollover(t *testing.T) {
	tmp := t.TempDir()
	t.Cleanup(func() { GlobalEngine.StopEngine(); initConfig() })
	t.Setenv("AGY_ACP_BIN", fakeAcpBin)
	t.Setenv("AGY_PURE_API_HOME", filepath.Join(tmp, "pure"))
	t.Setenv("AGY_CONV_DIR", filepath.Join(tmp, "convs"))
	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "gateway.db"))
	t.Setenv("AGY_GW_LOG", filepath.Join(tmp, "gateway.log"))
	t.Setenv("AGY_TOS_TRIPPED_FILE", filepath.Join(tmp, "tos-tripped"))
	initConfig()
	GlobalEngine.StopEngine()

	baseURL, cleanup := startTestGateway(t)
	defer cleanup()

	sidHdr := "proto10-explicit"
	// Turn 1: trigger a parked tool_use on Slot 0.
	code1, out1 := reviewPost(t, baseURL, "/v1/messages", sidHdr, reviewMessages("gemini-3.8-flash", []map[string]any{
		{"role": "user", "content": "[FAKE_TOOL_CALL] run bash"},
	}, nil))
	if code1 != 200 || !strings.Contains(out1, `"stop_reason":"tool_use"`) {
		t.Fatalf("turn 1 expected tool_use: code=%d out=%s", code1, out1)
	}

	// Review item 5a: a parallel subagent sharing sidHdr with a DIFFERENT system prompt MUST NOT
	// hijack or roll over the parent's parked tool_use session!
	subBody := reviewMessages("gemini-3.8-flash", []map[string]any{
		{"role": "user", "content": "Subagent independent task"},
	}, map[string]any{"system": "Subagent specialized prompt. Primary working directory: /tmp"})
	codeSub, outSub := reviewPost(t, baseURL, "/v1/messages", sidHdr, subBody)
	if codeSub != 200 || !strings.Contains(outSub, "FAKE_OK") {
		t.Fatalf("parallel subagent turn failed: code=%d out=%s", codeSub, outSub)
	}
	parentStillParked := false
	for _, snap := range listTestSessions() {
		snap.mu.Lock()
		nParked := len(snap.ParkedCalls)
		snap.mu.Unlock()
		if nParked > 0 {
			parentStillParked = true
		}
	}
	if !parentStillParked {
		t.Fatal("parallel subagent with different system prompt hijacked parent's parked tool_use session!")
	}

	// Turn 2: user aborts the tool call in Claude Code and sends a brand-new user turn WITHOUT tool_result!
	// Parent's Slot 0 MUST be reclaimed in-place while leaving the subagent session untouched (2 sessions total, not 3).
	msgs2 := []map[string]any{
		{"role": "user", "content": "Actually skip that command and answer directly."},
	}
	code2, out2 := reviewPost(t, baseURL, "/v1/messages", sidHdr, reviewMessages("gemini-3.8-flash", msgs2, nil))
	if code2 != 200 || !strings.Contains(out2, "FAKE_OK") {
		t.Fatalf("turn 2 after aborted tool failed: code=%d out=%s", code2, out2)
	}
	sessionsAfterT2 := listTestSessions()
	if len(sessionsAfterT2) != 2 {
		t.Fatalf("PROTO-10 regression: expected parent Slot 0 reclaimed in-place + 1 untouched subagent (2 sessions), got %d sessions", len(sessionsAfterT2))
	}

	// Turn 3: follow-up user turn MUST reuse that rolled-over Slot 0 sticky (turn=2, NOT a second cold rollover!).
	var parsed2 map[string]any
	_ = json.Unmarshal([]byte(out2), &parsed2)
	msgs3 := append(msgs2,
		map[string]any{"role": "assistant", "content": parsed2["content"]},
		map[string]any{"role": "user", "content": "Follow up sticky turn"},
	)
	code3, out3 := reviewPost(t, baseURL, "/v1/messages", sidHdr, reviewMessages("gemini-3.8-flash", msgs3, nil))
	if code3 != 200 || !strings.Contains(out3, "turn=2") {
		t.Fatalf("turn 3 expected sticky turn=2 without double rollover: code=%d out=%s", code3, out3)
	}
}

// ARCH-OPT-03 + ARCH-OPT-05 + UP-P2-1: bounded active usage step query + SQL allTime rollup,
// usage_update telemetry, 1.5.10 fallback semver, 1200s IdleWorkerTtl, and /health + /metrics counters.
func TestRound2ArchOpt03And05AndUPP21Telemetry(t *testing.T) {
	tmp := t.TempDir()
	t.Cleanup(func() { initConfig() })
	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "gateway.db"))
	t.Setenv("AGY_IDLE_WORKER_TTL_SEC", "")
	initConfig()

	if IdleWorkerTtl != 1200*time.Second {
		t.Fatalf("expected default IdleWorkerTtl=1200s (UP-P2-1), got %v", IdleWorkerTtl)
	}
	if gotVer := detectAgySemver(""); gotVer != "1.5.10" {
		t.Fatalf("expected fallback agy semver 1.5.10 (UP-P2-1), got %q", gotVer)
	}

	nowSec := time.Now().Unix()
	coldStep := ParsedModelStep{
		SessionID: "00000000-0000-4000-8000-000000003001",
		StepIdx:   1, TsSec: nowSec - 20*86400,
		ModelEnum: 1319, ModelID: "gemini-3.8-flash",
		RawIn: 10000, RawCr: 90000, OutTok: 500, IsDshBridge: true,
	}
	hotStep := ParsedModelStep{
		SessionID: "00000000-0000-4000-8000-000000003002",
		StepIdx:   1, TsSec: nowSec - 3600,
		ModelEnum: 1319, ModelID: "gemini-3.8-flash",
		RawIn: 20000, RawCr: 80000, OutTok: 1000, IsDshBridge: true,
	}
	if _, err := GlobalStore.persistUsageBatch(coldStep.SessionID, cursorMeta{lastStepIdx: 1}, []ParsedModelStep{coldStep}); err != nil {
		t.Fatal(err)
	}
	if _, err := GlobalStore.persistUsageBatch(hotStep.SessionID, cursorMeta{lastStepIdx: 1}, []ParsedModelStep{hotStep}); err != nil {
		t.Fatal(err)
	}

	activeOnly, ok := GlobalStore.QueryActiveUsageSteps(nil, 8*86400)
	if !ok || len(activeOnly) != 1 || activeOnly[0].SessionID != hotStep.SessionID {
		t.Fatalf("expected strictly 1 active-window step, got ok=%v steps=%+v", ok, activeOnly)
	}
	allSteps, ok := GlobalStore.QueryAllUsageSteps(nil)
	if !ok || len(allSteps) != 2 {
		t.Fatalf("expected 2 total steps from cached cold + active query, got ok=%v len=%d", ok, len(allSteps))
	}
	rollup, ok := GlobalStore.QueryUsageAllTimeRollup(nil)
	if !ok || rollup.StepsCount != 2 || rollup.RawIn != 30000 || rollup.RawCr != 170000 || rollup.OutTok != 1500 {
		t.Fatalf("unexpected SQL allTime rollup: %+v", rollup)
	}

	// Review item 7: multi-step Claude Opus 5.5 session mixing cold Step 0 ($4/M in, $0 saved),
	// warm Step 1 ($5/M write + $0.20/M cr, $0.18 saved), and tiny-cache Step 2 ($5/M write > $4/M base,
	// per-step saved clamped to $0.00) must match per-step PriceUsage sum in QueryAllUsageSteps!
	claudeSid := "00000000-0000-4000-8000-000000003099"
	claudeSteps := []ParsedModelStep{
		{
			SessionID: claudeSid, StepIdx: 1, TsSec: nowSec - 7200,
			ModelEnum: 1401, ModelID: "claude-opus-5-5",
			RawIn: 50000, RawCr: 0, OutTok: 1000, IsDshBridge: true,
		},
		{
			SessionID: claudeSid, StepIdx: 2, TsSec: nowSec - 3600,
			ModelEnum: 1401, ModelID: "claude-opus-5-5",
			RawIn: 10000, RawCr: 50000, OutTok: 1000, IsDshBridge: true,
		},
		{
			SessionID: claudeSid, StepIdx: 3, TsSec: nowSec - 1800,
			ModelEnum: 1401, ModelID: "claude-opus-5-5",
			RawIn: 10000, RawCr: 1000, OutTok: 500, IsDshBridge: true,
		},
	}
	if _, err := GlobalStore.persistUsageBatch(claudeSid, cursorMeta{lastStepIdx: 3}, claudeSteps); err != nil {
		t.Fatal(err)
	}
	allWithClaude, ok := GlobalStore.QueryAllUsageSteps(nil)
	if !ok || len(allWithClaude) != 5 {
		t.Fatalf("expected 5 total steps after Claude batch, got ok=%v len=%d", ok, len(allWithClaude))
	}
	var wantCost, wantSaved float64
	for _, stp := range allWithClaude {
		wantCost += stp.CostUsd
		wantSaved += stp.SavedUsd
	}
	wantCost = roundTo(wantCost, 4)
	wantSaved = roundTo(wantSaved, 4)
	rollupClaude, ok := GlobalStore.QueryUsageAllTimeRollup(nil)
	if !ok || rollupClaude.CostUsd != wantCost || rollupClaude.SavedUsd != wantSaved {
		t.Fatalf("Claude non-linear SQL rollup mismatch: got cost=%.4f saved=%.4f, want cost=%.4f saved=%.4f",
			rollupClaude.CostUsd, rollupClaude.SavedUsd, wantCost, wantSaved)
	}

	gs := NewGatewayServer()
	defer gs.Close()
	s := NewAcpProjectSession("sid:upp21:u:1", tmp, "")
	s.SessionID = "live-upp21"
	s.handleSessionUpdate(map[string]any{
		"sessionUpdate": "usage_update",
		"used":          64000,
		"size":          983040,
	})
	gs.sessions[s.ConvKey] = s

	hib := NewAcpProjectSession("sid:hib:u:1", tmp, "")
	hib.HibernatedSessionID = "hib-001"
	gs.sessions[hib.ConvKey] = hib

	liveCards, _ := gs.CollectDashboardTelemetry(map[string]bool{})
	if len(liveCards) != 1 || liveCards[0]["contextTokens"] != 64000 || liveCards[0]["reportedContextSize"] != 983040 {
		t.Fatalf("expected usage_update telemetry on live card, got %+v", liveCards)
	}

	stats := gs.GetStats()
	for _, key := range []string{"storeReady", "hibernatedSessionsCount", "engineExitsTotal", "workerCapRejectionsTotal", "softDetachJoined", "softDetachExpired", "softDetachSuperseded"} {
		if _, exists := stats[key]; !exists {
			t.Fatalf("GetStats() missing %s", key)
		}
	}
	if stats["storeReady"] != true || stats["hibernatedSessionsCount"] != 1 {
		t.Fatalf("unexpected storeReady/hibernatedSessionsCount: %+v", stats)
	}

	var dummy atomic.Int32
	_ = dummy.Load()
	prom := RenderPrometheusMetrics(gs)
	for _, metric := range []string{
		"agy_gateway_store_ready 1",
		"agy_gateway_hibernated_sessions 1",
		"agy_gateway_engine_exits_total",
		"agy_gateway_worker_cap_rejections_total",
		`agy_gateway_soft_detach_total{outcome="joined"}`,
		`agy_gateway_soft_detach_total{outcome="expired"}`,
		`agy_gateway_soft_detach_total{outcome="superseded"}`,
	} {
		if !strings.Contains(prom, metric) {
			t.Fatalf("Prometheus /metrics missing %q:\n%s", metric, prom)
		}
	}
}

// ARCH-OPT-01 + ARCH-OPT-02: streaming SHA-256 request fingerprint determinism and lazy read-only
// session SQLite handle reuse + automatic close on HibernateWorker / openFreshSession.
func TestRound2ArchOpt01And02StreamingFingerprintAndSessionRODBLifecycle(t *testing.T) {
	tmp := t.TempDir()
	convDir := filepath.Join(tmp, "convs")
	if err := os.MkdirAll(convDir, 0755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { initConfig() })
	t.Setenv("AGY_CONV_DIR", convDir)
	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "gateway.db"))
	initConfig()

	msgs := []map[string]any{
		{"role": "user", "content": "check <tag>&url=1"},
	}
	fp1 := computeTurnRequestFingerprint("gemini-4-argon", "high", "sys", []ToolDef{{Name: "Read"}}, msgs)
	fp2 := computeTurnRequestFingerprint("gemini-4-argon", "high", "sys", []ToolDef{{Name: "Read"}}, msgs)
	fp3 := computeTurnRequestFingerprint("gemini-4-argon", "high", "sys", []ToolDef{{Name: "Read"}}, []map[string]any{
		{"role": "user", "content": "check <tag>&url=2"},
	})
	if fp1 == "" || fp1 != fp2 || fp1 == fp3 {
		t.Fatalf("unexpected streaming request fingerprints: fp1=%q fp2=%q fp3=%q", fp1, fp2, fp3)
	}

	sid := "00000000-0000-4000-8000-000000002002"
	dbPath := filepath.Join(convDir, sid+".db")
	writeTestGenMetadataSQLite(t, dbPath, 0, buildTestGenMetadataBlob(1319, 400, 40, 2000, 0, 40, 2400, 1000000, 1.0, 0.5))

	s := NewAcpProjectSession("sid:archopt02:u:1", tmp, "")
	s.SessionID = sid
	db1 := s.getReadOnlySessionDB(sid)
	db2 := s.getReadOnlySessionDB(sid)
	if db1 == nil || db1 != db2 {
		t.Fatalf("expected lazy read-only *sql.DB handle reuse, got db1=%v db2=%v", db1, db2)
	}
	if gens, maxIdx, ok := s.readSessionGenMetadataAfter(sid, -1); !ok || len(gens) != 1 || maxIdx != 0 {
		t.Fatalf("expected 1 gen_metadata row from cached roDB, got ok=%v len=%d max=%d", ok, len(gens), maxIdx)
	}

	// HibernateWorker closes the cached read-only DB handle so file descriptors never leak.
	s.HibernateWorker("test_close_rodb", false)
	s.roDBMu.Lock()
	afterClose := s.roDB
	s.roDBMu.Unlock()
	if afterClose != nil {
		t.Fatal("expected s.roDB to be closed and cleared on HibernateWorker")
	}
}
