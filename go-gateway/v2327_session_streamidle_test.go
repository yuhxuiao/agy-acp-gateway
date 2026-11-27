package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"agy-acp-gateway-go/internal/acp"
)

type stubSplitAcpEngine struct {
	mu          sync.Mutex
	lastMethod  string
	lastParams  map[string]any
	splitHook   func(ctx context.Context, method string, params any, maxDuration time.Duration, lastAct *atomic.Int64, start time.Time, phaseFn func() promptIdlePhase) (json.RawMessage, error)
	rpcHook     func(method string, params any, timeout time.Duration) (json.RawMessage, error)
	cancelCalls int
}

func (e *stubSplitAcpEngine) EnsureStarted() error                         { return nil }
func (e *stubSplitAcpEngine) StopEngine()                                  {}
func (e *stubSplitAcpEngine) IsAlive() bool                                { return true }
func (e *stubSplitAcpEngine) IsBooting() bool                              { return false }
func (e *stubSplitAcpEngine) CurrentGen() uint64                           { return 1 }
func (e *stubSplitAcpEngine) Capabilities() acp.Capabilities               { return acp.Capabilities{McpHTTP: true} }
func (e *stubSplitAcpEngine) RegisterListener(string, acp.SessionListener) {}
func (e *stubSplitAcpEngine) UnregisterSession(string)                     {}
func (e *stubSplitAcpEngine) CloseSession(string) error                    { return nil }
func (e *stubSplitAcpEngine) EnqueueControlReply(map[string]any)           {}
func (e *stubSplitAcpEngine) BinaryInfo() map[string]any                   { return map[string]any{} }
func (e *stubSplitAcpEngine) CancelSession(string) {
	e.mu.Lock()
	e.cancelCalls++
	e.mu.Unlock()
}

func (e *stubSplitAcpEngine) RPC(method string, params any, timeout time.Duration) (json.RawMessage, error) {
	e.mu.Lock()
	e.lastMethod = method
	if m, ok := params.(map[string]any); ok {
		e.lastParams = m
	}
	hook := e.rpcHook
	e.mu.Unlock()
	if hook != nil {
		return hook(method, params, timeout)
	}
	return json.RawMessage(`{"stopReason":"end_turn"}`), nil
}

func (e *stubSplitAcpEngine) RPCWithSplitActivity(ctx context.Context, method string, params any, maxDuration time.Duration, lastActivityElapsedNs *atomic.Int64, start time.Time, phaseFn func() promptIdlePhase) (json.RawMessage, error) {
	e.mu.Lock()
	e.lastMethod = method
	if m, ok := params.(map[string]any); ok {
		e.lastParams = m
	}
	hook := e.splitHook
	e.mu.Unlock()
	if hook != nil {
		return hook(ctx, method, params, maxDuration, lastActivityElapsedNs, start, phaseFn)
	}
	return json.RawMessage(`{"stopReason":"end_turn"}`), nil
}

// STREAM-IDLE-01: filtered internal retry notice (droppedInfo==true, text=="") must still touch
// prompt activity while turn is unresolved so active upstream retries do not falsely trip stream_idle.
func TestV2327_StreamIdle01_FilteredRetryNoticeTouchesPromptActivity(t *testing.T) {
	s := NewAcpProjectSession("sid:v2327-streamidle01:u:1", t.TempDir(), "")
	turn := newActiveTurnState(nil, func() {})
	var actNs atomic.Int64
	start := time.Now().Add(-2 * time.Minute)
	initialNs := int64(1 * time.Second)
	actNs.Store(initialNs)

	s.mu.Lock()
	s.activeTurn = turn
	s.promptStart = start
	s.promptLastActivityNs = &actNs
	s.mu.Unlock()

	// Emit a retry notice line that acceptTextLocked filters down to "" (droppedInfo == true).
	s.HandleSessionTextChunk("agent_message_chunk", "\n⚠ API error (attempt 1): 503 Service Unavailable\n")

	afterUnresolved := actNs.Load()
	if afterUnresolved <= initialNs {
		t.Fatalf("expected filtered retry notice to advance promptLastActivityNs above %d, got %d", initialNs, afterUnresolved)
	}
	turn.mu.Lock()
	infoCount := turn.noticeInfo
	chunksLen := len(turn.textChunks)
	turn.mu.Unlock()
	if infoCount != 1 || chunksLen != 0 {
		t.Fatalf("expected retry notice to be filtered (noticeInfo=1, textChunks=0), got noticeInfo=%d textChunks=%d", infoCount, chunksLen)
	}

	// Once turn is resolved, filtered notices must NOT advance promptLastActivityNs.
	turn.mu.Lock()
	turn.resolved = true
	turn.mu.Unlock()
	actNs.Store(initialNs)
	s.HandleSessionTextChunk("agent_message_chunk", "\n⚠ API error (attempt 2): 503 Service Unavailable\n")
	if got := actNs.Load(); got != initialNs {
		t.Fatalf("expected resolved turn NOT to advance promptLastActivityNs, got %d (want %d)", got, initialNs)
	}
}

// STREAM-IDLE-02: relayed ws_await_result tool calls (s.RelayedCalls non-empty, s.ParkedCalls empty)
// must keep phaseFn in parked_idle instead of falling back to 3m stream_idle.
func TestV2327_StreamIdle02_RelayedCallsExemptFromStreamIdle(t *testing.T) {
	var phaseWhileRelayed, phaseAfterDrain promptIdlePhase
	eng := &stubSplitAcpEngine{}
	s := NewAcpProjectSession("sid:v2327-streamidle02:u:1", t.TempDir(), "")
	s.Engine = eng
	s.SessionID = "00000000-0000-4000-8000-000000002327"
	s.engineGen = 1

	eng.splitHook = func(ctx context.Context, method string, params any, maxDuration time.Duration, lastAct *atomic.Int64, start time.Time, phaseFn func() promptIdlePhase) (json.RawMessage, error) {
		// Simulate first chunk already streamed and tool call moved from ParkedCalls -> RelayedCalls.
		s.mu.Lock()
		turn := s.activeTurn
		if turn != nil {
			turn.mu.Lock()
			turn.firstChunk = time.Now().Add(-4 * time.Minute)
			turn.mu.Unlock()
		}
		s.ParkedCalls = map[string]*ParkedMcpCall{}
		s.RelayedCalls = map[string]*ParkedMcpCall{
			"call_relayed_1": {Call: ToolCallItem{ID: "call_relayed_1", Name: "Bash"}, SessionID: s.SessionID},
		}
		s.mu.Unlock()

		phaseWhileRelayed = phaseFn()

		// Clear RelayedCalls -> must transition back to stream_idle.
		s.mu.Lock()
		s.RelayedCalls = nil
		s.mu.Unlock()

		phaseAfterDrain = phaseFn()
		return json.RawMessage(`{"stopReason":"end_turn"}`), nil
	}

	res, err := s.executePromptOrPark(context.Background(), []map[string]any{{"type": "text", "text": "run long bash"}}, nil, 0)
	if err != nil {
		t.Fatalf("executePromptOrPark failed: %v", err)
	}
	if res.StopReason != "end_turn" {
		t.Fatalf("unexpected stop reason: %q", res.StopReason)
	}
	if phaseWhileRelayed.phaseName != "parked_idle" {
		t.Fatalf("expected parked_idle while RelayedCalls is non-empty, got %q", phaseWhileRelayed.phaseName)
	}
	if phaseAfterDrain.phaseName != "stream_idle" {
		t.Fatalf("expected stream_idle after RelayedCalls cleared and firstChunk set, got %q", phaseAfterDrain.phaseName)
	}
}

// BUG-07: concurrent callers hitting waitAndRecheckClosing on a closing hibernated-parked session
// must atomically claim under closingCand.mu so at most one caller wins (turnClaimCount == 1).
func TestV2327_Bug07_ConcurrentWaitAndRecheckClosingAtomicClaim(t *testing.T) {
	gs := NewGatewayServer()
	defer gs.Close()

	baseKey := "sid:v2327-bug07:u:1"
	callID := "acp_0123456789abcdef0123456789abcdef"
	s := NewAcpProjectSession(baseKey, t.TempDir(), "")
	doneCh := make(chan struct{})

	s.mu.Lock()
	s.HibernatedSessionID = "00000000-0000-4000-8000-000000000707"
	s.HibernatedParkedCallIDs = map[string]bool{callID: true}
	s.beginCloseLocked(doneCh)
	s.mu.Unlock()

	gs.mu.Lock()
	gs.sessions[baseKey] = s
	gs.tenants[s.TenantKey] = s
	gs.mu.Unlock()

	msgs := []map[string]any{
		{
			"role": "user",
			"content": []any{
				map[string]any{
					"type":        "tool_result",
					"tool_use_id": callID,
					"content":     "ok",
				},
			},
		},
	}

	const workers = 12
	startGate := make(chan struct{})
	var wg sync.WaitGroup
	var winners atomic.Int32
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startGate
			if got := gs.findSessionWithParkedToolResult(baseKey, msgs); got != nil {
				winners.Add(1)
			}
		}()
	}

	close(startGate)
	// Allow goroutines to enter waitIfClosing before completing the close transition.
	time.Sleep(15 * time.Millisecond)

	s.mu.Lock()
	s.finishCloseLocked("")
	s.mu.Unlock()
	close(doneCh)

	wg.Wait()

	if got := winners.Load(); got != 1 {
		t.Fatalf("expected exactly 1 atomic winner from waitAndRecheckClosing, got %d", got)
	}
	s.mu.Lock()
	claimCount := s.turnClaimCount
	claimed := s.turnClaimed
	s.mu.Unlock()
	if !claimed || claimCount != 1 {
		t.Fatalf("expected turnClaimed=true and turnClaimCount=1, got claimed=%v count=%d", claimed, claimCount)
	}
}

// BUG-09: runNewPromptResume fallback (len(fallbackPromptBlocks)==0) must encode HibernatedCallMeta
// ToolName and ArgSummary together with tool result text exactly once via ReversibleSessionCodec.
func TestV2327_Bug09_RunNewPromptResumeEncodesToolMetaOnce(t *testing.T) {
	var capturedPromptText string
	eng := &stubSplitAcpEngine{
		splitHook: func(ctx context.Context, method string, params any, maxDuration time.Duration, lastAct *atomic.Int64, start time.Time, phaseFn func() promptIdlePhase) (json.RawMessage, error) {
			if pm, ok := params.(map[string]any); ok {
				if blocks, okB := pm["prompt"].([]map[string]any); okB {
					for _, blk := range blocks {
						if txt, _ := blk["text"].(string); txt != "" {
							capturedPromptText += txt
						}
					}
				}
			}
			return json.RawMessage(`{"stopReason":"end_turn"}`), nil
		},
	}

	s := NewAcpProjectSession("sid:v2327-bug09:u:1", t.TempDir(), "")
	s.Engine = eng
	s.SessionID = "00000000-0000-4000-8000-000000000909"
	s.engineGen = 1
	codec := NewReversibleSessionCodec(s.TenantKey)
	s.Codec = codec

	callID := "call_meta_codec_1"
	secretTool := "read_anthropic_cfg"
	secretArg := "/home/user/anthropic_workspace/anthropic_secret.json"
	secretBody := "Loaded anthropic_secret_token successfully."
	codec.SeedFromConversation("", []map[string]any{
		{"role": "user", "content": secretTool + " " + secretArg + " " + secretBody},
	})

	s.mu.Lock()
	s.PromptInFlight = false
	s.HibernatedParkedCalls = map[string]HibernatedCallMeta{
		callID: {
			ToolName:   secretTool,
			ArgSummary: secretArg,
			Seq:        1,
		},
	}
	s.HibernatedParkedCallIDs = map[string]bool{callID: true}
	s.mu.Unlock()

	matched := []ToolResultInput{
		{
			ToolCallID: callID,
			Content:    secretBody,
		},
	}
	_, err := s.resumeParkedTurnInternal(context.Background(), matched, nil, nil, time.Now(), nil, nil, nil)
	if err != nil {
		t.Fatalf("resumeParkedTurnInternal failed: %v", err)
	}
	if capturedPromptText == "" {
		t.Fatal("expected outbound prompt text to be captured")
	}

	lowOut := strings.ToLower(capturedPromptText)
	if strings.Contains(lowOut, "anthropic") {
		t.Fatalf("BUG-09 regression: sensitive ToolName/ArgSummary/body leaked in plaintext: %q", capturedPromptText)
	}
	if !strings.Contains(capturedPromptText, "wsseg_") {
		t.Fatalf("expected ReversibleSessionCodec alias in encoded prompt, got %q", capturedPromptText)
	}

	// Decoding once must recover ToolName (neutralized), ArgSummary, and body cleanly with no residual wsseg_ aliases (proving single encoding, never double-encoded).
	decoded := codec.RestoreText(capturedPromptText)
	if strings.Contains(decoded, "wsseg_") {
		t.Fatalf("expected single decode to leave no residual aliases (double-encoding check): %q", decoded)
	}
	if !strings.Contains(decoded, secretArg) {
		t.Fatalf("expected single decode to restore ArgSummary %q, got %q", secretArg, decoded)
	}
	if !strings.Contains(decoded, secretBody) {
		t.Fatalf("expected single decode to restore tool result body %q, got %q", secretBody, decoded)
	}
	if !strings.Contains(decoded, NeutralizeToolIdentifier(secretTool)) {
		t.Fatalf("expected single decode to restore tool name %q, got %q", NeutralizeToolIdentifier(secretTool), decoded)
	}
}

// PERF-FP-01: streaming canonical SHA-256 fingerprint walker must be deterministic across map key
// orders, invariant across cache_control/reasoning/thinking blocks, preserve nested tool input keys,
// and handle volatile XML tags in history slice fingerprints.
func TestV2327_PerfFp01_StreamingFingerprintDeterminismAndSemantics(t *testing.T) {
	tmp := t.TempDir()
	tools := []ToolDef{{Name: "Read"}, {Name: "Bash"}}

	msgsA := []map[string]any{
		{
			"role": "user",
			"content": []any{
				map[string]any{"type": "text", "text": "hello world", "cache_control": map[string]any{"type": "ephemeral"}},
			},
		},
		{
			"role":              "assistant",
			"reasoning":         "internal scratchpad",
			"reasoning_content": "more thoughts",
			"content": []any{
				map[string]any{"type": "thinking", "thinking": "step 1"},
				map[string]any{"type": "redacted_thinking", "data": "abc"},
				map[string]any{
					"type":  "tool_use",
					"id":    "t1",
					"name":  "submit",
					"input": map[string]any{"reasoning": "user-visible tool arg", "count": int(42), "flag": true},
				},
			},
		},
	}
	msgsB := []map[string]any{
		{
			"content": []any{
				map[string]any{"text": "hello world", "type": "text"},
			},
			"role": "user",
		},
		{
			"content": []any{
				map[string]any{
					"input": map[string]any{"flag": true, "count": float64(42), "reasoning": "user-visible tool arg"},
					"name":  "submit",
					"id":    "t1",
					"type":  "tool_use",
				},
			},
			"role": "assistant",
		},
	}

	fpA := computeTurnRequestFingerprintWithDirective("gemini-4-carbon", "high", "Primary working directory: "+tmp, tools, msgsA, "dir1")
	fpB := computeTurnRequestFingerprintWithDirective("gemini-4-carbon", "high", "Primary working directory: "+tmp, tools, msgsB, "dir1")
	if fpA == "" || fpA != fpB {
		t.Fatalf("expected streaming request fingerprint equivalence across volatile fields/map order/int-vs-float: %q vs %q", fpA, fpB)
	}

	// Mutating a tool input field named "reasoning" MUST change the fingerprint (filterVolatile does not strip tool input).
	msgsDiffInput := []map[string]any{
		msgsB[0],
		{
			"role": "assistant",
			"content": []any{
				map[string]any{
					"type":  "tool_use",
					"id":    "t1",
					"name":  "submit",
					"input": map[string]any{"flag": true, "count": json.Number("42"), "reasoning": "DIFFERENT arg"},
				},
			},
		},
	}
	fpDiff := computeTurnRequestFingerprintWithDirective("gemini-4-carbon", "high", "Primary working directory: "+tmp, tools, msgsDiffInput, "dir1")
	if fpDiff == fpA {
		t.Fatal("expected tool input map key 'reasoning' to remain part of fingerprint")
	}

	// History slice fingerprint: strips volatile XML tags (with '<' guard) and trims whitespace while keeping non-XML fast path consistent.
	histWithTag := []map[string]any{
		{"role": "user", "content": "<system-reminder>ephemeral</system-reminder>\n  fix the bug  "},
	}
	histPlain := []map[string]any{
		{"role": "user", "content": "fix the bug"},
	}
	histDiff := []map[string]any{
		{"role": "user", "content": "fix another bug"},
	}
	h1 := computeMessagesSliceFingerprint(histWithTag)
	h2 := computeMessagesSliceFingerprint(histPlain)
	h3 := computeMessagesSliceFingerprint(histDiff)
	if h1 == "" || h1 != h2 || h1 == h3 {
		t.Fatalf("unexpected history slice fingerprints: withTag=%q plain=%q diff=%q", h1, h2, h3)
	}
}

// PERF-MEM-01: primary reapIdleWorkers evicts persisted non-parked hibernated sessions after 15m
// while preserving unpersisted sessions (<2h) and sessions with hibernated parked calls.
func TestV2327_PerfMem01_PrimaryPersistedHibernatedEvictTTL15m(t *testing.T) {
	gs := NewGatewayServer()
	defer gs.Close()

	now := time.Now()
	sPersistedOld := NewAcpProjectSession("sid:v2327-mem-old:u:1", t.TempDir(), "")
	sPersistedOld.persistedInDB = true
	sPersistedOld.LastUsedAt = now.Add(-16 * time.Minute)

	sPersistedFresh := NewAcpProjectSession("sid:v2327-mem-fresh:u:1", t.TempDir(), "")
	sPersistedFresh.persistedInDB = true
	sPersistedFresh.LastUsedAt = now.Add(-14 * time.Minute)

	sUnpersisted := NewAcpProjectSession("sid:v2327-mem-unpersisted:u:1", t.TempDir(), "")
	sUnpersisted.persistedInDB = false
	sUnpersisted.LastUsedAt = now.Add(-20 * time.Minute)

	sHibParked := NewAcpProjectSession("sid:v2327-mem-hibparked:u:1", t.TempDir(), "")
	sHibParked.persistedInDB = true
	sHibParked.HibernatedParkedCallIDs = map[string]bool{"call_1": true}
	sHibParked.LastUsedAt = now.Add(-20 * time.Minute)

	gs.mu.Lock()
	for _, sess := range []*AcpProjectSession{sPersistedOld, sPersistedFresh, sUnpersisted, sHibParked} {
		gs.sessions[sess.ConvKey] = sess
		gs.tenants[sess.TenantKey] = sess
	}
	gs.mu.Unlock()

	gs.reapIdleWorkers()

	gs.mu.RLock()
	_, hasOld := gs.sessions[sPersistedOld.ConvKey]
	_, hasFresh := gs.sessions[sPersistedFresh.ConvKey]
	_, hasUnpersisted := gs.sessions[sUnpersisted.ConvKey]
	_, hasHibParked := gs.sessions[sHibParked.ConvKey]
	gs.mu.RUnlock()

	if hasOld {
		t.Fatal("expected persisted non-parked hibernated session idle 16m to be evicted on primary")
	}
	if !hasFresh {
		t.Fatal("expected persisted non-parked hibernated session idle 14m to remain on primary")
	}
	if !hasUnpersisted {
		t.Fatal("expected unpersisted hibernated session idle 20m to remain (<2h)")
	}
	if !hasHibParked {
		t.Fatal("expected hibernated-parked session idle 20m to remain")
	}
}

// PERF-ACP-01: pipeBufSize (256KB primary vs 64KB auxiliary), readJSONRPCLineReuse 1MB cap,
// and flat fast-path decode in readStdoutLoop.
func TestV2327_PerfAcp01_BufSizeLineReuseAndFlatFastPath(t *testing.T) {
	prim := newSharedAcpEngine()
	aux := newAuxiliaryAcpEngine(ImageGenAgentName)
	if got := prim.pipeBufSize(); got != 256*1024 {
		t.Fatalf("expected primary engine pipeBufSize=256KB, got %d", got)
	}
	if got := aux.pipeBufSize(); got != 64*1024 {
		t.Fatalf("expected auxiliary engine pipeBufSize=64KB, got %d", got)
	}

	// Verify readJSONRPCLineReuse reuses buffer when <=1MB and releases it when >1MB.
	medLine := strings.Repeat("a", 80*1024) + "\n"
	bigLine := strings.Repeat("b", 1200*1024) + "\n"
	br := bufio.NewReaderSize(strings.NewReader(medLine+medLine+bigLine), 4096)
	line1, buf1, err := readJSONRPCLineReuse(br, 4*1024*1024, nil)
	if err != nil || len(line1) != 80*1024 || buf1 == nil {
		t.Fatalf("expected 80KB line to populate reusable buffer, err=%v len=%d bufNil=%v", err, len(line1), buf1 == nil)
	}
	line2, buf2, err := readJSONRPCLineReuse(br, 4*1024*1024, buf1)
	if err != nil || len(line2) != 80*1024 || buf2 == nil || &buf2[0] != &buf1[0] {
		t.Fatalf("expected second 80KB line to reuse backing buffer, err=%v len=%d", err, len(line2))
	}
	line3, buf3, err := readJSONRPCLineReuse(br, 4*1024*1024, buf2)
	if err != nil || len(line3) != 1200*1024 || buf3 != nil {
		t.Fatalf("expected >1MB line to release reusable buffer (buf3==nil), err=%v len=%d bufNil=%v", err, len(line3), buf3 == nil)
	}

	// Verify flat fast-path in readStdoutLoop dispatches agent_thought_chunk and agent_message_chunk.
	eng := newSharedAcpEngine()
	s := NewAcpProjectSession("sid:v2327-acp01:u:1", t.TempDir(), "")
	turn := newActiveTurnState(nil, func() {})
	s.mu.Lock()
	s.SessionID = "sid-fast-loop"
	s.activeTurn = turn
	s.mu.Unlock()
	eng.RegisterSession("sid-fast-loop", s)

	payload := strings.Join([]string{
		`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"sid-fast-loop","update":{"sessionUpdate":"agent_thought_chunk","content":{"type":"text","text":"thinking..."}}}}`,
		`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"sid-fast-loop","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"hello fast path"}}}}`,
	}, "\n") + "\n"
	eng.readStdoutLoop(bytes.NewReader([]byte(payload)), nil, eng.CurrentGen())

	turn.mu.Lock()
	gotText := strings.Join(turn.textChunks, "")
	gotThink := strings.Join(turn.thoughtChks, "")
	turn.mu.Unlock()
	if gotThink != "thinking..." || gotText != "hello fast path" {
		t.Fatalf("flat fast-path mismatch: think=%q text=%q", gotThink, gotText)
	}
}

// UPSTREAM-MIT-01: Unix quota/models subprocess isolation uses Setsid: true to detach /dev/tty.
func TestV2327_UpstreamMit01_QuotaSubprocessUsesSetsid(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix-only test")
	}
	cmd := exec.Command("true")
	configureQuotaSubprocess(cmd)
	if cmd.SysProcAttr == nil || !cmd.SysProcAttr.Setsid {
		t.Fatalf("expected configureQuotaSubprocess to set SysProcAttr.Setsid=true, got %+v", cmd.SysProcAttr)
	}
	if cmd.SysProcAttr.Setpgid {
		t.Fatalf("expected Setpgid=false when Setsid=true, got %+v", cmd.SysProcAttr)
	}
}
