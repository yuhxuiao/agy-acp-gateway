package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"agy-acp-gateway-go/internal/acp"
	"agy-acp-gateway-go/internal/protocol"
)

type slowPermFastChunkListener struct {
	permStarted chan struct{}
	permRelease chan struct{}
	permDone    chan struct{}
	mu          sync.Mutex
	chunks      []string
	usedTokens  int
	sizeTokens  int
}

func (l *slowPermFastChunkListener) SetEngineGen(uint64)           {}
func (l *slowPermFastChunkListener) OnEngineExited(string, uint64) {}
func (l *slowPermFastChunkListener) HandleSessionUpdate(map[string]any) {
	// Fast path should handle text/thought/usage updates without hitting map[string]any fallback.
}
func (l *slowPermFastChunkListener) HandleFastTextChunk(updType, text string) {
	l.mu.Lock()
	l.chunks = append(l.chunks, updType+":"+text)
	l.mu.Unlock()
}
func (l *slowPermFastChunkListener) HandleFastUsageUpdate(used, size int, hasU, hasS bool) {
	l.mu.Lock()
	if hasU {
		l.usedTokens = used
	}
	if hasS {
		l.sizeTokens = size
	}
	l.mu.Unlock()
}
func (l *slowPermFastChunkListener) HandlePermissionRequest(_ any, _ []acp.PermissionOption, _ map[string]any) {
	close(l.permStarted)
	<-l.permRelease
	close(l.permDone)
}

func TestReadStdoutLoop_AsyncPermissionDoesNotBlockSiblingSessionChunks(t *testing.T) {
	pr, pw, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	defer pr.Close()

	sleepCmd := exec.Command("sleep", "0.05")
	if err := sleepCmd.Start(); err != nil {
		t.Skipf("sleep unavailable: %v", err)
	}

	eng := newSharedAcpEngine()
	lA := &slowPermFastChunkListener{
		permStarted: make(chan struct{}),
		permRelease: make(chan struct{}),
		permDone:    make(chan struct{}),
	}
	lB := &slowPermFastChunkListener{
		permStarted: make(chan struct{}),
		permRelease: make(chan struct{}),
		permDone:    make(chan struct{}),
	}
	eng.RegisterListener("sid-A", lA)
	eng.RegisterListener("sid-B", lB)

	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		eng.readStdoutLoop(pr, sleepCmd, 1)
	}()

	// 1. Send permission request on sid-A (which blocks inside HandlePermissionRequest until permRelease),
	// immediately followed by streaming text + thought + usage_update on sid-B!
	lines := []string{
		`{"jsonrpc":"2.0","id":77,"method":"session/request_permission","params":{"sessionId":"sid-A","options":[{"optionId":"allow","kind":"allow_once"}],"toolCall":{"toolCallId":"tc1","name":"Bash"}}}`,
		`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"sid-B","update":{"sessionUpdate":"agent_thought_chunk","content":{"type":"text","text":"thinking-fast"}}}}`,
		`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"sid-B","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"hello-fast"}}}}`,
		`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"sid-B","update":{"sessionUpdate":"usage_update","usage":{"used":4096,"size":900000}}}}`,
	}
	for _, ln := range lines {
		_, _ = pw.WriteString(ln + "\n")
	}

	select {
	case <-lA.permStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for async permission handler to start")
	}

	// While sid-A's permission handler is still blocked, sid-B MUST receive all fast-path chunks!
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		lB.mu.Lock()
		nChunks := len(lB.chunks)
		used := lB.usedTokens
		lB.mu.Unlock()
		if nChunks == 2 && used == 4096 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	lB.mu.Lock()
	gotChunks := append([]string(nil), lB.chunks...)
	gotUsed, gotSize := lB.usedTokens, lB.sizeTokens
	lB.mu.Unlock()
	if len(gotChunks) != 2 || gotChunks[0] != "agent_thought_chunk:thinking-fast" || gotChunks[1] != "agent_message_chunk:hello-fast" || gotUsed != 4096 || gotSize != 900000 {
		t.Fatalf("expected sid-B chunks delivered without head-of-line blocking, got chunks=%v used=%d size=%d", gotChunks, gotUsed, gotSize)
	}

	close(lA.permRelease)
	_ = pw.Close()
	<-lA.permDone
	<-loopDone
}

type preConfirmedConfigEngine struct {
	*mockAcpEngine
	newConfigOpts []map[string]any
	setCalls      []string
}

func (e *preConfirmedConfigEngine) RPC(method string, params any, timeout time.Duration) (json.RawMessage, error) {
	p, _ := params.(map[string]any)
	switch method {
	case "session/new":
		e.mu.Lock()
		sid := e.nextSid
		e.rpcCalls = append(e.rpcCalls, method)
		e.mu.Unlock()
		return json.Marshal(map[string]any{
			"sessionId":     sid,
			"configOptions": e.newConfigOpts,
		})
	case "session/set_config_option":
		cid, _ := p["configId"].(string)
		val, _ := p["value"].(string)
		e.mu.Lock()
		e.setCalls = append(e.setCalls, cid+"="+val)
		e.mu.Unlock()
		return json.Marshal(map[string]any{
			"configOptions": []map[string]any{
				{"id": cid, "currentValue": val},
				{"id": "thought_level", "currentValue": "medium"},
			},
		})
	}
	return e.mockAcpEngine.RPC(method, params, timeout)
}

func TestPinSessionConfigFromAck_SkipsVerifiedConfigOptionsAndPinsMismatched(t *testing.T) {
	// Case 1: session/new already returns matching model + thought_level -> 0 session/set_config_option RPCs!
	eng1 := &preConfirmedConfigEngine{
		mockAcpEngine: newMockAcpEngine(),
		newConfigOpts: []map[string]any{
			{"id": "model", "currentValue": "gemini-4-carbon"},
			{"id": "thought_level", "currentValue": "medium"},
		},
	}
	s1 := NewAcpProjectSession("conv:ack-skip-both", t.TempDir(), "sys")
	s1.Engine = eng1
	s1.Store = &StateStore{}
	s1.setDesiredSelection(ResolveRequestedModel("gemini-4-carbon", nil))
	if err := s1.openFreshSession(nil, "medium"); err != nil {
		t.Fatalf("openFreshSession failed: %v", err)
	}
	if len(eng1.setCalls) != 0 {
		t.Fatalf("expected 0 redundant set_config_option RPCs when session/new already confirmed carbon/medium, got %v", eng1.setCalls)
	}

	// Case 2: session/new returns carbon/medium, session wants flash/medium -> sets model=gemini-3.8-flash (1 RPC),
	// and reuses thought_level=medium from the model-switch ack (skipping 2nd RPC)!
	eng2 := &preConfirmedConfigEngine{
		mockAcpEngine: newMockAcpEngine(),
		newConfigOpts: []map[string]any{
			{"id": "model", "currentValue": "gemini-4-carbon"},
			{"id": "thought_level", "currentValue": "medium"},
		},
	}
	s2 := NewAcpProjectSession("conv:ack-skip-tier", t.TempDir(), "sys")
	s2.Engine = eng2
	s2.Store = &StateStore{}
	s2.setDesiredSelection(ResolveRequestedModel("gemini-3.8-flash", nil))
	if err := s2.openFreshSession(nil, "medium"); err != nil {
		t.Fatalf("openFreshSession flash failed: %v", err)
	}
	if len(eng2.setCalls) != 1 || eng2.setCalls[0] != "model=gemini-3.8-flash" {
		t.Fatalf("expected exactly [model=gemini-3.8-flash], got %v", eng2.setCalls)
	}
}

type deadlineCountWriter struct {
	hdr       http.Header
	buf       bytes.Buffer
	deadlines atomic.Int32
}

func (d *deadlineCountWriter) Header() http.Header         { return d.hdr }
func (d *deadlineCountWriter) WriteHeader(int)             {}
func (d *deadlineCountWriter) Flush()                      {}
func (d *deadlineCountWriter) Write(p []byte) (int, error) { return d.buf.Write(p) }
func (d *deadlineCountWriter) SetWriteDeadline(t time.Time) error {
	if !t.IsZero() {
		d.deadlines.Add(1)
	}
	return nil
}

func TestSSEStreamSession_ThrottlesWriteDeadlineAndMatchesExactFrameBytes(t *testing.T) {
	dw := &deadlineCountWriter{hdr: make(http.Header)}
	sess := protocol.NewSSEStreamSession(dw, protocol.SSEStreamOptions{
		WriteTimeout: 30 * time.Second,
	})
	defer sess.ClearDeadline()

	for i := 0; i < 50; i++ {
		if !sess.WriteSSEEvent("content_block_delta", map[string]any{"text": "tok", "html": "<tag>&"}) {
			t.Fatal("WriteSSEEvent failed")
		}
	}
	if got := dw.deadlines.Load(); got != 1 {
		t.Fatalf("expected 50 rapid SSE frames with 30s timeout to call SetWriteDeadline once (throttled to 2s), got %d", got)
	}
	lines := strings.Split(dw.buf.String(), "\n\n")
	if len(lines) < 50 || !strings.HasPrefix(lines[0], "event: content_block_delta\ndata: {") || !strings.Contains(lines[0], `\u003ctag\u003e\u0026`) {
		t.Fatalf("unexpected SSE wire frame: %q", lines[0])
	}
}
