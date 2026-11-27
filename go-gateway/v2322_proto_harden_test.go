package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"agy-acp-gateway-go/internal/acp"
)

func encodeTestGrayJPEGWithAdobeAPP14(t *testing.T, grayscale bool) ([]byte, []byte) {
	t.Helper()
	var buf bytes.Buffer
	if grayscale {
		img := image.NewGray(image.Rect(0, 0, 4, 4))
		img.SetGray(0, 0, color.Gray{Y: 128})
		if err := jpeg.Encode(&buf, img, &jpeg.Options{Quality: 85}); err != nil {
			t.Fatalf("encode gray jpeg: %v", err)
		}
	} else {
		img := image.NewRGBA(image.Rect(0, 0, 4, 4))
		img.Set(0, 0, color.RGBA{R: 200, G: 100, B: 50, A: 255})
		if err := jpeg.Encode(&buf, img, &jpeg.Options{Quality: 85}); err != nil {
			t.Fatalf("encode color jpeg: %v", err)
		}
	}
	orig := buf.Bytes()
	// Adobe APP14 marker: FF EE + length 0x000E (14 = 2 len bytes + 12 payload bytes) + "Adobe\x00d\x00\x00\x00\x00\x00"
	app14 := []byte{
		0xff, 0xee, 0x00, 0x0e,
		'A', 'd', 'o', 'b', 'e', 0x00, 0x64,
		0x00, 0x00, 0x00, 0x00, 0x00,
	}
	withAPP14 := make([]byte, 0, len(orig)+len(app14))
	withAPP14 = append(withAPP14, orig[:2]...) // SOI
	withAPP14 = append(withAPP14, app14...)
	withAPP14 = append(withAPP14, orig[2:]...)
	return orig, withAPP14
}

func TestP31MultimodalFormatIntegrityAndGrayscaleAPP14Strip(t *testing.T) {
	// 1. Truncated JPEG (missing EOI FF D9) -> HTTP 400 MultimodalValidationError
	validJPEG, _ := base64.StdEncoding.DecodeString(validTestJPEGBase64())
	truncJPEG := validJPEG[:len(validJPEG)-2]
	if _, err := buildCanonicalInlineImage(truncJPEG, "", "messages[0].content[0]"); err == nil {
		t.Fatal("expected truncated JPEG without EOI to be rejected")
	} else {
		var mmErr *MultimodalValidationError
		if !errors.As(err, &mmErr) || mmErr.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected 400 MultimodalValidationError for truncated JPEG, got %T: %v", err, err)
		}
	}

	// 2. Corrupt JPEG (has trailing FF D9 but broken markers) -> HTTP 400
	corruptJPEG := []byte{0xff, 0xd8, 0xff, 0xe0, 0x00, 0x10, 'J', 'F', 0xff, 0xd9}
	if _, err := buildCanonicalInlineImage(corruptJPEG, "", "messages[0].content[0]"); err == nil {
		t.Fatal("expected corrupt JPEG segment length to be rejected")
	}

	// 3. Grayscale JPEG with Adobe APP14 (agy#1777) -> APP14 stripped cleanly without recompression!
	origGray, grayAPP14 := encodeTestGrayJPEGWithAdobeAPP14(t, true)
	origB64 := base64.StdEncoding.EncodeToString(grayAPP14)
	blk, err := decodeAndCanonicalizeImageB64(origB64, "messages[0].content[0]", false)
	if err != nil {
		t.Fatalf("grayscale JPEG with Adobe APP14 should be sanitized and accepted, got err: %v", err)
	}
	sanitizedB64, _ := blk["data"].(string)
	sanitizedBytes, decErr := base64.StdEncoding.DecodeString(sanitizedB64)
	if decErr != nil {
		t.Fatalf("decode sanitized jpeg: %v", decErr)
	}
	if !bytes.Equal(sanitizedBytes, origGray) {
		t.Fatalf("expected exact byte-level APP14 strip (zero recompression): got len=%d want len=%d", len(sanitizedBytes), len(origGray))
	}
	if bytes.Contains(sanitizedBytes, []byte("Adobe\x00")) {
		t.Fatal("sanitized grayscale JPEG still contains Adobe APP14 marker")
	}
	// Historical cached lookup using the original unstripped base64 must ALSO return stripped bytes!
	histBlk, err := decodeAndCanonicalizeImageB64(origB64, "messages[0].content[0]", true)
	if err != nil {
		t.Fatalf("historical lookup of sanitized grayscale JPEG failed: %v", err)
	}
	if histBlk["data"] != sanitizedB64 {
		t.Fatal("historical lookup returned unstripped grayscale JPEG base64 instead of sanitized bytes")
	}

	// 4. Color (3-component) JPEG with Adobe APP14 -> preserved untouched (do NOT break RGB/CMYK colorTransform)
	_, colorAPP14 := encodeTestGrayJPEGWithAdobeAPP14(t, false)
	colorBlk, err := buildCanonicalInlineImage(colorAPP14, "", "messages[0].content[0]")
	if err != nil {
		t.Fatalf("color JPEG with Adobe APP14 should be accepted, got err: %v", err)
	}
	colorOut, _ := base64.StdEncoding.DecodeString(colorBlk["data"].(string))
	if !bytes.Equal(colorOut, colorAPP14) {
		t.Fatalf("color JPEG APP14 should NOT be stripped: got len=%d want %d", len(colorOut), len(colorAPP14))
	}

	// 5. Truncated PNG (missing IEND) -> HTTP 400
	validPNG, _ := base64.StdEncoding.DecodeString(validTestPNGBase64())
	truncPNG := validPNG[:len(validPNG)-12] // strip IEND chunk
	if _, err := buildCanonicalInlineImage(truncPNG, "", "messages[0].content[0]"); err == nil {
		t.Fatal("expected PNG missing IEND to be rejected")
	}
	// Valid PNG passes
	if _, err := buildCanonicalInlineImage(validPNG, "", "messages[0].content[0]"); err != nil {
		t.Fatalf("expected valid PNG to pass, got %v", err)
	}

	// 6. Corrupt GIF -> HTTP 400; valid GIF passes
	if _, err := buildCanonicalInlineImage([]byte("GIF89a\x01\x00"), "", "messages[0].content[0]"); err == nil {
		t.Fatal("expected truncated GIF to be rejected")
	}
	validGIF, _ := base64.StdEncoding.DecodeString(validTestGIFBase64())
	if _, err := buildCanonicalInlineImage(validGIF, "", "messages[0].content[0]"); err != nil {
		t.Fatalf("expected valid GIF to pass, got %v", err)
	}

	// 7. Truncated WebP -> HTTP 400; valid WebP passes
	if _, err := buildCanonicalInlineImage([]byte("RIFF\x40\x00\x00\x00WEBPVP8 \x10\x00\x00\x00abcd"), "", "messages[0].content[0]"); err == nil {
		t.Fatal("expected truncated WebP (RIFF length > payload) to be rejected")
	}
	validWEBP, _ := base64.StdEncoding.DecodeString(validTestWEBPBase64())
	if _, err := buildCanonicalInlineImage(validWEBP, "", "messages[0].content[0]"); err != nil {
		t.Fatalf("expected valid WebP to pass, got %v", err)
	}

	// 8. Truncated HEIC/HEIF -> HTTP 400; valid HEIC passes
	truncHEIC := []byte{0x00, 0x00, 0x00, 0x40, 'f', 't', 'y', 'p', 'h', 'e', 'i', 'c', 0, 0, 0, 0}
	if _, err := buildCanonicalInlineImage(truncHEIC, "", "messages[0].content[0]"); err == nil {
		t.Fatal("expected truncated HEIC (ftyp box size > payload) to be rejected")
	}
	validHEIC := validTestHEICBytes("heic")
	if _, err := buildCanonicalInlineImage(validHEIC, "", "messages[0].content[0]"); err != nil {
		t.Fatalf("expected valid HEIC to pass, got %v", err)
	}

	// 9. Image-bearing turn failing upstream with 400 INVALID_ARGUMENT -> HibernateWorker("multimodal_invalid_argument", true)
	t.Run("image_turn_upstream_400_invalid_argument_hibernates_cleanly", func(t *testing.T) {
		var closeCount atomic.Int32
		eng := &p3StubEngine{
			onClose: func(string) { closeCount.Add(1) },
		}
		sess := NewAcpProjectSession("conv-p31-mm400", "/tmp", "sys")
		sess.Engine = eng
		sess.SessionID = "11111111-1111-4111-8111-111111111111"
		sess.MessageCount = 4

		// RPC error path
		sess.preserveOrHibernateOnTurnError(&acpRPCError{Raw: `{"code":-32603,"message":"400 INVALID_ARGUMENT: image decode failed"}`}, true)
		sess.mu.Lock()
		sidAfter, hibAfter, msgAfter := sess.SessionID, sess.HibernatedSessionID, sess.MessageCount
		sess.mu.Unlock()
		if sidAfter != "" || hibAfter != "" || msgAfter != 0 || closeCount.Load() != 1 {
			t.Fatalf("expected full rollover hibernate on image 400 INVALID_ARGUMENT, got sid=%q hib=%q msg=%d closes=%d",
				sidAfter, hibAfter, msgAfter, closeCount.Load())
		}

		// Inline FailedNotice path after partial reply
		sess.SessionID = "22222222-2222-4222-8222-222222222222"
		sess.MessageCount = 4
		res := TurnResult{
			StopReason:   "end_turn",
			Text:         "partial text",
			FailedNotice: "INVALID_ARGUMENT (code 400): unable to process input image",
			HadImages:    true,
		}
		_, err := sess.finalizeCompletedTurn(context.Background(), res, time.Now(), 0, nil, "", false, sess.SessionID, "sticky_delta", "")
		if err == nil {
			t.Fatal("expected error from FailedNotice")
		}
		sess.mu.Lock()
		sid2, hib2 := sess.SessionID, sess.HibernatedSessionID
		sess.mu.Unlock()
		if sid2 != "" || hib2 != "" || closeCount.Load() != 2 {
			t.Fatalf("expected inline image 400 FailedNotice to hibernate session, got sid=%q hib=%q closes=%d", sid2, hib2, closeCount.Load())
		}
	})
}

type p3StubEngine struct {
	mu           sync.Mutex
	onClose      func(sid string)
	onCancel     func(sid string)
	onPrompt     func(ctx context.Context, sid string, params any) (json.RawMessage, error)
	cancelCalls  atomic.Int32
	closeCalls   atomic.Int32
	promptCalls  atomic.Int32
	listeners    sync.Map
	splitHandler func(ctx context.Context, method string, params any, maxDur time.Duration, actNs *atomic.Int64, start time.Time, phaseFn func() promptIdlePhase) (json.RawMessage, error)
}

func (e *p3StubEngine) EnsureStarted() error               { return nil }
func (e *p3StubEngine) StopEngine()                        {}
func (e *p3StubEngine) IsAlive() bool                      { return true }
func (e *p3StubEngine) IsBooting() bool                    { return false }
func (e *p3StubEngine) CurrentGen() uint64                 { return 1 }
func (e *p3StubEngine) Capabilities() acp.Capabilities     { return acp.Capabilities{McpHTTP: true} }
func (e *p3StubEngine) BinaryInfo() map[string]any         { return nil }
func (e *p3StubEngine) EnqueueControlReply(map[string]any) {}
func (e *p3StubEngine) RegisterListener(sid string, l acp.SessionListener) {
	e.listeners.Store(sid, l)
}
func (e *p3StubEngine) UnregisterSession(sid string) {
	e.listeners.Delete(sid)
}
func (e *p3StubEngine) CloseSession(sid string) error {
	e.closeCalls.Add(1)
	if e.onClose != nil {
		e.onClose(sid)
	}
	return nil
}
func (e *p3StubEngine) CancelSession(sid string) {
	e.cancelCalls.Add(1)
	if v, ok := e.listeners.Load(sid); ok {
		if obs, okO := v.(acpCancelObserver); okO {
			obs.OnEngineCancelSession(sid)
		}
	}
	if e.onCancel != nil {
		e.onCancel(sid)
	}
}
func (e *p3StubEngine) RPC(method string, params any, _ time.Duration) (json.RawMessage, error) {
	if method == "session/prompt" {
		e.promptCalls.Add(1)
		if e.onPrompt != nil {
			pm, _ := params.(map[string]any)
			sid, _ := pm["sessionId"].(string)
			return e.onPrompt(context.Background(), sid, params)
		}
	}
	return json.RawMessage(`{"stopReason":"end_turn"}`), nil
}
func (e *p3StubEngine) RPCWithSplitActivity(ctx context.Context, method string, params any, maxDuration time.Duration, lastActivityElapsedNs *atomic.Int64, start time.Time, phaseFn func() promptIdlePhase) (json.RawMessage, error) {
	if e.splitHandler != nil {
		return e.splitHandler(ctx, method, params, maxDuration, lastActivityElapsedNs, start, phaseFn)
	}
	return e.RPC(method, params, maxDuration)
}

func TestP32SplitPromptTimeoutsCancelResyncAndBlindEOFAbort(t *testing.T) {
	t.Run("split_first_chunk_vs_stream_idle_and_parked_exempt", func(t *testing.T) {
		origIdle := PromptIdleTimeout
		origFirst := PromptFirstChunkTimeout
		origStream := PromptStreamIdleTimeout
		t.Cleanup(func() {
			PromptIdleTimeout = origIdle
			PromptFirstChunkTimeout = origFirst
			PromptStreamIdleTimeout = origStream
		})

		// Verify clamping to PromptIdleTimeout when user explicitly sets a smaller PromptIdleTimeout.
		PromptFirstChunkTimeout = 360 * time.Second
		PromptStreamIdleTimeout = 180 * time.Second
		fc, si := effectivePromptSplitTimeouts(90 * time.Second)
		if fc != 90*time.Second || si != 90*time.Second {
			t.Fatalf("expected split timeouts clamped to 90s, got first=%v stream=%v", fc, si)
		}
		fc2, si2 := effectivePromptSplitTimeouts(1800 * time.Second)
		if fc2 != 360*time.Second || si2 != 180*time.Second {
			t.Fatalf("expected 360s/180s split timeouts, got first=%v stream=%v", fc2, si2)
		}

		// Real SharedAcpEngine.RPCWithSplitActivity behavioral proof:
		var stdoutBuf bytes.Buffer
		_ = stdoutBuf
		pr, pw := io.Pipe()
		t.Cleanup(func() { _ = pr.Close(); _ = pw.Close() })
		go func() { _, _ = io.Copy(io.Discard, pr) }()

		eng := newSharedAcpEngine()
		eng.stdin = bufio.NewWriter(pw)

		// Case A: pre-first-chunk timeout (~140ms) vs shorter post-chunk stream idle (~60ms)
		startA := time.Now()
		var actA atomic.Int64
		_, errA := eng.RPCWithSplitActivity(context.Background(), "session/prompt", map[string]any{"sessionId": "s-first"}, 2*time.Second, &actA, startA, func() promptIdlePhase {
			return promptIdlePhase{timeout: 140 * time.Millisecond, phaseName: "first_chunk", since: startA, wakeCap: 60 * time.Millisecond}
		})
		elA := time.Since(startA)
		if errA == nil || !strings.Contains(errA.Error(), "first_chunk idle") {
			t.Fatalf("expected first_chunk idle timeout error, got %v", errA)
		}
		if elA < 120*time.Millisecond || elA > 500*time.Millisecond {
			t.Fatalf("unexpected first_chunk timeout latency: %v", elA)
		}

		// Case B: first chunk arrives at 15ms -> switches to stream_idle (60ms) and times out at ~75ms (well below 300ms first_chunk!)
		startB := time.Now()
		var actB atomic.Int64
		var gotChunk atomic.Bool
		var chunkAt atomic.Int64
		go func() {
			time.Sleep(15 * time.Millisecond)
			now := time.Now()
			chunkAt.Store(now.UnixNano())
			actB.Store(int64(now.Sub(startB)))
			gotChunk.Store(true)
		}()
		_, errB := eng.RPCWithSplitActivity(context.Background(), "session/prompt", map[string]any{"sessionId": "s-stream"}, 2*time.Second, &actB, startB, func() promptIdlePhase {
			if !gotChunk.Load() {
				return promptIdlePhase{timeout: 300 * time.Millisecond, phaseName: "first_chunk", since: startB, wakeCap: 60 * time.Millisecond}
			}
			return promptIdlePhase{timeout: 60 * time.Millisecond, phaseName: "stream_idle", since: time.Unix(0, chunkAt.Load()), wakeCap: 60 * time.Millisecond}
		})
		elB := time.Since(startB)
		if errB == nil || !strings.Contains(errB.Error(), "stream_idle idle") {
			t.Fatalf("expected stream_idle timeout error, got %v", errB)
		}
		if elB >= 240*time.Millisecond {
			t.Fatalf("stream_idle should abort well before 300ms first_chunk timeout, took %v", elB)
		}

		// Case C: parked tool wait stays alive past BOTH first_chunk (80ms) and stream_idle (50ms) until RPC resolves!
		startC := time.Now()
		var actC atomic.Int64
		go func() {
			// Wait for pending entry to be stored, then resolve after 180ms (> 80ms first_chunk & > 50ms stream_idle)
			time.Sleep(180 * time.Millisecond)
			eng.pending.Range(func(k, v any) bool {
				ch := v.(chan rpcResult)
				select {
				case ch <- rpcResult{Result: json.RawMessage(`{"stopReason":"end_turn"}`)}:
				default:
				}
				return true
			})
		}()
		resC, errC := eng.RPCWithSplitActivity(context.Background(), "session/prompt", map[string]any{"sessionId": "s-parked"}, 2*time.Second, &actC, startC, func() promptIdlePhase {
			return promptIdlePhase{timeout: 600 * time.Millisecond, phaseName: "parked_idle", wakeCap: 50 * time.Millisecond}
		})
		if errC != nil || len(resC) == 0 {
			t.Fatalf("parked tool wait must not be killed by stream_idle, got res=%s err=%v", string(resC), errC)
		}
	})

	t.Run("post_text_cancel_marks_rollover_while_pre_text_cancel_preserves_cache", func(t *testing.T) {
		eng := &p3StubEngine{}

		// 1. Pre-text CancelActiveTurn must NOT set ForceRolloverNext
		sPre := NewAcpProjectSession("conv-p32-pre", "/tmp", "sys")
		sPre.Engine = eng
		sPre.SessionID = "33333333-3333-4333-8333-333333333331"
		sPre.MessageCount = 2
		tPre := newActiveTurnState(nil, nil)
		sPre.attachTurnHardeningHooks(tPre, sPre.SessionID)
		sPre.activeTurn = tPre
		sPre.HandleSessionTextChunk("agent_thought_chunk", "thinking only, no visible text")
		sPre.CancelActiveTurn("client_disconnect")
		sPre.mu.Lock()
		preForce := sPre.ForceRolloverNext
		sPre.mu.Unlock()
		if preForce {
			t.Fatal("pre-text cancel must NOT force rollover")
		}

		// 2. Post-text CancelActiveTurn MUST set ForceRolloverNext=true with valid key rollover_cancelled_edit
		sPost := NewAcpProjectSession("conv-p32-post", "/tmp", "sys")
		sPost.Engine = eng
		sPost.SessionID = "33333333-3333-4333-8333-333333333332"
		sPost.MessageCount = 2
		tPost := newActiveTurnState(nil, nil)
		sPost.attachTurnHardeningHooks(tPost, sPost.SessionID)
		sPost.activeTurn = tPost
		sPost.HandleSessionTextChunk("agent_message_chunk", "Visible partial answer already sent to client")
		sPost.CancelActiveTurn("client_disconnect")
		sPost.mu.Lock()
		postForce, postWhy := sPost.ForceRolloverNext, sPost.ForceRolloverReason
		sPost.mu.Unlock()
		if !postForce || postWhy != "rollover_cancelled_edit" {
			t.Fatalf("post-text cancel expected ForceRolloverNext=true reason=rollover_cancelled_edit, got force=%v why=%q", postForce, postWhy)
		}

		// 3. Direct CancelSession after visible text ALSO marks rollover
		sDirect := NewAcpProjectSession("conv-p32-direct", "/tmp", "sys")
		sDirect.Engine = eng
		sDirect.SessionID = "33333333-3333-4333-8333-333333333333"
		eng.RegisterListener(sDirect.SessionID, sDirect)
		tDir := newActiveTurnState(nil, nil)
		sDirect.activeTurn = tDir
		sDirect.HandleSessionTextChunk("agent_message_chunk", "Streamed visible text")
		eng.CancelSession(sDirect.SessionID)
		sDirect.mu.Lock()
		dirForce, dirWhy := sDirect.ForceRolloverNext, sDirect.ForceRolloverReason
		sDirect.mu.Unlock()
		if !dirForce || dirWhy != "rollover_cancelled_edit" {
			t.Fatalf("direct CancelSession post-text expected ForceRolloverNext=true rollover_cancelled_edit, got force=%v why=%q", dirForce, dirWhy)
		}
	})

	t.Run("blind_unexpected_eof_and_keepalive_retry_loop_fast_aborted", func(t *testing.T) {
		origPollInt := dbRetryPollInterval
		origPollWin := dbRetryPollWindow
		origEOFAtt := EofLoopFastAbortAttempts
		dbRetryPollInterval = 15 * time.Millisecond
		dbRetryPollWindow = 2 * time.Second
		EofLoopFastAbortAttempts = 3
		t.Cleanup(func() {
			dbRetryPollInterval = origPollInt
			dbRetryPollWindow = origPollWin
			EofLoopFastAbortAttempts = origEOFAtt
		})

		convDir := t.TempDir()
		t.Setenv("AGY_CONV_DIR", convDir)
		sid := "44444444-4444-4444-8444-444444444444"
		wdb := createTestStepsWALDB(t, filepath.Join(convDir, sid+".db"))

		cancelledCh := make(chan struct{})
		var closeOnce sync.Once
		eng := &p3StubEngine{
			onCancel: func(string) {
				closeOnce.Do(func() { close(cancelledCh) })
			},
			onPrompt: func(ctx context.Context, _ string, _ any) (json.RawMessage, error) {
				// Simulate agy writing repeated silent EOF / keep-alive retry steps to <sid>.db
				go func() {
					time.Sleep(25 * time.Millisecond)
					insertTestStepRow(t, wdb, 1, 17, buildTestErrorStepPayload("", "API error (attempt 1): unexpected EOF", "", 0, false), nil)
					time.Sleep(25 * time.Millisecond)
					insertTestStepRow(t, wdb, 2, 17, buildTestErrorStepPayload("", "API error (attempt 2): error reading stream chunk: : keep-alive", "", 0, false), nil)
					time.Sleep(25 * time.Millisecond)
					insertTestStepRow(t, wdb, 3, 17, buildTestErrorStepPayload("", "API error (attempt 3): unexpected EOF", "", 0, false), nil)
				}()
				select {
				case <-cancelledCh:
					return json.RawMessage(`{"stopReason":"cancelled"}`), nil
				case <-time.After(3 * time.Second):
					return nil, errors.New("test timed out waiting for blind EOF fast-abort")
				}
			},
		}

		sess := NewAcpProjectSession("conv-p32-eof", "/tmp", "sys")
		sess.Engine = eng
		sess.SessionID = sid
		sess.MessageCount = 0

		_, err := sess.executePromptOrPark(context.Background(), []map[string]any{{"type": "text", "text": "hello"}}, nil, 0)
		if err == nil {
			t.Fatal("expected blind EOF retry loop to abort with error")
		}
		var eofErr *eofRetryLoopAbortedError
		if !errors.As(err, &eofErr) {
			t.Fatalf("expected *eofRetryLoopAbortedError, got %T: %v", err, err)
		}
		var wafErr *wafFastCancelledError
		if errors.As(err, &wafErr) {
			t.Fatal("blind EOF loop abort must NOT trigger WAF auto-bisect error type")
		}
		var se *SoftError
		if !errors.As(err, &se) || se.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("expected wrapped 503 SoftError, got %v", err)
		}
		if eng.cancelCalls.Load() < 1 {
			t.Fatalf("expected CancelSession called on blind EOF retry loop, got %d", eng.cancelCalls.Load())
		}
	})
}

func TestP33CorruptedTrajectoryStepImmediateRollover(t *testing.T) {
	cases := []string{
		`{"code":-32603,"message":"expected pending tool step at end of trajectory"}`,
		`{"code":-32603,"message":"INTERNAL: expected tool step at index 17, got planner step"}`,
	}
	for i, raw := range cases {
		err := &acpRPCError{Raw: raw}
		if !isNonTransientOrFatalUpstreamErr(err) {
			t.Fatalf("case %d: corrupted trajectory error must be fatal/non-transient", i)
		}
		if isRetryableAcpRpcError(err) || isTransientWarmRpcError(err) {
			t.Fatalf("case %d: corrupted trajectory error must NEVER be retryable or warm-preserved", i)
		}

		eng := &p3StubEngine{}
		sess := NewAcpProjectSession(fmt.Sprintf("conv-p33-%d", i), "/tmp", "sys")
		sess.Engine = eng
		sess.SessionID = fmt.Sprintf("55555555-5555-4555-8555-55555555555%d", i)
		sess.HibernatedSessionID = "old-hib-sid"
		sess.MessageCount = 6

		sess.preserveOrHibernateOnRpcError(err)
		sess.mu.Lock()
		sid, hib, msgCnt := sess.SessionID, sess.HibernatedSessionID, sess.MessageCount
		sess.mu.Unlock()
		if sid != "" || hib != "" || msgCnt != 0 || eng.closeCalls.Load() != 1 {
			t.Fatalf("case %d: expected immediate full rollover hibernate, got sid=%q hib=%q msgCnt=%d closes=%d",
				i, sid, hib, msgCnt, eng.closeCalls.Load())
		}
	}
}

func TestP34AllowedToolsFailClosed(t *testing.T) {
	// 1. Empty tools: [] with mode=auto -> fail closed to Mode="none"
	tc1 := parseClientToolChoice(map[string]any{"type": "allowed_tools", "mode": "auto", "tools": []any{}})
	if tc1.Mode != "none" || len(tc1.AllowedOrdered) != 0 {
		t.Fatalf("allowed_tools mode=auto tools=[] must fail closed to none, got %+v", tc1)
	}
	// 2. Empty tools: [] with mode=required -> fail closed to Mode="none" (never required with Allowed: nil!)
	tc2 := parseClientToolChoice(map[string]any{"type": "allowed_tools", "mode": "required", "tools": []any{}})
	if tc2.Mode != "none" || len(tc2.AllowedOrdered) != 0 {
		t.Fatalf("allowed_tools mode=required tools=[] must fail closed to none, got %+v", tc2)
	}
	// 3. Only blank entries -> fail closed to Mode="none"
	tc3 := parseClientToolChoice(map[string]any{"type": "allowed_tools", "mode": "any", "tools": []any{"  ", ""}})
	if tc3.Mode != "none" {
		t.Fatalf("allowed_tools blank tools must fail closed to none, got %+v", tc3)
	}
	// 4. Non-empty allowed_tools where NONE match declared tools -> fail closed to Mode="none"
	bodyNoMatch := map[string]any{
		"tools": []any{
			map[string]any{"name": "Read", "description": "read file", "input_schema": map[string]any{"type": "object"}},
			map[string]any{"name": "Bash", "description": "run cmd", "input_schema": map[string]any{"type": "object"}},
		},
		"tool_choice": map[string]any{
			"type":  "allowed_tools",
			"mode":  "required",
			"tools": []any{map[string]any{"type": "function", "name": "DoesNotExist"}},
		},
	}
	dirNoMatch := extractTurnPromptDirective(bodyNoMatch)
	if dirNoMatch.ToolChoice.Mode != "none" || len(dirNoMatch.ToolChoice.Allowed) != 0 {
		t.Fatalf("allowed_tools with 0 matching declared tools must fail closed to none, got %+v", dirNoMatch.ToolChoice)
	}
	// Verify runtime enforcement blocks tool calls when reconciled to Mode="none"
	sess := NewAcpProjectSession("conv-p34", "/tmp", "sys")
	sess.activeTurnToolChoice = dirNoMatch.ToolChoice
	sess.mu.Lock()
	_, _, _, rejMsg := sess.resolveEffectiveToolLocked("Read", map[string]any{})
	sess.mu.Unlock()
	if !strings.Contains(rejMsg, "tool_choice=none") {
		t.Fatalf("expected tool_choice=none rejection, got %q", rejMsg)
	}
	// 5. Partial match with mode=required -> tightens to Mode="tool" for the single matched tool
	bodyPartial := map[string]any{
		"tools": bodyNoMatch["tools"],
		"tool_choice": map[string]any{
			"type":  "allowed_tools",
			"mode":  "required",
			"tools": []any{"Read", "MissingTool"},
		},
	}
	dirPartial := extractTurnPromptDirective(bodyPartial)
	if dirPartial.ToolChoice.Mode != "tool" || strings.Join(dirPartial.ToolChoice.AllowedOrdered, ",") != "Read" {
		t.Fatalf("expected single matched allowed_tools required to tighten to Mode=tool Read, got %+v", dirPartial.ToolChoice)
	}
}

func TestP35AcpWireHardeningMalformedRPCAndBoundedPeerStrings(t *testing.T) {
	// 1. UTF-8 rune-boundary bounding <= 1024 bytes
	hugeCJK := strings.Repeat("中", 600) // 1800 bytes
	bounded := boundPeerStringUTF8(hugeCJK, 1024)
	if len(bounded) > 1024 || !utf8.ValidString(bounded) || len(bounded) != 1023 {
		t.Fatalf("expected 1023-byte valid UTF-8 CJK string, got len=%d valid=%v", len(bounded), utf8.ValidString(bounded))
	}

	// 2. readStdoutLoop fails pending RPC immediately with -32603 on malformed JSON carrying id (no method),
	//    does NOT falsely fail pending RPC when malformed line carries non-empty method,
	//    and bounds echoed -32601 unknown method errors to <= 1024 bytes valid UTF-8!
	prOut, pwOut := io.Pipe()
	eng := newSharedAcpEngine()
	replyCh := make(chan map[string]any, 4)
	eng.replyCh = replyCh

	rpcCh7 := make(chan rpcResult, 1)
	rpcCh8 := make(chan rpcResult, 1)
	eng.pending.Store(int64(7), rpcCh7)
	eng.pending.Store(int64(8), rpcCh8)

	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		eng.readStdoutLoop(prOut, nil, 1)
	}()

	// (a) Truncated/malformed JSON response with id=7 and no method -> fails rpcCh7 immediately with -32603!
	_, _ = pwOut.Write([]byte(`{"jsonrpc":"2.0","id":7,"result":{"stopReason":"broken` + "\n"))
	select {
	case res := <-rpcCh7:
		if res.Err == nil {
			t.Fatal("expected -32603 decode error for malformed response id=7")
		}
		code, msg, okRpc := parseAcpRPCError(res.Err)
		if !okRpc || code != -32603 || len(msg) > 1024 || !utf8.ValidString(msg) {
			t.Fatalf("expected -32603 bounded UTF-8 RPC error, got ok=%v code=%d len=%d msg=%q", okRpc, code, len(msg), msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pending RPC id=7 hung instead of failing immediately on malformed JSON response")
	}

	// (b) Malformed agent->client request with id=8 AND non-empty method -> must NOT fail pending RPC id=8!
	_, _ = pwOut.Write([]byte(`{"jsonrpc":"2.0","id":8,"method":"fs/read_text_file","params":{"broken` + "\n"))
	// Send valid response for id=8 right after to prove it stayed intact
	_, _ = pwOut.Write([]byte(`{"jsonrpc":"2.0","id":8,"result":{"ok":true}}` + "\n"))
	select {
	case res := <-rpcCh8:
		if res.Err != nil || !strings.Contains(string(res.Result), `"ok":true`) {
			t.Fatalf("expected valid response for id=8 after malformed peer request, got res=%s err=%v", string(res.Result), res.Err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for valid response id=8")
	}

	// (c) Oversized (>4KB multi-byte UTF-8) unknown peer method -> echoed -32601 error message <= 1024B valid UTF-8
	longMethod := "custom/" + strings.Repeat("测", 1500)
	reqLine, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 99, "method": longMethod, "params": map[string]any{}})
	_, _ = pwOut.Write(append(reqLine, '\n'))
	select {
	case rep := <-replyCh:
		errObj, _ := rep["error"].(map[string]any)
		msg, _ := errObj["message"].(string)
		if len(msg) > 1024 || !utf8.ValidString(msg) || !strings.HasPrefix(msg, "method not supported by agy-acp-gateway: ") {
			t.Fatalf("expected <=1024B valid UTF-8 -32601 echo, got len=%d valid=%v msg=%q", len(msg), utf8.ValidString(msg), msg[:min(80, len(msg))])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for bounded -32601 reply")
	}

	_ = pwOut.Close()
	<-loopDone
}

func TestP36McpToolResultPreBounding(t *testing.T) {
	origMax := McpToolResultMaxBytes
	t.Cleanup(func() { McpToolResultMaxBytes = origMax })

	// 1. Default 9600B: payload <= 9600B is untouched
	small := strings.Repeat("a", 9600)
	outSmall, trunc := preboundMcpToolText(small, 9600)
	if trunc || outSmall != small {
		t.Fatalf("payload <= maxBytes must stay untouched: trunc=%v len=%d", trunc, len(outSmall))
	}

	// 2. Disabled (0): huge payload untouched
	huge := "HEAD_MARKER:" + strings.Repeat("汉", 6000) + ":TAIL_PAGINATION_CURSOR_999"
	outDisabled, truncDis := preboundMcpToolText(huge, 0)
	if truncDis || outDisabled != huge {
		t.Fatalf("maxBytes=0 must disable pre-bounding: trunc=%v", truncDis)
	}

	// 3. Enabled (9600B): 70/30 UTF-8-safe head+tail with explicit marker, total <= 9600B, head & tail preserved!
	outBounded, truncOn := preboundMcpToolText(huge, 9600)
	if !truncOn {
		t.Fatal("expected huge payload to be truncated")
	}
	if len(outBounded) > 9600 {
		t.Fatalf("bounded output len=%d exceeds limit 9600", len(outBounded))
	}
	if !utf8.ValidString(outBounded) {
		t.Fatal("bounded output split a multi-byte UTF-8 rune")
	}
	if !strings.HasPrefix(outBounded, "HEAD_MARKER:") || !strings.HasSuffix(outBounded, ":TAIL_PAGINATION_CURSOR_999") {
		t.Fatalf("expected head and tail preserved; head=%q tail=%q", outBounded[:40], outBounded[len(outBounded)-40:])
	}
	if !strings.Contains(outBounded, "bytes to preserve structure] ...") {
		t.Fatalf("missing explicit truncation marker in output")
	}
	markerIdx := strings.Index(outBounded, "\n... [truncated ")
	if markerIdx < 6000 || markerIdx > 7200 {
		t.Fatalf("expected ~70/30 head/tail split around ~6600 bytes, got markerIdx=%d", markerIdx)
	}

	// 4. Live HandleMcpHTTP SSE response applies pre-bounding beneath agy's 10,240B cliff
	McpToolResultMaxBytes = 1024
	sess := NewAcpProjectSession("conv-p36-http", "/tmp", "sys")
	sess.SessionID = "66666666-6666-4666-8666-666666666666"
	sess.PromptInFlight = true
	cat := sanitizeToolsCatalog([]any{
		map[string]any{"name": "Read", "description": "Read file", "input_schema": map[string]any{"type": "object"}},
	})
	sess.InitialToolsCatalog = cat
	sess.ToolsCatalog = cat
	sess.McpToHostMap = map[string]string{cat[0].McpName: "Read"}
	turn := newActiveTurnState(nil, nil)
	sess.activeTurn = turn

	recCh := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		req := httptest.NewRequest(http.MethodPost, "/mcp/"+sess.TenantKey, strings.NewReader(
			`{"jsonrpc":"2.0","id":11,"method":"tools/call","params":{"name":"`+cat[0].McpName+`","arguments":{"path":"big.json"}}}`,
		))
		req.RemoteAddr = "127.0.0.1:45678"
		req.Header.Set("Authorization", "Bearer "+sess.TenantKey)
		rec := httptest.NewRecorder()
		sess.HandleMcpHTTP(rec, req)
		recCh <- rec
	}()

	select {
	case tr := <-turn.resolveChan:
		if len(tr.ToolCalls) != 1 {
			t.Fatalf("expected 1 parked tool call, got %+v", tr.ToolCalls)
		}
		bigResult := "START_JSON:" + strings.Repeat("x", 5000) + ":END_CURSOR"
		sess.mu.Lock()
		p := sess.ParkedCalls[tr.ToolCalls[0].ID]
		sess.mu.Unlock()
		p.ResultChan <- map[string]any{
			"content": []map[string]any{{"type": "text", "text": bigResult}},
			"isError": false,
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for MCP tool call to park")
	}

	rec := <-recCh
	body := rec.Body.String()
	dataIdx := strings.Index(body, "data: ")
	if dataIdx < 0 {
		t.Fatalf("expected SSE data payload, got %q", body)
	}
	jsonLine := strings.TrimSpace(strings.SplitN(body[dataIdx+len("data: "):], "\n", 2)[0])
	var rpcResp struct {
		Result struct {
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(jsonLine), &rpcResp); err != nil || len(rpcResp.Result.Content) != 1 {
		t.Fatalf("failed to parse MCP SSE JSON-RPC reply: err=%v body=%s", err, jsonLine)
	}
	gotTxt := rpcResp.Result.Content[0].Text
	if len(gotTxt) > 1024 || !strings.HasPrefix(gotTxt, "START_JSON:") || !strings.HasSuffix(gotTxt, ":END_CURSOR") || !strings.Contains(gotTxt, "truncated ") {
		t.Fatalf("expected pre-bounded <=1024B text with head/tail/marker, got len=%d txt=%q", len(gotTxt), gotTxt)
	}
}

func TestP37RunawayCRGuardAndNon429StickyEscalation(t *testing.T) {
	t.Run("runaway_cr_loop_aborts_above_4096_for_text_and_thinking", func(t *testing.T) {
		origLimit := RunawayCRMaxConsecutive
		RunawayCRMaxConsecutive = 4096
		t.Cleanup(func() { RunawayCRMaxConsecutive = origLimit })

		// Non-consecutive and exact-boundary 4096 '\r' must NOT abort
		engSafe := &p3StubEngine{}
		sSafe := NewAcpProjectSession("conv-p37-safe", "/tmp", "sys")
		sSafe.Engine = engSafe
		sSafe.SessionID = "77777777-7777-4777-8777-777777777771"
		tSafe := newActiveTurnState(nil, nil)
		sSafe.attachTurnHardeningHooks(tSafe, sSafe.SessionID)
		sSafe.activeTurn = tSafe

		if !sSafe.ObservePreChunkGuard("agent_message_chunk", strings.Repeat("\r", 4096)) {
			t.Fatal("exact 4096 consecutive \\r must not abort (threshold is > 4096)")
		}
		if !sSafe.ObservePreChunkGuard("agent_message_chunk", "ok"+strings.Repeat("\r", 4096)) {
			t.Fatal("reset by non-CR byte followed by 4096 \\r must not abort")
		}

		// Split across two text deltas totaling 4500 consecutive '\r' -> aborts immediately via dispatchAcpSessionUpdate!
		engAbort := &p3StubEngine{}
		sAbort := NewAcpProjectSession("conv-p37-abort", "/tmp", "sys")
		sAbort.Engine = engAbort
		sAbort.SessionID = "77777777-7777-4777-8777-777777777772"
		tAbort := newActiveTurnState(nil, nil)
		sAbort.attachTurnHardeningHooks(tAbort, sAbort.SessionID)
		sAbort.activeTurn = tAbort

		sharedEng := newSharedAcpEngine()
		sharedEng.RegisterListener(sAbort.SessionID, sAbort)
		mkUpdate := func(sid, updType, txt string) json.RawMessage {
			b, _ := json.Marshal(map[string]any{
				"sessionId": sid,
				"update": map[string]any{
					"sessionUpdate": updType,
					"content":       map[string]any{"type": "text", "text": txt},
				},
			})
			return b
		}
		dispatchAcpSessionUpdate(sharedEng, mkUpdate(sAbort.SessionID, "agent_message_chunk", strings.Repeat("\r", 2500)))
		dispatchAcpSessionUpdate(sharedEng, mkUpdate(sAbort.SessionID, "agent_message_chunk", strings.Repeat("\r", 2000)))

		select {
		case err := <-tAbort.errChan:
			var crErr *runawayCRAbortedError
			if !errors.As(err, &crErr) || crErr.ConsecutiveCR != 4500 {
				t.Fatalf("expected runawayCRAbortedError with count=4500, got %T: %v", err, err)
			}
			if !strings.Contains(err.Error(), "RUNAWAY_CR_LOOP_ABORT") {
				t.Fatalf("expected RUNAWAY_CR_LOOP_ABORT in error, got %q", err.Error())
			}
		case <-time.After(2 * time.Second):
			t.Fatal("expected runaway text \\r loop to abort turn immediately")
		}
		// Give async CancelSession goroutine a brief moment
		deadline := time.Now().Add(time.Second)
		for engAbort.cancelCalls.Load() == 0 && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if engAbort.cancelCalls.Load() < 1 {
			t.Fatal("expected CancelSession called on RUNAWAY_CR_LOOP_ABORT")
		}

		// Thinking delta >4096 consecutive '\r' also aborts!
		engThink := &p3StubEngine{}
		sThink := NewAcpProjectSession("conv-p37-think", "/tmp", "sys")
		sThink.Engine = engThink
		sThink.SessionID = "77777777-7777-4777-8777-777777777773"
		tThink := newActiveTurnState(nil, nil)
		sThink.attachTurnHardeningHooks(tThink, sThink.SessionID)
		sThink.activeTurn = tThink
		sharedEng.RegisterListener(sThink.SessionID, sThink)
		dispatchAcpSessionUpdate(sharedEng, mkUpdate(sThink.SessionID, "agent_thought_chunk", strings.Repeat("\r", 4097)))
		select {
		case err := <-tThink.errChan:
			var crErr *runawayCRAbortedError
			if !errors.As(err, &crErr) || crErr.StreamType != "thinking" {
				t.Fatalf("expected thinking runawayCRAbortedError, got %T: %v", err, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("expected runaway thinking \\r loop to abort turn immediately")
		}
	})

	t.Run("non_429_sticky_escalation_alert_and_counter_throttled", func(t *testing.T) {
		resetMonitorTripsForTest()
		t.Cleanup(resetMonitorTripsForTest)

		dir := t.TempDir()
		now := time.Now().Truncate(time.Second)
		fmtLine := func(ts time.Time, msg string) string {
			return fmt.Sprintf("I%02d%02d %02d:%02d:%02d.000000   900 llm_monitor.go:706] %s\n",
				int(ts.Month()), ts.Day(), ts.Hour(), ts.Minute(), ts.Second(), msg)
		}
		logPath := filepath.Join(dir, "acp-worker-"+now.Format("20060102_150405")+"-900.log")

		// Write 2 calls (with BOTH Calling LLM + FINISHED lines for each) + 0 429 trips:
		// must count as 2 (not 4!) and NOT trigger sticky escalation yet.
		twoCalls := "" +
			fmtLine(now.Add(-4*time.Minute), "LLM Monitor severe_internal_risks_agy: Calling LLM (model: gemini-4-hydrogen-sonar)") +
			fmtLine(now.Add(-4*time.Minute).Add(time.Second), `Monitor severe_internal_risks_agy FINISHED: triggered=false, reason="", duration=1s`) +
			fmtLine(now.Add(-2*time.Minute), "LLM Monitor severe_internal_risks_agy: Calling LLM (model: gemini-4-hydrogen-sonar)") +
			fmtLine(now.Add(-2*time.Minute).Add(time.Second), `Monitor severe_internal_risks_agy FINISHED: triggered=false, reason="", duration=1s`)
		if err := os.WriteFile(logPath, []byte(twoCalls), 0o600); err != nil {
			t.Fatal(err)
		}

		scanMonitorTripAccount("acc-sticky", dir, now)
		if !MonitorTripUntil("acc-sticky", now).IsZero() {
			t.Fatal("expected 0 429 trips for acc-sticky")
		}
		if got := MonitorStickyEscalationCount(); got != 0 {
			t.Fatalf("2 severe calls must not trigger sticky escalation (threshold=3), got count=%d", got)
		}

		// Append 3rd severe_internal_risks_agy call within 10m (still 0 429 trips!) -> triggers alert & increments counter!
		f, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = f.WriteString(fmtLine(now.Add(-30*time.Second), "LLM Monitor severe_internal_risks_agy: Calling LLM (model: gemini-4-hydrogen-sonar)"))
		_ = f.Close()

		scanMonitorTripAccount("acc-sticky", dir, now.Add(time.Second))
		if got := MonitorStickyEscalationCount(); got != 1 {
			t.Fatalf("3rd severe call within 10m (with 0 429 trips) must increment sticky escalation count to 1, got %d", got)
		}
		snap := MonitorStickyEscalationSnapshot(now.Add(time.Second))
		st, ok := snap["acc-sticky"]
		if !ok || st.SevereCalls10m != 3 || st.EscalationAlerts != 1 || st.Tripped429 {
			t.Fatalf("unexpected sticky escalation snapshot: %+v", snap)
		}

		// Rescan within 10m cooldown -> throttled (counter stays 1!)
		scanMonitorTripAccount("acc-sticky", dir, now.Add(2*time.Minute))
		if got := MonitorStickyEscalationCount(); got != 1 {
			t.Fatalf("expected throttled alert within 10m cooldown, got count=%d", got)
		}

		// Telemetry event path (single-account mode) for another account with 3 events within 10m -> fires!
		evs := []agyMonitorEvent{
			{TsSec: now.Add(-5 * time.Minute).Unix(), Monitor: "severe_internal_risks_agy", Model: "gemini-4-hydrogen-sonar", Escalated: true},
			{TsSec: now.Add(-3 * time.Minute).Unix(), Monitor: "severe_internal_risks_agy", Model: "gemini-4-hydrogen-sonar", Escalated: true},
			{TsSec: now.Add(-1 * time.Minute).Unix(), Monitor: "severe_internal_risks_agy", Model: "gemini-4-hydrogen-sonar", Escalated: true},
		}
		if c := evaluateStickyEscalationFromEvents("acc-telemetry", evs, now); c != 3 {
			t.Fatalf("expected 3 severe events from telemetry, got %d", c)
		}
		if got := MonitorStickyEscalationCount(); got != 2 {
			t.Fatalf("expected total sticky escalation count=2 after telemetry trigger, got %d", got)
		}
	})
}
