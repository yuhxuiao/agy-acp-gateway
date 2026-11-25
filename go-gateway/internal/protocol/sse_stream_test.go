package protocol

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type failingFlushWriter struct {
	header      http.Header
	statusCode  int
	failAfter   int
	writes      int
	flushes     int
	writtenBody strings.Builder
}

func newFailingFlushWriter(failAfter int) *failingFlushWriter {
	return &failingFlushWriter{
		header:    make(http.Header),
		failAfter: failAfter,
	}
}

func (f *failingFlushWriter) Header() http.Header { return f.header }
func (f *failingFlushWriter) WriteHeader(statusCode int) {
	if f.statusCode == 0 {
		f.statusCode = statusCode
	}
}
func (f *failingFlushWriter) Write(p []byte) (int, error) {
	f.writes++
	if f.failAfter > 0 && f.writes >= f.failAfter {
		return 0, errors.New("broken pipe")
	}
	f.writtenBody.Write(p)
	return len(p), nil
}
func (f *failingFlushWriter) Flush() { f.flushes++ }

func TestSSEStreamSessionHeartbeatAndDelayedError(t *testing.T) {
	// 1. Pre-header error with DelayHeaders leaves Content-Type free for application/json
	rec := httptest.NewRecorder()
	s := NewSSEStreamSession(rec, SSEStreamOptions{
		DelayHeaders:      true,
		HeartbeatInterval: 20 * time.Millisecond,
		WriteTimeout:      50 * time.Millisecond,
	})
	defer s.ClearDeadline()
	s.SetPreHeader("X-Agy-Cache-Warning", "warn-1")
	s.DoLocked(func(sw *StreamWriter) {
		if !sw.CommitErrorStatus(http.StatusTooManyRequests, map[string]string{"Retry-After": "3"}, "application/json") {
			t.Fatal("expected CommitErrorStatus to succeed before body bytes")
		}
		sw.WriteRaw(`{"type":"error"}`)
	})
	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") != "3" || rec.Header().Get("X-Agy-Cache-Warning") != "warn-1" {
		t.Fatalf("unexpected pre-header error response: code=%d hdrs=%+v", rec.Code, rec.Header())
	}

	// 2. Segmented heartbeat and race-free concurrent StopHeartbeat
	rec2 := httptest.NewRecorder()
	s2 := NewSSEStreamSession(rec2, SSEStreamOptions{
		HeartbeatInterval: 10 * time.Millisecond,
		WriteTimeout:      100 * time.Millisecond,
	})
	defer s2.ClearDeadline()
	var started atomic.Bool
	s2.StartHeartbeat(func(sw *StreamWriter) {
		if started.Load() {
			sw.WriteSSEEvent("ping", map[string]any{"type": "ping"})
		} else {
			sw.WriteRaw(": ping\n\n")
		}
	})
	time.Sleep(25 * time.Millisecond)
	started.Store(true)
	s2.WriteSSEEvent("message_start", map[string]any{"type": "message_start"})
	time.Sleep(25 * time.Millisecond)
	s2.StopHeartbeat()

	body := rec2.Body.String()
	if !strings.Contains(body, ": ping\n\n") || !strings.Contains(body, "event: message_start") {
		t.Fatalf("expected segmented ping and message_start in %q", body)
	}

	// 3. Write failure marks WriteFailed cleanly without panicking
	fw := newFailingFlushWriter(1)
	s3 := NewSSEStreamSession(fw, SSEStreamOptions{WriteTimeout: 50 * time.Millisecond})
	if s3.WriteSSEData(map[string]any{"ok": true}) {
		t.Fatal("expected WriteSSEData to report false on failing writer")
	}
	if !s3.WriteFailed() || !s3.HeadersCommitted() {
		t.Fatalf("expected WriteFailed & HeadersCommitted set, got failed=%v committed=%v", s3.WriteFailed(), s3.HeadersCommitted())
	}
}
