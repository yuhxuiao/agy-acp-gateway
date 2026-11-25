package protocol

import (
	"bytes"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// SSEStreamOptions configures a unified streaming response session across SSE and JSON-array transports.
type SSEStreamOptions struct {
	ContentType       string
	HeartbeatInterval time.Duration
	WriteTimeout      time.Duration
	DelayHeaders      bool
}

// SSEStreamSession coordinates header commitment, response write-deadline renewal,
// heartbeats, flushing, and serialized writes for one HTTP streaming response.
type SSEStreamSession struct {
	mu               sync.Mutex
	w                http.ResponseWriter
	flusher          http.Flusher
	rc               *http.ResponseController
	contentType      string
	heartbeatIntv    time.Duration
	writeTimeout     time.Duration
	delayHeaders     bool
	headersCommitted bool
	writeFailed      bool

	hbMu   sync.Mutex
	hbDone chan struct{}
	hbWg   sync.WaitGroup
}

// NewSSEStreamSession initializes an SSEStreamSession wrapping w.
// When opts.DelayHeaders is false, standard SSE response headers are populated immediately
// on w.Header() without calling WriteHeader yet so pre-header HTTP errors can still set status codes.
func NewSSEStreamSession(w http.ResponseWriter, opts SSEStreamOptions) *SSEStreamSession {
	ct := opts.ContentType
	if ct == "" {
		ct = "text/event-stream; charset=utf-8"
	}
	hb := opts.HeartbeatInterval
	if hb <= 0 {
		hb = 10 * time.Second
	}
	wt := opts.WriteTimeout
	if wt <= 0 {
		wt = 30 * time.Second
	}
	flusher, _ := w.(http.Flusher)
	s := &SSEStreamSession{
		w:             w,
		flusher:       flusher,
		rc:            http.NewResponseController(w),
		contentType:   ct,
		heartbeatIntv: hb,
		writeTimeout:  wt,
		delayHeaders:  opts.DelayHeaders,
	}
	if !opts.DelayHeaders {
		s.applyStreamHeadersLocked()
	}
	return s
}

func (s *SSEStreamSession) applyStreamHeadersLocked() {
	h := s.w.Header()
	h.Set("Content-Type", s.contentType)
	h.Set("Cache-Control", "no-cache, no-transform")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
}

func (s *SSEStreamSession) renewDeadlineLocked() {
	if s.rc != nil {
		_ = s.rc.SetWriteDeadline(time.Now().Add(s.writeTimeout))
	}
}

// ClearDeadline clears any per-write deadline on exit so the HTTP server can reclaim connections cleanly.
func (s *SSEStreamSession) ClearDeadline() {
	if s.rc != nil {
		_ = s.rc.SetWriteDeadline(time.Time{})
	}
}

// HeadersCommitted reports whether HTTP response status/headers have already been committed to the wire.
func (s *SSEStreamSession) HeadersCommitted() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.headersCommitted
}

// WriteFailed reports whether any underlying Write call returned an error.
func (s *SSEStreamSession) WriteFailed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeFailed
}

// SetPreHeader sets header k=v only if response headers have not yet been committed.
func (s *SSEStreamSession) SetPreHeader(k, v string) bool {
	if k == "" || v == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.headersCommitted {
		return false
	}
	s.w.Header().Set(k, v)
	return true
}

// ApplyPreHeaders invokes fn while holding the stream mutex if response headers are not yet committed.
func (s *SSEStreamSession) ApplyPreHeaders(fn func(http.Header)) bool {
	if fn == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.headersCommitted {
		return false
	}
	fn(s.w.Header())
	return true
}

// CommitHeaders ensures streaming response headers are committed (and sets optional extra headers if not yet committed).
func (s *SSEStreamSession) CommitHeaders(extra map[string]string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.commitHeadersLocked(extra)
}

func (s *SSEStreamSession) commitHeadersLocked(extra map[string]string) bool {
	if s.headersCommitted {
		return false
	}
	s.applyStreamHeadersLocked()
	for k, v := range extra {
		if k != "" && v != "" {
			s.w.Header().Set(k, v)
		}
	}
	s.headersCommitted = true
	return true
}

func (s *SSEStreamSession) writeBytesLocked(raw []byte) bool {
	if s.writeFailed {
		return false
	}
	if !s.headersCommitted {
		s.applyStreamHeadersLocked()
		s.headersCommitted = true
	}
	s.renewDeadlineLocked()
	if len(raw) > 0 {
		if _, err := s.w.Write(raw); err != nil {
			s.writeFailed = true
			return false
		}
	}
	if s.flusher != nil {
		s.flusher.Flush()
	}
	return !s.writeFailed
}

func (s *SSEStreamSession) writeLocked(raw string) bool {
	if s.writeFailed {
		return false
	}
	if raw == "" {
		return s.writeBytesLocked(nil)
	}
	return s.writeBytesLocked([]byte(raw))
}

func formatSSEEventBytes(evName string, jsonBytes []byte) []byte {
	var buf bytes.Buffer
	buf.Grow(len(evName) + len(jsonBytes) + 16)
	buf.WriteString("event: ")
	buf.WriteString(evName)
	buf.WriteString("\ndata: ")
	buf.Write(jsonBytes)
	buf.WriteString("\n\n")
	return buf.Bytes()
}

func formatSSEDataBytes(jsonBytes []byte) []byte {
	var buf bytes.Buffer
	buf.Grow(len(jsonBytes) + 8)
	buf.WriteString("data: ")
	buf.Write(jsonBytes)
	buf.WriteString("\n\n")
	return buf.Bytes()
}

// WriteRaw commits headers if needed, renews the write deadline, writes raw bytes, and flushes.
func (s *SSEStreamSession) WriteRaw(raw string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeLocked(raw)
}

// WriteSSEEvent marshals payload as JSON and writes an `event: <ev>\ndata: <json>\n\n` frame.
func (s *SSEStreamSession) WriteSSEEvent(evName string, payload any) bool {
	s.mu.Lock()
	if s.writeFailed {
		s.mu.Unlock()
		return false
	}
	s.mu.Unlock()
	b, _ := json.Marshal(payload)
	frame := formatSSEEventBytes(evName, b)
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeBytesLocked(frame)
}

// WriteSSEData marshals payload as JSON and writes a `data: <json>\n\n` frame.
func (s *SSEStreamSession) WriteSSEData(payload any) bool {
	s.mu.Lock()
	if s.writeFailed {
		s.mu.Unlock()
		return false
	}
	s.mu.Unlock()
	b, _ := json.Marshal(payload)
	frame := formatSSEDataBytes(b)
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeBytesLocked(frame)
}

// WriteSSEComment writes a W3C SSE comment line `: <comment>\n\n`.
func (s *SSEStreamSession) WriteSSEComment(comment string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.writeFailed {
		return false
	}
	var buf bytes.Buffer
	buf.Grow(len(comment) + 4)
	buf.WriteString(": ")
	buf.WriteString(comment)
	buf.WriteString("\n\n")
	return s.writeBytesLocked(buf.Bytes())
}

// DoLocked executes fn exclusively under the stream mutex.
// Inside fn, the caller receives a `StreamWriter` bound to this lock.
func (s *SSEStreamSession) DoLocked(fn func(w *StreamWriter)) {
	if fn == nil {
		return
	}
	sw := &StreamWriter{s: s}
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(sw)
}

// StreamWriter offers locked primitives inside SSEStreamSession.DoLocked.
type StreamWriter struct {
	s *SSEStreamSession
}

// HeadersCommitted reports whether headers have been committed.
func (sw *StreamWriter) HeadersCommitted() bool {
	return sw.s.headersCommitted
}

// Header returns the HTTP response header map (only safe to mutate before HeadersCommitted).
func (sw *StreamWriter) Header() http.Header {
	return sw.s.w.Header()
}

// CommitErrorStatus writes non-200 error status & headers if not yet committed. Returns true if committed now.
func (sw *StreamWriter) CommitErrorStatus(statusCode int, extraHeaders map[string]string, overwriteContentType string) bool {
	if sw.s.headersCommitted {
		return false
	}
	if overwriteContentType != "" {
		sw.s.w.Header().Set("Content-Type", overwriteContentType)
	}
	for k, v := range extraHeaders {
		if k != "" && v != "" {
			sw.s.w.Header().Set(k, v)
		}
	}
	sw.s.w.WriteHeader(statusCode)
	sw.s.headersCommitted = true
	return true
}

// WriteRaw writes raw bytes with header commitment, write-deadline renewal, and Flush.
func (sw *StreamWriter) WriteRaw(raw string) bool {
	return sw.s.writeLocked(raw)
}

// WriteSSEEvent writes `event: <evName>\ndata: <json>\n\n` with deadline renewal & Flush.
func (sw *StreamWriter) WriteSSEEvent(evName string, payload any) bool {
	if sw.s.writeFailed {
		return false
	}
	b, _ := json.Marshal(payload)
	return sw.s.writeBytesLocked(formatSSEEventBytes(evName, b))
}

// WriteSSEData writes `data: <json>\n\n` with deadline renewal & Flush.
func (sw *StreamWriter) WriteSSEData(payload any) bool {
	if sw.s.writeFailed {
		return false
	}
	b, _ := json.Marshal(payload)
	return sw.s.writeBytesLocked(formatSSEDataBytes(b))
}

// StartHeartbeat begins emitting periodic heartbeat frames via frameFn (or custom fn under stream lock).
// Pass nil frameFn to skip starting. Call StopHeartbeat before writing final terminal responses.
func (s *SSEStreamSession) StartHeartbeat(fn func(sw *StreamWriter)) {
	if fn == nil {
		return
	}
	s.hbMu.Lock()
	if s.hbDone != nil {
		s.hbMu.Unlock()
		return
	}
	done := make(chan struct{})
	s.hbDone = done
	s.hbWg.Add(1)
	intv := s.heartbeatIntv
	s.hbMu.Unlock()

	go func() {
		defer s.hbWg.Done()
		tk := time.NewTicker(intv)
		defer tk.Stop()
		sw := &StreamWriter{s: s}
		for {
			select {
			case <-done:
				return
			case <-tk.C:
				s.mu.Lock()
				failed := s.writeFailed
				if !failed {
					fn(sw)
					failed = s.writeFailed
				}
				s.mu.Unlock()
				if failed {
					return
				}
			}
		}
	}()
}

// StopHeartbeat stops any active heartbeat ticker goroutine and waits for it to exit.
func (s *SSEStreamSession) StopHeartbeat() {
	s.hbMu.Lock()
	done := s.hbDone
	s.hbDone = nil
	s.hbMu.Unlock()
	if done == nil {
		return
	}
	close(done)
	s.hbWg.Wait()
}
