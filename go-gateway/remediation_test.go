package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestStructuredPromptPreservesRulesAndPaths(t *testing.T) {
	input := map[string]any{"path": "/home/developer/.claude/CLAUDE.md", "skill": "claude-design", "note": "<system-reminder>Claude Code and OpenClaw</system-reminder>"}
	blocks := []any{map[string]any{"type": "tool_use", "input": input}, map[string]any{"type": "tool_result", "content": input}, map[string]any{"type": "custom", "payload": input}}
	got := formatSingleMessageText(map[string]any{"content": blocks})
	for _, bad := range []string{"Claude Code", "OpenClaw", "system-reminder"} {
		if strings.Contains(got, bad) {
			t.Errorf("unprocessed history: %s", bad)
		}
	}
	for _, keep := range []string{"/home/developer/.claude/CLAUDE.md", "claude-design"} {
		if !strings.Contains(got, keep) {
			t.Errorf("identifier lost: %s", keep)
		}
	}
	if input["note"] != "<system-reminder>Claude Code and OpenClaw</system-reminder>" {
		t.Fatal("mutated host input")
	}
	mixed := "You are Claude Code, Anthropic's official CLI. Never delete backups.\n# Project instructions\nPlease check claude.ai/code and retain all audit records.\nAlways require authorization for credential testing, exploit development."
	cleaned := PruneClientSystemPrompt(mixed)
	for _, keep := range []string{"Never delete backups.", "# Project instructions", "retain all audit records.", "Always require authorization for credential testing, exploit development."} {
		if !strings.Contains(cleaned, keep) {
			t.Errorf("project rule removed: %s", keep)
		}
	}
}

func TestACPCapabilityRecoveryMatrix(t *testing.T) {
	cases := []struct{ caps, method string }{
		{`{"sessionCapabilities":{"resume":{}},"loadSession":true}`, "session/resume"},
		{`{"loadSession":true}`, "session/load"},
		{`{}`, ""},
	}
	for _, tc := range cases {
		t.Run(tc.method+tc.caps, func(t *testing.T) {
			GlobalEngine.StopEngine()
			defer GlobalEngine.StopEngine()
			t.Setenv("FAKE_ACP_CAPABILITIES", tc.caps)
			s := NewAcpProjectSession("capability-test", "/tmp", "")
			s.HibernatedSessionID = "previous-synthetic-session"
			s.MessageCount = 2
			if err := s.ensureBooted(nil, "low"); err != nil {
				t.Fatal(err)
			}
			if s.LastWakeMethod != tc.method {
				t.Fatalf("used %q, want %q", s.LastWakeMethod, tc.method)
			}
			if tc.method == "" && s.SessionID == "previous-synthetic-session" {
				t.Fatal("restored without advertised capability")
			}
			if err := s.ensureBooted([]ToolDef{{Name: "Read"}}, "low"); err == nil {
				t.Fatal("accepted tools without HTTP capability")
			}
		})
	}
	if _, err := parseAcpCapabilities([]byte(`{"protocolVersion":2}`)); err == nil {
		t.Fatal("silently accepted ACP v2")
	}
}

func TestHTTPDrainPreservesActiveStream(t *testing.T) {
	started, finish := make(chan struct{}), make(chan struct{})
	srv := gatewayHTTPServer("127.0.0.1:0", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: start\n\n")
		w.(http.Flusher).Flush()
		close(started)
		<-finish
		_, _ = io.WriteString(w, "data: done\n\n")
	}))
	if srv.ReadHeaderTimeout != 15*time.Second || srv.MaxHeaderBytes != 1<<20 || srv.IdleTimeout != 120*time.Second || srv.WriteTimeout != 0 {
		t.Fatal("HTTP limits changed")
	}
	ln, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(ln)
	defer srv.Close()
	resp, err := http.Get("http://" + ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	<-started
	drained := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() { drained <- drainHTTPServers(ctx, srv) }()
	select {
	case err := <-drained:
		t.Fatalf("shutdown did not wait: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(finish)
	b, err := io.ReadAll(resp.Body)
	if err != nil || !strings.Contains(string(b), "data: done") {
		t.Fatal("active SSE was truncated")
	}
	if err := <-drained; err != nil {
		t.Fatal(err)
	}
}

func TestHTTPReadHeaderTimeout(t *testing.T) {
	srv := gatewayHTTPServer("127.0.0.1:0", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("incomplete headers reached handler") }))
	srv.ReadHeaderTimeout = 40 * time.Millisecond
	ln, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(ln)
	defer srv.Close()
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err = io.WriteString(conn, "GET / HTTP/1.1\r\nHost: localhost\r\nX-Slow: "); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	_, err = io.ReadAll(conn)
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("server retained slow header connection")
	}
}

func TestCircuitFailureLatchAndWaitingPrompt(t *testing.T) {
	oldFile := TosTrippedFile
	oldSem := promptSem
	tosMu.Lock()
	oldMem, oldReason, oldPersisted := tosTrippedMem.Load(), tosReasonMem, tosPersisted
	tosTrippedMem.Store(false)
	tosReasonMem = ""
	tosPersisted = false
	tosMu.Unlock()
	defer func() {
		promptSem = oldSem
		TosTrippedFile = oldFile
		tosMu.Lock()
		tosTrippedMem.Store(oldMem)
		tosReasonMem = oldReason
		tosPersisted = oldPersisted
		tosMu.Unlock()
	}()
	parent := filepath.Join(t.TempDir(), "not-directory")
	if err := os.WriteFile(parent, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	TosTrippedFile = filepath.Join(parent, "marker")
	for _, text := range []string{"The column ban_reason and enum TERMS_OF_SERVICE or Account suspended text", "Discuss: This service has been disabled in this account for violation of Terms of Service."} {
		if DetectAntigravitySoftError(text) != nil {
			t.Fatal("normal discussion triggered circuit")
		}
	}
	promptSem = make(chan struct{}, 1)
	promptSem <- struct{}{}
	result := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		s := NewAcpProjectSession("queued", "/tmp", "")
		_, err := s.executePromptOrPark(context.Background(), nil, nil, 0)
		result <- err
	}()
	<-started
	TripTosCircuitBreaker("This service has been disabled in this account for violation of Terms of Service.")
	<-promptSem
	select {
	case err := <-result:
		if se, ok := err.(*SoftError); !ok || se.ErrorType != "tos_circuit_breaker" {
			t.Fatalf("queued request not rejected: %T", err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued request hung")
	}
	// Repairing the marker directory must not reset an unpersisted latch.
	if err := os.Rename(parent, parent+".old"); err != nil {
		t.Fatal(err)
	}
	if tripped, _ := IsTosTripped(); !tripped {
		t.Fatal("lost unpersisted trip")
	}
	if GlobalEngine.EnsureStarted() == nil {
		t.Fatal("engine restarted through trip")
	}
	if _, err := GlobalEngine.RPC("session/new", nil, time.Second); err == nil {
		t.Fatal("RPC accepted through trip")
	}
}

func TestUsageBatchRollbackAndRetry(t *testing.T) {
	for _, table := range []string{"usage_steps", "ingest_cursor"} {
		t.Run(table, func(t *testing.T) {
			st := &StateStore{}
			st.Init(filepath.Join(t.TempDir(), "state.db"))
			defer st.db.Close()
			_, err := st.db.Exec("CREATE TRIGGER inject_failure BEFORE INSERT ON " + table + " BEGIN SELECT RAISE(ABORT, 'injected'); END;")
			if err != nil {
				t.Fatal(err)
			}
			steps := []ParsedModelStep{{SessionID: "synthetic", StepIdx: 1}, {SessionID: "synthetic", StepIdx: 2}}
			meta := cursorMeta{lastStepIdx: 2}
			if n, err := st.persistUsageBatch("synthetic", meta, steps); err == nil || n != 0 {
				t.Fatal("failed batch reported success")
			}
			if _, ok := st.cursors["synthetic"]; ok {
				t.Fatal("memory cursor advanced")
			}
			for _, tbl := range []string{"usage_steps", "ingest_cursor"} {
				var n int
				if err := st.db.QueryRow("SELECT count(*) FROM " + tbl).Scan(&n); err != nil || n != 0 {
					t.Fatal("partial transaction persisted")
				}
			}
			if _, err := st.db.Exec("DROP TRIGGER inject_failure"); err != nil {
				t.Fatal(err)
			}
			if n, err := st.persistUsageBatch("synthetic", meta, steps); err != nil || n != 2 {
				t.Fatalf("retry failed: %v", err)
			}
			if st.cursors["synthetic"].lastStepIdx != 2 {
				t.Fatal("successful retry lost cursor")
			}
		})
	}
}

func TestDetachedSessionsNeverMigrateAcrossKeys(t *testing.T) {
	gs := &GatewayServer{sessions: map[string]*AcpProjectSession{}, tenants: map[string]*AcpProjectSession{}}
	old := NewAcpProjectSession("sid:one:ag:agent-a:u:12345678", "/tmp", "rules A")
	old.HibernatedSessionID = "synthetic-old-session"
	old.MessageCount = 2
	gs.sessions[old.ConvKey] = old
	gs.tenants[old.TenantKey] = old
	for _, key := range []string{"sid:two:ag:agent-a:u:12345678", "sid:one:ag:agent-b:u:12345678", "fp:changed:u:12345678"} {
		got := gs.getOrCreateAvailableSession(key, "/tmp", "rules B")
		if got == old || got.HibernatedSessionID != "" {
			t.Fatal("cross-key session migration")
		}
	}
	if gs.sessions[old.ConvKey] != old {
		t.Fatal("original mapping changed")
	}
}

func TestRestoreRejectsAmbiguousLegacySessionRows(t *testing.T) {
	st := &StateStore{}
	st.Init(filepath.Join(t.TempDir(), "state.db"))
	defer st.db.Close()
	for _, key := range []string{"sid:one:u:12345678", "sid:two:u:12345678"} {
		s := NewAcpProjectSession(key, "/tmp", "")
		s.SessionID = "duplicate-synthetic-session"
		s.MessageCount = 2
		st.UpsertSession(s)
	}
	gs := &GatewayServer{sessions: map[string]*AcpProjectSession{}, tenants: map[string]*AcpProjectSession{}}
	if n := st.RestoreDetachedSessions(gs); n != 0 {
		t.Fatalf("restored %d ambiguous rows", n)
	}
	var rows int
	if err := st.db.QueryRow("SELECT count(*) FROM conversations").Scan(&rows); err != nil || rows != 2 {
		t.Fatal("legacy evidence was lost")
	}
}

func TestWorkerCapacityUnderConcurrentRequests(t *testing.T) {
	oldCap := MaxLiveWorkers
	MaxLiveWorkers = 2
	defer func() { MaxLiveWorkers = oldCap }()
	gs := NewGatewayServer()
	t.Cleanup(gs.Close)
	server := httptest.NewServer(gs)
	defer server.Close()
	defer GlobalEngine.StopEngine()
	client := &http.Client{Timeout: 15 * time.Second}
	stop := make(chan struct{})
	sampled := make(chan int, 1)
	go func() {
		max := 0
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				sampled <- max
				return
			case <-tick.C:
				if n := gs.countLiveWorkers(); n > max {
					max = n
				}
			}
		}
	}()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			raw, _ := json.Marshal(map[string]any{"model": "gemini-3.8-flash", "messages": []any{map[string]any{"role": "user", "content": fmt.Sprintf("[FAKE_SLOW] concurrent session %d", i)}}})
			req, _ := http.NewRequest("POST", server.URL+"/v1/messages", bytes.NewReader(raw))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("x-claude-code-session-id", fmt.Sprintf("capacity-%d", i))
			resp, err := client.Do(req)
			if err != nil {
				t.Error(err)
				return
			}
			defer resp.Body.Close()
			_, _ = io.Copy(io.Discard, resp.Body)
			if resp.StatusCode != 200 && resp.StatusCode != 529 {
				t.Errorf("unexpected response %d", resp.StatusCode)
			}
		}(i)
	}
	wg.Wait()
	close(stop)
	peak := <-sampled
	if peak > 2 {
		t.Fatalf("worker peak %d exceeds limit 2", peak)
	}
	gs.mu.RLock()
	var live []*AcpProjectSession
	for _, s := range gs.sessions {
		live = append(live, s)
	}
	gs.mu.RUnlock()
	for _, s := range live {
		s.HibernateWorker("test_release", false)
	}
	if gs.countLiveWorkers() != 0 {
		t.Fatal("slots did not release")
	}
	s := gs.getOrCreateAvailableSession("capacity-reuse", "/tmp", "")
	if err := s.ensureBooted(nil, "low"); err != nil {
		t.Fatal(err)
	}
	if gs.countLiveWorkers() != 1 {
		t.Fatal("released slot could not be reused")
	}
}

func TestAuthConfigurationFailsClosed(t *testing.T) {
	oldKeys, oldErr := apiKeys, authConfigErr
	defer func() { apiKeys, authConfigErr = oldKeys, oldErr }()
	t.Setenv("AGY_API_KEYS", "")
	dir := t.TempDir()
	empty := filepath.Join(dir, "empty")
	if err := os.WriteFile(empty, []byte("# no keys\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{filepath.Join(dir, "missing"), empty, dir} {
		t.Setenv("AGY_API_KEYS_FILE", path)
		apiKeys = loadAPIKeys()
		if !authEnabled() || authConfigErr == nil {
			t.Fatal("configured file failure disabled authentication")
		}
		for _, guard := range []func(http.ResponseWriter, *http.Request) bool{checkAPIAuth, checkDashboardAuth} {
			rec := httptest.NewRecorder()
			if guard(rec, newLoopbackRequest("GET", "/v1/models", nil)) || rec.Code != 401 {
				t.Fatal("configuration failure accepted request")
			}
		}
	}
	t.Setenv("AGY_API_KEYS_FILE", "")
	apiKeys = loadAPIKeys()
	if authEnabled() {
		t.Fatal("explicit local mode no longer available")
	}
}

func TestLoopbackHostValidation(t *testing.T) {
	for _, host := range []string{"localhost", "LOCALHOST:1234", "127.0.0.1:18892", "::1", "[::1]", "[::1]:18892"} {
		if !isAllowedLoopbackHost(host) {
			t.Errorf("rejected %q", host)
		}
	}
	for _, host := range []string{"", "example.com", "localhost.", "evil.com", "[::1]evil", "localhost:bad", "localhost:0", "localhost:65536", " localhost", "localhost:1:2"} {
		if isAllowedLoopbackHost(host) {
			t.Errorf("accepted %q", host)
		}
	}
}

func TestLogAndCaptureRedaction(t *testing.T) {
	oldLog, oldTmp, oldRecord := GatewayLogFile, TmpLogFile, AcpRecordDir
	defer func() { GatewayLogFile, TmpLogFile, AcpRecordDir = oldLog, oldTmp, oldRecord }()
	dir := t.TempDir()
	GatewayLogFile = filepath.Join(dir, "logs", "gateway.log")
	TmpLogFile = filepath.Join(dir, "temp", "gateway.log")
	AcpRecordDir = filepath.Join(dir, "capture")
	sentinel := "REDACTION_TEST_SENTINEL"
	payload := map[string]any{"tenant": sentinel, "params": map[string]any{"mcpServers": []any{map[string]any{"url": "http://localhost/mcp/" + sentinel, "headers": []any{map[string]any{"name": "Authorization", "value": "Bearer " + sentinel}, map[string]any{"name": "x-api-key", "value": sentinel}}}}}}
	raw, _ := json.Marshal(payload)
	LogEvent("TEST", "Bearer "+sentinel, payload)
	recordAcpLine("out", raw)
	for _, p := range []string{GatewayLogFile, TmpLogFile, filepath.Join(AcpRecordDir, "acp-traffic.ndjson")} {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(b), sentinel) {
			t.Fatal("credential was written to a log")
		}
		st, err := os.Stat(p)
		if err != nil || st.Mode().Perm() != 0600 {
			t.Fatal("insecure log permissions")
		}
		st, err = os.Stat(filepath.Dir(p))
		if err != nil || st.Mode().Perm() != 0700 {
			t.Fatal("insecure log directory permissions")
		}
	}
}
