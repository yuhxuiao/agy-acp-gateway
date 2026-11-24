package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRound2_SinglePassRoutingHintsAndFirstArrayElem(t *testing.T) {
	arrRaw := json.RawMessage(`[{"role":"user","content":"hello"},` + `{"role":"assistant","content":"` + strings.Repeat("x", 32768) + `"}]`)
	first := extractFirstJSONArrayElement(arrRaw)
	if !strings.Contains(string(first), `"hello"`) {
		t.Fatalf("expected first element, got %s", string(first))
	}
	if extractFirstJSONArrayElement(json.RawMessage(`[]`)) != nil {
		t.Fatalf("expected nil on empty array")
	}
	if extractFirstJSONArrayElement(json.RawMessage(`{"a":1}`)) != nil {
		t.Fatalf("expected nil on non-array")
	}

	// 1. Exact lowercase "model" + prompt_cache_key
	r1 := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"gemini-3.8-flash","prompt_cache_key":"k1","messages":[{"role":"user","content":"hi"}]}`))
	m1, sk1, _, mal1, big1, err1 := extractRequestRoutingHints(r1)
	if err1 != nil || mal1 || big1 || m1 != "gemini-3.8-flash" || sk1 != "pck:k1" {
		t.Fatalf("unexpected hints #1: model=%q sk=%q mal=%v big=%v err=%v", m1, sk1, mal1, big1, err1)
	}

	// 2. Case-sensitive rejection of "Model" and non-string "model": 123
	r2 := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"Model":"gemini-4-carbon","messages":[{"role":"user","content":"hi"}]}`))
	m2, sk2, _, mal2, _, _ := extractRequestRoutingHints(r2)
	if mal2 || m2 != "" || !strings.HasPrefix(sk2, "fp:") {
		t.Fatalf("expected empty model and fp sessionKey on capitalized Model, got m=%q sk=%q mal=%v", m2, sk2, mal2)
	}

	r3 := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":123,"metadata":{"user_id":"user_abc_session_999"},"messages":[{"role":"user","content":"hi"}]}`))
	m3, sk3, _, mal3, _, _ := extractRequestRoutingHints(r3)
	if mal3 || m3 != "" || sk3 != "meta:user_abc_session_999" {
		t.Fatalf("expected empty model and meta sessionKey on numeric model, got m=%q sk=%q mal=%v", m3, sk3, mal3)
	}

	// 3. Malformed JSON detected cleanly
	r4 := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"gemini-3.8-flash",`))
	_, _, _, mal4, _, _ := extractRequestRoutingHints(r4)
	if !mal4 {
		t.Fatalf("expected malformedJSON=true on truncated JSON")
	}
}

func TestRound2_ProxyFailClosedAndC1CompliantProbeEgress(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("AGY_GW_HOME", tmp)
	t.Setenv("AGY_ACCOUNTS_FILE", filepath.Join(tmp, "accounts.json"))
	t.Setenv("AGY_RESIN_PROXY_TOKEN", "")
	t.Setenv("AGY_UPSTREAM_PROXY", "")
	initConfig()
	t.Cleanup(initConfig)

	am := NewAccountManager()
	am.Init()

	_, err := am.UpsertAccount(AccountProfile{
		ID:            "sub-resin",
		Label:         "Sub Resin",
		Enabled:       true,
		ProxyPreset:   "us-sjc",
		AllowedModels: []string{"gemini-3.8-flash"},
	})
	if err != nil {
		t.Fatalf("UpsertAccount failed: %v", err)
	}
	subAcc, ok := am.GetAccount("sub-resin")
	if !ok {
		t.Fatalf("expected sub-resin account")
	}
	if _, err := am.ResolveAccountProxyChecked(subAcc); err == nil {
		t.Fatalf("expected ResolveAccountProxyChecked to fail closed when Resin token is missing")
	}
	if _, err := am.ProbeAccountEgress("sub-resin"); err == nil {
		t.Fatalf("expected ProbeAccountEgress to fail closed when Resin token is missing")
	}

	var mu sync.Mutex
	var requestedHosts []string
	mockProxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requestedHosts = append(requestedHosts, r.Host)
		mu.Unlock()
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer mockProxy.Close()

	_, err = am.UpsertAccount(AccountProfile{
		ID:            "sub-custom",
		Label:         "Sub Custom",
		Enabled:       true,
		ProxyPreset:   "custom",
		CustomProxy:   mockProxy.URL,
		AllowedModels: []string{"gemini-3.8-flash"},
	})
	if err != nil {
		t.Fatalf("UpsertAccount custom failed: %v", err)
	}
	_, _ = am.ProbeAccountEgress("sub-custom")

	mu.Lock()
	hosts := append([]string(nil), requestedHosts...)
	mu.Unlock()
	for _, h := range hosts {
		if strings.Contains(strings.ToLower(h), "cloudcode-pa.googleapis.com") {
			t.Fatalf("C1 violation: ProbeAccountEgress contacted private CCPA endpoint %q", h)
		}
	}
}

func TestRound2_AccountSandboxSeededAndAmbientEnvStripped(t *testing.T) {
	tmp := t.TempDir()
	pureHome := filepath.Join(tmp, "runtime", "pure-api-home")
	t.Setenv("AGY_GW_HOME", tmp)
	t.Setenv("AGY_PURE_API_HOME", pureHome)
	t.Setenv("AGY_ACCOUNTS_FILE", filepath.Join(tmp, "runtime", "accounts.json"))
	initConfig()
	t.Cleanup(initConfig)

	am := NewAccountManager()
	am.Init()
	prim, ok := am.GetPrimaryAccount()
	if !ok {
		t.Fatalf("expected primary account")
	}
	if err := am.EnsureAccountSandboxSeeded(prim.ID); err != nil {
		t.Fatalf("EnsureAccountSandboxSeeded failed: %v", err)
	}
	if !fileExists(filepath.Join(pureHome, "config", "projects", "default-cli-project.json")) {
		t.Fatalf("expected default-cli-project.json seeded")
	}
	if !fileExists(filepath.Join(pureHome, "antigravity-cli", "cache", "onboarding.json")) {
		t.Fatalf("expected onboarding.json seeded")
	}
	dotGemini := filepath.Join(am.AccountHomeDir(prim.ID), ".gemini")
	resolved, err := filepath.EvalSymlinks(dotGemini)
	expectedPureHome := pureHome
	if evalPure, evalErr := filepath.EvalSymlinks(pureHome); evalErr == nil {
		expectedPureHome = evalPure
	}
	if err != nil || filepath.Clean(resolved) != filepath.Clean(expectedPureHome) {
		t.Fatalf("expected %s symlink -> %s, got %q (err=%v)", dotGemini, expectedPureHome, resolved, err)
	}

	for _, kv := range []string{
		"GEMINI_API_KEY=secret",
		"GOOGLE_API_KEY=secret",
		"GOOGLE_APPLICATION_CREDENTIALS=/tmp/adc.json",
		"GOOGLE_CLOUD_PROJECT=my-proj",
		"GOOGLE_GENAI_USE_VERTEXAI=true",
		"GEMINI_HOME=/tmp/other",
		"ELECTRON_RUN_AS_NODE=1",
		"CHROME_CRASHPAD_PIPE_NAME=pipe",
		"DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/1000/bus",
	} {
		if !isStrippedAgyEngineEnv(kv) || !isStrippedSubprocessEnv(kv) {
			t.Fatalf("expected %q to be stripped from agy subprocess env", kv)
		}
	}
}

type failPinAcpEngine struct {
	*mockAcpEngine
	closeTransportFail bool
}

func (f *failPinAcpEngine) CloseSession(sid string) error {
	if f.closeTransportFail {
		return errors.New("simulated close transport timeout")
	}
	return f.mockAcpEngine.CloseSession(sid)
}

func (f *failPinAcpEngine) RPC(method string, params any, timeout time.Duration) (json.RawMessage, error) {
	if method == "session/set_config_option" {
		return nil, &acpRPCError{Raw: `{"code":-32602,"message":"tier rejected"}`}
	}
	if method == "session/resume" {
		return json.Marshal(map[string]any{"ok": true})
	}
	return f.mockAcpEngine.RPC(method, params, timeout)
}

func TestRound2_PinSessionConfigFailureRollsBackPhaseAndHibernation(t *testing.T) {
	eng := &failPinAcpEngine{
		mockAcpEngine:      newMockAcpEngine(),
		closeTransportFail: true,
	}
	sess := NewAcpProjectSession("conv:pin-rollback", t.TempDir(), "sys")
	sess.Engine = eng
	sess.Store = &StateStore{}
	sess.setDesiredSelection(ResolveRequestedModel("gemini-3.8-flash", nil))

	err := sess.openFreshSession(nil, "low")
	var mse *ModelSwitchError
	if !errors.As(err, &mse) {
		t.Fatalf("expected ModelSwitchError from openFreshSession, got %v", err)
	}
	if sess.SessionID != "" || sess.Phase() != PhaseHibernated {
		t.Fatalf("expected SessionID='' and PhaseHibernated after failed openFreshSession, got sid=%q phase=%q", sess.SessionID, sess.Phase())
	}
	eng.mu.Lock()
	_, leaked := eng.listeners[eng.nextSid]
	eng.mu.Unlock()
	if leaked {
		t.Fatalf("expected listener for %s unregistered even when CloseSession failed", eng.nextSid)
	}

	// Now verify reloadHibernatedSession restores HibernatedSessionID + HibernatedParkedCallIDs + PhaseHibernated
	sess.mu.Lock()
	sess.HibernatedSessionID = "00000000-0000-0000-0000-00000000b2b2"
	sess.HibernatedParkedCallIDs = map[string]bool{"call-parked-1": true}
	sess.syncPhaseLocked()
	sess.mu.Unlock()

	err2 := sess.reloadHibernatedSession("00000000-0000-0000-0000-00000000b2b2", nil, "low")
	if !errors.As(err2, &mse) {
		t.Fatalf("expected ModelSwitchError from reloadHibernatedSession, got %v", err2)
	}
	if sess.SessionID != "" || sess.HibernatedSessionID != "00000000-0000-0000-0000-00000000b2b2" || sess.Phase() != PhaseHibernated || !sess.HibernatedParkedCallIDs["call-parked-1"] {
		t.Fatalf("expected hibernated state restored after failed reload pin, got sid=%q hibSid=%q phase=%q parked=%v",
			sess.SessionID, sess.HibernatedSessionID, sess.Phase(), sess.HibernatedParkedCallIDs)
	}
}

func TestRound2_RoDBBorrowConcurrencyAndFalseHibernateGuard(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("AGY_CONV_DIR", tmp)
	initConfig()
	t.Cleanup(initConfig)

	sid := "00000000-0000-4000-8000-00000000c3c3"
	dbPath := filepath.Join(tmp, sid+".db")
	wdb, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open temp sqlite: %v", err)
	}
	if _, err := wdb.Exec(`CREATE TABLE trajectory_steps (step_index INTEGER PRIMARY KEY, step_type TEXT, step BLOB, error_details BLOB); INSERT INTO trajectory_steps(step_index, step_type, step) VALUES (1, 'CORTEX_STEP_TYPE_USER_INPUT', x'00');`); err != nil {
		_ = wdb.Close()
		t.Fatalf("seed temp sqlite: %v", err)
	}
	_ = wdb.Close()

	s := NewAcpProjectSession("conv:rodb", t.TempDir(), "sys")
	s.Engine = newMockAcpEngine()
	s.Store = &StateStore{}
	s.SessionID = sid
	s.syncPhaseLocked()

	db := s.getReadOnlySessionDB(sid)
	if db == nil {
		t.Fatalf("expected non-nil roDB")
	}
	// Non-matching expectSid must NOT close active s.roDB
	if s.HibernateIfSession("00000000-0000-4000-8000-00000000ffff", "stale_timer") {
		t.Fatalf("expected HibernateIfSession with mismatched sid to return false")
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("expected active s.roDB to remain open on rejected HibernateIfSession, got %v", err)
	}

	// Concurrent borrowReadOnlySessionDB vs closeReadOnlyDB must never panic or race on closed DB
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if bdb, rel := s.borrowReadOnlySessionDB(sid); bdb != nil {
					var cnt int
					errQ := bdb.QueryRow(`SELECT COUNT(*) FROM trajectory_steps`).Scan(&cnt)
					rel()
					if errQ != nil && strings.Contains(errQ.Error(), "database is closed") {
						t.Errorf("unexpected closed database while holding roDB RLock: %v", errQ)
						return
					}
				}
			}
		}()
	}
	for i := 0; i < 20; i++ {
		s.closeReadOnlyDB()
		time.Sleep(time.Millisecond)
	}
	close(stop)
	wg.Wait()
	s.closeReadOnlyDB()
}

func TestRound2_SchedulesTailBufferAndStdinWatchdog(t *testing.T) {
	if quotaAttemptCooldown(0) != 30*time.Second || quotaAttemptCooldown(1) != 2*time.Minute || quotaAttemptCooldown(2) != 5*time.Minute || quotaAttemptCooldown(5) != 15*time.Minute {
		t.Fatalf("unexpected quotaAttemptCooldown schedule")
	}
	if workerCrashBackoffDuration(1) != 2*time.Second || workerCrashBackoffDuration(2) != 4*time.Second || workerCrashBackoffDuration(10) != 30*time.Second {
		t.Fatalf("unexpected workerCrashBackoffDuration schedule")
	}

	tb := &boundedTailBuffer{max: 10}
	_, _ = tb.Write([]byte("012345"))
	_, _ = tb.Write([]byte("6789abcdef"))
	if tb.String() != "6789abcdef" {
		t.Fatalf("expected tail '6789abcdef', got %q", tb.String())
	}

	// Verify sendRawJSON stdinWriteWatchdogTimeout breaks a wedged pipe without hanging forever
	prevWd := stdinWriteWatchdogTimeout
	stdinWriteWatchdogTimeout = 60 * time.Millisecond
	defer func() { stdinWriteWatchdogTimeout = prevWd }()

	pr, pw, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	defer func() {
		_ = pr.Close()
		_ = pw.Close()
	}()

	eng := newSharedAcpEngine()
	done := make(chan struct{})
	// Simulate StopEngine closing the read end of the pipe when invoked
	sleepCmd := exec.Command("sleep", "5")
	if err := sleepCmd.Start(); err != nil {
		t.Skipf("sleep unavailable: %v", err)
	}
	go func() {
		_ = sleepCmd.Wait()
		_ = pr.Close()
		close(done)
	}()
	eng.mu.Lock()
	eng.cmd = sleepCmd
	eng.exitDone = done
	eng.stdin = bufio.NewWriterSize(pw, 1024)
	eng.stdinCloser = pw
	eng.mu.Unlock()

	errCh := make(chan error, 1)
	go func() {
		// 4MB payload overflows Linux 64KB pipe buffer and blocks until watchdog stops engine & closes pr
		errCh <- eng.sendRawJSON(map[string]any{
			"jsonrpc": "2.0",
			"id":      1,
			"method":  "session/cancel",
			"params":  map[string]any{"pad": string(bytes.Repeat([]byte("a"), 4*1024*1024))},
		})
	}()

	select {
	case wErr := <-errCh:
		if wErr == nil {
			t.Fatalf("expected broken pipe error after watchdog fired")
		}
	case <-time.After(4 * time.Second):
		eng.StopEngine()
		t.Fatalf("sendRawJSON hung despite stdinWriteWatchdogTimeout")
	}
}
