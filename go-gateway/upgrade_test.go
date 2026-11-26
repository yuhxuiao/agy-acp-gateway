package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeUpgradeStatus(t *testing.T, dir string, cand map[string]any) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(map[string]any{"candidate": cand, "gates": map[string]any{"contract": "passed"}})
	if err := os.WriteFile(filepath.Join(dir, "status.json"), b, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestUpgradeApproveGuards(t *testing.T) {
	oldKeys, oldErr := apiKeys, authConfigErr
	defer func() { apiKeys, authConfigErr = oldKeys, oldErr }()
	dir := t.TempDir()
	t.Setenv("AGY_UPGRADE_DIR", dir)
	sha := strings.Repeat("ab", 32)
	writeUpgradeStatus(t, dir, map[string]any{"id": "agy-1.5.6-abcd", "kind": "agy", "version": "1.5.6", "sha256": sha, "state": "ready"})
	approve := func(origin, marker string, body map[string]any) (int, string) {
		raw, _ := json.Marshal(body)
		req := httptest.NewRequest(http.MethodPost, "https://gw.example/api/upgrade/approve", bytes.NewReader(raw))
		req.Host = "gw.example"
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		if marker != "" {
			req.Header.Set("X-Agy-Requested-With", marker)
		}
		rec := httptest.NewRecorder()
		HandleUpgradeApprove(rec, req)
		return rec.Code, rec.Body.String()
	}
	good := map[string]any{"candidateId": "agy-1.5.6-abcd", "sha256": sha}

	apiKeys, authConfigErr = nil, nil
	if code, _ := approve("https://gw.example", "dashboard", good); code != http.StatusForbidden {
		t.Fatalf("approval on an unauthenticated gateway: %d", code)
	}
	t.Setenv("AGY_API_KEYS", "guard-api-key-0123456789")
	apiKeys = loadAPIKeys()
	if code, _ := approve("https://evil.example", "dashboard", good); code != http.StatusForbidden {
		t.Fatalf("cross-origin approval accepted: %d", code)
	}
	if code, _ := approve("", "dashboard", good); code != http.StatusForbidden {
		t.Fatalf("approval without Origin accepted: %d", code)
	}
	if code, _ := approve("https://gw.example", "", good); code != http.StatusForbidden {
		t.Fatalf("approval without custom header accepted: %d", code)
	}
	if code, _ := approve("https://gw.example", "dashboard", map[string]any{"candidateId": "agy-1.5.6-abcd", "sha256": strings.Repeat("cd", 32)}); code != http.StatusConflict {
		t.Fatalf("mismatched sha accepted: %d", code)
	}
	code, body := approve("https://gw.example", "dashboard", good)
	if code != http.StatusOK {
		t.Fatalf("valid approval rejected: %d %s", code, body)
	}
	b, err := os.ReadFile(filepath.Join(dir, "approvals", "agy-1.5.6-abcd.json"))
	if err != nil || !strings.Contains(string(b), sha) || strings.Contains(string(b), "guard-api-key") {
		t.Fatalf("approval file wrong (or leaks the key): %s %v", b, err)
	}
	writeUpgradeStatus(t, dir, map[string]any{"id": "agy-1.5.6-abcd", "kind": "agy", "version": "1.5.6", "sha256": sha, "state": "failed"})
	if code, _ := approve("https://gw.example", "dashboard", good); code != http.StatusConflict {
		t.Fatalf("approval of a failed candidate accepted: %d", code)
	}
}

// U4: switching the AGY_ACP_BIN symlink restarts the engine once idle, without restarting the
// gateway, and sessions resume on the new binary.
func TestAcpBinaryHotSwitch(t *testing.T) {
	tmp := t.TempDir()
	src, err := os.ReadFile(fakeAcpBin)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []string{"v1", "v2"} {
		d := filepath.Join(tmp, "agy", v)
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(d, "agy-acp-900k"), src, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	current := filepath.Join(tmp, "agy", "current")
	if err := os.Symlink("v1", current); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { GlobalEngine.StopEngine(); initConfig() })
	GlobalEngine.StopEngine()
	t.Setenv("AGY_ACP_BIN", filepath.Join(current, "agy-acp-900k"))
	t.Setenv("AGY_PURE_API_HOME", filepath.Join(tmp, "pure"))
	t.Setenv("AGY_UPGRADE_DIR", filepath.Join(tmp, "upgrade"))
	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "gateway.db"))
	t.Setenv("AGY_GW_LOG", filepath.Join(tmp, "gateway.log"))
	initConfig()
	gs := NewGatewayServer()
	defer gs.Close()
	srv := httptest.NewServer(gs)
	defer srv.Close()
	sid := "hot-switch"
	msgs := []map[string]any{{"role": "user", "content": "one"}}
	code, out := reviewPost(t, srv.URL, "/v1/messages", sid, reviewMessages("gemini-3.8-flash", msgs, nil))
	if code != 200 {
		t.Fatalf("first: %d %s", code, out)
	}
	if running, _ := GlobalEngine.binaryInfo()["running"].(string); !strings.HasSuffix(running, filepath.Join("v1", "agy-acp-900k")) {
		t.Fatalf("running binary not recorded: %q", running)
	}
	if _, err := os.Stat(filepath.Join(tmp, "upgrade", "engine.json")); err != nil {
		t.Fatalf("engine.json not published: %v", err)
	}
	if acpBinarySwitchPending() {
		t.Fatal("switch pending without a change")
	}
	tmpLink := current + ".new"
	if err := os.Symlink("v2", tmpLink); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmpLink, current); err != nil {
		t.Fatal(err)
	}
	if !acpBinarySwitchPending() {
		t.Fatal("switch not detected")
	}
	gs.reapIdleWorkers()
	if GlobalEngine.IsAlive() {
		t.Fatal("engine not stopped after idle switch")
	}
	var first map[string]any
	_ = json.Unmarshal([]byte(out), &first)
	msgs = append(msgs, map[string]any{"role": "assistant", "content": first["content"]}, map[string]any{"role": "user", "content": "two"})
	code, out = reviewPost(t, srv.URL, "/v1/messages", sid, reviewMessages("gemini-3.8-flash", msgs, nil))
	if code != 200 || !strings.Contains(out, "FAKE_OK") {
		t.Fatalf("turn after switch: %d %s", code, out)
	}
	logb, _ := os.ReadFile(filepath.Join(tmp, "gateway.log"))
	if !strings.Contains(string(logb), "SESSION_RESUME") || !strings.Contains(string(logb), "ACP_BINARY_SWITCHED") {
		t.Fatal("session was not resumed on the new engine (fake process state is lost on restart, real agy resumes from disk)")
	}
	if running, _ := GlobalEngine.binaryInfo()["running"].(string); !strings.HasSuffix(running, filepath.Join("v2", "agy-acp-900k")) {
		t.Fatalf("engine not on the new binary: %q", running)
	}
}

// The dashboard route works end to end with API-key auth on (production): the browser sends a
// same-origin POST with its Basic credentials (the API key) and the custom header.
func TestUpgradeApproveRoute(t *testing.T) {
	oldKeys, oldErr := apiKeys, authConfigErr
	defer func() { apiKeys, authConfigErr = oldKeys, oldErr }()
	t.Setenv("AGY_API_KEYS", "route-api-key-0123456789")
	apiKeys, authConfigErr = loadAPIKeys(), nil
	dir := t.TempDir()
	t.Setenv("AGY_UPGRADE_DIR", dir)
	sha := strings.Repeat("ef", 32)
	writeUpgradeStatus(t, dir, map[string]any{"id": "agy-1.5.7-route", "kind": "agy", "version": "1.5.7", "sha256": sha, "state": "ready"})
	gs := NewGatewayServer()
	defer gs.Close()
	srv := httptest.NewServer(gs)
	defer srv.Close()
	post := func(apiKey, origin string) int {
		raw, _ := json.Marshal(map[string]any{"candidateId": "agy-1.5.7-route", "sha256": sha})
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/upgrade/approve", bytes.NewReader(raw))
		req.SetBasicAuth("dash", apiKey)
		req.Header.Set("Origin", origin)
		req.Header.Set("X-Agy-Requested-With", "dashboard")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if code := post("wrong-api-key-000000000", srv.URL); code != http.StatusUnauthorized {
		t.Fatalf("approval without dashboard login: %d", code)
	}
	if code := post("route-api-key-0123456789", "https://evil.example"); code != http.StatusForbidden {
		t.Fatalf("cross-origin approval with valid login: %d", code)
	}
	if code := post("route-api-key-0123456789", srv.URL); code != http.StatusOK {
		t.Fatalf("valid dashboard approval: %d", code)
	}
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/upgrade", nil)
	req.Header.Set("x-api-key", "route-api-key-0123456789")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&payload)
	resp.Body.Close()
	if payload["enabled"] != true || payload["status"] == nil {
		t.Fatalf("GET /api/upgrade: %v", payload)
	}
	if payload["approvalPending"] != true {
		t.Fatalf("expected approvalPending=true after approval before host pickup, got %v", payload["approvalPending"])
	}
	if _, err := os.Stat(filepath.Join(dir, "approvals", "agy-1.5.7-route.json")); err != nil {
		t.Fatal(err)
	}
}

func TestUpgradeIdempotencyPortIsolationAndObservability(t *testing.T) {
	oldKeys, oldErr := apiKeys, authConfigErr
	prevDB := GatewayStateDB
	t.Cleanup(func() {
		apiKeys, authConfigErr = oldKeys, oldErr
		initConfig()
		InitGlobalStore(prevDB)
	})
	t.Setenv("AGY_API_KEYS", "obs-api-key-0123456789")
	apiKeys, authConfigErr = loadAPIKeys(), nil

	dir := t.TempDir()
	t.Setenv("AGY_UPGRADE_DIR", dir)
	t.Setenv("AGY_STATE_DB", filepath.Join(dir, "state.db"))
	t.Setenv("AGY_PURE_API_HOME", filepath.Join(dir, "pure"))
	t.Setenv("AGY_GW_LOG", filepath.Join(dir, "gateway.log"))
	initConfig()
	InitGlobalStore(filepath.Join(dir, "state.db"))

	gs := NewGatewayServer()
	defer gs.Close()

	// 1. No status.json -> state="none", approvalPending=false, lastCheckError=""
	if got := CurrentUpgradeMetricState(); got != "none" {
		t.Fatalf("expected state=none without status.json, got %q", got)
	}
	p0 := BuildUpgradePayload()
	if p0["approvalPending"] != false || p0["lastCheckError"] != "" {
		t.Fatalf("unexpected initial upgrade payload: %+v", p0)
	}

	// 2. Port isolation: GatewayOnlyHandler rejects /api/upgrade and /api/upgrade/approve with 404
	gwHandler := gs.GatewayOnlyHandler()
	for _, tc := range []struct{ method, path string }{
		{http.MethodGet, "/api/upgrade"},
		{http.MethodPost, "/api/upgrade/approve"},
	} {
		req := httptest.NewRequest(tc.method, "http://127.0.0.1:18890"+tc.path, nil)
		req.Header.Set("x-api-key", "obs-api-key-0123456789")
		rec := httptest.NewRecorder()
		gwHandler.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("GatewayOnlyHandler must return 404 for %s %s, got %d", tc.method, tc.path, rec.Code)
		}
	}

	// 3. Ready candidate -> first approve writes file (alreadyApproved=false), second is idempotent (alreadyApproved=true)
	sha := strings.Repeat("12", 32)
	writeUpgradeStatus(t, dir, map[string]any{
		"id": "agy-1.5.10-cand", "kind": "agy", "version": "1.5.10", "sha256": sha, "state": "ready",
	})
	if got := CurrentUpgradeMetricState(); got != "ready" {
		t.Fatalf("expected state=ready before approval, got %q", got)
	}

	dashHandler := gs.DashboardOnlyHandler()
	doApprove := func() map[string]any {
		raw, _ := json.Marshal(map[string]any{"candidateId": "agy-1.5.10-cand", "sha256": sha})
		req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18891/api/upgrade/approve", bytes.NewReader(raw))
		req.Host = "127.0.0.1:18891"
		req.Header.Set("x-api-key", "obs-api-key-0123456789")
		req.Header.Set("Origin", "http://127.0.0.1:18891")
		req.Header.Set("X-Agy-Requested-With", "dashboard")
		rec := httptest.NewRecorder()
		dashHandler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 on dashboard approve, got %d: %s", rec.Code, rec.Body.String())
		}
		var out map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
		return out
	}

	first := doApprove()
	if first["ok"] != true || first["alreadyApproved"] != false {
		t.Fatalf("unexpected first approval response: %+v", first)
	}
	firstApp, _ := first["approval"].(map[string]any)
	firstAt, _ := firstApp["approvedAt"].(string)

	second := doApprove()
	if second["ok"] != true || second["alreadyApproved"] != true {
		t.Fatalf("expected idempotent alreadyApproved=true on duplicate click, got %+v", second)
	}
	secondApp, _ := second["approval"].(map[string]any)
	if secondApp["approvedAt"] != firstAt {
		t.Fatalf("duplicate approval must preserve original approvedAt %q, got %v", firstAt, secondApp["approvedAt"])
	}
	if got := CurrentUpgradeMetricState(); got != "approved" {
		t.Fatalf("expected state=approved while approval file is pending, got %q", got)
	}
	prom := RenderPrometheusMetrics(gs)
	if !strings.Contains(prom, `agy_gateway_upgrade_state{state="approved"} 1`) || !strings.Contains(prom, `agy_gateway_upgrade_state{state="ready"} 0`) {
		t.Fatalf("expected Prometheus upgrade state=approved, got:\n%s", prom)
	}

	// 3b. Expired (>=24h old) approval file must NOT lock out #up-ok (approvalPending=false, state="ready")
	// and re-approving must overwrite it with a fresh approvedAt (alreadyApproved=false).
	approvalPath := filepath.Join(dir, "approvals", "agy-1.5.10-cand.json")
	staleAt := time.Now().UTC().Add(-25 * time.Hour).Format(time.RFC3339)
	staleRaw, _ := json.Marshal(map[string]any{
		"candidateId": "agy-1.5.10-cand",
		"sha256":      sha,
		"kind":        "agy",
		"version":     "1.5.10",
		"approvedAt":  staleAt,
	})
	if err := os.WriteFile(approvalPath, staleRaw, 0o644); err != nil {
		t.Fatal(err)
	}
	if pStale := BuildUpgradePayload(); pStale["approvalPending"] != false {
		t.Fatalf("expired approval file must report approvalPending=false, got %+v", pStale)
	}
	if got := CurrentUpgradeMetricState(); got != "ready" {
		t.Fatalf("expired approval file must report metric state=ready, got %q", got)
	}
	refreshed := doApprove()
	if refreshed["ok"] != true || refreshed["alreadyApproved"] != false {
		t.Fatalf("expected expired approval file to be overwritten with alreadyApproved=false, got %+v", refreshed)
	}
	refApp, _ := refreshed["approval"].(map[string]any)
	if refApp["approvedAt"] == staleAt {
		t.Fatalf("expected fresh approvedAt after re-approving expired file, still got %v", refApp["approvedAt"])
	}

	// 3c. SHA-mismatched approval file must also report approvalPending=false and get overwritten on re-approval.
	mismatchRaw, _ := json.Marshal(map[string]any{
		"candidateId": "agy-1.5.10-cand",
		"sha256":      strings.Repeat("99", 32),
		"kind":        "agy",
		"version":     "1.5.10",
		"approvedAt":  time.Now().UTC().Format(time.RFC3339),
	})
	if err := os.WriteFile(approvalPath, mismatchRaw, 0o644); err != nil {
		t.Fatal(err)
	}
	if pMis := BuildUpgradePayload(); pMis["approvalPending"] != false {
		t.Fatalf("SHA-mismatched approval file must report approvalPending=false, got %+v", pMis)
	}
	if got := CurrentUpgradeMetricState(); got != "ready" {
		t.Fatalf("SHA-mismatched approval file must report metric state=ready, got %q", got)
	}
	realigned := doApprove()
	if realigned["ok"] != true || realigned["alreadyApproved"] != false {
		t.Fatalf("expected SHA-mismatched approval file to be overwritten with alreadyApproved=false, got %+v", realigned)
	}
	realignedApp, _ := realigned["approval"].(map[string]any)
	if realignedApp["sha256"] != sha {
		t.Fatalf("expected overwritten approval sha256=%s, got %v", sha, realignedApp["sha256"])
	}
	if BuildUpgradePayload()["approvalPending"] != true {
		t.Fatal("expected approvalPending=true after overwriting SHA-mismatched approval")
	}

	// 4. Failed candidate surfaces lastCheckError and state="failed"
	failedRaw, _ := json.Marshal(map[string]any{
		"candidate": map[string]any{
			"id": "agy-1.5.11-bad", "kind": "agy", "version": "1.5.11", "sha256": sha,
			"state": "failed", "message": "G3 ACP contract: missing model option",
		},
		"gates": []map[string]any{{"name": "G3 ACP 契约", "ok": false, "detail": "missing model option"}},
	})
	if err := os.WriteFile(filepath.Join(dir, "status.json"), failedRaw, 0o644); err != nil {
		t.Fatal(err)
	}
	pf := BuildUpgradePayload()
	if pf["approvalPending"] != false || pf["lastCheckError"] != "G3 ACP contract: missing model option" {
		t.Fatalf("expected lastCheckError from failed candidate, got %+v", pf)
	}
	if got := CurrentUpgradeMetricState(); got != "failed" {
		t.Fatalf("expected metric state=failed, got %q", got)
	}
}

func TestAggregateUpgradeEngineInfoMultiAccountPrimaryDisabled(t *testing.T) {
	cur := "/data/agy/1.5.16-8996aedf/agy-acp-900k"
	old := "/data/agy/1.5.14-374676c3/agy-acp-900k"
	selfStopped := map[string]any{
		"configured": "/data/agy/current/agy-acp-900k",
		"alive":      false,
		"current":    cur,
	}

	// 1. Primary disabled (not running), active child worker alive on new 1.5.16 binary -> reports running=1.5.16, alive=true.
	eng1, pend1 := aggregateUpgradeEngineInfo(selfStopped, false, false, []peerUpgradeEngineReport{
		{ID: "uspro", Alive: false},
		{ID: "acc4", Alive: true, Running: cur, StartedAt: "2026-11-24T11:00:00Z"},
	})
	if eng1["alive"] != true || eng1["running"] != cur || pend1 {
		t.Fatalf("expected active child worker on new binary to satisfy /api/upgrade, got eng=%+v pend=%v", eng1, pend1)
	}

	// 2. One child worker still alive on old 1.5.14 binary -> reports old running path + alive=true + pendingSwitch=true so upgrader waits.
	eng2, pend2 := aggregateUpgradeEngineInfo(selfStopped, false, false, []peerUpgradeEngineReport{
		{ID: "acc3", Alive: true, Running: old, StartedAt: "2026-11-24T10:00:00Z", PendingSwitch: true},
		{ID: "acc4", Alive: true, Running: cur, StartedAt: "2026-11-24T11:00:00Z"},
	})
	if eng2["alive"] != true || eng2["running"] != old || !pend2 {
		t.Fatalf("expected old alive child worker to keep pendingSwitch=true and running=%s, got eng=%+v pend=%v", old, eng2, pend2)
	}

	// 3. All engines stopped/idle -> alive=false so upgrader proceeds directly to flash_probe without waiting.
	eng3, pend3 := aggregateUpgradeEngineInfo(selfStopped, false, false, []peerUpgradeEngineReport{
		{ID: "acc4", Alive: false, Running: old, StartedAt: "2026-11-24T10:00:00Z"},
	})
	if eng3["alive"] != false || pend3 {
		t.Fatalf("expected alive=false when all workers idle, got eng=%+v pend=%v", eng3, pend3)
	}
}

func TestSyncWaitingIdleEngineSignalWritesOnceAndThrottles(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("AGY_UPGRADE_DIR", dir)
	resetWaitingIdleSignalForTest()
	t.Cleanup(func() {
		resetWaitingIdleSignalForTest()
		initConfig()
	})
	initConfig()
	GlobalEngine.StopEngine()

	// 1. Not in waiting_idle -> must NOT create engine.json.
	writeUpgradeStatus(t, dir, map[string]any{
		"id": "agy-1.5.16-test", "kind": "agy", "version": "1.5.16",
		"sha256": strings.Repeat("ab", 32), "state": "ready", "installed": "1.5.16-8996aedf",
	})
	syncWaitingIdleEngineSignal()
	enginePath := filepath.Join(dir, "engine.json")
	if _, err := os.Stat(enginePath); !os.IsNotExist(err) {
		t.Fatalf("expected no engine.json when state=ready, got err=%v", err)
	}

	// 2. In waiting_idle with idle engine (alive=false, pendingSwitch=false) -> writes engine.json once.
	writeUpgradeStatus(t, dir, map[string]any{
		"id": "agy-1.5.16-test", "kind": "agy", "version": "1.5.16",
		"sha256": strings.Repeat("ab", 32), "state": "waiting_idle", "installed": "1.5.16-8996aedf",
	})
	syncWaitingIdleEngineSignal()
	info1, err := os.Stat(enginePath)
	if err != nil {
		t.Fatalf("expected engine.json to be written when waiting_idle is ready: %v", err)
	}

	// 3. Immediate second call within throttle window must NOT rewrite engine.json.
	oldTime := time.Now().Add(-10 * time.Minute)
	if err := os.Chtimes(enginePath, oldTime, oldTime); err != nil {
		t.Fatal(err)
	}
	syncWaitingIdleEngineSignal()
	info2, err := os.Stat(enginePath)
	if err != nil {
		t.Fatal(err)
	}
	if !info2.ModTime().Equal(info1.ModTime()) && info2.ModTime().After(oldTime.Add(time.Second)) {
		t.Fatalf("expected throttled second syncWaitingIdleEngineSignal not to rewrite engine.json")
	}
}

func TestUpgradeVerifyDeferredBackoffAndPayloadPreservation(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("AGY_UPGRADE_DIR", dir)
	resetWaitingIdleSignalForTest()
	t.Cleanup(func() {
		resetWaitingIdleSignalForTest()
		initConfig()
	})
	initConfig()
	GlobalEngine.StopEngine()

	futureNotBefore := time.Now().Add(15 * time.Minute).UTC().Format(time.RFC3339)
	writeUpgradeStatus(t, dir, map[string]any{
		"id":              "agy-1.5.18-def",
		"kind":            "agy",
		"version":         "1.5.18",
		"sha256":          strings.Repeat("cd", 32),
		"state":           "verify_deferred",
		"installed":       "1.5.18-85968a60",
		"message":         "切换后验证因号池额度不足暂缓 (1/3)",
		"verifyDeferrals": 1,
		"verifyNotBefore": futureNotBefore,
	})

	// Typed struct + /api/upgrade payload + Prometheus metric must preserve verify_deferred fields.
	_, st, err := readUpgradeStatusRaw()
	if err != nil || st == nil || st.Candidate == nil {
		t.Fatalf("readUpgradeStatusRaw failed: %v", err)
	}
	if st.Candidate.State != "verify_deferred" || st.Candidate.VerifyDeferrals != 1 || st.Candidate.VerifyNotBefore != futureNotBefore {
		t.Fatalf("expected verify_deferred metadata preserved on upgradeCandidate, got %+v", st.Candidate)
	}
	if got := CurrentUpgradeMetricState(); got != "verify_deferred" {
		t.Fatalf("expected Prometheus metric state verify_deferred, got %q", got)
	}
	payload := BuildUpgradePayload()
	rawStatus, _ := payload["status"].(map[string]any)
	rawCand, _ := rawStatus["candidate"].(map[string]any)
	if rawCand["verifyNotBefore"] != futureNotBefore || rawCand["verifyDeferrals"] != float64(1) {
		t.Fatalf("expected /api/upgrade payload to preserve verifyDeferrals & verifyNotBefore, got %+v", rawCand)
	}

	// 1. While now < verifyNotBefore: must NOT write engine.json.
	enginePath := filepath.Join(dir, "engine.json")
	syncWaitingIdleEngineSignal()
	if _, err := os.Stat(enginePath); !os.IsNotExist(err) {
		t.Fatalf("must NOT write engine.json while now < verifyNotBefore, err=%v", err)
	}

	// 2. Once now >= verifyNotBefore: writes engine.json once and throttles for >= 5m.
	pastNotBefore := time.Now().Add(-30 * time.Second).UTC().Format(time.RFC3339)
	writeUpgradeStatus(t, dir, map[string]any{
		"id":              "agy-1.5.18-def",
		"kind":            "agy",
		"version":         "1.5.18",
		"sha256":          strings.Repeat("cd", 32),
		"state":           "verify_deferred",
		"installed":       "1.5.18-85968a60",
		"message":         "切换后验证因号池额度不足暂缓 (1/3)",
		"verifyDeferrals": 1,
		"verifyNotBefore": pastNotBefore,
	})
	syncWaitingIdleEngineSignal()
	if _, err := os.Stat(enginePath); err != nil {
		t.Fatalf("expected engine.json written once verifyNotBefore expired: %v", err)
	}

	// Simulate 2 minutes elapsed (< 5m verifyDeferredSignalWindow): must remain throttled!
	oldTime := time.Now().Add(-10 * time.Minute)
	if err := os.Chtimes(enginePath, oldTime, oldTime); err != nil {
		t.Fatal(err)
	}
	waitingIdleSignalMu.Lock()
	lastWaitingIdleSigAt = time.Now().Add(-2 * time.Minute)
	waitingIdleSignalMu.Unlock()
	syncWaitingIdleEngineSignal()
	infoAfter2m, err := os.Stat(enginePath)
	if err != nil {
		t.Fatal(err)
	}
	if infoAfter2m.ModTime().After(oldTime.Add(time.Second)) {
		t.Fatalf("verify_deferred must throttle >=5m, but rewrote engine.json after 2m")
	}

	// Simulate 6 minutes elapsed (>= 5m): must wake agy-upgrader-apply.path again.
	waitingIdleSignalMu.Lock()
	lastWaitingIdleSigAt = time.Now().Add(-6 * time.Minute)
	waitingIdleSignalMu.Unlock()
	syncWaitingIdleEngineSignal()
	infoAfter6m, err := os.Stat(enginePath)
	if err != nil {
		t.Fatal(err)
	}
	if !infoAfter6m.ModTime().After(oldTime.Add(time.Second)) {
		t.Fatalf("expected verify_deferred to rewrite engine.json after >=5m throttle elapsed")
	}
}
