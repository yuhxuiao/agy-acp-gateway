package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseAntigravityCodeAuthURL(t *testing.T) {
	valid := "https://accounts.google.com/o/oauth2/auth?access_type=offline&client_id=1071006060591&prompt=consent&redirect_uri=https%3A%2F%2Fantigravity.google%2Foauth-callback&response_type=code&scope=openid&state=abc_XYZ-123"
	gotURL, redir, st, err := parseAntigravityCodeAuthURL(valid)
	if err != nil {
		t.Fatalf("expected valid URL to pass, got %v", err)
	}
	if gotURL != valid || redir != antigravityCodeRedirectURI || st != "abc_XYZ-123" {
		t.Fatalf("unexpected parsed result: url=%q redir=%q state=%q", gotURL, redir, st)
	}

	validV2 := "https://accounts.google.com/o/oauth2/v2/auth?response_type=code&redirect_uri=https%3A%2F%2Fantigravity.google%2Foauth-callback&state=s2"
	if _, _, st2, err := parseAntigravityCodeAuthURL(validV2); err != nil || st2 != "s2" {
		t.Fatalf("expected v2 auth path to pass, got st=%q err=%v", st2, err)
	}

	badCases := []string{
		"",
		"http://accounts.google.com/o/oauth2/auth?response_type=code&redirect_uri=https%3A%2F%2Fantigravity.google%2Foauth-callback&state=s",
		"https://evil.example/o/oauth2/auth?response_type=code&redirect_uri=https%3A%2F%2Fantigravity.google%2Foauth-callback&state=s",
		"https://accounts.google.com/other?response_type=code&redirect_uri=https%3A%2F%2Fantigravity.google%2Foauth-callback&state=s",
		"https://accounts.google.com/o/oauth2/auth?response_type=token&redirect_uri=https%3A%2F%2Fantigravity.google%2Foauth-callback&state=s",
		"https://accounts.google.com/o/oauth2/auth?response_type=code&redirect_uri=http%3A%2F%2F127.0.0.1%3A9999%2F&state=s",
		"https://accounts.google.com/o/oauth2/auth?response_type=code&redirect_uri=https%3A%2F%2Fantigravity.google%2Foauth-callback",
		"https://accounts.google.com/o/oauth2/auth?response_type=code&redirect_uri=https%3A%2F%2Fantigravity.google%2Foauth-callback&state=a&state=b",
		"https://accounts.google.com/o/oauth2/auth?response_type=code&redirect_uri=https%3A%2F%2Fantigravity.google%2Foauth-callback&state=s#frag",
		"https://accounts.google.com/o/oauth2/auth?response_type=code&redirect_uri=https%3A%2F%2Fantigravity.google%2Foauth-callback&state=s withspace",
	}
	for _, tc := range badCases {
		if _, _, _, err := parseAntigravityCodeAuthURL(tc); err == nil {
			t.Fatalf("expected invalid OAuth URL to be rejected: %q", tc)
		}
	}
}

func TestValidateAntigravityLoginResponse(t *testing.T) {
	const redir = antigravityCodeRedirectURI
	const st = "state_token_999"

	if got, err := validateAntigravityLoginResponse(redir, st, "  4/0AVGzR1_raw-code_123  "); err != nil || got != "4/0AVGzR1_raw-code_123" {
		t.Fatalf("expected raw code to pass, got %q err=%v", got, err)
	}
	cb := "https://antigravity.google/oauth-callback?state=state_token_999&code=4%2F0AVGzR1_url_code&scope=email"
	if got, err := validateAntigravityLoginResponse(redir, st, cb); err != nil || got != cb {
		t.Fatalf("expected matching callback URL to pass, got %q err=%v", got, err)
	}

	badInputs := []struct {
		in      string
		wantSub string
	}{
		{"   ", "不能为空"},
		{"4/0A\ninjected", "空白或换行"},
		{"https://antigravity.google/oauth-callback?state=wrong_state&code=4/0A", "state"},
		{"https://evil.example/oauth-callback?state=state_token_999&code=4/0A", "不匹配"},
		{"https://antigravity.google/oauth-callback?state=state_token_999&error=access_denied", "access_denied"},
		{"https://antigravity.google/oauth-callback?state=state_token_999", "code"},
		{"https://antigravity.google/oauth-callback?state=state_token_999&code=abc#frag", "#"},
		{"state=state_token_999&code=abc", "完整回调 URL"},
	}
	for _, tc := range badInputs {
		if _, err := validateAntigravityLoginResponse(redir, st, tc.in); err == nil || !strings.Contains(err.Error(), tc.wantSub) {
			t.Fatalf("input %q: expected error containing %q, got %v", tc.in, tc.wantSub, err)
		}
	}
}

func TestAccountGeminiDirMatchesTokenPath(t *testing.T) {
	prevDB := GatewayStateDB
	t.Cleanup(func() {
		GlobalAccountManager = NewAccountManager()
		initConfig()
		InitGlobalStore(prevDB)
	})

	tmp := t.TempDir()
	relPure := filepath.Join(tmp, "sub", "..", "pure-api-home")
	accFile := filepath.Join(tmp, "accounts.json")
	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "gateway.db"))
	t.Setenv("AGY_ACCOUNTS_FILE", accFile)
	t.Setenv("AGY_PURE_API_HOME", relPure)
	t.Setenv("AGY_INSTANCE_ID", "primary")
	initConfig()

	GlobalAccountManager = NewAccountManager()
	GlobalAccountManager.Init()
	if _, err := GlobalAccountManager.UpsertAccount(AccountProfile{
		ID:            "acct-2",
		Label:         "Second",
		Enabled:       true,
		ProxyPreset:   "direct",
		AllowedModels: []string{"gemini-4-neon"},
	}); err != nil {
		t.Fatal(err)
	}

	primGem := GlobalAccountManager.AccountGeminiDir("primary")
	wantPrimGem, _ := filepath.Abs(PureApiHome)
	if primGem != wantPrimGem || !filepath.IsAbs(primGem) {
		t.Fatalf("primary AccountGeminiDir = %q, want %q", primGem, wantPrimGem)
	}
	if got := GlobalAccountManager.AccountTokenPath("primary"); got != filepath.Join(primGem, "antigravity-cli", "antigravity-oauth-token") {
		t.Fatalf("primary token path mismatch: %q", got)
	}

	secGem := GlobalAccountManager.AccountGeminiDir("acct-2")
	wantSecGem := filepath.Join(tmp, "accounts", "acct-2", ".gemini")
	if secGem != wantSecGem || !filepath.IsAbs(secGem) {
		t.Fatalf("acct-2 AccountGeminiDir = %q, want %q", secGem, wantSecGem)
	}
	if got := GlobalAccountManager.AccountTokenPath("acct-2"); got != filepath.Join(secGem, "antigravity-cli", "antigravity-oauth-token") {
		t.Fatalf("acct-2 token path mismatch: %q", got)
	}
}

func writeFakeOAuthAgyScript(t *testing.T, dir, invLogPath string, sleepLoginOnly bool) string {
	t.Helper()
	scriptPath := filepath.Join(dir, "fake-agy.py")
	py := fmt.Sprintf(`#!/usr/bin/env python3
import json, os, signal, sys, time

INV_LOG = %q
SLEEP_ONLY = %v

def log_inv(kind, extra=None):
    rec = {
        "kind": kind,
        "argv": sys.argv[1:],
        "cwd": os.getcwd(),
        "env": {
            "HOME": os.environ.get("HOME", ""),
            "GEMINI_HOME": os.environ.get("GEMINI_HOME", ""),
            "AGY_ACP_FORCE_FILE_STORAGE": os.environ.get("AGY_ACP_FORCE_FILE_STORAGE", ""),
            "AGY_CLI_DISABLE_AUTO_UPDATE": os.environ.get("AGY_CLI_DISABLE_AUTO_UPDATE", ""),
            "SSH_CONNECTION": os.environ.get("SSH_CONNECTION", ""),
            "DISPLAY": os.environ.get("DISPLAY", ""),
            "BROWSER": os.environ.get("BROWSER", ""),
            "HTTP_PROXY": os.environ.get("HTTP_PROXY", ""),
        },
        "extra": extra,
    }
    with open(INV_LOG, "a") as f:
        f.write(json.dumps(rec) + "\n")

args = sys.argv[1:]
gemini_dir = ""
for i, a in enumerate(args):
    if a == "--gemini-dir" and i + 1 < len(args):
        gemini_dir = args[i + 1]
tok_path = os.path.join(gemini_dir, "antigravity-cli", "antigravity-oauth-token")

if "--login" in args:
    log_inv("login")
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    print("Open this link in a browser to log in with Google:", flush=True)
    print("", flush=True)
    print("  https://accounts.google.com/o/oauth2/auth?access_type=offline&client_id=1071006060591&prompt=consent&redirect_uri=https%%3A%%2F%%2Fantigravity.google%%2Foauth-callback&response_type=code&scope=openid&state=test-state-123", flush=True)
    print("", flush=True)
    print("Then paste the authorization code, or the full URL of the page you land on, and press Enter:", flush=True)
    if SLEEP_ONLY:
        time.sleep(30)
        sys.exit(0)
    line = sys.stdin.readline().strip()
    if "4/0A_valid_code" in line or "4%%2F0A_valid_code" in line:
        os.makedirs(os.path.dirname(tok_path), exist_ok=True)
        with open(tok_path, "w") as f:
            json.dump({
                "token": {"access_token": "ya29.fake_access_token_xyz", "refresh_token": "1//fake_refresh"},
                "auth_method": "oauth-personal",
                "id_token_email": "tester@example.com",
            }, f)
        sys.exit(0)
    print("invalid auth code", file=sys.stderr, flush=True)
    sys.exit(1)

# ACP RPC mode (verify / logout)
for raw in sys.stdin:
    raw = raw.strip()
    if not raw:
        continue
    msg = json.loads(raw)
    mid = msg.get("id")
    method = msg.get("method")
    if method == "initialize":
        log_inv("acp_init")
        print(json.dumps({
            "jsonrpc": "2.0",
            "id": mid,
            "result": {
                "protocolVersion": 1,
                "agentCapabilities": {"auth": {"logout": {}}},
            },
        }), flush=True)
    elif method == "authenticate":
        log_inv("acp_auth")
        if os.path.exists(tok_path):
            print(json.dumps({
                "jsonrpc": "2.0",
                "id": mid,
                "result": {
                    "_meta": {"antigravity": {"tier": {"name": "Google AI Ultra", "id": "g1-ultra-tier"}}},
                },
            }), flush=True)
        else:
            print(json.dumps({
                "jsonrpc": "2.0",
                "id": mid,
                "error": {"code": -32603, "message": "No browser is available here"},
            }), flush=True)
    elif method == "logout":
        log_inv("acp_logout")
        print(json.dumps({"jsonrpc": "2.0", "id": mid, "result": {}}), flush=True)
`, invLogPath, map[bool]string{true: "True", false: "False"}[sleepLoginOnly])
	if err := os.WriteFile(scriptPath, []byte(py), 0o755); err != nil {
		t.Fatal(err)
	}
	return scriptPath
}

func TestT3StyleAcpOAuthLifecycleStartSubmitVerifyLogout(t *testing.T) {
	prevDB := GatewayStateDB
	prevBin := PatchedAgyBin
	prevVerify := oauthVerifyInTest
	t.Cleanup(func() {
		GlobalAccountManager.StopAll()
		GlobalAccountManager = NewAccountManager()
		PatchedAgyBin = prevBin
		oauthVerifyInTest = prevVerify
		resetPeerDashboardCacheForTest()
		initConfig()
		apiKeys = loadAPIKeys()
		InitGlobalStore(prevDB)
	})

	tmp := t.TempDir()
	invLog := filepath.Join(tmp, "invocations.jsonl")
	fakeAgy := writeFakeOAuthAgyScript(t, tmp, invLog, false)

	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "gateway.db"))
	t.Setenv("AGY_ACCOUNTS_FILE", filepath.Join(tmp, "accounts.json"))
	t.Setenv("AGY_PURE_API_HOME", filepath.Join(tmp, "pure-api-home"))
	t.Setenv("AGY_INSTANCE_ID", "primary")
	t.Setenv("AGY_API_KEYS", "master-secret-key")
	t.Setenv("DISPLAY", ":99")
	initConfig()
	PatchedAgyBin = fakeAgy
	oauthVerifyInTest = true
	apiKeys = loadAPIKeys()

	GlobalAccountManager = NewAccountManager()
	GlobalAccountManager.Init()
	if _, err := GlobalAccountManager.UpsertAccount(AccountProfile{
		ID:            "acct-b",
		Label:         "Account B",
		Enabled:       true,
		ProxyPreset:   "direct",
		AllowedModels: []string{"gemini-4-neon"},
	}); err != nil {
		t.Fatal(err)
	}

	gs := NewGatewayServer()
	defer gs.Close()

	doJSON := func(method, path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("x-api-key", "master-secret-key")
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		gs.ServeHTTP(rec, req)
		return rec
	}

	for _, accID := range []string{"primary", "acct-b"} {
		// Plant a legacy acp_token.json to confirm post-login cleanup removes it.
		legacyTok := filepath.Join(filepath.Dir(GlobalAccountManager.AccountTokenPath(accID)), "acp_token.json")
		if err := os.MkdirAll(filepath.Dir(legacyTok), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(legacyTok, []byte(`{"legacy":"stale"}`), 0o600); err != nil {
			t.Fatal(err)
		}

		// 1. Start OAuth flow (must run against isolated staging dir and NOT touch live token path yet)
		recStart := doJSON(http.MethodPost, "/api/accounts/oauth/start", fmt.Sprintf(`{"accountId":%q}`, accID))
		if recStart.Code != http.StatusOK {
			t.Fatalf("%s start failed: code=%d body=%s", accID, recStart.Code, recStart.Body.String())
		}
		var startResp struct {
			OK      bool              `json:"ok"`
			FlowID  string            `json:"flowId"`
			AuthURL string            `json:"authUrl"`
			Phase   string            `json:"phase"`
			Status  OAuthFlowSnapshot `json:"status"`
		}
		if err := json.Unmarshal(recStart.Body.Bytes(), &startResp); err != nil || !startResp.OK || startResp.FlowID == "" || startResp.Phase != "waiting" {
			t.Fatalf("%s unexpected start response: %s (err=%v)", accID, recStart.Body.String(), err)
		}
		if GlobalAccountManager.HasOAuthToken(accID) {
			t.Fatalf("%s expected live token to remain absent during waiting phase before submit", accID)
		}

		// 2. Wrong state on submit returns 400 and preserves the active waiting flow
		recBad := doJSON(http.MethodPost, "/api/accounts/oauth/submit", fmt.Sprintf(
			`{"accountId":%q,"flowId":%q,"code":"https://antigravity.google/oauth-callback?state=wrong&code=4%%2F0A_valid_code"}`,
			accID, startResp.FlowID,
		))
		if recBad.Code != http.StatusBadRequest {
			t.Fatalf("%s expected 400 for mismatched state, got %d body=%s", accID, recBad.Code, recBad.Body.String())
		}
		recSt := doJSON(http.MethodGet, "/api/accounts/oauth/status?accountId="+accID, "")
		if recSt.Code != http.StatusOK || !strings.Contains(recSt.Body.String(), `"phase":"waiting"`) {
			t.Fatalf("%s expected flow to remain waiting after validation error, got %d %s", accID, recSt.Code, recSt.Body.String())
		}

		// 3. Valid callback URL completes staging exchange, verifies via ACP, installs 0600 token, removes legacy acp_token.json
		recSub := doJSON(http.MethodPost, "/api/accounts/oauth/submit", fmt.Sprintf(
			`{"accountId":%q,"flowId":%q,"code":"https://antigravity.google/oauth-callback?state=test-state-123&code=4%%2F0A_valid_code"}`,
			accID, startResp.FlowID,
		))
		if recSub.Code != http.StatusOK {
			t.Fatalf("%s submit failed: code=%d body=%s", accID, recSub.Code, recSub.Body.String())
		}
		var subResp struct {
			OK    bool   `json:"ok"`
			Tier  string `json:"tier"`
			Email string `json:"email"`
		}
		if err := json.Unmarshal(recSub.Body.Bytes(), &subResp); err != nil || !subResp.OK || subResp.Tier != "Google AI Ultra" || subResp.Email != "tester@example.com" {
			t.Fatalf("%s unexpected submit response: %s (err=%v)", accID, recSub.Body.String(), err)
		}
		if fileExists(legacyTok) {
			t.Fatalf("%s expected legacy acp_token.json to be removed after login", accID)
		}
		stInfo, err := os.Stat(GlobalAccountManager.AccountTokenPath(accID))
		if err != nil || stInfo.Mode().Perm() != 0o600 {
			t.Fatalf("%s expected 0600 token file, got mode=%v err=%v", accID, stInfo, err)
		}

		// 4. GET /api/accounts surfaces authenticated + email + oauthPhase
		recList := doJSON(http.MethodGet, "/api/accounts", "")
		if recList.Code != http.StatusOK || !strings.Contains(recList.Body.String(), `"email":"tester@example.com"`) {
			t.Fatalf("%s expected email in /api/accounts, got %d %s", accID, recList.Code, recList.Body.String())
		}

		// 5. Logout calls ACP logout against live GEMINI_HOME, removes token, and updates /api/accounts
		recOut := doJSON(http.MethodPost, "/api/accounts/oauth/logout", fmt.Sprintf(`{"accountId":%q}`, accID))
		if recOut.Code != http.StatusOK {
			t.Fatalf("%s logout failed: code=%d body=%s", accID, recOut.Code, recOut.Body.String())
		}
		if GlobalAccountManager.HasOAuthToken(accID) {
			t.Fatalf("%s expected token deleted after logout", accID)
		}
	}

	// Verify fake-agy saw staging GEMINI_HOME during --login, live GEMINI_HOME during acp_logout,
	// AGY_ACP_FORCE_FILE_STORAGE=1, SSH_CONNECTION, and stripped DISPLAY.
	invRaw, err := os.ReadFile(invLog)
	if err != nil {
		t.Fatal(err)
	}
	invText := string(invRaw)
	if !strings.Contains(invText, `"AGY_ACP_FORCE_FILE_STORAGE": "1"`) ||
		!strings.Contains(invText, `"SSH_CONNECTION": "127.0.0.1 0 127.0.0.1 0"`) ||
		!strings.Contains(invText, `"DISPLAY": ""`) ||
		!strings.Contains(invText, `"--login"`) ||
		!strings.Contains(invText, "agy-oauth-login-") ||
		!strings.Contains(invText, `"acp_logout"`) {
		t.Fatalf("unexpected fake-agy invocation log:\n%s", invText)
	}
	wantPrimGem := GlobalAccountManager.AccountGeminiDir("primary")
	wantAcctBGem := GlobalAccountManager.AccountGeminiDir("acct-b")
	if !strings.Contains(invText, wantPrimGem) || !strings.Contains(invText, wantAcctBGem) {
		t.Fatalf("expected both primary (%q) and acct-b (%q) gemini dirs in invocations:\n%s", wantPrimGem, wantAcctBGem, invText)
	}
}

func TestT3StyleAcpOAuthCancelAndExpiryPreserveLiveToken(t *testing.T) {
	prevDB := GatewayStateDB
	prevBin := PatchedAgyBin
	prevTTL := oauthSessionTTL
	t.Cleanup(func() {
		GlobalAccountManager.StopAll()
		GlobalAccountManager = NewAccountManager()
		PatchedAgyBin = prevBin
		oauthSessionTTL = prevTTL
		resetPeerDashboardCacheForTest()
		initConfig()
		apiKeys = loadAPIKeys()
		InitGlobalStore(prevDB)
	})

	tmp := t.TempDir()
	invLog := filepath.Join(tmp, "invocations.jsonl")
	fakeAgy := writeFakeOAuthAgyScript(t, tmp, invLog, true)

	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "gateway.db"))
	t.Setenv("AGY_ACCOUNTS_FILE", filepath.Join(tmp, "accounts.json"))
	t.Setenv("AGY_PURE_API_HOME", filepath.Join(tmp, "pure-api-home"))
	t.Setenv("AGY_INSTANCE_ID", "primary")
	t.Setenv("AGY_API_KEYS", "master-secret-key")
	initConfig()
	PatchedAgyBin = fakeAgy
	apiKeys = loadAPIKeys()

	GlobalAccountManager = NewAccountManager()
	GlobalAccountManager.Init()

	// Seed an existing live token for primary
	tokPath := GlobalAccountManager.AccountTokenPath("primary")
	if err := os.MkdirAll(filepath.Dir(tokPath), 0o700); err != nil {
		t.Fatal(err)
	}
	origContent := `{"token":{"access_token":"ya29.existing_live_token"},"auth_method":"oauth-personal","id_token_email":"orig@example.com"}` + "\n"
	if err := os.WriteFile(tokPath, []byte(origContent), 0o600); err != nil {
		t.Fatal(err)
	}

	gs := NewGatewayServer()
	defer gs.Close()

	doJSON := func(method, path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("x-api-key", "master-secret-key")
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		gs.ServeHTTP(rec, req)
		return rec
	}

	// Part A: Start then cancel -> live token stays present and untouched throughout, status becomes cancelled
	recStart := doJSON(http.MethodPost, "/api/accounts/oauth/start", `{"accountId":"primary"}`)
	if recStart.Code != http.StatusOK {
		t.Fatalf("start failed: %d %s", recStart.Code, recStart.Body.String())
	}
	var startResp struct {
		FlowID string `json:"flowId"`
	}
	_ = json.Unmarshal(recStart.Body.Bytes(), &startResp)
	if !GlobalAccountManager.HasOAuthToken("primary") || !fileExists(tokPath) || fileExists(tokPath+".oauth-bak") {
		t.Fatalf("expected live token untouched and no .oauth-bak created while login is waiting")
	}

	recCancel := doJSON(http.MethodPost, "/api/accounts/oauth/cancel", fmt.Sprintf(`{"accountId":"primary","flowId":%q}`, startResp.FlowID))
	if recCancel.Code != http.StatusOK || !strings.Contains(recCancel.Body.String(), `"phase":"cancelled"`) {
		t.Fatalf("cancel failed: %d %s", recCancel.Code, recCancel.Body.String())
	}
	restored, err := os.ReadFile(tokPath)
	if err != nil || string(restored) != origContent {
		t.Fatalf("expected live token unchanged after cancel, got err=%v content=%q", err, string(restored))
	}

	// Part B: Timeout expiry -> live token remains untouched, status transitions to failed
	oauthSessionTTL = 150 * time.Millisecond
	recExpStart := doJSON(http.MethodPost, "/api/accounts/oauth/start", `{"accountId":"primary"}`)
	if recExpStart.Code != http.StatusOK {
		t.Fatalf("expiry start failed: %d %s", recExpStart.Code, recExpStart.Body.String())
	}
	deadline := time.Now().Add(3 * time.Second)
	var expiredOK bool
	for time.Now().Before(deadline) {
		recSt := doJSON(http.MethodGet, "/api/accounts/oauth/status?accountId=primary", "")
		if strings.Contains(recSt.Body.String(), `"phase":"failed"`) && fileExists(tokPath) {
			expiredOK = true
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	if !expiredOK {
		t.Fatalf("expected expired OAuth flow to transition to failed while preserving live token")
	}
	restored2, err := os.ReadFile(tokPath)
	if err != nil || string(restored2) != origContent {
		t.Fatalf("expected live token unchanged after expiry, got err=%v content=%q", err, string(restored2))
	}
}
