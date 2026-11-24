package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	gwcfg "agy-acp-gateway-go/internal/config"
)

func setupAuthSplitTestEnv(t *testing.T) (am *AccountManager, gs *GatewayServer, tmpDir string) {
	t.Helper()
	am, _ = setupRound3AccountTestEnv(t)
	tmpDir = t.TempDir()
	ckFile := filepath.Join(tmpDir, "client_keys.json")
	t.Setenv("AGY_CLIENT_KEYS_FILE", ckFile)
	GlobalClientKeys.ResetForTest()
	t.Cleanup(func() {
		GlobalClientKeys.ResetForTest()
		initConfig()
		apiKeys = loadAPIKeys()
	})
	gs = NewGatewayServerWithDeps(newMockAcpEngine(), &mockSessionStore{})
	t.Cleanup(func() {
		gs.Close()
	})
	return am, gs, tmpDir
}

func TestAuthSplit_DashboardPasswordIsolatesAdminAndDataPlanes(t *testing.T) {
	_, gs, tmpDir := setupAuthSplitTestEnv(t)
	gwOnly := gs.GatewayOnlyHandler()
	dashOnly := gs.DashboardOnlyHandler()

	adminPass := "adm-CorrectHorseBatteryStaple-99"
	masterKey := "sk-master-dataplane-only-77"
	t.Setenv("AGY_DASHBOARD_PASSWORD", adminPass)
	t.Setenv("AGY_API_KEYS", masterKey)
	initConfig()

	if got := dashboardAuthMode(); got != "dashboard-password" {
		t.Fatalf("expected dashboardAuthMode=dashboard-password, got %q", got)
	}
	if !dashboardAuthEnabled() || !dataPlaneAuthEnabled() {
		t.Fatal("expected both dashboard and data-plane auth enabled")
	}

	// 1. Admin password unlocks :18891 (via x-api-key, Bearer, and HTTP Basic), while master API key is strictly 401.
	for _, adminPath := range []string{"/", "/dashboard", "/api/dashboard?local_only=1", "/api/accounts", "/api/client-keys", "/api/upgrade", "/metrics", "/health"} {
		reqMaster := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:18891"+adminPath, nil)
		reqMaster.Host = "127.0.0.1:18891"
		reqMaster.Header.Set("x-api-key", masterKey)
		recMaster := httptest.NewRecorder()
		dashOnly.ServeHTTP(recMaster, reqMaster)
		if recMaster.Code != http.StatusUnauthorized {
			t.Fatalf("expected master API key to get 401 on :18891 %s when AGY_DASHBOARD_PASSWORD is set, got %d", adminPath, recMaster.Code)
		}

		reqAdmin := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:18891"+adminPath, nil)
		reqAdmin.Host = "127.0.0.1:18891"
		reqAdmin.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("admin:"+adminPass)))
		recAdmin := httptest.NewRecorder()
		dashOnly.ServeHTTP(recAdmin, reqAdmin)
		if recAdmin.Code != http.StatusOK {
			t.Fatalf("expected AGY_DASHBOARD_PASSWORD Basic auth to get 200 on :18891 %s, got %d: %s", adminPath, recAdmin.Code, recAdmin.Body.String())
		}
	}

	// 2. Admin password is NOT accepted on :18890 data plane; master API key works on :18890.
	reqDataWithAdmin := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:18890/v1/models", nil)
	reqDataWithAdmin.Host = "127.0.0.1:18890"
	reqDataWithAdmin.Header.Set("Authorization", "Bearer "+adminPass)
	recDataAdmin := httptest.NewRecorder()
	gwOnly.ServeHTTP(recDataAdmin, reqDataWithAdmin)
	if recDataAdmin.Code != http.StatusUnauthorized {
		t.Fatalf("expected AGY_DASHBOARD_PASSWORD to be rejected with 401 on :18890 /v1/models, got %d", recDataAdmin.Code)
	}

	reqDataMaster := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:18890/v1/models", nil)
	reqDataMaster.Host = "127.0.0.1:18890"
	reqDataMaster.Header.Set("Authorization", "Bearer "+masterKey)
	recDataMaster := httptest.NewRecorder()
	gwOnly.ServeHTTP(recDataMaster, reqDataMaster)
	if recDataMaster.Code != http.StatusOK {
		t.Fatalf("expected master API key to get 200 on :18890 /v1/models, got %d", recDataMaster.Code)
	}

	// 3. sha256:<hex> digest and AGY_DASHBOARD_PASSWORD_FILE support + fail-closed on malformed/unreadable file.
	filePass := "file-admin-pass-2026"
	hashPass := "hash-only-admin-pass-2026"
	sum := sha256.Sum256([]byte(hashPass))
	pwFile := filepath.Join(tmpDir, "dash_pw.txt")
	if err := os.WriteFile(pwFile, []byte("# comment\n"+filePass+"\nsha256:"+hex.EncodeToString(sum[:])+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGY_DASHBOARD_PASSWORD", "")
	t.Setenv("AGY_DASHBOARD_PASSWORD_FILE", pwFile)
	initConfig()

	for _, okPw := range []string{filePass, hashPass} {
		req := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:18891/api/client-keys", nil)
		req.Host = "127.0.0.1:18891"
		req.Header.Set("x-api-key", okPw)
		rec := httptest.NewRecorder()
		dashOnly.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected password from AGY_DASHBOARD_PASSWORD_FILE to succeed, got %d", rec.Code)
		}
	}

	// Malformed sha256 digest fails closed even with otherwise valid password.
	t.Setenv("AGY_DASHBOARD_PASSWORD", "sha256:not-valid-hex")
	initConfig()
	reqFailClosed := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:18891/api/client-keys", nil)
	reqFailClosed.Host = "127.0.0.1:18891"
	reqFailClosed.Header.Set("x-api-key", filePass)
	recFailClosed := httptest.NewRecorder()
	dashOnly.ServeHTTP(recFailClosed, reqFailClosed)
	if recFailClosed.Code != http.StatusUnauthorized {
		t.Fatalf("expected malformed AGY_DASHBOARD_PASSWORD digest to fail closed (401), got %d", recFailClosed.Code)
	}
}

func TestAuthSplit_LegacyFallbackAndUnauthenticatedIssuanceGuard(t *testing.T) {
	_, gs, _ := setupAuthSplitTestEnv(t)
	dashOnly := gs.DashboardOnlyHandler()

	// 1. Legacy fallback: no AGY_DASHBOARD_PASSWORD, AGY_API_KEYS set -> master key works on :18891.
	t.Setenv("AGY_DASHBOARD_PASSWORD", "")
	t.Setenv("AGY_DASHBOARD_PASSWORD_FILE", "")
	t.Setenv("AGY_API_KEYS", "legacy-master-key-123")
	initConfig()

	if got := dashboardAuthMode(); got != "legacy-master-key" {
		t.Fatalf("expected legacy-master-key mode, got %q", got)
	}
	reqLegacy := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:18891/api/client-keys", nil)
	reqLegacy.Host = "127.0.0.1:18891"
	reqLegacy.Header.Set("x-api-key", "legacy-master-key-123")
	recLegacy := httptest.NewRecorder()
	dashOnly.ServeHTTP(recLegacy, reqLegacy)
	if recLegacy.Code != http.StatusOK {
		t.Fatalf("expected legacy master key to access :18891 when AGY_DASHBOARD_PASSWORD unset, got %d", recLegacy.Code)
	}

	// 2. Completely unauthenticated mode: POST /api/client-keys refuses to mint keys until admin auth is configured.
	t.Setenv("AGY_API_KEYS", "")
	initConfig()
	if got := dashboardAuthMode(); got != "disabled" {
		t.Fatalf("expected disabled mode, got %q", got)
	}
	reqMintOpen := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18891/api/client-keys", strings.NewReader(`{"name":"should-fail"}`))
	reqMintOpen.Host = "127.0.0.1:18891"
	reqMintOpen.Header.Set("Content-Type", "application/json")
	reqMintOpen.Header.Set("X-Agy-Requested-With", "dashboard")
	reqMintOpen.Header.Set("Origin", "http://127.0.0.1:18891")
	recMintOpen := httptest.NewRecorder()
	dashOnly.ServeHTTP(recMintOpen, reqMintOpen)
	if recMintOpen.Code != http.StatusForbidden {
		t.Fatalf("expected 403 refusing client key issuance when dashboard auth is disabled, got %d: %s", recMintOpen.Code, recMintOpen.Body.String())
	}
}

func TestClientKeys_LifecycleIsolationScopesAllowlistsAndChildForwarding(t *testing.T) {
	am, gs, tmpDir := setupAuthSplitTestEnv(t)
	gwOnly := gs.GatewayOnlyHandler()
	dashOnly := gs.DashboardOnlyHandler()

	// Configure ONLY AGY_DASHBOARD_PASSWORD (zero static AGY_API_KEYS!).
	adminPass := "only-dashboard-admin-secret"
	t.Setenv("AGY_DASHBOARD_PASSWORD", adminPass)
	t.Setenv("AGY_API_KEYS", "")
	initConfig()

	if dataPlaneAuthEnabled() {
		t.Fatal("expected dataPlaneAuthEnabled=false before any client key or AGY_API_KEYS is configured")
	}

	// Create group "vip-grp" containing acct-b
	accB, _ := am.GetAccount("acct-b")
	accB.Groups = []string{"vip-grp"}
	if _, err := am.UpsertAccount(accB); err != nil {
		t.Fatalf("upsert acct-b: %v", err)
	}
	if _, err := am.UpsertGroup(AccountGroup{
		ID:            "vip-grp",
		Label:         "VIP Group",
		Strategy:      RouteStrategyFillFirst,
		AllowedModels: []string{"gemini-3.8-flash", "gemini-4-neon"},
	}); err != nil {
		t.Fatalf("upsert vip-grp: %v", err)
	}

	// Mount mock child worker on acct-b to verify loopback auth header replacement & URL key scrubbing.
	var childHits atomic.Int32
	var seenChildAuthHeader atomic.Value
	var seenChildRawQuery atomic.Value
	var seenChildPath atomic.Value
	mountMockChildWorker(t, am, "acct-b", func(w http.ResponseWriter, r *http.Request) {
		childHits.Add(1)
		seenChildAuthHeader.Store(r.Header.Get("x-api-key") + "|" + r.Header.Get("Authorization") + "|" + r.Header.Get("x-goog-api-key"))
		seenChildRawQuery.Store(r.URL.RawQuery)
		seenChildPath.Store(r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"candidates":[{"content":{"role":"model","parts":[{"text":"OK_FROM_CHILD"}]}}]}`))
	})

	// CSRF / Origin guard on POST /api/client-keys
	for _, badCase := range []struct {
		name   string
		xr     string
		origin string
	}{
		{"missing-csrf-header", "", "http://127.0.0.1:18891"},
		{"cross-origin", "dashboard", "https://evil.example"},
	} {
		req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18891/api/client-keys", strings.NewReader(`{"name":"csrf-test"}`))
		req.Host = "127.0.0.1:18891"
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("x-api-key", adminPass)
		if badCase.xr != "" {
			req.Header.Set("X-Agy-Requested-With", badCase.xr)
		}
		if badCase.origin != "" {
			req.Header.Set("Origin", badCase.origin)
		}
		rec := httptest.NewRecorder()
		dashOnly.ServeHTTP(rec, req)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("%s: expected 403 from CSRF/Origin guard, got %d", badCase.name, rec.Code)
		}
	}

	dashJSON := func(method, path string, body any) (int, map[string]any) {
		var rdr *bytes.Reader
		if body != nil {
			b, _ := json.Marshal(body)
			rdr = bytes.NewReader(b)
		} else {
			rdr = bytes.NewReader(nil)
		}
		req := httptest.NewRequest(method, "http://127.0.0.1:18891"+path, rdr)
		req.Host = "127.0.0.1:18891"
		req.Header.Set("x-api-key", adminPass)
		req.Header.Set("X-Agy-Requested-With", "dashboard")
		req.Header.Set("Origin", "http://127.0.0.1:18891")
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		rec := httptest.NewRecorder()
		dashOnly.ServeHTTP(rec, req)
		var out map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
		return rec.Code, out
	}

	// 1. Issue a global client key restricted to gemini-3.8-flash
	code, createdGlobal := dashJSON(http.MethodPost, "/api/client-keys", map[string]any{
		"name":          "laptop-client",
		"notes":         "restricted flash key",
		"scope":         "global",
		"allowedModels": []string{"gemini-3.8-flash"},
	})
	if code != http.StatusOK || createdGlobal["ok"] != true {
		t.Fatalf("create global client key failed (%d): %+v", code, createdGlobal)
	}
	globalSecret, _ := createdGlobal["secret"].(string)
	globalKeyObj, _ := createdGlobal["key"].(map[string]any)
	globalID, _ := globalKeyObj["id"].(string)
	if !strings.HasPrefix(globalSecret, ClientKeyPrefix) || globalID == "" {
		t.Fatalf("unexpected secret=%q id=%q", globalSecret, globalID)
	}
	if _, leakedHash := globalKeyObj["hash"]; leakedHash {
		t.Fatalf("PublicView must never expose hash: %+v", globalKeyObj)
	}

	// Issuing 1 client key immediately turns data-plane auth ON even with AGY_API_KEYS=""!
	if !dataPlaneAuthEnabled() {
		t.Fatal("expected dataPlaneAuthEnabled=true after issuing first client key")
	}
	reqAnon := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:18890/v1/models", nil)
	reqAnon.Host = "127.0.0.1:18890"
	recAnon := httptest.NewRecorder()
	gwOnly.ServeHTTP(recAnon, reqAnon)
	if recAnon.Code != http.StatusUnauthorized {
		t.Fatalf("expected anonymous :18890 request to get 401 once a client key exists, got %d", recAnon.Code)
	}

	// Verify persisted file has 0600 permissions and NEVER contains the plaintext secret.
	ckBytes, err := os.ReadFile(filepath.Join(tmpDir, "client_keys.json"))
	if err != nil {
		t.Fatalf("read client_keys.json: %v", err)
	}
	if strings.Contains(string(ckBytes), globalSecret) {
		t.Fatal("SECURITY VIOLATION: client_keys.json persisted plaintext secret!")
	}
	if st, err := os.Stat(filepath.Join(tmpDir, "client_keys.json")); err != nil || st.Mode().Perm() != 0o600 {
		t.Fatalf("expected 0600 permissions on client_keys.json, got %v (err=%v)", st.Mode().Perm(), err)
	}

	// 2. Client key MUST strictly get 401 on ALL :18891 admin endpoints (and even if dashboard auth were disabled!)
	for _, adminPath := range []string{"/", "/api/dashboard?local_only=1", "/api/accounts", "/api/client-keys", "/api/upgrade", "/metrics"} {
		req := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:18891"+adminPath, nil)
		req.Host = "127.0.0.1:18891"
		req.Header.Set("Authorization", "Bearer "+globalSecret)
		rec := httptest.NewRecorder()
		dashOnly.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("SECURITY VIOLATION: client key accessed :18891 %s (status %d)", adminPath, rec.Code)
		}
	}
	// Also test open-dashboard edge case: even if AGY_DASHBOARD_PASSWORD is cleared, presenting a client key to :18891 still returns 401.
	t.Setenv("AGY_DASHBOARD_PASSWORD", "")
	initConfig()
	reqOpenDashWithCK := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:18891/api/dashboard?local_only=1", nil)
	reqOpenDashWithCK.Host = "127.0.0.1:18891"
	reqOpenDashWithCK.Header.Set("Authorization", "Bearer "+globalSecret)
	recOpenDashWithCK := httptest.NewRecorder()
	dashOnly.ServeHTTP(recOpenDashWithCK, reqOpenDashWithCK)
	if recOpenDashWithCK.Code != http.StatusUnauthorized {
		t.Fatalf("expected client key to be rejected with 401 on :18891 even when dashboard auth is disabled, got %d", recOpenDashWithCK.Code)
	}
	t.Setenv("AGY_DASHBOARD_PASSWORD", adminPass)
	initConfig()

	// 3. Global client key model allowlist filtering & enforcement on /v1/* and /v1beta/*
	reqModels := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:18890/v1/models", nil)
	reqModels.Host = "127.0.0.1:18890"
	reqModels.Header.Set("Authorization", "Bearer "+globalSecret)
	recModels := httptest.NewRecorder()
	gwOnly.ServeHTTP(recModels, reqModels)
	if recModels.Code != http.StatusOK {
		t.Fatalf("expected 200 on /v1/models with client key, got %d", recModels.Code)
	}
	if !strings.Contains(recModels.Body.String(), `"gemini-3.8-flash"`) || strings.Contains(recModels.Body.String(), `"gemini-4-carbon"`) {
		t.Fatalf("expected /v1/models filtered strictly to gemini-3.8-flash, got %s", recModels.Body.String())
	}

	// Disallowed model on POST /v1/messages -> 403 model_not_allowed
	reqDenyMsg := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/messages",
		strings.NewReader(`{"model":"gemini-4-carbon","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`))
	reqDenyMsg.Host = "127.0.0.1:18890"
	reqDenyMsg.Header.Set("Content-Type", "application/json")
	reqDenyMsg.Header.Set("x-api-key", globalSecret)
	recDenyMsg := httptest.NewRecorder()
	gwOnly.ServeHTTP(recDenyMsg, reqDenyMsg)
	if recDenyMsg.Code != http.StatusForbidden || !strings.Contains(recDenyMsg.Body.String(), "model_not_allowed") {
		t.Fatalf("expected 403 model_not_allowed for disallowed model, got %d: %s", recDenyMsg.Code, recDenyMsg.Body.String())
	}

	// Disallowed model on GET /v1beta/models/gemini-4-carbon -> 403
	reqDenyGemGet := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:18890/v1beta/models/gemini-4-carbon?key="+globalSecret, nil)
	reqDenyGemGet.Host = "127.0.0.1:18890"
	recDenyGemGet := httptest.NewRecorder()
	gwOnly.ServeHTTP(recDenyGemGet, reqDenyGemGet)
	if recDenyGemGet.Code != http.StatusForbidden {
		t.Fatalf("expected 403 on GET /v1beta/models/gemini-4-carbon for restricted client key, got %d: %s", recDenyGemGet.Code, recDenyGemGet.Body.String())
	}

	// Verify GET /api/client-keys lists lastUsedAt without secret or hash
	codeList, listResp := dashJSON(http.MethodGet, "/api/client-keys", nil)
	if codeList != http.StatusOK || strings.Contains(mustJSON(listResp), globalSecret) {
		t.Fatalf("GET /api/client-keys failed or leaked secret: %d %+v", codeList, listResp)
	}
	listedKeys, _ := listResp["keys"].([]any)
	if len(listedKeys) != 1 {
		t.Fatalf("expected 1 listed client key, got %d", len(listedKeys))
	}
	firstListed, _ := listedKeys[0].(map[string]any)
	if firstListed["lastUsedAt"] == nil || firstListed["lastUsedAt"] == "" {
		t.Fatalf("expected lastUsedAt populated after data-plane call, got %+v", firstListed)
	}

	// 4. Toggle disable -> 401 on :18890 AND data-plane stays locked (never silently reopens to anonymous!)
	codeTog, _ := dashJSON(http.MethodPost, "/api/client-keys/toggle", map[string]any{"id": globalID, "enabled": false})
	if codeTog != http.StatusOK {
		t.Fatalf("toggle disable failed: %d", codeTog)
	}
	reqDisabled := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:18890/v1/models", nil)
	reqDisabled.Host = "127.0.0.1:18890"
	reqDisabled.Header.Set("Authorization", "Bearer "+globalSecret)
	recDisabled := httptest.NewRecorder()
	gwOnly.ServeHTTP(recDisabled, reqDisabled)
	if recDisabled.Code != http.StatusUnauthorized {
		t.Fatalf("expected disabled client key to get 401, got %d", recDisabled.Code)
	}
	recStillLocked := httptest.NewRecorder()
	gwOnly.ServeHTTP(recStillLocked, httptest.NewRequest(http.MethodGet, "http://127.0.0.1:18890/v1/models", nil))
	if recStillLocked.Code != http.StatusUnauthorized {
		t.Fatalf("disabling the only client key must NOT reopen anonymous data-plane access, got %d", recStillLocked.Code)
	}

	// 5. Account-scoped client key -> routes to acct-b child worker using internal childLoopbackKey, blocks X-Agy-Account override
	codeAcc, createdAccKey := dashJSON(http.MethodPost, "/api/client-keys", map[string]any{
		"name":    "acct-b-client",
		"scope":   "account",
		"scopeId": "acct-b",
	})
	if codeAcc != http.StatusOK {
		t.Fatalf("create account-scoped key failed: %d %+v", codeAcc, createdAccKey)
	}
	accSecret, _ := createdAccKey["secret"].(string)

	reqOverride := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:18890/v1/models", nil)
	reqOverride.Host = "127.0.0.1:18890"
	reqOverride.Header.Set("x-api-key", accSecret)
	reqOverride.Header.Set("X-Agy-Account", "primary")
	recOverride := httptest.NewRecorder()
	gwOnly.ServeHTTP(recOverride, reqOverride)
	if recOverride.Code != http.StatusForbidden {
		t.Fatalf("expected 403 when X-Agy-Account tries to override account-scoped client key, got %d", recOverride.Code)
	}

	reqChild := httptest.NewRequest(http.MethodPost,
		"http://127.0.0.1:18890/v1beta/models/gemini-3.8-flash:generateContent/key="+accSecret+"?key="+accSecret+"&alt=json",
		strings.NewReader(`{"contents":[{"role":"user","parts":[{"text":"ping"}]}]}`))
	reqChild.Host = "127.0.0.1:18890"
	reqChild.Header.Set("Content-Type", "application/json")
	recChild := httptest.NewRecorder()
	gwOnly.ServeHTTP(recChild, reqChild)
	if recChild.Code != http.StatusOK || childHits.Load() != 1 {
		t.Fatalf("expected 200 routed to acct-b child worker, got code=%d hits=%d body=%s", recChild.Code, childHits.Load(), recChild.Body.String())
	}
	gotChildHdr, _ := seenChildAuthHeader.Load().(string)
	gotChildQuery, _ := seenChildRawQuery.Load().(string)
	gotChildPath, _ := seenChildPath.Load().(string)
	if strings.Contains(gotChildHdr, accSecret) || !strings.HasPrefix(gotChildHdr, am.childLoopbackKey+"||") {
		t.Fatalf("expected child worker to receive internal childLoopbackKey only, got %q", gotChildHdr)
	}
	if strings.Contains(gotChildQuery, accSecret) || gotChildQuery != "alt=json" || strings.Contains(gotChildPath, "/key=") {
		t.Fatalf("expected URL key stripped before forwarding to child worker, got path=%q query=%q", gotChildPath, gotChildQuery)
	}

	// 6. Group-scoped client key -> fails closed if group is subsequently deleted
	codeGrp, createdGrpKey := dashJSON(http.MethodPost, "/api/client-keys", map[string]any{
		"name":    "vip-grp-client",
		"scope":   "group",
		"scopeId": "vip-grp",
	})
	if codeGrp != http.StatusOK {
		t.Fatalf("create group-scoped key failed: %d %+v", codeGrp, createdGrpKey)
	}
	grpSecret, _ := createdGrpKey["secret"].(string)
	grpKeyID, _ := createdGrpKey["key"].(map[string]any)["id"].(string)

	if err := am.DeleteGroup("vip-grp"); err != nil {
		t.Fatalf("DeleteGroup failed: %v", err)
	}
	reqDeletedGrp := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/messages",
		strings.NewReader(`{"model":"gemini-3.8-flash","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`))
	reqDeletedGrp.Host = "127.0.0.1:18890"
	reqDeletedGrp.Header.Set("Content-Type", "application/json")
	reqDeletedGrp.Header.Set("Authorization", "Bearer "+grpSecret)
	recDeletedGrp := httptest.NewRecorder()
	gwOnly.ServeHTTP(recDeletedGrp, reqDeletedGrp)
	if recDeletedGrp.Code != http.StatusForbidden {
		t.Fatalf("expected group-scoped client key for deleted group to fail closed with 403, got %d: %s", recDeletedGrp.Code, recDeletedGrp.Body.String())
	}

	// Update group key -> global scope + unrestricted models, then revoke delete
	codeUpd, _ := dashJSON(http.MethodPost, "/api/client-keys/update", map[string]any{
		"id":      grpKeyID,
		"name":    "promoted-global",
		"scope":   "global",
		"enabled": true,
	})
	if codeUpd != http.StatusOK {
		t.Fatalf("update client key failed: %d", codeUpd)
	}
	codeDel, _ := dashJSON(http.MethodDelete, "/api/client-keys?id="+grpKeyID, nil)
	if codeDel != http.StatusOK {
		t.Fatalf("delete client key failed: %d", codeDel)
	}
	reqRevoked := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:18890/v1/models", nil)
	reqRevoked.Host = "127.0.0.1:18890"
	reqRevoked.Header.Set("Authorization", "Bearer "+grpSecret)
	recRevoked := httptest.NewRecorder()
	gwOnly.ServeHTTP(recRevoked, reqRevoked)
	if recRevoked.Code != http.StatusUnauthorized {
		t.Fatalf("expected revoked client key to get 401, got %d", recRevoked.Code)
	}

	// 7. Corrupt client_keys.json fails closed on data plane
	if err := os.WriteFile(filepath.Join(tmpDir, "client_keys.json"), []byte("{corrupt-json"), 0o600); err != nil {
		t.Fatal(err)
	}
	GlobalClientKeys.ResetForTest()
	reqCorrupt := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:18890/v1/models", nil)
	reqCorrupt.Host = "127.0.0.1:18890"
	reqCorrupt.Header.Set("x-api-key", accSecret)
	recCorrupt := httptest.NewRecorder()
	gwOnly.ServeHTTP(recCorrupt, reqCorrupt)
	if recCorrupt.Code != http.StatusUnauthorized {
		t.Fatalf("expected corrupt client_keys.json to fail closed (401), got %d", recCorrupt.Code)
	}
}

func TestAuthSplit_SecretRedactionAndSubprocessEnvHygiene(t *testing.T) {
	redacted := gwcfg.RedactSecretsInString("leaked key agy-ck-AbCdEfGh1234567890 and child agy-child-9988776655443322")
	if strings.Contains(redacted, "AbCdEfGh1234567890") || strings.Contains(redacted, "9988776655443322") {
		t.Fatalf("expected agy-ck-* and agy-child-* tokens to be redacted, got %q", redacted)
	}
	for _, kv := range []string{
		"AGY_DASHBOARD_PASSWORD=super-secret",
		"AGY_DASHBOARD_PASSWORD_FILE=/data/dash_pw.txt",
		"AGY_CLIENT_KEYS_FILE=/data/client_keys.json",
	} {
		if !isStrippedLeafAgySubprocessEnv(kv) || !isStrippedSubprocessEnv(kv) {
			t.Fatalf("expected %q to be stripped from subprocess environments", kv)
		}
	}
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
