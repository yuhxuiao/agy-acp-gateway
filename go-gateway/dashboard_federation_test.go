package main

import (
	"crypto/tls"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"agy-acp-gateway-go/internal/cleanroom"
	gwcfg "agy-acp-gateway-go/internal/config"
)

func TestParseDashboardPeers(t *testing.T) {
	raw := "node-b|Node B (JP)|http://agy-acp-gateway-nodeb:18891/|key123; backup=http://10.0.0.2:18891 , node-b|dup|http://dup:18891"
	peers := gwcfg.ParseDashboardPeers(raw)
	if len(peers) != 2 {
		t.Fatalf("expected 2 deduplicated peers, got %d: %+v", len(peers), peers)
	}
	if peers[0].ID != "node-b" || peers[0].Label != "Node B (JP)" || peers[0].BaseURL != "http://agy-acp-gateway-nodeb:18891" || peers[0].APIKey != "key123" {
		t.Fatalf("unexpected peer[0]: %+v", peers[0])
	}
	if peers[1].ID != "backup" || peers[1].Label != "backup" || peers[1].BaseURL != "http://10.0.0.2:18891" || peers[1].APIKey != "" {
		t.Fatalf("unexpected peer[1]: %+v", peers[1])
	}
}

func TestBuildQuotaViewExtractsArgonPoolWithoutShadowingGeminiModels(t *testing.T) {
	now := time.Date(2026, 11, 19, 10, 40, 0, 0, time.UTC)
	snap := QuotaSnapshot{
		Status:    "ok",
		UpdatedAt: now.Format(time.RFC3339),
		Groups: []QuotaGroup{
			{
				Name: "Gemini 4 Argon",
				Buckets: []QuotaBucket{
					{ID: "argon-wk", Name: "Weekly", Window: "weekly", RemainingFraction: 0.962, UsedFraction: 0.038, ResetTime: "2026-11-26T07:16:04Z"},
				},
			},
			{
				Name: "Gemini Models",
				Buckets: []QuotaBucket{
					{ID: "gem-5h", Name: "5h", Window: "5h", RemainingFraction: 0.25, UsedFraction: 0.75, ResetTime: "2026-11-19T11:57:46Z"},
					{ID: "gem-wk", Name: "Weekly", Window: "weekly", RemainingFraction: 0.643, UsedFraction: 0.357, ResetTime: "2026-11-26T01:57:46Z"},
				},
			},
			{
				Name: "Claude and GPT models",
				Buckets: []QuotaBucket{
					{ID: "3p-5h", Name: "5h", Window: "5h", RemainingFraction: 0.961, UsedFraction: 0.039, ResetTime: "2026-11-19T12:10:12Z"},
					{ID: "3p-wk", Name: "Weekly", Window: "weekly", RemainingFraction: 0.980, UsedFraction: 0.020, ResetTime: "2026-11-26T07:10:12Z"},
				},
			},
		},
	}

	got, _ := buildQuotaView(snap, nil, now)
	if got.PlanTier != "pro" || got.PlanLabel != "AI Pro" {
		t.Fatalf("expected Pro plan detection for 3-group quota with Argon pool, got tier=%q label=%q", got.PlanTier, got.PlanLabel)
	}
	if got.GeminiPool == nil || got.GeminiPool.State != QuotaStateKnown {
		t.Fatalf("expected GeminiPool known, got %+v", got.GeminiPool)
	}
	if got.Deduction == nil || got.Deduction.Cap5hUsd != 10.0 || got.Deduction.CapWeeklyUsd != 35.0 || got.Deduction.PoolRem5hUsd == nil || *got.Deduction.PoolRem5hUsd != 2.5 {
		t.Fatalf("expected Pro Gemini pool caps $10/$35 and 5h rem $2.50 (25%% of $10), got %+v", got.Deduction)
	}
	if got.ThirdPartyPool == nil || got.ThirdPartyPool.Cap5hUsd != 2.5 || got.ThirdPartyPool.CapWeeklyUsd != 5.0 {
		t.Fatalf("expected Pro Claude pool caps $2.50/$5.00, got %+v", got.ThirdPartyPool)
	}
	if got.ArgonPool == nil {
		t.Fatal("expected ArgonPool to be extracted for 3-group quota")
	}
	if got.ArgonPool.GroupName != "Gemini 4 Argon" || got.ArgonPool.Window != "weekly" || got.ArgonPool.RemainingFraction == nil || *got.ArgonPool.RemainingFraction != 0.962 {
		t.Fatalf("unexpected ArgonPool: %+v", got.ArgonPool)
	}

	// Verify 2-group Ultra (20x) snapshot retains $200/$800 Gemini and $25/$50 Claude caps
	ultraSnap := QuotaSnapshot{
		Status:    "ok",
		UpdatedAt: now.Format(time.RFC3339),
		Groups: []QuotaGroup{
			{
				Name:        "Gemini Models",
				Description: "Models within this group: Gemini 4 Neon, Gemini 4 Carbon, Gemini 4 Argon, Gemini 3.8 Flash",
				Buckets: []QuotaBucket{
					{ID: "gemini-5h", Name: "5h", Window: "5h", RemainingFraction: 0.25, UsedFraction: 0.75, ResetTime: "2026-11-19T11:57:46Z"},
					{ID: "gemini-weekly", Name: "Weekly", Window: "weekly", RemainingFraction: 0.50, UsedFraction: 0.50, ResetTime: "2026-11-26T01:57:46Z"},
				},
			},
			{
				Name: "Claude and GPT models",
				Buckets: []QuotaBucket{
					{ID: "3p-5h", Name: "5h", Window: "5h", RemainingFraction: 0.80, UsedFraction: 0.20, ResetTime: "2026-11-19T12:10:12Z"},
					{ID: "3p-weekly", Name: "Weekly", Window: "weekly", RemainingFraction: 0.90, UsedFraction: 0.10, ResetTime: "2026-11-26T07:10:12Z"},
				},
			},
		},
	}
	gotUltra, _ := buildQuotaView(ultraSnap, nil, now)
	if gotUltra.PlanTier != "ultra" || gotUltra.PlanLabel != "Ultra 20x" {
		t.Fatalf("expected Ultra plan detection, got tier=%q label=%q", gotUltra.PlanTier, gotUltra.PlanLabel)
	}
	if gotUltra.Deduction == nil || gotUltra.Deduction.Cap5hUsd != 200.0 || gotUltra.Deduction.CapWeeklyUsd != 800.0 || gotUltra.Deduction.PoolRem5hUsd == nil || *gotUltra.Deduction.PoolRem5hUsd != 50.0 {
		t.Fatalf("expected Ultra Gemini pool caps $200/$800 and 5h rem $50.0, got %+v", gotUltra.Deduction)
	}
	if gotUltra.ThirdPartyPool == nil || gotUltra.ThirdPartyPool.Cap5hUsd != 25.0 || gotUltra.ThirdPartyPool.CapWeeklyUsd != 50.0 {
		t.Fatalf("expected Ultra Claude pool caps $25/$50, got %+v", gotUltra.ThirdPartyPool)
	}
}

func TestMergeWindowAggregatesWeightedMath(t *testing.T) {
	w1 := WindowAggregate{
		StepsCount:   10,
		HitSteps:     9,
		Hits99:       8,
		AvgTtftMs:    2000,
		AvgTps:       60.0,
		AvgDecodeTps: 80.0,
		RawIn:        10000,
		RawCr:        90000,
		AlignedIn:    10000,
		AlignedCr:    90000,
		OutTok:       1000,
		CostUsd:      1.5,
		SavedUsd:     8.5,
		Models: []ModelAggregate{
			{
				ModelID:       "gemini-4-neon",
				ModelLabel:    "Gemini 4 Neon",
				Family:        "gemini",
				Steps:         10,
				RawIn:         10000,
				RawCr:         90000,
				OutTok:        1000,
				CostUsd:       1.5,
				SavedUsd:      8.5,
				AvgTtftMs:     2000,
				AvgTps:        60.0,
				AvgDecodeTps:  80.0,
				SteadySteps:   9,
				SteadyHits:    9,
				SteadyHitRate: 100.0,
				Tiers:         map[string]int{"high": 10},
			},
		},
	}
	w2 := WindowAggregate{
		StepsCount:   10,
		HitSteps:     0,
		Hits99:       0,
		AvgTtftMs:    4000,
		AvgTps:       30.0,
		AvgDecodeTps: 40.0,
		RawIn:        100000,
		RawCr:        0,
		AlignedIn:    100000,
		AlignedCr:    0,
		OutTok:       3000,
		CostUsd:      3.5,
		SavedUsd:     0.0,
		Models: []ModelAggregate{
			{
				ModelID:       "gemini-4-neon",
				ModelLabel:    "Gemini 4 Neon",
				Family:        "gemini",
				Steps:         10,
				RawIn:         100000,
				RawCr:         0,
				OutTok:        3000,
				CostUsd:       3.5,
				SavedUsd:      0.0,
				AvgTtftMs:     4000,
				AvgTps:        30.0,
				AvgDecodeTps:  40.0,
				SteadySteps:   9,
				SteadyHits:    0,
				SteadyHitRate: 0.0,
				Tiers:         map[string]int{"xhigh": 10},
			},
		},
	}

	m := mergeWindowAggregates([]WindowAggregate{w1, w2})
	if m.StepsCount != 20 || m.RawIn != 110000 || m.RawCr != 90000 {
		t.Fatalf("unexpected merged token totals: %+v", m)
	}
	// Exact token hit rate: 90000 / 200000 = 45.00%
	if m.RawHitRate != 45.0 || m.AlignedHitRate != 45.0 {
		t.Fatalf("expected 45.0%% weighted hit rate, got raw=%v aligned=%v", m.RawHitRate, m.AlignedHitRate)
	}
	if m.AvgTtftMs != 3000 {
		t.Fatalf("expected weighted AvgTtftMs=3000, got %d", m.AvgTtftMs)
	}
	// Weighted by OutTok (1000 @ 80 + 3000 @ 40) / 4000 = 50.0
	if m.AvgDecodeTps != 50.0 {
		t.Fatalf("expected weighted AvgDecodeTps=50.0, got %v", m.AvgDecodeTps)
	}
	if len(m.Models) != 1 || m.Models[0].SteadyHitRate != 50.0 || m.Models[0].Tiers["high"] != 10 || m.Models[0].Tiers["xhigh"] != 10 {
		t.Fatalf("unexpected merged model entry: %+v", m.Models)
	}
}

func TestFederatedDashboardEndToEndAndStaleFallback(t *testing.T) {
	resetPeerDashboardCacheForTest()
	t.Cleanup(func() {
		resetPeerDashboardCacheForTest()
		GlobalAccountManager = NewAccountManager()
		initConfig()
	})

	var peerFail atomic.Bool
	var sawFedHopHeader atomic.Bool
	peerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Agy-Federated-Hop") == "1" {
			sawFedHopHeader.Store(true)
		}
		if peerFail.Load() {
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte("upstream down"))
			return
		}
		rem := 0.962
		resp := map[string]any{
			"now": time.Now().UTC().Format(time.RFC3339),
			"gateway": map[string]any{
				"instanceId":           "node-b",
				"instanceLabel":        "Secondary Node B",
				"activeSessionsCount":  2,
				"maxLiveWorkers":       3,
				"maxConcurrentPrompts": 3,
				"tosTripped":           false,
			},
			"quota": QuotaSnapshot{
				Status:    "ok",
				UpdatedAt: time.Now().UTC().Format(time.RFC3339),
				ArgonPool: &ExtraPoolQuota{
					GroupName:         "Gemini 4 Argon",
					State:             QuotaStateKnown,
					Window:            "weekly",
					RemainingFraction: &rem,
				},
			},
			"windows": map[string]WindowAggregate{
				"last24h": {
					StepsCount:   4,
					RawIn:        8000,
					RawCr:        92000,
					AlignedIn:    8000,
					AlignedCr:    92000,
					OutTok:       500,
					CostUsd:      0.42,
					SavedUsd:     1.85,
					AvgTtftMs:    2400,
					AvgDecodeTps: 85.0,
				},
			},
			"liveSessions": []map[string]any{
				{"sessionId": "sess-nodeb-1", "convKey": "sid:nodeb-1", "model": "gemini-4-neon", "status": "IDLE"},
			},
			"gatewayTurns": []map[string]any{
				{"sessionId": "sess-nodeb-1", "convKey": "sid:nodeb-1", "turnIndex": 1, "completedAtMs": int64(2000), "model": "gemini-4-neon"},
			},
			"recentDbSteps": []map[string]any{
				{"sessionId": "sess-nodeb-1", "stepIdx": 1, "ts": "2026-11-19T10:35:00Z", "modelId": "gemini-4-neon"},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer peerSrv.Close()

	tmpDir := t.TempDir()
	t.Setenv("AGY_STATE_DB", "")
	t.Setenv("AGY_ACCOUNTS_FILE", tmpDir+"/accounts.json")
	t.Setenv("AGY_INSTANCE_ID", "node-a")
	t.Setenv("AGY_INSTANCE_LABEL", "Primary Node A")
	t.Setenv("AGY_DASHBOARD_PEERS", "node-b|Secondary Node B|"+peerSrv.URL)
	initConfig()
	GlobalAccountManager = NewAccountManager()

	gs := NewGatewayServer()
	defer gs.Close()

	payload := BuildFederatedDashboardPayload(gs, true)
	if !sawFedHopHeader.Load() {
		t.Fatal("expected peer request to carry X-Agy-Federated-Hop: 1 header")
	}

	insts, ok := payload["instances"].([]map[string]any)
	if !ok || len(insts) != 2 {
		t.Fatalf("expected 2 instances in federated payload, got %T len=%d", payload["instances"], len(insts))
	}
	if insts[0]["id"] != "node-a" || insts[1]["id"] != "node-b" || insts[1]["reachable"] != true {
		t.Fatalf("unexpected instances metadata: %+v", insts)
	}

	gwMap, _ := payload["gateway"].(map[string]any)
	// MaxLiveWorkers = primary (8) + node-b (3) = 11
	if mapInt64(gwMap, "maxLiveWorkers") != 11 {
		t.Fatalf("expected merged maxLiveWorkers=11, got %v", gwMap["maxLiveWorkers"])
	}

	live, _ := payload["liveSessions"].([]map[string]any)
	if len(live) != 1 || live[0]["instanceId"] != "node-b" || live[0]["instanceLabel"] != "Secondary Node B" {
		t.Fatalf("expected peer liveSession with instance attribution, got %+v", live)
	}

	// Simulate peer outage -> should degrade gracefully using 60s stale cache without failing primary
	peerFail.Store(true)
	payloadStale := BuildFederatedDashboardPayload(gs, true)
	instsStale, _ := payloadStale["instances"].([]map[string]any)
	if len(instsStale) != 2 || instsStale[1]["reachable"] != false || instsStale[1]["stale"] != true {
		t.Fatalf("expected peer outage to mark reachable=false, stale=true, got %+v", instsStale[1])
	}

	// Also verify HTTP handler loop prevention when X-Agy-Federated-Hop: 1 is sent
	req := httptest.NewRequest(http.MethodGet, "/api/dashboard", nil)
	req.Host = "127.0.0.1:18891"
	req.Header.Set("X-Agy-Federated-Hop", "1")
	rec := httptest.NewRecorder()
	gs.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from /api/dashboard with X-Agy-Federated-Hop, got %d", rec.Code)
	}
	if strings.Contains(rec.Body.String(), `"sess-nodeb-1"`) {
		t.Fatal("X-Agy-Federated-Hop: 1 request must return local_only payload and never federate downstream peers")
	}
}

func TestSingleContainerAccountManagerRoutingOAuthAndAPI(t *testing.T) {
	prevDB := GatewayStateDB
	t.Cleanup(func() {
		GlobalAccountManager.StopAll()
		GlobalAccountManager = NewAccountManager()
		resetPeerDashboardCacheForTest()
		initConfig()
		apiKeys = loadAPIKeys()
		InitGlobalStore(prevDB)
	})

	tmpDir := t.TempDir()
	accFile := tmpDir + "/accounts.json"
	presetsFile := tmpDir + "/presets.json"
	_ = os.WriteFile(presetsFile, []byte(`[{"id":"preset-c","label":"Region C Proxy","region":"RC","description":"Test proxy"}]`), 0o600)

	t.Setenv("AGY_STATE_DB", tmpDir+"/gateway.db")
	t.Setenv("AGY_ACCOUNTS_FILE", accFile)
	t.Setenv("AGY_PROXY_PRESETS_FILE", presetsFile)
	t.Setenv("AGY_INSTANCE_ID", "primary")
	t.Setenv("AGY_INSTANCE_LABEL", "Primary Account")
	t.Setenv("AGY_API_KEYS", "master-secret-key")
	t.Setenv("AGY_RESIN_PROXY_TOKEN", "rTok123")
	t.Setenv("AGY_RESIN_HOST", "proxy.internal:2260")
	initConfig()
	apiKeys = loadAPIKeys()

	GlobalAccountManager = NewAccountManager()
	GlobalAccountManager.Init()

	// Fresh install must seed ONLY 1 generic primary account with NO static backdoor boundKeys
	accs := GlobalAccountManager.ListAccounts()
	if len(accs) != 1 || accs[0].ID != "primary" || !accs[0].Primary || len(accs[0].BoundKeys) != 0 {
		t.Fatalf("expected 1 clean generic primary account with empty boundKeys, got %+v", accs)
	}
	if !accs[0].AllowsAllCanonicalModels() {
		t.Fatalf("expected fresh primary account to allow all canonical models, got %+v", accs[0].AllowedModels)
	}

	// Verify LoadProxyPresets merges AGY_PROXY_PRESETS_FILE cleanly
	presets := LoadProxyPresets()
	if len(presets) != 3 || presets[0].ID != "direct" || presets[1].ID != "preset-c" || presets[2].ID != "custom" {
		t.Fatalf("expected [direct, preset-c, custom] presets, got %+v", presets)
	}

	// Configure multi-account topology via UpsertAccount
	_, err := GlobalAccountManager.UpsertAccount(AccountProfile{
		ID:             "primary",
		Label:          "Primary Account",
		Enabled:        true,
		ProxyPreset:    "direct",
		MaxLiveWorkers: 8,
		BoundKeys:      []string{"sk-test-primary"},
		AllowedModels:  []string{"gemini-4-carbon", "gemini-4-hydrogen", "gemini-4-argon", "gemini-3.8-flash", "claude-opus-5-5", "claude-sonnet-5-5"},
	})
	if err != nil {
		t.Fatalf("upsert primary failed: %v", err)
	}
	for _, profile := range []AccountProfile{
		{ID: "acct-b", Label: "Account B", Enabled: true, ProxyPreset: "preset-b", MaxLiveWorkers: 3, BoundKeys: []string{"sk-test-b"}, AllowedModels: []string{"gemini-4-neon"}},
		{ID: "acct-c", Label: "Account C", Enabled: true, ProxyPreset: "preset-c", MaxLiveWorkers: 3, BoundKeys: []string{"sk-test-c"}, AllowedModels: []string{"gemini-4-neon"}},
		{ID: "acct-d", Label: "Account D", Enabled: true, ProxyPreset: "custom", CustomProxy: "http://user1:secretpw@127.0.0.1:8080", MaxLiveWorkers: 3, BoundKeys: []string{"sk-test-d"}, AllowedModels: []string{"gemini-4-neon"}},
	} {
		if _, err := GlobalAccountManager.UpsertAccount(profile); err != nil {
			t.Fatalf("upsert %s failed: %v", profile.ID, err)
		}
	}

	// Verify proxy preset resolution without persisting secrets
	acctC, ok := GlobalAccountManager.GetAccount("acct-c")
	if !ok || acctC.ProxyPreset != "preset-c" {
		t.Fatalf("expected acct-c with preset-c, got %+v", acctC)
	}
	resolved := GlobalAccountManager.ResolveAccountProxy(acctC)
	if resolved != "http://preset-c.acct-c:rTok123@proxy.internal:2260" {
		t.Fatalf("unexpected resolved proxy for acct-c: %q", resolved)
	}

	// Security P0 #4: reject malicious ProxyPreset injection and invalid CustomProxy scheme
	if _, err := GlobalAccountManager.UpsertAccount(AccountProfile{ID: "bad-p", ProxyPreset: "foo:bar@evil.com#"}); err == nil {
		t.Fatal("expected ProxyPreset with injection chars to be rejected")
	}
	if _, err := GlobalAccountManager.UpsertAccount(AccountProfile{ID: "bad-u", ProxyPreset: "custom", CustomProxy: "file:///etc/passwd"}); err == nil {
		t.Fatal("expected CustomProxy with non-proxy scheme to be rejected")
	}

	// Verify auth keyMatches accepts master key, @account suffix, #account suffix, and boundKeys
	for _, validKey := range []string{"master-secret-key", "master-secret-key@acct-b", "master-secret-key#acct-c", "sk-test-c", "sk-test-b"} {
		if !keyMatches(validKey) {
			t.Fatalf("expected keyMatches(%q) == true", validKey)
		}
	}
	if keyMatches("master-secret-key@nonexistent") || keyMatches("wrong-key@acct-c") {
		t.Fatal("expected invalid account or wrong master key to fail keyMatches")
	}

	// Verify extractAuthCodeFromInput handles both full callback URL and raw code
	cbURL := "https://antigravity.google/oauth-callback?state=xyz&code=4%2F0AVGzR1_test_code&scope=email"
	if got := extractAuthCodeFromInput(cbURL); got != "4/0AVGzR1_test_code" {
		t.Fatalf("expected extracted code '4/0AVGzR1_test_code', got %q", got)
	}
	if got := extractAuthCodeFromInput("  4/0A_raw_code  "); got != "4/0A_raw_code" {
		t.Fatalf("expected raw code '4/0A_raw_code', got %q", got)
	}

	gs := NewGatewayServer()
	defer gs.Close()

	// Security P0 #2: restricted boundKeys must NOT be able to access dashboard/admin /api/accounts or /api/dashboard
	for _, adminPath := range []string{"/api/accounts", "/api/dashboard"} {
		reqAdmin := httptest.NewRequest(http.MethodGet, adminPath, nil)
		reqAdmin.Header.Set("x-api-key", "sk-test-c")
		recAdmin := httptest.NewRecorder()
		gs.ServeHTTP(recAdmin, reqAdmin)
		if recAdmin.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401 when boundKey calls admin %s, got %d body=%s", adminPath, recAdmin.Code, recAdmin.Body.String())
		}
	}

	// Security P1 #6: GET /api/accounts masks customProxy password
	reqListAcc := httptest.NewRequest(http.MethodGet, "/api/accounts", nil)
	reqListAcc.Header.Set("x-api-key", "master-secret-key")
	recListAcc := httptest.NewRecorder()
	gs.ServeHTTP(recListAcc, reqListAcc)
	if recListAcc.Code != http.StatusOK || strings.Contains(recListAcc.Body.String(), "secretpw") || !strings.Contains(recListAcc.Body.String(), "preset-c") {
		t.Fatalf("expected masked customProxy and loaded presets in /api/accounts, got code=%d body=%s", recListAcc.Code, recListAcc.Body.String())
	}

	// Security P1 #5: browser cross-site mutation without X-Agy-Requested-With is rejected with 403
	reqCSRF := httptest.NewRequest(http.MethodDelete, "/api/accounts?id=acct-d", nil)
	reqCSRF.Host = "127.0.0.1:18891"
	reqCSRF.Header.Set("x-api-key", "master-secret-key")
	reqCSRF.Header.Set("Origin", "https://evil.example")
	reqCSRF.Header.Set("Sec-Fetch-Site", "cross-site")
	recCSRF := httptest.NewRecorder()
	gs.ServeHTTP(recCSRF, reqCSRF)
	if recCSRF.Code != http.StatusForbidden {
		t.Fatalf("expected 403 CSRF guard on cross-site DELETE /api/accounts, got %d body=%s", recCSRF.Code, recCSRF.Body.String())
	}
	// Same-origin dashboard request (also via X-Forwarded-Host reverse proxy) with the CSRF header must pass the guard.
	reqSame := httptest.NewRequest(http.MethodDelete, "/api/accounts?id=does-not-exist", nil)
	reqSame.Host = "127.0.0.1:18891"
	reqSame.Header.Set("x-api-key", "master-secret-key")
	reqSame.Header.Set("Origin", "https://console.example.com")
	reqSame.Header.Set("X-Forwarded-Host", "console.example.com")
	reqSame.Header.Set("Sec-Fetch-Site", "same-origin")
	reqSame.Header.Set("X-Agy-Requested-With", "dashboard")
	recSame := httptest.NewRecorder()
	gs.ServeHTTP(recSame, reqSame)
	if recSame.Code == http.StatusForbidden || recSame.Code == http.StatusUnauthorized {
		t.Fatalf("expected same-origin dashboard mutation to pass guard, got %d body=%s", recSame.Code, recSame.Body.String())
	}

	// Verify strict model allowlist enforcement on explicit key / @suffix
	reqKeySuffix := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	reqKeySuffix.Header.Set("x-api-key", "master-secret-key@acct-d")
	if tgt, mode, denyErr, st := GlobalAccountManager.ResolveTargetAccountDetailed(reqKeySuffix, "gemini-4-neon", ""); denyErr != nil || st != 0 || tgt.ID != "acct-d" || mode != "key-bound" {
		t.Fatalf("expected @acct-d suffix with bound model gemini-4-neon to bind acct-d, got tgt=%+v mode=%s err=%v st=%d", tgt, mode, denyErr, st)
	}
	if _, _, denyErr, st := GlobalAccountManager.ResolveTargetAccountDetailed(reqKeySuffix, "gemini-4-carbon", ""); denyErr == nil || st != http.StatusForbidden {
		t.Fatalf("expected @acct-d suffix with unbound model gemini-4-carbon to return 403, got st=%d err=%v", st, denyErr)
	}

	reqBoundKey := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	reqBoundKey.Header.Set("Authorization", "Bearer sk-test-c")
	if tgt, mode, denyErr, st := GlobalAccountManager.ResolveTargetAccountDetailed(reqBoundKey, "gemini-4-neon", ""); denyErr != nil || st != 0 || tgt.ID != "acct-c" || mode != "key-bound" {
		t.Fatalf("expected sk-test-c with gemini-4-neon to bind acct-c, got tgt=%+v mode=%s err=%v st=%d", tgt, mode, denyErr, st)
	}

	// Verify HTTP 403 JSON response when calling an unbound model via bound key on /v1/messages
	reqDenyHTTP := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"gemini-4-carbon","messages":[{"role":"user","content":"hi"}]}`))
	reqDenyHTTP.Header.Set("x-api-key", "sk-test-c")
	recDeny := httptest.NewRecorder()
	gs.ServeHTTP(recDeny, reqDenyHTTP)
	if recDeny.Code != http.StatusForbidden || !strings.Contains(recDeny.Body.String(), "model_not_allowed") {
		t.Fatalf("expected 403 model_not_allowed for sk-test-c calling gemini-4-carbon, got code=%d body=%s", recDeny.Code, recDeny.Body.String())
	}

	// Regression P3 #15: malformed JSON on restricted key must return 400 Bad Request, not 403
	reqBadJSON := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{not-valid-json`))
	reqBadJSON.Header.Set("x-api-key", "sk-test-c")
	recBadJSON := httptest.NewRecorder()
	gs.ServeHTTP(recBadJSON, reqBadJSON)
	if recBadJSON.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request for malformed JSON on restricted key, got %d body=%s", recBadJSON.Code, recBadJSON.Body.String())
	}

	// Verify GET /v1/models filters strictly by caller's bound key allowlist
	reqModAccC := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	reqModAccC.Header.Set("x-api-key", "sk-test-c")
	recModAccC := httptest.NewRecorder()
	gs.ServeHTTP(recModAccC, reqModAccC)
	if recModAccC.Code != http.StatusOK || recModAccC.Header().Get("X-Agy-Account") != "acct-c" {
		t.Fatalf("expected 200 and X-Agy-Account=acct-c for /v1/models, got %d hdr=%q", recModAccC.Code, recModAccC.Header().Get("X-Agy-Account"))
	}
	var modResp struct {
		Data []map[string]any `json:"data"`
	}
	_ = json.Unmarshal(recModAccC.Body.Bytes(), &modResp)
	if len(modResp.Data) != 1 || modResp.Data[0]["id"] != "gemini-4-neon" {
		t.Fatalf("expected /v1/models for sk-test-c to return only [gemini-4-neon], got %+v", modResp.Data)
	}

	// Import tokens for BOTH acct-b and acct-c to verify multi-account Round-Robin + Sticky Session
	for _, accID := range []string{"acct-b", "acct-c"} {
		tokBody := `{"accountId":"` + accID + `","token":"{\"access_token\":\"ya29.mock_token_value_1234567890\"}"}`
		reqTok := httptest.NewRequest(http.MethodPost, "/api/accounts/token", strings.NewReader(tokBody))
		reqTok.Header.Set("x-api-key", "master-secret-key")
		recTok := httptest.NewRecorder()
		gs.ServeHTTP(recTok, reqTok)
		if recTok.Code != http.StatusOK || !GlobalAccountManager.HasOAuthToken(accID) {
			t.Fatalf("expected /api/accounts/token 200 OK for %s, got code=%d body=%s", accID, recTok.Code, recTok.Body.String())
		}
		GlobalAccountManager.StopAccountWorker(accID)
	}

	// Multi-account same-model Round-Robin + per-(session, model) Sticky Session
	reqMasterA := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	reqMasterA.Header.Set("x-api-key", "master-secret-key")
	accA1, modeA1, errA1, _ := GlobalAccountManager.ResolveTargetAccountDetailed(reqMasterA, "gemini-4-neon", "hdr:sess-A")
	if errA1 != nil || accA1.ID != "acct-b" || modeA1 != "pool-rr" {
		t.Fatalf("expected sess-A turn 1 to RR -> acct-b, got id=%s mode=%s err=%v", accA1.ID, modeA1, errA1)
	}
	accB1, modeB1, errB1, _ := GlobalAccountManager.ResolveTargetAccountDetailed(reqMasterA, "gemini-4-neon", "hdr:sess-B")
	if errB1 != nil || accB1.ID != "acct-c" || modeB1 != "pool-rr" {
		t.Fatalf("expected sess-B turn 1 to RR -> acct-c, got id=%s mode=%s err=%v", accB1.ID, modeB1, errB1)
	}
	// Turn 2 for sess-A and sess-B must stay sticky to their respective accounts
	accA2, modeA2, _, _ := GlobalAccountManager.ResolveTargetAccountDetailed(reqMasterA, "gemini-4-neon", "hdr:sess-A")
	if accA2.ID != "acct-b" || modeA2 != "pool-sticky" {
		t.Fatalf("expected sess-A turn 2 sticky -> acct-b, got id=%s mode=%s", accA2.ID, modeA2)
	}
	accB2, modeB2, _, _ := GlobalAccountManager.ResolveTargetAccountDetailed(reqMasterA, "gemini-4-neon", "hdr:sess-B")
	if accB2.ID != "acct-c" || modeB2 != "pool-sticky" {
		t.Fatalf("expected sess-B turn 2 sticky -> acct-c, got id=%s mode=%s", accB2.ID, modeB2)
	}
	// Calling another model (gemini-4-carbon) under same sess-A must route to primary without clobbering sess-A's neon stickiness
	accACarb, _, errACarb, _ := GlobalAccountManager.ResolveTargetAccountDetailed(reqMasterA, "gemini-4-carbon", "hdr:sess-A")
	if errACarb != nil || accACarb.ID != "primary" {
		t.Fatalf("expected sess-A gemini-4-carbon -> primary, got id=%s err=%v", accACarb.ID, errACarb)
	}
	accA3, modeA3, _, _ := GlobalAccountManager.ResolveTargetAccountDetailed(reqMasterA, "gemini-4-neon", "hdr:sess-A")
	if accA3.ID != "acct-b" || modeA3 != "pool-sticky" {
		t.Fatalf("expected sess-A gemini-4-neon stickiness preserved -> acct-b, got id=%s mode=%s", accA3.ID, modeA3)
	}

	// Verify body-fingerprint session extraction stays identical across Turn 1 and Turn 2 without headers,
	// and splits two different conversations from the same OpenAI "user" (P3 #16)
	t1Req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"gemini-4-neon-high","user":"shared-user","system":"sys-1","messages":[{"role":"user","content":"hello world"}]}`))
	t2Req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"gemini-4-neon-high","user":"shared-user","system":"sys-1","messages":[{"role":"user","content":"hello world"},{"role":"assistant","content":"hi"},{"role":"user","content":"next turn"}]}`))
	t3Req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"gemini-4-neon-high","user":"shared-user","system":"sys-1","messages":[{"role":"user","content":"different conversation"}]}`))
	m1, sk1, _, bad1, tl1, re1 := extractRequestRoutingHints(t1Req)
	m2, sk2, _, bad2, tl2, re2 := extractRequestRoutingHints(t2Req)
	_, sk3, _, bad3, tl3, re3 := extractRequestRoutingHints(t3Req)
	if bad1 || bad2 || bad3 || tl1 || tl2 || tl3 || re1 != nil || re2 != nil || re3 != nil || m1 != "gemini-4-neon" || m2 != "gemini-4-neon" || !strings.HasPrefix(sk1, "fp:") || sk1 != sk2 || sk1 == sk3 {
		t.Fatalf("expected matching fp across same conv and distinct fp across parallel convs: m1=%q m2=%q sk1=%q sk2=%q sk3=%q", m1, m2, sk1, sk2, sk3)
	}

	// Regression #1: X-Agy-Account header must NOT override a restricted bound key
	reqOverride := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"gemini-4-carbon","messages":[{"role":"user","content":"hi"}]}`))
	reqOverride.Header.Set("x-api-key", "sk-test-c")
	reqOverride.Header.Set("X-Agy-Account", "primary")
	recOverride := httptest.NewRecorder()
	gs.ServeHTTP(recOverride, reqOverride)
	if recOverride.Code != http.StatusForbidden {
		t.Fatalf("expected 403 when sk-test-c tries to override account with X-Agy-Account: primary, got %d body=%s", recOverride.Code, recOverride.Body.String())
	}

	// Regression #2: Omitted "model" or non-string hint field (user: 123) must NOT bypass allowlist on bound key
	reqEmptyModel := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"user":123,"messages":[{"role":"user","content":"hi"}]}`))
	reqEmptyModel.Header.Set("x-api-key", "sk-test-c")
	recEmptyModel := httptest.NewRecorder()
	gs.ServeHTTP(recEmptyModel, reqEmptyModel)
	if recEmptyModel.Code != http.StatusForbidden {
		t.Fatalf("expected 403 when sk-test-c omits model (resolves to default gemini-4-carbon), got %d body=%s", recEmptyModel.Code, recEmptyModel.Body.String())
	}

	// Regression #3: /v1beta/models/models/gemini-4-neon:generateContent double-prefix must NOT bypass primary allowlist, and GET /v1beta/models must filter
	reqV1BetaBypass := httptest.NewRequest(http.MethodPost, "/v1beta/models/models/gemini-4-neon:generateContent", strings.NewReader(`{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`))
	reqV1BetaBypass.Header.Set("x-goog-api-key", "sk-test-primary")
	recV1BetaBypass := httptest.NewRecorder()
	gs.ServeHTTP(recV1BetaBypass, reqV1BetaBypass)
	if recV1BetaBypass.Code != http.StatusForbidden || !strings.Contains(recV1BetaBypass.Body.String(), "PERMISSION_DENIED") {
		t.Fatalf("expected 403 Gemini PERMISSION_DENIED for sk-test-primary calling double-prefix neon, got %d body=%s", recV1BetaBypass.Code, recV1BetaBypass.Body.String())
	}
	reqV1BetaList := httptest.NewRequest(http.MethodGet, "/v1beta/models", nil)
	reqV1BetaList.Header.Set("x-goog-api-key", "sk-test-c")
	recV1BetaList := httptest.NewRecorder()
	gs.ServeHTTP(recV1BetaList, reqV1BetaList)
	if recV1BetaList.Code != http.StatusOK || !strings.Contains(recV1BetaList.Body.String(), "models/gemini-4-neon") || strings.Contains(recV1BetaList.Body.String(), "models/gemini-4-carbon") {
		t.Fatalf("expected /v1beta/models for sk-test-c to include only neon, got %d body=%s", recV1BetaList.Code, recV1BetaList.Body.String())
	}

	// Regression #9: v1 accounts.json migration preserves all non-claimed models on primary
	v1Migrated := normalizeAccountsList([]AccountProfile{
		{ID: "primary", Primary: true, Enabled: true, DefaultModels: []string{"gemini-4-hydrogen", "gemini-4-carbon"}},
		{ID: "acct-b", Primary: false, Enabled: true, DefaultModels: []string{"gemini-4-neon"}},
	}, 1)
	if !v1Migrated[0].AllowsModel("claude-opus-5-5") || !v1Migrated[0].AllowsModel("gemini-3.8-flash") || v1Migrated[0].AllowsModel("gemini-4-neon") {
		t.Fatalf("expected v1->v2 migration to keep non-neon models on primary and exclude neon, got %+v", v1Migrated[0].AllowedModels)
	}

	// Regression #10: Case-variant "Model" or non-string "model": 123 must NOT bypass per-key allowlists
	for _, rawBody := range []string{
		`{"Model":"gemini-4-neon","messages":[{"role":"user","content":"hi"}]}`,
		`{"model":123,"messages":[{"role":"user","content":"hi"}]}`,
	} {
		reqBypass := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(rawBody))
		reqBypass.Header.Set("x-api-key", "sk-test-c")
		recBypass := httptest.NewRecorder()
		gs.ServeHTTP(recBypass, reqBypass)
		if recBypass.Code != http.StatusForbidden {
			t.Fatalf("expected 403 allowlist rejection for body=%s, got %d body=%s", rawBody, recBypass.Code, recBypass.Body.String())
		}
	}

	// Regression #11: Master-key #<accountId> selector binds explicit account without tripping simple-single-primary shortcut
	reqHashSel := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	reqHashSel.Header.Set("Authorization", "Bearer master-secret-key#acct-d")
	if tgt, mode, denyErr, st := GlobalAccountManager.ResolveTargetAccountDetailed(reqHashSel, "gemini-4-neon", ""); denyErr != nil || st != 0 || tgt.ID != "acct-d" || mode != "key-bound" {
		t.Fatalf("expected #acct-d suffix to bind acct-d, got tgt=%+v mode=%s err=%v st=%d", tgt, mode, denyErr, st)
	}

	// Regression #12: /v1beta/models/<pathModel>:countTokens locks onto URL pathModel over conflicting/invalid body "model"
	reqCntOK := httptest.NewRequest(http.MethodPost, "/v1beta/models/gemini-4-carbon:countTokens", strings.NewReader(`{"generateContentRequest":{"model":"models/nonexistent-spoof-model","contents":[{"role":"user","parts":[{"text":"hello count tokens"}]}]}}`))
	reqCntOK.Header.Set("x-goog-api-key", "sk-test-primary")
	recCntOK := httptest.NewRecorder()
	gs.ServeHTTP(recCntOK, reqCntOK)
	if recCntOK.Code != http.StatusOK || !strings.Contains(recCntOK.Body.String(), "totalTokens") {
		t.Fatalf("expected 200 OK on primary carbon :countTokens despite conflicting body model, got %d body=%s", recCntOK.Code, recCntOK.Body.String())
	}
	reqCntDeny := httptest.NewRequest(http.MethodPost, "/v1beta/models/gemini-4-neon:countTokens", strings.NewReader(`{"generateContentRequest":{"model":"models/gemini-4-carbon","contents":[{"role":"user","parts":[{"text":"hello count tokens"}]}]}}`))
	reqCntDeny.Header.Set("x-goog-api-key", "sk-test-primary")
	recCntDeny := httptest.NewRecorder()
	gs.ServeHTTP(recCntDeny, reqCntDeny)
	if recCntDeny.Code != http.StatusForbidden {
		t.Fatalf("expected 403 on disallowed neon :countTokens for sk-test-primary, got %d body=%s", recCntDeny.Code, recCntDeny.Body.String())
	}

	// Regression #13: UpsertAccount rejects master key and cross-account duplicate BoundKeys; disabled account worker refuses to start
	if _, err := GlobalAccountManager.UpsertAccount(AccountProfile{
		ID:            "acct-d",
		Label:         "Acct D",
		Enabled:       true,
		BoundKeys:     []string{"master-secret-key"},
		AllowedModels: []string{"gemini-4-neon"},
	}); err == nil {
		t.Fatal("expected UpsertAccount to reject binding the gateway master key")
	}
	if _, err := GlobalAccountManager.UpsertAccount(AccountProfile{
		ID:            "acct-d",
		Label:         "Acct D",
		Enabled:       true,
		BoundKeys:     []string{"sk-test-c"},
		AllowedModels: []string{"gemini-4-neon"},
	}); err == nil {
		t.Fatal("expected UpsertAccount to reject cross-account duplicate BoundKeys")
	}
	if _, err := GlobalAccountManager.UpsertAccount(AccountProfile{
		ID:            "acct-disabled",
		Label:         "Disabled",
		Enabled:       false,
		AllowedModels: []string{"gemini-4-neon"},
	}); err != nil {
		t.Fatalf("unexpected UpsertAccount error for disabled account: %v", err)
	}
	if err := GlobalAccountManager.EnsureAccountWorkerRunning("acct-disabled"); err == nil || !strings.Contains(err.Error(), "disabled") {
		t.Fatalf("expected EnsureAccountWorkerRunning to reject disabled account, got %v", err)
	}

	// Verify unauthenticated account (acct-d) appears in federated dashboard instances with unauthenticated=true
	payload := BuildFederatedDashboardPayload(gs, false)
	GlobalAccountManager.StopAll()
	insts, _ := payload["instances"].([]map[string]any)
	var sawUnauthD bool
	for _, inst := range insts {
		if inst["id"] == "acct-d" && inst["unauthenticated"] == true {
			sawUnauthD = true
		}
	}
	if !sawUnauthD {
		t.Fatalf("expected unauthenticated acct-d slot in federated dashboard instances, got %+v", insts)
	}
}

func TestAdversarialHardening_OAuthSanitizeOriginRedirectAndCleanroom(t *testing.T) {
	// 1. OAuth code extraction strips control characters and URL fragments
	gotCode := extractAuthCodeFromInput("https://localhost:8080/callback?code=4%2F0AeanS0_abc%0d%0aINJECT%00%7f&scope=email#frag")
	if gotCode != "4/0AeanS0_abcINJECT" {
		t.Fatalf("expected sanitized auth code without control bytes or fragment, got %q", gotCode)
	}
	if rawCode := extractAuthCodeFromInput("  code-with-\r\n-newlines\x1b  "); rawCode != "code-with--newlines" {
		t.Fatalf("unexpected sanitized raw code: %q", rawCode)
	}

	// 2. sameOrigin blocks HTTPS->HTTP downgrade and non-HTTP schemes
	reqHTTPS := httptest.NewRequest(http.MethodPost, "/api/accounts", nil)
	reqHTTPS.Host = "gw.internal:18891"
	reqHTTPS.Header.Set("X-Forwarded-Proto", "https")
	reqHTTPS.Header.Set("Origin", "http://gw.internal:18891")
	if sameOrigin(reqHTTPS) {
		t.Fatal("expected sameOrigin to reject http Origin when X-Forwarded-Proto is https")
	}
	reqHTTPS.Header.Set("Origin", "https://gw.internal:18891")
	if !sameOrigin(reqHTTPS) {
		t.Fatal("expected sameOrigin to accept matching https Origin")
	}
	reqTLS := httptest.NewRequest(http.MethodPost, "/api/accounts", nil)
	reqTLS.Host = "gw.internal:18891"
	reqTLS.TLS = &tls.ConnectionState{}
	reqTLS.Header.Set("Origin", "http://gw.internal:18891")
	if sameOrigin(reqTLS) {
		t.Fatal("expected sameOrigin to reject http Origin on direct TLS request")
	}
	reqBadScheme := httptest.NewRequest(http.MethodPost, "/api/accounts", nil)
	reqBadScheme.Host = "gw.internal:18891"
	reqBadScheme.Header.Set("Origin", "javascript://gw.internal:18891")
	if sameOrigin(reqBadScheme) {
		t.Fatal("expected sameOrigin to reject non-http(s) Origin scheme")
	}

	// 3. peerHTTPClient must NOT follow 302 redirects or leak x-api-key to redirect target
	var leaked atomic.Bool
	evilSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-api-key") != "" || r.Header.Get("Authorization") != "" {
			leaked.Store(true)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer evilSrv.Close()
	redirSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, evilSrv.URL+"/steal", http.StatusFound)
	}))
	defer redirSrv.Close()
	peerReq, err := http.NewRequest(http.MethodGet, redirSrv.URL+"/api/dashboard", nil)
	if err != nil {
		t.Fatal(err)
	}
	peerReq.Header.Set("x-api-key", "super-secret-peer-key")
	peerResp, err := peerHTTPClient.Do(peerReq)
	if err != nil {
		t.Fatalf("peerHTTPClient.Do error: %v", err)
	}
	_ = peerResp.Body.Close()
	if peerResp.StatusCode != http.StatusFound || leaked.Load() {
		t.Fatalf("expected unfollowed 302 without credential leak, got status=%d leaked=%v", peerResp.StatusCode, leaked.Load())
	}

	// 4. buildQuotaView suppresses default Ultra 20x PlanLabel on errored empty snapshot
	resetCreditsTierCacheForTest()
	t.Cleanup(resetCreditsTierCacheForTest)
	t.Setenv("AGY_ACCOUNT_PLAN_TIER_HINT", "")
	errSnap, _ := buildQuotaView(QuotaSnapshot{Status: "error", Error: "usage probe timeout"}, nil, time.Now())
	if errSnap.PlanLabel != "" {
		t.Fatalf("expected empty PlanLabel on errored empty QuotaSnapshot, got %q", errSnap.PlanLabel)
	}

	// 5. cleanroom.SanitizeStructuredValue scrubs []string slices
	scrubbed, ok := cleanroom.SanitizeStructuredValue([]string{"clean", "<system-reminder>internal</system-reminder>keep"}, nil).([]string)
	if !ok || len(scrubbed) != 2 || strings.Contains(scrubbed[1], "system-reminder") || !strings.Contains(scrubbed[1], "keep") {
		t.Fatalf("expected []string sanitization to strip reminder tags and preserve text, got %#v", scrubbed)
	}
}

func TestFederatedDashboardDisabledPrimaryBypassAndParallelPeers(t *testing.T) {
	waitQuotaRefreshIdle(t)
	resetPeerDashboardCacheForTest()
	resetCreditsTierCacheForTest()
	quotaMu.Lock()
	prevQuota, prevAt, prevErr, prevAtt, prevSteps := cachedQuota, cachedQuotaAt, lastQuotaErr, lastQuotaAttemptAt, quotaBackoffSteps
	cachedQuota, cachedQuotaAt, lastQuotaErr, lastQuotaAttemptAt, quotaBackoffSteps = nil, time.Time{}, nil, time.Time{}, 0
	quotaMu.Unlock()
	t.Cleanup(func() {
		waitQuotaRefreshIdle(t)
		GlobalAccountManager.StopAll()
		GlobalAccountManager = NewAccountManager()
		resetPeerDashboardCacheForTest()
		resetCreditsTierCacheForTest()
		quotaMu.Lock()
		cachedQuota, cachedQuotaAt, lastQuotaErr, lastQuotaAttemptAt, quotaBackoffSteps = prevQuota, prevAt, prevErr, prevAtt, prevSteps
		quotaMu.Unlock()
		initConfig()
	})

	tmp := t.TempDir()
	accFile := filepath.Join(tmp, "accounts.json")
	countFile := filepath.Join(tmp, "primary-quota-calls.txt")
	peerHitFile := filepath.Join(tmp, "peer-hit.flag")

	// Mock AGY_QUOTA_BIN that logs invocations and waits up to 2s until peerHitFile exists (proving parallelism!).
	slowPrimaryBin := filepath.Join(tmp, "agy-primary-slow")
	script := "#!/bin/sh\n" +
		"echo hit >> '" + countFile + "'\n" +
		"i=0\n" +
		"while [ ! -f '" + peerHitFile + "' ] && [ $i -lt 40 ]; do\n" +
		"  sleep 0.05\n" +
		"  i=$((i+1))\n" +
		"done\n" +
		"echo '{\"command\":{\"data\":{\"groups\":[{\"name\":\"Gemini Models\",\"description\":\"Gemini 4 Carbon\",\"buckets\":[{\"window\":\"5h\",\"remaining_fraction\":0.9,\"reset_time\":\"2026-11-26T00:00:00Z\"}]}]}}}'\n"
	if err := os.WriteFile(slowPrimaryBin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	var peerHits atomic.Int32
	peerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		peerHits.Add(1)
		_ = os.WriteFile(peerHitFile, []byte("1"), 0o600)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"now": time.Now().UTC().Format(time.RFC3339),
			"gateway": map[string]any{
				"instanceId":           "acct-b",
				"instanceLabel":        "Account B",
				"activeSessionsCount":  1,
				"maxLiveWorkers":       3,
				"maxConcurrentPrompts": 2,
			},
			"quota": QuotaSnapshot{
				Status: "error",
				Error:  "exit status 1: Account has been suspended",
				Groups: []QuotaGroup{},
			},
			"windows": map[string]WindowAggregate{},
		})
	}))
	defer peerSrv.Close()

	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "gateway.db"))
	t.Setenv("AGY_ACCOUNTS_FILE", accFile)
	t.Setenv("AGY_QUOTA_BIN", slowPrimaryBin)
	t.Setenv("AGY_INSTANCE_ID", "primary")
	t.Setenv("AGY_INSTANCE_LABEL", "Primary")
	t.Setenv("AGY_MAX_LIVE_WORKERS", "8")
	t.Setenv("AGY_DASHBOARD_PEERS", "acct-b|Account B|"+peerSrv.URL)
	initConfig()

	GlobalAccountManager.StopAll()
	GlobalAccountManager = NewAccountManager()
	GlobalAccountManager.Init()
	if _, err := GlobalAccountManager.UpsertAccount(AccountProfile{
		ID:             "acct-b",
		Label:          "Account B",
		Enabled:        true,
		ProxyPreset:    "direct",
		MaxLiveWorkers: 3,
		AllowedModels:  []string{"gemini-4-neon"},
	}); err != nil {
		t.Fatalf("upsert acct-b: %v", err)
	}

	gs := NewGatewayServer()
	defer gs.Close()

	// 1. Disable Primary -> BuildFederatedDashboardPayload(gs, true) must SKIP Primary FetchOfficialQuota,
	// mark selfInst["disabled"]=true, and omit disabled Primary's 8 maxLiveWorkers from totalMaxWorkers!
	if _, err := GlobalAccountManager.PerformAccountActionEx(AccountActionRequest{AccountID: "primary", Action: "disable"}); err != nil {
		t.Fatalf("disable primary: %v", err)
	}
	payloadDisabled := BuildFederatedDashboardPayload(gs, true)
	if _, err := os.Stat(countFile); !os.IsNotExist(err) {
		t.Fatal("disabled primary must skip FetchOfficialQuota completely")
	}
	insts, _ := payloadDisabled["instances"].([]map[string]any)
	if len(insts) < 2 {
		t.Fatalf("expected >=2 instances (primary + acct-b), got %d", len(insts))
	}
	if insts[0]["disabled"] != true {
		t.Fatalf("expected selfInst disabled=true when primary disabled, got %+v", insts[0])
	}
	if qSelf, ok := insts[0]["quota"].(QuotaSnapshot); !ok || qSelf.Status != QuotaStateNotEnabled {
		t.Fatalf("expected disabled primary quota status=not_enabled, got %+v", insts[0]["quota"])
	}
	aggGw, _ := payloadDisabled["gateway"].(map[string]any)
	if gotMax := mapInt64(aggGw, "maxLiveWorkers"); gotMax != 3 {
		t.Fatalf("expected totalMaxWorkers=3 (excluding disabled primary's 8 workers), got %d", gotMax)
	}
	// Peer ban error from quota.error should classify into acct-b's health view as tos_tripped
	accB, _ := GlobalAccountManager.GetAccount("acct-b")
	if hvB := GlobalAccountManager.BuildAccountHealthView(accB, true, 0, time.Now()); hvB.State != "tos_tripped" {
		t.Fatalf("expected peer /usage ban error to mark acct-b tos_tripped, got %+v", hvB)
	}

	// 2. Re-enable Primary and remove peerHitFile -> BuildFederatedDashboardPayload(gs, true) must run
	// Primary /usage AND peer fetch IN PARALLEL so peer writes peerHitFile and unblocks Primary immediately!
	if _, err := GlobalAccountManager.PerformAccountActionEx(AccountActionRequest{AccountID: "primary", Action: "enable"}); err != nil {
		t.Fatalf("enable primary: %v", err)
	}
	_ = os.Remove(peerHitFile)
	resetPeerDashboardCacheForTest()
	t0 := time.Now()
	payloadPar := BuildFederatedDashboardPayload(gs, true)
	elapsed := time.Since(t0)
	if elapsed >= 1500*time.Millisecond {
		t.Fatalf("expected parallel self + peer federation to unblock immediately (<1.5s), took %v (peer was blocked by primary!)", elapsed)
	}
	if peerHits.Load() < 2 {
		t.Fatalf("expected peer to be fetched in parallel, hits=%d", peerHits.Load())
	}
	if _, ok := payloadPar["instances"].([]map[string]any); !ok {
		t.Fatalf("missing instances in federated payload")
	}
}
