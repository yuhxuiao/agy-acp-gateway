package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestUltra5xTierNormalizationAndLabels(t *testing.T) {
	cases := map[string]string{
		"ultra_5x":           "ultra_5x",
		"ultra-5x":           "ultra_5x",
		"Ultra 5x":           "ultra_5x",
		"Ultra 5×":           "ultra_5x",
		"Google AI Ultra 5x": "ultra_5x",
		"5x":                 "ultra_5x",
		"pro":                "pro",
		"Google AI Pro":      "pro",
		"ultra":              "ultra",
		"Ultra 20x":          "ultra",
	}
	for input, want := range cases {
		if got := normalizePlanTierHint(input); got != want {
			t.Errorf("normalizePlanTierHint(%q) = %q, want %q", input, got, want)
		}
	}
	if got := planLabelForTier("ultra_5x"); got != "Ultra 5x" {
		t.Errorf("planLabelForTier(ultra_5x) = %q, want Ultra 5x", got)
	}
}

func TestUnknownCapDoesNotInventDollarUsage(t *testing.T) {
	fraction := 0.5
	if got := pctUsdPtr(0, &fraction); got != nil {
		t.Fatalf("unknown cap gave dollar value %v", *got)
	}

	now := time.Date(2026, 11, 27, 8, 0, 0, 0, time.UTC)
	r5 := now.Add(3 * time.Hour).Format(time.RFC3339)
	rw := now.Add(6 * 24 * time.Hour).Format(time.RFC3339)
	snap := QuotaSnapshot{
		Status:    "ok",
		UpdatedAt: now.Format(time.RFC3339),
		PlanTier:  "ultra_5x",
		Groups: []QuotaGroup{
			{
				Name: "Gemini Models",
				Buckets: []QuotaBucket{
					{ID: "gemini-5h", Window: "5h", RemainingFraction: 0.97, UsedFraction: 0.03, ResetTime: r5},
					{ID: "gemini-weekly", Window: "weekly", RemainingFraction: 0.67, UsedFraction: 0.33, ResetTime: rw},
				},
			},
			{
				Name: "Claude and GPT models",
				Buckets: []QuotaBucket{
					{ID: "3p-5h", Window: "5h", RemainingFraction: 1.0, UsedFraction: 0.0, ResetTime: "1970-01-01T00:00:00Z"},
					{ID: "3p-weekly", Window: "weekly", RemainingFraction: 0.99, UsedFraction: 0.01, ResetTime: rw},
				},
			},
		},
	}
	view, _ := buildQuotaView(snap, nil, now)
	if view.PlanTier != "ultra_5x" || view.PlanLabel != "Ultra 5x" {
		t.Fatalf("expected ultra_5x / Ultra 5x, got tier=%q label=%q", view.PlanTier, view.PlanLabel)
	}
	if view.Deduction == nil || view.Deduction.Cap5hUsd != 0 || view.Deduction.CapWeeklyUsd != 0 {
		t.Fatalf("expected 0 unknown dollar caps for ultra_5x, got %+v", view.Deduction)
	}
	if view.Deduction.PoolUsed5hUsd != nil || view.Deduction.PoolRem5hUsd != nil || view.Deduction.EffectiveRemUsd != nil {
		t.Fatalf("expected nil USD estimates for ultra_5x, got %+v", view.Deduction)
	}
	if view.ThirdPartyPool == nil || view.ThirdPartyPool.Cap5hUsd != 0 || view.ThirdPartyPool.PoolUsed5hUsd != nil {
		t.Fatalf("expected nil 3p USD estimates for ultra_5x, got %+v", view.ThirdPartyPool)
	}
	if got := view.Groups[0].Buckets[0].RemainingFraction; got != 0.97 {
		t.Fatalf("expected real RemainingFraction 0.97 preserved, got %v", got)
	}
}

func TestModelNamesAreNotDefinitivePlanEvidence(t *testing.T) {
	for _, tc := range []struct {
		name  string
		group QuotaGroup
	}{
		{"argon group", QuotaGroup{Name: "Argon models"}},
		{"argon bucket", QuotaGroup{Name: "Gemini models", Buckets: []QuotaBucket{{ID: "argon"}}}},
		{"non-carbon description", QuotaGroup{Name: "Gemini models", Description: "Gemini models"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, definitive := detectQuotaPlanTierDetailed(QuotaSnapshot{Groups: []QuotaGroup{tc.group}})
			if definitive {
				t.Fatal("model names must not be treated as definitive subscription tier evidence")
			}
		})
	}
}

func TestPlanTierOverridePersistsAndIsNeverClobberedByProbes(t *testing.T) {
	waitQuotaRefreshIdle(t)
	resetCreditsTierCacheForTest()

	origMgr := GlobalAccountManager
	origPure := PureApiHome
	quotaMu.Lock()
	savedCache, savedAt, savedErr, savedAttempt, savedSteps := cachedQuota, cachedQuotaAt, lastQuotaErr, lastQuotaAttemptAt, quotaBackoffSteps
	cachedQuota, cachedQuotaAt, lastQuotaErr, lastQuotaAttemptAt, quotaBackoffSteps = nil, time.Time{}, nil, time.Time{}, 0
	quotaMu.Unlock()

	tmp := t.TempDir()
	t.Setenv("AGY_GW_HOME", tmp)
	t.Setenv("AGY_ACCOUNTS_FILE", filepath.Join(tmp, "accounts.json"))
	t.Setenv("AGY_RESIN_PROXY_TOKEN", "")
	t.Setenv("AGY_UPSTREAM_PROXY", "")
	t.Setenv("AGY_ACCOUNT_PLAN_TIER_HINT", "")
	t.Setenv("AGY_ACCOUNT_PLAN_TIER_OVERRIDE", "")
	initConfig()

	t.Cleanup(func() {
		waitQuotaRefreshIdle(t)
		resetCreditsTierCacheForTest()
		GlobalAccountManager = origMgr
		PureApiHome = origPure
		initConfig()
		quotaMu.Lock()
		cachedQuota, cachedQuotaAt, lastQuotaErr, lastQuotaAttemptAt, quotaBackoffSteps = savedCache, savedAt, savedErr, savedAttempt, savedSteps
		quotaMu.Unlock()
	})

	PureApiHome = filepath.Join(tmp, "pure-home")
	_ = os.MkdirAll(PureApiHome, 0o700)
	am := NewAccountManager()
	am.Init()
	GlobalAccountManager = am

	if _, err := am.upsertAccount(AccountProfile{
		ID:                "primary",
		Label:             "Main",
		Enabled:           true,
		PlanTierOverride:  "ultra_5x",
		IgnoreBurnAnomaly: true,
		AllowedModels:     []string{"gemini-4-hydrogen"},
	}, true); err != nil {
		t.Fatalf("upsert primary override: %v", err)
	}

	// Automatic probe must not overwrite PlanTierOverride or pollute PlanTierHint
	am.SetAccountPlanTierHint("primary", "pro")
	prim, ok := am.GetPrimaryAccount()
	if !ok || prim.PlanTierOverride != "ultra_5x" || prim.EffectivePlanTier() != "ultra_5x" {
		t.Fatalf("expected PlanTierOverride=ultra_5x preserved, got %+v", prim)
	}
	if prim.PlanTierHint != "" {
		t.Fatalf("expected PlanTierHint untouched while override active, got %q", prim.PlanTierHint)
	}

	// Reload from disk and verify persistence
	am2 := NewAccountManager()
	am2.Init()
	if p2, ok := am2.GetPrimaryAccount(); !ok || p2.PlanTierOverride != "ultra_5x" {
		t.Fatalf("expected PlanTierOverride persisted on disk, got %+v", p2)
	}

	// Even when /usage + /credits claim Pro (upgrade_uri), override wins!
	r5 := time.Now().Add(3 * time.Hour).UTC().Format(time.RFC3339)
	rw := time.Now().Add(6 * 24 * time.Hour).UTC().Format(time.RFC3339)
	mockBin := filepath.Join(tmp, "agy-mock-pro")
	script := "#!/bin/sh\n" +
		"if [ \"$2\" = \"/credits\" ]; then\n" +
		"  echo '{\"status\":\"SUCCESS\",\"command\":{\"name\":\"credits\",\"data\":{\"upgrade_uri\":\"https://antigravity.google/g1-upgrade\"}}}'\n" +
		"  exit 0\n" +
		"fi\n" +
		"echo '{\"command\":{\"data\":{\"description\":\"Google AI Pro\",\"groups\":[{\"name\":\"Gemini Models\",\"buckets\":[{\"window\":\"5h\",\"remaining_fraction\":0.9,\"reset_time\":\"" + r5 + "\"},{\"window\":\"weekly\",\"remaining_fraction\":0.7,\"reset_time\":\"" + rw + "\"}]}]}}}'\n"
	if err := os.WriteFile(mockBin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGY_QUOTA_BIN", mockBin)

	snap := fetchOfficialQuotaDo()
	if snap.PlanTier != "ultra_5x" || snap.PlanLabel != "Ultra 5x" {
		t.Fatalf("expected override ultra_5x to win over /credits + /usage Pro cues, got tier=%q label=%q", snap.PlanTier, snap.PlanLabel)
	}
	view, _ := buildQuotaView(snap, nil, time.Now())
	if view.PlanTier != "ultra_5x" || view.Deduction.Cap5hUsd != 0 || view.Deduction.PoolUsed5hUsd != nil {
		t.Fatalf("expected ultra_5x view without fabricated caps, got %+v / %+v", view.PlanTier, view.Deduction)
	}

	// Verify child worker subprocess env propagation carries BOTH AGY_ACCOUNT_PLAN_TIER_OVERRIDE and AGY_IGNORE_BURN_ANOMALY
	childAcc := AccountProfile{ID: "acct-5x", Label: "Acct 5x", PlanTierHint: "pro", PlanTierOverride: "ultra_5x", IgnoreBurnAnomaly: true}
	env := buildAccountWorkerEnv(childAcc, filepath.Join(tmp, "accounts", "acct-5x"), "", "180MiB", 19001, 19002, 3, 6, 0, false)
	foundOv, foundIgn := false, false
	for _, kv := range env {
		if kv == "AGY_ACCOUNT_PLAN_TIER_OVERRIDE=ultra_5x" {
			foundOv = true
		}
		if kv == "AGY_IGNORE_BURN_ANOMALY=1" {
			foundIgn = true
		}
	}
	if !foundOv || !foundIgn || childAcc.EffectivePlanTier() != "ultra_5x" {
		t.Fatalf("expected AGY_ACCOUNT_PLAN_TIER_OVERRIDE=ultra_5x & AGY_IGNORE_BURN_ANOMALY=1 in worker env, got foundOv=%v foundIgn=%v", foundOv, foundIgn)
	}

	// Verify GET /api/accounts exposes planTierOverride and effectivePlanTier
	getReq := httptest.NewRequest(http.MethodGet, "/api/accounts", nil)
	getRec := httptest.NewRecorder()
	HandleAccountsAPI(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 on GET /api/accounts, got %d: %s", getRec.Code, getRec.Body.String())
	}
	var getPayload struct {
		OK       bool `json:"ok"`
		Accounts []struct {
			ID                string `json:"id"`
			PlanTierOverride  string `json:"planTierOverride"`
			EffectivePlanTier string `json:"effectivePlanTier"`
		} `json:"accounts"`
	}
	if err := json.Unmarshal(getRec.Body.Bytes(), &getPayload); err != nil {
		t.Fatalf("unmarshal GET /api/accounts: %v", err)
	}
	if len(getPayload.Accounts) == 0 || getPayload.Accounts[0].PlanTierOverride != "ultra_5x" || getPayload.Accounts[0].EffectivePlanTier != "ultra_5x" {
		t.Fatalf("expected planTierOverride & effectivePlanTier in GET /api/accounts, got %+v", getPayload.Accounts)
	}

	// Verify POST /api/accounts omitting planTierOverride & ignoreBurnAnomaly preserves both existing flags
	omitBody, _ := json.Marshal(map[string]any{
		"id":            "primary",
		"label":         "Main Updated",
		"enabled":       true,
		"allowedModels": []string{"gemini-4-hydrogen"},
	})
	omitReq := httptest.NewRequest(http.MethodPost, "/api/accounts", bytes.NewReader(omitBody))
	omitReq.Header.Set("Content-Type", "application/json")
	omitReq.Header.Set("X-Agy-Requested-With", "dashboard")
	omitRec := httptest.NewRecorder()
	HandleAccountsAPI(omitRec, omitReq)
	if omitRec.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 when omitting planTierOverride, got %d: %s", omitRec.Code, omitRec.Body.String())
	}
	if primKept, ok := am.GetPrimaryAccount(); !ok || primKept.PlanTierOverride != "ultra_5x" || !primKept.IgnoreBurnAnomaly {
		t.Fatalf("expected PlanTierOverride & IgnoreBurnAnomaly preserved when omitted from payload, got %+v", primKept)
	}

	// Verify POST /api/accounts can explicitly clear PlanTierOverride back to "" (Auto)
	clearBody, _ := json.Marshal(map[string]any{
		"id":               "primary",
		"label":            "Main",
		"enabled":          true,
		"planTierOverride": "",
		"allowedModels":    []string{"gemini-4-hydrogen"},
	})
	req := httptest.NewRequest(http.MethodPost, "/api/accounts", bytes.NewReader(clearBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Agy-Requested-With", "dashboard")
	rec := httptest.NewRecorder()
	HandleAccountsAPI(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 clearing planTierOverride, got %d: %s", rec.Code, rec.Body.String())
	}
	if primCleared, ok := am.GetPrimaryAccount(); !ok || primCleared.PlanTierOverride != "" {
		t.Fatalf("expected PlanTierOverride cleared to empty, got %+v", primCleared)
	}
}

func TestUltra5xMonitorCapsUseConservativeProDefaults(t *testing.T) {
	softPro, drainPro, hardPro := defaultMonitorCapsForTier("pro")
	soft5x, drain5x, hard5x := defaultMonitorCapsForTier("ultra_5x")
	if soft5x != softPro || drain5x != drainPro || hard5x != hardPro {
		t.Fatalf("expected ultra_5x monitor caps (%d/%d/%d) to match conservative pro caps (%d/%d/%d)",
			soft5x, drain5x, hard5x, softPro, drainPro, hardPro)
	}
}
