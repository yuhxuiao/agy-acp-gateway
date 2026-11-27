package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestV2325_EffectivePoolMemoryBudgetHonorsGlobalCap(t *testing.T) {
	accs := []AccountProfile{
		{ID: "ultra-a", Primary: true, Enabled: true, MaxLiveWorkers: 16},
		{ID: "acc2", Enabled: true, MaxLiveWorkers: 8},
		{ID: "acc3", Enabled: true, MaxLiveWorkers: 8},
		{ID: "acc4", Enabled: true, MaxLiveWorkers: 5},
		{ID: "acc5", Enabled: true, MaxLiveWorkers: 5},
		{ID: "off1", Enabled: false, MaxLiveWorkers: 8},
	}
	tot, eff, estMB, limitMB := computeEffectivePoolMemoryBudget(accs, 8)
	if tot != 42 {
		t.Fatalf("expected totalMaxLiveWorkers=42, got %d", tot)
	}
	if eff != 8 {
		t.Fatalf("expected effectiveMaxLiveWorkers=8, got %d", eff)
	}
	wantEst := 8*estWorkerRssMB + 4*estChildGatewayRssMB + 256
	if estMB != wantEst {
		t.Fatalf("expected governed estPeakRssMB=%d, got %d", wantEst, estMB)
	}
	if limitMB <= 0 {
		t.Fatalf("expected positive hostMemLimitMB on Linux, got %d", limitMB)
	}
	// When globalCap == 0 (unbounded), estPeak falls back to sum of enabled slots.
	tot0, eff0, est0, _ := computeEffectivePoolMemoryBudget(accs, 0)
	if tot0 != 42 || eff0 != 42 || est0 != 42*estWorkerRssMB {
		t.Fatalf("expected unbounded fallback 42/42/%d, got %d/%d/%d", 42*estWorkerRssMB, tot0, eff0, est0)
	}
}

func TestV2325_ImageGenModelInAccountsWhitelistMigrationAndRouting(t *testing.T) {
	origMgr := GlobalAccountManager
	tmp := t.TempDir()
	accPath := filepath.Join(tmp, "accounts.json")
	t.Setenv("AGY_GW_HOME", tmp)
	t.Setenv("AGY_ACCOUNTS_FILE", accPath)
	t.Setenv("AGY_GLOBAL_MAX_LIVE_WORKERS", "8")
	t.Setenv("AGY_API_KEYS", "")
	initConfig()
	t.Cleanup(func() {
		GlobalAccountManager = origMgr
		initConfig()
	})

	// Seed a legacy v3 accounts.json whose primary has gemini-3.8-flash (without gemini-3.1-flash-image)
	// and a secondary account that only allows gemini-4-neon (no flash, no image).
	v3 := `{
  "version": 3,
  "accounts": [
    {"id":"primary","label":"Main","primary":true,"enabled":true,"maxLiveWorkers":12,"allowedModels":["gemini-4-carbon","gemini-3.8-flash"]},
    {"id":"noimg","label":"NoImg","primary":false,"enabled":true,"maxLiveWorkers":4,"allowedModels":["gemini-4-neon"]}
  ]
}`
	if err := os.WriteFile(accPath, []byte(v3), 0o600); err != nil {
		t.Fatal(err)
	}

	am := NewAccountManager()
	am.Init()
	GlobalAccountManager = am

	prim, ok := am.GetAccount("primary")
	if !ok || !prim.AllowsModel(imageGenServedModel) {
		t.Fatalf("v3->v4 migration must auto-add %s when %s is allowed, got %+v", imageGenServedModel, imageGenOuterModel, prim.AllowedModels)
	}
	noimg, ok := am.GetAccount("noimg")
	if !ok || noimg.AllowsModel(imageGenServedModel) {
		t.Fatalf("account without %s must NOT auto-receive %s, got %+v", imageGenOuterModel, imageGenServedModel, noimg.AllowedModels)
	}

	// Verify GET /api/accounts includes gemini-3.1-flash-image (kind=image) AND governed poolStats
	req := httptest.NewRequest(http.MethodGet, "/api/accounts", nil)
	rec := httptest.NewRecorder()
	HandleAccountsAPI(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/accounts status=%d body=%s", rec.Code, rec.Body.String())
	}
	var payload struct {
		OK              bool             `json:"ok"`
		PoolStats       map[string]any   `json:"poolStats"`
		AvailableModels []map[string]any `json:"availableModels"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal /api/accounts: %v", err)
	}
	foundImg := false
	for _, m := range payload.AvailableModels {
		if m["id"] == imageGenServedModel && m["kind"] == "image" {
			foundImg = true
		}
	}
	if !foundImg {
		t.Fatalf("expected %s (kind=image) in availableModels, got %+v", imageGenServedModel, payload.AvailableModels)
	}
	if eff, _ := payload.PoolStats["effectiveMaxLiveWorkers"].(float64); eff != 8 {
		t.Fatalf("expected effectiveMaxLiveWorkers=8 in /api/accounts poolStats, got %+v", payload.PoolStats["effectiveMaxLiveWorkers"])
	}
	if est, _ := payload.PoolStats["estPeakRssMB"].(float64); est != float64(8*estWorkerRssMB+1*estChildGatewayRssMB+256) {
		t.Fatalf("expected governed estPeakRssMB=%d, got %v", 8*estWorkerRssMB+1*estChildGatewayRssMB+256, est)
	}

	// Verify extractRequestRoutingHintsV2 maps /v1/images/generations to gemini-3.1-flash-image
	imgReq := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"prompt":"sunset"}`))
	hints := extractRequestRoutingHintsV2(imgReq)
	if hints.canonicalModel != imageGenServedModel {
		t.Fatalf("expected canonicalModel=%s for /v1/images/generations, got %q", imageGenServedModel, hints.canonicalModel)
	}

	// Verify /v1/models discovery exposes gemini-3.1-flash-image when allowed in account allowlist
	hasImageInModels := func(items []map[string]any) bool {
		for _, item := range items {
			if id, _ := item["id"].(string); id == imageGenServedModel {
				return true
			}
		}
		return false
	}
	modelsReq := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	filteredBefore, _ := am.FilterModelsForRequest(modelsReq, ListModelsForAPI())
	if !hasImageInModels(filteredBefore) {
		t.Fatalf("expected %s exposed in /v1/models when primary allows it, got %+v", imageGenServedModel, filteredBefore)
	}

	// Unchecking gemini-3.1-flash-image on primary must strictly disallow image generation and hide it from /v1/models!
	prim.AllowedModels = []string{"gemini-4-carbon", "gemini-3.8-flash"}
	saved, err := am.UpsertAccount(prim)
	if err != nil {
		t.Fatalf("UpsertAccount removing image model: %v", err)
	}
	if saved.AllowsModel(imageGenServedModel) {
		t.Fatalf("expected %s removed after explicit UpsertAccount, got %+v", imageGenServedModel, saved.AllowedModels)
	}
	filteredAfter, _ := am.FilterModelsForRequest(modelsReq, ListModelsForAPI())
	if hasImageInModels(filteredAfter) {
		t.Fatalf("expected %s hidden from /v1/models when unchecked on all accounts, got %+v", imageGenServedModel, filteredAfter)
	}
	res := am.ResolveTargetAccountEx(imgReq, imageGenServedModel, ResolveOptions{})
	if res.DenyErr == nil || res.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 when no account allows %s, got acc=%q status=%d err=%v", imageGenServedModel, res.Account.ID, res.StatusCode, res.DenyErr)
	}
}
