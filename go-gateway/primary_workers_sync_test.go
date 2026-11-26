package main

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestPrimaryMaxLiveWorkersSyncOnInitAndLiveUpsert(t *testing.T) {
	origAM := GlobalAccountManager
	origMax := MaxLiveWorkers
	origBoot := maxLiveWorkersBoot
	t.Cleanup(func() {
		if GlobalAccountManager != nil {
			GlobalAccountManager.StopAll()
		}
		GlobalAccountManager = origAM
		MaxLiveWorkers = origMax
		maxLiveWorkersBoot = origBoot
		initConfig()
	})

	tmp := t.TempDir()
	accPath := filepath.Join(tmp, "accounts.json")
	raw := `{
  "version": 3,
  "routing": { "strategy": "weighted-rr" },
  "accounts": [
    {
      "id": "tr20x",
      "label": "Primary 20x",
      "primary": true,
      "enabled": true,
      "schedState": "active",
      "maxLiveWorkers": 16,
      "proxyPreset": "direct",
      "boundKeys": [],
      "allowedModels": []
    }
  ]
}`
	if err := os.WriteFile(accPath, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Setenv("AGY_ACCOUNTS_FILE", accPath)
	t.Setenv("AGY_MAX_LIVE_WORKERS", "8")
	t.Setenv("AGY_CHILD_ACCOUNT_WORKER", "")
	initConfig()

	GlobalAccountManager = NewAccountManager()
	GlobalAccountManager.Init()

	// 1. Startup Init syncs Primary maxLiveWorkers=16 into runtime effective cap
	if got := effectiveMaxLiveWorkers(); got != 16 {
		t.Fatalf("effectiveMaxLiveWorkers after Init = %d, want 16", got)
	}

	// 2. Dashboard, /api/status, and Prometheus metrics all reflect 16
	gw := NewGatewayServer()
	t.Cleanup(gw.Close)
	st := gw.GetStats()
	if got := mapInt64(st, "maxLiveWorkers"); got != 16 {
		t.Fatalf("GetStats maxLiveWorkers = %d, want 16", got)
	}
	metrics := RenderPrometheusMetrics(gw)
	if !strings.Contains(metrics, "agy_gateway_max_live_workers 16\n") {
		t.Fatalf("expected Prometheus gauge agy_gateway_max_live_workers 16, got:\n%s", metrics)
	}

	// 3. UpsertAccount with MaxLiveWorkers=0 preserves Primary's 16 instead of downgrading to 3
	prim, ok := GlobalAccountManager.GetAccount("tr20x")
	if !ok {
		t.Fatalf("primary account tr20x missing")
	}
	prim.MaxLiveWorkers = 0
	saved, err := GlobalAccountManager.UpsertAccount(prim)
	if err != nil {
		t.Fatalf("UpsertAccount(0): %v", err)
	}
	if saved.MaxLiveWorkers != 16 || effectiveMaxLiveWorkers() != 16 {
		t.Fatalf("UpsertAccount(0) downgraded Primary: saved=%d effective=%d, want 16", saved.MaxLiveWorkers, effectiveMaxLiveWorkers())
	}

	// 4. Live UpsertAccount updating Primary maxLiveWorkers to 12 takes effect immediately
	prim.MaxLiveWorkers = 12
	if _, err := GlobalAccountManager.UpsertAccount(prim); err != nil {
		t.Fatalf("UpsertAccount(12): %v", err)
	}
	if got := effectiveMaxLiveWorkers(); got != 12 {
		t.Fatalf("effectiveMaxLiveWorkers after live edit = %d, want 12", got)
	}

	// 5. Concurrent reads and writes remain race-free under -race
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 40; i++ {
			_ = effectiveMaxLiveWorkers()
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			p := prim
			if i%2 == 0 {
				p.MaxLiveWorkers = 14
			} else {
				p.MaxLiveWorkers = 12
			}
			_, _ = GlobalAccountManager.UpsertAccount(p)
		}
	}()
	wg.Wait()

	// 6. Direct test override of package var MaxLiveWorkers still takes precedence
	MaxLiveWorkers = 2
	if got := effectiveMaxLiveWorkers(); got != 2 {
		t.Fatalf("direct MaxLiveWorkers override = %d, want 2", got)
	}
	MaxLiveWorkers = maxLiveWorkersBoot

	// 7. Child account worker mode ignores parent primaryMaxLiveWorkers and uses its own env cap
	t.Setenv("AGY_CHILD_ACCOUNT_WORKER", "1")
	t.Setenv("AGY_MAX_LIVE_WORKERS", "1")
	initConfig()
	GlobalAccountManager.primaryMaxLiveWorkers.Store(16)
	if got := effectiveMaxLiveWorkers(); got != 1 {
		t.Fatalf("child worker effectiveMaxLiveWorkers = %d, want 1", got)
	}
}

func TestFederatedDashboardEnrichesAllowedModelsCount(t *testing.T) {
	origAM := GlobalAccountManager
	t.Cleanup(func() {
		if GlobalAccountManager != nil {
			GlobalAccountManager.StopAll()
		}
		GlobalAccountManager = origAM
		initConfig()
	})

	tmp := t.TempDir()
	accPath := filepath.Join(tmp, "accounts.json")
	raw := `{
  "version": 3,
  "routing": { "strategy": "weighted-rr" },
  "accounts": [
    {
      "id": "tr20x",
      "label": "Primary 20x",
      "primary": true,
      "enabled": true,
      "schedState": "active",
      "maxLiveWorkers": 16,
      "proxyPreset": "direct",
      "boundKeys": [],
      "allowedModels": []
    }
  ]
}`
	if err := os.WriteFile(accPath, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGY_ACCOUNTS_FILE", accPath)
	t.Setenv("AGY_PURE_API_HOME", filepath.Join(tmp, "pure"))
	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "state.db"))
	initConfig()

	GlobalAccountManager = NewAccountManager()
	GlobalAccountManager.Init()

	gw := NewGatewayServer()
	t.Cleanup(gw.Close)
	payload := BuildFederatedDashboardPayload(gw, false)
	insts, _ := payload["instances"].([]map[string]any)
	if len(insts) != 1 {
		t.Fatalf("expected 1 federated instance, got %d", len(insts))
	}
	if got := insts[0]["allowedModelsCount"]; got != 0 {
		t.Fatalf("allowedModelsCount = %v, want 0", got)
	}
	gwMap, _ := insts[0]["gateway"].(map[string]any)
	if got := mapInt64(gwMap, "maxLiveWorkers"); got != 16 {
		t.Fatalf("federated self gateway maxLiveWorkers = %d, want 16", got)
	}
}
