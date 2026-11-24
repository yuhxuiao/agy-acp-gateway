package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestA4_ProbeSlotIsolationProtectsWarmInteractiveWorkers(t *testing.T) {
	t.Cleanup(initConfig)
	t.Setenv("AGY_STATE_DB", "")
	initConfig()

	origMaxWorkers := MaxLiveWorkers
	origMaxProbes := MaxProbeWorkers
	origProbeRej := probeSlotRejectionsTotal.Load()
	origCapRej := workerCapRejectionsTotal.Load()
	defer func() {
		MaxLiveWorkers = origMaxWorkers
		MaxProbeWorkers = origMaxProbes
		probeSlotRejectionsTotal.Store(origProbeRej)
		workerCapRejectionsTotal.Store(origCapRej)
	}()

	t.Run("warm pool non-preemption rejects probe instead of evicting warm Carbon workers", func(t *testing.T) {
		MaxLiveWorkers = 2
		MaxProbeWorkers = 1
		gs := NewGatewayServer()
		defer gs.Close()

		warm1 := NewAcpProjectSession("sid:a4-warm-1", "/tmp", "")
		warm1.SessionID = "sess-a4-warm-1"
		warm1.ModelID = "gemini-4-carbon"
		warm1.LastCachedReadTokens = 280000
		warm1.LastUsedAt = time.Now()

		warm2 := NewAcpProjectSession("sid:a4-warm-2", "/tmp", "")
		warm2.SessionID = "sess-a4-warm-2"
		warm2.ModelID = "gemini-4-carbon"
		warm2.LastCachedReadTokens = 260000
		warm2.LastUsedAt = time.Now()

		gs.mu.Lock()
		gs.sessions[warm1.ConvKey] = warm1
		gs.sessions[warm2.ConvKey] = warm2
		gs.mu.Unlock()

		probe1 := NewAcpProjectSession("util:waf-bisect-1", "/tmp", "")
		probe1.isWafBisectProbe = true
		beforeProbeRej := probeSlotRejectionsTotal.Load()
		beforeCapRej := workerCapRejectionsTotal.Load()
		_, err := gs.reserveProbeWorkerSlot(probe1)
		var se *SoftError
		if !errors.As(err, &se) || se.StatusCode != 529 {
			t.Fatalf("expected 529 rejection instead of evicting warm Carbon workers, got err=%v", err)
		}
		if probeSlotRejectionsTotal.Load() != beforeProbeRej+1 || workerCapRejectionsTotal.Load() != beforeCapRej+1 {
			t.Fatalf("expected both rejection counters to increment, got probe=%d cap=%d", probeSlotRejectionsTotal.Load(), workerCapRejectionsTotal.Load())
		}
		warm1.mu.Lock()
		warm1Alive := warm1.SessionID == "sess-a4-warm-1"
		warm1.mu.Unlock()
		warm2.mu.Lock()
		warm2Alive := warm2.SessionID == "sess-a4-warm-2"
		warm2.mu.Unlock()
		if !warm1Alive || !warm2Alive {
			t.Fatal("reserveProbeWorkerSlot must NEVER evict warm interactive workers")
		}

		// Replace warm2 with a cold interactive worker; probe slot evicts ONLY the cold worker.
		warm2.mu.Lock()
		warm2.LastUsedAt = time.Now().Add(-10 * time.Minute)
		warm2.mu.Unlock()
		rel, err := gs.reserveProbeWorkerSlot(probe1)
		if err != nil {
			t.Fatalf("expected probe slot to evict cold worker and succeed, got %v", err)
		}
		if warm1.SessionID != "sess-a4-warm-1" || warm2.SessionID != "" {
			t.Fatalf("expected warm1 kept and cold warm2 evicted, got warm1=%q warm2=%q", warm1.SessionID, warm2.SessionID)
		}
		if gs.countPoolWorkers() != 1 || gs.countLiveWorkers() != 2 {
			t.Fatalf("expected pool=1 total=2, got pool=%d total=%d", gs.countPoolWorkers(), gs.countLiveWorkers())
		}
		rel()
	})

	t.Run("MaxProbeWorkers sub-budget caps concurrent probes while leaving pool slots free", func(t *testing.T) {
		MaxLiveWorkers = 4
		MaxProbeWorkers = 1
		gs := NewGatewayServer()
		defer gs.Close()

		warm := NewAcpProjectSession("sid:a4-warm-sub", "/tmp", "")
		warm.SessionID = "sess-a4-warm-sub"
		warm.ModelID = "gemini-4-carbon"
		warm.LastCachedReadTokens = 280000
		warm.LastUsedAt = time.Now()
		gs.mu.Lock()
		gs.sessions[warm.ConvKey] = warm
		gs.mu.Unlock()

		probe1 := NewAcpProjectSession("util:waf-bisect-sub1", "/tmp", "")
		probe1.isWafBisectProbe = true
		rel1, err := gs.reserveProbeWorkerSlot(probe1)
		if err != nil {
			t.Fatalf("expected 1st probe slot to succeed, got %v", err)
		}
		if gs.countPoolWorkers() != 1 || gs.countLiveWorkers() != 2 {
			t.Fatalf("unexpected worker counts: pool=%d live=%d", gs.countPoolWorkers(), gs.countLiveWorkers())
		}

		probe2 := NewAcpProjectSession("util:waf-bisect-sub2", "/tmp", "")
		probe2.isWafBisectProbe = true
		beforeRej := probeSlotRejectionsTotal.Load()
		_, err2 := gs.reserveProbeWorkerSlot(probe2)
		var se *SoftError
		if !errors.As(err2, &se) || se.StatusCode != 529 {
			t.Fatalf("expected 2nd concurrent probe slot to be rejected at MaxProbeWorkers=1, got %v", err2)
		}
		if probeSlotRejectionsTotal.Load() != beforeRej+1 {
			t.Fatalf("expected probeSlotRejectionsTotal to increment from %d, got %d", beforeRej, probeSlotRejectionsTotal.Load())
		}

		// Remaining 2 slots in MaxLiveWorkers=4 remain free for interactive sessions.
		inter := NewAcpProjectSession("sid:a4-interactive-free", "/tmp", "")
		if err := gs.reserveWorkerSlot(inter); err != nil {
			t.Fatalf("expected interactive session to admit into remaining capacity, got %v", err)
		}
		inter.mu.Lock()
		inter.spawning = false
		inter.mu.Unlock()

		rel1()
		probe1.mu.Lock()
		spawningAfterRel := probe1.spawning
		probe1.mu.Unlock()
		if spawningAfterRel {
			t.Fatal("expected probe slot releaser to clear probe1.spawning")
		}

		rel2, err3 := gs.reserveProbeWorkerSlot(probe2)
		if err3 != nil {
			t.Fatalf("expected probe slot to become available after release, got %v", err3)
		}
		rel2()

		metrics := RenderPrometheusMetrics(gs)
		if !strings.Contains(metrics, "agy_gateway_probe_slot_rejections_total ") {
			t.Fatalf("expected Prometheus output to include agy_gateway_probe_slot_rejections_total, got:\n%s", metrics)
		}
	})
}

func TestA4_UtilSessionNonPreemptiveAdmission(t *testing.T) {
	t.Cleanup(initConfig)
	t.Setenv("AGY_STATE_DB", "")
	initConfig()

	origMax := MaxLiveWorkers
	MaxLiveWorkers = 2
	defer func() { MaxLiveWorkers = origMax }()

	t.Run("rejects util admission instead of evicting warm or parked interactive workers", func(t *testing.T) {
		gs := NewGatewayServer()
		defer gs.Close()

		warm := NewAcpProjectSession("sid:a4-warm", "/tmp", "")
		warm.SessionID = "sess-warm"
		warm.ModelID = "gemini-4-carbon"
		warm.LastCachedReadTokens = 120000
		warm.LastUsedAt = time.Now()

		parked := NewAcpProjectSession("sid:a4-parked", "/tmp", "")
		parked.SessionID = "sess-parked"
		parked.ModelID = "gemini-4-hydrogen"
		parked.LastUsedAt = time.Now().Add(-10 * time.Minute)
		parked.orphanRelayHops = 10
		parked.ParkedCalls["call_1"] = &ParkedMcpCall{ResultChan: make(chan map[string]any, 1)}

		utilIn := NewAcpProjectSession("util:helper-1", "/tmp", "")
		gs.mu.Lock()
		gs.sessions[warm.ConvKey] = warm
		gs.sessions[parked.ConvKey] = parked
		gs.sessions[utilIn.ConvKey] = utilIn
		gs.mu.Unlock()

		beforeRej := probeSlotRejectionsTotal.Load()
		err := gs.reserveWorkerSlot(utilIn)
		var se *SoftError
		if !errors.As(err, &se) || se.StatusCode != 529 {
			t.Fatalf("expected 529 rejection for util session against warm+parked pool, got %v", err)
		}
		if probeSlotRejectionsTotal.Load() != beforeRej+1 {
			t.Fatalf("expected probeSlotRejectionsTotal to increment for rejected util session")
		}
		if warm.SessionID != "sess-warm" || parked.SessionID != "sess-parked" {
			t.Fatal("util admission must NOT evict warm or parked interactive sessions")
		}
	})

	t.Run("util admission evicts only cold or idle util workers", func(t *testing.T) {
		gs := NewGatewayServer()
		defer gs.Close()

		warm := NewAcpProjectSession("sid:a4-warm-keep", "/tmp", "")
		warm.SessionID = "sess-warm-keep"
		warm.ModelID = "gemini-4-carbon"
		warm.LastCachedReadTokens = 200000
		warm.LastUsedAt = time.Now()

		cold := NewAcpProjectSession("sid:a4-cold-victim", "/tmp", "")
		cold.SessionID = "sess-cold-victim"
		cold.ModelID = "gemini-3.8-flash"
		cold.LastCachedReadTokens = 5000
		cold.LastUsedAt = time.Now().Add(-10 * time.Minute)

		utilIn := NewAcpProjectSession("util:helper-2", "/tmp", "")
		gs.mu.Lock()
		gs.sessions[warm.ConvKey] = warm
		gs.sessions[cold.ConvKey] = cold
		gs.sessions[utilIn.ConvKey] = utilIn
		gs.mu.Unlock()

		if err := gs.reserveWorkerSlot(utilIn); err != nil {
			t.Fatalf("expected util admission to evict cold worker and succeed, got %v", err)
		}
		utilIn.mu.Lock()
		utilIn.spawning = false
		utilIn.mu.Unlock()

		if warm.SessionID != "sess-warm-keep" {
			t.Fatal("warm interactive worker should have been preserved")
		}
		if cold.SessionID != "" {
			t.Fatalf("expected cold worker to be evicted for util admission, still has sid=%q", cold.SessionID)
		}
	})
}

func TestA4_ReapIdleWorkersPreservesEngineWhileProbeActive(t *testing.T) {
	tmpDir := t.TempDir()
	t.Cleanup(initConfig)
	t.Setenv("AGY_ACP_BIN", fakeAcpBin)
	t.Setenv("AGY_PURE_API_HOME", filepath.Join(tmpDir, "pure-api-home"))
	t.Setenv("AGY_STATE_DB", "")
	initConfig()

	GlobalEngine.StopEngine()
	defer GlobalEngine.StopEngine()

	if err := GlobalEngine.EnsureStarted(); err != nil {
		t.Fatalf("EnsureStarted fakeacp failed: %v", err)
	}
	if !GlobalEngine.IsAlive() {
		t.Fatal("expected GlobalEngine to be alive")
	}

	gs := NewGatewayServer()
	defer gs.Close()

	probe := NewAcpProjectSession("util:waf-bisect-reap", "/tmp", "")
	probe.isWafBisectProbe = true
	rel, err := gs.reserveProbeWorkerSlot(probe)
	if err != nil {
		t.Fatalf("reserveProbeWorkerSlot failed: %v", err)
	}

	gs.reapIdleWorkers()
	if !GlobalEngine.IsAlive() {
		t.Fatal("reapIdleWorkers stopped shared ACP engine while a WAF bisect probe held an active probe slot")
	}

	rel()
	gs.reapIdleWorkers()
	if GlobalEngine.IsAlive() {
		t.Fatal("expected reapIdleWorkers to stop shared ACP engine once pool and probe slots reached 0")
	}
	_ = http.StatusOK
}

func TestA4_ClaudePermissionClassifierIsolatedAsNonPreemptiveUtil(t *testing.T) {
	t.Cleanup(initConfig)
	t.Setenv("AGY_STATE_DB", "")
	initConfig()

	origMaxWorkers := MaxLiveWorkers
	defer func() { MaxLiveWorkers = origMaxWorkers }()

	classifierSys := "You are a security monitor for autonomous AI coding agents.\n\n## Context\n" + strings.Repeat("policy rule line\n", 8000) // ~136KB (>16K)
	sharedPrefix := strings.Repeat("header context ", 60)                                                                                       // >512 chars
	msgsA := []map[string]any{
		{"role": "user", "content": sharedPrefix + "Action: Bash(ls -la)"},
		{"role": "assistant", "content": "<reasoning>"},
		{"role": "user", "content": "Evaluate this action."},
	}
	msgsB := []map[string]any{
		{"role": "user", "content": sharedPrefix + "Action: Bash(rm -rf /tmp/foo)"},
		{"role": "assistant", "content": "<reasoning>"},
		{"role": "user", "content": "Evaluate this action."},
	}

	req := httptest.NewRequest("POST", "/v1/messages", nil)
	req.Header.Set("x-claude-code-session-id", "60e77fce-6505-4b5e-863d-13b4d5dc716f")
	req.Header.Set("User-Agent", "claude-cli/2.1.323")
	body := map[string]any{"max_tokens": 512}

	kA := deriveConversationKey(req, body, classifierSys, nil, msgsA)
	kB := deriveConversationKey(req, body, classifierSys, nil, msgsB)
	if !strings.HasPrefix(kA, "util:") || !strings.HasPrefix(kB, "util:") {
		t.Fatalf("expected Claude permission classifier to map to util:, got kA=%q kB=%q", kA, kB)
	}
	if kA == kB {
		t.Fatalf("distinct permission classifier actions with >512-char shared prefix must not collide: %q", kA)
	}

	// Session naming sidecar with explicit session ID and 0 tools also maps to util:.
	namingSys := "You are naming a coding session so the user can pick it out of a long list of sessions. The title is a short noun phrase."
	kName := deriveConversationKey(req, body, namingSys, nil, []map[string]any{{"role": "user", "content": "Fix auth bug"}})
	if !strings.HasPrefix(kName, "util:") {
		t.Fatalf("expected session naming sidecar to map to util:, got %q", kName)
	}

	// Negative guards: with tools, >4 messages, or normal system prompt must preserve sid:.
	kWithTools := deriveConversationKey(req, body, classifierSys, []ToolDef{{Name: "Bash"}}, msgsA)
	if !strings.HasPrefix(kWithTools, "sid:60e77fce-6505-4b5e-863d-13b4d5dc716f:") {
		t.Fatalf("request with tools must not be classified as util:, got %q", kWithTools)
	}
	msgsLong := append(append([]map[string]any{}, msgsA...),
		map[string]any{"role": "assistant", "content": "<block>no</block>"},
		map[string]any{"role": "user", "content": "Follow-up turn"},
	)
	kLong := deriveConversationKey(req, body, classifierSys, nil, msgsLong)
	if !strings.HasPrefix(kLong, "sid:60e77fce-6505-4b5e-863d-13b4d5dc716f:") {
		t.Fatalf("multi-turn (>4 msgs) conversation must not be classified as util:, got %q", kLong)
	}

	// Verify derived util: classifier session NEVER evicts warm interactive workers when pool is full.
	MaxLiveWorkers = 2
	gs := NewGatewayServer()
	defer gs.Close()

	warm1 := NewAcpProjectSession("sid:main-1", "/tmp", "")
	warm1.SessionID = "sess-main-1"
	warm1.ModelID = "gemini-4-hydrogen"
	warm1.LastCachedReadTokens = 180000
	warm1.LastUsedAt = time.Now()

	warm2 := NewAcpProjectSession("sid:main-2", "/tmp", "")
	warm2.SessionID = "sess-main-2"
	warm2.ModelID = "gemini-4-neon"
	warm2.LastCachedReadTokens = 150000
	warm2.LastUsedAt = time.Now()

	clsSess := NewAcpProjectSession(kA, "/tmp", "")
	gs.mu.Lock()
	gs.sessions[warm1.ConvKey] = warm1
	gs.sessions[warm2.ConvKey] = warm2
	gs.sessions[clsSess.ConvKey] = clsSess
	gs.mu.Unlock()

	err := gs.reserveWorkerSlot(clsSess)
	var se *SoftError
	if !errors.As(err, &se) || se.StatusCode != 529 {
		t.Fatalf("expected 529 when pool is full of warm interactive workers, got err=%v", err)
	}
	if warm1.SessionID != "sess-main-1" || warm2.SessionID != "sess-main-2" {
		t.Fatalf("permission classifier must NEVER evict warm interactive workers, got warm1=%q warm2=%q", warm1.SessionID, warm2.SessionID)
	}

	// Once a previous finished util: classifier occupies a slot, a new classifier reuses its slot cleanly.
	warm2.mu.Lock()
	warm2.SessionID = ""
	warm2.mu.Unlock()
	prevCls := NewAcpProjectSession(kName, "/tmp", "")
	prevCls.SessionID = "sess-prev-util"
	prevCls.LastUsedAt = time.Now()
	gs.mu.Lock()
	gs.sessions[prevCls.ConvKey] = prevCls
	gs.mu.Unlock()

	if err := gs.reserveWorkerSlot(clsSess); err != nil {
		t.Fatalf("expected new classifier to evict finished util: session and succeed, got %v", err)
	}
	clsSess.mu.Lock()
	clsSess.spawning = false
	clsSess.mu.Unlock()
	if warm1.SessionID != "sess-main-1" || prevCls.SessionID != "" {
		t.Fatalf("expected warm1 kept and prevCls evicted, got warm1=%q prevCls=%q", warm1.SessionID, prevCls.SessionID)
	}
}
