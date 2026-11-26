package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	gwcfg "agy-acp-gateway-go/internal/config"
)

func TestDynamicPrimaryPromptLimiterLiveResize(t *testing.T) {
	prevDB := GatewayStateDB
	prevAM := GlobalAccountManager
	t.Cleanup(func() {
		GlobalAccountManager = prevAM
		if GlobalAccountManager != nil {
			GlobalAccountManager.primaryMaxLiveWorkers.Store(0)
		}
		initConfig()
		InitGlobalStore(prevDB)
	})

	tmp := t.TempDir()
	accPath := filepath.Join(tmp, "accounts.json")
	seed := `{
  "version": 3,
  "accounts": [
    {
      "id": "primary",
      "label": "Primary",
      "primary": true,
      "enabled": true,
      "maxLiveWorkers": 8,
      "allowedModels": ["gemini-4-neon"]
    }
  ]
}`
	if err := os.WriteFile(accPath, []byte(seed), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Setenv("AGY_MAX_LIVE_WORKERS", "8")
	t.Setenv("AGY_MAX_CONCURRENT_PROMPTS", "")
	t.Setenv("AGY_ACCOUNTS_FILE", accPath)
	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "gateway.db"))
	t.Setenv("AGY_GW_LOG", filepath.Join(tmp, "gateway.log"))
	t.Setenv("AGY_ACCOUNT_WORKER", "")

	initConfig()
	am := NewAccountManager()
	GlobalAccountManager = am
	am.Init()
	syncPrimaryPromptSemBeforeServe()

	if got := effectiveMaxConcurrentPrompts(); got != 8 {
		t.Fatalf("initial effectiveMaxConcurrentPrompts = %d, want 8", got)
	}

	// Acquire all 8 slots.
	var releases []func()
	for i := 0; i < 8; i++ {
		rel, err := acquirePromptSlot(context.Background())
		if err != nil {
			t.Fatalf("acquire slot %d failed: %v", i, err)
		}
		releases = append(releases, rel)
	}
	if got := promptsInFlightCount(); got != 8 {
		t.Fatalf("promptsInFlightCount = %d, want 8", got)
	}

	// 9th acquisition must block until limit scales up live.
	woke9 := make(chan func(), 1)
	err9 := make(chan error, 1)
	go func() {
		rel, err := acquirePromptSlot(context.Background())
		if err != nil {
			err9 <- err
			return
		}
		woke9 <- rel
	}()

	select {
	case <-woke9:
		t.Fatal("9th slot should block while limit is 8")
	case err := <-err9:
		t.Fatalf("unexpected error on 9th waiter: %v", err)
	case <-time.After(35 * time.Millisecond):
	}

	// Live edit Primary maxLiveWorkers 8 -> 16 via UpsertAccount (NO restart).
	prim, ok := am.GetAccount("primary")
	if !ok {
		t.Fatal("primary account missing")
	}
	prim.MaxLiveWorkers = 16
	if _, err := am.UpsertAccount(prim); err != nil {
		t.Fatalf("UpsertAccount(16) failed: %v", err)
	}
	if got := effectiveMaxLiveWorkers(); got != 16 {
		t.Fatalf("effectiveMaxLiveWorkers after live edit = %d, want 16", got)
	}
	if got := effectiveMaxConcurrentPrompts(); got != 16 {
		t.Fatalf("effectiveMaxConcurrentPrompts after live edit = %d, want 16", got)
	}

	// Blocked 9th waiter must wake up immediately.
	select {
	case rel9 := <-woke9:
		releases = append(releases, rel9)
	case err := <-err9:
		t.Fatalf("9th waiter errored: %v", err)
	case <-time.After(time.Second):
		t.Fatal("9th waiter did not wake after live UpsertAccount(16)")
	}

	// Acquire slots 10..16; 17th must time out cleanly without leaking.
	for i := 9; i < 16; i++ {
		rel, err := acquirePromptSlot(context.Background())
		if err != nil {
			t.Fatalf("acquire slot %d after live resize failed: %v", i+1, err)
		}
		releases = append(releases, rel)
	}
	if got := promptsInFlightCount(); got != 16 {
		t.Fatalf("promptsInFlightCount at full 16 = %d, want 16", got)
	}

	ctx17, cancel17 := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel17()
	if _, err := acquirePromptSlot(ctx17); err == nil {
		t.Fatal("17th slot should block at limit=16")
	}

	// Verify /metrics & dashboard report the live resized limit (16) and in-flight count (16).
	gs := NewGatewayServer()
	t.Cleanup(gs.Close)
	metricsOut := RenderPrometheusMetrics(gs)
	if !strings.Contains(metricsOut, "agy_gateway_max_concurrent_prompts 16\n") {
		t.Fatalf("expected agy_gateway_max_concurrent_prompts 16 in metrics:\n%s", metricsOut)
	}
	if !strings.Contains(metricsOut, "agy_gateway_prompts_inflight 16\n") {
		t.Fatalf("expected agy_gateway_prompts_inflight 16 in metrics:\n%s", metricsOut)
	}
	st := gs.GetStats()
	if st["maxConcurrentPrompts"] != 16 || st["promptsInFlight"] != 16 {
		t.Fatalf("GetStats mismatch: maxConcurrentPrompts=%v promptsInFlight=%v", st["maxConcurrentPrompts"], st["promptsInFlight"])
	}

	// Live scale DOWN 16 -> 10 while 16 are in-flight: no new slot admitted until < 10.
	prim.MaxLiveWorkers = 10
	if _, err := am.UpsertAccount(prim); err != nil {
		t.Fatalf("UpsertAccount(10) failed: %v", err)
	}
	if got := effectiveMaxConcurrentPrompts(); got != 10 {
		t.Fatalf("effectiveMaxConcurrentPrompts after scale down = %d, want 10", got)
	}
	// Release 5 (still 11 in-flight >= 10) -> must still block.
	for i := 0; i < 5; i++ {
		releases[i]()
	}
	releases = releases[5:]
	ctxBlocked, cancelBlocked := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancelBlocked()
	if _, err := acquirePromptSlot(ctxBlocked); err == nil {
		t.Fatal("slot should remain blocked while 11 in-flight > resized limit 10")
	}

	// Release remaining 11 and ensure clean zero in-flight.
	for _, rel := range releases {
		rel()
	}
	if got := promptsInFlightCount(); got != 0 {
		t.Fatalf("promptsInFlightCount after releasing all = %d, want 0", got)
	}

	// Race test: concurrent live resizes + acquire/release cycles under -race.
	var wg sync.WaitGroup
	for g := 0; g < 12; g++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				rel, err := acquirePromptSlot(ctx)
				cancel()
				if err == nil {
					_ = promptsInFlightCount()
					_ = effectiveMaxConcurrentPrompts()
					rel()
				}
			}
		}(g)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			p := prim
			if i%2 == 0 {
				p.MaxLiveWorkers = 16
			} else {
				p.MaxLiveWorkers = 8
			}
			_, _ = am.UpsertAccount(p)
		}
	}()
	wg.Wait()
	if got := promptsInFlightCount(); got != 0 {
		t.Fatalf("promptsInFlightCount leaked after concurrent stress: %d", got)
	}

	// Explicit AGY_MAX_CONCURRENT_PROMPTS env pin must NOT be overridden by live UpsertAccount.
	t.Setenv("AGY_MAX_CONCURRENT_PROMPTS", "5")
	initConfig()
	am2 := NewAccountManager()
	GlobalAccountManager = am2
	am2.Init()
	syncPrimaryPromptSemBeforeServe()
	p2, _ := am2.GetAccount("primary")
	p2.MaxLiveWorkers = 16
	_, _ = am2.UpsertAccount(p2)
	if got := effectiveMaxConcurrentPrompts(); got != 5 {
		t.Fatalf("explicit AGY_MAX_CONCURRENT_PROMPTS=5 overridden to %d", got)
	}
}

func TestParkedInterruptResumeDrainsSequentialPreDispatchedSiblingsWithoutFalseToolUse(t *testing.T) {
	setupM2IsolatedEnv(t)

	sess := NewAcpProjectSession("sid:v2313-sib-abort:u:1", "/tmp", "")
	sid := "00000000-0000-4000-8000-000000009913"
	sess.SessionID = sid
	sess.ModelID = "gemini-4-neon"
	sess.ThoughtLevel = "xhigh"
	sess.MessageCount = 4
	sess.lastTurnAssistant = true
	sess.HistoryFingerprint = computeFirstUserFingerprint([]map[string]any{
		{"role": "user", "content": "hello"},
		{"role": "assistant", "content": "hi"},
		{"role": "user", "content": "inspect three files"},
	})
	readMcp := mcpToolName("Read")
	bashMcp := mcpToolName("Bash")
	sess.DeclaredMcpTools = map[string]bool{"Read": true, "Bash": true}
	sess.McpToHostMap = map[string]string{readMcp: "Read", bashMcp: "Bash"}
	sess.ToolsCatalog = []ToolDef{
		{Name: "Read", McpName: readMcp},
		{Name: "Bash", McpName: bashMcp},
	}
	sess.ToolFingerprint = toolCatalogFingerprint(sess.ToolsCatalog)

	// Step 1 contains 3 parallel tool calls; agy-acp-900k executes them sequentially over MCP.
	dbPath := filepath.Join(ConversationsDir(), sid+".db")
	writeTestPlannerStepSQLite(t, dbPath, 1, buildTestPlannerStepPayload([][3]string{
		{"call_1", readMcp, `{"file_path":"/a.txt"}`},
		{"call_2", readMcp, `{"file_path":"/b.txt"}`},
		{"call_3", bashMcp, `{"command":"ls -la"}`},
	}))

	turn1 := newActiveTurnState(nil, func() {})
	sess.mu.Lock()
	sess.PromptInFlight = true
	sess.activeTurn = turn1
	sess.mu.Unlock()

	sibResults := make(chan []map[string]any, 1)
	go func() {
		// 1. agy invokes call #1 -> blocks awaiting client while #2 and #3 are pre-dispatched.
		r1 := sess.executeMcpToolCall(context.Background(), "Read", map[string]any{"file_path": "/a.txt"})
		// 2. Real agy-acp-900k behavior: after call #1 returns the abort/compaction prompt,
		//    agy STILL sequentially invokes remaining siblings #2 and #3 from step 1 BEFORE
		//    sending all 3 function responses to the LLM to generate the compaction summary!
		r2 := sess.executeMcpToolCall(context.Background(), "Read", map[string]any{"file_path": "/b.txt"})
		r3 := sess.executeMcpToolCall(context.Background(), "Bash", map[string]any{"command": "ls -la"})
		sibResults <- []map[string]any{r1, r2, r3}

		// 3. After draining #2 and #3 without parking, upstream LLM generates step 2 (compaction summary).
		sess.mu.Lock()
		sess.PromptInFlight = false
		cur := sess.activeTurn
		sess.activeTurn = nil
		sess.mu.Unlock()
		if cur != nil {
			cur.mu.Lock()
			alreadyResolved := cur.resolved
			cur.resolved = true
			cur.mu.Unlock()
			if !alreadyResolved {
				cur.closeDelta()
				cur.resolveChan <- TurnResult{
					StopReason: "end_turn",
					Text:       "COMPACTION_SUMMARY_CACHE_PRESERVED_OK",
				}
			}
		}
	}()

	var res1 TurnResult
	select {
	case res1 = <-turn1.resolveChan:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for batched 3-sibling turn 1")
	}
	if res1.StopReason != "tool_use" || len(res1.ToolCalls) != 3 {
		t.Fatalf("expected 3 batched sibling tool calls, got %+v", res1)
	}

	// Client sends compaction summary request with compMergedTail (N -> N-1 messages, tool_result + compaction text)
	// WITHOUT tool_choice=none (exact production Claude Code / workspace agent shape).
	msgsComp := []map[string]any{
		{"role": "user", "content": "hello"},
		{"role": "assistant", "content": "hi"},
		{
			"role": "user",
			"content": []any{
				map[string]any{"type": "tool_result", "tool_use_id": "prev_consumed_id", "content": "old tool output"},
				map[string]any{"type": "text", "text": "Your task is to create a detailed summary of the conversation so far, paying close attention to the user's explicit requests and your previous actions."},
			},
		},
	}

	res2, err2 := sess.RunTurn(context.Background(), msgsComp, sess.ToolsCatalog, "xhigh", nil, nil)
	if err2 != nil {
		t.Fatalf("RunTurn compaction interrupt failed: %v", err2)
	}
	if res2.StopReason != "end_turn" || !strings.Contains(res2.Text, "COMPACTION_SUMMARY_CACHE_PRESERVED_OK") {
		t.Fatalf("expected clean end_turn compaction summary (NOT bogus tool_use from sibling #2), got stop=%q tools=%d text=%q",
			res2.StopReason, len(res2.ToolCalls), res2.Text)
	}

	gotSibs := <-sibResults
	if !strings.Contains(fmt.Sprint(gotSibs[0]["content"]), "detailed summary of the conversation") {
		t.Fatalf("call #1 did not receive compaction prompt: %+v", gotSibs[0])
	}
	for idx := 1; idx <= 2; idx++ {
		if isErr, _ := gotSibs[idx]["isError"].(bool); !isErr {
			t.Fatalf("aborted sibling #%d should return isError=true, got %+v", idx+1, gotSibs[idx])
		}
		if !strings.Contains(fmt.Sprint(gotSibs[idx]["content"]), "Host user interrupted") {
			t.Fatalf("aborted sibling #%d should return short interrupt error, got %+v", idx+1, gotSibs[idx])
		}
	}

	sess.mu.Lock()
	if sess.SessionID != sid || sess.CompactionRollovers != 0 || len(sess.ParkedCalls) != 0 || len(sess.PendingSiblingCalls) != 0 || len(sess.abortedSiblingCalls) != 0 {
		sess.mu.Unlock()
		t.Fatalf("unexpected post-compaction state: sid=%q rolls=%d parked=%d pending=%d aborted=%d",
			sess.SessionID, sess.CompactionRollovers, len(sess.ParkedCalls), len(sess.PendingSiblingCalls), len(sess.abortedSiblingCalls))
	}
	sess.mu.Unlock()
}

func TestAbortedSiblingDrainPrunesOnNewPlannerStep(t *testing.T) {
	setupM2IsolatedEnv(t)

	sess := NewAcpProjectSession("sid:v2313-sib-prune:u:1", "/tmp", "")
	sid := "00000000-0000-4000-8000-000000009914"
	sess.SessionID = sid
	readMcp := mcpToolName("Read")
	sess.DeclaredMcpTools = map[string]bool{"Read": true}
	sess.McpToHostMap = map[string]string{readMcp: "Read"}
	sess.ToolsCatalog = []ToolDef{{Name: "Read", McpName: readMcp}}

	dbPath := filepath.Join(ConversationsDir(), sid+".db")
	// Step 2 is now the latest step in SQLite (LLM already moved past step 1).
	writeTestPlannerStepSQLite(t, dbPath, 2, buildTestPlannerStepPayload([][3]string{
		{"call_new", readMcp, `{"file_path":"/b.txt"}`},
	}))

	turn := newActiveTurnState(nil, func() {})
	sess.mu.Lock()
	sess.PromptInFlight = true
	sess.activeTurn = turn
	sess.abortedSiblingCalls = []*ParkedMcpCall{{
		Call:         ToolCallItem{ID: "acp_old_sib", Name: "Read", Input: map[string]any{"file_path": "/b.txt"}},
		SessionID:    sid,
		PlannerIdx:   1,
		CanonicalSig: `Read:{"file_path":"/b.txt"}`,
	}}
	sess.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = sess.executeMcpToolCall(ctx, "Read", map[string]any{"file_path": "/b.txt"})
	}()

	select {
	case res := <-turn.resolveChan:
		if res.StopReason != "tool_use" || len(res.ToolCalls) != 1 || res.ToolCalls[0].Name != "Read" {
			t.Fatalf("expected legitimate step-2 Read to park normally, got %+v", res)
		}
	case <-time.After(time.Second):
		t.Fatal("legitimate step-2 Read was wrongly swallowed by stale step-1 abortedSiblingCalls")
	}
	cancel()
	<-done
}

func TestCollectDashboardTelemetryTop60AndGwSids(t *testing.T) {
	gs := NewGatewayServer()
	t.Cleanup(gs.Close)

	// Seed 200 detached sessions x 50 turns = 10,000 turns (mirrors large production child DBs).
	gs.mu.Lock()
	gs.sessions = make(map[string]*AcpProjectSession, 200)
	for sIdx := 0; sIdx < 200; sIdx++ {
		convKey := fmt.Sprintf("sid:sess-%03d:u:1", sIdx)
		sess := NewAcpProjectSession(convKey, "/tmp", "")
		sess.ClientID = "claude-code"
		sess.ClientLabel = "Claude Code"
		sess.Protocol = "Messages"
		sess.TurnHistory = make([]TurnHistoryEntry, 0, 50)
		for tIdx := 1; tIdx <= 50; tIdx++ {
			tsMs := int64(1_700_000_000_000 + sIdx*100 + tIdx*1000)
			sess.TurnHistory = append(sess.TurnHistory, TurnHistoryEntry{
				TurnIndex:     tIdx,
				CompletedAtMs: tsMs,
				Ts:            time.UnixMilli(tsMs).UTC().Format(time.RFC3339),
				SessionID:     fmt.Sprintf("SID-%03d-%02d", sIdx, tIdx%3),
				Model:         "gemini-4-neon",
				ThoughtLevel:  "high",
				Mode:          "parked_mcp_resume",
				StopReason:    "tool_use",
				Usage: &TurnUsage{
					InputTokens:      1000,
					CachedReadTokens: 20000,
					OutputTokens:     250,
					UsageSource:      "db",
				},
			})
		}
		gs.sessions[convKey] = sess
	}
	gs.mu.Unlock()

	gwSids := make(map[string]bool)
	start := time.Now()
	_, turns := gs.CollectDashboardTelemetry(gwSids)
	elapsed := time.Since(start)

	if len(turns) != 60 {
		t.Fatalf("expected top 60 turns, got %d", len(turns))
	}
	if len(gwSids) != 600 {
		t.Fatalf("expected all 600 unique turn sessionIds recorded in gwSids, got %d", len(gwSids))
	}
	for i := 1; i < len(turns); i++ {
		prev, _ := turns[i-1]["completedAtMs"].(int64)
		cur, _ := turns[i]["completedAtMs"].(int64)
		if prev < cur {
			t.Fatalf("turns not sorted descending at index %d: %d < %d", i, prev, cur)
		}
	}
	// Highest timestamp should be sIdx=199, tIdx=50 => 1_700_000_000_000 + 19900 + 50000.
	wantTop := int64(1_700_000_000_000 + 199*100 + 50*1000)
	if gotTop, _ := turns[0]["completedAtMs"].(int64); gotTop != wantTop {
		t.Fatalf("top turn completedAtMs = %d, want %d", gotTop, wantTop)
	}
	t.Logf("CollectDashboardTelemetry(200 sessions x 50 turns = 10k turns) completed in %v", elapsed)
}

func TestFederatedPeerSlowResponseUnder5sDoesNotFalseOffline(t *testing.T) {
	resetPeerDashboardCacheForTest()
	t.Cleanup(resetPeerDashboardCacheForTest)

	if peerFetchTimeout < 4*time.Second {
		t.Fatalf("peerFetchTimeout = %v, want >= 4s to avoid transient 2-vCPU false-offline timeouts", peerFetchTimeout)
	}
	if peerFetchStaleTTL < 3*time.Minute {
		t.Fatalf("peerFetchStaleTTL = %v, want >= 3m for stale resilience", peerFetchStaleTTL)
	}

	// Simulate a slow child peer taking 2.0s (which previously exceeded the old 1.8s timeout and flashed "节点离线").
	slowSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2000 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"now": time.Now().UTC().Format(time.RFC3339),
			"gateway": map[string]any{
				"instanceId":           "worker-b",
				"instanceLabel":        "Worker Node B",
				"maxLiveWorkers":       16,
				"maxConcurrentPrompts": 16,
			},
			"quota": QuotaSnapshot{
				Status:    "ok",
				UpdatedAt: time.Now().UTC().Format(time.RFC3339),
				PlanTier:  "ultra",
				PlanLabel: "Ultra 20x",
			},
			"windows":       map[string]WindowAggregate{},
			"liveSessions":  []map[string]any{},
			"gatewayTurns":  []map[string]any{},
			"recentDbSteps": []map[string]any{},
		})
	}))
	defer slowSrv.Close()

	dec, reachable, stale, _, errMsg := fetchPeerDashboard(gwcfg.DashboardPeerConfig{
		ID:      "worker-b",
		Label:   "Worker Node B",
		BaseURL: slowSrv.URL,
	}, false, "")
	if !reachable || stale || dec == nil || errMsg != "" {
		t.Fatalf("2.0s slow peer should stay reachable without false offline: reachable=%v stale=%v err=%q", reachable, stale, errMsg)
	}
}
