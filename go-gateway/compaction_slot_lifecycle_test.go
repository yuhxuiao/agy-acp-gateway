package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCompactionAndPostCompactionDetectors(t *testing.T) {
	t.Run("multi-client compaction summary positive and negative matrix", func(t *testing.T) {
		positives := []struct {
			name string
			sys  string
			msgs []map[string]any
		}{
			{
				name: "claude code detailed summary",
				msgs: []map[string]any{{"role": "user", "content": "Your task is to create a detailed summary of the conversation so far."}},
			},
			{
				name: "codex checkpoint compaction in user message",
				msgs: []map[string]any{{"role": "user", "content": "You are performing a CONTEXT CHECKPOINT COMPACTION. Create a handoff summary."}},
			},
			{
				name: "codex checkpoint compaction in system prompt",
				sys:  "You are performing a CONTEXT CHECKPOINT COMPACTION.\n\nKeep exact file paths.",
				msgs: []map[string]any{{"role": "user", "content": "Please compact now."}},
			},
			{
				name: "opencode compaction prompt",
				msgs: []map[string]any{{"role": "user", "content": "Provide a detailed prompt for continuing our conversation above. Include modified files."}},
			},
			{
				name: "cline/roo explicit summarize_task tag",
				msgs: []map[string]any{{"role": "user", "content": `<explicit_instructions type="summarize_task">Condense the conversation</explicit_instructions>`}},
			},
			{
				name: "tightened legacy compaction phrase",
				msgs: []map[string]any{{"role": "user", "content": "Your task is to summarize this conversation so another model can continue."}},
			},
			{
				name: "legacy phrase with compaction context",
				msgs: []map[string]any{{"role": "user", "content": "Summarize this conversation into a context checkpoint handoff."}},
			},
			{
				name: "trailing user message merging tool_result with explicit compaction text",
				msgs: []map[string]any{{
					"role": "user",
					"content": []any{
						map[string]any{"type": "tool_result", "tool_use_id": "call_1", "content": "ok"},
						map[string]any{"type": "text", "text": "Your task is to create a detailed summary of the conversation so far."},
					},
				}},
			},
		}
		for _, tc := range positives {
			if !isCompactionSummaryRequestWithSystem(tc.sys, tc.msgs) {
				t.Errorf("%s: expected compaction summary match", tc.name)
			}
		}

		negatives := []struct {
			name string
			sys  string
			msgs []map[string]any
		}{
			{
				name: "casual slack summary request must not wipe session",
				msgs: []map[string]any{{"role": "user", "content": "Summarize this conversation in 3 bullets for Slack."}},
			},
			{
				name: "opencode generic question rejected",
				msgs: []map[string]any{{"role": "user", "content": "What did we do so far?"}},
			},
			{
				name: "tool_result quoting compaction phrase without sibling text block",
				msgs: []map[string]any{{
					"role": "user",
					"content": []any{
						map[string]any{"type": "tool_result", "tool_use_id": "call_1", "content": "Your task is to create a detailed summary of the conversation so far."},
					},
				}},
			},
			{
				name: "tool_result with loose sibling summarize phrase rejected",
				msgs: []map[string]any{{
					"role": "user",
					"content": []any{
						map[string]any{"type": "tool_result", "tool_use_id": "call_1", "content": "ok"},
						map[string]any{"type": "text", "text": "Summarize this conversation into a context checkpoint handoff."},
					},
				}},
			},
			{
				name: "trailing role=tool message",
				msgs: []map[string]any{{"role": "tool", "tool_call_id": "call_1", "content": "create a detailed summary of the conversation"}},
			},
			{
				name: "quoted compaction phrase buried past 600 bytes",
				msgs: []map[string]any{{"role": "user", "content": strings.Repeat("discussion line\n", 60) + "You are performing a CONTEXT CHECKPOINT COMPACTION."}},
			},
		}
		for _, tc := range negatives {
			if isCompactionSummaryRequestWithSystem(tc.sys, tc.msgs) {
				t.Errorf("%s: expected false (must not false-positive)", tc.name)
			}
		}
	})

	t.Run("Pi standalone compaction supports update/<previous-summary>, avoids util, and attributes to pi", func(t *testing.T) {
		piSys := "You are a context summarization assistant. Your task is to read a conversation between a user and an AI assistant, then produce a structured summary following the exact format specified."
		updateMsg := []map[string]any{{
			"role":    "user",
			"content": "<previous-summary>\n" + strings.Repeat("Detailed technical decision log entry for subsystem A.\n", 140) + "\n</previous-summary>\n\nUpdate the existing structured summary.",
		}}
		if !isStandaloneCompactionSummary(piSys, nil, updateMsg) {
			t.Fatal("expected Pi update compaction with >6KB <previous-summary> to match isStandaloneCompactionSummary")
		}

		// Short (<16K) standalone compaction with Claude UA must NOT fall into util:.
		req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
		req.Header.Set("User-Agent", "claude-cli/1.0.0")
		shortMsg := []map[string]any{{"role": "user", "content": "<conversation>\nUser: fix bug\n</conversation>"}}
		if isBackgroundUtilityCallWithContext(req, piSys, nil, shortMsg, map[string]any{"max_tokens": 2048}) {
			t.Fatal("short Pi standalone compaction must NOT be classified as background utility call")
		}
		k := deriveConversationKey(req, map[string]any{"max_tokens": 2048}, piSys, nil, shortMsg)
		if strings.HasPrefix(k, "util:") {
			t.Fatalf("expected non-util key for short standalone compaction, got %q", k)
		}

		// Client attribution: Pi standalone compaction attributes to pi (not Messages API).
		plainReq := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
		ci := DetectClientIdentity(plainReq, map[string]any{}, k, piSys, piSys, nil)
		if ci.ID != "pi" || ci.Label != "Pi" || !ci.Known {
			t.Fatalf("expected Pi compaction attribution pi/Pi, got %+v", ci)
		}
	})

	t.Run("post-compaction continuation prefixes and negative guards", func(t *testing.T) {
		pos := [][]map[string]any{
			{{"role": "user", "content": "<system-reminder>date</system-reminder>\nThis session is being continued from a previous conversation that ran out of context."}},
			{{"role": "user", "content": "The conversation history before this point was compacted into the following summary:\n<summary>goal</summary>"}},
			{{"role": "user", "content": "Another language model started to solve this problem and produced a summary of its thinking process."}},
			{{"role": "user", "content": "[CONTEXT COMPACTION]\nPrevious task summary..."}},
			{{"role": "user", "content": "This task is being continued from a previous conversation."}},
		}
		for i, msgs := range pos {
			if !isPostCompactionContinuation(msgs) {
				t.Errorf("pos[%d] expected true", i)
			}
		}
		neg := [][]map[string]any{
			{{"role": "user", "content": "What did we do so far?"}},
			{{"role": "user", "content": "Please inspect go-gateway/gateway_pool.go"}},
			{{"role": "assistant", "content": "This session is being continued from a previous conversation."}},
			{{"role": "user", "content": strings.Repeat("preamble ", 40) + "This session is being continued from a previous conversation."}},
		}
		for i, msgs := range neg {
			if isPostCompactionContinuation(msgs) {
				t.Errorf("neg[%d] expected false", i)
			}
		}
	})

	t.Run("content anchor filtering drops secrets and low entropy", func(t *testing.T) {
		msgs := []map[string]any{
			{"role": "user", "content": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
			{"role": "assistant", "content": "Authorization: Bearer " + "sk-" + "dummy-redacted-test-fixture-000123456789"},
			{"role": "user", "content": "Audit the SQLite WAL flush watermark regression in go-gateway/store.go line 522."},
		}
		got := extractSessionContentAnchors(msgs, "")
		for _, a := range got.Anchors {
			if strings.Contains(a, "bearer") || strings.Contains(a, "sk-") || strings.Contains(a, "aaaaaaaaaa") {
				t.Fatalf("unexpected sensitive or low-entropy anchor leaked: %q", a)
			}
		}
		if len(got.Anchors) != 1 || !strings.Contains(got.Anchors[0], "sqlite wal flush watermark") {
			t.Fatalf("expected only clean technical anchor, got %+v", got)
		}
	})
}

func TestPiConcurrentSubagentsPreCompactionReclamation(t *testing.T) {
	setupM2IsolatedEnv(t)
	baseURL, gs := startCarbonTestGateway(t)

	subagentSys := "You are the \"code-reviewer\" subagent, working on one task delegated by the main agent.\n# pi 全局规则\nPrimary working directory: /data/workspace"
	sharedHeader := "Review the following pull request carefully, focusing strictly on concurrency correctness, lock ordering, and resource leaks across all touched modules.\n\n"
	tools := []map[string]any{{
		"name":         "Read",
		"description":  "Read a file",
		"input_schema": map[string]any{"type": "object", "properties": map[string]any{"path": map[string]any{"type": "string"}}},
	}}

	// Subagent A (parked on tool_use) and Subagent B (idle end_turn) share exact same fp:<sysHash>, cwd, and template header!
	subAMsg0 := sharedHeader + "Target task ALPHA-9182: audit mcp_bridge.go sibling cursor rollover handling when loop_breaker trips on the fifth duplicate call."
	subAMsgMid := "Midpoint finding ALPHA-MID-4471: checked executeMcpToolCall in go-gateway/mcp_bridge.go and found nextPos advancement prevents double dispatch."
	subBMsg0 := sharedHeader + "Target task BETA-7391: audit dashboard_federation.go upstream peer timeout jitter and quota backoff cache behavior."

	stA, _, rawA := postJSONWithHeaders(t, baseURL+"/v1/messages", nil, map[string]any{
		"model":      "gemini-4-carbon",
		"max_tokens": 1024,
		"system":     subagentSys,
		"tools":      tools,
		"messages": []map[string]any{
			{"role": "user", "content": subAMsg0},
			{"role": "assistant", "content": "Reading mcp_bridge.go now."},
			{"role": "user", "content": subAMsgMid},
			{"role": "assistant", "content": "Checking sibling pre-dispatch queue."},
			{"role": "user", "content": "[FAKE_TOOL_CALL] inspect trailing parked file (trimmed by keepRecentTokens)"},
		},
	})
	if stA != http.StatusOK {
		t.Fatalf("subagent A turn failed: %d %s", stA, string(rawA))
	}

	stB, _, rawB := postJSONWithHeaders(t, baseURL+"/v1/messages", nil, map[string]any{
		"model":      "gemini-4-carbon",
		"max_tokens": 1024,
		"system":     subagentSys,
		"tools":      tools,
		"messages": []map[string]any{
			{"role": "user", "content": subBMsg0},
		},
	})
	if stB != http.StatusOK {
		t.Fatalf("subagent B turn failed: %d %s", stB, string(rawB))
	}

	if live := gs.countLiveWorkers(); live != 2 {
		t.Fatalf("expected 2 live subagents (A parked + B idle), got %d", live)
	}

	var sessA, sessB *AcpProjectSession
	gs.mu.RLock()
	for _, s := range gs.sessions {
		s.mu.Lock()
		p := s.syncPhaseLocked()
		s.mu.Unlock()
		if p == PhaseHotParked {
			sessA = s
		} else if p == PhaseIdle {
			sessB = s
		}
	}
	gs.mu.RUnlock()
	if sessA == nil || sessB == nil {
		t.Fatalf("expected both sessA(parked) and sessB(idle), got A=%v B=%v", sessA, sessB)
	}
	sessB.mu.Lock()
	sidB := sessB.SessionID
	msgCntB := sessB.MessageCount
	sessB.mu.Unlock()

	// Trigger Pi standalone compaction for Subagent A with cwd falling back to /tmp (no Primary working directory)
	// and with keepRecentTokens trimming the parked tail turn!
	piSys := "You are a context summarization assistant. Your task is to read a conversation between a user and an AI assistant, then produce a structured summary following the exact format specified."
	compBody := "<conversation>\n[User]: " + subAMsg0 + "\n[Assistant]: Reading mcp_bridge.go now.\n[User]: " + subAMsgMid + "\n</conversation>\n\nCreate a structured context checkpoint summary."
	stComp, _, rawComp := postJSONWithHeaders(t, baseURL+"/v1/messages", nil, map[string]any{
		"model":      "gemini-4-carbon",
		"max_tokens": 2048,
		"system":     piSys,
		"messages":   []map[string]any{{"role": "user", "content": compBody}},
	})
	if stComp != http.StatusOK {
		t.Fatalf("Pi compaction failed: %d %s", stComp, string(rawComp))
	}

	// Subagent A and the compaction session must BOTH be hibernated; ONLY Subagent B stays live intact!
	if live := gs.countLiveWorkers(); live != 1 {
		t.Fatalf("expected exactly 1 live worker (Subagent B intact; A + compaction hibernated), got %d", live)
	}
	sessA.mu.Lock()
	sidA, hibA := sessA.SessionID, sessA.HibernatedSessionID
	sessA.mu.Unlock()
	if sidA != "" || hibA != "" {
		t.Fatalf("expected Subagent A superseded cleanly, got sid=%q hib=%q", sidA, hibA)
	}
	sessB.mu.Lock()
	afterSidB, afterMsgCntB := sessB.SessionID, sessB.MessageCount
	sessB.mu.Unlock()
	if afterSidB != sidB || afterMsgCntB != msgCntB {
		t.Fatalf("Subagent B must remain completely untouched: sid %q->%q msgCnt %d->%d", sidB, afterSidB, msgCntB, afterMsgCntB)
	}
}

func TestPiSecondRoundCompactionPreviousSummaryAnchorMatch(t *testing.T) {
	setupM2IsolatedEnv(t)
	gs := NewGatewayServer()
	defer gs.Close()

	// Session 1 came from an earlier compaction: messages[0] wraps <summary>...</summary> (>6KB)
	// and keepRecentTokens omits the latest turns during 2nd compaction.
	summaryBody := "Goal: stabilize distributed gateway compaction slot lifecycle across Pi, Claude Code, and Codex.\n" +
		strings.Repeat("Verified lock ordering and protobuf ingest cursor boundaries for round 7 telemetry.\n", 45) +
		"UNIQUE-SECOND-ROUND-MIDPOINT-MARKER-88421: fixed Pass-1b compactionPending preemption ordering.\n" +
		strings.Repeat("Additional verified milestone details for production smoke test rollout.\n", 45)
	prevCompactedMsgs := []map[string]any{
		{"role": "user", "content": "The conversation history before this point was compacted into the following summary:\n<summary>\n" + summaryBody + "\n</summary>"},
		{"role": "assistant", "content": "Continuing from the checkpoint summary."},
		{"role": "user", "content": "Now inspect the live pool eviction comparator in internal/session/lru.go."},
		{"role": "assistant", "content": "Confirmed Disposable sits right after IsUtil and before ProtectedWarm."},
		{"role": "user", "content": "Trailing turn kept verbatim by keepRecentTokens and excluded from <conversation>."},
	}

	s1 := NewAcpProjectSession("fp:pi-round2-sys:u:11111111", "/data/workspace", "# pi 全局规则")
	s1.SessionID = "sid-pi-r2-pre"
	s1.ClientID = "pi"
	s1.MessageCount = len(prevCompactedMsgs) + 1
	s1.contentAnchors = extractSessionContentAnchors(prevCompactedMsgs, "")
	s1.ParkedCalls["acp_00000000000000000000000000000001"] = &ParkedMcpCall{}

	sComp := NewAcpProjectSession("fp:c464889dcfa60441:u:22222222", "/tmp", "You are a context summarization assistant.")
	gs.mu.Lock()
	gs.sessions[s1.ConvKey] = s1
	gs.sessions[sComp.ConvKey] = sComp
	gs.mu.Unlock()

	// 2nd compaction puts inner summary inside <previous-summary> without outer wrapper, and trims trailing turn.
	compMsgs := []map[string]any{{
		"role":    "user",
		"content": "<previous-summary>\n" + summaryBody + "\n</previous-summary>\n\n<conversation>\n[User]: Now inspect the live pool eviction comparator in internal/session/lru.go.\n</conversation>\n\nUpdate the existing structured summary.",
	}}
	ref := gs.findPreCompactionTarget(sComp, sComp.ConvKey, "/tmp", "22222222", true, ClientIdentity{ID: "pi", Label: "Pi", Known: true}, compMsgs)
	if ref == nil || ref.sess != s1 || ref.sid != "sid-pi-r2-pre" {
		t.Fatalf("expected 2nd-round Pi compaction to uniquely match s1 via <summary>/<previous-summary> anchors, got %+v", ref)
	}
	ref.release()
}

func TestCompactionPendingPass1bPreemptAndRollbackOrSupersede(t *testing.T) {
	setupM2IsolatedEnv(t)
	oldMax := MaxLiveWorkers
	MaxLiveWorkers = 2
	defer func() { MaxLiveWorkers = oldMax }()

	dbPath := filepath.Join(t.TempDir(), "state.db")
	GlobalStore.Init(dbPath)
	defer GlobalStore.Init(GatewayStateDB)

	gs := NewGatewayServer()
	defer gs.Close()

	// Session A: 363K ProtectedWarm active idle session (must NOT be evicted!).
	sWarm := NewAcpProjectSession("fp:warm-main:u:aaaaaaaa", "/data/workspace", "main")
	sWarm.SessionID = "sid-warm-363k"
	sWarm.ModelID = "gemini-4-carbon"
	sWarm.MessageCount = 40
	sWarm.LastCachedReadTokens = 363000
	sWarm.LastUsedAt = time.Now().Add(-30 * time.Second)
	gs.bindSessionDeps(sWarm)

	// Session B: HotParked pre-compaction target session with active compactionPending lease.
	sTarget := NewAcpProjectSession("fp:target-pre:u:bbbbbbbb", "/data/workspace", "main")
	sTarget.SessionID = "sid-target-parked"
	sTarget.ModelID = "gemini-4-carbon"
	sTarget.MessageCount = 50
	sTarget.TotalTurns = 20
	sTarget.HistoryFingerprint = "hist-target-fp"
	sTarget.LastCachedReadTokens = 410000
	sTarget.LastUsedAt = time.Now().Add(-10 * time.Second)
	sTarget.PromptInFlight = true
	sTarget.ParkedCalls["acp_11111111111111111111111111111111"] = &ParkedMcpCall{
		Call:       ToolCallItem{ID: "acp_11111111111111111111111111111111", Name: "Bash"},
		ResultChan: make(chan map[string]any, 1),
	}
	gs.bindSessionDeps(sTarget)
	sTarget.persistSession()
	GlobalStore.Flush()

	ref := sTarget.addCompactionPendingLease("sid-target-parked", 5*time.Minute, false)
	if ref == nil {
		t.Fatal("failed to acquire compactionPending lease on sTarget")
	}

	sComp := NewAcpProjectSession("fp:c464889dcfa60441:u:cccccccc", "/tmp", "You are a context summarization assistant.")
	gs.bindSessionDeps(sComp)

	gs.mu.Lock()
	gs.sessions[sWarm.ConvKey] = sWarm
	gs.sessions[sTarget.ConvKey] = sTarget
	gs.sessions[sComp.ConvKey] = sComp
	gs.mu.Unlock()

	// Pool is 2/2 full -> enforceWorkerCapMode for sComp MUST preempt sTarget via Pass-1b (NOT sWarm!).
	gs.enforceWorkerCapMode(sComp, true)

	sWarm.mu.Lock()
	warmSid := sWarm.SessionID
	sWarm.mu.Unlock()
	if warmSid != "sid-warm-363k" {
		t.Fatalf("ProtectedWarm active session must NOT be evicted when compactionPending parked session exists, got sid=%q", warmSid)
	}

	sTarget.mu.Lock()
	targetSid := sTarget.SessionID
	targetHib := sTarget.HibernatedSessionID
	targetHibParked := len(sTarget.HibernatedParkedCallIDs)
	targetStreak := sTarget.orphanCapStreak
	sTarget.mu.Unlock()
	if targetSid != "" || targetHib != "sid-target-parked" || targetHibParked != 1 || targetStreak != 0 {
		t.Fatalf("expected Pass-1b preemption preserving resume state with orphanCapStreak=0, got sid=%q hib=%q parked=%d streak=%d",
			targetSid, targetHib, targetHibParked, targetStreak)
	}

	// Branch A: Compaction failure releases lease -> resume state remains intact for session/resume!
	ref.release()
	sTarget.mu.Lock()
	stillHib := sTarget.HibernatedSessionID
	stillPending := sTarget.hasActiveCompactionPendingLocked(time.Now())
	sTarget.mu.Unlock()
	if stillHib != "sid-target-parked" || stillPending {
		t.Fatalf("on compaction failure, resume state must remain intact and pending cleared: hib=%q pending=%v", stillHib, stillPending)
	}

	// Branch B: Compaction succeeds -> supersedeAsCompactionTarget clears preempted HibernatedSessionID & SQLite stale agy_session_id!
	if !sTarget.supersedeAsCompactionTarget("sid-target-parked", false) {
		t.Fatal("expected supersedeAsCompactionTarget to clear preempted HibernatedSessionID")
	}
	sTarget.mu.Lock()
	afterHib := sTarget.HibernatedSessionID
	afterMsg := sTarget.MessageCount
	afterParked := len(sTarget.HibernatedParkedCallIDs)
	sTarget.mu.Unlock()
	if afterHib != "" || afterMsg != 0 || afterParked != 0 {
		t.Fatalf("expected preempted target fully cleared after supersede, got hib=%q msg=%d parked=%d", afterHib, afterMsg, afterParked)
	}

	GlobalStore.Flush()
	rawDB, err := sql.Open("sqlite", fmt.Sprintf("file:%s?mode=ro", filepath.ToSlash(dbPath)))
	if err != nil {
		t.Fatalf("open sqlite ro: %v", err)
	}
	defer rawDB.Close()
	var dbSid string
	var dbMsg int
	if err := rawDB.QueryRow(`SELECT agy_session_id, msg_count FROM conversations WHERE conv_key = ?`, sTarget.ConvKey).Scan(&dbSid, &dbMsg); err != nil {
		t.Fatalf("query conversations row: %v", err)
	}
	if dbSid != "" || dbMsg != 0 {
		t.Fatalf("expected SQLite conversations row cleared (agy_session_id='', msg_count=0) so restart never resurrects superseded session, got sid=%q msg=%d", dbSid, dbMsg)
	}
}

func TestMcpRelayPausedWhileCompactionPending(t *testing.T) {
	setupM2IsolatedEnv(t)
	t.Setenv("AGY_TOOL_RELAY_MS", "80")

	gs := NewGatewayServer()
	defer gs.Close()

	s := NewAcpProjectSession("fp:relay-pause:u:12345678", "/tmp", "rules")
	gs.bindSessionDeps(s)
	s.SessionID = "sid-relay-pause-1"
	s.MessageCount = 10
	s.PromptInFlight = true
	s.IsBusy = false
	s.ParkedSince = time.Now()
	s.DeclaredMcpTools = map[string]bool{"Bash": true}
	s.McpToHostMap = map[string]string{"Bash": "Bash"}
	entry := &ParkedMcpCall{
		Call:       ToolCallItem{ID: "call-relay-pause-1", Name: "Bash", Input: map[string]any{"command": "echo hi"}},
		ResultChan: make(chan map[string]any, 1),
		Delivered:  true,
		ParkedAt:   time.Now(),
		SessionID:  "sid-relay-pause-1",
	}
	s.ParkedCalls[entry.Call.ID] = entry

	ref := s.addCompactionPendingLease("sid-relay-pause-1", 5*time.Minute, false)
	if ref == nil {
		t.Fatal("expected lease acquired")
	}
	defer ref.release()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	out := s.awaitParkedEntry(ctx, entry)
	isErr, _ := out["isError"].(bool)
	if !isErr {
		t.Fatalf("expected hibernated tool error when compaction pause fires, got %+v", out)
	}
	if raw, _ := json.Marshal(out); !strings.Contains(string(raw), "Session hibernated.") {
		t.Fatalf("expected Session hibernated. payload, got %s", string(raw))
	}

	s.mu.Lock()
	sid := s.SessionID
	hib := s.HibernatedSessionID
	hops := s.orphanRelayHops
	streak := s.orphanCapStreak
	hibParked := len(s.HibernatedParkedCallIDs)
	s.mu.Unlock()
	if sid != "" || hib != "sid-relay-pause-1" || hibParked != 1 || hops != 0 || streak != 0 {
		t.Fatalf("expected clean relay pause with resume state and zero relay/streak burn, got sid=%q hib=%q hibParked=%d hops=%d streak=%d",
			sid, hib, hibParked, hops, streak)
	}
}

func TestClaudeCodeIdleAndParkedCompactionLifecycle(t *testing.T) {
	tools := []map[string]any{{
		"name":         "Bash",
		"description":  "Run shell command",
		"input_schema": map[string]any{"type": "object", "properties": map[string]any{"command": map[string]any{"type": "string"}}},
	}}
	ccSys := "You are an agent working with the user toward their goals.\nPrimary working directory: /tmp"

	t.Run("idle in-place compaction (SPLIT5) hibernates immediately and reuses canonical sid on post-compaction", func(t *testing.T) {
		setupM2IsolatedEnv(t)
		baseURL, gs := startCarbonTestGateway(t)
		hdrs := map[string]string{"x-session-id": "cc-idle-split5"}

		m1 := []map[string]any{{"role": "user", "content": "Inspect architecture for module X"}}
		st1, res1, raw1 := postJSONWithHeaders(t, baseURL+"/v1/messages", hdrs, map[string]any{
			"model":      "gemini-4-carbon",
			"max_tokens": 1024,
			"system":     ccSys,
			"tools":      tools,
			"messages":   m1,
		})
		if st1 != http.StatusOK {
			t.Fatalf("turn 1 failed: %d %s", st1, string(raw1))
		}
		if gs.countLiveWorkers() != 1 {
			t.Fatalf("expected 1 live worker after turn 1, got %d", gs.countLiveWorkers())
		}

		// Compaction summary turn with full tools attached (Claude Code style).
		compMsgs := []map[string]any{
			m1[0],
			{"role": "assistant", "content": res1["content"]},
			{"role": "user", "content": "Your task is to create a detailed summary of the conversation so far."},
		}
		stComp, _, rawComp := postJSONWithHeaders(t, baseURL+"/v1/messages", hdrs, map[string]any{
			"model":      "gemini-4-carbon",
			"max_tokens": 2048,
			"system":     ccSys,
			"tools":      tools,
			"messages":   compMsgs,
		})
		if stComp != http.StatusOK {
			t.Fatalf("compaction failed: %d %s", stComp, string(rawComp))
		}
		if live := gs.countLiveWorkers(); live != 0 {
			t.Fatalf("expected 0 live workers immediately after Claude Code idle compaction, got %d", live)
		}

		// Post-compaction continuation with NEW firstUserFp ("This session is being continued...").
		postMsgs := []map[string]any{
			{"role": "user", "content": "This session is being continued from a previous conversation that ran out of context.\nSummary: module X audited."},
		}
		stPost, _, rawPost := postJSONWithHeaders(t, baseURL+"/v1/messages", hdrs, map[string]any{
			"model":      "gemini-4-carbon",
			"max_tokens": 1024,
			"system":     ccSys,
			"tools":      tools,
			"messages":   postMsgs,
		})
		if stPost != http.StatusOK {
			t.Fatalf("post-compaction failed: %d %s", stPost, string(rawPost))
		}

		gs.mu.RLock()
		sessCount := len(gs.sessions)
		var mainSess *AcpProjectSession
		for _, s := range gs.sessions {
			mainSess = s
		}
		gs.mu.RUnlock()
		if sessCount != 1 {
			t.Fatalf("expected canonical sid session reused without spawning a 3rd convKey, got %d sessions", sessCount)
		}
		mainSess.mu.Lock()
		rolls := mainSess.CompactionRollovers
		mainSess.mu.Unlock()
		if rolls != 1 {
			t.Fatalf("expected CompactionRollovers == 1 on canonical session, got %d", rolls)
		}
	})

	t.Run("parked compaction reuses hot-parked main in-place (cache preserved), hibernates immediately, then reuses canonical main", func(t *testing.T) {
		setupM2IsolatedEnv(t)
		baseURL, gs := startCarbonTestGateway(t)
		hdrs := map[string]string{"x-session-id": "cc-parked-split"}

		m1 := []map[string]any{{"role": "user", "content": "[FAKE_TOOL_CALL] inspect parked task"}}
		st1, res1, raw1 := postJSONWithHeaders(t, baseURL+"/v1/messages", hdrs, map[string]any{
			"model":      "gemini-4-carbon",
			"max_tokens": 1024,
			"system":     ccSys,
			"tools":      tools,
			"messages":   m1,
		})
		if st1 != http.StatusOK || res1["stop_reason"] != "tool_use" {
			t.Fatalf("expected parked tool_use turn 1, got %d %s", st1, string(raw1))
		}

		var mainSess *AcpProjectSession
		gs.mu.RLock()
		for _, s := range gs.sessions {
			mainSess = s
		}
		gs.mu.RUnlock()

		// Compaction request arrives while mainSess is PhaseHotParked (same firstUserFp + same sid).
		compMsgs := []map[string]any{
			m1[0],
			{"role": "assistant", "content": "Executed intermediate check."},
			{"role": "user", "content": "Your task is to create a detailed summary of the conversation so far."},
		}
		stComp, _, rawComp := postJSONWithHeaders(t, baseURL+"/v1/messages", hdrs, map[string]any{
			"model":      "gemini-4-carbon",
			"max_tokens": 2048,
			"system":     ccSys,
			"tools":      tools,
			"messages":   compMsgs,
		})
		if stComp != http.StatusOK {
			t.Fatalf("parked compaction failed: %d %s", stComp, string(rawComp))
		}
		if live := gs.countLiveWorkers(); live != 0 {
			t.Fatalf("expected hot-parked main hibernated immediately after compaction (0 live), got %d", live)
		}

		// Eligible parked compaction reuses mainSess in-place without allocating a #2 slot.
		gs.mu.RLock()
		afterCompSessions := len(gs.sessions)
		gs.mu.RUnlock()
		if afterCompSessions != 1 {
			t.Fatalf("expected in-place hot-interrupt compaction with 1 session (no #2 slot), got %d", afterCompSessions)
		}

		mainSess.mu.Lock()
		mainRollFlag := mainSess.ForceRolloverNext
		mainRollWhy := mainSess.ForceRolloverReason
		mainSess.mu.Unlock()
		if !mainRollFlag || mainRollWhy != "rollover_compaction" {
			t.Fatalf("expected canonical mainSess stamped rollover_compaction, got flag=%v why=%q", mainRollFlag, mainRollWhy)
		}

		// Post-compaction continuation uniquely reuses canonical mainSess.
		postMsgs := []map[string]any{
			{"role": "user", "content": "This session is being continued from a previous conversation that ran out of context.\nSummary: parked work compacted."},
		}
		stPost, _, rawPost := postJSONWithHeaders(t, baseURL+"/v1/messages", hdrs, map[string]any{
			"model":      "gemini-4-carbon",
			"max_tokens": 1024,
			"system":     ccSys,
			"tools":      tools,
			"messages":   postMsgs,
		})
		if stPost != http.StatusOK {
			t.Fatalf("post-compaction failed: %d %s", stPost, string(rawPost))
		}
		gs.mu.RLock()
		totalSessions := len(gs.sessions)
		gs.mu.RUnlock()
		if totalSessions != 1 {
			t.Fatalf("expected canonical mainSess reused with 1 total session, got %d sessions", totalSessions)
		}
		mainSess.mu.Lock()
		afterMainSid := mainSess.SessionID
		afterMainRolls := mainSess.CompactionRollovers
		mainSess.mu.Unlock()
		if afterMainSid == "" || afterMainRolls != 1 {
			t.Fatalf("expected canonical mainSess active with CompactionRollovers==1, got sid=%q rolls=%d", afterMainSid, afterMainRolls)
		}
	})

	t.Run("parked N->N-1 merged tool_result + compaction text reuses cache in-place and reclaims slot", func(t *testing.T) {
		setupM2IsolatedEnv(t)
		baseURL, gs := startCarbonTestGateway(t)
		hdrs := map[string]string{"x-session-id": "cc-parked-merged-tail"}

		m1 := []map[string]any{{"role": "user", "content": "[FAKE_TOOL_CALL] [FAKE_TOOL_CHAIN] step 1"}}
		st1, res1, raw1 := postJSONWithHeaders(t, baseURL+"/v1/messages", hdrs, map[string]any{
			"model":      "gemini-4-carbon",
			"max_tokens": 1024,
			"system":     ccSys,
			"tools":      tools,
			"messages":   m1,
		})
		if st1 != http.StatusOK || res1["stop_reason"] != "tool_use" {
			t.Fatalf("turn 1 failed: %d %s", st1, string(raw1))
		}
		c1 := extractFirstToolUse(t, res1)

		// Turn 2 resumes parked tool 1 and parks chained tool 2 -> session has 4 msgs (lastTurnAssistant=true).
		m2 := []map[string]any{
			m1[0],
			{"role": "assistant", "content": []any{map[string]any{"type": "tool_use", "id": c1.ID, "name": c1.Name, "input": c1.Input}}},
			{"role": "user", "content": []any{
				map[string]any{"type": "tool_result", "tool_use_id": c1.ID, "content": "prev result already consumed"},
			}},
		}
		st2, res2, raw2 := postJSONWithHeaders(t, baseURL+"/v1/messages", hdrs, map[string]any{
			"model":      "gemini-4-carbon",
			"max_tokens": 1024,
			"system":     ccSys,
			"tools":      tools,
			"messages":   m2,
		})
		if st2 != http.StatusOK || res2["stop_reason"] != "tool_use" {
			t.Fatalf("turn 2 failed: %d %s", st2, string(raw2))
		}

		var mainSess *AcpProjectSession
		gs.mu.RLock()
		for _, s := range gs.sessions {
			mainSess = s
		}
		gs.mu.RUnlock()
		mainSess.mu.Lock()
		preCompSid := mainSess.SessionID
		preCompMsgCnt := mainSess.MessageCount
		mainSess.mu.Unlock()
		if preCompMsgCnt != 4 {
			t.Fatalf("expected MessageCount=4 after turn 2 tool_use, got %d", preCompMsgCnt)
		}

		// Claude Code strips unanswered assistant(tool_use_2) and merges compaction prompt onto m2[2] -> len=3 (N -> N-1)!
		compMsgs := []map[string]any{
			m2[0],
			m2[1],
			{"role": "user", "content": []any{
				map[string]any{"type": "tool_result", "tool_use_id": c1.ID, "content": "prev result already consumed"},
				map[string]any{"type": "text", "text": "Your task is to create a detailed summary of the conversation so far."},
			}},
		}
		stComp, resComp, rawComp := postJSONWithHeaders(t, baseURL+"/v1/messages", hdrs, map[string]any{
			"model":      "gemini-4-carbon",
			"max_tokens": 2048,
			"system":     ccSys,
			"tools":      tools,
			"messages":   compMsgs,
		})
		if stComp != http.StatusOK {
			t.Fatalf("N->N-1 parked compaction failed: %d %s", stComp, string(rawComp))
		}
		if strings.Contains(string(rawComp), "prev result already consumed") {
			t.Fatalf("compMergedTail must NOT re-inject already-consumed tool_result_prev, got: %s", string(rawComp))
		}
		if live := gs.countLiveWorkers(); live != 0 {
			t.Fatalf("expected 0 live workers immediately after N->N-1 parked compaction, got %d", live)
		}
		mainSess.mu.Lock()
		lastTurn := mainSess.TurnHistory[len(mainSess.TurnHistory)-1]
		rollFlag := mainSess.ForceRolloverNext
		rollWhy := mainSess.ForceRolloverReason
		mainSess.mu.Unlock()
		if lastTurn.SessionID != preCompSid || lastTurn.Mode != "parked_mcp_resume" {
			t.Fatalf("expected compaction to reuse preCompSid=%q via parked_mcp_resume, got sid=%q mode=%q", preCompSid, lastTurn.SessionID, lastTurn.Mode)
		}
		if !rollFlag || rollWhy != "rollover_compaction" {
			t.Fatalf("expected ForceRolloverNext=true rollover_compaction, got flag=%v why=%q", rollFlag, rollWhy)
		}
		_ = resComp
	})
}

func TestSettleDetachedTurnPriorMsgCountGuard(t *testing.T) {
	setupM2IsolatedEnv(t)

	// Case 1: In-place warm compaction (priorMsgCount > 0) disconnects & expires before upstream finishes ->
	// MUST preserve warm session (not wipe 400K cache!).
	sWarm := NewAcpProjectSession("sid:warm-inplace:u:11111111", "/tmp", "sys")
	sWarm.SessionID = "sid-warm-400k"
	sWarm.MessageCount = 50
	turn1 := newActiveTurnState(nil, nil)
	sWarm.activeTurn = turn1
	sTarget1 := NewAcpProjectSession("fp:target1:u:aaaaaaaa", "/tmp", "sys")
	sTarget1.SessionID = "sid-t1"
	ref1 := sTarget1.addCompactionPendingLease("sid-t1", 5*time.Minute, false)
	detWarm := &detachedTurnState{
		reqFp:                  "fp-warm-1",
		turn:                   turn1,
		sid:                    "sid-warm-400k",
		msgCount:               52,
		priorMsgCount:          50,
		isCompactionReq:        true,
		isStandaloneCompaction: false,
		compactionTarget:       ref1,
	}
	sWarm.settleDetachedTurn(detWarm, "prefill_grace_expired", "PREFILL_SOFT_DETACH_EXPIRED")
	sWarm.mu.Lock()
	warmSidAfter := sWarm.SessionID
	sWarm.mu.Unlock()
	if warmSidAfter != "sid-warm-400k" {
		t.Fatalf("in-place compaction with priorMsgCount>0 must preserve warm session on cancel/expiry, got sid=%q", warmSidAfter)
	}
	sTarget1.mu.Lock()
	t1Pending := sTarget1.hasActiveCompactionPendingLocked(time.Now())
	sTarget1.mu.Unlock()
	if t1Pending {
		t.Fatal("settleDetachedTurn must release compactionTarget lease on expiry")
	}

	// Case 2: Fresh compaction slot (priorMsgCount == 0) expires unclaimed -> MUST hibernate!
	sFresh := NewAcpProjectSession("sid:fresh-slot:u:22222222", "/tmp", "sys")
	sFresh.SessionID = "sid-fresh-comp"
	turn2 := newActiveTurnState(nil, nil)
	sFresh.activeTurn = turn2
	detFresh := &detachedTurnState{
		reqFp:                  "fp-fresh-1",
		turn:                   turn2,
		sid:                    "sid-fresh-comp",
		msgCount:               10,
		priorMsgCount:          0,
		isCompactionReq:        true,
		isStandaloneCompaction: false,
	}
	sFresh.settleDetachedTurn(detFresh, "prefill_grace_expired", "PREFILL_SOFT_DETACH_EXPIRED")
	sFresh.mu.Lock()
	freshSidAfter := sFresh.SessionID
	sFresh.mu.Unlock()
	if freshSidAfter != "" {
		t.Fatalf("fresh compaction slot (priorMsgCount==0) must hibernate on detach expiry, got sid=%q", freshSidAfter)
	}
}

func TestForceRolloverShortTTLAndConcurrentCompactionRace(t *testing.T) {
	setupM2IsolatedEnv(t)

	t.Run("ForceRolloverNext idle session reaped at UtilWorkerTtl instead of IdleWorkerTtl", func(t *testing.T) {
		oldIdle := IdleWorkerTtl
		IdleWorkerTtl = 10 * time.Minute
		defer func() { IdleWorkerTtl = oldIdle }()

		gs := NewGatewayServer()
		defer gs.Close()

		sDead := NewAcpProjectSession("sid:dead-roll:u:11111111", "/tmp", "sys")
		sDead.SessionID = "sid-dead-roll"
		sDead.ForceRolloverNext = true
		sDead.LastCachedReadTokens = 500000
		sDead.LastUsedAt = time.Now().Add(-(UtilWorkerTtl + 5*time.Second))
		gs.bindSessionDeps(sDead)

		sNormal := NewAcpProjectSession("sid:normal-warm:u:22222222", "/tmp", "sys")
		sNormal.SessionID = "sid-normal-warm"
		sNormal.LastCachedReadTokens = 100000
		sNormal.LastUsedAt = time.Now().Add(-(UtilWorkerTtl + 5*time.Second))
		gs.bindSessionDeps(sNormal)

		gs.mu.Lock()
		gs.sessions[sDead.ConvKey] = sDead
		gs.sessions[sNormal.ConvKey] = sNormal
		gs.mu.Unlock()

		gs.reapIdleWorkers()

		sDead.mu.Lock()
		deadSid := sDead.SessionID
		sDead.mu.Unlock()
		sNormal.mu.Lock()
		normalSid := sNormal.SessionID
		sNormal.mu.Unlock()
		if deadSid != "" {
			t.Fatalf("expected ForceRolloverNext session reaped at UtilWorkerTtl, still got sid=%q", deadSid)
		}
		if normalSid != "sid-normal-warm" {
			t.Fatalf("expected normal warm session retained, got sid=%q", normalSid)
		}
	})

	t.Run("coalesce handoff preserves compaction target ownership for Request 2 replay", func(t *testing.T) {
		gs := NewGatewayServer()
		defer gs.Close()

		sTarget := NewAcpProjectSession("fp:coal-target:u:aaaaaaaa", "/data/workspace", "sys")
		sTarget.SessionID = "sid-coal-target"
		sTarget.MessageCount = 12
		gs.bindSessionDeps(sTarget)

		sComp := NewAcpProjectSession("fp:c464889dcfa60441:u:bbbbbbbb", "/tmp", "You are a context summarization assistant.")
		sComp.SessionID = "sid-coal-comp"
		gs.bindSessionDeps(sComp)

		ref := sTarget.addCompactionPendingLease("sid-coal-target", 5*time.Minute, false)
		turn := newActiveTurnState(nil, nil)
		sComp.mu.Lock()
		sComp.claimTurnLocked() // Req 1
		sComp.activeTurn = turn
		sComp.activeTurnReqFp = "reqfp-coal-target-1"
		sComp.activeReqCancel = func() {}
		sComp.activeCompactionTarget = ref
		sComp.mu.Unlock()

		gs.mu.Lock()
		gs.sessions[sTarget.ConvKey] = sTarget
		gs.sessions[sComp.ConvKey] = sComp
		gs.mu.Unlock()

		// Req 2 coalesces onto active turn -> sets coalesceHandoff=true, turnClaimCount=2.
		act, cancelPrev := gs.findSessionWithActiveTurn(sComp.ConvKey, "/tmp", "reqfp-coal-target-1")
		if act != sComp || cancelPrev == nil {
			t.Fatal("expected Req 2 to coalesce onto sComp")
		}
		cancelPrev()

		// Req 1 finishes and records completed turn: MUST NOT clear coalesceHandoff yet, MUST transfer ref to lastCompletedCompactionTarget!
		res := TurnResult{SessionID: "sid-coal-comp", StopReason: "end_turn", Text: "SUMMARY_COALESCED"}
		sComp.recordRecentCompletedTurn("reqfp-coal-target-1", res)

		sComp.mu.Lock()
		handedOff := sComp.coalesceHandoff || sComp.turnClaimCount > 1
		sComp.coalesceHandoff = false
		sComp.mu.Unlock()
		if !handedOff {
			t.Fatal("Req 1 must observe handedOff == true after recordRecentCompletedTurn")
		}
		sComp.releaseTurnClaim()

		// Req 1 calls finishOneShotCompaction with handedOff=true -> neither sComp nor sTarget is closed yet!
		finishOneShotCompaction(sComp, res, nil, true, true, nil)
		sTarget.mu.Lock()
		targetStillLive := sTarget.SessionID == "sid-coal-target"
		sTarget.mu.Unlock()
		if !targetStillLive {
			t.Fatal("Req 1 must not supersede target before Req 2 replays")
		}

		// Req 2 replays, releases claim, and finishes -> BOTH sComp and sTarget hibernate cleanly!
		rep, repRef, ok := sComp.tryReplayRecentCompletedTurnWithCompaction("reqfp-coal-target-1", nil)
		if !ok || rep.Text != "SUMMARY_COALESCED" || repRef != ref {
			t.Fatalf("expected Req 2 to replay result and receive compactionTargetRef, got ok=%v text=%q ref=%v", ok, rep.Text, repRef)
		}
		sComp.releaseTurnClaim()
		finishOneShotCompaction(sComp, rep, nil, false, true, repRef)

		sComp.mu.Lock()
		compSidAfter := sComp.SessionID
		sComp.mu.Unlock()
		sTarget.mu.Lock()
		targetSidAfter := sTarget.SessionID
		targetPendingAfter := sTarget.hasActiveCompactionPendingLocked(time.Now())
		sTarget.mu.Unlock()
		if compSidAfter != "" || targetSidAfter != "" || targetPendingAfter {
			t.Fatalf("expected both sComp and sTarget hibernated with lease cleared, got compSid=%q targetSid=%q pending=%v",
				compSidAfter, targetSidAfter, targetPendingAfter)
		}
	})

	t.Run("concurrent race stress across lease, cap eviction, and supersede", func(t *testing.T) {
		oldMax := MaxLiveWorkers
		MaxLiveWorkers = 3
		defer func() { MaxLiveWorkers = oldMax }()

		gs := NewGatewayServer()
		defer gs.Close()

		var targets []*AcpProjectSession
		for i := 0; i < 6; i++ {
			s := NewAcpProjectSession(fmt.Sprintf("fp:stress-%d:u:12345678", i), "/data/workspace", "sys")
			s.SessionID = fmt.Sprintf("sid-stress-%d", i)
			s.MessageCount = 10
			s.LastUsedAt = time.Now().Add(-time.Duration(i) * time.Second)
			gs.bindSessionDeps(s)
			gs.mu.Lock()
			gs.sessions[s.ConvKey] = s
			gs.mu.Unlock()
			targets = append(targets, s)
		}

		var wg sync.WaitGroup
		for i, s := range targets {
			wg.Add(2)
			go func(idx int, sess *AcpProjectSession) {
				defer wg.Done()
				for k := 0; k < 10; k++ {
					sid := fmt.Sprintf("sid-stress-%d", idx)
					r := sess.addCompactionPendingLease(sid, time.Minute, idx%2 == 0)
					if k == 9 {
						if r != nil {
							r.release()
							sess.supersedeAsCompactionTarget(r.sid, r.markSidRollover)
						}
					} else {
						r.release()
					}
				}
			}(i, s)
			go func(sess *AcpProjectSession) {
				defer wg.Done()
				for k := 0; k < 10; k++ {
					gs.enforceWorkerCapMode(sess, true)
					gs.reapIdleWorkers()
				}
			}(s)
		}
		wg.Wait()

		// Verify JSON stats snapshot remains clean and no leases remain leaked.
		raw, err := json.Marshal(gs.GetStats())
		if err != nil || len(raw) == 0 {
			t.Fatalf("GetStats marshal failed: %v", err)
		}
		for _, s := range targets {
			s.mu.Lock()
			leaked := len(s.compactionLeases)
			s.mu.Unlock()
			if leaked != 0 {
				t.Fatalf("leaked %d compaction leases on %s", leaked, s.ConvKey)
			}
		}
	})
}

func TestCompactionSearchCorpusUnicodeMultiBlockLargePrevSummaryAndToolHeavyAnchors(t *testing.T) {
	t.Run("P0#1 Unicode case-folding length expansion before tags never panics", func(t *testing.T) {
		// İ (U+0130: 2B -> 3B), Ⱥ (U+023A: 2B -> 3B), K (U+212A: 3B -> 1B)
		msgs := []map[string]any{{
			"role": "user",
			"content": strings.Repeat("İȺK", 200) + "\n" +
				"<conversation>\nInvestigate Istanbul shard rebalance telemetry_alpha_9921.\n</conversation>\n" +
				"<previous-summary>\nMilestone gamma_7741 completed cleanly.\n</previous-summary>\n" +
				"Update the previous summary.",
		}}
		corpus := extractCompactionSearchCorpus(msgs)
		if !strings.Contains(corpus, "milestone gamma_7741 completed cleanly.") {
			t.Fatalf("expected <previous-summary> in corpus, got %q", corpus)
		}
		if !strings.Contains(corpus, "investigate istanbul shard rebalance telemetry_alpha_9921.") {
			t.Fatalf("expected <conversation> in corpus, got %q", corpus)
		}
	})

	t.Run("P1#7 multi-block standalone compaction and >2MB previous-summary priority", func(t *testing.T) {
		piSys := "You are a context summarization assistant. Produce structured summaries."
		prevAnchor := "Unique prior milestone checkpoint_zeta_884129 verified across three storage replicas."
		multiBlock := []map[string]any{{
			"role": "user",
			"content": []any{
				map[string]any{"type": "text", "text": "<conversation>\n" + strings.Repeat("verbose diagnostic line 0123456789abcdef\n", 56000) + "\n</conversation>\n<previous-summary>\n" + prevAnchor + "\n</previous-summary>"},
				map[string]any{"type": "text", "text": "Create a structured context checkpoint summary."},
			},
		}}
		if !isStandaloneCompactionSummary(piSys, nil, multiBlock) {
			t.Fatal("expected multi-block standalone compaction to match isStandaloneCompactionSummary")
		}
		corpus := extractCompactionSearchCorpus(multiBlock)
		if !strings.Contains(corpus, normalizeAnchorText(prevAnchor)) {
			t.Fatal("expected <previous-summary> anchor to survive >2MB <conversation> without truncation")
		}
	})

	t.Run("P1#7 multi-message compaction corpus includes earlier messages", func(t *testing.T) {
		msgs := []map[string]any{
			{"role": "user", "content": "Audit payment reconciliation ledger_delta_44102 across three shards."},
			{"role": "assistant", "content": "Audited ledger_delta_44102 successfully."},
			{"role": "user", "content": "Your task is to create a detailed summary of the conversation so far."},
		}
		corpus := extractCompactionSearchCorpus(msgs)
		if !strings.Contains(corpus, "audit payment reconciliation ledger_delta_44102 across three shards.") {
			t.Fatalf("expected multi-message corpus to include messages[0], got %q", corpus)
		}
	})

	t.Run("P2#3 tool-heavy session outward scan still extracts text anchors", func(t *testing.T) {
		msgs := []map[string]any{
			{"role": "user", "content": "short"},
		}
		for i := 0; i < 12; i++ {
			msgs = append(msgs,
				map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "tool_use", "id": fmt.Sprintf("c%d", i), "name": "Read", "input": map[string]any{}}}},
				map[string]any{"role": "user", "content": []any{map[string]any{"type": "tool_result", "tool_use_id": fmt.Sprintf("c%d", i), "content": "ok"}}},
			)
		}
		msgs = append(msgs,
			map[string]any{"role": "user", "content": "Refactor distributed cache invalidation pipeline_omega_77310 for failover safety."},
			map[string]any{"role": "assistant", "content": "Completed refactor of pipeline_omega_77310."},
		)
		anchors := extractSessionContentAnchors(msgs, "")
		found := false
		for _, a := range anchors.Anchors {
			if strings.Contains(a, "pipeline_omega_77310") {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected outward scan to capture text anchor in tool-heavy session, got %+v", anchors.Anchors)
		}
	})
}

func TestPiFallbackClientIDAndFpNonStandaloneDeterministicMatch(t *testing.T) {
	setupM2IsolatedEnv(t)
	gs := NewGatewayServer()
	defer gs.Close()

	// Standard open-source Pi session without custom "# pi 全局规则" -> ClientID is fallback "messages_api".
	sStdPi := NewAcpProjectSession("fp:stdpi11111111111:u:11111111", "/workspace/app", "You are a helpful coding assistant.")
	sStdPi.SessionID = "sid-std-pi"
	sStdPi.MessageCount = 12
	sStdPi.ClientID = fallbackClientForProtocol("Messages").ID // fallback (Known=false)
	sStdPi.contentAnchors = extractSessionContentAnchors([]map[string]any{
		{"role": "user", "content": "Implement zero-downtime compaction_reclaim_alpha_99182 in pure Go gateway."},
	}, "")

	// Known conflicting specific client ("claude_code") with overlapping text must be excluded.
	sClaude := NewAcpProjectSession("fp:cc22222222222222:u:22222222", "/workspace/app", "Claude system")
	sClaude.SessionID = "sid-other-cc"
	sClaude.MessageCount = 12
	sClaude.ClientID = "claude_code"
	sClaude.contentAnchors = extractSessionContentAnchors([]map[string]any{
		{"role": "user", "content": "Implement zero-downtime compaction_reclaim_alpha_99182 in pure Go gateway."},
	}, "")

	sComp := NewAcpProjectSession("fp:comp333333333333:u:33333333", DefaultCwd, "You are a context summarization assistant.")
	gs.mu.Lock()
	gs.sessions[sStdPi.ConvKey] = sStdPi
	gs.sessions[sClaude.ConvKey] = sClaude
	gs.sessions[sComp.ConvKey] = sComp
	gs.mu.Unlock()

	compMsgs := []map[string]any{{
		"role":    "user",
		"content": "<conversation>\nUser: Implement zero-downtime compaction_reclaim_alpha_99182 in pure Go gateway.\n</conversation>\nCreate a structured context checkpoint summary.",
	}}
	piIdent := ClientIdentity{ID: "pi", Label: "Pi", Protocol: "Messages", Known: true}
	ref := gs.findPreCompactionTarget(sComp, sComp.ConvKey, DefaultCwd, "fp-comp", true, piIdent, compMsgs)
	if ref == nil || ref.sess != sStdPi || ref.sid != "sid-std-pi" {
		if ref != nil {
			ref.release()
		}
		t.Fatalf("expected standard Pi session (fallback ClientID) to match while skipping claude_code, got %+v", ref)
	}
	ref.release()

	// P1#7a: Headerless fp: non-standalone compaction (!isStandalone) matches deterministically via fpScope+Cwd+firstUserFp.
	sFpParked := NewAcpProjectSession("fp:headless44444444:u:44444444", "/workspace/proj", "Generic coding rules")
	sFpParked.SessionID = "sid-fp-parked"
	sFpParked.MessageCount = 8
	sFpParked.HistoryFingerprint = "first-user-fp-headless-44"
	sFpParked.ParkedCalls["call-1"] = &ParkedMcpCall{Call: ToolCallItem{ID: "call-1", Name: "Bash"}, ResultChan: make(chan map[string]any, 1)}
	sFpSplit := NewAcpProjectSession("fp:headless44444444:u:44444444#2", "/workspace/proj", "Generic coding rules")
	gs.mu.Lock()
	gs.sessions[sFpParked.ConvKey] = sFpParked
	gs.sessions[sFpSplit.ConvKey] = sFpSplit
	gs.mu.Unlock()

	fpInConvMsgs := []map[string]any{
		{"role": "user", "content": "short"},
		{"role": "assistant", "content": "ok"},
		{"role": "user", "content": "Your task is to create a detailed summary of the conversation so far."},
	}
	ref2 := gs.findPreCompactionTarget(sFpSplit, "fp:headless44444444:u:44444444", "/workspace/proj", "first-user-fp-headless-44", false, ClientIdentity{ID: "messages_api", Known: false}, fpInConvMsgs)
	if ref2 == nil || ref2.sess != sFpParked || ref2.sid != "sid-fp-parked" {
		if ref2 != nil {
			ref2.release()
		}
		t.Fatalf("expected deterministic headerless fp: non-standalone match, got %+v", ref2)
	}
	ref2.release()
}

func TestSingleNewlineSystemPrefixDetection(t *testing.T) {
	// P1#9a: billing header + single \n + Pi compaction prompt with claude-cli UA still detects "pi".
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	req.Header.Set("User-Agent", "claude-cli/2.1.0")
	rawSys := "x-anthropic-billing-header: plan=max\nYou are a context summarization assistant. Produce structured summaries."
	cleanSys := "You are a context summarization assistant. Produce structured summaries."
	ci := DetectClientIdentity(req, map[string]any{}, "fp:single-nl:u:12345678", rawSys, cleanSys, nil)
	if ci.ID != "pi" || !ci.Known {
		t.Fatalf("expected pi client identity for single-newline rawSys + cleanSys, got %+v", ci)
	}

	// P1#9b: single \n joined developer system prompt for Codex checkpoint compaction.
	codexSys := "You are a coding assistant.\nYou are performing a CONTEXT CHECKPOINT COMPACTION."
	if !isCompactionSummaryRequestWithSystem(codexSys, []map[string]any{{"role": "user", "content": "Compact now."}}) {
		t.Fatal("expected single-newline Codex checkpoint compaction system prompt to match")
	}
}

func TestSupersedeWaitsForPhaseClosingTarget(t *testing.T) {
	t.Run("P0#4 waits for PhaseClosing then clears preempted state and SQLite", func(t *testing.T) {
		setupM2IsolatedEnv(t)
		dbPath := filepath.Join(t.TempDir(), "state.db")
		GlobalStore.Init(dbPath)
		defer GlobalStore.Init(GatewayStateDB)
		gs := NewGatewayServer()
		defer gs.Close()

		s := NewAcpProjectSession("sid:closing-target:u:11111111", "/tmp", "sys")
		gs.bindSessionDeps(s)
		s.SessionID = "sid-closing-1"
		s.MessageCount = 10
		s.TotalTurns = 2
		s.persistSession()

		doneCh := make(chan struct{})
		s.mu.Lock()
		s.beginCloseLocked(doneCh)
		s.HibernatedSessionID = "sid-closing-1"
		s.mu.Unlock()
		var closeWg sync.WaitGroup
		closeWg.Add(1)
		go func() {
			defer closeWg.Done()
			time.Sleep(60 * time.Millisecond)
			s.mu.Lock()
			s.finishCloseLocked("")
			close(doneCh)
			s.mu.Unlock()
			s.persistSession()
		}()

		if !s.supersedeAsCompactionTarget("sid-closing-1", true) {
			t.Fatal("expected supersedeAsCompactionTarget to wait for PhaseClosing and clear preempted state")
		}
		closeWg.Wait()
		s.mu.Lock()
		sid, hib, force, why := s.SessionID, s.HibernatedSessionID, s.ForceRolloverNext, s.ForceRolloverReason
		s.mu.Unlock()
		if sid != "" || hib != "" || !force || why != "rollover_compaction" {
			t.Fatalf("unexpected state after PhaseClosing supersede: sid=%q hib=%q force=%v why=%q", sid, hib, force, why)
		}
		GlobalStore.Flush()
		rawDB, err := sql.Open("sqlite", dbPath)
		if err != nil {
			t.Fatalf("open sqlite: %v", err)
		}
		defer rawDB.Close()
		var dbSid string
		var dbMsg int
		if err := rawDB.QueryRow(`SELECT agy_session_id, msg_count FROM conversations WHERE conv_key = ?`, s.ConvKey).Scan(&dbSid, &dbMsg); err != nil {
			t.Fatalf("query db: %v", err)
		}
		if dbSid != "" || dbMsg != 0 {
			t.Fatalf("expected cleared SQLite row after PhaseClosing supersede, got sid=%q msg=%d", dbSid, dbMsg)
		}
	})

	t.Run("P0#4 close failure rollback retries live HibernateIfSession", func(t *testing.T) {
		setupM2IsolatedEnv(t)
		gs := NewGatewayServer()
		defer gs.Close()

		s := NewAcpProjectSession("sid:closing-rollback:u:22222222", "/tmp", "sys")
		gs.bindSessionDeps(s)
		s.SessionID = "sid-closing-rb"
		s.MessageCount = 6

		doneCh := make(chan struct{})
		s.mu.Lock()
		s.beginCloseLocked(doneCh)
		s.mu.Unlock()

		go func() {
			time.Sleep(50 * time.Millisecond)
			s.mu.Lock()
			s.finishCloseLocked("sid-closing-rb") // simulate eng.CloseSession failure restoring SessionID
			close(doneCh)
			s.mu.Unlock()
		}()

		if !s.supersedeAsCompactionTarget("sid-closing-rb", true) {
			t.Fatal("expected supersedeAsCompactionTarget to retry HibernateIfSession after close rollback")
		}
		s.mu.Lock()
		sid, hib, force := s.SessionID, s.HibernatedSessionID, s.ForceRolloverNext
		s.mu.Unlock()
		if sid != "" || hib != "" || !force {
			t.Fatalf("expected live session hibernated with rollover_compaction after retry, got sid=%q hib=%q force=%v", sid, hib, force)
		}
	})
}

func TestHibernateWhileBusyPreservesTargetAndSplitRolloverAtomicity(t *testing.T) {
	setupM2IsolatedEnv(t)
	gs := NewGatewayServer()
	defer gs.Close()

	sTarget := NewAcpProjectSession("sid:waf-target:u:11111111", "/tmp", "sys")
	sTarget.SessionID = "sid-waf-target"
	sTarget.MessageCount = 10
	gs.bindSessionDeps(sTarget)

	sComp := NewAcpProjectSession("sid:waf-target:u:11111111#2", "/tmp", "sys")
	sComp.SessionID = "sid-waf-comp"
	sComp.MessageCount = 0
	sComp.IsBusy = true
	sComp.turnClaimed = true
	sComp.turnClaimCount = 1
	gs.bindSessionDeps(sComp)

	ref := sTarget.addCompactionPendingLease("sid-waf-target", 5*time.Minute, true)
	if ref == nil {
		t.Fatal("expected lease")
	}
	sComp.activeCompactionTarget = ref

	// P1#5: WAF auto-bisect internal HibernateWorker while IsBusy/turnClaimed MUST NOT drop activeCompactionTarget.
	if !sComp.HibernateWorker("waf_autobisect_reset", true) {
		t.Fatal("expected waf_autobisect_reset hibernate to succeed")
	}
	sComp.mu.Lock()
	keptRef := sComp.activeCompactionTarget
	sComp.mu.Unlock()
	sTarget.mu.Lock()
	stillPending := sTarget.hasActiveCompactionPendingLocked(time.Now())
	sTarget.mu.Unlock()
	if keptRef != ref || !stillPending {
		t.Fatalf("expected activeCompactionTarget preserved during busy WAF reset, got keptRef=%v pending=%v", keptRef, stillPending)
	}

	// P1#6: finalizeCompletedTurn on split slot #2 with activeCompactionTarget != nil MUST NOT stamp rollover_compaction on #2.
	sComp.SessionID = "sid-waf-comp-2"
	_, err := sComp.finalizeCompletedTurn(context.Background(), TurnResult{StopReason: "end_turn", Text: "SUMMARY"}, time.Now(), 0,
		[]map[string]any{{"role": "user", "content": "Your task is to create a detailed summary of the conversation so far."}}, "fp1", true, "sid-waf-comp-2", "cold_start", "")
	if err != nil {
		t.Fatalf("finalizeCompletedTurn error: %v", err)
	}
	sComp.mu.Lock()
	sCompForce := sComp.ForceRolloverNext
	sComp.IsBusy = false
	sComp.turnClaimed = false
	sComp.turnClaimCount = 0
	sComp.activeCompactionTarget = nil
	sComp.mu.Unlock()
	if sCompForce {
		t.Fatal("split slot #2 must not receive ForceRolloverNext during finalizeCompletedTurn when activeCompactionTarget != nil")
	}

	// Atomic finishOneShotCompaction clears #2 and supersedes sTarget with rollover_compaction.
	finishOneShotCompaction(sComp, TurnResult{SessionID: "sid-waf-comp-2", StopReason: "end_turn", Text: "SUMMARY"}, nil, false, false, ref)
	sComp.mu.Lock()
	compSid, compForce := sComp.SessionID, sComp.ForceRolloverNext
	sComp.mu.Unlock()
	sTarget.mu.Lock()
	tgtSid, tgtForce, tgtWhy := sTarget.SessionID, sTarget.ForceRolloverNext, sTarget.ForceRolloverReason
	sTarget.mu.Unlock()
	if compSid != "" || compForce || tgtSid != "" || !tgtForce || tgtWhy != "rollover_compaction" {
		t.Fatalf("unexpected atomic split compaction result: comp=(%q,%v) target=(%q,%v,%q)", compSid, compForce, tgtSid, tgtForce, tgtWhy)
	}
}

func TestCoalesceHandoffReq2CancelAndTTLExpiryCleanup(t *testing.T) {
	t.Run("P0#3 concurrent HandleMessagesRequest Req 2 cancel in acquireTurnQueue leaves zero leaks", func(t *testing.T) {
		setupM2IsolatedEnv(t)
		_, gs := startCarbonTestGateway(t)

		// Create live pre-compaction Pi session.
		piSys := "You are a helpful coding assistant."
		sTarget := NewAcpProjectSession("fp:coal-real-target:u:11111111", "/workspace/app", piSys)
		gs.bindSessionDeps(sTarget)
		sTarget.SessionID = "sid-coal-real-target"
		sTarget.MessageCount = 8
		sTarget.ClientID = fallbackClientForProtocol("Messages").ID
		sTarget.contentAnchors = extractSessionContentAnchors([]map[string]any{
			{"role": "user", "content": "Diagnose compaction_coalesce_cancel_marker_55190 across workers."},
		}, "")
		gs.mu.Lock()
		gs.sessions[sTarget.ConvKey] = sTarget
		gs.mu.Unlock()
		compSys := "You are a context summarization assistant. Produce structured summaries."
		compMsgs := []map[string]any{{
			"role":    "user",
			"content": "[FAKE_TTFT_300MS] <conversation>\nUser: Diagnose compaction_coalesce_cancel_marker_55190 across workers.\n</conversation>\nCreate a structured context checkpoint summary.",
		}}
		body := map[string]any{
			"model":      "gemini-4-carbon",
			"max_tokens": 1024,
			"system":     compSys,
			"messages":   compMsgs,
		}

		// Fire Req 1 in background.
		var wg sync.WaitGroup
		var res1 TurnResult
		var err1 error
		wg.Add(1)
		go func() {
			defer wg.Done()
			req1 := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
			res1, err1 = gs.HandleMessagesRequest(req1, body, compMsgs, nil)
		}()

		// Wait until Req 1 has attached compactionPending lease to sTarget (active turn running in RunTurn).
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			sTarget.mu.Lock()
			hasLease := sTarget.hasActiveCompactionPendingLocked(time.Now())
			sTarget.mu.Unlock()
			if hasLease {
				break
			}
			time.Sleep(15 * time.Millisecond)
		}

		// Fire Req 2 with identical payload and a short ctx timeout so it hits findSessionWithActiveTurn,
		// enters acquireTurnQueue, and cancels before/during handoff!
		ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel2()
		req2 := httptest.NewRequest(http.MethodPost, "/v1/messages", nil).WithContext(ctx2)
		_, _ = gs.HandleMessagesRequest(req2, body, compMsgs, nil)

		wg.Wait()
		// Allow soft-detach or normal completion to settle if Req 1 soft-detached.
		if isSoftDetachedErr(err1) {
			// Expire any detached turn deterministically.
			gs.mu.RLock()
			var all []*AcpProjectSession
			for _, s := range gs.sessions {
				all = append(all, s)
			}
			gs.mu.RUnlock()
			for _, s := range all {
				s.AbortDetachedTurn("test_settle")
			}
		} else if err1 == nil && res1.Text == "" {
			t.Fatalf("unexpected empty result from Req 1: %+v", res1)
		}

		// Verify ZERO leaked leases, ZERO stale lastCompletedReqFp, and clean worker slot state!
		sTarget.mu.Lock()
		leakedLeases := len(sTarget.compactionLeases)
		tgtSid := sTarget.SessionID
		sTarget.mu.Unlock()
		if leakedLeases != 0 {
			t.Fatalf("expected 0 leaked compaction leases on sTarget after Req 2 cancel, got %d", leakedLeases)
		}
		if err1 == nil && tgtSid != "" {
			t.Fatalf("expected sTarget superseded when Req 1 completed cleanly after Req 2 cancelled, got sid=%q", tgtSid)
		}
		if err1 == nil && gs.countLiveWorkers() != 0 {
			t.Fatalf("expected 0 live workers after clean Req 1 compaction + cancelled Req 2, got %d", gs.countLiveWorkers())
		}
	})

	t.Run("P0#3 15s TTL expiry cleans lastCompletedReqFp and releases compactionTargetRef", func(t *testing.T) {
		setupM2IsolatedEnv(t)
		gs := NewGatewayServer()
		defer gs.Close()

		sTarget := NewAcpProjectSession("fp:ttl-target:u:11111111", "/tmp", "sys")
		sTarget.SessionID = "sid-ttl-target"
		sTarget.MessageCount = 4
		sComp := NewAcpProjectSession("fp:ttl-comp:u:22222222", "/tmp", "sys")
		sComp.SessionID = "sid-ttl-comp"
		gs.bindSessionDeps(sTarget)
		gs.bindSessionDeps(sComp)
		gs.mu.Lock()
		gs.sessions[sTarget.ConvKey] = sTarget
		gs.sessions[sComp.ConvKey] = sComp
		gs.mu.Unlock()

		ref1 := sTarget.addCompactionPendingLease("sid-ttl-target", 5*time.Minute, false)
		sComp.mu.Lock()
		sComp.lastCompletedReqFp = "expired-fp-1"
		sComp.lastCompletedAt = time.Now().Add(-20 * time.Second)
		sComp.lastCompletedCompactionTarget = ref1
		sComp.mu.Unlock()

		if _, _, ok := sComp.tryReplayRecentCompletedTurnWithCompaction("expired-fp-1", nil); ok {
			t.Fatal("expected expired replay to return false")
		}
		sComp.mu.Lock()
		fpAfter := sComp.lastCompletedReqFp
		sComp.mu.Unlock()
		sTarget.mu.Lock()
		pendingAfter := sTarget.hasActiveCompactionPendingLocked(time.Now())
		sTarget.mu.Unlock()
		if fpAfter != "" || pendingAfter {
			t.Fatalf("expected expired replay to clear lastCompletedReqFp and release ref, got fp=%q pending=%v", fpAfter, pendingAfter)
		}

		// Also verify reapIdleWorkers clears expired lastCompletedCompactionTarget.
		ref2 := sTarget.addCompactionPendingLease("sid-ttl-target", 5*time.Minute, false)
		sComp.mu.Lock()
		sComp.lastCompletedReqFp = "expired-fp-2"
		sComp.lastCompletedAt = time.Now().Add(-25 * time.Second)
		sComp.lastCompletedCompactionTarget = ref2
		sComp.LastUsedAt = time.Now()
		sComp.mu.Unlock()
		gs.reapIdleWorkers()
		sComp.mu.Lock()
		fpAfterReap := sComp.lastCompletedReqFp
		sComp.mu.Unlock()
		sTarget.mu.Lock()
		pendingAfterReap := sTarget.hasActiveCompactionPendingLocked(time.Now())
		sTarget.mu.Unlock()
		if fpAfterReap != "" || pendingAfterReap {
			t.Fatalf("expected reapIdleWorkers to clear expired lastCompletedReqFp and release ref, got fp=%q pending=%v", fpAfterReap, pendingAfterReap)
		}
	})
}

func TestParkedSessOpenAICompactionCallsFinishOneShot(t *testing.T) {
	setupM2IsolatedEnv(t)
	baseURL, gs := startCarbonTestGateway(t)
	hdrs := map[string]string{"x-session-id": "openai-parked-comp-1"}
	tools := []map[string]any{{
		"name":         "Bash",
		"description":  "Run shell command",
		"input_schema": map[string]any{"type": "object", "properties": map[string]any{"command": map[string]any{"type": "string"}}},
	}}

	// Turn 1: park a tool call.
	st1, res1, raw1 := postJSONWithHeaders(t, baseURL+"/v1/messages", hdrs, map[string]any{
		"model":      "gemini-4-carbon",
		"max_tokens": 1024,
		"system":     "OpenAI agent workspace rules",
		"tools":      tools,
		"messages":   []map[string]any{{"role": "user", "content": "[FAKE_TOOL_CALL] inspect state"}},
	})
	if st1 != http.StatusOK {
		t.Fatalf("turn 1 failed: %d %s", st1, string(raw1))
	}
	var parkedID string
	if blocks, ok := res1["content"].([]any); ok {
		for _, elem := range blocks {
			if m, ok := elem.(map[string]any); ok && m["type"] == "tool_use" {
				parkedID, _ = m["id"].(string)
			}
		}
	}
	if parkedID == "" {
		t.Fatalf("expected parked tool_use id, got %+v", res1)
	}

	// Turn 2: OpenAI-style normalized sequence where messages[N-2] is tool_result and messages[N-1] is compaction user prompt.
	// Hits parkedSess branch in HandleMessagesRequest AND isOneShotCompaction == true!
	msgs2 := []map[string]any{
		{"role": "user", "content": "[FAKE_TOOL_CALL] inspect state"},
		{"role": "assistant", "content": res1["content"]},
		{"role": "user", "content": []any{map[string]any{"type": "tool_result", "tool_use_id": parkedID, "content": "done"}}},
		{"role": "user", "content": "Your task is to create a detailed summary of the conversation so far."},
	}
	st2, _, raw2 := postJSONWithHeaders(t, baseURL+"/v1/messages", hdrs, map[string]any{
		"model":      "gemini-4-carbon",
		"max_tokens": 2048,
		"system":     "OpenAI agent workspace rules",
		"tools":      tools,
		"messages":   msgs2,
	})
	if st2 != http.StatusOK {
		t.Fatalf("turn 2 parked compaction failed: %d %s", st2, string(raw2))
	}
	if live := gs.countLiveWorkers(); live != 0 {
		t.Fatalf("expected parkedSess compaction to hibernate immediately (0 live workers), got %d", live)
	}
	var canonical *AcpProjectSession
	gs.mu.RLock()
	for k, s := range gs.sessions {
		if strings.HasPrefix(k, "sid:openai-parked-comp-1:") {
			canonical = s
			break
		}
	}
	gs.mu.RUnlock()
	if canonical == nil {
		t.Fatal("expected canonical sid session")
	}
	canonical.mu.Lock()
	force, why := canonical.ForceRolloverNext, canonical.ForceRolloverReason
	canonical.mu.Unlock()
	if !force || why != "rollover_compaction" {
		t.Fatalf("expected parkedSess compaction to stamp rollover_compaction, got force=%v why=%q", force, why)
	}
}

func TestStoreSnapshotPersistedInDBAndCancelActiveTurnReleasesLease(t *testing.T) {
	t.Run("P2#1 empty session never bumps snapBlobGen and restored zero-turn session clears DB", func(t *testing.T) {
		setupM2IsolatedEnv(t)
		dbPath := filepath.Join(t.TempDir(), "state.db")
		GlobalStore.Init(dbPath)
		defer GlobalStore.Init(GatewayStateDB)
		gs := NewGatewayServer()
		defer gs.Close()

		empty := NewAcpProjectSession("fp:never-used:u:11111111", "/tmp", "sys")
		if _, ok := captureSessionSnapshot(empty); ok {
			t.Fatal("first captureSessionSnapshot on empty session must return false")
		}
		if _, ok := captureSessionSnapshot(empty); ok {
			t.Fatal("second captureSessionSnapshot on empty session must still return false")
		}
		if empty.snapBlobGen != 0 || empty.persistedInDB {
			t.Fatalf("empty session must not mutate snapBlobGen/persistedInDB, got gen=%d persisted=%v", empty.snapBlobGen, empty.persistedInDB)
		}

		// Simulate restored row with TotalTurns == 0 (persistedInDB == true, snapBlobGen == 0).
		restored := NewAcpProjectSession("fp:restored-zero-turns:u:22222222", "/tmp", "sys")
		gs.bindSessionDeps(restored)
		restored.persistedInDB = true
		restored.HibernatedSessionID = "sid-restored-0"
		restored.MessageCount = 4
		restored.persistSession()
		// Reset snapBlobGen to simulate fresh process right after buildSessionFromRow with 0 turns.
		restored.mu.Lock()
		restored.snapBlobGen = 0
		restored.TotalTurns = 0
		restored.persistedInDB = true
		restored.mu.Unlock()

		if !restored.supersedeAsCompactionTarget("sid-restored-0", false) {
			t.Fatal("expected supersedeAsCompactionTarget on restored zero-turn session to succeed")
		}
		GlobalStore.Flush()
		rawDB, err := sql.Open("sqlite", dbPath)
		if err != nil {
			t.Fatalf("open sqlite: %v", err)
		}
		defer rawDB.Close()
		var dbSid string
		var dbMsg int
		if err := rawDB.QueryRow(`SELECT agy_session_id, msg_count FROM conversations WHERE conv_key = ?`, restored.ConvKey).Scan(&dbSid, &dbMsg); err != nil {
			t.Fatalf("query db: %v", err)
		}
		if dbSid != "" || dbMsg != 0 {
			t.Fatalf("expected restored zero-turn session DB row cleared, got sid=%q msg=%d", dbSid, dbMsg)
		}
	})

	t.Run("P2#2 CancelActiveTurn releases detachedTurn.compactionTarget", func(t *testing.T) {
		setupM2IsolatedEnv(t)
		gs := NewGatewayServer()
		defer gs.Close()

		sTarget := NewAcpProjectSession("fp:cancel-tgt:u:11111111", "/tmp", "sys")
		sTarget.SessionID = "sid-cancel-tgt"
		ref := sTarget.addCompactionPendingLease("sid-cancel-tgt", 5*time.Minute, false)
		if ref == nil {
			t.Fatal("expected lease")
		}

		sComp := NewAcpProjectSession("fp:cancel-comp:u:22222222", "/tmp", "sys")
		gs.bindSessionDeps(sComp)
		sComp.SessionID = "sid-cancel-comp"
		sComp.detachedTurn = &detachedTurnState{
			turn:             newActiveTurnState(nil, nil),
			sid:              "sid-cancel-comp",
			compactionTarget: ref,
		}

		sComp.CancelActiveTurn("test_direct_cancel")
		sTarget.mu.Lock()
		stillPending := sTarget.hasActiveCompactionPendingLocked(time.Now())
		sTarget.mu.Unlock()
		if stillPending {
			t.Fatal("expected CancelActiveTurn to release detachedTurn.compactionTarget lease")
		}
	})
}
