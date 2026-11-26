package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"agy-acp-gateway-go/internal/acp"
)

type parkedTierMockEngine struct {
	mu          sync.Mutex
	configCalls []map[string]any
}

func (m *parkedTierMockEngine) EnsureStarted() error                         { return nil }
func (m *parkedTierMockEngine) StopEngine()                                  {}
func (m *parkedTierMockEngine) IsAlive() bool                                { return true }
func (m *parkedTierMockEngine) IsBooting() bool                              { return false }
func (m *parkedTierMockEngine) CurrentGen() uint64                           { return 1 }
func (m *parkedTierMockEngine) Capabilities() acp.Capabilities               { return acp.Capabilities{} }
func (m *parkedTierMockEngine) RegisterListener(string, acp.SessionListener) {}
func (m *parkedTierMockEngine) UnregisterSession(string)                     {}
func (m *parkedTierMockEngine) CloseSession(string) error                    { return nil }
func (m *parkedTierMockEngine) CancelSession(string)                         {}
func (m *parkedTierMockEngine) EnqueueControlReply(map[string]any)           {}
func (m *parkedTierMockEngine) BinaryInfo() map[string]any                   { return nil }
func (m *parkedTierMockEngine) RPC(method string, params any, _ time.Duration) (json.RawMessage, error) {
	if method != "session/set_config_option" {
		return json.RawMessage(`{}`), nil
	}
	pm, _ := params.(map[string]any)
	cid, _ := pm["configId"].(string)
	val, _ := pm["value"].(string)
	m.mu.Lock()
	m.configCalls = append(m.configCalls, map[string]any{"configId": cid, "value": val})
	m.mu.Unlock()
	raw, _ := json.Marshal(map[string]any{
		"configOptions": []map[string]any{{"id": cid, "currentValue": val}},
	})
	return raw, nil
}

func (m *parkedTierMockEngine) tierCalls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for _, c := range m.configCalls {
		if c["configId"] == "thought_level" {
			out = append(out, fmt.Sprint(c["value"]))
		}
	}
	return out
}

// TestParkedZeroCacheTierConsistencyAndNoDoubleStep reproduces the exact production failure mode:
// during a multi-turn parked_mcp_resume tool loop on gemini-4-carbon/high, upstream agy applies
// session/set_config_option(thought_level=medium) only to future session/prompt RPCs while the
// already in-flight prompt keeps running at high. The gateway must:
//  1. Keep liveSessions.thoughtLevel == "high" and expose pendingThoughtLevel == "medium" while parked.
//  2. Record executed thoughtLevel == "high" in TurnHistory and TURN_DONE (matching <sid>.db steps).
//  3. Never send redundant set_config_option RPCs or phantom double-step to "low" while still parked.
//  4. Promote thoughtLevel -> "medium" (clearing pendingThoughtLevel) once the prompt ends.
//  5. Allow stepping down medium -> low only after real zero-cache misses occur at medium.
func TestParkedZeroCacheTierConsistencyAndNoDoubleStep(t *testing.T) {
	t.Cleanup(initConfig)
	t.Setenv("AGY_STATE_DB", "")
	initConfig()

	eng := &parkedTierMockEngine{}
	gs := NewGatewayServer()
	defer gs.Close()

	s := NewAcpProjectSession("sid:parked-tier-consistency", "/tmp", "")
	s.Engine = eng
	s.SessionID = "sess-parked-tier-001"
	s.ModelID = "gemini-4-carbon"
	s.ThoughtLevel = "high"
	s.PromptInFlight = true
	s.ParkedCalls["call_1"] = &ParkedMcpCall{Call: ToolCallItem{ID: "call_1", Name: "Bash"}}

	gs.mu.Lock()
	gs.sessions[s.ConvKey] = s
	gs.mu.Unlock()

	implicitHighSel := &ModelSelection{
		Model:        GetModel("gemini-4-carbon"),
		Tier:         "high",
		Explicit:     true,
		ExplicitTier: false,
	}

	makeZeroCacheTurn := func(ctxIn int, actualTier, stopReason string) TurnResult {
		return TurnResult{
			StopReason: stopReason,
			TtftMs:     12000,
			Usage: &TurnUsage{
				InputTokens:            ctxIn,
				CachedReadTokens:       0,
				OutputTokens:           320,
				RawUncachedInputTokens: ctxIn,
				ContextInputTokens:     ctxIn,
				StepsInPrompt:          1,
				Generations:            1,
				UsageSource:            "db",
				ActualTier:             actualTier,
			},
		}
	}

	// Turn 1: cold_start (establishes baseline)
	s.logTurnStats("cold_start", 16000, makeZeroCacheTurn(60000, "high", "tool_use"))
	// Turns 2-3: two slow zero-cache parked resumes at high -> arms capacityFallbackTier = "medium"
	s.logTurnStats("parked_mcp_resume", 16000, makeZeroCacheTurn(64000, "high", "tool_use"))
	s.logTurnStats("parked_mcp_resume", 16000, makeZeroCacheTurn(68000, "high", "tool_use"))

	s.mu.Lock()
	if s.capacityFallbackTier != "medium" || s.ThoughtLevel != "high" || s.PendingThoughtLevel != "" {
		t.Fatalf("after 2 misses expected fb=medium tier=high pending='', got fb=%q tier=%q pending=%q",
			s.capacityFallbackTier, s.ThoughtLevel, s.PendingThoughtLevel)
	}
	s.mu.Unlock()

	// Turns 4-7: 4 consecutive parked_mcp_resume turns while still PromptInFlight == true.
	for i := 0; i < 4; i++ {
		if err := s.applyParkedResumeModelSelection(implicitHighSel); err != nil {
			t.Fatalf("parked resume %d applyParkedResumeModelSelection err=%v", i, err)
		}
		s.logTurnStats("parked_mcp_resume", 16000, makeZeroCacheTurn(72000+i*4000, "high", "tool_use"))

		s.mu.Lock()
		if s.ThoughtLevel != "high" || s.PendingThoughtLevel != "medium" {
			t.Fatalf("parked turn %d: expected ThoughtLevel=high PendingThoughtLevel=medium, got %q / %q",
				i, s.ThoughtLevel, s.PendingThoughtLevel)
		}
		if s.capacityFallbackTier != "medium" {
			t.Fatalf("parked turn %d: phantom double-step detected! capacityFallbackTier=%q (want medium)", i, s.capacityFallbackTier)
		}
		lastHist := s.TurnHistory[len(s.TurnHistory)-1]
		if lastHist.ThoughtLevel != "high" {
			t.Fatalf("parked turn %d: TurnHistory.ThoughtLevel=%q, want high", i, lastHist.ThoughtLevel)
		}
		s.mu.Unlock()

		live, _ := gs.CollectDashboardTelemetry(map[string]bool{})
		if len(live) != 1 || live[0]["thoughtLevel"] != "high" || live[0]["pendingThoughtLevel"] != "medium" {
			t.Fatalf("parked turn %d: liveSessions mismatch: %#v", i, live)
		}
	}

	// Only ONE set_config_option(thought_level=medium) RPC should have been sent across all 4 parked turns.
	if calls := eng.tierCalls(); len(calls) != 1 || calls[0] != "medium" {
		t.Fatalf("expected exactly one queued thought_level=medium RPC while parked, got %v", calls)
	}

	// Turn 8: final parked_mcp_resume finishes the prompt (PromptInFlight transitions false before logTurnStats).
	s.mu.Lock()
	s.PromptInFlight = false
	delete(s.ParkedCalls, "call_1")
	s.mu.Unlock()
	s.logTurnStats("parked_mcp_resume", 16000, makeZeroCacheTurn(88000, "high", "end_turn"))

	s.mu.Lock()
	endHist := s.TurnHistory[len(s.TurnHistory)-1]
	if endHist.ThoughtLevel != "high" {
		t.Fatalf("final parked turn executed at high, got TurnHistory.ThoughtLevel=%q", endHist.ThoughtLevel)
	}
	if s.ThoughtLevel != "medium" || s.PendingThoughtLevel != "" {
		t.Fatalf("after end_turn expected promoted ThoughtLevel=medium Pending='', got %q / %q", s.ThoughtLevel, s.PendingThoughtLevel)
	}
	s.mu.Unlock()

	liveAfterEnd, _ := gs.CollectDashboardTelemetry(map[string]bool{})
	if len(liveAfterEnd) != 1 || liveAfterEnd[0]["thoughtLevel"] != "medium" || liveAfterEnd[0]["pendingThoughtLevel"] != "" {
		t.Fatalf("after end_turn liveSessions mismatch: %#v", liveAfterEnd)
	}

	// Turns 9-10: next prompt starts at medium (no extra RPC needed) and hits 2 real misses -> NOW steps down to low.
	if err := s.applyModelSelection(*implicitHighSel); err != nil {
		t.Fatalf("next prompt applyModelSelection err=%v", err)
	}
	if calls := eng.tierCalls(); len(calls) != 1 {
		t.Fatalf("expected no redundant RPC when already promoted to medium, got %v", calls)
	}
	s.logTurnStats("sticky_delta", 16000, makeZeroCacheTurn(92000, "medium", "end_turn"))
	s.logTurnStats("sticky_delta", 16000, makeZeroCacheTurn(96000, "medium", "end_turn"))

	s.mu.Lock()
	if s.capacityFallbackTier != "low" {
		t.Fatalf("after 2 real medium misses expected capacityFallbackTier=low, got %q", s.capacityFallbackTier)
	}
	if last := s.TurnHistory[len(s.TurnHistory)-1]; last.ThoughtLevel != "medium" {
		t.Fatalf("medium miss turn should record ThoughtLevel=medium, got %q", last.ThoughtLevel)
	}
	s.mu.Unlock()
}

// TestNeonXHighAndProtocolsLiveFakeACP verifies gemini-4-neon end-to-end across Anthropic, OpenAI,
// and Gemini v1beta endpoints, including xhigh support, protobuf enum 1535 decoding, and ActualTier.
func TestNeonXHighAndProtocolsLiveFakeACP(t *testing.T) {
	setupM2IsolatedEnv(t)
	t.Setenv("FAKE_ACP_WRITE_DB", "1")
	baseURL, gs := startCarbonTestGateway(t)

	// 1. Anthropic /v1/messages with gemini-4-neon-xhigh
	st1, d1, raw1 := m2PostMessages(t, baseURL, "neon-e2e-anth", "gemini-4-neon-xhigh", nil, []map[string]any{
		{"role": "user", "content": "hello neon xhigh"},
	})
	if st1 != 200 || d1["model"] != "gemini-4-neon" || !strings.Contains(string(raw1), "FAKE_OK model=gemini-4-neon tier=xhigh") {
		t.Fatalf("Anthropic neon-xhigh failed: status=%d body=%s", st1, string(raw1))
	}
	sess := m2FindSession(gs, "neon-e2e-anth")
	if sess == nil {
		t.Fatal("expected neon-e2e-anth session")
	}
	sess.mu.Lock()
	sid := sess.SessionID
	gotModel := sess.ModelID
	gotTier := sess.ThoughtLevel
	var gotActualTier string
	var gotHistLen int
	if len(sess.TurnHistory) > 0 {
		gotHistLen = len(sess.TurnHistory)
		if sess.TurnHistory[0].Usage != nil {
			gotActualTier = sess.TurnHistory[0].Usage.ActualTier
		}
	}
	sess.mu.Unlock()
	if gotModel != "gemini-4-neon" || gotTier != "xhigh" {
		t.Fatalf("session state mismatch: model=%q tier=%q", gotModel, gotTier)
	}
	if gotHistLen == 0 || gotActualTier != "xhigh" {
		t.Fatalf("expected Usage.ActualTier=xhigh from steps enum 1535, got histLen=%d actualTier=%q", gotHistLen, gotActualTier)
	}

	steps, _, ok := ReadSessionStepsAfter(sid, -1)
	if !ok || len(steps) == 0 {
		t.Fatalf("expected steps in fakeacp DB for %s", sid)
	}
	if steps[0].ModelID != "gemini-4-neon" || steps[0].ModelLabel != "Gemini 4 Neon" || steps[0].ModelTier != "xhigh" {
		t.Fatalf("unexpected decoded step for neon-xhigh: %+v", steps[0])
	}

	// 2. Gemini /v1beta/models/gemini-4-neon:generateContent with THINKING_LEVEL_XHIGH
	gemBody, _ := json.Marshal(map[string]any{
		"generationConfig": map[string]any{
			"thinkingConfig": map[string]any{"thinkingLevel": "THINKING_LEVEL_XHIGH"},
		},
		"contents": []any{map[string]any{
			"role":  "user",
			"parts": []any{map[string]any{"text": "hello v1beta neon"}},
		}},
	})
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, baseURL+"/v1beta/models/gemini-4-neon:generateContent", bytes.NewReader(gemBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-session-id", "neon-e2e-v1beta")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("v1beta neon request failed: %v", err)
	}
	defer resp.Body.Close()
	rawGem, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || !strings.Contains(string(rawGem), `"modelVersion":"gemini-4-neon"`) || !strings.Contains(string(rawGem), "FAKE_OK model=gemini-4-neon tier=xhigh") {
		t.Fatalf("v1beta neon-xhigh failed: status=%d body=%s", resp.StatusCode, string(rawGem))
	}

	// 3. GET /v1/models and GET /v1beta/models both advertise gemini-4-neon
	rModels, err := http.Get(baseURL + "/v1/models")
	if err != nil {
		t.Fatalf("GET /v1/models failed: %v", err)
	}
	defer rModels.Body.Close()
	mRaw, _ := io.ReadAll(rModels.Body)
	if !strings.Contains(string(mRaw), `"id":"gemini-4-neon"`) {
		t.Fatalf("/v1/models missing gemini-4-neon: %s", string(mRaw))
	}
	rV1b, err := http.Get(baseURL + "/v1beta/models")
	if err != nil {
		t.Fatalf("GET /v1beta/models failed: %v", err)
	}
	defer rV1b.Body.Close()
	vRaw, _ := io.ReadAll(rV1b.Body)
	if !strings.Contains(string(vRaw), `"name":"models/gemini-4-neon"`) {
		t.Fatalf("/v1beta/models missing models/gemini-4-neon: %s", string(vRaw))
	}
}
