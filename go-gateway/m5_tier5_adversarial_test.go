package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func setupM5Tier5IsolatedEnv(t *testing.T) string {
	t.Helper()
	tmp := t.TempDir()
	prevDB := GatewayStateDB
	origPort := GatewayPort
	if GlobalEngine != nil {
		GlobalEngine.StopEngine()
	}
	t.Cleanup(func() {
		if GlobalEngine != nil {
			GlobalEngine.StopEngine()
		}
		tosMu.Lock()
		tosTrippedMem.Store(false)
		tosReasonMem = ""
		tosPersisted = false
		if TosTrippedFile != "" {
			_ = os.Remove(TosTrippedFile)
		}
		tosMu.Unlock()
		initConfig()
		InitGlobalStore(prevDB)
		GatewayPort = origPort
	})
	t.Setenv("AGY_ACP_BIN", fakeAcpBin)
	t.Setenv("AGY_PURE_API_HOME", filepath.Join(tmp, "pure"))
	t.Setenv("AGY_CONV_DIR", filepath.Join(tmp, "convs"))
	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "gateway.db"))
	t.Setenv("AGY_GW_LOG", filepath.Join(tmp, "gateway.log"))
	t.Setenv("AGY_TOS_TRIPPED_FILE", filepath.Join(tmp, "tos-tripped"))
	t.Setenv("AGY_ACP_RECORD", filepath.Join(tmp, "recordings"))
	initConfig()
	InitGlobalStore(GatewayStateDB)
	return tmp
}

func resetTosBreakerForTest() {
	tosMu.Lock()
	tosNegCacheUntilNs.Store(0)
	tosTrippedMem.Store(false)
	tosReasonMem = ""
	tosPersisted = false
	if TosTrippedFile != "" {
		_ = os.Remove(TosTrippedFile)
	}
	tosMu.Unlock()
}

func writeMockAcpScript(t *testing.T, dir, name, body string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(body), 0700); err != nil {
		t.Fatal(err)
	}
	return p
}

// ---------------------------------------------------------------------------
// Bug 1 Regression: dashboard.go annotateSteadySteps / aggregateAnnotatedSteps /
// computeDbStepCacheMisses parity on batch tool outputs & 0-token first steps.
// ---------------------------------------------------------------------------

func TestM5Tier5Bug1_DashboardAggregateSteadyAndZeroFirstStep(t *testing.T) {
	setupM5Tier5IsolatedEnv(t)
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC).Unix()

	// 1. Batch tool output: Step 1 cold 100K, Step 2 RawIn=35K RawCr=99K (cached prefix intact -> "")
	//    plus Step 3 prefix_only miss RawIn=35K RawCr=20K (negative control -> MissResendUsd > 0).
	steps := []ParsedModelStep{
		{SessionID: "sess-batch-1", StepIdx: 1, TsSec: now - 300, ElapsedMs: 5000, ModelID: "gemini-4-carbon", ModelLabel: "Gemini 4 Carbon", ModelFamily: "gemini", QuotaGroup: "gemini", RawIn: 100000, RawCr: 0, OutTok: 300},
		{SessionID: "sess-batch-1", StepIdx: 2, TsSec: now - 240, ElapsedMs: 4000, ModelID: "gemini-4-carbon", ModelLabel: "Gemini 4 Carbon", ModelFamily: "gemini", QuotaGroup: "gemini", RawIn: 35000, RawCr: 99000, OutTok: 300},
		// 0-token metadata step before first real cold-start on sess-zero-first
		{SessionID: "sess-zero-first", StepIdx: 0, TsSec: now - 200, ModelID: "gemini-4-carbon", ModelLabel: "Gemini 4 Carbon", ModelFamily: "gemini", QuotaGroup: "gemini", RawIn: 0, RawCr: 0},
		{SessionID: "sess-zero-first", StepIdx: 1, TsSec: now - 190, ElapsedMs: 2000, ModelID: "gemini-4-carbon", ModelLabel: "Gemini 4 Carbon", ModelFamily: "gemini", QuotaGroup: "gemini", RawIn: 50000, RawCr: 0, OutTok: 200},
		// AlignedIn/AlignedCr fallback + negative RawIn clamp on sess-aligned
		{SessionID: "sess-aligned", StepIdx: 1, TsSec: now - 150, ElapsedMs: 1000, ModelID: "gemini-3.8-flash", ModelLabel: "Gemini 3.8 Flash", ModelFamily: "gemini", QuotaGroup: "gemini", RawIn: 60000, RawCr: 0, OutTok: 100},
		{SessionID: "sess-aligned", StepIdx: 2, TsSec: now - 120, ElapsedMs: 1000, ModelID: "gemini-3.8-flash", ModelLabel: "Gemini 3.8 Flash", ModelFamily: "gemini", QuotaGroup: "gemini", RawIn: 0, RawCr: 0, AlignedIn: 25000, AlignedCr: 59500, OutTok: 100},
		{SessionID: "sess-aligned", StepIdx: 3, TsSec: now - 90, ElapsedMs: 1000, ModelID: "gemini-3.8-flash", ModelLabel: "Gemini 3.8 Flash", ModelFamily: "gemini", QuotaGroup: "gemini", RawIn: -10, RawCr: 84000, OutTok: 100},
	}

	misses := computeDbStepCacheMisses(steps, nil)
	wantMisses := []string{"cold_start", "", "", "cold_start", "cold_start", "", ""}
	if !reflect.DeepEqual(misses, wantMisses) {
		t.Fatalf("computeDbStepCacheMisses = %v, want %v", misses, wantMisses)
	}

	agg := AggregateSteps(steps)
	byMod := map[string]ModelAggregate{}
	for _, m := range agg.Models {
		byMod[m.ModelID] = m
	}
	carb := byMod["gemini-4-carbon"]
	if carb.SteadySteps != 1 || carb.SteadyHits != 1 || carb.SteadyHitRate != 100.0 || carb.MissResendUsd != 0 {
		t.Fatalf("batch tool hit + 0-tok init misclassified on carbon: %+v", carb)
	}
	flash := byMod["gemini-3.8-flash"]
	if flash.SteadySteps != 2 || flash.SteadyHits != 2 || flash.SteadyHitRate != 100.0 || flash.MissResendUsd != 0 {
		t.Fatalf("aligned fallback + negative clamp misclassified on flash: %+v", flash)
	}

	// Negative control: genuine prefix_only miss must charge MissResendUsd and NOT increment SteadyHits
	missOnly := []ParsedModelStep{
		{SessionID: "sess-miss", StepIdx: 1, TsSec: now - 100, ElapsedMs: 2000, ModelID: "gemini-4-carbon", ModelLabel: "Gemini 4 Carbon", ModelFamily: "gemini", QuotaGroup: "gemini", RawIn: 100000, RawCr: 0, OutTok: 100},
		{SessionID: "sess-miss", StepIdx: 2, TsSec: now - 50, ElapsedMs: 2000, ModelID: "gemini-4-carbon", ModelLabel: "Gemini 4 Carbon", ModelFamily: "gemini", QuotaGroup: "gemini", RawIn: 80000, RawCr: 20000, OutTok: 100},
	}
	aggMiss := AggregateSteps(missOnly)
	if len(aggMiss.Models) != 1 || aggMiss.Models[0].SteadySteps != 1 || aggMiss.Models[0].SteadyHits != 0 || aggMiss.Models[0].MissResendUsd <= 0 {
		t.Fatalf("expected prefix_only negative control to charge MissResendUsd with SteadyHits=0, got %+v", aggMiss.Models)
	}
}

// ---------------------------------------------------------------------------
// Bug 2 Regression: computeFirstUserFingerprint clamped at 2 for >=2 stripped
// assistant turns across all 4 protocol adapters.
// ---------------------------------------------------------------------------

func TestM5Tier5Bug2_ComputeFirstUserFingerprintDoubleStrippedAssistant(t *testing.T) {
	setupM5Tier5IsolatedEnv(t)

	u0 := map[string]any{"role": "user", "content": "first user turn"}
	u1 := map[string]any{"role": "user", "content": "second user turn"}
	u2 := map[string]any{"role": "user", "content": "third user turn"}
	a3 := map[string]any{"role": "assistant", "content": "assistant reply 3"}
	u3 := map[string]any{"role": "user", "content": "fourth user turn"}
	a4 := map[string]any{"role": "assistant", "content": "assistant reply 4"}
	u4 := map[string]any{"role": "user", "content": "fifth user turn"}

	fp1 := computeFirstUserFingerprint([]map[string]any{u0})
	fp2 := computeFirstUserFingerprint([]map[string]any{u0, u1})
	fp3 := computeFirstUserFingerprint([]map[string]any{u0, u1, u2})
	fp4 := computeFirstUserFingerprint([]map[string]any{u0, u1, u2, a3, u3})
	fp5 := computeFirstUserFingerprint([]map[string]any{u0, u1, u2, a3, u3, a4, u4})

	if fp2 == "" || fp2 != fp3 || fp3 != fp4 || fp4 != fp5 {
		t.Fatalf("double-stripped assistant rotated fingerprint: fp2=%q fp3=%q fp4=%q fp5=%q", fp2, fp3, fp4, fp5)
	}
	// Negative controls: [u0] vs [u0_new] differ (TestRound2PROTO10), [u0,a0,u1] vs [u0,u1,a1,u2] differ.
	fpOther := computeFirstUserFingerprint([]map[string]any{{"role": "user", "content": "different first turn"}})
	if fp1 == fpOther {
		t.Fatalf("independent 1-message prompts collided: %q", fp1)
	}
	fpNorm1 := computeFirstUserFingerprint([]map[string]any{u0, {"role": "assistant", "content": "a0"}, u1})
	fpNorm2 := computeFirstUserFingerprint([]map[string]any{u0, u1, {"role": "assistant", "content": "a1"}, u2})
	if fpNorm1 != fp1 || fpNorm2 != fp2 || fpNorm1 == fpNorm2 {
		t.Fatalf("expected [u0,a0,u1]==[u0] and [u0,u1,a1,u2]==[u0,u1] and distinct, got norm1=%q norm2=%q", fpNorm1, fpNorm2)
	}

	// Verify all 4 HTTP protocol adapters keep same ConvKey across Turn 2 -> Turn 4 and keep Turn 3 -> Turn 4 on same sessionId (0 rotation!)
	baseURL, gs := startCarbonTestGateway(t)

	checkSessionTurns := func(hdrSid, path, expectedFp string, expectedRolls int, expectedModes []string, turnPayloads []map[string]any) {
		t.Helper()
		for i, payload := range turnPayloads {
			raw, _ := json.Marshal(payload)
			req, err := http.NewRequest("POST", baseURL+path, bytes.NewReader(raw))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("x-claude-code-session-id", hdrSid)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			bodyBytes, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 200 {
				t.Fatalf("%s turn %d failed: %d %s", path, i+1, resp.StatusCode, string(bodyBytes))
			}
		}
		sess := findSessionByConvPrefix(gs, "sid:"+hdrSid+":u:"+expectedFp[:8])
		if sess == nil {
			t.Fatalf("missing expected stable convKey sid:%s:u:%s (%s)", hdrSid, expectedFp[:8], path)
		}
		sess.mu.Lock()
		defer sess.mu.Unlock()
		if sess.CompactionRollovers != expectedRolls || len(sess.TurnHistory) != len(expectedModes) {
			t.Fatalf("%s expected %d rollovers and %d TurnHistory entries on %s, got rolls=%d turns=%+v",
				path, expectedRolls, len(expectedModes), sess.ConvKey, sess.CompactionRollovers, sess.TurnHistory)
		}
		for i, wantMode := range expectedModes {
			if sess.TurnHistory[i].Mode != wantMode {
				t.Fatalf("%s TurnHistory[%d].Mode=%q != %q (turns=%+v)", path, i, sess.TurnHistory[i].Mode, wantMode, sess.TurnHistory)
			}
		}
		last := len(sess.TurnHistory) - 1
		if sess.TurnHistory[last].SessionID != sess.TurnHistory[last-1].SessionID {
			t.Fatalf("%s Turn 4 rotated sessionId: %q -> %q", path, sess.TurnHistory[last-1].SessionID, sess.TurnHistory[last].SessionID)
		}
	}

	modes3 := []string{"cold_start", "compaction_rollover", "sticky_delta"}
	checkSessionTurns("m5-b2-anth", "/v1/messages", fp2, 1, modes3, []map[string]any{
		{"model": "gemini-4-carbon", "max_tokens": 64, "messages": []any{u0}},
		{"model": "gemini-4-carbon", "max_tokens": 64, "messages": []any{u0, u1}},
		{"model": "gemini-4-carbon", "max_tokens": 64, "messages": []any{u0, u1, u2}},
		{"model": "gemini-4-carbon", "max_tokens": 64, "messages": []any{u0, u1, u2, a3, u3}},
	})
	checkSessionTurns("m5-b2-chat", "/v1/chat/completions", fp2, 1, modes3, []map[string]any{
		{"model": "gemini-4-carbon", "messages": []any{u0}},
		{"model": "gemini-4-carbon", "messages": []any{u0, u1}},
		{"model": "gemini-4-carbon", "messages": []any{u0, u1, u2}},
		{"model": "gemini-4-carbon", "messages": []any{u0, u1, u2, a3, u3}},
	})
	checkSessionTurns("m5-b2-resp", "/v1/responses", fp2, 1, modes3, []map[string]any{
		{"model": "gemini-4-carbon", "input": []any{u0}},
		{"model": "gemini-4-carbon", "input": []any{u0, u1}},
		{"model": "gemini-4-carbon", "input": []any{u0, u1, u2}},
		{"model": "gemini-4-carbon", "input": []any{u0, u1, u2, a3, u3}},
	})
	// Gemini /v1beta merges adjacent same-role contents into one user message, so [u0,u1,u2] becomes 1 message and [u0+u1+u2, a3, u3] keeps the exact same Turn-3 fingerprint!
	fpGem3 := computeFirstUserFingerprint([]map[string]any{{
		"role": "user",
		"content": []any{
			map[string]any{"type": "text", "text": "first user turn"},
			map[string]any{"type": "text", "text": "second user turn"},
			map[string]any{"type": "text", "text": "third user turn"},
		},
	}})
	checkSessionTurns("m5-b2-gem", "/v1beta/models/gemini-4-carbon:generateContent", fpGem3, 0, []string{"cold_start", "sticky_delta"}, []map[string]any{
		{"contents": []any{map[string]any{"role": "user", "parts": []any{map[string]any{"text": "first user turn"}}}}},
		{"contents": []any{map[string]any{"role": "user", "parts": []any{map[string]any{"text": "first user turn"}}}, map[string]any{"role": "user", "parts": []any{map[string]any{"text": "second user turn"}}}}},
		{"contents": []any{map[string]any{"role": "user", "parts": []any{map[string]any{"text": "first user turn"}}}, map[string]any{"role": "user", "parts": []any{map[string]any{"text": "second user turn"}}}, map[string]any{"role": "user", "parts": []any{map[string]any{"text": "third user turn"}}}}},
		{"contents": []any{map[string]any{"role": "user", "parts": []any{map[string]any{"text": "first user turn"}}}, map[string]any{"role": "user", "parts": []any{map[string]any{"text": "second user turn"}}}, map[string]any{"role": "user", "parts": []any{map[string]any{"text": "third user turn"}}}, map[string]any{"role": "model", "parts": []any{map[string]any{"text": "assistant reply 3"}}}, map[string]any{"role": "user", "parts": []any{map[string]any{"text": "fourth user turn"}}}}},
	})
}

// ---------------------------------------------------------------------------
// Bug 3 Regression: resumeParkedTurnInternal <-turn.errChan calls
// TripTosCircuitBreaker and preserveOrHibernateOnRpcError.
// ---------------------------------------------------------------------------

func TestM5Tier5Bug3_ParkedResumeErrChanHibernatesAndTripsTos(t *testing.T) {
	setupM5Tier5IsolatedEnv(t)

	setupParked := func(sid string, injectErr error) (*AcpProjectSession, error) {
		sess := NewAcpProjectSession("sid:"+sid, DefaultCwd, "")
		sess.SessionID = sid
		sess.MessageCount = 2
		sess.ModelID = "gemini-4-carbon"
		sess.ThoughtLevel = "medium"
		sess.PromptInFlight = true
		turn1 := newActiveTurnState(nil, func() {})
		turn1.mu.Lock()
		turn1.resolved = true
		turn1.mu.Unlock()
		sess.activeTurn = turn1

		parked := &ParkedMcpCall{
			Call:       ToolCallItem{ID: "acp_11112222333344445555666677778888", Name: "Bash"},
			SessionID:  sid,
			ResultChan: make(chan map[string]any, 1),
			ParkedAt:   time.Now(),
		}
		sess.ParkedCalls[parked.Call.ID] = parked

		go func() {
			<-parked.ResultChan
			// Wait until resumeParkedTurnInternal installs turn2 as activeTurn
			for i := 0; i < 100; i++ {
				sess.mu.Lock()
				t2 := sess.activeTurn
				sess.mu.Unlock()
				if t2 != nil && t2 != turn1 {
					select {
					case t2.errChan <- injectErr:
					default:
					}
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
		}()

		msgs := []map[string]any{
			{"role": "user", "content": "u1"},
			{"role": "assistant", "content": []any{map[string]any{"type": "tool_use", "id": parked.Call.ID, "name": "Bash", "input": map[string]any{}}}},
			{"role": "user", "content": []any{map[string]any{"type": "tool_result", "tool_use_id": parked.Call.ID, "content": "ok"}}},
		}
		matched := []ToolResultInput{{ToolCallID: parked.Call.ID, Content: "ok"}}
		var fd atomic.Int64
		_, err := sess.resumeParkedTurnInternal(context.Background(), matched, msgs, nil, time.Now(), &fd, nil, nil)
		return sess, err
	}

	t.Run("400 RPC error hibernates session", func(t *testing.T) {
		resetTosBreakerForTest()
		sess, err := setupParked("00000000-0000-4000-8000-000000000101", &acpRPCError{Raw: `{"code":-32603,"message":"400 INVALID_ARGUMENT: bad tool result"}`})
		if err == nil {
			t.Fatal("expected error from 400 RPC")
		}
		sess.mu.Lock()
		gotSid := sess.SessionID
		sess.mu.Unlock()
		if gotSid != "" {
			t.Fatalf("expected session hibernated after 400 parked resume errChan, got SessionID=%q", gotSid)
		}
	})

	t.Run("403 ToS error trips circuit breaker", func(t *testing.T) {
		resetTosBreakerForTest()
		t.Cleanup(resetTosBreakerForTest)
		_, err := setupParked("00000000-0000-4000-8000-000000000102", &acpRPCError{Raw: `{"code":403,"message":"PERMISSION_DENIED: violation of Terms of Service"}`})
		if err == nil {
			t.Fatal("expected error from ToS RPC")
		}
		if tripped, _ := IsTosTripped(); !tripped {
			t.Fatal("expected ToS circuit breaker tripped from parked resume errChan")
		}
	})

	t.Run("negative controls: transient 503 preserves warm session; context.Canceled does not hibernate as rpc error", func(t *testing.T) {
		resetTosBreakerForTest()
		sess503, err503 := setupParked("00000000-0000-4000-8000-000000000103", &acpRPCError{Raw: `{"code":-32603,"message":"503 MODEL_CAPACITY_EXHAUSTED: transient"}`})
		if err503 == nil {
			t.Fatal("expected 503 error")
		}
		sess503.mu.Lock()
		sid503 := sess503.SessionID
		sess503.mu.Unlock()
		if sid503 != "00000000-0000-4000-8000-000000000103" {
			t.Fatalf("expected warm session preserved on transient 503, got sid=%q", sid503)
		}

		sessCanc, errCanc := setupParked("00000000-0000-4000-8000-000000000104", context.Canceled)
		if !errors.Is(errCanc, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", errCanc)
		}
		sessCanc.mu.Lock()
		sidCanc := sessCanc.SessionID
		sessCanc.mu.Unlock()
		if sidCanc != "00000000-0000-4000-8000-000000000104" {
			t.Fatalf("context.Canceled on errChan must not trigger prompt_rpc_error hibernate, got sid=%q", sidCanc)
		}
	})
}

// ---------------------------------------------------------------------------
// Bug 4 Regression: Cold-start high_capacity_fallback loop returns retryErr
// instead of breaking into finalizeCompletedTurn with stale 529.
// ---------------------------------------------------------------------------

func TestM5Tier5Bug4_ColdStartFallbackPropagatesRetryErr(t *testing.T) {
	tmp := setupM5Tier5IsolatedEnv(t)

	mockBin := writeMockAcpScript(t, tmp, "mock-acp-bug4.sh", `#!/bin/sh
if [ "$1" = "--version" ] || [ "$1" = "-v" ]; then
  echo "1.5.10"
  exit 0
fi
seq=0
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*)
      id=$(echo "$line" | sed -n 's/.*"id":\([^,}]*\).*/\1/p')
      printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":1,"agentCapabilities":{"loadSession":true,"sessionCapabilities":{"resume":{},"close":{},"load":{}},"mcpCapabilities":{"http":true}}}}\n' "$id"
      ;;
    *'"method":"session/new"'*)
      id=$(echo "$line" | sed -n 's/.*"id":\([^,}]*\).*/\1/p')
      seq=$((seq + 1))
      printf '{"jsonrpc":"2.0","id":%s,"result":{"sessionId":"00000000-0000-4000-8000-%012d"}}\n' "$id" "$seq"
      ;;
    *'"method":"session/set_config_option"'*)
      id=$(echo "$line" | sed -n 's/.*"id":\([^,}]*\).*/\1/p')
      cid=$(echo "$line" | sed -n 's/.*"configId":"\([^"]*\)".*/\1/p')
      val=$(echo "$line" | sed -n 's/.*"value":"\([^"]*\)".*/\1/p')
      printf '{"jsonrpc":"2.0","id":%s,"result":{"configOptions":[{"id":"%s","currentValue":"%s"}]}}\n' "$id" "$cid" "$val"
      ;;
    *'"method":"session/close"'*)
      id=$(echo "$line" | sed -n 's/.*"id":\([^,}]*\).*/\1/p')
      printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id"
      ;;
    *'"method":"session/prompt"'*)
      id=$(echo "$line" | sed -n 's/.*"id":\([^,}]*\).*/\1/p')
      sid=$(echo "$line" | sed -n 's/.*"sessionId":"\([^"]*\)".*/\1/p')
      case "$sid" in
        *000000000001)
          printf '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"%s","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"⚠ MODEL_CAPACITY_EXHAUSTED (code 503): No capacity."}}}}\n' "$sid"
          printf '{"jsonrpc":"2.0","id":%s,"result":{"stopReason":"end_turn","usage":{"inputTokens":100,"outputTokens":5,"cachedReadTokens":0}}}\n' "$id"
          ;;
        *000000000002)
          case "$line" in
            *TRIGGER_TOS*)
              printf '{"jsonrpc":"2.0","id":%s,"error":{"code":403,"message":"PERMISSION_DENIED: violation of Terms of Service"}}\n' "$id"
              ;;
            *)
              printf '{"jsonrpc":"2.0","id":%s,"error":{"code":-32603,"message":"400 INVALID_ARGUMENT: fallback tier bad arg"}}\n' "$id"
              ;;
          esac
          ;;
        *000000000003)
          printf '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"%s","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"⚠ MODEL_CAPACITY_EXHAUSTED (code 503): No capacity."}}}}\n' "$sid"
          printf '{"jsonrpc":"2.0","id":%s,"result":{"stopReason":"end_turn","usage":{"inputTokens":100,"outputTokens":5,"cachedReadTokens":0}}}\n' "$id"
          ;;
        *)
          printf '{"jsonrpc":"2.0","id":%s,"error":{"code":403,"message":"PERMISSION_DENIED: violation of Terms of Service"}}\n' "$id"
          ;;
      esac
      ;;
  esac
done
`)
	t.Setenv("AGY_ACP_BIN", mockBin)
	initConfig()

	baseURL, _ := startCarbonTestGateway(t)

	// Subcase A: fallback returns 400 -> HTTP must return 400 (NOT 529) and no TURN_DONE with stale 503
	codeA, bodyA, rawA := m2PostMessages(t, baseURL, "bug4-400", "gemini-4-carbon", nil, []map[string]any{
		{"role": "user", "content": "TRIGGER_400"},
	})
	if codeA != 400 {
		t.Fatalf("expected HTTP 400 from cold-start fallback 400 RPC error, got %d body=%s", codeA, string(rawA))
	}
	if errMap, _ := bodyA["error"].(map[string]any); errMap["type"] != "invalid_request_error" {
		t.Fatalf("expected invalid_request_error, got %+v", bodyA)
	}
	logBytes, _ := os.ReadFile(GatewayLogFile)
	if strings.Contains(string(logBytes), `[TURN_DONE]`) && strings.Contains(string(logBytes), `MODEL_CAPACITY_EXHAUSTED`) {
		t.Fatalf("found stale TURN_DONE after fallback 400 error:\n%s", string(logBytes))
	}

	// Subcase B: fallback returns ToS 403 -> HTTP must return 503 tos_circuit_breaker (NOT 529)
	t.Cleanup(resetTosBreakerForTest)
	codeB, bodyB, rawB := m2PostMessages(t, baseURL, "bug4-tos", "gemini-4-carbon", nil, []map[string]any{
		{"role": "user", "content": "TRIGGER_TOS"},
	})
	if codeB != 503 {
		t.Fatalf("expected HTTP 503 tos_circuit_breaker from fallback ToS error, got %d body=%s", codeB, string(rawB))
	}
	if errMap, _ := bodyB["error"].(map[string]any); errMap["type"] != "tos_circuit_breaker" {
		t.Fatalf("expected tos_circuit_breaker, got %+v", bodyB)
	}
}

// ---------------------------------------------------------------------------
// Bug 5 Regression: Hot-parked resolved turn must NOT block onEngineExited,
// enforceWorkerCap Pass-2 LRU eviction, or reapIdleWorkers.
// ---------------------------------------------------------------------------

func TestM5Tier5Bug5_HotParkedResolvedTurnLifecycleEvictionReapCrash(t *testing.T) {
	setupM5Tier5IsolatedEnv(t)

	makeHotParked := func(gs *GatewayServer, key, sid, callID string, lastUsed time.Time) *AcpProjectSession {
		s := NewAcpProjectSession(key, DefaultCwd, "")
		s.SessionID = sid
		s.engineGen = 1
		s.MessageCount = 2
		s.LastUsedAt = lastUsed
		s.PromptInFlight = true
		turn := newActiveTurnState(nil, func() {})
		turn.mu.Lock()
		turn.resolved = true
		turn.upstreamDone = true
		turn.mu.Unlock()
		s.activeTurn = turn
		s.ParkedSince = lastUsed
		s.ParkedCalls[callID] = &ParkedMcpCall{
			Call:       ToolCallItem{ID: callID, Name: "Bash"},
			SessionID:  sid,
			ResultChan: make(chan map[string]any, 1),
			ParkedAt:   lastUsed,
		}
		if gs != nil {
			gs.mu.Lock()
			gs.sessions[key] = s
			gs.mu.Unlock()
		}
		return s
	}

	t.Run("5a onEngineExited hibernates hot-parked but skips unresolved activeTurn", func(t *testing.T) {
		s := makeHotParked(nil, "sid:b5a:u:1", "00000000-0000-4000-8000-000000000501", "acp_5a", time.Now())
		s.onEngineExited("00000000-0000-4000-8000-000000000501", 1)
		s.mu.Lock()
		if s.SessionID != "" || s.HibernatedSessionID != "00000000-0000-4000-8000-000000000501" || !s.HibernatedParkedCallIDs["acp_5a"] || len(s.ParkedCalls) != 0 || s.PromptInFlight {
			s.mu.Unlock()
			t.Fatalf("hot-parked onEngineExited failed: sid=%q hib=%q hibCalls=%v parked=%d inFlight=%v",
				s.SessionID, s.HibernatedSessionID, s.HibernatedParkedCallIDs, len(s.ParkedCalls), s.PromptInFlight)
		}
		s.mu.Unlock()

		// Negative control: unresolved activeTurn must NOT be hibernated by onEngineExited
		sLive := NewAcpProjectSession("sid:b5a-live:u:1", DefaultCwd, "")
		sLive.SessionID = "00000000-0000-4000-8000-000000000502"
		sLive.engineGen = 1
		sLive.MessageCount = 2
		sLive.activeTurn = newActiveTurnState(nil, func() {}) // unresolved!
		sLive.onEngineExited("00000000-0000-4000-8000-000000000502", 1)
		sLive.mu.Lock()
		if sLive.SessionID == "" {
			sLive.mu.Unlock()
			t.Fatal("unresolved activeTurn must NOT be hibernated by onEngineExited")
		}
		sLive.mu.Unlock()
	})

	t.Run("5b enforceWorkerCap Pass-2 evicts hot-parked idle >= parkedTtl/2", func(t *testing.T) {
		t.Setenv("AGY_MAX_LIVE_WORKERS", "2")
		t.Setenv("AGY_PARKED_TTL_SEC", "2")
		initConfig()

		gs := NewGatewayServer()
		t.Cleanup(gs.Close)
		now := time.Now()
		s1 := makeHotParked(gs, "sid:b5b-1:u:1", "00000000-0000-4000-8000-000000000511", "acp_5b1", now.Add(-1500*time.Millisecond))
		s2 := makeHotParked(gs, "sid:b5b-2:u:1", "00000000-0000-4000-8000-000000000512", "acp_5b2", now.Add(-1200*time.Millisecond))
		incoming := NewAcpProjectSession("sid:b5b-in:u:1", DefaultCwd, "")
		gs.mu.Lock()
		gs.sessions["sid:b5b-in:u:1"] = incoming
		gs.mu.Unlock()

		gs.enforceWorkerCap(incoming)
		s1.mu.Lock()
		s1Sid, s1Hib, s1HibCalls := s1.SessionID, s1.HibernatedSessionID, s1.HibernatedParkedCallIDs["acp_5b1"]
		s1.mu.Unlock()
		s2.mu.Lock()
		s2Sid := s2.SessionID
		s2.mu.Unlock()
		if s1Sid != "" || s1Hib != "00000000-0000-4000-8000-000000000511" || !s1HibCalls {
			t.Fatalf("expected oldest hot-parked s1 evicted with HibernatedParkedCallIDs, got sid=%q hib=%q callOk=%v", s1Sid, s1Hib, s1HibCalls)
		}
		if s2Sid == "" {
			t.Fatalf("expected newer s2 to stay live when MaxLiveWorkers=2")
		}
	})

	t.Run("5c reapIdleWorkers reaps hot-parked past ParkedTtl but spares unresolved activeTurn", func(t *testing.T) {
		t.Setenv("AGY_PARKED_TTL_SEC", "1")
		initConfig()
		gs := NewGatewayServer()
		t.Cleanup(gs.Close)
		old := time.Now().Add(-5 * time.Second)
		sParked := makeHotParked(gs, "sid:b5c-p:u:1", "00000000-0000-4000-8000-000000000521", "acp_5c1", old)
		sStreaming := NewAcpProjectSession("sid:b5c-s:u:1", DefaultCwd, "")
		sStreaming.SessionID = "00000000-0000-4000-8000-000000000522"
		sStreaming.MessageCount = 2
		sStreaming.LastUsedAt = old
		sStreaming.activeTurn = newActiveTurnState(nil, func() {}) // unresolved!
		gs.mu.Lock()
		gs.sessions["sid:b5c-s:u:1"] = sStreaming
		gs.mu.Unlock()

		gs.reapIdleWorkers()
		sParked.mu.Lock()
		if sParked.SessionID != "" || sParked.HibernatedSessionID != "00000000-0000-4000-8000-000000000521" || !sParked.HibernatedParkedCallIDs["acp_5c1"] {
			sParked.mu.Unlock()
			t.Fatalf("reapIdleWorkers failed to hibernate hot-parked past TTL: sid=%q hib=%q", sParked.SessionID, sParked.HibernatedSessionID)
		}
		sParked.mu.Unlock()

		sStreaming.mu.Lock()
		if sStreaming.SessionID == "" {
			sStreaming.mu.Unlock()
			t.Fatal("reapIdleWorkers must NOT reap unresolved streaming activeTurn")
		}
		sStreaming.mu.Unlock()
	})
}

// ---------------------------------------------------------------------------
// Bug 6 Regression: executeMcpToolCall 35ms planner wait race with
// CancelActiveTurn / HibernateWorker / promptGen rotation leaves 0 phantom ParkedCalls.
// ---------------------------------------------------------------------------

func TestM5Tier5Bug6_ExecuteMcpToolCallCancelRaceNoPhantomParkedCall(t *testing.T) {
	setupM5Tier5IsolatedEnv(t)

	for _, mode := range []string{"cancel", "new_prompt_gen"} {
		t.Run(mode, func(t *testing.T) {
			sid := "00000000-0000-4000-8000-000000000601"
			sess := NewAcpProjectSession("sid:b6-"+mode+":u:1", DefaultCwd, "")
			sess.SessionID = sid
			sess.DeclaredMcpTools = map[string]bool{"Bash": true}
			sess.ToolsCatalog = []ToolDef{{Name: "Bash", McpName: mcpToolName("Bash")}}
			sess.McpToHostMap = map[string]string{mcpToolName("Bash"): "Bash"}
			sess.PromptInFlight = true
			sess.promptGen = 1
			sess.activeTurn = newActiveTurnState(nil, func() {})

			// Create a >=100B dummy db file so readLatestPlannerToolCallsWithWait takes the 35ms sleep loop!
			dbPath := filepath.Join(ConversationsDir(), sid+".db")
			if err := os.MkdirAll(filepath.Dir(dbPath), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(dbPath, bytes.Repeat([]byte("x"), 256), 0600); err != nil {
				t.Fatal(err)
			}

			resCh := make(chan map[string]any, 1)
			go func() {
				resCh <- sess.executeMcpToolCall(context.Background(), "Bash", map[string]any{"command": "echo hi"})
			}()

			// Trigger cancel/rotation inside the 35ms planner wait window
			time.Sleep(12 * time.Millisecond)
			if mode == "cancel" {
				sess.CancelActiveTurn("client_disconnect")
			} else {
				sess.CancelActiveTurn("client_disconnect")
				sess.mu.Lock()
				sess.promptGen++
				sess.PromptInFlight = true
				sess.activeTurn = newActiveTurnState(nil, func() {})
				sess.mu.Unlock()
			}

			mcpRes := <-resCh
			if isErr, _ := mcpRes["isError"].(bool); !isErr {
				t.Fatalf("expected MCP error response when raced with %s, got %+v", mode, mcpRes)
			}
			sess.mu.Lock()
			parkedCnt := len(sess.ParkedCalls)
			parkedZero := sess.ParkedSince.IsZero()
			sess.mu.Unlock()
			if parkedCnt != 0 || !parkedZero {
				t.Fatalf("phantom ParkedCalls leaked after %s race: count=%d parkedSinceZero=%v", mode, parkedCnt, parkedZero)
			}
			if mode == "cancel" && !sess.IsAvailableForNewTurn() {
				t.Fatal("expected IsAvailableForNewTurn()==true after cancel")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Bug 7 Regression: Stale executePromptOrPark goroutine after CancelActiveTurn
// must NOT clobber newer promptGen's PromptInFlight / activeTurn, while
// same-promptGen parked_mcp_resume still replaces origTurn cleanly.
// ---------------------------------------------------------------------------

func TestM5Tier5Bug7_StaleExecutePromptOrParkDoesNotHijackNewTurn(t *testing.T) {
	tmp := setupM5Tier5IsolatedEnv(t)

	mockBin := writeMockAcpScript(t, tmp, "mock-acp-bug7.sh", `#!/bin/sh
if [ "$1" = "--version" ] || [ "$1" = "-v" ]; then
  echo "1.5.10"
  exit 0
fi
seq=0
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*)
      id=$(echo "$line" | sed -n 's/.*"id":\([^,}]*\).*/\1/p')
      printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":1,"agentCapabilities":{"loadSession":true,"sessionCapabilities":{"resume":{},"close":{},"load":{}},"mcpCapabilities":{"http":true}}}}\n' "$id"
      ;;
    *'"method":"session/new"'*)
      id=$(echo "$line" | sed -n 's/.*"id":\([^,}]*\).*/\1/p')
      seq=$((seq + 1))
      printf '{"jsonrpc":"2.0","id":%s,"result":{"sessionId":"00000000-0000-4000-8000-%012d"}}\n' "$id" "$seq"
      ;;
    *'"method":"session/set_config_option"'*)
      id=$(echo "$line" | sed -n 's/.*"id":\([^,}]*\).*/\1/p')
      cid=$(echo "$line" | sed -n 's/.*"configId":"\([^"]*\)".*/\1/p')
      val=$(echo "$line" | sed -n 's/.*"value":"\([^"]*\)".*/\1/p')
      printf '{"jsonrpc":"2.0","id":%s,"result":{"configOptions":[{"id":"%s","currentValue":"%s"}]}}\n' "$id" "$cid" "$val"
      ;;
    *'"method":"session/close"'*)
      id=$(echo "$line" | sed -n 's/.*"id":\([^,}]*\).*/\1/p')
      printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id"
      ;;
    *'"method":"session/cancel"'*)
      id=$(echo "$line" | sed -n 's/.*"id":\([^,}]*\).*/\1/p')
      if [ -n "$id" ]; then
        printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id"
      fi
      ;;
    *'"method":"session/prompt"'*)
      id=$(echo "$line" | sed -n 's/.*"id":\([^,}]*\).*/\1/p')
      sid=$(echo "$line" | sed -n 's/.*"sessionId":"\([^"]*\)".*/\1/p')
      case "$line" in
        *HEALTHY_REQ4*)
          (sleep 0.35; printf '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"%s","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"REQ4_HEALTHY_OK"}}}}\n' "$sid"; printf '{"jsonrpc":"2.0","id":%s,"result":{"stopReason":"end_turn","usage":{"inputTokens":100,"outputTokens":10,"cachedReadTokens":80}}}\n' "$id") &
          ;;
        *SLOW_CANCEL_REQ3*)
          (sleep 0.20; printf '{"jsonrpc":"2.0","id":%s,"result":{"stopReason":"cancelled","usage":{"inputTokens":100,"outputTokens":1,"cachedReadTokens":0}}}\n' "$id") &
          ;;
        *WARMUP_REQ*)
          printf '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"%s","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"WARMUP_OK"}}}}\n' "$sid"
          printf '{"jsonrpc":"2.0","id":%s,"result":{"stopReason":"end_turn","usage":{"inputTokens":50,"outputTokens":5,"cachedReadTokens":0}}}\n' "$id"
          ;;
      esac
      ;;
  esac
done
`)
	t.Setenv("AGY_ACP_BIN", mockBin)
	initConfig()

	baseURL, gs := startCarbonTestGateway(t)

	// Warm up session so REQ #3 and REQ #4 target the exact same open ACP session
	u0 := map[string]any{"role": "user", "content": "WARMUP_REQ"}
	c0, m0, r0 := m2PostMessages(t, baseURL, "bug7-race", "gemini-4-carbon", nil, []map[string]any{u0})
	if c0 != 200 {
		t.Fatalf("warmup failed: %d %s", c0, string(r0))
	}
	a0 := map[string]any{"role": "assistant", "content": m0["content"]}

	// 1. Send REQ #3, wait until activeTurn is installed, then disconnect -> CancelActiveTurn fires while SLOW_CANCEL_REQ3 waits 200ms
	ctx3, cancel3 := context.WithCancel(context.Background())
	req3Done := make(chan error, 1)
	uSlow := map[string]any{"role": "user", "content": "SLOW_CANCEL_REQ3"}
	go func() {
		_, _, err := doGatewayJSON(ctx3, baseURL+"/v1/messages", "x-claude-code-session-id", "bug7-race", map[string]any{
			"model": "gemini-4-carbon", "max_tokens": 128,
			"messages": []map[string]any{u0, a0, uSlow},
		})
		req3Done <- err
	}()
	sess := findSessionByConvPrefix(gs, "sid:bug7-race")
	for i := 0; i < 100; i++ {
		sess.mu.Lock()
		hasActive := sess.activeTurn != nil
		sess.mu.Unlock()
		if hasActive {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	sess.mu.Lock()
	origSid := sess.SessionID
	sess.mu.Unlock()
	cancel3()
	<-req3Done
	// Wait for REQ #3's server handler to finish CancelActiveTurn and release queueSem
	// (still well within SLOW_CANCEL_REQ3's 200ms upstream sleep) so REQ #4 reuses origSid
	// instead of racing into a #2 sibling session on slower CI runners.
	for i := 0; i < 40; i++ {
		sess.mu.Lock()
		busy := sess.isActivelyWorkingLocked() || len(sess.queueSem) > 0
		sess.mu.Unlock()
		if !busy {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	// 2. Immediately send REQ #4 (appending after cancelled uSlow so same sessionId origSid is reused via sticky_delta!)
	//    before REQ #3's upstream session/prompt finishes cancelling!
	code4, body4, err4 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "bug7-race", map[string]any{
		"model": "gemini-4-carbon", "max_tokens": 128,
		"messages": []map[string]any{u0, a0, uSlow, {"role": "user", "content": "HEALTHY_REQ4"}},
	})
	if err4 != nil || code4.StatusCode != 200 || !strings.Contains(body4, "REQ4_HEALTHY_OK") {
		t.Fatalf("REQ #4 was hijacked by stale REQ #3 goroutine: resp=%v err=%v body=%s", code4, err4, body4)
	}
	sess.mu.Lock()
	if sess.SessionID != origSid || sess.promptGen < 3 || sess.PromptInFlight || sess.activeTurn != nil {
		sess.mu.Unlock()
		t.Fatalf("unexpected post-REQ4 session state: sid=%q (want %q) gen=%d inFlight=%v active=%v",
			sess.SessionID, origSid, sess.promptGen, sess.PromptInFlight, sess.activeTurn != nil)
	}
	sess.mu.Unlock()

	// 3. Positive control: legitimate same-promptGen parked_mcp_resume still replaces origTurn cleanly
	t.Run("same-promptGen parked_mcp_resume replaces origTurn cleanly", func(t *testing.T) {
		setupM5Tier5IsolatedEnv(t)
		t.Setenv("AGY_TOOL_RELAY_MS", "2000")
		t.Setenv("AGY_MAX_TOOL_RELAYS", "4")
		initConfig()
		parkURL, parkGs := startCarbonTestGateway(t)
		tools := m2DefaultBashTools()
		u1 := map[string]any{"role": "user", "content": "[FAKE_TOOL_CALL] run bash"}
		c1, r1, raw1 := m2PostMessages(t, parkURL, "bug7-parked-ok", "gemini-4-carbon", tools, []map[string]any{u1})
		if c1 != 200 || r1["stop_reason"] != "tool_use" {
			t.Fatalf("expected tool_use, got %d %s", c1, string(raw1))
		}
		tus := r1["content"].([]any)
		var tcID string
		for _, item := range tus {
			if bm, ok := item.(map[string]any); ok && bm["type"] == "tool_use" {
				tcID, _ = bm["id"].(string)
				break
			}
		}
		c2, r2, raw2 := m2PostMessages(t, parkURL, "bug7-parked-ok", "gemini-4-carbon", tools, []map[string]any{
			u1,
			{"role": "assistant", "content": r1["content"]},
			{"role": "user", "content": []any{map[string]any{"type": "tool_result", "tool_use_id": tcID, "content": "PARKED_OK"}}},
		})
		if c2 != 200 || r2["stop_reason"] != "end_turn" || !strings.Contains(string(raw2), "TOOL_DONE:PARKED_OK") {
			t.Fatalf("expected same-promptGen parked_mcp_resume to succeed, got %d %s", c2, string(raw2))
		}
		pSess := findSessionByConvPrefix(parkGs, "sid:bug7-parked-ok")
		pSess.mu.Lock()
		if pSess.PromptInFlight || pSess.activeTurn != nil || pSess.promptGen != 1 {
			pSess.mu.Unlock()
			t.Fatalf("unexpected post-parked-resume state: inFlight=%v active=%v gen=%d", pSess.PromptInFlight, pSess.activeTurn != nil, pSess.promptGen)
		}
		pSess.mu.Unlock()
	})
}

// ---------------------------------------------------------------------------
// Category 2 Coverage Gaps (Challenger 1 Gaps 1-6 & Challenger 2 Gaps 1-8)
// ---------------------------------------------------------------------------

func TestM5Tier5Cat2_JoinDetachedSettledArgonErrAndMaxDetaches(t *testing.T) {
	setupM5Tier5IsolatedEnv(t)

	// 1. det.settledResult != nil with Argon degradation sets ForceRolloverNext
	s1 := NewAcpProjectSession("sid:join-settled:u:1", DefaultCwd, "")
	s1.SessionID = "00000000-0000-4000-8000-000000000701"
	s1.ModelID = "gemini-4-argon"
	t1 := newActiveTurnState(nil, func() {})
	s1.detachedTurn = &detachedTurnState{
		turn:      t1,
		reqFp:     "1234567890abcdef",
		sid:       s1.SessionID,
		mode:      "sticky_delta",
		turnStart: time.Now(),
		settledResult: &TurnResult{
			StopReason: "end_turn",
			Text:       "SETTLED_PASSIVE_HOLD",
			Thinking:   "THK",
			Usage: &TurnUsage{
				InputTokens: 100, CachedReadTokens: 2000, OutputTokens: 20,
				Degradation: &ArgonDegradationReport{Degraded: true, ServedModel: "gemini-4-argon-shadow"},
			},
		},
	}
	var deltas []string
	res1, joined, err := s1.JoinDetachedTurn(context.Background(), "1234567890abcdef", []map[string]any{{"role": "user", "content": "hi"}}, func(c DeltaChunk) {
		deltas = append(deltas, c.Type+":"+c.Text+c.Thinking)
	})
	if err != nil || !joined || res1.Text != "SETTLED_PASSIVE_HOLD" {
		t.Fatalf("settled JoinDetachedTurn failed: joined=%v err=%v res=%+v", joined, err, res1)
	}
	s1.mu.Lock()
	if !s1.ForceRolloverNext || s1.ForceRolloverReason != "rollover_degraded" {
		s1.mu.Unlock()
		t.Fatalf("expected ForceRolloverNext=true rollover_degraded, got %v %q", s1.ForceRolloverNext, s1.ForceRolloverReason)
	}
	s1.mu.Unlock()
	if len(deltas) != 2 {
		t.Fatalf("expected thinking+text deltas replayed, got %v", deltas)
	}

	// 2. det.turn.errChan error propagates and hibernates on 400
	s2 := NewAcpProjectSession("sid:join-err:u:1", DefaultCwd, "")
	s2.SessionID = "00000000-0000-4000-8000-000000000702"
	s2.MessageCount = 2
	t2 := newActiveTurnState(nil, func() {})
	s2.detachedTurn = &detachedTurnState{turn: t2, reqFp: "abcdef1234567890", sid: s2.SessionID, mode: "sticky_delta", turnStart: time.Now()}
	go func() {
		time.Sleep(10 * time.Millisecond)
		t2.errChan <- &acpRPCError{Raw: `{"code":-32603,"message":"400 INVALID_ARGUMENT: join fail"}`}
	}()
	_, joined2, err2 := s2.JoinDetachedTurn(context.Background(), "abcdef1234567890", nil, nil)
	if !joined2 || err2 == nil {
		t.Fatalf("expected joined error, got joined=%v err=%v", joined2, err2)
	}
	s2.mu.Lock()
	if s2.SessionID != "" {
		s2.mu.Unlock()
		t.Fatalf("expected session hibernated after detached 400 errChan, got %q", s2.SessionID)
	}
	s2.mu.Unlock()

	// 3. 2nd disconnect exhausting maxDetaches aborts cleanly
	t.Setenv("AGY_PREFILL_CANCEL_MIN_ELAPSED_SEC", "0")
	t.Setenv("AGY_PREFILL_CANCEL_GRACE_SEC", "5")
	initConfig()
	s3 := NewAcpProjectSession("sid:join-maxdet:u:1", DefaultCwd, "")
	s3.SessionID = "00000000-0000-4000-8000-000000000703"
	t3 := newActiveTurnState(nil, func() {})
	t3.mu.Lock()
	t3.softDetachCount = 2 // already at max
	t3.mu.Unlock()
	s3.detachedTurn = &detachedTurnState{turn: t3, reqFp: "fedcba0987654321", sid: s3.SessionID, mode: "sticky_delta", turnStart: time.Now()}
	ctx3, cancel3 := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel3()
	_, joined3, err3 := s3.JoinDetachedTurn(ctx3, "fedcba0987654321", []map[string]any{{"role": "user", "content": "hi"}}, nil)
	if !joined3 || !errors.Is(err3, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded on exhausted maxDetaches, got joined=%v err=%v", joined3, err3)
	}
}

func TestM5Tier5Cat2_ParkedResumeCancelAndPromptInFlightRace(t *testing.T) {
	setupM5Tier5IsolatedEnv(t)
	t.Setenv("AGY_PREFILL_CANCEL_MIN_ELAPSED_SEC", "5")
	initConfig()

	// 1. <minElapsed ctx disconnect aborts parked resume & calls CancelActiveTurn
	s1 := NewAcpProjectSession("sid:pr-canc:u:1", DefaultCwd, "")
	s1.SessionID = "00000000-0000-4000-8000-000000000711"
	s1.PromptInFlight = true
	turn1 := newActiveTurnState(nil, func() {})
	turn1.mu.Lock()
	turn1.resolved = true
	turn1.mu.Unlock()
	s1.activeTurn = turn1
	parked := &ParkedMcpCall{Call: ToolCallItem{ID: "acp_c1", Name: "Bash"}, SessionID: s1.SessionID, ResultChan: make(chan map[string]any, 1), ParkedAt: time.Now()}
	s1.ParkedCalls[parked.Call.ID] = parked
	msgs := []map[string]any{
		{"role": "user", "content": "u1"},
		{"role": "assistant", "content": []any{map[string]any{"type": "tool_use", "id": parked.Call.ID, "name": "Bash", "input": map[string]any{}}}},
		{"role": "user", "content": []any{map[string]any{"type": "tool_result", "tool_use_id": parked.Call.ID, "content": "ok"}}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	matched1 := []ToolResultInput{{ToolCallID: parked.Call.ID, Content: "ok"}}
	var fd1 atomic.Int64
	_, err := s1.resumeParkedTurnInternal(ctx, matched1, msgs, nil, time.Now(), &fd1, nil, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
	s1.mu.Lock()
	if !s1.wasCancelledTurn || s1.activeTurn != nil {
		s1.mu.Unlock()
		t.Fatalf("expected CancelActiveTurn cleanup, got wasCancelled=%v active=%v", s1.wasCancelledTurn, s1.activeTurn != nil)
	}
	s1.mu.Unlock()

	// 2. PromptInFlight drops false while waiting for promptSem -> falls back to runNewPromptResume
	s2 := NewAcpProjectSession("sid:pr-race:u:1", DefaultCwd, "")
	s2.SessionID = "00000000-0000-4000-8000-000000000712"
	s2.PromptInFlight = true
	s2.ParkedCalls["acp_r1"] = &ParkedMcpCall{Call: ToolCallItem{ID: "acp_r1", Name: "Bash"}, SessionID: s2.SessionID, ResultChan: make(chan map[string]any, 1)}
	// Hold promptSem briefly, flip PromptInFlight=false, then release sem so L2289 triggers runNewPromptResume
	promptSem <- struct{}{}
	var raceWg sync.WaitGroup
	raceWg.Add(1)
	defer raceWg.Wait()
	go func() {
		defer raceWg.Done()
		time.Sleep(15 * time.Millisecond)
		s2.mu.Lock()
		s2.PromptInFlight = false
		s2.mu.Unlock()
		<-promptSem
	}()
	// Trip ToS locally so runNewPromptResume fails immediately without launching real acp
	TripTosCircuitBreaker("test circuit")
	t.Cleanup(resetTosBreakerForTest)
	matched2 := []ToolResultInput{{ToolCallID: "acp_r1", Content: "ok"}}
	var fd2 atomic.Int64
	_, err2 := s2.resumeParkedTurnInternal(context.Background(), matched2, []map[string]any{
		{"role": "user", "content": "u1"},
		{"role": "assistant", "content": []any{map[string]any{"type": "tool_use", "id": "acp_r1", "name": "Bash", "input": map[string]any{}}}},
		{"role": "user", "content": []any{map[string]any{"type": "tool_result", "tool_use_id": "acp_r1", "content": "ok"}}},
	}, nil, time.Now(), &fd2, nil, nil)
	if err2 == nil || !strings.Contains(err2.Error(), "ToS circuit breaker") {
		t.Fatalf("expected runNewPromptResume via PromptInFlight=false race to hit circuit breaker, got %v", err2)
	}
}

func TestM5Tier5Cat2_KeepaliveIsResolvedPriceUsageQuotaAndModelResolve(t *testing.T) {
	tmp := setupM5Tier5IsolatedEnv(t)

	// 1. activeTurnState.IsResolved truth table
	var nilTurn *activeTurnState
	if !nilTurn.IsResolved() {
		t.Fatal("nil activeTurnState must report IsResolved()==true")
	}
	unres := newActiveTurnState(nil, func() {})
	if unres.IsResolved() {
		t.Fatal("fresh activeTurnState must report IsResolved()==false")
	}
	unres.mu.Lock()
	unres.resolved = true
	unres.mu.Unlock()
	if !unres.IsResolved() {
		t.Fatal("resolved activeTurnState must report IsResolved()==true")
	}

	// 2. RunKeepalivePing warm vs empty
	sWarm := NewAcpProjectSession("sid:ka-warm:u:1", DefaultCwd, "")
	sWarm.SessionID = "00000000-0000-4000-8000-000000000721"
	sWarm.LastCachedReadTokens = 120000
	var kaText string
	resWarm := sWarm.RunKeepalivePing(func(c DeltaChunk) { kaText += c.Text })
	if resWarm.Mode != "keepalive_fast_ack" || resWarm.Usage.CachedReadTokens != 120000 || kaText != "ok" {
		t.Fatalf("unexpected warm keepalive result: %+v text=%q", resWarm, kaText)
	}
	sEmpty := NewAcpProjectSession("sid:ka-empty:u:1", DefaultCwd, "")
	resEmpty := sEmpty.RunKeepalivePing(nil)
	if resEmpty.Usage.CachedReadTokens != 30000 {
		t.Fatalf("expected default 30000 cachedReadTokens on empty keepalive, got %+v", resEmpty.Usage)
	}

	// 3. PriceUsage mid & long context multipliers
	synth := &ModelInfo{
		ID: "synth-model",
		Pricing: ModelPricing{
			InPerM: 2.0, CachedPerM: 0.2, OutPerM: 10.0, UncachedWritePerM: 2.5,
			MidCtxThreshold: 100000, MidCtxMult: 1.5,
			LongCtxThreshold: 200000, LongCtxMult: 2.0,
			LongCtxInMult: 2.5, LongCtxCrMult: 2.0, LongCtxOutMult: 1.5,
		},
	}
	pMid := PriceUsage(synth, 10000, 100000, 1000, 0, 0)
	if pMid.TierMultiplier != 1.5 || pMid.InputCostUsd <= 0 {
		t.Fatalf("expected MidCtxMult=1.5, got %+v", pMid)
	}
	pLong := PriceUsage(synth, 10000, 200000, 1000, 500, 0)
	if pLong.TierMultiplier != 2.0 || pLong.ThinkCostUsd <= 0 || pLong.CostUsd <= pLong.Base1xCostUsd {
		t.Fatalf("expected LongCtxMult=2.0 with split multipliers, got %+v", pLong)
	}

	// 4. ResolveRequestedModel unknown model with explicit tier suffix
	sel := ResolveRequestedModel("unknown-vendor-model-x-high", nil)
	if sel.Explicit || !sel.ExplicitTier || sel.Tier != "high" || sel.Model.ID != DefaultModelID {
		t.Fatalf("unexpected ResolveRequestedModel for unknown+tier: %+v", sel)
	}

	// 5. fetchOfficialQuotaSync singleflight coalescing
	quotaMu.Lock()
	prevCached, prevAt, prevAtt := cachedQuota, cachedQuotaAt, lastQuotaAttemptAt
	cachedQuota = nil
	quotaMu.Unlock()
	t.Cleanup(func() {
		quotaMu.Lock()
		cachedQuota, cachedQuotaAt, lastQuotaAttemptAt = prevCached, prevAt, prevAtt
		quotaMu.Unlock()
	})
	countFile := filepath.Join(tmp, "quota-calls.txt")
	mockQuota := writeMockAcpScript(t, tmp, "mock-quota.sh", fmt.Sprintf(`#!/bin/sh
echo "$2" >> %q
sleep 0.08
echo '{"command":{"data":{"description":"Google AI Ultra","groups":[{"name":"Gemini","description":"g","buckets":[{"id":"b1","name":"b","window":"5h","remaining_fraction":0.8,"reset_time":"2026-11-18T18:00:00Z"}]}]}}}'
`, countFile))
	t.Setenv("AGY_QUOTA_BIN", mockQuota)

	var wg sync.WaitGroup
	snaps := make([]QuotaSnapshot, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			snaps[idx] = fetchOfficialQuotaSync()
		}(i)
	}
	wg.Wait()
	for i, sn := range snaps {
		if sn.Status != "ok" {
			t.Fatalf("caller %d got non-ok quota: %+v", i, sn)
		}
	}
	rawCalls, _ := os.ReadFile(countFile)
	lines := strings.Split(strings.TrimSpace(string(rawCalls)), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected singleflight to invoke AGY_QUOTA_BIN once, got %d calls", len(lines))
	}
}

func TestM5Tier5Cat2_GatewayPoolRestoreOverflowFpAndCleanup(t *testing.T) {
	tmp := setupM5Tier5IsolatedEnv(t)
	gs := NewGatewayServer()
	t.Cleanup(gs.Close)

	// 1. attachRestoredSessionLocked duplicate HibernatedSessionID rejection
	s1 := NewAcpProjectSession("sid:orig-1:u:1", DefaultCwd, "")
	s1.HibernatedSessionID = "00000000-0000-4000-8000-000000000801"
	s2 := NewAcpProjectSession("sid:dup-2:u:1", DefaultCwd, "")
	s2.HibernatedSessionID = "00000000-0000-4000-8000-000000000801"
	gs.mu.Lock()
	if got1 := gs.attachRestoredSessionLocked(s1); got1 != s1 {
		gs.mu.Unlock()
		t.Fatal("expected s1 attached")
	}
	if got2 := gs.attachRestoredSessionLocked(s2); got2 != nil {
		gs.mu.Unlock()
		t.Fatal("expected duplicate HibernatedSessionID s2 rejected")
	}
	gs.mu.Unlock()

	// 2. slot >= 8 overflow fallback key
	base := "sid:overflow-slots:u:1"
	for i := 0; i < 9; i++ {
		s := gs.getOrCreateClaimedSession(base, DefaultCwd, "")
		if s == nil {
			t.Fatalf("slot %d returned nil", i)
		}
		if i == 8 && !strings.Contains(s.ConvKey, "#") {
			t.Fatalf("expected 9th busy slot to use #xxxx overflow key, got %q", s.ConvKey)
		}
	}

	// 3. fp: hibernated parked acp_<32hex> lookup across system-prompt drift
	cryptoID := "acp_0123456789abcdef0123456789abcdef"
	sFp := NewAcpProjectSession("fp:oldsys:agent:a1", DefaultCwd, "")
	sFp.HibernatedSessionID = "00000000-0000-4000-8000-000000000802"
	sFp.HibernatedParkedCallIDs = map[string]bool{cryptoID: true}
	gs.mu.Lock()
	gs.sessions[sFp.ConvKey] = sFp
	gs.mu.Unlock()
	found := gs.findSessionWithParkedToolResult("fp:newdriftedsys:agent:a1", []map[string]any{
		{"role": "user", "content": []any{map[string]any{"type": "tool_result", "tool_use_id": cryptoID, "content": "ok"}}},
	})
	if found != sFp {
		t.Fatalf("expected fp: hibernated parked acp_<32hex> match, got %v", found)
	}
	found.releaseTurnClaim()

	// 4. CleanupExpiredConversations safety gate when appDir == convDir
	convDir := filepath.Join(tmp, "shared-conv")
	if err := os.MkdirAll(convDir, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGY_CONV_DIR", convDir)
	t.Setenv("AGY_APP_CONV_DIR", convDir)
	t.Setenv("AGY_CONV_KEEP_DAYS", "1")
	initConfig()
	if cleaned := gs.CleanupExpiredConversations(time.Now()); cleaned != 0 {
		t.Fatalf("expected CleanupExpiredConversations=0 when appDir==convDir, got %d", cleaned)
	}
}

type mockDeadlineRecorder struct {
	*httptest.ResponseRecorder
	deadlines []time.Time
}

func (m *mockDeadlineRecorder) SetWriteDeadline(t time.Time) error {
	m.deadlines = append(m.deadlines, t)
	return nil
}

func TestM5Tier5Cat2_McpLoopMaxRelaysPlannerWaitAndSseDeadline(t *testing.T) {
	setupM5Tier5IsolatedEnv(t)

	// 1. wouldTripToolLoopLocked 3x identical calls & reset
	s := NewAcpProjectSession("sid:mcp-loop:u:1", DefaultCwd, "")
	s.SessionID = "00000000-0000-4000-8000-000000000901"
	s.MessageCount = 2
	s.DeclaredMcpTools = map[string]bool{"Bash": true}
	s.McpToHostMap = map[string]string{mcpToolName("Bash"): "Bash"}
	s.ToolsCatalog = []ToolDef{{Name: "Bash", McpName: mcpToolName("Bash")}}
	s.PromptInFlight = true
	s.activeTurn = newActiveTurnState(nil, func() {})
	s.mu.Lock()
	s.appendToolSigLocked(`Bash:{"command":"ls"}`)
	if s.wouldTripToolLoopLocked(`Bash:{"command":"ls"}`) {
		s.mu.Unlock()
		t.Fatal("2nd identical call must not trip 3x tool loop")
	}
	s.appendToolSigLocked(`Bash:{"command":"ls"}`)
	if !s.wouldTripToolLoopLocked(`Bash:{"command":"ls"}`) {
		s.mu.Unlock()
		t.Fatal("expected 3rd identical call to trip tool loop")
	}
	s.mu.Unlock()
	loopRes := s.executeMcpToolCall(context.Background(), "Bash", map[string]any{"command": "ls"})
	if isErr, _ := loopRes["isError"].(bool); !isErr || !strings.Contains(fmt.Sprint(loopRes["content"]), "Loop circuit breaker") {
		t.Fatalf("expected Loop circuit breaker error, got %+v", loopRes)
	}

	// 2. maxRelays == 0 triggers relay cap on first timer tick
	t.Setenv("AGY_TOOL_RELAY_MS", "20")
	t.Setenv("AGY_MAX_TOOL_RELAYS", "0")
	initConfig()
	entry := &ParkedMcpCall{Call: ToolCallItem{ID: "acp_r0", Name: "Bash"}, SessionID: s.SessionID, ResultChan: make(chan map[string]any, 1), ParkedAt: time.Now()}
	s.mu.Lock()
	s.ParkedCalls[entry.Call.ID] = entry
	s.mu.Unlock()
	relayRes := s.awaitParkedEntry(context.Background(), entry)
	if !strings.Contains(fmt.Sprint(relayRes["content"]), "Session hibernated") {
		t.Fatalf("expected Session hibernated on maxRelays=0 cap, got %+v", relayRes)
	}
	s.mu.Lock()
	if s.SessionID != "" || s.HibernatedSessionID != "00000000-0000-4000-8000-000000000901" || !s.HibernatedParkedCallIDs["acp_r0"] || s.orphanCapStreak < 1 {
		s.mu.Unlock()
		t.Fatalf("expected maxRelays=0 to hibernate with HibernatedParkedCallIDs and increment orphanCapStreak, got sid=%q hib=%q streak=%d",
			s.SessionID, s.HibernatedSessionID, s.orphanCapStreak)
	}
	s.mu.Unlock()

	// 3. HandleMcpHTTP sets and clears SSE write deadline on client disconnect
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest("POST", "/mcp/"+s.TenantKey, strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"`+mcpToolName("Bash")+`","arguments":{"command":"echo ok"}}}`)).WithContext(ctx)
	req.RemoteAddr = "127.0.0.1:12345"
	req.Header.Set("Authorization", "Bearer "+s.TenantKey)
	rec := &mockDeadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
	go func() {
		time.Sleep(25 * time.Millisecond)
		cancel()
	}()
	s.HandleMcpHTTP(rec, req)
	if len(rec.deadlines) < 2 || rec.deadlines[0].IsZero() || !rec.deadlines[len(rec.deadlines)-1].IsZero() {
		t.Fatalf("expected non-zero SSE write deadline then zero clear deadline, got %v", rec.deadlines)
	}
}

func TestM5Tier5Cat2_SessionLifecyclePermissionsWaitClosingReloadFail(t *testing.T) {
	tmp := setupM5Tier5IsolatedEnv(t)

	// 1. Malformed request_permission payloads & validateDshBridgeIdentity conflicts
	s := NewAcpProjectSession("sid:perm:u:1", DefaultCwd, "")
	s.SessionID = "00000000-0000-4000-8000-000000000951"
	s.PromptInFlight = true
	s.activeTurn = newActiveTurnState(nil, func() {})
	s.McpToHostMap = map[string]string{"ws_Bash_123456": "Bash"}

	if allow, reason := s.authorizeToolCallPermission(map[string]any{
		"toolCallId": "tc1",
		"name":       "mcp_ws-bridge_ws_Other_123456",
		"serverName": "dsh-bridge",
		"toolName":   "ws_Bash_123456",
	}); allow || reason != "conflicting_tool_identity" {
		t.Fatalf("expected conflicting_tool_identity on bridge mismatch, got allow=%v reason=%q", allow, reason)
	}
	if allow, reason := s.authorizeToolCallPermission(map[string]any{
		"name": "unknown_tool",
	}); allow || reason != "unauthorized_tool" {
		t.Fatalf("expected unauthorized_tool on missing toolCallId/unrecognized tool, got allow=%v reason=%q", allow, reason)
	}

	// 2. Concurrent waitIfClosing callers unblock when closingDone closes
	sClose := NewAcpProjectSession("sid:close-wait:u:1", DefaultCwd, "")
	doneCh := make(chan struct{})
	sClose.mu.Lock()
	sClose.closingDone = doneCh
	sClose.mu.Unlock()
	var wg sync.WaitGroup
	var unblocked atomic.Int32
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sClose.waitIfClosing(2 * time.Second)
			unblocked.Add(1)
		}()
	}
	time.Sleep(15 * time.Millisecond)
	close(doneCh)
	wg.Wait()
	if unblocked.Load() != 4 {
		t.Fatalf("expected all 4 waitIfClosing callers unblocked, got %d", unblocked.Load())
	}

	// 3. reloadHibernatedSession when BOTH session/resume and session/load fail
	mockBin := writeMockAcpScript(t, tmp, "mock-acp-reload-fail.sh", `#!/bin/sh
if [ "$1" = "--version" ] || [ "$1" = "-v" ]; then
  echo "1.5.10"
  exit 0
fi
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*)
      id=$(echo "$line" | sed -n 's/.*"id":\([^,}]*\).*/\1/p')
      printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":1,"agentCapabilities":{"loadSession":true,"sessionCapabilities":{"resume":{},"load":{}},"mcpCapabilities":{"http":true}}}}\n' "$id"
      ;;
    *'"method":"session/resume"'*|*'"method":"session/load"'*)
      id=$(echo "$line" | sed -n 's/.*"id":\([^,}]*\).*/\1/p')
      printf '{"jsonrpc":"2.0","id":%s,"error":{"code":-32000,"message":"session not found"}}\n' "$id"
      ;;
  esac
done
`)
	t.Setenv("AGY_ACP_BIN", mockBin)
	initConfig()
	if err := GlobalEngine.EnsureStarted(); err != nil {
		t.Fatal(err)
	}
	sReload := NewAcpProjectSession("sid:reload-fail:u:1", DefaultCwd, "")
	if err := sReload.reloadHibernatedSession("00000000-0000-4000-8000-000000000999", nil, "medium"); err == nil || !strings.Contains(err.Error(), "session not found") {
		t.Fatalf("expected reloadHibernatedSession failure when resume+load fail, got %v", err)
	}
}

func TestM5Tier5Cat2_SchemasToolResultsResponsesAndGeminiRebind(t *testing.T) {
	setupM5Tier5IsolatedEnv(t)

	// 1. sanitizeSchemaNode + sanitizeStructuredOutputSchema
	rawSchema := map[string]any{
		"$schema":              "http://json-schema.org/draft-07/schema#",
		"type":                 []any{"null", "object"},
		"additionalProperties": false,
		"required":             []any{"b", "a", "a"},
		"properties": map[string]any{
			"nestedRef": map[string]any{"$ref": "#/$defs/Item"},
			"anyField":  map[string]any{"anyOf": []any{map[string]any{"type": "null"}, map[string]any{"type": "array"}}},
		},
	}
	cleanTool := sanitizeSchemaNode(rawSchema).(map[string]any)
	if _, hasAP := cleanTool["additionalProperties"]; hasAP || cleanTool["type"] != "object" {
		t.Fatalf("unexpected sanitizeSchemaNode result: %+v", cleanTool)
	}
	if reqs, _ := cleanTool["required"].([]string); !reflect.DeepEqual(reqs, []string{"a", "b"}) {
		t.Fatalf("expected sorted deduped required=[a b], got %+v", cleanTool["required"])
	}
	props := cleanTool["properties"].(map[string]any)
	if props["nestedRef"].(map[string]any)["type"] != "object" {
		t.Fatalf("expected $ref flattened to object, got %+v", props["nestedRef"])
	}
	if props["anyField"].(map[string]any)["items"].(map[string]any)["type"] != "string" {
		t.Fatalf("expected array without items filled, got %+v", props["anyField"])
	}

	cleanStruct := sanitizeStructuredOutputSchema(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"k": map[string]any{"type": "string", "description": "  clean me  "},
		},
	}).(map[string]any)
	if cleanStruct["properties"].(map[string]any)["k"].(map[string]any)["description"] != "clean me" {
		t.Fatalf("unexpected sanitizeStructuredOutputSchema result: %+v", cleanStruct)
	}

	// 2. extractToolResultsFromMessages with trailing steering text + image attachment
	pngB64 := "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="
	trs := extractToolResultsFromMessages([]map[string]any{
		{"role": "assistant", "content": []any{map[string]any{"type": "tool_use", "id": "c1", "name": "Bash", "input": map[string]any{}}}},
		{"role": "user", "content": []any{
			map[string]any{"type": "tool_result", "tool_use_id": "c1", "content": "tool ok"},
			map[string]any{"type": "text", "text": "trailing steering text"},
			map[string]any{"type": "image", "source": map[string]any{"type": "base64", "media_type": "image/png", "data": pngB64}},
		}},
	})
	if len(trs) != 1 || trs[0].TrailingUserText != "trailing steering text" || len(trs[0].TrailingUserImages) != 1 {
		t.Fatalf("expected trailing steering text + image attached, got %+v", trs)
	}

	// 3. Responses API function_call_output array / image / non-standard map parts
	if got := normalizeFunctionOutputContent(map[string]any{"k": "v"}); got != `{"k":"v"}` {
		t.Fatalf("expected non-content map marshaled to JSON, got %#v", got)
	}
	if got := normalizeFunctionOutputContent(map[string]any{"type": "input_image", "image_url": "data:image/png;base64," + pngB64}); !isRecognizedContentPartMap(got.(map[string]any)) {
		t.Fatalf("expected recognized input_image preserved, got %#v", got)
	}
	if blocks := contentValueToBlockSlice(map[string]any{"type": "text", "text": "hi"}); len(blocks) != 1 {
		t.Fatalf("expected single block slice, got %+v", blocks)
	}

	// 4. Gemini parallel/out-of-order functionResponse rebinding on ParkedCalls
	gs := NewGatewayServer()
	t.Cleanup(gs.Close)
	sGem := NewAcpProjectSession("sid:gem-rebind:u:1", DefaultCwd, "")
	sGem.LastDeliveredCallIDs = []string{"acp_aaa", "acp_bbb"}
	sGem.ParkedCalls["acp_aaa"] = &ParkedMcpCall{Call: ToolCallItem{ID: "acp_aaa", Name: "Read"}}
	sGem.ParkedCalls["acp_bbb"] = &ParkedMcpCall{Call: ToolCallItem{ID: "acp_bbb", Name: "Write"}}
	gs.mu.Lock()
	gs.sessions[sGem.ConvKey] = sGem
	gs.mu.Unlock()

	gNorm, err := normalizeGeminiV1BetaRequest("gemini-4-carbon", map[string]any{
		"contents": []any{
			map[string]any{"role": "user", "parts": []any{map[string]any{"text": "go"}}},
			map[string]any{"role": "model", "parts": []any{
				map[string]any{"functionCall": map[string]any{"name": "Write", "args": map[string]any{}}},
				map[string]any{"functionCall": map[string]any{"name": "Read", "args": map[string]any{}}},
			}},
			map[string]any{"role": "user", "parts": []any{
				map[string]any{"functionResponse": map[string]any{"name": "Write", "response": map[string]any{"ok": true}}},
				map[string]any{"functionResponse": map[string]any{"name": "Read", "response": map[string]any{"ok": true}}},
			}},
		},
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	gReq := httptest.NewRequest("POST", "/v1beta/models/gemini-4-carbon:generateContent", nil)
	gReq.Header.Set("x-claude-code-session-id", "gem-rebind")
	gs.rebindUnidentifiedGeminiToolResults(gReq, gNorm)
	gotTrs := extractToolResultsFromMessages(gNorm.Messages)
	if len(gotTrs) != 2 || gotTrs[0].ToolCallID != "acp_bbb" || gotTrs[1].ToolCallID != "acp_aaa" {
		t.Fatalf("expected out-of-order Gemini functionResponse rebound to [acp_bbb, acp_aaa], got %+v", gotTrs)
	}
}

func TestM5Tier5Cat2_AutoContinuePartialNoticeAndWarmFallbackErr(t *testing.T) {
	tmp := setupM5Tier5IsolatedEnv(t)

	// 1. max_tokens AUTO_CONTINUE returning soft error notice after partial reply
	mockBin := writeMockAcpScript(t, tmp, "mock-acp-autocont.sh", `#!/bin/sh
if [ "$1" = "--version" ] || [ "$1" = "-v" ]; then
  echo "1.5.10"
  exit 0
fi
seq=0
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*)
      id=$(echo "$line" | sed -n 's/.*"id":\([^,}]*\).*/\1/p')
      printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":1,"agentCapabilities":{"loadSession":true,"sessionCapabilities":{"resume":{},"close":{},"load":{}},"mcpCapabilities":{"http":true}}}}\n' "$id"
      ;;
    *'"method":"session/new"'*)
      id=$(echo "$line" | sed -n 's/.*"id":\([^,}]*\).*/\1/p')
      seq=$((seq + 1))
      printf '{"jsonrpc":"2.0","id":%s,"result":{"sessionId":"00000000-0000-4000-8000-%012d"}}\n' "$id" "$seq"
      ;;
    *'"method":"session/set_config_option"'*)
      id=$(echo "$line" | sed -n 's/.*"id":\([^,}]*\).*/\1/p')
      cid=$(echo "$line" | sed -n 's/.*"configId":"\([^"]*\)".*/\1/p')
      val=$(echo "$line" | sed -n 's/.*"value":"\([^"]*\)".*/\1/p')
      printf '{"jsonrpc":"2.0","id":%s,"result":{"configOptions":[{"id":"%s","currentValue":"%s"}]}}\n' "$id" "$cid" "$val"
      ;;
    *'"method":"session/close"'*)
      id=$(echo "$line" | sed -n 's/.*"id":\([^,}]*\).*/\1/p')
      printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id"
      ;;
    *'"method":"session/prompt"'*)
      id=$(echo "$line" | sed -n 's/.*"id":\([^,}]*\).*/\1/p')
      sid=$(echo "$line" | sed -n 's/.*"sessionId":"\([^"]*\)".*/\1/p')
      case "$line" in
        *Continue\ exactly\ where\ you\ stopped*)
          printf '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"%s","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"⚠ MODEL_CAPACITY_EXHAUSTED (code 503): overloaded"}}}}\n' "$sid"
          printf '{"jsonrpc":"2.0","id":%s,"result":{"stopReason":"end_turn","usage":{"inputTokens":20,"outputTokens":5,"cachedReadTokens":10}}}\n' "$id"
          ;;
        *)
          printf '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"%s","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"PART1 "}}}}\n' "$sid"
          printf '{"jsonrpc":"2.0","id":%s,"result":{"stopReason":"max_tokens","usage":{"inputTokens":20,"outputTokens":5,"cachedReadTokens":0}}}\n' "$id"
          ;;
      esac
      ;;
  esac
done
`)
	t.Setenv("AGY_ACP_BIN", mockBin)
	initConfig()

	baseURL, _ := startCarbonTestGateway(t)
	code, body, raw := m2PostMessages(t, baseURL, "m5-autocont", "gemini-4-carbon", nil, []map[string]any{
		{"role": "user", "content": "trigger max_tokens"},
	})
	if code != 529 {
		t.Fatalf("expected 529 overloaded_error when AUTO_CONTINUE gets 503 notice after partial text, got code=%d body=%s raw=%s", code, body, string(raw))
	}
}
