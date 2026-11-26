package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	gwcfg "agy-acp-gateway-go/internal/config"
)

func TestMonitorBudgetThreeZonesReconciliationAndWindowRollover(t *testing.T) {
	resetMonitorTripsForTest()
	t.Cleanup(resetMonitorTripsForTest)

	now := time.Now().UTC().Truncate(time.Second)
	ultraAcc := AccountProfile{ID: "u1", PlanTierHint: "ultra"}
	proAcc := AccountProfile{ID: "p1", PlanTierHint: "pro"}

	// Default Ultra (2800/3800/4800) and Pro (1800/2600/3400) caps.
	uSnap := EvaluateMonitorBudget(ultraAcc, now)
	if uSnap.SoftCap != 2800 || uSnap.DrainCap != 3800 || uSnap.HardCap != 4800 || uSnap.Zone != monitorZoneGreen {
		t.Fatalf("unexpected default Ultra caps: %+v", uSnap)
	}
	pSnap := EvaluateMonitorBudget(proAcc, now)
	if pSnap.SoftCap != 1800 || pSnap.DrainCap != 2600 || pSnap.HardCap != 3400 || pSnap.Zone != monitorZoneGreen {
		t.Fatalf("unexpected default Pro caps: %+v", pSnap)
	}

	// Real-time dispatches + monotonic log reconciliation within same 5h window.
	win1 := now.Add(4 * time.Hour).Format(time.RFC3339)
	RecordMonitorToolDispatchAt("u1", 2900, now)
	SyncMonitorBudgetCalls("u1", 2500, win1, now)
	uSnap = EvaluateMonitorBudget(ultraAcc, now)
	if uSnap.Calls5h != 2900 || uSnap.Zone != monitorZoneYellow {
		t.Fatalf("expected monotonic max(2900, 2500)=2900 yellow, got %+v", uSnap)
	}

	// Log catches up higher -> lifts calls5h into red zone (>= 3800).
	SyncMonitorBudgetCalls("u1", 3850, win1, now)
	uSnap = EvaluateMonitorBudget(ultraAcc, now)
	if uSnap.Calls5h != 3850 || uSnap.Zone != monitorZoneRed || uSnap.Tripped {
		t.Fatalf("expected soft-red at 3850, got %+v", uSnap)
	}

	// 5h window rolls over to win2 -> resets counters cleanly to green!
	win2 := now.Add(9 * time.Hour).Format(time.RFC3339)
	SyncMonitorBudgetCalls("u1", 12, win2, now.Add(5*time.Hour))
	uSnap = EvaluateMonitorBudget(ultraAcc, now.Add(5*time.Hour))
	if uSnap.Calls5h != 12 || uSnap.Zone != monitorZoneGreen {
		t.Fatalf("expected clean reset to 12/green on 5h window rollover, got %+v", uSnap)
	}

	// Auto-calibration ignores < 500 calls, lowers hardCap when 500 <= calls < hardCap (soft=60%, drain=80%).
	RecordMonitorToolDispatchAt("p1", 300, now)
	if autoCalibrateMonitorCapOnTrip("p1", now) {
		t.Fatal("must not auto-calibrate below 500 calls")
	}
	RecordMonitorToolDispatchAt("p1", 1700, now) // total 2000 < 3400
	if !autoCalibrateMonitorCapOnTrip("p1", now) {
		t.Fatal("expected auto-calibration at 2000 < 3400")
	}
	pSnap = EvaluateMonitorBudget(proAcc, now)
	if pSnap.HardCap != 2000 || pSnap.SoftCap != 1200 || pSnap.DrainCap != 1600 || pSnap.Zone != monitorZoneRed {
		t.Fatalf("expected calibrated Pro caps 1200/1600/2000, got %+v", pSnap)
	}
}

func TestMonitorBudgetHasToolsDetectionAcrossAllProtocols(t *testing.T) {
	cases := []struct {
		name     string
		path     string
		body     string
		wantTool bool
		wantCID  string
	}{
		{
			name:     "anthropic plain text mentioning tool_result prose",
			path:     "/v1/messages",
			body:     `{"model":"gemini-4-neon","messages":[{"role":"user","content":"explain what tool_result means"}]}`,
			wantTool: false,
		},
		{
			name:     "anthropic with tools",
			path:     "/v1/messages",
			body:     `{"model":"gemini-4-neon","tools":[{"name":"Bash","input_schema":{"type":"object"}}],"messages":[{"role":"user","content":"hi"}]}`,
			wantTool: true,
		},
		{
			name:     "anthropic tool_result continuation without tools field",
			path:     "/v1/messages",
			body:     `{"model":"gemini-4-neon","messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"acp_anth_1","content":"ok"}]}]}`,
			wantTool: true,
			wantCID:  "acp_anth_1",
		},
		{
			name:     "openai chat plain text",
			path:     "/v1/chat/completions",
			body:     `{"model":"gemini-4-neon","messages":[{"role":"user","content":"hello"}]}`,
			wantTool: false,
		},
		{
			name:     "openai chat pydantic null tool_calls and function_call on plain text assistant turn",
			path:     "/v1/chat/completions",
			body:     `{"model":"gemini-4-neon","messages":[{"role":"user","content":"hi"},{"role":"assistant","content":"hello","tool_calls":null,"function_call":null},{"role":"user","content":"next"}]}`,
			wantTool: false,
		},
		{
			name:     "openai chat whitespace empty tool_calls and function_call",
			path:     "/v1/chat/completions",
			body:     `{"model":"gemini-4-neon","messages":[{"role":"assistant","content":"hello","tool_calls":[ ],"function_call":{ }},{"role":"user","content":"next"}]}`,
			wantTool: false,
		},
		{
			name:     "openai chat non-empty tool_calls",
			path:     "/v1/chat/completions",
			body:     `{"model":"gemini-4-neon","messages":[{"role":"assistant","tool_calls":[{"id":"c1","type":"function","function":{"name":"Bash","arguments":"{}"}}]}]}`,
			wantTool: true,
		},
		{
			name:     "openai chat legacy functions",
			path:     "/v1/chat/completions",
			body:     `{"model":"gemini-4-neon","functions":[{"name":"get_weather"}],"messages":[{"role":"user","content":"hi"}]}`,
			wantTool: true,
		},
		{
			name:     "openai chat role=tool continuation",
			path:     "/v1/chat/completions",
			body:     `{"model":"gemini-4-neon","messages":[{"role":"user","content":"hi"},{"role":"tool","tool_call_id":"acp_oai_2","content":"done"}]}`,
			wantTool: true,
			wantCID:  "acp_oai_2",
		},
		{
			name:     "openai responses plain string input",
			path:     "/v1/responses",
			body:     `{"model":"gemini-4-neon","input":"summarize this"}`,
			wantTool: false,
		},
		{
			name:     "openai responses function_call_output continuation",
			path:     "/v1/responses",
			body:     `{"model":"gemini-4-neon","input":[{"type":"function_call_output","call_id":"acp_resp_3","output":"ok"}]}`,
			wantTool: true,
			wantCID:  "acp_resp_3",
		},
		{
			name:     "gemini plain text with empty functionDeclarations",
			path:     "/v1beta/models/gemini-3.8-flash:generateContent",
			body:     `{"tools":[{"functionDeclarations":[]}],"contents":[{"role":"user","parts":[{"text":"OK"}]}]}`,
			wantTool: false,
		},
		{
			name:     "gemini plain text with null and empty-object functionCall fields",
			path:     "/v1beta/models/gemini-3.8-flash:generateContent",
			body:     `{"contents":[{"role":"model","parts":[{"text":"hi","functionCall":null,"function_call":{ }}]},{"role":"user","parts":[{"text":"next"}]}]}`,
			wantTool: false,
		},
		{
			name:     "gemini non-empty functionCall continuation",
			path:     "/v1beta/models/gemini-3.8-flash:generateContent",
			body:     `{"contents":[{"role":"model","parts":[{"functionCall":{"name":"Bash","args":{}}}]}]}`,
			wantTool: true,
		},
		{
			name:     "gemini with functionDeclarations",
			path:     "/v1beta/models/gemini-3.8-flash:generateContent",
			body:     `{"tools":[{"functionDeclarations":[{"name":"Bash"}]}],"contents":[{"role":"user","parts":[{"text":"run"}]}]}`,
			wantTool: true,
		},
		{
			name:     "gemini functionResponse continuation",
			path:     "/v1beta/models/gemini-3.8-flash:streamGenerateContent",
			body:     `{"contents":[{"role":"user","parts":[{"functionResponse":{"id":"acp_gem_4","name":"Bash","response":{"ok":true}}}]}]}`,
			wantTool: true,
			wantCID:  "acp_gem_4",
		},
		{
			name:     "count_tokens is non-upstream and zero-tool",
			path:     "/v1/messages/count_tokens",
			body:     `{"model":"gemini-4-neon","tools":[{"name":"Bash"}],"messages":[{"role":"user","content":"hi"}]}`,
			wantTool: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.body))
			h := extractRequestRoutingHintsV2(req)
			if h.hasTools != tc.wantTool {
				t.Fatalf("hasTools=%v want %v", h.hasTools, tc.wantTool)
			}
			if tc.wantCID != "" {
				found := false
				for _, id := range h.toolResultCallIDs {
					if id == tc.wantCID {
						found = true
						break
					}
				}
				if !found {
					t.Fatalf("expected toolResultCallIDs to include %q, got %v", tc.wantCID, h.toolResultCallIDs)
				}
			}
		})
	}
}

func TestMonitorBudgetThreeZoneRoutingAndZeroToolPreference(t *testing.T) {
	am := newTripRoutingManager(t)
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	now := time.Now()

	// 1. Fill-first priority-before-spread (v2.3.18) + equal-priority Green zone spreading (AGY_MONITOR_BUDGET_SPREAD=1):
	//    a1 (Priority 1, 800 calls = 16.7% Green) STILL beats idle a2 (Priority 2, 0 calls) because priority beats spread.
	RecordMonitorToolDispatchAt("a1", 800, now)
	prioBeatPick := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:prio-before-spread", HasTools: true})
	if prioBeatPick.DenyErr != nil || prioBeatPick.Account.ID != "a1" {
		t.Fatalf("expected fill-first priority-1 Green a1 to beat idle priority-2 a2, got %q err=%v", prioBeatPick.Account.ID, prioBeatPick.DenyErr)
	}
	//    With equal priority (a1 P1, a2 P1), green spread steers new tool session from a1 (800 calls) to fresher a2 (0 calls)!
	am.mu.Lock()
	am.accounts[1].Priority = 1
	am.mu.Unlock()
	spreadPick := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:spread-1", HasTools: true})
	if spreadPick.DenyErr != nil || spreadPick.Account.ID != "a2" {
		t.Fatalf("expected equal-priority green spread to steer new tool session to fresher a2, got %q err=%v", spreadPick.Account.ID, spreadPick.DenyErr)
	}

	// 2. Yellow zone soft-drain:
	//    Seed a sticky session on a2, push a2 to Yellow (3000 calls: [2800, 3800)), keep a1 & ultra Green.
	//    - Sticky continuation on a2 MUST STAY PINNED (preserving U8 Prompt Cache!).
	//    - NEW tool session MUST skip Yellow a2 and pick Green peer!
	//    - Zero-tool request MUST slightly prefer Yellow a2 (healthy main quota) over Green peers!
	RecordMonitorToolDispatchAt("a2", 3000, now)
	contYellow := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:spread-1", IsContinuation: true, HasTools: true})
	if contYellow.DenyErr != nil || contYellow.Account.ID != "a2" || !contYellow.IsStickyHit {
		t.Fatalf("sticky tool continuation in Yellow zone must stay pinned on a2, got acc=%q sticky=%v err=%v", contYellow.Account.ID, contYellow.IsStickyHit, contYellow.DenyErr)
	}
	newWhileYellow := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:new-green", HasTools: true})
	if newWhileYellow.DenyErr != nil || newWhileYellow.Account.ID == "a2" {
		t.Fatalf("new tool session must skip Yellow a2 when Green peer exists, got %q err=%v", newWhileYellow.Account.ID, newWhileYellow.DenyErr)
	}
	zeroToolWarm := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:text-warm", HasTools: false})
	if zeroToolWarm.DenyErr != nil || zeroToolWarm.Account.ID != "a2" {
		t.Fatalf("zero-tool request with spread=1 must prefer Yellow a2 to save Green headroom, got %q err=%v", zeroToolWarm.Account.ID, zeroToolWarm.DenyErr)
	}

	// 3. Soft-Red zone (>= drainCap 3800, NOT tripped):
	//    Push a2 to 3900 (Red); sticky continuation hops off a2 to Green peer AND cancels old session!
	RecordMonitorToolDispatchAt("a2", 900, now) // total 3900 >= 3800
	var hoppedFrom string
	am.setMonitorTripHopCancelHookForTest(func(accID, _ string, _ []string, _ string) {
		hoppedFrom = accID
	})
	hopRed := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:spread-1", IsContinuation: true, HasTools: true})
	if hopRed.DenyErr != nil || hopRed.Account.ID == "a2" || hopRed.IsStickyHit {
		t.Fatalf("sticky continuation on soft-Red a2 must hop to healthy peer, got acc=%q sticky=%v err=%v", hopRed.Account.ID, hopRed.IsStickyHit, hopRed.DenyErr)
	}
	if hoppedFrom != "a2" {
		t.Fatalf("expected soft-Red hop to cancel old parked session on a2, got %q", hoppedFrom)
	}

	// 4. All peers in soft-Red (no Green/Yellow peers left, NO actual 429 lockout):
	//    Must fall back gracefully rather than returning premature 429!
	RecordMonitorToolDispatchAt("a1", 3200, now)    // total 4000 >= 3800
	RecordMonitorToolDispatchAt("ultra", 4100, now) // total 4100 >= 3800
	softFallback := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:soft-red-all", HasTools: true})
	if softFallback.DenyErr != nil {
		t.Fatalf("all-soft-red without actual 429 lockout must fallback before 429, got err=%v", softFallback.DenyErr)
	}
}

func TestMonitorTripHopCancelsLocalAndFederatedChildParkedSessions(t *testing.T) {
	// 1. Local Primary cancellation (both by explicit sid: and by headerless fp: call_id).
	am := newTripRoutingManager(t)
	gs := NewGatewayServer()
	defer gs.Close()
	am.SetPrimaryCancelParkedFn(gs.CancelParkedConversation)

	// Create two live parked sessions on primary ("ultra"): one sid:, one headerless fp:.
	sSid := NewAcpProjectSession("sid:conv-hop-local:u:11112222", DefaultCwd, "")
	sSid.SessionID = "00000000-0000-4000-8000-000000000801"
	sSid.ParkedCalls["acp_sid_call"] = &ParkedMcpCall{Call: ToolCallItem{ID: "acp_sid_call", Name: "Bash"}, ResultChan: make(chan map[string]any, 1)}

	sFp := NewAcpProjectSession("fp:deadbeef:u:33334444", DefaultCwd, "")
	sFp.SessionID = "00000000-0000-4000-8000-000000000802"
	sFp.ParkedCalls["acp_fp_call"] = &ParkedMcpCall{Call: ToolCallItem{ID: "acp_fp_call", Name: "Read"}, ResultChan: make(chan map[string]any, 1)}

	gs.mu.Lock()
	gs.sessions[sSid.ConvKey] = sSid
	gs.sessions[sFp.ConvKey] = sFp
	gs.mu.Unlock()

	// Seed sticky pins on primary ("ultra") then trip ultra.
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	am.stickyMu.Lock()
	am.stickyLRU.put("anon|hdr:conv-hop-local|gemini-4-neon", "ultra", "gemini-4-neon", time.Now(), time.Hour, 100)
	am.stickyLRU.put("anon|fp:clienthash|gemini-4-neon", "ultra", "gemini-4-neon", time.Now(), time.Hour, 100)
	am.stickyMu.Unlock()
	setMonitorTripForTest("ultra", time.Now().Add(time.Hour))

	resSid := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:conv-hop-local", IsContinuation: true, HasTools: true})
	if resSid.DenyErr != nil || resSid.Account.ID == "ultra" {
		t.Fatalf("expected hop off primary ultra, got %q err=%v", resSid.Account.ID, resSid.DenyErr)
	}
	sSid.mu.Lock()
	sidAfter := sSid.SessionID
	parkedAfter := len(sSid.ParkedCalls)
	sSid.mu.Unlock()
	if sidAfter != "" || parkedAfter != 0 {
		t.Fatalf("expected primary sid: parked session cancelled on MONITOR_TRIP_HOP, got sid=%q parked=%d", sidAfter, parkedAfter)
	}

	resFp := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{
		SessionKey:        "fp:clienthash",
		IsContinuation:    true,
		HasTools:          true,
		ToolResultCallIDs: []string{"acp_fp_call"},
	})
	if resFp.DenyErr != nil || resFp.Account.ID == "ultra" {
		t.Fatalf("expected fp: hop off primary ultra, got %q err=%v", resFp.Account.ID, resFp.DenyErr)
	}
	sFp.mu.Lock()
	fpSidAfter := sFp.SessionID
	sFp.mu.Unlock()
	if fpSidAfter != "" {
		t.Fatalf("expected headerless fp: parked session matched by call_id and cancelled, got sid=%q", fpSidAfter)
	}

	// 2. Federated child peer cancellation via POST /api/internal/cancel-parked-session!
	var gotBody struct {
		SessionKey string   `json:"sessionKey"`
		CallIDs    []string `json:"callIds"`
		Reason     string   `json:"reason"`
	}
	childDash := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/api/internal/cancel-parked-session" {
			_ = json.NewDecoder(r.Body).Decode(&gotBody)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"ok":true,"cancelled":1}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer childDash.Close()
	u, _ := url.Parse(childDash.URL)
	_, portStr, _ := net.SplitHostPort(u.Host)
	childPort, _ := strconv.Atoi(portStr)

	am.mu.Lock()
	am.workers["a1"] = &accountWorkerProc{dashPort: childPort}
	am.mu.Unlock()
	am.stickyMu.Lock()
	am.stickyLRU.put("anon|hdr:child-hop|gemini-4-neon", "a1", "gemini-4-neon", time.Now(), time.Hour, 100)
	am.stickyMu.Unlock()
	setMonitorTripForTest("a1", time.Now().Add(time.Hour))

	resChild := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{
		SessionKey:        "hdr:child-hop",
		IsContinuation:    true,
		HasTools:          true,
		ToolResultCallIDs: []string{"acp_child_9"},
	})
	if resChild.DenyErr != nil || resChild.Account.ID == "a1" {
		t.Fatalf("expected hop off child a1, got %q err=%v", resChild.Account.ID, resChild.DenyErr)
	}
	if gotBody.SessionKey != "hdr:child-hop" || len(gotBody.CallIDs) != 1 || gotBody.CallIDs[0] != "acp_child_9" {
		t.Fatalf("expected federated child cancel-parked-session POST, got %+v", gotBody)
	}
}

func TestAdaptiveRelayIntervalAndTightenedDefaults(t *testing.T) {
	resetMonitorTripsForTest()
	t.Cleanup(func() {
		resetMonitorTripsForTest()
		initConfig()
	})
	t.Setenv("AGY_MAX_TOOL_RELAYS", "")
	t.Setenv("AGY_TOOL_RELAY_SEC", "")
	t.Setenv("AGY_TOOL_RELAY_MS", "")
	t.Setenv("AGY_INSTANCE_ID", "relay-acc")
	initConfig()

	// Default MaxToolRelays tightened from 4 to 2.
	if got := gwcfg.Current().MaxToolRelays; got != 2 {
		t.Fatalf("expected default MaxToolRelays=2, got %d", got)
	}

	// Below 50% monitor utilization -> default 55s (and 120s after 30m parked).
	if got := toolRelayInterval(0); got != 55*time.Second {
		t.Fatalf("expected 55s below 50%% utilization, got %v", got)
	}
	if got := toolRelayInterval(31 * time.Minute); got != 120*time.Second {
		t.Fatalf("expected 120s at 31m parked below 50%% utilization, got %v", got)
	}

	// At 50%..SoftCap (2400/4800, still Green) -> adaptively widens to 75s!
	RecordMonitorToolDispatchAt(currentProcessAccountID(), 2400, time.Now())
	if got := toolRelayInterval(0); got != 75*time.Second {
		t.Fatalf("expected adaptive 75s at 50%%..SoftCap green, got %v", got)
	}
	if got := toolRelayInterval(31 * time.Minute); got != 120*time.Second {
		t.Fatalf("expected 120s cap at 31m parked in green 50%%..SoftCap, got %v", got)
	}

	// At Yellow (>= SoftCap 2800) -> adaptively widens to 135s base and 150s cap at >=30m parked!
	RecordMonitorToolDispatchAt(currentProcessAccountID(), 450, time.Now()) // total 2850 -> Yellow
	if got := toolRelayInterval(0); got != 135*time.Second {
		t.Fatalf("expected adaptive 135s in Yellow zone, got %v", got)
	}
	if got := toolRelayInterval(11 * time.Minute); got != 135*time.Second {
		t.Fatalf("expected 135s at 11m parked in Yellow zone, got %v", got)
	}
	if got := toolRelayInterval(31 * time.Minute); got != 150*time.Second {
		t.Fatalf("expected 150s cap at 31m parked in Yellow zone, got %v", got)
	}

	// At Red (>= DrainCap 3800) -> stays 135s base and 150s cap at >=30m parked!
	RecordMonitorToolDispatchAt(currentProcessAccountID(), 1050, time.Now()) // total 3900 -> Red
	if got := toolRelayInterval(0); got != 135*time.Second {
		t.Fatalf("expected adaptive 135s in Red zone, got %v", got)
	}
	if got := toolRelayInterval(31 * time.Minute); got != 150*time.Second {
		t.Fatalf("expected 150s cap at 31m parked in Red zone, got %v", got)
	}

	// Explicit user override AGY_TOOL_RELAY_SEC must be respected even in Yellow/Red!
	t.Setenv("AGY_TOOL_RELAY_SEC", "45")
	initConfig()
	if got := toolRelayInterval(0); got != 45*time.Second {
		t.Fatalf("expected explicit AGY_TOOL_RELAY_SEC=45 honored in Red zone, got %v", got)
	}
	t.Setenv("AGY_TOOL_RELAY_SEC", "")
	t.Setenv("AGY_TOOL_RELAY_MS", "30000")
	initConfig()
	if got := toolRelayInterval(0); got != 30*time.Second {
		t.Fatalf("expected explicit AGY_TOOL_RELAY_MS=30000 honored in Red zone, got %v", got)
	}
}

func TestMonitorBudgetTwentyMinuteDriftBothDirections(t *testing.T) {
	resetMonitorTripsForTest()
	t.Cleanup(resetMonitorTripsForTest)

	// Direction 1 (Forward drift):
	// sir_flash_lite 429 trip expired at 15:00 (liteResetAt = 15:00), while Gemini /usage still reports
	// ResetTime = 15:20 (windowStartAt = 10:20) and stale MonitorCalls5h = 4800.
	base := time.Date(2026, time.November, 26, 15, 0, 0, 0, time.UTC)
	ultraAcc := AccountProfile{ID: "u-fwd", PlanTierHint: "ultra"}

	// Pre-15:00 calls + Gemini /usage reporting 15:20 reset with 4800 calls.
	RecordMonitorToolDispatchAt("u-fwd", 4000, base.Add(-30*time.Minute))
	RecordMonitorLiteResetAt("u-fwd", base, base.Add(time.Minute)) // liteResetAt = 15:00
	SyncMonitorBudgetCalls("u-fwd", 4800, base.Add(20*time.Minute).Format(time.RFC3339), base.Add(time.Minute))

	// At 15:01: must evaluate Green with 0 calls (NOT false-Red from stale 4800 logCalls5h!).
	snap1501 := EvaluateMonitorBudget(ultraAcc, base.Add(time.Minute))
	if snap1501.Calls5h != 0 || snap1501.Zone != monitorZoneGreen {
		t.Fatalf("forward drift at 15:01 must be 0/green (ignoring stale 4800 pre-reset logCalls5h), got %+v", snap1501)
	}

	// Fresh dispatches after 15:00 ARE counted accurately.
	RecordMonitorToolDispatchAt("u-fwd", 15, base.Add(30*time.Second))
	snap1501After := EvaluateMonitorBudget(ultraAcc, base.Add(time.Minute))
	if snap1501After.Calls5h != 15 || snap1501After.Zone != monitorZoneGreen {
		t.Fatalf("forward drift must count post-15:00 calls only (=15/green), got %+v", snap1501After)
	}

	// Once Gemini finally rolls over at 15:20 to ResetTime=20:20 (windowStartAt=15:20 >= 15:00),
	// monotonic reconciliation with logCalls5h resumes cleanly.
	SyncMonitorBudgetCalls("u-fwd", 80, base.Add(5*time.Hour+20*time.Minute).Format(time.RFC3339), base.Add(21*time.Minute))
	snap1521 := EvaluateMonitorBudget(ultraAcc, base.Add(21*time.Minute))
	if snap1521.Calls5h != 80 || snap1521.Zone != monitorZoneGreen {
		t.Fatalf("after Gemini catches up past liteResetAt, expected 80/green, got %+v", snap1521)
	}

	// Direction 2 (Reverse drift):
	// Gemini resets at 15:00 (windowKey 15:00 -> 20:00, startAt = 15:00, logCalls = 0),
	// while sir_flash_lite won't reset until 15:20 (liteResetAt = 15:20 > now=15:05).
	revAcc := AccountProfile{ID: "u-rev", PlanTierHint: "ultra"}
	liteReset1520 := base.Add(20 * time.Minute)
	RecordMonitorToolDispatchAt("u-rev", 3500, base.Add(-45*time.Minute))
	SyncMonitorBudgetCalls("u-rev", 3500, base.Format(time.RFC3339), base.Add(-5*time.Minute))
	RecordMonitorLiteResetAt("u-rev", liteReset1520, base.Add(-5*time.Minute))

	// At 15:05: Gemini /usage rolls over to 20:00 with logCalls=0 AND a new dispatch arrives at 15:05.
	// Neither SyncMonitorBudgetCalls nor RecordMonitorToolDispatchAt may prematurely zero calls5h!
	SyncMonitorBudgetCalls("u-rev", 0, base.Add(5*time.Hour).Format(time.RFC3339), base.Add(5*time.Minute))
	RecordMonitorToolDispatchAt("u-rev", 10, base.Add(5*time.Minute))
	snapRev1505 := EvaluateMonitorBudget(revAcc, base.Add(5*time.Minute))
	if snapRev1505.Calls5h < 3500 || snapRev1505.Zone != monitorZoneYellow {
		t.Fatalf("reverse drift at 15:05 (before liteResetAt 15:20) must NOT prematurely zero calls5h, got %+v", snapRev1505)
	}

	// At 15:21 (after liteResetAt 15:20 passes): stale pre-15:20 counts drop cleanly -> 0 (or post-15:20 only), Green!
	RecordMonitorToolDispatchAt("u-rev", 7, base.Add(20*time.Minute+30*time.Second))
	snapRev1521 := EvaluateMonitorBudget(revAcc, base.Add(21*time.Minute))
	if snapRev1521.Calls5h != 7 || snapRev1521.Zone != monitorZoneGreen {
		t.Fatalf("after liteResetAt 15:20 passes, expected only post-15:20 calls (=7/green), got %+v", snapRev1521)
	}
}

func TestMonitorBudgetQuotaWindowIdlePreservesFreshRtRing(t *testing.T) {
	resetMonitorTripsForTest()
	t.Cleanup(resetMonitorTripsForTest)

	t0 := time.Date(2026, time.November, 26, 10, 0, 0, 0, time.UTC)
	acc := AccountProfile{ID: "u-idle", PlanTierHint: "ultra"}

	makeIdleQuota := func(readAt time.Time, calls int) *QuotaSnapshot {
		return &QuotaSnapshot{
			Status:    "ok",
			UpdatedAt: readAt.UTC().Format(time.RFC3339),
			Groups: []QuotaGroup{
				{
					Name: "Gemini",
					Buckets: []QuotaBucket{
						{
							Window:            "5h",
							UsedFraction:      0,
							RemainingFraction: 1,
							ResetTime:         readAt.Add(5 * time.Hour).UTC().Format(time.RFC3339),
						},
					},
				},
			},
			Deduction: &QuotaDeduction{
				MonitorCalls5h: calls,
			},
		}
	}

	// Poll #1: idle /usage at 10:00 (0% used, resetTime = 15:00) anchors windowStartAt = 10:00.
	SyncMonitorBudgetFromQuota("u-idle", makeIdleQuota(t0, 0), t0)

	// Fresh real-time tool dispatches at 10:00:30 (50 calls).
	RecordMonitorToolDispatchAt("u-idle", 50, t0.Add(30*time.Second))

	// Poll #2 at 10:02 and Poll #3 at 10:05: /usage still reports idle (0% used, drifting resetTime = 15:02, 15:05).
	// Must NOT trigger a windowKey rollover or prune the 50 fresh rtRing dispatches!
	SyncMonitorBudgetFromQuota("u-idle", makeIdleQuota(t0.Add(2*time.Minute), 0), t0.Add(2*time.Minute))
	SyncMonitorBudgetFromQuota("u-idle", makeIdleQuota(t0.Add(5*time.Minute), 0), t0.Add(5*time.Minute))

	snap := EvaluateMonitorBudget(acc, t0.Add(5*time.Minute))
	if snap.Calls5h != 50 || snap.Zone != monitorZoneGreen {
		t.Fatalf("drifting idle /usage polls must NOT wipe fresh rtRing calls (want 50/green), got %+v", snap)
	}

	// Also verify fresh rtRing calls arriving BEFORE the very first idle poll are preserved.
	RecordMonitorToolDispatchAt("u-idle-cold", 35, t0.Add(20*time.Second))
	SyncMonitorBudgetFromQuota("u-idle-cold", makeIdleQuota(t0.Add(2*time.Minute), 0), t0.Add(2*time.Minute))
	snapCold := EvaluateMonitorBudget(AccountProfile{ID: "u-idle-cold", PlanTierHint: "ultra"}, t0.Add(2*time.Minute))
	if snapCold.Calls5h != 35 {
		t.Fatalf("first idle /usage poll must preserve pre-existing fresh rtRing calls (want 35), got %+v", snapCold)
	}
}

func TestFillFirstPriorityBeforeMonitorBudgetSpread(t *testing.T) {
	am := newTripRoutingManager(t)
	t.Setenv("AGY_MONITOR_BUDGET_SPREAD", "1")
	initConfig()

	now := time.Now()
	am.mu.Lock()
	am.accounts = []AccountProfile{
		{ID: "u1", Label: "Ultra 1", Enabled: true, Primary: true, Priority: 10, PlanTierHint: "ultra", MaxLiveWorkers: 8, SchedState: SchedStateActive, AllowedModels: []string{"gemini-4-neon"}},
		{ID: "u2", Label: "Ultra 2", Enabled: true, Priority: 10, PlanTierHint: "ultra", MaxLiveWorkers: 8, SchedState: SchedStateActive, AllowedModels: []string{"gemini-4-neon"}},
		{ID: "p1", Label: "Pro 1", Enabled: true, Priority: 20, PlanTierHint: "pro", MaxLiveWorkers: 4, SchedState: SchedStateActive, AllowedModels: []string{"gemini-4-neon"}},
	}
	am.routing.Strategy = RouteStrategyFillFirst
	am.mu.Unlock()
	for _, id := range []string{"u2", "p1"} {
		tokPath := am.AccountTokenPath(id)
		_ = os.MkdirAll(filepath.Dir(tokPath), 0o700)
		_ = os.WriteFile(tokPath, []byte("dummy-oauth-token-fixture-32bytes"), 0o600)
		am.InvalidateTokenCache(id)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)

	// 1. Both P10 Ultras at 20% monitor usage (960/4800, Green), P20 Pro completely idle (0/3400, Green).
	//    P10 Ultra MUST still beat idle P20 Pro!
	RecordMonitorToolDispatchAt("u1", 960, now)
	RecordMonitorToolDispatchAt("u2", 960, now)
	pick1 := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:ff-prio-1", HasTools: true})
	if pick1.DenyErr != nil || (pick1.Account.ID != "u1" && pick1.Account.ID != "u2") {
		t.Fatalf("P10 Green Ultra at 20%% usage must beat idle P20 Pro in fill-first, got %q err=%v", pick1.Account.ID, pick1.DenyErr)
	}

	// 2. Equal-priority P10 Ultras: u1 at 30% (1440), u2 at 20% (960) -> spread picks fresher u2!
	RecordMonitorToolDispatchAt("u1", 480, now) // u1=1440 (30%), u2=960 (20%)
	pick2 := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:ff-prio-2", HasTools: true})
	if pick2.DenyErr != nil || pick2.Account.ID != "u2" {
		t.Fatalf("equal-priority P10 Ultras must still spread to fresher u2, got %q err=%v", pick2.Account.ID, pick2.DenyErr)
	}
}

func TestMcpBridgeRecordsMonitorToolDispatches(t *testing.T) {
	resetMonitorTripsForTest()
	t.Cleanup(resetMonitorTripsForTest)
	t.Setenv("AGY_INSTANCE_ID", "mcp-acc")
	initConfig()

	accID := currentProcessAccountID()
	s := NewAcpProjectSession("sid:mcp-count:u:1", DefaultCwd, "")
	s.SessionID = "00000000-0000-4000-8000-000000000999"
	s.DeclaredMcpTools = map[string]bool{"Bash": true}
	s.McpToHostMap = map[string]string{mcpToolName("Bash"): "Bash"}
	s.ToolsCatalog = []ToolDef{{Name: "Bash", McpName: mcpToolName("Bash")}}
	s.PromptInFlight = true
	s.activeTurn = newActiveTurnState(nil, func() {})

	// 1. MCP_TOOL_PARKED increments monitor dispatch counter by 1.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	_ = s.executeMcpToolCall(ctx, "Bash", map[string]any{"command": "echo hi"})
	// Cleanly hibernate the test session to stop the 350ms tool-batch flush timer before the next test calls initConfig().
	s.HibernateWorker("test_cleanup", true)
	snap1 := EvaluateMonitorBudget(AccountProfile{ID: accID}, time.Now())
	if snap1.Calls5h != 1 {
		t.Fatalf("expected Calls5h=1 after MCP_TOOL_PARKED, got %d", snap1.Calls5h)
	}

	// 2. ws_await_result increments monitor dispatch counter by 1.
	_ = s.executeMcpToolCall(context.Background(), AwaitToolName, map[string]any{"call_id": "nonexistent"})
	snap2 := EvaluateMonitorBudget(AccountProfile{ID: accID}, time.Now())
	if snap2.Calls5h != 2 {
		t.Fatalf("expected Calls5h=2 after ws_await_result, got %d", snap2.Calls5h)
	}
}
