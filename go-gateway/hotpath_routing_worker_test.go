package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func refExtractRoutingHintsFromJSON(raw []byte) (modelID, sessionKey string, isContinuation, malformed bool) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return "", "", false, true
	}
	var topRaw map[string]json.RawMessage
	if err := json.Unmarshal(raw, &topRaw); err != nil || topRaw == nil {
		return "", "", false, true
	}
	var mStr string
	if len(topRaw["model"]) > 0 && json.Unmarshal(topRaw["model"], &mStr) == nil && strings.TrimSpace(mStr) != "" {
		modelID = ResolveRequestedModel(strings.TrimSpace(mStr), nil).Model.ID
	}
	var prevResp string
	if len(topRaw["previous_response_id"]) > 0 && json.Unmarshal(topRaw["previous_response_id"], &prevResp) == nil && strings.TrimSpace(prevResp) != "" {
		isContinuation = true
	}
	inArrSys, firstElem, arrCont := inspectConversationTurns(topRaw["messages"])
	if len(bytes.TrimSpace(topRaw["messages"])) == 0 {
		if len(bytes.TrimSpace(topRaw["contents"])) > 0 {
			inArrSys, firstElem, arrCont = inspectConversationTurns(topRaw["contents"])
		} else if rawIn := bytes.TrimSpace(topRaw["input"]); len(rawIn) > 0 {
			if rawIn[0] == '[' {
				inArrSys, firstElem, arrCont = inspectConversationTurns(rawIn)
			} else {
				firstElem = rawIn
			}
		}
	}
	if arrCont {
		isContinuation = true
	}
	var pck string
	if len(topRaw["prompt_cache_key"]) > 0 && json.Unmarshal(topRaw["prompt_cache_key"], &pck) == nil && strings.TrimSpace(pck) != "" {
		sessionKey = "pck:" + strings.TrimSpace(pck)
	} else {
		var metaObj struct {
			UserID string `json:"user_id"`
		}
		metaUID := ""
		if len(topRaw["metadata"]) > 0 && json.Unmarshal(topRaw["metadata"], &metaObj) == nil {
			metaUID = strings.TrimSpace(metaObj.UserID)
		}
		if metaUID != "" && strings.Contains(strings.ToLower(metaUID), "session") {
			sessionKey = "meta:" + metaUID
		} else {
			firstMsg := clipBytes(firstElem, 1024)
			sysPart := clipBytes(topRaw["system"], 512)
			if len(sysPart) == 0 {
				sysPart = clipBytes(topRaw["instructions"], 512)
			}
			if len(sysPart) == 0 {
				sysPart = clipBytes(topRaw["systemInstruction"], 512)
			}
			arrSysClip := clipBytes(inArrSys, 512)
			if len(firstMsg) > 0 || len(sysPart) > 0 || len(arrSysClip) > 0 {
				h := sha256.New()
				h.Write(sysPart)
				h.Write([]byte{'|'})
				h.Write(arrSysClip)
				h.Write([]byte{'|'})
				h.Write(firstMsg)
				sessionKey = "fp:" + hex.EncodeToString(h.Sum(nil))[:16]
			} else if metaUID != "" {
				sessionKey = "meta:" + metaUID
			} else {
				var usr string
				if len(topRaw["user"]) > 0 && json.Unmarshal(topRaw["user"], &usr) == nil && strings.TrimSpace(usr) != "" {
					sessionKey = "usr:" + strings.TrimSpace(usr)
				}
			}
		}
	}
	return modelID, sessionKey, isContinuation, false
}

func TestHotpathZeroCopyRoutingHintsEquivalenceAndDuplicateKeyGuard(t *testing.T) {
	largeB64 := strings.Repeat("QUJD", 256*1024) // 1MB base64 payload
	cases := []struct {
		name string
		body string
	}{
		{
			name: "anthropic_turn1_multimodal",
			body: `{"model":"gemini-4-carbon","system":"You are helpful.","messages":[{"role":"user","content":[{"type":"text","text":"inspect"},{"type":"image","source":{"type":"base64","data":"` + largeB64 + `"}}]}]}`,
		},
		{
			name: "anthropic_turn2_tool_result_continuation",
			body: `{"model":"gemini-3.8-flash","system":"sys","messages":[{"role":"user","content":"hi"},{"role":"assistant","content":"ok"},{"role":"user","content":[{"type":"tool_result","tool_use_id":"1","content":"done"}]}]}`,
		},
		{
			name: "openai_chat_leading_system_turn1",
			body: `{"model":"gemini-4-neon","messages":[{"role":"system","content":"be brief"},{"role":"user","content":"hello world"}]}`,
		},
		{
			name: "openai_responses_previous_response_id",
			body: `{"model":"gemini-3.8-flash","instructions":"inst","previous_response_id":"resp_123","input":"next question"}`,
		},
		{
			name: "gemini_v1beta_contents",
			body: `{"systemInstruction":{"parts":[{"text":"gemini sys"}]},"contents":[{"role":"user","parts":[{"text":"first turn"}]}]}`,
		},
		{
			name: "escaped_lowercase_model_key",
			body: `{"\u006dodel":"gemini-4-argon","messages":[{"role":"user","content":"escaped"}]}`,
		},
		{
			name: "capitalized_Model_ignored",
			body: `{"Model":"gemini-4-argon","messages":[{"role":"user","content":"cap"}]}`,
		},
		{
			name: "duplicate_model_key_smuggling_uses_last_key",
			body: `{"model":"gemini-3.8-flash","messages":[{"role":"user","content":"dup"}],"model":"gemini-4-argon"}`,
		},
		{
			name: "malformed_trailing_object",
			body: `{"model":"gemini-3.8-flash"}{"extra":1}`,
		},
		{
			name: "malformed_truncated",
			body: `{"model":"gemini-3.8-flash","messages":[{"role":"user"`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(tc.body))
			gotModel, gotSK, gotCont, _, _, _, gotMal, _, err := extractRequestRoutingHintsFull(req)
			if err != nil {
				t.Fatalf("unexpected readErr: %v", err)
			}
			wantModel, wantSK, wantCont, wantMal := refExtractRoutingHintsFromJSON([]byte(tc.body))
			if gotMal != wantMal || gotModel != wantModel || gotSK != wantSK || gotCont != wantCont {
				t.Fatalf("mismatch on %s:\n got=(model=%q sk=%q cont=%v mal=%v)\nwant=(model=%q sk=%q cont=%v mal=%v)",
					tc.name, gotModel, gotSK, gotCont, gotMal, wantModel, wantSK, wantCont, wantMal)
			}
		})
	}
	// Specifically assert duplicate-key smuggling resolves to the last key ("gemini-4-argon"), matching downstream JSON decoder!
	dupReq := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"gemini-3.8-flash","model":"gemini-4-argon","messages":[{"role":"user","content":"x"}]}`))
	mDup, _, _, _, _, _, malDup, _, _ := extractRequestRoutingHintsFull(dupReq)
	if malDup || mDup != "gemini-4-argon" {
		t.Fatalf("duplicate model key must resolve to last key gemini-4-argon to prevent allowlist bypass, got %q mal=%v", mDup, malDup)
	}
}

func TestReapIdleAccountWorkers_RespectsInFlightLiveSessionsAndPreservesSleepingDashboard(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("AGY_GW_HOME", tmp)
	t.Setenv("AGY_ACCOUNTS_FILE", filepath.Join(tmp, "accounts.json"))
	initConfig()
	t.Cleanup(initConfig)
	InvalidatePeerDashboardCache()
	t.Cleanup(InvalidatePeerDashboardCache)

	am := NewAccountManager()
	am.Init()
	_, err := am.UpsertAccount(AccountProfile{
		ID:            "sub-idle",
		Label:         "Sub Idle",
		Enabled:       true,
		ProxyPreset:   "direct",
		AllowedModels: []string{"gemini-3.8-flash"},
	})
	if err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}
	tokPath := am.AccountTokenPath("sub-idle")
	_ = os.MkdirAll(filepath.Dir(tokPath), 0o700)
	_ = os.WriteFile(tokPath, []byte("valid-synthetic-oauth-token-content"), 0o600)
	am.InvalidateTokenCache("sub-idle")

	liveSessionsCount := 1
	mockChildDash := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var live []map[string]any
		for i := 0; i < liveSessionsCount; i++ {
			live = append(live, map[string]any{"sessionId": "s1"})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"gateway": map[string]any{"instanceId": "sub-idle", "maxLiveWorkers": 4},
			"quota": QuotaSnapshot{
				Status:   QuotaStateKnown,
				PlanTier: "pro",
				Groups:   []QuotaGroup{{Name: "Gemini Models"}},
			},
			"windows":      map[string]WindowAggregate{},
			"liveSessions": live,
		})
	}))
	defer mockChildDash.Close()
	u, _ := url.Parse(mockChildDash.URL)
	_, portStr, _ := strings.Cut(u.Host, ":")
	dashPort, _ := strconv.Atoi(portStr)

	now := time.Now()
	proc := &accountWorkerProc{
		id:        "sub-idle",
		gwPort:    dashPort,
		dashPort:  dashPort,
		done:      make(chan struct{}),
		healthyAt: now.Add(-30 * time.Minute),
	}
	close(proc.done)
	proc.lastActiveNs.Store(now.Add(-30 * time.Minute).UnixNano())
	am.mu.Lock()
	am.workers["sub-idle"] = proc
	am.mu.Unlock()

	// 1. Must NOT reap while inFlight > 0
	rel := am.AcquireAccountInFlight("sub-idle")
	proc.lastActiveNs.Store(now.Add(-30 * time.Minute).UnixNano())
	if n := am.ReapIdleAccountWorkers(now, 10*time.Minute); n != 0 {
		t.Fatalf("must not reap worker with inFlight > 0, got reaped=%d", n)
	}
	rel()
	proc.lastActiveNs.Store(now.Add(-30 * time.Minute).UnixNano())

	// 2. Must NOT reap while child reports LiveSessions > 0
	if n := am.ReapIdleAccountWorkers(now, 10*time.Minute); n != 0 {
		t.Fatalf("must not reap worker with LiveSessions > 0, got reaped=%d", n)
	}

	// 3. Reap succeeds once LiveSessions == 0 and idleTTL elapsed, without incrementing crashStates!
	liveSessionsCount = 0
	InvalidatePeerDashboardCache()
	if n := am.ReapIdleAccountWorkers(now, 10*time.Minute); n != 1 {
		t.Fatalf("expected 1 idle worker reaped, got %d", n)
	}
	if running, _, _ := am.IsWorkerRunning("sub-idle"); running {
		t.Fatal("expected sub-idle worker stopped after idle reap")
	}
	am.mu.RLock()
	cs := am.crashStates["sub-idle"]
	am.mu.RUnlock()
	if cs.failures != 0 {
		t.Fatalf("intentional idle hibernation must not count as crash failure, got %+v", cs)
	}

	// 4. Passive ManagedPeersForDashboardWithForce(false) serves cached quota snapshot as sleeping=true!
	peers, unauth := am.ManagedPeersForDashboardWithForce(false)
	if len(peers) != 0 || len(unauth) != 1 {
		t.Fatalf("expected 0 active peers and 1 sleeping instance, got peers=%d unauth=%d", len(peers), len(unauth))
	}
	if sleeping, _ := unauth[0]["sleeping"].(bool); !sleeping {
		t.Fatalf("expected sleeping=true on hibernated instance, got %+v", fmt.Sprint(unauth[0]))
	}
}
