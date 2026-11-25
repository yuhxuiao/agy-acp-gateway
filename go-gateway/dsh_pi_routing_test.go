package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func postJSONWithHeaders(t *testing.T, url string, headers map[string]string, payload map[string]any) (int, map[string]any, []byte) {
	t.Helper()
	rawBody, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	req, err := http.NewRequest("POST", url, bytes.NewReader(rawBody))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("http post %s: %v", url, err)
	}
	defer resp.Body.Close()
	outBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	var parsed map[string]any
	_ = json.Unmarshal(outBytes, &parsed)
	return resp.StatusCode, parsed, outBytes
}

func TestStandaloneCompactionSummaryDetection(t *testing.T) {
	piSys := "You are a context summarization assistant. Produce a clean handoff summary."
	piMsg := []map[string]any{
		{
			"role":    "user",
			"content": "<conversation>\nUser: fix auth\nAssistant: patched token check\n</conversation>\n\nCreate a structured context checkpoint summary.",
		},
	}

	if !isStandaloneCompactionSummary(piSys, nil, piMsg) {
		t.Fatal("expected Pi standalone compaction summary with <conversation> prefix to match")
	}

	// Block-form content in user message.
	piBlockMsg := []map[string]any{
		{
			"role": "user",
			"content": []any{
				map[string]any{"type": "text", "text": "  <conversation>\nhello\n</conversation>"},
			},
		},
	}
	if !isStandaloneCompactionSummary(piSys, nil, piBlockMsg) {
		t.Fatal("expected block-form user <conversation> message to match")
	}

	// Large payload (>10KB) without leading <conversation>, closing tag in last 4KB tail.
	tailTagMsg := []map[string]any{
		{
			"role":    "user",
			"content": "Preamble:\n" + strings.Repeat("log line content\n", 800) + "\n</conversation>\nSummarize.",
		},
	}
	if !isStandaloneCompactionSummary(piSys, nil, tailTagMsg) {
		t.Fatal("expected closing </conversation> within last 4KB tail to match")
	}

	// Large payload (>10KB) with checkpoint summary phrase in last 4KB tail (case-insensitive).
	tailPhraseMsg := []map[string]any{
		{
			"role":    "user",
			"content": "History dump:\n" + strings.Repeat("turn line\n", 800) + "\nPlease Create A Structured Context Checkpoint Summary now.",
		},
	}
	if !isStandaloneCompactionSummary(piSys, nil, tailPhraseMsg) {
		t.Fatal("expected checkpoint summary phrase in last 4KB tail to match")
	}

	// /v1/responses instructions + "\n\n" + developer block.
	joinedSys := "General assistant instructions.\n\n" + piSys
	if !isStandaloneCompactionSummary(joinedSys, nil, piMsg) {
		t.Fatal("expected \\n\\n-separated leading block with piCompactionSummaryPrefix to match")
	}

	// Negative: with tools.
	if isStandaloneCompactionSummary(piSys, []ToolDef{{Name: "Read"}}, piMsg) {
		t.Fatal("request with tools must not match standalone compaction")
	}

	// Negative: multi-turn (>1 message) or 0 messages.
	if isStandaloneCompactionSummary(piSys, nil, nil) {
		t.Fatal("empty messages must not match standalone compaction")
	}
	multiTurn := []map[string]any{
		{"role": "user", "content": "<conversation>a</conversation>"},
		{"role": "assistant", "content": "summary"},
		{"role": "user", "content": "<conversation>b</conversation>"},
	}
	if isStandaloneCompactionSummary(piSys, nil, multiTurn) {
		t.Fatal("multi-turn conversation must not match standalone compaction")
	}

	// Negative: contains tool blocks.
	toolBlockMsg := []map[string]any{
		{
			"role": "user",
			"content": []any{
				map[string]any{"type": "tool_result", "tool_use_id": "call_1", "content": "ok"},
				map[string]any{"type": "text", "text": "<conversation>a</conversation>"},
			},
		},
	}
	if isStandaloneCompactionSummary(piSys, nil, toolBlockMsg) {
		t.Fatal("message containing tool_result blocks must not match standalone compaction")
	}

	// Negative: normal system prompt even when user quotes both the system prefix and <conversation>.
	normalSys := "You are a helpful coding assistant."
	quotedMsg := []map[string]any{
		{
			"role":    "user",
			"content": "<conversation>\nYou are a context summarization assistant.\n</conversation>",
		},
	}
	if isStandaloneCompactionSummary(normalSys, nil, quotedMsg) {
		t.Fatal("normal system prompt with user quoting prefix must not match standalone compaction")
	}

	// Negative: matching system prompt, but </conversation> is buried before the last 4KB tail.
	buriedTagMsg := []map[string]any{
		{
			"role":    "user",
			"content": "Note: </conversation> was mentioned earlier.\n" + strings.Repeat("x", 5000),
		},
	}
	if isStandaloneCompactionSummary(piSys, nil, buriedTagMsg) {
		t.Fatal("closing tag outside last 4KB tail without leading <conversation> must not match")
	}

	// Negative: prefix buried in 5th \n\n block (past the first 4 non-empty blocks).
	deepSys := "b1\n\nb2\n\nb3\n\nb4\n\n" + piSys
	if isStandaloneCompactionSummary(deepSys, nil, piMsg) {
		t.Fatal("prefix after first 4 non-empty blocks must not match")
	}

	// Crucial invariant: Pi standalone compaction MUST NOT be classified as ephemeral util:.
	if isEphemeralBackgroundClassifier(piSys, nil, piMsg) {
		t.Fatal("Pi standalone compaction must NOT match isEphemeralBackgroundClassifier")
	}
	reqWithSid := httptest.NewRequest("POST", "/v1/messages", nil)
	reqWithSid.Header.Set("x-pi-session-id", "pi-key-check-1")
	kSid := deriveConversationKey(reqWithSid, map[string]any{"max_tokens": 2048}, piSys, nil, piMsg)
	if strings.HasPrefix(kSid, "util:") || !strings.HasPrefix(kSid, "sid:pi-key-check-1:") {
		t.Fatalf("expected Pi standalone compaction with x-pi-session-id to keep sid: key, got %q", kSid)
	}
	reqNoSid := httptest.NewRequest("POST", "/v1/messages", nil)
	kFp := deriveConversationKey(reqNoSid, map[string]any{"max_tokens": 2048}, piSys, nil, piMsg)
	if strings.HasPrefix(kFp, "util:") || !strings.HasPrefix(kFp, "fp:") {
		t.Fatalf("expected Pi standalone compaction without explicit session ID to keep fp: key, got %q", kFp)
	}
}

func TestPiStandaloneCompactionEndToEndHibernationAndNoHijack(t *testing.T) {
	piSys := "You are a context summarization assistant. Summarize cleanly."
	piCompactionMsg := []map[string]any{
		{
			"role":    "user",
			"content": "<conversation>\nUser: refactor routing\nAssistant: done\n</conversation>\n\nCreate a structured context checkpoint summary.",
		},
	}

	t.Run("standalone compaction keeps sid key, completes 200, and immediately hibernates", func(t *testing.T) {
		setupM2IsolatedEnv(t)
		baseURL, gs := startCarbonTestGateway(t)

		st, _, raw := postJSONWithHeaders(t, baseURL+"/v1/messages", map[string]string{
			"x-pi-session-id": "pi-oneshot-1",
		}, map[string]any{
			"model":      "gemini-4-carbon",
			"max_tokens": 1024,
			"system":     piSys,
			"messages":   piCompactionMsg,
		})
		if st != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d body=%s", st, string(raw))
		}

		var found *AcpProjectSession
		gs.mu.RLock()
		for k, s := range gs.sessions {
			if strings.HasPrefix(k, "util:") {
				gs.mu.RUnlock()
				t.Fatalf("Pi standalone compaction must NOT use util: key, found %q", k)
			}
			if strings.HasPrefix(k, "sid:pi-oneshot-1:") {
				found = s
			}
		}
		gs.mu.RUnlock()
		if found == nil {
			t.Fatal("expected session under sid:pi-oneshot-1: key")
		}

		found.mu.Lock()
		sid := found.SessionID
		hibSid := found.HibernatedSessionID
		histLen := len(found.TurnHistory)
		histSid := ""
		if histLen > 0 {
			histSid = found.TurnHistory[histLen-1].SessionID
		}
		found.mu.Unlock()

		if sid != "" {
			t.Fatalf("expected immediate hibernation clearing SessionID, got %q", sid)
		}
		if hibSid != "" {
			t.Fatalf("expected keepResumeState=false leaving empty HibernatedSessionID, got %q", hibSid)
		}
		if live := gs.countLiveWorkers(); live != 0 {
			t.Fatalf("expected 0 live workers after standalone compaction completes, got %d", live)
		}
		if histLen != 1 || histSid == "" {
			t.Fatalf("expected TurnHistory persisted with non-empty SessionID, got len=%d sid=%q", histLen, histSid)
		}
	})

	t.Run("standalone compaction with parent x-pi-session-id never hijacks parked parent session", func(t *testing.T) {
		setupM2IsolatedEnv(t)
		t.Setenv("AGY_TOOL_RELAY_MS", "2000")
		baseURL, gs := startCarbonTestGateway(t)

		tools := []map[string]any{
			{
				"name":        "Read",
				"description": "Read a file",
				"input_schema": map[string]any{
					"type":       "object",
					"properties": map[string]any{"path": map[string]any{"type": "string"}},
				},
			},
		}
		parentUser := map[string]any{"role": "user", "content": "[FAKE_TOOL_CALL] inspect config"}
		st1, d1, raw1 := postJSONWithHeaders(t, baseURL+"/v1/messages", map[string]string{
			"x-pi-session-id": "pi-shared-parent",
		}, map[string]any{
			"model":      "gemini-4-carbon",
			"max_tokens": 1024,
			"system":     "You are a coding assistant.",
			"tools":      tools,
			"messages":   []map[string]any{parentUser},
		})
		if st1 != http.StatusOK {
			t.Fatalf("parent Turn 1 status=%d body=%s", st1, string(raw1))
		}
		cs1, _ := d1["content"].([]any)
		var callID string
		for _, blk := range cs1 {
			if bm, ok := blk.(map[string]any); ok && bm["type"] == "tool_use" {
				callID, _ = bm["id"].(string)
			}
		}
		if callID == "" {
			t.Fatalf("expected parked tool_use in parent Turn 1, got %s", string(raw1))
		}

		parentSess := m2FindSession(gs, "sid:pi-shared-parent:")
		if parentSess == nil {
			t.Fatal("expected parent session registered under sid:pi-shared-parent:")
		}
		parentSess.mu.Lock()
		parentSid := parentSess.SessionID
		parentParked := len(parentSess.ParkedCalls)
		parentKey := parentSess.ConvKey
		parentSess.mu.Unlock()
		if parentSid == "" || parentParked == 0 {
			t.Fatalf("expected parent session live and parked, got sid=%q parked=%d", parentSid, parentParked)
		}

		// Send Pi standalone compaction carrying the SAME x-pi-session-id (and even compacted header
		// to prove the explicit !isStandaloneCompaction guard prevents abandoned-parked hijacking).
		stComp, _, rawComp := postJSONWithHeaders(t, baseURL+"/v1/messages", map[string]string{
			"x-pi-session-id":                 "pi-shared-parent",
			"x-claude-code-context-compacted": "true",
		}, map[string]any{
			"model":      "gemini-4-carbon",
			"max_tokens": 1024,
			"system":     piSys,
			"messages":   piCompactionMsg,
		})
		if stComp != http.StatusOK {
			t.Fatalf("standalone compaction status=%d body=%s", stComp, string(rawComp))
		}

		var compSess *AcpProjectSession
		gs.mu.RLock()
		for k, s := range gs.sessions {
			if strings.HasPrefix(k, "util:") {
				gs.mu.RUnlock()
				t.Fatalf("standalone compaction must not use util: key, got %q", k)
			}
			if strings.HasPrefix(k, "sid:pi-shared-parent:") && k != parentKey {
				compSess = s
			}
		}
		gs.mu.RUnlock()
		if compSess == nil {
			t.Fatal("expected standalone compaction to allocate its own session instead of hijacking parked parent")
		}

		compSess.mu.Lock()
		compSid := compSess.SessionID
		compHib := compSess.HibernatedSessionID
		compSess.mu.Unlock()
		if compSid != "" || compHib != "" {
			t.Fatalf("expected compaction session immediately hibernated with no resume state, got sid=%q hib=%q", compSid, compHib)
		}

		parentSess.mu.Lock()
		afterParentSid := parentSess.SessionID
		afterParentParked := len(parentSess.ParkedCalls)
		afterForceRoll := parentSess.ForceRolloverNext
		parentSess.mu.Unlock()
		if afterParentSid != parentSid || afterParentParked == 0 || afterForceRoll {
			t.Fatalf("parked parent session was hijacked/mutated: sid=%q->%q parked=%d forceRoll=%v",
				parentSid, afterParentSid, afterParentParked, afterForceRoll)
		}
		if live := gs.countLiveWorkers(); live != 1 {
			t.Fatalf("expected only parked parent live (1), got %d", live)
		}

		// Parent parked tool call must still resume cleanly on its original ACP session.
		toolResUser := map[string]any{
			"role": "user",
			"content": []any{
				map[string]any{"type": "tool_result", "tool_use_id": callID, "content": "PARENT_INTACT"},
			},
		}
		st2, _, raw2 := postJSONWithHeaders(t, baseURL+"/v1/messages", map[string]string{
			"x-pi-session-id": "pi-shared-parent",
		}, map[string]any{
			"model":      "gemini-4-carbon",
			"max_tokens": 1024,
			"system":     "You are a coding assistant.",
			"tools":      tools,
			"messages":   []map[string]any{parentUser, {"role": "assistant", "content": cs1}, toolResUser},
		})
		if st2 != http.StatusOK || !strings.Contains(string(raw2), "TOOL_DONE:PARENT_INTACT") {
			t.Fatalf("expected parent parked tool resume to succeed, got %d body=%s", st2, string(raw2))
		}
	})

	t.Run("maybeHibernateStandaloneCompaction respects failure and detached guards", func(t *testing.T) {
		s := NewAcpProjectSession("sid:guard-test:u:1234", "/tmp", "")
		s.SessionID = "sess-guard-1"

		// Non-nil error
		maybeHibernateStandaloneCompaction(s, TurnResult{SessionID: "sess-guard-1", Text: "ok"}, errors.New("boom"))
		if s.SessionID != "sess-guard-1" {
			t.Fatal("must not hibernate on non-nil error")
		}
		// Tool calls present
		maybeHibernateStandaloneCompaction(s, TurnResult{SessionID: "sess-guard-1", ToolCalls: []ToolCallItem{{ID: "c1", Name: "Read"}}}, nil)
		if s.SessionID != "sess-guard-1" {
			t.Fatal("must not hibernate when ToolCalls present")
		}
		// FailedNotice present
		maybeHibernateStandaloneCompaction(s, TurnResult{SessionID: "sess-guard-1", FailedNotice: "err"}, nil)
		if s.SessionID != "sess-guard-1" {
			t.Fatal("must not hibernate when FailedNotice present")
		}
		// Soft error text present
		softText := "The prompt could not be submitted due to policy."
		if DetectAntigravitySoftError(softText) == nil {
			t.Fatal("test fixture must trigger DetectAntigravitySoftError")
		}
		maybeHibernateStandaloneCompaction(s, TurnResult{SessionID: "sess-guard-1", Text: softText}, nil)
		if s.SessionID != "sess-guard-1" {
			t.Fatal("must not hibernate on soft error text")
		}
		// Active detached turn present
		s.detachedTurn = &detachedTurnState{}
		maybeHibernateStandaloneCompaction(s, TurnResult{SessionID: "sess-guard-1", Text: "ok"}, nil)
		s.detachedTurn = nil
		if s.SessionID != "sess-guard-1" {
			t.Fatal("must not hibernate while HasDetachedTurn is true")
		}
		// Empty result SessionID
		maybeHibernateStandaloneCompaction(s, TurnResult{SessionID: "", Text: "ok"}, nil)
		if s.SessionID != "sess-guard-1" {
			t.Fatal("must not hibernate when res.SessionID is empty")
		}
	})
}

func TestDshSessionTitleIsolationAndLowTierCap(t *testing.T) {
	dshTitleSys := "Create a concise title for an AI coding-assistant session from the supplied human messages."
	userMsgs := []map[string]any{
		{"role": "user", "content": "Human messages:\n1. Fix session routing bug in gateway"},
	}

	t.Run("classifier and session naming utility unit predicates", func(t *testing.T) {
		if !isEphemeralBackgroundClassifier(dshTitleSys, nil, userMsgs) {
			t.Fatal("expected dsh title prompt to match isEphemeralBackgroundClassifier")
		}
		if !isSessionNamingUtility(dshTitleSys, nil, userMsgs) {
			t.Fatal("expected dsh title prompt to match isSessionNamingUtility")
		}

		// /v1/responses instructions + "\n\n" + developer block.
		respJoined := "You are a helpful AI assistant.\n\n" + dshTitleSys
		if !isEphemeralBackgroundClassifier(respJoined, nil, userMsgs) || !isSessionNamingUtility(respJoined, nil, userMsgs) {
			t.Fatal("expected \\n\\n-joined instructions+developer dsh title prompt to match both predicates")
		}

		// Claude sessionNamingPrefix matches both.
		claudeNaming := sessionNamingPrefix + " Keep it short."
		if !isEphemeralBackgroundClassifier(claudeNaming, nil, userMsgs) || !isSessionNamingUtility(claudeNaming, nil, userMsgs) {
			t.Fatal("expected Claude sessionNamingPrefix to match both predicates")
		}

		// Permission classifier matches ephemeral util: isolation, but MUST NOT match isSessionNamingUtility.
		permSys := permissionClassifierPrefix + "\n\nEvaluate risk."
		if !isEphemeralBackgroundClassifier(permSys, nil, userMsgs) {
			t.Fatal("expected permissionClassifierPrefix to match isEphemeralBackgroundClassifier")
		}
		if isSessionNamingUtility(permSys, nil, userMsgs) {
			t.Fatal("permissionClassifierPrefix must NOT match isSessionNamingUtility (security monitor keeps its tier)")
		}

		// Explicit session ID header still maps to util:.
		req := httptest.NewRequest("POST", "/v1/chat/completions", nil)
		req.Header.Set("x-session-id", "dsh-explicit-sid-1")
		k := deriveConversationKey(req, map[string]any{"max_tokens": 128}, respJoined, nil, userMsgs)
		if !strings.HasPrefix(k, "util:") {
			t.Fatalf("expected dsh title generator to map to util: even with explicit session ID, got %q", k)
		}

		// Negative guards: tools, >4 messages, 5th \n\n block.
		if isSessionNamingUtility(dshTitleSys, []ToolDef{{Name: "Bash"}}, userMsgs) {
			t.Fatal("request with tools must not match isSessionNamingUtility")
		}
		longMsgs := []map[string]any{
			{"role": "user", "content": "1"},
			{"role": "assistant", "content": "2"},
			{"role": "user", "content": "3"},
			{"role": "assistant", "content": "4"},
			{"role": "user", "content": "5"},
		}
		if isSessionNamingUtility(dshTitleSys, nil, longMsgs) {
			t.Fatal(">4 messages must not match isSessionNamingUtility")
		}
		deepSys := "b1\n\nb2\n\nb3\n\nb4\n\n" + dshTitleSys
		if isSessionNamingUtility(deepSys, nil, userMsgs) || isEphemeralBackgroundClassifier(deepSys, nil, userMsgs) {
			t.Fatal("prefix in 5th \\n\\n block must not match")
		}
	})

	t.Run("e2e /v1/chat/completions role=developer and role=system isolate to util and cap tier to low", func(t *testing.T) {
		for _, sysRole := range []string{"developer", "system"} {
			t.Run("role="+sysRole, func(t *testing.T) {
				setupM2IsolatedEnv(t)
				baseURL, gs := startCarbonTestGateway(t)

				payload := map[string]any{
					"model":            "gemini-4-carbon",
					"reasoning_effort": "high",
					"messages": []map[string]any{
						{"role": sysRole, "content": dshTitleSys},
						{"role": "user", "content": "Human messages:\n1. Add metrics endpoint (" + sysRole + ")"},
					},
				}
				st, _, raw := postJSONWithHeaders(t, baseURL+"/v1/chat/completions", map[string]string{
					"x-session-id": "dsh-chat-" + sysRole,
				}, payload)
				if st != http.StatusOK {
					t.Fatalf("chat completions (%s) status=%d body=%s", sysRole, st, string(raw))
				}
				if !strings.Contains(string(raw), "tier=low") {
					t.Fatalf("expected tier=low in fakeacp response for chat completions (%s), got %s", sysRole, string(raw))
				}

				sess := findSessionByConvPrefix(gs, "util:")
				if sess == nil {
					t.Fatalf("expected util: session for chat completions (%s), none found", sysRole)
				}
				if findSessionByConvPrefix(gs, "sid:dsh-chat-"+sysRole) != nil {
					t.Fatalf("dsh title call (%s) must not create sid: session", sysRole)
				}
				sess.mu.Lock()
				gotTier := sess.ThoughtLevel
				sess.mu.Unlock()
				if gotTier != "low" {
					t.Fatalf("expected session ThoughtLevel=low, got %q", gotTier)
				}
			})
		}
	})

	t.Run("e2e /v1/messages isolates dsh title to util and caps body-inherited high tier to low", func(t *testing.T) {
		setupM2IsolatedEnv(t)
		baseURL, gs := startCarbonTestGateway(t)

		st, _, raw := postJSONWithHeaders(t, baseURL+"/v1/messages", map[string]string{
			"x-claude-code-session-id": "dsh-msg-parent",
		}, map[string]any{
			"model":      "gemini-4-carbon",
			"max_tokens": 32000,
			"thinking":   map[string]any{"type": "enabled", "budget_tokens": 31999},
			"system":     dshTitleSys,
			"messages":   userMsgs,
		})
		if st != http.StatusOK {
			t.Fatalf("/v1/messages status=%d body=%s", st, string(raw))
		}
		if !strings.Contains(string(raw), "tier=low") {
			t.Fatalf("expected tier=low for /v1/messages dsh title, got %s", string(raw))
		}
		if findSessionByConvPrefix(gs, "util:") == nil || findSessionByConvPrefix(gs, "sid:dsh-msg-parent") != nil {
			t.Fatal("expected /v1/messages dsh title routed strictly to util:")
		}
	})

	t.Run("e2e /v1/responses instructions+developer isolates to util and caps reasoning effort high to low", func(t *testing.T) {
		setupM2IsolatedEnv(t)
		baseURL, gs := startCarbonTestGateway(t)

		st, _, raw := postJSONWithHeaders(t, baseURL+"/v1/responses", map[string]string{
			"x-session-id": "dsh-resp-parent",
		}, map[string]any{
			"model":        "gemini-4-carbon",
			"instructions": "Follow formatting rules strictly.",
			"reasoning":    map[string]any{"effort": "high"},
			"input": []map[string]any{
				{"type": "message", "role": "developer", "content": dshTitleSys},
				{"type": "message", "role": "user", "content": "Human messages:\n1. Debug SQLite WAL lock"},
			},
		})
		if st != http.StatusOK {
			t.Fatalf("/v1/responses status=%d body=%s", st, string(raw))
		}
		if !strings.Contains(string(raw), "tier=low") {
			t.Fatalf("expected tier=low for /v1/responses dsh title, got %s", string(raw))
		}
		if findSessionByConvPrefix(gs, "util:") == nil || findSessionByConvPrefix(gs, "sid:dsh-resp-parent") != nil {
			t.Fatal("expected /v1/responses dsh title routed strictly to util:")
		}
	})

	t.Run("e2e explicit model suffix -high is honored on dsh title", func(t *testing.T) {
		setupM2IsolatedEnv(t)
		baseURL, gs := startCarbonTestGateway(t)

		st, _, raw := postJSONWithHeaders(t, baseURL+"/v1/chat/completions", map[string]string{
			"x-session-id": "dsh-explicit-high",
		}, map[string]any{
			"model": "gemini-4-carbon-high",
			"messages": []map[string]any{
				{"role": "developer", "content": dshTitleSys},
				{"role": "user", "content": "Human messages:\n1. Explicit high tier check"},
			},
		})
		if st != http.StatusOK {
			t.Fatalf("explicit -high status=%d body=%s", st, string(raw))
		}
		if !strings.Contains(string(raw), "tier=high") {
			t.Fatalf("expected explicit model suffix -high to be honored (tier=high), got %s", string(raw))
		}
		sess := findSessionByConvPrefix(gs, "util:")
		if sess == nil {
			t.Fatal("expected explicit -high dsh title to still isolate into util:")
		}
		sess.mu.Lock()
		gotTier := sess.ThoughtLevel
		sess.mu.Unlock()
		if gotTier != "high" {
			t.Fatalf("expected session ThoughtLevel=high, got %q", gotTier)
		}
	})

	t.Run("e2e permission classifier is isolated to util but NOT capped to low", func(t *testing.T) {
		setupM2IsolatedEnv(t)
		baseURL, gs := startCarbonTestGateway(t)

		st, _, raw := postJSONWithHeaders(t, baseURL+"/v1/messages", map[string]string{
			"x-claude-code-session-id": "perm-cls-parent",
		}, map[string]any{
			"model":      "gemini-4-carbon",
			"max_tokens": 32000,
			"thinking":   map[string]any{"type": "enabled", "budget_tokens": 31999},
			"system":     permissionClassifierPrefix + "\n\nInspect bash command safety.",
			"messages": []map[string]any{
				{"role": "user", "content": "Action: Bash(rm -rf /tmp/demo)"},
			},
		})
		if st != http.StatusOK {
			t.Fatalf("permission classifier status=%d body=%s", st, string(raw))
		}
		if !strings.Contains(string(raw), "tier=high") {
			t.Fatalf("permission classifier must retain requested tier=high (not capped to low), got %s", string(raw))
		}
		sess := findSessionByConvPrefix(gs, "util:")
		if sess == nil {
			t.Fatal("expected permission classifier routed to util:")
		}
		sess.mu.Lock()
		gotTier := sess.ThoughtLevel
		sess.mu.Unlock()
		if gotTier != "high" {
			t.Fatalf("expected permission classifier ThoughtLevel=high, got %q", gotTier)
		}
	})
}

func TestAppendChatAssistantBlocksNoReasoningLeak(t *testing.T) {
	t.Run("empty content assistant message drops reasoning_content and reasoning", func(t *testing.T) {
		raw := map[string]any{
			"role":              "assistant",
			"content":           "",
			"reasoning_content": "secret chain of thought",
			"reasoning":         "alternate reasoning field",
		}
		conv := appendChatAssistantBlocks(nil, nil, raw)
		if len(conv) != 1 {
			t.Fatalf("expected 1 message, got %d", len(conv))
		}
		got, ok := conv[0].(map[string]any)
		if !ok {
			t.Fatalf("expected map[string]any, got %T", conv[0])
		}
		if got["role"] != "assistant" || got["content"] != "" {
			t.Fatalf("unexpected normalized message: %#v", got)
		}
		if _, hasRC := got["reasoning_content"]; hasRC {
			t.Fatalf("reasoning_content leaked into normalized message: %#v", got)
		}
		if _, hasR := got["reasoning"]; hasR {
			t.Fatalf("reasoning leaked into normalized message: %#v", got)
		}
		// Original map must remain untouched.
		if raw["reasoning_content"] != "secret chain of thought" {
			t.Fatal("rawMsg must not be mutated")
		}
	})

	t.Run("nil content preserves audio and strips reasoning keys", func(t *testing.T) {
		audioVal := map[string]any{"id": "audio_123"}
		raw := map[string]any{
			"role":              "assistant",
			"content":           nil,
			"audio":             audioVal,
			"reasoning_content": "hidden",
			"reasoning":         "hidden2",
		}
		conv := appendChatAssistantBlocks(nil, nil, raw)
		got := conv[0].(map[string]any)
		if got["content"] != "" {
			t.Fatalf("expected empty string content for nil raw content, got %#v", got["content"])
		}
		if got["audio"] == nil {
			t.Fatalf("expected audio preserved, got %#v", got)
		}
		if _, has := got["reasoning_content"]; has {
			t.Fatalf("reasoning_content leaked: %#v", got)
		}
		if _, has := got["reasoning"]; has {
			t.Fatalf("reasoning leaked: %#v", got)
		}
	})

	t.Run("non-empty string content preserved with empty blocks slice while stripping reasoning", func(t *testing.T) {
		raw := map[string]any{
			"role":              "assistant",
			"content":           "plain reply",
			"reasoning_content": "hidden",
		}
		conv := appendChatAssistantBlocks(nil, nil, raw)
		got := conv[0].(map[string]any)
		if got["content"] != "plain reply" {
			t.Fatalf("expected non-empty string content preserved, got %#v", got["content"])
		}
		if _, has := got["reasoning_content"]; has {
			t.Fatalf("reasoning_content leaked: %#v", got)
		}
	})

	t.Run("normal assistant blocks and consecutive merge do not leak reasoning", func(t *testing.T) {
		rawEmpty := map[string]any{
			"role":              "assistant",
			"content":           "",
			"reasoning_content": "hidden-1",
		}
		rawWithTool := map[string]any{
			"role":              "assistant",
			"content":           "calling tool",
			"reasoning_content": "hidden-2",
		}
		conv := appendChatAssistantBlocks(nil, nil, rawEmpty)
		conv = appendChatAssistantBlocks(conv, []any{
			map[string]any{"type": "text", "text": "calling tool"},
			map[string]any{"type": "tool_use", "id": "call_1", "name": "Read", "input": map[string]any{}},
		}, rawWithTool)
		if len(conv) != 1 {
			t.Fatalf("expected consecutive assistant messages merged into 1, got %d", len(conv))
		}
		got := conv[0].(map[string]any)
		if _, has := got["reasoning_content"]; has {
			t.Fatalf("reasoning_content leaked after merge: %#v", got)
		}
		if _, has := got["reasoning"]; has {
			t.Fatalf("reasoning leaked after merge: %#v", got)
		}
		blocks, ok := got["content"].([]any)
		if !ok || len(blocks) != 2 {
			t.Fatalf("expected 2 merged blocks, got %#v", got["content"])
		}
	})

	t.Run("e2e /v1/chat/completions does not leak stray reasoning_content from empty assistant message", func(t *testing.T) {
		setupM2IsolatedEnv(t)
		baseURL, _ := startCarbonTestGateway(t)

		const leakTokenA = "LEAK_GUARD_REASONING_CONTENT_9911"
		const leakTokenB = "LEAK_GUARD_REASONING_ALT_9922"
		off := acpRecordOffset()

		st, _, raw := postJSONWithHeaders(t, baseURL+"/v1/chat/completions", map[string]string{
			"x-session-id": "chat-empty-asst-leak-check",
		}, map[string]any{
			"model": "gemini-4-carbon",
			"messages": []map[string]any{
				{"role": "user", "content": "First question"},
				{
					"role":              "assistant",
					"content":           "",
					"reasoning_content": leakTokenA,
					"reasoning":         leakTokenB,
				},
				{"role": "user", "content": "Follow up question"},
			},
		})
		if st != http.StatusOK {
			t.Fatalf("chat completions status=%d body=%s", st, string(raw))
		}
		if AcpRecordDir != "" {
			recBytes, err := os.ReadFile(filepath.Join(AcpRecordDir, "acp-traffic.ndjson"))
			if err == nil && int64(len(recBytes)) > off {
				chunk := string(recBytes[off:])
				if strings.Contains(chunk, leakTokenA) || strings.Contains(chunk, leakTokenB) {
					t.Fatalf("stray reasoning_content/reasoning leaked into outbound ACP traffic")
				}
			}
		}
	})
}

func TestPiStandaloneCompactionCoalesceHandoffClaimsAndHibernates(t *testing.T) {
	setupM2IsolatedEnv(t)
	gs := NewGatewayServer()
	defer gs.Close()

	sess := NewAcpProjectSession("fp:pi-coalesce-test:u:12345678", DefaultCwd, "You are a context summarization assistant.")
	sess.SessionID = "sid-pi-coalesce-1"
	turn := newActiveTurnState(nil, nil)
	canceled := false
	sess.mu.Lock()
	sess.claimTurnLocked() // Request 1 holds claim (count=1)
	sess.activeTurn = turn
	sess.activeTurnReqFp = "reqfp-pi-compact-1"
	sess.activeReqCancel = func() { canceled = true }
	sess.mu.Unlock()

	gs.mu.Lock()
	gs.sessions[sess.ConvKey] = sess
	gs.mu.Unlock()

	// Request 2 coalesces onto active turn -> MUST claimTurnLocked (count=2) and set coalesceHandoff.
	actSess, cancelPrev := gs.findSessionWithActiveTurn(sess.ConvKey, DefaultCwd, "reqfp-pi-compact-1")
	if actSess != sess || cancelPrev == nil {
		t.Fatalf("expected findSessionWithActiveTurn to match active session, got sess=%v cancel=%v", actSess, cancelPrev)
	}
	cancelPrev()
	if !canceled {
		t.Fatal("expected cancelPrev to invoke activeReqCancel")
	}

	sess.mu.Lock()
	claimCnt, handoff := sess.turnClaimCount, sess.coalesceHandoff
	sess.mu.Unlock()
	if claimCnt != 2 || !handoff {
		t.Fatalf("expected turnClaimCount=2 and coalesceHandoff=true during handoff, got count=%d handoff=%v", claimCnt, handoff)
	}

	// Request 1 records completed turn and releases its own claim (2 -> 1).
	// Even if anything calls HibernateIfSession now, Request 2's active claim protects lastCompletedResult!
	sess.recordRecentCompletedTurn("reqfp-pi-compact-1", TurnResult{
		SessionID:  "sid-pi-coalesce-1",
		StopReason: "end_turn",
		Text:       "SUMMARY_OK",
	})
	sess.releaseTurnClaim()
	if sess.HibernateIfSession("sid-pi-coalesce-1", "oneshot_compaction_summary") {
		t.Fatal("HibernateIfSession must not close session while Request 2 still holds turn claim")
	}

	// Request 2 replays completed result, releases its claim (1 -> 0), then hibernates cleanly!
	rep, okRep := sess.tryReplayRecentCompletedTurn("reqfp-pi-compact-1", nil)
	if !okRep || rep.Text != "SUMMARY_OK" {
		t.Fatalf("expected Request 2 to replay intact completed summary, got ok=%v res=%+v", okRep, rep)
	}
	sess.releaseTurnClaim()
	maybeHibernateStandaloneCompaction(sess, rep, nil)

	sess.mu.Lock()
	sidAfter := sess.SessionID
	hibAfter := sess.HibernatedSessionID
	claimAfter := sess.turnClaimCount
	sess.mu.Unlock()
	if sidAfter != "" || hibAfter != "" || claimAfter != 0 {
		t.Fatalf("expected clean hibernation after Request 2 handoff release, got sid=%q hib=%q claims=%d", sidAfter, hibAfter, claimAfter)
	}
}
