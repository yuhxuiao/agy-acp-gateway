package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Capture the actual JSON emitted by SharedAcpEngine.RPC. Replies use its native
// pending-request channel; no placement helper is called directly by the test.
type placementWire struct {
	e     *SharedAcpEngine
	calls []map[string]any
}

func (w *placementWire) Write(b []byte) (int, error) {
	var msg map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(b), &msg); err != nil {
		return 0, err
	}
	w.calls = append(w.calls, msg)
	p, _ := msg["params"].(map[string]any)
	var result any = map[string]any{}
	switch msg["method"] {
	case "session/new":
		result = map[string]any{"sessionId": "wire-session"}
	case "session/set_config_option":
		result = map[string]any{"configOptions": []map[string]any{{"id": p["configId"], "currentValue": p["value"]}}}
	case "session/prompt":
		result = map[string]any{"stopReason": "end_turn"}
	}
	if id, ok := msg["id"].(float64); ok {
		raw, _ := json.Marshal(result)
		ch, found := w.e.pending.Load(int64(id))
		if found {
			ch.(chan rpcResult) <- rpcResult{Result: raw}
		}
	}
	return len(b), nil
}
func TestPlacementSharedRPCDefaultAndOptInWire(t *testing.T) {
	for _, mode := range []string{"", "append", "first-prompt", "other"} {
		t.Run("mode="+mode, func(t *testing.T) {
			setupM2IsolatedEnv(t)
			t.Setenv("AGY_PROMPT_PLACEMENT", mode)
			e := newSharedAcpEngine()
			e.gen.Store(1)
			w := &placementWire{e: e}
			e.stdin = bufio.NewWriter(w)
			rpc := func(m string, p map[string]any) json.RawMessage {
				t.Helper()
				raw, err := e.RPC(m, p, time.Second)
				if err != nil {
					t.Fatal(err)
				}
				return raw
			}
			original := placementParams("synthetic routing\n  keep: 中文  ")
			rpc("session/new", original)
			sent := w.calls[len(w.calls)-1]["params"].(map[string]any)
			if mode == "first-prompt" {
				want, _, _, _ := takePlacementAppend(original)
				raw, _ := json.Marshal(want)
				var wantJSON map[string]any
				json.Unmarshal(raw, &wantJSON)
				if !reflect.DeepEqual(sent, wantJSON) {
					t.Fatal("append removal changed siblings/tools")
				}
			} else {
				raw, _ := json.Marshal(original)
				var want map[string]any
				json.Unmarshal(raw, &want)
				if !reflect.DeepEqual(sent, want) {
					t.Fatal("default native append changed")
				}
			}
			ack := rpc("session/set_config_option", map[string]any{"sessionId": "wire-session", "configId": "model", "value": "gemini-4-neon"})
			if verifyConfigOptionAck(ack, "model", "gemini-4-neon") != nil {
				t.Fatal("native ACK changed")
			}
			parts := []map[string]any{{"type": "text", "text": "current user"}, {"type": "image", "data": "AA", "mimeType": "image/png"}}
			req := map[string]any{"sessionId": "wire-session", "prompt": parts}
			rpc("session/prompt", req)
			got := w.calls[len(w.calls)-1]["params"].(map[string]any)["prompt"].([]any)
			if mode == "first-prompt" {
				if len(got) != 4 || got[0].(map[string]any)["text"] != "# Pi session instructions\n\nsynthetic routing\n  keep: 中文  " {
					t.Fatal("first prompt did not carry exact append")
				}
				got = got[2:]
			} else if len(got) != 2 {
				t.Fatal("default injected instructions")
			}
			raw, _ := json.Marshal(parts)
			var want []any
			json.Unmarshal(raw, &want)
			if !reflect.DeepEqual(got, want) {
				t.Fatal("user/tool media blocks changed")
			}
			rpc("session/cancel", map[string]any{"sessionId": "wire-session"})
			rpc("session/prompt", req)
			if len(w.calls[len(w.calls)-1]["params"].(map[string]any)["prompt"].([]any)) != 2 {
				t.Fatal("repeat/cancel injected twice")
			}
		})
	}
}

func placementRecordedRequests(t *testing.T) []map[string]any {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(AcpRecordDir, "acp-traffic.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	var out []map[string]any
	for _, line := range bytes.Split(raw, []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var entry struct {
			Dir  string `json:"dir"`
			Line string `json:"line"`
		}
		if json.Unmarshal(line, &entry) != nil || entry.Dir != "out" {
			continue
		}
		var msg map[string]any
		if json.Unmarshal([]byte(entry.Line), &msg) == nil {
			out = append(out, msg)
		}
	}
	return out
}
func TestPlacementGatewayFailureAndRestartRebuildHistory(t *testing.T) {
	setupM2IsolatedEnv(t)
	t.Setenv("AGY_PROMPT_PLACEMENT", "first-prompt")
	base, cleanup := startTestGateway(t)
	defer cleanup()
	messages := []map[string]any{{"role": "user", "content": "history-alpha [FAKE_RPC_32603_ONCE:placement-history]"}}
	post := func() int {
		t.Helper()
		body, _ := json.Marshal(map[string]any{"model": "gemini-3.8-flash", "max_tokens": 64, "system": "synthetic instructions", "messages": messages})
		req, _ := http.NewRequest("POST", base+"/v1/messages", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("x-claude-code-session-id", "placement-history-case")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp.StatusCode
	}
	if code := post(); code < 400 {
		t.Fatal("expected first prompt RPC failure")
	}
	count := 0
	for _, m := range placementRecordedRequests(t) {
		if m["method"] == "session/prompt" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("failed prompt automatically resubmitted %d times", count)
	}
	messages = append(messages, map[string]any{"role": "user", "content": "history-beta"})
	if code := post(); code != 200 {
		t.Fatalf("fresh retry returned %d", code)
	}
	GlobalEngine.StopEngine()
	messages = append(messages, map[string]any{"role": "assistant", "content": "FAKE_OK"}, map[string]any{"role": "user", "content": "history-gamma"})
	if code := post(); code != 200 {
		t.Fatalf("restart recovery returned %d", code)
	}
	var prompts []map[string]any
	news := 0
	for _, m := range placementRecordedRequests(t) {
		if m["method"] == "session/prompt" {
			prompts = append(prompts, m)
		}
		if m["method"] == "session/new" {
			news++
		}
	}
	if len(prompts) != 3 || news != 3 {
		t.Fatalf("expected 3 distinct fresh dispatches, got prompts=%d new=%d", len(prompts), news)
	}
	for i, words := range [][]string{{"history-alpha"}, {"history-alpha", "history-beta"}, {"history-alpha", "history-beta", "history-gamma"}} {
		b, _ := json.Marshal(prompts[i]["params"])
		for _, word := range words {
			if !strings.Contains(string(b), word) {
				t.Fatalf("dispatch %d lost %s from full host history", i, word)
			}
		}
	}
}
func TestPlacementCompactionCancelAndDetachedCompletion(t *testing.T) {
	t.Setenv("AGY_PROMPT_PLACEMENT", "first-prompt")
	t.Run("cancel-releases-current-upstream-lease", TestStoreSnapshotPersistedInDBAndCancelActiveTurnReleasesLease)
	t.Run("detached-expiry-keeps-warm-slot-and-releases-lease", TestSettleDetachedTurnPriorMsgCountGuard)
	t.Run("completed-reuse-transfers-target-once", func(t *testing.T) {
		setupM2IsolatedEnv(t)
		target := NewAcpProjectSession("target", "/tmp", "sys")
		target.SessionID = "target-sid"
		ref := target.addCompactionPendingLease("target-sid", time.Minute, false)
		s := NewAcpProjectSession("summary", "/tmp", "sys")
		s.coalesceHandoff = true
		s.activeCompactionTarget = ref
		s.recordRecentCompletedTurn("fingerprint", TurnResult{Text: "synthetic summary", StopReason: "end_turn"})
		if s.activeCompactionTarget != nil || s.lastCompletedCompactionTarget != ref {
			t.Fatal("completed summary lost target ownership")
		}
		got, transferred, ok := s.tryReplayRecentCompletedTurnWithCompaction("fingerprint", nil)
		if !ok || got.Text != "synthetic summary" || transferred != ref {
			t.Fatal("ordinary completed reuse disabled")
		}
		transferred.release()
		if _, _, ok = s.tryReplayRecentCompletedTurnWithCompaction("fingerprint", nil); ok {
			t.Fatal("completed reuse repeated")
		}
		if target.hasActiveCompactionPendingLocked(time.Now()) {
			t.Fatal("completed reuse leaked target lease")
		}
	})
}
