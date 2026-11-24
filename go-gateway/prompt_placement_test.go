package main

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
)

func placementParams(text string) map[string]any {
	return map[string]any{"cwd": "/tmp", "mcpServers": []any{map[string]any{"name": "read", "schema": "原样"}}, "_meta": map[string]any{"opaque": json.Number("1.00"), "antigravity": map[string]any{"route": "keep", "systemPrompt": map[string]any{"append": text, "other": "keep"}}}}
}
func TestPrivatePlacementLeafAndUnicode(t *testing.T) {
	text := "  YAML:\n    原文:  x  y\n\n\tPython\n"
	orig := placementParams(text)
	before, _ := json.Marshal(orig)
	clean, got, present, err := takePlacementAppend(orig)
	if err != nil || !present || got != text {
		t.Fatal("append changed")
	}
	after, _ := json.Marshal(orig)
	if string(before) != string(after) {
		t.Fatal("input mutated")
	}
	sys := clean["_meta"].(map[string]any)["antigravity"].(map[string]any)["systemPrompt"].(map[string]any)
	if _, ok := sys["append"]; ok || sys["other"] != "keep" {
		t.Fatal("leaf removal incorrect")
	}
	if !reflect.DeepEqual(clean["mcpServers"], orig["mcpServers"]) {
		t.Fatal("tools changed")
	}
}
func TestPrivatePlacementLifecycle(t *testing.T) {
	var p promptPlacement
	calls := 0
	var sent map[string]any
	native := func(method string, params any, _ time.Duration) (json.RawMessage, error) {
		calls++
		sent = params.(map[string]any)
		if method == "session/new" {
			return json.RawMessage(`{"sessionId":"s"}`), nil
		}
		return json.RawMessage(`{"configOptions":[{"id":"model","currentValue":"gemini-4-neon"}]}`), nil
	}
	call := func(method string, params map[string]any) {
		t.Helper()
		if _, err := p.call(method, params, 1, native, time.Second); err != nil {
			t.Fatal(err)
		}
	}
	call("session/new", placementParams("FULL\n  exact"))
	state := map[string]any{"sessionId": "s", "configId": "model", "value": "gemini-4-neon"}
	call("session/set_config_option", state)
	if !reflect.DeepEqual(sent, state) {
		t.Fatal("state RPC changed")
	}
	raw, err := p.call("session/set_config_option", state, 1, native, time.Second)
	if err != nil || verifyConfigOptionAck(raw, "model", "gemini-4-neon") != nil {
		t.Fatal("ACK lost")
	}
	parts := []map[string]any{{"type": "text", "text": "user"}, {"type": "image", "data": "AA"}}
	req := map[string]any{"sessionId": "s", "prompt": parts}
	call("session/prompt", req)
	blocks := sent["prompt"].([]map[string]any)
	if len(blocks) != 4 || blocks[0]["text"] != "# Pi session instructions\n\nFULL\n  exact" || !reflect.DeepEqual(blocks[2:], parts) {
		t.Fatal("first prompt mismatch")
	}
	call("session/cancel", map[string]any{"sessionId": "s"})
	for _, m := range []string{"session/resume", "session/load"} {
		r := placementParams("FULL\n  exact")
		r["sessionId"] = "s"
		call(m, r)
	}
	call("session/prompt", req)
	if !reflect.DeepEqual(sent["prompt"], parts) {
		t.Fatal("double injection after resume/cancel")
	}
	before := calls
	if _, err := p.call("session/resume", map[string]any{"sessionId": "old"}, 1, native, time.Second); err == nil || calls != before {
		t.Fatal("unknown resume dispatched")
	}
	if _, err := p.call("session/load", map[string]any{"sessionId": "s"}, 2, native, time.Second); err == nil {
		t.Fatal("old-generation session trusted")
	}
	call("session/close", map[string]any{"sessionId": "s"})
	if _, err := p.call("session/prompt", req, 1, native, time.Second); err == nil {
		t.Fatal("closed session accepted")
	}
}
func TestPrivatePlacementFailureNoDoubleInjection(t *testing.T) {
	var p promptPlacement
	native := func(m string, _ any, _ time.Duration) (json.RawMessage, error) {
		if m == "session/new" {
			return json.RawMessage(`{"sessionId":"s"}`), nil
		}
		return nil, errors.New("uncertain prompt transport")
	}
	p.call("session/new", placementParams("once"), 1, native, time.Second)
	req := map[string]any{"sessionId": "s", "prompt": []map[string]any{{"type": "text", "text": "user"}}}
	_, err := p.call("session/prompt", req, 1, native, time.Second)
	if err == nil {
		t.Fatal("missing transport error")
	}
	native2 := func(_ string, v any, _ time.Duration) (json.RawMessage, error) {
		t.Fatal("retired session silently dispatched")
		return nil, nil
	}
	if _, err := p.call("session/prompt", req, 1, native2, time.Second); err == nil {
		t.Fatal("failed first prompt retained provenance")
	}
	if _, err := p.call("session/resume", req, 1, native2, time.Second); err == nil {
		t.Fatal("uncertain session resumed")
	}
	p.call("session/new", placementParams("once"), 1, native, time.Second)
	fresh := func(_ string, v any, _ time.Duration) (json.RawMessage, error) {
		b := v.(map[string]any)["prompt"].([]map[string]any)
		if b[0]["text"] != "# Pi session instructions\n\nonce" || len(b) != 3 {
			t.Fatal("fresh instructions missing")
		}
		return json.RawMessage(`{}`), nil
	}
	if _, err := p.call("session/prompt", req, 1, fresh, time.Second); err != nil {
		t.Fatal(err)
	}
}
func TestPrivatePlacementConcurrentSessions(t *testing.T) {
	var p promptPlacement
	var wg sync.WaitGroup
	for _, id := range []string{"a", "b"} {
		wg.Add(1)
		go func(sid string) {
			defer wg.Done()
			native := func(m string, v any, _ time.Duration) (json.RawMessage, error) {
				if m == "session/new" {
					return json.RawMessage(`{"sessionId":"` + sid + `"}`), nil
				}
				b := v.(map[string]any)["prompt"].([]map[string]any)
				if b[0]["text"] != "# Pi session instructions\n\n"+sid {
					t.Error("cross-session injection")
				}
				return json.RawMessage(`{}`), nil
			}
			p.call("session/new", placementParams(sid), 1, native, time.Second)
			p.call("session/prompt", map[string]any{"sessionId": sid, "prompt": []map[string]any{{"type": "text", "text": "user"}}}, 1, native, time.Second)
		}(id)
	}
	wg.Wait()
}
func TestPrivatePlacementObservedWarning(t *testing.T) {
	warning := "\n⚠ RESOURCE_EXHAUSTED (code 429): Resource has been exhausted (e.g. check quota).\n"
	for split := 1; split < len(warning); split++ {
		if !utf8.ValidString(warning[:split]) {
			continue
		}
		turn := &activeTurnState{}
		out := turn.acceptTextLocked(warning[:split]) + turn.acceptTextLocked(warning[split:]) + turn.finishNoticeLocked()
		se := DetectAntigravitySoftError(noticeOnlyText(out, formatTerminalNoticeWithDetail(turn.noticeText, turn.lastRetryDetail), 0))
		if out != "" || se == nil || se.StatusCode != 429 {
			t.Fatalf("fragment %d did not produce 429", split)
		}
	}
	for _, normal := range []string{"Quoted warning: " + strings.TrimSpace(warning), "`" + strings.TrimSpace(warning) + "`", "The error was 429; here is the answer."} {
		turn := &activeTurnState{}
		out := turn.acceptTextLocked(normal) + turn.finishNoticeLocked()
		if out != normal || turn.noticeFailed {
			t.Fatal("normal quoted response misclassified")
		}
	}
}
