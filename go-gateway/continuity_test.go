package main

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCompactedParkedSessionKeepsCanonicalOwner(t *testing.T) {
	GlobalStore.Init(filepath.Join(t.TempDir(), "state.db"))
	defer GlobalStore.Init(GatewayStateDB)
	gs := NewGatewayServer()
	defer gs.Close()
	srv := httptest.NewUnstartedServer(gs)
	GatewayPort = srv.Listener.Addr().(*net.TCPAddr).Port
	srv.Start()
	defer srv.Close()
	defer GlobalEngine.StopEngine()
	before, _ := os.ReadFile(GatewayLogFile)
	newBefore := strings.Count(string(before), "[SESSION_NEW]")
	system := "Primary working directory: /tmp"
	tools := []map[string]any{{"name": "Bash", "input_schema": map[string]any{"type": "object"}}}
	post := func(msgs []map[string]any, compacted bool) map[string]any {
		t.Helper()
		b, _ := json.Marshal(map[string]any{"model": "gemini-3.8-flash-low", "system": system, "tools": tools, "messages": msgs})
		r, _ := http.NewRequest("POST", srv.URL+"/v1/messages", bytes.NewReader(b))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("x-session-id", "continuity-owner")
		if compacted {
			r.Header.Set("x-claude-code-context-compacted", "true")
		}
		resp, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil || resp.StatusCode != 200 {
			t.Fatalf("request failed: %d %v", resp.StatusCode, err)
		}
		return out
	}
	original := []map[string]any{{"role": "user", "content": "[FAKE_TOOL_CALL] original long history"}}
	first := post(original, false)
	var toolID string
	for _, item := range first["content"].([]any) {
		block := item.(map[string]any)
		if block["type"] == "tool_use" {
			toolID, _ = block["id"].(string)
		}
	}
	if toolID == "" {
		t.Fatal("missing parked tool")
	}
	compressed := []map[string]any{
		{"role": "user", "content": "Compressed summary of earlier work"},
		{"role": "assistant", "content": first["content"]},
		{"role": "user", "content": []map[string]any{{"type": "tool_result", "tool_use_id": toolID, "content": "completed"}}},
	}
	second := post(compressed, true)
	if second["stop_reason"] != "end_turn" {
		t.Fatalf("compacted turn did not finish: %v", second["stop_reason"])
	}
	after, _ := os.ReadFile(GatewayLogFile)
	if n := strings.Count(string(after), "[SESSION_NEW]") - newBefore; n != 2 {
		t.Fatalf("expected original + one compacted session, got %d", n)
	}
	var canonical *AcpProjectSession
	gs.mu.RLock()
	for _, s := range gs.sessions {
		canonical = s
	}
	gs.mu.RUnlock()
	canonical.mu.Lock()
	sid := canonical.SessionID
	canonical.mu.Unlock()
	next := append(compressed, map[string]any{"role": "assistant", "content": second["content"]}, map[string]any{"role": "user", "content": "Continue after compaction"})
	post(next, false)
	if len(gs.sessions) != 1 || canonical.SessionID != sid {
		t.Fatal("continuation started another canonical session")
	}
	if !canonical.HibernateWorker("test_restart", false) {
		t.Fatal("could not detach canonical session")
	}
	restored := &GatewayServer{sessions: map[string]*AcpProjectSession{}, tenants: map[string]*AcpProjectSession{}}
	if n := GlobalStore.RestoreDetachedSessions(restored); n != 1 {
		t.Fatalf("restored %d owners", n)
	}
	r := httptest.NewRequest("POST", "/v1/messages", nil)
	r.Header.Set("x-session-id", "continuity-owner")
	catalog := sanitizeToolsCatalog(tools)
	key := deriveConversationKey(r, nil, system, catalog, next)
	got := restored.getOrCreateAvailableSession(key, "/tmp", system, computeFirstUserFingerprint(next))
	if got.HibernatedSessionID != sid || len(restored.sessions) != 1 {
		t.Fatal("restart lost same-identity continuity")
	}
}

func TestContinuityRejectsAmbiguousOrForeignIdentity(t *testing.T) {
	for _, requestKey := range []string{"sid:other:ag:a:u:new", "sid:owner:ag:b:u:new", "fp:unknown:u:new"} {
		gs := &GatewayServer{sessions: map[string]*AcpProjectSession{}, tenants: map[string]*AcpProjectSession{}}
		s := NewAcpProjectSession("sid:owner:ag:a:u:old", "/tmp", "rules")
		s.HistoryFingerprint, s.MessageCount = "full-fingerprint", 3
		s.ParkedCalls["known-tool"] = &ParkedMcpCall{}
		gs.sessions[s.ConvKey] = s
		msgs := []map[string]any{{"role": "user", "content": []map[string]any{{"type": "tool_result", "tool_use_id": "known-tool", "content": "x"}}}}
		if gs.findSessionWithParkedToolResult(requestKey, msgs) != nil {
			t.Fatal("tool result crossed identity boundary")
		}
		if gs.getOrCreateAvailableSession(requestKey, "/tmp", "rules", "full-fingerprint") == s {
			t.Fatal("history fingerprint crossed identity boundary")
		}
	}
	gs := &GatewayServer{sessions: map[string]*AcpProjectSession{}, tenants: map[string]*AcpProjectSession{}}
	for _, key := range []string{"sid:owner:u:old-a", "sid:owner:u:old-b"} {
		s := NewAcpProjectSession(key, "/tmp", "rules")
		s.HistoryFingerprint, s.MessageCount = "full-fingerprint", 3
		gs.sessions[key] = s
	}
	got := gs.getOrCreateAvailableSession("sid:owner:u:new", "/tmp", "rules", "full-fingerprint")
	if got.ConvKey != "sid:owner:u:new" {
		t.Fatal("ambiguous owner was selected")
	}
}
