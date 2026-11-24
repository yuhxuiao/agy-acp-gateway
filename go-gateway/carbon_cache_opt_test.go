package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func startCarbonTestGateway(t *testing.T) (string, *GatewayServer) {
	t.Helper()
	gs := NewGatewayServer()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	origPort := GatewayPort
	GatewayPort = ln.Addr().(*net.TCPAddr).Port
	srv := &http.Server{Handler: gs}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		_ = srv.Close()
		gs.Close()
		GlobalEngine.StopEngine()
		GatewayPort = origPort
	})
	return fmt.Sprintf("http://127.0.0.1:%d", GatewayPort), gs
}

func countRecordedSessionRPCContains(startOff int64, sid, method, needle string) int {
	if AcpRecordDir == "" || sid == "" {
		return 0
	}
	b, err := os.ReadFile(filepath.Join(AcpRecordDir, "acp-traffic.ndjson"))
	if err != nil || int64(len(b)) <= startOff {
		return 0
	}
	chunk := string(b[startOff:])
	count := 0
	methodNeedle := `"method":"` + method + `"`
	sidNeedle := `"sessionId":"` + sid + `"`
	for _, line := range strings.Split(chunk, "\n") {
		if line == "" {
			continue
		}
		var rec struct {
			Dir  string `json:"dir"`
			Line string `json:"line"`
		}
		if json.Unmarshal([]byte(line), &rec) != nil || rec.Dir != "out" {
			continue
		}
		if strings.Contains(rec.Line, methodNeedle) && strings.Contains(rec.Line, sidNeedle) && (needle == "" || strings.Contains(rec.Line, needle)) {
			count++
		}
	}
	return count
}

func acpRecordOffset() int64 {
	if AcpRecordDir == "" {
		return 0
	}
	st, err := os.Stat(filepath.Join(AcpRecordDir, "acp-traffic.ndjson"))
	if err != nil {
		return 0
	}
	return st.Size()
}

func countRecordedSessionRPCs(startOff int64, sid, method string) int {
	if AcpRecordDir == "" || sid == "" {
		return 0
	}
	b, err := os.ReadFile(filepath.Join(AcpRecordDir, "acp-traffic.ndjson"))
	if err != nil || int64(len(b)) <= startOff {
		return 0
	}
	chunk := string(b[startOff:])
	count := 0
	methodNeedle := `"method":"` + method + `"`
	sidNeedle := `"sessionId":"` + sid + `"`
	for _, line := range strings.Split(chunk, "\n") {
		if line == "" {
			continue
		}
		var rec struct {
			Dir  string `json:"dir"`
			Line string `json:"line"`
		}
		if json.Unmarshal([]byte(line), &rec) != nil || rec.Dir != "out" {
			continue
		}
		if strings.Contains(rec.Line, methodNeedle) && strings.Contains(rec.Line, sidNeedle) {
			count++
		}
	}
	return count
}

func findSessionByConvPrefix(gs *GatewayServer, prefix string) *AcpProjectSession {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	for _, s := range gs.sessions {
		if s != nil && strings.HasPrefix(s.ConvKey, prefix) {
			return s
		}
	}
	return nil
}

func waitForDetachedSession(t *testing.T, gs *GatewayServer, timeout time.Duration) *AcpProjectSession {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		gs.mu.RLock()
		for _, s := range gs.sessions {
			if s != nil && s.HasDetachedTurn() {
				gs.mu.RUnlock()
				return s
			}
		}
		gs.mu.RUnlock()
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for session to enter detached state")
	return nil
}

func doGatewayJSON(ctx context.Context, url, sessionHdr, sessionVal string, payload any) (*http.Response, string, error) {
	b, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(b))
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("Content-Type", "application/json")
	if sessionHdr != "" && sessionVal != "" {
		req.Header.Set(sessionHdr, sessionVal)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, "", err
	}
	raw, readErr := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, string(raw), readErr
}

func TestPrefillSoftDetachAndSingleflightStreamJoin(t *testing.T) {
	origMin, origGrace := PrefillCancelMinElapsed, PrefillCancelGrace
	PrefillCancelMinElapsed = 50 * time.Millisecond
	PrefillCancelGrace = 800 * time.Millisecond
	t.Cleanup(func() {
		PrefillCancelMinElapsed = origMin
		PrefillCancelGrace = origGrace
	})
	if err := GlobalEngine.EnsureStarted(); err != nil {
		t.Fatal(err)
	}

	gs := NewGatewayServer()
	t.Cleanup(gs.Close)
	srv := httptest.NewServer(gs.GatewayOnlyHandler())
	defer srv.Close()

	startOff := acpRecordOffset()
	payload := map[string]any{
		"model":      "gemini-4-carbon",
		"max_tokens": 128,
		"stream":     true,
		"system":     "Primary working directory: /tmp",
		"messages": []map[string]any{
			{"role": "user", "content": "[FAKE_SLOW_PREFILL_300MS] hello singleflight"},
		},
	}

	ctxA, cancelA := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancelA()
	_, _, errA := doGatewayJSON(ctxA, srv.URL+"/v1/messages", "x-claude-code-session-id", "carbon-opt-stream-1", payload)
	if errA == nil {
		t.Fatalf("expected Request A to abort on 120ms client timeout")
	}

	sess := waitForDetachedSession(t, gs, 500*time.Millisecond)
	sess.mu.Lock()
	sid := sess.SessionID
	sess.mu.Unlock()
	if sid == "" {
		t.Fatalf("expected active sessionId after soft-detach")
	}
	if cancels := countRecordedSessionRPCs(startOff, sid, "session/cancel"); cancels != 0 {
		t.Fatalf("expected 0 session/cancel during prefill soft-detach, got %d", cancels)
	}

	respB, bodyB, errB := doGatewayJSON(context.Background(), srv.URL+"/v1/messages", "x-claude-code-session-id", "carbon-opt-stream-1", payload)
	if errB != nil || respB.StatusCode != 200 {
		t.Fatalf("Request B singleflight join failed: status=%v err=%v body=%s", respB, errB, bodyB)
	}
	if !strings.Contains(bodyB, "FAKE_OK model=gemini-4-carbon") || !strings.Contains(bodyB, "message_stop") {
		t.Fatalf("expected streamed joined reply to contain FAKE_OK and message_stop, got:\n%s", bodyB)
	}
	if prompts := countRecordedSessionRPCs(startOff, sid, "session/prompt"); prompts != 1 {
		t.Fatalf("expected exactly 1 upstream session/prompt for singleflight join, got %d", prompts)
	}
	if cancels := countRecordedSessionRPCs(startOff, sid, "session/cancel"); cancels != 0 {
		t.Fatalf("expected 0 session/cancel after singleflight join, got %d", cancels)
	}
	if !sess.IsAvailableForNewTurn() || sess.HasDetachedTurn() {
		t.Fatalf("session not cleanly settled after singleflight join (avail=%v detached=%v)", sess.IsAvailableForNewTurn(), sess.HasDetachedTurn())
	}
}

func TestPrefillSoftDetachParkedToolResumeJoin(t *testing.T) {
	origMin, origGrace := PrefillCancelMinElapsed, PrefillCancelGrace
	PrefillCancelMinElapsed = 50 * time.Millisecond
	PrefillCancelGrace = 800 * time.Millisecond
	t.Cleanup(func() {
		PrefillCancelMinElapsed = origMin
		PrefillCancelGrace = origGrace
	})
	if err := GlobalEngine.EnsureStarted(); err != nil {
		t.Fatal(err)
	}

	baseURL, gs := startCarbonTestGateway(t)

	startOff := acpRecordOffset()
	tools := []map[string]any{
		{
			"name":        "Bash",
			"description": "Execute command",
			"input_schema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"command": map[string]any{"type": "string"},
				},
				"required": []string{"command"},
			},
		},
	}
	sys := "Primary working directory: /tmp"
	msgs1 := []map[string]any{
		{"role": "user", "content": "[FAKE_TOOL_CALL] [FAKE_SLOW_RESUME_300MS] run check"},
	}

	resp1, body1, err1 := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "carbon-opt-parked-1", map[string]any{
		"model":      "gemini-4-carbon",
		"max_tokens": 128,
		"system":     sys,
		"tools":      tools,
		"messages":   msgs1,
	})
	if err1 != nil || resp1.StatusCode != 200 {
		t.Fatalf("Turn 1 failed: resp=%v err=%v body=%s", resp1, err1, body1)
	}
	var parsed1 struct {
		StopReason string           `json:"stop_reason"`
		Content    []map[string]any `json:"content"`
	}
	if err := json.Unmarshal([]byte(body1), &parsed1); err != nil || parsed1.StopReason != "tool_use" {
		t.Fatalf("expected tool_use in Turn 1, got %q err=%v body=%s", parsed1.StopReason, err, body1)
	}
	var toolID string
	for _, blk := range parsed1.Content {
		if blk["type"] == "tool_use" {
			toolID, _ = blk["id"].(string)
			break
		}
	}
	if toolID == "" {
		t.Fatalf("missing tool_use id in %s", body1)
	}

	sess := findSessionByConvPrefix(gs, "sid:carbon-opt-parked-1:")
	if sess == nil {
		t.Fatalf("missing session after Turn 1")
	}
	sess.mu.Lock()
	sid := sess.SessionID
	sess.mu.Unlock()

	msgs2 := append(append([]map[string]any{}, msgs1...),
		map[string]any{"role": "assistant", "content": parsed1.Content},
		map[string]any{"role": "user", "content": []map[string]any{{"type": "tool_result", "tool_use_id": toolID, "content": "CHECK_OK"}}},
	)
	payload2 := map[string]any{
		"model":      "gemini-4-carbon",
		"max_tokens": 128,
		"stream":     true,
		"system":     sys,
		"tools":      tools,
		"messages":   msgs2,
	}

	ctx2A, cancel2A := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel2A()
	_, _, err2A := doGatewayJSON(ctx2A, baseURL+"/v1/messages", "x-claude-code-session-id", "carbon-opt-parked-1", payload2)
	if err2A == nil {
		t.Fatalf("expected Turn 2A to abort on 120ms timeout during post-tool resume")
	}

	waitForDetachedSession(t, gs, 500*time.Millisecond)
	if cancels := countRecordedSessionRPCs(startOff, sid, "session/cancel"); cancels != 0 {
		t.Fatalf("expected 0 session/cancel during parked resume soft-detach, got %d", cancels)
	}

	resp2B, body2B, err2B := doGatewayJSON(context.Background(), baseURL+"/v1/messages", "x-claude-code-session-id", "carbon-opt-parked-1", payload2)
	if err2B != nil || resp2B.StatusCode != 200 {
		t.Fatalf("Turn 2B join failed: resp=%v err=%v body=%s", resp2B, err2B, body2B)
	}
	if !strings.Contains(body2B, "TOOL_DONE:CHECK_OK") {
		t.Fatalf("expected Turn 2B response to contain TOOL_DONE:CHECK_OK, got:\n%s", body2B)
	}
	sess.mu.Lock()
	afterSid := sess.SessionID
	rollovers := sess.CompactionRollovers
	sess.mu.Unlock()
	if afterSid != sid || rollovers != 0 {
		t.Fatalf("expected same session (%s vs %s) and 0 rollovers (%d)", sid, afterSid, rollovers)
	}
	if cancels := countRecordedSessionRPCs(startOff, sid, "session/cancel"); cancels != 0 {
		t.Fatalf("expected 0 session/cancel after parked resume join, got %d", cancels)
	}
}

func TestPrefillSoftDetachSupersededByDifferentPrompt(t *testing.T) {
	origMin, origGrace := PrefillCancelMinElapsed, PrefillCancelGrace
	PrefillCancelMinElapsed = 50 * time.Millisecond
	PrefillCancelGrace = 800 * time.Millisecond
	t.Cleanup(func() {
		PrefillCancelMinElapsed = origMin
		PrefillCancelGrace = origGrace
	})
	if err := GlobalEngine.EnsureStarted(); err != nil {
		t.Fatal(err)
	}

	gs := NewGatewayServer()
	t.Cleanup(gs.Close)
	srv := httptest.NewServer(gs.GatewayOnlyHandler())
	defer srv.Close()

	startOff := acpRecordOffset()
	sys := "Primary working directory: /tmp"
	ctxA, cancelA := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancelA()
	_, _, errA := doGatewayJSON(ctxA, srv.URL+"/v1/messages", "x-claude-code-session-id", "carbon-opt-supersede-1", map[string]any{
		"model":      "gemini-4-carbon",
		"max_tokens": 64,
		"stream":     true,
		"system":     sys,
		"messages": []map[string]any{
			{"role": "user", "content": "[FAKE_SLOW] stale prompt alpha"},
		},
	})
	if errA == nil {
		t.Fatalf("expected Turn A to time out")
	}

	sess := waitForDetachedSession(t, gs, 500*time.Millisecond)
	sess.mu.Lock()
	sid := sess.SessionID
	sess.mu.Unlock()
	if cancels := countRecordedSessionRPCs(startOff, sid, "session/cancel"); cancels != 0 {
		t.Fatalf("expected 0 session/cancel before superseding prompt, got %d", cancels)
	}

	respB, bodyB, errB := doGatewayJSON(context.Background(), srv.URL+"/v1/messages", "x-claude-code-session-id", "carbon-opt-supersede-1", map[string]any{
		"model":      "gemini-4-carbon",
		"max_tokens": 64,
		"system":     sys,
		"messages": []map[string]any{
			{"role": "user", "content": "prompt B fresh"},
		},
	})
	if errB != nil || respB.StatusCode != 200 {
		t.Fatalf("Turn B failed after superseding detached turn: resp=%v err=%v body=%s", respB, errB, bodyB)
	}
	if !strings.Contains(bodyB, "FAKE_OK model=gemini-4-carbon") || strings.Contains(bodyB, "stale prompt alpha") {
		t.Fatalf("unexpected Turn B body: %s", bodyB)
	}
	if cancels := countRecordedSessionRPCs(startOff, sid, "session/cancel"); cancels < 1 {
		t.Fatalf("expected >=1 session/cancel to abort superseded Turn A, got %d", cancels)
	}
}

func TestPrefillSoftDetachGraceExpiryCancelsUpstream(t *testing.T) {
	origMin, origGrace := PrefillCancelMinElapsed, PrefillCancelGrace
	PrefillCancelMinElapsed = 50 * time.Millisecond
	PrefillCancelGrace = 120 * time.Millisecond
	t.Cleanup(func() {
		PrefillCancelMinElapsed = origMin
		PrefillCancelGrace = origGrace
	})
	if err := GlobalEngine.EnsureStarted(); err != nil {
		t.Fatal(err)
	}

	gs := NewGatewayServer()
	t.Cleanup(gs.Close)
	srv := httptest.NewServer(gs.GatewayOnlyHandler())
	defer srv.Close()

	startOff := acpRecordOffset()
	ctxA, cancelA := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancelA()
	_, _, errA := doGatewayJSON(ctxA, srv.URL+"/v1/messages", "x-claude-code-session-id", "carbon-opt-expire-1", map[string]any{
		"model":      "gemini-4-carbon",
		"max_tokens": 64,
		"stream":     true,
		"system":     "Primary working directory: /tmp",
		"messages": []map[string]any{
			{"role": "user", "content": "[FAKE_SLOW] expire test"},
		},
	})
	if errA == nil {
		t.Fatalf("expected Turn A to time out")
	}

	sess := waitForDetachedSession(t, gs, 500*time.Millisecond)
	sess.mu.Lock()
	sid := sess.SessionID
	sess.mu.Unlock()
	if sess.IsAvailableForNewTurn() {
		t.Fatalf("session must not be available for new turn while grace window is active")
	}
	if cancels := countRecordedSessionRPCs(startOff, sid, "session/cancel"); cancels != 0 {
		t.Fatalf("expected 0 session/cancel before grace expiry, got %d", cancels)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !sess.HasDetachedTurn() && sess.IsAvailableForNewTurn() && countRecordedSessionRPCs(startOff, sid, "session/cancel") >= 1 {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("grace expiry did not cancel upstream cleanly (detached=%v avail=%v cancels=%d)",
		sess.HasDetachedTurn(), sess.IsAvailableForNewTurn(), countRecordedSessionRPCs(startOff, sid, "session/cancel"))
}

func TestMidTurnDeveloperRolePreservesSystemPrefix(t *testing.T) {
	gs := NewGatewayServer()
	t.Cleanup(gs.Close)
	srv := httptest.NewServer(gs.GatewayOnlyHandler())
	defer srv.Close()
	startOff := acpRecordOffset()

	// 1. /v1/responses: leading developer goes to SystemText, mid-turn developer stays in conversation without rollover
	respR1, bodyR1, errR1 := doGatewayJSON(context.Background(), srv.URL+"/v1/responses", "", "", map[string]any{
		"model":        "gemini-4-carbon",
		"instructions": "Primary working directory: /tmp/resp-prefix",
		"input": []map[string]any{
			{"type": "message", "role": "developer", "content": "lead-dev-rule-1"},
			{"type": "message", "role": "user", "content": "step 1"},
		},
	})
	if errR1 != nil || respR1.StatusCode != 200 {
		t.Fatalf("/v1/responses Turn 1 failed: %v %v %s", respR1, errR1, bodyR1)
	}

	var sessResp *AcpProjectSession
	gs.mu.RLock()
	for _, s := range gs.sessions {
		if s != nil && strings.Contains(s.SystemText, "resp-prefix") {
			sessResp = s
			break
		}
	}
	gs.mu.RUnlock()
	if sessResp == nil {
		t.Fatalf("failed to find /v1/responses session")
	}
	sessResp.mu.Lock()
	sidResp1 := sessResp.SessionID
	sysResp1 := sessResp.SystemText
	sessResp.mu.Unlock()
	if !strings.Contains(sysResp1, "lead-dev-rule-1") {
		t.Fatalf("expected leading developer rule in SystemText, got %q", sysResp1)
	}

	respR2, bodyR2, errR2 := doGatewayJSON(context.Background(), srv.URL+"/v1/responses", "", "", map[string]any{
		"model":        "gemini-4-carbon",
		"instructions": "Primary working directory: /tmp/resp-prefix",
		"input": []map[string]any{
			{"type": "message", "role": "developer", "content": "lead-dev-rule-1"},
			{"type": "message", "role": "user", "content": "step 1"},
			{"type": "message", "role": "assistant", "content": "ok"},
			{"type": "message", "role": "developer", "content": "midturn-rule-xyz"},
			{"type": "message", "role": "user", "content": "step 2"},
		},
	})
	if errR2 != nil || respR2.StatusCode != 200 {
		t.Fatalf("/v1/responses Turn 2 failed: %v %v %s", respR2, errR2, bodyR2)
	}
	sessResp.mu.Lock()
	sidResp2 := sessResp.SessionID
	sysResp2 := sessResp.SystemText
	rollResp := sessResp.CompactionRollovers
	sessResp.mu.Unlock()
	if sidResp2 != sidResp1 || rollResp != 0 {
		t.Fatalf("/v1/responses mid-turn developer forced rollover: sid1=%s sid2=%s rollovers=%d", sidResp1, sidResp2, rollResp)
	}
	if strings.Contains(sysResp2, "midturn-rule-xyz") || sysResp2 != sysResp1 {
		t.Fatalf("mid-turn developer polluted SystemText: %q -> %q", sysResp1, sysResp2)
	}
	if got := countRecordedSessionRPCContains(startOff, sidResp1, "session/prompt", "midturn-rule-xyz"); got != 1 {
		t.Fatalf("expected mid-turn developer text in session/prompt delta, got count=%d", got)
	}

	// 2. /v1/chat/completions: leading system stays prefix, mid-turn developer stays user message without rollover
	respC1, bodyC1, errC1 := doGatewayJSON(context.Background(), srv.URL+"/v1/chat/completions", "", "", map[string]any{
		"model": "gemini-4-carbon",
		"messages": []map[string]any{
			{"role": "system", "content": "Primary working directory: /tmp/chat-prefix"},
			{"role": "user", "content": "hi 1"},
		},
	})
	if errC1 != nil || respC1.StatusCode != 200 {
		t.Fatalf("/v1/chat/completions Turn 1 failed: %v %v %s", respC1, errC1, bodyC1)
	}

	var sessChat *AcpProjectSession
	gs.mu.RLock()
	for _, s := range gs.sessions {
		if s != nil && strings.Contains(s.SystemText, "chat-prefix") {
			sessChat = s
			break
		}
	}
	gs.mu.RUnlock()
	if sessChat == nil {
		t.Fatalf("failed to find /v1/chat/completions session")
	}
	sessChat.mu.Lock()
	sidChat1 := sessChat.SessionID
	sysChat1 := sessChat.SystemText
	sessChat.mu.Unlock()

	respC2, bodyC2, errC2 := doGatewayJSON(context.Background(), srv.URL+"/v1/chat/completions", "", "", map[string]any{
		"model": "gemini-4-carbon",
		"messages": []map[string]any{
			{"role": "system", "content": "Primary working directory: /tmp/chat-prefix"},
			{"role": "user", "content": "hi 1"},
			{"role": "assistant", "content": "ok"},
			{"role": "developer", "content": "midturn-chat-dev-abc"},
			{"role": "user", "content": "hi 2"},
		},
	})
	if errC2 != nil || respC2.StatusCode != 200 {
		t.Fatalf("/v1/chat/completions Turn 2 failed: %v %v %s", respC2, errC2, bodyC2)
	}
	sessChat.mu.Lock()
	sidChat2 := sessChat.SessionID
	sysChat2 := sessChat.SystemText
	rollChat := sessChat.CompactionRollovers
	sessChat.mu.Unlock()
	if sidChat2 != sidChat1 || rollChat != 0 {
		t.Fatalf("/v1/chat/completions mid-turn developer forced rollover: sid1=%s sid2=%s rollovers=%d", sidChat1, sidChat2, rollChat)
	}
	if strings.Contains(sysChat2, "midturn-chat-dev-abc") || sysChat2 != sysChat1 {
		t.Fatalf("/v1/chat/completions mid-turn developer polluted SystemText: %q -> %q", sysChat1, sysChat2)
	}
	if got := countRecordedSessionRPCContains(startOff, sidChat1, "session/prompt", "midturn-chat-dev-abc"); got != 1 {
		t.Fatalf("expected mid-turn developer text in chat session/prompt delta, got count=%d", got)
	}
}

func TestGapTtlUsesTurnStartTimeAnd80KWarnThreshold(t *testing.T) {
	if CacheWarnMinTokens != 80000 {
		t.Fatalf("expected default CacheWarnMinTokens=80000, got %d", CacheWarnMinTokens)
	}

	s := NewAcpProjectSession("sid:test-gap-ttl:1", "/tmp", "sys")
	s.SessionID = "00000000-0000-4000-8000-000000007777"
	s.ModelID = "gemini-4-carbon"
	s.ThoughtLevel = "medium"

	// Prev ended 7m ago, current turn ran for 3m -> turnStartAt was 4m after prev (< 5m TTL) -> "zero", NOT "gap_ttl".
	s.TurnHistory = append(s.TurnHistory, TurnHistoryEntry{
		CompletedAtMs: time.Now().UTC().Add(-7 * time.Minute).UnixMilli(),
		SessionID:     s.SessionID,
		Model:         s.ModelID,
		Mode:          "cold_start",
		Usage: &TurnUsage{
			InputTokens:        100000,
			ContextInputTokens: 100000,
			StepsInPrompt:      1,
			UsageSource:        "db",
		},
	})
	s.logTurnStats("sticky_delta", 180000, TurnResult{
		StopReason: "end_turn",
		Usage: &TurnUsage{
			InputTokens:        100000,
			ContextInputTokens: 100000,
			StepsInPrompt:      1,
			UsageSource:        "db",
		},
	})
	if got := s.TurnHistory[len(s.TurnHistory)-1].CacheMiss; got != "zero" {
		t.Fatalf("expected CacheMiss=zero when turnStartAt-prev=4m (<5m) despite now-prev=7m, got %q", got)
	}

	// Prev ended 9m ago, current turn ran for 3m -> turnStartAt was 6m after prev (>= 5m TTL) -> "gap_ttl".
	s.TurnHistory = []TurnHistoryEntry{{
		CompletedAtMs: time.Now().UTC().Add(-9 * time.Minute).UnixMilli(),
		SessionID:     s.SessionID,
		Model:         s.ModelID,
		Mode:          "cold_start",
		Usage: &TurnUsage{
			InputTokens:        100000,
			ContextInputTokens: 100000,
			StepsInPrompt:      1,
			UsageSource:        "db",
		},
	}}
	s.logTurnStats("sticky_delta", 180000, TurnResult{
		StopReason: "end_turn",
		Usage: &TurnUsage{
			InputTokens:        100000,
			ContextInputTokens: 100000,
			StepsInPrompt:      1,
			UsageSource:        "db",
		},
	})
	if got := s.TurnHistory[len(s.TurnHistory)-1].CacheMiss; got != "gap_ttl" {
		t.Fatalf("expected CacheMiss=gap_ttl when turnStartAt-prev=6m (>=5m), got %q", got)
	}

	// Verify CacheWarnMinTokens=80000 triggers streak=3 warning at 95K context.
	sWarn := NewAcpProjectSession("sid:test-warn-80k:1", "/tmp", "sys")
	sWarn.SessionID = "00000000-0000-4000-8000-000000007778"
	sWarn.ModelID = "gemini-4-carbon"
	sWarn.ThoughtLevel = "medium"
	mkZero95k := func() *TurnUsage {
		return &TurnUsage{
			InputTokens:            95000,
			CachedReadTokens:       0,
			ContextInputTokens:     95000,
			ContextCachedTokens:    0,
			RawUncachedInputTokens: 95000,
			OutputTokens:           64,
			StepsInPrompt:          1,
			UsageSource:            "acp",
		}
	}
	sWarn.logTurnStats("cold_start", 100, TurnResult{StopReason: "end_turn", Usage: mkZero95k()})
	for i := 0; i < 3; i++ {
		sWarn.logTurnStats("sticky_delta", 100, TurnResult{StopReason: "end_turn", Usage: mkZero95k()})
	}
	if sWarn.ZeroCacheStreak != 3 || !strings.Contains(sWarn.LastCacheWarning, "streak=3") {
		t.Fatalf("expected ZeroCacheStreak=3 warning at 95K (>=80K), got streak=%d warn=%q", sWarn.ZeroCacheStreak, sWarn.LastCacheWarning)
	}
}
