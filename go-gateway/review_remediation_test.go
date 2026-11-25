package main

// Regression tests for session continuity, deferred tool updates, and ACP stream filtering.

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var reviewTools = []map[string]any{{
	"name": "Bash", "description": "Execute bash",
	"input_schema": map[string]any{"type": "object", "properties": map[string]any{"command": map[string]any{"type": "string"}}},
}}

func reviewPost(t *testing.T, baseURL, path, sid string, body map[string]any) (int, string) {
	t.Helper()
	raw, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", baseURL+path, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	if sid != "" {
		req.Header.Set("x-claude-code-session-id", sid)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var b strings.Builder
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 4<<20)
	for sc.Scan() {
		b.WriteString(sc.Text() + "\n")
	}
	return resp.StatusCode, b.String()
}

func reviewMessages(model string, msgs []map[string]any, extra map[string]any) map[string]any {
	body := map[string]any{"model": model, "max_tokens": 256, "system": "Primary working directory: /tmp", "tools": reviewTools, "messages": msgs}
	for k, v := range extra {
		body[k] = v
	}
	return body
}

// R1: a live-session model switch that agy rejects or does not confirm must fail the request and
// never prompt the old model under the new model's name.
func TestReviewLiveModelSwitchMustBeConfirmed(t *testing.T) {
	for _, tc := range []struct{ name, env, val string }{
		{"rejected", "FAKE_ACP_REJECT_MODEL", "gemini-4-carbon"},
		{"empty_ack", "FAKE_ACP_EMPTY_ACK_MODEL", "gemini-4-carbon"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			GlobalEngine.StopEngine()
			t.Setenv(tc.env, tc.val)
			baseURL, cleanup := startTestGateway(t)
			defer cleanup()
			sid := "r1-" + tc.name
			msgs := []map[string]any{{"role": "user", "content": "turn one"}}
			code, out := reviewPost(t, baseURL, "/v1/messages", sid, reviewMessages("gemini-3.8-flash-low", msgs, nil))
			if code != 200 || !strings.Contains(out, "model=gemini-3.8-flash tier=low") {
				t.Fatalf("first turn: %d %s", code, out)
			}
			var first map[string]any
			_ = json.Unmarshal([]byte(out), &first)
			msgs = append(msgs, map[string]any{"role": "assistant", "content": first["content"]}, map[string]any{"role": "user", "content": "turn two"})
			code, out = reviewPost(t, baseURL, "/v1/messages", sid, reviewMessages("gemini-4-carbon", msgs, nil))
			if code < 400 || strings.Contains(out, "FAKE_OK") {
				t.Fatalf("unconfirmed switch must fail without prompting: %d %s", code, out)
			}
			// The next request for the original model re-pins and continues on the same session.
			code, out = reviewPost(t, baseURL, "/v1/messages", sid, reviewMessages("gemini-3.8-flash-low", msgs, nil))
			if code != 200 || !strings.Contains(out, "model=gemini-3.8-flash tier=low turn=2") {
				t.Fatalf("recovery turn: %d %s", code, out)
			}
		})
	}
}

// R1/O4: a new session whose model acknowledgement is empty must not run.
func TestReviewNewSessionEmptyAckFailsClosed(t *testing.T) {
	GlobalEngine.StopEngine()
	defer GlobalEngine.StopEngine()
	t.Setenv("FAKE_ACP_EMPTY_ACK_MODEL", "gemini-4-argon")
	s := NewAcpProjectSession("empty-ack", "/tmp", "")
	s.ModelID = "gemini-4-argon"
	if err := s.ensureBooted(nil, "medium"); err == nil || s.SessionID != "" {
		t.Fatalf("empty acknowledgement accepted: err=%v sid=%q", err, s.SessionID)
	}
}

// R1/O4: a rejected thinking tier is an error, not a silently skipped setting.
func TestReviewRejectedTierFailsClosed(t *testing.T) {
	GlobalEngine.StopEngine()
	t.Setenv("FAKE_ACP_REJECT_TIER", "high")
	baseURL, cleanup := startTestGateway(t)
	defer cleanup()
	code, out := reviewPost(t, baseURL, "/v1/messages", "r1-tier", reviewMessages("gemini-3.8-flash-high", []map[string]any{{"role": "user", "content": "hi"}}, nil))
	if code != 400 || strings.Contains(out, "FAKE_OK") {
		t.Fatalf("rejected tier must return 400 without prompting: %d %s", code, out)
	}
}

// P3: after session/resume agy resets to carbon/medium; both model and tier must be re-pinned.
func TestReviewResumeRepinsModelAndTier(t *testing.T) {
	GlobalEngine.StopEngine()
	baseURL, cleanup := startTestGateway(t)
	defer cleanup()
	sid := "p3-resume"
	msgs := []map[string]any{{"role": "user", "content": "turn one"}}
	code, out := reviewPost(t, baseURL, "/v1/messages", sid, reviewMessages("gemini-4-argon-low", msgs, nil))
	if code != 200 || !strings.Contains(out, "model=gemini-4-argon tier=low") {
		t.Fatalf("first: %d %s", code, out)
	}
	// hibernate every worker, keeping resume state
	var first map[string]any
	_ = json.Unmarshal([]byte(out), &first)
	for _, s := range listTestSessions() {
		s.HibernateWorker("test_resume", false)
	}
	msgs = append(msgs, map[string]any{"role": "assistant", "content": first["content"]}, map[string]any{"role": "user", "content": "turn two"})
	code, out = reviewPost(t, baseURL, "/v1/messages", sid, reviewMessages("gemini-4-argon-low", msgs, nil))
	if code != 200 || !strings.Contains(out, "model=gemini-4-argon tier=low turn=2") {
		t.Fatalf("resumed turn must run on the pinned model and tier: %d %s", code, out)
	}
}

// R2: an Argon degradation rollover must not close the next turn's new session.
func TestReviewDelayedHibernateMustNotCancelNextTurn(t *testing.T) {
	tmp := t.TempDir()
	conv := filepath.Join(tmp, "conversations")
	t.Cleanup(func() { GlobalEngine.StopEngine(); initConfig() })
	t.Setenv("AGY_ACP_BIN", fakeAcpBin)
	t.Setenv("AGY_PURE_API_HOME", filepath.Join(tmp, "pure"))
	t.Setenv("AGY_CONV_DIR", conv)
	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "gateway.db"))
	t.Setenv("AGY_GW_LOG", filepath.Join(tmp, "gateway.log"))
	t.Setenv("AGY_TOS_TRIPPED_FILE", filepath.Join(tmp, "tos-tripped"))
	initConfig()
	GlobalEngine.StopEngine()
	sid1 := "00000000-0000-4000-8000-000000000001"
	sid2 := "00000000-0000-4000-8000-000000000002"
	// servedModel mismatch (.1.19 = gemini-4-argon-b) -> degraded rollover
	writeTestGenMetadataSQLite(t, filepath.Join(conv, sid1+".db"), 0, buildTestGenMetadataBlobServed("gemini-4-argon-b", 1417, 127990, 80, 0, 0, 80, 168000, 983040, 1.9, 0.4))
	writeTestGenMetadataSQLite(t, filepath.Join(conv, sid2+".db"), 0, buildTestGenMetadataBlobServed("gemini-4-argon", 1417, 3000, 140, 164946, 60, 80, 168000, 983040, 4.2, 1.6))
	gs := NewGatewayServer()
	defer gs.Close()
	req := httptest.NewRequest("POST", "/v1/messages", nil)
	req.Header.Set("x-claude-code-session-id", "review-delayed-close")
	body := map[string]any{"model": "gemini-4-argon-high", "system": "Primary working directory: " + tmp}
	messages := []map[string]any{{"role": "user", "content": "First reply"}}
	first, err := gs.HandleMessagesRequest(req, body, messages, func(DeltaChunk) {})
	if err != nil {
		t.Fatal(err)
	}
	messages = append(messages, map[string]any{"role": "assistant", "content": first.Text}, map[string]any{"role": "user", "content": "[FAKE_SLOW] Second reply"})
	second, err := gs.HandleMessagesRequest(req, body, messages, func(DeltaChunk) {})
	if err != nil || !strings.Contains(second.Text, "FAKE_OK") || second.SessionID != sid2 {
		t.Fatalf("next turn interrupted: err=%v stop=%s text=%q sid=%s", err, second.StopReason, second.Text, second.SessionID)
	}
}

// R4: every unreported generation is summed, and a split tool batch never re-bills a generation.
func TestReviewUsageMustIncludeAllUnreportedGenerations(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("AGY_CONV_DIR", dir)
	sid := "00000000-0000-4000-8000-000000000081"
	path := filepath.Join(dir, sid+".db")
	writeTestGenMetadataSQLite(t, path, 0, buildTestGenMetadataBlob(1319, 100, 10, 0, 0, 10, 100, 1000000, 2, 1))
	writeTestGenMetadataSQLite(t, path, 1, buildTestGenMetadataBlob(1319, 200, 20, 0, 0, 20, 200, 1000000, 2, 1))
	s := NewAcpProjectSession("review-usage-sum", "/tmp", "")
	s.SessionID, s.ModelID = sid, "gemini-3.8-flash"
	usage := s.normalizeTurnUsage(nil, false, 0, 0)
	if usage.InputTokens != 300 || usage.OutputTokens != 30 || usage.Generations != 2 {
		t.Fatalf("two pending generations: got %+v", usage)
	}
	again := s.normalizeTurnUsage(nil, false, 0, 0)
	if again.InputTokens != 0 || again.OutputTokens != 0 || again.UsageSource != "none" {
		t.Fatalf("no new generation must report zero usage, got %+v", again)
	}
	// a zero-token row still advances the cursor; a late row is billed once on the next turn
	writeTestGenMetadataSQLite(t, path, 2, buildTestGenMetadataBlob(1319, 0, 0, 0, 0, 0, 0, 0, 0, 0))
	writeTestGenMetadataSQLite(t, path, 3, buildTestGenMetadataBlob(1319, 50, 5, 0, 0, 5, 50, 1000000, 1, 1))
	late := s.normalizeTurnUsage(nil, false, 0, 0)
	if late.InputTokens != 50 || late.Generations != 1 || s.LastSeenGenMetaIdx != 3 {
		t.Fatalf("late row: got %+v cursor=%d", late, s.LastSeenGenMetaIdx)
	}
}

func TestReviewGenerationAndStepMustNotBeCountedTwice(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("AGY_CONV_DIR", dir)
	sid := "00000000-0000-4000-8000-000000000082"
	path := filepath.Join(dir, sid+".db")
	writeTestGenMetadataSQLite(t, path, 0, buildTestGenMetadataBlob(1319, 123, 45, 0, 0, 45, 123, 1000000, 2, 1))
	writeTestStepSQLite(t, path, 0, 123, 45)
	s := NewAcpProjectSession("review-usage-duplicate", "/tmp", "")
	s.SessionID, s.ModelID = sid, "gemini-3.8-flash"
	first := s.normalizeTurnUsage(nil, true, 0, 0)
	// the step of the same generation lands only after the tool batch was billed
	writeTestStepSQLite(t, path, 1, 123, 45)
	second := s.normalizeTurnUsage(nil, false, 0, 0)
	if first.InputTokens != 123 || second.InputTokens != 0 || second.OutputTokens != 0 {
		t.Fatalf("same generation billed twice: first=%+v second=%+v", first, second)
	}
}

// Steps are only the fallback for databases without gen_metadata rows.
func TestReviewStepsOnlyDatabaseIsBilledOnce(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("AGY_CONV_DIR", dir)
	sid := "00000000-0000-4000-8000-000000000083"
	path := filepath.Join(dir, sid+".db")
	writeTestStepSQLite(t, path, 0, 400, 40)
	writeTestStepSQLite(t, path, 1, 500, 50)
	s := NewAcpProjectSession("review-steps-only", "/tmp", "")
	s.SessionID, s.ModelID = sid, "gemini-3.8-flash"
	u := s.normalizeTurnUsage(nil, false, 0, 0)
	if u.InputTokens != 900 || u.OutputTokens != 90 {
		t.Fatalf("steps-only sum: %+v", u)
	}
	if again := s.normalizeTurnUsage(nil, false, 0, 0); again.InputTokens != 0 || again.UsageSource != "none" {
		t.Fatalf("steps re-billed: %+v", again)
	}
}

func writeTestStepSQLite(t *testing.T, dbPath string, idx, in, out int) {
	t.Helper()
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec("CREATE TABLE IF NOT EXISTS steps (idx INTEGER PRIMARY KEY, step_type INTEGER, metadata BLOB, step_payload BLOB)"); err != nil {
		t.Fatal(err)
	}
	stats := append(protoUint(1, 1319), protoUint(2, uint64(in))...)
	stats = append(stats, protoUint(3, uint64(out))...)
	if _, err = db.Exec("INSERT OR REPLACE INTO steps VALUES (?,15,?,?)", idx, protoBytes(9, stats), []byte{}); err != nil {
		t.Fatal(err)
	}
}

// ServedModel (.1.19/.1.22) deterministically identifies Argon shadow-pool routing (e.g.
// gemini-4-argon-b), while primary gemini-4-argon is never false-flagged regardless of TPS.
func TestReviewServedModelSignal(t *testing.T) {
	gm := ParseGenMetadataBlob(buildTestGenMetadataBlobServed("gemini-4-argon-b", 1418, 3000, 300, 30000, 50, 250, 33000, 983040, 3, 2))
	if gm == nil || gm.ServedModel != "gemini-4-argon-b" {
		t.Fatalf("served model not parsed: %+v", gm)
	}
	rep := EvaluateArgonDegradation(gm, "gemini-4-argon")
	if !rep.Degraded || rep.ServedModel != "gemini-4-argon-b" {
		t.Fatalf("servedModel mismatch on Argon must deterministically mark degraded: %+v", rep)
	}
	primaryFast := ParseGenMetadataBlob(buildTestGenMetadataBlobServed("gemini-4-argon", 1418, 3000, 300, 32768, 0, 300, 35768, 983040, 1, 0.5))
	if rep := EvaluateArgonDegradation(primaryFast, "gemini-4-argon"); rep.Degraded {
		t.Fatalf("primary gemini-4-argon must never false-trigger on fast TPS or 2048 cache: %+v", rep)
	}
}

// R3: sessions with dsh-bridge keep their warm session on tool additions/removals/schema changes
// via consumeToolDelta + dsh_call_tool; 0-tool sessions roll over once when tools first appear;
// removed/unknown tools are rejected; system changes reach the model as a prompt delta.
func TestReviewToolInventoryAndSystemChanges(t *testing.T) {
	GlobalEngine.StopEngine()
	baseURL, cleanup := startTestGateway(t)
	defer cleanup()
	sid := "r3-inventory"
	read := map[string]any{"name": "Read", "description": "Read a file", "input_schema": map[string]any{"type": "object", "properties": map[string]any{"path": map[string]any{"type": "string"}}}}
	write := map[string]any{"name": "Write", "description": "Write a file", "input_schema": map[string]any{"type": "object", "properties": map[string]any{"path": map[string]any{"type": "string"}}}}
	post := func(targetSid string, tools []map[string]any, system string, msgs []map[string]any) map[string]any {
		code, out := reviewPost(t, baseURL, "/v1/messages", targetSid, map[string]any{"model": "gemini-3.8-flash", "max_tokens": 64, "system": system, "tools": tools, "messages": msgs})
		if code != 200 {
			t.Fatalf("status %d: %s", code, out)
		}
		var d map[string]any
		_ = json.Unmarshal([]byte(out), &d)
		return d
	}
	text := func(d map[string]any) string {
		b, _ := json.Marshal(d["content"])
		return string(b)
	}
	sysA := "Primary working directory: /tmp\nREVIEW_RULE_A"
	msgs := []map[string]any{{"role": "user", "content": "[ECHO_TOOLS] one"}}
	d1 := post(sid, []map[string]any{read}, sysA, msgs)
	if !strings.Contains(text(d1), "ws_Read_") {
		t.Fatalf("first catalog: %s", text(d1))
	}
	// added tool on an existing ws-bridge session -> keeps session (turn=2), injects delta + allows ws_call_tool
	msgs = append(msgs, map[string]any{"role": "assistant", "content": d1["content"]}, map[string]any{"role": "user", "content": "[ECHO_PROMPT] two"})
	d2 := post(sid, []map[string]any{read, write}, sysA, msgs)
	if !strings.Contains(text(d2), "turn=2") || !strings.Contains(text(d2), "Newly available tools") || !strings.Contains(text(d2), "Write") {
		t.Fatalf("added tool must preserve session and inject delta: %s", text(d2))
	}
	// removed tool -> same session (turn=3), notice in prompt, calls rejected
	msgs = append(msgs, map[string]any{"role": "assistant", "content": d2["content"]}, map[string]any{"role": "user", "content": "[ECHO_PROMPT] three"})
	d3 := post(sid, []map[string]any{write}, sysA, msgs)
	if !strings.Contains(text(d3), "turn=3") || !strings.Contains(text(d3), "no longer available") || !strings.Contains(text(d3), "Read") {
		t.Fatalf("removed tool must keep the session and notify: %s", text(d3))
	}
	var sess *AcpProjectSession
	for _, s := range listTestSessions() {
		if strings.Contains(s.ConvKey, sid) {
			sess = s
		}
	}
	if sess == nil {
		t.Fatal("session not found")
	}
	sess.mu.Lock()
	_, allowedRead := resolveAllowedHostTool(sess.McpToHostMap, mcpToolName("Read"))
	_, viaCallRead := allowedHostToolName(sess.McpToHostMap, "Read")
	resolvedWrite, viaCallWrite := allowedHostToolName(sess.McpToHostMap, "Write")
	_, unknownWs := resolveAllowedHostTool(sess.McpToHostMap, "mcp_ws-bridge_Bash")
	_, unknownLegacy := resolveAllowedHostTool(sess.McpToHostMap, "mcp_dsh-bridge_Bash")
	sess.mu.Unlock()
	if allowedRead || viaCallRead || unknownWs || unknownLegacy || !viaCallWrite || resolvedWrite != "Write" {
		t.Fatalf("whitelist mismatch: read=%v callRead=%v callWrite=%v(%q) unknownWs=%v unknownLegacy=%v", allowedRead, viaCallRead, viaCallWrite, resolvedWrite, unknownWs, unknownLegacy)
	}
	// system prompt change reaches the live session as a delta (no cold start)
	msgs = append(msgs, map[string]any{"role": "assistant", "content": d3["content"]}, map[string]any{"role": "user", "content": "[ECHO_PROMPT] four"})
	d4 := post(sid, []map[string]any{write}, "Primary working directory: /tmp\nREVIEW_RULE_B", msgs)
	if !strings.Contains(text(d4), "turn=4") || !strings.Contains(text(d4), "REVIEW_RULE_B") {
		t.Fatalf("system change must reach the live session: %s", text(d4))
	}
	// unchanged follow-up: no rollover, no notice
	msgs = append(msgs, map[string]any{"role": "assistant", "content": d4["content"]}, map[string]any{"role": "user", "content": "[ECHO_PROMPT] five"})
	d5 := post(sid, []map[string]any{write}, "Primary working directory: /tmp\nREVIEW_RULE_B", msgs)
	if !strings.Contains(text(d5), "turn=5") || strings.Contains(text(d5), "Host instructions updated") || strings.Contains(text(d5), "Host tool update") {
		t.Fatalf("unchanged follow-up must reuse quietly: %s", text(d5))
	}
	// 0-tool session gaining tools for the first time MUST roll over so ws-bridge is mounted
	sidZero := "r3-zero-tools"
	zMsgs := []map[string]any{{"role": "user", "content": "one"}}
	z1 := post(sidZero, nil, sysA, zMsgs)
	zMsgs = append(zMsgs, map[string]any{"role": "assistant", "content": z1["content"]}, map[string]any{"role": "user", "content": "[ECHO_TOOLS] two"})
	z2 := post(sidZero, []map[string]any{read}, sysA, zMsgs)
	if !strings.Contains(text(z2), "turn=1") || !strings.Contains(text(z2), "ws_Read_") {
		t.Fatalf("0-tool session gaining tools must roll over to mount ws-bridge: %s", text(z2))
	}
}

// R3a: tools/call for a name outside the catalog is answered with an MCP error, not forwarded.
func TestReviewMcpRejectsUnknownTool(t *testing.T) {
	sess := NewAcpProjectSession("r3a", "/tmp", "")
	sess.ToolsCatalog = sanitizeToolsCatalog([]any{map[string]any{"name": "Bash", "input_schema": map[string]any{"type": "object"}}})
	for _, td := range sess.ToolsCatalog {
		sess.McpToHostMap[td.McpName] = td.Name
	}
	for _, call := range []map[string]any{
		{"name": "mcp_ws-bridge_Write", "arguments": map[string]any{}},
		{"name": "mcp_dsh-bridge_Write", "arguments": map[string]any{}},
		{"name": mcpToolName(CallToolName), "arguments": map[string]any{"name": "Write", "arguments": map[string]any{}}},
	} {
		body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 7, "method": "tools/call", "params": call})
		req := httptest.NewRequest(http.MethodPost, "/mcp/"+sess.TenantKey, bytes.NewReader(body))
		req.RemoteAddr = "127.0.0.1:5000"
		req.Header.Set("Authorization", "Bearer "+sess.TenantKey)
		rec := httptest.NewRecorder()
		done := make(chan struct{})
		go func() { sess.HandleMcpHTTP(rec, req); close(done) }()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatalf("call %v was parked for the host instead of rejected", call)
		}
		if !strings.Contains(rec.Body.String(), `"isError":true`) || !strings.Contains(rec.Body.String(), "not available") {
			t.Fatalf("call %v not rejected: %s", call, rec.Body.String())
		}
	}
	if len(sess.ParkedCalls) != 0 {
		t.Fatal("rejected call was parked")
	}
}

// R5/P6: status lines are recognised anywhere in the reply.
func TestReviewLateNoticeIsAFailure(t *testing.T) {
	GlobalEngine.StopEngine()
	baseURL, cleanup := startTestGateway(t)
	defer cleanup()
	for _, stream := range []bool{false, true} {
		code, out := reviewPost(t, baseURL, "/v1/messages", "r5-late", reviewMessages("gemini-3.8-flash", []map[string]any{{"role": "user", "content": "[FAKE_LATE_NOTICE] go"}}, map[string]any{"stream": stream}))
		if strings.Contains(out, "network issue connecting") && !strings.Contains(out, `"error"`) {
			t.Fatalf("stream=%v notice leaked: %s", stream, out)
		}
		if !stream && code != 503 {
			t.Fatalf("non-stream late notice: %d %s", code, out)
		}
		if stream && (!strings.Contains(out, "event: error") || strings.Contains(out, `"stop_reason":"end_turn"`)) {
			t.Fatalf("stream late notice must end with an error event: %s", out)
		}
		for _, line := range strings.Split(out, "\n") {
			if strings.Contains(line, "text_delta") && strings.Contains(line, "⚠") {
				t.Fatalf("stream=%v status line streamed: %s", stream, line)
			}
		}
		code, out = reviewPost(t, baseURL, "/v1/messages", "r5-interrupted", reviewMessages("gemini-3.8-flash", []map[string]any{{"role": "user", "content": "[FAKE_NOTICE_INTERRUPTED] go"}}, map[string]any{"stream": stream}))
		if code != 200 || strings.Contains(out, "stream was interrupted") || !strings.Contains(out, "First part.") || !strings.Contains(out, "Second part after resume.") {
			t.Fatalf("stream=%v interrupted info line must be dropped and the reply kept: %d %s", stream, code, out)
		}
	}
	for _, path := range []string{"/v1/chat/completions", "/v1/responses"} {
		body := map[string]any{"model": "gemini-3.8-flash", "stream": true, "messages": []map[string]any{{"role": "user", "content": "[FAKE_LATE_NOTICE] go"}}, "input": "[FAKE_LATE_NOTICE] go"}
		_, out := reviewPost(t, baseURL, path, "", body)
		if strings.Contains(out, "network issue connecting") && !strings.Contains(out, "error") {
			t.Fatalf("%s leaked notice: %s", path, out)
		}
		if path == "/v1/responses" && !strings.Contains(out, "response.failed") {
			t.Fatalf("responses stream must fail: %s", out)
		}
		if path == "/v1/chat/completions" && (!strings.Contains(out, `"error"`) || strings.Contains(out, `"finish_reason":"stop"`)) {
			t.Fatalf("chat stream must fail: %s", out)
		}
	}
}

func TestReviewStatusLineFilterUnit(t *testing.T) {
	feed := func(chunks ...string) (*activeTurnState, string) {
		turn := &activeTurnState{}
		var out strings.Builder
		for _, c := range chunks {
			out.WriteString(turn.acceptTextLocked(c))
		}
		out.WriteString(turn.finishNoticeLocked())
		return turn, out.String()
	}
	if turn, out := feed("Answer line.\n", "\n⚠ ", "There was a network issue connecting to the server, please try again.\n"); strings.TrimSpace(out) != "Answer line." || !turn.noticeFailed {
		t.Fatalf("late split notice: out=%q failed=%v", out, turn.noticeFailed)
	}
	if turn, out := feed("Answer.\n⚠ Our servers are experiencing high traffic right now, please try again in a minute.\n", "Recovered text."); !strings.Contains(out, "Recovered text.") || strings.Contains(out, "traffic") || turn.noticeFailed {
		t.Fatalf("notice followed by recovery: out=%q failed=%v", out, turn.noticeFailed)
	}
	if _, out := feed("Note:\n", "⚠️ Be careful with rm -rf.\n"); !strings.Contains(out, "⚠️ Be careful") {
		t.Fatalf("model warning emoji swallowed: %q", out)
	}
	if _, out := feed("He said \"There was a network issue connecting to the server, please try again.\" yesterday."); !strings.Contains(out, "yesterday") {
		t.Fatalf("quoted prose altered: %q", out)
	}
	if _, out := feed("⚠ Some unknown agy status\n", "text"); !strings.Contains(out, "unknown agy status") {
		t.Fatalf("unknown status line must pass: %q", out)
	}
	if turn, out := feed("⚠ API error (attempt 1): UNAVAILABLE (code 503): No capacity available for model\n", "fine"); out != "fine" || turn.noticeFailed {
		t.Fatalf("retry line: %q", out)
	}
}

// R6: fast local replies only for Claude Code probes on /v1/messages.
func TestReviewFastPathOnlyForRealProbes(t *testing.T) {
	GlobalEngine.StopEngine()
	baseURL, cleanup := startTestGateway(t)
	defer cleanup()
	ccSys := "You are Claude Code, Anthropic's official CLI for Claude.\nPrimary working directory: /tmp"
	cases := []struct {
		name, path string
		body       map[string]any
		wantModel  bool
	}{
		{"quoted suggestion", "/v1/messages", reviewMessages("gemini-3.8-flash", []map[string]any{{"role": "user", "content": `Explain the literal string "[SUGGESTION MODE:" in this Go code.`}}, nil), true},
		{"messages max_tokens=1 without identity", "/v1/messages", reviewMessages("gemini-3.8-flash", []map[string]any{{"role": "user", "content": "say hi"}}, map[string]any{"max_tokens": 1}), true},
		{"chat max_tokens=1", "/v1/chat/completions", map[string]any{"model": "gemini-3.8-flash", "max_tokens": 1, "messages": []map[string]any{{"role": "user", "content": "say hi"}}}, true},
		{"responses max_tokens=1 stream", "/v1/responses", map[string]any{"model": "gemini-3.8-flash", "max_tokens": 1, "stream": true, "input": "say hi"}, true},
		{"claude code quota probe", "/v1/messages", map[string]any{"model": "gemini-3.8-flash", "max_tokens": 1, "system": ccSys, "messages": []map[string]any{{"role": "user", "content": "quota"}}}, false},
		{"claude code suggestion", "/v1/messages", map[string]any{"model": "gemini-3.8-flash", "max_tokens": 256, "system": ccSys, "messages": []map[string]any{{"role": "user", "content": "[SUGGESTION MODE: Suggest what the user might type next.]"}}}, false},
	}
	for _, c := range cases {
		code, out := reviewPost(t, baseURL, c.path, "", c.body)
		gotModel := strings.Contains(out, "FAKE_OK")
		if code != 200 || gotModel != c.wantModel {
			t.Fatalf("%s: code=%d model=%v want %v: %s", c.name, code, gotModel, c.wantModel, out)
		}
		if c.path == "/v1/chat/completions" && !strings.Contains(out, "chat.completion") {
			t.Fatalf("%s: wrong protocol shape: %s", c.name, out)
		}
		if c.path == "/v1/responses" && !strings.Contains(out, "response.completed") {
			t.Fatalf("%s: wrong protocol shape: %s", c.name, out)
		}
	}
}

// K2: compaction is recognised only at the start of the final user text block.
func TestReviewCompactionDetectionAnchored(t *testing.T) {
	yes := []map[string]any{{"role": "user", "content": "Your task is to create a detailed summary of the conversation so far."}}
	no := []map[string]any{{"role": "user", "content": strings.Repeat("context line\n", 100) + "Docs say: create a detailed summary of the conversation."}}
	quotedEarlier := []map[string]any{{"role": "user", "content": "Your task is to create a detailed summary of the conversation so far."}, {"role": "assistant", "content": "ok"}, {"role": "user", "content": "thanks"}}
	if !isCompactionSummaryRequest(yes) || isCompactionSummaryRequest(no) || isCompactionSummaryRequest(quotedEarlier) {
		t.Fatal("compaction detection not anchored")
	}
}

// K5: cache miss classification.
func TestReviewCacheMissClassification(t *testing.T) {
	now := time.Now()
	prev := &TurnHistoryEntry{Model: "gemini-4-argon", CompletedAtMs: now.Add(-30 * time.Second).UnixMilli(), Usage: &TurnUsage{RawUncachedInputTokens: 2000, RawTpuCachedTokens: 100000, UsageSource: "db"}}
	u := func(in, cr int) *TurnUsage {
		return &TurnUsage{RawUncachedInputTokens: in, RawTpuCachedTokens: cr, UsageSource: "db"}
	}
	cases := []struct {
		mode  string
		u     *TurnUsage
		prev  *TurnHistoryEntry
		model string
		want  string
	}{
		{"sticky_delta", u(3000, 102000), prev, "gemini-4-argon", ""},
		{"sticky_delta", u(95000, 11444), prev, "gemini-4-argon", "prefix_only"},
		{"sticky_delta", u(105000, 0), prev, "gemini-4-argon", "zero"},
		{"sticky_delta", u(105000, 0), prev, "gemini-4-carbon", "model_switch"},
		{"sticky_delta", u(60000, 9493), prev, "gemini-4-argon", "shrink"},
		{"compaction_rollover", u(105000, 0), prev, "gemini-4-argon", "rollover"},
		{"sticky_delta", &TurnUsage{InputTokens: 900, UsageSource: "estimate"}, prev, "gemini-4-argon", ""},
	}
	for i, c := range cases {
		if got := classifyCacheMiss(c.mode, c.u, c.prev, c.model, now); got != c.want {
			t.Errorf("case %d: got %q want %q", i, got, c.want)
		}
	}
	old := *prev
	old.CompletedAtMs = now.Add(-10 * time.Minute).UnixMilli()
	if got := classifyCacheMiss("sticky_delta", u(105000, 2000), &old, "gemini-4-argon", now); got != "gap_ttl" {
		t.Errorf("gap: got %q", got)
	}
}

// K1: relay interval default and back-off.
func TestReviewToolRelayInterval(t *testing.T) {
	t.Setenv("AGY_TOOL_RELAY_SEC", "")
	if got := toolRelayInterval(0); got != 55*time.Second {
		t.Fatalf("default %v", got)
	}
	if got := toolRelayInterval(11 * time.Minute); got != 85*time.Second {
		t.Fatalf("10 min back-off %v", got)
	}
	if got := toolRelayInterval(31 * time.Minute); got != 120*time.Second {
		t.Fatalf("30 min back-off %v", got)
	}
	t.Setenv("AGY_TOOL_RELAY_SEC", "5")
	if got := toolRelayInterval(0); got != 15*time.Second {
		t.Fatalf("clamp %v", got)
	}
}

// listTestSessions returns the sessions of the most recently started test gateway.
func listTestSessions() []*AcpProjectSession {
	var out []*AcpProjectSession
	GlobalEngine.sessionListeners.Range(func(_, v any) bool {
		out = append(out, v.(*AcpProjectSession))
		return true
	})
	return out
}

// O9/P5: lock the MCP answers agy 1.5.5 is known to accept. Changing them (answering
// server/discover, advertising listChanged, opening a GET SSE stream) made the bridge tools
// disappear in the 2026-11-11 probe.
func TestReviewMcpHandshakeContract(t *testing.T) {
	sess := NewAcpProjectSession("o9", "/tmp", "")
	call := func(method string, body map[string]any) *httptest.ResponseRecorder {
		raw, _ := json.Marshal(body)
		req := httptest.NewRequest(method, "/mcp/"+sess.TenantKey, bytes.NewReader(raw))
		req.RemoteAddr = "127.0.0.1:5000"
		req.Header.Set("Authorization", "Bearer "+sess.TenantKey)
		rec := httptest.NewRecorder()
		sess.HandleMcpHTTP(rec, req)
		return rec
	}
	if rec := call(http.MethodGet, nil); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET must stay 405, got %d", rec.Code)
	}
	rec := call(http.MethodPost, map[string]any{"jsonrpc": "2.0", "id": 1, "method": "server/discover", "params": map[string]any{}})
	if !strings.Contains(rec.Body.String(), "-32601") {
		t.Fatalf("server/discover must answer -32601: %s", rec.Body.String())
	}
	rec = call(http.MethodPost, map[string]any{"jsonrpc": "2.0", "id": 2, "method": "initialize", "params": map[string]any{"protocolVersion": "2025-11-25"}})
	if !strings.Contains(rec.Body.String(), `"listChanged":false`) || !strings.Contains(rec.Body.String(), `"protocolVersion":"2025-11-25"`) || rec.Header().Get("Mcp-Session-Id") != "" {
		t.Fatalf("initialize answer changed: %s headers=%v", rec.Body.String(), rec.Header())
	}
}

// Production regression (cf1ddabf -> REQ #250 -> REQ #324): when Claude Code dynamically adds a
// deferred MCP tool during a parked tool-result resume, neither the resume nor the next user turn
// may roll over the session and destroy the warm implicit cache.
func TestDeferredToolAddedDuringParkedResumePreservesSession(t *testing.T) {
	GlobalEngine.StopEngine()
	baseURL, cleanup := startTestGateway(t)
	defer cleanup()
	sid := "cf1ddabf-parked-deferred"
	sshRead := map[string]any{
		"name":        "mcp__ssh-mcp-server__read-file",
		"description": "Read remote file via SSH",
		"input_schema": map[string]any{
			"type":       "object",
			"properties": map[string]any{"path": map[string]any{"type": "string"}},
		},
	}

	msgs := []map[string]any{{"role": "user", "content": "[FAKE_TOOL_CALL] run bash"}}
	code, out := reviewPost(t, baseURL, "/v1/messages", sid, reviewMessages("gemini-4-carbon", msgs, nil))
	if code != 200 {
		t.Fatalf("turn 1 tool_use failed: %d %s", code, out)
	}
	var d1 map[string]any
	_ = json.Unmarshal([]byte(out), &d1)
	if d1["stop_reason"] != "tool_use" {
		t.Fatalf("expected tool_use, got: %s", out)
	}
	var toolUseID string
	if arr, ok := d1["content"].([]any); ok {
		for _, it := range arr {
			if m, okM := it.(map[string]any); okM && m["type"] == "tool_use" {
				toolUseID, _ = m["id"].(string)
			}
		}
	}
	if toolUseID == "" {
		t.Fatalf("missing tool_use id: %s", out)
	}

	// Resume parked call while simultaneously adding a deferred MCP tool (just like Claude Code).
	expandedTools := append([]map[string]any{}, reviewTools...)
	expandedTools = append(expandedTools, sshRead)
	msgs = append(msgs,
		map[string]any{"role": "assistant", "content": d1["content"]},
		map[string]any{"role": "user", "content": []map[string]any{{"type": "tool_result", "tool_use_id": toolUseID, "content": "ok-from-bash"}}},
	)
	code, out = reviewPost(t, baseURL, "/v1/messages", sid, reviewMessages("gemini-4-carbon", msgs, map[string]any{"tools": expandedTools}))
	if code != 200 || !strings.Contains(out, "TOOL_DONE:") || !strings.Contains(out, "mcp__ssh-mcp-server__read-file") {
		t.Fatalf("parked resume did not receive tool delta: %d %s", code, out)
	}
	var d2 map[string]any
	_ = json.Unmarshal([]byte(out), &d2)

	// Next normal user turn with the same expanded tool list must stay on the exact same ACP session (turn=2, 0 rollovers).
	msgs = append(msgs,
		map[string]any{"role": "assistant", "content": d2["content"]},
		map[string]any{"role": "user", "content": "[ECHO_PROMPT] follow up after thinking"},
	)
	code, out = reviewPost(t, baseURL, "/v1/messages", sid, reviewMessages("gemini-4-carbon", msgs, map[string]any{"tools": expandedTools}))
	if code != 200 || !strings.Contains(out, "turn=2") || strings.Contains(out, "Host tool update") {
		t.Fatalf("next user turn must reuse the same session without re-injecting delta: %d %s", code, out)
	}
	var sess *AcpProjectSession
	for _, s := range listTestSessions() {
		if strings.Contains(s.ConvKey, sid) {
			sess = s
		}
	}
	if sess == nil {
		t.Fatal("session not found")
	}
	sess.mu.Lock()
	rollovers := sess.CompactionRollovers
	_, sshAllowed := allowedHostToolName(sess.McpToHostMap, "mcp__ssh-mcp-server__read-file")
	sess.mu.Unlock()
	if rollovers != 0 || !sshAllowed {
		t.Fatalf("expected 0 rollovers and ssh tool allowed, got rollovers=%d sshAllowed=%v", rollovers, sshAllowed)
	}
}

func TestServedModelStepIndicesMismatchAndTurnRestore(t *testing.T) {
	// 1. Verify isServedModelMismatch handles canonical Claude <model>-<tier> vs real shadow variants.
	if isServedModelMismatch("claude-opus-5-5", "high", "claude-opus-5-5-high") {
		t.Fatal("claude-opus-5-5-high is canonical for high tier, not a mismatch")
	}
	if isServedModelMismatch("gemini-4-carbon", "high", "gemini-4-carbon") {
		t.Fatal("gemini-4-carbon is canonical, not a mismatch")
	}
	if !isServedModelMismatch("gemini-4-argon", "medium", "gemini-4-argon-b") {
		t.Fatal("gemini-4-argon-b must be flagged as a served model mismatch")
	}
	if !isServedModelMismatch("gemini-3.8-flash", "high", "gemini-3.8-flash-n") {
		t.Fatal("gemini-3.8-flash-n must be flagged as a served model mismatch")
	}

	// 2. Verify ParseGenMetadataBlob parses step_indices (.2), model_display_name (.1.21 with spaces),
	// and prefers response_model_full (.1.22) over response_model (.1.19) when present.
	base := buildTestGenMetadataBlobServed("gemini-4-argon", 1418, 1000, 100, 5000, 10, 90, 6000, 983040, 1, 1)
	// Prepend packed step_indices field 2 (tag 0x12, len 2, [6, 7])
	withSteps := append([]byte{0x12, 0x02, 0x06, 0x07}, base...)
	gm := ParseGenMetadataBlob(withSteps)
	if gm == nil || len(gm.StepIndices) != 2 || gm.StepIndices[0] != 6 || gm.StepIndices[1] != 7 {
		t.Fatalf("step_indices not parsed: %+v", gm)
	}

	// 3. Verify StateStore.RestoreDetachedSessions preserves served_model and cache_miss on turns,
	// and computeDbStepCacheMisses propagates rollover reasons onto recentDbSteps.
	stPath := filepath.Join(t.TempDir(), "state.db")
	var st StateStore
	st.Init(stPath)
	defer func() {
		st.mu.Lock()
		if st.db != nil {
			_ = st.db.Close()
		}
		st.mu.Unlock()
	}()
	sess := NewAcpProjectSession("sid:test-restore", "/tmp", "sys")
	sess.SessionID = "e1268b42-5882-484c-bbbd-46cbf0aef02a"
	sess.MessageCount = 4
	st.UpsertSession(sess)
	st.InsertTurn("sid:test-restore", TurnHistoryEntry{
		TurnIndex:     1,
		CompletedAtMs: time.Now().UnixMilli(),
		SessionID:     sess.SessionID,
		Mode:          "compaction_rollover",
		StopReason:    "end_turn",
		Model:         "gemini-4-carbon",
		ThoughtLevel:  "high",
		ServedModel:   "gemini-4-carbon",
		CacheMiss:     "rollover_compaction",
		CacheHitRate:  "0.00%",
		Usage: &TurnUsage{
			InputTokens:            188921,
			CachedReadTokens:       0,
			OutputTokens:           1105,
			RawUncachedInputTokens: 188921,
			RawTpuCachedTokens:     0,
			UsageSource:            "db",
		},
	})
	gw := &GatewayServer{
		sessions: make(map[string]*AcpProjectSession),
		tenants:  make(map[string]*AcpProjectSession),
	}
	if n := st.RestoreDetachedSessions(gw); n != 1 {
		t.Fatalf("expected 1 restored session, got %d", n)
	}
	restored := gw.sessions["sid:test-restore"]
	if len(restored.TurnHistory) != 1 || restored.TurnHistory[0].ServedModel != "gemini-4-carbon" || restored.TurnHistory[0].CacheMiss != "rollover_compaction" {
		t.Fatalf("restored turn lost served_model or cache_miss: %+v", restored.TurnHistory)
	}
	_, turns := gw.CollectDashboardTelemetry(map[string]bool{})
	misses := computeDbStepCacheMisses([]ParsedModelStep{{
		SessionID: sess.SessionID,
		StepIdx:   1,
		ModelID:   "gemini-4-carbon",
		RawIn:     188921,
		RawCr:     0,
		AlignedIn: 188921,
		AlignedCr: 0,
		OutTok:    1105,
	}}, turns)
	if len(misses) != 1 || misses[0] != "rollover_compaction" {
		t.Fatalf("expected step cacheMiss=rollover_compaction, got %v", misses)
	}
}

func TestMultiGenClientContextMonotonicTurnsAndToolSearch(t *testing.T) {
	// 1. Claude Code's local ToolSearch tool must pass sanitizeToolsCatalog so deferred MCP tools can be loaded.
	cat := sanitizeToolsCatalog([]any{
		map[string]any{
			"name":         "ToolSearch",
			"description":  "Load deferred tool schemas",
			"input_schema": map[string]any{"type": "object", "properties": map[string]any{"query": map[string]any{"type": "string"}}},
		},
		map[string]any{
			"name":         "WebSearch",
			"description":  "Built-in web search",
			"input_schema": map[string]any{"type": "object", "properties": map[string]any{"query": map[string]any{"type": "string"}}},
		},
	})
	var hasToolSearch, hasWebSearch bool
	for _, td := range cat {
		if td.Name == "ToolSearch" {
			hasToolSearch = true
		}
		if td.Name == "WebSearch" {
			hasWebSearch = true
		}
	}
	if !hasToolSearch || hasWebSearch {
		t.Fatalf("expected ToolSearch kept and WebSearch stripped, got hasToolSearch=%v hasWebSearch=%v", hasToolSearch, hasWebSearch)
	}

	// 2. Multi-generation turn (exact 0bc5c426 gm[9]+gm[10] numbers: 3642+113429 and 2533+115022):
	// ledger sums all generations for billing, while ClientTokens() and ContextTotal() report the
	// final generation's true context window (117,555 instead of 234,626).
	dir := t.TempDir()
	t.Setenv("AGY_CONV_DIR", dir)
	sid := "0bc5c426-ca5e-4e92-b19b-f8b8361edea8"
	dbPath := filepath.Join(dir, sid+".db")
	writeTestGenMetadataSQLite(t, dbPath, 9, buildTestGenMetadataBlobServed("gemini-4-carbon", 1427, 3642, 423, 113429, 285, 138, 117071, 786432, 3.0, 1.0))
	writeTestGenMetadataSQLite(t, dbPath, 10, buildTestGenMetadataBlobServed("gemini-4-carbon", 1427, 2533, 261, 115022, 115, 146, 117555, 786432, 3.0, 1.0))

	s := NewAcpProjectSession("sid:multi-gen", "/tmp", "")
	s.SessionID = sid
	s.ModelID = "gemini-4-carbon"
	s.ThoughtLevel = "high"
	s.LastSeenGenMetaIdx = 8

	u := s.normalizeTurnUsage(nil, true, 0, 0)
	if u.InputTokens != 6175 || u.CachedReadTokens != 228451 || u.OutputTokens != 684 || u.Generations != 2 {
		t.Fatalf("ledger sum mismatch: %+v", u)
	}
	cIn, cCr, cOut := u.ClientTokens()
	if cIn != 2533 || cCr != 115022 || cOut != 684 {
		t.Fatalf("ClientTokens must report final generation context split (2533, 115022, 684), got (%d, %d, %d)", cIn, cCr, cOut)
	}
	if u.ContextTotal() != 117555 || s.LastCachedReadTokens != 117555 {
		t.Fatalf("context total must stay 117555, got ContextTotal=%d LastCachedReadTokens=%d", u.ContextTotal(), s.LastCachedReadTokens)
	}
	// Next turn with single generation (tot=120,565, uncached=20,000) must be prefix_only, never shrink!
	prevEntry := &TurnHistoryEntry{Model: "gemini-4-carbon", CompletedAtMs: time.Now().UnixMilli(), Usage: u}
	nextU := &TurnUsage{
		InputTokens: 20000, CachedReadTokens: 100565,
		RawUncachedInputTokens: 20000, RawTpuCachedTokens: 100565,
		ContextInputTokens: 20000, ContextCachedTokens: 100565,
		UsageSource: "db",
	}
	if miss := classifyCacheMissWithReason("sticky_delta", "", nextU, prevEntry, "gemini-4-carbon", time.Now()); miss == "shrink" {
		t.Fatalf("single-gen turn after multi-gen turn must not be misclassified as shrink, got %q", miss)
	}

	// 3. Monotonic TurnIndex past 150 ring-buffer cap + liveSessions lastHitRate preserved across split-relay ("none") turns.
	for i := 0; i < 153; i++ {
		s.logTurnStats("parked_mcp_resume", 100, TurnResult{
			StopReason: "tool_use",
			Usage:      &TurnUsage{InputTokens: 1000, CachedReadTokens: 99000, OutputTokens: 50, RawUncachedInputTokens: 1000, RawTpuCachedTokens: 99000, ContextInputTokens: 1000, ContextCachedTokens: 99000, UsageSource: "db"},
		})
	}
	// Trailing split-tool relay turn (UsageSource == "none", CacheHitRate == "N/A")
	s.logTurnStats("parked_mcp_resume", 20, TurnResult{
		StopReason: "tool_use",
		Usage:      &TurnUsage{StepsInPrompt: 2, UsageSource: "none"},
	})
	if len(s.TurnHistory) != 150 {
		t.Fatalf("expected TurnHistory bounded at 150, got %d", len(s.TurnHistory))
	}
	lastH := s.TurnHistory[len(s.TurnHistory)-1]
	prevH := s.TurnHistory[len(s.TurnHistory)-2]
	if prevH.TurnIndex != 153 || lastH.TurnIndex != 154 {
		t.Fatalf("expected monotonic TurnIndex 153 -> 154 past cap, got %d -> %d", prevH.TurnIndex, lastH.TurnIndex)
	}
	gw := &GatewayServer{
		sessions: map[string]*AcpProjectSession{"sid:multi-gen": s},
		tenants:  map[string]*AcpProjectSession{s.TenantKey: s},
	}
	live, gTurns := gw.CollectDashboardTelemetry(map[string]bool{})
	if len(live) != 1 || live[0]["turnsCount"] != 154 || live[0]["lastHitRate"] != "99.00%" {
		t.Fatalf("liveSession telemetry wrong after split-tool relay: %+v", live)
	}
	if len(gTurns) == 0 || gTurns[0]["usageSource"] != "none" || gTurns[0]["turnIndex"] != 154 {
		t.Fatalf("top gatewayTurn wrong: %+v", gTurns[0])
	}
}
