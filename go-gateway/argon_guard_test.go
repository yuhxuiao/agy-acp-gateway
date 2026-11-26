package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func protoVarint(v uint64) []byte {
	var out []byte
	for {
		b := byte(v & 0x7f)
		v >>= 7
		if v != 0 {
			b |= 0x80
		}
		out = append(out, b)
		if v == 0 {
			break
		}
	}
	return out
}

func protoTag(fn uint64, wt uint64) []byte {
	return protoVarint((fn << 3) | wt)
}

func protoBytes(fn uint64, payload []byte) []byte {
	var out []byte
	out = append(out, protoTag(fn, 2)...)
	out = append(out, protoVarint(uint64(len(payload)))...)
	out = append(out, payload...)
	return out
}

func protoUint(fn uint64, v uint64) []byte {
	var out []byte
	out = append(out, protoTag(fn, 0)...)
	out = append(out, protoVarint(v)...)
	return out
}

func protoDuration(fn uint64, sec int64, nanos int64) []byte {
	var sub []byte
	if sec > 0 {
		sub = append(sub, protoUint(1, uint64(sec))...)
	}
	if nanos > 0 {
		sub = append(sub, protoUint(2, uint64(nanos))...)
	}
	return protoBytes(fn, sub)
}

func buildTestGenMetadataBlob(modelEnum, inTok, outTok, crTok, thTok, textOutTok, cwmEst, cwmMax int, ttftSec, streamSec float64) []byte {
	return buildTestGenMetadataBlobServed("", modelEnum, inTok, outTok, crTok, thTok, textOutTok, cwmEst, cwmMax, ttftSec, streamSec)
}

// buildTestGenMetadataBlobServed also sets gen_metadata .1.19 (the upstream serving model name).
func buildTestGenMetadataBlobServed(served string, modelEnum, inTok, outTok, crTok, thTok, textOutTok, cwmEst, cwmMax int, ttftSec, streamSec float64) []byte {
	var stats []byte
	stats = append(stats, protoUint(1, uint64(modelEnum))...)
	stats = append(stats, protoUint(2, uint64(inTok))...)
	stats = append(stats, protoUint(3, uint64(outTok))...)
	stats = append(stats, protoUint(5, uint64(crTok))...)
	stats = append(stats, protoUint(9, uint64(thTok))...)
	stats = append(stats, protoUint(10, uint64(textOutTok))...)

	var cwm10 []byte
	if cwmEst > 0 {
		cwm10 = append(cwm10, protoUint(1, uint64(cwmEst))...)
	}
	if cwmMax > 0 {
		cwm10 = append(cwm10, protoUint(4, uint64(cwmMax))...)
	}
	cwm9 := protoBytes(10, cwm10)

	ttftWhole := int64(ttftSec)
	ttftNanos := int64((ttftSec - float64(ttftWhole)) * 1e9)
	streamWhole := int64(streamSec)
	streamNanos := int64((streamSec - float64(streamWhole)) * 1e9)

	var f1 []byte
	f1 = append(f1, protoUint(3, uint64(modelEnum))...)
	f1 = append(f1, protoBytes(4, stats)...)
	f1 = append(f1, protoBytes(9, cwm9)...)
	f1 = append(f1, protoDuration(11, ttftWhole, ttftNanos)...)
	f1 = append(f1, protoDuration(12, streamWhole, streamNanos)...)
	if served != "" {
		f1 = append(f1, protoBytes(19, []byte(served))...)
	}

	return protoBytes(1, f1)
}

func writeTestGenMetadataSQLite(t *testing.T, dbPath string, idx int, blob []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(dbPath), 0755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(dbPath), err)
	}
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)", filepath.ToSlash(dbPath))
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open sqlite %s: %v", dbPath, err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS gen_metadata (idx INTEGER PRIMARY KEY, data BLOB);`); err != nil {
		t.Fatalf("create gen_metadata: %v", err)
	}
	if _, err := db.Exec(`INSERT OR REPLACE INTO gen_metadata (idx, data) VALUES (?, ?)`, idx, blob); err != nil {
		t.Fatalf("insert gen_metadata: %v", err)
	}
}

func TestParseGenMetadataAndSubSecondDuration(t *testing.T) {
	blob := buildTestGenMetadataBlob(1417, 4200, 150, 32768, 30, 120, 165000, 983040, 2.25, 0.45)
	gm := ParseGenMetadataBlob(blob)
	if gm == nil {
		t.Fatalf("expected non-nil ParsedGenMetadata")
	}
	if gm.ModelEnum != 1417 || gm.ModelID != "gemini-4-argon" {
		t.Fatalf("unexpected model: enum=%d id=%s", gm.ModelEnum, gm.ModelID)
	}
	if gm.InputTokens != 4200 || gm.OutputTokens != 150 || gm.CachedReadTokens != 32768 || gm.ThoughtTokens != 30 || gm.TextOutTokens != 120 {
		t.Fatalf("unexpected token stats: %+v", gm)
	}
	if gm.CwmEstTokens != 165000 || gm.CwmMaxTokens != 983040 {
		t.Fatalf("unexpected CWM stats: est=%d max=%d", gm.CwmEstTokens, gm.CwmMaxTokens)
	}
	if gm.TtftSeconds < 2.24 || gm.TtftSeconds > 2.26 {
		t.Fatalf("expected ttftSeconds ~2.25, got %v", gm.TtftSeconds)
	}
	if gm.StreamSeconds < 0.44 || gm.StreamSeconds > 0.46 {
		t.Fatalf("expected sub-second streamSeconds ~0.45, got %v", gm.StreamSeconds)
	}
}

func TestEvaluateArgonDegradationServedModel(t *testing.T) {
	// 1. Primary gemini-4-argon with fast TPS, 2048-aligned cache, and 0 thinking MUST NOT trigger degradation
	gmPrimary := ParseGenMetadataBlob(buildTestGenMetadataBlobServed("gemini-4-argon", 1417, 18000, 300, 32768, 0, 300, 50768, 983040, 1.2, 0.40))
	repPrimary := EvaluateArgonDegradation(gmPrimary, "gemini-4-argon")
	if repPrimary.Degraded {
		t.Fatalf("primary gemini-4-argon must not be marked degraded: %+v", repPrimary)
	}

	// 2. Shadow-routed gemini-4-argon-b MUST deterministically trigger degradation even at normal 110 tok/s
	gmShadow := ParseGenMetadataBlob(buildTestGenMetadataBlobServed("gemini-4-argon-b", 1417, 3000, 220, 32768, 80, 140, 35768, 983040, 4.2, 1.3))
	repShadow := EvaluateArgonDegradation(gmShadow, "gemini-4-argon")
	if !repShadow.Degraded || repShadow.ServedModel != "gemini-4-argon-b" {
		t.Fatalf("expected gemini-4-argon-b to be marked degraded, got %+v", repShadow)
	}

	// 3. Empty ServedModel on legacy rows MUST NOT false-trigger degradation
	gmLegacy := ParseGenMetadataBlob(buildTestGenMetadataBlob(1418, 28000, 60, 0, 20, 40, 28000, 983040, 3.0, 1.0))
	if rep := EvaluateArgonDegradation(gmLegacy, "gemini-4-argon"); rep.Degraded {
		t.Fatalf("empty servedModel must not trigger degradation: %+v", rep)
	}

	// 4. Non-Argon models (e.g. Flash gemini-3.8-flash-tiered / gemini-3.8-flash-n) MUST NOT be marked degraded
	gmFlash := ParseGenMetadataBlob(buildTestGenMetadataBlobServed("gemini-3.8-flash-tiered", 1322, 4000, 120, 16000, 0, 120, 20000, 983040, 1.2, 0.38))
	repFlash := EvaluateArgonDegradation(gmFlash, "gemini-3.8-flash")
	if repFlash.Degraded {
		t.Fatalf("non-Argon model must not be marked degraded: %+v", repFlash)
	}
}

func TestInTurnArgonDegradationTransparentRecoveryAndGenMetaUsage(t *testing.T) {
	tmp := t.TempDir()
	convDir := filepath.Join(tmp, "pure-api-home", "antigravity-cli", "conversations")
	t.Cleanup(func() {
		GlobalEngine.StopEngine()
		initConfig()
	})
	t.Setenv("AGY_ACP_BIN", fakeAcpBin)
	t.Setenv("AGY_PURE_API_HOME", filepath.Join(tmp, "pure-api-home"))
	t.Setenv("AGY_CONV_DIR", convDir)
	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "gateway.db"))
	t.Setenv("AGY_GW_LOG", filepath.Join(tmp, "gateway.log"))
	t.Setenv("AGY_TOS_TRIPPED_FILE", filepath.Join(tmp, "tos-tripped"))
	initConfig()
	GlobalEngine.StopEngine()

	beforeDet := argonDegradationDetectedTotal.Load()
	beforeRec := argonDegradationRecoveredTotal.Load()

	// Session 1 (00000000-0000-4000-8000-000000000001) gets shadow-routed (.1.19 = gemini-4-argon-b)
	sid1 := "00000000-0000-4000-8000-000000000001"
	degradedBlob := buildTestGenMetadataBlobServed("gemini-4-argon-b", 1417, 127990, 80, 0, 20, 60, 168000, 983040, 3.2, 1.1)
	writeTestGenMetadataSQLite(t, filepath.Join(convDir, sid1+".db"), 0, degradedBlob)

	// Session 2 (00000000-0000-4000-8000-000000000002) gets primary-pool gemini-4-argon
	sid2 := "00000000-0000-4000-8000-000000000002"
	healthyBlob := buildTestGenMetadataBlobServed("gemini-4-argon", 1417, 3054, 140, 164946, 60, 80, 168000, 983040, 4.2, 1.6)
	writeTestGenMetadataSQLite(t, filepath.Join(convDir, sid2+".db"), 0, healthyBlob)

	gs := NewGatewayServer()
	defer gs.Close()
	msgs := []map[string]any{
		{"role": "user", "content": "Verify Argon transparent recovery"},
	}
	req := httptest.NewRequest("POST", "/v1/messages", nil)
	req.Header.Set("x-claude-code-session-id", "argon-recovery-sess")
	body := map[string]any{
		"model":  "gemini-4-argon-high",
		"system": "Primary working directory: " + tmp,
	}
	res, err := gs.HandleMessagesRequest(req, body, msgs, nil)
	if err != nil {
		t.Fatalf("HandleMessagesRequest failed: %v", err)
	}
	if res.SessionID != sid2 {
		t.Fatalf("expected transparent recovery onto session %s, got %s", sid2, res.SessionID)
	}
	if res.Mode != "argon_degraded_recovery" {
		t.Fatalf("expected mode=argon_degraded_recovery, got %s", res.Mode)
	}
	if res.Usage == nil || res.Usage.UsageSource != "db" || res.Usage.ThinkingTokens != 60 {
		t.Fatalf("expected exact gen_metadata db usage with ThinkingTokens=60, got %+v", res.Usage)
	}
	if argonDegradationDetectedTotal.Load() != beforeDet+1 {
		t.Fatalf("expected detected counter +1, got %d -> %d", beforeDet, argonDegradationDetectedTotal.Load())
	}
	if argonDegradationRecoveredTotal.Load() != beforeRec+1 {
		t.Fatalf("expected recovered counter +1, got %d -> %d", beforeRec, argonDegradationRecoveredTotal.Load())
	}

	// Verify Prometheus metrics include the new Argon degradation counters
	prom := RenderPrometheusMetrics(gs)
	if !strings.Contains(prom, "agy_gateway_argon_degradation_detected_total") || !strings.Contains(prom, "agy_gateway_argon_degradation_recovered_total") {
		t.Fatalf("expected Argon degradation metrics in /metrics output:\n%s", prom)
	}

	// Verify streaming turn with degradation (emittedDeltas > 0) does NOT fail mid-stream,
	// sets ForceRolloverNext, and rolls over cleanly on the next turn
	sid3 := "00000000-0000-4000-8000-000000000003"
	writeTestGenMetadataSQLite(t, filepath.Join(convDir, sid3+".db"), 0, degradedBlob)
	sid4 := "00000000-0000-4000-8000-000000000004"
	writeTestGenMetadataSQLite(t, filepath.Join(convDir, sid4+".db"), 0, healthyBlob)

	reqStream := httptest.NewRequest("POST", "/v1/messages", nil)
	reqStream.Header.Set("x-claude-code-session-id", "argon-stream-sess")
	streamChunks := 0
	resStream1, err := gs.HandleMessagesRequest(reqStream, body, msgs, func(c DeltaChunk) {
		streamChunks++
	})
	if err != nil {
		t.Fatalf("streaming degraded turn should not throw error: %v", err)
	}
	if streamChunks == 0 || resStream1.SessionID != sid3 {
		t.Fatalf("expected streamed chunks from %s, got chunks=%d sid=%s", sid3, streamChunks, resStream1.SessionID)
	}

	msgs2 := append(msgs,
		map[string]any{"role": "assistant", "content": resStream1.Text},
		map[string]any{"role": "user", "content": "Next turn after streaming degradation"},
	)
	resStream2, err := gs.HandleMessagesRequest(reqStream, body, msgs2, func(c DeltaChunk) {})
	if err != nil {
		t.Fatalf("second turn after streaming degradation failed: %v", err)
	}
	if resStream2.SessionID != sid4 {
		t.Fatalf("expected rollover onto fresh session %s on next turn, got %s", sid4, resStream2.SessionID)
	}
}

func TestMcpToolsListAlwaysLoadMetaAndRelaxedDescriptions(t *testing.T) {
	longToolDesc := strings.Repeat("A", 240)
	longPropDesc := strings.Repeat("B", 160)
	rawTools := []any{
		map[string]any{
			"name":        "CustomTool",
			"description": longToolDesc,
			"input_schema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"query": map[string]any{
						"type":        "string",
						"description": longPropDesc,
					},
				},
			},
		},
	}
	cat := sanitizeToolsCatalog(rawTools)
	if len(cat) == 0 {
		t.Fatalf("expected non-empty tools catalog")
	}
	if len(cat[0].Description) != 240 {
		t.Fatalf("expected 240-char tool description preserved (limit 300), got %d", len(cat[0].Description))
	}
	props, _ := cat[0].Parameters["properties"].(map[string]any)
	qProp, _ := props["query"].(map[string]any)
	if len(fmt.Sprint(qProp["description"])) != 160 {
		t.Fatalf("expected 160-char property description preserved (limit 200), got %d", len(fmt.Sprint(qProp["description"])))
	}

	sess := NewAcpProjectSession("test-mcp-meta", "/tmp", "")
	sess.ToolsCatalog = cat
	reqBody, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "tools/list",
	})
	req := httptest.NewRequest(http.MethodPost, "/mcp/"+sess.TenantKey, bytes.NewReader(reqBody))
	req.RemoteAddr = "127.0.0.1:54321"
	req.Header.Set("Authorization", "Bearer "+sess.TenantKey)
	rec := httptest.NewRecorder()
	sess.HandleMcpHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 from tools/list, got %d", rec.Code)
	}
	var resp struct {
		Result struct {
			Tools []struct {
				Name  string         `json:"name"`
				Meta  map[string]any `json:"_meta"`
				Input map[string]any `json:"inputSchema"`
			} `json:"tools"`
		} `json:"result"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal tools/list response: %v", err)
	}
	if len(resp.Result.Tools) == 0 {
		t.Fatalf("expected tools in tools/list response")
	}
	for _, tl := range resp.Result.Tools {
		if tl.Meta["antigravity/alwaysLoad"] != true {
			t.Fatalf("expected _meta.antigravity/alwaysLoad=true on tool %s, got %+v", tl.Name, tl.Meta)
		}
	}
}

func buildTestPlannerStepPayload(toolCalls [][3]string) []byte {
	var planner []byte
	for _, tc := range toolCalls {
		var sub []byte
		sub = append(sub, protoBytes(1, []byte(tc[0]))...)
		sub = append(sub, protoBytes(2, []byte(tc[1]))...)
		sub = append(sub, protoBytes(3, []byte(tc[2]))...)
		planner = append(planner, protoBytes(7, sub)...)
	}
	var step []byte
	step = append(step, protoUint(1, 15)...)
	step = append(step, protoUint(4, 3)...)
	step = append(step, protoBytes(20, planner)...)
	return step
}

func writeTestPlannerStepSQLite(t *testing.T, dbPath string, idx int, stepPayload []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(dbPath), 0755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(dbPath), err)
	}
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)", filepath.ToSlash(dbPath))
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open sqlite %s: %v", dbPath, err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS steps (idx INTEGER PRIMARY KEY, step_type INTEGER, status INTEGER, metadata BLOB, step_payload BLOB);`); err != nil {
		t.Fatalf("create steps: %v", err)
	}
	if _, err := db.Exec(`INSERT OR REPLACE INTO steps (idx, step_type, status, metadata, step_payload) VALUES (?, 15, 3, ?, ?)`, idx, []byte{}, stepPayload); err != nil {
		t.Fatalf("insert step: %v", err)
	}
}

func TestPlannerResponseSequentialMcpSiblingBatchingEliminatesZeroUsageTurns(t *testing.T) {
	tmp := t.TempDir()
	convDir := filepath.Join(tmp, "conversations")
	t.Setenv("AGY_CONV_DIR", convDir)

	sid := "00000000-0000-4000-8000-000000000099"
	dbPath := filepath.Join(convDir, sid+".db")

	rawTools := []any{
		map[string]any{
			"name":        "Read",
			"description": "Read file",
			"input_schema": map[string]any{
				"type":       "object",
				"properties": map[string]any{"file_path": map[string]any{"type": "string"}},
			},
		},
		map[string]any{
			"name":        "Write",
			"description": "Write file",
			"input_schema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"file_path": map[string]any{"type": "string"},
					"content":   map[string]any{"type": "string"},
				},
			},
		},
	}
	cat := sanitizeToolsCatalog(rawTools)
	sess := NewAcpProjectSession("test-sibling-batch", tmp, "")
	sess.SessionID = sid
	sess.ModelID = "gemini-4-carbon"
	sess.InitialToolsCatalog = cat
	sess.ToolsCatalog = cat
	sess.DeclaredMcpTools = map[string]bool{"Read": true}
	for _, td := range cat {
		sess.McpToHostMap[td.McpName] = td.Name
	}

	readMcp := "mcp_ws-bridge_" + mcpToolName("Read")
	callHelperMcp := "mcp_ws-bridge_" + mcpToolName(CallToolName)

	// Generation 0 proposes 1 built-in search_web + 3 external host tools (2 direct Read + 1 deferred Write via dsh_call_tool).
	writeTestGenMetadataSQLite(t, dbPath, 0, buildTestGenMetadataBlobServed("gemini-4-carbon", 1427, 4200, 180, 32768, 40, 140, 36968, 983040, 2.0, 0.8))
	writeTestPlannerStepSQLite(t, dbPath, 1, buildTestPlannerStepPayload([][3]string{
		{"call_builtin_1", "search_web", `{"query":"golang"}`},
		{"call_ext_1", readMcp, `{"file_path":"/a.txt","toolSummary":"Read a"}`},
		{"call_ext_2", readMcp, `{"file_path":"/b.txt","toolSummary":"Read b"}`},
		{"call_ext_3", callHelperMcp, `{"name":"Write","arguments":{"file_path":"/c.txt","content":"hello","toolSummary":"Write c"}}`},
	}))

	turn1 := newActiveTurnState(nil, func() {})
	sess.mu.Lock()
	sess.PromptInFlight = true
	sess.activeTurn = turn1
	sess.mu.Unlock()

	// Simulate oneharness.(*Machine).stepToolDispatch: strictly sequential MCP tools/call where
	// call #2 is NOT sent until call #1 returns, and call #3 is NOT sent until call #2 returns.
	stepDispatchErr := make(chan error, 1)
	go func() {
		r1 := sess.executeMcpToolCall(context.Background(), "Read", map[string]any{"file_path": "/a.txt"})
		if !strings.Contains(fmt.Sprint(r1["content"]), "RES_A") {
			stepDispatchErr <- fmt.Errorf("unexpected r1: %+v", r1)
			return
		}
		r2 := sess.executeMcpToolCall(context.Background(), "Read", map[string]any{"file_path": "/b.txt"})
		if !strings.Contains(fmt.Sprint(r2["content"]), "RES_B") {
			stepDispatchErr <- fmt.Errorf("unexpected r2: %+v", r2)
			return
		}
		r3 := sess.executeMcpToolCall(context.Background(), CallToolName, map[string]any{
			"name":      "Write",
			"arguments": map[string]any{"file_path": "/c.txt", "content": "hello"},
		})
		if !strings.Contains(fmt.Sprint(r3["content"]), "RES_C") {
			stepDispatchErr <- fmt.Errorf("unexpected r3: %+v", r3)
			return
		}

		// After all 3 tool steps complete in oneharness, stepGenerate runs generation 1 (final end_turn).
		writeTestGenMetadataSQLite(t, dbPath, 1, buildTestGenMetadataBlobServed("gemini-4-carbon", 1427, 1800, 95, 36968, 25, 70, 38768, 983040, 1.5, 0.5))
		writeTestPlannerStepSQLite(t, dbPath, 5, buildTestPlannerStepPayload(nil))

		sess.mu.Lock()
		sess.PromptInFlight = false
		sess.PendingSiblingCalls = nil
		cur := sess.activeTurn
		sess.activeTurn = nil
		sess.mu.Unlock()

		usage2 := sess.normalizeTurnUsageAsync(nil, false, 0, 12)
		cur.mu.Lock()
		cur.resolved = true
		cur.mu.Unlock()
		cur.closeDelta()
		cur.resolveChan <- TurnResult{
			StopReason: "end_turn",
			Text:       "All done.",
			Usage:      usage2,
			TtftMs:     50,
		}
		stepDispatchErr <- nil
	}()

	var res1 TurnResult
	select {
	case res1 = <-turn1.resolveChan:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for batched tool_use turn")
	}

	if res1.StopReason != "tool_use" || len(res1.ToolCalls) != 3 {
		t.Fatalf("expected all 3 external MCP tool calls batched in HTTP turn 1, got %d: %+v", len(res1.ToolCalls), res1.ToolCalls)
	}
	if res1.ToolCalls[0].Name != "Read" || res1.ToolCalls[0].Input["file_path"] != "/a.txt" || res1.ToolCalls[0].Input["toolSummary"] != nil {
		t.Fatalf("unexpected tool[0]: %+v", res1.ToolCalls[0])
	}
	if res1.ToolCalls[1].Name != "Read" || res1.ToolCalls[1].Input["file_path"] != "/b.txt" || res1.ToolCalls[1].Input["toolSummary"] != nil {
		t.Fatalf("unexpected tool[1]: %+v", res1.ToolCalls[1])
	}
	if res1.ToolCalls[2].Name != "Write" || res1.ToolCalls[2].Input["file_path"] != "/c.txt" || res1.ToolCalls[2].Input["toolSummary"] != nil {
		t.Fatalf("unexpected tool[2]: %+v", res1.ToolCalls[2])
	}
	in1, cr1, out1 := res1.Usage.ClientTokens()
	if in1 != 4200 || cr1 != 32768 || out1 != 180 {
		t.Fatalf("expected turn 1 ClientTokens=(4200,32768,180), got (%d,%d,%d)", in1, cr1, out1)
	}

	// Client executes all 3 tools in parallel and sends all 3 tool_results in HTTP turn 2.
	matched := []ToolResultInput{
		{ToolCallID: res1.ToolCalls[0].ID, Content: "RES_A"},
		{ToolCallID: res1.ToolCalls[1].ID, Content: "RES_B"},
		{ToolCallID: res1.ToolCalls[2].ID, Content: "RES_C"},
	}
	res2, err := sess.resumeParkedTurn(context.Background(), matched, []map[string]any{{"role": "user", "content": "hi"}}, nil)
	if err != nil {
		t.Fatalf("resumeParkedTurn failed: %v", err)
	}
	if err := <-stepDispatchErr; err != nil {
		t.Fatalf("sequential MCP dispatch goroutine failed: %v", err)
	}
	if res2.StopReason != "end_turn" || res2.Text != "All done." {
		t.Fatalf("expected turn 2 to complete with end_turn (no split 0/0 tool_use turns), got %+v", res2)
	}
	in2, cr2, out2 := res2.Usage.ClientTokens()
	if in2 != 1800 || cr2 != 36968 || out2 != 95 || res2.Usage.UsageSource != "db" {
		t.Fatalf("expected turn 2 ClientTokens=(1800,36968,95) source=db, got (%d,%d,%d) usage=%+v", in2, cr2, out2, res2.Usage)
	}
	sess.mu.Lock()
	leftParked := len(sess.ParkedCalls)
	leftSibs := len(sess.PendingSiblingCalls)
	sess.mu.Unlock()
	if leftParked != 0 || leftSibs != 0 {
		t.Fatalf("expected clean parked/sibling state after completion, got parked=%d sibs=%d", leftParked, leftSibs)
	}
}

func TestComputeStepTimingAndWeightedAggregateTps(t *testing.T) {
	// 1. Genuinely streamed step (matches Carbon production gm#101: 5068 out, 11.925s ttft, 44.601s stream)
	ttft1, strm1, elap1, e2e1, dec1 := computeStepTiming(11925, 44601, 56526, 5068)
	if ttft1 != 11925 || strm1 != 44601 || elap1 != 56526 {
		t.Fatalf("unexpected normalized durations: ttft=%d stream=%d elapsed=%d", ttft1, strm1, elap1)
	}
	if e2e1 != 89.7 {
		t.Fatalf("expected e2eTps=89.7, got %v", e2e1)
	}
	if dec1 != 113.6 {
		t.Fatalf("expected decodeTps=113.6, got %v", dec1)
	}

	// 2. Server-buffered tool-call burst (matches gm#4: 3070 out, 31.225s ttft, 0.311s stream -> 9871/s raw burst)
	_, _, _, e2e2, dec2 := computeStepTiming(31225, 311, 31536, 3070)
	if e2e2 != 97.3 {
		t.Fatalf("expected buffered step e2eTps=97.3, got %v", e2e2)
	}
	if dec2 != 0 {
		t.Fatalf("expected buffered burst decodeTps=0 (not ~9871/s), got %v", dec2)
	}

	// 3. Window & per-model AggregateSteps must use time-weighted throughput sum(out)/sum(sec),
	// not unweighted mean of per-step ratios (where short tool calls drag 100 tok/s down to ~15 tok/s).
	steps := []ParsedModelStep{
		{
			SessionID: "s1", StepIdx: 1, ModelID: "gemini-4-carbon", ModelLabel: "Gemini 4 Carbon",
			ModelFamily: "gemini", QuotaGroup: "gemini",
			TtftMs: 12000, StreamMs: 1000, ElapsedMs: 13000, OutTok: 120, Tps: 9.2, DecodeTps: 120.0,
		},
		{
			SessionID: "s1", StepIdx: 2, ModelID: "gemini-4-carbon", ModelLabel: "Gemini 4 Carbon",
			ModelFamily: "gemini", QuotaGroup: "gemini",
			TtftMs: 12061, StreamMs: 52659, ElapsedMs: 64720, OutTok: 7317, Tps: 113.1, DecodeTps: 139.0,
		},
		// Interactive >10m host pause must be excluded symmetrically from both AvgTps and AvgDecodeTps.
		{
			SessionID: "s1", StepIdx: 3, ModelID: "gemini-4-carbon", ModelLabel: "Gemini 4 Carbon",
			ModelFamily: "gemini", QuotaGroup: "gemini",
			TtftMs: 75135, StreamMs: 1243924, ElapsedMs: 1319059, OutTok: 3502, Tps: 2.7, DecodeTps: 2.8,
		},
	}
	agg := AggregateSteps(steps)
	// True time-weighted E2E TPS = (120 + 7317) / ((13000 + 64720)/1000) = 7437 / 77.72 = 95.7 tok/s
	// (Old unweighted mean gave (9.2 + 113.1) / 2 = 61.2 tok/s!)
	if agg.AvgTps != 95.7 {
		t.Fatalf("expected time-weighted AvgTps=95.7, got %v", agg.AvgTps)
	}
	// True time-weighted Decode TPS = (120 + 7317) / ((1000 + 52659)/1000) = 7437 / 53.659 = 138.6 tok/s
	if agg.AvgDecodeTps != 138.6 {
		t.Fatalf("expected time-weighted AvgDecodeTps=138.6, got %v", agg.AvgDecodeTps)
	}
	if len(agg.Models) != 1 || agg.Models[0].AvgTps != 95.7 || agg.Models[0].AvgDecodeTps != 138.6 {
		t.Fatalf("expected model AvgTps=95.7 AvgDecodeTps=138.6, got %+v", agg.Models)
	}
}
