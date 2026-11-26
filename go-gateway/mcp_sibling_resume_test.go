package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func newTestSiblingSession(t *testing.T, convKey, sid string) (*AcpProjectSession, string, string) {
	t.Helper()
	tmp := t.TempDir()
	convDir := filepath.Join(tmp, "conversations")
	t.Setenv("AGY_CONV_DIR", convDir)
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
		map[string]any{
			"name":        "Ping",
			"description": "No-arg ping tool",
			"input_schema": map[string]any{
				"type":       "object",
				"properties": map[string]any{},
			},
		},
	}
	cat := sanitizeToolsCatalog(rawTools)
	sess := NewAcpProjectSession(convKey, tmp, "")
	sess.SessionID = sid
	sess.ModelID = "gemini-4-carbon"
	sess.InitialToolsCatalog = cat
	sess.ToolsCatalog = cat
	sess.DeclaredMcpTools = map[string]bool{"Read": true, "Write": true, "Ping": true}
	for _, td := range cat {
		sess.McpToHostMap[td.McpName] = td.Name
	}

	logPath := filepath.Join(tmp, "sibling-events.log")
	logMu.Lock()
	oldTmpLog := TmpLogFile
	TmpLogFile = logPath
	logMu.Unlock()
	t.Cleanup(func() {
		logMu.Lock()
		TmpLogFile = oldTmpLog
		logMu.Unlock()
	})

	return sess, dbPath, logPath
}

func readSiblingBreakEvents(t *testing.T, logPath string) []map[string]any {
	t.Helper()
	raw, err := os.ReadFile(logPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatalf("read log %s: %v", logPath, err)
	}
	var events []map[string]any
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.Contains(line, "[MCP_TOOL_SIBLING_BREAK]") {
			continue
		}
		idx := strings.Index(line, "{")
		if idx < 0 {
			continue
		}
		var meta map[string]any
		if err := json.Unmarshal([]byte(line[idx:]), &meta); err != nil {
			t.Fatalf("unmarshal MCP_TOOL_SIBLING_BREAK meta from %q: %v", line, err)
		}
		events = append(events, meta)
	}
	return events
}

func finishActiveTurnEnd(sess *AcpProjectSession, text string) {
	sess.mu.Lock()
	sess.PromptInFlight = false
	sess.PendingSiblingCalls = nil
	cur := sess.activeTurn
	sess.activeTurn = nil
	sess.mu.Unlock()

	usage := sess.normalizeTurnUsageAsync(nil, false, 0, len(text))
	cur.mu.Lock()
	cur.resolved = true
	cur.mu.Unlock()
	cur.closeDelta()
	cur.resolveChan <- TurnResult{
		StopReason: "end_turn",
		Text:       text,
		Usage:      usage,
		TtftMs:     50,
	}
}

func TestMcpSiblingBatchAllowsNullArgsJSON(t *testing.T) {
	sid := "00000000-0000-4000-8000-000000000401"
	sess, dbPath, logPath := newTestSiblingSession(t, "test-sibling-null-args", sid)

	readMcp := "mcp_ws-bridge_" + mcpToolName("Read")
	pingMcp := "mcp_ws-bridge_" + mcpToolName("Ping")

	proposals := make([][3]string, 0, 16)
	for i := 0; i < 16; i++ {
		if i == 9 {
			// Tool #10 (0-based idx 9) carries ArgsJSON == "null" as emitted by agy for no-arg calls.
			proposals = append(proposals, [3]string{fmt.Sprintf("call_%d", i), pingMcp, "null"})
			continue
		}
		proposals = append(proposals, [3]string{
			fmt.Sprintf("call_%d", i),
			readMcp,
			fmt.Sprintf(`{"file_path":"/f%d.txt"}`, i),
		})
	}
	writeTestGenMetadataSQLite(t, dbPath, 0, buildTestGenMetadataBlobServed("gemini-4-carbon", 1427, 4200, 180, 32768, 40, 140, 36968, 983040, 2.0, 0.8))
	writeTestPlannerStepSQLite(t, dbPath, 1, buildTestPlannerStepPayload(proposals))

	turn1 := newActiveTurnState(nil, func() {})
	sess.mu.Lock()
	sess.PromptInFlight = true
	sess.activeTurn = turn1
	sess.mu.Unlock()

	dispatchErr := make(chan error, 1)
	go func() {
		for i := 0; i < 16; i++ {
			var res map[string]any
			if i == 9 {
				// HandleMcpHTTP normalizes nil/null arguments to an empty map.
				res = sess.executeMcpToolCall(context.Background(), "Ping", map[string]any{})
			} else {
				res = sess.executeMcpToolCall(context.Background(), "Read", map[string]any{"file_path": fmt.Sprintf("/f%d.txt", i)})
			}
			want := fmt.Sprintf("RES_%d", i)
			if !strings.Contains(fmt.Sprint(res["content"]), want) {
				dispatchErr <- fmt.Errorf("call %d expected %s, got %+v", i, want, res)
				return
			}
		}
		finishActiveTurnEnd(sess, "All 16 done.")
		dispatchErr <- nil
	}()

	var res1 TurnResult
	select {
	case res1 = <-turn1.resolveChan:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for 16-tool batched turn")
	}

	if res1.StopReason != "tool_use" || len(res1.ToolCalls) != 16 {
		t.Fatalf("expected all 16 tools batched into 1 HTTP turn (no break at null tool #10), got stop=%s count=%d", res1.StopReason, len(res1.ToolCalls))
	}
	if res1.ToolCalls[9].Name != "Ping" || res1.ToolCalls[9].Input == nil || len(res1.ToolCalls[9].Input) != 0 {
		t.Fatalf("expected tool #10 to be Ping with empty non-nil input map, got %+v", res1.ToolCalls[9])
	}
	for i := 0; i < 16; i++ {
		if i == 9 {
			continue
		}
		wantPath := fmt.Sprintf("/f%d.txt", i)
		if res1.ToolCalls[i].Name != "Read" || res1.ToolCalls[i].Input["file_path"] != wantPath {
			t.Fatalf("tool[%d] mismatch: got %+v", i, res1.ToolCalls[i])
		}
	}

	sess.mu.Lock()
	batchIdx, nextPos, total := sess.LastPlannerBatchIdx, sess.LastPlannerBatchNextPos, sess.LastPlannerBatchTotal
	sess.mu.Unlock()
	if batchIdx != 1 || nextPos != 16 || total != 16 {
		t.Fatalf("expected cursor (idx=1, next=16, total=16), got (%d, %d, %d)", batchIdx, nextPos, total)
	}
	if breaks := readSiblingBreakEvents(t, logPath); len(breaks) != 0 {
		t.Fatalf("expected no MCP_TOOL_SIBLING_BREAK events, got %+v", breaks)
	}

	matched := make([]ToolResultInput, 0, 16)
	for i, tc := range res1.ToolCalls {
		matched = append(matched, ToolResultInput{ToolCallID: tc.ID, Content: fmt.Sprintf("RES_%d", i)})
	}
	res2, err := sess.resumeParkedTurn(context.Background(), matched, []map[string]any{{"role": "user", "content": "hi"}}, nil)
	if err != nil {
		t.Fatalf("resumeParkedTurn failed: %v", err)
	}
	if err := <-dispatchErr; err != nil {
		t.Fatalf("sequential dispatch failed: %v", err)
	}
	if res2.StopReason != "end_turn" || res2.Text != "All 16 done." {
		t.Fatalf("expected clean end_turn after 16 tools, got %+v", res2)
	}
}

func TestMcpSiblingResumeAfterAwaitBreak(t *testing.T) {
	sid := "00000000-0000-4000-8000-000000000402"
	sess, dbPath, logPath := newTestSiblingSession(t, "test-sibling-await-resume", sid)

	readMcp := "mcp_ws-bridge_" + mcpToolName("Read")
	awaitMcp := "mcp_ws-bridge_" + mcpToolName(AwaitToolName)

	// 7 resolved MCP entries: [0, 1, 2, ws_await_result(3), 4, 5, 6]
	writeTestGenMetadataSQLite(t, dbPath, 0, buildTestGenMetadataBlobServed("gemini-4-carbon", 1427, 4200, 180, 32768, 40, 140, 36968, 983040, 2.0, 0.8))
	writeTestPlannerStepSQLite(t, dbPath, 1, buildTestPlannerStepPayload([][3]string{
		{"c0", readMcp, `{"file_path":"/a0.txt"}`},
		{"c1", readMcp, `{"file_path":"/a1.txt"}`},
		{"c2", readMcp, `{"file_path":"/a2.txt"}`},
		{"c3", awaitMcp, `{"call_id":"acp_not_found"}`},
		{"c4", readMcp, `{"file_path":"/a4.txt"}`},
		{"c5", readMcp, `{"file_path":"/a5.txt"}`},
		{"c6", readMcp, `{"file_path":"/a6.txt"}`},
	}))

	turn1 := newActiveTurnState(nil, func() {})
	sess.mu.Lock()
	sess.PromptInFlight = true
	sess.activeTurn = turn1
	sess.mu.Unlock()

	dispatchErr := make(chan error, 1)
	go func() {
		for _, i := range []int{0, 1, 2} {
			res := sess.executeMcpToolCall(context.Background(), "Read", map[string]any{"file_path": fmt.Sprintf("/a%d.txt", i)})
			want := fmt.Sprintf("RES_%d", i)
			if !strings.Contains(fmt.Sprint(res["content"]), want) {
				dispatchErr <- fmt.Errorf("pos %d expected %s, got %+v", i, want, res)
				return
			}
		}
		// Pos 3: ws_await_result handled locally by executeMcpToolCall (returns immediately).
		_ = sess.executeMcpToolCall(context.Background(), AwaitToolName, map[string]any{"call_id": "acp_not_found"})

		// Pos 4, 5, 6 still belong to the same PlannerResponse step idx=1.
		for _, i := range []int{4, 5, 6} {
			res := sess.executeMcpToolCall(context.Background(), "Read", map[string]any{"file_path": fmt.Sprintf("/a%d.txt", i)})
			want := fmt.Sprintf("RES_%d", i)
			if !strings.Contains(fmt.Sprint(res["content"]), want) {
				dispatchErr <- fmt.Errorf("pos %d expected %s, got %+v", i, want, res)
				return
			}
		}
		finishActiveTurnEnd(sess, "Resumed tail complete.")
		dispatchErr <- nil
	}()

	var res1 TurnResult
	select {
	case res1 = <-turn1.resolveChan:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for turn 1 batch")
	}
	if res1.StopReason != "tool_use" || len(res1.ToolCalls) != 3 {
		t.Fatalf("expected turn 1 to batch [0,1,2] (3 tools), got stop=%s count=%d", res1.StopReason, len(res1.ToolCalls))
	}
	for i := 0; i < 3; i++ {
		if res1.ToolCalls[i].Input["file_path"] != fmt.Sprintf("/a%d.txt", i) {
			t.Fatalf("turn 1 tool[%d] mismatch: %+v", i, res1.ToolCalls[i])
		}
	}

	sess.mu.Lock()
	idx1, next1, total1 := sess.LastPlannerBatchIdx, sess.LastPlannerBatchNextPos, sess.LastPlannerBatchTotal
	sess.mu.Unlock()
	if idx1 != 1 || next1 != 3 || total1 != 7 {
		t.Fatalf("after break expected cursor (idx=1, next=3, total=7), got (%d, %d, %d)", idx1, next1, total1)
	}

	breaks := readSiblingBreakEvents(t, logPath)
	if len(breaks) != 1 {
		t.Fatalf("expected 1 MCP_TOOL_SIBLING_BREAK event, got %d: %+v", len(breaks), breaks)
	}
	if breaks[0]["reason"] != "await_helper" ||
		breaks[0]["breakIdx"] != float64(3) ||
		breaks[0]["curPos"] != float64(0) ||
		breaks[0]["totalResolved"] != float64(7) ||
		breaks[0]["batchedCount"] != float64(3) ||
		breaks[0]["rawToolName"] != awaitMcp ||
		breaks[0]["resumedStep"] != false {
		t.Fatalf("unexpected MCP_TOOL_SIBLING_BREAK payload: %+v", breaks[0])
	}

	// Return results for [0, 1, 2]; pos 3 runs locally and pos 4 triggers cursor resume for [4, 5, 6] in ONE turn!
	res2, err := sess.resumeParkedTurn(context.Background(), []ToolResultInput{
		{ToolCallID: res1.ToolCalls[0].ID, Content: "RES_0"},
		{ToolCallID: res1.ToolCalls[1].ID, Content: "RES_1"},
		{ToolCallID: res1.ToolCalls[2].ID, Content: "RES_2"},
	}, []map[string]any{{"role": "user", "content": "hi"}}, nil)
	if err != nil {
		t.Fatalf("resumeParkedTurn turn 2 failed: %v", err)
	}
	if res2.StopReason != "tool_use" || len(res2.ToolCalls) != 3 {
		t.Fatalf("expected resumed turn 2 to batch [4,5,6] (3 tools) in ONE turn, got stop=%s count=%d: %+v", res2.StopReason, len(res2.ToolCalls), res2.ToolCalls)
	}
	for i, pos := range []int{4, 5, 6} {
		if res2.ToolCalls[i].Input["file_path"] != fmt.Sprintf("/a%d.txt", pos) {
			t.Fatalf("turn 2 tool[%d] mismatch: want /a%d.txt, got %+v", i, pos, res2.ToolCalls[i])
		}
	}

	sess.mu.Lock()
	idx2, next2, total2 := sess.LastPlannerBatchIdx, sess.LastPlannerBatchNextPos, sess.LastPlannerBatchTotal
	sess.mu.Unlock()
	if idx2 != 1 || next2 != 7 || total2 != 7 {
		t.Fatalf("after resume expected cursor (idx=1, next=7, total=7), got (%d, %d, %d)", idx2, next2, total2)
	}
	if breaksAfter := readSiblingBreakEvents(t, logPath); len(breaksAfter) != 1 {
		t.Fatalf("expected no additional MCP_TOOL_SIBLING_BREAK on clean resume, got %+v", breaksAfter)
	}

	res3, err := sess.resumeParkedTurn(context.Background(), []ToolResultInput{
		{ToolCallID: res2.ToolCalls[0].ID, Content: "RES_4"},
		{ToolCallID: res2.ToolCalls[1].ID, Content: "RES_5"},
		{ToolCallID: res2.ToolCalls[2].ID, Content: "RES_6"},
	}, []map[string]any{{"role": "user", "content": "hi"}}, nil)
	if err != nil {
		t.Fatalf("resumeParkedTurn turn 3 failed: %v", err)
	}
	if err := <-dispatchErr; err != nil {
		t.Fatalf("sequential dispatch failed: %v", err)
	}
	if res3.StopReason != "end_turn" || res3.Text != "Resumed tail complete." {
		t.Fatalf("expected end_turn on turn 3, got %+v", res3)
	}
}

func TestMcpSiblingResumeAfterInvalidJSONAndLoopBreaker(t *testing.T) {
	t.Run("invalid_args_json", func(t *testing.T) {
		sid := "00000000-0000-4000-8000-000000000403"
		sess, dbPath, logPath := newTestSiblingSession(t, "test-sibling-badjson-resume", sid)
		readMcp := "mcp_ws-bridge_" + mcpToolName("Read")

		writeTestPlannerStepSQLite(t, dbPath, 1, buildTestPlannerStepPayload([][3]string{
			{"c0", readMcp, `{"file_path":"/j0.txt"}`},
			{"c1", readMcp, `{"file_path":"/j1.txt"}`},
			{"c2", readMcp, `{"file_path":"/j2.txt"}`},
			{"c3", readMcp, `{not valid json`},
			{"c4", readMcp, `{"file_path":"/j4.txt"}`},
			{"c5", readMcp, `{"file_path":"/j5.txt"}`},
			{"c6", readMcp, `{"file_path":"/j6.txt"}`},
		}))

		turn1 := newActiveTurnState(nil, func() {})
		sess.mu.Lock()
		sess.PromptInFlight = true
		sess.activeTurn = turn1
		sess.mu.Unlock()

		dispatchErr := make(chan error, 1)
		go func() {
			for _, i := range []int{0, 1, 2} {
				res := sess.executeMcpToolCall(context.Background(), "Read", map[string]any{"file_path": fmt.Sprintf("/j%d.txt", i)})
				if !strings.Contains(fmt.Sprint(res["content"]), fmt.Sprintf("J_%d", i)) {
					dispatchErr <- fmt.Errorf("pos %d bad result: %+v", i, res)
					return
				}
			}
			// Pos 3 had invalid JSON and was skipped/failed upstream; pos 4 arrives on same step 1.
			for _, i := range []int{4, 5, 6} {
				res := sess.executeMcpToolCall(context.Background(), "Read", map[string]any{"file_path": fmt.Sprintf("/j%d.txt", i)})
				if !strings.Contains(fmt.Sprint(res["content"]), fmt.Sprintf("J_%d", i)) {
					dispatchErr <- fmt.Errorf("pos %d bad result: %+v", i, res)
					return
				}
			}
			finishActiveTurnEnd(sess, "Bad JSON resumed.")
			dispatchErr <- nil
		}()

		var res1 TurnResult
		select {
		case res1 = <-turn1.resolveChan:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for turn 1")
		}
		if len(res1.ToolCalls) != 3 {
			t.Fatalf("expected 3 batched tools before invalid JSON, got %d", len(res1.ToolCalls))
		}
		breaks := readSiblingBreakEvents(t, logPath)
		if len(breaks) != 1 || breaks[0]["reason"] != "invalid_args_json" || breaks[0]["breakIdx"] != float64(3) {
			t.Fatalf("expected invalid_args_json break at idx 3, got %+v", breaks)
		}

		res2, err := sess.resumeParkedTurn(context.Background(), []ToolResultInput{
			{ToolCallID: res1.ToolCalls[0].ID, Content: "J_0"},
			{ToolCallID: res1.ToolCalls[1].ID, Content: "J_1"},
			{ToolCallID: res1.ToolCalls[2].ID, Content: "J_2"},
		}, []map[string]any{{"role": "user", "content": "hi"}}, nil)
		if err != nil {
			t.Fatalf("resumeParkedTurn turn 2 failed: %v", err)
		}
		if len(res2.ToolCalls) != 3 {
			t.Fatalf("expected resumed batch [4,5,6] (3 tools), got %d", len(res2.ToolCalls))
		}
		sess.mu.Lock()
		nextPos, total := sess.LastPlannerBatchNextPos, sess.LastPlannerBatchTotal
		sess.mu.Unlock()
		if nextPos != 7 || total != 7 {
			t.Fatalf("expected cursor (next=7, total=7), got (%d, %d)", nextPos, total)
		}

		res3, err := sess.resumeParkedTurn(context.Background(), []ToolResultInput{
			{ToolCallID: res2.ToolCalls[0].ID, Content: "J_4"},
			{ToolCallID: res2.ToolCalls[1].ID, Content: "J_5"},
			{ToolCallID: res2.ToolCalls[2].ID, Content: "J_6"},
		}, []map[string]any{{"role": "user", "content": "hi"}}, nil)
		if err != nil {
			t.Fatalf("resumeParkedTurn turn 3 failed: %v", err)
		}
		if err := <-dispatchErr; err != nil {
			t.Fatalf("dispatch error: %v", err)
		}
		if res3.StopReason != "end_turn" {
			t.Fatalf("expected end_turn, got %+v", res3)
		}
	})

	t.Run("loop_breaker", func(t *testing.T) {
		sid := "00000000-0000-4000-8000-000000000404"
		sess, dbPath, logPath := newTestSiblingSession(t, "test-sibling-loop-resume", sid)
		readMcp := "mcp_ws-bridge_" + mcpToolName("Read")

		// 3 identical Read("/same.txt") calls (pos 2 trips 3x loop_breaker) followed by 3 distinct Reads [3,4,5].
		writeTestPlannerStepSQLite(t, dbPath, 1, buildTestPlannerStepPayload([][3]string{
			{"l0", readMcp, `{"file_path":"/same.txt"}`},
			{"l1", readMcp, `{"file_path":"/same.txt"}`},
			{"l2", readMcp, `{"file_path":"/same.txt"}`},
			{"l3", readMcp, `{"file_path":"/x3.txt"}`},
			{"l4", readMcp, `{"file_path":"/x4.txt"}`},
			{"l5", readMcp, `{"file_path":"/x5.txt"}`},
		}))

		turn1 := newActiveTurnState(nil, func() {})
		sess.mu.Lock()
		sess.PromptInFlight = true
		sess.activeTurn = turn1
		sess.mu.Unlock()

		dispatchErr := make(chan error, 1)
		go func() {
			for i := 0; i < 2; i++ {
				res := sess.executeMcpToolCall(context.Background(), "Read", map[string]any{"file_path": "/same.txt"})
				if !strings.Contains(fmt.Sprint(res["content"]), fmt.Sprintf("L_%d", i)) {
					dispatchErr <- fmt.Errorf("pos %d bad result: %+v", i, res)
					return
				}
			}
			// Pos 2 trips 3x TOOL_LOOP_BREAKER and returns isError=true locally.
			loopRes := sess.executeMcpToolCall(context.Background(), "Read", map[string]any{"file_path": "/same.txt"})
			if loopRes["isError"] != true {
				dispatchErr <- fmt.Errorf("expected pos 2 to hit loop circuit breaker, got %+v", loopRes)
				return
			}
			// Pos 3 arrives next on same step 1 -> must align to pos 3 (exact sig) and batch [3, 4, 5].
			for _, i := range []int{3, 4, 5} {
				res := sess.executeMcpToolCall(context.Background(), "Read", map[string]any{"file_path": fmt.Sprintf("/x%d.txt", i)})
				if !strings.Contains(fmt.Sprint(res["content"]), fmt.Sprintf("L_%d", i)) {
					dispatchErr <- fmt.Errorf("pos %d bad result: %+v", i, res)
					return
				}
			}
			finishActiveTurnEnd(sess, "Loop breaker resumed.")
			dispatchErr <- nil
		}()

		var res1 TurnResult
		select {
		case res1 = <-turn1.resolveChan:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for turn 1")
		}
		if len(res1.ToolCalls) != 2 {
			t.Fatalf("expected 2 batched tools before 3x loop_breaker, got %d", len(res1.ToolCalls))
		}
		breaks := readSiblingBreakEvents(t, logPath)
		if len(breaks) != 1 || breaks[0]["reason"] != "loop_breaker" || breaks[0]["breakIdx"] != float64(2) {
			t.Fatalf("expected loop_breaker at idx 2, got %+v", breaks)
		}

		res2, err := sess.resumeParkedTurn(context.Background(), []ToolResultInput{
			{ToolCallID: res1.ToolCalls[0].ID, Content: "L_0"},
			{ToolCallID: res1.ToolCalls[1].ID, Content: "L_1"},
		}, []map[string]any{{"role": "user", "content": "hi"}}, nil)
		if err != nil {
			t.Fatalf("resumeParkedTurn turn 2 failed: %v", err)
		}
		if len(res2.ToolCalls) != 3 {
			t.Fatalf("expected resumed batch [3,4,5] (3 tools), got %d: %+v", len(res2.ToolCalls), res2.ToolCalls)
		}
		for i, pos := range []int{3, 4, 5} {
			if res2.ToolCalls[i].Input["file_path"] != fmt.Sprintf("/x%d.txt", pos) {
				t.Fatalf("turn 2 tool[%d] mismatch: got %+v", i, res2.ToolCalls[i])
			}
		}

		res3, err := sess.resumeParkedTurn(context.Background(), []ToolResultInput{
			{ToolCallID: res2.ToolCalls[0].ID, Content: "L_3"},
			{ToolCallID: res2.ToolCalls[1].ID, Content: "L_4"},
			{ToolCallID: res2.ToolCalls[2].ID, Content: "L_5"},
		}, []map[string]any{{"role": "user", "content": "hi"}}, nil)
		if err != nil {
			t.Fatalf("resumeParkedTurn turn 3 failed: %v", err)
		}
		if err := <-dispatchErr; err != nil {
			t.Fatalf("dispatch error: %v", err)
		}
		if res3.StopReason != "end_turn" {
			t.Fatalf("expected end_turn, got %+v", res3)
		}
	})
}

func TestMcpSiblingSkipsBuiltinToolInMiddle(t *testing.T) {
	sid := "00000000-0000-4000-8000-000000000405"
	sess, dbPath, logPath := newTestSiblingSession(t, "test-sibling-builtin-mid", sid)
	readMcp := "mcp_ws-bridge_" + mcpToolName("Read")

	writeTestPlannerStepSQLite(t, dbPath, 1, buildTestPlannerStepPayload([][3]string{
		{"m0", readMcp, `{"file_path":"/m0.txt"}`},
		{"m1", readMcp, `{"file_path":"/m1.txt"}`},
		{"builtin_mid", "search_web", `{"query":"golang sqlite wal"}`},
		{"m2", readMcp, `{"file_path":"/m2.txt"}`},
		{"m3", readMcp, `{"file_path":"/m3.txt"}`},
	}))

	turn1 := newActiveTurnState(nil, func() {})
	sess.mu.Lock()
	sess.PromptInFlight = true
	sess.activeTurn = turn1
	sess.mu.Unlock()

	dispatchErr := make(chan error, 1)
	go func() {
		for i := 0; i < 4; i++ {
			res := sess.executeMcpToolCall(context.Background(), "Read", map[string]any{"file_path": fmt.Sprintf("/m%d.txt", i)})
			if !strings.Contains(fmt.Sprint(res["content"]), fmt.Sprintf("M_%d", i)) {
				dispatchErr <- fmt.Errorf("m%d unexpected res: %+v", i, res)
				return
			}
		}
		finishActiveTurnEnd(sess, "Builtin skipped cleanly.")
		dispatchErr <- nil
	}()

	var res1 TurnResult
	select {
	case res1 = <-turn1.resolveChan:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for batched turn")
	}

	if res1.StopReason != "tool_use" || len(res1.ToolCalls) != 4 {
		t.Fatalf("expected all 4 MCP siblings batched across middle search_web, got stop=%s count=%d", res1.StopReason, len(res1.ToolCalls))
	}
	for i := 0; i < 4; i++ {
		if res1.ToolCalls[i].Input["file_path"] != fmt.Sprintf("/m%d.txt", i) {
			t.Fatalf("tool[%d] mismatch: %+v", i, res1.ToolCalls[i])
		}
	}
	sess.mu.Lock()
	nextPos, total := sess.LastPlannerBatchNextPos, sess.LastPlannerBatchTotal
	sess.mu.Unlock()
	if nextPos != 4 || total != 4 {
		t.Fatalf("expected cursor (next=4, total=4), got (%d, %d)", nextPos, total)
	}
	if breaks := readSiblingBreakEvents(t, logPath); len(breaks) != 0 {
		t.Fatalf("expected no break log for middle built-in tool, got %+v", breaks)
	}

	res2, err := sess.resumeParkedTurn(context.Background(), []ToolResultInput{
		{ToolCallID: res1.ToolCalls[0].ID, Content: "M_0"},
		{ToolCallID: res1.ToolCalls[1].ID, Content: "M_1"},
		{ToolCallID: res1.ToolCalls[2].ID, Content: "M_2"},
		{ToolCallID: res1.ToolCalls[3].ID, Content: "M_3"},
	}, []map[string]any{{"role": "user", "content": "hi"}}, nil)
	if err != nil {
		t.Fatalf("resumeParkedTurn failed: %v", err)
	}
	if err := <-dispatchErr; err != nil {
		t.Fatalf("dispatch error: %v", err)
	}
	if res2.StopReason != "end_turn" {
		t.Fatalf("expected end_turn, got %+v", res2)
	}
}

func TestMcpSiblingNewPlannerStepResetsCursor(t *testing.T) {
	sid := "00000000-0000-4000-8000-000000000406"
	sess, dbPath, _ := newTestSiblingSession(t, "test-sibling-new-step", sid)
	readMcp := "mcp_ws-bridge_" + mcpToolName("Read")
	writeMcp := "mcp_ws-bridge_" + mcpToolName("Write")
	awaitMcp := "mcp_ws-bridge_" + mcpToolName(AwaitToolName)

	// Step 1 leaves an unbatched tail (nextPos=2 < total=5) after breaking on ws_await_result at pos 2.
	writeTestPlannerStepSQLite(t, dbPath, 1, buildTestPlannerStepPayload([][3]string{
		{"s1_0", readMcp, `{"file_path":"/s1_0.txt"}`},
		{"s1_1", readMcp, `{"file_path":"/s1_1.txt"}`},
		{"s1_2", awaitMcp, `{"call_id":"acp_none"}`},
		{"s1_3", readMcp, `{"file_path":"/s1_3.txt"}`},
		{"s1_4", readMcp, `{"file_path":"/s1_4.txt"}`},
	}))

	turn1 := newActiveTurnState(nil, func() {})
	sess.mu.Lock()
	sess.PromptInFlight = true
	sess.activeTurn = turn1
	sess.mu.Unlock()

	dispatchErr := make(chan error, 1)
	go func() {
		for _, i := range []int{0, 1} {
			res := sess.executeMcpToolCall(context.Background(), "Read", map[string]any{"file_path": fmt.Sprintf("/s1_%d.txt", i)})
			if !strings.Contains(fmt.Sprint(res["content"]), fmt.Sprintf("S1_%d", i)) {
				dispatchErr <- fmt.Errorf("step1 pos %d bad res: %+v", i, res)
				return
			}
		}

		// Before resuming step 1's tail, agy commits a brand-new PlannerResponse step idx=5 with 3 Write tools.
		writeTestPlannerStepSQLite(t, dbPath, 5, buildTestPlannerStepPayload([][3]string{
			{"s5_0", writeMcp, `{"file_path":"/n0.txt","content":"a"}`},
			{"s5_1", writeMcp, `{"file_path":"/n1.txt","content":"b"}`},
			{"s5_2", writeMcp, `{"file_path":"/n2.txt","content":"c"}`},
		}))

		for i, c := range []string{"a", "b", "c"} {
			res := sess.executeMcpToolCall(context.Background(), "Write", map[string]any{
				"file_path": fmt.Sprintf("/n%d.txt", i),
				"content":   c,
			})
			if !strings.Contains(fmt.Sprint(res["content"]), fmt.Sprintf("S5_%d", i)) {
				dispatchErr <- fmt.Errorf("step5 pos %d bad res: %+v", i, res)
				return
			}
		}
		finishActiveTurnEnd(sess, "Step 5 complete.")
		dispatchErr <- nil
	}()

	var res1 TurnResult
	select {
	case res1 = <-turn1.resolveChan:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for step 1 turn")
	}
	if len(res1.ToolCalls) != 2 {
		t.Fatalf("expected 2 tools in step 1 batch, got %d", len(res1.ToolCalls))
	}
	sess.mu.Lock()
	idx1, next1, total1 := sess.LastPlannerBatchIdx, sess.LastPlannerBatchNextPos, sess.LastPlannerBatchTotal
	sess.mu.Unlock()
	if idx1 != 1 || next1 != 2 || total1 != 5 {
		t.Fatalf("expected step 1 cursor (1, 2, 5), got (%d, %d, %d)", idx1, next1, total1)
	}

	// Complete step 1's 2 tools -> goroutine commits step 5 and calls Write("/n0.txt").
	// Even though hasRemainingInStep was true for step 1, zero-wait read sees plannerIdx=5 > 1,
	// resets cursor, and batches all 3 Write calls in one turn.
	res2, err := sess.resumeParkedTurn(context.Background(), []ToolResultInput{
		{ToolCallID: res1.ToolCalls[0].ID, Content: "S1_0"},
		{ToolCallID: res1.ToolCalls[1].ID, Content: "S1_1"},
	}, []map[string]any{{"role": "user", "content": "hi"}}, nil)
	if err != nil {
		t.Fatalf("resumeParkedTurn for step 5 failed: %v", err)
	}
	if res2.StopReason != "tool_use" || len(res2.ToolCalls) != 3 {
		t.Fatalf("expected all 3 step 5 Write tools batched, got stop=%s count=%d", res2.StopReason, len(res2.ToolCalls))
	}
	for i := 0; i < 3; i++ {
		if res2.ToolCalls[i].Name != "Write" || res2.ToolCalls[i].Input["file_path"] != fmt.Sprintf("/n%d.txt", i) {
			t.Fatalf("step 5 tool[%d] mismatch: %+v", i, res2.ToolCalls[i])
		}
	}
	sess.mu.Lock()
	idx2, next2, total2 := sess.LastPlannerBatchIdx, sess.LastPlannerBatchNextPos, sess.LastPlannerBatchTotal
	sess.mu.Unlock()
	if idx2 != 5 || next2 != 3 || total2 != 3 {
		t.Fatalf("expected step 5 cursor (5, 3, 3), got (%d, %d, %d)", idx2, next2, total2)
	}

	res3, err := sess.resumeParkedTurn(context.Background(), []ToolResultInput{
		{ToolCallID: res2.ToolCalls[0].ID, Content: "S5_0"},
		{ToolCallID: res2.ToolCalls[1].ID, Content: "S5_1"},
		{ToolCallID: res2.ToolCalls[2].ID, Content: "S5_2"},
	}, []map[string]any{{"role": "user", "content": "hi"}}, nil)
	if err != nil {
		t.Fatalf("resumeParkedTurn final failed: %v", err)
	}
	if err := <-dispatchErr; err != nil {
		t.Fatalf("dispatch error: %v", err)
	}
	if res3.StopReason != "end_turn" {
		t.Fatalf("expected end_turn, got %+v", res3)
	}
}

func TestMcpSiblingPosNotFoundLogsAndClosesCursor(t *testing.T) {
	sid := "00000000-0000-4000-8000-000000000407"
	sess, dbPath, logPath := newTestSiblingSession(t, "test-sibling-not-found", sid)
	readMcp := "mcp_ws-bridge_" + mcpToolName("Read")
	awaitMcp := "mcp_ws-bridge_" + mcpToolName(AwaitToolName)

	// Step 1 only lists Read tools; incoming Write tool cannot align -> cur_pos_not_found.
	writeTestPlannerStepSQLite(t, dbPath, 1, buildTestPlannerStepPayload([][3]string{
		{"r0", readMcp, `{"file_path":"/r0.txt"}`},
		{"r1", readMcp, `{"file_path":"/r1.txt"}`},
	}))

	turn1 := newActiveTurnState(nil, func() {})
	sess.mu.Lock()
	sess.PromptInFlight = true
	sess.activeTurn = turn1
	sess.mu.Unlock()

	dispatchErr := make(chan error, 1)
	go func() {
		res := sess.executeMcpToolCall(context.Background(), "Write", map[string]any{"file_path": "/w.txt", "content": "x"})
		if !strings.Contains(fmt.Sprint(res["content"]), "W_OK") {
			dispatchErr <- fmt.Errorf("step1 unexpected res: %+v", res)
			return
		}
		// Step 2 has [Read(0), ws_await_result(1), Read(2)]; pos 0 breaks at 1 (NextPos=1, Total=3).
		// Then a drifted Write call arrives on resume -> resume_pos_not_found closes cursor to 3.
		writeTestPlannerStepSQLite(t, dbPath, 2, buildTestPlannerStepPayload([][3]string{
			{"p0", readMcp, `{"file_path":"/p0.txt"}`},
			{"p1", awaitMcp, `{"call_id":"acp_none"}`},
			{"p2", readMcp, `{"file_path":"/p2.txt"}`},
		}))
		resP0 := sess.executeMcpToolCall(context.Background(), "Read", map[string]any{"file_path": "/p0.txt"})
		if !strings.Contains(fmt.Sprint(resP0["content"]), "P0_OK") {
			dispatchErr <- fmt.Errorf("p0 unexpected res: %+v", resP0)
			return
		}
		resDrift := sess.executeMcpToolCall(context.Background(), "Write", map[string]any{"file_path": "/drift.txt", "content": "y"})
		if !strings.Contains(fmt.Sprint(resDrift["content"]), "DRIFT_OK") {
			dispatchErr <- fmt.Errorf("drift unexpected res: %+v", resDrift)
			return
		}
		finishActiveTurnEnd(sess, "Done.")
		dispatchErr <- nil
	}()

	var res1 TurnResult
	select {
	case res1 = <-turn1.resolveChan:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for step 1")
	}
	if len(res1.ToolCalls) != 1 || res1.ToolCalls[0].Name != "Write" {
		t.Fatalf("expected single unbatched Write call, got %+v", res1.ToolCalls)
	}
	sess.mu.Lock()
	next1, total1 := sess.LastPlannerBatchNextPos, sess.LastPlannerBatchTotal
	sess.mu.Unlock()
	if next1 != 2 || total1 != 2 {
		t.Fatalf("cur_pos_not_found must close cursor to total (2, 2), got (%d, %d)", next1, total1)
	}

	res2, err := sess.resumeParkedTurn(context.Background(), []ToolResultInput{
		{ToolCallID: res1.ToolCalls[0].ID, Content: "W_OK"},
	}, []map[string]any{{"role": "user", "content": "hi"}}, nil)
	if err != nil {
		t.Fatalf("resume step 2 p0 failed: %v", err)
	}
	if len(res2.ToolCalls) != 1 || res2.ToolCalls[0].Name != "Read" {
		t.Fatalf("expected p0 Read tool, got %+v", res2.ToolCalls)
	}

	res3, err := sess.resumeParkedTurn(context.Background(), []ToolResultInput{
		{ToolCallID: res2.ToolCalls[0].ID, Content: "P0_OK"},
	}, []map[string]any{{"role": "user", "content": "hi"}}, nil)
	if err != nil {
		t.Fatalf("resume step 2 drift failed: %v", err)
	}
	if len(res3.ToolCalls) != 1 || res3.ToolCalls[0].Name != "Write" {
		t.Fatalf("expected drifted Write tool, got %+v", res3.ToolCalls)
	}
	sess.mu.Lock()
	next3, total3 := sess.LastPlannerBatchNextPos, sess.LastPlannerBatchTotal
	sess.mu.Unlock()
	if next3 != 3 || total3 != 3 {
		t.Fatalf("resume_pos_not_found must close cursor to total (3, 3), got (%d, %d)", next3, total3)
	}

	res4, err := sess.resumeParkedTurn(context.Background(), []ToolResultInput{
		{ToolCallID: res3.ToolCalls[0].ID, Content: "DRIFT_OK"},
	}, []map[string]any{{"role": "user", "content": "hi"}}, nil)
	if err != nil {
		t.Fatalf("final resume failed: %v", err)
	}
	if err := <-dispatchErr; err != nil {
		t.Fatalf("dispatch error: %v", err)
	}
	if res4.StopReason != "end_turn" {
		t.Fatalf("expected end_turn, got %+v", res4)
	}

	breaks := readSiblingBreakEvents(t, logPath)
	if len(breaks) != 3 {
		t.Fatalf("expected 3 MCP_TOOL_SIBLING_BREAK events (cur_pos_not_found, await_helper, resume_pos_not_found), got %d: %+v", len(breaks), breaks)
	}
	if breaks[0]["reason"] != "cur_pos_not_found" || breaks[0]["resumedStep"] != false {
		t.Fatalf("unexpected break[0]: %+v", breaks[0])
	}
	if breaks[1]["reason"] != "await_helper" || breaks[1]["resumedStep"] != false {
		t.Fatalf("unexpected break[1]: %+v", breaks[1])
	}
	if breaks[2]["reason"] != "resume_pos_not_found" || breaks[2]["resumedStep"] != true {
		t.Fatalf("unexpected break[2]: %+v", breaks[2])
	}
}

func TestMcpSiblingLoopBreakerDoesNotDoubleDispatchFifthIdentical(t *testing.T) {
	sid := "00000000-0000-4000-8000-000000000409"
	sess, dbPath, _ := newTestSiblingSession(t, "test-sibling-loop-5x", sid)
	readMcp := "mcp_ws-bridge_" + mcpToolName("Read")

	// 4 identical Read(/same.txt) followed by Read(/tail.txt):
	// Pos 0..1 batch together; pos 2 trips 3x loop_breaker and advances nextPos=3;
	// Pos 3 MUST align to index 3 (NOT re-align to index 2 and double-dispatch index 3!).
	writeTestPlannerStepSQLite(t, dbPath, 1, buildTestPlannerStepPayload([][3]string{
		{"s0", readMcp, `{"file_path":"/same.txt"}`},
		{"s1", readMcp, `{"file_path":"/same.txt"}`},
		{"s2", readMcp, `{"file_path":"/same.txt"}`},
		{"s3", readMcp, `{"file_path":"/same.txt"}`},
		{"s4", readMcp, `{"file_path":"/tail.txt"}`},
	}))

	turn1 := newActiveTurnState(nil, func() {})
	sess.mu.Lock()
	sess.PromptInFlight = true
	sess.activeTurn = turn1
	sess.mu.Unlock()

	dispatchErr := make(chan error, 1)
	go func() {
		for i := 0; i < 2; i++ {
			res := sess.executeMcpToolCall(context.Background(), "Read", map[string]any{"file_path": "/same.txt"})
			if !strings.Contains(fmt.Sprint(res["content"]), fmt.Sprintf("S_%d", i)) {
				dispatchErr <- fmt.Errorf("pos %d bad result: %+v", i, res)
				return
			}
		}
		// Pos 2 trips 3x TOOL_LOOP_BREAKER locally.
		loopRes := sess.executeMcpToolCall(context.Background(), "Read", map[string]any{"file_path": "/same.txt"})
		if loopRes["isError"] != true {
			dispatchErr <- fmt.Errorf("expected pos 2 loop_breaker error, got %+v", loopRes)
			return
		}
		// Pos 3 & 4 resume cleanly as a 2-tool batch [pos 3, pos 4] with zero duplicate pos 3.
		r3 := sess.executeMcpToolCall(context.Background(), "Read", map[string]any{"file_path": "/same.txt"})
		if !strings.Contains(fmt.Sprint(r3["content"]), "S_3") {
			dispatchErr <- fmt.Errorf("pos 3 bad result: %+v", r3)
			return
		}
		r4 := sess.executeMcpToolCall(context.Background(), "Read", map[string]any{"file_path": "/tail.txt"})
		if !strings.Contains(fmt.Sprint(r4["content"]), "S_4") {
			dispatchErr <- fmt.Errorf("pos 4 bad result: %+v", r4)
			return
		}
		finishActiveTurnEnd(sess, "4x loop resolved cleanly.")
		dispatchErr <- nil
	}()

	var res1 TurnResult
	select {
	case res1 = <-turn1.resolveChan:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for turn 1")
	}
	if len(res1.ToolCalls) != 2 {
		t.Fatalf("expected 2 tools in turn 1, got %d", len(res1.ToolCalls))
	}
	sess.mu.Lock()
	nextAfterBreak, totalAfterBreak := sess.LastPlannerBatchNextPos, sess.LastPlannerBatchTotal
	sess.mu.Unlock()
	if nextAfterBreak != 3 || totalAfterBreak != 5 {
		t.Fatalf("loop_breaker at idx 2 must advance nextPos=3/total=5, got (%d, %d)", nextAfterBreak, totalAfterBreak)
	}

	res2, err := sess.resumeParkedTurn(context.Background(), []ToolResultInput{
		{ToolCallID: res1.ToolCalls[0].ID, Content: "S_0"},
		{ToolCallID: res1.ToolCalls[1].ID, Content: "S_1"},
	}, []map[string]any{{"role": "user", "content": "hi"}}, nil)
	if err != nil {
		t.Fatalf("resumeParkedTurn turn 2 failed: %v", err)
	}
	if len(res2.ToolCalls) != 2 ||
		res2.ToolCalls[0].Input["file_path"] != "/same.txt" ||
		res2.ToolCalls[1].Input["file_path"] != "/tail.txt" {
		t.Fatalf("expected exactly 2 resumed tools [/same.txt, /tail.txt] without double-dispatch, got %+v", res2.ToolCalls)
	}

	res3, err := sess.resumeParkedTurn(context.Background(), []ToolResultInput{
		{ToolCallID: res2.ToolCalls[0].ID, Content: "S_3"},
		{ToolCallID: res2.ToolCalls[1].ID, Content: "S_4"},
	}, []map[string]any{{"role": "user", "content": "hi"}}, nil)
	if err != nil {
		t.Fatalf("resumeParkedTurn turn 3 failed: %v", err)
	}
	if err := <-dispatchErr; err != nil {
		t.Fatalf("dispatch error: %v", err)
	}
	if res3.StopReason != "end_turn" {
		t.Fatalf("expected end_turn, got %+v", res3)
	}
	sess.mu.Lock()
	remPending := len(sess.PendingSiblingCalls)
	sess.mu.Unlock()
	if remPending != 0 {
		t.Fatalf("expected 0 orphaned PendingSiblingCalls, got %d", remPending)
	}
}

func TestMcpSiblingCursorResetOnNewPromptAndHibernateWake(t *testing.T) {
	setupM2IsolatedEnv(t)
	baseURL, gs := startCarbonTestGateway(t)

	u1 := map[string]any{"role": "user", "content": "seed session"}
	st1, d1, raw1 := m2PostMessages(t, baseURL, "sibling-cursor-reset", "gemini-4-carbon", m2DefaultBashTools(), []map[string]any{u1})
	if st1 != http.StatusOK {
		t.Fatalf("turn 1 failed: %d %s", st1, string(raw1))
	}
	sess := m2FindSession(gs, "sibling-cursor-reset")
	if sess == nil {
		t.Fatal("expected session")
	}

	// Simulate an unconsumed cursor left behind by a prior aborted/interrupted prompt.
	sess.mu.Lock()
	sess.LastPlannerBatchIdx = 5
	sess.LastPlannerBatchNextPos = 2
	sess.LastPlannerBatchTotal = 7
	sess.mu.Unlock()

	// Next prompt must unconditionally reset LastPlannerBatchNextPos/Total in executePromptOrPark.
	cs1, _ := d1["content"].([]any)
	st2, _, raw2 := m2PostMessages(t, baseURL, "sibling-cursor-reset", "gemini-4-carbon", m2DefaultBashTools(), []map[string]any{
		u1,
		{"role": "assistant", "content": cs1},
		{"role": "user", "content": "follow up turn"},
	})
	if st2 != http.StatusOK {
		t.Fatalf("turn 2 failed: %d %s", st2, string(raw2))
	}
	sess.mu.Lock()
	nextAfterPrompt, totalAfterPrompt := sess.LastPlannerBatchNextPos, sess.LastPlannerBatchTotal
	sess.mu.Unlock()
	if nextAfterPrompt != 0 || totalAfterPrompt != 0 {
		t.Fatalf("executePromptOrPark must reset sibling cursor to (0,0), got (%d,%d)", nextAfterPrompt, totalAfterPrompt)
	}

	// Hibernate with a simulated stale cursor and verify wake resets it cleanly.
	sess.mu.Lock()
	toolsCat := append([]ToolDef(nil), sess.ToolsCatalog...)
	sess.LastPlannerBatchIdx = 9
	sess.LastPlannerBatchNextPos = 3
	sess.LastPlannerBatchTotal = 8
	sess.mu.Unlock()
	if !sess.HibernateWorker("test_cursor_wake", false) {
		t.Fatal("expected HibernateWorker(false) to succeed")
	}
	sess.mu.Lock()
	hibSid := sess.HibernatedSessionID
	sess.mu.Unlock()
	if hibSid == "" {
		t.Fatal("expected HibernatedSessionID to be populated before wake")
	}
	if err := sess.ensureBooted(toolsCat, "medium"); err != nil {
		t.Fatalf("ensureBooted wake failed: %v", err)
	}
	sess.mu.Lock()
	nextAfterWake, totalAfterWake := sess.LastPlannerBatchNextPos, sess.LastPlannerBatchTotal
	sess.mu.Unlock()
	if nextAfterWake != 0 || totalAfterWake != 0 {
		t.Fatalf("reloadHibernatedSession must reset sibling cursor to (0,0), got (%d,%d)", nextAfterWake, totalAfterWake)
	}
}
