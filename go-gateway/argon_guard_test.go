package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
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

func TestEvaluateArgonDegradation5D(t *testing.T) {
	// 1. Block-aligned cache alone (F4 = +3) MUST NOT trigger degradation (corroborating only)
	gmBlockOnly := ParseGenMetadataBlob(buildTestGenMetadataBlob(1417, 2500, 180, 32768, 80, 100, 35268, 983040, 4.0, 2.0))
	repBlock := EvaluateArgonDegradation(gmBlockOnly, ArgonEvalContext{
		ModelID:      "gemini-4-argon",
		ThoughtLevel: "high",
	})
	if repBlock.Score != 3 || repBlock.Degraded {
		t.Fatalf("block_cache alone must not be marked degraded: score=%d degraded=%v signals=%+v", repBlock.Score, repBlock.Degraded, repBlock.Signals)
	}

	// 2. Context truncated at 128K (F1 = +5) MUST trigger degradation
	gmTrunc := ParseGenMetadataBlob(buildTestGenMetadataBlob(1417, 127980, 90, 0, 40, 50, 172000, 983040, 3.5, 1.2))
	repTrunc := EvaluateArgonDegradation(gmTrunc, ArgonEvalContext{
		ModelID:      "gemini-4-argon",
		ThoughtLevel: "high",
	})
	if !repTrunc.Degraded || repTrunc.Score < 5 {
		t.Fatalf("expected context_truncated degradation, got %+v", repTrunc)
	}

	// 3. Cache cliff without compaction (F2 = +4) MUST trigger degradation, but NOT when CompactedThisTurn=true
	gmCliff := ParseGenMetadataBlob(buildTestGenMetadataBlob(1418, 28000, 60, 0, 20, 40, 28000, 983040, 3.0, 1.0))
	repCliff := EvaluateArgonDegradation(gmCliff, ArgonEvalContext{
		ModelID:              "gemini-4-argon",
		ThoughtLevel:         "medium",
		CompactedThisTurn:    false,
		PrevRawCachedTokens:  32000,
		PrevTotalInputTokens: 34000,
	})
	if !repCliff.Degraded || repCliff.Score != 4 {
		t.Fatalf("expected cache_cliff degradation, got %+v", repCliff)
	}
	repCompacted := EvaluateArgonDegradation(gmCliff, ArgonEvalContext{
		ModelID:              "gemini-4-argon",
		ThoughtLevel:         "medium",
		CompactedThisTurn:    true,
		PrevRawCachedTokens:  32000,
		PrevTotalInputTokens: 34000,
	})
	if repCompacted.Degraded {
		t.Fatalf("compacted turn must not trigger cache_cliff: %+v", repCompacted)
	}

	// 4. Fast decode + shallow thought (F3 + F5) MUST trigger degradation
	gmFastShallow := ParseGenMetadataBlob(buildTestGenMetadataBlob(1417, 18000, 120, 0, 0, 120, 18000, 983040, 1.8, 0.42))
	repFast := EvaluateArgonDegradation(gmFastShallow, ArgonEvalContext{
		ModelID:      "gemini-4-argon",
		ThoughtLevel: "high",
	})
	if !repFast.Degraded || repFast.Score < 5 {
		t.Fatalf("expected fast_decode + shallow_thought degradation, got %+v", repFast)
	}

	// 5. Non-Argon model (Flash 1320) MUST NOT be marked degraded
	gmFlash := ParseGenMetadataBlob(buildTestGenMetadataBlob(1320, 127990, 120, 0, 0, 120, 168000, 983040, 1.2, 0.38))
	repFlash := EvaluateArgonDegradation(gmFlash, ArgonEvalContext{
		ModelID:      "gemini-3.8-flash",
		ThoughtLevel: "low",
	})
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

	// Session 1 (00000000-0000-4000-8000-000000000001) gets a degraded gen_metadata row (128K clamp)
	sid1 := "00000000-0000-4000-8000-000000000001"
	degradedBlob := buildTestGenMetadataBlob(1417, 127990, 80, 0, 0, 80, 168000, 983040, 1.9, 0.40)
	writeTestGenMetadataSQLite(t, filepath.Join(convDir, sid1+".db"), 0, degradedBlob)

	// Session 2 (00000000-0000-4000-8000-000000000002) gets a healthy 1M gen_metadata row
	sid2 := "00000000-0000-4000-8000-000000000002"
	healthyBlob := buildTestGenMetadataBlob(1417, 3054, 140, 164946, 60, 80, 168000, 983040, 4.2, 1.6)
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
