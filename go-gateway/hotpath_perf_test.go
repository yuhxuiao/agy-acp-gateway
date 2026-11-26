package main

import (
	"database/sql"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gwstore "agy-acp-gateway-go/internal/store"
)

func buildRealisticHotPathFixture(tb testing.TB) (rawSys string, rawTools []any, messages []map[string]any, body map[string]any, cwd string) {
	tb.Helper()
	cwd = tb.TempDir()

	// ~25KB Claude Code / Pi style system prompt with Primary working directory
	var sysBuf strings.Builder
	sysBuf.WriteString("You are Claude Code, Anthropic's official CLI for Claude.\n")
	sysBuf.WriteString("Primary working directory: " + cwd + "\n\n")
	for i := 0; i < 120; i++ {
		fmt.Fprintf(&sysBuf, "Rule %03d: Follow clean engineering practices, check tests in `src/module_%02d/handler.go`, avoid touching `.claude/settings.json` or https://docs.example.com/guide/%d unless requested.\n", i, i%20, i)
	}
	rawSys = sysBuf.String()

	// 20 tools with nested JSON schemas
	toolNames := []string{
		"Read", "Write", "Edit", "MultiEdit", "Bash", "Grep", "Glob", "LS",
		"TodoWrite", "TodoRead", "NotebookEdit", "WebFetch", "Task", "Agent",
		"mcp__dbx__execute_query", "mcp__dbx__list_tables", "mcp__git__status",
		"mcp__git__diff", "mcp__browser__open", "mcp__browser__click",
	}
	rawTools = make([]any, 0, len(toolNames))
	for i, name := range toolNames {
		rawTools = append(rawTools, map[string]any{
			"name":        name,
			"description": fmt.Sprintf("Workspace tool %s (#%d) to inspect or modify repository files and run diagnostics without modifying external state.", name, i),
			"input_schema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"path":    map[string]any{"type": "string", "description": "Relative or absolute path inside the repository"},
					"command": map[string]any{"type": "string", "description": "Shell command or query payload to run"},
					"options": map[string]any{
						"type": "object",
						"properties": map[string]any{
							"limit":   map[string]any{"type": "integer", "description": "Max items to return"},
							"offset":  map[string]any{"type": "integer", "description": "Offset into results"},
							"filters": map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
						},
					},
				},
				"required": []any{"path"},
			},
		})
	}

	// 150 alternating messages (~1.2MB total, realistic tool_use + tool_result with code/logs/paths)
	var sampleChunk strings.Builder
	for j := 0; j < 85; j++ {
		fmt.Fprintf(&sampleChunk, "  line %04d: func handleRequest_%d(ctx context.Context, path string) error { /* /Users/dev/project/internal/pkg_%d/service.go */ return nil }\n", j, j, j%10)
	}
	toolOutputStr := sampleChunk.String() // ~13KB per tool_result

	messages = make([]map[string]any, 0, 150)
	messages = append(messages, map[string]any{
		"role": "user",
		"content": []any{
			map[string]any{
				"type":          "text",
				"text":          "Investigate performance and cache drop behavior in go-gateway across the repository.",
				"cache_control": map[string]any{"type": "ephemeral"},
			},
		},
	})
	for i := 1; i < 149; i += 2 {
		callID := fmt.Sprintf("acp_call_%04d", i)
		messages = append(messages, map[string]any{
			"role": "assistant",
			"content": []any{
				map[string]any{"type": "text", "text": fmt.Sprintf("Checking module %d and running diagnostics.", i)},
				map[string]any{"type": "tool_use", "id": callID, "name": "Read", "input": map[string]any{"path": fmt.Sprintf("internal/pkg_%d/service.go", i%10)}},
			},
		})
		trContent := toolOutputStr
		if i%15 == 1 {
			trContent = "<system-reminder>Context note for turn</system-reminder>\n" + trContent
		}
		messages = append(messages, map[string]any{
			"role": "user",
			"content": []any{
				map[string]any{"type": "tool_result", "tool_use_id": callID, "content": trContent},
			},
		})
	}
	// Ensure 150th message is a tool_result turn
	messages = append(messages, map[string]any{
		"role": "user",
		"content": []any{
			map[string]any{"type": "tool_result", "tool_use_id": "acp_call_latest", "content": toolOutputStr},
		},
	})

	body = map[string]any{
		"model":      "gemini-4-neon",
		"max_tokens": 32000,
		"system":     rawSys,
		"tools":      rawTools,
		"messages":   messages,
		"stream":     true,
		"metadata":   map[string]any{"user_id": "user_abc_session_8f8b550a-4a29-4c65-94ec-7c5e0ffb8b6b"},
	}
	return rawSys, rawTools, messages, body, cwd
}

func BenchmarkHotPath_Components(b *testing.B) {
	rawSys, rawTools, messages, body, cwd := buildRealisticHotPathFixture(b)
	_ = os.MkdirAll(cwd, 0755)
	sysText := PruneClientSystemPrompt(rawSys)
	toolsCat := sanitizeToolsCatalog(rawTools)
	req := httptest.NewRequest("POST", "/v1/messages", nil)
	req.Header.Set("User-Agent", "claude-cli/2.1.305")
	req.Header.Set("x-claude-code-session-id", "8f8b550a-4a29-4c65-94ec-7c5e0ffb8b6b")

	codec := NewReversibleSessionCodec("bench-tenant-key")
	codec.SeedFromToolsCatalog(toolsCat)
	codec.SeedFromConversationOnce(sysText, messages[:len(messages)-1])

	b.Run("SeedFromConversationOnce_Warm", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			codec.SeedFromConversationOnce(sysText, messages[:len(messages)-1])
		}
	})

	b.Run("PruneClientSystemPrompt_Warm", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = PruneClientSystemPrompt(rawSys)
		}
	})

	b.Run("SanitizeToolsCatalog_Warm", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = sanitizeToolsCatalog(rawTools)
		}
	})

	b.Run("ExtractCwdFromSystem", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = extractCwdFromSystem(sysText, messages)
		}
	})

	b.Run("ComputeTurnReqFp", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = computeTurnRequestFingerprintWithDirective("gemini-4-neon", "xhigh", sysText, toolsCat, messages, "")
		}
	})

	b.Run("SeedFromToolsCatalog_x2_Warm", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			codec.SeedFromToolsCatalog(toolsCat)
			codec.SeedFromToolsCatalog(toolsCat)
		}
	})

	b.Run("FormatParkedToolResult_13KB_Clean", func(b *testing.B) {
		b.ReportAllocs()
		tr := ToolResultInput{
			ToolCallID: "acp_call_latest",
			Content:    messages[len(messages)-1]["content"].([]any)[0].(map[string]any)["content"].(string),
		}
		for i := 0; i < b.N; i++ {
			_ = formatParkedToolResultText(tr, codec, "OK")
		}
	})

	b.Run("SystemPromptDelta_1LineDrift", func(b *testing.B) {
		b.ReportAllocs()
		drifted := sysText + "\nCurrent branch: feature/hotpath-opt"
		for i := 0; i < b.N; i++ {
			_ = systemPromptDelta(sysText, drifted)
		}
	})

	b.Run("ComputeMessagesSliceFp", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = computeMessagesSliceFingerprint(messages)
		}
	})

	b.Run("Composite_HTTPReq_To_MCPUnblock", func(b *testing.B) {
		b.ReportAllocs()
		tr := ToolResultInput{
			ToolCallID: "acp_call_latest",
			Content:    messages[len(messages)-1]["content"].([]any)[0].(map[string]any)["content"].(string),
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rSys := rawSystemText(body["system"])
			pSys := PruneClientSystemPrompt(rSys)
			_ = extractCwdFromSystem(pSys, messages)
			tCat := sanitizeToolsCatalog(body["tools"])
			_ = toolCatalogFingerprint(tCat)
			_ = deriveConversationKey(req, body, pSys, tCat, messages)
			_ = computeFirstUserFingerprint(messages)
			_ = computeTurnRequestFingerprintWithDirective("gemini-4-neon", "xhigh", pSys, tCat, messages, "")
			codec.SeedFromToolsCatalog(tCat)
			codec.SeedFromToolsCatalog(tCat)
			codec.SeedFromConversationOnce(pSys, messages[:len(messages)-1])
			_ = extractToolResultsFromMessages(messages)
			_ = computeMessagesSliceFingerprint(messages)
			_ = formatParkedToolResultText(tr, codec, "OK")
		}
	})

	b.Run("Composite_HTTPReq_To_MCPUnblock_Precomputed", func(b *testing.B) {
		b.ReportAllocs()
		tr := ToolResultInput{
			ToolCallID: "acp_call_latest",
			Content:    messages[len(messages)-1]["content"].([]any)[0].(map[string]any)["content"].(string),
		}
		warmFp := toolCatalogFingerprint(toolsCat)
		codec.SeedFromToolsCatalogOnce(warmFp, toolsCat)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rSys := rawSystemText(body["system"])
			pSys := PruneClientSystemPrompt(rSys)
			c := extractCwdFromSystem(pSys, messages)
			tCat := sanitizeToolsCatalog(body["tools"])
			tFp, tDigests := toolCatalogFingerprintAndDigests(tCat)
			firstFp := computeFirstUserFingerprint(messages)
			_ = deriveConversationKeyWithFirstFp(req, body, pSys, tCat, messages, firstFp)
			_ = computeTurnRequestFingerprintPrecomputed("gemini-4-neon", "xhigh", c, tDigests, messages, "")
			codec.SeedFromToolsCatalogOnce(tFp, tCat)
			codec.SeedFromToolsCatalogOnce(tFp, tCat)
			codec.SeedFromConversationOnce(pSys, messages[:len(messages)-1])
			_ = extractToolResultsFromMessages(messages)
			_ = formatParkedToolResultText(tr, codec, "OK")
		}
	})
}

func TestHotPathInvariants(t *testing.T) {
	t.Run("CodecSeeding_SQLiteRestore_InPlaceUpdate_TenantSwitch", func(t *testing.T) {
		initCat := []ToolDef{
			{
				Name:        "Read",
				Description: "Inspect <claude_custom>files</claude_custom> in /Users/dev/DeepSeek Harness/repo",
				Parameters:  map[string]any{"type": "object", "properties": map[string]any{"path": map[string]any{"description": "Path inside /Users/dev/DeepSeek Harness/repo"}}},
			},
		}
		updatedCat := append(append([]ToolDef(nil), initCat...), ToolDef{
			Name:        "Deploy",
			Description: "Trigger <codex_deploy>job</codex_deploy> for workspace",
			Parameters:  map[string]any{"type": "object"},
		})
		fpInit := toolCatalogFingerprint(initCat)
		fpUpd := toolCatalogFingerprint(updatedCat)

		// (a) Post-restart SQLite hydration: fresh Codec with existing session ToolFingerprint MUST seed.
		codec := NewReversibleSessionCodec("tenant-A")
		if codec.IsToolsFPSeeded(fpInit) {
			t.Fatal("fresh codec must not claim init tools fp is seeded")
		}
		codec.SeedFromToolsCatalogOnce(fpInit, initCat)
		if !codec.IsToolsFPSeeded(fpInit) {
			t.Fatal("expected init tools fp to be marked seeded")
		}
		if codec.Count() == 0 {
			t.Fatal("expected sensitive tokens from initCat to be seeded into fresh codec")
		}

		// (b) TOOLS_UPDATED_IN_PLACE: both fpInit and fpUpd stay memoized without thrashing.
		codec.SeedFromToolsCatalogOnce(fpUpd, updatedCat)
		if !codec.IsToolsFPSeeded(fpInit) || !codec.IsToolsFPSeeded(fpUpd) {
			t.Fatal("expected both initial and updated tool catalog fps to remain seeded")
		}
		enc := codec.EncodeOutboundText("<codex_deploy>run</codex_deploy>")
		if strings.Contains(enc, "codex_deploy") {
			t.Fatalf("expected updated tool tag to be encoded, got %q", enc)
		}

		// History seeding once then short-circuiting.
		msgs := []map[string]any{
			{"role": "user", "content": "check <anthropic_note>alpha</anthropic_note>"},
		}
		codec.SeedFromConversationOnce("", msgs)
		if !codec.IsHistorySeeded() {
			t.Fatal("expected history to be marked seeded")
		}
		countAfterFirst := codec.Count()
		// Subsequent call with unseen tag must be skipped when history is already seeded.
		codec.SeedFromConversationOnce("", []map[string]any{
			{"role": "user", "content": "check <claude_unseen_second>beta</claude_unseen_second>"},
		})
		if codec.Count() != countAfterFirst {
			t.Fatalf("expected second SeedFromConversationOnce to short-circuit, count %d -> %d", countAfterFirst, codec.Count())
		}

		// (c) SetTenantKey: same key keeps state; different key resets both historySeeded & seededToolFps.
		codec.SetTenantKey("tenant-A")
		if !codec.IsHistorySeeded() || !codec.IsToolsFPSeeded(fpInit) || !codec.IsToolsFPSeeded(fpUpd) {
			t.Fatal("same-key SetTenantKey must not reset seeded state")
		}
		codec.SetTenantKey("tenant-B")
		if codec.IsHistorySeeded() || codec.IsToolsFPSeeded(fpInit) || codec.IsToolsFPSeeded(fpUpd) {
			t.Fatal("changed SetTenantKey must reset historySeeded and seededToolFps")
		}
	})

	t.Run("WafCacheInvalidation_SystemAndTools", func(t *testing.T) {
		dir := t.TempDir()
		wafPath := filepath.Join(dir, "waf_rewrite_rules.json")
		origPath := WafRewriteRulesFile
		WafRewriteRulesFile = wafPath
		resetWafRuleCacheForTest()
		t.Cleanup(func() {
			WafRewriteRulesFile = origPath
			resetWafRuleCacheForTest()
		})

		rawSys := "Workspace guide.\n- unique waf sentinel line alpha beta gamma\nEnd."
		rawTools := []any{
			map[string]any{
				"name":         "Echo",
				"description":  "<workspace-context>unique waf sentinel line alpha beta gamma</workspace-context>",
				"input_schema": map[string]any{"type": "object"},
			},
		}
		p1 := PruneClientSystemPrompt(rawSys)
		if !strings.Contains(p1, "unique waf sentinel line alpha beta gamma") {
			t.Fatalf("expected sentinel before WAF rule, got %q", p1)
		}
		t1 := sanitizeToolsCatalog(rawTools)
		if len(t1) == 0 || !strings.Contains(t1[0].Description, "unique waf sentinel line alpha beta gamma") {
			t.Fatalf("expected tool sentinel before WAF rule, got %+v", t1)
		}
		// Mutate WAF file directly on disk (bypassing persistDynamicWafRule) to prove live os.Stat invalidation.
		ruleJSON := `{"version":1,"rules":[{"id":"r1","literal":"unique waf sentinel line alpha beta gamma","replacement":"","source":"test","createdAt":"2026-11-19T00:00:00Z"}]}`
		if err := os.WriteFile(wafPath, []byte(ruleJSON), 0600); err != nil {
			t.Fatalf("write waf rules: %v", err)
		}
		p2 := PruneClientSystemPrompt(rawSys)
		if strings.Contains(p2, "unique waf sentinel line alpha beta gamma") {
			t.Fatalf("expected PruneClientSystemPrompt cache invalidated after WAF file edit, got %q", p2)
		}
		t2 := sanitizeToolsCatalog(rawTools)
		if len(t2) == 0 || strings.Contains(t2[0].Description, "unique waf sentinel line alpha beta gamma") {
			t.Fatalf("expected sanitizeToolsCatalog cache invalidated after WAF file edit, got %+v", t2)
		}
	})

	t.Run("ExtractCwdPrecedenceAndLiveStat", func(t *testing.T) {
		dir0 := filepath.Join(t.TempDir(), "primary_ws")
		dir1 := filepath.Join(t.TempDir(), "fallback_ws")
		if err := os.MkdirAll(dir0, 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(dir1, 0755); err != nil {
			t.Fatal(err)
		}
		sys := fmt.Sprintf("Primary working directory: %s\nWORKSPACE_DIR: %s\n", dir0, dir1)
		if got := extractCwdFromSystem(sys, nil); got != dir0 {
			t.Fatalf("expected primary dir %q, got %q", dir0, got)
		}
		// Remove dir0: live os.Stat must fall through to dir1 without returning stale cached dir0.
		if err := os.Remove(dir0); err != nil {
			t.Fatal(err)
		}
		if got := extractCwdFromSystem(sys, nil); got != dir1 {
			t.Fatalf("after removing primary dir, expected fallback dir %q, got %q", dir1, got)
		}
	})

	t.Run("PrecomputedReqFpAndLazyCancelledHistFp", func(t *testing.T) {
		rawSys, rawTools, messages, _, cwd := buildRealisticHotPathFixture(t)
		sysText := PruneClientSystemPrompt(rawSys)
		toolsCat := sanitizeToolsCatalog(rawTools)
		_, digests := toolCatalogFingerprintAndDigests(toolsCat)

		wantFp := computeTurnRequestFingerprintWithDirective("gemini-4-neon", "xhigh", sysText, toolsCat, messages, "dir-tok")
		gotFp := computeTurnRequestFingerprintPrecomputed("gemini-4-neon", "xhigh", cwd, digests, messages, "dir-tok")
		if gotFp != wantFp {
			t.Fatalf("precomputed reqFp mismatch: got %q want %q", gotFp, wantFp)
		}

		var calls atomic.Int32
		meta := &turnDetachMeta{
			cancelledHistFpFn: func() string {
				calls.Add(1)
				return computeMessagesSliceFingerprint(messages)
			},
		}
		if calls.Load() != 0 {
			t.Fatal("lazy cancelledHistFpFn must not run eagerly")
		}
		resolved := meta.resolveCancelledHistFp()
		wantHist := computeMessagesSliceFingerprint(messages)
		if resolved != wantHist || calls.Load() != 1 {
			t.Fatalf("lazy cancelledHistFp mismatch: got %q want %q (calls=%d)", resolved, wantHist, calls.Load())
		}
	})

	t.Run("PersistUsageBatchVersionGuardAndRODBReuse", func(t *testing.T) {
		dir := t.TempDir()
		st := &StateStore{}
		st.Init(filepath.Join(dir, "gateway.db"))
		defer st.Close()

		v0 := st.Version()
		n, err := st.persistUsageBatchWithForkPurge("sid-empty", cursorMeta{dbSize: 100, dbMtimeNs: 1, lastStepIdx: -1}, nil, "", -1)
		if err != nil || n != 0 {
			t.Fatalf("empty persist failed: n=%d err=%v", n, err)
		}
		if st.Version() != v0 {
			t.Fatalf("empty usage batch without fork purge must not bump store version (%d -> %d)", v0, st.Version())
		}

		step := ParsedModelStep{
			SessionID:      "sid-nonempty",
			StepIdx:        3,
			SourceCategory: "CLAUDE_GW",
			TsSec:          time.Now().Unix(),
			ModelID:        "gemini-4-neon",
			ModelTier:      "xhigh",
			RawIn:          1000,
			RawCr:          9000,
			OutTok:         200,
		}
		n, err = st.persistUsageBatchWithForkPurge("sid-nonempty", cursorMeta{dbSize: 200, dbMtimeNs: 2, lastStepIdx: 3}, []ParsedModelStep{step}, "", -1)
		if err != nil || n != 1 {
			t.Fatalf("non-empty persist failed: n=%d err=%v", n, err)
		}
		if st.Version() != v0+1 {
			t.Fatalf("non-empty usage batch must bump store version (%d -> %d)", v0, st.Version())
		}

		// Verify QueryStepTierAndMaxIdxAfterDB parity and SyncUsageStepsFromDirWithRODB reuse.
		convDir := filepath.Join(dir, "convs")
		if err := os.MkdirAll(convDir, 0755); err != nil {
			t.Fatal(err)
		}
		sid := "11111111-2222-3333-4444-555555555555"
		sessDBPath := filepath.Join(convDir, sid+".db")
		sdb, err := sql.Open("sqlite", sqliteFileURI(sessDBPath, "_pragma=journal_mode(WAL)"))
		if err != nil {
			t.Fatal(err)
		}
		_, err = sdb.Exec(`CREATE TABLE steps (idx INTEGER PRIMARY KEY, step_type INTEGER, metadata BLOB, step_payload BLOB);
CREATE TABLE gen_metadata (idx INTEGER PRIMARY KEY, data BLOB);`)
		if err != nil {
			t.Fatal(err)
		}
		// Build a valid step metadata blob with model enum 1039 (gemini-4-neon xhigh or known enum) & tokens.
		// Field 9 (length-delimited usage): field 1 = modelEnum (1037 = gemini-4-carbon high), field 2 = inTok (500), field 3 = outTok (120), field 5 = crTok (4500).
		encodeVarint := func(v uint64) []byte {
			var out []byte
			for v >= 0x80 {
				out = append(out, byte(v)|0x80)
				v >>= 7
			}
			return append(out, byte(v))
		}
		var usageSub []byte
		usageSub = append(usageSub, 0x08) // field 1 varint
		usageSub = append(usageSub, encodeVarint(1037)...)
		usageSub = append(usageSub, 0x10) // field 2 varint
		usageSub = append(usageSub, encodeVarint(500)...)
		usageSub = append(usageSub, 0x18) // field 3 varint
		usageSub = append(usageSub, encodeVarint(120)...)
		usageSub = append(usageSub, 0x28) // field 5 varint
		usageSub = append(usageSub, encodeVarint(4500)...)
		var metaBlob []byte
		metaBlob = append(metaBlob, (9<<3)|2)
		metaBlob = append(metaBlob, encodeVarint(uint64(len(usageSub)))...)
		metaBlob = append(metaBlob, usageSub...)

		if _, err := sdb.Exec(`INSERT INTO steps (idx, step_type, metadata, step_payload) VALUES (1, 10, ?, ?), (5, 15, ?, ?)`, metaBlob, []byte("non-model"), metaBlob, []byte("payload")); err != nil {
			t.Fatal(err)
		}
		_ = sdb.Close()

		roDB, err := openReadOnlySQLite(sessDBPath)
		if err != nil {
			t.Fatal(err)
		}
		defer roDB.Close()

		fullSteps, fullOK := gwstore.QueryModelStepsFromDB(roDB, sid, "HOST", map[string]bool{sid: true}, -1, time.Now().Unix())
		tierFast, maxFast, fastOK := gwstore.QueryStepTierAndMaxIdxAfterDB(roDB, -1)
		if !fullOK || !fastOK || len(fullSteps) != 1 {
			t.Fatalf("parity check failed: fullOK=%v fastOK=%v len=%d", fullOK, fastOK, len(fullSteps))
		}
		if tierFast != fullSteps[0].ModelTier || maxFast != fullSteps[0].StepIdx {
			t.Fatalf("QueryStepTierAndMaxIdxAfterDB mismatch: got (%q, %d) want (%q, %d)", tierFast, maxFast, fullSteps[0].ModelTier, fullSteps[0].StepIdx)
		}

		var roLookups atomic.Int32
		ingested := st.SyncUsageStepsFromDirWithRODB(convDir, map[string]bool{sid: true}, func(gotSid string) *sql.DB {
			if strings.EqualFold(gotSid, sid) {
				roLookups.Add(1)
				return roDB
			}
			return nil
		})
		if ingested != 1 || roLookups.Load() != 1 {
			t.Fatalf("expected SyncUsageStepsFromDirWithRODB to ingest 1 step using roDB lookup (ingested=%d lookups=%d)", ingested, roLookups.Load())
		}
	})

	t.Run("ConversationHotWALReductionAndFallback", func(t *testing.T) {
		dir := t.TempDir()
		dbPath := filepath.Join(dir, "gateway.db")
		st := &StateStore{}
		st.Init(dbPath)
		defer st.Close()

		// Build a realistic ~200KB tool catalog + system prompt session.
		bigTools := make([]ToolDef, 25)
		pad := strings.Repeat("schema_doc_token_", 200)
		for i := range bigTools {
			bigTools[i] = ToolDef{
				Name:        fmt.Sprintf("tool_%02d", i),
				McpName:     fmt.Sprintf("ws_tool_%02d_1234567890ab", i),
				Description: fmt.Sprintf("Tool %d %s", i, pad),
				Parameters:  map[string]any{"type": "object", "properties": map[string]any{"arg": map[string]any{"type": "string", "description": pad}}},
			}
		}
		bigSys := strings.Repeat("Workspace rule line for cache testing.\n", 800)

		s := NewAcpProjectSession("conv-hot-test", dir, bigSys)
		s.SessionID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
		s.ModelID = "gemini-4-neon"
		s.ThoughtLevel = "xhigh"
		s.InitialSystemText = bigSys
		s.InitialToolsCatalog = append([]ToolDef(nil), bigTools...)
		s.ToolsCatalog = append([]ToolDef(nil), bigTools...)
		s.ToolFingerprint = toolCatalogFingerprint(bigTools)
		s.HistoryFingerprint = "hist-fp-1"
		s.ClientID = "claude-code"
		s.ClientLabel = "Claude Code"
		s.Protocol = "Anthropic"
		s.MessageCount = 2
		s.SessionReuses = 0
		s.LastCachedReadTokens = 10000
		s.LastSeenDbStepIdx = 1
		s.LastSeenGenMetaIdx = 1

		// Cold turn 1: full upsert into conversations + conversation_hot.
		st.InsertTurnAndUpsertSession(s, TurnHistoryEntry{
			TurnIndex:     1,
			CompletedAtMs: time.Now().UnixMilli(),
			SessionID:     s.SessionID,
			Mode:          "cold_start",
			Model:         s.ModelID,
			ThoughtLevel:  s.ThoughtLevel,
			StopReason:    "tool_use",
		})
		// Checkpoint WAL and disable auto-checkpoint so WAL file size measures exact total bytes written across 20 turns.
		// Note: close readDB temporarily so TRUNCATE checkpoint resets WAL file to 0 bytes before the 20-turn window.
		st.mu.Lock()
		_, _ = st.db.Exec(`PRAGMA wal_autocheckpoint = 0`)
		_, _ = st.db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`)
		st.mu.Unlock()
		var baseWalSize int64
		if wi, errW := os.Stat(dbPath + "-wal"); errW == nil {
			baseWalSize = wi.Size()
		}

		// Run 20 hot turns with unchanged static blobs, varying state/counters like real production turns.
		states := []string{"BusyPrompting", "ParkedWaitingTool", "LiveIdle"}
		for i := 2; i <= 21; i++ {
			s.mu.Lock()
			s.MessageCount = i * 2
			s.SessionReuses = i - 1
			s.LastCachedReadTokens = 10000 + i*1000
			s.LastSeenDbStepIdx = i
			s.LastSeenGenMetaIdx = i
			s.LastUsedAt = time.Now().Add(time.Duration(i) * time.Second)
			s.mu.Unlock()
			snap, ok := captureSessionSnapshot(s)
			if !ok {
				t.Fatal("captureSessionSnapshot failed")
			}
			snap.State = states[i%len(states)]
			st.InsertTurnWithSnapshot(snap, true, s.ConvKey, TurnHistoryEntry{
				TurnIndex:     i,
				CompletedAtMs: time.Now().UnixMilli() + int64(i),
				SessionID:     s.SessionID,
				Mode:          "parked_mcp_resume",
				Model:         s.ModelID,
				ThoughtLevel:  s.ThoughtLevel,
				StopReason:    "tool_use",
			})
		}

		walInfo, err := os.Stat(dbPath + "-wal")
		if err != nil {
			t.Fatalf("stat wal: %v", err)
		}
		hotDelta := walInfo.Size() - baseWalSize
		if baseWalSize == 0 {
			hotDelta = walInfo.Size()
		}
		// Prove conversations table was NOT rewritten on hot turns (msg_count stays at cold turn 1's 2)
		// while conversation_hot has msg_count=42.
		var baseMsgCnt, hotMsgCnt int
		st.mu.Lock()
		_ = st.db.QueryRow(`SELECT msg_count FROM conversations WHERE conv_key = ?`, "conv-hot-test").Scan(&baseMsgCnt)
		_ = st.db.QueryRow(`SELECT msg_count FROM conversation_hot WHERE conv_key = ?`, "conv-hot-test").Scan(&hotMsgCnt)
		st.mu.Unlock()
		if baseMsgCnt != 2 || hotMsgCnt != 42 {
			t.Fatalf("expected conversations.msg_count=2 (untouched) and conversation_hot.msg_count=42, got base=%d hot=%d", baseMsgCnt, hotMsgCnt)
		}
		// Compare against 20 full-blob upserts on a second DB to verify >90% WAL write reduction.
		fullDBPath := filepath.Join(dir, "gateway_full.db")
		stFull := &StateStore{}
		stFull.Init(fullDBPath)
		defer stFull.Close()
		snapFull, _ := captureSessionSnapshot(s)
		snapFull.BlobGen = 0 // Force full upsertConversationSQL every turn
		stFull.InsertTurnWithSnapshot(snapFull, true, s.ConvKey, TurnHistoryEntry{TurnIndex: 1, CompletedAtMs: time.Now().UnixMilli(), SessionID: s.SessionID, Mode: "cold_start", Model: s.ModelID, ThoughtLevel: s.ThoughtLevel, StopReason: "tool_use"})
		stFull.mu.Lock()
		_, _ = stFull.db.Exec(`PRAGMA wal_autocheckpoint = 0`)
		_, _ = stFull.db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`)
		stFull.mu.Unlock()
		for i := 2; i <= 21; i++ {
			snapFull.MsgCount = i * 2
			snapFull.SessionReuses = i - 1
			snapFull.LastCachedTok = 10000 + i*1000
			snapFull.LastDbStepIdx = i
			snapFull.LastGenMetaIdx = i
			snapFull.State = states[i%len(states)]
			snapFull.UpdatedAtMs = time.Now().UnixMilli() + int64(i)*1000
			stFull.InsertTurnWithSnapshot(snapFull, true, s.ConvKey, TurnHistoryEntry{TurnIndex: i, CompletedAtMs: time.Now().UnixMilli() + int64(i), SessionID: s.SessionID, Mode: "parked_mcp_resume", Model: s.ModelID, ThoughtLevel: s.ThoughtLevel, StopReason: "tool_use"})
		}
		fullWalInfo, err := os.Stat(fullDBPath + "-wal")
		if err != nil {
			t.Fatalf("stat full wal: %v", err)
		}
		t.Logf("20-turn WAL size: conversation_hot=%d bytes (delta=%d) vs full_upsert=%d bytes (%.1f%% reduction)",
			walInfo.Size(), hotDelta, fullWalInfo.Size(), (1.0-float64(walInfo.Size())/float64(fullWalInfo.Size()))*100)
		if walInfo.Size()*10 >= fullWalInfo.Size() {
			t.Fatalf("expected >90%% WAL write reduction: hot=%d vs full=%d", walInfo.Size(), fullWalInfo.Size())
		}

		// Verify RestoreSingleSession merges hot counters with full blobs.
		restored := st.RestoreSingleSession("conv-hot-test")
		if restored == nil {
			t.Fatal("RestoreSingleSession returned nil")
		}
		if restored.MessageCount != 42 || restored.SessionReuses != 20 || restored.LastCachedReadTokens != 31000 ||
			restored.LastSeenDbStepIdx != 21 || restored.LastSeenGenMetaIdx != 21 {
			t.Fatalf("restored hot counters mismatch: msg=%d reuses=%d cached=%d dbIdx=%d genIdx=%d",
				restored.MessageCount, restored.SessionReuses, restored.LastCachedReadTokens,
				restored.LastSeenDbStepIdx, restored.LastSeenGenMetaIdx)
		}
		if len(restored.InitialToolsCatalog) != len(bigTools) || len(restored.ToolsCatalog) != len(bigTools) || restored.SystemText != bigSys {
			t.Fatalf("restored blobs mismatch: initTools=%d curTools=%d sysLen=%d",
				len(restored.InitialToolsCatalog), len(restored.ToolsCatalog), len(restored.SystemText))
		}

		// Verify Prometheus totals reflect hot session_reuses (20).
		_, _, _, reusesTot, _ := st.queryPrometheusTotals()
		if reusesTot != 20 {
			t.Fatalf("expected queryPrometheusTotals reuses=20, got %d", reusesTot)
		}

		// Verify fallback on RowsAffected==0 (simulate external delete of conversation_hot).
		st.mu.Lock()
		_, _ = st.db.Exec(`DELETE FROM conversation_hot WHERE conv_key = ?`, "conv-hot-test")
		st.mu.Unlock()
		s.mu.Lock()
		s.MessageCount = 44
		s.SessionReuses = 21
		s.mu.Unlock()
		st.UpsertSession(s)
		restored2 := st.RestoreSingleSession("conv-hot-test")
		if restored2 == nil || restored2.MessageCount != 44 || restored2.SessionReuses != 21 {
			t.Fatalf("expected fallback full upsert after missing conversation_hot row, got %+v", restored2)
		}

		// Verify dirty blob (SystemText/ThoughtLevel change) bumps BlobGen and persists to conversations.
		s.mu.Lock()
		s.ThoughtLevel = "high"
		s.SystemText = bigSys + "\nUpdated rule."
		s.mu.Unlock()
		st.UpsertSession(s)
		var dbTier string
		st.mu.Lock()
		err = st.db.QueryRow(`SELECT thought_level FROM conversations WHERE conv_key = ?`, "conv-hot-test").Scan(&dbTier)
		st.mu.Unlock()
		if err != nil || dbTier != "high" {
			t.Fatalf("expected dirty blob change to persist thought_level='high' in conversations, got %q (err=%v)", dbTier, err)
		}
	})
}
