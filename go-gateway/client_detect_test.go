package main

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDetectClientIdentityAndHistoricalBackfill(t *testing.T) {
	t.Run("DSH via UA or prompt or tools", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
		ci := DetectClientIdentity(r, map[string]any{}, "fp:dsh1", "You are an interactive coding agent.\nTokens prefixed with @ are paths the user explicitly referenced.", "", []any{
			map[string]any{"function": map[string]any{"name": "create_goal"}},
			map[string]any{"function": map[string]any{"name": "get_goal"}},
		})
		if ci.ID != "dsh" || ci.Label != "DSH" || ci.Protocol != "Chat" || !ci.Known {
			t.Fatalf("unexpected DSH identity: %+v", ci)
		}
	})

	t.Run("Legacy bridge helpers dsh_call_tool do NOT misclassify Claude Code or Pi as DSH", func(t *testing.T) {
		initToolsClaude := `[{"name":"dsh_call_tool"},{"name":"dsh_await_result"},{"name":"ws_call_tool"},{"name":"TodoWrite"},{"name":"Bash"}]`
		ciClaude := InferHistoricalClientIdentity("sid:09e40bab-1111-2222-3333-444455556666", "# System\nFollow project instructions.", initToolsClaude, "")
		if ciClaude.ID != "claude_code" || ciClaude.Label != "Claude Code" {
			t.Fatalf("expected Claude Code despite legacy dsh_call_tool helper, got %+v", ciClaude)
		}

		initToolsPi := `[{"name":"dsh_call_tool"},{"name":"dsh_await_result"},{"name":"read"},{"name":"bash"}]`
		ciPi := InferHistoricalClientIdentity("fp:c3dddbb1", "# Pi 的全局规则\nUse tools carefully.", initToolsPi, "")
		if ciPi.ID != "pi" || ciPi.Label != "Pi" {
			t.Fatalf("expected Pi despite legacy dsh_call_tool helper, got %+v", ciPi)
		}
	})

	t.Run("Claude Code raw prompt detected before cleanroom rewrite", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
		rawSys := "You are Claude Code, Anthropic's official CLI for Claude."
		cleanSys := "You are the Workspace Agent, an interactive CLI coding agent."
		ci := DetectClientIdentity(r, map[string]any{}, "sid:cc-1", rawSys, cleanSys, nil)
		if ci.ID != "claude_code" || ci.Label != "Claude Code" || ci.Protocol != "Messages" {
			t.Fatalf("unexpected Claude Code identity: %+v", ci)
		}
	})

	t.Run("Codex via Responses endpoint and exec_command tools even with goal MCP tools", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
		rawTools := []any{
			map[string]any{"name": "exec_command"},
			map[string]any{"name": "write_stdin"},
			map[string]any{"name": "create_goal"},
			map[string]any{"name": "get_goal"},
		}
		ci := DetectClientIdentity(r, map[string]any{}, "fp:codex-1", "Skill note (DeepSeek Harness).", "", rawTools)
		if ci.ID != "codex" || ci.Label != "Codex" || ci.Protocol != "Responses" {
			t.Fatalf("unexpected Codex identity: %+v", ci)
		}
	})

	t.Run("GenericAgent overrides proxy-spoofed Codex header on Chat endpoint", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
		r.Header.Set("Originator", "codex_exec")
		r.Header.Set("User-Agent", "codex_exec/0.139.0 (Windows 10.0.26200; x86_64)")
		rawTools := []any{
			map[string]any{"function": map[string]any{"name": "code_run"}},
			map[string]any{"function": map[string]any{"name": "file_read"}},
			map[string]any{"function": map[string]any{"name": "file_patch"}},
			map[string]any{"function": map[string]any{"name": "update_working_checkpoint"}},
		}
		ci := DetectClientIdentity(r, map[string]any{}, "fp:ga1", "cwd = C:\\Users\\dev\\.t3\\tools\\ga-platform\\current\\temp\n<t3_code_instructions>", "", rawTools)
		if ci.ID != "genericagent" || ci.Label != "GenericAgent" || ci.Protocol != "Chat" || !ci.Known {
			t.Fatalf("expected GenericAgent identity over spoofed codex_exec header, got %+v", ci)
		}
	})

		t.Run("Pi via x-pi-session-id header", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
		r.Header.Set("x-pi-session-id", "pi-sess-42")
		ci := DetectClientIdentity(r, map[string]any{}, "sid:pi-sess-42", "", "", nil)
		if ci.ID != "pi" || ci.Label != "Pi" || ci.Protocol != "Messages" {
			t.Fatalf("unexpected Pi identity: %+v", ci)
		}
	})

	t.Run("Synthetic probe key and generic protocol fallback upgrade rules", func(t *testing.T) {
		rProbe := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
		ciProbe := DetectClientIdentity(rProbe, map[string]any{}, "sid:prod-e2e-flash", "", "", nil)
		if ciProbe.ID != "probe" || ciProbe.Label != "E2E 探针" {
			t.Fatalf("expected probe identity, got %+v", ciProbe)
		}

		rGen := httptest.NewRequest(http.MethodPost, "/v1beta/models/gemini-4-carbon:generateContent", nil)
		ciGen := DetectClientIdentity(rGen, map[string]any{}, "fp:gen", "Say hello", "", nil)
		if ciGen.ID != "gemini_api" || ciGen.Label != "Gemini API" || ciGen.Protocol != "Gemini" || ciGen.Known {
			t.Fatalf("expected generic Gemini API fallback, got %+v", ciGen)
		}

		sess := NewAcpProjectSession("fp:upgrade", DefaultCwd, "")
		sess.mu.Lock()
		sess.setClientIdentityLocked(ClientIdentity{ID: "anthropic_api", Label: "Messages API", Protocol: "Messages", Known: false})
		sess.setClientIdentityLocked(ClientIdentity{ID: "openai_chat", Label: "Chat API", Protocol: "Chat", Known: false})
		if sess.ClientID != "openai_chat" || sess.Protocol != "Chat" {
			t.Fatalf("expected generic fallback protocol to update to Chat, got id=%s proto=%s", sess.ClientID, sess.Protocol)
		}
		sess.setClientIdentityLocked(ClientIdentity{ID: "claude_code", Label: "Claude Code", Protocol: "Messages", Known: true})
		// Subsequent generic fallback must NOT overwrite known specific client.
		sess.setClientIdentityLocked(ClientIdentity{ID: "anthropic_api", Label: "Messages API", Protocol: "Messages", Known: false})
		gotID, gotLabel := sess.ClientID, sess.ClientLabel
		sess.mu.Unlock()
		if gotID != "claude_code" || gotLabel != "Claude Code" {
			t.Fatalf("specific client overwritten by generic fallback: id=%s label=%s", gotID, gotLabel)
		}
	})
}

func TestExtractExplicitSessionHeaderVariants(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	r.Header.Set("x-pi-session-id", "pi-abc-123")
	sid, _ := extractExplicitSessionAndAgent(r, map[string]any{})
	if sid != "pi-abc-123" {
		t.Fatalf("expected x-pi-session-id extraction, got %q", sid)
	}

	r2 := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	sid2, _ := extractExplicitSessionAndAgent(r2, map[string]any{
		"user": "acct_99_session_a095e1f8-6328-424d-bf80-e6b9070b1a99",
	})
	if sid2 != "a095e1f8-6328-424d-bf80-e6b9070b1a99" {
		t.Fatalf("expected session UUID from body.user, got %q", sid2)
	}
}

func TestCancelledTurnAdvancesGenMetadataWatermark(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("AGY_CONV_DIR", tmp)
	initConfig()
	t.Cleanup(func() { initConfig() })

	sid := "09e40bab-aaaa-bbbb-cccc-ddddeeeeffff"
	dbPath := filepath.Join(tmp, sid+".db")

	// idx=0: aborted/cancelled non-streaming turn (90K uncached tokens, 300s stream duration)
	blobCancelled := buildTestGenMetadataBlob(1427, 90000, 3500, 0, 2000, 1500, 90000, 983040, 45.0, 255.0)
	writeTestGenMetadataSQLite(t, dbPath, 0, blobCancelled)

	sess := NewAcpProjectSession("sid:cancel-test", DefaultCwd, "")
	sess.SessionID = sid

	// Simulate cancelled turn advancing watermarks
	sess.advanceUsageWatermarks(sid)
	if sess.LastSeenGenMetaIdx != 0 {
		t.Fatalf("expected LastSeenGenMetaIdx=0 after cancelled turn, got %d", sess.LastSeenGenMetaIdx)
	}

	// idx=1: subsequent healthy turn (2K input, 88K cached, 400 output, 2.0s stream)
	blobNext := buildTestGenMetadataBlob(1427, 2000, 400, 88000, 50, 350, 90000, 983040, 2.5, 2.0)
	writeTestGenMetadataSQLite(t, dbPath, 1, blobNext)

	u := sess.normalizeTurnUsage(nil, false, 200, 800)
	if u.Generations != 1 {
		t.Fatalf("expected only 1 generation counted after cancelled watermark advance, got %d", u.Generations)
	}
	if u.OutputTokens != 400 {
		t.Fatalf("expected OutputTokens=400 (excluding cancelled 3500), got %d", u.OutputTokens)
	}
	if u.StreamMs < 1900 || u.StreamMs > 2100 {
		t.Fatalf("expected StreamMs ~2000ms (excluding cancelled 255000ms), got %d", u.StreamMs)
	}
}

func TestResolveTurnTtftAndComputeStepTiming(t *testing.T) {
	// 1. Tool-call / non-streaming turn where firstDeltaNs == 0 and FirstChunkAt was stamped at tool arrival (end of decode)
	t0 := time.Now().Add(-20120 * time.Millisecond)
	res := TurnResult{
		StopReason:   "tool_use",
		FirstChunkAt: t0.Add(20101 * time.Millisecond),
		TtftMs:       20101,
		Usage: &TurnUsage{
			GenTtftMs: 17150,
			StreamMs:  2926,
			DecodeTps: 120.0,
		},
	}
	ttft := resolveTurnTtftMs(t0, 20120, 0, res)
	if ttft != 17194 {
		t.Fatalf("expected derived ttft=17194 (20120 - 2926), got %d", ttft)
	}
	if ttft+res.Usage.StreamMs > 20120 {
		t.Fatalf("invariant violated: ttft(%d) + streamMs(%d) > elapsedMs(20120)", ttft, res.Usage.StreamMs)
	}

	// 2. Hydrogen & Flash fast streaming decode (350-950 tok/s, <= 1200 tok/s) must be preserved
	_, normStream, _, _, decFast := computeStepTiming(2100, 1011, 3111, 450)
	if normStream != 1011 || decFast < 444.0 || decFast > 446.0 {
		t.Fatalf("expected fast Hydrogen decode ~445.1 tok/s preserved, got stream=%d decodeTps=%v", normStream, decFast)
	}
	_, normFlash, _, _, decFlash := computeStepTiming(7513, 1977, 9490, 1642)
	if normFlash != 1977 || decFlash < 830.0 || decFlash > 831.0 {
		t.Fatalf("expected fast Gemini 3.8 Flash decode ~830.6 tok/s preserved, got stream=%d decodeTps=%v", normFlash, decFlash)
	}

	// 3. Long prefill (56s or 12s TTFT) followed by genuine 8.9s or 1.0s stream must be preserved
	_, normStreamLong, _, _, decLong := computeStepTiming(56000, 8900, 64900, 325)
	if normStreamLong != 8900 || decLong < 36.4 || decLong > 36.6 {
		t.Fatalf("expected 8.9s stream after 56s TTFT preserved (~36.5 tok/s), got stream=%d decodeTps=%v", normStreamLong, decLong)
	}
	_, normShortStream, _, _, decShortAfterPrefill := computeStepTiming(12000, 1000, 13000, 120)
	if normShortStream != 1000 || decShortAfterPrefill != 120.0 {
		t.Fatalf("expected 1.0s stream after 12s TTFT preserved (120.0 tok/s), got stream=%d decodeTps=%v", normShortStream, decShortAfterPrefill)
	}

	// 4. Non-streaming single-chunk burst (3070 tokens in 311ms = 9871 tok/s) must still return 0.0
	_, _, _, _, decBurst := computeStepTiming(12000, 311, 12311, 3070)
	if decBurst != 0 {
		t.Fatalf("expected implausible 9871 tok/s burst zeroed, got %v", decBurst)
	}
}

func TestStateStoreClientBackfillAndTimingRepair(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "gateway.db")

	// Seed a pre-migration database with empty client columns and an inflated stream_ms > elapsed_ms row.
	var st StateStore
	st.Init(dbPath)
	if !st.IsReady() {
		t.Fatalf("store failed to init")
	}

	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)", filepath.ToSlash(dbPath))
	rawDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open raw db: %v", err)
	}
	nowMs := time.Now().UnixMilli()
	_, err = rawDB.Exec(`
INSERT INTO conversations (
	conv_key, agy_session_id, tenant_key, cwd, model_id, thought_level,
	tool_fp, init_tools_json, cur_tools_json, system_text, msg_count, history_fp,
	last_cached_tok, last_db_step_idx, last_gen_meta_idx, client_id, client_label, protocol,
	session_reuses, compaction_rollovers, state, created_at_ms, updated_at_ms
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', '', '', 3, 0, 'LiveIdle', ?, ?)
`,
		"fp:a095e1f8", "a095e1f8-6328-424d-bf80-e6b9070b1a99", "tk1", "/tmp", "gemini-4-carbon", "medium",
		"tfp1", `[{"name":"create_goal"},{"name":"get_goal"}]`, "", "Tokens prefixed with @ are paths the user explicitly referenced.",
		4, "hfp1", 14336, 5, 3, nowMs-10000, nowMs,
	)
	if err != nil {
		t.Fatalf("seed conversation: %v", err)
	}
	_, err = rawDB.Exec(`
INSERT INTO turns (
	conv_key, agy_session_id, turn_idx, mode, model_id, thought_level,
	stop_reason, elapsed_ms, ttft_ms, upstream_ttft_ms, stream_ms, tps, decode_tps,
	in_tok, cached_tok, out_tok, raw_in_tok, raw_cr_tok, thinking_tok, steps_in_prompt,
	usage_source, cache_hit_rate, completed_at_ms, served_model, cache_miss, client_id, client_label, protocol
) VALUES (?, ?, 11, 'sticky_delta', 'gemini-4-carbon', 'high', 'end_turn', 138859, 68859, 65000, 1860368, 151.2, 11.3, 628452, 0, 2100, 628452, 0, 500, 7, 'db', '0.0%', ?, '', 'zero', '', '', '')
`,
		"fp:a095e1f8", "a095e1f8-6328-424d-bf80-e6b9070b1a99", nowMs,
	)
	if err != nil {
		t.Fatalf("seed turn: %v", err)
	}
	// Seed an orphan turn whose conv_key was pruned from conversations
	_, err = rawDB.Exec(`
INSERT INTO turns (
	conv_key, agy_session_id, turn_idx, mode, model_id, thought_level,
	stop_reason, elapsed_ms, ttft_ms, upstream_ttft_ms, stream_ms, tps, decode_tps,
	in_tok, cached_tok, out_tok, raw_in_tok, raw_cr_tok, thinking_tok, steps_in_prompt,
	usage_source, cache_hit_rate, completed_at_ms, served_model, cache_miss, client_id, client_label, protocol
) VALUES (?, ?, 1, 'cold_start', 'gemini-3.8-flash', 'low', 'end_turn', 3200, 2900, 2800, 300, 12.0, 0, 1000, 0, 36, 1000, 0, 0, 1, 'db', '0.0%', ?, '', 'cold_start', '', '', '')
`,
		"sid:p19s7-0e4de4:u:ac1e1ad0", "7c62899b-0000-4000-8000-000000000001", nowMs-500,
	)
	if err != nil {
		t.Fatalf("seed orphan turn: %v", err)
	}
	_ = rawDB.Close()
	st.Init("")

	// Re-open store to trigger backfillClientAndTimingLocked()
	var st2 StateStore
	st2.Init(dbPath)
	defer st2.Init("")

	cmap := st2.LookupSessionClientMap()
	meta, ok := cmap["a095e1f8-6328-424d-bf80-e6b9070b1a99"]
	if !ok || meta.ClientID != "dsh" || meta.ClientLabel != "DSH" || meta.Protocol != "Chat" {
		t.Fatalf("expected DSH in LookupSessionClientMap, got ok=%v meta=%+v", ok, meta)
	}
	orphanMeta, ok := cmap["7c62899b-0000-4000-8000-000000000001"]
	if !ok || orphanMeta.ClientID != "probe" || orphanMeta.ClientLabel != "E2E 探针" {
		t.Fatalf("expected orphan probe turn backfilled in LookupSessionClientMap, got ok=%v meta=%+v", ok, orphanMeta)
	}

	gs := NewGatewayServer()
	defer gs.Close()
	if n := st2.RestoreDetachedSessions(gs); n != 1 {
		t.Fatalf("expected 1 restored session, got %d", n)
	}
	sess := gs.sessions["fp:a095e1f8"]
	if sess == nil {
		t.Fatalf("missing restored session")
	}
	if sess.ClientID != "dsh" || sess.ClientLabel != "DSH" || sess.LastSeenGenMetaIdx != 3 {
		t.Fatalf("unexpected restored session fields: client=%s/%s lastGenIdx=%d", sess.ClientID, sess.ClientLabel, sess.LastSeenGenMetaIdx)
	}
	if len(sess.TurnHistory) != 1 {
		t.Fatalf("expected 1 restored turn, got %d", len(sess.TurnHistory))
	}
	th := sess.TurnHistory[0]
	if th.StreamMs != 70000 || th.StreamMs > th.ElapsedMs {
		t.Fatalf("expected inflated stream_ms repaired to 70000 (138859 - 68859), got %d", th.StreamMs)
	}
	if th.DecodeTps != 30.0 {
		t.Fatalf("expected recomputed decode_tps=30.0 (2100 tok / 70s), got %v", th.DecodeTps)
	}
	if th.ClientID != "dsh" || th.ClientLabel != "DSH" || th.UpstreamTtftMs != 65000 {
		t.Fatalf("unexpected restored turn metadata: %+v", th)
	}
}

func TestCarbonHighChronicZeroCacheArmsInPlaceMediumFallback(t *testing.T) {
	sess := NewAcpProjectSession("sid:carbon-high", DefaultCwd, "")
	sess.SessionID = "sid-live-carbon-1"
	sess.ModelID = "gemini-4-carbon"
	sess.ThoughtLevel = "high"

	// Turn 1: cold_start (normal, sets baseline history)
	sess.logTurnStats("cold_start", 12000, TurnResult{
		StopReason: "end_turn",
		TtftMs:     10000,
		Usage: &TurnUsage{
			InputTokens:            60000,
			CachedReadTokens:       0,
			ContextInputTokens:     60000,
			ContextCachedTokens:    0,
			RawUncachedInputTokens: 60000,
			OutputTokens:           300,
			StepsInPrompt:          1,
			UsageSource:            "db",
		},
	})
	if sess.capacityFallbackSid != "" {
		t.Fatalf("cold_start should not arm fallback")
	}

	// Turn 2 & Turn 3: sticky_delta with 0% cache at 65K-70K context and 45s TTFT
	for i, ctxTok := range []int{65000, 70000} {
		sess.logTurnStats("sticky_delta", 50000, TurnResult{
			StopReason: "end_turn",
			TtftMs:     45000,
			Usage: &TurnUsage{
				InputTokens:            ctxTok,
				CachedReadTokens:       0,
				ContextInputTokens:     ctxTok,
				ContextCachedTokens:    0,
				RawUncachedInputTokens: ctxTok,
				OutputTokens:           250,
				StepsInPrompt:          1,
				UsageSource:            "db",
			},
		})
		if sess.ZeroCacheStreak != 0 {
			t.Fatalf("turn %d: ZeroCacheStreak below 80K must remain 0, got %d", i+2, sess.ZeroCacheStreak)
		}
	}
	if sess.capacityFallbackSid != "sid-live-carbon-1" {
		t.Fatalf("expected capacityFallbackSid armed after 2 slow carbon-high zero-cache turns, got %q", sess.capacityFallbackSid)
	}

	// Explicit client tier overrides capacityFallbackSid instead of forcing medium
	sess.SessionID = ""
	err := sess.applyModelSelection(ModelSelection{
		Model:        AgyModels[0],
		Tier:         "high",
		ExplicitTier: true,
	})
	if err != nil {
		t.Fatalf("applyModelSelection explicit: %v", err)
	}
	if sess.capacityFallbackSid != "" || sess.ThoughtLevel != "high" {
		t.Fatalf("explicit tier should clear capacityFallbackSid and keep high, got fallback=%q tier=%q", sess.capacityFallbackSid, sess.ThoughtLevel)
	}
}

func TestDashboardUIBuildIDHandshake(t *testing.T) {
	buildID := DashboardBuildID()
	if len(buildID) != 12 {
		t.Fatalf("expected 12-char hex DashboardBuildID, got %q", buildID)
	}
	html := RenderDashboardHTML()
	wantConst := `const UI_BUILD_ID = "` + buildID + `";`
	if !strings.Contains(html, wantConst) {
		t.Fatalf("RenderDashboardHTML missing %s", wantConst)
	}
	if strings.Contains(html, "/*__UI_BUILD__*/") {
		t.Fatalf("RenderDashboardHTML still contains unreplaced __UI_BUILD__ placeholder")
	}
	gs := NewGatewayServer()
	defer gs.Close()
	payload := BuildDashboardPayload(gs, true)
	gw, _ := payload["gateway"].(map[string]any)
	if got, _ := gw["uiBuildId"].(string); got != buildID {
		t.Fatalf("expected gateway.uiBuildId=%q, got %q", buildID, got)
	}
	if !strings.Contains(html, "对齐 Agy 原生速率") || strings.Contains(html, "建议切换 Hydrogen") {
		t.Fatalf("RenderDashboardHTML should align primary speed to Agy stream tok/s and respect Decision U8 model pinning")
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	stamp := time.Now().Add(-250 * time.Millisecond)
	req = withHttpRequestStart(req, stamp)
	if got, ok := httpRequestStartFromContext(req.Context()); !ok || !got.Equal(stamp) {
		t.Fatalf("expected httpRequestStartFromContext=%v, got ok=%v t=%v", stamp, ok, got)
	}
}

func TestNormalizeTurnUsagePreservesShortAndBurstStreamMs(t *testing.T) {
	prevDB := GatewayStateDB
	t.Cleanup(func() {
		initConfig()
		InitGlobalStore(prevDB)
	})
	tmp := t.TempDir()
	t.Setenv("AGY_CONV_DIR", tmp)
	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "state.db"))
	t.Setenv("AGY_PURE_API_HOME", filepath.Join(tmp, "pure"))
	t.Setenv("AGY_GW_LOG", filepath.Join(tmp, "gateway.log"))
	initConfig()
	InitGlobalStore(filepath.Join(tmp, "state.db"))

	sid := "09e40bab-1234-4000-8000-000000000250"
	dbPath := filepath.Join(tmp, sid+".db")

	// 250ms stream (<400ms decode window -> DecodeTps == 0, but StreamMs == 250ms is authoritative).
	blobShort := buildTestGenMetadataBlob(1427, 12000, 48, 36000, 0, 48, 48000, 983040, 1.0, 0.25)
	writeTestGenMetadataSQLite(t, dbPath, 0, blobShort)

	sess := NewAcpProjectSession("sid:short-stream-tool", tmp, "")
	sess.SessionID = sid
	sess.ModelID = "gemini-4-carbon"
	sess.ThoughtLevel = "low"

	u := sess.normalizeTurnUsage(nil, true, 400, 180)
	if u.UsageSource != "db" {
		t.Fatalf("expected db usage source, got %q", u.UsageSource)
	}
	if u.StreamMs != 250 {
		t.Fatalf("expected StreamMs=250 preserved when DecodeTps=0 (<400ms stream), got %d", u.StreamMs)
	}
	if u.DecodeTps != 0 {
		t.Fatalf("expected DecodeTps=0 on <400ms stream window, got %v", u.DecodeTps)
	}

	// Tool-only turn (firstDeltaNs == 0) where FirstChunkAt was stamped at MCP tools/call (1240ms)
	t0 := time.Now().Add(-1250 * time.Millisecond)
	res := TurnResult{
		StopReason:   "tool_use",
		FirstChunkAt: t0.Add(1240 * time.Millisecond),
		TtftMs:       1240,
		Usage:        u,
	}
	res.TtftMs = resolveTurnTtftMs(t0, 1250, 0, res)
	if res.TtftMs != 1000 {
		t.Fatalf("expected tool-only turn ttft=1000 (1250 - 250), got %d", res.TtftMs)
	}
	sess.logTurnStats("sticky_delta", 1250, res)
	if len(sess.TurnHistory) != 1 {
		t.Fatalf("expected 1 logged turn, got %d", len(sess.TurnHistory))
	}
	th := sess.TurnHistory[0]
	if th.StreamMs != 250 || th.TtftMs != 1000 || th.DecodeTps != 0 {
		t.Fatalf("expected gatewayTurn StreamMs=250 TtftMs=1000 DecodeTps=0, got StreamMs=%d TtftMs=%d DecodeTps=%v", th.StreamMs, th.TtftMs, th.DecodeTps)
	}
}
