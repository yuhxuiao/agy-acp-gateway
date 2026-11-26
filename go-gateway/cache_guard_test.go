package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestLongContextCacheBurnLedger(t *testing.T) {
	beforeCounter := cacheBurnWarningsTotal.Load()

	s := NewAcpProjectSession("sid:test-burn:u:1", "/tmp", "sys")
	s.SessionID = "00000000-0000-4000-8000-000000009999"
	s.ModelID = "gemini-4-carbon"
	s.ThoughtLevel = "medium"

	mkUsage := func(inTok, crTok int, source string) *TurnUsage {
		return &TurnUsage{
			InputTokens:            inTok,
			CachedReadTokens:       crTok,
			OutputTokens:           32,
			RawUncachedInputTokens: inTok,
			RawTpuCachedTokens:     crTok,
			ContextInputTokens:     inTok,
			ContextCachedTokens:    crTok,
			StepsInPrompt:          1,
			UsageSource:            source,
		}
	}

	// 1. Cold start at 245K: never counts toward zero-cache streak.
	u1 := mkUsage(245000, 0, "acp")
	s.logTurnStats("cold_start", 100, TurnResult{StopReason: "end_turn", Usage: u1})
	if s.ZeroCacheStreak != 0 || s.LastCacheWarning != "" || u1.CacheWarning != "" {
		t.Fatalf("cold_start should not increment streak: streak=%d warn=%q", s.ZeroCacheStreak, s.LastCacheWarning)
	}

	// 2. Miss #1 (zero at 250K): streak=1, below threshold (3).
	u2 := mkUsage(250000, 0, "acp")
	s.logTurnStats("sticky_delta", 100, TurnResult{StopReason: "end_turn", Usage: u2})
	if s.ZeroCacheStreak != 1 || s.LastCacheWarning != "" || u2.CacheWarning != "" {
		t.Fatalf("miss #1 unexpected state: streak=%d warn=%q", s.ZeroCacheStreak, s.LastCacheWarning)
	}

	// 3. Genuine prefix_only at 255K: must NOT increment zero-cache streak or uncached counter.
	u3 := mkUsage(240000, 15000, "acp")
	s.logTurnStats("sticky_delta", 100, TurnResult{StopReason: "end_turn", Usage: u3})
	if s.ZeroCacheStreak != 1 || s.ZeroCacheUncachedTokens != 250000 || s.LastCacheWarning != "" || u3.CacheWarning != "" {
		t.Fatalf("prefix_only should not increment streak: streak=%d uncached=%d warn=%q", s.ZeroCacheStreak, s.ZeroCacheUncachedTokens, s.LastCacheWarning)
	}

	// 4. Same-batch split tool relay (usageSource="none", 0 tokens): neither resets nor increments streak.
	uRelay := mkUsage(0, 0, "none")
	s.logTurnStats("parked_mcp_resume", 20, TurnResult{StopReason: "tool_use", Usage: uRelay})
	if s.ZeroCacheStreak != 1 || s.ZeroCacheUncachedTokens != 250000 || s.LastCacheWarning != "" {
		t.Fatalf("split relay altered streak: streak=%d uncached=%d warn=%q", s.ZeroCacheStreak, s.ZeroCacheUncachedTokens, s.LastCacheWarning)
	}

	// 5. Miss #2 (zero at 260K): streak=2, still below threshold (3).
	u4 := mkUsage(260000, 0, "acp")
	s.logTurnStats("sticky_delta", 100, TurnResult{StopReason: "end_turn", Usage: u4})
	if s.ZeroCacheStreak != 2 || s.ZeroCacheUncachedTokens != 510000 || s.LastCacheWarning != "" || u4.CacheWarning != "" {
		t.Fatalf("miss #2 unexpected state: streak=%d uncached=%d warn=%q", s.ZeroCacheStreak, s.ZeroCacheUncachedTokens, s.LastCacheWarning)
	}

	// 5b. Miss #3 (zero at 265K): streak=3 -> triggers CACHE_BURN_WARNING!
	u5 := mkUsage(265000, 0, "acp")
	s.logTurnStats("sticky_delta", 100, TurnResult{StopReason: "end_turn", Usage: u5})
	if s.ZeroCacheStreak != 3 || !strings.Contains(s.LastCacheWarning, "streak=3") || u5.CacheWarning != s.LastCacheWarning {
		t.Fatalf("miss #3 did not emit cache warning: streak=%d last=%q u5=%q", s.ZeroCacheStreak, s.LastCacheWarning, u5.CacheWarning)
	}
	if cacheBurnWarningsTotal.Load() != beforeCounter+1 {
		t.Fatalf("expected cacheBurnWarningsTotal=%d, got %d", beforeCounter+1, cacheBurnWarningsTotal.Load())
	}

	// 6. Steady cache hit at 265K: immediately clears streak and warning.
	uHit := mkUsage(5000, 260000, "acp")
	s.logTurnStats("sticky_delta", 80, TurnResult{StopReason: "end_turn", Usage: uHit})
	if s.ZeroCacheStreak != 0 || s.LastCacheWarning != "" || uHit.CacheWarning != "" {
		t.Fatalf("cache hit did not clear warning: streak=%d last=%q uHit=%q", s.ZeroCacheStreak, s.LastCacheWarning, uHit.CacheWarning)
	}

	// 6b. P0-B batch-delta turn (curIn > slack, but curCr+2500 >= prevTotal) classifies as "" and resets ZeroCacheStreak.
	sBatch := NewAcpProjectSession("sid:test-batch-reset:u:1", "/tmp", "sys")
	sBatch.SessionID = "00000000-0000-4000-8000-000000009996"
	sBatch.ModelID = "gemini-4-carbon"
	sBatch.logTurnStats("cold_start", 100, TurnResult{StopReason: "end_turn", Usage: mkUsage(245000, 0, "acp")})
	sBatch.logTurnStats("sticky_delta", 100, TurnResult{StopReason: "end_turn", Usage: mkUsage(250000, 0, "acp")})
	sBatch.logTurnStats("sticky_delta", 100, TurnResult{StopReason: "end_turn", Usage: mkUsage(255000, 0, "acp")})
	if sBatch.ZeroCacheStreak != 2 {
		t.Fatalf("expected pre-batch streak=2, got %d", sBatch.ZeroCacheStreak)
	}
	sBatch.logTurnStats("sticky_delta", 100, TurnResult{StopReason: "end_turn", Usage: mkUsage(45000, 254000, "acp")})
	if sBatch.ZeroCacheStreak != 0 || sBatch.ZeroCacheUncachedTokens != 0 {
		t.Fatalf("P0-B batch-delta turn should reset streak to 0: streak=%d uncached=%d", sBatch.ZeroCacheStreak, sBatch.ZeroCacheUncachedTokens)
	}

	// 6c. Consecutive genuine prefix_only turns never increment streak or emit CACHE_BURN_WARNING.
	sPrefix := NewAcpProjectSession("sid:test-prefix-only:u:1", "/tmp", "sys")
	sPrefix.SessionID = "00000000-0000-4000-8000-000000009997"
	sPrefix.ModelID = "gemini-4-carbon"
	sPrefix.logTurnStats("cold_start", 100, TurnResult{StopReason: "end_turn", Usage: mkUsage(245000, 0, "acp")})
	for i := 0; i < 4; i++ {
		uP := mkUsage(235000+i*5000, 15000, "acp")
		sPrefix.logTurnStats("sticky_delta", 100, TurnResult{StopReason: "end_turn", Usage: uP})
		if sPrefix.ZeroCacheStreak != 0 || sPrefix.ZeroCacheUncachedTokens != 0 || sPrefix.LastCacheWarning != "" || uP.CacheWarning != "" {
			t.Fatalf("prefix_only turn %d altered burn state: streak=%d uncached=%d warn=%q", i, sPrefix.ZeroCacheStreak, sPrefix.ZeroCacheUncachedTokens, sPrefix.LastCacheWarning)
		}
	}
	if cacheBurnWarningsTotal.Load() != beforeCounter+1 {
		t.Fatalf("prefix_only turns should not increment cacheBurnWarningsTotal: got %d want %d", cacheBurnWarningsTotal.Load(), beforeCounter+1)
	}

	// 7. Short context (< 200K) consecutive zero misses never warn.
	sShort := NewAcpProjectSession("sid:test-short:u:1", "/tmp", "sys")
	sShort.SessionID = "00000000-0000-4000-8000-000000009998"
	sShort.ModelID = "gemini-4-carbon"
	for i := 0; i < 5; i++ {
		mode := "sticky_delta"
		if i == 0 {
			mode = "cold_start"
		}
		u := mkUsage(45000+i*2000, 0, "acp")
		sShort.logTurnStats(mode, 50, TurnResult{StopReason: "end_turn", Usage: u})
		if sShort.ZeroCacheStreak != 0 || sShort.LastCacheWarning != "" || u.CacheWarning != "" {
			t.Fatalf("short context turn %d triggered burn state: streak=%d warn=%q", i, sShort.ZeroCacheStreak, sShort.LastCacheWarning)
		}
	}
}

func TestCacheBurnWarningAcrossProtocolsAndDashboard(t *testing.T) {
	if GlobalEngine != nil {
		GlobalEngine.StopEngine()
	}
	prevDB := GatewayStateDB
	dbPath := filepath.Join(t.TempDir(), "cache_guard_m1.db")
	t.Cleanup(func() {
		if GlobalEngine != nil {
			GlobalEngine.StopEngine()
		}
		InitGlobalStore(prevDB)
	})
	InitGlobalStore(dbPath)

	gs := NewGatewayServer()
	t.Cleanup(gs.Close)
	gwSrv := httptest.NewServer(gs.GatewayOnlyHandler())
	defer gwSrv.Close()
	dashSrv := httptest.NewServer(gs.DashboardOnlyHandler())
	defer dashSrv.Close()

	doJSON := func(method, url, sid string, payload any) (*http.Response, string) {
		var body io.Reader
		if payload != nil {
			b, _ := json.Marshal(payload)
			body = bytes.NewReader(b)
		}
		req, err := http.NewRequest(method, url, body)
		if err != nil {
			t.Fatal(err)
		}
		if payload != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if sid != "" {
			req.Header.Set("x-claude-code-session-id", sid)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return resp, string(raw)
	}

	// Verify AllowedCorsOrigins exposes X-Agy-Cache-Warning
	AllowedCorsOrigins[gwSrv.URL] = true
	t.Cleanup(func() { delete(AllowedCorsOrigins, gwSrv.URL) })
	corsReq, _ := http.NewRequest("GET", gwSrv.URL+"/v1/models", nil)
	corsReq.Header.Set("Origin", gwSrv.URL)
	corsResp, err := http.DefaultClient.Do(corsReq)
	if err != nil {
		t.Fatal(err)
	}
	corsResp.Body.Close()
	if !strings.Contains(corsResp.Header.Get("Access-Control-Expose-Headers"), "X-Agy-Cache-Warning") {
		t.Fatalf("CORS expose header missing X-Agy-Cache-Warning: %q", corsResp.Header.Get("Access-Control-Expose-Headers"))
	}

	// Build a growing Anthropic sticky conversation on session "burn-proto"
	sid := "burn-proto"
	sys := "Primary working directory: /tmp"
	msgs := []map[string]any{
		{"role": "user", "content": "[FAKE_ZERO_250K] step 1"},
	}

	// Turn 1: cold_start -> no warning
	r1, out1 := doJSON("POST", gwSrv.URL+"/v1/messages", sid, map[string]any{
		"model": "gemini-4-carbon", "max_tokens": 64, "system": sys, "messages": msgs,
	})
	if r1.StatusCode != 200 || r1.Header.Get("X-Agy-Cache-Warning") != "" {
		t.Fatalf("turn 1 unexpected status/header: %d hdr=%q body=%s", r1.StatusCode, r1.Header.Get("X-Agy-Cache-Warning"), out1)
	}

	// Turn 2 (zero): miss #1 -> streak=1, below threshold (3).
	// Turn 3 (prefix_only): must NOT increment streak (stays 1).
	// Turn 4 (zero): miss #2 -> streak=2, still below threshold (proves Turn 3 did not increment).
	for _, tc := range []struct {
		step int
		tag  string
	}{
		{2, "[FAKE_ZERO_250K] step 2"},
		{3, "[FAKE_PREFIX_250K] step 3"},
		{4, "[FAKE_ZERO_250K] step 4"},
	} {
		msgs = append(msgs,
			map[string]any{"role": "assistant", "content": "ok"},
			map[string]any{"role": "user", "content": tc.tag},
		)
		rn, outN := doJSON("POST", gwSrv.URL+"/v1/messages", sid, map[string]any{
			"model": "gemini-4-carbon", "max_tokens": 64, "system": sys, "messages": msgs,
		})
		if rn.StatusCode != 200 || rn.Header.Get("X-Agy-Cache-Warning") != "" {
			t.Fatalf("turn %d unexpected warning before threshold: %d hdr=%q body=%s", tc.step, rn.StatusCode, rn.Header.Get("X-Agy-Cache-Warning"), outN)
		}
	}

	// Turn 5 (Anthropic non-streaming): miss #3 (zero) -> X-Agy-Cache-Warning emitted!
	msgs = append(msgs,
		map[string]any{"role": "assistant", "content": "ok"},
		map[string]any{"role": "user", "content": "[FAKE_ZERO_250K] step 5"},
	)
	r5warn, out5warn := doJSON("POST", gwSrv.URL+"/v1/messages", sid, map[string]any{
		"model": "gemini-4-carbon", "max_tokens": 64, "system": sys, "messages": msgs,
	})
	if r5warn.StatusCode != 200 || !strings.Contains(r5warn.Header.Get("X-Agy-Cache-Warning"), "streak=3") {
		t.Fatalf("turn 5 Anthropic non-stream missing X-Agy-Cache-Warning: %d hdr=%q body=%s", r5warn.StatusCode, r5warn.Header.Get("X-Agy-Cache-Warning"), out5warn)
	}

	// Turn 6 (Anthropic streaming): pre-warned session flushes X-Agy-Cache-Warning before SSE body!
	msgs = append(msgs,
		map[string]any{"role": "assistant", "content": "ok"},
		map[string]any{"role": "user", "content": "[FAKE_ZERO_250K] step 6 stream"},
	)
	r5, out5 := doJSON("POST", gwSrv.URL+"/v1/messages", sid, map[string]any{
		"model": "gemini-4-carbon", "max_tokens": 64, "stream": true, "system": sys, "messages": msgs,
	})
	if r5.StatusCode != 200 || !strings.Contains(r5.Header.Get("X-Agy-Cache-Warning"), "consecutive-zero-cache") {
		t.Fatalf("turn 6 Anthropic stream missing X-Agy-Cache-Warning: %d hdr=%q body=%s", r5.StatusCode, r5.Header.Get("X-Agy-Cache-Warning"), out5)
	}

	// Verify Dashboard JSON and Prometheus /metrics expose the active warning
	rDash, rawDash := doJSON("GET", dashSrv.URL+"/api/dashboard", "", nil)
	if rDash.StatusCode != 200 {
		t.Fatalf("GET /api/dashboard failed: %d %s", rDash.StatusCode, rawDash)
	}
	var dash map[string]any
	if err := json.Unmarshal([]byte(rawDash), &dash); err != nil {
		t.Fatalf("decode dashboard failed: %v", err)
	}
	cw, _ := dash["cacheWarnings"].([]any)
	if len(cw) != 1 {
		t.Fatalf("expected 1 active cacheWarning in dashboard, got %+v", dash["cacheWarnings"])
	}
	rMet, rawMet := doJSON("GET", dashSrv.URL+"/metrics", "", nil)
	if rMet.StatusCode != 200 || !strings.Contains(rawMet, "agy_gateway_cache_burn_warnings_total ") {
		t.Fatalf("missing prometheus cache burn counter: %d %s", rMet.StatusCode, rawMet)
	}

	// Verify OpenAI Chat Completions, OpenAI Responses, and Gemini v1beta all surface X-Agy-Cache-Warning on the same sticky session
	msgs = append(msgs,
		map[string]any{"role": "assistant", "content": "ok"},
		map[string]any{"role": "user", "content": "[FAKE_ZERO_250K] step 6 chat"},
	)
	chatMsgs := []map[string]any{{"role": "system", "content": sys}}
	chatMsgs = append(chatMsgs, msgs...)
	for _, stream := range []bool{false, true} {
		if stream {
			msgs = append(msgs,
				map[string]any{"role": "assistant", "content": "ok"},
				map[string]any{"role": "user", "content": "[FAKE_ZERO_250K] step 7 chat stream"},
			)
			chatMsgs = append([]map[string]any{{"role": "system", "content": sys}}, msgs...)
		}
		rc, outC := doJSON("POST", gwSrv.URL+"/v1/chat/completions", sid, map[string]any{
			"model": "gemini-4-carbon", "stream": stream, "messages": chatMsgs,
		})
		if rc.StatusCode != 200 || !strings.Contains(rc.Header.Get("X-Agy-Cache-Warning"), "consecutive-zero-cache") {
			t.Fatalf("chat/completions stream=%v missing X-Agy-Cache-Warning: %d hdr=%q body=%s", stream, rc.StatusCode, rc.Header.Get("X-Agy-Cache-Warning"), outC)
		}
	}

	for _, stream := range []bool{false, true} {
		msgs = append(msgs,
			map[string]any{"role": "assistant", "content": "ok"},
			map[string]any{"role": "user", "content": "[FAKE_ZERO_250K] step responses"},
		)
		rr, outR := doJSON("POST", gwSrv.URL+"/v1/responses", sid, map[string]any{
			"model": "gemini-4-carbon", "stream": stream, "instructions": sys, "input": msgs,
		})
		if rr.StatusCode != 200 || !strings.Contains(rr.Header.Get("X-Agy-Cache-Warning"), "consecutive-zero-cache") {
			t.Fatalf("responses stream=%v missing X-Agy-Cache-Warning: %d hdr=%q body=%s", stream, rr.StatusCode, rr.Header.Get("X-Agy-Cache-Warning"), outR)
		}
	}

	toGemContents := func(ms []map[string]any) []map[string]any {
		out := make([]map[string]any, 0, len(ms))
		for _, m := range ms {
			role := "user"
			if m["role"] == "assistant" {
				role = "model"
			}
			out = append(out, map[string]any{
				"role":  role,
				"parts": []map[string]any{{"text": m["content"]}},
			})
		}
		return out
	}
	for _, stream := range []bool{false, true} {
		msgs = append(msgs,
			map[string]any{"role": "assistant", "content": "ok"},
			map[string]any{"role": "user", "content": "[FAKE_ZERO_250K] step gemini"},
		)
		ep := "/v1beta/models/gemini-4-carbon:generateContent"
		if stream {
			ep = "/v1beta/models/gemini-4-carbon:streamGenerateContent?alt=sse"
		}
		rg, outG := doJSON("POST", gwSrv.URL+ep, sid, map[string]any{
			"systemInstruction": map[string]any{"parts": []map[string]any{{"text": sys}}},
			"contents":          toGemContents(msgs),
		})
		if rg.StatusCode != 200 || !strings.Contains(rg.Header.Get("X-Agy-Cache-Warning"), "consecutive-zero-cache") {
			t.Fatalf("v1beta stream=%v missing X-Agy-Cache-Warning: %d hdr=%q body=%s", stream, rg.StatusCode, rg.Header.Get("X-Agy-Cache-Warning"), outG)
		}
	}

	// Recovery: a cache hit on a non-streaming turn clears the pre-set header and clears dashboard cacheWarnings!
	msgs = append(msgs,
		map[string]any{"role": "assistant", "content": "ok"},
		map[string]any{"role": "user", "content": "[FAKE_HIT_250K] recovered cache"},
	)
	rRec, outRec := doJSON("POST", gwSrv.URL+"/v1/messages", sid, map[string]any{
		"model": "gemini-4-carbon", "max_tokens": 64, "system": sys, "messages": msgs,
	})
	if rRec.StatusCode != 200 || rRec.Header.Get("X-Agy-Cache-Warning") != "" {
		t.Fatalf("expected cache hit to clear X-Agy-Cache-Warning header, got hdr=%q body=%s", rRec.Header.Get("X-Agy-Cache-Warning"), outRec)
	}
	_, rawDashAfter := doJSON("GET", dashSrv.URL+"/api/dashboard", "", nil)
	var dashAfter map[string]any
	_ = json.Unmarshal([]byte(rawDashAfter), &dashAfter)
	cwAfter, _ := dashAfter["cacheWarnings"].([]any)
	if len(cwAfter) != 0 {
		t.Fatalf("expected 0 active cacheWarnings after cache hit, got %+v", cwAfter)
	}

	// Re-arm warning (3 consecutive zero-cache steps), then verify a STREAMING model switch clears header immediately!
	for step := 1; step <= 3; step++ {
		msgs = append(msgs,
			map[string]any{"role": "assistant", "content": "ok"},
			map[string]any{"role": "user", "content": "[FAKE_ZERO_250K] rearm"},
		)
		doJSON("POST", gwSrv.URL+"/v1/messages", sid, map[string]any{
			"model": "gemini-4-carbon", "max_tokens": 64, "system": sys, "messages": msgs,
		})
	}
	msgs = append(msgs,
		map[string]any{"role": "assistant", "content": "ok"},
		map[string]any{"role": "user", "content": "[FAKE_HIT_250K] switched to hydrogen stream"},
	)
	rSwitch, outSwitch := doJSON("POST", gwSrv.URL+"/v1/messages", sid, map[string]any{
		"model": "gemini-4-hydrogen", "max_tokens": 64, "stream": true, "system": sys, "messages": msgs,
	})
	if rSwitch.StatusCode != 200 || rSwitch.Header.Get("X-Agy-Cache-Warning") != "" {
		t.Fatalf("streaming model switch carried stale X-Agy-Cache-Warning: %d hdr=%q body=%s", rSwitch.StatusCode, rSwitch.Header.Get("X-Agy-Cache-Warning"), outSwitch)
	}
}
