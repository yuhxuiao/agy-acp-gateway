package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestClassifyCacheMissPrefixHitAndBoundaries(t *testing.T) {
	now := time.Now()

	cases := []struct {
		name          string
		prevIn        int64
		prevCr        int64
		curIn         int64
		curCr         int64
		gapMs         int64
		modelSwitched bool
		want          string
	}{
		{
			name:   "batch_tool_delta_hit",
			prevIn: 2000, prevCr: 98000,
			curIn: 35000, curCr: 99000,
			gapMs: 30_000,
			want:  "",
		},
		{
			name:   "gap_over_5m_with_preserved_cache_hit",
			prevIn: 2000, prevCr: 98000,
			curIn: 35000, curCr: 98000,
			gapMs: 360_000,
			want:  "",
		},
		{
			name:   "gap_over_5m_with_lost_cache",
			prevIn: 2000, prevCr: 98000,
			curIn: 85000, curCr: 15000,
			gapMs: 360_000,
			want:  "gap_ttl",
		},
		{
			name:   "model_switch_overrides_preserved_cache",
			prevIn: 2000, prevCr: 98000,
			curIn: 35000, curCr: 99000,
			gapMs:         30_000,
			modelSwitched: true,
			want:          "model_switch",
		},
		{
			name:   "shrink",
			prevIn: 2000, prevCr: 98000,
			curIn: 60000, curCr: 37000,
			gapMs: 30_000,
			want:  "shrink",
		},
		{
			name:   "prev_total_zero_prefix_only",
			prevIn: 0, prevCr: 0,
			curIn: 20000, curCr: 5000,
			gapMs: 30_000,
			want:  "prefix_only",
		},
		{
			name:   "prev_total_1500_cr_0_zero",
			prevIn: 1500, prevCr: 0,
			curIn: 9000, curCr: 0,
			gapMs: 30_000,
			want:  "zero",
		},
		{
			name:   "prev_total_2000_cr_1_prefix_only",
			prevIn: 2000, prevCr: 0,
			curIn: 9000, curCr: 1,
			gapMs: 30_000,
			want:  "prefix_only",
		},
		{
			name:   "prev_total_2500_cr_1250_exact_50pct_hit",
			prevIn: 2500, prevCr: 0,
			curIn: 9000, curCr: 1250,
			gapMs: 30_000,
			want:  "",
		},
		{
			name:   "prev_total_2500_cr_1249_below_50pct_prefix_only",
			prevIn: 2500, prevCr: 0,
			curIn: 9000, curCr: 1249,
			gapMs: 30_000,
			want:  "prefix_only",
		},
		{
			name:   "prev_total_2501_cr_1_below_50pct_prefix_only",
			prevIn: 2501, prevCr: 0,
			curIn: 9000, curCr: 1,
			gapMs: 30_000,
			want:  "prefix_only",
		},
		{
			name:   "prev_total_3000_cr_2000_hit",
			prevIn: 3000, prevCr: 0,
			curIn: 9000, curCr: 2000,
			gapMs: 30_000,
			want:  "",
		},
		{
			name:   "prev_total_100000_cr_97500_exact_margin_hit",
			prevIn: 2000, prevCr: 98000,
			curIn: 20000, curCr: 97500,
			gapMs: 30_000,
			want:  "",
		},
		{
			name:   "prev_total_100000_cr_97499_one_below_margin_prefix_only",
			prevIn: 2000, prevCr: 98000,
			curIn: 20000, curCr: 97499,
			gapMs: 30_000,
			want:  "prefix_only",
		},
		{
			name:   "negative_inputs_clamped_zero",
			prevIn: -100, prevCr: -200,
			curIn: 9000, curCr: -50,
			gapMs: -5000,
			want:  "zero",
		},
		{
			name:   "negative_cur_inputs_clamped_total_zero",
			prevIn: -10, prevCr: 100,
			curIn: -5, curCr: -5,
			gapMs: 30_000,
			want:  "",
		},
		{
			name:   "maxint_prev_shrink_no_overflow",
			prevIn: math.MaxInt64, prevCr: 100,
			curIn: 9000, curCr: 1000,
			gapMs: 30_000,
			want:  "shrink",
		},
		{
			name:   "maxint_cur_input_prefix_only_no_overflow",
			prevIn: 100000, prevCr: 0,
			curIn: math.MaxInt64, curCr: 100,
			gapMs: 30_000,
			want:  "prefix_only",
		},
		{
			name:   "maxint_prev_half_floor_no_overflow",
			prevIn: math.MaxInt64, prevCr: 0,
			curIn: math.MaxInt64 / 2, curCr: math.MaxInt64/2 + 1,
			gapMs: 30_000,
			want:  "prefix_only",
		},
		{
			name:   "maxint_prev_margin_hit_no_overflow",
			prevIn: math.MaxInt64, prevCr: 0,
			curIn: math.MaxInt64/10 + 1000, curCr: math.MaxInt64 - 100,
			gapMs: 30_000,
			want:  "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotSpec := classifyCacheMissSpec(tc.prevIn, tc.prevCr, tc.curIn, tc.curCr, tc.gapMs, tc.modelSwitched)
			if gotSpec != tc.want {
				t.Fatalf("classifyCacheMissSpec(%d,%d,%d,%d,%d,%v) = %q, want %q",
					tc.prevIn, tc.prevCr, tc.curIn, tc.curCr, tc.gapMs, tc.modelSwitched, gotSpec, tc.want)
			}

			curU := &TurnUsage{
				ContextInputTokens:  int(tc.curIn),
				ContextCachedTokens: int(tc.curCr),
				UsageSource:         "db",
			}
			var prevEntry *TurnHistoryEntry
			if tc.prevIn != 0 || tc.prevCr != 0 || tc.gapMs != 0 || tc.modelSwitched {
				prevModel := "gemini-4-carbon"
				completedAt := now.Add(-time.Duration(tc.gapMs) * time.Millisecond).UnixMilli()
				if tc.gapMs <= 0 {
					completedAt = now.UnixMilli()
				}
				prevEntry = &TurnHistoryEntry{
					Model:         prevModel,
					CompletedAtMs: completedAt,
					Usage: &TurnUsage{
						ContextInputTokens:  int(tc.prevIn),
						ContextCachedTokens: int(tc.prevCr),
						UsageSource:         "db",
					},
				}
			}
			curModel := "gemini-4-carbon"
			if tc.modelSwitched {
				curModel = "gemini-4-hydrogen"
			}
			gotReason := classifyCacheMissWithReason("sticky_delta", "", curU, prevEntry, curModel, now)
			if gotReason != tc.want {
				t.Fatalf("classifyCacheMissWithReason = %q, want %q", gotReason, tc.want)
			}
		})
	}
}

func TestComputeDbStepCacheMissesPrefixHit(t *testing.T) {
	prevDB := GatewayStateDB
	dbPath := filepath.Join(t.TempDir(), "db_miss_m1.db")
	t.Cleanup(func() { InitGlobalStore(prevDB) })
	InitGlobalStore(dbPath)

	steps := []ParsedModelStep{
		{SessionID: "sid-db-prefix-hit", StepIdx: 1, TsSec: 1000, ElapsedMs: 10000, ModelID: "gemini-4-carbon", RawIn: 100000, RawCr: 0, OutTok: 100},
		{SessionID: "sid-db-prefix-hit", StepIdx: 2, TsSec: 1030, ElapsedMs: 10000, ModelID: "gemini-4-carbon", RawIn: 35000, RawCr: 99000, OutTok: 100},
		{SessionID: "sid-db-prefix-hit", StepIdx: 3, TsSec: 1400, ElapsedMs: 10000, ModelID: "gemini-4-carbon", RawIn: 35000, RawCr: 133000, OutTok: 100},
		{SessionID: "sid-db-prefix-hit", StepIdx: 4, TsSec: 1420, ElapsedMs: 5000, ModelID: "gemini-4-carbon", RawIn: 150000, RawCr: 20000, OutTok: 100},
		{SessionID: "sid-db-prefix-hit", StepIdx: 5, TsSec: 1430, ElapsedMs: 5000, ModelID: "gemini-4-hydrogen", RawIn: 170000, RawCr: 0, OutTok: 100},
		{SessionID: "sid-db-prefix-hit", StepIdx: 6, TsSec: 1440, ElapsedMs: 5000, ModelID: "gemini-4-hydrogen", RawIn: 175000, RawCr: 0, OutTok: 100},
		{SessionID: "sid-db-prefix-hit", StepIdx: 7, TsSec: 1450, ElapsedMs: 5000, ModelID: "gemini-4-hydrogen", RawIn: 60000, RawCr: 10000, OutTok: 100},
		{SessionID: "sid-db-prefix-hit", StepIdx: 8, TsSec: 1800, ElapsedMs: 5000, ModelID: "gemini-4-hydrogen", RawIn: 75000, RawCr: 0, OutTok: 100},

		// Boundary [2501, 4999] checks
		{SessionID: "sid-db-boundary-1", StepIdx: 1, TsSec: 2000, ElapsedMs: 1000, ModelID: "gemini-4-carbon", RawIn: 2501, RawCr: 0, OutTok: 50},
		{SessionID: "sid-db-boundary-1", StepIdx: 2, TsSec: 2010, ElapsedMs: 1000, ModelID: "gemini-4-carbon", RawIn: 9000, RawCr: 1, OutTok: 50},
		{SessionID: "sid-db-boundary-2", StepIdx: 1, TsSec: 2100, ElapsedMs: 1000, ModelID: "gemini-4-carbon", RawIn: 3000, RawCr: 0, OutTok: 50},
		{SessionID: "sid-db-boundary-2", StepIdx: 2, TsSec: 2110, ElapsedMs: 1000, ModelID: "gemini-4-carbon", RawIn: 9000, RawCr: 2000, OutTok: 50},

		// AlignedIn/AlignedCr fallback + total == 0 guard
		{SessionID: "sid-db-aligned", StepIdx: 1, TsSec: 3000, ElapsedMs: 1000, ModelID: "gemini-4-carbon", AlignedIn: 100000, AlignedCr: 0, OutTok: 50},
		{SessionID: "sid-db-aligned", StepIdx: 2, TsSec: 3010, ElapsedMs: 1000, ModelID: "gemini-4-carbon", AlignedIn: 35000, AlignedCr: 99000, OutTok: 50},
		{SessionID: "sid-db-aligned", StepIdx: 3, TsSec: 3020, ElapsedMs: 1000, ModelID: "gemini-4-carbon", AlignedIn: 120000, AlignedCr: 15000, OutTok: 50},
		{SessionID: "sid-db-aligned", StepIdx: 4, TsSec: 3030, ElapsedMs: 1000, ModelID: "gemini-4-carbon", AlignedIn: 0, AlignedCr: 0, OutTok: 50},

		// Zero prev.TsSec with ElapsedMs > 0 must NOT emit spurious "gap_ttl"
		{SessionID: "sid-db-zero-ts", StepIdx: 1, TsSec: 0, ElapsedMs: 1000, ModelID: "gemini-4-carbon", RawIn: 40000, RawCr: 0, OutTok: 50},
		{SessionID: "sid-db-zero-ts", StepIdx: 2, TsSec: 400, ElapsedMs: 1000, ModelID: "gemini-4-carbon", RawIn: 50000, RawCr: 0, OutTok: 50},

		// Negative RawCr must clamp to 0 and classify as "zero" (not "")
		{SessionID: "sid-db-neg-cr", StepIdx: 1, TsSec: 4000, ElapsedMs: 1000, ModelID: "gemini-4-carbon", RawIn: 50000, RawCr: 0, OutTok: 50},
		{SessionID: "sid-db-neg-cr", StepIdx: 2, TsSec: 4010, ElapsedMs: 1000, ModelID: "gemini-4-carbon", RawIn: 90000, RawCr: -90000, OutTok: 50},
	}

	want := []string{
		"cold_start",
		"",
		"",
		"prefix_only",
		"model_switch",
		"zero",
		"shrink",
		"gap_ttl",
		"cold_start",
		"prefix_only",
		"cold_start",
		"",
		"cold_start",
		"",
		"prefix_only",
		"",
		"cold_start",
		"zero",
		"cold_start",
		"zero",
	}

	got := computeDbStepCacheMisses(steps, nil)
	if len(got) != len(want) {
		t.Fatalf("len mismatch: got %d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("step[%d] (%s #%d): got %q, want %q", i, steps[i].SessionID, steps[i].StepIdx, got[i], want[i])
		}
	}
}

func TestMcpRejectedToolCallSessionIDRace(t *testing.T) {
	if GlobalEngine != nil {
		GlobalEngine.StopEngine()
		if GlobalEngine.IsAlive() {
			t.Fatalf("expected GlobalEngine to be inactive during unit race test")
		}
	}
	prevDB := GatewayStateDB
	dbPath := filepath.Join(t.TempDir(), "mcp_race_m1.db")
	t.Cleanup(func() {
		if GlobalEngine != nil {
			GlobalEngine.StopEngine()
		}
		InitGlobalStore(prevDB)
	})
	InitGlobalStore(dbPath)

	s := NewAcpProjectSession("sid:mcp-sid-race:u:1", "/tmp", "")
	s.MessageCount = 0
	s.McpToHostMap[mcpToolName("Bash")] = "Bash"

	payload, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "tools/call",
		"params": map[string]any{
			"name":      "ws_unoffered_tool_000000000000",
			"arguments": map[string]any{},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	const iterations = 120
	for i := 0; i < iterations; i++ {
		sid := fmt.Sprintf("sid-mcp-race-%d", i)
		s.mu.Lock()
		s.SessionID = sid
		s.mu.Unlock()

		start := make(chan struct{})
		errCh := make(chan string, 6)
		var wg sync.WaitGroup

		for r := 0; r < 4; r++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				req := httptest.NewRequest(http.MethodPost, "/mcp/"+s.TenantKey, bytes.NewReader(payload))
				req.RemoteAddr = "127.0.0.1:5000"
				req.Header.Set("Authorization", "Bearer "+s.TenantKey)
				rec := httptest.NewRecorder()
				s.HandleMcpHTTP(rec, req)
				if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "not available") {
					errCh <- fmt.Sprintf("unexpected MCP response: code=%d body=%s", rec.Code, rec.Body.String())
				}
			}()
		}

		wg.Add(1)
		go func(iter int) {
			defer wg.Done()
			<-start
			for g := 0; g < (iter%8)+1; g++ {
				runtime.Gosched()
			}
			if iter%2 == 0 {
				s.HibernateWorker("test_mcp_race", false)
			} else {
				s.onEngineExited(sid, 0)
			}
		}(i)

		close(start)
		wg.Wait()
		close(errCh)

		for msg := range errCh {
			t.Fatalf("iteration %d: %s", i, msg)
		}

		s.mu.Lock()
		afterSid := s.SessionID
		s.mu.Unlock()
		if afterSid != "" {
			t.Fatalf("iteration %d: expected SessionID cleared by hibernate/exit, got %q", i, afterSid)
		}
	}
}
