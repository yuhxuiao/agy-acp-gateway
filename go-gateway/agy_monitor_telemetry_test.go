package main

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestEstimateAgyMonitorCallUsd(t *testing.T) {
	// sir_flash_lite: (trajChars + 8331) / 3.2 * 0.10 / 1e6 + respChars / 3.2 * 0.40 / 1e6
	gotLite := estimateAgyMonitorCallUsd("sir_flash_lite", "MODEL_TIER_FLASH_LITE", 32000, 320)
	wantLite := (float64(32000+sirFlashLiteRubricChars)/agyMonitorCharsPerToken*sirFlashLiteInPerM +
		float64(320)/agyMonitorCharsPerToken*sirFlashLiteOutPerM) / 1e6
	if math.Abs(gotLite-wantLite) > 1e-9 {
		t.Fatalf("sir_flash_lite cost = %.9f, want %.9f", gotLite, wantLite)
	}
	if isEscalatedMonitor("sir_flash_lite", "MODEL_TIER_FLASH_LITE") {
		t.Fatalf("sir_flash_lite should not be marked escalated")
	}

	// severe_internal_risks_agy (gemini-4-hydrogen-sonar): $0.50 / $2.50 per M, rubric 16425
	gotSonar := estimateAgyMonitorCallUsd("severe_internal_risks_agy", "gemini-4-hydrogen-sonar", 320000, 640)
	wantSonar := (float64(320000+severeInternalRubricChars)/agyMonitorCharsPerToken*severeInternalInPerM +
		float64(640)/agyMonitorCharsPerToken*severeInternalOutPerM) / 1e6
	if math.Abs(gotSonar-wantSonar) > 1e-9 {
		t.Fatalf("severe_internal_risks_agy cost = %.9f, want %.9f", gotSonar, wantSonar)
	}
	if !isEscalatedMonitor("severe_internal_risks_agy", "gemini-4-hydrogen-sonar") {
		t.Fatalf("severe_internal_risks_agy should be marked escalated")
	}

	// Negative lengths clamp cleanly to zero without NaN/negative cost
	gotNeg := estimateAgyMonitorCallUsd("sir_flash_lite", "", -100, -50)
	if gotNeg <= 0 || math.IsNaN(gotNeg) {
		t.Fatalf("negative lengths should clamp cleanly to rubric-only positive cost, got %v", gotNeg)
	}
}

func TestCollectAgyMonitorEventsIncrementalAndConcurrent(t *testing.T) {
	resetAgyMonitorCacheForTest()
	t.Cleanup(resetAgyMonitorCacheForTest)

	loc := time.FixedZone("CST", 8*3600)
	now := time.Date(2026, time.November, 24, 17, 0, 0, 0, loc)
	dir := t.TempDir()

	// Ignored non-worker files
	if err := os.WriteFile(filepath.Join(dir, "other.log"), []byte("I1124 16:10:00.000000 10 llm_monitor.go:630] LLM Monitor sir_flash_lite: Trajectory formatted, length: 99999\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	logPath := filepath.Join(dir, "acp-worker-20261124_160000-814.log")
	// Part 1: two concurrent sir_flash_lite calls under same PID 814 + one escalated sonar call + partial trailing line (no newline yet)
	part1 := strings.Join([]string{
		"I1124 16:08:30.100000   814 llm_monitor.go:630] LLM Monitor sir_flash_lite: Trajectory formatted, length: 12000",
		"I1124 16:08:30.200000   814 llm_monitor.go:630] LLM Monitor sir_flash_lite: Trajectory formatted, length: 48000",
		"I1124 16:08:30.250000   814 llm_monitor.go:706] LLM Monitor sir_flash_lite: Calling LLM (model: MODEL_TIER_FLASH_LITE)",
		"I1124 16:08:30.300000   814 llm_monitor.go:706] LLM Monitor sir_flash_lite: Calling LLM (model: MODEL_TIER_FLASH_LITE)",
		"I1124 16:08:31.100000   814 llm_monitor.go:725] LLM Monitor sir_flash_lite: LLM response received, length: 203",
		"I1124 16:08:31.400000   814 llm_monitor.go:725] LLM Monitor sir_flash_lite: LLM response received, length: 188",
		"I1124 16:08:32.000000   814 llm_monitor.go:630] LLM Monitor severe_internal_risks_agy: Trajectory formatted, length: 160000",
		"I1124 16:08:32.010000   814 llm_monitor.go:706] LLM Monitor severe_internal_risks_agy: Calling LLM (model: gemini-4-hydrogen-sonar)",
		// Partial line without newline at end of write:
		"I1124 16:08:34.500000   814 llm_monitor.go:725] LLM Monitor severe_internal_risks_agy: LLM response received, leng",
	}, "\n")
	if err := os.WriteFile(logPath, []byte(part1), 0o644); err != nil {
		t.Fatal(err)
	}

	ev1 := collectAgyMonitorEventsFromDir(dir, now, loc, true)
	if len(ev1) != 3 {
		t.Fatalf("initial scan got %d events, want 3 (2 completed + 1 in-flight): %+v", len(ev1), ev1)
	}
	esc1 := 0
	for _, ev := range ev1 {
		if ev.Escalated {
			esc1++
			if ev.TrajChars != 160000 {
				t.Fatalf("escalated TrajChars = %d, want 160000", ev.TrajChars)
			}
		}
	}
	if esc1 != 1 {
		t.Fatalf("initial escalated count = %d, want 1", esc1)
	}

	// Part 2: finish the truncated line and append an errored call (no response) followed >120s later by a new call
	part2 := "th: 512\n" +
		"I1124 16:10:00.000000   814 llm_monitor.go:630] LLM Monitor sir_flash_lite: Trajectory formatted, length: 5000\n" +
		"I1124 16:10:00.010000   814 llm_monitor.go:706] LLM Monitor sir_flash_lite: Calling LLM (model: MODEL_TIER_FLASH_LITE)\n" +
		"I1124 16:13:00.000000   814 llm_monitor.go:630] LLM Monitor sir_flash_lite: Trajectory formatted, length: 9000\n" +
		"I1124 16:13:00.020000   814 llm_monitor.go:706] LLM Monitor sir_flash_lite: Calling LLM (model: MODEL_TIER_FLASH_LITE)\n" +
		"I1124 16:13:01.000000   814 llm_monitor.go:725] LLM Monitor sir_flash_lite: LLM response received, length: 120\n"

	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(part2); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	_ = f.Close()
	// Bump mtime explicitly so fast filesystems still register change
	newMtime := now.Add(10 * time.Second)
	_ = os.Chtimes(logPath, newMtime, newMtime)

	ev2 := collectAgyMonitorEventsFromDir(dir, now.Add(10*time.Second), loc, false)
	if len(ev2) != 5 {
		t.Fatalf("incremental scan got %d events, want 5: %+v", len(ev2), ev2)
	}
	foundResp512 := false
	for _, ev := range ev2 {
		if ev.Monitor == "severe_internal_risks_agy" && ev.RespChars == 512 {
			foundResp512 = true
		}
	}
	if !foundResp512 {
		t.Fatalf("expected truncated response line to be completed cleanly on incremental tail read, got: %+v", ev2)
	}
}

func TestParseGlogHeaderYearRollover(t *testing.T) {
	loc := time.UTC
	ts, tid, ok := parseGlogHeader([]byte("I0101 00:05:10.123456 42 llm_monitor.go:630] msg"), 2026, time.December, loc)
	if !ok {
		t.Fatalf("expected header parse ok")
	}
	if tid != "42" {
		t.Fatalf("tid = %q, want 42", tid)
	}
	got := time.Unix(ts, 0).UTC()
	want := time.Date(2027, time.January, 1, 0, 5, 10, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("rollover timestamp = %v, want %v", got, want)
	}
}

func TestAttachAgyMonitorQuotaDeductionWindowsAndJSON(t *testing.T) {
	resetAgyMonitorCacheForTest()
	t.Cleanup(resetAgyMonitorCacheForTest)

	loc := time.UTC
	now := time.Date(2026, time.November, 24, 18, 0, 0, 0, loc)
	dir := t.TempDir()

	// Official windows:
	//   5h reset at 21:00 UTC -> start at 16:00 UTC
	//   weekly reset at Nov 26 18:00 UTC -> start at Nov 19 18:00 UTC
	// Events:
	//   1) Nov 18 10:00 UTC -> before weekly window (excluded from both)
	//   2) Nov 23 12:00 UTC -> inside weekly, outside 5h (1 escalated sonar call)
	//   3) Nov 24 17:10 UTC -> inside both 5h and weekly (1 sir_flash_lite + 1 escalated sonar call)
	logOld := filepath.Join(dir, "acp-worker-20261118_100000-100.log")
	logMid := filepath.Join(dir, "acp-worker-20261123_120000-200.log")
	logCur := filepath.Join(dir, "acp-worker-20261124_170000-300.log")

	_ = os.WriteFile(logOld, []byte(
		"I1118 10:00:00.000000 100 llm_monitor.go:630] LLM Monitor severe_internal_risks_agy: Trajectory formatted, length: 320000\n"+
			"I1118 10:00:00.100000 100 llm_monitor.go:706] LLM Monitor severe_internal_risks_agy: Calling LLM (model: gemini-4-hydrogen-sonar)\n"+
			"I1118 10:00:01.000000 100 llm_monitor.go:725] LLM Monitor severe_internal_risks_agy: LLM response received, length: 300\n",
	), 0o644)
	_ = os.WriteFile(logMid, []byte(
		"I1123 12:00:00.000000 200 llm_monitor.go:630] LLM Monitor severe_internal_risks_agy: Trajectory formatted, length: 320000\n"+
			"I1123 12:00:00.100000 200 llm_monitor.go:706] LLM Monitor severe_internal_risks_agy: Calling LLM (model: gemini-4-hydrogen-sonar)\n"+
			"I1123 12:00:01.000000 200 llm_monitor.go:725] LLM Monitor severe_internal_risks_agy: LLM response received, length: 300\n",
	), 0o644)
	_ = os.WriteFile(logCur, []byte(
		"I1124 17:10:00.000000 300 llm_monitor.go:630] LLM Monitor sir_flash_lite: Trajectory formatted, length: 64000\n"+
			"I1124 17:10:00.100000 300 llm_monitor.go:706] LLM Monitor sir_flash_lite: Calling LLM (model: MODEL_TIER_FLASH_LITE)\n"+
			"I1124 17:10:00.900000 300 llm_monitor.go:725] LLM Monitor sir_flash_lite: LLM response received, length: 200\n"+
			"I1124 17:12:00.000000 300 llm_monitor.go:630] LLM Monitor severe_internal_risks_agy: Trajectory formatted, length: 320000\n"+
			"I1124 17:12:00.100000 300 llm_monitor.go:706] LLM Monitor severe_internal_risks_agy: Calling LLM (model: gemini-4-hydrogen-sonar)\n"+
			"I1124 17:12:01.000000 300 llm_monitor.go:725] LLM Monitor severe_internal_risks_agy: LLM response received, length: 300\n",
	), 0o644)

	snap := QuotaSnapshot{
		Status:    "ok",
		UpdatedAt: now.Format(time.RFC3339),
		PlanTier:  "ultra",
		Groups: []QuotaGroup{
			{
				Name: "Gemini Models",
				Buckets: []QuotaBucket{
					{Window: "5h", RemainingFraction: 0.85, UsedFraction: 0.15, ResetTime: now.Add(3 * time.Hour).Format(time.RFC3339)},
					{Window: "weekly", RemainingFraction: 0.65, UsedFraction: 0.35, ResetTime: now.Add(48 * time.Hour).Format(time.RFC3339)},
				},
			},
		},
	}

	built, _ := buildQuotaView(snap, nil, now)
	attachAgyMonitorQuotaDeduction(&built, dir, now, loc, true)

	if built.Deduction == nil {
		t.Fatalf("expected non-nil Deduction")
	}
	if built.Deduction.MonitorCalls5h != 2 || built.Deduction.MonitorEscalated5h != 1 {
		t.Fatalf("5h monitor counts = (%d, esc=%d), want (2, esc=1)", built.Deduction.MonitorCalls5h, built.Deduction.MonitorEscalated5h)
	}
	if built.Deduction.MonitorCallsWeekly != 3 || built.Deduction.MonitorEscalatedWeekly != 2 {
		t.Fatalf("weekly monitor counts = (%d, esc=%d), want (3, esc=2)", built.Deduction.MonitorCallsWeekly, built.Deduction.MonitorEscalatedWeekly)
	}
	if built.Deduction.MonitorEst5hUsd <= 0 || built.Deduction.MonitorEstWeeklyUsd <= built.Deduction.MonitorEst5hUsd {
		t.Fatalf("unexpected monitor USD estimates: 5h=%.4f weekly=%.4f", built.Deduction.MonitorEst5hUsd, built.Deduction.MonitorEstWeeklyUsd)
	}

	// Verify federation JSON round-trip preserves all monitor fields
	raw, err := json.Marshal(built)
	if err != nil {
		t.Fatalf("marshal QuotaSnapshot: %v", err)
	}
	var decoded QuotaSnapshot
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal QuotaSnapshot: %v", err)
	}
	if decoded.Deduction == nil ||
		decoded.Deduction.MonitorCalls5h != built.Deduction.MonitorCalls5h ||
		decoded.Deduction.MonitorEscalatedWeekly != built.Deduction.MonitorEscalatedWeekly ||
		decoded.Deduction.MonitorEstWeeklyUsd != built.Deduction.MonitorEstWeeklyUsd {
		t.Fatalf("JSON round-trip mismatch: got %+v, want %+v", decoded.Deduction, built.Deduction)
	}

	// Idle 5h window (100% remaining, no reset time): events before UpdatedAt are excluded
	idleSnap := QuotaSnapshot{
		Status:    "ok",
		UpdatedAt: now.Format(time.RFC3339),
		PlanTier:  "ultra",
		Groups: []QuotaGroup{
			{
				Name: "Gemini Models",
				Buckets: []QuotaBucket{
					{Window: "5h", RemainingFraction: 1.0, UsedFraction: 0.0, ResetTime: ""},
					{Window: "weekly", RemainingFraction: 0.65, UsedFraction: 0.35, ResetTime: now.Add(48 * time.Hour).Format(time.RFC3339)},
				},
			},
		},
	}
	idleBuilt, _ := buildQuotaView(idleSnap, nil, now)
	attachAgyMonitorQuotaDeduction(&idleBuilt, dir, now, loc, true)
	if idleBuilt.Deduction.MonitorCalls5h != 0 {
		t.Fatalf("idle 5h window should ignore pre-read events, got %d", idleBuilt.Deduction.MonitorCalls5h)
	}
	if idleBuilt.Deduction.MonitorCallsWeekly != 3 {
		t.Fatalf("active weekly window should still count 3 events, got %d", idleBuilt.Deduction.MonitorCallsWeekly)
	}
}
