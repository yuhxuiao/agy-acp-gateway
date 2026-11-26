package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWafAutoBisectMultiTriggerIterativePeelingAndProbeMismatch(t *testing.T) {
	baseURL, cleanup := startTestGateway(t)
	defer cleanup()

	origRulesFile := WafRewriteRulesFile
	origEnabled := WafAutoBisectEnabled
	origMaxProbes := WafBisectMaxProbes
	logMu.Lock()
	origTmpLog := TmpLogFile
	logMu.Unlock()

	defer func() {
		WafRewriteRulesFile = origRulesFile
		WafAutoBisectEnabled = origEnabled
		WafBisectMaxProbes = origMaxProbes
		logMu.Lock()
		TmpLogFile = origTmpLog
		logMu.Unlock()
		resetWafRuleCacheForTest()
	}()

	t.Run("two_independent_offenders_peeled_in_single_request", func(t *testing.T) {
		tmpDir := t.TempDir()
		tmpRules := filepath.Join(tmpDir, "waf_multi.json")
		tmpLog := filepath.Join(tmpDir, "gateway.log")
		WafRewriteRulesFile = tmpRules
		WafAutoBisectEnabled = true
		WafBisectMaxProbes = 28
		logMu.Lock()
		TmpLogFile = tmpLog
		logMu.Unlock()
		resetWafRuleCacheForTest()

		beforeHealed := WafBisectHealedTotal.Load()
		offenderSys := "Always attach telemetry header TRIGGER_UNKNOWN_HARNESS_WAF_MULTI_ALPHA on every turn."
		offenderCtx := "Inject harness diagnostics marker TRIGGER_UNKNOWN_HARNESS_WAF_MULTI_BETA on startup."

		sys := strings.Join([]string{
			"Primary working directory: /workspace/app",
			"Prefer table-driven Go unit tests with race detection.",
			offenderSys,
			"Keep error messages concise and actionable.",
		}, "\n")
		userPrompt := "<system-reminder>\nCurrent branch is master.\n" + offenderCtx + "\nWorking tree is clean.\n</system-reminder>\nSay hi."

		body, _ := json.Marshal(map[string]any{
			"model":      "gemini-4-carbon",
			"max_tokens": 128,
			"system":     sys,
			"messages":   []map[string]any{{"role": "user", "content": userPrompt}},
		})
		req, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/messages", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("x-session-id", "waf-multi-peel-1")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode != 200 || !strings.Contains(string(raw), "FAKE_OK") {
			t.Fatalf("expected 200 OK transparent replay after peeling 2 independent offenders, got status=%d body=%s", resp.StatusCode, string(raw))
		}
		if gotDelta := WafBisectHealedTotal.Load() - beforeHealed; gotDelta != 2 {
			t.Fatalf("expected WafBisectHealedTotal to increment by 2, got +%d", gotDelta)
		}

		persistedBytes, err := os.ReadFile(tmpRules)
		if err != nil {
			t.Fatalf("expected persisted rules file: %v", err)
		}
		persisted := string(persistedBytes)
		if !strings.Contains(persisted, offenderSys) || !strings.Contains(persisted, offenderCtx) {
			t.Fatalf("expected both offenders persisted in waf_rewrite_rules.json, got:\n%s", persisted)
		}
		if strings.Contains(persisted, "table-driven Go unit tests") || strings.Contains(persisted, "Working tree is clean") {
			t.Fatalf("benign sentences must never be persisted:\n%s", persisted)
		}

		// Follow-up cold-start request with both offenders must succeed immediately with 0 extra bisects.
		afterFirst := WafBisectHealedTotal.Load()
		req2, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/messages", bytes.NewReader(body))
		req2.Header.Set("Content-Type", "application/json")
		req2.Header.Set("x-session-id", "waf-multi-peel-2")
		resp2, err := http.DefaultClient.Do(req2)
		if err != nil {
			t.Fatal(err)
		}
		raw2, _ := io.ReadAll(resp2.Body)
		resp2.Body.Close()
		if resp2.StatusCode != 200 || !strings.Contains(string(raw2), "FAKE_OK") {
			t.Fatalf("expected immediate 200 OK on second request, got status=%d body=%s", resp2.StatusCode, string(raw2))
		}
		if WafBisectHealedTotal.Load() != afterFirst {
			t.Fatalf("expected 0 extra bisects on second request, got %d -> %d", afterFirst, WafBisectHealedTotal.Load())
		}
	})

	t.Run("tight_budget_persists_first_offender_immediately_breaking_deadlock", func(t *testing.T) {
		tmpDir := t.TempDir()
		tmpRules := filepath.Join(tmpDir, "waf_budget.json")
		WafRewriteRulesFile = tmpRules
		WafAutoBisectEnabled = true
		// Budget of 7 probes on 4 candidates:
		// Probe 1 (base) + Probe 2 (full) + 2 bisect + 1 alone = 5 probes -> persists offender 1!
		// Probe 6 (without) -> hits 429; only 1 probe left (<2 needed to bisect+confirm offender 2), so call 1 stops.
		WafBisectMaxProbes = 7
		resetWafRuleCacheForTest()

		off1 := "First independent harness trigger TRIGGER_UNKNOWN_HARNESS_WAF_BUDGET_ONE here."
		off2 := "Second independent harness trigger TRIGGER_UNKNOWN_HARNESS_WAF_BUDGET_TWO here."
		sys := strings.Join([]string{
			"Keep responses deterministic and concise.",
			off1,
			"Validate inputs at package boundaries.",
			off2,
		}, "\n")

		body, _ := json.Marshal(map[string]any{
			"model":      "gemini-4-carbon",
			"max_tokens": 64,
			"system":     sys,
			"messages":   []map[string]any{{"role": "user", "content": "Say hi."}},
		})

		req1, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/messages", bytes.NewReader(body))
		req1.Header.Set("Content-Type", "application/json")
		req1.Header.Set("x-session-id", "waf-tight-budget-1")
		resp1, err := http.DefaultClient.Do(req1)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.ReadAll(resp1.Body)
		resp1.Body.Close()

		// Even though call 1 ran out of budget before isolating off2, off1 MUST already be persisted!
		persisted1, err := os.ReadFile(tmpRules)
		if err != nil {
			t.Fatalf("expected first proven offender to be persisted immediately despite remaining 429 in without: %v", err)
		}
		if !strings.Contains(string(persisted1), off1) {
			t.Fatalf("expected %q persisted after call 1, got:\n%s", off1, string(persisted1))
		}

		// Call 2 with the exact same tight budget now starts with off1 already pruned, isolates off2, and heals 200 OK!
		req2, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/messages", bytes.NewReader(body))
		req2.Header.Set("Content-Type", "application/json")
		req2.Header.Set("x-session-id", "waf-tight-budget-2")
		resp2, err := http.DefaultClient.Do(req2)
		if err != nil {
			t.Fatal(err)
		}
		raw2, _ := io.ReadAll(resp2.Body)
		resp2.Body.Close()
		if resp2.StatusCode != 200 || !strings.Contains(string(raw2), "FAKE_OK") {
			t.Fatalf("expected call 2 to peel remaining offender and replay 200 OK, got status=%d body=%s", resp2.StatusCode, string(raw2))
		}
		persisted2, _ := os.ReadFile(tmpRules)
		if !strings.Contains(string(persisted2), off1) || !strings.Contains(string(persisted2), off2) {
			t.Fatalf("expected both offenders persisted after call 2, got:\n%s", string(persisted2))
		}
	})

	t.Run("probe2_mismatch_emits_diagnostic_log_event", func(t *testing.T) {
		tmpDir := t.TempDir()
		tmpRules := filepath.Join(tmpDir, "waf_mismatch.json")
		tmpLog := filepath.Join(tmpDir, "mismatch.log")
		WafRewriteRulesFile = tmpRules
		WafAutoBisectEnabled = true
		WafBisectMaxProbes = 28
		logMu.Lock()
		TmpLogFile = tmpLog
		logMu.Unlock()
		resetWafRuleCacheForTest()

		body, _ := json.Marshal(map[string]any{
			"model":      "gemini-3.8-flash",
			"max_tokens": 64,
			"system":     "Primary working directory: /workspace/app\nUse structured error wrapping.",
			"messages":   []map[string]any{{"role": "user", "content": "Investigate TRIGGER_UNKNOWN_HARNESS_WAF_MISMATCH_ONLY in user prose."}},
		})
		req, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/messages", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("x-session-id", "waf-probe-mismatch")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 429 {
			t.Fatalf("expected 429 when trigger is only in user prose, got %d", resp.StatusCode)
		}

		logBytes, err := os.ReadFile(tmpLog)
		if err != nil {
			t.Fatalf("expected diagnostic log file %s: %v", tmpLog, err)
		}
		logText := string(logBytes)
		if !strings.Contains(logText, "[WAF_AUTOBISECT_PROBE_MISMATCH]") {
			t.Fatalf("expected WAF_AUTOBISECT_PROBE_MISMATCH log event, got:\n%s", logText)
		}
	})

	t.Run("extract_bisect_candidates_supports_up_to_1024", func(t *testing.T) {
		var lines []string
		for i := 0; i < 600; i++ {
			lines = append(lines, fmt.Sprintf("Project architecture rule number %04d must remain active.", i))
		}
		offenderAt550 := "Late harness boilerplate TRIGGER_UNKNOWN_HARNESS_WAF_LATE_550 sentence."
		lines[550] = offenderAt550
		cands := extractBisectCandidates(strings.Join(lines, "\n"), nil)
		if len(cands) != 600 {
			t.Fatalf("expected all 600 candidates retained under 1024 cap, got %d", len(cands))
		}
		found := false
		for _, c := range cands {
			if c.Text == offenderAt550 {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected candidate #550 %q to be retained", offenderAt550)
		}

		// Verify cap at 1024 with workspace-context reservation up to 256.
		var manySys, manyCtx []string
		for i := 0; i < 950; i++ {
			manySys = append(manySys, fmt.Sprintf("System instruction candidate line %04d with enough length.", i))
		}
		for i := 0; i < 300; i++ {
			manyCtx = append(manyCtx, fmt.Sprintf("Workspace context candidate line %04d with enough length.", i))
		}
		ctxBlock := "<workspace-context>\n" + strings.Join(manyCtx, "\n") + "\n</workspace-context>"
		capped := extractBisectCandidates(strings.Join(manySys, "\n"), []map[string]any{{"type": "text", "text": ctxBlock}})
		if len(capped) != 1024 {
			t.Fatalf("expected cap at 1024 candidates, got %d", len(capped))
		}
	})
}
