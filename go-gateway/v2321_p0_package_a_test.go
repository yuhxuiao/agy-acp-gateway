package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"agy-acp-gateway-go/internal/acp"
	gwcfg "agy-acp-gateway-go/internal/config"
)

// --- A1 (#1828): Parked MCP tool_result image visibility via ACP ImageContentBlock ---

func TestV2321A1ParkedToolResultImageDeliveredViaACP(t *testing.T) {
	baseURL, cleanup := startTestGateway(t)
	defer cleanup()

	tools := []map[string]any{{
		"name":         "Bash",
		"description":  "Execute bash",
		"input_schema": map[string]any{"type": "object", "properties": map[string]any{"command": map[string]any{"type": "string"}}},
	}}
	sid := "v2321-a1-parked-img"

	post := func(msgs []map[string]any) (int, map[string]any, string) {
		body, _ := json.Marshal(map[string]any{
			"model":      "gemini-3.8-flash",
			"max_tokens": 128,
			"stream":     false,
			"tools":      tools,
			"system":     "Primary working directory: /tmp",
			"messages":   msgs,
		})
		req, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/messages", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("x-claude-code-session-id", sid)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("HTTP POST failed: %v", err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		var decoded map[string]any
		_ = json.Unmarshal(raw, &decoded)
		return resp.StatusCode, decoded, string(raw)
	}

	// Turn 1: triggers parked MCP tool call.
	turn1Msgs := []map[string]any{
		{"role": "user", "content": "[FAKE_TOOL_CALL_IMAGE_VERIFY] take a screenshot"},
	}
	code1, d1, raw1 := post(turn1Msgs)
	if code1 != http.StatusOK {
		t.Fatalf("turn 1 failed: %d %s", code1, raw1)
	}
	if stopReason, _ := d1["stop_reason"].(string); stopReason != "tool_use" {
		t.Fatalf("turn 1 expected stop_reason=tool_use, got %q (%s)", stopReason, raw1)
	}
	var toolUseID string
	var toolUseBlock map[string]any
	if content, ok := d1["content"].([]any); ok {
		for _, item := range content {
			if bm, ok := item.(map[string]any); ok && bm["type"] == "tool_use" {
				toolUseID, _ = bm["id"].(string)
				toolUseBlock = bm
				break
			}
		}
	}
	if toolUseID == "" {
		t.Fatalf("turn 1 missing tool_use id: %s", raw1)
	}

	// Turn 2a: invalid image inside parked tool_result must fail fast with 400 without corrupting parked state.
	badTurn2Msgs := []map[string]any{
		turn1Msgs[0],
		{"role": "assistant", "content": []any{toolUseBlock}},
		{
			"role": "user",
			"content": []map[string]any{{
				"type":        "tool_result",
				"tool_use_id": toolUseID,
				"content": []map[string]any{
					{"type": "text", "text": "screenshot captured"},
					{"type": "image", "source": map[string]any{"type": "base64", "media_type": "image/png", "data": "not-valid-png-data!!!"}},
				},
			}},
		},
	}
	badCode, _, badRaw := post(badTurn2Msgs)
	if badCode != http.StatusBadRequest {
		t.Fatalf("invalid image in parked tool_result expected 400, got %d: %s", badCode, badRaw)
	}

	// Turn 2b: valid PNG inside parked tool_result must roll over cleanly and deliver native ACP ImageContentBlock.
	validPNG := validTestPNGBase64()
	turn2Msgs := []map[string]any{
		turn1Msgs[0],
		{"role": "assistant", "content": []any{toolUseBlock}},
		{
			"role": "user",
			"content": []map[string]any{{
				"type":        "tool_result",
				"tool_use_id": toolUseID,
				"content": []map[string]any{
					{"type": "text", "text": "screenshot captured [FAKE_TOOL_CALL_IMAGE_VERIFY]"},
					{"type": "image", "source": map[string]any{"type": "base64", "media_type": "image/png", "data": validPNG}},
				},
			}},
		},
	}
	code2, d2, raw2 := post(turn2Msgs)
	if code2 != http.StatusOK {
		t.Fatalf("turn 2 failed: %d %s", code2, raw2)
	}
	if strings.Contains(raw2, "MCP_IMAGE_OFFLOADED_BUG") {
		t.Fatalf("image was routed through MCP tools/call instead of ACP ImageContentBlock: %s", raw2)
	}
	if !strings.Contains(raw2, "ACP_PROMPT_IMAGES=1") || !strings.Contains(raw2, "mimes=image/png") {
		t.Fatalf("expected model to receive 1 PNG via ACP ImageContentBlock, got: %s", raw2)
	}

	// Turn 3: follow-up turn on the same session must succeed cleanly (no lingering parked/canceled state).
	asst2Text := ""
	if content, ok := d2["content"].([]any); ok && len(content) > 0 {
		if bm, ok := content[0].(map[string]any); ok {
			asst2Text, _ = bm["text"].(string)
		}
	}
	turn3Msgs := append(append([]map[string]any(nil), turn2Msgs...),
		map[string]any{"role": "assistant", "content": asst2Text},
		map[string]any{"role": "user", "content": "follow-up after image tool result"},
	)
	code3, _, raw3 := post(turn3Msgs)
	if code3 != http.StatusOK || !strings.Contains(raw3, "FAKE_OK") {
		t.Fatalf("turn 3 follow-up failed: %d %s", code3, raw3)
	}
}

// --- A2 (#1091/#1794): Transient mid-session 401 retry & account scheduler grace cooldown ---

func TestV2321A2TransientMidSession401RetryAndPermanentAuthNoRetry(t *testing.T) {
	// Unit classification checks
	transientCases := []error{
		&acpRPCError{Raw: `{"code":401,"message":"UNAUTHENTICATED: access token expired"}`},
		errors.New("UNAUTHENTICATED (code 401): Request had invalid authentication credentials"),
		&SoftError{StatusCode: http.StatusUnauthorized, ErrorType: "authentication_error", Message: "401 Unauthorized"},
	}
	for _, err := range transientCases {
		if !isTransientMidSessionAuthErr(err) {
			t.Fatalf("expected transient mid-session auth error: %v", err)
		}
		if isNonTransientOrFatalUpstreamErr(err) {
			t.Fatalf("expected transient 401 to NOT be marked fatal/non-transient: %v", err)
		}
	}
	permanentCases := []error{
		&acpRPCError{Raw: `{"code":401,"message":"invalid_grant: Token has been expired or revoked."}`},
		errors.New("UNAUTHENTICATED: token revoked"),
		errors.New("401 Unauthorized: login required"),
		errors.New("401 Unauthorized: please log in again"),
		errors.New("Authentication required"),
		errors.New("401 invalid_api_key"),
		errors.New("You do not have a valid license (#3501)"),
		errors.New("PERMISSION_DENIED (code 403): VALIDATION_REQUIRED: Verify your account"),
		errors.New("PERMISSION_DENIED (code 403): forbidden"),
	}
	for _, err := range permanentCases {
		if isTransientMidSessionAuthErr(err) {
			t.Fatalf("permanent auth/permission error must NOT be treated as transient: %v", err)
		}
		if !isNonTransientOrFatalUpstreamErr(err) {
			t.Fatalf("permanent auth/permission error must be marked non-transient/fatal: %v", err)
		}
	}

	// End-to-end fakeacp tests with fast retry backoff.
	origBackoff := transientAuthRetryBackoff
	transientAuthRetryBackoff = 15 * time.Millisecond
	t.Cleanup(func() { transientAuthRetryBackoff = origBackoff })

	baseURL, cleanup := startTestGateway(t)
	defer cleanup()

	post := func(prompt, sid string) (int, string) {
		body, _ := json.Marshal(map[string]any{
			"model":      "gemini-3.8-flash",
			"max_tokens": 64,
			"stream":     false,
			"messages":   []map[string]any{{"role": "user", "content": prompt}},
		})
		req, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/messages", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("x-claude-code-session-id", sid)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(raw)
	}

	// 1. Transient RPC 401 once -> succeeds on the single short retry.
	code, out := post("[FAKE_RPC_401_ONCE:a2rpc] hello", "v2321-a2-rpc-401")
	if code != http.StatusOK || !strings.Contains(out, "FAKE_OK") {
		t.Fatalf("transient RPC 401 should recover on 1 short retry, got %d: %s", code, out)
	}

	// 2. Transient inline ⚠ UNAUTHENTICATED (code 401) notice once -> succeeds on 1 short retry.
	codeN, outN := post("[FAKE_NOTICE_401_ONCE:a2notice] hello", "v2321-a2-notice-401")
	if codeN != http.StatusOK || !strings.Contains(outN, "FAKE_OK") {
		t.Fatalf("transient notice 401 should recover on 1 short retry, got %d: %s", codeN, outN)
	}

	// 3. Permanent auth failure (invalid_grant: token revoked) -> must NOT retry and returns error.
	t0 := time.Now()
	codeP, outP := post("[FAKE_RPC_401_PERM] hello", "v2321-a2-perm-401")
	if codeP == http.StatusOK {
		t.Fatalf("permanent auth error unexpectedly succeeded: %s", outP)
	}
	if !strings.Contains(strings.ToLower(outP), "invalid_grant") {
		t.Fatalf("permanent auth error lost detail: %d %s", codeP, outP)
	}
	if time.Since(t0) > 2*time.Second {
		t.Fatalf("permanent auth error took unexpectedly long (%v)", time.Since(t0))
	}
}

func TestV2321A2AccountSchedulerTransient401GraceAndEscalation(t *testing.T) {
	am := setupV2318FiveAccountPool(t, false)
	origHook := transientAuthReprobeHook
	origGap := transientAuthReprobeMinGap
	var reprobeMu sync.Mutex
	reprobed := make(chan string, 4)
	reprobeCount := 0
	transientAuthReprobeMinGap = 0
	transientAuthReprobeHook = func(accountID string) {
		reprobeMu.Lock()
		reprobeCount++
		reprobeMu.Unlock()
		select {
		case reprobed <- accountID:
		default:
		}
	}
	t.Cleanup(func() {
		transientAuthReprobeHook = origHook
		transientAuthReprobeMinGap = origGap
	})

	acc, ok := am.GetAccount("u2")
	if !ok {
		t.Fatal("missing u2 account in test pool")
	}

	// 1. First transient 401 on healthy account -> 30s grace cooldown (NOT 30m) + async /usage re-probe.
	am.RecordAccountOutcome("u2", "gemini-4-neon", http.StatusUnauthorized, "authentication_error", "", "UNAUTHENTICATED (code 401): access token expired")
	hv := am.BuildAccountHealthView(acc, true, 0, time.Now())
	if hv.State != "auth_error" {
		t.Fatalf("expected state=auth_error during grace window, got %+v", hv)
	}
	if hv.CooldownSec <= 0 || hv.CooldownSec > 60 {
		t.Fatalf("first transient 401 expected ~30s grace cooldown, got %ds (%+v)", hv.CooldownSec, hv)
	}
	select {
	case gotID := <-reprobed:
		if gotID != "u2" {
			t.Fatalf("expected async reprobe for u2, got %q", gotID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected async /usage re-probe hook to be triggered on first transient 401")
	}

	// 2. Successful /usage clears grace cooldown back to healthy.
	am.ClearQuotaSyncAuthError("u2")
	if hv2 := am.BuildAccountHealthView(acc, true, 0, time.Now()); hv2.State != "healthy" {
		t.Fatalf("expected ClearQuotaSyncAuthError to clear grace cooldown, got %+v", hv2)
	}

	// Reset consecutiveFails via a 2xx so next test starts clean.
	am.RecordAccountOutcome("u2", "gemini-4-neon", http.StatusOK, "", "", "")

	// 3. First transient 401 followed by a second consecutive transient 401 escalates to 30m lockout.
	am.RecordAccountOutcome("u2", "gemini-4-neon", http.StatusUnauthorized, "authentication_error", "", "401 Unauthorized")
	hvGrace := am.BuildAccountHealthView(acc, true, 0, time.Now())
	if hvGrace.CooldownSec > 60 {
		t.Fatalf("expected 30s grace on first hit, got %ds", hvGrace.CooldownSec)
	}
	am.RecordAccountOutcome("u2", "gemini-4-neon", http.StatusUnauthorized, "authentication_error", "", "401 Unauthorized")
	hvEsc := am.BuildAccountHealthView(acc, true, 0, time.Now())
	if hvEsc.CooldownSec < 1500 {
		t.Fatalf("expected consecutive 401 to escalate to 30m (~1800s), got %ds", hvEsc.CooldownSec)
	}

	// 4. Failed /usage re-probe escalates a grace-window account to 30m immediately.
	am.ClearAccountCooldown("u2")
	am.RecordAccountOutcome("u2", "gemini-4-neon", http.StatusUnauthorized, "authentication_error", "", "UNAUTHENTICATED")
	am.RecordQuotaSyncOutcome("u2", "exit status 1: UNAUTHENTICATED token expired")
	hvProbeFail := am.BuildAccountHealthView(acc, true, 0, time.Now())
	if hvProbeFail.CooldownSec < 1500 {
		t.Fatalf("expected failed /usage re-probe to escalate to 30m, got %ds", hvProbeFail.CooldownSec)
	}

	// 5. Explicit permanent auth errors immediately lock out for 30m on first hit.
	for _, permMsg := range []string{
		"invalid_grant: Token has been expired or revoked",
		"OAuth token revoked",
		"not logged in, login required",
		"You do not have a valid license (#3501)",
	} {
		am.ClearAccountCooldown("u2")
		am.RecordAccountOutcome("u2", "gemini-4-neon", http.StatusUnauthorized, "authentication_error", "", permMsg)
		hvPerm := am.BuildAccountHealthView(acc, true, 0, time.Now())
		if hvPerm.State != "auth_error" || hvPerm.CooldownSec < 1500 {
			t.Fatalf("permanent auth error %q expected immediate 30m lockout, got state=%q sec=%d", permMsg, hvPerm.State, hvPerm.CooldownSec)
		}
	}
}

// --- A3: VALIDATION_REQUIRED / "Verify your account" first-class account state ---

func TestV2321A3VerificationURLValidationAndExtraction(t *testing.T) {
	validCases := []struct {
		raw  string
		want string
	}{
		{
			raw:  "PERMISSION_DENIED: VALIDATION_REQUIRED. Verify your account at https://accounts.google.com/signin/continue?continue=https://antigravity.google/verify).",
			want: "https://accounts.google.com/signin/continue?continue=https://antigravity.google/verify",
		},
		{
			raw:  "Visit https://google.com/verify/account, then retry!",
			want: "https://google.com/verify/account",
		},
		{
			// Skips support.google.com and extracts the real accounts.google.com verification URL.
			raw:  "See https://support.google.com/accounts/answer/12345 or verify at https://accounts.google.com/AccountChooser?flowName=verify;",
			want: "https://accounts.google.com/AccountChooser?flowName=verify",
		},
	}
	for _, tc := range validCases {
		if got := acp.ExtractVerificationURL(tc.raw); got != tc.want {
			t.Errorf("ExtractVerificationURL(%q) = %q, want %q", tc.raw, got, tc.want)
		}
	}

	rejectCases := []string{
		"http://accounts.google.com/verify",                                   // non-https
		"https://support.google.com/accounts/answer/40039",                    // excluded support.google.com
		"https://support.google.com./accounts/answer/40039",                   // trailing dot support.google.com
		"https://evilgoogle.com/verify",                                       // lookalike domain
		"https://google.com.evil.com/verify",                                  // suffix spoof
		"https://user:pass@accounts.google.com/verify",                        // userinfo credentials
		"https://accounts.google.com/verify?access_token=ya29.secretvalue123", // secret query param
		"https://accounts.google.com/verify?api_key=AIzaSecretKey123",         // api_key param
		"https://accounts.google.com/verify?token=secret123",                  // token param
		"https://accounts.google.com/verify?pad=" + strings.Repeat("a", 520),  // > 512 chars
	}
	for _, raw := range rejectCases {
		if got := acp.ExtractVerificationURL(raw); got != "" {
			t.Errorf("ExtractVerificationURL(%q) = %q, want empty", raw, got)
		}
	}
}

func TestV2321A3VerificationRequiredStateLifecycleAndFederation(t *testing.T) {
	verifyURL := "https://accounts.google.com/signin/continue?sarp=1&scc=1&continue=https://antigravity.google/verify"
	rawErr := "PERMISSION_DENIED (code 403): VALIDATION_REQUIRED: Verify your account to continue: " + verifyURL + "."

	// 1. SoftError & classifyUpstreamError & child/stream/healthcheck classifiers.
	se := DetectAntigravitySoftError("⚠ " + rawErr)
	if se == nil || se.ErrorType != "verification_required" {
		t.Fatalf("DetectAntigravitySoftError failed: %+v", se)
	}
	sc, et, retryAfter, msg := classifyUpstreamError(errors.New(rawErr))
	if sc != http.StatusServiceUnavailable || et != "verification_required" || retryAfter || !strings.Contains(msg, verifyURL) {
		t.Fatalf("classifyUpstreamError got (%d, %q, %v, %q)", sc, et, retryAfter, msg)
	}
	if !shouldRetryChildPreStreamFailure(http.StatusForbidden, "permission_denied", rawErr) {
		t.Fatal("shouldRetryChildPreStreamFailure must return true for VALIDATION_REQUIRED even on HTTP 403")
	}
	if ok, st, ot, om := classifyStructuredStreamError("permission_denied", "PERMISSION_DENIED", rawErr, 403); !ok || ot != "verification_required" || st != http.StatusServiceUnavailable || !strings.Contains(om, verifyURL) {
		t.Fatalf("classifyStructuredStreamError failed: (%v, %d, %q, %q)", ok, st, ot, om)
	}
	hcCode, hcType, hcMsg := classifyHealthCheckFailure(rawErr, "exit status 1", "", nil)
	if hcCode != http.StatusServiceUnavailable || hcType != "verification_required" || !strings.Contains(hcMsg, verifyURL) {
		t.Fatalf("classifyHealthCheckFailure failed: (%d, %q, %q)", hcCode, hcType, hcMsg)
	}

	// 2. AccountManager state machine, JSON contract, hard-block & pool failover.
	am := setupV2318FiveAccountPool(t, false)
	accU1, _ := am.GetAccount("u1")
	am.RecordAccountOutcome("u1", "gemini-4-neon", http.StatusForbidden, "permission_denied", "", rawErr)

	hv := am.BuildAccountHealthView(accU1, true, 0, time.Now())
	if hv.State != "verification_required" || !hv.VerificationRequired || hv.VerificationURL != verifyURL || hv.VerificationReason == "" || hv.CooldownReason != hv.VerificationReason {
		t.Fatalf("unexpected AccountHealthView: %+v", hv)
	}
	rawJSON, err := json.Marshal(hv)
	if err != nil {
		t.Fatal(err)
	}
	var jsonMap map[string]any
	if err := json.Unmarshal(rawJSON, &jsonMap); err != nil {
		t.Fatal(err)
	}
	if jsonMap["state"] != "verification_required" || jsonMap["verificationRequired"] != true ||
		jsonMap["verificationUrl"] != verifyURL || jsonMap["verificationReason"] != hv.VerificationReason ||
		jsonMap["cooldownReason"] != hv.VerificationReason {
		t.Fatalf("JSON contract mismatch: %s", string(rawJSON))
	}

	blocked, bState, _, _ := am.isAccountHardBlocked("u1", time.Now())
	if !blocked || bState != "verification_required" {
		t.Fatalf("expected u1 hard-blocked as verification_required, got blocked=%v state=%q", blocked, bState)
	}

	// Pool failover must skip verification_required account and pick a healthy sibling.
	reqFailover := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/messages", nil)
	picked, _, denyErr, _, _ := am.ResolveTargetAccountWithHints(reqFailover, "gemini-4-neon", "", false)
	if denyErr != nil {
		t.Fatalf("ResolveTargetAccountWithHints failed: %v", denyErr)
	}
	if picked.ID == "u1" {
		t.Fatalf("expected pool scheduler to fail over away from verification_required u1, picked %q", picked.ID)
	}

	// 3. Auto-clear checks:
	// 3a. Clear on 2xx
	am.RecordAccountOutcome("u1", "gemini-4-neon", http.StatusOK, "", "", "")
	if hvClr := am.BuildAccountHealthView(accU1, true, 0, time.Now()); hvClr.State != "healthy" || hvClr.VerificationRequired || hvClr.VerificationURL != "" {
		t.Fatalf("expected 2xx to auto-clear verification_required, got %+v", hvClr)
	}

	// 3b. Set via RecordQuotaSyncOutcome -> clear via RecordQuotaSyncOutcome("")
	am.RecordQuotaSyncOutcome("u1", "exit status 1: "+rawErr)
	if hvQ := am.BuildAccountHealthView(accU1, true, 0, time.Now()); hvQ.State != "verification_required" || hvQ.VerificationURL != verifyURL {
		t.Fatalf("expected /usage VALIDATION_REQUIRED to set verification_required + URL, got %+v", hvQ)
	}
	am.RecordQuotaSyncOutcome("u1", "")
	if hvQClr := am.BuildAccountHealthView(accU1, true, 0, time.Now()); hvQClr.State != "healthy" || hvQClr.VerificationRequired {
		t.Fatalf("expected RecordQuotaSyncOutcome(\"\") to auto-clear verification_required, got %+v", hvQClr)
	}

	// 3c. Clear via ClearQuotaSyncAuthError
	am.RecordQuotaSyncOutcome("u1", rawErr)
	am.ClearQuotaSyncAuthError("u1")
	if hvQClr2 := am.BuildAccountHealthView(accU1, true, 0, time.Now()); hvQClr2.State != "healthy" || hvQClr2.VerificationRequired {
		t.Fatalf("expected ClearQuotaSyncAuthError to auto-clear verification_required, got %+v", hvQClr2)
	}

	// 3d. Clear via ClearAccountCooldown
	am.RecordAccountOutcome("u1", "gemini-4-neon", http.StatusServiceUnavailable, "verification_required", "", rawErr)
	am.ClearAccountCooldown("u1")
	if hvCd := am.BuildAccountHealthView(accU1, true, 0, time.Now()); hvCd.State != "healthy" || hvCd.VerificationRequired {
		t.Fatalf("expected ClearAccountCooldown to clear verification_required, got %+v", hvCd)
	}

	// 3e. Manual health_check re-probes /usage: if /usage still reports VALIDATION_REQUIRED, keep verification_required!
	am.RecordAccountOutcome("u1", "gemini-4-neon", http.StatusServiceUnavailable, "verification_required", "", rawErr)
	origProbeHook := healthCheckUsageProbeHook
	t.Cleanup(func() { healthCheckUsageProbeHook = origProbeHook })
	healthCheckUsageProbeHook = func(acc AccountProfile) string {
		return rawErr
	}
	if _, err := am.PerformAccountActionEx(AccountActionRequest{AccountID: "u1", Action: "health_check"}); err == nil {
		t.Fatal("expected health_check to fail when /usage still reports VALIDATION_REQUIRED")
	}
	if hvStill := am.BuildAccountHealthView(accU1, true, 0, time.Now()); hvStill.State != "verification_required" || !hvStill.VerificationRequired {
		t.Fatalf("expected failed health_check to retain verification_required, got %+v", hvStill)
	}
	// Once /usage succeeds, health_check clears verification_required cleanly.
	healthCheckUsageProbeHook = func(acc AccountProfile) string { return "" }
	if _, err := am.PerformAccountActionEx(AccountActionRequest{AccountID: "u1", Action: "health_check"}); err != nil {
		t.Fatalf("health_check failed after recovery: %v", err)
	}
	if hvHC := am.BuildAccountHealthView(accU1, true, 0, time.Now()); hvHC.State != "healthy" || hvHC.VerificationRequired {
		t.Fatalf("expected health_check success to clear verification_required, got %+v", hvHC)
	}
	healthCheckUsageProbeHook = origProbeHook

	// 3f. Secret fragment URLs rejected by ExtractVerificationURL, and ClearQuotaSyncAuthError resets consecutiveFails.
	if got := acp.ExtractVerificationURL("https://accounts.google.com/verify#access_token=secret123"); got != "" {
		t.Fatalf("expected fragment secret URL to be rejected, got %q", got)
	}
	am.RecordAccountOutcome("u2", "gemini-4-neon", http.StatusUnauthorized, "authentication_error", "", "UNAUTHENTICATED (code 401)")
	am.ClearQuotaSyncAuthError("u2")
	am.RecordAccountOutcome("u2", "gemini-4-neon", http.StatusUnauthorized, "authentication_error", "", "UNAUTHENTICATED (code 401)")
	if hvGrace := am.BuildAccountHealthView(AccountProfile{ID: "u2", Enabled: true}, true, 0, time.Now()); hvGrace.CooldownSec > 60 {
		t.Fatalf("expected transient 401 after ClearQuotaSyncAuthError to get 30s grace (not 30m), got cooldownSec=%d", hvGrace.CooldownSec)
	}

	// 4. Dashboard /usage error path preserves full verification URL (>200 chars) and populates primary state.
	longVerifyURL := "https://accounts.google.com/v3/signin/challenge/pwd?TL=" + strings.Repeat("x", 180) + "&continue=https://antigravity.google/verify"
	scriptDir := t.TempDir()
	mockBin := filepath.Join(scriptDir, "mock-agy-verify.sh")
	script := fmt.Sprintf("#!/bin/sh\necho 'PERMISSION_DENIED (code 403): VALIDATION_REQUIRED: Please verify your account to continue at %s' >&2\nexit 1\n", longVerifyURL)
	if err := os.WriteFile(mockBin, []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	snap := fetchOfficialQuotaDoWith(mockBin, os.Environ())
	if !strings.Contains(snap.Error, longVerifyURL) {
		t.Fatalf("expected /usage QuotaSnapshot.Error to preserve full >200-char verification URL, got: %q", snap.Error)
	}
	if hvPrim := am.BuildAccountHealthView(accU1, true, 0, time.Now()); hvPrim.State != "verification_required" || hvPrim.VerificationURL != longVerifyURL {
		t.Fatalf("expected primary health view to capture full verification URL from /usage, got %+v", hvPrim)
	}

	// 5. Federated dashboard preserves verificationRequired/verificationUrl/verificationReason for peer instances.
	peerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"now":     time.Now().UTC().Format(time.RFC3339),
			"gateway": map[string]any{"instanceId": "ext-peer", "instanceLabel": "External Peer", "engineAlive": true},
			"health": AccountHealthView{
				State:                "verification_required",
				VerificationRequired: true,
				VerificationURL:      verifyURL,
				VerificationReason:   "VALIDATION_REQUIRED: Verify your account",
				CooldownReason:       "VALIDATION_REQUIRED: Verify your account",
			},
			"quota":   QuotaSnapshot{Status: "error", Error: rawErr, Groups: []QuotaGroup{}},
			"windows": map[string]WindowAggregate{},
		})
	}))
	defer peerSrv.Close()

	t.Setenv("AGY_DASHBOARD_PEERS", fmt.Sprintf("ext-peer|External Peer|%s", peerSrv.URL))
	initConfig()
	resetPeerDashboardCacheForTest()
	gw := NewGatewayServer()
	t.Cleanup(gw.Close)
	fed := BuildFederatedDashboardPayload(gw, false)
	insts, _ := fed["instances"].([]map[string]any)
	foundExt := false
	foundU1 := false
	for _, inst := range insts {
		id, _ := inst["id"].(string)
		hvAny, ok := inst["health"].(AccountHealthView)
		if !ok {
			continue
		}
		if id == "u1" {
			foundU1 = true
			if hvAny.State != "verification_required" || !hvAny.VerificationRequired || hvAny.VerificationURL != longVerifyURL {
				t.Fatalf("federated primary u1 health lost verification fields: %+v", hvAny)
			}
		}
		if id == "ext-peer" {
			foundExt = true
			if hvAny.State != "verification_required" || !hvAny.VerificationRequired || hvAny.VerificationURL != verifyURL {
				t.Fatalf("federated external peer health lost verification fields: %+v", hvAny)
			}
		}
	}
	if !foundU1 || !foundExt {
		t.Fatalf("expected both u1 (found=%v) and ext-peer (found=%v) in federated instances", foundU1, foundExt)
	}

	// 6. Managed child peer returning VALIDATION_REQUIRED via /usage syncs into master AccountHealthView.
	childSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"now":     time.Now().UTC().Format(time.RFC3339),
			"gateway": map[string]any{"instanceId": "u2", "instanceLabel": "Ultra 2", "engineAlive": true},
			"quota":   QuotaSnapshot{Status: "error", Error: rawErr, LastErrorAt: time.Now().UTC().Format(time.RFC3339), Groups: []QuotaGroup{}},
			"windows": map[string]WindowAggregate{},
		})
	}))
	defer childSrv.Close()
	t.Setenv("AGY_DASHBOARD_PEERS", fmt.Sprintf("u2|Ultra 2|%s", childSrv.URL))
	initConfig()
	resetPeerDashboardCacheForTest()
	fedChild := BuildFederatedDashboardPayload(gw, false)
	insts2, _ := fedChild["instances"].([]map[string]any)
	foundU2 := false
	for _, inst := range insts2 {
		if id, _ := inst["id"].(string); id == "u2" {
			foundU2 = true
			hvAny, ok := inst["health"].(AccountHealthView)
			if !ok || hvAny.State != "verification_required" || !hvAny.VerificationRequired || hvAny.VerificationURL != verifyURL {
				t.Fatalf("federated managed child u2 health lost verification fields: ok=%v hv=%+v", ok, hvAny)
			}
		}
	}
	if !foundU2 {
		t.Fatal("expected managed child u2 in federated instances")
	}
}

// --- A4 (#1825): Isolated app-data-dir for ws-imagegen & /usage / /credits / models ---

func TestV2321A4IsolatedAppDataDirAndCLIFlags(t *testing.T) {
	tmpGem := t.TempDir()
	origHome := PureApiHome
	PureApiHome = tmpGem
	t.Cleanup(func() {
		PureApiHome = origHome
		resetCreditsTierCacheForTest()
	})

	mainApp := filepath.Join(tmpGem, DefaultAppDataDirName)
	if err := os.MkdirAll(filepath.Join(mainApp, "conversations"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(mainApp, "plugin_data"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(mainApp, "cache"), 0700); err != nil {
		t.Fatal(err)
	}
	// Create dummy files (fake test placeholders only, never real secrets).
	for _, rel := range []string{
		"antigravity-oauth-token",
		"installation_id",
		"settings.json",
		filepath.Join("cache", "antigravity-keyring-unavailable"),
		filepath.Join("conversations", "huge-001.db"),
		filepath.Join("plugin_data", "state.json"),
	} {
		if err := os.WriteFile(filepath.Join(mainApp, rel), []byte("stub"), 0600); err != nil {
			t.Fatal(err)
		}
	}

	EnsurePureApiHome()

	for _, sub := range []string{ImageGenAppDataDirName, UsageAppDataDirName} {
		isoDir := filepath.Join(tmpGem, sub)
		// Required auth/identity files MUST be symlinks to mainApp.
		for _, rel := range []string{
			"antigravity-oauth-token",
			"installation_id",
			"settings.json",
			filepath.Join("cache", "antigravity-keyring-unavailable"),
		} {
			dst := filepath.Join(isoDir, rel)
			fi, err := os.Lstat(dst)
			if err != nil {
				t.Fatalf("%s missing %s: %v", sub, rel, err)
			}
			if fi.Mode()&os.ModeSymlink == 0 {
				t.Fatalf("%s/%s expected symlink, got mode %v", sub, rel, fi.Mode())
			}
			target, err := os.Readlink(dst)
			if err != nil || target != filepath.Join(mainApp, rel) {
				t.Fatalf("%s/%s symlink target = %q (err=%v), want %q", sub, rel, target, err, filepath.Join(mainApp, rel))
			}
		}
		// Onboarding file MUST be a local regular file (not symlinked to mainApp).
		onbFi, err := os.Lstat(filepath.Join(isoDir, "cache", "onboarding.json"))
		if err != nil || onbFi.Mode()&os.ModeSymlink != 0 {
			t.Fatalf("%s cache/onboarding.json expected local file, got fi=%v err=%v", sub, onbFi, err)
		}
		// conversations/ and plugin_data/ MUST NEVER be symlinked/shared!
		for _, forbidden := range []string{"conversations", "plugin_data"} {
			if _, err := os.Lstat(filepath.Join(isoDir, forbidden)); !os.IsNotExist(err) {
				t.Fatalf("%s must NOT share %s with ws-bridge (err=%v)", sub, forbidden, err)
			}
		}
	}

	// Dangling symlink + materialized regular oauth token file repair + concurrency safety test.
	brokenLink := filepath.Join(tmpGem, UsageAppDataDirName, "antigravity-oauth-token")
	_ = os.Remove(brokenLink)
	if err := os.Symlink(filepath.Join(tmpGem, "does-not-exist"), brokenLink); err != nil {
		t.Fatal(err)
	}
	ensureIsolatedAppDataDir(tmpGem, UsageAppDataDirName)
	repaired, err := os.Readlink(brokenLink)
	if err != nil || repaired != filepath.Join(mainApp, "antigravity-oauth-token") {
		t.Fatalf("expected dangling symlink to be repaired, got %q (err=%v)", repaired, err)
	}
	// If agy materialized a stale regular copy of antigravity-oauth-token, ensureIsolatedAppDataDir must re-link it!
	_ = os.Remove(brokenLink)
	if err := os.WriteFile(brokenLink, []byte("stale-copy"), 0600); err != nil {
		t.Fatal(err)
	}
	var concWg sync.WaitGroup
	for i := 0; i < 8; i++ {
		concWg.Add(1)
		go func() {
			defer concWg.Done()
			ensureIsolatedAppDataDir(tmpGem, UsageAppDataDirName)
		}()
	}
	concWg.Wait()
	if fi, lerr := os.Lstat(brokenLink); lerr != nil || fi.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("expected materialized regular antigravity-oauth-token to be atomically re-linked, got fi=%v err=%v", fi, lerr)
	}

	// Verify /usage and /credits CLI invocation passes underscored --gemini_dir & --app_data_dir AFTER -p /usage / /credits.
	argsFile := filepath.Join(t.TempDir(), "captured-args.txt")
	mockBin := filepath.Join(t.TempDir(), "mock-agy-usage.sh")
	script := fmt.Sprintf(`#!/bin/sh
echo "$@" >> %q
if [ "$2" = "/credits" ]; then
  echo '{"status":"SUCCESS","command":{"name":"credits","data":{"plan_name":"Google AI Ultra"}}}'
  exit 0
fi
echo '{"command":{"data":{"description":"ok","groups":[{"name":"Gemini Models","buckets":[{"id":"b1","name":"5h","window":"5h","remaining_fraction":0.8,"reset_time":"2099-01-01T00:00:00Z"}]}]}}}'
`, argsFile)
	if err := os.WriteFile(mockBin, []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	waitQuotaRefreshIdle(t)
	snap := fetchOfficialQuotaDoWith(mockBin, os.Environ())
	if snap.Status != "ok" {
		t.Fatalf("mock /usage failed: %+v", snap)
	}
	capturedBytes, err := os.ReadFile(argsFile)
	if err != nil {
		t.Fatal(err)
	}
	captured := string(capturedBytes)
	wantSuffix := fmt.Sprintf("--gemini_dir %s --app_data_dir %s", tmpGem, UsageAppDataDirName)
	if !strings.Contains(captured, "-p /usage --output-format json "+wantSuffix) {
		t.Fatalf("/usage did not pass isolated app_data_dir flags: %q", captured)
	}
	if !strings.Contains(captured, "-p /credits --output-format json "+wantSuffix) {
		t.Fatalf("/credits did not pass isolated app_data_dir flags: %q", captured)
	}

	// Verify accountModelsCmdArgs puts --gemini_dir & --app_data_dir BEFORE `models`.
	am := NewAccountManager()
	am.mu.Lock()
	am.accounts = []AccountProfile{{ID: "primary", Primary: true, Enabled: true}}
	am.mu.Unlock()
	modelArgs := am.accountModelsCmdArgs("primary")
	wantModelArgs := []string{"--gemini_dir", tmpGem, "--app_data_dir", UsageAppDataDirName, "models"}
	if strings.Join(modelArgs, "|") != strings.Join(wantModelArgs, "|") {
		t.Fatalf("accountModelsCmdArgs = %v, want %v", modelArgs, wantModelArgs)
	}

	// Verify ws-imagegen uses dedicated ImageGenAppDataDirName on its auxiliary SharedAcpEngine.
	eng := newAuxiliaryAcpEngine(ImageGenAgentName)
	if eng.appDataDir != ImageGenAppDataDirName {
		t.Fatalf("ws-imagegen engine appDataDir = %q, want %q", eng.appDataDir, ImageGenAppDataDirName)
	}
	mainEng := newSharedAcpEngine()
	if mainEng.appDataDir != DefaultAppDataDirName {
		t.Fatalf("ws-bridge engine appDataDir = %q, want %q", mainEng.appDataDir, DefaultAppDataDirName)
	}
	// Ensure gwcfg import is exercised cleanly.
	_ = gwcfg.Current()
}

// --- A5 (#1898): ws-bridge customization isolation ---

func TestV2321A5BridgeCustomizationIsolation(t *testing.T) {
	tmpGem := t.TempDir()
	origHome := PureApiHome
	PureApiHome = tmpGem
	t.Cleanup(func() { PureApiHome = origHome })

	EnsurePureApiHome()

	for _, name := range []string{McpBridgeName, LegacyMcpBridgeName, ImageGenAgentName} {
		mdPath := filepath.Join(tmpGem, "config", "plugins", name, "agents", name+".md")
		raw, err := os.ReadFile(mdPath)
		if err != nil {
			t.Fatalf("failed to read %s: %v", mdPath, err)
		}
		text := string(raw)
		if !strings.Contains(text, "excludeDefaultComponents: true") {
			t.Errorf("%s missing excludeDefaultComponents: true", name)
		}
		if !strings.Contains(text, "inheritCustomizations: false") {
			t.Errorf("%s missing inheritCustomizations: false", name)
		}
	}
}
