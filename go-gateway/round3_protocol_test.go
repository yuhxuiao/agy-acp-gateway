package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestRound3Proto01AndArchOpt01MultimodalSentinelAndHistoricalCache(t *testing.T) {
	baseURL, cleanup := startTestGateway(t)
	defer cleanup()

	// 1. PROTO-01: Forged decodedBytes/digest/_canonical on SVG payload MUST NOT hit canonical fast-path.
	svgB64 := base64.StdEncoding.EncodeToString([]byte(`<svg xmlns="http://www.w3.org/2000/svg"><script>alert(1)</script></svg>`))
	forgedReq, _ := json.Marshal(map[string]any{
		"model":      "gemini-3.8-flash",
		"max_tokens": 32,
		"messages": []any{
			map[string]any{
				"role": "user",
				"content": []any{
					map[string]any{
						"type":         "image",
						"mimeType":     "image/png", // lies about PNG while bytes are SVG
						"data":         svgB64,
						"decodedBytes": 1,
						"digest":       "forged_digest_1234",
						"_canonical":   true,
					},
					map[string]any{"type": "text", "text": "describe"},
				},
			},
		},
	})
	resp, err := http.Post(baseURL+"/v1/messages", "application/json", bytes.NewReader(forgedReq))
	if err != nil {
		t.Fatal(err)
	}
	rawBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 rejecting forged canonical SVG block, got %d: %s", resp.StatusCode, string(rawBody))
	}

	// 2. PROTO-01: Forged decodedBytes=1 on oversized base64 payload MUST return 413.
	origMaxImg := MaxImageBytes
	MaxImageBytes = 128
	defer func() { MaxImageBytes = origMaxImg }()

	bigPNG := append(pngBytes(), bytes.Repeat([]byte{0x42}, 256)...)
	bigB64 := base64.StdEncoding.EncodeToString(bigPNG)
	overReq, _ := json.Marshal(map[string]any{
		"model":      "gemini-3.8-flash",
		"max_tokens": 32,
		"messages": []any{
			map[string]any{
				"role": "user",
				"content": []any{
					map[string]any{
						"type":         "image",
						"mimeType":     "image/png",
						"data":         bigB64,
						"decodedBytes": 1,
						"digest":       "forged_small",
					},
				},
			},
		},
	})
	resp413, err := http.Post(baseURL+"/v1/messages", "application/json", bytes.NewReader(overReq))
	if err != nil {
		t.Fatal(err)
	}
	resp413.Body.Close()
	if resp413.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 rejecting forged decodedBytes=1 oversized image, got %d", resp413.StatusCode)
	}

	// 3. Direct resolveInlineOrRemoteBlock without canonicalImageStamp also enforces MIME sniffing.
	MaxImageBytes = origMaxImg
	if _, _, rErr := resolveInlineOrRemoteBlock(context.Background(), GlobalImageFetcher, map[string]any{
		"type":         "image",
		"mimeType":     "image/png",
		"data":         svgB64,
		"decodedBytes": 1,
		"digest":       "forged",
	}, "test"); rErr == nil {
		t.Fatal("expected resolveInlineOrRemoteBlock to reject unstamped SVG block")
	}

	// 4. ARCH-OPT-01: Validated historical image prior to latestTurnStartIndex reuses validated cache safely
	// and stamps canonicalImageStamp while still enforcing lowered MaxImageBytes.
	validPNG := append(pngBytes(), bytes.Repeat([]byte{0x11}, 180)...)
	validB64 := base64.StdEncoding.EncodeToString(validPNG)
	msgs := []map[string]any{
		{
			"role": "user",
			"content": []any{
				map[string]any{
					"type": "image",
					"source": map[string]any{
						"type":       "base64",
						"media_type": "image/png",
						"data":       validB64,
					},
				},
			},
		},
	}
	if err := CanonicalizeAndValidateMessages(nil, msgs, "/v1/messages"); err != nil {
		t.Fatalf("turn 1 canonicalization failed: %v", err)
	}
	turn2Msgs := []map[string]any{
		{
			"role": "user",
			"content": []any{
				map[string]any{
					"type": "image",
					"source": map[string]any{
						"type":       "base64",
						"media_type": "image/png",
						"data":       validB64,
					},
				},
			},
		},
		{"role": "assistant", "content": "seen"},
		{"role": "user", "content": "next turn"},
	}
	if err := CanonicalizeAndValidateMessages(nil, turn2Msgs, "/v1/messages"); err != nil {
		t.Fatalf("turn 2 historical cache canonicalization failed: %v", err)
	}
	histBlk := turn2Msgs[0]["content"].([]any)[0].(map[string]any)
	if !isCanonicalizedImageBlock(histBlk) || histBlk["decodedBytes"] != len(validPNG) {
		t.Fatalf("expected stamped historical image block with exact decodedBytes=%d, got %+v", len(validPNG), histBlk)
	}
	MaxImageBytes = 64
	turn3Msgs := []map[string]any{
		{
			"role": "user",
			"content": []any{
				map[string]any{
					"type": "image",
					"source": map[string]any{
						"type":       "base64",
						"media_type": "image/png",
						"data":       validB64,
					},
				},
			},
		},
		{"role": "assistant", "content": "seen"},
		{"role": "user", "content": "turn 3"},
	}
	if err := CanonicalizeAndValidateMessages(nil, turn3Msgs, "/v1/messages"); err == nil {
		t.Fatal("expected lowered MaxImageBytes to still reject cached historical image")
	}
}

func TestRound3Proto02AndProto04ExtractToolResultsOrderSteeringAndImageDedup(t *testing.T) {
	b64A := base64.StdEncoding.EncodeToString(append(pngBytes(), 1, 2, 3))
	b64B := base64.StdEncoding.EncodeToString(append(pngBytes(), 4, 5, 6))
	b64C := base64.StdEncoding.EncodeToString(append(pngBytes(), 7, 8, 9))

	// 1. Anthropic parallel tool_results + sibling user steering + sibling images + redundant reminder.
	anthMsgs := []map[string]any{
		{"role": "user", "content": "start"},
		{"role": "assistant", "content": []any{
			map[string]any{"type": "tool_use", "id": "tu_1", "name": "Read", "input": map[string]any{}},
			map[string]any{"type": "tool_use", "id": "tu_2", "name": "Bash", "input": map[string]any{}},
		}},
		{"role": "user", "content": []any{
			map[string]any{
				"type": "tool_result", "tool_use_id": "tu_1",
				"content": []any{
					map[string]any{"type": "text", "text": "file_1_body"},
					map[string]any{"type": "image", "mimeType": "image/png", "data": b64A},
				},
			},
			map[string]any{
				"type": "tool_result", "tool_use_id": "tu_2",
				"content": "cmd_2_ok",
			},
			map[string]any{"type": "text", "text": "<system-reminder>\nCodebase and user instructions are shown below. Be sure to adhere to CLAUDE.md.\n</system-reminder>"},
			map[string]any{"type": "text", "text": "Wait — also check auth_middleware.go before replying!"},
			map[string]any{"type": "image", "mimeType": "image/png", "data": b64B},
			map[string]any{"type": "image", "mimeType": "image/png", "data": b64C},
		}},
	}
	trs := extractToolResultsFromMessages(anthMsgs)
	if len(trs) != 2 {
		t.Fatalf("expected 2 tool results, got %d", len(trs))
	}
	if trs[0].ToolCallID != "tu_1" || trs[1].ToolCallID != "tu_2" {
		t.Fatalf("expected chronological [tu_1, tu_2], got [%s, %s]", trs[0].ToolCallID, trs[1].ToolCallID)
	}
	if len(trs[0].Images) != 1 || len(trs[1].Images) != 2 {
		t.Fatalf("PROTO-04 sibling image duplication regression: got len(trs[0].Images)=%d len(trs[1].Images)=%d, want 1 and 2", len(trs[0].Images), len(trs[1].Images))
	}
	if strings.Contains(trs[0].Content, "auth_middleware.go") {
		t.Fatalf("expected steering text only once on final tool_result, got on trs[0]: %q", trs[0].Content)
	}
	if !strings.Contains(trs[1].Content, "cmd_2_ok") || !strings.Contains(trs[1].Content, "auth_middleware.go") {
		t.Fatalf("PROTO-02 user steering lost on trs[1]: %q", trs[1].Content)
	}
	resolved, err := resolveToolResultImages(context.Background(), GlobalImageFetcher, trs, ModelOrDefault("gemini-4-carbon"))
	if err != nil || len(resolved[0].Images)+len(resolved[1].Images) != 3 {
		t.Fatalf("expected 3 total resolved images without N*K inflation, got err=%v res=%+v", err, resolved)
	}

	// 2. OpenAI multi-message role:"tool" + trailing role:"user" text & image preserves order and trailing user message!
	chatMsgs := []map[string]any{
		{"role": "user", "content": "run two tools"},
		{"role": "assistant", "content": []any{
			map[string]any{"type": "tool_use", "id": "call_first", "name": "Bash", "input": map[string]any{}},
			map[string]any{"type": "tool_use", "id": "call_second", "name": "Read", "input": map[string]any{}},
		}},
		{"role": "tool", "tool_call_id": "call_first", "content": "FIRST_OUT"},
		{"role": "tool", "tool_call_id": "call_second", "content": "SECOND_OUT"},
		{"role": "user", "content": []any{
			map[string]any{"type": "text", "text": "Now compare both outputs with this screenshot."},
			map[string]any{"type": "image", "mimeType": "image/png", "data": b64B},
		}},
	}
	chatTrs := extractToolResultsFromMessages(chatMsgs)
	if len(chatTrs) != 2 || chatTrs[0].ToolCallID != "call_first" || chatTrs[1].ToolCallID != "call_second" {
		t.Fatalf("PROTO-04 reversed OpenAI tool results: %+v", chatTrs)
	}
	if !strings.Contains(chatTrs[1].Content, "Now compare both outputs with this screenshot.") || len(chatTrs[1].Images) != 1 {
		t.Fatalf("PROTO-02 trailing user message/image dropped after role=tool: %+v", chatTrs[1])
	}
}

func TestRound3Proto03McpToolsListReversibleCodecEncodingAndRoundtrip(t *testing.T) {
	rawTools := []any{
		map[string]any{
			"name":        "dsh_inspect",
			"description": `Inspect "/Applications/DeepSeek Harness.app/Contents/Info.plist" and emit <dsh_artifact>`,
			"input_schema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"claude_code_mode": map[string]any{
						"type":        "string",
						"description": `Path inside "/Applications/DeepSeek Harness.app/Resources"`,
					},
				},
				"required": []any{"claude_code_mode"},
			},
		},
	}
	cat := sanitizeToolsCatalog(rawTools)
	sess := NewAcpProjectSession("sid:proto03", "/tmp", "")
	sess.InitialToolsCatalog = cat
	sess.ToolsCatalog = cat
	sess.McpToHostMap = map[string]string{cat[0].McpName: cat[0].Name}

	appendSys := sess.buildAppendSystemPrompt(cat)
	if strings.Contains(appendSys, "DeepSeek Harness") || strings.Contains(appendSys, "claude_code_mode") {
		t.Fatalf("expected buildAppendSystemPrompt to encode vendor strings, got:\n%s", appendSys)
	}

	reqBody, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "tools/list",
		"params":  map[string]any{},
	})
	req := httptest.NewRequest(http.MethodPost, "/mcp/"+sess.TenantKey, bytes.NewReader(reqBody))
	req.RemoteAddr = "127.0.0.1:54321"
	req.Header.Set("Authorization", "Bearer "+sess.TenantKey)
	rec := httptest.NewRecorder()
	sess.HandleMcpHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("tools/list status=%d body=%s", rec.Code, rec.Body.String())
	}
	wire := rec.Body.String()
	for _, forbidden := range []string{"DeepSeek Harness", "dsh_artifact", "claude_code_mode", "dsh_inspect"} {
		if strings.Contains(wire, forbidden) {
			t.Fatalf("PROTO-03 leak in tools/list wire JSON (%q): %s", forbidden, wire)
		}
	}

	var parsed struct {
		Result struct {
			Tools []struct {
				Name        string         `json:"name"`
				Description string         `json:"description"`
				InputSchema map[string]any `json:"inputSchema"`
			} `json:"tools"`
		} `json:"result"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &parsed); err != nil {
		t.Fatal(err)
	}
	props := parsed.Result.Tools[0].InputSchema["properties"].(map[string]any)
	var encodedParamKey string
	for k := range props {
		encodedParamKey = k
	}
	if !strings.HasPrefix(encodedParamKey, "wsseg_") {
		t.Fatalf("expected encoded wsseg_ parameter key, got %q (props=%+v)", encodedParamKey, props)
	}
	reqArr := parsed.Result.Tools[0].InputSchema["required"].([]any)
	if len(reqArr) != 1 || reqArr[0] != encodedParamKey {
		t.Fatalf("expected required array to match encoded key %q, got %+v", encodedParamKey, reqArr)
	}
	if !strings.Contains(appendSys, "(keys: "+encodedParamKey+")") {
		t.Fatalf("expected buildAppendSystemPrompt keys to match tools/list %q, got:\n%s", encodedParamKey, appendSys)
	}

	// Simulate gateway restart hydration via SeedFromToolsCatalog and verify argument key/value restoration.
	sess.Codec = NewReversibleSessionCodec(sess.TenantKey)
	sess.Codec.SeedFromToolsCatalog(sess.InitialToolsCatalog)
	dshPhraseAlias := sess.Codec.EncodeOutboundText("DeepSeek Harness")
	sess.mu.Lock()
	effName, effInput, _, errMsg := sess.resolveEffectiveToolLocked(cat[0].Name, map[string]any{
		encodedParamKey: "/Applications/" + dshPhraseAlias + ".app",
	})
	sess.mu.Unlock()
	if errMsg != "" || effName != "dsh_inspect" || effInput["claude_code_mode"] != "/Applications/DeepSeek Harness.app" {
		t.Fatalf("expected full roundtrip restoration of tool name & schema key/value, got name=%q input=%+v err=%q", effName, effInput, errMsg)
	}
}

func TestRound3Proto05Proto06Proto13AndUpP14ResponsesAndChatCompleteness(t *testing.T) {
	baseURL, cleanup := startTestGateway(t)
	defer cleanup()

	// 1. PROTO-13: shortItemID never panics on short/empty IDs.
	if got := shortItemID("fc_", "c1"); got != "fc_c1" {
		t.Fatalf("unexpected shortItemID for c1: %q", got)
	}
	if got := shortItemID("ctc_", ""); !strings.HasPrefix(got, "ctc_") || len(got) <= 4 {
		t.Fatalf("unexpected shortItemID for empty id: %q", got)
	}

	// 2. PROTO-05 + PROTO-06 + PROTO-13: /v1/responses custom tool line-1 hint, coalesced history,
	// id fallback + map output marshaling, and non-streaming parity fields.
	respPayload, _ := json.Marshal(map[string]any{
		"model":               "gemini-3.8-flash",
		"max_output_tokens":   128,
		"parallel_tool_calls": false,
		"tool_choice":         "auto",
		"tools": []any{
			map[string]any{
				"type":        "custom",
				"name":        "apply_patch",
				"description": "Patch workspace files cleanly.\nDetailed multi-line grammar omitted.",
			},
		},
		"input": []any{
			map[string]any{"type": "message", "role": "user", "content": "fix bug"},
			map[string]any{"type": "reasoning", "id": "rs_1", "summary": []any{map[string]any{"type": "summary_text", "text": "thk"}}},
			map[string]any{"type": "message", "role": "assistant", "content": "Applying patch and checking status."},
			map[string]any{"type": "custom_tool_call", "id": "c1", "name": "apply_patch", "input": "*** Begin Patch"},
			map[string]any{"type": "function_call", "id": "c2", "name": "Bash", "arguments": `{"command":"git status"}`},
			map[string]any{"type": "custom_tool_call_output", "id": "c1", "output": map[string]any{"applied": true, "files": 1}},
			map[string]any{"type": "function_call_output", "call_id": "c2", "output": "clean"},
			map[string]any{"type": "message", "role": "user", "content": "Great, summarize."},
		},
	})
	r1, err := http.Post(baseURL+"/v1/responses", "application/json", bytes.NewReader(respPayload))
	if err != nil {
		t.Fatal(err)
	}
	var d1 map[string]any
	_ = json.NewDecoder(r1.Body).Decode(&d1)
	r1.Body.Close()
	if r1.StatusCode != http.StatusOK {
		t.Fatalf("/v1/responses failed: %d %+v", r1.StatusCode, d1)
	}
	if _, hasErrKey := d1["error"]; !hasErrKey || d1["error"] != nil {
		t.Fatalf("PROTO-13 expected explicit error:nil in non-streaming /v1/responses, got %+v", d1)
	}
	if d1["store"] != false || d1["parallel_tool_calls"] != false || d1["tool_choice"] != "auto" {
		t.Fatalf("PROTO-13 missing parity echo fields in non-streaming /v1/responses: %+v", d1)
	}

	// Verify sanitizeToolsCatalog preserved custom tool's Pass free-form text instruction on line 1.
	normCustomCat := sanitizeToolsCatalog([]any{
		map[string]any{
			"name":        "apply_patch",
			"description": "Patch workspace files cleanly. (Pass free-form text in arguments.input.)",
			"parameters": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"input": map[string]any{"type": "string", "description": "Raw free-form text input for this custom tool."},
				},
				"required": []string{"input"},
			},
		},
	})
	if len(normCustomCat) == 0 || !strings.Contains(normCustomCat[0].Description, "Pass free-form text in arguments.input.") {
		t.Fatalf("PROTO-06 custom tool instruction lost: %+v", normCustomCat)
	}

	// 3. UP-P1-4: /v1/responses stream:true pre-commit error returns non-200 application/json (NOT text/event-stream).
	preErrBody, _ := json.Marshal(map[string]any{
		"model":  "gemini-3.8-flash",
		"stream": true,
		"input":  "[FAKE_RPC_400]",
	})
	rPre, err := http.Post(baseURL+"/v1/responses", "application/json", bytes.NewReader(preErrBody))
	if err != nil {
		t.Fatal(err)
	}
	preBytes, _ := io.ReadAll(rPre.Body)
	rPre.Body.Close()
	if rPre.StatusCode != http.StatusBadRequest || !strings.Contains(rPre.Header.Get("Content-Type"), "application/json") {
		t.Fatalf("UP-P1-4 expected pre-commit 400 application/json, got status=%d ct=%q body=%s", rPre.StatusCode, rPre.Header.Get("Content-Type"), string(preBytes))
	}
	var preJSON map[string]any
	if err := json.Unmarshal(preBytes, &preJSON); err != nil || preJSON["error"] == nil {
		t.Fatalf("UP-P1-4 expected parseable OpenAI JSON error body, got err=%v body=%s", err, string(preBytes))
	}

	// Post-commit error (after partial text already flushed 200 text/event-stream) MUST still emit SSE response.failed.
	postErrBody, _ := json.Marshal(map[string]any{
		"model":  "gemini-3.8-flash",
		"stream": true,
		"input":  "[FAKE_LATE_NOTICE]",
	})
	rPost, err := http.Post(baseURL+"/v1/responses", "application/json", bytes.NewReader(postErrBody))
	if err != nil {
		t.Fatal(err)
	}
	postBytes, _ := io.ReadAll(rPost.Body)
	rPost.Body.Close()
	if rPost.StatusCode != http.StatusOK || !strings.Contains(rPost.Header.Get("Content-Type"), "text/event-stream") || !strings.Contains(string(postBytes), "event: response.failed") {
		t.Fatalf("expected post-commit /v1/responses error to emit SSE response.failed, got status=%d ct=%q body=%s", rPost.StatusCode, rPost.Header.Get("Content-Type"), string(postBytes))
	}

	// 4. UP-P1-4: GET /v1/models exposes max_input_tokens and max_tokens.
	rMod, err := http.Get(baseURL + "/v1/models")
	if err != nil {
		t.Fatal(err)
	}
	var modPayload struct {
		Data []map[string]any `json:"data"`
	}
	_ = json.NewDecoder(rMod.Body).Decode(&modPayload)
	rMod.Body.Close()
	if len(modPayload.Data) == 0 {
		t.Fatal("expected non-empty /v1/models list")
	}
	for _, m := range modPayload.Data {
		if m["max_input_tokens"] != m["context_window"] || toIntOrZero(m["max_tokens"]) != 65536 {
			t.Fatalf("UP-P1-4 expected max_input_tokens==context_window and max_tokens=65536, got %+v", m)
		}
	}
}

func toIntOrZero(v any) int {
	n, _ := toInt(v)
	return n
}

func TestRound3Proto07AndProto16CleanroomIndentationAndWafAutobisect(t *testing.T) {
	// 1. PROTO-07: Preserve line-1 horizontal indentation for code & unified git diffs.
	codeSnippet := "    if err != nil {\n        return err // Claude Code helper\n    }\n"
	gotCode := SanitizeOutboundText(codeSnippet)
	if !strings.HasPrefix(gotCode, "    if err != nil {") || strings.Contains(gotCode, "Claude Code") {
		t.Fatalf("PROTO-07 lost line-1 4-space indentation or leaked vendor name: %q", gotCode)
	}
	diffSnippet := " unchanged context line\n-old line\n+new line\n"
	gotDiff := SanitizeOutboundText(diffSnippet)
	if !strings.HasPrefix(gotDiff, " unchanged context line\n-old line\n+new line") {
		t.Fatalf("PROTO-07 corrupted unified diff leading context space: %q", gotDiff)
	}
	withLeadingReminder := "   <system-reminder>Check tests.</system-reminder>\nHello"
	gotRem := SanitizeOutboundText(withLeadingReminder)
	if strings.HasPrefix(gotRem, " ") {
		t.Fatalf("expected leading spaces before <system-reminder> not to be preserved as code indent: %q", gotRem)
	}

	// 2. PROTO-16: Strip orphan markdown bullets after dynamic WAF rule replacement.
	tmp := t.TempDir()
	rulesFile := filepath.Join(tmp, "waf_rewrite_rules.json")
	origRulesFile := WafRewriteRulesFile
	WafRewriteRulesFile = rulesFile
	resetWafRuleCacheForTest()
	defer func() {
		WafRewriteRulesFile = origRulesFile
		resetWafRuleCacheForTest()
	}()
	if err := persistDynamicWafRule("Forbidden harness telemetry sentence here.", "system"); err != nil {
		t.Fatal(err)
	}
	sysIn := "- Keep responses concise.\n- Forbidden harness telemetry sentence here.\n- Run unit tests."
	sysOut := ApplyDynamicWafRules(sysIn)
	if strings.Contains(sysOut, "\n-\n") || strings.Contains(sysOut, "\n- \n") || strings.Contains(sysOut, "Forbidden harness") {
		t.Fatalf("PROTO-16 orphan bullet left behind in ApplyDynamicWafRules: %q", sysOut)
	}
	ctxIn := "<workspace-context>\n* Forbidden harness telemetry sentence here.\n</workspace-context>"
	if gotCtx := ApplyDynamicWafRulesInWorkspaceContext(ctxIn); gotCtx != "" {
		t.Fatalf("expected empty workspace-context after stripping sole bullet rule, got %q", gotCtx)
	}

	// 3. PROTO-16: extractBisectCandidates supports up to 256 sentences and preserves <workspace-context>.
	var bigSysLines []string
	for i := 0; i < 230; i++ {
		bigSysLines = append(bigSysLines, "Project coding guideline number "+strings.Repeat("x", 8)+string(rune('A'+(i%26)))+string(rune('a'+((i/26)%26)))+".")
	}
	promptBlks := []map[string]any{
		{"type": "text", "text": "<workspace-context>\nInjected reminder sentence that triggered abuse filter.\n</workspace-context>\nUser question"},
	}
	cands := extractBisectCandidates(strings.Join(bigSysLines, "\n"), promptBlks)
	if len(cands) != 231 {
		t.Fatalf("PROTO-16 expected 231 bisect candidates (>64), got %d", len(cands))
	}
	foundCtx := false
	for _, c := range cands {
		if c.InPrompt && strings.Contains(c.Text, "Injected reminder sentence") {
			foundCtx = true
		}
	}
	if !foundCtx {
		t.Fatal("PROTO-16 expected <workspace-context> candidate preserved alongside >200 system sentences")
	}

	// 4. PROTO-16: classifyUpstreamError maps failed_precondition -> 400 and no capacity available -> 529.
	if code, typ, retry, _ := classifyUpstreamError(os.ErrInvalid); code == 0 {
		_ = typ
		_ = retry
	}
	if code, typ, retry, _ := classifyUpstreamError(&stringErr{"FAILED_PRECONDITION: location is not supported"}); code != 400 || typ != "invalid_request_error" || retry {
		t.Fatalf("expected 400 invalid_request_error for FAILED_PRECONDITION, got %d %s retry=%v", code, typ, retry)
	}
	if code, typ, retry, _ := classifyUpstreamError(&stringErr{"no capacity available for model gemini-4-carbon"}); code != 529 || typ != "overloaded_error" || !retry {
		t.Fatalf("expected 529 overloaded_error for no capacity available, got %d %s retry=%v", code, typ, retry)
	}
}

type stringErr struct{ s string }

func (e *stringErr) Error() string { return e.s }

func TestRound3Proto08Proto11Proto12Proto15AndOpt05Security(t *testing.T) {
	gs := NewGatewayServer()
	defer gs.Close()
	gwOnly := gs.GatewayOnlyHandler()

	// 1. PROTO-08 / BUG-UI-01: GatewayOnlyHandler blocks /api/upgrade, /api/upgrade/approve, and all /api/*.
	for _, blockedPath := range []string{"/", "/dashboard", "/metrics", "/api/dashboard", "/api/quota", "/api/upgrade", "/api/upgrade/approve", "/api/future-admin"} {
		method := http.MethodGet
		if strings.HasSuffix(blockedPath, "/approve") {
			method = http.MethodPost
		}
		req := httptest.NewRequest(method, blockedPath, nil)
		req.Host = "127.0.0.1:18890"
		rec := httptest.NewRecorder()
		gwOnly.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound || !strings.Contains(rec.Body.String(), "Not found on gateway API port") {
			t.Fatalf("PROTO-08 expected 404 on GatewayOnlyHandler for %s %s, got %d %s", method, blockedPath, rec.Code, rec.Body.String())
		}
	}
	// Public gateway endpoints remain open on GatewayOnlyHandler.
	for _, okPath := range []string{"/healthz", "/v1/models"} {
		req := httptest.NewRequest(http.MethodGet, okPath, nil)
		req.Host = "127.0.0.1:18890"
		rec := httptest.NewRecorder()
		gwOnly.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 on GatewayOnlyHandler for %s, got %d %s", okPath, rec.Code, rec.Body.String())
		}
	}

	// 2. PROTO-11: Secret redaction for Basic auth, ?key=, &api_key=, /key=, and signed image error URLs.
	rawSecret := `Authorization: Basic dXNlcjpzdXBlcnNlY3JldA== url=https://host/v1beta/models/gemini-4-carbon/key=AIzaPathKey123:generateContent?key=AIzaQueryKey456&api_key=sk-secret789`
	redacted := redactSecretsInString(rawSecret)
	for _, leak := range []string{"dXNlcjpzdXBlcnNlY3JldA==", "AIzaPathKey123", "AIzaQueryKey456", "sk-secret789"} {
		if strings.Contains(redacted, leak) {
			t.Fatalf("PROTO-11 leaked %q in %q", leak, redacted)
		}
	}
	errRedacted := redactURLSecretsForError(`Get "https://cdn.example.com/img.png?X-Amz-Signature=deadbeef&se=2026": dial tcp timeout`)
	if strings.Contains(errRedacted, "deadbeef") || strings.Contains(errRedacted, "2026") {
		t.Fatalf("PROTO-11 leaked signed URL query params in image error: %q", errRedacted)
	}

	// 3. PROTO-12: inlineImageDigest full sha256 sensitivity to middle bytes + cacheOrder seq safety.
	midA := append(append([]byte(nil), pngBytes()...), bytes.Repeat([]byte{0xAA}, 200)...)
	midB := append([]byte(nil), midA...)
	midB[100] = 0xBB // differs strictly outside 64B head and 64B tail!
	if inlineImageDigest("image/png", midA) == inlineImageDigest("image/png", midB) {
		t.Fatal("PROTO-12 collision: inlineImageDigest ignored middle bytes")
	}

	f := newRemoteImageFetcher(nil)
	now := time.Now()
	// Put keyA (24MB), then re-put keyA (leaving an old seq=1 entry at cacheOrder[0]), then put keyB (24MB), then keyC (24MB -> triggers eviction).
	// Oldest LIVE entry is keyB? No: keyA was updated at seq=2, then keyB at seq=3: Wait — if keyB (24MB) was inserted at seq=1, keyA (24MB) at seq=2,
	// then keyB expired/re-inserted at seq=3, cacheOrder is [B(1), A(2), B(3)]. Adding keyC (24MB) puts total=72MB > 64MB:
	// B(1) is a stale tombstone (cur.seq=3 != 1), so A(2) MUST be evicted while fresh B(3) survives!
	mkItem := func() cachedRemoteImage {
		return cachedRemoteImage{mime: "image/png", b64: strings.Repeat("A", 24*1024*1024), rawBytes: 1024, digest: "d"}
	}
	f.putCached("keyB", mkItem(), now)
	f.putCached("keyA", mkItem(), now.Add(time.Second))
	f.putCached("keyB", mkItem(), now.Add(2*time.Second))
	f.putCached("keyC", mkItem(), now.Add(3*time.Second))
	if _, hasB := f.getCached("keyB", now.Add(4*time.Second)); !hasB {
		t.Fatal("PROTO-12 stale tombstone in cacheOrder prematurely evicted refreshed keyB")
	}
	if _, hasA := f.getCached("keyA", now.Add(4*time.Second)); hasA {
		t.Fatal("expected older keyA to be evicted before refreshed keyB")
	}

	// 4. PROTO-15 + OPT-05: Dual-stack loopback, loopback-before-404 on /mcp/*, and ::/96 + ::ffff:0:0/96 SSRF block.
	for _, good := range []string{"127.0.0.1:8080", "[::1]:8080", "::1", "[::ffff:127.0.0.1]:18890", "localhost:18890"} {
		if !isLoopbackRemoteAddr(good) {
			t.Fatalf("PROTO-15 expected loopback %q allowed", good)
		}
	}
	for _, bad := range []string{"203.0.113.10:8080", "[::ffff:10.0.0.8]:8080", "[2001:db8::1]:8080"} {
		if isLoopbackRemoteAddr(bad) {
			t.Fatalf("PROTO-15 expected non-loopback %q rejected", bad)
		}
	}
	nonLoopMcp := httptest.NewRequest(http.MethodPost, "/mcp/unknown-tenant-probe", strings.NewReader(`{}`))
	nonLoopMcp.RemoteAddr = "203.0.113.99:44444"
	recMcp := httptest.NewRecorder()
	gs.ServeHTTP(recMcp, nonLoopMcp)
	if recMcp.Code != http.StatusForbidden {
		t.Fatalf("PROTO-15 expected 403 Forbidden before tenant lookup for remote IP, got %d", recMcp.Code)
	}
	for _, ssrfIP := range []string{"::127.0.0.1", "::ffff:0:127.0.0.1", "::ffff:0:10.0.0.1"} {
		if isPublicNetipAddr(netip.MustParseAddr(ssrfIP)) {
			t.Fatalf("OPT-05 expected IPv6 transition address %q blocked", ssrfIP)
		}
	}
}

func TestRound3Proto09AndProto14RolloverAndGeminiV1BetaFixes(t *testing.T) {
	// 1. PROTO-09: Rollover preserves tool_result before failed assistant turn, keeps UTF-8 valid,
	// and keeps tool_results in currentMsgs when followed by user text.
	cjkChunk := strings.Repeat("中文🙂测试边界", 80)
	rollMsgs := []map[string]any{
		{"role": "user", "content": cjkChunk},
		{"role": "assistant", "content": []any{
			map[string]any{"type": "tool_use", "id": "tu_keep", "name": "Bash", "input": map[string]any{"command": "ls"}},
		}},
		{"role": "user", "content": []any{
			map[string]any{"type": "tool_result", "tool_use_id": "tu_keep", "content": "IMPORTANT_TOOL_OUTPUT_SURVIVES"},
		}},
		{"role": "assistant", "content": "⚠ Agent execution terminated due to error: INTERNAL (code 500): transient"},
		{"role": "user", "content": "Retry now"},
	}
	blocks, err := resolveAndBuildPromptBlocks(context.Background(), GlobalImageFetcher, rollMsgs[4:], true, rollMsgs, 350, ModelOrDefault("gemini-4-carbon"))
	if err != nil {
		t.Fatalf("rollover failed: %v", err)
	}
	resTxt, _ := blocks[0]["resource"].(map[string]any)["text"].(string)
	curTxt, _ := blocks[len(blocks)-1]["text"].(string)
	if !utf8.ValidString(resTxt) || !utf8.ValidString(curTxt) {
		t.Fatal("PROTO-09 rollover mid-rune slice produced invalid UTF-8")
	}
	if !strings.Contains(resTxt+"\n"+curTxt, "IMPORTANT_TOOL_OUTPUT_SURVIVES") {
		t.Fatalf("PROTO-09 failed assistant turn cleanup popped preceding tool_result! hist=%s\ncur=%s", resTxt, curTxt)
	}

	curCheck := []map[string]any{
		{"role": "user", "content": "init"},
		{"role": "assistant", "content": []any{map[string]any{"type": "tool_use", "id": "t1", "name": "Bash", "input": map[string]any{}}}},
		{"role": "user", "content": []any{map[string]any{"type": "tool_result", "tool_use_id": "t1", "content": "ok"}}},
		{"role": "user", "content": "Now summarize"},
	}
	if start := rolloverCurrentTurnStart(curCheck); start != 2 {
		t.Fatalf("PROTO-09 expected rolloverCurrentTurnStart=2 to keep tool_result in current turn, got %d", start)
	}

	// 2. PROTO-14: Gemini /v1beta mid-turn system guard, omitted-role thought inference, and thoughtsTokenCount fallback.
	gemNorm, err := normalizeGeminiV1BetaRequest("gemini-4-carbon", map[string]any{
		"systemInstruction": "Initial system rule.",
		"contents": []any{
			map[string]any{"role": "user", "parts": []any{map[string]any{"text": "Hello"}}},
			map[string]any{
				// Omitted role with thought:true + answer text -> BOTH must belong to assistant!
				"parts": []any{
					map[string]any{"thought": true, "text": "Let me reason..."},
					map[string]any{"text": "First assistant answer"},
				},
			},
			map[string]any{"role": "developer", "parts": []any{map[string]any{"text": "Mid-turn developer steer"}}},
		},
	}, false)
	if err != nil {
		t.Fatalf("normalizeGeminiV1BetaRequest failed: %v", err)
	}
	if gemNorm.Body["system"] != "Initial system rule." {
		t.Fatalf("PROTO-14 mid-turn developer role mutated systemInstruction: %q", gemNorm.Body["system"])
	}
	if len(gemNorm.Messages) != 3 || gemNorm.Messages[1]["role"] != "assistant" || gemNorm.Messages[2]["role"] != "user" {
		t.Fatalf("PROTO-14 unexpected normalized roles: %+v", gemNorm.Messages)
	}
	usageMeta := buildGeminiTurnUsageMetadata(TurnResult{
		Thinking: "Some model thought text here",
		Text:     "Answer",
		Usage:    &TurnUsage{InputTokens: 100, OutputTokens: 30, ThinkingTokens: 0},
	})
	if toIntOrZero(usageMeta["thoughtsTokenCount"]) <= 0 {
		t.Fatalf("PROTO-14 expected thoughtsTokenCount fallback > 0, got %+v", usageMeta)
	}
}

func TestRound3UpstreamFeaturesP01P02P03P11P12P13AndOpt01To04(t *testing.T) {
	// 1. UP-P0-1: agy 1.5.10 <1s spend-cap/prepaid/weekly quota fast-stop exclusions, 403 license, 400 safety/policy.
	for _, hardCap := range []string{
		"RESOURCE_EXHAUSTED (code 429): Spending cap reached for this billing period.",
		"RESOURCE_EXHAUSTED (code 429): Prepaid credit balance is depleted.",
		"429 Too Many Requests: Weekly quota limit exceeded.",
		"RESOURCE_EXHAUSTED: Daily quota exhausted.",
	} {
		se := &SoftError{StatusCode: 429, ErrorType: "rate_limit_error", Message: hardCap}
		if isUpstreamPoolCapacitySoftError(se) {
			t.Fatalf("UP-P0-1 expected isUpstreamPoolCapacitySoftError=false for %q", hardCap)
		}
		if isBisectableInstant429Detail(hardCap) {
			t.Fatalf("UP-P0-1 expected isBisectableInstant429Detail=false for %q", hardCap)
		}
	}
	transientCap := &SoftError{StatusCode: 429, ErrorType: "rate_limit_error", Message: "RESOURCE_EXHAUSTED (code 429): Resource has been exhausted."}
	if !isUpstreamPoolCapacitySoftError(transientCap) || !isBisectableInstant429Detail(transientCap.Message) {
		t.Fatal("expected transient capacity 429 to remain eligible for fallback/bisect")
	}

	for _, licErr := range []string{
		"PERMISSION_DENIED: GEMINI_ENTERPRISE_LICENSE_NOT_VALID",
		"403 ccpa_ineligible_tier_id for consumer account",
	} {
		code, typ, retry, _ := classifyUpstreamError(&stringErr{licErr})
		if code != http.StatusForbidden || typ != "permission_error" || retry {
			t.Fatalf("UP-P0-1 expected 403 permission_error retry=false for %q, got %d %s %v", licErr, code, typ, retry)
		}
		if tripped, _ := IsTosTripped(); tripped {
			t.Fatalf("license error %q must NOT trip ToS circuit breaker", licErr)
		}
	}
	if se := DetectAntigravitySoftError("⚠ GEMINI_ENTERPRISE_LICENSE_NOT_VALID: seat missing"); se == nil || se.StatusCode != 403 {
		t.Fatalf("expected DetectAntigravitySoftError 403 on GEMINI_ENTERPRISE_LICENSE_NOT_VALID, got %+v", se)
	}

	for _, polErr := range []string{
		"Blocked: finishReason=SAFETY",
		"400 PROHIBITED_CONTENT: request blocked",
		"content_policy_violation: disallowed prompt",
		"bio_policy violation",
		"RECITATION block triggered",
		"SPII block triggered",
	} {
		code, typ, retry, _ := classifyUpstreamError(&stringErr{polErr})
		if code != http.StatusBadRequest || typ != "invalid_request_error" || retry {
			t.Fatalf("UP-P0-1 expected 400 invalid_request_error retry=false for %q, got %d %s %v", polErr, code, typ, retry)
		}
	}

	// 2. UP-P0-2 + OPT-02: Effort suffixes (-minimal, -max, -ultra) & budget_tokens > 32768 -> xhigh.
	if sel := ResolveRequestedModel("claude-opus-5-5-max", nil); sel.Model.ID != "claude-opus-5-5" || sel.Tier != "high" || !sel.ExplicitTier {
		t.Fatalf("UP-P0-2 claude-opus-5-5-max got %+v", sel)
	}
	if sel := ResolveRequestedModel("gemini-4-hydrogen-max", nil); sel.Model.ID != "gemini-4-hydrogen" || sel.Tier != "xhigh" || !sel.ExplicitTier {
		t.Fatalf("UP-P0-2 gemini-4-hydrogen-max got %+v", sel)
	}
	if sel := ResolveRequestedModel("gemini-4-hydrogen-ultra", nil); sel.Model.ID != "gemini-4-hydrogen" || sel.Tier != "xhigh" {
		t.Fatalf("UP-P0-2 gemini-4-hydrogen-ultra got %+v", sel)
	}
	if sel := ResolveRequestedModel("gemini-3.8-flash-minimal", nil); sel.Model.ID != "gemini-3.8-flash" || sel.Tier != "low" || !sel.ExplicitTier {
		t.Fatalf("UP-P0-2 gemini-3.8-flash-minimal got %+v", sel)
	}
	if sel := ResolveRequestedModel("gemini-4-hydrogen", map[string]any{
		"thinking": map[string]any{"type": "enabled", "budget_tokens": 65536},
	}); sel.Tier != "xhigh" || sel.ExplicitTier {
		t.Fatalf("OPT-02 expected budget_tokens=65536 -> xhigh with ExplicitTier=false, got %+v", sel)
	}

	// 3. UP-P1-2: Dynamic Retry-After extraction.
	if got := extractRetryAfterHeaderValue("Please retry in 42.2s."); got != "43" {
		t.Fatalf("UP-P1-2 expected 43, got %q", got)
	}
	if got := extractRetryAfterHeaderValue("Individual quota reached. Resets in 2m 15s."); got != "135" {
		t.Fatalf("UP-P1-2 expected 135, got %q", got)
	}
	if got := extractRetryAfterHeaderValue("Resets in 4h 10m"); got != "3600" {
		t.Fatalf("UP-P1-2 expected 3600 clamp, got %q", got)
	}
	if got := extractRetryAfterHeaderValue("retry in 1s"); got != "5" {
		t.Fatalf("UP-P1-2 expected 5 floor, got %q", got)
	}
	if got := extractRetryAfterHeaderValue("transient error without duration"); got != "5" {
		t.Fatalf("UP-P1-2 expected fallback 5, got %q", got)
	}

	// 4. UP-P1-3: 64-bit integer precision (> 2^53) preserved via unmarshalJSONPreserveNumbers.
	var numMap map[string]any
	if err := unmarshalJSONPreserveNumbers([]byte(`{"snowflake":1234567890123456789}`), &numMap); err != nil {
		t.Fatal(err)
	}
	reMarshaled, _ := json.Marshal(numMap)
	if string(reMarshaled) != `{"snowflake":1234567890123456789}` {
		t.Fatalf("UP-P1-3 lost 64-bit integer precision: %s", string(reMarshaled))
	}

	// 5. OPT-03 & OPT-04: CORS whitelisted headers & Gemini CLI tool-signature detection.
	id := DetectClientIdentity(httptest.NewRequest(http.MethodPost, "/v1beta/models/gemini-4-carbon:generateContent", nil), nil, "fp:test", "", "", []any{
		map[string]any{"functionDeclarations": []any{
			map[string]any{"name": "run_shell_command"},
			map[string]any{"name": "replace"},
		}},
	})
	if id.ID != "gemini_cli" {
		t.Fatalf("OPT-04 expected gemini_cli from run_shell_command+replace signature, got %+v", id)
	}

	origCors := AllowedCorsOrigins
	AllowedCorsOrigins = map[string]bool{"https://app.example.com": true}
	defer func() { AllowedCorsOrigins = origCors }()

	baseURL, cleanup := startTestGateway(t)
	defer cleanup()

	optReq, _ := http.NewRequest(http.MethodOptions, baseURL+"/v1/messages", nil)
	optReq.Header.Set("Origin", "https://app.example.com")
	optResp, err := http.DefaultClient.Do(optReq)
	if err != nil {
		t.Fatal(err)
	}
	optResp.Body.Close()
	allowHdrs := optResp.Header.Get("Access-Control-Allow-Headers")
	for _, wantH := range []string{"anthropic-beta", "anthropic-dangerous-direct-browser-access", "OpenAI-Beta", "x-pi-session-id"} {
		if !strings.Contains(allowHdrs, wantH) {
			t.Fatalf("OPT-03 missing %q in Access-Control-Allow-Headers: %q", wantH, allowHdrs)
		}
	}

	// 6. UP-P0-3: Reject previous_response_id (400) on /v1/responses and thread (404) on /v1/messages + count_tokens.
	prevReq, _ := json.Marshal(map[string]any{
		"model":                "gemini-3.8-flash",
		"previous_response_id": "resp_prev_999",
		"input":                "continue",
	})
	rPrev, err := http.Post(baseURL+"/v1/responses", "application/json", bytes.NewReader(prevReq))
	if err != nil {
		t.Fatal(err)
	}
	prevOut, _ := io.ReadAll(rPrev.Body)
	rPrev.Body.Close()
	if rPrev.StatusCode != http.StatusBadRequest || !strings.Contains(string(prevOut), "previous_response_not_found") {
		t.Fatalf("UP-P0-3 expected 400 previous_response_not_found, got %d: %s", rPrev.StatusCode, string(prevOut))
	}

	for _, ep := range []string{"/v1/messages", "/v1/messages/count_tokens"} {
		thReq, _ := json.Marshal(map[string]any{
			"model":    "claude-sonnet-5-5",
			"thread":   map[string]any{"id": "msgthread_abc"},
			"messages": []any{map[string]any{"role": "user", "content": "hi"}},
		})
		rTh, err := http.Post(baseURL+ep, "application/json", bytes.NewReader(thReq))
		if err != nil {
			t.Fatal(err)
		}
		thOut, _ := io.ReadAll(rTh.Body)
		rTh.Body.Close()
		if rTh.StatusCode != http.StatusNotFound || !strings.Contains(string(thOut), "thread_not_found") {
			t.Fatalf("UP-P0-3 expected 404 thread_not_found on %s, got %d: %s", ep, rTh.StatusCode, string(thOut))
		}
	}

	// 7. UP-P1-1 + OPT-01: Unified Structured Output (count_tokens + 3 protocols) & cache-preserving tool_choice.
	baseCT, _ := estimateCountTokens([]byte(`{"model":"claude-sonnet-5-5","messages":[{"role":"user","content":"extract json"}]}`))
	withSchemaCT, err := estimateCountTokens([]byte(`{"model":"claude-sonnet-5-5","messages":[{"role":"user","content":"extract json"}],"output_config":{"format":{"type":"json_schema","schema":{"type":"object","properties":{"summary":{"type":"string"},"score":{"type":"integer"}},"required":["summary","score"]}}}}`))
	if err != nil || withSchemaCT <= baseCT {
		t.Fatalf("UP-P1-1 expected count_tokens with output_config.format (%d) > base (%d), err=%v", withSchemaCT, baseCT, err)
	}
	for _, tc := range []struct {
		name string
		body map[string]any
	}{
		{"anthropic", map[string]any{"output_config": map[string]any{"format": map[string]any{"type": "json_schema", "schema": map[string]any{"type": "object"}}}}},
		{"chat", map[string]any{"response_format": map[string]any{"type": "json_schema", "json_schema": map[string]any{"name": "res", "schema": map[string]any{"type": "object"}}}}},
		{"responses", map[string]any{"text": map[string]any{"format": map[string]any{"type": "json_schema", "name": "res", "schema": map[string]any{"type": "object"}}}}},
	} {
		inst := extractStructuredOutputInstruction(tc.body)
		if !strings.Contains(inst, "JSON Schema") {
			t.Fatalf("UP-P1-1 %s structured output instruction missing schema: %q", tc.name, inst)
		}
	}

	// End-to-end OPT-01 cache-preserving tool_choice="none" -> "auto" on the same warm session!
	tools := []map[string]any{
		{
			"name":        "Bash",
			"description": "Run shell command",
			"input_schema": map[string]any{
				"type":       "object",
				"properties": map[string]any{"command": map[string]any{"type": "string"}},
				"required":   []string{"command"},
			},
		},
	}
	tcNoneBody, _ := json.Marshal(map[string]any{
		"model":       "gemini-3.8-flash",
		"max_tokens":  128,
		"system":      "Primary working directory: /tmp",
		"tools":       tools,
		"tool_choice": "none",
		"messages":    []map[string]any{{"role": "user", "content": "[FAKE_TOOL_CALL] summarize without calling tools"}},
	})
	req1, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/messages", bytes.NewReader(tcNoneBody))
	req1.Header.Set("Content-Type", "application/json")
	req1.Header.Set("x-claude-code-session-id", "opt01-tool-choice-cache")
	res1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatal(err)
	}
	var m1 map[string]any
	_ = json.NewDecoder(res1.Body).Decode(&m1)
	res1.Body.Close()
	if res1.StatusCode != http.StatusOK || m1["stop_reason"] == "tool_use" {
		t.Fatalf("OPT-01 tool_choice=none must block tool_use and return assistant text, got status=%d body=%+v", res1.StatusCode, m1)
	}
	rawM1, _ := json.Marshal(m1["content"])
	if !strings.Contains(string(rawM1), "tool_choice=none") {
		t.Fatalf("expected fakeacp to receive MCP rejection mentioning tool_choice=none, got %s", string(rawM1))
	}

	// Turn 2 on the SAME warm session with tool_choice="auto" reuses the session (sticky_delta, 0 rollovers)!
	tcAutoBody, _ := json.Marshal(map[string]any{
		"model":       "gemini-3.8-flash",
		"max_tokens":  128,
		"system":      "Primary working directory: /tmp",
		"tools":       tools,
		"tool_choice": "auto",
		"messages": []any{
			map[string]any{"role": "user", "content": "[FAKE_TOOL_CALL] summarize without calling tools"},
			map[string]any{"role": "assistant", "content": m1["content"]},
			map[string]any{"role": "user", "content": "Now reply normally"},
		},
	})
	req2, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/messages", bytes.NewReader(tcAutoBody))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("x-claude-code-session-id", "opt01-tool-choice-cache")
	res2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	var m2 map[string]any
	_ = json.NewDecoder(res2.Body).Decode(&m2)
	res2.Body.Close()
	if res2.StatusCode != http.StatusOK {
		t.Fatalf("OPT-01 warm follow-up with tool_choice=auto failed: %d %+v", res2.StatusCode, m2)
	}
	rawM2, _ := json.Marshal(m2["content"])
	if !strings.Contains(string(rawM2), "turn=2") {
		t.Fatalf("OPT-01 expected warm sticky session reuse (turn=2, 0 rollover), got %s", string(rawM2))
	}

	// End-to-end UP-P1-3 MCP tools/call 64-bit integer (> 2^53) preservation!
	sess := NewAcpProjectSession("sid:forced-tool", "/tmp", "")
	sess.ToolsCatalog = sanitizeToolsCatalog([]any{
		map[string]any{"name": "Bash", "input_schema": map[string]any{"type": "object"}},
		map[string]any{"name": "Read", "input_schema": map[string]any{"type": "object"}},
	})
	sess.InitialToolsCatalog = sess.ToolsCatalog
	sess.McpToHostMap = map[string]string{
		sess.ToolsCatalog[0].McpName: "Bash",
		sess.ToolsCatalog[1].McpName: "Read",
	}
	sess.PromptInFlight = true
	sess.SessionID = "00000000-0000-4000-8000-000000009999"
	turn := newActiveTurnState(nil, nil)
	sess.activeTurn = turn
	mcpCallDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		mcpReq := httptest.NewRequest(http.MethodPost, "/mcp/"+sess.TenantKey, strings.NewReader(
			`{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"`+sess.ToolsCatalog[1].McpName+`","arguments":{"snowflake_id":9007199254740993,"big_id":1234567890123456789}}}`,
		))
		mcpReq.RemoteAddr = "[::ffff:127.0.0.1]:55555"
		mcpReq.Header.Set("Authorization", "Bearer "+sess.TenantKey)
		rec := httptest.NewRecorder()
		sess.HandleMcpHTTP(rec, mcpReq)
		mcpCallDone <- rec
	}()
	select {
	case turnRes := <-turn.resolveChan:
		if len(turnRes.ToolCalls) != 1 {
			t.Fatalf("expected 1 parked tool call, got %+v", turnRes.ToolCalls)
		}
		inJSON, _ := json.Marshal(turnRes.ToolCalls[0].Input)
		if !strings.Contains(string(inJSON), `"snowflake_id":9007199254740993`) || !strings.Contains(string(inJSON), `"big_id":1234567890123456789`) {
			t.Fatalf("UP-P1-3 MCP tools/call lost 64-bit integer precision: %s", string(inJSON))
		}
		sess.CancelActiveTurn("test_cleanup")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for MCP tools/call to park")
	}
	<-mcpCallDone

	sess.activeTurnToolChoice = parseClientToolChoice(map[string]any{"type": "tool", "name": "Read"})
	sess.mu.Lock()
	_, _, _, bashReject := sess.resolveEffectiveToolLocked("Bash", map[string]any{})
	_, _, _, readAllow := sess.resolveEffectiveToolLocked("Read", map[string]any{})
	sess.mu.Unlock()
	if bashReject == "" || readAllow != "" {
		t.Fatalf("OPT-01 forced tool Read should block Bash (%q) and allow Read (%q)", bashReject, readAllow)
	}
}

func TestRound3VerifierPass1ReviewFixes(t *testing.T) {
	origCors := AllowedCorsOrigins
	AllowedCorsOrigins = map[string]bool{"https://app.example.com": true}
	defer func() { AllowedCorsOrigins = origCors }()

	baseURL, cleanup := startTestGateway(t)
	defer cleanup()

	// 1. [UP-P1-1 + OPT-01] Gemini /v1beta preserves full tools catalog while mapping toolConfig -> tool_choice
	// and generationConfig.responseMimeType/responseSchema -> response_format (without mutating systemInstruction).
	gemTools := []any{
		map[string]any{
			"functionDeclarations": []any{
				map[string]any{"name": "Bash", "description": "Run command", "parameters": map[string]any{"type": "OBJECT", "properties": map[string]any{"command": map[string]any{"type": "STRING"}}}},
				map[string]any{"name": "Read", "description": "Read file", "parameters": map[string]any{"type": "OBJECT", "properties": map[string]any{"path": map[string]any{"type": "STRING"}}}},
			},
		},
	}
	for _, tc := range []struct {
		name      string
		fcc       map[string]any
		wantMode  string
		wantNames string
	}{
		{"NONE", map[string]any{"mode": "NONE"}, "none", ""},
		{"ANY_all", map[string]any{"mode": "ANY"}, "required", ""},
		{"ANY_single", map[string]any{"mode": "ANY", "allowedFunctionNames": []any{"Read"}}, "tool", "Read"},
		{"AUTO_subset", map[string]any{"mode": "AUTO", "allowedFunctionNames": []any{"Read", "Bash"}}, "allowed", "Bash,Read"},
	} {
		norm, err := normalizeGeminiV1BetaRequest("gemini-4-carbon", map[string]any{
			"systemInstruction": "Keep base system stable.",
			"tools":             gemTools,
			"toolConfig":        map[string]any{"functionCallingConfig": tc.fcc},
			"generationConfig": map[string]any{
				"responseMimeType": "application/json",
				"responseSchema": map[string]any{
					"type":       "OBJECT",
					"properties": map[string]any{"ok": map[string]any{"type": "BOOLEAN"}},
					"required":   []any{"ok"},
				},
			},
			"contents": []any{map[string]any{"role": "user", "parts": []any{map[string]any{"text": "hi"}}}},
		}, false)
		if err != nil {
			t.Fatalf("normalizeGeminiV1BetaRequest(%s) failed: %v", tc.name, err)
		}
		normTools, _ := norm.Body["tools"].([]any)
		if len(normTools) != 2 {
			t.Fatalf("Gemini %s stripped declared tools (len=%d, want 2): %+v", tc.name, len(normTools), normTools)
		}
		parsedTC := parseClientToolChoice(norm.Body["tool_choice"])
		if parsedTC.Mode != tc.wantMode || strings.Join(parsedTC.AllowedOrdered, ",") != tc.wantNames {
			t.Fatalf("Gemini %s tool_choice mismatch: got mode=%q allowed=%v, want mode=%q allowed=%q", tc.name, parsedTC.Mode, parsedTC.AllowedOrdered, tc.wantMode, tc.wantNames)
		}
		if norm.Body["system"] != "Keep base system stable." {
			t.Fatalf("Gemini %s structured output mutated systemInstruction: %q", tc.name, norm.Body["system"])
		}
		soInst := extractStructuredOutputInstruction(norm.Body)
		if !strings.Contains(soInst, `"type":"object"`) || !strings.Contains(soInst, `"ok":{"type":"boolean"}`) {
			t.Fatalf("Gemini %s missing normalized response_format instruction: %q", tc.name, soInst)
		}
	}

	// End-to-end Gemini /v1beta: Turn 1 with mode=NONE + declared tools boots MCP bridge & blocks tool_use,
	// so Turn 2 switching to mode=AUTO reuses the SAME warm session (turn=2, 0 cold rollover)!
	gemT1, _ := json.Marshal(map[string]any{
		"systemInstruction": "Primary working directory: /tmp",
		"tools":             gemTools,
		"toolConfig":        map[string]any{"functionCallingConfig": map[string]any{"mode": "NONE"}},
		"contents": []any{
			map[string]any{"role": "user", "parts": []any{map[string]any{"text": "[FAKE_TOOL_CALL] gemini-none-then-auto"}}},
		},
	})
	gr1, err := http.Post(baseURL+"/v1beta/models/gemini-3.8-flash:generateContent", "application/json", bytes.NewReader(gemT1))
	if err != nil {
		t.Fatal(err)
	}
	var gd1 map[string]any
	_ = json.NewDecoder(gr1.Body).Decode(&gd1)
	gr1.Body.Close()
	rawGD1, _ := json.Marshal(gd1)
	if gr1.StatusCode != http.StatusOK || strings.Contains(string(rawGD1), `"functionCall"`) || !strings.Contains(string(rawGD1), "tool_choice=none") {
		t.Fatalf("Gemini Turn 1 mode=NONE failed to block functionCall via MCP rejection: status=%d body=%s", gr1.StatusCode, string(rawGD1))
	}
	cand1Parts, _ := gd1["candidates"].([]any)[0].(map[string]any)["content"].(map[string]any)["parts"].([]any)
	gemT2, _ := json.Marshal(map[string]any{
		"systemInstruction": "Primary working directory: /tmp",
		"tools":             gemTools,
		"toolConfig":        map[string]any{"functionCallingConfig": map[string]any{"mode": "AUTO"}},
		"contents": []any{
			map[string]any{"role": "user", "parts": []any{map[string]any{"text": "[FAKE_TOOL_CALL] gemini-none-then-auto"}}},
			map[string]any{"role": "model", "parts": cand1Parts},
			map[string]any{"role": "user", "parts": []any{map[string]any{"text": "Now reply normally on warm session"}}},
		},
	})
	gr2, err := http.Post(baseURL+"/v1beta/models/gemini-3.8-flash:generateContent", "application/json", bytes.NewReader(gemT2))
	if err != nil {
		t.Fatal(err)
	}
	var gd2 map[string]any
	_ = json.NewDecoder(gr2.Body).Decode(&gd2)
	gr2.Body.Close()
	rawGD2, _ := json.Marshal(gd2)
	if gr2.StatusCode != http.StatusOK || !strings.Contains(string(rawGD2), "turn=2") {
		t.Fatalf("Gemini Turn 2 mode=AUTO expected warm session reuse (turn=2, 0 rollover), got status=%d body=%s", gr2.StatusCode, string(rawGD2))
	}

	// Gemini :countTokens accounts for generationConfig.responseSchema via extractStructuredOutputInstruction.
	ctBaseBody, _ := json.Marshal(map[string]any{
		"contents": []any{map[string]any{"role": "user", "parts": []any{map[string]any{"text": "extract json"}}}},
	})
	ctSchemaBody, _ := json.Marshal(map[string]any{
		"generationConfig": map[string]any{
			"responseMimeType": "application/json",
			"responseSchema": map[string]any{
				"type":       "OBJECT",
				"properties": map[string]any{"summary": map[string]any{"type": "STRING"}, "count": map[string]any{"type": "INTEGER"}},
				"required":   []any{"summary", "count"},
			},
		},
		"contents": []any{map[string]any{"role": "user", "parts": []any{map[string]any{"text": "extract json"}}}},
	})
	countResp := func(b []byte) int {
		resp, errC := http.Post(baseURL+"/v1beta/models/gemini-4-carbon:countTokens", "application/json", bytes.NewReader(b))
		if errC != nil {
			t.Fatal(errC)
		}
		defer resp.Body.Close()
		var out struct {
			TotalTokens int `json:"totalTokens"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return out.TotalTokens
	}
	if gotSchema, gotBase := countResp(ctSchemaBody), countResp(ctBaseBody); gotSchema <= gotBase {
		t.Fatalf("Gemini :countTokens expected schema tokens (%d) > base (%d)", gotSchema, gotBase)
	}

	// 2. [UP-P1-1 / Cleanroom WAF] formatStructuredOutputSchemaHint deep-copies and scrubs description/title
	// while keeping properties keys, required, enum, and const untouched.
	dirtySchema := map[string]any{
		"type":        "object",
		"title":       "Cline Report",
		"description": "Summary for Claude from https://code.claude.com/docs/en/code-mode in DSH",
		"properties": map[string]any{
			"title": map[string]any{
				"type":        "string",
				"description": "Headline for Claude Code",
			},
			"claude_code_mode": map[string]any{
				"type":  "string",
				"enum":  []any{"Claude", "Cline", "DSH"},
				"const": "Claude",
			},
		},
		"required": []any{"title", "claude_code_mode"},
	}
	soScrubbed := extractStructuredOutputInstruction(map[string]any{
		"output_config": map[string]any{
			"format": map[string]any{
				"type":   "json_schema",
				"schema": dirtySchema,
			},
		},
	})
	if dirtySchema["title"] != "Cline Report" {
		t.Fatal("sanitizeStructuredOutputSchema mutated caller map in-place")
	}
	for _, forbidden := range []string{"Cline Report", "code.claude.com/docs", "Summary for Claude", "Headline for Claude Code"} {
		if strings.Contains(soScrubbed, forbidden) {
			t.Fatalf("structured output schema leaked unsanitized docstring %q: %s", forbidden, soScrubbed)
		}
	}
	for _, preserved := range []string{`"claude_code_mode":{`, `"title":{"description":`, `"enum":["Claude","Cline","DSH"]`, `"const":"Claude"`, `"required":["title","claude_code_mode"]`} {
		if !strings.Contains(soScrubbed, preserved) {
			t.Fatalf("structured output schema corrupted structural field %q: %s", preserved, soScrubbed)
		}
	}

	// 3. [PROTO-13] Tool JSON map outputs with domain "type" keys marshal to string (200 OK) on both
	// /v1/responses and /v1/chat/completions, while real input_audio content parts still return 400.
	for _, domObj := range []map[string]any{
		{"type": "file", "path": "a.go"},
		{"type": "audio", "duration": 12},
		{"type": "video", "id": 1},
	} {
		got := normalizeFunctionOutputContent(domObj)
		if _, isStr := got.(string); !isStr {
			t.Fatalf("PROTO-13 expected domain map %+v to marshal to JSON string, got %T (%+v)", domObj, got, got)
		}
	}
	respDomPayload, _ := json.Marshal(map[string]any{
		"model": "gemini-3.8-flash",
		"input": []any{
			map[string]any{"type": "message", "role": "user", "content": "inspect assets"},
			map[string]any{"type": "function_call", "call_id": "c_f", "name": "stat_asset", "arguments": `{}`},
			map[string]any{"type": "function_call_output", "call_id": "c_f", "output": map[string]any{"type": "file", "path": "a.go"}},
			map[string]any{"type": "function_call", "call_id": "c_a", "name": "stat_audio", "arguments": `{}`},
			map[string]any{"type": "function_call_output", "call_id": "c_a", "output": map[string]any{"type": "audio", "duration": 12}},
		},
	})
	rDomResp, err := http.Post(baseURL+"/v1/responses", "application/json", bytes.NewReader(respDomPayload))
	if err != nil {
		t.Fatal(err)
	}
	domRespOut, _ := io.ReadAll(rDomResp.Body)
	rDomResp.Body.Close()
	if rDomResp.StatusCode != http.StatusOK {
		t.Fatalf("PROTO-13 /v1/responses rejected domain-typed tool map output with %d: %s", rDomResp.StatusCode, string(domRespOut))
	}

	chatDomPayload, _ := json.Marshal(map[string]any{
		"model": "gemini-3.8-flash",
		"messages": []any{
			map[string]any{"role": "user", "content": "inspect video"},
			map[string]any{"role": "assistant", "tool_calls": []any{
				map[string]any{"id": "call_v", "type": "function", "function": map[string]any{"name": "stat_video", "arguments": `{}`}},
			}},
			map[string]any{"role": "tool", "tool_call_id": "call_v", "content": map[string]any{"type": "video", "id": 1}},
		},
	})
	rDomChat, err := http.Post(baseURL+"/v1/chat/completions", "application/json", bytes.NewReader(chatDomPayload))
	if err != nil {
		t.Fatal(err)
	}
	domChatOut, _ := io.ReadAll(rDomChat.Body)
	rDomChat.Body.Close()
	if rDomChat.StatusCode != http.StatusOK {
		t.Fatalf("PROTO-13 /v1/chat/completions rejected domain-typed role:tool map output with %d: %s", rDomChat.StatusCode, string(domChatOut))
	}

	realAudioPartPayload, _ := json.Marshal(map[string]any{
		"model": "gemini-3.8-flash",
		"input": []any{
			map[string]any{"type": "message", "role": "user", "content": "listen"},
			map[string]any{"type": "function_call", "call_id": "c_aud", "name": "rec", "arguments": `{}`},
			map[string]any{"type": "function_call_output", "call_id": "c_aud", "output": map[string]any{"type": "input_audio", "input_audio": map[string]any{"data": "AAAA", "format": "wav"}}},
		},
	})
	rAudResp, err := http.Post(baseURL+"/v1/responses", "application/json", bytes.NewReader(realAudioPartPayload))
	if err != nil {
		t.Fatal(err)
	}
	rAudResp.Body.Close()
	if rAudResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected real input_audio content part in function_call_output to return 400, got %d", rAudResp.StatusCode)
	}

	// 4. [PROTO-05] /v1/chat/completions preserves assistant m["audio"] when content or tool_calls are non-empty -> 400.
	for _, asstAudioMsg := range []map[string]any{
		{"role": "assistant", "content": "spoken transcript", "audio": map[string]any{"id": "aud_text"}},
		{"role": "assistant", "content": "calling", "audio": map[string]any{"id": "aud_tc"}, "tool_calls": []any{
			map[string]any{"id": "tc_1", "type": "function", "function": map[string]any{"name": "Bash", "arguments": `{}`}},
		}},
	} {
		audChatReq, _ := json.Marshal(map[string]any{
			"model": "gemini-3.8-flash",
			"messages": []any{
				map[string]any{"role": "user", "content": "hi"},
				asstAudioMsg,
				map[string]any{"role": "user", "content": "next"},
			},
		})
		rAudChat, errA := http.Post(baseURL+"/v1/chat/completions", "application/json", bytes.NewReader(audChatReq))
		if errA != nil {
			t.Fatal(errA)
		}
		audBody, _ := io.ReadAll(rAudChat.Body)
		rAudChat.Body.Close()
		if rAudChat.StatusCode != http.StatusBadRequest || !strings.Contains(string(audBody), ".audio") {
			t.Fatalf("PROTO-05 assistant audio field bypassed 400 validation: status=%d body=%s", rAudChat.StatusCode, string(audBody))
		}
	}

	// 5. [PROTO-11 & PROTO-07] Trailing /key=tenant:secret redaction + line-1 steering indentation preservation.
	tailColonURL := "POST /v1beta/models/gemini-4-carbon:generateContent/key=tenant:supersecret456 HTTP/1.1"
	redTail := redactSecretsInString(tailColonURL)
	if strings.Contains(redTail, "tenant") || strings.Contains(redTail, "supersecret456") || !strings.Contains(redTail, ":generateContent/key=[REDACTED]") {
		t.Fatalf("PROTO-11 trailing /key=tenant:secret redaction failed: %q", redTail)
	}
	midColonURL := "POST /v1beta/models/gemini-4-carbon/key=tenant:supersecret789:streamGenerateContent?alt=sse"
	redMid := redactSecretsInString(redactSecretsInString(midColonURL))
	if strings.Contains(redMid, "tenant") || strings.Contains(redMid, "supersecret789") || !strings.Contains(redMid, "/key=[REDACTED]:streamGenerateContent?alt=sse") {
		t.Fatalf("PROTO-11 mid-path /key=tenant:secret:streamGenerateContent redaction failed: %q", redMid)
	}

	indentedTR := extractToolResultsFromMessages([]map[string]any{
		{"role": "user", "content": "fix python"},
		{"role": "assistant", "content": []any{map[string]any{"type": "tool_use", "id": "tu_py", "name": "Read", "input": map[string]any{}}}},
		{"role": "user", "content": []any{
			map[string]any{"type": "tool_result", "tool_use_id": "tu_py", "content": "ok"},
			map[string]any{"type": "text", "text": "\n    def foo():\n        return 1\n"},
		}},
	})
	if len(indentedTR) != 1 || !strings.Contains(indentedTR[0].Content, "[User message]\n    def foo():\n        return 1") {
		t.Fatalf("PROTO-07 formatAttachedUserSteeringText stripped line-1 indentation: %+v", indentedTR)
	}

	// 6. [OPT-03] CORS Access-Control-Allow-Headers includes agent/subagent/compaction/request-class headers.
	optReq, _ := http.NewRequest(http.MethodOptions, baseURL+"/v1/messages", nil)
	optReq.Header.Set("Origin", "https://app.example.com")
	optResp, err := http.DefaultClient.Do(optReq)
	if err != nil {
		t.Fatal(err)
	}
	optResp.Body.Close()
	corsAllow := optResp.Header.Get("Access-Control-Allow-Headers")
	for _, wantHdr := range []string{
		"x-claude-code-agent-id",
		"x-agent-id",
		"x-subagent-id",
		"x-claude-code-compaction",
		"x-claude-code-context-compacted",
		"x-claude-code-request-class",
	} {
		if !strings.Contains(corsAllow, wantHdr) {
			t.Fatalf("OPT-03 missing %q in Access-Control-Allow-Headers: %q", wantHdr, corsAllow)
		}
	}
}
