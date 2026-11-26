package main

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func validTestPNGBase64() string {
	raw := append([]byte("\x89PNG\r\n\x1a\n"), []byte{0, 0, 0, 13, 'I', 'H', 'D', 'R', 0, 0, 0, 1, 0, 0, 0, 1, 8, 6, 0, 0, 0}...)
	return base64.StdEncoding.EncodeToString(raw)
}

func validTestJPEGBase64() string {
	raw := []byte{0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 'J', 'F', 'I', 'F', 0x00}
	return base64.StdEncoding.EncodeToString(raw)
}

func validTestWEBPBase64() string {
	raw := []byte("RIFF\x1a\x00\x00\x00WEBPVP8 ")
	return base64.StdEncoding.EncodeToString(raw)
}

func validTestGIFBase64() string {
	raw := []byte("GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00\xff\xff\xff!\xf9\x04\x01\x00\x00\x00\x00,\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02D\x01\x00;")
	return base64.StdEncoding.EncodeToString(raw)
}

func TestMultimodalStage1FourProtocolsAndCountTokens(t *testing.T) {
	gs := NewGatewayServer()
	t.Cleanup(gs.Close)
	h := gs.GatewayOnlyHandler()

	pngB64 := validTestPNGBase64()
	jpegB64 := validTestJPEGBase64()
	webpB64 := validTestWEBPBase64()
	gifB64 := validTestGIFBase64()

	t.Run("all_four_image_magics_and_nested_tool_results_preserved", func(t *testing.T) {
		msgs := []map[string]any{
			{
				"role": "user",
				"content": []any{
					map[string]any{
						"type":        "tool_result",
						"tool_use_id": "call_mm_1",
						"content": []any{
							map[string]any{"type": "text", "text": "screenshot captured"},
							map[string]any{"type": "image", "source": map[string]any{"type": "base64", "media_type": "image/png", "data": pngB64}},
							map[string]any{"type": "image_url", "image_url": map[string]any{"url": "data:image/jpeg;base64," + jpegB64}},
							map[string]any{"type": "image_url", "image_url": "data:image/webp;base64," + webpB64},
							map[string]any{"type": "image_url", "image_url": "data:image/gif;base64," + gifB64},
						},
					},
				},
			},
		}
		if err := CanonicalizeAndValidateMessages(map[string]any{"model": "gemini-4-carbon"}, msgs, "/v1/messages"); err != nil {
			t.Fatalf("expected valid canonicalization of all 4 image magics in tool_result, got %v", err)
		}
		trs := extractToolResultsFromMessages(msgs)
		if len(trs) != 1 || len(trs[0].Images) != 4 {
			t.Fatalf("expected 4 extracted tool_result images, got %+v", trs)
		}
		if trs[0].Images[0]["mimeType"] != "image/png" || trs[0].Images[1]["mimeType"] != "image/jpeg" ||
			trs[0].Images[2]["mimeType"] != "image/webp" || trs[0].Images[3]["mimeType"] != "image/gif" {
			t.Fatalf("unexpected sniffed mime types: %+v", trs[0].Images)
		}
		histTxt := formatSingleMessageText(msgs[0])
		if strings.Contains(histTxt, pngB64) || strings.Contains(histTxt, jpegB64) {
			t.Fatalf("raw base64 leaked into formatted tool_result history text: %s", histTxt)
		}
	})

	t.Run("dual_form_utf8_text_file_unpacked_across_protocols", func(t *testing.T) {
		helloB64 := base64.StdEncoding.EncodeToString([]byte("line1=alpha\nline2=beta"))
		msgs := []map[string]any{
			{
				"role": "user",
				"content": []any{
					map[string]any{
						"type": "document",
						"source": map[string]any{
							"type":       "base64",
							"media_type": "text/plain",
							"data":       helloB64,
						},
					},
					map[string]any{
						"type": "input_file",
						"file": map[string]any{
							"filename":  "cfg.yaml",
							"file_data": "data:text/plain;base64," + helloB64,
						},
					},
				},
			},
		}
		if err := CanonicalizeAndValidateMessages(nil, msgs, "/v1/messages"); err != nil {
			t.Fatalf("expected UTF-8 text files to normalize, got %v", err)
		}
		txt := formatSingleMessageText(msgs[0])
		if !strings.Contains(txt, "line1=alpha") || !strings.Contains(txt, "cfg.yaml") {
			t.Fatalf("expected unpacked UTF-8 file text, got %s", txt)
		}
	})

	t.Run("rejects_unsupported_media_spoofed_magic_system_images_and_oversize_across_endpoints", func(t *testing.T) {
		spoofB64 := base64.StdEncoding.EncodeToString([]byte("<html><body>not a png</body></html>"))
		pdfB64 := base64.StdEncoding.EncodeToString([]byte("%PDF-1.4\n1 0 obj\n"))
		badUTF8B64 := base64.StdEncoding.EncodeToString([]byte{0xff, 0xfe, 0x00, 0x01})

		cases := []struct {
			name   string
			path   string
			body   map[string]any
			status int
		}{
			{
				"messages_spoofed_png",
				"/v1/messages",
				map[string]any{
					"model": "gemini-4-carbon", "stream": true,
					"messages": []any{map[string]any{"role": "user", "content": []any{
						map[string]any{"type": "image", "source": map[string]any{"type": "base64", "media_type": "image/png", "data": spoofB64}},
					}}},
				},
				http.StatusBadRequest,
			},
			{
				"messages_pdf_rejected",
				"/v1/messages",
				map[string]any{
					"model": "gemini-4-carbon",
					"messages": []any{map[string]any{"role": "user", "content": []any{
						map[string]any{"type": "document", "source": map[string]any{"type": "base64", "media_type": "application/pdf", "data": pdfB64}},
					}}},
				},
				http.StatusBadRequest,
			},
			{
				"count_tokens_svg_rejected",
				"/v1/messages/count_tokens",
				map[string]any{
					"model": "gemini-4-carbon",
					"messages": []any{map[string]any{"role": "user", "content": []any{
						map[string]any{"type": "image_url", "image_url": "data:image/svg+xml;base64," + spoofB64},
					}}},
				},
				http.StatusBadRequest,
			},
			{
				"chat_completions_system_image_rejected",
				"/v1/chat/completions",
				map[string]any{
					"model": "gemini-4-carbon",
					"messages": []any{
						map[string]any{"role": "system", "content": []any{map[string]any{"type": "image_url", "image_url": "data:image/png;base64," + pngB64}}},
						map[string]any{"role": "user", "content": "hi"},
					},
				},
				http.StatusBadRequest,
			},
			{
				"chat_completions_audio_rejected",
				"/v1/chat/completions",
				map[string]any{
					"model": "gemini-4-carbon",
					"messages": []any{map[string]any{"role": "user", "content": []any{
						map[string]any{"type": "input_audio", "input_audio": map[string]any{"data": "AAAA", "format": "mp3"}},
					}}},
				},
				http.StatusBadRequest,
			},
			{
				"responses_unsupported_item_type_rejected",
				"/v1/responses",
				map[string]any{
					"model": "gemini-4-carbon",
					"input": []any{map[string]any{"type": "item_reference", "id": "item_1"}},
				},
				http.StatusBadRequest,
			},
			{
				"responses_file_id_rejected",
				"/v1/responses",
				map[string]any{
					"model": "gemini-4-carbon",
					"input": []any{map[string]any{"type": "message", "role": "user", "content": []any{
						map[string]any{"type": "input_image", "file_id": "file-123"},
					}}},
				},
				http.StatusBadRequest,
			},
			{
				"gemini_gs_uri_rejected",
				"/v1beta/models/gemini-4-carbon:generateContent",
				map[string]any{
					"contents": []any{map[string]any{"role": "user", "parts": []any{
						map[string]any{"fileData": map[string]any{"mimeType": "image/png", "fileUri": "gs://my-bucket/a.png"}},
					}}},
				},
				http.StatusBadRequest,
			},
			{
				"gemini_count_tokens_invalid_utf8_text_file",
				"/v1beta/models/gemini-4-carbon:countTokens",
				map[string]any{
					"contents": []any{map[string]any{"role": "user", "parts": []any{
						map[string]any{"inlineData": map[string]any{"mimeType": "text/plain", "data": badUTF8B64}},
					}}},
				},
				http.StatusBadRequest,
			},
			{
				"gemini_function_response_parts_pdf_rejected",
				"/v1beta/models/gemini-4-carbon:generateContent",
				map[string]any{
					"contents": []any{
						map[string]any{"role": "model", "parts": []any{map[string]any{"functionCall": map[string]any{"id": "c1", "name": "ReadFile", "args": map[string]any{}}}}},
						map[string]any{"role": "user", "parts": []any{map[string]any{"functionResponse": map[string]any{
							"id": "c1", "name": "ReadFile", "response": map[string]any{"ok": true},
							"parts": []any{map[string]any{"inlineData": map[string]any{"mimeType": "application/pdf", "data": pdfB64}}},
						}}}},
					},
				},
				http.StatusBadRequest,
			},
			{
				"messages_over_10mb_single_image_returns_413",
				"/v1/messages",
				func() map[string]any {
					big := append([]byte("\x89PNG\r\n\x1a\n"), make([]byte, MaxImageBytes+16)...)
					return map[string]any{
						"model": "gemini-4-carbon",
						"messages": []any{map[string]any{"role": "user", "content": []any{
							map[string]any{"type": "image", "source": map[string]any{"type": "base64", "media_type": "image/png", "data": base64.StdEncoding.EncodeToString(big)}},
						}}},
					}
				}(),
				http.StatusRequestEntityTooLarge,
			},
		}

		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				b, _ := json.Marshal(c.body)
				req := newLoopbackRequest(http.MethodPost, c.path, bytes.NewReader(b))
				req.Header.Set("Content-Type", "application/json")
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, req)
				if rec.Code != c.status {
					t.Fatalf("%s: expected HTTP %d, got %d body=%s", c.name, c.status, rec.Code, rec.Body.String())
				}
			})
		}
	})

	t.Run("gemini_function_response_parts_preserves_image_and_remote_ref", func(t *testing.T) {
		body := map[string]any{
			"contents": []any{
				map[string]any{"role": "model", "parts": []any{map[string]any{"functionCall": map[string]any{"id": "c_img", "name": "Snap", "args": map[string]any{}}}}},
				map[string]any{"role": "user", "parts": []any{map[string]any{"functionResponse": map[string]any{
					"id": "c_img", "name": "Snap", "response": map[string]any{"status": "done"},
					"parts": []any{
						map[string]any{"inlineData": map[string]any{"mimeType": "image/png", "data": pngB64}},
						map[string]any{"fileData": map[string]any{"mimeType": "image/jpeg", "fileUri": "https://cdn.example.com/pic.jpg"}},
					},
				}}}},
			},
		}
		norm, err := normalizeGeminiV1BetaRequest("gemini-4-carbon", body, false)
		if err != nil {
			t.Fatalf("unexpected normalizeGeminiV1BetaRequest error: %v", err)
		}
		trs := extractToolResultsFromMessages(norm.Messages)
		if len(trs) != 1 || len(trs[0].Images) != 2 {
			t.Fatalf("expected 2 images in Gemini functionResponse, got %+v", trs)
		}
		if trs[0].Images[0]["type"] != "image" || trs[0].Images[1]["type"] != "image_ref" {
			t.Fatalf("unexpected image descriptors: %+v", trs[0].Images)
		}
	})

	t.Run("image_count_caps_and_32mb_budget_and_fingerprint", func(t *testing.T) {
		mkImgMsg := func(count int, payload string) map[string]any {
			blocks := make([]any, 0, count+1)
			blocks = append(blocks, map[string]any{"type": "text", "text": "describe"})
			for i := 0; i < count; i++ {
				blocks = append(blocks, map[string]any{"type": "image", "mimeType": "image/png", "data": payload, "decodedBytes": 64})
			}
			return map[string]any{"role": "user", "content": blocks}
		}

		if _, _, _, err := validateLatestTurnImageBudget([]map[string]any{mkImgMsg(200, pngB64)}, ModelOrDefault("gemini-4-carbon")); err != nil {
			t.Fatalf("200 images should pass Gemini limit: %v", err)
		}
		if _, _, _, err := validateLatestTurnImageBudget([]map[string]any{mkImgMsg(201, pngB64)}, ModelOrDefault("gemini-4-carbon")); err == nil {
			t.Fatalf("201 images should fail Gemini 200 limit")
		}
		if _, _, _, err := validateLatestTurnImageBudget([]map[string]any{mkImgMsg(100, pngB64)}, ModelOrDefault("claude-opus-5-5")); err != nil {
			t.Fatalf("100 images should pass Claude limit: %v", err)
		}
		if _, _, _, err := validateLatestTurnImageBudget([]map[string]any{mkImgMsg(101, pngB64)}, ModelOrDefault("claude-opus-5-5")); err == nil {
			t.Fatalf("101 images should fail Claude 100 limit")
		}
		if selCl := ResolveRequestedModel("claude-sonnet-5-5", nil); effectiveModelMaxImages(selCl.Model) != 100 {
			t.Fatalf("expected mapped Claude model max images 100, got %d (%+v)", effectiveModelMaxImages(selCl.Model), selCl.Model)
		}

		// Older history with 120 images + latest turn with 1 image must pass Stage 1 budget.
		withHist := []map[string]any{
			mkImgMsg(120, pngB64),
			{"role": "assistant", "content": "noted"},
			mkImgMsg(1, pngB64),
		}
		if _, totalImgs, _, err := validateLatestTurnImageBudget(withHist, ModelOrDefault("claude-opus-5-5")); err != nil || totalImgs != 121 {
			t.Fatalf("expected latest turn with history >100 to pass Stage 1 with total=121, got err=%v total=%d", err, totalImgs)
		}

		// Fingerprint sensitivity to different image bytes on the same text.
		fp1 := computeFirstUserFingerprint([]map[string]any{mkImgMsg(1, pngB64)})
		fp2 := computeFirstUserFingerprint([]map[string]any{mkImgMsg(1, validTestGIFBase64())})
		if fp1 == "" || fp2 == "" || fp1 == fp2 {
			t.Fatalf("expected different first-user fingerprints for different images: fp1=%s fp2=%s", fp1, fp2)
		}

		// 32MB decoded prompt budget -> 413
		fourImagesAt9MB := make([]any, 0, 4)
		for i := 0; i < 4; i++ {
			fourImagesAt9MB = append(fourImagesAt9MB, map[string]any{"type": "image", "mimeType": "image/png", "data": pngB64, "decodedBytes": 9 * 1024 * 1024})
		}
		_, _, _, err := validateLatestTurnImageBudget([]map[string]any{{"role": "user", "content": fourImagesAt9MB}}, ModelOrDefault("gemini-4-carbon"))
		var mmErr *MultimodalValidationError
		if err == nil || !errorAsMM(err, &mmErr) || mmErr.StatusCode != http.StatusRequestEntityTooLarge {
			t.Fatalf("expected 413 prompt image budget error, got %v", err)
		}
	})
}

func errorAsMM(err error, out **MultimodalValidationError) bool {
	if err == nil {
		return false
	}
	mm, ok := err.(*MultimodalValidationError)
	if ok {
		*out = mm
		return true
	}
	return false
}

func TestModuleCAntiDeadlockReadLineSendGuardAndWaitKill(t *testing.T) {
	// 1. readJSONRPCLine reads >1MB lines and rejects >maxBytes cleanly.
	bigLine := append(bytes.Repeat([]byte("a"), 2*1024*1024), '\n')
	br := bufio.NewReaderSize(bytes.NewReader(bigLine), 64*1024)
	got, err := readJSONRPCLine(br, 4*1024*1024)
	if err != nil || len(got) != 2*1024*1024 {
		t.Fatalf("expected 2MB line read cleanly, got len=%d err=%v", len(got), err)
	}
	brTooBig := bufio.NewReaderSize(bytes.NewReader(bigLine), 64*1024)
	if _, err := readJSONRPCLine(brTooBig, 1024*1024); err == nil {
		t.Fatalf("expected error when line exceeds 1MB limit")
	}

	// 2. sendRawJSON rejects >56MB before writing anything to stdin pipe.
	pr, pw := io.Pipe()
	defer pr.Close()
	defer pw.Close()
	eng := &SharedAcpEngine{
		stdin: bufio.NewWriter(pw),
	}
	oversize := map[string]any{"jsonrpc": "2.0", "id": 1, "method": "x", "params": map[string]any{"blob": strings.Repeat("Z", 57*1024*1024)}}
	sendErr := eng.sendRawJSON(oversize)
	code, errTyp, retry, _ := classifyUpstreamError(sendErr)
	if sendErr == nil || code != http.StatusRequestEntityTooLarge || errTyp != "invalid_request_error" || retry {
		t.Fatalf("expected 413 non-retryable error from sendRawJSON, got err=%v (%d %s %v)", sendErr, code, errTyp, retry)
	}

	// 3. waitCmdWithKill kills process group holding inherited stdout pipe without deadlock.
	cmd := exec.Command("sh", "-c", "sleep 30 & echo ready")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	lineBr := bufio.NewReader(stdout)
	line, err := lineBr.ReadString('\n')
	if err != nil || !strings.Contains(line, "ready") {
		t.Fatalf("expected ready from child, got %q err=%v", line, err)
	}
	done := make(chan struct{})
	go func() {
		waitCmdWithKill(cmd, true)
		_ = stdout.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("waitCmdWithKill deadlocked waiting on child holding stdout pipe")
	}
}

func TestModuleDLicense3501FourProtocolsNoRetryNoFallbackNoTos(t *testing.T) {
	baseURL, cleanup := startTestGateway(t)
	defer cleanup()
	origTosFile := TosTrippedFile
	TosTrippedFile = filepath.Join(t.TempDir(), "tos-tripped")
	tosMu.Lock()
	oldMem, oldReason, oldPersisted := tosTrippedMem.Load(), tosReasonMem, tosPersisted
	tosTrippedMem.Store(false)
	tosReasonMem = ""
	tosPersisted = false
	tosMu.Unlock()
	t.Cleanup(func() {
		tosMu.Lock()
		tosTrippedMem.Store(oldMem)
		tosReasonMem = oldReason
		tosPersisted = oldPersisted
		tosMu.Unlock()
		TosTrippedFile = origTosFile
	})

	postAndRead := func(path string, payload map[string]any) (int, string) {
		b, _ := json.Marshal(payload)
		resp, err := http.Post(baseURL+path, "application/json", bytes.NewReader(b))
		if err != nil {
			t.Fatalf("POST %s failed: %v", path, err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(raw)
	}

	// Non-streaming across all 4 protocols.
	nonStreamCases := []struct {
		name  string
		path  string
		body  map[string]any
		check string
	}{
		{"anthropic", "/v1/messages", map[string]any{"model": "gemini-4-carbon-high", "stream": false, "messages": []any{map[string]any{"role": "user", "content": "[FAKE_LICENSE_3501]"}}}, "permission_error"},
		{"chat", "/v1/chat/completions", map[string]any{"model": "gemini-4-carbon-high", "stream": false, "messages": []any{map[string]any{"role": "user", "content": "[FAKE_LICENSE_3501]"}}}, "permission_error"},
		{"responses", "/v1/responses", map[string]any{"model": "gemini-4-carbon-high", "stream": false, "input": "[FAKE_LICENSE_3501]"}, "permission_error"},
		{"gemini", "/v1beta/models/gemini-4-carbon-high:generateContent", map[string]any{"contents": []any{map[string]any{"role": "user", "parts": []any{map[string]any{"text": "[FAKE_LICENSE_3501]"}}}}}, "PERMISSION_DENIED"},
	}
	for _, tc := range nonStreamCases {
		code, body := postAndRead(tc.path, tc.body)
		if code != http.StatusForbidden || !strings.Contains(body, tc.check) || !strings.Contains(body, "#3501") {
			t.Fatalf("%s non-stream expected 403 %s with #3501, got code=%d body=%s", tc.name, tc.check, code, body)
		}
	}

	// Streaming pre-chunk failure must emit HTTP 403 too across all 4 protocols.
	streamCases := []struct {
		name  string
		path  string
		body  map[string]any
		check string
	}{
		{"anthropic_stream", "/v1/messages", map[string]any{"model": "gemini-4-carbon-high", "stream": true, "messages": []any{map[string]any{"role": "user", "content": "[FAKE_LICENSE_3501]"}}}, "permission_error"},
		{"chat_stream", "/v1/chat/completions", map[string]any{"model": "gemini-4-carbon-high", "stream": true, "messages": []any{map[string]any{"role": "user", "content": "[FAKE_LICENSE_3501]"}}}, "permission_error"},
		{"responses_stream", "/v1/responses", map[string]any{"model": "gemini-4-carbon-high", "stream": true, "input": "[FAKE_LICENSE_3501]"}, "permission_error"},
		{"gemini_stream", "/v1beta/models/gemini-4-carbon-high:streamGenerateContent?alt=sse", map[string]any{"contents": []any{map[string]any{"role": "user", "parts": []any{map[string]any{"text": "[FAKE_LICENSE_3501]"}}}}}, "PERMISSION_DENIED"},
	}
	for _, tc := range streamCases {
		code, body := postAndRead(tc.path, tc.body)
		if code != http.StatusForbidden || !strings.Contains(body, tc.check) {
			t.Fatalf("%s stream expected 403 %s, got code=%d body=%s", tc.name, tc.check, code, body)
		}
	}

	if tripped, reason := IsTosTripped(); tripped {
		t.Fatalf("#3501 must NOT trip ToS circuit breaker, got reason=%q", reason)
	}
}

func TestTask31ThinkingMsRetryCountAndDbSteps(t *testing.T) {
	origDB := GatewayStateDB
	tmpDir := t.TempDir()
	dbFile := filepath.Join(tmpDir, "gateway.db")
	GatewayStateDB = dbFile
	GlobalStore.Init(dbFile)
	t.Cleanup(func() {
		GatewayStateDB = origDB
		GlobalStore.Init(origDB)
	})

	sess := NewAcpProjectSession("conv-obs-1", "/tmp", "sys")
	sess.SessionID = "11111111-1111-4111-8111-111111111111"
	sess.ModelID = "gemini-4-carbon"
	sess.ThoughtLevel = "medium"
	sess.MessageCount = 2
	GlobalStore.UpsertSession(sess)

	nowMs := time.Now().UnixMilli()
	entry := TurnHistoryEntry{
		TurnIndex:      1,
		CompletedAtMs:  nowMs,
		Ts:             time.UnixMilli(nowMs).UTC().Format(time.RFC3339),
		SessionID:      sess.SessionID,
		Mode:           "cold_start",
		ElapsedMs:      5000,
		TtftMs:         800,
		UpstreamTtftMs: 750,
		StreamMs:       4200,
		ThinkingMs:     2500,
		RetryCount:     2,
		Tps:            20.0,
		DecodeTps:      23.8,
		HasThinking:    true,
		ThinkingTokens: 60,
		Usage: &TurnUsage{
			InputTokens:    1000,
			OutputTokens:   100,
			ThinkingTokens: 60,
			UsageSource:    "acp",
			StepsInPrompt:  1,
			GenTtftMs:      750,
			StreamMs:       4200,
			DecodeTps:      23.8,
		},
		StopReason:   "end_turn",
		Model:        sess.ModelID,
		ThoughtLevel: "medium",
	}
	GlobalStore.InsertTurn(sess.ConvKey, entry)

	gs := NewGatewayServer()
	t.Cleanup(gs.Close)
	_, gwTurns := gs.CollectDashboardTelemetry(make(map[string]bool))
	if len(gwTurns) == 0 {
		t.Fatalf("expected restored turn in CollectDashboardTelemetry")
	}
	var found map[string]any
	for _, gt := range gwTurns {
		if gt["sessionId"] == sess.SessionID {
			found = gt
			break
		}
	}
	if found == nil {
		t.Fatalf("did not find restored session turn in telemetry: %+v", gwTurns)
	}
	if found["thinkingMs"] != 2500 || found["visibleStreamMs"] != 1700 || found["retryCount"] != 2 {
		t.Fatalf("unexpected telemetry stage timings: thinkingMs=%v visibleStreamMs=%v retryCount=%v", found["thinkingMs"], found["visibleStreamMs"], found["retryCount"])
	}

	steps := buildRecentDbSteps([]ParsedModelStep{
		{
			SessionID:   sess.SessionID,
			StepIdx:     1,
			TsSec:       time.Now().Unix(),
			TtftMs:      500,
			StreamMs:    4000,
			ElapsedMs:   4500,
			OutTok:      100,
			ThTok:       50,
			HasThinking: true,
		},
	}, gwTurns)
	if len(steps) != 1 || steps[0]["thinkingMs"] != 2000 || steps[0]["visibleStreamMs"] != 2000 {
		t.Fatalf("unexpected buildRecentDbSteps stage breakdown: %+v", steps)
	}
}

func TestTask32CleanupExpiredConversationsFourGates(t *testing.T) {
	origDB := GatewayStateDB
	origKeep := ConvKeepDays
	tmpDir := t.TempDir()
	tmpConv := filepath.Join(tmpDir, "convs")
	if err := os.MkdirAll(tmpConv, 0755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGY_CONV_DIR", tmpConv)

	dbFile := filepath.Join(tmpDir, "gateway.db")
	GatewayStateDB = dbFile
	GlobalStore.Init(dbFile)
	t.Cleanup(func() {
		GatewayStateDB = origDB
		ConvKeepDays = origKeep
		GlobalStore.Init(origDB)
	})

	gs := NewGatewayServer()
	t.Cleanup(gs.Close)

	sidExpired := "aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa"
	sidActive := "bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb"
	sidRecent := "cccccccc-3333-4333-8333-cccccccccccc"
	sidForeign := "dddddddd-4444-4444-8444-dddddddddddd"

	// Mark expired, active, and recent as gateway-known in StateStore.
	for i, sid := range []string{sidExpired, sidActive, sidRecent} {
		convKey := "conv-clean-" + sid[:8]
		s := NewAcpProjectSession(convKey, "/tmp", "sys")
		s.HibernatedSessionID = sid
		s.MessageCount = 2
		GlobalStore.UpsertSession(s)
		GlobalStore.InsertTurn(convKey, TurnHistoryEntry{
			TurnIndex:     i + 1,
			SessionID:     sid,
			CompletedAtMs: time.Now().Add(-10 * 24 * time.Hour).UnixMilli(),
			Model:         "gemini-4-carbon",
			ThoughtLevel:  "medium",
			StopReason:    "end_turn",
		})
	}
	// Hold sidActive in memory to trigger Gate 3.
	liveSess := NewAcpProjectSession("conv-live-hold", "/tmp", "sys")
	liveSess.HibernatedSessionID = sidActive
	gs.mu.Lock()
	gs.sessions["conv-live-hold"] = liveSess
	gs.mu.Unlock()

	createTestStepsDB := func(path string, mtime time.Time) {
		sdb, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		_, err = sdb.Exec(`CREATE TABLE steps (idx INTEGER PRIMARY KEY, step_type INTEGER, metadata BLOB, step_payload BLOB);
			INSERT INTO steps VALUES (1, 15, X'00', X'00');`)
		_ = sdb.Close()
		if err != nil {
			t.Fatal(err)
		}
		_ = os.Chtimes(path, mtime, mtime)
	}

	oldTime := time.Now().Add(-10 * 24 * time.Hour)
	newTime := time.Now().Add(-2 * 24 * time.Hour)

	for _, sid := range []string{sidExpired, sidActive, sidForeign} {
		pb := filepath.Join(tmpConv, sid+".pb")
		db := filepath.Join(tmpConv, sid+".db")
		_ = os.WriteFile(pb, []byte("pb"), 0600)
		_ = os.Chtimes(pb, oldTime, oldTime)
		createTestStepsDB(db, oldTime)
	}
	// sidRecent has old .pb but recent .db -> Gate 4a must keep both.
	_ = os.WriteFile(filepath.Join(tmpConv, sidRecent+".pb"), []byte("pb"), 0600)
	_ = os.Chtimes(filepath.Join(tmpConv, sidRecent+".pb"), oldTime, oldTime)
	createTestStepsDB(filepath.Join(tmpConv, sidRecent+".db"), newTime)

	// Gate 1a: disabled when ConvKeepDays == 0
	ConvKeepDays = 0
	if got := gs.CleanupExpiredConversations(time.Now()); got != 0 {
		t.Fatalf("expected 0 cleaned when ConvKeepDays=0, got %d", got)
	}

	// Gate 1b-4: ConvKeepDays = 7 -> cleans ONLY sidExpired
	ConvKeepDays = 7
	if got := gs.CleanupExpiredConversations(time.Now()); got != 1 {
		t.Fatalf("expected exactly 1 cleaned (sidExpired), got %d", got)
	}
	if _, err := os.Stat(filepath.Join(tmpConv, sidExpired+".pb")); !os.IsNotExist(err) {
		t.Fatalf("expected sidExpired .pb removed")
	}
	if _, err := os.Stat(filepath.Join(tmpConv, sidExpired+".db")); !os.IsNotExist(err) {
		t.Fatalf("expected sidExpired .db removed")
	}
	for _, kept := range []string{sidActive, sidRecent, sidForeign} {
		if _, err := os.Stat(filepath.Join(tmpConv, kept+".pb")); err != nil {
			t.Fatalf("expected %s .pb preserved, got %v", kept, err)
		}
	}
	// Verify turns preserved while agy_session_id cleared on cleaned conversation
	GlobalStore.mu.Lock()
	var agySid string
	var turnCnt int
	_ = GlobalStore.db.QueryRow(`SELECT agy_session_id FROM conversations WHERE conv_key = ?`, "conv-clean-"+sidExpired[:8]).Scan(&agySid)
	_ = GlobalStore.db.QueryRow(`SELECT COUNT(*) FROM turns WHERE agy_session_id = ?`, sidExpired).Scan(&turnCnt)
	GlobalStore.mu.Unlock()
	if agySid != "" || turnCnt != 1 {
		t.Fatalf("expected agy_session_id cleared and turns preserved, got agySid=%q turnCnt=%d", agySid, turnCnt)
	}
}

func TestFinding1RunTurnPreservesSessionStateOnStage2Failure(t *testing.T) {
	sess := NewAcpProjectSession("conv-find1", "/tmp", "sys-old")
	sess.SessionID = "99999999-1111-4111-8111-999999999999"
	sess.MessageCount = 2
	sess.ForceRolloverNext = true
	sess.ForceRolloverReason = "rollover_compaction"
	sess.pendingSystemText = "sys-new"

	// Remote image to loopback SSRF URL fails in Stage 2 preflight BEFORE mutating session state.
	msgs := []map[string]any{
		{"role": "user", "content": []any{map[string]any{"type": "image_ref", "url": "http://127.0.0.1/blocked.png"}}},
	}
	_, err := sess.RunTurn(context.Background(), msgs, nil, "medium", nil, nil)
	if err == nil {
		t.Fatalf("expected RunTurn to fail Stage 2 SSRF preflight")
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.SessionID != "99999999-1111-4111-8111-999999999999" || sess.MessageCount != 2 ||
		!sess.ForceRolloverNext || sess.ForceRolloverReason != "rollover_compaction" ||
		sess.pendingSystemText != "sys-new" || sess.SystemText != "sys-old" {
		t.Fatalf("RunTurn mutated session state on Stage 2 failure: sid=%q msgCnt=%d force=%v reason=%q pend=%q sys=%q",
			sess.SessionID, sess.MessageCount, sess.ForceRolloverNext, sess.ForceRolloverReason, sess.pendingSystemText, sess.SystemText)
	}
}
