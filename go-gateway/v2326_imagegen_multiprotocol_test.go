package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"agy-acp-gateway-go/internal/protocol"
)

func makeDistinctTestPNG(r, g, b uint8) []byte {
	img := image.NewRGBA(image.Rect(0, 0, 4, 4))
	c := color.RGBA{R: r, G: g, B: b, A: 255}
	for y := 0; y < 4; y++ {
		for x := 0; x < 4; x++ {
			img.SetRGBA(x, y, c)
		}
	}
	var buf bytes.Buffer
	_ = png.Encode(&buf, img)
	return buf.Bytes()
}

func sha256Hex(raw []byte) string {
	s := sha256.Sum256(raw)
	return hex.EncodeToString(s[:])
}

func setLowCgroupMemoryForTest(t *testing.T) {
	t.Helper()
	restore := setCgroupMemoryReaderForTest(func() CgroupMemoryStatus {
		return CgroupMemoryStatus{
			Version:         2,
			CurrentBytes:    1800 * 1024 * 1024,
			LimitBytes:      6144 * 1024 * 1024,
			WorkingSetBytes: 1800 * 1024 * 1024,
			UsagePct:        29.3,
			HasLiveUsage:    true,
		}
	})
	t.Cleanup(restore)
}

func TestV2326_ProcessPerSessionIsolationForWsImageGenVsWsBridge(t *testing.T) {
	setLowCgroupMemoryForTest(t)
	argsLog := filepath.Join(t.TempDir(), "fakeacp-args.jsonl")
	t.Setenv("FAKE_ACP_ARGS_LOG", argsLog)

	GlobalEngine.StopEngine()
	StopImageGenEngine()
	origTTL := imageGenIdleTTL
	imageGenIdleTTL = 30 * time.Second
	t.Cleanup(func() {
		imageGenIdleTTL = origTTL
		StopImageGenEngine()
		GlobalEngine.StopEngine()
	})

	gs := NewGatewayServer()
	t.Cleanup(gs.Close)
	h := gs.GatewayOnlyHandler()

	// 1. Trigger ws-bridge chat turn.
	chatReq := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/chat/completions",
		strings.NewReader(`{"model":"gemini-4-carbon","messages":[{"role":"user","content":"ping ws-bridge"}]}`))
	chatReq.Header.Set("Content-Type", "application/json")
	chatRec := httptest.NewRecorder()
	h.ServeHTTP(chatRec, chatReq)
	if chatRec.Code != http.StatusOK {
		t.Fatalf("ws-bridge chat expected 200, got %d: %s", chatRec.Code, chatRec.Body.String())
	}

	// 2. Trigger ws-imagegen turn.
	imgReq := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/images/generations",
		strings.NewReader(`{"model":"gemini-3.1-flash-image","prompt":"single process test"}`))
	imgReq.Header.Set("Content-Type", "application/json")
	imgRec := httptest.NewRecorder()
	h.ServeHTTP(imgRec, imgReq)
	if imgRec.Code != http.StatusOK {
		t.Fatalf("ws-imagegen expected 200, got %d: %s", imgRec.Code, imgRec.Body.String())
	}

	rawLog, err := os.ReadFile(argsLog)
	if err != nil {
		t.Fatalf("read args log: %v", err)
	}
	var sawBridge, sawImageGen bool
	sc := bufio.NewScanner(bytes.NewReader(rawLog))
	for sc.Scan() {
		var entry struct {
			Agent                string   `json:"agent"`
			Args                 []string `json:"args"`
			HasProcessPerSession bool     `json:"hasProcessPerSession"`
		}
		if err := json.Unmarshal(sc.Bytes(), &entry); err != nil {
			t.Fatalf("unmarshal args log entry: %v", err)
		}
		switch entry.Agent {
		case McpBridgeName:
			sawBridge = true
			if !entry.HasProcessPerSession {
				t.Fatalf("ws-bridge MUST keep --process-per-session, got args=%v", entry.Args)
			}
		case ImageGenAgentName:
			sawImageGen = true
			if entry.HasProcessPerSession {
				t.Fatalf("ws-imagegen MUST NOT pass --process-per-session, got args=%v", entry.Args)
			}
		}
	}
	if !sawBridge {
		t.Fatalf("expected ws-bridge spawn in args log:\n%s", string(rawLog))
	}
	if !sawImageGen {
		t.Fatalf("expected ws-imagegen spawn in args log:\n%s", string(rawLog))
	}
}

func TestV2326_ConcurrentN2OrderingAndPNGTranscodeAndPressureSerialDowngrade(t *testing.T) {
	setLowCgroupMemoryForTest(t)
	capFile := filepath.Join(t.TempDir(), "imagegen-capture.jsonl")
	t.Setenv("FAKE_IMAGEGEN_CAPTURE_FILE", capFile)

	StopImageGenEngine()
	origTTL := imageGenIdleTTL
	imageGenIdleTTL = 30 * time.Second
	t.Cleanup(func() {
		imageGenIdleTTL = origTTL
		StopImageGenEngine()
	})

	gs := NewGatewayServer()
	t.Cleanup(gs.Close)
	h := gs.GatewayOnlyHandler()

	// 1. n=2 concurrent ordering (slot 1 slower 160ms RED, slot 2 faster 20ms BLUE) + output_format="png" transcode from JPEG.
	reqBody := `{"model":"gemini-3.1-flash-image","prompt":"[FAKE_IMAGEGEN_ORDER_CONCUR] [FAKE_IMAGEGEN_JPEG] ordered dual png","n":2,"output_format":"png"}`
	req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/images/generations", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("n=2 png expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Created      int64  `json:"created"`
		OutputFormat string `json:"output_format"`
		Data         []struct {
			B64JSON string `json:"b64_json"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal n=2 response: %v", err)
	}
	if resp.OutputFormat != "png" || len(resp.Data) != 2 {
		t.Fatalf("expected output_format=png and 2 images, got format=%q len=%d", resp.OutputFormat, len(resp.Data))
	}
	decodedRGB := make([][3]uint8, 2)
	for i, item := range resp.Data {
		raw, err := base64.StdEncoding.DecodeString(item.B64JSON)
		if err != nil {
			t.Fatalf("data[%d] invalid base64: %v", i, err)
		}
		if mime, ok := sniffAllowedImageMime(raw); !ok || mime != "image/png" {
			t.Fatalf("data[%d] expected real PNG magic after JPEG->PNG transcode, got mime=%q ok=%v", i, mime, ok)
		}
		img, err := png.Decode(bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("data[%d] png.Decode failed: %v", i, err)
		}
		r16, g16, b16, _ := img.At(4, 4).RGBA()
		decodedRGB[i] = [3]uint8{uint8(r16 >> 8), uint8(g16 >> 8), uint8(b16 >> 8)}
	}
	// data[0] must be RED (img_*_1) and data[1] must be BLUE (img_*_2) despite img_*_2 completing much faster!
	if decodedRGB[0][0] < 180 || decodedRGB[0][2] > 60 {
		t.Fatalf("data[0] out-of-order pixel: expected RED, got RGB=%v", decodedRGB[0])
	}
	if decodedRGB[1][2] < 180 || decodedRGB[1][0] > 60 {
		t.Fatalf("data[1] out-of-order pixel: expected BLUE, got RGB=%v", decodedRGB[1])
	}
	// Verify true concurrency (maxConcurrent >= 2) inside single ws-imagegen process.
	capRaw, err := os.ReadFile(capFile)
	if err != nil {
		t.Fatalf("read capture file: %v", err)
	}
	maxConcSeen := 0
	sc := bufio.NewScanner(bytes.NewReader(capRaw))
	for sc.Scan() {
		var recCap struct {
			MaxConcurrent        int  `json:"maxConcurrent"`
			HasProcessPerSession bool `json:"hasProcessPerSession"`
		}
		if json.Unmarshal(sc.Bytes(), &recCap) == nil {
			if recCap.HasProcessPerSession {
				t.Fatal("ws-imagegen must not run with --process-per-session")
			}
			if recCap.MaxConcurrent > maxConcSeen {
				maxConcSeen = recCap.MaxConcurrent
			}
		}
	}
	if maxConcSeen < 2 {
		t.Fatalf("expected true n=2 concurrency inside ws-imagegen (maxConcurrent >= 2), got %d", maxConcSeen)
	}

	// 2. Default output_format="jpeg" passes upstream JPEG through zero-decode untouched.
	reqJPG := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/images/generations",
		strings.NewReader(`{"model":"gemini-3.1-flash-image","prompt":"[FAKE_IMAGEGEN_JPEG] zero decode jpeg"}`))
	reqJPG.Header.Set("Content-Type", "application/json")
	recJPG := httptest.NewRecorder()
	h.ServeHTTP(recJPG, reqJPG)
	if recJPG.Code != http.StatusOK {
		t.Fatalf("default jpeg expected 200, got %d: %s", recJPG.Code, recJPG.Body.String())
	}
	var respJPG struct {
		OutputFormat string `json:"output_format"`
		Data         []struct {
			B64JSON string `json:"b64_json"`
		} `json:"data"`
	}
	if err := json.Unmarshal(recJPG.Body.Bytes(), &respJPG); err != nil {
		t.Fatalf("unmarshal jpeg response: %v", err)
	}
	if respJPG.OutputFormat != "jpeg" || len(respJPG.Data) != 1 {
		t.Fatalf("expected output_format=jpeg and 1 image, got %+v", respJPG)
	}
	rawJPG, err := base64.StdEncoding.DecodeString(respJPG.Data[0].B64JSON)
	if err != nil {
		t.Fatalf("decode jpeg b64: %v", err)
	}
	if mime, ok := sniffAllowedImageMime(rawJPG); !ok || mime != "image/jpeg" {
		t.Fatalf("expected zero-decode JPEG passthrough, got mime=%q ok=%v", mime, ok)
	}

	// 3. Invalid output_format -> 400; multipart /v1/images/edits output_format=png -> 200 real PNG.
	reqBadFmt := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/images/generations",
		strings.NewReader(`{"prompt":"bad fmt","output_format":"webp"}`))
	reqBadFmt.Header.Set("Content-Type", "application/json")
	recBadFmt := httptest.NewRecorder()
	h.ServeHTTP(recBadFmt, reqBadFmt)
	if recBadFmt.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid output_format=webp, got %d", recBadFmt.Code)
	}

	var mpBuf bytes.Buffer
	mw := multipart.NewWriter(&mpBuf)
	_ = mw.WriteField("prompt", "[FAKE_IMAGEGEN_JPEG] multipart png transcode")
	_ = mw.WriteField("output_format", "png")
	fw, _ := mw.CreateFormFile("image", "ref.png")
	_, _ = fw.Write(makeDistinctTestPNG(10, 200, 10))
	_ = mw.Close()
	reqEditMP := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/images/edits", &mpBuf)
	reqEditMP.Header.Set("Content-Type", mw.FormDataContentType())
	recEditMP := httptest.NewRecorder()
	h.ServeHTTP(recEditMP, reqEditMP)
	if recEditMP.Code != http.StatusOK {
		t.Fatalf("multipart edit output_format=png expected 200, got %d: %s", recEditMP.Code, recEditMP.Body.String())
	}
	var editResp struct {
		OutputFormat string `json:"output_format"`
		Data         []struct {
			B64JSON string `json:"b64_json"`
		} `json:"data"`
	}
	if err := json.Unmarshal(recEditMP.Body.Bytes(), &editResp); err != nil || editResp.OutputFormat != "png" || len(editResp.Data) != 1 {
		t.Fatalf("unexpected multipart edit png response: %s", recEditMP.Body.String())
	}
	editPNGBytes, _ := base64.StdEncoding.DecodeString(editResp.Data[0].B64JSON)
	if mime, ok := sniffAllowedImageMime(editPNGBytes); !ok || mime != "image/png" {
		t.Fatalf("expected PNG bytes from multipart edit output_format=png, got mime=%q ok=%v", mime, ok)
	}

	// 4. Verify parseImageEditsJSON immediately clears preReadBody from request context (Go heap retention fix).
	jsonEditBody, _ := json.Marshal(map[string]any{
		"prompt": "heap release check",
		"image":  base64.StdEncoding.EncodeToString(makeDistinctTestPNG(50, 60, 70)),
	})
	reqPre := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/images/edits", bytes.NewReader(jsonEditBody))
	reqPre.Header.Set("Content-Type", "application/json")
	reqPre = withPreReadBody(reqPre, jsonEditBody)
	_, parsedImgs, code, msg := parseImageEditsJSON(httptest.NewRecorder(), reqPre)
	if code != 0 || len(parsedImgs) != 1 {
		t.Fatalf("parseImageEditsJSON failed: code=%d msg=%s", code, msg)
	}
	if _, stillPinned := preReadBodyFromContext(reqPre); stillPinned {
		t.Fatal("parseImageEditsJSON MUST call clearPreReadBody(r) so rawBody is not pinned on Go heap")
	}

	// 5. Verify cgroup memory pressure downgrades n>1 concurrency to 1 and adjusts dynamic idle TTL (45s -> 10s -> 0s).
	imageGenIdleTTL = 45 * time.Second
	if got := effectiveImageGenIdleTTL(); got != 45*time.Second {
		t.Fatalf("normal idle TTL expected 45s, got %v", got)
	}
	if got := effectiveImageGenBatchConcurrency(2); got != 2 {
		t.Fatalf("normal batch concurrency expected 2, got %d", got)
	}
	restoreWarn := setCgroupMemoryReaderForTest(func() CgroupMemoryStatus {
		return CgroupMemoryStatus{Version: 2, CurrentBytes: 4600 << 20, LimitBytes: 6144 << 20, WorkingSetBytes: 4600 << 20, UsagePct: 74.9, HasLiveUsage: true}
	})
	if got := effectiveImageGenBatchConcurrency(2); got != 1 {
		restoreWarn()
		t.Fatalf("warn pressure (>=warnPct) must downgrade batch concurrency to 1, got %d", got)
	}
	if got := effectiveImageGenIdleTTL(); got != 10*time.Second {
		restoreWarn()
		t.Fatalf("warn pressure idle TTL expected 10s, got %v", got)
	}
	restoreWarn()

	restoreHigh := setCgroupMemoryReaderForTest(func() CgroupMemoryStatus {
		return CgroupMemoryStatus{Version: 2, CurrentBytes: 5300 << 20, LimitBytes: 6144 << 20, WorkingSetBytes: 5300 << 20, UsagePct: 86.2, HasLiveUsage: true}
	})
	defer restoreHigh()
	if got := effectiveImageGenIdleTTL(); got != 0 {
		t.Fatalf("high pressure (>=highPct) idle TTL expected 0s immediate stop, got %v", got)
	}
	reqHigh := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/images/generations",
		strings.NewReader(`{"prompt":"stop right after high pressure"}`))
	reqHigh.Header.Set("Content-Type", "application/json")
	recHigh := httptest.NewRecorder()
	h.ServeHTTP(recHigh, reqHigh)
	if recHigh.Code != http.StatusOK {
		t.Fatalf("high-pressure generation expected 200, got %d: %s", recHigh.Code, recHigh.Body.String())
	}
	if GlobalImageGen.IsWarm() {
		t.Fatal("expected ws-imagegen engine to stop immediately on release() under high cgroup memory pressure")
	}
}

func TestV2326_GeminiV1BetaImageGenMultiTurnLast3AndStreamingAndModels(t *testing.T) {
	setLowCgroupMemoryForTest(t)
	capFile := filepath.Join(t.TempDir(), "v1beta-capture.jsonl")
	t.Setenv("FAKE_IMAGEGEN_CAPTURE_FILE", capFile)

	StopImageGenEngine()
	origTTL := imageGenIdleTTL
	imageGenIdleTTL = 30 * time.Second
	t.Cleanup(func() {
		imageGenIdleTTL = origTTL
		StopImageGenEngine()
	})

	gs := NewGatewayServer()
	t.Cleanup(gs.Close)
	h := gs.GatewayOnlyHandler()

	// 1. GET /v1beta/models exposes gemini-3.1-flash-image (thinking=false, generateContent+streamGenerateContent).
	reqList := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:18890/v1beta/models", nil)
	recList := httptest.NewRecorder()
	h.ServeHTTP(recList, reqList)
	if recList.Code != http.StatusOK {
		t.Fatalf("GET /v1beta/models status=%d body=%s", recList.Code, recList.Body.String())
	}
	var listResp struct {
		Models []struct {
			Name                       string   `json:"name"`
			Thinking                   bool     `json:"thinking"`
			SupportedGenerationMethods []string `json:"supportedGenerationMethods"`
		} `json:"models"`
	}
	if err := json.Unmarshal(recList.Body.Bytes(), &listResp); err != nil {
		t.Fatalf("unmarshal /v1beta/models: %v", err)
	}
	foundV1BetaImg := false
	for _, m := range listResp.Models {
		if m.Name == "models/"+imageGenServedModel {
			foundV1BetaImg = true
			if m.Thinking {
				t.Fatalf("expected thinking=false for %s", m.Name)
			}
			if len(m.SupportedGenerationMethods) != 2 || m.SupportedGenerationMethods[0] != "generateContent" || m.SupportedGenerationMethods[1] != "streamGenerateContent" {
				t.Fatalf("unexpected supportedGenerationMethods for %s: %v", m.Name, m.SupportedGenerationMethods)
			}
		}
	}
	if !foundV1BetaImg {
		t.Fatalf("expected models/%s in GET /v1beta/models", imageGenServedModel)
	}

	// 2. GET /v1beta/models/gemini-3.1-flash-image and -preview return 200 model details.
	for _, ep := range []string{
		"/v1beta/models/gemini-3.1-flash-image",
		"/v1beta/models/gemini-3.1-flash-image-preview",
	} {
		reqGet := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:18890"+ep, nil)
		recGet := httptest.NewRecorder()
		h.ServeHTTP(recGet, reqGet)
		if recGet.Code != http.StatusOK {
			t.Fatalf("GET %s expected 200, got %d: %s", ep, recGet.Code, recGet.Body.String())
		}
		var detail map[string]any
		if err := json.Unmarshal(recGet.Body.Bytes(), &detail); err != nil {
			t.Fatalf("unmarshal %s: %v", ep, err)
		}
		if detail["baseModelId"] != imageGenServedModel || detail["thinking"] != false {
			t.Fatalf("unexpected model detail for %s: %+v", ep, detail)
		}
	}

	// 3. Multi-turn :generateContent with 5 distinct images -> must automatically pick LAST 3 images in order!
	pngs := [][]byte{
		makeDistinctTestPNG(10, 10, 10),  // img0 (dropped)
		makeDistinctTestPNG(40, 40, 40),  // img1 (dropped)
		makeDistinctTestPNG(200, 10, 10), // img2 (kept #1)
		makeDistinctTestPNG(10, 200, 10), // img3 (kept #2)
		makeDistinctTestPNG(10, 10, 200), // img4 (kept #3)
	}
	b64Of := func(raw []byte) string { return base64.StdEncoding.EncodeToString(raw) }
	genPayload := map[string]any{
		"contents": []any{
			map[string]any{
				"role": "user",
				"parts": []any{
					map[string]any{"text": "first turn draw a base sketch"},
					map[string]any{"inlineData": map[string]any{"mimeType": "image/png", "data": b64Of(pngs[0])}},
					map[string]any{"inline_data": map[string]any{"mime_type": "image/png", "data": b64Of(pngs[1])}},
				},
			},
			map[string]any{
				"role": "model",
				"parts": []any{
					map[string]any{"text": "generated first draft"},
					map[string]any{"inlineData": map[string]any{"mimeType": "image/png", "data": b64Of(pngs[2])}},
				},
			},
			map[string]any{
				"role": "user",
				"parts": []any{
					map[string]any{"inlineData": map[string]any{"mimeType": "image/png", "data": b64Of(pngs[3])}},
					map[string]any{"inline_data": map[string]any{"mime_type": "image/png", "data": b64Of(pngs[4])}},
					map[string]any{"text": "[FAKE_IMAGEGEN_JPEG] blend these latest references into warm sunset poster"},
				},
			},
		},
		"generationConfig": map[string]any{
			"candidateCount":   2,
			"responseMimeType": "image/png",
			"imageConfig": map[string]any{
				"aspectRatio": "16:9",
			},
		},
	}
	genBytes, _ := json.Marshal(genPayload)
	reqGen := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1beta/models/gemini-3.1-flash-image:generateContent", bytes.NewReader(genBytes))
	reqGen.Header.Set("Content-Type", "application/json")
	recGen := httptest.NewRecorder()
	h.ServeHTTP(recGen, reqGen)
	if recGen.Code != http.StatusOK {
		t.Fatalf(":generateContent expected 200, got %d: %s", recGen.Code, recGen.Body.String())
	}
	if got := recGen.Header().Get("X-Agy-Served-Model"); got != imageGenServedModel {
		t.Fatalf("expected X-Agy-Served-Model=%s, got %q", imageGenServedModel, got)
	}
	var gemResp struct {
		ModelVersion string `json:"modelVersion"`
		Candidates   []struct {
			Index        int    `json:"index"`
			FinishReason string `json:"finishReason"`
			Content      struct {
				Role  string `json:"role"`
				Parts []struct {
					InlineData *struct {
						MimeType string `json:"mimeType"`
						Data     string `json:"data"`
					} `json:"inlineData"`
				} `json:"parts"`
			} `json:"content"`
		} `json:"candidates"`
	}
	if err := json.Unmarshal(recGen.Body.Bytes(), &gemResp); err != nil {
		t.Fatalf("unmarshal gemini image response: %v", err)
	}
	if gemResp.ModelVersion != imageGenServedModel || len(gemResp.Candidates) != 2 {
		t.Fatalf("unexpected gemini response meta/candidates: model=%q count=%d", gemResp.ModelVersion, len(gemResp.Candidates))
	}
	for i, c := range gemResp.Candidates {
		if c.Index != i || c.FinishReason != "STOP" || len(c.Content.Parts) != 1 || c.Content.Parts[0].InlineData == nil {
			t.Fatalf("candidate[%d] malformed: %+v", i, c)
		}
		if c.Content.Parts[0].InlineData.MimeType != "image/png" {
			t.Fatalf("candidate[%d] expected image/png mimeType, got %q", i, c.Content.Parts[0].InlineData.MimeType)
		}
		rawOut, err := base64.StdEncoding.DecodeString(c.Content.Parts[0].InlineData.Data)
		if err != nil {
			t.Fatalf("candidate[%d] invalid base64: %v", i, err)
		}
		if _, err := png.Decode(bytes.NewReader(rawOut)); err != nil {
			t.Fatalf("candidate[%d] png.Decode failed: %v", i, err)
		}
	}

	// Verify fakeacp received EXACTLY the last 3 images (pngs[2], pngs[3], pngs[4]), aspectRatio=16:9, and folded prompt.
	wantHashes := []string{sha256Hex(pngs[2]), sha256Hex(pngs[3]), sha256Hex(pngs[4])}
	capRaw, err := os.ReadFile(capFile)
	if err != nil {
		t.Fatalf("read v1beta capture file: %v", err)
	}
	sc := bufio.NewScanner(bytes.NewReader(capRaw))
	checkedCap := false
	for sc.Scan() {
		var recCap struct {
			AspectRatio  string   `json:"aspectRatio"`
			Prompt       string   `json:"prompt"`
			ImageCount   int      `json:"imageCount"`
			ImageSha256s []string `json:"imageSha256s"`
		}
		if err := json.Unmarshal(sc.Bytes(), &recCap); err != nil {
			t.Fatalf("unmarshal capture line: %v", err)
		}
		checkedCap = true
		if recCap.AspectRatio != "16:9" {
			t.Fatalf("expected aspectRatio=16:9, got %q", recCap.AspectRatio)
		}
		if recCap.ImageCount != 3 || len(recCap.ImageSha256s) != 3 {
			t.Fatalf("expected exactly last 3 images, got count=%d hashes=%v", recCap.ImageCount, recCap.ImageSha256s)
		}
		for i := 0; i < 3; i++ {
			if recCap.ImageSha256s[i] != wantHashes[i] {
				t.Fatalf("staged image[%d] hash=%s, want %s (must keep last 3 chronologically)", i, recCap.ImageSha256s[i], wantHashes[i])
			}
		}
		if !strings.Contains(recCap.Prompt, "blend these latest references") || !strings.Contains(recCap.Prompt, "first turn draw a base sketch") {
			t.Fatalf("expected folded prior context + latest instruction in prompt, got: %s", recCap.Prompt)
		}
	}
	if !checkedCap {
		t.Fatal("expected at least one capture entry from fakeacp")
	}

	// Verify temp dirs cleaned up.
	if entries, err := os.ReadDir(filepath.Join(PureApiHome, "tmp")); err == nil {
		for _, ent := range entries {
			if strings.HasPrefix(ent.Name(), "imagegen-") {
				t.Errorf("leaked temp dir after /v1beta image edit: %s", ent.Name())
			}
		}
	}

	// 4. Streaming :streamGenerateContent?alt=sse
	sseBody := `{"contents":[{"role":"user","parts":[{"text":"[FAKE_IMAGEGEN_JPEG] sse stream cat"}]}],"generationConfig":{"imageConfig":{"aspect_ratio":"3:4"}}}`
	reqSSE := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1beta/models/gemini-3.1-flash-image:streamGenerateContent?alt=sse", strings.NewReader(sseBody))
	reqSSE.Header.Set("Content-Type", "application/json")
	recSSE := httptest.NewRecorder()
	h.ServeHTTP(recSSE, reqSSE)
	if recSSE.Code != http.StatusOK {
		t.Fatalf(":streamGenerateContent?alt=sse expected 200, got %d: %s", recSSE.Code, recSSE.Body.String())
	}
	if !strings.Contains(recSSE.Header().Get("Content-Type"), "text/event-stream") {
		t.Fatalf("expected text/event-stream Content-Type, got %q", recSSE.Header().Get("Content-Type"))
	}
	var sseDataLine string
	for _, line := range strings.Split(recSSE.Body.String(), "\n") {
		if strings.HasPrefix(line, "data: ") {
			sseDataLine = strings.TrimPrefix(line, "data: ")
		}
	}
	if sseDataLine == "" {
		t.Fatalf("expected SSE data line in body: %s", recSSE.Body.String())
	}
	var sseObj struct {
		ModelVersion string `json:"modelVersion"`
		Candidates   []struct {
			Content struct {
				Parts []struct {
					InlineData *struct {
						MimeType string `json:"mimeType"`
						Data     string `json:"data"`
					} `json:"inlineData"`
				} `json:"parts"`
			} `json:"content"`
		} `json:"candidates"`
	}
	if err := json.Unmarshal([]byte(sseDataLine), &sseObj); err != nil || len(sseObj.Candidates) != 1 || sseObj.Candidates[0].Content.Parts[0].InlineData == nil {
		t.Fatalf("invalid SSE inlineData payload: err=%v line=%s", err, sseDataLine)
	}
	if sseObj.Candidates[0].Content.Parts[0].InlineData.MimeType != "image/jpeg" {
		t.Fatalf("expected image/jpeg in SSE inlineData, got %q", sseObj.Candidates[0].Content.Parts[0].InlineData.MimeType)
	}

	// 5. Streaming JSON array :streamGenerateContent (without alt=sse)
	reqArr := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1beta/models/gemini-3.1-flash-image-preview:streamGenerateContent", strings.NewReader(sseBody))
	reqArr.Header.Set("Content-Type", "application/json")
	recArr := httptest.NewRecorder()
	h.ServeHTTP(recArr, reqArr)
	if recArr.Code != http.StatusOK {
		t.Fatalf(":streamGenerateContent JSON array expected 200, got %d: %s", recArr.Code, recArr.Body.String())
	}
	var arrObjs []map[string]any
	if err := json.Unmarshal(recArr.Body.Bytes(), &arrObjs); err != nil || len(arrObjs) != 1 {
		t.Fatalf("expected valid 1-element JSON array stream, got err=%v body=%s", err, recArr.Body.String())
	}
}

func TestV2326_ChatCompletionsImageGenStreamingNonStreamingAnd403Allowlist(t *testing.T) {
	setLowCgroupMemoryForTest(t)
	capFile := filepath.Join(t.TempDir(), "chat-imagegen-capture.jsonl")
	t.Setenv("FAKE_IMAGEGEN_CAPTURE_FILE", capFile)

	StopImageGenEngine()
	origTTL := imageGenIdleTTL
	imageGenIdleTTL = 30 * time.Second
	t.Cleanup(func() {
		imageGenIdleTTL = origTTL
		StopImageGenEngine()
	})

	gs := NewGatewayServer()
	t.Cleanup(gs.Close)
	h := gs.GatewayOnlyHandler()

	// 1. Non-streaming /v1/chat/completions with 4 image_url refs (must take last 3), n=2, output_format=png, NO message.images.
	p0 := makeDistinctTestPNG(11, 22, 33)
	p1 := makeDistinctTestPNG(44, 55, 66)
	p2 := makeDistinctTestPNG(77, 88, 99)
	p3 := makeDistinctTestPNG(120, 140, 160)
	dataURI := func(raw []byte) string { return "data:image/png;base64," + base64.StdEncoding.EncodeToString(raw) }
	chatPayload, _ := json.Marshal(map[string]any{
		"model":         "gemini-3.1-flash-image",
		"aspect_ratio":  "4:3",
		"n":             2,
		"output_format": "png",
		"messages": []any{
			map[string]any{
				"role": "user",
				"content": []any{
					map[string]any{"type": "text", "text": "start with this moodboard"},
					map[string]any{"type": "image_url", "image_url": map[string]any{"url": dataURI(p0)}},
					map[string]any{"type": "image_url", "image_url": map[string]any{"url": dataURI(p1)}},
				},
			},
			map[string]any{
				"role": "user",
				"content": []any{
					map[string]any{"type": "image_url", "image_url": map[string]any{"url": dataURI(p2)}},
					map[string]any{"type": "image_url", "image_url": map[string]any{"url": dataURI(p3)}},
					map[string]any{"type": "text", "text": "[FAKE_IMAGEGEN_JPEG] render final studio photograph"},
				},
			},
		},
	})
	reqNonStream := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/chat/completions", bytes.NewReader(chatPayload))
	reqNonStream.Header.Set("Content-Type", "application/json")
	recNonStream := httptest.NewRecorder()
	h.ServeHTTP(recNonStream, reqNonStream)
	if recNonStream.Code != http.StatusOK {
		t.Fatalf("chat completions non-stream expected 200, got %d: %s", recNonStream.Code, recNonStream.Body.String())
	}
	if got := recNonStream.Header().Get("X-Agy-Served-Model"); got != imageGenServedModel {
		t.Fatalf("expected X-Agy-Served-Model=%s, got %q", imageGenServedModel, got)
	}
	var chatResp struct {
		Object  string `json:"object"`
		Model   string `json:"model"`
		Choices []struct {
			Message      map[string]any `json:"message"`
			FinishReason string         `json:"finish_reason"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(recNonStream.Body.Bytes(), &chatResp); err != nil {
		t.Fatalf("unmarshal chat image response: %v", err)
	}
	if chatResp.Object != "chat.completion" || chatResp.Model != imageGenServedModel || len(chatResp.Choices) != 1 {
		t.Fatalf("unexpected chat image response envelope: %+v", chatResp)
	}
	msgObj := chatResp.Choices[0].Message
	if _, hasDupImages := msgObj["images"]; hasDupImages {
		t.Fatal("chat completion image response MUST NOT duplicate base64 into message.images")
	}
	contentStr, _ := msgObj["content"].(string)
	mdParts := strings.Split(contentStr, "\n\n")
	if len(mdParts) != 2 {
		t.Fatalf("expected 2 Markdown images joined by \\n\\n, got %d parts: %q", len(mdParts), contentStr[:min(120, len(contentStr))])
	}
	mdImgRe := regexp.MustCompile(`^!\[image\]\(data:(image/[a-z]+);base64,([A-Za-z0-9+/=]+)\)$`)
	for i, part := range mdParts {
		m := mdImgRe.FindStringSubmatch(part)
		if len(m) != 3 || m[1] != "image/png" {
			t.Fatalf("mdParts[%d] invalid Markdown PNG image: %q", i, part[:min(80, len(part))])
		}
		raw, err := base64.StdEncoding.DecodeString(m[2])
		if err != nil {
			t.Fatalf("mdParts[%d] invalid b64: %v", i, err)
		}
		if _, err := png.Decode(bytes.NewReader(raw)); err != nil {
			t.Fatalf("mdParts[%d] png.Decode failed: %v", i, err)
		}
	}

	// Verify last 3 images (p1, p2, p3) were staged to fakeacp.
	wantChatHashes := []string{sha256Hex(p1), sha256Hex(p2), sha256Hex(p3)}
	capRaw, _ := os.ReadFile(capFile)
	sc := bufio.NewScanner(bytes.NewReader(capRaw))
	for sc.Scan() {
		var rc struct {
			AspectRatio  string   `json:"aspectRatio"`
			ImageCount   int      `json:"imageCount"`
			ImageSha256s []string `json:"imageSha256s"`
		}
		if json.Unmarshal(sc.Bytes(), &rc) == nil {
			if rc.AspectRatio != "4:3" || rc.ImageCount != 3 {
				t.Fatalf("expected aspectRatio=4:3 and 3 staged images, got %+v", rc)
			}
			for i := 0; i < 3; i++ {
				if rc.ImageSha256s[i] != wantChatHashes[i] {
					t.Fatalf("chat staged image[%d] hash=%s, want %s", i, rc.ImageSha256s[i], wantChatHashes[i])
				}
			}
		}
	}

	// 2. Streaming /v1/chat/completions (stream=true) with -preview alias
	streamPayload := `{"model":"gemini-3.1-flash-image-preview","stream":true,"messages":[{"role":"user","content":"[FAKE_IMAGEGEN_JPEG] streaming watercolor cat"}]}`
	reqStream := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/chat/completions", strings.NewReader(streamPayload))
	reqStream.Header.Set("Content-Type", "application/json")
	recStream := httptest.NewRecorder()
	h.ServeHTTP(recStream, reqStream)
	if recStream.Code != http.StatusOK {
		t.Fatalf("chat completions stream expected 200, got %d: %s", recStream.Code, recStream.Body.String())
	}
	if !strings.Contains(recStream.Header().Get("Content-Type"), "text/event-stream") {
		t.Fatalf("expected SSE Content-Type, got %q", recStream.Header().Get("Content-Type"))
	}
	var assembled strings.Builder
	sawDone := false
	sawStop := false
	for _, line := range strings.Split(recStream.Body.String(), "\n") {
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data: "))
		if payload == "[DONE]" {
			sawDone = true
			continue
		}
		var chunk struct {
			Object  string `json:"object"`
			Choices []struct {
				Delta        map[string]any `json:"delta"`
				FinishReason any            `json:"finish_reason"`
			} `json:"choices"`
		}
		if err := json.Unmarshal([]byte(payload), &chunk); err != nil {
			t.Fatalf("invalid SSE chunk json: %v line=%s", err, payload)
		}
		if chunk.Object != "chat.completion.chunk" {
			t.Fatalf("expected chat.completion.chunk, got %q", chunk.Object)
		}
		if len(chunk.Choices) > 0 {
			if cStr, ok := chunk.Choices[0].Delta["content"].(string); ok {
				assembled.WriteString(cStr)
			}
			if chunk.Choices[0].FinishReason == "stop" {
				sawStop = true
			}
		}
	}
	if !sawDone || !sawStop {
		t.Fatalf("expected finish_reason=stop and data: [DONE], sawStop=%v sawDone=%v", sawStop, sawDone)
	}
	mStream := mdImgRe.FindStringSubmatch(assembled.String())
	if len(mStream) != 3 || mStream[1] != "image/jpeg" {
		t.Fatalf("expected assembled stream Markdown JPEG image, got %q", assembled.String()[:min(80, assembled.Len())])
	}
	rawStreamImg, err := base64.StdEncoding.DecodeString(mStream[2])
	if err != nil {
		t.Fatalf("invalid streamed b64: %v", err)
	}
	if mime, ok := sniffAllowedImageMime(rawStreamImg); !ok || mime != "image/jpeg" {
		t.Fatalf("expected JPEG magic in streamed chat completion image, got mime=%q ok=%v", mime, ok)
	}

	// 3. SSRF protection on remote image_url in /v1/chat/completions
	ssrfBody := `{"model":"gemini-3.1-flash-image","messages":[{"role":"user","content":[{"type":"text","text":"edit"},{"type":"image_url","image_url":{"url":"http://127.0.0.1:8080/internal.png"}}]}]}`
	reqSSRF := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/chat/completions", strings.NewReader(ssrfBody))
	reqSSRF.Header.Set("Content-Type", "application/json")
	recSSRF := httptest.NewRecorder()
	h.ServeHTTP(recSSRF, reqSSRF)
	if recSSRF.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 SSRF block for loopback image_url, got %d: %s", recSSRF.Code, recSSRF.Body.String())
	}

	// 4. Strict per-account allowlist 403 enforcement across ALL 3 protocols + warm pool preference!
	origMgr := GlobalAccountManager
	tmp := t.TempDir()
	accPath := filepath.Join(tmp, "accounts.json")
	t.Setenv("AGY_GW_HOME", tmp)
	t.Setenv("AGY_ACCOUNTS_FILE", accPath)
	t.Setenv("AGY_API_KEYS", "")
	initConfig()
	t.Cleanup(func() {
		if GlobalAccountManager != nil && GlobalAccountManager != origMgr {
			GlobalAccountManager.StopAll()
		}
		GlobalAccountManager = origMgr
		initConfig()
	})

	seedAccounts := `{
  "version": 4,
  "routing": {"strategy": "round-robin"},
  "accounts": [
    {"id":"primary","label":"ChatOnly","primary":true,"enabled":true,"maxLiveWorkers":4,"allowedModels":["gemini-4-carbon"]},
    {"id":"img-a","label":"ImgA","primary":false,"enabled":true,"maxLiveWorkers":4,"allowedModels":["gemini-3.1-flash-image"]},
    {"id":"img-b","label":"ImgB","primary":false,"enabled":true,"maxLiveWorkers":4,"allowedModels":["gemini-3.1-flash-image"]}
  ]
}`
	if err := os.WriteFile(accPath, []byte(seedAccounts), 0o600); err != nil {
		t.Fatal(err)
	}
	am := NewAccountManager()
	am.Init()
	// Mark img-a and img-b as having OAuth tokens so pool routing considers them eligible.
	for _, id := range []string{"img-a", "img-b"} {
		tokPath := am.AccountTokenPath(id)
		_ = os.MkdirAll(filepath.Dir(tokPath), 0o700)
		_ = os.WriteFile(tokPath, []byte(`{"access_token":"fake"}`), 0o600)
		am.InvalidateTokenCache(id)
	}
	GlobalAccountManager = am

	// Verify extractRequestRoutingHintsV2 marks canonicalModel=gemini-3.1-flash-image and hasTools=false across all 3 protocols.
	hintCases := []*http.Request{
		httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"prompt":"x"}`)),
		httptest.NewRequest(http.MethodGet, "/v1beta/models/gemini-3.1-flash-image", nil),
		httptest.NewRequest(http.MethodPost, "/v1beta/models/gemini-3.1-flash-image:generateContent", strings.NewReader(`{"tools":[{"functionDeclarations":[{"name":"f"}]}],"contents":[{"role":"user","parts":[{"text":"x"}]}]}`)),
		httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"gemini-3.1-flash-image-preview","tools":[{"type":"function","function":{"name":"f"}}],"messages":[{"role":"user","content":"x"}]}`)),
	}
	for i, reqH := range hintCases {
		hints := extractRequestRoutingHintsV2(reqH)
		if hints.canonicalModel != imageGenServedModel || hints.hasTools {
			t.Fatalf("hintCase[%d] %s %s: got canonical=%q hasTools=%v, want %q/false",
				i, reqH.Method, reqH.URL.Path, hints.canonicalModel, hints.hasTools, imageGenServedModel)
		}
	}

	// Requests explicitly routed to primary (ChatOnly, no gemini-3.1-flash-image) MUST return 403 on ALL 3 protocols!
	denyCases := []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodPost, "/v1/images/generations", `{"prompt":"cat"}`},
		{http.MethodGet, "/v1beta/models/gemini-3.1-flash-image", ""},
		{http.MethodPost, "/v1beta/models/gemini-3.1-flash-image:generateContent", `{"contents":[{"role":"user","parts":[{"text":"cat"}]}]}`},
		{http.MethodPost, "/v1beta/models/gemini-3.1-flash-image:streamGenerateContent?alt=sse", `{"contents":[{"role":"user","parts":[{"text":"cat"}]}]}`},
		{http.MethodPost, "/v1/chat/completions", `{"model":"gemini-3.1-flash-image","messages":[{"role":"user","content":"cat"}]}`},
		{http.MethodPost, "/v1/chat/completions", `{"model":"gemini-3.1-flash-image-preview","stream":true,"messages":[{"role":"user","content":"cat"}]}`},
	}
	for _, dc := range denyCases {
		var rBody *strings.Reader
		if dc.body != "" {
			rBody = strings.NewReader(dc.body)
		} else {
			rBody = strings.NewReader("")
		}
		reqDeny := httptest.NewRequest(dc.method, "http://127.0.0.1:18890"+dc.path, rBody)
		reqDeny.Header.Set("Content-Type", "application/json")
		reqDeny.Header.Set("X-Agy-Account", "primary")
		recDeny := httptest.NewRecorder()
		h.ServeHTTP(recDeny, reqDeny)
		if recDeny.Code != http.StatusForbidden {
			t.Fatalf("unauthorized primary on %s %s MUST return 403 (never route to gemini-4-carbon), got %d: %s",
				dc.method, dc.path, recDeny.Code, recDeny.Body.String())
		}
	}

	// GET /v1beta/models for ChatOnly primary must hide gemini-3.1-flash-image.
	reqListPrim := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:18890/v1beta/models", nil)
	reqListPrim.Header.Set("X-Agy-Account", "primary")
	recListPrim := httptest.NewRecorder()
	h.ServeHTTP(recListPrim, reqListPrim)
	if recListPrim.Code != http.StatusOK || strings.Contains(recListPrim.Body.String(), imageGenServedModel) {
		t.Fatalf("expected /v1beta/models for primary to hide %s, status=%d body=%s", imageGenServedModel, recListPrim.Code, recListPrim.Body.String())
	}

	// Verify warm ws-imagegen mild preference in pool round-robin:
	// Mark img-b as having succeeded at image gen just now (<40s). Consecutive pool picks must reuse img-b!
	am.RecordAccountOutcome("img-b", imageGenServedModel, http.StatusOK, "", "", "")
	poolReq := httptest.NewRequest(http.MethodPost, "/v1/images/generations", nil)
	for i := 0; i < 3; i++ {
		pick := am.ResolveTargetAccountEx(poolReq, imageGenServedModel, ResolveOptions{})
		if pick.DenyErr != nil || pick.Account.ID != "img-b" {
			t.Fatalf("warm reuse pick[%d]: expected img-b, got acc=%q err=%v", i, pick.Account.ID, pick.DenyErr)
		}
	}
	// Once img-b has 2 in-flight image generations (imageGenInFlight >= 2), pool must spill over to img-a!
	rel1 := am.AcquireAccountImageGenInFlight("img-b")
	rel2 := am.AcquireAccountImageGenInFlight("img-b")
	defer rel1()
	defer rel2()
	if got := am.AccountImageGenInFlight("img-b"); got != 2 {
		t.Fatalf("expected img-b imageGenInFlight=2, got %d", got)
	}
	sawImgA := false
	for i := 0; i < 3; i++ {
		pick := am.ResolveTargetAccountEx(poolReq, imageGenServedModel, ResolveOptions{})
		if pick.DenyErr != nil {
			t.Fatalf("spill pick[%d] failed: %v", i, pick.DenyErr)
		}
		if pick.Account.ID == "img-a" {
			sawImgA = true
			break
		}
	}
	if !sawImgA {
		t.Fatal("expected pool routing to spill over to img-a once warm img-b reached imageGenInFlight == 2")
	}
}

func TestV2326_ImageGen40CountRecycleAndConversationsDBCleanupAndPressureGovernor(t *testing.T) {
	setLowCgroupMemoryForTest(t)
	StopImageGenEngine()
	origTTL := imageGenIdleTTL
	origMaxGen := imageGenMaxGenerations
	origMaxErr := imageGenMaxConsecErrors
	imageGenIdleTTL = 30 * time.Second
	imageGenMaxGenerations = 40
	imageGenMaxConsecErrors = 2
	t.Cleanup(func() {
		imageGenIdleTTL = origTTL
		imageGenMaxGenerations = origMaxGen
		imageGenMaxConsecErrors = origMaxErr
		StopImageGenEngine()
	})

	gs := NewGatewayServer()
	t.Cleanup(gs.Close)
	h := gs.GatewayOnlyHandler()

	convDir := filepath.Join(PureApiHome, ImageGenAppDataDirName, "conversations")
	countDBFiles := func() int {
		entries, err := os.ReadDir(convDir)
		if err != nil {
			return 0
		}
		n := 0
		for _, ent := range entries {
			low := strings.ToLower(ent.Name())
			if strings.HasSuffix(low, ".db") || strings.HasSuffix(low, ".db-wal") || strings.HasSuffix(low, ".db-shm") {
				n++
			}
		}
		return n
	}

	var firstGen uint64
	for i := 1; i <= 40; i++ {
		req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/images/generations",
			strings.NewReader(`{"prompt":"recycle test"}`))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("gen #%d expected 200, got %d: %s", i, rec.Code, rec.Body.String())
		}
		if i == 1 {
			firstGen = GlobalImageGen.eng.CurrentGen()
			if !GlobalImageGen.IsWarm() {
				t.Fatal("expected ws-imagegen to be warm after gen #1")
			}
			if countDBFiles() == 0 {
				t.Fatalf("expected fakeacp to create conversations/*.db files in %s", convDir)
			}
		}
		if i == 39 {
			if !GlobalImageGen.IsWarm() || GlobalImageGen.eng.CurrentGen() != firstGen {
				t.Fatalf("expected ws-imagegen to stay on single engine gen=%d through #39, warm=%v curGen=%d",
					firstGen, GlobalImageGen.IsWarm(), GlobalImageGen.eng.CurrentGen())
			}
		}
	}

	// After 40th generation completes and drains (inFlight == 0), engine MUST have stopped and cleaned conversations/*.db*!
	if GlobalImageGen.IsWarm() {
		t.Fatal("expected ws-imagegen to recycle/stop automatically at genCount == 40")
	}
	if left := countDBFiles(); left != 0 {
		t.Fatalf("expected all conversations/*.db* files cleaned on stopEngineLocked, %d left in %s", left, convDir)
	}

	// 41st generation boots a fresh engine cleanly.
	req41 := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/images/generations",
		strings.NewReader(`{"prompt":"post recycle gen 41"}`))
	req41.Header.Set("Content-Type", "application/json")
	rec41 := httptest.NewRecorder()
	h.ServeHTTP(rec41, req41)
	if rec41.Code != http.StatusOK {
		t.Fatalf("gen #41 expected 200, got %d: %s", rec41.Code, rec41.Body.String())
	}
	secondGen := GlobalImageGen.eng.CurrentGen()
	if !GlobalImageGen.IsWarm() || secondGen <= firstGen {
		t.Fatalf("expected fresh engine gen > %d after recycle, warm=%v curGen=%d",
			firstGen, GlobalImageGen.IsWarm(), secondGen)
	}

	// Verify engineStartedAt >= 20m age recycle valve drains and recycles to a new engine.
	GlobalImageGen.mu.Lock()
	GlobalImageGen.engineStartedAt = time.Now().Add(-21 * time.Minute)
	GlobalImageGen.mu.Unlock()
	reqAge := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/images/generations",
		strings.NewReader(`{"prompt":"post age recycle"}`))
	reqAge.Header.Set("Content-Type", "application/json")
	recAge := httptest.NewRecorder()
	h.ServeHTTP(recAge, reqAge)
	if recAge.Code != http.StatusOK {
		t.Fatalf("age recycle gen expected 200, got %d: %s", recAge.Code, recAge.Body.String())
	}
	if GlobalImageGen.eng.CurrentGen() <= secondGen {
		t.Fatalf("expected engine_age >= 20m to recycle engine gen > %d, got %d", secondGen, GlobalImageGen.eng.CurrentGen())
	}

	// Verify consecutiveErrors >= 2 also stops engine and cleans conversations/*.db*.
	for i := 0; i < 2; i++ {
		reqErr := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/images/generations",
			strings.NewReader(`{"prompt":"[FAKE_IMAGEGEN_QUOTA] fail"}`))
		reqErr.Header.Set("Content-Type", "application/json")
		recErr := httptest.NewRecorder()
		h.ServeHTTP(recErr, reqErr)
		if recErr.Code != http.StatusTooManyRequests {
			t.Fatalf("quota err #%d expected 429, got %d", i+1, recErr.Code)
		}
	}
	if GlobalImageGen.IsWarm() {
		t.Fatal("expected ws-imagegen to recycle/stop after 2 consecutive errors")
	}
	if left := countDBFiles(); left != 0 {
		t.Fatalf("expected conversations/*.db* cleaned after consecutive-error stop, %d left", left)
	}

	// Verify /api/internal/stop-idle-imagegen loopback security + functional stop, and global memory governor hook.
	reqWarm := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/images/generations",
		strings.NewReader(`{"prompt":"warm before internal stop"}`))
	reqWarm.Header.Set("Content-Type", "application/json")
	recWarm := httptest.NewRecorder()
	h.ServeHTTP(recWarm, reqWarm)
	if recWarm.Code != http.StatusOK || !GlobalImageGen.IsWarm() {
		t.Fatalf("warmup before internal stop failed: code=%d warm=%v", recWarm.Code, GlobalImageGen.IsWarm())
	}

	dashH := gs.DashboardOnlyHandler()
	reqNonLoop := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18891/api/internal/stop-idle-imagegen", nil)
	reqNonLoop.RemoteAddr = "198.51.100.7:54321"
	recNonLoop := httptest.NewRecorder()
	dashH.ServeHTTP(recNonLoop, reqNonLoop)
	if recNonLoop.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for non-loopback /api/internal/stop-idle-imagegen, got %d", recNonLoop.Code)
	}

	reqLoop := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18891/api/internal/stop-idle-imagegen", nil)
	reqLoop.RemoteAddr = "127.0.0.1:54321"
	recLoop := httptest.NewRecorder()
	dashH.ServeHTTP(recLoop, reqLoop)
	if recLoop.Code != http.StatusOK || GlobalImageGen.IsWarm() {
		t.Fatalf("loopback /api/internal/stop-idle-imagegen failed: code=%d body=%s warm=%v", recLoop.Code, recLoop.Body.String(), GlobalImageGen.IsWarm())
	}
	if !strings.Contains(recLoop.Body.String(), `"stopped":true`) {
		t.Fatalf("expected stopped:true in response, got %s", recLoop.Body.String())
	}

	// Verify runGlobalMemoryGovernorTick under warn pressure stops idle ws-imagegen BEFORE touching conversation workers.
	reqWarm2 := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/images/generations",
		strings.NewReader(`{"prompt":"warm before governor tick"}`))
	reqWarm2.Header.Set("Content-Type", "application/json")
	recWarm2 := httptest.NewRecorder()
	h.ServeHTTP(recWarm2, reqWarm2)
	if recWarm2.Code != http.StatusOK || !GlobalImageGen.IsWarm() {
		t.Fatalf("warmup before governor tick failed: code=%d warm=%v", recWarm2.Code, GlobalImageGen.IsWarm())
	}

	restoreMem := setCgroupMemoryReaderForTest(func() CgroupMemoryStatus {
		usage := 75.0
		if !GlobalImageGen.IsWarm() {
			// Simulate ~261MB reclaimed dropping container usage below warn threshold (70%).
			usage = 62.0
		}
		return CgroupMemoryStatus{
			Version:         2,
			CurrentBytes:    4500 << 20,
			LimitBytes:      6144 << 20,
			WorkingSetBytes: 4500 << 20,
			UsagePct:        usage,
			HasLiveUsage:    true,
		}
	})
	defer restoreMem()

	am := NewAccountManager()
	am.accounts = []AccountProfile{{ID: "primary", Label: "Primary", Primary: true, Enabled: true, MaxLiveWorkers: 8}}
	am.setPrimaryGatewayForTest(gs)
	tickRes := am.runGlobalMemoryGovernorTick(time.Now())
	if GlobalImageGen.IsWarm() {
		t.Fatal("expected runGlobalMemoryGovernorTick under warn pressure to stop idle ws-imagegen first")
	}
	if tickRes.HibernatedByMemWarn != 0 || tickRes.HibernatedByMemHigh != 0 {
		t.Fatalf("expected 0 cached conversation workers hibernated once stopping idle ws-imagegen relieved pressure, got %+v", tickRes)
	}
}

func TestV2326_P1_6_ImageGenGlobalConcurrencyCapCtxCancelAndP2_4Errors(t *testing.T) {
	setLowCgroupMemoryForTest(t)
	capFile := filepath.Join(t.TempDir(), "global-cap-capture.jsonl")
	t.Setenv("FAKE_IMAGEGEN_CAPTURE_FILE", capFile)

	StopImageGenEngine()
	origTTL := imageGenIdleTTL
	origMaxConc := imageGenMaxConcurrency
	imageGenIdleTTL = 30 * time.Second
	imageGenMaxConcurrency = 2
	t.Cleanup(func() {
		imageGenIdleTTL = origTTL
		imageGenMaxConcurrency = origMaxConc
		StopImageGenEngine()
	})

	gs := NewGatewayServer()
	t.Cleanup(gs.Close)
	h := gs.GatewayOnlyHandler()

	// 1. Cross-request global concurrency cap <= 2: launch 4 simultaneous HTTP clients (each n=1 with order delay).
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/images/generations",
				strings.NewReader(`{"model":"gemini-3.1-flash-image","prompt":"[FAKE_IMAGEGEN_ORDER_CONCUR] cross client cap"}`))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Errorf("concurrent imagegen request failed: code=%d body=%s", rec.Code, rec.Body.String())
			}
		}()
	}
	wg.Wait()

	capRaw, err := os.ReadFile(capFile)
	if err != nil {
		t.Fatalf("read capture file: %v", err)
	}
	maxConcSeen := 0
	sc := bufio.NewScanner(bytes.NewReader(capRaw))
	for sc.Scan() {
		var rc struct {
			MaxConcurrent int `json:"maxConcurrent"`
		}
		if json.Unmarshal(sc.Bytes(), &rc) == nil && rc.MaxConcurrent > maxConcSeen {
			maxConcSeen = rc.MaxConcurrent
		}
	}
	if maxConcSeen > 2 {
		t.Fatalf("ws-imagegen process-wide concurrency exceeded cap 2 across concurrent requests: observed maxConcurrent=%d", maxConcSeen)
	}

	// 2. Direct acquire(ctx) cap & prompt ctx cancellation (no 300s hang) + dynamic warnPct tightening to 1.
	_, rel1, err1 := GlobalImageGen.acquire(context.Background())
	if err1 != nil {
		t.Fatalf("acquire #1 failed: %v", err1)
	}
	_, rel2, err2 := GlobalImageGen.acquire(context.Background())
	if err2 != nil {
		rel1(true)
		t.Fatalf("acquire #2 failed: %v", err2)
	}

	ctx3, cancel3 := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel3()
	t0 := time.Now()
	_, _, err3 := GlobalImageGen.acquire(ctx3)
	if !errors.Is(err3, context.DeadlineExceeded) {
		rel2(true)
		rel1(true)
		t.Fatalf("expected 3rd acquire to abort promptly with DeadlineExceeded, got err=%v", err3)
	}
	if time.Since(t0) > 500*time.Millisecond {
		rel2(true)
		rel1(true)
		t.Fatalf("acquire(ctx) took too long to abort on ctx.Done(): %v", time.Since(t0))
	}
	rel2(true)

	// Under warnPct (>=70%), process-wide cap tightens to 1 so a 2nd acquire blocks and honors ctx cancellation.
	restoreWarn := setCgroupMemoryReaderForTest(func() CgroupMemoryStatus {
		return CgroupMemoryStatus{Version: 2, LimitBytes: 6144 << 20, WorkingSetBytes: 4600 << 20, UsagePct: 75.0, HasLiveUsage: true}
	})
	ctxWarn, cancelWarn := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancelWarn()
	_, _, errWarn := GlobalImageGen.acquire(ctxWarn)
	restoreWarn()
	rel1(true)
	if !errors.Is(errWarn, context.DeadlineExceeded) {
		t.Fatalf("expected warnPct (>=70%%) to tighten process-wide cap to 1 and block 2nd acquire until ctx cancel, got err=%v", errWarn)
	}

	// 3. P2-4: /v1beta image generation rejects fileData/file_data with explicit 400 instructing inlineData.
	fdBody := `{"contents":[{"role":"user","parts":[{"text":"edit this"},{"fileData":{"mimeType":"image/png","fileUri":"gs://bucket/a.png"}}]}]}`
	reqFD := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1beta/models/gemini-3.1-flash-image:generateContent", strings.NewReader(fdBody))
	reqFD.Header.Set("Content-Type", "application/json")
	recFD := httptest.NewRecorder()
	h.ServeHTTP(recFD, reqFD)
	if recFD.Code != http.StatusBadRequest || !strings.Contains(recFD.Body.String(), "inlineData") {
		t.Fatalf("expected 400 mentioning inlineData for /v1beta fileData image gen, got code=%d body=%s", recFD.Code, recFD.Body.String())
	}

	// 4. P2-4: /v1/chat/completions stream=true failing before headers committed returns standard JSON error (not raw SSE data:).
	reqStreamErr := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/chat/completions",
		strings.NewReader(`{"model":"gemini-3.1-flash-image","stream":true,"messages":[{"role":"user","content":"[FAKE_IMAGEGEN_QUOTA] fail before commit"}]}`))
	reqStreamErr.Header.Set("Content-Type", "application/json")
	recStreamErr := httptest.NewRecorder()
	h.ServeHTTP(recStreamErr, reqStreamErr)
	if recStreamErr.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 for stream quota failure, got code=%d body=%s", recStreamErr.Code, recStreamErr.Body.String())
	}
	if !strings.Contains(recStreamErr.Header().Get("Content-Type"), "application/json") {
		t.Fatalf("expected application/json Content-Type when error occurs before SSE commit, got %q", recStreamErr.Header().Get("Content-Type"))
	}
	var errJSON map[string]any
	if err := json.Unmarshal(recStreamErr.Body.Bytes(), &errJSON); err != nil || errJSON["error"] == nil {
		t.Fatalf("expected valid JSON error object before SSE commit, got err=%v body=%s", err, recStreamErr.Body.String())
	}
}

func TestV2327_GeminiRebindUnidentifiedToolResultsHibernatedPath(t *testing.T) {
	makeReqAndNorm := func(t *testing.T, body map[string]any) (*http.Request, *normalizedGeminiRequest, string) {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1beta/models/gemini-4-carbon:generateContent", nil)
		req.Header.Set("X-Session-Id", "gem-hib-conv-1")
		norm, err := normalizeGeminiV1BetaRequest("gemini-4-carbon", body, false)
		if err != nil {
			t.Fatalf("normalizeGeminiV1BetaRequest failed: %v", err)
		}
		sysText := normalizeSystemText(norm.Body["system"])
		toolsCatalog := sanitizeToolsCatalog(norm.Body["tools"])
		baseKey := deriveConversationKey(req, norm.Body, sysText, toolsCatalog, norm.Messages)
		return req, norm, baseKey
	}

	collectToolIDs := func(norm *normalizedGeminiRequest) (uses []string, results []string) {
		for _, m := range norm.Messages {
			arr, _ := m["content"].([]any)
			for _, item := range arr {
				bm, _ := item.(map[string]any)
				switch bm["type"] {
				case "tool_use":
					if id, _ := bm["id"].(string); id != "" {
						uses = append(uses, id)
					}
				case "tool_result":
					if id, _ := bm["tool_use_id"].(string); id != "" {
						results = append(results, id)
					}
				}
			}
		}
		return uses, results
	}

	t.Run("single_hibernated_match", func(t *testing.T) {
		gs := NewGatewayServer()
		t.Cleanup(gs.Close)

		body := map[string]any{
			"tools": []any{
				map[string]any{
					"functionDeclarations": []any{
						map[string]any{"name": "read_file", "description": "read a file"},
					},
				},
			},
			"contents": []any{
				map[string]any{"role": "user", "parts": []any{map[string]any{"text": "read main.go"}}},
				map[string]any{"role": "model", "parts": []any{
					map[string]any{"functionCall": map[string]any{"name": "read_file", "args": map[string]any{"path": "main.go"}}},
				}},
				map[string]any{"role": "user", "parts": []any{
					map[string]any{"functionResponse": map[string]any{"name": "read_file", "response": map[string]any{"content": "package main"}}},
				}},
			},
		}
		req, norm, baseKey := makeReqAndNorm(t, body)
		sess := NewAcpProjectSession(baseKey, DefaultCwd, "")
		sess.HibernatedParkedCalls = map[string]HibernatedCallMeta{
			"mcp_hib_real_1": {ToolName: "read_file", Seq: 1},
		}
		sess.HibernatedParkedCallIDs = map[string]bool{"mcp_hib_real_1": true}
		gs.mu.Lock()
		gs.sessions[baseKey] = sess
		gs.mu.Unlock()

		gs.rebindUnidentifiedGeminiToolResults(req, norm)
		uses, results := collectToolIDs(norm)
		if len(uses) != 1 || uses[0] != "mcp_hib_real_1" || len(results) != 1 || results[0] != "mcp_hib_real_1" {
			t.Fatalf("expected single hibernated rebind to mcp_hib_real_1, got uses=%v results=%v", uses, results)
		}
	})

	t.Run("ambiguous_hibernated_fail_closed", func(t *testing.T) {
		gs := NewGatewayServer()
		t.Cleanup(gs.Close)

		body := map[string]any{
			"tools": []any{
				map[string]any{
					"functionDeclarations": []any{
						map[string]any{"name": "read_file", "description": "read a file"},
					},
				},
			},
			"contents": []any{
				map[string]any{"role": "user", "parts": []any{map[string]any{"text": "read main.go"}}},
				map[string]any{"role": "model", "parts": []any{
					map[string]any{"functionCall": map[string]any{"name": "read_file", "args": map[string]any{"path": "main.go"}}},
				}},
				map[string]any{"role": "user", "parts": []any{
					map[string]any{"functionResponse": map[string]any{"name": "read_file", "response": map[string]any{"content": "package main"}}},
				}},
			},
		}
		req, norm, baseKey := makeReqAndNorm(t, body)
		s1 := NewAcpProjectSession(baseKey, DefaultCwd, "")
		s1.HibernatedParkedCalls = map[string]HibernatedCallMeta{"mcp_hib_a": {ToolName: "read_file", Seq: 1}}
		s1.HibernatedParkedCallIDs = map[string]bool{"mcp_hib_a": true}
		s2 := NewAcpProjectSession(baseKey+"#2", DefaultCwd, "")
		s2.HibernatedParkedCalls = map[string]HibernatedCallMeta{"mcp_hib_b": {ToolName: "read_file", Seq: 1}}
		s2.HibernatedParkedCallIDs = map[string]bool{"mcp_hib_b": true}
		gs.mu.Lock()
		gs.sessions[baseKey] = s1
		gs.sessions[baseKey+"#2"] = s2
		gs.mu.Unlock()

		gs.rebindUnidentifiedGeminiToolResults(req, norm)
		uses, results := collectToolIDs(norm)
		if len(uses) != 1 || !strings.HasPrefix(uses[0], "gemini_fifo_") || len(results) != 1 || !strings.HasPrefix(results[0], "gemini_fifo_") {
			t.Fatalf("expected ambiguous hibernated sessions to fail closed and keep gemini_fifo_*, got uses=%v results=%v", uses, results)
		}
	})

	t.Run("explicit_and_anonymous_mix", func(t *testing.T) {
		gs := NewGatewayServer()
		t.Cleanup(gs.Close)

		body := map[string]any{
			"tools": []any{
				map[string]any{
					"functionDeclarations": []any{
						map[string]any{"name": "read_file", "description": "read a file"},
					},
				},
			},
			"contents": []any{
				map[string]any{"role": "user", "parts": []any{map[string]any{"text": "read two files"}}},
				map[string]any{"role": "model", "parts": []any{
					map[string]any{"functionCall": map[string]any{"id": "mcp_hib_1", "name": "read_file", "args": map[string]any{"path": "a.go"}}},
					map[string]any{"functionCall": map[string]any{"name": "read_file", "args": map[string]any{"path": "b.go"}}},
				}},
				map[string]any{"role": "user", "parts": []any{
					map[string]any{"functionResponse": map[string]any{"id": "mcp_hib_1", "name": "read_file", "response": map[string]any{"content": "A"}}},
					map[string]any{"functionResponse": map[string]any{"name": "read_file", "response": map[string]any{"content": "B"}}},
				}},
			},
		}
		req, norm, baseKey := makeReqAndNorm(t, body)
		sess := NewAcpProjectSession(baseKey, DefaultCwd, "")
		sess.HibernatedParkedCalls = map[string]HibernatedCallMeta{
			"mcp_hib_1": {ToolName: "read_file", Seq: 1},
			"mcp_hib_2": {ToolName: "read_file", Seq: 2},
		}
		sess.HibernatedParkedCallIDs = map[string]bool{"mcp_hib_1": true, "mcp_hib_2": true}
		gs.mu.Lock()
		gs.sessions[baseKey] = sess
		gs.mu.Unlock()

		gs.rebindUnidentifiedGeminiToolResults(req, norm)
		uses, results := collectToolIDs(norm)
		if len(uses) != 2 || uses[0] != "mcp_hib_1" || uses[1] != "mcp_hib_2" ||
			len(results) != 2 || results[0] != "mcp_hib_1" || results[1] != "mcp_hib_2" {
			t.Fatalf("expected explicit+anon mix to keep mcp_hib_1 and rebind anon to mcp_hib_2, got uses=%v results=%v", uses, results)
		}
	})

	t.Run("global_worker_slot_header_classifier", func(t *testing.T) {
		if !isGeminiGlobalWorkerSlotError(529, "All global live worker slots are busy; retry shortly") {
			t.Fatal("expected 529 global live worker slot error to match")
		}
		if !isGeminiGlobalWorkerSlotError(http.StatusTooManyRequests, "Global worker admission queue is full; retry shortly") {
			t.Fatal("expected 429-mapped global admission queue error to match")
		}
		if isGeminiGlobalWorkerSlotError(529, "All live worker slots are busy; retry shortly") {
			t.Fatal("local worker slot error must NOT match global worker slot classifier")
		}
		if isGeminiGlobalWorkerSlotError(http.StatusInternalServerError, "All global live worker slots are busy") {
			t.Fatal("500 status must NOT match global worker slot classifier")
		}
	})
}

func TestV2327_ImageGenSafetyValveRestartAndToSSoftError(t *testing.T) {
	setLowCgroupMemoryForTest(t)
	StopImageGenEngine()

	origIdle := imageGenIdleTTL
	origMaxGen := imageGenMaxGenerations
	origMaxAge := imageGenMaxEngineAge
	origMaxErr := imageGenMaxConsecErrors
	t.Cleanup(func() {
		imageGenIdleTTL = origIdle
		imageGenMaxGenerations = origMaxGen
		imageGenMaxEngineAge = origMaxAge
		imageGenMaxConsecErrors = origMaxErr
		StopImageGenEngine()
	})
	imageGenIdleTTL = 30 * time.Second

	t.Run("gen_count_safety_valve_restarts_engine", func(t *testing.T) {
		StopImageGenEngine()
		imageGenMaxGenerations = 2
		imageGenMaxConsecErrors = 10
		imageGenMaxEngineAge = 20 * time.Minute

		eng1, rel1, err1 := GlobalImageGen.acquire(context.Background())
		if err1 != nil {
			t.Fatalf("acquire #1 failed: %v", err1)
		}
		gen1 := eng1.CurrentGen()
		rel1(true)
		if !GlobalImageGen.IsWarm() {
			t.Fatal("engine should stay warm after 1st generation (below cap 2)")
		}

		_, rel2, err2 := GlobalImageGen.acquire(context.Background())
		if err2 != nil {
			t.Fatalf("acquire #2 failed: %v", err2)
		}
		rel2(true)
		if GlobalImageGen.IsWarm() {
			t.Fatal("expected gen_count safety valve to stop engine at genCount==2")
		}

		eng3, rel3, err3 := GlobalImageGen.acquire(context.Background())
		if err3 != nil {
			t.Fatalf("acquire #3 after recycle failed: %v", err3)
		}
		gen3 := eng3.CurrentGen()
		rel3(true)
		if gen3 <= gen1 {
			t.Fatalf("expected fresh engine generation after recycle (> %d), got %d", gen1, gen3)
		}
	})

	t.Run("consecutive_errors_safety_valve_restarts_engine", func(t *testing.T) {
		StopImageGenEngine()
		imageGenMaxGenerations = 40
		imageGenMaxConsecErrors = 2
		imageGenMaxEngineAge = 20 * time.Minute

		_, rel1, err1 := GlobalImageGen.acquire(context.Background())
		if err1 != nil {
			t.Fatalf("acquire #1 failed: %v", err1)
		}
		rel1(false)
		if !GlobalImageGen.IsWarm() {
			t.Fatal("engine should stay warm after 1 error (below cap 2)")
		}

		_, rel2, err2 := GlobalImageGen.acquire(context.Background())
		if err2 != nil {
			t.Fatalf("acquire #2 failed: %v", err2)
		}
		rel2(false)
		if GlobalImageGen.IsWarm() {
			t.Fatal("expected consecutive_errors safety valve to stop engine at consecutiveErrors==2")
		}
	})

	t.Run("engine_age_safety_valve_restarts_engine", func(t *testing.T) {
		StopImageGenEngine()
		imageGenMaxGenerations = 40
		imageGenMaxConsecErrors = 10
		imageGenMaxEngineAge = 15 * time.Millisecond

		_, rel1, err1 := GlobalImageGen.acquire(context.Background())
		if err1 != nil {
			t.Fatalf("acquire #1 failed: %v", err1)
		}
		time.Sleep(25 * time.Millisecond)
		rel1(true)
		if GlobalImageGen.IsWarm() {
			t.Fatal("expected engine_age safety valve to stop engine after maxEngineAge elapsed")
		}
	})

	t.Run("classifyImageGenError_unwraps_tos_SoftError_503_without_retripping", func(t *testing.T) {
		if tripped, _ := IsTosTripped(); tripped {
			t.Skip("ToS breaker already tripped in process")
		}
		directSE := &SoftError{
			StatusCode: http.StatusServiceUnavailable,
			ErrorType:  "tos_circuit_breaker",
			Message:    "Antigravity ToS circuit breaker is tripped; operator intervention required",
		}
		gotDirect := classifyImageGenError(directSE, nil, "")
		if gotDirect.StatusCode != http.StatusServiceUnavailable || gotDirect.ErrType != "tos_circuit_breaker" || gotDirect.ErrCode != "tos_circuit_breaker" {
			t.Fatalf("expected 503 tos_circuit_breaker for direct SoftError, got %+v", gotDirect)
		}
		wrappedSE := fmt.Errorf("acquire failed: %w", directSE)
		gotWrapped := classifyImageGenError(wrappedSE, nil, "")
		if gotWrapped.StatusCode != http.StatusServiceUnavailable || gotWrapped.ErrType != "tos_circuit_breaker" || gotWrapped.ErrCode != "tos_circuit_breaker" {
			t.Fatalf("expected 503 tos_circuit_breaker for wrapped SoftError, got %+v", gotWrapped)
		}
		if tripped, _ := IsTosTripped(); tripped {
			t.Fatal("classifyImageGenError must NOT re-trip global ToS circuit breaker when unwrapping SoftError")
		}
	})
}

func makeLargeMultiChunkTestPNGAndJPEG(t *testing.T) (pngBytes []byte, jpegBytes []byte) {
	t.Helper()
	// 256x256 deterministic noisy RGBA image -> >120KB PNG (exercises >=3 48KB raw chunks + tail padding) and valid JPEG.
	img := image.NewRGBA(image.Rect(0, 0, 256, 256))
	var state uint32 = 0x12345678
	for y := 0; y < 256; y++ {
		for x := 0; x < 256; x++ {
			state = state*1664525 + 1013904223
			img.SetRGBA(x, y, color.RGBA{
				R: uint8(state >> 24),
				G: uint8(state >> 16),
				B: uint8(state >> 8),
				A: 255,
			})
		}
	}
	var pngBuf bytes.Buffer
	if err := png.Encode(&pngBuf, img); err != nil {
		t.Fatalf("png.Encode failed: %v", err)
	}
	var jpgBuf bytes.Buffer
	if err := jpeg.Encode(&jpgBuf, img, &jpeg.Options{Quality: 85}); err != nil {
		t.Fatalf("jpeg.Encode failed: %v", err)
	}
	if pngBuf.Len() <= 96*1024 {
		t.Fatalf("expected test PNG > 96KB to exercise multi-chunk streaming, got %d bytes", pngBuf.Len())
	}
	return pngBuf.Bytes(), jpgBuf.Bytes()
}

func TestV2327_ImageGenStreamingBase64ByteExactAndMarkdownScanner(t *testing.T) {
	origPNG, origJPEG := makeLargeMultiChunkTestPNGAndJPEG(t)

	// Verify pooled fastPNGEncoder transcode produces valid decodable PNG.
	transcoded, err := transcodeToPNGSerial(origJPEG, "image/jpeg")
	if err != nil {
		t.Fatalf("transcodeToPNGSerial failed: %v", err)
	}
	if mime, ok := sniffAllowedImageMime(transcoded); !ok || mime != "image/png" {
		t.Fatalf("expected PNG magic after fastPNGEncoder transcode, got mime=%q ok=%v", mime, ok)
	}
	if _, err := png.Decode(bytes.NewReader(transcoded)); err != nil {
		t.Fatalf("png.Decode on fastPNGEncoder output failed: %v", err)
	}

	makeResults := func() []generatedImageResult {
		return []generatedImageResult{
			{Raw: append([]byte(nil), origPNG...), Mime: "image/png"},
			{Raw: append([]byte(nil), origJPEG...), Mime: "image/jpeg"},
		}
	}

	t.Run("openai_images_json_stream_byte_exact", func(t *testing.T) {
		res := makeResults()
		rec := httptest.NewRecorder()
		writeOpenAIImagesJSONResponse(rec, "png", res)
		if res[0].Raw != nil || res[1].Raw != nil {
			t.Fatal("expected writeOpenAIImagesJSONResponse to clear Raw slices immediately")
		}
		var parsed struct {
			Created      int64  `json:"created"`
			OutputFormat string `json:"output_format"`
			Data         []struct {
				B64JSON string `json:"b64_json"`
			} `json:"data"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &parsed); err != nil {
			t.Fatalf("invalid streamed JSON: %v", err)
		}
		if parsed.OutputFormat != "png" || len(parsed.Data) != 2 {
			t.Fatalf("unexpected response metadata: %+v", parsed)
		}
		got0, err0 := base64.StdEncoding.DecodeString(parsed.Data[0].B64JSON)
		got1, err1 := base64.StdEncoding.DecodeString(parsed.Data[1].B64JSON)
		if err0 != nil || err1 != nil || !bytes.Equal(got0, origPNG) || !bytes.Equal(got1, origJPEG) {
			t.Fatalf("OpenAI images streamed base64 mismatch: err0=%v err1=%v match0=%v match1=%v",
				err0, err1, bytes.Equal(got0, origPNG), bytes.Equal(got1, origJPEG))
		}
	})

	t.Run("chat_non_stream_json_byte_exact", func(t *testing.T) {
		res := makeResults()
		rec := httptest.NewRecorder()
		writeChatImageJSONResponse(rec, "chatcmpl-test", 1700000000, imageGenServedModel, res, 12)
		if res[0].Raw != nil || res[1].Raw != nil {
			t.Fatal("expected writeChatImageJSONResponse to clear Raw slices immediately")
		}
		var parsed struct {
			Choices []struct {
				Message struct {
					Content string `json:"content"`
				} `json:"message"`
			} `json:"choices"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &parsed); err != nil || len(parsed.Choices) != 1 {
			t.Fatalf("invalid chat non-stream JSON: err=%v bodyLen=%d", err, rec.Body.Len())
		}
		var refs []chatImageRef
		stripped := extractAndStripMarkdownDataImages(parsed.Choices[0].Message.Content, "content", &refs)
		if stripped != "[image]\n\n[image]" || len(refs) != 2 {
			t.Fatalf("expected two markdown images separated by \\n\\n, got stripped=%q refs=%d", stripped, len(refs))
		}
		vi0, err0 := decodeEditImageString(refs[0].url, "r0")
		vi1, err1 := decodeEditImageString(refs[1].url, "r1")
		if err0 != nil || err1 != nil || !bytes.Equal(vi0.raw, origPNG) || !bytes.Equal(vi1.raw, origJPEG) {
			t.Fatalf("chat non-stream decoded bytes mismatch: err0=%v err1=%v match0=%v match1=%v",
				err0, err1, bytes.Equal(vi0.raw, origPNG), bytes.Equal(vi1.raw, origJPEG))
		}
	})

	t.Run("chat_sse_chunked_48kb_raw_byte_exact", func(t *testing.T) {
		res := makeResults()
		rec := httptest.NewRecorder()
		stream := protocol.NewSSEStreamSession(rec, protocol.SSEStreamOptions{})
		stream.DoLocked(func(sw *protocol.StreamWriter) {
			writeChatImageSSEChunks(sw, "chatcmpl-sse", 1700000000, imageGenServedModel, res, 12)
		})
		if res[0].Raw != nil || res[1].Raw != nil {
			t.Fatal("expected writeChatImageSSEChunks to clear Raw slices immediately")
		}
		var contentBuilder strings.Builder
		dataFrames := 0
		sawDone := false
		sc := bufio.NewScanner(bytes.NewReader(rec.Body.Bytes()))
		sc.Buffer(make([]byte, 0, 128*1024), 512*1024)
		for sc.Scan() {
			line := sc.Text()
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			payload := strings.TrimPrefix(line, "data: ")
			if payload == "[DONE]" {
				sawDone = true
				continue
			}
			dataFrames++
			var frame struct {
				Choices []struct {
					Delta struct {
						Content string `json:"content"`
					} `json:"delta"`
				} `json:"choices"`
			}
			if err := json.Unmarshal([]byte(payload), &frame); err != nil {
				t.Fatalf("invalid SSE JSON frame: %v", err)
			}
			if len(frame.Choices) > 0 {
				contentBuilder.WriteString(frame.Choices[0].Delta.Content)
			}
		}
		if !sawDone || dataFrames < 6 {
			t.Fatalf("expected multi-chunk SSE stream ending with [DONE], sawDone=%v dataFrames=%d", sawDone, dataFrames)
		}
		var refs []chatImageRef
		stripped := extractAndStripMarkdownDataImages(contentBuilder.String(), "sse", &refs)
		if stripped != "[image]\n\n[image]" || len(refs) != 2 {
			t.Fatalf("expected 2 images from concatenated SSE chunks, got stripped=%q refs=%d", stripped, len(refs))
		}
		vi0, err0 := decodeEditImageString(refs[0].url, "r0")
		vi1, err1 := decodeEditImageString(refs[1].url, "r1")
		if err0 != nil || err1 != nil || !bytes.Equal(vi0.raw, origPNG) || !bytes.Equal(vi1.raw, origJPEG) {
			t.Fatalf("chat SSE decoded bytes mismatch: err0=%v err1=%v match0=%v match1=%v",
				err0, err1, bytes.Equal(vi0.raw, origPNG), bytes.Equal(vi1.raw, origJPEG))
		}
	})

	t.Run("gemini_v1beta_sse_and_array_stream_byte_exact", func(t *testing.T) {
		for _, useSSE := range []bool{true, false} {
			res := makeResults()
			rec := httptest.NewRecorder()
			stream := protocol.NewSSEStreamSession(rec, protocol.SSEStreamOptions{})
			stream.DoLocked(func(sw *protocol.StreamWriter) {
				ok := sw.WriteStream(func(out io.Writer) error {
					bw := bufio.NewWriterSize(out, 32*1024)
					if useSSE {
						_, _ = bw.WriteString("data: ")
					} else {
						_, _ = bw.WriteString("[\n")
					}
					if err := writeGeminiImageResponseJSONToBuf(bw, "gem_img_1", imageGenServedModel, res, 15); err != nil {
						return err
					}
					if useSSE {
						_, _ = bw.WriteString("\n\n")
					} else {
						_, _ = bw.WriteString("\n]\n")
					}
					return bw.Flush()
				})
				if !ok {
					t.Errorf("WriteStream returned false (useSSE=%v)", useSSE)
				}
			})
			if res[0].Raw != nil || res[1].Raw != nil {
				t.Fatalf("expected Gemini streaming writer to clear Raw slices (useSSE=%v)", useSSE)
			}
			type gemResp struct {
				Candidates []struct {
					Content struct {
						Parts []struct {
							InlineData struct {
								MimeType string `json:"mimeType"`
								Data     string `json:"data"`
							} `json:"inlineData"`
						} `json:"parts"`
					} `json:"content"`
				} `json:"candidates"`
			}
			var got gemResp
			if useSSE {
				raw := strings.TrimSpace(rec.Body.String())
				if !strings.HasPrefix(raw, "data: ") {
					t.Fatalf("expected SSE data: prefix, got %.40q", raw)
				}
				if err := json.Unmarshal([]byte(strings.TrimPrefix(raw, "data: ")), &got); err != nil {
					t.Fatalf("unmarshal Gemini SSE JSON failed: %v", err)
				}
			} else {
				var arr []gemResp
				if err := json.Unmarshal(rec.Body.Bytes(), &arr); err != nil || len(arr) != 1 {
					t.Fatalf("unmarshal Gemini JSON array failed: err=%v len=%d", err, len(arr))
				}
				got = arr[0]
			}
			if len(got.Candidates) != 2 {
				t.Fatalf("expected 2 candidates (useSSE=%v), got %d", useSSE, len(got.Candidates))
			}
			d0, e0 := base64.StdEncoding.DecodeString(got.Candidates[0].Content.Parts[0].InlineData.Data)
			d1, e1 := base64.StdEncoding.DecodeString(got.Candidates[1].Content.Parts[0].InlineData.Data)
			if e0 != nil || e1 != nil || !bytes.Equal(d0, origPNG) || !bytes.Equal(d1, origJPEG) {
				t.Fatalf("Gemini streamed bytes mismatch (useSSE=%v): e0=%v e1=%v match0=%v match1=%v",
					useSSE, e0, e1, bytes.Equal(d0, origPNG), bytes.Equal(d1, origJPEG))
			}
		}
	})

	t.Run("single_pass_markdown_data_image_scanner", func(t *testing.T) {
		bigB64 := base64.StdEncoding.EncodeToString(origPNG)
		input := "Keep ![link](https://example.com/a.png) and ![broken](data:image/png;base64,ABC " +
			"and ![img1](data:image/png;base64," + bigB64 + ") plus ![outer ![inner](data:image/jpeg;base64,AAAA)"
		var refs []chatImageRef
		cleaned := extractAndStripMarkdownDataImages(input, "msg.0", &refs)
		wantCleaned := "Keep ![link](https://example.com/a.png) and ![broken](data:image/png;base64,ABC and [image] plus [image]"
		if cleaned != wantCleaned {
			t.Fatalf("unexpected stripped markdown:\ngot:  %q\nwant: %q", cleaned, wantCleaned)
		}
		if len(refs) != 2 || refs[0].url != "data:image/png;base64,"+bigB64 || refs[1].url != "data:image/jpeg;base64,AAAA" {
			t.Fatalf("unexpected extracted refs: count=%d", len(refs))
		}
	})
}

func TestV2327_MultipartStreamingEditStaging(t *testing.T) {
	origHome := PureApiHome
	tmpHome := t.TempDir()
	PureApiHome = tmpHome
	t.Cleanup(func() { PureApiHome = origHome })

	pngA := makeDistinctTestPNG(220, 30, 40)
	pngB := makeDistinctTestPNG(30, 40, 220)

	assertNoTempDirsLeaked := func(t *testing.T) {
		t.Helper()
		tmpRoot := filepath.Join(tmpHome, "tmp")
		entries, err := os.ReadDir(tmpRoot)
		if err != nil {
			return
		}
		for _, ent := range entries {
			if strings.HasPrefix(ent.Name(), "imagegen-") {
				t.Fatalf("leaked temp edit dir: %s", filepath.Join(tmpRoot, ent.Name()))
			}
		}
	}

	t.Run("streams_multiple_images_regardless_of_field_order", func(t *testing.T) {
		var buf bytes.Buffer
		mw := multipart.NewWriter(&buf)
		fw1, _ := mw.CreateFormFile("image", "first.png")
		_, _ = fw1.Write(pngA)
		_ = mw.WriteField("prompt", "blend these two")
		_ = mw.WriteField("output_format", "png")
		fw2, _ := mw.CreateFormFile("image[]", "second.png")
		_, _ = fw2.Write(pngB)
		_ = mw.Close()

		req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/images/edits", &buf)
		req.Header.Set("Content-Type", mw.FormDataContentType())
		params, tmpDir, absPaths, code, msg := parseImageEditsMultipart(httptest.NewRecorder(), req, 701)
		if tmpDir != "" {
			defer os.RemoveAll(tmpDir)
		}
		if code != 0 || msg != "" {
			t.Fatalf("expected success, got code=%d msg=%s", code, msg)
		}
		if params.Prompt != "blend these two" || params.OutputFormat != "png" || len(absPaths) != 2 {
			t.Fatalf("unexpected params=%+v paths=%v", params, absPaths)
		}
		gotA, errA := os.ReadFile(absPaths[0])
		gotB, errB := os.ReadFile(absPaths[1])
		if errA != nil || errB != nil || !bytes.Equal(gotA, pngA) || !bytes.Equal(gotB, pngB) {
			t.Fatalf("staged file bytes mismatch: errA=%v errB=%v", errA, errB)
		}
	})

	t.Run("rejects_bad_magic_after_32B_sniff_without_temp_leak", func(t *testing.T) {
		var buf bytes.Buffer
		mw := multipart.NewWriter(&buf)
		_ = mw.WriteField("prompt", "bad magic")
		fw, _ := mw.CreateFormFile("image", "spoof.png")
		_, _ = fw.Write([]byte("<html>this is definitely not a png image payload</html>"))
		_ = mw.Close()

		req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/images/edits", &buf)
		req.Header.Set("Content-Type", mw.FormDataContentType())
		_, tmpDir, _, code, _ := parseImageEditsMultipart(httptest.NewRecorder(), req, 702)
		if code != http.StatusBadRequest || tmpDir != "" {
			t.Fatalf("expected 400 and empty tmpDir, got code=%d tmpDir=%q", code, tmpDir)
		}
		assertNoTempDirsLeaked(t)
	})

	t.Run("rejects_oversize_413_and_cleans_up_temp", func(t *testing.T) {
		origMax := MaxImageBytes
		MaxImageBytes = 512
		t.Cleanup(func() { MaxImageBytes = origMax })

		oversize := append([]byte(nil), pngA...)
		oversize = append(oversize, bytes.Repeat([]byte{0x42}, 1024)...)

		var buf bytes.Buffer
		mw := multipart.NewWriter(&buf)
		_ = mw.WriteField("prompt", "too big")
		fw, _ := mw.CreateFormFile("image", "big.png")
		_, _ = fw.Write(oversize)
		_ = mw.Close()

		req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/images/edits", &buf)
		req.Header.Set("Content-Type", mw.FormDataContentType())
		_, tmpDir, _, code, _ := parseImageEditsMultipart(httptest.NewRecorder(), req, 703)
		if code != http.StatusRequestEntityTooLarge || tmpDir != "" {
			t.Fatalf("expected 413 and cleaned tmpDir, got code=%d tmpDir=%q", code, tmpDir)
		}
		assertNoTempDirsLeaked(t)
	})

	t.Run("rejects_more_than_maxImageEditInputs_and_cleans_up_temp", func(t *testing.T) {
		var buf bytes.Buffer
		mw := multipart.NewWriter(&buf)
		_ = mw.WriteField("prompt", "four images")
		for i := 0; i < maxImageEditInputs+1; i++ {
			fw, _ := mw.CreateFormFile("image[]", fmt.Sprintf("img_%d.png", i))
			_, _ = fw.Write(pngA)
		}
		_ = mw.Close()

		req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/images/edits", &buf)
		req.Header.Set("Content-Type", mw.FormDataContentType())
		_, tmpDir, _, code, _ := parseImageEditsMultipart(httptest.NewRecorder(), req, 704)
		if code != http.StatusBadRequest || tmpDir != "" {
			t.Fatalf("expected 400 for >%d images, got code=%d tmpDir=%q", maxImageEditInputs, code, tmpDir)
		}
		assertNoTempDirsLeaked(t)
	})
}

func TestV2327_TruncatedPDFMissingEOFRejected400(t *testing.T) {
	validShort := []byte("%PDF-1.4\n1 0 obj\n<< /Type /Catalog >>\nendobj\n%%EOF\n")
	if !verifyMediaMagicBytes("application/pdf", validShort) {
		t.Fatalf("expected valid PDF with %s to pass magic check", "%%EOF")
	}

	validLarge := append([]byte("%PDF-1.7\n"), bytes.Repeat([]byte("A"), 10000)...)
	validLarge = append(validLarge, []byte("\n%%EOF\n")...)
	if !verifyMediaMagicBytes("application/pdf", validLarge) {
		t.Fatalf("expected large PDF with %s in trailing 4096B to pass magic check", "%%EOF")
	}

	truncated := []byte("%PDF-1.4\n1 0 obj\n<< /Type /Catalog >>\nstream\nabcdef12345")
	if verifyMediaMagicBytes("application/pdf", truncated) {
		t.Fatalf("expected truncated PDF without %s to fail magic check", "%%EOF")
	}

	staleEOF := append([]byte("%PDF-1.4\n%%EOF\n"), bytes.Repeat([]byte("B"), 5000)...)
	if verifyMediaMagicBytes("application/pdf", staleEOF) {
		t.Fatalf("expected PDF whose only %s is >4096 bytes before EOF to fail magic check", "%%EOF")
	}

	// End-to-end HTTP 400 verification on /v1beta and /v1/messages.
	gs := NewGatewayServer()
	t.Cleanup(gs.Close)
	h := gs.GatewayOnlyHandler()
	truncB64 := base64.StdEncoding.EncodeToString(truncated)

	msgPayload, _ := json.Marshal(map[string]any{
		"model": "gemini-4-carbon",
		"messages": []any{map[string]any{
			"role": "user",
			"content": []any{
				map[string]any{
					"type": "document",
					"source": map[string]any{
						"type":       "base64",
						"media_type": "application/pdf",
						"data":       truncB64,
					},
				},
			},
		}},
	})
	reqMsg := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/messages", bytes.NewReader(msgPayload))
	reqMsg.Header.Set("Content-Type", "application/json")
	recMsg := httptest.NewRecorder()
	h.ServeHTTP(recMsg, reqMsg)
	if recMsg.Code != http.StatusBadRequest || !strings.Contains(recMsg.Body.String(), "%%EOF") {
		t.Fatalf("expected HTTP 400 mentioning %%%%EOF for truncated PDF on /v1/messages, got %d: %s", recMsg.Code, recMsg.Body.String())
	}

	gemPayload, _ := json.Marshal(map[string]any{
		"contents": []any{map[string]any{
			"role": "user",
			"parts": []any{
				map[string]any{"text": "read this pdf"},
				map[string]any{"inlineData": map[string]any{"mimeType": "application/pdf", "data": truncB64}},
			},
		}},
	})
	reqGem := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1beta/models/gemini-4-carbon:generateContent", bytes.NewReader(gemPayload))
	reqGem.Header.Set("Content-Type", "application/json")
	recGem := httptest.NewRecorder()
	h.ServeHTTP(recGem, reqGem)
	if recGem.Code != http.StatusBadRequest || !strings.Contains(recGem.Body.String(), "%%EOF") {
		t.Fatalf("expected HTTP 400 mentioning %%%%EOF for truncated PDF on /v1beta, got %d: %s", recGem.Code, recGem.Body.String())
	}
}
