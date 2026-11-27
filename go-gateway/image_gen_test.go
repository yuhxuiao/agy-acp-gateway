package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func validTestHEICBytes(brand string) []byte {
	b := []byte{0x00, 0x00, 0x00, 0x18, 'f', 't', 'y', 'p'}
	b = append(b, []byte(brand)...)
	b = append(b, 0x00, 0x00, 0x00, 0x00, 'm', 'i', 'f', '1', 'h', 'e', 'i', 'c')
	return b
}

func TestWsImageGenPluginIsolation(t *testing.T) {
	tmpHome := t.TempDir()
	origHome := PureApiHome
	PureApiHome = tmpHome
	t.Cleanup(func() { PureApiHome = origHome })

	EnsurePureApiHome()

	pluginJSONPath := filepath.Join(tmpHome, "config", "plugins", ImageGenAgentName, "plugin.json")
	pluginRaw, err := os.ReadFile(pluginJSONPath)
	if err != nil {
		t.Fatalf("missing ws-imagegen plugin.json: %v", err)
	}
	if !strings.Contains(string(pluginRaw), `"ws-imagegen"`) {
		t.Fatalf("unexpected plugin.json: %s", string(pluginRaw))
	}

	agentMDPath := filepath.Join(tmpHome, "config", "plugins", ImageGenAgentName, "agents", ImageGenAgentName+".md")
	mdRaw, err := os.ReadFile(agentMDPath)
	if err != nil {
		t.Fatalf("missing ws-imagegen.md: %v", err)
	}
	md := string(mdRaw)
	for _, want := range []string{
		"name: ws-imagegen",
		"mainAgent: true",
		"subagent: false",
		"inheritCustomizations: false",
		"excludeDefaultComponents: true",
		"  - generate_image",
	} {
		if !strings.Contains(md, want) {
			t.Fatalf("ws-imagegen.md missing %q:\n%s", want, md)
		}
	}

	// Verify strict isolation: ws-bridge must NOT include generate_image.
	bridgeMDPath := filepath.Join(tmpHome, "config", "plugins", McpBridgeName, "agents", McpBridgeName+".md")
	bridgeRaw, err := os.ReadFile(bridgeMDPath)
	if err != nil {
		t.Fatalf("missing ws-bridge.md: %v", err)
	}
	if strings.Contains(string(bridgeRaw), "generate_image") {
		t.Fatalf("ws-bridge.md must stay strictly isolated from generate_image:\n%s", string(bridgeRaw))
	}
}

func TestMapImageAspectRatio(t *testing.T) {
	cases := []struct {
		aspect  string
		size    string
		want    string
		wantErr bool
	}{
		{"", "", "1:1", false},
		{"", "1024x1024", "1:1", false},
		{"", "1536x1024", "3:2", false},
		{"", "1024x1536", "2:3", false},
		{"", "1792x1024", "16:9", false},
		{"", "1024x1792", "9:16", false},
		{"", "1280x960", "4:3", false},
		{"", "960x1280", "3:4", false},
		{"", "auto", "1:1", false},
		{"", "1600x900", "16:9", false},
		{"9:16", "1024x1024", "9:16", false},
		{"21:9", "", "", true},
		{"", "not-a-size", "", true},
		{"", "0x1024", "", true},
	}
	for _, tc := range cases {
		got, err := mapImageAspectRatio(tc.aspect, tc.size)
		if tc.wantErr {
			if err == nil {
				t.Errorf("mapImageAspectRatio(%q, %q) expected error, got %q", tc.aspect, tc.size, got)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("mapImageAspectRatio(%q, %q) = (%q, %v), want (%q, nil)", tc.aspect, tc.size, got, err, tc.want)
		}
	}
}

func TestImagesGenerationsAndEditsEndToEnd(t *testing.T) {
	origIdleTTL := imageGenIdleTTL
	imageGenIdleTTL = 30 * time.Second
	t.Cleanup(func() {
		imageGenIdleTTL = origIdleTTL
		StopImageGenEngine()
	})

	gs := NewGatewayServer()
	t.Cleanup(gs.Close)
	h := gs.GatewayOnlyHandler()

	t.Run("generations_single_and_multi_n_with_cleanup", func(t *testing.T) {
		body := `{"model":"gemini-3.1-flash-image","prompt":"a minimalist mountain sunrise","n":2,"size":"1536x1024","response_format":"b64_json"}`
		req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/images/generations", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
		}
		if got := rec.Header().Get("X-Agy-Served-Model"); got != imageGenServedModel {
			t.Fatalf("expected X-Agy-Served-Model=%s, got %q", imageGenServedModel, got)
		}
		var resp struct {
			Created int64 `json:"created"`
			Data    []struct {
				B64JSON string `json:"b64_json"`
			} `json:"data"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("invalid json response: %v", err)
		}
		if resp.Created <= 0 || len(resp.Data) != 2 {
			t.Fatalf("expected 2 images with created timestamp, got %+v", resp)
		}
		for i, item := range resp.Data {
			raw, err := base64.StdEncoding.DecodeString(item.B64JSON)
			if err != nil {
				t.Fatalf("data[%d] invalid base64: %v", i, err)
			}
			if mime, ok := sniffAllowedImageMime(raw); !ok || mime != "image/png" {
				t.Fatalf("data[%d] expected valid PNG bytes, got mime=%q ok=%v", i, mime, ok)
			}
		}
		// Verify temp artifacts in brain dir were deleted.
		brainRoot := filepath.Join(PureApiHome, ImageGenAppDataDirName, "brain")
		_ = filepath.WalkDir(brainRoot, func(p string, d os.DirEntry, err error) error {
			if err == nil && !d.IsDir() && strings.HasSuffix(d.Name(), ".png") {
				t.Errorf("leaked generated image artifact in brain dir: %s", p)
			}
			return nil
		})
	})

	t.Run("generations_fallback_brain_scan", func(t *testing.T) {
		body := `{"model":"nano-banana","prompt":"[FAKE_IMAGEGEN_FALLBACK_SCAN] sunset","aspect_ratio":"16:9"}`
		req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/images/generations", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 via fallback scan, got %d: %s", rec.Code, rec.Body.String())
		}
		var resp struct {
			Data []struct {
				B64JSON string `json:"b64_json"`
			} `json:"data"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil || len(resp.Data) != 1 || resp.Data[0].B64JSON == "" {
			t.Fatalf("expected 1 b64_json from fallback scan, got %s", rec.Body.String())
		}
	})

	t.Run("generations_quota_429_and_safety_400", func(t *testing.T) {
		reqQ := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/images/generations",
			strings.NewReader(`{"prompt":"[FAKE_IMAGEGEN_QUOTA] cat"}`))
		reqQ.Header.Set("Content-Type", "application/json")
		recQ := httptest.NewRecorder()
		h.ServeHTTP(recQ, reqQ)
		if recQ.Code != http.StatusTooManyRequests {
			t.Fatalf("expected 429 on quota error, got %d: %s", recQ.Code, recQ.Body.String())
		}
		if !strings.Contains(recQ.Body.String(), "rate_limit_error") {
			t.Fatalf("expected rate_limit_error in body: %s", recQ.Body.String())
		}

		reqS := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/images/generations",
			strings.NewReader(`{"prompt":"[FAKE_IMAGEGEN_SAFETY] blocked"}`))
		reqS.Header.Set("Content-Type", "application/json")
		recS := httptest.NewRecorder()
		h.ServeHTTP(recS, reqS)
		if recS.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 on safety error, got %d: %s", recS.Code, recS.Body.String())
		}
		if !strings.Contains(recS.Body.String(), "content_policy_violation") {
			t.Fatalf("expected content_policy_violation in body: %s", recS.Body.String())
		}
	})

	t.Run("generations_rejects_path_traversal_and_symlink_escape", func(t *testing.T) {
		for _, marker := range []string{"[FAKE_IMAGEGEN_TRAVERSAL]", "[FAKE_IMAGEGEN_SYMLINK_ESCAPE]"} {
			req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/images/generations",
				strings.NewReader(`{"prompt":"`+marker+` test"}`))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code == http.StatusOK {
				t.Fatalf("%s must NEVER succeed (got 200: %s)", marker, rec.Body.String())
			}
		}
	})

	t.Run("edits_multipart_and_json_and_temp_cleanup", func(t *testing.T) {
		pngRaw, _ := base64.StdEncoding.DecodeString(validTestPNGBase64())
		heicRaw := validTestHEICBytes("heic")

		// 1. Multipart with 2 images (image + image[])
		var buf bytes.Buffer
		mw := multipart.NewWriter(&buf)
		_ = mw.WriteField("prompt", "add warm golden hour light")
		_ = mw.WriteField("model", "gpt-image-1")
		_ = mw.WriteField("size", "1280x960")
		fw1, _ := mw.CreateFormFile("image", "photo1.png")
		_, _ = fw1.Write(pngRaw)
		fw2, _ := mw.CreateFormFile("image[]", "photo2.heic")
		_, _ = fw2.Write(heicRaw)
		_ = mw.Close()

		reqM := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/images/edits", &buf)
		reqM.Header.Set("Content-Type", mw.FormDataContentType())
		recM := httptest.NewRecorder()
		h.ServeHTTP(recM, reqM)
		if recM.Code != http.StatusOK {
			t.Fatalf("multipart /v1/images/edits expected 200, got %d: %s", recM.Code, recM.Body.String())
		}

		// 2. JSON with base64 + data URI
		jsonBody, _ := json.Marshal(map[string]any{
			"prompt": "make watercolor style",
			"images": []string{
				"data:image/png;base64," + validTestPNGBase64(),
				base64.StdEncoding.EncodeToString(heicRaw),
			},
			"aspect_ratio": "3:4",
		})
		reqJ := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/images/edits", bytes.NewReader(jsonBody))
		reqJ.Header.Set("Content-Type", "application/json")
		recJ := httptest.NewRecorder()
		h.ServeHTTP(recJ, reqJ)
		if recJ.Code != http.StatusOK {
			t.Fatalf("json /v1/images/edits expected 200, got %d: %s", recJ.Code, recJ.Body.String())
		}

		// 3. Reject >3 images & invalid magic bytes
		tooMany, _ := json.Marshal(map[string]any{
			"prompt": "too many",
			"images": []string{validTestPNGBase64(), validTestPNGBase64(), validTestPNGBase64(), validTestPNGBase64()},
		})
		req4 := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/images/edits", bytes.NewReader(tooMany))
		req4.Header.Set("Content-Type", "application/json")
		rec4 := httptest.NewRecorder()
		h.ServeHTTP(rec4, req4)
		if rec4.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 for >3 images, got %d", rec4.Code)
		}

		badImg, _ := json.Marshal(map[string]any{
			"prompt": "bad magic",
			"image":  base64.StdEncoding.EncodeToString([]byte("not an image payload")),
		})
		reqBad := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/images/edits", bytes.NewReader(badImg))
		reqBad.Header.Set("Content-Type", "application/json")
		recBad := httptest.NewRecorder()
		h.ServeHTTP(recBad, reqBad)
		if recBad.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 for bad image magic, got %d", recBad.Code)
		}

		// 4. Verify <PureApiHome>/tmp/imagegen-* temp dirs were cleaned up.
		tmpRoot := filepath.Join(PureApiHome, "tmp")
		if entries, err := os.ReadDir(tmpRoot); err == nil {
			for _, ent := range entries {
				if strings.HasPrefix(ent.Name(), "imagegen-") {
					t.Errorf("leaked temp edit dir: %s", filepath.Join(tmpRoot, ent.Name()))
				}
			}
		}
	})

	t.Run("auth_middleware_protects_image_endpoints", func(t *testing.T) {
		origKeys := apiKeys
		apiKeys = [][]byte{[]byte("test-secret-imagegen-key")}
		t.Cleanup(func() { apiKeys = origKeys })

		reqNoAuth := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/images/generations",
			strings.NewReader(`{"prompt":"hello"}`))
		reqNoAuth.Header.Set("Content-Type", "application/json")
		recNoAuth := httptest.NewRecorder()
		h.ServeHTTP(recNoAuth, reqNoAuth)
		if recNoAuth.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401 without API key, got %d", recNoAuth.Code)
		}

		reqWithAuth := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/images/generations",
			strings.NewReader(`{"prompt":"hello authorized"}`))
		reqWithAuth.Header.Set("Content-Type", "application/json")
		reqWithAuth.Header.Set("Authorization", "Bearer test-secret-imagegen-key")
		recWithAuth := httptest.NewRecorder()
		h.ServeHTTP(recWithAuth, reqWithAuth)
		if recWithAuth.Code != http.StatusOK {
			t.Fatalf("expected 200 with valid API key, got %d: %s", recWithAuth.Code, recWithAuth.Body.String())
		}
	})

	t.Run("idle_reaper_stops_ws_imagegen_engine", func(t *testing.T) {
		StopImageGenEngine()
		savedTTL := imageGenIdleTTL
		imageGenIdleTTL = 120 * time.Millisecond
		t.Cleanup(func() {
			imageGenIdleTTL = savedTTL
			StopImageGenEngine()
		})

		req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/images/generations",
			strings.NewReader(`{"prompt":"idle test"}`))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
		}
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if !GlobalImageGen.eng.IsAlive() {
				return
			}
			time.Sleep(30 * time.Millisecond)
		}
		t.Fatal("expected idle ws-imagegen engine to stop after imageGenIdleTTL")
	})
}

func TestImagesMultiAccountRoutingHints(t *testing.T) {
	// Verify extractRequestRoutingHintsV2 routes both image endpoints with gemini-3.1-flash-image and HasTools=false,
	// preserving multipart bodies intact for child worker reverse-proxy forwarding.
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	_ = mw.WriteField("prompt", "edit test")
	fw, _ := mw.CreateFormFile("image", "a.png")
	pngRaw, _ := base64.StdEncoding.DecodeString(validTestPNGBase64())
	_, _ = fw.Write(pngRaw)
	_ = mw.Close()

	origBytes := append([]byte(nil), buf.Bytes()...)
	req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/images/edits", bytes.NewReader(origBytes))
	req.Header.Set("Content-Type", mw.FormDataContentType())

	hints := extractRequestRoutingHintsV2(req)
	if hints.canonicalModel != imageGenServedModel {
		t.Fatalf("expected canonicalModel=%s, got %q", imageGenServedModel, hints.canonicalModel)
	}
	if hints.hasTools {
		t.Fatal("expected hasTools=false for /v1/images/edits")
	}
	if hints.malformedJSON {
		t.Fatal("multipart /v1/images/edits must not be flagged as malformedJSON")
	}
	if !bytes.Equal(hints.rawBody, origBytes) {
		t.Fatal("pre-read multipart body did not match original bytes")
	}
	reRead, _ := io.ReadAll(req.Body)
	if !bytes.Equal(reRead, origBytes) {
		t.Fatal("restored request body did not match original multipart bytes")
	}
}

func TestModelTelemetryServedModelFallbackAndStoreRollup(t *testing.T) {
	// Verify ModelFromEnumOrServed resolves known enums + dynamic served_model fallbacks.
	cases := []struct {
		enumVal int
		served  string
		wantID  string
		wantTr  string
	}{
		{1667, "", "gemini-pro-orchestrator", "high"},
		{1669, "", "gemini-flash-worker-agy", "medium"},
		{1322, "", "gemini-3.8-flash", "high"},
		{342, "", "gpt-oss-120b-medium", "medium"},
		{99991, "gemini-pro-orchestrator-neon", "gemini-pro-orchestrator-neon", "high"},
		{99992, "gemini-pro-orchestrator-argon", "gemini-pro-orchestrator-argon", "high"},
		{99993, "gemini-pro-orchestrator-g1-pro", "gemini-pro-orchestrator-argon", "high"},
		{99994, "unknown-future-model", "unknown", ""},
	}
	for _, tc := range cases {
		m, tr := ModelFromEnumOrServed(tc.enumVal, tc.served)
		if m.ID != tc.wantID || tr != tc.wantTr {
			t.Errorf("ModelFromEnumOrServed(%d, %q) = (%s, %s), want (%s, %s)", tc.enumVal, tc.served, m.ID, tr, tc.wantID, tc.wantTr)
		}
	}

	// Verify StateStore normalizes dynamic-enum orchestrator steps via served_model on both active & allTime queries.
	dbPath := filepath.Join(t.TempDir(), "telemetry_v2320.db")
	st := &StateStore{}
	st.Init(dbPath)
	t.Cleanup(st.Close)

	nowSec := time.Now().Unix()
	steps := []ParsedModelStep{
		{
			SessionID: "sess-orch-neon", StepIdx: 1, SourceCategory: "CLAUDE_GW", TsSec: nowSec,
			ModelEnum: 99991, ModelID: "unknown", ModelLabel: "其他/旧模型", ModelFamily: "other", QuotaGroup: "gemini",
			ServedModel: "gemini-pro-orchestrator-neon",
			RawIn:       1000000, RawCr: 0, OutTok: 100000, IsDshBridge: true,
		},
		{
			SessionID: "sess-orch-argon", StepIdx: 2, SourceCategory: "CLAUDE_GW", TsSec: nowSec,
			ModelEnum: 99992, ModelID: "unknown", ModelLabel: "其他/旧模型", ModelFamily: "other", QuotaGroup: "gemini",
			ServedModel: "gemini-pro-orchestrator-argon",
			RawIn:       1000000, RawCr: 0, OutTok: 100000, IsDshBridge: true,
		},
	}
	if _, err := st.persistUsageBatch("sess-orch-neon", cursorMeta{dbMtimeNs: 1, dbSize: 1, lastStepIdx: 2}, steps); err != nil {
		t.Fatalf("persistUsageBatch failed: %v", err)
	}

	active, ok := st.QueryActiveUsageSteps(nil, 86400)
	if !ok || len(active) != 2 {
		t.Fatalf("expected 2 active steps, got ok=%v len=%d", ok, len(active))
	}
	if active[0].ModelID != "gemini-pro-orchestrator-neon" || active[0].CostUsd != 1.5 {
		t.Fatalf("neon step misattributed/mispriced: id=%s cost=%v", active[0].ModelID, active[0].CostUsd)
	}
	if active[1].ModelID != "gemini-pro-orchestrator-argon" || active[1].CostUsd != 3.0 {
		t.Fatalf("argon step misattributed/mispriced: id=%s cost=%v", active[1].ModelID, active[1].CostUsd)
	}

	roll, ok := st.QueryUsageAllTimeRollup(nil)
	if !ok || roll.StepsCount != 2 || roll.CostUsd != 4.5 {
		t.Fatalf("expected allTime rollup steps=2 cost=4.5, got %+v ok=%v", roll, ok)
	}
}

func TestMultimodalHEICHEIFAndClearUnsupportedMediaErrors(t *testing.T) {
	// 1. Verify all 5 HEIC/HEIF ISOBMFF ftyp brands are sniffed and canonicalized.
	brands := map[string]string{
		"heic": "image/heic",
		"heix": "image/heic",
		"hevc": "image/heic",
		"mif1": "image/heif",
		"msf1": "image/heif",
	}
	for brand, wantMime := range brands {
		raw := validTestHEICBytes(brand)
		gotMime, ok := sniffAllowedImageMime(raw)
		if !ok || gotMime != wantMime {
			t.Fatalf("brand %q: got (%q, %v), want (%q, true)", brand, gotMime, ok, wantMime)
		}
		msgs := []map[string]any{
			{
				"role": "user",
				"content": []any{
					map[string]any{"type": "image", "source": map[string]any{
						"type": "base64", "media_type": wantMime, "data": base64.StdEncoding.EncodeToString(raw),
					}},
				},
			},
		}
		if err := CanonicalizeAndValidateMessages(nil, msgs, "/v1/messages"); err != nil {
			t.Fatalf("HEIC/HEIF brand %q rejected in CanonicalizeAndValidateMessages: %v", brand, err)
		}
	}

	// 2. Verify PDF, audio/*, video/* return 400 citing upstream agy acp PromptCapabilities{Image:true, Audio:false}.
	for _, mime := range []string{"application/pdf", "audio/mpeg", "video/mp4"} {
		err := rejectUnsupportedMedia(mime, "messages[0].content[0]")
		if err == nil {
			t.Fatalf("expected error for %s", mime)
		}
		msg := err.Error()
		if !strings.Contains(msg, "PromptCapabilities{Image:true, Audio:false}") {
			t.Fatalf("expected clear upstream PromptCapabilities note for %s, got: %s", mime, msg)
		}
	}
}

func TestClientWebFetchRestoredWhileServerWebFetchAndWebSearchSkipped(t *testing.T) {
	rawTools := []any{
		map[string]any{
			"name":        "WebFetch",
			"description": "Fetch a URL on the client",
			"input_schema": map[string]any{
				"type":       "object",
				"properties": map[string]any{"url": map[string]any{"type": "string"}},
				"required":   []any{"url"},
			},
		},
		map[string]any{
			"name": "WebSearch",
			"input_schema": map[string]any{
				"type":       "object",
				"properties": map[string]any{"query": map[string]any{"type": "string"}},
			},
		},
		map[string]any{
			"type": "web_fetch_20250910",
			"name": "web_fetch",
		},
	}
	cat := sanitizeToolsCatalog(rawTools)
	hasWebFetch := false
	for _, td := range cat {
		if td.Name == "WebFetch" {
			hasWebFetch = true
		}
		if td.Name == "WebSearch" {
			t.Fatal("WebSearch must remain stripped from MCP catalog (built-in search_web)")
		}
		if td.Name == "web_fetch" {
			t.Fatal("server-side web_fetch_20250910 without input_schema must remain skipped")
		}
	}
	if !hasWebFetch {
		t.Fatalf("client-executed WebFetch with input_schema must be preserved in tool catalog: %+v", cat)
	}
}

func TestExtractSavedImagePathUnicodeCaseFoldingSafety(t *testing.T) {
	// Turkish dotted capital 'İ' (U+0130) expands from 2 bytes to 3 bytes under strings.ToLower;
	// extractSavedImagePathFromText must never panic or corrupt byte offsets.
	want := "/tmp/pure/antigravity-cli/brain/sid-1/output_123.png"
	input := "İstanbul İİİ prefix — Generated image is saved at " + want + ". Extra trailing text."
	if got := extractSavedImagePathFromText(input); got != want {
		t.Fatalf("extractSavedImagePathFromText = %q, want %q", got, want)
	}
	// Multiple Turkish 'İ' before and after with short tail that would previously panic on slice bounds.
	short := "İİİİİİİİİİ Generated image is saved at /a/b.png."
	if got := extractSavedImagePathFromText(short); got != "/a/b.png" {
		t.Fatalf("extractSavedImagePathFromText(short) = %q, want /a/b.png", got)
	}
}

func TestBrainSessionSymlinkRejectionAndCancelCleanup(t *testing.T) {
	tmpHome := t.TempDir()
	origHome := PureApiHome
	PureApiHome = tmpHome
	t.Cleanup(func() {
		StopImageGenEngine()
		PureApiHome = origHome
	})
	EnsurePureApiHome()

	// 1. Direct brain/<sid> symlink pointing outside brain/ MUST be rejected & never deleted/followed.
	brainRoot := filepath.Join(tmpHome, ImageGenAppDataDirName, "brain")
	if err := os.MkdirAll(brainRoot, 0700); err != nil {
		t.Fatal(err)
	}
	outsideDir := filepath.Join(tmpHome, "outside-secret-dir")
	if err := os.MkdirAll(outsideDir, 0700); err != nil {
		t.Fatal(err)
	}
	outsidePNG := filepath.Join(outsideDir, "secret.png")
	pngRaw, _ := base64.StdEncoding.DecodeString(validTestPNGBase64())
	if err := os.WriteFile(outsidePNG, pngRaw, 0600); err != nil {
		t.Fatal(err)
	}
	sidSym := filepath.Join(brainRoot, "evil-sym-sid")
	if err := os.Symlink(outsideDir, sidSym); err != nil {
		t.Fatal(err)
	}
	if _, _, err := resolveGeneratedImageInBrainDir("evil-sym-sid", filepath.Join(sidSym, "secret.png"), time.Now().Add(-time.Minute)); err == nil {
		t.Fatal("expected brain/<sid> symlink to be rejected")
	}
	cleanupSessionBrainDir("evil-sym-sid")
	if _, err := os.Stat(outsidePNG); err != nil {
		t.Fatalf("cleanupSessionBrainDir must never follow brain/<sid> symlink into outside target: %v", err)
	}

	// 2. Cancelled turn + sidecar file inside verified brain/<sid> MUST be completely cleaned up.
	gs := NewGatewayServer()
	t.Cleanup(gs.Close)
	h := gs.GatewayOnlyHandler()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/images/generations",
		strings.NewReader(`{"prompt":"[FAKE_IMAGEGEN_SIDECAR] [FAKE_IMAGEGEN_CANCEL_AFTER_WRITE] cancel me"}`)).WithContext(ctx)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	// Confirm no leaked png or sidecar files remain in brainRoot (except our pre-created evil-sym-sid).
	entries, err := os.ReadDir(brainRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, ent := range entries {
		if ent.Name() == "evil-sym-sid" {
			continue
		}
		t.Errorf("leaked brain session dir after cancelled turn: %s", ent.Name())
	}
}

func TestImagesEditsSizeLimits413(t *testing.T) {
	origMaxImg := MaxImageBytes
	origMaxBody := MaxBodyBytes
	MaxImageBytes = 64
	MaxBodyBytes = 4096
	t.Cleanup(func() {
		MaxImageBytes = origMaxImg
		MaxBodyBytes = origMaxBody
	})

	gs := NewGatewayServer()
	t.Cleanup(gs.Close)
	h := gs.GatewayOnlyHandler()

	bigPNG := append([]byte("\x89PNG\r\n\x1a\n"), bytes.Repeat([]byte{'A'}, 128)...)

	// 1. Multipart file > MaxImageBytes -> 413
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	_ = mw.WriteField("prompt", "edit big")
	fw, _ := mw.CreateFormFile("image", "big.png")
	_, _ = fw.Write(bigPNG)
	_ = mw.Close()

	reqM := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/images/edits", &buf)
	reqM.Header.Set("Content-Type", mw.FormDataContentType())
	recM := httptest.NewRecorder()
	h.ServeHTTP(recM, reqM)
	if recM.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 for multipart image > MaxImageBytes, got %d: %s", recM.Code, recM.Body.String())
	}

	// 2. JSON image > MaxImageBytes -> 413
	jsonBody, _ := json.Marshal(map[string]any{
		"prompt": "edit big json",
		"image":  base64.StdEncoding.EncodeToString(bigPNG),
	})
	reqJ := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/images/edits", bytes.NewReader(jsonBody))
	reqJ.Header.Set("Content-Type", "application/json")
	recJ := httptest.NewRecorder()
	h.ServeHTTP(recJ, reqJ)
	if recJ.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 for JSON image > MaxImageBytes, got %d: %s", recJ.Code, recJ.Body.String())
	}

	// 3. Body > MaxBodyBytes -> 413
	hugeBuf := bytes.NewBuffer(bytes.Repeat([]byte{'X'}, int(MaxBodyBytes)+512))
	reqHuge := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/images/edits", hugeBuf)
	reqHuge.Header.Set("Content-Type", "application/json")
	recHuge := httptest.NewRecorder()
	h.ServeHTTP(recHuge, reqHuge)
	if recHuge.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 for body > MaxBodyBytes, got %d: %s", recHuge.Code, recHuge.Body.String())
	}
}

func TestImageGenConcurrentIdleAndBinarySwitchRace(t *testing.T) {
	StopImageGenEngine()
	tmpDir := t.TempDir()
	binV1 := filepath.Join(tmpDir, "fakeacp-v1")
	binV2 := filepath.Join(tmpDir, "fakeacp-v2")
	srcBytes, err := os.ReadFile(fakeAcpBin)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(binV1, srcBytes, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(binV2, srcBytes, 0755); err != nil {
		t.Fatal(err)
	}
	linkPath := filepath.Join(tmpDir, "agy-link")
	if err := os.Symlink(binV1, linkPath); err != nil {
		t.Fatal(err)
	}

	origBin := PatchedAgyBin
	origTTL := imageGenIdleTTL
	PatchedAgyBin = linkPath
	imageGenIdleTTL = 80 * time.Millisecond
	t.Cleanup(func() {
		StopImageGenEngine()
		PatchedAgyBin = origBin
		imageGenIdleTTL = origTTL
	})

	gs := NewGatewayServer()
	t.Cleanup(gs.Close)
	h := gs.GatewayOnlyHandler()

	// Warm up on v1.
	reqWarm := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/images/generations",
		strings.NewReader(`{"prompt":"warmup v1"}`))
	reqWarm.Header.Set("Content-Type", "application/json")
	recWarm := httptest.NewRecorder()
	h.ServeHTTP(recWarm, reqWarm)
	if recWarm.Code != http.StatusOK {
		t.Fatalf("warmup expected 200, got %d: %s", recWarm.Code, recWarm.Body.String())
	}
	startedBin, _, alive := GlobalImageGen.eng.StartedBinaryAndHome()
	if !alive || startedBin != binV1 {
		t.Fatalf("expected live v1 engine (%s), got alive=%v bin=%s", binV1, alive, startedBin)
	}

	// Run 4 concurrent slow requests on v1 while switching symlink v1 -> v2 mid-flight
	// and hammering StopIfIdle/StopIfBinaryChanged concurrently.
	var wg sync.WaitGroup
	errs := make(chan string, 8)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/images/generations",
				strings.NewReader(`{"prompt":"[FAKE_IMAGEGEN_SLOW_150MS] concurrent batch"}`))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				errs <- rec.Body.String()
			}
		}(i)
	}

	// Switch symlink atomically mid-flight while v1 requests are running.
	time.Sleep(40 * time.Millisecond)
	tmpLink := linkPath + ".new"
	if err := os.Symlink(binV2, tmpLink); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmpLink, linkPath); err != nil {
		t.Fatal(err)
	}
	// Hammer idle/switch reapers while requests are still in-flight (must not kill in-flight turns).
	for i := 0; i < 5; i++ {
		StopImageGenEngineIfBinaryChanged()
		GlobalImageGen.StopIfIdle()
		time.Sleep(10 * time.Millisecond)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatalf("concurrent v1 request failed during mid-flight binary switch: %s", e)
	}

	// Once the in-flight v1 wave drained, release() must have stopped v1 immediately.
	// With process-wide concurrency cap=2 and binaryChangedLocked() drain, queued requests #3/#4
	// may already have booted v2 cleanly without starving the binary switch.
	if binAfter, _, aliveAfter := GlobalImageGen.eng.StartedBinaryAndHome(); aliveAfter && binAfter == binV1 {
		t.Fatalf("expected v1 engine (%s) to stop immediately after draining once symlink switched to v2", binV1)
	}

	// Next request must boot cleanly on v2!
	reqV2 := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/images/generations",
		strings.NewReader(`{"prompt":"post switch v2"}`))
	reqV2.Header.Set("Content-Type", "application/json")
	recV2 := httptest.NewRecorder()
	h.ServeHTTP(recV2, reqV2)
	if recV2.Code != http.StatusOK {
		t.Fatalf("v2 request expected 200, got %d: %s", recV2.Code, recV2.Body.String())
	}
	startedBin2, _, alive2 := GlobalImageGen.eng.StartedBinaryAndHome()
	if !alive2 || startedBin2 != binV2 {
		t.Fatalf("expected live v2 engine (%s), got alive=%v bin=%s", binV2, alive2, startedBin2)
	}
}

func TestFixedTierModelsChatTurnNo32602(t *testing.T) {
	gs := NewGatewayServer()
	t.Cleanup(gs.Close)
	h := gs.GatewayOnlyHandler()

	for _, model := range []string{"gpt-oss-120b-medium", "gemini-pro-orchestrator"} {
		body, _ := json.Marshal(map[string]any{
			"model":            model,
			"reasoning_effort": "low",
			"messages":         []any{map[string]any{"role": "user", "content": "ping fixed tier"}},
		})
		req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1/chat/completions", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("model=%s with reasoning_effort=low expected 200 (no -32602), got %d: %s", model, rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), "FAKE_OK model="+model) {
			t.Fatalf("model=%s unexpected response body: %s", model, rec.Body.String())
		}
	}
}
