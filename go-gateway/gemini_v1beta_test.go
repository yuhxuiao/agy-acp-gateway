package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestGeminiModelsListGetAndPagination(t *testing.T) {
	gs := NewGatewayServer()
	t.Cleanup(gs.Close)
	srv := httptest.NewServer(gs.GatewayOnlyHandler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/v1beta/models")
	if err != nil {
		t.Fatalf("GET /v1beta/models failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var listPayload struct {
		Models        []map[string]any `json:"models"`
		NextPageToken string           `json:"nextPageToken"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&listPayload); err != nil {
		t.Fatalf("decode models failed: %v", err)
	}
	if len(listPayload.Models) != len(AgyModels) {
		t.Fatalf("expected %d canonical models, got %d", len(AgyModels), len(listPayload.Models))
	}
	if listPayload.Models[0]["name"] != "models/gemini-4-carbon" {
		t.Fatalf("unexpected first model name: %v", listPayload.Models[0]["name"])
	}

	// Pagination: pageSize=2 -> 3 pages
	rPage1, err := http.Get(srv.URL + "/v1beta/models?pageSize=2")
	if err != nil {
		t.Fatalf("GET page 1 failed: %v", err)
	}
	var p1 struct {
		Models        []map[string]any `json:"models"`
		NextPageToken string           `json:"nextPageToken"`
	}
	_ = json.NewDecoder(rPage1.Body).Decode(&p1)
	rPage1.Body.Close()
	if len(p1.Models) != 2 || p1.NextPageToken != "2" {
		t.Fatalf("unexpected page 1: len=%d next=%q", len(p1.Models), p1.NextPageToken)
	}

	// Single model lookup (exact, models/ prefix, tier suffix, and 404 unknown)
	for _, p := range []string{
		"/v1beta/models/gemini-4-carbon",
		"/v1beta/models/models/gemini-4-hydrogen-xhigh",
	} {
		gr, errG := http.Get(srv.URL + p)
		if errG != nil {
			t.Fatalf("GET %s failed: %v", p, errG)
		}
		gr.Body.Close()
		if gr.StatusCode != http.StatusOK {
			t.Fatalf("GET %s expected 200, got %d", p, gr.StatusCode)
		}
	}

	notFoundResp, err := http.Get(srv.URL + "/v1beta/models/nonexistent-model-xyz")
	if err != nil {
		t.Fatalf("GET unknown model failed: %v", err)
	}
	var errBody map[string]any
	_ = json.NewDecoder(notFoundResp.Body).Decode(&errBody)
	notFoundResp.Body.Close()
	if notFoundResp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown model, got %d", notFoundResp.StatusCode)
	}
	errObj, _ := errBody["error"].(map[string]any)
	if errObj["status"] != "NOT_FOUND" {
		t.Fatalf("expected Gemini NOT_FOUND error envelope, got %+v", errBody)
	}
}

func TestGeminiAuthScopingAndDashboardIsolation(t *testing.T) {
	origKeys := apiKeys
	origAuthErr := authConfigErr
	apiKeys = [][]byte{[]byte("test-gemini-secret")}
	authConfigErr = nil
	t.Cleanup(func() {
		apiKeys = origKeys
		authConfigErr = origAuthErr
	})

	gs := NewGatewayServer()
	t.Cleanup(gs.Close)
	gwSrv := httptest.NewServer(gs.GatewayOnlyHandler())
	defer gwSrv.Close()
	dashSrv := httptest.NewServer(gs.DashboardOnlyHandler())
	defer dashSrv.Close()

	// 1. Unauthenticated /v1beta/ returns 401 UNAUTHENTICATED in Gemini format
	rUnauth, err := http.Get(gwSrv.URL + "/v1beta/models")
	if err != nil {
		t.Fatalf("GET unauth failed: %v", err)
	}
	var unauthBody map[string]any
	_ = json.NewDecoder(rUnauth.Body).Decode(&unauthBody)
	rUnauth.Body.Close()
	if rUnauth.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rUnauth.StatusCode)
	}
	if errObj, _ := unauthBody["error"].(map[string]any); errObj["status"] != "UNAUTHENTICATED" {
		t.Fatalf("expected UNAUTHENTICATED envelope, got %+v", unauthBody)
	}

	// 2. x-goog-api-key header, ?key= query, and trailing /key= work on /v1beta/
	reqHdr, _ := http.NewRequest(http.MethodGet, gwSrv.URL+"/v1beta/models", nil)
	reqHdr.Header.Set("x-goog-api-key", "test-gemini-secret")
	if r, err := http.DefaultClient.Do(reqHdr); err != nil || r.StatusCode != http.StatusOK {
		t.Fatalf("x-goog-api-key failed: status=%v err=%v", r, err)
	} else {
		r.Body.Close()
	}

	if r, err := http.Get(gwSrv.URL + "/v1beta/models?key=test-gemini-secret"); err != nil || r.StatusCode != http.StatusOK {
		t.Fatalf("?key= on /v1beta/models failed: status=%v err=%v", r, err)
	} else {
		r.Body.Close()
	}

	if r, err := http.Get(gwSrv.URL + "/v1beta/models/gemini-4-carbon/key=test-gemini-secret"); err != nil || r.StatusCode != http.StatusOK {
		t.Fatalf("trailing /key= on /v1beta/ failed: status=%v err=%v", r, err)
	} else {
		r.Body.Close()
	}

	// 3. ?key= MUST NOT authenticate /v1/models or dashboard routes
	if r, err := http.Get(gwSrv.URL + "/v1/models?key=test-gemini-secret"); err != nil || r.StatusCode != http.StatusUnauthorized {
		t.Fatalf("?key= leaked onto /v1/models: status=%v err=%v", r, err)
	} else {
		r.Body.Close()
	}
	if r, err := http.Get(dashSrv.URL + "/api/dashboard?key=test-gemini-secret"); err != nil || r.StatusCode != http.StatusUnauthorized {
		t.Fatalf("?key= leaked onto /api/dashboard: status=%v err=%v", r, err)
	} else {
		r.Body.Close()
	}

	// 4. Dashboard port blocks /v1beta/ with 404
	reqDash, _ := http.NewRequest(http.MethodGet, dashSrv.URL+"/v1beta/models", nil)
	reqDash.Header.Set("x-goog-api-key", "test-gemini-secret")
	if r, err := http.DefaultClient.Do(reqDash); err != nil || r.StatusCode != http.StatusNotFound {
		t.Fatalf("dashboard port did not isolate /v1beta/: status=%v err=%v", r, err)
	} else {
		r.Body.Close()
	}
}

func TestGeminiThinkingConfigAndModelAliases(t *testing.T) {
	cases := []struct {
		name     string
		model    string
		body     map[string]any
		wantID   string
		wantTier string
	}{
		{
			name:     "models prefix + THINKING_LEVEL_XHIGH on hydrogen",
			model:    "models/gemini-4-hydrogen",
			body:     map[string]any{"generationConfig": map[string]any{"thinkingConfig": map[string]any{"thinkingLevel": "THINKING_LEVEL_XHIGH"}}},
			wantID:   "gemini-4-hydrogen",
			wantTier: "xhigh",
		},
		{
			name:     "thinkingBudget -1 dynamic keeps medium default",
			model:    "gemini-4-carbon",
			body:     map[string]any{"generationConfig": map[string]any{"thinkingConfig": map[string]any{"thinkingBudget": -1}}},
			wantID:   "gemini-4-carbon",
			wantTier: "medium",
		},
		{
			name:     "thinkingBudget 0 maps to low",
			model:    "gemini-4-carbon-high",
			body:     map[string]any{"generationConfig": map[string]any{"thinkingConfig": map[string]any{"thinkingBudget": 0}}},
			wantID:   "gemini-4-carbon",
			wantTier: "low",
		},
		{
			name:     "snake_case thinking_budget 24576 maps to high",
			model:    "gemini-3.8-flash",
			body:     map[string]any{"generation_config": map[string]any{"thinking_config": map[string]any{"thinking_budget": 24576}}},
			wantID:   "gemini-3.8-flash",
			wantTier: "high",
		},
		{
			name:     "thinkingBudget 65536 maps to xhigh on hydrogen and clamps to high on carbon",
			model:    "gemini-4-hydrogen",
			body:     map[string]any{"generationConfig": map[string]any{"thinkingConfig": map[string]any{"thinkingBudget": 65536}}},
			wantID:   "gemini-4-hydrogen",
			wantTier: "xhigh",
		},
		{
			name:     "gemini-2.5-flash utility alias routes to gemini-3.8-flash",
			model:    "models/gemini-2.5-flash-lite",
			body:     nil,
			wantID:   "gemini-3.8-flash",
			wantTier: "medium",
		},
	}
	for _, tc := range cases {
		sel := ResolveRequestedModel(tc.model, tc.body)
		if sel.Model.ID != tc.wantID || sel.Tier != tc.wantTier {
			t.Errorf("%s: got (%s, %s), want (%s, %s)", tc.name, sel.Model.ID, sel.Tier, tc.wantID, tc.wantTier)
		}
	}

	// Top-level thinkingConfig alongside generationConfig must not lose thinkingLevel
	normCoexist, err := normalizeGeminiV1BetaRequest("gemini-4-carbon", map[string]any{
		"generationConfig": map[string]any{"temperature": 0.2, "maxOutputTokens": 512},
		"thinkingConfig":   map[string]any{"thinkingLevel": "HIGH"},
		"contents":         []any{map[string]any{"role": "user", "parts": []any{map[string]any{"text": "hi"}}}},
	}, false)
	if err != nil {
		t.Fatalf("normalizeGeminiV1BetaRequest coexist failed: %v", err)
	}
	selCoexist := ResolveRequestedModel("gemini-4-carbon", normCoexist.Body)
	if selCoexist.Tier != "high" {
		t.Fatalf("expected top-level thinkingConfig HIGH preserved alongside generationConfig, got tier=%s", selCoexist.Tier)
	}
}

func TestGeminiCountTokensAndUsageMetadataMath(t *testing.T) {
	u := &TurnUsage{
		InputTokens:      4200,
		CachedReadTokens: 32768,
		OutputTokens:     180,
		ThinkingTokens:   60,
	}
	meta := buildGeminiUsageMetadata(u)
	if meta["promptTokenCount"] != 36968 ||
		meta["cachedContentTokenCount"] != 32768 ||
		meta["candidatesTokenCount"] != 120 ||
		meta["thoughtsTokenCount"] != 60 ||
		meta["totalTokenCount"] != 37148 {
		t.Fatalf("unexpected Gemini usageMetadata math: %+v", meta)
	}

	gs := NewGatewayServer()
	t.Cleanup(gs.Close)
	srv := httptest.NewServer(gs.GatewayOnlyHandler())
	defer srv.Close()

	imgB64 := validTestPNGBase64()
	body1, _ := json.Marshal(map[string]any{
		"systemInstruction": map[string]any{"parts": []any{map[string]any{"text": "Be concise."}}},
		"contents": []any{
			map[string]any{
				"role": "user",
				"parts": []any{
					map[string]any{"text": strings.Repeat("abcd", 10)},
					map[string]any{"inlineData": map[string]any{"mimeType": "image/png", "data": imgB64}},
				},
			},
		},
	})
	r1, err := http.Post(srv.URL+"/v1beta/models/gemini-4-carbon:countTokens", "application/json", bytes.NewReader(body1))
	if err != nil {
		t.Fatalf("countTokens top-level failed: %v", err)
	}
	var ct1 struct {
		TotalTokens int `json:"totalTokens"`
	}
	_ = json.NewDecoder(r1.Body).Decode(&ct1)
	r1.Body.Close()
	if r1.StatusCode != http.StatusOK || ct1.TotalTokens <= 1600 {
		t.Fatalf("expected image+text tokens > 1600, got status=%d total=%d", r1.StatusCode, ct1.TotalTokens)
	}

	// Wrapped generateContentRequest form
	body2, _ := json.Marshal(map[string]any{
		"generateContentRequest": map[string]any{
			"model":    "models/gemini-4-carbon",
			"contents": []any{map[string]any{"role": "user", "parts": []any{map[string]any{"text": strings.Repeat("abcd", 10)}}}},
		},
	})
	r2, err := http.Post(srv.URL+"/v1beta/models/gemini-4-carbon:countTokens", "application/json", bytes.NewReader(body2))
	if err != nil {
		t.Fatalf("countTokens wrapped failed: %v", err)
	}
	var ct2 struct {
		TotalTokens int `json:"totalTokens"`
	}
	_ = json.NewDecoder(r2.Body).Decode(&ct2)
	r2.Body.Close()
	if r2.StatusCode != http.StatusOK || ct2.TotalTokens != 10 {
		t.Fatalf("expected totalTokens=10 on wrapped countTokens, got status=%d total=%d", r2.StatusCode, ct2.TotalTokens)
	}
}

func TestGeminiEndToEndGenerateStreamAndToolRoundtrip(t *testing.T) {
	origInterval := sseHeartbeatInterval
	origWriteTO := sseWriteTimeout
	sseHeartbeatInterval = 25 * time.Millisecond
	sseWriteTimeout = 80 * time.Millisecond
	t.Cleanup(func() {
		sseHeartbeatInterval = origInterval
		sseWriteTimeout = origWriteTO
	})

	baseURL, cleanup := startTestGateway(t)
	defer cleanup()

	// 1. Non-streaming :generateContent with omitted includeThoughts defaults to false (official Gemini v1beta spec)
	genBody, _ := json.Marshal(map[string]any{
		"systemInstruction": map[string]any{"parts": []any{map[string]any{"text": "Primary working directory: /tmp"}}},
		"generationConfig": map[string]any{
			"thinkingConfig": map[string]any{
				"thinkingLevel": "HIGH",
			},
		},
		"contents": []any{
			map[string]any{"role": "user", "parts": []any{map[string]any{"text": "[FAKE_THINK] hello native gemini"}}},
		},
	})
	rGen, err := http.Post(baseURL+"/v1beta/models/gemini-4-hydrogen:generateContent", "application/json", bytes.NewReader(genBody))
	if err != nil {
		t.Fatalf("generateContent failed: %v", err)
	}
	var genData map[string]any
	_ = json.NewDecoder(rGen.Body).Decode(&genData)
	rGen.Body.Close()
	if rGen.StatusCode != http.StatusOK {
		t.Fatalf("generateContent status=%d body=%+v", rGen.StatusCode, genData)
	}
	if genData["modelVersion"] != "gemini-4-hydrogen" {
		t.Fatalf("expected modelVersion=gemini-4-hydrogen, got %v", genData["modelVersion"])
	}
	cands, _ := genData["candidates"].([]any)
	cand0, _ := cands[0].(map[string]any)
	content0, _ := cand0["content"].(map[string]any)
	parts0, _ := content0["parts"].([]any)
	var gotText string
	for _, rp := range parts0 {
		pm, _ := rp.(map[string]any)
		if pm["thought"] == true {
			t.Fatalf("includeThoughts=false failed to suppress thought part: %+v", pm)
		}
		if tx, ok := pm["text"].(string); ok {
			gotText += tx
		}
	}
	if !strings.Contains(gotText, "FAKE_OK model=gemini-4-hydrogen tier=high") {
		t.Fatalf("unexpected generateContent text: %q", gotText)
	}

	// 2. Streaming :streamGenerateContent?alt=sse with includeThoughts=true + TTFT delay + post-thinking silence
	streamBody, _ := json.Marshal(map[string]any{
		"generationConfig": map[string]any{
			"thinkingConfig": map[string]any{"includeThoughts": true},
		},
		"contents": []any{
			map[string]any{
				"role":  "user",
				"parts": []any{map[string]any{"text": "[FAKE_TTFT_300MS] [FAKE_THINK] [FAKE_POST_THINK_SILENCE_300MS] stream native"}},
			},
		},
	})
	t0 := time.Now()
	rStream, err := http.Post(baseURL+"/v1beta/models/gemini-3.8-flash:streamGenerateContent?alt=sse", "application/json", bytes.NewReader(streamBody))
	if err != nil {
		t.Fatalf("streamGenerateContent SSE failed: %v", err)
	}
	sc := bufio.NewScanner(rStream.Body)
	firstDataMs := -1
	pings := 0
	sawThought := false
	sawFinish := false
	var streamText strings.Builder
	for sc.Scan() {
		ln := sc.Text()
		if ln == ": ping" {
			pings++
		} else if strings.HasPrefix(ln, "data:") {
			if firstDataMs < 0 {
				firstDataMs = int(time.Since(t0).Milliseconds())
			}
			var chunk map[string]any
			if err := json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(ln, "data:"))), &chunk); err != nil {
				t.Fatalf("invalid SSE JSON chunk: %v (%s)", err, ln)
			}
			cs, _ := chunk["candidates"].([]any)
			if len(cs) > 0 {
				c0, _ := cs[0].(map[string]any)
				if c0["finishReason"] == "STOP" {
					sawFinish = true
				}
				cnt, _ := c0["content"].(map[string]any)
				ps, _ := cnt["parts"].([]any)
				for _, rp := range ps {
					pm, _ := rp.(map[string]any)
					if pm["thought"] == true {
						sawThought = true
					} else if tx, ok := pm["text"].(string); ok {
						streamText.WriteString(tx)
					}
				}
			}
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("SSE scanner error: %v", err)
	}
	rStream.Body.Close()
	if firstDataMs < 270 {
		t.Fatalf("premature SSE first data chunk at %dms (<270ms)", firstDataMs)
	}
	if pings < 2 || !sawThought || !sawFinish {
		t.Fatalf("SSE incomplete: pings=%d sawThought=%v sawFinish=%v", pings, sawThought, sawFinish)
	}
	if strings.Count(streamText.String(), "FAKE_OK") != 1 {
		t.Fatalf("expected non-duplicated streamed text, got %q", streamText.String())
	}

	// 3. Streaming :streamGenerateContent without alt=sse returns valid JSON array
	rArr, err := http.Post(baseURL+"/v1beta/models/gemini-3.8-flash:streamGenerateContent", "application/json", bytes.NewReader(genBody))
	if err != nil {
		t.Fatalf("JSON array stream failed: %v", err)
	}
	arrBytes, err := io.ReadAll(rArr.Body)
	rArr.Body.Close()
	if err != nil {
		t.Fatalf("read JSON array stream failed: %v", err)
	}
	var arrChunks []map[string]any
	if err := json.Unmarshal(arrBytes, &arrChunks); err != nil || len(arrChunks) == 0 {
		t.Fatalf("invalid JSON array stream (err=%v): %s", err, string(arrBytes))
	}

	// 4. Tool roundtrip WITHOUT functionCall.id / functionResponse.id AND toolConfig.mode=NONE on Turn 2
	//    (also uses uppercase Gemini Schema types OBJECT / STRING and no explicit x-session-id header!)
	geminiTools := []any{
		map[string]any{
			"functionDeclarations": []any{
				map[string]any{
					"name":        "Bash",
					"description": "Run a shell command",
					"parameters": map[string]any{
						"type": "OBJECT",
						"properties": map[string]any{
							"command": map[string]any{"type": "STRING", "nullable": false},
						},
						"required": []any{"command"},
					},
				},
			},
		},
	}
	userPrompt := "[FAKE_TOOL_CALL] unique-gemini-fifo-roundtrip"
	turn1Req, _ := json.Marshal(map[string]any{
		"tools": geminiTools,
		"contents": []any{
			map[string]any{"role": "user", "parts": []any{map[string]any{"text": userPrompt}}},
		},
	})
	rt1, err := http.Post(baseURL+"/v1beta/models/gemini-4-carbon:generateContent", "application/json", bytes.NewReader(turn1Req))
	if err != nil {
		t.Fatalf("tool turn 1 failed: %v", err)
	}
	var d1 map[string]any
	_ = json.NewDecoder(rt1.Body).Decode(&d1)
	rt1.Body.Close()
	c1, _ := d1["candidates"].([]any)[0].(map[string]any)
	p1, _ := c1["content"].(map[string]any)["parts"].([]any)
	var fcMap map[string]any
	for _, rp := range p1 {
		if pm, ok := rp.(map[string]any); ok {
			if fc, okF := pm["functionCall"].(map[string]any); okF {
				fcMap = fc
			}
		}
	}
	if fcMap == nil || fcMap["name"] != "Bash" || fcMap["id"] == "" {
		t.Fatalf("expected Bash functionCall with id in turn 1, got %+v", d1)
	}

	// Strip "id" from BOTH model functionCall and user functionResponse + pass toolConfig.mode=NONE!
	turn2Req, _ := json.Marshal(map[string]any{
		"tools": geminiTools,
		"toolConfig": map[string]any{
			"functionCallingConfig": map[string]any{"mode": "NONE"},
		},
		"contents": []any{
			map[string]any{"role": "user", "parts": []any{map[string]any{"text": userPrompt}}},
			map[string]any{
				"role": "model",
				"parts": []any{
					map[string]any{
						"functionCall": map[string]any{
							"name": "Bash",
							"args": fcMap["args"],
						},
					},
				},
			},
			map[string]any{
				"role": "user",
				"parts": []any{
					map[string]any{
						"functionResponse": map[string]any{
							"name":     "Bash",
							"response": map[string]any{"output": "FIFO_TOOL_SUCCESS"},
						},
					},
				},
			},
		},
	})
	rt2, err := http.Post(baseURL+"/v1beta/models/gemini-4-carbon:generateContent", "application/json", bytes.NewReader(turn2Req))
	if err != nil {
		t.Fatalf("tool turn 2 failed: %v", err)
	}
	var d2 map[string]any
	_ = json.NewDecoder(rt2.Body).Decode(&d2)
	rt2.Body.Close()
	if rt2.StatusCode != http.StatusOK {
		t.Fatalf("tool turn 2 status=%d body=%+v", rt2.StatusCode, d2)
	}
	c2, _ := d2["candidates"].([]any)[0].(map[string]any)
	p2, _ := c2["content"].(map[string]any)["parts"].([]any)
	var turn2Text string
	for _, rp := range p2 {
		if pm, ok := rp.(map[string]any); ok {
			if tx, okT := pm["text"].(string); okT {
				turn2Text += tx
			}
		}
	}
	if !strings.Contains(turn2Text, "TOOL_DONE:FIFO_TOOL_SUCCESS") {
		t.Fatalf("expected warm parked MCP resume with TOOL_DONE:FIFO_TOOL_SUCCESS, got %q", turn2Text)
	}
}

func TestGeminiRebindRejectsForeignOrAmbiguousSession(t *testing.T) {
	gs := &GatewayServer{sessions: map[string]*AcpProjectSession{}, tenants: map[string]*AcpProjectSession{}}
	owner := NewAcpProjectSession("sid:owner:u:abc", "/tmp", "")
	owner.ParkedCalls["real_call_1"] = &ParkedMcpCall{Call: ToolCallItem{ID: "real_call_1", Name: "Bash"}, Delivered: true}
	owner.LastDeliveredCallIDs = []string{"real_call_1"}
	gs.sessions[owner.ConvKey] = owner

	reqBody := map[string]any{
		"contents": []any{
			map[string]any{"role": "user", "parts": []any{map[string]any{"text": "hi"}}},
			map[string]any{"role": "model", "parts": []any{map[string]any{"functionCall": map[string]any{"name": "Bash", "args": map[string]any{}}}}},
			map[string]any{"role": "user", "parts": []any{map[string]any{"functionResponse": map[string]any{"name": "Bash", "response": map[string]any{"output": "ok"}}}}},
		},
	}

	// Foreign identity must NOT rebind to owner's real_call_1
	normForeign, err := normalizeGeminiV1BetaRequest("gemini-4-carbon", reqBody, false)
	if err != nil {
		t.Fatalf("normalize foreign failed: %v", err)
	}
	rForeign, _ := http.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1beta/models/gemini-4-carbon:generateContent", nil)
	rForeign.Header.Set("x-session-id", "attacker")
	gs.rebindUnidentifiedGeminiToolResults(rForeign, normForeign)
	trsForeign := extractToolResultsFromMessages(normForeign.Messages)
	if len(trsForeign) != 1 || trsForeign[0].ToolCallID == "real_call_1" {
		t.Fatalf("foreign session hijacked parked call: %+v", trsForeign)
	}

	// Same identity with single parked session MUST rebind to real_call_1
	normOwner, _ := normalizeGeminiV1BetaRequest("gemini-4-carbon", reqBody, false)
	rOwner, _ := http.NewRequest(http.MethodPost, "http://127.0.0.1:18890/v1beta/models/gemini-4-carbon:generateContent", nil)
	rOwner.Header.Set("x-session-id", "owner")
	gs.rebindUnidentifiedGeminiToolResults(rOwner, normOwner)
	trsOwner := extractToolResultsFromMessages(normOwner.Messages)
	if len(trsOwner) != 1 || trsOwner[0].ToolCallID != "real_call_1" {
		t.Fatalf("owner session failed to rebind parked call: %+v", trsOwner)
	}

	// Ambiguous parallel slots under same identity without explicit IDs must fail closed
	slot2 := NewAcpProjectSession("sid:owner:u:abc#2", "/tmp", "")
	slot2.ParkedCalls["real_call_2"] = &ParkedMcpCall{Call: ToolCallItem{ID: "real_call_2", Name: "Bash"}, Delivered: true}
	gs.sessions[slot2.ConvKey] = slot2
	normAmb, _ := normalizeGeminiV1BetaRequest("gemini-4-carbon", reqBody, false)
	gs.rebindUnidentifiedGeminiToolResults(rOwner, normAmb)
	trsAmb := extractToolResultsFromMessages(normAmb.Messages)
	if len(trsAmb) != 1 || !strings.HasPrefix(trsAmb[0].ToolCallID, "gemini_fifo_") {
		t.Fatalf("ambiguous parallel slots did not fail closed: %+v", trsAmb)
	}

	// Remove slot2; verify stale unclosed historical functionCall + mixed explicit/omitted parallel IDs
	delete(gs.sessions, slot2.ConvKey)
	owner.ParkedCalls["real_call_2"] = &ParkedMcpCall{Call: ToolCallItem{ID: "real_call_2", Name: "Bash"}, Delivered: true}
	owner.LastDeliveredCallIDs = []string{"real_call_1", "real_call_2"}
	mixedHistory := map[string]any{
		"contents": []any{
			map[string]any{"role": "user", "parts": []any{map[string]any{"text": "turn 1"}}},
			// Unclosed historical explicit-ID call aborted by user in turn 1
			map[string]any{"role": "model", "parts": []any{map[string]any{"functionCall": map[string]any{"id": "stale_old_call", "name": "Bash", "args": map[string]any{}}}}},
			map[string]any{"role": "user", "parts": []any{map[string]any{"text": "never mind, run two bash commands now"}}},
			// Current turn: 1 explicit ID + 1 omitted ID for same tool name
			map[string]any{"role": "model", "parts": []any{
				map[string]any{"functionCall": map[string]any{"id": "real_call_1", "name": "Bash", "args": map[string]any{}}},
				map[string]any{"functionCall": map[string]any{"name": "Bash", "args": map[string]any{}}},
			}},
			map[string]any{"role": "user", "parts": []any{
				map[string]any{"functionResponse": map[string]any{"id": "real_call_1", "name": "Bash", "response": map[string]any{"output": "first"}}},
				map[string]any{"functionResponse": map[string]any{"name": "Bash", "response": map[string]any{"output": "second"}}},
			}},
		},
	}
	normMixed, err := normalizeGeminiV1BetaRequest("gemini-4-carbon", mixedHistory, false)
	if err != nil {
		t.Fatalf("normalize mixedHistory failed: %v", err)
	}
	gs.rebindUnidentifiedGeminiToolResults(rOwner, normMixed)
	trsMixed := extractToolResultsFromMessages(normMixed.Messages)
	if len(trsMixed) != 2 || trsMixed[0].ToolCallID != "real_call_1" || trsMixed[1].ToolCallID != "real_call_2" {
		t.Fatalf("expected [real_call_1, real_call_2] after stale FIFO reset + explicit reservation, got %+v", trsMixed)
	}
}
