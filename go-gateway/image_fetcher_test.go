package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func pngBytes() []byte {
	return append([]byte("\x89PNG\r\n\x1a\n"), []byte{0, 0, 0, 13, 'I', 'H', 'D', 'R', 0, 0, 0, 1, 0, 0, 0, 1, 8, 6, 0, 0, 0}...)
}

func TestModuleBImageFetcherLazySkipCacheSingleflightAndRolloverDegrade(t *testing.T) {
	var hits sync.Map
	hitCount := func(url string) int {
		v, ok := hits.Load(url)
		if !ok {
			return 0
		}
		return int(v.(*atomic.Int64).Load())
	}
	incHit := func(url string) {
		val, _ := hits.LoadOrStore(url, &atomic.Int64{})
		val.(*atomic.Int64).Add(1)
	}

	rawPNG := pngBytes()
	fetcher := newRemoteImageFetcher(nil)
	fetcher.client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		u := req.URL.String()
		incHit(u)
		switch {
		case strings.HasSuffix(u, "/gone.png"):
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("gone")),
				Request:    req,
			}, nil
		default:
			return &http.Response{
				StatusCode:    http.StatusOK,
				ContentLength: int64(len(rawPNG)),
				Header:        http.Header{"Content-Type": []string{"image/png"}},
				Body:          io.NopCloser(bytes.NewReader(rawPNG)),
				Request:       req,
			}, nil
		}
	})

	histURL := "https://cdn.example.com/hist.png"
	curURL := "https://cdn.example.com/cur.png"
	toolURL := "https://cdn.example.com/tool.png"
	goneURL := "https://cdn.example.com/gone.png"

	histMsg := map[string]any{
		"role":    "user",
		"content": []any{map[string]any{"type": "image_ref", "url": histURL}, map[string]any{"type": "text", "text": "old"}},
	}
	asstMsg := map[string]any{"role": "assistant", "content": "ack"}
	curMsg := map[string]any{
		"role":    "user",
		"content": []any{map[string]any{"type": "image_ref", "url": curURL}, map[string]any{"type": "text", "text": "now"}},
	}
	allMsgs := []map[string]any{histMsg, asstMsg, curMsg}

	// 1. sticky_delta / resumed_delta: ONLY unseen curMsg resolved; histURL gets 0 requests.
	blocks, err := resolveAndBuildPromptBlocks(context.Background(), fetcher, []map[string]any{curMsg}, false, allMsgs, MaxTranscriptChar, ModelOrDefault("gemini-4-carbon"))
	if err != nil {
		t.Fatalf("unexpected sticky_delta error: %v", err)
	}
	if hitCount(histURL) != 0 || hitCount(curURL) != 1 {
		t.Fatalf("expected hist=0 cur=1, got hist=%d cur=%d", hitCount(histURL), hitCount(curURL))
	}
	if len(blocks) != 2 || blocks[0]["type"] != "image" || blocks[0]["mimeType"] != "image/png" {
		t.Fatalf("expected resolved image block + text block, got %+v", blocks)
	}

	// 2. Cache hit on repeat curURL fetch: hitCount stays 1.
	blocks2, err := resolveAndBuildPromptBlocks(context.Background(), fetcher, []map[string]any{curMsg}, false, allMsgs, MaxTranscriptChar, ModelOrDefault("gemini-4-carbon"))
	if err != nil || len(blocks2) != 2 || hitCount(curURL) != 1 {
		t.Fatalf("expected cache hit (cur=1), got hits=%d err=%v", hitCount(curURL), err)
	}

	// 3. parked_mcp_resume: resolves remote image_ref inside tool_result.
	trs := []ToolResultInput{{
		ToolCallID: "call_parked_1",
		Content:    "ok",
		Images:     []map[string]any{{"type": "image_ref", "url": toolURL}},
	}}
	resTrs, err := resolveToolResultImages(context.Background(), fetcher, trs, ModelOrDefault("gemini-4-carbon"))
	if err != nil || len(resTrs) != 1 || len(resTrs[0].Images) != 1 || resTrs[0].Images[0]["type"] != "image" || hitCount(toolURL) != 1 {
		t.Fatalf("expected parked tool_result image resolved, got err=%v trs=%+v hits=%d", err, resTrs, hitCount(toolURL))
	}

	// 4. Singleflight dedup across 12 concurrent callers.
	sharedURL := "https://cdn.example.com/shared.png"
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, fErr := fetcher.Fetch(context.Background(), sharedURL, "test"); fErr != nil {
				t.Errorf("concurrent Fetch failed: %v", fErr)
			}
		}()
	}
	wg.Wait()
	if hitCount(sharedURL) != 1 {
		t.Fatalf("expected singleflight to deduplicate 12 calls to 1 HTTP GET, got %d", hitCount(sharedURL))
	}

	// 5. Rollover graceful degradation: gone historical URL does NOT abort rollover, histURL gets anchor.
	goneMsg := map[string]any{
		"role":    "user",
		"content": []any{map[string]any{"type": "image_ref", "url": goneURL}, map[string]any{"type": "text", "text": "missing old"}},
	}
	rollAll := []map[string]any{goneMsg, asstMsg, histMsg, {"role": "assistant", "content": "ack2"}, curMsg}
	rollBlocks, err := resolveAndBuildPromptBlocks(context.Background(), fetcher, []map[string]any{curMsg}, true, rollAll, MaxTranscriptChar, ModelOrDefault("gemini-4-carbon"))
	if err != nil {
		t.Fatalf("rollover must degrade cleanly on expired history image, got %v", err)
	}
	resText, _ := rollBlocks[0]["resource"].(map[string]any)["text"].(string)
	if !strings.Contains(resText, "[Earlier remote image unavailable during rollover]") || !strings.Contains(resText, "[Image #1 (image/png") {
		t.Fatalf("expected rollover unavailable notice and [Image #1] anchor, got:\n%s", resText)
	}

	// 6. Rollover budget truncation keeps newest 100 images for Claude and emits omission note on oldest 5.
	b64PNG := base64.StdEncoding.EncodeToString(rawPNG)
	var histOverMsgs []map[string]any
	var imgArr []any
	for i := 0; i < 103; i++ {
		imgArr = append(imgArr, map[string]any{"type": "image", "mimeType": "image/png", "data": b64PNG, "decodedBytes": 64})
	}
	histOverMsgs = append(histOverMsgs, map[string]any{"role": "user", "content": imgArr})
	histOverMsgs = append(histOverMsgs, map[string]any{"role": "assistant", "content": "ok"})
	histOverMsgs = append(histOverMsgs, curMsg)
	claudeBlocks, err := resolveAndBuildPromptBlocks(context.Background(), fetcher, []map[string]any{curMsg}, true, histOverMsgs, MaxTranscriptChar, ModelOrDefault("claude-opus-5-5"))
	if err != nil {
		t.Fatalf("unexpected Claude rollover cap error: %v", err)
	}
	imgCnt := 0
	for _, b := range claudeBlocks {
		if b["type"] == "image" {
			imgCnt++
		}
	}
	if imgCnt != 100 {
		t.Fatalf("expected rollover to keep exactly 100 images (99 hist + 1 cur) for Claude, got %d", imgCnt)
	}
	resClaude, _ := claudeBlocks[0]["resource"].(map[string]any)["text"].(string)
	if !strings.Contains(resClaude, "[Earlier image omitted due to prompt image budget]") {
		t.Fatalf("expected omission marker for trimmed old images, got:\n%s", resClaude)
	}

	// 7. Finding 2 regression: Rollover with 2 consecutive trailing tool_result user messages keeps BOTH in currentMsgs strictly.
	trMsgs := []map[string]any{
		histMsg,
		asstMsg,
		{"role": "assistant", "content": []any{
			map[string]any{"type": "tool_use", "id": "t1", "name": "Snap", "input": map[string]any{}},
			map[string]any{"type": "tool_use", "id": "t2", "name": "Snap", "input": map[string]any{}},
		}},
		{"role": "user", "content": []any{map[string]any{
			"type": "tool_result", "tool_use_id": "t1",
			"content": []any{map[string]any{"type": "image_ref", "url": goneURL}},
		}}},
		{"role": "user", "content": []any{map[string]any{
			"type": "tool_result", "tool_use_id": "t2",
			"content": []any{map[string]any{"type": "text", "text": "done"}},
		}}},
	}
	if _, trErr := resolveAndBuildPromptBlocks(context.Background(), fetcher, trMsgs[3:], true, trMsgs, MaxTranscriptChar, ModelOrDefault("gemini-4-carbon")); trErr == nil {
		t.Fatalf("expected rollover with broken remote image in 1st parallel tool_result of current turn to fail strictly")
	}

	// 8. Finding 3 regression: Canceled context during rollover history fetch aborts instead of degrading.
	canceledCtx, cancelFn := context.WithCancel(context.Background())
	cancelFn()
	if _, cErr := resolveAndBuildPromptBlocks(canceledCtx, fetcher, []map[string]any{{"role": "user", "content": "hi"}}, true, rollAll, MaxTranscriptChar, ModelOrDefault("gemini-4-carbon")); cErr == nil {
		t.Fatalf("expected canceled ctx during rollover history fetch to return context error, got nil")
	}
}

func TestModuleBSSRFAndMagicSniffBlocking(t *testing.T) {
	f := newRemoteImageFetcher(nil)
	blocked := []string{
		"http://127.0.0.1/a.png",
		"http://[::1]/a.png",
		"http://localhost/a.png",
		"http://user:pass@cdn.example.com/a.png",
		"http://169.254.169.254/latest/meta-data",
		"http://cdn.example.com:8080/a.png",
		"http://[::ffff:127.0.0.1]/a.png",
		"gs://bucket/a.png",
		"https://generativelanguage.googleapis.com/v1beta/files/xyz",
		"ftp://cdn.example.com/a.png",
	}
	for _, u := range blocked {
		if _, err := f.Fetch(context.Background(), u, "image_url"); err == nil {
			t.Fatalf("expected SSRF/scheme block on %q", u)
		}
	}

	// Direct kernel dialer Control check for resolved private/loopback/mapped IPs & ports.
	badTargets := []string{
		"127.0.0.1:443",
		"[::1]:443",
		"169.254.169.254:80",
		"10.0.0.1:443",
		"192.168.1.1:80",
		"172.16.0.1:443",
		"[::ffff:127.0.0.1]:443",
		"[::ffff:10.0.0.1]:443",
		"93.184.216.34:8443",
	}
	for _, target := range badTargets {
		if err := safePublicSocketControl("tcp", target, nil); err == nil {
			t.Fatalf("expected safePublicSocketControl to block %s", target)
		}
	}
	if err := safePublicSocketControl("tcp", "93.184.216.34:443", nil); err != nil {
		t.Fatalf("expected public 93.184.216.34:443 allowed, got %v", err)
	}
	if !isPublicNetipAddr(netip.MustParseAddr("1.1.1.1")) || isPublicNetipAddr(netip.MustParseAddr("::ffff:127.0.0.1")) {
		t.Fatalf("unexpected isPublicNetipAddr classification")
	}
	// Finding 5 regression: IPv6 transition prefixes and zone IDs must be rejected.
	for _, nonPub := range []string{"64:ff9b::1", "64:ff9b:1::1", "2002::1", "2001::1", "100::1", "fec0::1", "fe80::1%eth0"} {
		if isPublicNetipAddr(netip.MustParseAddr(nonPub)) {
			t.Fatalf("expected non-public / zoned IPv6 %q blocked", nonPub)
		}
	}

	// HTTP redirect to loopback, spoofed image body, and >MaxImageBytes Content-Length.
	redF := newRemoteImageFetcher(nil)
	redF.client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/redir.png":
			return &http.Response{
				StatusCode: http.StatusFound,
				Header:     http.Header{"Location": []string{"http://127.0.0.1/secret.png"}},
				Body:       io.NopCloser(strings.NewReader("")),
				Request:    req,
			}, nil
		case "/html.png":
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"image/png"}},
				Body:       io.NopCloser(strings.NewReader("<html>not-image</html>")),
				Request:    req,
			}, nil
		default:
			return &http.Response{
				StatusCode:    http.StatusOK,
				ContentLength: int64(MaxImageBytes + 100),
				Header:        http.Header{"Content-Type": []string{"image/png"}},
				Body:          io.NopCloser(bytes.NewReader(make([]byte, 16))),
				Request:       req,
			}, nil
		}
	})
	if _, err := redF.Fetch(context.Background(), "https://cdn.example.com/redir.png", "image_url"); err == nil {
		t.Fatalf("expected redirect to loopback blocked")
	}
	if _, err := redF.Fetch(context.Background(), "https://cdn.example.com/html.png", "image_url"); err == nil {
		t.Fatalf("expected non-image payload claiming image/png rejected")
	}
	_, tooBigErr := redF.Fetch(context.Background(), fmt.Sprintf("https://cdn.example.com/%s.png", "huge"), "image_url")
	var mmErr *MultimodalValidationError
	if tooBigErr == nil || !errorAsMM(tooBigErr, &mmErr) || mmErr.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 for oversized Content-Length, got %v", tooBigErr)
	}
}
