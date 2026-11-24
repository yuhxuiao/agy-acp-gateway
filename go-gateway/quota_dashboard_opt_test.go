package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDashboardETag304RequiresAuthFirstAndHonorsRefreshBypass(t *testing.T) {
	t.Setenv("AGY_API_KEYS", "sk-etag-test-admin-secret")
	initConfig()
	t.Cleanup(initConfig)

	gs := NewGatewayServerWithDeps(newMockAcpEngine(), &mockSessionStore{})
	defer gs.Close()
	dashHandler := gs.DashboardOnlyHandler()

	// 1. Authenticated first request returns 200 OK with weak ETag, private Cache-Control, and Vary.
	req1 := httptest.NewRequest(http.MethodGet, "/api/dashboard?local_only=1", nil)
	req1.Header.Set("x-api-key", "sk-etag-test-admin-secret")
	rec1 := httptest.NewRecorder()
	dashHandler.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("expected 200 on first authenticated /api/dashboard, got %d", rec1.Code)
	}
	etag := rec1.Header().Get("ETag")
	if !strings.HasPrefix(etag, `W/"`) {
		t.Fatalf("expected weak ETag header, got %q", etag)
	}
	if cc := rec1.Header().Get("Cache-Control"); !strings.Contains(cc, "private") || !strings.Contains(cc, "no-cache") {
		t.Fatalf("expected private, no-cache Cache-Control, got %q", cc)
	}

	// 2. SECURITY: Unauthenticated request with matching If-None-Match MUST return 401 (NEVER 304!).
	reqUnauth := httptest.NewRequest(http.MethodGet, "/api/dashboard?local_only=1", nil)
	reqUnauth.Header.Set("If-None-Match", etag)
	recUnauth := httptest.NewRecorder()
	dashHandler.ServeHTTP(recUnauth, reqUnauth)
	if recUnauth.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated /api/dashboard with matching ETag must return 401, got %d", recUnauth.Code)
	}

	// 3. Authenticated conditional request with matching If-None-Match returns 304 Not Modified with empty body.
	req2 := httptest.NewRequest(http.MethodGet, "/api/dashboard?local_only=1", nil)
	req2.Header.Set("x-api-key", "sk-etag-test-admin-secret")
	req2.Header.Set("If-None-Match", etag)
	rec2 := httptest.NewRecorder()
	dashHandler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusNotModified {
		t.Fatalf("expected 304 Not Modified on unchanged /api/dashboard, got %d", rec2.Code)
	}
	if rec2.Body.Len() != 0 {
		t.Fatalf("expected empty body on 304 Not Modified, got %d bytes", rec2.Body.Len())
	}

	// 4. Force refresh (?refresh=1) bypasses 304 even when If-None-Match matches.
	req3 := httptest.NewRequest(http.MethodGet, "/api/dashboard?local_only=1&refresh=1", nil)
	req3.Header.Set("x-api-key", "sk-etag-test-admin-secret")
	req3.Header.Set("If-None-Match", etag)
	rec3 := httptest.NewRecorder()
	dashHandler.ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusOK || rec3.Body.Len() == 0 {
		t.Fatalf("expected 200 OK with body on refresh=1, got code=%d len=%d", rec3.Code, rec3.Body.Len())
	}
}

func TestAgyCliProbeSemaphoreCapsConcurrencyAndHonorsContextCancel(t *testing.T) {
	ctx := context.Background()
	rel1, ok1 := acquireAgyCliProbeSlot(ctx)
	if !ok1 {
		t.Fatal("expected slot 1 acquired")
	}
	defer rel1()

	rel2, ok2 := acquireAgyCliProbeSlot(ctx)
	if !ok2 {
		t.Fatal("expected slot 2 acquired")
	}
	defer rel2()

	// 3rd concurrent caller must block until context expires while 2 slots are held.
	cCtx, cancel := context.WithTimeout(ctx, 40*time.Millisecond)
	defer cancel()
	rel3, ok3 := acquireAgyCliProbeSlot(cCtx)
	if ok3 {
		rel3()
		t.Fatal("expected 3rd concurrent acquireAgyCliProbeSlot to block and time out when cap=2")
	}

	// Releasing 1 slot allows immediate acquisition.
	rel1()
	rel4, ok4 := acquireAgyCliProbeSlot(ctx)
	if !ok4 {
		t.Fatal("expected slot acquisition after release")
	}
	rel4()
}

func TestRecordAccountOutcome429ThrottlesQuotaSyncTimestamp(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("AGY_GW_HOME", tmp)
	t.Setenv("AGY_ACCOUNTS_FILE", filepath.Join(tmp, "accounts.json"))
	initConfig()
	t.Cleanup(initConfig)

	am := NewAccountManager()
	am.Init()
	am.RecordAccountOutcome("primary", "gemini-4-carbon", http.StatusTooManyRequests, "rate_limit_error", "30", "quota exhausted")

	am.healthMu.RLock()
	st := am.healthStates["primary"]
	firstSyncAt := st.lastQuota429SyncAt
	am.healthMu.RUnlock()
	if firstSyncAt.IsZero() {
		t.Fatal("expected first 429 outcome to stamp lastQuota429SyncAt")
	}

	// Immediate second 429 within 45s cooldown must NOT overwrite lastQuota429SyncAt.
	time.Sleep(5 * time.Millisecond)
	am.RecordAccountOutcome("primary", "gemini-4-carbon", http.StatusTooManyRequests, "rate_limit_error", "30", "quota exhausted again")
	am.healthMu.RLock()
	secondSyncAt := am.healthStates["primary"].lastQuota429SyncAt
	am.healthMu.RUnlock()
	if !secondSyncAt.Equal(firstSyncAt) {
		t.Fatalf("expected 429 quota refresh cooldown to preserve firstSyncAt=%v, got %v", firstSyncAt, secondSyncAt)
	}
}

func TestDashboardHTMLContainsAdaptivePollingAndClientCopyTemplates(t *testing.T) {
	html := RenderDashboardHTML()
	for _, needle := range []string{
		"If-None-Match",
		"r.status === 304",
		"scheduleNextPoll",
		"data-copy-client-tpl=\"anthropic\"",
		"data-copy-client-tpl=\"openai\"",
		"data-copy-client-tpl=\"gemini\"",
		"<YOUR_API_KEY>",
	} {
		if !strings.Contains(html, needle) {
			t.Fatalf("expected dashboard HTML to contain %q", needle)
		}
	}
}
