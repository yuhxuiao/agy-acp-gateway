package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func saveAndIsolateQuotaGlobals(t *testing.T) {
	t.Helper()
	waitQuotaRefreshIdle(t)
	quotaMu.Lock()
	savedQ, savedQAt, savedQErr, savedQAttempt, savedQBackoff := cachedQuota, cachedQuotaAt, lastQuotaErr, lastQuotaAttemptAt, quotaBackoffSteps
	cachedQuota = nil
	cachedQuotaAt = time.Time{}
	lastQuotaErr = nil
	lastQuotaAttemptAt = time.Time{}
	quotaBackoffSteps = 0
	quotaMu.Unlock()
	t.Cleanup(func() {
		waitQuotaRefreshIdle(t)
		quotaMu.Lock()
		cachedQuota, cachedQuotaAt, lastQuotaErr, lastQuotaAttemptAt, quotaBackoffSteps = savedQ, savedQAt, savedQErr, savedQAttempt, savedQBackoff
		quotaMu.Unlock()
	})
}

func TestQuotaAuthPromptMessagesClassifyCorrectly(t *testing.T) {
	if isToSOrBanError(0, "", quotaAuthPromptMsg) {
		t.Fatalf("login prompt must not be classified as ToS ban: %q", quotaAuthPromptMsg)
	}
	if !isAuthErrorMessage(quotaAuthPromptMsg) {
		t.Fatalf("login prompt must stay an auth error: %q", quotaAuthPromptMsg)
	}
	if isToSOrBanError(0, "", quotaNetworkUnavailableMsg) || isAuthErrorMessage(quotaNetworkUnavailableMsg) {
		t.Fatalf("network failure must be neither ban nor auth error: %q", quotaNetworkUnavailableMsg)
	}
	// Resin / loadCodeAssist transport failures seen in production must not trip accounts either.
	for _, msg := range []string{
		`exit status 1: error: Eligibility check failed: Post "https://daily-cloudcode-pa.googleapis.com/v1internal:loadCodeAssist": Service Unavailable`,
	} {
		if isToSOrBanError(0, "", msg) || isAuthErrorMessage(msg) {
			t.Fatalf("transport failure misclassified: %q", msg)
		}
	}
}

func TestClassifyQuotaAuthPromptUsesNetworkProbe(t *testing.T) {
	waitQuotaRefreshIdle(t)
	prev := quotaNetworkProbe
	t.Cleanup(func() {
		waitQuotaRefreshIdle(t)
		quotaNetworkProbe = prev
	})

	quotaNetworkProbe = func([]string) error { return errors.New("proxyconnect: 503 Service Unavailable") }
	if got := classifyQuotaAuthPrompt(nil); got != quotaNetworkUnavailableMsg {
		t.Fatalf("unreachable egress must yield network message, got %q", got)
	}
	quotaNetworkProbe = func([]string) error { return nil }
	if got := classifyQuotaAuthPrompt(nil); got != quotaAuthPromptMsg {
		t.Fatalf("reachable egress must yield auth message, got %q", got)
	}
}

func TestProbeEgressReachable(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound) // 4xx means reachable
	}))
	defer target.Close()
	if err := probeEgressReachable(nil, target.URL); err != nil {
		t.Fatalf("direct reachable target must succeed: %v", err)
	}

	// HTTP >= 500 (e.g. Google outage) must be treated as unavailable, NOT expired credentials.
	err503Target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer err503Target.Close()
	if err := probeEgressReachable(nil, err503Target.URL); err == nil {
		t.Fatal("HTTP 503 egress target must be reported as unavailable")
	}

	// Redirects must NOT be followed (CheckRedirect returns http.ErrUseLastResponse).
	redirectHits := 0
	var redirSrv *httptest.Server
	redirSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirectHits++
		if r.URL.Path == "/" {
			http.Redirect(w, r, redirSrv.URL+"/dest", http.StatusFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer redirSrv.Close()
	if err := probeEgressReachable(nil, redirSrv.URL+"/"); err != nil {
		t.Fatalf("302 response itself should stop without error: %v", err)
	}
	if redirectHits != 1 {
		t.Fatalf("probe must not follow redirects, got %d requests", redirectHits)
	}

	// Proxy that refuses every request like Resin with NO_AVAILABLE_NODES.
	deadProxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Resin-Error", "NO_AVAILABLE_NODES")
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer deadProxy.Close()
	env := []string{"HTTPS_PROXY=" + strings.Replace(deadProxy.URL, "http://", "http://u:secret@", 1)}
	err := probeEgressReachable(env, "https://oauth2.googleapis.com/")
	if err == nil {
		t.Fatal("CONNECT refused by proxy must be reported as unreachable")
	}
	if strings.Contains(redactProbeErr(err), "secret") {
		t.Fatalf("probe error must not leak proxy credentials: %v", redactProbeErr(err))
	}

	// Scheme-less host:port proxy must be normalized to http://host:port (not rejected as invalid proxy url).
	bareHostPort := strings.TrimPrefix(deadProxy.URL, "http://")
	bareErr := probeEgressReachable([]string{"HTTPS_PROXY=" + bareHostPort}, "https://oauth2.googleapis.com/")
	if bareErr == nil || strings.Contains(bareErr.Error(), "invalid proxy url") {
		t.Fatalf("scheme-less host:port proxy must normalize to http:// and attempt CONNECT, got %v", bareErr)
	}
}

func TestRedactProbeErrMultiURLAndRunes(t *testing.T) {
	// Earlier '@' before proxy URL must not prevent redaction of proxy credentials.
	rawErr := errors.New(`user@host Get "https://oauth2.googleapis.com/?token=qsecret": proxyconnect tcp: http://user:topsecret@resin:2260: 503`)
	got := redactProbeErr(rawErr)
	for _, secret := range []string{"topsecret", "qsecret"} {
		if strings.Contains(got, secret) {
			t.Fatalf("redactProbeErr leaked %q: %q", secret, got)
		}
	}
	// Multi-byte UTF-8 truncation must remain valid UTF-8.
	longUTF8 := errors.New(strings.Repeat("中", 250))
	trunc := redactProbeErr(longUTF8)
	if len([]rune(trunc)) > 200 {
		t.Fatalf("expected rune-bounded <=200, got %d runes", len([]rune(trunc)))
	}
}

func TestFetchOfficialQuotaLoginPromptWithDeadNetworkIsNotAuthError(t *testing.T) {
	saveAndIsolateQuotaGlobals(t)
	prev := quotaNetworkProbe
	t.Cleanup(func() {
		waitQuotaRefreshIdle(t)
		quotaNetworkProbe = prev
	})
	quotaNetworkProbe = func([]string) error { return errors.New("unreachable") }

	dir := t.TempDir()
	bin := filepath.Join(dir, "fake-agy")
	script := "#!/bin/sh\necho 'Authentication required. Please visit the URL to log in:'\nexit 1\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	snap := fetchOfficialQuotaDoWith(bin, os.Environ())
	if snap.Error != quotaNetworkUnavailableMsg && !strings.Contains(snap.Error, "Network unavailable") {
		t.Fatalf("expected network-unavailable classification, got %+v", snap)
	}
}

func TestFetchOfficialQuotaExitZeroAuthPromptRoutesThroughNetworkProbe(t *testing.T) {
	saveAndIsolateQuotaGlobals(t)
	prev := quotaNetworkProbe
	t.Cleanup(func() {
		waitQuotaRefreshIdle(t)
		quotaNetworkProbe = prev
	})
	quotaNetworkProbe = func([]string) error { return errors.New("proxyconnect 503") }

	dir := t.TempDir()
	bin := filepath.Join(dir, "fake-agy-exit0")
	script := "#!/bin/sh\necho 'Authentication required. Please visit the URL to log in:'\nexit 0\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	snap := fetchOfficialQuotaDoWith(bin, os.Environ())
	if snap.Error != quotaNetworkUnavailableMsg {
		t.Fatalf("exit-0 non-JSON auth prompt with dead network must yield network unavailable, got %+v", snap)
	}
}

func TestResolvePrimaryUpstreamProxyUsesCustomPrimaryID(t *testing.T) {
	prevAM := GlobalAccountManager
	t.Cleanup(func() {
		GlobalAccountManager = prevAM
		initConfig()
	})
	t.Setenv("AGY_UPSTREAM_PROXY", "")
	initConfig()

	GlobalAccountManager = &AccountManager{
		accounts: []AccountProfile{
			{ID: "tr20x", Label: "TR20X", Primary: true, Enabled: true, ProxyPreset: "custom", CustomProxy: "http://proxy.local:8080"},
		},
		workers: make(map[string]*accountWorkerProc),
	}
	if got := resolvePrimaryUpstreamProxy(); got != "http://proxy.local:8080" {
		t.Fatalf("expected custom primary proxy http://proxy.local:8080, got %q", got)
	}
}

func TestHealthCheckFailureClassificationAndPeerQuotaDedup(t *testing.T) {
	waitQuotaRefreshIdle(t)
	prevProbe := quotaNetworkProbe
	resetPeerDashboardCacheForTest()
	t.Cleanup(func() {
		waitQuotaRefreshIdle(t)
		quotaNetworkProbe = prevProbe
		resetPeerDashboardCacheForTest()
	})

	// 1. Proxy 503 failure in health_check must map to 502 probe_error (NOT 401 auth_error!).
	sc, et, _ := classifyHealthCheckFailure("proxyconnect tcp: 503 Service Unavailable", "exit status 1", "http://u:pw@resin:2260", nil)
	if sc != http.StatusBadGateway || et != "probe_error" {
		t.Fatalf("expected 502 probe_error for proxy failure, got status=%d type=%q", sc, et)
	}

	// 2. Interactive auth prompt with dead egress must map to 502 probe_error, NOT 401 auth_error.
	quotaNetworkProbe = func([]string) error { return errors.New("proxy down") }
	sc2, et2, msg2 := classifyHealthCheckFailure("Authentication required. Please visit the URL to log in:", "exit status 1", "", nil)
	if sc2 != http.StatusBadGateway || et2 != "probe_error" || msg2 != quotaNetworkUnavailableMsg {
		t.Fatalf("dead-egress auth prompt must not hard-block auth_error: status=%d type=%q msg=%q", sc2, et2, msg2)
	}

	// 3. Interactive auth prompt with reachable egress maps to 401 authentication_error.
	quotaNetworkProbe = func([]string) error { return nil }
	sc3, et3, msg3 := classifyHealthCheckFailure("Authentication required. Please visit the URL to log in:", "exit status 1", "", nil)
	if sc3 != http.StatusUnauthorized || et3 != "authentication_error" || msg3 != quotaAuthPromptMsg {
		t.Fatalf("reachable-egress auth prompt must be 401 auth_error: status=%d type=%q msg=%q", sc3, et3, msg3)
	}

	// 4. Federated stale Quota.Error deduplication prevents clear_cooldown from being undone 5s later.
	qErr1 := QuotaSnapshot{Status: "stale", Error: quotaAuthPromptMsg, LastErrorAt: "2026-11-26T07:00:00Z"}
	if !shouldRecordPeerQuotaError("acc4", qErr1) {
		t.Fatal("first sighting of peer quota error must be recorded")
	}
	if shouldRecordPeerQuotaError("acc4", qErr1) {
		t.Fatal("duplicate stale peer quota error with same LastErrorAt must be suppressed")
	}
	// New failure timestamp must be recorded again.
	qErr2 := QuotaSnapshot{Status: "stale", Error: quotaAuthPromptMsg, LastErrorAt: "2026-11-26T07:15:00Z"}
	if !shouldRecordPeerQuotaError("acc4", qErr2) {
		t.Fatal("newer LastErrorAt must be recorded")
	}
	// Successful sync clears dedup memory and auth_error state.
	clearPeerQuotaErrorSeen("acc4")
	if !shouldRecordPeerQuotaError("acc4", qErr2) {
		t.Fatal("after clearPeerQuotaErrorSeen, subsequent error must be recordable again")
	}
}
