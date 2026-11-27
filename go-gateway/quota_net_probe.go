package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	gwcfg "agy-acp-gateway-go/internal/config"
)

// Quota probe network classification.
//
// When the account's egress proxy (e.g. Resin with no routable node) or Google's OAuth endpoint is
// unreachable, agy cannot refresh the saved token, falls back to its interactive OAuth flow and prints
// "Authentication required ...". Treating that as an auth failure (or worse, a ToS ban) evicts a
// perfectly valid account from routing. Before blaming credentials, the gateway checks whether the
// account's own egress path can reach Google at all.

const (
	quotaNetProbeURL     = "https://oauth2.googleapis.com/"
	quotaNetProbeTimeout = 4 * time.Second
	// Deliberately free of auth/ban keywords so RecordQuotaSyncOutcome neither trips nor cools the account.
	quotaNetworkUnavailableMsg = "Network unavailable via account egress proxy (Google endpoint unreachable); credentials not verified"
	quotaAuthPromptMsg         = "Authentication required (login expired or revoked)"
)

// quotaNetworkProbe returns nil when the egress path described by cmdEnv reaches Google (non-5xx HTTP
// response without redirects), or an error when the proxy/upstream is unreachable. Overridable in tests.
var quotaNetworkProbe = defaultQuotaNetworkProbe

func defaultQuotaNetworkProbe(cmdEnv []string) error {
	if testing.Testing() {
		return nil
	}
	return probeEgressReachable(cmdEnv, quotaNetProbeURL)
}

func probeEgressReachable(cmdEnv []string, target string) error {
	return probeEgressReachableWithTimeout(cmdEnv, target, quotaNetProbeTimeout)
}

func probeEgressReachableWithTimeout(cmdEnv []string, target string, timeout time.Duration) error {
	if timeout <= 0 || timeout > quotaNetProbeTimeout {
		timeout = quotaNetProbeTimeout
	}
	proxyRaw := envLookup(cmdEnv, "HTTPS_PROXY", "https_proxy", "ALL_PROXY", "all_proxy")
	tr := &http.Transport{
		Proxy:                 nil,
		TLSHandshakeTimeout:   timeout,
		ResponseHeaderTimeout: timeout,
		DisableKeepAlives:     true,
	}
	defer tr.CloseIdleConnections()
	if proxyRaw != "" {
		if !strings.Contains(proxyRaw, "://") {
			proxyRaw = "http://" + proxyRaw
		}
		pu, err := url.Parse(proxyRaw)
		if err != nil || pu.Host == "" {
			return errors.New("invalid proxy url")
		}
		tr.Proxy = http.ProxyURL(pu)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return err
	}
	cli := &http.Client{
		Transport: tr,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	resp, err := cli.Do(req)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	if resp.StatusCode >= http.StatusInternalServerError {
		return fmt.Errorf("egress target returned HTTP %d", resp.StatusCode)
	}
	return nil
}

func envLookup(env []string, keys ...string) string {
	for _, k := range keys {
		prefix := k + "="
		for i := len(env) - 1; i >= 0; i-- {
			if strings.HasPrefix(env[i], prefix) {
				if v := strings.TrimSpace(strings.TrimPrefix(env[i], prefix)); v != "" {
					return v
				}
				break
			}
		}
	}
	return ""
}

// classifyQuotaAuthPrompt decides what an interactive-login prompt from `/usage` really means.
func classifyQuotaAuthPrompt(cmdEnv []string) string {
	return classifyQuotaAuthPromptWithTimeout(cmdEnv, quotaNetProbeTimeout)
}

func classifyQuotaAuthPromptWithTimeout(cmdEnv []string, timeout time.Duration) string {
	var err error
	if testing.Testing() {
		err = quotaNetworkProbe(cmdEnv)
	} else {
		// Honor custom timeout budget (so 15s /usage + probe fits comfortably inside 18s federated refresh).
		err = probeEgressReachableWithTimeout(cmdEnv, quotaNetProbeURL, timeout)
	}
	if err != nil {
		LogEvent("QUOTA_NETWORK_UNAVAILABLE", "agy /usage asked for login but the egress path cannot reach Google; not treating as auth failure", map[string]any{
			"error": strings.TrimSpace(redactProbeErr(err)),
		})
		return quotaNetworkUnavailableMsg
	}
	return quotaAuthPromptMsg
}

func redactProbeErr(err error) string {
	if err == nil {
		return ""
	}
	return gwcfg.TruncateRunes(gwcfg.RedactURLSecretsForError(err.Error()), 200)
}
