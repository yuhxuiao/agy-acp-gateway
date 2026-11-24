package main

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"net/http"
	"os"
	"strconv"
	"strings"

	gwcfg "agy-acp-gateway-go/internal/config"
)

// API keys: AGY_API_KEYS (comma separated) and/or AGY_API_KEYS_FILE (one per line, # comments).
// No keys configured = auth disabled (local loopback use).
var authConfigErr error
var apiKeys = loadAPIKeys()

func formatPercent(v float64) string {
	return strconv.FormatFloat(v, 'f', 2, 64) + "%"
}

func loadAPIKeys() [][]byte {
	authConfigErr = nil
	snap := gwcfg.Current()
	var raw []string
	if v := snap.APIKeysRaw; v != "" {
		raw = append(raw, strings.Split(v, ",")...)
	}
	if f := snap.APIKeysFile; f != "" {
		b, err := os.ReadFile(f)
		if err != nil {
			authConfigErr = errors.New("configured API key file is unreadable")
		} else {
			valid := false
			for _, line := range strings.Split(string(b), "\n") {
				line = strings.TrimSpace(line)
				if line != "" && !strings.HasPrefix(line, "#") {
					valid = true
				}
			}
			if !valid {
				authConfigErr = errors.New("configured API key file contains no keys")
			}
			raw = append(raw, strings.Split(string(b), "\n")...)
		}
	}
	var out [][]byte
	for _, k := range raw {
		k = strings.TrimSpace(k)
		if k != "" && !strings.HasPrefix(k, "#") {
			out = append(out, []byte(k))
		}
	}
	dashboardCreds = loadDashboardCredentials()
	return out
}

// authEnabled reports whether legacy master API keys (AGY_API_KEYS / AGY_API_KEYS_FILE) are configured.
func authEnabled() bool { return len(apiKeys) > 0 || authConfigErr != nil }

// dataPlaneAuthEnabled reports whether :18890 requires a credential. Issuing any dashboard client key
// turns data-plane auth on even without master keys (disabled keys keep it on, so revoking never reopens it).
func dataPlaneAuthEnabled() bool {
	return authEnabled() || GlobalClientKeys.HasAny()
}

// --- Dashboard (admin plane) credentials ---
//
// AGY_DASHBOARD_PASSWORD        plaintext password (or "sha256:<64 hex>" digest)
// AGY_DASHBOARD_PASSWORD_FILE   file holding one password / "sha256:<hex>" digest per line (# comments)
//
// When any dashboard credential is configured, :18891 (WebUI, /api/*, /metrics, /health) accepts ONLY
// that credential: master API keys and client keys are rejected. When none is configured, the legacy
// behaviour is kept (master API key doubles as the dashboard password) for backward compatibility.

type dashboardCredentials struct {
	digests   [][]byte // sha256 digests of accepted passwords
	plaintext string   // first plaintext password (used to authenticate to federated peers)
	err       error
}

var dashboardCreds = loadDashboardCredentials()

func parseDashboardSecret(raw string) (digest []byte, plain string, ok bool, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || strings.HasPrefix(raw, "#") {
		return nil, "", false, nil
	}
	if len(raw) > 7 && strings.EqualFold(raw[:7], "sha256:") {
		d, decErr := hex.DecodeString(strings.TrimSpace(raw[7:]))
		if decErr != nil || len(d) != sha256.Size {
			return nil, "", false, errors.New("configured dashboard password digest is malformed")
		}
		return d, "", true, nil
	}
	sum := sha256.Sum256([]byte(raw))
	return sum[:], raw, true, nil
}

func loadDashboardCredentials() dashboardCredentials {
	snap := gwcfg.Current()
	var c dashboardCredentials
	add := func(raw string) {
		d, plain, ok, err := parseDashboardSecret(raw)
		if err != nil && c.err == nil {
			c.err = err
		}
		if ok {
			c.digests = append(c.digests, d)
			if c.plaintext == "" && plain != "" {
				c.plaintext = plain
			}
		}
	}
	if v := snap.DashboardPassword; strings.TrimSpace(v) != "" {
		add(v)
	}
	if f := snap.DashboardPasswordFile; f != "" {
		b, err := os.ReadFile(f)
		if err != nil {
			c.err = errors.New("configured dashboard password file is unreadable")
		} else {
			before := len(c.digests)
			for _, line := range strings.Split(string(b), "\n") {
				add(line)
			}
			if len(c.digests) == before && c.err == nil {
				c.err = errors.New("configured dashboard password file contains no passwords")
			}
		}
	}
	return c
}

// dashboardPasswordConfigured reports whether a dedicated admin credential is set (or failed to load).
func dashboardPasswordConfigured() bool {
	return len(dashboardCreds.digests) > 0 || dashboardCreds.err != nil
}

// dashboardAuthEnabled reports whether :18891 requires any credential.
func dashboardAuthEnabled() bool {
	return dashboardPasswordConfigured() || authEnabled()
}

// dashboardAuthMode describes the active admin-plane auth mode for the UI / docs (never secrets).
func dashboardAuthMode() string {
	switch {
	case dashboardPasswordConfigured():
		return "dashboard-password"
	case authEnabled():
		return "legacy-master-key"
	default:
		return "disabled"
	}
}

func dashboardPasswordMatches(candidate string) bool {
	if candidate == "" || dashboardCreds.err != nil {
		return false
	}
	sum := sha256.Sum256([]byte(candidate))
	ok := 0
	for _, d := range dashboardCreds.digests {
		ok |= subtle.ConstantTimeCompare(sum[:], d)
	}
	return ok == 1
}

// dashboardPeerCredential is the credential presented to federated peer dashboards.
func dashboardPeerCredential() string {
	return dashboardCreds.plaintext
}

func rawMasterKeyMatches(candidate string) bool {
	if candidate == "" {
		return false
	}
	c := []byte(candidate)
	ok := 0
	for _, k := range apiKeys {
		ok |= subtle.ConstantTimeCompare(c, k)
	}
	return ok == 1
}

func keyMatches(candidate string) bool {
	if rawMasterKeyMatches(candidate) {
		return true
	}
	if _, ok := GlobalClientKeys.Match(candidate); ok {
		return true
	}
	if _, ok := GlobalAccountManager.MatchesAccountKey(candidate); ok {
		return true
	}
	if _, ok := GlobalAccountManager.MatchesGroupKey(candidate); ok {
		return true
	}
	return false
}

// extractGeminiPathKey strips an optional trailing "/key=<secret>" segment on /v1beta/* routes.
func extractGeminiPathKey(path string) (cleanPath, pathKey string) {
	if !strings.HasPrefix(path, "/v1beta/") {
		return path, ""
	}
	idx := strings.LastIndex(path, "/key=")
	if idx < len("/v1beta") {
		return path, ""
	}
	tail := path[idx+len("/key="):]
	if tail == "" || strings.ContainsRune(tail, '/') {
		return path, ""
	}
	return path[:idx], strings.TrimSpace(tail)
}

// requestKey extracts a client key from Bearer / x-api-key / x-goog-api-key / Basic (password),
// plus ?key= and trailing /key= strictly scoped to /v1beta/* Gemini routes.
func requestKey(r *http.Request) string {
	if h := r.Header.Get("Authorization"); h != "" {
		if strings.HasPrefix(strings.ToLower(h), "bearer ") {
			return strings.TrimSpace(h[7:])
		}
		if strings.HasPrefix(strings.ToLower(h), "basic ") {
			if dec, err := base64.StdEncoding.DecodeString(strings.TrimSpace(h[6:])); err == nil {
				if i := strings.IndexByte(string(dec), ':'); i >= 0 {
					return string(dec[i+1:])
				}
			}
		}
	}
	if k := r.Header.Get("x-api-key"); k != "" {
		return k
	}
	if k := r.Header.Get("x-goog-api-key"); k != "" {
		return k
	}
	if r != nil && r.URL != nil && strings.HasPrefix(r.URL.Path, "/v1beta/") {
		if qk := strings.TrimSpace(r.URL.Query().Get("key")); qk != "" {
			return qk
		}
		if _, pk := extractGeminiPathKey(r.URL.Path); pk != "" {
			return pk
		}
	}
	return ""
}

// checkAPIAuth guards /v1/* and /v1beta/*. Returns false after writing a 401.
func checkAPIAuth(w http.ResponseWriter, r *http.Request) bool {
	if authConfigErr == nil && GlobalClientKeys.LoadError() == nil && (!dataPlaneAuthEnabled() || keyMatches(requestKey(r))) {
		return true
	}
	if r != nil && r.URL != nil && strings.HasPrefix(r.URL.Path, "/v1beta/") {
		writeGeminiError(w, http.StatusUnauthorized, "invalid or missing API key")
		return false
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	_, _ = w.Write([]byte(`{"type":"error","error":{"type":"authentication_error","message":"invalid or missing API key"}}`))
	return false
}

// checkDashboardAuth guards the admin page and management APIs on :18891.
//   - Dedicated AGY_DASHBOARD_PASSWORD configured: only that password is accepted.
//   - Otherwise (legacy): the raw master API key; restricted account/group boundKeys are rejected.
//   - Dashboard-issued client keys are ALWAYS rejected, even when the dashboard is otherwise open.
func checkDashboardAuth(w http.ResponseWriter, r *http.Request) bool {
	cand := requestKey(r)
	allowed := false
	if _, isClientKey := GlobalClientKeys.Lookup(cand); !isClientKey {
		switch {
		case dashboardPasswordConfigured():
			allowed = dashboardPasswordMatches(cand)
		case authConfigErr != nil:
			allowed = false
		case !authEnabled():
			allowed = true
		default:
			allowed = rawMasterKeyMatches(cand)
		}
	}
	if allowed {
		return true
	}
	w.Header().Set("WWW-Authenticate", `Basic realm="ACP Gateway", charset="UTF-8"`)
	w.WriteHeader(http.StatusUnauthorized)
	if dashboardPasswordConfigured() {
		_, _ = w.Write([]byte("401 需要登录: 用户名任意, 密码填管理后台密码 (AGY_DASHBOARD_PASSWORD)"))
	} else {
		_, _ = w.Write([]byte("401 需要登录: 用户名任意, 密码填主 API Key"))
	}
	return false
}
