package main

import (
	"crypto/subtle"
	"encoding/base64"
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
	return out
}

func authEnabled() bool { return len(apiKeys) > 0 || authConfigErr != nil }

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
	if authConfigErr == nil && (!authEnabled() || keyMatches(requestKey(r))) {
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

// checkDashboardAuth guards the admin page and management APIs (master key only; restricted channel boundKeys are rejected).
func checkDashboardAuth(w http.ResponseWriter, r *http.Request) bool {
	if authConfigErr == nil && (!authEnabled() || rawMasterKeyMatches(requestKey(r))) {
		return true
	}
	w.Header().Set("WWW-Authenticate", `Basic realm="ACP Gateway", charset="UTF-8"`)
	w.WriteHeader(http.StatusUnauthorized)
	_, _ = w.Write([]byte("401 需要登录: 用户名任意, 密码填主 API Key"))
	return false
}
