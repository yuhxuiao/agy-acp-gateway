package main

import (
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"net/http"
	"os"
	"strconv"
	"strings"
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
	var raw []string
	if v := os.Getenv("AGY_API_KEYS"); v != "" {
		raw = append(raw, strings.Split(v, ",")...)
	}
	if f := os.Getenv("AGY_API_KEYS_FILE"); f != "" {
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

func keyMatches(candidate string) bool {
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

// requestKey extracts a client key from Bearer / x-api-key / x-goog-api-key / Basic (password).
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
	return r.Header.Get("x-goog-api-key")
}

// checkAPIAuth guards /v1/*. Returns false after writing a 401.
func checkAPIAuth(w http.ResponseWriter, r *http.Request) bool {
	if authConfigErr == nil && (!authEnabled() || keyMatches(requestKey(r))) {
		return true
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	_, _ = w.Write([]byte(`{"type":"error","error":{"type":"authentication_error","message":"invalid or missing API key"}}`))
	return false
}

// checkDashboardAuth guards the admin page (browser Basic auth: any user name, password = API key).
func checkDashboardAuth(w http.ResponseWriter, r *http.Request) bool {
	if authConfigErr == nil && (!authEnabled() || keyMatches(requestKey(r))) {
		return true
	}
	w.Header().Set("WWW-Authenticate", `Basic realm="ACP Gateway", charset="UTF-8"`)
	w.WriteHeader(http.StatusUnauthorized)
	_, _ = w.Write([]byte("401 需要登录: 用户名任意, 密码填 API Key"))
	return false
}
