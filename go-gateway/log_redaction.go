package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var bearerLogRE = regexp.MustCompile(`(?i)(Bearer\s+)[A-Za-z0-9._~+/-]+=*`)
var mcpPathLogRE = regexp.MustCompile(`/mcp/[^\s"'\\<>?#]+`)
var secretFieldLogRE = regexp.MustCompile(`(?i)("(?:tenant|tenantkey|authorization|x-api-key|x-goog-api-key|api_key|apikey|access_token|refresh_token)"\s*:\s*)"[^"\r\n]*"`)

func redactSecretsInString(s string) string {
	s = bearerLogRE.ReplaceAllString(s, `${1}[REDACTED]`)
	s = mcpPathLogRE.ReplaceAllString(s, `/mcp/[REDACTED]`)
	return secretFieldLogRE.ReplaceAllString(s, `${1}"[REDACTED]"`)
}

// Redact structured metadata before encoding, including name/value header pairs.
func redactLogValue(v any) any {
	switch x := v.(type) {
	case string:
		return redactSecretsInString(x)
	case map[string]any:
		out := make(map[string]any, len(x))
		header, _ := x["name"].(string)
		for k, val := range x {
			low := strings.ToLower(k)
			if isSecretLogKey(low) || (low == "value" && isSecretLogKey(strings.ToLower(header))) {
				out[k] = "[REDACTED]"
			} else {
				out[k] = redactLogValue(val)
			}
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, val := range x {
			out[i] = redactLogValue(val)
		}
		return out
	default:
		return v
	}
}

func isSecretLogKey(k string) bool {
	switch k {
	case "tenant", "tenantkey", "authorization", "x-api-key", "x-goog-api-key", "api_key", "apikey", "access_token", "refresh_token":
		return true
	}
	return false
}

func redactLogJSON(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var parsed any
	if err = json.Unmarshal(raw, &parsed); err != nil {
		return nil, err
	}
	return json.Marshal(redactLogValue(parsed))
}

func openPrivateLog(path string) (*os.File, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	// Never change permissions on shared parent directories such as /tmp.
	if dir != os.TempDir() && dir != "/" && dir != "." && dir != ProjectHome {
		if err := os.Chmod(dir, 0700); err != nil {
			return nil, err
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}
	if err = f.Chmod(0600); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}
