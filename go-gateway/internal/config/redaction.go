package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var (
	authSchemeLogRE        = regexp.MustCompile(`(?i)((?:Bearer|Basic)\s+)[A-Za-z0-9._~+/-]+=*`)
	mcpPathLogRE           = regexp.MustCompile(`/mcp/[^\s"'\\<>?#]+`)
	queryKeyLogRE          = regexp.MustCompile(`(?i)([?&](?:key|api_key|apikey|access_token|refresh_token|token|sig|signature|x-amz-signature|x-goog-signature)=)[^&\s"'\\<>#]+`)
	geminiMidPathKeyLogRE  = regexp.MustCompile(`(?i)^(/key=)[^/\s"'\\<>?#]+(:(?:generateContent|streamGenerateContent|countTokens))$`)
	geminiTailPathKeyLogRE = regexp.MustCompile(`(?i)(/key=)[^/\s"'\\<>?#]+`)
	secretFieldLogRE       = regexp.MustCompile(`(?i)("(?:tenant|tenantkey|authorization|x-api-key|x-goog-api-key|api_key|apikey|access_token|refresh_token)"\s*:\s*)"[^"\r\n]*"`)
	anyURLQueryValRE       = regexp.MustCompile(`([?&][^=&#\s"'\\<>]+)=[^&#\s"'\\<>]+`)
	urlUserInfoLogRE       = regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9+.-]*://)[^/\s"'\\<>@]+@`)
	clientKeyLogRE         = regexp.MustCompile(`agy-(?:ck|child)-[A-Za-z0-9_-]{8,}`)
)

func hasAuthSchemeHint(s string) bool {
	if len(s) < 6 {
		return false
	}
	if strings.IndexByte(s, 'b') < 0 && strings.IndexByte(s, 'B') < 0 {
		return false
	}
	return strings.ContainsAny(s, " \t\n\r\v\f")
}

// RedactSecretsInString masks bearer tokens, MCP paths, API keys, and credential query parameters in log strings.
func RedactSecretsInString(s string) string {
	if s == "" {
		return s
	}
	if strings.Contains(s, "agy-ck-") || strings.Contains(s, "agy-child-") {
		s = clientKeyLogRE.ReplaceAllString(s, "agy-ck-[REDACTED]")
	}
	if hasAuthSchemeHint(s) {
		s = authSchemeLogRE.ReplaceAllString(s, `${1}[REDACTED]`)
	}
	if strings.Contains(s, "://") && strings.IndexByte(s, '@') >= 0 {
		s = urlUserInfoLogRE.ReplaceAllString(s, `${1}[REDACTED]@`)
	}
	if strings.Contains(s, "/mcp/") {
		s = mcpPathLogRE.ReplaceAllString(s, `/mcp/[REDACTED]`)
	}
	if strings.IndexByte(s, '=') >= 0 && (strings.IndexByte(s, '?') >= 0 || strings.IndexByte(s, '&') >= 0) {
		s = queryKeyLogRE.ReplaceAllString(s, `${1}[REDACTED]`)
	}
	if strings.IndexByte(s, '/') >= 0 && strings.IndexByte(s, '=') >= 0 {
		s = geminiTailPathKeyLogRE.ReplaceAllStringFunc(s, func(m string) string {
			if geminiMidPathKeyLogRE.MatchString(m) {
				return geminiMidPathKeyLogRE.ReplaceAllString(m, `${1}[REDACTED]${2}`)
			}
			return m[:5] + "[REDACTED]"
		})
	}
	if strings.IndexByte(s, '"') >= 0 && strings.IndexByte(s, ':') >= 0 {
		s = secretFieldLogRE.ReplaceAllString(s, `${1}"[REDACTED]"`)
	}
	return s
}

// RedactURLSecretsForError scrubs all query parameter values in addition to known secret patterns.
func RedactURLSecretsForError(s string) string {
	s = RedactSecretsInString(s)
	if strings.IndexByte(s, '=') >= 0 && (strings.IndexByte(s, '?') >= 0 || strings.IndexByte(s, '&') >= 0) {
		return anyURLQueryValRE.ReplaceAllString(s, `${1}=[REDACTED]`)
	}
	return s
}

// RedactLogValue redacts secret fields and string values recursively before log encoding.
func RedactLogValue(v any) any {
	switch x := v.(type) {
	case string:
		return RedactSecretsInString(x)
	case map[string]any:
		out := make(map[string]any, len(x))
		header, _ := x["name"].(string)
		for k, val := range x {
			low := strings.ToLower(k)
			if isSecretLogKey(low) || (low == "value" && isSecretLogKey(strings.ToLower(header))) {
				out[k] = "[REDACTED]"
			} else {
				out[k] = RedactLogValue(val)
			}
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, val := range x {
			out[i] = RedactLogValue(val)
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

// UnmarshalJSONPreserveNumbers decodes single-value JSON while preserving integers/decimals as json.Number.
func UnmarshalJSONPreserveNumbers(data []byte, dst any) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return errors.New("unexpected end of JSON input")
	}
	dec := json.NewDecoder(bytes.NewReader(trimmed))
	dec.UseNumber()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return errors.New("invalid trailing characters after JSON value")
	}
	return nil
}

// RedactLogJSON marshals v while preserving exact JSON numbers and masking credential fields.
func RedactLogJSON(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var parsed any
	if err = UnmarshalJSONPreserveNumbers(raw, &parsed); err != nil {
		return nil, err
	}
	return json.Marshal(RedactLogValue(parsed))
}

// OpenPrivateLog opens or creates a private log file with 0600 permissions inside a 0700 parent dir.
func OpenPrivateLog(path, projectHome string) (*os.File, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	// Never change permissions on shared parent directories such as /tmp.
	if dir != os.TempDir() && dir != "/" && dir != "." && (projectHome == "" || dir != projectHome) {
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
