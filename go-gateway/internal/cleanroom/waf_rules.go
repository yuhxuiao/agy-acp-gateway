package cleanroom

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	gwcfg "agy-acp-gateway-go/internal/config"
)

// WafRewriteRule represents a learned literal rewrite rule persisted for instant 429 healing.
type WafRewriteRule struct {
	ID          string `json:"id"`
	Literal     string `json:"literal"`
	Replacement string `json:"replacement"`
	Source      string `json:"source"`
	CreatedAt   string `json:"createdAt"`
}

// WafRuleFilePayload is the JSON envelope stored in waf_rewrite_rules.json.
type WafRuleFilePayload struct {
	Version int              `json:"version"`
	Rules   []WafRewriteRule `json:"rules"`
}

var workspaceContextSpanRe = regexp.MustCompile(`(?s)<workspace-context>.*?</workspace-context>`)

// StripOrphanBulletLines drops empty bullet markers left behind after inline rule deletion.
func StripOrphanBulletLines(s string) string {
	lines := strings.Split(s, "\n")
	out := make([]string, 0, len(lines))
	for _, ln := range lines {
		trimmed := strings.TrimSpace(ln)
		if trimmed == "-" || trimmed == "*" {
			continue
		}
		out = append(out, ln)
	}
	return strings.Join(out, "\n")
}

// ApplyRules applies literal WAF rewrite rules to s, cleaning up orphan bullets and extra blank lines.
func ApplyRules(s string, rules []WafRewriteRule) string {
	if s == "" || len(rules) == 0 {
		return s
	}
	out := s
	changed := false
	for _, r := range rules {
		if r.Literal != "" && strings.Contains(out, r.Literal) {
			out = strings.ReplaceAll(out, r.Literal, r.Replacement)
			changed = true
		}
	}
	if !changed {
		return s
	}
	out = StripOrphanBulletLines(out)
	return multiBlankLinesRe.ReplaceAllString(strings.TrimSpace(out), "\n\n")
}

// ApplyRulesInWorkspaceContext applies literal WAF rewrite rules only inside <workspace-context> spans.
func ApplyRulesInWorkspaceContext(s string, rules []WafRewriteRule) string {
	if s == "" || len(rules) == 0 || !strings.Contains(s, "<workspace-context>") {
		return s
	}
	return workspaceContextSpanRe.ReplaceAllStringFunc(s, func(span string) string {
		inner := strings.TrimPrefix(span, "<workspace-context>")
		inner = strings.TrimSuffix(inner, "</workspace-context>")
		changed := false
		for _, r := range rules {
			if r.Literal != "" && strings.Contains(inner, r.Literal) {
				inner = strings.ReplaceAll(inner, r.Literal, r.Replacement)
				changed = true
			}
		}
		if !changed {
			return span
		}
		inner = StripOrphanBulletLines(inner)
		trimmed := strings.TrimSpace(multiBlankLinesRe.ReplaceAllString(inner, "\n\n"))
		if trimmed == "" {
			return ""
		}
		return "<workspace-context>\n" + trimmed + "\n</workspace-context>"
	})
}

// ExtractWorkspaceContextContents extracts the inner bodies of <workspace-context> spans.
func ExtractWorkspaceContextContents(txt string) []string {
	if txt == "" || !strings.Contains(txt, "<workspace-context>") {
		return nil
	}
	spans := workspaceContextSpanRe.FindAllString(txt, -1)
	out := make([]string, 0, len(spans))
	for _, span := range spans {
		inner := strings.TrimSuffix(strings.TrimPrefix(span, "<workspace-context>"), "</workspace-context>")
		out = append(out, inner)
	}
	return out
}

// SplitCandidateSentences splits text by line and sentence boundaries into deduplicated >=12-byte candidates.
func SplitCandidateSentences(text string) []string {
	var out []string
	seen := make(map[string]bool)
	for _, rawLine := range strings.Split(strings.ReplaceAll(text, "\r\n", "\n"), "\n") {
		line := strings.TrimSpace(rawLine)
		if line == "" {
			continue
		}
		if bm := leadingMarkdownBulletRe.FindStringSubmatch(line); len(bm) == 3 {
			line = strings.TrimSpace(bm[2])
		}
		start := 0
		for _, loc := range sentenceBoundaryRe.FindAllStringIndex(line, -1) {
			part := strings.TrimSpace(line[start:loc[1]])
			if len(part) >= 12 && !seen[part] {
				seen[part] = true
				out = append(out, part)
			}
			start = loc[1]
		}
		tail := strings.TrimSpace(line[start:])
		if len(tail) >= 12 && !seen[tail] {
			seen[tail] = true
			out = append(out, tail)
		}
	}
	return out
}

// ParseRuleFile decodes and validates a waf_rewrite_rules.json payload.
func ParseRuleFile(raw []byte) ([]WafRewriteRule, error) {
	var parsed WafRuleFilePayload
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, err
	}
	var valid []WafRewriteRule
	for _, r := range parsed.Rules {
		lit := strings.TrimSpace(r.Literal)
		if len(lit) >= 12 {
			r.Literal = lit
			valid = append(valid, r)
		}
	}
	return valid, nil
}

// PersistRuleFile appends a learned literal rule to target atomically with 0600 permissions.
func PersistRuleFile(target, literal, source string, existing []WafRewriteRule) error {
	lit := strings.TrimSpace(gwcfg.RedactSecretsInString(literal))
	if len(lit) < 12 {
		return errors.New("waf rule literal too short")
	}
	if target == "" || gwcfg.IsWindowsCDrivePath(target) {
		return errors.New("waf rules target path unavailable or forbidden")
	}
	for _, r := range existing {
		if r.Literal == lit {
			return nil
		}
	}
	updated := append([]WafRewriteRule(nil), existing...)
	updated = append(updated, WafRewriteRule{
		ID:          "waf_" + gwcfg.ShortSHA256(lit)[:12],
		Literal:     lit,
		Replacement: "",
		Source:      source,
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
	})
	if len(updated) > 256 {
		updated = updated[len(updated)-256:]
	}
	if err := os.MkdirAll(filepath.Dir(target), 0700); err != nil {
		return err
	}
	payloadBytes, err := json.MarshalIndent(WafRuleFilePayload{
		Version: 1,
		Rules:   updated,
	}, "", "  ")
	if err != nil {
		return err
	}
	payloadBytes = append(payloadBytes, '\n')
	f, err := os.CreateTemp(filepath.Dir(target), ".waf-rules-*")
	if err != nil {
		return err
	}
	tmpName := f.Name()
	_, wErr := f.Write(payloadBytes)
	if wErr == nil {
		wErr = f.Chmod(0600)
	}
	if wErr == nil {
		wErr = f.Sync()
	}
	cErr := f.Close()
	if wErr == nil {
		wErr = cErr
	}
	if wErr != nil {
		_ = os.Remove(tmpName)
		return wErr
	}
	if err := os.Rename(tmpName, target); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return nil
}

// WafRuleCache caches parsed WAF rewrite rules keyed by file mtime+size.
type WafRuleCache struct {
	mu      sync.RWMutex
	path    string
	mtimeNs int64
	size    int64
	rules   []WafRewriteRule
}

// Reset clears the in-memory rule cache.
func (c *WafRuleCache) Reset() {
	c.mu.Lock()
	c.path = ""
	c.mtimeNs = 0
	c.size = 0
	c.rules = nil
	c.mu.Unlock()
}

// Load returns the active rules from target, reloading if mtime or size changed.
func (c *WafRuleCache) Load(target string) []WafRewriteRule {
	if target == "" {
		return nil
	}
	st, err := os.Stat(target)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			c.mu.Lock()
			if c.path == target && len(c.rules) > 0 {
				c.rules = nil
				c.mtimeNs = 0
				c.size = 0
			}
			c.mu.Unlock()
		}
		return nil
	}
	mtimeNs := st.ModTime().UnixNano()
	sz := st.Size()

	c.mu.RLock()
	if c.path == target && c.mtimeNs == mtimeNs && c.size == sz {
		cp := append([]WafRewriteRule(nil), c.rules...)
		c.mu.RUnlock()
		return cp
	}
	c.mu.RUnlock()

	raw, err := os.ReadFile(target)
	if err != nil {
		return nil
	}
	valid, err := ParseRuleFile(raw)
	if err != nil {
		return nil
	}
	c.mu.Lock()
	c.path = target
	c.mtimeNs = mtimeNs
	c.size = sz
	c.rules = valid
	cp := append([]WafRewriteRule(nil), valid...)
	c.mu.Unlock()
	return cp
}
