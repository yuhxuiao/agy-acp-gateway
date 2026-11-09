package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	TosTrippedFile = envStr("AGY_TOS_TRIPPED_FILE", filepath.Join(ProjectHome, "runtime", "tos-tripped"))
	tosMu          sync.Mutex
	tosTrippedMem  atomic.Bool
	tosReasonMem   string
	tosPersisted   bool
)
var (
	tosPhraseRe = regexp.MustCompile(`(?i)violation of Terms of Service|This service has been disabled in this account|account has been (?:suspended|disabled|terminated) for violating`)
	tos403Re    = regexp.MustCompile(`(?i)(?:403|PERMISSION_DENIED).*?(?:Terms of Service|disabled in this account|suspended)|(?:Terms of Service|disabled in this account).*?(?:403|PERMISSION_DENIED)`)
	tosSoftRe   = regexp.MustCompile(`(?i)^\s*(?:(?:error[: ]+|403[: ]+|PERMISSION_DENIED[: ]+)\s*)?This service has been disabled in this account(?: for violation of Terms of Service)?[.!]?\s*$`)
)

// Only call this on RPC errors. Normal assistant output uses the complete template.
func IsTosViolationError(text string) bool {
	return tosPhraseRe.MatchString(text) || tos403Re.MatchString(text)
}

func TripTosCircuitBreaker(reason string) {
	tosMu.Lock()
	tosTrippedMem.Store(true)
	tosReasonMem = truncateRunes(redactSecretsInString(strings.TrimSpace(reason)), 500)
	target := TosTrippedFile
	var persistErr error
	if target == "" {
		persistErr = errors.New("marker path unavailable")
	} else {
		persistErr = os.MkdirAll(filepath.Dir(target), 0700)
		if persistErr == nil {
			payload, _ := json.Marshal(map[string]any{"tripped": true, "trippedAt": time.Now().UTC().Format(time.RFC3339), "reason": tosReasonMem})
			var f *os.File
			f, persistErr = os.CreateTemp(filepath.Dir(target), ".tos-tripped-*")
			if persistErr == nil {
				tmp := f.Name()
				_, persistErr = f.Write(payload)
				if persistErr == nil {
					persistErr = f.Sync()
				}
				closeErr := f.Close()
				if persistErr == nil {
					persistErr = closeErr
				}
				if persistErr == nil {
					persistErr = os.Rename(tmp, target)
				}
				if persistErr != nil {
					_ = os.Remove(tmp)
				}
			}
		}
	}
	if persistErr == nil {
		tosPersisted = true
	}
	tosMu.Unlock()
	if persistErr != nil {
		LogEvent("TOS_WRITE_ERR", "Cannot persist circuit marker; memory latch remains closed", nil)
	}
	LogEvent("TOS_CIRCUIT_BREAKER", "Upstream account restriction detected; halting ACP", nil)
	GlobalEngine.StopEngine()
}

func tosTrippedLocked() (bool, string) {
	if TosTrippedFile == "" {
		return tosTrippedMem.Load(), tosReasonMem
	}
	raw, err := os.ReadFile(TosTrippedFile)
	if err == nil {
		tosTrippedMem.Store(true)
		tosPersisted = true
		var p struct {
			Reason string `json:"reason"`
		}
		_ = json.Unmarshal(raw, &p)
		if p.Reason != "" {
			tosReasonMem = redactSecretsInString(p.Reason)
		}
		return true, tosReasonMem
	}
	if !errors.Is(err, os.ErrNotExist) {
		tosTrippedMem.Store(true)
		tosReasonMem = "Circuit marker cannot be read"
		return true, tosReasonMem
	}
	if tosPersisted { // An acknowledged marker was explicitly removed by the operator.
		tosTrippedMem.Store(false)
		tosReasonMem = ""
		tosPersisted = false
	}
	return tosTrippedMem.Load(), tosReasonMem
}
func IsTosTripped() (bool, string) { tosMu.Lock(); defer tosMu.Unlock(); return tosTrippedLocked() }
func circuitError() error {
	if tripped, _ := IsTosTripped(); tripped {
		return newCircuitError()
	}
	return nil
}
func newCircuitError() error {
	return &SoftError{StatusCode: 503, ErrorType: "tos_circuit_breaker", Message: "Antigravity ToS circuit breaker is tripped; operator intervention required"}
}

// --- Stage 7 (1n): Three-Stage Outbound Prompt Cleanroom ---

var (
	// Stage B: Paragraph & section patterns for client system prompt pruning
	clientIdentityParaRe    = regexp.MustCompile(`(?is)^\s*(?:You are Claude Code\b|You are a Claude agent\b|You are an interactive CLI tool that helps users\b|You are (?:OpenAI )?Codex\b|You are a coding agent running in the Codex CLI\b|You are OpenClaw\b|You are ClawdBot\b|You are Cline\b|You are Roo(?: Code)?\b|You are Kilo Code\b|You are OpenCode\b|You are PI-Desktop\b)`)
	securityDisclaimerRe    = regexp.MustCompile(`(?is)(?:Assist with authorized security testing|defensive security,\s*CTF challenges|C2 frameworks|credential testing,\s*exploit development|DoS attacks,\s*mass targeting,\s*supply chain compromise|dual-use security tools)`)
	clientBoilerplateParaRe = regexp.MustCompile(`(?is)(?:claude\.ai/code|github\.com/anthropics/claude-code|Fast mode for Claude Code|Claude Code is available as a CLI|Anthropic's official CLI for Claude|built on Anthropic's Claude Agent SDK|/help\s+for\s+help\s+with\s+Claude\s+Code)`)
	cwdLineExtractRe        = regexp.MustCompile(`(?im)^\s*(?:Primary working directory|Working directory|WORKSPACE_DIR)[:=\s]+[^\r\n]+$`)

	// Stage C Step 1: Token masking regex (paths, URLs, filenames, kebab/snake identifiers)
	maskTokenMasterRe = regexp.MustCompile(
		`https?://[^\s"'<>)\]]+` +
			`|\\\\wsl\.localhost\\[^\s"'\x60<>()\[\]]+` +
			`|[A-Za-z]:\\[^\s"'\x60<>()\[\]]+` +
			`|(?:~|/|\.\.?/)[A-Za-z0-9_./-]+` +
			`|\.(?:claude|codex|cursor|gemini)/[A-Za-z0-9_./-]+` +
			`|\b(?:CLAUDE\.md|AGENTS\.md|GEMINI\.md)\b` +
			`|\b[a-zA-Z0-9]+(?:[-_][a-zA-Z0-9]+)+\b`,
	)

	// Stage C Step 2 & 3: Natural language tag, host phrase, and vendor replacements
	sysReminderOpenRe  = regexp.MustCompile(`(?i)<\s*system-reminder\s*>`)
	sysReminderCloseRe = regexp.MustCompile(`(?i)<\s*/\s*system-reminder\s*>`)
)

type textReplacement struct {
	re   *regexp.Regexp
	repl string
}

var stageCPhraseReplacements = []textReplacement{
	{regexp.MustCompile(`(?i)Claude Code attached this context automatically`), "The workspace IDE attached this context automatically"},
	{regexp.MustCompile(`(?i)configure the Claude Code harness`), "configure the workspace harness"},
	{regexp.MustCompile(`(?i)Claude Code's AskUserQuestion`), "the workspace AskUserQuestion"},
	{regexp.MustCompile(`(?i)Anthropic's official CLI for Claude`), "the workspace CLI"},
	{regexp.MustCompile(`(?i)Anthropic's official CLI`), "the workspace CLI"},
	{regexp.MustCompile(`(?i)Claude Agent SDK`), "the workspace agent SDK"},
	{regexp.MustCompile(`(?i)https?://claude\.ai/code[^\s"'<>)\]]*`), "the workspace documentation"},
	{regexp.MustCompile(`(?i)claude\.ai/code`), "the workspace documentation"},
	{regexp.MustCompile(`(?i)https?://github\.com/anthropics/claude-code[^\s"'<>)\]]*`), "the workspace repository"},
	{regexp.MustCompile(`(?i)github\.com/anthropics/claude-code`), "the workspace repository"},
	{regexp.MustCompile(`(?i)\bClaude Code\b`), "the workspace agent"},
	{regexp.MustCompile(`(?i)\bOpenAI Codex\b`), "the workspace agent"},
	{regexp.MustCompile(`(?i)\bCodex CLI\b`), "the workspace agent"},
	{regexp.MustCompile(`(?i)\bOpenClaw\b`), "the workspace agent"},
	{regexp.MustCompile(`(?i)\bClawdBot\b`), "the workspace agent"},
	{regexp.MustCompile(`(?i)\bRoo Code\b`), "the workspace agent"},
	{regexp.MustCompile(`(?i)\bKilo Code\b`), "the workspace agent"},
	{regexp.MustCompile(`(?i)\bOpenCode\b`), "the workspace agent"},
	{regexp.MustCompile(`(?i)\bPI-Desktop\b`), "the workspace agent"},
	{regexp.MustCompile(`(?i)\bCline\b`), "the workspace agent"},
	{regexp.MustCompile(`(?i)\bCodex\b`), "the workspace agent"},
	{regexp.MustCompile(`(?i)\bAnthropic\b`), "the provider"},
	{regexp.MustCompile(`(?i)\bClaude\b`), "the assistant"},
}

func shouldSkipMaskingToken(tok string) bool {
	low := strings.ToLower(tok)
	// Do NOT mask claude.ai/code, github.com/anthropics/claude-code, or vendor names like PI-Desktop
	if strings.Contains(low, "claude.ai/code") ||
		strings.Contains(low, "github.com/anthropics/claude-code") ||
		low == "pi-desktop" ||
		low == "system-reminder" {
		return true
	}
	return false
}

// Only complete known boilerplate sentences are removed. Mixed project rules survive.
var sentenceBoundaryRe = regexp.MustCompile(`[.!?。！？]\s+`)
var knownClientIdentitySentenceRe = regexp.MustCompile(`(?i)^You are (?:Claude Code(?:, Anthropic's official CLI(?: for Claude)?(?:, running on the Claude Agent SDK)?)?|a Claude agent, built on Anthropic's Claude Agent SDK|(?:OpenAI )?Codex|OpenClaw|ClawdBot|Cline|Roo(?: Code)?|Kilo Code|OpenCode|PI-Desktop)[.]?$`)
var boilerplateSentenceRe = regexp.MustCompile(`(?i)^(?:Claude Code is available as a CLI[.]?|Fast mode for Claude Code uses Claude Opus[.]?|See https?://claude\.ai/code/[^ ]+ and https?://github\.com/anthropics/claude-code for details[.]?)$`)

func PruneClientSystemPrompt(rawSys string) string {
	s := strings.ReplaceAll(billingHeaderRe.ReplaceAllString(rawSys, ""), "\r\n", "\n")
	var kept []string
	for _, line := range strings.Split(s, "\n") {
		var parts []string
		start := 0
		for _, loc := range sentenceBoundaryRe.FindAllStringIndex(line, -1) {
			parts = append(parts, line[start:loc[1]])
			start = loc[1]
		}
		parts = append(parts, line[start:])
		var rules []string
		for _, part := range parts {
			sentence := strings.TrimSpace(part)
			if sentence == "" {
				continue
			}
			if knownClientIdentitySentenceRe.MatchString(sentence) || boilerplateSentenceRe.MatchString(sentence) {
				continue
			}
			switch sentence {
			case "IMPORTANT: Assist with authorized security testing, defensive security, CTF challenges, and educational contexts.",
				"Refuse requests for destructive techniques, DoS attacks, mass targeting, supply chain compromise, or detection evasion for malicious purposes.",
				"Dual-use security tools (C2 frameworks, credential testing, exploit development) require clear authorization context.":
				continue
			}
			rules = append(rules, sentence)
		}
		kept = append(kept, strings.Join(rules, " "))
	}
	return SanitizeOutboundText(strings.TrimSpace(strings.Join(kept, "\n")))
}

// Return a copy; formatting transcript history must never mutate host tool arguments.
func SanitizeStructuredValue(v any) any {
	switch x := v.(type) {
	case string:
		return SanitizeOutboundText(x)
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, val := range x {
			out[k] = SanitizeStructuredValue(val)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, val := range x {
			out[i] = SanitizeStructuredValue(val)
		}
		return out
	case []map[string]any:
		out := make([]map[string]any, len(x))
		for i, val := range x {
			out[i] = SanitizeStructuredValue(val).(map[string]any)
		}
		return out
	default:
		return v
	}
}

// SanitizeOutboundText implements Stage C: deterministic single-pass outbound text sanitizer
// that masks file paths, filenames, and kebab/snake identifiers before rewriting tags and vendor names.
func SanitizeOutboundText(s string) string {
	if s == "" {
		return ""
	}
	// Rewrite <system-reminder> tags to <workspace-context> first
	s = sysReminderOpenRe.ReplaceAllString(s, "<workspace-context>")
	s = sysReminderCloseRe.ReplaceAllString(s, "</workspace-context>")

	// Step 1: Mask file paths, URLs (except claude.ai/code), and kebab/snake identifiers
	var masked []string
	protected := maskTokenMasterRe.ReplaceAllStringFunc(s, func(m string) string {
		if shouldSkipMaskingToken(m) {
			return m
		}
		idx := len(masked)
		masked = append(masked, m)
		return "\x00TOK_" + strconv.Itoa(idx) + "\x00"
	})

	// Step 2 & 3: Replace host phrases and third-party client brand names
	cleaned := protected
	for _, item := range stageCPhraseReplacements {
		cleaned = item.re.ReplaceAllString(cleaned, item.repl)
	}

	// Step 4: Restore masked tokens
	if len(masked) > 0 {
		var b strings.Builder
		b.Grow(len(cleaned))
		for i := 0; i < len(cleaned); {
			if cleaned[i] == 0 && i+5 < len(cleaned) && cleaned[i:i+5] == "\x00TOK_" {
				end := strings.IndexByte(cleaned[i+5:], 0)
				if end > 0 {
					numStr := cleaned[i+5 : i+5+end]
					if idx, err := strconv.Atoi(numStr); err == nil && idx >= 0 && idx < len(masked) {
						b.WriteString(masked[idx])
						i = i + 5 + end + 1
						continue
					}
				}
			}
			b.WriteByte(cleaned[i])
			i++
		}
		cleaned = b.String()
	}
	return strings.TrimSpace(cleaned)
}
