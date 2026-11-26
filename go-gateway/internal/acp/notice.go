package acp

import (
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"agy-acp-gateway-go/internal/cleanroom"
	gwcfg "agy-acp-gateway-go/internal/config"
)

// SoftError represents a structured HTTP error extracted from an upstream ACP notice or failure.
type SoftError struct {
	StatusCode int
	ErrorType  string
	Message    string
}

func (e *SoftError) Error() string { return e.Message }

const (
	NoticeNetwork     = "There was a network issue connecting to the server, please try again."
	NoticeHighTraffic = "Our servers are experiencing high traffic right now, please try again in a minute."
	NoticeTerminated  = "Agent execution terminated due to error."
)

// TerminalNotices lists known canonical terminal failure sentences from agy.
var TerminalNotices = []string{NoticeNetwork, NoticeHighTraffic, NoticeTerminated}

// InfoNotices lists informational status notices dropped without failing the turn.
var InfoNotices = []string{
	"The stream was interrupted. Please continue the task you were working on.",
}

// NoticeVerdict determines whether an incoming text chunk should be emitted, held, or dropped.
type NoticeVerdict int

const (
	NoticeEmit NoticeVerdict = iota
	NoticeHold
	NoticeDrop
)

var (
	RetryNoticeRe           = regexp.MustCompile(`(?i)^\s*⚠\s*API error \(attempt \d+\)|your previous response was cut off because it exceeded the output token limit|context canceledThe request was cancelled by the client`)
	RetryApiErrorLineRe     = regexp.MustCompile(`(?i)^\s*⚠\s*API error \(attempt (\d+)\)(?::\s*(.*))?$`)
	RetryApiErrorTextRe     = regexp.MustCompile(`(?i)^\s*(?:⚠\s*)?API error \(attempt (\d+)\)(?::\s*(.*))?$`)
	AgyDynamicTerminalErrRe = regexp.MustCompile(
		`^(?:(?i:error):\s*)?(?:` +
			`(?:RESOURCE_EXHAUSTED|MODEL_CAPACITY_EXHAUSTED|INVALID_ARGUMENT|PERMISSION_DENIED|UNAUTHENTICATED|UNAVAILABLE|DEADLINE_EXCEEDED|INTERNAL|ABORTED|FAILED_PRECONDITION|NOT_FOUND|ALREADY_EXISTS|OUT_OF_RANGE|UNIMPLEMENTED|DATA_LOSS|UNKNOWN)\s*\(code\s*\d{3}\)\s*:` +
			`|(?:RESOURCE_EXHAUSTED|MODEL_CAPACITY_EXHAUSTED|INVALID_ARGUMENT|PERMISSION_DENIED|DEADLINE_EXCEEDED|FAILED_PRECONDITION|OUT_OF_RANGE|DATA_LOSS)\s*:` +
			`|[A-Z][A-Z0-9_]{2,}\s*\(code\s+[45]\d{2}\)\s*:` +
			`|(?i:(?:HTTP(?:/\d(?:\.\d)?)?\s+)?[45]\d{2}\s+(?:RESOURCE_EXHAUSTED|MODEL_CAPACITY_EXHAUSTED|INVALID_ARGUMENT|PERMISSION_DENIED|UNAUTHENTICATED|UNAVAILABLE|DEADLINE_EXCEEDED|INTERNAL|Bad Request|Unauthorized|Forbidden|Too Many Requests|Internal Server Error|Bad Gateway|Service Unavailable|Gateway Timeout)\b)` +
			`|(?i:rpc\s+error:\s*code\s*=|api\s+error:\s*\S|request\s+failed\s+with\s+status\s+[45]\d{2}\b)` +
			`|(?i:individual\s+quota\s+reached\b|no\s+capacity\s+available\s+for\s+model\b|this\s+service\s+has\s+been\s+disabled\s+in\s+this\s+account\b)` +
			`|(?i:you\s+do\s+not\s+have\s+a\s+valid\s+license\b|gemini_enterprise_license_not_valid\b|ccpa_ineligible_tier_id\b|[^\r\n]*#3501\b)` +
			`|(?i:finish[_\s]*reason\s*[:=]\s*["']?(?:SAFETY|PROHIBITED_CONTENT|BLOCKLIST|RECITATION|SPII)\b|(?:[a-z0-9_]+_policy_violation|bio_policy|prohibited_content)\b|blocked\s+(?:by|due\s+to)\s+(?:safety|policy|content\s+filter)\b)` +
			`)`,
	)

	SoftErrBlockedRe = regexp.MustCompile(`(?i)^\s*The prompt could not be submitted|Generative AI Prohibited Use policy|prompt contains sensitive words`)
	softErrVerifyRe  = regexp.MustCompile(`(?im)^\s*⚠?\s*Verification Required\s*$|Verify your account to continue`)
	SoftErrLicenseRe = regexp.MustCompile(`(?i)(?:you\s+do\s+not\s+have\s+a\s+valid\s+license\b|gemini_enterprise_license_not_valid\b|ccpa_ineligible_tier_id\b|\(#3501\)|#3501\b)`)
	softErrTermRe    = regexp.MustCompile(`(?im)^\s*⚠\s*Agent execution terminated due to error`)
	softErrNetRe     = regexp.MustCompile(`^\s*⚠\s*There was a network issue connecting to the server, please try again\.?\s*$`)
	softErrTrafficRe = regexp.MustCompile(`^\s*⚠\s*Our servers are experiencing high traffic right now, please try again in a minute\.?\s*$`)

	upstreamPolicySafetyRe = regexp.MustCompile(
		`(?i)(?:` +
			`\b(?:PROHIBITED_CONTENT|BLOCKLIST|RECITATION|SPII|IMAGE_SAFETY)\b` +
			`|\bfinish[_\s]*reason\s*[:=]\s*["']?SAFETY["']?\b` +
			`|\bblocked\s+(?:by|due\s+to)\s+(?:safety|policy|content\s+filter|prohibited)` +
			`|\bsafety\s+(?:settings?|ratings?|filter|policy)\b` +
			`|\b(?:[a-z0-9_]+_policy_violation|bio_policy)\b` +
			`|Generative\s+AI\s+Prohibited\s+Use\s+policy` +
			`)`,
	)
	upstreamErrorSafetyTokenRe = regexp.MustCompile(`(?i)\bSAFETY\b`)
	retryHintSpanRe            = regexp.MustCompile(`(?i)(?:retry\s+in|resets?\s+in|retrydelay["':\s]+)\s*((?:\d+(?:\.\d+)?\s*(?:h|hr|hrs|hour|hours|m|min|mins|minute|minutes|s|sec|secs|second|seconds)\s*)+|\d+(?:\.\d+)?\s*s?\b)`)
	retryDurUnitRe             = regexp.MustCompile(`(?i)(\d+(?:\.\d+)?)\s*(h|hr|hrs|hour|hours|m|min|mins|minute|minutes|s|sec|secs|second|seconds)?`)
)

// SanitizeNoticeDetail redacts and collapses whitespace in upstream notice detail text.
func SanitizeNoticeDetail(raw string) string {
	det := strings.TrimSpace(gwcfg.RedactSecretsInString(raw))
	det = strings.Join(strings.Fields(det), " ")
	return gwcfg.TruncateRunes(det, 240)
}

// ParseRetryNoticeLine parses `⚠ API error (attempt N): detail`.
func ParseRetryNoticeLine(line string) (int, string, bool) {
	m := RetryApiErrorLineRe.FindStringSubmatch(strings.TrimSpace(line))
	if len(m) == 0 {
		return 0, "", false
	}
	att, _ := strconv.Atoi(m[1])
	det := ""
	if len(m) > 2 {
		det = SanitizeNoticeDetail(m[2])
	}
	return att, det, true
}

// ParseRetryErrorText parses `API error (attempt N): detail` with or without a leading `⚠`
// (SQLite step_type=17 short_error omits the leading `⚠` marker).
func ParseRetryErrorText(raw string) (int, string, bool) {
	m := RetryApiErrorTextRe.FindStringSubmatch(strings.TrimSpace(raw))
	if len(m) == 0 {
		return 0, "", false
	}
	att, _ := strconv.Atoi(m[1])
	det := ""
	if len(m) > 2 {
		det = SanitizeNoticeDetail(m[2])
	}
	return att, det, true
}

// IsStatusMarker reports whether line starts with agy's status warning rune ⚠ (not emoji ⚠️).
func IsStatusMarker(line string) bool {
	return strings.HasPrefix(line, "⚠") && !strings.HasPrefix(line, "⚠\ufe0f")
}

// ParseDynamicTerminalErrorLine inspects unmarked/dynamic ⚠ terminal lines added in agy 1.5.8+.
func ParseDynamicTerminalErrorLine(line, lastRetryDetail string) (string, bool) {
	trimmed := strings.TrimSpace(line)
	if !IsStatusMarker(trimmed) {
		return "", false
	}
	rawBody := strings.TrimSpace(strings.TrimPrefix(trimmed, "⚠"))
	if rawBody == "" {
		return "", false
	}
	clean := SanitizeNoticeDetail(rawBody)
	if clean == "" {
		return "", false
	}
	if lastRetryDetail != "" {
		if clean == lastRetryDetail || (len(clean) >= 12 && len(lastRetryDetail) >= 12 && (strings.HasPrefix(clean, lastRetryDetail) || strings.HasPrefix(lastRetryDetail, clean))) {
			return clean, true
		}
	}
	if AgyDynamicTerminalErrRe.MatchString(rawBody) {
		return clean, true
	}
	return "", false
}

// FormatTerminalNoticeWithDetail appends captured retry error details to generic terminated notice text.
func FormatTerminalNoticeWithDetail(notice, retryDetail string) string {
	if notice == "" || retryDetail == "" || notice != NoticeTerminated {
		return notice
	}
	return strings.TrimSuffix(notice, ".") + ": " + retryDetail
}

// StripNoticeMarker strips leading `⚠` and optional `Error:` prefix.
func StripNoticeMarker(raw string) (body string, marked bool) {
	t := strings.TrimSpace(raw)
	if !IsStatusMarker(t) {
		return t, false
	}
	t = strings.TrimSpace(strings.TrimPrefix(t, "⚠"))
	if len(t) >= 6 && strings.EqualFold(t[:6], "error:") {
		t = strings.TrimSpace(t[6:])
	}
	return t, true
}

// ClassifyAgyNotice classifies buffered opening turn text against known agy status notices.
func ClassifyAgyNotice(raw string, final bool) (v NoticeVerdict, notice, rest string) {
	if RetryNoticeRe.MatchString(raw) {
		return NoticeDrop, "", ""
	}
	body, marked := StripNoticeMarker(raw)
	if marked && !final && (body == "" || strings.HasPrefix("error:", strings.ToLower(body))) {
		return NoticeHold, "", ""
	}
	if marked && !final && strings.HasPrefix("api error (attempt", strings.ToLower(body)) {
		return NoticeHold, "", ""
	}
	for _, n := range TerminalNotices {
		bare := strings.TrimSuffix(n, ".")
		switch {
		case marked && strings.HasPrefix(body, n):
			return NoticeDrop, n, strings.TrimLeft(body[len(n):], " \r\n")
		case body == n || body == bare:
			if (marked && body == n) || final {
				return NoticeDrop, n, ""
			}
			return NoticeHold, "", ""
		case !final && body != "" && strings.HasPrefix(n, body):
			return NoticeHold, "", ""
		}
	}
	return NoticeEmit, "", ""
}

// KnownAgyNoticeTemplates lists every status template recognized by the gateway for upgrade checks.
func KnownAgyNoticeTemplates() []string {
	out := append([]string{}, TerminalNotices...)
	out = append(out, InfoNotices...)
	return append(out, "API error (attempt N)", "Agent execution terminated due to error.", "\n⚠ %s\n")
}

// ClassifyStatusLine classifies a single streaming status line candidate.
func ClassifyStatusLine(line string, complete, opening bool) (v NoticeVerdict, notice string, info bool) {
	if RetryApiErrorLineRe.MatchString(line) {
		if !complete {
			return NoticeHold, "", false
		}
		return NoticeDrop, "", true
	}
	if RetryNoticeRe.MatchString(line) {
		if !complete && IsStatusMarker(strings.TrimSpace(line)) {
			return NoticeHold, "", false
		}
		return NoticeDrop, "", true
	}
	body, marked := StripNoticeMarker(line)
	if marked {
		for _, n := range InfoNotices {
			if strings.HasPrefix(body, n) {
				return NoticeDrop, "", true
			}
			if !complete && strings.HasPrefix(n, body) {
				return NoticeHold, "", false
			}
		}
	}
	if !marked && !opening {
		return NoticeEmit, "", false
	}
	cv, cn, rest := ClassifyAgyNotice(line, complete)
	if cv == NoticeDrop && strings.TrimSpace(rest) != "" {
		return NoticeEmit, "", false
	}
	if cv == NoticeEmit && marked && !complete {
		return NoticeHold, "", false
	}
	return cv, cn, false
}

// ClassifyTurnStatusLine combines static status line matching with agy 1.5.8+ dynamic error parsing.
func ClassifyTurnStatusLine(line string, complete, opening bool, lastRetryDetail string) (v NoticeVerdict, notice, detail string, info bool) {
	v, notice, info = ClassifyStatusLine(line, complete, opening)
	if v == NoticeEmit && complete {
		if det, ok := ParseDynamicTerminalErrorLine(line, lastRetryDetail); ok {
			return NoticeDrop, NoticeTerminated, det, false
		}
	}
	return v, notice, "", info
}

// NoticeOnlyText formats a terminal notice for soft-error detection when no model output was produced.
func NoticeOnlyText(text, notice string, toolCalls int) string {
	if strings.TrimSpace(text) == "" && toolCalls == 0 && notice != "" {
		return "⚠ " + notice
	}
	return text
}

// IsAccountQuotaOrBillingExhausted reports whether detail indicates exhausted quota/billing limits.
func IsAccountQuotaOrBillingExhausted(detail string) bool {
	low := strings.ToLower(detail)
	for _, marker := range []string{
		"individual quota reached",
		"resets in",
		"spend cap",
		"spending cap",
		"spending limit",
		"prepaid",
		"depleted",
		"weekly limit",
		"weekly quota",
		"daily quota",
		"daily limit",
		"billing",
		"insufficient credit",
		"credit balance",
	} {
		if strings.Contains(low, marker) {
			return true
		}
	}
	return false
}

// IsUpstreamPolicyOrSafetyBlock detects Google/upstream safety or content policy rejections.
func IsUpstreamPolicyOrSafetyBlock(text string, inErrorContext bool) bool {
	if text == "" {
		return false
	}
	if upstreamPolicySafetyRe.MatchString(text) {
		return true
	}
	return inErrorContext && upstreamErrorSafetyTokenRe.MatchString(text)
}

// ExtractRetryAfterHeaderValue parses duration hints into seconds (clamped to [5, 3600]).
func ExtractRetryAfterHeaderValue(msg string) string {
	const fallback = "5"
	if msg == "" {
		return fallback
	}
	m := retryHintSpanRe.FindStringSubmatch(msg)
	if len(m) < 2 || strings.TrimSpace(m[1]) == "" {
		return fallback
	}
	var total float64
	matchedAny := false
	for _, part := range retryDurUnitRe.FindAllStringSubmatch(m[1], -1) {
		if len(part) < 2 {
			continue
		}
		val, err := strconv.ParseFloat(part[1], 64)
		if err != nil || val <= 0 {
			continue
		}
		matchedAny = true
		unit := strings.ToLower(strings.TrimSpace(part[2]))
		switch {
		case strings.HasPrefix(unit, "h"):
			total += val * 3600
		case strings.HasPrefix(unit, "m"):
			total += val * 60
		default:
			total += val
		}
	}
	if !matchedAny || total <= 0 {
		return fallback
	}
	sec := int(math.Ceil(total))
	if sec < 5 {
		sec = 5
	} else if sec > 3600 {
		sec = 3600
	}
	return strconv.Itoa(sec)
}

// DetectAntigravitySoftError maps inline agy notice/error text to a structured HTTP SoftError.
// If a ToS notice is detected, onToS is invoked before returning a 503 tos_circuit_breaker SoftError.
func DetectAntigravitySoftError(text string, onToS func(reason string)) *SoftError {
	if text == "" {
		return nil
	}
	head := text
	if len(head) > 600 {
		head = head[:600]
	}
	if cleanroom.IsTosSoftNotice(text) {
		if onToS != nil {
			onToS(head)
		}
		return &SoftError{StatusCode: http.StatusServiceUnavailable, ErrorType: "tos_circuit_breaker", Message: "Antigravity ToS circuit breaker tripped: " + strings.TrimSpace(head)}
	}
	if SoftErrBlockedRe.MatchString(head) {
		return &SoftError{StatusCode: 400, ErrorType: "invalid_request_error", Message: "Blocked by Antigravity prompt filter: " + strings.TrimSpace(head)}
	}
	if softErrVerifyRe.MatchString(head) {
		return &SoftError{StatusCode: 503, ErrorType: "api_error", Message: "Google account verification required: " + strings.TrimSpace(head)}
	}
	if softErrNetRe.MatchString(text) {
		return &SoftError{StatusCode: http.StatusServiceUnavailable, ErrorType: "api_error", Message: "Antigravity upstream network issue: " + strings.TrimSpace(head)}
	}
	if softErrTrafficRe.MatchString(text) {
		return &SoftError{StatusCode: 529, ErrorType: "overloaded_error", Message: "Antigravity upstream overloaded: " + strings.TrimSpace(head)}
	}
	if IsStatusMarker(strings.TrimSpace(head)) && SoftErrLicenseRe.MatchString(head) {
		return &SoftError{StatusCode: http.StatusForbidden, ErrorType: "permission_error", Message: "Antigravity account lacks a valid license (#3501): " + strings.TrimSpace(head)}
	}
	if IsStatusMarker(strings.TrimSpace(head)) && IsUpstreamPolicyOrSafetyBlock(head, true) {
		return &SoftError{StatusCode: http.StatusBadRequest, ErrorType: "invalid_request_error", Message: "Blocked by Antigravity safety or content policy: " + strings.TrimSpace(head)}
	}
	if softErrTermRe.MatchString(head) {
		if SoftErrLicenseRe.MatchString(head) {
			return &SoftError{StatusCode: http.StatusForbidden, ErrorType: "permission_error", Message: "Antigravity account lacks a valid license (#3501): " + strings.TrimSpace(head)}
		}
		if cleanroom.IsTosViolationError(head) {
			if onToS != nil {
				onToS(head)
			}
			return &SoftError{StatusCode: http.StatusServiceUnavailable, ErrorType: "tos_circuit_breaker", Message: "Antigravity ToS circuit breaker tripped: " + strings.TrimSpace(head)}
		}
		if IsUpstreamPolicyOrSafetyBlock(head, true) {
			return &SoftError{StatusCode: http.StatusBadRequest, ErrorType: "invalid_request_error", Message: "Blocked by Antigravity safety or content policy: " + strings.TrimSpace(head)}
		}
		lowHead := strings.ToLower(head)
		if strings.Contains(lowHead, "invalid_argument") || strings.Contains(lowHead, "failed_precondition") || strings.Contains(lowHead, "code 400") || strings.Contains(lowHead, "status 400") || strings.Contains(lowHead, "400 bad request") {
			return &SoftError{StatusCode: 400, ErrorType: "invalid_request_error", Message: "Antigravity upstream invalid request: " + strings.TrimSpace(head)}
		}
		if (strings.Contains(lowHead, "model_capacity_exhausted") || strings.Contains(lowHead, "code 503") || strings.Contains(lowHead, "status 503") || strings.Contains(lowHead, "service unavailable") || strings.Contains(lowHead, "no capacity available") || strings.Contains(lowHead, "overloaded")) &&
			!strings.Contains(lowHead, "429") && !strings.Contains(lowHead, "quota") && !strings.Contains(lowHead, "resource_exhausted") {
			return &SoftError{StatusCode: 529, ErrorType: "overloaded_error", Message: "Antigravity upstream overloaded: " + strings.TrimSpace(head)}
		}
		if strings.Contains(lowHead, "429") || strings.Contains(lowHead, "quota") || strings.Contains(lowHead, "resource_exhausted") || strings.Contains(lowHead, "rate limit") || strings.Contains(lowHead, "rate_limit") || strings.Contains(lowHead, "too many requests") {
			return &SoftError{StatusCode: 429, ErrorType: "rate_limit_error", Message: "Antigravity upstream rate limit or transient failure: " + strings.TrimSpace(head)}
		}
		if strings.Contains(lowHead, "unauthenticated") || strings.Contains(lowHead, "permission_denied") || strings.Contains(lowHead, "code 401") || strings.Contains(lowHead, "code 403") || strings.Contains(lowHead, "status 401") || strings.Contains(lowHead, "status 403") || strings.Contains(lowHead, "401 unauthorized") || strings.Contains(lowHead, "403 forbidden") || strings.Contains(lowHead, "deadline_exceeded") || strings.Contains(lowHead, "code 504") || strings.Contains(lowHead, "code 500") || strings.Contains(lowHead, "code 502") || strings.Contains(lowHead, "status 504") || strings.Contains(lowHead, "status 500") || strings.Contains(lowHead, "status 502") || strings.Contains(lowHead, "internal server error") || strings.Contains(lowHead, "bad gateway") || strings.Contains(lowHead, "gateway timeout") || strings.Contains(lowHead, "internal (code") {
			return &SoftError{StatusCode: http.StatusServiceUnavailable, ErrorType: "api_error", Message: "Antigravity upstream error: " + strings.TrimSpace(head)}
		}
		return &SoftError{StatusCode: 429, ErrorType: "rate_limit_error", Message: "Antigravity upstream rate limit or transient failure: " + strings.TrimSpace(head)}
	}
	return nil
}

// IsLicenseError reports whether msg matches an upstream missing license (#3501) error.
func IsLicenseError(msg string) bool {
	return SoftErrLicenseRe.MatchString(msg)
}
