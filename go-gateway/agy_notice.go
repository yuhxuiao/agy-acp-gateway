package main

import (
	"strings"

	"agy-acp-gateway-go/internal/acp"
)

var (
	retryApiErrorLineRe     = acp.RetryApiErrorLineRe
	agyDynamicTerminalErrRe = acp.AgyDynamicTerminalErrRe
	retryNoticeRe           = acp.RetryNoticeRe
)

const (
	agyNoticeNetwork     = acp.NoticeNetwork
	agyNoticeHighTraffic = acp.NoticeHighTraffic
	agyNoticeTerminated  = acp.NoticeTerminated
)

var agyTerminalNotices = acp.TerminalNotices
var agyInfoNotices = acp.InfoNotices

type noticeVerdict = acp.NoticeVerdict

const (
	noticeEmit = acp.NoticeEmit
	noticeHold = acp.NoticeHold
	noticeDrop = acp.NoticeDrop
)

func parseRetryNoticeLine(line string) (int, string, bool) { return acp.ParseRetryNoticeLine(line) }

func parseRetryErrorText(raw string) (int, string, bool) { return acp.ParseRetryErrorText(raw) }

func sanitizeNoticeDetail(raw string) string { return acp.SanitizeNoticeDetail(raw) }

func parseDynamicTerminalErrorLine(line, lastRetryDetail string) (string, bool) {
	return acp.ParseDynamicTerminalErrorLine(line, lastRetryDetail)
}

func formatTerminalNoticeWithDetail(notice, retryDetail string) string {
	return acp.FormatTerminalNoticeWithDetail(notice, retryDetail)
}

func classifyAgyNotice(raw string, final bool) (v noticeVerdict, notice, rest string) {
	return acp.ClassifyAgyNotice(raw, final)
}

func KnownAgyNoticeTemplates() []string { return acp.KnownAgyNoticeTemplates() }

func isStatusMarker(line string) bool { return acp.IsStatusMarker(line) }

func classifyTurnStatusLine(line string, complete, opening bool, lastRetryDetail string) (v noticeVerdict, notice, detail string, info bool) {
	return acp.ClassifyTurnStatusLine(line, complete, opening, lastRetryDetail)
}

func noticeOnlyText(text, notice string, toolCalls int) string {
	return acp.NoticeOnlyText(text, notice, toolCalls)
}

func (t *activeTurnState) acceptTextLocked(text string) string {
	buf := t.noticeHold + text
	t.noticeHold = ""
	var out strings.Builder
	for buf != "" {
		if !t.midLine {
			trimmed := strings.TrimLeft(buf, " \t\r\n")
			ws := buf[:len(buf)-len(trimmed)]
			if trimmed == "" {
				t.noticeHold = buf
				break
			}
			opening := !t.noticeSettled
			if isStatusMarker(trimmed) || opening {
				nl := strings.IndexByte(trimmed, '\n')
				line, rest, complete := trimmed, "", false
				if nl >= 0 {
					line, rest, complete = trimmed[:nl], trimmed[nl+1:], true
				}
				v, notice, detail, info := classifyTurnStatusLine(line, complete, opening, t.lastRetryDetail)
				switch v {
				case noticeHold:
					t.noticeHold = buf
					return out.String()
				case noticeDrop:
					if notice != "" {
						t.noticeText = notice
						t.noticeFailed = true
						if detail != "" {
							t.lastRetryDetail = detail
						}
					} else if info {
						t.recordDroppedInfoLineLocked(line)
					}
					if out.Len() > 0 || t.emittedText {
						out.WriteString(ws)
					}
					buf = rest
					continue
				}
				t.noticeSettled = true
				if !complete {
					out.WriteString(ws + line)
					t.midLine = true
					t.markNormalLocked(line)
					buf = ""
					continue
				}
				out.WriteString(ws + line + "\n")
				t.markNormalLocked(line)
				buf = rest
				continue
			}
			out.WriteString(ws)
			buf = trimmed
			t.midLine = true
		}
		nl := strings.IndexByte(buf, '\n')
		if nl < 0 {
			out.WriteString(buf)
			t.markNormalLocked(buf)
			buf = ""
			continue
		}
		out.WriteString(buf[:nl+1])
		t.markNormalLocked(buf[:nl])
		buf = buf[nl+1:]
		t.midLine = false
	}
	if out.Len() > 0 {
		t.emittedText = true
	}
	return out.String()
}

func (t *activeTurnState) recordDroppedInfoLineLocked(line string) {
	t.noticeInfo++
	if att, det, ok := parseRetryNoticeLine(line); ok {
		t.apiRetryCount++
		if att > 0 {
			t.lastRetryAttempt = att
		}
		if det != "" {
			t.lastRetryDetail = det
		}
	}
}

func (t *activeTurnState) markNormalLocked(s string) {
	if strings.TrimSpace(s) != "" {
		t.noticeSettled = true
		t.noticeFailed = false
		t.lastRetryDetail = ""
	}
}

func (t *activeTurnState) finishNoticeLocked() string {
	held := t.noticeHold
	t.noticeHold = ""
	if strings.TrimSpace(held) == "" {
		t.noticeSettled = true
		return ""
	}
	trimmed := strings.TrimLeft(held, " \t\r\n")
	ws := held[:len(held)-len(trimmed)]
	v, notice, detail, info := classifyTurnStatusLine(trimmed, true, !t.noticeSettled, t.lastRetryDetail)
	t.noticeSettled = true
	if v == noticeDrop {
		if notice != "" {
			t.noticeText = notice
			t.noticeFailed = true
			if detail != "" {
				t.lastRetryDetail = detail
			}
		} else if info {
			t.recordDroppedInfoLineLocked(trimmed)
		}
		return ""
	}
	t.markNormalLocked(trimmed)
	if !t.emittedText {
		ws = ""
	}
	t.emittedText = true
	return ws + trimmed
}
