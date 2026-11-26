package main

import (
	"strings"
	"testing"
)

func TestDetectClientUtilityProbe(t *testing.T) {
	big := strings.Repeat("func x() {}\n", 2000)
	blocks := func(texts ...string) []any {
		var out []any
		for _, s := range texts {
			out = append(out, map[string]any{"type": "text", "text": s})
		}
		return out
	}
	cases := []struct {
		name                string
		msgs                []map[string]any
		suggestion, spinner bool
	}{
		{"suggestion string", []map[string]any{{"role": "user", "content": "[SUGGESTION MODE: Suggest what the user might type next.]"}}, true, false},
		{"suggestion after tool_result", []map[string]any{{"role": "user", "content": []any{
			map[string]any{"type": "tool_result", "tool_use_id": "t1", "content": "ok"},
			map[string]any{"type": "text", "text": "[SUGGESTION MODE: next]"},
		}}}, true, false},
		{"suggestion then reminder block", []map[string]any{{"role": "user", "content": blocks("[SUGGESTION MODE: next]", "<system-reminder>ctx</system-reminder>")}}, true, false},
		{"spinner short with preamble", []map[string]any{{"role": "user", "content": "Context above.\nDescribe your most recent action in 3-5 words."}}, false, true},
		{"marker inside large pasted source", []map[string]any{{"role": "user", "content": big + `isSuggestion := strings.Contains(s, "[SUGGESTION MODE:")` + big}}, false, false},
		{"spinner marker inside large text", []map[string]any{{"role": "user", "content": blocks(big + "Describe your most recent action in 3-5 words" + big)}}, false, false},
		{"marker only in earlier block", []map[string]any{{"role": "user", "content": blocks("[SUGGESTION MODE: x]", "please review the diff")}}, false, false},
		{"marker in assistant message", []map[string]any{{"role": "assistant", "content": "[SUGGESTION MODE: x]"}}, false, false},
		{"marker in tool_result only", []map[string]any{{"role": "user", "content": []any{
			map[string]any{"type": "tool_result", "tool_use_id": "t1", "content": "[SUGGESTION MODE: x]"},
		}}}, false, false},
		{"empty", nil, false, false},
		{"suggestion marker quoted mid-sentence", []map[string]any{{"role": "user", "content": `Explain the literal string "[SUGGESTION MODE:" in this Go code.`}}, false, false},
		{"spinner marker quoted for translation", []map[string]any{{"role": "user", "content": `Translate to French: "Describe your most recent action in 3-5 words"`}}, false, false},
	}
	for _, c := range cases {
		s, sp := detectClientUtilityProbe(c.msgs)
		if s != c.suggestion || sp != c.spinner {
			t.Errorf("%s: got suggestion=%v spinner=%v, want %v %v", c.name, s, sp, c.suggestion, c.spinner)
		}
	}
}
