package cleanroom

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"unicode"
)

func TestReversibleCodecAndChunkSplitRestorer(t *testing.T) {
	codec := NewReversibleSessionCodec("tenant-key-1")
	orig := `<claude_custom id="x">Keep /Users/dev/DeepSeek Harness/app intact</claude_custom>`
	encoded := codec.EncodeOutboundText(orig)
	if strings.Contains(encoded, "claude_custom") || strings.Contains(encoded, "DeepSeek Harness") {
		t.Fatalf("expected encoded output to mask wrapper tag & phrase, got %q", encoded)
	}
	for split := 1; split <= 13; split++ {
		sr := codec.NewStreamRestorer()
		var out strings.Builder
		for i := 0; i < len(encoded); i += split {
			end := i + split
			if end > len(encoded) {
				end = len(encoded)
			}
			out.WriteString(sr.Push(encoded[i:end]))
		}
		out.WriteString(sr.Flush())
		if out.String() != orig {
			t.Fatalf("split=%d mismatch: got %q want %q", split, out.String(), orig)
		}
	}
}

func TestPruneSystemPromptAndOutboundSanitization(t *testing.T) {
	in := "x-anthropic-billing-header: foo\nYou are Claude Code, Anthropic's official CLI for Claude.\nPreserve ~/repo/claude_runner.py when helping Claude Code users."
	out := PruneClientSystemPrompt(in, nil, nil)
	if strings.Contains(out, "Claude Code") || strings.Contains(out, "x-anthropic-billing-header") {
		t.Fatalf("client identity or billing header leaked: %q", out)
	}
	if !strings.Contains(out, "~/repo/claude_runner.py") {
		t.Fatalf("masked file path lost during sanitization: %q", out)
	}
}

func TestTosBreakerFileMode0600AndWafRuleCache(t *testing.T) {
	dir := t.TempDir()
	tosPath := filepath.Join(dir, "tos-tripped")
	var trips atomic.Int32
	brk := NewTosBreaker(tosPath, func() { trips.Add(1) })
	if err := brk.Trip("violation of Terms of Service"); err != nil {
		t.Fatalf("Trip error: %v", err)
	}
	if trips.Load() != 1 {
		t.Fatalf("expected 1 onTrip callback, got %d", trips.Load())
	}
	info, err := os.Stat(tosPath)
	if err != nil {
		t.Fatalf("Stat tosPath error: %v", err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("expected 0600 mode, got %#o", info.Mode().Perm())
	}
	if tripped, _ := brk.IsTripped(); !tripped {
		t.Fatal("expected IsTripped()==true")
	}

	rulesPath := filepath.Join(dir, "waf.json")
	if err := PersistRuleFile(rulesPath, "blocked exact phrase longer than twelve chars", "bisect", nil); err != nil {
		t.Fatalf("PersistRuleFile error: %v", err)
	}
	var cache WafRuleCache
	rules := cache.Load(rulesPath)
	if len(rules) != 1 {
		t.Fatalf("expected 1 loaded rule, got %d", len(rules))
	}
	rewritten := ApplyRules("hello\n- blocked exact phrase longer than twelve chars\nworld", rules)
	if strings.Contains(rewritten, "blocked exact phrase") || strings.Contains(rewritten, "\n- \n") {
		t.Fatalf("unexpected WAF rule output: %q", rewritten)
	}
}

func TestTosBreakerNegativeCacheAndImmediateTripLatch(t *testing.T) {
	dir := t.TempDir()
	tosPath := filepath.Join(dir, "tos-tripped")
	brk := NewTosBreaker(tosPath, nil)

	// Initial absent check caches negative status for 1s.
	if tripped, _ := brk.IsTripped(); tripped {
		t.Fatal("expected initial IsTripped==false")
	}
	if brk.negCacheUntilNs.Load() == 0 {
		t.Fatal("expected negative cache deadline to be populated")
	}
	// External file creation is masked while within the 1s negative cache window.
	if err := os.WriteFile(tosPath, []byte(`{"tripped":true,"reason":"external"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if tripped, _ := brk.IsTripped(); tripped {
		t.Fatal("expected negative cache hit within 1s TTL")
	}
	// ResetNegativeCache forces immediate re-stat.
	brk.ResetNegativeCache()
	if tripped, reason := brk.IsTripped(); !tripped || reason != "external" {
		t.Fatalf("expected tripped=true reason=external after cache reset, got %v %q", tripped, reason)
	}
	// Removing marker clears persisted latch and repopulates negative cache; Trip latches immediately.
	if err := os.Remove(tosPath); err != nil {
		t.Fatal(err)
	}
	if tripped, _ := brk.IsTripped(); tripped {
		t.Fatal("expected cleared latch after removing marker")
	}
	if err := brk.Trip("violation of Terms of Service"); err != nil {
		t.Fatalf("Trip error: %v", err)
	}
	if tripped, _ := brk.IsTripped(); !tripped {
		t.Fatal("expected Trip to immediately invalidate negative cache and latch tripped=true")
	}
}

func refSanitizeOutboundTextUngated(s string, applyWafCtx func(string) string) string {
	if s == "" || strings.TrimSpace(s) == "" {
		return ""
	}
	leadIndent := leadingLine1HorizontalIndent(s)
	s = sysReminderOpenRe.ReplaceAllString(s, "<workspace-context>")
	s = sysReminderCloseRe.ReplaceAllString(s, "</workspace-context>")
	s = pastedContentOpenRe.ReplaceAllString(s, "<user-excerpt$1>")
	s = pastedContentCloseRe.ReplaceAllString(s, "</user-excerpt>")

	var masked []string
	var protBuf strings.Builder
	protBuf.Grow(len(s))
	lastPos := 0
	for _, loc := range maskTokenMasterRe.FindAllStringIndex(s, -1) {
		start, end := loc[0], loc[1]
		m := s[start:end]
		protBuf.WriteString(s[lastPos:start])
		if shouldSkipMaskingToken(m) || isSlashJoinedVendorList(s, start, end) {
			protBuf.WriteString(m)
		} else {
			stored := m
			if len(m) == len("tools.opencode.") && strings.EqualFold(m, "tools.opencode.") {
				stored = m[:5] + `["` + m[6:14] + `"].`
			}
			idx := len(masked)
			masked = append(masked, stored)
			protBuf.WriteString("\x00TOK_")
			protBuf.WriteString(strconv.Itoa(idx))
			protBuf.WriteByte(0)
		}
		lastPos = end
	}
	protBuf.WriteString(s[lastPos:])
	cleaned := protBuf.String()

	for _, item := range stageCPhraseReplacements {
		cleaned = item.re.ReplaceAllString(cleaned, item.repl)
	}

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
	if applyWafCtx != nil {
		cleaned = applyWafCtx(cleaned)
	}
	if strings.TrimSpace(cleaned) == "" {
		return ""
	}
	if leadIndent != "" && strings.HasPrefix(cleaned, leadIndent) {
		return strings.TrimRightFunc(cleaned, unicode.IsSpace)
	}
	return strings.TrimSpace(cleaned)
}

func refNeutralizeToolIdentifierUngated(name string) string {
	if name == "" {
		return ""
	}
	out := name
	for _, rep := range toolVendorTokenReplacements {
		out = rep.re.ReplaceAllString(out, rep.repl)
	}
	return out
}

func TestSanitizeOutboundTextStemGateEquivalence(t *testing.T) {
	waf := func(s string) string {
		return ApplyRulesInWorkspaceContext(s, []WafRewriteRule{{Literal: "secret trigger phrase"}})
	}
	samples := []string{
		"",
		"   \n\t  ",
		"Plain Go source code: func main() { fmt.Println(\"hello world\") }",
		"  indented code line without any vendor keywords\n\t/Users/dev/project/internal/pkg/service.go",
		"<workspace-context>\nsecret trigger phrase\nkeep this line\n</workspace-context>",
		"<system-reminder>internal note</system-reminder>\nKeep normal text.",
		"<pasted_content id=\"abc\">hello world</pasted_content>",
		"<system-reminder>unclosed\n<claude_instructions mode=\"strict\">hello</claude_instructions>\n<codex_tool name=\"x\"/>",
		"<anthropic_block>x</anthropic_block><openai_block>y</openai_block><cursor_rule>z</cursor_rule><cline_step>a</cline_step><roocode_cfg>b</roocode_cfg><dsh_env>c</dsh_env>",
		"Preserve ~/.claude/settings.json and https://docs.anthropic.com/claude-code while replacing Claude Code and Anthropic.",
		"CLAUDE CODE claude-code claude_code Claude.md CLAUDE.md Claude Sonnet 4.6 Claude Opus 4.7 Claude Haiku 4.5 Claude 3.7 claude-sonnet-4 claude-opus-4 claude-haiku-4",
		"Anthropic's official CLI for Claude, Anthropic API, ANTHROPIC_API_KEY, anthropic.com, support.claude.com, code.claude.com",
		"Anthropic / Claude model and claude-cli/2.1.305 plus x-claude-code-session-id",
		"OpenAI Codex CLI, Codex CLI, codex-cli, codex, AGENTS.md, ChatGPT, GPT-5, GPT-4o, o3-mini, OPENAI_API_KEY, openai.com",
		"DeepSeek Harness, deepseek-harness, DeepSeek CLI, DeepSeek-V3, DeepSeek-R1, DeepSeek, dsh-bridge, dsh_, DSH, window.__DSH_BOOT__",
		"Cursor Agent, Cursor IDE, .cursorrules, Windsurf, Roo Code, Roo-Code, Kilo Code, OpenCode, OpenClaw, ClawdBot, Augment Code, PI-Desktop, Cline",
		"  Claude Code with leading spaces\n\tCodex CLI with leading tab\n  DeepSeek Harness with indent\n",
		"Mixed edge: my-claude-file.txt /tmp/.claude/cache.db http://openai.com/docs/codex `CLAUDE.md` vs Claude Code.",
		"Non-matching partial substrings: applaud, claustrophobic, anthropometry, rooco, recline, cursorily.",
		"Scoped & standalone coding-agent paths: /usr/lib/node_modules/@mariozechner/pi-coding-agent/README.md /home/dev/@earendil-works/pi-coding-agent/docs /opt/@acme/foobot-coding-agent/bin /opt/pi-coding-agent/cli.js /opt/@earendil-works/pi-ai/index.js",
	}
	for i, in := range samples {
		gotNoWaf := SanitizeOutboundText(in, nil)
		wantNoWaf := refSanitizeOutboundTextUngated(in, nil)
		if gotNoWaf != wantNoWaf {
			t.Fatalf("sample[%d] (no WAF) mismatch:\n  in:   %q\n  got:  %q\n  want: %q", i, in, gotNoWaf, wantNoWaf)
		}
		gotWaf := SanitizeOutboundText(in, waf)
		wantWaf := refSanitizeOutboundTextUngated(in, waf)
		if gotWaf != wantWaf {
			t.Fatalf("sample[%d] (with WAF) mismatch:\n  in:   %q\n  got:  %q\n  want: %q", i, in, gotWaf, wantWaf)
		}
		// Verify sensitivePathPhraseRe stem gate never misses any regex match.
		if sensitivePathPhraseRe.MatchString(in) && !hasSensitivePathPhraseStem(in) {
			t.Fatalf("hasSensitivePathPhraseStem missed regex match in sample[%d]: %q", i, in)
		}
	}

	toolNames := []string{
		"", "Read", "Write", "Bash", "Grep", "mcp__git__status", "mcp__dbx__execute_query",
		"claude_code_run", "CLAUDE_EXEC", "ClaudeCodeRun", "anthropic_fetch", "codex_patch", "codex-cli-run", "openai_chat",
		"deepseek_harness_run", "DeepSeekHarnessExec", "dsh_shell", "run_dsh", "run_dsh_now",
		"openclaw_exec", "open_claw_run", "clawdbot_go", "clawd_bot_go",
		"roo_code_apply", "kilo_code_run", "augment_code_patch", "cursor_edit", "cline_task",
		"MixedAnthropicClaudeCodex", "my_dsh_bridge_tool", "non_vendor_tool_123",
		"__pi_deferred_placeholder__", "__foobot_deferred_placeholder__", "pi_coding_agent_exec", "pi-desktop-open",
		"pi_ask", "mcp__pi__run", "run_pi",
		"pipe", "copy", "api", "api_call", "topic", "ping", "pin_item", "pip_install", "kpi_metrics",
	}
	for _, name := range toolNames {
		got := NeutralizeToolIdentifier(name)
		want := refNeutralizeToolIdentifierUngated(name)
		if got != want {
			t.Fatalf("NeutralizeToolIdentifier(%q) = %q, want %q", name, got, want)
		}
	}
}

func TestVanillaAndEarendilPiSystemPromptsAndReversibleRoundtrip(t *testing.T) {
	t.Run("vanilla_mariozechner_pi_coding_agent", func(t *testing.T) {
		skillPath := "/home/dev/.npm-global/lib/node_modules/@mariozechner/pi-coding-agent/skills/code-review/SKILL.md"
		rawSys := strings.Join([]string{
			"You are an expert coding assistant operating inside pi, a coding agent harness. You help users by reading files, executing commands, editing code, and writing new files.",
			"",
			"Available tools:",
			"- read: Read file contents",
			"- bash: Execute bash commands (ls, grep, find, etc.)",
			"- edit: Make surgical edits to files (find exact text and replace)",
			"- write: Create or overwrite files",
			"",
			"In addition to the tools above, you may have access to other custom tools depending on the project.",
			"",
			"Guidelines:",
			"- Use bash for file operations like ls, rg, find",
			"- Use read to examine files before editing. You must use this tool instead of cat or sed.",
			"- Be concise in your responses",
			"- Show file paths clearly when working with files",
			"",
			"Pi documentation (read only when the user asks about pi itself, its SDK, extensions, themes, skills, or TUI):",
			"- Main documentation: /home/dev/.npm-global/lib/node_modules/@mariozechner/pi-coding-agent/README.md",
			"- Additional docs: /home/dev/.npm-global/lib/node_modules/@mariozechner/pi-coding-agent/docs",
			"- Examples: /home/dev/.npm-global/lib/node_modules/@mariozechner/pi-coding-agent/examples (extensions, custom tools, SDK)",
			"- When asked about: extensions (docs/extensions.md, examples/extensions/), themes (docs/themes.md), skills (docs/skills.md), prompt templates (docs/prompt-templates.md), TUI components (docs/tui.md), keybindings (docs/keybindings.md), SDK integrations (docs/sdk.md), custom providers (docs/custom-provider.md), adding models (docs/models.md), pi packages (docs/packages.md)",
			"- When working on pi topics, read the docs and examples, and follow .md cross-references before implementing",
			"- Always read pi .md files completely and follow links to related docs (e.g., tui.md for TUI API details)",
			"",
			"# Project Context",
			"Project-specific instructions and guidelines:",
			"services:",
			"  gateway:",
			"    workers: 4",
			"def check_health():",
			"    if True:",
			"        return 200",
			"",
			"<available_skills>",
			"  <skill>",
			"    <name>code-review</name>",
			"    <location>" + skillPath + "</location>",
			"  </skill>",
			"</available_skills>",
			"",
			"Current working directory: /workspace/demo",
		}, "\n")

		pruned := PruneClientSystemPrompt(rawSys, nil, nil)
		for _, forbidden := range []string{
			"operating inside pi",
			"coding agent harness",
			"You help users by reading files",
			"Pi documentation (",
			"Main documentation:",
			"Additional docs:",
			"When working on pi topics",
			"Always read pi .md files",
			"\n\n\n",
		} {
			if strings.Contains(pruned, forbidden) {
				t.Fatalf("expected %q pruned from vanilla Pi prompt, got:\n%s", forbidden, pruned)
			}
		}
		for _, mustKeep := range []string{
			"Available tools:\n- read: Read file contents",
			"Guidelines:\n- Use bash for file operations like ls, rg, find",
			"services:\n  gateway:\n    workers: 4",
			"def check_health():\n    if True:\n        return 200",
			"<location>" + skillPath + "</location>",
			"Current working directory: /workspace/demo",
		} {
			if !strings.Contains(pruned, mustKeep) {
				t.Fatalf("expected %q preserved verbatim in vanilla Pi prompt, got:\n%s", mustKeep, pruned)
			}
		}

		codec := NewReversibleSessionCodec("pi-vanilla-session")
		encoded := codec.EncodeOutboundText(pruned)
		if strings.Contains(encoded, "@mariozechner") || strings.Contains(encoded, "pi-coding-agent") {
			t.Fatalf("expected @mariozechner/pi-coding-agent reversibly encoded outbound, got:\n%s", encoded)
		}
		if !strings.Contains(encoded, "wsseg_") {
			t.Fatalf("expected wsseg_ token in encoded output, got:\n%s", encoded)
		}
		if restored := codec.RestoreText(encoded); restored != pruned {
			t.Fatalf("RestoreText mismatch:\ngot:  %q\nwant: %q", restored, pruned)
		}
		toolArgs := map[string]any{"file_path": strings.ReplaceAll(skillPath, "@mariozechner/pi-coding-agent", encoded[strings.Index(encoded, "wsseg_"):strings.Index(encoded, "wsseg_")+14])}
		restoredArgs := codec.RestoreMap(toolArgs)
		if restoredArgs["file_path"] != skillPath {
			t.Fatalf("RestoreMap file_path = %q, want %q", restoredArgs["file_path"], skillPath)
		}
	})

	t.Run("earendil_works_pi_coding_agent_xml_sections", func(t *testing.T) {
		skillPath := "/home/dev/.local/share/npm/lib/node_modules/@earendil-works/pi-coding-agent/skills/audit/SKILL.md"
		rawSys := strings.Join([]string{
			"You are an expert coding assistant operating inside pi, a coding agent harness. You help users by reading files, executing commands, editing code, and writing new files.",
			"",
			"<tools>",
			"- read: Read file contents",
			"- bash: Execute bash commands",
			"",
			"In addition to the tools above, you may have access to other custom tools depending on the project.",
			"</tools>",
			"",
			"<rules>",
			"- Use bash for file operations like ls, rg, find",
			"- Be concise in your responses",
			"- Show file paths clearly when working with files",
			"</rules>",
			"",
			"<docs>",
			"Pi documentation (read only when the user asks about pi itself, its SDK, extensions, themes, skills, or TUI):",
			"- Main documentation: /home/dev/.local/share/npm/lib/node_modules/@earendil-works/pi-coding-agent/README.md",
			"- Additional docs: /home/dev/.local/share/npm/lib/node_modules/@earendil-works/pi-coding-agent/docs",
			"- Examples: /home/dev/.local/share/npm/lib/node_modules/@earendil-works/pi-coding-agent/examples (extensions, custom tools, SDK)",
			"- When reading pi docs or examples, resolve docs/... under Additional docs and examples/... under Examples, not the current working directory",
			"- When asked about: extensions (docs/extensions.md, examples/extensions/), themes (docs/themes.md), skills (docs/skills.md), prompt templates (docs/prompt-templates.md), TUI components (docs/tui.md), keybindings (docs/keybindings.md), SDK integrations (docs/sdk.md), custom providers (docs/custom-provider.md), adding models (docs/models.md), pi packages (docs/packages.md), environment variables (docs/environment-variables.md), MCP servers (docs/mcp.md), codemode scripts and non-LLM models such as classifiers and image models (docs/codemode.md)",
			"- When working on pi topics, read the docs and examples, and follow .md cross-references before implementing",
			"- Always read pi .md files completely and follow links to related docs (e.g., tui.md for TUI API details)",
			"</docs>",
			"",
			"<project_context>",
			"Project-specific instructions and guidelines:",
			"",
			`<project_instructions path="/workspace/app/AGENTS.md">`,
			"pipeline:",
			"  lint:",
			"    strict: true",
			"def verify():",
			"    return True",
			"</project_instructions>",
			"</project_context>",
			"",
			"<skills>",
			"  <skill>" + skillPath + "</skill>",
			"</skills>",
			"",
			"<cwd>",
			"/workspace/app",
			"</cwd>",
		}, "\n")

		pruned := PruneClientSystemPrompt(rawSys, nil, nil)
		for _, forbidden := range []string{
			"operating inside pi",
			"You help users by reading files",
			"Pi documentation (",
			"When reading pi docs or examples",
			"<docs>",
			"</docs>",
			"\n\n\n",
		} {
			if strings.Contains(pruned, forbidden) {
				t.Fatalf("expected %q pruned (including empty <docs></docs> wrapper), got:\n%s", forbidden, pruned)
			}
		}
		for _, mustKeep := range []string{
			"<tools>\n- read: Read file contents",
			"</tools>",
			"<rules>\n- Use bash for file operations like ls, rg, find",
			"</rules>",
			"pipeline:\n  lint:\n    strict: true\ndef verify():\n    return True",
			"<skills>\n  <skill>" + skillPath + "</skill>\n</skills>",
			"<cwd>\n/workspace/app\n</cwd>",
		} {
			if !strings.Contains(pruned, mustKeep) {
				t.Fatalf("expected %q preserved intact, got:\n%s", mustKeep, pruned)
			}
		}

		codec := NewReversibleSessionCodec("pi-earendil-session")
		encoded := codec.EncodeOutboundText(pruned)
		if strings.Contains(encoded, "@earendil-works") || strings.Contains(encoded, "pi-coding-agent") {
			t.Fatalf("expected @earendil-works/pi-coding-agent reversibly encoded outbound, got:\n%s", encoded)
		}
		for step := 1; step <= 14; step++ {
			sr := codec.NewStreamRestorer()
			var sb strings.Builder
			for i := 0; i < len(encoded); i += step {
				end := i + step
				if end > len(encoded) {
					end = len(encoded)
				}
				sb.WriteString(sr.Push(encoded[i:end]))
			}
			sb.WriteString(sr.Flush())
			if sb.String() != pruned {
				t.Fatalf("step=%d stream restore mismatch:\ngot:  %q\nwant: %q", step, sb.String(), pruned)
			}
		}

		// Legitimate user <docs> block must NEVER be deleted.
		userDocsSys := "<docs>\nInternal Billing Service Reference:\n- Main endpoint: POST /v1/invoices\n</docs>\n<cwd>/workspace/app</cwd>"
		if gotUserDocs := PruneClientSystemPrompt(userDocsSys, nil, nil); gotUserDocs != userDocsSys {
			t.Fatalf("user <docs> block must survive untouched:\ngot:  %q\nwant: %q", gotUserDocs, userDocsSys)
		}
	})
}

func TestGenericUnknownHarnessIdentityDocsOrderedListAndToolCodec(t *testing.T) {
	// 1. Unknown harness ("FooBot") identity + capability tail + self-doc block + @acme/foobot-coding-agent reversible path + <t3_code_*> tag
	fooSkillPath := "/opt/node_modules/@acme/foobot-coding-agent/skills/deploy/SKILL.md"
	rawFoo := strings.Join([]string{
		"You are FooBot coding agent harness. You help users by reading files, executing commands, editing code, and writing new files.",
		"You are pi, a coding agent harness.",
		"You are foobot, a coding assistant.",
		"",
		"<docs>",
		"FooBot documentation (read only when the user asks about foobot itself, its SDK, extensions, themes, skills, or TUI):",
		"- Main documentation: /opt/node_modules/@acme/foobot-coding-agent/README.md",
		"- Additional docs: /opt/node_modules/@acme/foobot-coding-agent/docs",
		"- Examples: /opt/node_modules/@acme/foobot-coding-agent/examples",
		"- When reading foobot docs or examples, resolve under Additional docs",
		"- When asked about: extensions (docs/extensions.md), plugins (docs/plugins.md), SDK (docs/sdk.md)",
		"- When working on foobot topics, read the docs and examples before implementing",
		"- Always read foobot .md files completely",
		"</docs>",
		"",
		"<t3_code_instructions>Keep strict error handling and inspect " + fooSkillPath + "</t3_code_instructions>",
	}, "\n")

	prunedFoo := PruneClientSystemPrompt(rawFoo, nil, nil)
	for _, forbidden := range []string{
		"FooBot coding agent harness",
		"You are pi",
		"You are foobot",
		"You help users by reading files",
		"FooBot documentation",
		"Main documentation:",
		"<docs>",
		"</docs>",
	} {
		if strings.Contains(prunedFoo, forbidden) {
			t.Fatalf("expected generic harness boilerplate %q pruned, got:\n%s", forbidden, prunedFoo)
		}
	}
	wantPrunedFoo := "<t3_code_instructions>Keep strict error handling and inspect " + fooSkillPath + "</t3_code_instructions>"
	if prunedFoo != wantPrunedFoo {
		t.Fatalf("prunedFoo =\n%q\nwant:\n%q", prunedFoo, wantPrunedFoo)
	}

	codec := NewReversibleSessionCodec("generic-foobot-tenant")
	encFoo := codec.EncodeOutboundText(prunedFoo)
	if strings.Contains(encFoo, "t3_code_instructions") || strings.Contains(encFoo, "@acme/foobot-coding-agent") || strings.Contains(encFoo, "coding-agent") {
		t.Fatalf("expected <t3_code_instructions> and @acme/foobot-coding-agent reversibly encoded, got: %q", encFoo)
	}
	if !strings.Contains(encFoo, "wstag_") || !strings.Contains(encFoo, "wsseg_") {
		t.Fatalf("expected both wstag_ and wsseg_ aliases in %q", encFoo)
	}
	if decFoo := codec.RestoreText(encFoo); decFoo != wantPrunedFoo {
		t.Fatalf("RestoreText(encFoo) = %q, want %q", decFoo, wantPrunedFoo)
	}

	// 2. Ordered-list prefix handling: no orphan "1." / "2)" on full removal, prefix kept on mixed, untouched verbatim!
	orderedCases := []struct {
		name, in, want string
	}{
		{"orphan_dot_removed", "1. You are Claude Code.", ""},
		{"orphan_paren_removed", "head\n2) You are Codex.\ntail", "head\ntail"},
		{"mixed_ordered_keeps_prefix", "head\n  1. You are Codex.   Keep this rule.  \ntail", "head\n  1. Keep this rule.\ntail"},
		{"untouched_ordered_verbatim", "head\n  1. First   item  \n  2. Second\titem  \ntail", "head\n  1. First   item  \n  2. Second\titem  \ntail"},
	}
	for _, tc := range orderedCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := PruneClientSystemPrompt(tc.in, nil, nil); got != tc.want {
				t.Fatalf("got %q; want %q", got, tc.want)
			}
		})
	}

	// 3. Tool identifier neutralization: __pi_deferred_placeholder__ neutralized while normal "pi" words stay intact.
	toolChecks := map[string]string{
		"__pi_deferred_placeholder__":     "__ws_deferred_placeholder__",
		"__foobot_deferred_placeholder__": "__ws_deferred_placeholder__",
		"__ws_deferred_placeholder__":     "__ws_deferred_placeholder__",
		"pi_coding_agent_exec":            "ws_coding_agent_exec",
		"pipe":                            "pipe",
		"copy":                            "copy",
		"api":                             "api",
		"api_call":                        "api_call",
		"topic":                           "topic",
		"ping":                            "ping",
		"pin_item":                        "pin_item",
		"pip_install":                     "pip_install",
		"kpi_metrics":                     "kpi_metrics",
	}
	for in, want := range toolChecks {
		if got := NeutralizeToolIdentifier(in); got != want {
			t.Fatalf("NeutralizeToolIdentifier(%q) = %q, want %q", in, got, want)
		}
	}

	// 4. Standalone pi-coding-agent path & stem-gate coverage.
	standalonePath := "/opt/tools/pi-coding-agent/README.md"
	if !hasSensitivePathPhraseStem(standalonePath) {
		t.Fatalf("hasSensitivePathPhraseStem missed %q", standalonePath)
	}
	encStandalone := codec.EncodeOutboundText(standalonePath)
	if strings.Contains(encStandalone, "pi-coding-agent") || !strings.Contains(encStandalone, "wsseg_") {
		t.Fatalf("expected standalone pi-coding-agent encoded as wsseg_*, got %q", encStandalone)
	}
	if dec := codec.RestoreText(encStandalone); dec != standalonePath {
		t.Fatalf("RestoreText(%q) = %q, want %q", encStandalone, dec, standalonePath)
	}
}

func TestAdjacentParagraphCapabilityFillerAndSelfDocSubItems(t *testing.T) {
	// Adjacent-line capability filler after pruned identity is absorbed.
	twoLineOpener := strings.Join([]string{
		"You are pi, a coding agent harness.",
		"You help users by reading files, executing commands, editing code, and writing new files.",
		"",
		"Always run go test -race.",
	}, "\n")
	if got := PruneClientSystemPrompt(twoLineOpener, nil, nil); got != "Always run go test -race." {
		t.Fatalf("two-line opener got %q, want %q", got, "Always run go test -race.")
	}

	// Standalone capability sentence after a user persona must NEVER be pruned.
	userPersona := strings.Join([]string{
		"You are a senior Go engineer.",
		"You help users by reading files, executing commands, editing code, and writing new files.",
	}, "\n")
	if got := PruneClientSystemPrompt(userPersona, nil, nil); got != userPersona {
		t.Fatalf("user persona + capability line must stay untouched:\ngot:  %q\nwant: %q", got, userPersona)
	}

	// Harness self-doc block with internal blank line, "When asked to create...", and indented sub-bullets.
	selfDocNested := strings.Join([]string{
		"<docs>",
		"FooBot documentation (read only when the user asks about foobot itself, its SDK, extensions, or CLI):",
		"",
		"- Main documentation: /opt/foobot/README.md",
		"- Examples: /opt/foobot/examples",
		"  - extensions: docs/extensions.md",
		"  - themes: docs/themes.md",
		"- When asked to create extensions, themes, or plugins, read the relevant docs first",
		"</docs>",
		"",
		"Guidelines:",
		"- Keep diffs minimal",
	}, "\n")
	wantNested := "Guidelines:\n- Keep diffs minimal"
	if got := PruneClientSystemPrompt(selfDocNested, nil, nil); got != wantNested {
		t.Fatalf("nested self-doc got:\n%q\nwant:\n%q", got, wantNested)
	}
}

func TestToolsOpenCodeSemanticRewrite(t *testing.T) {
	cases := []struct {
		name  string
		in    string
		want  string
		noSub string
	}{
		{
			name:  "basic_session_move",
			in:    `Call tools.opencode.session_move(id) to migrate state.`,
			want:  `Call tools["opencode"].session_move(id) to migrate state.`,
			noSub: `tools.opencode.`,
		},
		{
			name:  "mixed_prose_and_code",
			in:    `OpenCode invokes tools.opencode.session_move and Tools.OpenCode.run().`,
			want:  `the workspace agent invokes tools["opencode"].session_move and Tools["OpenCode"].run().`,
			noSub: `tools.opencode.`,
		},
		{
			name:  "url_preserved",
			in:    `Docs at https://example.com/tools.opencode.js and code tools.opencode.list()`,
			want:  `Docs at https://example.com/tools.opencode.js and code tools["opencode"].list()`,
			noSub: `tools.opencode.list`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := SanitizeOutboundText(tc.in, nil)
			if got != tc.want {
				t.Fatalf("SanitizeOutboundText(%q)\n got: %q\nwant: %q", tc.in, got, tc.want)
			}
			if strings.Contains(strings.ToLower(got), strings.ToLower(tc.noSub)) {
				t.Fatalf("expected %q to be neutralized, got %q", tc.noSub, got)
			}
			if strings.ContainsRune(got, '\u200b') {
				t.Fatalf("must NOT insert ZWSP U+200B, got %q", got)
			}
		})
	}
}
