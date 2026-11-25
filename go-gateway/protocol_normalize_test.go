package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestExtractCwdFromSystemXMLTagAndPrecedence(t *testing.T) {
	base := t.TempDir()
	earendilDir := filepath.Join(base, "earendil-pi-workspace")
	inlineDir := filepath.Join(base, "inline-cwd-workspace")
	msgDir := filepath.Join(base, "msg-cwd-workspace")
	primaryDir := filepath.Join(base, "primary-workspace")
	for _, d := range []string{earendilDir, inlineDir, msgDir, primaryDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("earendil_pi_multiline_xml_cwd_after_prune", func(t *testing.T) {
		rawSys := fmt.Sprintf(`You are an expert coding assistant operating inside pi, a coding agent harness. You help users by reading files, executing commands, editing code, and writing new files.

<tools>
- read: Read file contents
- bash: Execute bash commands
</tools>

<rules>
- Be concise in your responses
</rules>

<docs>
Pi documentation (read only when the user asks about pi itself, its SDK, extensions, themes, skills, or TUI):
- Main documentation: /usr/local/lib/node_modules/@earendil-works/pi-coding-agent/README.md
- Additional docs: /usr/local/lib/node_modules/@earendil-works/pi-coding-agent/docs
</docs>

<cwd>
%s
</cwd>`, earendilDir)
		pruned := PruneClientSystemPrompt(rawSys)
		if got := extractCwdFromSystem(pruned, nil); got != earendilDir {
			t.Fatalf("extractCwdFromSystem(pruned) = %q, want %q (pruned=%q)", got, earendilDir, pruned)
		}
	})

	t.Run("inline_and_uppercase_xml_cwd", func(t *testing.T) {
		sys := fmt.Sprintf("<CWD>  %s  </CWD>", inlineDir)
		if got := extractCwdFromSystem(sys, nil); got != inlineDir {
			t.Fatalf("got %q, want %q", got, inlineDir)
		}
	})

	t.Run("xml_cwd_in_initial_user_message", func(t *testing.T) {
		msgs := []map[string]any{
			{"role": "user", "content": fmt.Sprintf("<cwd>\n%s\n</cwd>\nRun tests.", msgDir)},
		}
		if got := extractCwdFromSystem("Follow repository guidelines.", msgs); got != msgDir {
			t.Fatalf("got %q, want %q", got, msgDir)
		}
	})

	t.Run("nonexistent_xml_cwd_falls_back_to_default", func(t *testing.T) {
		missing := filepath.Join(base, "does-not-exist")
		if got := extractCwdFromSystem("<cwd>"+missing+"</cwd>", nil); got != DefaultCwd {
			t.Fatalf("got %q, want DefaultCwd %q", got, DefaultCwd)
		}
	})

	t.Run("primary_working_directory_precedence_and_fallthrough", func(t *testing.T) {
		sys := fmt.Sprintf("Primary working directory: %s\n<cwd>\n%s\n</cwd>", primaryDir, earendilDir)
		if got := extractCwdFromSystem(sys, nil); got != primaryDir {
			t.Fatalf("expected Primary working directory %q to win, got %q", primaryDir, got)
		}
		if err := os.Remove(primaryDir); err != nil {
			t.Fatal(err)
		}
		if got := extractCwdFromSystem(sys, nil); got != earendilDir {
			t.Fatalf("expected fallthrough to <cwd> %q after primaryDir removal, got %q", earendilDir, got)
		}
	})
}
