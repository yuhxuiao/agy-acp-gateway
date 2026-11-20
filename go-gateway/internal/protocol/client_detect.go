package protocol

import (
	"encoding/json"
	"net/http"
	"strings"

	gwcfg "agy-acp-gateway-go/internal/config"
)

// ClientIdentity describes the calling agent harness/client and inbound wire protocol.
type ClientIdentity struct {
	ID       string `json:"id"`
	Label    string `json:"label"`
	Protocol string `json:"protocol"`
	Known    bool   `json:"known"`
}

// DetectInboundProtocol infers the API wire protocol from the HTTP request URL path.
func DetectInboundProtocol(r *http.Request) string {
	if r == nil || r.URL == nil {
		return "Messages"
	}
	p := strings.ToLower(r.URL.Path)
	switch {
	case strings.Contains(p, "/chat/completions"):
		return "Chat"
	case strings.Contains(p, "/responses"):
		return "Responses"
	case strings.Contains(p, "/v1beta/"):
		return "Gemini"
	default:
		return "Messages"
	}
}

// FallbackClientForProtocol returns the generic API client attribution for proto.
func FallbackClientForProtocol(proto string) ClientIdentity {
	switch proto {
	case "Chat":
		return ClientIdentity{ID: "openai_chat", Label: "Chat API", Protocol: "Chat"}
	case "Responses":
		return ClientIdentity{ID: "openai_responses", Label: "Responses API", Protocol: "Responses"}
	case "Gemini":
		return ClientIdentity{ID: "gemini_api", Label: "Gemini API", Protocol: "Gemini"}
	default:
		return ClientIdentity{ID: "anthropic_api", Label: "Messages API", Protocol: "Messages"}
	}
}

// IsKnownSpecificClientID returns true if id identifies a named agent harness rather than a generic API fallback.
func IsKnownSpecificClientID(id string) bool {
	switch id {
	case "", "anthropic_api", "openai_chat", "openai_responses", "gemini_api":
		return false
	default:
		return true
	}
}

func extractRawHostToolNames(rawTools any) []string {
	arr, ok := rawTools.([]any)
	if !ok || len(arr) == 0 {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, item := range arr {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if fn, okF := m["function"].(map[string]any); okF {
			if n, _ := fn["name"].(string); strings.TrimSpace(n) != "" {
				out = append(out, strings.TrimSpace(n))
				continue
			}
		}
		if decls, okD := m["functionDeclarations"].([]any); okD {
			for _, d := range decls {
				if dm, okM := d.(map[string]any); okM {
					if n, _ := dm["name"].(string); strings.TrimSpace(n) != "" {
						out = append(out, strings.TrimSpace(n))
					}
				}
			}
			continue
		}
		if n, _ := m["name"].(string); strings.TrimSpace(n) != "" {
			out = append(out, strings.TrimSpace(n))
		}
	}
	return out
}

func extractToolNamesFromStoredJSON(toolsJSON string) []string {
	if strings.TrimSpace(toolsJSON) == "" {
		return nil
	}
	var defs []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal([]byte(toolsJSON), &defs); err != nil {
		return nil
	}
	out := make([]string, 0, len(defs))
	for _, d := range defs {
		n := strings.TrimSpace(d.Name)
		if n == "" || gwcfg.IsInternalBridgeTool(n) {
			continue
		}
		out = append(out, n)
	}
	return out
}

func isSyntheticProbeKey(keyOrSid string) bool {
	s := strings.ToLower(strings.TrimSpace(keyOrSid))
	s = strings.TrimPrefix(s, "sid:")
	return strings.HasPrefix(s, "e2e-") ||
		strings.HasPrefix(s, "prod-e2e-") ||
		strings.HasPrefix(s, "probe-") ||
		strings.HasPrefix(s, "smoke-") ||
		strings.HasPrefix(s, "quota-probe") ||
		strings.HasPrefix(s, "waf-bisect") ||
		strings.HasPrefix(s, "p19s") ||
		strings.HasPrefix(s, "full-nop19-")
}

func detectClientFromPromptAndTools(convKey, sysCombined string, toolNames []string) (id, label, protoGuess string, ok bool) {
	if isSyntheticProbeKey(convKey) {
		return "probe", "E2E 探针", "", true
	}
	if strings.HasPrefix(convKey, "util:") {
		return "utility", "后台辅助", "", true
	}

	lowSys := strings.ToLower(sysCombined)
	toolSet := make(map[string]bool, len(toolNames))
	hasDshNativeTool := false
	for _, tn := range toolNames {
		low := strings.ToLower(strings.TrimSpace(tn))
		if low == "" || gwcfg.IsInternalBridgeTool(low) {
			continue
		}
		toolSet[low] = true
		if strings.HasPrefix(low, "dsh_") {
			hasDshNativeTool = true
		}
	}

	if strings.Contains(lowSys, "you are codex") ||
		strings.Contains(lowSys, "openai codex") ||
		(strings.Contains(lowSys, "you are the workspace agent, a coding agent") && strings.Contains(lowSys, "skills_instructions")) ||
		(toolSet["exec_command"] && toolSet["write_stdin"]) {
		return "codex", "Codex", "Responses", true
	}

	if strings.Contains(lowSys, "powered by deepseek harness") ||
		strings.Contains(sysCombined, "Tokens prefixed with @ are paths the user explicitly referenced.") ||
		hasDshNativeTool ||
		(toolSet["create_goal"] && toolSet["get_goal"]) {
		return "dsh", "DSH", "Chat", true
	}

	if strings.Contains(lowSys, "# pi 的全局规则") ||
		strings.Contains(lowSys, "# pi 全局规则") ||
		strings.Contains(lowSys, "/.pi/agent/") ||
		strings.Contains(lowSys, `\.pi\agent\`) ||
		strings.Contains(lowSys, "pi-coding-agent") ||
		toolSet["asktool"] ||
		(toolSet["entergoalmode"] && toolSet["taskwait"]) {
		return "pi", "Pi", "Messages", true
	}

	if strings.Contains(lowSys, "ga-platform") ||
		(toolSet["code_run"] && toolSet["file_read"] && toolSet["file_patch"] && (toolSet["update_working_checkpoint"] || toolSet["ws_call_tool"])) {
		return "genericagent", "GenericAgent", "Chat", true
	}
	if strings.Contains(lowSys, "<t3_code_instructions>") {
		return "t3_code", "T3 Code", "Chat", true
	}

	if strings.Contains(lowSys, "openclaw") {
		return "openclaw", "OpenClaw", "Messages", true
	}

	switch {
	case strings.Contains(lowSys, "kilo code") || strings.Contains(lowSys, "kilocode"):
		return "kilo_code", "Kilo Code", "Chat", true
	case strings.Contains(lowSys, "you are roo"):
		return "roo_code", "Roo Code", "Chat", true
	case strings.Contains(lowSys, "you are cline") || (toolSet["attempt_completion"] && toolSet["replace_in_file"]):
		return "cline", "Cline", "Chat", true
	case strings.Contains(lowSys, "you are opencode") || strings.Contains(lowSys, "opencode cli"):
		return "opencode", "OpenCode", "Messages", true
	case (strings.Contains(lowSys, "gemini cli") && strings.Contains(lowSys, "interactive cli agent")) ||
		(toolSet["run_shell_command"] && (toolSet["replace"] || toolSet["save_memory"] || toolSet["write_file"] || toolSet["read_many_files"])):
		return "gemini_cli", "Gemini CLI", "Gemini", true
	}

	if strings.Contains(lowSys, "you are claude code") ||
		strings.Contains(lowSys, "claude agent sdk") ||
		strings.Contains(lowSys, "anthropic's official cli for claude") ||
		(strings.Contains(sysCombined, "# Harness") && strings.Contains(sysCombined, "Text you output outside of tool use")) ||
		toolSet["todowrite"] ||
		toolSet["enterworktree"] ||
		(toolSet["agent"] && toolSet["bash"] && (toolSet["askuserquestion"] || toolSet["croncreate"] || toolSet["reportfindings"])) {
		return "claude_code", "Claude Code", "Messages", true
	}

	return "", "", "", false
}

// DetectClientIdentity identifies the calling client harness BEFORE cleanroom prompt/tool stripping.
func DetectClientIdentity(r *http.Request, body map[string]any, convKey, rawSys, cleanSys string, rawTools any) ClientIdentity {
	proto := DetectInboundProtocol(r)
	if isSyntheticProbeKey(convKey) {
		return ClientIdentity{ID: "probe", Label: "E2E 探针", Protocol: proto, Known: true}
	}
	combinedSys := rawSys
	if cleanSys != "" && cleanSys != rawSys {
		combinedSys = rawSys + "\n" + cleanSys
	}
	promptID, promptLabel, _, promptOK := detectClientFromPromptAndTools(convKey, combinedSys, extractRawHostToolNames(rawTools))

	if r != nil {
		for _, hdr := range []string{"x-claude-code-session-id", "x-session-id", "session-id", "session_id"} {
			if v := strings.TrimSpace(r.Header.Get(hdr)); isSyntheticProbeKey(v) {
				return ClientIdentity{ID: "probe", Label: "E2E 探针", Protocol: proto, Known: true}
			}
		}
		ua := strings.ToLower(strings.TrimSpace(r.Header.Get("User-Agent")))
		xApp := strings.ToLower(strings.TrimSpace(r.Header.Get("x-app")))
		xTitle := strings.ToLower(strings.TrimSpace(r.Header.Get("X-Title")))
		ref := strings.ToLower(strings.TrimSpace(r.Header.Get("HTTP-Referer") + " " + r.Header.Get("Referer")))
		orig := strings.ToLower(strings.TrimSpace(r.Header.Get("originator")))
		goog := strings.ToLower(strings.TrimSpace(r.Header.Get("x-goog-api-client")))

		switch {
		case strings.Contains(ua, "deepseek harness") || strings.Contains(ua, "dsh/") || xApp == "dsh" || strings.Contains(xTitle, "deepseek harness"):
			return ClientIdentity{ID: "dsh", Label: "DSH", Protocol: proto, Known: true}
		case strings.Contains(ua, "pi-coding-agent") || strings.Contains(ua, "pi-desktop") || strings.Contains(xApp, "pi-coding") || strings.Contains(xTitle, "pi-coding") || strings.TrimSpace(r.Header.Get("x-pi-session-id")) != "":
			return ClientIdentity{ID: "pi", Label: "Pi", Protocol: proto, Known: true}
		case strings.Contains(ua, "codex_cli_rs") || strings.Contains(ua, "codex-cli") || strings.Contains(ua, "codex desktop") || strings.Contains(orig, "codex"):
			// Proxy aggregators (e.g. Magpie client-shapes) may inject Originator: codex_exec on /v1/chat/completions
			// while forwarding another agent harness. Prefer explicit prompt/tool identity when it disagrees.
			if promptOK && promptID != "codex" {
				return ClientIdentity{ID: promptID, Label: promptLabel, Protocol: proto, Known: true}
			}
			return ClientIdentity{ID: "codex", Label: "Codex", Protocol: proto, Known: true}
		case strings.Contains(ua, "openclaw") || strings.Contains(xTitle, "openclaw") || strings.Contains(xApp, "openclaw"):
			return ClientIdentity{ID: "openclaw", Label: "OpenClaw", Protocol: proto, Known: true}
		case strings.Contains(ua, "kilocode") || strings.Contains(ua, "kilo-code") || strings.Contains(xTitle, "kilo code"):
			return ClientIdentity{ID: "kilo_code", Label: "Kilo Code", Protocol: proto, Known: true}
		case strings.Contains(ua, "roo-code") || strings.Contains(ua, "roocode") || strings.Contains(xTitle, "roo code") || strings.Contains(ref, "rooveterinaryinc"):
			return ClientIdentity{ID: "roo_code", Label: "Roo Code", Protocol: proto, Known: true}
		case strings.Contains(ua, "cline") || strings.Contains(xTitle, "cline") || strings.Contains(ref, "cline.bot"):
			return ClientIdentity{ID: "cline", Label: "Cline", Protocol: proto, Known: true}
		case strings.Contains(ua, "opencode") || strings.Contains(xTitle, "opencode") || strings.Contains(xApp, "opencode"):
			return ClientIdentity{ID: "opencode", Label: "OpenCode", Protocol: proto, Known: true}
		case strings.Contains(ua, "cursor/") || strings.Contains(xTitle, "cursor"):
			return ClientIdentity{ID: "cursor", Label: "Cursor", Protocol: proto, Known: true}
		case strings.Contains(ua, "gemini-cli") || strings.Contains(goog, "gemini-cli"):
			return ClientIdentity{ID: "gemini_cli", Label: "Gemini CLI", Protocol: proto, Known: true}
		case strings.Contains(ua, "aider") || strings.Contains(xTitle, "aider"):
			return ClientIdentity{ID: "aider", Label: "Aider", Protocol: proto, Known: true}
		case strings.Contains(ua, "cherrystudio") || strings.Contains(xTitle, "cherry studio"):
			return ClientIdentity{ID: "cherry_studio", Label: "Cherry Studio", Protocol: proto, Known: true}
		case strings.Contains(ua, "claude-cli/") || strings.Contains(ua, "claude-code"):
			return ClientIdentity{ID: "claude_code", Label: "Claude Code", Protocol: proto, Known: true}
		}
	}

	if promptOK {
		return ClientIdentity{ID: promptID, Label: promptLabel, Protocol: proto, Known: true}
	}

	if r != nil && strings.TrimSpace(r.Header.Get("x-claude-code-session-id")) != "" {
		return ClientIdentity{ID: "claude_code", Label: "Claude Code", Protocol: proto, Known: true}
	}
	if meta, ok := body["metadata"].(map[string]any); ok {
		if uid, _ := meta["user_id"].(string); strings.Contains(uid, "_account_") && strings.Contains(uid, "_session_") {
			return ClientIdentity{ID: "claude_code", Label: "Claude Code", Protocol: proto, Known: true}
		}
	}

	return FallbackClientForProtocol(proto)
}

// InferHistoricalClientIdentity reconstructs client identity for persisted rows created before client tracking.
func InferHistoricalClientIdentity(convKey, sysText, initToolsJSON, curToolsJSON string) ClientIdentity {
	tools := extractToolNamesFromStoredJSON(initToolsJSON)
	if len(tools) == 0 {
		tools = extractToolNamesFromStoredJSON(curToolsJSON)
	}
	if id, lbl, proto, ok := detectClientFromPromptAndTools(convKey, sysText, tools); ok {
		if proto == "" {
			proto = "Messages"
		}
		return ClientIdentity{ID: id, Label: lbl, Protocol: proto, Known: true}
	}
	return FallbackClientForProtocol("Messages")
}
