package protocol

import (
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	gwcfg "agy-acp-gateway-go/internal/config"
)

func TestModelRegistryAndPricing(t *testing.T) {
	sel := ResolveRequestedModel("claude-sonnet-4-5-thinking", map[string]any{})
	if sel.Model == nil || sel.Tier != "medium" {
		t.Fatalf("unexpected selection: %+v", sel)
	}
	priced := PriceUsage(GetModel("gemini-3.8-flash"), 1000, 10000, 500, 100, 0)
	if priced.CostUsd <= 0 || priced.SavedUsd <= 0 {
		t.Fatalf("expected positive pricing and savings: %+v", priced)
	}
}

func TestClientIdentityAndToolCatalogDelta(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	req.Header.Set("User-Agent", "claude-cli/2.0")
	id := DetectClientIdentity(req, nil, "sid:test:u:1", "", "", nil)
	if id.ID != "claude_code" || id.Protocol != "Messages" || !id.Known {
		t.Fatalf("unexpected client identity: %+v", id)
	}

	oldCat := []ToolDef{{Name: "Read", McpName: McpToolName("Read"), Description: "read file"}}
	newCat := []ToolDef{
		oldCat[0],
		{Name: "mcp__claude_code__Edit", OutboundAlias: "mcp__ws_agent__Edit", McpName: McpToolName("mcp__claude_code__Edit"), Description: "edit file"},
	}
	if ToolCatalogNeedsRollover(true, oldCat, ToolCatalogFingerprint(oldCat), newCat) {
		t.Fatal("mounted ws-bridge session must not roll over on added tool")
	}
	notice, added, removed, changed := BuildToolDeltaNotice(oldCat, newCat, map[string]bool{"Read": true})
	if len(added) != 1 || len(removed) != 0 || len(changed) != 0 || !strings.Contains(notice, "mcp__ws_agent__Edit") {
		t.Fatalf("unexpected delta notice=%q added=%v", notice, added)
	}
}

func TestMcpToolNameFitsAgy64CharLimit(t *testing.T) {
	validRe := regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)
	cases := []string{
		"",
		"Read",
		strings.Repeat("a", 34), // exact ws-bridge max clean length
		strings.Repeat("b", 35),
		"mcp__ssh-mcp-server__execute-command", // 36 chars (prod failure)
		"mcp__dbx__dbx_execute_redis_command",  // 35 chars
		strings.Repeat("very_long_tool_name_", 5) + "alpha",
		strings.Repeat("very_long_tool_name_", 5) + "beta",
		"中文工具名称_with spaces/and:symbols!@#",
	}
	seenWs := make(map[string]string, len(cases))
	seenDsh := make(map[string]string, len(cases))
	for _, hostName := range cases {
		wsName := McpToolName(hostName)
		fullWs := "mcp_" + gwcfg.McpBridgeName + "_" + wsName
		if len(fullWs) > 64 || !validRe.MatchString(fullWs) {
			t.Fatalf("ws-bridge full tool name invalid for %q: %q (len=%d)", hostName, fullWs, len(fullWs))
		}
		if prev, dup := seenWs[wsName]; dup && prev != hostName {
			t.Fatalf("ws-bridge collision between %q and %q -> %q", prev, hostName, wsName)
		}
		seenWs[wsName] = hostName

		dshName := LegacyDshMcpNameFromWs(wsName)
		fullDsh := "mcp_" + gwcfg.LegacyMcpBridgeName + "_" + dshName
		if len(fullDsh) > 64 || !validRe.MatchString(fullDsh) {
			t.Fatalf("dsh-bridge full tool name invalid for %q: %q (len=%d)", hostName, fullDsh, len(fullDsh))
		}
		if prev, dup := seenDsh[dshName]; dup && prev != hostName {
			t.Fatalf("dsh-bridge collision between %q and %q -> %q", prev, hostName, dshName)
		}
		seenDsh[dshName] = hostName
	}
}
