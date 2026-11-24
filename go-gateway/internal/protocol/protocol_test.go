package protocol

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
