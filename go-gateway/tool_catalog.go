package main

import (
	gwcfg "agy-acp-gateway-go/internal/config"
	"agy-acp-gateway-go/internal/protocol"
)

func toolCatalogFingerprint(cat []ToolDef) string { return protocol.ToolCatalogFingerprint(cat) }

func toolCatalogFingerprintAndDigests(cat []ToolDef) (string, []string) {
	return protocol.ToolCatalogFingerprintAndDigests(cat)
}

func legacyToolFingerprint(cat []ToolDef) string { return protocol.LegacyToolFingerprint(cat) }

func toolDefDigest(t ToolDef) string { return protocol.ToolDefDigest(t) }

func isInternalBridgeTool(name string) bool { return gwcfg.IsInternalBridgeTool(name) }

func sessionBridgeServerName(cat []ToolDef) string { return protocol.SessionBridgeServerName(cat) }

func sessionHelperMcpName(cat []ToolDef, wantAwait bool) string {
	return protocol.SessionHelperMcpName(cat, wantAwait)
}

func stripBridgeMcpPrefix(raw string) string { return protocol.StripBridgeMcpPrefix(raw) }

func diffToolCatalogs(oldCat, newCat []ToolDef) (added, removed, changed []string) {
	return protocol.DiffToolCatalogs(oldCat, newCat)
}

func toolCatalogNeedsRollover(hasBridge bool, oldCat []ToolDef, oldFp string, newCat []ToolDef) bool {
	return protocol.ToolCatalogNeedsRollover(hasBridge, oldCat, oldFp, newCat)
}

func buildToolDeltaNotice(oldCat, newCat []ToolDef, declared map[string]bool) (notice string, added, removed, changed []string) {
	return protocol.BuildToolDeltaNotice(oldCat, newCat, declared)
}

func resolveAllowedHostTool(mcpToHost map[string]string, rawName string) (string, bool) {
	return protocol.ResolveAllowedHostTool(mcpToHost, rawName)
}

func allowedHostToolName(mcpToHost map[string]string, requested string, catalogs ...[]ToolDef) (string, bool) {
	return protocol.AllowedHostToolName(mcpToHost, requested, catalogs...)
}

func mcpToolError(text string) map[string]any {
	return map[string]any{
		"content": []map[string]any{{"type": "text", "text": text}},
		"isError": true,
	}
}

func systemPromptDelta(oldSys, newSys string) string {
	return protocol.SystemPromptDelta(oldSys, newSys, PruneClientSystemPrompt)
}
