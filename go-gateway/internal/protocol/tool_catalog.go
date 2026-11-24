package protocol

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"agy-acp-gateway-go/internal/cleanroom"
	gwcfg "agy-acp-gateway-go/internal/config"
)

// ToolDef describes one client-declared host tool and its MCP bridge alias.
type ToolDef struct {
	Name          string         `json:"name"`
	OutboundAlias string         `json:"outboundAlias,omitempty"`
	McpName       string         `json:"mcpName"`
	Description   string         `json:"description"`
	Parameters    map[string]any `json:"parameters"`
}

// OutboundName returns the sanitized identifier exposed to the upstream model.
func (t ToolDef) OutboundName() string {
	if t.OutboundAlias != "" {
		return t.OutboundAlias
	}
	return cleanroom.NeutralizeToolIdentifier(t.Name)
}

// ToolCallItem captures one model-initiated tool invocation.
type ToolCallItem struct {
	ID    string         `json:"id"`
	Name  string         `json:"name"`
	Input map[string]any `json:"input"`
}

// ToolResultInput delivers host tool execution output (plus optional user attachments) back into a parked ACP turn.
type ToolResultInput struct {
	ToolCallID         string
	Content            string
	SystemTail         string
	Images             []map[string]any
	IsError            bool
	TrailingUserText   string
	TrailingUserImages []map[string]any
}

const (
	toolDeltaMaxChars   = 20000
	systemDeltaMaxChars = 20000
)

var mcpToolNameRe = regexp.MustCompile(`[^a-zA-Z0-9_-]`)

// McpToolNameFromNeutral computes the neutral MCP bridge identifier from an already-neutralized name.
func McpToolNameFromNeutral(hostName, neutral string) string {
	clean := mcpToolNameRe.ReplaceAllString(neutral, "_")
	if len(clean) > 40 {
		clean = clean[:40]
	}
	return fmt.Sprintf("ws_%s_%s", clean, gwcfg.ShortSHA256(hostName)[:12])
}

// McpToolName computes the neutral MCP bridge identifier for hostName.
func McpToolName(hostName string) string {
	return McpToolNameFromNeutral(hostName, cleanroom.NeutralizeToolIdentifier(hostName))
}

// ToolCatalogFingerprintAndDigests computes both the catalog fingerprint and sorted per-tool digests in one pass.
func ToolCatalogFingerprintAndDigests(cat []ToolDef) (string, []string) {
	if len(cat) == 0 {
		return gwcfg.ShortSHA256(""), nil
	}
	parts := make([]string, 0, len(cat))
	for _, t := range cat {
		parts = append(parts, ToolDefDigest(t))
	}
	sort.Strings(parts)
	return gwcfg.ShortSHA256(strings.Join(parts, "\n")), parts
}

// ToolCatalogFingerprint hashes name, description and normalized schema of every tool.
func ToolCatalogFingerprint(cat []ToolDef) string {
	fp, _ := ToolCatalogFingerprintAndDigests(cat)
	return fp
}

// LegacyToolFingerprint computes the legacy names-only fingerprint for persisted sessions.
func LegacyToolFingerprint(cat []ToolDef) string {
	names := make([]string, 0, len(cat))
	for _, t := range cat {
		names = append(names, t.Name)
	}
	return gwcfg.ShortSHA256(strings.Join(names, ","))
}

// ToolDefDigest produces a deterministic digest string for one ToolDef.
func ToolDefDigest(t ToolDef) string {
	schema, _ := json.Marshal(t.Parameters)
	return t.Name + "\x00" + t.Description + "\x00" + string(schema)
}

// SessionBridgeServerName returns the active bridge server identifier used by cat.
func SessionBridgeServerName(cat []ToolDef) string {
	for _, t := range cat {
		if strings.HasPrefix(t.McpName, "dsh_") {
			return gwcfg.LegacyMcpBridgeName
		}
		if strings.HasPrefix(t.McpName, "ws_") {
			return gwcfg.McpBridgeName
		}
	}
	return gwcfg.McpBridgeName
}

// SessionHelperMcpName returns the MCP name of the await or call helper in cat.
func SessionHelperMcpName(cat []ToolDef, wantAwait bool) string {
	isLegacy := SessionBridgeServerName(cat) == gwcfg.LegacyMcpBridgeName
	for _, t := range cat {
		if t.McpName == "" {
			continue
		}
		if isLegacy && !strings.HasPrefix(t.McpName, "dsh_") {
			continue
		}
		if !isLegacy && !strings.HasPrefix(t.McpName, "ws_") {
			continue
		}
		if wantAwait && (t.Name == gwcfg.AwaitToolName || t.Name == gwcfg.LegacyAwaitToolName) {
			return t.McpName
		}
		if !wantAwait && (t.Name == gwcfg.CallToolName || t.Name == gwcfg.LegacyCallToolName) {
			return t.McpName
		}
	}
	if isLegacy {
		if wantAwait {
			return fmt.Sprintf("dsh_%s_%s", gwcfg.LegacyAwaitToolName, gwcfg.ShortSHA256(gwcfg.LegacyAwaitToolName)[:12])
		}
		return fmt.Sprintf("dsh_%s_%s", gwcfg.LegacyCallToolName, gwcfg.ShortSHA256(gwcfg.LegacyCallToolName)[:12])
	}
	if wantAwait {
		return McpToolName(gwcfg.AwaitToolName)
	}
	return McpToolName(gwcfg.CallToolName)
}

// StripBridgeMcpPrefix removes "mcp_<bridge>_" prefix when present.
func StripBridgeMcpPrefix(raw string) string {
	switch {
	case strings.HasPrefix(raw, "mcp_"+gwcfg.McpBridgeName+"_"):
		return strings.TrimPrefix(raw, "mcp_"+gwcfg.McpBridgeName+"_")
	case strings.HasPrefix(raw, "mcp_"+gwcfg.LegacyMcpBridgeName+"_"):
		return strings.TrimPrefix(raw, "mcp_"+gwcfg.LegacyMcpBridgeName+"_")
	default:
		return raw
	}
}

// DiffToolCatalogs compares two catalogs by host tool name, ignoring internal bridge helpers.
func DiffToolCatalogs(oldCat, newCat []ToolDef) (added, removed, changed []string) {
	oldBy := make(map[string]string, len(oldCat))
	for _, t := range oldCat {
		if !gwcfg.IsInternalBridgeTool(t.Name) {
			oldBy[t.Name] = ToolDefDigest(t)
		}
	}
	newBy := make(map[string]bool, len(newCat))
	for _, t := range newCat {
		if gwcfg.IsInternalBridgeTool(t.Name) {
			continue
		}
		newBy[t.Name] = true
		prev, ok := oldBy[t.Name]
		switch {
		case !ok:
			added = append(added, t.Name)
		case prev != ToolDefDigest(t):
			changed = append(changed, t.Name)
		}
	}
	for name := range oldBy {
		if !newBy[name] {
			removed = append(removed, name)
		}
	}
	sort.Strings(added)
	sort.Strings(removed)
	sort.Strings(changed)
	return added, removed, changed
}

// ToolCatalogNeedsRollover reports whether a session must roll over to serve newCat.
func ToolCatalogNeedsRollover(hasBridge bool, oldCat []ToolDef, oldFp string, newCat []ToolDef) bool {
	if oldCat == nil {
		if oldFp == "" {
			return len(newCat) > 0
		}
		return oldFp != ToolCatalogFingerprint(newCat) && oldFp != LegacyToolFingerprint(newCat)
	}
	if !hasBridge && len(newCat) > 0 {
		return true
	}
	return false
}

// BuildToolDeltaNotice describes added, changed, and removed tools for prompt-tail injection.
func BuildToolDeltaNotice(oldCat, newCat []ToolDef, declared map[string]bool) (notice string, added, removed, changed []string) {
	added, removed, changed = DiffToolCatalogs(oldCat, newCat)
	if len(added) == 0 && len(removed) == 0 && len(changed) == 0 {
		return "", nil, nil, nil
	}
	newByName := make(map[string]ToolDef, len(newCat))
	for _, t := range newCat {
		newByName[t.Name] = t
	}
	refCat := oldCat
	if len(refCat) == 0 {
		refCat = newCat
	}
	bridgeSrv := SessionBridgeServerName(refCat)
	mcpPrefix := "mcp_" + bridgeSrv + "_"
	callHelperMcp := mcpPrefix + SessionHelperMcpName(refCat, false)

	var sections []string
	if len(removed) > 0 {
		outRemoved := make([]string, len(removed))
		for i, rn := range removed {
			outRemoved[i] = cleanroom.NeutralizeToolIdentifier(rn)
		}
		sections = append(sections, fmt.Sprintf("These tools are no longer available in this session and calls to them will fail: %s.", strings.Join(outRemoved, ", ")))
	}
	if len(added) > 0 {
		var dynamicLines, reEnabledLines []string
		for _, name := range added {
			t := newByName[name]
			outName := t.OutboundName()
			schemaBytes, _ := json.Marshal(t.Parameters)
			if declared[name] {
				reEnabledLines = append(reEnabledLines, fmt.Sprintf("- %s (%s%s): %s | parameters: %s", outName, mcpPrefix, t.McpName, t.Description, string(schemaBytes)))
			} else {
				dynamicLines = append(dynamicLines, fmt.Sprintf("- %s: %s | parameters: %s", outName, t.Description, string(schemaBytes)))
			}
		}
		if len(dynamicLines) > 0 {
			sections = append(sections, fmt.Sprintf("Newly available tools in this session (invoke via %s with {\"name\": \"<tool_name>\", \"arguments\": {...}}):\n%s", callHelperMcp, strings.Join(dynamicLines, "\n")))
		}
		if len(reEnabledLines) > 0 {
			sections = append(sections, "Re-enabled tools in this session:\n"+strings.Join(reEnabledLines, "\n"))
		}
	}
	if len(changed) > 0 {
		var changedLines []string
		for _, name := range changed {
			t := newByName[name]
			outName := t.OutboundName()
			schemaBytes, _ := json.Marshal(t.Parameters)
			via := mcpPrefix + t.McpName
			if !declared[name] {
				via = callHelperMcp
			}
			changedLines = append(changedLines, fmt.Sprintf("- %s (%s): %s | parameters: %s", outName, via, t.Description, string(schemaBytes)))
		}
		sections = append(sections, "Updated tool definitions in this session:\n"+strings.Join(changedLines, "\n"))
	}

	out := "[Host tool update] " + strings.Join(sections, "\n")
	if len(out) > toolDeltaMaxChars {
		out = gwcfg.TruncateRunes(out, toolDeltaMaxChars)
	}
	return out, added, removed, changed
}

// ResolveAllowedHostTool maps an MCP alias from agy to the host tool name if declared.
func ResolveAllowedHostTool(mcpToHost map[string]string, rawName string) (string, bool) {
	if h, ok := mcpToHost[rawName]; ok {
		return h, true
	}
	trimmed := StripBridgeMcpPrefix(rawName)
	if h, ok := mcpToHost[trimmed]; ok {
		return h, true
	}
	return "", false
}

// AllowedHostToolName validates a host tool name requested through bridge call helpers.
func AllowedHostToolName(mcpToHost map[string]string, requested string, catalogs ...[]ToolDef) (string, bool) {
	requested = strings.TrimSpace(StripBridgeMcpPrefix(strings.TrimSpace(requested)))
	if requested == "" {
		return "", false
	}
	if h, ok := mcpToHost[requested]; ok {
		requested = h
	}
	if gwcfg.IsInternalBridgeTool(requested) {
		return "", false
	}
	for _, cat := range catalogs {
		for _, t := range cat {
			if gwcfg.IsInternalBridgeTool(t.Name) {
				continue
			}
			if t.Name == requested || t.OutboundName() == requested {
				return t.Name, true
			}
		}
	}
	for _, h := range mcpToHost {
		if gwcfg.IsInternalBridgeTool(h) {
			continue
		}
		if h == requested || cleanroom.NeutralizeToolIdentifier(h) == requested {
			return h, true
		}
	}
	return "", false
}

// SystemPromptDelta describes system instruction changes for prompt-tail injection.
func SystemPromptDelta(oldSys, newSys string, pruneFn func(string) string) string {
	if pruneFn == nil {
		pruneFn = func(s string) string {
			return cleanroom.PruneClientSystemPrompt(s, nil, nil)
		}
	}
	oldP, newP := pruneFn(oldSys), pruneFn(newSys)
	if oldP == newP {
		return ""
	}
	oldLines := map[string]int{}
	for _, l := range strings.Split(oldP, "\n") {
		if l = strings.TrimSpace(l); l != "" {
			oldLines[l]++
		}
	}
	newLines := map[string]int{}
	var added []string
	for _, l := range strings.Split(newP, "\n") {
		if l = strings.TrimSpace(l); l == "" {
			continue
		}
		newLines[l]++
		if newLines[l] > oldLines[l] {
			added = append(added, l)
		}
	}
	var removed []string
	seen := map[string]int{}
	for _, l := range strings.Split(oldP, "\n") {
		if l = strings.TrimSpace(l); l == "" {
			continue
		}
		seen[l]++
		if seen[l] > newLines[l] {
			removed = append(removed, l)
		}
	}
	if len(added) == 0 && len(removed) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("[Host instructions updated] The workspace context changed since the session started. Where they differ, these lines take precedence over the earlier workspace context.")
	if len(added) > 0 {
		b.WriteString("\nAdded or changed:\n")
		b.WriteString(strings.Join(added, "\n"))
	}
	if len(removed) > 0 {
		b.WriteString("\nNo longer in effect:\n")
		b.WriteString(strings.Join(removed, "\n"))
	}
	out := b.String()
	if len(out) > systemDeltaMaxChars {
		out = "[Host instructions updated] The full current workspace context follows and replaces the earlier one.\n" + gwcfg.TruncateRunes(newP, systemDeltaMaxChars)
	}
	return out
}
