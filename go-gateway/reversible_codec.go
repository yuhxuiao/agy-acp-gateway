package main

import (
	"agy-acp-gateway-go/internal/cleanroom"
)

func NeutralizeToolIdentifier(name string) string {
	return cleanroom.NeutralizeToolIdentifier(name)
}

type StreamRestorer = cleanroom.StreamRestorer

// ReversibleSessionCodec wraps cleanroom.ReversibleSessionCodec with root ToolDef and conversation seeding helpers.
type ReversibleSessionCodec struct {
	inner *cleanroom.ReversibleSessionCodec
}

func NewReversibleSessionCodec(tenantKey string) *ReversibleSessionCodec {
	return &ReversibleSessionCodec{inner: cleanroom.NewReversibleSessionCodec(tenantKey)}
}

func (c *ReversibleSessionCodec) SetTenantKey(tenantKey string) {
	if c == nil || c.inner == nil {
		return
	}
	c.inner.SetTenantKey(tenantKey)
}

func (c *ReversibleSessionCodec) IsEmpty() bool {
	if c == nil || c.inner == nil {
		return true
	}
	return c.inner.IsEmpty()
}

func (c *ReversibleSessionCodec) Count() int {
	if c == nil || c.inner == nil {
		return 0
	}
	return c.inner.Count()
}

func (c *ReversibleSessionCodec) Lookup(alias string) (string, bool) {
	if c == nil || c.inner == nil {
		return "", false
	}
	return c.inner.Lookup(alias)
}

func (c *ReversibleSessionCodec) EncodeOutboundText(s string) string {
	if c == nil || c.inner == nil {
		return s
	}
	return c.inner.EncodeOutboundText(s)
}

func (c *ReversibleSessionCodec) EncodePromptBlocks(blocks []map[string]any) []map[string]any {
	if c == nil || c.inner == nil {
		return blocks
	}
	return c.inner.EncodePromptBlocks(blocks)
}

func (c *ReversibleSessionCodec) EncodeValue(v any) any {
	if c == nil || c.inner == nil {
		return v
	}
	return c.inner.EncodeValue(v)
}

func (c *ReversibleSessionCodec) EncodeMap(m map[string]any) map[string]any {
	if c == nil || c.inner == nil {
		return m
	}
	return c.inner.EncodeMap(m)
}

func (c *ReversibleSessionCodec) SeedFromToolsCatalog(cat []ToolDef) {
	if c == nil || c.inner == nil || len(cat) == 0 {
		return
	}
	items := make([]cleanroom.ToolSeedItem, len(cat))
	for i, t := range cat {
		items[i] = cleanroom.ToolSeedItem{
			Description: t.Description,
			Parameters:  t.Parameters,
		}
	}
	c.inner.SeedTools(items)
}

func (c *ReversibleSessionCodec) IsToolsFPSeeded(fp string) bool {
	if c == nil || c.inner == nil || fp == "" {
		return false
	}
	return c.inner.IsToolsFPSeeded(fp)
}

func (c *ReversibleSessionCodec) SeedFromToolsCatalogOnce(fp string, cat []ToolDef) {
	if c == nil || c.inner == nil || len(cat) == 0 {
		return
	}
	if fp != "" && c.inner.IsToolsFPSeeded(fp) {
		return
	}
	items := make([]cleanroom.ToolSeedItem, len(cat))
	for i, t := range cat {
		items[i] = cleanroom.ToolSeedItem{
			Description: t.Description,
			Parameters:  t.Parameters,
		}
	}
	c.inner.SeedToolsOnce(fp, items)
}

func (c *ReversibleSessionCodec) SeedFromConversation(sysText string, messages []map[string]any) {
	if c == nil || c.inner == nil {
		return
	}
	texts := make([]string, 0, len(messages))
	for _, m := range messages {
		if txt := formatSingleMessageText(m); txt != "" {
			texts = append(texts, txt)
		}
	}
	c.inner.SeedConversationStrings(sysText, texts, false)
}

func (c *ReversibleSessionCodec) SeedFromConversationOnce(sysText string, messages []map[string]any) {
	if c == nil || c.inner == nil || c.inner.IsHistorySeeded() {
		return
	}
	texts := make([]string, 0, len(messages))
	for _, m := range messages {
		if txt := formatSingleMessageText(m); txt != "" {
			texts = append(texts, txt)
		}
	}
	c.inner.SeedConversationStrings(sysText, texts, true)
}

func (c *ReversibleSessionCodec) IsHistorySeeded() bool {
	if c == nil || c.inner == nil {
		return true
	}
	return c.inner.IsHistorySeeded()
}

func (c *ReversibleSessionCodec) MarkHistorySeeded() {
	if c == nil || c.inner == nil {
		return
	}
	c.inner.MarkHistorySeeded()
}

func (c *ReversibleSessionCodec) RestoreText(s string) string {
	if c == nil || c.inner == nil {
		return s
	}
	return c.inner.RestoreText(s)
}

func (c *ReversibleSessionCodec) RestoreValue(v any) any {
	if c == nil || c.inner == nil {
		return v
	}
	return c.inner.RestoreValue(v)
}

func (c *ReversibleSessionCodec) RestoreMap(m map[string]any) map[string]any {
	if c == nil || c.inner == nil {
		return m
	}
	return c.inner.RestoreMap(m)
}

func (c *ReversibleSessionCodec) NewStreamRestorer() *StreamRestorer {
	var inner *cleanroom.ReversibleSessionCodec
	if c != nil {
		inner = c.inner
	}
	return inner.NewStreamRestorer()
}
