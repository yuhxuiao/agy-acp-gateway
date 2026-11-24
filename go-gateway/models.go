package main

import (
	gwcfg "agy-acp-gateway-go/internal/config"
	"agy-acp-gateway-go/internal/protocol"
)

type ModelPricing = protocol.ModelPricing
type ModelInfo = protocol.ModelInfo
type ModelSelection = protocol.ModelSelection
type PricedUsage = protocol.PricedUsage

var (
	AgyModels      = protocol.AgyModels
	RetiredModels  = protocol.RetiredModels
	UnknownModel   = protocol.UnknownModel
	DefaultModelID = protocol.DefaultModelID
)

func effectiveModelMaxImages(m *ModelInfo) int {
	return protocol.EffectiveModelMaxImages(m, MaxImagesGemini, MaxImagesClaude)
}

func envPosInt(k string, def int) int {
	return gwcfg.EnvInt(k, def)
}

func syncModelRegistryConfig(s *gwcfg.Snapshot) {
	if s == nil {
		return
	}
	protocol.Configure(protocol.RuntimeSettings{
		RouteClaudeNames: s.RouteClaudeNames,
		ExposeTierModels: s.ExposeTierModels,
		DefaultModelID:   s.DefaultModel,
	})
	DefaultModelID = protocol.DefaultModelID
}

func GetModel(id string) *ModelInfo { return protocol.GetModel(id) }

func ModelOrDefault(id string) *ModelInfo { return protocol.ModelOrDefault(id) }

func ModelFromEnum(e int) (*ModelInfo, string) { return protocol.ModelFromEnum(e) }

func ResolveRequestedModel(requested string, body map[string]any) ModelSelection {
	return protocol.ResolveRequestedModel(requested, body)
}

func clientThinkingTier(body map[string]any) string { return protocol.ClientThinkingTier(body) }

func PriceUsage(m *ModelInfo, inTok, crTok, outTok, thTok, cwTok int) PricedUsage {
	return protocol.PriceUsage(m, inTok, crTok, outTok, thTok, cwTok)
}

func ListModelsForAPI() []map[string]any { return protocol.ListModelsForAPI() }

func BuildGeminiModelResource(id, label string, m *ModelInfo) map[string]any {
	return protocol.BuildGeminiModelResource(id, label, m)
}

func ListGeminiModelsForAPI() []map[string]any { return protocol.ListGeminiModelsForAPI() }
