package protocol

import (
	"encoding/json"
	"regexp"
	"strings"
	"sync"

	gwcfg "agy-acp-gateway-go/internal/config"
)

// ModelPricing stores USD per 1M token weights for quota estimation and billing telemetry.
type ModelPricing struct {
	InPerM            float64 `json:"inPerM"`
	CachedPerM        float64 `json:"cachedPerM"`
	OutPerM           float64 `json:"outPerM"`
	UncachedWritePerM float64 `json:"uncachedWritePerM,omitempty"`
	MidCtxThreshold   int     `json:"midCtxThreshold,omitempty"`
	MidCtxMult        float64 `json:"midCtxMult,omitempty"`
	LongCtxThreshold  int     `json:"longCtxThreshold,omitempty"`
	LongCtxMult       float64 `json:"longCtxMult,omitempty"`
	LongCtxInMult     float64 `json:"longCtxInMult,omitempty"`
	LongCtxCrMult     float64 `json:"longCtxCrMult,omitempty"`
	LongCtxOutMult    float64 `json:"longCtxOutMult,omitempty"`
}

// ModelInfo describes a supported Antigravity model family, context window, and enum mapping.
type ModelInfo struct {
	ID              string         `json:"id"`
	Label           string         `json:"label"`
	Family          string         `json:"family"`
	QuotaGroup      string         `json:"quotaGroup"`
	ContextWindow   int            `json:"contextWindow"`
	TranscriptChars int            `json:"-"`
	MaxImages       int            `json:"maxImages,omitempty"`
	Enums           map[int]string `json:"-"`
	FixedTier       string         `json:"fixedTier,omitempty"`
	SupportsXHigh   bool           `json:"supportsXHigh,omitempty"`
	Pricing         ModelPricing   `json:"pricing"`
}

// RuntimeSettings controls reloadable model routing and listing flags.
type RuntimeSettings struct {
	RouteClaudeNames bool
	ExposeTierModels bool
	DefaultModelID   string
}

var (
	cfgMu            sync.RWMutex
	DefaultModelID   = "gemini-4-carbon"
	routeClaudeNames bool
	exposeTierModels bool

	modelByID   = map[string]*ModelInfo{}
	retiredByID = map[string]*ModelInfo{}
	modelByEnum = map[int]struct {
		m    *ModelInfo
		tier string
	}{}

	tierSuffixRe = regexp.MustCompile(`(?i)[-_](minimal|low|medium|high|xhigh|max|ultra)$`)
	oneMSuffixRe = regexp.MustCompile(`\[1m\]$`)
)

// AgyModels lists the active routable Antigravity models.
var AgyModels []*ModelInfo

// RetiredModels holds historical enum mappings for non-routable deprecated models.
var RetiredModels []*ModelInfo

// UnknownModel is returned when a SQLite step references an unrecognized model enum.
var UnknownModel *ModelInfo

func init() {
	snap := gwcfg.Loaded()
	AgyModels = []*ModelInfo{
		{
			ID: "gemini-4-carbon", Label: "Gemini 4 Carbon", Family: "gemini", QuotaGroup: "gemini",
			ContextWindow: snap.ContextGemini, TranscriptChars: snap.TranscriptCharsGemini, MaxImages: 200,
			Enums:   map[int]string{1427: "high", 1437: "medium", 1438: "low"},
			Pricing: ModelPricing{InPerM: 2.0, CachedPerM: 0.10, OutPerM: 10.0},
		},
		{
			ID: "gemini-4-neon", Label: "Gemini 4 Neon", Family: "gemini", QuotaGroup: "gemini",
			ContextWindow: snap.ContextGemini, TranscriptChars: snap.TranscriptCharsGemini, MaxImages: 200,
			Enums:         map[int]string{1532: "high", 1533: "medium", 1534: "low", 1535: "xhigh"},
			SupportsXHigh: true,
			Pricing:       ModelPricing{InPerM: 1.0, CachedPerM: 0.05, OutPerM: 5.0},
		},
		{
			ID: "gemini-4-argon", Label: "Gemini 4 Argon", Family: "gemini", QuotaGroup: "gemini",
			ContextWindow: snap.ContextGemini, TranscriptChars: snap.TranscriptCharsGemini, MaxImages: 200,
			Enums:   map[int]string{1417: "high", 1418: "medium", 1419: "low"},
			Pricing: ModelPricing{InPerM: 2.0, CachedPerM: 0.10, OutPerM: 10.0},
		},
		{
			ID: "gemini-4-hydrogen", Label: "Gemini 4 Hydrogen", Family: "gemini", QuotaGroup: "gemini",
			ContextWindow: snap.ContextGemini, TranscriptChars: snap.TranscriptCharsGemini, MaxImages: 200,
			Enums:         map[int]string{1525: "high", 1526: "medium", 1527: "low", 1528: "xhigh", 1530: "high", 1531: "xhigh"},
			SupportsXHigh: true,
			Pricing:       ModelPricing{InPerM: 0.50, CachedPerM: 0.025, OutPerM: 2.50},
		},
		{
			ID: "gemini-3.8-flash", Label: "Gemini 3.8 Flash", Family: "gemini", QuotaGroup: "gemini",
			ContextWindow: snap.ContextGemini, TranscriptChars: snap.TranscriptCharsGemini, MaxImages: 200,
			Enums:   map[int]string{1318: "high", 1319: "medium", 1320: "low", 1322: "high"},
			Pricing: ModelPricing{InPerM: 0.25, CachedPerM: 0.025, OutPerM: 1.25},
		},
		{
			ID: "claude-opus-5-5", Label: "Claude Opus 5.5", Family: "claude", QuotaGroup: "3p",
			ContextWindow: snap.ContextClaude, TranscriptChars: snap.TranscriptCharsClaude, MaxImages: 100,
			Enums: map[int]string{1400: "low", 1401: "medium", 1402: "high"},
			Pricing: ModelPricing{
				InPerM: 4.0, UncachedWritePerM: 5.0, CachedPerM: 0.20, OutPerM: 20.0,
			},
		},
		{
			ID: "claude-sonnet-5-5", Label: "Claude Sonnet 5.5", Family: "claude", QuotaGroup: "3p",
			ContextWindow: snap.ContextClaude, TranscriptChars: snap.TranscriptCharsClaude, MaxImages: 100,
			Enums: map[int]string{1403: "low", 1404: "medium", 1405: "high"},
			Pricing: ModelPricing{
				InPerM: 2.0, UncachedWritePerM: 2.50, CachedPerM: 0.10, OutPerM: 10.0,
			},
		},
		{
			ID: "gpt-oss-120b-medium", Label: "GPT-OSS 120B", Family: "gpt", QuotaGroup: "3p",
			ContextWindow: 131072, TranscriptChars: 300000, MaxImages: 100,
			Enums: map[int]string{342: "medium"}, FixedTier: "medium",
			Pricing: ModelPricing{InPerM: 0.15, CachedPerM: 0.015, OutPerM: 0.60},
		},
		{
			ID: "gemini-pro-orchestrator", Label: "Gemini Pro Orchestrator", Family: "gemini", QuotaGroup: "gemini",
			ContextWindow: snap.ContextGemini, TranscriptChars: snap.TranscriptCharsGemini, MaxImages: 200,
			Enums: map[int]string{1667: "high"}, FixedTier: "high",
			Pricing: ModelPricing{InPerM: 2.0, CachedPerM: 0.10, OutPerM: 10.0},
		},
		{
			ID: "gemini-pro-orchestrator-neon", Label: "Gemini Pro Orchestrator Neon", Family: "gemini", QuotaGroup: "gemini",
			ContextWindow: snap.ContextGemini, TranscriptChars: snap.TranscriptCharsGemini, MaxImages: 200,
			Enums: map[int]string{}, FixedTier: "high",
			Pricing: ModelPricing{InPerM: 1.0, CachedPerM: 0.05, OutPerM: 5.0},
		},
		{
			ID: "gemini-pro-orchestrator-argon", Label: "Gemini Pro Orchestrator Argon", Family: "gemini", QuotaGroup: "gemini",
			ContextWindow: snap.ContextGemini, TranscriptChars: snap.TranscriptCharsGemini, MaxImages: 200,
			Enums: map[int]string{}, FixedTier: "high",
			Pricing: ModelPricing{InPerM: 2.0, CachedPerM: 0.10, OutPerM: 10.0},
		},
	}
	RetiredModels = []*ModelInfo{
		{
			ID: "gemini-flash-worker-agy", Label: "Gemini Flash Worker (AGY)", Family: "gemini", QuotaGroup: "gemini",
			ContextWindow: snap.ContextGemini, TranscriptChars: snap.TranscriptCharsGemini, MaxImages: 200,
			Enums:   map[int]string{1669: "medium"},
			Pricing: ModelPricing{InPerM: 0.25, CachedPerM: 0.025, OutPerM: 1.25},
		},
	}
	UnknownModel = &ModelInfo{
		ID: "unknown", Label: "其他/旧模型", Family: "other", QuotaGroup: "gemini",
		ContextWindow: 1048576, TranscriptChars: 1800000, Enums: map[int]string{},
		Pricing: AgyModels[0].Pricing,
	}
	for _, m := range AgyModels {
		modelByID[m.ID] = m
	}
	for _, m := range RetiredModels {
		retiredByID[m.ID] = m
	}
	for _, m := range append(append([]*ModelInfo(nil), AgyModels...), RetiredModels...) {
		for e, t := range m.Enums {
			modelByEnum[e] = struct {
				m    *ModelInfo
				tier string
			}{m, t}
		}
	}
	Configure(RuntimeSettings{
		RouteClaudeNames: snap.RouteClaudeNames,
		ExposeTierModels: snap.ExposeTierModels,
		DefaultModelID:   snap.DefaultModel,
	})
}

// Configure updates routable model defaults from a config Snapshot.
func Configure(rs RuntimeSettings) {
	cfgMu.Lock()
	routeClaudeNames = rs.RouteClaudeNames
	exposeTierModels = rs.ExposeTierModels
	if rs.DefaultModelID != "" {
		if _, ok := modelByID[rs.DefaultModelID]; ok {
			DefaultModelID = rs.DefaultModelID
		}
	}
	cfgMu.Unlock()
}

// EffectiveModelMaxImages returns the configured per-model image cap bounded by overrides.
func EffectiveModelMaxImages(m *ModelInfo, maxImagesGemini, maxImagesClaude int) int {
	if m != nil {
		if m.Family == "claude" || m.QuotaGroup == "3p" {
			if maxImagesClaude > 0 {
				return maxImagesClaude
			}
			if m.MaxImages > 0 {
				return m.MaxImages
			}
			return 100
		}
		if maxImagesGemini > 0 {
			return maxImagesGemini
		}
		if m.MaxImages > 0 {
			return m.MaxImages
		}
	}
	if maxImagesGemini > 0 {
		return maxImagesGemini
	}
	return 200
}

// ToInt converts numeric JSON / Go integer values to int.
func ToInt(v any) (int, bool) {
	switch x := v.(type) {
	case float64:
		return int(x), true
	case int:
		return x, true
	case int64:
		return int(x), true
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return int(i), true
		}
		if f, err := x.Float64(); err == nil {
			return int(f), true
		}
	}
	return 0, false
}

// GetModel returns the registered ModelInfo for id (case-insensitive, stripping "models/").
func GetModel(id string) *ModelInfo {
	clean := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(id)), "models/")
	if m, ok := modelByID[clean]; ok {
		return m
	}
	return nil
}

// ModelOrDefault returns GetModel(id) or the active default model.
func ModelOrDefault(id string) *ModelInfo {
	if m := GetModel(id); m != nil {
		return m
	}
	cfgMu.RLock()
	defID := DefaultModelID
	cfgMu.RUnlock()
	return modelByID[defID]
}

// LruProtectionWeight scales session context tokens by relative uncached input price.
func (m *ModelInfo) LruProtectionWeight() float64 {
	if m == nil {
		return 0.5
	}
	w := m.Pricing.InPerM / 0.50
	if w < 0.5 {
		return 0.5
	}
	return w
}

// ModelFromEnum looks up model and thinking tier from a protobuf step model enum.
func ModelFromEnum(e int) (*ModelInfo, string) {
	if hit, ok := modelByEnum[e]; ok {
		return hit.m, hit.tier
	}
	return UnknownModel, ""
}

// ModelFromEnumOrServed looks up model and thinking tier from a protobuf step model enum,
// falling back to servedModel (gen_metadata modelVersion) when the server assigns dynamic enums
// (such as gemini-pro-orchestrator-neon / gemini-pro-orchestrator-argon).
func ModelFromEnumOrServed(e int, servedModel string) (*ModelInfo, string) {
	if hit, ok := modelByEnum[e]; ok {
		return hit.m, hit.tier
	}
	clean := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(servedModel)), "models/")
	if clean == "" || clean == "unknown" {
		return UnknownModel, ""
	}
	if m, ok := modelByID[clean]; ok {
		return m, m.FixedTier
	}
	if m, ok := retiredByID[clean]; ok {
		return m, m.FixedTier
	}
	if clean == "gemini-pro-orchestrator-g1-pro" {
		if m, ok := modelByID["gemini-pro-orchestrator-argon"]; ok {
			return m, m.FixedTier
		}
	}
	if clean == "gemini-3.8-flash-tiered" {
		if m, ok := modelByID["gemini-3.8-flash"]; ok {
			return m, "high"
		}
	}
	if mSub := tierSuffixRe.FindStringSubmatch(clean); mSub != nil {
		base := tierSuffixRe.ReplaceAllString(clean, "")
		tier := normalizeEffortTier(mSub[1])
		if m, ok := modelByID[base]; ok {
			if m.FixedTier != "" {
				tier = m.FixedTier
			}
			return m, tier
		}
		if m, ok := retiredByID[base]; ok {
			if m.FixedTier != "" {
				tier = m.FixedTier
			}
			return m, tier
		}
	}
	return UnknownModel, ""
}

// ModelSelection represents the resolved model + normalized thinking tier for a request.
type ModelSelection struct {
	Model        *ModelInfo
	Tier         string
	Explicit     bool
	ExplicitTier bool
}

func normalizeEffortTier(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "none", "minimal", "low":
		return "low"
	case "medium":
		return "medium"
	case "high":
		return "high"
	case "xhigh", "max", "ultra":
		return "xhigh"
	default:
		return ""
	}
}

// ResolveRequestedModel maps a client model identifier and body thinking config to ModelSelection.
func ResolveRequestedModel(requested string, body map[string]any) ModelSelection {
	cfgMu.RLock()
	defID := DefaultModelID
	routeClaude := routeClaudeNames
	cfgMu.RUnlock()

	s := strings.ToLower(strings.TrimSpace(requested))
	s = strings.TrimPrefix(s, "models/")
	s = oneMSuffixRe.ReplaceAllString(s, "")
	tier := ""
	explicitTier := false
	if _, exact := modelByID[s]; !exact {
		if m := tierSuffixRe.FindStringSubmatch(s); m != nil {
			tier = normalizeEffortTier(m[1])
			explicitTier = tier != ""
			s = tierSuffixRe.ReplaceAllString(s, "")
		}
	}
	if t := ClientThinkingTier(body); t != "" {
		tier = t
	}

	m := modelByID[s]
	if m == nil {
		switch {
		case strings.HasPrefix(s, "gemini-pro-orchestrator-neon"):
			m = modelByID["gemini-pro-orchestrator-neon"]
		case strings.HasPrefix(s, "gemini-pro-orchestrator-argon") || strings.HasPrefix(s, "gemini-pro-orchestrator-g1-pro"):
			m = modelByID["gemini-pro-orchestrator-argon"]
		case strings.HasPrefix(s, "gemini-pro-orchestrator"):
			m = modelByID["gemini-pro-orchestrator"]
		case strings.HasPrefix(s, "gpt-oss-120b") || s == "gpt-oss":
			m = modelByID["gpt-oss-120b-medium"]
		case strings.HasPrefix(s, "gemini-4-carbon") || s == "carbon":
			m = modelByID["gemini-4-carbon"]
		case strings.HasPrefix(s, "gemini-4-neon") || s == "neon":
			m = modelByID["gemini-4-neon"]
		case strings.HasPrefix(s, "gemini-4-hydrogen") || s == "hydrogen":
			m = modelByID["gemini-4-hydrogen"]
		case strings.HasPrefix(s, "gemini-4") || s == "argon":
			m = modelByID["gemini-4-argon"]
		case strings.HasPrefix(s, "gemini-3.8-flash") || strings.HasPrefix(s, "gemini-2.5-flash") || strings.HasPrefix(s, "gemini-3.0-flash") || strings.HasPrefix(s, "gemini-3-flash") || s == "gemini-flash":
			m = modelByID["gemini-3.8-flash"]
		case strings.HasPrefix(s, "claude-opus-5") || s == "agy-opus":
			m = modelByID["claude-opus-5-5"]
		case strings.HasPrefix(s, "claude-sonnet-5") || s == "agy-sonnet":
			m = modelByID["claude-sonnet-5-5"]
		case routeClaude && strings.Contains(s, "opus"):
			m = modelByID["claude-opus-5-5"]
		case routeClaude && strings.Contains(s, "sonnet"):
			m = modelByID["claude-sonnet-5-5"]
		case routeClaude && strings.Contains(s, "haiku"):
			m = modelByID["gemini-3.8-flash"]
		}
	}
	explicit := m != nil
	if m == nil {
		m = modelByID[defID]
	}
	if m.FixedTier != "" {
		tier = m.FixedTier
	}
	if tier == "xhigh" && !m.SupportsXHigh {
		tier = "high"
	}
	if tier == "" {
		tier = "medium"
	}
	return ModelSelection{Model: m, Tier: tier, Explicit: explicit, ExplicitTier: explicitTier}
}

func geminiThinkingTier(tc map[string]any) string {
	if tc == nil {
		return ""
	}
	rawLvl, _ := tc["thinkingLevel"].(string)
	if rawLvl == "" {
		rawLvl, _ = tc["thinking_level"].(string)
	}
	if rawLvl != "" {
		lvl := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(rawLvl)), "thinking_level_")
		switch lvl {
		case "none", "minimal", "low":
			return "low"
		case "medium":
			return "medium"
		case "high":
			return "high"
		case "xhigh", "max", "ultra":
			return "xhigh"
		}
	}
	rawBudget, hasBudget := tc["thinkingBudget"]
	if !hasBudget {
		rawBudget, hasBudget = tc["thinking_budget"]
	}
	if hasBudget {
		if b, ok := ToInt(rawBudget); ok {
			switch {
			case b < 0:
				return ""
			case b == 0 || b <= 4096:
				return "low"
			case b <= 16384:
				return "medium"
			case b <= 32768:
				return "high"
			default:
				return "xhigh"
			}
		}
	}
	return ""
}

// ClientThinkingTier extracts normalized thinking tier from Anthropic/OpenAI/Gemini request bodies.
func ClientThinkingTier(body map[string]any) string {
	if body == nil {
		return ""
	}
	effort := ""
	if v, ok := body["reasoning_effort"].(string); ok {
		effort = v
	} else if rm, ok := body["reasoning"].(map[string]any); ok {
		effort, _ = rm["effort"].(string)
	} else if oc, ok := body["output_config"].(map[string]any); ok {
		effort, _ = oc["effort"].(string)
	}
	switch strings.ToLower(strings.TrimSpace(effort)) {
	case "none", "minimal", "low":
		return "low"
	case "medium":
		return "medium"
	case "high":
		return "high"
	case "xhigh", "max", "ultra":
		return "xhigh"
	}
	if th, ok := body["thinking"].(map[string]any); ok {
		typ, _ := th["type"].(string)
		if strings.EqualFold(typ, "disabled") {
			return "low"
		}
		if b, ok := ToInt(th["budget_tokens"]); ok && b > 0 {
			switch {
			case b <= 4096:
				return "low"
			case b <= 16384:
				return "medium"
			case b <= 32768:
				return "high"
			default:
				return "xhigh"
			}
		}
	}
	if gc, ok := body["generationConfig"].(map[string]any); ok {
		if tc, okT := gc["thinkingConfig"].(map[string]any); okT {
			if t := geminiThinkingTier(tc); t != "" {
				return t
			}
		}
		if tc, okT := gc["thinking_config"].(map[string]any); okT {
			if t := geminiThinkingTier(tc); t != "" {
				return t
			}
		}
	}
	if gc, ok := body["generation_config"].(map[string]any); ok {
		if tc, okT := gc["thinkingConfig"].(map[string]any); okT {
			if t := geminiThinkingTier(tc); t != "" {
				return t
			}
		}
		if tc, okT := gc["thinking_config"].(map[string]any); okT {
			if t := geminiThinkingTier(tc); t != "" {
				return t
			}
		}
	}
	if tc, okT := body["thinkingConfig"].(map[string]any); okT {
		if t := geminiThinkingTier(tc); t != "" {
			return t
		}
	}
	if tc, okT := body["thinking_config"].(map[string]any); okT {
		if t := geminiThinkingTier(tc); t != "" {
			return t
		}
	}
	return ""
}

// PricedUsage captures USD cost breakdown for one turn.
type PricedUsage struct {
	TierMultiplier float64
	InputCostUsd   float64
	CacheCostUsd   float64
	OutputCostUsd  float64
	ThinkCostUsd   float64
	Base1xCostUsd  float64
	CostUsd        float64
	NoCacheCostUsd float64
	SavedUsd       float64
}

// PriceUsage computes dollar quota cost and cache savings for a turn.
func PriceUsage(m *ModelInfo, inTok, crTok, outTok, thTok, cwTok int) PricedUsage {
	p := m.Pricing
	totalCtx := inTok + crTok
	effInPerM := p.InPerM
	if p.UncachedWritePerM > 0 && crTok > 0 {
		effInPerM = p.UncachedWritePerM
	}

	inMult, crMult, outMult, badgeMult := 1.0, 1.0, 1.0, 1.0
	if p.LongCtxThreshold > 0 && totalCtx > p.LongCtxThreshold {
		badgeMult = p.LongCtxMult
		if p.LongCtxInMult > 0 {
			inMult = p.LongCtxInMult
			crMult = p.LongCtxCrMult
			outMult = p.LongCtxOutMult
		} else {
			inMult, crMult, outMult = p.LongCtxMult, p.LongCtxMult, p.LongCtxMult
		}
	} else if p.MidCtxThreshold > 0 && totalCtx > p.MidCtxThreshold {
		badgeMult = p.MidCtxMult
		inMult, crMult, outMult = p.MidCtxMult, p.MidCtxMult, p.MidCtxMult
	}

	inputCost := (float64(inTok) * effInPerM / 1e6) * inMult
	cacheCost := (float64(crTok) * p.CachedPerM / 1e6) * crMult
	outputCost := (float64(outTok) * p.OutPerM / 1e6) * outMult
	thinkCost := (float64(thTok) * p.OutPerM / 1e6) * outMult

	base1x := (float64(inTok)*effInPerM + float64(crTok)*p.CachedPerM + float64(outTok)*p.OutPerM) / 1e6
	cost := inputCost + cacheCost + outputCost
	noCache := (float64(inTok+crTok)*p.InPerM*inMult + float64(outTok)*p.OutPerM*outMult) / 1e6
	saved := noCache - cost
	if saved < 0 {
		saved = 0
	}
	return PricedUsage{
		TierMultiplier: badgeMult,
		InputCostUsd:   inputCost,
		CacheCostUsd:   cacheCost,
		OutputCostUsd:  outputCost,
		ThinkCostUsd:   thinkCost,
		Base1xCostUsd:  base1x,
		CostUsd:        cost,
		NoCacheCostUsd: noCache,
		SavedUsd:       saved,
	}
}

// ListModelsForAPI renders `/v1/models` items.
func ListModelsForAPI() []map[string]any {
	cfgMu.RLock()
	expose := exposeTierModels
	cfgMu.RUnlock()

	out := []map[string]any{}
	for _, m := range AgyModels {
		out = append(out, map[string]any{
			"id":               m.ID,
			"object":           "model",
			"display_name":     m.Label,
			"context_window":   m.ContextWindow,
			"max_input_tokens": m.ContextWindow,
			"max_tokens":       65536,
			"owned_by":         m.Family,
		})
		if expose && m.FixedTier == "" {
			tiers := []string{"low", "high"}
			if m.SupportsXHigh {
				tiers = []string{"low", "high", "xhigh"}
			}
			for _, t := range tiers {
				out = append(out, map[string]any{
					"id":               m.ID + "-" + t,
					"object":           "model",
					"display_name":     m.Label + " (" + t + ")",
					"context_window":   m.ContextWindow,
					"max_input_tokens": m.ContextWindow,
					"max_tokens":       65536,
					"owned_by":         m.Family,
				})
			}
		}
	}
	return out
}

// BuildGeminiModelResource formats a `/v1beta/models/*` discovery object.
func BuildGeminiModelResource(id, label string, m *ModelInfo) map[string]any {
	ctxWin := 1048576
	if m != nil && m.ContextWindow > 0 {
		ctxWin = m.ContextWindow
	}
	return map[string]any{
		"name":                       "models/" + id,
		"baseModelId":                id,
		"version":                    "001",
		"displayName":                label,
		"description":                label + " via Antigravity ACP Gateway",
		"inputTokenLimit":            ctxWin,
		"outputTokenLimit":           65536,
		"supportedGenerationMethods": []string{"generateContent", "streamGenerateContent", "countTokens"},
		"thinking":                   true,
	}
}

// ListGeminiModelsForAPI renders `/v1beta/models` entries.
func ListGeminiModelsForAPI() []map[string]any {
	cfgMu.RLock()
	expose := exposeTierModels
	cfgMu.RUnlock()

	out := []map[string]any{}
	for _, m := range AgyModels {
		out = append(out, BuildGeminiModelResource(m.ID, m.Label, m))
		if expose && m.FixedTier == "" {
			tiers := []string{"low", "high"}
			if m.SupportsXHigh {
				tiers = []string{"low", "high", "xhigh"}
			}
			for _, t := range tiers {
				out = append(out, BuildGeminiModelResource(m.ID+"-"+t, m.Label+" ("+t+")", m))
			}
		}
	}
	return out
}
