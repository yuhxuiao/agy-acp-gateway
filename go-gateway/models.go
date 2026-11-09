package main

import (
	"os"
	"regexp"
	"strconv"
	"strings"
)

// Model registry for agy ACP models and alias routing.
// Enums: step usage f9.1 numeric model enum -> thinking tier, verified 2026-11-08 from conversation DBs
// (1418 argon-medium, 1417 argon-high, 1318/1319/1320 flash high/medium/low, 1401 opus medium,
// 1402 opus high, 1403/1404/1405 sonnet low/medium/high, 342 gpt-oss; 1419 argon-low, 1400 opus-low inferred).

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

type ModelInfo struct {
	ID              string         `json:"id"`
	Label           string         `json:"label"`
	Family          string         `json:"family"`
	QuotaGroup      string         `json:"quotaGroup"`
	ContextWindow   int            `json:"contextWindow"`
	TranscriptChars int            `json:"-"`
	Enums           map[int]string `json:"-"`
	FixedTier       string         `json:"fixedTier,omitempty"`
	Pricing         ModelPricing   `json:"pricing"`
}

func envPosInt(k string, def int) int {
	if v, err := strconv.Atoi(strings.TrimSpace(os.Getenv(k))); err == nil && v > 0 {
		return v
	}
	return def
}

var AgyModels = []*ModelInfo{
	{
		ID: "gemini-4-argon", Label: "Gemini 4 Argon", Family: "gemini", QuotaGroup: "gemini",
		ContextWindow: envPosInt("AGY_CONTEXT_GEMINI", 1048576), TranscriptChars: envPosInt("AGY_TRANSCRIPT_CHARS_GEMINI", 1800000),
		Enums: map[int]string{1417: "high", 1418: "medium", 1419: "low"},
		Pricing: ModelPricing{
			InPerM: 1.25, CachedPerM: 0.125, OutPerM: 10.0,
			MidCtxThreshold: 256000, MidCtxMult: 1.3,
			LongCtxThreshold: 512000, LongCtxMult: 1.8,
		},
	},
	{
		ID: "gemini-3.8-flash", Label: "Gemini 3.8 Flash", Family: "gemini", QuotaGroup: "gemini",
		ContextWindow: envPosInt("AGY_CONTEXT_GEMINI", 1048576), TranscriptChars: envPosInt("AGY_TRANSCRIPT_CHARS_GEMINI", 1800000),
		Enums:   map[int]string{1318: "high", 1319: "medium", 1320: "low"},
		Pricing: ModelPricing{InPerM: 0.5, CachedPerM: 0.05, OutPerM: 3.0},
	},
	{
		// Verified from SQLite step #2723 (857K) & #3214 (495K): Claude in Antigravity supports 1M context.
		// Uncached input tokens in f9.2 are written into the prompt cache (1.25x base = $6.25/M);
		// above 200K total context, Anthropic/Vertex 1M tier charges 2.0x on uncached input ($12.50/M)
		// and 1.5x on output ($37.50/M), while cached read stays 1.0x ($0.50/M).
		ID: "claude-opus-5-5", Label: "Claude Opus 5.5", Family: "claude", QuotaGroup: "3p",
		ContextWindow: envPosInt("AGY_CONTEXT_CLAUDE", 1048576), TranscriptChars: envPosInt("AGY_TRANSCRIPT_CHARS_CLAUDE", 1800000),
		Enums: map[int]string{1400: "low", 1401: "medium", 1402: "high"},
		Pricing: ModelPricing{
			InPerM: 5.0, UncachedWritePerM: 6.25, CachedPerM: 0.5, OutPerM: 25.0,
			LongCtxThreshold: 200000, LongCtxMult: 2.0,
			LongCtxInMult: 2.0, LongCtxCrMult: 1.0, LongCtxOutMult: 1.5,
		},
	},
	{
		ID: "claude-sonnet-5-5", Label: "Claude Sonnet 5.5", Family: "claude", QuotaGroup: "3p",
		ContextWindow: envPosInt("AGY_CONTEXT_CLAUDE", 1048576), TranscriptChars: envPosInt("AGY_TRANSCRIPT_CHARS_CLAUDE", 1800000),
		Enums: map[int]string{1403: "low", 1404: "medium", 1405: "high"},
		Pricing: ModelPricing{
			InPerM: 3.0, UncachedWritePerM: 3.75, CachedPerM: 0.3, OutPerM: 15.0,
			LongCtxThreshold: 200000, LongCtxMult: 2.0,
			LongCtxInMult: 2.0, LongCtxCrMult: 1.0, LongCtxOutMult: 1.5,
		},
	},
	{
		ID: "gpt-oss-120b-medium", Label: "GPT-OSS 120B", Family: "gpt", QuotaGroup: "3p",
		ContextWindow: envPosInt("AGY_CONTEXT_GPT_OSS", 131072), TranscriptChars: envPosInt("AGY_TRANSCRIPT_CHARS_GPT_OSS", 300000),
		Enums: map[int]string{342: "medium"}, FixedTier: "medium",
		Pricing: ModelPricing{InPerM: 0.15, CachedPerM: 0.015, OutPerM: 0.6},
	},
}

var UnknownModel = &ModelInfo{
	ID: "unknown", Label: "其他/旧模型", Family: "other", QuotaGroup: "gemini",
	ContextWindow: 1048576, TranscriptChars: 1800000, Enums: map[int]string{},
	Pricing: AgyModels[0].Pricing,
}

var (
	modelByID   = map[string]*ModelInfo{}
	modelByEnum = map[int]struct {
		m    *ModelInfo
		tier string
	}{}
	DefaultModelID   = "gemini-4-argon"
	routeClaudeNames = os.Getenv("AGY_ROUTE_CLAUDE_NAMES") == "1"
	tierSuffixRe     = regexp.MustCompile(`(?i)[-_](low|medium|high)$`)
	oneMSuffixRe     = regexp.MustCompile(`\[1m\]$`)
)

func init() {
	for _, m := range AgyModels {
		modelByID[m.ID] = m
		for e, t := range m.Enums {
			modelByEnum[e] = struct {
				m    *ModelInfo
				tier string
			}{m, t}
		}
	}
	if v := strings.TrimSpace(os.Getenv("AGY_DEFAULT_MODEL")); v != "" {
		if _, ok := modelByID[v]; ok {
			DefaultModelID = v
		}
	}
}

func GetModel(id string) *ModelInfo {
	if m, ok := modelByID[id]; ok {
		return m
	}
	return nil
}

func ModelOrDefault(id string) *ModelInfo {
	if m := GetModel(id); m != nil {
		return m
	}
	return modelByID[DefaultModelID]
}

func ModelFromEnum(e int) (*ModelInfo, string) {
	if hit, ok := modelByEnum[e]; ok {
		return hit.m, hit.tier
	}
	return UnknownModel, ""
}

type ModelSelection struct {
	Model    *ModelInfo
	Tier     string
	Explicit bool
}

func ResolveRequestedModel(requested string, body map[string]any) ModelSelection {
	s := strings.ToLower(strings.TrimSpace(requested))
	s = oneMSuffixRe.ReplaceAllString(s, "")
	tier := ""
	if _, exact := modelByID[s]; !exact {
		if m := tierSuffixRe.FindStringSubmatch(s); m != nil {
			tier = strings.ToLower(m[1])
			s = tierSuffixRe.ReplaceAllString(s, "")
		}
	}
	if t := clientThinkingTier(body); t != "" {
		tier = t
	}

	m := modelByID[s]
	if m == nil {
		switch {
		case s == "gpt-oss-120b" || s == "gpt-oss":
			m = modelByID["gpt-oss-120b-medium"]
		case strings.HasPrefix(s, "gemini-4"):
			m = modelByID["gemini-4-argon"]
		case strings.HasPrefix(s, "gemini-3.8-flash") || s == "gemini-flash":
			m = modelByID["gemini-3.8-flash"]
		case strings.HasPrefix(s, "claude-opus-5") || s == "agy-opus":
			m = modelByID["claude-opus-5-5"]
		case strings.HasPrefix(s, "claude-sonnet-5") || s == "agy-sonnet":
			m = modelByID["claude-sonnet-5-5"]
		case routeClaudeNames && strings.Contains(s, "opus"):
			m = modelByID["claude-opus-5-5"]
		case routeClaudeNames && strings.Contains(s, "sonnet"):
			m = modelByID["claude-sonnet-5-5"]
		case routeClaudeNames && strings.Contains(s, "haiku"):
			m = modelByID["gemini-3.8-flash"]
		}
	}
	explicit := m != nil
	if m == nil {
		m = modelByID[DefaultModelID]
	}
	if m.FixedTier != "" {
		tier = m.FixedTier
	}
	if tier == "" {
		tier = "medium"
	}
	return ModelSelection{Model: m, Tier: tier, Explicit: explicit}
}

func clientThinkingTier(body map[string]any) string {
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
	case "high", "xhigh", "max":
		return "high"
	}
	if th, ok := body["thinking"].(map[string]any); ok {
		typ, _ := th["type"].(string)
		if strings.EqualFold(typ, "disabled") {
			return "low"
		}
		if b, ok := toInt(th["budget_tokens"]); ok && b > 0 {
			switch {
			case b <= 4096:
				return "low"
			case b <= 16384:
				return "medium"
			default:
				return "high"
			}
		}
	}
	return ""
}

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

func PriceUsage(m *ModelInfo, inTok, crTok, outTok, thTok, cwTok int) PricedUsage {
	p := m.Pricing
	totalCtx := inTok + crTok
	effInPerM := p.InPerM
	if p.UncachedWritePerM > 0 {
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

func ListModelsForAPI() []map[string]any {
	out := []map[string]any{}
	for _, m := range AgyModels {
		out = append(out, map[string]any{"id": m.ID, "object": "model", "display_name": m.Label, "context_window": m.ContextWindow, "owned_by": m.Family})
		if m.FixedTier == "" {
			for _, t := range []string{"low", "high"} {
				out = append(out, map[string]any{"id": m.ID + "-" + t, "object": "model", "display_name": m.Label + " (" + t + ")", "context_window": m.ContextWindow, "owned_by": m.Family})
			}
		}
	}
	return out
}
