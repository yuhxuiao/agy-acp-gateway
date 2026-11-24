package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"agy-acp-gateway-go/internal/protocol"
	"agy-acp-gateway-go/internal/session"
)

func (s *AcpProjectSession) alignTpuTailUsage(inTok, cachedTok, outTok, steps int, source string) *TurnUsage {
	// Report agy's raw split. Quota measurements on 2026-11-11 matched the raw uncached/cached
	// token counts; the former fixed 2054-token "TPU tail" shift under-stated billable input.
	alignedIn := inTok
	alignedCr := cachedTok
	if alignedIn+alignedCr > 0 {
		s.mu.Lock()
		s.LastCachedReadTokens = alignedIn + alignedCr
		s.mu.Unlock()
	}
	if outTok <= 0 {
		outTok = 16
	}
	return &TurnUsage{
		InputTokens:            alignedIn,
		CachedReadTokens:       alignedCr,
		OutputTokens:           outTok,
		RawUncachedInputTokens: inTok,
		RawTpuCachedTokens:     cachedTok,
		ContextInputTokens:     alignedIn,
		ContextCachedTokens:    alignedCr,
		StepsInPrompt:          steps,
		UsageSource:            source,
	}
}

// normalizeTurnUsage is the per-session usage ledger. gen_metadata rows are the primary key:
// every row after the cursor is summed (several generations per prompt happen on retries and
// multi-step turns) and the cursor moves past all of them, so each generation is reported
// exactly once. Steps are only a fallback for databases that never had gen_metadata rows; when
// gen_metadata exists but has nothing new (a tool batch split from an already-billed generation),
// the turn reports zero usage instead of re-reading an old row.
func (s *AcpProjectSession) normalizeTurnUsage(rawUsage map[string]any, isToolUseStep bool, promptChars, outChars int) *TurnUsage {
	return s.normalizeTurnUsageWithPrefetch(rawUsage, isToolUseStep, promptChars, outChars, nil, 0, false, false)
}

func (s *AcpProjectSession) normalizeTurnUsageWithPrefetch(rawUsage map[string]any, isToolUseStep bool, promptChars, outChars int, prefetchedGens []*ParsedGenMetadata, prefetchedMaxGen int, prefetchedGenOK bool, hasPrefetch bool) *TurnUsage {
	s.mu.Lock()
	steps := s.PromptStepCount
	if steps < 1 {
		steps = 1
	}
	sid := s.SessionID
	lastIdx := s.LastSeenDbStepIdx
	lastGenIdx := s.LastSeenGenMetaIdx
	lastCached := s.LastCachedReadTokens
	modelID := s.ModelID
	s.mu.Unlock()

	var gens []*ParsedGenMetadata
	var maxGen int
	var genOK bool
	if hasPrefetch {
		gens, maxGen, genOK = prefetchedGens, prefetchedMaxGen, prefetchedGenOK
	} else {
		gens, maxGen, genOK = s.readSessionGenMetadataAfter(sid, lastGenIdx)
	}
	if len(gens) > 0 {
		var sumIn, sumCr, sumOut, sumTh int
		var sumStreamMs, sumDecOut, sumDecStreamMs, firstGenTtftMs int
		var report *ArgonDegradationReport
		for i, gm := range gens {
			sumIn += gm.InputTokens
			sumCr += gm.CachedReadTokens
			sumOut += gm.OutputTokens
			sumTh += gm.ThoughtTokens
			rawTtft := int(math.Round(gm.TtftSeconds * 1000))
			rawStream := int(math.Round(gm.StreamSeconds * 1000))
			gTtft, gStream, _, _, gDec := computeStepTiming(rawTtft, rawStream, rawTtft+rawStream, gm.OutputTokens)
			if i == 0 && gTtft > 0 {
				firstGenTtftMs = gTtft
			}
			if gStream > 0 {
				sumStreamMs += gStream
			}
			if gDec > 0 && gStream > 0 {
				sumDecOut += gm.OutputTokens
				sumDecStreamMs += gStream
			}
			deg := EvaluateArgonDegradation(gm, modelID)
			if report == nil || (deg != nil && deg.Degraded && !report.Degraded) {
				report = deg
			}
		}
		last := gens[len(gens)-1]
		actualTier, maxStep, _ := s.readSessionStepTierAndMaxIdxAfter(sid, lastIdx)
		if maxStep <= lastIdx {
			if ms := s.readMaxSessionStepIdx(sid); ms > maxStep {
				maxStep = ms
			}
		}
		s.mu.Lock()
		if maxGen > s.LastSeenGenMetaIdx {
			s.LastSeenGenMetaIdx = maxGen
		}
		if maxStep > s.LastSeenDbStepIdx {
			s.LastSeenDbStepIdx = maxStep
		}
		s.mu.Unlock()
		u := s.alignTpuTailUsage(sumIn, sumCr, sumOut, len(gens), "db")
		u.ContextInputTokens = last.InputTokens
		u.ContextCachedTokens = last.CachedReadTokens
		s.mu.Lock()
		s.LastCachedReadTokens = last.InputTokens + last.CachedReadTokens
		s.mu.Unlock()
		u.ThinkingTokens = sumTh
		u.Degradation = report
		u.Generations = len(gens)
		u.ServedModel = last.ServedModel
		u.ActualTier = actualTier
		u.GenTtftMs = firstGenTtftMs
		if sumStreamMs > 0 {
			u.StreamMs = sumStreamMs
		}
		if sumDecStreamMs > 0 && sumDecOut > 0 {
			u.DecodeTps = math.Round((float64(sumDecOut)/(float64(sumDecStreamMs)/1000.0))*10) / 10
		}
		return u
	}
	hasGenRows := genOK && (lastGenIdx >= 0 || maxGen >= 0)
	if genOK && maxGen > lastGenIdx {
		s.mu.Lock()
		if maxGen > s.LastSeenGenMetaIdx {
			s.LastSeenGenMetaIdx = maxGen
		}
		s.mu.Unlock()
	}
	if hasGenRows {
		return &TurnUsage{StepsInPrompt: 0, UsageSource: "none"}
	}

	stepRows, maxStep, stepOK := s.readSessionStepsAfter(sid, lastIdx)
	if len(stepRows) > 0 {
		var sumIn, sumCr, sumOut, sumTh int
		var sumStreamMs, sumDecOut, sumDecStreamMs, firstStepTtftMs int
		for i, st := range stepRows {
			sumIn += st.RawIn
			sumCr += st.RawCr
			sumOut += st.OutTok
			sumTh += st.ThTok
			if i == 0 && st.TtftMs > 0 {
				firstStepTtftMs = st.TtftMs
			}
			if st.StreamMs > 0 {
				sumStreamMs += st.StreamMs
			}
			if st.DecodeTps > 0 && st.StreamMs > 0 {
				sumDecOut += st.OutTok
				sumDecStreamMs += st.StreamMs
			}
		}
		last := stepRows[len(stepRows)-1]
		s.mu.Lock()
		if maxStep > s.LastSeenDbStepIdx {
			s.LastSeenDbStepIdx = maxStep
		}
		s.mu.Unlock()
		u := s.alignTpuTailUsage(sumIn, sumCr, sumOut, len(stepRows), "db")
		u.ContextInputTokens = last.RawIn
		u.ContextCachedTokens = last.RawCr
		s.mu.Lock()
		s.LastCachedReadTokens = last.RawIn + last.RawCr
		s.mu.Unlock()
		u.ThinkingTokens = sumTh
		u.Generations = len(stepRows)
		u.ServedModel = last.ServedModel
		for i := len(stepRows) - 1; i >= 0; i-- {
			if stepRows[i].ModelTier != "" {
				u.ActualTier = stepRows[i].ModelTier
				break
			}
		}
		u.GenTtftMs = firstStepTtftMs
		if sumStreamMs > 0 {
			u.StreamMs = sumStreamMs
		}
		if sumDecStreamMs > 0 && sumDecOut > 0 {
			u.DecodeTps = math.Round((float64(sumDecOut)/(float64(sumDecStreamMs)/1000.0))*10) / 10
		}
		return u
	}
	if stepOK && (lastIdx >= 0 || genOK) {
		// Readable session database without any new generation: nothing new to bill.
		return &TurnUsage{StepsInPrompt: 0, UsageSource: "none"}
	}

	estIn := promptChars / 4
	if estIn < 16 {
		estIn = 120
	}
	estOut := outChars / 4
	if estOut < 16 {
		estOut = 32
	}

	if isToolUseStep || rawUsage == nil {
		return &TurnUsage{
			InputTokens:      estIn,
			CachedReadTokens: lastCached,
			OutputTokens:     estOut,
			StepsInPrompt:    1,
			UsageSource:      "estimate",
		}
	}
	cumIn, _ := toInt(rawUsage["inputTokens"])
	cumCr, _ := toInt(rawUsage["cachedReadTokens"])
	outTok, _ := toInt(rawUsage["outputTokens"])
	thTok, _ := toInt(rawUsage["thoughtTokens"])
	u := s.alignTpuTailUsage(cumIn/steps, cumCr/steps, outTok, steps, "acp")
	if thTok > 0 {
		u.ThinkingTokens = thTok
	}
	return u
}

// normalizeTurnUsageAsync gives agy a short adaptive window to land the generation row of a turn/tool batch.
func (s *AcpProjectSession) normalizeTurnUsageAsync(rawUsage map[string]any, isToolUseStep bool, promptChars, outChars int) *TurnUsage {
	s.mu.Lock()
	sid := s.SessionID
	lastGenIdx := s.LastSeenGenMetaIdx
	s.mu.Unlock()

	backoffs := [...]time.Duration{
		5 * time.Millisecond,
		10 * time.Millisecond,
		20 * time.Millisecond,
		40 * time.Millisecond,
		60 * time.Millisecond,
		60 * time.Millisecond,
		60 * time.Millisecond,
	}
	var gens []*ParsedGenMetadata
	var maxGen int
	var ok bool
	for attempt := 0; attempt <= len(backoffs); attempt++ {
		gens, maxGen, ok = s.readSessionGenMetadataAfter(sid, lastGenIdx)
		if len(gens) > 0 || !ok {
			break
		}
		if attempt < len(backoffs) {
			time.Sleep(backoffs[attempt])
		}
	}
	s.mu.Lock()
	sameCursor := s.SessionID == sid && s.LastSeenGenMetaIdx == lastGenIdx
	s.mu.Unlock()
	return s.normalizeTurnUsageWithPrefetch(rawUsage, isToolUseStep, promptChars, outChars, gens, maxGen, ok, sameCursor)
}

func (s *AcpProjectSession) logTurnStats(mode string, elapsedMs int, res TurnResult) {
	s.logTurnStatsWithReason(mode, "", elapsedMs, res)
}

func (s *AcpProjectSession) logTurnStatsWithReason(mode, rolloverReason string, elapsedMs int, res TurnResult) {
	inTok := 0
	crTok := 0
	outTok := 0
	stepsInPrompt := 1
	rawIn := 0
	rawCr := 0
	usageSource := "unknown"
	if res.Usage != nil {
		inTok = res.Usage.InputTokens
		crTok = res.Usage.CachedReadTokens
		outTok = res.Usage.OutputTokens
		stepsInPrompt = res.Usage.StepsInPrompt
		rawIn = res.Usage.RawUncachedInputTokens
		rawCr = res.Usage.RawTpuCachedTokens
		usageSource = res.Usage.UsageSource
	}
	measured := usageSource == "db" || usageSource == "acp"
	hitRate := "N/A"
	if measured && inTok+crTok > 0 {
		hitRate = formatPercent(float64(crTok) / float64(inTok+crTok) * 100)
	}
	ttft := res.TtftMs
	if ttft <= 0 {
		ttft = elapsedMs
	}
	if elapsedMs > 0 && ttft > elapsedMs {
		ttft = elapsedMs
	}
	tps := 0.0
	streamMs := 0
	decodeTps := 0.0
	upstreamTtftMs := 0
	if res.Usage != nil && res.Usage.GenTtftMs > 0 {
		upstreamTtftMs = res.Usage.GenTtftMs
	}
	if measured && elapsedMs > 0 && outTok > 0 {
		tps = math.Round((float64(outTok)/(float64(elapsedMs)/1000.0))*10) / 10
		if res.Usage != nil && res.Usage.StreamMs > 0 {
			if res.Usage.StreamMs <= elapsedMs {
				streamMs = res.Usage.StreamMs
				decodeTps = res.Usage.DecodeTps
			} else if elapsedMs > ttft && ttft > 0 {
				_, streamMs, _, _, decodeTps = computeStepTiming(ttft, 0, elapsedMs, outTok)
			} else {
				_, streamMs, _, _, decodeTps = computeStepTiming(0, elapsedMs, elapsedMs, outTok)
			}
		} else if elapsedMs > ttft && ttft > 0 {
			_, streamMs, _, _, decodeTps = computeStepTiming(ttft, 0, elapsedMs, outTok)
		}
	}
	thinkLen := len(res.Thinking)
	thinkTok := 0
	if res.Usage != nil && res.Usage.ThinkingTokens > 0 {
		thinkTok = res.Usage.ThinkingTokens
	} else if thinkLen > 0 {
		thinkTok = int(math.Round(float64(thinkLen) / 3.2))
		if thinkTok < 1 {
			thinkTok = 1
		}
		if thinkTok > outTok {
			thinkTok = outTok
		}
	}
	thinkingMs := res.ThinkingMs
	if streamMs > 0 && thinkingMs > streamMs {
		thinkingMs = streamMs
	}
	if thinkingMs == 0 && thinkTok > 0 && outTok >= thinkTok && streamMs > 0 {
		thinkingMs = int(math.Round(float64(streamMs) * float64(thinkTok) / float64(outTok)))
	}

	now := time.Now().UTC()
	servedModel := ""
	actualTier := ""
	if res.Usage != nil {
		servedModel = res.Usage.ServedModel
		actualTier = res.Usage.ActualTier
	}
	s.mu.Lock()
	turnTier := s.ThoughtLevel
	if actualTier != "" {
		turnTier = actualTier
		if s.PromptInFlight || s.PendingThoughtLevel == "" {
			s.ThoughtLevel = actualTier
		}
	}
	var prev *TurnHistoryEntry
	afterColdStart := false
	for i := len(s.TurnHistory) - 1; i >= 0 && i >= len(s.TurnHistory)-16; i-- {
		h := &s.TurnHistory[i]
		if h.SessionID != s.SessionID {
			break
		}
		if h.Mode == "cold_start" || h.Mode == "compaction_rollover" || h.Mode == "argon_degraded_recovery" || h.Mode == "high_capacity_fallback" || h.Mode == "waf_autobisect_recovery" {
			afterColdStart = true
		}
		if h.Usage != nil && (h.Usage.UsageSource == "db" || h.Usage.UsageSource == "acp") {
			prev = h
			break
		}
	}
	turnStartAt := now
	if elapsedMs > 0 {
		turnStartAt = now.Add(-time.Duration(elapsedMs) * time.Millisecond)
	}
	cacheMiss := classifyCacheMissWithReason(mode, rolloverReason, res.Usage, prev, s.ModelID, turnStartAt)
	var emitBurnWarn string
	var burnStreak int
	var burnUncached int64
	var burnCtx int
	emitCacheDegradeTo := ""
	emitCacheDegradeStrikes := 0
	emitCacheDegradeCooldownSec := 0
	recoveredTier := ""
	recoveredPrevStrikes := 0
	if measured && res.Usage != nil {
		ctxTok := res.Usage.ContextTotal()
		uncachedIn := inTok
		if res.Usage.ContextInputTokens > 0 {
			uncachedIn = res.Usage.ContextInputTokens
		} else if rawIn > 0 {
			uncachedIn = rawIn
		}
		if ctxTok > 0 {
			switch {
			case cacheMiss == "" || ctxTok < CacheWarnMinTokens || cacheMiss == "cold_start" || cacheMiss == "model_switch" || strings.HasPrefix(cacheMiss, "rollover"):
				s.resetCacheBurnLocked()
			case cacheMiss == "zero":
				s.ZeroCacheStreak++
				s.ZeroCacheUncachedTokens += int64(uncachedIn)
				if s.ZeroCacheStreak >= CacheWarnStreak {
					s.LastCacheWarning = fmt.Sprintf(
						"consecutive-zero-cache; streak=%d; context_tokens=%d; uncached_tokens=%d; model=%s",
						s.ZeroCacheStreak, ctxTok, s.ZeroCacheUncachedTokens, s.ModelID,
					)
					emitBurnWarn = s.LastCacheWarning
					burnStreak = s.ZeroCacheStreak
					burnUncached = s.ZeroCacheUncachedTokens
					burnCtx = ctxTok
				}
			}
			alreadyFallbackArmed := s.capacityFallbackSid == s.SessionID &&
				s.capacityFallbackTier != "" &&
				tierRank(s.capacityFallbackTier) < tierRank(turnTier)
			if nextFb := nextFallbackTier(s.ModelID, turnTier); !s.lastTurnExplicitTier && s.ModelID == "gemini-4-carbon" && nextFb != "" && !alreadyFallbackArmed {
				if cacheMiss == "zero" && ctxTok >= 48000 && (ttft >= 10000 || elapsedMs >= 15000) {
					s.highTierZeroCacheStreak++
					if s.highTierZeroCacheStreak >= 2 && s.SessionID != "" &&
						(s.capacityFallbackSid != s.SessionID || s.capacityFallbackTier == "" || tierRank(nextFb) < tierRank(s.capacityFallbackTier)) {
						fbStrikes, fbCooldown := s.armCapacityFallbackLocked(s.SessionID, nextFb, "zero_cache_degrade", capacityFallbackNow())
						s.highTierZeroCacheStreak = 0
						emitCacheDegradeTo = nextFb
						emitCacheDegradeStrikes = fbStrikes
						emitCacheDegradeCooldownSec = int(fbCooldown.Seconds())
						burnCtx = ctxTok
					}
				} else if cacheMiss == "" || cacheMiss == "cold_start" || cacheMiss == "model_switch" || strings.HasPrefix(cacheMiss, "rollover") {
					s.highTierZeroCacheStreak = 0
				}
			} else {
				s.highTierZeroCacheStreak = 0
			}
		}
	}
	if s.capacityFallbackProbing {
		hasSoftErr := DetectAntigravitySoftError(res.Text) != nil || res.FailedNotice != ""
		if !hasSoftErr && emitCacheDegradeTo == "" && s.highTierZeroCacheStreak == 0 {
			recoveredTier = turnTier
			recoveredPrevStrikes = s.capacityFallbackStrikes
			s.capacityFallbackProbing = false
			s.capacityFallbackStrikes = 0
			s.capacityFallbackStrikeSid = ""
		} else if hasSoftErr || emitCacheDegradeTo != "" {
			s.capacityFallbackProbing = false
		}
	}
	if res.Usage != nil && s.LastCacheWarning != "" {
		res.Usage.CacheWarning = s.LastCacheWarning
	}
	relays := s.relaysThisTurn
	s.relaysThisTurn = 0
	s.TotalTurns++
	if n := len(s.TurnHistory); n > 0 && s.TurnHistory[n-1].TurnIndex >= s.TotalTurns {
		s.TotalTurns = s.TurnHistory[n-1].TurnIndex + 1
	}
	idx := s.TotalTurns
	entry := TurnHistoryEntry{
		TurnIndex:      idx,
		CompletedAtMs:  now.UnixMilli(),
		Ts:             now.Format(time.RFC3339),
		SessionID:      s.SessionID,
		Mode:           mode,
		ElapsedMs:      elapsedMs,
		TtftMs:         ttft,
		UpstreamTtftMs: upstreamTtftMs,
		StreamMs:       streamMs,
		Tps:            tps,
		DecodeTps:      decodeTps,
		HasThinking:    thinkTok > 0,
		ThinkingTokens: thinkTok,
		ThinkingMs:     thinkingMs,
		RetryCount:     res.RetryCount,
		Usage:          res.Usage,
		CacheHitRate:   hitRate,
		StopReason:     res.StopReason,
		Model:          s.ModelID,
		ThoughtLevel:   turnTier,
		ServedModel:    servedModel,
		CacheMiss:      cacheMiss,
		ClientID:       s.ClientID,
		ClientLabel:    s.ClientLabel,
		Protocol:       s.Protocol,
	}
	s.TurnHistory = append(s.TurnHistory, entry)
	// Bounded ring buffer (max 150 entries) to prevent unbounded memory growth
	if len(s.TurnHistory) > 150 {
		s.TurnHistory = s.TurnHistory[len(s.TurnHistory)-150:]
	}
	sid := s.SessionID
	turnModel := s.ModelID
	turnClient, turnProto := s.ClientLabel, s.Protocol
	s.promotePendingTierIfIdleLocked()
	s.mu.Unlock()
	if emitCacheDegradeTo != "" {
		LogEvent("HIGH_TIER_CACHE_DEGRADE_FALLBACK", "Armed in-place lower-tier fallback after consecutive carbon zero-cache misses", map[string]any{
			"convKey":       s.ConvKey,
			"sessionId":     sid,
			"model":         turnModel,
			"fromTier":      turnTier,
			"toTier":        emitCacheDegradeTo,
			"contextTokens": burnCtx,
			"reason":        "zero_cache_degrade",
			"strikes":       emitCacheDegradeStrikes,
			"cooldownSec":   emitCacheDegradeCooldownSec,
		})
	}
	if recoveredTier != "" {
		LogEvent("CAPACITY_FALLBACK_RECOVERED", "Recovered higher thinking tier cleanly after cooldown probe", map[string]any{
			"convKey":         s.ConvKey,
			"sessionId":       sid,
			"model":           turnModel,
			"recoveredTier":   recoveredTier,
			"previousStrikes": recoveredPrevStrikes,
		})
	}
	if emitBurnWarn != "" {
		cacheBurnWarningsTotal.Add(1)
		LogEvent("CACHE_BURN_WARNING", fmt.Sprintf("Long-context session missed cache %d consecutive turns (%s)", burnStreak, turnModel), map[string]any{
			"convKey":        s.ConvKey,
			"sessionId":      sid,
			"model":          turnModel,
			"thoughtLevel":   turnTier,
			"servedModel":    servedModel,
			"streak":         burnStreak,
			"contextTokens":  burnCtx,
			"uncachedTokens": burnUncached,
			"cacheMiss":      cacheMiss,
		})
	}
	s.persistTurnAndSession(entry)

	preview := res.Text
	if len(preview) > 120 {
		preview = preview[:120]
	}
	NotifyGatewayTurnCompleted()
	LogEvent("TURN_DONE", fmt.Sprintf("Completed turn (%s) in %dms", res.StopReason, elapsedMs), map[string]any{
		"convKey":                s.ConvKey,
		"sessionId":              sid,
		"client":                 turnClient,
		"protocol":               turnProto,
		"model":                  turnModel,
		"thoughtLevel":           turnTier,
		"mode":                   mode,
		"ttftMs":                 ttft,
		"upstreamTtftMs":         upstreamTtftMs,
		"streamMs":               streamMs,
		"tps":                    tps,
		"decodeTps":              decodeTps,
		"hasThinking":            thinkTok > 0,
		"thinkingTokens":         thinkTok,
		"thinkingMs":             thinkingMs,
		"retryCount":             res.RetryCount,
		"inputTokens":            inTok,
		"cachedReadTokens":       crTok,
		"rawUncachedInputTokens": rawIn,
		"rawTpuCachedTokens":     rawCr,
		"usageSource":            usageSource,
		"stepsInPrompt":          stepsInPrompt,
		"cacheHitRate":           hitRate,
		"outputTokens":           outTok,
		"toolCallsCount":         len(res.ToolCalls),
		"textPreview":            preview,
		"servedModel":            servedModel,
		"cacheMiss":              cacheMiss,
		"afterColdStart":         afterColdStart,
		"toolRelays":             relays,
		"generations":            generationsOf(res.Usage),
	})
}

func generationsOf(u *TurnUsage) int {
	if u == nil {
		return 0
	}
	return u.Generations
}

type softDetachError struct {
	cause error
}

func (e *softDetachError) Error() string {
	if e == nil || e.cause == nil {
		return "turn soft-detached during prefill"
	}
	return e.cause.Error()
}

func (e *softDetachError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func isSoftDetachedErr(err error) bool {
	var se *softDetachError
	return errors.As(err, &se)
}

type turnDetachMeta struct {
	sid                    string
	mode                   string
	rolloverReason         string
	turnStart              time.Time
	msgCount               int
	priorMsgCount          int
	firstUserFp            string
	cancelledHistFp        string
	cancelledHistFpFn      func() string
	isCompactionReq        bool
	isStandaloneCompaction bool
}

func (m *turnDetachMeta) resolveCancelledHistFp() string {
	if m == nil {
		return ""
	}
	if m.cancelledHistFp != "" {
		return m.cancelledHistFp
	}
	if m.cancelledHistFpFn != nil {
		return m.cancelledHistFpFn()
	}
	return ""
}

type turnDetachMetaKey struct{}

func withTurnDetachMeta(ctx context.Context, meta *turnDetachMeta) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if meta == nil {
		return ctx
	}
	return context.WithValue(ctx, turnDetachMetaKey{}, meta)
}

func turnDetachMetaFromContext(ctx context.Context) *turnDetachMeta {
	if ctx == nil {
		return nil
	}
	meta, _ := ctx.Value(turnDetachMetaKey{}).(*turnDetachMeta)
	return meta
}

type detachedTurnState struct {
	reqFp                  string
	turn                   *activeTurnState
	sid                    string
	mode                   string
	rolloverReason         string
	turnStart              time.Time
	msgCount               int
	priorMsgCount          int
	promptChars            int
	firstUserFp            string
	cancelledHistFp        string
	isCompactionReq        bool
	isStandaloneCompaction bool
	compactionTarget       *compactionTargetRef
	settledResult          *TurnResult
	passiveExtended        bool
	graceTimer             *time.Timer
}

func isThinkingBlockMap(m map[string]any) bool {
	if m == nil {
		return false
	}
	t, _ := m["type"].(string)
	return t == "thinking" || t == "redacted_thinking"
}

func valueHasCacheControl(v any) bool {
	switch x := v.(type) {
	case map[string]any:
		if _, ok := x["cache_control"]; ok {
			return true
		}
		if _, ok := x["reasoning_content"]; ok {
			return true
		}
		if _, ok := x["reasoning"]; ok {
			return true
		}
		if isThinkingBlockMap(x) {
			return true
		}
		if bc, ok := x["content"]; ok && bc != nil {
			return valueHasCacheControl(bc)
		}
	case []any:
		for _, it := range x {
			if valueHasCacheControl(it) {
				return true
			}
		}
	case []map[string]any:
		for _, it := range x {
			if valueHasCacheControl(it) {
				return true
			}
		}
	}
	return false
}

func stripCacheControlValue(v any) any {
	switch x := v.(type) {
	case map[string]any:
		if !valueHasCacheControl(x) {
			return x
		}
		cp := make(map[string]any, len(x))
		for k, val := range x {
			if k == "cache_control" || k == "reasoning_content" || k == "reasoning" {
				continue
			}
			if k == "content" && val != nil {
				cp[k] = stripCacheControlValue(val)
			} else {
				cp[k] = val
			}
		}
		return cp
	case []any:
		if !valueHasCacheControl(x) {
			return x
		}
		out := make([]any, 0, len(x))
		for _, it := range x {
			if m, ok := it.(map[string]any); ok && isThinkingBlockMap(m) {
				continue
			}
			out = append(out, stripCacheControlValue(it))
		}
		return out
	case []map[string]any:
		if !valueHasCacheControl(x) {
			return x
		}
		out := make([]map[string]any, 0, len(x))
		for _, it := range x {
			if isThinkingBlockMap(it) {
				continue
			}
			if m, ok := stripCacheControlValue(it).(map[string]any); ok {
				out = append(out, m)
			} else {
				out = append(out, it)
			}
		}
		return out
	default:
		return v
	}
}

func canonicalizeRequestFpMessages(messages []map[string]any) []map[string]any {
	if len(messages) == 0 || !valueHasCacheControl(messages) {
		return messages
	}
	if cleaned, ok := stripCacheControlValue(messages).([]map[string]any); ok {
		return cleaned
	}
	return messages
}

func stripCacheControlFromMessages(messages []map[string]any) []map[string]any {
	return canonicalizeRequestFpMessages(messages)
}

func computeTurnRequestFingerprint(modelID, tier, sysText string, tools []ToolDef, messages []map[string]any) string {
	return computeTurnRequestFingerprintWithDirective(modelID, tier, sysText, tools, messages, "")
}

func computeTurnRequestFingerprintWithDirective(modelID, tier, sysText string, tools []ToolDef, messages []map[string]any, directiveToken string) string {
	var toolDigests []string
	if len(tools) > 0 {
		_, toolDigests = toolCatalogFingerprintAndDigests(tools)
	}
	cleanMsgs := canonicalizeRequestFpMessages(messages)
	cwd := extractCwdFromSystem(sysText, cleanMsgs)
	return computeTurnRequestFingerprintPrecomputedClean(modelID, tier, cwd, toolDigests, cleanMsgs, directiveToken)
}

func computeTurnRequestFingerprintPrecomputed(modelID, tier, cwd string, toolDigests []string, messages []map[string]any, directiveToken string) string {
	return computeTurnRequestFingerprintPrecomputedClean(modelID, tier, cwd, toolDigests, canonicalizeRequestFpMessages(messages), directiveToken)
}

func computeTurnRequestFingerprintPrecomputedClean(modelID, tier, cwd string, toolDigests []string, cleanMsgs []map[string]any, directiveToken string) string {
	type fpPayload struct {
		Model       string           `json:"m"`
		Tier        string           `json:"t"`
		Cwd         string           `json:"c,omitempty"`
		Directive   string           `json:"d,omitempty"`
		ToolDigests []string         `json:"td,omitempty"`
		Messages    []map[string]any `json:"ms"`
	}
	h := sha256.New()
	enc := json.NewEncoder(h)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(fpPayload{
		Model:       modelID,
		Tier:        tier,
		Cwd:         cwd,
		Directive:   directiveToken,
		ToolDigests: toolDigests,
		Messages:    cleanMsgs,
	}); err != nil {
		return ""
	}
	return hex.EncodeToString(h.Sum(nil))
}

func canonicalizeHistoryFpValue(v any) any {
	switch x := v.(type) {
	case map[string]any:
		cp := make(map[string]any, len(x))
		for k, val := range x {
			if k == "cache_control" || k == "reasoning_content" || k == "reasoning" {
				continue
			}
			if (k == "text" || k == "content") && val != nil {
				if s, okS := val.(string); okS {
					if core := strings.TrimSpace(volatileFirstTurnTagRe.ReplaceAllString(s, "")); core != "" {
						cp[k] = core
						continue
					}
				}
				cp[k] = canonicalizeHistoryFpValue(val)
			} else {
				cp[k] = val
			}
		}
		return cp
	case []any:
		out := make([]any, 0, len(x))
		for _, it := range x {
			if m, ok := it.(map[string]any); ok && isThinkingBlockMap(m) {
				continue
			}
			out = append(out, canonicalizeHistoryFpValue(it))
		}
		return out
	case []map[string]any:
		out := make([]map[string]any, 0, len(x))
		for _, it := range x {
			if isThinkingBlockMap(it) {
				continue
			}
			if m, ok := canonicalizeHistoryFpValue(it).(map[string]any); ok {
				out = append(out, m)
			} else {
				out = append(out, it)
			}
		}
		return out
	default:
		return v
	}
}

func computeMessagesSliceFingerprint(messages []map[string]any) string {
	if len(messages) == 0 {
		return ""
	}
	normMsgs := canonicalizeHistoryFpValue(messages)
	h := sha256.New()
	enc := json.NewEncoder(h)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(normMsgs); err != nil {
		return ""
	}
	return hex.EncodeToString(h.Sum(nil))
}

func cancelledHistoryDiverged(messages []map[string]any, msgCnt int, lastTurnAsst bool, cancelledFp string) bool {
	if msgCnt <= 0 || lastTurnAsst {
		return false
	}
	if len(messages) < msgCnt || cancelledFp == "" {
		return true
	}
	return computeMessagesSliceFingerprint(messages[:msgCnt]) != cancelledFp
}

func (s *AcpProjectSession) HasDetachedTurn() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	has := s.detachedTurn != nil
	s.mu.Unlock()
	return has
}

func (s *AcpProjectSession) effectiveSoftDetachPolicy(promptChars int) (minElapsed, grace time.Duration, maxDetaches int) {
	minElapsed = PrefillCancelMinElapsed
	grace = PrefillCancelGrace
	maxDetaches = 3
	if s == nil {
		return minElapsed, grace, maxDetaches
	}
	s.mu.Lock()
	estTokens := s.LastCachedReadTokens
	if s.LastReportedContextUsed > estTokens {
		estTokens = s.LastReportedContextUsed
	}
	sysLen := len(s.SystemText)
	s.mu.Unlock()
	if charEst := (promptChars + sysLen) / 4; charEst > estTokens {
		estTokens = charEst
	}
	canScaleMin := !prefillMinElapsedEnvSet && PrefillCancelMinElapsed == 30*time.Second
	canScaleGrace := PrefillCancelGrace >= 45*time.Second
	switch {
	case estTokens >= 120000:
		if canScaleMin {
			minElapsed = 5 * time.Second
		}
		if canScaleGrace && grace < 120*time.Second {
			grace = 120 * time.Second
		}
		maxDetaches = 6
	case estTokens >= 48000:
		if canScaleMin {
			minElapsed = 8 * time.Second
		}
		if canScaleGrace && grace < 90*time.Second {
			grace = 90 * time.Second
		}
		maxDetaches = 5
	}
	return minElapsed, grace, maxDetaches
}

func (s *AcpProjectSession) tryStartSoftDetach(turn *activeTurnState, reqFp, sid, mode, rolloverReason string, turnStart, waitStart time.Time, msgCount int, firstUserFp, cancelledHistFp string, isCompactionReq bool) bool {
	return s.tryStartSoftDetachFull(turn, reqFp, sid, mode, rolloverReason, turnStart, waitStart, msgCount, firstUserFp, cancelledHistFp, isCompactionReq, 0, false)
}

func (s *AcpProjectSession) tryStartSoftDetachWithChars(turn *activeTurnState, reqFp, sid, mode, rolloverReason string, turnStart, waitStart time.Time, msgCount int, firstUserFp, cancelledHistFp string, isCompactionReq bool, promptChars int) bool {
	return s.tryStartSoftDetachFull(turn, reqFp, sid, mode, rolloverReason, turnStart, waitStart, msgCount, firstUserFp, cancelledHistFp, isCompactionReq, promptChars, false)
}

func (s *AcpProjectSession) tryStartSoftDetachFull(turn *activeTurnState, reqFp, sid, mode, rolloverReason string, turnStart, waitStart time.Time, msgCount int, firstUserFp, cancelledHistFp string, isCompactionReq bool, promptChars int, isStandaloneCompaction bool) bool {
	return s.tryStartSoftDetachWithMeta(turn, reqFp, sid, mode, rolloverReason, turnStart, waitStart, msgCount, 0, firstUserFp, cancelledHistFp, isCompactionReq, promptChars, isStandaloneCompaction, nil)
}

func (s *AcpProjectSession) tryStartSoftDetachWithMeta(turn *activeTurnState, reqFp, sid, mode, rolloverReason string, turnStart, waitStart time.Time, msgCount, priorMsgCount int, firstUserFp, cancelledHistFp string, isCompactionReq bool, promptChars int, isStandaloneCompaction bool, overrideCompTarget *compactionTargetRef) bool {
	if s == nil || turn == nil || reqFp == "" || PrefillCancelGrace <= 0 {
		return false
	}
	minElapsed, graceDur, maxDetaches := s.effectiveSoftDetachPolicy(promptChars)
	s.mu.Lock()
	coalesced := s.coalesceHandoff
	s.mu.Unlock()

	elapsedStart := waitStart
	if !turnStart.IsZero() && (elapsedStart.IsZero() || turnStart.Before(elapsedStart)) {
		elapsedStart = turnStart
	}
	if elapsedStart.IsZero() {
		elapsedStart = turn.createdAt
	}
	if !coalesced && time.Since(elapsedStart) < minElapsed {
		return false
	}
	prevDetaches := turn.SoftDetachCount()
	if (!coalesced && prevDetaches >= maxDetaches) || !turn.CanSoftDetach() {
		return false
	}
	s.mu.Lock()
	if s.closing || !turn.CanSoftDetach() || (prevDetaches == 0 && s.activeTurn != turn) {
		s.mu.Unlock()
		return false
	}
	if sid == "" {
		sid = s.SessionID
	}
	compTarget := overrideCompTarget
	if compTarget == nil {
		compTarget = s.activeCompactionTarget
		s.activeCompactionTarget = nil
	}
	det := &detachedTurnState{
		reqFp:                  reqFp,
		turn:                   turn,
		sid:                    sid,
		mode:                   mode,
		rolloverReason:         rolloverReason,
		turnStart:              turnStart,
		msgCount:               msgCount,
		priorMsgCount:          priorMsgCount,
		promptChars:            promptChars,
		firstUserFp:            firstUserFp,
		cancelledHistFp:        cancelledHistFp,
		isCompactionReq:        isCompactionReq,
		isStandaloneCompaction: isStandaloneCompaction,
		compactionTarget:       compTarget,
	}
	s.enterDetachedTurnLocked(det)
	det.graceTimer = time.AfterFunc(graceDur, func() {
		s.expireDetachedTurn(det)
	})
	s.mu.Unlock()
	turn.DetachSubscriber()
	LogEvent("PREFILL_SOFT_DETACH", "Client disconnected after prefill threshold; holding upstream turn in grace window", map[string]any{
		"convKey":        s.ConvKey,
		"sessionId":      sid,
		"mode":           mode,
		"waitMs":         time.Since(elapsedStart).Milliseconds(),
		"graceSec":       int(graceDur.Seconds()),
		"detachCount":    turn.SoftDetachCount(),
		"reqFingerprint": reqFp[:12],
	})
	return true
}

func (s *AcpProjectSession) settleDetachedTurn(det *detachedTurnState, reason, eventName string) {
	if s == nil || det == nil || det.turn == nil {
		return
	}
	compRef := det.compactionTarget
	det.compactionTarget = nil
	defer compRef.release()
	settled := det.settledResult
	if settled == nil {
		select {
		case res := <-det.turn.resolveChan:
			det.turn.MarkResultConsumed()
			settled = &res
		default:
		}
	}
	det.turn.mu.Lock()
	alreadyUpstreamDone := det.turn.upstreamDone && !det.turn.failed
	det.turn.mu.Unlock()

	if settled != nil || alreadyUpstreamDone {
		s.mu.Lock()
		s.ForceRolloverNext = true
		s.ForceRolloverReason = "rollover_unclaimed_detached"
		if s.activeTurn == det.turn {
			s.activeTurn = nil
		}
		s.mu.Unlock()
		det.turn.CleanupSubscriber()
		if det.turn.releaseSem != nil {
			det.turn.releaseOnce.Do(det.turn.releaseSem)
		}
		det.turn.CancelContext()
		det.turn.closeDelta()
		// Close and reset session state (including any unclaimed tool_use ParkedCalls and gateway.db
		// msg_count) so both in-process next turns and restarts roll over cleanly with full history.
		s.HibernateWorker("unclaimed_detached_rollover", true)
		s.mu.Lock()
		s.ForceRolloverNext = true
		s.ForceRolloverReason = "rollover_unclaimed_detached"
		s.mu.Unlock()
		LogEvent(eventName, "Detached turn finished unclaimed before cleanup; rolling over on next turn", map[string]any{
			"convKey":   s.ConvKey,
			"sessionId": det.sid,
			"reason":    reason,
		})
		return
	}

	s.CancelActiveTurn(reason)
	isFreshOneshotCompaction := det.isStandaloneCompaction || (det.isCompactionReq && det.priorMsgCount == 0)
	if (reason == "superseded_by_new_request" && det.msgCount <= 2) || isFreshOneshotCompaction {
		hibReason := "superseded_turn1"
		if isFreshOneshotCompaction {
			hibReason = "oneshot_compaction_expired"
		}
		s.HibernateWorker(hibReason, true)
	} else {
		s.mu.Lock()
		s.MessageCount = det.msgCount
		s.lastTurnAssistant = false
		s.cancelledHistoryFp = det.cancelledHistFp
		if det.firstUserFp != "" {
			s.HistoryFingerprint = det.firstUserFp
		}
		s.mu.Unlock()
	}
	LogEvent(eventName, "Cancelled detached upstream turn", map[string]any{
		"convKey":   s.ConvKey,
		"sessionId": det.sid,
		"reason":    reason,
	})
}

func (s *AcpProjectSession) expireDetachedTurn(det *detachedTurnState) {
	if s == nil || det == nil {
		return
	}
	s.mu.Lock()
	if s.detachedTurn != det {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	ch := s.ensureQueueSem()
	ch <- struct{}{}
	defer s.releaseTurnQueue()

	s.mu.Lock()
	if s.detachedTurn != det {
		s.mu.Unlock()
		return
	}
	s.clearDetachedTurnLocked(true)
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.leaveStreamingTurnLocked()
		s.mu.Unlock()
	}()

	// If a non-trivial (>=16K-token) upstream turn already finished cleanly as a pure end_turn
	// (0 tool calls, no errors), release promptSem immediately and hold the completed TurnResult
	// passively so a late retry claims the warm result in 0ms instead of rollover_unclaimed_detached.
	if det.settledResult == nil && !det.passiveExtended && det.turn != nil {
		select {
		case res := <-det.turn.resolveChan:
			det.turn.MarkResultConsumed()
			s.mu.Lock()
			estCtx := s.LastCachedReadTokens
			if s.LastReportedContextUsed > estCtx {
				estCtx = s.LastReportedContextUsed
			}
			s.mu.Unlock()
			if uTot := res.Usage.ContextTotal(); uTot > estCtx {
				estCtx = uTot
			}
			if cEst := det.promptChars / 4; cEst > estCtx {
				estCtx = cEst
			}
			if estCtx >= 16000 && len(res.ToolCalls) == 0 && res.StopReason != "tool_use" &&
				res.FailedNotice == "" && DetectAntigravitySoftError(res.Text) == nil {
				det.turn.CleanupSubscriber()
				if det.turn.releaseSem != nil {
					det.turn.releaseOnce.Do(det.turn.releaseSem)
				}
				det.turn.CancelContext()
				det.turn.closeDelta()
				passiveHold := PrefillCancelGrace
				if PrefillCancelGrace >= 45*time.Second && passiveHold < 120*time.Second {
					passiveHold = 120 * time.Second
				}
				s.mu.Lock()
				if s.activeTurn == det.turn {
					s.activeTurn = nil
				}
				det.settledResult = &res
				det.passiveExtended = true
				s.enterDetachedTurnLocked(det)
				det.graceTimer = time.AfterFunc(passiveHold, func() {
					s.expireDetachedTurn(det)
				})
				s.mu.Unlock()
				LogEvent("PREFILL_SOFT_DETACH_HOLD", "Holding completed detached end_turn result in passive claim buffer", map[string]any{
					"convKey":   s.ConvKey,
					"sessionId": det.sid,
					"holdSec":   int(passiveHold.Seconds()),
				})
				return
			}
			det.settledResult = &res
		default:
		}
	}

	softDetachExpiredTotal.Add(1)
	s.settleDetachedTurn(det, "prefill_grace_expired", "PREFILL_SOFT_DETACH_EXPIRED")
}

func (s *AcpProjectSession) AbortDetachedTurn(reason string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	det := s.detachedTurn
	if det == nil {
		s.mu.Unlock()
		return
	}
	wasBusy := s.IsBusy
	s.clearDetachedTurnLocked(true)
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.IsBusy = wasBusy
		s.syncPhaseLocked()
		s.mu.Unlock()
	}()
	if det.graceTimer != nil {
		det.graceTimer.Stop()
	}
	if reason == "superseded_by_new_request" {
		softDetachSupersededTotal.Add(1)
	}
	s.settleDetachedTurn(det, reason, "PREFILL_SOFT_DETACH_SUPERSEDED")
}

func (s *AcpProjectSession) recordRecentCompletedTurn(reqFp string, res TurnResult) {
	if s == nil || reqFp == "" || len(res.ToolCalls) > 0 || res.FailedNotice != "" || DetectAntigravitySoftError(res.Text) != nil {
		return
	}
	var displacedRef *compactionTargetRef
	s.mu.Lock()
	if s.coalesceHandoff {
		s.lastCompletedReqFp = reqFp
		s.lastCompletedResult = res
		s.lastCompletedAt = time.Now()
		if s.activeCompactionTarget != nil {
			displacedRef = s.lastCompletedCompactionTarget
			s.lastCompletedCompactionTarget = s.activeCompactionTarget
			s.activeCompactionTarget = nil
		}
	}
	s.mu.Unlock()
	displacedRef.release()
}

func (s *AcpProjectSession) tryReplayRecentCompletedTurn(reqFp string, onDelta func(DeltaChunk)) (TurnResult, bool) {
	res, ref, ok := s.tryReplayRecentCompletedTurnWithCompaction(reqFp, onDelta)
	ref.release()
	return res, ok
}

func (s *AcpProjectSession) tryReplayRecentCompletedTurnWithCompaction(reqFp string, onDelta func(DeltaChunk)) (TurnResult, *compactionTargetRef, bool) {
	if s == nil || reqFp == "" {
		return TurnResult{}, nil, false
	}
	var expiredRef *compactionTargetRef
	s.mu.Lock()
	if s.lastCompletedReqFp != "" && (s.lastCompletedAt.IsZero() || time.Since(s.lastCompletedAt) >= 15*time.Second) {
		expiredRef = s.lastCompletedCompactionTarget
		s.lastCompletedReqFp = ""
		s.lastCompletedResult = TurnResult{}
		s.lastCompletedAt = time.Time{}
		s.lastCompletedCompactionTarget = nil
	}
	matched := s.lastCompletedReqFp == reqFp && !s.lastCompletedAt.IsZero() && time.Since(s.lastCompletedAt) < 15*time.Second
	var res TurnResult
	var compRef *compactionTargetRef
	if matched {
		res = s.lastCompletedResult
		s.lastCompletedReqFp = ""
		s.lastCompletedResult = TurnResult{}
		s.lastCompletedAt = time.Time{}
		compRef = s.lastCompletedCompactionTarget
		s.lastCompletedCompactionTarget = nil
	}
	s.mu.Unlock()
	expiredRef.release()
	if !matched {
		return TurnResult{}, nil, false
	}
	if onDelta != nil {
		if res.Thinking != "" {
			onDelta(DeltaChunk{Type: "thinking", Thinking: res.Thinking})
		}
		if res.Text != "" {
			onDelta(DeltaChunk{Type: "text", Text: res.Text})
		}
	}
	return res, compRef, true
}

func (s *AcpProjectSession) finalizeCompletedTurn(ctx context.Context, res TurnResult, turnStart time.Time, firstDeltaNs int64, messages []map[string]any, currentFirstFp string, isCompactionReq bool, sid, mode, rolloverReason string) (TurnResult, error) {
	return s.finalizeCompletedTurnWithTarget(ctx, res, turnStart, firstDeltaNs, messages, currentFirstFp, isCompactionReq, false, sid, mode, rolloverReason)
}

func (s *AcpProjectSession) finalizeCompletedTurnWithTarget(ctx context.Context, res TurnResult, turnStart time.Time, firstDeltaNs int64, messages []map[string]any, currentFirstFp string, isCompactionReq, hasDetachedCompTarget bool, sid, mode, rolloverReason string) (TurnResult, error) {
	elapsed := int(time.Since(turnStart).Milliseconds())
	res.TtftMs = resolveTurnTtftMs(turnStart, elapsed, firstDeltaNs, res)

	if se := DetectAntigravitySoftError(res.Text); se != nil {
		s.mu.Lock()
		warmMsgCnt := s.MessageCount
		hasRemainingParked := len(s.ParkedCalls) > 0
		s.mu.Unlock()
		isTransientWarm := !hasRemainingParked &&
			(se.StatusCode == http.StatusTooManyRequests || se.StatusCode == http.StatusServiceUnavailable || se.StatusCode == 529) &&
			se.ErrorType != "tos_circuit_breaker"
		if !isTransientWarm || warmMsgCnt == 0 {
			s.HibernateWorker("soft_error_reset", true)
		}
	} else if res.FailedNotice != "" {
		if mode == "parked_mcp_resume" {
			s.HibernateWorker("soft_error_reset", true)
		}
		// Keep MessageCount on non-parked failed notice so client can retry onto the live session.
	} else {
		var anchors sessionContentAnchors
		updateAnchors := !isCompactionReq && !isStandaloneCompactionFromContext(ctx)
		if updateAnchors {
			anchors = extractSessionContentAnchors(messages, res.Text)
		}
		s.mu.Lock()
		s.MessageCount = len(messages) + 1
		s.lastTurnAssistant = true
		s.cancelledHistoryFp = ""
		if res.StopReason != "tool_use" && len(res.ToolCalls) == 0 && len(s.ParkedCalls) == 0 {
			s.orphanCapStreak = 0
		}
		if currentFirstFp != "" {
			s.HistoryFingerprint = currentFirstFp
		}
		if updateAnchors {
			s.contentAnchors = anchors
		}
		hasCompTarget := hasDetachedCompTarget || s.activeCompactionTarget != nil
		if isCompactionReq && !hasCompTarget && !isStandaloneCompactionFromContext(ctx) {
			s.ForceRolloverNext = true
			s.ForceRolloverReason = "rollover_compaction"
			s.lastCompactedAt = time.Now()
		}
		if ctxTot := res.Usage.ContextTotal(); ctxTot > 0 {
			s.LastCachedReadTokens = ctxTot
		}
		s.mu.Unlock()
	}

	res.SessionID = sid
	res.ConvKey = s.ConvKey
	res.ElapsedMs = elapsed
	res.Mode = mode
	s.logTurnStatsWithReason(mode, rolloverReason, elapsed, res)
	if err := failedNoticeError(res); err != nil {
		return TurnResult{}, err
	}
	s.recordRecentCompletedTurn(requestFingerprintFromContext(ctx), res)
	return res, nil
}

func (s *AcpProjectSession) JoinDetachedTurn(ctx context.Context, reqFp string, messages []map[string]any, onDelta func(DeltaChunk)) (TurnResult, bool, error) {
	res, ref, joined, err := s.JoinDetachedTurnWithCompaction(ctx, reqFp, messages, onDelta)
	ref.release()
	return res, joined, err
}

func (s *AcpProjectSession) JoinDetachedTurnWithCompaction(ctx context.Context, reqFp string, messages []map[string]any, onDelta func(DeltaChunk)) (TurnResult, *compactionTargetRef, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = withRequestFingerprint(ctx, reqFp)
	var finishDeltaGuard func()
	ctx, onDelta, finishDeltaGuard = guardDeltaCallback(ctx, onDelta)
	defer finishDeltaGuard()
	s.mu.Lock()
	det := s.detachedTurn
	if det == nil || det.reqFp != reqFp {
		s.mu.Unlock()
		return TurnResult{}, nil, false, nil
	}
	if det.graceTimer != nil {
		det.graceTimer.Stop()
	}
	compRef := det.compactionTarget
	det.compactionTarget = nil
	s.clearDetachedTurnLocked(true)
	s.LastUsedAt = time.Now()
	s.mu.Unlock()
	softDetachJoinedTotal.Add(1)

	defer func() {
		s.mu.Lock()
		s.leaveStreamingTurnLocked()
		s.LastUsedAt = time.Now()
		s.mu.Unlock()
	}()

	notifySessionWarn(ctx, s)
	var firstDeltaNs atomic.Int64
	wrappedDelta := wrapDeltaFirstTimer(onDelta, &firstDeltaNs)
	LogEvent("PREFILL_SOFT_DETACH_JOIN", "Singleflight joined retry onto live detached turn", map[string]any{
		"convKey":        s.ConvKey,
		"sessionId":      det.sid,
		"mode":           det.mode,
		"reqFingerprint": reqFp[:12],
	})

	if det.settledResult != nil {
		res := *det.settledResult
		if wrappedDelta != nil {
			if res.Thinking != "" {
				wrappedDelta(DeltaChunk{Type: "thinking", Thinking: res.Thinking})
			}
			if res.Text != "" {
				wrappedDelta(DeltaChunk{Type: "text", Text: res.Text})
			}
		}
		if res.Usage != nil && res.Usage.Degradation != nil && res.Usage.Degradation.Degraded {
			s.recordArgonDegradation(det.sid, res.Usage.Degradation, res.EmittedDeltas)
			s.mu.Lock()
			s.ForceRolloverNext = true
			s.ForceRolloverReason = "rollover_degraded"
			s.mu.Unlock()
		}
		finalRes, err := s.finalizeCompletedTurnWithTarget(ctx, res, det.turnStart, firstDeltaNs.Load(), messages, det.firstUserFp, det.isCompactionReq, compRef != nil, det.sid, det.mode, det.rolloverReason)
		return finalRes, compRef, true, err
	}

	if det.turn.HasDeltaChannel() {
		det.turn.AttachSubscriber(wrappedDelta)
	}

	select {
	case res := <-det.turn.resolveChan:
		det.turn.MarkResultConsumed()
		if !det.turn.HasDeltaChannel() && wrappedDelta != nil {
			if res.Thinking != "" {
				wrappedDelta(DeltaChunk{Type: "thinking", Thinking: res.Thinking})
			}
			if res.Text != "" {
				wrappedDelta(DeltaChunk{Type: "text", Text: res.Text})
			}
		}
		if len(res.ToolCalls) == 0 && det.turn.deltaDone != nil {
			<-det.turn.deltaDone
		}
		det.turn.DrainAndDetachSubscriber(ctx)
		if det.turn.releaseSem != nil {
			det.turn.releaseOnce.Do(det.turn.releaseSem)
		}
		if res.Usage != nil && res.Usage.Degradation != nil && res.Usage.Degradation.Degraded {
			s.recordArgonDegradation(det.sid, res.Usage.Degradation, res.EmittedDeltas)
			s.mu.Lock()
			s.ForceRolloverNext = true
			s.ForceRolloverReason = "rollover_degraded"
			s.mu.Unlock()
		}
		finalRes, err := s.finalizeCompletedTurnWithTarget(ctx, res, det.turnStart, firstDeltaNs.Load(), messages, det.firstUserFp, det.isCompactionReq, compRef != nil, det.sid, det.mode, det.rolloverReason)
		return finalRes, compRef, true, err
	case err := <-det.turn.errChan:
		det.turn.MarkResultConsumed()
		det.turn.CleanupSubscriber()
		if det.turn.releaseSem != nil {
			det.turn.releaseOnce.Do(det.turn.releaseSem)
		}
		if IsTosViolationError(err.Error()) {
			TripTosCircuitBreaker(err.Error())
		}
		s.preserveOrHibernateOnRpcError(err)
		compRef.release()
		return TurnResult{}, nil, true, err
	case <-ctx.Done():
		cancFp := det.cancelledHistFp
		if cancFp == "" {
			cancFp = computeMessagesSliceFingerprint(messages)
		}
		if s.tryStartSoftDetachWithMeta(det.turn, det.reqFp, det.sid, det.mode, det.rolloverReason, det.turnStart, det.turnStart, det.msgCount, det.priorMsgCount, det.firstUserFp, cancFp, det.isCompactionReq, det.promptChars, det.isStandaloneCompaction, compRef) {
			return TurnResult{}, nil, true, &softDetachError{cause: ctx.Err()}
		}
		compRef.release()
		var settled *TurnResult
		select {
		case res := <-det.turn.resolveChan:
			det.turn.MarkResultConsumed()
			settled = &res
		default:
		}
		det.turn.mu.Lock()
		alreadyUpstreamDone := det.turn.upstreamDone && !det.turn.failed
		det.turn.mu.Unlock()
		det.turn.CleanupSubscriber()
		if det.turn.releaseSem != nil {
			det.turn.releaseOnce.Do(det.turn.releaseSem)
		}
		det.turn.CancelContext()
		det.turn.closeDelta()
		if settled != nil || alreadyUpstreamDone {
			s.HibernateWorker("unclaimed_detached_rollover", true)
			s.mu.Lock()
			s.ForceRolloverNext = true
			s.ForceRolloverReason = "rollover_unclaimed_detached"
			s.mu.Unlock()
			return TurnResult{}, nil, true, ctx.Err()
		}
		s.CancelActiveTurn("client_disconnect")
		if det.sid != "" {
			s.engine().CancelSession(det.sid)
		}
		if det.isStandaloneCompaction || (det.isCompactionReq && det.priorMsgCount == 0) {
			s.HibernateWorker("oneshot_compaction_cancelled", true)
			return TurnResult{}, nil, true, ctx.Err()
		}
		s.mu.Lock()
		s.MessageCount = len(messages)
		s.lastTurnAssistant = false
		s.cancelledHistoryFp = cancFp
		s.mu.Unlock()
		return TurnResult{}, nil, true, ctx.Err()
	}
}

// cacheHitSlackTokens: a turn counts as a cache hit when the uncached part of its prompt is at
// most max(8K, 10%) tokens, i.e. only the new user/tool delta was uncached.
const (
	cacheHitSlackTokens        = session.CacheHitSlackTokens
	cacheShrinkMarginTokens    = session.CacheShrinkMarginTokens
	cachePrefixHitMarginTokens = session.CachePrefixHitMarginTokens
	cacheGapTTLMillis          = session.CacheGapTTLMillis
)

func clampedSumInt64(a, b int64) int64 {
	return session.ClampedSumInt64(a, b)
}

func turnUsageContextSplit(u *TurnUsage) (int64, int64) {
	if u == nil {
		return 0, 0
	}
	in, cr := u.ContextInputTokens, u.ContextCachedTokens
	if in == 0 && cr == 0 {
		in, cr = u.RawUncachedInputTokens, u.RawTpuCachedTokens
	}
	if in == 0 && cr == 0 {
		in, cr = u.InputTokens, u.CachedReadTokens
	}
	return int64(in), int64(cr)
}

func classifyCacheMissSpec(prevInput, prevCached, curInput, curCached int64, gapMs int64, modelSwitched bool) string {
	return session.ClassifyCacheMissSpec(prevInput, prevCached, curInput, curCached, gapMs, modelSwitched)
}

// classifyCacheMiss labels why a measured turn missed the upstream implicit cache:
// rollover/cold_start (gateway started a fresh session), model_switch, shrink (agy shortened the
// prompt), gap_ttl (idle >= 5 min, beyond the observed cache lifetime), zero (nothing cached) and
// prefix_only (only the system prefix was cached). Hits and estimated rows return "".
func classifyCacheMiss(mode string, u *TurnUsage, prev *TurnHistoryEntry, curModel string, now time.Time) string {
	return classifyCacheMissWithReason(mode, "", u, prev, curModel, now)
}

func classifyCacheMissWithReason(mode, rolloverReason string, u *TurnUsage, prev *TurnHistoryEntry, curModel string, now time.Time) string {
	if u == nil || (u.UsageSource != "db" && u.UsageSource != "acp") {
		return ""
	}
	in, cr := turnUsageContextSplit(u)
	if in < 0 {
		in = 0
	}
	if cr < 0 {
		cr = 0
	}
	total := clampedSumInt64(in, cr)
	if total <= 0 {
		return ""
	}
	slack := total / 10
	if slack < cacheHitSlackTokens {
		slack = cacheHitSlackTokens
	}
	if in <= slack {
		return ""
	}
	switch mode {
	case "cold_start":
		return "cold_start"
	case "argon_degraded_recovery", "high_capacity_fallback", "waf_autobisect_recovery":
		if rolloverReason != "" {
			return rolloverReason
		}
		return "rollover"
	case "compaction_rollover":
		if rolloverReason != "" {
			return rolloverReason
		}
		return "rollover"
	}
	var prevIn, prevCr, gapMs int64
	var modelSwitched bool
	if prev != nil {
		modelSwitched = prev.Model != "" && curModel != "" && prev.Model != curModel
		prevIn, prevCr = turnUsageContextSplit(prev.Usage)
		if prev.CompletedAtMs > 0 {
			gapMs = now.UnixMilli() - prev.CompletedAtMs
		}
	}
	return classifyCacheMissSpec(prevIn, prevCr, in, cr, gapMs, modelSwitched)
}

func promptBlocksCharLen(blocks []map[string]any) int {
	total := 0
	for _, b := range blocks {
		if t, ok := b["text"].(string); ok {
			total += len(t)
		} else if res, ok := b["resource"].(map[string]any); ok {
			if rt, okT := res["text"].(string); okT {
				total += len(rt)
			}
		}
	}
	return total
}

var rpcRetryBackoffs = []time.Duration{500 * time.Millisecond, 1500 * time.Millisecond}

type rpcRetryAbortedError struct {
	cause error
}

func (e *rpcRetryAbortedError) Error() string {
	if e == nil || e.cause == nil {
		return context.Canceled.Error()
	}
	return e.cause.Error()
}

func (e *rpcRetryAbortedError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func parseAcpRPCError(err error) (code int, msg string, ok bool) {
	var rpcErr *acpRPCError
	if !errors.As(err, &rpcErr) || rpcErr == nil {
		return 0, "", false
	}
	var parsed struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	if json.Unmarshal([]byte(rpcErr.Raw), &parsed) == nil {
		return parsed.Code, parsed.Message, true
	}
	return 0, rpcErr.Raw, true
}

func isNonTransientOrFatalUpstreamErr(err error) bool {
	if err == nil {
		return true
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || isSoftDetachedErr(err) {
		return true
	}
	var fastErr *wafFastCancelledError
	if errors.As(err, &fastErr) {
		return true
	}
	var msErr *ModelSwitchError
	if errors.As(err, &msErr) {
		return true
	}
	var mmErr *MultimodalValidationError
	if errors.As(err, &mmErr) {
		return true
	}
	var se *SoftError
	if errors.As(err, &se) && se != nil {
		if se.ErrorType == "tos_circuit_breaker" || se.StatusCode == http.StatusBadRequest ||
			se.StatusCode == http.StatusUnauthorized || se.StatusCode == http.StatusForbidden ||
			se.StatusCode == http.StatusRequestEntityTooLarge {
			return true
		}
	}
	raw := err.Error()
	if IsTosViolationError(raw) || softErrLicenseRe.MatchString(raw) ||
		softErrBlockedRe.MatchString(raw) || isUpstreamPolicyOrSafetyBlock(raw, true) {
		return true
	}
	if isAccountQuotaOrBillingExhausted(raw) {
		return true
	}
	low := strings.ToLower(raw)
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) || errors.Is(err, syscall.EPIPE) ||
		strings.Contains(low, "shared acp engine not running") ||
		strings.Contains(low, "shared acp engine exited") ||
		strings.Contains(low, "broken pipe") ||
		strings.Contains(low, "pipe is closed") ||
		strings.Contains(low, "not running") ||
		isBenignCloseSessionErr(err) {
		return true
	}
	if code, msg, isRpc := parseAcpRPCError(err); isRpc {
		switch code {
		case 400, 401, 403, -32600, -32601, -32602, -32700:
			return true
		}
		trimMsg := strings.TrimSpace(strings.ToLower(msg))
		if strings.HasPrefix(trimMsg, "400 ") || strings.HasPrefix(trimMsg, "401 ") || strings.HasPrefix(trimMsg, "403 ") {
			return true
		}
	} else if strings.Contains(low, "eof") || strings.Contains(low, "timed out") {
		return true
	}
	if strings.Contains(low, "unauthenticated") || strings.Contains(low, "permission_denied") ||
		strings.Contains(low, "code 401") || strings.Contains(low, "code 403") ||
		strings.Contains(low, "status 401") || strings.Contains(low, "status 403") ||
		strings.Contains(low, "401 unauthorized") || strings.Contains(low, "403 forbidden") ||
		strings.Contains(low, "invalid_api_key") || strings.Contains(low, "api key not valid") {
		return true
	}
	hasRateLimitMarker := strings.Contains(low, "429") || strings.Contains(low, "rate limit") ||
		strings.Contains(low, "rate_limit") || strings.Contains(low, "quota") || strings.Contains(low, "resource_exhausted")
	if strings.Contains(low, "invalid_argument") || strings.Contains(low, "failed_precondition") ||
		strings.Contains(low, "code 400") || strings.Contains(low, "status 400") ||
		strings.Contains(low, "400 bad request") ||
		(!hasRateLimitMarker && (strings.Contains(low, "thinking budget") ||
			strings.Contains(low, "prompt is too long") || strings.Contains(low, "context length exceeded") ||
			strings.Contains(low, "exceeds the maximum number of tokens"))) {
		return true
	}
	return false
}

func isRetryableAcpRpcError(err error) bool {
	if isNonTransientOrFatalUpstreamErr(err) {
		return false
	}
	code, msg, isRpc := parseAcpRPCError(err)
	if !isRpc {
		return false
	}
	switch code {
	case -32603, 429, 500, 502, 503, 504, 529:
		return true
	}
	low := strings.ToLower(msg)
	return strings.Contains(low, "-32603") || strings.Contains(low, "429") ||
		strings.Contains(low, "503") || strings.Contains(low, "529") ||
		strings.Contains(low, "model_capacity_exhausted") || strings.Contains(low, "overloaded") ||
		strings.Contains(low, "no capacity available")
}

func isTransientWarmRpcError(err error) bool {
	if isNonTransientOrFatalUpstreamErr(err) {
		return false
	}
	if isRetryableAcpRpcError(err) {
		return true
	}
	var se *SoftError
	if errors.As(err, &se) && se != nil {
		return se.StatusCode == http.StatusTooManyRequests || se.StatusCode == http.StatusServiceUnavailable || se.StatusCode == 529
	}
	return false
}

func (s *AcpProjectSession) preserveOrHibernateOnRpcError(err error) {
	s.mu.Lock()
	isWarm := s.MessageCount > 0 && s.SessionID != ""
	sidSnap := s.SessionID
	msgCntSnap := s.MessageCount
	s.mu.Unlock()
	if isWarm && isTransientWarmRpcError(err) {
		errTxt := ""
		if err != nil {
			errTxt = redactSecretsInString(truncateRunes(err.Error(), 240))
		}
		LogEvent("WARM_SESSION_PRESERVED", "Preserving warm ACP session after transient RPC error", map[string]any{
			"convKey":      s.ConvKey,
			"sessionId":    sidSnap,
			"messageCount": msgCntSnap,
			"error":        errTxt,
		})
		return
	}
	s.HibernateWorker("prompt_rpc_error", true)
}

func (s *AcpProjectSession) executePromptOrParkWithRetry(ctx context.Context, promptBlocks []map[string]any, onDelta func(DeltaChunk), contDepth int) (TurnResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var anyDelta atomic.Bool
	wrappedDelta := onDelta
	if onDelta != nil {
		wrappedDelta = func(c DeltaChunk) {
			if (c.Type == "thinking" && c.Thinking != "") || (c.Type == "text" && c.Text != "") {
				anyDelta.Store(true)
			}
			onDelta(c)
		}
	}
	for attempt := 0; ; attempt++ {
		res, err := s.executePromptOrPark(ctx, promptBlocks, wrappedDelta, contDepth)
		if err == nil {
			res.RetryCount += attempt
			return res, nil
		}
		if firstPromptPlacement() || attempt >= len(rpcRetryBackoffs) || anyDelta.Load() || ctx.Err() != nil ||
			!s.engine().IsAlive() || !isRetryableAcpRpcError(err) {
			return TurnResult{}, err
		}
		waitDur := rpcRetryBackoffs[attempt]
		s.mu.Lock()
		sidSnap := s.SessionID
		s.mu.Unlock()
		LogEvent("ACP_RPC_RETRY", "Retrying transient ACP prompt RPC error in-session", map[string]any{
			"convKey":   s.ConvKey,
			"sessionId": sidSnap,
			"attempt":   attempt + 1,
			"backoffMs": waitDur.Milliseconds(),
			"error":     redactSecretsInString(truncateRunes(err.Error(), 240)),
		})
		timer := time.NewTimer(waitDur)
		select {
		case <-ctx.Done():
			timer.Stop()
			return TurnResult{}, &rpcRetryAbortedError{cause: ctx.Err()}
		case <-timer.C:
		}
	}
}

var (
	dbRetryPollInterval = 150 * time.Millisecond
	dbRetryPollWindow   = 8500 * time.Millisecond
)

// pollTurnErrorStepsOnce reads newly committed step_type=17 rows from <sid>.db for this turn.
// Must be called without holding s.mu or turn.mu.
func (s *AcpProjectSession) pollTurnErrorStepsOnce(sid string, turn *activeTurnState, fireCallback bool) {
	if s == nil || turn == nil || sid == "" {
		return
	}
	turn.mu.Lock()
	afterIdx := turn.dbSeenErrorStepIdx
	turn.mu.Unlock()

	steps, _, ok := s.readSessionErrorStepsAfter(sid, afterIdx)
	if !ok || len(steps) == 0 {
		return
	}

	turn.mu.Lock()
	newHidden, maxAttempt, latestDetail := turn.applyDBErrorStepsLocked(steps)
	retryCb := turn.onRetryNotice
	canFastCancel := !turn.emittedText && len(turn.thoughtChks) == 0 && len(turn.toolCalls) == 0 &&
		!turn.fastCancelledForBisect && !turn.resolved
	shouldLogSilent := false
	if newHidden && turn.dbRetryCount > turn.apiRetryCount && turn.dbRetryCount > turn.dbLoggedRetryCount {
		turn.dbLoggedRetryCount = turn.dbRetryCount
		shouldLogSilent = true
	}
	turn.mu.Unlock()

	if shouldLogSilent {
		meta := map[string]any{"convKey": s.ConvKey, "source": "sqlite"}
		if maxAttempt > 0 {
			meta["attempt"] = maxAttempt
		}
		if latestDetail != "" {
			meta["detail"] = latestDetail
		}
		LogEvent("RETRY_NOTICE_FILTERED", "Observed silent agy retry step in session DB", meta)
	}
	if fireCallback && newHidden && canFastCancel && retryCb != nil && maxAttempt > 0 {
		retryCb(maxAttempt, latestDetail)
	}
}

// startDBRetryWatcher runs a bounded short-lived poller during the fast-cancel window (first ~8s)
// for WAF bisect probes and cold-start first turns, restoring real-time retry sensing on agy >= 1.5.13
// where internal retry steps are written to <sid>.db WAL but withheld from ACP agent_message_chunk.
func (s *AcpProjectSession) startDBRetryWatcher(sid string, turn *activeTurnState) {
	if s == nil || turn == nil || sid == "" {
		return
	}
	interval := dbRetryPollInterval
	if interval <= 0 {
		interval = 150 * time.Millisecond
	}
	window := dbRetryPollWindow
	if window <= 0 {
		window = 8500 * time.Millisecond
	}
	ctx := turn.turnCtx
	if ctx == nil {
		ctx = context.Background()
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			turn.mu.Lock()
			stop := turn.resolved || turn.emittedText || len(turn.thoughtChks) > 0 ||
				len(turn.toolCalls) > 0 || turn.fastCancelledForBisect ||
				time.Since(turn.createdAt) >= window
			turn.mu.Unlock()
			if stop {
				return
			}
			s.pollTurnErrorStepsOnce(sid, turn, true)
		}
	}()
}

func (s *AcpProjectSession) executePromptOrPark(ctx context.Context, promptBlocks []map[string]any, onDelta func(DeltaChunk), contDepth int) (TurnResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	sem := promptSem
	select {
	case sem <- struct{}{}:
	case <-ctx.Done():
		return TurnResult{}, ctx.Err()
	}

	if err := circuitError(); err != nil {
		<-sem
		return TurnResult{}, err
	}
	s.mu.Lock()
	s.promptGen++
	myPromptGen := s.promptGen
	myEngineGen := s.engineGen
	s.PromptStepCount = 1
	s.orphanRelayHops = 0
	s.lastOrphanRelayAt = time.Time{}
	s.orphanCapLogged = false
	s.RecentToolSigs = nil
	s.PendingSiblingCalls = nil
	s.RelayedCalls = nil
	s.reportedToolCalls = make(map[string]reportedToolIdentity)
	if s.LastSeenDbStepIdx > s.LastPlannerBatchIdx {
		s.LastPlannerBatchIdx = s.LastSeenDbStepIdx
	}
	s.LastPlannerBatchNextPos = 0
	s.LastPlannerBatchTotal = 0
	s.PromptInFlight = true
	sid := s.SessionID
	clearCancelledWatermark := s.wasCancelledTurn && contDepth == 0
	if clearCancelledWatermark {
		s.wasCancelledTurn = false
	}
	isProbe := s.isWafBisectProbe
	allowFast := s.allowWafFastCancel && contDepth == 0 && s.MessageCount == 0
	codec := s.Codec
	turn := newActiveTurnStateWithCodec(onDelta, func() { <-sem }, codec)
	if isProbe || allowFast {
		turn.onRetryNotice = func(attempt int, detail string) {
			if !isBisectableInstant429Detail(detail) {
				return
			}
			turn.mu.Lock()
			elapsed := time.Since(turn.createdAt)
			minAtt := 2
			if isProbe {
				minAtt = 1
			}
			shouldCancel := !turn.fastCancelledForBisect && !turn.resolved && !turn.emittedText &&
				len(turn.thoughtChks) == 0 && len(turn.toolCalls) == 0 &&
				attempt >= minAtt && elapsed < 8*time.Second
			if shouldCancel {
				turn.fastCancelledForBisect = true
				turn.fastCancelDetail = detail
			}
			turn.mu.Unlock()
			if shouldCancel {
				if !isProbe {
					LogEvent("WAF_FAST_CANCEL", "Aborted first-turn instant 429 retry loop to run WAF auto-bisect", map[string]any{
						"convKey":   s.ConvKey,
						"sessionId": sid,
						"attempt":   attempt,
						"elapsedMs": elapsed.Milliseconds(),
						"detail":    detail,
					})
				}
				go s.engine().CancelSession(sid)
			}
		}
	}
	s.activeTurn = turn
	s.syncPhaseLocked()
	s.mu.Unlock()
	if clearCancelledWatermark {
		s.advanceUsageWatermarks(sid)
	}
	turn.setStartAllStepIdx(s.readMaxSessionAllStepIdx(sid))
	if isProbe || allowFast {
		s.startDBRetryWatcher(sid, turn)
	}

	promptChars := promptBlocksCharLen(promptBlocks)

	origTurn := turn
	go func() {
		defer close(origTurn.bgDone)
		defer origTurn.releaseOnce.Do(origTurn.releaseSem)
		rawRes, err := s.engine().RPC("session/prompt", map[string]any{
			"sessionId": sid,
			"prompt":    promptBlocks,
		}, 3600*time.Second)

		s.mu.Lock()
		staleOwner := s.promptGen != myPromptGen || s.SessionID != sid || s.engineGen != myEngineGen
		target := turn
		if !staleOwner {
			s.PromptInFlight = false
			s.PendingSiblingCalls = nil
			s.RelayedCalls = nil
			s.reportedToolCalls = nil
			cur := s.activeTurn
			// After a parked tool resumes, resumeParkedTurn installs a NEW activeTurn on the same in-flight
			// prompt; the original turn was already resolved (tool_use) without failing. Resolve whichever turn is current.
			turn.mu.Lock()
			origResolved := turn.resolved
			origFailed := turn.failed
			turn.mu.Unlock()
			if origResolved && !origFailed && cur != nil && cur != turn {
				target = cur
			}
			if cur == target || cur == turn {
				s.activeTurn = nil
			}
			s.syncPhaseLocked()
		}
		s.mu.Unlock()
		turn := target // shadow: do not mutate the outer variable from this goroutine
		defer func() {
			if turn.releaseSem != nil {
				turn.releaseOnce.Do(turn.releaseSem)
			}
		}()

		turn.mu.Lock()
		if turn.batchTimer != nil {
			turn.batchTimer.Stop()
		}
		alreadyResolved := turn.resolved
		turn.mu.Unlock()

		if alreadyResolved {
			if !staleOwner {
				s.promotePendingTierIfIdle()
				s.mu.Lock()
				wasCanc := s.wasCancelledTurn
				s.mu.Unlock()
				if wasCanc {
					s.advanceUsageWatermarks(sid)
				}
			}
			return
		}

		if err != nil {
			if !staleOwner {
				s.promotePendingTierIfIdle()
				s.advanceUsageWatermarks(sid)
			}
			turn.mu.Lock()
			fastCancelled := turn.fastCancelledForBisect
			fastDetail := turn.fastCancelDetail
			turn.resolved = true
			turn.failed = true
			turn.upstreamDone = true
			turn.mu.Unlock()
			turn.CancelContext()
			turn.closeDelta()
			if turn.releaseSem != nil {
				turn.releaseOnce.Do(turn.releaseSem)
			}
			outErr := err
			if fastCancelled {
				outErr = &wafFastCancelledError{Detail: fastDetail}
			}
			select {
			case turn.errChan <- outErr:
			default:
			}
			return
		}

		var parsed struct {
			StopReason string         `json:"stopReason"`
			Usage      map[string]any `json:"usage"`
		}
		_ = json.Unmarshal(rawRes, &parsed)

		turn.mu.Lock()
		prevFinishInfo := turn.noticeInfo
		held := turn.finishNoticeLocked()
		finishDroppedInfo := turn.noticeInfo > prevFinishInfo
		finishRetryAttempt := turn.lastRetryAttempt
		finishRetryDetail := turn.lastRetryDetail
		if held != "" {
			turn.textChunks = append(turn.textChunks, held)
		}
		turn.mu.Unlock()
		if finishDroppedInfo {
			meta := map[string]any{"convKey": s.ConvKey}
			if finishRetryAttempt > 0 {
				meta["attempt"] = finishRetryAttempt
			}
			if finishRetryDetail != "" {
				meta["detail"] = finishRetryDetail
			}
			LogEvent("RETRY_NOTICE_FILTERED", "Dropped internal agy retry/limit/cancel notice", meta)
		}
		if held != "" {
			turn.pushDelta(DeltaChunk{Type: "text", Text: held})
		}
		s.pollTurnErrorStepsOnce(sid, turn, false)

		turn.mu.Lock()
		txt := strings.Join(turn.textChunks, "")
		thk := strings.Join(turn.thoughtChks, "")
		tcalls := turn.toolCalls
		effectiveNotice := formatTerminalNoticeWithDetail(turn.noticeText, turn.effectiveTerminalDetailLocked())
		if effectiveNotice != "" {
			LogEvent("AGY_NOTICE_FILTERED", "Withheld agy terminal notice from client stream", map[string]any{
				"convKey": s.ConvKey, "notice": effectiveNotice, "replyChars": len(txt),
			})
		}
		failedNotice := ""
		if turn.noticeFailed && len(tcalls) == 0 && strings.TrimSpace(txt) != "" {
			// agy failed after part of the reply was already produced (R5): report a failure
			failedNotice = effectiveNotice
		}
		txt = noticeOnlyText(txt, effectiveNotice, len(tcalls))
		if s.Codec != nil {
			if DetectAntigravitySoftError(txt) == nil {
				txt = s.Codec.RestoreText(txt)
			}
			thk = s.Codec.RestoreText(thk)
		}
		fastCancelled := turn.fastCancelledForBisect
		fastDetail := turn.fastCancelDetail
		firstAt := turn.firstChunk
		if firstAt.IsZero() {
			firstAt = time.Now()
		}
		ttft := int(firstAt.Sub(turn.createdAt).Milliseconds())
		if ttft < 10 {
			ttft = 10
		}
		thinkingMs := turn.computeThinkingMsLocked()
		retryCnt := turn.retryCountLocked()
		turn.resolved = true
		turn.upstreamDone = true
		turn.mu.Unlock()

		if parsed.StopReason == "max_tokens" && contDepth < 3 && len(tcalls) == 0 && failedNotice == "" {
			if !staleOwner {
				s.promotePendingTierIfIdle()
			}
			turn.closeDelta()
			emittedSoFar := int(turn.emittedDeltas.Load())
			if turn.releaseSem != nil {
				turn.releaseOnce.Do(turn.releaseSem)
			}
			LogEvent("AUTO_CONTINUE", fmt.Sprintf("Auto-continuing truncated response (depth=%d)", contDepth+1), map[string]any{
				"convKey":   s.ConvKey,
				"sessionId": sid,
			})
			var contDelta func(DeltaChunk)
			if turn.HasDeltaChannel() {
				contDelta = func(c DeltaChunk) { turn.emitSubscriberDelta(c) }
			}
			contCtx := turn.turnCtx
			if contCtx == nil {
				contCtx = context.Background()
			}
			nextRes, contErr := s.executePromptOrPark(contCtx, []map[string]any{
				{"type": "text", "text": "Continue exactly where you stopped. Do not repeat anything already written."},
			}, contDelta, contDepth+1)
			if contErr != nil {
				turn.mu.Lock()
				turn.failed = true
				turn.mu.Unlock()
				turn.CancelContext()
				turn.closeDelta()
				select {
				case turn.errChan <- contErr:
				default:
				}
				return
			}
			if strings.TrimSpace(txt) != "" && nextRes.FailedNotice == "" && len(nextRes.ToolCalls) == 0 && DetectAntigravitySoftError(nextRes.Text) != nil {
				nextRes.FailedNotice = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(nextRes.Text), "⚠"))
				nextRes.Text = txt
			} else {
				nextRes.Text = txt + nextRes.Text
			}
			nextRes.Thinking = thk + nextRes.Thinking
			nextRes.EmittedDeltas += emittedSoFar
			if nextRes.Usage != nil && nextRes.Usage.UsageSource == "acp" && parsed.Usage != nil {
				if hopOut, ok := toInt(parsed.Usage["outputTokens"]); ok && hopOut > 0 {
					nextRes.Usage.OutputTokens += hopOut
				}
				if hopTh, ok := toInt(parsed.Usage["thoughtTokens"]); ok && hopTh > 0 {
					nextRes.Usage.ThinkingTokens += hopTh
				}
			}
			nextRes.FirstChunkAt = firstAt
			nextRes.TtftMs = ttft
			nextRes.ThinkingMs += thinkingMs
			nextRes.RetryCount += retryCnt
			turn.CancelContext()
			turn.closeDelta()
			select {
			case turn.resolveChan <- nextRes:
			default:
			}
			return
		}

		if parsed.StopReason == "cancelled" {
			// The prompt was cancelled upstream (session closed, cancel, engine restart). Never
			// report it as a successful (possibly empty) end_turn, and advance generation cursors
			// so aborted rows in <sid>.db are never summed into a subsequent turn.
			if !staleOwner {
				s.promotePendingTierIfIdle()
			}
			turn.mu.Lock()
			turn.failed = true
			turn.mu.Unlock()
			s.advanceUsageWatermarks(sid)
			turn.CancelContext()
			turn.closeDelta()
			if turn.releaseSem != nil {
				turn.releaseOnce.Do(turn.releaseSem)
			}
			if fastCancelled {
				select {
				case turn.errChan <- &wafFastCancelledError{Detail: fastDetail}:
				default:
				}
				return
			}
			LogEvent("TURN_CANCELLED", "Upstream prompt ended with stopReason=cancelled", map[string]any{
				"convKey": s.ConvKey, "sessionId": sid, "replyChars": len(txt),
			})
			select {
			case turn.errChan <- &SoftError{StatusCode: http.StatusServiceUnavailable, ErrorType: "api_error", Message: "Antigravity upstream cancelled the turn before it completed"}:
			default:
			}
			return
		}

		turn.CancelContext()
		turn.closeDelta()
		emitted := int(turn.emittedDeltas.Load())
		usage := s.normalizeTurnUsageAsync(parsed.Usage, false, promptChars, len(txt)+len(thk))
		if turn.releaseSem != nil {
			turn.releaseOnce.Do(turn.releaseSem)
		}
		finalStop := "end_turn"
		if parsed.StopReason == "max_tokens" {
			finalStop = "max_tokens"
		}
		select {
		case turn.resolveChan <- TurnResult{
			StopReason:    finalStop,
			Text:          txt,
			Thinking:      thk,
			ToolCalls:     nil,
			Usage:         usage,
			TtftMs:        ttft,
			ThinkingMs:    thinkingMs,
			RetryCount:    retryCnt,
			EmittedDeltas: emitted,
			FirstChunkAt:  firstAt,
			FailedNotice:  failedNotice,
		}:
		default:
		}
	}()

	waitStart := time.Now()
	select {
	case res := <-turn.resolveChan:
		turn.MarkResultConsumed()
		turn.DrainAndDetachSubscriber(ctx)
		return res, nil
	case err := <-turn.errChan:
		turn.MarkResultConsumed()
		turn.CleanupSubscriber()
		return TurnResult{}, err
	case <-ctx.Done():
		if contDepth == 0 {
			reqFp := requestFingerprintFromContext(ctx)
			meta := turnDetachMetaFromContext(ctx)
			sidDet := sid
			modeDet := "cold_start"
			rollDet := ""
			turnStartDet := waitStart
			msgCountDet := 0
			firstUserDet := ""
			cancFpDet := ""
			isCompDet := false
			priorMsgDet := 0
			isStandaloneDet := isStandaloneCompactionFromContext(ctx)
			if meta != nil {
				if meta.sid != "" {
					sidDet = meta.sid
				}
				if meta.mode != "" {
					modeDet = meta.mode
				}
				rollDet = meta.rolloverReason
				if !meta.turnStart.IsZero() {
					turnStartDet = meta.turnStart
				}
				msgCountDet = meta.msgCount
				priorMsgDet = meta.priorMsgCount
				firstUserDet = meta.firstUserFp
				cancFpDet = meta.resolveCancelledHistFp()
				isCompDet = meta.isCompactionReq
				if meta.isStandaloneCompaction {
					isStandaloneDet = true
				}
			}
			if s.tryStartSoftDetachWithMeta(turn, reqFp, sidDet, modeDet, rollDet, turnStartDet, waitStart, msgCountDet, priorMsgDet, firstUserDet, cancFpDet, isCompDet, promptChars, isStandaloneDet, nil) {
				return TurnResult{}, &softDetachError{cause: ctx.Err()}
			}
		}
		turn.CancelContext()
		turn.CleanupSubscriber()
		s.CancelActiveTurn("client_disconnect")
		return TurnResult{}, ctx.Err()
	}
}

func (s *AcpProjectSession) recordArgonDegradation(sid string, deg *ArgonDegradationReport, emittedDeltas int) {
	if deg == nil || !deg.Degraded {
		return
	}
	argonDegradationDetectedTotal.Add(1)
	LogEvent("ARGON_DEGRADATION_DETECTED", fmt.Sprintf("Detected Gemini 4 Argon shadow pool routing (served=%s)", deg.ServedModel), map[string]any{
		"servedModel":   deg.ServedModel,
		"convKey":       s.ConvKey,
		"sessionId":     sid,
		"cwmEstTokens":  deg.CwmEstTokens,
		"promptTokens":  deg.PromptTokens,
		"cachedTokens":  deg.CachedTokens,
		"outputTokens":  deg.OutputTokens,
		"thoughtTokens": deg.ThoughtTokens,
		"ttftSeconds":   deg.TtftSeconds,
		"streamSeconds": deg.StreamSeconds,
		"emittedDeltas": emittedDeltas,
	})
}

func wrapDeltaFirstTimer(onDelta func(DeltaChunk), firstDeltaNs *atomic.Int64) func(DeltaChunk) {
	if onDelta == nil || firstDeltaNs == nil {
		return onDelta
	}
	return func(c DeltaChunk) {
		if (c.Type == "thinking" && c.Thinking != "") || (c.Type == "text" && c.Text != "") {
			firstDeltaNs.CompareAndSwap(0, time.Now().UnixNano())
		}
		onDelta(c)
	}
}

func resolveTurnTtftMs(turnStart time.Time, elapsedMs int, firstDeltaNs int64, res TurnResult) int {
	ttft := 0
	if firstDeltaNs > 0 {
		ttft = int((firstDeltaNs - turnStart.UnixNano()) / 1e6)
	} else {
		if !res.FirstChunkAt.IsZero() && !res.FirstChunkAt.Before(turnStart) {
			ttft = int(res.FirstChunkAt.Sub(turnStart).Milliseconds())
		} else if res.TtftMs > 0 {
			ttft = res.TtftMs
		} else {
			ttft = elapsedMs
		}
		if res.Usage != nil && res.Usage.StreamMs > 0 && elapsedMs > res.Usage.StreamMs {
			derived := elapsedMs - res.Usage.StreamMs
			if ttft <= 0 || ttft > derived {
				ttft = derived
			}
		}
	}
	if ttft < 10 && elapsedMs >= 10 {
		ttft = 10
	} else if ttft < 1 && elapsedMs > 0 {
		ttft = 1
	}
	if elapsedMs > 0 && ttft > elapsedMs {
		ttft = elapsedMs
	}
	return ttft
}

func (s *AcpProjectSession) resumeParkedTurn(ctx context.Context, matched []ToolResultInput, messages []map[string]any, onDelta func(DeltaChunk)) (TurnResult, error) {
	var firstDeltaNs atomic.Int64
	t0 := time.Now()
	return s.resumeParkedTurnTimed(ctx, matched, messages, wrapDeltaFirstTimer(onDelta, &firstDeltaNs), t0, &firstDeltaNs)
}

func formatParkedToolResultText(tr ToolResultInput, codec *ReversibleSessionCodec, emptyFallback string) string {
	body := stripRedundantSystemReminders(tr.Content)
	if body == "" && emptyFallback != "" {
		body = emptyFallback
	}
	combined := body
	if tr.SystemTail != "" {
		if combined == "" {
			combined = tr.SystemTail
		} else {
			combined = combined + "\n\n" + tr.SystemTail
		}
	}
	if codec != nil && combined != "" {
		combined = codec.EncodeOutboundText(combined)
	}
	return combined
}

func (s *AcpProjectSession) resumeParkedTurnTimed(ctx context.Context, matched []ToolResultInput, messages []map[string]any, onDelta func(DeltaChunk), t0 time.Time, firstDeltaNs *atomic.Int64) (TurnResult, error) {
	return s.resumeParkedTurnInternal(ctx, matched, messages, onDelta, t0, firstDeltaNs, nil, nil)
}

func (s *AcpProjectSession) resumeParkedTurnInternal(ctx context.Context, matched []ToolResultInput, messages []map[string]any, onDelta func(DeltaChunk), t0 time.Time, firstDeltaNs *atomic.Int64, abortContentBlocks []map[string]any, fallbackPromptBlocks []map[string]any) (TurnResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if t0.IsZero() {
		t0 = time.Now()
	}
	meta := turnDetachMetaFromContext(ctx)
	currentFirstFp := ""
	isCompactionReq := false
	if meta != nil {
		currentFirstFp = meta.firstUserFp
		isCompactionReq = meta.isCompactionReq
	} else {
		isCompactionReq = isCompactionSummaryRequest(messages)
	}
	if currentFirstFp == "" {
		currentFirstFp = computeFirstUserFingerprint(messages)
	}

	s.mu.Lock()
	s.PromptStepCount++
	s.orphanRelayHops = 0
	s.lastOrphanRelayAt = time.Time{}
	s.orphanCapLogged = false
	inFlight := s.PromptInFlight
	sid := s.SessionID
	codec := s.Codec
	s.mu.Unlock()

	handleParkedCompletion := func(val TurnResult) (TurnResult, error) {
		if val.Usage != nil && val.Usage.Degradation != nil && val.Usage.Degradation.Degraded {
			s.recordArgonDegradation(sid, val.Usage.Degradation, val.EmittedDeltas)
			s.mu.Lock()
			s.ForceRolloverNext = true
			s.ForceRolloverReason = "rollover_degraded"
			s.mu.Unlock()
		}
		var fdNs int64
		if firstDeltaNs != nil {
			fdNs = firstDeltaNs.Load()
		}
		return s.finalizeCompletedTurn(ctx, val, t0, fdNs, messages, currentFirstFp, isCompactionReq, sid, "parked_mcp_resume", "")
	}

	runNewPromptResume := func() (TurnResult, error) {
		s.mu.Lock()
		if len(fallbackPromptBlocks) > 0 {
			for k, p := range s.ParkedCalls {
				select {
				case p.ResultChan <- map[string]any{
					"content": []map[string]any{{"type": "text", "text": "Host user interrupted the pending tool execution; follow the updated instruction."}},
					"isError": true,
				}:
				default:
				}
				delete(s.ParkedCalls, k)
			}
			for _, sib := range s.PendingSiblingCalls {
				select {
				case sib.ResultChan <- map[string]any{
					"content": []map[string]any{{"type": "text", "text": "Host user interrupted the pending tool execution; follow the updated instruction."}},
					"isError": true,
				}:
				default:
				}
			}
		}
		s.ParkedCalls = make(map[string]*ParkedMcpCall)
		s.PendingSiblingCalls = nil
		s.RelayedCalls = nil
		s.LastDeliveredCallIDs = nil
		s.ParkedSince = time.Time{}
		s.mu.Unlock()
		blocks := fallbackPromptBlocks
		if len(blocks) == 0 {
			var textLines []string
			for _, tr := range matched {
				cleanTxt := formatParkedToolResultText(tr, codec, "")
				textLines = append(textLines, fmt.Sprintf("[Tool Result (%s)]: %s", tr.ToolCallID, cleanTxt))
				for _, img := range tr.Images {
					blocks = append(blocks, img)
				}
			}
			textLines = append(textLines, "\nContinue the task with these tool results.")
			blocks = append(blocks, map[string]any{"type": "text", "text": strings.Join(textLines, "\n\n")})
		}
		val, err := s.executePromptOrParkWithRetry(ctx, blocks, onDelta, 0)
		if err != nil {
			if isSoftDetachedErr(err) {
				return TurnResult{}, err
			}
			if IsTosViolationError(err.Error()) {
				TripTosCircuitBreaker(err.Error())
			}
			var retryAbort *rpcRetryAbortedError
			if errors.As(err, &retryAbort) {
				return TurnResult{}, err
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				var cancFp string
				if meta != nil {
					cancFp = meta.resolveCancelledHistFp()
				}
				if cancFp == "" {
					cancFp = computeMessagesSliceFingerprint(messages)
				}
				s.mu.Lock()
				s.MessageCount = len(messages)
				s.lastTurnAssistant = false
				s.cancelledHistoryFp = cancFp
				s.mu.Unlock()
				return TurnResult{}, err
			}
			s.preserveOrHibernateOnRpcError(err)
			return TurnResult{}, err
		}
		return handleParkedCompletion(val)
	}

	if !inFlight {
		return runNewPromptResume()
	}

	sem := promptSem
	select {
	case sem <- struct{}{}:
	case <-ctx.Done():
		s.CancelActiveTurn("client_disconnect")
		return TurnResult{}, ctx.Err()
	}

	if err := circuitError(); err != nil {
		<-sem
		return TurnResult{}, err
	}
	var preformattedTexts []string
	if len(abortContentBlocks) == 0 && len(matched) > 0 {
		preformattedTexts = make([]string, len(matched))
		for i, tr := range matched {
			preformattedTexts[i] = formatParkedToolResultText(tr, codec, "OK")
		}
	}
	turn := newActiveTurnStateWithCodec(onDelta, func() { <-sem }, codec)
	turn.setStartAllStepIdx(s.readMaxSessionAllStepIdx(sid))
	var lateCalls []ToolCallItem
	s.mu.Lock()
	if !s.PromptInFlight {
		s.mu.Unlock()
		turn.closeDelta()
		turn.CleanupSubscriber()
		turn.releaseOnce.Do(turn.releaseSem)
		return runNewPromptResume()
	}
	s.activeTurn = turn
	s.syncPhaseLocked()
	if len(abortContentBlocks) > 0 {
		pendingSet := make(map[string]bool, len(s.PendingSiblingCalls))
		for _, sib := range s.PendingSiblingCalls {
			if sib != nil && sib.Call.ID != "" {
				pendingSet[sib.Call.ID] = true
			}
		}
		isPendingSib := func(id string, p *ParkedMcpCall) bool {
			return pendingSet[id] || (p != nil && p.PreDispatched)
		}

		primaryID := ""
		// Prefer actively awaited / relayed calls over un-awaited pre-dispatched siblings.
		for i := len(s.LastDeliveredCallIDs) - 1; i >= 0; i-- {
			cand := s.LastDeliveredCallIDs[i]
			if p, ok := s.ParkedCalls[cand]; ok && !isPendingSib(cand, p) && s.RelayedCalls[cand] != nil {
				primaryID = cand
				break
			}
		}
		if primaryID == "" {
			for i := len(s.LastDeliveredCallIDs) - 1; i >= 0; i-- {
				cand := s.LastDeliveredCallIDs[i]
				if p, ok := s.ParkedCalls[cand]; ok && !isPendingSib(cand, p) {
					primaryID = cand
					break
				}
			}
		}
		if primaryID == "" && len(s.ParkedCalls) > 0 {
			ids := make([]string, 0, len(s.ParkedCalls))
			for id, p := range s.ParkedCalls {
				if !isPendingSib(id, p) {
					ids = append(ids, id)
				}
			}
			sort.Strings(ids)
			for _, id := range ids {
				if s.RelayedCalls[id] != nil {
					primaryID = id
					break
				}
			}
			if primaryID == "" && len(ids) > 0 {
				primaryID = ids[0]
			}
		}
		if primaryID == "" && len(s.RelayedCalls) > 0 {
			ids := make([]string, 0, len(s.RelayedCalls))
			for id, p := range s.RelayedCalls {
				if !isPendingSib(id, p) {
					ids = append(ids, id)
				}
			}
			sort.Strings(ids)
			if len(ids) > 0 {
				primaryID = ids[0]
			}
		}
		// Fallback to a pending sibling only if NO non-pending ParkedCalls/RelayedCalls entry exists.
		if primaryID == "" {
			for i := len(s.LastDeliveredCallIDs) - 1; i >= 0; i-- {
				cand := s.LastDeliveredCallIDs[i]
				if _, ok := s.ParkedCalls[cand]; ok {
					primaryID = cand
					break
				}
			}
		}
		if primaryID == "" && len(s.ParkedCalls) > 0 {
			ids := make([]string, 0, len(s.ParkedCalls))
			for id := range s.ParkedCalls {
				ids = append(ids, id)
			}
			sort.Strings(ids)
			primaryID = ids[0]
		}
		if primaryID != "" {
			primary := s.ParkedCalls[primaryID]
			if primary == nil && s.RelayedCalls != nil {
				primary = s.RelayedCalls[primaryID]
			}
			delete(s.ParkedCalls, primaryID)
			if primary != nil {
				select {
				case primary.ResultChan <- map[string]any{
					"content": abortContentBlocks,
					"isError": true,
				}:
				default:
				}
			}
		}
		for id, parked := range s.ParkedCalls {
			delete(s.ParkedCalls, id)
			select {
			case parked.ResultChan <- map[string]any{
				"content": []map[string]any{{"type": "text", "text": "Host user interrupted the pending tool execution; follow the updated instruction delivered in this batch."}},
				"isError": true,
			}:
			default:
			}
		}
		for _, sib := range s.PendingSiblingCalls {
			select {
			case sib.ResultChan <- map[string]any{
				"content": []map[string]any{{"type": "text", "text": "Host user interrupted the pending tool execution; follow the updated instruction delivered in this batch."}},
				"isError": true,
			}:
			default:
			}
		}
		s.PendingSiblingCalls = nil
		s.ParkedSince = time.Time{}
		s.LastDeliveredCallIDs = nil
	} else {
		for i, tr := range matched {
			if parked, ok := s.ParkedCalls[tr.ToolCallID]; ok {
				delete(s.ParkedCalls, tr.ToolCallID)
				txt := preformattedTexts[i]
				contentBlocks := []map[string]any{{"type": "text", "text": txt}}
				contentBlocks = append(contentBlocks, tr.Images...)
				select {
				case parked.ResultChan <- map[string]any{
					"content": contentBlocks,
					"isError": tr.IsError,
				}:
				default:
				}
			}
		}
		for id, parked := range s.ParkedCalls {
			if !parked.Delivered {
				// Late parallel tool call arrived after the 80ms batch window: keep it parked and deliver it now!
				lateCalls = append(lateCalls, parked.Call)
				continue
			}
			delete(s.ParkedCalls, id)
			select {
			case parked.ResultChan <- map[string]any{
				"content": []map[string]any{{"type": "text", "text": "Tool result was not provided by host client."}},
				"isError": true,
			}:
			default:
			}
		}
		if len(s.ParkedCalls) == 0 {
			s.ParkedSince = time.Time{}
			s.LastDeliveredCallIDs = nil
		}
	}
	s.mu.Unlock()

	for _, lc := range lateCalls {
		s.enqueueToolCallToTurn(lc)
	}

	waitStart := time.Now()
	select {
	case val := <-turn.resolveChan:
		turn.MarkResultConsumed()
		turn.DrainAndDetachSubscriber(ctx)
		if len(val.ToolCalls) == 0 && val.StopReason != "tool_use" {
			turn.CancelContext()
		}
		turn.releaseOnce.Do(turn.releaseSem)
		return handleParkedCompletion(val)
	case err := <-turn.errChan:
		turn.MarkResultConsumed()
		turn.CleanupSubscriber()
		turn.CancelContext()
		turn.releaseOnce.Do(turn.releaseSem)
		if err != nil && IsTosViolationError(err.Error()) {
			TripTosCircuitBreaker(err.Error())
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return TurnResult{}, err
		}
		s.preserveOrHibernateOnRpcError(err)
		return TurnResult{}, err
	case <-ctx.Done():
		reqFp := requestFingerprintFromContext(ctx)
		turnStartDet := t0
		firstFpDet := currentFirstFp
		isCompDet := isCompactionReq
		priorMsgDet := len(messages)
		isStandaloneDet := isStandaloneCompactionFromContext(ctx)
		var cancFpDet string
		if meta != nil {
			if !meta.turnStart.IsZero() {
				turnStartDet = meta.turnStart
			}
			if meta.priorMsgCount > 0 {
				priorMsgDet = meta.priorMsgCount
			}
			cancFpDet = meta.resolveCancelledHistFp()
			if meta.isStandaloneCompaction {
				isStandaloneDet = true
			}
		}
		if cancFpDet == "" {
			cancFpDet = computeMessagesSliceFingerprint(messages)
		}
		if s.tryStartSoftDetachWithMeta(turn, reqFp, sid, "parked_mcp_resume", "", turnStartDet, waitStart, len(messages), priorMsgDet, firstFpDet, cancFpDet, isCompDet, 0, isStandaloneDet, nil) {
			return TurnResult{}, &softDetachError{cause: ctx.Err()}
		}
		turn.CancelContext()
		turn.CleanupSubscriber()
		s.CancelActiveTurn("client_disconnect")
		s.mu.Lock()
		s.MessageCount = len(messages)
		s.lastTurnAssistant = false
		s.cancelledHistoryFp = cancFpDet
		s.mu.Unlock()
		return TurnResult{}, ctx.Err()
	}
}

func (s *AcpProjectSession) applyParkedResumeModelSelection(sel *ModelSelection) error {
	if s == nil || sel == nil || sel.Model == nil {
		return nil
	}
	s.mu.Lock()
	sid := s.SessionID
	curModel := s.ModelID
	curTier := s.ThoughtLevel
	curPending := s.PendingThoughtLevel
	s.mu.Unlock()
	if err := s.applyModelSelection(*sel); err != nil {
		s.mu.Lock()
		if s.SessionID == sid && s.ModelID == "" && curModel != "" {
			s.ModelID = curModel
			s.ThoughtLevel = curTier
			s.PendingThoughtLevel = curPending
		}
		s.mu.Unlock()
		if !sel.ExplicitTier {
			LogEvent("PARKED_TIER_SWITCH_SKIPPED", "Failed in-place tier switch during parked MCP resume; continuing on current tier", map[string]any{
				"convKey":     s.ConvKey,
				"sessionId":   sid,
				"model":       curModel,
				"currentTier": curTier,
				"targetTier":  sel.Tier,
				"error":       err.Error(),
			})
			return nil
		}
		return err
	}
	return nil
}

func (s *AcpProjectSession) RunTurn(ctx context.Context, messages []map[string]any, toolsCatalog []ToolDef, thoughtLevel string, onDelta func(DeltaChunk), sel *ModelSelection) (TurnResult, error) {
	if err := circuitError(); err != nil {
		return TurnResult{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var finishDeltaGuard func()
	ctx, onDelta, finishDeltaGuard = guardDeltaCallback(ctx, onDelta)
	defer finishDeltaGuard()
	turnStart := time.Now()
	if reqStart, ok := httpRequestStartFromContext(ctx); ok && !reqStart.After(turnStart) {
		turnStart = reqStart
	}
	var firstDeltaNs atomic.Int64
	onDelta = wrapDeltaFirstTimer(onDelta, &firstDeltaNs)
	turnDir := turnDirectiveFromContext(ctx)
	tailDirective := buildTurnDirectivePromptTail(turnDir, toolsCatalog)
	s.mu.Lock()
	prevLastUsedAt := s.LastUsedAt
	s.LastUsedAt = turnStart
	s.lastTurnExplicitTier = sel != nil && sel.ExplicitTier
	s.enterStreamingTurnLocked(turnDir.ToolChoice)
	hasParked := len(s.ParkedCalls) > 0
	initMsgCnt := s.MessageCount
	initSys := s.SystemText
	initModel := s.ModelID
	codec := s.Codec
	var initTools []ToolDef
	var initToolsFp string
	initPtr := toolSliceDataPtr(s.InitialToolsCatalog)
	initLen := len(s.InitialToolsCatalog)
	if initLen > 0 && codec != nil {
		if s.cachedInitToolsPtr == initPtr && s.cachedInitToolsLen == initLen && s.cachedInitToolsFp != "" {
			initToolsFp = s.cachedInitToolsFp
		}
		if initToolsFp == "" || !codec.IsToolsFPSeeded(initToolsFp) {
			initTools = append([]ToolDef(nil), s.InitialToolsCatalog...)
		}
	}
	s.mu.Unlock()
	if requestFingerprintFromContext(ctx) == "" {
		fpModel := initModel
		if sel != nil && sel.Model != nil && sel.Model.ID != "" {
			fpModel = sel.Model.ID
		}
		if fp := computeTurnRequestFingerprintWithDirective(fpModel, thoughtLevel, initSys, toolsCatalog, messages, turnDir.FingerprintToken()); fp != "" {
			ctx = withRequestFingerprint(ctx, fp)
		}
	}
	if codec != nil {
		if len(initTools) > 0 {
			if initToolsFp == "" {
				initToolsFp = toolCatalogFingerprint(initTools)
				s.mu.Lock()
				if toolSliceDataPtr(s.InitialToolsCatalog) == initPtr && len(s.InitialToolsCatalog) == initLen {
					s.cachedInitToolsPtr = initPtr
					s.cachedInitToolsLen = initLen
					s.cachedInitToolsFp = initToolsFp
				}
				s.mu.Unlock()
			}
			codec.SeedFromToolsCatalogOnce(initToolsFp, initTools)
		}
		if len(toolsCatalog) > 0 {
			curToolFp := toolCatalogFpFromContext(ctx)
			if curToolFp == "" {
				curToolFp = toolCatalogFingerprint(toolsCatalog)
			}
			codec.SeedFromToolsCatalogOnce(curToolFp, toolsCatalog)
		}
		if initMsgCnt > 0 && len(messages) > 0 {
			bound := initMsgCnt
			if bound > len(messages) {
				bound = len(messages)
			}
			codec.SeedFromConversationOnce(initSys, messages[:bound])
		} else if initMsgCnt == 0 {
			codec.MarkHistorySeeded()
		}
	}

	defer func() {
		s.mu.Lock()
		s.leaveStreamingTurnLocked()
		s.LastUsedAt = time.Now()
		s.mu.Unlock()
	}()

	currentFirstFp := firstUserFingerprintFromContext(ctx)
	if currentFirstFp == "" {
		currentFirstFp = computeFirstUserFingerprint(messages)
	}
	lazyHistFp := sync.OnceValue(func() string {
		return computeMessagesSliceFingerprint(messages)
	})
	s.mu.Lock()
	isCompactionReq := s.explicitCompactionReq || isCompactionSummaryRequestWithSystem(s.SystemText, messages)
	s.explicitCompactionReq = false
	s.mu.Unlock()
	isStandaloneComp := isStandaloneCompactionFromContext(ctx)
	rolledOverFromParked := false
	rolloverReason := ""
	if hasParked {
		if len(messages) == 0 {
			s.HibernateWorker("aborted_parked_rollover", true)
			s.mu.Lock()
			s.ForceRolloverNext = false
			s.ForceRolloverReason = ""
			s.CompactionRollovers++
			s.clearCapacityFallbackLocked(true)
			s.highTierZeroCacheStreak = 0
			s.mu.Unlock()
			hasParked = false
			rolledOverFromParked = true
			rolloverReason = "rollover_aborted_tool"
		} else {
			trs := extractToolResultsFromMessages(messages)
			var matched []ToolResultInput
			lastTrMatched := false
			s.mu.Lock()
			forced := s.ForceRolloverNext
			forcedWhy := s.ForceRolloverReason
			msgCnt := s.MessageCount
			lastAsst := s.lastTurnAssistant
			cancEdited := cancelledHistoryDiverged(messages, msgCnt, lastAsst, s.cancelledHistoryFp)
			fpChanged := s.HistoryFingerprint != "" && currentFirstFp != "" && currentFirstFp != s.HistoryFingerprint
			if fpChanged && msgCnt == 2 && lastAsst && len(messages) >= 2 && messages[1]["role"] == "user" {
				if computeFirstUserFingerprint(messages[:1]) == s.HistoryFingerprint {
					fpChanged = false
				}
			}
			modelChanged := sel != nil && sel.Model != nil && sel.Model.ID != "" && s.ModelID != "" && sel.Model.ID != s.ModelID
			oldMsgCnt := msgCnt
			oldSid := s.SessionID
			oldModel := s.ModelID
			promptInFlight := s.PromptInFlight
			parkedCnt := len(s.ParkedCalls)
			hasTurns := len(s.TurnHistory) > 0 && s.TurnHistory[len(s.TurnHistory)-1].SessionID == oldSid
			hasBridge := len(s.DeclaredMcpTools) > 0
			liveCat := s.ToolsCatalog
			liveFp := s.ToolFingerprint
			for i, tr := range trs {
				if _, ok := s.ParkedCalls[tr.ToolCallID]; ok {
					matched = append(matched, tr)
					if i == len(trs)-1 {
						lastTrMatched = true
					}
				}
			}
			s.mu.Unlock()

			rolloverParked := func(why, hibWhy, logMsg string) {
				rolloverReason = why
				if hibWhy == "model_switch_parked" {
					LogEvent("COMPACTION_ROLLOVER", logMsg, map[string]any{
						"convKey":      s.ConvKey,
						"oldSessionId": oldSid,
						"oldModel":     oldModel,
						"newModel":     sel.Model.ID,
						"newMsgCount":  len(messages),
						"reason":       rolloverReason,
					})
				} else {
					LogEvent("COMPACTION_ROLLOVER", logMsg, map[string]any{
						"convKey":      s.ConvKey,
						"oldSessionId": oldSid,
						"newMsgCount":  len(messages),
						"reason":       rolloverReason,
					})
				}
				s.HibernateWorker(hibWhy, true)
				s.mu.Lock()
				s.ForceRolloverNext = false
				s.ForceRolloverReason = ""
				s.CompactionRollovers++
				s.clearCapacityFallbackLocked(true)
				s.highTierZeroCacheStreak = 0
				s.mu.Unlock()
				hasParked = false
				rolledOverFromParked = true
			}

			if len(matched) > 0 {
				shrank := msgCnt > 0 && (len(messages) < msgCnt || (lastAsst && messages[msgCnt-1]["role"] != "assistant"))
				compactedWhileParked := forced || shrank || cancEdited || fpChanged
				if compactedWhileParked || modelChanged {
					var why, hibWhy, logMsg string
					switch {
					case forced && forcedWhy != "":
						why = forcedWhy
					case cancEdited:
						why = "rollover_cancelled_edit"
					case shrank:
						why = "rollover_compaction"
					case fpChanged:
						why = "rollover_prefix"
					case modelChanged:
						why = "rollover_model_switch"
					default:
						why = "rollover"
					}
					if compactedWhileParked {
						hibWhy = "compaction_rollover_parked"
						logMsg = fmt.Sprintf("Conversation compacted while tool was parked (%d -> %d msgs); closing old session and rolling over", oldMsgCnt, len(messages))
					} else {
						hibWhy = "model_switch_parked"
						logMsg = fmt.Sprintf("Canonical model switched while tool was parked (%s -> %s); closing old session and rolling over", oldModel, sel.Model.ID)
					}
					rolloverParked(why, hibWhy, logMsg)
				} else {
					if !lastTrMatched && len(trs) > 0 {
						tail := trs[len(trs)-1]
						if tail.TrailingUserText != "" || len(tail.TrailingUserImages) > 0 {
							attachTrailingUserInputsToToolResult(&matched[len(matched)-1], tail.TrailingUserText, tail.TrailingUserImages)
						}
					}
					var mmErr error
					matched, mmErr = resolveToolResultImages(ctx, GlobalImageFetcher, matched, ModelOrDefault(initModel))
					if mmErr != nil {
						return TurnResult{}, mmErr
					}
					if err := s.applyParkedResumeModelSelection(sel); err != nil {
						return TurnResult{}, err
					}
					var parkedToolNotice, parkedSysDelta string
					var oldSysForDelta, newSysForDelta string
					var pAdded, pRemoved, pChanged []string
					s.mu.Lock()
					s.SessionReuses++
					s.orphanCapStreak = 0
					if s.hasPendingSystem || s.pendingSystemText != "" {
						pend := s.pendingSystemText
						s.pendingSystemText = ""
						s.hasPendingSystem = false
						if pend != s.SystemText {
							oldSysForDelta = s.SystemText
							newSysForDelta = pend
							s.SystemText = pend
						}
						if s.pendingRawSystemText != "" || pend == "" {
							s.RawSystemText = s.pendingRawSystemText
							s.pendingRawSystemText = ""
						}
					}
					if len(s.DeclaredMcpTools) > 0 {
						parkedToolNotice, pAdded, pRemoved, pChanged = s.consumeToolCatalogLocked(toolsCatalog)
					}
					sid := s.SessionID
					s.mu.Unlock()
					if oldSysForDelta != newSysForDelta {
						parkedSysDelta = protocol.SystemPromptDelta(oldSysForDelta, newSysForDelta, ApplyDynamicWafRules)
					}

					appendSystemTail := func(extra string) {
						if extra == "" {
							return
						}
						last := len(matched) - 1
						if matched[last].SystemTail == "" {
							matched[last].SystemTail = extra
						} else {
							matched[last].SystemTail += "\n\n" + extra
						}
					}
					if parkedSysDelta != "" {
						appendSystemTail(parkedSysDelta)
						LogEvent("SYSTEM_PROMPT_CHANGED", "Client system prompt changed during parked MCP resume", map[string]any{
							"convKey": s.ConvKey, "deltaChars": len(parkedSysDelta),
						})
					}
					if parkedToolNotice != "" {
						appendSystemTail(parkedToolNotice)
						LogEvent("TOOLS_UPDATED_IN_PLACE", "Updated session tool catalog in-place during parked MCP resume (cache preserved)", map[string]any{
							"convKey": s.ConvKey, "sessionId": sid, "added": pAdded, "removed": pRemoved, "changed": pChanged,
						})
					}
					if parkedSysDelta != "" || parkedToolNotice != "" {
						s.persistSessionAsync()
					}
					if tailDirective != "" {
						appendSystemTail(tailDirective)
					}
					var ids []string
					for _, m := range matched {
						ids = append(ids, m.ToolCallID)
					}
					LogEvent("PARKED_MCP_RESUME", fmt.Sprintf("Resuming %d parked MCP tool call(s)", len(matched)), map[string]any{
						"convKey":     s.ConvKey,
						"sessionId":   sid,
						"toolCallIds": ids,
					})
					notifySessionWarn(ctx, s)
					resCtx := withTurnDetachMeta(ctx, &turnDetachMeta{
						sid:                    sid,
						mode:                   "parked_mcp_resume",
						turnStart:              turnStart,
						msgCount:               len(messages),
						priorMsgCount:          msgCnt,
						firstUserFp:            currentFirstFp,
						cancelledHistFpFn:      lazyHistFp,
						isCompactionReq:        isCompactionReq,
						isStandaloneCompaction: isStandaloneComp,
					})
					return s.resumeParkedTurnTimed(resCtx, matched, messages, onDelta, turnStart, &firstDeltaNs)
				}
			} else {
				tooShort := msgCnt > 0 && len(messages) < msgCnt
				unseenStart := 0
				badRoleOrEmpty := false
				if !tooShort && msgCnt > 0 {
					if lastAsst {
						role, _ := messages[msgCnt-1]["role"].(string)
						switch role {
						case "assistant":
							unseenStart = msgCnt
						case "user":
							unseenStart = msgCnt - 1
						default:
							badRoleOrEmpty = true
						}
					} else {
						unseenStart = msgCnt
					}
				} else if hasTurns {
					badRoleOrEmpty = true
				}
				if unseenStart >= len(messages) {
					badRoleOrEmpty = true
				}
				toolRollover := toolCatalogNeedsRollover(hasBridge, liveCat, liveFp, toolsCatalog)
				hasUnseenToolResult := false
				if !tooShort && !badRoleOrEmpty && unseenStart >= 0 && unseenStart < len(messages) {
					hasUnseenToolResult = len(extractToolResultsFromMessages(messages[unseenStart:])) > 0
				}
				canHotInterrupt := oldSid != "" && promptInFlight && parkedCnt > 0 &&
					!forced && !isCompactionReq && !modelChanged && !fpChanged && !cancEdited &&
					!tooShort && !badRoleOrEmpty && !toolRollover && !hasUnseenToolResult

				if canHotInterrupt {
					curModelInfo := ModelOrDefault(oldModel)
					rawPromptBlocks, mmErr := resolveAndBuildPromptBlocks(ctx, GlobalImageFetcher, messages[unseenStart:], false, messages, curModelInfo.TranscriptChars, curModelInfo)
					if mmErr != nil {
						return TurnResult{}, mmErr
					}
					if err := s.applyParkedResumeModelSelection(sel); err != nil {
						return TurnResult{}, err
					}
					var parkedToolNotice, parkedSysDelta string
					var oldSysForDelta, newSysForDelta string
					var pAdded, pRemoved, pChanged []string
					s.mu.Lock()
					s.SessionReuses++
					if s.hasPendingSystem || s.pendingSystemText != "" {
						pend := s.pendingSystemText
						s.pendingSystemText = ""
						s.hasPendingSystem = false
						if pend != s.SystemText {
							oldSysForDelta = s.SystemText
							newSysForDelta = pend
							s.SystemText = pend
						}
						if s.pendingRawSystemText != "" || pend == "" {
							s.RawSystemText = s.pendingRawSystemText
							s.pendingRawSystemText = ""
						}
					} else if pendRaw := s.pendingRawSystemText; pendRaw != "" {
						s.pendingRawSystemText = ""
						s.RawSystemText = pendRaw
					}
					if len(s.DeclaredMcpTools) > 0 {
						parkedToolNotice, pAdded, pRemoved, pChanged = s.consumeToolCatalogLocked(toolsCatalog)
					}
					sid := s.SessionID
					codecHot := s.Codec
					s.mu.Unlock()
					if oldSysForDelta != newSysForDelta {
						parkedSysDelta = protocol.SystemPromptDelta(oldSysForDelta, newSysForDelta, ApplyDynamicWafRules)
					}

					if parkedSysDelta != "" {
						LogEvent("SYSTEM_PROMPT_CHANGED", "Client system prompt changed during hot-parked interrupt resume", map[string]any{
							"convKey": s.ConvKey, "deltaChars": len(parkedSysDelta),
						})
					}
					if parkedToolNotice != "" {
						LogEvent("TOOLS_UPDATED_IN_PLACE", "Updated session tool catalog in-place during hot-parked interrupt resume (cache preserved)", map[string]any{
							"convKey": s.ConvKey, "sessionId": sid, "added": pAdded, "removed": pRemoved, "changed": pChanged,
						})
					}
					if parkedSysDelta != "" || parkedToolNotice != "" {
						s.persistSessionAsync()
					}

					const abortHeader = "Host user interrupted the pending tool execution before completion; do not retry or continue that tool call. Respond directly to the updated user instruction below."
					var textParts []string
					var mediaBlocks []map[string]any
					for _, blk := range rawPromptBlocks {
						if blk == nil {
							continue
						}
						if t, _ := blk["type"].(string); t == "text" {
							if txt, _ := blk["text"].(string); txt != "" {
								textParts = append(textParts, txt)
							}
						} else {
							mediaBlocks = append(mediaBlocks, blk)
						}
					}
					combinedParts := []string{abortHeader}
					var fallbackNotes []string
					if parkedSysDelta != "" {
						combinedParts = append(combinedParts, parkedSysDelta)
						fallbackNotes = append(fallbackNotes, parkedSysDelta)
					}
					if parkedToolNotice != "" {
						combinedParts = append(combinedParts, parkedToolNotice)
						fallbackNotes = append(fallbackNotes, parkedToolNotice)
					}
					combinedParts = append(combinedParts, textParts...)
					if tailDirective != "" {
						combinedParts = append(combinedParts, tailDirective)
					}
					combinedText := strings.Join(combinedParts, "\n\n")
					if codecHot != nil {
						combinedText = codecHot.EncodeOutboundText(combinedText)
						if len(mediaBlocks) > 0 {
							mediaBlocks = codecHot.EncodePromptBlocks(mediaBlocks)
						}
					}
					primaryContentBlocks := []map[string]any{{"type": "text", "text": combinedText}}
					primaryContentBlocks = append(primaryContentBlocks, mediaBlocks...)

					fallbackPromptBlocks := rawPromptBlocks
					if len(fallbackNotes) > 0 {
						fallbackPromptBlocks = append([]map[string]any{{"type": "text", "text": strings.Join(fallbackNotes, "\n\n")}}, fallbackPromptBlocks...)
					}
					if tailDirective != "" {
						fallbackPromptBlocks = appendTurnDirectiveToPromptBlocks(fallbackPromptBlocks, tailDirective)
					}
					if codecHot != nil {
						fallbackPromptBlocks = codecHot.EncodePromptBlocks(fallbackPromptBlocks)
					}

					LogEvent("PARKED_MCP_INTERRUPT_RESUME", "Resuming hot-parked session in-place after client tool abort (cache preserved)", map[string]any{
						"convKey":       s.ConvKey,
						"sessionId":     sid,
						"unseenStart":   unseenStart,
						"totalMessages": len(messages),
						"strippedAsst":  lastAsst && msgCnt > 0 && unseenStart == msgCnt-1,
					})
					notifySessionWarn(ctx, s)
					resCtx := withTurnDetachMeta(ctx, &turnDetachMeta{
						sid:                    sid,
						mode:                   "parked_mcp_resume",
						turnStart:              turnStart,
						msgCount:               len(messages),
						priorMsgCount:          msgCnt,
						firstUserFp:            currentFirstFp,
						cancelledHistFpFn:      lazyHistFp,
						isCompactionReq:        isCompactionReq,
						isStandaloneCompaction: isStandaloneComp,
					})
					return s.resumeParkedTurnInternal(resCtx, nil, messages, onDelta, turnStart, &firstDeltaNs, primaryContentBlocks, fallbackPromptBlocks)
				}

				var why, hibWhy, logMsg string
				switch {
				case forced && forcedWhy != "":
					why = forcedWhy
				case cancEdited:
					why = "rollover_cancelled_edit"
				case tooShort || badRoleOrEmpty:
					why = "rollover_compaction"
				case fpChanged:
					why = "rollover_prefix"
				case modelChanged:
					why = "rollover_model_switch"
				case toolRollover:
					why = "rollover_tools"
				case isCompactionReq:
					why = "rollover_compaction"
				default:
					why = "rollover_aborted_tool"
				}
				if modelChanged && !forced && !cancEdited && !tooShort && !badRoleOrEmpty && !fpChanged {
					hibWhy = "model_switch_parked"
					logMsg = fmt.Sprintf("Canonical model switched while tool was parked (%s -> %s); closing old session and rolling over", oldModel, sel.Model.ID)
				} else {
					hibWhy = "compaction_rollover_parked"
					logMsg = fmt.Sprintf("Parked session ineligible for hot interrupt resume (%d -> %d msgs, reason=%s); closing old session and rolling over", oldMsgCnt, len(messages), why)
				}
				rolloverParked(why, hibWhy, logMsg)
			}
		}
	}

	// Preflight Stage 2/3 remote image fetches and rollover budgets before consuming pending system/tool
	// diffs or opening/replacing ACP sessions, so 4xx validation/fetch errors leave session state intact.
	type promptPlan struct {
		useRollover   bool
		msgCnt        int
		curModel      string
		transcriptCap int
	}
	buildPrePlan := func() promptPlan {
		s.mu.Lock()
		forceNext := s.ForceRolloverNext
		msgCnt := s.MessageCount
		lastAsst := s.lastTurnAssistant
		histFp := s.HistoryFingerprint
		cancFp := s.cancelledHistoryFp
		sidNow := s.SessionID
		hibNow := s.HibernatedSessionID
		hibParked := len(s.HibernatedParkedCallIDs) > 0
		hasTurns := len(s.TurnHistory) > 0 && s.TurnHistory[len(s.TurnHistory)-1].SessionID == sidNow
		hasBridge := len(s.DeclaredMcpTools) > 0
		liveCat := s.ToolsCatalog
		liveFp := s.ToolFingerprint
		curModel := s.ModelID
		s.mu.Unlock()

		isRoll := forceNext || rolledOverFromParked
		if sidNow == "" && hibNow == "" {
			msgCnt = 0
		}
		strippedTurn1Hib := false
		if msgCnt > 0 {
			if len(messages) < msgCnt {
				isRoll = true
			} else if lastAsst && messages[msgCnt-1]["role"] != "assistant" {
				if !isRoll && hibParked && msgCnt >= 2 && messages[msgCnt-1]["role"] == "user" {
					strippedTurn1Hib = msgCnt == 2
					msgCnt--
				} else {
					isRoll = true
				}
			} else if cancelledHistoryDiverged(messages, msgCnt, lastAsst, cancFp) {
				isRoll = true
			}
			if !isRoll && histFp != "" && currentFirstFp != "" && currentFirstFp != histFp {
				if !(strippedTurn1Hib && len(messages) >= 2 && messages[1]["role"] == "user" && computeFirstUserFingerprint(messages[:1]) == histFp) {
					isRoll = true
				}
			}
		} else if sidNow != "" && hasTurns {
			isRoll = true
		}
		if !isRoll && toolCatalogNeedsRollover(hasBridge, liveCat, liveFp, toolsCatalog) {
			isRoll = true
		}
		effMsgCnt := msgCnt
		if isRoll {
			effMsgCnt = 0
		}
		effModel := curModel
		if sel != nil && sel.Model != nil && sel.Model.ID != "" {
			effModel = sel.Model.ID
		}
		mInfo := ModelOrDefault(effModel)
		isMid := effMsgCnt == 0 && len(messages) > 2
		return promptPlan{
			useRollover:   isRoll || rolledOverFromParked || isMid,
			msgCnt:        effMsgCnt,
			curModel:      mInfo.ID,
			transcriptCap: mInfo.TranscriptChars,
		}
	}

	prePlan := buildPrePlan()
	var preUnseen []map[string]any
	if prePlan.msgCnt < len(messages) {
		preUnseen = messages[prePlan.msgCnt:]
	}
	rawPromptBlocks, mmErr := resolveAndBuildPromptBlocks(ctx, GlobalImageFetcher, preUnseen, prePlan.useRollover, messages, prePlan.transcriptCap, ModelOrDefault(prePlan.curModel))
	if mmErr != nil {
		return TurnResult{}, mmErr
	}

	// System prompt changes are consumed into a tail delta so session/new's prefix stays frozen.
	s.mu.Lock()
	systemDelta := ""
	var oldSysForDelta, newSysForDelta string
	hasPersistedSession := s.SessionID != "" || s.HibernatedSessionID != ""
	if s.hasPendingSystem || s.pendingSystemText != "" {
		pend := s.pendingSystemText
		s.pendingSystemText = ""
		s.hasPendingSystem = false
		if pend != s.SystemText {
			oldSysForDelta = s.SystemText
			newSysForDelta = pend
			s.SystemText = pend
		}
		if s.pendingRawSystemText != "" || pend == "" {
			s.RawSystemText = s.pendingRawSystemText
			s.pendingRawSystemText = ""
		}
	} else if pendRaw := s.pendingRawSystemText; pendRaw != "" {
		s.pendingRawSystemText = ""
		s.RawSystemText = pendRaw
	}
	s.mu.Unlock()
	if oldSysForDelta != newSysForDelta {
		systemDelta = protocol.SystemPromptDelta(oldSysForDelta, newSysForDelta, ApplyDynamicWafRules)
	}
	if systemDelta != "" {
		if hasPersistedSession {
			s.persistSessionAsync()
		}
		LogEvent("SYSTEM_PROMPT_CHANGED", "Client system prompt changed within a conversation", map[string]any{
			"convKey": s.ConvKey, "deltaChars": len(systemDelta),
		})
	}

	origThoughtLevel := thoughtLevel
	trsCheck := extractToolResultsFromMessages(messages)
	// Decide rollover before booting/resuming or touching the model:
	// 1) A hibernated session that already needs rollover skips a wasted session/resume child spawn
	//    and goes straight to openFreshSession.
	// 2) A live session that is about to be replaced does not receive a model switch first.
	s.mu.Lock()
	hasExistingSession := s.SessionID != "" || s.HibernatedSessionID != ""
	prevForceRoll := s.ForceRolloverNext
	prevForceWhy := s.ForceRolloverReason
	isRollover := s.ForceRolloverNext
	if isRollover && rolloverReason == "" {
		if s.ForceRolloverReason != "" {
			rolloverReason = s.ForceRolloverReason
		} else {
			rolloverReason = "rollover"
		}
	}
	effCheckMsgCnt := s.MessageCount
	if !hasExistingSession {
		effCheckMsgCnt = 0
	}
	hibParked := len(s.HibernatedParkedCallIDs) > 0
	matchedHibParkedTool := false
	if hibParked && len(trsCheck) > 0 {
		for _, tr := range trsCheck {
			if s.HibernatedParkedCallIDs[tr.ToolCallID] {
				matchedHibParkedTool = true
				break
			}
		}
	}
	strippedHibernatedAsst := false
	if effCheckMsgCnt > 0 {
		if len(messages) < effCheckMsgCnt {
			isRollover = true
			if rolloverReason == "" {
				rolloverReason = "rollover_compaction"
			}
		} else if s.lastTurnAssistant && messages[effCheckMsgCnt-1]["role"] != "assistant" {
			if !isRollover && hibParked && effCheckMsgCnt >= 2 && messages[effCheckMsgCnt-1]["role"] == "user" {
				strippedHibernatedAsst = true
			} else {
				isRollover = true
				if rolloverReason == "" {
					rolloverReason = "rollover_compaction"
				}
			}
		} else if cancelledHistoryDiverged(messages, effCheckMsgCnt, s.lastTurnAssistant, s.cancelledHistoryFp) {
			isRollover = true
			if rolloverReason == "" {
				rolloverReason = "rollover_cancelled_edit"
			}
		}
		if !isRollover && s.HistoryFingerprint != "" && currentFirstFp != "" && currentFirstFp != s.HistoryFingerprint {
			if !(strippedHibernatedAsst && effCheckMsgCnt == 2 && len(messages) >= 2 && messages[1]["role"] == "user" && computeFirstUserFingerprint(messages[:1]) == s.HistoryFingerprint) {
				isRollover = true
				strippedHibernatedAsst = false
				if rolloverReason == "" {
					rolloverReason = "rollover_prefix"
				}
			}
		}
	} else if s.SessionID != "" && len(s.TurnHistory) > 0 && s.TurnHistory[len(s.TurnHistory)-1].SessionID == s.SessionID {
		// Safety guard: if MessageCount was reset to 0 on an already-used session, never append full history onto it
		isRollover = true
		if rolloverReason == "" {
			rolloverReason = "rollover"
		}
	}
	oldSid := s.SessionID
	if oldSid == "" {
		oldSid = s.HibernatedSessionID
	}
	hasBridge := len(s.DeclaredMcpTools) > 0
	liveCat := s.ToolsCatalog
	liveFp := s.ToolFingerprint
	noSession := s.SessionID == ""
	hibBeforeBoot := s.HibernatedSessionID
	prevModelBeforeBoot := s.ModelID
	prevTierBeforeBoot := s.ThoughtLevel
	s.mu.Unlock()

	if !isRollover && hasExistingSession && toolCatalogNeedsRollover(hasBridge, liveCat, liveFp, toolsCatalog) {
		added, _, changed := diffToolCatalogs(liveCat, toolsCatalog)
		LogEvent("TOOLS_CHANGED_ROLLOVER", "Session had no MCP bridge (or unknown catalog after restore); opening fresh session with tools", map[string]any{
			"convKey": s.ConvKey, "sessionId": oldSid, "added": added, "changed": changed,
		})
		isRollover = true
		strippedHibernatedAsst = false
		rolloverReason = "rollover_tools"
	}

	var stickyFallback, startedProbe bool
	var clampedTier, probeFromTier, prevFbReason string
	if !isRollover && sel != nil && sel.Model != nil {
		s.mu.Lock()
		evalSid := s.SessionID
		if evalSid == "" {
			evalSid = s.HibernatedSessionID
		}
		prevModelForEval := s.ModelID
		prevFbReason = s.capacityFallbackReason
		clampedTier, stickyFallback, startedProbe, probeFromTier = s.evaluateCapacityFallbackWithIdleLocked(*sel, evalSid, prevModelForEval, capacityFallbackNow(), prevLastUsedAt)
		if sel.ExplicitTier || (prevModelForEval != "" && sel.Model.ID != prevModelForEval) {
			s.clearCapacityFallbackLocked(true)
		}
		s.mu.Unlock()
	}

	var effSel *ModelSelection
	if sel != nil {
		tmp := *sel
		if !isRollover && (stickyFallback || startedProbe) && clampedTier != "" {
			tmp.Tier = clampedTier
			thoughtLevel = clampedTier
		}
		effSel = &tmp
		if isRollover {
			s.setDesiredSelection(*sel)
		} else if noSession {
			s.setDesiredSelection(tmp)
		}
	}
	if startedProbe && !isRollover {
		LogEvent("CAPACITY_FALLBACK_PROBE", "Cooldown expired at safe boundary; probing higher thinking tier", map[string]any{
			"convKey":   s.ConvKey,
			"sessionId": oldSid,
			"model":     sel.Model.ID,
			"fromTier":  probeFromTier,
			"toTier":    clampedTier,
		})
	}

	var toolNotice string
	if isRollover {
		s.mu.Lock()
		s.ForceRolloverNext = false
		s.ForceRolloverReason = ""
		s.CompactionRollovers++
		s.clearCapacityFallbackLocked(true)
		s.highTierZeroCacheStreak = 0
		s.mu.Unlock()
		LogEvent("COMPACTION_ROLLOVER", "Prefix changed or compacted, rolling over session", map[string]any{
			"convKey":      s.ConvKey,
			"oldSessionId": oldSid,
			"reason":       rolloverReason,
		})
		if err := s.openFreshSession(toolsCatalog, origThoughtLevel); err != nil {
			s.mu.Lock()
			if s.SessionID == "" && s.HibernatedSessionID == hibBeforeBoot && prevForceRoll {
				s.ForceRolloverNext = prevForceRoll
				s.ForceRolloverReason = prevForceWhy
			}
			s.mu.Unlock()
			return TurnResult{}, err
		}
	} else {
		if err := s.ensureBooted(toolsCatalog, thoughtLevel); err != nil {
			var msErr *ModelSwitchError
			if startedProbe && sel != nil && sel.Model != nil && !sel.ExplicitTier && noSession && hibBeforeBoot != "" && prevModelBeforeBoot != "" && errors.As(err, &msErr) {
				rearmTier := probeFromTier
				if rearmTier == "" {
					rearmTier = prevTierBeforeBoot
				}
				rearmReason := prevFbReason
				if rearmReason == "" {
					rearmReason = "capacity_429"
				}
				var strikes int
				var cd time.Duration
				s.mu.Lock()
				if s.SessionID == "" && s.HibernatedSessionID == hibBeforeBoot {
					s.ModelID = prevModelBeforeBoot
					s.ThoughtLevel = prevTierBeforeBoot
					strikes, cd = s.armCapacityFallbackLocked(hibBeforeBoot, rearmTier, rearmReason, capacityFallbackNow())
				}
				s.mu.Unlock()
				LogEvent("CAPACITY_TIER_SWITCH_SKIPPED", "Failed recovery probe tier during hibernated wake; retrying fallback tier", map[string]any{
					"convKey":     s.ConvKey,
					"sessionId":   hibBeforeBoot,
					"model":       prevModelBeforeBoot,
					"probeTier":   clampedTier,
					"retryTier":   rearmTier,
					"probeFailed": true,
					"strikes":     strikes,
					"cooldownSec": int(cd.Seconds()),
					"error":       err.Error(),
				})
				thoughtLevel = rearmTier
				if effSel != nil {
					tmpFb := *sel
					tmpFb.Tier = rearmTier
					effSel = &tmpFb
					s.setDesiredSelection(tmpFb)
				}
				if retryErr := s.ensureBooted(toolsCatalog, rearmTier); retryErr == nil {
					err = nil
				} else {
					err = retryErr
				}
			} else if stickyFallback && sel != nil && sel.Model != nil && !sel.ExplicitTier && noSession && hibBeforeBoot != "" && prevModelBeforeBoot != "" && errors.As(err, &msErr) {
				s.mu.Lock()
				if s.SessionID == "" && s.HibernatedSessionID == hibBeforeBoot {
					s.ModelID = prevModelBeforeBoot
					s.ThoughtLevel = prevTierBeforeBoot
					s.clearCapacityFallbackLocked(true)
				}
				s.mu.Unlock()
				LogEvent("CAPACITY_TIER_SWITCH_SKIPPED", "Failed sticky fallback tier during hibernated wake; retrying requested tier", map[string]any{
					"convKey":      s.ConvKey,
					"sessionId":    hibBeforeBoot,
					"model":        prevModelBeforeBoot,
					"fallbackTier": clampedTier,
					"retryTier":    origThoughtLevel,
					"error":        err.Error(),
				})
				thoughtLevel = origThoughtLevel
				if effSel != nil {
					tmpOrig := *sel
					effSel = &tmpOrig
					s.setDesiredSelection(tmpOrig)
				}
				if retryErr := s.ensureBooted(toolsCatalog, origThoughtLevel); retryErr == nil {
					err = nil
				} else {
					err = retryErr
				}
			}
			if err != nil {
				if noSession && hibBeforeBoot != "" && prevModelBeforeBoot != "" {
					s.mu.Lock()
					if s.SessionID == "" && s.HibernatedSessionID == hibBeforeBoot {
						s.ModelID = prevModelBeforeBoot
						s.ThoughtLevel = prevTierBeforeBoot
					}
					s.mu.Unlock()
				}
				return TurnResult{}, err
			}
		}
		if strippedHibernatedAsst {
			s.mu.Lock()
			if s.MessageCount > 1 {
				s.MessageCount--
			}
			s.lastTurnAssistant = false
			if s.MessageCount <= len(messages) {
				s.cancelledHistoryFp = computeMessagesSliceFingerprint(messages[:s.MessageCount])
			}
			s.mu.Unlock()
		} else if matchedHibParkedTool {
			s.mu.Lock()
			s.orphanCapStreak = 0
			s.mu.Unlock()
		}
		s.mu.Lock()
		curSidAfterBoot := s.SessionID
		var added, removed, changed []string
		toolNotice, added, removed, changed = s.consumeToolCatalogLocked(toolsCatalog)
		s.mu.Unlock()
		if toolNotice != "" {
			s.persistSessionAsync()
			LogEvent("TOOLS_UPDATED_IN_PLACE", "Updated session tool catalog in-place via dsh_call_tool + prompt delta (cache preserved)", map[string]any{
				"convKey": s.ConvKey, "sessionId": curSidAfterBoot, "added": added, "removed": removed, "changed": changed,
			})
		}
		if effSel != nil {
			s.mu.Lock()
			preSelSid, preSelModel, preSelTier := s.SessionID, s.ModelID, s.ThoughtLevel
			s.mu.Unlock()
			if err := s.applyModelSelection(*effSel); err != nil {
				sameModelSwitch := effSel.Model != nil && effSel.Model.ID == preSelModel && preSelModel != ""
				if sameModelSwitch {
					s.mu.Lock()
					if s.SessionID == preSelSid && s.ModelID == "" {
						s.ModelID = preSelModel
						s.ThoughtLevel = preSelTier
					}
					s.mu.Unlock()
				}
				if startedProbe && sameModelSwitch && sel != nil && !sel.ExplicitTier {
					rearmTier := probeFromTier
					if rearmTier == "" {
						rearmTier = preSelTier
					}
					rearmReason := prevFbReason
					if rearmReason == "" {
						rearmReason = "capacity_429"
					}
					var strikes int
					var cd time.Duration
					s.mu.Lock()
					if s.SessionID == preSelSid {
						strikes, cd = s.armCapacityFallbackLocked(preSelSid, rearmTier, rearmReason, capacityFallbackNow())
					}
					s.mu.Unlock()
					thoughtLevel = preSelTier
					LogEvent("CAPACITY_TIER_SWITCH_SKIPPED", "Failed recovery probe tier switch; re-arming backoff on fallback tier", map[string]any{
						"convKey":     s.ConvKey,
						"sessionId":   preSelSid,
						"model":       preSelModel,
						"currentTier": preSelTier,
						"targetTier":  effSel.Tier,
						"rearmedTier": rearmTier,
						"probeFailed": true,
						"strikes":     strikes,
						"cooldownSec": int(cd.Seconds()),
						"error":       err.Error(),
					})
				} else if stickyFallback && sameModelSwitch && sel != nil && !sel.ExplicitTier {
					s.mu.Lock()
					if s.SessionID == preSelSid {
						s.clearCapacityFallbackLocked(true)
					}
					s.mu.Unlock()
					thoughtLevel = preSelTier
					LogEvent("CAPACITY_TIER_SWITCH_SKIPPED", "Failed sticky fallback tier switch; continuing on current tier", map[string]any{
						"convKey":     s.ConvKey,
						"sessionId":   preSelSid,
						"model":       preSelModel,
						"currentTier": preSelTier,
						"targetTier":  effSel.Tier,
						"error":       err.Error(),
					})
				} else {
					return TurnResult{}, err
				}
			}
		}
		s.mu.Lock()
		if s.MessageCount > 0 {
			s.SessionReuses++
		}
		s.mu.Unlock()
	}

	s.mu.Lock()
	s.lastTurnExplicitTier = sel != nil && sel.ExplicitTier
	msgCnt := s.MessageCount
	reuses := s.SessionReuses
	sid := s.SessionID
	wakeMethod := s.LastWakeMethod
	s.LastWakeMethod = ""
	s.mu.Unlock()
	isMidColdStart := msgCnt == 0 && len(messages) > 2
	var unseen []map[string]any
	if msgCnt < len(messages) {
		unseen = messages[msgCnt:]
	}
	s.mu.Lock()
	transcriptCap := ModelOrDefault(s.ModelID).TranscriptChars
	curModel, curTier := s.ModelID, s.ThoughtLevel
	curSys := s.SystemText
	s.mu.Unlock()
	curModelInfo := ModelOrDefault(curModel)
	postUseRollover := isRollover || rolledOverFromParked || isMidColdStart
	if msgCnt != prePlan.msgCnt || postUseRollover != prePlan.useRollover || curModelInfo.ID != prePlan.curModel {
		rawPromptBlocks, mmErr = resolveAndBuildPromptBlocks(ctx, GlobalImageFetcher, unseen, postUseRollover, messages, curModelInfo.TranscriptChars, curModelInfo)
		if mmErr != nil {
			return TurnResult{}, mmErr
		}
	}
	if msgCnt > 0 && !isRollover && !rolledOverFromParked {
		// The live/resumed session still carries the old system prompt and tool list; tell the model.
		var notes []string
		if systemDelta != "" {
			notes = append(notes, systemDelta)
		}
		if toolNotice != "" {
			notes = append(notes, toolNotice)
		}
		if len(notes) > 0 {
			rawPromptBlocks = append([]map[string]any{{"type": "text", "text": strings.Join(notes, "\n\n")}}, rawPromptBlocks...)
		}
	}
	if tailDirective != "" {
		rawPromptBlocks = appendTurnDirectiveToPromptBlocks(rawPromptBlocks, tailDirective)
	}
	promptBlocks := rawPromptBlocks
	if s.Codec != nil {
		promptBlocks = s.Codec.EncodePromptBlocks(rawPromptBlocks)
	}
	mode := "cold_start"
	if isRollover || rolledOverFromParked {
		mode = "compaction_rollover"
	} else if wakeMethod != "" {
		mode = "resumed_delta"
	} else if reuses > 0 {
		mode = "sticky_delta"
	}

	canBisectColdStart := !firstPromptPlacement() && WafAutoBisectEnabled && mode == "cold_start" && !s.isWafBisectProbe &&
		!isWafBisectInCooldown() && len(extractBisectCandidates(curSys, rawPromptBlocks)) >= 2
	s.mu.Lock()
	s.allowWafFastCancel = canBisectColdStart
	s.mu.Unlock()

	LogEvent("TURN_START", fmt.Sprintf("Sending session/prompt (%s)", mode), map[string]any{
		"convKey":          s.ConvKey,
		"sessionId":        sid,
		"model":            curModel,
		"thoughtLevel":     curTier,
		"totalMessages":    len(messages),
		"unseenDeltaCount": len(unseen),
	})

	notifySessionWarn(ctx, s)
	execCtx := withTurnDetachMeta(ctx, &turnDetachMeta{
		sid:                    sid,
		mode:                   mode,
		rolloverReason:         rolloverReason,
		turnStart:              turnStart,
		msgCount:               len(messages),
		priorMsgCount:          msgCnt,
		firstUserFp:            currentFirstFp,
		cancelledHistFpFn:      lazyHistFp,
		isCompactionReq:        isCompactionReq,
		isStandaloneCompaction: isStandaloneComp,
	})
	res, err := s.executePromptOrParkWithRetry(execCtx, promptBlocks, onDelta, 0)
	s.mu.Lock()
	s.allowWafFastCancel = false
	s.mu.Unlock()

	var fastCancelErr *wafFastCancelledError
	isFastCancelled := errors.As(err, &fastCancelErr)
	isInstantNotice429 := err == nil && canBisectColdStart && res.EmittedDeltas == 0 &&
		len(res.ToolCalls) == 0 && time.Since(turnStart) < 8*time.Second &&
		isBisectableSoftError(DetectAntigravitySoftError(res.Text))

	if !firstPromptPlacement() && (isFastCancelled || isInstantNotice429) && ctx.Err() == nil {
		s.HibernateWorker("waf_autobisect_reset", true)
		if healed, _ := runWafAutoBisectAndPersist(ctx, s.ConvKey, curSys, rawPromptBlocks); healed {
			s.mu.Lock()
			if s.RawSystemText != "" {
				s.SystemText = PruneClientSystemPrompt(s.RawSystemText)
			} else {
				s.SystemText = ApplyDynamicWafRules(s.SystemText)
			}
			s.mu.Unlock()
			if openErr := s.openFreshSession(toolsCatalog, thoughtLevel); openErr == nil {
				s.mu.Lock()
				newSid := s.SessionID
				s.mu.Unlock()
				rebuilt, rbErr := resolveAndBuildPromptBlocks(ctx, GlobalImageFetcher, unseen, isMidColdStart, messages, transcriptCap, curModelInfo)
				if rbErr != nil {
					return TurnResult{}, rbErr
				}
				if tailDirective != "" {
					rebuilt = appendTurnDirectiveToPromptBlocks(rebuilt, tailDirective)
				}
				if s.Codec != nil {
					rebuilt = s.Codec.EncodePromptBlocks(rebuilt)
				}
				retryCtx := withTurnDetachMeta(ctx, &turnDetachMeta{
					sid:                    newSid,
					mode:                   "waf_autobisect_recovery",
					rolloverReason:         "rollover_waf_healed",
					turnStart:              turnStart,
					msgCount:               len(messages),
					firstUserFp:            currentFirstFp,
					cancelledHistFpFn:      lazyHistFp,
					isCompactionReq:        isCompactionReq,
					isStandaloneCompaction: isStandaloneComp,
				})
				if retryRes, retryErr := s.executePromptOrParkWithRetry(retryCtx, rebuilt, onDelta, 0); retryErr == nil {
					res = retryRes
					err = nil
					isFastCancelled = false
					sid = newSid
					mode = "waf_autobisect_recovery"
					rolloverReason = "rollover_waf_healed"
				} else {
					err = retryErr
					isFastCancelled = false
				}
			}
		}
		if isFastCancelled {
			// Auto-bisect confirmed cleanroom Probe 2 was 200 OK (not a WAF rule) or upstream is throttled.
			// Synthesize the notice-only 429 text so high-tier capacity fallback (if eligible) or standard
			// 429 SoftError handling runs cleanly without waiting 140s.
			det := strings.TrimSpace(fastCancelErr.Detail)
			if det == "" {
				det = "RESOURCE_EXHAUSTED (code 429): Resource has been exhausted (e.g. check quota)."
			}
			res = TurnResult{
				StopReason: "end_turn",
				Text:       "⚠ Agent execution terminated due to error: " + det,
			}
			err = nil
		}
	}

	if err != nil {
		if isSoftDetachedErr(err) {
			return TurnResult{}, err
		}
		if IsTosViolationError(err.Error()) {
			TripTosCircuitBreaker(err.Error())
		}
		var retryAbort *rpcRetryAbortedError
		if errors.As(err, &retryAbort) {
			return TurnResult{}, err
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			if isStandaloneComp || (isCompactionReq && msgCnt == 0) {
				s.HibernateWorker("oneshot_compaction_cancelled", true)
				return TurnResult{}, err
			}
			s.mu.Lock()
			s.MessageCount = len(messages)
			s.lastTurnAssistant = false
			s.cancelledHistoryFp = lazyHistFp()
			s.mu.Unlock()
			return TurnResult{}, err
		}
		s.preserveOrHibernateOnRpcError(err)
		return TurnResult{}, err
	}

	if !firstPromptPlacement() && res.Usage != nil && res.Usage.Degradation != nil && res.Usage.Degradation.Degraded {
		deg := res.Usage.Degradation
		s.recordArgonDegradation(sid, deg, res.EmittedDeltas)
		if res.EmittedDeltas == 0 {
			if openErr := s.openFreshSession(toolsCatalog, thoughtLevel); openErr == nil {
				s.mu.Lock()
				newSid := s.SessionID
				s.mu.Unlock()
				retryBlocks, rbErr := resolveAndBuildPromptBlocks(ctx, GlobalImageFetcher, messages, true, messages, transcriptCap, curModelInfo)
				if rbErr != nil {
					return TurnResult{}, rbErr
				}
				if tailDirective != "" {
					retryBlocks = appendTurnDirectiveToPromptBlocks(retryBlocks, tailDirective)
				}
				if s.Codec != nil {
					retryBlocks = s.Codec.EncodePromptBlocks(retryBlocks)
				}
				retryCtx := withTurnDetachMeta(ctx, &turnDetachMeta{
					sid:                    newSid,
					mode:                   "argon_degraded_recovery",
					rolloverReason:         "rollover_degraded",
					turnStart:              turnStart,
					msgCount:               len(messages),
					firstUserFp:            currentFirstFp,
					cancelledHistFpFn:      lazyHistFp,
					isCompactionReq:        isCompactionReq,
					isStandaloneCompaction: isStandaloneComp,
				})
				if retryRes, retryErr := s.executePromptOrParkWithRetry(retryCtx, retryBlocks, onDelta, 0); retryErr == nil {
					argonDegradationRecoveredTotal.Add(1)
					LogEvent("ARGON_DEGRADATION_RECOVERED", "Recovered from Gemini 4 Argon shadow pool routing via transparent fresh-session replay", map[string]any{
						"convKey":      s.ConvKey,
						"oldSessionId": sid,
						"newSessionId": newSid,
						"servedModel":  deg.ServedModel,
					})
					res = retryRes
					sid = newSid
					mode = "argon_degraded_recovery"
					rolloverReason = "rollover_degraded"
				} else if isSoftDetachedErr(retryErr) {
					return TurnResult{}, retryErr
				} else {
					var retryAbort *rpcRetryAbortedError
					if errors.As(retryErr, &retryAbort) {
						return TurnResult{}, retryErr
					}
					if errors.Is(retryErr, context.Canceled) || errors.Is(retryErr, context.DeadlineExceeded) {
						s.mu.Lock()
						s.MessageCount = len(messages)
						s.lastTurnAssistant = false
						s.cancelledHistoryFp = lazyHistFp()
						s.mu.Unlock()
						return TurnResult{}, retryErr
					}
					if IsTosViolationError(retryErr.Error()) {
						TripTosCircuitBreaker(retryErr.Error())
					}
					s.preserveOrHibernateOnRpcError(retryErr)
					return TurnResult{}, retryErr
				}
			} else {
				s.HibernateWorker("argon_degraded_retry", true)
				return TurnResult{}, openErr
			}
		} else {
			// Text already reached the client: roll over on the next turn. RunTurn's rollover path
			// opens the fresh session synchronously, so no delayed close is needed (a delayed close
			// raced with, and killed, the next turn's new session).
			s.mu.Lock()
			s.ForceRolloverNext = true
			s.ForceRolloverReason = "rollover_degraded"
			s.mu.Unlock()
		}
	}
	if se := DetectAntigravitySoftError(res.Text); !firstPromptPlacement() && isUpstreamPoolCapacitySoftError(se) &&
		res.FailedNotice == "" && res.EmittedDeltas == 0 && len(res.ToolCalls) == 0 &&
		sel != nil && sel.Model != nil && sel.Model.Family == "gemini" && !sel.ExplicitTier &&
		nextFallbackTier(curModel, curTier) != "" &&
		ctx.Err() == nil && time.Since(turnStart) < 90*time.Second {
		if msgCnt > 0 && sid != "" {
			for targetTier := nextFallbackTier(curModel, curTier); targetTier != "" && ctx.Err() == nil && time.Since(turnStart) < 90*time.Second; targetTier = nextFallbackTier(curModel, curTier) {
				s.mu.Lock()
				prevLoopModel, prevLoopTier := s.ModelID, s.ThoughtLevel
				s.mu.Unlock()
				tierErr := s.applyModelSelection(ModelSelection{Model: sel.Model, Tier: targetTier})
				if tierErr != nil {
					s.mu.Lock()
					if s.SessionID == sid && s.ModelID == "" && prevLoopModel != "" {
						s.ModelID = prevLoopModel
						s.ThoughtLevel = prevLoopTier
					}
					s.mu.Unlock()
					LogEvent("WARM_CAPACITY_TIER_SWITCH_SKIPPED", "Failed in-place lower-tier switch during warm capacity fallback; keeping current tier", map[string]any{
						"convKey":     s.ConvKey,
						"sessionId":   sid,
						"model":       prevLoopModel,
						"currentTier": prevLoopTier,
						"targetTier":  targetTier,
						"error":       tierErr.Error(),
					})
					break
				}
				inPlaceCtx := withTurnDetachMeta(ctx, &turnDetachMeta{
					sid:                    sid,
					mode:                   mode,
					rolloverReason:         rolloverReason,
					turnStart:              turnStart,
					msgCount:               len(messages),
					priorMsgCount:          msgCnt,
					firstUserFp:            currentFirstFp,
					cancelledHistFpFn:      lazyHistFp,
					isCompactionReq:        isCompactionReq,
					isStandaloneCompaction: isStandaloneComp,
				})
				retryRes, retryErr := s.executePromptOrParkWithRetry(inPlaceCtx, promptBlocks, onDelta, 0)
				if retryErr == nil && DetectAntigravitySoftError(retryRes.Text) == nil && retryRes.FailedNotice == "" {
					s.mu.Lock()
					strikes, cd := s.armCapacityFallbackLocked(sid, targetTier, "capacity_429", capacityFallbackNow())
					s.mu.Unlock()
					LogEvent("HIGH_TIER_CAPACITY_FALLBACK", "Recovered from high-tier TPU capacity 429/529 via in-place lower-tier switch (cache preserved)", map[string]any{
						"convKey":      s.ConvKey,
						"oldSessionId": sid,
						"newSessionId": sid,
						"model":        curModel,
						"fromTier":     curTier,
						"toTier":       targetTier,
						"inPlace":      true,
						"strikes":      strikes,
						"cooldownSec":  int(cd.Seconds()),
					})
					res = retryRes
					curTier = targetTier
					break
				}
				if isSoftDetachedErr(retryErr) {
					return TurnResult{}, retryErr
				}
				var retryAbort *rpcRetryAbortedError
				if errors.As(retryErr, &retryAbort) {
					return TurnResult{}, retryErr
				}
				if errors.Is(retryErr, context.Canceled) || errors.Is(retryErr, context.DeadlineExceeded) {
					s.mu.Lock()
					s.MessageCount = len(messages)
					s.lastTurnAssistant = false
					s.cancelledHistoryFp = lazyHistFp()
					s.mu.Unlock()
					return TurnResult{}, retryErr
				}
				if retryErr != nil {
					if IsTosViolationError(retryErr.Error()) {
						TripTosCircuitBreaker(retryErr.Error())
					}
					s.preserveOrHibernateOnRpcError(retryErr)
					return TurnResult{}, retryErr
				}
				res = retryRes
				curTier = targetTier
				if retrySe := DetectAntigravitySoftError(retryRes.Text); !isUpstreamPoolCapacitySoftError(retrySe) ||
					retryRes.FailedNotice != "" || retryRes.EmittedDeltas > 0 || len(retryRes.ToolCalls) > 0 {
					break
				}
			}
		} else {
			for targetTier := nextFallbackTier(curModel, curTier); targetTier != "" && ctx.Err() == nil && time.Since(turnStart) < 90*time.Second; targetTier = nextFallbackTier(curModel, curTier) {
				prevSid := sid
				openErr := s.openFreshSession(toolsCatalog, targetTier)
				if openErr != nil {
					s.HibernateWorker("high_tier_capacity_fallback", true)
					break
				}
				s.mu.Lock()
				newSid := s.SessionID
				s.mu.Unlock()
				retryBlocks, rbErr := resolveAndBuildPromptBlocks(ctx, GlobalImageFetcher, messages, true, messages, transcriptCap, curModelInfo)
				if rbErr != nil {
					return TurnResult{}, rbErr
				}
				if tailDirective != "" {
					retryBlocks = appendTurnDirectiveToPromptBlocks(retryBlocks, tailDirective)
				}
				if s.Codec != nil {
					retryBlocks = s.Codec.EncodePromptBlocks(retryBlocks)
				}
				retryCtx := withTurnDetachMeta(ctx, &turnDetachMeta{
					sid:                    newSid,
					mode:                   "high_capacity_fallback",
					rolloverReason:         "rollover_high_tier_429",
					turnStart:              turnStart,
					msgCount:               len(messages),
					firstUserFp:            currentFirstFp,
					cancelledHistFpFn:      lazyHistFp,
					isCompactionReq:        isCompactionReq,
					isStandaloneCompaction: isStandaloneComp,
				})
				retryRes, retryErr := s.executePromptOrParkWithRetry(retryCtx, retryBlocks, onDelta, 0)
				if retryErr == nil {
					res = retryRes
					sid = newSid
					mode = "high_capacity_fallback"
					rolloverReason = "rollover_high_tier_429"
					if retrySe := DetectAntigravitySoftError(retryRes.Text); isUpstreamPoolCapacitySoftError(retrySe) &&
						retryRes.FailedNotice == "" && retryRes.EmittedDeltas == 0 && len(retryRes.ToolCalls) == 0 &&
						nextFallbackTier(curModel, targetTier) != "" {
						curTier = targetTier
						continue
					}
					if DetectAntigravitySoftError(retryRes.Text) == nil && retryRes.FailedNotice == "" {
						s.mu.Lock()
						strikes, cd := s.armCapacityFallbackLocked(newSid, targetTier, "capacity_429", capacityFallbackNow())
						s.mu.Unlock()
						LogEvent("HIGH_TIER_CAPACITY_FALLBACK", "Recovered from high-tier TPU capacity 429/529 via fresh lower-tier session", map[string]any{
							"convKey":      s.ConvKey,
							"oldSessionId": prevSid,
							"newSessionId": newSid,
							"model":        curModel,
							"fromTier":     curTier,
							"toTier":       targetTier,
							"strikes":      strikes,
							"cooldownSec":  int(cd.Seconds()),
						})
					}
					curTier = targetTier
					break
				}
				if isSoftDetachedErr(retryErr) {
					return TurnResult{}, retryErr
				}
				var retryAbort *rpcRetryAbortedError
				if errors.As(retryErr, &retryAbort) {
					return TurnResult{}, retryErr
				}
				if errors.Is(retryErr, context.Canceled) || errors.Is(retryErr, context.DeadlineExceeded) {
					s.mu.Lock()
					s.MessageCount = len(messages)
					s.lastTurnAssistant = false
					s.cancelledHistoryFp = lazyHistFp()
					s.mu.Unlock()
					return TurnResult{}, retryErr
				}
				if IsTosViolationError(retryErr.Error()) {
					TripTosCircuitBreaker(retryErr.Error())
				}
				s.preserveOrHibernateOnRpcError(retryErr)
				return TurnResult{}, retryErr
			}
		}
	}
	return s.finalizeCompletedTurn(ctx, res, turnStart, firstDeltaNs.Load(), messages, currentFirstFp, isCompactionReq, sid, mode, rolloverReason)
}

// consumeToolCatalogLocked updates the session's active tool whitelist in-place and returns any
// prompt-tail delta notice needed so the model learns about added/changed/removed tools without
// invalidating session/new's tools/list prefix cache. Caller holds s.mu.
func (s *AcpProjectSession) consumeToolCatalogLocked(newCat []ToolDef) (notice string, added, removed, changed []string) {
	aligned := make([]ToolDef, len(newCat))
	copy(aligned, newCat)
	initByName := make(map[string]string, len(s.InitialToolsCatalog))
	for _, it := range s.InitialToolsCatalog {
		if it.McpName != "" {
			initByName[it.Name] = it.McpName
		}
	}
	isLegacyBridge := sessionBridgeServerName(s.InitialToolsCatalog) == LegacyMcpBridgeName
	for i := range aligned {
		if isLegacyBridge && (aligned[i].Name == AwaitToolName || aligned[i].Name == LegacyAwaitToolName) {
			aligned[i].Name = LegacyAwaitToolName
			aligned[i].OutboundAlias = ""
			aligned[i].McpName = fmt.Sprintf("dsh_%s_%s", LegacyAwaitToolName, shortSha256(LegacyAwaitToolName)[:12])
			continue
		}
		if isLegacyBridge && (aligned[i].Name == CallToolName || aligned[i].Name == LegacyCallToolName) {
			aligned[i].Name = LegacyCallToolName
			aligned[i].OutboundAlias = ""
			aligned[i].McpName = fmt.Sprintf("dsh_%s_%s", LegacyCallToolName, shortSha256(LegacyCallToolName)[:12])
			continue
		}
		if initMcp, ok := initByName[aligned[i].Name]; ok {
			aligned[i].McpName = initMcp
		} else if isLegacyBridge && strings.HasPrefix(aligned[i].McpName, "ws_") {
			aligned[i].McpName = "dsh_" + strings.TrimPrefix(aligned[i].McpName, "ws_")
		}
	}
	refOld := s.ToolsCatalog
	if len(refOld) == 0 {
		refOld = s.InitialToolsCatalog
	}
	notice, added, removed, changed = buildToolDeltaNotice(refOld, aligned, s.DeclaredMcpTools)
	if len(added) == 0 && len(removed) == 0 && len(changed) == 0 {
		return "", nil, nil, nil
	}
	s.ToolsCatalog = aligned
	s.McpToHostMap = make(map[string]string, len(aligned)*2)
	for _, t := range aligned {
		s.McpToHostMap[t.McpName] = t.Name
	}
	for _, t := range newCat {
		if t.McpName != "" {
			s.McpToHostMap[t.McpName] = t.Name
		}
	}
	if len(s.ParkedCalls) > 0 || len(s.RelayedCalls) > 0 {
		for _, it := range s.InitialToolsCatalog {
			if (it.Name == AwaitToolName || it.Name == LegacyAwaitToolName) && it.McpName != "" {
				s.McpToHostMap[it.McpName] = it.Name
			}
		}
	}
	s.ToolFingerprint = toolCatalogFingerprint(aligned)
	return notice, added, removed, changed
}

// isUpstreamPoolCapacitySoftError identifies upstream per-tier TPU capacity throttling (429/529)
// while excluding account-level quota exhaustion ("Individual quota reached. Resets in ..."),
// which shares the Gemini quota pool across all thinking tiers.
func isUpstreamPoolCapacitySoftError(se *SoftError) bool {
	if se == nil || (se.StatusCode != http.StatusTooManyRequests && se.StatusCode != 529) {
		return false
	}
	if isAccountQuotaOrBillingExhausted(se.Message) {
		return false
	}
	return true
}

// failedNoticeError converts a turn that ended on an agy terminal notice after partial text into
// the same retryable error a notice-only turn produces.
func failedNoticeError(res TurnResult) error {
	if res.FailedNotice == "" {
		return nil
	}
	if se := DetectAntigravitySoftError("⚠ " + res.FailedNotice); se != nil {
		se.Message += " (after partial reply)"
		return se
	}
	return &SoftError{StatusCode: http.StatusServiceUnavailable, ErrorType: "api_error", Message: "Antigravity upstream failure after partial reply: " + res.FailedNotice}
}

func (s *AcpProjectSession) RunKeepalivePing(onDelta func(DeltaChunk)) TurnResult {
	s.mu.Lock()
	idleSec := int(time.Since(s.LastUsedAt).Seconds())
	estCached := s.LastCachedReadTokens
	if estCached == 0 {
		estCached = 30000
	}
	sid := s.SessionID
	s.mu.Unlock()

	LogEvent("KEEPALIVE_SKIP_BUSY", fmt.Sprintf("Session %s fast-acknowledged keepalive (0 upstream cost)", s.ConvKey), map[string]any{
		"sessionId":        sid,
		"cachedReadTokens": estCached,
		"idleSec":          idleSec,
	})
	if onDelta != nil {
		onDelta(DeltaChunk{Type: "text", Text: "ok"})
	}
	return TurnResult{
		StopReason: "end_turn",
		Text:       "ok",
		Usage:      &TurnUsage{InputTokens: 10, CachedReadTokens: estCached, OutputTokens: 1, StepsInPrompt: 1},
		SessionID:  sid,
		ConvKey:    s.ConvKey,
		ElapsedMs:  1,
		TtftMs:     1,
		Mode:       "keepalive_fast_ack",
	}
}
