package session

import "math"

const (
	CacheHitSlackTokens        = 8192
	CacheShrinkMarginTokens    = 2048
	CachePrefixHitMarginTokens = 2500
	CacheGapTTLMillis          = 300000
)

// ClampedSumInt64 sums non-negative int64 inputs without signed overflow.
func ClampedSumInt64(a, b int64) int64 {
	if a <= 0 {
		if b <= 0 {
			return 0
		}
		return b
	}
	if b <= 0 {
		return a
	}
	if a > math.MaxInt64-b {
		return math.MaxInt64
	}
	return a + b
}

// ClassifyCacheMissSpec labels why a measured turn missed the upstream implicit prompt cache.
func ClassifyCacheMissSpec(prevInput, prevCached, curInput, curCached int64, gapMs int64, modelSwitched bool) string {
	if prevInput < 0 {
		prevInput = 0
	}
	if prevCached < 0 {
		prevCached = 0
	}
	if curInput < 0 {
		curInput = 0
	}
	if curCached < 0 {
		curCached = 0
	}
	total := ClampedSumInt64(curInput, curCached)
	if total <= 0 {
		return ""
	}
	slack := total / 10
	if slack < CacheHitSlackTokens {
		slack = CacheHitSlackTokens
	}
	if curInput <= slack {
		return ""
	}
	if modelSwitched {
		return "model_switch"
	}
	prevTotal := ClampedSumInt64(prevInput, prevCached)
	if prevTotal > total && prevTotal-total > CacheShrinkMarginTokens {
		return "shrink"
	}
	halfPrev := prevTotal/2 + (prevTotal & 1)
	if prevTotal > 0 && curCached > 0 &&
		(curCached >= prevTotal || prevTotal-curCached <= CachePrefixHitMarginTokens) &&
		curCached >= halfPrev {
		return ""
	}
	if gapMs >= CacheGapTTLMillis {
		return "gap_ttl"
	}
	if curCached == 0 {
		return "zero"
	}
	return "prefix_only"
}
