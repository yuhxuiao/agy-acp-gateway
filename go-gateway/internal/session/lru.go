package session

import "time"

// LruCandidate describes the comparable cost/recency attributes of an evictable pool worker.
type LruCandidate struct {
	IsUtil        bool
	Disposable    bool
	LastUsed      time.Time
	ProtectedWarm bool
	CacheWarm     bool
	CostScore     float64
	ConvKey       string
}

// CompareLruCandidates orders lower-priority eviction victims first (util first, then disposable
// sessions such as ForceRolloverNext/compaction-pending, then unprotected, cold, lower weighted
// token cost score, older last-used timestamp, lexical convKey tiebreak).
func CompareLruCandidates(a, b LruCandidate) bool {
	if a.IsUtil != b.IsUtil {
		return a.IsUtil
	}
	if a.Disposable != b.Disposable {
		return a.Disposable
	}
	if a.ProtectedWarm != b.ProtectedWarm {
		return !a.ProtectedWarm
	}
	if a.CacheWarm != b.CacheWarm {
		return !a.CacheWarm
	}
	if a.CostScore != b.CostScore {
		return a.CostScore < b.CostScore
	}
	if !a.LastUsed.Equal(b.LastUsed) {
		return a.LastUsed.Before(b.LastUsed)
	}
	return a.ConvKey < b.ConvKey
}
