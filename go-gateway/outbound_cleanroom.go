package main

import (
	"crypto/sha256"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"agy-acp-gateway-go/internal/cleanroom"
	gwcfg "agy-acp-gateway-go/internal/config"
)

const tosNegCacheTTL = time.Second

var (
	TosTrippedFile     = envStr("AGY_TOS_TRIPPED_FILE", filepath.Join(ProjectHome, "runtime", "tos-tripped"))
	tosMu              sync.Mutex
	tosTrippedMem      atomic.Bool
	tosNegCacheUntilNs atomic.Int64
	tosNegCacheFile    atomic.Value
	tosReasonMem       string
	tosPersisted       bool
)

func resetTosNegativeCache() {
	tosNegCacheUntilNs.Store(0)
}

// Only call this on RPC errors. Normal assistant output uses the complete template.
func IsTosViolationError(text string) bool {
	return cleanroom.IsTosViolationError(text)
}

func TripTosCircuitBreaker(reason string) {
	tosMu.Lock()
	tosNegCacheUntilNs.Store(0)
	tosTrippedMem.Store(true)
	sanitized, persistErr := cleanroom.WriteTosMarker(TosTrippedFile, reason)
	tosReasonMem = sanitized
	if persistErr == nil {
		tosPersisted = true
	}
	tosMu.Unlock()
	if persistErr != nil {
		LogEvent("TOS_WRITE_ERR", "Cannot persist circuit marker; memory latch remains closed", nil)
	}
	LogEvent("TOS_CIRCUIT_BREAKER", "Upstream account restriction detected; halting ACP", nil)
	GlobalEngine.StopEngine()
	StopImageGenEngine()
}

func tosTrippedLocked() (bool, string) {
	curFile := TosTrippedFile
	if curFile == "" {
		return tosTrippedMem.Load(), tosReasonMem
	}
	present, unreadable, reason := cleanroom.ReadTosMarker(curFile)
	if present {
		tosNegCacheUntilNs.Store(0)
		tosTrippedMem.Store(true)
		tosPersisted = true
		if reason != "" {
			tosReasonMem = reason
		}
		return true, tosReasonMem
	}
	if unreadable {
		tosNegCacheUntilNs.Store(0)
		tosTrippedMem.Store(true)
		tosReasonMem = reason
		return true, tosReasonMem
	}
	if tosPersisted {
		tosTrippedMem.Store(false)
		tosReasonMem = ""
		tosPersisted = false
	}
	if !tosTrippedMem.Load() {
		tosNegCacheFile.Store(curFile)
		tosNegCacheUntilNs.Store(time.Now().Add(tosNegCacheTTL).UnixNano())
	}
	return tosTrippedMem.Load(), tosReasonMem
}

func IsTosTripped() (bool, string) {
	if !tosTrippedMem.Load() {
		curFile := TosTrippedFile
		if curFile == "" {
			return false, ""
		}
		if until := tosNegCacheUntilNs.Load(); until > 0 && time.Now().UnixNano() < until {
			if cached, _ := tosNegCacheFile.Load().(string); cached == curFile {
				return false, ""
			}
		}
	}
	tosMu.Lock()
	defer tosMu.Unlock()
	return tosTrippedLocked()
}

func circuitError() error {
	if tripped, _ := IsTosTripped(); tripped {
		return newCircuitError()
	}
	return nil
}

func newCircuitError() error {
	return &SoftError{StatusCode: 503, ErrorType: "tos_circuit_breaker", Message: "Antigravity ToS circuit breaker is tripped; operator intervention required"}
}

func truncateRunesSafe(s string, n int) string {
	return gwcfg.TruncateRunes(s, n)
}

const maxPrunedSysCacheEntries = 64

type prunedSysCacheKey struct {
	hash     [32]byte
	rawLen   int
	wafStamp wafStateKey
}

type prunedSysCacheEntry struct {
	rawSys string
	pruned string
}

var prunedSysCache = struct {
	mu      sync.RWMutex
	entries map[prunedSysCacheKey]prunedSysCacheEntry
	order   []prunedSysCacheKey
}{
	entries: make(map[prunedSysCacheKey]prunedSysCacheEntry, maxPrunedSysCacheEntries),
}

func PruneClientSystemPrompt(rawSys string) string {
	if rawSys == "" {
		return ""
	}
	stamp := currentWafStateKey()
	key := prunedSysCacheKey{
		hash:     sha256.Sum256([]byte(rawSys)),
		rawLen:   len(rawSys),
		wafStamp: stamp,
	}
	prunedSysCache.mu.RLock()
	if ent, ok := prunedSysCache.entries[key]; ok && ent.rawSys == rawSys {
		prunedSysCache.mu.RUnlock()
		return ent.pruned
	}
	prunedSysCache.mu.RUnlock()

	pruned := cleanroom.PruneClientSystemPrompt(rawSys, ApplyDynamicWafRules, ApplyDynamicWafRulesInWorkspaceContext)
	if len(rawSys) <= 512*1024 {
		prunedSysCache.mu.Lock()
		if _, exists := prunedSysCache.entries[key]; !exists {
			if len(prunedSysCache.order) >= maxPrunedSysCacheEntries {
				evict := prunedSysCache.order[0]
				prunedSysCache.order = prunedSysCache.order[1:]
				delete(prunedSysCache.entries, evict)
			}
			prunedSysCache.order = append(prunedSysCache.order, key)
		}
		prunedSysCache.entries[key] = prunedSysCacheEntry{rawSys: rawSys, pruned: pruned}
		prunedSysCache.mu.Unlock()
	}
	return pruned
}

func SanitizeStructuredValue(v any) any {
	return cleanroom.SanitizeStructuredValue(v, ApplyDynamicWafRulesInWorkspaceContext)
}

func SanitizeOutboundText(s string) string {
	return cleanroom.SanitizeOutboundText(s, ApplyDynamicWafRulesInWorkspaceContext)
}
