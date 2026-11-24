package main

import (
	"crypto/sha256"
	"path/filepath"
	"sync"
	"sync/atomic"

	"agy-acp-gateway-go/internal/cleanroom"
	gwcfg "agy-acp-gateway-go/internal/config"
)

var (
	TosTrippedFile = envStr("AGY_TOS_TRIPPED_FILE", filepath.Join(ProjectHome, "runtime", "tos-tripped"))
	tosMu          sync.Mutex
	tosTrippedMem  atomic.Bool
	tosReasonMem   string
	tosPersisted   bool
)

// Only call this on RPC errors. Normal assistant output uses the complete template.
func IsTosViolationError(text string) bool {
	return cleanroom.IsTosViolationError(text)
}

func TripTosCircuitBreaker(reason string) {
	tosMu.Lock()
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
}

func tosTrippedLocked() (bool, string) {
	if TosTrippedFile == "" {
		return tosTrippedMem.Load(), tosReasonMem
	}
	present, unreadable, reason := cleanroom.ReadTosMarker(TosTrippedFile)
	if present {
		tosTrippedMem.Store(true)
		tosPersisted = true
		if reason != "" {
			tosReasonMem = reason
		}
		return true, tosReasonMem
	}
	if unreadable {
		tosTrippedMem.Store(true)
		tosReasonMem = reason
		return true, tosReasonMem
	}
	if tosPersisted {
		tosTrippedMem.Store(false)
		tosReasonMem = ""
		tosPersisted = false
	}
	return tosTrippedMem.Load(), tosReasonMem
}

func IsTosTripped() (bool, string) {
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
	mu      sync.Mutex
	entries map[prunedSysCacheKey]prunedSysCacheEntry
	order   []prunedSysCacheKey
}{
	entries: make(map[prunedSysCacheKey]prunedSysCacheEntry, maxPrunedSysCacheEntries),
}

func movePrunedKeyToMRULocked(key prunedSysCacheKey) {
	n := len(prunedSysCache.order)
	if n <= 1 || prunedSysCache.order[n-1] == key {
		return
	}
	for i := n - 2; i >= 0; i-- {
		if prunedSysCache.order[i] == key {
			copy(prunedSysCache.order[i:], prunedSysCache.order[i+1:])
			prunedSysCache.order[n-1] = key
			return
		}
	}
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
	prunedSysCache.mu.Lock()
	if ent, ok := prunedSysCache.entries[key]; ok && ent.rawSys == rawSys {
		movePrunedKeyToMRULocked(key)
		res := ent.pruned
		prunedSysCache.mu.Unlock()
		return res
	}
	prunedSysCache.mu.Unlock()

	compiled := currentCompiledWafRules()
	var applyWaf, applyWafCtx func(string) string
	if compiled != nil {
		applyWaf = compiled.Apply
		applyWafCtx = compiled.ApplyInWorkspaceContext
	}
	pruned := cleanroom.PruneClientSystemPrompt(rawSys, applyWaf, applyWafCtx)
	if len(rawSys) <= 512*1024 {
		prunedSysCache.mu.Lock()
		if _, exists := prunedSysCache.entries[key]; exists {
			movePrunedKeyToMRULocked(key)
		} else {
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
	compiled := currentCompiledWafRules()
	var applyWafCtx func(string) string
	if compiled != nil {
		applyWafCtx = compiled.ApplyInWorkspaceContext
	}
	return cleanroom.SanitizeStructuredValue(v, applyWafCtx)
}

func SanitizeOutboundText(s string) string {
	return cleanroom.SanitizeOutboundText(s, ApplyDynamicWafRulesInWorkspaceContext)
}
