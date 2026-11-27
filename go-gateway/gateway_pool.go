package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"agy-acp-gateway-go/internal/acp"
	gwcfg "agy-acp-gateway-go/internal/config"
	"agy-acp-gateway-go/internal/session"
	gwstore "agy-acp-gateway-go/internal/store"
)

var strictLowerUUIDRe = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

type sessionWarnHookKey struct{}
type httpReqStartKey struct{}
type reqFingerprintKey struct{}
type firstUserFpKey struct{}
type toolCatalogFpKey struct{}

func withRequestFingerprint(ctx context.Context, fp string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if fp == "" {
		return ctx
	}
	return context.WithValue(ctx, reqFingerprintKey{}, fp)
}

func requestFingerprintFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	fp, _ := ctx.Value(reqFingerprintKey{}).(string)
	return fp
}

func withFirstUserFingerprint(ctx context.Context, fp string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if fp == "" {
		return ctx
	}
	return context.WithValue(ctx, firstUserFpKey{}, fp)
}

func firstUserFingerprintFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	fp, _ := ctx.Value(firstUserFpKey{}).(string)
	return fp
}

func withToolCatalogFp(ctx context.Context, fp string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if fp == "" {
		return ctx
	}
	return context.WithValue(ctx, toolCatalogFpKey{}, fp)
}

func toolCatalogFpFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	fp, _ := ctx.Value(toolCatalogFpKey{}).(string)
	return fp
}

func withHttpRequestStart(r *http.Request, t time.Time) *http.Request {
	if r == nil || t.IsZero() {
		return r
	}
	return r.WithContext(context.WithValue(r.Context(), httpReqStartKey{}, t))
}

func httpRequestStartFromContext(ctx context.Context) (time.Time, bool) {
	if ctx == nil {
		return time.Time{}, false
	}
	t, ok := ctx.Value(httpReqStartKey{}).(time.Time)
	return t, ok && !t.IsZero()
}

func withSessionWarnHook(r *http.Request, fn func(string)) *http.Request {
	if r == nil || fn == nil {
		return r
	}
	return r.WithContext(context.WithValue(r.Context(), sessionWarnHookKey{}, fn))
}

func notifySessionWarn(ctx context.Context, s *AcpProjectSession) {
	if ctx == nil || s == nil {
		return
	}
	fn, ok := ctx.Value(sessionWarnHookKey{}).(func(string))
	if !ok || fn == nil {
		return
	}
	s.mu.Lock()
	warn := s.LastCacheWarning
	s.mu.Unlock()
	if warn != "" {
		fn(warn)
	}
}

type GatewayServer struct {
	Engine              acp.Engine
	Store               gwstore.SessionStore
	stopReaper          chan struct{}
	reaperDone          chan struct{}
	closeOnce           sync.Once
	workerAdmitMu       sync.Mutex
	activeProbeWorkers  atomic.Int32
	mu                  sync.RWMutex
	sessions            map[string]*AcpProjectSession
	tenants             map[string]*AcpProjectSession
	reqCounter          atomic.Int64
	lastConvCleanupUnix   atomic.Int64
	lastDbCompactUnix     atomic.Int64
	lastWalCheckpointUnix atomic.Int64
	engineIdleSinceNs     atomic.Int64
	shuttingDown        atomic.Bool
}

// IsShuttingDown reports whether BeginShutdown has been triggered.
func (gs *GatewayServer) IsShuttingDown() bool {
	return gs != nil && gs.shuttingDown.Load()
}

// BeginShutdown marks the server as shutting down (causing new non-health HTTP requests to receive 503)
// and immediately unblocks idle HotParked MCP tools/call handlers (!s.IsBusy && s.activeTurn == nil)
// so http.Server.Shutdown drains cleanly in milliseconds without disturbing active streaming SSE turns.
func (gs *GatewayServer) BeginShutdown() int {
	if gs == nil {
		return 0
	}
	gs.shuttingDown.Store(true)
	eng := gs.engine()

	gs.mu.RLock()
	targets := make([]*AcpProjectSession, 0, len(gs.sessions))
	for _, s := range gs.sessions {
		if s != nil {
			targets = append(targets, s)
		}
	}
	gs.mu.RUnlock()

	unblocked := 0
	for _, s := range targets {
		var cancelSid string
		var cancelPrompt context.CancelFunc
		var shouldPersist bool
		var pendingResultChans []chan map[string]any

		s.mu.Lock()
		// Never disturb an active streaming SSE turn or claimed in-flight request.
		if s.IsBusy || s.activeTurn != nil || s.turnClaimed {
			s.mu.Unlock()
			continue
		}
		hasParked := len(s.ParkedCalls) > 0 || len(s.RelayedCalls) > 0 || len(s.PendingSiblingCalls) > 0
		if !hasParked {
			s.mu.Unlock()
			continue
		}
		sid := s.SessionID
		keepResume := sid != "" && s.MessageCount > 0 && !strings.HasPrefix(s.ConvKey, "util:")
		if keepResume {
			s.recordHibernatedParkedCallsLocked()
		}
		// Detach pending MCP ResultChans under s.mu, and push shutdown errors ONLY AFTER
		// eng.CancelSession(cancelSid) so agy --worker cannot trigger ghost follow-up steps during shutdown.
		seenChan := make(map[chan map[string]any]bool, len(s.ParkedCalls)+len(s.RelayedCalls)+len(s.PendingSiblingCalls))
		collectChan := func(ch chan map[string]any) {
			if ch != nil && !seenChan[ch] {
				seenChan[ch] = true
				pendingResultChans = append(pendingResultChans, ch)
			}
		}
		for k, p := range s.ParkedCalls {
			if p != nil {
				collectChan(p.ResultChan)
			}
			delete(s.ParkedCalls, k)
		}
		for k, p := range s.RelayedCalls {
			if p != nil {
				collectChan(p.ResultChan)
			}
			delete(s.RelayedCalls, k)
		}
		for _, sib := range s.PendingSiblingCalls {
			if sib != nil {
				collectChan(sib.ResultChan)
			}
		}
		s.LastDeliveredCallIDs = nil
		s.PendingSiblingCalls = nil
		s.abortedSiblingCalls = nil
		s.ParkedSince = time.Time{}
		cancelPrompt = s.promptCancel
		if cancelPrompt != nil {
			s.promptGen++
		}
		s.promptCancel = nil
		s.promptLastActivityNs = nil
		s.promptStart = time.Time{}
		if sid != "" {
			cancelSid = sid
			hibSid := ""
			if keepResume {
				hibSid = sid
			}
			s.demoteToHibernatedLocked(hibSid)
			shouldPersist = true
		} else {
			s.syncPhaseLocked()
		}
		s.mu.Unlock()

		unblocked++
		if cancelPrompt != nil {
			cancelPrompt()
		}
		if cancelSid != "" && eng != nil {
			eng.CancelSession(cancelSid)
		}
		errMsg := mcpToolError("Gateway is shutting down; session hibernated.")
		for _, ch := range pendingResultChans {
			select {
			case ch <- errMsg:
			default:
			}
		}
		if shouldPersist {
			s.persistSessionAsync()
		}
	}
	return unblocked
}

func (gs *GatewayServer) engine() acp.Engine {
	if gs != nil && gs.Engine != nil {
		return gs.Engine
	}
	return GlobalEngine
}

func (gs *GatewayServer) store() gwstore.SessionStore {
	if gs != nil && gs.Store != nil {
		return gs.Store
	}
	return GlobalStore
}

// LookupReadOnlySessionDB returns a live session's persistent read-only SQLite handle for sid,
// if one exists, without holding gs.mu or s.mu across SQLite I/O.
func (gs *GatewayServer) LookupReadOnlySessionDB(sid string) *sql.DB {
	if gs == nil || sid == "" {
		return nil
	}
	gs.mu.RLock()
	cands := make([]*AcpProjectSession, 0, len(gs.sessions))
	for _, s := range gs.sessions {
		if s != nil {
			cands = append(cands, s)
		}
	}
	gs.mu.RUnlock()
	for _, s := range cands {
		s.mu.Lock()
		match := strings.EqualFold(s.SessionID, sid)
		liveSid := s.SessionID
		s.mu.Unlock()
		if match {
			return s.getReadOnlySessionDB(liveSid)
		}
	}
	return nil
}

func (gs *GatewayServer) stateStore() (*StateStore, bool) {
	st, ok := gs.store().(*StateStore)
	return st, ok && st != nil
}

func (gs *GatewayServer) bindSessionDeps(s *AcpProjectSession) {
	if gs == nil || s == nil {
		return
	}
	s.onBeforeSpawn = gs.reserveWorkerSlot
	if s.Engine == nil && gs.Engine != nil {
		s.Engine = gs.Engine
	}
	if s.Store == nil && gs.Store != nil {
		s.Store = gs.Store
	}
}

var activeGatewayPool atomic.Pointer[GatewayServer]

func reserveGlobalProbeWorkerSlot(s *AcpProjectSession) (func(), error) {
	gs := activeGatewayPool.Load()
	if gs == nil {
		return func() {}, nil
	}
	return gs.reserveProbeWorkerSlot(s)
}

func NewGatewayServer() *GatewayServer {
	return NewGatewayServerWithDeps(GlobalEngine, GlobalStore)
}

func NewGatewayServerWithDeps(eng acp.Engine, st gwstore.SessionStore) *GatewayServer {
	if st == nil {
		st = GlobalStore
	}
	if eng == nil {
		eng = GlobalEngine
	}
	if st == GlobalStore && !GlobalStore.IsReady() && GatewayStateDB != "" {
		GlobalStore.Init(GatewayStateDB)
	}
	gs := &GatewayServer{
		Engine:     eng,
		Store:      st,
		stopReaper: make(chan struct{}), reaperDone: make(chan struct{}),
		sessions: make(map[string]*AcpProjectSession),
		tenants:  make(map[string]*AcpProjectSession),
	}
	activeGatewayPool.Store(gs)
	if stateSt, ok := gs.stateStore(); ok {
		if restored := stateSt.RestoreDetachedSessions(gs); restored > 0 {
			LogEvent("STATE_STORE_RESTORE", fmt.Sprintf("Restored %d detached sessions from gateway.db", restored), map[string]any{
				"db": GatewayStateDB,
			})
		}
	}
	go func() {
		defer close(gs.reaperDone)
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-gs.stopReaper:
				return
			case <-ticker.C:
				gs.reapIdleWorkers()
			}
		}
	}()
	return gs
}

func (gs *GatewayServer) Close() {
	activeGatewayPool.CompareAndSwap(gs, nil)
	gs.mu.RLock()
	var targets []*AcpProjectSession
	for _, s := range gs.sessions {
		if s != nil {
			targets = append(targets, s)
		}
	}
	gs.mu.RUnlock()
	for _, s := range targets {
		if s.HasDetachedTurn() {
			s.AbortDetachedTurn("server_close")
		}
	}
	if gs.stopReaper != nil {
		gs.closeOnce.Do(func() { close(gs.stopReaper) })
		<-gs.reaperDone
	}
	gs.store().Flush()
}

func isNonPreemptiveSession(s *AcpProjectSession) bool {
	return s != nil && (s.isWafBisectProbe || strings.HasPrefix(s.ConvKey, "util:"))
}

func (gs *GatewayServer) countPoolWorkers() int {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	cnt := 0
	for _, s := range gs.sessions {
		s.mu.Lock()
		if s.isEngineSlotOccupiedLocked() {
			cnt++
		}
		s.mu.Unlock()
	}
	return cnt
}

func (gs *GatewayServer) countLiveWorkers() int {
	return gs.countPoolWorkers() + int(gs.activeProbeWorkers.Load())
}

func (gs *GatewayServer) countTotalLiveWorkers() int {
	return gs.countLiveWorkers()
}

// routingSessionKeyToExplicitIdent converts a router sticky sessionKey ("hdr:<id>", "pck:<id>",
// "meta:<id>", optionally with ":ag:<agent>") into the canonical "sid:<id>[:ag:<agent>]" prefix
// used by explicitConversationIdentity(s.ConvKey).
func routingSessionKeyToExplicitIdent(sessionKey string) string {
	sk := strings.TrimSpace(sessionKey)
	if sk == "" {
		return ""
	}
	if strings.HasPrefix(sk, "sid:") {
		if ident := explicitConversationIdentity(stripSlotSuffix(sk)); ident != "" {
			return ident
		}
		return stripSlotSuffix(sk)
	}
	var rest string
	switch {
	case strings.HasPrefix(sk, "hdr:"):
		rest = sk[4:]
	case strings.HasPrefix(sk, "pck:"):
		rest = sk[4:]
	case strings.HasPrefix(sk, "meta:"):
		rest = sk[5:]
	case strings.HasPrefix(sk, "usr:"):
		rest = sk[4:]
	default:
		return ""
	}
	agentID := ""
	if idx := strings.Index(rest, ":ag:"); idx >= 0 {
		agentID = strings.TrimSpace(rest[idx+4:])
		rest = rest[:idx]
	}
	rest = strings.TrimSpace(rest)
	if rest == "" {
		return ""
	}
	if strings.HasPrefix(rest, "{") {
		var parsed struct {
			SessionID string `json:"session_id"`
			AgentID   string `json:"agent_id"`
		}
		if json.Unmarshal([]byte(rest), &parsed) == nil {
			if sid := strings.TrimSpace(parsed.SessionID); sid != "" {
				rest = sid
			}
			if agentID == "" {
				agentID = strings.TrimSpace(parsed.AgentID)
			}
		}
	} else if m := userIDSessionUUIDRe.FindStringSubmatch(rest); len(m) > 1 {
		rest = m[1]
	}
	if agentID != "" {
		return "sid:" + rest + ":ag:" + agentID
	}
	return "sid:" + rest
}

// CancelParkedConversation forcefully hibernates/cancels any live parked or idle session belonging
// to sessionKey or owning any of callIDs. Used when MONITOR_TRIP_HOP re-routes an active conversation
// away from a tripped/red account so zombie ws_await_result relays cannot keep burning 80x quota.
func (gs *GatewayServer) CancelParkedConversation(sessionKey string, callIDs []string, reason string) int {
	if gs == nil || (strings.TrimSpace(sessionKey) == "" && len(callIDs) == 0) {
		return 0
	}
	if strings.TrimSpace(reason) == "" {
		reason = "monitor_trip_hop_cancel"
	}
	targetIdent := routingSessionKeyToExplicitIdent(sessionKey)
	callIDSet := make(map[string]bool, len(callIDs))
	for _, cid := range callIDs {
		if cid = strings.TrimSpace(cid); cid != "" {
			callIDSet[cid] = true
		}
	}

	gs.mu.RLock()
	candidates := make([]*AcpProjectSession, 0, len(gs.sessions))
	for _, s := range gs.sessions {
		if s != nil {
			candidates = append(candidates, s)
		}
	}
	gs.mu.RUnlock()

	var matched []*AcpProjectSession
	for _, s := range candidates {
		s.mu.Lock()
		live := s.SessionID != ""
		hasParked := len(s.ParkedCalls) > 0 || len(s.RelayedCalls) > 0 || len(s.PendingSiblingCalls) > 0
		isHit := false
		if live {
			for cid := range callIDSet {
				if _, ok := s.ParkedCalls[cid]; ok {
					isHit = true
					break
				}
				if _, ok := s.RelayedCalls[cid]; ok {
					isHit = true
					break
				}
			}
			if !isHit && len(callIDSet) > 0 {
				for _, deliv := range s.LastDeliveredCallIDs {
					if callIDSet[deliv] {
						isHit = true
						break
					}
				}
			}
			if !isHit && sessionKey != "" {
				stripped := stripSlotSuffix(s.ConvKey)
				if stripped == sessionKey || s.ConvKey == sessionKey {
					isHit = true
				} else if targetIdent != "" && explicitConversationIdentity(stripped) == targetIdent {
					isHit = true
				}
			}
		}
		shouldCancel := isHit && (hasParked || !s.IsBusy || s.detachedTurn != nil)
		s.mu.Unlock()
		if shouldCancel {
			matched = append(matched, s)
		}
	}

	cancelled := 0
	for _, s := range matched {
		if s.HibernateWorker(reason, true) {
			cancelled++
		}
	}
	if cancelled > 0 {
		LogEvent("MONITOR_TRIP_HOP_CANCELLED", "Cancelled orphaned parked session(s) on hopped-off account", map[string]any{
			"sessionKey": sessionKey,
			"cancelled":  cancelled,
			"reason":     reason,
		})
	}
	return cancelled
}

// CountOccupiedWorkerSlots returns the number of live engine slots that cannot currently
// be Pass-1 evicted (spawning, busy, hot-parked, or active probes). Used for slot-aware
// Turn-1 routing across multi-account workers without misclassifying evictable idle sessions.
func (gs *GatewayServer) CountOccupiedWorkerSlots() int {
	if gs == nil {
		return 0
	}
	gs.mu.RLock()
	cnt := 0
	for _, s := range gs.sessions {
		if s == nil {
			continue
		}
		s.mu.Lock()
		if s.isEngineSlotOccupiedLocked() && !s.canPass1EvictLocked() {
			cnt++
		}
		s.mu.Unlock()
	}
	gs.mu.RUnlock()
	return cnt + int(gs.activeProbeWorkers.Load())
}

// WorkerSlotCandidate describes an evictable (IDLE or PARKED) live worker on this gateway instance.
// BUSY/Spawning/Detached sessions are never returned as eviction candidates.
type WorkerSlotCandidate struct {
	ConvKey        string  `json:"convKey"`
	SessionID      string  `json:"sessionId,omitempty"`
	Status         string  `json:"status"` // "IDLE" or "PARKED"
	IdleSec        float64 `json:"idleSec"`
	ContextTokens  int     `json:"contextTokens"`
	ModelID        string  `json:"modelId,omitempty"`
	LruWeight      float64 `json:"lruWeight,omitempty"`
	LruScore       float64 `json:"lruScore"`
	Disposable     bool    `json:"disposable,omitempty"`
	IsUtil         bool    `json:"isUtil,omitempty"`
	ProtectedWarm  bool    `json:"protectedWarm,omitempty"`
	CacheWarm      bool    `json:"cacheWarm,omitempty"`
	ParkedToolName string  `json:"parkedToolName,omitempty"`
	ParkedLongExec bool    `json:"parkedLongExec,omitempty"`
	ParkedForSec   float64 `json:"parkedForSec,omitempty"`
	EvictTier      int     `json:"evictTier"`
	LastUsedMs     int64   `json:"lastUsedMs,omitempty"`
}

// WorkerSlotsSnapshot summarizes live/logical/hibernated session counts and ordered LRU candidates.
type WorkerSlotsSnapshot struct {
	InstanceID         string                `json:"instanceId"`
	LiveWorkers        int                   `json:"liveWorkers"`
	BusyWorkers        int                   `json:"busyWorkers"`
	IdleWorkers        int                   `json:"idleWorkers"`
	ParkedWorkers      int                   `json:"parkedWorkers"`
	MaxLiveWorkers     int                   `json:"maxLiveWorkers"`
	LogicalSessions    int                   `json:"logicalSessions"`
	HibernatedSessions int                   `json:"hibernatedSessions"`
	Candidates         []WorkerSlotCandidate `json:"candidates"`
}

// HibernateLRURequest configures a local or internal cross-account LRU hibernation request.
type HibernateLRURequest struct {
	Count         int      `json:"count"`
	IncludeParked bool     `json:"includeParked"`
	MinIdleSec    float64  `json:"minIdleSec"`
	MinParkedSec  float64  `json:"minParkedSec,omitempty"`
	ConvKeys      []string `json:"convKeys,omitempty"`
	Reason        string   `json:"reason"`
}

// HibernateLRUResponse reports how many sessions were hibernated and remaining live workers.
type HibernateLRUResponse struct {
	OK                   bool     `json:"ok"`
	Hibernated           int      `json:"hibernated"`
	HibernatedConvKeys   []string `json:"hibernatedConvKeys,omitempty"`
	RemainingLiveWorkers int      `json:"remainingLiveWorkers"`
}

const (
	elasticParkedHotProtectSec = 15.0
	elasticParkedRelayRaceSec  = 45.0
	elasticParkedSmallCtxMax   = 64000
	smallSubagentCtxMax        = 32000
	largeMainSessionCtxMin     = 100000
)

// computeWorkerEvictTier assigns a candidate to one of the 5 watermark-driven LRU tiers:
//   - T0: util:/Disposable (ForceRolloverNext / compactionPending) IDLE sessions
//   - T1: <32K IDLE sessions
//   - T2: PARKED sessions (eligibility gated by isElasticParkedOffloadEligible)
//   - T3: 32K..100K IDLE sessions (Carbon 4x protection via LruScore)
//   - T4: >=100K IDLE main sessions (evicted only under >=80% high pressure)
func computeWorkerEvictTier(cand WorkerSlotCandidate) int {
	if cand.Status == "PARKED" {
		return 2
	}
	if cand.IsUtil || cand.Disposable {
		return 0
	}
	if cand.ContextTokens < smallSubagentCtxMax {
		return 1
	}
	if cand.ContextTokens < largeMainSessionCtxMin {
		return 3
	}
	return 4
}

// isElasticParkedOffloadEligible reports whether a PARKED candidate can be offloaded cleanly
// while preserving 15s hot protection for fast tools:
//   - Normal/warn/burst pressure (allowHighCtx=false):
//     (a) long-exec tool (Bash/Agent/Task/...) parked >=15s with ctx <64K, or
//     (b) any tool parked >=45s (ahead of 55s ws_await_result relay) with ctx <64K.
//   - High cgroup pressure (>=80%, allowHighCtx=true):
//     also allows >=45s parked sessions with >=64K context.
func isElasticParkedOffloadEligible(cand WorkerSlotCandidate, allowHighCtx bool) bool {
	if cand.Status != "PARKED" {
		return false
	}
	parkedSec := cand.ParkedForSec
	if parkedSec <= 0 {
		parkedSec = cand.IdleSec
	}
	if parkedSec < elasticParkedHotProtectSec {
		return false
	}
	if cand.ParkedLongExec && cand.ContextTokens < elasticParkedSmallCtxMax {
		return true
	}
	if parkedSec >= elasticParkedRelayRaceSec {
		if allowHighCtx || cand.ContextTokens < elasticParkedSmallCtxMax {
			return true
		}
	}
	return false
}

func computeWorkerLruScore(isUtil, disposable, cacheWarm, protectedWarm bool, costScore, idleSec float64, isParked bool) float64 {
	if isParked {
		return 2e9 - idleSec
	}
	if isUtil {
		return -2e9 - idleSec
	}
	if disposable {
		return -1e9 - idleSec
	}
	if !cacheWarm {
		return -idleSec
	}
	if protectedWarm {
		return 1e9 + costScore - idleSec
	}
	return costScore - idleSec
}

// CollectWorkerSlots returns live worker counts, logical/hibernated session counts, and
// LRU-ordered IDLE/PARKED candidates (never including BUSY sessions).
func (gs *GatewayServer) CollectWorkerSlots(now time.Time) WorkerSlotsSnapshot {
	snap := WorkerSlotsSnapshot{
		InstanceID:     gwcfg.Current().InstanceID,
		MaxLiveWorkers: effectiveMaxLiveWorkers(),
		Candidates:     []WorkerSlotCandidate{},
	}
	if gs == nil {
		return snap
	}
	if now.IsZero() {
		now = time.Now()
	}
	gs.mu.RLock()
	for _, s := range gs.sessions {
		if s == nil {
			continue
		}
		snap.LogicalSessions++
		s.mu.Lock()
		occupied := s.isEngineSlotOccupiedLocked()
		if !occupied {
			if s.HibernatedSessionID != "" {
				snap.HibernatedSessions++
			}
			s.mu.Unlock()
			continue
		}
		snap.LiveWorkers++
		lastUsed := s.LastUsedAt
		idleSec := 0.0
		if !lastUsed.IsZero() {
			idleSec = now.Sub(lastUsed).Seconds()
			if idleSec < 0 {
				idleSec = 0
			}
		}
		ctxTok := s.LastCachedReadTokens
		if s.LastReportedContextUsed > ctxTok {
			ctxTok = s.LastReportedContextUsed
		}
		convKey := s.ConvKey
		sid := s.SessionID
		modelID := s.ModelID
		if s.canPass1EvictLocked() {
			snap.IdleWorkers++
			compPending := s.hasActiveCompactionPendingLocked(now)
			disposable := s.ForceRolloverNext || compPending
			s.mu.Unlock()

			isUtil := strings.HasPrefix(convKey, "util:")
			weight := GetModel(modelID).LruProtectionWeight()
			weightedTok := float64(ctxTok) * weight
			age := now.Sub(lastUsed)
			cacheWarm := !disposable && age < 300*time.Second
			protectedWarm := !disposable && cacheWarm && weightedTok >= 64000
			costScore := 0.0
			if cacheWarm {
				ageMin := age.Minutes()
				if ageMin < 1.0 {
					ageMin = 1.0
				}
				costScore = weightedTok / ageMin
			}
			score := computeWorkerLruScore(isUtil, disposable, cacheWarm, protectedWarm, costScore, idleSec, false)
			cand := WorkerSlotCandidate{
				ConvKey:       convKey,
				SessionID:     sid,
				Status:        "IDLE",
				IdleSec:       roundTo(idleSec, 1),
				ContextTokens: ctxTok,
				ModelID:       modelID,
				LruWeight:     weight,
				LruScore:      roundTo(score, 2),
				Disposable:    disposable,
				IsUtil:        isUtil,
				ProtectedWarm: protectedWarm,
				CacheWarm:     cacheWarm,
				LastUsedMs:    lastUsed.UnixMilli(),
			}
			cand.EvictTier = computeWorkerEvictTier(cand)
			snap.Candidates = append(snap.Candidates, cand)
		} else if s.canPass2ParkedEvictLocked() && sid != "" {
			snap.ParkedWorkers++
			parkedForSec := idleSec
			if !s.ParkedSince.IsZero() {
				if d := now.Sub(s.ParkedSince).Seconds(); d >= 0 {
					parkedForSec = d
				}
			}
			parkedToolName := ""
			parkedLongExec := false
			inspectParked := func(p *ParkedMcpCall) {
				if p == nil {
					return
				}
				name := strings.TrimSpace(p.Call.Name)
				if name != "" {
					if parkedToolName == "" {
						parkedToolName = name
					}
					if isLongExecToolName(name) {
						parkedLongExec = true
						parkedToolName = name
					}
				}
			}
			for _, cid := range s.LastDeliveredCallIDs {
				if p, ok := s.ParkedCalls[cid]; ok {
					inspectParked(p)
				}
			}
			for _, p := range s.ParkedCalls {
				inspectParked(p)
			}
			for _, p := range s.RelayedCalls {
				inspectParked(p)
			}
			s.mu.Unlock()
			weight := GetModel(modelID).LruProtectionWeight()
			score := computeWorkerLruScore(false, false, false, false, 0, parkedForSec, true)
			cand := WorkerSlotCandidate{
				ConvKey:        convKey,
				SessionID:      sid,
				Status:         "PARKED",
				IdleSec:        roundTo(idleSec, 1),
				ContextTokens:  ctxTok,
				ModelID:        modelID,
				LruWeight:      weight,
				LruScore:       roundTo(score, 2),
				ParkedToolName: parkedToolName,
				ParkedLongExec: parkedLongExec,
				ParkedForSec:   roundTo(parkedForSec, 1),
				EvictTier:      2,
				LastUsedMs:     lastUsed.UnixMilli(),
			}
			snap.Candidates = append(snap.Candidates, cand)
		} else {
			snap.BusyWorkers++
			s.mu.Unlock()
		}
	}
	gs.mu.RUnlock()

	probes := int(gs.activeProbeWorkers.Load())
	snap.LiveWorkers += probes
	snap.BusyWorkers += probes

	sort.Slice(snap.Candidates, func(i, j int) bool {
		a, b := snap.Candidates[i], snap.Candidates[j]
		aParked := a.Status == "PARKED"
		bParked := b.Status == "PARKED"
		if aParked != bParked {
			return !aParked
		}
		if a.LruScore != b.LruScore {
			return a.LruScore < b.LruScore
		}
		if a.IdleSec != b.IdleSec {
			return a.IdleSec > b.IdleSec
		}
		return a.ConvKey < b.ConvKey
	})
	return snap
}

// HibernateLRU hibernates the lowest-value LRU IDLE (and optionally PARKED) live sessions
// matching the request criteria, while never evicting BUSY sessions.
func (gs *GatewayServer) HibernateLRU(req HibernateLRURequest) HibernateLRUResponse {
	resp := HibernateLRUResponse{OK: true}
	if gs == nil {
		return resp
	}
	now := time.Now()
	reason := strings.TrimSpace(req.Reason)
	if reason == "" {
		reason = "hibernate_lru"
	}
	parkedReason := reason
	if !isMemoryGovernorHibernateReason(parkedReason) {
		parkedReason = "hibernate_lru_parked_" + parkedReason
	}
	minParkedSec := req.MinParkedSec
	if minParkedSec <= 0 {
		minParkedSec = req.MinIdleSec
	}

	snap := gs.CollectWorkerSlots(now)
	var targetSet map[string]bool
	if len(req.ConvKeys) > 0 {
		targetSet = make(map[string]bool, len(req.ConvKeys))
		for _, k := range req.ConvKeys {
			if k = strings.TrimSpace(k); k != "" {
				targetSet[k] = true
			}
		}
	}
	limit := req.Count
	if limit <= 0 {
		if len(targetSet) > 0 {
			limit = len(targetSet)
		} else if req.MinIdleSec > 0 || req.MinParkedSec > 0 {
			limit = len(snap.Candidates)
		} else {
			limit = 1
		}
	}

	for _, cand := range snap.Candidates {
		if resp.Hibernated >= limit {
			break
		}
		if len(targetSet) > 0 && !targetSet[cand.ConvKey] {
			continue
		}
		isParked := cand.Status == "PARKED"
		if isParked {
			if !req.IncludeParked {
				continue
			}
			if minParkedSec > 0 && cand.IdleSec < minParkedSec {
				continue
			}
		} else {
			if req.MinIdleSec > 0 && cand.IdleSec < req.MinIdleSec {
				continue
			}
		}

		gs.mu.RLock()
		sess := gs.sessions[cand.ConvKey]
		gs.mu.RUnlock()
		if sess == nil {
			continue
		}
		var ok bool
		if isParked {
			if cand.SessionID != "" {
				ok = sess.HibernateIfSession(cand.SessionID, parkedReason)
			}
		} else {
			ok = sess.HibernateWorker(reason, false)
		}
		if ok {
			resp.Hibernated++
			resp.HibernatedConvKeys = append(resp.HibernatedConvKeys, cand.ConvKey)
		}
	}
	resp.RemainingLiveWorkers = gs.countLiveWorkers()
	return resp
}

func acquireGlobalSlotLease(admitCtx context.Context, excl, priorityHint string) (func(), error) {
	if !isChildAccountWorker() && GlobalAccountManager != nil {
		primID := "primary"
		if prim, ok := GlobalAccountManager.GetPrimaryAccount(); ok && prim.ID != "" {
			primID = prim.ID
		}
		leaseID, admitErr := GlobalAccountManager.AcquireGlobalWorkerSlotExcluding(admitCtx, primID, excl, priorityHint)
		if admitErr != nil {
			return nil, admitErr
		}
		if leaseID != "" {
			amRef := GlobalAccountManager
			return func() { amRef.ReleaseGlobalWorkerSlot(leaseID) }, nil
		}
		return nil, nil
	}
	if isChildAccountWorker() {
		cfgSnap := gwcfg.Current()
		if cfgSnap.ParentDashPort > 0 {
			apiKey := defaultPeerAPIKey(cfgSnap)
			leaseID, admitErr := acquireGlobalWorkerSlotFromParent(admitCtx, cfgSnap.ParentDashPort, apiKey, cfgSnap.InstanceID, excl, priorityHint)
			if admitErr != nil {
				return nil, admitErr
			}
			if leaseID != "" {
				pPort := cfgSnap.ParentDashPort
				return func() { releaseGlobalWorkerSlotToParent(pPort, apiKey, leaseID) }, nil
			}
		}
	}
	return nil, nil
}

// Lock order: global admit gate (admitExecMu) -> workerAdmitMu -> gs.mu -> s.mu. Never enter with s.mu held.
func (gs *GatewayServer) reserveWorkerSlot(s *AcpProjectSession) error {
	nonPreemptive := isNonPreemptiveSession(s)
	excl := ""
	priorityHint := "normal"
	admitCtx := context.Background()
	if s != nil {
		excl = s.ConvKey
		admitCtx = s.getAdmitContext()
		if strings.HasPrefix(excl, "util:") {
			priorityHint = "util"
		} else if nonPreemptive {
			priorityHint = "probe"
		}
	}
	releaseGlobalLease, admitErr := acquireGlobalSlotLease(admitCtx, excl, priorityHint)
	if admitErr != nil {
		if !errors.Is(admitErr, context.Canceled) {
			workerCapRejectionsTotal.Add(1)
			if nonPreemptive {
				probeSlotRejectionsTotal.Add(1)
			}
		}
		return admitErr
	}
	if releaseGlobalLease != nil {
		// Deferred before workerAdmitMu.Lock() so workerAdmitMu.Unlock() runs first (LIFO).
		// By the time this runs on success, s.beginSpawnLocked() is already visible in CollectWorkerSlots().
		defer releaseGlobalLease()
	}
	gs.workerAdmitMu.Lock()
	defer gs.workerAdmitMu.Unlock()
	maxW := effectiveMaxLiveWorkers()
	gs.enforceWorkerCapWithLimit(s, !nonPreemptive, maxW)
	if gs.countLiveWorkers() >= maxW {
		workerCapRejectionsTotal.Add(1)
		if nonPreemptive {
			probeSlotRejectionsTotal.Add(1)
		}
		return &SoftError{StatusCode: 529, ErrorType: "overloaded_error", Message: "All live worker slots are busy; retry shortly"}
	}
	s.mu.Lock()
	s.beginSpawnLocked()
	s.mu.Unlock()
	return nil
}

// reserveProbeWorkerSlot isolates background WAF bisect probes into a non-preemptive
// sub-budget (MaxProbeWorkers) within both the global dual-layer admission gate and the
// local effectiveMaxLiveWorkers ceiling so probes never evict warm interactive or parked workers.
func (gs *GatewayServer) reserveProbeWorkerSlot(s *AcpProjectSession) (func(), error) {
	maxProbes := MaxProbeWorkers
	if maxProbes < 1 {
		maxProbes = 1
	}
	if int(gs.activeProbeWorkers.Load()) >= maxProbes {
		probeSlotRejectionsTotal.Add(1)
		workerCapRejectionsTotal.Add(1)
		return nil, &SoftError{StatusCode: 529, ErrorType: "overloaded_error", Message: "WAF probe worker slot busy; retry shortly"}
	}

	excl := ""
	admitCtx := context.Background()
	if s != nil {
		excl = s.ConvKey
		admitCtx = s.getAdmitContext()
	}
	releaseGlobalLease, admitErr := acquireGlobalSlotLease(admitCtx, excl, "probe")
	if admitErr != nil {
		if !errors.Is(admitErr, context.Canceled) {
			probeSlotRejectionsTotal.Add(1)
			workerCapRejectionsTotal.Add(1)
		}
		return nil, admitErr
	}
	var leaseOnce sync.Once
	releaseLease := func() {
		if releaseGlobalLease != nil {
			leaseOnce.Do(releaseGlobalLease)
		}
	}
	// Deferred before workerAdmitMu.Lock() so workerAdmitMu.Unlock() runs first (LIFO).
	// On success, activeProbeWorkers.Add(1) is already visible in CollectWorkerSlots() before releaseLease() runs.
	defer releaseLease()

	gs.workerAdmitMu.Lock()
	defer gs.workerAdmitMu.Unlock()
	maxW := effectiveMaxLiveWorkers()
	if int(gs.activeProbeWorkers.Load()) >= maxProbes {
		probeSlotRejectionsTotal.Add(1)
		workerCapRejectionsTotal.Add(1)
		return nil, &SoftError{StatusCode: 529, ErrorType: "overloaded_error", Message: "WAF probe worker slot busy; retry shortly"}
	}
	gs.enforceWorkerCapWithLimit(s, false, maxW)
	if gs.countLiveWorkers() >= maxW {
		probeSlotRejectionsTotal.Add(1)
		workerCapRejectionsTotal.Add(1)
		return nil, &SoftError{StatusCode: 529, ErrorType: "overloaded_error", Message: "All live worker slots are busy; retry shortly"}
	}
	gs.activeProbeWorkers.Add(1)
	if s != nil {
		s.mu.Lock()
		s.beginSpawnLocked()
		s.mu.Unlock()
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			if s != nil {
				s.mu.Lock()
				s.endSpawnLocked(false)
				s.mu.Unlock()
			}
			gs.activeProbeWorkers.Add(-1)
			notifyGlobalWorkerSlotFreed()
		})
	}, nil
}

type lruWorkerCandidate struct {
	sess              *AcpProjectSession
	isUtil            bool
	disposable        bool
	compactionPending bool
	lastUsed          time.Time
	ctxTok            int
	weightedTok       float64
	protectedWarm     bool
	cacheWarm         bool
	costScore         float64
	convKey           string
}

func compareLruWorkerCandidates(a, b lruWorkerCandidate) bool {
	return session.CompareLruCandidates(session.LruCandidate{
		IsUtil:        a.isUtil,
		Disposable:    a.disposable,
		LastUsed:      a.lastUsed,
		ProtectedWarm: a.protectedWarm,
		CacheWarm:     a.cacheWarm,
		CostScore:     a.costScore,
		ConvKey:       a.convKey,
	}, session.LruCandidate{
		IsUtil:        b.isUtil,
		Disposable:    b.disposable,
		LastUsed:      b.lastUsed,
		ProtectedWarm: b.protectedWarm,
		CacheWarm:     b.cacheWarm,
		CostScore:     b.costScore,
		ConvKey:       b.convKey,
	})
}

type parkedVictimCandidate struct {
	sess     *AcpProjectSession
	sid      string
	lastUsed time.Time
	convKey  string
}

func (gs *GatewayServer) enforceWorkerCap(exclude *AcpProjectSession) {
	gs.enforceWorkerCapMode(exclude, true)
}

func (gs *GatewayServer) enforceWorkerCapMode(exclude *AcpProjectSession, allowPreemptWarm bool) {
	gs.enforceWorkerCapWithLimit(exclude, allowPreemptWarm, effectiveMaxLiveWorkers())
}

func (gs *GatewayServer) enforceWorkerCapWithLimit(exclude *AcpProjectSession, allowPreemptWarm bool, maxW int) {
	if gs.countLiveWorkers() < maxW {
		return
	}
	evictNow := time.Now()
	gs.mu.RLock()
	var pass1a []lruWorkerCandidate
	var pass1bParked []parkedVictimCandidate
	var pass1c []lruWorkerCandidate
	for _, s := range gs.sessions {
		if s == nil || s == exclude {
			continue
		}
		s.mu.Lock()
		avail := s.canPass1EvictLocked()
		compPending := s.hasActiveCompactionPendingLocked(evictNow)
		if !avail {
			if allowPreemptWarm && compPending && s.canPass2ParkedEvictLocked() && s.SessionID != "" {
				pass1bParked = append(pass1bParked, parkedVictimCandidate{
					sess:     s,
					sid:      s.SessionID,
					lastUsed: s.LastUsedAt,
					convKey:  s.ConvKey,
				})
			}
			s.mu.Unlock()
			continue
		}
		lastUsed := s.LastUsedAt
		ctxTok := s.LastCachedReadTokens
		if s.LastReportedContextUsed > ctxTok {
			ctxTok = s.LastReportedContextUsed
		}
		modelID := s.ModelID
		convKey := s.ConvKey
		disposable := s.ForceRolloverNext || compPending
		s.mu.Unlock()

		isUtil := strings.HasPrefix(convKey, "util:")
		weight := GetModel(modelID).LruProtectionWeight()
		weightedTok := float64(ctxTok) * weight
		age := evictNow.Sub(lastUsed)
		cacheWarm := !disposable && age < 300*time.Second
		if !allowPreemptWarm && !isUtil && !disposable && cacheWarm {
			continue
		}
		protectedWarm := !disposable && cacheWarm && weightedTok >= 64000
		costScore := 0.0
		if cacheWarm {
			ageMin := age.Minutes()
			if ageMin < 1.0 {
				ageMin = 1.0
			}
			costScore = weightedTok / ageMin
		}
		cand := lruWorkerCandidate{
			sess:              s,
			isUtil:            isUtil,
			disposable:        disposable,
			compactionPending: compPending,
			lastUsed:          lastUsed,
			ctxTok:            ctxTok,
			weightedTok:       weightedTok,
			protectedWarm:     protectedWarm,
			cacheWarm:         cacheWarm,
			costScore:         costScore,
			convKey:           convKey,
		}
		if isUtil || disposable {
			pass1a = append(pass1a, cand)
		} else {
			pass1c = append(pass1c, cand)
		}
	}
	gs.mu.RUnlock()

	// Pass 1a: util: and disposable (ForceRolloverNext / compactionPending) idle sessions first.
	sort.Slice(pass1a, func(i, j int) bool {
		return compareLruWorkerCandidates(pass1a[i], pass1a[j])
	})
	for gs.countLiveWorkers() >= maxW && len(pass1a) > 0 {
		victim := pass1a[0]
		pass1a = pass1a[1:]
		reason := fmt.Sprintf("lru_cap_%d", maxW)
		if victim.compactionPending {
			reason = "lru_cap_compaction_pending"
		}
		victim.sess.HibernateWorker(reason, false)
	}

	// Pass 1b: compactionPending hot-parked sessions before evicting any innocent warm idle session,
	// preserving HibernatedSessionID + HibernatedParkedCallIDs (keepResumeState=true, no orphanCapStreak).
	if gs.countLiveWorkers() >= maxW && allowPreemptWarm && len(pass1bParked) > 0 {
		sort.Slice(pass1bParked, func(i, j int) bool {
			if !pass1bParked[i].lastUsed.Equal(pass1bParked[j].lastUsed) {
				return pass1bParked[i].lastUsed.Before(pass1bParked[j].lastUsed)
			}
			return pass1bParked[i].convKey < pass1bParked[j].convKey
		})
		for gs.countLiveWorkers() >= maxW && len(pass1bParked) > 0 {
			victim := pass1bParked[0]
			pass1bParked = pass1bParked[1:]
			if victim.sid != "" {
				victim.sess.HibernateIfSession(victim.sid, "compaction_pending_preempt")
			}
		}
	}

	// Pass 1c: regular cold/warm idle sessions ordered by cost-aware LRU.
	if gs.countLiveWorkers() >= maxW && len(pass1c) > 0 {
		sort.Slice(pass1c, func(i, j int) bool {
			return compareLruWorkerCandidates(pass1c[i], pass1c[j])
		})
		for gs.countLiveWorkers() >= maxW && len(pass1c) > 0 {
			victim := pass1c[0].sess
			pass1c = pass1c[1:]
			victim.HibernateWorker(fmt.Sprintf("lru_cap_%d", maxW), false)
		}
	}

	if gs.countLiveWorkers() < maxW || !allowPreemptWarm {
		return
	}
	// Pass 2: remaining idle parked workers that already hit MaxToolRelays or half of ParkedTtl,
	// preserving HibernatedSessionID + HibernatedParkedCallIDs for resume.
	now := time.Now()
	maxRelays := effectiveMaxToolRelays()
	parkedTtl := effectiveParkedTtl()
	gs.mu.RLock()
	var parkedVictims []parkedVictimCandidate
	for _, s := range gs.sessions {
		if s == nil || s == exclude {
			continue
		}
		s.mu.Lock()
		sessMaxRelays := maxRelays
		if sessMaxRelays > 0 && s.orphanCapStreak >= 1 {
			sessMaxRelays = 0
		}
		eligible := s.canPass2ParkedEvictLocked() &&
			((sessMaxRelays >= 0 && s.orphanRelayHops > 0 && s.orphanRelayHops >= sessMaxRelays) || (parkedTtl > 0 && now.Sub(s.LastUsedAt) >= parkedTtl/2))
		sid := s.SessionID
		lastUsed := s.LastUsedAt
		convKey := s.ConvKey
		s.mu.Unlock()
		if eligible {
			parkedVictims = append(parkedVictims, parkedVictimCandidate{
				sess:     s,
				sid:      sid,
				lastUsed: lastUsed,
				convKey:  convKey,
			})
		}
	}
	gs.mu.RUnlock()

	sort.Slice(parkedVictims, func(i, j int) bool {
		if !parkedVictims[i].lastUsed.Equal(parkedVictims[j].lastUsed) {
			return parkedVictims[i].lastUsed.Before(parkedVictims[j].lastUsed)
		}
		return parkedVictims[i].convKey < parkedVictims[j].convKey
	})

	for gs.countLiveWorkers() >= maxW && len(parkedVictims) > 0 {
		victim := parkedVictims[0]
		parkedVictims = parkedVictims[1:]
		if victim.sid != "" {
			victim.sess.HibernateIfSession(victim.sid, "reap_orphan_parked_lru")
		}
	}
}

func (gs *GatewayServer) reapIdleWorkers() {
	gs.prepareBinarySwitch()
	now := time.Now()
	eng := gs.engine()
	anyBusy := eng.IsBooting()

	gs.mu.Lock()
	var toHibernate []struct {
		sess   *AcpProjectSession
		sid    string
		reason string
		force  bool
	}
	var expiredCompRefs []*compactionTargetRef
	for k, s := range gs.sessions {
		s.mu.Lock()
		isActive := s.isActivelyWorkingLocked()
		if isActive {
			anyBusy = true
		}
		if !isActive && !s.turnClaimed && s.lastCompletedReqFp != "" && (s.lastCompletedAt.IsZero() || now.Sub(s.lastCompletedAt) >= 15*time.Second) {
			if s.lastCompletedCompactionTarget != nil {
				expiredCompRefs = append(expiredCompRefs, s.lastCompletedCompactionTarget)
				s.lastCompletedCompactionTarget = nil
			}
			s.lastCompletedReqFp = ""
			s.lastCompletedResult = TurnResult{}
			s.lastCompletedAt = time.Time{}
		}
		idleDur := now.Sub(s.LastUsedAt)
		sid := s.SessionID
		busy := isActive
		turnClaimed := s.turnClaimed
		persistedInDB := s.persistedInDB
		parkedCnt := len(s.ParkedCalls)
		hibParkedCnt := len(s.HibernatedParkedCallIDs)
		orphanHops := s.orphanRelayHops
		orphanStreak := s.orphanCapStreak
		var sinceRelay time.Duration
		if !s.lastOrphanRelayAt.IsZero() {
			sinceRelay = now.Sub(s.lastOrphanRelayAt)
		}
		forceRollover := s.ForceRolloverNext
		onlyStruct := parkedCnt > 0
		for _, p := range s.ParkedCalls {
			if p.Call.Name != "StructuredOutput" {
				onlyStruct = false
				break
			}
		}
		s.mu.Unlock()
		if sid == "" {
			evictTTL := 7200 * time.Second
			if persistedInDB && parkedCnt == 0 && hibParkedCnt == 0 {
				if isChildAccountWorker() {
					evictTTL = 10 * time.Minute
				} else {
					evictTTL = 15 * time.Minute
				}
			}
			if !busy && !turnClaimed && parkedCnt == 0 && idleDur >= evictTTL {
				delete(gs.sessions, k)
				delete(gs.tenants, s.TenantKey)
			}
			continue
		}

		if !s.IsAvailableForNewTurn() {
			maxRelays := effectiveMaxToolRelays()
			if maxRelays > 0 && orphanStreak >= 1 {
				maxRelays = 0
			}
			parkedTtl := effectiveParkedTtl()
			relayCapIdle := !busy && maxRelays >= 0 && orphanHops > 0 && orphanHops >= maxRelays && sinceRelay >= 90*time.Second
			if (onlyStruct && idleDur >= 5*time.Second) || (!busy && idleDur >= parkedTtl) || relayCapIdle {
				reason := fmt.Sprintf("reap_orphan_parked_%ds", int(idleDur.Seconds()))
				if onlyStruct {
					reason = "reap_structured_output"
				} else if relayCapIdle && idleDur < parkedTtl {
					reason = "reap_orphan_parked_relay_cap"
				}
				toHibernate = append(toHibernate, struct {
					sess   *AcpProjectSession
					sid    string
					reason string
					force  bool
				}{s, sid, reason, true})
			}
			continue
		}

		ttl := IdleWorkerTtl
		if strings.HasPrefix(s.ConvKey, "util:") || (forceRollover && UtilWorkerTtl < ttl) {
			ttl = UtilWorkerTtl
		}
		if idleDur >= ttl {
			toHibernate = append(toHibernate, struct {
				sess   *AcpProjectSession
				sid    string
				reason string
				force  bool
			}{s, sid, fmt.Sprintf("idle_%ds", int(idleDur.Seconds())), false})
		}
	}
	gs.mu.Unlock()
	for _, ref := range expiredCompRefs {
		ref.release()
	}

	for _, item := range toHibernate {
		if item.force && item.sid != "" {
			item.sess.HibernateIfSession(item.sid, item.reason)
		} else {
			item.sess.HibernateWorker(item.reason, item.force)
		}
	}

	if !anyBusy && promptsInFlightCount() == 0 && eng.IsAlive() {
		gs.workerAdmitMu.Lock()
		zeroLive := promptsInFlightCount() == 0 && eng.IsAlive() && gs.countTotalLiveWorkers() == 0
		shouldStop := false
		var switching bool
		if !zeroLive {
			gs.engineIdleSinceNs.Store(0)
		} else {
			switching = acpBinarySwitchPendingOn(eng)
			grace := effectiveEngineIdleGrace()
			if switching || grace <= 0 {
				shouldStop = true
			} else {
				nowNs := now.UnixNano()
				sinceNs := gs.engineIdleSinceNs.Load()
				if sinceNs == 0 {
					gs.engineIdleSinceNs.Store(nowNs)
				} else if time.Duration(nowNs-sinceNs) >= grace {
					shouldStop = true
				}
			}
			if shouldStop {
				gs.engineIdleSinceNs.Store(0)
				eng.StopEngine()
			}
		}
		gs.workerAdmitMu.Unlock()
		if shouldStop {
			if switching {
				LogEvent("ACP_BINARY_SWITCHED", "Stopped the engine for a new agy binary; the next request starts it", eng.BinaryInfo())
				publishEngineInfo()
			} else {
				LogEvent("ACP_ENGINE_IDLE_STOP", "Stopped shared agy-acp-900k engine (0 active sessions, 0 MB Go child RAM)", nil)
			}
		}
	} else {
		gs.engineIdleSinceNs.Store(0)
	}

	syncWaitingIdleEngineSignal()
	EvictStaleDashboardCache(now)

	if ConvKeepDays > 0 || AcpLogKeepDays > 0 || AcpLogMaxMB > 0 {
		prev := gs.lastConvCleanupUnix.Load()
		if now.Unix()-prev >= 600 && gs.lastConvCleanupUnix.CompareAndSwap(prev, now.Unix()) {
			if ConvKeepDays > 0 {
				gs.CleanupExpiredConversations(now)
			}
			gs.CleanupExpiredAcpLogs(now)
		}
	}

	if !testing.Testing() {
		if stateSt, ok := gs.stateStore(); ok && stateSt.IsReady() {
			prevCompact := gs.lastDbCompactUnix.Load()
			if prevCompact == 0 {
				// Defer first daily compaction by 1h after boot to avoid startup I/O contention.
				gs.lastDbCompactUnix.CompareAndSwap(0, now.Unix()-23*3600)
			} else if now.Unix()-prevCompact >= 86400 && gs.lastDbCompactUnix.CompareAndSwap(prevCompact, now.Unix()) {
				go func(st *StateStore, ts time.Time, keep int) {
					_, _ = st.CompactExpiredData(ts, keep)
				}(stateSt, now, ConvKeepDays)
			}
			prevCkpt := gs.lastWalCheckpointUnix.Load()
			if prevCkpt == 0 {
				gs.lastWalCheckpointUnix.CompareAndSwap(0, now.Unix())
			} else if now.Unix()-prevCkpt >= 900 && gs.lastWalCheckpointUnix.CompareAndSwap(prevCkpt, now.Unix()) {
				go func(st *StateStore) {
					st.mu.Lock()
					defer st.mu.Unlock()
					if st.db != nil {
						_, _ = st.db.Exec(`PRAGMA wal_checkpoint(PASSIVE);`)
					}
				}(stateSt)
			}
		}
	}
}

var strictAcpTimestampLogRe = regexp.MustCompile(`^acp-(worker|front)-\d{8}_\d{6}-\d+\.log$`)

func effectiveEngineIdleGrace() time.Duration {
	if EngineIdleGrace != engineIdleGraceBoot {
		if EngineIdleGrace < 0 {
			return 0
		}
		return EngineIdleGrace
	}
	cfg := gwcfg.Current()
	if cfg.EngineIdleGraceEnvSet {
		if cfg.EngineIdleGraceSec <= 0 {
			return 0
		}
		return time.Duration(cfg.EngineIdleGraceSec) * time.Second
	}
	if testing.Testing() {
		return 0
	}
	if cfg.EngineIdleGraceSec <= 0 {
		return 0
	}
	return time.Duration(cfg.EngineIdleGraceSec) * time.Second
}

func isolatedPureApiSubdir(sub string) string {
	if PureApiHome == "" || isWindowsCDrivePath(PureApiHome) {
		return ""
	}
	appRoot := filepath.Join(PureApiHome, "antigravity-cli")
	if isWindowsCDrivePath(appRoot) {
		return ""
	}
	resolvedRoot := appRoot
	if eval, err := filepath.EvalSymlinks(appRoot); err == nil && eval != "" {
		resolvedRoot = eval
	}
	if isWindowsCDrivePath(resolvedRoot) || strings.EqualFold(filepath.Base(resolvedRoot), "antigravity") {
		return ""
	}
	if appDir := rawConfiguredOrDiscoveredAppConvDir(); appDir != "" {
		appParent := filepath.Dir(appDir)
		if sameDirOrPath(appRoot, appParent) || sameDirOrPath(resolvedRoot, appParent) {
			return ""
		}
	}
	target := filepath.Join(appRoot, sub)
	if isWindowsCDrivePath(target) {
		return ""
	}
	if info, err := os.Lstat(target); err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return ""
	}
	return target
}

// CleanupExpiredAcpLogs removes expired or over-budget timestamped acp-worker-*.log and acp-front-*.log files
// under <PureApiHome>/antigravity-cli/log/ when AGY_ACP_LOG_KEEP_DAYS (or AGY_CONV_KEEP_DAYS) > 0
// or AGY_ACP_LOG_MAX_MB > 0. Enforces a hard >= 24h floor (never deleting active/5h/24h logs),
// strict filename regex, non-symlink check, and PureApiHome isolation.
func (gs *GatewayServer) CleanupExpiredAcpLogs(now time.Time) int {
	keepDays := AcpLogKeepDays
	if keepDays <= 0 {
		keepDays = ConvKeepDays
	}
	maxMB := AcpLogMaxMB
	if keepDays <= 0 && maxMB <= 0 {
		return 0
	}
	logDir := isolatedPureApiSubdir("log")
	if logDir == "" {
		return 0
	}
	entries, err := os.ReadDir(logDir)
	if err != nil {
		return 0
	}
	if now.IsZero() {
		now = time.Now()
	}
	protectCutoff := now.Add(-24 * time.Hour)
	var ageCutoff time.Time
	if keepDays > 0 {
		age := time.Duration(keepDays) * 24 * time.Hour
		if age < 24*time.Hour {
			age = 24 * time.Hour
		}
		ageCutoff = now.Add(-age)
	}

	type logFileEntry struct {
		path    string
		size    int64
		modTime time.Time
	}
	var remaining []logFileEntry
	var totalBytes int64
	cleanedByAge := 0
	cleanedBySize := 0

	for _, e := range entries {
		name := e.Name()
		if !strictAcpTimestampLogRe.MatchString(name) {
			continue
		}
		fp := filepath.Join(logDir, name)
		info, errL := os.Lstat(fp)
		if errL != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			continue
		}
		mt := info.ModTime()
		if keepDays > 0 && mt.Before(ageCutoff) && mt.Before(protectCutoff) {
			if remErr := os.Remove(fp); remErr == nil {
				cleanedByAge++
				continue
			}
		}
		remaining = append(remaining, logFileEntry{path: fp, size: info.Size(), modTime: mt})
		totalBytes += info.Size()
	}

	if maxMB > 0 {
		maxBytes := int64(maxMB) * 1024 * 1024
		if totalBytes > maxBytes {
			sort.Slice(remaining, func(i, j int) bool {
				if !remaining[i].modTime.Equal(remaining[j].modTime) {
					return remaining[i].modTime.Before(remaining[j].modTime)
				}
				return remaining[i].path < remaining[j].path
			})
			for _, lf := range remaining {
				if totalBytes <= maxBytes {
					break
				}
				// Hard 24h floor: never delete active/5h/24h logs regardless of size budget.
				if !lf.modTime.Before(protectCutoff) {
					continue
				}
				if remErr := os.Remove(lf.path); remErr == nil {
					totalBytes -= lf.size
					cleanedBySize++
				}
			}
		}
	}

	cleaned := cleanedByAge + cleanedBySize
	if cleaned > 0 {
		LogEvent("ACP_LOG_CLEANUP", fmt.Sprintf("Cleaned %d ACP log file(s) (age=%d, size=%d, keepDays=%d, maxMB=%d)", cleaned, cleanedByAge, cleanedBySize, keepDays, maxMB), map[string]any{
			"cleaned":       cleaned,
			"cleanedByAge":  cleanedByAge,
			"cleanedBySize": cleanedBySize,
			"keepDays":      keepDays,
			"maxMB":         maxMB,
		})
	}
	return cleaned
}

// CleanupExpiredConversations removes expired gateway conversation files from ConversationsDir
// only when all four safety gates pass (ConvKeepDays>0 & strict UUID, known gateway SID,
// zero active/hibernated references in memory, mtime older than cutoff & usage_steps synced).
// AppConversationsDir() is strictly read-only and is NEVER touched by cleanup.
func (gs *GatewayServer) CleanupExpiredConversations(now time.Time) int {
	convDir := ConversationsDir()
	st := gs.store()
	if gs == nil || ConvKeepDays <= 0 || convDir == "" || !st.IsReady() {
		return 0
	}
	if appDir := rawConfiguredOrDiscoveredAppConvDir(); appDir != "" && sameDirOrPath(convDir, appDir) {
		return 0
	}
	entries, err := os.ReadDir(convDir)
	if err != nil {
		return 0
	}
	candidates := make(map[string]bool)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		var stem string
		switch {
		case strings.HasSuffix(name, ".pb"):
			stem = strings.TrimSuffix(name, ".pb")
		case strings.HasSuffix(name, ".db"):
			stem = strings.TrimSuffix(name, ".db")
		case strings.HasSuffix(name, ".db-wal"):
			stem = strings.TrimSuffix(name, ".db-wal")
		case strings.HasSuffix(name, ".db-shm"):
			stem = strings.TrimSuffix(name, ".db-shm")
		default:
			continue
		}
		// Gate 1: strict lowercase UUID stem only.
		if strictLowerUUIDRe.MatchString(stem) {
			candidates[stem] = true
		}
	}
	if len(candidates) == 0 {
		return 0
	}

	// Gate 2: only clean session IDs recorded as owned by the gateway.
	gwSids := getKnownGatewaySids()
	if len(gwSids) == 0 {
		return 0
	}

	// Gate 3: exclude any SID referenced by live or hibernated in-memory sessions.
	activeSids := make(map[string]bool)
	gs.mu.RLock()
	for _, s := range gs.sessions {
		if s == nil {
			continue
		}
		s.mu.Lock()
		if s.SessionID != "" {
			activeSids[strings.ToLower(s.SessionID)] = true
		}
		if s.HibernatedSessionID != "" {
			activeSids[strings.ToLower(s.HibernatedSessionID)] = true
		}
		s.mu.Unlock()
	}
	gs.mu.RUnlock()

	if now.IsZero() {
		now = time.Now()
	}
	cutoff := now.Add(-time.Duration(ConvKeepDays) * 24 * time.Hour)
	brainDir := ""
	if sameDirOrPath(convDir, filepath.Join(PureApiHome, "antigravity-cli", "conversations")) {
		brainDir = isolatedPureApiSubdir("brain")
	}
	cleaned := 0
	cleanedBrains := 0

	for sid := range candidates {
		if !gwSids[sid] || activeSids[sid] {
			continue
		}
		pbPath := filepath.Join(convDir, sid+".pb")
		dbPath := filepath.Join(convDir, sid+".db")
		walPath := filepath.Join(convDir, sid+".db-wal")
		shmPath := filepath.Join(convDir, sid+".db-shm")

		lstatRegular := func(p string) (os.FileInfo, bool, bool) {
			info, errL := os.Lstat(p)
			if errL != nil {
				if os.IsNotExist(errL) {
					return nil, false, true
				}
				return nil, false, false
			}
			if info.Mode()&os.ModeSymlink != 0 || info.IsDir() {
				return nil, true, false
			}
			return info, true, true
		}

		pbInfo, hasPB, okPB := lstatRegular(pbPath)
		dbInfo, hasDB, okDB := lstatRegular(dbPath)
		walInfo, hasWAL, okWAL := lstatRegular(walPath)
		_, hasSHM, okSHM := lstatRegular(shmPath)
		if !okPB || !okDB || !okWAL || !okSHM || (!hasPB && !hasDB) {
			continue
		}
		// Gate 4a: all existing primary/WAL files must be strictly older than cutoff.
		if (hasPB && !pbInfo.ModTime().Before(cutoff)) ||
			(hasDB && !dbInfo.ModTime().Before(cutoff)) ||
			(hasWAL && !walInfo.ModTime().Before(cutoff)) {
			continue
		}
		// Gate 4b: if a non-empty analytics DB exists, ensure ingest_cursor is caught up first.
		if hasDB && dbInfo.Size() >= 100 {
			var walSize, walMtime int64
			if hasWAL {
				walSize = walInfo.Size()
				walMtime = walInfo.ModTime().UnixNano()
			}
			if !st.IsCursorUpToDate(sid, dbInfo.Size(), dbInfo.ModTime().UnixNano(), walSize, walMtime) {
				st.SyncUsageStepsFromDir(convDir, gwSids)
				dbInfo2, hasDB2, okDB2 := lstatRegular(dbPath)
				walInfo2, hasWAL2, okWAL2 := lstatRegular(walPath)
				if !okDB2 || !okWAL2 || !hasDB2 {
					continue
				}
				var wSz2, wMt2 int64
				if hasWAL2 {
					wSz2 = walInfo2.Size()
					wMt2 = walInfo2.ModTime().UnixNano()
				}
				if !st.IsCursorUpToDate(sid, dbInfo2.Size(), dbInfo2.ModTime().UnixNano(), wSz2, wMt2) {
					continue
				}
			}
		}

		// Re-verify active session membership & pbPath mtime right before deletion to close the
		// TOCTOU window where an evicted session (<7d) is lazily restored from SQLite during cleanup.
		isNowActive := false
		gs.mu.RLock()
		for _, s := range gs.sessions {
			if s == nil {
				continue
			}
			s.mu.Lock()
			if s.SessionID == sid || s.HibernatedSessionID == sid {
				isNowActive = true
			}
			s.mu.Unlock()
			if isNowActive {
				break
			}
		}
		gs.mu.RUnlock()
		if isNowActive {
			continue
		}
		if hasPB {
			if reInfo, reHasPB, reOkPB := lstatRegular(pbPath); !reOkPB || (reHasPB && !reInfo.ModTime().Before(cutoff)) {
				continue
			}
		}

		removedAny := false
		removedAllPrimary := true
		for _, target := range []struct {
			path      string
			exists    bool
			isPrimary bool
		}{
			{pbPath, hasPB, true},
			{dbPath, hasDB, true},
			{walPath, hasWAL, false},
			{shmPath, hasSHM, false},
		} {
			if !target.exists {
				continue
			}
			if remErr := os.Remove(target.path); remErr == nil || os.IsNotExist(remErr) {
				removedAny = true
			} else if target.isPrimary {
				removedAllPrimary = false
			}
		}
		if hasDB {
			GlobalDBCache.Delete(dbPath)
		}
		if removedAny && removedAllPrimary {
			if brainDir != "" {
				brainPath := filepath.Join(brainDir, sid)
				if !isWindowsCDrivePath(brainPath) {
					if bInfo, errB := os.Lstat(brainPath); errB == nil && bInfo.Mode()&os.ModeSymlink == 0 && bInfo.IsDir() {
						if errR := os.RemoveAll(brainPath); errR == nil {
							cleanedBrains++
						}
					}
				}
			}
			st.ClearCleanedConversationSid(sid)
			cleaned++
		}
	}
	if cleaned > 0 {
		LogEvent("CONV_CLEANUP", fmt.Sprintf("Cleaned %d expired gateway conversation bundle(s) (> %dd)", cleaned, ConvKeepDays), map[string]any{
			"cleaned":       cleaned,
			"cleanedBrains": cleanedBrains,
			"keepDays":      ConvKeepDays,
		})
	}
	return cleaned
}

// Remove only the mutable history suffix; never infer identity from content.
func explicitConversationIdentity(key string) string {
	if !strings.HasPrefix(key, "sid:") {
		return ""
	}
	if i := strings.LastIndex(key, ":u:"); i > 4 {
		return key[:i]
	}
	return ""
}

// stripSlotSuffix removes the "#N" concurrency-slot suffix that getOrCreateAvailableSession adds
// when the base slot is busy/parked. Slot sessions belong to the same conversation as the base key;
// without this their parked tool results never match, so every tool_use turn on a slot session
// spawns yet another cold-start slot and leaves the previous one orphaned (hot until ParkedTtl).
func stripSlotSuffix(key string) string {
	if i := strings.LastIndex(key, "#"); i > 0 {
		return key[:i]
	}
	return key
}

func sameConversationIdentity(a, b string) bool {
	a, b = stripSlotSuffix(a), stripSlotSuffix(b)
	if a == b {
		return true
	}
	id := explicitConversationIdentity(a)
	return id != "" && id == explicitConversationIdentity(b)
}

func extractFpAgentScope(key string) string {
	k := stripSlotSuffix(key)
	if !strings.HasPrefix(k, "fp:") {
		return "\x00"
	}
	if i := strings.Index(k, ":ag:"); i > 3 {
		rest := k[i+4:]
		if j := strings.LastIndex(rest, ":u:"); j >= 0 {
			return rest[:j]
		}
		return rest
	}
	return ""
}

func isCryptographicAcpToolCallID(id string) bool {
	if len(id) != 36 || !strings.HasPrefix(id, "acp_") {
		return false
	}
	for i := 4; i < len(id); i++ {
		c := id[i]
		isHex := (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
		if !isHex {
			return false
		}
	}
	return true
}

func (gs *GatewayServer) attachRestoredSessionLocked(restored *AcpProjectSession) *AcpProjectSession {
	if restored == nil {
		return nil
	}
	if existing, ok := gs.sessions[restored.ConvKey]; ok {
		return existing
	}
	targetSid := strings.ToLower(restored.HibernatedSessionID)
	if targetSid != "" {
		for _, s := range gs.sessions {
			if s == nil {
				continue
			}
			s.mu.Lock()
			dup := strings.EqualFold(s.SessionID, targetSid) || strings.EqualFold(s.HibernatedSessionID, targetSid)
			s.mu.Unlock()
			if dup {
				return nil
			}
		}
	}
	restored.mu.Lock()
	restored.LastUsedAt = time.Now()
	restored.mu.Unlock()
	gs.bindSessionDeps(restored)
	gs.sessions[restored.ConvKey] = restored
	gs.tenants[restored.TenantKey] = restored
	if restored.HibernatedSessionID != "" {
		RegisterGatewaySessionID(restored.HibernatedSessionID)
	}
	return restored
}

func markSessionClaimedIfRequested(s *AcpProjectSession, claim bool) *AcpProjectSession {
	if s != nil && claim {
		s.mu.Lock()
		s.claimTurnLocked()
		s.mu.Unlock()
	}
	return s
}

func (gs *GatewayServer) getOrCreateAvailableSession(baseKey, cwd, sysText string, historyFp ...string) *AcpProjectSession {
	return gs.getOrCreateSessionWithMessages(baseKey, cwd, sysText, false, false, nil, historyFp...)
}

func (gs *GatewayServer) getOrCreateClaimedSession(baseKey, cwd, sysText string, historyFp ...string) *AcpProjectSession {
	return gs.getOrCreateSessionWithMessages(baseKey, cwd, sysText, true, false, nil, historyFp...)
}

func (gs *GatewayServer) getOrCreateClaimedSessionForMessages(baseKey, cwd, sysText, firstUserFp string, messages []map[string]any) *AcpProjectSession {
	return gs.getOrCreateSessionWithMessages(baseKey, cwd, sysText, true, false, messages, firstUserFp)
}

func (gs *GatewayServer) getOrCreateClaimedSessionForMessagesWithCompaction(baseKey, cwd, sysText, firstUserFp string, messages []map[string]any, isOneShotCompaction bool) *AcpProjectSession {
	return gs.getOrCreateSessionWithMessages(baseKey, cwd, sysText, true, isOneShotCompaction, messages, firstUserFp)
}

func (gs *GatewayServer) getOrCreateSessionInternal(baseKey, cwd, sysText string, claim bool, historyFp ...string) *AcpProjectSession {
	return gs.getOrCreateSessionWithMessages(baseKey, cwd, sysText, claim, false, nil, historyFp...)
}

func (gs *GatewayServer) getOrCreateSessionWithMessages(baseKey, cwd, sysText string, claim, isOneShotCompaction bool, messages []map[string]any, historyFp ...string) *AcpProjectSession {
	gs.mu.RLock()
	needBaseRestore := gs.sessions[baseKey] == nil
	gs.mu.RUnlock()

	var preRestoredBase *AcpProjectSession
	queriedBase := false
	if needBaseRestore {
		if stateSt, ok := gs.stateStore(); ok {
			preRestoredBase = stateSt.RestoreSingleSession(baseKey)
		}
		queriedBase = true
	}

	locked := false
	lock := func() {
		if !locked {
			gs.mu.Lock()
			locked = true
		}
	}
	unlock := func() {
		if locked {
			locked = false
			gs.mu.Unlock()
		}
	}
	defer unlock()

	lock()
	if preRestoredBase != nil && gs.sessions[baseKey] == nil {
		gs.attachRestoredSessionLocked(preRestoredBase)
	}

	// Parked compaction or mid-conversation system prompt drift updates HistoryFingerprint/SystemText, not ConvKey.
	// Only a unique FULL fingerprint within the same explicit identity can reuse that canonical session.
	if gs.sessions[baseKey] == nil && explicitConversationIdentity(baseKey) != "" && len(historyFp) > 0 && historyFp[0] != "" {
		fp0 := historyFp[0]
		turn1SingleFp := ""
		if len(messages) >= 2 && messages[1]["role"] == "user" {
			turn1SingleFp = computeFirstUserFingerprint(messages[:1])
		}
		findUniqueFpCandidateLocked := func() (*AcpProjectSession, bool) {
			var cand *AcpProjectSession
			for key, s := range gs.sessions {
				if !sameConversationIdentity(key, baseKey) {
					continue
				}
				s.mu.Lock()
				matchesTurn1 := turn1SingleFp != "" &&
					s.MessageCount == 2 && s.lastTurnAssistant &&
					(len(s.ParkedCalls) > 0 || len(s.HibernatedParkedCallIDs) > 0) &&
					s.HistoryFingerprint == turn1SingleFp
				matches := (s.HistoryFingerprint == fp0 || matchesTurn1) && s.MessageCount > 0 && s.Cwd == cwd
				s.mu.Unlock()
				if matches {
					if cand != nil {
						return nil, true
					}
					cand = s
				}
			}
			return cand, false
		}

		candidate, ambiguous := findUniqueFpCandidateLocked()
		if candidate == nil && !ambiguous {
			ident := explicitConversationIdentity(baseKey)
			unlock()
			var restored *AcpProjectSession
			if stateSt, ok := gs.stateStore(); ok {
				restored = stateSt.RestoreSessionByIdentityFingerprint(ident, cwd, sysText, fp0)
			}
			lock()
			if gs.sessions[baseKey] == nil {
				candidate, ambiguous = findUniqueFpCandidateLocked()
				if candidate == nil && !ambiguous && restored != nil {
					candidate = gs.attachRestoredSessionLocked(restored)
				}
			}
		}
		if candidate != nil && !ambiguous && candidate.IsAvailableForNewTurn() {
			LogEvent("SESSION_CONTINUITY", "Reused same-identity session after history key changed", map[string]any{"reason": "full_history_fingerprint_match"})
			return markSessionClaimedIfRequested(candidate, claim)
		}
		// Post-compaction continuation for explicit sid: clients (dual-locked by
		// isPostCompactionContinuation + recent rollover_compaction marker).
		if candidate == nil && !ambiguous && !isOneShotCompaction && isPostCompactionContinuation(messages) {
			now := time.Now()
			var compCand *AcpProjectSession
			compAmbiguous := false
			for key, s := range gs.sessions {
				if s == nil || !sameConversationIdentity(key, baseKey) {
					continue
				}
				s.mu.Lock()
				matchesComp := s.Cwd == cwd &&
					s.ForceRolloverNext &&
					s.ForceRolloverReason == "rollover_compaction" &&
					!s.lastCompactedAt.IsZero() &&
					now.Sub(s.lastCompactedAt) < 15*time.Minute
				s.mu.Unlock()
				if matchesComp {
					if compCand != nil {
						compAmbiguous = true
						break
					}
					compCand = s
				}
			}
			if compCand != nil && !compAmbiguous {
				compCand.mu.Lock()
				avail := compCand.isAvailableForNewTurnLocked()
				if avail && claim {
					compCand.claimTurnLocked()
				}
				compCand.mu.Unlock()
				if avail {
					LogEvent("SESSION_CONTINUITY", "Reused canonical sid session after compaction", map[string]any{
						"reason":  "post_compaction_sid_reuse",
						"convKey": compCand.ConvKey,
					})
					return compCand
				}
			}
		}
	}
	// Headerless ("fp:") clients hash the first 4KB of system prompt into ConvKey; if dynamic dates,
	// git status, or editor state mutate system on Turn 2+ (len(messages) >= 3), match the unique
	// headerless session sharing (agentScope, Cwd, HistoryFingerprint) and consistent message history.
	if gs.sessions[baseKey] == nil && strings.HasPrefix(baseKey, "fp:") && len(messages) >= 3 && len(historyFp) > 0 && historyFp[0] != "" {
		fp0 := historyFp[0]
		fpScope := extractFpAgentScope(baseKey)
		findUniqueHeaderlessCandidateLocked := func() (*AcpProjectSession, bool) {
			var cand *AcpProjectSession
			for key, s := range gs.sessions {
				if s == nil || !strings.HasPrefix(key, "fp:") || extractFpAgentScope(key) != fpScope {
					continue
				}
				s.mu.Lock()
				msgCnt := s.MessageCount
				lastAsst := s.lastTurnAssistant
				cancFp := s.cancelledHistoryFp
				matches := s.HistoryFingerprint == fp0 && msgCnt > 0 && len(messages) >= msgCnt && s.Cwd == cwd
				s.mu.Unlock()
				if !matches {
					continue
				}
				if lastAsst && messages[msgCnt-1]["role"] != "assistant" {
					continue
				}
				if cancelledHistoryDiverged(messages, msgCnt, lastAsst, cancFp) {
					continue
				}
				if cand != nil {
					return nil, true
				}
				cand = s
			}
			return cand, false
		}

		candidate, ambiguous := findUniqueHeaderlessCandidateLocked()
		if candidate == nil && !ambiguous {
			unlock()
			var restored *AcpProjectSession
			if stateSt, ok := gs.stateStore(); ok {
				restored = stateSt.RestoreFpSessionByHistoryFingerprint(fpScope, cwd, fp0, len(messages))
			}
			lock()
			if gs.sessions[baseKey] == nil {
				candidate, ambiguous = findUniqueHeaderlessCandidateLocked()
				if candidate == nil && !ambiguous && restored != nil {
					restored.mu.Lock()
					rCnt := restored.MessageCount
					rLastAsst := restored.lastTurnAssistant
					rCancFp := restored.cancelledHistoryFp
					restored.mu.Unlock()
					if rCnt > 0 && len(messages) >= rCnt && (!rLastAsst || messages[rCnt-1]["role"] == "assistant") && !cancelledHistoryDiverged(messages, rCnt, rLastAsst, rCancFp) {
						candidate = gs.attachRestoredSessionLocked(restored)
					}
				}
			}
		}
		if candidate != nil && !ambiguous && candidate.IsAvailableForNewTurn() {
			LogEvent("SESSION_CONTINUITY", "Reused headerless session after system prompt drift", map[string]any{"reason": "fp_history_continuity"})
			return markSessionClaimedIfRequested(candidate, claim)
		}
	}

	for slot := 0; slot < 8; slot++ {
		slotKey := baseKey
		if slot > 0 {
			slotKey = fmt.Sprintf("%s#%d", baseKey, slot+1)
		}
		existing, ok := gs.sessions[slotKey]
		if !ok && (slot > 0 || !queriedBase) {
			if slot == 0 {
				queriedBase = true
			}
			unlock()
			var restored *AcpProjectSession
			if stateSt, ok := gs.stateStore(); ok {
				restored = stateSt.RestoreSingleSession(slotKey)
			}
			lock()
			existing, ok = gs.sessions[slotKey]
			if !ok && restored != nil {
				if attached := gs.attachRestoredSessionLocked(restored); attached != nil {
					existing, ok = attached, true
				}
			}
		}
		if !ok {
			created := NewAcpProjectSession(slotKey, cwd, sysText)
			gs.bindSessionDeps(created)
			gs.sessions[slotKey] = created
			gs.tenants[created.TenantKey] = created
			return markSessionClaimedIfRequested(created, claim)
		}
		if existing.IsAvailableForNewTurn() {
			return markSessionClaimedIfRequested(existing, claim)
		}
	}
	fallbackKey := fmt.Sprintf("%s#%s", baseKey, randomHex(2))
	fallback := NewAcpProjectSession(fallbackKey, cwd, sysText)
	gs.bindSessionDeps(fallback)
	gs.sessions[fallbackKey] = fallback
	gs.tenants[fallback.TenantKey] = fallback
	return markSessionClaimedIfRequested(fallback, claim)
}

func (gs *GatewayServer) findSessionWithParkedToolResult(baseKey string, messages []map[string]any) *AcpProjectSession {
	if len(messages) == 0 {
		return nil
	}
	trs := extractToolResultsFromMessages(messages)
	if len(trs) == 0 {
		return nil
	}
	basePrefix := stripSlotSuffix(baseKey)

	waitAndRecheckClosing := func(closingCand *AcpProjectSession, requireCrypto bool) *AcpProjectSession {
		if closingCand == nil {
			return nil
		}
		closingCand.waitIfClosing(10 * time.Second)
		closingCand.mu.Lock()
		matched := false
		if closingCand.canResumeHibernatedParkedLocked() {
			for _, tr := range trs {
				if requireCrypto && !isCryptographicAcpToolCallID(tr.ToolCallID) {
					continue
				}
				if closingCand.HibernatedParkedCallIDs[tr.ToolCallID] {
					matched = true
					break
				}
			}
		}
		if matched {
			closingCand.claimTurnLocked()
		}
		closingCand.mu.Unlock()
		if !matched {
			return nil
		}
		return closingCand
	}

	gs.mu.Lock()
	var candidate *AcpProjectSession
	for key, s := range gs.sessions {
		if s == nil || (stripSlotSuffix(key) != basePrefix && !sameConversationIdentity(key, baseKey)) {
			continue
		}
		s.mu.Lock()
		matched := false
		if !s.closing {
			for _, tr := range trs {
				if _, ok := s.ParkedCalls[tr.ToolCallID]; ok {
					matched = true
					break
				}
			}
		}
		s.mu.Unlock()
		if matched {
			if candidate != nil {
				gs.mu.Unlock()
				return nil
			}
			candidate = s
		}
	}
	if candidate != nil {
		claimed := markSessionClaimedIfRequested(candidate, true)
		gs.mu.Unlock()
		return claimed
	}

	// Second pass: match parked tool IDs preserved when a slot session hibernated with keepResumeState
	// (or is currently in the CloseSession closing window with HibernatedParkedCallIDs already recorded).
	var closingCand *AcpProjectSession
	closingAmbiguous := false
	for key, s := range gs.sessions {
		if s == nil || (stripSlotSuffix(key) != basePrefix && !sameConversationIdentity(key, baseKey)) {
			continue
		}
		s.mu.Lock()
		matched := false
		matchedClosing := false
		if s.canResumeHibernatedParkedLocked() {
			for _, tr := range trs {
				if s.HibernatedParkedCallIDs[tr.ToolCallID] {
					matched = true
					break
				}
			}
		} else if s.closing && len(s.HibernatedParkedCallIDs) > 0 {
			for _, tr := range trs {
				if s.HibernatedParkedCallIDs[tr.ToolCallID] {
					matchedClosing = true
					break
				}
			}
		}
		s.mu.Unlock()
		if matched {
			if candidate != nil {
				gs.mu.Unlock()
				return nil
			}
			candidate = s
		} else if matchedClosing {
			if closingCand != nil {
				closingAmbiguous = true
			} else {
				closingCand = s
			}
		}
	}
	if candidate != nil {
		claimed := markSessionClaimedIfRequested(candidate, true)
		gs.mu.Unlock()
		return claimed
	}
	if closingCand != nil && !closingAmbiguous {
		gs.mu.Unlock()
		if resumed := waitAndRecheckClosing(closingCand, false); resumed != nil {
			return resumed
		}
		gs.mu.Lock()
	}

	// Third/fourth pass for headerless ("fp:") sessions whose system prompt drifted while a
	// cryptographic gateway-issued "acp_<32hex>" tool_use ID was parked on the client.
	if strings.HasPrefix(baseKey, "fp:") {
		hasCryptoID := false
		for _, tr := range trs {
			if isCryptographicAcpToolCallID(tr.ToolCallID) {
				hasCryptoID = true
				break
			}
		}
		if hasCryptoID {
			fpScope := extractFpAgentScope(baseKey)
			for key, s := range gs.sessions {
				if s == nil || !strings.HasPrefix(key, "fp:") || extractFpAgentScope(key) != fpScope {
					continue
				}
				s.mu.Lock()
				matched := false
				if !s.closing {
					for _, tr := range trs {
						if isCryptographicAcpToolCallID(tr.ToolCallID) {
							if _, ok := s.ParkedCalls[tr.ToolCallID]; ok {
								matched = true
								break
							}
						}
					}
				}
				s.mu.Unlock()
				if matched {
					if candidate != nil {
						gs.mu.Unlock()
						return nil
					}
					candidate = s
				}
			}
			if candidate != nil {
				claimed := markSessionClaimedIfRequested(candidate, true)
				gs.mu.Unlock()
				return claimed
			}
			closingCand = nil
			closingAmbiguous = false
			for key, s := range gs.sessions {
				if s == nil || !strings.HasPrefix(key, "fp:") || extractFpAgentScope(key) != fpScope {
					continue
				}
				s.mu.Lock()
				matched := false
				matchedClosing := false
				if s.canResumeHibernatedParkedLocked() {
					for _, tr := range trs {
						if isCryptographicAcpToolCallID(tr.ToolCallID) && s.HibernatedParkedCallIDs[tr.ToolCallID] {
							matched = true
							break
						}
					}
				} else if s.closing && len(s.HibernatedParkedCallIDs) > 0 {
					for _, tr := range trs {
						if isCryptographicAcpToolCallID(tr.ToolCallID) && s.HibernatedParkedCallIDs[tr.ToolCallID] {
							matchedClosing = true
							break
						}
					}
				}
				s.mu.Unlock()
				if matched {
					if candidate != nil {
						gs.mu.Unlock()
						return nil
					}
					candidate = s
				} else if matchedClosing {
					if closingCand != nil {
						closingAmbiguous = true
					} else {
						closingCand = s
					}
				}
			}
			if candidate != nil {
				claimed := markSessionClaimedIfRequested(candidate, true)
				gs.mu.Unlock()
				return claimed
			}
			if closingCand != nil && !closingAmbiguous {
				gs.mu.Unlock()
				return waitAndRecheckClosing(closingCand, true)
			}
		}
	}
	claimed := markSessionClaimedIfRequested(candidate, true)
	gs.mu.Unlock()
	return claimed
}

// findAbandonedParkedSessionForExplicitIdentity reclaims a unique idle-parked session for an
// explicit-identity client ("sid:...") when the client aborted its pending tool_use and sent a
// new user turn without matching tool_result, preventing orphaned relay burn and slot sharding.
// Requires matching Cwd and first-user HistoryFingerprint (unless compacted) so parallel subagents
// sharing a parent x-claude-code-session-id never hijack or kill the parent's parked tool session,
// while dirty git-status / timestamp drift in SystemText still reclaims cleanly.
func (gs *GatewayServer) findAbandonedParkedSessionForExplicitIdentity(baseKey, cwd, sysText, firstUserFp, turn1SingleFp string, isExplicitCompacted bool) *AcpProjectSession {
	if explicitConversationIdentity(baseKey) == "" {
		return nil
	}
	gs.mu.Lock()
	defer gs.mu.Unlock()
	var parkedCand *AcpProjectSession
	for key, s := range gs.sessions {
		if s == nil || !sameConversationIdentity(key, baseKey) {
			continue
		}
		s.mu.Lock()
		turn1Match := turn1SingleFp != "" && s.MessageCount == 2 && s.lastTurnAssistant && len(s.ParkedCalls) > 0 && s.HistoryFingerprint == turn1SingleFp
		sameScope := s.Cwd == cwd && (isExplicitCompacted ||
			(firstUserFp != "" && s.HistoryFingerprint == firstUserFp) ||
			turn1Match ||
			(s.MessageCount <= 2 && (stripSlotSuffix(key) == stripSlotSuffix(baseKey) || s.SystemText == sysText || s.InitialSystemText == sysText)))
		busy := s.isActivelyWorkingLocked()
		hasParked := len(s.ParkedCalls) > 0
		s.mu.Unlock()
		if !sameScope {
			continue
		}
		if busy {
			return nil
		}
		if hasParked {
			if parkedCand != nil {
				return nil
			}
			parkedCand = s
		}
	}
	return markSessionClaimedIfRequested(parkedCand, true)
}

func isKeepalivePingRequest(messages []map[string]any) bool {
	if len(messages) == 0 {
		return false
	}
	lastMsg := messages[len(messages)-1]
	role, _ := lastMsg["role"].(string)
	if role != "user" || messagesContainToolBlocks(messages[len(messages)-1:]) {
		return false
	}
	txt := strings.TrimSpace(formatSingleMessageText(lastMsg))
	return len(txt) <= 200 && strings.HasPrefix(txt, "Automated prompt-cache keep-alive ping")
}

func (gs *GatewayServer) findSessionWithDetachedTurn(baseKey, cwd, sysText, toolFp, reqFp, firstUserFp string, msgCount int) (*AcpProjectSession, bool) {
	gs.mu.RLock()
	defer gs.mu.RUnlock()

	baseStripped := stripSlotSuffix(baseKey)
	var supersedeCand *AcpProjectSession
	for key, s := range gs.sessions {
		if s == nil || (!sameConversationIdentity(key, baseKey) && !(strings.HasPrefix(baseKey, "fp:") && strings.HasPrefix(key, "fp:") && extractFpAgentScope(key) == extractFpAgentScope(baseKey) && reqFp != "")) {
			continue
		}
		s.mu.Lock()
		det := s.detachedTurn
		matchesExact := det != nil && reqFp != "" && det.reqFp == reqFp && s.Cwd == cwd
		sameEntryKey := stripSlotSuffix(key) == baseStripped
		sameHist := firstUserFp != "" && s.HistoryFingerprint != "" && firstUserFp == s.HistoryFingerprint
		sameTurn1Harness := det != nil && s.MessageCount == 0 && det.msgCount <= 1 &&
			s.Cwd == cwd && (s.SystemText == sysText || s.InitialSystemText == sysText) &&
			(s.ToolFingerprint == "" || s.ToolFingerprint == toolFp)
		canSupersede := det != nil && sameConversationIdentity(key, baseKey) && (sameEntryKey || sameHist || sameTurn1Harness)
		s.mu.Unlock()
		if matchesExact {
			return s, true
		}
		if canSupersede && supersedeCand == nil {
			supersedeCand = s
		}
	}
	if supersedeCand != nil {
		return supersedeCand, false
	}
	return nil, false
}

func (gs *GatewayServer) findSessionWithActiveTurn(baseKey, cwd, reqFp string) (*AcpProjectSession, context.CancelFunc) {
	if reqFp == "" {
		return nil, nil
	}
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	baseStripped := stripSlotSuffix(baseKey)
	for key, s := range gs.sessions {
		if s == nil {
			continue
		}
		sameScope := stripSlotSuffix(key) == baseStripped || sameConversationIdentity(key, baseKey) ||
			(strings.HasPrefix(baseKey, "fp:") && strings.HasPrefix(key, "fp:") && extractFpAgentScope(key) == extractFpAgentScope(baseKey))
		if !sameScope {
			continue
		}
		s.mu.Lock()
		canCoalesce := s.Cwd == cwd && s.detachedTurn == nil && s.activeTurn != nil &&
			s.activeTurn.CanSoftDetach() && s.activeTurnReqFp == reqFp && s.activeReqCancel != nil && !s.closing
		var cancelFn context.CancelFunc
		if canCoalesce {
			s.coalesceHandoff = true
			s.claimTurnLocked()
			cancelFn = s.activeReqCancel
		}
		s.mu.Unlock()
		if canCoalesce && cancelFn != nil {
			return s, cancelFn
		}
	}
	return nil, nil
}

func setSessionPendingSystemLocked(s *AcpProjectSession, rawSys, sysText string) {
	if sysText != s.SystemText {
		s.pendingSystemText = sysText
		s.pendingRawSystemText = rawSys
		s.hasPendingSystem = true
		return
	}
	s.pendingSystemText = ""
	s.pendingRawSystemText = ""
	s.hasPendingSystem = false
	if s.RawSystemText == "" && rawSys != "" {
		s.RawSystemText = rawSys
	}
}

func maybeHibernateStandaloneCompaction(sess *AcpProjectSession, res TurnResult, err error) {
	finishOneShotCompaction(sess, res, err, false, true, nil)
}

func finishOneShotCompaction(sess *AcpProjectSession, res TurnResult, err error, handedOff, isStandalone bool, targetRef *compactionTargetRef) {
	if handedOff {
		return
	}
	clean := sess != nil && err == nil && !isSoftDetachedErr(err) && !sess.HasDetachedTurn() &&
		len(res.ToolCalls) == 0 && res.FailedNotice == "" && DetectAntigravitySoftError(res.Text) == nil &&
		res.SessionID != ""
	if !clean {
		if !isSoftDetachedErr(err) {
			targetRef.release()
		}
		return
	}
	keepInPlaceSidMarker := !isStandalone && targetRef == nil && explicitConversationIdentity(sess.ConvKey) != ""
	hibReason := "oneshot_compaction_summary"
	if keepInPlaceSidMarker {
		hibReason = "oneshot_compaction_summary_sid"
	}
	sess.HibernateIfSession(res.SessionID, hibReason)
	if targetRef != nil {
		targetRef.release()
		targetRef.sess.supersedeAsCompactionTarget(targetRef.sid, targetRef.markSidRollover)
	}
}

func (gs *GatewayServer) findPreCompactionTarget(compSess *AcpProjectSession, baseKey, cwd, firstUserFp string, isStandalone bool, clientIdent ClientIdentity, messages []map[string]any) *compactionTargetRef {
	if gs == nil || compSess == nil || len(messages) == 0 {
		return nil
	}
	type candInfo struct {
		sess      *AcpProjectSession
		convKey   string
		sid       string
		cwd       string
		historyFp string
		clientID  string
		anchors   sessionContentAnchors
	}
	gs.mu.RLock()
	rawSessions := make([]*AcpProjectSession, 0, len(gs.sessions))
	for key, s := range gs.sessions {
		if s == nil || s == compSess || strings.HasPrefix(key, "util:") {
			continue
		}
		rawSessions = append(rawSessions, s)
	}
	gs.mu.RUnlock()

	cands := make([]candInfo, 0, len(rawSessions))
	for _, s := range rawSessions {
		s.mu.Lock()
		p := s.syncPhaseLocked()
		eligible := !s.isWafBisectProbe && s.SessionID != "" && s.MessageCount >= 2 && (p == PhaseIdle || p == PhaseHotParked)
		if !eligible {
			s.mu.Unlock()
			continue
		}
		info := candInfo{
			sess:      s,
			convKey:   s.ConvKey,
			sid:       s.SessionID,
			cwd:       s.Cwd,
			historyFp: s.HistoryFingerprint,
			clientID:  s.ClientID,
			anchors:   s.contentAnchors,
		}
		s.mu.Unlock()
		cands = append(cands, info)
	}
	if len(cands) == 0 {
		return nil
	}

	// Branch 1: explicit sid: non-standalone compaction (e.g. Claude Code / Codex) ->
	// deterministic match by sameConversationIdentity + Cwd + HistoryFingerprint.
	if explicitConversationIdentity(baseKey) != "" && !isStandalone {
		if firstUserFp == "" {
			return nil
		}
		var matched *candInfo
		for i := range cands {
			c := &cands[i]
			if sameConversationIdentity(c.convKey, baseKey) && c.cwd == cwd && c.historyFp == firstUserFp {
				if matched != nil {
					return nil
				}
				matched = c
			}
		}
		if matched == nil {
			return nil
		}
		return matched.sess.addCompactionPendingLease(matched.sid, 5*time.Minute, true)
	}

	// Branch 1b: headerless fp: non-standalone compaction (e.g. headerless Claude Code / Roo / Cline
	// / OpenCode splitting onto #2 while S1 is parked) -> deterministic unique match by fpScope + Cwd + HistoryFingerprint.
	if explicitConversationIdentity(baseKey) == "" && strings.HasPrefix(baseKey, "fp:") && !isStandalone && firstUserFp != "" {
		fpScope := extractFpAgentScope(baseKey)
		var matched *candInfo
		ambiguous := false
		for i := range cands {
			c := &cands[i]
			if !strings.HasPrefix(c.convKey, "fp:") || extractFpAgentScope(c.convKey) != fpScope {
				continue
			}
			if clientIdent.Known && isKnownSpecificClientID(c.clientID) && c.clientID != clientIdent.ID {
				continue
			}
			if c.cwd == cwd && c.historyFp == firstUserFp {
				if matched != nil {
					ambiguous = true
					break
				}
				matched = c
			}
		}
		if ambiguous {
			return nil
		}
		if matched != nil {
			return matched.sess.addCompactionPendingLease(matched.sid, 5*time.Minute, false)
		}
	}

	// Branch 2 & 3: content-anchor unique matching inside the compaction corpus.
	corpus := extractCompactionSearchCorpus(messages)
	if corpus == "" {
		return nil
	}
	var filtered []candInfo
	markSid := false
	if explicitConversationIdentity(baseKey) != "" {
		// Branch 2: explicit sid: standalone compaction -> restrict to sameConversationIdentity
		// and require content-anchor match so a shared-sid parked parent is never hijacked.
		markSid = true
		for _, c := range cands {
			if sameConversationIdentity(c.convKey, baseKey) && len(c.anchors.Anchors) > 0 {
				filtered = append(filtered, c)
			}
		}
	} else if strings.HasPrefix(baseKey, "fp:") {
		// Branch 3: headerless fp: compaction (e.g. Pi) -> scope to same fp agentScope and
		// non-conflicting known specific ClientID; ignore Cwd on standalone compaction (often /tmp).
		fpScope := extractFpAgentScope(baseKey)
		for _, c := range cands {
			if !strings.HasPrefix(c.convKey, "fp:") || extractFpAgentScope(c.convKey) != fpScope {
				continue
			}
			if clientIdent.Known && isKnownSpecificClientID(c.clientID) && c.clientID != clientIdent.ID {
				continue
			}
			if !isStandalone && c.cwd != cwd {
				continue
			}
			if len(c.anchors.Anchors) > 0 {
				filtered = append(filtered, c)
			}
		}
	}
	if len(filtered) == 0 {
		return nil
	}

	// Deduplicate shared template anchors appearing across >=2 live candidates.
	anchorFreq := make(map[string]int, len(filtered)*4)
	for _, c := range filtered {
		seen := make(map[string]bool, len(c.anchors.Anchors))
		for _, a := range c.anchors.Anchors {
			if !seen[a] {
				seen[a] = true
				anchorFreq[a]++
			}
		}
	}

	var winner *candInfo
	for i := range filtered {
		c := &filtered[i]
		primaryHits := 0
		totalHits := 0
		for idx, a := range c.anchors.Anchors {
			if anchorFreq[a] >= 2 {
				continue
			}
			if strings.Contains(corpus, a) {
				totalHits++
				if idx < c.anchors.PrimaryCount {
					primaryHits++
				}
			}
		}
		if primaryHits >= 1 || totalHits >= 2 {
			if winner != nil {
				return nil // ambiguous: never risk killing a sibling session
			}
			winner = c
		}
	}
	if winner == nil {
		return nil
	}
	return winner.sess.addCompactionPendingLease(winner.sid, 5*time.Minute, markSid)
}

func (gs *GatewayServer) HandleMessagesRequest(r *http.Request, body map[string]any, messages []map[string]any, onDelta func(DeltaChunk)) (outRes TurnResult, outErr error) {
	ctx := context.Background()
	if r != nil {
		ctx = r.Context()
	}
	var finishDeltaGuard func()
	ctx, onDelta, finishDeltaGuard = guardDeltaCallback(ctx, onDelta)
	defer finishDeltaGuard()
	rawSys := rawSystemText(body["system"])
	sysText := PruneClientSystemPrompt(rawSys)
	cwd := extractCwdFromSystem(sysText, messages)
	toolsCatalog := sanitizeToolsCatalog(body["tools"])
	toolFp, toolDigests := toolCatalogFingerprintAndDigests(toolsCatalog)
	isStandaloneCompaction := isStandaloneCompactionSummary(sysText, toolsCatalog, messages)
	ctx = withStandaloneCompaction(ctx, isStandaloneCompaction)
	modelStr, _ := body["model"].(string)
	sel := ResolveRequestedModel(modelStr, body)
	if isSessionNamingUtility(sysText, toolsCatalog, messages) && !sel.ExplicitTier && (sel.Model == nil || sel.Model.FixedTier == "") {
		sel.Tier = "low"
	}
	thoughtLevel := sel.Tier
	modelID := modelStr
	if sel.Model != nil && sel.Model.ID != "" {
		modelID = sel.Model.ID
	}
	defer func() {
		if meta, ok := routedAccountMetaFromRequest(r); ok && !meta.skipHealth.Load() {
			checkErr := outErr
			if checkErr == nil {
				if softErr := DetectAntigravitySoftError(outRes.Text); softErr != nil {
					checkErr = softErr
				}
			}
			meta.outcomeRecorded.Store(true)
			if checkErr != nil {
				sc, eType, rAfter, eMsg := classifyUpstreamError(checkErr)
				hdrVal := ""
				if rAfter {
					hdrVal = extractRetryAfterHeaderValue(eMsg)
				}
				GlobalAccountManager.RecordAccountOutcome(meta.accountID, modelID, sc, eType, hdrVal, eMsg)
			} else {
				GlobalAccountManager.RecordAccountOutcome(meta.accountID, modelID, http.StatusOK, "", "", "")
			}
		}
	}()
	if mediaAttachments := collectCanonicalMediaRelayAttachments(messages); len(mediaAttachments) > 0 {
		return gs.RunMediaRelayTurn(ctx, r, body, messages, sysText, sel, mediaAttachments, onDelta)
	}
	compactionHdr := ""
	compactedHdr := ""
	reqClass := ""
	if r != nil {
		compactionHdr = strings.ToLower(strings.TrimSpace(r.Header.Get("x-claude-code-compaction")))
		compactedHdr = strings.ToLower(strings.TrimSpace(r.Header.Get("x-claude-code-context-compacted")))
		reqClass = strings.ToLower(strings.TrimSpace(r.Header.Get("x-claude-code-request-class")))
	}
	isExplicitCompaction := compactionHdr == "1" || compactionHdr == "true" || reqClass == "compaction"
	isExplicitCompacted := compactedHdr == "1" || compactedHdr == "true"
	isInConvCompaction := isCompactionSummaryRequestWithSystem(sysText, messages)
	isOneShotCompaction := isStandaloneCompaction || isExplicitCompaction || isInConvCompaction

	firstUserFp := computeFirstUserFingerprint(messages)
	baseKey := deriveConversationKeyWithFirstFp(r, body, sysText, toolsCatalog, messages, firstUserFp)
	clientIdent := DetectClientIdentity(r, body, baseKey, rawSys, sysText, body["tools"])
	turnDir := extractTurnPromptDirective(body)
	ctx = withTurnDirective(ctx, turnDir)
	ctx = withFirstUserFingerprint(ctx, firstUserFp)
	ctx = withToolCatalogFp(ctx, toolFp)
	reqFp := computeTurnRequestFingerprintPrecomputed(modelID, thoughtLevel, cwd, toolDigests, messages, turnDir.FingerprintToken())
	ctx = withRequestFingerprint(ctx, reqFp)

	if detSess, matched := gs.findSessionWithDetachedTurn(baseKey, cwd, sysText, toolFp, reqFp, firstUserFp, len(messages)); detSess != nil {
		if err := detSess.acquireTurnQueue(ctx); err != nil {
			return TurnResult{}, err
		}
		if matched {
			detSess.mu.Lock()
			detSess.setClientIdentityLocked(clientIdent)
			detSess.mu.Unlock()
			res, compRef, joined, err := detSess.JoinDetachedTurnWithCompaction(ctx, reqFp, messages, onDelta)
			detSess.releaseTurnQueue()
			if joined {
				if isOneShotCompaction {
					finishOneShotCompaction(detSess, res, err, false, isStandaloneCompaction, compRef)
				} else {
					compRef.release()
				}
				return res, err
			}
			compRef.release()
		} else {
			detSess.AbortDetachedTurn("superseded_by_new_request")
			detSess.releaseTurnQueue()
		}
	}

	if actSess, cancelPrev := gs.findSessionWithActiveTurn(baseKey, cwd, reqFp); actSess != nil {
		actClaimReleased := false
		releaseActClaim := func() {
			if !actClaimReleased {
				actClaimReleased = true
				actSess.releaseTurnClaim()
			}
		}
		defer releaseActClaim()
		cancelPrev()
		if err := actSess.acquireTurnQueue(ctx); err != nil {
			var orphanRes TurnResult
			var orphanRef *compactionTargetRef
			orphanCompleted := false
			actSess.mu.Lock()
			if !actClaimReleased {
				actClaimReleased = true
				actSess.releaseTurnClaimLocked()
			}
			if actSess.turnClaimCount == 0 {
				actSess.coalesceHandoff = false
				if actSess.lastCompletedReqFp == reqFp {
					orphanRes = actSess.lastCompletedResult
					orphanRef = actSess.lastCompletedCompactionTarget
					orphanCompleted = true
					actSess.lastCompletedReqFp = ""
					actSess.lastCompletedResult = TurnResult{}
					actSess.lastCompletedAt = time.Time{}
					actSess.lastCompletedCompactionTarget = nil
				}
			} else if actSess.turnClaimCount == 1 && actSess.activeTurnReqFp == reqFp {
				// Only Request 1 remains; clear coalesceHandoff so Request 1 finishes cleanup itself.
				actSess.coalesceHandoff = false
			}
			actSess.mu.Unlock()
			if orphanCompleted {
				if isOneShotCompaction {
					finishOneShotCompaction(actSess, orphanRes, nil, false, isStandaloneCompaction, orphanRef)
				} else {
					orphanRef.release()
				}
			}
			return TurnResult{}, err
		}
		actSess.mu.Lock()
		actSess.setClientIdentityLocked(clientIdent)
		actSess.mu.Unlock()
		res, compRef, joined, err := actSess.JoinDetachedTurnWithCompaction(ctx, reqFp, messages, onDelta)
		if joined {
			releaseActClaim()
			actSess.releaseTurnQueue()
			if isOneShotCompaction {
				finishOneShotCompaction(actSess, res, err, false, isStandaloneCompaction, compRef)
			} else {
				compRef.release()
			}
			return res, err
		}
		compRef.release()
		if repRes, repCompRef, okRep := actSess.tryReplayRecentCompletedTurnWithCompaction(reqFp, onDelta); okRep {
			releaseActClaim()
			actSess.releaseTurnQueue()
			if isOneShotCompaction {
				finishOneShotCompaction(actSess, repRes, nil, false, isStandaloneCompaction, repCompRef)
			} else {
				repCompRef.release()
			}
			return repRes, nil
		}
		releaseActClaim()
		actSess.releaseTurnQueue()
	}

	runMatchedParkedSession := func(parkedSess *AcpProjectSession) (TurnResult, error) {
		parkedClaimReleased := false
		releaseParkedClaim := func() {
			if !parkedClaimReleased {
				parkedClaimReleased = true
				parkedSess.releaseTurnClaim()
			}
		}
		defer releaseParkedClaim()
		if err := parkedSess.acquireTurnQueue(ctx); err != nil {
			return TurnResult{}, err
		}
		parkedQueueReleased := false
		releaseParkedQueue := func() {
			if !parkedQueueReleased {
				parkedQueueReleased = true
				parkedSess.releaseTurnQueue()
			}
		}
		defer releaseParkedQueue()
		turnCtx, cancelActive := context.WithCancel(ctx)
		defer cancelActive()
		var staleParkedCompRef *compactionTargetRef
		parkedSess.mu.Lock()
		parkedSess.setClientIdentityLocked(clientIdent)
		if isExplicitCompacted {
			parkedSess.ForceRolloverNext = true
		}
		parkedSess.explicitCompactionReq = isExplicitCompaction || isInConvCompaction
		if parkedSess.lastCompletedReqFp != "" {
			staleParkedCompRef = parkedSess.lastCompletedCompactionTarget
			parkedSess.lastCompletedReqFp = ""
			parkedSess.lastCompletedResult = TurnResult{}
			parkedSess.lastCompletedAt = time.Time{}
			parkedSess.lastCompletedCompactionTarget = nil
		}
		setSessionPendingSystemLocked(parkedSess, rawSys, sysText)
		parkedSess.activeTurnReqFp = reqFp
		parkedSess.activeReqCancel = cancelActive
		parkedSess.coalesceHandoff = false
		parkedSess.mu.Unlock()
		staleParkedCompRef.release()

		res, err := parkedSess.RunTurn(turnCtx, messages, toolsCatalog, thoughtLevel, onDelta, &sel)
		var leftoverParkedRef *compactionTargetRef
		var displacedParkedRef *compactionTargetRef
		parkedSess.mu.Lock()
		handedOff := parkedSess.coalesceHandoff || parkedSess.turnClaimCount > 1
		if handedOff && err == nil && parkedSess.lastCompletedReqFp != reqFp && reqFp != "" &&
			len(res.ToolCalls) == 0 && res.FailedNotice == "" && DetectAntigravitySoftError(res.Text) == nil {
			parkedSess.lastCompletedReqFp = reqFp
			parkedSess.lastCompletedResult = res
			parkedSess.lastCompletedAt = time.Now()
			if parkedSess.activeCompactionTarget != nil {
				displacedParkedRef = parkedSess.lastCompletedCompactionTarget
				parkedSess.lastCompletedCompactionTarget = parkedSess.activeCompactionTarget
				parkedSess.activeCompactionTarget = nil
			}
		}
		handoffActive := handedOff && (isSoftDetachedErr(err) || (err == nil && parkedSess.lastCompletedReqFp == reqFp))
		if !handoffActive {
			leftoverParkedRef = parkedSess.activeCompactionTarget
			parkedSess.activeCompactionTarget = nil
			if parkedSess.lastCompletedReqFp == reqFp && reqFp != "" {
				if leftoverParkedRef == nil {
					leftoverParkedRef = parkedSess.lastCompletedCompactionTarget
				} else if parkedSess.lastCompletedCompactionTarget != nil {
					displacedParkedRef = parkedSess.lastCompletedCompactionTarget
				}
				parkedSess.lastCompletedReqFp = ""
				parkedSess.lastCompletedResult = TurnResult{}
				parkedSess.lastCompletedAt = time.Time{}
				parkedSess.lastCompletedCompactionTarget = nil
			}
		}
		parkedSess.activeTurnReqFp = ""
		parkedSess.activeReqCancel = nil
		parkedSess.coalesceHandoff = false
		if !parkedClaimReleased {
			parkedClaimReleased = true
			parkedSess.releaseTurnClaimLocked()
		}
		parkedSess.mu.Unlock()
		displacedParkedRef.release()
		releaseParkedQueue()
		if isOneShotCompaction {
			finishOneShotCompaction(parkedSess, res, err, handoffActive, isStandaloneCompaction, leftoverParkedRef)
		} else {
			leftoverParkedRef.release()
		}
		return res, err
	}

	if parkedSess := gs.findSessionWithParkedToolResult(baseKey, messages); parkedSess != nil {
		return runMatchedParkedSession(parkedSess)
	}

	if isKeepalivePingRequest(messages) {
		gs.mu.RLock()
		existing := gs.sessions[baseKey]
		gs.mu.RUnlock()
		if existing != nil {
			return existing.RunKeepalivePing(onDelta), nil
		}
	}

	gs.mu.RLock()
	var closingTargets []*AcpProjectSession
	for key, s := range gs.sessions {
		if sameConversationIdentity(key, baseKey) {
			closingTargets = append(closingTargets, s)
		}
	}
	gs.mu.RUnlock()
	waitedClosing := false
	for _, s := range closingTargets {
		s.mu.Lock()
		isClosing := s.closingDone != nil
		s.mu.Unlock()
		if isClosing {
			waitedClosing = true
			s.waitIfClosing(3 * time.Second)
		}
	}
	if waitedClosing && len(extractToolResultsFromMessages(messages)) > 0 {
		if parkedSess := gs.findSessionWithParkedToolResult(baseKey, messages); parkedSess != nil {
			return runMatchedParkedSession(parkedSess)
		}
	}

	if !isStandaloneCompaction {
		turn1SingleFp := ""
		if explicitConversationIdentity(baseKey) != "" && len(messages) >= 2 && messages[1]["role"] == "user" {
			turn1SingleFp = computeFirstUserFingerprint(messages[:1])
		}
		if abortedParked := gs.findAbandonedParkedSessionForExplicitIdentity(baseKey, cwd, sysText, firstUserFp, turn1SingleFp, isExplicitCompacted); abortedParked != nil {
			abortedClaimReleased := false
			releaseAbortedClaim := func() {
				if !abortedClaimReleased {
					abortedClaimReleased = true
					abortedParked.releaseTurnClaim()
				}
			}
			defer releaseAbortedClaim()
			if err := abortedParked.acquireTurnQueue(ctx); err != nil {
				return TurnResult{}, err
			}
			abortedQueueReleased := false
			releaseAbortedQueue := func() {
				if !abortedQueueReleased {
					abortedQueueReleased = true
					abortedParked.releaseTurnQueue()
				}
			}
			defer releaseAbortedQueue()
			turnCtx, cancelActive := context.WithCancel(ctx)
			defer cancelActive()
			abortedParked.mu.Lock()
			abortedParked.setClientIdentityLocked(clientIdent)
			canHotResume := !isExplicitCompacted &&
				abortedParked.SessionID != "" &&
				abortedParked.PromptInFlight &&
				len(abortedParked.ParkedCalls) > 0
			if !abortedParked.ForceRolloverNext && !canHotResume {
				abortedParked.ForceRolloverNext = true
				if !isExplicitCompacted && abortedParked.ForceRolloverReason == "" {
					abortedParked.ForceRolloverReason = "rollover_aborted_tool"
				}
			}
			var staleAbortedCompRef *compactionTargetRef
			if abortedParked.lastCompletedReqFp != "" {
				staleAbortedCompRef = abortedParked.lastCompletedCompactionTarget
				abortedParked.lastCompletedReqFp = ""
				abortedParked.lastCompletedResult = TurnResult{}
				abortedParked.lastCompletedAt = time.Time{}
				abortedParked.lastCompletedCompactionTarget = nil
			}
			setSessionPendingSystemLocked(abortedParked, rawSys, sysText)
			abortedParked.explicitCompactionReq = isExplicitCompaction || isInConvCompaction
			abortedParked.activeTurnReqFp = reqFp
			abortedParked.activeReqCancel = cancelActive
			abortedParked.coalesceHandoff = false
			abortedParked.mu.Unlock()
			staleAbortedCompRef.release()
			res, err := abortedParked.RunTurn(turnCtx, messages, toolsCatalog, thoughtLevel, onDelta, &sel)
			abortedParked.mu.Lock()
			abortedParked.explicitCompactionReq = false
			handedOff := abortedParked.coalesceHandoff || abortedParked.turnClaimCount > 1
			var displacedAbortedRef, leftoverAbortedRef *compactionTargetRef
			if err == nil && handedOff && reqFp != "" && abortedParked.lastCompletedReqFp != reqFp &&
				len(res.ToolCalls) == 0 && res.FailedNotice == "" && DetectAntigravitySoftError(res.Text) == nil {
				abortedParked.lastCompletedReqFp = reqFp
				abortedParked.lastCompletedResult = res
				abortedParked.lastCompletedAt = time.Now()
				if abortedParked.activeCompactionTarget != nil {
					displacedAbortedRef = abortedParked.lastCompletedCompactionTarget
					abortedParked.lastCompletedCompactionTarget = abortedParked.activeCompactionTarget
					abortedParked.activeCompactionTarget = nil
				}
			}
			handoffActive := handedOff && (isSoftDetachedErr(err) || (err == nil && abortedParked.lastCompletedReqFp == reqFp))
			if !handoffActive {
				leftoverAbortedRef = abortedParked.activeCompactionTarget
				abortedParked.activeCompactionTarget = nil
				if abortedParked.lastCompletedReqFp == reqFp && reqFp != "" {
					if leftoverAbortedRef == nil {
						leftoverAbortedRef = abortedParked.lastCompletedCompactionTarget
					} else if abortedParked.lastCompletedCompactionTarget != nil {
						displacedAbortedRef = abortedParked.lastCompletedCompactionTarget
					}
					abortedParked.lastCompletedReqFp = ""
					abortedParked.lastCompletedResult = TurnResult{}
					abortedParked.lastCompletedAt = time.Time{}
					abortedParked.lastCompletedCompactionTarget = nil
				}
			}
			abortedParked.activeTurnReqFp = ""
			abortedParked.activeReqCancel = nil
			abortedParked.coalesceHandoff = false
			if !abortedClaimReleased {
				abortedClaimReleased = true
				abortedParked.releaseTurnClaimLocked()
			}
			abortedParked.mu.Unlock()
			displacedAbortedRef.release()
			releaseAbortedQueue()
			if isOneShotCompaction {
				finishOneShotCompaction(abortedParked, res, err, handoffActive, isStandaloneCompaction, leftoverAbortedRef)
			} else {
				leftoverAbortedRef.release()
			}
			return res, err
		}
	}

	sess := gs.getOrCreateClaimedSessionForMessagesWithCompaction(baseKey, cwd, sysText, firstUserFp, messages, isOneShotCompaction)
	sessClaimReleased := false
	releaseSessClaim := func() {
		if !sessClaimReleased {
			sessClaimReleased = true
			sess.releaseTurnClaim()
		}
	}
	defer releaseSessClaim()
	if err := sess.acquireTurnQueue(ctx); err != nil {
		return TurnResult{}, err
	}
	sessQueueReleased := false
	releaseSessQueue := func() {
		if !sessQueueReleased {
			sessQueueReleased = true
			sess.releaseTurnQueue()
		}
	}
	defer releaseSessQueue()
	var boundTargetRef *compactionTargetRef
	if isOneShotCompaction {
		boundTargetRef = gs.findPreCompactionTarget(sess, baseKey, cwd, firstUserFp, isStandaloneCompaction, clientIdent, messages)
	}
	turnCtx, cancelActive := context.WithCancel(ctx)
	defer cancelActive()
	sess.mu.Lock()
	sess.setClientIdentityLocked(clientIdent)
	if isExplicitCompacted {
		sess.ForceRolloverNext = true
	}
	// A compaction summary only marks the session that answers it (sub-agents and sibling slots
	// keep their caches); the compacted follow-up is also caught by the prefix/length checks.
	sess.explicitCompactionReq = isExplicitCompaction || isInConvCompaction
	var staleCompletedRef *compactionTargetRef
	if sess.lastCompletedReqFp != "" {
		staleCompletedRef = sess.lastCompletedCompactionTarget
		sess.lastCompletedReqFp = ""
		sess.lastCompletedResult = TurnResult{}
		sess.lastCompletedAt = time.Time{}
		sess.lastCompletedCompactionTarget = nil
	}
	displacedCompRef := sess.activeCompactionTarget
	sess.activeCompactionTarget = boundTargetRef
	setSessionPendingSystemLocked(sess, rawSys, sysText)
	sess.activeTurnReqFp = reqFp
	sess.activeReqCancel = cancelActive
	sess.coalesceHandoff = false
	sess.mu.Unlock()
	staleCompletedRef.release()
	displacedCompRef.release()

	res, err := sess.RunTurn(turnCtx, messages, toolsCatalog, thoughtLevel, onDelta, &sel)
	var leftoverCompRef *compactionTargetRef
	var displacedCompletedRef *compactionTargetRef
	sess.mu.Lock()
	handedOff := sess.coalesceHandoff || sess.turnClaimCount > 1
	if handedOff && err == nil && sess.lastCompletedReqFp != reqFp && reqFp != "" &&
		len(res.ToolCalls) == 0 && res.FailedNotice == "" && DetectAntigravitySoftError(res.Text) == nil {
		// Coalesce handoff arrived after finalizeCompletedTurn -> recordRecentCompletedTurn;
		// populate replay state atomically so Request 2 replays and receives compaction target ownership.
		sess.lastCompletedReqFp = reqFp
		sess.lastCompletedResult = res
		sess.lastCompletedAt = time.Now()
		if sess.activeCompactionTarget != nil {
			displacedCompletedRef = sess.lastCompletedCompactionTarget
			sess.lastCompletedCompactionTarget = sess.activeCompactionTarget
			sess.activeCompactionTarget = nil
		}
	}
	handoffActive := handedOff && (isSoftDetachedErr(err) || (err == nil && sess.lastCompletedReqFp == reqFp))
	if !handoffActive {
		leftoverCompRef = sess.activeCompactionTarget
		sess.activeCompactionTarget = nil
		if sess.lastCompletedReqFp == reqFp && reqFp != "" {
			if leftoverCompRef == nil {
				leftoverCompRef = sess.lastCompletedCompactionTarget
			} else if sess.lastCompletedCompactionTarget != nil {
				displacedCompletedRef = sess.lastCompletedCompactionTarget
			}
			sess.lastCompletedReqFp = ""
			sess.lastCompletedResult = TurnResult{}
			sess.lastCompletedAt = time.Time{}
			sess.lastCompletedCompactionTarget = nil
		}
	}
	sess.activeTurnReqFp = ""
	sess.activeReqCancel = nil
	sess.coalesceHandoff = false
	if !sessClaimReleased {
		sessClaimReleased = true
		sess.releaseTurnClaimLocked()
	}
	sess.mu.Unlock()
	displacedCompletedRef.release()
	releaseSessQueue()
	if isOneShotCompaction {
		finishOneShotCompaction(sess, res, err, handoffActive, isStandaloneCompaction, leftoverCompRef)
	} else {
		leftoverCompRef.release()
	}
	return res, err
}

func (gs *GatewayServer) GetStats() map[string]any {
	gs.mu.RLock()
	defer gs.mu.RUnlock()

	var projects []map[string]any
	liveWorkers := 0
	hibernatedCount := 0
	parkedCalls := 0
	for key, s := range gs.sessions {
		s.mu.Lock()
		if s.SessionID != "" {
			liveWorkers++
			parkedCalls += len(s.ParkedCalls)
		} else if s.HibernatedSessionID != "" {
			hibernatedCount++
		}
		if s.SessionID == "" && !s.IsBusy {
			s.mu.Unlock()
			continue
		}
		turnsCopy := append([]TurnHistoryEntry(nil), s.TurnHistory...)
		pendingTier := ""
		if s.PendingThoughtLevel != "" && s.PendingThoughtLevel != s.ThoughtLevel {
			pendingTier = s.PendingThoughtLevel
		}
		fbTier, fbRemSec, fbStrikes, fbReason := s.capacityFallbackTelemetryLocked(capacityFallbackNow())
		proj := map[string]any{
			"convKey":             key,
			"cwd":                 s.Cwd,
			"sessionId":           s.SessionID,
			"clientId":            s.ClientID,
			"clientLabel":         s.ClientLabel,
			"protocol":            s.Protocol,
			"model":               s.ModelID,
			"thoughtLevel":        s.ThoughtLevel,
			"pendingThoughtLevel": pendingTier,
			"messageCount":        s.MessageCount,
			"sessionReuses":       s.SessionReuses,
			"compactionRollovers": s.CompactionRollovers,
			"createdAtMs":         s.CreatedAt.UnixMilli(),
			"lastUsedAt":          s.LastUsedAt.UTC().Format(time.RFC3339),
			"turns":               turnsCopy,
		}
		if fbTier != "" {
			proj["capacityFallbackTier"] = fbTier
			proj["capacityFallbackRemainingSec"] = fbRemSec
			proj["capacityFallbackStrikes"] = fbStrikes
			proj["capacityFallbackReason"] = fbReason
		}
		projects = append(projects, proj)
		s.mu.Unlock()
	}

	sort.Slice(projects, func(i, j int) bool {
		ci, _ := projects[i]["createdAtMs"].(int64)
		cj, _ := projects[j]["createdAtMs"].(int64)
		if ci != cj {
			return ci < cj
		}
		ki, _ := projects[i]["convKey"].(string)
		kj, _ := projects[j]["convKey"].(string)
		return ki < kj
	})

	snap := gwcfg.Current()
	gogc := snap.GoGC
	gomem := snap.GoMemLimit

	tosTripped, tosReason := IsTosTripped()

	return map[string]any{
		"activeProjects":            len(projects),
		"liveWorkers":               liveWorkers,
		"hibernatedSessionsCount":   hibernatedCount,
		"hibernatedSessions":        hibernatedCount,
		"promptsInFlight":           promptsInFlightCount(),
		"parkedCalls":               parkedCalls,
		"storeReady":                gs.store().IsReady(),
		"maxLiveWorkers":            effectiveMaxLiveWorkers(),
		"maxConcurrentPrompts":      effectiveMaxConcurrentPrompts(),
		"maxProbeWorkers":           MaxProbeWorkers,
		"idleWorkerTtlMs":           int(IdleWorkerTtl.Milliseconds()),
		"parkedTtlMs":               int(effectiveParkedTtl().Milliseconds()),
		"maxToolRelays":             effectiveMaxToolRelays(),
		"orphanRelayCaps":           orphanRelayCapsTotal.Load(),
		"engineExitsTotal":          engineExitsTotal.Load(),
		"workerCapRejectionsTotal":  workerCapRejectionsTotal.Load(),
		"probeSlotRejectionsTotal":  probeSlotRejectionsTotal.Load(),
		"softDetachJoined":          softDetachJoinedTotal.Load(),
		"softDetachExpired":         softDetachExpiredTotal.Load(),
		"softDetachSuperseded":      softDetachSupersededTotal.Load(),
		"binary":                    PatchedAgyBin,
		"engine":                    "Go Native Single-Binary Gateway",
		"contextWindowMax":          ModelOrDefault(DefaultModelID).ContextWindow,
		"internalAutoCheckpoint":    "disabled (delegated to host Claude Code)",
		"defaultModel":              DefaultModelID,
		"models":                    AgyModels,
		"gogc":                      gogc,
		"gomemlimit":                gomem,
		"tosTripped":                tosTripped,
		"tosTrippedReason":          tosReason,
		"argonDegradationDetected":  argonDegradationDetectedTotal.Load(),
		"argonDegradationRecovered": argonDegradationRecoveredTotal.Load(),
		"wafAutoBisectHealed":       WafBisectHealedTotal.Load(),
		"cacheBurnWarnings":         cacheBurnWarningsTotal.Load(),
		"logFile":                   GatewayLogFile,
		"tmpLogFile":                TmpLogFile,
		"projects":                  projects,
	}
}

func (gs *GatewayServer) CollectDashboardTelemetry(gwSids map[string]bool) ([]map[string]any, []map[string]any) {
	gs.mu.RLock()
	defer gs.mu.RUnlock()

	type dashTurnCandidate struct {
		convKey      string
		sClientID    string
		sClientLabel string
		sProtocol    string
		entry        TurnHistoryEntry
	}

	var liveSessions []map[string]any
	var turnCandidates []dashTurnCandidate

	for key, s := range gs.sessions {
		s.mu.Lock()
		lastSid := ""
		if s.SessionID != "" {
			gwSids[strings.ToLower(s.SessionID)] = true
			lastSid = s.SessionID
		}
		var lastTurn *TurnHistoryEntry
		var lastMeasured *TurnHistoryEntry
		if len(s.TurnHistory) > 0 {
			lt := s.TurnHistory[len(s.TurnHistory)-1]
			lastTurn = &lt
			for i := len(s.TurnHistory) - 1; i >= 0; i-- {
				h := s.TurnHistory[i]
				if h.Usage != nil && (h.Usage.UsageSource == "db" || h.Usage.UsageSource == "acp") {
					hm := h
					lastMeasured = &hm
					break
				}
			}
		}
		ctxTokens := s.LastCachedReadTokens
		if ctxTokens == 0 && lastMeasured != nil && lastMeasured.Usage != nil {
			ctxTokens = lastMeasured.Usage.ContextTotal()
		} else if ctxTokens == 0 && lastTurn != nil && lastTurn.Usage != nil {
			ctxTokens = lastTurn.Usage.ContextTotal()
		}
		if ctxTokens == 0 && s.LastReportedContextUsed > 0 {
			ctxTokens = s.LastReportedContextUsed
		}
		phase := s.syncPhaseLocked()
		if s.SessionID != "" || s.IsBusy {
			status := "IDLE"
			switch phase {
			case PhaseHotParked:
				status = "PARKED"
			case PhaseStreaming, PhaseSpawning, PhaseDetached:
				status = "BUSY"
			}
			var parkedTools []string
			for _, p := range s.ParkedCalls {
				parkedTools = append(parkedTools, p.Call.Name)
			}
			sort.Strings(parkedTools)
			lastHit := "N/A"
			if lastMeasured != nil && lastMeasured.CacheHitRate != "" {
				lastHit = lastMeasured.CacheHitRate
			} else if lastTurn != nil {
				lastHit = lastTurn.CacheHitRate
			}
			lastServed := ""
			if lastMeasured != nil && lastMeasured.ServedModel != "" {
				lastServed = lastMeasured.ServedModel
			} else if lastTurn != nil && lastTurn.ServedModel != "" {
				lastServed = lastTurn.ServedModel
			}
			totalTurns := s.TotalTurns
			if len(s.TurnHistory) > totalTurns {
				totalTurns = len(s.TurnHistory)
			}
			sm := ModelOrDefault(s.ModelID)
			pendingTier := ""
			if s.PendingThoughtLevel != "" && s.PendingThoughtLevel != s.ThoughtLevel {
				pendingTier = s.PendingThoughtLevel
			}
			fbTier, fbRemSec, fbStrikes, fbReason := s.capacityFallbackTelemetryLocked(capacityFallbackNow())
			ls := map[string]any{
				"convKey":             key,
				"sessionId":           s.SessionID,
				"clientId":            s.ClientID,
				"clientLabel":         s.ClientLabel,
				"protocol":            s.Protocol,
				"model":               sm.ID,
				"modelLabel":          sm.Label,
				"thoughtLevel":        s.ThoughtLevel,
				"pendingThoughtLevel": pendingTier,
				"servedModel":         lastServed,
				"servedMismatch":      isServedModelMismatch(sm.ID, s.ThoughtLevel, lastServed),
				"contextWindow":       sm.ContextWindow,
				"reportedContextUsed": s.LastReportedContextUsed,
				"reportedContextSize": s.LastReportedContextSize,
				"cwd":                 s.Cwd,
				"status":              status,
				"parkedTools":         parkedTools,
				"messageCount":        s.MessageCount,
				"sessionReuses":       s.SessionReuses,
				"compactionRollovers": s.CompactionRollovers,
				"contextTokens":       ctxTokens,
				"contextPct":          roundTo(float64(ctxTokens)/float64(sm.ContextWindow)*100.0, 2),
				"createdAtMs":         s.CreatedAt.UnixMilli(),
				"lastUsedAt":          s.LastUsedAt.UTC().Format(time.RFC3339),
				"turnsCount":          totalTurns,
				"lastHitRate":         lastHit,
				"zeroCacheStreak":     s.ZeroCacheStreak,
				"zeroCacheTokens":     s.ZeroCacheUncachedTokens,
				"cacheWarning":        s.LastCacheWarning != "",
				"cacheWarningDetail":  s.LastCacheWarning,
			}
			if fbTier != "" {
				ls["capacityFallbackTier"] = fbTier
				ls["capacityFallbackRemainingSec"] = fbRemSec
				ls["capacityFallbackStrikes"] = fbStrikes
				ls["capacityFallbackReason"] = fbReason
			}
			liveSessions = append(liveSessions, ls)
		}

		for _, t := range s.TurnHistory {
			if t.SessionID != "" && t.SessionID != lastSid {
				gwSids[strings.ToLower(t.SessionID)] = true
				lastSid = t.SessionID
			}
			turnCandidates = append(turnCandidates, dashTurnCandidate{
				convKey:      key,
				sClientID:    s.ClientID,
				sClientLabel: s.ClientLabel,
				sProtocol:    s.Protocol,
				entry:        t,
			})
		}
		s.mu.Unlock()
	}

	// Sort liveSessions deterministically by creation time (then convKey) so cards never jump around on refresh
	sort.Slice(liveSessions, func(i, j int) bool {
		ci, _ := liveSessions[i]["createdAtMs"].(int64)
		cj, _ := liveSessions[j]["createdAtMs"].(int64)
		if ci != cj {
			return ci < cj
		}
		ki, _ := liveSessions[i]["convKey"].(string)
		kj, _ := liveSessions[j]["convKey"].(string)
		return ki < kj
	})

	// Sort lightweight turn candidates chronologically (newest first) and materialize maps ONLY for the top 60,
	// avoiding tens of thousands of 35-key map allocations across hundreds of restored sessions on every poll.
	sort.Slice(turnCandidates, func(i, j int) bool {
		ti := turnCandidates[i].entry.CompletedAtMs
		tj := turnCandidates[j].entry.CompletedAtMs
		if ti != tj {
			return ti > tj
		}
		if turnCandidates[i].convKey != turnCandidates[j].convKey {
			return turnCandidates[i].convKey < turnCandidates[j].convKey
		}
		return turnCandidates[i].entry.TurnIndex > turnCandidates[j].entry.TurnIndex
	})
	if len(turnCandidates) > 60 {
		turnCandidates = turnCandidates[:60]
	}

	gatewayTurns := make([]map[string]any, 0, len(turnCandidates))
	for _, c := range turnCandidates {
		t := c.entry
		inT := 0
		crT := 0
		rawIn := 0
		rawCr := 0
		outT := 0
		usageSrc := ""
		gens := 0
		ctxTok := 0
		if t.Usage != nil {
			inT = t.Usage.InputTokens
			crT = t.Usage.CachedReadTokens
			rawIn = t.Usage.RawUncachedInputTokens
			if rawIn == 0 {
				rawIn = inT
			}
			rawCr = t.Usage.RawTpuCachedTokens
			if rawCr == 0 {
				rawCr = crT
			}
			outT = t.Usage.OutputTokens
			usageSrc = t.Usage.UsageSource
			gens = t.Usage.Generations
			ctxTok = t.Usage.ContextTotal()
		}
		tm := ModelOrDefault(t.Model)
		stepCost := PriceUsage(tm, rawIn, rawCr, outT, 0, 0).CostUsd
		cID, cLbl, cProto := t.ClientID, t.ClientLabel, t.Protocol
		if cID == "" {
			cID = c.sClientID
			cLbl = c.sClientLabel
		}
		if cProto == "" {
			cProto = c.sProtocol
		}
		visStreamMs := t.StreamMs - t.ThinkingMs
		if visStreamMs < 0 {
			visStreamMs = 0
		}
		gatewayTurns = append(gatewayTurns, map[string]any{
			"model":            tm.ID,
			"modelLabel":       tm.Label,
			"thoughtLevel":     t.ThoughtLevel,
			"convKey":          c.convKey,
			"sessionId":        t.SessionID,
			"clientId":         cID,
			"clientLabel":      cLbl,
			"protocol":         cProto,
			"turnIndex":        t.TurnIndex,
			"completedAtMs":    t.CompletedAtMs,
			"ts":               t.Ts,
			"mode":             t.Mode,
			"stopReason":       t.StopReason,
			"elapsedMs":        t.ElapsedMs,
			"ttftMs":           t.TtftMs,
			"upstreamTtftMs":   t.UpstreamTtftMs,
			"streamMs":         t.StreamMs,
			"thinkingMs":       t.ThinkingMs,
			"visibleStreamMs":  visStreamMs,
			"retryCount":       t.RetryCount,
			"tps":              t.Tps,
			"decodeTps":        t.DecodeTps,
			"hasThinking":      t.HasThinking,
			"thinkingTokens":   t.ThinkingTokens,
			"inputTokens":      inT,
			"cachedReadTokens": crT,
			"contextTokens":    ctxTok,
			"outputTokens":     outT,
			"usageSource":      usageSrc,
			"generations":      gens,
			"cacheHitRate":     t.CacheHitRate,
			"costUsd":          roundTo(stepCost, 4),
			"servedModel":      t.ServedModel,
			"servedMismatch":   isServedModelMismatch(tm.ID, t.ThoughtLevel, t.ServedModel),
			"cacheMiss":        t.CacheMiss,
		})
	}
	return liveSessions, gatewayTurns
}
