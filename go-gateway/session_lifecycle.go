package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"agy-acp-gateway-go/internal/acp"
	gwstore "agy-acp-gateway-go/internal/store"
)

var _ acp.SessionListener = (*AcpProjectSession)(nil)
var capacityFallbackNow = time.Now
var nextCompactionLeaseToken atomic.Uint64

type compactionPendingLease struct {
	token   uint64
	sid     string
	expires time.Time
}

type compactionTargetRef struct {
	sess            *AcpProjectSession
	sid             string
	token           uint64
	markSidRollover bool
	released        atomic.Bool
}

func (r *compactionTargetRef) release() {
	if r == nil || r.sess == nil || !r.released.CompareAndSwap(false, true) {
		return
	}
	r.sess.removeCompactionPendingLease(r.token)
}

// HibernatedCallMeta preserves lightweight tool identity and sequence order across
// Elastic Parked Offload / hibernation so resumed_delta prompts and Gemini secondary
// rebinding remain unambiguous for multi-sibling tool batches.
type HibernatedCallMeta struct {
	ToolName   string `json:"toolName,omitempty"`
	ArgSummary string `json:"argSummary,omitempty"`
	Seq        int    `json:"seq,omitempty"`
}

func summarizeToolCallArgs(input map[string]any) string {
	if len(input) == 0 {
		return ""
	}
	cleanVal := func(v any) string {
		var s string
		switch x := v.(type) {
		case string:
			s = x
		case fmt.Stringer:
			s = x.String()
		default:
			if b, err := json.Marshal(x); err == nil {
				s = string(b)
			}
		}
		s = strings.TrimSpace(strings.NewReplacer("\r\n", " ", "\n", " ", "\r", " ", "\t", " ").Replace(s))
		if len(s) == 0 {
			return ""
		}
		return truncateRunes(s, 72)
	}
	for _, pref := range []string{"file_path", "path", "command", "cmd", "pattern", "query", "url", "target", "description", "prompt"} {
		if v, ok := input[pref]; ok {
			if cs := cleanVal(v); cs != "" {
				return cs
			}
		}
	}
	keys := make([]string, 0, len(input))
	for k := range input {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		if cs := cleanVal(input[k]); cs != "" {
			parts = append(parts, k+"="+cs)
			if len(parts) >= 2 {
				break
			}
		}
	}
	return truncateRunes(strings.Join(parts, ", "), 72)
}

type sessionBlobSig struct {
	sid              string
	tenantKey        string
	cwd              string
	modelID          string
	tier             string
	toolFp           string
	sysText          string
	initSysText      string
	initAppendPrompt string
	hibParkedJSON    string
	initToolsPtr     *ToolDef
	initToolsLen     int
	curToolsPtr      *ToolDef
	curToolsLen      int
}

func toolSliceDataPtr(cat []ToolDef) *ToolDef {
	if len(cat) == 0 {
		return nil
	}
	return &cat[0]
}

type AcpProjectSession struct {
	mu                            sync.Mutex
	ConvKey                       string
	TenantKey                     string
	Cwd                           string
	RawSystemText                 string
	SystemText                    string
	InitialSystemText             string
	InitialAppendPrompt           string
	Codec                         *ReversibleSessionCodec
	SessionID                     string
	engineGen                     uint64
	HibernatedSessionID           string
	HibernatedParkedCallIDs       map[string]bool
	HibernatedParkedCalls         map[string]HibernatedCallMeta
	LastWakeMethod                string
	InitialToolsCatalog           []ToolDef
	DeclaredMcpTools              map[string]bool
	ToolsCatalog                  []ToolDef
	McpToHostMap                  map[string]string
	ToolFingerprint               string
	cachedInitToolsPtr            *ToolDef
	cachedInitToolsLen            int
	cachedInitToolsFp             string
	cachedInitToolsJSON           string
	cachedCurToolsPtr             *ToolDef
	cachedCurToolsLen             int
	cachedCurToolsFp              string
	cachedCurToolsJSON            string
	snapBlobGen                   uint64
	snapBlobSig                   sessionBlobSig
	persistedInDB                 bool
	ParkedCalls                   map[string]*ParkedMcpCall
	LastDeliveredCallIDs          []string
	PendingSiblingCalls           []*ParkedMcpCall
	abortedSiblingCalls           []*ParkedMcpCall
	RelayedCalls                  map[string]*ParkedMcpCall
	reportedToolCalls             map[string]reportedToolIdentity
	ParkedSince                   time.Time
	MessageCount                  int
	HistoryFingerprint            string
	cancelledHistoryFp            string
	TotalTurns                    int
	TurnHistory                   []TurnHistoryEntry
	CreatedAt                     time.Time
	LastUsedAt                    time.Time
	CompactionRollovers           int
	SessionReuses                 int
	IsBusy                        bool
	turnClaimed                   bool
	turnClaimCount                int
	PromptInFlight                bool
	promptGen                     uint64
	promptStart                   time.Time
	promptLastActivityNs          *atomic.Int64
	promptCancel                  context.CancelFunc
	activeTurnToolChoice          TurnToolChoice
	ForceRolloverNext             bool
	ForceRolloverReason           string
	explicitCompactionReq         bool
	lastCompactedAt               time.Time
	contentAnchors                sessionContentAnchors
	compactionLeases              []compactionPendingLease
	pendingRawSystemText          string
	pendingSystemText             string
	hasPendingSystem              bool
	isWafBisectProbe              bool
	skipSystemReprune             bool
	allowWafFastCancel            bool
	relaysThisTurn                int
	orphanRelayHops               int
	lastOrphanRelayAt             time.Time
	orphanCapLogged               bool
	orphanCapStreak               int
	lastTurnAssistant             bool
	PromptStepCount               int
	LastSeenDbStepIdx             int
	LastPlannerBatchIdx           int
	LastPlannerBatchNextPos       int
	LastPlannerBatchTotal         int
	LastSeenGenMetaIdx            int
	LastCachedReadTokens          int
	LastReportedContextUsed       int
	LastReportedContextSize       int
	ZeroCacheStreak               int
	ZeroCacheUncachedTokens       int64
	LastCacheWarning              string
	RecentToolSigs                []string
	ModelID                       string
	ThoughtLevel                  string
	PendingThoughtLevel           string
	ClientID                      string
	ClientLabel                   string
	Protocol                      string
	capacityFallbackSid           string
	capacityFallbackTier          string
	capacityFallbackUntil         time.Time
	capacityFallbackStrikes       int
	capacityFallbackStrikeSid     string
	capacityFallbackReason        string
	capacityFallbackProbing       bool
	lastTurnExplicitTier          bool
	highTierZeroCacheStreak       int
	pendingTierZeroCacheStreak    int
	parkedPromptFailedSid         string
	wasCancelledTurn              bool
	activeTurn                    *activeTurnState
	activeTurnReqFp               string
	activeReqCancel               context.CancelFunc
	coalesceHandoff               bool
	activeCompactionTarget        *compactionTargetRef
	lastCompletedCompactionTarget *compactionTargetRef
	lastCompletedReqFp            string
	lastCompletedResult           TurnResult
	lastCompletedAt               time.Time
	detachedTurn                  *detachedTurnState
	queueSem                      chan struct{}
	roDBMu                        sync.RWMutex
	roDB                          *sql.DB
	roDBSid                       string
	roDBPath                      string
	onBeforeSpawn                 func(*AcpProjectSession) error
	admitCtx                      context.Context
	spawning                      bool
	closing                       bool
	closingDone                   chan struct{}
	phase                         SessionPhase
	phaseTransitions              uint64
	Engine                        acp.Engine
	Store                         gwstore.SessionStore
}

func (s *AcpProjectSession) setAdmitContext(ctx context.Context) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.admitCtx = ctx
	s.mu.Unlock()
}

func (s *AcpProjectSession) getAdmitContext() context.Context {
	if s == nil {
		return context.Background()
	}
	s.mu.Lock()
	ctx := s.admitCtx
	s.mu.Unlock()
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

// recordHibernatedParkedCallsLocked snapshots all pending/parked/relayed/sibling tool call IDs
// along with lightweight tool metadata and 1-based delivery sequence numbers. Caller holds s.mu.
func (s *AcpProjectSession) recordHibernatedParkedCallsLocked() {
	total := len(s.ParkedCalls) + len(s.RelayedCalls) + len(s.PendingSiblingCalls)
	if total == 0 {
		return
	}
	if s.HibernatedParkedCallIDs == nil {
		s.HibernatedParkedCallIDs = make(map[string]bool, total)
	}
	if s.HibernatedParkedCalls == nil {
		s.HibernatedParkedCalls = make(map[string]HibernatedCallMeta, total)
	}
	seq := 0
	for _, m := range s.HibernatedParkedCalls {
		if m.Seq > seq {
			seq = m.Seq
		}
	}
	addOne := func(cid string, call ToolCallItem) {
		cid = strings.TrimSpace(cid)
		if cid == "" {
			return
		}
		s.HibernatedParkedCallIDs[cid] = true
		if existing, exists := s.HibernatedParkedCalls[cid]; exists {
			if existing.ToolName == "" && call.Name != "" {
				existing.ToolName = call.Name
			}
			if existing.ArgSummary == "" && len(call.Input) > 0 {
				existing.ArgSummary = summarizeToolCallArgs(call.Input)
			}
			s.HibernatedParkedCalls[cid] = existing
			return
		}
		seq++
		s.HibernatedParkedCalls[cid] = HibernatedCallMeta{
			ToolName:   call.Name,
			ArgSummary: summarizeToolCallArgs(call.Input),
			Seq:        seq,
		}
	}
	// 1. Exact delivered order first.
	for _, cid := range s.LastDeliveredCallIDs {
		if p, ok := s.ParkedCalls[cid]; ok && p != nil {
			addOne(cid, p.Call)
		} else if p, ok := s.RelayedCalls[cid]; ok && p != nil {
			addOne(cid, p.Call)
		}
	}
	// 2. Remaining ParkedCalls in deterministic key order.
	if len(s.ParkedCalls) > 0 {
		keys := make([]string, 0, len(s.ParkedCalls))
		for k := range s.ParkedCalls {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if p := s.ParkedCalls[k]; p != nil {
				addOne(k, p.Call)
			} else {
				addOne(k, ToolCallItem{ID: k})
			}
		}
	}
	// 3. Remaining RelayedCalls in deterministic key order.
	if len(s.RelayedCalls) > 0 {
		keys := make([]string, 0, len(s.RelayedCalls))
		for k := range s.RelayedCalls {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if p := s.RelayedCalls[k]; p != nil {
				addOne(k, p.Call)
			} else {
				addOne(k, ToolCallItem{ID: k})
			}
		}
	}
	// 4. PendingSiblingCalls in slice order.
	for _, sib := range s.PendingSiblingCalls {
		if sib != nil && sib.Call.ID != "" {
			addOne(sib.Call.ID, sib.Call)
		}
	}
}

func (s *AcpProjectSession) clearHibernatedParkedCallsLocked() {
	s.HibernatedParkedCallIDs = nil
	s.HibernatedParkedCalls = nil
}

func (s *AcpProjectSession) snapshotHibernatedParkedCalls() map[string]HibernatedCallMeta {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.HibernatedParkedCalls) == 0 {
		return nil
	}
	out := make(map[string]HibernatedCallMeta, len(s.HibernatedParkedCalls))
	for k, v := range s.HibernatedParkedCalls {
		out[k] = v
	}
	return out
}

// MatchHibernatedParkedCallID performs deterministic secondary rebinding for protocols without
// native tool_use_id (such as Gemini /v1beta functionResponse) after Elastic Parked Offload.
// Matches unused hibernated calls in (Seq ASC, CallID ASC) order by exact ToolName (or first unused
// entry when funcName is empty). Callers should add the returned ID to used.
func (s *AcpProjectSession) MatchHibernatedParkedCallID(funcName string, used map[string]bool) string {
	if s == nil {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.HibernatedParkedCalls) == 0 && len(s.HibernatedParkedCallIDs) == 0 {
		return ""
	}
	type entry struct {
		id   string
		name string
		seq  int
	}
	list := make([]entry, 0, len(s.HibernatedParkedCalls)+len(s.HibernatedParkedCallIDs))
	seen := make(map[string]bool, len(s.HibernatedParkedCalls))
	for cid, meta := range s.HibernatedParkedCalls {
		seen[cid] = true
		seq := meta.Seq
		if seq <= 0 {
			seq = 1<<30 - 1
		}
		list = append(list, entry{id: cid, name: meta.ToolName, seq: seq})
	}
	for cid, ok := range s.HibernatedParkedCallIDs {
		if ok && !seen[cid] {
			list = append(list, entry{id: cid, name: "", seq: 1<<30 - 1})
		}
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].seq != list[j].seq {
			return list[i].seq < list[j].seq
		}
		return list[i].id < list[j].id
	})
	want := strings.TrimSpace(funcName)
	if want != "" {
		for _, e := range list {
			if used[e.id] {
				continue
			}
			if e.name == want {
				return e.id
			}
		}
		for _, e := range list {
			if used[e.id] {
				continue
			}
			if e.name != "" && strings.EqualFold(e.name, want) {
				return e.id
			}
		}
		return ""
	}
	for _, e := range list {
		if !used[e.id] {
			return e.id
		}
	}
	return ""
}

func (s *AcpProjectSession) engine() acp.Engine {
	if s != nil && s.Engine != nil {
		return s.Engine
	}
	return GlobalEngine
}

func (s *AcpProjectSession) store() gwstore.SessionStore {
	if s != nil && s.Store != nil {
		return s.Store
	}
	return GlobalStore
}

func (s *AcpProjectSession) persistSession() {
	if s == nil {
		return
	}
	if snap, ok := captureSessionSnapshot(s); ok {
		s.store().UpsertSessionSnapshot(snap)
	}
}

func (s *AcpProjectSession) persistSessionAsync() {
	if s == nil {
		return
	}
	st := s.store()
	if !st.IsReady() {
		return
	}
	if snap, ok := captureSessionSnapshot(s); ok {
		st.UpsertSessionSnapshotAsync(snap)
	}
}

func (s *AcpProjectSession) persistTurnAndSession(entry TurnHistoryEntry) {
	if s == nil || strings.HasPrefix(s.ConvKey, "util:") {
		return
	}
	snap, hasSnap := captureSessionSnapshot(s)
	s.store().InsertTurnWithSnapshot(snap, hasSnap, s.ConvKey, entry)
}

func NewAcpProjectSession(convKey, cwd, systemText string) *AcpProjectSession {
	if cwd == "" {
		cwd = DefaultCwd
	}
	tk := randomHex(12)
	return &AcpProjectSession{
		ConvKey:             convKey,
		TenantKey:           tk,
		Cwd:                 cwd,
		SystemText:          systemText,
		InitialSystemText:   systemText,
		Codec:               NewReversibleSessionCodec(tk),
		McpToHostMap:        make(map[string]string),
		ParkedCalls:         make(map[string]*ParkedMcpCall),
		reportedToolCalls:   make(map[string]reportedToolIdentity),
		CreatedAt:           time.Now(),
		LastUsedAt:          time.Now(),
		PromptStepCount:     1,
		LastSeenDbStepIdx:   -1,
		LastPlannerBatchIdx: -1,
		LastSeenGenMetaIdx:  -1,
		ModelID:             DefaultModelID,
		ThoughtLevel:        "medium",
		queueSem:            make(chan struct{}, 1),
	}
}

func (s *AcpProjectSession) SetEngineGen(gen uint64) {
	s.setEngineGen(gen)
}

func (s *AcpProjectSession) HandleSessionUpdate(u map[string]any) {
	s.handleSessionUpdate(u)
}

func (s *AcpProjectSession) HandlePermissionRequest(rpcID any, options []acp.PermissionOption, tc map[string]any) {
	s.handlePermissionRequest(rpcID, options, tc)
}

func (s *AcpProjectSession) OnEngineExited(sid string, exitedGen uint64) {
	s.onEngineExited(sid, exitedGen)
}

func (s *AcpProjectSession) setEngineGen(gen uint64) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.engineGen = gen
	s.mu.Unlock()
}

func (s *AcpProjectSession) claimTurnLocked() {
	if s == nil {
		return
	}
	s.turnClaimCount++
	s.turnClaimed = true
	s.syncPhaseLocked()
}

func (s *AcpProjectSession) releaseTurnClaimLocked() {
	if s == nil {
		return
	}
	if s.turnClaimCount > 0 {
		s.turnClaimCount--
	}
	s.turnClaimed = s.turnClaimCount > 0
	s.syncPhaseLocked()
}

func (s *AcpProjectSession) releaseTurnClaim() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.releaseTurnClaimLocked()
	s.mu.Unlock()
}

func (s *AcpProjectSession) ensureQueueSem() chan struct{} {
	s.mu.Lock()
	if s.queueSem == nil {
		s.queueSem = make(chan struct{}, 1)
	}
	ch := s.queueSem
	s.mu.Unlock()
	return ch
}

func (s *AcpProjectSession) acquireTurnQueue(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	ch := s.ensureQueueSem()
	select {
	case ch <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *AcpProjectSession) releaseTurnQueue() {
	ch := s.ensureQueueSem()
	select {
	case <-ch:
	default:
	}
}

func (s *AcpProjectSession) ensureReadOnlySessionDB(sid string) *sql.DB {
	if s == nil || sid == "" {
		return nil
	}
	dbPath := filepath.Join(ConversationsDir(), sid+".db")
	s.roDBMu.RLock()
	if s.roDB != nil && s.roDBSid == sid && s.roDBPath == dbPath {
		db := s.roDB
		s.roDBMu.RUnlock()
		return db
	}
	s.roDBMu.RUnlock()

	st, err := os.Stat(dbPath)
	if err != nil || st.Size() < 100 {
		return nil
	}

	s.roDBMu.Lock()
	defer s.roDBMu.Unlock()
	if s.roDB != nil && s.roDBSid == sid && s.roDBPath == dbPath {
		return s.roDB
	}
	if s.roDB != nil {
		_ = s.roDB.Close()
		s.roDB = nil
		s.roDBSid = ""
		s.roDBPath = ""
	}
	db, err := openReadOnlySQLite(dbPath)
	if err != nil {
		return nil
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	s.roDB = db
	s.roDBSid = sid
	s.roDBPath = dbPath
	return db
}

// invalidateReadOnlySessionDB closes a cached read-only DB handle for sid after a query error
// so the next ensureReadOnlySessionDB call re-stats and reopens the file.
func (s *AcpProjectSession) invalidateReadOnlySessionDB(sid string) {
	if s == nil || sid == "" {
		return
	}
	s.roDBMu.Lock()
	if s.roDB != nil && s.roDBSid == sid {
		_ = s.roDB.Close()
		s.roDB = nil
		s.roDBSid = ""
		s.roDBPath = ""
	}
	s.roDBMu.Unlock()
}

func (s *AcpProjectSession) getReadOnlySessionDB(sid string) *sql.DB {
	return s.ensureReadOnlySessionDB(sid)
}

func (s *AcpProjectSession) borrowReadOnlySessionDB(sid string) (*sql.DB, func()) {
	if s.ensureReadOnlySessionDB(sid) == nil {
		return nil, func() {}
	}
	dbPath := filepath.Join(ConversationsDir(), sid+".db")
	s.roDBMu.RLock()
	if s.roDB != nil && s.roDBSid == sid && s.roDBPath == dbPath {
		return s.roDB, s.roDBMu.RUnlock
	}
	s.roDBMu.RUnlock()
	return nil, func() {}
}

func (s *AcpProjectSession) closeReadOnlyDB() {
	if s == nil {
		return
	}
	s.roDBMu.Lock()
	db := s.roDB
	s.roDB = nil
	s.roDBSid = ""
	s.roDBPath = ""
	if db != nil {
		_ = db.Close()
	}
	s.roDBMu.Unlock()
}

func (s *AcpProjectSession) touchPromptActivity() {
	if s == nil {
		return
	}
	s.mu.Lock()
	start := s.promptStart
	act := s.promptLastActivityNs
	s.mu.Unlock()
	if act != nil && !start.IsZero() {
		act.Store(int64(time.Since(start)))
	}
}

func (s *AcpProjectSession) touchPromptActivityLocked() {
	if s == nil || s.promptLastActivityNs == nil || s.promptStart.IsZero() {
		return
	}
	s.promptLastActivityNs.Store(int64(time.Since(s.promptStart)))
}

func (s *AcpProjectSession) onEngineExited(sid string, exitedGen uint64) {
	if s == nil || sid == "" {
		return
	}
	demoted := false
	var cancelPrompt context.CancelFunc
	s.mu.Lock()
	if s.SessionID == sid && (s.engineGen == 0 || exitedGen == 0 || s.engineGen == exitedGen) &&
		!s.isActivelyWorkingLocked() {
		cancelPrompt = s.promptCancel
		if cancelPrompt != nil {
			s.promptGen++
		}
		s.promptCancel = nil
		s.promptLastActivityNs = nil
		s.promptStart = time.Time{}
		s.pendingTierZeroCacheStreak = 0
		s.parkedPromptFailedSid = ""
		keepResume := s.MessageCount > 0 && !strings.HasPrefix(s.ConvKey, "util:")
		if len(s.ParkedCalls) > 0 || len(s.RelayedCalls) > 0 || len(s.PendingSiblingCalls) > 0 {
			if keepResume {
				s.recordHibernatedParkedCallsLocked()
			}
			for k, p := range s.ParkedCalls {
				select {
				case p.ResultChan <- map[string]any{
					"content": []map[string]any{{"type": "text", "text": "Session hibernated."}},
					"isError": true,
				}:
				default:
				}
				delete(s.ParkedCalls, k)
			}
			s.LastDeliveredCallIDs = nil
			s.PendingSiblingCalls = nil
			s.abortedSiblingCalls = nil
			s.RelayedCalls = nil
			s.ParkedSince = time.Time{}
		}
		hibSid := ""
		if keepResume {
			hibSid = sid
		}
		s.demoteToHibernatedLocked(hibSid)
		demoted = true
	}
	s.mu.Unlock()
	if cancelPrompt != nil {
		cancelPrompt()
	}
	if demoted {
		s.closeReadOnlyDB()
		s.persistSession()
	}
}

func (s *AcpProjectSession) bridgeServerName() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.InitialToolsCatalog) > 0 {
		return sessionBridgeServerName(s.InitialToolsCatalog)
	}
	return sessionBridgeServerName(s.ToolsCatalog)
}

func (s *AcpProjectSession) awaitHelperFullMcpName() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	cat := s.InitialToolsCatalog
	if len(cat) == 0 {
		cat = s.ToolsCatalog
	}
	srv := sessionBridgeServerName(cat)
	return "mcp_" + srv + "_" + sessionHelperMcpName(cat, true)
}

// configOptionTimeout bounds session/set_config_option. agy answers it locally in 1-5 ms
// (probe 2026-11-11, P1), so a slow reply means the engine is wedged, not that the model is busy.
var configOptionTimeout = 5 * time.Second

// setAndVerifySessionConfigOption requires a positive acknowledgement: agy returns the full
// configOptions list whose entry for configID must carry currentValue == expectedValue.
// Missing results, missing entries and mismatches all fail. Only transport errors are retried once;
// -32602 (unknown model/tier, tier not offered for the model) fails immediately.
func setAndVerifySessionConfigOption(sessionID, configID, expectedValue string, timeout time.Duration) error {
	return setAndVerifySessionConfigOptionWith(GlobalEngine, sessionID, configID, expectedValue, timeout)
}

func setAndVerifySessionConfigOptionWith(eng acp.Engine, sessionID, configID, expectedValue string, timeout time.Duration) error {
	_, err := setAndVerifySessionConfigOptionWithRaw(eng, sessionID, configID, expectedValue, timeout)
	return err
}

func setAndVerifySessionConfigOptionWithRaw(eng acp.Engine, sessionID, configID, expectedValue string, timeout time.Duration) (json.RawMessage, error) {
	if timeout <= 0 {
		timeout = configOptionTimeout
	}
	if eng == nil {
		eng = GlobalEngine
	}
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		rawRes, err := eng.RPC("session/set_config_option", map[string]any{
			"sessionId": sessionID,
			"configId":  configID,
			"value":     expectedValue,
		}, timeout)
		if err != nil {
			lastErr = err
			var rpcErr *acpRPCError
			if errors.As(err, &rpcErr) || strings.Contains(err.Error(), "-32602") {
				return nil, err
			}
			continue
		}
		if err := verifyConfigOptionAck(rawRes, configID, expectedValue); err != nil {
			return nil, err
		}
		return rawRes, nil
	}
	return nil, lastErr
}

func parseConfigOptionValues(rawRes json.RawMessage) map[string]string {
	if len(rawRes) == 0 {
		return nil
	}
	var parsed struct {
		ConfigOptions []struct {
			ID           string `json:"id"`
			CurrentValue any    `json:"currentValue"`
		} `json:"configOptions"`
	}
	if json.Unmarshal(rawRes, &parsed) != nil || len(parsed.ConfigOptions) == 0 {
		return nil
	}
	out := make(map[string]string, len(parsed.ConfigOptions))
	for _, opt := range parsed.ConfigOptions {
		if opt.ID != "" && opt.CurrentValue != nil {
			out[opt.ID] = fmt.Sprint(opt.CurrentValue)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func verifyConfigOptionAck(rawRes json.RawMessage, configID, expectedValue string) error {
	var parsed struct {
		ConfigOptions []struct {
			ID           string `json:"id"`
			CurrentValue any    `json:"currentValue"`
		} `json:"configOptions"`
	}
	if len(rawRes) == 0 || json.Unmarshal(rawRes, &parsed) != nil {
		return fmt.Errorf("configOption %s: unreadable acknowledgement", configID)
	}
	for _, opt := range parsed.ConfigOptions {
		if opt.ID != configID {
			continue
		}
		got := fmt.Sprint(opt.CurrentValue)
		if opt.CurrentValue == nil || got != expectedValue {
			return fmt.Errorf("configOption %s currentValue=%q != expected %q", configID, got, expectedValue)
		}
		return nil
	}
	return fmt.Errorf("configOption %s missing from acknowledgement", configID)
}

// pinSessionConfig applies the gateway-selected model and thinking tier to a just-opened or
// resumed ACP session. session/new opens on the server default (gemini-4-carbon/medium) and
// session/resume resets both model and tier to that default (probe P3, agy 1.5.5), so both must be
// re-applied and positively acknowledged whenever initial configOptions do not already confirm
// the target values. A session that cannot be confirmed is closed instead of silently running
// (and billing) a different model or tier.
func (s *AcpProjectSession) pinSessionConfig(sid string, m *ModelInfo, tier string) (string, error) {
	applied, _, err := s.pinSessionConfigWithInitial(sid, m, tier, nil)
	return applied, err
}

func (s *AcpProjectSession) pinSessionConfigWithInitial(sid string, m *ModelInfo, tier string, initialRaw json.RawMessage) (string, map[string]string, error) {
	eng := s.engine()
	curOpts := parseConfigOptionValues(initialRaw)
	if tier == "" {
		tier = "medium"
	}
	setModel := curOpts == nil || curOpts["model"] != m.ID
	var applied string
	var err error
	if setModel {
		var ackRaw json.RawMessage
		ackRaw, err = setAndVerifySessionConfigOptionWithRaw(eng, sid, "model", m.ID, configOptionTimeout)
		if err == nil {
			// Always replace curOpts with post-model-switch options so stale pre-switch
			// thought_level values can never cause skipping thought_level after switching models.
			curOpts = parseConfigOptionValues(ackRaw)
		}
	}
	if err == nil {
		if m.FixedTier != "" {
			applied = m.FixedTier
		} else {
			setTier := curOpts == nil || curOpts["thought_level"] != tier
			if setTier {
				var ackRaw json.RawMessage
				ackRaw, err = setAndVerifySessionConfigOptionWithRaw(eng, sid, "thought_level", tier, configOptionTimeout)
				if err == nil {
					curOpts = parseConfigOptionValues(ackRaw)
				}
			}
			if err == nil {
				applied = tier
			}
		}
	}
	if err == nil {
		return applied, curOpts, nil
	}
	LogEvent("MODEL_PIN_FAILED", "closing ACP session whose model/tier could not be applied: "+m.ID+"/"+tier, map[string]any{
		"convKey": s.ConvKey, "sessionId": sid, "model": m.ID, "thoughtLevel": tier, "error": err.Error(),
	})
	_ = eng.CloseSession(sid)
	eng.UnregisterSession(sid)
	s.mu.Lock()
	if s.SessionID == sid {
		s.SessionID = ""
		s.syncPhaseLocked()
	}
	s.mu.Unlock()
	return "", nil, &ModelSwitchError{From: "upstream default", To: m.ID + "/" + tier, Err: err}
}

// setSessionModelAndTier sets model and/or tier with strict acknowledgement and returns the tier
// that is actually in effect. Models with a fixed tier never receive thought_level.
func setSessionModelAndTier(sid string, m *ModelInfo, tier string, setModel, setTier bool) (string, error) {
	return setSessionModelAndTierWith(GlobalEngine, sid, m, tier, setModel, setTier)
}

func setSessionModelAndTierWith(eng acp.Engine, sid string, m *ModelInfo, tier string, setModel, setTier bool) (string, error) {
	if tier == "" {
		tier = "medium"
	}
	if setModel {
		if err := setAndVerifySessionConfigOptionWith(eng, sid, "model", m.ID, configOptionTimeout); err != nil {
			return "", err
		}
	}
	if m.FixedTier != "" {
		return m.FixedTier, nil
	}
	if setTier {
		if err := setAndVerifySessionConfigOptionWith(eng, sid, "thought_level", tier, configOptionTimeout); err != nil {
			return "", err
		}
	}
	return tier, nil
}

// setDesiredSelection records the requested model/tier without any RPC. Used before a session is
// opened or rolled over: openFreshSession / reloadHibernatedSession pin these values themselves.
func (s *AcpProjectSession) setDesiredSelection(sel ModelSelection) {
	tier := sel.Tier
	if tier == "" {
		tier = "medium"
	}
	s.mu.Lock()
	s.ModelID, s.ThoughtLevel, s.PendingThoughtLevel = sel.Model.ID, tier, ""
	s.mu.Unlock()
}

func (s *AcpProjectSession) promotePendingTierIfIdleLocked() bool {
	if s.PromptInFlight || s.PendingThoughtLevel == "" {
		return false
	}
	s.ThoughtLevel = s.PendingThoughtLevel
	s.PendingThoughtLevel = ""
	return true
}

func (s *AcpProjectSession) promotePendingTierIfIdle() {
	if s == nil {
		return
	}
	s.mu.Lock()
	promoted := s.promotePendingTierIfIdleLocked()
	s.mu.Unlock()
	if promoted {
		s.persistSessionAsync()
	}
}

// ModelSwitchError reports a live-session model/tier switch that agy did not confirm.
// No prompt is sent after it; the client gets an error instead of a reply from another model.
type ModelSwitchError struct {
	From, To string
	Err      error
}

func (e *ModelSwitchError) Error() string {
	return fmt.Sprintf("model switch %s -> %s not confirmed by upstream: %v", e.From, e.To, e.Err)
}

func (e *ModelSwitchError) Unwrap() error { return e.Err }

func tierRank(t string) int {
	switch t {
	case "low":
		return 1
	case "medium":
		return 2
	case "high":
		return 3
	case "xhigh":
		return 4
	default:
		return 0
	}
}

func nextFallbackTier(modelID, curTier string) string {
	switch curTier {
	case "xhigh":
		if m := GetModel(modelID); m != nil && m.SupportsXHigh {
			return "high"
		}
		return "medium"
	case "high":
		return "medium"
	case "medium":
		if modelID == "gemini-4-carbon" {
			return "low"
		}
		return ""
	default:
		return ""
	}
}

// clearCapacityFallbackLocked clears active capacity fallback clamping state. Caller holds s.mu.
func (s *AcpProjectSession) clearCapacityFallbackLocked(resetStrikes bool) {
	s.capacityFallbackSid = ""
	s.capacityFallbackTier = ""
	s.capacityFallbackUntil = time.Time{}
	s.capacityFallbackReason = ""
	s.capacityFallbackProbing = false
	if resetStrikes {
		s.capacityFallbackStrikes = 0
		s.capacityFallbackStrikeSid = ""
	}
}

// armCapacityFallbackLocked arms lower-tier capacity fallback with exponential backoff cooldown.
// Caller holds s.mu.
func (s *AcpProjectSession) armCapacityFallbackLocked(sid, tier, reason string, now time.Time) (int, time.Duration) {
	s.capacityFallbackSid = sid
	s.capacityFallbackTier = tier
	s.capacityFallbackReason = reason
	s.capacityFallbackProbing = false
	if sid != "" && s.capacityFallbackStrikeSid == sid {
		s.capacityFallbackStrikes++
	} else {
		s.capacityFallbackStrikeSid = sid
		s.capacityFallbackStrikes = 1
	}
	var cooldown time.Duration
	base := CapacityFallbackTTL
	if base <= 0 {
		s.capacityFallbackUntil = time.Time{}
	} else {
		maxTTL := CapacityFallbackMaxTTL
		if maxTTL < base {
			maxTTL = base
		}
		cooldown = base
		for i := 1; i < s.capacityFallbackStrikes; i++ {
			if cooldown >= maxTTL || cooldown > maxTTL/2 {
				cooldown = maxTTL
				break
			}
			cooldown *= 2
		}
		if cooldown > maxTTL {
			cooldown = maxTTL
		}
		if now.IsZero() {
			now = capacityFallbackNow()
		}
		s.capacityFallbackUntil = now.Add(cooldown)
	}
	capacityFallbackEventsTotal.Add(1)
	return s.capacityFallbackStrikes, cooldown
}

// evaluateCapacityFallbackLocked checks whether an active capacity fallback clamps sel.Tier or
// has expired and is eligible to probe recovery. Caller holds s.mu.
func (s *AcpProjectSession) evaluateCapacityFallbackLocked(sel ModelSelection, sid, prevModel string, now time.Time) (clampedTier string, stickyActive bool, startedProbe bool, probeFromTier string) {
	return s.evaluateCapacityFallbackWithIdleLocked(sel, sid, prevModel, now, s.LastUsedAt)
}

// evaluateCapacityFallbackWithIdleLocked evaluates capacity fallback using lastUsedAt captured
// before RunTurn overwrites s.LastUsedAt. Caller holds s.mu.
func (s *AcpProjectSession) evaluateCapacityFallbackWithIdleLocked(sel ModelSelection, sid, prevModel string, now, lastUsedAt time.Time) (clampedTier string, stickyActive bool, startedProbe bool, probeFromTier string) {
	nextTier := sel.Tier
	if nextTier == "" {
		nextTier = "medium"
	}
	if sel.Model != nil && sel.Model.FixedTier != "" {
		nextTier = sel.Model.FixedTier
	}
	if sel.Model == nil || sel.ExplicitTier || sid == "" || s.capacityFallbackSid == "" {
		return nextTier, false, false, ""
	}
	if s.capacityFallbackSid != sid || (sid != s.SessionID && sid != s.HibernatedSessionID) {
		return nextTier, false, false, ""
	}
	if sel.Model.ID != prevModel {
		return nextTier, false, false, ""
	}
	fbTier := s.capacityFallbackTier
	if fbTier == "" {
		fbTier = "medium"
	}
	if tierRank(nextTier) <= tierRank(fbTier) {
		return nextTier, false, false, ""
	}

	if now.IsZero() {
		now = capacityFallbackNow()
	}
	// Zero capacityFallbackUntil means non-expiring (opt-out TTL=0 or legacy direct field assignment).
	if s.capacityFallbackUntil.IsZero() || now.Before(s.capacityFallbackUntil) {
		return fbTier, true, false, ""
	}

	// Never probe while an upstream prompt is still in flight (e.g. parked MCP resume).
	if s.PromptInFlight {
		return fbTier, true, false, ""
	}

	// Hot-cache boundary gating: avoid busting a warm large-context KV cache mid-burst.
	wakingFromHibernate := s.SessionID == "" && s.HibernatedSessionID != ""
	idleCold := !lastUsedAt.IsZero() && now.Sub(lastUsedAt) >= 3*time.Minute
	ctxTok := s.LastCachedReadTokens
	if s.LastReportedContextUsed > ctxTok {
		ctxTok = s.LastReportedContextUsed
	}
	smallCtx := ctxTok < 48000
	lowerTierMissingCache := s.ZeroCacheStreak > 0
	deferCap := CapacityFallbackTTL
	if deferCap <= 0 {
		deferCap = 5 * time.Minute
	}
	deferCapReached := now.Sub(s.capacityFallbackUntil) >= deferCap

	if !(wakingFromHibernate || idleCold || smallCtx || lowerTierMissingCache || deferCapReached) {
		return fbTier, true, false, ""
	}

	prevFb := fbTier
	if prevFb == "low" && tierRank(nextTier) >= tierRank("high") && sel.Model.ID == "gemini-4-carbon" {
		// Step-up recovery for Carbon: probe medium first with a fresh baseTTL cooldown before high.
		s.capacityFallbackTier = "medium"
		if base := CapacityFallbackTTL; base > 0 {
			s.capacityFallbackUntil = now.Add(base)
		} else {
			s.capacityFallbackUntil = time.Time{}
		}
		s.capacityFallbackProbing = true
		capacityFallbackProbesTotal.Add(1)
		return "medium", true, true, prevFb
	}

	s.clearCapacityFallbackLocked(false)
	s.capacityFallbackProbing = true
	capacityFallbackProbesTotal.Add(1)
	return nextTier, false, true, prevFb
}

// capacityFallbackTelemetryLocked returns active capacity fallback metadata for dashboard/API snapshots.
// Caller holds s.mu.
func (s *AcpProjectSession) capacityFallbackTelemetryLocked(now time.Time) (tier string, remainingSec int, strikes int, reason string) {
	if s == nil || s.capacityFallbackSid == "" || s.capacityFallbackTier == "" {
		return "", 0, 0, ""
	}
	if s.capacityFallbackSid != s.SessionID && s.capacityFallbackSid != s.HibernatedSessionID {
		return "", 0, 0, ""
	}
	if now.IsZero() {
		now = capacityFallbackNow()
	}
	if !s.capacityFallbackUntil.IsZero() && s.capacityFallbackUntil.After(now) {
		rem := int(s.capacityFallbackUntil.Sub(now).Seconds())
		if rem < 1 {
			rem = 1
		}
		remainingSec = rem
	}
	return s.capacityFallbackTier, remainingSec, s.capacityFallbackStrikes, s.capacityFallbackReason
}

// applyModelSelection switches model / thinking tier on the live agy session (agy keeps the
// context). It returns an error when agy does not positively confirm the switch; the session's
// recorded model is then cleared so the next turn re-pins both values before prompting.
// While an ACP session/prompt is still in flight (e.g. parked_mcp_resume), upstream agy applies
// session/set_config_option only to future session/prompt RPCs; keep ThoughtLevel at the
// actually executing tier and track the confirmed next-prompt tier in PendingThoughtLevel.
func (s *AcpProjectSession) applyModelSelection(sel ModelSelection) error {
	nextModel := sel.Model.ID
	nextTier := sel.Tier
	if nextTier == "" {
		nextTier = "medium"
	}
	if sel.Model.FixedTier != "" {
		nextTier = sel.Model.FixedTier
	}
	s.mu.Lock()
	sid := s.SessionID
	inFlight := s.PromptInFlight
	prevModel, prevTier := s.ModelID, s.ThoughtLevel
	effectiveTier := prevTier
	if s.PendingThoughtLevel != "" {
		effectiveTier = s.PendingThoughtLevel
	}
	clampedTier, stickyFallback, startedProbe, probeFromTier := s.evaluateCapacityFallbackLocked(sel, sid, prevModel, capacityFallbackNow())
	if stickyFallback || startedProbe {
		nextTier = clampedTier
	} else if nextModel != prevModel || sel.ExplicitTier {
		s.clearCapacityFallbackLocked(true)
	}
	if sid == "" {
		if nextModel != prevModel && prevModel != "" {
			s.resetCacheBurnLocked()
			s.highTierZeroCacheStreak = 0
		}
		s.ModelID, s.ThoughtLevel, s.PendingThoughtLevel = nextModel, nextTier, ""
		s.mu.Unlock()
		return nil
	}
	if nextModel == prevModel && nextTier == effectiveTier {
		if !inFlight && s.PendingThoughtLevel != "" {
			s.ThoughtLevel = s.PendingThoughtLevel
			s.PendingThoughtLevel = ""
			s.highTierZeroCacheStreak = 0
		}
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()
	if startedProbe {
		LogEvent("CAPACITY_FALLBACK_PROBE", "Cooldown expired at safe boundary; probing higher thinking tier", map[string]any{
			"convKey":   s.ConvKey,
			"sessionId": sid,
			"model":     nextModel,
			"fromTier":  probeFromTier,
			"toTier":    nextTier,
		})
	}
	applied, err := setSessionModelAndTierWith(s.engine(), sid, sel.Model, nextTier, nextModel != prevModel, nextTier != effectiveTier || nextModel != prevModel)
	if err != nil {
		s.mu.Lock()
		if s.SessionID == sid && !s.PromptInFlight {
			s.ModelID, s.ThoughtLevel, s.PendingThoughtLevel = "", "", ""
		}
		s.mu.Unlock()
		LogEvent("MODEL_SWITCH_FAILED", fmt.Sprintf("Live session switch %s/%s -> %s/%s not confirmed", prevModel, effectiveTier, nextModel, nextTier), map[string]any{
			"convKey": s.ConvKey, "sessionId": sid, "model": nextModel, "thoughtLevel": nextTier, "error": err.Error(),
		})
		return &ModelSwitchError{From: prevModel + "/" + effectiveTier, To: nextModel + "/" + nextTier, Err: err}
	}
	s.mu.Lock()
	if nextModel != prevModel {
		s.resetCacheBurnLocked()
	}
	s.highTierZeroCacheStreak = 0
	queued := false
	activeTier := s.ThoughtLevel
	if s.PromptInFlight && nextModel == prevModel {
		if applied == s.ThoughtLevel {
			s.PendingThoughtLevel = ""
			s.pendingTierZeroCacheStreak = 0
		} else {
			if s.PendingThoughtLevel != applied {
				s.pendingTierZeroCacheStreak = 0
			}
			s.PendingThoughtLevel = applied
			queued = true
		}
	} else {
		s.ModelID, s.ThoughtLevel, s.PendingThoughtLevel = nextModel, applied, ""
		s.pendingTierZeroCacheStreak = 0
	}
	s.mu.Unlock()
	if queued {
		LogEvent("PARKED_TIER_SWITCH_QUEUED", fmt.Sprintf("Queued live session tier %s -> %s for next prompt (active prompt stays on %s)", activeTier, applied, activeTier), map[string]any{
			"convKey": s.ConvKey, "sessionId": sid, "model": nextModel, "activeTier": activeTier, "pendingThoughtLevel": applied,
		})
		return nil
	}
	LogEvent("MODEL_SWITCH", fmt.Sprintf("Switched live session model %s/%s -> %s/%s", prevModel, effectiveTier, nextModel, applied), map[string]any{
		"convKey": s.ConvKey, "sessionId": sid, "model": nextModel, "thoughtLevel": applied,
	})
	return nil
}

func (s *AcpProjectSession) hasActiveUnresolvedTurnLocked() bool {
	return s.activeTurn != nil && !s.activeTurn.IsResolved()
}

func (s *AcpProjectSession) pruneExpiredCompactionLeasesLocked(now time.Time) {
	if len(s.compactionLeases) == 0 {
		return
	}
	n := 0
	for _, l := range s.compactionLeases {
		if now.Before(l.expires) && l.sid != "" {
			s.compactionLeases[n] = l
			n++
		}
	}
	if n == 0 {
		s.compactionLeases = nil
	} else {
		s.compactionLeases = s.compactionLeases[:n]
	}
}

func (s *AcpProjectSession) addCompactionPendingLease(sid string, ttl time.Duration, markSidRollover bool) *compactionTargetRef {
	if s == nil || sid == "" {
		return nil
	}
	now := time.Now()
	s.mu.Lock()
	if s.SessionID != sid || s.closing || s.spawning {
		s.mu.Unlock()
		return nil
	}
	s.pruneExpiredCompactionLeasesLocked(now)
	tok := nextCompactionLeaseToken.Add(1)
	s.compactionLeases = append(s.compactionLeases, compactionPendingLease{
		token:   tok,
		sid:     sid,
		expires: now.Add(ttl),
	})
	s.mu.Unlock()
	return &compactionTargetRef{
		sess:            s,
		sid:             sid,
		token:           tok,
		markSidRollover: markSidRollover,
	}
}

func (s *AcpProjectSession) removeCompactionPendingLease(token uint64) {
	if s == nil || token == 0 {
		return
	}
	now := time.Now()
	s.mu.Lock()
	n := 0
	for _, l := range s.compactionLeases {
		if l.token != token && now.Before(l.expires) {
			s.compactionLeases[n] = l
			n++
		}
	}
	if n == 0 {
		s.compactionLeases = nil
	} else {
		s.compactionLeases = s.compactionLeases[:n]
	}
	s.mu.Unlock()
}

func (s *AcpProjectSession) hasActiveCompactionPendingLocked(now time.Time) bool {
	s.pruneExpiredCompactionLeasesLocked(now)
	if len(s.compactionLeases) == 0 {
		return false
	}
	activeSid := s.SessionID
	if activeSid == "" {
		activeSid = s.HibernatedSessionID
	}
	if activeSid == "" {
		return false
	}
	for _, l := range s.compactionLeases {
		if l.sid == activeSid && now.Before(l.expires) {
			return true
		}
	}
	return false
}

func (s *AcpProjectSession) isDisposableForEvictionLocked(now time.Time) bool {
	return s.ForceRolloverNext || s.hasActiveCompactionPendingLocked(now)
}

func (s *AcpProjectSession) supersedeAsCompactionTarget(sid string, markSidRollover bool) bool {
	if s == nil || sid == "" {
		return false
	}
	reason := "compaction_superseded"
	if markSidRollover {
		reason = "compaction_superseded_sid"
	}
	if s.HibernateIfSession(sid, reason) {
		return true
	}
	// Wait if the target session is mid-PhaseClosing (e.g. compaction_pending_relay_pause or
	// Pass-1b compaction_pending_preempt calling eng.CloseSession) so we never drop supersede.
	s.waitIfClosing(5 * time.Second)
	if s.HibernateIfSession(sid, reason) {
		return true
	}
	// Preempted / relay-paused branch: session was already hibernated with keepResumeState=true
	// while compaction was in flight; clear HibernatedSessionID and stale DB state now so Gate 3
	// and restarts never retain the superseded conversation.
	var releasedRefs []*compactionTargetRef
	s.mu.Lock()
	if s.SessionID != "" || s.closing || s.spawning || s.IsBusy || s.turnClaimed || s.HibernatedSessionID != sid {
		s.mu.Unlock()
		return false
	}
	s.HibernatedSessionID = ""
	s.clearHibernatedParkedCallsLocked()
	s.InitialAppendPrompt = ""
	s.lastCompletedReqFp = ""
	s.lastCompletedResult = TurnResult{}
	s.lastCompletedAt = time.Time{}
	if s.lastCompletedCompactionTarget != nil {
		releasedRefs = append(releasedRefs, s.lastCompletedCompactionTarget)
		s.lastCompletedCompactionTarget = nil
	}
	s.MessageCount = 0
	s.lastTurnAssistant = false
	s.HistoryFingerprint = ""
	s.cancelledHistoryFp = ""
	s.contentAnchors = sessionContentAnchors{}
	s.compactionLeases = nil
	s.clearCapacityFallbackLocked(true)
	s.lastTurnExplicitTier = false
	s.highTierZeroCacheStreak = 0
	s.resetCacheBurnLocked()
	if markSidRollover {
		s.ForceRolloverNext = true
		s.ForceRolloverReason = "rollover_compaction"
		s.lastCompactedAt = time.Now()
	}
	s.syncPhaseLocked()
	convKey := s.ConvKey
	s.mu.Unlock()
	for _, r := range releasedRefs {
		r.release()
	}
	s.persistSession()
	LogEvent("ACP_HIBERNATE", fmt.Sprintf("Cleared preempted superseded session state for %s", convKey), map[string]any{
		"reason":          reason,
		"closedSessionId": sid,
	})
	return true
}

func (s *AcpProjectSession) IsAvailableForNewTurn() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.isAvailableForNewTurnLocked()
}

func (s *AcpProjectSession) HibernateWorker(reason string, force bool) bool {
	return s.hibernateWorker(reason, force, "")
}

// HibernateIfSession force-closes the worker only if it still runs ACP session sid and no new
// request has claimed it. Delayed cleanup must use this so it can never close a session that a
// later turn opened or is using.
func (s *AcpProjectSession) HibernateIfSession(sid, reason string) bool {
	if sid == "" {
		return false
	}
	return s.hibernateWorker(reason, true, sid)
}

const hibernatedTurnHistoryMax = 16

func isMemoryGovernorHibernateReason(reason string) bool {
	return strings.HasPrefix(reason, "global_mem_") ||
		strings.HasPrefix(reason, "global_worker_cap") ||
		strings.HasPrefix(reason, "hibernate_lru") ||
		strings.HasPrefix(reason, "elastic_parked_offload")
}

// trimHibernatedTransientMemoryLocked releases transient per-session working memory that is
// not needed while hibernated, while preserving InitialSystemText, InitialAppendPrompt,
// InitialToolsCatalog, ToolsCatalog, ToolFingerprint, HibernatedSessionID, HibernatedParkedCallIDs,
// HistoryFingerprint, and contentAnchors for 100% lossless session/resume. Caller holds s.mu.
func (s *AcpProjectSession) trimHibernatedTransientMemoryLocked(reason string, keepResumeState bool) []*compactionTargetRef {
	var released []*compactionTargetRef
	s.RecentToolSigs = nil
	s.reportedToolCalls = nil
	s.LastDeliveredCallIDs = nil
	s.PendingSiblingCalls = nil
	s.abortedSiblingCalls = nil
	s.RelayedCalls = nil
	if reason != "waf_autobisect_reset" && (s.SystemText != "" || s.InitialSystemText != "") {
		s.RawSystemText = ""
	}
	if !s.IsBusy && !s.turnClaimed {
		s.pendingRawSystemText = ""
		s.pendingSystemText = ""
		s.hasPendingSystem = false
		if !keepResumeState || s.lastCompletedAt.IsZero() || time.Since(s.lastCompletedAt) >= 15*time.Second {
			s.lastCompletedReqFp = ""
			s.lastCompletedResult = TurnResult{}
			s.lastCompletedAt = time.Time{}
			if s.lastCompletedCompactionTarget != nil {
				released = append(released, s.lastCompletedCompactionTarget)
				s.lastCompletedCompactionTarget = nil
			}
		}
	}
	if len(s.TurnHistory) > hibernatedTurnHistoryMax {
		trimmed := make([]TurnHistoryEntry, hibernatedTurnHistoryMax)
		copy(trimmed, s.TurnHistory[len(s.TurnHistory)-hibernatedTurnHistoryMax:])
		s.TurnHistory = trimmed
	}
	return released
}

func (s *AcpProjectSession) hibernateWorker(reason string, force bool, expectSid string) bool {
	s.mu.Lock()
	pNow := s.syncPhaseLocked()
	if s.SessionID == "" || pNow == PhaseSpawning || pNow == PhaseClosing {
		s.mu.Unlock()
		return false
	}
	if expectSid != "" && (s.SessionID != expectSid || s.IsBusy || s.turnClaimed) {
		s.mu.Unlock()
		return false
	}
	if !force && !s.canPass1EvictLocked() {
		s.mu.Unlock()
		return false
	}
	if force && isMemoryGovernorHibernateReason(reason) && !s.canPass1EvictLocked() && !s.canPass2ParkedEvictLocked() {
		s.mu.Unlock()
		return false
	}
	keepResumeState := (!force ||
		strings.HasPrefix(reason, "reap_orphan_parked") ||
		reason == "parked_tier_degrade" ||
		reason == "compaction_pending_preempt" ||
		reason == "compaction_pending_relay_pause" ||
		isMemoryGovernorHibernateReason(reason)) &&
		s.MessageCount > 0 && !strings.HasPrefix(s.ConvKey, "util:")
	cancelPrompt := s.promptCancel
	if cancelPrompt != nil {
		s.promptGen++
	}
	s.promptCancel = nil
	s.promptLastActivityNs = nil
	s.promptStart = time.Time{}
	s.pendingTierZeroCacheStreak = 0
	s.parkedPromptFailedSid = ""
	var clearedTurn *activeTurnState
	var releasedRefs []*compactionTargetRef
	var pendingResultChans []chan map[string]any
	hasPendingParked := false
	if force {
		hasPendingParked = len(s.ParkedCalls) > 0 || len(s.RelayedCalls) > 0 || len(s.PendingSiblingCalls) > 0
		if keepResumeState && hasPendingParked {
			s.recordHibernatedParkedCallsLocked()
		}
		clearedTurn = s.activeTurn
		s.activeTurn = nil
		s.activeTurnReqFp = ""
		s.activeReqCancel = nil
		s.coalesceHandoff = false
		if !s.IsBusy && !s.turnClaimed && s.activeCompactionTarget != nil {
			releasedRefs = append(releasedRefs, s.activeCompactionTarget)
			s.activeCompactionTarget = nil
		}
		if s.detachedTurn != nil {
			if s.detachedTurn.graceTimer != nil {
				s.detachedTurn.graceTimer.Stop()
			}
			if s.detachedTurn.compactionTarget != nil {
				releasedRefs = append(releasedRefs, s.detachedTurn.compactionTarget)
				s.detachedTurn.compactionTarget = nil
			}
			s.detachedTurn = nil
		}
		// Detach pending MCP ResultChans under s.mu, but defer pushing error/cancel replies
		// until AFTER eng.CancelSession + eng.CloseSession complete when keepResumeState is true
		// (or whenever parked calls exist) so agy --worker can never receive "Session hibernated."
		// before process retirement and emit ghost tool steps or extra monitor calls.
		seenChan := make(map[chan map[string]any]bool, len(s.ParkedCalls)+len(s.PendingSiblingCalls)+len(s.RelayedCalls))
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
		for _, sib := range s.PendingSiblingCalls {
			if sib != nil {
				collectChan(sib.ResultChan)
			}
		}
		for _, rel := range s.RelayedCalls {
			if rel != nil {
				collectChan(rel.ResultChan)
			}
		}
		s.LastDeliveredCallIDs = nil
		s.PendingSiblingCalls = nil
		s.abortedSiblingCalls = nil
		s.RelayedCalls = nil
	}
	sid := s.SessionID
	prevGen := s.engineGen
	doneCh := make(chan struct{})
	s.beginCloseLocked(doneCh)
	s.orphanRelayHops = 0
	s.lastOrphanRelayAt = time.Time{}
	s.orphanCapLogged = false
	if strings.HasPrefix(reason, "reap_orphan_parked") {
		s.orphanCapStreak++
	}
	if keepResumeState {
		s.HibernatedSessionID = sid
	} else {
		s.HibernatedSessionID = ""
		s.clearHibernatedParkedCallsLocked()
		s.InitialAppendPrompt = ""
		s.lastCompletedReqFp = ""
		s.lastCompletedResult = TurnResult{}
		s.lastCompletedAt = time.Time{}
		if s.lastCompletedCompactionTarget != nil {
			releasedRefs = append(releasedRefs, s.lastCompletedCompactionTarget)
			s.lastCompletedCompactionTarget = nil
		}
		s.MessageCount = 0
		s.lastTurnAssistant = false
		s.HistoryFingerprint = ""
		s.cancelledHistoryFp = ""
		s.contentAnchors = sessionContentAnchors{}
		s.compactionLeases = nil
		s.clearCapacityFallbackLocked(true)
		s.lastTurnExplicitTier = false
		s.highTierZeroCacheStreak = 0
		s.resetCacheBurnLocked()
		switch reason {
		case "compaction_superseded_sid", "oneshot_compaction_summary_sid":
			s.ForceRolloverNext = true
			s.ForceRolloverReason = "rollover_compaction"
			s.lastCompactedAt = time.Now()
		case "oneshot_compaction_summary":
			s.ForceRolloverNext = false
			s.ForceRolloverReason = ""
			s.lastCompactedAt = time.Time{}
		}
	}
	if trimmedRefs := s.trimHibernatedTransientMemoryLocked(reason, keepResumeState); len(trimmedRefs) > 0 {
		releasedRefs = append(releasedRefs, trimmedRefs...)
	}
	s.mu.Unlock()
	if cancelPrompt != nil {
		cancelPrompt()
	}
	for _, r := range releasedRefs {
		r.release()
	}
	s.closeReadOnlyDB()
	if clearedTurn != nil {
		clearedTurn.mu.Lock()
		if clearedTurn.batchTimer != nil {
			clearedTurn.batchTimer.Stop()
		}
		clearedTurn.resolved = true
		clearedTurn.failed = true
		clearedTurn.resultConsumed = true
		if clearedTurn.releaseSem != nil {
			clearedTurn.releaseOnce.Do(clearedTurn.releaseSem)
		}
		clearedTurn.mu.Unlock()
		clearedTurn.CancelContext()
		clearedTurn.closeDelta()
		clearedTurn.CleanupSubscriber()
		select {
		case clearedTurn.errChan <- context.Canceled:
		default:
		}
	}

	var closeErr error
	eng := s.engine()
	if prevGen == 0 || prevGen == eng.CurrentGen() {
		if hasPendingParked {
			eng.CancelSession(sid)
		}
		closeErr = eng.CloseSession(sid)
	} else {
		eng.UnregisterSession(sid)
	}
	for _, ch := range pendingResultChans {
		select {
		case ch <- map[string]any{
			"content": []map[string]any{{"type": "text", "text": "Session hibernated."}},
			"isError": true,
		}:
		default:
		}
	}
	s.mu.Lock()
	restoreSid := ""
	// If force && hasPendingParked, MCP channels were already interrupted and CancelSession was sent;
	// do NOT roll back to a live session on CloseSession error (keep hibernated state & HibernatedParkedCalls).
	forcedDisconnectOnCloseErr := closeErr != nil && force && hasPendingParked
	if closeErr != nil && !forcedDisconnectOnCloseErr {
		restoreSid = sid
	}
	s.finishCloseLocked(restoreSid)
	close(doneCh)
	s.mu.Unlock()
	if forcedDisconnectOnCloseErr {
		eng.UnregisterSession(sid)
		LogEvent("ACP_CLOSE_ERR", "Forced parked session to hibernated after CloseSession error (MCP channels already interrupted)", map[string]any{
			"convKey":   s.ConvKey,
			"reason":    reason,
			"sessionId": sid,
			"error":     closeErr.Error(),
		})
	} else if closeErr != nil {
		LogEvent("ACP_CLOSE_ERR", "Worker slot retained after close failure", nil)
		return false
	}
	notifyGlobalWorkerSlotFreed()
	s.persistSession()
	// Release cached serialized tool JSON strings while hibernated (recomputed lazily on next persist).
	s.mu.Lock()
	if s.SessionID == "" && !s.IsBusy && !s.spawning {
		s.cachedInitToolsJSON = ""
		s.cachedInitToolsPtr = nil
		s.cachedInitToolsLen = 0
		s.cachedInitToolsFp = ""
		s.cachedCurToolsJSON = ""
		s.cachedCurToolsPtr = nil
		s.cachedCurToolsLen = 0
		s.cachedCurToolsFp = ""
	}
	s.mu.Unlock()
	LogEvent("ACP_HIBERNATE", fmt.Sprintf("Closed idle session in shared engine for %s", s.ConvKey), map[string]any{
		"reason":          reason,
		"closedSessionId": sid,
	})
	return true
}

type reportedToolIdentity struct {
	kind       string
	serverName string
	toolName   string
	name       string
	conflict   bool
}

func stringFromMap(m map[string]any, keys ...string) string {
	if m == nil {
		return ""
	}
	for _, k := range keys {
		if s, ok := m[k].(string); ok {
			if trimmed := strings.TrimSpace(s); trimmed != "" {
				return trimmed
			}
		}
	}
	return ""
}

func consistentNonEmpty(a, b string) (string, bool) {
	if a != "" && b != "" && a != b {
		return a, false
	}
	if a != "" {
		return a, true
	}
	return b, true
}

func isBridgeServerName(srv string) bool {
	return srv == McpBridgeName || srv == LegacyMcpBridgeName
}

func parseBridgeMcpPrefix(raw string) (srv, tool string, ok bool) {
	switch {
	case strings.HasPrefix(raw, "mcp_"+McpBridgeName+"_"):
		return McpBridgeName, strings.TrimPrefix(raw, "mcp_"+McpBridgeName+"_"), true
	case strings.HasPrefix(raw, "mcp_"+LegacyMcpBridgeName+"_"):
		return LegacyMcpBridgeName, strings.TrimPrefix(raw, "mcp_"+LegacyMcpBridgeName+"_"), true
	default:
		return "", "", false
	}
}

func validateDshBridgeIdentity(id *reportedToolIdentity) {
	if id == nil {
		return
	}
	if srv, parsedTool, ok := parseBridgeMcpPrefix(id.name); ok {
		if parsedTool == "" {
			id.conflict = true
			return
		}
		if id.serverName != "" && id.serverName != srv {
			id.conflict = true
		}
		if id.toolName != "" && id.toolName != parsedTool {
			id.conflict = true
		}
		id.serverName = srv
		id.toolName = parsedTool
	} else if strings.HasPrefix(id.name, "mcp_") && isBridgeServerName(id.serverName) {
		id.conflict = true
	}
	if srv, trimmed, ok := parseBridgeMcpPrefix(id.toolName); ok && (id.serverName == "" || id.serverName == srv) {
		if trimmed == "" {
			id.conflict = true
			return
		}
		id.serverName = srv
		id.toolName = trimmed
	}
	if isBridgeServerName(id.serverName) && id.toolName == "" && (strings.HasPrefix(id.name, "ws_") || strings.HasPrefix(id.name, "dsh_")) {
		id.toolName = id.name
	}
	if isBridgeServerName(id.serverName) && id.name != "" {
		if id.toolName == "" || (id.name != id.toolName && id.name != "mcp_"+id.serverName+"_"+id.toolName) {
			id.conflict = true
		}
	}
}

// extractToolIdentity reads ONLY structured tool identity fields (never model-controlled rawInput
// or human display title) from an ACP session/update or session/request_permission carrier.
func extractToolIdentity(carrier map[string]any) (string, reportedToolIdentity) {
	if carrier == nil {
		return "", reportedToolIdentity{}
	}
	tcID := stringFromMap(carrier, "toolCallId", "id")
	sub, _ := carrier["toolCall"].(map[string]any)
	if tcID == "" && sub != nil {
		tcID = stringFromMap(sub, "toolCallId", "id")
	}

	readOne := func(m map[string]any) reportedToolIdentity {
		if m == nil {
			return reportedToolIdentity{}
		}
		var out reportedToolIdentity
		out.kind = stringFromMap(m, "kind")
		meta, _ := m["_meta"].(map[string]any)
		agMeta, _ := meta["antigravity"].(map[string]any)

		s1 := stringFromMap(m, "mcpServerName", "serverName")
		s2, okS2 := consistentNonEmpty(stringFromMap(meta, "mcpServerName", "serverName", "server"), stringFromMap(agMeta, "mcpServerName", "serverName", "server"))
		srv, okSrv := consistentNonEmpty(s1, s2)
		if !okS2 || !okSrv {
			out.conflict = true
		}
		out.serverName = srv

		t1 := stringFromMap(m, "mcpToolName")
		t2, okT2 := consistentNonEmpty(stringFromMap(meta, "mcpToolName"), stringFromMap(agMeta, "mcpToolName"))
		tl, okTl := consistentNonEmpty(t1, t2)
		if !okT2 || !okTl {
			out.conflict = true
		}

		n1 := stringFromMap(m, "name")
		n2, okN2 := consistentNonEmpty(stringFromMap(meta, "name"), stringFromMap(agMeta, "name"))
		nm, okNm := consistentNonEmpty(n1, n2)
		if !okN2 || !okNm {
			out.conflict = true
		}

		genericTool, okGT := consistentNonEmpty(
			stringFromMap(m, "toolName"),
			func() string {
				v, ok := consistentNonEmpty(stringFromMap(meta, "toolName", "tool"), stringFromMap(agMeta, "toolName", "tool"))
				if !ok {
					out.conflict = true
				}
				return v
			}(),
		)
		if !okGT {
			out.conflict = true
		}
		if genericTool != "" {
			if out.serverName != "" || strings.HasPrefix(genericTool, "ws_") || strings.HasPrefix(genericTool, "dsh_") {
				mergedTl, ok := consistentNonEmpty(tl, genericTool)
				if !ok {
					out.conflict = true
				}
				tl = mergedTl
			} else {
				mergedNm, ok := consistentNonEmpty(nm, genericTool)
				if !ok {
					out.conflict = true
				}
				nm = mergedNm
			}
		}

		out.toolName = tl
		out.name = nm
		validateDshBridgeIdentity(&out)
		return out
	}

	top := readOne(carrier)
	if sub != nil {
		top = mergeReportedToolIdentity(top, readOne(sub))
	}
	return tcID, top
}

func mergeReportedToolIdentity(prev, next reportedToolIdentity) reportedToolIdentity {
	var out reportedToolIdentity
	out.conflict = prev.conflict || next.conflict
	var ok bool
	switch {
	case prev.kind == "other" && next.kind != "":
		out.kind = next.kind
	case next.kind == "other" && prev.kind != "":
		out.kind = prev.kind
	default:
		if out.kind, ok = consistentNonEmpty(prev.kind, next.kind); !ok {
			out.conflict = true
		}
	}
	if out.serverName, ok = consistentNonEmpty(prev.serverName, next.serverName); !ok {
		out.conflict = true
	}
	if out.toolName, ok = consistentNonEmpty(prev.toolName, next.toolName); !ok {
		out.conflict = true
	}
	if prev.name != "" && next.name != "" && prev.name != next.name && isBridgeServerName(out.serverName) && stripBridgeMcpPrefix(prev.name) == stripBridgeMcpPrefix(next.name) {
		out.name = stripBridgeMcpPrefix(prev.name)
	} else if out.name, ok = consistentNonEmpty(prev.name, next.name); !ok {
		out.conflict = true
	}
	validateDshBridgeIdentity(&out)
	return out
}

func (s *AcpProjectSession) recordReportedToolUpdate(u map[string]any) {
	tcID, id := extractToolIdentity(u)
	if tcID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.PromptInFlight {
		return
	}
	if s.reportedToolCalls == nil {
		s.reportedToolCalls = make(map[string]reportedToolIdentity)
	}
	if prev, exists := s.reportedToolCalls[tcID]; exists {
		s.reportedToolCalls[tcID] = mergeReportedToolIdentity(prev, id)
	} else {
		s.reportedToolCalls[tcID] = id
	}
}

func (s *AcpProjectSession) authorizeToolCallPermission(tc map[string]any) (bool, string) {
	tcID, extracted := extractToolIdentity(tc)
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.PromptInFlight {
		return false, "no_prompt_in_flight"
	}
	if s.activeTurn == nil && len(s.ParkedCalls) == 0 && len(s.RelayedCalls) == 0 {
		return false, "turn_cancelled_or_inactive"
	}
	if tcID != "" {
		if prev, exists := s.reportedToolCalls[tcID]; exists {
			extracted = mergeReportedToolIdentity(prev, extracted)
		}
		if s.reportedToolCalls == nil {
			s.reportedToolCalls = make(map[string]reportedToolIdentity)
		}
		s.reportedToolCalls[tcID] = extracted
	}
	if extracted.conflict {
		return false, "conflicting_tool_identity"
	}
	if isBridgeServerName(extracted.serverName) && extracted.toolName != "" {
		if hostName, ok := resolveAllowedHostTool(s.McpToHostMap, extracted.toolName); ok {
			if (hostName == AwaitToolName || hostName == LegacyAwaitToolName) && (len(s.ParkedCalls) > 0 || len(s.RelayedCalls) > 0) {
				return true, "ws_bridge_internal_await"
			}
		}
	}
	if s.activeTurnToolChoice.Mode == "none" {
		return false, "tool_choice_none"
	}
	if extracted.serverName == "" && extracted.toolName == "" && (extracted.name == "search_web" || extracted.name == "read_url_content") {
		if len(s.activeTurnToolChoice.Allowed) > 0 && !s.activeTurnToolChoice.Allowed[extracted.name] {
			return false, "tool_choice_restricted"
		}
		return true, "builtin_web_tool"
	}
	if isBridgeServerName(extracted.serverName) && extracted.toolName != "" {
		if hostName, ok := resolveAllowedHostTool(s.McpToHostMap, extracted.toolName); ok {
			if len(s.activeTurnToolChoice.Allowed) > 0 && !isInternalBridgeTool(hostName) && !s.activeTurnToolChoice.Allowed[hostName] {
				return false, "tool_choice_restricted"
			}
			return true, "ws_bridge_mcp_tool"
		}
		return false, "unoffered_mcp_tool"
	}
	return false, "unauthorized_tool"
}

func (s *AcpProjectSession) ownsToolCall(tc map[string]any) bool {
	allow, _ := s.authorizeToolCallPermission(tc)
	return allow
}

func replyPermissionOutcome(rpcID any, options []acp.PermissionOption, allow bool) {
	replyPermissionOutcomeWithEngine(GlobalEngine, rpcID, options, allow)
}

func replyPermissionOutcomeWithEngine(eng acp.Engine, rpcID any, options []acp.PermissionOption, allow bool) {
	if eng == nil {
		eng = GlobalEngine
	}
	if allow {
		chosenID := ""
		for _, o := range options {
			if o.Kind == "allow_once" {
				chosenID = o.OptionID
				break
			}
		}
		if chosenID == "" {
			for _, o := range options {
				if o.Kind == "allow_always" {
					chosenID = o.OptionID
					break
				}
			}
		}
		if chosenID == "" {
			chosenID = "allow_once"
		}
		eng.EnqueueControlReply(map[string]any{
			"jsonrpc": "2.0",
			"id":      rpcID,
			"result": map[string]any{
				"outcome": map[string]any{"outcome": "selected", "optionId": chosenID},
			},
		})
		return
	}
	chosenReject := ""
	for _, o := range options {
		if o.Kind == "reject_once" {
			chosenReject = o.OptionID
			break
		}
	}
	if chosenReject == "" {
		for _, o := range options {
			if o.Kind == "reject_always" {
				chosenReject = o.OptionID
				break
			}
		}
	}
	if chosenReject != "" {
		eng.EnqueueControlReply(map[string]any{
			"jsonrpc": "2.0",
			"id":      rpcID,
			"result": map[string]any{
				"outcome": map[string]any{"outcome": "selected", "optionId": chosenReject},
			},
		})
		return
	}
	eng.EnqueueControlReply(map[string]any{
		"jsonrpc": "2.0",
		"id":      rpcID,
		"result": map[string]any{
			"outcome": map[string]any{"outcome": "cancelled"},
		},
	})
}

func (s *AcpProjectSession) handlePermissionRequest(rpcID any, options []acp.PermissionOption, tc map[string]any) {
	allow, reason := s.authorizeToolCallPermission(tc)
	if !allow {
		tcID, id := extractToolIdentity(tc)
		s.mu.Lock()
		sid := s.SessionID
		s.mu.Unlock()
		LogEvent("PERMISSION_REJECTED", "Rejected ACP tool permission request ("+reason+")", map[string]any{
			"convKey":    s.ConvKey,
			"sessionId":  sid,
			"reason":     reason,
			"toolCallId": tcID,
			"kind":       id.kind,
			"serverName": id.serverName,
			"toolName":   id.toolName,
			"name":       id.name,
		})
	}
	if allow && reason != "ws_bridge_internal_await" {
		s.mu.Lock()
		turn := s.activeTurn
		s.mu.Unlock()
		if turn != nil && !turn.IsResolved() {
			s.touchPromptActivity()
		}
	}
	replyPermissionOutcomeWithEngine(s.engine(), rpcID, options, allow)
}

func (s *AcpProjectSession) CancelActiveTurn(reason string) {
	s.mu.Lock()
	sid := s.SessionID
	turn := s.activeTurn
	det := s.detachedTurn
	var detCompRef *compactionTargetRef
	if det != nil && det.compactionTarget != nil {
		detCompRef = det.compactionTarget
		det.compactionTarget = nil
	}
	s.activeTurn = nil
	s.detachedTurn = nil
	s.wasCancelledTurn = true
	for k, p := range s.ParkedCalls {
		select {
		case p.ResultChan <- map[string]any{
			"content": []map[string]any{{"type": "text", "text": "Request cancelled by client."}},
			"isError": true,
		}:
		default:
		}
		delete(s.ParkedCalls, k)
	}
	for _, sib := range s.PendingSiblingCalls {
		select {
		case sib.ResultChan <- map[string]any{
			"content": []map[string]any{{"type": "text", "text": "Request cancelled by client."}},
			"isError": true,
		}:
		default:
		}
	}
	for _, rel := range s.RelayedCalls {
		select {
		case rel.ResultChan <- map[string]any{
			"content": []map[string]any{{"type": "text", "text": "Request cancelled by client."}},
			"isError": true,
		}:
		default:
		}
	}
	s.LastDeliveredCallIDs = nil
	s.PendingSiblingCalls = nil
	s.abortedSiblingCalls = nil
	s.RelayedCalls = nil
	s.reportedToolCalls = nil
	s.activeTurnToolChoice = TurnToolChoice{}
	s.ParkedSince = time.Time{}
	s.syncPhaseLocked()
	s.mu.Unlock()
	detCompRef.release()

	if det != nil && det.graceTimer != nil {
		det.graceTimer.Stop()
	}
	cleanupTurn := func(t *activeTurnState) {
		if t == nil {
			return
		}
		t.mu.Lock()
		if t.batchTimer != nil {
			t.batchTimer.Stop()
		}
		t.resolved = true
		t.failed = true
		t.resultConsumed = true
		if t.releaseSem != nil {
			t.releaseOnce.Do(t.releaseSem)
		}
		t.mu.Unlock()
		t.CancelContext()
		t.closeDelta()
		t.CleanupSubscriber()
		select {
		case t.errChan <- context.Canceled:
		default:
		}
	}
	cleanupTurn(turn)
	if det != nil && det.turn != nil && det.turn != turn {
		cleanupTurn(det.turn)
	}

	if sid != "" {
		s.engine().CancelSession(sid)
		LogEvent("SESSION_CANCEL", fmt.Sprintf("Sent session/cancel to ACP engine (%s)", reason), map[string]any{
			"convKey":   s.ConvKey,
			"sessionId": sid,
		})
	}
}

func (s *AcpProjectSession) handleSessionUpdate(u map[string]any) {
	if u == nil {
		return
	}
	updType, _ := u["sessionUpdate"].(string)
	if updType == "tool_call" || updType == "tool_call_update" {
		s.recordReportedToolUpdate(u)
		return
	}
	if updType == "usage_update" {
		used, okU := toInt(u["used"])
		size, okS := toInt(u["size"])
		if subU, okM := u["usage"].(map[string]any); okM {
			if !okU {
				used, okU = toInt(subU["used"])
			}
			if !okS {
				size, okS = toInt(subU["size"])
			}
		}
		if (okU && used >= 0) || (okS && size > 0) {
			s.mu.Lock()
			if okU && used >= 0 {
				s.LastReportedContextUsed = used
			}
			if okS && size > 0 {
				s.LastReportedContextSize = size
			}
			s.mu.Unlock()
		}
		return
	}
	content, _ := u["content"].(map[string]any)
	text, _ := content["text"].(string)
	s.HandleSessionTextChunk(updType, text)
}

// HandleSessionTextChunk processes streaming text and thought delta chunks without map allocation.
func (s *AcpProjectSession) HandleSessionTextChunk(updType, text string) {
	if text == "" {
		return
	}
	s.mu.Lock()
	turn := s.activeTurn
	s.mu.Unlock()
	if turn == nil {
		return
	}

	if updType == "agent_message_chunk" {
		turn.mu.Lock()
		prevInfo := turn.noticeInfo
		text = turn.acceptTextLocked(text)
		droppedInfo := turn.noticeInfo > prevInfo
		retryAttempt := turn.lastRetryAttempt
		retryDetail := turn.lastRetryDetail
		retryCb := turn.onRetryNotice
		canFastCancel := !turn.emittedText && len(turn.thoughtChks) == 0 && len(turn.toolCalls) == 0 && !turn.fastCancelledForBisect && !turn.resolved
		if text != "" {
			now := time.Now()
			if turn.firstChunk.IsZero() {
				turn.firstChunk = now
			}
			if turn.firstTextAt.IsZero() {
				turn.firstTextAt = now
			}
			turn.textChunks = append(turn.textChunks, text)
		}
		turn.mu.Unlock()
		if droppedInfo {
			meta := map[string]any{"convKey": s.ConvKey}
			if retryAttempt > 0 {
				meta["attempt"] = retryAttempt
			}
			if retryDetail != "" {
				meta["detail"] = retryDetail
			}
			LogEvent("RETRY_NOTICE_FILTERED", "Dropped internal agy retry/limit/cancel notice", meta)
			if canFastCancel && retryCb != nil && retryAttempt > 0 {
				retryCb(retryAttempt, retryDetail)
			}
		}
		if text != "" {
			if !turn.IsResolved() {
				s.touchPromptActivity()
			}
			turn.pushDelta(DeltaChunk{Type: "text", Text: text})
		} else if droppedInfo && !turn.IsResolved() {
			s.touchPromptActivity()
		}
	} else if updType == "agent_thought_chunk" {
		turn.mu.Lock()
		now := time.Now()
		if turn.firstChunk.IsZero() {
			turn.firstChunk = now
		}
		if turn.firstThoughtAt.IsZero() {
			turn.firstThoughtAt = now
		}
		turn.lastThoughtAt = now
		turn.thoughtChks = append(turn.thoughtChks, text)
		resolvedNow := turn.resolved
		turn.mu.Unlock()
		if !resolvedNow {
			s.touchPromptActivity()
		}
		turn.pushDelta(DeltaChunk{Type: "thinking", Thinking: text})
	}
}

func (s *AcpProjectSession) waitIfClosing(maxWait time.Duration) {
	s.mu.Lock()
	ch := s.closingDone
	s.mu.Unlock()
	if ch == nil {
		return
	}
	t := time.NewTimer(maxWait)
	defer t.Stop()
	select {
	case <-ch:
	case <-t.C:
	}
}

func (s *AcpProjectSession) ensureBooted(toolsCatalog []ToolDef, thoughtLevel string) error {
	eng := s.engine()
	if err := eng.EnsureStarted(); err != nil {
		return err
	}
	if len(toolsCatalog) > 0 && !eng.Capabilities().McpHTTP {
		return errors.New("upstream does not advertise required HTTP MCP transport")
	}
	s.waitIfClosing(5 * time.Second)
	curGen := eng.CurrentGen()
	demotedStaleGen := false
	s.mu.Lock()
	if s.SessionID != "" && s.engineGen != 0 && curGen != 0 && s.engineGen != curGen {
		staleSid := s.SessionID
		hibSid := ""
		if s.MessageCount > 0 && !strings.HasPrefix(s.ConvKey, "util:") {
			hibSid = staleSid
		}
		s.demoteToHibernatedLocked(hibSid)
		demotedStaleGen = true
	}
	sid := s.SessionID
	hibSid := s.HibernatedSessionID
	hibParked := s.HibernatedParkedCallIDs
	hibParkedMeta := s.HibernatedParkedCalls
	hasBridge := len(s.DeclaredMcpTools) > 0
	oldFp := s.ToolFingerprint
	oldCat := s.ToolsCatalog
	msgCnt := s.MessageCount
	cb := s.onBeforeSpawn
	closing := s.closing
	s.mu.Unlock()
	if demotedStaleGen {
		s.closeReadOnlyDB()
	}
	if closing {
		return &SoftError{StatusCode: 529, ErrorType: "overloaded_error", Message: "Session is closing; retry shortly"}
	}

	if sid == "" {
		if cb != nil {
			if err := cb(s); err != nil {
				return err
			}
			defer func() { s.mu.Lock(); s.endSpawnLocked(false); s.mu.Unlock() }()
		}
		needNew := toolCatalogNeedsRollover(hasBridge, oldCat, oldFp, toolsCatalog)
		if hibSid != "" && msgCnt > 0 && !needNew {
			err := s.reloadHibernatedSession(hibSid, toolsCatalog, thoughtLevel)
			if err == nil {
				return nil
			}
			var msErr *ModelSwitchError
			if errors.As(err, &msErr) {
				s.mu.Lock()
				if s.SessionID == "" {
					s.HibernatedSessionID = hibSid
					s.HibernatedParkedCallIDs = hibParked
					s.HibernatedParkedCalls = hibParkedMeta
					s.LastWakeMethod = ""
				}
				s.mu.Unlock()
				return err
			}
			s.mu.Lock()
			s.HibernatedSessionID = ""
			s.clearHibernatedParkedCallsLocked()
			s.MessageCount = 0
			s.lastTurnAssistant = false
			s.HistoryFingerprint = ""
			s.cancelledHistoryFp = ""
			s.mu.Unlock()
		}
		return s.openFreshSession(toolsCatalog, thoughtLevel)
	}
	return nil
}

func (s *AcpProjectSession) buildAppendSystemPrompt(toolsCatalog []ToolDef) string {
	s.mu.Lock()
	sysText := s.SystemText
	s.mu.Unlock()
	return s.buildAppendSystemPromptWithText(toolsCatalog, sysText)
}

func (s *AcpProjectSession) buildAppendSystemPromptWithText(toolsCatalog []ToolDef, sysText string) string {
	var toolMappingSummary string
	if len(toolsCatalog) > 0 {
		bridgeSrv := sessionBridgeServerName(toolsCatalog)
		toolPfx := "ws"
		if bridgeSrv == LegacyMcpBridgeName {
			toolPfx = "dsh"
		}
		lines := []string{
			"CRITICAL TOOL ROUTING RULES:",
			fmt.Sprintf("1. The external host workspace owns all tools via the `%s` MCP server.", bridgeSrv),
			"2. NEVER invoke built-in local file/shell tools (`read_file`, `write_file`, `edit_file`, `run_command`) directly. For web search, use the built-in `search_web` tool directly.",
			"3. When reading files with `Read`, read the whole file at once (or up to `limit: 2000` lines per call); do NOT slice files into small 150-220 line chunks. Do not re-`Read` unchanged files already in context; use `Grep` or `Glob` first before reading multiple files.",
			fmt.Sprintf("4. Always invoke the eager `mcp_%s_%s_<ToolName>_<hash>` tools mapped below:", bridgeSrv, toolPfx),
		}
		for _, t := range toolsCatalog {
			var keys []string
			if props, ok := t.Parameters["properties"].(map[string]any); ok {
				for k := range props {
					keys = append(keys, k)
				}
				sort.Strings(keys)
			}
			lines = append(lines, fmt.Sprintf("- %s -> mcp_%s_%s (keys: %s)", t.OutboundName(), bridgeSrv, t.McpName, strings.Join(keys, ", ")))
		}
		toolMappingSummary = strings.Join(lines, "\n")
	} else {
		toolMappingSummary = "No external workspace tools are attached. You may use the built-in `search_web` tool whenever web search is helpful."
	}
	s.mu.Lock()
	skipReprune := s.skipSystemReprune
	cwd := s.Cwd
	codec := s.Codec
	s.mu.Unlock()

	var customSys string
	if skipReprune {
		customSys = strings.TrimSpace(sysText)
	} else {
		customSys = PruneClientSystemPrompt(sysText)
	}
	if customSys == "" && cwd != "" {
		customSys = "Primary working directory: " + cwd
	}
	appendPrompt := toolMappingSummary
	if customSys != "" {
		appendPrompt += "\n\n[Workspace Context]\n" + customSys
	}
	if codec != nil {
		appendPrompt = codec.EncodeOutboundText(appendPrompt)
	}
	return appendPrompt
}

func (s *AcpProjectSession) reloadHibernatedSession(hibSid string, toolsCatalog []ToolDef, thoughtLevel string) error {
	eng := s.engine()
	if len(toolsCatalog) > 0 && !eng.Capabilities().McpHTTP {
		return errors.New("upstream does not advertise required HTTP MCP transport")
	}
	s.mu.Lock()
	s.ParkedCalls = make(map[string]*ParkedMcpCall)
	s.LastDeliveredCallIDs = nil
	s.PendingSiblingCalls = nil
	s.abortedSiblingCalls = nil
	s.RelayedCalls = nil
	s.reportedToolCalls = nil
	if s.InitialToolsCatalog == nil && (s.ToolFingerprint == "" || s.ToolFingerprint == toolCatalogFingerprint(toolsCatalog) || s.ToolFingerprint == legacyToolFingerprint(toolsCatalog)) {
		// Legacy session restored from gateway.db without init_tools_json whose fingerprint matched toolsCatalog.
		s.InitialToolsCatalog = append([]ToolDef(nil), toolsCatalog...)
		s.DeclaredMcpTools = make(map[string]bool, len(toolsCatalog))
		for _, t := range toolsCatalog {
			s.DeclaredMcpTools[t.Name] = true
		}
		s.ToolsCatalog = append([]ToolDef(nil), toolsCatalog...)
		s.McpToHostMap = make(map[string]string, len(toolsCatalog))
		for _, t := range toolsCatalog {
			s.McpToHostMap[t.McpName] = t.Name
		}
		s.ToolFingerprint = toolCatalogFingerprint(toolsCatalog)
	}
	if s.Codec != nil {
		s.Codec.SeedFromToolsCatalog(s.InitialToolsCatalog)
		s.Codec.SeedFromToolsCatalog(toolsCatalog)
	}
	initCat := s.InitialToolsCatalog
	frozenAppend := s.InitialAppendPrompt
	initSys := s.InitialSystemText
	if initSys == "" {
		initSys = s.SystemText
	}
	tenantURL := fmt.Sprintf("http://127.0.0.1:%d/mcp/%s", GatewayPort, s.TenantKey)
	modelID := s.ModelID
	s.mu.Unlock()

	mcpServers := make([]map[string]any, 0)
	if len(initCat) > 0 {
		mcpServers = append(mcpServers, map[string]any{
			"name": sessionBridgeServerName(initCat),
			"type": "http",
			"url":  tenantURL,
			"headers": []map[string]any{
				{"name": "Authorization", "value": "Bearer " + s.TenantKey},
			},
		})
	}

	// Replay the exact session/new append prompt verbatim so upstream SetTurnOverlay on
	// session/resume keeps byte-0 of SystemInstruction identical even after mid-turn system edits.
	appendPrompt := frozenAppend
	if appendPrompt == "" {
		appendPrompt = s.buildAppendSystemPromptWithText(initCat, initSys)
		s.mu.Lock()
		s.InitialAppendPrompt = appendPrompt
		if s.InitialSystemText == "" {
			s.InitialSystemText = initSys
		}
		s.mu.Unlock()
	}
	t0 := time.Now()
	params := map[string]any{
		"sessionId":  hibSid,
		"cwd":        s.Cwd,
		"mcpServers": mcpServers,
		"_meta": map[string]any{
			"antigravity": map[string]any{
				"systemPrompt": map[string]any{"append": appendPrompt},
			},
		},
	}
	caps := eng.Capabilities()
	wakeMethod := ""
	var rawWakeRes json.RawMessage
	var err error = errors.New("upstream does not advertise session recovery")
	if caps.Resume {
		wakeMethod = "session/resume"
		rawWakeRes, err = eng.RPC(wakeMethod, params, 60*time.Second)
	}
	if err != nil && caps.Load {
		wakeMethod = "session/load"
		rawWakeRes, err = eng.RPC(wakeMethod, params, 90*time.Second)
	}
	if err != nil {
		return err
	}
	rpcWakeMs := time.Since(t0).Milliseconds()

	s.mu.Lock()
	prevHibParked := s.HibernatedParkedCallIDs
	prevHibMeta := s.HibernatedParkedCalls
	s.SessionID = hibSid
	s.HibernatedSessionID = ""
	s.clearHibernatedParkedCallsLocked()
	s.LastWakeMethod = wakeMethod
	s.syncPhaseLocked()
	needGenWatermark := s.LastSeenGenMetaIdx < 0
	needStepWatermark := s.LastSeenDbStepIdx < 0
	s.mu.Unlock()

	if needGenWatermark {
		if maxGen := s.readMaxSessionGenMetadataIdx(hibSid); maxGen >= 0 {
			s.mu.Lock()
			if s.LastSeenGenMetaIdx < maxGen {
				s.LastSeenGenMetaIdx = maxGen
			}
			s.mu.Unlock()
		}
	}
	if maxStep := s.readMaxSessionStepIdx(hibSid); maxStep >= 0 {
		s.mu.Lock()
		if needStepWatermark && s.LastSeenDbStepIdx < maxStep {
			s.LastSeenDbStepIdx = maxStep
		}
		if s.LastPlannerBatchIdx < maxStep {
			s.LastPlannerBatchIdx = maxStep
		}
		s.LastPlannerBatchNextPos = 0
		s.LastPlannerBatchTotal = 0
		s.mu.Unlock()
	} else {
		s.mu.Lock()
		s.LastPlannerBatchNextPos = 0
		s.LastPlannerBatchTotal = 0
		s.mu.Unlock()
	}

	eng.RegisterListener(hibSid, s)
	mInfo := ModelOrDefault(modelID)
	tPin := time.Now()
	appliedTier, curOpts, err := s.pinSessionConfigWithInitial(hibSid, mInfo, thoughtLevel, rawWakeRes)
	if err != nil {
		s.mu.Lock()
		if s.SessionID == "" && s.HibernatedSessionID == "" {
			s.HibernatedSessionID = hibSid
			s.HibernatedParkedCallIDs = prevHibParked
			s.HibernatedParkedCalls = prevHibMeta
			s.syncPhaseLocked()
		}
		s.mu.Unlock()
		return err
	}
	if UseG1Credits && (curOpts == nil || curOpts["use_g1_credits"] != "true") {
		_ = setAndVerifySessionConfigOptionWith(eng, hibSid, "use_g1_credits", "true", configOptionTimeout)
	}
	pinMs := time.Since(tPin).Milliseconds()
	s.mu.Lock()
	s.ModelID = mInfo.ID
	s.ThoughtLevel = appliedTier
	s.PendingThoughtLevel = ""
	s.mu.Unlock()
	s.persistSessionAsync()

	evCat := "SESSION_RESUME"
	if wakeMethod == "session/load" {
		evCat = "SESSION_LOAD"
	}
	LogEvent(evCat, fmt.Sprintf("Woke hibernated ACP session via %s; cache hit depends on upstream usage", wakeMethod), map[string]any{
		"convKey":      s.ConvKey,
		"sessionId":    hibSid,
		"method":       wakeMethod,
		"model":        mInfo.ID,
		"thoughtLevel": appliedTier,
		"messageCount": s.MessageCount,
		"rpcWakeMs":    rpcWakeMs,
		"pinMs":        pinMs,
		"setupMs":      time.Since(t0).Milliseconds(),
	})
	return nil
}

func (s *AcpProjectSession) openFreshSession(toolsCatalog []ToolDef, thoughtLevel string) error {
	eng := s.engine()
	if err := eng.EnsureStarted(); err != nil {
		return err
	}
	if len(toolsCatalog) > 0 && !eng.Capabilities().McpHTTP {
		return errors.New("upstream does not advertise required HTTP MCP transport")
	}
	s.closeReadOnlyDB()
	s.waitIfClosing(5 * time.Second)
	s.mu.Lock()
	prevLiveSid := s.SessionID
	prevGen := s.engineGen
	wasSpawning := s.spawning
	cb := s.onBeforeSpawn
	closing := s.closing
	s.mu.Unlock()
	if closing {
		return &SoftError{StatusCode: 529, ErrorType: "overloaded_error", Message: "Session is closing; retry shortly"}
	}
	if prevLiveSid == "" && !wasSpawning && cb != nil {
		if err := cb(s); err != nil {
			return err
		}
		defer func() { s.mu.Lock(); s.endSpawnLocked(false); s.mu.Unlock() }()
		wasSpawning = true
	}

	s.mu.Lock()
	if prevLiveSid == "" {
		prevLiveSid = s.SessionID
		prevGen = s.engineGen
	}
	s.beginSpawnLocked()
	defer func() { s.mu.Lock(); s.endSpawnLocked(wasSpawning); s.mu.Unlock() }()

	s.SessionID = ""
	s.HibernatedSessionID = ""
	s.clearHibernatedParkedCallsLocked()
	s.ForceRolloverNext = false
	s.ForceRolloverReason = ""
	s.lastCompactedAt = time.Time{}
	s.contentAnchors = sessionContentAnchors{}
	s.ParkedCalls = make(map[string]*ParkedMcpCall)
	s.LastDeliveredCallIDs = nil
	s.PendingSiblingCalls = nil
	s.abortedSiblingCalls = nil
	s.RelayedCalls = nil
	s.reportedToolCalls = nil
	s.InitialToolsCatalog = append([]ToolDef(nil), toolsCatalog...)
	s.DeclaredMcpTools = make(map[string]bool, len(toolsCatalog))
	for _, t := range toolsCatalog {
		s.DeclaredMcpTools[t.Name] = true
	}
	s.ToolsCatalog = append([]ToolDef(nil), toolsCatalog...)
	s.McpToHostMap = make(map[string]string, len(toolsCatalog))
	for _, t := range toolsCatalog {
		s.McpToHostMap[t.McpName] = t.Name
	}
	tenantURL := fmt.Sprintf("http://127.0.0.1:%d/mcp/%s", GatewayPort, s.TenantKey)
	s.mu.Unlock()
	if prevLiveSid != "" {
		if prevGen == 0 || prevGen == eng.CurrentGen() {
			if err := eng.CloseSession(prevLiveSid); err != nil {
				s.mu.Lock()
				s.SessionID = prevLiveSid
				s.mu.Unlock()
				return err
			}
		} else {
			eng.UnregisterSession(prevLiveSid)
		}
	}

	mcpServers := make([]map[string]any, 0)
	if len(toolsCatalog) > 0 {
		mcpServers = append(mcpServers, map[string]any{
			"name": sessionBridgeServerName(toolsCatalog),
			"type": "http",
			"url":  tenantURL,
			"headers": []map[string]any{
				{"name": "Authorization", "value": "Bearer " + s.TenantKey},
			},
		})
	}

	appendPrompt := s.buildAppendSystemPrompt(toolsCatalog)

	t0 := time.Now()
	rawRes, err := eng.RPC("session/new", map[string]any{
		"cwd":        s.Cwd,
		"mcpServers": mcpServers,
		"_meta": map[string]any{
			"antigravity": map[string]any{
				"systemPrompt": map[string]any{"append": appendPrompt},
			},
		},
	}, 120*time.Second)
	if err != nil {
		return err
	}
	rpcNewMs := time.Since(t0).Milliseconds()
	var parsedNew struct {
		SessionID string `json:"sessionId"`
	}
	if err := json.Unmarshal(rawRes, &parsedNew); err != nil || parsedNew.SessionID == "" {
		return errors.New("Failed to start Antigravity conversation")
	}

	s.mu.Lock()
	s.SessionID = parsedNew.SessionID
	s.HibernatedSessionID = ""
	s.clearHibernatedParkedCallsLocked()
	s.InitialSystemText = s.SystemText
	s.InitialAppendPrompt = appendPrompt
	s.lastCompletedReqFp = ""
	s.lastCompletedResult = TurnResult{}
	s.lastCompletedAt = time.Time{}
	staleCompRef := s.lastCompletedCompactionTarget
	s.lastCompletedCompactionTarget = nil
	s.ToolFingerprint = toolCatalogFingerprint(toolsCatalog)
	s.MessageCount = 0
	s.lastTurnAssistant = false
	s.HistoryFingerprint = ""
	s.cancelledHistoryFp = ""
	s.contentAnchors = sessionContentAnchors{}
	s.LastCachedReadTokens = 0
	s.LastReportedContextUsed = 0
	s.LastReportedContextSize = 0
	s.resetCacheBurnLocked()
	s.clearCapacityFallbackLocked(true)
	s.lastTurnExplicitTier = false
	s.highTierZeroCacheStreak = 0
	s.orphanCapStreak = 0
	s.wasCancelledTurn = false
	s.LastSeenDbStepIdx = -1
	s.LastPlannerBatchIdx = -1
	s.LastPlannerBatchNextPos = 0
	s.LastPlannerBatchTotal = 0
	s.LastSeenGenMetaIdx = -1
	s.LastWakeMethod = ""
	s.syncPhaseLocked()
	s.mu.Unlock()
	staleCompRef.release()
	eng.RegisterListener(parsedNew.SessionID, s)

	s.mu.Lock()
	modelID := s.ModelID
	s.mu.Unlock()
	mInfo := ModelOrDefault(modelID)
	tPin := time.Now()
	appliedTier, curOpts, err := s.pinSessionConfigWithInitial(parsedNew.SessionID, mInfo, thoughtLevel, rawRes)
	if err != nil {
		return err
	}
	if UseG1Credits && (curOpts == nil || curOpts["use_g1_credits"] != "true") {
		_ = setAndVerifySessionConfigOptionWith(eng, parsedNew.SessionID, "use_g1_credits", "true", configOptionTimeout)
	}
	pinMs := time.Since(tPin).Milliseconds()
	s.mu.Lock()
	s.ModelID = mInfo.ID
	s.ThoughtLevel = appliedTier
	s.PendingThoughtLevel = ""
	s.mu.Unlock()
	s.persistSessionAsync()

	LogEvent("SESSION_NEW", "Opened 1M Pure-API ACP session in shared engine (Single-Port MCP)", map[string]any{
		"convKey":       s.ConvKey,
		"sessionId":     parsedNew.SessionID,
		"model":         mInfo.ID,
		"thoughtLevel":  appliedTier,
		"mcpToolsCount": len(toolsCatalog),
		"rpcNewMs":      rpcNewMs,
		"pinMs":         pinMs,
		"setupMs":       time.Since(t0).Milliseconds(),
	})
	return nil
}

func (s *AcpProjectSession) resetCacheBurnLocked() {
	s.ZeroCacheStreak = 0
	s.ZeroCacheUncachedTokens = 0
	s.LastCacheWarning = ""
}

func (s *AcpProjectSession) setClientIdentityLocked(ci ClientIdentity) {
	if ci.ID == "" && ci.Label == "" && ci.Protocol == "" {
		return
	}
	if s.ClientID == "" || !isKnownSpecificClientID(s.ClientID) || ci.Known {
		if ci.ID != "" {
			s.ClientID = ci.ID
		}
		if ci.Label != "" {
			s.ClientLabel = ci.Label
		}
	}
	if ci.Protocol != "" && (s.Protocol == "" || !isKnownSpecificClientID(s.ClientID) || ci.Known) {
		s.Protocol = ci.Protocol
	}
}

func (s *AcpProjectSession) advanceUsageWatermarks(sid string) {
	if sid == "" {
		return
	}
	maxGen := s.readMaxSessionGenMetadataIdx(sid)
	maxStep := s.readMaxSessionStepIdx(sid)
	if maxGen < 0 && maxStep < 0 {
		return
	}
	s.mu.Lock()
	changed := false
	if maxGen > s.LastSeenGenMetaIdx {
		s.LastSeenGenMetaIdx = maxGen
		changed = true
	}
	if maxStep > s.LastSeenDbStepIdx {
		s.LastSeenDbStepIdx = maxStep
		changed = true
	}
	if maxStep > s.LastPlannerBatchIdx {
		s.LastPlannerBatchIdx = maxStep
		s.LastPlannerBatchNextPos = 0
		s.LastPlannerBatchTotal = 0
	}
	s.mu.Unlock()
	if changed {
		s.persistSessionAsync()
	}
}
