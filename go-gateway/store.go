package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gwstore "agy-acp-gateway-go/internal/store"
)

var _ gwstore.SessionStore = (*StateStore)(nil)

const (
	currentAppForkDedupVer     = 1
	maxPendingSessionSnapshots = 256
	asyncSnapshotCoalesceDelay = 10 * time.Millisecond
)

type cursorMeta struct {
	dbSize       int64
	dbMtimeNs    int64
	walSize      int64
	walMtimeNs   int64
	lastStepIdx  int
	forkDedupVer int
}

type SessionSnapshot = gwstore.SessionSnapshot
type TurnMissKey = gwstore.TurnMissKey
type UsageAllTimeRollup = gwstore.UsageAllTimeRollup
type SessionClientMeta = gwstore.SessionClientMeta

type StateStore struct {
	initMu           sync.Mutex
	mu               sync.Mutex
	readMu           sync.RWMutex
	cursorMu         sync.RWMutex
	coldMu           sync.Mutex
	promMu           sync.Mutex
	snapMu           sync.Mutex
	dbPath           string
	db               *sql.DB
	readDB           *sql.DB
	ready            atomic.Bool
	cursors          map[string]cursorMeta
	version          atomic.Uint64
	coldEpoch        atomic.Uint64
	coldBuiltEpoch   uint64
	promCacheVer     uint64
	promTurns        map[string]int
	promTokens       map[string]int64
	promCompactions  int
	promReuses       int
	promHasRows      bool
	coldSteps        []ParsedModelStep
	coldCutoffSec    int64
	coldValid        bool
	pendingSnaps     map[string]SessionSnapshot
	snapFlushing     bool
	snapFlushDone    chan struct{}
	snapStarted      bool
	snapWakeCh       chan struct{}
	snapStopCh       chan struct{}
	snapDoneCh       chan struct{}
	committedBlobGen map[string]uint64
	committedSnapSeq map[string]uint64
}

var (
	globalSnapBlobGen atomic.Uint64
	globalSnapSeq     atomic.Uint64
)

type restoredConvRow struct {
	convKey, sid, tenantKey, cwd, modelID, tier              string
	toolFp, initToolsJSON, curToolsJSON, sysText, histFp     string
	initSysText, initAppendPrompt                            string
	clientID, clientLabel, protocol                          string
	msgCnt, lastCached, lastDbIdx, lastGenIdx, reuses, rolls int
	createdMs, updatedMs                                     int64
}

var GlobalStore = &StateStore{}

func InitGlobalStore(dbPath string) {
	GlobalStore.Init(dbPath)
}

func (st *StateStore) Init(dbPath string) {
	st.initMu.Lock()
	defer st.initMu.Unlock()
	st.ready.Store(false)
	st.stopSnapshotFlusher()
	st.readMu.Lock()
	if st.readDB != nil {
		_ = st.readDB.Close()
		st.readDB = nil
	}
	st.readMu.Unlock()

	st.mu.Lock()
	if st.db != nil {
		_ = st.db.Close()
		st.db = nil
	}
	st.dbPath = strings.TrimSpace(dbPath)
	st.committedBlobGen = nil
	st.committedSnapSeq = nil
	st.cursorMu.Lock()
	st.cursors = nil
	st.cursorMu.Unlock()
	if st.dbPath == "" {
		st.mu.Unlock()
		st.resetCachesOnInit()
		return
	}
	_ = os.MkdirAll(filepath.Dir(st.dbPath), 0755)
	dsn := sqliteFileURI(st.dbPath, "_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)&_pragma=journal_size_limit(67108864)")
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		st.mu.Unlock()
		st.resetCachesOnInit()
		LogEvent("STORE_INIT_ERR", "failed to open gateway state store", map[string]any{"path": st.dbPath, "error": err.Error()})
		return
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	schema := `
CREATE TABLE IF NOT EXISTS conversations (
	conv_key             TEXT PRIMARY KEY,
	agy_session_id       TEXT NOT NULL,
	tenant_key           TEXT NOT NULL,
	cwd                  TEXT NOT NULL,
	model_id             TEXT NOT NULL,
	thought_level        TEXT NOT NULL,
	tool_fp              TEXT NOT NULL,
	system_text          TEXT NOT NULL DEFAULT '',
	msg_count            INTEGER NOT NULL,
	history_fp           TEXT NOT NULL,
	last_cached_tok      INTEGER NOT NULL DEFAULT 0,
	last_db_step_idx     INTEGER NOT NULL DEFAULT -1,
	session_reuses       INTEGER NOT NULL DEFAULT 0,
	compaction_rollovers INTEGER NOT NULL DEFAULT 0,
	state                TEXT NOT NULL,
	created_at_ms        INTEGER NOT NULL,
	updated_at_ms        INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS conversation_hot (
	conv_key             TEXT PRIMARY KEY,
	agy_session_id       TEXT NOT NULL DEFAULT '',
	msg_count            INTEGER NOT NULL DEFAULT 0,
	history_fp           TEXT NOT NULL DEFAULT '',
	last_cached_tok      INTEGER NOT NULL DEFAULT 0,
	last_db_step_idx     INTEGER NOT NULL DEFAULT -1,
	last_gen_meta_idx    INTEGER NOT NULL DEFAULT -1,
	client_id            TEXT NOT NULL DEFAULT '',
	client_label         TEXT NOT NULL DEFAULT '',
	protocol             TEXT NOT NULL DEFAULT '',
	session_reuses       INTEGER NOT NULL DEFAULT 0,
	compaction_rollovers INTEGER NOT NULL DEFAULT 0,
	state                TEXT NOT NULL DEFAULT 'LiveIdle',
	updated_at_ms        INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS turns (
	id               INTEGER PRIMARY KEY AUTOINCREMENT,
	conv_key         TEXT NOT NULL,
	agy_session_id   TEXT NOT NULL,
	turn_idx         INTEGER NOT NULL,
	mode             TEXT NOT NULL,
	model_id         TEXT NOT NULL,
	thought_level    TEXT NOT NULL,
	stop_reason      TEXT NOT NULL,
	elapsed_ms       INTEGER NOT NULL,
	ttft_ms          INTEGER NOT NULL,
	stream_ms        INTEGER NOT NULL DEFAULT 0,
	tps              REAL NOT NULL,
	decode_tps       REAL NOT NULL DEFAULT 0,
	in_tok           INTEGER NOT NULL,
	cached_tok       INTEGER NOT NULL,
	out_tok          INTEGER NOT NULL,
	raw_in_tok       INTEGER NOT NULL DEFAULT 0,
	raw_cr_tok       INTEGER NOT NULL DEFAULT 0,
	thinking_tok     INTEGER NOT NULL,
	thinking_ms      INTEGER NOT NULL DEFAULT 0,
	retry_count      INTEGER NOT NULL DEFAULT 0,
	steps_in_prompt  INTEGER NOT NULL,
	usage_source     TEXT NOT NULL,
	cache_hit_rate   TEXT NOT NULL DEFAULT '0.00%',
	completed_at_ms  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_turns_completed ON turns(completed_at_ms DESC);
CREATE INDEX IF NOT EXISTS idx_turns_conv ON turns(conv_key, turn_idx);

CREATE TABLE IF NOT EXISTS usage_steps (
	session_id       TEXT NOT NULL,
	step_idx         INTEGER NOT NULL,
	source_cat       TEXT NOT NULL,
	ts_sec           INTEGER NOT NULL,
	ttft_ms          INTEGER NOT NULL,
	stream_ms        INTEGER NOT NULL DEFAULT 0,
	elapsed_ms       INTEGER NOT NULL,
	tps              REAL NOT NULL,
	decode_tps       REAL NOT NULL DEFAULT 0,
	model_enum       INTEGER NOT NULL,
	model_id         TEXT NOT NULL,
	model_label      TEXT NOT NULL DEFAULT '',
	model_family     TEXT NOT NULL DEFAULT '',
	model_tier       TEXT NOT NULL,
	quota_group      TEXT NOT NULL,
	raw_in           INTEGER NOT NULL,
	raw_cr           INTEGER NOT NULL,
	aligned_in       INTEGER NOT NULL,
	aligned_cr       INTEGER NOT NULL,
	out_tok          INTEGER NOT NULL,
	vis_out_tok      INTEGER NOT NULL,
	th_tok           INTEGER NOT NULL,
	has_thinking     INTEGER NOT NULL DEFAULT 0,
	cw_tok           INTEGER NOT NULL,
	tier_mult        REAL NOT NULL,
	in_cost_usd      REAL NOT NULL,
	cr_cost_usd      REAL NOT NULL,
	out_cost_usd     REAL NOT NULL,
	th_cost_usd      REAL NOT NULL,
	cost_usd         REAL NOT NULL,
	base1x_cost_usd  REAL NOT NULL DEFAULT 0,
	nocache_cost_usd REAL NOT NULL DEFAULT 0,
	saved_usd        REAL NOT NULL,
	is_dsh_bridge    INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (session_id, step_idx)
);
CREATE INDEX IF NOT EXISTS idx_steps_ts ON usage_steps(ts_sec);

CREATE TABLE IF NOT EXISTS ingest_cursor (
	session_id    TEXT PRIMARY KEY,
	db_size       INTEGER NOT NULL,
	db_mtime_ns   INTEGER NOT NULL,
	wal_size      INTEGER NOT NULL,
	wal_mtime_ns  INTEGER NOT NULL,
	last_step_idx INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS known_gateway_sids (
	session_id TEXT PRIMARY KEY
);
`
	if _, err := db.Exec(schema); err != nil {
		LogEvent("STORE_INIT_ERR", "failed to initialize gateway state schema", map[string]any{"path": st.dbPath, "error": err.Error()})
		_ = db.Close()
		st.mu.Unlock()
		st.resetCachesOnInit()
		return
	}
	for _, col := range []struct{ table, name, decl string }{
		{"conversations", "init_tools_json", "TEXT NOT NULL DEFAULT ''"},
		{"conversations", "cur_tools_json", "TEXT NOT NULL DEFAULT ''"},
		{"conversations", "init_system_text", "TEXT NOT NULL DEFAULT ''"},
		{"conversations", "init_append_prompt", "TEXT NOT NULL DEFAULT ''"},
		{"conversations", "last_gen_meta_idx", "INTEGER NOT NULL DEFAULT -1"},
		{"conversations", "client_id", "TEXT NOT NULL DEFAULT ''"},
		{"conversations", "client_label", "TEXT NOT NULL DEFAULT ''"},
		{"conversations", "protocol", "TEXT NOT NULL DEFAULT ''"},
		{"turns", "served_model", "TEXT NOT NULL DEFAULT ''"},
		{"turns", "cache_miss", "TEXT NOT NULL DEFAULT ''"},
		{"turns", "stream_ms", "INTEGER NOT NULL DEFAULT 0"},
		{"turns", "decode_tps", "REAL NOT NULL DEFAULT 0"},
		{"turns", "upstream_ttft_ms", "INTEGER NOT NULL DEFAULT 0"},
		{"turns", "thinking_ms", "INTEGER NOT NULL DEFAULT 0"},
		{"turns", "retry_count", "INTEGER NOT NULL DEFAULT 0"},
		{"turns", "client_id", "TEXT NOT NULL DEFAULT ''"},
		{"turns", "client_label", "TEXT NOT NULL DEFAULT ''"},
		{"turns", "protocol", "TEXT NOT NULL DEFAULT ''"},
		{"usage_steps", "served_model", "TEXT NOT NULL DEFAULT ''"},
		{"usage_steps", "stream_ms", "INTEGER NOT NULL DEFAULT 0"},
		{"usage_steps", "decode_tps", "REAL NOT NULL DEFAULT 0"},
		{"ingest_cursor", "fork_dedup_ver", "INTEGER NOT NULL DEFAULT 0"},
	} {
		if err := addColumnIfMissing(db, col.table, col.name, col.decl); err != nil {
			LogEvent("STORE_INIT_ERR", "failed to migrate gateway state column", map[string]any{"path": st.dbPath, "table": col.table, "column": col.name, "error": err.Error()})
			_ = db.Close()
			st.mu.Unlock()
			st.resetCachesOnInit()
			return
		}
	}
	_, _ = db.Exec(`INSERT OR IGNORE INTO known_gateway_sids (session_id) SELECT DISTINCT lower(agy_session_id) FROM conversations WHERE agy_session_id != ''`)
	_, _ = db.Exec(`INSERT OR IGNORE INTO known_gateway_sids (session_id) SELECT DISTINCT lower(agy_session_id) FROM turns WHERE agy_session_id != ''`)
	st.db = db
	st.backfillClientAndTimingLocked()

	cursors := make(map[string]cursorMeta)
	if rows, err := db.Query(`SELECT session_id, db_size, db_mtime_ns, wal_size, wal_mtime_ns, last_step_idx, fork_dedup_ver FROM ingest_cursor`); err == nil {
		for rows.Next() {
			var sid string
			var c cursorMeta
			if rows.Scan(&sid, &c.dbSize, &c.dbMtimeNs, &c.walSize, &c.walMtimeNs, &c.lastStepIdx, &c.forkDedupVer) == nil {
				cursors[sid] = c
			}
		}
		_ = rows.Close()
	}
	st.cursorMu.Lock()
	st.cursors = cursors
	st.cursorMu.Unlock()
	dbPathCopy := st.dbPath
	st.mu.Unlock()

	readDSN := sqliteFileURI(dbPathCopy, "mode=ro&_pragma=query_only(1)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)")
	if rDB, errR := sql.Open("sqlite", readDSN); errR == nil {
		rDB.SetMaxOpenConns(4)
		rDB.SetMaxIdleConns(4)
		if pErr := rDB.Ping(); pErr == nil {
			st.readMu.Lock()
			st.readDB = rDB
			st.readMu.Unlock()
		} else {
			_ = rDB.Close()
		}
	}

	st.resetCachesOnInit()
	st.version.Add(1)
	st.ready.Store(true)
}

func (st *StateStore) resetCachesOnInit() {
	st.coldMu.Lock()
	st.coldSteps = nil
	st.coldCutoffSec = 0
	st.coldValid = false
	st.coldBuiltEpoch = st.coldEpoch.Add(1)
	st.coldMu.Unlock()

	st.promMu.Lock()
	st.promCacheVer = 0
	st.promTurns = nil
	st.promTokens = nil
	st.promCompactions = 0
	st.promReuses = 0
	st.promHasRows = false
	st.promMu.Unlock()
}

func (st *StateStore) Close() {
	st.initMu.Lock()
	defer st.initMu.Unlock()
	st.ready.Store(false)
	st.stopSnapshotFlusher()
	st.readMu.Lock()
	if st.readDB != nil {
		_ = st.readDB.Close()
		st.readDB = nil
	}
	st.readMu.Unlock()

	st.mu.Lock()
	if st.db != nil {
		_ = st.db.Close()
		st.db = nil
	}
	st.committedBlobGen = nil
	st.committedSnapSeq = nil
	st.mu.Unlock()

	st.resetCachesOnInit()
}

func (st *StateStore) acquireReadDB() (*sql.DB, func()) {
	st.readMu.RLock()
	if st.readDB != nil {
		return st.readDB, st.readMu.RUnlock
	}
	st.readMu.RUnlock()
	st.mu.Lock()
	if st.db != nil {
		return st.db, st.mu.Unlock
	}
	st.mu.Unlock()
	return nil, func() {}
}

// addColumnIfMissing is the schema migration for columns added after the first release.
func addColumnIfMissing(db *sql.DB, table, column, decl string) error {
	rows, err := db.Query("PRAGMA table_info(" + table + ")")
	if err != nil {
		return err
	}
	found := false
	for rows.Next() {
		var cid, notNull, pk int
		var name, typ string
		var dflt sql.NullString
		if rows.Scan(&cid, &name, &typ, &notNull, &dflt, &pk) == nil && name == column {
			found = true
		}
	}
	_ = rows.Close()
	if found {
		return nil
	}
	_, err = db.Exec("ALTER TABLE " + table + " ADD COLUMN " + column + " " + decl)
	return err
}

func (st *StateStore) IsReady() bool {
	return st.ready.Load()
}

func (st *StateStore) Version() uint64 {
	return st.version.Load()
}

func (st *StateStore) DBPath() string {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.dbPath
}

func (s *AcpProjectSession) SessionStateLabel() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessionStateLabelLocked()
}

func (s *AcpProjectSession) sessionStateLabelLocked() string {
	if s.SessionID == "" {
		return "Detached"
	}
	switch s.syncPhaseLocked() {
	case PhaseHotParked:
		return "ParkedWaitingTool"
	case PhaseStreaming, PhaseDetached, PhaseSpawning:
		return "BusyPrompting"
	default:
		return "LiveIdle"
	}
}

func captureSessionSnapshot(s *AcpProjectSession) (SessionSnapshot, bool) {
	if s == nil || strings.HasPrefix(s.ConvKey, "util:") {
		return SessionSnapshot{}, false
	}
	s.mu.Lock()
	convKey := s.ConvKey
	sid := s.SessionID
	if sid == "" {
		sid = s.HibernatedSessionID
	}
	tenantKey := s.TenantKey
	cwd := s.Cwd
	modelID := s.ModelID
	tier := s.ThoughtLevel
	toolFp := s.ToolFingerprint
	initPtr := toolSliceDataPtr(s.InitialToolsCatalog)
	initLen := len(s.InitialToolsCatalog)
	if initLen == 0 {
		s.cachedInitToolsPtr = nil
		s.cachedInitToolsLen = 0
		s.cachedInitToolsFp = ""
		s.cachedInitToolsJSON = ""
	} else if s.cachedInitToolsPtr != initPtr || s.cachedInitToolsLen != initLen || s.cachedInitToolsJSON == "" {
		if s.cachedInitToolsPtr != initPtr || s.cachedInitToolsLen != initLen {
			s.cachedInitToolsFp = ""
		}
		if b, err := json.Marshal(s.InitialToolsCatalog); err == nil {
			s.cachedInitToolsJSON = string(b)
		} else {
			s.cachedInitToolsJSON = ""
		}
		s.cachedInitToolsPtr = initPtr
		s.cachedInitToolsLen = initLen
	}
	initToolsJSON := s.cachedInitToolsJSON

	curPtr := toolSliceDataPtr(s.ToolsCatalog)
	curLen := len(s.ToolsCatalog)
	if curLen == 0 {
		s.cachedCurToolsPtr = nil
		s.cachedCurToolsLen = 0
		s.cachedCurToolsFp = ""
		s.cachedCurToolsJSON = ""
	} else if s.cachedCurToolsPtr != curPtr || s.cachedCurToolsLen != curLen || s.cachedCurToolsFp != toolFp || s.cachedCurToolsJSON == "" {
		if b, err := json.Marshal(s.ToolsCatalog); err == nil {
			s.cachedCurToolsJSON = string(b)
		} else {
			s.cachedCurToolsJSON = ""
		}
		s.cachedCurToolsPtr = curPtr
		s.cachedCurToolsLen = curLen
		s.cachedCurToolsFp = toolFp
	}
	curToolsJSON := s.cachedCurToolsJSON

	sysText := s.SystemText
	initSysText := s.InitialSystemText
	initAppendPrompt := s.InitialAppendPrompt
	sig := sessionBlobSig{
		sid:              sid,
		tenantKey:        tenantKey,
		cwd:              cwd,
		modelID:          modelID,
		tier:             tier,
		toolFp:           toolFp,
		sysText:          sysText,
		initSysText:      initSysText,
		initAppendPrompt: initAppendPrompt,
		initToolsPtr:     initPtr,
		initToolsLen:     initLen,
		curToolsPtr:      curPtr,
		curToolsLen:      curLen,
	}
	msgCnt := s.MessageCount
	hadPersisted := s.persistedInDB || s.snapBlobGen > 0 || s.TotalTurns > 0
	if sid == "" && msgCnt == 0 && !hadPersisted {
		s.mu.Unlock()
		return SessionSnapshot{}, false
	}
	if s.snapBlobGen == 0 || sig != s.snapBlobSig {
		s.snapBlobGen = globalSnapBlobGen.Add(1)
		s.snapBlobSig = sig
	}
	s.persistedInDB = true
	blobGen := s.snapBlobGen
	snapSeq := globalSnapSeq.Add(1)
	histFp := s.HistoryFingerprint
	lastCached := s.LastCachedReadTokens
	lastDbIdx := s.LastSeenDbStepIdx
	lastGenIdx := s.LastSeenGenMetaIdx
	clientID := s.ClientID
	clientLabel := s.ClientLabel
	protocol := s.Protocol
	reuses := s.SessionReuses
	rollovers := s.CompactionRollovers
	state := s.sessionStateLabelLocked()
	createdMs := s.CreatedAt.UnixMilli()
	updatedMs := s.LastUsedAt.UnixMilli()
	s.mu.Unlock()
	return SessionSnapshot{
		ConvKey:          convKey,
		SessionID:        sid,
		TenantKey:        tenantKey,
		Cwd:              cwd,
		ModelID:          modelID,
		ThoughtLevel:     tier,
		ToolFingerprint:  toolFp,
		InitToolsJSON:    initToolsJSON,
		CurToolsJSON:     curToolsJSON,
		SystemText:       sysText,
		InitSystemText:   initSysText,
		InitAppendPrompt: initAppendPrompt,
		MsgCount:         msgCnt,
		HistoryFp:        histFp,
		LastCachedTok:    lastCached,
		LastDbStepIdx:    lastDbIdx,
		LastGenMetaIdx:   lastGenIdx,
		ClientID:         clientID,
		ClientLabel:      clientLabel,
		Protocol:         protocol,
		SessionReuses:    reuses,
		Rollovers:        rollovers,
		State:            state,
		CreatedAtMs:      createdMs,
		UpdatedAtMs:      updatedMs,
		BlobGen:          blobGen,
		SnapSeq:          snapSeq,
	}, true
}

const upsertConversationSQL = `
INSERT INTO conversations (
	conv_key, agy_session_id, tenant_key, cwd, model_id, thought_level,
	tool_fp, init_tools_json, cur_tools_json, system_text, init_system_text, init_append_prompt, msg_count, history_fp, last_cached_tok, last_db_step_idx, last_gen_meta_idx,
	client_id, client_label, protocol,
	session_reuses, compaction_rollovers, state, created_at_ms, updated_at_ms
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(conv_key) DO UPDATE SET
	agy_session_id = excluded.agy_session_id,
	tenant_key = excluded.tenant_key,
	cwd = excluded.cwd,
	model_id = excluded.model_id,
	thought_level = excluded.thought_level,
	tool_fp = excluded.tool_fp,
	init_tools_json = CASE WHEN excluded.init_tools_json != '' OR excluded.agy_session_id != conversations.agy_session_id THEN excluded.init_tools_json ELSE conversations.init_tools_json END,
	cur_tools_json = CASE WHEN excluded.cur_tools_json != '' OR excluded.agy_session_id != conversations.agy_session_id THEN excluded.cur_tools_json ELSE conversations.cur_tools_json END,
	system_text = excluded.system_text,
	init_system_text = CASE WHEN excluded.init_system_text != '' OR excluded.agy_session_id != conversations.agy_session_id THEN excluded.init_system_text ELSE conversations.init_system_text END,
	init_append_prompt = CASE WHEN excluded.init_append_prompt != '' OR excluded.agy_session_id != conversations.agy_session_id THEN excluded.init_append_prompt ELSE conversations.init_append_prompt END,
	msg_count = excluded.msg_count,
	history_fp = excluded.history_fp,
	last_cached_tok = excluded.last_cached_tok,
	last_db_step_idx = excluded.last_db_step_idx,
	last_gen_meta_idx = excluded.last_gen_meta_idx,
	client_id = CASE WHEN excluded.client_id != '' THEN excluded.client_id ELSE conversations.client_id END,
	client_label = CASE WHEN excluded.client_label != '' THEN excluded.client_label ELSE conversations.client_label END,
	protocol = CASE WHEN excluded.protocol != '' THEN excluded.protocol ELSE conversations.protocol END,
	session_reuses = MAX(conversations.session_reuses, excluded.session_reuses),
	compaction_rollovers = MAX(conversations.compaction_rollovers, excluded.compaction_rollovers),
	state = excluded.state,
	updated_at_ms = excluded.updated_at_ms
`

const updateConversationHotSQL = `
UPDATE conversation_hot SET
	msg_count = ?,
	history_fp = ?,
	last_cached_tok = ?,
	last_db_step_idx = ?,
	last_gen_meta_idx = ?,
	client_id = CASE WHEN ? != '' THEN ? ELSE client_id END,
	client_label = CASE WHEN ? != '' THEN ? ELSE client_label END,
	protocol = CASE WHEN ? != '' THEN ? ELSE protocol END,
	session_reuses = MAX(session_reuses, ?),
	compaction_rollovers = MAX(compaction_rollovers, ?),
	state = ?,
	updated_at_ms = ?
WHERE conv_key = ? AND agy_session_id = ?
`

const upsertConversationHotSQL = `
INSERT INTO conversation_hot (
	conv_key, agy_session_id, msg_count, history_fp, last_cached_tok, last_db_step_idx, last_gen_meta_idx,
	client_id, client_label, protocol, session_reuses, compaction_rollovers, state, updated_at_ms
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(conv_key) DO UPDATE SET
	agy_session_id = excluded.agy_session_id,
	msg_count = excluded.msg_count,
	history_fp = excluded.history_fp,
	last_cached_tok = excluded.last_cached_tok,
	last_db_step_idx = excluded.last_db_step_idx,
	last_gen_meta_idx = excluded.last_gen_meta_idx,
	client_id = CASE WHEN excluded.client_id != '' THEN excluded.client_id ELSE conversation_hot.client_id END,
	client_label = CASE WHEN excluded.client_label != '' THEN excluded.client_label ELSE conversation_hot.client_label END,
	protocol = CASE WHEN excluded.protocol != '' THEN excluded.protocol ELSE conversation_hot.protocol END,
	session_reuses = MAX(conversation_hot.session_reuses, excluded.session_reuses),
	compaction_rollovers = MAX(conversation_hot.compaction_rollovers, excluded.compaction_rollovers),
	state = excluded.state,
	updated_at_ms = excluded.updated_at_ms
`

type sqlExecer interface {
	Exec(query string, args ...any) (sql.Result, error)
}

func snapshotExecArgs(snap SessionSnapshot) []any {
	return []any{
		snap.ConvKey, snap.SessionID, snap.TenantKey, snap.Cwd, snap.ModelID, snap.ThoughtLevel,
		snap.ToolFingerprint, snap.InitToolsJSON, snap.CurToolsJSON, snap.SystemText, snap.InitSystemText, snap.InitAppendPrompt, snap.MsgCount, snap.HistoryFp, snap.LastCachedTok, snap.LastDbStepIdx, snap.LastGenMetaIdx,
		snap.ClientID, snap.ClientLabel, snap.Protocol,
		snap.SessionReuses, snap.Rollovers, snap.State, snap.CreatedAtMs, snap.UpdatedAtMs,
	}
}

func snapshotHotUpdateArgs(snap SessionSnapshot) []any {
	return []any{
		snap.MsgCount, snap.HistoryFp, snap.LastCachedTok, snap.LastDbStepIdx, snap.LastGenMetaIdx,
		snap.ClientID, snap.ClientID, snap.ClientLabel, snap.ClientLabel, snap.Protocol, snap.Protocol,
		snap.SessionReuses, snap.Rollovers, snap.State, snap.UpdatedAtMs,
		snap.ConvKey, snap.SessionID,
	}
}

func snapshotHotUpsertArgs(snap SessionSnapshot) []any {
	return []any{
		snap.ConvKey, snap.SessionID, snap.MsgCount, snap.HistoryFp, snap.LastCachedTok, snap.LastDbStepIdx, snap.LastGenMetaIdx,
		snap.ClientID, snap.ClientLabel, snap.Protocol, snap.SessionReuses, snap.Rollovers, snap.State, snap.UpdatedAtMs,
	}
}

func (st *StateStore) execSnapshotUpsertOrHotLocked(execer sqlExecer, snap SessionSnapshot) (usedHot bool, err error) {
	if snap.BlobGen > 0 && st.committedBlobGen != nil && st.committedBlobGen[snap.ConvKey] == snap.BlobGen {
		res, uErr := execer.Exec(updateConversationHotSQL, snapshotHotUpdateArgs(snap)...)
		if uErr != nil {
			return false, uErr
		}
		if n, _ := res.RowsAffected(); n > 0 {
			return true, nil
		}
	}
	if _, err := execer.Exec(upsertConversationSQL, snapshotExecArgs(snap)...); err != nil {
		return false, err
	}
	if _, err := execer.Exec(upsertConversationHotSQL, snapshotHotUpsertArgs(snap)...); err != nil {
		return false, err
	}
	return false, nil
}

func (st *StateStore) recordCommittedBlobGenLocked(snap SessionSnapshot) {
	if snap.BlobGen == 0 {
		if st.committedBlobGen != nil {
			delete(st.committedBlobGen, snap.ConvKey)
		}
		return
	}
	if st.committedBlobGen == nil {
		st.committedBlobGen = make(map[string]uint64)
	}
	st.committedBlobGen[snap.ConvKey] = snap.BlobGen
}

func (st *StateStore) isStaleSnapshotLocked(snap SessionSnapshot) bool {
	if snap.SnapSeq == 0 || st.committedSnapSeq == nil {
		return false
	}
	return snap.SnapSeq < st.committedSnapSeq[snap.ConvKey]
}

func (st *StateStore) recordCommittedSnapSeqLocked(snap SessionSnapshot) {
	if snap.ConvKey == "" || snap.SnapSeq == 0 {
		return
	}
	if st.committedSnapSeq == nil {
		st.committedSnapSeq = make(map[string]uint64)
	}
	if snap.SnapSeq > st.committedSnapSeq[snap.ConvKey] {
		st.committedSnapSeq[snap.ConvKey] = snap.SnapSeq
	}
}

func (st *StateStore) writeSnapshotBatch(batch []SessionSnapshot) {
	if len(batch) == 0 {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.db == nil {
		return
	}
	filtered := batch[:0]
	for _, snap := range batch {
		if !st.isStaleSnapshotLocked(snap) {
			filtered = append(filtered, snap)
		}
	}
	batch = filtered
	if len(batch) == 0 {
		return
	}
	if len(batch) == 1 {
		snap := batch[0]
		if snap.BlobGen > 0 && st.committedBlobGen != nil && st.committedBlobGen[snap.ConvKey] == snap.BlobGen {
			if res, err := st.db.Exec(updateConversationHotSQL, snapshotHotUpdateArgs(snap)...); err == nil {
				if n, _ := res.RowsAffected(); n > 0 {
					st.recordCommittedSnapSeqLocked(snap)
					st.version.Add(1)
					return
				}
			}
		}
	}
	tx, err := st.db.Begin()
	if err != nil {
		LogEvent("STORE_WRITE_ERR", "upsert session batch begin failed", map[string]any{"error": err.Error()})
		return
	}
	defer tx.Rollback()
	sidSet := make(map[string]struct{}, len(batch))
	for _, snap := range batch {
		usedHot, err := st.execSnapshotUpsertOrHotLocked(tx, snap)
		if err != nil {
			LogEvent("STORE_WRITE_ERR", "upsert session batch exec failed", map[string]any{"error": err.Error()})
			return
		}
		if !usedHot && snap.SessionID != "" {
			sidSet[strings.ToLower(snap.SessionID)] = struct{}{}
		}
	}
	for sid := range sidSet {
		_, _ = tx.Exec(`INSERT OR IGNORE INTO known_gateway_sids (session_id) VALUES (?)`, sid)
	}
	if err := tx.Commit(); err != nil {
		LogEvent("STORE_WRITE_ERR", "upsert session batch commit failed", map[string]any{"error": err.Error()})
		return
	}
	for _, snap := range batch {
		st.recordCommittedBlobGenLocked(snap)
		st.recordCommittedSnapSeqLocked(snap)
	}
	st.version.Add(1)
}

func (st *StateStore) startSnapshotFlusherLocked() {
	if st.snapStarted {
		return
	}
	st.snapStarted = true
	st.snapWakeCh = make(chan struct{}, 1)
	st.snapStopCh = make(chan struct{})
	st.snapDoneCh = make(chan struct{})
	go st.snapshotFlusherLoop(st.snapWakeCh, st.snapStopCh, st.snapDoneCh)
}

func (st *StateStore) stopSnapshotFlusher() {
	st.snapMu.Lock()
	// Fence any UpsertSessionSnapshotAsync caller that entered snapMu before ready=false.
	st.snapMu.Unlock()
	st.Flush()
	st.snapMu.Lock()
	if !st.snapStarted {
		st.snapMu.Unlock()
		return
	}
	stopCh := st.snapStopCh
	doneCh := st.snapDoneCh
	st.snapStarted = false
	st.snapWakeCh = nil
	st.snapStopCh = nil
	st.snapDoneCh = nil
	st.snapMu.Unlock()
	close(stopCh)
	<-doneCh
}

func (st *StateStore) snapshotFlusherLoop(wakeCh, stopCh, doneCh chan struct{}) {
	defer close(doneCh)
	for {
		select {
		case <-stopCh:
			st.drainPendingSnapshots()
			return
		case <-wakeCh:
		}
		t := time.NewTimer(asyncSnapshotCoalesceDelay)
		select {
		case <-t.C:
		case <-stopCh:
			t.Stop()
			st.drainPendingSnapshots()
			return
		}
		st.drainPendingSnapshots()
	}
}

func (st *StateStore) drainPendingSnapshots() {
	for {
		st.snapMu.Lock()
		if len(st.pendingSnaps) == 0 {
			st.snapMu.Unlock()
			return
		}
		if st.snapFlushing {
			doneCh := st.snapFlushDone
			st.snapMu.Unlock()
			if doneCh != nil {
				<-doneCh
			}
			continue
		}
		batch := make([]SessionSnapshot, 0, len(st.pendingSnaps))
		for k, snap := range st.pendingSnaps {
			batch = append(batch, snap)
			delete(st.pendingSnaps, k)
		}
		st.snapFlushing = true
		flushDone := make(chan struct{})
		st.snapFlushDone = flushDone
		st.snapMu.Unlock()

		st.writeSnapshotBatch(batch)

		st.snapMu.Lock()
		st.snapFlushing = false
		st.snapFlushDone = nil
		close(flushDone)
		more := len(st.pendingSnaps) > 0
		st.snapMu.Unlock()
		if !more {
			return
		}
	}
}

func (st *StateStore) Flush() {
	for {
		st.drainPendingSnapshots()
		st.snapMu.Lock()
		flushing := st.snapFlushing
		doneCh := st.snapFlushDone
		hasMore := len(st.pendingSnaps) > 0
		st.snapMu.Unlock()
		if !flushing && !hasMore {
			return
		}
		if flushing && doneCh != nil {
			<-doneCh
		}
	}
}

func (st *StateStore) UpsertSession(s *AcpProjectSession) {
	snap, ok := captureSessionSnapshot(s)
	if !ok {
		return
	}
	st.UpsertSessionSnapshot(snap)
}

func (st *StateStore) UpsertSessionSnapshot(snap SessionSnapshot) {
	if snap.ConvKey == "" || strings.HasPrefix(snap.ConvKey, "util:") {
		return
	}
	for {
		st.snapMu.Lock()
		if st.pendingSnaps != nil {
			delete(st.pendingSnaps, snap.ConvKey)
		}
		flushing := st.snapFlushing
		doneCh := st.snapFlushDone
		st.snapMu.Unlock()
		if !flushing {
			break
		}
		if doneCh != nil {
			<-doneCh
		}
	}
	st.writeSnapshotBatch([]SessionSnapshot{snap})
}

func (st *StateStore) UpsertSessionAsync(s *AcpProjectSession) {
	if !st.ready.Load() {
		return
	}
	snap, ok := captureSessionSnapshot(s)
	if !ok {
		return
	}
	st.UpsertSessionSnapshotAsync(snap)
}

func (st *StateStore) UpsertSessionSnapshotAsync(snap SessionSnapshot) {
	if !st.ready.Load() || snap.ConvKey == "" || strings.HasPrefix(snap.ConvKey, "util:") {
		return
	}
	for {
		st.snapMu.Lock()
		if !st.ready.Load() {
			st.snapMu.Unlock()
			return
		}
		if st.pendingSnaps == nil {
			st.pendingSnaps = make(map[string]SessionSnapshot)
		}
		if prev, exists := st.pendingSnaps[snap.ConvKey]; exists {
			if snap.SnapSeq == 0 || snap.SnapSeq >= prev.SnapSeq {
				st.pendingSnaps[snap.ConvKey] = snap
			}
			st.startSnapshotFlusherLocked()
			wakeCh := st.snapWakeCh
			st.snapMu.Unlock()
			if wakeCh != nil {
				select {
				case wakeCh <- struct{}{}:
				default:
				}
			}
			return
		}
		if len(st.pendingSnaps) >= maxPendingSessionSnapshots {
			flushing := st.snapFlushing
			doneCh := st.snapFlushDone
			st.snapMu.Unlock()
			if flushing && doneCh != nil {
				<-doneCh
				continue
			}
			st.writeSnapshotBatch([]SessionSnapshot{snap})
			return
		}
		st.pendingSnaps[snap.ConvKey] = snap
		st.startSnapshotFlusherLocked()
		wakeCh := st.snapWakeCh
		st.snapMu.Unlock()
		if wakeCh != nil {
			select {
			case wakeCh <- struct{}{}:
			default:
			}
		}
		return
	}
}

func (st *StateStore) InsertTurnAndUpsertSession(s *AcpProjectSession, entry TurnHistoryEntry) {
	if s == nil || strings.HasPrefix(s.ConvKey, "util:") {
		return
	}
	snap, hasSnap := captureSessionSnapshot(s)
	st.InsertTurnWithSnapshot(snap, hasSnap, s.ConvKey, entry)
}

func (st *StateStore) InsertTurnWithSnapshot(snap SessionSnapshot, hasSnap bool, convKey string, entry TurnHistoryEntry) {
	if convKey == "" || strings.HasPrefix(convKey, "util:") {
		return
	}
	if !hasSnap {
		st.InsertTurn(convKey, entry)
		return
	}
	for {
		st.snapMu.Lock()
		if st.pendingSnaps != nil {
			delete(st.pendingSnaps, snap.ConvKey)
		}
		flushing := st.snapFlushing
		doneCh := st.snapFlushDone
		st.snapMu.Unlock()
		if !flushing {
			break
		}
		if doneCh != nil {
			<-doneCh
		}
	}
	inTok, crTok, outTok, rawIn, rawCr, steps := extractTurnInsertTokens(entry)
	src := extractTurnInsertSource(entry)

	st.mu.Lock()
	defer st.mu.Unlock()
	if st.db == nil {
		return
	}
	tx, err := st.db.Begin()
	if err != nil {
		LogEvent("STORE_WRITE_ERR", "insert turn+session begin failed", map[string]any{"error": err.Error()})
		return
	}
	defer tx.Rollback()
	if _, err := tx.Exec(insertTurnSQL,
		convKey, entry.SessionID, entry.TurnIndex, entry.Mode, entry.Model, entry.ThoughtLevel,
		entry.StopReason, entry.ElapsedMs, entry.TtftMs, entry.UpstreamTtftMs, entry.StreamMs, entry.Tps, entry.DecodeTps, inTok, crTok, outTok,
		rawIn, rawCr, entry.ThinkingTokens, entry.ThinkingMs, entry.RetryCount, steps, src, entry.CacheHitRate, entry.CompletedAtMs,
		entry.ServedModel, entry.CacheMiss, entry.ClientID, entry.ClientLabel, entry.Protocol,
	); err != nil {
		LogEvent("STORE_WRITE_ERR", "insert turn failed", map[string]any{"error": err.Error()})
		return
	}
	usedHot := false
	if !st.isStaleSnapshotLocked(snap) {
		var upErr error
		usedHot, upErr = st.execSnapshotUpsertOrHotLocked(tx, snap)
		if upErr != nil {
			LogEvent("STORE_WRITE_ERR", "upsert session failed", map[string]any{"error": upErr.Error()})
			return
		}
	}
	if !usedHot || entry.SessionID != snap.SessionID {
		if entry.SessionID != "" {
			_, _ = tx.Exec(`INSERT OR IGNORE INTO known_gateway_sids (session_id) VALUES (?)`, strings.ToLower(entry.SessionID))
		}
		if snap.SessionID != "" && snap.SessionID != entry.SessionID {
			_, _ = tx.Exec(`INSERT OR IGNORE INTO known_gateway_sids (session_id) VALUES (?)`, strings.ToLower(snap.SessionID))
		}
	}
	if err := tx.Commit(); err != nil {
		LogEvent("STORE_WRITE_ERR", "insert turn+session commit failed", map[string]any{"error": err.Error()})
		return
	}
	if !st.isStaleSnapshotLocked(snap) {
		st.recordCommittedBlobGenLocked(snap)
		st.recordCommittedSnapSeqLocked(snap)
	}
	st.version.Add(1)
}

const insertTurnSQL = `
INSERT INTO turns (
	conv_key, agy_session_id, turn_idx, mode, model_id, thought_level,
	stop_reason, elapsed_ms, ttft_ms, upstream_ttft_ms, stream_ms, tps, decode_tps, in_tok, cached_tok, out_tok,
	raw_in_tok, raw_cr_tok, thinking_tok, thinking_ms, retry_count, steps_in_prompt, usage_source,
	cache_hit_rate, completed_at_ms, served_model, cache_miss, client_id, client_label, protocol
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`

func extractTurnInsertTokens(entry TurnHistoryEntry) (inTok, crTok, outTok, rawIn, rawCr, steps int) {
	steps = 1
	if entry.Usage != nil {
		inTok = entry.Usage.InputTokens
		crTok = entry.Usage.CachedReadTokens
		outTok = entry.Usage.OutputTokens
		rawIn = entry.Usage.RawUncachedInputTokens
		rawCr = entry.Usage.RawTpuCachedTokens
		if entry.Usage.StepsInPrompt > 0 || entry.Usage.UsageSource == "none" {
			steps = entry.Usage.StepsInPrompt
		}
	}
	return inTok, crTok, outTok, rawIn, rawCr, steps
}

func extractTurnInsertSource(entry TurnHistoryEntry) string {
	if entry.Usage != nil && entry.Usage.UsageSource != "" {
		return entry.Usage.UsageSource
	}
	return "estimate"
}

func (st *StateStore) InsertTurn(convKey string, entry TurnHistoryEntry) {
	if strings.HasPrefix(convKey, "util:") {
		return
	}
	inTok, crTok, outTok, rawIn, rawCr, steps := extractTurnInsertTokens(entry)
	src := extractTurnInsertSource(entry)
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.db == nil {
		return
	}
	_, err := st.db.Exec(insertTurnSQL,
		convKey, entry.SessionID, entry.TurnIndex, entry.Mode, entry.Model, entry.ThoughtLevel,
		entry.StopReason, entry.ElapsedMs, entry.TtftMs, entry.UpstreamTtftMs, entry.StreamMs, entry.Tps, entry.DecodeTps, inTok, crTok, outTok,
		rawIn, rawCr, entry.ThinkingTokens, entry.ThinkingMs, entry.RetryCount, steps, src, entry.CacheHitRate, entry.CompletedAtMs,
		entry.ServedModel, entry.CacheMiss, entry.ClientID, entry.ClientLabel, entry.Protocol,
	)
	if err != nil {
		LogEvent("STORE_WRITE_ERR", "insert turn failed", map[string]any{"error": err.Error()})
		return
	}
	if entry.SessionID != "" {
		_, _ = st.db.Exec(`INSERT OR IGNORE INTO known_gateway_sids (session_id) VALUES (?)`, strings.ToLower(entry.SessionID))
	}
	st.version.Add(1)
}

func scanRestoredConvRow(scan func(dest ...any) error) (restoredConvRow, error) {
	var r restoredConvRow
	err := scan(
		&r.convKey, &r.sid, &r.tenantKey, &r.cwd, &r.modelID, &r.tier,
		&r.toolFp, &r.initToolsJSON, &r.curToolsJSON, &r.sysText, &r.initSysText, &r.initAppendPrompt,
		&r.msgCnt, &r.histFp, &r.lastCached, &r.lastDbIdx, &r.lastGenIdx,
		&r.clientID, &r.clientLabel, &r.protocol,
		&r.reuses, &r.rolls, &r.createdMs, &r.updatedMs,
	)
	return r, err
}

type genMetaBackfill struct {
	convKey string
	maxGen  int
}

func buildSessionFromRow(rDB *sql.DB, r restoredConvRow, backfills *[]genMetaBackfill) *AcpProjectSession {
	sess := NewAcpProjectSession(r.convKey, r.cwd, r.sysText)
	sess.persistedInDB = true
	if r.initSysText != "" {
		sess.InitialSystemText = r.initSysText
	} else {
		sess.InitialSystemText = r.sysText
	}
	sess.InitialAppendPrompt = r.initAppendPrompt
	if r.tenantKey != "" {
		sess.TenantKey = r.tenantKey
		sess.Codec.SetTenantKey(r.tenantKey)
	}
	sess.HibernatedSessionID = r.sid
	sess.ModelID = r.modelID
	sess.ThoughtLevel = r.tier
	sess.ToolFingerprint = r.toolFp
	if r.initToolsJSON != "" {
		var initCat []ToolDef
		if unmarshalJSONPreserveNumbers([]byte(r.initToolsJSON), &initCat) == nil && len(initCat) > 0 {
			sess.DeclaredMcpTools = make(map[string]bool, len(initCat))
			for i := range initCat {
				if initCat[i].McpName == "" {
					initCat[i].McpName = mcpToolName(initCat[i].Name)
				}
				sess.DeclaredMcpTools[initCat[i].Name] = true
				sess.McpToHostMap[initCat[i].McpName] = initCat[i].Name
			}
			sess.InitialToolsCatalog = initCat
		}
	}
	if r.curToolsJSON != "" {
		var curCat []ToolDef
		if unmarshalJSONPreserveNumbers([]byte(r.curToolsJSON), &curCat) == nil && len(curCat) > 0 {
			for i := range curCat {
				if curCat[i].McpName == "" {
					curCat[i].McpName = mcpToolName(curCat[i].Name)
				}
				sess.McpToHostMap[curCat[i].McpName] = curCat[i].Name
			}
			sess.ToolsCatalog = curCat
		}
	} else if len(sess.InitialToolsCatalog) > 0 {
		sess.ToolsCatalog = append([]ToolDef(nil), sess.InitialToolsCatalog...)
	}
	sess.MessageCount = r.msgCnt
	sess.lastTurnAssistant = r.msgCnt > 0
	sess.HistoryFingerprint = r.histFp
	sess.LastCachedReadTokens = r.lastCached
	sess.LastSeenDbStepIdx = r.lastDbIdx
	sess.LastSeenGenMetaIdx = r.lastGenIdx
	if sess.LastSeenGenMetaIdx < 0 && r.sid != "" && r.msgCnt > 0 {
		if maxGen := ReadMaxSessionGenMetadataIdx(r.sid); maxGen >= 0 {
			sess.LastSeenGenMetaIdx = maxGen
			if backfills != nil {
				*backfills = append(*backfills, genMetaBackfill{convKey: r.convKey, maxGen: maxGen})
			}
		}
	}
	if r.clientID != "" {
		sess.ClientID = r.clientID
		sess.ClientLabel = r.clientLabel
		sess.Protocol = r.protocol
	} else {
		ci := InferHistoricalClientIdentity(r.convKey, r.sysText, r.initToolsJSON, r.curToolsJSON)
		sess.ClientID = ci.ID
		sess.ClientLabel = ci.Label
		sess.Protocol = ci.Protocol
	}
	sess.SessionReuses = r.reuses
	sess.CompactionRollovers = r.rolls
	sess.CreatedAt = time.UnixMilli(r.createdMs)
	sess.LastUsedAt = time.UnixMilli(r.updatedMs)

	var totalCnt, maxTurnIdx int
	_ = rDB.QueryRow(`SELECT COUNT(*), COALESCE(MAX(turn_idx), 0) FROM turns WHERE conv_key = ?`, r.convKey).Scan(&totalCnt, &maxTurnIdx)
	if maxTurnIdx > totalCnt {
		totalCnt = maxTurnIdx
	}
	sess.TotalTurns = totalCnt

	// Load up to 150 most recent turns for this conversation in chronological order
	tRows, err := rDB.Query(`
SELECT turn_idx, completed_at_ms, agy_session_id, mode, elapsed_ms, ttft_ms, upstream_ttft_ms, stream_ms, tps, decode_tps,
       thinking_tok, thinking_ms, retry_count, in_tok, cached_tok, out_tok, raw_in_tok, raw_cr_tok,
       steps_in_prompt, usage_source, cache_hit_rate, stop_reason, model_id, thought_level,
       served_model, cache_miss, client_id, client_label, protocol
FROM (
	SELECT id, turn_idx, completed_at_ms, agy_session_id, mode, elapsed_ms, ttft_ms, upstream_ttft_ms, stream_ms, tps, decode_tps,
	       thinking_tok, thinking_ms, retry_count, in_tok, cached_tok, out_tok, raw_in_tok, raw_cr_tok,
	       steps_in_prompt, usage_source, cache_hit_rate, stop_reason, model_id, thought_level,
	       served_model, cache_miss, client_id, client_label, protocol
	FROM turns
	WHERE conv_key = ?
	ORDER BY id DESC
	LIMIT 150
)
ORDER BY id ASC
`, r.convKey)
	if err == nil {
		for tRows.Next() {
			var te TurnHistoryEntry
			var u TurnUsage
			if err := tRows.Scan(
				&te.TurnIndex, &te.CompletedAtMs, &te.SessionID, &te.Mode, &te.ElapsedMs, &te.TtftMs, &te.UpstreamTtftMs, &te.StreamMs, &te.Tps, &te.DecodeTps,
				&te.ThinkingTokens, &te.ThinkingMs, &te.RetryCount, &u.InputTokens, &u.CachedReadTokens, &u.OutputTokens, &u.RawUncachedInputTokens, &u.RawTpuCachedTokens,
				&u.StepsInPrompt, &u.UsageSource, &te.CacheHitRate, &te.StopReason, &te.Model, &te.ThoughtLevel,
				&te.ServedModel, &te.CacheMiss, &te.ClientID, &te.ClientLabel, &te.Protocol,
			); err == nil {
				te.Ts = time.UnixMilli(te.CompletedAtMs).UTC().Format(time.RFC3339)
				te.HasThinking = te.ThinkingTokens > 0
				if te.ClientID == "" {
					te.ClientID = sess.ClientID
					te.ClientLabel = sess.ClientLabel
				}
				if te.Protocol == "" {
					te.Protocol = sess.Protocol
				}
				if te.StreamMs > te.ElapsedMs && te.ElapsedMs > 0 {
					if te.ElapsedMs > te.TtftMs && te.TtftMs > 0 {
						te.StreamMs = te.ElapsedMs - te.TtftMs
					} else {
						te.StreamMs = te.ElapsedMs
					}
				}
				if u.OutputTokens > 0 && (te.StreamMs > 0 || (te.TtftMs > 0 && te.ElapsedMs > te.TtftMs)) {
					_, te.StreamMs, _, _, te.DecodeTps = computeStepTiming(te.TtftMs, te.StreamMs, te.ElapsedMs, u.OutputTokens)
				}
				if te.StreamMs > 0 && te.ThinkingMs > te.StreamMs {
					te.ThinkingMs = te.StreamMs
				}
				if te.ThinkingMs == 0 && te.ThinkingTokens > 0 && te.StreamMs > 0 && u.OutputTokens >= te.ThinkingTokens {
					te.ThinkingMs = int(math.Round(float64(te.StreamMs) * float64(te.ThinkingTokens) / float64(u.OutputTokens)))
				}
				u.GenTtftMs = te.UpstreamTtftMs
				u.StreamMs = te.StreamMs
				u.DecodeTps = te.DecodeTps
				if (u.UsageSource == "db" || u.UsageSource == "acp") && u.StepsInPrompt > 0 {
					u.Generations = u.StepsInPrompt
				}
				uCopy := u
				te.Usage = &uCopy
				sess.TurnHistory = append(sess.TurnHistory, te)
			}
		}
		_ = tRows.Close()
	}
	// Repair legacy #151 ring-cap collisions in restored history so dashboard turn numbers are monotonic.
	if n := len(sess.TurnHistory); n > 1 && sess.TotalTurns >= n {
		hasCollision := false
		for i := 1; i < n; i++ {
			if sess.TurnHistory[i].TurnIndex <= sess.TurnHistory[i-1].TurnIndex {
				hasCollision = true
				break
			}
		}
		if hasCollision {
			startIdx := sess.TotalTurns - n + 1
			for i := 0; i < n; i++ {
				sess.TurnHistory[i].TurnIndex = startIdx + i
			}
		}
	}
	return sess
}

func (st *StateStore) applyGenMetaBackfills(backfills []genMetaBackfill) {
	if len(backfills) == 0 {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.db == nil {
		return
	}
	for _, b := range backfills {
		_, _ = st.db.Exec(`UPDATE conversations SET last_gen_meta_idx = ? WHERE conv_key = ?`, b.maxGen, b.convKey)
		_, _ = st.db.Exec(`UPDATE conversation_hot SET last_gen_meta_idx = ? WHERE conv_key = ?`, b.maxGen, b.convKey)
	}
}

func isUniqueSessionIDWithDB(rDB *sql.DB, sid string) bool {
	if rDB == nil || sid == "" {
		return false
	}
	var cnt int
	if err := rDB.QueryRow(`SELECT COUNT(*) FROM conversations WHERE agy_session_id = ?`, sid).Scan(&cnt); err != nil {
		return false
	}
	return cnt == 1
}

const selectRestoredConvPrefix = `
SELECT c.conv_key, c.agy_session_id, c.tenant_key, c.cwd, c.model_id, c.thought_level,
       c.tool_fp, c.init_tools_json, c.cur_tools_json, c.system_text, c.init_system_text, c.init_append_prompt,
       COALESCE(h.msg_count, c.msg_count),
       COALESCE(h.history_fp, c.history_fp),
       COALESCE(h.last_cached_tok, c.last_cached_tok),
       COALESCE(h.last_db_step_idx, c.last_db_step_idx),
       COALESCE(h.last_gen_meta_idx, c.last_gen_meta_idx),
       COALESCE(NULLIF(h.client_id, ''), c.client_id),
       COALESCE(NULLIF(h.client_label, ''), c.client_label),
       COALESCE(NULLIF(h.protocol, ''), c.protocol),
       MAX(c.session_reuses, COALESCE(h.session_reuses, 0)),
       MAX(c.compaction_rollovers, COALESCE(h.compaction_rollovers, 0)),
       c.created_at_ms,
       MAX(c.updated_at_ms, COALESCE(h.updated_at_ms, 0))
FROM conversations c
LEFT JOIN conversation_hot h ON h.conv_key = c.conv_key AND h.agy_session_id = c.agy_session_id
`

// RestoreSingleSession lazily reloads an evicted hibernated session from gateway.db within the 7-day
// upstream recovery window, preserving TenantKey/Codec, HibernatedSessionID, and monotonic counters.
func (st *StateStore) RestoreSingleSession(convKey string) *AcpProjectSession {
	if convKey == "" {
		return nil
	}
	st.Flush()
	rDB, relRead := st.acquireReadDB()
	if rDB == nil {
		return nil
	}
	cutoffMs := time.Now().Add(-7 * 24 * time.Hour).UnixMilli()
	row := rDB.QueryRow(selectRestoredConvPrefix+`
WHERE c.conv_key = ? AND c.agy_session_id != '' AND COALESCE(h.msg_count, c.msg_count) > 0 AND MAX(c.updated_at_ms, COALESCE(h.updated_at_ms, 0)) >= ?
`, convKey, cutoffMs)
	r, err := scanRestoredConvRow(row.Scan)
	if err != nil || r.sid == "" {
		relRead()
		return nil
	}
	if !isUniqueSessionIDWithDB(rDB, r.sid) {
		relRead()
		LogEvent("STATE_RESTORE_CONFLICT", "Skipped ambiguous duplicate ACP session on lazy restore; next request will cold start", nil)
		return nil
	}
	var backfills []genMetaBackfill
	sess := buildSessionFromRow(rDB, r, &backfills)
	relRead()
	st.applyGenMetaBackfills(backfills)
	return sess
}

func (st *StateStore) RestoreSessionByIdentityFingerprint(idPrefix, cwd, _ string, historyFp string) *AcpProjectSession {
	if idPrefix == "" || historyFp == "" {
		return nil
	}
	st.Flush()
	rDB, relRead := st.acquireReadDB()
	if rDB == nil {
		return nil
	}
	cutoffMs := time.Now().Add(-7 * 24 * time.Hour).UnixMilli()
	rows, err := rDB.Query(selectRestoredConvPrefix+`
WHERE c.conv_key LIKE ? AND c.cwd = ? AND COALESCE(h.history_fp, c.history_fp) = ? AND c.agy_session_id != '' AND COALESCE(h.msg_count, c.msg_count) > 0 AND MAX(c.updated_at_ms, COALESCE(h.updated_at_ms, 0)) >= ?
LIMIT 2
`, idPrefix+":%", cwd, historyFp, cutoffMs)
	if err != nil {
		relRead()
		return nil
	}
	var list []restoredConvRow
	for rows.Next() {
		if r, sErr := scanRestoredConvRow(rows.Scan); sErr == nil {
			list = append(list, r)
		}
	}
	_ = rows.Close()
	if len(list) != 1 || stripSlotSuffix(list[0].convKey) == "" || explicitConversationIdentity(stripSlotSuffix(list[0].convKey)) != idPrefix {
		relRead()
		return nil
	}
	if !isUniqueSessionIDWithDB(rDB, list[0].sid) {
		relRead()
		LogEvent("STATE_RESTORE_CONFLICT", "Skipped ambiguous duplicate ACP session on continuity restore", nil)
		return nil
	}
	var backfills []genMetaBackfill
	sess := buildSessionFromRow(rDB, list[0], &backfills)
	relRead()
	st.applyGenMetaBackfills(backfills)
	return sess
}

func (st *StateStore) RestoreFpSessionByHistoryFingerprint(agentScope, cwd, historyFp string, maxMsgCount int) *AcpProjectSession {
	if historyFp == "" || maxMsgCount <= 0 {
		return nil
	}
	st.Flush()
	rDB, relRead := st.acquireReadDB()
	if rDB == nil {
		return nil
	}
	cutoffMs := time.Now().Add(-7 * 24 * time.Hour).UnixMilli()
	rows, err := rDB.Query(selectRestoredConvPrefix+`
WHERE c.conv_key LIKE 'fp:%' AND c.cwd = ? AND COALESCE(h.history_fp, c.history_fp) = ? AND c.agy_session_id != '' AND COALESCE(h.msg_count, c.msg_count) > 0 AND COALESCE(h.msg_count, c.msg_count) <= ? AND MAX(c.updated_at_ms, COALESCE(h.updated_at_ms, 0)) >= ?
LIMIT 4
`, cwd, historyFp, maxMsgCount, cutoffMs)
	if err != nil {
		relRead()
		return nil
	}
	var list []restoredConvRow
	for rows.Next() {
		if r, sErr := scanRestoredConvRow(rows.Scan); sErr == nil {
			if extractFpAgentScope(r.convKey) == agentScope {
				list = append(list, r)
			}
		}
	}
	_ = rows.Close()
	if len(list) != 1 {
		relRead()
		return nil
	}
	if !isUniqueSessionIDWithDB(rDB, list[0].sid) {
		relRead()
		LogEvent("STATE_RESTORE_CONFLICT", "Skipped ambiguous duplicate ACP session on headerless continuity restore", nil)
		return nil
	}
	var backfills []genMetaBackfill
	sess := buildSessionFromRow(rDB, list[0], &backfills)
	relRead()
	st.applyGenMetaBackfills(backfills)
	return sess
}

func (st *StateStore) RestoreDetachedSessions(gs *GatewayServer) int {
	if gs == nil {
		return 0
	}
	st.Flush()
	st.mu.Lock()
	if st.db == nil {
		st.mu.Unlock()
		return 0
	}
	_, _ = st.db.Exec(`UPDATE conversations SET state = 'Detached' WHERE state != 'Detached'`)
	_, _ = st.db.Exec(`UPDATE conversation_hot SET state = 'Detached' WHERE state != 'Detached'`)
	st.mu.Unlock()

	rDB, relRead := st.acquireReadDB()
	if rDB == nil {
		return 0
	}
	cutoffMs := time.Now().Add(-7 * 24 * time.Hour).UnixMilli()
	rows, err := rDB.Query(selectRestoredConvPrefix+`
WHERE c.agy_session_id != '' AND COALESCE(h.msg_count, c.msg_count) > 0 AND MAX(c.updated_at_ms, COALESCE(h.updated_at_ms, 0)) >= ?
ORDER BY c.created_at_ms ASC
`, cutoffMs)
	if err != nil {
		relRead()
		return 0
	}

	var list []restoredConvRow
	for rows.Next() {
		if r, sErr := scanRestoredConvRow(rows.Scan); sErr == nil {
			list = append(list, r)
		}
	}
	_ = rows.Close()

	// Ambiguous legacy rows are kept on disk but never resumed under either identity.
	sidCounts := make(map[string]int)
	for _, r := range list {
		sidCounts[r.sid]++
	}
	var built []*AcpProjectSession
	var backfills []genMetaBackfill
	for _, r := range list {
		if sidCounts[r.sid] > 1 {
			LogEvent("STATE_RESTORE_CONFLICT", "Skipped ambiguous duplicate ACP session; next request will cold start", nil)
			continue
		}
		built = append(built, buildSessionFromRow(rDB, r, &backfills))
	}
	relRead()
	st.applyGenMetaBackfills(backfills)

	restored := 0
	gs.mu.Lock()
	defer gs.mu.Unlock()
	for _, sess := range built {
		if _, exists := gs.sessions[sess.ConvKey]; exists {
			continue
		}
		sess.onBeforeSpawn = gs.reserveWorkerSlot
		gs.sessions[sess.ConvKey] = sess
		gs.tenants[sess.TenantKey] = sess
		RegisterGatewaySessionID(sess.HibernatedSessionID)
		restored++
	}
	return restored
}

func (st *StateStore) LoadKnownGatewaySids(dest map[string]bool) {
	st.Flush()
	rDB, relRead := st.acquireReadDB()
	if rDB == nil {
		return
	}
	defer relRead()
	rows, err := rDB.Query(`
SELECT DISTINCT agy_session_id FROM conversations WHERE agy_session_id != ''
UNION SELECT DISTINCT agy_session_id FROM turns WHERE agy_session_id != ''
UNION SELECT session_id FROM known_gateway_sids WHERE session_id != ''
`)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var sid string
		if rows.Scan(&sid) == nil && sid != "" {
			dest[strings.ToLower(sid)] = true
		}
	}
}

func (st *StateStore) IsCursorUpToDate(sid string, dbSize, dbMtimeNs, walSize, walMtimeNs int64) bool {
	if sid == "" || !st.IsReady() {
		return false
	}
	st.cursorMu.RLock()
	defer st.cursorMu.RUnlock()
	c, ok := st.cursors[sid]
	if !ok {
		c, ok = st.cursors[strings.ToLower(sid)]
	}
	if !ok {
		return false
	}
	return c.dbSize == dbSize && c.dbMtimeNs == dbMtimeNs && c.walSize == walSize && c.walMtimeNs == walMtimeNs
}

func (st *StateStore) ClearCleanedConversationSid(sid string) {
	if sid == "" {
		return
	}
	st.Flush()
	low := strings.ToLower(sid)
	st.mu.Lock()
	defer st.mu.Unlock()
	st.cursorMu.Lock()
	if st.cursors != nil {
		delete(st.cursors, sid)
		delete(st.cursors, low)
	}
	st.cursorMu.Unlock()
	if st.db == nil {
		return
	}
	_, _ = st.db.Exec(`INSERT OR IGNORE INTO known_gateway_sids (session_id) VALUES (?)`, low)
	_, _ = st.db.Exec(`DELETE FROM ingest_cursor WHERE lower(session_id) = ?`, low)
	_, _ = st.db.Exec(`UPDATE conversations SET agy_session_id = '' WHERE lower(agy_session_id) = ?`, low)
	_, _ = st.db.Exec(`DELETE FROM conversation_hot WHERE lower(agy_session_id) = ?`, low)
	st.committedBlobGen = nil
	st.committedSnapSeq = nil
	st.version.Add(1)
}

func (st *StateStore) SyncUsageStepsFromDir(convDir string, gwSids map[string]bool) int {
	return st.SyncUsageStepsFromDirWithRODB(convDir, gwSids, nil)
}

func (st *StateStore) SyncUsageStepsFromDirWithRODB(convDir string, gwSids map[string]bool, roDBLookup func(sid string) *sql.DB) int {
	return st.syncUsageStepsFromSourceDirInternal(convDir, "HOST", "AGY_CLI", "AGY CLI", gwSids, roDBLookup)
}

func (st *StateStore) SyncUsageStepsFromSourceDir(convDir, defaultSourceSide, defaultSourceCat, defaultSourceLabel string, gwSids map[string]bool) int {
	return st.syncUsageStepsFromSourceDirInternal(convDir, defaultSourceSide, defaultSourceCat, defaultSourceLabel, gwSids, nil)
}

func (st *StateStore) syncUsageStepsFromSourceDirInternal(convDir, defaultSourceSide, defaultSourceCat, defaultSourceLabel string, gwSids map[string]bool, roDBLookup func(sid string) *sql.DB) int {
	if strings.TrimSpace(convDir) == "" {
		return 0
	}
	entries, err := os.ReadDir(convDir)
	if err != nil {
		return 0
	}
	scanStartNs := time.Now().UnixNano()
	const coarseMtimeGuardNs = int64(20 * time.Millisecond)

	type pendingSource struct {
		sid       string
		cursorKey string
		filePath  string
		isPB      bool
		meta      cursorMeta
	}
	dbStems := make(map[string]bool)
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".db") {
			if info, errI := e.Info(); errI == nil && info.Size() >= 100 {
				dbStems[strings.ToLower(strings.TrimSuffix(e.Name(), ".db"))] = true
			}
		}
	}

	var toScan []pendingSource
	isAppSource := defaultSourceCat == "AGY_APP" || defaultSourceSide == "APP"
	cursorPrefix := ""
	if isAppSource {
		cursorPrefix = "app:"
	}

	if !st.IsReady() {
		return 0
	}
	st.cursorMu.RLock()
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		isDB := strings.HasSuffix(name, ".db")
		isPB := !isDB && strings.HasSuffix(name, ".pb")
		if !isDB && !isPB {
			continue
		}
		var sid, cursorKey string
		if isDB {
			sid = strings.TrimSuffix(name, ".db")
			cursorKey = cursorPrefix + sid
		} else {
			sid = strings.TrimSuffix(name, ".pb")
			if dbStems[strings.ToLower(sid)] {
				continue
			}
			cursorKey = cursorPrefix + sid + ".pb"
		}
		filePath := filepath.Join(convDir, name)
		fInfo, err := e.Info()
		if err != nil || (isDB && fInfo.Size() < 100) || (isPB && fInfo.Size() == 0) {
			continue
		}
		var walSize, walMtime int64
		if isDB {
			if wInfo, errW := os.Stat(filePath + "-wal"); errW == nil {
				walSize = wInfo.Size()
				walMtime = wInfo.ModTime().UnixNano()
			}
		}
		prev, hasPrev := st.cursors[cursorKey]
		needForkDedupPass := isAppSource && isDB && prev.forkDedupVer < currentAppForkDedupVer
		if hasPrev && !needForkDedupPass &&
			prev.dbSize == fInfo.Size() &&
			prev.dbMtimeNs == fInfo.ModTime().UnixNano() &&
			prev.walSize == walSize &&
			prev.walMtimeNs == walMtime {
			continue
		}
		afterIdx := -1
		if hasPrev {
			afterIdx = prev.lastStepIdx
		}
		dedupVer := prev.forkDedupVer
		if isAppSource {
			dedupVer = currentAppForkDedupVer
		}
		toScan = append(toScan, pendingSource{
			sid:       sid,
			cursorKey: cursorKey,
			filePath:  filePath,
			isPB:      isPB,
			meta: cursorMeta{
				dbSize:       fInfo.Size(),
				dbMtimeNs:    fInfo.ModTime().UnixNano(),
				walSize:      walSize,
				walMtimeNs:   walMtime,
				lastStepIdx:  afterIdx,
				forkDedupVer: dedupVer,
			},
		})
	}
	st.cursorMu.RUnlock()

	if len(toScan) == 0 {
		return 0
	}

	ingested := 0
	for _, item := range toScan {
		var steps []ParsedModelStep
		maxForkIdx := -1
		var ok bool
		var liveRO *sql.DB
		if item.isPB {
			steps, ok = ReadModelStepsFromProtobuf(item.filePath, defaultSourceSide, defaultSourceCat, defaultSourceLabel, gwSids, item.meta.lastStepIdx, item.meta.dbMtimeNs/1e9)
		} else if isAppSource {
			steps, maxForkIdx, ok = readModelStepsFromSQLiteAfterInternal(item.filePath, defaultSourceSide, gwSids, item.meta.lastStepIdx)
		} else {
			if roDBLookup != nil {
				liveRO = roDBLookup(item.sid)
				if liveRO != nil {
					steps, ok = queryModelStepsFromDB(liveRO, strings.ToLower(item.sid), defaultSourceSide, gwSids, item.meta.lastStepIdx, item.meta.dbMtimeNs/1e9)
				}
			}
			if !ok {
				steps, ok = ReadModelStepsSQL(item.filePath, defaultSourceSide, gwSids, item.meta.lastStepIdx, item.meta.dbMtimeNs/1e9)
			}
			if !ok {
				steps, ok = ReadModelStepsFromSQLiteAfter(item.filePath, defaultSourceSide, gwSids, item.meta.lastStepIdx)
			}
		}
		if !ok {
			LogEvent("STORE_SYNC_ERR", "Cannot read usage source; retaining cursor for retry", nil)
			continue
		}
		for i := range steps {
			isGw := steps[i].SourceCategory == "CLAUDE_GW" || steps[i].IsDshBridge || (gwSids != nil && gwSids[strings.ToLower(steps[i].SessionID)])
			steps[i].SourceSide, steps[i].SourceCategory, steps[i].SourceLabel = classifyStepSource(defaultSourceSide, defaultSourceCat, defaultSourceLabel, isGw)
		}
		maxIdx := item.meta.lastStepIdx
		if isAppSource && maxForkIdx > maxIdx {
			maxIdx = maxForkIdx
		}
		for _, s := range steps {
			if s.StepIdx > maxIdx {
				maxIdx = s.StepIdx
			}
		}
		persistMeta := item.meta
		persistMeta.lastStepIdx = maxIdx
		// Linux coarse-mtime guard: when db/wal mtime is within 20ms of scanStartNs, ingest steps
		// immediately so telemetry is live, but leave mtime unstamped (0) so the next pass >= 20ms
		// later verifies no same-jiffy WAL write landed after our read.
		if scanStartNs-persistMeta.dbMtimeNs < coarseMtimeGuardNs {
			persistMeta.dbMtimeNs = 0
		}
		if persistMeta.walMtimeNs > 0 && scanStartNs-persistMeta.walMtimeNs < coarseMtimeGuardNs {
			persistMeta.walMtimeNs = 0
		}

		purgeAppSid := ""
		if isAppSource && maxForkIdx >= 0 {
			purgeAppSid = strings.ToLower(item.sid)
		}
		n, err := st.persistUsageBatchWithForkPurge(item.cursorKey, persistMeta, steps, purgeAppSid, maxForkIdx)
		if err != nil {
			LogEvent("STORE_SYNC_ERR", "Usage batch rolled back; cursor retained", map[string]any{"error": err.Error()})
			continue
		}
		if !item.isPB {
			st.backfillEmptyServedModels(item.sid, item.filePath, isAppSource, liveRO)
		}
		ingested += n
	}
	return ingested
}

// backfillEmptyServedModels fills served_model on rows ingested before gen_metadata landed in WAL.
// When pureImageOnly is true (e.g. AppConversationsDir()), openReadOnlySQLite is bypassed completely
// so modernc.org/sqlite VFS never creates/unlinks -shm/-wal files in writable host directories.
func (st *StateStore) backfillEmptyServedModels(sid, dbPath string, pureImageOnly bool, liveRO *sql.DB) {
	rDB, relRead := st.acquireReadDB()
	if rDB == nil {
		return
	}
	var missingCnt int
	_ = rDB.QueryRow(`SELECT COUNT(*) FROM usage_steps WHERE session_id = ? AND served_model = ''`, strings.ToLower(sid)).Scan(&missingCnt)
	relRead()
	if missingCnt == 0 {
		return
	}
	var byStep map[int]string
	var byTok map[[3]int]string
	if !pureImageOnly {
		if liveRO != nil {
			byStep, byTok = readSessionServedModels(liveRO)
		}
		if len(byStep) == 0 && len(byTok) == 0 {
			if srcDB, err := openReadOnlySQLite(dbPath); err == nil {
				byStep, byTok = readSessionServedModels(srcDB)
				_ = srcDB.Close()
			}
		}
	}
	if len(byStep) == 0 && len(byTok) == 0 {
		if img, errI := openSQLiteImage(dbPath); errI == nil {
			mStep, mTok := readGenMetaIndexFromImage(img)
			if len(mStep) > 0 || len(mTok) > 0 {
				byStep = map[int]string{}
				byTok = map[[3]int]string{}
				for k, v := range mStep {
					if v.ServedModel != "" {
						byStep[k] = v.ServedModel
					}
				}
				for k, v := range mTok {
					if v.ServedModel != "" {
						byTok[k] = v.ServedModel
					}
				}
			}
		}
	}
	if len(byStep) == 0 && len(byTok) == 0 {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.db == nil {
		return
	}
	rows, err := st.db.Query(`SELECT step_idx, raw_in, raw_cr, out_tok FROM usage_steps WHERE session_id = ? AND served_model = ''`, strings.ToLower(sid))
	if err != nil {
		return
	}
	type patch struct {
		idx   int
		model string
	}
	var patches []patch
	for rows.Next() {
		var idx, rIn, rCr, outT int
		if rows.Scan(&idx, &rIn, &rCr, &outT) == nil {
			if m, ok := byStep[idx]; ok && m != "" {
				patches = append(patches, patch{idx: idx, model: m})
			} else if m, ok := byTok[[3]int{rIn, rCr, outT}]; ok && m != "" {
				patches = append(patches, patch{idx: idx, model: m})
			}
		}
	}
	_ = rows.Close()
	for _, p := range patches {
		_, _ = st.db.Exec(`UPDATE usage_steps SET served_model = ? WHERE session_id = ? AND step_idx = ? AND served_model = ''`, p.model, strings.ToLower(sid), p.idx)
	}
	if len(patches) > 0 {
		st.coldEpoch.Add(1)
		st.version.Add(1)
	}
}

// TurnMissKey is an alias for gwstore.TurnMissKey.

func (st *StateStore) QueryTurnCacheMissIndex() (byStep map[TurnMissKey]string, byFirstSession map[string]string) {
	byStep = map[TurnMissKey]string{}
	byFirstSession = map[string]string{}
	rDB, relRead := st.acquireReadDB()
	if rDB == nil {
		return byStep, byFirstSession
	}
	defer relRead()
	rows, err := rDB.Query(`
SELECT agy_session_id, mode, in_tok, cached_tok, out_tok, raw_in_tok, raw_cr_tok, cache_miss
FROM turns
WHERE agy_session_id != '' AND cache_miss != ''
ORDER BY id DESC
LIMIT 500
`)
	if err != nil {
		return byStep, byFirstSession
	}
	defer rows.Close()
	for rows.Next() {
		var sid, mode, miss string
		var inT, crT, outT, rawIn, rawCr int
		if rows.Scan(&sid, &mode, &inT, &crT, &outT, &rawIn, &rawCr, &miss) != nil {
			continue
		}
		lsid := strings.ToLower(sid)
		if rawIn > 0 || rawCr > 0 {
			k := TurnMissKey{SessionID: lsid, InTok: rawIn, CrTok: rawCr, OutTok: outT}
			if _, exists := byStep[k]; !exists {
				byStep[k] = miss
			}
		}
		if inT > 0 || crT > 0 {
			k := TurnMissKey{SessionID: lsid, InTok: inT, CrTok: crT, OutTok: outT}
			if _, exists := byStep[k]; !exists {
				byStep[k] = miss
			}
		}
		if mode == "compaction_rollover" || mode == "argon_degraded_recovery" || mode == "high_capacity_fallback" || mode == "waf_autobisect_recovery" || mode == "cold_start" {
			if _, exists := byFirstSession[lsid]; !exists {
				byFirstSession[lsid] = miss
			}
		}
	}
	return byStep, byFirstSession
}

// A cursor is committed in the same transaction as its steps; memory follows commit.
func (st *StateStore) persistUsageBatch(sid string, meta cursorMeta, steps []ParsedModelStep) (int, error) {
	return st.persistUsageBatchWithForkPurge(sid, meta, steps, "", -1)
}

func (st *StateStore) persistUsageBatchWithForkPurge(sid string, meta cursorMeta, steps []ParsedModelStep, purgeAppSid string, maxForkIdx int) (int, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.db == nil {
		return 0, fmt.Errorf("state store unavailable")
	}
	st.cursorMu.RLock()
	previous, hasPrev := st.cursors[sid]
	st.cursorMu.RUnlock()
	if hasPrev && previous.lastStepIdx > meta.lastStepIdx && previous.forkDedupVer >= meta.forkDedupVer {
		return 0, nil
	}
	tx, err := st.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var purgedForkRows int64
	if purgeAppSid != "" && maxForkIdx >= 0 {
		resDel, delErr := tx.Exec(`DELETE FROM usage_steps WHERE lower(session_id) = ? AND source_cat = 'AGY_APP' AND step_idx <= ?`, purgeAppSid, maxForkIdx)
		if delErr != nil {
			return 0, delErr
		}
		purgedForkRows, _ = resDel.RowsAffected()
	}
	if len(steps) > 0 {
		stmt, err := tx.Prepare(`
INSERT OR REPLACE INTO usage_steps (
	session_id, step_idx, source_cat, ts_sec, ttft_ms, stream_ms, elapsed_ms, tps, decode_tps,
	model_enum, model_id, model_label, model_family, model_tier, quota_group,
	raw_in, raw_cr, aligned_in, aligned_cr, out_tok, vis_out_tok, th_tok,
	has_thinking, cw_tok, tier_mult, in_cost_usd, cr_cost_usd, out_cost_usd,
	th_cost_usd, cost_usd, base1x_cost_usd, nocache_cost_usd, saved_usd, is_dsh_bridge, served_model
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`)
		if err != nil {
			return 0, err
		}
		defer stmt.Close()
		for _, s := range steps {
			hasTh, isDsh := 0, 0
			if s.HasThinking {
				hasTh = 1
			}
			if s.IsDshBridge {
				isDsh = 1
			}
			if _, err = stmt.Exec(s.SessionID, s.StepIdx, s.SourceCategory, s.TsSec, s.TtftMs, s.StreamMs, s.ElapsedMs, s.Tps, s.DecodeTps,
				s.ModelEnum, s.ModelID, s.ModelLabel, s.ModelFamily, s.ModelTier, s.QuotaGroup,
				s.RawIn, s.RawCr, s.AlignedIn, s.AlignedCr, s.OutTok, s.VisOutTok, s.ThTok,
				hasTh, s.CwTok, s.TierMultiplier, s.InputCostUsd, s.CacheCostUsd, s.OutputCostUsd,
				s.ThinkCostUsd, s.CostUsd, s.Base1xCostUsd, s.NoCacheCostUsd, s.SavedUsd, isDsh, s.ServedModel); err != nil {
				return 0, err
			}
		}
	}
	if _, err = tx.Exec(`INSERT OR REPLACE INTO ingest_cursor (session_id,db_size,db_mtime_ns,wal_size,wal_mtime_ns,last_step_idx,fork_dedup_ver) VALUES (?,?,?,?,?,?,?)`, sid, meta.dbSize, meta.dbMtimeNs, meta.walSize, meta.walMtimeNs, meta.lastStepIdx, meta.forkDedupVer); err != nil {
		return 0, err
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	oldestActiveCutoff := time.Now().Unix() - activeUsageStepsWindowSec + 3600
	invalidateCold := purgedForkRows > 0
	if !invalidateCold {
		for _, s := range steps {
			if s.TsSec < oldestActiveCutoff {
				invalidateCold = true
				break
			}
		}
	}
	if invalidateCold {
		st.coldEpoch.Add(1)
	}
	st.cursorMu.Lock()
	if st.cursors == nil {
		st.cursors = make(map[string]cursorMeta)
	}
	st.cursors[sid] = meta
	st.cursorMu.Unlock()
	if len(steps) > 0 || purgedForkRows > 0 {
		st.version.Add(1)
	}
	return len(steps), nil
}

const activeUsageStepsWindowSec = 8 * 86400

func normalizeScannedUsageStep(s *ParsedModelStep) {
	if s.OutTok > 0 && (s.ElapsedMs > 0 || s.StreamMs > 0) {
		s.TtftMs, s.StreamMs, s.ElapsedMs, s.Tps, s.DecodeTps = computeStepTiming(s.TtftMs, s.StreamMs, s.ElapsedMs, s.OutTok)
	}
	if s.RawIn > 0 || s.RawCr > 0 || s.OutTok > 0 {
		model, tier := ModelFromEnum(s.ModelEnum)
		if model == UnknownModel && s.ModelID != "" && s.ModelID != "unknown" {
			if byID := GetModel(s.ModelID); byID != nil {
				model = byID
			}
		}
		if model != UnknownModel {
			s.ModelID = model.ID
			s.ModelLabel = model.Label
			s.ModelFamily = model.Family
			s.QuotaGroup = model.QuotaGroup
			if tier != "" {
				s.ModelTier = tier
			}
		}
		s.AlignedIn = s.RawIn
		s.AlignedCr = s.RawCr
		pr := PriceUsage(model, s.RawIn, s.RawCr, s.OutTok, s.ThTok, s.CwTok)
		s.TierMultiplier = pr.TierMultiplier
		s.InputCostUsd = pr.InputCostUsd
		s.CacheCostUsd = pr.CacheCostUsd
		s.OutputCostUsd = pr.OutputCostUsd
		s.ThinkCostUsd = pr.ThinkCostUsd
		s.CostUsd = pr.CostUsd
		s.Base1xCostUsd = pr.Base1xCostUsd
		s.NoCacheCostUsd = pr.NoCacheCostUsd
		s.SavedUsd = pr.SavedUsd
	}
	switch s.SourceCategory {
	case "AGY_APP":
		s.SourceSide = "APP"
		s.SourceLabel = "Antigravity App"
	case "WIN_HOST":
		s.SourceSide = "WIN"
		s.SourceLabel = "Win 宿主机"
	case "CLAUDE_GW":
		s.SourceSide = "HOST"
		s.SourceLabel = "ACP Gateway"
	default:
		s.SourceSide = "HOST"
		s.SourceLabel = "AGY CLI"
	}
}

func applyGatewaySidSourceLabel(s *ParsedModelStep, gwSids map[string]bool) {
	if s.IsDshBridge || s.SourceCategory == "CLAUDE_GW" || (gwSids != nil && gwSids[strings.ToLower(s.SessionID)]) {
		s.SourceSide = "HOST"
		s.SourceCategory = "CLAUDE_GW"
		s.SourceLabel = "ACP Gateway"
		return
	}
	switch s.SourceCategory {
	case "AGY_APP":
		s.SourceSide = "APP"
		s.SourceCategory = "AGY_APP"
		s.SourceLabel = "Antigravity App"
	case "WIN_HOST":
		s.SourceSide = "WIN"
		s.SourceLabel = "Win 宿主机"
	case "", "WSL_AGY", "WSL_HOST", "AGY_CLI":
		s.SourceSide = "HOST"
		s.SourceCategory = "AGY_CLI"
		s.SourceLabel = "AGY CLI"
	default:
		if s.SourceSide == "" {
			s.SourceSide = "HOST"
		}
		if s.SourceLabel == "" {
			s.SourceLabel = s.SourceCategory
		}
	}
}

func scanUsageStepRows(rows *sql.Rows) []ParsedModelStep {
	var out []ParsedModelStep
	for rows.Next() {
		var s ParsedModelStep
		var hasTh, isDsh int
		if err := rows.Scan(
			&s.SessionID, &s.StepIdx, &s.SourceCategory, &s.TsSec, &s.TtftMs, &s.StreamMs, &s.ElapsedMs, &s.Tps, &s.DecodeTps,
			&s.ModelEnum, &s.ModelID, &s.ModelLabel, &s.ModelFamily, &s.ModelTier, &s.QuotaGroup,
			&s.RawIn, &s.RawCr, &s.AlignedIn, &s.AlignedCr, &s.OutTok, &s.VisOutTok, &s.ThTok,
			&hasTh, &s.CwTok, &s.TierMultiplier, &s.InputCostUsd, &s.CacheCostUsd, &s.OutputCostUsd,
			&s.ThinkCostUsd, &s.CostUsd, &s.Base1xCostUsd, &s.NoCacheCostUsd, &s.SavedUsd, &isDsh, &s.ServedModel,
		); err == nil {
			s.HasThinking = hasTh != 0
			s.IsDshBridge = isDsh != 0
			normalizeScannedUsageStep(&s)
			out = append(out, s)
		}
	}
	return out
}

// QueryActiveUsageSteps materializes strictly the active window (default 8*86400s) using idx_steps_ts.
func (st *StateStore) QueryActiveUsageSteps(gwSids map[string]bool, windowSec int64) ([]ParsedModelStep, bool) {
	if windowSec <= 0 {
		windowSec = activeUsageStepsWindowSec
	}
	cutoffSec := time.Now().Unix() - windowSec
	rDB, relRead := st.acquireReadDB()
	if rDB == nil {
		return nil, false
	}
	defer relRead()
	rows, err := rDB.Query(`
SELECT session_id, step_idx, source_cat, ts_sec, ttft_ms, stream_ms, elapsed_ms, tps, decode_tps,
       model_enum, model_id, model_label, model_family, model_tier, quota_group,
       raw_in, raw_cr, aligned_in, aligned_cr, out_tok, vis_out_tok, th_tok,
       has_thinking, cw_tok, tier_mult, in_cost_usd, cr_cost_usd, out_cost_usd,
       th_cost_usd, cost_usd, base1x_cost_usd, nocache_cost_usd, saved_usd, is_dsh_bridge, served_model
FROM usage_steps
WHERE ts_sec >= ?
ORDER BY ts_sec ASC, step_idx ASC
`, cutoffSec)
	if err != nil {
		return nil, false
	}
	defer rows.Close()
	out := scanUsageStepRows(rows)
	excludeApp := isAppConvExplicitlyDisabled()
	filtered := out[:0]
	for i := range out {
		applyGatewaySidSourceLabel(&out[i], gwSids)
		if excludeApp && out[i].SourceCategory == "AGY_APP" {
			continue
		}
		filtered = append(filtered, out[i])
	}
	return filtered, true
}

// UsageAllTimeRollup is an alias for gwstore.UsageAllTimeRollup.

func priceUsageLinearBucket(p ModelPricing, rIn, rCr, outT int64, hasCr bool) (costUsd, savedUsd float64) {
	effIn := p.InPerM
	if p.UncachedWritePerM > 0 && hasCr {
		effIn = p.UncachedWritePerM
	}
	costUsd = (float64(rIn)*effIn + float64(rCr)*p.CachedPerM + float64(outT)*p.OutPerM) / 1e6
	noCache := (float64(rIn+rCr)*p.InPerM + float64(outT)*p.OutPerM) / 1e6
	savedUsd = noCache - costUsd
	if savedUsd < 0 {
		savedUsd = 0
	}
	return costUsd, savedUsd
}

func queryExactModelPricingWithDB(rDB *sql.DB, modelEnum int, modelID string, p ModelPricing, excludeApp bool) (costUsd, savedUsd float64) {
	if rDB == nil {
		return 0, 0
	}
	longInMult, longCrMult, longOutMult := p.LongCtxMult, p.LongCtxMult, p.LongCtxMult
	if p.LongCtxInMult > 0 {
		longInMult, longCrMult, longOutMult = p.LongCtxInMult, p.LongCtxCrMult, p.LongCtxOutMult
	}
	exclFlag := 0
	if excludeApp {
		exclFlag = 1
	}
	_ = rDB.QueryRow(`
SELECT COALESCE(SUM(row_cost), 0),
       COALESCE(SUM(CASE WHEN row_nocache > row_cost THEN row_nocache - row_cost ELSE 0.0 END), 0)
FROM (
	SELECT
		(
			(raw_in * 1.0) * (CASE WHEN ? > 0 AND raw_cr > 0 THEN ? ELSE ? END) *
				(CASE WHEN ? > 0 AND (raw_in + raw_cr) > ? THEN ? WHEN ? > 0 AND (raw_in + raw_cr) > ? THEN ? ELSE 1.0 END)
			+ (raw_cr * 1.0) * ? *
				(CASE WHEN ? > 0 AND (raw_in + raw_cr) > ? THEN ? WHEN ? > 0 AND (raw_in + raw_cr) > ? THEN ? ELSE 1.0 END)
			+ (out_tok * 1.0) * ? *
				(CASE WHEN ? > 0 AND (raw_in + raw_cr) > ? THEN ? WHEN ? > 0 AND (raw_in + raw_cr) > ? THEN ? ELSE 1.0 END)
		) / 1000000.0 AS row_cost,
		(
			((raw_in + raw_cr) * 1.0) * ? *
				(CASE WHEN ? > 0 AND (raw_in + raw_cr) > ? THEN ? WHEN ? > 0 AND (raw_in + raw_cr) > ? THEN ? ELSE 1.0 END)
			+ (out_tok * 1.0) * ? *
				(CASE WHEN ? > 0 AND (raw_in + raw_cr) > ? THEN ? WHEN ? > 0 AND (raw_in + raw_cr) > ? THEN ? ELSE 1.0 END)
		) / 1000000.0 AS row_nocache
	FROM usage_steps
	WHERE model_enum = ? AND model_id = ? AND (? = 0 OR source_cat != 'AGY_APP') AND (raw_in > 0 OR raw_cr > 0 OR out_tok > 0)
)`,
		p.UncachedWritePerM, p.UncachedWritePerM, p.InPerM,
		p.LongCtxThreshold, p.LongCtxThreshold, longInMult, p.MidCtxThreshold, p.MidCtxThreshold, p.MidCtxMult,
		p.CachedPerM,
		p.LongCtxThreshold, p.LongCtxThreshold, longCrMult, p.MidCtxThreshold, p.MidCtxThreshold, p.MidCtxMult,
		p.OutPerM,
		p.LongCtxThreshold, p.LongCtxThreshold, longOutMult, p.MidCtxThreshold, p.MidCtxThreshold, p.MidCtxMult,
		p.InPerM,
		p.LongCtxThreshold, p.LongCtxThreshold, longInMult, p.MidCtxThreshold, p.MidCtxThreshold, p.MidCtxMult,
		p.OutPerM,
		p.LongCtxThreshold, p.LongCtxThreshold, longOutMult, p.MidCtxThreshold, p.MidCtxThreshold, p.MidCtxMult,
		modelEnum, modelID, exclFlag,
	).Scan(&costUsd, &savedUsd)
	return costUsd, savedUsd
}

// QueryUsageAllTimeRollup computes exact allTime usage totals in SQLite without materializing historical rows.
func (st *StateStore) QueryUsageAllTimeRollup(gwSids map[string]bool) (UsageAllTimeRollup, bool) {
	excludeApp := isAppConvExplicitlyDisabled()
	rDB, relRead := st.acquireReadDB()
	if rDB == nil {
		return UsageAllTimeRollup{}, false
	}
	defer relRead()
	rows, err := rDB.Query(`
SELECT lower(session_id), model_enum, model_id, is_dsh_bridge, source_cat,
       CASE WHEN raw_cr > 0 THEN 1 ELSE 0 END AS has_cr,
       COUNT(*), COALESCE(SUM(raw_in), 0), COALESCE(SUM(raw_cr), 0),
       COALESCE(SUM(out_tok), 0), COALESCE(SUM(th_tok), 0), COALESCE(SUM(cw_tok), 0)
FROM usage_steps
GROUP BY lower(session_id), model_enum, model_id, is_dsh_bridge, source_cat, has_cr
`)
	if err != nil {
		return UsageAllTimeRollup{}, false
	}

	type modelPricingKey struct {
		enum int
		id   string
	}
	nonLinearModels := make(map[modelPricingKey]ModelPricing)

	var roll UsageAllTimeRollup
	for rows.Next() {
		var sid, modelID, srcCat string
		var modelEnum, isDsh, hasCrInt, cnt int
		var rIn, rCr, outT, thT, cwT int64
		if rows.Scan(&sid, &modelEnum, &modelID, &isDsh, &srcCat, &hasCrInt, &cnt, &rIn, &rCr, &outT, &thT, &cwT) != nil {
			continue
		}
		isGw := isDsh != 0 || srcCat == "CLAUDE_GW" || (gwSids != nil && gwSids[sid])
		if excludeApp && !isGw && srcCat == "AGY_APP" {
			continue
		}
		roll.StepsCount += cnt
		if isGw {
			roll.ClaudeGwStep += cnt
		} else {
			roll.WslHostSteps += cnt
		}
		roll.RawIn += rIn
		roll.RawCr += rCr
		roll.OutTok += outT
		roll.ThTok += thT
		roll.CwTok += cwT
		if rIn > 0 || rCr > 0 || outT > 0 {
			model, _ := ModelFromEnum(modelEnum)
			if model == UnknownModel && modelID != "" && modelID != "unknown" {
				if byID := GetModel(modelID); byID != nil {
					model = byID
				}
			}
			p := model.Pricing
			hasCr := hasCrInt != 0
			if p.LongCtxThreshold > 0 || p.MidCtxThreshold > 0 || p.UncachedWritePerM > p.InPerM || p.CachedPerM > p.InPerM {
				nonLinearModels[modelPricingKey{enum: modelEnum, id: modelID}] = p
			} else {
				c, s := priceUsageLinearBucket(p, rIn, rCr, outT, hasCr)
				roll.CostUsd += c
				roll.SavedUsd += s
			}
		}
	}
	_ = rows.Close()

	for mk, p := range nonLinearModels {
		c, s := queryExactModelPricingWithDB(rDB, mk.enum, mk.id, p, excludeApp)
		roll.CostUsd += c
		roll.SavedUsd += s
	}
	roll.CostUsd = roundTo(roll.CostUsd, 4)
	roll.SavedUsd = roundTo(roll.SavedUsd, 4)
	return roll, true
}

func (st *StateStore) QueryAllUsageSteps(gwSids map[string]bool) ([]ParsedModelStep, bool) {
	rDB, relRead := st.acquireReadDB()
	if rDB == nil {
		return nil, false
	}
	defer relRead()

	nowSec := time.Now().Unix()
	activeCutoff := nowSec - activeUsageStepsWindowSec

	st.coldMu.Lock()
	epoch0 := st.coldEpoch.Load()
	var cold []ParsedModelStep
	var cutoffUsed int64
	if !st.coldValid || st.coldBuiltEpoch != epoch0 || nowSec-st.coldCutoffSec > 3600 {
		cRows, err := rDB.Query(`
SELECT session_id, step_idx, source_cat, ts_sec, ttft_ms, stream_ms, elapsed_ms, tps, decode_tps,
       model_enum, model_id, model_label, model_family, model_tier, quota_group,
       raw_in, raw_cr, aligned_in, aligned_cr, out_tok, vis_out_tok, th_tok,
       has_thinking, cw_tok, tier_mult, in_cost_usd, cr_cost_usd, out_cost_usd,
       th_cost_usd, cost_usd, base1x_cost_usd, nocache_cost_usd, saved_usd, is_dsh_bridge, served_model
FROM usage_steps
WHERE ts_sec < ?
ORDER BY ts_sec ASC, step_idx ASC
`, activeCutoff)
		if err != nil {
			st.coldMu.Unlock()
			return nil, false
		}
		cold = scanUsageStepRows(cRows)
		_ = cRows.Close()
		st.coldSteps = cold
		st.coldCutoffSec = activeCutoff
		if st.coldEpoch.Load() == epoch0 {
			st.coldBuiltEpoch = epoch0
			st.coldValid = true
		} else {
			st.coldValid = false
		}
		cutoffUsed = activeCutoff
	} else {
		cold = st.coldSteps
		cutoffUsed = st.coldCutoffSec
	}
	st.coldMu.Unlock()

	// Only scan the bounded active window (>= cutoffUsed, ~8 days) from SQLite on per-turn refreshes.
	aRows, err := rDB.Query(`
SELECT session_id, step_idx, source_cat, ts_sec, ttft_ms, stream_ms, elapsed_ms, tps, decode_tps,
       model_enum, model_id, model_label, model_family, model_tier, quota_group,
       raw_in, raw_cr, aligned_in, aligned_cr, out_tok, vis_out_tok, th_tok,
       has_thinking, cw_tok, tier_mult, in_cost_usd, cr_cost_usd, out_cost_usd,
       th_cost_usd, cost_usd, base1x_cost_usd, nocache_cost_usd, saved_usd, is_dsh_bridge, served_model
FROM usage_steps
WHERE ts_sec >= ?
ORDER BY ts_sec ASC, step_idx ASC
`, cutoffUsed)
	if err != nil {
		return nil, false
	}
	activeSteps := scanUsageStepRows(aRows)
	_ = aRows.Close()

	out := make([]ParsedModelStep, 0, len(cold)+len(activeSteps))
	excludeApp := isAppConvExplicitlyDisabled()
	for _, s := range cold {
		applyGatewaySidSourceLabel(&s, gwSids)
		if excludeApp && s.SourceCategory == "AGY_APP" {
			continue
		}
		out = append(out, s)
	}
	for _, s := range activeSteps {
		applyGatewaySidSourceLabel(&s, gwSids)
		if excludeApp && s.SourceCategory == "AGY_APP" {
			continue
		}
		out = append(out, s)
	}
	return out, true
}

func clonePromIntMap(src map[string]int) map[string]int {
	out := make(map[string]int, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

func clonePromInt64Map(src map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

func (st *StateStore) queryPrometheusTotals() (turnsByKey map[string]int, tokensByModelKind map[string]int64, compactions int, reuses int, ok bool) {
	st.Flush()
	st.promMu.Lock()
	defer st.promMu.Unlock()
	rDB, relRead := st.acquireReadDB()
	if rDB == nil {
		return nil, nil, 0, 0, false
	}
	defer relRead()
	curVer := st.version.Load()
	if st.promCacheVer == curVer && st.promTurns != nil {
		return clonePromIntMap(st.promTurns), clonePromInt64Map(st.promTokens), st.promCompactions, st.promReuses, st.promHasRows
	}
	turnsMap := make(map[string]int)
	tokensMap := make(map[string]int64)
	hasRows := false
	rows, err := rDB.Query(`
SELECT model_id, mode, stop_reason, usage_source, COUNT(*),
       COALESCE(SUM(in_tok), 0), COALESCE(SUM(cached_tok), 0),
       COALESCE(SUM(out_tok), 0), COALESCE(SUM(thinking_tok), 0)
FROM turns
GROUP BY model_id, mode, stop_reason, usage_source
`)
	if err != nil {
		return nil, nil, 0, 0, false
	}
	for rows.Next() {
		var m, mode, stop, src string
		var cnt int
		var sumIn, sumCr, sumOut, sumTh int64
		if rows.Scan(&m, &mode, &stop, &src, &cnt, &sumIn, &sumCr, &sumOut, &sumTh) == nil {
			hasRows = true
			if m == "" {
				m = DefaultModelID
			}
			if src == "" {
				src = "estimate"
			}
			em := promQuote(m)
			tk := fmt.Sprintf(`model=%s,mode=%s,stop_reason=%s,usage_source=%s`, em, promQuote(mode), promQuote(stop), promQuote(src))
			turnsMap[tk] += cnt
			tokensMap[fmt.Sprintf(`model=%s,kind="input"`, em)] += sumIn
			tokensMap[fmt.Sprintf(`model=%s,kind="cached"`, em)] += sumCr
			tokensMap[fmt.Sprintf(`model=%s,kind="output"`, em)] += sumOut
			tokensMap[fmt.Sprintf(`model=%s,kind="thinking"`, em)] += sumTh
		}
	}
	_ = rows.Close()

	var compTot, reuseTot int
	_ = rDB.QueryRow(`
SELECT COALESCE(SUM(MAX(c.compaction_rollovers, COALESCE(h.compaction_rollovers, 0))), 0),
       COALESCE(SUM(MAX(c.session_reuses, COALESCE(h.session_reuses, 0))), 0)
FROM conversations c
LEFT JOIN conversation_hot h ON h.conv_key = c.conv_key AND h.agy_session_id = c.agy_session_id
`).Scan(&compTot, &reuseTot)

	st.promCacheVer = curVer
	st.promTurns = turnsMap
	st.promTokens = tokensMap
	st.promCompactions = compTot
	st.promReuses = reuseTot
	st.promHasRows = hasRows
	return clonePromIntMap(turnsMap), clonePromInt64Map(tokensMap), compTot, reuseTot, hasRows
}

func RenderPrometheusMetrics(gs *GatewayServer) string {
	var b strings.Builder
	liveWorkers := 0
	hibernatedSessions := 0
	promptsInFlight := len(promptSem)
	parkedCalls := 0
	capacityFallbackActive := 0
	var compactionsTotal int
	var reusesTotal int
	turnsByKey := make(map[string]int)
	tokensByModelKind := make(map[string]int64)

	persistedTurns, persistedTokens, persistedComp, persistedReuses, hasPersisted := GlobalStore.queryPrometheusTotals()
	if hasPersisted {
		turnsByKey = persistedTurns
		tokensByModelKind = persistedTokens
		compactionsTotal = persistedComp
		reusesTotal = persistedReuses
	}

	if gs != nil {
		gs.mu.RLock()
		for _, s := range gs.sessions {
			s.mu.Lock()
			if s.SessionID != "" {
				liveWorkers++
			} else if s.HibernatedSessionID != "" {
				hibernatedSessions++
			}
			parkedCalls += len(s.ParkedCalls)
			if s.capacityFallbackSid != "" && s.capacityFallbackSid == s.SessionID && s.capacityFallbackTier != "" {
				capacityFallbackActive++
			}
			if !hasPersisted {
				compactionsTotal += s.CompactionRollovers
				reusesTotal += s.SessionReuses
				for _, t := range s.TurnHistory {
					src := "estimate"
					inT, crT, outT := 0, 0, 0
					if t.Usage != nil {
						inT = t.Usage.InputTokens
						crT = t.Usage.CachedReadTokens
						outT = t.Usage.OutputTokens
						if t.Usage.UsageSource != "" {
							src = t.Usage.UsageSource
						}
					}
					m := t.Model
					if m == "" {
						m = DefaultModelID
					}
					em := promQuote(m)
					tk := fmt.Sprintf(`model=%s,mode=%s,stop_reason=%s,usage_source=%s`, em, promQuote(t.Mode), promQuote(t.StopReason), promQuote(src))
					turnsByKey[tk]++
					tokensByModelKind[fmt.Sprintf(`model=%s,kind="input"`, em)] += int64(inT)
					tokensByModelKind[fmt.Sprintf(`model=%s,kind="cached"`, em)] += int64(crT)
					tokensByModelKind[fmt.Sprintf(`model=%s,kind="output"`, em)] += int64(outT)
					tokensByModelKind[fmt.Sprintf(`model=%s,kind="thinking"`, em)] += int64(t.ThinkingTokens)
				}
			}
			s.mu.Unlock()
		}
		gs.mu.RUnlock()
	}

	var reqTotal int64
	if gs != nil {
		reqTotal = gs.reqCounter.Load()
	}
	storeReadyVal := 0
	if GlobalStore.IsReady() {
		storeReadyVal = 1
	}

	b.WriteString("# HELP agy_gateway_store_ready Whether the persistent SQLite state store is initialized and healthy (1=ready, 0=degraded).\n")
	b.WriteString("# TYPE agy_gateway_store_ready gauge\n")
	fmt.Fprintf(&b, "agy_gateway_store_ready %d\n", storeReadyVal)

	b.WriteString("# HELP agy_gateway_live_workers Number of live ACP sessions in shared engine.\n")
	b.WriteString("# TYPE agy_gateway_live_workers gauge\n")
	fmt.Fprintf(&b, "agy_gateway_live_workers %d\n", liveWorkers)

	b.WriteString("# HELP agy_gateway_hibernated_sessions Number of hibernated sessions eligible for warm resume.\n")
	b.WriteString("# TYPE agy_gateway_hibernated_sessions gauge\n")
	fmt.Fprintf(&b, "agy_gateway_hibernated_sessions %d\n", hibernatedSessions)

	b.WriteString("# HELP agy_gateway_max_live_workers Configured maximum live ACP sessions.\n")
	b.WriteString("# TYPE agy_gateway_max_live_workers gauge\n")
	fmt.Fprintf(&b, "agy_gateway_max_live_workers %d\n", effectiveMaxLiveWorkers())

	b.WriteString("# HELP agy_gateway_prompts_inflight Number of concurrent prompts currently holding semaphore slots.\n")
	b.WriteString("# TYPE agy_gateway_prompts_inflight gauge\n")
	fmt.Fprintf(&b, "agy_gateway_prompts_inflight %d\n", promptsInFlight)

	b.WriteString("# HELP agy_gateway_max_concurrent_prompts Configured maximum concurrent prompts.\n")
	b.WriteString("# TYPE agy_gateway_max_concurrent_prompts gauge\n")
	fmt.Fprintf(&b, "agy_gateway_max_concurrent_prompts %d\n", MaxConcurrentPrompts)

	b.WriteString("# HELP agy_gateway_parked_calls Number of MCP tool calls currently parked waiting for host client.\n")
	b.WriteString("# TYPE agy_gateway_parked_calls gauge\n")
	fmt.Fprintf(&b, "agy_gateway_parked_calls %d\n", parkedCalls)

	b.WriteString("# HELP agy_gateway_requests_total Total HTTP requests handled by gateway.\n")
	b.WriteString("# TYPE agy_gateway_requests_total counter\n")
	fmt.Fprintf(&b, "agy_gateway_requests_total %d\n", reqTotal)

	b.WriteString("# HELP agy_gateway_engine_exits_total Total shared ACP engine process exits.\n")
	b.WriteString("# TYPE agy_gateway_engine_exits_total counter\n")
	fmt.Fprintf(&b, "agy_gateway_engine_exits_total %d\n", engineExitsTotal.Load())

	b.WriteString("# HELP agy_gateway_worker_cap_rejections_total Total HTTP 529 rejections caused by MaxLiveWorkers saturation.\n")
	b.WriteString("# TYPE agy_gateway_worker_cap_rejections_total counter\n")
	fmt.Fprintf(&b, "agy_gateway_worker_cap_rejections_total %d\n", workerCapRejectionsTotal.Load())

	b.WriteString("# HELP agy_gateway_probe_slot_rejections_total Total background probe or non-preemptive utility slot rejections.\n")
	b.WriteString("# TYPE agy_gateway_probe_slot_rejections_total counter\n")
	fmt.Fprintf(&b, "agy_gateway_probe_slot_rejections_total %d\n", probeSlotRejectionsTotal.Load())

	b.WriteString("# HELP agy_gateway_soft_detach_total Total prefill soft-detach outcomes by outcome (joined, expired, superseded).\n")
	b.WriteString("# TYPE agy_gateway_soft_detach_total counter\n")
	fmt.Fprintf(&b, "agy_gateway_soft_detach_total{outcome=\"joined\"} %d\n", softDetachJoinedTotal.Load())
	fmt.Fprintf(&b, "agy_gateway_soft_detach_total{outcome=\"expired\"} %d\n", softDetachExpiredTotal.Load())
	fmt.Fprintf(&b, "agy_gateway_soft_detach_total{outcome=\"superseded\"} %d\n", softDetachSupersededTotal.Load())

	b.WriteString("# HELP agy_gateway_compaction_rollovers_total Total session rollovers caused by compaction.\n")
	b.WriteString("# TYPE agy_gateway_compaction_rollovers_total counter\n")
	fmt.Fprintf(&b, "agy_gateway_compaction_rollovers_total %d\n", compactionsTotal)

	b.WriteString("# HELP agy_gateway_session_reuses_total Total sticky session reuses.\n")
	b.WriteString("# TYPE agy_gateway_session_reuses_total counter\n")
	fmt.Fprintf(&b, "agy_gateway_session_reuses_total %d\n", reusesTotal)

	b.WriteString("# HELP agy_gateway_argon_degradation_detected_total Total Gemini 4 Argon shadow-degradation events detected.\n")
	b.WriteString("# TYPE agy_gateway_argon_degradation_detected_total counter\n")
	fmt.Fprintf(&b, "agy_gateway_argon_degradation_detected_total %d\n", argonDegradationDetectedTotal.Load())

	b.WriteString("# HELP agy_gateway_argon_degradation_recovered_total Total Gemini 4 Argon shadow-degradation events transparently recovered in-turn.\n")
	b.WriteString("# TYPE agy_gateway_argon_degradation_recovered_total counter\n")
	fmt.Fprintf(&b, "agy_gateway_argon_degradation_recovered_total %d\n", argonDegradationRecoveredTotal.Load())

	b.WriteString("# HELP agy_gateway_cache_burn_warnings_total Total long-context consecutive zero-cache burn warnings emitted.\n")
	b.WriteString("# TYPE agy_gateway_cache_burn_warnings_total counter\n")
	fmt.Fprintf(&b, "agy_gateway_cache_burn_warnings_total %d\n", cacheBurnWarningsTotal.Load())

	b.WriteString("# HELP agy_gateway_orphan_relay_caps_total Total orphan parked tool relay loops stopped by MaxToolRelays/ParkedTtl.\n")
	b.WriteString("# TYPE agy_gateway_orphan_relay_caps_total counter\n")
	fmt.Fprintf(&b, "agy_gateway_orphan_relay_caps_total %d\n", orphanRelayCapsTotal.Load())

	b.WriteString("# HELP agy_gateway_waf_autobisect_healed_total Total instant-429 WAF false-positive turns healed via auto-bisect.\n")
	b.WriteString("# TYPE agy_gateway_waf_autobisect_healed_total counter\n")
	fmt.Fprintf(&b, "agy_gateway_waf_autobisect_healed_total %d\n", WafBisectHealedTotal.Load())

	b.WriteString("# HELP agy_gateway_capacity_fallback_active_sessions Number of live sessions currently clamped to a lower capacity fallback tier.\n")
	b.WriteString("# TYPE agy_gateway_capacity_fallback_active_sessions gauge\n")
	fmt.Fprintf(&b, "agy_gateway_capacity_fallback_active_sessions %d\n", capacityFallbackActive)

	b.WriteString("# HELP agy_gateway_capacity_fallback_events_total Total capacity fallback arm events (429/529 or chronic zero-cache degrade).\n")
	b.WriteString("# TYPE agy_gateway_capacity_fallback_events_total counter\n")
	fmt.Fprintf(&b, "agy_gateway_capacity_fallback_events_total %d\n", capacityFallbackEventsTotal.Load())

	b.WriteString("# HELP agy_gateway_capacity_fallback_probes_total Total higher-tier recovery probes initiated after capacity fallback cooldown expiry.\n")
	b.WriteString("# TYPE agy_gateway_capacity_fallback_probes_total counter\n")
	fmt.Fprintf(&b, "agy_gateway_capacity_fallback_probes_total %d\n", capacityFallbackProbesTotal.Load())

	upState := CurrentUpgradeMetricState()
	b.WriteString("# HELP agy_gateway_upgrade_state Current upgrade candidate state (1 for the active state, 0 otherwise).\n")
	b.WriteString("# TYPE agy_gateway_upgrade_state gauge\n")
	for _, stLabel := range knownUpgradeMetricStates {
		v := 0
		if stLabel == upState {
			v = 1
		}
		fmt.Fprintf(&b, "agy_gateway_upgrade_state{state=%q} %d\n", stLabel, v)
	}
	if upState == "other" {
		fmt.Fprintf(&b, "agy_gateway_upgrade_state{state=\"other\"} 1\n")
	}

	b.WriteString("# HELP agy_gateway_turns_total Completed gateway turns by model, mode, stop_reason, and usage_source.\n")
	b.WriteString("# TYPE agy_gateway_turns_total counter\n")
	var tKeys []string
	for k := range turnsByKey {
		tKeys = append(tKeys, k)
	}
	sort.Strings(tKeys)
	for _, k := range tKeys {
		fmt.Fprintf(&b, "agy_gateway_turns_total{%s} %d\n", k, turnsByKey[k])
	}

	b.WriteString("# HELP agy_gateway_tokens_total Total tokens processed by gateway turns.\n")
	b.WriteString("# TYPE agy_gateway_tokens_total counter\n")
	var tokKeys []string
	for k := range tokensByModelKind {
		tokKeys = append(tokKeys, k)
	}
	sort.Strings(tokKeys)
	for _, k := range tokKeys {
		fmt.Fprintf(&b, "agy_gateway_tokens_total{%s} %d\n", k, tokensByModelKind[k])
	}
	if !isChildAccountWorker() && GlobalAccountManager != nil && GlobalAccountManager.HasConfiguredAccounts() {
		now := time.Now()
		accs := GlobalAccountManager.ListAccounts()
		ttl := GlobalAccountManager.stickyTTL()
		GlobalAccountManager.stickyMu.Lock()
		_, stickyByAcc := GlobalAccountManager.stickyLRU.snapshotCounts(now, ttl)
		GlobalAccountManager.stickyMu.Unlock()
		hvs := GlobalAccountManager.SnapshotHealthViews(accs, stickyByAcc, now)
		b.WriteString("# HELP agy_account_inflight Current in-flight requests per account.\n# TYPE agy_account_inflight gauge\n")
		for _, a := range accs {
			hv := hvs[a.ID]
			b.WriteString(fmt.Sprintf("agy_account_inflight{account=%s,state=%s} %d\n", promQuote(a.ID), promQuote(hv.State), hv.InFlight))
		}
		b.WriteString("# HELP agy_account_sticky_sessions Active sticky sessions pinned to each account.\n# TYPE agy_account_sticky_sessions gauge\n")
		for _, a := range accs {
			b.WriteString(fmt.Sprintf("agy_account_sticky_sessions{account=%q} %d\n", a.ID, hvs[a.ID].StickySessions))
		}
		b.WriteString("# HELP agy_account_cooldown_seconds Remaining cooldown seconds per account.\n# TYPE agy_account_cooldown_seconds gauge\n")
		for _, a := range accs {
			b.WriteString(fmt.Sprintf("agy_account_cooldown_seconds{account=%q} %d\n", a.ID, hvs[a.ID].CooldownSec))
		}
		b.WriteString("# HELP agy_account_requests_total Total upstream requests observed per account.\n# TYPE agy_account_requests_total counter\n")
		for _, a := range accs {
			b.WriteString(fmt.Sprintf("agy_account_requests_total{account=%q} %d\n", a.ID, hvs[a.ID].TotalRequests))
		}
		b.WriteString("# HELP agy_account_failures_total Total upstream failures observed per account.\n# TYPE agy_account_failures_total counter\n")
		for _, a := range accs {
			b.WriteString(fmt.Sprintf("agy_account_failures_total{account=%q} %d\n", a.ID, hvs[a.ID].TotalFailures))
		}
		b.WriteString("# HELP agy_account_rate_limits_total Total HTTP 429 rate limits per account.\n# TYPE agy_account_rate_limits_total counter\n")
		for _, a := range accs {
			b.WriteString(fmt.Sprintf("agy_account_rate_limits_total{account=%q} %d\n", a.ID, hvs[a.ID].Error429Count))
		}
		b.WriteString("# HELP agy_account_failovers_total Total pre-stream 1st-turn failovers departing from each account.\n# TYPE agy_account_failovers_total counter\n")
		for _, a := range accs {
			b.WriteString(fmt.Sprintf("agy_account_failovers_total{account=%q} %d\n", a.ID, hvs[a.ID].FailoverOutCount))
		}
	}
	return b.String()
}

// SessionClientMeta is an alias for gwstore.SessionClientMeta.

func (st *StateStore) backfillClientAndTimingLocked() {
	if st.db == nil {
		return
	}
	type convSeed struct {
		convKey, sysText, initTools, curTools, proto string
	}
	var pending []convSeed
	if rows, err := st.db.Query(`
SELECT conv_key, system_text, init_tools_json, cur_tools_json, protocol
FROM conversations
WHERE client_id = '' OR client_label = '' OR (client_id = 'dsh' AND init_tools_json LIKE '%exec_command%' AND init_tools_json LIKE '%write_stdin%')
`); err == nil {
		for rows.Next() {
			var c convSeed
			if rows.Scan(&c.convKey, &c.sysText, &c.initTools, &c.curTools, &c.proto) == nil {
				pending = append(pending, c)
			}
		}
		_ = rows.Close()
	}
	for _, c := range pending {
		ci := InferHistoricalClientIdentity(c.convKey, c.sysText, c.initTools, c.curTools)
		proto := c.proto
		if proto == "" || (ci.ID == "codex" && proto == "Chat") {
			proto = ci.Protocol
		}
		_, _ = st.db.Exec(
			`UPDATE conversations SET client_id = ?, client_label = ?, protocol = ? WHERE conv_key = ?`,
			ci.ID, ci.Label, proto, c.convKey,
		)
	}
	_, _ = st.db.Exec(`
UPDATE turns
SET client_id = COALESCE((SELECT c.client_id FROM conversations c WHERE c.conv_key = turns.conv_key), turns.client_id),
    client_label = COALESCE((SELECT c.client_label FROM conversations c WHERE c.conv_key = turns.conv_key), turns.client_label),
    protocol = CASE
        WHEN turns.protocol = '' OR (turns.client_id = 'dsh' AND COALESCE((SELECT c.client_id FROM conversations c WHERE c.conv_key = turns.conv_key), '') = 'codex')
        THEN COALESCE((SELECT c.protocol FROM conversations c WHERE c.conv_key = turns.conv_key), turns.protocol)
        ELSE turns.protocol
    END
WHERE client_id = '' OR client_label = '' OR (turns.client_id = 'dsh' AND COALESCE((SELECT c.client_id FROM conversations c WHERE c.conv_key = turns.conv_key), '') = 'codex')
`)
	type orphanTurnSeed struct {
		id            int
		convKey, prot string
	}
	var orphanTurns []orphanTurnSeed
	if rows, err := st.db.Query(`SELECT id, conv_key, protocol FROM turns WHERE client_id = '' OR client_label = ''`); err == nil {
		for rows.Next() {
			var t orphanTurnSeed
			if rows.Scan(&t.id, &t.convKey, &t.prot) == nil {
				orphanTurns = append(orphanTurns, t)
			}
		}
		_ = rows.Close()
	}
	for _, t := range orphanTurns {
		ci := InferHistoricalClientIdentity(t.convKey, "", "", "")
		proto := t.prot
		if proto == "" {
			proto = ci.Protocol
		}
		_, _ = st.db.Exec(`UPDATE turns SET client_id = ?, client_label = ?, protocol = ? WHERE id = ?`, ci.ID, ci.Label, proto, t.id)
	}
	type badTimingRow struct {
		id, ttftMs, elapsedMs, outTok int
	}
	var badRows []badTimingRow
	if rows, err := st.db.Query(`
SELECT id, ttft_ms, elapsed_ms, out_tok
FROM turns
WHERE stream_ms > elapsed_ms AND elapsed_ms > 0
`); err == nil {
		for rows.Next() {
			var r badTimingRow
			if rows.Scan(&r.id, &r.ttftMs, &r.elapsedMs, &r.outTok) == nil {
				badRows = append(badRows, r)
			}
		}
		_ = rows.Close()
	}
	for _, r := range badRows {
		rawStream := 0
		if r.elapsedMs > r.ttftMs && r.ttftMs > 0 {
			rawStream = r.elapsedMs - r.ttftMs
		} else {
			rawStream = r.elapsedMs
		}
		_, normStream, _, _, decTps := computeStepTiming(r.ttftMs, rawStream, r.elapsedMs, r.outTok)
		_, _ = st.db.Exec(`UPDATE turns SET stream_ms = ?, decode_tps = ? WHERE id = ?`, normStream, decTps, r.id)
	}
}

func (st *StateStore) LookupSessionClientMap() map[string]SessionClientMeta {
	st.Flush()
	rDB, relRead := st.acquireReadDB()
	out := make(map[string]SessionClientMeta)
	if rDB == nil {
		return out
	}
	defer relRead()
	mergeMeta := func(sid string, m SessionClientMeta) {
		prev, exists := out[sid]
		if !exists || isKnownSpecificClientID(m.ClientID) || !isKnownSpecificClientID(prev.ClientID) {
			out[sid] = m
		}
	}
	if rows, err := rDB.Query(`
SELECT lower(agy_session_id), conv_key, client_id, client_label, protocol
FROM turns
WHERE agy_session_id != '' AND (client_id != '' OR client_label != '')
ORDER BY id ASC
`); err == nil {
		for rows.Next() {
			var sid string
			var m SessionClientMeta
			if rows.Scan(&sid, &m.ConvKey, &m.ClientID, &m.ClientLabel, &m.Protocol) == nil && sid != "" {
				mergeMeta(sid, m)
			}
		}
		_ = rows.Close()
	}
	if rows, err := rDB.Query(`
SELECT lower(c.agy_session_id), c.conv_key,
       COALESCE(NULLIF(h.client_id, ''), c.client_id),
       COALESCE(NULLIF(h.client_label, ''), c.client_label),
       COALESCE(NULLIF(h.protocol, ''), c.protocol)
FROM conversations c
LEFT JOIN conversation_hot h ON h.conv_key = c.conv_key AND h.agy_session_id = c.agy_session_id
WHERE c.agy_session_id != '' AND (c.client_id != '' OR c.client_label != '' OR COALESCE(h.client_id, '') != '' OR COALESCE(h.client_label, '') != '')
`); err == nil {
		for rows.Next() {
			var sid string
			var m SessionClientMeta
			if rows.Scan(&sid, &m.ConvKey, &m.ClientID, &m.ClientLabel, &m.Protocol) == nil && sid != "" {
				mergeMeta(sid, m)
			}
		}
		_ = rows.Close()
	}
	return out
}

var promLabelReplacer = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`, "\r", "")

func promQuote(s string) string {
	return `"` + promLabelReplacer.Replace(s) + `"`
}
