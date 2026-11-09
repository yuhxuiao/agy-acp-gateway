package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type cursorMeta struct {
	dbSize      int64
	dbMtimeNs   int64
	walSize     int64
	walMtimeNs  int64
	lastStepIdx int
}

type StateStore struct {
	mu      sync.Mutex
	dbPath  string
	db      *sql.DB
	cursors map[string]cursorMeta
}

var GlobalStore = &StateStore{}

func InitGlobalStore(dbPath string) {
	GlobalStore.Init(dbPath)
}

func (st *StateStore) Init(dbPath string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.db != nil {
		_ = st.db.Close()
		st.db = nil
	}
	st.dbPath = strings.TrimSpace(dbPath)
	if st.dbPath == "" {
		return
	}
	_ = os.MkdirAll(filepath.Dir(st.dbPath), 0755)
	dsn := "file:" + filepath.ToSlash(st.dbPath) + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
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
	tps              REAL NOT NULL,
	in_tok           INTEGER NOT NULL,
	cached_tok       INTEGER NOT NULL,
	out_tok          INTEGER NOT NULL,
	raw_in_tok       INTEGER NOT NULL DEFAULT 0,
	raw_cr_tok       INTEGER NOT NULL DEFAULT 0,
	thinking_tok     INTEGER NOT NULL,
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
	elapsed_ms       INTEGER NOT NULL,
	tps              REAL NOT NULL,
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
`
	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return
	}
	st.db = db
	st.cursors = make(map[string]cursorMeta)
	if rows, err := db.Query(`SELECT session_id, db_size, db_mtime_ns, wal_size, wal_mtime_ns, last_step_idx FROM ingest_cursor`); err == nil {
		for rows.Next() {
			var sid string
			var c cursorMeta
			if rows.Scan(&sid, &c.dbSize, &c.dbMtimeNs, &c.walSize, &c.walMtimeNs, &c.lastStepIdx) == nil {
				st.cursors[sid] = c
			}
		}
		_ = rows.Close()
	}
}

func (st *StateStore) IsReady() bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.db != nil
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
	if len(s.ParkedCalls) > 0 {
		return "ParkedWaitingTool"
	}
	if s.IsBusy || s.PromptInFlight {
		return "BusyPrompting"
	}
	return "LiveIdle"
}

func (st *StateStore) UpsertSession(s *AcpProjectSession) {
	if s == nil || strings.HasPrefix(s.ConvKey, "util:") {
		return
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
	sysText := s.SystemText
	msgCnt := s.MessageCount
	histFp := s.HistoryFingerprint
	lastCached := s.LastCachedReadTokens
	lastDbIdx := s.LastSeenDbStepIdx
	reuses := s.SessionReuses
	rollovers := s.CompactionRollovers
	state := s.sessionStateLabelLocked()
	createdMs := s.CreatedAt.UnixMilli()
	updatedMs := s.LastUsedAt.UnixMilli()
	s.mu.Unlock()

	if sid == "" && msgCnt == 0 {
		return
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	if st.db == nil {
		return
	}
	_, err := st.db.Exec(`
INSERT INTO conversations (
	conv_key, agy_session_id, tenant_key, cwd, model_id, thought_level,
	tool_fp, system_text, msg_count, history_fp, last_cached_tok, last_db_step_idx,
	session_reuses, compaction_rollovers, state, created_at_ms, updated_at_ms
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(conv_key) DO UPDATE SET
	agy_session_id = excluded.agy_session_id,
	tenant_key = excluded.tenant_key,
	cwd = excluded.cwd,
	model_id = excluded.model_id,
	thought_level = excluded.thought_level,
	tool_fp = excluded.tool_fp,
	system_text = excluded.system_text,
	msg_count = excluded.msg_count,
	history_fp = excluded.history_fp,
	last_cached_tok = excluded.last_cached_tok,
	last_db_step_idx = excluded.last_db_step_idx,
	session_reuses = excluded.session_reuses,
	compaction_rollovers = excluded.compaction_rollovers,
	state = excluded.state,
	updated_at_ms = excluded.updated_at_ms
`,
		convKey, sid, tenantKey, cwd, modelID, tier,
		toolFp, sysText, msgCnt, histFp, lastCached, lastDbIdx,
		reuses, rollovers, state, createdMs, updatedMs,
	)
	if err != nil {
		LogEvent("STORE_WRITE_ERR", "upsert session failed", map[string]any{"error": err.Error()})
	}

}

func (st *StateStore) InsertTurn(convKey string, entry TurnHistoryEntry) {
	if strings.HasPrefix(convKey, "util:") {
		return
	}
	inTok, crTok, outTok, rawIn, rawCr, steps := 0, 0, 0, 0, 0, 1
	src := "estimate"
	if entry.Usage != nil {
		inTok = entry.Usage.InputTokens
		crTok = entry.Usage.CachedReadTokens
		outTok = entry.Usage.OutputTokens
		rawIn = entry.Usage.RawUncachedInputTokens
		rawCr = entry.Usage.RawTpuCachedTokens
		if entry.Usage.StepsInPrompt > 0 {
			steps = entry.Usage.StepsInPrompt
		}
		if entry.Usage.UsageSource != "" {
			src = entry.Usage.UsageSource
		}
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.db == nil {
		return
	}
	_, err := st.db.Exec(`
INSERT INTO turns (
	conv_key, agy_session_id, turn_idx, mode, model_id, thought_level,
	stop_reason, elapsed_ms, ttft_ms, tps, in_tok, cached_tok, out_tok,
	raw_in_tok, raw_cr_tok, thinking_tok, steps_in_prompt, usage_source,
	cache_hit_rate, completed_at_ms
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`,
		convKey, entry.SessionID, entry.TurnIndex, entry.Mode, entry.Model, entry.ThoughtLevel,
		entry.StopReason, entry.ElapsedMs, entry.TtftMs, entry.Tps, inTok, crTok, outTok,
		rawIn, rawCr, entry.ThinkingTokens, steps, src, entry.CacheHitRate, entry.CompletedAtMs,
	)
	if err != nil {
		LogEvent("STORE_WRITE_ERR", "insert turn failed", map[string]any{"error": err.Error()})
	}

}

func (st *StateStore) RestoreDetachedSessions(gs *GatewayServer) int {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.db == nil || gs == nil {
		return 0
	}
	cutoffMs := time.Now().Add(-7 * 24 * time.Hour).UnixMilli()
	_, _ = st.db.Exec(`UPDATE conversations SET state = 'Detached' WHERE state != 'Detached'`)

	rows, err := st.db.Query(`
SELECT conv_key, agy_session_id, tenant_key, cwd, model_id, thought_level,
       tool_fp, system_text, msg_count, history_fp, last_cached_tok, last_db_step_idx,
       session_reuses, compaction_rollovers, created_at_ms, updated_at_ms
FROM conversations
WHERE agy_session_id != '' AND msg_count > 0 AND updated_at_ms >= ?
ORDER BY created_at_ms ASC
`, cutoffMs)
	if err != nil {
		return 0
	}
	defer rows.Close()

	type restoredRow struct {
		convKey, sid, tenantKey, cwd, modelID, tier, toolFp, sysText, histFp string
		msgCnt, lastCached, lastDbIdx, reuses, rollovers                     int
		createdMs, updatedMs                                                 int64
	}
	var list []restoredRow
	for rows.Next() {
		var r restoredRow
		if err := rows.Scan(
			&r.convKey, &r.sid, &r.tenantKey, &r.cwd, &r.modelID, &r.tier,
			&r.toolFp, &r.sysText, &r.msgCnt, &r.histFp, &r.lastCached, &r.lastDbIdx,
			&r.reuses, &r.rollovers, &r.createdMs, &r.updatedMs,
		); err == nil {
			list = append(list, r)
		}
	}

	// Ambiguous legacy rows are kept on disk but never resumed under either identity.
	sidCounts := make(map[string]int)
	for _, r := range list {
		sidCounts[r.sid]++
	}
	restored := 0
	gs.mu.Lock()
	defer gs.mu.Unlock()
	for _, r := range list {
		if sidCounts[r.sid] > 1 {
			LogEvent("STATE_RESTORE_CONFLICT", "Skipped ambiguous duplicate ACP session; next request will cold start", nil)
			continue
		}
		if _, exists := gs.sessions[r.convKey]; exists {
			continue
		}
		sess := NewAcpProjectSession(r.convKey, r.cwd, r.sysText)
		if r.tenantKey != "" {
			sess.TenantKey = r.tenantKey
		}
		sess.HibernatedSessionID = r.sid
		sess.ModelID = r.modelID
		sess.ThoughtLevel = r.tier
		sess.ToolFingerprint = r.toolFp
		sess.MessageCount = r.msgCnt
		sess.HistoryFingerprint = r.histFp
		sess.LastCachedReadTokens = r.lastCached
		sess.LastSeenDbStepIdx = r.lastDbIdx
		sess.SessionReuses = r.reuses
		sess.CompactionRollovers = r.rollovers
		sess.CreatedAt = time.UnixMilli(r.createdMs)
		sess.LastUsedAt = time.UnixMilli(r.updatedMs)
		sess.onBeforeSpawn = gs.reserveWorkerSlot

		// Load up to 50 recent turns for this conversation
		tRows, err := st.db.Query(`
SELECT turn_idx, completed_at_ms, agy_session_id, mode, elapsed_ms, ttft_ms, tps,
       thinking_tok, in_tok, cached_tok, out_tok, raw_in_tok, raw_cr_tok,
       steps_in_prompt, usage_source, cache_hit_rate, stop_reason, model_id, thought_level
FROM turns
WHERE conv_key = ?
ORDER BY id ASC
LIMIT 150
`, r.convKey)
		if err == nil {
			for tRows.Next() {
				var te TurnHistoryEntry
				var u TurnUsage
				if err := tRows.Scan(
					&te.TurnIndex, &te.CompletedAtMs, &te.SessionID, &te.Mode, &te.ElapsedMs, &te.TtftMs, &te.Tps,
					&te.ThinkingTokens, &u.InputTokens, &u.CachedReadTokens, &u.OutputTokens, &u.RawUncachedInputTokens, &u.RawTpuCachedTokens,
					&u.StepsInPrompt, &u.UsageSource, &te.CacheHitRate, &te.StopReason, &te.Model, &te.ThoughtLevel,
				); err == nil {
					te.Ts = time.UnixMilli(te.CompletedAtMs).UTC().Format(time.RFC3339)
					te.HasThinking = te.ThinkingTokens > 0
					uCopy := u
					te.Usage = &uCopy
					sess.TurnHistory = append(sess.TurnHistory, te)
				}
			}
			_ = tRows.Close()
		}

		gs.sessions[r.convKey] = sess
		gs.tenants[sess.TenantKey] = sess
		RegisterGatewaySessionID(r.sid)
		restored++
	}
	return restored
}

func (st *StateStore) LoadKnownGatewaySids(dest map[string]bool) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.db == nil {
		return
	}
	rows, err := st.db.Query(`SELECT DISTINCT agy_session_id FROM conversations WHERE agy_session_id != '' UNION SELECT DISTINCT agy_session_id FROM turns WHERE agy_session_id != ''`)
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

func (st *StateStore) SyncUsageStepsFromDir(convDir string, gwSids map[string]bool) int {
	entries, err := os.ReadDir(convDir)
	if err != nil {
		return 0
	}
	type pendingDB struct {
		sid    string
		dbPath string
		meta   cursorMeta
	}
	var toScan []pendingDB

	st.mu.Lock()
	if st.db == nil {
		st.mu.Unlock()
		return 0
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".db") {
			continue
		}
		sid := strings.TrimSuffix(e.Name(), ".db")
		dbPath := filepath.Join(convDir, e.Name())
		dbInfo, err := e.Info()
		if err != nil || dbInfo.Size() < 100 {
			continue
		}
		var walSize, walMtime int64
		if wInfo, errW := os.Stat(dbPath + "-wal"); errW == nil {
			walSize = wInfo.Size()
			walMtime = wInfo.ModTime().UnixNano()
		}
		prev, hasPrev := st.cursors[sid]
		if hasPrev &&
			prev.dbSize == dbInfo.Size() &&
			prev.dbMtimeNs == dbInfo.ModTime().UnixNano() &&
			prev.walSize == walSize &&
			prev.walMtimeNs == walMtime {
			continue
		}
		afterIdx := -1
		if hasPrev {
			afterIdx = prev.lastStepIdx
		}
		toScan = append(toScan, pendingDB{
			sid:    sid,
			dbPath: dbPath,
			meta: cursorMeta{
				dbSize:      dbInfo.Size(),
				dbMtimeNs:   dbInfo.ModTime().UnixNano(),
				walSize:     walSize,
				walMtimeNs:  walMtime,
				lastStepIdx: afterIdx,
			},
		})
	}
	st.mu.Unlock()

	if len(toScan) == 0 {
		return 0
	}

	ingested := 0
	for _, item := range toScan {
		steps, ok := ReadModelStepsSQL(item.dbPath, "WSL", gwSids, item.meta.lastStepIdx, item.meta.dbMtimeNs/1e9)
		if !ok {
			LogEvent("STORE_SYNC_ERR", "Cannot read usage source; retaining cursor for retry", nil)
			continue
		}
		maxIdx := item.meta.lastStepIdx
		for _, s := range steps {
			if s.StepIdx > maxIdx {
				maxIdx = s.StepIdx
			}
		}
		item.meta.lastStepIdx = maxIdx

		n, err := st.persistUsageBatch(item.sid, item.meta, steps)
		if err != nil {
			LogEvent("STORE_SYNC_ERR", "Usage batch rolled back; cursor retained", map[string]any{"error": err.Error()})
			continue
		}
		ingested += n
	}
	return ingested
}

// A cursor is committed in the same transaction as its steps; memory follows commit.
func (st *StateStore) persistUsageBatch(sid string, meta cursorMeta, steps []ParsedModelStep) (int, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.db == nil {
		return 0, fmt.Errorf("state store unavailable")
	}
	if previous, ok := st.cursors[sid]; ok && previous.lastStepIdx > meta.lastStepIdx {
		return 0, nil
	}
	tx, err := st.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	stmt, err := tx.Prepare(`
INSERT OR REPLACE INTO usage_steps (
	session_id, step_idx, source_cat, ts_sec, ttft_ms, elapsed_ms, tps,
	model_enum, model_id, model_label, model_family, model_tier, quota_group,
	raw_in, raw_cr, aligned_in, aligned_cr, out_tok, vis_out_tok, th_tok,
	has_thinking, cw_tok, tier_mult, in_cost_usd, cr_cost_usd, out_cost_usd,
	th_cost_usd, cost_usd, base1x_cost_usd, nocache_cost_usd, saved_usd, is_dsh_bridge
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
		if _, err = stmt.Exec(s.SessionID, s.StepIdx, s.SourceCategory, s.TsSec, s.TtftMs, s.ElapsedMs, s.Tps,
			s.ModelEnum, s.ModelID, s.ModelLabel, s.ModelFamily, s.ModelTier, s.QuotaGroup,
			s.RawIn, s.RawCr, s.AlignedIn, s.AlignedCr, s.OutTok, s.VisOutTok, s.ThTok,
			hasTh, s.CwTok, s.TierMultiplier, s.InputCostUsd, s.CacheCostUsd, s.OutputCostUsd,
			s.ThinkCostUsd, s.CostUsd, s.Base1xCostUsd, s.NoCacheCostUsd, s.SavedUsd, isDsh); err != nil {
			return 0, err
		}
	}
	if _, err = tx.Exec(`INSERT OR REPLACE INTO ingest_cursor (session_id,db_size,db_mtime_ns,wal_size,wal_mtime_ns,last_step_idx) VALUES (?,?,?,?,?,?)`, sid, meta.dbSize, meta.dbMtimeNs, meta.walSize, meta.walMtimeNs, meta.lastStepIdx); err != nil {
		return 0, err
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	st.cursors[sid] = meta
	return len(steps), nil
}

func (st *StateStore) QueryAllUsageSteps(gwSids map[string]bool) ([]ParsedModelStep, bool) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.db == nil {
		return nil, false
	}
	rows, err := st.db.Query(`
SELECT session_id, step_idx, source_cat, ts_sec, ttft_ms, elapsed_ms, tps,
       model_enum, model_id, model_label, model_family, model_tier, quota_group,
       raw_in, raw_cr, aligned_in, aligned_cr, out_tok, vis_out_tok, th_tok,
       has_thinking, cw_tok, tier_mult, in_cost_usd, cr_cost_usd, out_cost_usd,
       th_cost_usd, cost_usd, base1x_cost_usd, nocache_cost_usd, saved_usd, is_dsh_bridge
FROM usage_steps
ORDER BY ts_sec ASC, step_idx ASC
`)
	if err != nil {
		return nil, false
	}
	defer rows.Close()

	var out []ParsedModelStep
	for rows.Next() {
		var s ParsedModelStep
		var hasTh, isDsh int
		if err := rows.Scan(
			&s.SessionID, &s.StepIdx, &s.SourceCategory, &s.TsSec, &s.TtftMs, &s.ElapsedMs, &s.Tps,
			&s.ModelEnum, &s.ModelID, &s.ModelLabel, &s.ModelFamily, &s.ModelTier, &s.QuotaGroup,
			&s.RawIn, &s.RawCr, &s.AlignedIn, &s.AlignedCr, &s.OutTok, &s.VisOutTok, &s.ThTok,
			&hasTh, &s.CwTok, &s.TierMultiplier, &s.InputCostUsd, &s.CacheCostUsd, &s.OutputCostUsd,
			&s.ThinkCostUsd, &s.CostUsd, &s.Base1xCostUsd, &s.NoCacheCostUsd, &s.SavedUsd, &isDsh,
		); err == nil {
			s.SourceSide = "WSL"
			s.HasThinking = hasTh != 0
			s.IsDshBridge = isDsh != 0
			if s.IsDshBridge || (gwSids != nil && gwSids[strings.ToLower(s.SessionID)]) {
				s.SourceCategory = "CLAUDE_GW"
				s.SourceLabel = "WSL · Claude/Codex GW"
			} else if s.SourceCategory == "" || s.SourceCategory == "WSL_AGY" {
				s.SourceCategory = "WSL_AGY"
				s.SourceLabel = "WSL · Antigravity Host"
			}
			out = append(out, s)
		}
	}
	return out, true
}

func RenderPrometheusMetrics(gs *GatewayServer) string {
	var b strings.Builder
	liveWorkers := 0
	promptsInFlight := len(promptSem)
	parkedCalls := 0
	var compactionsTotal int
	var reusesTotal int
	turnsByKey := make(map[string]int)
	tokensByModelKind := make(map[string]int64)

	if gs != nil {
		gs.mu.RLock()
		for _, s := range gs.sessions {
			s.mu.Lock()
			if s.SessionID != "" {
				liveWorkers++
			}
			parkedCalls += len(s.ParkedCalls)
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
				tk := fmt.Sprintf(`model=%q,mode=%q,stop_reason=%q,usage_source=%q`, m, t.Mode, t.StopReason, src)
				turnsByKey[tk]++
				tokensByModelKind[fmt.Sprintf(`model=%q,kind="input"`, m)] += int64(inT)
				tokensByModelKind[fmt.Sprintf(`model=%q,kind="cached"`, m)] += int64(crT)
				tokensByModelKind[fmt.Sprintf(`model=%q,kind="output"`, m)] += int64(outT)
				tokensByModelKind[fmt.Sprintf(`model=%q,kind="thinking"`, m)] += int64(t.ThinkingTokens)
			}
			s.mu.Unlock()
		}
		gs.mu.RUnlock()
	}

	var reqTotal int64
	if gs != nil {
		reqTotal = gs.reqCounter.Load()
	}

	b.WriteString("# HELP agy_gateway_live_workers Number of live ACP sessions in shared engine.\n")
	b.WriteString("# TYPE agy_gateway_live_workers gauge\n")
	fmt.Fprintf(&b, "agy_gateway_live_workers %d\n", liveWorkers)

	b.WriteString("# HELP agy_gateway_max_live_workers Configured maximum live ACP sessions.\n")
	b.WriteString("# TYPE agy_gateway_max_live_workers gauge\n")
	fmt.Fprintf(&b, "agy_gateway_max_live_workers %d\n", MaxLiveWorkers)

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
	return b.String()
}
