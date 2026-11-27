package main

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	gwstore "agy-acp-gateway-go/internal/store"
)

type GeminiPricing = gwstore.GeminiPricing
type ParsedModelStep = gwstore.ParsedModelStep
type ParsedGenMetadata = gwstore.ParsedGenMetadata
type ArgonDegradationReport = gwstore.ArgonDegradationReport
type PlannerToolCall = gwstore.PlannerToolCall
type ParsedErrorStep = gwstore.ParsedErrorStep
type genStepMeta = gwstore.GenStepMeta
type sqliteImage = gwstore.SQLiteImage

var (
	DefaultGeminiPricing     = gwstore.DefaultGeminiPricing
	DefaultClaudePoolPricing = gwstore.DefaultClaudePoolPricing
	ProGeminiPricing         = gwstore.ProGeminiPricing
	ProClaudePoolPricing     = gwstore.ProClaudePoolPricing
)

func ParseGenMetadataBlob(data []byte) *ParsedGenMetadata {
	return gwstore.ParseGenMetadataBlob(data)
}

func EvaluateArgonDegradation(gm *ParsedGenMetadata, modelID string) *ArgonDegradationReport {
	return gwstore.EvaluateArgonDegradation(gm, modelID)
}

func ParseStepMetadataBlob(r4, r9 []byte) *ParsedModelStep {
	return gwstore.ParseStepMetadataBlob(r4, r9)
}

func openSQLiteImage(dbPath string) (*sqliteImage, error) {
	return gwstore.OpenSQLiteImage(dbPath)
}

func classifyStepSource(sourceSide, defaultCat, defaultLabel string, isGateway bool) (side, cat, label string) {
	return gwstore.ClassifyStepSource(sourceSide, defaultCat, defaultLabel, isGateway)
}

func readGenMetaIndexFromImage(img *sqliteImage) (byStep map[int]genStepMeta, byTok map[[3]int]genStepMeta) {
	return gwstore.ReadGenMetaIndexFromImage(img)
}

func ReadMaxForkStepIdxFromSQLite(dbPath string) int {
	return gwstore.ReadMaxForkStepIdxFromSQLite(dbPath)
}

func readModelStepsFromSQLiteAfterInternal(dbPath, sourceSide string, gwSids map[string]bool, afterIdx int) ([]ParsedModelStep, int, bool) {
	return gwstore.ReadModelStepsFromSQLiteAfterInternal(dbPath, sourceSide, gwSids, afterIdx)
}

func ReadModelStepsFromSQLiteAfter(dbPath, sourceSide string, gwSids map[string]bool, afterIdx int) ([]ParsedModelStep, bool) {
	return gwstore.ReadModelStepsFromSQLiteAfter(dbPath, sourceSide, gwSids, afterIdx)
}

func ReadModelStepsFromSQLite(dbPath, sourceSide string, gwSids map[string]bool) []ParsedModelStep {
	return gwstore.ReadModelStepsFromSQLite(dbPath, sourceSide, gwSids)
}

func ReadModelStepsFromProtobuf(pbPath, sourceSide, defaultCat, defaultLabel string, gwSids map[string]bool, afterIdx int, fallbackTsSec int64) ([]ParsedModelStep, bool) {
	return gwstore.ReadModelStepsFromProtobuf(pbPath, sourceSide, defaultCat, defaultLabel, gwSids, afterIdx, fallbackTsSec)
}

type dbCacheEntry struct {
	mtimeUnixNano int64
	size          int64
	walMtimeNano  int64
	walSize       int64
	isKnownGw     bool
	sourceSide    string
	steps         []ParsedModelStep
	lastCheckedAt time.Time
}

type DBStepCacheManager struct {
	mu      sync.RWMutex
	entries map[string]*dbCacheEntry
}

var GlobalDBCache = &DBStepCacheManager{
	entries: make(map[string]*dbCacheEntry),
}

func (m *DBStepCacheManager) GetSteps(dbPath, sourceSide string, gwSids map[string]bool) []ParsedModelStep {
	now := time.Now()
	sessionID := strings.ToLower(strings.TrimSuffix(filepath.Base(dbPath), ".db"))
	isKnownGw := gwSids[sessionID]

	m.mu.RLock()
	cached, exists := m.entries[dbPath]
	m.mu.RUnlock()

	if exists && cached.isKnownGw == isKnownGw && cached.sourceSide == sourceSide {
		ageSinceMod := now.Sub(time.Unix(0, cached.mtimeUnixNano))
		if ageSinceMod > time.Hour && now.Sub(cached.lastCheckedAt) < 30*time.Second {
			return cached.steps
		}
	}

	st, err := os.Stat(dbPath)
	if err != nil || st.Size() < 100 {
		return nil
	}
	var walMtime, walSize int64
	if !strings.HasPrefix(dbPath, "/mnt/c/") {
		if wst, errW := os.Stat(dbPath + "-wal"); errW == nil {
			walMtime = wst.ModTime().UnixNano()
			walSize = wst.Size()
		}
	}

	if exists &&
		cached.mtimeUnixNano == st.ModTime().UnixNano() &&
		cached.size == st.Size() &&
		cached.walMtimeNano == walMtime &&
		cached.walSize == walSize &&
		cached.isKnownGw == isKnownGw &&
		cached.sourceSide == sourceSide {
		m.mu.Lock()
		cached.lastCheckedAt = now
		m.mu.Unlock()
		return cached.steps
	}

	var steps []ParsedModelStep
	if sourceSide == "APP" {
		if exists && len(cached.steps) > 0 && cached.isKnownGw == isKnownGw && cached.sourceSide == sourceSide {
			lastIdx := cached.steps[len(cached.steps)-1].StepIdx
			if delta, ok := ReadModelStepsFromSQLiteAfter(dbPath, sourceSide, gwSids, lastIdx); ok {
				steps = make([]ParsedModelStep, 0, len(cached.steps)+len(delta))
				steps = append(steps, cached.steps...)
				steps = append(steps, delta...)
			}
		}
		if steps == nil {
			steps = ReadModelStepsFromSQLite(dbPath, sourceSide, gwSids)
		}
	} else {
		if exists && len(cached.steps) > 0 && cached.isKnownGw == isKnownGw && cached.sourceSide == sourceSide {
			lastIdx := cached.steps[len(cached.steps)-1].StepIdx
			if delta, ok := ReadModelStepsSQL(dbPath, sourceSide, gwSids, lastIdx, st.ModTime().Unix()); ok {
				steps = make([]ParsedModelStep, 0, len(cached.steps)+len(delta))
				steps = append(steps, cached.steps...)
				steps = append(steps, delta...)
			}
		}
		if steps == nil {
			if sqlSteps, ok := ReadModelStepsSQL(dbPath, sourceSide, gwSids, -1, st.ModTime().Unix()); ok {
				steps = sqlSteps
			} else {
				steps = ReadModelStepsFromSQLite(dbPath, sourceSide, gwSids)
			}
		}
	}

	m.mu.Lock()
	m.entries[dbPath] = &dbCacheEntry{
		mtimeUnixNano: st.ModTime().UnixNano(),
		size:          st.Size(),
		walMtimeNano:  walMtime,
		walSize:       walSize,
		isKnownGw:     isKnownGw,
		sourceSide:    sourceSide,
		steps:         steps,
		lastCheckedAt: now,
	}
	m.mu.Unlock()
	return steps
}

func (m *DBStepCacheManager) Delete(dbPath string) {
	if m == nil || dbPath == "" {
		return
	}
	m.mu.Lock()
	delete(m.entries, dbPath)
	m.mu.Unlock()
}

func sqliteFileURI(dbPath, rawQuery string) string { return gwstore.SQLiteFileURI(dbPath, rawQuery) }

func openReadOnlySQLite(dbPath string) (*sql.DB, error) {
	return gwstore.OpenReadOnlySQLite(dbPath)
}

func queryModelStepsFromDB(db *sql.DB, sessionID, sourceSide string, gwSids map[string]bool, afterIdx int, fallbackTsSec int64) ([]ParsedModelStep, bool) {
	return gwstore.QueryModelStepsFromDB(db, sessionID, sourceSide, gwSids, afterIdx, fallbackTsSec)
}

func ReadModelStepsSQL(dbPath, sourceSide string, gwSids map[string]bool, afterIdx int, fallbackTsSec int64) ([]ParsedModelStep, bool) {
	return gwstore.ReadModelStepsSQL(dbPath, sourceSide, gwSids, afterIdx, fallbackTsSec)
}

func computeStepTiming(ttftMs, streamMs, elapsedMs, outTok int) (normTtftMs, normStreamMs, normElapsedMs int, e2eTps, decodeTps float64) {
	return gwstore.ComputeStepTiming(ttftMs, streamMs, elapsedMs, outTok)
}

func readSessionServedModels(db *sql.DB) (byStep map[int]string, byTok map[[3]int]string) {
	return gwstore.ReadSessionServedModels(db)
}

func ReadLatestStepFromSQLiteFast(dbPath string) *ParsedModelStep {
	return gwstore.ReadLatestStepFromSQLiteFast(dbPath)
}

func ReadMaxSessionGenMetadataIdx(sessionID string) int {
	return gwstore.ReadMaxSessionGenMetadataIdx(ConversationsDir(), sessionID)
}

func (s *AcpProjectSession) readMaxSessionGenMetadataIdx(sessionID string) int {
	if s != nil {
		if db, rel := s.borrowReadOnlySessionDB(sessionID); db != nil {
			defer rel()
			return gwstore.QueryMaxGenMetadataIdxDB(db)
		}
	}
	return ReadMaxSessionGenMetadataIdx(sessionID)
}

func ReadMaxSessionStepIdx(sessionID string) int {
	return gwstore.ReadMaxSessionStepIdx(ConversationsDir(), sessionID)
}

func (s *AcpProjectSession) readMaxSessionStepIdx(sessionID string) int {
	if s != nil {
		if db, rel := s.borrowReadOnlySessionDB(sessionID); db != nil {
			defer rel()
			return gwstore.QueryMaxSessionStepIdxDB(db)
		}
	}
	return ReadMaxSessionStepIdx(sessionID)
}

func ReadSessionGenMetadataAfter(sessionID string, afterIdx int) (rows []*ParsedGenMetadata, maxIdx int, ok bool) {
	return gwstore.ReadSessionGenMetadataAfter(ConversationsDir(), sessionID, afterIdx)
}

func (s *AcpProjectSession) readSessionGenMetadataAfter(sessionID string, afterIdx int) (rows []*ParsedGenMetadata, maxIdx int, ok bool) {
	if s != nil {
		if db, rel := s.borrowReadOnlySessionDB(sessionID); db != nil {
			rows, maxIdx, ok = gwstore.QuerySessionGenMetadataAfterDB(db, sessionID, afterIdx)
			rel()
			if ok {
				return rows, maxIdx, true
			}
			s.invalidateReadOnlySessionDB(sessionID)
		}
	}
	return ReadSessionGenMetadataAfter(sessionID, afterIdx)
}

func ReadSessionStepsAfter(sessionID string, afterIdx int) (steps []ParsedModelStep, maxIdx int, ok bool) {
	return gwstore.ReadSessionStepsAfter(ConversationsDir(), sessionID, afterIdx)
}

func (s *AcpProjectSession) readSessionStepsAfter(sessionID string, afterIdx int) (steps []ParsedModelStep, maxIdx int, ok bool) {
	if s != nil && sessionID != "" {
		if db, rel := s.borrowReadOnlySessionDB(sessionID); db != nil {
			maxIdx = afterIdx
			dbPath := filepath.Join(ConversationsDir(), sessionID+".db")
			var fallbackTs int64
			if st, err := os.Stat(dbPath); err == nil {
				fallbackTs = st.ModTime().Unix()
			}
			steps, ok = gwstore.QueryModelStepsFromDB(db, strings.ToLower(sessionID), "WSL", map[string]bool{strings.ToLower(sessionID): true}, afterIdx, fallbackTs)
			rel()
			if ok {
				for _, stp := range steps {
					if stp.StepIdx > maxIdx {
						maxIdx = stp.StepIdx
					}
				}
				return steps, maxIdx, true
			}
			s.invalidateReadOnlySessionDB(sessionID)
		}
	}
	return ReadSessionStepsAfter(sessionID, afterIdx)
}

func (s *AcpProjectSession) readSessionStepTierAndMaxIdxAfter(sessionID string, afterIdx int) (actualTier string, maxIdx int, ok bool) {
	if s != nil && sessionID != "" {
		if db, rel := s.borrowReadOnlySessionDB(sessionID); db != nil {
			actualTier, maxIdx, ok = gwstore.QueryStepTierAndMaxIdxAfterDB(db, afterIdx)
			rel()
			if ok {
				return actualTier, maxIdx, true
			}
			s.invalidateReadOnlySessionDB(sessionID)
		}
	}
	return gwstore.ReadSessionStepTierAndMaxIdxAfter(ConversationsDir(), sessionID, afterIdx)
}

func ReadLatestPlannerToolCalls(sessionID string, afterIdx int) (plannerIdx int, calls []PlannerToolCall) {
	return gwstore.ReadLatestPlannerToolCalls(ConversationsDir(), sessionID, afterIdx)
}

func (s *AcpProjectSession) readLatestPlannerToolCalls(sessionID string, afterIdx int) (plannerIdx int, calls []PlannerToolCall) {
	if s != nil {
		if db, rel := s.borrowReadOnlySessionDB(sessionID); db != nil {
			defer rel()
			return gwstore.QueryLatestPlannerToolCallsDB(db, afterIdx)
		}
	}
	return ReadLatestPlannerToolCalls(sessionID, afterIdx)
}

func ParseErrorStepPayload(stepPayload, errorDetailsCol []byte) *ParsedErrorStep {
	return gwstore.ParseErrorStepPayload(stepPayload, errorDetailsCol)
}

func ReadMaxSessionAllStepIdx(sessionID string) int {
	return gwstore.ReadMaxSessionAllStepIdx(ConversationsDir(), sessionID)
}

func (s *AcpProjectSession) readMaxSessionAllStepIdx(sessionID string) int {
	floorIdx := -1
	if s != nil && sessionID != "" {
		s.mu.Lock()
		if s.SessionID == sessionID && s.LastSeenDbStepIdx > floorIdx {
			floorIdx = s.LastSeenDbStepIdx
		}
		s.mu.Unlock()
	}
	idx := -1
	if s != nil {
		if db, rel := s.borrowReadOnlySessionDB(sessionID); db != nil {
			idx = gwstore.QueryMaxAllStepIdxDB(db)
			rel()
		} else {
			idx = ReadMaxSessionAllStepIdx(sessionID)
		}
	} else {
		idx = ReadMaxSessionAllStepIdx(sessionID)
	}
	if idx < floorIdx {
		return floorIdx
	}
	return idx
}

func ReadSessionErrorStepsAfter(sessionID string, afterIdx int) (steps []ParsedErrorStep, maxIdx int, ok bool) {
	return gwstore.ReadSessionErrorStepsAfter(ConversationsDir(), sessionID, afterIdx)
}

func (s *AcpProjectSession) readSessionErrorStepsAfter(sessionID string, afterIdx int) (steps []ParsedErrorStep, maxIdx int, ok bool) {
	if s != nil {
		if db, rel := s.borrowReadOnlySessionDB(sessionID); db != nil {
			steps, maxIdx, ok = gwstore.QueryErrorStepsAfterDB(db, afterIdx)
			rel()
			if ok {
				return steps, maxIdx, true
			}
			s.invalidateReadOnlySessionDB(sessionID)
		}
	}
	return ReadSessionErrorStepsAfter(sessionID, afterIdx)
}
