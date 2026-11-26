package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"agy-acp-gateway-go/internal/acp"
	gwstore "agy-acp-gateway-go/internal/store"
)

func pbVarint(v uint64) []byte {
	var out []byte
	for {
		b := byte(v & 0x7f)
		v >>= 7
		if v != 0 {
			b |= 0x80
		}
		out = append(out, b)
		if v == 0 {
			break
		}
	}
	return out
}

func pbTag(fn, wt uint64) []byte {
	return pbVarint((fn << 3) | wt)
}

func pbBytesField(fn uint64, data []byte) []byte {
	var out []byte
	out = append(out, pbTag(fn, 2)...)
	out = append(out, pbVarint(uint64(len(data)))...)
	out = append(out, data...)
	return out
}

func pbStringField(fn uint64, s string) []byte {
	return pbBytesField(fn, []byte(s))
}

func pbVarintField(fn, v uint64) []byte {
	var out []byte
	out = append(out, pbTag(fn, 0)...)
	out = append(out, pbVarint(v)...)
	return out
}

func buildTestErrorStepPayload(userMsg, shortErr, modelErr string, errCode int, shouldShowUser bool) []byte {
	var details []byte
	if userMsg != "" {
		details = append(details, pbStringField(1, userMsg)...)
	}
	if shortErr != "" {
		details = append(details, pbStringField(2, shortErr)...)
	}
	if errCode > 0 {
		details = append(details, pbVarintField(7, uint64(errCode))...)
	}
	if modelErr != "" {
		details = append(details, pbStringField(9, modelErr)...)
	}
	var errMsg []byte
	if len(details) > 0 {
		errMsg = append(errMsg, pbBytesField(3, details)...)
	}
	if shouldShowUser {
		errMsg = append(errMsg, pbVarintField(5, 1)...)
	}
	var step []byte
	step = append(step, pbVarintField(1, 17)...)
	step = append(step, pbVarintField(4, 3)...)
	step = append(step, pbBytesField(24, errMsg)...)
	return step
}

func createTestStepsWALDB(t *testing.T, dbPath string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open sqlite %s: %v", dbPath, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	stmts := []string{
		`PRAGMA journal_mode=WAL;`,
		`CREATE TABLE steps (
			idx INTEGER PRIMARY KEY,
			step_type INTEGER,
			status INTEGER,
			has_subtrajectory INTEGER,
			metadata BLOB,
			error_details BLOB,
			permissions BLOB,
			task_details BLOB,
			render_info BLOB,
			step_payload BLOB,
			step_format INTEGER DEFAULT 0
		);`,
		`CREATE TABLE gen_metadata (idx INTEGER PRIMARY KEY, data BLOB);`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}
	return db
}

func insertTestStepRow(t *testing.T, db *sql.DB, idx, stepType int, payload, errCol []byte) {
	t.Helper()
	if _, err := db.Exec(
		`INSERT INTO steps (idx, step_type, status, step_payload, error_details, step_format) VALUES (?, ?, 3, ?, ?, 0)`,
		idx, stepType, payload, errCol,
	); err != nil {
		t.Fatalf("insert step idx=%d: %v", idx, err)
	}
}

func TestParseErrorStepPayload_HiddenTerminalAndFallback(t *testing.T) {
	hiddenPayload := buildTestErrorStepPayload(
		"",
		"API error (attempt 1): RESOURCE_EXHAUSTED (code 429): Resource has been exhausted (e.g. check quota).",
		"",
		0,
		false,
	)
	got := ParseErrorStepPayload(hiddenPayload, nil)
	if got == nil {
		t.Fatalf("expected non-nil ParsedErrorStep for hidden retry")
	}
	if got.ShouldShowUser {
		t.Fatalf("expected ShouldShowUser=false for hidden retry")
	}
	if !strings.Contains(got.ShortError, "API error (attempt 1): RESOURCE_EXHAUSTED") {
		t.Fatalf("unexpected ShortError: %q", got.ShortError)
	}
	att, det, ok := parseRetryErrorText(got.ShortError)
	if !ok || att != 1 || !strings.Contains(det, "RESOURCE_EXHAUSTED (code 429)") {
		t.Fatalf("parseRetryErrorText mismatch: ok=%v att=%d det=%q", ok, att, det)
	}

	termPayload := buildTestErrorStepPayload(
		"Agent execution terminated due to error.",
		"RESOURCE_EXHAUSTED (code 429): Resource has been exhausted (e.g. check quota).",
		"model detail",
		429,
		true,
	)
	gotTerm := ParseErrorStepPayload(termPayload, nil)
	if gotTerm == nil || !gotTerm.ShouldShowUser || gotTerm.ErrorCode != 429 || gotTerm.UserErrorMessage != "Agent execution terminated due to error." {
		t.Fatalf("unexpected terminal ParsedErrorStep: %+v", gotTerm)
	}

	// Fallback to error_details column when step_payload lacks field 24.
	var rawDetails []byte
	rawDetails = append(rawDetails, pbStringField(2, "API error (attempt 3): UNAVAILABLE (code 503): backend down")...)
	rawDetails = append(rawDetails, pbVarintField(7, 503)...)
	gotFallback := ParseErrorStepPayload(pbVarintField(1, 17), rawDetails)
	if gotFallback == nil || gotFallback.ErrorCode != 503 || !strings.Contains(gotFallback.ShortError, "attempt 3") {
		t.Fatalf("unexpected fallback ParsedErrorStep: %+v", gotFallback)
	}
}

func TestQueryErrorStepsAfterDB_AllStepWatermarkAndWAL(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "sess-wal.db")
	wdb := createTestStepsWALDB(t, dbPath)

	// Prior turn: user input (14), planner (15), old error (17) at idx=3.
	insertTestStepRow(t, wdb, 0, 14, pbVarintField(1, 14), nil)
	insertTestStepRow(t, wdb, 1, 15, pbVarintField(1, 15), nil)
	insertTestStepRow(t, wdb, 3, 17, buildTestErrorStepPayload("", "API error (attempt 1): UNAVAILABLE (code 503): old", "", 503, false), nil)

	roDB, err := openReadOnlySQLite(dbPath)
	if err != nil {
		t.Fatalf("openReadOnlySQLite: %v", err)
	}
	defer roDB.Close()

	// QueryMaxSessionStepIdxDB only sees step_type=15 (idx=1), whereas QueryMaxAllStepIdxDB sees idx=3.
	if max15 := gwstore.QueryMaxSessionStepIdxDB(roDB); max15 != 1 {
		t.Fatalf("expected QueryMaxSessionStepIdxDB=1, got %d", max15)
	}
	watermark := gwstore.QueryMaxAllStepIdxDB(roDB)
	if watermark != 3 {
		t.Fatalf("expected QueryMaxAllStepIdxDB=3, got %d", watermark)
	}

	// New turn writes idx=4 (user), idx=5 (hidden retry 1), idx=6 (hidden retry 2) in WAL.
	insertTestStepRow(t, wdb, 4, 14, pbVarintField(1, 14), nil)
	insertTestStepRow(t, wdb, 5, 17, buildTestErrorStepPayload("", "API error (attempt 1): RESOURCE_EXHAUSTED (code 429): quota", "", 0, false), nil)
	insertTestStepRow(t, wdb, 6, 17, buildTestErrorStepPayload("", "API error (attempt 2): RESOURCE_EXHAUSTED (code 429): quota", "", 0, false), nil)

	rows, maxIdx, ok := gwstore.QueryErrorStepsAfterDB(roDB, watermark)
	if !ok || maxIdx != 6 || len(rows) != 2 {
		t.Fatalf("expected 2 error rows after watermark=3 (maxIdx=6), got ok=%v maxIdx=%d len=%d", ok, maxIdx, len(rows))
	}
	if rows[0].StepIdx != 5 || rows[1].StepIdx != 6 {
		t.Fatalf("unexpected row indices: %+v", rows)
	}
}

func TestActiveTurnDBRetryDedup_1512And1513(t *testing.T) {
	// 1. agy 1.5.12: both ACP stream and DB report the same 2 retries -> retryCountLocked must be 2 (no double count).
	t1512 := newActiveTurnState(nil, nil)
	t1512.mu.Lock()
	_ = t1512.acceptTextLocked("⚠ API error (attempt 1): UNAVAILABLE (code 503): transient\n")
	_ = t1512.acceptTextLocked("⚠ API error (attempt 2): UNAVAILABLE (code 503): transient\n")
	t1512.applyDBErrorStepsLocked([]ParsedErrorStep{
		{StepIdx: 1, ShouldShowUser: false, ShortError: "API error (attempt 1): UNAVAILABLE (code 503): transient"},
		{StepIdx: 2, ShouldShowUser: false, ShortError: "API error (attempt 2): UNAVAILABLE (code 503): transient"},
	})
	if got := t1512.retryCountLocked(); got != 2 {
		t.Fatalf("1.5.12 dedup failed: expected retryCount=2, got %d", got)
	}
	t1512.mu.Unlock()

	// 2. agy 1.5.13 silent retries followed by normal text recovery: retryCount=2, no fake terminal failure.
	tRecovered := newActiveTurnState(nil, nil)
	tRecovered.mu.Lock()
	out := tRecovered.acceptTextLocked("Hello world")
	tRecovered.applyDBErrorStepsLocked([]ParsedErrorStep{
		{StepIdx: 1, ShouldShowUser: false, ShortError: "API error (attempt 1): UNAVAILABLE (code 503): transient"},
		{StepIdx: 2, ShouldShowUser: false, ShortError: "API error (attempt 2): UNAVAILABLE (code 503): transient"},
	})
	if out != "Hello world" || tRecovered.retryCountLocked() != 2 || tRecovered.effectiveTerminalDetailLocked() != "" {
		t.Fatalf("1.5.13 recovered turn mismatch: out=%q retry=%d detail=%q", out, tRecovered.retryCountLocked(), tRecovered.effectiveTerminalDetailLocked())
	}
	tRecovered.mu.Unlock()

	// 3. agy 1.5.13 silent 429 retries followed by generic terminal notice on ACP -> recovers 429 SoftError!
	tFailed := newActiveTurnState(nil, nil)
	tFailed.mu.Lock()
	_ = tFailed.acceptTextLocked("⚠ Agent execution terminated due to error.\n")
	tFailed.applyDBErrorStepsLocked([]ParsedErrorStep{
		{StepIdx: 1, ShouldShowUser: false, ShortError: "API error (attempt 1): RESOURCE_EXHAUSTED (code 429): Resource has been exhausted (e.g. check quota)."},
		{StepIdx: 2, ShouldShowUser: false, ShortError: "API error (attempt 2): RESOURCE_EXHAUSTED (code 429): Resource has been exhausted (e.g. check quota)."},
		{StepIdx: 3, ShouldShowUser: true, UserErrorMessage: "Agent execution terminated due to error."},
	})
	eff := formatTerminalNoticeWithDetail(tFailed.noticeText, tFailed.effectiveTerminalDetailLocked())
	retryCnt := tFailed.retryCountLocked()
	tFailed.mu.Unlock()
	if retryCnt != 2 {
		t.Fatalf("expected retryCnt=2, got %d", retryCnt)
	}
	se := DetectAntigravitySoftError(noticeOnlyText("", eff, 0))
	if se == nil || se.StatusCode != http.StatusTooManyRequests || se.ErrorType != "rate_limit_error" {
		t.Fatalf("expected 429 rate_limit_error after DB detail backfill, got eff=%q se=%+v", eff, se)
	}

	// 4. Partial text + later distinct visible terminal error -> prefers latest terminal error over earlier retry.
	tPartial := newActiveTurnState(nil, nil)
	tPartial.mu.Lock()
	_ = tPartial.acceptTextLocked("partial reply\n⚠ Agent execution terminated due to error.\n")
	tPartial.applyDBErrorStepsLocked([]ParsedErrorStep{
		{StepIdx: 1, ShouldShowUser: false, ShortError: "API error (attempt 1): UNAVAILABLE (code 503): earlier transient"},
		{StepIdx: 4, ShouldShowUser: true, UserErrorMessage: "Agent execution terminated due to error.", ShortError: "DEADLINE_EXCEEDED (code 504): upstream deadline exceeded"},
	})
	det := tPartial.effectiveTerminalDetailLocked()
	tPartial.mu.Unlock()
	if !strings.Contains(det, "DEADLINE_EXCEEDED") {
		t.Fatalf("expected latest terminal detail DEADLINE_EXCEEDED, got %q", det)
	}
}

type silentRetryMockEngine struct {
	mu          sync.Mutex
	promptFn    func(sid string, cancelled <-chan struct{}) (json.RawMessage, error)
	cancelChans map[string]chan struct{}
	cancelCount int
}

func newSilentRetryMockEngine(fn func(sid string, cancelled <-chan struct{}) (json.RawMessage, error)) *silentRetryMockEngine {
	return &silentRetryMockEngine{
		promptFn:    fn,
		cancelChans: map[string]chan struct{}{},
	}
}

func (m *silentRetryMockEngine) EnsureStarted() error                         { return nil }
func (m *silentRetryMockEngine) StopEngine()                                  {}
func (m *silentRetryMockEngine) IsAlive() bool                                { return true }
func (m *silentRetryMockEngine) IsBooting() bool                              { return false }
func (m *silentRetryMockEngine) CurrentGen() uint64                           { return 1 }
func (m *silentRetryMockEngine) Capabilities() acp.Capabilities               { return acp.Capabilities{} }
func (m *silentRetryMockEngine) RegisterListener(string, acp.SessionListener) {}
func (m *silentRetryMockEngine) UnregisterSession(string)                     {}
func (m *silentRetryMockEngine) CloseSession(string) error                    { return nil }
func (m *silentRetryMockEngine) EnqueueControlReply(map[string]any)           {}
func (m *silentRetryMockEngine) BinaryInfo() map[string]any                   { return nil }
func (m *silentRetryMockEngine) CancelSession(sid string) {
	m.mu.Lock()
	m.cancelCount++
	ch, ok := m.cancelChans[sid]
	if ok {
		select {
		case <-ch:
		default:
			close(ch)
		}
	}
	m.mu.Unlock()
}
func (m *silentRetryMockEngine) RPC(method string, params any, _ time.Duration) (json.RawMessage, error) {
	if method != "session/prompt" {
		return json.RawMessage(`{}`), nil
	}
	pm, _ := params.(map[string]any)
	sid, _ := pm["sessionId"].(string)
	ch := make(chan struct{})
	m.mu.Lock()
	m.cancelChans[sid] = ch
	m.mu.Unlock()
	return m.promptFn(sid, ch)
}

func TestDBRetryWatcher_FastCancelWafProbeAndColdStart(t *testing.T) {
	convDir := t.TempDir()
	t.Setenv("AGY_CONV_DIR", convDir)

	origInterval := dbRetryPollInterval
	dbRetryPollInterval = 15 * time.Millisecond
	t.Cleanup(func() { dbRetryPollInterval = origInterval })

	// Subtest A: WAF bisect probe (minAtt = 1) cancels immediately on silent DB attempt 1.
	t.Run("WafProbeCancelsOnAttempt1", func(t *testing.T) {
		sid := "probe-silent-429"
		dbPath := filepath.Join(convDir, sid+".db")
		wdb := createTestStepsWALDB(t, dbPath)

		eng := newSilentRetryMockEngine(func(sid string, cancelled <-chan struct{}) (json.RawMessage, error) {
			insertTestStepRow(t, wdb, 0, 14, pbVarintField(1, 14), nil)
			insertTestStepRow(t, wdb, 1, 17, buildTestErrorStepPayload(
				"",
				"API error (attempt 1): RESOURCE_EXHAUSTED (code 429): Resource has been exhausted (e.g. check quota).",
				"", 0, false,
			), nil)
			select {
			case <-cancelled:
				return json.RawMessage(`{"stopReason":"cancelled"}`), nil
			case <-time.After(3 * time.Second):
				return json.RawMessage(`{"stopReason":"end_turn"}`), nil
			}
		})

		sess := NewAcpProjectSession("util:waf-bisect-test", DefaultCwd, "")
		sess.Engine = eng
		sess.engineGen = 1
		sess.SessionID = sid
		sess.isWafBisectProbe = true
		defer sess.closeReadOnlyDB()

		t0 := time.Now()
		_, err := sess.executePromptOrPark(context.Background(), []map[string]any{{"type": "text", "text": "Reply OK"}}, nil, 0)
		var fc *wafFastCancelledError
		if !errors.As(err, &fc) {
			t.Fatalf("expected *wafFastCancelledError, got %v", err)
		}
		if !strings.Contains(fc.Detail, "RESOURCE_EXHAUSTED (code 429)") {
			t.Fatalf("unexpected fast-cancel detail: %q", fc.Detail)
		}
		if elapsed := time.Since(t0); elapsed > 2*time.Second {
			t.Fatalf("expected fast cancel in <2s, took %v", elapsed)
		}
	})

	// Subtest B: Cold-start user turn (allowWafFastCancel=true, MessageCount=0, minAtt=2)
	// waits past attempt 1 and fast-cancels immediately once attempt 2 lands in WAL.
	t.Run("ColdStartCancelsOnAttempt2Only", func(t *testing.T) {
		sid := "coldstart-silent-429"
		dbPath := filepath.Join(convDir, sid+".db")
		wdb := createTestStepsWALDB(t, dbPath)

		attempt2Written := make(chan struct{})
		eng := newSilentRetryMockEngine(func(sid string, cancelled <-chan struct{}) (json.RawMessage, error) {
			insertTestStepRow(t, wdb, 0, 14, pbVarintField(1, 14), nil)
			insertTestStepRow(t, wdb, 1, 17, buildTestErrorStepPayload(
				"",
				"API error (attempt 1): RESOURCE_EXHAUSTED (code 429): Resource has been exhausted (e.g. check quota).",
				"", 0, false,
			), nil)
			select {
			case <-cancelled:
				return nil, errors.New("cancelled too early on attempt 1")
			case <-time.After(80 * time.Millisecond):
			}
			insertTestStepRow(t, wdb, 2, 17, buildTestErrorStepPayload(
				"",
				"API error (attempt 2): RESOURCE_EXHAUSTED (code 429): Resource has been exhausted (e.g. check quota).",
				"", 0, false,
			), nil)
			close(attempt2Written)
			select {
			case <-cancelled:
				return json.RawMessage(`{"stopReason":"cancelled"}`), nil
			case <-time.After(3 * time.Second):
				return json.RawMessage(`{"stopReason":"end_turn"}`), nil
			}
		})

		sess := NewAcpProjectSession("conv:coldstart-test", DefaultCwd, "")
		sess.Engine = eng
		sess.engineGen = 1
		sess.SessionID = sid
		sess.allowWafFastCancel = true
		sess.MessageCount = 0
		defer sess.closeReadOnlyDB()
		_, err := sess.executePromptOrPark(context.Background(), []map[string]any{{"type": "text", "text": "hello"}}, nil, 0)
		select {
		case <-attempt2Written:
		default:
			t.Fatalf("cancelled before attempt 2 was written")
		}
		var fc *wafFastCancelledError
		if !errors.As(err, &fc) {
			t.Fatalf("expected *wafFastCancelledError, got %v", err)
		}
	})

	// Subtest C: Steady-state turn (no poller) still backfills RetryCount and terminal detail at turn end.
	t.Run("SteadyStateTurnEndBackfill", func(t *testing.T) {
		sid := "steady-silent-retry"
		dbPath := filepath.Join(convDir, sid+".db")
		wdb := createTestStepsWALDB(t, dbPath)
		// Seed a prior turn's retry at idx=1 so we verify watermark isolation.
		insertTestStepRow(t, wdb, 1, 17, buildTestErrorStepPayload("", "API error (attempt 1): UNAVAILABLE (code 503): old turn", "", 0, false), nil)

		var sess *AcpProjectSession
		eng := newSilentRetryMockEngine(func(sid string, _ <-chan struct{}) (json.RawMessage, error) {
			insertTestStepRow(t, wdb, 2, 14, pbVarintField(1, 14), nil)
			insertTestStepRow(t, wdb, 3, 17, buildTestErrorStepPayload("", "API error (attempt 1): UNAVAILABLE (code 503): retry 1", "", 0, false), nil)
			insertTestStepRow(t, wdb, 4, 17, buildTestErrorStepPayload("", "API error (attempt 2): UNAVAILABLE (code 503): retry 2", "", 0, false), nil)
			sess.handleSessionUpdate(map[string]any{
				"sessionUpdate": "agent_message_chunk",
				"content":       map[string]any{"type": "text", "text": "Recovered OK"},
			})
			return json.RawMessage(`{"stopReason":"end_turn"}`), nil
		})

		sess = NewAcpProjectSession("conv:steady-test", DefaultCwd, "")
		sess.Engine = eng
		sess.engineGen = 1
		sess.SessionID = sid
		sess.MessageCount = 4
		defer sess.closeReadOnlyDB()
		res, err := sess.executePromptOrPark(context.Background(), []map[string]any{{"type": "text", "text": "hi"}}, nil, 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Text != "Recovered OK" || res.RetryCount != 2 {
			t.Fatalf("expected Text='Recovered OK' RetryCount=2, got Text=%q RetryCount=%d", res.Text, res.RetryCount)
		}
	})

	// Subtest D: Silent 429 retries exhaust -> agy 1.5.13 emits only generic terminal notice on ACP ->
	// turn-end backfill restores 429 detail so DetectAntigravitySoftError returns HTTP 429.
	t.Run("SilentRetryExhaustedBackfills429SoftError", func(t *testing.T) {
		sid := "exhausted-silent-429"
		dbPath := filepath.Join(convDir, sid+".db")
		wdb := createTestStepsWALDB(t, dbPath)

		var sess *AcpProjectSession
		eng := newSilentRetryMockEngine(func(sid string, _ <-chan struct{}) (json.RawMessage, error) {
			insertTestStepRow(t, wdb, 0, 14, pbVarintField(1, 14), nil)
			insertTestStepRow(t, wdb, 1, 17, buildTestErrorStepPayload(
				"",
				"API error (attempt 1): RESOURCE_EXHAUSTED (code 429): Resource has been exhausted (e.g. check quota).",
				"", 0, false,
			), nil)
			insertTestStepRow(t, wdb, 2, 17, buildTestErrorStepPayload(
				"Agent execution terminated due to error.",
				"",
				"", 429, true,
			), nil)
			sess.handleSessionUpdate(map[string]any{
				"sessionUpdate": "agent_message_chunk",
				"content":       map[string]any{"type": "text", "text": "\n⚠ Agent execution terminated due to error.\n"},
			})
			return json.RawMessage(`{"stopReason":"end_turn"}`), nil
		})

		sess = NewAcpProjectSession("conv:exhausted-test", DefaultCwd, "")
		sess.Engine = eng
		sess.engineGen = 1
		sess.SessionID = sid
		sess.MessageCount = 2
		defer sess.closeReadOnlyDB()

		res, err := sess.executePromptOrPark(context.Background(), []map[string]any{{"type": "text", "text": "hi"}}, nil, 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.RetryCount != 1 {
			t.Fatalf("expected RetryCount=1, got %d", res.RetryCount)
		}
		se := DetectAntigravitySoftError(res.Text)
		if se == nil || se.StatusCode != http.StatusTooManyRequests || se.ErrorType != "rate_limit_error" {
			t.Fatalf("expected 429 rate_limit_error from res.Text=%q, got %+v", res.Text, se)
		}
	})
}

func TestParseErrorStepPayload_VarintOverflowAndCodeOnlyFallback(t *testing.T) {
	// 1. Huge 64-bit length varints must never overflow int conversion or panic on slice bounds
	for _, hugeLen := range []uint64{math.MaxUint64, uint64(math.MaxInt), uint64(math.MaxInt) + 1, 1 << 62} {
		var badOuter []byte
		badOuter = append(badOuter, pbTag(24, 2)...)
		badOuter = append(badOuter, pbVarint(hugeLen)...)
		badOuter = append(badOuter, 0x08, 0x01)
		_ = ParseErrorStepPayload(badOuter, nil)

		var badInner []byte
		badInner = append(badInner, pbTag(3, 2)...)
		badInner = append(badInner, pbVarint(hugeLen)...)
		badInner = append(badInner, 0x08, 0x01)
		var wrapped []byte
		wrapped = append(wrapped, pbVarintField(1, 17)...)
		wrapped = append(wrapped, pbBytesField(24, badInner)...)
		_ = ParseErrorStepPayload(wrapped, badInner)
	}

	// 2. Hidden retry & terminal step with empty text fields and ErrorCode > 0 synthesize "HTTP <code>"
	var turn activeTurnState
	steps := []ParsedErrorStep{
		{StepIdx: 10, ErrorCode: 503, ShouldShowUser: false},
		{StepIdx: 11, ErrorCode: 503, ShouldShowUser: true},
	}
	newHidden, _, _ := turn.applyDBErrorStepsLocked(steps)
	if !newHidden || turn.dbRetryCount != 1 || turn.dbLastRetryDetail != "HTTP 503" || turn.dbLastTerminalDetail != "HTTP 503" {
		t.Fatalf("expected HTTP 503 fallback on code-only error steps, got hidden=%v count=%d retryDet=%q termDet=%q",
			newHidden, turn.dbRetryCount, turn.dbLastRetryDetail, turn.dbLastTerminalDetail)
	}

	// 3. readMaxSessionAllStepIdx clamps to s.LastSeenDbStepIdx when DB file is unreadable/absent
	sess := NewAcpProjectSession("conv:floor-test", DefaultCwd, "")
	sess.SessionID = "nonexistent-floor-sid"
	sess.LastSeenDbStepIdx = 42
	if got := sess.readMaxSessionAllStepIdx("nonexistent-floor-sid"); got != 42 {
		t.Fatalf("expected readMaxSessionAllStepIdx floor=42 when DB missing, got %d", got)
	}
}
