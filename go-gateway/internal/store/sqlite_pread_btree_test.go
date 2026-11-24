package store

import (
	"bytes"
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

func testProtoVarint(dst []byte, v uint64) []byte {
	for v >= 0x80 {
		dst = append(dst, byte(v)|0x80)
		v >>= 7
	}
	return append(dst, byte(v))
}

func testProtoBytes(dst []byte, fn uint64, b []byte) []byte {
	dst = testProtoVarint(dst, (fn<<3)|2)
	dst = testProtoVarint(dst, uint64(len(b)))
	return append(dst, b...)
}

func testProtoVarintField(dst []byte, fn, val uint64) []byte {
	dst = testProtoVarint(dst, (fn<<3)|0)
	return testProtoVarint(dst, val)
}

func buildTestStep15Meta(tsSec int64, inTok, crTok, outTok int, withDshBridge bool) ([]byte, []byte) {
	var usage []byte
	usage = testProtoVarintField(usage, 1, 1319)
	usage = testProtoVarintField(usage, 2, uint64(inTok))
	usage = testProtoVarintField(usage, 3, uint64(outTok))
	usage = testProtoVarintField(usage, 5, uint64(crTok))
	var ts []byte
	ts = testProtoVarintField(ts, 1, uint64(tsSec))
	var meta []byte
	meta = testProtoBytes(meta, 1, ts)
	meta = testProtoBytes(meta, 9, usage)
	var payload []byte
	if withDshBridge {
		toolSub := testProtoBytes(nil, 7, []byte(`{"name":"mcp_ws-bridge_ws_read_3316348dbadf"}`))
		payload = testProtoBytes(nil, 20, toolSub)
	} else {
		payload = bytes.Repeat([]byte("x"), 6000) // Force multi-page B-Tree + overflow pages
	}
	return meta, payload
}

func TestSQLiteImageOnDemandPreadAndBTreeRowIDPruning(t *testing.T) {
	dir := t.TempDir()
	sid := "00000000-0000-4000-8000-00000000beef"
	dbPath := filepath.Join(dir, sid+".db")
	db, err := sql.Open("sqlite", SQLiteFileURI(dbPath, "_pragma=journal_mode(WAL)"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE steps (idx INTEGER PRIMARY KEY, step_type INTEGER, step_status INTEGER, view_mask BLOB, metadata BLOB, err_msg TEXT, a BLOB, b BLOB, c BLOB, step_payload BLOB)`); err != nil {
		t.Fatalf("create steps: %v", err)
	}
	baseTs := time.Now().Unix() - 1000
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	const totalSteps = 180
	for i := 1; i <= totalSteps; i++ {
		meta, payload := buildTestStep15Meta(baseTs+int64(i), 400+i, 1200+i, 30+i, false)
		if _, err := tx.Exec(`INSERT INTO steps (idx, step_type, metadata, step_payload) VALUES (?, 15, ?, ?)`, i, meta, payload); err != nil {
			t.Fatalf("insert step %d: %v", i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	// Force partial checkpoint + append one new live WAL step so both .db and .db-wal hold committed pages.
	_, _ = db.Exec(`PRAGMA wal_checkpoint(PASSIVE)`)
	metaLast, payloadLast := buildTestStep15Meta(baseTs+999, 999, 2999, 88, false)
	if _, err := db.Exec(`INSERT INTO steps (idx, step_type, metadata, step_payload) VALUES (?, 15, ?, ?)`, totalSteps+1, metaLast, payloadLast); err != nil {
		t.Fatalf("insert wal tail step: %v", err)
	}

	gwMap := map[string]bool{sid: true}
	fullSteps := ReadModelStepsFromSQLite(dbPath, "HOST", gwMap)
	if len(fullSteps) != totalSteps+1 {
		t.Fatalf("expected %d full steps, got %d", totalSteps+1, len(fullSteps))
	}

	incSteps, ok := ReadModelStepsFromSQLiteAfter(dbPath, "HOST", gwMap, totalSteps-2)
	if !ok || len(incSteps) != 3 {
		t.Fatalf("expected 3 incremental steps, got ok=%v len=%d", ok, len(incSteps))
	}
	for k := 0; k < 3; k++ {
		want := fullSteps[len(fullSteps)-3+k]
		got := incSteps[k]
		if got.StepIdx != want.StepIdx || got.RawIn != want.RawIn || got.RawCr != want.RawCr || got.OutTok != want.OutTok || got.SourceCategory != want.SourceCategory {
			t.Fatalf("incremental step[%d] mismatch: got=%+v want=%+v", k, got, want)
		}
	}

	// Verify B-Tree rowid pruning actually skips historical leaf/overflow pages on incremental read!
	img, err := OpenSQLiteImage(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLiteImage: %v", err)
	}
	defer img.Close()
	if img.CachedPageCount() != 0 {
		t.Fatalf("expected 0 pages loaded immediately after OpenSQLiteImage, got %d", img.CachedPageCount())
	}
	rootPg, hasIntPK := img.findTableInfo("steps")
	if rootPg == 0 || !hasIntPK {
		t.Fatalf("expected steps rootPg>0 and hasIntPK=true, got root=%d pk=%v", rootPg, hasIntPK)
	}
	visitedRows := 0
	img.walkTableBTreeAfter(rootPg, 0, int64(totalSteps), true, func(rowID int64, _ []byte, _ int) {
		visitedRows++
		if rowID <= int64(totalSteps) {
			t.Fatalf("pruned walk visited stale rowID %d <= %d", rowID, totalSteps)
		}
	})
	if visitedRows != 1 {
		t.Fatalf("expected pruned walk to visit exactly 1 tail row, got %d", visitedRows)
	}
	// 181 rows * 6KB > 270 pages total, whereas pruned walk must read <= 12 pages!
	if img.CachedPageCount() > 12 {
		t.Fatalf("expected on-demand pruned walk to cache <= 12 pages, got %d", img.CachedPageCount())
	}
}

func TestSQLiteImagePreservesUnknownSessionDshBridgeClassificationAcrossIncrementalSync(t *testing.T) {
	dir := t.TempDir()
	sid := "00000000-0000-4000-8000-00000000cafe"
	dbPath := filepath.Join(dir, sid+".db")
	db, err := sql.Open("sqlite", SQLiteFileURI(dbPath, "_pragma=journal_mode(WAL)"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE steps (idx INTEGER PRIMARY KEY, step_type INTEGER, step_status INTEGER, view_mask BLOB, metadata BLOB, err_msg TEXT, a BLOB, b BLOB, c BLOB, step_payload BLOB)`); err != nil {
		t.Fatalf("create steps: %v", err)
	}
	now := time.Now().Unix()
	m1, p1 := buildTestStep15Meta(now-10, 100, 200, 10, true) // step 1 has IsDshBridge=true
	m2, p2 := buildTestStep15Meta(now-5, 150, 300, 20, false) // step 2 has no bridge string
	if _, err := db.Exec(`INSERT INTO steps (idx, step_type, metadata, step_payload) VALUES (1, 15, ?, ?), (2, 15, ?, ?)`, m1, p1, m2, p2); err != nil {
		t.Fatalf("insert steps: %v", err)
	}

	// Unknown session (gwSids empty) read incrementally afterIdx=1 MUST still see step 1's IsDshBridge
	// and classify step 2 as CLAUDE_GW rather than AGY_CLI!
	inc, ok := ReadModelStepsFromSQLiteAfter(dbPath, "HOST", nil, 1)
	if !ok || len(inc) != 1 || inc[0].StepIdx != 2 || inc[0].SourceCategory != "CLAUDE_GW" {
		t.Fatalf("expected step 2 classified as CLAUDE_GW from step 1 bridge marker, got ok=%v inc=%+v", ok, inc)
	}
}
