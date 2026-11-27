package store

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

func TestSQLiteFileURIAndStepTiming(t *testing.T) {
	uri := SQLiteFileURI(filepath.Join(t.TempDir(), "test.db"), "mode=ro")
	if !strings.HasPrefix(uri, "file:/") || !strings.Contains(uri, "mode=ro") {
		t.Fatalf("unexpected SQLite URI: %q", uri)
	}

	ttft, stream, elapsed, e2e, dec := ComputeStepTiming(2100, 1011, 3111, 450)
	if ttft != 2100 || stream != 1011 || elapsed != 3111 || e2e <= 0 || dec <= 0 {
		t.Fatalf("unexpected ComputeStepTiming: (%d,%d,%d,%.1f,%.1f)", ttft, stream, elapsed, e2e, dec)
	}
}

func TestOpenSQLiteImageRejectsTinyFileAndTurnUsageClientTokens(t *testing.T) {
	tiny := filepath.Join(t.TempDir(), "tiny.db")
	if err := os.WriteFile(tiny, []byte("small"), 0600); err != nil {
		t.Fatal(err)
	}
	if img, err := OpenSQLiteImage(tiny); img != nil || err == nil {
		t.Fatalf("expected error on <100B sqlite file, got img=%v err=%v", img, err)
	}

	u := &TurnUsage{
		InputTokens:         500,
		CachedReadTokens:    2000,
		OutputTokens:        40,
		ContextInputTokens:  120,
		ContextCachedTokens: 900,
		Generations:         2,
	}
	inT, crT, outT := u.ClientTokens()
	if inT != 120 || crT != 900 || outT != 40 || u.ContextTotal() != 1020 {
		t.Fatalf("unexpected ClientTokens=(%d,%d,%d) total=%d", inT, crT, outT, u.ContextTotal())
	}
}

func appendTestVarint(dst []byte, v uint64) []byte {
	for v >= 0x80 {
		dst = append(dst, byte(v)|0x80)
		v >>= 7
	}
	return append(dst, byte(v))
}

func buildProtoDuration(sec int64, nanos int32) []byte {
	var b []byte
	if sec > 0 {
		b = appendTestVarint(b, (1<<3)|0)
		b = appendTestVarint(b, uint64(sec))
	}
	if nanos > 0 {
		b = appendTestVarint(b, (2<<3)|0)
		b = appendTestVarint(b, uint64(nanos))
	}
	return b
}

func buildTestGenMetaRow(stepIdx int, servedModel string, ttftSec int64, ttftNanos int32, streamSec int64, streamNanos int32) []byte {
	var usage []byte
	usage = appendTestVarint(usage, (2<<3)|0)
	usage = appendTestVarint(usage, 1000)
	usage = appendTestVarint(usage, (3<<3)|0)
	usage = appendTestVarint(usage, 100)

	var inner []byte
	// field 4 (wire 2): usage submessage
	inner = appendTestVarint(inner, (4<<3)|2)
	inner = appendTestVarint(inner, uint64(len(usage)))
	inner = append(inner, usage...)
	// field 19 (wire 2): served model string
	inner = appendTestVarint(inner, (19<<3)|2)
	inner = appendTestVarint(inner, uint64(len(servedModel)))
	inner = append(inner, servedModel...)
	// field 11 (wire 2): ttft protobuf Duration
	ttftDur := buildProtoDuration(ttftSec, ttftNanos)
	inner = appendTestVarint(inner, (11<<3)|2)
	inner = appendTestVarint(inner, uint64(len(ttftDur)))
	inner = append(inner, ttftDur...)
	// field 12 (wire 2): stream protobuf Duration
	streamDur := buildProtoDuration(streamSec, streamNanos)
	inner = appendTestVarint(inner, (12<<3)|2)
	inner = appendTestVarint(inner, uint64(len(streamDur)))
	inner = append(inner, streamDur...)

	var outer []byte
	// field 1 (wire 2): chat_model submessage
	outer = appendTestVarint(outer, (1<<3)|2)
	outer = appendTestVarint(outer, uint64(len(inner)))
	outer = append(outer, inner...)
	// field 2 (wire 0): step_indices
	outer = appendTestVarint(outer, (2<<3)|0)
	outer = appendTestVarint(outer, uint64(stepIdx))
	return outer
}

func TestReadSessionGenMetaIndexAfterAndIncrementalAttach(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "genmeta.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE gen_metadata (idx INTEGER PRIMARY KEY, data BLOB);`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		stepIdx := (i + 1) * 10
		served := "gemini-4-carbon"
		if i == 5 {
			served = "gemini-4-carbon-exp"
		}
		if _, err := db.Exec(`INSERT INTO gen_metadata (idx, data) VALUES (?, ?)`, i, buildTestGenMetaRow(stepIdx, served, 1, 200_000_000, 0, 800_000_000)); err != nil {
			t.Fatal(err)
		}
	}

	// Verify afterIdx filter excludes rows <= 3.
	byStep, _ := readSessionGenMetaIndex(db, 3)
	if len(byStep) != 2 {
		t.Fatalf("expected 2 rows with idx > 3, got %d (%+v)", len(byStep), byStep)
	}
	if _, hasOld := byStep[10]; hasOld {
		t.Fatalf("did not expect stepIdx=10 in afterIdx=3 result: %+v", byStep)
	}
	if byStep[60].ServedModel != "gemini-4-carbon-exp" {
		t.Fatalf("expected stepIdx=60 servedModel=gemini-4-carbon-exp, got %+v", byStep[60])
	}

	// Verify incremental attach matches newest step even when gen_metadata.idx (5) < step_idx (60).
	steps := []ParsedModelStep{{StepIdx: 60, ModelID: "gemini-4-carbon", OutTok: 100}}
	attachServedModelsIncremental(db, steps, true)
	if steps[0].ServedModel != "gemini-4-carbon-exp" || steps[0].TtftMs != 1200 {
		t.Fatalf("expected incremental attach to match step 60, got %+v", steps[0])
	}
}

func appendTestProtoBytes(dst []byte, fieldNum uint64, payload []byte) []byte {
	dst = appendTestVarint(dst, (fieldNum<<3)|2)
	dst = appendTestVarint(dst, uint64(len(payload)))
	return append(dst, payload...)
}

func appendTestProtoVarintField(dst []byte, fieldNum, val uint64) []byte {
	dst = appendTestVarint(dst, (fieldNum<<3)|0)
	return appendTestVarint(dst, val)
}

func buildTestStep15MetaBlob(tsSec int64, inTok, outTok, crTok uint64) []byte {
	var tsStart, tsFirst, tsEnd []byte
	tsStart = appendTestProtoVarintField(tsStart, 1, uint64(tsSec))
	tsFirst = appendTestProtoVarintField(tsFirst, 1, uint64(tsSec+1))
	tsEnd = appendTestProtoVarintField(tsEnd, 1, uint64(tsSec+2))

	var usage []byte
	usage = appendTestProtoVarintField(usage, 1, 1026) // Gemini model enum
	usage = appendTestProtoVarintField(usage, 2, inTok)
	usage = appendTestProtoVarintField(usage, 3, outTok)
	usage = appendTestProtoVarintField(usage, 5, crTok)

	var meta []byte
	meta = appendTestProtoBytes(meta, 1, tsStart)
	meta = appendTestProtoBytes(meta, 6, tsFirst)
	meta = appendTestProtoBytes(meta, 7, tsEnd)
	meta = appendTestProtoBytes(meta, 9, usage)
	return meta
}

func buildTestStepPayloadBlob(dshBridge bool, padBytes int) []byte {
	var sub []byte
	if dshBridge {
		sub = appendTestProtoBytes(sub, 7, []byte(`{"tool":"mcp_ws-bridge_read_file"}`))
	}
	if padBytes > 0 {
		sub = appendTestProtoBytes(sub, 1, bytes.Repeat([]byte("a"), padBytes))
	}
	return appendTestProtoBytes(nil, 20, sub)
}

func buildTestParentRefBlob(forkIdx int) []byte {
	var b []byte
	b = appendTestProtoBytes(b, 1, []byte("parent-session-id"))
	b = appendTestProtoVarintField(b, 2, uint64(forkIdx))
	return b
}

func createMultiPageStepsFixture(t *testing.T, dbPath string, totalRows int, dshAtIdx int) {
	t.Helper()
	db, err := sql.Open("sqlite", SQLiteFileURI(dbPath, "_pragma=journal_mode(DELETE)&_pragma=page_size(4096)"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`
		CREATE TABLE steps (
			idx INTEGER PRIMARY KEY,
			step_type INTEGER,
			step_status INTEGER,
			view_mask BLOB,
			metadata BLOB,
			err_msg TEXT,
			a BLOB,
			b BLOB,
			c BLOB,
			step_payload BLOB
		);
		CREATE TABLE gen_metadata (
			idx INTEGER PRIMARY KEY,
			data BLOB
		);
	`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	stmtStep, err := tx.Prepare(`INSERT INTO steps (idx, step_type, metadata, step_payload) VALUES (?, ?, ?, ?)`)
	if err != nil {
		t.Fatal(err)
	}
	stmtGen, err := tx.Prepare(`INSERT INTO gen_metadata (idx, data) VALUES (?, ?)`)
	if err != nil {
		t.Fatal(err)
	}
	genIdx := 0
	for i := 1; i <= totalRows; i++ {
		if i%3 == 0 && i != dshAtIdx {
			// Non-model step with ~1.2KB payload to force multi-page B-tree growth
			if _, err := stmtStep.Exec(i, 5, nil, bytes.Repeat([]byte("x"), 1200)); err != nil {
				t.Fatal(err)
			}
			continue
		}
		pad := 900
		if i%25 == 0 {
			// Force multi-page overflow chain (> 4096 bytes) on selected model steps
			pad = 6500
		}
		meta := buildTestStep15MetaBlob(1760000000+int64(i*10), uint64(100+i), uint64(50+i), uint64(1000+i*10))
		payload := buildTestStepPayloadBlob(i == dshAtIdx, pad)
		if _, err := stmtStep.Exec(i, 15, meta, payload); err != nil {
			t.Fatal(err)
		}
		if i%10 == 1 {
			if _, err := stmtGen.Exec(genIdx, buildTestGenMetaRow(i, "gemini-4-neon-served", 1, 100_000_000, 0, 900_000_000)); err != nil {
				t.Fatal(err)
			}
			genIdx++
		}
	}
	_ = stmtStep.Close()
	_ = stmtGen.Close()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteImageReadAtPagesOverflowAndClose(t *testing.T) {
	dir := t.TempDir()
	sid := "00000000-0000-4000-8000-00000000c001"
	dbPath := filepath.Join(dir, sid+".db")
	createMultiPageStepsFixture(t, dbPath, 120, -1)

	img, err := OpenSQLiteImage(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLiteImage failed: %v", err)
	}
	if img.dbFile == nil {
		t.Fatal("expected dbFile handle to be open")
	}
	rootPg := img.findStepsRootPage()
	if rootPg == 0 {
		_ = img.Close()
		t.Fatal("expected non-zero steps root page")
	}
	rootBytes := img.getPage(rootPg)
	if len(rootBytes) != img.pageSize || rootBytes[0] != 0x05 {
		_ = img.Close()
		t.Fatalf("expected multi-page interior 0x05 root page, got len=%d type=0x%02x", len(rootBytes), rootBytes[0])
	}

	// Verify Close() cleanup and idempotency (including nil receiver).
	if err := img.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if img.dbFile != nil || img.walFile != nil || img.walFrames != nil {
		t.Fatal("expected Close() to nil out file handles and walFrames")
	}
	if err := img.Close(); err != nil {
		t.Fatalf("second Close() should be idempotent, got: %v", err)
	}
	var nilImg *SQLiteImage
	if err := nilImg.Close(); err != nil {
		t.Fatalf("nil Close() should return nil, got: %v", err)
	}
	if pg := img.getPage(1); pg != nil {
		t.Fatalf("expected getPage after Close() to return nil, got len=%d", len(pg))
	}
	if img.HasWALPage(1) {
		t.Fatal("expected HasWALPage after Close() to return false")
	}

	// Verify full ReadModelStepsFromSQLiteAfterInternal parses normal + overflow (>6KB) rows + gen_metadata.
	gwSids := map[string]bool{sid: true}
	steps, maxFork, ok := ReadModelStepsFromSQLiteAfterInternal(dbPath, "HOST", gwSids, -1)
	if !ok || maxFork != -1 {
		t.Fatalf("ReadModelStepsFromSQLiteAfterInternal failed: ok=%v maxFork=%d", ok, maxFork)
	}
	// 120 rows minus 40 non-model rows (multiples of 3) = 80 step_type=15 rows.
	if len(steps) != 80 {
		t.Fatalf("expected 80 model steps, got %d", len(steps))
	}
	foundOverflow := false
	foundServed := false
	for _, s := range steps {
		if s.StepIdx == 25 {
			foundOverflow = true
			if s.RawIn != 125 || s.OutTok != 75 {
				t.Fatalf("overflow row idx=25 tokens mismatch: %+v", s)
			}
		}
		if s.StepIdx == 11 && s.ServedModel == "gemini-4-neon-served" && s.TtftMs == 1100 {
			foundServed = true
		}
	}
	if !foundOverflow {
		t.Fatal("expected overflow model step idx=25 to be parsed")
	}
	if !foundServed {
		t.Fatal("expected gen_metadata ServedModel/TtftMs to attach to step idx=11")
	}
}

func TestSQLiteImageWALFrameOverridesAndEdgeCases(t *testing.T) {
	dir := t.TempDir()
	sid := "00000000-0000-4000-8000-00000000c002"
	dbPath := filepath.Join(dir, sid+".db")
	createMultiPageStepsFixture(t, dbPath, 30, -1)

	// 1. Real SQLite WAL writes with autocheckpoint disabled so modified/new pages stay in -wal.
	db, err := sql.Open("sqlite", SQLiteFileURI(dbPath, "_pragma=journal_mode(WAL)&_pragma=wal_autocheckpoint(0)"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 31; i <= 40; i++ {
		meta := buildTestStep15MetaBlob(1760005000+int64(i*10), uint64(200+i), uint64(80+i), uint64(2000+i*10))
		payload := buildTestStepPayloadBlob(false, 1200)
		if _, err := db.Exec(`INSERT INTO steps (idx, step_type, metadata, step_payload) VALUES (?, 15, ?, ?)`, i, meta, payload); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`INSERT INTO gen_metadata (idx, data) VALUES (999, ?)`, buildTestGenMetaRow(40, "gemini-wal-served", 2, 0, 1, 0)); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	// Keep WAL uncheckpointed while reading via OpenSQLiteImage (simulates live Desktop App WAL).
	img, err := OpenSQLiteImage(dbPath)
	if err != nil {
		_ = db.Close()
		t.Fatalf("OpenSQLiteImage with active WAL failed: %v", err)
	}
	if img.walFile == nil || len(img.walFrames) == 0 {
		_ = img.Close()
		_ = db.Close()
		t.Fatalf("expected non-empty WAL frame offset index, walFile=%v frames=%d", img.walFile, len(img.walFrames))
	}
	var someWALPg uint32
	for pgNo := range img.walFrames {
		someWALPg = pgNo
		if !img.HasWALPage(pgNo) {
			_ = img.Close()
			_ = db.Close()
			t.Fatalf("HasWALPage(%d) returned false for indexed WAL page", pgNo)
		}
		break
	}
	_ = img.Close()

	steps, ok := ReadModelStepsFromSQLiteAfter(dbPath, "APP", nil, 29)
	_ = db.Close()
	if !ok {
		t.Fatal("ReadModelStepsFromSQLiteAfter failed on WAL database")
	}
	// Row 30 is step_type=5 (30%3==0), rows 31..40 are all step_type=15 => 10 steps from WAL!
	if len(steps) != 10 {
		t.Fatalf("expected 10 WAL steps (idx 31..40), got %d (%+v)", len(steps), steps)
	}
	if steps[len(steps)-1].StepIdx != 40 || steps[len(steps)-1].ServedModel != "gemini-wal-served" {
		t.Fatalf("expected WAL step 40 with gemini-wal-served, got %+v", steps[len(steps)-1])
	}

	// 2. Synthetic WAL edge cases: multiple committed frames for same page (latest wins),
	//    uncommitted frame ignored, stale-salt frame ignored, committed truncation eviction,
	//    and safe handling of concurrent WAL truncation (no panic / no SIGBUS).
	synthPath := filepath.Join(dir, "00000000-0000-4000-8000-00000000c003.db")
	createMultiPageStepsFixture(t, synthPath, 5, -1)
	baseBytes, err := os.ReadFile(synthPath)
	if err != nil || len(baseBytes) < 4096 {
		t.Fatalf("read synth base db failed: err=%v len=%d", err, len(baseBytes))
	}
	ps := int(binary.BigEndian.Uint16(baseBytes[16:18]))
	if ps == 1 {
		ps = 65536
	}
	saltGood := []byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88}
	saltStale := []byte{0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00}
	walHdr := make([]byte, 32)
	binary.BigEndian.PutUint32(walHdr[0:4], 0x377f0682)
	binary.BigEndian.PutUint32(walHdr[4:8], 3007000)
	binary.BigEndian.PutUint32(walHdr[8:12], uint32(ps))
	copy(walHdr[16:24], saltGood)

	makeFrame := func(pgNo, dbSizeAfter uint32, salt []byte, fill byte) []byte {
		f := make([]byte, 24+ps)
		binary.BigEndian.PutUint32(f[0:4], pgNo)
		binary.BigEndian.PutUint32(f[4:8], dbSizeAfter)
		copy(f[8:16], salt)
		for i := 24; i < len(f); i++ {
			f[i] = fill
		}
		return f
	}

	var walBuf bytes.Buffer
	walBuf.Write(walHdr)
	walBuf.Write(makeFrame(2, 6, saltGood, 0xA1))  // tx1 commits pg 2 (older) and pg 6 (will be truncated)
	walBuf.Write(makeFrame(6, 6, saltGood, 0x66))  // tx1 commits pg 6
	walBuf.Write(makeFrame(2, 4, saltGood, 0xB2))  // tx2 overrides pg 2 with 0xB2 and truncates dbSizeAfter=4 (evicts pg 6)
	walBuf.Write(makeFrame(2, 0, saltGood, 0xC3))  // uncommitted override of pg 2 -> must be ignored
	walBuf.Write(makeFrame(2, 4, saltStale, 0xD4)) // stale salt override of pg 2 -> must stop scan
	if err := os.WriteFile(synthPath+"-wal", walBuf.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}

	sImg, err := OpenSQLiteImage(synthPath)
	if err != nil {
		t.Fatalf("OpenSQLiteImage on synthetic WAL failed: %v", err)
	}
	defer sImg.Close()

	if !sImg.HasWALPage(2) {
		t.Fatal("expected pg 2 to be indexed in WAL")
	}
	if sImg.HasWALPage(6) {
		t.Fatal("expected pg 6 to be evicted by committed truncation dbSizeAfter=4")
	}
	pg2 := sImg.getPage(2)
	if len(pg2) != ps || pg2[0] != 0xB2 || pg2[ps-1] != 0xB2 {
		t.Fatalf("expected latest committed WAL frame (0xB2) for pg 2, got len=%d first=0x%02x", len(pg2), pg2[0])
	}

	// Simulate concurrent WAL truncation by external writer while sImg is open: ReadAt must fail gracefully without SIGBUS.
	if err := os.Truncate(synthPath+"-wal", 0); err != nil {
		t.Fatal(err)
	}
	if pg2AfterTrunc := sImg.getPage(2); pg2AfterTrunc != nil {
		t.Fatalf("expected nil page after concurrent WAL truncation, got len=%d", len(pg2AfterTrunc))
	}
	_ = someWALPg
}

func TestSQLiteImageGuardedMinRowIDPruningParityAndGuard(t *testing.T) {
	dir := t.TempDir()
	sid := "00000000-0000-4000-8000-00000000c004"
	dbPath := filepath.Join(dir, sid+".db")
	// Put a DSH bridge marker ONLY on early step idx=1 across a 160-row multi-page B-tree.
	createMultiPageStepsFixture(t, dbPath, 160, 1)

	// 1. Direct B-tree walk parity & page-read savings proof on interior 0x05 B-tree.
	imgFull, err := OpenSQLiteImage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer imgFull.Close()
	rootPg := imgFull.findStepsRootPage()
	if rootPg == 0 {
		t.Fatal("missing steps root page")
	}
	rootPage := imgFull.getPage(rootPg)
	if len(rootPage) == 0 || rootPage[0] != 0x05 {
		t.Fatalf("expected interior 0x05 root page for 160-row fixture, got 0x%02x", rootPage[0])
	}

	afterThresholds := []int{-1, 0, 1, 25, 80, 120, 155, 160, 500}
	for _, after := range afterThresholds {
		imgA, err := OpenSQLiteImage(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		var fullIDs []int64
		imgA.walkTableBTreeMinRowID(rootPg, 0, 0, func(rowID int64, _ []byte, _ int) {
			if rowID > int64(after) {
				fullIDs = append(fullIDs, rowID)
			}
		})
		fullPages := imgA.pagesRead
		_ = imgA.Close()

		imgB, err := OpenSQLiteImage(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		minRowID := int64(0)
		if after >= 0 {
			minRowID = int64(after) + 1
		}
		var prunedIDs []int64
		imgB.walkTableBTreeMinRowID(rootPg, 0, minRowID, func(rowID int64, _ []byte, _ int) {
			if rowID > int64(after) {
				prunedIDs = append(prunedIDs, rowID)
			}
		})
		prunedPages := imgB.pagesRead
		_ = imgB.Close()

		if !reflect.DeepEqual(fullIDs, prunedIDs) {
			t.Fatalf("after=%d: minRowID pruning mismatch: full=%d rows vs pruned=%d rows", after, len(fullIDs), len(prunedIDs))
		}
		if after >= 120 && prunedPages >= fullPages {
			t.Fatalf("after=%d: expected minRowID pruning to read fewer pages than full scan (pruned=%d, full=%d)", after, prunedPages, fullPages)
		}
	}

	// 2. Verify high-level ReadModelStepsFromSQLiteAfterInternal parity for APP and known-gateway HOST (pruning ON).
	gwSids := map[string]bool{sid: true}
	fullGwSteps, _, okFull := ReadModelStepsFromSQLiteAfterInternal(dbPath, "HOST", gwSids, -1)
	if !okFull {
		t.Fatal("full known-gw read failed")
	}
	for _, after := range []int{0, 40, 120, 159} {
		got, _, ok := ReadModelStepsFromSQLiteAfterInternal(dbPath, "HOST", gwSids, after)
		if !ok {
			t.Fatalf("after=%d known-gw read failed", after)
		}
		var want []ParsedModelStep
		for _, s := range fullGwSteps {
			if s.StepIdx > after {
				want = append(want, s)
			}
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("after=%d known-gw steps mismatch: got %d vs want %d", after, len(got), len(want))
		}
	}

	// 3. Guard test: unknown HOST session (not in gwSids, sourceSide != "APP") with afterIdx=120
	//    MUST NOT prune away idx=1 so the early IsDshBridge marker is still detected and classifies
	//    the returned steps as CLAUDE_GW.
	unclassifiedHostSteps, _, okGuard := ReadModelStepsFromSQLiteAfterInternal(dbPath, "HOST", nil, 120)
	if !okGuard || len(unclassifiedHostSteps) == 0 {
		t.Fatalf("guarded HOST scan failed: ok=%v len=%d", okGuard, len(unclassifiedHostSteps))
	}
	for _, s := range unclassifiedHostSteps {
		if s.SourceCategory != "CLAUDE_GW" {
			t.Fatalf("expected guarded HOST scan to detect early DSH bridge at idx=1 and classify step %d as CLAUDE_GW, got %q", s.StepIdx, s.SourceCategory)
		}
	}

	// 4. APP source with parent_references fork dedup + guarded minRowID pruning.
	db, err := sql.Open("sqlite", SQLiteFileURI(dbPath, "_pragma=journal_mode(DELETE)"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE parent_references (id INTEGER PRIMARY KEY, ref BLOB); INSERT INTO parent_references (id, ref) VALUES (1, ?)`, buildTestParentRefBlob(140)); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	_ = db.Close()

	if forkIdx := ReadMaxForkStepIdxFromSQLite(dbPath); forkIdx != 140 {
		t.Fatalf("expected ReadMaxForkStepIdxFromSQLite=140, got %d", forkIdx)
	}
	appSteps, maxFork, _, _, _, okApp := ReadModelStepsAndGenMetaFromSQLiteAfter(dbPath, "APP", nil, -1)
	if !okApp || maxFork != 140 {
		t.Fatalf("APP forked read failed: ok=%v maxFork=%d", okApp, maxFork)
	}
	if len(appSteps) == 0 {
		t.Fatal("expected post-fork APP steps (> 140)")
	}
	for _, s := range appSteps {
		if s.StepIdx <= 140 {
			t.Fatalf("expected pre-fork step %d (<= 140) to be excluded", s.StepIdx)
		}
		if s.SourceSide != "APP" || s.SourceCategory != "AGY_APP" {
			t.Fatalf("expected APP/AGY_APP classification, got %s/%s", s.SourceSide, s.SourceCategory)
		}
	}
}
