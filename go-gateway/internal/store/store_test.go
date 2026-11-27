package store

import (
	"database/sql"
	"os"
	"path/filepath"
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
