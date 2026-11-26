package store

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
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
