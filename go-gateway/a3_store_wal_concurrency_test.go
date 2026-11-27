package main

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestA3_WALReadWriteSeparationAndCoalescedAsyncUpsert(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "wal_store.db")

	st := &StateStore{}
	st.Init(dbPath)
	defer st.Close()

	if !st.IsReady() {
		t.Fatal("expected StateStore to be ready")
	}
	st.readMu.RLock()
	hasReadDB := st.readDB != nil && st.readDB != st.db
	st.readMu.RUnlock()
	if !hasReadDB {
		t.Fatal("expected StateStore to initialize separate WAL readDB pool")
	}
	var queryOnly int
	if err := st.readDB.QueryRow(`PRAGMA query_only`).Scan(&queryOnly); err != nil || queryOnly != 1 {
		t.Fatalf("expected readDB PRAGMA query_only=1, got %d (err=%v)", queryOnly, err)
	}

	// Seed 5,000 usage_steps across both cold (>8d) and active (<8d) windows.
	nowSec := time.Now().Unix()
	const totalSteps = 5000
	steps := make([]ParsedModelStep, 0, totalSteps)
	for i := 0; i < totalSteps; i++ {
		ts := nowSec - int64(i%12)*86400 - int64(i%3600)
		steps = append(steps, ParsedModelStep{
			SessionID:      fmt.Sprintf("00000000-0000-4000-8000-%012d", i%25),
			StepIdx:        i,
			SourceCategory: "CLAUDE_GW",
			TsSec:          ts,
			ElapsedMs:      50,
			StreamMs:       20,
			ModelEnum:      1500,
			ModelID:        "gemini-4-carbon",
			ModelLabel:     "Gemini 4 Carbon",
			ModelFamily:    "Gemini",
			ModelTier:      "medium",
			QuotaGroup:     "gemini",
			RawIn:          100,
			RawCr:          900,
			AlignedIn:      100,
			AlignedCr:      900,
			OutTok:         40,
			CostUsd:        0.001,
		})
	}
	if n, err := st.persistUsageBatch("seed-wal", cursorMeta{lastStepIdx: totalSteps - 1}, steps); err != nil || n != totalSteps {
		t.Fatalf("seed persistUsageBatch failed: n=%d err=%v", n, err)
	}

	// Verify concurrent 8 readers + 8 writers under -race without SQLITE_BUSY or lock inversion.
	gs := &GatewayServer{sessions: map[string]*AcpProjectSession{}, tenants: map[string]*AcpProjectSession{}}
	var wg sync.WaitGroup
	errCh := make(chan error, 32)

	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for it := 0; it < 6; it++ {
				all, ok := st.QueryAllUsageSteps(nil)
				if !ok || len(all) != totalSteps {
					errCh <- fmt.Errorf("reader %d iter %d: QueryAllUsageSteps ok=%v len=%d", id, it, ok, len(all))
					return
				}
				roll, okR := st.QueryUsageAllTimeRollup(nil)
				if !okR || roll.StepsCount != totalSteps {
					errCh <- fmt.Errorf("reader %d iter %d: rollup ok=%v count=%d", id, it, okR, roll.StepsCount)
					return
				}
				_ = RenderPrometheusMetrics(gs)
			}
		}(r)
	}

	const writers = 8
	const turnsPerWriter = 10
	sessions := make([]*AcpProjectSession, writers)
	for w := 0; w < writers; w++ {
		s := NewAcpProjectSession(fmt.Sprintf("sid:a3-w-%d:u:1", w), "/tmp", "")
		s.HibernatedSessionID = fmt.Sprintf("11111111-0000-4000-8000-%012d", w)
		s.ModelID = "gemini-4-carbon"
		sessions[w] = s
		wg.Add(1)
		go func(idx int, sess *AcpProjectSession) {
			defer wg.Done()
			for tIdx := 1; tIdx <= turnsPerWriter; tIdx++ {
				sess.mu.Lock()
				sess.MessageCount = tIdx * 2
				sess.LastCachedReadTokens = tIdx * 1000
				sess.LastUsedAt = time.Now()
				sess.mu.Unlock()
				st.UpsertSessionAsync(sess)
				st.InsertTurn(sess.ConvKey, TurnHistoryEntry{
					TurnIndex:     tIdx,
					SessionID:     sess.HibernatedSessionID,
					Mode:          "sticky_delta",
					Model:         "gemini-4-carbon",
					ThoughtLevel:  "medium",
					StopReason:    "end_turn",
					ElapsedMs:     100,
					CompletedAtMs: time.Now().UnixMilli(),
					Usage:         &TurnUsage{InputTokens: 50, CachedReadTokens: tIdx * 1000, OutputTokens: 20, UsageSource: "db"},
				})
				if tIdx%4 == 0 {
					st.Flush()
				}
			}
		}(w, s)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	st.Flush()
	for w := 0; w < writers; w++ {
		restored := st.RestoreSingleSession(fmt.Sprintf("sid:a3-w-%d:u:1", w))
		if restored == nil {
			t.Fatalf("expected writer %d session to restore after Flush", w)
		}
		if restored.MessageCount != turnsPerWriter*2 || restored.LastCachedReadTokens != turnsPerWriter*1000 {
			t.Fatalf("writer %d coalesced state mismatch: msgCount=%d cached=%d", w, restored.MessageCount, restored.LastCachedReadTokens)
		}
		if len(restored.TurnHistory) != turnsPerWriter {
			t.Fatalf("writer %d expected %d persisted turns, got %d", w, turnsPerWriter, len(restored.TurnHistory))
		}
	}

	// Verify bounded queue overflow falls back synchronously without losing snapshots.
	for i := 0; i < maxPendingSessionSnapshots+20; i++ {
		s := NewAcpProjectSession(fmt.Sprintf("sid:a3-burst-%d:u:1", i), "/tmp", "")
		s.HibernatedSessionID = fmt.Sprintf("22222222-0000-4000-8000-%012d", i)
		s.MessageCount = 2
		st.UpsertSessionAsync(s)
	}
	st.Flush()
	var convCount int
	if err := st.readDB.QueryRow(`SELECT COUNT(*) FROM conversations`).Scan(&convCount); err != nil {
		t.Fatal(err)
	}
	expectedConvs := writers + maxPendingSessionSnapshots + 20
	if convCount != expectedConvs {
		t.Fatalf("expected %d conversations after bounded burst flush, got %d", expectedConvs, convCount)
	}
}

func TestA3_StoreCloseAndInitStopFlusherCleanly(t *testing.T) {
	dir := t.TempDir()
	st := &StateStore{}
	st.Init(filepath.Join(dir, "a.db"))

	s := NewAcpProjectSession("sid:a3-close:u:1", "/tmp", "")
	s.HibernatedSessionID = "33333333-0000-4000-8000-000000000001"
	s.MessageCount = 4
	st.UpsertSessionAsync(s)

	// Re-init on empty path stops flusher after draining queued snapshots cleanly.
	st.Init("")
	if st.IsReady() {
		t.Fatal("expected store to be not ready after Init(\"\")")
	}
	st.snapMu.Lock()
	started := st.snapStarted
	pending := len(st.pendingSnaps)
	st.snapMu.Unlock()
	if started || pending != 0 {
		t.Fatalf("expected flusher stopped and pending=0, got started=%v pending=%d", started, pending)
	}

	// Verify queued snapshot made it to a.db before closing.
	verify := &StateStore{}
	verify.Init(filepath.Join(dir, "a.db"))
	defer verify.Close()
	if got := verify.RestoreSingleSession("sid:a3-close:u:1"); got == nil || got.MessageCount != 4 {
		t.Fatalf("expected queued async snapshot to flush on Init(\"\")/Close, got %+v", got)
	}
}

func TestA3_SnapSeqStaleSnapshotGuardAndConcurrentClose(t *testing.T) {
	dir := t.TempDir()
	st := &StateStore{}
	st.Init(filepath.Join(dir, "snapseq.db"))

	nowMs := time.Now().UnixMilli()
	newerSnap := SessionSnapshot{
		ConvKey:      "sid:snapseq:u:1",
		SessionID:    "44444444-0000-4000-8000-000000000001",
		TenantKey:    "u",
		Cwd:          "/tmp",
		ModelID:      "gemini-4-carbon",
		ThoughtLevel: "high",
		MsgCount:     12,
		HistoryFp:    "fp-new",
		State:        "idle",
		CreatedAtMs:  nowMs - 1000,
		UpdatedAtMs:  nowMs,
		BlobGen:      2,
		SnapSeq:      50,
	}
	st.UpsertSessionSnapshot(newerSnap)

	staleSnap := newerSnap
	staleSnap.MsgCount = 2
	staleSnap.HistoryFp = "fp-stale"
	staleSnap.UpdatedAtMs = nowMs - 500
	staleSnap.SnapSeq = 40

	// Stale SnapSeq must not overwrite conversations via writeSnapshotBatch or InsertTurnWithSnapshot,
	// while InsertTurnWithSnapshot must still persist the turn log entry.
	st.UpsertSessionSnapshot(staleSnap)
	st.InsertTurnWithSnapshot(staleSnap, true, staleSnap.ConvKey, TurnHistoryEntry{
		TurnIndex:     1,
		SessionID:     staleSnap.SessionID,
		Mode:          "sticky_delta",
		Model:         "gemini-4-carbon",
		ThoughtLevel:  "high",
		StopReason:    "end_turn",
		CompletedAtMs: 2100,
		Usage:         &TurnUsage{InputTokens: 20, CachedReadTokens: 100, OutputTokens: 10, UsageSource: "db"},
	})

	restored := st.RestoreSingleSession(newerSnap.ConvKey)
	if restored == nil {
		t.Fatal("expected restored session")
	}
	if restored.MessageCount != 12 || restored.HistoryFingerprint != "fp-new" {
		t.Fatalf("stale snapshot overwrote newer state: msgCount=%d histFp=%q", restored.MessageCount, restored.HistoryFingerprint)
	}
	if len(restored.TurnHistory) != 1 {
		t.Fatalf("expected turn entry to persist even when paired snapshot is stale, got %d turns", len(restored.TurnHistory))
	}

	// Hammer UpsertSessionSnapshotAsync concurrently across queue overflow while Close() runs under -race.
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 80; i++ {
				st.UpsertSessionSnapshotAsync(SessionSnapshot{
					ConvKey:      fmt.Sprintf("sid:race-%d-%d:u:1", worker, i),
					SessionID:    fmt.Sprintf("55555555-0000-4000-8000-%06d%06d", worker, i),
					TenantKey:    "u",
					Cwd:          "/tmp",
					ModelID:      "gemini-4-carbon",
					ThoughtLevel: "medium",
					MsgCount:     i + 1,
					HistoryFp:    "fp-race",
					State:        "idle",
					CreatedAtMs:  1000,
					UpdatedAtMs:  2000,
					BlobGen:      1,
					SnapSeq:      uint64(i + 1),
				})
			}
		}(g)
	}
	time.Sleep(2 * time.Millisecond)
	st.Close()
	wg.Wait()
}
