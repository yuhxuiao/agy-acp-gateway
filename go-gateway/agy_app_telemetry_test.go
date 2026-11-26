package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode"
)

func writeRealSchemaAppStepDB(t *testing.T, dbPath string, tsSec int64, modelEnum int, servedModel string, inTok, crTok, outTok, thTok int, walMode bool) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		t.Fatal(err)
	}
	dsn := sqliteFileURI(dbPath, "_pragma=busy_timeout(2000)")
	if walMode {
		dsn = sqliteFileURI(dbPath, "_pragma=journal_mode(WAL)&_pragma=busy_timeout(2000)")
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	schema := `
CREATE TABLE IF NOT EXISTS steps (
	idx INTEGER PRIMARY KEY,
	step_type INTEGER,
	status INTEGER,
	has_subtrajectory NUMERIC,
	metadata BLOB,
	error_details BLOB,
	permissions BLOB,
	task_details BLOB,
	render_info BLOB,
	step_payload BLOB,
	step_format INTEGER
);
CREATE TABLE IF NOT EXISTS gen_metadata (
	idx INTEGER PRIMARY KEY,
	data BLOB,
	size INTEGER
);
`
	if _, err := db.Exec(schema); err != nil {
		t.Fatal(err)
	}

	tsStart := append(protoUint(1, uint64(tsSec)), protoUint(2, 0)...)
	tsFirst := append(protoUint(1, uint64(tsSec+1)), protoUint(2, 200_000_000)...)
	tsEnd := append(protoUint(1, uint64(tsSec+2)), protoUint(2, 400_000_000)...)
	usage := append(protoUint(1, uint64(modelEnum)), protoUint(2, uint64(inTok))...)
	usage = append(usage, protoUint(3, uint64(outTok))...)
	usage = append(usage, protoUint(5, uint64(crTok))...)
	if thTok > 0 {
		usage = append(usage, protoUint(9, uint64(thTok))...)
	}
	meta := append(protoBytes(1, tsStart), protoBytes(6, tsFirst)...)
	meta = append(meta, protoBytes(7, tsEnd)...)
	meta = append(meta, protoBytes(9, usage)...)

	plannerSub := protoBytes(1, []byte("synthetic assistant text"))
	stepPayload := protoBytes(20, plannerSub)

	if _, err := db.Exec(
		`INSERT OR REPLACE INTO steps VALUES (?, 15, 3, 0, ?, NULL, NULL, NULL, NULL, ?, 1)`,
		2, meta, stepPayload,
	); err != nil {
		t.Fatal(err)
	}

	if servedModel != "" {
		gmBlob := buildTestGenMetadataBlobServed(servedModel, modelEnum, inTok, outTok, crTok, thTok, outTok-thTok, inTok+crTok, 1048576, 1.2, 1.2)
		// Attach step_indices = [2] at top-level field 2 so step #2 matches directly.
		gmBlob = append(gmBlob, protoUint(2, 2)...)
		if _, err := db.Exec(`INSERT OR REPLACE INTO gen_metadata VALUES (0, ?, ?)`, gmBlob, len(gmBlob)); err != nil {
			t.Fatal(err)
		}
	}
}

func buildTestLegacyTrajectoryPB(tsSec int64, modelEnum, inTok, crTok, outTok int) []byte {
	tsStart := protoUint(1, uint64(tsSec))
	tsFirst := protoUint(1, uint64(tsSec+1))
	tsEnd := protoUint(1, uint64(tsSec+2))
	usage := append(protoUint(1, uint64(modelEnum)), protoUint(2, uint64(inTok))...)
	usage = append(usage, protoUint(3, uint64(outTok))...)
	usage = append(usage, protoUint(5, uint64(crTok))...)
	meta := append(protoBytes(1, tsStart), protoBytes(6, tsFirst)...)
	meta = append(meta, protoBytes(7, tsEnd)...)
	meta = append(meta, protoBytes(9, usage)...)

	// Step 0: non-planner user step (step_type = 14)
	userStep := protoUint(1, 14)
	// Step 1: planner model step (step_type = 15, metadata at field 4, planner_response at field 20)
	modelStep := append(protoUint(1, 15), protoBytes(4, meta)...)
	modelStep = append(modelStep, protoBytes(20, protoBytes(1, []byte("legacy pb reply")))...)

	traj := append(protoBytes(2, userStep), protoBytes(2, modelStep)...)
	return traj
}

func TestAppConversationsDirDiscoveryAndHermeticIsolation(t *testing.T) {
	tmp := t.TempDir()
	geminiRoot := filepath.Join(tmp, ".gemini")
	cliAppData := filepath.Join(geminiRoot, "antigravity-cli")
	appConvs := filepath.Join(geminiRoot, "antigravity", "conversations")
	pureHome := filepath.Join(tmp, "pure")
	if err := os.MkdirAll(cliAppData, 0o755); err != nil {
		t.Fatal(err)
	}

	t.Setenv("AGY_PURE_API_HOME", pureHome)
	t.Setenv("AGY_CONV_DIR", "")
	t.Setenv("AGY_APP_CONV_DIR", "")
	t.Setenv("AGY_APPDATA_LINK", cliAppData)
	PureApiHome = pureHome

	// 1. Missing ~/.gemini/antigravity/conversations directory -> returns ""
	if got := AppConversationsDir(); got != "" {
		t.Fatalf("expected empty AppConversationsDir when directory does not exist, got %q", got)
	}

	// 2. Once sibling antigravity/conversations exists and AGY_APPDATA_LINK is set -> auto-discovered!
	if err := os.MkdirAll(appConvs, 0o755); err != nil {
		t.Fatal(err)
	}
	if got := AppConversationsDir(); got != appConvs {
		t.Fatalf("expected auto-discovered %q, got %q", appConvs, got)
	}

	// 3. Hermetic isolation: empty AGY_APPDATA_LINK (go test, non-18890 gw.sh, agy_upgrade) -> ""
	t.Setenv("AGY_APPDATA_LINK", "")
	if got := AppConversationsDir(); got != "" {
		t.Fatalf("expected empty AppConversationsDir when AGY_APPDATA_LINK is empty, got %q", got)
	}

	// 4. Hermetic isolation: custom AGY_CONV_DIR (fake_instance.py, unit tests) -> ""
	t.Setenv("AGY_APPDATA_LINK", cliAppData)
	t.Setenv("AGY_CONV_DIR", filepath.Join(tmp, "custom-convs"))
	if got := AppConversationsDir(); got != "" {
		t.Fatalf("expected empty AppConversationsDir when AGY_CONV_DIR is set, got %q", got)
	}
	t.Setenv("AGY_CONV_DIR", "")

	// 5. Explicit opt-out values disable discovery even when AGY_APPDATA_LINK is set
	for _, offVal := range []string{"off", "OFF", "none", "0", "false", "no"} {
		t.Setenv("AGY_APP_CONV_DIR", offVal)
		if got := AppConversationsDir(); got != "" {
			t.Fatalf("expected AGY_APP_CONV_DIR=%q to disable discovery, got %q", offVal, got)
		}
	}

	// 6. Explicit path override works even when AGY_APPDATA_LINK is empty or AGY_CONV_DIR is set
	explicitAppDir := filepath.Join(tmp, "mounted-app-convs")
	if err := os.MkdirAll(explicitAppDir, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGY_APPDATA_LINK", "")
	t.Setenv("AGY_CONV_DIR", filepath.Join(tmp, "custom-convs"))
	t.Setenv("AGY_APP_CONV_DIR", explicitAppDir)
	if got := AppConversationsDir(); got != explicitAppDir {
		t.Fatalf("expected explicit AGY_APP_CONV_DIR=%q, got %q", explicitAppDir, got)
	}

	// 7. Same-dir collision with ConversationsDir() and Windows C-drive paths are rejected
	sameConv := ConversationsDir()
	if err := os.MkdirAll(sameConv, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGY_APP_CONV_DIR", sameConv)
	if got := AppConversationsDir(); got != "" {
		t.Fatalf("expected same directory as ConversationsDir() to return empty, got %q", got)
	}
	t.Setenv("AGY_APP_CONV_DIR", "/mnt/c/Users/Public/antigravity/conversations")
	if got := AppConversationsDir(); got != "" {
		t.Fatalf("expected /mnt/c path to be rejected, got %q", got)
	}
}

func TestAgyAppVsCliVsGatewayAttributionAndQuotaAggregation(t *testing.T) {
	prevDB := GatewayStateDB
	t.Cleanup(func() {
		initConfig()
		InitGlobalStore(prevDB)
	})

	tmp := t.TempDir()
	convDir := filepath.Join(tmp, "cli-convs")
	appDir := filepath.Join(tmp, "app-convs")
	stateDB := filepath.Join(tmp, "gateway.db")
	if err := os.MkdirAll(convDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(appDir, 0o755); err != nil {
		t.Fatal(err)
	}

	t.Setenv("AGY_CONV_DIR", convDir)
	t.Setenv("AGY_APP_CONV_DIR", appDir)
	t.Setenv("AGY_STATE_DB", stateDB)
	t.Setenv("AGY_PURE_API_HOME", filepath.Join(tmp, "pure"))
	t.Setenv("AGY_GW_LOG", filepath.Join(tmp, "gateway.log"))
	t.Setenv("AGY_QUOTA_BIN", filepath.Join(tmp, "nonexistent-agy"))
	initConfig()
	InitGlobalStore(stateDB)

	nowSec := time.Now().Unix() - 120
	sidGw := "aaaa1111-1111-4000-8000-000000000001"
	sidCli := "bbbb2222-2222-4000-8000-000000000002"
	sidAppDb := "cccc3333-3333-4000-8000-000000000003"
	sidAppPb := "dddd4444-4444-4000-8000-000000000004"
	sidEncPb := "eeee5555-5555-4000-8000-000000000005"

	// 1. Gateway-owned session in convDir (Claude Code client)
	writeRealSchemaAppStepDB(t, filepath.Join(convDir, sidGw+".db"), nowSec-30, 1427, "gemini-4-carbon", 10000, 40000, 500, 100, false)
	sessGw := NewAcpProjectSession("sid:gw-1", tmp, "")
	sessGw.SessionID = sidGw
	sessGw.MessageCount = 2
	sessGw.ClientID = "claude_code"
	sessGw.ClientLabel = "Claude Code"
	sessGw.Protocol = "Messages"
	GlobalStore.UpsertSession(sessGw)

	// 2. Standalone AGY CLI session in convDir (not in gwSids)
	writeRealSchemaAppStepDB(t, filepath.Join(convDir, sidCli+".db"), nowSec-20, 1437, "gemini-4-carbon", 8000, 12000, 300, 50, false)

	// 3. Antigravity Desktop App .db session in appDir (+ shadowed .pb with same UUID that must be ignored)
	writeRealSchemaAppStepDB(t, filepath.Join(appDir, sidAppDb+".db"), nowSec-10, 1427, "gemini-4-carbon-canary", 15000, 35000, 600, 200, false)
	if err := os.WriteFile(filepath.Join(appDir, sidAppDb+".pb"), buildTestLegacyTrajectoryPB(nowSec-10, 1427, 99999, 0, 999), 0o600); err != nil {
		t.Fatal(err)
	}

	// 4. Unencrypted legacy .pb session in appDir (no .db counterpart)
	if err := os.WriteFile(filepath.Join(appDir, sidAppPb+".pb"), buildTestLegacyTrajectoryPB(nowSec-5, 1528, 6000, 4000, 250), 0o600); err != nil {
		t.Fatal(err)
	}

	// 5. Encrypted/opaque macOS Keychain .pb blob in appDir (must not fail or spam errors; cursor advances)
	opaqueBlob := []byte{0x4d, 0xda, 0x89, 0x9d, 0x66, 0x53, 0x51, 0x7e, 0xff, 0x00, 0xab, 0xcd}
	if err := os.WriteFile(filepath.Join(appDir, sidEncPb+".pb"), opaqueBlob, 0o600); err != nil {
		t.Fatal(err)
	}

	// Seed a known official Gemini 5h/weekly quota snapshot so deduction math is verified too
	quotaMu.Lock()
	savedQ, savedQAt, savedQErr, savedQAttempt, savedQBackoff := cachedQuota, cachedQuotaAt, lastQuotaErr, lastQuotaAttemptAt, quotaBackoffSteps
	cachedQuota = &QuotaSnapshot{
		Status:    "ok",
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
		Groups: []QuotaGroup{
			{
				Name: "Gemini models",
				Buckets: []QuotaBucket{
					{ID: "gem-5h", Window: "5h", RemainingFraction: 0.85, UsedFraction: 0.15, ResetTime: time.Now().Add(4 * time.Hour).UTC().Format(time.RFC3339)},
					{ID: "gem-wk", Window: "weekly", RemainingFraction: 0.90, UsedFraction: 0.10, ResetTime: time.Now().Add(6 * 24 * time.Hour).UTC().Format(time.RFC3339)},
				},
			},
		},
	}
	cachedQuotaAt = time.Now()
	quotaMu.Unlock()
	t.Cleanup(func() {
		waitQuotaRefreshIdle(t)
		quotaMu.Lock()
		cachedQuota, cachedQuotaAt, lastQuotaErr, lastQuotaAttemptAt, quotaBackoffSteps = savedQ, savedQAt, savedQErr, savedQAttempt, savedQBackoff
		quotaMu.Unlock()
	})

	gs := NewGatewayServer()
	defer gs.Close()

	payload := BuildDashboardPayload(gs, false)
	recent, ok := payload["recentDbSteps"].([]map[string]any)
	if !ok || len(recent) != 4 {
		t.Fatalf("expected exactly 4 recentDbSteps (gw + cli + app.db + app.pb, excluding shadowed .pb and encrypted .pb), got %d: %+v", len(recent), recent)
	}

	bySid := map[string]map[string]any{}
	for _, r := range recent {
		bySid[r["sessionId"].(string)] = r
	}

	if r := bySid[sidGw]; r["sourceCategory"] != "CLAUDE_GW" || r["clientId"] != "claude_code" || r["clientLabel"] != "Claude Code" {
		t.Fatalf("unexpected gateway step attribution: %+v", r)
	}
	if r := bySid[sidCli]; r["sourceSide"] != "HOST" || r["sourceCategory"] != "AGY_CLI" || r["sourceLabel"] != "AGY CLI" || r["clientId"] != "agy_host" || r["clientLabel"] != "AGY CLI" || r["protocol"] != "ACP" {
		t.Fatalf("unexpected AGY CLI step attribution: %+v", r)
	}
	if r := bySid[sidAppDb]; r["sourceSide"] != "APP" || r["sourceCategory"] != "AGY_APP" || r["sourceLabel"] != "Antigravity App" || r["clientId"] != "agy_app" || r["clientLabel"] != "AGY App" || r["protocol"] != "App" || r["servedModel"] != "gemini-4-carbon-canary" || r["servedMismatch"] != true {
		t.Fatalf("unexpected AGY App .db step attribution: %+v", r)
	}
	if r := bySid[sidAppPb]; r["sourceSide"] != "APP" || r["sourceCategory"] != "AGY_APP" || r["sourceLabel"] != "Antigravity App" || r["clientId"] != "agy_app" || r["clientLabel"] != "AGY App" || r["protocol"] != "App" || r["modelId"] != "gemini-4-hydrogen" {
		t.Fatalf("unexpected AGY App .pb step attribution: %+v", r)
	}

	// Verify rolling5h, allTime, and quota.deduction aggregate all 4 steps including Antigravity App!
	windows := payload["windows"].(map[string]WindowAggregate)
	if windows["rolling5h"].StepsCount != 4 || windows["allTime"].StepsCount != 4 {
		t.Fatalf("expected rolling5h & allTime StepsCount=4, got rolling5h=%d allTime=%d", windows["rolling5h"].StepsCount, windows["allTime"].StepsCount)
	}
	qSnap := payload["quota"].(QuotaSnapshot)
	if qSnap.Deduction == nil || qSnap.Deduction.LocalSteps5h != 4 || qSnap.Deduction.LocalTotal5hUsd <= 0 {
		t.Fatalf("expected QuotaDeduction LocalSteps5h=4 with positive USD cost, got %+v", qSnap.Deduction)
	}

	// Verify encrypted .pb cursor was persisted so a second sync >25ms later performs 0 re-ingestions
	time.Sleep(30 * time.Millisecond)
	if n := GlobalStore.SyncUsageStepsFromSourceDir(appDir, "APP", "AGY_APP", "Antigravity App", getKnownGatewaySids()); n != 0 {
		t.Fatalf("expected 0 re-ingested steps on unchanged appDir, got %d", n)
	}

	// Verify toggling AGY_APP_CONV_DIR=off invalidates dashboard cache and excludes previously persisted AGY_APP rows
	t.Setenv("AGY_APP_CONV_DIR", "off")
	offPayload := BuildDashboardPayload(gs, false)
	offRecent := offPayload["recentDbSteps"].([]map[string]any)
	if len(offRecent) != 2 {
		t.Fatalf("expected 2 non-App steps when AGY_APP_CONV_DIR=off, got %d: %+v", len(offRecent), offRecent)
	}
	if roll, ok := GlobalStore.QueryUsageAllTimeRollup(getKnownGatewaySids()); !ok || roll.StepsCount != 2 {
		t.Fatalf("expected all-time rollup StepsCount=2 when AGY_APP_CONV_DIR=off, got ok=%v roll=%+v", ok, roll)
	}
	t.Setenv("AGY_APP_CONV_DIR", appDir)

	// Verify fallback mode (!GlobalStore.IsReady()) also merges convDir + appDir with identical attribution
	GlobalStore.Init("")
	if GlobalStore.IsReady() {
		t.Fatal("expected GlobalStore to be unready after Init(\"\")")
	}
	fallbackPayload := BuildDashboardPayload(gs, false)
	fbRecent := fallbackPayload["recentDbSteps"].([]map[string]any)
	if len(fbRecent) != 4 {
		t.Fatalf("expected 4 steps in !GlobalStore.IsReady() fallback mode, got %d", len(fbRecent))
	}
	fbBySid := map[string]map[string]any{}
	for _, r := range fbRecent {
		fbBySid[r["sessionId"].(string)] = r
	}
	if r := fbBySid[sidAppDb]; r["sourceCategory"] != "AGY_APP" || r["clientId"] != "agy_app" || r["clientLabel"] != "AGY App" {
		t.Fatalf("fallback mode lost AGY_APP attribution: %+v", r)
	}
	if r := fbBySid[sidCli]; r["sourceCategory"] != "AGY_CLI" || r["clientId"] != "agy_host" {
		t.Fatalf("fallback mode lost AGY_CLI attribution: %+v", r)
	}
}

func TestReadOnlyWALFallbackAndZeroMutationOfAppConversationsDir(t *testing.T) {
	tmp := t.TempDir()
	appDir := filepath.Join(tmp, "ro-app-convs")
	stateDB := filepath.Join(tmp, "state.db")
	sid := "ffff6666-6666-4000-8000-000000000006"
	dbPath := filepath.Join(appDir, sid+".db")

	writeRealSchemaAppStepDB(t, dbPath, time.Now().Unix()-60, 1427, "gemini-4-carbon", 12000, 28000, 450, 120, true)

	snapshotDir := func(dir string) map[string]string {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		out := make(map[string]string, len(entries))
		for _, e := range entries {
			p := filepath.Join(dir, e.Name())
			b, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256(b)
			out[e.Name()] = hex.EncodeToString(sum[:])
		}
		return out
	}

	before := snapshotDir(appDir)
	// Lock down files and directory to read-only (0444 / 0555) to simulate a strict :ro bind mount.
	entries, err := os.ReadDir(appDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		_ = os.Chmod(filepath.Join(appDir, e.Name()), 0o444)
	}
	_ = os.Chmod(appDir, 0o555)
	t.Cleanup(func() {
		_ = os.Chmod(appDir, 0o755)
		for _, e := range entries {
			_ = os.Chmod(filepath.Join(appDir, e.Name()), 0o644)
		}
	})

	st := &StateStore{}
	st.Init(stateDB)
	defer func() {
		st.mu.Lock()
		if st.db != nil {
			_ = st.db.Close()
		}
		st.mu.Unlock()
	}()

	ingested := st.SyncUsageStepsFromSourceDir(appDir, "APP", "AGY_APP", "Antigravity App", nil)
	if ingested != 1 {
		t.Fatalf("expected 1 step ingested from read-only App directory, got %d", ingested)
	}
	steps, ok := st.QueryAllUsageSteps(nil)
	if !ok || len(steps) != 1 || steps[0].SourceCategory != "AGY_APP" || steps[0].ServedModel != "gemini-4-carbon" {
		t.Fatalf("unexpected ingested steps from read-only App directory: ok=%v steps=%+v", ok, steps)
	}

	after := snapshotDir(appDir)
	if len(before) != len(after) {
		t.Fatalf("file count in read-only App directory changed: before=%v after=%v", before, after)
	}
	for name, digest := range before {
		if after[name] != digest {
			t.Fatalf("file %s in AppConversationsDir was mutated!", name)
		}
	}
}

func TestWritableAppConversationsDirZeroSideFilesAndUnchangedDirMtime(t *testing.T) {
	tmp := t.TempDir()
	// Writable 0755 directory matching real ~/.gemini/antigravity/conversations permissions
	appDir := filepath.Join(tmp, "writable-app-convs")
	if err := os.MkdirAll(appDir, 0o755); err != nil {
		t.Fatal(err)
	}
	stateDB := filepath.Join(tmp, "state.db")
	sid := "77778888-9999-4000-8000-000000000007"
	dbPath := filepath.Join(appDir, sid+".db")
	tinyPath := filepath.Join(appDir, "tiny.db")

	// Build a cleanly closed journal_mode=WAL database whose steps.idx is a non-rowid INTEGER column
	// (exercising SQLite zero-byte integer serial types 8 -> 0 and 9 -> 1, plus serial type 1 -> 2,
	// a view/index named before/alongside steps, and an empty-servedModel step triggering backfillEmptyServedModels).
	func() {
		dsn := sqliteFileURI(dbPath, "_pragma=journal_mode(WAL)&_pragma=busy_timeout(2000)")
		db, err := sql.Open("sqlite", dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		schema := `
CREATE TABLE dummy_anchor (id INTEGER);
CREATE INDEX steps ON dummy_anchor(id);
DROP INDEX steps;
CREATE TABLE steps (
	idx INTEGER,
	step_type INTEGER,
	status INTEGER,
	has_subtrajectory NUMERIC,
	metadata BLOB,
	error_details BLOB,
	permissions BLOB,
	task_details BLOB,
	render_info BLOB,
	step_payload BLOB,
	step_format INTEGER
);
CREATE VIEW steps_view AS SELECT * FROM steps;
CREATE TABLE gen_metadata (
	idx INTEGER PRIMARY KEY,
	data BLOB,
	size INTEGER
);
`
		if _, err := db.Exec(schema); err != nil {
			t.Fatal(err)
		}
		nowSec := time.Now().Unix() - 90
		makeMeta := func(ts int64, inTok, crTok, outTok int) []byte {
			usage := append(protoUint(1, 1427), protoUint(2, uint64(inTok))...)
			usage = append(usage, protoUint(3, uint64(outTok))...)
			usage = append(usage, protoUint(5, uint64(crTok))...)
			m := append(protoBytes(1, protoUint(1, uint64(ts))), protoBytes(6, protoUint(1, uint64(ts+1)))...)
			m = append(m, protoBytes(7, protoUint(1, uint64(ts+2)))...)
			m = append(m, protoBytes(9, usage)...)
			return m
		}
		payload := protoBytes(20, protoBytes(1, []byte("ok")))
		// idx = 0 (serial type 8), idx = 1 (serial type 9), idx = 2 (serial type 1)
		// idx = 0 intentionally has no gen_metadata match -> served_model == "" so backfillEmptyServedModels runs!
		for _, row := range []struct {
			idx, inTok, crTok, outTok int
		}{
			{0, 10000, 0, 200},
			{1, 4000, 10000, 150},
			{2, 5000, 14000, 300},
		} {
			if _, err := db.Exec(
				`INSERT INTO steps VALUES (?, 15, 3, 0, ?, NULL, NULL, NULL, NULL, ?, 1)`,
				row.idx, makeMeta(nowSec+int64(row.idx*5), row.inTok, row.crTok, row.outTok), payload,
			); err != nil {
				t.Fatal(err)
			}
		}
		gm2 := buildTestGenMetadataBlobServed("gemini-4-carbon", 1427, 5000, 300, 14000, 0, 300, 19000, 1048576, 1.0, 1.0)
		gm2 = append(gm2, protoUint(2, 2)...)
		if _, err := db.Exec(`INSERT INTO gen_metadata VALUES (0, ?, ?)`, gm2, len(gm2)); err != nil {
			t.Fatal(err)
		}
	}()

	// Write a <100-byte file to verify openSQLiteImage returns os.ErrInvalid (never (nil, nil) panic)
	if err := os.WriteFile(tinyPath, []byte("short-file-under-100-bytes"), 0o644); err != nil {
		t.Fatal(err)
	}
	if img, err := openSQLiteImage(tinyPath); img != nil || err == nil {
		t.Fatalf("expected openSQLiteImage on <100B file to return non-nil error, got img=%v err=%v", img, err)
	}

	// Confirm clean WAL close left zero -wal/-shm files before read-only ingestion
	for _, side := range []string{dbPath + "-wal", dbPath + "-shm"} {
		if _, err := os.Stat(side); !os.IsNotExist(err) {
			t.Fatalf("expected %s absent after clean WAL close, got err=%v", side, err)
		}
	}

	pastStamp := time.Now().Add(-10 * time.Minute).Truncate(time.Second)
	for _, p := range []string{dbPath, tinyPath, appDir} {
		if err := os.Chtimes(p, pastStamp, pastStamp); err != nil {
			t.Fatal(err)
		}
	}
	beforeDirInfo, err := os.Stat(appDir)
	if err != nil {
		t.Fatal(err)
	}
	beforeDbBytes, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	beforeDbSum := sha256.Sum256(beforeDbBytes)

	// 1. Exercise SyncUsageStepsFromSourceDir + backfillEmptyServedModels on writable 0755 appDir
	st := &StateStore{}
	st.Init(stateDB)
	defer func() {
		st.mu.Lock()
		if st.db != nil {
			_ = st.db.Close()
		}
		st.mu.Unlock()
	}()
	if n := st.SyncUsageStepsFromSourceDir(appDir, "APP", "AGY_APP", "Antigravity App", nil); n != 3 {
		t.Fatalf("expected 3 steps (idx 0, 1, 2) ingested from writable WAL DB, got %d", n)
	}
	queried, ok := st.QueryAllUsageSteps(nil)
	if !ok || len(queried) != 3 {
		t.Fatalf("expected 3 queried steps, got ok=%v steps=%+v", ok, queried)
	}
	if queried[0].StepIdx != 0 || queried[1].StepIdx != 1 || queried[2].StepIdx != 2 {
		t.Fatalf("expected exact StepIdx [0, 1, 2] for SQLite serial types 8/9/1, got [%d, %d, %d]", queried[0].StepIdx, queried[1].StepIdx, queried[2].StepIdx)
	}
	if queried[2].ServedModel != "gemini-4-carbon" {
		t.Fatalf("expected step 2 ServedModel=gemini-4-carbon, got %q", queried[2].ServedModel)
	}

	// 2. Exercise GlobalDBCache.GetSteps(..., "APP", ...) AND ReadModelStepsSQL(..., "APP", ...)
	GlobalDBCache.Delete(dbPath)
	cachedSteps := GlobalDBCache.GetSteps(dbPath, "APP", nil)
	if len(cachedSteps) != 3 || cachedSteps[0].StepIdx != 0 || cachedSteps[1].StepIdx != 1 || cachedSteps[2].StepIdx != 2 {
		t.Fatalf("unexpected GlobalDBCache.GetSteps result: %+v", cachedSteps)
	}
	directSteps, okD := ReadModelStepsSQL(dbPath, "APP", nil, 0, pastStamp.Unix())
	if !okD || len(directSteps) != 2 || directSteps[0].StepIdx != 1 || directSteps[1].StepIdx != 2 {
		t.Fatalf("unexpected ReadModelStepsSQL(APP, afterIdx=0) result: ok=%v steps=%+v", okD, directSteps)
	}

	// 3. Assert ZERO -wal/-shm creation (not even transient create+unlink!) via unchanged directory ModTime & entries
	afterDirInfo, err := os.Stat(appDir)
	if err != nil {
		t.Fatal(err)
	}
	if !afterDirInfo.ModTime().Equal(beforeDirInfo.ModTime()) {
		t.Fatalf("writable appDir ModTime was mutated (%v -> %v): transient -shm/-wal file was created/unlinked!", beforeDirInfo.ModTime(), afterDirInfo.ModTime())
	}
	entries, err := os.ReadDir(appDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected exact 2 files in writable appDir, got %d", len(entries))
	}
	for _, side := range []string{dbPath + "-wal", dbPath + "-shm"} {
		if _, err := os.Stat(side); !os.IsNotExist(err) {
			t.Fatalf("unexpected side file created in writable appDir: %s", side)
		}
	}
	afterDbBytes, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(afterDbBytes) != beforeDbSum {
		t.Fatal("dbPath bytes mutated in writable appDir!")
	}
}

func TestCleanupExpiredConversationsNeverTouchesAppConversationsDir(t *testing.T) {
	prevDB := GatewayStateDB
	prevKeep := ConvKeepDays
	t.Cleanup(func() {
		ConvKeepDays = prevKeep
		initConfig()
		InitGlobalStore(prevDB)
	})

	tmp := t.TempDir()
	convDir := filepath.Join(tmp, "gw-convs")
	appDir := filepath.Join(tmp, "app-convs")
	stateDB := filepath.Join(tmp, "state.db")
	if err := os.MkdirAll(convDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(appDir, 0o755); err != nil {
		t.Fatal(err)
	}

	t.Setenv("AGY_CONV_DIR", convDir)
	t.Setenv("AGY_APP_CONV_DIR", appDir)
	t.Setenv("AGY_STATE_DB", stateDB)
	t.Setenv("AGY_PURE_API_HOME", filepath.Join(tmp, "pure"))
	t.Setenv("AGY_GW_LOG", filepath.Join(tmp, "gateway.log"))
	initConfig()
	InitGlobalStore(stateDB)
	ConvKeepDays = 1

	oldTime := time.Now().Add(-10 * 24 * time.Hour)
	gwSid := "11112222-3333-4000-8000-000000000001"
	appSid := "99998888-7777-4000-8000-000000000002"

	gwDbPath := filepath.Join(convDir, gwSid+".db")
	appDbPath := filepath.Join(appDir, appSid+".db")
	appPbPath := filepath.Join(appDir, appSid+".pb")

	writeRealSchemaAppStepDB(t, gwDbPath, oldTime.Unix(), 1427, "gemini-4-carbon", 5000, 0, 100, 0, false)
	writeRealSchemaAppStepDB(t, appDbPath, oldTime.Unix(), 1427, "gemini-4-carbon", 9000, 0, 200, 0, false)
	if err := os.WriteFile(appPbPath, []byte("untouched-app-pb"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{gwDbPath, appDbPath, appPbPath} {
		if err := os.Chtimes(p, oldTime, oldTime); err != nil {
			t.Fatal(err)
		}
	}

	// Adversarially register BOTH SIDs in known_gateway_sids and sync both dirs;
	// CleanupExpiredConversations MUST still only clean convDir and NEVER touch appDir!
	RegisterGatewaySessionID(gwSid)
	RegisterGatewaySessionID(appSid)
	GlobalStore.SyncUsageStepsFromDir(convDir, getKnownGatewaySids())
	GlobalStore.SyncUsageStepsFromSourceDir(appDir, "APP", "AGY_APP", "Antigravity App", getKnownGatewaySids())

	gs := NewGatewayServer()
	defer gs.Close()

	cleaned := gs.CleanupExpiredConversations(time.Now())
	if cleaned != 1 {
		t.Fatalf("expected exactly 1 gateway conversation cleaned from convDir, got %d", cleaned)
	}
	if _, err := os.Stat(gwDbPath); !os.IsNotExist(err) {
		t.Fatalf("expected expired gateway DB in convDir to be removed, stat err=%v", err)
	}
	for _, p := range []string{appDbPath, appPbPath} {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("AppConversationsDir file %s was touched or deleted: %v", p, err)
		}
	}
}

func TestDashboardHTMLAgyAppBadgeFilterAndAutoTabVM(t *testing.T) {
	html := RenderDashboardHTML()

	for i, r := range html {
		if r == '\u2014' || r == '\u2013' {
			t.Fatalf("dashboard.html contains forbidden dash U+%04X at byte %d", r, i)
		}
		if isEmojiRune(r) && !unicode.IsSpace(r) {
			t.Fatalf("dashboard.html contains forbidden emoji rune U+%04X at byte %d", r, i)
		}
	}
	if strings.Contains(html, "建议切换 Hydrogen") {
		t.Fatal("dashboard.html violates Decision U8 strict model pinning")
	}

	startTag := "<script>"
	endTag := "</script>"
	si := strings.Index(html, startTag)
	ei := strings.LastIndex(html, endTag)
	if si < 0 || ei <= si {
		t.Fatal("missing <script>...</script> in RenderDashboardHTML")
	}
	scriptSrc := html[si+len(startTag) : ei]

	nodeBin, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not installed; skipping JS VM verification")
	}
	tmp := t.TempDir()
	jsFile := filepath.Join(tmp, "dashboard_app.js")
	if err := os.WriteFile(jsFile, []byte(scriptSrc), 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command(nodeBin, "--check", jsFile).CombinedOutput(); err != nil {
		t.Fatalf("dashboard.html <script> failed node --check: %v\n%s", err, string(out))
	}

	harness := `
const fs = require('fs');
const vm = require('vm');
const assert = require('assert');

const code = fs.readFileSync(process.argv[2], 'utf8') + '\n;globalThis.__state = state;\n';

class Elem {
  constructor(id = '', tag = 'DIV') {
    this.id = id;
    this.tagName = tag;
    this.textContent = '';
    this._innerHTML = '';
    this.hidden = false;
    this.value = 'ALL';
    this.dataset = {};
    this.style = {};
    this.attrs = {};
    this.listeners = {};
    this.children = [];
    const classes = new Set();
    this.classList = {
      add: (...cs) => cs.forEach(c => classes.add(c)),
      remove: (...cs) => cs.forEach(c => classes.delete(c)),
      toggle: (c, on) => {
        const next = on === undefined ? !classes.has(c) : !!on;
        next ? classes.add(c) : classes.delete(c);
        return next;
      },
      contains: (c) => classes.has(c),
    };
  }
  set innerHTML(v) {
    this._innerHTML = String(v);
    if (this.id === 'river-bar') {
      const cnt = (this._innerHTML.match(/<i /g) || []).length;
      this.children = Array.from({ length: cnt }, () => new Elem('', 'I'));
    }
  }
  get innerHTML() { return this._innerHTML; }
  getAttribute(k) { return this.attrs[k] || ''; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  insertAdjacentHTML() {}
  addEventListener(ev, fn) { (this.listeners[ev] = this.listeners[ev] || []).push(fn); }
  dispatch(ev, obj) { for (const fn of (this.listeners[ev] || [])) fn(obj); }
  querySelectorAll(sel) {
    if (this.id === 'tab-seg' && sel === 'button') return [getEl('tab-db-btn'), getEl('tab-gw-btn')];
    return [];
  }
  querySelector() { return null; }
  contains() { return false; }
  closest() { return null; }
  scrollIntoView() {}
  focus() {}
}

const els = new Map();
function getEl(id) {
  if (!els.has(id)) els.set(id, new Elem(id));
  return els.get(id);
}
getEl('tab-db-btn').dataset.t = 'db';
getEl('tab-gw-btn').dataset.t = 'gw';

const ctx = {
  console,
  Math, Date, Number, String, Boolean, Array, Object, Set, Map, RegExp, JSON, Promise, Error,
  parseFloat, isNaN,
  location: { protocol: 'http:', href: 'http://127.0.0.1:18891/', search: '', reload() {}, replace() {} },
  history: { replaceState() {} },
  localStorage: { getItem: () => 'rolling5h', setItem() {} },
  sessionStorage: { getItem: () => null, setItem() {} },
  navigator: { clipboard: { writeText: async () => {} } },
  window: { getSelection: () => null },
  performance: { now: () => 100 },
  matchMedia: () => ({ matches: true }),
  requestAnimationFrame: () => 1,
  cancelAnimationFrame: () => {},
  setInterval: () => 0,
  setTimeout: () => 0,
  fetch: async () => ({ ok: true, status: 200, json: async () => ({}) }),
  document: {
    hidden: false,
    activeElement: null,
    getElementById: (id) => getEl(id),
    addEventListener: () => {},
    querySelectorAll: (sel) => {
      if (sel === '#tab-seg button') return [getEl('tab-db-btn'), getEl('tab-gw-btn')];
      return [];
    },
  },
};
ctx.globalThis = ctx;
vm.runInNewContext(code, ctx);

const appOnlyPayload = {
  now: '2026-11-18T12:00:00Z',
  gateway: { uiBuildId: ctx.UI_BUILD_ID, defaultModel: 'gemini-4-carbon', models: [{ id: 'gemini-4-carbon', label: 'Gemini 4 Carbon', family: 'gemini' }], activeSessionsCount: 0, maxLiveWorkers: 8 },
  quota: { status: 'ok', updatedAt: '2026-11-18T11:59:00Z', geminiPool: { state: 'known', updatedAt: '2026-11-18T11:59:00Z' }, groups: [], deduction: {}, thirdPartyPool: { state: 'not_enabled', available: false, window5h: {}, windowWeekly: {} } },
  windows: { rolling5h: { stepsCount: 2, models: [] }, last24h: { stepsCount: 2, models: [] }, rolling7d: { stepsCount: 2, models: [] }, allTime: { stepsCount: 2, models: [] } },
  canaryRoutes: [],
  cacheWarnings: [],
  liveSessions: [],
  gatewayTurns: [],
  recentDbSteps: [
    { ts: '2026-11-18T11:59:00Z', sourceSide: 'APP', sourceCategory: 'AGY_APP', sourceLabel: 'Antigravity App', sessionId: 'app-sid-01', clientId: 'agy_app', clientLabel: 'AGY App', protocol: 'App', stepIdx: 2, modelId: 'gemini-4-carbon', modelLabel: 'Gemini 4 Carbon', modelTier: 'high', alignedHitRate: '95.0%', totalCtx: 50000, alignedCr: 47500, alignedIn: 2500, outTok: 600, ttftMs: 900, streamMs: 1200, elapsedMs: 2100, decodeTps: 500, tps: 285.7, costUsd: 0.0158, savedUsd: 0.089 },
    { ts: '2026-11-18T11:58:00Z', sourceSide: 'HOST', sourceCategory: 'AGY_CLI', sourceLabel: 'AGY CLI', sessionId: 'cli-sid-02', clientId: 'agy_host', clientLabel: 'AGY CLI', protocol: 'ACP', stepIdx: 1, modelId: 'gemini-4-carbon', modelLabel: 'Gemini 4 Carbon', modelTier: 'medium', alignedHitRate: '0.0%', cacheMiss: 'cold_start', totalCtx: 10000, alignedCr: 0, alignedIn: 10000, outTok: 200, ttftMs: 700, streamMs: 600, elapsedMs: 1300, costUsd: 0.022, savedUsd: 0 },
  ],
};

// Even if state.tab started on 'gw', !userPickedTab && gatewayTurns.length === 0 && recentDbSteps.length > 0 must auto-switch to 'db'!
ctx.__state.tab = 'gw';
ctx.__state.userPickedTab = false;
ctx.__state.open.add('db:app-sid-01:2');
ctx.render(appOnlyPayload, false);

assert.strictEqual(ctx.__state.tab, 'db', 'Expected auto-switch to db tab when gatewayTurns is empty');
assert.strictEqual(getEl('tab-db-btn').textContent, '单步明细 (2)');
assert.strictEqual(getEl('tab-gw-btn').textContent, '网关轮次 (0)');
assert.strictEqual(getEl('tab-db-btn').classList.contains('on'), true);
assert.strictEqual(getEl('tab-gw-btn').classList.contains('on'), false);

const logHTML = getEl('log-body').innerHTML;
assert(logHTML.includes('cli-badge agy_app'), 'Missing .cli-badge.agy_app class: ' + logHTML);
assert(logHTML.includes('AGY App'), 'Missing AGY App badge label: ' + logHTML);
assert(logHTML.includes('Antigravity App · 桌面端会话'), 'Missing Antigravity App badge tooltip: ' + logHTML);
assert(logHTML.includes('<em>Antigravity App</em>'), 'Detail drawer should display Antigravity App source label instead of App 协议: ' + logHTML);
assert(!logHTML.includes('App 协议'), 'Detail drawer must not render awkward App 协议 text');

const cliOptHTML = getEl('cli-sel').innerHTML;
assert(cliOptHTML.includes('>Antigravity App (1)</option>'), 'Missing Antigravity App option in #cli-sel: ' + cliOptHTML);
assert(cliOptHTML.includes('>AGY CLI (1)</option>'), 'Missing AGY CLI option in #cli-sel: ' + cliOptHTML);

// Filter by agy_app in #cli-sel
getEl('cli-sel').value = 'agy_app';
getEl('cli-sel').dispatch('change', { target: getEl('cli-sel') });
assert.strictEqual(getEl('tab-db-btn').textContent, '单步明细 (1)');
assert(getEl('log-body').innerHTML.includes('app-sid-01') && !getEl('log-body').innerHTML.includes('cli-sid-02'), 'Client filter agy_app failed');

// Reset client filter, then manually click #tab-gw-btn -> sets userPickedTab = true and stays on 'gw' across polls!
getEl('cli-sel').value = 'ALL';
getEl('cli-sel').dispatch('change', { target: getEl('cli-sel') });
const gwBtn = getEl('tab-gw-btn');
gwBtn.closest = (sel) => sel === 'button' ? gwBtn : null;
getEl('tab-seg').dispatch('click', { target: gwBtn });
assert.strictEqual(ctx.__state.userPickedTab, true, 'Manual click on #tab-seg must set userPickedTab=true');
assert.strictEqual(ctx.__state.tab, 'gw');

ctx.render(appOnlyPayload, true);
assert.strictEqual(ctx.__state.tab, 'gw', 'Subsequent poll must respect manual tab choice when userPickedTab=true');
assert(getEl('log-body').innerHTML.includes('暂无匹配的网关轮次'), 'Expected empty gw tab placeholder after manual tab selection');
`
	harnessFile := filepath.Join(tmp, "run_app_vm.js")
	if err := os.WriteFile(harnessFile, []byte(harness), 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command(nodeBin, harnessFile, jsFile).CombinedOutput(); err != nil {
		t.Fatalf("dashboard.html App telemetry VM test failed: %v\n%s", err, string(out))
	}
}
