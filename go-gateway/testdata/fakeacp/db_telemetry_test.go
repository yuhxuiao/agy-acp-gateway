package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func readTestVarint(buf []byte, pos int) (uint64, int, bool) {
	var val uint64
	var shift uint
	for pos < len(buf) {
		b := buf[pos]
		pos++
		val |= uint64(b&0x7f) << shift
		if (b & 0x80) == 0 {
			return val, pos, true
		}
		shift += 7
		if shift >= 64 {
			return 0, pos, false
		}
	}
	return 0, pos, false
}

func TestVersionAndConvDirResolution(t *testing.T) {
	t.Setenv("FAKE_ACP_VERSION", "")
	if got := fakeAcpVersion(); got != "1.5.10-fakeacp" {
		t.Fatalf("expected 1.5.10-fakeacp, got %q", got)
	}

	t.Setenv("FAKE_ACP_CONV_DIR", "")
	t.Setenv("FAKE_ACP_WRITE_DB", "")
	t.Setenv("AGY_CONV_DIR", "")
	if got := resolveFakeConvDir([]string{"acp", "--gemini-dir", "/tmp/home"}); got != "" {
		t.Fatalf("expected empty convDir when DB write disabled, got %q", got)
	}

	tmp := t.TempDir()
	t.Setenv("FAKE_ACP_WRITE_DB", "1")
	gotDerived := resolveFakeConvDir([]string{"acp", "--gemini-dir", tmp, "--app-data-dir", "antigravity-cli"})
	wantDerived := filepath.Join(tmp, "antigravity-cli", "conversations")
	if gotDerived != wantDerived {
		t.Fatalf("expected derived %q, got %q", wantDerived, gotDerived)
	}

	// Reject unsafe C-drive paths fail-closed
	t.Setenv("FAKE_ACP_CONV_DIR", "/mnt/c/Users/test/convs")
	if got := resolveFakeConvDir(nil); got != "" {
		t.Fatalf("expected /mnt/c path to be rejected, got %q", got)
	}

	explicit := filepath.Join(tmp, "custom-convs")
	t.Setenv("FAKE_ACP_CONV_DIR", explicit)
	if got := resolveFakeConvDir(nil); got != explicit {
		t.Fatalf("expected explicit %q, got %q", explicit, got)
	}
}

func TestAppendSessionGenerationWireCompatibilityAndROConcurrency(t *testing.T) {
	convDir := filepath.Join(t.TempDir(), "convs")
	sid := "00000000-0000-4000-8000-000000000042"

	if err := appendSessionGeneration(convDir, "../escape", fakeGenRecord{InTok: 10, OutTok: 5}); err == nil {
		t.Fatal("expected path-traversal session ID to fail")
	}

	rec1 := fakeGenRecord{
		ModelID:      "gemini-4-argon",
		Tier:         "high",
		ServedModel:  "gemini-4-argon-b",
		InTok:        4200,
		CrTok:        32768,
		OutTok:       150,
		ThTok:        30,
		TtftMs:       225,
		StreamMs:     650,
		ReplyText:    "FAKE_OK turn 1",
		ThoughtText:  "Thinking step 1...",
		ToolCalls:    []fakeToolProposal{{ID: "call_1", RawToolName: "mcp_ws-bridge_ws_Bash_123456789abc", ArgsJSON: `{"command":"echo ok"}`}},
		TimestampUTC: time.Now().UTC(),
	}
	if err := appendSessionGeneration(convDir, sid, rec1); err != nil {
		t.Fatalf("appendSessionGeneration turn 1 failed: %v", err)
	}

	dbPath := filepath.Join(convDir, sid+".db")
	st, err := os.Stat(dbPath)
	if err != nil || st.Size() < 4096 {
		t.Fatalf("expected base <sid>.db >= 4096 bytes for pure-Go B-tree reader, got err=%v size=%d", err, func() int64 {
			if st != nil {
				return st.Size()
			}
			return -1
		}())
	}

	// Keep a read-only SQLite connection open (matching go-gateway getReadOnlySessionDB) while Turn 2 writes!
	roDB, err := sql.Open("sqlite", sqliteDSN(dbPath, "mode=ro&_pragma=busy_timeout(100)&_pragma=query_only(1)"))
	if err != nil {
		t.Fatal(err)
	}
	defer roDB.Close()

	var stepCount int
	if err := roDB.QueryRow(`SELECT COUNT(*) FROM steps WHERE step_type = 15`).Scan(&stepCount); err != nil || stepCount != 1 {
		t.Fatalf("expected 1 step on roDB, got err=%v count=%d", err, stepCount)
	}

	rec2 := fakeGenRecord{
		ModelID:      "claude-sonnet-5-5",
		Tier:         "medium",
		ServedModel:  "claude-sonnet-5-5",
		InTok:        1500,
		CrTok:        36968,
		CwTok:        1500,
		OutTok:       80,
		TtftMs:       180,
		StreamMs:     500,
		ReplyText:    "FAKE_OK turn 2",
		TimestampUTC: time.Now().UTC(),
	}
	if err := appendSessionGeneration(convDir, sid, rec2); err != nil {
		t.Fatalf("appendSessionGeneration turn 2 while roDB open failed: %v", err)
	}

	if err := roDB.QueryRow(`SELECT COUNT(*) FROM steps WHERE step_type = 15`).Scan(&stepCount); err != nil || stepCount != 2 {
		t.Fatalf("expected 2 steps on open roDB after turn 2, got err=%v count=%d", err, stepCount)
	}

	// Verify 10-column schema order (idx=0, step_type=1, ..., metadata=4, ..., step_payload=9)
	cols, err := roDB.Query(`PRAGMA table_info(steps)`)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for cols.Next() {
		var cid, notnull, pk int
		var name, ctype string
		var dflt sql.NullString
		if err := cols.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
	}
	_ = cols.Close()
	if len(names) != 10 || names[0] != "idx" || names[1] != "step_type" || names[4] != "metadata" || names[9] != "step_payload" {
		t.Fatalf("steps column layout does not match ReadModelStepsFromSQLite expectations: %v", names)
	}

	// Verify gen_metadata row 0 carries field 1 (ChatModelMetadata with servedModel) and field 2 (step_indices = 1)
	var gmData []byte
	if err := roDB.QueryRow(`SELECT data FROM gen_metadata WHERE idx = 0`).Scan(&gmData); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(gmData, []byte("gemini-4-argon-b")) || !bytes.Contains(gmData, []byte("Gemini 4 Argon")) {
		t.Fatalf("gen_metadata missing servedModel or display label: %x", gmData)
	}
	foundStepLink := false
	pos := 0
	for pos < len(gmData) {
		tag, next, ok := readTestVarint(gmData, pos)
		if !ok {
			break
		}
		pos = next
		fn, wt := tag>>3, tag&7
		if wt == 0 {
			v, nextV, okV := readTestVarint(gmData, pos)
			if !okV {
				break
			}
			pos = nextV
			if fn == 2 && v == 1 {
				foundStepLink = true
			}
		} else if wt == 2 {
			ln, nextL, okL := readTestVarint(gmData, pos)
			if !okL || nextL+int(ln) > len(gmData) {
				break
			}
			pos = nextL + int(ln)
		} else {
			break
		}
	}
	if !foundStepLink {
		t.Fatal("expected gen_metadata idx=0 to link step_indices=[1]")
	}
}

func TestFakeAcpSubprocessVersionQuotaUsageUpdateAndDB(t *testing.T) {
	tmp := t.TempDir()
	bin := filepath.Join(tmp, "fakeacp-bin")
	buildCmd := exec.Command("go", "build", "-o", bin, ".")
	buildCmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("go build fakeacp failed: %v: %s", err, string(out))
	}

	// 1. --version outputs 1.5.10-fakeacp
	verOut, err := exec.Command(bin, "--version").Output()
	if err != nil || strings.TrimSpace(string(verOut)) != "1.5.10-fakeacp" {
		t.Fatalf("unexpected --version: err=%v out=%q", err, string(verOut))
	}

	// 2. -p /usage --output-format json outputs valid quota groups JSON
	usageOut, err := exec.Command(bin, "-p", "/usage", "--output-format", "json").Output()
	if err != nil {
		t.Fatalf("-p /usage failed: %v", err)
	}
	var parsedUsage map[string]any
	if err := json.Unmarshal(usageOut, &parsedUsage); err != nil || !strings.Contains(string(usageOut), "Gemini models") {
		t.Fatalf("unexpected /usage output: err=%v raw=%s", err, string(usageOut))
	}

	// 3. ACP session/prompt with FAKE_ACP_WRITE_DB=1, FAKE_ACP_EMIT_USAGE_UPDATE=1, FAKE_ACP_SERVED_MODEL, FAKE_ACP_CACHE_READ_TOKENS, FAKE_ACP_THINKING_MS
	gemHome := filepath.Join(tmp, "pure-home")
	proc := exec.Command(bin, "acp", "--gemini-dir", gemHome, "--app-data-dir", "antigravity-cli")
	proc.Env = append(os.Environ(),
		"FAKE_ACP_WRITE_DB=1",
		"FAKE_ACP_EMIT_USAGE_UPDATE=1",
		"FAKE_ACP_SERVED_MODEL=gemini-4-carbon-canary",
		"FAKE_ACP_CACHE_READ_TOKENS=16384",
		"FAKE_ACP_THINKING_MS=40",
	)
	stdin, err := proc.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := proc.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := proc.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = stdin.Close()
		_ = proc.Process.Kill()
		_ = proc.Wait()
	}()

	sc := bufio.NewScanner(stdout)
	sendRPC := func(v map[string]any) {
		b, _ := json.Marshal(v)
		_, _ = stdin.Write(append(b, '\n'))
	}
	readUntilID := func(wantID float64) ([]map[string]any, map[string]any) {
		var updates []map[string]any
		for sc.Scan() {
			var m map[string]any
			if json.Unmarshal(sc.Bytes(), &m) != nil {
				continue
			}
			if m["method"] == "session/update" {
				updates = append(updates, m)
				continue
			}
			if id, ok := m["id"].(float64); ok && id == wantID {
				return updates, m
			}
		}
		t.Fatalf("EOF waiting for id %v", wantID)
		return nil, nil
	}

	sendRPC(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": map[string]any{"protocolVersion": 1}})
	_, _ = readUntilID(1)

	sendRPC(map[string]any{"jsonrpc": "2.0", "id": 2, "method": "session/new", "params": map[string]any{"cwd": tmp}})
	_, newResp := readUntilID(2)
	sid := newResp["result"].(map[string]any)["sessionId"].(string)

	sendRPC(map[string]any{
		"jsonrpc": "2.0",
		"id":      3,
		"method":  "session/prompt",
		"params": map[string]any{
			"sessionId": sid,
			"prompt":    []map[string]any{{"type": "text", "text": "Hello telemetry"}},
		},
	})
	updates, promptResp := readUntilID(3)

	sawUsageUpdate := false
	sawThought := false
	for _, u := range updates {
		upd := u["params"].(map[string]any)["update"].(map[string]any)
		switch upd["sessionUpdate"] {
		case "usage_update":
			used, _ := upd["used"].(float64)
			size, _ := upd["size"].(float64)
			if int(used) == 256+16384 && int(size) == 983040 {
				sawUsageUpdate = true
			}
		case "agent_thought_chunk":
			sawThought = true
		}
	}
	if !sawUsageUpdate {
		t.Fatalf("expected usage_update with used=%d size=983040, got updates=%+v", 256+16384, updates)
	}
	if !sawThought {
		t.Fatalf("expected agent_thought_chunk when FAKE_ACP_THINKING_MS=40, got updates=%+v", updates)
	}
	resUsage := promptResp["result"].(map[string]any)["usage"].(map[string]any)
	if int(resUsage["cachedReadTokens"].(float64)) != 16384 {
		t.Fatalf("expected cachedReadTokens=16384, got %+v", resUsage)
	}

	dbFile := filepath.Join(gemHome, "antigravity-cli", "conversations", sid+".db")
	db, err := sql.Open("sqlite", sqliteDSN(dbFile, "mode=ro&_pragma=query_only(1)"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var gmBlob []byte
	if err := db.QueryRow(`SELECT data FROM gen_metadata WHERE idx = 0`).Scan(&gmBlob); err != nil {
		t.Fatalf("expected gen_metadata row in %s: %v", dbFile, err)
	}
	if !bytes.Contains(gmBlob, []byte("gemini-4-carbon-canary")) {
		t.Fatalf("expected FAKE_ACP_SERVED_MODEL in gen_metadata blob")
	}
}
