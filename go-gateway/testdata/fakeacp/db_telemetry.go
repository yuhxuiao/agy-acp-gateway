package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "modernc.org/sqlite"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultFakeAcpVersion = "1.5.10-fakeacp"
	defaultContextMax     = 983040
	gptOssContextMax      = 131072
	defaultTtftMs         = 120
	defaultStreamMs       = 480
)

var (
	reFakeServedModel  = regexp.MustCompile(`\[FAKE_SERVED_MODEL:([A-Za-z0-9._-]+)\]`)
	reFakeInputTokens  = regexp.MustCompile(`\[FAKE_INPUT_TOKENS:(\d+)\]`)
	reFakeCacheRead    = regexp.MustCompile(`\[FAKE_CACHE_READ:(\d+)\]`)
	reFakeOutputTokens = regexp.MustCompile(`\[FAKE_OUTPUT_TOKENS:(\d+)\]`)
	reFakeThoughtToks  = regexp.MustCompile(`\[FAKE_THOUGHT_TOKENS:(\d+)\]`)
	reFakeThinkingMs   = regexp.MustCompile(`\[FAKE_THINKING_MS:(\d+)\]`)
	reFakeTtftMs       = regexp.MustCompile(`\[FAKE_TTFT_MS:(\d+)\]`)
	reFakeStreamMs     = regexp.MustCompile(`\[FAKE_STREAM_MS:(\d+)\]`)

	dbWriteMu       sync.Mutex
	shadowedPrompts = map[string]bool{}
	shadowMu        sync.Mutex
)

type fakeModelMeta struct {
	label      string
	contextMax int
	tiers      map[string]int
}

// Mirrors go-gateway/models.go ModelFromEnum mappings so SQLite steps/gen_metadata
// round-trip cleanly through both ReadModelStepsSQL and ReadModelStepsFromSQLite.
var fakeModelRegistry = map[string]fakeModelMeta{
	"gemini-4-carbon": {
		label:      "Gemini 4 Carbon",
		contextMax: defaultContextMax,
		tiers:      map[string]int{"high": 1427, "medium": 1437, "low": 1438},
	},
	"gemini-4-neon": {
		label:      "Gemini 4 Neon",
		contextMax: defaultContextMax,
		tiers:      map[string]int{"high": 1532, "medium": 1533, "low": 1534, "xhigh": 1535},
	},
	"gemini-4-argon": {
		label:      "Gemini 4 Argon",
		contextMax: defaultContextMax,
		tiers:      map[string]int{"high": 1417, "medium": 1418, "low": 1419},
	},
	"gemini-4-hydrogen": {
		label:      "Gemini 4 Hydrogen",
		contextMax: defaultContextMax,
		tiers:      map[string]int{"high": 1525, "medium": 1526, "low": 1527, "xhigh": 1528},
	},
	"gemini-3.8-flash": {
		label:      "Gemini 3.8 Flash",
		contextMax: defaultContextMax,
		tiers:      map[string]int{"high": 1318, "medium": 1319, "low": 1320},
	},
	"claude-opus-5-5": {
		label:      "Claude Opus 5.5",
		contextMax: defaultContextMax,
		tiers:      map[string]int{"low": 1400, "medium": 1401, "high": 1402},
	},
	"claude-sonnet-5-5": {
		label:      "Claude Sonnet 5.5",
		contextMax: defaultContextMax,
		tiers:      map[string]int{"low": 1403, "medium": 1404, "high": 1405},
	},
	"gpt-oss-120b-medium": {
		label:      "GPT-OSS 120B",
		contextMax: gptOssContextMax,
		tiers:      map[string]int{"medium": 342},
	},
}

type fakeToolProposal struct {
	ID          string
	RawToolName string
	ArgsJSON    string
}

type fakeGenRecord struct {
	ModelID      string
	Tier         string
	ServedModel  string
	InTok        int
	CrTok        int
	CwTok        int
	OutTok       int
	ThTok        int
	TtftMs       int
	StreamMs     int
	ThinkingMs   int
	RealTiming   bool
	ReplyText    string
	ThoughtText  string
	ToolCalls    []fakeToolProposal
	TimestampUTC time.Time
}

func fakeAcpVersion() string {
	if v := strings.TrimSpace(os.Getenv("FAKE_ACP_VERSION")); v != "" {
		return v
	}
	return defaultFakeAcpVersion
}

func isUsageQuotaInvocation(args []string) bool {
	hasPrompt := false
	hasUsage := false
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-p", "--prompt":
			hasPrompt = true
			if i+1 < len(args) && args[i+1] == "/usage" {
				hasUsage = true
			}
		case "/usage":
			hasUsage = true
		}
	}
	return hasPrompt && hasUsage
}

func renderFakeQuotaJSON(now time.Time) ([]byte, error) {
	if raw := strings.TrimSpace(os.Getenv("FAKE_ACP_QUOTA_JSON")); raw != "" {
		return []byte(raw), nil
	}
	u := now.UTC()
	r5h := u.Add(3 * time.Hour).Format(time.RFC3339)
	rwk := u.Add(96 * time.Hour).Format(time.RFC3339)
	gem5 := 0.90
	gemWk := 0.95
	tp5 := 0.85
	tpWk := 0.90
	payload := map[string]any{
		"command": map[string]any{
			"data": map[string]any{
				"groups": []map[string]any{
					{
						"name":        "Gemini models",
						"description": "Gemini 4 & 3.8 quota pool (fakeacp)",
						"buckets": []map[string]any{
							{"id": "gemini_5h", "name": "5-hour window", "window": "5h", "remaining_fraction": gem5, "reset_time": r5h},
							{"id": "gemini_weekly", "name": "Weekly window", "window": "weekly", "remaining_fraction": gemWk, "reset_time": rwk},
						},
					},
					{
						"name":        "Claude models",
						"description": "Third-party Claude quota pool (fakeacp)",
						"buckets": []map[string]any{
							{"id": "claude_5h", "name": "5-hour window", "window": "5h", "remaining_fraction": tp5, "reset_time": r5h},
							{"id": "claude_weekly", "name": "Weekly window", "window": "weekly", "remaining_fraction": tpWk, "reset_time": rwk},
						},
					},
				},
			},
		},
	}
	return json.Marshal(payload)
}

func fakeEnvBool(key string, def bool) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	switch v {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return def
	}
}

func fakeEnvNonNegInt(key string) (int, bool) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return 0, false
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return 0, false
	}
	return n, true
}

func fakeEnvPosInt(key string) (int, bool) {
	n, ok := fakeEnvNonNegInt(key)
	if !ok || n <= 0 {
		return 0, false
	}
	return n, true
}

func parsePromptInt(re *regexp.Regexp, txt string) (int, bool) {
	m := re.FindStringSubmatch(txt)
	if len(m) != 2 {
		return 0, false
	}
	n, err := strconv.Atoi(m[1])
	if err != nil || n < 0 {
		return 0, false
	}
	return n, true
}

func parseSpawnDirs(args []string) (geminiDir, appDataDir string) {
	appDataDir = "antigravity-cli"
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--gemini-dir" && i+1 < len(args):
			geminiDir = args[i+1]
			i++
		case strings.HasPrefix(a, "--gemini-dir="):
			geminiDir = strings.TrimPrefix(a, "--gemini-dir=")
		case a == "--app-data-dir" && i+1 < len(args):
			appDataDir = args[i+1]
			i++
		case strings.HasPrefix(a, "--app-data-dir="):
			appDataDir = strings.TrimPrefix(a, "--app-data-dir=")
		}
	}
	return strings.TrimSpace(geminiDir), strings.TrimSpace(appDataDir)
}

func isUnsafeFakePath(p string) bool {
	if strings.TrimSpace(p) == "" {
		return true
	}
	clean := filepath.Clean(p)
	if clean == "/mnt/c" || strings.HasPrefix(clean, "/mnt/c/") {
		return true
	}
	if len(clean) >= 2 && (clean[0] == 'C' || clean[0] == 'c') && clean[1] == ':' {
		return true
	}
	return false
}

func resolveFakeConvDir(args []string) string {
	if explicit := strings.TrimSpace(os.Getenv("FAKE_ACP_CONV_DIR")); explicit != "" {
		if isUnsafeFakePath(explicit) {
			return ""
		}
		return explicit
	}
	if !fakeEnvBool("FAKE_ACP_WRITE_DB", false) {
		return ""
	}
	if agyConv := strings.TrimSpace(os.Getenv("AGY_CONV_DIR")); agyConv != "" && !isUnsafeFakePath(agyConv) {
		return agyConv
	}
	geminiDir, appDataDir := parseSpawnDirs(args)
	if geminiDir == "" || isUnsafeFakePath(geminiDir) {
		return ""
	}
	if appDataDir == "" {
		appDataDir = "antigravity-cli"
	}
	return filepath.Join(geminiDir, appDataDir, "conversations")
}

func shouldEmitUsageUpdate() bool {
	return fakeEnvBool("FAKE_ACP_EMIT_USAGE_UPDATE", true)
}

func modelContextMax(modelID string) int {
	if m, ok := fakeModelRegistry[modelID]; ok && m.contextMax > 0 {
		return m.contextMax
	}
	return defaultContextMax
}

func emitUsageUpdate(sid, modelID string, inTok, crTok int) {
	if !shouldEmitUsageUpdate() || sid == "" {
		return
	}
	used := inTok + crTok
	if u, ok := fakeEnvNonNegInt("FAKE_ACP_USAGE_USED"); ok {
		used = u
	}
	size := modelContextMax(modelID)
	if s, ok := fakeEnvPosInt("FAKE_ACP_USAGE_SIZE"); ok {
		size = s
	}
	sendUpdate(sid, map[string]any{
		"sessionUpdate": "usage_update",
		"used":          used,
		"size":          size,
	})
}

func lookupFakeModelEnum(modelID, tier string) (int, string, int) {
	m, ok := fakeModelRegistry[modelID]
	if !ok {
		m = fakeModelRegistry["gemini-4-carbon"]
	}
	t := strings.ToLower(strings.TrimSpace(tier))
	if t == "" {
		t = "medium"
	}
	if e, okE := m.tiers[t]; okE {
		return e, m.label, m.contextMax
	}
	if e, okM := m.tiers["medium"]; okM {
		return e, m.label, m.contextMax
	}
	return 1437, m.label, m.contextMax
}

func resolveServedModel(modelID, txt string) string {
	if m := reFakeServedModel.FindStringSubmatch(txt); len(m) == 2 && m[1] != "" {
		return m[1]
	}
	if strings.Contains(txt, "[FAKE_SERVED_SHADOW_ONCE]") {
		shadowMu.Lock()
		alreadyShadowed := shadowedPrompts[txt]
		shadowedPrompts[txt] = true
		shadowMu.Unlock()
		if !alreadyShadowed {
			return modelID + "-b"
		}
		return modelID
	}
	if envServed := strings.TrimSpace(os.Getenv("FAKE_ACP_SERVED_MODEL")); envServed != "" {
		return envServed
	}
	if modelID != "" {
		return modelID
	}
	return "gemini-4-carbon"
}

func resolveTurnTelemetry(modelID, tier, txt string, baseIn, baseCr, baseOut, baseTh int) fakeGenRecord {
	inTok := baseIn
	crTok := baseCr
	outTok := baseOut
	thTok := baseTh

	if v, ok := fakeEnvPosInt("FAKE_ACP_INPUT_TOKENS"); ok {
		inTok = v
	}
	if v, ok := fakeEnvNonNegInt("FAKE_ACP_CACHE_READ_TOKENS"); ok {
		crTok = v
	}
	if v, ok := fakeEnvPosInt("FAKE_ACP_OUTPUT_TOKENS"); ok {
		outTok = v
	}
	if v, ok := fakeEnvNonNegInt("FAKE_ACP_THOUGHT_TOKENS"); ok {
		thTok = v
	}

	if v, ok := parsePromptInt(reFakeInputTokens, txt); ok && v > 0 {
		inTok = v
	}
	if v, ok := parsePromptInt(reFakeCacheRead, txt); ok {
		crTok = v
	}
	if v, ok := parsePromptInt(reFakeOutputTokens, txt); ok && v > 0 {
		outTok = v
	}
	if v, ok := parsePromptInt(reFakeThoughtToks, txt); ok {
		thTok = v
	}

	thinkingMs := 0
	realTiming := strings.Contains(txt, "[FAKE_REAL_TIMING]")
	if v, ok := fakeEnvNonNegInt("FAKE_ACP_THINKING_MS"); ok {
		thinkingMs = v
		if v > 0 {
			realTiming = true
		}
	}
	if v, ok := parsePromptInt(reFakeThinkingMs, txt); ok {
		thinkingMs = v
	}
	if thinkingMs > 0 && thTok == 0 {
		thTok = 12
	}
	if thTok > 0 && outTok < thTok+8 {
		outTok = thTok + 18
	}

	ttftMs := defaultTtftMs
	if v, ok := fakeEnvPosInt("FAKE_ACP_TTFT_MS"); ok {
		ttftMs = v
	}
	if v, ok := parsePromptInt(reFakeTtftMs, txt); ok && v > 0 {
		ttftMs = v
	}

	streamMs := defaultStreamMs
	if thinkingMs > 0 && streamMs < thinkingMs*2 {
		streamMs = thinkingMs * 2
	}
	if streamMs < 400 {
		streamMs = 400
	}
	if v, ok := fakeEnvPosInt("FAKE_ACP_STREAM_MS"); ok {
		streamMs = v
	}
	if v, ok := parsePromptInt(reFakeStreamMs, txt); ok && v > 0 {
		streamMs = v
	}

	cwTok := 0
	if strings.HasPrefix(modelID, "claude-") && crTok > 0 && inTok > 0 {
		cwTok = inTok
	}

	return fakeGenRecord{
		ModelID:      modelID,
		Tier:         tier,
		ServedModel:  resolveServedModel(modelID, txt),
		InTok:        inTok,
		CrTok:        crTok,
		CwTok:        cwTok,
		OutTok:       outTok,
		ThTok:        thTok,
		TtftMs:       ttftMs,
		StreamMs:     streamMs,
		ThinkingMs:   thinkingMs,
		RealTiming:   realTiming,
		TimestampUTC: time.Now().UTC(),
	}
}

func sleepCancellable(d time.Duration, cancelCh <-chan struct{}) bool {
	if d <= 0 {
		return true
	}
	if d > 2*time.Second {
		d = 2 * time.Second
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-cancelCh:
		return false
	case <-timer.C:
		return true
	}
}

// Protobuf wire helpers matching go-gateway/sqlite_protobuf.go.
func pbVarint(v uint64) []byte {
	var out [10]byte
	n := 0
	for {
		b := byte(v & 0x7f)
		v >>= 7
		if v != 0 {
			b |= 0x80
		}
		out[n] = b
		n++
		if v == 0 {
			break
		}
	}
	return out[:n]
}

func pbTag(fn, wt uint64) []byte {
	return pbVarint((fn << 3) | wt)
}

func pbUint(fn, v uint64) []byte {
	tag := pbTag(fn, 0)
	val := pbVarint(v)
	out := make([]byte, 0, len(tag)+len(val))
	out = append(out, tag...)
	out = append(out, val...)
	return out
}

func pbBytes(fn uint64, payload []byte) []byte {
	tag := pbTag(fn, 2)
	ln := pbVarint(uint64(len(payload)))
	out := make([]byte, 0, len(tag)+len(ln)+len(payload))
	out = append(out, tag...)
	out = append(out, ln...)
	out = append(out, payload...)
	return out
}

func pbTimestamp(fn uint64, t time.Time) []byte {
	sec := t.Unix()
	nanos := int64(t.Nanosecond())
	var sub []byte
	if sec > 0 {
		sub = append(sub, pbUint(1, uint64(sec))...)
	}
	if nanos > 0 {
		sub = append(sub, pbUint(2, uint64(nanos))...)
	}
	return pbBytes(fn, sub)
}

func pbDurationMs(fn uint64, ms int) []byte {
	if ms <= 0 {
		return nil
	}
	sec := int64(ms / 1000)
	nanos := int64(ms%1000) * 1_000_000
	var sub []byte
	if sec > 0 {
		sub = append(sub, pbUint(1, uint64(sec))...)
	}
	if nanos > 0 {
		sub = append(sub, pbUint(2, uint64(nanos))...)
	}
	return pbBytes(fn, sub)
}

func buildFakeStepBlobs(rec fakeGenRecord) (metaBlob, payloadBlob []byte) {
	modelEnum, _, _ := lookupFakeModelEnum(rec.ModelID, rec.Tier)
	now := rec.TimestampUTC
	if now.IsZero() {
		now = time.Now().UTC()
	}
	totalDur := time.Duration(rec.TtftMs+rec.StreamMs) * time.Millisecond
	tStart := now.Add(-totalDur)
	tFirst := tStart.Add(time.Duration(rec.TtftMs) * time.Millisecond)
	tEnd := now

	var usageSub []byte
	usageSub = append(usageSub, pbUint(1, uint64(modelEnum))...)
	if rec.InTok > 0 {
		usageSub = append(usageSub, pbUint(2, uint64(rec.InTok))...)
	}
	if rec.OutTok > 0 {
		usageSub = append(usageSub, pbUint(3, uint64(rec.OutTok))...)
	}
	if rec.CwTok > 0 {
		usageSub = append(usageSub, pbUint(4, uint64(rec.CwTok))...)
	}
	if rec.CrTok > 0 {
		usageSub = append(usageSub, pbUint(5, uint64(rec.CrTok))...)
	}
	if rec.ThTok > 0 {
		usageSub = append(usageSub, pbUint(9, uint64(rec.ThTok))...)
	}

	metaBlob = append(metaBlob, pbTimestamp(1, tStart)...)
	metaBlob = append(metaBlob, pbTimestamp(6, tFirst)...)
	metaBlob = append(metaBlob, pbTimestamp(7, tEnd)...)
	metaBlob = append(metaBlob, pbBytes(9, usageSub)...)

	var plannerSub []byte
	if rec.ReplyText != "" {
		plannerSub = append(plannerSub, pbBytes(1, []byte(rec.ReplyText))...)
	}
	if rec.ThoughtText != "" {
		plannerSub = append(plannerSub, pbBytes(3, []byte(rec.ThoughtText))...)
	}
	for _, tc := range rec.ToolCalls {
		var tcSub []byte
		if tc.ID != "" {
			tcSub = append(tcSub, pbBytes(1, []byte(tc.ID))...)
		}
		if tc.RawToolName != "" {
			tcSub = append(tcSub, pbBytes(2, []byte(tc.RawToolName))...)
		}
		if tc.ArgsJSON != "" {
			tcSub = append(tcSub, pbBytes(3, []byte(tc.ArgsJSON))...)
		}
		if len(tcSub) > 0 {
			plannerSub = append(plannerSub, pbBytes(7, tcSub)...)
		}
	}
	if len(plannerSub) > 0 {
		payloadBlob = pbBytes(20, plannerSub)
	}
	return metaBlob, payloadBlob
}

func buildFakeGenMetadataBlob(rec fakeGenRecord, stepIdx int) []byte {
	modelEnum, modelLabel, ctxMax := lookupFakeModelEnum(rec.ModelID, rec.Tier)
	visOut := rec.OutTok - rec.ThTok
	if visOut < 0 {
		visOut = 0
	}

	var stats []byte
	stats = append(stats, pbUint(1, uint64(modelEnum))...)
	if rec.InTok > 0 {
		stats = append(stats, pbUint(2, uint64(rec.InTok))...)
	}
	if rec.OutTok > 0 {
		stats = append(stats, pbUint(3, uint64(rec.OutTok))...)
	}
	if rec.CwTok > 0 {
		stats = append(stats, pbUint(4, uint64(rec.CwTok))...)
	}
	if rec.CrTok > 0 {
		stats = append(stats, pbUint(5, uint64(rec.CrTok))...)
	}
	if rec.ThTok > 0 {
		stats = append(stats, pbUint(9, uint64(rec.ThTok))...)
	}
	if visOut > 0 {
		stats = append(stats, pbUint(10, uint64(visOut))...)
	}

	var cwm10 []byte
	if est := rec.InTok + rec.CrTok; est > 0 {
		cwm10 = append(cwm10, pbUint(1, uint64(est))...)
	}
	if ctxMax > 0 {
		cwm10 = append(cwm10, pbUint(4, uint64(ctxMax))...)
	}

	var f1 []byte
	f1 = append(f1, pbUint(3, uint64(modelEnum))...)
	f1 = append(f1, pbBytes(4, stats)...)
	f1 = append(f1, pbBytes(9, pbBytes(10, cwm10))...)
	if d11 := pbDurationMs(11, rec.TtftMs); len(d11) > 0 {
		f1 = append(f1, d11...)
	}
	if d12 := pbDurationMs(12, rec.StreamMs); len(d12) > 0 {
		f1 = append(f1, d12...)
	}
	if rec.ServedModel != "" {
		f1 = append(f1, pbBytes(19, []byte(rec.ServedModel))...)
	}
	if modelLabel != "" {
		f1 = append(f1, pbBytes(21, []byte(modelLabel))...)
	}

	var root []byte
	root = append(root, pbBytes(1, f1)...)
	if stepIdx > 0 {
		root = append(root, pbUint(2, uint64(stepIdx))...)
	}
	return root
}

func sqliteDSN(dbPath, rawQuery string) string {
	p := dbPath
	if abs, err := filepath.Abs(dbPath); err == nil && abs != "" {
		p = abs
	}
	slash := filepath.ToSlash(p)
	if !strings.HasPrefix(slash, "/") {
		slash = "/" + slash
	}
	u := url.URL{
		Scheme:   "file",
		Path:     slash,
		OmitHost: true,
		RawQuery: rawQuery,
	}
	return u.String()
}

const fakeSessionSchemaSQL = `
CREATE TABLE IF NOT EXISTS steps (
	idx INTEGER PRIMARY KEY,
	step_type INTEGER,
	step_status INTEGER,
	view_Mask BLOB,
	metadata BLOB,
	err_msg TEXT,
	a BLOB,
	b BLOB,
	c BLOB,
	step_payload BLOB
);
CREATE TABLE IF NOT EXISTS gen_metadata (
	idx INTEGER PRIMARY KEY,
	data BLOB
);
`

func ensureBaseSessionDB(dbPath string) error {
	if st, err := os.Stat(dbPath); err == nil && st.Size() >= 4096 {
		return nil
	}
	// Initialize schema in rollback mode first so the base <sid>.db file on disk is
	// immediately >= 12288 bytes (>= 4096 byte check in ReadModelStepsFromSQLite / GlobalDBCache).
	initDB, err := sql.Open("sqlite", sqliteDSN(dbPath, "_pragma=page_size(4096)&_pragma=journal_mode(DELETE)&_pragma=busy_timeout(5000)"))
	if err != nil {
		return err
	}
	_, execErr := initDB.Exec(fakeSessionSchemaSQL)
	closeErr := initDB.Close()
	if execErr != nil {
		return execErr
	}
	return closeErr
}

func appendSessionGeneration(convDir, sid string, rec fakeGenRecord) error {
	if convDir == "" || sid == "" || isUnsafeFakePath(convDir) {
		return nil
	}
	cleanSid := filepath.Base(sid)
	if cleanSid != sid || strings.Contains(sid, "..") || strings.ContainsAny(sid, `/\`) {
		return fmt.Errorf("invalid session id %q", sid)
	}
	if err := os.MkdirAll(convDir, 0o700); err != nil {
		return err
	}
	dbPath := filepath.Join(convDir, cleanSid+".db")

	dbWriteMu.Lock()
	defer dbWriteMu.Unlock()

	if err := ensureBaseSessionDB(dbPath); err != nil {
		return err
	}
	db, err := sql.Open("sqlite", sqliteDSN(dbPath, "_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)"))
	if err != nil {
		return err
	}
	defer db.Close()

	if _, err := db.Exec(fakeSessionSchemaSQL); err != nil {
		return err
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	var nextStepIdx int
	if err := tx.QueryRow(`SELECT COALESCE(MAX(idx), 0) + 1 FROM steps`).Scan(&nextStepIdx); err != nil {
		return err
	}
	var nextGenIdx int
	if err := tx.QueryRow(`SELECT COALESCE(MAX(idx), -1) + 1 FROM gen_metadata`).Scan(&nextGenIdx); err != nil {
		return err
	}

	metaBlob, payloadBlob := buildFakeStepBlobs(rec)
	genBlob := buildFakeGenMetadataBlob(rec, nextStepIdx)

	if _, err := tx.Exec(
		`INSERT INTO steps (idx, step_type, step_status, metadata, step_payload) VALUES (?, 15, 3, ?, ?)`,
		nextStepIdx, metaBlob, payloadBlob,
	); err != nil {
		return err
	}
	if _, err := tx.Exec(
		`INSERT INTO gen_metadata (idx, data) VALUES (?, ?)`,
		nextGenIdx, genBlob,
	); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	_, _ = db.Exec(`PRAGMA wal_checkpoint(PASSIVE)`)
	return nil
}
