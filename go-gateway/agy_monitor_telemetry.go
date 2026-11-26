package main

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// agyMonitorEvent captures one upstream LLM call executed internally by agy's
// built-in pre-tool safety monitors (e.g. sir_flash_lite, severe_internal_risks_agy).
// These calls deduct directly from the account's Gemini quota pool but are not
// written into conversation SQLite `steps` / `gen_metadata`.
type agyMonitorEvent struct {
	TsSec     int64
	Monitor   string
	Model     string
	Escalated bool
	TrajChars int64
	RespChars int64
	CostUsd   float64
}

type agyMonitorWindowSummary struct {
	Calls     int
	Escalated int
	EstUsd    float64
}

type agyMonitorFileCache struct {
	offset    int64
	size      int64
	modTimeNs int64
	events    []agyMonitorEvent
	// FIFO carry-over queue keyed by "<pid>|<monitor>" across incremental tail reads
	// so concurrent monitor calls in the same acp-worker process stay paired.
	pending map[string][]*agyMonitorEvent
}

const (
	agyMonitorScanMinInterval = 5 * time.Second
	agyMonitorRetentionWindow = 8 * 24 * time.Hour
	agyMonitorCharsPerToken   = 3.2
	sirFlashLiteRubricChars   = 8331
	severeInternalRubricChars = 16425
	defaultMonitorRubricChars = 8000
	sirFlashLiteInPerM        = 0.10
	sirFlashLiteOutPerM       = 0.40
	severeInternalInPerM      = 0.50 // gemini-4-hydrogen-sonar
	severeInternalOutPerM     = 2.50
	maxAgyMonitorLogLineBytes = 256 * 1024
)

var (
	agyMonitorCacheMu      sync.Mutex
	agyMonitorCacheDir     string
	agyMonitorLastScanAt   time.Time
	agyMonitorFilesByPath  = make(map[string]*agyMonitorFileCache)
	agyMonitorCachedEvents []agyMonitorEvent
)

func resetAgyMonitorCacheForTest() {
	agyMonitorCacheMu.Lock()
	agyMonitorCacheDir = ""
	agyMonitorLastScanAt = time.Time{}
	agyMonitorFilesByPath = make(map[string]*agyMonitorFileCache)
	agyMonitorCachedEvents = nil
	agyMonitorCacheMu.Unlock()
}

func agyMonitorLogDir() string {
	if strings.TrimSpace(PureApiHome) == "" {
		return ""
	}
	return filepath.Join(PureApiHome, "antigravity-cli", "log")
}

func isEscalatedMonitor(monitorName, modelName string) bool {
	lm := strings.ToLower(strings.TrimSpace(monitorName))
	lmod := strings.ToLower(strings.TrimSpace(modelName))
	if strings.Contains(lm, "severe_internal_risks") {
		return true
	}
	if lmod != "" && !strings.Contains(lmod, "flash_lite") && !strings.Contains(lmod, "flash-lite") {
		return true
	}
	return false
}

func estimateAgyMonitorCallUsd(monitorName, modelName string, trajChars, respChars int64) float64 {
	if trajChars < 0 {
		trajChars = 0
	}
	if respChars < 0 {
		respChars = 0
	}
	lm := strings.ToLower(strings.TrimSpace(monitorName))
	lmod := strings.ToLower(strings.TrimSpace(modelName))

	rubric := int64(defaultMonitorRubricChars)
	inPerM := sirFlashLiteInPerM
	outPerM := sirFlashLiteOutPerM

	switch {
	case strings.Contains(lmod, "carbon") || strings.Contains(lmod, "argon"):
		rubric = severeInternalRubricChars
		inPerM, outPerM = 2.0, 10.0
	case strings.Contains(lmod, "neon"):
		rubric = severeInternalRubricChars
		inPerM, outPerM = 1.0, 5.0
	case strings.Contains(lmod, "hydrogen") || strings.Contains(lm, "severe_internal_risks"):
		rubric = severeInternalRubricChars
		inPerM, outPerM = severeInternalInPerM, severeInternalOutPerM
	case strings.Contains(lmod, "flash_lite") || strings.Contains(lmod, "flash-lite") || strings.Contains(lm, "flash_lite"):
		rubric = sirFlashLiteRubricChars
		inPerM, outPerM = sirFlashLiteInPerM, sirFlashLiteOutPerM
	case strings.Contains(lmod, "flash"):
		rubric = sirFlashLiteRubricChars
		inPerM, outPerM = 0.25, 1.25
	}

	inTok := float64(trajChars+rubric) / agyMonitorCharsPerToken
	outTok := float64(respChars) / agyMonitorCharsPerToken
	return (inTok*inPerM + outTok*outPerM) / 1e6
}

// parseAgyWorkerFileDate extracts (year, month) from filenames like
// `acp-worker-20261124_160808-21974.log`, falling back to fallbackTime.
func parseAgyWorkerFileDate(baseName string, fallbackTime time.Time) (int, time.Month) {
	if idx := strings.Index(baseName, "acp-worker-"); idx >= 0 {
		rest := baseName[idx+len("acp-worker-"):]
		if len(rest) >= 8 {
			y, errY := strconv.Atoi(rest[0:4])
			m, errM := strconv.Atoi(rest[4:6])
			if errY == nil && errM == nil && y >= 2020 && y <= 2100 && m >= 1 && m <= 12 {
				return y, time.Month(m)
			}
		}
	}
	return fallbackTime.Year(), fallbackTime.Month()
}

// parseGlogHeader parses a glog prefix `I1124 16:08:30.193634   814 file.go:630] ...`
// returning unix timestamp (using fileYear/fileMonth + loc) and thread/goroutine ID.
func parseGlogHeader(line []byte, fileYear int, fileMonth time.Month, loc *time.Location) (int64, string, bool) {
	// Minimum: "I1124 16:08:30.193634 1 " -> len >= 22
	if len(line) < 22 {
		return 0, "", false
	}
	switch line[0] {
	case 'I', 'W', 'E', 'F':
	default:
		return 0, "", false
	}
	if line[5] != ' ' || line[8] != ':' || line[11] != ':' {
		return 0, "", false
	}
	mo, err1 := strconv.Atoi(string(line[1:3]))
	dy, err2 := strconv.Atoi(string(line[3:5]))
	hr, err3 := strconv.Atoi(string(line[6:8]))
	mn, err4 := strconv.Atoi(string(line[9:11]))
	sc, err5 := strconv.Atoi(string(line[12:14]))
	if err1 != nil || err2 != nil || err3 != nil || err4 != nil || err5 != nil {
		return 0, "", false
	}
	if mo < 1 || mo > 12 || dy < 1 || dy > 31 || hr > 23 || mn > 59 || sc > 59 {
		return 0, "", false
	}
	yr := fileYear
	if fileMonth == time.December && time.Month(mo) == time.January {
		yr++
	} else if fileMonth == time.January && time.Month(mo) == time.December {
		yr--
	}
	if loc == nil {
		loc = time.Local
	}
	ts := time.Date(yr, time.Month(mo), dy, hr, mn, sc, 0, loc).Unix()

	// Skip fractional seconds and whitespace to read thread/goroutine ID column.
	i := 14
	for i < len(line) && line[i] != ' ' {
		i++
	}
	for i < len(line) && line[i] == ' ' {
		i++
	}
	tidStart := i
	for i < len(line) && line[i] != ' ' {
		i++
	}
	tid := ""
	if i > tidStart {
		tid = string(line[tidStart:i])
	}
	return ts, tid, true
}

var (
	markerLLMMonitor    = []byte("LLM Monitor ")
	markerTrajFormatted = ": Trajectory formatted, length: "
	markerCallingLLM    = ": Calling LLM (model: "
	markerRespReceived  = ": LLM response received, length: "
)

func parseAgyMonitorLogChunk(r io.Reader, fileYear int, fileMonth time.Month, loc *time.Location, entry *agyMonitorFileCache) {
	if entry.pending == nil {
		entry.pending = make(map[string][]*agyMonitorEvent)
	}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 32*1024), maxAgyMonitorLogLineBytes)
	for sc.Scan() {
		raw := sc.Bytes()
		pos := bytes.Index(raw, markerLLMMonitor)
		if pos < 0 {
			continue
		}
		ts, tid, ok := parseGlogHeader(raw, fileYear, fileMonth, loc)
		if !ok {
			continue
		}
		rest := string(raw[pos+len(markerLLMMonitor):])
		colon := strings.IndexByte(rest, ':')
		if colon <= 0 {
			continue
		}
		monName := strings.TrimSpace(rest[:colon])
		if monName == "" {
			continue
		}
		key := tid + "|" + monName

		if idx := strings.Index(rest, markerTrajFormatted); idx >= 0 {
			numStr := strings.TrimSpace(rest[idx+len(markerTrajFormatted):])
			if sp := strings.IndexAny(numStr, " \t\r\n,;)"); sp >= 0 {
				numStr = numStr[:sp]
			}
			trajChars, err := strconv.ParseInt(numStr, 10, 64)
			if err != nil || trajChars < 0 {
				continue
			}
			// Flush stale head items (>120s old, e.g. failed/aborted calls without a response line)
			// while keeping active concurrent calls in FIFO order.
			q := entry.pending[key]
			keep := 0
			for keep < len(q) && q[keep] != nil && ts-q[keep].TsSec > 120 {
				prev := q[keep]
				prev.Escalated = isEscalatedMonitor(prev.Monitor, prev.Model)
				prev.CostUsd = estimateAgyMonitorCallUsd(prev.Monitor, prev.Model, prev.TrajChars, prev.RespChars)
				entry.events = append(entry.events, *prev)
				keep++
			}
			q = append(q[keep:], &agyMonitorEvent{
				TsSec:     ts,
				Monitor:   monName,
				TrajChars: trajChars,
			})
			entry.pending[key] = q
			continue
		}

		if idx := strings.Index(rest, markerCallingLLM); idx >= 0 {
			modStr := rest[idx+len(markerCallingLLM):]
			if end := strings.IndexByte(modStr, ')'); end >= 0 {
				modStr = strings.TrimSpace(modStr[:end])
			} else {
				modStr = strings.TrimSpace(modStr)
			}
			q := entry.pending[key]
			matched := false
			for _, cur := range q {
				if cur != nil && cur.Model == "" {
					cur.Model = modStr
					matched = true
					break
				}
			}
			if !matched {
				entry.pending[key] = append(q, &agyMonitorEvent{
					TsSec:   ts,
					Monitor: monName,
					Model:   modStr,
				})
			}
			continue
		}

		if idx := strings.Index(rest, markerRespReceived); idx >= 0 {
			numStr := strings.TrimSpace(rest[idx+len(markerRespReceived):])
			if sp := strings.IndexAny(numStr, " \t\r\n,;)"); sp >= 0 {
				numStr = numStr[:sp]
			}
			respChars, err := strconv.ParseInt(numStr, 10, 64)
			if err != nil || respChars < 0 {
				respChars = 0
			}
			q := entry.pending[key]
			if len(q) > 0 && q[0] != nil {
				cur := q[0]
				cur.RespChars = respChars
				cur.Escalated = isEscalatedMonitor(cur.Monitor, cur.Model)
				cur.CostUsd = estimateAgyMonitorCallUsd(cur.Monitor, cur.Model, cur.TrajChars, cur.RespChars)
				entry.events = append(entry.events, *cur)
				if len(q) == 1 {
					delete(entry.pending, key)
				} else {
					entry.pending[key] = q[1:]
				}
			}
		}
	}
}

// collectAgyMonitorEventsFromDir incrementally reads recent `acp-worker-*.log` files
// from logDir (within agyMonitorRetentionWindow of now) and returns all parsed monitor calls.
func collectAgyMonitorEventsFromDir(logDir string, now time.Time, loc *time.Location, force bool) []agyMonitorEvent {
	if strings.TrimSpace(logDir) == "" {
		return nil
	}
	if loc == nil {
		loc = time.Local
	}

	agyMonitorCacheMu.Lock()
	defer agyMonitorCacheMu.Unlock()

	if agyMonitorCacheDir != logDir {
		agyMonitorCacheDir = logDir
		agyMonitorLastScanAt = time.Time{}
		agyMonitorFilesByPath = make(map[string]*agyMonitorFileCache)
		agyMonitorCachedEvents = nil
	}
	if !force && !agyMonitorLastScanAt.IsZero() && now.Sub(agyMonitorLastScanAt) < agyMonitorScanMinInterval {
		return agyMonitorCachedEvents
	}
	agyMonitorLastScanAt = now

	entries, err := os.ReadDir(logDir)
	if err != nil {
		agyMonitorFilesByPath = make(map[string]*agyMonitorFileCache)
		agyMonitorCachedEvents = nil
		return nil
	}

	cutoff := now.Add(-agyMonitorRetentionWindow)
	seen := make(map[string]bool, len(entries))
	anyChanged := false

	for _, de := range entries {
		if de.IsDir() {
			continue
		}
		name := de.Name()
		if !strings.HasPrefix(name, "acp-worker-") || !strings.HasSuffix(name, ".log") {
			continue
		}
		info, errInfo := de.Info()
		if errInfo != nil || info.Size() == 0 {
			continue
		}
		if info.ModTime().Before(cutoff) {
			continue
		}
		fullPath := filepath.Join(logDir, name)
		seen[fullPath] = true

		curSize := info.Size()
		curMtime := info.ModTime().UnixNano()
		cached := agyMonitorFilesByPath[fullPath]
		if cached != nil && cached.size == curSize && cached.modTimeNs == curMtime {
			continue
		}

		anyChanged = true
		f, errOpen := os.Open(fullPath)
		if errOpen != nil {
			continue
		}
		fileYear, fileMonth := parseAgyWorkerFileDate(name, info.ModTime().In(loc))
		var target *agyMonitorFileCache
		var startOff int64
		if cached != nil && curSize >= cached.offset && curSize >= cached.size {
			target = cached
			startOff = cached.offset
		} else {
			target = &agyMonitorFileCache{
				pending: make(map[string][]*agyMonitorEvent),
			}
			agyMonitorFilesByPath[fullPath] = target
		}
		if startOff > 0 {
			if _, errSeek := f.Seek(startOff, io.SeekStart); errSeek != nil {
				_ = f.Close()
				continue
			}
		}
		chunk, errRead := io.ReadAll(f)
		_ = f.Close()
		if errRead != nil {
			continue
		}
		// Only consume complete newline-terminated lines so concurrent writes never split a line.
		if nl := bytes.LastIndexByte(chunk, '\n'); nl >= 0 {
			complete := chunk[:nl+1]
			parseAgyMonitorLogChunk(bytes.NewReader(complete), fileYear, fileMonth, loc, target)
			target.offset = startOff + int64(len(complete))
		}
		target.size = curSize
		target.modTimeNs = curMtime
	}

	for p := range agyMonitorFilesByPath {
		if !seen[p] {
			delete(agyMonitorFilesByPath, p)
			anyChanged = true
		}
	}

	if !anyChanged && agyMonitorCachedEvents != nil {
		return agyMonitorCachedEvents
	}

	minTs := cutoff.Unix()
	total := 0
	for _, fc := range agyMonitorFilesByPath {
		total += len(fc.events)
		for _, q := range fc.pending {
			total += len(q)
		}
	}
	out := make([]agyMonitorEvent, 0, total)
	for _, fc := range agyMonitorFilesByPath {
		for _, ev := range fc.events {
			if ev.TsSec >= minTs {
				out = append(out, ev)
			}
		}
		// Include in-flight or errored calls that emitted Trajectory formatted / Calling LLM
		// without a subsequent "LLM response received" line.
		for _, q := range fc.pending {
			for _, pend := range q {
				if pend != nil && pend.TsSec >= minTs {
					cp := *pend
					cp.Escalated = isEscalatedMonitor(cp.Monitor, cp.Model)
					cp.CostUsd = estimateAgyMonitorCallUsd(cp.Monitor, cp.Model, cp.TrajChars, cp.RespChars)
					out = append(out, cp)
				}
			}
		}
	}
	agyMonitorCachedEvents = out
	return out
}

func summarizeAgyMonitorEvents(events []agyMonitorEvent, hasValidReset bool, startSec int64, idle bool, readUnix int64) agyMonitorWindowSummary {
	var sum agyMonitorWindowSummary
	for _, ev := range events {
		inWin := false
		if idle {
			inWin = readUnix > 0 && ev.TsSec > readUnix
		} else if hasValidReset {
			inWin = ev.TsSec >= startSec-quotaWindowStartSkewSec
		} else {
			inWin = ev.TsSec >= startSec
		}
		if !inWin {
			continue
		}
		sum.Calls++
		if ev.Escalated {
			sum.Escalated++
		}
		sum.EstUsd += ev.CostUsd
	}
	sum.EstUsd = roundTo(sum.EstUsd, 2)
	return sum
}

// attachAgyMonitorQuotaDeduction enriches quota.Deduction with estimated hidden LLM
// safety-monitor spend (sir_flash_lite / severe_internal_risks_agy) sliced to the
// exact official Gemini 5h and weekly windows.
func attachAgyMonitorQuotaDeduction(quota *QuotaSnapshot, logDir string, now time.Time, loc *time.Location, force bool) {
	if quota == nil || quota.Deduction == nil || quota.GeminiPool == nil || strings.TrimSpace(logDir) == "" {
		return
	}
	if quota.GeminiPool.State != QuotaStateKnown && quota.GeminiPool.State != QuotaStateStale {
		return
	}
	geminiGroup := findGroup(quota.Groups, reGeminiModelsGroup)
	if geminiGroup == nil {
		geminiGroup = findGroup(quota.Groups, reGeminiFallbackGroup)
	}
	if geminiGroup == nil {
		return
	}
	b5h := findBucket(geminiGroup, "5h")
	bWeekly := findBucket(geminiGroup, "weekly")
	if b5h == nil && bWeekly == nil {
		return
	}

	events := collectAgyMonitorEventsFromDir(logDir, now, loc, force)
	if len(events) == 0 {
		return
	}

	nowSec := now.Unix()
	var readUnix int64
	if rt, err := time.Parse(time.RFC3339, quota.UpdatedAt); err == nil {
		readUnix = rt.Unix()
	}
	if b5h != nil {
		start5hSec := windowStartUnix(b5h, nowSec, 5*3600)
		idle5h := quotaWindowIdle(b5h, quota.UpdatedAt, 5*time.Hour)
		s5 := summarizeAgyMonitorEvents(events, hasValidBucketResetTime(b5h), start5hSec, idle5h, readUnix)
		quota.Deduction.MonitorCalls5h = s5.Calls
		quota.Deduction.MonitorEscalated5h = s5.Escalated
		quota.Deduction.MonitorEst5hUsd = s5.EstUsd
	}
	if bWeekly != nil {
		startWeeklySec := windowStartUnix(bWeekly, nowSec, 7*86400)
		idleWeekly := quotaWindowIdle(bWeekly, quota.UpdatedAt, 7*24*time.Hour)
		sw := summarizeAgyMonitorEvents(events, hasValidBucketResetTime(bWeekly), startWeeklySec, idleWeekly, readUnix)
		quota.Deduction.MonitorCallsWeekly = sw.Calls
		quota.Deduction.MonitorEscalatedWeekly = sw.Escalated
		quota.Deduction.MonitorEstWeeklyUsd = sw.EstUsd
	}
}
