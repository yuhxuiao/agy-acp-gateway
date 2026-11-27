package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gwcfg "agy-acp-gateway-go/internal/config"
)

// Monitor budget governor — trip detector & persistence.
//
// agy runs a cheap pre-tool safety monitor (`sir_flash_lite`) for every external MCP tool call. That
// model has its own per-account capacity limit. Once it answers 429 (ResourceExhausted, "reset after
// XhYmZs"), agy escalates 1:1 into the expensive `severe_internal_risks_agy` monitor (full ~300k-char
// trajectory on every tool call, ~80x the price), which drains the whole 5h Gemini pool within minutes.
//
// The gateway cannot (and must not) alter the monitor. Instead it observes the 429 in each account's
// acp-worker logs and treats the account as "monitor-tripped" until the reported reset time: new
// tool-bearing sessions are routed to other accounts, active tool conversations pinned to that
// account are re-routed at the next request (and their orphaned parked session cancelled), zero-tool
// text requests remain exempt, and when no healthy account is left the gateway answers 429 + Retry-After.
const (
	// A transient 429 ("reset after 0s") is ignored; only a real capacity lock-out trips the account.
	monitorTripMinResetSec           = 120
	monitorTripRecoverHysteresis     = 15 * time.Second
	monitorTripScanEvery             = 3 * time.Second
	monitorTripStartupWindow         = 6 * time.Hour
	monitorTripFileKeepWindow        = 30 * time.Minute
	monitorTripInitTail              = 512 * 1024
	monitorTripMaxRead               = 4 * 1024 * 1024
	monitorTripMaxLineBytes          = 256 * 1024
	monitorTripPersistDebounce       = 2 * time.Second
	monitorTripsFileName             = "monitor-trips.json"
	monitorStickyEscalationWindow    = 10 * time.Minute
	monitorStickyEscalationThreshold = 3
	monitorStickyEscalationCooldown  = 10 * time.Minute
)

var (
	monitorTripResetRe      = regexp.MustCompile(`reset after (?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+(?:\.\d+)?)s)?`)
	markerLiteFinished      = []byte("Monitor sir_flash_lite FINISHED")
	markerSevereFinished    = []byte("Monitor severe_internal_risks_agy FINISHED")
	markerSevereCallingLLM  = []byte("LLM Monitor severe_internal_risks_agy: Calling LLM")
	markerResExhausted      = []byte("ResourceExhausted")
	markerResExhaustedUpper = []byte("RESOURCE_EXHAUSTED")
	markerLiteFailedTag     = []byte(`reason="Failed:`)
	markerLiteReasonEmpty   = []byte(`reason=""`)
	markerLiteTriggeredTrue = []byte("triggered=true")
	markerLiteReasonPrefix  = []byte(`reason="`)

	monitorStickyEscalationTotal atomic.Int64
)

// MonitorStickyEscalationState exposes non-429 sticky escalation tracking (agy#1886 c2)
// where an account/session repeatedly invokes severe_internal_risks_agy (>=3 times in 10m)
// even with zero sir_flash_lite 429 trips.
type MonitorStickyEscalationState struct {
	AccountID        string `json:"accountId"`
	SevereCalls10m   int    `json:"severeCalls10m"`
	EscalationAlerts int64  `json:"escalationAlerts"`
	Tripped429       bool   `json:"tripped429"`
	LastSevereAt     string `json:"lastSevereAt,omitempty"`
	LastAlertAt      string `json:"lastAlertAt,omitempty"`
}

type monitorTripFile struct {
	offset int64
}

type monitorTripVerdict struct {
	until  time.Time // when the lite monitor quota resets
	failAt time.Time
	okAt   time.Time
}

type monitorTripAccount struct {
	until              time.Time
	failAt             time.Time
	okAt               time.Time
	files              map[string]*monitorTripFile
	inited             bool
	severeFinishTimes  []time.Time
	severeCallingTimes []time.Time
}

// Lock order: monitorTrips.scanMu -> monitorTrips.stateMu (never reverse).
// scanMu serializes disk scans, file cursors, and persistence; stateMu guards only the tiny
// read-only verdicts map so hot routing reads never block on filesystem I/O.
var monitorTrips = struct {
	scanMu        sync.Mutex
	scanners      map[string]*monitorTripAccount
	reader        *bufio.Reader
	persistDir    string
	lastPersistAt time.Time

	stateMu         sync.RWMutex
	verdicts        map[string]monitorTripVerdict
	stickyStates    map[string]MonitorStickyEscalationState
	stickyCooldowns map[string]time.Time
}{
	scanners:        make(map[string]*monitorTripAccount),
	verdicts:        make(map[string]monitorTripVerdict),
	stickyStates:    make(map[string]MonitorStickyEscalationState),
	stickyCooldowns: make(map[string]time.Time),
}

type persistedMonitorTripEntry struct {
	Until  string `json:"until"`
	FailAt string `json:"failAt"`
	OkAt   string `json:"okAt,omitempty"`
}

type persistedMonitorCapEntry struct {
	HardCap      int    `json:"hardCap"`
	SoftCap      int    `json:"softCap"`
	DrainCap     int    `json:"drainCap"`
	CalibratedAt string `json:"calibratedAt,omitempty"`
}

type persistedMonitorWindowEntry struct {
	WindowKey     string `json:"windowKey,omitempty"`     // official Gemini 5h b5h.ResetTime (RFC3339)
	WindowStartAt string `json:"windowStartAt,omitempty"` // RFC3339/RFC3339Nano
	WindowResetAt string `json:"windowResetAt,omitempty"` // RFC3339/RFC3339Nano
	LiteResetAt   string `json:"liteResetAt,omitempty"`   // authoritative sir_flash_lite reset boundary from real 429 `until`
	LogCalls5h    int    `json:"logCalls5h,omitempty"`    // last reconciled logCalls5h in current window
}

type persistedMonitorStateFile struct {
	Version        int                                    `json:"version"`
	UpdatedAt      string                                 `json:"updatedAt"`
	Trips          map[string]persistedMonitorTripEntry   `json:"trips,omitempty"`
	CalibratedCaps map[string]persistedMonitorCapEntry    `json:"calibratedCaps,omitempty"`
	Windows        map[string]persistedMonitorWindowEntry `json:"windows,omitempty"`
}

func monitorTripEnabled() bool {
	return gwcfg.Current().MonitorTripRouting
}

func monitorTripHopEnabled() bool {
	return gwcfg.Current().MonitorTripHop
}

// isMonitorTripped reports whether a verdict represents an active lockout at now.
// Hysteresis requires a healthy OK at least 15s AFTER failAt so concurrent in-flight
// 0.5-3s OKs started before the 12ms 429 cannot flap or wipe out an active lockout.
func isMonitorTripped(until, failAt, okAt, now time.Time) bool {
	if failAt.IsZero() || !until.After(now) {
		return false
	}
	if !okAt.IsZero() && okAt.Sub(failAt) >= monitorTripRecoverHysteresis {
		return false
	}
	return true
}

// MonitorTripUntil returns the lite-monitor reset time when the account is currently monitor-tripped,
// or the zero time when it is healthy. Never blocks on disk I/O.
func MonitorTripUntil(accountID string, now time.Time) time.Time {
	monitorTrips.stateMu.RLock()
	v, ok := monitorTrips.verdicts[accountID]
	monitorTrips.stateMu.RUnlock()
	if !ok || !isMonitorTripped(v.until, v.failAt, v.okAt, now) {
		return time.Time{}
	}
	return v.until
}

// MonitorTripSnapshot lists every currently tripped account with its reset time (RFC3339).
func MonitorTripSnapshot(now time.Time) map[string]string {
	monitorTrips.stateMu.RLock()
	out := make(map[string]string, len(monitorTrips.verdicts))
	for id, v := range monitorTrips.verdicts {
		if isMonitorTripped(v.until, v.failAt, v.okAt, now) {
			out[id] = v.until.UTC().Format(time.RFC3339)
		}
	}
	monitorTrips.stateMu.RUnlock()
	return out
}

// MonitorStickyEscalationCount returns the total number of throttled MONITOR_STICKY_ESCALATION
// alerts recorded across all accounts (agy#1886 c2).
func MonitorStickyEscalationCount() int64 {
	return monitorStickyEscalationTotal.Load()
}

// MonitorStickyEscalationSnapshot returns per-account sticky escalation stats for accounts that
// have recent severe_internal_risks_agy calls or recorded sticky escalation alerts.
func MonitorStickyEscalationSnapshot(now time.Time) map[string]MonitorStickyEscalationState {
	monitorTrips.stateMu.RLock()
	out := make(map[string]MonitorStickyEscalationState, len(monitorTrips.stickyStates))
	for id, st := range monitorTrips.stickyStates {
		if st.SevereCalls10m > 0 || st.EscalationAlerts > 0 {
			out[id] = st
		}
	}
	monitorTrips.stateMu.RUnlock()
	return out
}

func pruneMonitorTimeWindow(ts []time.Time, cutoff, upper time.Time) []time.Time {
	if len(ts) == 0 {
		return nil
	}
	kept := ts[:0]
	for _, t := range ts {
		if !t.Before(cutoff) && !t.After(upper) {
			kept = append(kept, t)
		}
	}
	if len(kept) == 0 {
		return nil
	}
	return kept
}

// recordMonitorStickyEscalationCount updates per-account severe_internal_risks_agy 10m state and
// logs a throttled MONITOR_STICKY_ESCALATION warning whenever >= 3 severe calls occur within 10m
// (even with 0 sir_flash_lite 429 trips). Safe to call without holding monitorTrips.scanMu.
func recordMonitorStickyEscalationCount(accountID string, severeCalls10m int, lastSevereAt, now time.Time, tripped429 bool) bool {
	if now.IsZero() {
		now = time.Now()
	}
	id := strings.ToLower(strings.TrimSpace(accountID))
	if id == "" {
		id = "default"
	}
	shouldAlert := false
	var acctAlerts, totalAlerts int64

	monitorTrips.stateMu.Lock()
	if monitorTrips.stickyStates == nil {
		monitorTrips.stickyStates = make(map[string]MonitorStickyEscalationState)
	}
	if monitorTrips.stickyCooldowns == nil {
		monitorTrips.stickyCooldowns = make(map[string]time.Time)
	}
	st := monitorTrips.stickyStates[id]
	st.AccountID = id
	st.SevereCalls10m = severeCalls10m
	st.Tripped429 = tripped429
	if !lastSevereAt.IsZero() {
		st.LastSevereAt = lastSevereAt.UTC().Format(time.RFC3339)
	}
	if severeCalls10m >= monitorStickyEscalationThreshold {
		lastAlert := monitorTrips.stickyCooldowns[id]
		if lastAlert.IsZero() || now.Sub(lastAlert) >= monitorStickyEscalationCooldown {
			monitorTrips.stickyCooldowns[id] = now
			st.EscalationAlerts++
			st.LastAlertAt = now.UTC().Format(time.RFC3339)
			acctAlerts = st.EscalationAlerts
			totalAlerts = monitorStickyEscalationTotal.Add(1)
			shouldAlert = true
		}
	}
	if st.SevereCalls10m > 0 || st.EscalationAlerts > 0 {
		monitorTrips.stickyStates[id] = st
	} else {
		delete(monitorTrips.stickyStates, id)
	}
	monitorTrips.stateMu.Unlock()

	if shouldAlert {
		LogEvent("MONITOR_STICKY_ESCALATION", "Detected sticky escalation to severe_internal_risks_agy (>=3 calls within 10m)", map[string]any{
			"accountId":             id,
			"severeCalls10m":        severeCalls10m,
			"tripped429":            tripped429,
			"escalationAlerts":      acctAlerts,
			"totalEscalationAlerts": totalAlerts,
		})
	}
	return shouldAlert
}

func setMonitorTripForTest(accountID string, until time.Time) {
	now := time.Now()
	monitorTrips.scanMu.Lock()
	a := monitorTrips.scanners[accountID]
	if a == nil {
		a = &monitorTripAccount{files: make(map[string]*monitorTripFile), inited: true}
		monitorTrips.scanners[accountID] = a
	}
	a.until = until
	a.failAt = now
	a.okAt = time.Time{}
	monitorTrips.stateMu.Lock()
	monitorTrips.verdicts[accountID] = monitorTripVerdict{until: until, failAt: now}
	monitorTrips.stateMu.Unlock()
	monitorTrips.scanMu.Unlock()
}

func resetMonitorTripsForTest() {
	monitorTrips.scanMu.Lock()
	monitorTrips.scanners = make(map[string]*monitorTripAccount)
	monitorTrips.persistDir = ""
	monitorTrips.lastPersistAt = time.Time{}
	monitorTrips.stateMu.Lock()
	monitorTrips.verdicts = make(map[string]monitorTripVerdict)
	monitorTrips.stickyStates = make(map[string]MonitorStickyEscalationState)
	monitorTrips.stickyCooldowns = make(map[string]time.Time)
	monitorStickyEscalationTotal.Store(0)
	monitorTrips.stateMu.Unlock()
	monitorTrips.scanMu.Unlock()
	resetMonitorBudgetsForTest()
}

// ClearMonitorTrip clears an active monitor trip for one account (or all accounts when accountID is "" or "*"),
// preserving log file cursors so already-scanned historical 429 lines do not re-trip the account.
func ClearMonitorTrip(accountID string) {
	id := strings.ToLower(strings.TrimSpace(accountID))
	now := time.Now()
	changed := false
	monitorTrips.scanMu.Lock()
	monitorTrips.stateMu.Lock()
	if id == "" || id == "*" {
		for k, a := range monitorTrips.scanners {
			if !a.until.IsZero() || !a.failAt.IsZero() {
				changed = true
			}
			a.until = time.Time{}
			a.failAt = time.Time{}
			a.okAt = now
			monitorTrips.verdicts[k] = monitorTripVerdict{okAt: now}
		}
		for k, v := range monitorTrips.verdicts {
			if !v.until.IsZero() || !v.failAt.IsZero() {
				changed = true
				monitorTrips.verdicts[k] = monitorTripVerdict{okAt: now}
			}
		}
	} else {
		if a := monitorTrips.scanners[id]; a != nil {
			if !a.until.IsZero() || !a.failAt.IsZero() {
				changed = true
			}
			a.until = time.Time{}
			a.failAt = time.Time{}
			a.okAt = now
		}
		if v, ok := monitorTrips.verdicts[id]; ok {
			if !v.until.IsZero() || !v.failAt.IsZero() {
				changed = true
			}
			monitorTrips.verdicts[id] = monitorTripVerdict{okAt: now}
		}
	}
	monitorTrips.stateMu.Unlock()
	if changed {
		saveMonitorStateLocked(now, true)
	}
	monitorTrips.scanMu.Unlock()
}

// ResetAccountMonitorState clears active monitor trips, calibrated caps, real-time counters,
// and optionally drops scanner state (e.g. on OAuth re-auth/import or account deletion).
func ResetAccountMonitorState(accountID string, removeAccount bool) {
	id := strings.ToLower(strings.TrimSpace(accountID))
	if id == "" {
		return
	}
	now := time.Now()
	monitorTrips.scanMu.Lock()
	monitorTrips.stateMu.Lock()
	if removeAccount {
		delete(monitorTrips.scanners, id)
		delete(monitorTrips.verdicts, id)
		delete(monitorTrips.stickyStates, id)
		delete(monitorTrips.stickyCooldowns, id)
	} else {
		if a := monitorTrips.scanners[id]; a != nil {
			a.until = time.Time{}
			a.failAt = time.Time{}
			a.okAt = now
			a.severeFinishTimes = nil
			a.severeCallingTimes = nil
		}
		monitorTrips.verdicts[id] = monitorTripVerdict{okAt: now}
		delete(monitorTrips.stickyStates, id)
		delete(monitorTrips.stickyCooldowns, id)
	}
	monitorTrips.stateMu.Unlock()
	resetAccountMonitorBudget(id, removeAccount)
	saveMonitorStateLocked(now, true)
	monitorTrips.scanMu.Unlock()
}

// PruneMonitorTripAccounts drops scanner cursors and verdicts for accounts that were removed or disabled.
func PruneMonitorTripAccounts(enabledIDs map[string]bool) {
	now := time.Now()
	changed := false
	monitorTrips.scanMu.Lock()
	monitorTrips.stateMu.Lock()
	for id := range monitorTrips.scanners {
		if !enabledIDs[id] {
			delete(monitorTrips.scanners, id)
			changed = true
		}
	}
	for id, v := range monitorTrips.verdicts {
		if !enabledIDs[id] {
			if isMonitorTripped(v.until, v.failAt, v.okAt, now) {
				changed = true
			}
			delete(monitorTrips.verdicts, id)
		}
	}
	for id := range monitorTrips.stickyStates {
		if !enabledIDs[id] {
			delete(monitorTrips.stickyStates, id)
			delete(monitorTrips.stickyCooldowns, id)
		}
	}
	monitorTrips.stateMu.Unlock()
	if changed {
		saveMonitorStateLocked(now, false)
	}
	monitorTrips.scanMu.Unlock()
}

// LoadMonitorStateFromDir configures atomic persistence under stateDir/monitor-trips.json
// and loads any active trips and calibrated caps written before restart.
func LoadMonitorStateFromDir(stateDir string) {
	dir := strings.TrimSpace(stateDir)
	if dir == "" {
		return
	}
	now := time.Now()
	monitorTrips.scanMu.Lock()
	monitorTrips.persistDir = dir
	raw, err := os.ReadFile(filepath.Join(dir, monitorTripsFileName))
	if err != nil {
		monitorTrips.scanMu.Unlock()
		return
	}
	var pf persistedMonitorStateFile
	if json.Unmarshal(raw, &pf) != nil {
		monitorTrips.scanMu.Unlock()
		return
	}
	monitorTrips.stateMu.Lock()
	var tripLiteResets map[string]time.Time
	for id, e := range pf.Trips {
		until, errU := time.Parse(time.RFC3339Nano, strings.TrimSpace(e.Until))
		failAt, errF := time.Parse(time.RFC3339Nano, strings.TrimSpace(e.FailAt))
		if errU != nil || errF != nil {
			continue
		}
		if now.Sub(until) < monitorBudgetWindow && until.Sub(now) <= 6*time.Hour {
			if tripLiteResets == nil {
				tripLiteResets = make(map[string]time.Time)
			}
			if prev, ok := tripLiteResets[id]; !ok || until.After(prev) {
				tripLiteResets[id] = until
			}
		}
		var okAt time.Time
		if strings.TrimSpace(e.OkAt) != "" {
			okAt, _ = time.Parse(time.RFC3339Nano, strings.TrimSpace(e.OkAt))
		}
		if !isMonitorTripped(until, failAt, okAt, now) {
			continue
		}
		a := monitorTrips.scanners[id]
		if a == nil {
			a = &monitorTripAccount{files: make(map[string]*monitorTripFile)}
			monitorTrips.scanners[id] = a
		}
		if until.After(a.until) {
			a.until = until
		}
		if failAt.After(a.failAt) {
			a.failAt = failAt
		}
		if okAt.After(a.okAt) {
			a.okAt = okAt
		}
		monitorTrips.verdicts[id] = monitorTripVerdict{until: a.until, failAt: a.failAt, okAt: a.okAt}
	}
	monitorTrips.stateMu.Unlock()
	loadCalibratedMonitorCaps(pf.CalibratedCaps)
	loadMonitorWindows(pf.Windows, now)
	for id, until := range tripLiteResets {
		recordMonitorLiteResetAtBudget(id, until, now)
	}
	monitorTrips.scanMu.Unlock()
}

// saveMonitorStateLocked atomically persists active trips, calibrated caps, and 5h window boundaries
// to <persistDir>/monitor-trips.json (Version: 2). Caller must hold monitorTrips.scanMu.
func saveMonitorStateLocked(now time.Time, force bool) {
	dir := strings.TrimSpace(monitorTrips.persistDir)
	if dir == "" {
		return
	}
	if !force && !monitorTrips.lastPersistAt.IsZero() && now.Sub(monitorTrips.lastPersistAt) < monitorTripPersistDebounce {
		return
	}
	pf := persistedMonitorStateFile{
		Version:        2,
		UpdatedAt:      now.UTC().Format(time.RFC3339),
		Trips:          make(map[string]persistedMonitorTripEntry),
		CalibratedCaps: snapshotCalibratedMonitorCapsForPersist(),
		Windows:        snapshotMonitorWindowsForPersist(now),
	}
	monitorTrips.stateMu.RLock()
	for id, v := range monitorTrips.verdicts {
		if isMonitorTripped(v.until, v.failAt, v.okAt, now) {
			entry := persistedMonitorTripEntry{
				Until:  v.until.UTC().Format(time.RFC3339Nano),
				FailAt: v.failAt.UTC().Format(time.RFC3339Nano),
			}
			if !v.okAt.IsZero() {
				entry.OkAt = v.okAt.UTC().Format(time.RFC3339Nano)
			}
			pf.Trips[id] = entry
		}
	}
	monitorTrips.stateMu.RUnlock()
	b, err := json.MarshalIndent(pf, "", "  ")
	if err != nil {
		return
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return
	}
	target := filepath.Join(dir, monitorTripsFileName)
	tmp := target + ".tmp." + randomHex(4)
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return
	}
	if err := os.Rename(tmp, target); err != nil {
		_ = os.Remove(tmp)
		return
	}
	monitorTrips.lastPersistAt = now
}

// persistMonitorStateIfConfigured flushes monitor state when persistDir is set.
// Never call while holding monitorBudgets.mu (lock order: monitorTrips.scanMu -> monitorBudgets.mu).
func persistMonitorStateIfConfigured(now time.Time, force bool) {
	if now.IsZero() {
		now = time.Now()
	}
	monitorTrips.scanMu.Lock()
	if strings.TrimSpace(monitorTrips.persistDir) != "" {
		saveMonitorStateLocked(now, force)
	}
	monitorTrips.scanMu.Unlock()
}

// PersistMonitorStateNow flushes monitor trips and calibrated caps immediately if persistence dir is configured.
func PersistMonitorStateNow() {
	now := time.Now()
	monitorTrips.scanMu.Lock()
	saveMonitorStateLocked(now, true)
	monitorTrips.scanMu.Unlock()
}

// parseMonitorTripResetDuration extracts durations such as "reset after 2h29m49s",
// "reset after 2h29m49.5s", "reset after 125.4s", or "reset after 1d2h" from a monitor failure line.
func parseMonitorTripResetDuration(line []byte) (time.Duration, bool) {
	m := monitorTripResetRe.FindSubmatch(line)
	if m == nil || (len(m[1]) == 0 && len(m[2]) == 0 && len(m[3]) == 0 && len(m[4]) == 0) {
		return 0, false
	}
	atoi := func(b []byte) int {
		if len(b) == 0 {
			return 0
		}
		n, _ := strconv.Atoi(string(b))
		return n
	}
	var secDur time.Duration
	if len(m[4]) > 0 {
		if sf, err := strconv.ParseFloat(string(m[4]), 64); err == nil && sf >= 0 {
			secDur = time.Duration(math.Round(sf * float64(time.Second)))
		}
	}
	dur := time.Duration(atoi(m[1]))*24*time.Hour +
		time.Duration(atoi(m[2]))*time.Hour +
		time.Duration(atoi(m[3]))*time.Minute +
		secDur
	return dur, true
}

func isResourceExhaustedLine(line []byte) bool {
	return bytes.Contains(line, markerResExhausted) || bytes.Contains(line, markerResExhaustedUpper)
}

func extractLiteReasonLower(line []byte) string {
	idx := bytes.Index(line, markerLiteReasonPrefix)
	if idx < 0 {
		return ""
	}
	rest := line[idx+len(markerLiteReasonPrefix):]
	end := bytes.IndexByte(rest, '"')
	if end < 0 {
		end = len(rest)
	}
	return strings.ToLower(string(rest[:end]))
}

// isHealthyMonitorFinishLine requires an explicit healthy completion:
// reason="" without Failed:/error/ResourceExhausted, OR a positive safety verdict (triggered=true)
// whose reason contains no error/failure/timeout markers.
func isHealthyMonitorFinishLine(line []byte) bool {
	if bytes.Contains(line, markerLiteFailedTag) || isResourceExhaustedLine(line) {
		return false
	}
	if bytes.Contains(line, markerLiteReasonEmpty) {
		return true
	}
	if bytes.Contains(line, markerLiteTriggeredTrue) {
		reason := extractLiteReasonLower(line)
		for _, bad := range []string{"failed", "error", "timeout", "deadline", "canceled", "cancelled", "exhausted", "429"} {
			if strings.Contains(reason, bad) {
				return false
			}
		}
		return true
	}
	return false
}

// applyLine folds one acp-worker log line into the account verdict inputs (failure with a real reset
// time, or strict evidence that the lite monitor works again). Lines whose glog header fails to parse
// are ignored so mid-line tail fragments can never synthesize phantom timestamps.
func (a *monitorTripAccount) applyLine(line []byte, fileYear int, fileMonth time.Month, loc *time.Location, now time.Time) {
	a.applyLineWithHook(line, fileYear, fileMonth, loc, now, nil)
}

func (a *monitorTripAccount) applyLineWithHook(line []byte, fileYear int, fileMonth time.Month, loc *time.Location, now time.Time, onLiteFinish func(time.Time)) {
	a.applyLineWithHooks(line, fileYear, fileMonth, loc, now, onLiteFinish, nil)
}

func (a *monitorTripAccount) applyLineWithHooks(line []byte, fileYear int, fileMonth time.Month, loc *time.Location, now time.Time, onLiteFinish func(time.Time), onLiteReset func(time.Time)) {
	if bytes.Contains(line, markerSevereFinished) {
		if ts, _, ok := parseGlogHeaderTime(line, fileYear, fileMonth, loc); ok {
			if now.IsZero() || (!ts.Before(now.Add(-monitorStickyEscalationWindow)) && !ts.After(now.Add(time.Minute))) {
				a.severeFinishTimes = append(a.severeFinishTimes, ts)
			}
		}
		return
	}
	if bytes.Contains(line, markerSevereCallingLLM) {
		if ts, _, ok := parseGlogHeaderTime(line, fileYear, fileMonth, loc); ok {
			if now.IsZero() || (!ts.Before(now.Add(-monitorStickyEscalationWindow)) && !ts.After(now.Add(time.Minute))) {
				a.severeCallingTimes = append(a.severeCallingTimes, ts)
			}
		}
		return
	}
	if !bytes.Contains(line, markerLiteFinished) {
		return
	}
	ts, _, ok := parseGlogHeaderTime(line, fileYear, fileMonth, loc)
	if !ok {
		return
	}
	if onLiteFinish != nil {
		onLiteFinish(ts)
	}
	if bytes.Contains(line, markerLiteFailedTag) {
		if !isResourceExhaustedLine(line) {
			return
		}
		dur, okDur := parseMonitorTripResetDuration(line)
		if !okDur || dur < monitorTripMinResetSec*time.Second {
			return
		}
		until := ts.Add(dur)
		if onLiteReset != nil {
			onLiteReset(until)
		}
		if ts.After(a.failAt) {
			a.failAt = ts
		}
		if until.After(a.until) || !a.until.After(now) {
			a.until = until
		}
		return
	}
	if isHealthyMonitorFinishLine(line) {
		if ts.After(a.okAt) {
			a.okAt = ts
		}
	}
}

// discardPartialLine consumes bytes up to and including the first '\n'.
// Returns (bytesDiscarded, foundNewline).
func discardPartialLine(br *bufio.Reader) (int64, bool) {
	var discarded int64
	for {
		frag, err := br.ReadSlice('\n')
		discarded += int64(len(frag))
		if err == nil {
			return discarded, true
		}
		if err == bufio.ErrBufferFull {
			continue
		}
		return discarded, false
	}
}

// scanMonitorTripStream streams complete '\n'-terminated lines from br using bounded memory
// (zero 4 MiB whole-file allocations) and returns exact bytes consumed.
func (a *monitorTripAccount) scanMonitorTripStream(
	br *bufio.Reader,
	skipPartialFirst bool,
	fileYear int,
	fileMonth time.Month,
	loc *time.Location,
	now time.Time,
	onLiteFinish func(time.Time),
	onLiteReset func(time.Time),
) int64 {
	var consumed int64
	if skipPartialFirst {
		n, found := discardPartialLine(br)
		if !found {
			return 0
		}
		consumed += n
	}
	var overflow []byte
	for {
		frag, err := br.ReadSlice('\n')
		if err == nil {
			consumed += int64(len(frag))
			line := frag
			if len(line) > 0 && line[len(line)-1] == '\n' {
				line = line[:len(line)-1]
			}
			if len(line) > 0 && line[len(line)-1] == '\r' {
				line = line[:len(line)-1]
			}
			a.applyLineWithHooks(line, fileYear, fileMonth, loc, now, onLiteFinish, onLiteReset)
			continue
		}
		if err == bufio.ErrBufferFull {
			overflow = append(overflow[:0], frag...)
			lineBytes := int64(len(frag))
			tooLong := len(overflow) > monitorTripMaxLineBytes
			completed := false
			for {
				more, err2 := br.ReadSlice('\n')
				lineBytes += int64(len(more))
				if !tooLong {
					if len(overflow)+len(more) <= monitorTripMaxLineBytes {
						overflow = append(overflow, more...)
					} else {
						tooLong = true
					}
				}
				if err2 == nil {
					completed = true
					break
				}
				if err2 != bufio.ErrBufferFull {
					break
				}
			}
			if !completed {
				break
			}
			consumed += lineBytes
			if !tooLong {
				line := overflow
				if len(line) > 0 && line[len(line)-1] == '\n' {
					line = line[:len(line)-1]
				}
				if len(line) > 0 && line[len(line)-1] == '\r' {
					line = line[:len(line)-1]
				}
				a.applyLineWithHooks(line, fileYear, fileMonth, loc, now, onLiteFinish, onLiteReset)
			}
			continue
		}
		break
	}
	return consumed
}

func (a *monitorTripAccount) scanDir(logDir string, now time.Time, loc *time.Location, br *bufio.Reader, onLiteFinish func(time.Time), onLiteReset func(time.Time)) {
	entries, err := os.ReadDir(logDir)
	if err != nil {
		return
	}
	if br == nil {
		br = bufio.NewReaderSize(nil, 64*1024)
	}
	freshWin := monitorTripFileKeepWindow
	if !a.inited {
		freshWin = monitorTripStartupWindow
	}
	type cand struct {
		name string
		info os.FileInfo
	}
	var cands []cand
	seen := make(map[string]bool, len(entries))
	for _, de := range entries {
		name := de.Name()
		if de.IsDir() || !strings.HasPrefix(name, "acp-worker-") || !strings.HasSuffix(name, ".log") {
			continue
		}
		info, err := de.Info()
		if err != nil || info.Size() == 0 {
			continue
		}
		age := now.Sub(info.ModTime())
		full := filepath.Join(logDir, name)
		if age <= monitorTripFileKeepWindow {
			seen[full] = true
		}
		if age > freshWin {
			continue
		}
		cands = append(cands, cand{name, info})
		seen[full] = true
	}
	sort.Slice(cands, func(i, j int) bool { return cands[i].info.ModTime().Before(cands[j].info.ModTime()) })

	for _, c := range cands {
		full := filepath.Join(logDir, c.name)
		size := c.info.Size()
		tf := a.files[full]
		var start int64
		switch {
		case tf == nil && !a.inited:
			// First scan after (re)start: bounded recent tail across the 6h startup window.
			start = size - monitorTripInitTail
		case tf == nil:
			start = 0
		case size < tf.offset:
			start = 0
		default:
			start = tf.offset
		}
		if start < 0 {
			start = 0
		}
		if size-start > monitorTripMaxRead {
			start = size - monitorTripMaxRead
		}
		if tf != nil && size == tf.offset {
			continue
		}
		skipPartialFirst := start > 0 && (tf == nil || start != tf.offset)

		f, err := os.Open(full)
		if err != nil {
			continue
		}
		if start > 0 {
			if _, errSeek := f.Seek(start, io.SeekStart); errSeek != nil {
				_ = f.Close()
				continue
			}
		}
		br.Reset(f)
		fy, fm := parseAgyWorkerFileDate(c.name, c.info.ModTime().In(loc))
		consumed := a.scanMonitorTripStream(br, skipPartialFirst, fy, fm, loc, now, onLiteFinish, onLiteReset)
		_ = f.Close()
		if consumed > 0 {
			a.files[full] = &monitorTripFile{offset: start + consumed}
		} else if tf == nil {
			a.files[full] = &monitorTripFile{offset: start}
		}
	}
	for p := range a.files {
		if !seen[p] {
			delete(a.files, p)
		}
	}
	a.inited = true
}

// scanMonitorTripAccount refreshes one account's verdict from its log directory.
// Disk I/O runs exclusively under scanMu; stateMu is locked only for the brief verdict publish.
func scanMonitorTripAccount(accountID, logDir string, now time.Time) {
	monitorTrips.scanMu.Lock()
	a := monitorTrips.scanners[accountID]
	if a == nil {
		a = &monitorTripAccount{files: make(map[string]*monitorTripFile)}
		monitorTrips.scanners[accountID] = a
	}
	if monitorTrips.reader == nil {
		monitorTrips.reader = bufio.NewReaderSize(nil, 64*1024)
	}
	prevVerdict := monitorTripVerdict{until: a.until, failAt: a.failAt, okAt: a.okAt}
	wasTripped := isMonitorTripped(a.until, a.failAt, a.okAt, now)

	liteResetChanged := false
	onLiteFinish := func(ts time.Time) {
		recordMonitorLogFinishAt(accountID, ts, now)
	}
	onLiteReset := func(until time.Time) {
		if recordMonitorLiteResetAtBudget(accountID, until, now) {
			liteResetChanged = true
		}
	}
	a.scanDir(logDir, now, time.Local, monitorTrips.reader, onLiteFinish, onLiteReset)

	cutoff := now.Add(-monitorStickyEscalationWindow)
	upper := now.Add(time.Minute)
	a.severeFinishTimes = pruneMonitorTimeWindow(a.severeFinishTimes, cutoff, upper)
	a.severeCallingTimes = pruneMonitorTimeWindow(a.severeCallingTimes, cutoff, upper)
	recentSevere := len(a.severeFinishTimes)
	if len(a.severeCallingTimes) > recentSevere {
		recentSevere = len(a.severeCallingTimes)
	}
	var lastSevereAt time.Time
	for _, ts := range a.severeFinishTimes {
		if ts.After(lastSevereAt) {
			lastSevereAt = ts
		}
	}
	for _, ts := range a.severeCallingTimes {
		if ts.After(lastSevereAt) {
			lastSevereAt = ts
		}
	}

	newVerdict := monitorTripVerdict{until: a.until, failAt: a.failAt, okAt: a.okAt}
	isTripped := isMonitorTripped(a.until, a.failAt, a.okAt, now)
	verdictChanged := !newVerdict.until.Equal(prevVerdict.until) ||
		!newVerdict.failAt.Equal(prevVerdict.failAt) ||
		isTripped != wasTripped

	monitorTrips.stateMu.Lock()
	monitorTrips.verdicts[accountID] = newVerdict
	monitorTrips.stateMu.Unlock()

	calibrated := false
	if isTripped && !wasTripped {
		calibrated = autoCalibrateMonitorCapOnTrip(accountID, now)
	}
	if verdictChanged || calibrated || liteResetChanged {
		saveMonitorStateLocked(now, isTripped != wasTripped || calibrated || liteResetChanged)
	}
	until := a.until
	monitorTrips.scanMu.Unlock()

	recordMonitorStickyEscalationCount(accountID, recentSevere, lastSevereAt, now, isTripped)

	if isTripped && !wasTripped {
		LogEvent("MONITOR_TRIP_ENTER", "agy safety monitor (sir_flash_lite) is capacity-limited; account removed from tool routing until reset", map[string]any{
			"accountId": accountID,
			"until":     until.UTC().Format(time.RFC3339),
		})
	} else if !isTripped && wasTripped {
		LogEvent("MONITOR_TRIP_CLEAR", "agy safety monitor recovered; account back in tool routing", map[string]any{"accountId": accountID})
	}
}

var (
	monitorTripLoopMu   sync.Mutex
	monitorTripStopCh   chan struct{}
	monitorTripDoneCh   chan struct{}
	monitorTripLoopRuns bool
)

func (am *AccountManager) scanAllMonitorTrips(now time.Time) {
	accs := am.ListAccounts()
	enabledIDs := make(map[string]bool, len(accs))
	for _, acc := range accs {
		if !acc.Enabled {
			continue
		}
		enabledIDs[acc.ID] = true
		dir := filepath.Join(am.AccountGeminiDir(acc.ID), "antigravity-cli", "log")
		scanMonitorTripAccount(acc.ID, dir, now)
	}
	PruneMonitorTripAccounts(enabledIDs)
}

// startMonitorTripWatcher launches the background log watcher in the router (non-child) process.
func (am *AccountManager) startMonitorTripWatcher() {
	if testing.Testing() || isChildAccountWorker() || !monitorTripEnabled() {
		return
	}
	am.startMonitorTripWatcherWithInterval(monitorTripScanEvery)
}

func (am *AccountManager) startMonitorTripWatcherWithInterval(interval time.Duration) {
	if interval <= 0 {
		interval = monitorTripScanEvery
	}
	monitorTripLoopMu.Lock()
	if monitorTripLoopRuns {
		monitorTripLoopMu.Unlock()
		return
	}
	stopCh := make(chan struct{})
	doneCh := make(chan struct{})
	monitorTripStopCh = stopCh
	monitorTripDoneCh = doneCh
	monitorTripLoopRuns = true
	monitorTripLoopMu.Unlock()

	go func() {
		defer close(doneCh)
		// Immediate initial scan on startup (eliminates the 3s cold-start blind spot).
		am.scanAllMonitorTrips(time.Now())
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-t.C:
				am.scanAllMonitorTrips(time.Now())
			}
		}
	}()
}

// stopMonitorTripWatcher cleanly halts the monitor trip watcher goroutine if running.
func (am *AccountManager) stopMonitorTripWatcher() {
	monitorTripLoopMu.Lock()
	if !monitorTripLoopRuns {
		monitorTripLoopMu.Unlock()
		return
	}
	stopCh := monitorTripStopCh
	doneCh := monitorTripDoneCh
	monitorTripStopCh = nil
	monitorTripDoneCh = nil
	monitorTripLoopRuns = false
	monitorTripLoopMu.Unlock()
	if stopCh != nil {
		close(stopCh)
	}
	if doneCh != nil {
		<-doneCh
	}
}
