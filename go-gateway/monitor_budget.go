package main

import (
	"math"
	"strings"
	"sync"
	"time"

	gwcfg "agy-acp-gateway-go/internal/config"
)

// Safety Monitor Governor v2 — 3-Zone Predictive Soft Budget.
//
// Tracks per-account 5h `sir_flash_lite` call counts by monotonically reconciling
// real-time MCP tool dispatch counters (`RecordMonitorToolDispatch`), 3s worker log
// finish counts (`recordMonitorLogFinishAt`), and official 5h-window log telemetry
// (`SyncMonitorBudgetCalls` / `SyncMonitorBudgetFromQuota`).
//
// Zones:
//   - "green"  (< softCap):                healthy; new tool sessions spread proactively when AGY_MONITOR_BUDGET_SPREAD=1
//   - "yellow" ([softCap, drainCap)):      soft-drain; keep U8 sticky sessions pinned, route NEW tool sessions to Green peers
//   - "red"    (>= drainCap OR tripped):   monitor-protected; skip for new tool sessions, hop sticky tool sessions if healthy peer exists
const (
	monitorZoneGreen             = "green"
	monitorZoneYellow            = "yellow"
	monitorZoneRed               = "red"
	monitorBudgetWindow          = 5 * time.Hour
	monitorBudgetBucketSec       = 300 // 5-minute buckets
	monitorBudgetNumBuckets      = 60  // 60 * 5m = 5 hours
	monitorAutoCalibrateMinCalls = 500
	monitorSoftCapRatio          = 0.60
	monitorDrainCapRatio         = 0.80
)

// MonitorBudgetSnapshot is the JSON-safe 3-zone budget status exposed in /api/dashboard,
// /api/accounts, and AccountHealthView.
type MonitorBudgetSnapshot struct {
	Calls5h   int    `json:"calls5h"`
	SoftCap   int    `json:"softCap"`
	DrainCap  int    `json:"drainCap"`
	HardCap   int    `json:"hardCap"`
	Zone      string `json:"zone"`
	Tripped   bool   `json:"tripped"`
	TripUntil string `json:"tripUntil,omitempty"`
}

type monitorBudgetBucket struct {
	slot  int64
	count int
}

type monitorBudgetRing struct {
	buckets [monitorBudgetNumBuckets]monitorBudgetBucket
}

func (r *monitorBudgetRing) add(at time.Time, n int) {
	if n <= 0 || at.IsZero() {
		return
	}
	slot := at.Unix() / monitorBudgetBucketSec
	idx := int(slot % monitorBudgetNumBuckets)
	if idx < 0 {
		idx += monitorBudgetNumBuckets
	}
	if r.buckets[idx].slot != slot {
		r.buckets[idx] = monitorBudgetBucket{slot: slot, count: n}
		return
	}
	r.buckets[idx].count += n
}

func (r *monitorBudgetRing) sumSince(minUnix, nowUnix int64) int {
	if minUnix <= 0 {
		minUnix = nowUnix - int64(monitorBudgetWindow.Seconds())
	}
	minSlot := minUnix / monitorBudgetBucketSec
	maxSlot := nowUnix / monitorBudgetBucketSec
	total := 0
	for _, b := range r.buckets {
		if b.count > 0 && b.slot >= minSlot && b.slot <= maxSlot {
			total += b.count
		}
	}
	return total
}

func (r *monitorBudgetRing) pruneBefore(minUnix int64) {
	minSlot := minUnix / monitorBudgetBucketSec
	for i, b := range r.buckets {
		if b.slot < minSlot {
			r.buckets[i] = monitorBudgetBucket{}
		}
	}
}

func (r *monitorBudgetRing) clear() {
	r.buckets = [monitorBudgetNumBuckets]monitorBudgetBucket{}
}

type calibratedMonitorCap struct {
	HardCap      int
	SoftCap      int
	DrainCap     int
	CalibratedAt time.Time
}

type monitorBudgetAccount struct {
	windowKey     string
	windowStartAt time.Time
	windowResetAt time.Time
	logCalls5h    int
	rtRing        monitorBudgetRing
	scanRing      monitorBudgetRing
	calibrated    *calibratedMonitorCap
}

var monitorBudgets = struct {
	mu    sync.RWMutex
	accts map[string]*monitorBudgetAccount
}{
	accts: make(map[string]*monitorBudgetAccount),
}

func monitorBudgetSpreadEnabled() bool {
	return gwcfg.Current().MonitorBudgetSpread
}

func normalizeMonitorCaps(soft, drain, hard int) (int, int, int) {
	if hard < 100 {
		hard = 100
	}
	if drain < 2 {
		drain = 2
	}
	if drain > hard {
		drain = hard
	}
	if soft < 1 {
		soft = 1
	}
	if soft > drain {
		soft = drain
	}
	return soft, drain, hard
}

func defaultMonitorCapsForTier(planTier string) (soft, drain, hard int) {
	cfg := gwcfg.Current()
	if normalizePlanTierHint(planTier) == "pro" {
		return normalizeMonitorCaps(cfg.MonitorSoftCapPro, cfg.MonitorDrainCapPro, cfg.MonitorHardCapPro)
	}
	return normalizeMonitorCaps(cfg.MonitorSoftCapUltra, cfg.MonitorDrainCapUltra, cfg.MonitorHardCapUltra)
}

func calibratedCapsForHard(hard int) (soft, drain, hardOut int) {
	if hard < monitorAutoCalibrateMinCalls {
		hard = monitorAutoCalibrateMinCalls
	}
	soft = int(math.Round(float64(hard) * monitorSoftCapRatio))
	drain = int(math.Round(float64(hard) * monitorDrainCapRatio))
	if drain >= hard {
		drain = hard - 1
	}
	if soft >= drain {
		soft = drain - 1
	}
	return normalizeMonitorCaps(soft, drain, hard)
}

func ensureMonitorBudgetAccountLocked(id string) *monitorBudgetAccount {
	a := monitorBudgets.accts[id]
	if a == nil {
		a = &monitorBudgetAccount{}
		monitorBudgets.accts[id] = a
	}
	return a
}

// currentProcessAccountID returns the account ID served by the current process
// (child worker's InstanceID, or primary account's ID in the router process).
func currentProcessAccountID() string {
	if isChildAccountWorker() {
		id := strings.ToLower(strings.TrimSpace(gwcfg.Current().InstanceID))
		if id != "" {
			return id
		}
		return "primary"
	}
	if GlobalAccountManager != nil && GlobalAccountManager.HasConfiguredAccounts() {
		if prim, ok := GlobalAccountManager.GetPrimaryAccount(); ok && prim.ID != "" {
			return prim.ID
		}
	}
	id := strings.ToLower(strings.TrimSpace(gwcfg.Current().InstanceID))
	if id != "" {
		return id
	}
	return "primary"
}

// RecordMonitorToolDispatch increments the real-time in-memory safety monitor dispatch counter
// for accountID (called when MCP_TOOL_PARKED or ws_await_result fires).
func RecordMonitorToolDispatch(accountID string, n int) {
	RecordMonitorToolDispatchAt(accountID, n, time.Now())
}

// RecordMonitorToolDispatchAt records n monitor tool dispatches at an explicit timestamp.
func RecordMonitorToolDispatchAt(accountID string, n int, at time.Time) {
	id := strings.ToLower(strings.TrimSpace(accountID))
	if id == "" || n <= 0 {
		return
	}
	if at.IsZero() {
		at = time.Now()
	}
	monitorBudgets.mu.Lock()
	a := ensureMonitorBudgetAccountLocked(id)
	if !a.windowResetAt.IsZero() && !at.Before(a.windowResetAt) {
		// 5h window rolled over before the next /usage poll arrived.
		a.logCalls5h = 0
		a.rtRing.pruneBefore(a.windowResetAt.Unix())
		a.scanRing.pruneBefore(a.windowResetAt.Unix())
		a.windowStartAt = a.windowResetAt
		a.windowResetAt = time.Time{}
		a.windowKey = ""
	}
	a.rtRing.add(at, n)
	monitorBudgets.mu.Unlock()
}

// recordMonitorLogFinishAt folds one parsed `Monitor sir_flash_lite FINISHED` line from worker logs
// into the account's 5h sliding counter (keeps router budgets fresh within 3s for primary & children).
func recordMonitorLogFinishAt(accountID string, ts, now time.Time) {
	id := strings.ToLower(strings.TrimSpace(accountID))
	if id == "" || ts.IsZero() {
		return
	}
	if now.Sub(ts) > monitorBudgetWindow+10*time.Minute || ts.Sub(now) > 5*time.Minute {
		return
	}
	monitorBudgets.mu.Lock()
	a := ensureMonitorBudgetAccountLocked(id)
	a.scanRing.add(ts, 1)
	monitorBudgets.mu.Unlock()
}

// SyncMonitorBudgetCalls reconciles log-derived 5h monitor calls against real-time counters.
// Within the same 5h window (same windowKey), reconciliation is monotonic (max); when the
// official 5h window rolls over (windowKey changes), counters reset cleanly for the new window.
func SyncMonitorBudgetCalls(accountID string, logCalls int, windowKey string, now time.Time) {
	id := strings.ToLower(strings.TrimSpace(accountID))
	if id == "" {
		return
	}
	if logCalls < 0 {
		logCalls = 0
	}
	if now.IsZero() {
		now = time.Now()
	}
	wk := strings.TrimSpace(windowKey)
	var resetAt, startAt time.Time
	if wk != "" {
		if parsed, err := time.Parse(time.RFC3339, wk); err == nil {
			resetAt = parsed
			startAt = parsed.Add(-monitorBudgetWindow)
		}
	}

	monitorBudgets.mu.Lock()
	a := ensureMonitorBudgetAccountLocked(id)
	if wk != "" && wk != a.windowKey {
		prevKey := a.windowKey
		a.windowKey = wk
		a.windowResetAt = resetAt
		a.windowStartAt = startAt
		if prevKey != "" {
			// Window rolled over to a fresh 5h bucket!
			a.logCalls5h = logCalls
			if !startAt.IsZero() {
				a.rtRing.pruneBefore(startAt.Unix())
				a.scanRing.pruneBefore(startAt.Unix())
			} else {
				a.rtRing.clear()
				a.scanRing.clear()
			}
		} else {
			// First official windowKey observed: trim pre-window buckets and reconcile monotonically.
			if !startAt.IsZero() {
				a.rtRing.pruneBefore(startAt.Unix())
				a.scanRing.pruneBefore(startAt.Unix())
			}
			if logCalls > a.logCalls5h {
				a.logCalls5h = logCalls
			}
		}
	} else {
		if !a.windowResetAt.IsZero() && !now.Before(a.windowResetAt) {
			a.logCalls5h = logCalls
			a.rtRing.pruneBefore(a.windowResetAt.Unix())
			a.scanRing.pruneBefore(a.windowResetAt.Unix())
			a.windowStartAt = a.windowResetAt
			a.windowResetAt = time.Time{}
			a.windowKey = ""
		} else if logCalls > a.logCalls5h {
			a.logCalls5h = logCalls
		}
	}
	monitorBudgets.mu.Unlock()
}

// SyncMonitorBudgetFromQuota extracts the Gemini 5h window reset time and log MonitorCalls5h
// from a QuotaSnapshot and reconciles it into accountID's 3-zone budget state.
func SyncMonitorBudgetFromQuota(accountID string, quota *QuotaSnapshot, now time.Time) {
	if quota == nil || quota.Deduction == nil {
		return
	}
	geminiGroup := findGroup(quota.Groups, reGeminiModelsGroup)
	if geminiGroup == nil {
		geminiGroup = findGroup(quota.Groups, reGeminiFallbackGroup)
	}
	windowKey := ""
	if geminiGroup != nil {
		if b5h := findBucket(geminiGroup, "5h"); b5h != nil {
			windowKey = strings.TrimSpace(b5h.ResetTime)
		}
	}
	if windowKey == "" && quota.Deduction.MonitorCalls5h == 0 {
		return
	}
	SyncMonitorBudgetCalls(accountID, quota.Deduction.MonitorCalls5h, windowKey, now)
}

func (a *monitorBudgetAccount) effectiveCallsLocked(now time.Time) int {
	if a == nil {
		return 0
	}
	nowUnix := now.Unix()
	if !a.windowResetAt.IsZero() && !now.Before(a.windowResetAt) {
		minUnix := a.windowResetAt.Unix()
		rt := a.rtRing.sumSince(minUnix, nowUnix)
		sc := a.scanRing.sumSince(minUnix, nowUnix)
		if rt > sc {
			return rt
		}
		return sc
	}
	minUnix := nowUnix - int64(monitorBudgetWindow.Seconds())
	if !a.windowStartAt.IsZero() && a.windowStartAt.Unix() > minUnix {
		minUnix = a.windowStartAt.Unix() - quotaWindowStartSkewSec
	}
	calls := a.logCalls5h
	if rt := a.rtRing.sumSince(minUnix, nowUnix); rt > calls {
		calls = rt
	}
	if sc := a.scanRing.sumSince(minUnix, nowUnix); sc > calls {
		calls = sc
	}
	return calls
}

func (a *monitorBudgetAccount) effectiveCapsLocked(planTier string) (soft, drain, hard int) {
	soft, drain, hard = defaultMonitorCapsForTier(planTier)
	if a != nil && a.calibrated != nil && a.calibrated.HardCap >= monitorAutoCalibrateMinCalls && a.calibrated.HardCap < hard {
		return normalizeMonitorCaps(a.calibrated.SoftCap, a.calibrated.DrainCap, a.calibrated.HardCap)
	}
	return soft, drain, hard
}

// EvaluateMonitorBudget computes the current 3-zone budget snapshot for one account.
func EvaluateMonitorBudget(acc AccountProfile, now time.Time) MonitorBudgetSnapshot {
	return evaluateMonitorBudgetByIDTier(acc.ID, acc.PlanTierHint, now)
}

func evaluateMonitorBudgetByIDTier(accountID, planTier string, now time.Time) MonitorBudgetSnapshot {
	id := strings.ToLower(strings.TrimSpace(accountID))
	if now.IsZero() {
		now = time.Now()
	}
	tripUntil := MonitorTripUntil(id, now)
	tripped := !tripUntil.IsZero()

	monitorBudgets.mu.RLock()
	a := monitorBudgets.accts[id]
	calls := a.effectiveCallsLocked(now)
	soft, drain, hard := a.effectiveCapsLocked(planTier)
	monitorBudgets.mu.RUnlock()

	zone := monitorZoneGreen
	switch {
	case tripped || calls >= drain:
		zone = monitorZoneRed
	case calls >= soft:
		zone = monitorZoneYellow
	}
	snap := MonitorBudgetSnapshot{
		Calls5h:  calls,
		SoftCap:  soft,
		DrainCap: drain,
		HardCap:  hard,
		Zone:     zone,
		Tripped:  tripped,
	}
	if tripped {
		snap.TripUntil = tripUntil.UTC().Format(time.RFC3339)
	}
	return snap
}

// MonitorUtilizationRatio returns calls5h / hardCap in [0, +inf) (or 1.0 when tripped).
func MonitorUtilizationRatio(accountID string, now time.Time) float64 {
	id := strings.ToLower(strings.TrimSpace(accountID))
	if id == "" {
		return 0
	}
	tier := ""
	if GlobalAccountManager != nil && GlobalAccountManager.HasConfiguredAccounts() {
		if acc, ok := GlobalAccountManager.GetAccount(id); ok {
			tier = acc.PlanTierHint
		}
	}
	if tier == "" {
		tier = authoritativeAccountPlanTierHint()
	}
	snap := evaluateMonitorBudgetByIDTier(id, tier, now)
	if snap.Tripped {
		return 1.0
	}
	if snap.HardCap <= 0 {
		return 0
	}
	return float64(snap.Calls5h) / float64(snap.HardCap)
}

// MonitorRemainingRatio returns max(0, (hardCap - calls5h) / hardCap) in [0, 1] (0 when tripped).
func (s MonitorBudgetSnapshot) RemainingRatio() float64 {
	if s.Tripped || s.HardCap <= 0 || s.Calls5h >= s.HardCap {
		return 0
	}
	rem := float64(s.HardCap-s.Calls5h) / float64(s.HardCap)
	if rem < 0 {
		return 0
	}
	if rem > 1 {
		return 1
	}
	return rem
}

// SnapshotMonitorBudgetsForAccounts returns 3-zone budget snapshots keyed by account ID.
func SnapshotMonitorBudgetsForAccounts(accs []AccountProfile, now time.Time) map[string]MonitorBudgetSnapshot {
	if now.IsZero() {
		now = time.Now()
	}
	out := make(map[string]MonitorBudgetSnapshot, len(accs))
	for _, a := range accs {
		out[a.ID] = EvaluateMonitorBudget(a, now)
	}
	return out
}

// SnapshotMonitorBudgets returns 3-zone budget snapshots for all configured (or tracked) accounts.
func SnapshotMonitorBudgets(now time.Time) map[string]MonitorBudgetSnapshot {
	if now.IsZero() {
		now = time.Now()
	}
	if GlobalAccountManager != nil && GlobalAccountManager.HasConfiguredAccounts() {
		return SnapshotMonitorBudgetsForAccounts(GlobalAccountManager.ListAccounts(), now)
	}
	monitorBudgets.mu.RLock()
	ids := make([]string, 0, len(monitorBudgets.accts))
	for id := range monitorBudgets.accts {
		ids = append(ids, id)
	}
	monitorBudgets.mu.RUnlock()
	out := make(map[string]MonitorBudgetSnapshot, len(ids))
	for _, id := range ids {
		out[id] = evaluateMonitorBudgetByIDTier(id, "", now)
	}
	return out
}

// autoCalibrateMonitorCapOnTrip lowers an account's learned hardCap (with soft=60%, drain=80%)
// when a genuine sir_flash_lite 429 lockout occurs below its current hardCap and calls5h >= 500.
func autoCalibrateMonitorCapOnTrip(accountID string, now time.Time) bool {
	id := strings.ToLower(strings.TrimSpace(accountID))
	if id == "" {
		return false
	}
	tier := ""
	if GlobalAccountManager != nil && GlobalAccountManager.HasConfiguredAccounts() {
		if acc, ok := GlobalAccountManager.GetAccount(id); ok {
			tier = acc.PlanTierHint
		}
	}
	monitorBudgets.mu.Lock()
	a := ensureMonitorBudgetAccountLocked(id)
	calls := a.effectiveCallsLocked(now)
	_, _, curHard := a.effectiveCapsLocked(tier)
	if calls < monitorAutoCalibrateMinCalls || calls >= curHard {
		monitorBudgets.mu.Unlock()
		return false
	}
	newSoft, newDrain, newHard := calibratedCapsForHard(calls)
	a.calibrated = &calibratedMonitorCap{
		HardCap:      newHard,
		SoftCap:      newSoft,
		DrainCap:     newDrain,
		CalibratedAt: now.UTC(),
	}
	monitorBudgets.mu.Unlock()
	LogEvent("MONITOR_BUDGET_CALIBRATED", "Auto-calibrated per-account safety monitor budget from real 429 lockout", map[string]any{
		"accountId": id,
		"calls5h":   calls,
		"softCap":   newSoft,
		"drainCap":  newDrain,
		"hardCap":   newHard,
	})
	return true
}

func loadCalibratedMonitorCaps(caps map[string]persistedMonitorCapEntry) {
	if len(caps) == 0 {
		return
	}
	monitorBudgets.mu.Lock()
	for id, c := range caps {
		if c.HardCap < monitorAutoCalibrateMinCalls {
			continue
		}
		soft, drain, hard := c.SoftCap, c.DrainCap, c.HardCap
		if soft <= 0 || drain <= 0 || soft > drain || drain > hard {
			soft, drain, hard = calibratedCapsForHard(c.HardCap)
		} else {
			soft, drain, hard = normalizeMonitorCaps(soft, drain, hard)
		}
		calAt, _ := time.Parse(time.RFC3339Nano, strings.TrimSpace(c.CalibratedAt))
		a := ensureMonitorBudgetAccountLocked(id)
		a.calibrated = &calibratedMonitorCap{
			HardCap:      hard,
			SoftCap:      soft,
			DrainCap:     drain,
			CalibratedAt: calAt,
		}
	}
	monitorBudgets.mu.Unlock()
}

func snapshotCalibratedMonitorCapsForPersist() map[string]persistedMonitorCapEntry {
	monitorBudgets.mu.RLock()
	out := make(map[string]persistedMonitorCapEntry)
	for id, a := range monitorBudgets.accts {
		if a == nil || a.calibrated == nil || a.calibrated.HardCap < monitorAutoCalibrateMinCalls {
			continue
		}
		entry := persistedMonitorCapEntry{
			HardCap:  a.calibrated.HardCap,
			SoftCap:  a.calibrated.SoftCap,
			DrainCap: a.calibrated.DrainCap,
		}
		if !a.calibrated.CalibratedAt.IsZero() {
			entry.CalibratedAt = a.calibrated.CalibratedAt.UTC().Format(time.RFC3339Nano)
		}
		out[id] = entry
	}
	monitorBudgets.mu.RUnlock()
	if len(out) == 0 {
		return nil
	}
	return out
}

func resetAccountMonitorBudget(accountID string, removeAccount bool) {
	id := strings.ToLower(strings.TrimSpace(accountID))
	if id == "" {
		return
	}
	monitorBudgets.mu.Lock()
	if removeAccount {
		delete(monitorBudgets.accts, id)
	} else {
		monitorBudgets.accts[id] = &monitorBudgetAccount{}
	}
	monitorBudgets.mu.Unlock()
}

func resetMonitorBudgetsForTest() {
	monitorBudgets.mu.Lock()
	monitorBudgets.accts = make(map[string]*monitorBudgetAccount)
	monitorBudgets.mu.Unlock()
}
