package main

import (
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gwcfg "agy-acp-gateway-go/internal/config"
)

// Safety Monitor Governor v3 — 3-Zone Predictive Soft Budget + Smart Pacing + Presets.
//
// Tracks per-account 5h `sir_flash_lite` call counts by monotonically reconciling
// real-time MCP tool dispatch counters (`RecordMonitorToolDispatch`), 3s worker log
// finish counts (`recordMonitorLogFinishAt`), and official 5h-window log telemetry
// (`SyncMonitorBudgetCalls` / `SyncMonitorBudgetFromQuota`).
//
// Zones:
//   - "green"  (< effectiveSoftCap):                     healthy; new tool sessions spread proactively when AGY_MONITOR_BUDGET_SPREAD=1
//   - "yellow" ([effectiveSoftCap, effectiveDrainCap)):  soft-drain; keep U8 sticky sessions pinned, route NEW tool sessions to Green peers
//   - "red"    (>= effectiveDrainCap OR tripped):        monitor-protected; stop NEW tool sessions; keep sticky sessions pinned by default unless real 429 tripped, MonitorSoftRedHop=true, or emergency cap reached with same-tier peer
const (
	monitorZoneGreen             = "green"
	monitorZoneYellow            = "yellow"
	monitorZoneRed               = "red"
	monitorBudgetWindow          = 5 * time.Hour
	monitorBudgetBucketSec       = 10   // 10-second buckets for sharp 5h reset boundary isolation
	monitorBudgetNumBuckets      = 1860 // 1860 * 10s = 5h10m (wrap-safe margin for 5h window)
	monitorAutoCalibrateMinCalls = 500
	monitorSoftCapRatio          = 0.60
	monitorDrainCapRatio         = 0.80

	MonitorPresetConservative = "conservative"
	MonitorPresetBalanced     = "balanced"
	MonitorPresetEdge         = "edge"
	MonitorPresetCustom       = "custom"
)

// MonitorBudgetSnapshot is the JSON-safe 3-zone budget status exposed in /api/dashboard,
// /api/accounts, and AccountHealthView.
type MonitorBudgetSnapshot struct {
	Calls5h           int     `json:"calls5h"`
	SoftCap           int     `json:"softCap"`
	DrainCap          int     `json:"drainCap"`
	HardCap           int     `json:"hardCap"`
	EffectiveSoftCap  int     `json:"effectiveSoftCap,omitempty"`
	EffectiveDrainCap int     `json:"effectiveDrainCap,omitempty"`
	SmartPacingActive bool    `json:"smartPacingActive,omitempty"`
	Preset            string  `json:"preset,omitempty"`
	Zone              string  `json:"zone"`
	Tripped           bool    `json:"tripped"`
	TripUntil         string  `json:"tripUntil,omitempty"`
	BurnRatePerHr     float64 `json:"burnRatePerHr,omitempty"`
	EtaSoftCapSec     int     `json:"etaSoftCapSec,omitempty"`
	EtaDrainCapSec    int     `json:"etaDrainCapSec,omitempty"`
}

// MonitorPolicy is the JSON-safe runtime governor policy propagated to child account workers
// via POST /api/internal/monitor-policy.
type MonitorPolicy struct {
	Preset            string `json:"monitorPreset,omitempty"`
	SmartPacing       *bool  `json:"monitorSmartPacing,omitempty"`
	SoftRedHop        *bool  `json:"monitorSoftRedHop,omitempty"`
	SoftCapUltra      int    `json:"monitorSoftCapUltra,omitempty"`
	DrainCapUltra     int    `json:"monitorDrainCapUltra,omitempty"`
	HardCapUltra      int    `json:"monitorHardCapUltra,omitempty"`
	SoftCapPro        int    `json:"monitorSoftCapPro,omitempty"`
	DrainCapPro       int    `json:"monitorDrainCapPro,omitempty"`
	HardCapPro        int    `json:"monitorHardCapPro,omitempty"`
	IgnoreBurnAnomaly *bool  `json:"ignoreBurnAnomaly,omitempty"`
}

type runtimeMonitorPolicy struct {
	preset        string
	smartPacing   bool
	softRedHop    bool
	softCapUltra  int
	drainCapUltra int
	hardCapUltra  int
	softCapPro    int
	drainCapPro   int
	hardCapPro    int
}

var (
	runtimeMonitorPolicyPtr  atomic.Pointer[runtimeMonitorPolicy]
	runtimeIgnoreBurnAnomaly atomic.Int32 // 0=unset, 1=false, 2=true
)

func setRuntimeIgnoreBurnAnomaly(v bool) {
	if v {
		runtimeIgnoreBurnAnomaly.Store(2)
	} else {
		runtimeIgnoreBurnAnomaly.Store(1)
	}
}

func runtimeIgnoreBurnAnomalyOverride() (bool, bool) {
	switch runtimeIgnoreBurnAnomaly.Load() {
	case 2:
		return true, true
	case 1:
		return false, true
	default:
		return false, false
	}
}

func normalizeMonitorPreset(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case MonitorPresetConservative:
		return MonitorPresetConservative
	case MonitorPresetEdge:
		return MonitorPresetEdge
	case MonitorPresetCustom:
		return MonitorPresetCustom
	default:
		return MonitorPresetBalanced
	}
}

func isValidMonitorPreset(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", MonitorPresetConservative, MonitorPresetBalanced, MonitorPresetEdge, MonitorPresetCustom:
		return true
	default:
		return false
	}
}

func isConservativeMonitorTier(planTier string) bool {
	tier := normalizePlanTierHint(planTier)
	return tier == "pro" || tier == "ultra_5x"
}

func presetMonitorCaps(preset, planTier string) (soft, drain, hard int) {
	isPro := isConservativeMonitorTier(planTier)
	switch normalizeMonitorPreset(preset) {
	case MonitorPresetConservative:
		if isPro {
			return 1800, 2600, 3400
		}
		return 2800, 3800, 4800
	case MonitorPresetEdge:
		if isPro {
			return 2300, 3100, 3400
		}
		return 3600, 4450, 4800
	default: // balanced (and fallback for custom)
		if isPro {
			return 2100, 2900, 3400
		}
		return 3300, 4250, 4800
	}
}

func defaultRuntimeMonitorPolicy() *runtimeMonitorPolicy {
	us, ud, uh := presetMonitorCaps(MonitorPresetBalanced, "ultra")
	ps, pd, ph := presetMonitorCaps(MonitorPresetBalanced, "pro")
	return &runtimeMonitorPolicy{
		preset:        MonitorPresetBalanced,
		smartPacing:   true,
		softRedHop:    false,
		softCapUltra:  us,
		drainCapUltra: ud,
		hardCapUltra:  uh,
		softCapPro:    ps,
		drainCapPro:   pd,
		hardCapPro:    ph,
	}
}

func buildRuntimeMonitorPolicy(c AccountPoolConfig) *runtimeMonitorPolicy {
	preset := normalizeMonitorPreset(c.MonitorPreset)
	smartPacing := c.MonitorSmartPacingEnabled()
	softRedHop := c.MonitorSoftRedHopEnabled()
	var us, ud, uh, ps, pd, ph int
	if preset == MonitorPresetCustom {
		bus, bud, buh := presetMonitorCaps(MonitorPresetBalanced, "ultra")
		us, ud, uh = c.MonitorSoftCapUltra, c.MonitorDrainCapUltra, c.MonitorHardCapUltra
		if us <= 0 {
			us = bus
		}
		if ud <= 0 {
			ud = bud
		}
		if uh <= 0 {
			uh = buh
		}
		us, ud, uh = normalizeMonitorCaps(us, ud, uh)

		bps, bpd, bph := presetMonitorCaps(MonitorPresetBalanced, "pro")
		ps, pd, ph = c.MonitorSoftCapPro, c.MonitorDrainCapPro, c.MonitorHardCapPro
		if ps <= 0 {
			ps = bps
		}
		if pd <= 0 {
			pd = bpd
		}
		if ph <= 0 {
			ph = bph
		}
		ps, pd, ph = normalizeMonitorCaps(ps, pd, ph)
	} else {
		us, ud, uh = presetMonitorCaps(preset, "ultra")
		ps, pd, ph = presetMonitorCaps(preset, "pro")
	}
	return &runtimeMonitorPolicy{
		preset:        preset,
		smartPacing:   smartPacing,
		softRedHop:    softRedHop,
		softCapUltra:  us,
		drainCapUltra: ud,
		hardCapUltra:  uh,
		softCapPro:    ps,
		drainCapPro:   pd,
		hardCapPro:    ph,
	}
}

func currentRuntimeMonitorPolicy() *runtimeMonitorPolicy {
	if p := runtimeMonitorPolicyPtr.Load(); p != nil {
		return p
	}
	return defaultRuntimeMonitorPolicy()
}

// SetRuntimeMonitorPolicy publishes the active AccountPoolConfig's monitor governor policy
// into lock-free process runtime state (used by EvaluateMonitorBudget and toolRelayIntervalForTool).
func SetRuntimeMonitorPolicy(cfg AccountPoolConfig) {
	runtimeMonitorPolicyPtr.Store(buildRuntimeMonitorPolicy(cfg))
}

// MonitorPolicyFromRouting converts an AccountPoolConfig into the internal propagation payload.
func MonitorPolicyFromRouting(cfg AccountPoolConfig, ignoreBurn *bool) MonitorPolicy {
	apiCfg := cfg.ForAPI()
	sp := apiCfg.MonitorSmartPacingEnabled()
	srh := apiCfg.MonitorSoftRedHopEnabled()
	pol := MonitorPolicy{
		Preset:            apiCfg.EffectiveMonitorPreset(),
		SmartPacing:       &sp,
		SoftRedHop:        &srh,
		IgnoreBurnAnomaly: ignoreBurn,
	}
	if pol.Preset == MonitorPresetCustom {
		pol.SoftCapUltra = apiCfg.MonitorSoftCapUltra
		pol.DrainCapUltra = apiCfg.MonitorDrainCapUltra
		pol.HardCapUltra = apiCfg.MonitorHardCapUltra
		pol.SoftCapPro = apiCfg.MonitorSoftCapPro
		pol.DrainCapPro = apiCfg.MonitorDrainCapPro
		pol.HardCapPro = apiCfg.MonitorHardCapPro
	}
	return pol
}

// SetRuntimeMonitorPolicyFromInternal applies a policy pushed by the primary router to a child worker.
func SetRuntimeMonitorPolicyFromInternal(pol MonitorPolicy) {
	if pol.IgnoreBurnAnomaly != nil {
		setRuntimeIgnoreBurnAnomaly(*pol.IgnoreBurnAnomaly)
	}
	hasPolicyField := strings.TrimSpace(pol.Preset) != "" ||
		pol.SmartPacing != nil ||
		pol.SoftRedHop != nil ||
		pol.SoftCapUltra > 0 || pol.DrainCapUltra > 0 || pol.HardCapUltra > 0 ||
		pol.SoftCapPro > 0 || pol.DrainCapPro > 0 || pol.HardCapPro > 0
	if !hasPolicyField {
		return
	}
	cur := currentRuntimeMonitorPolicy()
	curSP := cur.smartPacing
	curSRH := cur.softRedHop
	cfg := AccountPoolConfig{
		MonitorPreset:        cur.preset,
		MonitorSmartPacing:   &curSP,
		MonitorSoftRedHop:    &curSRH,
		MonitorSoftCapUltra:  cur.softCapUltra,
		MonitorDrainCapUltra: cur.drainCapUltra,
		MonitorHardCapUltra:  cur.hardCapUltra,
		MonitorSoftCapPro:    cur.softCapPro,
		MonitorDrainCapPro:   cur.drainCapPro,
		MonitorHardCapPro:    cur.hardCapPro,
	}
	if strings.TrimSpace(pol.Preset) != "" {
		cfg.MonitorPreset = pol.Preset
	}
	if pol.SmartPacing != nil {
		cfg.MonitorSmartPacing = pol.SmartPacing
	}
	if pol.SoftRedHop != nil {
		cfg.MonitorSoftRedHop = pol.SoftRedHop
	}
	if pol.SoftCapUltra > 0 {
		cfg.MonitorSoftCapUltra = pol.SoftCapUltra
	}
	if pol.DrainCapUltra > 0 {
		cfg.MonitorDrainCapUltra = pol.DrainCapUltra
	}
	if pol.HardCapUltra > 0 {
		cfg.MonitorHardCapUltra = pol.HardCapUltra
	}
	if pol.SoftCapPro > 0 {
		cfg.MonitorSoftCapPro = pol.SoftCapPro
	}
	if pol.DrainCapPro > 0 {
		cfg.MonitorDrainCapPro = pol.DrainCapPro
	}
	if pol.HardCapPro > 0 {
		cfg.MonitorHardCapPro = pol.HardCapPro
	}
	SetRuntimeMonitorPolicy(cfg)
}

func currentMonitorPolicySnapshot() MonitorPolicy {
	pol := currentRuntimeMonitorPolicy()
	cfg := gwcfg.Current()
	us, ud, uh := resolveMonitorCapsFromPolicy(pol, "ultra", cfg)
	ps, pd, ph := resolveMonitorCapsFromPolicy(pol, "pro", cfg)
	sp := pol.smartPacing
	srh := pol.softRedHop
	out := MonitorPolicy{
		Preset:        pol.preset,
		SmartPacing:   &sp,
		SoftRedHop:    &srh,
		SoftCapUltra:  us,
		DrainCapUltra: ud,
		HardCapUltra:  uh,
		SoftCapPro:    ps,
		DrainCapPro:   pd,
		HardCapPro:    ph,
	}
	if v, ok := runtimeIgnoreBurnAnomalyOverride(); ok {
		out.IgnoreBurnAnomaly = &v
	}
	return out
}

func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
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

func (r *monitorBudgetRing) earliestSlotUnix(minUnix, nowUnix int64) int64 {
	if minUnix <= 0 {
		minUnix = nowUnix - int64(monitorBudgetWindow.Seconds())
	}
	minSlot := minUnix / monitorBudgetBucketSec
	maxSlot := nowUnix / monitorBudgetBucketSec
	var earliestSlot int64
	found := false
	for _, b := range r.buckets {
		if b.count > 0 && b.slot >= minSlot && b.slot <= maxSlot {
			if !found || b.slot < earliestSlot {
				earliestSlot = b.slot
				found = true
			}
		}
	}
	if !found {
		return 0
	}
	return earliestSlot * monitorBudgetBucketSec
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
	liteResetAt   time.Time
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

func resolveMonitorCapsFromPolicy(pol *runtimeMonitorPolicy, planTier string, cfg *gwcfg.Snapshot) (soft, drain, hard int) {
	if pol == nil {
		pol = defaultRuntimeMonitorPolicy()
	}
	isPro := isConservativeMonitorTier(planTier)
	if isPro {
		soft, drain, hard = pol.softCapPro, pol.drainCapPro, pol.hardCapPro
	} else {
		soft, drain, hard = pol.softCapUltra, pol.drainCapUltra, pol.hardCapUltra
	}
	if cfg != nil {
		if isPro {
			if cfg.MonitorSoftCapProEnvSet {
				soft = cfg.MonitorSoftCapPro
			}
			if cfg.MonitorDrainCapProEnvSet {
				drain = cfg.MonitorDrainCapPro
			}
			if cfg.MonitorHardCapProEnvSet {
				hard = cfg.MonitorHardCapPro
			}
		} else {
			if cfg.MonitorSoftCapUltraEnvSet {
				soft = cfg.MonitorSoftCapUltra
			}
			if cfg.MonitorDrainCapUltraEnvSet {
				drain = cfg.MonitorDrainCapUltra
			}
			if cfg.MonitorHardCapUltraEnvSet {
				hard = cfg.MonitorHardCapUltra
			}
		}
	}
	return normalizeMonitorCaps(soft, drain, hard)
}

func resolveMonitorCapsForConfig(c AccountPoolConfig, planTier string) (soft, drain, hard int) {
	return resolveMonitorCapsFromPolicy(buildRuntimeMonitorPolicy(c), planTier, gwcfg.Current())
}

func defaultMonitorCapsForTier(planTier string) (soft, drain, hard int) {
	return resolveMonitorCapsFromPolicy(currentRuntimeMonitorPolicy(), planTier, gwcfg.Current())
}

func activeMonitorCapRatiosForTier(planTier string) (softRatio, drainRatio float64) {
	pol := currentRuntimeMonitorPolicy()
	cfg := gwcfg.Current()
	isPro := isConservativeMonitorTier(planTier)
	hasEnvOverride := cfg != nil && ((isPro && (cfg.MonitorSoftCapProEnvSet || cfg.MonitorDrainCapProEnvSet || cfg.MonitorHardCapProEnvSet)) ||
		(!isPro && (cfg.MonitorSoftCapUltraEnvSet || cfg.MonitorDrainCapUltraEnvSet || cfg.MonitorHardCapUltraEnvSet)))
	if pol.preset == MonitorPresetCustom || hasEnvOverride {
		s, d, h := resolveMonitorCapsFromPolicy(pol, planTier, cfg)
		if h > 0 {
			sr := float64(s) / float64(h)
			dr := float64(d) / float64(h)
			if sr > 0 && sr <= dr && dr <= 1.0 {
				return sr, dr
			}
		}
	}
	switch pol.preset {
	case MonitorPresetConservative:
		return monitorSoftCapRatio, monitorDrainCapRatio
	case MonitorPresetEdge:
		if isPro {
			return 2300.0 / 3400.0, 3100.0 / 3400.0
		}
		return 3600.0 / 4800.0, 4450.0 / 4800.0
	default: // balanced
		if isPro {
			return 2100.0 / 3400.0, 2900.0 / 3400.0
		}
		return 3300.0 / 4800.0, 4250.0 / 4800.0
	}
}

func calibratedCapsForHard(hard int) (soft, drain, hardOut int) {
	return calibratedCapsForHardTier(hard, "")
}

func calibratedCapsForHardTier(hard int, planTier string) (soft, drain, hardOut int) {
	if hard < monitorAutoCalibrateMinCalls {
		hard = monitorAutoCalibrateMinCalls
	}
	softRatio, drainRatio := activeMonitorCapRatiosForTier(planTier)
	soft = int(math.Round(float64(hard) * softRatio))
	drain = int(math.Round(float64(hard) * drainRatio))
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
	liteFuture := !a.liteResetAt.IsZero() && a.liteResetAt.After(at)
	if !liteFuture && !a.windowResetAt.IsZero() && !at.Before(a.windowResetAt) {
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

// recordMonitorLiteResetAtBudget records an authoritative sir_flash_lite 5h reset boundary
// parsed from a real 429 ("reset after XhYmZs") log line without acquiring monitorTrips.scanMu.
// Returns true if the account's liteResetAt advanced.
func recordMonitorLiteResetAtBudget(accountID string, until, now time.Time) bool {
	id := strings.ToLower(strings.TrimSpace(accountID))
	if id == "" || until.IsZero() {
		return false
	}
	if now.IsZero() {
		now = time.Now()
	}
	if now.Sub(until) >= monitorBudgetWindow || until.Sub(now) > 6*time.Hour {
		return false
	}
	u := until.UTC()
	monitorBudgets.mu.Lock()
	a := ensureMonitorBudgetAccountLocked(id)
	if !a.liteResetAt.IsZero() && !u.After(a.liteResetAt) {
		monitorBudgets.mu.Unlock()
		return false
	}
	a.liteResetAt = u
	monitorBudgets.mu.Unlock()
	return true
}

// RecordMonitorLiteResetAt records an authoritative sir_flash_lite 5h reset boundary
// and persists monitor window state if changed. Never call while holding monitorTrips.scanMu.
func RecordMonitorLiteResetAt(accountID string, until, now time.Time) bool {
	if now.IsZero() {
		now = time.Now()
	}
	changed := recordMonitorLiteResetAtBudget(accountID, until, now)
	if changed {
		persistMonitorStateIfConfigured(now, true)
	}
	return changed
}

// recordMonitorLogFinishAt folds one parsed `Monitor sir_flash_lite FINISHED` line from worker logs
// into the account's 5h sliding counter (keeps router budgets fresh within 3s for primary & children).
func recordMonitorLogFinishAt(accountID string, ts, now time.Time) {
	id := strings.ToLower(strings.TrimSpace(accountID))
	if id == "" || ts.IsZero() {
		return
	}
	if now.Sub(ts) >= monitorBudgetWindow || ts.Sub(now) > 5*time.Minute {
		return
	}
	monitorBudgets.mu.Lock()
	a := ensureMonitorBudgetAccountLocked(id)
	a.scanRing.add(ts, 1)
	monitorBudgets.mu.Unlock()
}

// SyncMonitorBudgetCalls reconciles log-derived 5h monitor calls against real-time counters.
// Within the same 5h window (same windowKey), reconciliation is monotonic (max); when the
// official 5h window rolls over (windowKey changes), counters reset cleanly for the new window
// unless an authoritative future sir_flash_lite reset boundary (liteResetAt > now) still protects
// the current lite window from premature zeroing.
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
			resetAt = parsed.UTC()
			startAt = resetAt.Add(-monitorBudgetWindow)
		}
	}

	windowChanged := false
	logChanged := false
	monitorBudgets.mu.Lock()
	a := ensureMonitorBudgetAccountLocked(id)
	liteFuture := !a.liteResetAt.IsZero() && a.liteResetAt.After(now)

	// Guard 1: Ignore out-of-order stale polls carrying an older reset timestamp than our active window.
	if !resetAt.IsZero() && !a.windowResetAt.IsZero() && resetAt.Before(a.windowResetAt) {
		monitorBudgets.mu.Unlock()
		return
	}

	// Guard 2: Incoming windowKey is already expired (resetAt <= now) — e.g. stale-while-revalidate
	// cached /usage returned right after a 5h rollover. Never stamp the expired window's old logCalls
	// onto the new window or re-install the expired windowKey over a rolled-over account!
	if !resetAt.IsZero() && !now.Before(resetAt) {
		if liteFuture {
			liteStart := a.liteResetAt.Add(-monitorBudgetWindow)
			a.rtRing.pruneBefore(liteStart.Unix())
			a.scanRing.pruneBefore(liteStart.Unix())
			if (a.windowKey == "" || a.windowKey == wk) && logCalls > a.logCalls5h {
				a.logCalls5h = logCalls
				logChanged = true
			}
		} else {
			if a.logCalls5h != 0 {
				a.logCalls5h = 0
				logChanged = true
			}
			a.rtRing.pruneBefore(resetAt.Unix())
			a.scanRing.pruneBefore(resetAt.Unix())
			if a.windowStartAt.IsZero() || resetAt.After(a.windowStartAt) {
				a.windowStartAt = resetAt
				windowChanged = true
			}
			if !a.windowResetAt.IsZero() && !now.Before(a.windowResetAt) {
				a.windowResetAt = time.Time{}
				windowChanged = true
			}
			if a.windowKey != "" && a.windowKey == wk {
				a.windowKey = ""
				windowChanged = true
			}
		}
		monitorBudgets.mu.Unlock()
		if windowChanged || logChanged {
			persistMonitorStateIfConfigured(now, windowChanged)
		}
		return
	}

	if wk != "" && wk != a.windowKey {
		prevKey := a.windowKey
		prevStartAt := a.windowStartAt
		a.windowKey = wk
		a.windowResetAt = resetAt
		a.windowStartAt = startAt
		windowChanged = true
		if liteFuture && !startAt.IsZero() && startAt.Before(a.liteResetAt) {
			// Reverse 20m drift: Gemini rolled over before sir_flash_lite's known future reset.
			// Do NOT prematurely zero logCalls5h or prune rings past liteResetAt - 5h.
			liteStart := a.liteResetAt.Add(-monitorBudgetWindow)
			a.rtRing.pruneBefore(liteStart.Unix())
			a.scanRing.pruneBefore(liteStart.Unix())
			if logCalls > a.logCalls5h {
				a.logCalls5h = logCalls
				logChanged = true
			}
		} else if prevKey != "" || (!prevStartAt.IsZero() && !startAt.IsZero() && startAt.Sub(prevStartAt) > time.Minute) {
			// Window rolled over to a fresh 5h bucket (even if prevKey was already cleared on expiry)!
			if a.logCalls5h != logCalls {
				logChanged = true
			}
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
				logChanged = true
			}
		}
	} else {
		if !a.windowResetAt.IsZero() && !now.Before(a.windowResetAt) {
			if liteFuture {
				liteStart := a.liteResetAt.Add(-monitorBudgetWindow)
				a.rtRing.pruneBefore(liteStart.Unix())
				a.scanRing.pruneBefore(liteStart.Unix())
				if logCalls > a.logCalls5h {
					a.logCalls5h = logCalls
					logChanged = true
				}
			} else {
				// Window expired and no fresh future windowKey was provided (wk == ""):
				// incoming logCalls is a rolling-5h sum containing pre-reset calls, so reset cleanly to 0!
				if a.logCalls5h != 0 {
					logChanged = true
				}
				a.logCalls5h = 0
				a.rtRing.pruneBefore(a.windowResetAt.Unix())
				a.scanRing.pruneBefore(a.windowResetAt.Unix())
				a.windowStartAt = a.windowResetAt
				a.windowResetAt = time.Time{}
				a.windowKey = ""
				windowChanged = true
			}
		} else if logCalls > a.logCalls5h {
			a.logCalls5h = logCalls
			logChanged = true
		}
	}
	monitorBudgets.mu.Unlock()

	if windowChanged || logChanged {
		persistMonitorStateIfConfigured(now, windowChanged)
	}
}

// syncMonitorBudgetIdle handles an idle /usage 5h bucket (0% used, resetTime ≈ readAt + 5h).
// It anchors windowStartAt without overwriting windowKey with a drifting reset timestamp that
// would trigger spurious rollovers and prune freshly dispatched rtRing calls.
func syncMonitorBudgetIdle(accountID string, logCalls int, readAt, now time.Time) {
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
	if readAt.IsZero() {
		readAt = now
	}
	readAt = readAt.UTC()

	windowChanged := false
	logChanged := false
	monitorBudgets.mu.Lock()
	a := ensureMonitorBudgetAccountLocked(id)
	liteFuture := !a.liteResetAt.IsZero() && a.liteResetAt.After(now)
	if liteFuture {
		if logCalls > a.logCalls5h {
			a.logCalls5h = logCalls
			logChanged = true
		}
	} else {
		nowUnix := now.Unix()
		hadActiveWindow := a.windowKey != "" || !a.windowResetAt.IsZero()
		prevResetAt := a.windowResetAt
		if hadActiveWindow {
			a.windowKey = ""
			a.windowResetAt = time.Time{}
			windowChanged = true
			if !prevResetAt.IsZero() && readAt.After(prevResetAt) && a.rtRing.sumSince(prevResetAt.Unix(), nowUnix) > 0 {
				a.windowStartAt = prevResetAt
			} else {
				a.windowStartAt = readAt
			}
			if a.logCalls5h != logCalls {
				a.logCalls5h = logCalls
				logChanged = true
			}
		} else if a.windowStartAt.IsZero() {
			earliest := a.rtRing.earliestSlotUnix(now.Add(-monitorBudgetWindow).Unix(), nowUnix)
			if earliest > 0 && earliest < readAt.Unix() {
				a.windowStartAt = time.Unix(earliest, 0).UTC()
			} else {
				a.windowStartAt = readAt
			}
			windowChanged = true
			if logCalls > a.logCalls5h {
				a.logCalls5h = logCalls
				logChanged = true
			}
		} else if a.rtRing.sumSince(a.windowStartAt.Unix(), nowUnix) > 0 {
			// Fresh rtRing dispatches exist since our idle anchor: keep anchor intact and reconcile monotonically.
			if logCalls > a.logCalls5h {
				a.logCalls5h = logCalls
				logChanged = true
			}
		} else {
			if !a.windowStartAt.Equal(readAt) {
				a.windowStartAt = readAt
				windowChanged = true
			}
			if a.logCalls5h != logCalls {
				a.logCalls5h = logCalls
				logChanged = true
			}
		}
	}
	monitorBudgets.mu.Unlock()

	if windowChanged || logChanged {
		persistMonitorStateIfConfigured(now, false)
	}
}

// SyncMonitorBudgetFromQuota extracts the Gemini 5h window reset time and log MonitorCalls5h
// from a QuotaSnapshot and reconciles it into accountID's 3-zone budget state.
func SyncMonitorBudgetFromQuota(accountID string, quota *QuotaSnapshot, now time.Time) {
	if quota == nil || quota.Deduction == nil {
		return
	}
	if now.IsZero() {
		now = time.Now()
	}
	geminiGroup := findGroup(quota.Groups, reGeminiModelsGroup)
	if geminiGroup == nil {
		geminiGroup = findGroup(quota.Groups, reGeminiFallbackGroup)
	}
	var b5h *QuotaBucket
	windowKey := ""
	if geminiGroup != nil {
		if b := findBucket(geminiGroup, "5h"); b != nil {
			b5h = b
			windowKey = strings.TrimSpace(b.ResetTime)
		}
	}
	if b5h != nil && quotaWindowIdle(b5h, quota.UpdatedAt, monitorBudgetWindow) {
		readAt, err := time.Parse(time.RFC3339, strings.TrimSpace(quota.UpdatedAt))
		if err != nil {
			readAt = now
		}
		syncMonitorBudgetIdle(accountID, quota.Deduction.MonitorCalls5h, readAt, now)
		return
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

	// Case 1: Authoritative sir_flash_lite reset boundary is in (now - 5h, now].
	if !a.liteResetAt.IsZero() && !a.liteResetAt.After(now) && now.Sub(a.liteResetAt) < monitorBudgetWindow {
		if !a.windowResetAt.IsZero() && !now.Before(a.windowResetAt) {
			effectiveStart := a.liteResetAt
			if a.windowResetAt.After(effectiveStart) {
				effectiveStart = a.windowResetAt
			}
			minUnix := effectiveStart.Unix()
			rt := a.rtRing.sumSince(minUnix, nowUnix)
			sc := a.scanRing.sumSince(minUnix, nowUnix)
			if rt > sc {
				return rt
			}
			return sc
		}
		if a.windowStartAt.IsZero() || a.windowStartAt.Before(a.liteResetAt) {
			// 20m drift case: sir_flash_lite already reset at liteResetAt while Gemini /usage
			// still reports an older windowStartAt < liteResetAt. Do NOT fold stale pre-reset logCalls5h.
			minUnix := a.liteResetAt.Unix()
			rt := a.rtRing.sumSince(minUnix, nowUnix)
			sc := a.scanRing.sumSince(minUnix, nowUnix)
			if rt > sc {
				return rt
			}
			return sc
		}
		// Gemini window rolled over at or after liteResetAt: resume max(logCalls5h, rtRing, scanRing).
		minUnix := a.windowStartAt.Unix() - quotaWindowStartSkewSec
		if minUnix < a.liteResetAt.Unix() {
			minUnix = a.liteResetAt.Unix()
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

	// Case 2: Authoritative sir_flash_lite reset is still in the future (liteResetAt > now).
	// Protects calls5h when Gemini /usage resets earlier than sir_flash_lite.
	if !a.liteResetAt.IsZero() && a.liteResetAt.After(now) {
		minUnix := a.liteResetAt.Add(-monitorBudgetWindow).Unix()
		if rolling := nowUnix - int64(monitorBudgetWindow.Seconds()); rolling > minUnix {
			minUnix = rolling
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

	// Case 3: Normal / no active liteResetAt within 5h.
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

func (a *monitorBudgetAccount) activeWindowFloorUnixLocked(now time.Time) int64 {
	nowUnix := now.Unix()
	rolling := nowUnix - int64(monitorBudgetWindow.Seconds())
	if a == nil {
		return rolling
	}
	if !a.liteResetAt.IsZero() && !a.liteResetAt.After(now) && now.Sub(a.liteResetAt) < monitorBudgetWindow {
		floor := a.liteResetAt.Unix()
		if !a.windowResetAt.IsZero() && !now.Before(a.windowResetAt) && a.windowResetAt.Unix() > floor {
			floor = a.windowResetAt.Unix()
		} else if !a.windowStartAt.IsZero() && !a.windowStartAt.Before(a.liteResetAt) {
			ws := a.windowStartAt.Unix() - quotaWindowStartSkewSec
			if ws > floor {
				floor = ws
			}
		}
		return floor
	}
	if !a.liteResetAt.IsZero() && a.liteResetAt.After(now) {
		floor := a.liteResetAt.Add(-monitorBudgetWindow).Unix()
		if rolling > floor {
			return rolling
		}
		return floor
	}
	if !a.windowResetAt.IsZero() && !now.Before(a.windowResetAt) {
		return a.windowResetAt.Unix()
	}
	if !a.windowStartAt.IsZero() && a.windowStartAt.Unix() > rolling {
		return a.windowStartAt.Unix() - quotaWindowStartSkewSec
	}
	return rolling
}

// recentBurnRatePerHrLocked computes the active monitor burn rate (calls/hr) from the last 15m
// (falling back to 30m) of 10s ring buckets within the active 5h window.
func (a *monitorBudgetAccount) recentBurnRatePerHrLocked(now time.Time) float64 {
	if a == nil {
		return 0
	}
	nowUnix := now.Unix()
	winFloor := a.activeWindowFloorUnixLocked(now)
	calc := func(lookbackSec int64) float64 {
		minU := nowUnix - lookbackSec
		if winFloor > minU {
			minU = winFloor
		}
		if minU > nowUnix {
			return 0
		}
		rtC := a.rtRing.sumSince(minU, nowUnix)
		scC := a.scanRing.sumSince(minU, nowUnix)
		c := rtC
		if scC > c {
			c = scC
		}
		if c <= 0 {
			return 0
		}
		span := nowUnix - minU
		if span < 60 {
			span = 60
		}
		if span > lookbackSec {
			span = lookbackSec
		}
		return roundTo(float64(c)*3600.0/float64(span), 1)
	}
	if r15 := calc(15 * 60); r15 > 0 {
		return r15
	}
	return calc(30 * 60)
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
	return evaluateMonitorBudgetByIDTier(acc.ID, acc.EffectivePlanTier(), now)
}

func evaluateMonitorBudgetByIDTier(accountID, planTier string, now time.Time) MonitorBudgetSnapshot {
	id := strings.ToLower(strings.TrimSpace(accountID))
	if now.IsZero() {
		now = time.Now()
	}
	tripUntil := MonitorTripUntil(id, now)
	tripped := !tripUntil.IsZero()
	pol := currentRuntimeMonitorPolicy()

	monitorBudgets.mu.RLock()
	a := monitorBudgets.accts[id]
	calls := a.effectiveCallsLocked(now)
	baseSoft, baseDrain, hard := a.effectiveCapsLocked(planTier)
	burnRate := a.recentBurnRatePerHrLocked(now)
	var resetAt time.Time
	liteFuture := false
	if a != nil {
		if !a.liteResetAt.IsZero() && a.liteResetAt.After(now) {
			resetAt = a.liteResetAt
			liteFuture = true
		} else if !a.windowResetAt.IsZero() && a.windowResetAt.After(now) {
			resetAt = a.windowResetAt
		}
	}
	monitorBudgets.mu.RUnlock()

	effSoft := baseSoft
	effDrain := baseDrain
	smartPacingActive := false
	if !tripped && pol.smartPacing && !resetAt.IsZero() && resetAt.After(now) {
		remTime := resetAt.Sub(now)
		if remTime > 0 && remTime <= monitorBudgetWindow+time.Hour {
			remEff := remTime
			if !liteFuture {
				remEff += 20 * time.Minute
			}
			margin := int(math.Round(float64(hard) * 0.07))
			if margin < 300 {
				margin = 300
			}
			safeCap := hard - margin
			if safeCap < baseDrain {
				safeCap = baseDrain
			}
			safeRate := burnRate
			if math.IsNaN(safeRate) || math.IsInf(safeRate, 0) || safeRate < 0 {
				safeRate = 0
			}
			effRate := math.Max(safeRate, 600.0)
			projF := math.Ceil(effRate * remEff.Hours() * 1.5)
			projNeed := hard
			if !math.IsNaN(projF) && !math.IsInf(projF, 0) && projF < float64(hard) {
				projNeed = int(projF)
				if projNeed < 0 {
					projNeed = 0
				}
			}
			effDrain = clampInt(safeCap-projNeed, baseDrain, safeCap)
			effSoft = clampInt(effDrain-(baseDrain-baseSoft), baseSoft, effDrain)
			smartPacingActive = effDrain > baseDrain
		}
	}

	zone := monitorZoneGreen
	switch {
	case tripped || calls >= effDrain:
		zone = monitorZoneRed
	case calls >= effSoft:
		zone = monitorZoneYellow
	}
	snap := MonitorBudgetSnapshot{
		Calls5h:           calls,
		SoftCap:           baseSoft,
		DrainCap:          baseDrain,
		HardCap:           hard,
		EffectiveSoftCap:  effSoft,
		EffectiveDrainCap: effDrain,
		SmartPacingActive: smartPacingActive,
		Preset:            pol.preset,
		Zone:              zone,
		Tripped:           tripped,
		BurnRatePerHr:     burnRate,
	}
	if !tripped && burnRate > 0 {
		if calls < effSoft {
			eta := int(math.Round(float64(effSoft-calls) / burnRate * 3600.0))
			if eta < 1 {
				eta = 1
			}
			snap.EtaSoftCapSec = eta
		}
		if calls < effDrain {
			eta := int(math.Round(float64(effDrain-calls) / burnRate * 3600.0))
			if eta < 1 {
				eta = 1
			}
			snap.EtaDrainCapSec = eta
		}
	}
	if tripped {
		snap.TripUntil = tripUntil.UTC().Format(time.RFC3339)
	}
	return snap
}

// currentProcessMonitorBudget returns the 3-zone monitor budget snapshot for the account served
// by the current process (resolving its EffectivePlanTier from GlobalAccountManager or env fallback).
func currentProcessMonitorBudget(now time.Time) MonitorBudgetSnapshot {
	id := currentProcessAccountID()
	tier := ""
	if GlobalAccountManager != nil && GlobalAccountManager.HasConfiguredAccounts() {
		if acc, ok := GlobalAccountManager.GetAccount(id); ok {
			tier = acc.EffectivePlanTier()
		}
	}
	if tier == "" {
		tier = authoritativeAccountPlanTierHint()
	}
	return evaluateMonitorBudgetByIDTier(id, tier, now)
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
			tier = acc.EffectivePlanTier()
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

// autoCalibrateMonitorCapOnTrip lowers an account's learned hardCap
// when a genuine sir_flash_lite 429 lockout occurs below its current hardCap and calls5h >= 500.
func autoCalibrateMonitorCapOnTrip(accountID string, now time.Time) bool {
	id := strings.ToLower(strings.TrimSpace(accountID))
	if id == "" {
		return false
	}
	tier := ""
	if GlobalAccountManager != nil && GlobalAccountManager.HasConfiguredAccounts() {
		if acc, ok := GlobalAccountManager.GetAccount(id); ok {
			tier = acc.EffectivePlanTier()
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
	newSoft, newDrain, newHard := calibratedCapsForHardTier(calls, tier)
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

func loadMonitorWindows(windows map[string]persistedMonitorWindowEntry, now time.Time) {
	if len(windows) == 0 {
		return
	}
	if now.IsZero() {
		now = time.Now()
	}
	monitorBudgets.mu.Lock()
	for rawID, w := range windows {
		id := strings.ToLower(strings.TrimSpace(rawID))
		if id == "" {
			continue
		}
		var startAt, resetAt, liteResetAt time.Time
		if s := strings.TrimSpace(w.WindowStartAt); s != "" {
			if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
				startAt = t.UTC()
			}
		}
		if s := strings.TrimSpace(w.WindowResetAt); s != "" {
			if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
				resetAt = t.UTC()
			}
		}
		if s := strings.TrimSpace(w.LiteResetAt); s != "" {
			if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
				liteResetAt = t.UTC()
			}
		}
		wk := strings.TrimSpace(w.WindowKey)
		if resetAt.IsZero() && wk != "" {
			if t, err := time.Parse(time.RFC3339, wk); err == nil {
				resetAt = t.UTC()
				if startAt.IsZero() {
					startAt = resetAt.Add(-monitorBudgetWindow)
				}
			}
		}

		a := ensureMonitorBudgetAccountLocked(id)
		if !liteResetAt.IsZero() && now.Sub(liteResetAt) < monitorBudgetWindow && liteResetAt.Sub(now) <= 6*time.Hour {
			if a.liteResetAt.IsZero() || liteResetAt.After(a.liteResetAt) {
				a.liteResetAt = liteResetAt
			}
		}

		if !resetAt.IsZero() {
			if now.Before(resetAt) {
				a.windowKey = wk
				a.windowResetAt = resetAt
				if !startAt.IsZero() {
					a.windowStartAt = startAt
				} else {
					a.windowStartAt = resetAt.Add(-monitorBudgetWindow)
				}
				if w.LogCalls5h > a.logCalls5h {
					a.logCalls5h = w.LogCalls5h
				}
			} else if now.Sub(resetAt) < monitorBudgetWindow {
				// Official window rolled over while the gateway was down: anchor windowStartAt
				// at the reset boundary so startup log scans skip pre-reset lines at t=0s!
				if a.windowStartAt.IsZero() || resetAt.After(a.windowStartAt) {
					a.windowStartAt = resetAt
				}
				a.windowResetAt = time.Time{}
				a.windowKey = ""
			}
		} else if !startAt.IsZero() && now.Sub(startAt) < monitorBudgetWindow && !startAt.After(now.Add(5*time.Minute)) {
			if a.windowStartAt.IsZero() || startAt.After(a.windowStartAt) {
				a.windowStartAt = startAt
			}
			if w.LogCalls5h > a.logCalls5h {
				a.logCalls5h = w.LogCalls5h
			}
		}
	}
	monitorBudgets.mu.Unlock()
}

func snapshotMonitorWindowsForPersist(now time.Time) map[string]persistedMonitorWindowEntry {
	if now.IsZero() {
		now = time.Now()
	}
	monitorBudgets.mu.RLock()
	out := make(map[string]persistedMonitorWindowEntry)
	for id, a := range monitorBudgets.accts {
		if a == nil {
			continue
		}
		var entry persistedMonitorWindowEntry
		hasAny := false
		if !a.windowResetAt.IsZero() && now.Before(a.windowResetAt) {
			entry.WindowKey = a.windowKey
			entry.WindowResetAt = a.windowResetAt.UTC().Format(time.RFC3339Nano)
			if !a.windowStartAt.IsZero() {
				entry.WindowStartAt = a.windowStartAt.UTC().Format(time.RFC3339Nano)
			}
			if a.logCalls5h > 0 {
				entry.LogCalls5h = a.logCalls5h
			}
			hasAny = true
		} else if !a.windowResetAt.IsZero() && now.Sub(a.windowResetAt) < monitorBudgetWindow {
			entry.WindowStartAt = a.windowResetAt.UTC().Format(time.RFC3339Nano)
			hasAny = true
		} else if !a.windowStartAt.IsZero() && now.Sub(a.windowStartAt) < monitorBudgetWindow {
			entry.WindowStartAt = a.windowStartAt.UTC().Format(time.RFC3339Nano)
			if a.logCalls5h > 0 {
				entry.LogCalls5h = a.logCalls5h
			}
			hasAny = true
		}
		if !a.liteResetAt.IsZero() && now.Sub(a.liteResetAt) < monitorBudgetWindow && a.liteResetAt.Sub(now) <= 6*time.Hour {
			entry.LiteResetAt = a.liteResetAt.UTC().Format(time.RFC3339Nano)
			hasAny = true
		}
		if hasAny {
			out[id] = entry
		}
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
	runtimeMonitorPolicyPtr.Store(nil)
	runtimeIgnoreBurnAnomaly.Store(0)
}
