package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"agy-acp-gateway-go/internal/acp"
	gwcfg "agy-acp-gateway-go/internal/config"
)

var (
	quotaRefreshing atomic.Bool
	lastGatewayTurn atomic.Int64
)

func NotifyGatewayTurnCompleted() {
	lastGatewayTurn.Store(time.Now().Unix())
}

// Quota is read via `<agy> -p /usage`; prefer the official CLI when present, else the patched binary
func quotaAgyBin() string {
	if v := gwcfg.Current().QuotaBin; v != "" {
		return v
	}
	if home, err := os.UserHomeDir(); err == nil {
		if p := filepath.Join(home, ".local", "bin", "agy"); fileExists(p) {
			return p
		}
	}
	return PatchedAgyBin
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

type QuotaBucket struct {
	ID                string  `json:"id"`
	Name              string  `json:"name"`
	Description       string  `json:"description,omitempty"`
	Window            string  `json:"window"`
	RemainingFraction float64 `json:"remainingFraction"`
	UsedFraction      float64 `json:"usedFraction"`
	ResetTime         string  `json:"resetTime"`
}

type QuotaGroup struct {
	Name        string        `json:"name"`
	Description string        `json:"description"`
	Buckets     []QuotaBucket `json:"buckets"`
}

// QuotaBurnAnomaly reports an unexplained divergence between official pool USD burn and local+monitor
// expected spend (e.g., upstream billing multiplier regression or unattributed external usage).
type QuotaBurnAnomaly struct {
	Kind        string  `json:"kind"`     // "multiplier_anomaly" | "unattributed_burn"
	Severity    string  `json:"severity"` // "warn" | "critical"
	PoolUsedUsd float64 `json:"poolUsedUsd"`
	ExpectedUsd float64 `json:"expectedUsd"`
	Ratio       float64 `json:"ratio"`
	AgyVersion  string  `json:"agyVersion,omitempty"`
	Detail      string  `json:"detail,omitempty"`
}

// QuotaDeduction converts the official pool fractions into dollars and runnable tokens. The pool
// fields are nil whenever the matching official bucket is unknown (read failure, group not enabled
// or window missing), so the dashboard never presents an unknown quota as a full pool.
type QuotaDeduction struct {
	Cap5hUsd                       float64           `json:"cap5hUsd"`
	CapWeeklyUsd                   float64           `json:"capWeeklyUsd"`
	PoolUsed5hUsd                  *float64          `json:"poolUsed5hUsd"`
	PoolRem5hUsd                   *float64          `json:"poolRem5hUsd"`
	LocalWsl5hUsd                  float64           `json:"localWsl5hUsd"`
	LocalWin5hUsd                  float64           `json:"localWin5hUsd"`
	LocalClaudeGw5hUsd             float64           `json:"localClaudeGw5hUsd"`
	LocalWslHost5hUsd              float64           `json:"localWslHost5hUsd"`
	LocalTotal5hUsd                float64           `json:"localTotal5hUsd"`
	LocalSteps5h                   int               `json:"localSteps5h"`
	MonitorCalls5h                 int               `json:"monitorCalls5h,omitempty"`
	MonitorEscalated5h             int               `json:"monitorEscalated5h,omitempty"`
	MonitorEst5hUsd                float64           `json:"monitorEst5hUsd,omitempty"`
	BurnAnomaly5h                  *QuotaBurnAnomaly `json:"burnAnomaly5h,omitempty"`
	PoolUsedWeeklyUsd              *float64          `json:"poolUsedWeeklyUsd"`
	PoolRemWeeklyUsd               *float64          `json:"poolRemWeeklyUsd"`
	LocalWslWeeklyUsd              float64           `json:"localWslWeeklyUsd"`
	LocalWinWeeklyUsd              float64           `json:"localWinWeeklyUsd"`
	LocalClaudeGwWeeklyUsd         float64           `json:"localClaudeGwWeeklyUsd"`
	LocalWslHostWeeklyUsd          float64           `json:"localWslHostWeeklyUsd"`
	LocalTotalWeeklyUsd            float64           `json:"localTotalWeeklyUsd"`
	LocalStepsWeekly               int               `json:"localStepsWeekly"`
	MonitorCallsWeekly             int               `json:"monitorCallsWeekly,omitempty"`
	MonitorEscalatedWeekly         int               `json:"monitorEscalatedWeekly,omitempty"`
	MonitorEstWeeklyUsd            float64           `json:"monitorEstWeeklyUsd,omitempty"`
	BurnAnomalyWeekly              *QuotaBurnAnomaly `json:"burnAnomalyWeekly,omitempty"`
	Rem5hTokensAt99PctCache        *int64            `json:"rem5hTokensAt99PctCache"`
	Rem5hTokensAt0PctCache         *int64            `json:"rem5hTokensAt0PctCache"`
	RemWeeklyTokensAt99PctCache    *int64            `json:"remWeeklyTokensAt99PctCache"`
	RemWeeklyTokensAt0PctCache     *int64            `json:"remWeeklyTokensAt0PctCache"`
	EffectiveRemUsd                *float64          `json:"effectiveRemUsd"`
	Bottleneck                     string            `json:"bottleneck,omitempty"`
	RemEffectiveTokensAt99PctCache *int64            `json:"remEffectiveTokensAt99PctCache"`
	RemEffectiveTokensAt0PctCache  *int64            `json:"remEffectiveTokensAt0PctCache"`
}

// Pool quota states shown by the dashboard.
const (
	QuotaStateKnown      = "known"       // fresh official reading
	QuotaStateStale      = "stale"       // last good reading, older than quotaStaleAfter or followed by a failed refresh
	QuotaStateError      = "error"       // no reading at all (first /usage call failed)
	QuotaStateLoading    = "loading"     // initial background /usage refresh in flight
	QuotaStateNotEnabled = "not_enabled" // /usage answered but this model group (or every window) is absent
)

const quotaStaleAfter = 30 * time.Minute

// PoolQuotaState tells the dashboard how far the pool numbers can be trusted.
type PoolQuotaState struct {
	State     string `json:"state"`
	UpdatedAt string `json:"updatedAt,omitempty"`
	Error     string `json:"error,omitempty"`
}

type ExtraPoolQuota struct {
	GroupName         string   `json:"groupName"`
	State             string   `json:"state"`
	UpdatedAt         string   `json:"updatedAt,omitempty"`
	Window            string   `json:"window"`
	RemainingFraction *float64 `json:"remainingFraction,omitempty"`
	UsedFraction      *float64 `json:"usedFraction,omitempty"`
	ResetTime         string   `json:"resetTime,omitempty"`
}

type QuotaSnapshot struct {
	Status             string          `json:"status"` // ok | stale (cached reading after a failed refresh) | error
	UpdatedAt          string          `json:"updatedAt"`
	PlanTier           string          `json:"planTier,omitempty"`           // "pro" | "ultra"
	PlanLabel          string          `json:"planLabel,omitempty"`          // "AI Pro" | "Ultra 20x"
	SummaryDescription string          `json:"summaryDescription,omitempty"` // command.data.description from /usage
	Error              string          `json:"error,omitempty"`
	LastErrorAt        string          `json:"lastErrorAt,omitempty"`
	GeminiPool         *PoolQuotaState `json:"geminiPool,omitempty"`
	ArgonPool          *ExtraPoolQuota `json:"argonPool,omitempty"`
	Groups             []QuotaGroup    `json:"groups"`
	Deduction          *QuotaDeduction `json:"deduction,omitempty"`
	ThirdPartyPool     *ThirdPartyPool `json:"thirdPartyPool,omitempty"`
}

type WindowAggregate struct {
	Start string `json:"start,omitempty"`
	Reset string `json:"reset,omitempty"`
	// Idle marks an official window that has not started yet: the previous one ended, /usage reports
	// 0% used and its reset time is one full window after the read. It starts on the next request.
	Idle               bool             `json:"idle,omitempty"`
	StepsCount         int              `json:"stepsCount"`
	WslSteps           int              `json:"wslSteps"`
	WinSteps           int              `json:"winSteps"`
	ClaudeGwSteps      int              `json:"claudeGwSteps"`
	WslHostSteps       int              `json:"wslHostSteps"`
	HitSteps           int              `json:"hitSteps"`
	Hits99             int              `json:"hits99"`
	ThinkingStepsCount int              `json:"thinkingStepsCount"`
	AvgTtftMs          int              `json:"avgTtftMs"`
	AvgTps             float64          `json:"avgTps"`
	AvgDecodeTps       float64          `json:"avgDecodeTps,omitempty"`
	RawIn              int64            `json:"rawIn"`
	RawCr              int64            `json:"rawCr"`
	AlignedIn          int64            `json:"alignedIn"`
	AlignedCr          int64            `json:"alignedCr"`
	TotalPromptTokens  int64            `json:"totalPromptTokens"`
	OutTok             int64            `json:"outTok"`
	ThTok              int64            `json:"thTok"`
	RawHitRate         float64          `json:"rawHitRate"`
	AlignedHitRate     float64          `json:"alignedHitRate"`
	InputCostUsd       float64          `json:"inputCostUsd"`
	CacheCostUsd       float64          `json:"cacheCostUsd"`
	OutputCostUsd      float64          `json:"outputCostUsd"`
	ThinkCostUsd       float64          `json:"thinkCostUsd"`
	CostUsd            float64          `json:"costUsd"`
	WslCostUsd         float64          `json:"wslCostUsd"`
	WinCostUsd         float64          `json:"winCostUsd"`
	ClaudeGwCostUsd    float64          `json:"claudeGwCostUsd"`
	WslHostCostUsd     float64          `json:"wslHostCostUsd"`
	Base1xCostUsd      float64          `json:"base1xCostUsd"`
	NoCacheCostUsd     float64          `json:"noCacheCostUsd"`
	SavedUsd           float64          `json:"savedUsd"`
	GeminiPoolCostUsd  float64          `json:"geminiPoolCostUsd"`
	ThirdPoolCostUsd   float64          `json:"thirdPartyPoolCostUsd"`
	Models             []ModelAggregate `json:"models"`
}

type ModelAggregate struct {
	ModelID       string         `json:"modelId"`
	ModelLabel    string         `json:"modelLabel"`
	Family        string         `json:"family"`
	QuotaGroup    string         `json:"quotaGroup"`
	Steps         int            `json:"steps"`
	ClaudeGwSteps int            `json:"claudeGwSteps"`
	RawIn         int64          `json:"rawIn"`
	RawCr         int64          `json:"rawCr"`
	OutTok        int64          `json:"outTok"`
	ThTok         int64          `json:"thTok"`
	HitRate       float64        `json:"hitRate"`
	CostUsd       float64        `json:"costUsd"`
	SavedUsd      float64        `json:"savedUsd"`
	AvgTtftMs     int            `json:"avgTtftMs"`
	AvgTps        float64        `json:"avgTps"`
	AvgDecodeTps  float64        `json:"avgDecodeTps,omitempty"`
	Tiers         map[string]int `json:"tiers"`
	// SteadyHitRate: share of non-first steps per session whose uncached input stayed within
	// max(8K, 10%); MissResendUsd: extra cost of re-sending the uncached part on the misses.
	SteadySteps         int            `json:"steadySteps"`
	SteadyHits          int            `json:"steadyHits"`
	SteadyHitRate       float64        `json:"steadyHitRate"`
	MissResendUsd       float64        `json:"missResendUsd"`
	ServedMismatch      int            `json:"servedMismatch"`
	ServedVariants      []string       `json:"servedVariants,omitempty"`
	ServedVariantCounts map[string]int `json:"servedVariantCounts,omitempty"`
	ttftSum             int64
	ttftCnt             int
	e2eOutSum           int64
	e2eMsSum            int64
	decOutSum           int64
	decMsSum            int64
}

type PoolWindow struct {
	Reset             string           `json:"reset"`
	UsedFraction      *float64         `json:"usedFraction"`
	RemainingFraction *float64         `json:"remainingFraction"`
	Idle              bool             `json:"idle,omitempty"`
	LocalCostUsd      float64          `json:"localCostUsd"`
	Steps             int              `json:"steps"`
	ImpliedCapUsd     *float64         `json:"impliedCapUsd"`
	Models            []ModelAggregate `json:"models"`
}

type ThirdPartyPool struct {
	GroupName                      string     `json:"groupName"`
	Description                    string     `json:"description"`
	Available                      bool       `json:"available"`
	State                          string     `json:"state"`
	UpdatedAt                      string     `json:"updatedAt,omitempty"`
	Error                          string     `json:"error,omitempty"`
	Cap5hUsd                       float64    `json:"cap5hUsd"`
	CapWeeklyUsd                   float64    `json:"capWeeklyUsd"`
	PoolUsed5hUsd                  *float64   `json:"poolUsed5hUsd"`
	PoolRem5hUsd                   *float64   `json:"poolRem5hUsd"`
	PoolUsedWeeklyUsd              *float64   `json:"poolUsedWeeklyUsd"`
	PoolRemWeeklyUsd               *float64   `json:"poolRemWeeklyUsd"`
	EffectiveRemUsd                *float64   `json:"effectiveRemUsd"`
	Bottleneck                     string     `json:"bottleneck,omitempty"`
	RemEffectiveTokensAt99PctCache *int64     `json:"remEffectiveTokensAt99PctCache"`
	RemEffectiveTokensAt0PctCache  *int64     `json:"remEffectiveTokensAt0PctCache"`
	Window5h                       PoolWindow `json:"window5h"`
	WindowWeekly                   PoolWindow `json:"windowWeekly"`
}

var (
	quotaMu            sync.Mutex
	quotaHomeMu        sync.Mutex
	cachedQuota        *QuotaSnapshot
	cachedQuotaAt      time.Time
	lastQuotaErr       *QuotaSnapshot
	lastQuotaAttemptAt time.Time
	quotaBackoffSteps  int
	quotaFlightMu      sync.Mutex
	quotaFlightWait    chan struct{}
	quotaFlightRes     QuotaSnapshot
	gwSidMu            sync.RWMutex
	gwSidRegSeq        atomic.Uint64
	registeredGwSids   = make(map[string]bool)
	logGwSids          = make(map[string]bool)
	storeGwSids        = make(map[string]bool)
	knownGwSidsLogPath string
	knownGwSidsDBPath  string
	knownGwSidsVer     uint64
)

func RegisterGatewaySessionID(sid string) {
	if sid == "" {
		return
	}
	low := strings.ToLower(sid)
	gwSidMu.Lock()
	if !registeredGwSids[low] {
		registeredGwSids[low] = true
		gwSidRegSeq.Add(1)
	}
	gwSidMu.Unlock()
}

func snapshotKnownGwSidsLocked(includeStore bool) map[string]bool {
	n := len(registeredGwSids) + len(logGwSids)
	if includeStore {
		n += len(storeGwSids)
	}
	out := make(map[string]bool, n)
	for k, v := range registeredGwSids {
		out[k] = v
	}
	for k, v := range logGwSids {
		out[k] = v
	}
	if includeStore {
		for k, v := range storeGwSids {
			out[k] = v
		}
	}
	return out
}

func getKnownGatewaySids() map[string]bool {
	storeReady := GlobalStore.IsReady()
	if storeReady {
		dbPath := GlobalStore.DBPath()
		curVer := GlobalStore.Version()

		gwSidMu.RLock()
		if dbPath == knownGwSidsDBPath && curVer == knownGwSidsVer {
			out := make(map[string]bool, len(registeredGwSids)+len(storeGwSids))
			for k, v := range registeredGwSids {
				out[k] = v
			}
			for k, v := range storeGwSids {
				out[k] = v
			}
			gwSidMu.RUnlock()
			return out
		}
		gwSidMu.RUnlock()

		gwSidMu.Lock()
		defer gwSidMu.Unlock()
		if dbPath != knownGwSidsDBPath || curVer != knownGwSidsVer {
			if dbPath != knownGwSidsDBPath {
				storeGwSids = make(map[string]bool)
			}
			GlobalStore.LoadKnownGatewaySids(storeGwSids)
			knownGwSidsDBPath = dbPath
			knownGwSidsVer = curVer
		}
		out := make(map[string]bool, len(registeredGwSids)+len(storeGwSids))
		for k, v := range registeredGwSids {
			out[k] = v
		}
		for k, v := range storeGwSids {
			out[k] = v
		}
		return out
	}

	logPath := GatewayLogFile
	gwSidMu.RLock()
	if logPath == knownGwSidsLogPath {
		out := snapshotKnownGwSidsLocked(false)
		gwSidMu.RUnlock()
		return out
	}
	gwSidMu.RUnlock()

	gwSidMu.Lock()
	defer gwSidMu.Unlock()
	if logPath != knownGwSidsLogPath {
		knownGwSidsLogPath = logPath
		logGwSids = make(map[string]bool)
		if logPath != "" {
			if raw, err := os.ReadFile(logPath); err == nil {
				re := regexp.MustCompile(`"sessionId"\s*:\s*"([0-9a-fA-F-]{36})"`)
				matches := re.FindAllSubmatch(raw, -1)
				for _, m := range matches {
					logGwSids[strings.ToLower(string(m[1]))] = true
				}
			}
		}
	}
	return snapshotKnownGwSidsLocked(false)
}

func quotaHasExpiredReset(snap QuotaSnapshot, now time.Time) bool {
	for _, g := range snap.Groups {
		for _, b := range g.Buckets {
			if b.ResetTime == "" {
				continue
			}
			if rt, err := time.Parse(time.RFC3339, b.ResetTime); err == nil && !now.Before(rt) {
				return true
			}
		}
	}
	return false
}
func quotaAttemptCooldown(steps int) time.Duration {
	switch {
	case steps <= 0:
		return 30 * time.Second
	case steps == 1:
		return 2 * time.Minute
	case steps == 2:
		return 5 * time.Minute
	default:
		return 15 * time.Minute
	}
}

func ResetOfficialQuotaBackoff() {
	quotaMu.Lock()
	quotaBackoffSteps = 0
	lastQuotaErr = nil
	lastQuotaAttemptAt = time.Time{}
	quotaMu.Unlock()
}

func FetchOfficialQuota(force bool) QuotaSnapshot {
	quotaMu.Lock()
	hasCache := cachedQuota != nil
	var snap QuotaSnapshot
	if hasCache {
		snap = *cachedQuota
	}
	hasErr := lastQuotaErr != nil
	var errSnap QuotaSnapshot
	if hasErr {
		errSnap = *lastQuotaErr
	}
	age := time.Since(cachedQuotaAt)
	attemptAge := age
	hasFailedSinceCache := !lastQuotaAttemptAt.IsZero() && (cachedQuotaAt.IsZero() || lastQuotaAttemptAt.After(cachedQuotaAt))
	if hasFailedSinceCache {
		attemptAge = time.Since(lastQuotaAttemptAt)
	}
	backoffSteps := quotaBackoffSteps
	if hasCache && !hasFailedSinceCache && snap.Status == "ok" && !quotaHasExpiredReset(snap, cachedQuotaAt) {
		backoffSteps = 0
		quotaBackoffSteps = 0
	}
	if force {
		quotaBackoffSteps = 0
	}
	quotaMu.Unlock()

	// Non-blocking stale-while-revalidate for dashboard polling:
	// Never block GET /api/dashboard on the 5-22s /usage subprocess when force=false.
	if !force {
		cooldown := quotaAttemptCooldown(backoffSteps)
		if hasCache {
			lastTurn := lastGatewayTurn.Load()
			resetPassed := age >= 30*time.Second && quotaHasExpiredReset(snap, time.Now())
			needBg := attemptAge >= cooldown && (age >= 15*time.Minute || (age >= 3*time.Minute && lastTurn > cachedQuotaAt.Unix()) || resetPassed)
			if needBg {
				startBackgroundQuotaRefresh()
			}
			return snap
		}
		// Cold start or previous error without cached reading: trigger background refresh (with backoff cooldown) and return immediately.
		needBg := !hasErr || attemptAge >= cooldown
		if needBg {
			startBackgroundQuotaRefresh()
		}
		if hasErr {
			return errSnap
		}
		return QuotaSnapshot{
			Status:    QuotaStateLoading,
			UpdatedAt: time.Now().UTC().Format(time.RFC3339),
			Groups:    []QuotaGroup{},
		}
	}
	return fetchOfficialQuotaSync()
}

// resolveIsolatedAgyHome returns a per-profile HOME directory whose .gemini resolves to pureDir
// whenever pureDir/antigravity-cli/antigravity-oauth-token exists.
//   - If pureDir is already named ".gemini" (e.g. child account workers), its parent directory is returned directly.
//   - Otherwise, a sibling quota-home-<base> directory is created with an atomic .gemini -> absPure symlink so
//     sibling profiles never collide and relative AGY_PURE_API_HOME paths never produce broken relative symlinks.
func resolveIsolatedAgyHome(pureDir string) string {
	if strings.TrimSpace(pureDir) == "" || isWindowsCDrivePath(pureDir) {
		return ""
	}
	absPure, err := filepath.Abs(pureDir)
	if err != nil || isWindowsCDrivePath(absPure) {
		return ""
	}
	tokPath := filepath.Join(absPure, "antigravity-cli", "antigravity-oauth-token")
	if !fileExists(tokPath) {
		return ""
	}
	if filepath.Base(absPure) == ".gemini" {
		return filepath.Dir(absPure)
	}
	qh := filepath.Join(filepath.Dir(absPure), "quota-home-"+filepath.Base(absPure))
	if isWindowsCDrivePath(qh) {
		return ""
	}
	linkPath := filepath.Join(qh, ".gemini")
	quotaHomeMu.Lock()
	defer quotaHomeMu.Unlock()
	if err := os.MkdirAll(qh, 0o700); err != nil {
		return ""
	}
	if cur, err := os.Readlink(linkPath); err == nil && cur == absPure {
		return qh
	}
	tmpLink := fmt.Sprintf("%s.tmp.%d", linkPath, os.Getpid())
	_ = os.Remove(tmpLink)
	if err := os.Symlink(absPure, tmpLink); err == nil {
		if err := os.Rename(tmpLink, linkPath); err == nil {
			return qh
		}
		_ = os.Remove(tmpLink)
	}
	_ = os.Remove(linkPath)
	if err := os.Symlink(absPure, linkPath); err != nil {
		return ""
	}
	return qh
}

func resolvePrimaryUpstreamProxy() string {
	proxy := gwcfg.Current().UpstreamProxy
	if !isChildAccountWorker() && GlobalAccountManager != nil && GlobalAccountManager.HasConfiguredAccounts() {
		if acc, ok := GlobalAccountManager.GetPrimaryAccount(); ok {
			if resolved := GlobalAccountManager.ResolveAccountProxy(acc); resolved != "" {
				return resolved
			}
		}
	}
	return proxy
}

// quotaCmdEnv builds the sanitized subprocess environment for `<agy> -p /usage --output-format json`.
func quotaCmdEnv(baseEnv []string) []string {
	quotaHome := resolveIsolatedAgyHome(PureApiHome)
	proxy := resolvePrimaryUpstreamProxy()
	env := make([]string, 0, len(baseEnv)+14)
	hasSSH := false
	for _, kv := range baseEnv {
		if isStrippedLeafAgySubprocessEnv(kv) {
			continue
		}
		if quotaHome != "" && strings.HasPrefix(kv, "HOME=") {
			continue
		}
		if strings.HasPrefix(kv, "SSH_CONNECTION=") {
			hasSSH = true
		}
		env = append(env, kv)
	}
	if quotaHome != "" {
		env = append(env, "HOME="+quotaHome)
		// On macOS, agy uses file-based token storage when an SSH session is detected;
		// otherwise it attempts macOS Keychain lookup which can hang in headless daemons.
		if runtime.GOOS == "darwin" && !hasSSH {
			env = append(env, "SSH_CONNECTION=1 1 1 1")
		}
	}
	env = append(env,
		"GOGC=30",
		"GOMEMLIMIT=120MiB",
		"DBUS_SESSION_BUS_ADDRESS=unix:path=/dev/null",
		"AGY_CLI_DISABLE_AUTO_UPDATE=1",
		"HTTP_PROXY="+proxy,
		"HTTPS_PROXY="+proxy,
		"http_proxy="+proxy,
		"https_proxy="+proxy,
		"ALL_PROXY=",
		"all_proxy=",
		"NO_PROXY=127.0.0.1,localhost,::1",
		"no_proxy=127.0.0.1,localhost,::1",
	)
	return env
}

func startBackgroundQuotaRefresh() {
	if !quotaRefreshing.CompareAndSwap(false, true) {
		return
	}
	// Snapshot binary path, subprocess env, and PureApiHome synchronously on the request goroutine
	// so background /usage execution never races with subsequent test t.Setenv or config reloads.
	binPath := quotaAgyBin()
	cmdEnv := quotaCmdEnv(os.Environ())
	pureHome := PureApiHome
	go func(bin string, env []string, home string) {
		defer quotaRefreshing.Store(false)
		_ = fetchOfficialQuotaSyncWithHome(bin, env, home)
	}(binPath, cmdEnv, pureHome)
}

func fetchOfficialQuotaSync() QuotaSnapshot {
	return fetchOfficialQuotaSyncWithHome(quotaAgyBin(), quotaCmdEnv(os.Environ()), PureApiHome)
}

func fetchOfficialQuotaSyncWith(binPath string, cmdEnv []string) QuotaSnapshot {
	return fetchOfficialQuotaSyncWithHome(binPath, cmdEnv, PureApiHome)
}

func fetchOfficialQuotaSyncWithHome(binPath string, cmdEnv []string, pureHome string) QuotaSnapshot {
	quotaFlightMu.Lock()
	if quotaFlightWait != nil {
		waitCh := quotaFlightWait
		quotaFlightMu.Unlock()
		<-waitCh
		quotaFlightMu.Lock()
		res := quotaFlightRes
		quotaFlightMu.Unlock()
		return res
	}
	waitCh := make(chan struct{})
	quotaFlightWait = waitCh
	quotaFlightMu.Unlock()

	res := fetchOfficialQuotaDoWithHome(binPath, cmdEnv, pureHome)

	quotaFlightMu.Lock()
	quotaFlightRes = res
	quotaFlightWait = nil
	close(waitCh)
	quotaFlightMu.Unlock()
	return res
}

var (
	officialQuotaTimeout = 15 * time.Second
	creditsProbeTimeout  = 8 * time.Second

	creditsTierMu    sync.Mutex
	creditsTierCache = make(map[string]creditsTierCacheEntry)
)

type creditsTierCacheEntry struct {
	tier      string
	checkedAt time.Time
}

func normalizePlanTierHint(raw string) string {
	low := strings.ToLower(strings.TrimSpace(raw))
	switch {
	case (strings.Contains(low, "ultra") && (strings.Contains(low, "5x") || strings.Contains(low, "5×"))) || low == "ultra_5x" || low == "ultra-5x" || low == "5x" || strings.Contains(low, "g1-ultra-5x"):
		return "ultra_5x"
	case low == "pro" || strings.Contains(low, "google ai pro") || strings.Contains(low, "ai pro") || strings.Contains(low, "pro plan") || strings.Contains(low, "g1-pro"):
		return "pro"
	case low == "ultra" || strings.Contains(low, "ultra") || strings.Contains(low, "20x"):
		return "ultra"
	default:
		return ""
	}
}

func planLabelForTier(tier string) string {
	switch normalizePlanTierHint(tier) {
	case "pro":
		return "AI Pro"
	case "ultra_5x":
		return "Ultra 5x"
	case "ultra":
		return "Ultra 20x"
	default:
		return ""
	}
}

func primaryAccountDisabled() bool {
	if isChildAccountWorker() || GlobalAccountManager == nil || !GlobalAccountManager.HasConfiguredAccounts() {
		return false
	}
	prim, ok := GlobalAccountManager.GetPrimaryAccount()
	return ok && !prim.Enabled
}

func creditsTierCacheKey(pureHome, binPath string) string {
	h := strings.TrimSpace(pureHome)
	if h != "" {
		h = filepath.Clean(h)
	}
	return h + "|" + strings.TrimSpace(binPath)
}

func InvalidateCreditsTierCache(pureHome string) {
	prefix := strings.TrimSpace(pureHome)
	if prefix != "" {
		prefix = filepath.Clean(prefix) + "|"
	}
	creditsTierMu.Lock()
	if prefix == "" {
		creditsTierCache = make(map[string]creditsTierCacheEntry)
	} else {
		for k := range creditsTierCache {
			if strings.HasPrefix(k, prefix) {
				delete(creditsTierCache, k)
			}
		}
	}
	creditsTierMu.Unlock()
}

func resetCreditsTierCacheForTest() {
	creditsTierMu.Lock()
	creditsTierCache = make(map[string]creditsTierCacheEntry)
	creditsTierMu.Unlock()
}

func cachedCreditsPlanTier(pureHome, binPath string) (tier string, fresh bool) {
	key := creditsTierCacheKey(pureHome, binPath)
	now := time.Now()
	creditsTierMu.Lock()
	entry, ok := creditsTierCache[key]
	creditsTierMu.Unlock()
	if !ok {
		return "", false
	}
	ttl := 5 * time.Minute
	if entry.tier != "" {
		ttl = 6 * time.Hour
	}
	if now.Sub(entry.checkedAt) > ttl {
		return "", false
	}
	return entry.tier, true
}

type authPromptAbortWriter struct {
	mu          sync.Mutex
	buf         bytes.Buffer
	cancel      context.CancelFunc
	authTripped bool
}

func (w *authPromptAbortWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	n, err := w.buf.Write(p)
	shouldCancel := false
	if !w.authTripped && hasInteractiveAuthPrompt(w.buf.String()) {
		w.authTripped = true
		shouldCancel = true
	}
	w.mu.Unlock()
	if shouldCancel && w.cancel != nil {
		w.cancel()
	}
	return n, err
}

func (w *authPromptAbortWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

func (w *authPromptAbortWriter) Bytes() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	b := w.buf.Bytes()
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

func (w *authPromptAbortWriter) AuthTripped() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.authTripped
}

func hasInteractiveAuthPrompt(text string) bool {
	low := strings.ToLower(text)
	return strings.Contains(low, "authentication required") ||
		strings.Contains(low, "please visit the url to log in") ||
		strings.Contains(low, "waiting for authentication") ||
		strings.Contains(low, "paste the authorization code")
}

// quotaAppDataArgs returns root-CLI flags pointing `/usage` and `/credits` at an isolated
// `<pureHome>/antigravity-cli-usage` app-data-dir so agy 1.5.x does not scan thousands of
// ws-bridge `conversations/*.db` files on startup (#1825). Note: agy's root CLI accepts
// underscored `--gemini_dir` / `--app_data_dir` (and rejects hyphenated variants).
func quotaAppDataArgs(pureHome string) []string {
	cleanHome := strings.TrimSpace(pureHome)
	if cleanHome == "" || isWindowsCDrivePath(cleanHome) {
		return nil
	}
	absHome, err := filepath.Abs(cleanHome)
	if err != nil || absHome == "" || isWindowsCDrivePath(absHome) {
		return nil
	}
	if ensureIsolatedAppDataDir(absHome, UsageAppDataDirName) == "" {
		return nil
	}
	return []string{"--gemini_dir", absHome, "--app_data_dir", UsageAppDataDirName}
}

func resolveAuthoritativePlanTierFromCredits(binPath string, cmdEnv []string, pureHome string) string {
	if tier, fresh := cachedCreditsPlanTier(pureHome, binPath); fresh {
		return tier
	}
	timeout := creditsProbeTimeout
	if timeout <= 0 {
		timeout = 8 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	args := append([]string{"-p", "/credits", "--output-format", "json"}, quotaAppDataArgs(pureHome)...)
	cmd := exec.CommandContext(ctx, binPath, args...)
	cmd.Env = cmdEnv
	configureQuotaSubprocess(cmd)
	stdoutW := &authPromptAbortWriter{cancel: cancel}
	stderrW := &authPromptAbortWriter{cancel: cancel}
	cmd.Stdout = stdoutW
	cmd.Stderr = stderrW
	err := cmd.Run()

	detected := ""
	outBytes := stdoutW.Bytes()
	if err == nil && len(outBytes) > 0 {
		var raw struct {
			Status  string `json:"status"`
			Command struct {
				Name string `json:"name"`
				Data struct {
					PlanName   string `json:"plan_name"`
					PlanTier   string `json:"plan_tier"`
					Tier       string `json:"tier"`
					UpgradeURI string `json:"upgrade_uri"`
				} `json:"data"`
			} `json:"command"`
		}
		if json.Unmarshal(outBytes, &raw) == nil {
			for _, cand := range []string{raw.Command.Data.PlanName, raw.Command.Data.PlanTier, raw.Command.Data.Tier} {
				if norm := normalizePlanTierHint(cand); norm != "" {
					detected = norm
					break
				}
			}
			if detected == "" {
				if strings.TrimSpace(raw.Command.Data.UpgradeURI) != "" {
					detected = "pro"
				} else if strings.EqualFold(strings.TrimSpace(raw.Status), "SUCCESS") &&
					strings.EqualFold(strings.TrimSpace(raw.Command.Name), "credits") {
					detected = "ultra"
				}
			}
		}
	}

	key := creditsTierCacheKey(pureHome, binPath)
	creditsTierMu.Lock()
	creditsTierCache[key] = creditsTierCacheEntry{tier: detected, checkedAt: time.Now()}
	creditsTierMu.Unlock()
	return detected
}

func authoritativeAccountPlanTierOverride() string {
	if envOv := normalizePlanTierHint(gwcfg.Current().AccountPlanTierOverride); envOv != "" {
		return envOv
	}
	if !isChildAccountWorker() && GlobalAccountManager != nil && GlobalAccountManager.HasConfiguredAccounts() {
		if prim, ok := GlobalAccountManager.GetPrimaryAccount(); ok {
			if ov := normalizePlanTierHint(prim.PlanTierOverride); ov != "" {
				return ov
			}
		}
	}
	return ""
}

func authoritativeAccountPlanTierHint() string {
	if ov := authoritativeAccountPlanTierOverride(); ov != "" {
		return ov
	}
	if tier, fresh := cachedCreditsPlanTier(PureApiHome, quotaAgyBin()); fresh && tier != "" {
		return tier
	}
	if envTier := normalizePlanTierHint(gwcfg.Current().AccountPlanTierHint); envTier != "" {
		return envTier
	}
	if !isChildAccountWorker() && GlobalAccountManager != nil && GlobalAccountManager.HasConfiguredAccounts() {
		if prim, ok := GlobalAccountManager.GetPrimaryAccount(); ok {
			if hint := prim.EffectivePlanTier(); hint != "" {
				return hint
			}
		}
	}
	return ""
}

// InvalidatePrimaryQuotaTierCache resets cached tier/view state when the primary
// account's manual planTierOverride changes so dashboard views update immediately.
func InvalidatePrimaryQuotaTierCache(overrideTier string) {
	InvalidateCreditsTierCache(PureApiHome)
	normOv := normalizePlanTierHint(overrideTier)
	quotaMu.Lock()
	if cachedQuota != nil {
		if normOv != "" {
			cachedQuota.PlanTier = normOv
			cachedQuota.PlanLabel = planLabelForTier(normOv)
		} else {
			cachedQuota.PlanTier = ""
			cachedQuota.PlanLabel = ""
		}
	}
	quotaMu.Unlock()
	dashStepsCacheMu.Lock()
	dashViewGen = 0
	dashViewQuotaKey = ""
	dashStepsCacheMu.Unlock()
}

func hasNonNullJSON(raw json.RawMessage) bool {
	s := strings.TrimSpace(string(raw))
	return s != "" && s != "null"
}

func fetchOfficialQuotaDo() QuotaSnapshot {
	return fetchOfficialQuotaDoWithHome(quotaAgyBin(), quotaCmdEnv(os.Environ()), PureApiHome)
}

func fetchOfficialQuotaDoWith(binPath string, cmdEnv []string) QuotaSnapshot {
	return fetchOfficialQuotaDoWithHome(binPath, cmdEnv, PureApiHome)
}

func fetchOfficialQuotaDoWithHome(binPath string, cmdEnv []string, pureHome string) QuotaSnapshot {
	startQuota := time.Now()
	timeout := officialQuotaTimeout
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	probeBudget := func() time.Duration {
		// Fit comfortably inside peerFetchRefreshTimeout (18s) even after a slow /usage invocation.
		rem := 17*time.Second - time.Since(startQuota)
		if rem < 2*time.Second {
			rem = 2 * time.Second
		}
		if rem > quotaNetProbeTimeout {
			rem = quotaNetProbeTimeout
		}
		return rem
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	args := append([]string{"-p", "/usage", "--output-format", "json"}, quotaAppDataArgs(pureHome)...)
	cmd := exec.CommandContext(ctx, binPath, args...)
	cmd.Env = cmdEnv
	configureQuotaSubprocess(cmd)
	stdoutW := &authPromptAbortWriter{cancel: cancel}
	stderrW := &authPromptAbortWriter{cancel: cancel}
	cmd.Stdout = stdoutW
	cmd.Stderr = stderrW
	err := cmd.Run()
	out := stdoutW.Bytes()
	if err != nil || len(out) == 0 {
		errMsg := "Empty /usage output"
		rawDiag := strings.TrimSpace(stderrW.String())
		if rawDiag == "" {
			rawDiag = strings.TrimSpace(string(out))
		}
		if isVerificationRequiredMessage(rawDiag) {
			// Keep up to 700 runes (redacted) so any Google verification HTTPS URL survives intact
			// into QuotaSnapshot.Error, RecordQuotaSyncOutcome, and federated dashboard views.
			diag := gwcfg.RedactSecretsInString(truncateRunes(rawDiag, 700))
			if err != nil {
				errMsg = gwcfg.RedactSecretsInString(err.Error()) + ": " + diag
			} else {
				errMsg = diag
			}
		} else if stdoutW.AuthTripped() || stderrW.AuthTripped() || hasInteractiveAuthPrompt(rawDiag) {
			errMsg = classifyQuotaAuthPromptWithTimeout(cmdEnv, probeBudget())
		} else if ctx.Err() == context.DeadlineExceeded {
			errMsg = fmt.Sprintf("timeout (%s) running /usage (check auth/proxy)", timeout.Round(time.Second))
		} else {
			diag := rawDiag
			if diag != "" {
				diag = gwcfg.RedactSecretsInString(truncateRunes(diag, 200))
			}
			if err != nil {
				base := gwcfg.RedactSecretsInString(err.Error())
				if diag != "" {
					errMsg = base + ": " + diag
				} else {
					errMsg = base
				}
			} else if diag != "" {
				errMsg = diag
			}
		}
		if !isChildAccountWorker() && GlobalAccountManager != nil && GlobalAccountManager.HasConfiguredAccounts() {
			if prim, ok := GlobalAccountManager.GetPrimaryAccount(); ok && prim.Enabled {
				GlobalAccountManager.RecordQuotaSyncOutcome(prim.ID, errMsg)
			}
		}
		quotaMu.Lock()
		defer quotaMu.Unlock()
		nowErr := time.Now()
		lastQuotaAttemptAt = nowErr
		quotaBackoffSteps++
		if cachedQuota != nil {
			return markQuotaStaleLocked(errMsg)
		}
		nowErrISO := nowErr.UTC().Format(time.RFC3339)
		errSnap := QuotaSnapshot{
			Status:      "error",
			UpdatedAt:   nowErrISO,
			LastErrorAt: nowErrISO,
			Error:       errMsg,
			Groups:      []QuotaGroup{},
		}
		lastQuotaErr = &errSnap
		return errSnap
	}

	var raw struct {
		Command struct {
			Data struct {
				Description string `json:"description"`
				Groups      []struct {
					Name        string `json:"name"`
					Description string `json:"description"`
					Buckets     []struct {
						ID                string          `json:"id"`
						Name              string          `json:"name"`
						Description       string          `json:"description"`
						Window            string          `json:"window"`
						Disabled          bool            `json:"disabled"`
						RemainingFraction *float64        `json:"remaining_fraction"`
						RemainingAmount   json.RawMessage `json:"remaining_amount"`
						ResetTime         string          `json:"reset_time"`
					} `json:"buckets"`
				} `json:"groups"`
			} `json:"data"`
		} `json:"command"`
	}
	if err := json.Unmarshal(out, &raw); err != nil {
		errMsg := "Unparseable /usage output: " + gwcfg.RedactSecretsInString(err.Error())
		if snippet := strings.TrimSpace(string(out)); snippet != "" {
			maxSnip := 160
			if isVerificationRequiredMessage(snippet) {
				maxSnip = 700
			}
			cleanSnip := gwcfg.RedactSecretsInString(truncateRunes(snippet, maxSnip))
			if isVerificationRequiredMessage(snippet) {
				errMsg = cleanSnip
			} else if hasInteractiveAuthPrompt(snippet) {
				classified := classifyQuotaAuthPromptWithTimeout(cmdEnv, probeBudget())
				errMsg = classified
				cleanSnip = classified
			}
			if !isChildAccountWorker() && GlobalAccountManager != nil && GlobalAccountManager.HasConfiguredAccounts() {
				if prim, ok := GlobalAccountManager.GetPrimaryAccount(); ok && prim.Enabled {
					GlobalAccountManager.RecordQuotaSyncOutcome(prim.ID, cleanSnip)
				}
			}
		}
		quotaMu.Lock()
		defer quotaMu.Unlock()
		nowErr := time.Now()
		lastQuotaAttemptAt = nowErr
		quotaBackoffSteps++
		if cachedQuota != nil {
			return markQuotaStaleLocked(errMsg)
		}
		nowErrISO := nowErr.UTC().Format(time.RFC3339)
		errSnap := QuotaSnapshot{
			Status:      "error",
			UpdatedAt:   nowErrISO,
			LastErrorAt: nowErrISO,
			Error:       errMsg,
			Groups:      []QuotaGroup{},
		}
		lastQuotaErr = &errSnap
		return errSnap
	}

	var groups []QuotaGroup
	for _, g := range raw.Command.Data.Groups {
		var bkts []QuotaBucket
		for _, b := range g.Buckets {
			if b.Disabled || (b.RemainingFraction == nil && hasNonNullJSON(b.RemainingAmount)) {
				continue
			}
			rem := 0.0
			if b.RemainingFraction != nil {
				rem = *b.RemainingFraction
			}
			used := math.Max(0, 1.0-rem)
			bkts = append(bkts, QuotaBucket{
				ID:                b.ID,
				Name:              b.Name,
				Description:       b.Description,
				Window:            b.Window,
				RemainingFraction: rem,
				UsedFraction:      used,
				ResetTime:         b.ResetTime,
			})
		}
		groups = append(groups, QuotaGroup{
			Name:        g.Name,
			Description: g.Description,
			Buckets:     bkts,
		})
	}

	snap := QuotaSnapshot{
		Status:             "ok",
		UpdatedAt:          time.Now().UTC().Format(time.RFC3339),
		SummaryDescription: strings.TrimSpace(raw.Command.Data.Description),
		Groups:             groups,
	}
	if len(groups) > 0 {
		if ov := authoritativeAccountPlanTierOverride(); ov != "" {
			snap.PlanTier = ov
			snap.PlanLabel = planLabelForTier(ov)
		} else {
			tier, label, definitive := detectQuotaPlanTierDetailed(snap)
			if definitive {
				snap.PlanTier = tier
				snap.PlanLabel = label
			} else {
				// Authoritative /credits (cached 6h per process) takes precedence over a
				// potentially stale persisted AGY_ACCOUNT_PLAN_TIER_HINT so plan upgrades/
				// downgrades or earlier heuristic misclassifications self-heal immediately.
				hint := resolveAuthoritativePlanTierFromCredits(binPath, cmdEnv, pureHome)
				if hint == "" {
					hint = authoritativeAccountPlanTierHint()
				}
				if hint != "" {
					snap.PlanTier = hint
					snap.PlanLabel = planLabelForTier(hint)
				}
			}
			if snap.PlanTier != "" && !isChildAccountWorker() && GlobalAccountManager != nil && GlobalAccountManager.HasConfiguredAccounts() {
				if prim, ok := GlobalAccountManager.GetPrimaryAccount(); ok {
					GlobalAccountManager.SetAccountPlanTierHint(prim.ID, snap.PlanTier)
				}
			}
		}
	}
	now := time.Now()
	stillExpired := quotaHasExpiredReset(snap, now)
	if !isChildAccountWorker() && GlobalAccountManager != nil && GlobalAccountManager.HasConfiguredAccounts() {
		if prim, ok := GlobalAccountManager.GetPrimaryAccount(); ok && prim.Enabled {
			GlobalAccountManager.ClearQuotaSyncAuthError(prim.ID)
		}
	}
	quotaMu.Lock()
	cachedQuota = &snap
	cachedQuotaAt = now
	lastQuotaErr = nil
	lastQuotaAttemptAt = now
	if stillExpired {
		quotaBackoffSteps++
	} else {
		quotaBackoffSteps = 0
	}
	quotaMu.Unlock()
	return snap
}

// markQuotaStaleLocked keeps the last good reading after a failed refresh but labels it, so the
// dashboard shows its age instead of presenting it as current. Caller holds quotaMu.
func markQuotaStaleLocked(errMsg string) QuotaSnapshot {
	cachedQuota.Status = "stale"
	cachedQuota.Error = errMsg
	cachedQuota.LastErrorAt = time.Now().UTC().Format(time.RFC3339)
	return *cachedQuota
}

func roundTo(v float64, decimals int) float64 {
	p := math.Pow(10, float64(decimals))
	return math.Round(v*p) / p
}

type annotatedStep struct {
	step   ParsedModelStep
	steady bool
	inTok  int64
	crTok  int64
	slack  int64
	miss   string
}

func stepTokenSplit(s ParsedModelStep) (in64, cr64, total, slack int64) {
	in64, cr64 = int64(s.RawIn), int64(s.RawCr)
	if in64 == 0 && cr64 == 0 {
		in64, cr64 = int64(s.AlignedIn), int64(s.AlignedCr)
	}
	if in64 < 0 {
		in64 = 0
	}
	if cr64 < 0 {
		cr64 = 0
	}
	total = clampedSumInt64(in64, cr64)
	slack = total / 10
	if slack < cacheHitSlackTokens {
		slack = cacheHitSlackTokens
	}
	return in64, cr64, total, slack
}

func stepGapMs(prev, cur ParsedModelStep) int64 {
	if prev.TsSec <= 0 {
		return 0
	}
	prevEndSec := prev.TsSec
	if prev.ElapsedMs > 0 {
		prevEndSec += int64(prev.ElapsedMs+500) / 1000
	}
	if prevEndSec > 0 && cur.TsSec > prevEndSec {
		return (cur.TsSec - prevEndSec) * 1000
	}
	return 0
}

func annotateSingleStep(s ParsedModelStep, lastModelBySid map[string]string, prevBySid map[string]ParsedModelStep) annotatedStep {
	in64, cr64, total, slack := stepTokenSplit(s)
	if total <= 0 {
		return annotatedStep{step: s, steady: false}
	}
	mk := s.ModelID
	if mk == "" {
		mk = UnknownModel.ID
	}
	lsid := strings.ToLower(strings.TrimSpace(s.SessionID))
	isSteady := false
	miss := ""
	if lsid != "" {
		prevModel, seenModel := lastModelBySid[lsid]
		prev, hasPrev := prevBySid[lsid]
		if seenModel {
			isSteady = prevModel == mk
		}
		if isSteady && hasPrev {
			prevIn, prevCr, _, _ := stepTokenSplit(prev)
			modelSwitched := prev.ModelID != "" && s.ModelID != "" && prev.ModelID != s.ModelID
			miss = classifyCacheMissSpec(prevIn, prevCr, in64, cr64, stepGapMs(prev, s), modelSwitched)
		}
		lastModelBySid[lsid] = mk
		prevBySid[lsid] = s
	}
	return annotatedStep{
		step:   s,
		steady: isSteady,
		inTok:  in64,
		crTok:  cr64,
		slack:  slack,
		miss:   miss,
	}
}

// annotateSteadySteps marks continuation steps across chronological steps before window slicing so
// rolling windows retain warm continuation steps and in-session model_switch cold prefixes are excluded.
func annotateSteadySteps(steps []ParsedModelStep) []annotatedStep {
	return annotateSteadyStepsWithSeed(steps, nil)
}

func annotateSteadyStepsWithSeed(steps []ParsedModelStep, seedPrevBySid map[string]ParsedModelStep) []annotatedStep {
	out := make([]annotatedStep, len(steps))
	lastModelBySid := make(map[string]string, 64+len(seedPrevBySid))
	prevBySid := make(map[string]ParsedModelStep, 64+len(seedPrevBySid))
	for lsid, prev := range seedPrevBySid {
		mk := prev.ModelID
		if mk == "" {
			mk = UnknownModel.ID
		}
		lastModelBySid[lsid] = mk
		prevBySid[lsid] = prev
	}
	for i, s := range steps {
		out[i] = annotateSingleStep(s, lastModelBySid, prevBySid)
	}
	return out
}

type windowStepAccumulator struct {
	agg       WindowAggregate
	ttftSum   int64
	ttftCnt   int
	e2eOutSum int64
	e2eMsSum  int64
	decOutSum int64
	decMsSum  int64
	byModel   map[string]*ModelAggregate
	order     []string
}

func newWindowStepAccumulator() *windowStepAccumulator {
	return &windowStepAccumulator{
		byModel: map[string]*ModelAggregate{},
	}
}

func (a *windowStepAccumulator) add(item annotatedStep) {
	s := item.step
	a.agg.StepsCount++
	if s.QuotaGroup == "3p" {
		a.agg.ThirdPoolCostUsd += s.CostUsd
	} else {
		a.agg.GeminiPoolCostUsd += s.CostUsd
	}
	mk := s.ModelID
	if mk == "" {
		mk = UnknownModel.ID
	}
	mm := a.byModel[mk]
	if mm == nil {
		lbl, fam, qg := s.ModelLabel, s.ModelFamily, s.QuotaGroup
		if lbl == "" {
			lbl, fam, qg = UnknownModel.Label, "other", "gemini"
		}
		mm = &ModelAggregate{ModelID: mk, ModelLabel: lbl, Family: fam, QuotaGroup: qg, Tiers: map[string]int{}}
		a.byModel[mk] = mm
		a.order = append(a.order, mk)
	}
	mm.Steps++
	if s.SourceCategory == "CLAUDE_GW" {
		mm.ClaudeGwSteps++
	}
	mm.RawIn += int64(s.RawIn)
	mm.RawCr += int64(s.RawCr)
	mm.OutTok += int64(s.OutTok)
	mm.ThTok += int64(s.ThTok)
	mm.CostUsd += s.CostUsd
	mm.SavedUsd += s.SavedUsd
	if s.ModelTier != "" {
		mm.Tiers[s.ModelTier]++
	}
	if tot := clampedSumInt64(item.inTok, item.crTok); tot > 0 && item.steady {
		mm.SteadySteps++
		if item.miss == "" {
			mm.SteadyHits++
		} else if m := GetModel(s.ModelID); m != nil && item.inTok > item.slack {
			mm.MissResendUsd += float64(item.inTok-item.slack) * (m.Pricing.InPerM - m.Pricing.CachedPerM) / 1e6
		}
	}
	if isServedModelMismatch(s.ModelID, s.ModelTier, s.ServedModel) {
		mm.ServedMismatch++
		if mm.ServedVariantCounts == nil {
			mm.ServedVariantCounts = map[string]int{}
		}
		mm.ServedVariantCounts[s.ServedModel]++
	}
	if s.TtftMs > 0 && s.TtftMs < 300000 {
		mm.ttftSum += int64(s.TtftMs)
		mm.ttftCnt++
	}
	if s.OutTok > 0 && s.ElapsedMs > 0 && s.ElapsedMs < 600000 {
		mm.e2eOutSum += int64(s.OutTok)
		mm.e2eMsSum += int64(s.ElapsedMs)
	}
	if s.DecodeTps > 0 && s.StreamMs > 0 && s.StreamMs < 600000 && (s.ElapsedMs == 0 || s.ElapsedMs < 600000) && s.OutTok > 0 {
		mm.decOutSum += int64(s.OutTok)
		mm.decMsSum += int64(s.StreamMs)
	}
	if s.SourceSide == "WIN" {
		a.agg.WinSteps++
		a.agg.WinCostUsd += s.CostUsd
	} else {
		a.agg.WslSteps++
		a.agg.WslCostUsd += s.CostUsd
		if s.SourceCategory == "CLAUDE_GW" {
			a.agg.ClaudeGwSteps++
			a.agg.ClaudeGwCostUsd += s.CostUsd
		} else {
			a.agg.WslHostSteps++
			a.agg.WslHostCostUsd += s.CostUsd
		}
	}
	a.agg.RawIn += int64(s.RawIn)
	a.agg.RawCr += int64(s.RawCr)
	a.agg.AlignedIn += int64(s.AlignedIn)
	a.agg.AlignedCr += int64(s.AlignedCr)
	a.agg.OutTok += int64(s.OutTok)
	a.agg.ThTok += int64(s.ThTok)
	if s.HasThinking {
		a.agg.ThinkingStepsCount++
	}
	a.agg.InputCostUsd += s.InputCostUsd
	a.agg.CacheCostUsd += s.CacheCostUsd
	a.agg.OutputCostUsd += s.OutputCostUsd
	a.agg.ThinkCostUsd += s.ThinkCostUsd
	a.agg.CostUsd += s.CostUsd
	a.agg.Base1xCostUsd += s.Base1xCostUsd
	a.agg.NoCacheCostUsd += s.NoCacheCostUsd
	a.agg.SavedUsd += s.SavedUsd
	if s.RawCr > 0 {
		a.agg.HitSteps++
	}
	tot := s.AlignedIn + s.AlignedCr
	if tot > 0 && float64(s.AlignedCr)/float64(tot) >= 0.99 {
		a.agg.Hits99++
	}
	if s.TtftMs > 0 && s.TtftMs < 300000 {
		a.ttftSum += int64(s.TtftMs)
		a.ttftCnt++
	}
	if s.OutTok > 0 && s.ElapsedMs > 0 && s.ElapsedMs < 600000 {
		a.e2eOutSum += int64(s.OutTok)
		a.e2eMsSum += int64(s.ElapsedMs)
	}
	if s.DecodeTps > 0 && s.StreamMs > 0 && s.StreamMs < 600000 && (s.ElapsedMs == 0 || s.ElapsedMs < 600000) && s.OutTok > 0 {
		a.decOutSum += int64(s.OutTok)
		a.decMsSum += int64(s.StreamMs)
	}
}

func (a *windowStepAccumulator) finalize() WindowAggregate {
	agg := a.agg
	agg.TotalPromptTokens = agg.RawIn + agg.RawCr
	if agg.RawIn+agg.RawCr > 0 {
		agg.RawHitRate = roundTo(float64(agg.RawCr)/float64(agg.RawIn+agg.RawCr)*100, 2)
	}
	if agg.AlignedIn+agg.AlignedCr > 0 {
		agg.AlignedHitRate = roundTo(float64(agg.AlignedCr)/float64(agg.AlignedIn+agg.AlignedCr)*100, 2)
	}
	if a.ttftCnt > 0 {
		agg.AvgTtftMs = int(a.ttftSum / int64(a.ttftCnt))
	}
	if a.e2eMsSum > 0 && a.e2eOutSum > 0 {
		agg.AvgTps = roundTo(float64(a.e2eOutSum)/(float64(a.e2eMsSum)/1000.0), 1)
	}
	if a.decMsSum > 0 && a.decOutSum > 0 {
		agg.AvgDecodeTps = roundTo(float64(a.decOutSum)/(float64(a.decMsSum)/1000.0), 1)
	}
	agg.InputCostUsd = roundTo(agg.InputCostUsd, 4)
	agg.CacheCostUsd = roundTo(agg.CacheCostUsd, 4)
	agg.OutputCostUsd = roundTo(agg.OutputCostUsd, 4)
	agg.ThinkCostUsd = roundTo(agg.ThinkCostUsd, 4)
	agg.CostUsd = roundTo(agg.CostUsd, 4)
	agg.WslCostUsd = roundTo(agg.WslCostUsd, 4)
	agg.WinCostUsd = roundTo(agg.WinCostUsd, 4)
	agg.ClaudeGwCostUsd = roundTo(agg.ClaudeGwCostUsd, 4)
	agg.WslHostCostUsd = roundTo(agg.WslHostCostUsd, 4)
	agg.Base1xCostUsd = roundTo(agg.Base1xCostUsd, 4)
	agg.NoCacheCostUsd = roundTo(agg.NoCacheCostUsd, 4)
	agg.SavedUsd = roundTo(agg.SavedUsd, 4)
	agg.GeminiPoolCostUsd = roundTo(agg.GeminiPoolCostUsd, 4)
	agg.ThirdPoolCostUsd = roundTo(agg.ThirdPoolCostUsd, 4)
	agg.Models = make([]ModelAggregate, 0, len(a.order))
	for _, k := range a.order {
		m := a.byModel[k]
		if m.RawIn+m.RawCr > 0 {
			m.HitRate = roundTo(float64(m.RawCr)/float64(m.RawIn+m.RawCr)*100, 2)
		}
		if m.ttftCnt > 0 {
			m.AvgTtftMs = int(m.ttftSum / int64(m.ttftCnt))
		}
		if m.e2eMsSum > 0 && m.e2eOutSum > 0 {
			m.AvgTps = roundTo(float64(m.e2eOutSum)/(float64(m.e2eMsSum)/1000.0), 1)
		}
		if m.decMsSum > 0 && m.decOutSum > 0 {
			m.AvgDecodeTps = roundTo(float64(m.decOutSum)/(float64(m.decMsSum)/1000.0), 1)
		}
		m.CostUsd = roundTo(m.CostUsd, 4)
		m.SavedUsd = roundTo(m.SavedUsd, 4)
		if m.SteadySteps > 0 {
			m.SteadyHitRate = roundTo(float64(m.SteadyHits)/float64(m.SteadySteps)*100, 1)
		}
		m.MissResendUsd = roundTo(m.MissResendUsd, 4)
		if len(m.ServedVariantCounts) > 0 {
			vars := make([]string, 0, len(m.ServedVariantCounts))
			for v := range m.ServedVariantCounts {
				vars = append(vars, v)
			}
			sort.Strings(vars)
			m.ServedVariants = vars
		}
		agg.Models = append(agg.Models, *m)
	}
	sort.SliceStable(agg.Models, func(i, j int) bool {
		if agg.Models[i].CostUsd != agg.Models[j].CostUsd {
			return agg.Models[i].CostUsd > agg.Models[j].CostUsd
		}
		return agg.Models[i].Steps > agg.Models[j].Steps
	})
	return agg
}

func AggregateSteps(steps []ParsedModelStep) WindowAggregate {
	return aggregateAnnotatedSteps(annotateSteadySteps(steps))
}

func aggregateAnnotatedSteps(items []annotatedStep) WindowAggregate {
	acc := newWindowStepAccumulator()
	for _, item := range items {
		acc.add(item)
	}
	return acc.finalize()
}

func filterSteps(steps []ParsedModelStep, keep func(*ParsedModelStep) bool) []ParsedModelStep {
	out := make([]ParsedModelStep, 0, len(steps))
	for i := range steps {
		if keep(&steps[i]) {
			out = append(out, steps[i])
		}
	}
	return out
}

func findGroup(groups []QuotaGroup, re *regexp.Regexp) *QuotaGroup {
	for i := range groups {
		if re.MatchString(strings.ToLower(groups[i].Name)) {
			return &groups[i]
		}
	}
	return nil
}

func findBucket(g *QuotaGroup, window string) *QuotaBucket {
	if g == nil {
		return nil
	}
	for i := range g.Buckets {
		if g.Buckets[i].Window == window {
			return &g.Buckets[i]
		}
	}
	return nil
}

const (
	quotaWindowStartSkewSec = 15
	maxStepDurationSkewSec  = 300
)

func windowStartUnix(b *QuotaBucket, nowSec, lengthSec int64) int64 {
	if b != nil && b.ResetTime != "" {
		if t, err := time.Parse(time.RFC3339, b.ResetTime); err == nil {
			return t.Unix() - lengthSec
		}
	}
	// When an official bucket reports non-zero usage while omitting resetTime (e.g., 3p-5h while
	// 3p-weekly is at 0% remaining), fall back to the rolling window [now - length, now] rather
	// than filtering out all historical steps with start = now.
	if b != nil && b.UsedFraction > 0.0005 {
		return nowSec - lengthSec
	}
	return nowSec
}

// inOfficialWindow matches local usage_steps against an official quota bucket [reset - length, reset).
// agy records step start time in ts_sec, whereas cloudcode-pa anchors resetTime when the opening
// generation finishes billing, so the opening step can have ts_sec < reset - length by its elapsed
// duration plus sub-second/clock skew. When /usage reported an idle window at readUnix, historical
// steps from the ended window are excluded while brand-new steps strictly after readUnix (occurring
// within the 3m /usage cache TTL) are retained.
func hasValidBucketResetTime(b *QuotaBucket) bool {
	if b == nil || b.ResetTime == "" {
		return false
	}
	_, err := time.Parse(time.RFC3339, b.ResetTime)
	return err == nil
}

func inOfficialWindow(s ParsedModelStep, b *QuotaBucket, startSec int64, idle bool, readUnix int64) bool {
	return inOfficialWindowFast(s, hasValidBucketResetTime(b), startSec, idle, readUnix)
}

func inOfficialWindowFast(s ParsedModelStep, hasValidReset bool, startSec int64, idle bool, readUnix int64) bool {
	if idle {
		return readUnix > 0 && s.TsSec > readUnix
	}
	if hasValidReset {
		endSec := s.TsSec
		if s.ElapsedMs > 0 {
			durMs := int64(s.ElapsedMs)
			if durMs > maxStepDurationSkewSec*1000 {
				durMs = maxStepDurationSkewSec * 1000
			}
			endSec += (durMs + 999) / 1000
		}
		return endSec >= startSec-quotaWindowStartSkewSec
	}
	return s.TsSec >= startSec
}

func effectivePoolRem(rem5h, remWeekly *float64) (*float64, string) {
	switch {
	case rem5h != nil && remWeekly != nil:
		if *remWeekly+0.005 < *rem5h {
			v := *remWeekly
			return &v, "weekly"
		}
		v := *rem5h
		return &v, "5h"
	case rem5h != nil:
		v := *rem5h
		return &v, "5h"
	case remWeekly != nil:
		v := *remWeekly
		return &v, "weekly"
	default:
		return nil, ""
	}
}

func buildPoolWindow(b *QuotaBucket, steps []ParsedModelStep) PoolWindow {
	return buildPoolWindowAnnotated(b, annotateSteadySteps(steps))
}

func buildPoolWindowAnnotated(b *QuotaBucket, steps []annotatedStep) PoolWindow {
	agg := aggregateAnnotatedSteps(steps)
	pw := PoolWindow{LocalCostUsd: roundTo(agg.CostUsd, 2), Steps: agg.StepsCount, Models: agg.Models}
	if b != nil {
		used, rem := b.UsedFraction, b.RemainingFraction
		pw.Reset = b.ResetTime
		pw.UsedFraction = &used
		pw.RemainingFraction = &rem
		if used > 0.005 && agg.CostUsd > 0 {
			v := roundTo(agg.CostUsd/used, 2)
			pw.ImpliedCapUsd = &v
		}
	}
	return pw
}

// classifyPoolQuota decides how trustworthy a pool reading is. g is the pool's official group.
func classifyPoolQuota(snap QuotaSnapshot, g *QuotaGroup, now time.Time) PoolQuotaState {
	st := PoolQuotaState{UpdatedAt: snap.UpdatedAt, Error: snap.Error}
	switch {
	case snap.Status == QuotaStateLoading:
		st.State = QuotaStateLoading
		st.UpdatedAt = ""
	case snap.Status == "error":
		st.State = QuotaStateError
		st.UpdatedAt = ""
	case g == nil || (findBucket(g, "5h") == nil && findBucket(g, "weekly") == nil):
		st.State = QuotaStateNotEnabled
	case snap.Status == "stale":
		st.State = QuotaStateStale
	default:
		st.State = QuotaStateKnown
		if t, err := time.Parse(time.RFC3339, snap.UpdatedAt); err == nil && now.Sub(t) > quotaStaleAfter {
			st.State = QuotaStateStale
		}
	}
	return st
}

// quotaWindowIdle reports an official window that has not started: the previous one ended, /usage
// says 0% used and the reset is one full window length after the read (2026-11-11 F1: the pool shows
// "resets in 5h" from the moment it is read until the next request opens a new window).
func quotaWindowIdle(b *QuotaBucket, readAt string, length time.Duration) bool {
	if b == nil || b.UsedFraction > 1e-6 {
		return false
	}
	reset, err1 := time.Parse(time.RFC3339, b.ResetTime)
	read, err2 := time.Parse(time.RFC3339, readAt)
	if err1 != nil || err2 != nil {
		return false
	}
	d := reset.Sub(read) - length
	return d >= -10*time.Minute && d <= 10*time.Minute
}

func pctUsdPtr(capUsd float64, frac *float64) *float64 {
	if frac == nil || capUsd <= 0 {
		return nil
	}
	v := roundTo(capUsd**frac, 2)
	return &v
}

func tokensPtr(usd *float64, perM float64) *int64 {
	if usd == nil || perM <= 0 {
		return nil
	}
	v := int64(math.Round((*usd / perM) * 1e6))
	return &v
}

func bucketFracs(b *QuotaBucket) (used, rem *float64) {
	if b == nil {
		return nil, nil
	}
	u, r := b.UsedFraction, b.RemainingFraction
	return &u, &r
}

func descriptionHasProPlanCue(desc string) bool {
	low := strings.ToLower(desc)
	return strings.Contains(low, "pro plan") ||
		strings.Contains(low, "google ai pro") ||
		strings.Contains(low, "g1-pro") ||
		strings.Contains(low, "ai pro")
}

func descriptionHasUltra5xPlanCue(desc string) bool {
	low := strings.ToLower(desc)
	return strings.Contains(low, "ultra") && (strings.Contains(low, "5x") || strings.Contains(low, "5×"))
}

func descriptionHasUltraPlanCue(desc string) bool {
	if descriptionHasUltra5xPlanCue(desc) {
		return false
	}
	low := strings.ToLower(desc)
	return strings.Contains(low, "google ai ultra") ||
		strings.Contains(low, "ai ultra") ||
		strings.Contains(low, "ultra plan") ||
		strings.Contains(low, "20x")
}

// detectQuotaPlanTierDetailed identifies whether the official /usage snapshot belongs to Google AI Pro,
// Google AI Ultra 5x, or Google AI Ultra (20x) and reports whether the heuristic signal was definitive.
func detectQuotaPlanTierDetailed(snap QuotaSnapshot) (tier, label string, definitive bool) {
	if descriptionHasUltra5xPlanCue(snap.SummaryDescription) {
		return "ultra_5x", "Ultra 5x", true
	}
	if descriptionHasProPlanCue(snap.SummaryDescription) {
		return "pro", "AI Pro", true
	}
	hasGemini := false
	geminiHasCarbon := false
	geminiHasDesc := false
	sawArgon := false
	sawUltraCue := descriptionHasUltraPlanCue(snap.SummaryDescription)
	for _, g := range snap.Groups {
		lowName := strings.ToLower(strings.TrimSpace(g.Name))
		lowDesc := strings.ToLower(g.Description)
		if strings.Contains(lowName, "argon") && lowName != "gemini models" {
			sawArgon = true
		}
		if descriptionHasUltra5xPlanCue(lowDesc) {
			return "ultra_5x", "Ultra 5x", true
		}
		if descriptionHasProPlanCue(lowDesc) {
			return "pro", "AI Pro", true
		}
		if descriptionHasUltraPlanCue(lowDesc) {
			sawUltraCue = true
		}
		for _, b := range g.Buckets {
			if strings.Contains(strings.ToLower(b.ID), "argon") {
				sawArgon = true
			}
			if descriptionHasUltra5xPlanCue(b.Description) {
				return "ultra_5x", "Ultra 5x", true
			}
			if descriptionHasProPlanCue(b.Description) {
				return "pro", "AI Pro", true
			}
			if descriptionHasUltraPlanCue(b.Description) {
				sawUltraCue = true
			}
		}
		if lowName == "gemini models" || strings.Contains(lowName, "gemini") {
			hasGemini = true
			if strings.TrimSpace(lowDesc) != "" {
				geminiHasDesc = true
				if strings.Contains(lowDesc, "carbon") {
					geminiHasCarbon = true
				}
			}
		}
	}
	if sawUltraCue {
		return "ultra", "Ultra 20x", true
	}
	if sawArgon || (hasGemini && geminiHasDesc && !geminiHasCarbon) {
		return "pro", "AI Pro", false
	}
	return "ultra", "Ultra 20x", false
}

// detectQuotaPlanTier identifies whether the official /usage snapshot belongs to a Google AI Pro
// plan or a Google AI Ultra (20x) plan so pool dollar caps ($10/$35 & $2.50/$5 vs $200/$800 & $25/$50)
// match the account's actual subscription limits.
func detectQuotaPlanTier(groups []QuotaGroup) (tier, label string) {
	t, l, _ := detectQuotaPlanTierDetailed(QuotaSnapshot{Groups: groups})
	return t, l
}

var (
	reGeminiModelsGroup   = regexp.MustCompile(`^gemini\s+models$`)
	reGeminiFallbackGroup = regexp.MustCompile(`gemini`)
	reArgonGroup          = regexp.MustCompile(`argon`)
	reThirdPartyGroup     = regexp.MustCompile(`claude|gpt`)
)

// buildQuotaView joins the official /usage snapshot with local usage steps: per-pool trust state,
// official-window aggregates (start = reset - window length), rolling aggregates and the dollar /
// token deduction. Unknown pools yield nil numbers instead of a full pool.
func buildQuotaView(quota QuotaSnapshot, allSteps []ParsedModelStep, now time.Time) (QuotaSnapshot, map[string]WindowAggregate) {
	annotatedAll := annotateSteadySteps(allSteps)
	allTimeAgg := aggregateAnnotatedSteps(annotatedAll)
	return buildQuotaViewAnnotated(quota, annotatedAll, &allTimeAgg, now)
}

func buildQuotaViewAnnotated(quota QuotaSnapshot, annotatedAll []annotatedStep, precomputedAllTime *WindowAggregate, now time.Time) (QuotaSnapshot, map[string]WindowAggregate) {
	ovTier := authoritativeAccountPlanTierOverride()
	hintTier := normalizePlanTierHint(quota.PlanTier)
	if hintTier == "" {
		hintTier = authoritativeAccountPlanTierHint()
	}
	if ovTier != "" {
		quota.PlanTier = ovTier
		quota.PlanLabel = planLabelForTier(ovTier)
	} else if len(quota.Groups) == 0 {
		if hintTier != "" {
			quota.PlanTier = hintTier
			quota.PlanLabel = planLabelForTier(hintTier)
		} else {
			quota.PlanTier = ""
			quota.PlanLabel = ""
		}
	} else {
		planTier, planLabel, definitive := detectQuotaPlanTierDetailed(quota)
		if !definitive && hintTier != "" {
			planTier = hintTier
			planLabel = planLabelForTier(hintTier)
		}
		quota.PlanTier = planTier
		quota.PlanLabel = planLabel
	}
	gemPricing := DefaultGeminiPricing
	claudePricing := DefaultClaudePoolPricing
	if quota.PlanTier == "pro" {
		gemPricing = ProGeminiPricing
		claudePricing = ProClaudePoolPricing
	} else if quota.PlanTier == "ultra_5x" {
		// 5x dollar caps are not published by /usage; preserve real fractions without inventing 20x caps.
		gemPricing.Cap5hUsd, gemPricing.CapWeeklyUsd = 0, 0
		claudePricing.Cap5hUsd, claudePricing.CapWeeklyUsd = 0, 0
	}

	geminiGroup := findGroup(quota.Groups, reGeminiModelsGroup)
	if geminiGroup == nil {
		geminiGroup = findGroup(quota.Groups, reGeminiFallbackGroup)
	}
	gemState := classifyPoolQuota(quota, geminiGroup, now)
	quota.GeminiPool = &gemState
	gemKnown := gemState.State == QuotaStateKnown || gemState.State == QuotaStateStale

	if argonGroup := findGroup(quota.Groups, reArgonGroup); argonGroup != nil && argonGroup != geminiGroup {
		arState := classifyPoolQuota(quota, argonGroup, now)
		arBucket := findBucket(argonGroup, "weekly")
		winName := "weekly"
		if arBucket == nil {
			arBucket = findBucket(argonGroup, "5h")
			winName = "5h"
		}
		used, rem := bucketFracs(arBucket)
		resetTime := ""
		if arBucket != nil {
			resetTime = arBucket.ResetTime
		}
		quota.ArgonPool = &ExtraPoolQuota{
			GroupName:         argonGroup.Name,
			State:             arState.State,
			UpdatedAt:         arState.UpdatedAt,
			Window:            winName,
			RemainingFraction: rem,
			UsedFraction:      used,
			ResetTime:         resetTime,
		}
	}

	var b5h, bWeekly *QuotaBucket
	if gemKnown {
		b5h, bWeekly = findBucket(geminiGroup, "5h"), findBucket(geminiGroup, "weekly")
	}

	nowSec := now.Unix()
	var readUnix int64
	if rt, err := time.Parse(time.RFC3339, quota.UpdatedAt); err == nil {
		readUnix = rt.Unix()
	}
	start5hSec := windowStartUnix(b5h, nowSec, 5*3600)
	startWeeklySec := windowStartUnix(bWeekly, nowSec, 7*86400)
	idle5h := quotaWindowIdle(b5h, quota.UpdatedAt, 5*time.Hour)
	idleWeekly := quotaWindowIdle(bWeekly, quota.UpdatedAt, 7*24*time.Hour)

	thirdGroup := findGroup(quota.Groups, reThirdPartyGroup)
	tpState := classifyPoolQuota(quota, thirdGroup, now)
	tpKnown := tpState.State == QuotaStateKnown || tpState.State == QuotaStateStale
	var tp5h, tpWk *QuotaBucket
	if tpKnown {
		tp5h, tpWk = findBucket(thirdGroup, "5h"), findBucket(thirdGroup, "weekly")
	}
	tpStart5h := windowStartUnix(tp5h, nowSec, 5*3600)
	tpStartWk := windowStartUnix(tpWk, nowSec, 7*86400)
	tpIdle5h := quotaWindowIdle(tp5h, quota.UpdatedAt, 5*time.Hour)
	tpIdleWk := quotaWindowIdle(tpWk, quota.UpdatedAt, 7*24*time.Hour)

	validGem5h := hasValidBucketResetTime(b5h)
	validGemWk := hasValidBucketResetTime(bWeekly)
	validTp5h := hasValidBucketResetTime(tp5h)
	validTpWk := hasValidBucketResetTime(tpWk)

	var steps5h, stepsWeekly, gemSteps5h, gemStepsWeekly, tpSteps5h, tpStepsWk []annotatedStep
	var stepsRolling5h, stepsRolling7d, steps24h []annotatedStep
	for _, item := range annotatedAll {
		s := item.step
		if s.QuotaGroup == "3p" {
			if tpKnown && inOfficialWindowFast(s, validTp5h, tpStart5h, tpIdle5h, readUnix) {
				tpSteps5h = append(tpSteps5h, item)
				steps5h = append(steps5h, item)
			}
			if tpKnown && inOfficialWindowFast(s, validTpWk, tpStartWk, tpIdleWk, readUnix) {
				tpStepsWk = append(tpStepsWk, item)
				stepsWeekly = append(stepsWeekly, item)
			}
		} else {
			if gemKnown && inOfficialWindowFast(s, validGem5h, start5hSec, idle5h, readUnix) {
				gemSteps5h = append(gemSteps5h, item)
				steps5h = append(steps5h, item)
			}
			if gemKnown && inOfficialWindowFast(s, validGemWk, startWeeklySec, idleWeekly, readUnix) {
				gemStepsWeekly = append(gemStepsWeekly, item)
				stepsWeekly = append(stepsWeekly, item)
			}
		}
		if s.TsSec >= nowSec-5*3600 {
			stepsRolling5h = append(stepsRolling5h, item)
		}
		if s.TsSec >= nowSec-7*86400 {
			stepsRolling7d = append(stepsRolling7d, item)
		}
		if s.TsSec >= nowSec-86400 {
			steps24h = append(steps24h, item)
		}
	}
	if len(gemSteps5h) > 0 {
		idle5h = false
	}
	if len(gemStepsWeekly) > 0 {
		idleWeekly = false
	}
	if len(tpSteps5h) > 0 {
		tpIdle5h = false
	}
	if len(tpStepsWk) > 0 {
		tpIdleWk = false
	}

	agg5h := aggregateAnnotatedSteps(steps5h)
	aggWeekly := aggregateAnnotatedSteps(stepsWeekly)
	gem5h := aggregateAnnotatedSteps(gemSteps5h)
	gemWeekly := aggregateAnnotatedSteps(gemStepsWeekly)
	tpCap5h := claudePricing.Cap5hUsd
	tpCapWk := claudePricing.CapWeeklyUsd
	tpUsed5h, tpRem5h := bucketFracs(tp5h)
	tpUsedWk, tpRemWk := bucketFracs(tpWk)
	tpPoolRem5h := pctUsdPtr(tpCap5h, tpRem5h)
	tpPoolRemWk := pctUsdPtr(tpCapWk, tpRemWk)
	tpEffRem, tpBottle := effectivePoolRem(tpPoolRem5h, tpPoolRemWk)
	tpBlended99 := 0.993*claudePricing.BaseCachedReadPerM + 0.007*claudePricing.InputPerM + 0.003*claudePricing.BaseOutputPerM
	tpBlended0 := 1.0*claudePricing.BaseInputPerM + 0.003*claudePricing.BaseOutputPerM
	tpp := &ThirdPartyPool{
		GroupName:                      "Claude models",
		Available:                      tpKnown,
		State:                          tpState.State,
		UpdatedAt:                      tpState.UpdatedAt,
		Error:                          tpState.Error,
		Cap5hUsd:                       tpCap5h,
		CapWeeklyUsd:                   tpCapWk,
		PoolUsed5hUsd:                  pctUsdPtr(tpCap5h, tpUsed5h),
		PoolRem5hUsd:                   tpPoolRem5h,
		PoolUsedWeeklyUsd:              pctUsdPtr(tpCapWk, tpUsedWk),
		PoolRemWeeklyUsd:               tpPoolRemWk,
		EffectiveRemUsd:                tpEffRem,
		Bottleneck:                     tpBottle,
		RemEffectiveTokensAt99PctCache: tokensPtr(tpEffRem, tpBlended99),
		RemEffectiveTokensAt0PctCache:  tokensPtr(tpEffRem, tpBlended0),
	}
	if thirdGroup != nil {
		tpp.GroupName = thirdGroup.Name
		tpp.Description = thirdGroup.Description
	}
	tpp.Window5h = buildPoolWindowAnnotated(tp5h, tpSteps5h)
	tpp.WindowWeekly = buildPoolWindowAnnotated(tpWk, tpStepsWk)
	tpp.Window5h.Idle = tpIdle5h
	tpp.WindowWeekly.Idle = tpIdleWk
	quota.ThirdPartyPool = tpp

	agg5h.Start = time.Unix(start5hSec, 0).UTC().Format(time.RFC3339)
	aggWeekly.Start = time.Unix(startWeeklySec, 0).UTC().Format(time.RFC3339)
	if b5h != nil {
		agg5h.Reset = b5h.ResetTime
		agg5h.Idle = idle5h && (!tpKnown || tpIdle5h)
	}
	if bWeekly != nil {
		aggWeekly.Reset = bWeekly.ResetTime
		aggWeekly.Idle = idleWeekly && (!tpKnown || tpIdleWk)
	}
	rolling5h := aggregateAnnotatedSteps(stepsRolling5h)
	rolling5h.Start = time.Unix(nowSec-5*3600, 0).UTC().Format(time.RFC3339)
	rolling7d := aggregateAnnotatedSteps(stepsRolling7d)
	rolling7d.Start = time.Unix(nowSec-7*86400, 0).UTC().Format(time.RFC3339)

	cap5h := gemPricing.Cap5hUsd
	capWeekly := gemPricing.CapWeeklyUsd
	used5h, rem5h := bucketFracs(b5h)
	usedWk, remWk := bucketFracs(bWeekly)
	poolRem5h := pctUsdPtr(cap5h, rem5h)
	poolRemWeekly := pctUsdPtr(capWeekly, remWk)
	gemEffRem, gemBottle := effectivePoolRem(poolRem5h, poolRemWeekly)

	blended99 := 0.993*gemPricing.BaseCachedReadPerM + 0.007*gemPricing.BaseInputPerM + 0.003*gemPricing.BaseOutputPerM
	blended0 := 1.0*gemPricing.BaseInputPerM + 0.003*gemPricing.BaseOutputPerM

	quota.Deduction = &QuotaDeduction{
		Cap5hUsd:                       cap5h,
		CapWeeklyUsd:                   capWeekly,
		PoolUsed5hUsd:                  pctUsdPtr(cap5h, used5h),
		PoolRem5hUsd:                   poolRem5h,
		LocalWsl5hUsd:                  roundTo(gem5h.WslCostUsd, 2),
		LocalWin5hUsd:                  roundTo(gem5h.WinCostUsd, 2),
		LocalClaudeGw5hUsd:             roundTo(gem5h.ClaudeGwCostUsd, 2),
		LocalWslHost5hUsd:              roundTo(gem5h.WslHostCostUsd, 2),
		LocalTotal5hUsd:                roundTo(gem5h.CostUsd, 2),
		LocalSteps5h:                   gem5h.StepsCount,
		PoolUsedWeeklyUsd:              pctUsdPtr(capWeekly, usedWk),
		PoolRemWeeklyUsd:               poolRemWeekly,
		LocalWslWeeklyUsd:              roundTo(gemWeekly.WslCostUsd, 2),
		LocalWinWeeklyUsd:              roundTo(gemWeekly.WinCostUsd, 2),
		LocalClaudeGwWeeklyUsd:         roundTo(gemWeekly.ClaudeGwCostUsd, 2),
		LocalWslHostWeeklyUsd:          roundTo(gemWeekly.WslHostCostUsd, 2),
		LocalTotalWeeklyUsd:            roundTo(gemWeekly.CostUsd, 2),
		LocalStepsWeekly:               gemWeekly.StepsCount,
		Rem5hTokensAt99PctCache:        tokensPtr(poolRem5h, blended99),
		Rem5hTokensAt0PctCache:         tokensPtr(poolRem5h, blended0),
		RemWeeklyTokensAt99PctCache:    tokensPtr(poolRemWeekly, blended99),
		RemWeeklyTokensAt0PctCache:     tokensPtr(poolRemWeekly, blended0),
		EffectiveRemUsd:                gemEffRem,
		Bottleneck:                     gemBottle,
		RemEffectiveTokensAt99PctCache: tokensPtr(gemEffRem, blended99),
		RemEffectiveTokensAt0PctCache:  tokensPtr(gemEffRem, blended0),
	}

	allTimeAgg := WindowAggregate{}
	if precomputedAllTime != nil {
		allTimeAgg = *precomputedAllTime
	} else {
		allTimeAgg = aggregateAnnotatedSteps(annotatedAll)
	}
	return quota, map[string]WindowAggregate{
		"window5h":     agg5h,
		"windowWeekly": aggWeekly,
		"rolling5h":    rolling5h,
		"rolling7d":    rolling7d,
		"last24h":      aggregateAnnotatedSteps(steps24h),
		"allTime":      allTimeAgg,
	}
}

// isServedModelMismatch reports whether upstream servedModel differs from the requested model.
// On cloudcode-pa, Claude models return "<modelId>-<tier>" (e.g. claude-opus-5-5-high) as their
// canonical modelVersion, whereas shadow/canary routing returns suffixed variants such as
// gemini-4-argon-b or gemini-3.8-flash-n.
func isServedModelMismatch(modelID, tier, served string) bool {
	if served == "" || modelID == "" {
		return false
	}
	if strings.EqualFold(served, modelID) {
		return false
	}
	if tier != "" && strings.EqualFold(served, modelID+"-"+tier) {
		return false
	}
	return true
}

func computeDbStepCacheMisses(allSteps []ParsedModelStep, gatewayTurns []map[string]any) []string {
	if len(allSteps) == 0 {
		return nil
	}
	sids := make([]string, 0, len(allSteps))
	seen := make(map[string]bool, 32)
	for _, s := range allSteps {
		if lsid := strings.ToLower(strings.TrimSpace(s.SessionID)); lsid != "" && !seen[lsid] {
			seen[lsid] = true
			sids = append(sids, lsid)
		}
	}
	byStep, byFirstSession := GlobalStore.QueryTurnCacheMissIndexForSessions(sids)
	return computeDbStepCacheMissesWithIndex(allSteps, gatewayTurns, byStep, byFirstSession)
}

func computeDbStepCacheMissesWithIndex(allSteps []ParsedModelStep, gatewayTurns []map[string]any, byStep map[TurnMissKey]string, byFirstSession map[string]string) []string {
	out := make([]string, len(allSteps))
	if len(allSteps) == 0 {
		return out
	}
	if byStep == nil {
		byStep = map[TurnMissKey]string{}
	}
	if byFirstSession == nil {
		byFirstSession = map[string]string{}
	}
	for _, t := range gatewayTurns {
		sid, _ := t["sessionId"].(string)
		miss, _ := t["cacheMiss"].(string)
		mode, _ := t["mode"].(string)
		if sid == "" || miss == "" {
			continue
		}
		lsid := strings.ToLower(sid)
		inT, _ := toInt(t["inputTokens"])
		crT, _ := toInt(t["cachedReadTokens"])
		outT, _ := toInt(t["outputTokens"])
		if inT > 0 || crT > 0 {
			byStep[TurnMissKey{SessionID: lsid, InTok: inT, CrTok: crT, OutTok: outT}] = miss
		}
		if mode == "compaction_rollover" || mode == "argon_degraded_recovery" || mode == "high_capacity_fallback" || mode == "waf_autobisect_recovery" || mode == "cold_start" {
			byFirstSession[lsid] = miss
		}
	}

	prevBySid := make(map[string]ParsedModelStep)
	for i, s := range allSteps {
		lsid := strings.ToLower(s.SessionID)

		in64, cr64, total, slack := stepTokenSplit(s)
		if total <= 0 {
			continue
		}
		prev, hasPrev := prevBySid[lsid]
		prevBySid[lsid] = s
		if !hasPrev && cr64 == 0 {
			if m, ok := byStep[TurnMissKey{SessionID: lsid, InTok: s.RawIn, CrTok: s.RawCr, OutTok: s.OutTok}]; ok && m != "" {
				out[i] = m
			} else if m, ok := byFirstSession[lsid]; ok && m != "" {
				out[i] = m
			} else {
				out[i] = "cold_start"
			}
			continue
		}
		if in64 <= slack {
			continue
		}
		if m, ok := byStep[TurnMissKey{SessionID: lsid, InTok: s.RawIn, CrTok: s.RawCr, OutTok: s.OutTok}]; ok && m != "" {
			out[i] = m
			continue
		}
		if m, ok := byStep[TurnMissKey{SessionID: lsid, InTok: s.AlignedIn, CrTok: s.AlignedCr, OutTok: s.OutTok}]; ok && m != "" {
			out[i] = m
			continue
		}
		if !hasPrev {
			if m, ok := byFirstSession[lsid]; ok && m != "" {
				out[i] = m
			} else {
				out[i] = "cold_start"
			}
			continue
		}
		prevIn, prevCr, _, _ := stepTokenSplit(prev)
		modelSwitched := prev.ModelID != "" && s.ModelID != "" && prev.ModelID != s.ModelID
		out[i] = classifyCacheMissSpec(prevIn, prevCr, in64, cr64, stepGapMs(prev, s), modelSwitched)
	}
	return out
}

var (
	dashStepsCacheMu    sync.Mutex
	dashCachedDBPath    string
	dashCachedConvDir   string
	dashCachedVer       uint64
	dashCachedGwCnt     int
	dashCachedRegSeq    uint64
	dashCachedTurnStamp int64
	dashLastDirSync     time.Time
	dashCacheLastAccess time.Time
	dashCacheIdleTTL    = 5 * time.Minute
	dashCachedAnnotated []annotatedStep
	dashCachedAllTime   WindowAggregate
	dashCachedRecent    []map[string]any
	dashCachedGen       uint64
	dashDirSyncRunning  atomic.Bool
	dashViewGen         uint64
	dashViewQuotaKey    string
	dashViewBuiltAt     time.Time
	dashViewQuota       QuotaSnapshot
	dashViewWindows     map[string]WindowAggregate
)

// EvictStaleDashboardCache drops cached dashboard step slices and view snapshots when no
// dashboard request has arrived for dashCacheIdleTTL (default 5m), returning idle heap to ~0
// and rebuilding transparently on the next view.
func EvictStaleDashboardCache(now time.Time) bool {
	if now.IsZero() {
		now = time.Now()
	}
	dashStepsCacheMu.Lock()
	defer dashStepsCacheMu.Unlock()
	if dashCacheIdleTTL <= 0 || dashCacheLastAccess.IsZero() || now.Sub(dashCacheLastAccess) < dashCacheIdleTTL {
		return false
	}
	if dashCachedAnnotated == nil && dashCachedRecent == nil && dashViewWindows == nil {
		return false
	}
	dashCachedAnnotated = nil
	dashCachedRecent = nil
	dashCachedAllTime = WindowAggregate{}
	dashViewGen = 0
	dashViewQuotaKey = ""
	dashViewBuiltAt = time.Time{}
	dashViewQuota = QuotaSnapshot{}
	dashViewWindows = nil
	return true
}

func cloneQuotaSnapshot(q QuotaSnapshot) QuotaSnapshot {
	out := q
	if q.GeminiPool != nil {
		cp := *q.GeminiPool
		out.GeminiPool = &cp
	}
	if q.ArgonPool != nil {
		cp := *q.ArgonPool
		out.ArgonPool = &cp
	}
	if q.Deduction != nil {
		cp := *q.Deduction
		if q.Deduction.BurnAnomaly5h != nil {
			ba := *q.Deduction.BurnAnomaly5h
			cp.BurnAnomaly5h = &ba
		}
		if q.Deduction.BurnAnomalyWeekly != nil {
			ba := *q.Deduction.BurnAnomalyWeekly
			cp.BurnAnomalyWeekly = &ba
		}
		out.Deduction = &cp
	}
	if q.ThirdPartyPool != nil {
		cp := *q.ThirdPartyPool
		out.ThirdPartyPool = &cp
	}
	return out
}

func cloneWindowMap(src map[string]WindowAggregate) map[string]WindowAggregate {
	if src == nil {
		return nil
	}
	out := make(map[string]WindowAggregate, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

func quotaViewCacheKey(q QuotaSnapshot, tierHint string) string {
	var b strings.Builder
	b.Grow(160)
	b.WriteString(q.Status)
	b.WriteByte('|')
	b.WriteString(q.UpdatedAt)
	b.WriteByte('|')
	b.WriteString(q.PlanTier)
	b.WriteByte('|')
	b.WriteString(tierHint)
	b.WriteByte('|')
	b.WriteString(q.Error)
	b.WriteByte('|')
	b.WriteString(q.SummaryDescription)
	for _, g := range q.Groups {
		b.WriteByte(';')
		b.WriteString(g.Name)
		for _, bk := range g.Buckets {
			b.WriteByte(',')
			b.WriteString(bk.Window)
			b.WriteByte('=')
			b.WriteString(bk.ResetTime)
			b.WriteByte(':')
			fmt.Fprintf(&b, "%.6f", bk.RemainingFraction)
		}
	}
	return b.String()
}

const recentDbStepsTailWindow = 240

func buildRecentDbSteps(allSteps []ParsedModelStep, gatewayTurns []map[string]any) []map[string]any {
	if len(allSteps) == 0 {
		return nil
	}
	tailStart := len(allSteps) - recentDbStepsTailWindow
	if tailStart < 0 {
		tailStart = 0
	}
	tailSteps := allSteps[tailStart:]

	tailSids := make([]string, 0, 32)
	seenSid := make(map[string]bool, 32)
	for _, s := range tailSteps {
		if lsid := strings.ToLower(strings.TrimSpace(s.SessionID)); lsid != "" && !seenSid[lsid] {
			seenSid[lsid] = true
			tailSids = append(tailSids, lsid)
		}
	}
	byStep, byFirstSession := GlobalStore.QueryTurnCacheMissIndexForSessions(tailSids)
	stepMisses := computeDbStepCacheMissesWithIndex(tailSteps, gatewayTurns, byStep, byFirstSession)
	clientBySid := GlobalStore.LookupSessionClientMapForSessions(tailSids)
	for _, gt := range gatewayTurns {
		sid := strings.ToLower(strings.TrimSpace(fmt.Sprintf("%v", gt["sessionId"])))
		if sid == "" || sid == "<nil>" || !seenSid[sid] {
			continue
		}
		if existing, ok := clientBySid[sid]; ok && existing.ClientLabel != "" {
			continue
		}
		cID, _ := gt["clientId"].(string)
		cLbl, _ := gt["clientLabel"].(string)
		cProto, _ := gt["protocol"].(string)
		convKey, _ := gt["convKey"].(string)
		if cID != "" || cLbl != "" {
			clientBySid[sid] = SessionClientMeta{
				ConvKey:     convKey,
				ClientID:    cID,
				ClientLabel: cLbl,
				Protocol:    cProto,
			}
		}
	}
	var recentDbSteps []map[string]any
	startIdx := len(tailSteps) - 120
	if startIdx < 0 {
		startIdx = 0
	}
	for i := len(tailSteps) - 1; i >= startIdx; i-- {
		s := tailSteps[i]
		alignedTot := s.AlignedIn + s.AlignedCr
		hitRateStr := "0.00%"
		if alignedTot > 0 {
			hitRateStr = formatPercent(float64(s.AlignedCr) / float64(alignedTot) * 100)
		}
		cm := clientBySid[strings.ToLower(s.SessionID)]
		if cm.ClientLabel == "" {
			switch s.SourceCategory {
			case "AGY_APP":
				cm.ClientID = "agy_app"
				cm.ClientLabel = "AGY App"
				cm.Protocol = "App"
			case "CLAUDE_GW":
				cm.ClientID = "gateway_api"
				cm.ClientLabel = "网关调用"
			default:
				cm.ClientID = "agy_host"
				cm.ClientLabel = "AGY CLI"
				cm.Protocol = "ACP"
			}
		}
		thinkingMs := 0
		if s.HasThinking && s.ThTok > 0 && s.OutTok >= s.ThTok && s.StreamMs > 0 {
			thinkingMs = int(math.Round(float64(s.StreamMs) * float64(s.ThTok) / float64(s.OutTok)))
		}
		visStreamMs := s.StreamMs - thinkingMs
		if visStreamMs < 0 {
			visStreamMs = 0
		}
		recentDbSteps = append(recentDbSteps, map[string]any{
			"ts":              time.Unix(s.TsSec, 0).UTC().Format(time.RFC3339),
			"sourceSide":      s.SourceSide,
			"sourceCategory":  s.SourceCategory,
			"sourceLabel":     s.SourceLabel,
			"sessionId":       s.SessionID,
			"convKey":         cm.ConvKey,
			"clientId":        cm.ClientID,
			"clientLabel":     cm.ClientLabel,
			"protocol":        cm.Protocol,
			"stepIdx":         s.StepIdx,
			"modelId":         s.ModelID,
			"servedModel":     s.ServedModel,
			"servedMismatch":  isServedModelMismatch(s.ModelID, s.ModelTier, s.ServedModel),
			"cacheMiss":       stepMisses[i],
			"modelLabel":      s.ModelLabel,
			"modelTier":       s.ModelTier,
			"quotaGroup":      s.QuotaGroup,
			"ttftMs":          s.TtftMs,
			"streamMs":        s.StreamMs,
			"thinkingMs":      thinkingMs,
			"visibleStreamMs": visStreamMs,
			"elapsedMs":       s.ElapsedMs,
			"tps":             s.Tps,
			"decodeTps":       s.DecodeTps,
			"alignedIn":       s.AlignedIn,
			"alignedCr":       s.AlignedCr,
			"outTok":          s.OutTok,
			"visOutTok":       s.VisOutTok,
			"thTok":           s.ThTok,
			"hasThinking":     s.HasThinking,
			"totalCtx":        alignedTot,
			"tierMultiplier":  s.TierMultiplier,
			"alignedHitRate":  hitRateStr,
			"inputCostUsd":    roundTo(s.InputCostUsd, 4),
			"cacheCostUsd":    roundTo(s.CacheCostUsd, 4),
			"outputCostUsd":   roundTo(s.OutputCostUsd, 4),
			"thinkCostUsd":    roundTo(s.ThinkCostUsd, 4),
			"costUsd":         roundTo(s.CostUsd, 4),
			"savedUsd":        roundTo(s.SavedUsd, 4),
		})
	}
	return recentDbSteps
}

func summarizeCanaryRoutes(liveSessions []map[string]any, rolling5h WindowAggregate) []map[string]any {
	type routeRec struct {
		modelID     string
		modelLabel  string
		servedModel string
		steps       int
		live        bool
	}
	byKey := map[string]*routeRec{}
	var order []string
	for _, s := range liveSessions {
		mm, _ := s["servedMismatch"].(bool)
		served, _ := s["servedModel"].(string)
		if !mm || served == "" {
			continue
		}
		mid, _ := s["model"].(string)
		mlbl, _ := s["modelLabel"].(string)
		k := mid + "->" + served
		rec := byKey[k]
		if rec == nil {
			rec = &routeRec{modelID: mid, modelLabel: mlbl, servedModel: served}
			byKey[k] = rec
			order = append(order, k)
		}
		rec.live = true
	}
	for _, m := range rolling5h.Models {
		if m.ServedMismatch <= 0 {
			continue
		}
		for _, sv := range m.ServedVariants {
			k := m.ModelID + "->" + sv
			rec := byKey[k]
			if rec == nil {
				rec = &routeRec{modelID: m.ModelID, modelLabel: m.ModelLabel, servedModel: sv}
				byKey[k] = rec
				order = append(order, k)
			}
			cnt := m.ServedMismatch
			if len(m.ServedVariantCounts) > 0 {
				cnt = m.ServedVariantCounts[sv]
			}
			if cnt > rec.steps {
				rec.steps = cnt
			}
		}
	}
	out := make([]map[string]any, 0, len(order))
	for _, k := range order {
		r := byKey[k]
		out = append(out, map[string]any{
			"modelId":     r.modelID,
			"modelLabel":  r.modelLabel,
			"servedModel": r.servedModel,
			"steps":       r.steps,
			"live":        r.live,
		})
	}
	return out
}

func summarizeCacheWarnings(liveSessions []map[string]any) []map[string]any {
	out := make([]map[string]any, 0)
	for _, s := range liveSessions {
		active, _ := s["cacheWarning"].(bool)
		if !active {
			continue
		}
		out = append(out, map[string]any{
			"convKey":        s["convKey"],
			"sessionId":      s["sessionId"],
			"modelId":        s["model"],
			"modelLabel":     s["modelLabel"],
			"servedModel":    s["servedModel"],
			"streak":         s["zeroCacheStreak"],
			"uncachedTokens": s["zeroCacheTokens"],
			"contextTokens":  s["contextTokens"],
		})
	}
	return out
}

func mergeKnownAndLiveGwSids(liveGwSids map[string]bool) map[string]bool {
	out := getKnownGatewaySids()
	for k, v := range liveGwSids {
		out[k] = v
	}
	return out
}

func readFallbackStepsFromDir(convDir, sourceSide, defaultCat, defaultLabel string, gwSids map[string]bool) []ParsedModelStep {
	if strings.TrimSpace(convDir) == "" {
		return nil
	}
	entries, err := os.ReadDir(convDir)
	if err != nil {
		return nil
	}
	dbStems := make(map[string]bool)
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".db") {
			if info, errI := e.Info(); errI == nil && info.Size() >= 100 {
				dbStems[strings.ToLower(strings.TrimSuffix(e.Name(), ".db"))] = true
			}
		}
	}
	var out []ParsedModelStep
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.HasSuffix(name, ".db") {
			dbPath := filepath.Join(convDir, name)
			out = append(out, GlobalDBCache.GetSteps(dbPath, sourceSide, gwSids)...)
		} else if strings.HasSuffix(name, ".pb") {
			sid := strings.ToLower(strings.TrimSuffix(name, ".pb"))
			if dbStems[sid] {
				continue
			}
			pbPath := filepath.Join(convDir, name)
			var fallbackTs int64
			if info, errI := e.Info(); errI == nil {
				fallbackTs = info.ModTime().Unix()
			}
			steps, _ := ReadModelStepsFromProtobuf(pbPath, sourceSide, defaultCat, defaultLabel, gwSids, -1, fallbackTs)
			out = append(out, steps...)
		}
	}
	return out
}

func sortModelStepsChronological(steps []ParsedModelStep) {
	sort.Slice(steps, func(i, j int) bool {
		if steps[i].TsSec != steps[j].TsSec {
			return steps[i].TsSec < steps[j].TsSec
		}
		if steps[i].StepIdx != steps[j].StepIdx {
			return steps[i].StepIdx < steps[j].StepIdx
		}
		return steps[i].SessionID < steps[j].SessionID
	})
}

const quotaBurnAnomalyLogDebounce = 10 * time.Minute

var (
	burnAnomalyLogMu   sync.Mutex
	burnAnomalyLogLast = map[string]time.Time{}
)

// detectQuotaBurnAnomaly evaluates a single Gemini quota window (5h or weekly) against expected
// local+monitor spend to catch billing multiplier regressions (e.g. agy#1890) or large unattributed burn.
func detectQuotaBurnAnomaly(poolUsed *float64, localTotal, monitorEst, capUsd float64, idle bool, agyVersion string) *QuotaBurnAnomaly {
	if idle || poolUsed == nil || *poolUsed <= 0 || capUsd <= 0 {
		return nil
	}
	used := roundTo(*poolUsed, 2)
	expected := roundTo(localTotal+monitorEst, 2)
	minExp := math.Max(2.00, capUsd*0.02)
	minExcess := math.Max(2.00, capUsd*0.015)

	verSuffix := ""
	if agyVersion != "" {
		verSuffix = " · agy " + agyVersion
	}

	if expected >= minExp {
		excess := used - expected
		if excess >= minExcess {
			ratio := used / expected
			if ratio >= 2.2 {
				sev := "warn"
				if ratio >= 3.0 {
					sev = "critical"
				}
				ratioR := roundTo(ratio, 2)
				return &QuotaBurnAnomaly{
					Kind:        "multiplier_anomaly",
					Severity:    sev,
					PoolUsedUsd: used,
					ExpectedUsd: expected,
					Ratio:       ratioR,
					AgyVersion:  agyVersion,
					Detail:      fmt.Sprintf("实扣 $%.2f vs 预期 $%.2f (%.2fx)%s", used, expected, ratioR, verSuffix),
				}
			}
		}
		return nil
	}

	minUnattributed := math.Max(6.00, capUsd*0.04)
	if used >= minUnattributed {
		ratioR := 0.0
		if expected > 0.01 {
			ratioR = roundTo(used/expected, 2)
		}
		return &QuotaBurnAnomaly{
			Kind:        "unattributed_burn",
			Severity:    "warn",
			PoolUsedUsd: used,
			ExpectedUsd: expected,
			Ratio:       ratioR,
			AgyVersion:  agyVersion,
			Detail:      fmt.Sprintf("实扣 $%.2f vs 预期 $%.2f (未归因/他端消耗)%s", used, expected, verSuffix),
		}
	}
	return nil
}

func logQuotaBurnAnomalyDebounced(accountID, window string, a *QuotaBurnAnomaly, now time.Time) {
	if a == nil {
		return
	}
	if now.IsZero() {
		now = time.Now()
	}
	key := accountID + "|" + window + "|" + a.Kind + "|" + a.Severity
	burnAnomalyLogMu.Lock()
	last, seen := burnAnomalyLogLast[key]
	if seen && now.Sub(last) < quotaBurnAnomalyLogDebounce {
		burnAnomalyLogMu.Unlock()
		return
	}
	burnAnomalyLogLast[key] = now
	burnAnomalyLogMu.Unlock()

	LogEvent("QUOTA_BURN_ANOMALY", fmt.Sprintf("[%s] %s %s %s: %s", strings.ToUpper(a.Severity), accountID, window, a.Kind, a.Detail), map[string]any{
		"accountId":   accountID,
		"window":      window,
		"kind":        a.Kind,
		"severity":    a.Severity,
		"poolUsedUsd": a.PoolUsedUsd,
		"expectedUsd": a.ExpectedUsd,
		"ratio":       a.Ratio,
		"agyVersion":  a.AgyVersion,
	})
}

func isAccountBurnAnomalyIgnored(accountID string) bool {
	id := strings.ToLower(strings.TrimSpace(accountID))
	if !isChildAccountWorker() && GlobalAccountManager != nil && GlobalAccountManager.HasConfiguredAccounts() && id != "" {
		if acc, ok := GlobalAccountManager.GetAccount(id); ok {
			return acc.IgnoreBurnAnomaly
		}
	}
	if v, ok := runtimeIgnoreBurnAnomalyOverride(); ok {
		return v
	}
	return gwcfg.Current().IgnoreBurnAnomaly
}

func attachQuotaBurnAnomalies(quota *QuotaSnapshot, accountID, agyVersion string, now time.Time) {
	if quota == nil || quota.Deduction == nil {
		return
	}
	quota.Deduction.BurnAnomaly5h = nil
	quota.Deduction.BurnAnomalyWeekly = nil
	if isAccountBurnAnomalyIgnored(accountID) {
		return
	}
	if quota.GeminiPool == nil || quota.GeminiPool.State != QuotaStateKnown {
		return
	}
	geminiGroup := findGroup(quota.Groups, reGeminiModelsGroup)
	if geminiGroup == nil {
		geminiGroup = findGroup(quota.Groups, reGeminiFallbackGroup)
	}
	b5h := findBucket(geminiGroup, "5h")
	bWeekly := findBucket(geminiGroup, "weekly")
	idle5h := quotaWindowIdle(b5h, quota.UpdatedAt, 5*time.Hour) && quota.Deduction.LocalSteps5h == 0
	idleWeekly := quotaWindowIdle(bWeekly, quota.UpdatedAt, 7*24*time.Hour) && quota.Deduction.LocalStepsWeekly == 0

	a5 := detectQuotaBurnAnomaly(
		quota.Deduction.PoolUsed5hUsd,
		quota.Deduction.LocalTotal5hUsd,
		quota.Deduction.MonitorEst5hUsd,
		quota.Deduction.Cap5hUsd,
		idle5h,
		agyVersion,
	)
	quota.Deduction.BurnAnomaly5h = a5
	if a5 != nil {
		logQuotaBurnAnomalyDebounced(accountID, "5h", a5, now)
	}

	aw := detectQuotaBurnAnomaly(
		quota.Deduction.PoolUsedWeeklyUsd,
		quota.Deduction.LocalTotalWeeklyUsd,
		quota.Deduction.MonitorEstWeeklyUsd,
		quota.Deduction.CapWeeklyUsd,
		idleWeekly,
		agyVersion,
	)
	quota.Deduction.BurnAnomalyWeekly = aw
	if aw != nil {
		logQuotaBurnAnomalyDebounced(accountID, "weekly", aw, now)
	}
}

func collectQuotaBurnAnomalies(accountID, accountLabel string, q QuotaSnapshot) []map[string]any {
	out := make([]map[string]any, 0)
	if q.Deduction == nil {
		return out
	}
	appendOne := func(win string, a *QuotaBurnAnomaly) {
		if a == nil {
			return
		}
		out = append(out, map[string]any{
			"accountId":    accountID,
			"accountLabel": accountLabel,
			"window":       win,
			"kind":         a.Kind,
			"severity":     a.Severity,
			"poolUsedUsd":  a.PoolUsedUsd,
			"expectedUsd":  a.ExpectedUsd,
			"ratio":        a.Ratio,
			"agyVersion":   a.AgyVersion,
			"detail":       a.Detail,
		})
	}
	appendOne("5h", q.Deduction.BurnAnomaly5h)
	appendOne("weekly", q.Deduction.BurnAnomalyWeekly)
	return out
}

func resolveGatewayBinaryStatus(eng acp.Engine) (agyVersion string, engineAlive bool, switchPending bool) {
	if eng == nil {
		eng = GlobalEngine
	}
	engInfo := eng.BinaryInfo()
	engineAlive, _ = engInfo["alive"].(bool)
	switchPending = acpBinarySwitchPendingOn(eng)
	binPath := ""
	if engineAlive {
		if r, _ := engInfo["running"].(string); r != "" {
			binPath = r
		}
	}
	if binPath == "" {
		if c, _ := engInfo["current"].(string); c != "" {
			binPath = c
		}
	}
	if binPath == "" {
		binPath = PatchedAgyBin
	}
	if binPath != "" {
		if _, err := os.Stat(binPath); err == nil {
			agyVersion = detectAgySemver(binPath)
		}
	}
	return agyVersion, engineAlive, switchPending
}

func BuildDashboardPayload(gw *GatewayServer, forceQuota bool) map[string]any {
	var quota QuotaSnapshot
	if primaryAccountDisabled() {
		quota = QuotaSnapshot{
			Status:    QuotaStateNotEnabled,
			UpdatedAt: time.Now().UTC().Format(time.RFC3339),
			Error:     "账号已停用",
			Groups:    []QuotaGroup{},
		}
	} else {
		quota = FetchOfficialQuota(forceQuota)
	}
	liveGwSids := make(map[string]bool)

	liveSessions, gatewayTurns := gw.CollectDashboardTelemetry(liveGwSids)

	var latestTurnStamp int64
	if len(gatewayTurns) > 0 {
		latestTurnStamp, _ = gatewayTurns[0]["completedAtMs"].(int64)
	}

	var allSteps []ParsedModelStep
	var annotatedAll []annotatedStep
	var allTimeAgg WindowAggregate
	hasPrecomputedAllTime := false
	var stepsGen uint64
	var recentDbSteps []map[string]any
	convDir := ConversationsDir()
	appConvDir := AppConversationsDir()
	combinedConvDir := convDir + "\x00" + appConvDir + "\x00" + gwcfg.Current().AppConvDirRaw
	if GlobalStore.IsReady() {
		dbPath := GlobalStore.DBPath()
		now := time.Now()
		dashStepsCacheMu.Lock()
		dirSyncInterval := 2 * time.Second
		if !testing.Testing() {
			dirSyncInterval = 15 * time.Second
		}
		needDirSync := forceQuota || dbPath != dashCachedDBPath || combinedConvDir != dashCachedConvDir || now.Sub(dashLastDirSync) >= dirSyncInterval
		if needDirSync {
			dashLastDirSync = now
		}
		dashStepsCacheMu.Unlock()

		var gwSids map[string]bool
		if needDirSync {
			gwSids = mergeKnownAndLiveGwSids(liveGwSids)
			if testing.Testing() {
				if convDir != "" {
					GlobalStore.SyncUsageStepsFromDirWithRODB(convDir, gwSids, gw.LookupReadOnlySessionDB)
				}
				if appConvDir != "" {
					GlobalStore.SyncUsageStepsFromSourceDir(appConvDir, "APP", "AGY_APP", "Antigravity App", gwSids)
				}
			} else if dashDirSyncRunning.CompareAndSwap(false, true) {
				// Non-blocking catch-up directory scan on production dashboard polls:
				// gateway turns already write steps directly to SQLite, so scanning thousands
				// of conversation .db/.db-wal files must never stall /api/dashboard.
				sidsCopy := make(map[string]bool, len(gwSids))
				for k, v := range gwSids {
					sidsCopy[k] = v
				}
				storeRef := GlobalStore
				go func(st *StateStore, cDir, aDir string, sids map[string]bool, g *GatewayServer) {
					defer dashDirSyncRunning.Store(false)
					if st == nil || !st.IsReady() {
						return
					}
					if cDir != "" {
						st.SyncUsageStepsFromDirWithRODB(cDir, sids, g.LookupReadOnlySessionDB)
					}
					if aDir != "" {
						st.SyncUsageStepsFromSourceDir(aDir, "APP", "AGY_APP", "Antigravity App", sids)
					}
				}(storeRef, convDir, appConvDir, sidsCopy, gw)
			}
		}
		curVer := GlobalStore.Version()
		curRegSeq := gwSidRegSeq.Load()

		dashStepsCacheMu.Lock()
		dashCacheLastAccess = now
		needReload := dbPath != dashCachedDBPath ||
			combinedConvDir != dashCachedConvDir ||
			curVer != dashCachedVer ||
			len(liveGwSids) != dashCachedGwCnt ||
			curRegSeq != dashCachedRegSeq ||
			latestTurnStamp != dashCachedTurnStamp ||
			dashCachedAnnotated == nil
		if !needReload {
			annotatedAll = dashCachedAnnotated
			allTimeAgg = dashCachedAllTime
			hasPrecomputedAllTime = true
			stepsGen = dashCachedGen
			recentDbSteps = dashCachedRecent
			dashStepsCacheMu.Unlock()
		} else {
			dashStepsCacheMu.Unlock()
			if gwSids == nil {
				gwSids = mergeKnownAndLiveGwSids(liveGwSids)
			}
			requestedCutoff := time.Now().Unix() - activeUsageStepsWindowSec
			coldAgg, coldTail, cutoffUsed, okCold := GlobalStore.QueryColdWindowRollup(gwSids, requestedCutoff)
			if !okCold {
				cutoffUsed = requestedCutoff
			}
			if indexed, ok := GlobalStore.QueryActiveUsageStepsSince(gwSids, cutoffUsed); ok {
				allSteps = indexed
			}
			sortModelStepsChronological(allSteps)
			if okCold && coldAgg.StepsCount > 0 {
				annotatedAll = annotateSteadyStepsWithSeed(allSteps, coldTail)
				activeAgg := aggregateAnnotatedSteps(annotatedAll)
				allTimeAgg = mergeWindowAggregates([]WindowAggregate{coldAgg, activeAgg})
			} else {
				annotatedAll = annotateSteadySteps(allSteps)
				allTimeAgg = aggregateAnnotatedSteps(annotatedAll)
			}
			hasPrecomputedAllTime = true
			recentDbSteps = buildRecentDbSteps(allSteps, gatewayTurns)
			allSteps = nil
			dashStepsCacheMu.Lock()
			dashCachedDBPath = dbPath
			dashCachedConvDir = combinedConvDir
			dashCachedVer = curVer
			dashCachedGwCnt = len(liveGwSids)
			dashCachedRegSeq = curRegSeq
			dashCachedTurnStamp = latestTurnStamp
			dashCachedAnnotated = annotatedAll
			dashCachedAllTime = allTimeAgg
			dashCachedRecent = recentDbSteps
			dashCacheLastAccess = time.Now()
			dashCachedGen++
			stepsGen = dashCachedGen
			dashStepsCacheMu.Unlock()
		}
	} else {
		gwSids := mergeKnownAndLiveGwSids(liveGwSids)
		allSteps = append(allSteps, readFallbackStepsFromDir(convDir, "HOST", "AGY_CLI", "AGY CLI", gwSids)...)
		if appConvDir != "" {
			allSteps = append(allSteps, readFallbackStepsFromDir(appConvDir, "APP", "AGY_APP", "Antigravity App", gwSids)...)
		}
		sortModelStepsChronological(allSteps)
		annotatedAll = annotateSteadySteps(allSteps)
		allTimeAgg = aggregateAnnotatedSteps(annotatedAll)
		hasPrecomputedAllTime = true
		recentDbSteps = buildRecentDbSteps(allSteps, gatewayTurns)
	}

	now := time.Now()
	var windows map[string]WindowAggregate
	hitViewCache := false
	var viewKey string
	if stepsGen > 0 {
		viewKey = quotaViewCacheKey(quota, authoritativeAccountPlanTierOverride()+"|"+authoritativeAccountPlanTierHint())
	}
	if !forceQuota && stepsGen > 0 {
		dashStepsCacheMu.Lock()
		if dashViewGen == stepsGen && dashViewQuotaKey == viewKey && !dashViewBuiltAt.IsZero() && now.Sub(dashViewBuiltAt) >= 0 && now.Sub(dashViewBuiltAt) < 2*time.Second {
			quota = cloneQuotaSnapshot(dashViewQuota)
			windows = cloneWindowMap(dashViewWindows)
			hitViewCache = true
		}
		dashStepsCacheMu.Unlock()
	}
	if !hitViewCache {
		var allTimePtr *WindowAggregate
		if hasPrecomputedAllTime {
			allTimePtr = &allTimeAgg
		}
		quota, windows = buildQuotaViewAnnotated(quota, annotatedAll, allTimePtr, now)
		if stepsGen > 0 {
			dashStepsCacheMu.Lock()
			dashViewGen = stepsGen
			dashViewQuotaKey = viewKey
			dashViewBuiltAt = now
			dashViewQuota = cloneQuotaSnapshot(quota)
			dashViewWindows = cloneWindowMap(windows)
			dashStepsCacheMu.Unlock()
		}
	}
	if !testing.Testing() {
		// Pre-warm monitor log cache asynchronously even while /usage is still loading.
		_ = collectAgyMonitorEventsNonBlocking(agyMonitorLogDir(), now, time.Local, false)
	}
	attachAgyMonitorQuotaDeduction(&quota, agyMonitorLogDir(), now, time.Local, forceQuota)
	snap := gwcfg.Current()
	agyVersion, engineAlive, binarySwitchPending := resolveGatewayBinaryStatus(gw.engine())
	selfAccID := currentProcessAccountID()
	attachQuotaBurnAnomalies(&quota, selfAccID, agyVersion, now)
	burnAnomalies := collectQuotaBurnAnomalies(snap.InstanceID, snap.InstanceLabel, quota)
	canaryRoutes := summarizeCanaryRoutes(liveSessions, windows["rolling5h"])
	cacheWarnings := summarizeCacheWarnings(liveSessions)
	tosTripped, tosReason := IsTosTripped()
	slotSnap := gw.CollectWorkerSlots(now)
	memStat := readCgroupMemoryStatus()
	globalMaxW := effectiveGlobalMaxLiveWorkers()
	if globalMaxW <= 0 {
		globalMaxW = effectiveMaxLiveWorkers()
	}

	return map[string]any{
		"now": time.Now().UTC().Format(time.RFC3339),
		"gateway": map[string]any{
			"instanceId":                snap.InstanceID,
			"instanceLabel":             snap.InstanceLabel,
			"port":                      GatewayPort,
			"binary":                    PatchedAgyBin,
			"agyVersion":                agyVersion,
			"engineAlive":               engineAlive,
			"binarySwitchPending":       binarySwitchPending,
			"engine":                    "Go Native",
			"uiBuildId":                 DashboardBuildID(),
			"defaultModel":              DefaultModelID,
			"models":                    AgyModels,
			"contextWindowMax":          ModelOrDefault(DefaultModelID).ContextWindow,
			"activeSessionsCount":       len(liveSessions),
			"maxLiveWorkers":            effectiveMaxLiveWorkers(),
			"maxConcurrentPrompts":      effectiveMaxConcurrentPrompts(),
			"globalLiveWorkers":         slotSnap.LiveWorkers,
			"globalMaxLiveWorkers":      globalMaxW,
			"globalLogicalSessions":     slotSnap.LogicalSessions,
			"globalHibernatedSessions":  slotSnap.HibernatedSessions,
			"cgroupWorkingSetBytes":     memStat.WorkingSetBytes,
			"cgroupAnonBytes":           memStat.AnonBytes,
			"cgroupFileBytes":           memStat.FileBytes,
			"cgroupLimitBytes":          memStat.LimitBytes,
			"cgroupUsagePct":            roundTo(memStat.UsagePct, 2),
			"tosTripped":                tosTripped,
			"tosTrippedReason":          tosReason,
			"argonDegradationDetected":  argonDegradationDetectedTotal.Load(),
			"argonDegradationRecovered": argonDegradationRecoveredTotal.Load(),
			"cacheBurnWarnings":         cacheBurnWarningsTotal.Load(),
			"wafAutoBisectHealed":       WafBisectHealedTotal.Load(),
		},
		"quota":              quota,
		"windows":            windows,
		"canaryRoutes":       canaryRoutes,
		"cacheWarnings":      cacheWarnings,
		"quotaBurnAnomalies": burnAnomalies,
		"liveSessions":       liveSessions,
		"gatewayTurns":       gatewayTurns,
		"recentDbSteps":      recentDbSteps,
	}
}
