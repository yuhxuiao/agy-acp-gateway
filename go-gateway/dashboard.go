package main

import (
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
	"time"

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

// QuotaDeduction converts the official pool fractions into dollars and runnable tokens. The pool
// fields are nil whenever the matching official bucket is unknown (read failure, group not enabled
// or window missing), so the dashboard never presents an unknown quota as a full pool.
type QuotaDeduction struct {
	Cap5hUsd                       float64  `json:"cap5hUsd"`
	CapWeeklyUsd                   float64  `json:"capWeeklyUsd"`
	PoolUsed5hUsd                  *float64 `json:"poolUsed5hUsd"`
	PoolRem5hUsd                   *float64 `json:"poolRem5hUsd"`
	LocalWsl5hUsd                  float64  `json:"localWsl5hUsd"`
	LocalWin5hUsd                  float64  `json:"localWin5hUsd"`
	LocalClaudeGw5hUsd             float64  `json:"localClaudeGw5hUsd"`
	LocalWslHost5hUsd              float64  `json:"localWslHost5hUsd"`
	LocalTotal5hUsd                float64  `json:"localTotal5hUsd"`
	LocalSteps5h                   int      `json:"localSteps5h"`
	PoolUsedWeeklyUsd              *float64 `json:"poolUsedWeeklyUsd"`
	PoolRemWeeklyUsd               *float64 `json:"poolRemWeeklyUsd"`
	LocalWslWeeklyUsd              float64  `json:"localWslWeeklyUsd"`
	LocalWinWeeklyUsd              float64  `json:"localWinWeeklyUsd"`
	LocalClaudeGwWeeklyUsd         float64  `json:"localClaudeGwWeeklyUsd"`
	LocalWslHostWeeklyUsd          float64  `json:"localWslHostWeeklyUsd"`
	LocalTotalWeeklyUsd            float64  `json:"localTotalWeeklyUsd"`
	LocalStepsWeekly               int      `json:"localStepsWeekly"`
	Rem5hTokensAt99PctCache        *int64   `json:"rem5hTokensAt99PctCache"`
	Rem5hTokensAt0PctCache         *int64   `json:"rem5hTokensAt0PctCache"`
	RemWeeklyTokensAt99PctCache    *int64   `json:"remWeeklyTokensAt99PctCache"`
	RemWeeklyTokensAt0PctCache     *int64   `json:"remWeeklyTokensAt0PctCache"`
	EffectiveRemUsd                *float64 `json:"effectiveRemUsd"`
	Bottleneck                     string   `json:"bottleneck,omitempty"`
	RemEffectiveTokensAt99PctCache *int64   `json:"remEffectiveTokensAt99PctCache"`
	RemEffectiveTokensAt0PctCache  *int64   `json:"remEffectiveTokensAt0PctCache"`
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
	Status         string          `json:"status"` // ok | stale (cached reading after a failed refresh) | error
	UpdatedAt      string          `json:"updatedAt"`
	PlanTier       string          `json:"planTier,omitempty"`  // "pro" | "ultra"
	PlanLabel      string          `json:"planLabel,omitempty"` // "AI Pro" | "Ultra 20x"
	Error          string          `json:"error,omitempty"`
	LastErrorAt    string          `json:"lastErrorAt,omitempty"`
	GeminiPool     *PoolQuotaState `json:"geminiPool,omitempty"`
	ArgonPool      *ExtraPoolQuota `json:"argonPool,omitempty"`
	Groups         []QuotaGroup    `json:"groups"`
	Deduction      *QuotaDeduction `json:"deduction,omitempty"`
	ThirdPartyPool *ThirdPartyPool `json:"thirdPartyPool,omitempty"`
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
	cachedQuota        *QuotaSnapshot
	cachedQuotaAt      time.Time
	lastQuotaErr       *QuotaSnapshot
	lastQuotaAttemptAt time.Time
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
	logPath := GatewayLogFile
	storeReady := GlobalStore.IsReady()
	dbPath := ""
	var curVer uint64
	if storeReady {
		dbPath = GlobalStore.DBPath()
		curVer = GlobalStore.Version()
	}

	gwSidMu.RLock()
	if logPath == knownGwSidsLogPath && (!storeReady || (dbPath == knownGwSidsDBPath && curVer == knownGwSidsVer)) {
		out := snapshotKnownGwSidsLocked(storeReady)
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
	if storeReady && (dbPath != knownGwSidsDBPath || curVer != knownGwSidsVer) {
		if dbPath != knownGwSidsDBPath {
			storeGwSids = make(map[string]bool)
		}
		GlobalStore.LoadKnownGatewaySids(storeGwSids)
		knownGwSidsDBPath = dbPath
		knownGwSidsVer = curVer
	}
	return snapshotKnownGwSidsLocked(storeReady)
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
	if !lastQuotaAttemptAt.IsZero() && (cachedQuotaAt.IsZero() || lastQuotaAttemptAt.After(cachedQuotaAt)) {
		attemptAge = time.Since(lastQuotaAttemptAt)
	}
	quotaMu.Unlock()

	// Non-blocking stale-while-revalidate for dashboard polling:
	// Never block GET /api/dashboard on the 5-22s /usage subprocess when force=false.
	if !force {
		if hasCache {
			lastTurn := lastGatewayTurn.Load()
			resetPassed := age >= 30*time.Second && quotaHasExpiredReset(snap, time.Now())
			needBg := attemptAge >= 30*time.Second && (age >= 15*time.Minute || (age >= 3*time.Minute && lastTurn > cachedQuotaAt.Unix()) || resetPassed)
			if needBg && quotaRefreshing.CompareAndSwap(false, true) {
				go func() {
					defer quotaRefreshing.Store(false)
					_ = fetchOfficialQuotaSync()
				}()
			}
			return snap
		}
		// Cold start or previous error: trigger background refresh (with 30s error cooldown) and return immediately.
		needBg := !hasErr || attemptAge >= 30*time.Second
		if needBg && quotaRefreshing.CompareAndSwap(false, true) {
			go func() {
				defer quotaRefreshing.Store(false)
				_ = fetchOfficialQuotaSync()
			}()
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

func fetchOfficialQuotaSync() QuotaSnapshot {
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

	res := fetchOfficialQuotaDo()

	quotaFlightMu.Lock()
	quotaFlightRes = res
	quotaFlightWait = nil
	close(waitCh)
	quotaFlightMu.Unlock()
	return res
}

// quotaCmdEnv builds the subprocess environment for `<agy> -p /usage`.
// When PureApiHome holds an isolated gateway account token (antigravity-cli/antigravity-oauth-token),
// it points HOME at a lightweight sibling quota-home whose .gemini symlinks to PureApiHome so /usage
// reads the active gateway account instead of a stale/unlogged host ~/.gemini profile (which would
// otherwise block on interactive OAuth until the 22s context timeout sends SIGKILL / "signal: killed").
func quotaCmdEnv(baseEnv []string) []string {
	env := make([]string, 0, len(baseEnv)+8)
	hasSSH := false
	var quotaHome string
	if PureApiHome != "" {
		tokPath := filepath.Join(PureApiHome, "antigravity-cli", "antigravity-oauth-token")
		if fileExists(tokPath) {
			qh := filepath.Join(filepath.Dir(PureApiHome), "quota-home")
			linkPath := filepath.Join(qh, ".gemini")
			if err := os.MkdirAll(qh, 0o700); err == nil {
				if cur, err := os.Readlink(linkPath); err != nil || cur != PureApiHome {
					_ = os.Remove(linkPath)
					_ = os.Symlink(PureApiHome, linkPath)
				}
				quotaHome = qh
			}
		}
	}
	for _, kv := range baseEnv {
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
	env = append(env, "GOGC=30", "GOMEMLIMIT=120MiB")
	if proxy := gwcfg.Current().UpstreamProxy; proxy != "" {
		env = append(env, "HTTP_PROXY="+proxy, "HTTPS_PROXY="+proxy, "NO_PROXY=127.0.0.1,localhost,::1")
	}
	return env
}

func fetchOfficialQuotaDo() QuotaSnapshot {
	ctx, cancel := context.WithTimeout(context.Background(), 22*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, quotaAgyBin(), "-p", "/usage", "--output-format", "json")
	cmd.Env = quotaCmdEnv(os.Environ())
	out, err := cmd.Output()
	if err != nil || len(out) == 0 {
		errMsg := "Empty /usage output"
		if ctx.Err() == context.DeadlineExceeded {
			errMsg = "timeout (22s) running /usage (check auth/proxy)"
		} else if err != nil {
			errMsg = err.Error()
		}
		quotaMu.Lock()
		defer quotaMu.Unlock()
		lastQuotaAttemptAt = time.Now()
		if cachedQuota != nil {
			return markQuotaStaleLocked(errMsg)
		}
		errSnap := QuotaSnapshot{
			Status:    "error",
			UpdatedAt: time.Now().UTC().Format(time.RFC3339),
			Error:     errMsg,
			Groups:    []QuotaGroup{},
		}
		lastQuotaErr = &errSnap
		return errSnap
	}

	var raw struct {
		Command struct {
			Data struct {
				Groups []struct {
					Name        string `json:"name"`
					Description string `json:"description"`
					Buckets     []struct {
						ID                string   `json:"id"`
						Name              string   `json:"name"`
						Window            string   `json:"window"`
						RemainingFraction *float64 `json:"remaining_fraction"`
						ResetTime         string   `json:"reset_time"`
					} `json:"buckets"`
				} `json:"groups"`
			} `json:"data"`
		} `json:"command"`
	}
	if err := json.Unmarshal(out, &raw); err != nil {
		quotaMu.Lock()
		defer quotaMu.Unlock()
		lastQuotaAttemptAt = time.Now()
		if cachedQuota != nil {
			return markQuotaStaleLocked("Unparseable /usage output: " + err.Error())
		}
		return QuotaSnapshot{
			Status:    "error",
			UpdatedAt: time.Now().UTC().Format(time.RFC3339),
			Error:     err.Error(),
			Groups:    []QuotaGroup{},
		}
	}

	var groups []QuotaGroup
	for _, g := range raw.Command.Data.Groups {
		var bkts []QuotaBucket
		for _, b := range g.Buckets {
			rem := 0.0
			if b.RemainingFraction != nil {
				rem = *b.RemainingFraction
			}
			used := math.Max(0, 1.0-rem)
			bkts = append(bkts, QuotaBucket{
				ID:                b.ID,
				Name:              b.Name,
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
		Status:    "ok",
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
		Groups:    groups,
	}
	quotaMu.Lock()
	cachedQuota = &snap
	cachedQuotaAt = time.Now()
	lastQuotaErr = nil
	lastQuotaAttemptAt = cachedQuotaAt
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

// annotateSteadySteps marks continuation steps across chronological steps before window slicing so
// rolling windows retain warm continuation steps and in-session model_switch cold prefixes are excluded.
func annotateSteadySteps(steps []ParsedModelStep) []annotatedStep {
	out := make([]annotatedStep, len(steps))
	lastModelBySid := make(map[string]string, 64)
	prevBySid := make(map[string]ParsedModelStep, 64)
	for i, s := range steps {
		in64, cr64, total, slack := stepTokenSplit(s)
		if total <= 0 {
			out[i] = annotatedStep{step: s, steady: false}
			continue
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
		out[i] = annotatedStep{
			step:   s,
			steady: isSteady,
			inTok:  in64,
			crTok:  cr64,
			slack:  slack,
			miss:   miss,
		}
	}
	return out
}

func AggregateSteps(steps []ParsedModelStep) WindowAggregate {
	return aggregateAnnotatedSteps(annotateSteadySteps(steps))
}

func aggregateAnnotatedSteps(items []annotatedStep) WindowAggregate {
	var agg WindowAggregate
	var ttftSum int64
	var ttftCnt int
	var e2eOutSum int64
	var e2eMsSum int64
	var decOutSum int64
	var decMsSum int64

	byModel := map[string]*ModelAggregate{}
	var order []string
	for _, item := range items {
		s := item.step
		agg.StepsCount++
		if s.QuotaGroup == "3p" {
			agg.ThirdPoolCostUsd += s.CostUsd
		} else {
			agg.GeminiPoolCostUsd += s.CostUsd
		}
		mk := s.ModelID
		if mk == "" {
			mk = UnknownModel.ID
		}
		mm := byModel[mk]
		if mm == nil {
			lbl, fam, qg := s.ModelLabel, s.ModelFamily, s.QuotaGroup
			if lbl == "" {
				lbl, fam, qg = UnknownModel.Label, "other", "gemini"
			}
			mm = &ModelAggregate{ModelID: mk, ModelLabel: lbl, Family: fam, QuotaGroup: qg, Tiers: map[string]int{}}
			byModel[mk] = mm
			order = append(order, mk)
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
		// Steady-state cache: skip each session's cold start and in-session model_switch cold prefix,
		// and count a step as a hit when classifyCacheMissSpec returns "" (covering both <= max(8K, 10%)
		// uncached slack and large batch tool outputs whose prior prefix remained cached).
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
			agg.WinSteps++
			agg.WinCostUsd += s.CostUsd
		} else {
			agg.WslSteps++
			agg.WslCostUsd += s.CostUsd
			if s.SourceCategory == "CLAUDE_GW" {
				agg.ClaudeGwSteps++
				agg.ClaudeGwCostUsd += s.CostUsd
			} else {
				agg.WslHostSteps++
				agg.WslHostCostUsd += s.CostUsd
			}
		}
		agg.RawIn += int64(s.RawIn)
		agg.RawCr += int64(s.RawCr)
		agg.AlignedIn += int64(s.AlignedIn)
		agg.AlignedCr += int64(s.AlignedCr)
		agg.OutTok += int64(s.OutTok)
		agg.ThTok += int64(s.ThTok)
		if s.HasThinking {
			agg.ThinkingStepsCount++
		}
		agg.InputCostUsd += s.InputCostUsd
		agg.CacheCostUsd += s.CacheCostUsd
		agg.OutputCostUsd += s.OutputCostUsd
		agg.ThinkCostUsd += s.ThinkCostUsd
		agg.CostUsd += s.CostUsd
		agg.Base1xCostUsd += s.Base1xCostUsd
		agg.NoCacheCostUsd += s.NoCacheCostUsd
		agg.SavedUsd += s.SavedUsd
		if s.RawCr > 0 {
			agg.HitSteps++
		}
		tot := s.AlignedIn + s.AlignedCr
		if tot > 0 && float64(s.AlignedCr)/float64(tot) >= 0.99 {
			agg.Hits99++
		}
		if s.TtftMs > 0 && s.TtftMs < 300000 {
			ttftSum += int64(s.TtftMs)
			ttftCnt++
		}
		if s.OutTok > 0 && s.ElapsedMs > 0 && s.ElapsedMs < 600000 {
			e2eOutSum += int64(s.OutTok)
			e2eMsSum += int64(s.ElapsedMs)
		}
		if s.DecodeTps > 0 && s.StreamMs > 0 && s.StreamMs < 600000 && (s.ElapsedMs == 0 || s.ElapsedMs < 600000) && s.OutTok > 0 {
			decOutSum += int64(s.OutTok)
			decMsSum += int64(s.StreamMs)
		}
	}

	agg.TotalPromptTokens = agg.RawIn + agg.RawCr
	if agg.RawIn+agg.RawCr > 0 {
		agg.RawHitRate = roundTo(float64(agg.RawCr)/float64(agg.RawIn+agg.RawCr)*100, 2)
	}
	if agg.AlignedIn+agg.AlignedCr > 0 {
		agg.AlignedHitRate = roundTo(float64(agg.AlignedCr)/float64(agg.AlignedIn+agg.AlignedCr)*100, 2)
	}
	if ttftCnt > 0 {
		agg.AvgTtftMs = int(ttftSum / int64(ttftCnt))
	}
	if e2eMsSum > 0 && e2eOutSum > 0 {
		agg.AvgTps = roundTo(float64(e2eOutSum)/(float64(e2eMsSum)/1000.0), 1)
	}
	if decMsSum > 0 && decOutSum > 0 {
		agg.AvgDecodeTps = roundTo(float64(decOutSum)/(float64(decMsSum)/1000.0), 1)
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
	agg.Models = make([]ModelAggregate, 0, len(order))
	for _, k := range order {
		m := byModel[k]
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
func inOfficialWindow(s ParsedModelStep, b *QuotaBucket, startSec int64, idle bool, readUnix int64) bool {
	if idle {
		return readUnix > 0 && s.TsSec > readUnix
	}
	if b != nil && b.ResetTime != "" {
		if _, err := time.Parse(time.RFC3339, b.ResetTime); err == nil {
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
	if frac == nil {
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

// detectQuotaPlanTier identifies whether the official /usage snapshot belongs to a Google AI Pro
// plan or a Google AI Ultra (20x) plan so pool dollar caps ($10/$35 & $2.50/$5 vs $200/$800 & $25/$50)
// match the account's actual subscription limits.
func detectQuotaPlanTier(groups []QuotaGroup) (tier, label string) {
	hasGemini := false
	geminiHasCarbon := false
	geminiHasDesc := false
	for _, g := range groups {
		lowName := strings.ToLower(strings.TrimSpace(g.Name))
		lowDesc := strings.ToLower(g.Description)
		if strings.Contains(lowName, "argon") && lowName != "gemini models" {
			return "pro", "AI Pro"
		}
		if strings.Contains(lowDesc, "pro plan") {
			return "pro", "AI Pro"
		}
		for _, b := range g.Buckets {
			if strings.Contains(strings.ToLower(b.ID), "argon") {
				return "pro", "AI Pro"
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
	if hasGemini && geminiHasDesc && !geminiHasCarbon {
		return "pro", "AI Pro"
	}
	return "ultra", "Ultra 20x"
}

// buildQuotaView joins the official /usage snapshot with local usage steps: per-pool trust state,
// official-window aggregates (start = reset - window length), rolling aggregates and the dollar /
// token deduction. Unknown pools yield nil numbers instead of a full pool.
func buildQuotaView(quota QuotaSnapshot, allSteps []ParsedModelStep, now time.Time) (QuotaSnapshot, map[string]WindowAggregate) {
	planTier, planLabel := detectQuotaPlanTier(quota.Groups)
	quota.PlanTier = planTier
	quota.PlanLabel = planLabel

	gemPricing := DefaultGeminiPricing
	claudePricing := DefaultClaudePoolPricing
	if planTier == "pro" {
		gemPricing = ProGeminiPricing
		claudePricing = ProClaudePoolPricing
	}

	geminiGroup := findGroup(quota.Groups, regexp.MustCompile(`^gemini\s+models$`))
	if geminiGroup == nil {
		geminiGroup = findGroup(quota.Groups, regexp.MustCompile(`gemini`))
	}
	gemState := classifyPoolQuota(quota, geminiGroup, now)
	quota.GeminiPool = &gemState
	gemKnown := gemState.State == QuotaStateKnown || gemState.State == QuotaStateStale

	if argonGroup := findGroup(quota.Groups, regexp.MustCompile(`argon`)); argonGroup != nil && argonGroup != geminiGroup {
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

	thirdGroup := findGroup(quota.Groups, regexp.MustCompile(`claude|gpt`))
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

	annotatedAll := annotateSteadySteps(allSteps)
	var steps5h, stepsWeekly, gemSteps5h, gemStepsWeekly, tpSteps5h, tpStepsWk []annotatedStep
	var stepsRolling5h, stepsRolling7d, steps24h []annotatedStep
	for _, item := range annotatedAll {
		s := item.step
		if s.QuotaGroup == "3p" {
			if tpKnown && inOfficialWindow(s, tp5h, tpStart5h, tpIdle5h, readUnix) {
				tpSteps5h = append(tpSteps5h, item)
				steps5h = append(steps5h, item)
			}
			if tpKnown && inOfficialWindow(s, tpWk, tpStartWk, tpIdleWk, readUnix) {
				tpStepsWk = append(tpStepsWk, item)
				stepsWeekly = append(stepsWeekly, item)
			}
		} else {
			if gemKnown && inOfficialWindow(s, b5h, start5hSec, idle5h, readUnix) {
				gemSteps5h = append(gemSteps5h, item)
				steps5h = append(steps5h, item)
			}
			if gemKnown && inOfficialWindow(s, bWeekly, startWeeklySec, idleWeekly, readUnix) {
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

	return quota, map[string]WindowAggregate{
		"window5h":     agg5h,
		"windowWeekly": aggWeekly,
		"rolling5h":    rolling5h,
		"rolling7d":    rolling7d,
		"last24h":      aggregateAnnotatedSteps(steps24h),
		"allTime":      aggregateAnnotatedSteps(annotatedAll),
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
	out := make([]string, len(allSteps))
	if len(allSteps) == 0 {
		return out
	}
	byStep, byFirstSession := GlobalStore.QueryTurnCacheMissIndex()
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
	dashCachedAllSteps  []ParsedModelStep
	dashCachedRecent    []map[string]any
)

func buildRecentDbSteps(allSteps []ParsedModelStep, gatewayTurns []map[string]any) []map[string]any {
	stepMisses := computeDbStepCacheMisses(allSteps, gatewayTurns)
	clientBySid := GlobalStore.LookupSessionClientMap()
	for _, gt := range gatewayTurns {
		sid := strings.ToLower(strings.TrimSpace(fmt.Sprintf("%v", gt["sessionId"])))
		if sid == "" || sid == "<nil>" {
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
	startIdx := len(allSteps) - 120
	if startIdx < 0 {
		startIdx = 0
	}
	for i := len(allSteps) - 1; i >= startIdx; i-- {
		s := allSteps[i]
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

func BuildDashboardPayload(gw *GatewayServer, forceQuota bool) map[string]any {
	quota := FetchOfficialQuota(forceQuota)
	liveGwSids := make(map[string]bool)

	liveSessions, gatewayTurns := gw.CollectDashboardTelemetry(liveGwSids)

	var latestTurnStamp int64
	if len(gatewayTurns) > 0 {
		latestTurnStamp, _ = gatewayTurns[0]["completedAtMs"].(int64)
	}

	var allSteps []ParsedModelStep
	var recentDbSteps []map[string]any
	convDir := ConversationsDir()
	appConvDir := AppConversationsDir()
	combinedConvDir := convDir + "\x00" + appConvDir + "\x00" + gwcfg.Current().AppConvDirRaw
	if GlobalStore.IsReady() {
		dbPath := GlobalStore.DBPath()
		now := time.Now()
		dashStepsCacheMu.Lock()
		needDirSync := forceQuota || dbPath != dashCachedDBPath || combinedConvDir != dashCachedConvDir || now.Sub(dashLastDirSync) >= 2*time.Second
		if needDirSync {
			dashLastDirSync = now
		}
		dashStepsCacheMu.Unlock()

		var gwSids map[string]bool
		if needDirSync {
			gwSids = mergeKnownAndLiveGwSids(liveGwSids)
			if convDir != "" {
				GlobalStore.SyncUsageStepsFromDirWithRODB(convDir, gwSids, gw.LookupReadOnlySessionDB)
			}
			if appConvDir != "" {
				GlobalStore.SyncUsageStepsFromSourceDir(appConvDir, "APP", "AGY_APP", "Antigravity App", gwSids)
			}
		}
		curVer := GlobalStore.Version()
		curRegSeq := gwSidRegSeq.Load()

		dashStepsCacheMu.Lock()
		needReload := dbPath != dashCachedDBPath ||
			combinedConvDir != dashCachedConvDir ||
			curVer != dashCachedVer ||
			len(liveGwSids) != dashCachedGwCnt ||
			curRegSeq != dashCachedRegSeq ||
			latestTurnStamp != dashCachedTurnStamp ||
			dashCachedAllSteps == nil
		if !needReload {
			allSteps = dashCachedAllSteps
			recentDbSteps = dashCachedRecent
			dashStepsCacheMu.Unlock()
		} else {
			dashStepsCacheMu.Unlock()
			if gwSids == nil {
				gwSids = mergeKnownAndLiveGwSids(liveGwSids)
			}
			if indexed, ok := GlobalStore.QueryAllUsageSteps(gwSids); ok {
				allSteps = indexed
			}
			sortModelStepsChronological(allSteps)
			recentDbSteps = buildRecentDbSteps(allSteps, gatewayTurns)
			dashStepsCacheMu.Lock()
			dashCachedDBPath = dbPath
			dashCachedConvDir = combinedConvDir
			dashCachedVer = curVer
			dashCachedGwCnt = len(liveGwSids)
			dashCachedRegSeq = curRegSeq
			dashCachedTurnStamp = latestTurnStamp
			dashCachedAllSteps = allSteps
			dashCachedRecent = recentDbSteps
			dashStepsCacheMu.Unlock()
		}
	} else {
		gwSids := mergeKnownAndLiveGwSids(liveGwSids)
		allSteps = append(allSteps, readFallbackStepsFromDir(convDir, "HOST", "AGY_CLI", "AGY CLI", gwSids)...)
		if appConvDir != "" {
			allSteps = append(allSteps, readFallbackStepsFromDir(appConvDir, "APP", "AGY_APP", "Antigravity App", gwSids)...)
		}
		sortModelStepsChronological(allSteps)
		recentDbSteps = buildRecentDbSteps(allSteps, gatewayTurns)
	}

	quota, windows := buildQuotaView(quota, allSteps, time.Now())
	canaryRoutes := summarizeCanaryRoutes(liveSessions, windows["rolling5h"])
	cacheWarnings := summarizeCacheWarnings(liveSessions)
	tosTripped, tosReason := IsTosTripped()
	snap := gwcfg.Current()

	return map[string]any{
		"now": time.Now().UTC().Format(time.RFC3339),
		"gateway": map[string]any{
			"instanceId":                snap.InstanceID,
			"instanceLabel":             snap.InstanceLabel,
			"port":                      GatewayPort,
			"binary":                    PatchedAgyBin,
			"engine":                    "Go Native",
			"uiBuildId":                 DashboardBuildID(),
			"defaultModel":              DefaultModelID,
			"models":                    AgyModels,
			"contextWindowMax":          ModelOrDefault(DefaultModelID).ContextWindow,
			"activeSessionsCount":       len(liveSessions),
			"maxLiveWorkers":            MaxLiveWorkers,
			"maxConcurrentPrompts":      MaxConcurrentPrompts,
			"tosTripped":                tosTripped,
			"tosTrippedReason":          tosReason,
			"argonDegradationDetected":  argonDegradationDetectedTotal.Load(),
			"argonDegradationRecovered": argonDegradationRecoveredTotal.Load(),
			"cacheBurnWarnings":         cacheBurnWarningsTotal.Load(),
			"wafAutoBisectHealed":       WafBisectHealedTotal.Load(),
		},
		"quota":         quota,
		"windows":       windows,
		"canaryRoutes":  canaryRoutes,
		"cacheWarnings": cacheWarnings,
		"liveSessions":  liveSessions,
		"gatewayTurns":  gatewayTurns,
		"recentDbSteps": recentDbSteps,
	}
}
