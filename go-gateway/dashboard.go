package main

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
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
	if v := strings.TrimSpace(os.Getenv("AGY_QUOTA_BIN")); v != "" {
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

type QuotaDeduction struct {
	Cap5hUsd                    float64 `json:"cap5hUsd"`
	CapWeeklyUsd                float64 `json:"capWeeklyUsd"`
	PoolUsed5hUsd               float64 `json:"poolUsed5hUsd"`
	PoolRem5hUsd                float64 `json:"poolRem5hUsd"`
	LocalWsl5hUsd               float64 `json:"localWsl5hUsd"`
	LocalWin5hUsd               float64 `json:"localWin5hUsd"`
	LocalClaudeGw5hUsd          float64 `json:"localClaudeGw5hUsd"`
	LocalWslHost5hUsd           float64 `json:"localWslHost5hUsd"`
	LocalTotal5hUsd             float64 `json:"localTotal5hUsd"`
	PoolUsedWeeklyUsd           float64 `json:"poolUsedWeeklyUsd"`
	PoolRemWeeklyUsd            float64 `json:"poolRemWeeklyUsd"`
	LocalWslWeeklyUsd           float64 `json:"localWslWeeklyUsd"`
	LocalWinWeeklyUsd           float64 `json:"localWinWeeklyUsd"`
	LocalClaudeGwWeeklyUsd      float64 `json:"localClaudeGwWeeklyUsd"`
	LocalWslHostWeeklyUsd       float64 `json:"localWslHostWeeklyUsd"`
	LocalTotalWeeklyUsd         float64 `json:"localTotalWeeklyUsd"`
	Rem5hTokensAt99PctCache     int64   `json:"rem5hTokensAt99PctCache"`
	Rem5hTokensAt0PctCache      int64   `json:"rem5hTokensAt0PctCache"`
	RemWeeklyTokensAt99PctCache int64   `json:"remWeeklyTokensAt99PctCache"`
	RemWeeklyTokensAt0PctCache  int64   `json:"remWeeklyTokensAt0PctCache"`
}

type QuotaSnapshot struct {
	Status         string          `json:"status"`
	UpdatedAt      string          `json:"updatedAt"`
	Error          string          `json:"error,omitempty"`
	Groups         []QuotaGroup    `json:"groups"`
	Deduction      *QuotaDeduction `json:"deduction,omitempty"`
	ThirdPartyPool *ThirdPartyPool `json:"thirdPartyPool,omitempty"`
}

type WindowAggregate struct {
	Start              string           `json:"start,omitempty"`
	Reset              string           `json:"reset,omitempty"`
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
	Tiers         map[string]int `json:"tiers"`
	ttftSum       int64
	ttftCnt       int
	tpsSum        float64
	tpsCnt        int
}

type PoolWindow struct {
	Reset             string           `json:"reset"`
	UsedFraction      float64          `json:"usedFraction"`
	RemainingFraction float64          `json:"remainingFraction"`
	LocalCostUsd      float64          `json:"localCostUsd"`
	Steps             int              `json:"steps"`
	ImpliedCapUsd     *float64         `json:"impliedCapUsd"`
	Models            []ModelAggregate `json:"models"`
}

type ThirdPartyPool struct {
	GroupName    string     `json:"groupName"`
	Description  string     `json:"description"`
	Available    bool       `json:"available"`
	Window5h     PoolWindow `json:"window5h"`
	WindowWeekly PoolWindow `json:"windowWeekly"`
}

var (
	quotaMu         sync.Mutex
	cachedQuota     *QuotaSnapshot
	cachedQuotaAt   time.Time
	quotaFlightMu   sync.Mutex
	quotaFlightWait chan struct{}
	quotaFlightRes  QuotaSnapshot
	gwSidMu         sync.RWMutex
	knownGwSids     = make(map[string]bool)
	gwSidLoaded     bool
)

func RegisterGatewaySessionID(sid string) {
	if sid == "" {
		return
	}
	gwSidMu.Lock()
	knownGwSids[strings.ToLower(sid)] = true
	gwSidMu.Unlock()
}

func getKnownGatewaySids() map[string]bool {
	gwSidMu.Lock()
	if !gwSidLoaded {
		gwSidLoaded = true
		if raw, err := os.ReadFile(GatewayLogFile); err == nil {
			re := regexp.MustCompile(`"sessionId"\s*:\s*"([0-9a-fA-F-]{36})"`)
			matches := re.FindAllSubmatch(raw, -1)
			for _, m := range matches {
				knownGwSids[strings.ToLower(string(m[1]))] = true
			}
		}
	}
	GlobalStore.LoadKnownGatewaySids(knownGwSids)
	out := make(map[string]bool, len(knownGwSids))
	for k, v := range knownGwSids {
		out[k] = v
	}
	gwSidMu.Unlock()
	return out
}

func FetchOfficialQuota(force bool) QuotaSnapshot {
	quotaMu.Lock()
	hasCache := cachedQuota != nil
	var snap QuotaSnapshot
	if hasCache {
		snap = *cachedQuota
	}
	age := time.Since(cachedQuotaAt)
	quotaMu.Unlock()

	// Non-blocking stale-while-revalidate for dashboard polling:
	// If we already have a cached snapshot and force=false, return immediately in 0ms;
	// trigger a background refresh only if cache is >180s old and a turn occurred recently (or >15m old).
	if hasCache && !force {
		lastTurn := lastGatewayTurn.Load()
		needBg := age >= 15*time.Minute || (age >= 3*time.Minute && lastTurn > cachedQuotaAt.Unix())
		if needBg && quotaRefreshing.CompareAndSwap(false, true) {
			go func() {
				defer quotaRefreshing.Store(false)
				_ = fetchOfficialQuotaSync()
			}()
		}
		return snap
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

func fetchOfficialQuotaDo() QuotaSnapshot {
	ctx, cancel := context.WithTimeout(context.Background(), 22*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, quotaAgyBin(), "-p", "/usage", "--output-format", "json")
	env := os.Environ()
	env = append(env, "GOGC=30", "GOMEMLIMIT=120MiB")
	if proxy := strings.TrimSpace(os.Getenv("AGY_UPSTREAM_PROXY")); proxy != "" {
		env = append(env, "HTTP_PROXY="+proxy, "HTTPS_PROXY="+proxy, "NO_PROXY=127.0.0.1,localhost,::1")
	}
	cmd.Env = env
	out, err := cmd.Output()
	if err != nil || len(out) == 0 {
		quotaMu.Lock()
		defer quotaMu.Unlock()
		if cachedQuota != nil {
			return *cachedQuota
		}
		errMsg := "Empty /usage output"
		if err != nil {
			errMsg = err.Error()
		}
		return QuotaSnapshot{
			Status:    "error",
			UpdatedAt: time.Now().UTC().Format(time.RFC3339),
			Error:     errMsg,
			Groups:    []QuotaGroup{},
		}
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
		if cachedQuota != nil {
			return *cachedQuota
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
	quotaMu.Unlock()
	return snap
}

func roundTo(v float64, decimals int) float64 {
	p := math.Pow(10, float64(decimals))
	return math.Round(v*p) / p
}

func AggregateSteps(steps []ParsedModelStep) WindowAggregate {
	var agg WindowAggregate
	var ttftSum int64
	var ttftCnt int
	var tpsSum float64
	var tpsCnt int

	byModel := map[string]*ModelAggregate{}
	var order []string
	for _, s := range steps {
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
		if s.TtftMs > 0 && s.TtftMs < 300000 {
			mm.ttftSum += int64(s.TtftMs)
			mm.ttftCnt++
		}
		if s.Tps > 0 && s.Tps < 500 {
			mm.tpsSum += s.Tps
			mm.tpsCnt++
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
		if s.Tps > 0 && s.Tps < 500 {
			tpsSum += s.Tps
			tpsCnt++
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
	if tpsCnt > 0 {
		agg.AvgTps = roundTo(tpsSum/float64(tpsCnt), 1)
	}
	agg.InputCostUsd = roundTo(agg.InputCostUsd, 4)
	agg.CacheCostUsd = roundTo(agg.CacheCostUsd, 4)
	agg.OutputCostUsd = roundTo(agg.OutputCostUsd, 4)
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
		if m.tpsCnt > 0 {
			m.AvgTps = roundTo(m.tpsSum/float64(m.tpsCnt), 1)
		}
		m.CostUsd = roundTo(m.CostUsd, 4)
		m.SavedUsd = roundTo(m.SavedUsd, 4)
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

func resetUnix(b *QuotaBucket, fallback int64) int64 {
	if b != nil && b.ResetTime != "" {
		if t, err := time.Parse(time.RFC3339, b.ResetTime); err == nil {
			return t.Unix()
		}
	}
	return fallback
}

func buildPoolWindow(b *QuotaBucket, steps []ParsedModelStep) PoolWindow {
	agg := AggregateSteps(steps)
	pw := PoolWindow{RemainingFraction: 1, LocalCostUsd: roundTo(agg.CostUsd, 2), Steps: agg.StepsCount, Models: agg.Models}
	if b != nil {
		pw.Reset = b.ResetTime
		pw.UsedFraction = b.UsedFraction
		pw.RemainingFraction = b.RemainingFraction
	}
	if pw.UsedFraction > 0.005 && agg.CostUsd > 0 {
		v := roundTo(agg.CostUsd/pw.UsedFraction, 2)
		pw.ImpliedCapUsd = &v
	}
	return pw
}

func BuildDashboardPayload(gw *GatewayServer, forceQuota bool) map[string]any {
	quota := FetchOfficialQuota(forceQuota)
	gwSids := getKnownGatewaySids()

	liveSessions, gatewayTurns := gw.CollectDashboardTelemetry(gwSids)

	var allSteps []ParsedModelStep
	convDir := ConversationsDir()
	if GlobalStore.IsReady() {
		GlobalStore.SyncUsageStepsFromDir(convDir, gwSids)
		if indexed, ok := GlobalStore.QueryAllUsageSteps(gwSids); ok {
			allSteps = indexed
		}
	} else if entries, err := os.ReadDir(convDir); err == nil {
		for _, e := range entries {
			if !e.IsDir() && strings.HasSuffix(e.Name(), ".db") {
				dbPath := filepath.Join(convDir, e.Name())
				allSteps = append(allSteps, GlobalDBCache.GetSteps(dbPath, "WSL", gwSids)...)
			}
		}
	}

	sort.Slice(allSteps, func(i, j int) bool {
		if allSteps[i].TsSec != allSteps[j].TsSec {
			return allSteps[i].TsSec < allSteps[j].TsSec
		}
		return allSteps[i].StepIdx < allSteps[j].StepIdx
	})

	var geminiGroup *QuotaGroup
	for i := range quota.Groups {
		if strings.Contains(strings.ToLower(quota.Groups[i].Name), "gemini") {
			geminiGroup = &quota.Groups[i]
			break
		}
	}
	if geminiGroup == nil && len(quota.Groups) > 0 {
		geminiGroup = &quota.Groups[0]
	}

	var b5h, bWeekly *QuotaBucket
	if geminiGroup != nil {
		for i := range geminiGroup.Buckets {
			if geminiGroup.Buckets[i].Window == "5h" {
				b5h = &geminiGroup.Buckets[i]
			} else if geminiGroup.Buckets[i].Window == "weekly" {
				bWeekly = &geminiGroup.Buckets[i]
			}
		}
	}

	nowSec := time.Now().Unix()
	reset5hSec := nowSec + 5*3600
	if b5h != nil && b5h.ResetTime != "" {
		if t, err := time.Parse(time.RFC3339, b5h.ResetTime); err == nil {
			reset5hSec = t.Unix()
		}
	}
	start5hSec := reset5hSec - 5*3600

	resetWeeklySec := nowSec + 7*86400
	if bWeekly != nil && bWeekly.ResetTime != "" {
		if t, err := time.Parse(time.RFC3339, bWeekly.ResetTime); err == nil {
			resetWeeklySec = t.Unix()
		}
	}
	startWeeklySec := resetWeeklySec - 7*86400

	var steps5h, stepsWeekly, steps24h []ParsedModelStep
	for _, s := range allSteps {
		if s.TsSec >= start5hSec {
			steps5h = append(steps5h, s)
		}
		if s.TsSec >= startWeeklySec {
			stepsWeekly = append(stepsWeekly, s)
		}
		if s.TsSec >= nowSec-86400 {
			steps24h = append(steps24h, s)
		}
	}

	agg5h := AggregateSteps(steps5h)
	aggWeekly := AggregateSteps(stepsWeekly)
	notThird := func(st *ParsedModelStep) bool { return st.QuotaGroup != "3p" }
	gem5h := AggregateSteps(filterSteps(steps5h, notThird))
	gemWeekly := AggregateSteps(filterSteps(stepsWeekly, notThird))

	thirdGroup := findGroup(quota.Groups, regexp.MustCompile(`claude|gpt`))
	tp5h, tpWk := findBucket(thirdGroup, "5h"), findBucket(thirdGroup, "weekly")
	tpStart5h := resetUnix(tp5h, nowSec+5*3600) - 5*3600
	tpStartWk := resetUnix(tpWk, nowSec+7*86400) - 7*86400
	thirdSteps := filterSteps(allSteps, func(st *ParsedModelStep) bool { return st.QuotaGroup == "3p" })
	tpp := &ThirdPartyPool{GroupName: "Claude and GPT models", Available: thirdGroup != nil}
	if thirdGroup != nil {
		tpp.GroupName = thirdGroup.Name
		tpp.Description = thirdGroup.Description
	}
	tpp.Window5h = buildPoolWindow(tp5h, filterSteps(thirdSteps, func(st *ParsedModelStep) bool { return st.TsSec >= tpStart5h }))
	tpp.WindowWeekly = buildPoolWindow(tpWk, filterSteps(thirdSteps, func(st *ParsedModelStep) bool { return st.TsSec >= tpStartWk }))
	quota.ThirdPartyPool = tpp
	agg24h := AggregateSteps(steps24h)
	aggAll := AggregateSteps(allSteps)

	agg5h.Start = time.Unix(start5hSec, 0).UTC().Format(time.RFC3339)
	if b5h != nil {
		agg5h.Reset = b5h.ResetTime
	}
	aggWeekly.Start = time.Unix(startWeeklySec, 0).UTC().Format(time.RFC3339)
	if bWeekly != nil {
		aggWeekly.Reset = bWeekly.ResetTime
	}

	used5hFrac := 0.0
	rem5hFrac := 1.0
	if b5h != nil {
		used5hFrac = b5h.UsedFraction
		rem5hFrac = b5h.RemainingFraction
	}
	usedWeeklyFrac := 0.0
	remWeeklyFrac := 1.0
	if bWeekly != nil {
		usedWeeklyFrac = bWeekly.UsedFraction
		remWeeklyFrac = bWeekly.RemainingFraction
	}

	cap5h := DefaultGeminiPricing.Cap5hUsd
	capWeekly := DefaultGeminiPricing.CapWeeklyUsd
	poolUsed5h := roundTo(cap5h*used5hFrac, 2)
	poolRem5h := roundTo(cap5h*rem5hFrac, 2)
	poolUsedWeekly := roundTo(capWeekly*usedWeeklyFrac, 2)
	poolRemWeekly := roundTo(capWeekly*remWeeklyFrac, 2)

	blended99 := (0.993*DefaultGeminiPricing.BaseCachedReadPerM + 0.007*DefaultGeminiPricing.BaseInputPerM + 0.003*DefaultGeminiPricing.BaseOutputPerM) * 1.3
	blended0 := (1.0*DefaultGeminiPricing.BaseInputPerM + 0.003*DefaultGeminiPricing.BaseOutputPerM) * 1.3

	quota.Deduction = &QuotaDeduction{
		Cap5hUsd:                    cap5h,
		CapWeeklyUsd:                capWeekly,
		PoolUsed5hUsd:               poolUsed5h,
		PoolRem5hUsd:                poolRem5h,
		LocalWsl5hUsd:               roundTo(gem5h.WslCostUsd, 2),
		LocalWin5hUsd:               roundTo(gem5h.WinCostUsd, 2),
		LocalClaudeGw5hUsd:          roundTo(gem5h.ClaudeGwCostUsd, 2),
		LocalWslHost5hUsd:           roundTo(gem5h.WslHostCostUsd, 2),
		LocalTotal5hUsd:             roundTo(gem5h.CostUsd, 2),
		PoolUsedWeeklyUsd:           poolUsedWeekly,
		PoolRemWeeklyUsd:            poolRemWeekly,
		LocalWslWeeklyUsd:           roundTo(gemWeekly.WslCostUsd, 2),
		LocalWinWeeklyUsd:           roundTo(gemWeekly.WinCostUsd, 2),
		LocalClaudeGwWeeklyUsd:      roundTo(gemWeekly.ClaudeGwCostUsd, 2),
		LocalWslHostWeeklyUsd:       roundTo(gemWeekly.WslHostCostUsd, 2),
		LocalTotalWeeklyUsd:         roundTo(gemWeekly.CostUsd, 2),
		Rem5hTokensAt99PctCache:     int64(math.Round((poolRem5h / blended99) * 1e6)),
		Rem5hTokensAt0PctCache:      int64(math.Round((poolRem5h / blended0) * 1e6)),
		RemWeeklyTokensAt99PctCache: int64(math.Round((poolRemWeekly / blended99) * 1e6)),
		RemWeeklyTokensAt0PctCache:  int64(math.Round((poolRemWeekly / blended0) * 1e6)),
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
		recentDbSteps = append(recentDbSteps, map[string]any{
			"ts":             time.Unix(s.TsSec, 0).UTC().Format(time.RFC3339),
			"sourceSide":     s.SourceSide,
			"sourceCategory": s.SourceCategory,
			"sourceLabel":    s.SourceLabel,
			"sessionId":      s.SessionID,
			"stepIdx":        s.StepIdx,
			"modelId":        s.ModelID,
			"modelLabel":     s.ModelLabel,
			"modelTier":      s.ModelTier,
			"quotaGroup":     s.QuotaGroup,
			"ttftMs":         s.TtftMs,
			"elapsedMs":      s.ElapsedMs,
			"tps":            s.Tps,
			"alignedIn":      s.AlignedIn,
			"alignedCr":      s.AlignedCr,
			"outTok":         s.OutTok,
			"visOutTok":      s.VisOutTok,
			"thTok":          s.ThTok,
			"hasThinking":    s.HasThinking,
			"totalCtx":       alignedTot,
			"tierMultiplier": s.TierMultiplier,
			"alignedHitRate": hitRateStr,
			"inputCostUsd":   roundTo(s.InputCostUsd, 4),
			"cacheCostUsd":   roundTo(s.CacheCostUsd, 4),
			"outputCostUsd":  roundTo(s.OutputCostUsd, 4),
			"thinkCostUsd":   roundTo(s.ThinkCostUsd, 4),
			"costUsd":        roundTo(s.CostUsd, 4),
			"savedUsd":       roundTo(s.SavedUsd, 4),
		})
	}

	tosTripped, tosReason := IsTosTripped()

	return map[string]any{
		"now": time.Now().UTC().Format(time.RFC3339),
		"gateway": map[string]any{
			"port":                      GatewayPort,
			"binary":                    PatchedAgyBin,
			"engine":                    "Go Native",
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
		},
		"quota": quota,
		"windows": map[string]any{
			"window5h":     agg5h,
			"windowWeekly": aggWeekly,
			"last24h":      agg24h,
			"allTime":      aggAll,
		},
		"liveSessions":  liveSessions,
		"gatewayTurns":  gatewayTurns,
		"recentDbSteps": recentDbSteps,
	}
}
