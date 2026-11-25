package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	gwcfg "agy-acp-gateway-go/internal/config"
)

const (
	peerFetchFreshTTL       = 2 * time.Second
	peerFetchStaleTTL       = 60 * time.Second
	peerFetchTimeout        = 1800 * time.Millisecond
	peerFetchRefreshTimeout = 18 * time.Second
	maxMergedGatewayTurns   = 80
	maxMergedRecentDbSteps  = 120
)

type peerDashboardDecoded struct {
	Now           string                     `json:"now"`
	Gateway       map[string]any             `json:"gateway"`
	Quota         QuotaSnapshot              `json:"quota"`
	Windows       map[string]WindowAggregate `json:"windows"`
	CanaryRoutes  []map[string]any           `json:"canaryRoutes"`
	CacheWarnings []map[string]any           `json:"cacheWarnings"`
	LiveSessions  []map[string]any           `json:"liveSessions"`
	GatewayTurns  []map[string]any           `json:"gatewayTurns"`
	RecentDbSteps []map[string]any           `json:"recentDbSteps"`
}

type cachedPeerDashboard struct {
	fetchedAt time.Time
	decoded   *peerDashboardDecoded
}

type peerFetchFlight struct {
	done      chan struct{}
	decoded   *peerDashboardDecoded
	reachable bool
	stale     bool
	fetchedAt string
	errMsg    string
}

var (
	peerDashCacheMu     sync.Mutex
	peerDashCacheGen    uint64
	peerDashCache       = make(map[string]cachedPeerDashboard)
	peerDashLastAttempt = make(map[string]time.Time)
	peerDashInflight    = make(map[string]*peerFetchFlight)
	peerHTTPClient      = &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Transport: &http.Transport{
			Proxy:               nil, // Internal peer calls must never inherit external HTTP_PROXY.
			MaxIdleConns:        16,
			MaxIdleConnsPerHost: 4,
			IdleConnTimeout:     30 * time.Second,
		},
	}
)

func InvalidatePeerDashboardCache() {
	peerDashCacheMu.Lock()
	peerDashCacheGen++
	peerDashCache = make(map[string]cachedPeerDashboard)
	peerDashLastAttempt = make(map[string]time.Time)
	peerDashCacheMu.Unlock()
}

func resetPeerDashboardCacheForTest() {
	peerDashCacheMu.Lock()
	peerDashCacheGen++
	peerDashCache = make(map[string]cachedPeerDashboard)
	peerDashLastAttempt = make(map[string]time.Time)
	peerDashInflight = make(map[string]*peerFetchFlight)
	peerDashCacheMu.Unlock()
}
func defaultPeerAPIKey(snap *gwcfg.Snapshot) string {
	if len(apiKeys) > 0 && len(apiKeys[0]) > 0 {
		return string(apiKeys[0])
	}
	if snap != nil && snap.APIKeysRaw != "" {
		for _, k := range strings.Split(snap.APIKeysRaw, ",") {
			if k = strings.TrimSpace(k); k != "" {
				return k
			}
		}
	}
	return ""
}

func cloneMapListWithInstance(items []map[string]any, instID, instLabel string) []map[string]any {
	if len(items) == 0 {
		return []map[string]any{}
	}
	out := make([]map[string]any, 0, len(items))
	for _, m := range items {
		if m == nil {
			continue
		}
		cp := make(map[string]any, len(m)+2)
		for k, v := range m {
			cp[k] = v
		}
		cp["instanceId"] = instID
		cp["instanceLabel"] = instLabel
		out = append(out, cp)
	}
	return out
}

func cloneGatewayMapWithInstance(gw map[string]any, instID, instLabel string) map[string]any {
	cp := make(map[string]any, len(gw)+2)
	for k, v := range gw {
		cp[k] = v
	}
	cp["instanceId"] = instID
	cp["instanceLabel"] = instLabel
	return cp
}

func fetchPeerDashboard(peer gwcfg.DashboardPeerConfig, forceQuota bool, fallbackKey string) (decoded *peerDashboardDecoded, reachable bool, stale bool, fetchedAt string, errMsg string) {
	cacheKey := peer.ID + "|" + peer.BaseURL
	flightKey := cacheKey
	if forceQuota {
		flightKey += "|refresh"
	}
	now := time.Now()

	peerDashCacheMu.Lock()
	if !forceQuota {
		if cached, ok := peerDashCache[cacheKey]; ok && cached.decoded != nil && now.Sub(cached.fetchedAt) < peerFetchFreshTTL {
			peerDashCacheMu.Unlock()
			return cached.decoded, true, false, cached.fetchedAt.UTC().Format(time.RFC3339), ""
		}
	}
	if inflight, exists := peerDashInflight[flightKey]; exists {
		peerDashCacheMu.Unlock()
		<-inflight.done
		return inflight.decoded, inflight.reachable, inflight.stale, inflight.fetchedAt, inflight.errMsg
	}
	startGen := peerDashCacheGen
	flight := &peerFetchFlight{done: make(chan struct{})}
	peerDashInflight[flightKey] = flight
	peerDashCacheMu.Unlock()

	defer func() {
		flight.decoded = decoded
		flight.reachable = reachable
		flight.stale = stale
		flight.fetchedAt = fetchedAt
		flight.errMsg = errMsg
		peerDashCacheMu.Lock()
		if peerDashInflight[flightKey] == flight {
			delete(peerDashInflight, flightKey)
		}
		peerDashCacheMu.Unlock()
		close(flight.done)
	}()
	timeout := peerFetchTimeout
	if forceQuota {
		timeout = peerFetchRefreshTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	url := strings.TrimRight(peer.BaseURL, "/") + "/api/dashboard?local_only=1"
	if forceQuota {
		url += "&refresh=1"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err == nil {
		req.Header.Set("X-Agy-Federated-Hop", "1")
		key := peer.APIKey
		if key == "" {
			key = fallbackKey
		}
		if key != "" {
			req.Header.Set("x-api-key", key)
		}
		resp, doErr := peerHTTPClient.Do(req)
		if doErr != nil {
			err = doErr
		} else {
			defer resp.Body.Close()
			body, readErr := io.ReadAll(io.LimitReader(resp.Body, 8*1024*1024))
			if readErr != nil {
				err = readErr
			} else if resp.StatusCode != http.StatusOK {
				err = fmt.Errorf("HTTP %d", resp.StatusCode)
			} else {
				var d peerDashboardDecoded
				if uErr := json.Unmarshal(body, &d); uErr != nil {
					err = uErr
				} else {
					stamp := time.Now()
					peerDashCacheMu.Lock()
					if peerDashCacheGen == startGen {
						peerDashCache[cacheKey] = cachedPeerDashboard{fetchedAt: stamp, decoded: &d}
					}
					peerDashCacheMu.Unlock()
					return &d, true, false, stamp.UTC().Format(time.RFC3339), ""
				}
			}
		}
	}

	errStr := "peer unreachable"
	if err != nil {
		errStr = err.Error()
	}

	peerDashCacheMu.Lock()
	cached, ok := peerDashCache[cacheKey]
	peerDashCacheMu.Unlock()
	if ok && cached.decoded != nil && now.Sub(cached.fetchedAt) < peerFetchStaleTTL {
		return cached.decoded, false, true, cached.fetchedAt.UTC().Format(time.RFC3339), errStr
	}
	return nil, false, false, "", errStr
}

func mergeWindowAggregates(wins []WindowAggregate) WindowAggregate {
	if len(wins) == 0 {
		return WindowAggregate{Models: []ModelAggregate{}}
	}
	if len(wins) == 1 {
		return wins[0]
	}

	var out WindowAggregate
	var ttftWeightedSum int64
	var ttftSteps int64
	var tpsWeightedSum float64
	var decTpsWeightedSum float64
	var tpsOutTok int64
	var decTpsOutTok int64

	byModel := make(map[string]*ModelAggregate)
	var modelOrder []string
	modelTtftSum := make(map[string]int64)
	modelTtftSteps := make(map[string]int64)
	modelTpsSum := make(map[string]float64)
	modelTpsOut := make(map[string]int64)
	modelDecSum := make(map[string]float64)
	modelDecOut := make(map[string]int64)

	out.Idle = true
	for _, w := range wins {
		if out.Start == "" || (w.Start != "" && w.Start < out.Start) {
			out.Start = w.Start
		}
		if out.Reset == "" || (w.Reset != "" && w.Reset < out.Reset) {
			out.Reset = w.Reset
		}
		if !w.Idle {
			out.Idle = false
		}
		out.StepsCount += w.StepsCount
		out.WslSteps += w.WslSteps
		out.WinSteps += w.WinSteps
		out.ClaudeGwSteps += w.ClaudeGwSteps
		out.WslHostSteps += w.WslHostSteps
		out.HitSteps += w.HitSteps
		out.Hits99 += w.Hits99
		out.ThinkingStepsCount += w.ThinkingStepsCount
		out.RawIn += w.RawIn
		out.RawCr += w.RawCr
		out.AlignedIn += w.AlignedIn
		out.AlignedCr += w.AlignedCr
		out.TotalPromptTokens += w.TotalPromptTokens
		out.OutTok += w.OutTok
		out.ThTok += w.ThTok
		out.InputCostUsd += w.InputCostUsd
		out.CacheCostUsd += w.CacheCostUsd
		out.OutputCostUsd += w.OutputCostUsd
		out.ThinkCostUsd += w.ThinkCostUsd
		out.CostUsd += w.CostUsd
		out.WslCostUsd += w.WslCostUsd
		out.WinCostUsd += w.WinCostUsd
		out.ClaudeGwCostUsd += w.ClaudeGwCostUsd
		out.WslHostCostUsd += w.WslHostCostUsd
		out.Base1xCostUsd += w.Base1xCostUsd
		out.NoCacheCostUsd += w.NoCacheCostUsd
		out.SavedUsd += w.SavedUsd
		out.GeminiPoolCostUsd += w.GeminiPoolCostUsd
		out.ThirdPoolCostUsd += w.ThirdPoolCostUsd

		if w.AvgTtftMs > 0 && w.StepsCount > 0 {
			ttftWeightedSum += int64(w.AvgTtftMs) * int64(w.StepsCount)
			ttftSteps += int64(w.StepsCount)
		}
		weightTok := w.OutTok
		if weightTok <= 0 {
			weightTok = int64(w.StepsCount)
		}
		if w.AvgTps > 0 && weightTok > 0 {
			tpsWeightedSum += w.AvgTps * float64(weightTok)
			tpsOutTok += weightTok
		}
		if w.AvgDecodeTps > 0 && weightTok > 0 {
			decTpsWeightedSum += w.AvgDecodeTps * float64(weightTok)
			decTpsOutTok += weightTok
		}

		for _, m := range w.Models {
			if m.ModelID == "" {
				continue
			}
			aggM := byModel[m.ModelID]
			if aggM == nil {
				cp := m
				cp.Tiers = make(map[string]int)
				cp.ServedVariantCounts = make(map[string]int)
				cp.ServedVariants = nil
				cp.Steps = 0
				cp.ClaudeGwSteps = 0
				cp.RawIn = 0
				cp.RawCr = 0
				cp.OutTok = 0
				cp.ThTok = 0
				cp.CostUsd = 0
				cp.SavedUsd = 0
				cp.SteadySteps = 0
				cp.SteadyHits = 0
				cp.MissResendUsd = 0
				cp.ServedMismatch = 0
				byModel[m.ModelID] = &cp
				modelOrder = append(modelOrder, m.ModelID)
				aggM = &cp
			}
			aggM.Steps += m.Steps
			aggM.ClaudeGwSteps += m.ClaudeGwSteps
			aggM.RawIn += m.RawIn
			aggM.RawCr += m.RawCr
			aggM.OutTok += m.OutTok
			aggM.ThTok += m.ThTok
			aggM.CostUsd += m.CostUsd
			aggM.SavedUsd += m.SavedUsd
			aggM.SteadySteps += m.SteadySteps
			aggM.SteadyHits += m.SteadyHits
			aggM.MissResendUsd += m.MissResendUsd
			aggM.ServedMismatch += m.ServedMismatch
			for tk, tv := range m.Tiers {
				aggM.Tiers[tk] += tv
			}
			if len(m.ServedVariantCounts) > 0 {
				for vk, vv := range m.ServedVariantCounts {
					aggM.ServedVariantCounts[vk] += vv
				}
			} else if len(m.ServedVariants) == 1 {
				aggM.ServedVariantCounts[m.ServedVariants[0]] += max(1, m.ServedMismatch)
			} else {
				for _, vk := range m.ServedVariants {
					aggM.ServedVariantCounts[vk] += 1
				}
			}
			if m.AvgTtftMs > 0 && m.Steps > 0 {
				modelTtftSum[m.ModelID] += int64(m.AvgTtftMs) * int64(m.Steps)
				modelTtftSteps[m.ModelID] += int64(m.Steps)
			}
			mWeight := m.OutTok
			if mWeight <= 0 {
				mWeight = int64(m.Steps)
			}
			if m.AvgTps > 0 && mWeight > 0 {
				modelTpsSum[m.ModelID] += m.AvgTps * float64(mWeight)
				modelTpsOut[m.ModelID] += mWeight
			}
			if m.AvgDecodeTps > 0 && mWeight > 0 {
				modelDecSum[m.ModelID] += m.AvgDecodeTps * float64(mWeight)
				modelDecOut[m.ModelID] += mWeight
			}
		}
	}

	if out.RawIn+out.RawCr > 0 {
		out.RawHitRate = roundTo(float64(out.RawCr)/float64(out.RawIn+out.RawCr)*100, 2)
	}
	if out.AlignedIn+out.AlignedCr > 0 {
		out.AlignedHitRate = roundTo(float64(out.AlignedCr)/float64(out.AlignedIn+out.AlignedCr)*100, 2)
	}
	if ttftSteps > 0 {
		out.AvgTtftMs = int(ttftWeightedSum / ttftSteps)
	}
	if tpsOutTok > 0 {
		out.AvgTps = roundTo(tpsWeightedSum/float64(tpsOutTok), 1)
	}
	if decTpsOutTok > 0 {
		out.AvgDecodeTps = roundTo(decTpsWeightedSum/float64(decTpsOutTok), 1)
	}

	out.InputCostUsd = roundTo(out.InputCostUsd, 4)
	out.CacheCostUsd = roundTo(out.CacheCostUsd, 4)
	out.OutputCostUsd = roundTo(out.OutputCostUsd, 4)
	out.ThinkCostUsd = roundTo(out.ThinkCostUsd, 4)
	out.CostUsd = roundTo(out.CostUsd, 4)
	out.WslCostUsd = roundTo(out.WslCostUsd, 4)
	out.WinCostUsd = roundTo(out.WinCostUsd, 4)
	out.ClaudeGwCostUsd = roundTo(out.ClaudeGwCostUsd, 4)
	out.WslHostCostUsd = roundTo(out.WslHostCostUsd, 4)
	out.Base1xCostUsd = roundTo(out.Base1xCostUsd, 4)
	out.NoCacheCostUsd = roundTo(out.NoCacheCostUsd, 4)
	out.SavedUsd = roundTo(out.SavedUsd, 4)
	out.GeminiPoolCostUsd = roundTo(out.GeminiPoolCostUsd, 4)
	out.ThirdPoolCostUsd = roundTo(out.ThirdPoolCostUsd, 4)

	out.Models = make([]ModelAggregate, 0, len(modelOrder))
	for _, mid := range modelOrder {
		m := byModel[mid]
		if m.RawIn+m.RawCr > 0 {
			m.HitRate = roundTo(float64(m.RawCr)/float64(m.RawIn+m.RawCr)*100, 2)
		}
		if m.SteadySteps > 0 {
			m.SteadyHitRate = roundTo(float64(m.SteadyHits)/float64(m.SteadySteps)*100, 1)
		}
		if modelTtftSteps[mid] > 0 {
			m.AvgTtftMs = int(modelTtftSum[mid] / modelTtftSteps[mid])
		}
		if modelTpsOut[mid] > 0 {
			m.AvgTps = roundTo(modelTpsSum[mid]/float64(modelTpsOut[mid]), 1)
		}
		if modelDecOut[mid] > 0 {
			m.AvgDecodeTps = roundTo(modelDecSum[mid]/float64(modelDecOut[mid]), 1)
		}
		m.CostUsd = roundTo(m.CostUsd, 4)
		m.SavedUsd = roundTo(m.SavedUsd, 4)
		m.MissResendUsd = roundTo(m.MissResendUsd, 4)
		if len(m.ServedVariantCounts) > 0 {
			vars := make([]string, 0, len(m.ServedVariantCounts))
			for v := range m.ServedVariantCounts {
				vars = append(vars, v)
			}
			sort.Strings(vars)
			m.ServedVariants = vars
		}
		out.Models = append(out.Models, *m)
	}
	sort.SliceStable(out.Models, func(i, j int) bool {
		if out.Models[i].CostUsd != out.Models[j].CostUsd {
			return out.Models[i].CostUsd > out.Models[j].CostUsd
		}
		return out.Models[i].Steps > out.Models[j].Steps
	})
	return out
}

func mapInt64(m map[string]any, key string) int64 {
	if m == nil {
		return 0
	}
	switch v := m[key].(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	case json.Number:
		n, _ := v.Int64()
		return n
	}
	return 0
}

// BuildFederatedDashboardPayload returns the primary dashboard payload enriched with all configured
// AGY_DASHBOARD_PEERS instances. Each peer retains its own quota snapshot in instances[*], while
// windows, liveSessions, gatewayTurns, recentDbSteps, cacheWarnings, and canaryRoutes are merged
// across all reachable instances with instanceId / instanceLabel attribution.
func BuildFederatedDashboardPayload(gw *GatewayServer, forceQuota bool) map[string]any {
	snap := gwcfg.Current()
	primDisabled := primaryAccountDisabled()

	// Run self BuildDashboardPayload concurrently with peer warmup & fetches so a slow/hanging Primary
	// never blocks child workers from refreshing.
	var self map[string]any
	var selfWg sync.WaitGroup
	selfWg.Add(1)
	go func() {
		defer selfWg.Done()
		self = BuildDashboardPayload(gw, forceQuota)
	}()

	// On manual refresh (forceQuota), await startup of cold enabled authenticated non-primary workers
	// with a bounded 5s budget so "同步额度" refreshes cold workers too.
	if forceQuota && !isChildAccountWorker() && !testing.Testing() && GlobalAccountManager != nil && GlobalAccountManager.HasConfiguredAccounts() {
		accs := GlobalAccountManager.ListAccounts()
		var coldIDs []string
		for _, acc := range accs {
			if acc.Primary || !acc.Enabled || !GlobalAccountManager.HasOAuthToken(acc.ID) {
				continue
			}
			if running, _, dashPort := GlobalAccountManager.IsWorkerRunning(acc.ID); !running || dashPort <= 0 {
				coldIDs = append(coldIDs, acc.ID)
			}
		}
		if len(coldIDs) > 0 {
			warmCtx, warmCancel := context.WithTimeout(context.Background(), 5*time.Second)
			var warmWg sync.WaitGroup
			for _, id := range coldIDs {
				warmWg.Add(1)
				go func(accID string) {
					defer warmWg.Done()
					_ = GlobalAccountManager.EnsureAccountWorkerRunningWithContext(warmCtx, accID)
				}(id)
			}
			warmWg.Wait()
			warmCancel()
		}
	}

	managedPeers, unauthInstances := GlobalAccountManager.ManagedPeersForDashboard()
	seenPeer := make(map[string]bool)
	var peers []gwcfg.DashboardPeerConfig
	for _, p := range managedPeers {
		if p.ID != "" && !seenPeer[p.ID] {
			seenPeer[p.ID] = true
			peers = append(peers, p)
		}
	}
	for _, p := range snap.DashboardPeers {
		if p.ID != "" && !seenPeer[p.ID] {
			seenPeer[p.ID] = true
			peers = append(peers, p)
		}
	}

	type peerResult struct {
		cfg       gwcfg.DashboardPeerConfig
		decoded   *peerDashboardDecoded
		reachable bool
		stale     bool
		fetchedAt string
		errMsg    string
	}
	results := make([]peerResult, len(peers))
	if len(peers) > 0 {
		fallbackKey := defaultPeerAPIKey(snap)
		var peerWg sync.WaitGroup
		for i, p := range peers {
			peerWg.Add(1)
			go func(idx int, peer gwcfg.DashboardPeerConfig) {
				defer peerWg.Done()
				dec, reach, st, fat, em := fetchPeerDashboard(peer, forceQuota, fallbackKey)
				results[idx] = peerResult{
					cfg:       peer,
					decoded:   dec,
					reachable: reach,
					stale:     st,
					fetchedAt: fat,
					errMsg:    em,
				}
			}(i, p)
		}
		peerWg.Wait()
	}

	selfWg.Wait()

	selfID := snap.InstanceID
	selfLabel := snap.InstanceLabel
	if !isChildAccountWorker() {
		if prim, ok := GlobalAccountManager.GetPrimaryAccount(); ok && prim.ID != "" {
			if selfID == "" || selfID == "primary" || selfID == prim.ID {
				selfID = prim.ID
				if strings.TrimSpace(prim.Label) != "" {
					selfLabel = prim.Label
				}
			}
		}
	}

	selfGw, _ := self["gateway"].(map[string]any)
	selfGw = cloneGatewayMapWithInstance(selfGw, selfID, selfLabel)
	self["gateway"] = selfGw

	selfLive, _ := self["liveSessions"].([]map[string]any)
	selfLive = cloneMapListWithInstance(selfLive, selfID, selfLabel)
	self["liveSessions"] = selfLive

	selfTurns, _ := self["gatewayTurns"].([]map[string]any)
	selfTurns = cloneMapListWithInstance(selfTurns, selfID, selfLabel)
	self["gatewayTurns"] = selfTurns

	selfSteps, _ := self["recentDbSteps"].([]map[string]any)
	selfSteps = cloneMapListWithInstance(selfSteps, selfID, selfLabel)
	self["recentDbSteps"] = selfSteps

	selfCanary, _ := self["canaryRoutes"].([]map[string]any)
	selfCanary = cloneMapListWithInstance(selfCanary, selfID, selfLabel)
	self["canaryRoutes"] = selfCanary

	selfWarn, _ := self["cacheWarnings"].([]map[string]any)
	selfWarn = cloneMapListWithInstance(selfWarn, selfID, selfLabel)
	self["cacheWarnings"] = selfWarn

	selfWins, _ := self["windows"].(map[string]WindowAggregate)
	nowIso, _ := self["now"].(string)

	selfInst := map[string]any{
		"id":            selfID,
		"label":         selfLabel,
		"self":          true,
		"reachable":     true,
		"stale":         false,
		"error":         "",
		"fetchedAt":     nowIso,
		"gateway":       selfGw,
		"quota":         self["quota"],
		"windows":       selfWins,
		"canaryRoutes":  selfCanary,
		"cacheWarnings": selfWarn,
	}
	if primDisabled {
		selfInst["disabled"] = true
		selfInst["error"] = "账号已停用"
	}

	enrichInstancesWithScheduling := func(instList []map[string]any) map[string]any {
		if isChildAccountWorker() || !GlobalAccountManager.HasConfiguredAccounts() {
			return nil
		}
		now := time.Now()
		accs := GlobalAccountManager.ListAccounts()
		accByID := make(map[string]AccountProfile, len(accs))
		for _, a := range accs {
			accByID[a.ID] = a
		}
		rCfg := GlobalAccountManager.GetRoutingConfig()
		ttl := GlobalAccountManager.stickyTTL()
		GlobalAccountManager.stickyMu.Lock()
		totalSticky, stickyByAcc := GlobalAccountManager.stickyLRU.snapshotCounts(now, ttl)
		GlobalAccountManager.stickyMu.Unlock()
		healthByID := GlobalAccountManager.SnapshotHealthViews(accs, stickyByAcc, now)
		var totalInFlight int64
		healthyCnt := 0
		enabledCnt := 0
		for _, a := range accs {
			hv := healthByID[a.ID]
			totalInFlight += hv.InFlight
			if a.Enabled {
				enabledCnt++
				if hv.State == "healthy" {
					healthyCnt++
				}
			}
		}
		for _, inst := range instList {
			idStr, _ := inst["id"].(string)
			if a, ok := accByID[idStr]; ok {
				hv := healthByID[a.ID]
				inst["schedState"] = a.EffectiveSchedState()
				inst["isActiveTarget"] = rCfg.ActiveAccountID != "" && rCfg.ActiveAccountID == a.ID
				inst["priority"] = a.EffectivePriority()
				inst["weight"] = a.EffectiveWeight()
				inst["groups"] = a.Groups
				inst["notes"] = a.Notes
				inst["allowedModels"] = a.AllowedModels
				inst["allowedModelsCount"] = len(a.AllowedModels)
				inst["boundKeyCount"] = len(a.BoundKeys)
				inst["boundKeys"] = maskBoundKeysForDashboard(a.BoundKeys)
				inst["health"] = hv
			}
		}
		return map[string]any{
			"routing": rCfg,
			"groups":  sanitizeGroupsForDashboard(GlobalAccountManager.ListGroups()),
			"poolStats": map[string]any{
				"totalAccounts":    len(accs),
				"enabledAccounts":  enabledCnt,
				"healthyAccounts":  healthyCnt,
				"totalInFlight":    totalInFlight,
				"stickySessions":   totalSticky,
				"stickyMaxEntries": rCfg.StickyMaxEntries,
			},
		}
	}

	if len(peers) == 0 && len(unauthInstances) == 0 {
		instList := []map[string]any{selfInst}
		if pr := enrichInstancesWithScheduling(instList); pr != nil {
			self["poolRouting"] = pr
		}
		self["instances"] = instList
		return self
	}

	instances := make([]map[string]any, 0, 1+len(peers))
	instances = append(instances, selfInst)

	winKeys := []string{"window5h", "windowWeekly", "rolling5h", "rolling7d", "last24h", "allTime"}
	winsByKey := make(map[string][]WindowAggregate, len(winKeys))
	for _, k := range winKeys {
		if w, ok := selfWins[k]; ok {
			winsByKey[k] = append(winsByKey[k], w)
		}
	}

	mergedLive := append([]map[string]any{}, selfLive...)
	mergedTurns := append([]map[string]any{}, selfTurns...)
	mergedSteps := append([]map[string]any{}, selfSteps...)
	mergedCanary := append([]map[string]any{}, selfCanary...)
	mergedWarn := append([]map[string]any{}, selfWarn...)

	totalActive := len(selfLive)
	totalMaxWorkers := 0
	totalMaxPrompts := 0
	anyTosTripped := false
	tosReason := ""
	if !primDisabled {
		totalMaxWorkers = int(mapInt64(selfGw, "maxLiveWorkers"))
		totalMaxPrompts = int(mapInt64(selfGw, "maxConcurrentPrompts"))
		anyTosTripped, _ = selfGw["tosTripped"].(bool)
		tosReason, _ = selfGw["tosTrippedReason"].(string)
	}

	for _, res := range results {
		pID := res.cfg.ID
		pLabel := res.cfg.Label
		if res.decoded == nil {
			instances = append(instances, map[string]any{
				"id":        pID,
				"label":     pLabel,
				"self":      false,
				"reachable": false,
				"stale":     false,
				"error":     res.errMsg,
				"fetchedAt": "",
				"gateway": map[string]any{
					"instanceId":          pID,
					"instanceLabel":       pLabel,
					"activeSessionsCount": 0,
					"maxLiveWorkers":      0,
				},
				"quota": QuotaSnapshot{
					Status: "error",
					Error:  res.errMsg,
				},
				"windows":       map[string]WindowAggregate{},
				"canaryRoutes":  []map[string]any{},
				"cacheWarnings": []map[string]any{},
			})
			continue
		}

		pGw := cloneGatewayMapWithInstance(res.decoded.Gateway, pID, pLabel)
		pLive := cloneMapListWithInstance(res.decoded.LiveSessions, pID, pLabel)
		pTurns := cloneMapListWithInstance(res.decoded.GatewayTurns, pID, pLabel)
		pSteps := cloneMapListWithInstance(res.decoded.RecentDbSteps, pID, pLabel)
		pCanary := cloneMapListWithInstance(res.decoded.CanaryRoutes, pID, pLabel)
		pWarn := cloneMapListWithInstance(res.decoded.CacheWarnings, pID, pLabel)

		if !isChildAccountWorker() && GlobalAccountManager != nil && GlobalAccountManager.HasConfiguredAccounts() {
			if res.decoded.Quota.Error != "" {
				GlobalAccountManager.RecordQuotaSyncOutcome(pID, res.decoded.Quota.Error)
			}
			if tripped, _ := pGw["tosTripped"].(bool); tripped {
				r, _ := pGw["tosTrippedReason"].(string)
				GlobalAccountManager.RecordQuotaSyncOutcome(pID, "tos_circuit_breaker: "+r)
			}
			if len(res.decoded.Quota.Groups) > 0 {
				if tier := normalizePlanTierHint(res.decoded.Quota.PlanTier); tier != "" {
					GlobalAccountManager.SetAccountPlanTierHint(pID, tier)
				}
			}
		}

		instances = append(instances, map[string]any{
			"id":            pID,
			"label":         pLabel,
			"self":          false,
			"reachable":     res.reachable,
			"stale":         res.stale,
			"error":         res.errMsg,
			"fetchedAt":     res.fetchedAt,
			"gateway":       pGw,
			"quota":         res.decoded.Quota,
			"windows":       res.decoded.Windows,
			"canaryRoutes":  pCanary,
			"cacheWarnings": pWarn,
		})

		for _, k := range winKeys {
			if w, ok := res.decoded.Windows[k]; ok {
				winsByKey[k] = append(winsByKey[k], w)
			}
		}
		mergedLive = append(mergedLive, pLive...)
		mergedTurns = append(mergedTurns, pTurns...)
		mergedSteps = append(mergedSteps, pSteps...)
		mergedCanary = append(mergedCanary, pCanary...)
		mergedWarn = append(mergedWarn, pWarn...)

		totalActive += len(pLive)
		totalMaxWorkers += int(mapInt64(pGw, "maxLiveWorkers"))
		totalMaxPrompts += int(mapInt64(pGw, "maxConcurrentPrompts"))
		if tripped, _ := pGw["tosTripped"].(bool); tripped && !anyTosTripped {
			anyTosTripped = true
			if r, _ := pGw["tosTrippedReason"].(string); r != "" {
				tosReason = pLabel + ": " + r
			}
		}
	}
	for _, unauth := range unauthInstances {
		if uid, _ := unauth["id"].(string); uid != "" && !seenPeer[uid] {
			instances = append(instances, unauth)
		}
	}

	mergedWins := make(map[string]WindowAggregate, len(winKeys))
	for _, k := range winKeys {
		mergedWins[k] = mergeWindowAggregates(winsByKey[k])
	}

	sort.SliceStable(mergedTurns, func(i, j int) bool {
		ti := mapInt64(mergedTurns[i], "completedAtMs")
		tj := mapInt64(mergedTurns[j], "completedAtMs")
		if ti != tj {
			return ti > tj
		}
		si, _ := mergedTurns[i]["ts"].(string)
		sj, _ := mergedTurns[j]["ts"].(string)
		return si > sj
	})
	if len(mergedTurns) > maxMergedGatewayTurns {
		mergedTurns = mergedTurns[:maxMergedGatewayTurns]
	}

	sort.SliceStable(mergedSteps, func(i, j int) bool {
		si, _ := mergedSteps[i]["ts"].(string)
		sj, _ := mergedSteps[j]["ts"].(string)
		if si != sj {
			return si > sj
		}
		return mapInt64(mergedSteps[i], "stepIdx") > mapInt64(mergedSteps[j], "stepIdx")
	})
	if len(mergedSteps) > maxMergedRecentDbSteps {
		mergedSteps = mergedSteps[:maxMergedRecentDbSteps]
	}

	aggGw := cloneGatewayMapWithInstance(selfGw, selfID, selfLabel)
	aggGw["activeSessionsCount"] = totalActive
	aggGw["maxLiveWorkers"] = totalMaxWorkers
	aggGw["maxConcurrentPrompts"] = totalMaxPrompts
	aggGw["tosTripped"] = anyTosTripped
	aggGw["tosTrippedReason"] = tosReason

	out := make(map[string]any, len(self)+1)
	for k, v := range self {
		out[k] = v
	}
	out["gateway"] = aggGw
	out["windows"] = mergedWins
	out["liveSessions"] = mergedLive
	out["gatewayTurns"] = mergedTurns
	out["recentDbSteps"] = mergedSteps
	out["canaryRoutes"] = mergedCanary
	out["cacheWarnings"] = mergedWarn
	if pr := enrichInstancesWithScheduling(instances); pr != nil {
		out["poolRouting"] = pr
	}
	out["instances"] = instances
	return out
}
