package main

import (
	"bytes"
	"container/list"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gwcfg "agy-acp-gateway-go/internal/config"
)

const (
	RouteStrategyRoundRobin   = "round-robin"
	RouteStrategyFillFirst    = "fill-first"
	RouteStrategyWeightedRR   = "weighted-rr"
	RouteStrategyLeastLoad    = "least-load"
	RouteStrategyManualActive = "manual-active"

	SchedStateActive   = "active"
	SchedStateDraining = "draining"
	SchedStateDisabled = "disabled"

	defaultAccountPriority  = 100
	defaultAccountWeight    = 1
	defaultStickyMaxEntries = 4096
	defaultMaxFailoverHops  = 2
)

// AccountPoolConfig stores global multi-account scheduling, failover, and anti-ban sticky session settings.
type AccountPoolConfig struct {
	Strategy             string `json:"strategy"`
	ActiveAccountID      string `json:"activeAccountId,omitempty"`
	StickyTTLSec         int    `json:"stickyTtlSec,omitempty"`
	StickyMaxEntries     int    `json:"stickyMaxEntries,omitempty"`
	StrictU8NoMidTurnHop *bool  `json:"strictU8NoMidTurnHop,omitempty"`
	ManualAutoFailover   *bool  `json:"manualAutoFailover,omitempty"`
	PreStreamFailover    *bool  `json:"preStreamFailover,omitempty"`
	MaxFailoverHops      int    `json:"maxFailoverHops,omitempty"`
}

func (c AccountPoolConfig) Normalized() AccountPoolConfig {
	out := c
	out.Strategy = normalizeRouteStrategy(out.Strategy)
	out.ActiveAccountID = strings.ToLower(strings.TrimSpace(out.ActiveAccountID))
	if out.ActiveAccountID != "" && !validAccountIDRe.MatchString(out.ActiveAccountID) {
		out.ActiveAccountID = ""
	}
	if out.StickyTTLSec != 0 {
		if out.StickyTTLSec < 60 {
			out.StickyTTLSec = 60
		} else if out.StickyTTLSec > 86400 {
			out.StickyTTLSec = 86400
		}
	}
	if out.StickyMaxEntries <= 0 {
		out.StickyMaxEntries = defaultStickyMaxEntries
	} else if out.StickyMaxEntries < 64 {
		out.StickyMaxEntries = 64
	} else if out.StickyMaxEntries > 32768 {
		out.StickyMaxEntries = 32768
	}
	if out.StrictU8NoMidTurnHop == nil {
		t := true
		out.StrictU8NoMidTurnHop = &t
	}
	if out.ManualAutoFailover == nil {
		t := true
		out.ManualAutoFailover = &t
	}
	if out.PreStreamFailover == nil {
		t := true
		out.PreStreamFailover = &t
	}
	if out.MaxFailoverHops <= 0 {
		out.MaxFailoverHops = defaultMaxFailoverHops
	} else if out.MaxFailoverHops > 4 {
		out.MaxFailoverHops = 4
	}
	return out
}

func (c AccountPoolConfig) StrictU8Enabled() bool {
	if c.StrictU8NoMidTurnHop == nil {
		return true
	}
	return *c.StrictU8NoMidTurnHop
}

func (c AccountPoolConfig) ManualAutoFailoverEnabled() bool {
	if c.ManualAutoFailover == nil {
		return true
	}
	return *c.ManualAutoFailover
}

func (c AccountPoolConfig) PreStreamFailoverEnabled() bool {
	if c.PreStreamFailover == nil {
		return true
	}
	return *c.PreStreamFailover
}

func (c AccountPoolConfig) EffectiveMaxFailoverHops() int {
	if c.MaxFailoverHops <= 0 {
		return defaultMaxFailoverHops
	}
	if c.MaxFailoverHops > 4 {
		return 4
	}
	return c.MaxFailoverHops
}

// AccountGroup represents a sub2api-style channel group / pool binding multiple accounts under shared API keys.
type AccountGroup struct {
	ID                 string   `json:"id"`
	Label              string   `json:"label"`
	Strategy           string   `json:"strategy,omitempty"`
	ActiveAccountID    string   `json:"activeAccountId,omitempty"`
	ManualAutoFailover *bool    `json:"manualAutoFailover,omitempty"`
	BoundKeys          []string `json:"boundKeys"`
	AllowedModels      []string `json:"allowedModels,omitempty"`
}

func (g AccountGroup) AllowsModel(canonicalModelID string) bool {
	if canonicalModelID == "" || len(g.AllowedModels) == 0 {
		return true
	}
	for _, m := range g.AllowedModels {
		if m == "*" || strings.EqualFold(strings.TrimSpace(m), canonicalModelID) {
			return true
		}
	}
	return false
}

func (g AccountGroup) EffectiveManualAutoFailover(fallback bool) bool {
	if g.ManualAutoFailover == nil {
		return fallback
	}
	return *g.ManualAutoFailover
}

func maskSingleSecretPreview(raw string) string {
	s := strings.TrimSpace(raw)
	if s == "" {
		return ""
	}
	if len(s) <= 8 {
		return "***"
	}
	return s[:4] + "***" + s[len(s)-3:]
}

func maskBoundKeysForDashboard(keys []string) []string {
	if len(keys) == 0 {
		return []string{}
	}
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		if m := maskSingleSecretPreview(k); m != "" {
			out = append(out, m)
		}
	}
	return out
}

func sanitizeGroupsForDashboard(groups []AccountGroup) []AccountGroup {
	if len(groups) == 0 {
		return []AccountGroup{}
	}
	out := make([]AccountGroup, len(groups))
	for i, g := range groups {
		cp := g
		cp.BoundKeys = maskBoundKeysForDashboard(g.BoundKeys)
		out[i] = cp
	}
	return out
}

func normalizeRouteStrategy(s string) string {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case RouteStrategyFillFirst, "priority":
		return RouteStrategyFillFirst
	case RouteStrategyWeightedRR, "weighted", "wrr":
		return RouteStrategyWeightedRR
	case RouteStrategyLeastLoad, "smart", "quota-aware":
		return RouteStrategyLeastLoad
	case RouteStrategyManualActive, "manual", "active":
		return RouteStrategyManualActive
	case RouteStrategyRoundRobin, "", "rr":
		return RouteStrategyRoundRobin
	default:
		return RouteStrategyRoundRobin
	}
}

func isValidRouteStrategy(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", RouteStrategyRoundRobin, "rr",
		RouteStrategyFillFirst, "priority",
		RouteStrategyWeightedRR, "weighted", "wrr",
		RouteStrategyLeastLoad, "smart", "quota-aware",
		RouteStrategyManualActive, "manual", "active":
		return true
	default:
		return false
	}
}

func normalizeSchedState(schedState string, enabled bool) (string, bool) {
	st := strings.ToLower(strings.TrimSpace(schedState))
	if !enabled || st == SchedStateDisabled {
		return SchedStateDisabled, false
	}
	if st == SchedStateDraining {
		return SchedStateDraining, true
	}
	return SchedStateActive, true
}

func clampAccountPriority(p int) int {
	if p <= 0 {
		return defaultAccountPriority
	}
	if p > 1000 {
		return 1000
	}
	return p
}

func clampAccountWeight(w int) int {
	if w <= 0 {
		return defaultAccountWeight
	}
	if w > 100 {
		return 100
	}
	return w
}

func clampAccountMaxConcurrency(c, maxLiveWorkers int) int {
	if c < 0 {
		c = 0
	}
	if c > 64 {
		return 64
	}
	return c
}

func normalizeGroupList(groups []string) []string {
	if len(groups) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(groups))
	out := make([]string, 0, len(groups))
	for _, g := range groups {
		id := strings.ToLower(strings.TrimSpace(g))
		if id == "" || seen[id] || !validAccountIDRe.MatchString(id) {
			continue
		}
		seen[id] = true
		out = append(out, id)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (a AccountProfile) EffectiveSchedState() string {
	st, _ := normalizeSchedState(a.SchedState, a.Enabled)
	return st
}

func (a AccountProfile) EffectivePriority() int {
	return clampAccountPriority(a.Priority)
}

func (a AccountProfile) EffectiveWeight() int {
	return clampAccountWeight(a.Weight)
}

func (a AccountProfile) EffectiveMaxConcurrency() int {
	if a.MaxConcurrency > 0 {
		if a.MaxConcurrency > 64 {
			return 64
		}
		return a.MaxConcurrency
	}
	if a.MaxLiveWorkers > 0 {
		return a.MaxLiveWorkers
	}
	return 4
}

func (a AccountProfile) InGroup(groupID string) bool {
	gid := strings.ToLower(strings.TrimSpace(groupID))
	if gid == "" {
		return true
	}
	for _, g := range a.Groups {
		if strings.EqualFold(strings.TrimSpace(g), gid) {
			return true
		}
	}
	return false
}

// --- Bounded O(1) Sticky Session LRU ---

type stickyCacheNode struct {
	compositeKey string
	accountID    string
	modelID      string
	updatedAt    time.Time
}

type stickySessionLRU struct {
	ll        *list.List
	items     map[string]*list.Element
	byAccount map[string]int
}

func newStickySessionLRU() *stickySessionLRU {
	return &stickySessionLRU{
		ll:        list.New(),
		items:     make(map[string]*list.Element),
		byAccount: make(map[string]int),
	}
}

func (c *stickySessionLRU) ensureInit() {
	if c.ll == nil {
		c.ll = list.New()
	}
	if c.items == nil {
		c.items = make(map[string]*list.Element)
	}
	if c.byAccount == nil {
		c.byAccount = make(map[string]int)
	}
}

func (c *stickySessionLRU) removeElem(el *list.Element) {
	if el == nil {
		return
	}
	c.ll.Remove(el)
	node := el.Value.(*stickyCacheNode)
	delete(c.items, node.compositeKey)
	if cnt := c.byAccount[node.accountID] - 1; cnt <= 0 {
		delete(c.byAccount, node.accountID)
	} else {
		c.byAccount[node.accountID] = cnt
	}
}

func (c *stickySessionLRU) getFresh(compositeKey string, now time.Time, ttl time.Duration) (*stickyCacheNode, bool) {
	c.ensureInit()
	el, ok := c.items[compositeKey]
	if !ok {
		return nil, false
	}
	node := el.Value.(*stickyCacheNode)
	if now.Sub(node.updatedAt) >= ttl {
		c.removeElem(el)
		return nil, false
	}
	return node, true
}

func (c *stickySessionLRU) touch(compositeKey string, now time.Time) {
	c.ensureInit()
	if el, ok := c.items[compositeKey]; ok {
		el.Value.(*stickyCacheNode).updatedAt = now
		c.ll.MoveToFront(el)
	}
}

func (c *stickySessionLRU) put(compositeKey, accountID, modelID string, now time.Time, ttl time.Duration, maxEntries int) {
	c.ensureInit()
	if maxEntries <= 0 {
		maxEntries = defaultStickyMaxEntries
	}
	if el, ok := c.items[compositeKey]; ok {
		node := el.Value.(*stickyCacheNode)
		if node.accountID != accountID {
			if cnt := c.byAccount[node.accountID] - 1; cnt <= 0 {
				delete(c.byAccount, node.accountID)
			} else {
				c.byAccount[node.accountID] = cnt
			}
			c.byAccount[accountID]++
			node.accountID = accountID
		}
		node.modelID = modelID
		node.updatedAt = now
		c.ll.MoveToFront(el)
		return
	}
	for c.ll.Len() >= maxEntries {
		back := c.ll.Back()
		if back == nil {
			break
		}
		c.removeElem(back)
	}
	node := &stickyCacheNode{
		compositeKey: compositeKey,
		accountID:    accountID,
		modelID:      modelID,
		updatedAt:    now,
	}
	el := c.ll.PushFront(node)
	c.items[compositeKey] = el
	c.byAccount[accountID]++
}

func (c *stickySessionLRU) deleteKey(compositeKey string) {
	c.ensureInit()
	if el, ok := c.items[compositeKey]; ok {
		c.removeElem(el)
	}
}

func (c *stickySessionLRU) clearForAccount(accountID string) int {
	c.ensureInit()
	id := strings.ToLower(strings.TrimSpace(accountID))
	removed := 0
	for el := c.ll.Front(); el != nil; {
		next := el.Next()
		node := el.Value.(*stickyCacheNode)
		if id == "" || id == "*" || node.accountID == id {
			c.removeElem(el)
			removed++
		}
		el = next
	}
	return removed
}

func (c *stickySessionLRU) clearDisallowedModels(accountID string, allowedSet map[string]bool) int {
	c.ensureInit()
	if allowedSet["*"] {
		return 0
	}
	id := strings.ToLower(strings.TrimSpace(accountID))
	removed := 0
	for el := c.ll.Front(); el != nil; {
		next := el.Next()
		node := el.Value.(*stickyCacheNode)
		if node.accountID == id && node.modelID != "" && !allowedSet[node.modelID] {
			c.removeElem(el)
			removed++
		}
		el = next
	}
	return removed
}

func (c *stickySessionLRU) pruneExpired(now time.Time, ttl time.Duration) {
	c.ensureInit()
	for el := c.ll.Back(); el != nil; {
		prev := el.Prev()
		node := el.Value.(*stickyCacheNode)
		if now.Sub(node.updatedAt) < ttl {
			break
		}
		c.removeElem(el)
		el = prev
	}
}

func (c *stickySessionLRU) snapshotCounts(now time.Time, ttl time.Duration) (total int, perAccount map[string]int) {
	c.ensureInit()
	c.pruneExpired(now, ttl)
	out := make(map[string]int, len(c.byAccount))
	for k, v := range c.byAccount {
		if v > 0 {
			out[k] = v
			total += v
		}
	}
	return total, out
}

// --- Smooth Weighted Round-Robin (Nginx SWRR with dynamic candidate drift clamping) ---

type swrrEntry struct {
	currentWeight int
}

func pickSmoothWeighted(state map[string]*swrrEntry, candidates []AccountProfile) AccountProfile {
	if len(candidates) == 1 {
		if st := state[candidates[0].ID]; st != nil {
			st.currentWeight = 0
		}
		return candidates[0]
	}
	totalWeight := 0
	for _, acc := range candidates {
		totalWeight += acc.EffectiveWeight()
	}
	if totalWeight <= 0 {
		totalWeight = len(candidates)
	}
	var bestIdx int
	var bestEntry *swrrEntry
	bestVal := math.MinInt
	for i, acc := range candidates {
		w := acc.EffectiveWeight()
		st := state[acc.ID]
		if st == nil {
			st = &swrrEntry{}
			state[acc.ID] = st
		} else {
			if st.currentWeight > totalWeight {
				st.currentWeight = totalWeight
			} else if st.currentWeight < -totalWeight {
				st.currentWeight = -totalWeight
			}
		}
		st.currentWeight += w
		if i == 0 || st.currentWeight > bestVal {
			bestVal = st.currentWeight
			bestIdx = i
			bestEntry = st
		}
	}
	if bestEntry != nil {
		bestEntry.currentWeight -= totalWeight
	}
	return candidates[bestIdx]
}

// ResetRoutingWeights clears SWRR and RR state when account/group weights or memberships change.
func (am *AccountManager) ResetRoutingWeights() {
	am.stickyMu.Lock()
	am.swrrStates = make(map[string]map[string]*swrrEntry)
	am.rrCounters = make(map[string]uint64)
	am.stickyMu.Unlock()
}

// --- Per-Account Runtime Health, Cooldown, Concurrency & Telemetry ---

type familyCooldownEntry struct {
	Until      time.Time `json:"until"`
	Reason     string    `json:"reason"`
	StatusCode int       `json:"statusCode"`
	Strikes    int       `json:"strikes"`
}

type accountHealthRuntime struct {
	authErrorUntil    time.Time
	authErrorReason   string
	tosTripped        bool
	tosReason         string
	familyCooldowns   map[string]familyCooldownEntry
	consecutiveFails  int
	totalRequests     uint64
	totalFailures     uint64
	routedNewCount    uint64
	routedStickyCount uint64
	failoverOutCount  uint64
	error429Count     uint64
	error529Count     uint64
	lastRoutedAt      time.Time
	lastRouteMode     string
	lastSuccessAt     time.Time
	lastErrorAt       time.Time
	lastError         string
}

// AccountHealthView is the JSON-safe health & routing telemetry summary returned by /api/accounts and federated dashboard cards.
type AccountHealthView struct {
	State             string         `json:"state"` // "healthy" | "cooldown" | "auth_error" | "tos_tripped" | "draining" | "disabled" | "unauthenticated"
	CooldownSec       int            `json:"cooldownSec"`
	CooldownUntil     string         `json:"cooldownUntil,omitempty"`
	CooldownReason    string         `json:"cooldownReason,omitempty"`
	FamilyCooldowns   map[string]int `json:"familyCooldowns,omitempty"`
	InFlight          int64          `json:"inFlight"`
	MaxConcurrency    int            `json:"maxConcurrency"`
	StickySessions    int            `json:"stickySessions"`
	ConsecutiveFails  int            `json:"consecutiveFails"`
	TotalRequests     uint64         `json:"totalRequests"`
	TotalFailures     uint64         `json:"totalFailures"`
	RoutedNewCount    uint64         `json:"routedNewCount"`
	RoutedStickyCount uint64         `json:"routedStickyCount"`
	FailoverOutCount  uint64         `json:"failoverOutCount"`
	Error429Count     uint64         `json:"error429Count"`
	Error529Count     uint64         `json:"error529Count"`
	LastRoutedAt      string         `json:"lastRoutedAt,omitempty"`
	LastRouteMode     string         `json:"lastRouteMode,omitempty"`
	LastSuccessAt     string         `json:"lastSuccessAt,omitempty"`
	LastErrorAt       string         `json:"lastErrorAt,omitempty"`
	LastError         string         `json:"lastError,omitempty"`
}

func accountCooldownFamily(canonicalModelID string) string {
	low := strings.ToLower(strings.TrimSpace(canonicalModelID))
	if low == "gemini-4-argon" {
		return "argon"
	}
	if strings.HasPrefix(low, "claude-") || strings.HasPrefix(low, "gpt-") {
		return "claude"
	}
	return "gemini"
}

func parseRetryAfterDuration(hdrVal string, maxCap time.Duration) (time.Duration, bool) {
	s := strings.TrimSpace(hdrVal)
	if s == "" {
		return 0, false
	}
	if secs, err := strconv.Atoi(s); err == nil && secs > 0 {
		d := time.Duration(secs) * time.Second
		if d < 5*time.Second {
			d = 5 * time.Second
		}
		if maxCap > 0 && d > maxCap {
			d = maxCap
		}
		return d, true
	}
	if ts, err := http.ParseTime(s); err == nil {
		d := time.Until(ts)
		if d <= 0 {
			return 5 * time.Second, true
		}
		if d < 5*time.Second {
			d = 5 * time.Second
		}
		if maxCap > 0 && d > maxCap {
			d = maxCap
		}
		return d, true
	}
	return 0, false
}

func rateLimitBackoffDuration(strikes int) time.Duration {
	if strikes <= 1 {
		return 30 * time.Second
	}
	steps := []time.Duration{
		30 * time.Second,
		60 * time.Second,
		2 * time.Minute,
		5 * time.Minute,
		10 * time.Minute,
	}
	if strikes-1 >= len(steps) {
		return steps[len(steps)-1]
	}
	return steps[strikes-1]
}

func overloadedBackoffDuration(strikes int) time.Duration {
	if strikes <= 1 {
		return 15 * time.Second
	}
	if strikes == 2 {
		return 30 * time.Second
	}
	return 60 * time.Second
}

func (am *AccountManager) counterForAccount(accountID string) *atomic.Int64 {
	if v, ok := am.inflightMap.Load(accountID); ok {
		return v.(*atomic.Int64)
	}
	actual, _ := am.inflightMap.LoadOrStore(accountID, &atomic.Int64{})
	return actual.(*atomic.Int64)
}

func (am *AccountManager) AcquireAccountInFlight(accountID string) func() {
	if accountID == "" {
		return func() {}
	}
	c := am.counterForAccount(accountID)
	c.Add(1)
	var once sync.Once
	return func() {
		once.Do(func() {
			c.Add(-1)
		})
	}
}

func (am *AccountManager) AccountInFlight(accountID string) int64 {
	if accountID == "" {
		return 0
	}
	if v, ok := am.inflightMap.Load(accountID); ok {
		n := v.(*atomic.Int64).Load()
		if n < 0 {
			return 0
		}
		return n
	}
	return 0
}

func (am *AccountManager) ensureHealthRuntimeLocked(id string) *accountHealthRuntime {
	if am.healthStates == nil {
		am.healthStates = make(map[string]*accountHealthRuntime)
	}
	st := am.healthStates[id]
	if st == nil {
		st = &accountHealthRuntime{
			familyCooldowns: make(map[string]familyCooldownEntry),
		}
		am.healthStates[id] = st
	}
	return st
}

// RecordAccountRouteHit records routing selection telemetry (new vs sticky session).
func (am *AccountManager) RecordAccountRouteHit(accountID, routeMode string, isSticky bool) {
	id := strings.ToLower(strings.TrimSpace(accountID))
	if id == "" || isChildAccountWorker() {
		return
	}
	now := time.Now()
	am.healthMu.Lock()
	st := am.ensureHealthRuntimeLocked(id)
	if isSticky {
		st.routedStickyCount++
	} else {
		st.routedNewCount++
	}
	st.lastRoutedAt = now
	if routeMode != "" {
		st.lastRouteMode = routeMode
	}
	am.healthMu.Unlock()
}

// RecordAccountFailoverOut increments the pre-stream failover departure counter on an account.
func (am *AccountManager) RecordAccountFailoverOut(accountID string) {
	id := strings.ToLower(strings.TrimSpace(accountID))
	if id == "" || isChildAccountWorker() {
		return
	}
	am.healthMu.Lock()
	st := am.ensureHealthRuntimeLocked(id)
	st.failoverOutCount++
	am.healthMu.Unlock()
}

func isToSOrBanError(statusCode int, errType, errMsg string) bool {
	low := strings.ToLower(errType + " " + errMsg)
	for _, needle := range []string{
		"tos_circuit_breaker",
		"tos circuit",
		"terms of service",
		"tos violation",
		"tos_ban_error",
		"account has been disabled",
		"account is disabled",
		"account disabled",
		"permanently disabled",
		"account is suspended",
		"account suspended",
		"has been suspended",
		"permanently suspended",
		"banned",
		"policy violation",
		"abuse violation",
		"terms violation",
		"account violation",
		"not eligible",
		"ineligible",
	} {
		if strings.Contains(low, needle) {
			return true
		}
	}
	if strings.Contains(low, "permission_denied") || statusCode == http.StatusForbidden {
		for _, needle := range []string{"suspend", "disabled", "restricted"} {
			if strings.Contains(low, needle) &&
				!strings.Contains(low, "bound to this api key is disabled") &&
				!strings.Contains(low, "is not found or disabled") {
				return true
			}
		}
	}
	return false
}

func isAuthErrorMessage(errMsg string) bool {
	low := strings.ToLower(errMsg)
	for _, needle := range []string{
		"authentication required",
		"authentication failed",
		"not logged in",
		"login required",
		"please login",
		"please log in",
		"re-authenticate",
		"reauthenticate",
		"invalid_grant",
		"token expired",
		"token revoked",
		"expired or revoked",
		"unauthenticated",
		"unauthorized",
		"not_authenticated",
		"3501",
		"valid license",
	} {
		if strings.Contains(low, needle) {
			return true
		}
	}
	return false
}

func isAccountAuthFailure(statusCode int, errType, errMsg string) bool {
	if statusCode == http.StatusUnauthorized {
		return true
	}
	lowErr := strings.ToLower(errType + " " + errMsg)
	if isAuthErrorMessage(lowErr) {
		return true
	}
	if statusCode == http.StatusForbidden &&
		(strings.Contains(lowErr, "3501") || strings.Contains(lowErr, "authentication") ||
			strings.Contains(lowErr, "oauth") || strings.Contains(lowErr, "license") ||
			strings.Contains(lowErr, "unauthenticated")) {
		return true
	}
	if statusCode == http.StatusServiceUnavailable &&
		(strings.Contains(lowErr, "not_authenticated") || strings.Contains(lowErr, "oauth")) {
		return true
	}
	return false
}

// RecordAccountOutcome updates the account's real-time health & cooldown state machine from an upstream result.
func (am *AccountManager) RecordAccountOutcome(accountID, canonicalModelID string, statusCode int, errType, retryAfterHdr, errMsg string) {
	id := strings.ToLower(strings.TrimSpace(accountID))
	if id == "" || isChildAccountWorker() {
		return
	}
	now := time.Now()
	fam := accountCooldownFamily(canonicalModelID)

	am.healthMu.Lock()
	st := am.ensureHealthRuntimeLocked(id)
	st.totalRequests++

	if statusCode >= 200 && statusCode < 300 {
		st.consecutiveFails = 0
		st.lastSuccessAt = now
		delete(st.familyCooldowns, fam)
		if !st.authErrorUntil.IsZero() {
			st.authErrorUntil = time.Time{}
			st.authErrorReason = ""
		}
		am.healthMu.Unlock()
		return
	}

	// Ignore client-side 400/404/413/422 validation errors and 499 disconnects.
	if statusCode == http.StatusBadRequest || statusCode == http.StatusNotFound ||
		statusCode == http.StatusRequestEntityTooLarge || statusCode == http.StatusUnprocessableEntity ||
		statusCode == 499 {
		am.healthMu.Unlock()
		return
	}

	cleanMsg := gwcfg.RedactSecretsInString(truncateRunes(strings.TrimSpace(errMsg), 200))
	if cleanMsg == "" {
		cleanMsg = fmt.Sprintf("HTTP %d %s", statusCode, strings.TrimSpace(errType))
	}

	// Permanent ban / suspension / ToS circuit breaker -> persistent tosTripped state
	// (never silently expires after 30 minutes; cleared explicitly on ClearAccountCooldown / health_check / re-auth / enable).
	if isToSOrBanError(statusCode, errType, errMsg) {
		st.totalFailures++
		st.consecutiveFails++
		st.tosTripped = true
		st.tosReason = cleanMsg
		st.lastErrorAt = now
		st.lastError = cleanMsg
		am.healthMu.Unlock()
		return
	}

	// 401 or non-ToS 403/503 auth/license failure (#3501 / expired OAuth / login required)
	if isAccountAuthFailure(statusCode, errType, errMsg) {
		st.totalFailures++
		st.consecutiveFails++
		st.authErrorUntil = now.Add(30 * time.Minute)
		st.authErrorReason = cleanMsg
		st.lastErrorAt = now
		st.lastError = cleanMsg
		am.healthMu.Unlock()
		return
	}

	// Non-auth 403 (e.g. model allowlist denial) -> client permission issue, not account health.
	if statusCode == http.StatusForbidden {
		am.healthMu.Unlock()
		return
	}

	// 429 Rate limit / quota cooldown (scoped to model family!)
	if statusCode == http.StatusTooManyRequests {
		st.totalFailures++
		st.error429Count++
		st.consecutiveFails++
		prev := st.familyCooldowns[fam]
		strikes := prev.Strikes + 1
		if !prev.Until.IsZero() && now.Sub(prev.Until) > 10*time.Minute {
			strikes = 1
		}
		dur, ok := parseRetryAfterDuration(retryAfterHdr, 6*time.Hour)
		if !ok {
			dur = rateLimitBackoffDuration(strikes)
		}
		until := now.Add(dur)
		if prev.Until.After(until) {
			until = prev.Until
		}
		st.familyCooldowns[fam] = familyCooldownEntry{
			Until:      until,
			Reason:     cleanMsg,
			StatusCode: statusCode,
			Strikes:    strikes,
		}
		st.lastErrorAt = now
		st.lastError = cleanMsg
		am.healthMu.Unlock()
		return
	}

	// 529 Overloaded (scoped to model family)
	if statusCode == 529 {
		st.totalFailures++
		st.error529Count++
		st.consecutiveFails++
		prev := st.familyCooldowns[fam]
		strikes := prev.Strikes + 1
		if !prev.Until.IsZero() && now.Sub(prev.Until) > 5*time.Minute {
			strikes = 1
		}
		dur, ok := parseRetryAfterDuration(retryAfterHdr, 10*time.Minute)
		if !ok {
			dur = overloadedBackoffDuration(strikes)
		}
		until := now.Add(dur)
		if prev.Until.After(until) {
			until = prev.Until
		}
		st.familyCooldowns[fam] = familyCooldownEntry{
			Until:      until,
			Reason:     cleanMsg,
			StatusCode: statusCode,
			Strikes:    strikes,
		}
		st.lastErrorAt = now
		st.lastError = cleanMsg
		am.healthMu.Unlock()
		return
	}

	// 500/502/503/504 transient upstream or child worker errors
	if statusCode >= 500 {
		st.totalFailures++
		st.consecutiveFails++
		st.lastErrorAt = now
		st.lastError = cleanMsg
		if statusCode == http.StatusBadGateway || st.consecutiveFails >= 2 {
			prev := st.familyCooldowns[fam]
			coolDur := 20 * time.Second
			if statusCode == http.StatusBadGateway {
				coolDur = 15 * time.Second
			}
			until := now.Add(coolDur)
			if !prev.Until.After(until) {
				st.familyCooldowns[fam] = familyCooldownEntry{
					Until:      until,
					Reason:     cleanMsg,
					StatusCode: statusCode,
					Strikes:    prev.Strikes + 1,
				}
			}
		}
		am.healthMu.Unlock()
		return
	}

	am.healthMu.Unlock()
}

// RecordQuotaSyncOutcome classifies recognizable ban, suspension, or auth errors from /usage
// into the account's health state without inflating request counts or tripping on transient timeouts.
func (am *AccountManager) RecordQuotaSyncOutcome(accountID, errMsg string) {
	id := strings.ToLower(strings.TrimSpace(accountID))
	raw := strings.TrimSpace(errMsg)
	if id == "" || raw == "" || isChildAccountWorker() || !am.HasConfiguredAccounts() {
		return
	}
	acc, ok := am.GetAccount(id)
	if !ok || !acc.Enabled {
		return
	}
	isBan := isToSOrBanError(0, "", raw)
	isAuth := !isBan && isAuthErrorMessage(raw)
	if !isBan && !isAuth {
		return
	}
	cleanMsg := gwcfg.RedactSecretsInString(truncateRunes(raw, 200))
	now := time.Now()
	am.healthMu.Lock()
	st := am.ensureHealthRuntimeLocked(id)
	if isBan {
		st.tosTripped = true
		st.tosReason = cleanMsg
	} else {
		st.authErrorUntil = now.Add(30 * time.Minute)
		st.authErrorReason = cleanMsg
	}
	st.lastErrorAt = now
	st.lastError = cleanMsg
	am.healthMu.Unlock()
}

// ClearAccountCooldown resets transient rate-limit, overload, and auth error states for one or all ("*") accounts.
func (am *AccountManager) ClearAccountCooldown(accountID string) {
	id := strings.ToLower(strings.TrimSpace(accountID))
	am.healthMu.Lock()
	if id == "" || id == "*" {
		for _, st := range am.healthStates {
			if st != nil {
				st.authErrorUntil = time.Time{}
				st.authErrorReason = ""
				st.tosTripped = false
				st.tosReason = ""
				st.consecutiveFails = 0
				st.familyCooldowns = make(map[string]familyCooldownEntry)
			}
		}
	} else if st := am.healthStates[id]; st != nil {
		st.authErrorUntil = time.Time{}
		st.authErrorReason = ""
		st.tosTripped = false
		st.tosReason = ""
		st.consecutiveFails = 0
		st.familyCooldowns = make(map[string]familyCooldownEntry)
	}
	am.healthMu.Unlock()

	am.mu.Lock()
	if id == "" || id == "*" {
		am.crashStates = make(map[string]workerCrashState)
	} else {
		delete(am.crashStates, id)
	}
	am.mu.Unlock()
}

type accountHardBlockInfo struct {
	blocked  bool
	state    string
	reason   string
	retrySec int
}

type accountCoolInfo struct {
	cooling  bool
	reason   string
	retrySec int
}

// snapshotRoutingHealth captures hard-block & family-cooldown state across all accounts in a single RLock.
func (am *AccountManager) snapshotRoutingHealth(accs []AccountProfile, canonicalModelID string, now time.Time) (hardMap map[string]accountHardBlockInfo, coolMap map[string]accountCoolInfo) {
	hardMap = make(map[string]accountHardBlockInfo, len(accs))
	coolMap = make(map[string]accountCoolInfo, len(accs))
	fam := accountCooldownFamily(canonicalModelID)

	var primaryTripped bool
	var primaryTosReason string
	for _, a := range accs {
		if a.Primary {
			primaryTripped, primaryTosReason = IsTosTripped()
			break
		}
	}

	am.healthMu.RLock()
	for _, a := range accs {
		if a.Primary && primaryTripped {
			hardMap[a.ID] = accountHardBlockInfo{
				blocked: true,
				state:   "tos_tripped",
				reason:  "ToS circuit breaker tripped: " + primaryTosReason,
			}
			continue
		}
		st := am.healthStates[a.ID]
		if st == nil {
			continue
		}
		if st.tosTripped {
			hardMap[a.ID] = accountHardBlockInfo{
				blocked: true,
				state:   "tos_tripped",
				reason:  st.tosReason,
			}
			continue
		}
		if !st.authErrorUntil.IsZero() && now.Before(st.authErrorUntil) {
			sec := int(math.Ceil(st.authErrorUntil.Sub(now).Seconds()))
			if sec < 1 {
				sec = 1
			}
			hardMap[a.ID] = accountHardBlockInfo{
				blocked:  true,
				state:    "auth_error",
				reason:   st.authErrorReason,
				retrySec: sec,
			}
			continue
		}
		if fc, ok := st.familyCooldowns[fam]; ok && !fc.Until.IsZero() && now.Before(fc.Until) {
			sec := int(math.Ceil(fc.Until.Sub(now).Seconds()))
			if sec < 1 {
				sec = 1
			}
			coolMap[a.ID] = accountCoolInfo{
				cooling:  true,
				reason:   fc.Reason,
				retrySec: sec,
			}
		}
	}
	am.healthMu.RUnlock()
	return hardMap, coolMap
}

func (am *AccountManager) isAccountHardBlocked(accountID string, now time.Time) (blocked bool, state, reason string, retrySec int) {
	if accountID == "primary" {
		if tripped, r := IsTosTripped(); tripped {
			return true, "tos_tripped", "ToS circuit breaker tripped: " + r, 0
		}
	}
	am.healthMu.RLock()
	defer am.healthMu.RUnlock()
	st := am.healthStates[accountID]
	if st == nil {
		return false, "", "", 0
	}
	if st.tosTripped {
		return true, "tos_tripped", st.tosReason, 0
	}
	if !st.authErrorUntil.IsZero() && now.Before(st.authErrorUntil) {
		sec := int(math.Ceil(st.authErrorUntil.Sub(now).Seconds()))
		if sec < 1 {
			sec = 1
		}
		return true, "auth_error", st.authErrorReason, sec
	}
	return false, "", "", 0
}

func (am *AccountManager) isAccountFamilyCoolingDown(accountID, canonicalModelID string, now time.Time) (cooling bool, reason string, retrySec int) {
	fam := accountCooldownFamily(canonicalModelID)
	am.healthMu.RLock()
	defer am.healthMu.RUnlock()
	st := am.healthStates[accountID]
	if st == nil {
		return false, "", 0
	}
	fc, ok := st.familyCooldowns[fam]
	if !ok || fc.Until.IsZero() || !now.Before(fc.Until) {
		return false, "", 0
	}
	sec := int(math.Ceil(fc.Until.Sub(now).Seconds()))
	if sec < 1 {
		sec = 1
	}
	return true, fc.Reason, sec
}

func buildSingleHealthViewFromRuntime(acc AccountProfile, hasToken bool, inFlight int64, stickyCount int, st *accountHealthRuntime, primaryTripped bool, primaryTosReason string, now time.Time) AccountHealthView {
	sched := acc.EffectiveSchedState()
	view := AccountHealthView{
		State:          "healthy",
		InFlight:       inFlight,
		MaxConcurrency: acc.EffectiveMaxConcurrency(),
		StickySessions: stickyCount,
	}
	if sched == SchedStateDisabled {
		view.State = "disabled"
	} else if !acc.Primary && !hasToken {
		view.State = "unauthenticated"
	} else if sched == SchedStateDraining {
		view.State = "draining"
	}
	if acc.Primary && primaryTripped {
		view.CooldownReason = primaryTosReason
		if sched != SchedStateDisabled {
			view.State = "tos_tripped"
		}
	}
	if st == nil {
		return view
	}

	view.ConsecutiveFails = st.consecutiveFails
	view.TotalRequests = st.totalRequests
	view.TotalFailures = st.totalFailures
	view.RoutedNewCount = st.routedNewCount
	view.RoutedStickyCount = st.routedStickyCount
	view.FailoverOutCount = st.failoverOutCount
	view.Error429Count = st.error429Count
	view.Error529Count = st.error529Count
	view.LastRouteMode = st.lastRouteMode
	if !st.lastRoutedAt.IsZero() {
		view.LastRoutedAt = st.lastRoutedAt.UTC().Format(time.RFC3339)
	}
	if !st.lastSuccessAt.IsZero() {
		view.LastSuccessAt = st.lastSuccessAt.UTC().Format(time.RFC3339)
	}
	if !st.lastErrorAt.IsZero() {
		view.LastErrorAt = st.lastErrorAt.UTC().Format(time.RFC3339)
		view.LastError = st.lastError
	}

	// Disabled accounts ALWAYS report state="disabled" while preserving diagnostic error details.
	if sched == SchedStateDisabled {
		view.State = "disabled"
		if st.tosTripped && st.tosReason != "" {
			view.CooldownReason = st.tosReason
		} else if !st.authErrorUntil.IsZero() && now.Before(st.authErrorUntil) && st.authErrorReason != "" {
			view.CooldownReason = st.authErrorReason
		}
		return view
	}

	if st.tosTripped {
		view.State = "tos_tripped"
		view.CooldownReason = st.tosReason
		return view
	}
	if view.State == "tos_tripped" {
		return view
	}
	if !st.authErrorUntil.IsZero() && now.Before(st.authErrorUntil) {
		sec := int(math.Ceil(st.authErrorUntil.Sub(now).Seconds()))
		if sec < 1 {
			sec = 1
		}
		view.State = "auth_error"
		view.CooldownSec = sec
		view.CooldownUntil = st.authErrorUntil.UTC().Format(time.RFC3339)
		view.CooldownReason = st.authErrorReason
		return view
	}

	var maxUntil time.Time
	var maxReason string
	var famMap map[string]int
	for fam, fc := range st.familyCooldowns {
		if !fc.Until.IsZero() && now.Before(fc.Until) {
			sec := int(math.Ceil(fc.Until.Sub(now).Seconds()))
			if sec < 1 {
				sec = 1
			}
			if famMap == nil {
				famMap = make(map[string]int, len(st.familyCooldowns))
			}
			famMap[fam] = sec
			if fc.Until.After(maxUntil) {
				maxUntil = fc.Until
				maxReason = fc.Reason
			}
		}
	}
	if len(famMap) > 0 {
		view.FamilyCooldowns = famMap
		sec := int(math.Ceil(maxUntil.Sub(now).Seconds()))
		if sec < 1 {
			sec = 1
		}
		view.CooldownSec = sec
		view.CooldownUntil = maxUntil.UTC().Format(time.RFC3339)
		view.CooldownReason = maxReason
		if view.State == "healthy" {
			view.State = "cooldown"
		}
	}
	return view
}

// BuildAccountHealthView constructs the public health & cooldown view for one account.
func (am *AccountManager) BuildAccountHealthView(acc AccountProfile, hasToken bool, stickyCount int, now time.Time) AccountHealthView {
	inFlight := am.AccountInFlight(acc.ID)
	primaryTripped, primaryReason := false, ""
	if acc.Primary {
		primaryTripped, primaryReason = IsTosTripped()
	}
	am.healthMu.RLock()
	var stCopy accountHealthRuntime
	var stPtr *accountHealthRuntime
	if raw := am.healthStates[acc.ID]; raw != nil {
		stCopy = *raw
		if len(raw.familyCooldowns) > 0 {
			stCopy.familyCooldowns = make(map[string]familyCooldownEntry, len(raw.familyCooldowns))
			for k, v := range raw.familyCooldowns {
				stCopy.familyCooldowns[k] = v
			}
		}
		stPtr = &stCopy
	}
	am.healthMu.RUnlock()
	return buildSingleHealthViewFromRuntime(acc, hasToken, inFlight, stickyCount, stPtr, primaryTripped, primaryReason, now)
}

// SnapshotHealthViews builds AccountHealthView for all supplied accounts using a single healthMu.RLock.
func (am *AccountManager) SnapshotHealthViews(accs []AccountProfile, stickyByAcc map[string]int, now time.Time) map[string]AccountHealthView {
	out := make(map[string]AccountHealthView, len(accs))
	if len(accs) == 0 {
		return out
	}
	hasTokByID := make(map[string]bool, len(accs))
	inFlightByID := make(map[string]int64, len(accs))
	primaryTripped, primaryReason := IsTosTripped()
	for _, a := range accs {
		hasTokByID[a.ID] = a.Primary || am.HasOAuthToken(a.ID)
		inFlightByID[a.ID] = am.AccountInFlight(a.ID)
	}
	am.healthMu.RLock()
	for _, a := range accs {
		out[a.ID] = buildSingleHealthViewFromRuntime(
			a,
			hasTokByID[a.ID],
			inFlightByID[a.ID],
			stickyByAcc[a.ID],
			am.healthStates[a.ID],
			primaryTripped,
			primaryReason,
			now,
		)
	}
	am.healthMu.RUnlock()
	return out
}

// --- Quota Exhaustion & Remaining Fraction (5h + Weekly) ---

func quotaGroupsModelQuotaState(groups []QuotaGroup, argonPool *ExtraPoolQuota, canonicalModelID string, now time.Time) (exhausted bool, minRemFrac float64, resetAt time.Time) {
	lowModel := strings.ToLower(strings.TrimSpace(canonicalModelID))
	isClaude := strings.HasPrefix(lowModel, "claude-") || strings.HasPrefix(lowModel, "gpt-")
	isArgon := lowModel == "gemini-4-argon"

	minRemFrac = 1.0
	sawKnown := false

	if isArgon && argonPool != nil && argonPool.RemainingFraction != nil {
		sawKnown = true
		rf := *argonPool.RemainingFraction
		if rf < minRemFrac {
			minRemFrac = rf
		}
		if extraPoolStillExhausted(argonPool, now) {
			exhausted = true
			if argonPool.ResetTime != "" {
				if rt, err := time.Parse(time.RFC3339, argonPool.ResetTime); err == nil {
					resetAt = rt
				}
			}
		}
	}

	for _, g := range groups {
		lowName := strings.ToLower(g.Name)
		matched := false
		if isClaude {
			matched = strings.Contains(lowName, "claude") || strings.Contains(lowName, "gpt")
		} else if !isArgon {
			matched = strings.EqualFold(g.Name, "Gemini Models") || (strings.Contains(lowName, "gemini") && !strings.Contains(lowName, "argon"))
		}
		if !matched {
			continue
		}
		for _, b := range g.Buckets {
			if b.Window != "5h" && b.Window != "weekly" {
				continue
			}
			sawKnown = true
			if b.RemainingFraction < minRemFrac {
				minRemFrac = b.RemainingFraction
			}
			if quotaBucketStillExhausted(b, now) {
				exhausted = true
				if b.ResetTime != "" {
					if rt, err := time.Parse(time.RFC3339, b.ResetTime); err == nil && (resetAt.IsZero() || rt.After(resetAt)) {
						resetAt = rt
					}
				}
			}
		}
	}
	if !sawKnown {
		return false, 0.5, time.Time{}
	}
	if minRemFrac < 0 {
		minRemFrac = 0
	}
	return exhausted, minRemFrac, resetAt
}

func (am *AccountManager) evaluateAccountQuota(acc AccountProfile, dashPort int, canonicalModelID string, now time.Time) (exhausted bool, remFrac float64, resetAt time.Time) {
	if acc.Primary {
		quotaMu.Lock()
		snap := cachedQuota
		snapAt := cachedQuotaAt
		quotaMu.Unlock()
		if !testing.Testing() && (snap == nil || now.Sub(snapAt) > 15*time.Minute) {
			go func() {
				_ = FetchOfficialQuota(false)
			}()
		}
		if snap == nil || now.Sub(snapAt) > 20*time.Minute {
			return false, 0.5, time.Time{}
		}
		return quotaGroupsModelQuotaState(snap.Groups, snap.ArgonPool, canonicalModelID, now)
	}
	if dashPort <= 0 {
		return false, 0.5, time.Time{}
	}
	cacheKey := fmt.Sprintf("%s|http://127.0.0.1:%d", acc.ID, dashPort)
	peerDashCacheMu.Lock()
	cached, ok := peerDashCache[cacheKey]
	lastAttempt := peerDashLastAttempt[cacheKey]
	shouldTrigger := !testing.Testing() &&
		(!ok || cached.decoded == nil || now.Sub(cached.fetchedAt) > 60*time.Second) &&
		now.Sub(lastAttempt) > 10*time.Second
	if shouldTrigger {
		peerDashLastAttempt[cacheKey] = now
	}
	peerDashCacheMu.Unlock()

	if shouldTrigger {
		peer := gwcfg.DashboardPeerConfig{
			ID:      acc.ID,
			Label:   acc.ID,
			BaseURL: fmt.Sprintf("http://127.0.0.1:%d", dashPort),
		}
		fallbackKey := defaultPeerAPIKey(gwcfg.Current())
		go func(p gwcfg.DashboardPeerConfig, k string) {
			_, _, _, _, _ = fetchPeerDashboard(p, false, k)
		}(peer, fallbackKey)
	}
	if !ok || cached.decoded == nil || now.Sub(cached.fetchedAt) > 20*time.Minute {
		return false, 0.5, time.Time{}
	}
	return quotaGroupsModelQuotaState(cached.decoded.Quota.Groups, cached.decoded.Quota.ArgonPool, canonicalModelID, now)
}

// --- Group Key & Header Resolution ---

func splitKeyGroupSuffix(candidate string) (baseKey, groupID string, ok bool) {
	for _, sep := range []string{"@group:", "#group:"} {
		if idx := strings.LastIndex(strings.ToLower(candidate), sep); idx > 0 && idx+len(sep) < len(candidate) {
			base := candidate[:idx]
			gid := strings.ToLower(strings.TrimSpace(candidate[idx+len(sep):]))
			if validAccountIDRe.MatchString(gid) {
				return base, gid, true
			}
		}
	}
	return "", "", false
}

// MatchesGroupKey reports whether candidate matches any group's boundKeys or "<master_key>@group:<group_id>".
func (am *AccountManager) MatchesGroupKey(candidate string) (AccountGroup, bool) {
	if candidate == "" {
		return AccountGroup{}, false
	}
	am.ensureInitialized()
	if base, gid, ok := splitKeyGroupSuffix(candidate); ok {
		if rawMasterKeyMatches(base) {
			if grp, exists := am.GetGroup(gid); exists {
				return grp, true
			}
		}
	}
	cBytes := []byte(candidate)
	am.mu.RLock()
	defer am.mu.RUnlock()
	for _, grp := range am.groups {
		for _, bk := range grp.BoundKeys {
			bk = strings.TrimSpace(bk)
			if bk != "" && subtle.ConstantTimeCompare(cBytes, []byte(bk)) == 1 {
				return grp, true
			}
		}
	}
	return AccountGroup{}, false
}

func (am *AccountManager) GetGroup(groupID string) (AccountGroup, bool) {
	am.ensureInitialized()
	gid := strings.ToLower(strings.TrimSpace(groupID))
	am.mu.RLock()
	defer am.mu.RUnlock()
	for _, g := range am.groups {
		if g.ID == gid {
			return g, true
		}
	}
	return AccountGroup{}, false
}

func (am *AccountManager) ListGroups() []AccountGroup {
	am.ensureInitialized()
	am.mu.RLock()
	defer am.mu.RUnlock()
	out := make([]AccountGroup, len(am.groups))
	copy(out, am.groups)
	return out
}

func (am *AccountManager) GetRoutingConfig() AccountPoolConfig {
	am.ensureInitialized()
	am.mu.RLock()
	defer am.mu.RUnlock()
	return am.routing.Normalized()
}

func (am *AccountManager) ResolveGroupScope(r *http.Request) (grp *AccountGroup, explicit bool, invalidErr error) {
	if r == nil {
		return nil, false, nil
	}
	hdrGrp := strings.ToLower(strings.TrimSpace(r.Header.Get("X-Agy-Group")))
	if reqK := requestKey(r); reqK != "" {
		if matchedGrp, ok := am.MatchesGroupKey(reqK); ok {
			if hdrGrp != "" && hdrGrp != matchedGrp.ID {
				return nil, true, fmt.Errorf("X-Agy-Group %q cannot override API key bound to group %q", hdrGrp, matchedGrp.ID)
			}
			cp := matchedGrp
			return &cp, true, nil
		}
	}
	if hdrGrp != "" {
		if matchedGrp, ok := am.GetGroup(hdrGrp); ok {
			cp := matchedGrp
			return &cp, true, nil
		}
		return nil, true, fmt.Errorf("account group %q is not found", hdrGrp)
	}
	return nil, false, nil
}

// --- Routing Config, Group CRUD & One-Click Account Actions ---

func (am *AccountManager) UpdateRoutingConfig(in AccountPoolConfig) (AccountPoolConfig, error) {
	am.ensureInitialized()
	if !isValidRouteStrategy(in.Strategy) {
		return AccountPoolConfig{}, fmt.Errorf("不支持的路由策略 %q", in.Strategy)
	}
	norm := in.Normalized()
	am.mu.Lock()
	if norm.ActiveAccountID != "" {
		found := false
		for _, a := range am.accounts {
			if a.ID == norm.ActiveAccountID {
				found = true
				break
			}
		}
		if !found {
			am.mu.Unlock()
			return AccountPoolConfig{}, fmt.Errorf("指定的主路由账号 %q 不存在", norm.ActiveAccountID)
		}
	}
	am.routing = norm
	err := am.saveToDiskLocked()
	am.mu.Unlock()
	if err != nil {
		return AccountPoolConfig{}, err
	}
	InvalidatePeerDashboardCache()
	return norm, nil
}

func (am *AccountManager) UpsertGroup(in AccountGroup) (AccountGroup, error) {
	am.ensureInitialized()
	in.ID = strings.ToLower(strings.TrimSpace(in.ID))
	if !validAccountIDRe.MatchString(in.ID) {
		return AccountGroup{}, errors.New("分组 ID 仅允许小写字母、数字、连字符或下划线 (1-32 位)")
	}
	in.Label = strings.TrimSpace(in.Label)
	if in.Label == "" {
		in.Label = in.ID
	}
	if !isValidRouteStrategy(in.Strategy) {
		return AccountGroup{}, fmt.Errorf("不支持的分组路由策略 %q", in.Strategy)
	}
	if strings.TrimSpace(in.Strategy) != "" {
		in.Strategy = normalizeRouteStrategy(in.Strategy)
	}
	in.ActiveAccountID = strings.ToLower(strings.TrimSpace(in.ActiveAccountID))
	if in.ActiveAccountID != "" && !validAccountIDRe.MatchString(in.ActiveAccountID) {
		return AccountGroup{}, fmt.Errorf("激活账号 ID %q 格式不合法", in.ActiveAccountID)
	}

	seenKeys := make(map[string]bool, len(in.BoundKeys))
	cleanKeys := make([]string, 0, len(in.BoundKeys))
	for _, k := range in.BoundKeys {
		k = strings.TrimSpace(k)
		if k == "" || seenKeys[k] {
			continue
		}
		if rawMasterKeyMatches(k) {
			return AccountGroup{}, errors.New("分组专属 Key 不可与网关主管理 Key 相同")
		}
		seenKeys[k] = true
		cleanKeys = append(cleanKeys, k)
	}
	in.BoundKeys = cleanKeys
	if len(in.AllowedModels) > 0 {
		in.AllowedModels = normalizeCanonicalModelList(in.AllowedModels)
	} else {
		in.AllowedModels = nil
	}

	am.mu.Lock()
	if in.ActiveAccountID != "" {
		foundAct := false
		for _, a := range am.accounts {
			if a.ID == in.ActiveAccountID {
				foundAct = true
				break
			}
		}
		if !foundAct {
			am.mu.Unlock()
			return AccountGroup{}, fmt.Errorf("激活账号 %q 不存在", in.ActiveAccountID)
		}
	}
	for _, a := range am.accounts {
		for _, ak := range a.BoundKeys {
			if seenKeys[strings.TrimSpace(ak)] {
				am.mu.Unlock()
				return AccountGroup{}, fmt.Errorf("Key 已绑定至账号 %q，不可同时作为分组池 Key", a.ID)
			}
		}
	}
	foundIdx := -1
	for i, g := range am.groups {
		if g.ID == in.ID {
			foundIdx = i
			continue
		}
		for _, gk := range g.BoundKeys {
			if seenKeys[strings.TrimSpace(gk)] {
				am.mu.Unlock()
				return AccountGroup{}, fmt.Errorf("Key 已绑定至其他分组 %q，请勿重复绑定", g.ID)
			}
		}
	}
	if foundIdx >= 0 {
		am.groups[foundIdx] = in
	} else {
		am.groups = append(am.groups, in)
	}
	err := am.saveToDiskLocked()
	am.mu.Unlock()
	if err != nil {
		return AccountGroup{}, err
	}
	am.ResetRoutingWeights()
	InvalidatePeerDashboardCache()
	return in, nil
}

func (am *AccountManager) DeleteGroup(groupID string) error {
	am.ensureInitialized()
	gid := strings.ToLower(strings.TrimSpace(groupID))
	if gid == "" {
		return errors.New("分组 ID 不能为空")
	}
	am.mu.Lock()
	idx := -1
	for i, g := range am.groups {
		if g.ID == gid {
			idx = i
			break
		}
	}
	if idx < 0 {
		am.mu.Unlock()
		return errors.New("未找到该分组")
	}
	am.groups = append(am.groups[:idx], am.groups[idx+1:]...)
	for i := range am.accounts {
		if len(am.accounts[i].Groups) > 0 {
			filtered := make([]string, 0, len(am.accounts[i].Groups))
			for _, g := range am.accounts[i].Groups {
				if !strings.EqualFold(strings.TrimSpace(g), gid) {
					filtered = append(filtered, g)
				}
			}
			am.accounts[i].Groups = filtered
		}
	}
	err := am.saveToDiskLocked()
	am.mu.Unlock()
	if err == nil {
		am.ResetRoutingWeights()
		InvalidatePeerDashboardCache()
	}
	return err
}

// AccountActionRequest specifies a one-click operator action on an account or group pool.
type AccountActionRequest struct {
	AccountID      string `json:"accountId"`
	GroupID        string `json:"groupId,omitempty"`
	Action         string `json:"action"`
	SwitchStrategy bool   `json:"switchStrategy,omitempty"`
	DrainPrevious  bool   `json:"drainPrevious,omitempty"`
}

// PerformAccountAction executes one-click scheduling/switching/recovery operations (legacy wrapper).
func (am *AccountManager) PerformAccountAction(accountID, action string, switchToManual bool) (map[string]any, error) {
	return am.PerformAccountActionEx(AccountActionRequest{
		AccountID:      accountID,
		Action:         action,
		SwitchStrategy: switchToManual,
	})
}

// PerformAccountActionEx executes one-click scheduling, rotation, graceful drain, cooldown reset, and health probing.
func (am *AccountManager) PerformAccountActionEx(req AccountActionRequest) (map[string]any, error) {
	am.ensureInitialized()
	act := strings.ToLower(strings.TrimSpace(req.Action))
	id := strings.ToLower(strings.TrimSpace(req.AccountID))
	gid := strings.ToLower(strings.TrimSpace(req.GroupID))

	switch act {
	case "clear_cooldown":
		am.ClearAccountCooldown(id)
		InvalidatePeerDashboardCache()
		return map[string]any{"ok": true, "action": act, "accountId": id}, nil

	case "clear_sticky":
		am.stickyMu.Lock()
		removed := am.stickyLRU.clearForAccount(id)
		am.stickyMu.Unlock()
		return map[string]any{"ok": true, "action": act, "accountId": id, "removed": removed}, nil

	case "rotate_next", "drain_and_rotate":
		now := time.Now()
		accs := am.ListAccounts()
		if len(accs) == 0 {
			return nil, errors.New("当前无可用账号")
		}
		hardMap, coolMap := am.snapshotRoutingHealth(accs, DefaultModelID, now)
		hasTok := make(map[string]bool, len(accs))
		for _, a := range accs {
			hasTok[a.ID] = a.Primary || am.HasOAuthToken(a.ID)
		}

		am.mu.Lock()
		prevActive := am.routing.Normalized().ActiveAccountID
		grpIdx := -1
		if gid != "" {
			for i, g := range am.groups {
				if g.ID == gid {
					grpIdx = i
					if g.ActiveAccountID != "" {
						prevActive = g.ActiveAccountID
					}
					break
				}
			}
			if grpIdx < 0 {
				am.mu.Unlock()
				return nil, fmt.Errorf("未找到分组 %q", gid)
			}
		}
		if id != "" && id != "*" {
			prevActive = id
		}
		if prevActive == "" {
			for _, a := range am.accounts {
				if gid == "" || a.InGroup(gid) {
					prevActive = a.ID
					break
				}
			}
		}

		startIdx := -1
		for i, a := range am.accounts {
			if a.ID == prevActive {
				startIdx = i
				break
			}
		}

		pickCandidate := func(allowCooling bool) int {
			n := len(am.accounts)
			for step := 1; step <= n; step++ {
				candIdx := (startIdx + step + n) % n
				a := am.accounts[candIdx]
				if a.ID == prevActive && n > 1 {
					continue
				}
				if gid != "" && !a.InGroup(gid) {
					continue
				}
				if a.EffectiveSchedState() != SchedStateActive {
					continue
				}
				if !hasTok[a.ID] {
					continue
				}
				if hardMap[a.ID].blocked {
					continue
				}
				if !allowCooling && coolMap[a.ID].cooling {
					continue
				}
				return candIdx
			}
			return -1
		}

		chosenIdx := pickCandidate(false)
		if chosenIdx < 0 {
			chosenIdx = pickCandidate(true)
		}
		if chosenIdx < 0 {
			am.mu.Unlock()
			return nil, errors.New("未找到可轮转切换的健康已授权账号")
		}

		nextAcc := am.accounts[chosenIdx]
		drainedPrev := ""
		if (req.DrainPrevious || act == "drain_and_rotate") && prevActive != "" && prevActive != nextAcc.ID {
			for i := range am.accounts {
				if am.accounts[i].ID == prevActive && am.accounts[i].Enabled {
					am.accounts[i].SchedState = SchedStateDraining
					drainedPrev = prevActive
					break
				}
			}
		}

		activeStrategy := am.routing.Normalized().Strategy
		if grpIdx >= 0 {
			am.groups[grpIdx].ActiveAccountID = nextAcc.ID
			if req.SwitchStrategy || act == "drain_and_rotate" {
				am.groups[grpIdx].Strategy = RouteStrategyManualActive
			}
			if am.groups[grpIdx].Strategy != "" {
				activeStrategy = am.groups[grpIdx].Strategy
			}
		} else {
			norm := am.routing.Normalized()
			norm.ActiveAccountID = nextAcc.ID
			if req.SwitchStrategy || act == "drain_and_rotate" {
				norm.Strategy = RouteStrategyManualActive
			}
			am.routing = norm
			activeStrategy = norm.Strategy
		}
		err := am.saveToDiskLocked()
		am.mu.Unlock()
		if err != nil {
			return nil, err
		}
		am.ClearAccountCooldown(nextAcc.ID)
		if !nextAcc.Primary && nextAcc.Enabled && am.HasOAuthToken(nextAcc.ID) && !testing.Testing() {
			go func(aid string) {
				_ = am.EnsureAccountWorkerRunning(aid)
			}(nextAcc.ID)
		}
		InvalidatePeerDashboardCache()
		return map[string]any{
			"ok":                true,
			"action":            act,
			"previousAccountId": prevActive,
			"drainedAccountId":  drainedPrev,
			"activeAccountId":   nextAcc.ID,
			"groupId":           gid,
			"strategy":          activeStrategy,
		}, nil

	case "set_active":
		am.mu.Lock()
		idx := -1
		for i, a := range am.accounts {
			if a.ID == id {
				idx = i
				break
			}
		}
		if idx < 0 {
			am.mu.Unlock()
			return nil, fmt.Errorf("未找到账号 %q", id)
		}
		if am.accounts[idx].EffectiveSchedState() == SchedStateDisabled {
			am.accounts[idx].Enabled = true
			am.accounts[idx].SchedState = SchedStateActive
		} else if am.accounts[idx].EffectiveSchedState() == SchedStateDraining {
			am.accounts[idx].SchedState = SchedStateActive
		}
		prevActive := am.routing.Normalized().ActiveAccountID
		activeStrategy := ""
		if gid != "" {
			grpIdx := -1
			for i, g := range am.groups {
				if g.ID == gid {
					grpIdx = i
					prevActive = g.ActiveAccountID
					break
				}
			}
			if grpIdx < 0 {
				am.mu.Unlock()
				return nil, fmt.Errorf("未找到分组 %q", gid)
			}
			if !am.accounts[idx].InGroup(gid) {
				am.mu.Unlock()
				return nil, fmt.Errorf("账号 %q 不属于分组 %q，无法设为该组激活账号", id, gid)
			}
			am.groups[grpIdx].ActiveAccountID = id
			if req.SwitchStrategy {
				am.groups[grpIdx].Strategy = RouteStrategyManualActive
			}
			activeStrategy = am.groups[grpIdx].Strategy
		} else {
			norm := am.routing.Normalized()
			prevActive = norm.ActiveAccountID
			norm.ActiveAccountID = id
			if req.SwitchStrategy {
				norm.Strategy = RouteStrategyManualActive
			}
			am.routing = norm
			activeStrategy = norm.Strategy
		}
		drainedPrev := ""
		if req.DrainPrevious && prevActive != "" && prevActive != id {
			for i := range am.accounts {
				if am.accounts[i].ID == prevActive && am.accounts[i].Enabled {
					am.accounts[i].SchedState = SchedStateDraining
					drainedPrev = prevActive
					break
				}
			}
		}
		accCopy := am.accounts[idx]
		err := am.saveToDiskLocked()
		am.mu.Unlock()
		if err != nil {
			return nil, err
		}
		am.ClearAccountCooldown(id)
		if !accCopy.Primary && accCopy.Enabled && am.HasOAuthToken(id) && !testing.Testing() {
			go func(aid string) {
				_ = am.EnsureAccountWorkerRunning(aid)
			}(id)
		}
		InvalidatePeerDashboardCache()
		return map[string]any{
			"ok":               true,
			"action":           act,
			"accountId":        id,
			"activeAccountId":  id,
			"drainedAccountId": drainedPrev,
			"groupId":          gid,
			"strategy":         activeStrategy,
		}, nil

	case "drain":
		am.mu.Lock()
		idx := -1
		for i, a := range am.accounts {
			if a.ID == id {
				idx = i
				break
			}
		}
		if idx < 0 {
			am.mu.Unlock()
			return nil, fmt.Errorf("未找到账号 %q", id)
		}
		am.accounts[idx].Enabled = true
		am.accounts[idx].SchedState = SchedStateDraining
		err := am.saveToDiskLocked()
		am.mu.Unlock()
		if err != nil {
			return nil, err
		}
		InvalidatePeerDashboardCache()
		return map[string]any{"ok": true, "action": act, "accountId": id, "schedState": SchedStateDraining}, nil

	case "enable":
		opMu := am.accountOpLock(id)
		opMu.Lock()
		defer opMu.Unlock()
		am.mu.Lock()
		idx := -1
		for i, a := range am.accounts {
			if a.ID == id {
				idx = i
				break
			}
		}
		if idx < 0 {
			am.mu.Unlock()
			return nil, fmt.Errorf("未找到账号 %q", id)
		}
		am.accounts[idx].Enabled = true
		am.accounts[idx].SchedState = SchedStateActive
		accCopy := am.accounts[idx]
		err := am.saveToDiskLocked()
		am.mu.Unlock()
		if err != nil {
			return nil, err
		}
		am.ClearAccountCooldown(id)
		if !accCopy.Primary && am.HasOAuthToken(id) && !testing.Testing() {
			go func(aid string) {
				_ = am.EnsureAccountWorkerRunning(aid)
			}(id)
		}
		InvalidatePeerDashboardCache()
		return map[string]any{"ok": true, "action": act, "accountId": id, "schedState": SchedStateActive}, nil

	case "disable":
		opMu := am.accountOpLock(id)
		opMu.Lock()
		defer opMu.Unlock()
		hasTok := am.snapshotAuthMap()
		am.mu.Lock()
		idx := -1
		for i, a := range am.accounts {
			if a.ID == id {
				idx = i
				break
			}
		}
		if idx < 0 {
			am.mu.Unlock()
			return nil, fmt.Errorf("未找到账号 %q", id)
		}
		isPrimary := am.accounts[idx].Primary
		am.accounts[idx].Enabled = false
		am.accounts[idx].SchedState = SchedStateDisabled
		am.advanceActiveOnDisableLocked(id, hasTok)
		nextActive := am.routing.ActiveAccountID
		err := am.saveToDiskLocked()
		am.mu.Unlock()
		if err != nil {
			return nil, err
		}
		am.ClearStickyForAccount(id)
		am.ResetRoutingWeights()
		if isPrimary {
			if GlobalEngine != nil {
				GlobalEngine.StopEngine()
			}
		} else {
			am.StopAccountWorker(id)
		}
		InvalidatePeerDashboardCache()
		return map[string]any{
			"ok":              true,
			"action":          act,
			"accountId":       id,
			"schedState":      SchedStateDisabled,
			"activeAccountId": nextActive,
		}, nil

	case "health_check":
		acc, ok := am.GetAccount(id)
		if !ok {
			return nil, fmt.Errorf("未找到账号 %q", id)
		}
		if !acc.Primary && !am.HasOAuthToken(id) {
			return nil, fmt.Errorf("账号 %q 尚未完成 OAuth 授权", id)
		}
		proxy, pErr := am.ResolveAccountProxyChecked(acc)
		if pErr != nil {
			return nil, pErr
		}
		t0 := time.Now()
		tier, cnt := am.probeAccountModelsTier(acc)
		ms := int(time.Since(t0).Milliseconds())
		if !testing.Testing() && cnt == 0 && fileExists(PatchedAgyBin) {
			homeDir := am.AccountHomeDir(acc.ID)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, PatchedAgyBin, "models")
			cmd.Dir = homeDir
			cmd.Env = am.buildAccountSubprocessEnv(homeDir, proxy)
			if out, err := cmd.CombinedOutput(); err != nil {
				msg := redactAccountErrorString(truncateRunes(strings.TrimSpace(string(out)), 180), proxy)
				if msg == "" {
					msg = redactAccountErrorString(err.Error(), proxy)
				}
				if isToSOrBanError(http.StatusForbidden, "probe_error", msg) {
					am.RecordAccountOutcome(acc.ID, DefaultModelID, http.StatusForbidden, "tos_ban_error", "", msg)
				} else if isAccountAuthFailure(http.StatusUnauthorized, "", msg) || isAuthErrorMessage(msg) {
					am.RecordAccountOutcome(acc.ID, DefaultModelID, http.StatusUnauthorized, "authentication_error", "", msg)
				} else {
					am.RecordAccountOutcome(acc.ID, DefaultModelID, http.StatusBadGateway, "probe_error", "", msg)
				}
				return nil, fmt.Errorf("健康探活失败: %s", msg)
			}
		}
		am.ClearAccountCooldown(acc.ID)
		if !acc.Primary && acc.Enabled && !testing.Testing() {
			_ = am.EnsureAccountWorkerRunning(acc.ID)
		}
		InvalidatePeerDashboardCache()
		return map[string]any{
			"ok":          true,
			"action":      act,
			"accountId":   acc.ID,
			"tier":        tier,
			"modelsCount": cnt,
			"latencyMs":   ms,
		}, nil

	default:
		return nil, fmt.Errorf("未知账号操作 %q", req.Action)
	}
}

// snapshotAuthMap captures OAuth token presence for all accounts without holding am.mu.
func (am *AccountManager) snapshotAuthMap() map[string]bool {
	accs := am.ListAccounts()
	out := make(map[string]bool, len(accs))
	for _, a := range accs {
		out[a.ID] = a.Primary || am.HasOAuthToken(a.ID)
	}
	return out
}

// advanceActiveOnDisableLocked clears or rotates global and group ActiveAccountID away from a disabled account.
// Caller must hold am.mu.Lock() and pass a pre-snapshotted hasTok map to avoid lock re-entrancy.
func (am *AccountManager) advanceActiveOnDisableLocked(disabledID string, hasTok map[string]bool) {
	pickNext := func(groupID string) string {
		for _, a := range am.accounts {
			if a.ID == disabledID || a.EffectiveSchedState() != SchedStateActive {
				continue
			}
			if groupID != "" && !a.InGroup(groupID) {
				continue
			}
			if a.Primary || hasTok[a.ID] {
				return a.ID
			}
		}
		return ""
	}
	if am.routing.ActiveAccountID == disabledID {
		am.routing.ActiveAccountID = pickNext("")
	}
	for i := range am.groups {
		if am.groups[i].ActiveAccountID == disabledID {
			am.groups[i].ActiveAccountID = pickNext(am.groups[i].ID)
		}
	}
}

// --- Candidate Sorting & Strategy Picker ---

type candidateScore struct {
	acc       AccountProfile
	priority  int
	inFlight  int64
	maxConc   int
	remQuota  float64
	configOrd int
}

// quotaBucket50 discretizes remaining quota [0..1] into 50 buckets (2% steps) so comparators satisfy strict weak ordering.
func quotaBucket50(rem float64) int {
	if rem <= 0 {
		return 0
	}
	if rem >= 1 {
		return 50
	}
	return int(math.Round(rem * 50))
}

func sortCandidatesFillFirst(scored []candidateScore) []candidateScore {
	cp := append([]candidateScore(nil), scored...)
	sort.SliceStable(cp, func(i, j int) bool {
		if cp[i].priority != cp[j].priority {
			return cp[i].priority < cp[j].priority
		}
		qi, qj := quotaBucket50(cp[i].remQuota), quotaBucket50(cp[j].remQuota)
		if qi != qj {
			return qi > qj
		}
		if cp[i].inFlight != cp[j].inFlight {
			return cp[i].inFlight < cp[j].inFlight
		}
		return cp[i].configOrd < cp[j].configOrd
	})
	return cp
}

func selectAccountByStrategy(
	strategy string,
	activeAccountID string,
	scopeKey string,
	canonicalModelID string,
	scored []candidateScore,
	rrCounters map[string]uint64,
	swrrMap map[string]map[string]*swrrEntry,
) (AccountProfile, string) {
	normStrat := normalizeRouteStrategy(strategy)
	rrKey := scopeKey + "|" + canonicalModelID

	switch normStrat {
	case RouteStrategyManualActive:
		if activeAccountID != "" {
			for _, c := range scored {
				if c.acc.ID == activeAccountID {
					return c.acc, "pool-manual-active"
				}
			}
		}
		cp := sortCandidatesFillFirst(scored)
		return cp[0].acc, "pool-manual-failover"

	case RouteStrategyFillFirst:
		cp := sortCandidatesFillFirst(scored)
		return cp[0].acc, "pool-fill-first"

	case RouteStrategyWeightedRR:
		accs := make([]AccountProfile, len(scored))
		for i, c := range scored {
			accs[i] = c.acc
		}
		if swrrMap[rrKey] == nil {
			swrrMap[rrKey] = make(map[string]*swrrEntry, len(accs))
		}
		chosen := pickSmoothWeighted(swrrMap[rrKey], accs)
		return chosen, "pool-weighted-rr"

	case RouteStrategyLeastLoad:
		if len(scored) == 1 {
			return scored[0].acc, "pool-least-load"
		}
		best := scored[0]
		bestScore := float64(best.inFlight)/float64(maxInt(best.maxConc, 1)) - 0.35*best.remQuota
		for _, c := range scored[1:] {
			sc := float64(c.inFlight)/float64(maxInt(c.maxConc, 1)) - 0.35*c.remQuota
			if sc < bestScore-1e-6 || (math.Abs(sc-bestScore) <= 1e-6 && (c.inFlight < best.inFlight || (c.inFlight == best.inFlight && (c.priority < best.priority || (c.priority == best.priority && c.configOrd < best.configOrd))))) {
				best = c
				bestScore = sc
			}
		}
		return best.acc, "pool-least-load"

	default: // RouteStrategyRoundRobin
		rr := rrCounters[rrKey]
		chosen := scored[rr%uint64(len(scored))].acc
		rrCounters[rrKey] = rr + 1
		return chosen, "pool-rr"
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// --- Child Worker Error & Streaming SSE Outcome Classification ---

func classifyChildErrorBody(statusCode int, bodyBytes []byte) (effectiveStatus int, errType string, errMsg string) {
	effectiveStatus = statusCode
	trimmed := bytes.TrimSpace(bodyBytes)
	if len(trimmed) == 0 {
		return effectiveStatus, "", ""
	}
	var parsed struct {
		Error struct {
			Type    string `json:"type"`
			Code    any    `json:"code"`
			Status  string `json:"status"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(trimmed, &parsed) == nil {
		errType = strings.TrimSpace(parsed.Error.Type)
		if errType == "" {
			errType = strings.TrimSpace(parsed.Error.Status)
		}
		if errType == "" {
			if s, ok := parsed.Error.Code.(string); ok {
				errType = strings.TrimSpace(s)
			}
		}
		errMsg = strings.TrimSpace(parsed.Error.Message)
	}
	if errMsg == "" {
		errMsg = string(clipBytes(trimmed, 240))
	}
	low := strings.ToLower(errType + " " + errMsg)
	if effectiveStatus == http.StatusTooManyRequests && (strings.Contains(low, "overloaded") || strings.Contains(low, "529")) {
		effectiveStatus = 529
	}
	return effectiveStatus, errType, errMsg
}

func shouldRetryChildPreStreamFailure(effectiveStatus int, errType, errMsg string) bool {
	switch effectiveStatus {
	case http.StatusTooManyRequests, 529, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout, http.StatusInternalServerError:
		return true
	case http.StatusUnauthorized, http.StatusForbidden:
		return isAccountAuthFailure(effectiveStatus, errType, errMsg)
	default:
		return false
	}
}

func classifyStructuredStreamError(errType, errStatus, errMsg string, errCode int) (failed bool, statusCode int, outType, outMsg string) {
	low := strings.ToLower(errType + " " + errStatus + " " + errMsg)
	cleanMsg := truncateRunes(strings.TrimSpace(errMsg), 200)
	if cleanMsg == "" {
		cleanMsg = strings.TrimSpace(errType)
	}
	switch {
	case strings.Contains(low, "tos_circuit_breaker") || strings.Contains(low, "tos circuit"):
		return true, http.StatusServiceUnavailable, "tos_circuit_breaker", cleanMsg
	case errCode == 529 || strings.Contains(low, "overloaded_error") || strings.Contains(low, "overloaded") || strings.Contains(low, "529"):
		return true, 529, "overloaded_error", cleanMsg
	case errCode == http.StatusTooManyRequests || strings.Contains(low, "rate_limit_error") || strings.Contains(low, "resource_exhausted") || strings.Contains(low, "too many requests") || strings.Contains(low, "quota"):
		return true, http.StatusTooManyRequests, "rate_limit_error", cleanMsg
	case errCode == http.StatusUnauthorized || strings.Contains(low, "authentication_error") || strings.Contains(low, "unauthenticated") || strings.Contains(low, "3501") || strings.Contains(low, "invalid_grant"):
		return true, http.StatusUnauthorized, "authentication_error", cleanMsg
	case errCode >= 500 || strings.Contains(low, "unavailable") || strings.Contains(low, "internal_error"):
		sc := errCode
		if sc < 500 {
			sc = http.StatusServiceUnavailable
		}
		et := strings.TrimSpace(errType)
		if et == "" {
			et = "api_error"
		}
		return true, sc, et, cleanMsg
	default:
		return false, http.StatusOK, "", ""
	}
}

func extractJSONErrorEnvelope(raw []byte) (isErr bool, errType, errStatus, errMsg string, errCode int) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) < 2 || trimmed[0] != '{' || trimmed[len(trimmed)-1] != '}' {
		return false, "", "", "", 0
	}
	var env map[string]json.RawMessage
	if json.Unmarshal(trimmed, &env) != nil || len(env) == 0 {
		return false, "", "", "", 0
	}
	// Reject normal generation chunks that carry assistant/model content fields.
	if _, hasChoices := env["choices"]; hasChoices {
		return false, "", "", "", 0
	}
	if _, hasCandidates := env["candidates"]; hasCandidates {
		return false, "", "", "", 0
	}
	if _, hasDelta := env["delta"]; hasDelta {
		return false, "", "", "", 0
	}
	topType := strings.ToLower(rawJSONString(env["type"]))

	parseErrObj := func(rawErr json.RawMessage) (bool, string, string, string, int) {
		rawErr = bytes.TrimSpace(rawErr)
		if len(rawErr) == 0 || bytes.Equal(rawErr, []byte("null")) {
			return false, "", "", "", 0
		}
		if rawErr[0] == '"' {
			s := rawJSONString(rawErr)
			return s != "", "error", "", s, 0
		}
		var eo struct {
			Type    string `json:"type"`
			Status  string `json:"status"`
			Message string `json:"message"`
			Code    any    `json:"code"`
		}
		if json.Unmarshal(rawErr, &eo) != nil {
			return false, "", "", "", 0
		}
		codeInt := 0
		switch v := eo.Code.(type) {
		case float64:
			codeInt = int(v)
		case string:
			if eo.Type == "" {
				eo.Type = v
			}
		}
		if eo.Type == "" && eo.Status == "" && eo.Message == "" && codeInt == 0 {
			return false, "", "", "", 0
		}
		return true, eo.Type, eo.Status, eo.Message, codeInt
	}

	if rawErr, ok := env["error"]; ok {
		if okE, et, es, em, ec := parseErrObj(rawErr); okE {
			return true, et, es, em, ec
		}
	}
	if topType == "response.failed" || rawJSONString(env["status"]) == "failed" {
		if rawResp, ok := env["response"]; ok {
			var respMap map[string]json.RawMessage
			if json.Unmarshal(rawResp, &respMap) == nil {
				if okE, et, es, em, ec := parseErrObj(respMap["error"]); okE {
					return true, et, es, em, ec
				}
			}
		}
	}
	return false, "", "", "", 0
}

// inspectStreamTailForUpstreamError parses SSE frames or trailing JSON error envelopes in the rolling 2KB tail
// so mid-stream upstream 429/529/ToS/auth errors are recorded accurately WITHOUT false positives on model text.
func inspectStreamTailForUpstreamError(tail []byte) (failed bool, statusCode int, errType, errMsg string) {
	if len(tail) == 0 {
		return false, http.StatusOK, "", ""
	}
	if !bytes.Contains(tail, []byte("error")) && !bytes.Contains(tail, []byte("failed")) {
		return false, http.StatusOK, "", ""
	}

	norm := bytes.ReplaceAll(tail, []byte("\r\n"), []byte("\n"))
	blocks := bytes.Split(norm, []byte("\n\n"))
	for i := len(blocks) - 1; i >= 0; i-- {
		blk := bytes.TrimSpace(blocks[i])
		if len(blk) == 0 {
			continue
		}
		eventName := ""
		var dataLines [][]byte
		hasSSEField := false
		for _, line := range bytes.Split(blk, []byte("\n")) {
			line = bytes.TrimSpace(line)
			if bytes.HasPrefix(line, []byte("event:")) {
				hasSSEField = true
				eventName = strings.ToLower(strings.TrimSpace(string(bytes.TrimPrefix(line, []byte("event:")))))
			} else if bytes.HasPrefix(line, []byte("data:")) {
				hasSSEField = true
				d := bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
				if !bytes.Equal(d, []byte("[DONE]")) && len(d) > 0 {
					dataLines = append(dataLines, d)
				}
			}
		}
		if hasSSEField {
			if len(dataLines) == 0 {
				continue
			}
			joined := bytes.Join(dataLines, []byte("\n"))
			if isErr, et, es, em, ec := extractJSONErrorEnvelope(joined); isErr {
				if okC, sc, ot, om := classifyStructuredStreamError(et, es, em, ec); okC {
					return true, sc, ot, om
				}
			}
			if eventName == "error" || eventName == "response.failed" {
				if okC, sc, ot, om := classifyStructuredStreamError(eventName, "", string(joined), 0); okC {
					return true, sc, ot, om
				}
			}
			continue
		}
		// Non-SSE chunked JSON error object (e.g. Gemini JSON array stream element or trailing JSON error)
		cand := bytes.Trim(blk, "[], \t\n")
		if isErr, et, es, em, ec := extractJSONErrorEnvelope(cand); isErr {
			if okC, sc, ot, om := classifyStructuredStreamError(et, es, em, ec); okC {
				return true, sc, ot, om
			}
		}
	}
	return false, http.StatusOK, "", ""
}

var hopByHopProxyHeaders = []string{
	"Connection",
	"Keep-Alive",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"Te",
	"Trailer",
	"Transfer-Encoding",
	"Upgrade",
}

func copyCleanProxyResponseHeaders(dst, src http.Header) {
	skip := make(map[string]bool, len(hopByHopProxyHeaders)+4)
	for _, h := range hopByHopProxyHeaders {
		skip[http.CanonicalHeaderKey(h)] = true
	}
	for _, connVal := range src.Values("Connection") {
		for _, tok := range strings.Split(connVal, ",") {
			if t := strings.TrimSpace(tok); t != "" {
				skip[http.CanonicalHeaderKey(t)] = true
			}
		}
	}
	for k, vals := range src {
		if skip[http.CanonicalHeaderKey(k)] {
			continue
		}
		dst.Del(k)
		for _, v := range vals {
			dst.Add(k, v)
		}
	}
}

var proxyStreamBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 32*1024)
		return &b
	},
}

func streamProxyBodyAndRecordOutcome(w http.ResponseWriter, body io.ReadCloser, trackSSE bool) (streamComplete bool, midStreamStatus int, midStreamType, midStreamMsg string) {
	defer body.Close()
	flusher, _ := w.(http.Flusher)
	bufPtr := proxyStreamBufPool.Get().(*[]byte)
	buf := *bufPtr
	defer proxyStreamBufPool.Put(bufPtr)
	var tail []byte
	const maxTail = 2048

	for {
		n, rErr := body.Read(buf)
		if n > 0 {
			chunk := buf[:n]
			if trackSSE {
				if len(chunk) >= maxTail {
					tail = append(tail[:0], chunk[len(chunk)-maxTail:]...)
				} else {
					if len(tail)+len(chunk) > maxTail {
						keep := maxTail - len(chunk)
						tail = append(tail[:0], tail[len(tail)-keep:]...)
					}
					tail = append(tail, chunk...)
				}
			}
			if _, wErr := w.Write(chunk); wErr != nil {
				return false, 0, "", ""
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if rErr != nil {
			if errors.Is(rErr, io.EOF) {
				if trackSSE {
					if failed, sc, et, em := inspectStreamTailForUpstreamError(tail); failed {
						return true, sc, et, em
					}
				}
				return true, http.StatusOK, "", ""
			}
			return false, 0, "", ""
		}
	}
}

// --- Primary Route Outcome ResponseWriter Wrapper (Atomic, Race-Free) ---

type routeOutcomeWriter struct {
	http.ResponseWriter
	statusCode atomic.Int32
	wroteHdr   atomic.Bool
}

func (rw *routeOutcomeWriter) WriteHeader(code int) {
	if rw.wroteHdr.CompareAndSwap(false, true) {
		rw.statusCode.Store(int32(code))
	}
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *routeOutcomeWriter) Write(b []byte) (int, error) {
	if rw.wroteHdr.CompareAndSwap(false, true) {
		rw.statusCode.Store(http.StatusOK)
	}
	return rw.ResponseWriter.Write(b)
}

func (rw *routeOutcomeWriter) Flush() {
	if f, ok := rw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (rw *routeOutcomeWriter) Unwrap() http.ResponseWriter {
	return rw.ResponseWriter
}

type routedAccountCtxKeyType struct{}

var routedAccountCtxKey = routedAccountCtxKeyType{}

type routedAccountMeta struct {
	accountID       string
	modelID         string
	outcomeRecorded atomic.Bool
	skipHealth      atomic.Bool
}

func withRoutedAccountMeta(r *http.Request, accountID, modelID string, skipHealth bool) *http.Request {
	if r == nil || accountID == "" {
		return r
	}
	m := &routedAccountMeta{
		accountID: accountID,
		modelID:   modelID,
	}
	if skipHealth {
		m.skipHealth.Store(true)
	}
	return r.WithContext(context.WithValue(r.Context(), routedAccountCtxKey, m))
}

func routedAccountMetaFromRequest(r *http.Request) (*routedAccountMeta, bool) {
	if r == nil {
		return nil, false
	}
	m, ok := r.Context().Value(routedAccountCtxKey).(*routedAccountMeta)
	return m, ok && m != nil && m.accountID != ""
}

func markRoutedSkipHealth(r *http.Request) {
	if meta, ok := routedAccountMetaFromRequest(r); ok {
		meta.skipHealth.Store(true)
	}
}

func recordPrimaryDirectOutcome(r *http.Request, statusCode int, errType, retryAfterHdr, errMsg string) {
	if meta, ok := routedAccountMetaFromRequest(r); ok && !meta.skipHealth.Load() {
		if meta.outcomeRecorded.CompareAndSwap(false, true) {
			GlobalAccountManager.RecordAccountOutcome(meta.accountID, meta.modelID, statusCode, errType, retryAfterHdr, errMsg)
		}
	}
}
