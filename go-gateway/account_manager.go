package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gwcfg "agy-acp-gateway-go/internal/config"
)

// ProxyPresetOption describes a zero-secret selectable proxy egress option in the web UI.
type ProxyPresetOption struct {
	ID          string `json:"id"`
	Label       string `json:"label"`
	Region      string `json:"region"`
	Description string `json:"description"`
}

// BuiltinProxyPresets contains only generic egress options safe for open-source distribution.
// Operators can provide additional named proxy presets via AGY_PROXY_PRESETS_FILE (JSON array).
var BuiltinProxyPresets = []ProxyPresetOption{
	{ID: "direct", Label: "宿主机直连 (Direct)", Region: "DIRECT", Description: "不走代理，使用宿主机原生出口 IP"},
	{ID: "custom", Label: "自定义 HTTP/SOCKS5 代理", Region: "CUSTOM", Description: "手动指定代理地址 (支持 http://, https://, socks5://)"},
}

var (
	validAccountIDRe   = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,31}$`)
	validProxyPresetRe = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)
	oauthURLRe         = regexp.MustCompile(`https://accounts\.google\.com/o/oauth2/[^\s"']+`)
)

type preReadBodyCtxKeyType struct{}

var preReadBodyCtxKey = preReadBodyCtxKeyType{}

func withPreReadBody(r *http.Request, raw []byte) *http.Request {
	if r == nil {
		return r
	}
	return r.WithContext(context.WithValue(r.Context(), preReadBodyCtxKey, raw))
}

func preReadBodyFromContext(r *http.Request) ([]byte, bool) {
	if r == nil {
		return nil, false
	}
	b, ok := r.Context().Value(preReadBodyCtxKey).([]byte)
	return b, ok
}

// LoadProxyPresets returns generic presets merged with optional external presets from AGY_PROXY_PRESETS_FILE.
func LoadProxyPresets() []ProxyPresetOption {
	direct := BuiltinProxyPresets[0]
	custom := BuiltinProxyPresets[1]
	out := []ProxyPresetOption{direct}
	seen := map[string]bool{"direct": true, "custom": true}

	if path := gwcfg.Current().ProxyPresetsFile; path != "" {
		if b, err := os.ReadFile(path); err == nil {
			var extra []ProxyPresetOption
			if json.Unmarshal(b, &extra) == nil {
				for _, p := range extra {
					p.ID = strings.ToLower(strings.TrimSpace(p.ID))
					if p.ID == "" || seen[p.ID] || !validProxyPresetRe.MatchString(p.ID) {
						continue
					}
					seen[p.ID] = true
					if strings.TrimSpace(p.Label) == "" {
						p.Label = p.ID
					}
					out = append(out, p)
				}
			}
		}
	}
	out = append(out, custom)
	return out
}

func validateProxyPreset(preset string) error {
	p := strings.ToLower(strings.TrimSpace(preset))
	if p == "" || p == "direct" || p == "custom" {
		return nil
	}
	if !validProxyPresetRe.MatchString(p) {
		return errors.New("代理预设 ID 仅允许小写字母、数字、连字符或下划线 (1-64 位)")
	}
	return nil
}

func validateCustomProxyURL(raw string) error {
	s := strings.TrimSpace(raw)
	if s == "" {
		return nil
	}
	if strings.ContainsAny(s, "\r\n\t ") {
		return errors.New("自定义代理 URL 不可包含空白或换行符")
	}
	u, err := url.Parse(s)
	if err != nil || u.Host == "" {
		return errors.New("自定义代理 URL 格式无效")
	}
	switch strings.ToLower(u.Scheme) {
	case "http", "https", "socks5", "socks5h":
		return nil
	default:
		return errors.New("自定义代理协议仅支持 http / https / socks5 / socks5h")
	}
}

func maskProxyURL(raw string) string {
	s := strings.TrimSpace(raw)
	if s == "" {
		return ""
	}
	u, err := url.Parse(s)
	if err != nil {
		return "***"
	}
	if u.User != nil {
		if _, hasPw := u.User.Password(); hasPw {
			u.User = url.UserPassword(u.User.Username(), "***")
		} else if u.User.Username() != "" {
			u.User = url.User("***")
		}
	}
	return u.String()
}

const (
	AccountRolePrimary = "primary"
	AccountRoleStandby = "standby"
)

func normalizeAccountRole(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case AccountRoleStandby:
		return AccountRoleStandby
	default:
		return AccountRolePrimary
	}
}

// AccountProfile persists configuration for one isolated account environment.
type AccountProfile struct {
	ID             string   `json:"id"`
	Label          string   `json:"label"`
	Primary        bool     `json:"primary"`
	Enabled        bool     `json:"enabled"`
	Role           string   `json:"role,omitempty"`
	ProxyPreset    string   `json:"proxyPreset"`
	CustomProxy    string   `json:"customProxy,omitempty"`
	MaxLiveWorkers int      `json:"maxLiveWorkers"`
	BoundKeys      []string `json:"boundKeys"`
	AllowedModels  []string `json:"allowedModels"`
	DefaultModels  []string `json:"defaultModels,omitempty"`
	SchedState     string   `json:"schedState,omitempty"`
	Priority       int      `json:"priority,omitempty"`
	Weight         int      `json:"weight,omitempty"`
	MaxConcurrency int      `json:"maxConcurrency,omitempty"`
	Groups         []string `json:"groups,omitempty"`
	Notes          string   `json:"notes,omitempty"`
	PlanTierHint   string   `json:"planTierHint,omitempty"`
}

func (a AccountProfile) EffectiveRole() string {
	return normalizeAccountRole(a.Role)
}

func (a AccountProfile) IsStandby() bool {
	return normalizeAccountRole(a.Role) == AccountRoleStandby
}

func (a AccountProfile) AllowsModel(canonicalModelID string) bool {
	if canonicalModelID == "" {
		return true
	}
	for _, m := range a.AllowedModels {
		if m == "*" || strings.EqualFold(strings.TrimSpace(m), canonicalModelID) {
			return true
		}
	}
	return false
}

func (a AccountProfile) AllowsAllCanonicalModels() bool {
	for _, m := range a.AllowedModels {
		if m == "*" {
			return true
		}
	}
	for _, m := range AgyModels {
		if !a.AllowsModel(m.ID) {
			return false
		}
	}
	return len(AgyModels) > 0
}

func normalizeCanonicalModelList(in []string) []string {
	if len(in) == 0 {
		return []string{}
	}
	seen := make(map[string]bool, len(in))
	out := make([]string, 0, len(in))
	for _, raw := range in {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		if raw == "*" {
			for _, m := range AgyModels {
				if !seen[m.ID] {
					seen[m.ID] = true
					out = append(out, m.ID)
				}
			}
			continue
		}
		sel := ResolveRequestedModel(raw, nil)
		if !sel.Explicit || sel.Model == nil || sel.Model.ID == "" {
			continue
		}
		cid := sel.Model.ID
		if !seen[cid] {
			seen[cid] = true
			out = append(out, cid)
		}
	}
	return out
}

type accountsFileSchema struct {
	Version  int               `json:"version"`
	Routing  AccountPoolConfig `json:"routing,omitempty"`
	Groups   []AccountGroup    `json:"groups,omitempty"`
	Accounts []AccountProfile  `json:"accounts"`
}

type boundedTailBuffer struct {
	mu  sync.Mutex
	buf []byte
	max int
}

func (b *boundedTailBuffer) Write(p []byte) (int, error) {
	n := len(p)
	if b == nil || b.max <= 0 || n == 0 {
		return n, nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(p) >= b.max {
		b.buf = append(b.buf[:0], p[len(p)-b.max:]...)
		return n, nil
	}
	if len(b.buf)+len(p) > b.max {
		drop := len(b.buf) + len(p) - b.max
		copy(b.buf, b.buf[drop:])
		b.buf = b.buf[:len(b.buf)-drop]
	}
	b.buf = append(b.buf, p...)
	return n, nil
}

func (b *boundedTailBuffer) String() string {
	if b == nil {
		return ""
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return strings.TrimSpace(string(b.buf))
}

type workerCrashState struct {
	failures     int
	backoffUntil time.Time
}

type accountWorkerProc struct {
	id              string
	gwPort          int
	dashPort        int
	cmd             *exec.Cmd
	proxy           *httputil.ReverseProxy
	transport       *http.Transport
	trCloseOnce     sync.Once
	done            chan struct{}
	readyCh         chan struct{}
	readyOnce       sync.Once
	readyErr        error
	healthyAt       time.Time
	intentionalStop bool
	stopped         bool
}

func (p *accountWorkerProc) closeTransport() {
	if p == nil || p.transport == nil {
		return
	}
	p.trCloseOnce.Do(func() {
		p.transport.CloseIdleConnections()
	})
}

func (p *accountWorkerProc) finishReady(err error) {
	if p == nil {
		return
	}
	p.readyOnce.Do(func() {
		p.readyErr = err
		if p.readyCh != nil {
			close(p.readyCh)
		}
	})
}

type pendingOAuthSession struct {
	accountID string
	authURL   string
	cmd       *exec.Cmd
	stdin     io.WriteCloser
	stdinOnce sync.Once
	output    *bytes.Buffer
	outMu     sync.Mutex
	tokenPath string
	bakPath   string
	startedAt time.Time
	done      chan struct{}
	doneCh    chan error
}

func (s *pendingOAuthSession) closeStdin() {
	if s != nil && s.stdin != nil {
		s.stdinOnce.Do(func() {
			_ = s.stdin.Close()
		})
	}
}

type tokenCacheEntry struct {
	exists    bool
	checkedAt time.Time
}

// AccountManager supervises isolated per-account directories, OAuth handshakes, child worker
// processes, strict per-account model allowlists, account groups, and strategy-aware sticky routing.
type AccountManager struct {
	mu                     sync.RWMutex
	filePath               string
	baseDataDir            string
	routing                AccountPoolConfig
	groups                 []AccountGroup
	accounts               []AccountProfile
	workers                map[string]*accountWorkerProc
	crashStates            map[string]workerCrashState
	opMu                   sync.Mutex
	opLocks                map[string]*sync.Mutex
	oauthMu                sync.Mutex
	oauthActive            map[string]*pendingOAuthSession
	stickyMu               sync.Mutex
	stickyLRU              *stickySessionLRU
	stickyLastPrune        time.Time
	rrCounters             map[string]uint64
	swrrStates             map[string]map[string]*swrrEntry
	healthMu               sync.RWMutex
	healthStates           map[string]*accountHealthRuntime
	inflightMap            sync.Map
	tokCacheMu             sync.RWMutex
	tokCache               map[string]tokenCacheEntry
	warmupMu               sync.Mutex
	warmupAttempt          map[string]time.Time
	primaryMaxLiveWorkers  atomic.Int64
	primarySlotMu          sync.RWMutex
	primaryOccupiedSlotsFn func() int
	primaryCancelParkedFn  func(sessionKey string, callIDs []string, reason string) int
	onMonitorTripHopCancel func(accountID, sessionKey string, callIDs []string, reason string)
}

func (am *AccountManager) SetPrimaryOccupiedSlotsFn(fn func() int) {
	if am == nil {
		return
	}
	am.primarySlotMu.Lock()
	am.primaryOccupiedSlotsFn = fn
	am.primarySlotMu.Unlock()
}

func (am *AccountManager) SetPrimaryCancelParkedFn(fn func(sessionKey string, callIDs []string, reason string) int) {
	if am == nil {
		return
	}
	am.primarySlotMu.Lock()
	am.primaryCancelParkedFn = fn
	am.primarySlotMu.Unlock()
}

func (am *AccountManager) getPrimaryCancelParkedFn() func(sessionKey string, callIDs []string, reason string) int {
	if am == nil {
		return nil
	}
	am.primarySlotMu.RLock()
	fn := am.primaryCancelParkedFn
	am.primarySlotMu.RUnlock()
	return fn
}

func (am *AccountManager) setMonitorTripHopCancelHookForTest(fn func(accountID, sessionKey string, callIDs []string, reason string)) {
	if am == nil {
		return
	}
	am.primarySlotMu.Lock()
	am.onMonitorTripHopCancel = fn
	am.primarySlotMu.Unlock()
}

func (am *AccountManager) getMonitorTripHopCancelHook() func(accountID, sessionKey string, callIDs []string, reason string) {
	if am == nil {
		return nil
	}
	am.primarySlotMu.RLock()
	fn := am.onMonitorTripHopCancel
	am.primarySlotMu.RUnlock()
	return fn
}

func (am *AccountManager) getPrimaryOccupiedSlotsFn() func() int {
	if am == nil {
		return nil
	}
	am.primarySlotMu.RLock()
	fn := am.primaryOccupiedSlotsFn
	am.primarySlotMu.RUnlock()
	return fn
}

func (am *AccountManager) isPrimaryAccountID(id string) bool {
	if am == nil || id == "" {
		return false
	}
	am.mu.RLock()
	defer am.mu.RUnlock()
	for _, a := range am.accounts {
		if a.Primary && a.ID == id {
			return true
		}
	}
	return false
}

var GlobalAccountManager = NewAccountManager()

func isChildAccountWorker() bool {
	return gwcfg.Current().ChildAccountWorker
}

func NewAccountManager() *AccountManager {
	return &AccountManager{
		routing:       (AccountPoolConfig{}).Normalized(),
		workers:       make(map[string]*accountWorkerProc),
		crashStates:   make(map[string]workerCrashState),
		opLocks:       make(map[string]*sync.Mutex),
		oauthActive:   make(map[string]*pendingOAuthSession),
		stickyLRU:     newStickySessionLRU(),
		rrCounters:    make(map[string]uint64),
		swrrStates:    make(map[string]map[string]*swrrEntry),
		healthStates:  make(map[string]*accountHealthRuntime),
		tokCache:      make(map[string]tokenCacheEntry),
		warmupAttempt: make(map[string]time.Time),
	}
}

func (am *AccountManager) accountOpLock(accountID string) *sync.Mutex {
	id := strings.ToLower(strings.TrimSpace(accountID))
	am.opMu.Lock()
	defer am.opMu.Unlock()
	if am.opLocks == nil {
		am.opLocks = make(map[string]*sync.Mutex)
	}
	m, ok := am.opLocks[id]
	if !ok {
		m = &sync.Mutex{}
		am.opLocks[id] = m
	}
	return m
}

func resolveAccountsFilePath() (filePath, baseDataDir string) {
	if v := gwcfg.Current().AccountsFile; v != "" {
		return v, filepath.Dir(v)
	}
	dbPath := strings.TrimSpace(GatewayStateDB)
	if dbPath == "" {
		dbPath = gwcfg.Current().GatewayStateDB
	}
	baseDir := filepath.Dir(dbPath)
	if baseDir == "" || baseDir == "." {
		baseDir = "."
	}
	return filepath.Join(baseDir, "accounts.json"), baseDir
}

func defaultSeededAccounts() []AccountProfile {
	snap := gwcfg.Current()
	primID := strings.ToLower(strings.TrimSpace(snap.InstanceID))
	if !validAccountIDRe.MatchString(primID) {
		primID = "primary"
	}
	primLabel := strings.TrimSpace(snap.InstanceLabel)
	if primLabel == "" {
		primLabel = "主实例"
	}
	maxW := snap.MaxLiveWorkers
	if maxW <= 0 {
		maxW = 8
	}
	allModels := make([]string, 0, len(AgyModels))
	for _, m := range AgyModels {
		allModels = append(allModels, m.ID)
	}
	return []AccountProfile{
		{
			ID:             primID,
			Label:          primLabel,
			Primary:        true,
			Enabled:        true,
			SchedState:     SchedStateActive,
			Priority:       defaultAccountPriority,
			Weight:         defaultAccountWeight,
			ProxyPreset:    "direct",
			MaxLiveWorkers: maxW,
			BoundKeys:      []string{},
			AllowedModels:  allModels,
			DefaultModels:  allModels,
		},
	}
}

func maxAllowedAccountLiveWorkers() int {
	if envW := gwcfg.Current().MaxLiveWorkers; envW > 16 {
		return envW
	}
	return 16
}

func (am *AccountManager) syncPrimaryMaxLiveWorkersLocked() {
	if isChildAccountWorker() {
		return
	}
	stored := int64(0)
	for _, a := range am.accounts {
		if a.Primary && a.MaxLiveWorkers > 0 {
			stored = int64(a.MaxLiveWorkers)
			break
		}
	}
	am.primaryMaxLiveWorkers.Store(stored)
	syncPrimaryPromptLimitLive()
}

const (
	estWorkerRssMB           = 260
	maxParallelChildBoot     = 4
	cgroupV1UnlimitedBytes   = int64(1) << 50 // ~1 PiB sentinel used by cgroup v1 when unlimited
	cgroupMaxPlausibleMemGiB = int64(1) << 20 // 1M GiB sanity guard
)

var (
	hostMemLimitOnce sync.Once
	hostMemLimitVal  int
)

// parseHostMemLimitMB extracts container/host memory ceiling in MB from cgroup v2 memory.max,
// falling back to cgroup v1 memory.limit_in_bytes and /proc/meminfo MemTotal.
func parseHostMemLimitMB(cgroupV2, cgroupV1, procMeminfo string) int {
	if s := strings.TrimSpace(cgroupV2); s != "" && !strings.EqualFold(s, "max") {
		if b, err := strconv.ParseInt(s, 10, 64); err == nil && b > 0 && b < cgroupV1UnlimitedBytes {
			if mb := int(b / (1024 * 1024)); mb > 0 && int64(mb) < cgroupMaxPlausibleMemGiB*1024 {
				return mb
			}
		}
	}
	if s := strings.TrimSpace(cgroupV1); s != "" {
		if b, err := strconv.ParseInt(s, 10, 64); err == nil && b > 0 && b < cgroupV1UnlimitedBytes {
			if mb := int(b / (1024 * 1024)); mb > 0 && int64(mb) < cgroupMaxPlausibleMemGiB*1024 {
				return mb
			}
		}
	}
	for _, line := range strings.Split(procMeminfo, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "MemTotal:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) >= 2 {
			if kb, err := strconv.ParseInt(fields[1], 10, 64); err == nil && kb > 0 {
				return int(kb / 1024)
			}
		}
	}
	return 0
}

// detectHostMemLimitMB reads the container/host memory limit once at startup.
func detectHostMemLimitMB() int {
	hostMemLimitOnce.Do(func() {
		readTrim := func(p string) string {
			b, err := os.ReadFile(p)
			if err != nil {
				return ""
			}
			return string(b)
		}
		hostMemLimitVal = parseHostMemLimitMB(
			readTrim("/sys/fs/cgroup/memory.max"),
			readTrim("/sys/fs/cgroup/memory/memory.limit_in_bytes"),
			readTrim("/proc/meminfo"),
		)
	})
	return hostMemLimitVal
}

// computePoolWorkerMemoryBudget returns enabled worker slot sum, estimated peak RSS (MB),
// and the detected cgroup/host memory ceiling (MB).
func computePoolWorkerMemoryBudget(accs []AccountProfile) (totalMaxLiveWorkers, estPeakRssMB, hostMemLimitMB int) {
	for _, a := range accs {
		if !a.Enabled {
			continue
		}
		w := a.MaxLiveWorkers
		if w <= 0 {
			w = effectiveMaxLiveWorkers()
		}
		totalMaxLiveWorkers += w
	}
	if len(accs) == 0 {
		totalMaxLiveWorkers = effectiveMaxLiveWorkers()
	}
	estPeakRssMB = totalMaxLiveWorkers * estWorkerRssMB
	hostMemLimitMB = detectHostMemLimitMB()
	return totalMaxLiveWorkers, estPeakRssMB, hostMemLimitMB
}

// Init loads or seeds accounts.json and starts workers for any authenticated non-primary accounts.
func (am *AccountManager) Init() {
	if isChildAccountWorker() {
		return
	}
	am.mu.Lock()
	am.filePath, am.baseDataDir = resolveAccountsFilePath()
	loaded := am.loadFromDiskLocked()
	if !loaded || len(am.accounts) == 0 {
		if !testing.Testing() || gwcfg.Current().AccountsFile != "" {
			am.accounts = defaultSeededAccounts()
			_ = am.saveToDiskLocked()
		} else {
			am.accounts = nil
		}
	}
	am.syncPrimaryMaxLiveWorkersLocked()
	accountsCopy := make([]AccountProfile, len(am.accounts))
	copy(accountsCopy, am.accounts)
	stateDir := am.baseDataDir
	am.mu.Unlock()

	if !testing.Testing() && stateDir != "" {
		LoadMonitorStateFromDir(stateDir)
	}

	enabledCount := 0
	for _, acc := range accountsCopy {
		if acc.Enabled {
			enabledCount++
		}
	}
	totalEnabledWorkers, estPeakMB, hostLimitMB := computePoolWorkerMemoryBudget(accountsCopy)
	overcommitted := false
	if hostLimitMB > 0 {
		overcommitted = estPeakMB > (hostLimitMB*3)/4
	} else {
		overcommitted = enabledCount > 1 && totalEnabledWorkers > 20
	}
	if overcommitted && !testing.Testing() {
		LogEvent("WORKER_SLOT_OVERCOMMIT_WARNING", fmt.Sprintf("Sum of enabled accounts maxLiveWorkers (%d across %d accounts, ~%dMB peak RSS) exceeds safe memory ceiling (host/cgroup limit %dMB)", totalEnabledWorkers, enabledCount, estPeakMB, hostLimitMB), map[string]any{
			"enabledAccounts":     enabledCount,
			"totalMaxLiveWorkers": totalEnabledWorkers,
			"estPeakRssMB":        estPeakMB,
			"hostMemLimitMB":      hostLimitMB,
		})
	}

	startNonPrimary := func(accs []AccountProfile) {
		var toStart []string
		for _, acc := range accs {
			if !acc.Primary && acc.Enabled && am.HasOAuthToken(acc.ID) {
				toStart = append(toStart, acc.ID)
			}
		}
		if len(toStart) == 0 {
			return
		}
		sem := make(chan struct{}, maxParallelChildBoot)
		var wg sync.WaitGroup
		for _, accID := range toStart {
			wg.Add(1)
			sem <- struct{}{}
			go func(id string) {
				defer wg.Done()
				defer func() { <-sem }()
				_ = am.EnsureAccountWorkerRunning(id)
			}(accID)
		}
		wg.Wait()
	}
	if testing.Testing() {
		startNonPrimary(accountsCopy)
	} else {
		go startNonPrimary(accountsCopy)
	}
	am.startMonitorTripWatcher()
}

func (am *AccountManager) ensureInitialized() {
	am.mu.RLock()
	fp := am.filePath
	ready := len(am.accounts) > 0
	am.mu.RUnlock()
	if !ready && (!testing.Testing() || gwcfg.Current().AccountsFile != "" || (fp != "" && fileExists(fp))) {
		am.Init()
	}
}

func (am *AccountManager) loadFromDiskLocked() bool {
	if am.filePath == "" || !fileExists(am.filePath) {
		return false
	}
	b, err := os.ReadFile(am.filePath)
	if err != nil {
		return false
	}
	var f accountsFileSchema
	if err := json.Unmarshal(b, &f); err != nil || len(f.Accounts) == 0 {
		return false
	}
	am.routing = f.Routing.Normalized()
	am.groups = normalizeGroupsList(f.Groups)
	am.accounts = normalizeAccountsList(f.Accounts, f.Version)
	return true
}

func normalizeGroupsList(in []AccountGroup) []AccountGroup {
	if len(in) == 0 {
		return nil
	}
	out := make([]AccountGroup, 0, len(in))
	seen := make(map[string]bool, len(in))
	for _, g := range in {
		g.ID = strings.ToLower(strings.TrimSpace(g.ID))
		if !validAccountIDRe.MatchString(g.ID) || seen[g.ID] {
			continue
		}
		seen[g.ID] = true
		g.Label = strings.TrimSpace(g.Label)
		if g.Label == "" {
			g.Label = g.ID
		}
		if strings.TrimSpace(g.Strategy) != "" {
			g.Strategy = normalizeRouteStrategy(g.Strategy)
		}
		g.ActiveAccountID = strings.ToLower(strings.TrimSpace(g.ActiveAccountID))
		if g.BoundKeys == nil {
			g.BoundKeys = []string{}
		}
		if len(g.AllowedModels) > 0 {
			g.AllowedModels = normalizeCanonicalModelList(g.AllowedModels)
		}
		out = append(out, g)
	}
	return out
}

func normalizeGroupIDSlice(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(in))
	out := make([]string, 0, len(in))
	for _, raw := range in {
		g := strings.ToLower(strings.TrimSpace(raw))
		if g == "" || !validAccountIDRe.MatchString(g) || seen[g] {
			continue
		}
		seen[g] = true
		out = append(out, g)
	}
	return out
}

func normalizeAccountsList(in []AccountProfile, schemaVer int) []AccountProfile {
	out := make([]AccountProfile, 0, len(in))
	seen := make(map[string]bool)
	hasPrimary := false
	for _, a := range in {
		a.ID = strings.ToLower(strings.TrimSpace(a.ID))
		if !validAccountIDRe.MatchString(a.ID) || seen[a.ID] {
			continue
		}
		seen[a.ID] = true
		a.Label = strings.TrimSpace(a.Label)
		if a.Label == "" {
			a.Label = a.ID
		}
		if a.Primary {
			if hasPrimary {
				a.Primary = false
			} else {
				hasPrimary = true
			}
		}
		a.SchedState, a.Enabled = normalizeSchedState(a.SchedState, a.Enabled)
		a.Role = normalizeAccountRole(a.Role)
		a.PlanTierHint = normalizePlanTierHint(a.PlanTierHint)
		if a.Priority <= 0 {
			a.Priority = defaultAccountPriority
		} else if a.Priority > 1000 {
			a.Priority = 1000
		}
		if a.Weight <= 0 {
			a.Weight = defaultAccountWeight
		} else if a.Weight > 100 {
			a.Weight = 100
		}
		if a.MaxConcurrency < 0 {
			a.MaxConcurrency = 0
		} else if a.MaxConcurrency > 64 {
			a.MaxConcurrency = 64
		}
		a.Groups = normalizeGroupIDSlice(a.Groups)
		a.Notes = truncateRunes(strings.TrimSpace(a.Notes), 240)
		if a.ProxyPreset == "" {
			a.ProxyPreset = "direct"
		}
		if a.BoundKeys == nil {
			a.BoundKeys = []string{}
		}
		out = append(out, a)
	}
	if len(out) > 0 && !hasPrimary {
		out[0].Primary = true
		if out[0].Enabled && out[0].SchedState == "" {
			out[0].SchedState = SchedStateActive
		}
	}
	maxCap := maxAllowedAccountLiveWorkers()
	bootW := gwcfg.Current().MaxLiveWorkers
	if bootW <= 0 {
		bootW = 8
	}
	for i := range out {
		if out[i].MaxLiveWorkers <= 0 {
			if out[i].Primary {
				out[i].MaxLiveWorkers = bootW
			} else {
				out[i].MaxLiveWorkers = 3
			}
		} else if out[i].MaxLiveWorkers > maxCap {
			out[i].MaxLiveWorkers = maxCap
		}
	}

	// For v1 -> v2 migration, collect models claimed by non-primary accounts so primary retains all other models cleanly.
	nonPrimClaimed := make(map[string]bool)
	if schemaVer < 2 {
		for _, a := range out {
			if !a.Primary {
				src := a.AllowedModels
				if src == nil {
					src = a.DefaultModels
				}
				for _, cid := range normalizeCanonicalModelList(src) {
					nonPrimClaimed[cid] = true
				}
			}
		}
	}
	for i := range out {
		a := &out[i]
		if a.AllowedModels == nil {
			if schemaVer < 2 && a.Primary {
				primSet := make([]string, 0, len(AgyModels))
				for _, m := range AgyModels {
					if !nonPrimClaimed[m.ID] {
						primSet = append(primSet, m.ID)
					}
				}
				if len(primSet) == 0 {
					for _, m := range AgyModels {
						primSet = append(primSet, m.ID)
					}
				}
				a.AllowedModels = primSet
			} else if len(a.DefaultModels) > 0 {
				a.AllowedModels = normalizeCanonicalModelList(a.DefaultModels)
			} else {
				a.AllowedModels = []string{}
			}
		} else {
			a.AllowedModels = normalizeCanonicalModelList(a.AllowedModels)
		}
		a.DefaultModels = append([]string{}, a.AllowedModels...)
	}
	return out
}

func (am *AccountManager) saveToDiskLocked() error {
	if am.filePath == "" {
		am.filePath, am.baseDataDir = resolveAccountsFilePath()
	}
	if err := os.MkdirAll(filepath.Dir(am.filePath), 0o700); err != nil {
		return err
	}
	payload := accountsFileSchema{
		Version:  3,
		Routing:  am.routing.Normalized(),
		Groups:   am.groups,
		Accounts: am.accounts,
	}
	b, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	tmp := am.filePath + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, am.filePath)
}

func (am *AccountManager) AccountHomeDir(accountID string) string {
	am.ensureInitialized()
	am.mu.RLock()
	base := am.baseDataDir
	var isPrim bool
	for _, a := range am.accounts {
		if a.ID == accountID && a.Primary {
			isPrim = true
			break
		}
	}
	am.mu.RUnlock()
	if isPrim {
		return filepath.Dir(PureApiHome)
	}
	return filepath.Join(base, "accounts", accountID)
}
func (am *AccountManager) AccountGeminiDir(accountID string) string {
	am.ensureInitialized()
	am.mu.RLock()
	var isPrim bool
	for _, a := range am.accounts {
		if a.ID == accountID && a.Primary {
			isPrim = true
			break
		}
	}
	am.mu.RUnlock()
	if isPrim {
		return PureApiHome
	}
	return filepath.Join(am.AccountHomeDir(accountID), ".gemini")
}

func (am *AccountManager) EnsureAccountSandboxSeeded(accountID string) error {
	homeDir := am.AccountHomeDir(accountID)
	geminiDir := am.AccountGeminiDir(accountID)
	if isWindowsCDrivePath(homeDir) || isWindowsCDrivePath(geminiDir) {
		return nil
	}
	appDir := filepath.Join(geminiDir, "antigravity-cli")
	if err := os.MkdirAll(appDir, 0o700); err != nil {
		return err
	}
	seedDefaultCliProject(filepath.Join(geminiDir, "config", "projects", "default-cli-project.json"))
	seedOnboardingFile(filepath.Join(appDir, "cache", "onboarding.json"))
	if filepath.Clean(filepath.Join(homeDir, ".gemini")) != filepath.Clean(geminiDir) {
		dotGemini := filepath.Join(homeDir, ".gemini")
		if _, err := os.Lstat(dotGemini); os.IsNotExist(err) {
			_ = os.MkdirAll(homeDir, 0o700)
			_ = os.Symlink(geminiDir, dotGemini)
		}
	}
	return nil
}

func (am *AccountManager) ClearWorkerBackoff(accountID string) {
	id := strings.ToLower(strings.TrimSpace(accountID))
	am.mu.Lock()
	delete(am.crashStates, id)
	am.mu.Unlock()
}

func (am *AccountManager) AccountTokenPath(accountID string) string {
	am.ensureInitialized()
	am.mu.RLock()
	var isPrim bool
	for _, a := range am.accounts {
		if a.ID == accountID && a.Primary {
			isPrim = true
			break
		}
	}
	am.mu.RUnlock()
	if isPrim {
		return filepath.Join(PureApiHome, "antigravity-cli", "antigravity-oauth-token")
	}
	return filepath.Join(am.AccountHomeDir(accountID), ".gemini", "antigravity-cli", "antigravity-oauth-token")
}

func (am *AccountManager) InvalidateTokenCache(accountID string) {
	am.tokCacheMu.Lock()
	if accountID == "" {
		am.tokCache = make(map[string]tokenCacheEntry)
	} else {
		delete(am.tokCache, accountID)
	}
	am.tokCacheMu.Unlock()
}

func (am *AccountManager) HasOAuthToken(accountID string) bool {
	if !testing.Testing() {
		am.tokCacheMu.RLock()
		if ce, ok := am.tokCache[accountID]; ok && time.Since(ce.checkedAt) < 2*time.Second {
			am.tokCacheMu.RUnlock()
			return ce.exists
		}
		am.tokCacheMu.RUnlock()
	}
	p := am.AccountTokenPath(accountID)
	info, err := os.Stat(p)
	exists := err == nil && !info.IsDir() && info.Size() > 16
	if !testing.Testing() {
		am.tokCacheMu.Lock()
		am.tokCache[accountID] = tokenCacheEntry{exists: exists, checkedAt: time.Now()}
		am.tokCacheMu.Unlock()
	}
	return exists
}

// ResolveAccountProxy turns an AccountProfile's proxyPreset / customProxy into an upstream HTTP/SOCKS5
// proxy URL at runtime without persisting or exposing proxy tokens in accounts.json.
func (am *AccountManager) ResolveAccountProxy(acc AccountProfile) string {
	preset := strings.ToLower(strings.TrimSpace(acc.ProxyPreset))
	snap := gwcfg.Current()
	switch preset {
	case "", "direct":
		if acc.Primary {
			return snap.UpstreamProxy
		}
		return ""
	case "custom":
		if err := validateCustomProxyURL(acc.CustomProxy); err != nil {
			return ""
		}
		return strings.TrimSpace(acc.CustomProxy)
	default:
		if !validProxyPresetRe.MatchString(preset) {
			return ""
		}
		resinTok := snap.ResinProxyToken
		resinHost := snap.ResinHost
		if resinHost == "" {
			resinHost = "resin:2260"
		}
		if resinTok == "" {
			// Fallback: extract password from AGY_UPSTREAM_PROXY if formatted as http://user:pass@host:port
			if raw := snap.UpstreamProxy; raw != "" {
				if u, err := url.Parse(raw); err == nil && u.User != nil {
					if pw, ok := u.User.Password(); ok && pw != "" {
						resinTok = pw
						if u.Host != "" {
							resinHost = u.Host
						}
					}
				}
			}
		}
		if resinTok == "" {
			if err := validateCustomProxyURL(acc.CustomProxy); err == nil {
				return strings.TrimSpace(acc.CustomProxy)
			}
			return ""
		}
		u := &url.URL{
			Scheme: "http",
			User:   url.UserPassword(preset+"."+acc.ID, resinTok),
			Host:   resinHost,
		}
		return u.String()
	}
}

// ResolveAccountProxyChecked enforces fail-closed proxy resolution so an account configured with
// a custom or named non-direct proxy preset never silently falls back to direct host egress.
func (am *AccountManager) ResolveAccountProxyChecked(acc AccountProfile) (string, error) {
	preset := strings.ToLower(strings.TrimSpace(acc.ProxyPreset))
	switch preset {
	case "", "direct":
		return am.ResolveAccountProxy(acc), nil
	case "custom":
		if strings.TrimSpace(acc.CustomProxy) == "" {
			return "", fmt.Errorf("账号 %q 配置为自定义代理，但代理地址为空", acc.ID)
		}
		if err := validateCustomProxyURL(acc.CustomProxy); err != nil {
			return "", err
		}
		return strings.TrimSpace(acc.CustomProxy), nil
	default:
		if !validProxyPresetRe.MatchString(preset) {
			return "", fmt.Errorf("账号 %q 代理预设 %q 格式无效", acc.ID, preset)
		}
		resolved := am.ResolveAccountProxy(acc)
		if resolved == "" {
			return "", fmt.Errorf("账号 %q 配置了代理预设 %q，但未配置 AGY_RESIN_PROXY_TOKEN 或备用自定义代理（已拒绝静默直连）", acc.ID, preset)
		}
		return resolved, nil
	}
}

func (am *AccountManager) ListAccounts() []AccountProfile {
	am.ensureInitialized()
	am.mu.RLock()
	defer am.mu.RUnlock()
	out := make([]AccountProfile, len(am.accounts))
	copy(out, am.accounts)
	return out
}

func (am *AccountManager) GetPrimaryAccount() (AccountProfile, bool) {
	am.ensureInitialized()
	am.mu.RLock()
	defer am.mu.RUnlock()
	for _, a := range am.accounts {
		if a.Primary {
			return a, true
		}
	}
	return AccountProfile{}, false
}

func (am *AccountManager) GetAccount(id string) (AccountProfile, bool) {
	am.ensureInitialized()
	am.mu.RLock()
	defer am.mu.RUnlock()
	for _, a := range am.accounts {
		if a.ID == id {
			return a, true
		}
	}
	return AccountProfile{}, false
}

// SetAccountPlanTierHint persists an authoritative subscription tier ("pro" | "ultra") on an account profile
// when detected via `/credits` or `agy models`, avoiding unnecessary disk writes when unchanged.
func (am *AccountManager) SetAccountPlanTierHint(accountID, tier string) {
	if isChildAccountWorker() {
		return
	}
	norm := normalizePlanTierHint(tier)
	if norm == "" {
		return
	}
	id := strings.ToLower(strings.TrimSpace(accountID))
	if id == "" {
		return
	}
	am.ensureInitialized()
	am.mu.Lock()
	changed := false
	for i := range am.accounts {
		if am.accounts[i].ID == id {
			if am.accounts[i].PlanTierHint != norm {
				am.accounts[i].PlanTierHint = norm
				changed = true
			}
			break
		}
	}
	if changed {
		_ = am.saveToDiskLocked()
	}
	am.mu.Unlock()
}

func isLoopbackPortFree(port int) bool {
	if port <= 0 || port > 65535 {
		return false
	}
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return false
	}
	_ = ln.Close()
	return true
}

func workerCrashBackoffDuration(failures int) time.Duration {
	if failures <= 0 {
		return 0
	}
	d := 2 * time.Second
	for i := 1; i < failures && d < 30*time.Second; i++ {
		d *= 2
	}
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}

func (am *AccountManager) allocatePortsLocked(accountID string) (gwPort, dashPort int, err error) {
	if w, ok := am.workers[accountID]; ok && w != nil && w.gwPort > 0 && !w.stopped {
		return w.gwPort, w.dashPort, nil
	}
	idx := 1
	for i, a := range am.accounts {
		if a.ID == accountID {
			idx = i + 1
			break
		}
	}
	used := make(map[int]bool, len(am.workers)*2)
	for _, w := range am.workers {
		if w != nil && !w.stopped && w.gwPort > 0 {
			used[w.gwPort] = true
			used[w.dashPort] = true
		}
	}
	base := 28900 + idx*2
	for attempts := 0; attempts < 128; attempts++ {
		if !used[base] && !used[base+1] && isLoopbackPortFree(base) && isLoopbackPortFree(base+1) {
			return base, base + 1, nil
		}
		base += 2
	}
	return 0, 0, fmt.Errorf("no free loopback port pair available for account %q", accountID)
}

// EnsureAccountWorkerRunning spawns or verifies the isolated child gateway process for a non-primary account.
func (am *AccountManager) EnsureAccountWorkerRunning(accountID string) error {
	return am.EnsureAccountWorkerRunningWithContext(context.Background(), accountID)
}

// EnsureAccountWorkerRunningWithContext spawns or verifies the isolated child gateway process, honoring caller cancellation while waiting on an in-progress startup.
func (am *AccountManager) EnsureAccountWorkerRunningWithContext(ctx context.Context, accountID string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if isChildAccountWorker() {
		return nil
	}
	am.ensureInitialized()

	am.mu.Lock()
	var acc AccountProfile
	found := false
	for _, a := range am.accounts {
		if a.ID == accountID {
			acc = a
			found = true
			break
		}
	}
	if !found {
		am.mu.Unlock()
		return fmt.Errorf("unknown account %q", accountID)
	}
	if acc.Primary {
		am.mu.Unlock()
		return nil
	}
	if !acc.Enabled {
		am.mu.Unlock()
		return fmt.Errorf("account %q is disabled", accountID)
	}
	if testing.Testing() {
		am.mu.Unlock()
		return nil
	}
	if w, ok := am.workers[accountID]; ok && w != nil && !w.stopped {
		readyCh := w.readyCh
		am.mu.Unlock()
		if readyCh != nil {
			select {
			case <-readyCh:
				return w.readyErr
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	if am.crashStates == nil {
		am.crashStates = make(map[string]workerCrashState)
	}
	if cs, hasCS := am.crashStates[accountID]; hasCS && !cs.backoffUntil.IsZero() {
		if rem := time.Until(cs.backoffUntil); rem > 0 {
			am.mu.Unlock()
			return fmt.Errorf("worker for account %q in crash backoff (%ds remaining)", accountID, int(rem.Seconds())+1)
		}
	}
	gwPort, dashPort, portErr := am.allocatePortsLocked(acc.ID)
	if portErr != nil {
		am.mu.Unlock()
		return portErr
	}
	proc := &accountWorkerProc{
		id:       acc.ID,
		gwPort:   gwPort,
		dashPort: dashPort,
		done:     make(chan struct{}),
		readyCh:  make(chan struct{}),
	}
	am.workers[acc.ID] = proc
	baseDataDir := am.baseDataDir
	am.mu.Unlock()

	abortPlaceholder := func(err error, countFailure bool) error {
		am.mu.Lock()
		if am.workers[acc.ID] == proc {
			delete(am.workers, acc.ID)
		}
		if countFailure && !proc.intentionalStop {
			cs := am.crashStates[acc.ID]
			cs.failures++
			cs.backoffUntil = time.Now().Add(workerCrashBackoffDuration(cs.failures))
			am.crashStates[acc.ID] = cs
		}
		proc.stopped = true
		proc.finishReady(err)
		am.mu.Unlock()
		proc.closeTransport()
		return err
	}

	resolvedProxy, err := am.ResolveAccountProxyChecked(acc)
	if err != nil {
		return abortPlaceholder(err, false)
	}
	homeDir := filepath.Join(baseDataDir, "accounts", acc.ID)
	for _, sub := range []string{"", ".gemini/antigravity-cli", "logs", "workspace"} {
		p := filepath.Join(homeDir, sub)
		if err := os.MkdirAll(p, 0o700); err != nil {
			return abortPlaceholder(err, false)
		}
	}
	_ = am.EnsureAccountSandboxSeeded(acc.ID)

	exePath, err := os.Executable()
	if err != nil {
		return abortPlaceholder(err, false)
	}

	cfgSnap := gwcfg.Current()
	childMemLimit := strings.TrimSpace(cfgSnap.ChildGatewayGoMemLimit)
	if childMemLimit == "" {
		childMemLimit = "128MiB"
	}
	if !cfgSnap.ChildGatewayGoMemLimitEnvSet && acc.MaxLiveWorkers > 8 {
		childMemLimit = "224MiB"
	}
	childMaxPrompts := acc.MaxLiveWorkers
	if childMaxPrompts < 6 {
		childMaxPrompts = 6
	}
	if childMaxPrompts > 16 {
		childMaxPrompts = 16
	}

	cmd := exec.Command(exePath)
	cmd.Dir = filepath.Join(homeDir, "workspace")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stderrTail := &boundedTailBuffer{max: 4096}
	cmd.Stderr = stderrTail

	env := make([]string, 0, len(os.Environ())+30)
	hasSSH := false
	for _, kv := range os.Environ() {
		if isStrippedAgyEngineEnv(kv) {
			continue
		}
		if strings.HasPrefix(kv, "SSH_CONNECTION=") {
			hasSSH = true
		}
		switch {
		case strings.HasPrefix(kv, "HOME="),
			strings.HasPrefix(kv, "AGY_CHILD_ACCOUNT_WORKER="),
			strings.HasPrefix(kv, "AGY_PURE_API_HOME="),
			strings.HasPrefix(kv, "AGY_DEFAULT_CWD="),
			strings.HasPrefix(kv, "AGY_STATE_DB="),
			strings.HasPrefix(kv, "AGY_TOS_TRIPPED_FILE="),
			strings.HasPrefix(kv, "AGY_GW_LOG="),
			strings.HasPrefix(kv, "AGY_GW_TMPLOG="),
			strings.HasPrefix(kv, "AGY_APPDATA_LINK="),
			strings.HasPrefix(kv, "AGY_CONV_DIR="),
			strings.HasPrefix(kv, "AGY_APP_CONV_DIR="),
			strings.HasPrefix(kv, "AGY_WAF_RULES_FILE="),
			strings.HasPrefix(kv, "AGY_ACP_RECORD="),
			strings.HasPrefix(kv, "AGY_LISTEN_HOST="),
			strings.HasPrefix(kv, "AGY_GW_PORT="),
			strings.HasPrefix(kv, "AGY_DASH_PORT="),
			strings.HasPrefix(kv, "AGY_INSTANCE_ID="),
			strings.HasPrefix(kv, "AGY_INSTANCE_LABEL="),
			strings.HasPrefix(kv, "AGY_MAX_LIVE_WORKERS="),
			(!cfgSnap.MaxConcurrentPromptsEnvSet && strings.HasPrefix(kv, "AGY_MAX_CONCURRENT_PROMPTS=")),
			strings.HasPrefix(kv, "AGY_UPSTREAM_PROXY="),
			strings.HasPrefix(kv, "AGY_DASHBOARD_PEERS="),
			strings.HasPrefix(kv, "AGY_ACCOUNTS_FILE="),
			strings.HasPrefix(kv, "AGY_PROXY_PRESETS_FILE="),
			strings.HasPrefix(kv, "AGY_RESIN_PROXY_TOKEN="),
			strings.HasPrefix(kv, "AGY_RESIN_HOST="),
			strings.HasPrefix(kv, "AGY_ACCOUNT_PLAN_TIER_HINT="):
			continue
		}
		env = append(env, kv)
	}
	if runtime.GOOS == "darwin" && !hasSSH {
		env = append(env, "SSH_CONNECTION=1 1 1 1")
	}
	env = append(env,
		"AGY_CHILD_ACCOUNT_WORKER=1",
		"GOMEMLIMIT="+childMemLimit,
		"DBUS_SESSION_BUS_ADDRESS=unix:path=/dev/null",
		"AGY_CLI_DISABLE_AUTO_UPDATE=1",
		"HOME="+homeDir,
		"AGY_PURE_API_HOME="+filepath.Join(homeDir, ".gemini"),
		"AGY_DEFAULT_CWD="+filepath.Join(homeDir, "workspace"),
		"AGY_STATE_DB="+filepath.Join(homeDir, "gateway.db"),
		"AGY_TOS_TRIPPED_FILE="+filepath.Join(homeDir, "tos-tripped"),
		"AGY_GW_LOG="+filepath.Join(homeDir, "logs", "gateway.log"),
		"AGY_APP_CONV_DIR=off",
		"AGY_LISTEN_HOST=127.0.0.1",
		fmt.Sprintf("AGY_GW_PORT=%d", gwPort),
		fmt.Sprintf("AGY_DASH_PORT=%d", dashPort),
		"AGY_INSTANCE_ID="+acc.ID,
		"AGY_INSTANCE_LABEL="+acc.Label,
		fmt.Sprintf("AGY_MAX_LIVE_WORKERS=%d", acc.MaxLiveWorkers),
		"AGY_UPSTREAM_PROXY="+resolvedProxy,
		"HTTP_PROXY="+resolvedProxy,
		"HTTPS_PROXY="+resolvedProxy,
		"http_proxy="+resolvedProxy,
		"https_proxy="+resolvedProxy,
		"ALL_PROXY=",
		"all_proxy=",
		"NO_PROXY=127.0.0.1,localhost,::1",
		"no_proxy=127.0.0.1,localhost,::1",
		"AGY_DASHBOARD_PEERS=",
	)
	if hint := normalizePlanTierHint(acc.PlanTierHint); hint != "" {
		env = append(env, "AGY_ACCOUNT_PLAN_TIER_HINT="+hint)
	}
	if !cfgSnap.MaxConcurrentPromptsEnvSet {
		env = append(env, fmt.Sprintf("AGY_MAX_CONCURRENT_PROMPTS=%d", childMaxPrompts))
	}
	cmd.Env = env

	if err := cmd.Start(); err != nil {
		return abortPlaceholder(err, true)
	}

	targetURL, _ := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", gwPort))
	rp := httputil.NewSingleHostReverseProxy(targetURL)
	rp.FlushInterval = -1
	baseDirector := rp.Director
	rp.Director = func(req *http.Request) {
		baseDirector(req)
		req.Header.Del("X-Forwarded-Host")
		req.Header.Del("X-Forwarded-Proto")
		req.Header.Del("X-Forwarded-For")
	}
	tr := &http.Transport{
		Proxy:               nil,
		MaxIdleConns:        32,
		MaxIdleConnsPerHost: 16,
		IdleConnTimeout:     60 * time.Second,
	}
	rp.Transport = tr
	accIDCopy := acc.ID
	rp.ModifyResponse = func(resp *http.Response) error {
		if resp != nil {
			modelID := ""
			if meta, ok := routedAccountMetaFromRequest(resp.Request); ok {
				modelID = meta.modelID
			}
			am.RecordAccountOutcome(accIDCopy, modelID, resp.StatusCode, "", resp.Header.Get("Retry-After"), "")
		}
		return nil
	}
	rp.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, proxyErr error) {
		modelID := ""
		if meta, ok := routedAccountMetaFromRequest(req); ok {
			modelID = meta.modelID
		}
		redacted := gwcfg.RedactSecretsInString(proxyErr.Error())
		am.RecordAccountOutcome(accIDCopy, modelID, http.StatusBadGateway, "account_worker_unreachable", "", redacted)
		LogEvent("ACCOUNT_PROXY_ERR", "Child account worker reverse proxy error", map[string]any{
			"accountId": accIDCopy,
			"error":     redacted,
		})
		writeAccountRouteError(rw, req.URL.Path, http.StatusBadGateway, "account_worker_error", "account_worker_unreachable",
			fmt.Sprintf("Account %q worker is temporarily unreachable; retry shortly.", accIDCopy))
	}

	am.mu.Lock()
	if proc.stopped || am.workers[acc.ID] != proc {
		am.mu.Unlock()
		tr.CloseIdleConnections()
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
		close(proc.done)
		proc.finishReady(fmt.Errorf("worker startup cancelled for account %q", acc.ID))
		return proc.readyErr
	}
	proc.gwPort = gwPort
	proc.dashPort = dashPort
	proc.cmd = cmd
	proc.proxy = rp
	proc.transport = tr
	am.mu.Unlock()

	go func(p *accountWorkerProc, proxyForRedact string, tail *boundedTailBuffer) {
		waitErr := p.cmd.Wait()
		am.mu.Lock()
		wasIntentional := p.intentionalStop
		healthyAt := p.healthyAt
		p.stopped = true
		if am.workers[p.id] == p {
			delete(am.workers, p.id)
		}
		if !wasIntentional {
			cs := am.crashStates[p.id]
			if !healthyAt.IsZero() && time.Since(healthyAt) >= 30*time.Second {
				cs.failures = 1
			} else {
				cs.failures++
			}
			cs.backoffUntil = time.Now().Add(workerCrashBackoffDuration(cs.failures))
			am.crashStates[p.id] = cs
		}
		am.mu.Unlock()
		p.closeTransport()
		close(p.done)
		InvalidatePeerDashboardCache()
		if !wasIntentional {
			errText := ""
			if waitErr != nil {
				errText = waitErr.Error()
			}
			LogEvent("ACCOUNT_WORKER_EXIT", "Child account worker exited unexpectedly", map[string]any{
				"accountId":  p.id,
				"error":      redactAccountErrorString(errText, proxyForRedact),
				"stderrTail": redactAccountErrorString(tail.String(), proxyForRedact),
			})
		}
	}(proc, resolvedProxy, stderrTail)

	cleanupFailed := func(err error) error {
		am.mu.Lock()
		if am.workers[acc.ID] == proc {
			delete(am.workers, acc.ID)
		}
		wasIntentional := proc.intentionalStop
		proc.intentionalStop = true
		proc.stopped = true
		if !wasIntentional {
			cs := am.crashStates[acc.ID]
			cs.failures++
			cs.backoffUntil = time.Now().Add(workerCrashBackoffDuration(cs.failures))
			am.crashStates[acc.ID] = cs
		}
		proc.finishReady(err)
		am.mu.Unlock()
		proc.closeTransport()
		alreadyReaped := false
		if proc.done != nil {
			select {
			case <-proc.done:
				alreadyReaped = true
			default:
			}
		}
		if !alreadyReaped && proc.cmd != nil && proc.cmd.Process != nil {
			pid := proc.cmd.Process.Pid
			_ = syscall.Kill(-pid, syscall.SIGTERM)
			go func(done <-chan struct{}, pgid int) {
				t := time.NewTimer(2 * time.Second)
				defer t.Stop()
				select {
				case <-done:
					return
				case <-t.C:
					_ = syscall.Kill(-pgid, syscall.SIGKILL)
				}
			}(proc.done, pid)
		}
		return err
	}

	// Wait for child /healthz on loopback with bounded per-probe timeout, aborting immediately if child exits early
	// or caller cancels. Allow 12s so large-DB child workers (hundreds of restored sessions) finish SQLite restore cleanly.
	deadline := time.Now().Add(12 * time.Second)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	healthURL := fmt.Sprintf("http://127.0.0.1:%d/healthz", gwPort)
	healthy := false
	for time.Now().Before(deadline) {
		select {
		case <-proc.done:
			return cleanupFailed(fmt.Errorf("worker for account %q exited before becoming healthy", acc.ID))
		case <-ctx.Done():
			return cleanupFailed(ctx.Err())
		default:
		}
		probeTimeout := time.Until(deadline)
		if probeTimeout > 500*time.Millisecond {
			probeTimeout = 500 * time.Millisecond
		}
		if probeTimeout <= 0 {
			break
		}
		probeCtx, probeCancel := context.WithTimeout(context.Background(), probeTimeout)
		reqH, reqErr := http.NewRequestWithContext(probeCtx, http.MethodGet, healthURL, nil)
		if reqErr == nil {
			resp, doErr := peerHTTPClient.Do(reqH)
			if doErr == nil {
				_ = resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					probeCancel()
					healthy = true
					break
				}
			}
		}
		probeCancel()
		time.Sleep(40 * time.Millisecond)
	}
	if !healthy {
		return cleanupFailed(fmt.Errorf("worker for account %q healthz timeout", acc.ID))
	}
	am.mu.Lock()
	if proc.stopped || am.workers[acc.ID] != proc {
		err := proc.readyErr
		am.mu.Unlock()
		if err != nil {
			return err
		}
		return fmt.Errorf("worker for account %q was stopped during startup", acc.ID)
	}
	proc.healthyAt = time.Now()
	proc.finishReady(nil)
	am.mu.Unlock()
	return nil
}

func (am *AccountManager) StopAccountWorker(accountID string) {
	am.mu.Lock()
	delete(am.crashStates, accountID)
	w, ok := am.workers[accountID]
	if ok {
		delete(am.workers, accountID)
		if w != nil {
			w.intentionalStop = true
			w.stopped = true
			w.finishReady(fmt.Errorf("worker for account %q stopped", accountID))
		}
	}
	am.mu.Unlock()
	if w != nil {
		w.closeTransport()
	}
	if ok && w != nil && w.cmd != nil && w.cmd.Process != nil {
		_ = syscall.Kill(-w.cmd.Process.Pid, syscall.SIGTERM)
		if w.done != nil {
			select {
			case <-w.done:
			case <-time.After(7 * time.Second):
				_ = syscall.Kill(-w.cmd.Process.Pid, syscall.SIGKILL)
				<-w.done
			}
		}
	}
}

func (am *AccountManager) StopAll() {
	am.stopMonitorTripWatcher()

	am.oauthMu.Lock()
	for _, s := range am.oauthActive {
		am.cancelOAuthSessionLocked(s, true)
	}
	am.oauthMu.Unlock()

	am.mu.RLock()
	ids := make([]string, 0, len(am.workers))
	for id := range am.workers {
		ids = append(ids, id)
	}
	am.mu.RUnlock()

	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(accID string) {
			defer wg.Done()
			am.StopAccountWorker(accID)
		}(id)
	}
	wg.Wait()
}

func (am *AccountManager) IsWorkerRunning(accountID string) (running bool, gwPort, dashPort int) {
	am.mu.RLock()
	defer am.mu.RUnlock()
	if w, ok := am.workers[accountID]; ok && w != nil && !w.stopped && w.proxy != nil && w.gwPort > 0 {
		return true, w.gwPort, w.dashPort
	}
	return false, 0, 0
}

// splitKeyAccountSuffix splits "<master_key>@<account_id>", "<master_key>#<account_id>",
// "<master_key>@acct:<account_id>", or "<master_key>#acct:<account_id>".
func splitKeyAccountSuffix(candidate string) (baseKey, accountID string, ok bool) {
	lowCand := strings.ToLower(candidate)
	for _, sep := range []string{"@acct:", "#acct:"} {
		if idx := strings.LastIndex(lowCand, sep); idx > 0 && idx+len(sep) < len(candidate) {
			base := candidate[:idx]
			acc := strings.ToLower(strings.TrimSpace(candidate[idx+len(sep):]))
			if validAccountIDRe.MatchString(acc) {
				return base, acc, true
			}
		}
	}
	for _, sep := range []string{"@", "#"} {
		if idx := strings.LastIndex(candidate, sep); idx > 0 && idx < len(candidate)-1 {
			base := candidate[:idx]
			acc := strings.ToLower(strings.TrimSpace(candidate[idx+1:]))
			if validAccountIDRe.MatchString(acc) {
				return base, acc, true
			}
		}
	}
	return "", "", false
}

// MatchesAccountKey reports whether candidate matches any account's boundKeys or "<master_key>@<account_id>".
func (am *AccountManager) MatchesAccountKey(candidate string) (accountID string, matched bool) {
	if candidate == "" {
		return "", false
	}
	am.ensureInitialized()
	if base, accID, ok := splitKeyAccountSuffix(candidate); ok {
		if rawMasterKeyMatches(base) {
			if _, exists := am.GetAccount(accID); exists {
				return accID, true
			}
		}
	}
	cBytes := []byte(candidate)
	am.mu.RLock()
	defer am.mu.RUnlock()
	for _, acc := range am.accounts {
		if !acc.Enabled {
			continue
		}
		for _, bk := range acc.BoundKeys {
			bk = strings.TrimSpace(bk)
			if bk != "" && subtle.ConstantTimeCompare(cBytes, []byte(bk)) == 1 {
				return acc.ID, true
			}
		}
	}
	return "", false
}

func quotaBucketStillExhausted(b QuotaBucket, now time.Time) bool {
	if b.RemainingFraction > 0.0005 {
		return false
	}
	if b.ResetTime != "" {
		if rt, err := time.Parse(time.RFC3339, b.ResetTime); err == nil && !now.Before(rt) {
			return false
		}
	}
	return true
}

func extraPoolStillExhausted(ep *ExtraPoolQuota, now time.Time) bool {
	if ep == nil || ep.RemainingFraction == nil || *ep.RemainingFraction > 0.0005 {
		return false
	}
	if ep.ResetTime != "" {
		if rt, err := time.Parse(time.RFC3339, ep.ResetTime); err == nil && !now.Before(rt) {
			return false
		}
	}
	return true
}

func quotaGroupsModel5hExhausted(groups []QuotaGroup, argonPool *ExtraPoolQuota, canonicalModelID string, now time.Time) bool {
	lowModel := strings.ToLower(strings.TrimSpace(canonicalModelID))
	isClaude := strings.HasPrefix(lowModel, "claude-")
	isArgon := lowModel == "gemini-4-argon"
	if isArgon && extraPoolStillExhausted(argonPool, now) {
		return true
	}
	for _, g := range groups {
		if isClaude {
			lowName := strings.ToLower(g.Name)
			if strings.Contains(lowName, "claude") || strings.Contains(lowName, "gpt") {
				for _, b := range g.Buckets {
					if b.Window == "5h" && quotaBucketStillExhausted(b, now) {
						return true
					}
				}
			}
		} else if strings.EqualFold(g.Name, "Gemini Models") {
			for _, b := range g.Buckets {
				if b.Window == "5h" && quotaBucketStillExhausted(b, now) {
					return true
				}
			}
		}
	}
	return false
}

func isPrimaryModel5hExhausted(canonicalModelID string) bool {
	quotaMu.Lock()
	snap := cachedQuota
	snapAt := cachedQuotaAt
	quotaMu.Unlock()
	if snap == nil || time.Since(snapAt) > 20*time.Minute {
		return false
	}
	return quotaGroupsModel5hExhausted(snap.Groups, snap.ArgonPool, canonicalModelID, time.Now())
}

func isAccountModel5hExhausted(accountID string, dashPort int, canonicalModelID string) bool {
	if dashPort <= 0 {
		return false
	}
	cacheKey := fmt.Sprintf("%s|http://127.0.0.1:%d", accountID, dashPort)
	peerDashCacheMu.Lock()
	cached, ok := peerDashCache[cacheKey]
	peerDashCacheMu.Unlock()
	now := time.Now()
	// Trigger non-blocking background refresh if peer quota cache is missing or older than 60s
	if !testing.Testing() && (!ok || cached.decoded == nil || now.Sub(cached.fetchedAt) > 60*time.Second) {
		peer := gwcfg.DashboardPeerConfig{
			ID:      accountID,
			Label:   accountID,
			BaseURL: fmt.Sprintf("http://127.0.0.1:%d", dashPort),
		}
		fallbackKey := defaultPeerAPIKey(gwcfg.Current())
		go func(p gwcfg.DashboardPeerConfig, k string) {
			_, _, _, _, _ = fetchPeerDashboard(p, false, k)
		}(peer, fallbackKey)
	}
	if !ok || cached.decoded == nil || now.Sub(cached.fetchedAt) > 20*time.Minute {
		return false
	}
	return quotaGroupsModel5hExhausted(cached.decoded.Quota.Groups, cached.decoded.Quota.ArgonPool, canonicalModelID, now)
}

func (am *AccountManager) stickyTTL() time.Duration {
	cfg := am.GetRoutingConfig()
	if cfg.StickyTTLSec > 0 {
		return time.Duration(cfg.StickyTTLSec) * time.Second
	}
	ttl := gwcfg.Current().IdleWorkerTtl
	if ttl < 15*time.Minute {
		ttl = 15 * time.Minute
	}
	return ttl
}

// ClearStickyForAccount drops all sticky session bindings pointing to accountID (e.g., on disable/delete).
func (am *AccountManager) ClearStickyForAccount(accountID string) {
	am.stickyMu.Lock()
	defer am.stickyMu.Unlock()
	am.stickyLRU.clearForAccount(accountID)
}

// ClearStickyDisallowedModels prunes sticky bindings for models removed from an account's allowlist without disrupting still-allowed active sessions.
func (am *AccountManager) ClearStickyDisallowedModels(accountID string, allowed []string) {
	set := make(map[string]bool, len(allowed))
	for _, m := range allowed {
		set[m] = true
	}
	am.stickyMu.Lock()
	defer am.stickyMu.Unlock()
	am.stickyLRU.clearDisallowedModels(accountID, set)
}

// shouldTriggerWorkerWarmup throttles background EnsureAccountWorkerRunning triggers per account.
func (am *AccountManager) shouldTriggerWorkerWarmup(accountID string, minInterval time.Duration) bool {
	now := time.Now()
	am.warmupMu.Lock()
	defer am.warmupMu.Unlock()
	if am.warmupAttempt == nil {
		am.warmupAttempt = make(map[string]time.Time)
	}
	if last, ok := am.warmupAttempt[accountID]; ok && now.Sub(last) < minInterval {
		return false
	}
	am.warmupAttempt[accountID] = now
	return true
}

func callerKeyScopeDigest(r *http.Request) string {
	if r == nil {
		return "anon"
	}
	rk := strings.TrimSpace(requestKey(r))
	if rk == "" {
		return "anon"
	}
	if base, _, ok := splitKeyGroupSuffix(rk); ok {
		rk = base
	} else if base, _, ok := splitKeyAccountSuffix(rk); ok {
		rk = base
	}
	sum := sha256.Sum256([]byte(rk))
	return hex.EncodeToString(sum[:4])
}

// ResolveExplicitAccount resolves an explicit target account from bound API key (highest priority)
// or X-Agy-Account header (only permitted for master keys / auth-disabled, never to override a bound key or group key).
func (am *AccountManager) ResolveExplicitAccount(r *http.Request) (acc AccountProfile, routeMode string, explicit bool, invalidErr error) {
	am.ensureInitialized()
	if r == nil {
		return AccountProfile{}, "", false, nil
	}
	hdr := strings.ToLower(strings.TrimSpace(r.Header.Get("X-Agy-Account")))
	hdrGrp := strings.ToLower(strings.TrimSpace(r.Header.Get("X-Agy-Group")))
	if reqK := requestKey(r); reqK != "" {
		if accID, ok := am.MatchesAccountKey(reqK); ok {
			if hdr != "" && hdr != accID {
				return AccountProfile{}, "key-bound", true, fmt.Errorf("X-Agy-Account %q cannot override API key bound to account %q", hdr, accID)
			}
			a, exists := am.GetAccount(accID)
			if !exists || !a.Enabled {
				return AccountProfile{}, "key-bound", true, fmt.Errorf("account %q bound to this API key is disabled", accID)
			}
			if hdrGrp != "" && !a.InGroup(hdrGrp) {
				return AccountProfile{}, "key-bound", true, fmt.Errorf("X-Agy-Group %q conflicts with API key bound to account %q", hdrGrp, accID)
			}
			return a, "key-bound", true, nil
		}
		if grp, okGrp := am.MatchesGroupKey(reqK); okGrp && hdr != "" {
			return AccountProfile{}, "group-bound", true, fmt.Errorf("X-Agy-Account %q cannot override API key bound to group %q", hdr, grp.ID)
		}
	}
	if hdr != "" {
		a, ok := am.GetAccount(hdr)
		if !ok || !a.Enabled {
			return AccountProfile{}, "header-bound", true, fmt.Errorf("account %q is not found or disabled", hdr)
		}
		if hdrGrp != "" {
			grp, okG := am.GetGroup(hdrGrp)
			if !okG {
				return AccountProfile{}, "header-bound", true, fmt.Errorf("account group %q is not found", hdrGrp)
			}
			if !a.InGroup(grp.ID) {
				return AccountProfile{}, "header-bound", true, fmt.Errorf("account %q is not a member of group %q", a.ID, grp.ID)
			}
		}
		return a, "header-bound", true, nil
	}
	return AccountProfile{}, "", false, nil
}

// ResolveOptions configures extended pool/group account resolution.
type ResolveOptions struct {
	SessionKey        string
	IsContinuation    bool
	HasTools          bool
	ToolResultCallIDs []string
	ExcludeIDs        map[string]bool
	ReserveInFlight   bool
	DeferStickyCommit bool
}

// cancelHoppedSession terminates any orphaned parked ACP session for sessionKey/callIDs
// on the account that MONITOR_TRIP_HOP just routed away from (local primary or federated child peer),
// preventing zombie ws_await_result relays from burning 80x monitor quota or blocking agy hot-switches.
func (am *AccountManager) cancelHoppedSession(oldAccountID, sessionKey string, callIDs []string, reason string) {
	if am == nil || oldAccountID == "" || (sessionKey == "" && len(callIDs) == 0) {
		return
	}
	if hook := am.getMonitorTripHopCancelHook(); hook != nil {
		hook(oldAccountID, sessionKey, callIDs, reason)
	}
	if am.isPrimaryAccountID(oldAccountID) {
		if fn := am.getPrimaryCancelParkedFn(); fn != nil {
			fn(sessionKey, callIDs, reason)
			return
		}
		if gs := activeGatewayPool.Load(); gs != nil {
			gs.CancelParkedConversation(sessionKey, callIDs, reason)
		}
		return
	}
	am.mu.RLock()
	dashPort := 0
	if p := am.workers[oldAccountID]; p != nil && !p.stopped && p.dashPort > 0 {
		dashPort = p.dashPort
	}
	am.mu.RUnlock()
	if dashPort <= 0 {
		return
	}
	fallbackKey := defaultPeerAPIKey(gwcfg.Current())
	doCancel := func(port int, sk string, cids []string, rsn, apiKey, accID string) {
		payload, err := json.Marshal(map[string]any{
			"sessionKey": sk,
			"callIds":    cids,
			"reason":     rsn,
		})
		if err != nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		urlStr := fmt.Sprintf("http://127.0.0.1:%d/api/internal/cancel-parked-session", port)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, urlStr, bytes.NewReader(payload))
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/json")
		if apiKey != "" {
			req.Header.Set("x-api-key", apiKey)
		}
		resp, err := peerHTTPClient.Do(req)
		if err != nil {
			LogEvent("MONITOR_TRIP_HOP_CANCEL_ERR", "Failed to cancel parked session on child peer after hop", map[string]any{
				"accountId": accID,
				"error":     gwcfg.RedactSecretsInString(err.Error()),
			})
			return
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
	}
	if testing.Testing() {
		doCancel(dashPort, sessionKey, append([]string(nil), callIDs...), reason, fallbackKey, oldAccountID)
	} else {
		go doCancel(dashPort, sessionKey, append([]string(nil), callIDs...), reason, fallbackKey, oldAccountID)
	}
}

// RoutePickResult holds the resolved account, routing metadata, and lifecycle callbacks.
type RoutePickResult struct {
	Account       AccountProfile
	GroupID       string
	RouteMode     string
	IsExplicit    bool
	IsStickyHit   bool
	ReleaseFlight func()
	CommitSticky  func()
	DenyErr       error
	StatusCode    int
	RetryAfterSec int
}

// ResolveTargetAccount determines which account should serve an incoming API request.
func (am *AccountManager) ResolveTargetAccount(r *http.Request, canonicalModelID string) (AccountProfile, bool) {
	acc, _, _, status := am.ResolveTargetAccountDetailed(r, canonicalModelID, extractSessionKeyFromHeaders(r))
	return acc, status == 0 && acc.ID != ""
}

// ResolveTargetAccountDetailed returns the chosen account, routeMode, or an HTTP denial error (403/429/503).
func (am *AccountManager) ResolveTargetAccountDetailed(r *http.Request, canonicalModelID, sessionKey string) (acc AccountProfile, routeMode string, denyErr error, statusCode int) {
	acc, routeMode, denyErr, statusCode, _ = am.ResolveTargetAccountWithHints(r, canonicalModelID, sessionKey, false)
	return acc, routeMode, denyErr, statusCode
}

// ResolveTargetAccountWithHints schedules new conversations across eligible accounts/groups while
// enforcing Decision U8 (never replaying active multi-turn conversations across personal OAuth accounts).
func (am *AccountManager) ResolveTargetAccountWithHints(r *http.Request, canonicalModelID, sessionKey string, isContinuation bool) (acc AccountProfile, routeMode string, denyErr error, statusCode int, retryAfterSec int) {
	res := am.ResolveTargetAccountEx(r, canonicalModelID, ResolveOptions{
		SessionKey:        sessionKey,
		IsContinuation:    isContinuation,
		ReserveInFlight:   false,
		DeferStickyCommit: false,
	})
	return res.Account, res.RouteMode, res.DenyErr, res.StatusCode, res.RetryAfterSec
}

// ResolveTargetAccountEx resolves the target account with single-pass lock snapshots, optional candidate exclusions
// (for pre-stream 1st-turn failover), atomic inFlight reservation, and deferred sticky session commit.
func (am *AccountManager) ResolveTargetAccountEx(r *http.Request, canonicalModelID string, opts ResolveOptions) RoutePickResult {
	noop := func() {}
	am.ensureInitialized()
	now := time.Now()

	// 1. Explicit header or bound account key -> strict single-account routing + strict allowlist
	if expAcc, mode, isExplicit, invErr := am.ResolveExplicitAccount(r); isExplicit {
		if invErr != nil {
			return RoutePickResult{RouteMode: mode, IsExplicit: true, ReleaseFlight: noop, CommitSticky: noop, DenyErr: invErr, StatusCode: http.StatusForbidden}
		}
		if r != nil {
			if hdrGrp := strings.ToLower(strings.TrimSpace(r.Header.Get("X-Agy-Group"))); hdrGrp != "" {
				if grp, okG := am.GetGroup(hdrGrp); okG && canonicalModelID != "" && !grp.AllowsModel(canonicalModelID) {
					return RoutePickResult{
						Account: expAcc, GroupID: grp.ID, RouteMode: mode, IsExplicit: true, ReleaseFlight: noop, CommitSticky: noop,
						DenyErr:    fmt.Errorf("Account group %q does not allow model %q", grp.ID, canonicalModelID),
						StatusCode: http.StatusForbidden,
					}
				}
			}
		}
		if canonicalModelID != "" && !expAcc.AllowsModel(canonicalModelID) {
			allowedDesc := "无"
			if len(expAcc.AllowedModels) > 0 {
				allowedDesc = strings.Join(expAcc.AllowedModels, ", ")
			}
			return RoutePickResult{
				Account: expAcc, RouteMode: mode, IsExplicit: true, ReleaseFlight: noop, CommitSticky: noop,
				DenyErr:    fmt.Errorf("Account %q (%s) does not bind model %q (allowed: %s)", expAcc.ID, expAcc.Label, canonicalModelID, allowedDesc),
				StatusCode: http.StatusForbidden,
			}
		}
		if blocked, bState, reason, rSec := am.isAccountHardBlocked(expAcc.ID, now); blocked && (bState == "tos_tripped" || !am.isSinglePrimaryOnly()) {
			return RoutePickResult{
				Account: expAcc, RouteMode: mode, IsExplicit: true, ReleaseFlight: noop, CommitSticky: noop,
				DenyErr:       fmt.Errorf("Account %q is temporarily unavailable: %s", expAcc.ID, reason),
				StatusCode:    http.StatusServiceUnavailable,
				RetryAfterSec: rSec,
			}
		}
		if opts.HasTools && monitorTripEnabled() && !am.isSinglePrimaryOnly() {
			if tripUntil := MonitorTripUntil(expAcc.ID, now); !tripUntil.IsZero() {
				rs := int(tripUntil.Sub(now).Seconds()) + 1
				if rs < 1 {
					rs = 1
				}
				return RoutePickResult{
					Account: expAcc, RouteMode: mode + "-monitor-tripped", IsExplicit: true, ReleaseFlight: noop, CommitSticky: noop,
					DenyErr:       fmt.Errorf("Account %q safety-monitor capacity is exhausted until %s; retry after reset", expAcc.ID, tripUntil.UTC().Format(time.RFC3339)),
					StatusCode:    http.StatusTooManyRequests,
					RetryAfterSec: rs,
				}
			}
		}
		rel := noop
		if opts.ReserveInFlight && expAcc.ID != "" {
			rel = am.AcquireAccountInFlight(expAcc.ID)
		}
		return RoutePickResult{
			Account: expAcc, RouteMode: mode, IsExplicit: true, ReleaseFlight: rel, CommitSticky: noop,
		}
	}

	// 2. Optional Group / Pool scope (bound group key or X-Agy-Group header)
	grpScope, isGrpExplicit, grpErr := am.ResolveGroupScope(r)
	if isGrpExplicit && grpErr != nil {
		return RoutePickResult{RouteMode: "group-deny", ReleaseFlight: noop, CommitSticky: noop, DenyErr: grpErr, StatusCode: http.StatusForbidden}
	}
	grpID := ""
	if grpScope != nil {
		grpID = grpScope.ID
		if canonicalModelID != "" && !grpScope.AllowsModel(canonicalModelID) {
			return RoutePickResult{
				GroupID: grpID, RouteMode: "group-deny", ReleaseFlight: noop, CommitSticky: noop,
				DenyErr:    fmt.Errorf("Account group %q does not allow model %q", grpScope.ID, canonicalModelID),
				StatusCode: http.StatusForbidden,
			}
		}
	}

	if canonicalModelID == "" {
		return RoutePickResult{GroupID: grpID, ReleaseFlight: noop, CommitSticky: noop}
	}

	// Single RLock snapshot of routing config, eligible accounts, and child worker dashboard ports
	am.mu.RLock()
	routingCfg := am.routing.Normalized()
	boundEnabled := make([]AccountProfile, 0, len(am.accounts))
	dashPorts := make(map[string]int, len(am.accounts))
	for _, a := range am.accounts {
		if !a.Enabled || !a.AllowsModel(canonicalModelID) {
			continue
		}
		if grpScope != nil && !a.InGroup(grpScope.ID) {
			continue
		}
		boundEnabled = append(boundEnabled, a)
		if p := am.workers[a.ID]; p != nil && !p.stopped && p.dashPort > 0 {
			dashPorts[a.ID] = p.dashPort
		}
	}
	am.mu.RUnlock()

	if len(boundEnabled) == 0 {
		if grpScope != nil {
			return RoutePickResult{
				GroupID: grpID, RouteMode: "group-deny", ReleaseFlight: noop, CommitSticky: noop,
				DenyErr:    fmt.Errorf("No enabled account in group %q binds model %q", grpScope.ID, canonicalModelID),
				StatusCode: http.StatusForbidden,
			}
		}
		return RoutePickResult{
			RouteMode: "pool-deny", ReleaseFlight: noop, CommitSticky: noop,
			DenyErr:    fmt.Errorf("Model %q is not bound to any enabled account; bind it in /acp/ dashboard first", canonicalModelID),
			StatusCode: http.StatusForbidden,
		}
	}

	authed := make([]AccountProfile, 0, len(boundEnabled))
	authedByID := make(map[string]AccountProfile, len(boundEnabled))
	for _, a := range boundEnabled {
		if a.Primary || am.HasOAuthToken(a.ID) {
			authed = append(authed, a)
			authedByID[a.ID] = a
		}
	}
	if len(authed) == 0 {
		return RoutePickResult{
			Account: boundEnabled[0], GroupID: grpID, RouteMode: "pool-unauth", ReleaseFlight: noop, CommitSticky: noop,
			DenyErr:    fmt.Errorf("No authenticated account available for model %q; authorize one in /acp/ dashboard first", canonicalModelID),
			StatusCode: http.StatusServiceUnavailable,
		}
	}

	scopeKey := "global"
	effectiveStrategy := routingCfg.Strategy
	effectiveActiveID := routingCfg.ActiveAccountID
	allowManualFailover := routingCfg.ManualAutoFailoverEnabled()
	if grpScope != nil {
		scopeKey = "grp:" + grpScope.ID
		if grpScope.Strategy != "" {
			effectiveStrategy = grpScope.Strategy
		}
		if grpScope.ActiveAccountID != "" {
			if _, okG := authedByID[grpScope.ActiveAccountID]; okG {
				effectiveActiveID = grpScope.ActiveAccountID
			} else if !grpScope.EffectiveManualAutoFailover(allowManualFailover) {
				effectiveActiveID = grpScope.ActiveAccountID
			} else {
				effectiveActiveID = ""
			}
		} else if effectiveActiveID != "" {
			if _, okG := authedByID[effectiveActiveID]; !okG {
				effectiveActiveID = ""
			}
		}
		allowManualFailover = grpScope.EffectiveManualAutoFailover(allowManualFailover)
	}
	normStrat := normalizeRouteStrategy(effectiveStrategy)

	ttl := gwcfg.Current().IdleWorkerTtl
	if ttl < 15*time.Minute {
		ttl = 15 * time.Minute
	}
	if routingCfg.StickyTTLSec > 0 {
		ttl = time.Duration(routingCfg.StickyTTLSec) * time.Second
	}

	sessionKey := opts.SessionKey
	isContinuation := opts.IsContinuation
	compositeKey := ""
	if sessionKey != "" {
		callerScope := callerKeyScopeDigest(r)
		if grpScope != nil {
			compositeKey = callerScope + "|grp:" + grpScope.ID + "|" + sessionKey + "|" + canonicalModelID
		} else {
			compositeKey = callerScope + "|" + sessionKey + "|" + canonicalModelID
		}
	}

	// Single RLock snapshot of health & cooldowns across all authed candidates (zero lock nesting inside stickyMu).
	// When only the host primary account is configured (no multi-account pool), degrade-open on transient cooldown/auth_error
	// so single-account deployments never self-lockout after a model-specific #3501 or transient 429/529 (tos_tripped still halts).
	hardMap, coolMap := am.snapshotRoutingHealth(authed, canonicalModelID, now)
	if am.isSinglePrimaryOnly() {
		for id, hb := range hardMap {
			if hb.state != "tos_tripped" {
				delete(hardMap, id)
			}
		}
		clear(coolMap)
	}

	var idealScored []candidateScore
	var healthyAtCapScored []candidateScore
	var quotaExhaustedScored []candidateScore
	var monitorRedIdealScored []candidateScore
	var monitorRedAtCapScored []candidateScore
	var monitorRedExhaustedScored []candidateScore
	idealByID := make(map[string]bool, len(authed))
	idealScoreByID := make(map[string]candidateScore, len(authed))
	atCapByID := make(map[string]candidateScore, len(authed))
	exhaustedByID := make(map[string]candidateScore, len(authed))
	monitorRedByID := make(map[string]candidateScore, len(authed))
	exhaustedResetSec := make(map[string]int, len(authed))
	minCoolRetrySec := 0
	minHardBlockSec := 0
	lastCoolReason := ""
	lastHardBlockReason := ""
	activeNonHardBlocked := 0

	// Safety Monitor Governor v2:
	// - Zero-tool requests (!opts.HasTools) trigger ZERO sir_flash_lite calls and are NEVER blocked
	//   for monitor trips or red-zone soft caps (they even prefer yellow/red accounts with healthy
	//   main quota when AGY_MONITOR_BUDGET_SPREAD=1 to preserve green headroom for tool agents).
	// - Tool-bearing requests (opts.HasTools) enforce 3-zone routing (green spread, yellow soft-drain
	//   keeping U8 sticky pinned while sending new sessions to green, red protected + hop cancel,
	//   soft-red fallback before premature 429, and real 429 lockout -> 429 Retry-After).
	// - Single-primary deployments have nowhere to move, so they degrade open.
	monitorGovActive := monitorTripEnabled() && !am.isSinglePrimaryOnly()
	monitorToolGating := monitorGovActive && opts.HasTools
	monitorBudgetSpread := monitorGovActive && routingCfg.MonitorBudgetSpreadEnabled()
	monitorTripped := make(map[string]time.Time, len(authed))
	monitorSoftRed := make(map[string]MonitorBudgetSnapshot, len(authed))
	minTripRetrySec := 0

	for ord, a := range authed {
		if len(opts.ExcludeIDs) > 0 && opts.ExcludeIDs[a.ID] {
			continue
		}
		if a.EffectiveSchedState() != SchedStateActive {
			continue
		}
		if hb := hardMap[a.ID]; hb.blocked {
			lastHardBlockReason = hb.reason
			if hb.retrySec > 0 && (minHardBlockSec == 0 || hb.retrySec < minHardBlockSec) {
				minHardBlockSec = hb.retrySec
			}
			continue
		}
		mb := EvaluateMonitorBudget(a, now)
		if monitorToolGating && mb.Tripped {
			if tripUntil := MonitorTripUntil(a.ID, now); !tripUntil.IsZero() {
				monitorTripped[a.ID] = tripUntil
				rs := int(tripUntil.Sub(now).Seconds()) + 1
				if rs < 1 {
					rs = 1
				}
				if minTripRetrySec == 0 || rs < minTripRetrySec {
					minTripRetrySec = rs
				}
				continue
			}
		}
		activeNonHardBlocked++
		if ci := coolMap[a.ID]; ci.cooling {
			lastCoolReason = ci.reason
			if ci.retrySec > 0 && (minCoolRetrySec == 0 || ci.retrySec < minCoolRetrySec) {
				minCoolRetrySec = ci.retrySec
			}
			continue
		}
		exhausted, remFrac, resetAt := am.evaluateAccountQuota(a, dashPorts[a.ID], canonicalModelID, now)
		inFlight := am.AccountInFlight(a.ID)
		maxConc := a.EffectiveMaxConcurrency()
		effOccupied := am.EffectiveOccupiedSlots(a.ID, inFlight, maxConc, now)
		cs := candidateScore{
			acc:            a,
			priority:       a.EffectivePriority(),
			inFlight:       effOccupied,
			maxConc:        maxConc,
			remQuota:       remFrac,
			configOrd:      ord,
			monitorZone:    mb.Zone,
			monitorRem:     mb.RemainingRatio(),
			monitorCalls:   mb.Calls5h,
			monitorHardCap: mb.HardCap,
		}
		if monitorToolGating && !mb.Tripped && mb.Zone == monitorZoneRed {
			monitorSoftRed[a.ID] = mb
			monitorRedByID[a.ID] = cs
			if exhausted {
				monitorRedExhaustedScored = append(monitorRedExhaustedScored, cs)
				exhaustedByID[a.ID] = cs
				if !resetAt.IsZero() && resetAt.After(now) {
					exhaustedResetSec[a.ID] = int(resetAt.Sub(now).Seconds()) + 1
				}
			} else if effOccupied < int64(maxConc) {
				monitorRedIdealScored = append(monitorRedIdealScored, cs)
			} else {
				monitorRedAtCapScored = append(monitorRedAtCapScored, cs)
			}
			continue
		}
		if exhausted {
			quotaExhaustedScored = append(quotaExhaustedScored, cs)
			exhaustedByID[a.ID] = cs
			if !resetAt.IsZero() && resetAt.After(now) {
				exhaustedResetSec[a.ID] = int(resetAt.Sub(now).Seconds()) + 1
			}
			continue
		}
		if effOccupied < int64(maxConc) {
			idealScored = append(idealScored, cs)
			idealByID[a.ID] = true
			idealScoreByID[a.ID] = cs
		} else {
			healthyAtCapScored = append(healthyAtCapScored, cs)
			atCapByID[a.ID] = cs
		}
	}

	hasGreenOrYellowHealthyPeer := len(idealScored) > 0 || len(healthyAtCapScored) > 0
	var hopCancelAccID, hopCancelReason string
	defer func() {
		if hopCancelAccID != "" {
			am.cancelHoppedSession(hopCancelAccID, sessionKey, opts.ToolResultCallIDs, hopCancelReason)
		}
	}()

	am.stickyMu.Lock()
	defer am.stickyMu.Unlock()

	if now.Sub(am.stickyLastPrune) >= time.Minute {
		am.stickyLastPrune = now
		am.stickyLRU.pruneExpired(now, ttl)
	}

	// 3. Sticky Session Lookup (enforcing Decision U8 anti-ban continuity on multi-turn continuations,
	// while ensuring brand-new 1st-turn fp: conversations distribute across the pool and respect manual switches).
	isFingerprintOnly := strings.HasPrefix(sessionKey, "fp:")
	shouldLookupSticky := compositeKey != "" && len(opts.ExcludeIDs) == 0 && (isContinuation || !isFingerprintOnly)
	if shouldLookupSticky {
		if entry, ok := am.stickyLRU.getFresh(compositeKey, now, ttl); ok {
			if stickyAcc, okA := authedByID[entry.accountID]; okA {
				hb := hardMap[stickyAcc.ID]
				ci := coolMap[stickyAcc.ID]
				tripUntil, stickyTripped := monitorTripped[stickyAcc.ID]
				softRedSnap, stickySoftRed := monitorSoftRed[stickyAcc.ID]
				shouldHopTrip := stickyTripped && monitorTripHopEnabled()
				shouldHopSoftRed := !stickyTripped && stickySoftRed && monitorTripHopEnabled() && hasGreenOrYellowHealthyPeer
				if shouldHopTrip {
					// Deliberate, budget-driven exception to U8: this account's safety monitor is capacity-locked,
					// every further tool call costs ~80x. Drop the pin and cancel the old parked session so the
					// request re-routes cleanly without leaving zombie ws_await_result relays burning quota.
					LogEvent("MONITOR_TRIP_HOP", "Re-routing conversation away from monitor-tripped account", map[string]any{
						"accountId":    stickyAcc.ID,
						"until":        tripUntil.UTC().Format(time.RFC3339),
						"continuation": isContinuation,
						"reason":       "monitor_tripped",
					})
					am.stickyLRU.deleteKey(compositeKey)
					hopCancelAccID = stickyAcc.ID
					hopCancelReason = "monitor_trip_hop_cancel"
				} else if shouldHopSoftRed {
					LogEvent("MONITOR_TRIP_HOP", "Re-routing conversation away from monitor red-zone account to healthy peer", map[string]any{
						"accountId":    stickyAcc.ID,
						"calls5h":      softRedSnap.Calls5h,
						"drainCap":     softRedSnap.DrainCap,
						"continuation": isContinuation,
						"reason":       "monitor_drain_cap",
					})
					am.stickyLRU.deleteKey(compositeKey)
					hopCancelAccID = stickyAcc.ID
					hopCancelReason = "monitor_drain_hop_cancel"
				} else if hb.blocked {
					if isContinuation && routingCfg.StrictU8Enabled() {
						return RoutePickResult{
							Account: stickyAcc, GroupID: grpID, RouteMode: "pool-sticky-blocked", ReleaseFlight: noop, CommitSticky: noop,
							DenyErr:       fmt.Errorf("Account %q bound to this active conversation is unavailable (%s); start a new conversation to switch accounts", stickyAcc.ID, hb.reason),
							StatusCode:    http.StatusServiceUnavailable,
							RetryAfterSec: hb.retrySec,
						}
					}
					am.stickyLRU.deleteKey(compositeKey)
				} else if isContinuation && routingCfg.StrictU8Enabled() {
					// U8 hard invariant: active multi-turn conversations stay pinned to their original OAuth account
					// (including in Green & Yellow monitor zones, during graceful draining, or transient cooldown).
					am.stickyLRU.touch(compositeKey, now)
					rel := noop
					if opts.ReserveInFlight {
						rel = am.AcquireAccountInFlight(stickyAcc.ID)
					}
					mode := "pool-sticky"
					if grpScope != nil {
						mode = "group-sticky"
					}
					return RoutePickResult{
						Account: stickyAcc, GroupID: grpID, RouteMode: mode, IsStickyHit: true, ReleaseFlight: rel, CommitSticky: noop,
					}
				} else if normStrat == RouteStrategyManualActive && effectiveActiveID != "" && stickyAcc.ID != effectiveActiveID && (idealByID[effectiveActiveID] || !allowManualFailover) {
					// Brand-new 1st-turn session after operator switched manual active account -> switch cleanly.
					am.stickyLRU.deleteKey(compositeKey)
				} else if idealByID[stickyAcc.ID] || (!stickyTripped && !stickySoftRed && !ci.cooling && len(idealScored) == 0 && len(healthyAtCapScored) == 0 && stickyAcc.EffectiveSchedState() == SchedStateActive) {
					am.stickyLRU.touch(compositeKey, now)
					rel := noop
					if opts.ReserveInFlight {
						rel = am.AcquireAccountInFlight(stickyAcc.ID)
					}
					mode := "pool-sticky"
					if grpScope != nil {
						mode = "group-sticky"
					}
					return RoutePickResult{
						Account: stickyAcc, GroupID: grpID, RouteMode: mode, IsStickyHit: true, ReleaseFlight: rel, CommitSticky: noop,
					}
				} else {
					// Brand-new 1st-turn request whose prior sticky account is draining, cooling, tripped, red, exhausted, or full -> safely re-pick.
					am.stickyLRU.deleteKey(compositeKey)
				}
			} else {
				if isContinuation && routingCfg.StrictU8Enabled() {
					return RoutePickResult{
						Account: AccountProfile{ID: entry.accountID}, GroupID: grpID, RouteMode: "pool-sticky-orphaned", ReleaseFlight: noop, CommitSticky: noop,
						DenyErr:    fmt.Errorf("Account %q bound to this active conversation is disabled or unavailable; start a new conversation to route to another account", entry.accountID),
						StatusCode: http.StatusServiceUnavailable,
					}
				}
				am.stickyLRU.deleteKey(compositeKey)
			}
		}
	}

	// 4. Enforce strict manual-active lock (ManualAutoFailover == false) before general pool selection
	var pool []candidateScore
	manualLockedActive := normStrat == RouteStrategyManualActive && effectiveActiveID != "" && !allowManualFailover
	if manualLockedActive {
		if c, okI := idealScoreByID[effectiveActiveID]; okI {
			pool = []candidateScore{c}
		} else if c, okC := atCapByID[effectiveActiveID]; okC {
			pool = []candidateScore{c}
		} else if c, okR := monitorRedByID[effectiveActiveID]; okR {
			// Soft-red without actual 429 lockout degrades open rather than premature 429.
			pool = []candidateScore{c}
		} else if _, okE := exhaustedByID[effectiveActiveID]; okE {
			return RoutePickResult{
				Account: AccountProfile{ID: effectiveActiveID}, GroupID: grpID, RouteMode: "pool-manual-locked-quota",
				ReleaseFlight: noop, CommitSticky: noop,
				DenyErr:       fmt.Errorf("Manual active account %q 5h quota is exhausted for model %q and auto-failover is disabled", effectiveActiveID, canonicalModelID),
				StatusCode:    http.StatusTooManyRequests,
				RetryAfterSec: exhaustedResetSec[effectiveActiveID],
			}
		} else if tripUntil, okT := monitorTripped[effectiveActiveID]; okT {
			rs := int(tripUntil.Sub(now).Seconds()) + 1
			if rs < 1 {
				rs = 1
			}
			return RoutePickResult{
				Account: AccountProfile{ID: effectiveActiveID}, GroupID: grpID, RouteMode: "pool-manual-locked-monitor",
				ReleaseFlight: noop, CommitSticky: noop,
				DenyErr:       fmt.Errorf("Manual active account %q safety-monitor capacity is exhausted until %s and auto-failover is disabled", effectiveActiveID, tripUntil.UTC().Format(time.RFC3339)),
				StatusCode:    http.StatusTooManyRequests,
				RetryAfterSec: rs,
			}
		} else if ci := coolMap[effectiveActiveID]; ci.cooling {
			return RoutePickResult{
				Account: AccountProfile{ID: effectiveActiveID}, GroupID: grpID, RouteMode: "pool-manual-locked-cooldown",
				ReleaseFlight: noop, CommitSticky: noop,
				DenyErr:       fmt.Errorf("Manual active account %q is in cooldown (%s) and auto-failover is disabled", effectiveActiveID, ci.reason),
				StatusCode:    http.StatusTooManyRequests,
				RetryAfterSec: ci.retrySec,
			}
		} else {
			reason := "draining, disabled, excluded, or unauthenticated"
			rSec := 0
			if hb := hardMap[effectiveActiveID]; hb.blocked {
				reason = hb.reason
				rSec = hb.retrySec
			}
			return RoutePickResult{
				Account: AccountProfile{ID: effectiveActiveID}, GroupID: grpID, RouteMode: "pool-manual-locked-unavailable",
				ReleaseFlight: noop, CommitSticky: noop,
				DenyErr:       fmt.Errorf("Manual active account %q is unavailable (%s) and auto-failover is disabled", effectiveActiveID, reason),
				StatusCode:    http.StatusServiceUnavailable,
				RetryAfterSec: rSec,
			}
		}
	} else {
		switch {
		case len(idealScored) > 0:
			pool = idealScored
			// In manual-active mode, keep activeAccountID eligible if it is healthy at soft concurrency cap (< 2x maxConc)
			// unless it recently returned a local worker-slot 529 (allow Turn-1 spillover to healthy idle peers).
			if normStrat == RouteStrategyManualActive && effectiveActiveID != "" && !idealByID[effectiveActiveID] && !am.HasActiveLocalSlotBusy(effectiveActiveID, now) {
				if capCand, okCap := atCapByID[effectiveActiveID]; okCap && capCand.inFlight < int64(maxInt(capCand.maxConc, 1)*2) {
					pool = append([]candidateScore{capCand}, pool...)
				}
			}
		case len(healthyAtCapScored) > 0:
			pool = healthyAtCapScored
		case len(monitorRedIdealScored) > 0:
			// No Green/Yellow peer exists and only soft-drain cap was hit without actual 429 lockout -> fallback before 429.
			pool = monitorRedIdealScored
		case len(monitorRedAtCapScored) > 0:
			pool = monitorRedAtCapScored
		case len(quotaExhaustedScored) > 0:
			pool = quotaExhaustedScored
		case len(monitorRedExhaustedScored) > 0:
			pool = monitorRedExhaustedScored
		default:
			hasCooling := activeNonHardBlocked > 0 && minCoolRetrySec > 0
			hasTripped := len(monitorTripped) > 0
			// Denial precedence: return whichever recovers sooner (cooldown wins ties).
			if hasCooling && (!hasTripped || minCoolRetrySec <= minTripRetrySec) {
				msg := fmt.Sprintf("All eligible accounts for model %q are temporarily in rate-limit/overload cooldown", canonicalModelID)
				if lastCoolReason != "" {
					msg += ": " + lastCoolReason
				}
				return RoutePickResult{
					GroupID: grpID, RouteMode: "pool-cooldown", ReleaseFlight: noop, CommitSticky: noop,
					DenyErr: errors.New(msg), StatusCode: http.StatusTooManyRequests, RetryAfterSec: minCoolRetrySec,
				}
			}
			if hasTripped {
				return RoutePickResult{
					GroupID: grpID, RouteMode: "pool-monitor-tripped", ReleaseFlight: noop, CommitSticky: noop,
					DenyErr:       fmt.Errorf("All eligible accounts for model %q have exhausted their safety-monitor capacity; further tool calls would drain the quota. Retry after the reset", canonicalModelID),
					StatusCode:    http.StatusTooManyRequests,
					RetryAfterSec: minTripRetrySec,
				}
			}
			msg := fmt.Sprintf("No active healthy account available for model %q (all accounts draining or unavailable)", canonicalModelID)
			if lastHardBlockReason != "" {
				msg += ": " + lastHardBlockReason
			}
			return RoutePickResult{
				GroupID: grpID, RouteMode: "pool-unavailable", ReleaseFlight: noop, CommitSticky: noop,
				DenyErr: errors.New(msg), StatusCode: http.StatusServiceUnavailable, RetryAfterSec: minHardBlockSec,
			}
		}
	}

	// Multi-account pool governance & 3-Zone candidate shaping for non-manual-locked selections
	// (explicit X-Agy-Account / bound keys and healthy manual-active activeAccountId remain unrestricted):
	manualActiveInPool := false
	if normStrat == RouteStrategyManualActive && effectiveActiveID != "" {
		for _, c := range pool {
			if c.acc.ID == effectiveActiveID {
				manualActiveInPool = true
				break
			}
		}
	}
	selectWinningTier := func(tiers [][]candidateScore, spillOnYellow bool) []candidateScore {
		if len(tiers) <= 1 {
			if len(tiers) == 1 {
				return tiers[0]
			}
			return nil
		}
		if opts.HasTools && monitorToolGating && spillOnYellow {
			for _, tier := range tiers {
				var greenInTier []candidateScore
				for _, c := range tier {
					if c.monitorZone == monitorZoneGreen {
						greenInTier = append(greenInTier, c)
					}
				}
				if len(greenInTier) > 0 {
					return greenInTier
				}
			}
		}
		return tiers[0]
	}

	if !manualLockedActive && !manualActiveInPool && len(pool) > 1 {
		spillOnYellow := routingCfg.MonitorSpillOnYellowEnabled()

		// Step 1: Standby separation (always active when any account has role: "standby").
		var primaryPool, standbyPool []candidateScore
		for _, c := range pool {
			if c.acc.IsStandby() {
				standbyPool = append(standbyPool, c)
			} else {
				primaryPool = append(primaryPool, c)
			}
		}
		if len(primaryPool) > 0 && len(standbyPool) > 0 {
			pool = selectWinningTier([][]candidateScore{primaryPool, standbyPool}, spillOnYellow)
		}

		// Step 2: Optional PriorityTiering across ALL strategies (round-robin, weighted-rr, least-load, fill-first).
		if routingCfg.PriorityTieringEnabled() && len(pool) > 1 {
			tiersByPrio := make(map[int][]candidateScore)
			var prios []int
			for _, c := range pool {
				if _, seen := tiersByPrio[c.priority]; !seen {
					prios = append(prios, c.priority)
				}
				tiersByPrio[c.priority] = append(tiersByPrio[c.priority], c)
			}
			if len(prios) > 1 {
				sort.Ints(prios)
				tiers := make([][]candidateScore, 0, len(prios))
				for _, p := range prios {
					tiers = append(tiers, tiersByPrio[p])
				}
				pool = selectWinningTier(tiers, spillOnYellow)
			}
		}
	}

	// Step 3: 3-Zone candidate shaping within the winning tier:
	// - Tool-bearing requests: stop assigning NEW sessions to Yellow accounts whenever any Green peer exists.
	// - Zero-tool requests with monitorBudgetSpread enabled: slightly prefer Yellow/Red accounts whose main
	//   quota is healthy (remQuota >= 0.15) so they productively burn main quota while saving Green headroom.
	if monitorToolGating && !manualLockedActive && !manualActiveInPool && len(pool) > 1 {
		var greenOnly []candidateScore
		for _, c := range pool {
			if c.monitorZone == monitorZoneGreen {
				greenOnly = append(greenOnly, c)
			}
		}
		if len(greenOnly) > 0 && len(greenOnly) < len(pool) {
			pool = greenOnly
		}
	} else if !opts.HasTools && monitorBudgetSpread && !manualLockedActive && normStrat != RouteStrategyManualActive && len(pool) > 1 && len(idealScored) > 0 {
		var warmHealthy []candidateScore
		for _, c := range pool {
			if (c.monitorZone == monitorZoneRed || c.monitorZone == monitorZoneYellow) && c.remQuota >= 0.15 {
				warmHealthy = append(warmHealthy, c)
			}
		}
		if len(warmHealthy) > 0 && len(warmHealthy) < len(pool) {
			pool = warmHealthy
		}
	}

	spreadTools := opts.HasTools && monitorBudgetSpread && !isContinuation
	preferWarmNoTools := !opts.HasTools && monitorBudgetSpread && normStrat != RouteStrategyManualActive
	chosen, mode := selectAccountByStrategyWithMonitor(effectiveStrategy, effectiveActiveID, scopeKey, canonicalModelID, pool, am.rrCounters, am.swrrStates, spreadTools, preferWarmNoTools)
	if grpScope != nil && strings.HasPrefix(mode, "pool-") {
		mode = "group-" + strings.TrimPrefix(mode, "pool-")
	}

	rel := noop
	if opts.ReserveInFlight {
		rel = am.AcquireAccountInFlight(chosen.ID)
	}

	maxSticky := routingCfg.StickyMaxEntries
	commitFn := noop
	if compositeKey != "" {
		if opts.DeferStickyCommit {
			chosenID := chosen.ID
			commitFn = func() {
				am.stickyMu.Lock()
				am.stickyLRU.put(compositeKey, chosenID, canonicalModelID, time.Now(), ttl, maxSticky)
				am.stickyMu.Unlock()
			}
		} else {
			am.stickyLRU.put(compositeKey, chosen.ID, canonicalModelID, now, ttl, maxSticky)
		}
	}
	return RoutePickResult{
		Account:       chosen,
		GroupID:       grpID,
		RouteMode:     mode,
		ReleaseFlight: rel,
		CommitSticky:  commitFn,
	}
}

// FilterModelsForRequest filters /v1/models output strictly by the caller's bound account, group, or pool allowlists.
func (am *AccountManager) FilterModelsForRequest(r *http.Request, all []map[string]any) (filtered []map[string]any, accountID string) {
	if isChildAccountWorker() || !am.HasConfiguredAccounts() {
		return all, ""
	}
	return am.filterModelItems(r, all, "id")
}

// FilterGeminiModelsForRequest filters /v1beta/models output strictly by the caller's bound account, group, or pool allowlists.
func (am *AccountManager) FilterGeminiModelsForRequest(r *http.Request, all []map[string]any) (filtered []map[string]any, accountID string) {
	if isChildAccountWorker() || !am.HasConfiguredAccounts() {
		return all, ""
	}
	return am.filterModelItems(r, all, "name")
}

func (am *AccountManager) filterModelItems(r *http.Request, all []map[string]any, idField string) (filtered []map[string]any, accountID string) {
	am.ensureInitialized()
	extractCID := func(item map[string]any) string {
		idStr, _ := item[idField].(string)
		idStr = strings.TrimPrefix(idStr, "models/")
		return ResolveRequestedModel(idStr, nil).Model.ID
	}
	if expAcc, _, isExplicit, invErr := am.ResolveExplicitAccount(r); isExplicit {
		if invErr != nil {
			return []map[string]any{}, ""
		}
		var optGrp *AccountGroup
		if r != nil {
			if hdrGrp := strings.ToLower(strings.TrimSpace(r.Header.Get("X-Agy-Group"))); hdrGrp != "" {
				if g, okG := am.GetGroup(hdrGrp); okG {
					optGrp = &g
				}
			}
		}
		out := make([]map[string]any, 0, len(all))
		for _, item := range all {
			cid := extractCID(item)
			if expAcc.AllowsModel(cid) && (optGrp == nil || optGrp.AllowsModel(cid)) {
				out = append(out, item)
			}
		}
		return out, expAcc.ID
	}
	grpScope, isGrpExplicit, grpErr := am.ResolveGroupScope(r)
	if isGrpExplicit && grpErr != nil {
		return []map[string]any{}, ""
	}

	am.mu.RLock()
	enabled := make([]AccountProfile, 0, len(am.accounts))
	for _, a := range am.accounts {
		if a.Enabled && (grpScope == nil || a.InGroup(grpScope.ID)) {
			enabled = append(enabled, a)
		}
	}
	am.mu.RUnlock()

	out := make([]map[string]any, 0, len(all))
	for _, item := range all {
		cid := extractCID(item)
		if grpScope != nil && !grpScope.AllowsModel(cid) {
			continue
		}
		for _, a := range enabled {
			if a.AllowsModel(cid) {
				out = append(out, item)
				break
			}
		}
	}
	return out, ""
}

func (am *AccountManager) HasConfiguredAccounts() bool {
	am.ensureInitialized()
	am.mu.RLock()
	defer am.mu.RUnlock()
	return len(am.accounts) > 0
}

func (am *AccountManager) HasNonPrimaryAccounts() bool {
	am.ensureInitialized()
	am.mu.RLock()
	defer am.mu.RUnlock()
	for _, a := range am.accounts {
		if !a.Primary && a.Enabled {
			return true
		}
	}
	return false
}

func extractSessionKeyFromHeaders(r *http.Request) string {
	if r == nil {
		return ""
	}
	for _, hdr := range []string{
		"X-Session-Id",
		"X-Pi-Session-Id",
		"OpenAI-Conversation-Id",
		"X-Client-Session-Id",
		"Thread-Id",
		"X-Claude-Code-Session-Id",
		"X-Claude-Session-Id",
		"X-Conversation-Id",
		"X-Thread-Id",
		"Session-Id",
		"Session_Id",
		"Conversation-Id",
		"Conversation_Id",
		"Anthropic-Session-Id",
	} {
		if v := strings.TrimSpace(r.Header.Get(hdr)); v != "" {
			return "hdr:" + v
		}
	}
	return ""
}

func extractArrayFirstAndHasSecond(raw json.RawMessage) (first json.RawMessage, hasSecond bool) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) < 2 || trimmed[0] != '[' {
		return nil, false
	}
	dec := json.NewDecoder(bytes.NewReader(trimmed))
	tok, err := dec.Token()
	if err != nil || tok != json.Delim('[') || !dec.More() {
		return nil, false
	}
	if err := dec.Decode(&first); err != nil {
		return nil, false
	}
	return first, dec.More()
}

func extractFirstJSONArrayElement(raw json.RawMessage) json.RawMessage {
	first, _ := extractArrayFirstAndHasSecond(raw)
	return first
}

// inspectConversationTurns examines a messages/contents/input JSON array so Turn-1 requests carrying
// leading system/developer prompts ([system, user]) are accurately recognized as Turn-1 (isContinuation=false)
// and fingerprinted using BOTH leading system items AND the first user turn (matching Turn 2+ of the same thread).
func inspectConversationTurns(raw json.RawMessage) (sysLead, firstUser json.RawMessage, isContinuation bool) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) < 2 || trimmed[0] != '[' {
		return nil, nil, false
	}
	dec := json.NewDecoder(bytes.NewReader(trimmed))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('[') {
		return nil, nil, false
	}
	var sysBuf []byte
	userCount := 0
	for idx := 0; dec.More() && idx < 64; idx++ {
		var elem json.RawMessage
		if err := dec.Decode(&elem); err != nil {
			break
		}
		eTrim := bytes.TrimSpace(elem)
		if len(eTrim) == 0 {
			continue
		}
		if eTrim[0] != '{' {
			if len(firstUser) == 0 {
				firstUser = elem
			}
			userCount++
			if userCount >= 2 {
				isContinuation = true
				break
			}
			continue
		}
		var lite struct {
			Role    string          `json:"role"`
			Type    string          `json:"type"`
			Content json.RawMessage `json:"content"`
			Parts   json.RawMessage `json:"parts"`
		}
		if json.Unmarshal(eTrim, &lite) != nil {
			if len(firstUser) == 0 {
				firstUser = elem
			}
			continue
		}
		role := strings.ToLower(strings.TrimSpace(lite.Role))
		typ := strings.ToLower(strings.TrimSpace(lite.Type))
		switch role {
		case "system", "developer":
			if len(firstUser) == 0 && len(sysBuf) < 512 {
				sysBuf = append(sysBuf, clipBytes(eTrim, 512-len(sysBuf))...)
			}
			continue
		case "assistant", "model", "tool", "function":
			isContinuation = true
		}
		switch typ {
		case "function_call", "function_call_output", "custom_tool_call", "custom_tool_call_output", "computer_call_output", "reasoning", "item_reference":
			isContinuation = true
		}
		if role == "user" || (role == "" && typ == "message") || (role == "" && len(lite.Parts) > 0) {
			userCount++
			if len(firstUser) == 0 {
				firstUser = elem
			}
			if userCount >= 2 {
				isContinuation = true
			}
			if bytes.Contains(lite.Content, []byte(`"tool_result"`)) || bytes.Contains(lite.Parts, []byte(`"functionResponse"`)) || bytes.Contains(lite.Parts, []byte(`"functionCall"`)) {
				isContinuation = true
			}
		}
		if isContinuation && len(firstUser) > 0 {
			break
		}
	}
	if len(firstUser) == 0 && len(sysBuf) > 0 {
		firstUser = sysBuf
		sysBuf = nil
	}
	return sysBuf, firstUser, isContinuation
}

func rawJSONString(raw json.RawMessage) string {
	var s string
	if len(raw) > 0 && json.Unmarshal(raw, &s) == nil {
		return strings.TrimSpace(s)
	}
	return ""
}

func clipBytes(b []byte, maxLen int) []byte {
	b = bytes.TrimSpace(b)
	if len(b) > maxLen {
		return b[:maxLen]
	}
	return b
}

func cleanGeminiV1BetaPath(rawPath string) string {
	clean, _ := extractGeminiPathKey(rawPath)
	if clean != "" {
		return clean
	}
	return rawPath
}

func (am *AccountManager) isSinglePrimaryOnly() bool {
	am.mu.RLock()
	defer am.mu.RUnlock()
	return len(am.accounts) == 1 && len(am.groups) == 0 && am.accounts[0].Primary
}

func (am *AccountManager) isSimpleSinglePrimary(r *http.Request) (string, bool) {
	if r != nil {
		if strings.TrimSpace(r.Header.Get("X-Agy-Account")) != "" || strings.TrimSpace(r.Header.Get("X-Agy-Group")) != "" {
			return "", false
		}
		if rawKey := requestKey(r); strings.ContainsAny(rawKey, "@#") {
			return "", false
		}
	}
	am.mu.RLock()
	if len(am.accounts) != 1 || len(am.groups) > 0 {
		am.mu.RUnlock()
		return "", false
	}
	a := am.accounts[0]
	am.mu.RUnlock()
	if !a.Primary || a.EffectiveSchedState() != SchedStateActive || len(a.BoundKeys) > 0 {
		return "", false
	}
	for _, m := range AgyModels {
		if !a.AllowsModel(m.ID) {
			return "", false
		}
	}
	// Single-primary fast path delegates ToS circuit-breaker responses directly to protocol handlers
	// so /v1/messages, /v1/chat/completions, /v1/responses, and /v1beta emit native tos_circuit_breaker envelopes.
	return a.ID, true
}

type requestRoutingHints struct {
	canonicalModel    string
	sessionKey        string
	isContinuation    bool
	isNonUpstream     bool
	invalidPathModel  bool
	hasTools          bool
	toolResultCallIDs []string
	rawBody           []byte
	malformedJSON     bool
	tooLarge          bool
	readErr           error
}

func jsonRawHasNonEmptyComposite(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return false
	}
	switch trimmed[0] {
	case '[':
		var arr []json.RawMessage
		return json.Unmarshal(trimmed, &arr) == nil && len(arr) > 0
	case '{':
		var obj map[string]json.RawMessage
		return json.Unmarshal(trimmed, &obj) == nil && len(obj) > 0
	default:
		return false
	}
}

func jsonToolsFieldHasTools(raw json.RawMessage, isGemini bool) bool {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) < 2 || trimmed[0] != '[' {
		return false
	}
	var items []json.RawMessage
	if json.Unmarshal(trimmed, &items) != nil || len(items) == 0 {
		return false
	}
	if !isGemini {
		return true
	}
	for _, item := range items {
		var obj map[string]json.RawMessage
		if json.Unmarshal(item, &obj) != nil || len(obj) == 0 {
			continue
		}
		for k, v := range obj {
			if k == "functionDeclarations" || k == "function_declarations" {
				if jsonRawHasNonEmptyComposite(v) {
					return true
				}
				continue
			}
			vt := bytes.TrimSpace(v)
			if len(vt) > 0 && !bytes.Equal(vt, []byte("null")) && (vt[0] != '[' || jsonRawHasNonEmptyComposite(vt)) {
				return true
			}
		}
	}
	return false
}

// inspectConversationToolsAndCallIDs inspects messages/contents/input structurally for
// tool-result continuations (across Anthropic, OpenAI Chat, OpenAI Responses, and Gemini)
// and collects recent tool result call IDs so MONITOR_TRIP_HOP can cancel exact parked sessions
// even for headerless (fp:) clients.
func inspectConversationToolsAndCallIDs(raw json.RawMessage) (hasToolContinuation bool, callIDs []string) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) < 2 || trimmed[0] != '[' {
		return false, nil
	}
	var items []json.RawMessage
	if json.Unmarshal(trimmed, &items) != nil || len(items) == 0 {
		return false, nil
	}
	seenID := make(map[string]bool)
	addCallID := func(id string) {
		id = strings.TrimSpace(id)
		if id != "" && !seenID[id] && len(callIDs) < 16 {
			seenID[id] = true
			callIDs = append(callIDs, id)
		}
	}
	// Scan from the tail so recent tool_result continuations and active parked call_ids are found first.
	// Cap full scan at 64 tail items (plus checking up to 64 head items only if not yet found) to stay O(1)-bounded on huge histories.
	scanItem := func(item json.RawMessage, collectIDs bool) {
		it := bytes.TrimSpace(item)
		if len(it) < 2 || it[0] != '{' {
			return
		}
		var lite struct {
			Role         string          `json:"role"`
			Type         string          `json:"type"`
			ToolCallID   string          `json:"tool_call_id"`
			CallID       string          `json:"call_id"`
			ToolCalls    json.RawMessage `json:"tool_calls"`
			FunctionCall json.RawMessage `json:"function_call"`
			Content      json.RawMessage `json:"content"`
			Parts        json.RawMessage `json:"parts"`
		}
		if json.Unmarshal(it, &lite) != nil {
			return
		}
		role := strings.ToLower(strings.TrimSpace(lite.Role))
		typ := strings.ToLower(strings.TrimSpace(lite.Type))
		if role == "tool" || role == "function" {
			hasToolContinuation = true
			if collectIDs {
				addCallID(lite.ToolCallID)
				addCallID(lite.CallID)
			}
		}
		switch typ {
		case "function_call", "function_call_output", "custom_tool_call", "custom_tool_call_output", "computer_call", "computer_call_output":
			hasToolContinuation = true
			if collectIDs && strings.HasSuffix(typ, "_output") {
				addCallID(lite.CallID)
			}
		}
		if jsonRawHasNonEmptyComposite(lite.ToolCalls) || jsonRawHasNonEmptyComposite(lite.FunctionCall) {
			hasToolContinuation = true
		}
		if lite.ToolCallID != "" {
			hasToolContinuation = true
			if collectIDs {
				addCallID(lite.ToolCallID)
			}
		}
		if ct := bytes.TrimSpace(lite.Content); len(ct) > 2 && ct[0] == '[' &&
			(bytes.Contains(ct, []byte(`"tool_result"`)) || bytes.Contains(ct, []byte(`"tool_use"`))) {
			var blocks []struct {
				Type      string `json:"type"`
				ToolUseID string `json:"tool_use_id"`
			}
			if json.Unmarshal(ct, &blocks) == nil {
				for _, b := range blocks {
					bt := strings.ToLower(strings.TrimSpace(b.Type))
					if bt == "tool_result" || bt == "tool_use" {
						hasToolContinuation = true
						if collectIDs && bt == "tool_result" {
							addCallID(b.ToolUseID)
						}
					}
				}
			}
		}
		if pt := bytes.TrimSpace(lite.Parts); len(pt) > 2 && pt[0] == '[' &&
			(bytes.Contains(pt, []byte(`"functionResponse"`)) || bytes.Contains(pt, []byte(`"function_response"`)) ||
				bytes.Contains(pt, []byte(`"functionCall"`)) || bytes.Contains(pt, []byte(`"function_call"`))) {
			var parts []struct {
				FunctionResponse *struct {
					ID string `json:"id"`
				} `json:"functionResponse"`
				FunctionResponseSnake *struct {
					ID string `json:"id"`
				} `json:"function_response"`
				FunctionCall      json.RawMessage `json:"functionCall"`
				FunctionCallSnake json.RawMessage `json:"function_call"`
			}
			if json.Unmarshal(pt, &parts) == nil {
				for _, p := range parts {
					if p.FunctionResponse != nil {
						hasToolContinuation = true
						if collectIDs {
							addCallID(p.FunctionResponse.ID)
						}
					}
					if p.FunctionResponseSnake != nil {
						hasToolContinuation = true
						if collectIDs {
							addCallID(p.FunctionResponseSnake.ID)
						}
					}
					if jsonRawHasNonEmptyComposite(p.FunctionCall) || jsonRawHasNonEmptyComposite(p.FunctionCallSnake) {
						hasToolContinuation = true
					}
				}
			}
		}
	}

	tailStart := len(items) - 16
	if tailStart < 0 {
		tailStart = 0
	}
	for i := len(items) - 1; i >= tailStart; i-- {
		scanItem(items[i], true)
	}
	if !hasToolContinuation && tailStart > 0 {
		scanStart := tailStart - 48
		if scanStart < 0 {
			scanStart = 0
		}
		for i := tailStart - 1; i >= scanStart && !hasToolContinuation; i-- {
			scanItem(items[i], false)
		}
	}
	return hasToolContinuation, callIDs
}

func extractRequestRoutingHints(r *http.Request) (canonicalModel, sessionKey string, rawBody []byte, malformedJSON, tooLarge bool, readErr error) {
	canonicalModel, sessionKey, _, _, _, rawBody, malformedJSON, tooLarge, readErr = extractRequestRoutingHintsFull(r)
	return
}

func extractRequestRoutingHintsDetailed(r *http.Request) (canonicalModel, sessionKey string, isContinuation bool, rawBody []byte, malformedJSON, tooLarge bool, readErr error) {
	canonicalModel, sessionKey, isContinuation, _, _, rawBody, malformedJSON, tooLarge, readErr = extractRequestRoutingHintsFull(r)
	return
}

// extractRequestRoutingHintsFull parses model, session key, continuation hint, and Gemini path action
// with 100% parser parity against handleGeminiV1Beta so model allowlists cannot be bypassed via colon-stuffing.
func extractRequestRoutingHintsFull(r *http.Request) (
	canonicalModel, sessionKey string,
	isContinuation, isNonUpstream, invalidPathModel bool,
	rawBody []byte,
	malformedJSON, tooLarge bool,
	readErr error,
) {
	h := extractRequestRoutingHintsV2(r)
	return h.canonicalModel, h.sessionKey, h.isContinuation, h.isNonUpstream, h.invalidPathModel, h.rawBody, h.malformedJSON, h.tooLarge, h.readErr
}

func extractRequestRoutingHintsV2(r *http.Request) (h requestRoutingHints) {
	if r == nil || r.URL == nil {
		return h
	}
	h.sessionKey = extractSessionKeyFromHeaders(r)
	p := cleanGeminiV1BetaPath(r.URL.Path)
	isGemini := strings.HasPrefix(p, "/v1beta/models/")
	isImageGen := r.Method == http.MethodPost && (p == "/v1/images/generations" || p == "/v1/images/edits")
	if p == "/v1/messages/count_tokens" {
		h.isNonUpstream = true
	}
	if isImageGen {
		h.canonicalModel = imageGenOuterModel
		h.hasTools = false
	}
	if isGemini {
		rest := strings.TrimPrefix(p, "/v1beta/models/")
		if r.Method == http.MethodGet {
			h.isNonUpstream = true
			rest = strings.TrimSpace(strings.TrimPrefix(rest, "models/"))
			if rest != "" && !strings.ContainsAny(rest, ":/") {
				h.canonicalModel = ResolveRequestedModel(rest, nil).Model.ID
			}
		} else if r.Method == http.MethodPost {
			colon := strings.LastIndexByte(rest, ':')
			if colon > 0 {
				rawModel := strings.TrimPrefix(strings.TrimSpace(rest[:colon]), "models/")
				action := strings.TrimSpace(rest[colon+1:])
				if rawModel == "" || strings.ContainsAny(rawModel, ":/") {
					h.invalidPathModel = true
				} else {
					h.canonicalModel = ResolveRequestedModel(rawModel, nil).Model.ID
				}
				if action == "countTokens" {
					h.isNonUpstream = true
				}
			} else {
				h.isNonUpstream = true
			}
		}
	}
	if r.Body == nil || r.Method != http.MethodPost {
		return h
	}
	if r.ContentLength > MaxBodyBytes {
		h.tooLarge = true
		return h
	}
	origBody := r.Body
	raw, err := io.ReadAll(io.LimitReader(origBody, MaxBodyBytes+1))
	_ = origBody.Close()
	if err != nil {
		h.readErr = err
		return h
	}
	if int64(len(raw)) > MaxBodyBytes {
		h.tooLarge = true
		return h
	}
	r.Body = io.NopCloser(bytes.NewReader(raw))
	r.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(raw)), nil
	}
	r.ContentLength = int64(len(raw))
	r.TransferEncoding = nil
	h.rawBody = raw

	if h.invalidPathModel || isImageGen {
		return h
	}

	trimmedRaw := bytes.TrimSpace(raw)
	if len(trimmedRaw) == 0 || trimmedRaw[0] != '{' {
		h.malformedJSON = true
		return h
	}

	var topRaw map[string]json.RawMessage
	if err := json.Unmarshal(raw, &topRaw); err != nil || topRaw == nil {
		h.malformedJSON = true
		return h
	}
	if mStr := rawJSONString(topRaw["model"]); h.canonicalModel == "" && mStr != "" {
		h.canonicalModel = ResolveRequestedModel(mStr, nil).Model.ID
	}
	if prevRespID := rawJSONString(topRaw["previous_response_id"]); prevRespID != "" {
		h.isContinuation = true
	}

	if !h.isNonUpstream {
		if jsonToolsFieldHasTools(topRaw["tools"], isGemini) || jsonToolsFieldHasTools(topRaw["functions"], false) {
			h.hasTools = true
		}
	}

	var inArrSys json.RawMessage
	var firstElem json.RawMessage
	var arrCont bool
	var convArr json.RawMessage
	if len(bytes.TrimSpace(topRaw["messages"])) > 0 {
		convArr = topRaw["messages"]
		inArrSys, firstElem, arrCont = inspectConversationTurns(convArr)
	} else if len(bytes.TrimSpace(topRaw["contents"])) > 0 {
		convArr = topRaw["contents"]
		inArrSys, firstElem, arrCont = inspectConversationTurns(convArr)
	} else if rawInput := bytes.TrimSpace(topRaw["input"]); len(rawInput) > 0 {
		if rawInput[0] == '[' {
			convArr = rawInput
			inArrSys, firstElem, arrCont = inspectConversationTurns(rawInput)
		} else {
			firstElem = rawInput
		}
	}
	if arrCont {
		h.isContinuation = true
	}
	if !h.isNonUpstream && len(convArr) > 0 {
		hasToolCont, callIDs := inspectConversationToolsAndCallIDs(convArr)
		if hasToolCont {
			h.hasTools = true
		}
		h.toolResultCallIDs = callIDs
	}

	if h.sessionKey == "" {
		if k := rawJSONString(topRaw["prompt_cache_key"]); k != "" {
			h.sessionKey = "pck:" + k
		} else {
			var metaObj struct {
				SessionID      string `json:"session_id"`
				ConversationID string `json:"conversation_id"`
				UserID         string `json:"user_id"`
			}
			metaUID := ""
			if rawMeta := topRaw["metadata"]; len(rawMeta) > 0 && json.Unmarshal(rawMeta, &metaObj) == nil {
				if sid := strings.TrimSpace(metaObj.SessionID); sid != "" {
					h.sessionKey = "meta:" + sid
				} else if cid := strings.TrimSpace(metaObj.ConversationID); cid != "" {
					h.sessionKey = "meta:" + cid
				}
				metaUID = strings.TrimSpace(metaObj.UserID)
			}
			if h.sessionKey == "" {
				if rawLabels := topRaw["labels"]; len(rawLabels) > 0 {
					var labelObj struct {
						SessionID string `json:"session_id"`
					}
					if json.Unmarshal(rawLabels, &labelObj) == nil {
						if sid := strings.TrimSpace(labelObj.SessionID); sid != "" {
							h.sessionKey = "meta:" + sid
						}
					}
				}
			}
			if h.sessionKey == "" {
				if metaUID != "" && strings.Contains(strings.ToLower(metaUID), "session") {
					h.sessionKey = "meta:" + metaUID
				} else {
					firstMsg := clipBytes(firstElem, 1024)
					sysPart := clipBytes(topRaw["system"], 512)
					if len(sysPart) == 0 {
						sysPart = clipBytes(topRaw["instructions"], 512)
					}
					if len(sysPart) == 0 {
						sysPart = clipBytes(topRaw["systemInstruction"], 512)
					}
					arrSysClip := clipBytes(inArrSys, 512)
					if len(firstMsg) > 0 || len(sysPart) > 0 || len(arrSysClip) > 0 {
						sum := sha256.New()
						sum.Write(sysPart)
						sum.Write([]byte{'|'})
						sum.Write(arrSysClip)
						sum.Write([]byte{'|'})
						sum.Write(firstMsg)
						h.sessionKey = "fp:" + hex.EncodeToString(sum.Sum(nil))[:16]
					} else if metaUID != "" {
						h.sessionKey = "meta:" + metaUID
					} else if k := rawJSONString(topRaw["user"]); k != "" {
						h.sessionKey = "usr:" + k
					}
				}
			}
		}
	}

	// Scope subagent sticky routing keys independently from their parent session across hdr:/pck:/meta:/usr:/fp:
	// so concurrent subagents sharing a parent session do not overwrite each other's stickyLRU binding.
	extractRoutingAgentID := func() string {
		if r != nil {
			for _, hdr := range []string{"x-claude-code-agent-id", "x-agent-id", "x-subagent-id"} {
				if v := strings.TrimSpace(r.Header.Get(hdr)); v != "" {
					return v
				}
			}
		}
		if rawMeta := topRaw["metadata"]; len(rawMeta) > 0 {
			var metaObj struct {
				AgentID string `json:"agent_id"`
				UserID  string `json:"user_id"`
			}
			if json.Unmarshal(rawMeta, &metaObj) == nil {
				if v := strings.TrimSpace(metaObj.AgentID); v != "" {
					return v
				}
				uidStr := strings.TrimSpace(metaObj.UserID)
				if strings.HasPrefix(uidStr, "{") {
					var parsedUID struct {
						AgentID string `json:"agent_id"`
					}
					if json.Unmarshal([]byte(uidStr), &parsedUID) == nil {
						if v := strings.TrimSpace(parsedUID.AgentID); v != "" {
							return v
						}
					}
				}
			}
		}
		if rawLabels := topRaw["labels"]; len(rawLabels) > 0 {
			var labelObj struct {
				AgentID string `json:"agent_id"`
			}
			if json.Unmarshal(rawLabels, &labelObj) == nil {
				if v := strings.TrimSpace(labelObj.AgentID); v != "" {
					return v
				}
			}
		}
		return ""
	}
	if h.sessionKey != "" && !strings.Contains(h.sessionKey, ":ag:") {
		if agentID := extractRoutingAgentID(); agentID != "" && !strings.Contains(h.sessionKey, agentID) {
			h.sessionKey = h.sessionKey + ":ag:" + agentID
		}
	}
	return h
}

func writeAccountRouteError(w http.ResponseWriter, path string, statusCode int, errType, errCode, msg string) {
	if strings.HasPrefix(path, "/v1beta/") {
		writeGeminiError(w, statusCode, msg)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"type": "error",
		"error": map[string]any{
			"type":    errType,
			"code":    errCode,
			"message": msg,
		},
	})
}

// TryRouteAPIRequest routes /v1/* and /v1beta/* requests (compatibility wrapper for direct callers).
func (am *AccountManager) TryRouteAPIRequest(w http.ResponseWriter, r *http.Request) (*http.Request, bool) {
	reqOut, _, finish, handled := am.TryRouteAPIRequestWithFinish(w, r)
	if finish != nil {
		finish()
	}
	return reqOut, handled
}

// TryRouteAPIRequestWithFinish routes /v1/* and /v1beta/* requests with strict model allowlists,
// strategy-based pool/group scheduling, U8 sticky continuity, bounded pre-stream 1st-turn failover,
// and unified primary/child inFlight & health tracking.
func (am *AccountManager) TryRouteAPIRequestWithFinish(w http.ResponseWriter, r *http.Request) (*http.Request, http.ResponseWriter, func(), bool) {
	noop := func() {}
	if isChildAccountWorker() || !am.HasConfiguredAccounts() {
		return r, w, noop, false
	}
	p := cleanGeminiV1BetaPath(r.URL.Path)
	isModelList := r.Method == http.MethodGet && (p == "/v1/models" || p == "/v1beta/models" || p == "/v1beta/models/")
	if isModelList {
		if expAcc, mode, isExplicit, invErr := am.ResolveExplicitAccount(r); isExplicit {
			if invErr != nil {
				writeAccountRouteError(w, p, http.StatusForbidden, "permission_error", "account_not_available", invErr.Error())
				return r, w, noop, true
			}
			w.Header().Set("X-Agy-Account", expAcc.ID)
			w.Header().Set("X-Agy-Route-Mode", mode)
			return r, w, noop, false
		}
		if grpScope, isGrp, grpErr := am.ResolveGroupScope(r); isGrp {
			if grpErr != nil {
				writeAccountRouteError(w, p, http.StatusForbidden, "permission_error", "group_not_available", grpErr.Error())
				return r, w, noop, true
			}
			w.Header().Set("X-Agy-Group", grpScope.ID)
			w.Header().Set("X-Agy-Route-Mode", "group:"+grpScope.ID)
		}
		return r, w, noop, false
	}
	isPostGen := r.Method == http.MethodPost && (p == "/v1/messages" || p == "/v1/chat/completions" || p == "/v1/responses" || p == "/v1/messages/count_tokens")
	isImageGen := r.Method == http.MethodPost && (p == "/v1/images/generations" || p == "/v1/images/edits")
	isV1BetaModelOp := strings.HasPrefix(p, "/v1beta/models/") && strings.Trim(strings.TrimPrefix(p, "/v1beta/models/"), "/") != ""
	if !isPostGen && !isV1BetaModelOp && !isImageGen {
		return r, w, noop, false
	}

	wrapPrimaryExecution := func(req *http.Request, primaryID, grpID, routeMode, modelID string, isSticky, skipHealth bool, preRelease func(), failoverCount int) (*http.Request, http.ResponseWriter, func(), bool) {
		w.Header().Set("X-Agy-Account", primaryID)
		if grpID != "" {
			w.Header().Set("X-Agy-Group", grpID)
		}
		if routeMode != "" {
			w.Header().Set("X-Agy-Route-Mode", routeMode)
		}
		if failoverCount > 0 {
			w.Header().Set("X-Agy-Failover-Count", strconv.Itoa(failoverCount))
		}
		if !skipHealth {
			am.RecordAccountRouteHit(primaryID, routeMode, isSticky)
		}
		req = withRoutedAccountMeta(req, primaryID, modelID, skipHealth)
		meta, _ := routedAccountMetaFromRequest(req)
		release := preRelease
		if release == nil {
			if !skipHealth {
				release = am.AcquireAccountInFlight(primaryID)
			} else {
				release = noop
			}
		}
		ow := &routeOutcomeWriter{ResponseWriter: w}
		finish := func() {
			release()
			sc := int(ow.statusCode.Load())
			if !skipHealth && (meta == nil || !meta.outcomeRecorded.Load()) && !meta.skipHealth.Load() && sc > 0 {
				am.RecordAccountOutcome(primaryID, modelID, sc, "", ow.Header().Get("Retry-After"), "")
			}
		}
		return req, ow, finish, false
	}

	// Single-account fast path: skip pre-reading request body when only the unrestricted primary exists.
	if primaryID, fast := am.isSimpleSinglePrimary(r); fast {
		skipFastHealth := p == "/v1/messages/count_tokens" || (strings.HasPrefix(p, "/v1beta/models/") && (r.Method == http.MethodGet || strings.HasSuffix(p, ":countTokens")))
		fastModel := DefaultModelID
		if isImageGen {
			fastModel = imageGenOuterModel
		}
		return wrapPrimaryExecution(r, primaryID, "", "primary-fast", fastModel, false, skipFastHealth, nil, 0)
	}

	hints := extractRequestRoutingHintsV2(r)
	modelID := hints.canonicalModel
	sessionKey := hints.sessionKey
	isContinuation := hints.isContinuation
	isNonUpstream := hints.isNonUpstream
	invalidPathModel := hints.invalidPathModel
	rawBody := hints.rawBody
	malformedJSON := hints.malformedJSON
	tooLarge := hints.tooLarge
	readErr := hints.readErr
	hasTools := hints.hasTools
	toolResultCallIDs := hints.toolResultCallIDs
	if tooLarge {
		writeAccountRouteError(w, p, http.StatusRequestEntityTooLarge, "invalid_request_error", "request_too_large", "Request body exceeds maximum size")
		return r, w, noop, true
	}
	if readErr != nil {
		writeAccountRouteError(w, p, http.StatusBadRequest, "invalid_request_error", "body_read_error", "Failed to read request body")
		return r, w, noop, true
	}
	if rawBody != nil {
		r = withPreReadBody(r, rawBody)
	}
	if invalidPathModel {
		if _, _, isExplicit, invErr := am.ResolveExplicitAccount(r); isExplicit && invErr != nil {
			writeAccountRouteError(w, p, http.StatusForbidden, "permission_error", "account_not_available", invErr.Error())
			return r, w, noop, true
		}
		if _, isGrp, grpErr := am.ResolveGroupScope(r); isGrp && grpErr != nil {
			writeAccountRouteError(w, p, http.StatusForbidden, "permission_error", "group_not_available", grpErr.Error())
			return r, w, noop, true
		}
		writeAccountRouteError(w, p, http.StatusBadRequest, "invalid_request_error", "invalid_model_path", "Invalid model identifier in path")
		return r, w, noop, true
	}
	if r.Method == http.MethodPost && malformedJSON {
		if _, _, isExplicit, invErr := am.ResolveExplicitAccount(r); isExplicit && invErr != nil {
			writeAccountRouteError(w, p, http.StatusForbidden, "permission_error", "account_not_available", invErr.Error())
			return r, w, noop, true
		}
		if _, isGrp, grpErr := am.ResolveGroupScope(r); isGrp && grpErr != nil {
			writeAccountRouteError(w, p, http.StatusForbidden, "permission_error", "group_not_available", grpErr.Error())
			return r, w, noop, true
		}
		return r, w, noop, false
	}

	if modelID == "" && (isPostGen || (isV1BetaModelOp && r.Method == http.MethodPost)) {
		modelID = ResolveRequestedModel("", nil).Model.ID
	}

	rCfg := am.GetRoutingConfig()
	canPreStreamFailover := !isContinuation && !isNonUpstream && rCfg.PreStreamFailoverEnabled()
	maxAttempts := 1
	if canPreStreamFailover {
		maxAttempts = 1 + rCfg.EffectiveMaxFailoverHops()
	}

	var excluded map[string]bool
	type pendingBufferedFailure struct {
		accID         string
		grpID         string
		routeMode     string
		statusCode    int
		headers       http.Header
		body          []byte
		errType       string
		errCode       string
		errMsg        string
		isStructured  bool
		failoverCount int
	}
	var lastFail *pendingBufferedFailure

	writeDenyResponse := func(pick RoutePickResult) {
		if pick.Account.ID != "" {
			w.Header().Set("X-Agy-Account", pick.Account.ID)
		}
		if pick.GroupID != "" {
			w.Header().Set("X-Agy-Group", pick.GroupID)
		}
		if pick.RouteMode != "" {
			w.Header().Set("X-Agy-Route-Mode", pick.RouteMode)
		}
		if pick.RetryAfterSec > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(pick.RetryAfterSec))
		}
		errType := "permission_error"
		errCode := "model_not_allowed"
		switch pick.StatusCode {
		case http.StatusTooManyRequests:
			errType = "rate_limit_error"
			errCode = "pool_cooldown"
		case http.StatusServiceUnavailable:
			lowDeny := ""
			if pick.DenyErr != nil {
				lowDeny = strings.ToLower(pick.DenyErr.Error())
			}
			if strings.Contains(lowDeny, "tos circuit") || strings.Contains(lowDeny, "tos_circuit_breaker") {
				errType = "tos_circuit_breaker"
				errCode = "tos_circuit_breaker"
			} else if strings.HasPrefix(pick.RouteMode, "pool-sticky") || strings.HasPrefix(pick.RouteMode, "group-sticky") {
				errType = "sticky_account_unavailable"
				errCode = "sticky_account_unavailable"
			} else if strings.Contains(pick.RouteMode, "locked") {
				errType = "manual_account_unavailable"
				errCode = "manual_account_unavailable"
			} else {
				errType = "account_not_authenticated"
				errCode = "account_not_authenticated"
			}
		}
		writeAccountRouteError(w, p, pick.StatusCode, errType, errCode, pick.DenyErr.Error())
	}

	flushBufferedFailure := func(bf *pendingBufferedFailure) {
		if bf.accID != "" {
			w.Header().Set("X-Agy-Account", bf.accID)
		}
		if bf.grpID != "" {
			w.Header().Set("X-Agy-Group", bf.grpID)
		}
		if bf.routeMode != "" {
			w.Header().Set("X-Agy-Route-Mode", bf.routeMode)
		}
		if bf.failoverCount > 0 {
			w.Header().Set("X-Agy-Failover-Count", strconv.Itoa(bf.failoverCount))
		}
		if bf.isStructured {
			copyCleanProxyResponseHeaders(w.Header(), bf.headers)
			w.Header().Set("Content-Length", strconv.Itoa(len(bf.body)))
			w.Header().Set("X-Agy-Account", bf.accID)
			if bf.grpID != "" {
				w.Header().Set("X-Agy-Group", bf.grpID)
			}
			if bf.routeMode != "" {
				w.Header().Set("X-Agy-Route-Mode", bf.routeMode)
			}
			if bf.failoverCount > 0 {
				w.Header().Set("X-Agy-Failover-Count", strconv.Itoa(bf.failoverCount))
			}
			w.WriteHeader(bf.statusCode)
			_, _ = w.Write(bf.body)
			return
		}
		writeAccountRouteError(w, p, bf.statusCode, bf.errType, bf.errCode, bf.errMsg)
	}

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if rawBody != nil {
			r.Body = io.NopCloser(bytes.NewReader(rawBody))
		}
		pick := am.ResolveTargetAccountEx(r, modelID, ResolveOptions{
			SessionKey:        sessionKey,
			IsContinuation:    isContinuation,
			HasTools:          hasTools,
			ToolResultCallIDs: toolResultCallIDs,
			ExcludeIDs:        excluded,
			ReserveInFlight:   !isNonUpstream,
			DeferStickyCommit: true,
		})
		if pick.DenyErr != nil {
			if attempt > 0 && lastFail != nil {
				flushBufferedFailure(lastFail)
				return r, w, noop, true
			}
			writeDenyResponse(pick)
			return r, w, noop, true
		}

		manualLocked := strings.HasSuffix(pick.RouteMode, "manual-active") && !rCfg.ManualAutoFailoverEnabled()
		if pick.GroupID != "" && strings.HasSuffix(pick.RouteMode, "manual-active") {
			if grp, okG := am.GetGroup(pick.GroupID); okG {
				manualLocked = !grp.EffectiveManualAutoFailover(rCfg.ManualAutoFailoverEnabled())
			}
		}
		allowHop := canPreStreamFailover && !pick.IsExplicit && !manualLocked && attempt+1 < maxAttempts
		acc := pick.Account

		// Primary account execution
		if acc.ID == "" || acc.Primary {
			if acc.ID == "" {
				pick.ReleaseFlight()
				return r, w, noop, false
			}
			if !isNonUpstream {
				if tripped, reason := IsTosTripped(); tripped {
					am.RecordAccountOutcome(acc.ID, modelID, http.StatusServiceUnavailable, "tos_circuit_breaker", "", reason)
					pick.ReleaseFlight()
					if allowHop && r.Context().Err() == nil {
						am.RecordAccountFailoverOut(acc.ID)
						if excluded == nil {
							excluded = make(map[string]bool, maxAttempts)
						}
						excluded[acc.ID] = true
						lastFail = &pendingBufferedFailure{
							accID: acc.ID, grpID: pick.GroupID, routeMode: pick.RouteMode,
							statusCode: http.StatusServiceUnavailable, errType: "tos_circuit_breaker", errCode: "tos_circuit_breaker",
							errMsg: "Gateway halted by ToS circuit breaker: " + reason, failoverCount: attempt,
						}
						continue
					}
				}
			}
			pick.CommitSticky()
			return wrapPrimaryExecution(r, acc.ID, pick.GroupID, pick.RouteMode, modelID, pick.IsStickyHit, isNonUpstream, pick.ReleaseFlight, attempt)
		}

		// Secondary child-worker execution
		if !am.HasOAuthToken(acc.ID) {
			pick.ReleaseFlight()
			bf := &pendingBufferedFailure{
				accID: acc.ID, grpID: pick.GroupID, routeMode: pick.RouteMode,
				statusCode: http.StatusServiceUnavailable, errType: "account_not_authenticated", errCode: "account_not_authenticated",
				errMsg:        fmt.Sprintf("Account %q has no OAuth credentials yet; authorize it in the /acp/ dashboard first.", acc.ID),
				failoverCount: attempt,
			}
			if allowHop && r.Context().Err() == nil {
				am.RecordAccountFailoverOut(acc.ID)
				if excluded == nil {
					excluded = make(map[string]bool, maxAttempts)
				}
				excluded[acc.ID] = true
				lastFail = bf
				continue
			}
			flushBufferedFailure(bf)
			return r, w, noop, true
		}

		if err := am.EnsureAccountWorkerRunningWithContext(r.Context(), acc.ID); err != nil {
			redErr := gwcfg.RedactSecretsInString(err.Error())
			if !isNonUpstream && r.Context().Err() == nil {
				am.RecordAccountOutcome(acc.ID, modelID, http.StatusBadGateway, "account_worker_error", "", redErr)
			}
			LogEvent("ACCOUNT_WORKER_START_ERR", "Failed to start child worker for routed request", map[string]any{
				"accountId": acc.ID,
				"error":     redErr,
			})
			pick.ReleaseFlight()
			bf := &pendingBufferedFailure{
				accID: acc.ID, grpID: pick.GroupID, routeMode: pick.RouteMode,
				statusCode: http.StatusBadGateway, errType: "account_worker_error", errCode: "account_worker_error",
				errMsg:        fmt.Sprintf("Worker for account %q failed to become ready; retry shortly.", acc.ID),
				failoverCount: attempt,
			}
			if allowHop && r.Context().Err() == nil {
				am.RecordAccountFailoverOut(acc.ID)
				if excluded == nil {
					excluded = make(map[string]bool, maxAttempts)
				}
				excluded[acc.ID] = true
				lastFail = bf
				continue
			}
			flushBufferedFailure(bf)
			return r, w, noop, true
		}

		am.mu.RLock()
		proc := am.workers[acc.ID]
		gwPort := 0
		var tr *http.Transport
		if proc != nil && !proc.stopped {
			gwPort = proc.gwPort
			tr = proc.transport
		}
		am.mu.RUnlock()
		if gwPort <= 0 || tr == nil {
			pick.ReleaseFlight()
			bf := &pendingBufferedFailure{
				accID: acc.ID, grpID: pick.GroupID, routeMode: pick.RouteMode,
				statusCode: http.StatusServiceUnavailable, errType: "account_worker_error", errCode: "account_worker_unavailable",
				errMsg:        fmt.Sprintf("Worker for account %q is temporarily unavailable; retry shortly.", acc.ID),
				failoverCount: attempt,
			}
			if allowHop && r.Context().Err() == nil {
				am.RecordAccountFailoverOut(acc.ID)
				if excluded == nil {
					excluded = make(map[string]bool, maxAttempts)
				}
				excluded[acc.ID] = true
				lastFail = bf
				continue
			}
			flushBufferedFailure(bf)
			return r, w, noop, true
		}

		outReq := r.Clone(r.Context())
		outReq.URL.Scheme = "http"
		outReq.URL.Host = fmt.Sprintf("127.0.0.1:%d", gwPort)
		outReq.RequestURI = ""
		outReq.Host = outReq.URL.Host
		for _, h := range hopByHopProxyHeaders {
			outReq.Header.Del(h)
		}
		outReq.Header.Del("X-Forwarded-Host")
		outReq.Header.Del("X-Forwarded-Proto")
		outReq.Header.Del("X-Forwarded-For")
		if masterKey := defaultPeerAPIKey(gwcfg.Current()); masterKey != "" {
			outReq.Header.Set("x-api-key", masterKey)
			outReq.Header.Del("Authorization")
			outReq.Header.Del("x-goog-api-key")
		}
		if rawBody != nil {
			outReq.Body = io.NopCloser(bytes.NewReader(rawBody))
			outReq.ContentLength = int64(len(rawBody))
		}

		resp, rtErr := tr.RoundTrip(outReq)
		if rtErr != nil {
			redacted := gwcfg.RedactSecretsInString(rtErr.Error())
			if !isNonUpstream && r.Context().Err() == nil {
				am.RecordAccountOutcome(acc.ID, modelID, http.StatusBadGateway, "account_worker_unreachable", "", redacted)
			}
			LogEvent("ACCOUNT_PROXY_ERR", "Child account worker reverse proxy error", map[string]any{
				"accountId": acc.ID,
				"error":     redacted,
			})
			pick.ReleaseFlight()
			bf := &pendingBufferedFailure{
				accID: acc.ID, grpID: pick.GroupID, routeMode: pick.RouteMode,
				statusCode: http.StatusBadGateway, errType: "account_worker_error", errCode: "account_worker_unreachable",
				errMsg:        fmt.Sprintf("Account %q worker is temporarily unreachable; retry shortly.", acc.ID),
				failoverCount: attempt,
			}
			if allowHop && r.Context().Err() == nil {
				am.RecordAccountFailoverOut(acc.ID)
				if excluded == nil {
					excluded = make(map[string]bool, maxAttempts)
				}
				excluded[acc.ID] = true
				lastFail = bf
				continue
			}
			flushBufferedFailure(bf)
			return r, w, noop, true
		}

		if occ, okOcc := parseSlotUsageHeader(resp.Header.Get(internalSlotUsageHeader)); okOcc {
			am.RecordLocalSlotHint(acc.ID, occ, false, time.Now())
		}

		if resp.StatusCode >= 400 {
			errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 256*1024))
			_ = resp.Body.Close()
			effStatus, eType, eMsg := classifyChildErrorBody(resp.StatusCode, errBody)
			localSlotFull := isLocalWorkerSlotError(effStatus, eType, eMsg, resp.Header)
			if localSlotFull {
				am.RecordLocalSlotHint(acc.ID, -1, true, time.Now())
			} else if !isNonUpstream {
				am.RecordAccountOutcome(acc.ID, modelID, effStatus, eType, resp.Header.Get("Retry-After"), eMsg)
			}
			bf := &pendingBufferedFailure{
				accID: acc.ID, grpID: pick.GroupID, routeMode: pick.RouteMode,
				statusCode: resp.StatusCode, headers: resp.Header.Clone(), body: errBody,
				isStructured: true, failoverCount: attempt,
			}
			if allowHop && r.Context().Err() == nil && shouldRetryChildPreStreamFailure(effStatus, eType, eMsg) {
				pick.ReleaseFlight()
				am.RecordAccountFailoverOut(acc.ID)
				if excluded == nil {
					excluded = make(map[string]bool, maxAttempts)
				}
				excluded[acc.ID] = true
				lastFail = bf
				continue
			}
			if !isNonUpstream {
				am.RecordAccountRouteHit(acc.ID, pick.RouteMode, pick.IsStickyHit)
			}
			pick.ReleaseFlight()
			flushBufferedFailure(bf)
			return r, w, noop, true
		}

		// Upstream 2xx response accepted -> commit sticky session, copy headers, stream body, and record accurate outcome
		pick.CommitSticky()
		if !isNonUpstream {
			am.RecordAccountRouteHit(acc.ID, pick.RouteMode, pick.IsStickyHit)
		}
		copyCleanProxyResponseHeaders(w.Header(), resp.Header)
		w.Header().Set("X-Agy-Account", acc.ID)
		if pick.GroupID != "" {
			w.Header().Set("X-Agy-Group", pick.GroupID)
		}
		if pick.RouteMode != "" {
			w.Header().Set("X-Agy-Route-Mode", pick.RouteMode)
		}
		if attempt > 0 {
			w.Header().Set("X-Agy-Failover-Count", strconv.Itoa(attempt))
		}
		w.WriteHeader(resp.StatusCode)

		ctLow := strings.ToLower(resp.Header.Get("Content-Type"))
		isStreamingResp := strings.Contains(ctLow, "event-stream") || strings.Contains(p, ":streamGenerateContent")
		completed, midStatus, midType, midMsg := streamProxyBodyAndRecordOutcome(w, resp.Body, !isNonUpstream && isStreamingResp)
		if occ, okOcc := parseSlotUsageHeader(resp.Trailer.Get(internalSlotUsageHeader)); okOcc {
			am.RecordLocalSlotHint(acc.ID, occ, false, time.Now())
		}
		pick.ReleaseFlight()
		if !isNonUpstream {
			if isStreamingResp {
				if completed {
					if isLocalWorkerSlotError(midStatus, midType, midMsg, resp.Header) {
						am.RecordLocalSlotHint(acc.ID, -1, true, time.Now())
					} else {
						am.RecordAccountOutcome(acc.ID, modelID, midStatus, midType, "", midMsg)
					}
				}
			} else {
				am.RecordAccountOutcome(acc.ID, modelID, resp.StatusCode, "", "", "")
			}
		}
		return r, w, noop, true
	}

	if lastFail != nil {
		flushBufferedFailure(lastFail)
		return r, w, noop, true
	}
	return r, w, noop, false
}

// Keep unused error import lint-clean if needed.
var _ = errors.New
