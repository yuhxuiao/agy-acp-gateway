package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gwcfg "agy-acp-gateway-go/internal/config"
	"agy-acp-gateway-go/internal/protocol"
)

const (
	McpBridgeName       = "ws-bridge"
	LegacyMcpBridgeName = "dsh-bridge"
	ImageGenAgentName   = "ws-imagegen"
	MediaRelayAgentName = "ws-media"
	AwaitToolName       = "ws_await_result"
	LegacyAwaitToolName = "dsh_await_result"
	CallToolName        = "ws_call_tool"
	LegacyCallToolName  = "dsh_call_tool"
	UtilWorkerTtl       = 20 * time.Second
	MaxTranscriptChar   = 1800000
)

// ProjectHome: project root (<root>/bin/agy-acp-gateway -> <root>); override with AGY_GW_HOME
var ProjectHome = resolveProjectHome()

func resolveProjectHome() string {
	return gwcfg.ResolveProjectHome()
}

var (
	bootSnap                       = gwcfg.Loaded()
	PatchedAgyBin                  = bootSnap.PatchedAgyBin
	PureApiHome                    = bootSnap.PureApiHome
	DefaultCwd                     = bootSnap.DefaultCwd
	MaxLiveWorkers                 = bootSnap.MaxLiveWorkers
	maxLiveWorkersBoot             = bootSnap.MaxLiveWorkers
	GlobalMaxLiveWorkers           = bootSnap.GlobalMaxLiveWorkers
	globalMaxLiveWorkersBoot       = bootSnap.GlobalMaxLiveWorkers
	MemPressureWarnPct             = bootSnap.MemPressureWarnPct
	memPressureWarnPctBoot         = bootSnap.MemPressureWarnPct
	MemPressureHighPct             = bootSnap.MemPressureHighPct
	memPressureHighPctBoot         = bootSnap.MemPressureHighPct
	ImageCacheMaxMB                = bootSnap.ImageCacheMaxMB
	imageCacheMaxMBBoot            = bootSnap.ImageCacheMaxMB
	MaxProbeWorkers                = bootSnap.MaxProbeWorkers
	MaxConcurrentPrompts           = bootSnap.MaxConcurrentPrompts
	PromptIdleTimeout              = bootSnap.PromptIdleTimeout
	PromptMaxDuration              = bootSnap.PromptMaxDuration
	IdleWorkerTtl                  = bootSnap.IdleWorkerTtl
	ParkedTtl                      = bootSnap.ParkedTtl
	MaxToolRelays                  = bootSnap.MaxToolRelays
	MaxBodyBytes                   = bootSnap.MaxBodyBytes
	AllowedCorsOrigins             = bootSnap.CloneCorsOrigins()
	promptSem                      = make(chan struct{}, MaxConcurrentPrompts)
	promptSemBoot                  = promptSem
	promptLimiter                  = newDynamicPromptLimiter(MaxConcurrentPrompts)
	ListenHost                     = bootSnap.ListenHost
	GatewayPort                    = bootSnap.GatewayPort
	DashboardPort                  = bootSnap.DashboardPort
	GatewayLogFile                 = bootSnap.GatewayLogFile
	GatewayStateDB                 = bootSnap.GatewayStateDB
	WafRewriteRulesFile            = bootSnap.WafRewriteRulesFile
	WafAutoBisectEnabled           = bootSnap.WafAutoBisectEnabled
	TmpLogFile                     = bootSnap.TmpLogFile
	McpTrace                       = bootSnap.McpTrace
	LogHeaders                     = bootSnap.LogHeaders
	UseG1Credits                   = bootSnap.UseG1Credits
	CacheWarnMinTokens             = bootSnap.CacheWarnMinTokens
	CacheWarnStreak                = bootSnap.CacheWarnStreak
	PrefillCancelMinElapsed        = bootSnap.PrefillCancelMinElap
	PrefillCancelGrace             = bootSnap.PrefillCancelGrace
	prefillMinElapsedEnvSet        = bootSnap.PrefillMinElapEnvSet
	prefillGraceEnvSet             = bootSnap.PrefillGraceEnvSet
	CapacityFallbackTTL            = bootSnap.CapacityFallbackTTL
	CapacityFallbackMaxTTL         = bootSnap.CapacityFallbackMaxTTL
	MaxImagesGemini                = bootSnap.MaxImagesGemini
	MaxImagesClaude                = bootSnap.MaxImagesClaude
	MaxImageBytes                  = bootSnap.MaxImageBytes
	MaxPromptImageBytes            = bootSnap.MaxPromptImageBytes
	MaxTextFileBytes               = bootSnap.MaxTextFileBytes
	FetchRemoteImages              = bootSnap.FetchRemoteImages
	MediaRelayEnabled              = bootSnap.MediaRelayEnabled
	mediaRelayEnabledBoot          = bootSnap.MediaRelayEnabled
	MediaRelayMaxBytes             = bootSnap.MediaRelayMaxBytes
	mediaRelayMaxBytesBoot         = bootSnap.MediaRelayMaxBytes
	MediaRelayMaxFiles             = bootSnap.MediaRelayMaxFiles
	mediaRelayMaxFilesBoot         = bootSnap.MediaRelayMaxFiles
	MediaRelayMaxConcurrency       = bootSnap.MediaRelayMaxConcurrency
	mediaRelayMaxConcurrencyBoot   = bootSnap.MediaRelayMaxConcurrency
	PdfTextFastpathEnabled         = bootSnap.PdfTextFastpathEnabled
	pdfTextFastpathEnabledBoot     = bootSnap.PdfTextFastpathEnabled
	ConvKeepDays                   = bootSnap.ConvKeepDays
	AcpLogKeepDays                 = bootSnap.AcpLogKeepDays
	AcpLogMaxMB                    = bootSnap.AcpLogMaxMB
	EngineIdleGrace                = time.Duration(maxInt(bootSnap.EngineIdleGraceSec, 0)) * time.Second
	engineIdleGraceBoot            = EngineIdleGrace
	engineIdleGraceEnvSet          = bootSnap.EngineIdleGraceEnvSet
	AcpRecordDir                   = bootSnap.AcpRecordDir
	acpRecordMu                    sync.Mutex
	argonDegradationDetectedTotal  atomic.Int64
	argonDegradationRecoveredTotal atomic.Int64
	cacheBurnWarningsTotal         atomic.Int64
	orphanRelayCapsTotal           atomic.Int64
	engineExitsTotal               atomic.Int64
	workerCapRejectionsTotal       atomic.Int64
	probeSlotRejectionsTotal       atomic.Int64
	softDetachJoinedTotal          atomic.Int64
	softDetachExpiredTotal         atomic.Int64
	softDetachSupersededTotal      atomic.Int64
	capacityFallbackEventsTotal    atomic.Int64
	capacityFallbackProbesTotal    atomic.Int64
)

func parseCorsOrigins(raw string) map[string]bool {
	return gwcfg.ParseCorsOrigins(raw)
}

// autoGlobalMaxLiveWorkers derives a safe cross-account live worker cap from container/host
// memory budget in MB: clamp((memLimitMB - 900) / 220, 4, 12), falling back to 8 when unknown.
func autoGlobalMaxLiveWorkers(memLimitMB int) int {
	if memLimitMB <= 0 {
		return 8
	}
	n := (memLimitMB - 900) / 220
	if n < 4 {
		return 4
	}
	if n > 12 {
		return 12
	}
	return n
}

// effectiveGlobalMaxLiveWorkers returns the cross-account global live worker ceiling.
// An explicit AGY_GLOBAL_MAX_LIVE_WORKERS > 0 (or direct test mutation) always overrides;
// when unset (0), multi-account deployments (>1 enabled accounts) auto-compute a safe cap
// from container/host cgroup memory, while single-account deployments return 0 (no extra clamp).
func effectiveGlobalMaxLiveWorkers() int {
	if GlobalMaxLiveWorkers != globalMaxLiveWorkersBoot {
		if GlobalMaxLiveWorkers < 0 {
			return 0
		}
		return GlobalMaxLiveWorkers
	}
	if cfgVal := gwcfg.Current().GlobalMaxLiveWorkers; cfgVal > 0 {
		return cfgVal
	}
	if !isChildAccountWorker() {
		if am := GlobalAccountManager; am != nil && am.EnabledAccountCountFast() > 1 {
			return autoGlobalMaxLiveWorkers(detectHostMemLimitMB())
		}
	}
	return 0
}

func effectiveMemPressureThresholds() (warnPct, highPct int) {
	warnPct = MemPressureWarnPct
	if MemPressureWarnPct == memPressureWarnPctBoot {
		warnPct = gwcfg.Current().MemPressureWarnPct
	}
	highPct = MemPressureHighPct
	if MemPressureHighPct == memPressureHighPctBoot {
		highPct = gwcfg.Current().MemPressureHighPct
	}
	if warnPct < 1 {
		warnPct = 70
	} else if warnPct > 98 {
		warnPct = 98
	}
	if highPct <= warnPct {
		highPct = warnPct + 5
	}
	if highPct > 99 {
		highPct = 99
	}
	return warnPct, highPct
}

func effectiveImageCacheMaxBytes() int {
	mb := ImageCacheMaxMB
	if ImageCacheMaxMB == imageCacheMaxMBBoot {
		mb = gwcfg.Current().ImageCacheMaxMB
	}
	if mb <= 0 {
		return 0
	}
	return mb * 1024 * 1024
}

// effectiveMaxLiveWorkers returns the race-safe runtime worker cap for the current process.
// In production parent processes, the Primary account's configured maxLiveWorkers overrides
// the boot env default immediately on startup and live edits, while direct test mutations
// of the package var MaxLiveWorkers continue to take precedence in unit tests.
// Whenever a positive global cross-account cap is active, the per-instance cap is clamped to <= global cap.
func effectiveMaxLiveWorkers() int {
	capVal := MaxLiveWorkers
	if MaxLiveWorkers != maxLiveWorkersBoot {
		if capVal < 1 {
			capVal = 1
		}
	} else {
		if !isChildAccountWorker() {
			if am := GlobalAccountManager; am != nil {
				if v := int(am.primaryMaxLiveWorkers.Load()); v > 0 {
					capVal = v
				}
			}
		}
		if capVal < 1 {
			capVal = 8
		}
	}
	if gCap := effectiveGlobalMaxLiveWorkers(); gCap > 0 && capVal > gCap {
		capVal = gCap
	}
	return capVal
}

type promptWaiter struct {
	ch      chan struct{}
	granted bool
}

// dynamicPromptLimiter is a race-safe, FIFO, context-cancellable concurrency semaphore whose
// limit can be resized live (both up and down) without splitting in-flight accounting.
type dynamicPromptLimiter struct {
	mu      sync.Mutex
	limit   int
	inUse   int
	waiters []*promptWaiter
}

func newDynamicPromptLimiter(limit int) *dynamicPromptLimiter {
	if limit < 1 {
		limit = 1
	}
	return &dynamicPromptLimiter{limit: limit}
}

func (l *dynamicPromptLimiter) wakeWaitersLocked() {
	for l.inUse < l.limit && len(l.waiters) > 0 {
		w := l.waiters[0]
		l.waiters[0] = nil
		l.waiters = l.waiters[1:]
		l.inUse++
		w.granted = true
		close(w.ch)
	}
}

func (l *dynamicPromptLimiter) SetLimit(limit int) {
	if limit < 1 {
		limit = 1
	}
	l.mu.Lock()
	l.limit = limit
	l.wakeWaitersLocked()
	l.mu.Unlock()
}

func (l *dynamicPromptLimiter) Reset(limit int) {
	if limit < 1 {
		limit = 1
	}
	l.mu.Lock()
	l.limit = limit
	l.inUse = 0
	for _, w := range l.waiters {
		w.granted = true
		close(w.ch)
	}
	l.waiters = nil
	l.mu.Unlock()
}

func (l *dynamicPromptLimiter) Limit() int {
	l.mu.Lock()
	n := l.limit
	l.mu.Unlock()
	return n
}

func (l *dynamicPromptLimiter) InFlight() int {
	l.mu.Lock()
	n := l.inUse
	l.mu.Unlock()
	return n
}

func (l *dynamicPromptLimiter) releaseOne() {
	l.mu.Lock()
	if l.inUse > 0 {
		l.inUse--
	}
	l.wakeWaitersLocked()
	l.mu.Unlock()
}

func (l *dynamicPromptLimiter) Acquire(ctx context.Context) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	l.mu.Lock()
	if l.inUse < l.limit && len(l.waiters) == 0 {
		l.inUse++
		l.mu.Unlock()
		var once sync.Once
		return func() { once.Do(l.releaseOne) }, nil
	}
	if err := ctx.Err(); err != nil {
		l.mu.Unlock()
		return nil, err
	}
	w := &promptWaiter{ch: make(chan struct{})}
	l.waiters = append(l.waiters, w)
	l.mu.Unlock()

	select {
	case <-w.ch:
		var once sync.Once
		return func() { once.Do(l.releaseOne) }, nil
	case <-ctx.Done():
		l.mu.Lock()
		if w.granted {
			if l.inUse > 0 {
				l.inUse--
			}
			l.wakeWaitersLocked()
		} else {
			for i, cand := range l.waiters {
				if cand == w {
					copy(l.waiters[i:], l.waiters[i+1:])
					l.waiters[len(l.waiters)-1] = nil
					l.waiters = l.waiters[:len(l.waiters)-1]
					break
				}
			}
		}
		l.mu.Unlock()
		return nil, ctx.Err()
	}
}

func useDirectPromptSem() bool {
	if promptSem != promptSemBoot {
		return true
	}
	return isChildAccountWorker() || gwcfg.Current().MaxConcurrentPromptsEnvSet
}

func desiredPrimaryMaxConcurrentPrompts() int {
	want := effectiveMaxLiveWorkers()
	if want < 6 {
		want = 6
	}
	maxCap := maxAllowedAccountLiveWorkers()
	if want > maxCap {
		want = maxCap
	}
	return want
}

func effectiveMaxConcurrentPrompts() int {
	if useDirectPromptSem() {
		if promptSem != nil {
			return cap(promptSem)
		}
		if MaxConcurrentPrompts > 0 {
			return MaxConcurrentPrompts
		}
		return 1
	}
	return promptLimiter.Limit()
}

func promptsInFlightCount() int {
	if useDirectPromptSem() {
		return len(promptSem)
	}
	return promptLimiter.InFlight()
}

func acquirePromptSlot(ctx context.Context) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if useDirectPromptSem() {
		sem := promptSem
		select {
		case sem <- struct{}{}:
			var once sync.Once
			return func() {
				once.Do(func() { <-sem })
			}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return promptLimiter.Acquire(ctx)
}

// syncPrimaryPromptLimitLive resizes the Primary process's dynamic prompt limiter immediately
// when Primary's maxLiveWorkers is updated at runtime via UpsertAccount (without touching
// non-atomic boot vars). No-op in child workers or when AGY_MAX_CONCURRENT_PROMPTS is pinned.
func syncPrimaryPromptLimitLive() {
	if isChildAccountWorker() {
		return
	}
	if gwcfg.Current().MaxConcurrentPromptsEnvSet {
		return
	}
	promptLimiter.SetLimit(desiredPrimaryMaxConcurrentPrompts())
}

// syncPrimaryPromptSemBeforeServe resizes the Primary process's promptSem once at startup
// (after GlobalAccountManager.Init() syncs accounts.json maxLiveWorkers and BEFORE any HTTP
// handlers start) when AGY_MAX_CONCURRENT_PROMPTS was not explicitly set in env.
func syncPrimaryPromptSemBeforeServe() {
	if isChildAccountWorker() {
		return
	}
	if gwcfg.Current().MaxConcurrentPromptsEnvSet {
		return
	}
	want := desiredPrimaryMaxConcurrentPrompts()
	promptLimiter.SetLimit(want)
	if want != MaxConcurrentPrompts || cap(promptSem) != want {
		MaxConcurrentPrompts = want
		promptSem = make(chan struct{}, want)
		promptSemBoot = promptSem
	}
}

func initConfig() {
	s := gwcfg.Reload()
	ProjectHome = s.ProjectHome
	PatchedAgyBin = s.PatchedAgyBin
	PureApiHome = s.PureApiHome
	DefaultCwd = s.DefaultCwd
	MaxLiveWorkers = s.MaxLiveWorkers
	maxLiveWorkersBoot = s.MaxLiveWorkers
	GlobalMaxLiveWorkers = s.GlobalMaxLiveWorkers
	globalMaxLiveWorkersBoot = s.GlobalMaxLiveWorkers
	MemPressureWarnPct = s.MemPressureWarnPct
	memPressureWarnPctBoot = s.MemPressureWarnPct
	MemPressureHighPct = s.MemPressureHighPct
	memPressureHighPctBoot = s.MemPressureHighPct
	ImageCacheMaxMB = s.ImageCacheMaxMB
	imageCacheMaxMBBoot = s.ImageCacheMaxMB
	if GlobalAccountManager != nil {
		GlobalAccountManager.primaryMaxLiveWorkers.Store(0)
	}
	fedMetricsMu.Lock()
	fedMetricsCache = nil
	fedMetricsAt = time.Time{}
	fedMetricsMu.Unlock()
	MaxProbeWorkers = s.MaxProbeWorkers
	MaxConcurrentPrompts = s.MaxConcurrentPrompts
	PromptIdleTimeout = s.PromptIdleTimeout
	PromptMaxDuration = s.PromptMaxDuration
	IdleWorkerTtl = s.IdleWorkerTtl
	ParkedTtl = s.ParkedTtl
	MaxToolRelays = s.MaxToolRelays
	MaxBodyBytes = s.MaxBodyBytes
	AllowedCorsOrigins = s.CloneCorsOrigins()
	promptSem = make(chan struct{}, MaxConcurrentPrompts)
	promptSemBoot = promptSem
	promptLimiter.Reset(MaxConcurrentPrompts)
	ListenHost = s.ListenHost
	GatewayPort = s.GatewayPort
	DashboardPort = s.DashboardPort
	GatewayLogFile = s.GatewayLogFile
	GatewayStateDB = s.GatewayStateDB
	WafRewriteRulesFile = s.WafRewriteRulesFile
	WafAutoBisectEnabled = s.WafAutoBisectEnabled
	TosTrippedFile = s.TosTrippedFile
	TmpLogFile = s.TmpLogFile
	McpTrace = s.McpTrace
	LogHeaders = s.LogHeaders
	UseG1Credits = s.UseG1Credits
	CacheWarnMinTokens = s.CacheWarnMinTokens
	CacheWarnStreak = s.CacheWarnStreak
	PrefillCancelMinElapsed = s.PrefillCancelMinElap
	PrefillCancelGrace = s.PrefillCancelGrace
	prefillMinElapsedEnvSet = s.PrefillMinElapEnvSet
	prefillGraceEnvSet = s.PrefillGraceEnvSet
	CapacityFallbackTTL = s.CapacityFallbackTTL
	CapacityFallbackMaxTTL = s.CapacityFallbackMaxTTL
	MaxImagesGemini = s.MaxImagesGemini
	MaxImagesClaude = s.MaxImagesClaude
	MaxImageBytes = s.MaxImageBytes
	MaxPromptImageBytes = s.MaxPromptImageBytes
	MaxTextFileBytes = s.MaxTextFileBytes
	FetchRemoteImages = s.FetchRemoteImages
	MediaRelayEnabled = s.MediaRelayEnabled
	mediaRelayEnabledBoot = s.MediaRelayEnabled
	MediaRelayMaxBytes = s.MediaRelayMaxBytes
	mediaRelayMaxBytesBoot = s.MediaRelayMaxBytes
	MediaRelayMaxFiles = s.MediaRelayMaxFiles
	mediaRelayMaxFilesBoot = s.MediaRelayMaxFiles
	MediaRelayMaxConcurrency = s.MediaRelayMaxConcurrency
	mediaRelayMaxConcurrencyBoot = s.MediaRelayMaxConcurrency
	PdfTextFastpathEnabled = s.PdfTextFastpathEnabled
	pdfTextFastpathEnabledBoot = s.PdfTextFastpathEnabled
	mediaRelayLimiter.Reset(s.MediaRelayMaxConcurrency)
	ConvKeepDays = s.ConvKeepDays
	AcpLogKeepDays = s.AcpLogKeepDays
	AcpLogMaxMB = s.AcpLogMaxMB
	EngineIdleGrace = time.Duration(maxInt(s.EngineIdleGraceSec, 0)) * time.Second
	engineIdleGraceBoot = EngineIdleGrace
	engineIdleGraceEnvSet = s.EngineIdleGraceEnvSet
	AcpRecordDir = s.AcpRecordDir
	syncModelRegistryConfig(s)
	apiKeys = loadAPIKeys()
	GlobalStore.Init(GatewayStateDB)
}

func truncateRunes(s string, maxRunes int) string {
	return gwcfg.TruncateRunes(s, maxRunes)
}

func recordAcpLine(direction string, raw []byte) {
	if AcpRecordDir == "" {
		return
	}
	_ = os.MkdirAll(AcpRecordDir, 0700)
	var parsed map[string]any
	lineStr := string(bytes.TrimSpace(raw))
	if json.Unmarshal(raw, &parsed) == nil {
		if params, ok := parsed["params"].(map[string]any); ok {
			if blocks, okB := params["prompt"].([]any); okB {
				for _, b := range blocks {
					if bm, okM := b.(map[string]any); okM {
						if t, okT := bm["text"].(string); okT && len(t) > 200 {
							bm["text"] = truncateRunes(t, 200) + "...[truncated]"
						}
					}
				}
			}
		}
		if b2, err := redactLogJSON(parsed); err == nil {
			lineStr = string(b2)
		}
	}
	rec, _ := json.Marshal(map[string]any{
		"ts":   time.Now().UTC().Format(time.RFC3339Nano),
		"dir":  direction,
		"line": redactSecretsInString(lineStr),
	})
	acpRecordMu.Lock()
	if f, err := openPrivateLog(filepath.Join(AcpRecordDir, "acp-traffic.ndjson")); err == nil {
		_, _ = f.Write(append(rec, '\n'))
		_ = f.Close()
	}
	acpRecordMu.Unlock()
}

func envStr(k, def string) string { return gwcfg.EnvStr(k, def) }

func envInt(k string, def int) int { return gwcfg.EnvInt(k, def) }

func envIntAllowSigned(k string, def int) int { return gwcfg.EnvIntAllowSigned(k, def) }

func envBool(k string, def bool) bool { return gwcfg.EnvBool(k, def) }

func effectiveWafRulesFile() string {
	if p := strings.TrimSpace(WafRewriteRulesFile); p != "" {
		return p
	}
	if p := gwcfg.Current().WafRewriteRulesFile; p != "" {
		return p
	}
	if db := strings.TrimSpace(GatewayStateDB); db != "" {
		return filepath.Join(filepath.Dir(db), "waf_rewrite_rules.json")
	}
	return filepath.Join(ProjectHome, "runtime", "waf_rewrite_rules.json")
}

func effectiveMaxToolRelays() int {
	if s := gwcfg.Current(); s.MaxToolRelaysEnvSet {
		return s.MaxToolRelays
	}
	return MaxToolRelays
}

func effectiveParkedTtl() time.Duration {
	if s := gwcfg.Current(); s.ParkedTtlEnvSet {
		return s.ParkedTtl
	}
	return ParkedTtl
}

var (
	logMu         sync.Mutex
	logFileHandle *os.File
	logFilePath   string
	logFileSize   int64
	LogMaxBytes   = int64(envInt("AGY_LOG_MAX_MB", 20)) * 1024 * 1024
)

func rotateLogFileLocked(path string) {
	if logFileHandle != nil {
		_ = logFileHandle.Close()
		logFileHandle = nil
	}
	for i := 4; i >= 1; i-- {
		src := fmt.Sprintf("%s.%d", path, i)
		dst := fmt.Sprintf("%s.%d", path, i+1)
		if _, err := os.Stat(src); err == nil {
			_ = os.Rename(src, dst)
		}
	}
	_ = os.Rename(path, path+".1")
	logFileSize = 0
}

func writeGatewayLogLocked(line string) {
	if GatewayLogFile == "" {
		return
	}
	if logFileHandle != nil && logFilePath != GatewayLogFile {
		_ = logFileHandle.Close()
		logFileHandle = nil
	}
	if logFileHandle != nil && LogMaxBytes > 0 && logFileSize+int64(len(line)) > LogMaxBytes {
		rotateLogFileLocked(GatewayLogFile)
	}
	if logFileHandle == nil {
		_ = os.MkdirAll(filepath.Dir(GatewayLogFile), 0700)
		if f, err := openPrivateLog(GatewayLogFile); err == nil {
			logFileHandle = f
			logFilePath = GatewayLogFile
			if st, errS := f.Stat(); errS == nil {
				logFileSize = st.Size()
			} else {
				logFileSize = 0
			}
		}
	}
	if logFileHandle != nil {
		if n, err := logFileHandle.WriteString(line); err == nil {
			logFileSize += int64(n)
		}
	}
}

func LogEvent(category, message string, meta map[string]any) {
	ts := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	metaStr := ""
	if len(meta) > 0 {
		if b, err := redactLogJSON(meta); err == nil {
			metaStr = " " + string(b)
		}
	}
	line := redactSecretsInString(fmt.Sprintf("[%s] [%s] %s%s\n", ts, category, message, metaStr))
	os.Stdout.WriteString(line)
	logMu.Lock()
	writeGatewayLogLocked(line)
	if TmpLogFile != "" {
		if f, err := openPrivateLog(TmpLogFile); err == nil {
			_, _ = f.WriteString(line)
			_ = f.Close()
		}
	}
	logMu.Unlock()
}

func randomHex(nBytes int) string {
	b := make([]byte, nBytes)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func shortSha256(s string) string {
	return gwcfg.ShortSHA256(s)
}

func mcpToolName(hostName string) string {
	return protocol.McpToolName(hostName)
}

func writeFileIfChanged(path string, content []byte, perm os.FileMode) {
	if existing, err := os.ReadFile(path); err == nil && bytes.Equal(existing, content) {
		return
	}
	_ = os.MkdirAll(filepath.Dir(path), 0755)
	_ = os.WriteFile(path, content, perm)
}

func isWindowsCDrivePath(p string) bool {
	return gwcfg.IsWindowsCDrivePath(p)
}

func seedDefaultCliProject(projectPath string) {
	if isWindowsCDrivePath(projectPath) {
		return
	}
	if raw, err := os.ReadFile(projectPath); err == nil {
		var existing map[string]any
		if json.Unmarshal(raw, &existing) == nil && existing["id"] == "default-cli-project" {
			return
		}
	}
	b, _ := json.MarshalIndent(map[string]any{
		"id":               "default-cli-project",
		"name":             "CLI Project",
		"projectResources": map[string]any{},
	}, "", "  ")
	writeFileIfChanged(projectPath, append(b, '\n'), 0644)
}

func seedOnboardingFile(onboardingPath string) {
	if isWindowsCDrivePath(onboardingPath) {
		return
	}
	existing := map[string]any{}
	if raw, err := os.ReadFile(onboardingPath); err == nil {
		_ = json.Unmarshal(raw, &existing)
		if existing == nil {
			existing = map[string]any{}
		}
	}
	cDone, _ := existing["consumerOnboardingComplete"].(bool)
	oDone, _ := existing["onboardingComplete"].(bool)
	if cDone && oDone {
		return
	}
	existing["consumerOnboardingComplete"] = true
	if _, hasEnt := existing["enterpriseOnboardingComplete"].(bool); !hasEnt {
		existing["enterpriseOnboardingComplete"] = false
	}
	existing["onboardingComplete"] = true
	b, err := json.Marshal(existing)
	if err != nil {
		return
	}
	_ = os.MkdirAll(filepath.Dir(onboardingPath), 0700)
	tmpPath := fmt.Sprintf("%s.tmp.%d.%d", onboardingPath, os.Getpid(), isolatedTmpSeq.Add(1))
	if err := os.WriteFile(tmpPath, append(b, '\n'), 0600); err == nil {
		if err := os.Rename(tmpPath, onboardingPath); err == nil {
			return
		}
		_ = os.Remove(tmpPath)
	}
	_ = os.WriteFile(onboardingPath, append(b, '\n'), 0600)
}

func writeBridgeAgentPlugin(bridgeName string) {
	agentDir := filepath.Join(PureApiHome, "config", "plugins", bridgeName, "agents")
	_ = os.MkdirAll(agentDir, 0755)
	pluginJson := filepath.Join(PureApiHome, "config", "plugins", bridgeName, "plugin.json")
	writeFileIfChanged(pluginJson, []byte(fmt.Sprintf("{\"name\":%q}\n", bridgeName)), 0644)
	agentMd := filepath.Join(agentDir, bridgeName+".md")
	// Keep <identity> byte-identical to bin/agy-acp-900k's LINT.IfChange(antigravity_identity) block
	// so upstream abuse.pi sees native Antigravity identity without CLI-tool boilerplate mismatches.
	mdContent := strings.Join([]string{
		"---",
		"name: " + bridgeName,
		`description: "Primary-model workspace bridge"`,
		"excludeDefaultComponents: true",
		"mainAgent: true",
		"inheritCustomizations: false",
		"tools:",
		"  - search_web",
		"---",
		"<identity>",
		"You are Antigravity, a powerful agentic AI coding assistant designed by the Google Deepmind team working on Advanced Agentic Coding.",
		"You are pair programming with a USER to solve their coding task. The task may require creating a new codebase, modifying or debugging an existing codebase, or simply answering a question.",
		"The USER will send you requests, which you must always prioritize addressing. User requests are enclosed within <USER_REQUEST> tags.",
		"</identity>",
		"SPECIAL INSTRUCTION: think silently if needed. EFFORT LEVEL: 1.00.",
		fmt.Sprintf("Follow the appended system instructions. Use the %s MCP tools for all workspace file and command operations, and use built-in search_web when web search is needed.", bridgeName),
		"",
	}, "\n")
	writeFileIfChanged(agentMd, []byte(mdContent), 0644)
}

func writeImageGenAgentPlugin() {
	agentDir := filepath.Join(PureApiHome, "config", "plugins", ImageGenAgentName, "agents")
	_ = os.MkdirAll(agentDir, 0755)
	pluginJson := filepath.Join(PureApiHome, "config", "plugins", ImageGenAgentName, "plugin.json")
	writeFileIfChanged(pluginJson, []byte(fmt.Sprintf("{\"name\":%q}\n", ImageGenAgentName)), 0644)
	agentMd := filepath.Join(agentDir, ImageGenAgentName+".md")
	mdContent := strings.Join([]string{
		"---",
		"name: " + ImageGenAgentName,
		`description: "Dedicated image generation and editing agent"`,
		"excludeDefaultComponents: true",
		"mainAgent: true",
		"subagent: false",
		"inheritCustomizations: false",
		"tools:",
		"  - generate_image",
		"---",
		"<identity>",
		"You are Antigravity, a powerful agentic AI assistant designed by the Google Deepmind team.",
		"You are working with a USER to solve their image generation or editing task.",
		"The USER will send you requests, which you must always prioritize addressing. User requests are enclosed within <USER_REQUEST> tags.",
		"</identity>",
		"SPECIAL INSTRUCTION: think silently if needed. EFFORT LEVEL: 0.25.",
		"For every user request, immediately invoke the built-in generate_image tool once with the exact Prompt, ImageName, AspectRatio, and optional ImagePaths requested. Do not ask follow-up questions or write lengthy prose.",
		"",
	}, "\n")
	writeFileIfChanged(agentMd, []byte(mdContent), 0644)
}

const (
	DefaultAppDataDirName    = "antigravity-cli"
	ImageGenAppDataDirName   = "antigravity-cli-imagegen"
	MediaRelayAppDataDirName = "antigravity-cli-media"
	MediaRelayHomeSubdir     = "media-home"
	UsageAppDataDirName      = "antigravity-cli-usage"
)

var (
	isolatedAppDataMu sync.Mutex
	isolatedTmpSeq    atomic.Uint64
)

// syncIsolatedAuthSymlink maintains a symlink at dstPath pointing to srcPath when srcPath exists,
// atomically refreshing dangling or mispointed symlinks and cleaning stale dangling links when
// srcPath is absent. When forceReplace is true, a materialized regular file at dstPath is replaced
// with a symlink to srcPath so auth/identity files never silently diverge after OAuth re-auth or
// atomic rename refreshes. Never reads or prints token file contents (C3).
func syncIsolatedAuthSymlink(srcPath, dstPath string, forceReplace bool) {
	if srcPath == "" || dstPath == "" || isWindowsCDrivePath(srcPath) || isWindowsCDrivePath(dstPath) {
		return
	}
	absSrc, err := filepath.Abs(srcPath)
	if err != nil || absSrc == "" || isWindowsCDrivePath(absSrc) {
		return
	}
	if _, err := os.Stat(absSrc); err != nil {
		// If source does not exist, clean up any dangling symlink at dstPath.
		if fi, lerr := os.Lstat(dstPath); lerr == nil && fi.Mode()&os.ModeSymlink != 0 {
			if _, serr := os.Stat(dstPath); serr != nil {
				_ = os.Remove(dstPath)
			}
		}
		return
	}
	if fi, lerr := os.Lstat(dstPath); lerr == nil {
		if fi.Mode()&os.ModeSymlink != 0 {
			if cur, rerr := os.Readlink(dstPath); rerr == nil && cur == absSrc {
				if _, serr := os.Stat(dstPath); serr == nil {
					return
				}
			}
		} else if !forceReplace || fi.IsDir() {
			// Preserve an explicit regular file/dir rather than clobbering user data unless forceReplace is set.
			return
		}
	}
	_ = os.MkdirAll(filepath.Dir(dstPath), 0700)
	tmpLink := fmt.Sprintf("%s.tmp.%d.%d.%d", dstPath, os.Getpid(), time.Now().UnixNano(), isolatedTmpSeq.Add(1))
	if err := os.Symlink(absSrc, tmpLink); err == nil {
		if err := os.Rename(tmpLink, dstPath); err == nil {
			return
		}
		_ = os.Remove(tmpLink)
	}
	_ = os.Remove(dstPath)
	_ = os.Symlink(absSrc, dstPath)
}

// ensureIsolatedAppDataDir creates a dedicated <geminiDir>/<subDirName> directory for auxiliary
// agy commands (`ws-imagegen`, `/usage`, `/credits`, `models`) that symlinks ONLY the minimal
// auth/identity files from <geminiDir>/antigravity-cli while NEVER sharing `conversations/`,
// `conversation_summaries.db`, `plugin_data/`, or `brain/` with the main `ws-bridge` engine.
// Prevents agy 1.5.x's startup scan of thousands of ws-bridge conversation DBs (#1825) and
// shields auxiliary workloads from main-session state corruption.
func ensureIsolatedAppDataDir(geminiDir, subDirName string) string {
	cleanGem := strings.TrimSpace(geminiDir)
	cleanSub := strings.TrimSpace(subDirName)
	if cleanGem == "" || cleanSub == "" || cleanSub == DefaultAppDataDirName ||
		isWindowsCDrivePath(cleanGem) || strings.ContainsAny(cleanSub, `/\`) || cleanSub == "." || cleanSub == ".." {
		return ""
	}
	absGem, err := filepath.Abs(cleanGem)
	if err != nil || absGem == "" || isWindowsCDrivePath(absGem) {
		return ""
	}
	srcAppDir := filepath.Join(absGem, DefaultAppDataDirName)
	dstAppDir := filepath.Join(absGem, cleanSub)
	if isWindowsCDrivePath(dstAppDir) {
		return ""
	}
	isolatedAppDataMu.Lock()
	defer isolatedAppDataMu.Unlock()

	_ = os.MkdirAll(filepath.Join(dstAppDir, "cache"), 0700)

	// Whitelist ONLY auth/identity files read by agy 1.5.16+ (never conversations/ or plugin_data/).
	for _, entry := range []struct {
		rel          string
		forceReplace bool
	}{
		{rel: "antigravity-oauth-token", forceReplace: true},
		{rel: "installation_id", forceReplace: true},
		{rel: "settings.json", forceReplace: false},
		{rel: filepath.Join("cache", "antigravity-keyring-unavailable"), forceReplace: false},
	} {
		syncIsolatedAuthSymlink(filepath.Join(srcAppDir, entry.rel), filepath.Join(dstAppDir, entry.rel), entry.forceReplace)
	}
	// Seed an independent local onboarding marker to avoid cross-process atomic-rename races.
	seedOnboardingFile(filepath.Join(dstAppDir, "cache", "onboarding.json"))
	return dstAppDir
}

func EnsurePureApiHome() {
	if isWindowsCDrivePath(PureApiHome) {
		return
	}
	writeBridgeAgentPlugin(McpBridgeName)
	writeBridgeAgentPlugin(LegacyMcpBridgeName)
	writeImageGenAgentPlugin()
	seedDefaultCliProject(filepath.Join(PureApiHome, "config", "projects", "default-cli-project.json"))

	// App data (oauth token, conversations, logs). Locally it may be a symlink to ~/.gemini/antigravity-cli
	// (AGY_APPDATA_LINK); in the container it is a plain directory on the data volume.
	appDataLink := filepath.Join(PureApiHome, DefaultAppDataDirName)
	if _, err := os.Lstat(appDataLink); os.IsNotExist(err) {
		if target := gwcfg.Current().AppDataLink; target != "" && !isWindowsCDrivePath(target) {
			_ = os.Symlink(target, appDataLink)
		} else {
			_ = os.MkdirAll(appDataLink, 0700)
		}
	}
	seedOnboardingFile(filepath.Join(appDataLink, "cache", "onboarding.json"))
	ensureIsolatedAppDataDir(PureApiHome, ImageGenAppDataDirName)
	ensureIsolatedAppDataDir(PureApiHome, UsageAppDataDirName)
}

func ConversationsDir() string {
	if v := gwcfg.Current().ConvDirEnv; v != "" {
		return v
	}
	return filepath.Join(PureApiHome, "antigravity-cli", "conversations")
}

func sameDirOrPath(a, b string) bool {
	if a == "" || b == "" {
		return false
	}
	ca := filepath.Clean(a)
	cb := filepath.Clean(b)
	if ca == cb {
		return true
	}
	ia, errA := os.Stat(ca)
	ib, errB := os.Stat(cb)
	return errA == nil && errB == nil && os.SameFile(ia, ib)
}

func isAppConvExplicitlyDisabled() bool {
	switch strings.ToLower(gwcfg.Current().AppConvDirRaw) {
	case "off", "none", "0", "false", "no":
		return true
	default:
		return false
	}
}

// rawConfiguredOrDiscoveredAppConvDir returns the cleaned AGY_APP_CONV_DIR or auto-discovered
// sibling Desktop App conversations dir WITHOUT suppressing when it aliases ConversationsDir().
// Used exclusively by cleanup safety guards so aliasing ConversationsDir() with the Desktop App
// directory can never bypass read-only protection.
func rawConfiguredOrDiscoveredAppConvDir() string {
	if isAppConvExplicitlyDisabled() {
		return ""
	}
	snap := gwcfg.Current()
	if raw := snap.AppConvDirRaw; raw != "" {
		clean := filepath.Clean(raw)
		if isWindowsCDrivePath(clean) {
			return ""
		}
		return clean
	}
	appdataLink := snap.AppDataLink
	if appdataLink == "" || isWindowsCDrivePath(appdataLink) {
		return ""
	}
	candidate := filepath.Join(filepath.Dir(filepath.Clean(appdataLink)), "antigravity", "conversations")
	if isWindowsCDrivePath(candidate) {
		return ""
	}
	if info, err := os.Stat(candidate); err == nil && info.IsDir() {
		return candidate
	}
	return ""
}

// AppConversationsDir returns the strictly READ-ONLY Antigravity Desktop App conversations
// directory (e.g. ~/.gemini/antigravity/conversations).
//   - Controlled by AGY_APP_CONV_DIR: "off"/"none"/"0"/"false"/"no" disables it; any other
//     non-empty path (unless Windows C: drive or identical to ConversationsDir()) is used.
//   - When AGY_APP_CONV_DIR is unset: auto-discovers sibling <parent(AGY_APPDATA_LINK)>/antigravity/conversations
//     ONLY when AGY_CONV_DIR is unset and AGY_APPDATA_LINK is non-empty, keeping isolated test
//     instances (go test, fake_instance.py, non-18890 gw.sh) 100% hermetic.
func AppConversationsDir() string {
	if isAppConvExplicitlyDisabled() {
		return ""
	}
	snap := gwcfg.Current()
	if raw := snap.AppConvDirRaw; raw != "" {
		clean := filepath.Clean(raw)
		if isWindowsCDrivePath(clean) || sameDirOrPath(clean, ConversationsDir()) {
			return ""
		}
		return clean
	}
	if snap.ConvDirEnv != "" {
		return ""
	}
	appdataLink := snap.AppDataLink
	if appdataLink == "" || isWindowsCDrivePath(appdataLink) {
		return ""
	}
	candidate := filepath.Join(filepath.Dir(filepath.Clean(appdataLink)), "antigravity", "conversations")
	if isWindowsCDrivePath(candidate) || sameDirOrPath(candidate, ConversationsDir()) {
		return ""
	}
	if info, err := os.Stat(candidate); err == nil && info.IsDir() {
		return candidate
	}
	return ""
}
