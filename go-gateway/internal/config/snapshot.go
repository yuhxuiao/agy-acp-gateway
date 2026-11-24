package config

import (
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Snapshot holds all process configuration parsed from environment variables at startup or Reload.
// Callers must treat the returned *Snapshot and any collections accessed via it as immutable.
type Snapshot struct {
	ProjectHome            string
	PatchedAgyBin          string
	PureApiHome            string
	DefaultCwd             string
	MaxLiveWorkers         int
	MaxProbeWorkers        int
	MaxConcurrentPrompts   int
	IdleWorkerTtl          time.Duration
	ParkedTtl              time.Duration
	ParkedTtlEnvSet        bool
	MaxToolRelays          int
	MaxToolRelaysEnvSet    bool
	MaxBodyBytes           int64
	allowedCorsOrigins     map[string]bool
	ListenHost             string
	GatewayPort            int
	DashboardPort          int
	GatewayLogFile         string
	GatewayStateDB         string
	WafRewriteRulesFile    string
	WafAutoBisectEnabled   bool
	TosTrippedFile         string
	TmpLogFile             string
	McpTrace               bool
	LogHeaders             bool
	UseG1Credits           bool
	CacheWarnMinTokens     int
	CacheWarnStreak        int
	PrefillCancelMinElap   time.Duration
	PrefillCancelGrace     time.Duration
	PrefillMinElapEnvSet   bool
	PrefillGraceEnvSet     bool
	CapacityFallbackTTL    time.Duration
	CapacityFallbackMaxTTL time.Duration
	MaxImagesGemini        int
	MaxImagesClaude        int
	MaxImageBytes          int
	MaxPromptImageBytes    int
	MaxTextFileBytes       int
	FetchRemoteImages      bool
	ConvKeepDays           int
	AcpRecordDir           string
	AppDataLink            string
	ConvDirEnv             string
	AppConvDirRaw          string
	UpgradeDirEnv          string
	UpstreamProxy          string
	ChildGoMemLimit        string
	ChildGatewayGoMemLimit string
	ChildGoGC              string
	QuotaBin               string
	APIKeysRaw             string
	APIKeysFile            string
	GoGC                   string
	GoMemLimit             string
	RouteClaudeNames       bool
	ExposeTierModels       bool
	DefaultModel           string
	ContextGemini          int
	TranscriptCharsGemini  int
	ContextClaude          int
	TranscriptCharsClaude  int
	InstanceID             string
	InstanceLabel          string
	DashboardPeers         []DashboardPeerConfig
	ChildAccountWorker     bool
	AccountsFile           string
	ProxyPresetsFile       string
	ResinProxyToken        string
	ResinHost              string
	AccountPlanTierHint    string
}

// DashboardPeerConfig describes a peer gateway instance aggregated into the primary dashboard.
type DashboardPeerConfig struct {
	ID      string
	Label   string
	BaseURL string
	APIKey  string
}

// ParseDashboardPeers parses AGY_DASHBOARD_PEERS entries formatted as
// "<id>|<label>|<base_url>[|<api_key>]" (or "<id>=<base_url>"), separated by ";" or "," or newlines.
func ParseDashboardPeers(raw string) []DashboardPeerConfig {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	normalized := strings.ReplaceAll(raw, "\r\n", ";")
	normalized = strings.ReplaceAll(normalized, "\n", ";")
	normalized = strings.ReplaceAll(normalized, ",", ";")
	var out []DashboardPeerConfig
	seen := make(map[string]bool)
	for _, entry := range strings.Split(normalized, ";") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		var peer DashboardPeerConfig
		if strings.Contains(entry, "|") {
			parts := strings.Split(entry, "|")
			if len(parts) < 3 {
				continue
			}
			peer.ID = strings.TrimSpace(parts[0])
			peer.Label = strings.TrimSpace(parts[1])
			peer.BaseURL = strings.TrimRight(strings.TrimSpace(parts[2]), "/")
			if len(parts) >= 4 {
				peer.APIKey = strings.TrimSpace(parts[3])
			}
		} else if idx := strings.Index(entry, "="); idx > 0 {
			peer.ID = strings.TrimSpace(entry[:idx])
			peer.Label = peer.ID
			peer.BaseURL = strings.TrimRight(strings.TrimSpace(entry[idx+1:]), "/")
		}
		if peer.ID == "" || peer.BaseURL == "" || seen[peer.ID] {
			continue
		}
		if peer.Label == "" {
			peer.Label = peer.ID
		}
		seen[peer.ID] = true
		out = append(out, peer)
	}
	return out
}

var currentSnap atomic.Pointer[Snapshot]

func init() {
	Reload()
}

// Current returns the immutable active configuration snapshot.
// In production (!testing.Testing()) it reads the atomic pointer loaded at startup/Reload with zero os.Getenv calls.
// Under `go test` (`testing.Testing()`), it refreshes from the current test environment so tests using
// t.Setenv without calling initConfig observe updated values immediately.
func Current() *Snapshot {
	if testing.Testing() {
		return LoadFromEnv()
	}
	if s := currentSnap.Load(); s != nil {
		return s
	}
	return Reload()
}

// Loaded returns the most recently stored startup/Reload snapshot without re-reading environment variables.
func Loaded() *Snapshot {
	if s := currentSnap.Load(); s != nil {
		return s
	}
	return Reload()
}

// Reload parses the current process environment into a fresh immutable Snapshot and publishes it atomically.
func Reload() *Snapshot {
	s := LoadFromEnv()
	currentSnap.Store(s)
	return s
}

// AllowedCorsOrigin reports whether origin is present in the configured CORS allow-list.
func (s *Snapshot) AllowedCorsOrigin(origin string) bool {
	if s == nil {
		return false
	}
	return s.allowedCorsOrigins[origin]
}

// CloneCorsOrigins returns a fresh defensive copy of the configured CORS allow-list.
func (s *Snapshot) CloneCorsOrigins() map[string]bool {
	out := make(map[string]bool)
	if s == nil {
		return out
	}
	for k, v := range s.allowedCorsOrigins {
		out[k] = v
	}
	return out
}

// ParseCorsOrigins parses a comma-separated CORS origins list into a set.
func ParseCorsOrigins(raw string) map[string]bool {
	out := make(map[string]bool)
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item != "" {
			out[item] = true
		}
	}
	return out
}

// EnvStr returns trimmed os.Getenv(k) when non-empty, otherwise def.
func EnvStr(k, def string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return def
}

// EnvInt returns positive integer env value for k, otherwise def.
func EnvInt(k string, def int) int {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

// EnvIntAllowSigned returns signed integer env value for k, otherwise def.
func EnvIntAllowSigned(k string, def int) int {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// EnvBool parses boolean env value for k, otherwise def.
func EnvBool(k string, def bool) bool {
	if v := strings.ToLower(strings.TrimSpace(os.Getenv(k))); v != "" {
		switch v {
		case "1", "true", "yes", "on":
			return true
		case "0", "false", "no", "off":
			return false
		}
	}
	return def
}

// ResolveProjectHome resolves the installation root directory (<root>/bin/agy-acp-gateway -> <root>).
func ResolveProjectHome() string {
	if v := strings.TrimSpace(os.Getenv("AGY_GW_HOME")); v != "" {
		return v
	}
	if exe, err := os.Executable(); err == nil {
		if real, err2 := filepath.EvalSymlinks(exe); err2 == nil {
			exe = real
		}
		dir := filepath.Dir(exe)
		if filepath.Base(dir) == "bin" {
			return filepath.Dir(dir)
		}
		return dir
	}
	if wd, err := os.Getwd(); err == nil {
		return wd
	}
	return "."
}

// LoadFromEnv reads all environment variables once into a standalone immutable Snapshot.
func LoadFromEnv() *Snapshot {
	home := ResolveProjectHome()
	parkedSec := EnvInt("AGY_PARKED_TTL_SEC", 0)
	parkedDur := 300 * time.Second
	if parkedSec > 0 {
		parkedDur = time.Duration(parkedSec) * time.Second
	}
	maxRelaysRaw := strings.TrimSpace(os.Getenv("AGY_MAX_TOOL_RELAYS"))
	maxRelaysEnvSet := false
	maxRelays := 4
	if maxRelaysRaw != "" {
		if n, err := strconv.Atoi(maxRelaysRaw); err == nil {
			maxRelays = n
			maxRelaysEnvSet = true
		}
	}
	gogc := strings.TrimSpace(os.Getenv("GOGC"))
	if gogc == "" {
		gogc = "25"
	}
	gomem := strings.TrimSpace(os.Getenv("GOMEMLIMIT"))
	if gomem == "" {
		gomem = "256MiB"
	}
	capFallbackSec := EnvIntAllowSigned("AGY_CAPACITY_FALLBACK_TTL_SEC", 300)
	if capFallbackSec < 0 {
		capFallbackSec = 300
	}
	capFallbackTTL := time.Duration(capFallbackSec) * time.Second
	capFallbackMaxSec := EnvInt("AGY_CAPACITY_FALLBACK_MAX_TTL_SEC", 1800)
	capFallbackMaxTTL := time.Duration(capFallbackMaxSec) * time.Second
	if capFallbackMaxTTL < capFallbackTTL {
		capFallbackMaxTTL = capFallbackTTL
	}
	return &Snapshot{
		ProjectHome:            home,
		PatchedAgyBin:          EnvStr("AGY_ACP_BIN", filepath.Join(home, "bin", "agy-acp-900k")),
		PureApiHome:            EnvStr("AGY_PURE_API_HOME", filepath.Join(home, "runtime", "pure-api-home")),
		DefaultCwd:             EnvStr("AGY_DEFAULT_CWD", os.TempDir()),
		MaxLiveWorkers:         EnvInt("AGY_MAX_LIVE_WORKERS", 8),
		MaxProbeWorkers:        EnvInt("AGY_MAX_PROBE_WORKERS", 1),
		MaxConcurrentPrompts:   EnvInt("AGY_MAX_CONCURRENT_PROMPTS", 6),
		IdleWorkerTtl:          time.Duration(EnvInt("AGY_IDLE_WORKER_TTL_SEC", 1200)) * time.Second,
		ParkedTtl:              parkedDur,
		ParkedTtlEnvSet:        parkedSec > 0,
		MaxToolRelays:          maxRelays,
		MaxToolRelaysEnvSet:    maxRelaysEnvSet,
		MaxBodyBytes:           int64(EnvInt("AGY_MAX_BODY_MB", 64)) * 1024 * 1024,
		allowedCorsOrigins:     ParseCorsOrigins(os.Getenv("AGY_CORS_ORIGINS")),
		ListenHost:             EnvStr("AGY_LISTEN_HOST", "127.0.0.1"),
		GatewayPort:            EnvInt("AGY_GW_PORT", 18890),
		DashboardPort:          EnvInt("AGY_DASH_PORT", 18891),
		GatewayLogFile:         EnvStr("AGY_GW_LOG", filepath.Join(home, "logs", "gateway.log")),
		GatewayStateDB:         EnvStr("AGY_STATE_DB", filepath.Join(home, "runtime", "gateway.db")),
		WafRewriteRulesFile:    strings.TrimSpace(os.Getenv("AGY_WAF_RULES_FILE")),
		WafAutoBisectEnabled:   EnvBool("AGY_WAF_AUTOBISECT", true),
		TosTrippedFile:         EnvStr("AGY_TOS_TRIPPED_FILE", filepath.Join(home, "runtime", "tos-tripped")),
		TmpLogFile:             strings.TrimSpace(os.Getenv("AGY_GW_TMPLOG")),
		McpTrace:               os.Getenv("AGY_MCP_TRACE") == "1",
		LogHeaders:             os.Getenv("AGY_LOG_HEADERS") == "1",
		UseG1Credits:           os.Getenv("AGY_USE_G1_CREDITS") == "1",
		CacheWarnMinTokens:     EnvInt("AGY_CACHE_WARN_MIN_TOKENS", 80000),
		CacheWarnStreak:        EnvInt("AGY_CACHE_WARN_STREAK", 3),
		PrefillCancelMinElap:   time.Duration(EnvInt("AGY_PREFILL_CANCEL_MIN_ELAPSED_SEC", 30)) * time.Second,
		PrefillCancelGrace:     time.Duration(EnvInt("AGY_PREFILL_CANCEL_GRACE_SEC", 45)) * time.Second,
		PrefillMinElapEnvSet:   strings.TrimSpace(os.Getenv("AGY_PREFILL_CANCEL_MIN_ELAPSED_SEC")) != "",
		PrefillGraceEnvSet:     strings.TrimSpace(os.Getenv("AGY_PREFILL_CANCEL_GRACE_SEC")) != "",
		CapacityFallbackTTL:    capFallbackTTL,
		CapacityFallbackMaxTTL: capFallbackMaxTTL,
		MaxImagesGemini:        EnvInt("AGY_MAX_IMAGES_GEMINI", 200),
		MaxImagesClaude:        EnvInt("AGY_MAX_IMAGES_CLAUDE", 100),
		MaxImageBytes:          EnvInt("AGY_MAX_IMAGE_BYTES", 10*1024*1024),
		MaxPromptImageBytes:    EnvInt("AGY_MAX_PROMPT_IMAGE_BYTES", 32*1024*1024),
		MaxTextFileBytes:       EnvInt("AGY_MAX_TEXT_FILE_BYTES", 2*1024*1024),
		FetchRemoteImages:      EnvBool("AGY_FETCH_IMAGES", true),
		ConvKeepDays:           EnvIntAllowSigned("AGY_CONV_KEEP_DAYS", 0),
		AcpRecordDir:           strings.TrimSpace(os.Getenv("AGY_ACP_RECORD")),
		AppDataLink:            strings.TrimSpace(os.Getenv("AGY_APPDATA_LINK")),
		ConvDirEnv:             strings.TrimSpace(os.Getenv("AGY_CONV_DIR")),
		AppConvDirRaw:          strings.TrimSpace(os.Getenv("AGY_APP_CONV_DIR")),
		UpgradeDirEnv:          strings.TrimSpace(os.Getenv("AGY_UPGRADE_DIR")),
		UpstreamProxy:          strings.TrimSpace(os.Getenv("AGY_UPSTREAM_PROXY")),
		ChildGoMemLimit:        EnvStr("AGY_CHILD_GOMEMLIMIT", "180MiB"),
		ChildGatewayGoMemLimit: EnvStr("AGY_CHILD_GATEWAY_GOMEMLIMIT", "128MiB"),
		ChildGoGC:              EnvStr("AGY_CHILD_GOGC", "30"),
		QuotaBin:               strings.TrimSpace(os.Getenv("AGY_QUOTA_BIN")),
		APIKeysRaw:             os.Getenv("AGY_API_KEYS"),
		APIKeysFile:            os.Getenv("AGY_API_KEYS_FILE"),
		GoGC:                   gogc,
		GoMemLimit:             gomem,
		RouteClaudeNames:       os.Getenv("AGY_ROUTE_CLAUDE_NAMES") == "1",
		ExposeTierModels:       os.Getenv("AGY_EXPOSE_TIER_MODELS") == "1",
		DefaultModel:           strings.TrimSpace(os.Getenv("AGY_DEFAULT_MODEL")),
		ContextGemini:          EnvInt("AGY_CONTEXT_GEMINI", 1048576),
		TranscriptCharsGemini:  EnvInt("AGY_TRANSCRIPT_CHARS_GEMINI", 1800000),
		ContextClaude:          EnvInt("AGY_CONTEXT_CLAUDE", 1048576),
		TranscriptCharsClaude:  EnvInt("AGY_TRANSCRIPT_CHARS_CLAUDE", 1800000),
		InstanceID:             EnvStr("AGY_INSTANCE_ID", "primary"),
		InstanceLabel:          EnvStr("AGY_INSTANCE_LABEL", "主实例"),
		DashboardPeers:         ParseDashboardPeers(os.Getenv("AGY_DASHBOARD_PEERS")),
		ChildAccountWorker:     os.Getenv("AGY_CHILD_ACCOUNT_WORKER") == "1",
		AccountsFile:           strings.TrimSpace(os.Getenv("AGY_ACCOUNTS_FILE")),
		ProxyPresetsFile:       strings.TrimSpace(os.Getenv("AGY_PROXY_PRESETS_FILE")),
		ResinProxyToken:        strings.TrimSpace(os.Getenv("AGY_RESIN_PROXY_TOKEN")),
		ResinHost:              EnvStr("AGY_RESIN_HOST", "resin:2260"),
		AccountPlanTierHint:    strings.TrimSpace(os.Getenv("AGY_ACCOUNT_PLAN_TIER_HINT")),
	}
}

// InitProcessGCDefaults configures default Go GC and soft memory limit when GOGC / GOMEMLIMIT are unset.
func InitProcessGCDefaults() {
	if os.Getenv("GOGC") == "" {
		debug.SetGCPercent(25)
	}
	if os.Getenv("GOMEMLIMIT") == "" {
		debug.SetMemoryLimit(256 * 1024 * 1024)
	}
	Reload()
}
