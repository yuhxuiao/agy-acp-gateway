package main

import (
	"bytes"
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
	MaxProbeWorkers                = bootSnap.MaxProbeWorkers
	MaxConcurrentPrompts           = bootSnap.MaxConcurrentPrompts
	IdleWorkerTtl                  = bootSnap.IdleWorkerTtl
	ParkedTtl                      = bootSnap.ParkedTtl
	MaxToolRelays                  = bootSnap.MaxToolRelays
	MaxBodyBytes                   = bootSnap.MaxBodyBytes
	AllowedCorsOrigins             = bootSnap.CloneCorsOrigins()
	promptSem                      = make(chan struct{}, MaxConcurrentPrompts)
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
	ConvKeepDays                   = bootSnap.ConvKeepDays
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

func initConfig() {
	s := gwcfg.Reload()
	ProjectHome = s.ProjectHome
	PatchedAgyBin = s.PatchedAgyBin
	PureApiHome = s.PureApiHome
	DefaultCwd = s.DefaultCwd
	MaxLiveWorkers = s.MaxLiveWorkers
	MaxProbeWorkers = s.MaxProbeWorkers
	MaxConcurrentPrompts = s.MaxConcurrentPrompts
	IdleWorkerTtl = s.IdleWorkerTtl
	ParkedTtl = s.ParkedTtl
	MaxToolRelays = s.MaxToolRelays
	MaxBodyBytes = s.MaxBodyBytes
	AllowedCorsOrigins = s.CloneCorsOrigins()
	promptSem = make(chan struct{}, MaxConcurrentPrompts)
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
	ConvKeepDays = s.ConvKeepDays
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
		"tools:",
		"  - search_web",
		"  - read_url_content",
		"---",
		"<identity>",
		"You are Antigravity, a powerful agentic AI coding assistant designed by the Google Deepmind team working on Advanced Agentic Coding.",
		"You are pair programming with a USER to solve their coding task. The task may require creating a new codebase, modifying or debugging an existing codebase, or simply answering a question.",
		"The USER will send you requests, which you must always prioritize addressing. User requests are enclosed within <USER_REQUEST> tags.",
		"</identity>",
		"SPECIAL INSTRUCTION: think silently if needed. EFFORT LEVEL: 1.00.",
		fmt.Sprintf("Follow the appended system instructions. Use the %s MCP tools for all workspace file and command operations, and use built-in search_web / read_url_content when web search or URL reading is needed.", bridgeName),
		"",
	}, "\n")
	writeFileIfChanged(agentMd, []byte(mdContent), 0644)
}

func EnsurePureApiHome() {
	if isWindowsCDrivePath(PureApiHome) {
		return
	}
	writeBridgeAgentPlugin(McpBridgeName)
	writeBridgeAgentPlugin(LegacyMcpBridgeName)
	seedDefaultCliProject(filepath.Join(PureApiHome, "config", "projects", "default-cli-project.json"))

	// App data (oauth token, conversations, logs). Locally it may be a symlink to ~/.gemini/antigravity-cli
	// (AGY_APPDATA_LINK); in the container it is a plain directory on the data volume.
	appDataLink := filepath.Join(PureApiHome, "antigravity-cli")
	if _, err := os.Lstat(appDataLink); os.IsNotExist(err) {
		if target := gwcfg.Current().AppDataLink; target != "" && !isWindowsCDrivePath(target) {
			_ = os.Symlink(target, appDataLink)
		} else {
			_ = os.MkdirAll(appDataLink, 0700)
		}
	}
	seedOnboardingFile(filepath.Join(appDataLink, "cache", "onboarding.json"))
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
