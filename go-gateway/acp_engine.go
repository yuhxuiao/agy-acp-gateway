package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	AwaitToolName     = "dsh_await_result"
	CallToolName      = "dsh_call_tool"
	RelayAfterMs      = 85000
	UtilWorkerTtl     = 20 * time.Second
	MaxTranscriptChar = 1800000
)

// ProjectHome: project root (<root>/bin/agy-acp-gateway -> <root>); override with AGY_GW_HOME
var ProjectHome = resolveProjectHome()

func resolveProjectHome() string {
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

var (
	PatchedAgyBin                  = envStr("AGY_ACP_BIN", filepath.Join(ProjectHome, "bin", "agy-acp-900k"))
	PureApiHome                    = envStr("AGY_PURE_API_HOME", filepath.Join(ProjectHome, "runtime", "pure-api-home"))
	DefaultCwd                     = envStr("AGY_DEFAULT_CWD", os.TempDir())
	MaxLiveWorkers                 = envInt("AGY_MAX_LIVE_WORKERS", 8)
	MaxConcurrentPrompts           = envInt("AGY_MAX_CONCURRENT_PROMPTS", 6)
	IdleWorkerTtl                  = time.Duration(envInt("AGY_IDLE_WORKER_TTL_SEC", 360)) * time.Second
	ParkedTtl                      = time.Duration(envInt("AGY_PARKED_TTL_SEC", 1800)) * time.Second
	MaxBodyBytes                   = int64(envInt("AGY_MAX_BODY_MB", 64)) * 1024 * 1024
	AllowedCorsOrigins             = parseCorsOrigins(os.Getenv("AGY_CORS_ORIGINS"))
	promptSem                      = make(chan struct{}, MaxConcurrentPrompts)
	ListenHost                     = envStr("AGY_LISTEN_HOST", "127.0.0.1")
	GatewayPort                    = envInt("AGY_GW_PORT", 18890)
	DashboardPort                  = envInt("AGY_DASH_PORT", 18891)
	GatewayLogFile                 = envStr("AGY_GW_LOG", filepath.Join(ProjectHome, "logs", "gateway.log"))
	GatewayStateDB                 = envStr("AGY_STATE_DB", filepath.Join(ProjectHome, "runtime", "gateway.db"))
	TmpLogFile                     = envStr("AGY_GW_TMPLOG", "")
	McpTrace                       = os.Getenv("AGY_MCP_TRACE") == "1"
	LogHeaders                     = os.Getenv("AGY_LOG_HEADERS") == "1"
	UseG1Credits                   = os.Getenv("AGY_USE_G1_CREDITS") == "1"
	AcpRecordDir                   = strings.TrimSpace(os.Getenv("AGY_ACP_RECORD"))
	mcpToolNameRe                  = regexp.MustCompile(`[^a-zA-Z0-9_-]`)
	acpRecordMu                    sync.Mutex
	argonDegradationDetectedTotal  atomic.Int64
	argonDegradationRecoveredTotal atomic.Int64
)

func parseCorsOrigins(raw string) map[string]bool {
	out := make(map[string]bool)
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item != "" {
			out[item] = true
		}
	}
	return out
}

func initConfig() {
	ProjectHome = resolveProjectHome()
	PatchedAgyBin = envStr("AGY_ACP_BIN", filepath.Join(ProjectHome, "bin", "agy-acp-900k"))
	PureApiHome = envStr("AGY_PURE_API_HOME", filepath.Join(ProjectHome, "runtime", "pure-api-home"))
	DefaultCwd = envStr("AGY_DEFAULT_CWD", os.TempDir())
	MaxLiveWorkers = envInt("AGY_MAX_LIVE_WORKERS", 8)
	MaxConcurrentPrompts = envInt("AGY_MAX_CONCURRENT_PROMPTS", 6)
	IdleWorkerTtl = time.Duration(envInt("AGY_IDLE_WORKER_TTL_SEC", 360)) * time.Second
	ParkedTtl = time.Duration(envInt("AGY_PARKED_TTL_SEC", 1800)) * time.Second
	MaxBodyBytes = int64(envInt("AGY_MAX_BODY_MB", 64)) * 1024 * 1024
	AllowedCorsOrigins = parseCorsOrigins(os.Getenv("AGY_CORS_ORIGINS"))
	promptSem = make(chan struct{}, MaxConcurrentPrompts)
	ListenHost = envStr("AGY_LISTEN_HOST", "127.0.0.1")
	GatewayPort = envInt("AGY_GW_PORT", 18890)
	DashboardPort = envInt("AGY_DASH_PORT", 18891)
	GatewayLogFile = envStr("AGY_GW_LOG", filepath.Join(ProjectHome, "logs", "gateway.log"))
	GatewayStateDB = envStr("AGY_STATE_DB", filepath.Join(ProjectHome, "runtime", "gateway.db"))
	TosTrippedFile = envStr("AGY_TOS_TRIPPED_FILE", filepath.Join(ProjectHome, "runtime", "tos-tripped"))
	TmpLogFile = strings.TrimSpace(os.Getenv("AGY_GW_TMPLOG"))
	McpTrace = os.Getenv("AGY_MCP_TRACE") == "1"
	LogHeaders = os.Getenv("AGY_LOG_HEADERS") == "1"
	UseG1Credits = os.Getenv("AGY_USE_G1_CREDITS") == "1"
	AcpRecordDir = strings.TrimSpace(os.Getenv("AGY_ACP_RECORD"))
	apiKeys = loadAPIKeys()
	GlobalStore.Init(GatewayStateDB)
}

func truncateRunes(s string, maxRunes int) string {
	if maxRunes <= 0 || len(s) <= maxRunes {
		return s
	}
	r := []rune(s)
	if len(r) <= maxRunes {
		return s
	}
	return string(r[:maxRunes])
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

func envStr(k, def string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
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
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])[:16]
}

func mcpToolName(hostName string) string {
	clean := mcpToolNameRe.ReplaceAllString(hostName, "_")
	if len(clean) > 40 {
		clean = clean[:40]
	}
	return fmt.Sprintf("dsh_%s_%s", clean, shortSha256(hostName)[:12])
}

func EnsurePureApiHome() {
	agentDir := filepath.Join(PureApiHome, "config", "plugins", "dsh-bridge", "agents")
	_ = os.MkdirAll(agentDir, 0755)
	pluginJson := filepath.Join(PureApiHome, "config", "plugins", "dsh-bridge", "plugin.json")
	if _, err := os.Stat(pluginJson); os.IsNotExist(err) {
		_ = os.WriteFile(pluginJson, []byte("{\"name\":\"dsh-bridge\"}\n"), 0644)
	}
	agentMd := filepath.Join(agentDir, "dsh-bridge.md")
	mdContent := strings.Join([]string{
		"---",
		"name: dsh-bridge",
		`description: "Pure API bridge: host harness tools + built-in Google web search"`,
		"excludeDefaultComponents: true",
		"mainAgent: true",
		"tools:",
		"  - search_web",
		"  - read_url_content",
		"---",
		"You are Antigravity Agent, a powerful agentic AI coding assistant designed by the Google engineering team.",
		"SPECIAL INSTRUCTION: think silently if needed. EFFORT LEVEL: 1.00.",
		"Follow the appended system instructions. Use the dsh-bridge MCP tools for all workspace file and command operations, and use built-in search_web / read_url_content when web search or URL reading is needed.",
		"",
	}, "\n")
	_ = os.WriteFile(agentMd, []byte(mdContent), 0644)
	// App data (oauth token, conversations, logs). Locally it may be a symlink to ~/.gemini/antigravity-cli
	// (AGY_APPDATA_LINK); in the container it is a plain directory on the data volume.
	appDataLink := filepath.Join(PureApiHome, "antigravity-cli")
	if _, err := os.Lstat(appDataLink); os.IsNotExist(err) {
		if target := strings.TrimSpace(os.Getenv("AGY_APPDATA_LINK")); target != "" {
			_ = os.Symlink(target, appDataLink)
		} else {
			_ = os.MkdirAll(appDataLink, 0700)
		}
	}
}

func ConversationsDir() string {
	return envStr("AGY_CONV_DIR", filepath.Join(PureApiHome, "antigravity-cli", "conversations"))
}

type ToolDef struct {
	Name        string         `json:"name"`
	McpName     string         `json:"mcpName"`
	Description string         `json:"description"`
	Parameters  map[string]any `json:"parameters"`
}

type ToolCallItem struct {
	ID    string         `json:"id"`
	Name  string         `json:"name"`
	Input map[string]any `json:"input"`
}

type ToolResultInput struct {
	ToolCallID string
	Content    string
	Images     []map[string]any
	IsError    bool
}

type ParkedMcpCall struct {
	Call       ToolCallItem
	ResultChan chan map[string]any
	Delivered  bool
}

type TurnUsage struct {
	InputTokens                int                     `json:"inputTokens"`
	CachedReadTokens           int                     `json:"cachedReadTokens"`
	OutputTokens               int                     `json:"outputTokens"`
	ThinkingTokens             int                     `json:"thinkingTokens,omitempty"`
	RawUncachedInputTokens     int                     `json:"rawUncachedInputTokens,omitempty"`
	RawTpuCachedTokens         int                     `json:"rawTpuCachedTokens,omitempty"`
	CumulativeInputTokens      int                     `json:"cumulativeInputTokens,omitempty"`
	CumulativeCachedReadTokens int                     `json:"cumulativeCachedReadTokens,omitempty"`
	StepsInPrompt              int                     `json:"stepsInPrompt"`
	UsageSource                string                  `json:"usageSource,omitempty"`
	Degradation                *ArgonDegradationReport `json:"degradation,omitempty"`
}

type TurnResult struct {
	StopReason    string         `json:"stopReason"`
	Text          string         `json:"text"`
	Thinking      string         `json:"thinking"`
	ToolCalls     []ToolCallItem `json:"toolCalls"`
	Usage         *TurnUsage     `json:"usage"`
	SessionID     string         `json:"sessionId"`
	ConvKey       string         `json:"convKey"`
	ElapsedMs     int            `json:"elapsedMs"`
	TtftMs        int            `json:"ttftMs"`
	Mode          string         `json:"mode"`
	EmittedDeltas int            `json:"-"`
}

type TurnHistoryEntry struct {
	TurnIndex      int        `json:"turnIndex"`
	CompletedAtMs  int64      `json:"completedAtMs"`
	Ts             string     `json:"ts"`
	SessionID      string     `json:"sessionId"`
	Mode           string     `json:"mode"`
	ElapsedMs      int        `json:"elapsedMs"`
	TtftMs         int        `json:"ttftMs"`
	Tps            float64    `json:"tps"`
	HasThinking    bool       `json:"hasThinking"`
	ThinkingTokens int        `json:"thinkingTokens"`
	Usage          *TurnUsage `json:"usage"`
	CacheHitRate   string     `json:"cacheHitRate"`
	StopReason     string     `json:"stopReason"`
	Model          string     `json:"model"`
	ThoughtLevel   string     `json:"thoughtLevel"`
}

type DeltaChunk struct {
	Type     string
	Text     string
	Thinking string
}

type rpcResult struct {
	Result json.RawMessage
	Err    error
}

type SharedAcpEngine struct {
	capsMu           sync.RWMutex
	caps             acpCapabilities
	mu               sync.Mutex
	cmd              *exec.Cmd
	exitDone         chan struct{}
	stdin            *bufio.Writer
	stdinMu          sync.Mutex
	nextID           atomic.Int64
	pending          sync.Map // int64 -> chan rpcResult
	sessionListeners sync.Map // sessionId -> *AcpProjectSession
	booting          bool
	stopping         bool
	bootCond         *sync.Cond
}

var GlobalEngine = newSharedAcpEngine()

func newSharedAcpEngine() *SharedAcpEngine {
	e := &SharedAcpEngine{}
	e.bootCond = sync.NewCond(&e.mu)
	return e
}

func (e *SharedAcpEngine) IsAlive() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.cmd != nil && e.cmd.Process != nil
}

func (e *SharedAcpEngine) IsBooting() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.booting || e.stopping
}

func (e *SharedAcpEngine) EnsureStarted() error {
	if err := circuitError(); err != nil {
		return err
	}
	e.mu.Lock()
	for e.booting || e.stopping {
		e.bootCond.Wait()
	}
	if e.cmd != nil && e.cmd.Process != nil {
		e.mu.Unlock()
		return circuitError()
	}
	e.booting = true
	e.mu.Unlock()

	defer func() {
		e.mu.Lock()
		e.booting = false
		e.bootCond.Broadcast()
		e.mu.Unlock()
	}()

	EnsurePureApiHome()
	gomem := os.Getenv("GOMEMLIMIT")
	if gomem == "" {
		gomem = "180MiB"
	}
	gogc := os.Getenv("GOGC")
	if gogc == "" {
		gogc = "30"
	}
	// Upstream proxy for agy -> Google. Empty = direct (servers outside CN); local WSL sets AGY_UPSTREAM_PROXY.
	proxy := strings.TrimSpace(os.Getenv("AGY_UPSTREAM_PROXY"))

	LogEvent("ACP_ENGINE_START", "Starting single shared agy-acp-900k Pure-API engine (Go Native)", map[string]any{
		"home":       PureApiHome,
		"gomemlimit": gomem,
	})

	cmd := exec.Command(
		PatchedAgyBin,
		"acp",
		"--gemini-dir", PureApiHome,
		"--app-data-dir", "antigravity-cli",
		"--agent", "dsh-bridge",
		"--process-per-session",
		"--disable-telemetry",
	)
	cmd.Dir = DefaultCwd
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	env := os.Environ()
	env = append(env,
		"GOGC="+gogc,
		"GOMEMLIMIT="+gomem,
		"DBUS_SESSION_BUS_ADDRESS=unix:path=/dev/null",
		"HTTP_PROXY="+proxy,
		"HTTPS_PROXY="+proxy,
		"NO_PROXY=127.0.0.1,localhost,::1",
		"no_proxy=127.0.0.1,localhost,::1",
	)
	cmd.Env = env

	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	tosMu.Lock()
	if tripped, _ := tosTrippedLocked(); tripped {
		tosMu.Unlock()
		return newCircuitError()
	}
	if err := cmd.Start(); err != nil {
		tosMu.Unlock()
		return err
	}

	e.mu.Lock()
	e.cmd = cmd
	done := make(chan struct{})
	e.exitDone = done
	e.stdin = bufio.NewWriterSize(stdinPipe, 256*1024)
	e.mu.Unlock()

	go func() { defer close(done); e.readStdoutLoop(stdoutPipe, cmd) }()
	tosMu.Unlock()

	rawInit, err := e.RPC("initialize", map[string]any{
		"protocolVersion": 1,
		"clientCapabilities": map[string]any{
			"fs":       map[string]any{"readTextFile": false, "writeTextFile": false},
			"terminal": false,
		},
		"clientInfo": map[string]any{"name": "agy-acp-gateway", "version": "1.0.0"},
	}, 60*time.Second)
	if err != nil {
		e.StopEngine()
		return err
	}
	caps, err := parseAcpCapabilities(rawInit)
	if err != nil {
		e.StopEngine()
		return err
	}
	e.capsMu.Lock()
	e.caps = caps
	e.capsMu.Unlock()
	return nil
}

func (e *SharedAcpEngine) StopEngine() {
	e.mu.Lock()
	for e.stopping {
		e.bootCond.Wait()
	}
	e.stopping = true
	cmd, done := e.cmd, e.exitDone
	e.cmd, e.stdin = nil, nil
	e.mu.Unlock()
	defer func() { e.mu.Lock(); e.stopping = false; e.bootCond.Broadcast(); e.mu.Unlock() }()
	if cmd != nil && cmd.Process != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
	}
	if done != nil {
		select {
		case <-done:
			return
		case <-time.After(2 * time.Second):
		}
		if cmd != nil && cmd.Process != nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		<-done
	}
}

func (e *SharedAcpEngine) sendRawJSON(payload map[string]any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	recordAcpLine("out", b)
	if McpTrace {
		pv := redactSecretsInString(string(b))
		if len(pv) > 400 {
			pv = pv[:400]
		}
		LogEvent("ACP_OUT", pv, nil)
	}
	b = append(b, '\n')
	e.stdinMu.Lock()
	defer e.stdinMu.Unlock()
	tosMu.Lock()
	defer tosMu.Unlock()
	if method, _ := payload["method"].(string); method != "" && method != "session/close" && method != "session/cancel" {
		if tripped, _ := tosTrippedLocked(); tripped {
			return newCircuitError()
		}
	}
	e.mu.Lock()
	w := e.stdin
	e.mu.Unlock()
	if w == nil {
		return errors.New("shared ACP engine not running")
	}
	if _, err := w.Write(b); err != nil {
		return err
	}
	return w.Flush()
}

func (e *SharedAcpEngine) RPC(method string, params any, timeout time.Duration) (json.RawMessage, error) {
	if method != "session/close" && method != "session/cancel" {
		if err := circuitError(); err != nil {
			return nil, err
		}
	}
	id := e.nextID.Add(1)
	ch := make(chan rpcResult, 1)
	e.pending.Store(id, ch)
	defer e.pending.Delete(id)

	err := e.sendRawJSON(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"method":  method,
		"params":  params,
	})
	if err != nil {
		return nil, err
	}

	if timeout <= 0 {
		timeout = 180 * time.Second
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case res := <-ch:
		if res.Err != nil && IsTosViolationError(res.Err.Error()) {
			TripTosCircuitBreaker(res.Err.Error())
			return nil, newCircuitError()
		}
		return res.Result, res.Err
	case <-timer.C:
		return nil, fmt.Errorf("ACP RPC %s timed out after %v", method, timeout)
	}
}

func (e *SharedAcpEngine) RegisterSession(sid string, sess *AcpProjectSession) {
	e.sessionListeners.Store(sid, sess)
	RegisterGatewaySessionID(sid)
}

func (e *SharedAcpEngine) CloseSession(sid string) error {
	if sid == "" || !e.IsAlive() {
		e.sessionListeners.Delete(sid)
		return nil
	}
	_, err := e.RPC("session/close", map[string]any{"sessionId": sid}, 10*time.Second)
	if err == nil {
		e.sessionListeners.Delete(sid)
	}
	return err
}

func (e *SharedAcpEngine) CancelSession(sid string) {
	if sid == "" || !e.IsAlive() {
		return
	}
	_ = e.sendRawJSON(map[string]any{
		"jsonrpc": "2.0",
		"method":  "session/cancel",
		"params":  map[string]any{"sessionId": sid},
	})
}

func (e *SharedAcpEngine) readStdoutLoop(stdoutPipe any, cmd *exec.Cmd) {
	r, ok := stdoutPipe.(interface{ Read([]byte) (int, error) })
	if !ok {
		return
	}
	scanner := bufio.NewScanner(r)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 64*1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		recordAcpLine("in", line)
		if McpTrace {
			pv := redactSecretsInString(string(line))
			if len(pv) > 400 {
				pv = pv[:400]
			}
			LogEvent("ACP_IN", pv, nil)
		}
		var msg struct {
			ID     any             `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
			Result json.RawMessage `json:"result"`
			Error  json.RawMessage `json:"error"`
		}
		if err := json.Unmarshal(line, &msg); err != nil {
			continue
		}

		if msg.ID != nil && msg.Method != "" && len(msg.Result) == 0 && len(msg.Error) == 0 {
			if msg.Method == "session/request_permission" {
				var p struct {
					SessionID string `json:"sessionId"`
					Options   []struct {
						OptionID string `json:"optionId"`
						Kind     string `json:"kind"`
					} `json:"options"`
					ToolCall map[string]any `json:"toolCall"`
				}
				_ = json.Unmarshal(msg.Params, &p)
				if v, ok := e.sessionListeners.Load(p.SessionID); ok {
					sess := v.(*AcpProjectSession)
					sess.handlePermissionRequest(msg.ID, p.Options, p.ToolCall)
				} else {
					replyPermissionOutcome(msg.ID, p.Options, false)
				}
			}
			continue
		}

		if msg.Method == "session/update" {
			var p struct {
				SessionID string         `json:"sessionId"`
				Update    map[string]any `json:"update"`
			}
			if err := json.Unmarshal(msg.Params, &p); err == nil && p.SessionID != "" {
				if v, ok := e.sessionListeners.Load(p.SessionID); ok {
					v.(*AcpProjectSession).handleSessionUpdate(p.Update)
				}
			}
			continue
		}

		if msg.ID != nil {
			if numID, ok := toInt(msg.ID); ok {
				if v, okP := e.pending.Load(int64(numID)); okP {
					ch := v.(chan rpcResult)
					if len(msg.Error) > 0 && string(msg.Error) != "null" {
						ch <- rpcResult{Err: errors.New(string(msg.Error))}
					} else {
						ch <- rpcResult{Result: msg.Result}
					}
				}
			}
		}
	}

	_ = cmd.Wait()
	e.mu.Lock()
	if e.cmd == cmd {
		e.cmd = nil
		e.stdin = nil
	}
	e.mu.Unlock()

	e.pending.Range(func(k, v any) bool {
		ch := v.(chan rpcResult)
		select {
		case ch <- rpcResult{Err: errors.New("shared ACP engine exited")}:
		default:
		}
		e.pending.Delete(k)
		return true
	})
	LogEvent("ACP_ENGINE_EXIT", "Shared agy-acp-900k engine exited", nil)
}

type activeTurnState struct {
	mu            sync.Mutex
	createdAt     time.Time
	firstChunk    time.Time
	textChunks    []string
	thoughtChks   []string
	toolCalls     []ToolCallItem
	batchTimer    *time.Timer
	resolved      bool
	deltaCh       chan DeltaChunk
	deltaClosed   bool
	deltaDone     chan struct{}
	emittedDeltas atomic.Int64
	resolveChan   chan TurnResult
	errChan       chan error
	releaseOnce   sync.Once
	releaseSem    func()
}

func newActiveTurnState(onDelta func(DeltaChunk), releaseSem func()) *activeTurnState {
	t := &activeTurnState{
		createdAt:   time.Now(),
		resolveChan: make(chan TurnResult, 1),
		errChan:     make(chan error, 1),
		releaseSem:  releaseSem,
	}
	if onDelta != nil {
		t.deltaCh = make(chan DeltaChunk, 1024)
		t.deltaDone = make(chan struct{})
		go func(ch <-chan DeltaChunk, done chan<- struct{}, cb func(DeltaChunk)) {
			defer close(done)
			for chunk := range ch {
				t.emittedDeltas.Add(1)
				cb(chunk)
			}
		}(t.deltaCh, t.deltaDone, onDelta)
	}
	return t
}

func (t *activeTurnState) pushDelta(chunk DeltaChunk) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.deltaCh == nil || t.deltaClosed || t.resolved {
		return
	}
	select {
	case t.deltaCh <- chunk:
	default:
		// Slow client buffer full: drop live delta chunk so readStdoutLoop never blocks other sessions;
		// full text is still preserved in t.textChunks / t.thoughtChks.
	}
}

func (t *activeTurnState) closeDelta() {
	t.mu.Lock()
	ch := t.deltaCh
	done := t.deltaDone
	if ch != nil && !t.deltaClosed {
		t.deltaClosed = true
		close(ch)
	}
	t.mu.Unlock()
	if done != nil {
		<-done
	}
}

type AcpProjectSession struct {
	mu                   sync.Mutex
	ConvKey              string
	TenantKey            string
	Cwd                  string
	SystemText           string
	SessionID            string
	HibernatedSessionID  string
	LastWakeMethod       string
	ToolsCatalog         []ToolDef
	McpToHostMap         map[string]string
	ToolFingerprint      string
	ParkedCalls          map[string]*ParkedMcpCall
	ParkedSince          time.Time
	MessageCount         int
	HistoryFingerprint   string
	TurnHistory          []TurnHistoryEntry
	CreatedAt            time.Time
	LastUsedAt           time.Time
	CompactionRollovers  int
	SessionReuses        int
	IsBusy               bool
	PromptInFlight       bool
	ForceRolloverNext    bool
	PromptStepCount      int
	LastSeenDbStepIdx    int
	LastSeenGenMetaIdx   int
	LastCachedReadTokens int
	LastRawCachedTokens  int
	CompactedCurrentTurn bool
	RecentToolSigs       []string
	ModelID              string
	ThoughtLevel         string
	activeTurn           *activeTurnState
	queueMu              sync.Mutex
	onBeforeSpawn        func(*AcpProjectSession) error
	spawning             bool
	closing              bool
}

func NewAcpProjectSession(convKey, cwd, systemText string) *AcpProjectSession {
	if cwd == "" {
		cwd = DefaultCwd
	}
	return &AcpProjectSession{
		ConvKey:            convKey,
		TenantKey:          randomHex(12),
		Cwd:                cwd,
		SystemText:         systemText,
		McpToHostMap:       make(map[string]string),
		ParkedCalls:        make(map[string]*ParkedMcpCall),
		CreatedAt:          time.Now(),
		LastUsedAt:         time.Now(),
		PromptStepCount:    1,
		LastSeenDbStepIdx:  -1,
		LastSeenGenMetaIdx: -1,
		ModelID:            DefaultModelID,
		ThoughtLevel:       "medium",
	}
}

func setAndVerifySessionConfigOption(sessionID, configID, expectedValue string, timeout time.Duration) error {
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		rawRes, err := GlobalEngine.RPC("session/set_config_option", map[string]any{
			"sessionId": sessionID,
			"configId":  configID,
			"value":     expectedValue,
		}, timeout)
		if err != nil {
			lastErr = err
			if strings.Contains(err.Error(), "-32602") {
				return err
			}
			continue
		}
		var parsed struct {
			ConfigOptions []struct {
				ID           string `json:"id"`
				CurrentValue string `json:"currentValue"`
			} `json:"configOptions"`
		}
		if len(rawRes) > 0 && json.Unmarshal(rawRes, &parsed) == nil && len(parsed.ConfigOptions) > 0 {
			mismatch := false
			for _, opt := range parsed.ConfigOptions {
				if opt.ID == configID && opt.CurrentValue != "" && opt.CurrentValue != expectedValue {
					mismatch = true
					lastErr = fmt.Errorf("configOption %s currentValue=%q != expected %q", configID, opt.CurrentValue, expectedValue)
					break
				}
			}
			if mismatch {
				continue
			}
		}
		return nil
	}
	return lastErr
}

// applyModelSelection switches model / thinking tier on the live agy session (agy keeps the context).
func (s *AcpProjectSession) applyModelSelection(sel ModelSelection) {
	nextModel := sel.Model.ID
	nextTier := sel.Tier
	if nextTier == "" {
		nextTier = "medium"
	}
	s.mu.Lock()
	sid := s.SessionID
	prevModel, prevTier := s.ModelID, s.ThoughtLevel
	if sid == "" {
		s.ModelID, s.ThoughtLevel = nextModel, nextTier
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	if nextModel == prevModel && nextTier == prevTier {
		return
	}
	if nextModel != prevModel {
		if err := setAndVerifySessionConfigOption(sid, "model", nextModel, 30*time.Second); err == nil {
			s.mu.Lock()
			s.ModelID = nextModel
			s.mu.Unlock()
		}
	}
	if sel.Model.FixedTier != "" {
		s.mu.Lock()
		s.ThoughtLevel = sel.Model.FixedTier
		s.mu.Unlock()
	} else if nextTier != prevTier {
		if err := setAndVerifySessionConfigOption(sid, "thought_level", nextTier, 30*time.Second); err == nil {
			s.mu.Lock()
			s.ThoughtLevel = nextTier
			s.mu.Unlock()
		} else {
			LogEvent("THOUGHT_LEVEL_SKIPPED", "thought_level rejected for "+nextModel, map[string]any{"convKey": s.ConvKey, "error": err.Error()})
		}
	}
	LogEvent("MODEL_SWITCH", fmt.Sprintf("Switched live session model %s/%s -> %s/%s", prevModel, prevTier, nextModel, nextTier), map[string]any{
		"convKey": s.ConvKey, "sessionId": sid, "model": nextModel, "thoughtLevel": nextTier,
	})
}

func (s *AcpProjectSession) IsAvailableForNewTurn() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.IsBusy && !s.spawning && !s.closing && len(s.ParkedCalls) == 0 && s.activeTurn == nil
}

func (s *AcpProjectSession) HibernateWorker(reason string, force bool) bool {
	s.mu.Lock()
	if s.SessionID == "" || s.spawning || s.closing {
		s.mu.Unlock()
		return false
	}
	if !force && (s.IsBusy || len(s.ParkedCalls) > 0 || s.activeTurn != nil) {
		s.mu.Unlock()
		return false
	}
	if force {
		for k, p := range s.ParkedCalls {
			select {
			case p.ResultChan <- map[string]any{
				"content": []map[string]any{{"type": "text", "text": "Session hibernated."}},
				"isError": true,
			}:
			default:
			}
			delete(s.ParkedCalls, k)
		}
	}
	sid := s.SessionID
	s.closing = true
	s.SessionID = ""
	s.PromptInFlight = false
	s.ParkedSince = time.Time{}
	keepResumeState := (!force || strings.HasPrefix(reason, "reap_orphan_parked")) && s.MessageCount > 0 && !strings.HasPrefix(s.ConvKey, "util:")
	if keepResumeState {
		s.HibernatedSessionID = sid
	} else {
		s.HibernatedSessionID = ""
		s.MessageCount = 0
		s.HistoryFingerprint = ""
	}
	s.mu.Unlock()

	closeErr := GlobalEngine.CloseSession(sid)
	s.mu.Lock()
	s.closing = false
	if closeErr != nil {
		s.SessionID = sid
		s.HibernatedSessionID = ""
	}
	s.mu.Unlock()
	if closeErr != nil {
		LogEvent("ACP_CLOSE_ERR", "Worker slot retained after close failure", nil)
		return false
	}
	GlobalStore.UpsertSession(s)
	LogEvent("ACP_HIBERNATE", fmt.Sprintf("Closed idle session in shared engine for %s", s.ConvKey), map[string]any{
		"reason":          reason,
		"closedSessionId": sid,
	})
	return true
}

func (s *AcpProjectSession) ownsToolCall(tc map[string]any) bool {
	rawJson, _ := json.Marshal(tc)
	str := string(rawJson)
	if strings.Contains(str, "dsh-bridge") || strings.Contains(str, "search_web") || strings.Contains(str, "read_url_content") {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for mName := range s.McpToHostMap {
		if strings.Contains(str, mName) {
			return true
		}
	}
	return false
}

func replyPermissionOutcome(rpcID any, options []struct {
	OptionID string `json:"optionId"`
	Kind     string `json:"kind"`
}, allow bool) {
	if allow {
		chosenID := ""
		for _, o := range options {
			if o.Kind == "allow_always" || o.Kind == "allow_once" {
				chosenID = o.OptionID
				break
			}
		}
		if chosenID == "" {
			chosenID = "allow_once"
		}
		_ = GlobalEngine.sendRawJSON(map[string]any{
			"jsonrpc": "2.0",
			"id":      rpcID,
			"result": map[string]any{
				"outcome": map[string]any{"outcome": "selected", "optionId": chosenID},
			},
		})
		return
	}
	chosenReject := ""
	for _, o := range options {
		if o.Kind == "reject_once" || o.Kind == "reject_always" {
			chosenReject = o.OptionID
			break
		}
	}
	if chosenReject != "" {
		_ = GlobalEngine.sendRawJSON(map[string]any{
			"jsonrpc": "2.0",
			"id":      rpcID,
			"result": map[string]any{
				"outcome": map[string]any{"outcome": "selected", "optionId": chosenReject},
			},
		})
		return
	}
	_ = GlobalEngine.sendRawJSON(map[string]any{
		"jsonrpc": "2.0",
		"id":      rpcID,
		"result": map[string]any{
			"outcome": map[string]any{"outcome": "cancelled"},
		},
	})
}

func (s *AcpProjectSession) handlePermissionRequest(rpcID any, options []struct {
	OptionID string `json:"optionId"`
	Kind     string `json:"kind"`
}, tc map[string]any) {
	isOur := s.ownsToolCall(tc)
	replyPermissionOutcome(rpcID, options, isOur)
}

func (s *AcpProjectSession) CancelActiveTurn(reason string) {
	s.mu.Lock()
	sid := s.SessionID
	turn := s.activeTurn
	s.activeTurn = nil
	for k, p := range s.ParkedCalls {
		select {
		case p.ResultChan <- map[string]any{
			"content": []map[string]any{{"type": "text", "text": "Request cancelled by client."}},
			"isError": true,
		}:
		default:
		}
		delete(s.ParkedCalls, k)
	}
	s.ParkedSince = time.Time{}
	s.mu.Unlock()

	if turn != nil {
		turn.mu.Lock()
		if turn.batchTimer != nil {
			turn.batchTimer.Stop()
		}
		turn.resolved = true
		if turn.releaseSem != nil {
			turn.releaseOnce.Do(turn.releaseSem)
		}
		turn.mu.Unlock()
		turn.closeDelta()
	}

	if sid != "" {
		GlobalEngine.CancelSession(sid)
		LogEvent("SESSION_CANCEL", fmt.Sprintf("Sent session/cancel to ACP engine (%s)", reason), map[string]any{
			"convKey":   s.ConvKey,
			"sessionId": sid,
		})
	}
}

var retryNoticeRe = regexp.MustCompile(`(?i)^\s*⚠\s*API error \(attempt \d+\)|your previous response was cut off because it exceeded the output token limit|context canceledThe request was cancelled by the client`)

func (s *AcpProjectSession) handleSessionUpdate(u map[string]any) {
	s.mu.Lock()
	turn := s.activeTurn
	s.mu.Unlock()
	if turn == nil || u == nil {
		return
	}
	updType, _ := u["sessionUpdate"].(string)
	content, _ := u["content"].(map[string]any)
	text, _ := content["text"].(string)
	if text == "" {
		return
	}

	if updType == "agent_message_chunk" {
		if retryNoticeRe.MatchString(text) {
			LogEvent("RETRY_NOTICE_FILTERED", "Dropped internal agy retry/limit/cancel notice", map[string]any{
				"convKey": s.ConvKey,
			})
			return
		}
		turn.mu.Lock()
		if turn.firstChunk.IsZero() {
			turn.firstChunk = time.Now()
		}
		turn.textChunks = append(turn.textChunks, text)
		turn.mu.Unlock()
		turn.pushDelta(DeltaChunk{Type: "text", Text: text})
	} else if updType == "agent_thought_chunk" {
		turn.mu.Lock()
		if turn.firstChunk.IsZero() {
			turn.firstChunk = time.Now()
		}
		turn.thoughtChks = append(turn.thoughtChks, text)
		turn.mu.Unlock()
		turn.pushDelta(DeltaChunk{Type: "thinking", Thinking: text})
	}
}

func isLoopbackRemoteAddr(remoteAddr string) bool {
	if remoteAddr == "" {
		return true
	}
	host := remoteAddr
	if strings.HasPrefix(host, "[") {
		if idx := strings.IndexByte(host, ']'); idx > 0 {
			host = host[1:idx]
		}
	} else if idx := strings.LastIndexByte(host, ':'); idx > 0 {
		host = host[:idx]
	}
	return host == "127.0.0.1" || host == "::1" || host == "localhost" || host == "192.0.2.1"
}

func (s *AcpProjectSession) HandleMcpHTTP(w http.ResponseWriter, r *http.Request) {
	if !isLoopbackRemoteAddr(r.RemoteAddr) {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	// Same guards as Node DshMcpBridge: bearer token per tenant, no browser Origin
	if r.Header.Get("Authorization") != "Bearer "+s.TenantKey {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	if r.Header.Get("Origin") != "" {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	if r.Method == http.MethodDelete {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST, DELETE")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)
	var msg struct {
		ID     any            `json:"id"`
		Method string         `json:"method"`
		Params map[string]any `json:"params"`
	}
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"Parse error"}}`))
		return
	}

	if McpTrace {
		LogEvent("MCP_REQ", msg.Method, map[string]any{
			"id":      msg.ID,
			"auth":    r.Header.Get("Authorization") != "",
			"accept":  r.Header.Get("Accept"),
			"proto":   r.Header.Get("Mcp-Protocol-Version"),
			"session": r.Header.Get("Mcp-Session-Id"),
		})
	}
	if msg.ID == nil {
		w.WriteHeader(http.StatusAccepted)
		return
	}

	switch msg.Method {
	case "initialize":
		protoVer := "2025-06-18"
		if pv, ok := msg.Params["protocolVersion"].(string); ok && pv != "" {
			protoVer = pv
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      msg.ID,
			"result": map[string]any{
				"protocolVersion": protoVer,
				"capabilities":    map[string]any{"tools": map[string]any{"listChanged": false}},
				"serverInfo":      map[string]any{"name": "dsh-bridge", "version": "2.0.0"},
				"instructions":    "Host harness tools bridge.",
			},
		})
	case "ping":
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": msg.ID, "result": map[string]any{}})
	case "resources/list":
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": msg.ID, "result": map[string]any{"resources": []any{}}})
	case "resources/templates/list":
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": msg.ID, "result": map[string]any{"resourceTemplates": []any{}}})
	case "prompts/list":
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": msg.ID, "result": map[string]any{"prompts": []any{}}})
	case "tools/list":
		s.mu.Lock()
		tools := make([]map[string]any, 0, len(s.ToolsCatalog))
		for _, t := range s.ToolsCatalog {
			tools = append(tools, map[string]any{
				"name":        t.McpName,
				"description": fmt.Sprintf("DSH tool %s: %s", t.Name, t.Description),
				"inputSchema": t.Parameters,
				"_meta":       map[string]any{"antigravity/alwaysLoad": true},
			})
		}
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      msg.ID,
			"result":  map[string]any{"tools": tools},
		})
	case "tools/call":
		rawName, _ := msg.Params["name"].(string)
		rawArgs, _ := msg.Params["arguments"].(map[string]any)
		if rawArgs == nil {
			rawArgs = make(map[string]any)
		}
		delete(rawArgs, "toolSummary")

		var progressToken any
		if meta, ok := msg.Params["_meta"].(map[string]any); ok {
			progressToken = meta["progressToken"]
		}

		// Streamable HTTP MCP requires immediate 200 text/event-stream headers + SSE message reply for tools/call
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache, no-transform")
		w.Header().Set("Connection", "keep-alive")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		if flusher != nil {
			flusher.Flush()
		}

		var sseMu sync.Mutex
		doneCh := make(chan struct{})
		go func() {
			tk := time.NewTicker(15 * time.Second)
			defer tk.Stop()
			ticks := 0
			for {
				select {
				case <-doneCh:
					return
				case <-r.Context().Done():
					return
				case <-tk.C:
					ticks++
					sseMu.Lock()
					_, _ = w.Write([]byte(": keepalive\n\n"))
					if progressToken != nil {
						progBytes, _ := json.Marshal(map[string]any{
							"jsonrpc": "2.0",
							"method":  "notifications/progress",
							"params": map[string]any{
								"progressToken": progressToken,
								"progress":      ticks,
								"message":       "Host harness is running the tool",
							},
						})
						_, _ = fmt.Fprintf(w, "event: message\ndata: %s\n\n", string(progBytes))
					}
					if flusher != nil {
						flusher.Flush()
					}
					sseMu.Unlock()
				}
			}
		}()

		s.mu.Lock()
		hostName := s.McpToHostMap[rawName]
		s.mu.Unlock()
		if hostName == "" {
			hostName = strings.TrimPrefix(rawName, "mcp_dsh-bridge_")
		}

		resObj := s.executeMcpToolCall(hostName, rawArgs)
		close(doneCh)

		replyBytes, _ := json.Marshal(map[string]any{
			"jsonrpc": "2.0",
			"id":      msg.ID,
			"result":  resObj,
		})
		sseMu.Lock()
		_, _ = fmt.Fprintf(w, "event: message\ndata: %s\n\n", string(replyBytes))
		if flusher != nil {
			flusher.Flush()
		}
		sseMu.Unlock()
	default:
		// Match MCP SDK behaviour: unknown request methods (e.g. server/discover) -> -32601
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      msg.ID,
			"error":   map[string]any{"code": -32601, "message": "Method not found"},
		})
	}
}

func (s *AcpProjectSession) awaitParkedCall(callID string) map[string]any {
	s.mu.Lock()
	entry, ok := s.ParkedCalls[callID]
	s.mu.Unlock()
	if !ok {
		return map[string]any{
			"content": []map[string]any{{"type": "text", "text": fmt.Sprintf("Unknown or already completed call_id %s.", callID)}},
			"isError": true,
		}
	}

	awaitMcpName := "mcp_dsh-bridge_" + mcpToolName(AwaitToolName)
	timer := time.NewTimer(RelayAfterMs * time.Millisecond)
	defer timer.Stop()

	select {
	case res := <-entry.ResultChan:
		s.mu.Lock()
		delete(s.ParkedCalls, callID)
		s.mu.Unlock()
		return res
	case <-timer.C:
		LogEvent("MCP_TOOL_RELAY", fmt.Sprintf("Relaying long-running tool call via %s", AwaitToolName), map[string]any{
			"convKey":    s.ConvKey,
			"sessionId":  s.SessionID,
			"toolCallId": callID,
			"toolName":   entry.Call.Name,
		})
		return map[string]any{
			"content": []map[string]any{
				{
					"type": "text",
					"text": fmt.Sprintf("Tool call %s (%s) is still running in the host harness. Call %s with {\"call_id\":\"%s\"} immediately to continue waiting for its result.", callID, entry.Call.Name, awaitMcpName, callID),
				},
			},
		}
	}
}

func (s *AcpProjectSession) executeMcpToolCall(hostName string, parsedInput map[string]any) map[string]any {
	if hostName == AwaitToolName {
		targetID, _ := parsedInput["call_id"].(string)
		return s.awaitParkedCall(strings.TrimSpace(targetID))
	}

	effectiveName := hostName
	effectiveInput := parsedInput
	if hostName == CallToolName {
		if n, ok := parsedInput["name"].(string); ok {
			effectiveName = strings.TrimSpace(strings.TrimPrefix(n, "mcp_dsh-bridge_"))
		}
		if a, ok := parsedInput["arguments"].(map[string]any); ok {
			effectiveInput = a
			delete(effectiveInput, "toolSummary")
		} else {
			effectiveInput = make(map[string]any)
		}
	}

	callID := "acp_" + randomHex(16)
	call := ToolCallItem{
		ID:    callID,
		Name:  effectiveName,
		Input: effectiveInput,
	}

	inBytes, _ := json.Marshal(effectiveInput)
	sig := effectiveName + ":" + string(inBytes)

	s.mu.Lock()
	s.RecentToolSigs = append(s.RecentToolSigs, sig)
	if len(s.RecentToolSigs) > 6 {
		s.RecentToolSigs = s.RecentToolSigs[len(s.RecentToolSigs)-6:]
	}
	isLoop := false
	if len(s.RecentToolSigs) >= 4 {
		last4 := s.RecentToolSigs[len(s.RecentToolSigs)-4:]
		if last4[0] == sig && last4[1] == sig && last4[2] == sig && last4[3] == sig {
			isLoop = true
			s.RecentToolSigs = nil
		}
	}
	s.mu.Unlock()

	if isLoop {
		LogEvent("TOOL_LOOP_BREAKER", fmt.Sprintf("Intercepted 4x identical consecutive tool call (%s)", effectiveName), map[string]any{
			"convKey":   s.ConvKey,
			"sessionId": s.SessionID,
		})
		return map[string]any{
			"content": []map[string]any{
				{
					"type": "text",
					"text": fmt.Sprintf("Loop circuit breaker: You have called %s with identical arguments 4 times consecutively. Do not repeat this identical call; proceed with a different approach.", effectiveName),
				},
			},
			"isError": true,
		}
	}

	preview := string(inBytes)
	if len(preview) > 220 {
		preview = preview[:220]
	}
	LogEvent("MCP_TOOL_PARKED", "Intercepted & parked tool call from Gemini 4 Argon", map[string]any{
		"convKey":      s.ConvKey,
		"sessionId":    s.SessionID,
		"toolCallId":   call.ID,
		"toolName":     call.Name,
		"inputPreview": preview,
	})

	if call.Name == "StructuredOutput" {
		s.enqueueToolCallToTurn(call)
		time.Sleep(600 * time.Millisecond)
		go func() {
			time.Sleep(400 * time.Millisecond)
			s.HibernateWorker("structured_output_terminal", true)
		}()
		return map[string]any{
			"content": []map[string]any{{"type": "text", "text": "StructuredOutput delivered to host; subagent finished."}},
		}
	}

	parked := &ParkedMcpCall{
		Call:       call,
		ResultChan: make(chan map[string]any, 1),
	}
	s.mu.Lock()
	if len(s.ParkedCalls) == 0 {
		s.ParkedSince = time.Now()
	}
	s.ParkedCalls[call.ID] = parked
	s.mu.Unlock()
	GlobalStore.UpsertSession(s)

	s.enqueueToolCallToTurn(call)
	return s.awaitParkedCall(call.ID)
}

func (s *AcpProjectSession) enqueueToolCallToTurn(call ToolCallItem) {
	s.mu.Lock()
	turn := s.activeTurn
	s.mu.Unlock()
	if turn == nil {
		return
	}
	turn.mu.Lock()
	if turn.firstChunk.IsZero() {
		turn.firstChunk = time.Now()
	}
	turn.toolCalls = append(turn.toolCalls, call)
	if turn.batchTimer != nil {
		turn.batchTimer.Stop()
	}
	turn.batchTimer = time.AfterFunc(80*time.Millisecond, func() {
		s.flushToolBatch(turn)
	})
	turn.mu.Unlock()
}

func (s *AcpProjectSession) flushToolBatch(turn *activeTurnState) {
	turn.mu.Lock()
	if turn.resolved || len(turn.toolCalls) == 0 {
		turn.mu.Unlock()
		return
	}
	turn.resolved = true
	if turn.releaseSem != nil {
		turn.releaseOnce.Do(turn.releaseSem)
	}
	text := strings.Join(turn.textChunks, "")
	thinking := strings.Join(turn.thoughtChks, "")
	calls := append([]ToolCallItem(nil), turn.toolCalls...)
	ttft := int(time.Since(turn.createdAt).Milliseconds())
	if !turn.firstChunk.IsZero() {
		ttft = int(turn.firstChunk.Sub(turn.createdAt).Milliseconds())
	}
	if ttft < 10 {
		ttft = 10
	}
	turn.mu.Unlock()

	s.mu.Lock()
	for _, tc := range calls {
		if p, ok := s.ParkedCalls[tc.ID]; ok {
			p.Delivered = true
		}
	}
	s.mu.Unlock()

	outChars := len(text) + len(thinking)
	for _, tc := range calls {
		if b, err := json.Marshal(tc.Input); err == nil {
			outChars += len(b)
		}
	}
	turn.closeDelta()
	emitted := int(turn.emittedDeltas.Load())
	usage := s.normalizeTurnUsageAsync(nil, true, 0, outChars)
	select {
	case turn.resolveChan <- TurnResult{
		StopReason:    "tool_use",
		Text:          text,
		Thinking:      thinking,
		ToolCalls:     calls,
		Usage:         usage,
		TtftMs:        ttft,
		EmittedDeltas: emitted,
	}:
	default:
	}
}

func (s *AcpProjectSession) ensureBooted(toolsCatalog []ToolDef, thoughtLevel string) error {
	var names []string
	for _, t := range toolsCatalog {
		names = append(names, t.Name)
	}
	newFp := shortSha256(strings.Join(names, ","))
	if err := GlobalEngine.EnsureStarted(); err != nil {
		return err
	}
	if len(toolsCatalog) > 0 && !GlobalEngine.capabilities().McpHTTP {
		return errors.New("upstream does not advertise required HTTP MCP transport")
	}
	s.mu.Lock()
	sid := s.SessionID
	hibSid := s.HibernatedSessionID
	oldFp := s.ToolFingerprint
	msgCnt := s.MessageCount
	cb := s.onBeforeSpawn
	closing := s.closing
	s.mu.Unlock()
	if closing {
		return &SoftError{StatusCode: 529, ErrorType: "overloaded_error", Message: "Session is closing; retry shortly"}
	}

	if sid == "" {
		if cb != nil {
			if err := cb(s); err != nil {
				return err
			}
			defer func() { s.mu.Lock(); s.spawning = false; s.mu.Unlock() }()
		}
		if hibSid != "" && msgCnt > 0 && (oldFp == "" || oldFp == newFp) {
			if err := s.reloadHibernatedSession(hibSid, toolsCatalog, thoughtLevel, newFp); err == nil {
				return nil
			}
			s.mu.Lock()
			s.HibernatedSessionID = ""
			s.MessageCount = 0
			s.HistoryFingerprint = ""
			s.mu.Unlock()
		}
		return s.openFreshSession(toolsCatalog, thoughtLevel, newFp)
	}
	return nil
}

func (s *AcpProjectSession) buildAppendSystemPrompt(toolsCatalog []ToolDef) string {
	var toolMappingSummary string
	if len(toolsCatalog) > 0 {
		lines := []string{
			"CRITICAL TOOL ROUTING RULES:",
			"1. The external host workspace owns all tools via the `dsh-bridge` MCP server.",
			"2. NEVER invoke built-in local file/shell tools (`read_file`, `write_file`, `edit_file`, `run_command`) directly. For web search or fetching web URLs, use the built-in `search_web` and `read_url_content` tools directly.",
			"3. When reading files with `Read`, read the whole file at once (or up to `limit: 2000` lines per call); do NOT slice files into small 150-220 line chunks.",
			"4. Always invoke the eager `mcp_dsh-bridge_dsh_<ToolName>_<hash>` tools mapped below:",
		}
		for _, t := range toolsCatalog {
			var keys []string
			if props, ok := t.Parameters["properties"].(map[string]any); ok {
				for k := range props {
					keys = append(keys, k)
				}
				sort.Strings(keys)
			}
			lines = append(lines, fmt.Sprintf("- %s -> mcp_dsh-bridge_%s (keys: %s)", t.Name, t.McpName, strings.Join(keys, ", ")))
		}
		toolMappingSummary = strings.Join(lines, "\n")
	} else {
		toolMappingSummary = "No external workspace tools are attached. You may use the built-in `search_web` and `read_url_content` tools whenever web search or reading a URL is helpful."
	}
	customSys := PruneClientSystemPrompt(s.SystemText)
	if customSys == "" && s.Cwd != "" {
		customSys = "Primary working directory: " + s.Cwd
	}
	appendPrompt := toolMappingSummary
	if customSys != "" {
		appendPrompt += "\n\n[Workspace Context]\n" + customSys
	}
	return appendPrompt
}

func (s *AcpProjectSession) reloadHibernatedSession(hibSid string, toolsCatalog []ToolDef, thoughtLevel, newToolFp string) error {
	if len(toolsCatalog) > 0 && !GlobalEngine.capabilities().McpHTTP {
		return errors.New("upstream does not advertise required HTTP MCP transport")
	}
	s.mu.Lock()
	s.ParkedCalls = make(map[string]*ParkedMcpCall)
	s.ToolsCatalog = toolsCatalog
	s.McpToHostMap = make(map[string]string, len(toolsCatalog))
	for _, t := range toolsCatalog {
		s.McpToHostMap[t.McpName] = t.Name
	}
	tenantURL := fmt.Sprintf("http://127.0.0.1:%d/mcp/%s", GatewayPort, s.TenantKey)
	modelID := s.ModelID
	s.mu.Unlock()

	mcpServers := make([]map[string]any, 0)
	if len(toolsCatalog) > 0 {
		mcpServers = append(mcpServers, map[string]any{
			"name": "dsh-bridge",
			"type": "http",
			"url":  tenantURL,
			"headers": []map[string]any{
				{"name": "Authorization", "value": "Bearer " + s.TenantKey},
			},
		})
	}

	appendPrompt := s.buildAppendSystemPrompt(toolsCatalog)
	t0 := time.Now()
	params := map[string]any{
		"sessionId":  hibSid,
		"cwd":        s.Cwd,
		"mcpServers": mcpServers,
		"_meta": map[string]any{
			"antigravity": map[string]any{
				"systemPrompt": map[string]any{"append": appendPrompt},
			},
		},
	}
	caps := GlobalEngine.capabilities()
	wakeMethod := ""
	var err error = errors.New("upstream does not advertise session recovery")
	if caps.Resume {
		wakeMethod = "session/resume"
		_, err = GlobalEngine.RPC(wakeMethod, params, 60*time.Second)
	}
	if err != nil && caps.Load {
		wakeMethod = "session/load"
		_, err = GlobalEngine.RPC(wakeMethod, params, 90*time.Second)
	}
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.SessionID = hibSid
	s.HibernatedSessionID = ""
	s.LastWakeMethod = wakeMethod
	s.ToolFingerprint = newToolFp
	needGenWatermark := s.LastSeenGenMetaIdx < 0
	needStepWatermark := s.LastSeenDbStepIdx < 0
	s.mu.Unlock()

	if needGenWatermark {
		if maxGen := ReadMaxSessionGenMetadataIdx(hibSid); maxGen >= 0 {
			s.mu.Lock()
			if s.LastSeenGenMetaIdx < maxGen {
				s.LastSeenGenMetaIdx = maxGen
			}
			s.mu.Unlock()
		}
	}
	if needStepWatermark {
		if maxStep := ReadMaxSessionStepIdx(hibSid); maxStep >= 0 {
			s.mu.Lock()
			if s.LastSeenDbStepIdx < maxStep {
				s.LastSeenDbStepIdx = maxStep
			}
			s.mu.Unlock()
		}
	}

	GlobalEngine.RegisterSession(hibSid, s)
	mInfo := ModelOrDefault(modelID)
	_ = setAndVerifySessionConfigOption(hibSid, "model", mInfo.ID, 30*time.Second)
	appliedTier := thoughtLevel
	if mInfo.FixedTier != "" {
		appliedTier = mInfo.FixedTier
	} else {
		_ = setAndVerifySessionConfigOption(hibSid, "thought_level", thoughtLevel, 30*time.Second)
	}
	if UseG1Credits {
		_ = setAndVerifySessionConfigOption(hibSid, "use_g1_credits", "true", 15*time.Second)
	}
	s.mu.Lock()
	s.ModelID = mInfo.ID
	s.ThoughtLevel = appliedTier
	s.mu.Unlock()
	GlobalStore.UpsertSession(s)

	evCat := "SESSION_RESUME"
	if wakeMethod == "session/load" {
		evCat = "SESSION_LOAD"
	}
	LogEvent(evCat, fmt.Sprintf("Woke hibernated ACP session via %s; cache hit depends on upstream usage", wakeMethod), map[string]any{
		"convKey":      s.ConvKey,
		"sessionId":    hibSid,
		"method":       wakeMethod,
		"model":        mInfo.ID,
		"thoughtLevel": appliedTier,
		"messageCount": s.MessageCount,
		"setupMs":      time.Since(t0).Milliseconds(),
	})
	return nil
}

func (s *AcpProjectSession) openFreshSession(toolsCatalog []ToolDef, thoughtLevel, newToolFp string) error {
	if len(toolsCatalog) > 0 && !GlobalEngine.capabilities().McpHTTP {
		return errors.New("upstream does not advertise required HTTP MCP transport")
	}
	s.mu.Lock()
	prevSid := s.SessionID
	if prevSid == "" {
		prevSid = s.HibernatedSessionID
	}
	wasSpawning := s.spawning
	s.spawning = true
	defer func() { s.mu.Lock(); s.spawning = wasSpawning; s.mu.Unlock() }()

	s.SessionID = ""
	s.HibernatedSessionID = ""
	s.ForceRolloverNext = false
	s.ParkedCalls = make(map[string]*ParkedMcpCall)
	s.ToolsCatalog = toolsCatalog
	s.McpToHostMap = make(map[string]string, len(toolsCatalog))
	for _, t := range toolsCatalog {
		s.McpToHostMap[t.McpName] = t.Name
	}
	tenantURL := fmt.Sprintf("http://127.0.0.1:%d/mcp/%s", GatewayPort, s.TenantKey)
	s.mu.Unlock()
	if prevSid != "" {
		if err := GlobalEngine.CloseSession(prevSid); err != nil {
			s.mu.Lock()
			s.SessionID = prevSid
			s.mu.Unlock()
			return err
		}
	}

	mcpServers := make([]map[string]any, 0)
	if len(toolsCatalog) > 0 {
		mcpServers = append(mcpServers, map[string]any{
			"name": "dsh-bridge",
			"type": "http",
			"url":  tenantURL,
			"headers": []map[string]any{
				{"name": "Authorization", "value": "Bearer " + s.TenantKey},
			},
		})
	}

	appendPrompt := s.buildAppendSystemPrompt(toolsCatalog)

	t0 := time.Now()
	rawRes, err := GlobalEngine.RPC("session/new", map[string]any{
		"cwd":        s.Cwd,
		"mcpServers": mcpServers,
		"_meta": map[string]any{
			"antigravity": map[string]any{
				"systemPrompt": map[string]any{"append": appendPrompt},
			},
		},
	}, 120*time.Second)
	if err != nil {
		return err
	}
	var parsedNew struct {
		SessionID string `json:"sessionId"`
	}
	if err := json.Unmarshal(rawRes, &parsedNew); err != nil || parsedNew.SessionID == "" {
		return errors.New("Failed to start Antigravity conversation")
	}

	s.mu.Lock()
	s.SessionID = parsedNew.SessionID
	s.HibernatedSessionID = ""
	s.ToolFingerprint = newToolFp
	s.MessageCount = 0
	s.HistoryFingerprint = ""
	s.LastCachedReadTokens = 0
	s.LastRawCachedTokens = 0
	s.LastSeenDbStepIdx = -1
	s.LastSeenGenMetaIdx = -1
	s.LastWakeMethod = ""
	s.mu.Unlock()

	GlobalEngine.RegisterSession(parsedNew.SessionID, s)

	s.mu.Lock()
	modelID := s.ModelID
	s.mu.Unlock()
	mInfo := ModelOrDefault(modelID)
	_ = setAndVerifySessionConfigOption(parsedNew.SessionID, "model", mInfo.ID, 30*time.Second)
	// GPT-OSS has a fixed tier: agy rejects thought_level (-32602) for it
	appliedTier := thoughtLevel
	if mInfo.FixedTier != "" {
		appliedTier = mInfo.FixedTier
	} else if err := setAndVerifySessionConfigOption(parsedNew.SessionID, "thought_level", thoughtLevel, 30*time.Second); err != nil {
		LogEvent("THOUGHT_LEVEL_SKIPPED", "thought_level rejected for "+mInfo.ID, map[string]any{"convKey": s.ConvKey, "error": err.Error()})
	}
	if UseG1Credits {
		_ = setAndVerifySessionConfigOption(parsedNew.SessionID, "use_g1_credits", "true", 15*time.Second)
	}
	s.mu.Lock()
	s.ModelID = mInfo.ID
	s.ThoughtLevel = appliedTier
	s.mu.Unlock()
	GlobalStore.UpsertSession(s)

	LogEvent("SESSION_NEW", "Opened 1M Pure-API ACP session in shared engine (Single-Port MCP)", map[string]any{
		"convKey":       s.ConvKey,
		"sessionId":     parsedNew.SessionID,
		"model":         mInfo.ID,
		"thoughtLevel":  appliedTier,
		"mcpToolsCount": len(toolsCatalog),
		"setupMs":       time.Since(t0).Milliseconds(),
	})
	return nil
}

func (s *AcpProjectSession) alignTpuTailUsage(inTok, cachedTok, outTok, steps int, source string) *TurnUsage {
	alignedIn := inTok
	alignedCr := cachedTok
	if alignedCr > 0 && alignedIn > TpuTailReserveTokens {
		trueDelta := alignedIn - TpuTailReserveTokens
		if trueDelta < 1 {
			trueDelta = 1
		}
		alignedCr += alignedIn - trueDelta
		alignedIn = trueDelta
	}
	if alignedIn+alignedCr > 0 {
		s.mu.Lock()
		s.LastCachedReadTokens = alignedIn + alignedCr
		s.mu.Unlock()
	}
	if outTok <= 0 {
		outTok = 16
	}
	return &TurnUsage{
		InputTokens:            alignedIn,
		CachedReadTokens:       alignedCr,
		OutputTokens:           outTok,
		RawUncachedInputTokens: inTok,
		RawTpuCachedTokens:     cachedTok,
		StepsInPrompt:          steps,
		UsageSource:            source,
	}
}

func (s *AcpProjectSession) normalizeTurnUsage(rawUsage map[string]any, isToolUseStep bool, promptChars, outChars int) *TurnUsage {
	s.mu.Lock()
	steps := s.PromptStepCount
	if steps < 1 {
		steps = 1
	}
	sid := s.SessionID
	lastIdx := s.LastSeenDbStepIdx
	lastGenIdx := s.LastSeenGenMetaIdx
	lastCached := s.LastCachedReadTokens
	prevRawCr := s.LastRawCachedTokens
	modelID := s.ModelID
	thoughtLevel := s.ThoughtLevel
	compactedTurn := s.CompactedCurrentTurn
	s.mu.Unlock()

	if gm := ReadLatestSessionGenMetadata(sid, lastGenIdx); gm != nil {
		deg := EvaluateArgonDegradation(gm, ArgonEvalContext{
			ModelID:              modelID,
			ThoughtLevel:         thoughtLevel,
			CompactedThisTurn:    compactedTurn,
			PrevRawCachedTokens:  prevRawCr,
			PrevTotalInputTokens: lastCached,
		})
		s.mu.Lock()
		if gm.Idx > s.LastSeenGenMetaIdx {
			s.LastSeenGenMetaIdx = gm.Idx
		}
		s.LastRawCachedTokens = gm.CachedReadTokens
		s.mu.Unlock()
		u := s.alignTpuTailUsage(gm.InputTokens, gm.CachedReadTokens, gm.OutputTokens, steps, "db")
		u.ThinkingTokens = gm.ThoughtTokens
		u.Degradation = deg
		return u
	}

	if dbStep := ReadLatestSessionStepUsage(sid); dbStep != nil {
		isNew := lastIdx < 0 || dbStep.StepIdx > lastIdx
		s.mu.Lock()
		s.LastSeenDbStepIdx = dbStep.StepIdx
		if isNew || !isToolUseStep {
			s.LastRawCachedTokens = dbStep.RawCr
		}
		s.mu.Unlock()
		if isNew || !isToolUseStep {
			u := s.alignTpuTailUsage(dbStep.RawIn, dbStep.RawCr, dbStep.OutTok, steps, "db")
			u.ThinkingTokens = dbStep.ThTok
			return u
		}
	}

	estIn := promptChars / 4
	if estIn < 16 {
		estIn = 120
	}
	estOut := outChars / 4
	if estOut < 16 {
		estOut = 32
	}

	if isToolUseStep {
		return &TurnUsage{
			InputTokens:      estIn,
			CachedReadTokens: lastCached,
			OutputTokens:     estOut,
			StepsInPrompt:    steps,
			UsageSource:      "estimate",
		}
	}

	if rawUsage == nil {
		return &TurnUsage{
			InputTokens:      estIn,
			CachedReadTokens: lastCached,
			OutputTokens:     estOut,
			StepsInPrompt:    steps,
			UsageSource:      "estimate",
		}
	}
	cumIn, _ := toInt(rawUsage["inputTokens"])
	cumCr, _ := toInt(rawUsage["cachedReadTokens"])
	outTok, _ := toInt(rawUsage["outputTokens"])
	s.mu.Lock()
	s.LastRawCachedTokens = cumCr / steps
	s.mu.Unlock()
	return s.alignTpuTailUsage(cumIn/steps, cumCr/steps, outTok, steps, "acp")
}

func (s *AcpProjectSession) normalizeTurnUsageAsync(rawUsage map[string]any, isToolUseStep bool, promptChars, outChars int) *TurnUsage {
	s.mu.Lock()
	sid := s.SessionID
	lastIdx := s.LastSeenDbStepIdx
	lastGenIdx := s.LastSeenGenMetaIdx
	s.mu.Unlock()

	for attempt := 0; attempt < 3; attempt++ {
		if gm := ReadLatestSessionGenMetadata(sid, lastGenIdx); gm != nil {
			break
		}
		if dbStep := ReadLatestSessionStepUsage(sid); dbStep != nil {
			if lastIdx < 0 || dbStep.StepIdx > lastIdx {
				break
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	return s.normalizeTurnUsage(rawUsage, isToolUseStep, promptChars, outChars)
}

func (s *AcpProjectSession) logTurnStats(mode string, elapsedMs int, res TurnResult) {
	inTok := 0
	crTok := 0
	outTok := 0
	stepsInPrompt := 1
	rawIn := 0
	rawCr := 0
	usageSource := "unknown"
	if res.Usage != nil {
		inTok = res.Usage.InputTokens
		crTok = res.Usage.CachedReadTokens
		outTok = res.Usage.OutputTokens
		stepsInPrompt = res.Usage.StepsInPrompt
		rawIn = res.Usage.RawUncachedInputTokens
		rawCr = res.Usage.RawTpuCachedTokens
		usageSource = res.Usage.UsageSource
	}
	hitRate := "N/A"
	if inTok+crTok > 0 {
		hitRate = formatPercent(float64(crTok) / float64(inTok+crTok) * 100)
	}
	ttft := res.TtftMs
	if ttft <= 0 {
		ttft = elapsedMs
	}
	tps := 0.0
	if elapsedMs > 0 && outTok > 0 {
		tps = math.Round((float64(outTok)/(float64(elapsedMs)/1000.0))*10) / 10
	}
	thinkLen := len(res.Thinking)
	thinkTok := 0
	if res.Usage != nil && res.Usage.ThinkingTokens > 0 {
		thinkTok = res.Usage.ThinkingTokens
	} else if thinkLen > 0 {
		thinkTok = int(math.Round(float64(thinkLen) / 3.2))
		if thinkTok < 1 {
			thinkTok = 1
		}
		if thinkTok > outTok {
			thinkTok = outTok
		}
	}

	now := time.Now().UTC()
	s.mu.Lock()
	idx := len(s.TurnHistory) + 1
	entry := TurnHistoryEntry{
		TurnIndex:      idx,
		CompletedAtMs:  now.UnixMilli(),
		Ts:             now.Format(time.RFC3339),
		SessionID:      s.SessionID,
		Mode:           mode,
		ElapsedMs:      elapsedMs,
		TtftMs:         ttft,
		Tps:            tps,
		HasThinking:    thinkTok > 0,
		ThinkingTokens: thinkTok,
		Usage:          res.Usage,
		CacheHitRate:   hitRate,
		StopReason:     res.StopReason,
		Model:          s.ModelID,
		ThoughtLevel:   s.ThoughtLevel,
	}
	s.TurnHistory = append(s.TurnHistory, entry)
	// Bounded ring buffer (max 150 entries) to prevent unbounded memory growth
	if len(s.TurnHistory) > 150 {
		s.TurnHistory = s.TurnHistory[len(s.TurnHistory)-150:]
	}
	sid := s.SessionID
	turnModel, turnTier := s.ModelID, s.ThoughtLevel
	s.mu.Unlock()
	GlobalStore.InsertTurn(s.ConvKey, entry)
	GlobalStore.UpsertSession(s)

	preview := res.Text
	if len(preview) > 120 {
		preview = preview[:120]
	}
	NotifyGatewayTurnCompleted()
	LogEvent("TURN_DONE", fmt.Sprintf("Completed turn (%s) in %dms", res.StopReason, elapsedMs), map[string]any{
		"convKey":                s.ConvKey,
		"sessionId":              sid,
		"model":                  turnModel,
		"thoughtLevel":           turnTier,
		"mode":                   mode,
		"ttftMs":                 ttft,
		"tps":                    tps,
		"hasThinking":            thinkTok > 0,
		"thinkingTokens":         thinkTok,
		"inputTokens":            inTok,
		"cachedReadTokens":       crTok,
		"rawUncachedInputTokens": rawIn,
		"rawTpuCachedTokens":     rawCr,
		"usageSource":            usageSource,
		"stepsInPrompt":          stepsInPrompt,
		"cacheHitRate":           hitRate,
		"outputTokens":           outTok,
		"toolCallsCount":         len(res.ToolCalls),
		"textPreview":            preview,
	})
}

func promptBlocksCharLen(blocks []map[string]any) int {
	total := 0
	for _, b := range blocks {
		if t, ok := b["text"].(string); ok {
			total += len(t)
		} else if res, ok := b["resource"].(map[string]any); ok {
			if rt, okT := res["text"].(string); okT {
				total += len(rt)
			}
		}
	}
	return total
}

func (s *AcpProjectSession) executePromptOrPark(ctx context.Context, promptBlocks []map[string]any, onDelta func(DeltaChunk), contDepth int) (TurnResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	sem := promptSem
	select {
	case sem <- struct{}{}:
	case <-ctx.Done():
		return TurnResult{}, ctx.Err()
	}

	if err := circuitError(); err != nil {
		<-sem
		return TurnResult{}, err
	}
	s.mu.Lock()
	s.PromptStepCount = 1
	s.RecentToolSigs = nil
	s.PromptInFlight = true
	sid := s.SessionID
	turn := newActiveTurnState(onDelta, func() { <-sem })
	s.activeTurn = turn
	s.mu.Unlock()

	promptChars := promptBlocksCharLen(promptBlocks)

	go func() {
		defer turn.releaseOnce.Do(turn.releaseSem)
		rawRes, err := GlobalEngine.RPC("session/prompt", map[string]any{
			"sessionId": sid,
			"prompt":    promptBlocks,
		}, 3600*time.Second)

		s.mu.Lock()
		s.PromptInFlight = false
		cur := s.activeTurn
		// After a parked tool resumes, resumeParkedTurn installs a NEW activeTurn on the same in-flight
		// prompt; the original turn was already resolved (tool_use). Resolve whichever turn is current.
		target := turn
		turn.mu.Lock()
		origResolved := turn.resolved
		turn.mu.Unlock()
		if origResolved && cur != nil && cur != turn {
			target = cur
		}
		if cur == target || cur == turn {
			s.activeTurn = nil
		}
		s.mu.Unlock()
		turn := target // shadow: do not mutate the outer variable from this goroutine
		defer func() {
			if turn.releaseSem != nil {
				turn.releaseOnce.Do(turn.releaseSem)
			}
		}()

		turn.mu.Lock()
		if turn.batchTimer != nil {
			turn.batchTimer.Stop()
		}
		alreadyResolved := turn.resolved
		turn.mu.Unlock()

		if alreadyResolved {
			return
		}

		if err != nil {
			turn.mu.Lock()
			turn.resolved = true
			turn.mu.Unlock()
			turn.closeDelta()
			if turn.releaseSem != nil {
				turn.releaseOnce.Do(turn.releaseSem)
			}
			select {
			case turn.errChan <- err:
			default:
			}
			return
		}

		var parsed struct {
			StopReason string         `json:"stopReason"`
			Usage      map[string]any `json:"usage"`
		}
		_ = json.Unmarshal(rawRes, &parsed)

		turn.mu.Lock()
		txt := strings.Join(turn.textChunks, "")
		thk := strings.Join(turn.thoughtChks, "")
		tcalls := turn.toolCalls
		ttft := int(time.Since(turn.createdAt).Milliseconds())
		if !turn.firstChunk.IsZero() {
			ttft = int(turn.firstChunk.Sub(turn.createdAt).Milliseconds())
		}
		if ttft < 10 {
			ttft = 10
		}
		turn.resolved = true
		turn.mu.Unlock()

		if parsed.StopReason == "max_tokens" && contDepth < 3 && len(tcalls) == 0 {
			turn.closeDelta()
			if turn.releaseSem != nil {
				turn.releaseOnce.Do(turn.releaseSem)
			}
			LogEvent("AUTO_CONTINUE", fmt.Sprintf("Auto-continuing truncated response (depth=%d)", contDepth+1), map[string]any{
				"convKey":   s.ConvKey,
				"sessionId": sid,
			})
			nextRes, contErr := s.executePromptOrPark(ctx, []map[string]any{
				{"type": "text", "text": "Continue exactly where you stopped. Do not repeat anything already written."},
			}, onDelta, contDepth+1)
			if contErr != nil {
				select {
				case turn.errChan <- contErr:
				default:
				}
				return
			}
			nextRes.Text = txt + nextRes.Text
			nextRes.Thinking = thk + nextRes.Thinking
			select {
			case turn.resolveChan <- nextRes:
			default:
			}
			return
		}

		turn.closeDelta()
		emitted := int(turn.emittedDeltas.Load())
		usage := s.normalizeTurnUsage(parsed.Usage, false, promptChars, len(txt)+len(thk))
		if turn.releaseSem != nil {
			turn.releaseOnce.Do(turn.releaseSem)
		}
		select {
		case turn.resolveChan <- TurnResult{
			StopReason:    "end_turn",
			Text:          txt,
			Thinking:      thk,
			ToolCalls:     nil,
			Usage:         usage,
			TtftMs:        ttft,
			EmittedDeltas: emitted,
		}:
		default:
		}
	}()

	select {
	case res := <-turn.resolveChan:
		return res, nil
	case err := <-turn.errChan:
		return TurnResult{}, err
	case <-ctx.Done():
		s.CancelActiveTurn("client_disconnect")
		return TurnResult{}, ctx.Err()
	}
}

func (s *AcpProjectSession) recordArgonDegradation(sid string, deg *ArgonDegradationReport, emittedDeltas int) {
	if deg == nil || !deg.Degraded {
		return
	}
	argonDegradationDetectedTotal.Add(1)
	LogEvent("ARGON_DEGRADATION_DETECTED", fmt.Sprintf("Detected Gemini 4 Argon shadow degradation (score=%d)", deg.Score), map[string]any{
		"convKey":       s.ConvKey,
		"sessionId":     sid,
		"score":         deg.Score,
		"signals":       deg.Signals,
		"cwmEstTokens":  deg.CwmEstTokens,
		"promptTokens":  deg.PromptTokens,
		"cachedTokens":  deg.CachedTokens,
		"outputTokens":  deg.OutputTokens,
		"thoughtTokens": deg.ThoughtTokens,
		"ttftSeconds":   deg.TtftSeconds,
		"streamSeconds": deg.StreamSeconds,
		"emittedDeltas": emittedDeltas,
	})
}

func (s *AcpProjectSession) resumeParkedTurn(ctx context.Context, matched []ToolResultInput, messages []map[string]any, onDelta func(DeltaChunk)) (TurnResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	t0 := time.Now()
	s.mu.Lock()
	s.PromptStepCount++
	s.CompactedCurrentTurn = false
	inFlight := s.PromptInFlight
	sid := s.SessionID
	s.mu.Unlock()

	if !inFlight {
		s.mu.Lock()
		s.ParkedCalls = make(map[string]*ParkedMcpCall)
		s.ParkedSince = time.Time{}
		s.mu.Unlock()
		var blocks []map[string]any
		var textLines []string
		for _, tr := range matched {
			textLines = append(textLines, fmt.Sprintf("[Tool Result (%s)]: %s", tr.ToolCallID, tr.Content))
			for _, img := range tr.Images {
				blocks = append(blocks, img)
			}
		}
		textLines = append(textLines, "\nContinue the task with these tool results.")
		blocks = append(blocks, map[string]any{"type": "text", "text": strings.Join(textLines, "\n\n")})
		val, err := s.executePromptOrPark(ctx, blocks, onDelta, 0)
		if err != nil {
			return TurnResult{}, err
		}
		if val.Usage != nil && val.Usage.Degradation != nil && val.Usage.Degradation.Degraded {
			s.recordArgonDegradation(sid, val.Usage.Degradation, val.EmittedDeltas)
			s.mu.Lock()
			s.ForceRolloverNext = true
			s.mu.Unlock()
		}
		elapsed := int(time.Since(t0).Milliseconds())
		s.mu.Lock()
		s.MessageCount = len(messages) + 1
		if fp := computeFirstUserFingerprint(messages); fp != "" {
			s.HistoryFingerprint = fp
		}
		if val.Usage != nil && (val.Usage.CachedReadTokens > 0 || val.Usage.InputTokens > 0) {
			s.LastCachedReadTokens = val.Usage.CachedReadTokens + val.Usage.InputTokens
		}
		s.mu.Unlock()
		val.SessionID = sid
		val.ConvKey = s.ConvKey
		val.ElapsedMs = elapsed
		val.Mode = "parked_mcp_resume"
		s.logTurnStats("parked_mcp_resume", elapsed, val)
		return val, nil
	}

	sem := promptSem
	select {
	case sem <- struct{}{}:
	case <-ctx.Done():
		s.CancelActiveTurn("client_disconnect")
		return TurnResult{}, ctx.Err()
	}

	if err := circuitError(); err != nil {
		<-sem
		return TurnResult{}, err
	}
	turn := newActiveTurnState(onDelta, func() { <-sem })
	var lateCalls []ToolCallItem
	s.mu.Lock()
	s.activeTurn = turn
	for _, tr := range matched {
		if parked, ok := s.ParkedCalls[tr.ToolCallID]; ok {
			delete(s.ParkedCalls, tr.ToolCallID)
			txt := tr.Content
			if txt == "" {
				txt = "OK"
			}
			contentBlocks := []map[string]any{{"type": "text", "text": txt}}
			contentBlocks = append(contentBlocks, tr.Images...)
			select {
			case parked.ResultChan <- map[string]any{
				"content": contentBlocks,
				"isError": tr.IsError,
			}:
			default:
			}
		}
	}
	for id, parked := range s.ParkedCalls {
		if !parked.Delivered {
			// Late parallel tool call arrived after the 80ms batch window: keep it parked and deliver it now!
			lateCalls = append(lateCalls, parked.Call)
			continue
		}
		delete(s.ParkedCalls, id)
		select {
		case parked.ResultChan <- map[string]any{
			"content": []map[string]any{{"type": "text", "text": "Tool result was not provided by host client."}},
			"isError": true,
		}:
		default:
		}
	}
	if len(s.ParkedCalls) == 0 {
		s.ParkedSince = time.Time{}
	}
	s.mu.Unlock()

	for _, lc := range lateCalls {
		s.enqueueToolCallToTurn(lc)
	}

	select {
	case val := <-turn.resolveChan:
		turn.releaseOnce.Do(turn.releaseSem)
		if val.Usage != nil && val.Usage.Degradation != nil && val.Usage.Degradation.Degraded {
			s.recordArgonDegradation(sid, val.Usage.Degradation, val.EmittedDeltas)
			s.mu.Lock()
			s.ForceRolloverNext = true
			s.mu.Unlock()
		}
		elapsed := int(time.Since(t0).Milliseconds())
		s.mu.Lock()
		s.MessageCount = len(messages) + 1
		if fp := computeFirstUserFingerprint(messages); fp != "" {
			s.HistoryFingerprint = fp
		}
		if val.Usage != nil && (val.Usage.CachedReadTokens > 0 || val.Usage.InputTokens > 0) {
			s.LastCachedReadTokens = val.Usage.CachedReadTokens + val.Usage.InputTokens
		}
		s.mu.Unlock()
		val.SessionID = sid
		val.ConvKey = s.ConvKey
		val.ElapsedMs = elapsed
		val.Mode = "parked_mcp_resume"
		s.logTurnStats("parked_mcp_resume", elapsed, val)
		return val, nil
	case err := <-turn.errChan:
		turn.releaseOnce.Do(turn.releaseSem)
		return TurnResult{}, err
	case <-ctx.Done():
		s.CancelActiveTurn("client_disconnect")
		return TurnResult{}, ctx.Err()
	}
}

func (s *AcpProjectSession) RunTurn(ctx context.Context, messages []map[string]any, toolsCatalog []ToolDef, thoughtLevel string, onDelta func(DeltaChunk), sel *ModelSelection) (TurnResult, error) {
	if err := circuitError(); err != nil {
		return TurnResult{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	s.LastUsedAt = time.Now()
	s.IsBusy = true
	hasParked := len(s.ParkedCalls) > 0
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.IsBusy = false
		s.LastUsedAt = time.Now()
		s.mu.Unlock()
	}()

	currentFirstFp := computeFirstUserFingerprint(messages)
	isCompactionReq := isCompactionSummaryRequest(messages)

	rolledOverFromParked := false
	if hasParked && len(messages) > 0 {
		s.mu.Lock()
		compactedWhileParked := s.ForceRolloverNext ||
			(s.MessageCount > 0 && len(messages) < s.MessageCount) ||
			(s.HistoryFingerprint != "" && currentFirstFp != "" && currentFirstFp != s.HistoryFingerprint)
		oldMsgCnt := s.MessageCount
		oldSid := s.SessionID
		s.mu.Unlock()

		if compactedWhileParked {
			LogEvent("COMPACTION_ROLLOVER", fmt.Sprintf("Conversation compacted while tool was parked (%d -> %d msgs); closing old session and rolling over", oldMsgCnt, len(messages)), map[string]any{
				"convKey":      s.ConvKey,
				"oldSessionId": oldSid,
				"newMsgCount":  len(messages),
			})
			s.HibernateWorker("compaction_rollover_parked", true)
			s.mu.Lock()
			s.ForceRolloverNext = false
			s.CompactionRollovers++
			s.mu.Unlock()
			hasParked = false
			rolledOverFromParked = true
		} else {
			trs := extractToolResultsFromMessages(messages)
			var matched []ToolResultInput
			s.mu.Lock()
			for _, tr := range trs {
				if _, ok := s.ParkedCalls[tr.ToolCallID]; ok {
					matched = append(matched, tr)
				}
			}
			if len(matched) > 0 {
				s.SessionReuses++
			}
			sid := s.SessionID
			s.mu.Unlock()

			if len(matched) > 0 {
				var ids []string
				for _, m := range matched {
					ids = append(ids, m.ToolCallID)
				}
				LogEvent("PARKED_MCP_RESUME", fmt.Sprintf("Resuming %d parked MCP tool call(s)", len(matched)), map[string]any{
					"convKey":     s.ConvKey,
					"sessionId":   sid,
					"toolCallIds": ids,
				})
				return s.resumeParkedTurn(ctx, matched, messages, onDelta)
			}
		}
	}

	if sel != nil {
		s.mu.Lock()
		noSession := s.SessionID == ""
		s.mu.Unlock()
		if noSession {
			s.applyModelSelection(*sel)
		}
	}
	if err := s.ensureBooted(toolsCatalog, thoughtLevel); err != nil {
		return TurnResult{}, err
	}
	if sel != nil {
		s.applyModelSelection(*sel)
	}

	s.mu.Lock()
	isRollover := s.ForceRolloverNext
	s.ForceRolloverNext = false
	if s.MessageCount > 0 {
		if len(messages) < s.MessageCount {
			isRollover = true
		} else if s.HistoryFingerprint != "" && currentFirstFp != "" && currentFirstFp != s.HistoryFingerprint {
			isRollover = true
		}
	} else if s.SessionID != "" && len(s.TurnHistory) > 0 && s.TurnHistory[len(s.TurnHistory)-1].SessionID == s.SessionID {
		// Safety guard: if MessageCount was reset to 0 on an already-used session, never append full history onto it
		isRollover = true
	}
	oldSid := s.SessionID
	toolFp := s.ToolFingerprint
	s.mu.Unlock()

	if isRollover {
		s.mu.Lock()
		s.CompactionRollovers++
		s.mu.Unlock()
		LogEvent("COMPACTION_ROLLOVER", fmt.Sprintf("Prefix changed or compacted, rolling over session"), map[string]any{
			"convKey":      s.ConvKey,
			"oldSessionId": oldSid,
		})
		if err := s.openFreshSession(toolsCatalog, thoughtLevel, toolFp); err != nil {
			return TurnResult{}, err
		}
	} else {
		s.mu.Lock()
		if s.MessageCount > 0 {
			s.SessionReuses++
		}
		s.mu.Unlock()
	}

	s.mu.Lock()
	msgCnt := s.MessageCount
	reuses := s.SessionReuses
	sid := s.SessionID
	wakeMethod := s.LastWakeMethod
	s.LastWakeMethod = ""
	s.CompactedCurrentTurn = isRollover || rolledOverFromParked
	s.mu.Unlock()

	isMidColdStart := msgCnt == 0 && len(messages) > 2
	var unseen []map[string]any
	if msgCnt < len(messages) {
		unseen = messages[msgCnt:]
	}
	s.mu.Lock()
	transcriptCap := ModelOrDefault(s.ModelID).TranscriptChars
	curModel, curTier := s.ModelID, s.ThoughtLevel
	s.mu.Unlock()
	promptBlocks := buildPromptBlocks(unseen, isRollover || rolledOverFromParked || isMidColdStart, messages, transcriptCap)
	mode := "cold_start"
	if isRollover || rolledOverFromParked {
		mode = "compaction_rollover"
	} else if wakeMethod != "" {
		mode = "resumed_delta"
	} else if reuses > 0 {
		mode = "sticky_delta"
	}

	LogEvent("TURN_START", fmt.Sprintf("Sending session/prompt (%s)", mode), map[string]any{
		"convKey":          s.ConvKey,
		"sessionId":        sid,
		"model":            curModel,
		"thoughtLevel":     curTier,
		"totalMessages":    len(messages),
		"unseenDeltaCount": len(unseen),
	})

	t0 := time.Now()
	res, err := s.executePromptOrPark(ctx, promptBlocks, onDelta, 0)
	if err != nil {
		if IsTosViolationError(err.Error()) {
			TripTosCircuitBreaker(err.Error())
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			s.mu.Lock()
			s.MessageCount = len(messages)
			s.mu.Unlock()
			return TurnResult{}, err
		}
		s.HibernateWorker("prompt_rpc_error", true)
		return TurnResult{}, err
	}

	if res.Usage != nil && res.Usage.Degradation != nil && res.Usage.Degradation.Degraded {
		deg := res.Usage.Degradation
		s.recordArgonDegradation(sid, deg, res.EmittedDeltas)
		if res.EmittedDeltas == 0 {
			s.HibernateWorker("argon_degraded_retry", true)
			s.mu.Lock()
			s.CompactedCurrentTurn = true
			s.mu.Unlock()
			if openErr := s.openFreshSession(toolsCatalog, thoughtLevel, toolFp); openErr == nil {
				if sel != nil {
					s.applyModelSelection(*sel)
				}
				s.mu.Lock()
				newSid := s.SessionID
				s.mu.Unlock()
				retryBlocks := buildPromptBlocks(messages, true, messages, transcriptCap)
				if retryRes, retryErr := s.executePromptOrPark(ctx, retryBlocks, onDelta, 0); retryErr == nil {
					argonDegradationRecoveredTotal.Add(1)
					LogEvent("ARGON_DEGRADATION_RECOVERED", "Recovered from Gemini 4 Argon shadow degradation via transparent fresh-session replay", map[string]any{
						"convKey":      s.ConvKey,
						"oldSessionId": sid,
						"newSessionId": newSid,
						"prevScore":    deg.Score,
					})
					res = retryRes
					sid = newSid
					mode = "argon_degraded_recovery"
				}
			}
		} else {
			s.mu.Lock()
			s.ForceRolloverNext = true
			hasParkedAfter := len(s.ParkedCalls) > 0
			s.mu.Unlock()
			if !hasParkedAfter {
				go func() {
					time.Sleep(200 * time.Millisecond)
					s.HibernateWorker("argon_degraded_rollover", true)
				}()
			}
		}
	}
	elapsed := int(time.Since(t0).Milliseconds())

	if DetectAntigravitySoftError(res.Text) != nil {
		s.HibernateWorker("soft_error_reset", true)
	} else {
		s.mu.Lock()
		s.MessageCount = len(messages) + 1
		if currentFirstFp != "" {
			s.HistoryFingerprint = currentFirstFp
		}
		if isCompactionReq {
			// Next turn after a compaction summary will carry the compacted message list; force a clean rollover
			s.ForceRolloverNext = true
			go func() {
				time.Sleep(500 * time.Millisecond)
				s.HibernateWorker("compaction_summary_done", true)
			}()
		}
		if res.Usage != nil && (res.Usage.CachedReadTokens > 0 || res.Usage.InputTokens > 0) {
			s.LastCachedReadTokens = res.Usage.CachedReadTokens + res.Usage.InputTokens
		}
		s.mu.Unlock()
	}

	res.SessionID = sid
	res.ConvKey = s.ConvKey
	res.ElapsedMs = elapsed
	res.Mode = mode
	s.logTurnStats(mode, elapsed, res)
	return res, nil
}

func (s *AcpProjectSession) RunKeepalivePing(onDelta func(DeltaChunk)) TurnResult {
	s.mu.Lock()
	idleSec := int(time.Since(s.LastUsedAt).Seconds())
	estCached := s.LastCachedReadTokens
	if estCached == 0 {
		estCached = 30000
	}
	sid := s.SessionID
	s.mu.Unlock()

	LogEvent("KEEPALIVE_SKIP_BUSY", fmt.Sprintf("Session %s fast-acknowledged keepalive (0 upstream cost)", s.ConvKey), map[string]any{
		"sessionId":        sid,
		"cachedReadTokens": estCached,
		"idleSec":          idleSec,
	})
	if onDelta != nil {
		onDelta(DeltaChunk{Type: "text", Text: "ok"})
	}
	return TurnResult{
		StopReason: "end_turn",
		Text:       "ok",
		Usage:      &TurnUsage{InputTokens: 10, CachedReadTokens: estCached, OutputTokens: 1, StepsInPrompt: 1},
		SessionID:  sid,
		ConvKey:    s.ConvKey,
		ElapsedMs:  1,
		TtftMs:     1,
		Mode:       "keepalive_fast_ack",
	}
}
