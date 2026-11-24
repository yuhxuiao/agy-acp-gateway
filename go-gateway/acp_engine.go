package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"agy-acp-gateway-go/internal/acp"
	gwcfg "agy-acp-gateway-go/internal/config"
	"agy-acp-gateway-go/internal/protocol"
	gwstore "agy-acp-gateway-go/internal/store"
)

type ToolDef = protocol.ToolDef
type ToolCallItem = protocol.ToolCallItem
type ToolResultInput = protocol.ToolResultInput
type TurnUsage = gwstore.TurnUsage
type TurnHistoryEntry = gwstore.TurnHistoryEntry

var _ acp.Engine = (*SharedAcpEngine)(nil)

type ParkedMcpCall struct {
	Call          ToolCallItem
	SessionID     string
	ResultChan    chan map[string]any
	Delivered     bool
	PreDispatched bool
	PlannerIdx    int
	CanonicalSig  string
	ParkedAt      time.Time
	RelayCount    int
}

// toolRelayInterval is how long one MCP tools/call waits for a host result before relaying the
// model to dsh_await_result. Upstream implicit KV cache survival stays near 100% inside 30-90s
// before decaying past 90-135s, so the default is 55s (AGY_TOOL_RELAY_SEC). A call that keeps
// relaying for 10+ minutes backs off to 85s and after 30 minutes to 120s to limit extra generations.
func toolRelayInterval(parkedFor time.Duration) time.Duration {
	if ms := envInt("AGY_TOOL_RELAY_MS", 0); ms > 0 {
		return time.Duration(ms) * time.Millisecond
	}
	base := time.Duration(envInt("AGY_TOOL_RELAY_SEC", 55)) * time.Second
	if base < 15*time.Second {
		base = 15 * time.Second
	}
	if base > 300*time.Second {
		base = 300 * time.Second
	}
	switch {
	case parkedFor >= 30*time.Minute && base < 120*time.Second:
		return 120 * time.Second
	case parkedFor >= 10*time.Minute && base < 85*time.Second:
		return 85 * time.Second
	}
	return base
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
	ThinkingMs    int            `json:"thinkingMs,omitempty"`
	RetryCount    int            `json:"retryCount,omitempty"`
	Mode          string         `json:"mode"`
	EmittedDeltas int            `json:"-"`
	FirstChunkAt  time.Time      `json:"-"`
	// FailedNotice is set when agy ended the turn on a terminal failure notice after some reply
	// text; the HTTP layer turns it into a protocol error instead of a successful end_turn.
	FailedNotice string `json:"-"`
}

type DeltaChunk struct {
	Type     string
	Text     string
	Thinking string
}

// acpRPCError is a JSON-RPC error object returned by agy (as opposed to a transport failure or
// timeout). Error() keeps the raw JSON so existing substring classification still applies.
type acpRPCError struct{ Raw string }

func (e *acpRPCError) Error() string { return e.Raw }

type rpcResult struct {
	Result json.RawMessage
	Err    error
}

type SharedAcpEngine struct {
	placement        promptPlacement
	capsMu           sync.RWMutex
	caps             acpCapabilities
	mu               sync.Mutex
	cmd              *exec.Cmd
	exitDone         chan struct{}
	replyCh          chan map[string]any
	stdin            *bufio.Writer
	stdinCloser      io.Closer
	stdinMu          sync.Mutex
	gen              atomic.Uint64
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

func (e *SharedAcpEngine) CurrentGen() uint64 {
	return e.gen.Load()
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

var (
	acpSemverRe   = regexp.MustCompile(`\b(\d+\.\d+\.\d+)\b`)
	semverCacheMu sync.Mutex
	semverCache   = map[string]string{}
)

func detectAgySemver(binPath string) string {
	const fallback = "1.5.10"
	if strings.TrimSpace(binPath) == "" {
		return fallback
	}
	cacheKey := binPath
	if resolved, err := filepath.EvalSymlinks(binPath); err == nil && resolved != "" {
		if st, sErr := os.Stat(resolved); sErr == nil {
			cacheKey = fmt.Sprintf("%s|%d|%d", resolved, st.Size(), st.ModTime().UnixNano())
		} else {
			cacheKey = resolved
		}
	}
	semverCacheMu.Lock()
	if v, ok := semverCache[cacheKey]; ok {
		semverCacheMu.Unlock()
		return v
	}
	semverCacheMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, binPath, "--version").Output()
	ver := fallback
	if err == nil {
		if m := acpSemverRe.FindStringSubmatch(string(out)); len(m) == 2 {
			ver = m[1]
		}
	}
	semverCacheMu.Lock()
	semverCache[cacheKey] = ver
	semverCacheMu.Unlock()
	return ver
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
	// Child agy processes keep an isolated 180MiB GOMEMLIMIT rather than inheriting the
	// gateway's 256MiB ceiling so 8 resident workers stay well inside container memory bounds.
	snap := gwcfg.Current()
	gomem := snap.ChildGoMemLimit
	gogc := snap.ChildGoGC
	// Upstream proxy for agy -> Google. Respects primary account proxy overrides if configured.
	proxy := resolvePrimaryUpstreamProxy()

	LogEvent("ACP_ENGINE_START", "Starting single shared agy-acp-900k Pure-API engine (Go Native)", map[string]any{
		"home":       PureApiHome,
		"gomemlimit": gomem,
	})

	clientVer := detectAgySemver(PatchedAgyBin)
	cmd := exec.Command(
		PatchedAgyBin,
		"acp",
		"--gemini-dir", PureApiHome,
		"--app-data-dir", "antigravity-cli",
		"--agent", McpBridgeName,
		"--process-per-session",
		"--override_user_agent_name", "antigravity",
		"--disable-telemetry",
	)
	cmd.Dir = DefaultCwd
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	agyHome := resolveIsolatedAgyHome(PureApiHome)
	env := make([]string, 0, len(os.Environ())+14)
	hasSSH := false
	for _, kv := range os.Environ() {
		if isStrippedLeafAgySubprocessEnv(kv) {
			continue
		}
		if agyHome != "" && strings.HasPrefix(kv, "HOME=") {
			continue
		}
		if strings.HasPrefix(kv, "SSH_CONNECTION=") {
			hasSSH = true
		}
		env = append(env, kv)
	}
	if agyHome != "" {
		env = append(env, "HOME="+agyHome)
		if runtime.GOOS == "darwin" && !hasSSH {
			env = append(env, "SSH_CONNECTION=1 1 1 1")
		}
	}
	env = append(env,
		"GOGC="+gogc,
		"GOMEMLIMIT="+gomem,
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
	// Native Linux credentials use the existing user Secret Service. Placement
	// mode reuses that authorized login; masking its bus makes ACP unauthenticated.
	if !firstPromptPlacement() {
		env = append(env, "DBUS_SESSION_BUS_ADDRESS=unix:path=/dev/null")
	}
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
	recordEngineBinary()
	if err := cmd.Start(); err != nil {
		tosMu.Unlock()
		return err
	}

	myGen := e.gen.Add(1)
	e.placement.activate(myGen)
	e.mu.Lock()
	e.cmd = cmd
	done := make(chan struct{})
	replyCh := make(chan map[string]any, 256)
	e.exitDone = done
	e.replyCh = replyCh
	e.stdin = bufio.NewWriterSize(stdinPipe, 256*1024)
	e.stdinCloser = stdinPipe
	e.mu.Unlock()

	go e.writeControlRepliesLoop(replyCh, done)
	go func() { defer close(done); e.readStdoutLoop(stdoutPipe, cmd, myGen) }()
	tosMu.Unlock()

	rawInit, err := e.RPC("initialize", map[string]any{
		"protocolVersion": 1,
		"clientCapabilities": map[string]any{
			"fs":       map[string]any{"readTextFile": false, "writeTextFile": false},
			"terminal": false,
		},
		"clientInfo": map[string]any{"name": "antigravity", "version": clientVer},
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
	publishEngineInfo()
	return nil
}

func processGroupAlive(pgid int) bool {
	if pgid <= 0 {
		return false
	}
	err := syscall.Kill(-pgid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

func ensureProcessGroupStopped(pgid int, grace time.Duration) {
	if !processGroupAlive(pgid) {
		return
	}
	if grace > 0 {
		deadline := time.Now().Add(grace)
		for time.Now().Before(deadline) {
			if !processGroupAlive(pgid) {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	if processGroupAlive(pgid) {
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
	}
}

func (e *SharedAcpEngine) StopEngine() {
	e.mu.Lock()
	for e.stopping {
		e.bootCond.Wait()
	}
	e.stopping = true
	cmd, done, stdinCloser := e.cmd, e.exitDone, e.stdinCloser
	retiringGen := e.CurrentGen()
	e.cmd, e.stdin, e.stdinCloser, e.replyCh = nil, nil, nil, nil
	e.mu.Unlock()
	e.placement.retire(retiringGen)
	defer func() { e.mu.Lock(); e.stopping = false; e.bootCond.Broadcast(); e.mu.Unlock() }()
	if stdinCloser != nil {
		_ = stdinCloser.Close()
	}
	pgid := 0
	if cmd != nil && cmd.Process != nil {
		pgid = cmd.Process.Pid
		if processGroupAlive(pgid) {
			_ = syscall.Kill(-pgid, syscall.SIGTERM)
		} else {
			_ = cmd.Process.Signal(syscall.SIGTERM)
		}
	}
	if done != nil {
		select {
		case <-done:
			ensureProcessGroupStopped(pgid, 150*time.Millisecond)
			return
		case <-time.After(2 * time.Second):
		}
		if processGroupAlive(pgid) {
			_ = syscall.Kill(-pgid, syscall.SIGKILL)
		} else if cmd != nil && cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		<-done
		ensureProcessGroupStopped(pgid, 0)
	}
}

const (
	maxAcpOutboundJSONBytes = 56 * 1024 * 1024
	maxAcpInboundLineBytes  = 128 * 1024 * 1024
)

var stdinWriteWatchdogTimeout = 15 * time.Second

var errAcpStdoutLineTooLong = errors.New("ACP stdout JSON-RPC line exceeded 128MB limit")

func isStrippedAgyEngineEnv(kv string) bool {
	for _, prefix := range []string{
		"GOGC=", "GOMEMLIMIT=", "DBUS_SESSION_BUS_ADDRESS=", "AGY_CLI_DISABLE_AUTO_UPDATE=",
		"HTTP_PROXY=", "HTTPS_PROXY=", "http_proxy=", "https_proxy=",
		"ALL_PROXY=", "all_proxy=", "NO_PROXY=", "no_proxy=",
		"GEMINI_API_KEY=", "GOOGLE_API_KEY=", "GOOGLE_APPLICATION_CREDENTIALS=",
		"GOOGLE_CLOUD_PROJECT=", "GOOGLE_CLOUD_LOCATION=", "GOOGLE_CLOUD_QUOTA_PROJECT=",
		"GOOGLE_GENAI_USE_VERTEXAI=", "GCLOUD_PROJECT=", "CLOUDSDK_CORE_PROJECT=",
		"AGY_ACP_CCPA_PROJECT=", "GEMINI_HOME=",
		"ELECTRON_RUN_AS_NODE=", "CHROME_CRASHPAD_PIPE_NAME=",
	} {
		if strings.HasPrefix(kv, prefix) {
			return true
		}
	}
	return false
}

// isStrippedLeafAgySubprocessEnv strips gateway secrets and Resin proxy credentials from leaf
// official `agy` subprocesses (`agy acp` and `agy -p /usage`) so they never appear in /proc/<pid>/environ.
func isStrippedLeafAgySubprocessEnv(kv string) bool {
	if isStrippedAgyEngineEnv(kv) {
		return true
	}
	for _, prefix := range []string{
		"AGY_API_KEYS=", "AGY_API_KEYS_FILE=",
		"AGY_RESIN_PROXY_TOKEN=", "AGY_RESIN_HOST=",
		"AGY_PROXY_PRESETS_FILE=", "AGY_UPSTREAM_PROXY=",
		"AGY_DASHBOARD_PEERS=",
	} {
		if strings.HasPrefix(kv, prefix) {
			return true
		}
	}
	return false
}

func (e *SharedAcpEngine) writeControlRepliesLoop(replyCh <-chan map[string]any, stop <-chan struct{}) {
	for {
		select {
		case payload := <-replyCh:
			if payload != nil {
				_ = e.sendRawJSON(payload)
			}
		case <-stop:
			return
		}
	}
}

func (e *SharedAcpEngine) EnqueueControlReply(payload map[string]any) {
	e.enqueueControlReply(payload)
}

func (e *SharedAcpEngine) enqueueControlReply(payload map[string]any) {
	if payload == nil {
		return
	}
	e.mu.Lock()
	ch := e.replyCh
	e.mu.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- payload:
	default:
		go func() {
			_ = e.sendRawJSON(payload)
		}()
	}
}

func (e *SharedAcpEngine) sendRawJSON(payload map[string]any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if len(b)+1 > maxAcpOutboundJSONBytes {
		return &SoftError{
			StatusCode: http.StatusRequestEntityTooLarge,
			ErrorType:  "invalid_request_error",
			Message:    fmt.Sprintf("outbound ACP JSON-RPC payload (%d bytes) exceeds %d byte pipe limit", len(b)+1, maxAcpOutboundJSONBytes),
		}
	}
	recordAcpLine("out", b)
	if McpTrace {
		pv := redactSecretsInString(string(b))
		if len(pv) > 400 {
			pv = pv[:400]
		}
		LogEvent("ACP_OUT", pv, nil)
	}
	e.stdinMu.Lock()
	defer e.stdinMu.Unlock()
	if method, _ := payload["method"].(string); method != "" && method != "session/close" && method != "session/cancel" {
		tosMu.Lock()
		tripped, _ := tosTrippedLocked()
		tosMu.Unlock()
		if tripped {
			return newCircuitError()
		}
	}
	e.mu.Lock()
	w := e.stdin
	curCmd := e.cmd
	e.mu.Unlock()
	if w == nil {
		return errors.New("shared ACP engine not running")
	}
	if stdinWriteWatchdogTimeout > 0 && curCmd != nil {
		wd := time.AfterFunc(stdinWriteWatchdogTimeout, func() {
			e.mu.Lock()
			same := e.cmd == curCmd
			e.mu.Unlock()
			if same {
				LogEvent("ACP_STDIN_WEDGED", "Shared ACP engine stdin blocked past watchdog; restarting engine", nil)
				e.StopEngine()
			}
		})
		defer wd.Stop()
	}
	if _, err := w.Write(b); err != nil {
		return err
	}
	if err := w.WriteByte('\n'); err != nil {
		return err
	}
	return w.Flush()
}

func (e *SharedAcpEngine) RPC(method string, params any, timeout time.Duration) (json.RawMessage, error) {
	if firstPromptPlacement() {
		return e.placement.call(method, params, e.CurrentGen(), e.rpcNative, timeout)
	}
	return e.rpcNative(method, params, timeout)
}

func (e *SharedAcpEngine) rpcNative(method string, params any, timeout time.Duration) (json.RawMessage, error) {
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

func isBenignCloseSessionErr(err error) bool {
	if err == nil {
		return true
	}
	low := strings.ToLower(err.Error())
	return strings.Contains(low, "not found") ||
		strings.Contains(low, "unknown session") ||
		strings.Contains(low, "no such session") ||
		strings.Contains(low, "session does not exist") ||
		strings.Contains(low, "shared acp engine not running") ||
		strings.Contains(low, "shared acp engine exited")
}

func (e *SharedAcpEngine) RegisterListener(sid string, listener acp.SessionListener) {
	if listener != nil {
		listener.SetEngineGen(e.CurrentGen())
	}
	e.sessionListeners.Store(sid, listener)
	RegisterGatewaySessionID(sid)
}

func (e *SharedAcpEngine) RegisterSession(sid string, sess *AcpProjectSession) {
	if sess == nil {
		e.RegisterListener(sid, nil)
		return
	}
	e.RegisterListener(sid, sess)
}

func (e *SharedAcpEngine) UnregisterSession(sid string) {
	if sid == "" {
		return
	}
	e.sessionListeners.Delete(sid)
}

func (e *SharedAcpEngine) CloseSession(sid string) error {
	closingGen := e.CurrentGen()
	if sid == "" || !e.IsAlive() {
		e.placement.forget(sid, closingGen)
		if e.CurrentGen() == closingGen {
			e.sessionListeners.Delete(sid)
		}
		return nil
	}
	var err error
	if firstPromptPlacement() {
		_, err = e.placement.call("session/close", map[string]any{"sessionId": sid}, closingGen, e.rpcNative, 10*time.Second)
	} else {
		_, err = e.RPC("session/close", map[string]any{"sessionId": sid}, 10*time.Second)
	}
	if isBenignCloseSessionErr(err) {
		e.placement.forget(sid, closingGen)
		if e.CurrentGen() == closingGen {
			e.sessionListeners.Delete(sid)
		}
		return nil
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

func readJSONRPCLine(br *bufio.Reader, maxBytes int) ([]byte, error) {
	if maxBytes <= 0 {
		maxBytes = maxAcpInboundLineBytes
	}
	var buf []byte
	for {
		frag, err := br.ReadSlice('\n')
		if errors.Is(err, bufio.ErrBufferFull) {
			if len(buf)+len(frag) > maxBytes {
				return nil, errAcpStdoutLineTooLong
			}
			buf = append(buf, frag...)
			continue
		}
		if len(buf) == 0 {
			trimmed := bytes.TrimRight(frag, "\r\n")
			if len(trimmed) > maxBytes {
				return nil, errAcpStdoutLineTooLong
			}
			return trimmed, err
		}
		if len(buf)+len(frag) > maxBytes {
			return nil, errAcpStdoutLineTooLong
		}
		buf = append(buf, frag...)
		return bytes.TrimRight(buf, "\r\n"), err
	}
}

func waitCmdWithKill(cmd *exec.Cmd, killOnWait bool) {
	if cmd == nil {
		return
	}
	pgid := 0
	if cmd.Process != nil {
		pgid = cmd.Process.Pid
	}
	if killOnWait && processGroupAlive(pgid) {
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
	}
	var reaped atomic.Bool
	waitCh := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		reaped.Store(true)
		close(waitCh)
	}()
	timer := time.NewTimer(1 * time.Second)
	defer timer.Stop()
	select {
	case <-waitCh:
	case <-timer.C:
		if !reaped.Load() || processGroupAlive(pgid) {
			_ = syscall.Kill(-pgid, syscall.SIGKILL)
		}
		<-waitCh
	}
	if killOnWait {
		ensureProcessGroupStopped(pgid, 0)
	}
}

func (e *SharedAcpEngine) readStdoutLoop(stdoutPipe any, cmd *exec.Cmd, myGen uint64) {
	defer e.placement.retire(myGen)
	r, ok := stdoutPipe.(io.Reader)
	if !ok {
		return
	}
	br := bufio.NewReaderSize(r, 256*1024)
	var loopErr error

	for {
		line, readErr := readJSONRPCLine(br, maxAcpInboundLineBytes)
		if len(line) > 0 {
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
			if err := json.Unmarshal(line, &msg); err == nil {
				if msg.ID != nil && msg.Method != "" && len(msg.Result) == 0 && len(msg.Error) == 0 {
					if msg.Method == "session/request_permission" {
						var p struct {
							SessionID string                 `json:"sessionId"`
							Options   []acp.PermissionOption `json:"options"`
							ToolCall  map[string]any         `json:"toolCall"`
						}
						_ = json.Unmarshal(msg.Params, &p)
						if v, ok := e.sessionListeners.Load(p.SessionID); ok {
							if l, okL := v.(acp.SessionListener); okL && l != nil {
								l.HandlePermissionRequest(msg.ID, p.Options, p.ToolCall)
							} else {
								replyPermissionOutcomeWithEngine(e, msg.ID, p.Options, false)
							}
						} else {
							replyPermissionOutcomeWithEngine(e, msg.ID, p.Options, false)
						}
					} else {
						// The gateway advertises no fs/terminal/elicitation client capabilities. Answer any other
						// agent->client request asynchronously so readStdoutLoop never blocks on stdinPipe.
						LogEvent("ACP_CLIENT_REQUEST_REJECTED", "Rejected unsupported agent->client request: "+msg.Method, nil)
						e.enqueueControlReply(map[string]any{
							"jsonrpc": "2.0",
							"id":      msg.ID,
							"error":   map[string]any{"code": -32601, "message": "method not supported by agy-acp-gateway: " + msg.Method},
						})
					}
				} else if msg.Method == "session/update" {
					var p struct {
						SessionID string         `json:"sessionId"`
						Update    map[string]any `json:"update"`
					}
					if err := json.Unmarshal(msg.Params, &p); err == nil && p.SessionID != "" {
						if v, ok := e.sessionListeners.Load(p.SessionID); ok {
							if l, okL := v.(acp.SessionListener); okL && l != nil {
								l.HandleSessionUpdate(p.Update)
							}
						}
					}
				} else if msg.ID != nil {
					if numID, ok := toInt(msg.ID); ok {
						if v, okP := e.pending.LoadAndDelete(int64(numID)); okP {
							ch := v.(chan rpcResult)
							var res rpcResult
							if len(msg.Error) > 0 && string(msg.Error) != "null" {
								res = rpcResult{Err: &acpRPCError{Raw: string(msg.Error)}}
							} else {
								res = rpcResult{Result: msg.Result}
							}
							select {
							case ch <- res:
							default:
							}
						}
					}
				}
			}
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				loopErr = readErr
			}
			break
		}
	}

	// Gate concurrent EnsureStarted() / StopEngine() while reaping the dead engine and draining
	// its pending RPCs & sessionListeners so a gen+1 engine cannot boot and have its state wiped.
	e.mu.Lock()
	ownStopping := false
	if !e.stopping {
		e.stopping = true
		ownStopping = true
	}
	e.mu.Unlock()

	killChild := loopErr != nil
	if killChild {
		LogEvent("ACP_STDOUT_ERR", "Aborted ACP stdout read loop: "+loopErr.Error(), nil)
		if rc, okC := stdoutPipe.(io.Closer); okC {
			_ = rc.Close()
		}
	}
	waitCmdWithKill(cmd, killChild)
	engineExitsTotal.Add(1)

	e.pending.Range(func(k, v any) bool {
		ch := v.(chan rpcResult)
		select {
		case ch <- rpcResult{Err: errors.New("shared ACP engine exited")}:
		default:
		}
		e.pending.Delete(k)
		return true
	})
	e.sessionListeners.Range(func(k, v any) bool {
		sid, _ := k.(string)
		e.sessionListeners.Delete(k)
		if l, okS := v.(acp.SessionListener); okS && l != nil {
			l.OnEngineExited(sid, myGen)
		}
		return true
	})

	e.mu.Lock()
	if e.cmd == cmd {
		e.cmd = nil
		e.stdin = nil
		e.stdinCloser = nil
		e.replyCh = nil
		e.exitDone = nil
	}
	if ownStopping {
		e.stopping = false
		e.bootCond.Broadcast()
	}
	e.mu.Unlock()
	LogEvent("ACP_ENGINE_EXIT", "Shared agy-acp-900k engine exited", nil)
}

const maxTurnDeltaCoalesceBytes = 32 * 1024 * 1024

type turnSubscriber struct {
	cb         func(DeltaChunk)
	queue      []DeltaChunk
	queueBytes int
	draining   bool
	wake       chan struct{}
	stop       chan struct{}
	stopOnce   sync.Once
	done       chan struct{}
}

func (sub *turnSubscriber) signalStop() {
	if sub != nil {
		sub.stopOnce.Do(func() { close(sub.stop) })
	}
}

const maxSingleDeltaChunkBytes = 4096

func appendOrCoalesceDelta(buf []DeltaChunk, curBytes int, c DeltaChunk, maxBytes int) ([]DeltaChunk, int, bool) {
	n := len(c.Text) + len(c.Thinking)
	if curBytes+n > maxBytes {
		return buf, curBytes, false
	}
	if ln := len(buf); ln > 0 && buf[ln-1].Type == c.Type {
		if c.Type == "text" && len(buf[ln-1].Text)+len(c.Text) <= maxSingleDeltaChunkBytes {
			buf[ln-1].Text += c.Text
			return buf, curBytes + n, true
		}
		if c.Type == "thinking" && len(buf[ln-1].Thinking)+len(c.Thinking) <= maxSingleDeltaChunkBytes {
			buf[ln-1].Thinking += c.Thinking
			return buf, curBytes + n, true
		}
	}
	return append(buf, c), curBytes + n, true
}

type deltaGuardKey struct{}

// guardDeltaCallback wraps a non-nil HTTP/caller streaming callback with an uncontended per-request
// mutex + closed latch so session locks/queues release in O(1) nanoseconds on soft-detach while the
// departing HTTP ServeHTTP goroutine waits for any in-flight ResponseWriter Flush() before returning.
func guardDeltaCallback(ctx context.Context, onDelta func(DeltaChunk)) (context.Context, func(DeltaChunk), func()) {
	if ctx == nil {
		ctx = context.Background()
	}
	if onDelta == nil {
		return ctx, nil, func() {}
	}
	if already, _ := ctx.Value(deltaGuardKey{}).(bool); already {
		return ctx, onDelta, func() {}
	}
	var cbMu sync.Mutex
	cbClosed := false
	wrapped := func(c DeltaChunk) {
		cbMu.Lock()
		defer cbMu.Unlock()
		if cbClosed {
			return
		}
		onDelta(c)
	}
	finish := func() {
		cbMu.Lock()
		cbClosed = true
		cbMu.Unlock()
	}
	return context.WithValue(ctx, deltaGuardKey{}, true), wrapped, finish
}

type activeTurnState struct {
	mu                     sync.Mutex
	createdAt              time.Time
	turnCtx                context.Context
	cancelTurn             context.CancelFunc
	deltaSubMu             sync.Mutex
	activeSub              *turnSubscriber
	replayedDeltas         []DeltaChunk
	replayBytes            int
	replayCapped           bool
	softDetachCount        int
	firstChunk             time.Time
	firstThoughtAt         time.Time
	lastThoughtAt          time.Time
	firstTextAt            time.Time
	textChunks             []string
	thoughtChks            []string
	noticeHold             string // text that may still be (part of) an agy status line
	noticeSettled          bool   // model content was seen; unmarked text is no longer a notice candidate
	noticeText             string // last agy terminal notice seen in this turn (canonical text)
	noticeFailed           bool   // a terminal notice was not followed by model content (agy did not recover)
	noticeInfo             int    // dropped retry/info status lines
	apiRetryCount          int    // count of API error (attempt N) lines dropped in this turn
	lastRetryAttempt       int    // attempt number from the latest dropped API error status line
	lastRetryDetail        string // redacted detail from the latest dropped API error status line
	startAllStepIdx        int    // max steps.idx before this turn started (-1 if DB absent/empty)
	dbRetryCount           int    // count of hidden step_type=17 retry steps observed from <sid>.db
	dbLastRetryAttempt     int    // max attempt N observed from <sid>.db step_type=17 rows
	dbLastRetryDetail      string // redacted detail from latest hidden <sid>.db step_type=17 retry row
	dbLastTerminalDetail   string // redacted detail from latest visible (ShouldShowUser) step_type=17 row
	dbSeenErrorStepIdx     int    // highest step_type=17 idx already processed by poller/backfill
	dbLoggedRetryCount     int    // silent DB retry count already logged via RETRY_NOTICE_FILTERED
	onRetryNotice          func(attempt int, detail string)
	fastCancelledForBisect bool
	fastCancelDetail       string
	midLine                bool // the streamed text currently ends inside a line
	emittedText            bool // some model text was accepted
	toolCalls              []ToolCallItem
	batchTimer             *time.Timer
	resolved               bool
	failed                 bool
	upstreamDone           bool
	resultConsumed         bool
	deltaEnabled           bool
	deltaBuf               []DeltaChunk
	deltaBufBytes          int
	deltaWake              chan struct{}
	deltaClosed            bool
	deltaDone              chan struct{}
	emittedDeltas          atomic.Int64
	bgDone                 chan struct{}
	resolveChan            chan TurnResult
	errChan                chan error
	releaseOnce            sync.Once
	releaseSem             func()
}

func newActiveTurnState(onDelta func(DeltaChunk), releaseSem func()) *activeTurnState {
	return newActiveTurnStateWithCodec(onDelta, releaseSem, nil)
}

func (t *activeTurnState) startSubscriberLocked(cb func(DeltaChunk), initial []DeltaChunk, initialBytes int) *turnSubscriber {
	if cb == nil {
		return nil
	}
	q := make([]DeltaChunk, len(initial))
	copy(q, initial)
	sub := &turnSubscriber{
		cb:         cb,
		queue:      q,
		queueBytes: initialBytes,
		wake:       make(chan struct{}, 1),
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
	}
	if len(q) > 0 {
		sub.wake <- struct{}{}
	}
	t.activeSub = sub
	go t.runSubscriber(sub)
	return sub
}

func (t *activeTurnState) runSubscriber(sub *turnSubscriber) {
	defer close(sub.done)
	for {
		t.deltaSubMu.Lock()
		if t.activeSub != sub {
			t.deltaSubMu.Unlock()
			return
		}
		if len(sub.queue) == 0 {
			if sub.draining {
				t.activeSub = nil
				t.deltaSubMu.Unlock()
				return
			}
			t.deltaSubMu.Unlock()
			select {
			case <-sub.wake:
				continue
			case <-sub.stop:
				return
			}
		}
		chunk := sub.queue[0]
		sub.queue[0] = DeltaChunk{}
		sub.queue = sub.queue[1:]
		if len(sub.queue) == 0 {
			sub.queue = sub.queue[:0]
			sub.queueBytes = 0
		} else {
			sub.queueBytes -= len(chunk.Text) + len(chunk.Thinking)
			if sub.queueBytes < 0 {
				sub.queueBytes = 0
			}
		}
		t.deltaSubMu.Unlock()

		// Deliver outside deltaSubMu and s.mu so a slow/stalled TCP Flush never blocks
		// readStdoutLoop, DetachSubscriber, or singleflight AttachSubscriber.
		sub.cb(chunk)
	}
}

func (t *activeTurnState) emitSubscriberDelta(c DeltaChunk) {
	if t == nil {
		return
	}
	t.deltaSubMu.Lock()
	if !t.replayCapped {
		var ok bool
		t.replayedDeltas, t.replayBytes, ok = appendOrCoalesceDelta(t.replayedDeltas, t.replayBytes, c, maxTurnDeltaCoalesceBytes)
		if !ok {
			t.replayCapped = true
			t.replayedDeltas = nil
			t.replayBytes = 0
		}
	}
	if sub := t.activeSub; sub != nil {
		var ok bool
		sub.queue, sub.queueBytes, ok = appendOrCoalesceDelta(sub.queue, sub.queueBytes, c, maxTurnDeltaCoalesceBytes)
		if ok {
			select {
			case sub.wake <- struct{}{}:
			default:
			}
		}
	}
	t.deltaSubMu.Unlock()
}

func (t *activeTurnState) computeThinkingMsLocked() int {
	if t == nil || t.firstThoughtAt.IsZero() {
		return 0
	}
	endAt := t.lastThoughtAt
	if !t.firstTextAt.IsZero() && t.firstTextAt.After(t.firstThoughtAt) {
		endAt = t.firstTextAt
	}
	if endAt.Before(t.firstThoughtAt) {
		return 0
	}
	ms := int(endAt.Sub(t.firstThoughtAt).Milliseconds())
	if ms < 0 {
		return 0
	}
	return ms
}

func (t *activeTurnState) setStartAllStepIdx(idx int) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.startAllStepIdx = idx
	if idx > t.dbSeenErrorStepIdx {
		t.dbSeenErrorStepIdx = idx
	}
	t.mu.Unlock()
}

func (t *activeTurnState) retryCountLocked() int {
	if t == nil {
		return 0
	}
	n := t.apiRetryCount
	if t.lastRetryAttempt > n {
		n = t.lastRetryAttempt
	}
	if t.dbRetryCount > n {
		n = t.dbRetryCount
	}
	if t.dbLastRetryAttempt > n {
		n = t.dbLastRetryAttempt
	}
	return n
}

func firstNonEmptyString(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func isGenericTerminalNoticeText(detail, notice string) bool {
	d := strings.TrimSpace(strings.TrimSuffix(detail, "."))
	if d == "" {
		return true
	}
	if notice != "" && strings.EqualFold(d, strings.TrimSpace(strings.TrimSuffix(notice, "."))) {
		return true
	}
	for _, n := range agyTerminalNotices {
		if strings.EqualFold(d, strings.TrimSpace(strings.TrimSuffix(n, "."))) {
			return true
		}
	}
	return false
}

// applyDBErrorStepsLocked merges newly read step_type=17 rows from <sid>.db into turn state
// idempotently. Returns the highest newly observed hidden retry attempt and its sanitized detail.
func (t *activeTurnState) applyDBErrorStepsLocked(steps []ParsedErrorStep) (newHidden bool, maxAttempt int, latestDetail string) {
	if t == nil || len(steps) == 0 {
		return false, 0, ""
	}
	for _, st := range steps {
		if st.StepIdx <= t.dbSeenErrorStepIdx {
			continue
		}
		t.dbSeenErrorStepIdx = st.StepIdx
		if !st.ShouldShowUser {
			att, det, ok := parseRetryErrorText(st.ShortError)
			if !ok {
				att, det, ok = parseRetryErrorText(st.UserErrorMessage)
			}
			if !ok {
				det = sanitizeNoticeDetail(firstNonEmptyString(st.ShortError, st.UserErrorMessage, st.ModelErrorMessage))
				if det == "" && st.ErrorCode > 0 {
					det = fmt.Sprintf("HTTP %d", st.ErrorCode)
				}
			}
			t.dbRetryCount++
			if att > t.dbLastRetryAttempt {
				t.dbLastRetryAttempt = att
			}
			effAtt := att
			if effAtt <= 0 {
				effAtt = t.dbRetryCount
			}
			if effAtt > maxAttempt {
				maxAttempt = effAtt
			}
			if det != "" {
				t.dbLastRetryDetail = det
				latestDetail = det
			}
			newHidden = true
		} else {
			raw := firstNonEmptyString(st.ShortError, st.UserErrorMessage, st.ModelErrorMessage)
			if isGenericTerminalNoticeText(raw, "") {
				raw = firstNonEmptyString(st.ModelErrorMessage, st.UserErrorMessage, st.ShortError)
			}
			det := sanitizeNoticeDetail(raw)
			if det == "" && st.ErrorCode > 0 {
				det = fmt.Sprintf("HTTP %d", st.ErrorCode)
			}
			if det != "" && !isGenericTerminalNoticeText(det, "") {
				t.dbLastTerminalDetail = det
			}
		}
	}
	if latestDetail == "" {
		latestDetail = t.dbLastRetryDetail
	}
	return newHidden, maxAttempt, latestDetail
}

func (t *activeTurnState) effectiveTerminalDetailLocked() string {
	if t == nil {
		return ""
	}
	if t.lastRetryDetail != "" {
		return t.lastRetryDetail
	}
	if !t.noticeFailed {
		return ""
	}
	if t.dbLastTerminalDetail != "" && !isGenericTerminalNoticeText(t.dbLastTerminalDetail, t.noticeText) {
		return t.dbLastTerminalDetail
	}
	if t.dbLastRetryDetail != "" && !isGenericTerminalNoticeText(t.dbLastRetryDetail, t.noticeText) {
		return t.dbLastRetryDetail
	}
	return ""
}

func (t *activeTurnState) detachSubscriberInternal(countAsSoftDetach bool) {
	if t == nil {
		return
	}
	t.deltaSubMu.Lock()
	sub := t.activeSub
	t.activeSub = nil
	if countAsSoftDetach {
		t.softDetachCount++
	}
	t.deltaSubMu.Unlock()
	if sub != nil {
		sub.signalStop()
	}
}

// DetachSubscriber pauses chunk delivery to the departing HTTP subscriber while
// keeping replayedDeltas intact for a singleflight retry. Never call with s.mu held.
func (t *activeTurnState) DetachSubscriber() {
	t.detachSubscriberInternal(true)
}

// CleanupSubscriber stops any active subscriber goroutine without counting as a soft detach.
func (t *activeTurnState) CleanupSubscriber() {
	t.detachSubscriberInternal(false)
}

// DrainAndDetachSubscriber flushes all queued restored chunks to the active subscriber
// before normal turn return so the HTTP handler can safely write terminal SSE frames.
func (t *activeTurnState) DrainAndDetachSubscriber(ctx context.Context) {
	if t == nil {
		return
	}
	t.deltaSubMu.Lock()
	sub := t.activeSub
	if sub == nil {
		t.deltaSubMu.Unlock()
		return
	}
	sub.draining = true
	select {
	case sub.wake <- struct{}{}:
	default:
	}
	t.deltaSubMu.Unlock()

	if ctx == nil {
		<-sub.done
		return
	}
	select {
	case <-sub.done:
	case <-ctx.Done():
		t.CleanupSubscriber()
	}
}

// AttachSubscriber queues any buffered restored chunks in order onto a fresh async
// subscriber without holding deltaSubMu across network Flush() calls.
func (t *activeTurnState) AttachSubscriber(cb func(DeltaChunk)) {
	if t == nil {
		return
	}
	t.deltaSubMu.Lock()
	prev := t.activeSub
	t.activeSub = nil
	if cb != nil {
		t.startSubscriberLocked(cb, t.replayedDeltas, t.replayBytes)
	}
	t.deltaSubMu.Unlock()
	if prev != nil {
		prev.signalStop()
	}
}

func (t *activeTurnState) SoftDetachCount() int {
	if t == nil {
		return 0
	}
	t.deltaSubMu.Lock()
	n := t.softDetachCount
	t.deltaSubMu.Unlock()
	return n
}

func (t *activeTurnState) HasDeltaChannel() bool {
	return t != nil && t.deltaEnabled
}

func (t *activeTurnState) IsResolved() bool {
	if t == nil {
		return true
	}
	t.mu.Lock()
	done := t.resolved
	t.mu.Unlock()
	return done
}

func (t *activeTurnState) CanSoftDetach() bool {
	if t == nil {
		return false
	}
	t.mu.Lock()
	ok := !t.resultConsumed && !t.failed && !t.fastCancelledForBisect
	t.mu.Unlock()
	return ok
}

func (t *activeTurnState) MarkResultConsumed() {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.resultConsumed = true
	t.mu.Unlock()
}

func (t *activeTurnState) CancelContext() {
	if t != nil && t.cancelTurn != nil {
		t.cancelTurn()
	}
}

func newActiveTurnStateWithCodec(onDelta func(DeltaChunk), releaseSem func(), codec *ReversibleSessionCodec) *activeTurnState {
	turnCtx, cancelTurn := context.WithCancel(context.Background())
	t := &activeTurnState{
		createdAt:          time.Now(),
		turnCtx:            turnCtx,
		cancelTurn:         cancelTurn,
		startAllStepIdx:    -1,
		dbSeenErrorStepIdx: -1,
		bgDone:             make(chan struct{}),
		resolveChan:        make(chan TurnResult, 1),
		errChan:            make(chan error, 1),
		releaseSem:         releaseSem,
	}
	if onDelta != nil {
		t.deltaEnabled = true
		t.deltaWake = make(chan struct{}, 1)
		t.deltaDone = make(chan struct{})
		t.deltaSubMu.Lock()
		t.startSubscriberLocked(onDelta, nil, 0)
		t.deltaSubMu.Unlock()
		cb := func(c DeltaChunk) {
			t.emitSubscriberDelta(c)
		}
		go func(done chan<- struct{}) {
			defer close(done)
			textRestorer := codec.NewStreamRestorer()
			thinkRestorer := codec.NewStreamRestorer()
			for {
				t.mu.Lock()
				for len(t.deltaBuf) == 0 && !t.deltaClosed {
					wake := t.deltaWake
					t.mu.Unlock()
					<-wake
					t.mu.Lock()
				}
				if len(t.deltaBuf) == 0 && t.deltaClosed {
					t.mu.Unlock()
					break
				}
				batch := t.deltaBuf
				t.deltaBuf = nil
				t.deltaBufBytes = 0
				t.mu.Unlock()

				for _, chunk := range batch {
					switch chunk.Type {
					case "text":
						if tailThink := thinkRestorer.Flush(); tailThink != "" {
							t.emittedDeltas.Add(1)
							cb(DeltaChunk{Type: "thinking", Thinking: tailThink})
						}
						chunk.Text = textRestorer.Push(chunk.Text)
						if chunk.Text == "" {
							continue
						}
					case "thinking":
						if tailText := textRestorer.Flush(); tailText != "" {
							t.emittedDeltas.Add(1)
							cb(DeltaChunk{Type: "text", Text: tailText})
						}
						chunk.Thinking = thinkRestorer.Push(chunk.Thinking)
						if chunk.Thinking == "" {
							continue
						}
					}
					t.emittedDeltas.Add(1)
					cb(chunk)
				}
			}
			if tailThink := thinkRestorer.Flush(); tailThink != "" {
				t.emittedDeltas.Add(1)
				cb(DeltaChunk{Type: "thinking", Thinking: tailThink})
			}
			if tailText := textRestorer.Flush(); tailText != "" {
				t.emittedDeltas.Add(1)
				cb(DeltaChunk{Type: "text", Text: tailText})
			}
		}(t.deltaDone)
	}
	return t
}

func (t *activeTurnState) pushDelta(chunk DeltaChunk) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pushDeltaLocked(chunk)
}

func (t *activeTurnState) pushDeltaLocked(chunk DeltaChunk) {
	if !t.deltaEnabled || t.deltaClosed || t.resolved {
		return
	}
	var ok bool
	t.deltaBuf, t.deltaBufBytes, ok = appendOrCoalesceDelta(t.deltaBuf, t.deltaBufBytes, chunk, maxTurnDeltaCoalesceBytes)
	if !ok {
		return
	}
	select {
	case t.deltaWake <- struct{}{}:
	default:
	}
}

func (t *activeTurnState) closeDelta() {
	t.mu.Lock()
	done := t.deltaDone
	if t.deltaEnabled && !t.deltaClosed {
		t.deltaClosed = true
		select {
		case t.deltaWake <- struct{}{}:
		default:
		}
	}
	t.mu.Unlock()
	if done != nil {
		<-done
	}
}
