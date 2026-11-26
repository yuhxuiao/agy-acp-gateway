package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

var (
	fakeSegTokRe = regexp.MustCompile(`wsseg_[0-9a-f]{8}`)
	fakeTagTokRe = regexp.MustCompile(`wstag_[0-9a-f]{8}`)
)

type rpcMsg struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  any             `json:"result,omitempty"`
	Error   any             `json:"error,omitempty"`
}

type sessionInfo struct {
	ID              string
	Cwd             string
	Model           string
	Tier            string
	McpURL          string
	McpAuth         string
	McpTools        []string
	SysAppend       string
	TurnCount       int
	ResumeCount     int
	RelayFollows    int
	AlwaysMaxTokens bool
	CancelChan      chan struct{}
}

var (
	pendingMu        sync.Mutex
	pendingReplies   = map[string]chan json.RawMessage{}
	outMu            sync.Mutex
	sessions         = map[string]*sessionInfo{}
	sessMu           sync.Mutex
	seq              int
	initMu           sync.Mutex
	initClientName   string
	initClientVer    string
	rpc32603Mu       sync.Mutex
	rpc32603Counts   = map[string]int{}
	fake32603OnceRe  = regexp.MustCompile(`\[FAKE_RPC_32603_ONCE(?::([A-Za-z0-9_.-]+))?\]`)
	fake32603TwiceRe = regexp.MustCompile(`\[FAKE_RPC_32603_TWICE(?::([A-Za-z0-9_.-]+))?\]`)
	fakeImagePathsRe = regexp.MustCompile(`(?m)^-\s*ImagePaths:\s*(\[[^\r\n]+\])`)
	fakeMinimalPNG   = []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n', 0, 0, 0, 13, 'I', 'H', 'D', 'R', 0, 0, 0, 1, 0, 0, 0, 1, 8, 6, 0, 0, 0, 0x1f, 0x15, 0xc4, 0x89, 0, 0, 0, 10, 'I', 'D', 'A', 'T', 0x78, 0x9c, 0x63, 0, 1, 0, 0, 5, 0, 1, 0x0d, 0x0a, 0x2d, 0xb4, 0, 0, 0, 0, 'I', 'E', 'N', 'D', 0xae, 0x42, 0x60, 0x82}
)

func parseSpawnAgentName(args []string) string {
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--agent" && i+1 < len(args) {
			return strings.TrimSpace(args[i+1])
		}
		if strings.HasPrefix(a, "--agent=") {
			return strings.TrimSpace(strings.TrimPrefix(a, "--agent="))
		}
	}
	return ""
}

func handleFakeImageGen(reqID any, sess *sessionInfo, txt string, tNum int, cCh <-chan struct{}) {
	tcID := fmt.Sprintf("img_tc_%d_%d", tNum, time.Now().UnixNano())
	sendUpdate(sess.ID, map[string]any{
		"sessionUpdate": "tool_call",
		"toolCallId":    tcID,
		"name":          "generate_image",
		"kind":          "other",
		"status":        "in_progress",
	})
	if strings.Contains(txt, "[FAKE_IMAGEGEN_SLOW_150MS]") {
		if !sleepCancellable(150*time.Millisecond, cCh) {
			send(rpcMsg{JSONRPC: "2.0", ID: reqID, Result: map[string]any{"stopReason": "cancelled"}})
			return
		}
	}

	if strings.Contains(txt, "[FAKE_IMAGEGEN_QUOTA]") {
		sendUpdate(sess.ID, map[string]any{
			"sessionUpdate": "tool_call_update",
			"toolCallId":    tcID,
			"status":        "failed",
			"content": []map[string]any{
				{"type": "content", "content": map[string]any{"type": "text", "text": "image generation quota exceeded, try again later: ResourceExhausted"}},
			},
		})
		send(rpcMsg{JSONRPC: "2.0", ID: reqID, Result: map[string]any{"stopReason": "end_turn"}})
		return
	}
	if strings.Contains(txt, "[FAKE_IMAGEGEN_SAFETY]") {
		sendUpdate(sess.ID, map[string]any{
			"sessionUpdate": "tool_call_update",
			"toolCallId":    tcID,
			"status":        "failed",
			"content": []map[string]any{
				{"type": "content", "content": map[string]any{"type": "text", "text": "Blocked by safety content policy: cannot generate image"}},
			},
		})
		send(rpcMsg{JSONRPC: "2.0", ID: reqID, Result: map[string]any{"stopReason": "end_turn"}})
		return
	}
	if strings.Contains(txt, "[FAKE_IMAGEGEN_TRAVERSAL]") {
		geminiDir, appDataDir := parseSpawnDirs(os.Args[1:])
		if geminiDir != "" && !isUnsafeFakePath(geminiDir) {
			brainDir := filepath.Join(geminiDir, appDataDir, "brain", sess.ID)
			_ = os.MkdirAll(brainDir, 0700)
		}
		sendUpdate(sess.ID, map[string]any{
			"sessionUpdate": "tool_call_update",
			"toolCallId":    tcID,
			"status":        "completed",
			"content": []map[string]any{
				{"type": "content", "content": map[string]any{"type": "text", "text": "Generated image is saved at /etc/passwd."}},
			},
		})
		send(rpcMsg{JSONRPC: "2.0", ID: reqID, Result: map[string]any{"stopReason": "end_turn"}})
		return
	}

	// Validate staged ImagePaths if present (for /v1/images/edits).
	if m := fakeImagePathsRe.FindStringSubmatch(txt); len(m) == 2 {
		var paths []string
		if err := json.Unmarshal([]byte(m[1]), &paths); err != nil || len(paths) == 0 {
			sendUpdate(sess.ID, map[string]any{
				"sessionUpdate": "tool_call_update",
				"toolCallId":    tcID,
				"status":        "failed",
				"content":       "invalid ImagePaths JSON in prompt",
			})
			send(rpcMsg{JSONRPC: "2.0", ID: reqID, Result: map[string]any{"stopReason": "end_turn"}})
			return
		}
		for _, p := range paths {
			raw, err := os.ReadFile(p)
			if err != nil || len(raw) < 8 {
				sendUpdate(sess.ID, map[string]any{
					"sessionUpdate": "tool_call_update",
					"toolCallId":    tcID,
					"status":        "failed",
					"content":       fmt.Sprintf("input image unreadable: %s", p),
				})
				send(rpcMsg{JSONRPC: "2.0", ID: reqID, Result: map[string]any{"stopReason": "end_turn"}})
				return
			}
		}
	}

	geminiDir, appDataDir := parseSpawnDirs(os.Args[1:])
	if geminiDir == "" || isUnsafeFakePath(geminiDir) {
		sendUpdate(sess.ID, map[string]any{
			"sessionUpdate": "tool_call_update",
			"toolCallId":    tcID,
			"status":        "failed",
			"content":       "gemini-dir unavailable for image output",
		})
		send(rpcMsg{JSONRPC: "2.0", ID: reqID, Result: map[string]any{"stopReason": "end_turn"}})
		return
	}
	if appDataDir == "" {
		appDataDir = "antigravity-cli"
	}
	brainDir := filepath.Join(geminiDir, appDataDir, "brain", sess.ID)
	_ = os.MkdirAll(brainDir, 0700)

	if strings.Contains(txt, "[FAKE_IMAGEGEN_SYMLINK_ESCAPE]") {
		outsideFile := filepath.Join(geminiDir, "outside_secret.png")
		_ = os.WriteFile(outsideFile, fakeMinimalPNG, 0600)
		symPath := filepath.Join(brainDir, "escape.png")
		_ = os.Symlink(outsideFile, symPath)
		sendUpdate(sess.ID, map[string]any{
			"sessionUpdate": "tool_call_update",
			"toolCallId":    tcID,
			"status":        "completed",
			"content": []map[string]any{
				{"type": "content", "content": map[string]any{"type": "text", "text": "Generated image is saved at " + symPath + "."}},
			},
		})
		send(rpcMsg{JSONRPC: "2.0", ID: reqID, Result: map[string]any{"stopReason": "end_turn"}})
		return
	}

	outPath := filepath.Join(brainDir, fmt.Sprintf("generated_%d_%d.png", tNum, time.Now().UnixMilli()))
	_ = os.WriteFile(outPath, fakeMinimalPNG, 0600)
	if strings.Contains(txt, "[FAKE_IMAGEGEN_SIDECAR]") {
		_ = os.WriteFile(filepath.Join(brainDir, "sidecar.meta.json"), []byte(`{"ok":true}`), 0600)
	}
	if strings.Contains(txt, "[FAKE_IMAGEGEN_CANCEL_AFTER_WRITE]") {
		select {
		case <-cCh:
			send(rpcMsg{JSONRPC: "2.0", ID: reqID, Result: map[string]any{"stopReason": "cancelled"}})
			return
		case <-time.After(3 * time.Second):
		}
	}

	updateText := "Generated image is saved at " + outPath + "."
	if strings.Contains(txt, "[FAKE_IMAGEGEN_FALLBACK_SCAN]") {
		updateText = "Image generated successfully."
	}
	sendUpdate(sess.ID, map[string]any{
		"sessionUpdate": "tool_call_update",
		"toolCallId":    tcID,
		"status":        "completed",
		"content": []map[string]any{
			{"type": "content", "content": map[string]any{"type": "text", "text": updateText}},
		},
	})
	sendUpdate(sess.ID, map[string]any{
		"sessionUpdate": "agent_message_chunk",
		"content":       map[string]any{"type": "text", "text": "Done."},
	})
	send(rpcMsg{
		JSONRPC: "2.0",
		ID:      reqID,
		Result: map[string]any{
			"stopReason": "end_turn",
			"usage":      map[string]any{"inputTokens": 64, "outputTokens": 12, "cachedReadTokens": 0},
		},
	})
}

func send(v any) {
	b, _ := json.Marshal(v)
	outMu.Lock()
	os.Stdout.Write(append(b, '\n'))
	outMu.Unlock()
}

func sendUpdate(sid string, upd map[string]any) {
	send(rpcMsg{
		JSONRPC: "2.0",
		Method:  "session/update",
		Params: mustRaw(map[string]any{
			"sessionId": sid,
			"update":    upd,
		}),
	})
}

func mustRaw(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

func callMcp(s *sessionInfo, method string, params any) map[string]any {
	if s.McpURL == "" {
		return nil
	}
	body, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  method,
		"params":  params,
	})
	req, err := http.NewRequest("POST", s.McpURL, bytes.NewReader(body))
	if err != nil {
		return nil
	}
	req.Header.Set("Content-Type", "application/json")
	if s.McpAuth != "" {
		req.Header.Set("Authorization", s.McpAuth)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	lines := strings.Split(string(raw), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if strings.HasPrefix(line, "data:") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		}
		if strings.HasPrefix(line, "{") {
			var parsed map[string]any
			if json.Unmarshal([]byte(line), &parsed) == nil {
				if r, ok := parsed["result"].(map[string]any); ok {
					return r
				}
			}
		}
	}
	return nil
}

// mcpErrorCode sends a request and returns its JSON-RPC error code (0 when it succeeded).
func mcpErrorCode(s *sessionInfo, method string, params any) int {
	body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	req, err := http.NewRequest("POST", s.McpURL, bytes.NewReader(body))
	if err != nil {
		return -1
	}
	req.Header.Set("Content-Type", "application/json")
	if s.McpAuth != "" {
		req.Header.Set("Authorization", s.McpAuth)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	var parsed struct {
		Error *struct {
			Code int `json:"code"`
		} `json:"error"`
	}
	if json.NewDecoder(resp.Body).Decode(&parsed) != nil {
		return -1
	}
	if parsed.Error == nil {
		return 0
	}
	return parsed.Error.Code
}

func extractMcpInfo(s *sessionInfo, params map[string]any) {
	if cwd, ok := params["cwd"].(string); ok && cwd != "" {
		s.Cwd = cwd
	}
	if meta, ok := params["_meta"].(map[string]any); ok {
		if ag, ok := meta["antigravity"].(map[string]any); ok {
			if sp, ok := ag["systemPrompt"].(map[string]any); ok {
				if ap, ok := sp["append"].(string); ok {
					s.SysAppend = ap
				}
			}
		}
	}
	if srvs, ok := params["mcpServers"].([]any); ok && len(srvs) > 0 {
		if srv, ok := srvs[0].(map[string]any); ok {
			s.McpURL, _ = srv["url"].(string)
			if hdrs, ok := srv["headers"].([]any); ok {
				for _, h := range hdrs {
					if hm, ok := h.(map[string]any); ok {
						if hm["name"] == "Authorization" {
							s.McpAuth, _ = hm["value"].(string)
						}
					}
				}
			}
		}
	}
	if s.McpURL != "" {
		// agy 1.5.5 MCP client (probe P5): try server/discover (protocol 2026-07-28) first and fall
		// back to initialize only on -32601; with any other answer agy took a path in which the
		// bridge tools never reached the model. Mirror that so a gateway change is caught here.
		if code := mcpErrorCode(s, "server/discover", map[string]any{"protocolVersion": "2026-07-28"}); code != -32601 {
			s.McpTools = nil
			return
		}
		_ = callMcp(s, "initialize", map[string]any{"protocolVersion": "2025-11-25"})
		res := callMcp(s, "tools/list", map[string]any{})
		if tools, ok := res["tools"].([]any); ok {
			s.McpTools = nil
			for _, t := range tools {
				if tm, ok := t.(map[string]any); ok {
					if n, ok := tm["name"].(string); ok {
						s.McpTools = append(s.McpTools, n)
					}
				}
			}
		}
	}
}

func findMcpTool(s *sessionInfo, hostName string) string {
	prefixes := []string{"ws_" + hostName + "_", "dsh_" + hostName + "_"}
	switch hostName {
	case "ws_await_result", "dsh_await_result":
		prefixes = []string{"ws_ws_await_result_", "dsh_dsh_await_result_"}
	case "ws_call_tool", "dsh_call_tool":
		prefixes = []string{"ws_ws_call_tool_", "dsh_dsh_call_tool_"}
	}
	for _, p := range prefixes {
		for _, n := range s.McpTools {
			if strings.HasPrefix(n, p) {
				return n
			}
		}
	}
	if len(s.McpTools) > 0 {
		return s.McpTools[0]
	}
	return "ws_" + hostName + "_000000000000"
}

func askPermission(sid, permID string, toolCall map[string]any) string {
	waitCh := make(chan json.RawMessage, 1)
	pendingMu.Lock()
	pendingReplies[permID] = waitCh
	pendingMu.Unlock()
	permParams, _ := json.Marshal(map[string]any{
		"sessionId": sid,
		"options": []map[string]any{
			{"optionId": "opt_allow_always", "kind": "allow_always"},
			{"optionId": "opt_allow_once", "kind": "allow_once"},
			{"optionId": "opt_reject_always", "kind": "reject_always"},
			{"optionId": "opt_reject_once", "kind": "reject_once"},
		},
		"toolCall": toolCall,
	})
	send(rpcMsg{JSONRPC: "2.0", ID: permID, Method: "session/request_permission", Params: permParams})
	select {
	case raw := <-waitCh:
		var parsed struct {
			Result struct {
				Outcome struct {
					Outcome  string `json:"outcome"`
					OptionID string `json:"optionId"`
				} `json:"outcome"`
			} `json:"result"`
		}
		if json.Unmarshal(raw, &parsed) == nil {
			return parsed.Result.Outcome.Outcome + ":" + parsed.Result.Outcome.OptionID
		}
		return string(raw)
	case <-time.After(3 * time.Second):
		return "TIMEOUT"
	}
}

func sessModelAndTier(s *sessionInfo) (string, string) {
	if s == nil {
		return "gemini-4-carbon", "medium"
	}
	sessMu.Lock()
	m, t := s.Model, s.Tier
	sessMu.Unlock()
	if m == "" {
		m = "gemini-4-carbon"
	}
	if t == "" {
		t = "medium"
	}
	return m, t
}

func main() {
	for _, a := range os.Args[1:] {
		if a == "--version" || a == "-v" {
			fmt.Println(fakeAcpVersion())
			return
		}
	}
	if isUsageQuotaInvocation(os.Args[1:]) {
		if fakeEnvBool("FAKE_ACP_QUOTA_ERROR", false) {
			os.Exit(3)
		}
		if b, err := renderFakeQuotaJSON(time.Now()); err == nil {
			fmt.Println(string(b))
		}
		return
	}
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 0, 1024*1024), 16*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var msg rpcMsg
		if err := json.Unmarshal(line, &msg); err != nil {
			continue
		}
		var params map[string]any
		_ = json.Unmarshal(msg.Params, &params)

		if msg.Method == "" && msg.ID != nil {
			// reply to an agent->client request sent by this fake
			pendingMu.Lock()
			ch := pendingReplies[fmt.Sprint(msg.ID)]
			delete(pendingReplies, fmt.Sprint(msg.ID))
			pendingMu.Unlock()
			if ch != nil {
				ch <- append(json.RawMessage(nil), line...)
			}
			continue
		}

		switch msg.Method {
		case "initialize":
			if ci, ok := params["clientInfo"].(map[string]any); ok {
				initMu.Lock()
				initClientName, _ = ci["name"].(string)
				initClientVer, _ = ci["version"].(string)
				initMu.Unlock()
			}
			if caps := os.Getenv("FAKE_ACP_CAPABILITIES"); caps != "" {
				var c map[string]any
				if json.Unmarshal([]byte(caps), &c) != nil {
					panic("invalid test capabilities")
				}
				send(rpcMsg{JSONRPC: "2.0", ID: msg.ID, Result: map[string]any{"protocolVersion": 1, "agentCapabilities": c}})
				continue
			}

			send(rpcMsg{
				JSONRPC: "2.0",
				ID:      msg.ID,
				Result: map[string]any{
					"protocolVersion": 1,
					"agentCapabilities": map[string]any{
						"loadSession": true,
						"sessionCapabilities": map[string]any{
							"resume": map[string]any{},
							"close":  map[string]any{},
							"load":   map[string]any{},
							"list":   map[string]any{},
						},
						"mcpCapabilities": map[string]any{"http": true},
					},
				},
			})

		case "session/new":
			sessMu.Lock()
			seq++
			sid := fmt.Sprintf("00000000-0000-4000-8000-%012d", seq)
			// the server-side default for new sessions is gemini-4-carbon (since 2026-11-10)
			s := &sessionInfo{ID: sid, Model: "gemini-4-carbon", Tier: "medium", CancelChan: make(chan struct{})}
			sessions[sid] = s
			sessMu.Unlock()
			extractMcpInfo(s, params)
			send(rpcMsg{
				JSONRPC: "2.0",
				ID:      msg.ID,
				Result:  map[string]any{"sessionId": sid},
			})

		case "session/resume", "session/load":
			sid, _ := params["sessionId"].(string)
			sessMu.Lock()
			s := sessions[sid]
			if s == nil {
				s = &sessionInfo{ID: sid, CancelChan: make(chan struct{})}
				sessions[sid] = s
			} else {
				s.ResumeCount++
				s.CancelChan = make(chan struct{})
			}
			// agy 1.5.5 resets model and tier to the server default on resume (probe P3)
			s.Model, s.Tier = "gemini-4-carbon", "medium"
			sessMu.Unlock()
			extractMcpInfo(s, params)
			if msg.Method == "session/load" {
				sendUpdate(sid, map[string]any{
					"sessionUpdate": "agent_message_chunk",
					"content":       map[string]any{"type": "text", "text": "REPLAYED_HISTORY_CHUNK"},
				})
			}
			send(rpcMsg{
				JSONRPC: "2.0",
				ID:      msg.ID,
				Result:  map[string]any{"sessionId": sid},
			})

		case "session/set_config_option":
			sid, _ := params["sessionId"].(string)
			cid, _ := params["configId"].(string)
			val, _ := params["value"].(string)
			if cid == "model" && val != "" && val == os.Getenv("FAKE_ACP_REJECT_MODEL") {
				send(rpcMsg{JSONRPC: "2.0", ID: msg.ID, Error: map[string]any{"code": -32603, "message": "model switch failed"}})
				continue
			}
			if cid == "model" && val != "" && val == os.Getenv("FAKE_ACP_EMPTY_ACK_MODEL") {
				// success RPC without a confirmation; the session keeps its previous model
				send(rpcMsg{JSONRPC: "2.0", ID: msg.ID, Result: map[string]any{}})
				continue
			}
			if cid == "thought_level" && val != "" && val == os.Getenv("FAKE_ACP_REJECT_TIER") {
				send(rpcMsg{JSONRPC: "2.0", ID: msg.ID, Error: map[string]any{"code": -32602, "message": "Thinking level " + val + " not offered for model"}})
				continue
			}
			sessMu.Lock()
			s := sessions[sid]
			curModel := ""
			if s != nil {
				curModel = s.Model
			}
			sessMu.Unlock()
			if (curModel == "gpt-oss-120b-medium" ||
				curModel == "gemini-pro-orchestrator" ||
				curModel == "gemini-pro-orchestrator-neon" ||
				curModel == "gemini-pro-orchestrator-argon") && cid == "thought_level" {
				send(rpcMsg{
					JSONRPC: "2.0",
					ID:      msg.ID,
					Error:   map[string]any{"code": -32602, "message": "Thinking level not offered for " + curModel},
				})
				continue
			}
			if cid == "thought_level" && val == "xhigh" && curModel != "gemini-4-hydrogen" && curModel != "gemini-4-neon" {
				send(rpcMsg{
					JSONRPC: "2.0",
					ID:      msg.ID,
					Error:   map[string]any{"code": -32602, "message": "Thinking level xhigh not offered for " + curModel},
				})
				continue
			}
			sessMu.Lock()
			if s != nil {
				if cid == "model" {
					s.Model = val
				} else if cid == "thought_level" {
					s.Tier = val
				}
			}
			sessMu.Unlock()
			send(rpcMsg{
				JSONRPC: "2.0",
				ID:      msg.ID,
				Result: map[string]any{
					"configOptions": []map[string]any{
						{"id": cid, "currentValue": val},
					},
				},
			})

		case "session/close":
			sid, _ := params["sessionId"].(string)
			sessMu.Lock()
			if s := sessions[sid]; s != nil {
				select {
				case <-s.CancelChan:
				default:
					close(s.CancelChan)
				}
			}
			sessMu.Unlock()
			if msg.ID != nil {
				send(rpcMsg{JSONRPC: "2.0", ID: msg.ID, Result: map[string]any{}})
			}

		case "session/cancel":
			sid, _ := params["sessionId"].(string)
			sessMu.Lock()
			if s := sessions[sid]; s != nil {
				select {
				case <-s.CancelChan:
				default:
					close(s.CancelChan)
				}
			}
			sessMu.Unlock()
			if msg.ID != nil {
				send(rpcMsg{JSONRPC: "2.0", ID: msg.ID, Result: map[string]any{}})
			}

		case "session/prompt":
			sid, _ := params["sessionId"].(string)
			sessMu.Lock()
			s := sessions[sid]
			if s == nil {
				s = &sessionInfo{ID: sid, Model: "gemini-4-argon", Tier: "medium", CancelChan: make(chan struct{})}
				sessions[sid] = s
			}
			s.TurnCount++
			turnNum := s.TurnCount
			cancelCh := s.CancelChan
			sessMu.Unlock()

			var promptText string
			if blocks, ok := params["prompt"].([]any); ok {
				for _, b := range blocks {
					if bm, ok := b.(map[string]any); ok {
						if t, ok := bm["text"].(string); ok {
							promptText += t + "\n"
						}
					}
				}
			}

			go func(reqID any, sess *sessionInfo, txt string, tNum int, cCh chan struct{}) {
				if parseSpawnAgentName(os.Args[1:]) == "ws-imagegen" || strings.Contains(txt, "[FAKE_IMAGEGEN") {
					handleFakeImageGen(reqID, sess, txt, tNum, cCh)
					return
				}
				if strings.Contains(txt, "[FAKE_RPC_ERROR]") {
					send(rpcMsg{
						JSONRPC: "2.0",
						ID:      reqID,
						Error:   map[string]any{"code": -32000, "message": "simulated upstream RPC failure"},
					})
					return
				}
				if strings.Contains(txt, "[FAKE_RPC_400]") {
					send(rpcMsg{
						JSONRPC: "2.0",
						ID:      reqID,
						Error:   map[string]any{"code": -32603, "message": "400 INVALID_ARGUMENT: thinking budget is not supported for this model"},
					})
					return
				}
				if strings.Contains(txt, "[FAKE_RPC_429]") {
					send(rpcMsg{
						JSONRPC: "2.0",
						ID:      reqID,
						Error:   map[string]any{"code": -32603, "message": "429 RESOURCE_EXHAUSTED: Individual quota reached for model"},
					})
					return
				}
				if strings.Contains(txt, "[FAKE_RPC_529]") {
					send(rpcMsg{
						JSONRPC: "2.0",
						ID:      reqID,
						Error:   map[string]any{"code": -32603, "message": "503 MODEL_CAPACITY_EXHAUSTED: server overloaded"},
					})
					return
				}
				if strings.Contains(txt, "[FAKE_RPC_32603_ALWAYS]") {
					send(rpcMsg{
						JSONRPC: "2.0",
						ID:      reqID,
						Error:   map[string]any{"code": -32603, "message": "503 MODEL_CAPACITY_EXHAUSTED: transient upstream rpc failure"},
					})
					return
				}
				if m := fake32603TwiceRe.FindStringSubmatch(txt); len(m) > 0 {
					key := "twice:" + sess.ID
					if m[1] != "" {
						key = "twice:" + m[1]
					}
					rpc32603Mu.Lock()
					seen := rpc32603Counts[key]
					shouldFail := seen < 2
					if shouldFail {
						rpc32603Counts[key] = seen + 1
					}
					rpc32603Mu.Unlock()
					if shouldFail {
						send(rpcMsg{
							JSONRPC: "2.0",
							ID:      reqID,
							Error:   map[string]any{"code": -32603, "message": "503 MODEL_CAPACITY_EXHAUSTED: transient upstream rpc failure"},
						})
						return
					}
				}
				if m := fake32603OnceRe.FindStringSubmatch(txt); len(m) > 0 {
					key := "once:" + sess.ID
					if m[1] != "" {
						key = "once:" + m[1]
					}
					rpc32603Mu.Lock()
					seen := rpc32603Counts[key]
					shouldFail := seen < 1
					if shouldFail {
						rpc32603Counts[key] = seen + 1
					}
					rpc32603Mu.Unlock()
					if shouldFail {
						send(rpcMsg{
							JSONRPC: "2.0",
							ID:      reqID,
							Error:   map[string]any{"code": -32603, "message": "503 MODEL_CAPACITY_EXHAUSTED: transient upstream rpc failure"},
						})
						return
					}
				}
				if strings.Contains(txt, "[FAKE_TOS_RPC_ERROR]") {
					send(rpcMsg{
						JSONRPC: "2.0",
						ID:      reqID,
						Error:   map[string]any{"code": 403, "message": "PERMISSION_DENIED: This service has been disabled in this account for violation of Terms of Service"},
					})
					return
				}
				if strings.Contains(txt, "[FAKE_TOS_SOFT_ERROR]") {
					sendUpdate(sess.ID, map[string]any{
						"sessionUpdate": "agent_message_chunk",
						"content":       map[string]any{"type": "text", "text": "This service has been disabled in this account for violation of Terms of Service."},
					})
					send(rpcMsg{
						JSONRPC: "2.0",
						ID:      reqID,
						Result: map[string]any{
							"stopReason": "end_turn",
							"usage":      map[string]any{"inputTokens": 100, "outputTokens": 12, "cachedReadTokens": 0},
						},
					})
					return
				}
				if strings.Contains(txt, "[FAKE_SLOW]") {
					select {
					case <-cCh:
						send(rpcMsg{
							JSONRPC: "2.0",
							ID:      reqID,
							Result: map[string]any{
								"stopReason": "cancelled",
								"usage":      map[string]any{"inputTokens": 500, "outputTokens": 5, "cachedReadTokens": 400},
							},
						})
						return
					case <-time.After(8 * time.Second):
					}
				}
				if strings.Contains(txt, "[FAKE_TTFT_300MS]") {
					select {
					case <-cCh:
						return
					case <-time.After(300 * time.Millisecond):
					}
				}
				if strings.Contains(txt, "[FAKE_SLOW_PREFILL_300MS]") {
					select {
					case <-cCh:
						send(rpcMsg{
							JSONRPC: "2.0",
							ID:      reqID,
							Result:  map[string]any{"stopReason": "cancelled"},
						})
						return
					case <-time.After(300 * time.Millisecond):
					}
				}
				if strings.Contains(txt, "[FAKE_THINK]") {
					sendUpdate(sess.ID, map[string]any{
						"sessionUpdate": "agent_thought_chunk",
						"content":       map[string]any{"type": "text", "text": "Thinking step 1..."},
					})
				}
				if strings.Contains(txt, "[FAKE_POST_THINK_SILENCE_300MS]") {
					select {
					case <-cCh:
						return
					case <-time.After(300 * time.Millisecond):
					}
				}
				if strings.Contains(txt, "[FAKE_TOOL_CALL]") && tNum == 1 {
					curModel, curTier := sessModelAndTier(sess)
					convDir := resolveFakeConvDir(os.Args[1:])
					mcpName := findMcpTool(sess, "Bash")
					bridgePrefix := "mcp_ws-bridge_"
					if strings.HasPrefix(mcpName, "dsh_") {
						bridgePrefix = "mcp_dsh-bridge_"
					}
					gen1 := resolveTurnTelemetry(curModel, curTier, txt, 240, 2048, 28, 0)
					if !strings.Contains(txt, "[FAKE_LATE_SECOND_TOOL]") {
						gen1.ToolCalls = []fakeToolProposal{{
							ID:          "call_1",
							RawToolName: bridgePrefix + mcpName,
							ArgsJSON:    `{"command":"echo FAKE_TOOL_OK"}`,
						}}
					}
					_ = appendSessionGeneration(convDir, sess.ID, gen1)
					emitUsageUpdate(sess.ID, curModel, gen1.InTok, gen1.CrTok)

					var wg sync.WaitGroup
					if strings.Contains(txt, "[FAKE_LATE_SECOND_TOOL]") {
						wg.Add(1)
						go func() {
							defer wg.Done()
							time.Sleep(180 * time.Millisecond)
							_ = callMcp(sess, "tools/call", map[string]any{
								"name":      mcpName,
								"arguments": map[string]any{"command": "echo LATE_TOOL_2"},
							})
						}()
					}
					res := callMcp(sess, "tools/call", map[string]any{
						"name":      mcpName,
						"arguments": map[string]any{"command": "echo FAKE_TOOL_OK"},
					})
					wg.Wait()
					var toolOut string
					extractText := func(r map[string]any) (string, bool) {
						if r == nil {
							return "", false
						}
						isErr, _ := r["isError"].(bool)
						if arr, ok := r["content"].([]any); ok && len(arr) > 0 {
							if cm, ok := arr[0].(map[string]any); ok {
								s, _ := cm["text"].(string)
								return s, isErr
							}
						}
						return "", isErr
					}
					var isErr bool
					toolOut, isErr = extractText(res)
					if strings.Contains(txt, "[FAKE_TOOL_CHAIN]") && !isErr {
						res2 := callMcp(sess, "tools/call", map[string]any{
							"name":      mcpName,
							"arguments": map[string]any{"command": "echo FAKE_TOOL_CHAIN_2"},
						})
						toolOut, isErr = extractText(res2)
					}
					if strings.Contains(txt, "[FAKE_FOLLOW_RELAYS]") {
						for hop := 1; hop <= 20 && !isErr && strings.Contains(toolOut, `"call_id":"`); hop++ {
							select {
							case <-cCh:
								send(rpcMsg{
									JSONRPC: "2.0",
									ID:      reqID,
									Result:  map[string]any{"stopReason": "cancelled"},
								})
								return
							default:
							}
							idx := strings.Index(toolOut, `"call_id":"`)
							rest := toolOut[idx+len(`"call_id":"`):]
							end := strings.IndexByte(rest, '"')
							if end <= 0 {
								break
							}
							cid := rest[:end]
							sessMu.Lock()
							sess.RelayFollows++
							sessMu.Unlock()
							relayGen := resolveTurnTelemetry(curModel, curTier, txt, 128, 2288+hop*64, 16, 0)
							_ = appendSessionGeneration(convDir, sess.ID, relayGen)
							if strings.Contains(txt, "[FAKE_ROGUE_NEW_TOOL]") && hop == 2 {
								res = callMcp(sess, "tools/call", map[string]any{
									"name":      mcpName,
									"arguments": map[string]any{"command": fmt.Sprintf("echo ROGUE_%d", hop)},
								})
							} else {
								res = callMcp(sess, "tools/call", map[string]any{
									"name":      findMcpTool(sess, "ws_await_result"),
									"arguments": map[string]any{"call_id": cid},
								})
							}
							toolOut, isErr = extractText(res)
						}
						if isErr && strings.Contains(toolOut, "hibernated") {
							send(rpcMsg{
								JSONRPC: "2.0",
								ID:      reqID,
								Result:  map[string]any{"stopReason": "cancelled"},
							})
							return
						}
					}
					if isErr && strings.Contains(strings.ToLower(toolOut), "cancelled") {
						send(rpcMsg{
							JSONRPC: "2.0",
							ID:      reqID,
							Result:  map[string]any{"stopReason": "cancelled"},
						})
						return
					}
					if strings.Contains(txt, "[FAKE_SLOW_RESUME_300MS]") {
						select {
						case <-cCh:
							send(rpcMsg{
								JSONRPC: "2.0",
								ID:      reqID,
								Result:  map[string]any{"stopReason": "cancelled"},
							})
							return
						case <-time.After(300 * time.Millisecond):
						}
					}
					finalReply := "TOOL_DONE:" + toolOut
					sendUpdate(sess.ID, map[string]any{
						"sessionUpdate": "agent_message_chunk",
						"content":       map[string]any{"type": "text", "text": finalReply},
					})
					gen2 := resolveTurnTelemetry(curModel, curTier, txt, 320, 4096, 24, 0)
					gen2.ReplyText = finalReply
					_ = appendSessionGeneration(convDir, sess.ID, gen2)
					emitUsageUpdate(sess.ID, curModel, gen2.InTok, gen2.CrTok)
					send(rpcMsg{
						JSONRPC: "2.0",
						ID:      reqID,
						Result: map[string]any{
							"stopReason": "end_turn",
							"usage": map[string]any{
								"inputTokens":      gen2.InTok,
								"outputTokens":     gen2.OutTok,
								"cachedReadTokens": gen2.CrTok,
							},
						},
					})
					return
				}
				if strings.Contains(txt, "[FAKE_MAX_TOKENS_ALWAYS]") {
					sessMu.Lock()
					sess.AlwaysMaxTokens = true
					sessMu.Unlock()
				}
				sessMu.Lock()
				alwaysMax := sess.AlwaysMaxTokens
				sessMu.Unlock()
				if alwaysMax {
					curModel, curTier := sessModelAndTier(sess)
					segText := fmt.Sprintf("SEG%d_", tNum)
					sendUpdate(sess.ID, map[string]any{
						"sessionUpdate": "agent_message_chunk",
						"content":       map[string]any{"type": "text", "text": segText},
					})
					maxRec := resolveTurnTelemetry(curModel, curTier, txt, 200, 1000, 10, 0)
					maxRec.ReplyText = segText
					_ = appendSessionGeneration(resolveFakeConvDir(os.Args[1:]), sess.ID, maxRec)
					emitUsageUpdate(sess.ID, curModel, maxRec.InTok, maxRec.CrTok)
					send(rpcMsg{
						JSONRPC: "2.0",
						ID:      reqID,
						Result: map[string]any{
							"stopReason": "max_tokens",
							"usage":      map[string]any{"inputTokens": maxRec.InTok, "outputTokens": maxRec.OutTok, "cachedReadTokens": maxRec.CrTok},
						},
					})
					return
				}
				if strings.Contains(txt, "[FAKE_PERM_NAMEONLY]") {
					mcpName := findMcpTool(sess, "Bash")
					bridgePrefix := "mcp_ws-bridge_"
					if strings.HasPrefix(mcpName, "dsh_") {
						bridgePrefix = "mcp_dsh-bridge_"
					}
					tcID := fmt.Sprintf("tc-nameonly-%d", tNum)
					sendUpdate(sess.ID, map[string]any{
						"sessionUpdate": "tool_call",
						"toolCallId":    tcID,
						"name":          bridgePrefix + mcpName,
						"kind":          "other",
					})
					verdict := askPermission(sess.ID, "perm-nameonly", map[string]any{
						"toolCallId": tcID,
						"title":      "Run host Bash",
						"kind":       "other",
						"rawInput":   map[string]any{"command": "echo hi"},
					})
					sendUpdate(sess.ID, map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]any{"type": "text", "text": "PERM_NAMEONLY=" + verdict}})
					send(rpcMsg{JSONRPC: "2.0", ID: reqID, Result: map[string]any{"stopReason": "end_turn", "usage": map[string]any{"inputTokens": 64, "outputTokens": 12}}})
					return
				}
				if strings.Contains(txt, "[FAKE_PERM_SPOOF]") {
					mcpName := findMcpTool(sess, "Bash")
					tcID := fmt.Sprintf("tc-spoof-%d", tNum)
					sendUpdate(sess.ID, map[string]any{
						"sessionUpdate": "tool_call",
						"toolCallId":    tcID,
						"name":          "run_command",
						"kind":          "execute",
					})
					verdict := askPermission(sess.ID, "perm-spoof", map[string]any{
						"toolCallId": tcID,
						"name":       "run_command",
						"kind":       "execute",
						"title":      "ws-bridge dsh-bridge search_web " + mcpName,
						"rawInput": map[string]any{
							"CommandLine": "echo ws-bridge dsh-bridge search_web read_url_content " + mcpName,
						},
					})
					sendUpdate(sess.ID, map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]any{"type": "text", "text": "PERM_SPOOF=" + verdict}})
					send(rpcMsg{JSONRPC: "2.0", ID: reqID, Result: map[string]any{"stopReason": "end_turn", "usage": map[string]any{"inputTokens": 64, "outputTokens": 12}}})
					return
				}
				if strings.Contains(txt, "[FAKE_MAX_TOKENS]") && tNum == 1 {
					curModel, curTier := sessModelAndTier(sess)
					sendUpdate(sess.ID, map[string]any{
						"sessionUpdate": "agent_message_chunk",
						"content":       map[string]any{"type": "text", "text": "PART1_"},
					})
					maxRec := resolveTurnTelemetry(curModel, curTier, txt, 200, 1000, 10, 0)
					maxRec.ReplyText = "PART1_"
					_ = appendSessionGeneration(resolveFakeConvDir(os.Args[1:]), sess.ID, maxRec)
					emitUsageUpdate(sess.ID, curModel, maxRec.InTok, maxRec.CrTok)
					send(rpcMsg{
						JSONRPC: "2.0",
						ID:      reqID,
						Result: map[string]any{
							"stopReason": "max_tokens",
							"usage":      map[string]any{"inputTokens": maxRec.InTok, "outputTokens": maxRec.OutTok, "cachedReadTokens": maxRec.CrTok},
						},
					})
					return
				}
				if strings.Contains(txt, "[FAKE_ELICIT]") {
					// agent->client request the gateway does not support; the turn must still finish
					elicitID := fmt.Sprintf("elicit-%d", tNum)
					waitCh := make(chan json.RawMessage, 1)
					pendingMu.Lock()
					pendingReplies[elicitID] = waitCh
					pendingMu.Unlock()
					elicitParams, _ := json.Marshal(map[string]any{"sessionId": sess.ID, "message": "Pick one"})
					send(rpcMsg{JSONRPC: "2.0", ID: elicitID, Method: "elicitation/create", Params: elicitParams})
					answer := "NO_REPLY"
					select {
					case raw := <-waitCh:
						if strings.Contains(string(raw), "-32601") {
							answer = "ELICIT_REJECTED"
						} else {
							answer = "ELICIT_OTHER"
						}
					case <-time.After(3 * time.Second):
					}
					sendUpdate(sess.ID, map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]any{"type": "text", "text": answer}})
					send(rpcMsg{JSONRPC: "2.0", ID: reqID, Result: map[string]any{"stopReason": "end_turn", "usage": map[string]any{"inputTokens": 10, "outputTokens": 2}}})
					return
				}
				if strings.Contains(txt, "[FAKE_LATE_NOTICE]") {
					// partial reply, then a terminal notice as its own chunk (R5)
					for _, c := range []string{"Partial answer before an upstream failure.\n", "\n⚠ There was a network issue connecting to the server, please try again.\n"} {
						sendUpdate(sess.ID, map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]any{"type": "text", "text": c}})
					}
					send(rpcMsg{JSONRPC: "2.0", ID: reqID, Result: map[string]any{"stopReason": "end_turn", "usage": map[string]any{"inputTokens": 100, "outputTokens": 8}}})
					return
				}
				if strings.Contains(txt, "[FAKE_NOTICE_INTERRUPTED]") {
					// real P6 text: agy reports the interruption and keeps working
					for _, c := range []string{"First part. ", "\n⚠ The stream was interrupted. Please continue the task you were working on.\n", "Second part after resume."} {
						sendUpdate(sess.ID, map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]any{"type": "text", "text": c}})
					}
					send(rpcMsg{JSONRPC: "2.0", ID: reqID, Result: map[string]any{"stopReason": "end_turn", "usage": map[string]any{"inputTokens": 100, "outputTokens": 8}}})
					return
				}
				if strings.Contains(txt, "[FAKE_HIGH_429_MEDIUM_OK]") {
					sessMu.Lock()
					tier := sess.Tier
					sessMu.Unlock()
					if tier == "high" || tier == "xhigh" {
						for _, c := range []string{"\n⚠ API error (attempt 1): RESOURCE_EXHAUSTED (code 429): Resource has been exhausted (e.g. check quota).\n", "\n⚠ RESOURCE_EXHAUSTED (code 429): Resource has been exhausted (e.g. check quota).\n"} {
							sendUpdate(sess.ID, map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]any{"type": "text", "text": c}})
						}
					} else {
						sendUpdate(sess.ID, map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]any{"type": "text", "text": "FALLBACK_MEDIUM_OK"}})
					}
					send(rpcMsg{JSONRPC: "2.0", ID: reqID, Result: map[string]any{"stopReason": "end_turn", "usage": map[string]any{"inputTokens": 100, "outputTokens": 8}}})
					return
				}
				if strings.Contains(txt, "[FAKE_MEDIUM_429_LOW_OK]") {
					sessMu.Lock()
					tier := sess.Tier
					sessMu.Unlock()
					if tier == "medium" || tier == "high" || tier == "xhigh" {
						for _, c := range []string{"\n⚠ API error (attempt 1): RESOURCE_EXHAUSTED (code 429): Resource has been exhausted (e.g. check quota).\n", "\n⚠ RESOURCE_EXHAUSTED (code 429): Resource has been exhausted (e.g. check quota).\n"} {
							sendUpdate(sess.ID, map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]any{"type": "text", "text": c}})
						}
					} else {
						sendUpdate(sess.ID, map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]any{"type": "text", "text": "FALLBACK_LOW_OK"}})
					}
					send(rpcMsg{JSONRPC: "2.0", ID: reqID, Result: map[string]any{"stopReason": "end_turn", "usage": map[string]any{"inputTokens": 100, "outputTokens": 8}}})
					return
				}
				if strings.Contains(txt, "[FAKE_AGY159_LATE_429]") {
					for _, c := range []string{"Partial answer before rate limit.\n", "\n⚠ RESOURCE_EXHAUSTED (code 429): ", "Resource has been exhausted (e.g. check quota).\n"} {
						sendUpdate(sess.ID, map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]any{"type": "text", "text": c}})
					}
					send(rpcMsg{JSONRPC: "2.0", ID: reqID, Result: map[string]any{"stopReason": "end_turn", "usage": map[string]any{"inputTokens": 100, "outputTokens": 8}}})
					return
				}
				global429 := false
				if sess.Cwd != "" {
					if _, stErr := os.Stat(filepath.Join(sess.Cwd, ".fake_global_429")); stErr == nil {
						global429 = true
					}
				}
				rawDshPathWaf := (strings.Contains(sess.SysAppend, "WAF_IF_RAW_DSH_PATH") || strings.Contains(txt, "WAF_IF_RAW_DSH_PATH")) &&
					(strings.Contains(sess.SysAppend, "DeepSeek Harness") || strings.Contains(txt, "DeepSeek Harness"))
				if global429 || rawDshPathWaf ||
					strings.Contains(sess.SysAppend, "TRIGGER_UNKNOWN_HARNESS_WAF_") ||
					strings.Contains(txt, "TRIGGER_UNKNOWN_HARNESS_WAF_") {
					sendUpdate(sess.ID, map[string]any{
						"sessionUpdate": "agent_message_chunk",
						"content":       map[string]any{"type": "text", "text": "\n⚠ API error (attempt 1): RESOURCE_EXHAUSTED (code 429): Resource has been exhausted (e.g. check quota).\n"},
					})
					select {
					case <-cCh:
						send(rpcMsg{JSONRPC: "2.0", ID: reqID, Result: map[string]any{"stopReason": "cancelled"}})
						return
					case <-time.After(35 * time.Millisecond):
					}
					sendUpdate(sess.ID, map[string]any{
						"sessionUpdate": "agent_message_chunk",
						"content":       map[string]any{"type": "text", "text": "\n⚠ API error (attempt 2): RESOURCE_EXHAUSTED (code 429): Resource has been exhausted (e.g. check quota).\n"},
					})
					select {
					case <-cCh:
						send(rpcMsg{JSONRPC: "2.0", ID: reqID, Result: map[string]any{"stopReason": "cancelled"}})
						return
					case <-time.After(1500 * time.Millisecond):
					}
					sendUpdate(sess.ID, map[string]any{
						"sessionUpdate": "agent_message_chunk",
						"content":       map[string]any{"type": "text", "text": "\n⚠ RESOURCE_EXHAUSTED (code 429): Resource has been exhausted (e.g. check quota).\n"},
					})
					send(rpcMsg{JSONRPC: "2.0", ID: reqID, Result: map[string]any{"stopReason": "end_turn", "usage": map[string]any{"inputTokens": 100, "outputTokens": 8}}})
					return
				}
				if strings.Contains(txt, "[FAKE_REVERSIBLE_ROUNDTRIP]") {
					combined := sess.SysAppend + "\n" + txt
					segTok := fakeSegTokRe.FindString(combined)
					tagTok := fakeTagTokRe.FindString(combined)
					var toolOut string
					if tNum == 1 && len(sess.McpTools) > 0 && segTok != "" {
						var res map[string]any
						if strings.Contains(txt, "[FAKE_CALL_VENDOR_ALIAS]") {
							res = callMcp(sess, "tools/call", map[string]any{
								"name": findMcpTool(sess, "ws_call_tool"),
								"arguments": map[string]any{
									"name": "ws_present",
									"arguments": map[string]any{
										"path": "/Applications/" + segTok + ".app/Contents/MacOS/dsh",
									},
								},
							})
						} else {
							res = callMcp(sess, "tools/call", map[string]any{
								"name": findMcpTool(sess, "Read"),
								"arguments": map[string]any{
									"file_path": "/Applications/" + segTok + ".app/Contents/MacOS/dsh",
								},
							})
						}
						if res != nil {
							if arr, ok := res["content"].([]any); ok && len(arr) > 0 {
								if cm, ok := arr[0].(map[string]any); ok {
									toolOut, _ = cm["text"].(string)
								}
							}
						}
					}
					toolOutSeg := fakeSegTokRe.FindString(toolOut)
					if segTok == "" {
						segTok = toolOutSeg
					}
					leaked := strings.Contains(combined, "DeepSeek Harness") ||
						strings.Contains(combined, "Claude Code") ||
						strings.Contains(combined, "<dsh_") ||
						strings.Contains(combined, "<system-reminder>") ||
						strings.Contains(strings.Join(sess.McpTools, ","), "dsh_present") ||
						strings.Contains(toolOut, "DeepSeek Harness") ||
						strings.Contains(toolOut, "Claude Code") ||
						strings.Contains(toolOut, "<system-reminder>")
					var chunks []string
					if tagTok != "" && segTok != "" && len(tagTok) == 14 && len(segTok) == 14 {
						chunks = []string{
							fmt.Sprintf("UPSTREAM_CLEAN=%v TOOL_SEG=%v <", !leaked, toolOutSeg != "") + tagTok[:9],
							tagTok[9:] + ">/Applications/" + segTok[:8],
							segTok[8:] + ".app</" + tagTok[:6],
							tagTok[6:] + ">",
						}
					} else {
						chunks = []string{fmt.Sprintf("UPSTREAM_CLEAN=%v seg=%q tag=%q", !leaked, segTok, tagTok)}
					}
					for _, c := range chunks {
						sendUpdate(sess.ID, map[string]any{
							"sessionUpdate": "agent_message_chunk",
							"content":       map[string]any{"type": "text", "text": c},
						})
					}
					send(rpcMsg{JSONRPC: "2.0", ID: reqID, Result: map[string]any{"stopReason": "end_turn", "usage": map[string]any{"inputTokens": 180, "outputTokens": 24, "cachedReadTokens": 1024}}})
					return
				}
				if strings.Contains(txt, "[FAKE_THINK_TRAILING_W]") {
					sendUpdate(sess.ID, map[string]any{
						"sessionUpdate": "agent_thought_chunk",
						"content":       map[string]any{"type": "text", "text": "Let's check how"},
					})
					sendUpdate(sess.ID, map[string]any{
						"sessionUpdate": "agent_message_chunk",
						"content":       map[string]any{"type": "text", "text": "Answer ready"},
					})
					send(rpcMsg{JSONRPC: "2.0", ID: reqID, Result: map[string]any{
						"stopReason": "end_turn",
						"usage":      map[string]any{"inputTokens": 120, "outputTokens": 12, "thoughtTokens": 5},
					}})
					return
				}
				if m := regexp.MustCompile(`\[FAKE_EMIT_TOKEN:([A-Za-z0-9_]+)\]`).FindStringSubmatch(txt); len(m) == 2 {
					tok := m[1]
					sendUpdate(sess.ID, map[string]any{
						"sessionUpdate": "agent_message_chunk",
						"content":       map[string]any{"type": "text", "text": "<" + tok + ">ok</" + tok + ">"},
					})
					send(rpcMsg{JSONRPC: "2.0", ID: reqID, Result: map[string]any{
						"stopReason": "end_turn",
						"usage":      map[string]any{"inputTokens": 100, "outputTokens": 8},
					}})
					return
				}
				noticePlans := map[string][]string{
					"[FAKE_NOTICE_NET]":               {"\n⚠ ", "There was a network issue ", "connecting to the server, please try again", ".\n"},
					"[FAKE_NOTICE_TRAFFIC]":           {"Our servers are experiencing high traffic right now, ", "please try again in a minute."},
					"[FAKE_NOTICE_RECOVER]":           {"⚠ There was a network issue connecting to the server, please try again.\n", "recovered answer"},
					"[FAKE_NOTICE_QUOTE]":             {"There was a network issue connecting to the server, please try again.", " This sentence is quoted in prose."},
					"[FAKE_RETRY_SAME_CHUNK_RECOVER]": {"⚠ API error (attempt 1): 503 MODEL_CAPACITY_EXHAUSTED\nRecovered after retry"},
					"[FAKE_RETRY_SPLIT_RECOVER]":      {"⚠ API error (attempt 1): 503 ", "MODEL_CAPACITY_EXHAUSTED\n", "Recovered split retry"},
					"[FAKE_RETRY_EXHAUST_429]":        {"⚠ API error (attempt 2): Individual quota reached. Resets in 12h.\n", "⚠ Agent execution terminated due to error."},
					"[FAKE_RETRY_EXHAUST_529]":        {"⚠ API error (attempt 2): 503 MODEL_CAPACITY_EXHAUSTED: no capacity\n", "⚠ Agent execution terminated due to error."},
					"[FAKE_RETRY_EXHAUST_400]":        {"⚠ API error (attempt 1): 400 INVALID_ARGUMENT: prompt too large\n", "⚠ Agent execution terminated due to error."},
					"[FAKE_AGY159_RETRY_EXHAUST_429]": {"\n⚠ API error (attempt 1): RESOURCE_EXHAUSTED (code 429): Resource has been exhausted (e.g. check quota).\n", "\n⚠ API error (attempt 8): RESOURCE_EXHAUSTED (code 429): Resource has been exhausted (e.g. check quota).\n", "\n⚠ RESOURCE_EXHAUSTED ", "(code 429): Resource has been exhausted (e.g. check quota).\n"},
					"[FAKE_AGY159_RAW_529]":           {"\n⚠ UNAVAILABLE (code 503): ", "No capacity available for model gemini-4-carbon\n"},
					"[FAKE_AGY159_RAW_400]":           {"\n⚠ INVALID_ARGUMENT (code 400): Request contains an invalid argument.\n"},
					"[FAKE_LICENSE_3501]":             {"\n⚠ You do not have a valid license to use Gemini Code Assist. (#3501)\n"},
				}
				for marker, chunks := range noticePlans {
					if !strings.Contains(txt, marker) {
						continue
					}
					for _, c := range chunks {
						sendUpdate(sess.ID, map[string]any{
							"sessionUpdate": "agent_message_chunk",
							"content":       map[string]any{"type": "text", "text": c},
						})
					}
					send(rpcMsg{JSONRPC: "2.0", ID: reqID, Result: map[string]any{
						"stopReason": "end_turn",
						"usage":      map[string]any{"inputTokens": 100, "outputTokens": 8, "cachedReadTokens": 0},
					}})
					return
				}
				if strings.Contains(txt, "[FAKE_SOFT_429]") {
					sendUpdate(sess.ID, map[string]any{
						"sessionUpdate": "agent_message_chunk",
						"content":       map[string]any{"type": "text", "text": "⚠ Agent execution terminated due to error."},
					})
					send(rpcMsg{
						JSONRPC: "2.0",
						ID:      reqID,
						Result: map[string]any{
							"stopReason": "end_turn",
							"usage":      map[string]any{"inputTokens": 100, "outputTokens": 8, "cachedReadTokens": 0},
						},
					})
					return
				}

				curModel, curTier := sessModelAndTier(sess)
				inTok, crTok := 256, 2048
				switch {
				case strings.Contains(txt, "[FAKE_ZERO_250K]"):
					inTok, crTok = 240000+tNum*5000, 0
				case strings.Contains(txt, "[FAKE_PREFIX_250K]"):
					inTok, crTok = 225000+tNum*5000, 15000
				case strings.Contains(txt, "[FAKE_HIT_250K]"):
					inTok, crTok = 5000, 240000+tNum*5000
				case strings.Contains(txt, "[FAKE_ZERO_50K]"):
					inTok, crTok = 40000+tNum*2000, 0
				}
				baseTh := 0
				if strings.Contains(txt, "[FAKE_THINK]") {
					baseTh = 7
				}
				genRec := resolveTurnTelemetry(curModel, curTier, txt, inTok, crTok, 18, baseTh)

				if genRec.RealTiming && !strings.Contains(txt, "[FAKE_TTFT_300MS]") {
					if !sleepCancellable(time.Duration(genRec.TtftMs)*time.Millisecond, cCh) {
						return
					}
				}
				if (genRec.ThTok > 0 || genRec.ThinkingMs > 0) && !strings.Contains(txt, "[FAKE_THINK]") {
					sendUpdate(sess.ID, map[string]any{
						"sessionUpdate": "agent_thought_chunk",
						"content":       map[string]any{"type": "text", "text": "Thinking step 1..."},
					})
				}
				if genRec.ThTok > 0 || strings.Contains(txt, "[FAKE_THINK]") {
					genRec.ThoughtText = "Thinking step 1..."
				}
				if genRec.ThinkingMs > 0 {
					if !sleepCancellable(time.Duration(genRec.ThinkingMs)*time.Millisecond, cCh) {
						return
					}
				}

				reply := fmt.Sprintf("FAKE_OK model=%s tier=%s turn=%d", curModel, curTier, tNum)
				if strings.Contains(txt, "[ECHO_SYS_APPEND]") && sess.SysAppend != "" {
					reply += " sys_has_rules=" + fmt.Sprintf("%v", strings.Contains(sess.SysAppend, "CRITICAL TOOL ROUTING RULES"))
				}
				if strings.Contains(txt, "[ECHO_TOOLS]") {
					reply += " tools=" + strings.Join(sess.McpTools, ",")
				}
				if strings.Contains(txt, "[ECHO_CLIENT_INFO]") {
					initMu.Lock()
					cn, cv := initClientName, initClientVer
					initMu.Unlock()
					reply += fmt.Sprintf(" client=%s/%s args=%s", cn, cv, strings.Join(os.Args[1:], "|"))
				}
				if strings.Contains(txt, "[ECHO_PROMPT]") {
					reply += "\nSYS_APPEND<<" + sess.SysAppend + ">>\nPROMPT<<" + txt + ">>"
				}
				if strings.Contains(txt, "[ECHO_RELAY_META]") {
					sessMu.Lock()
					rc, rf := sess.ResumeCount, sess.RelayFollows
					sessMu.Unlock()
					reply += fmt.Sprintf(" sid=%s resumes=%d relay_follows=%d", sess.ID, rc, rf)
				}
				genRec.ReplyText = reply
				sendUpdate(sess.ID, map[string]any{
					"sessionUpdate": "agent_message_chunk",
					"content":       map[string]any{"type": "text", "text": reply},
				})
				if genRec.RealTiming && genRec.StreamMs > genRec.ThinkingMs {
					if !sleepCancellable(time.Duration(genRec.StreamMs-genRec.ThinkingMs)*time.Millisecond, cCh) {
						return
					}
				}
				_ = appendSessionGeneration(resolveFakeConvDir(os.Args[1:]), sess.ID, genRec)
				emitUsageUpdate(sess.ID, curModel, genRec.InTok, genRec.CrTok)
				usageMap := map[string]any{
					"inputTokens":      genRec.InTok,
					"outputTokens":     genRec.OutTok,
					"cachedReadTokens": genRec.CrTok,
				}
				if genRec.ThTok > 0 {
					usageMap["thoughtTokens"] = genRec.ThTok
				}
				send(rpcMsg{
					JSONRPC: "2.0",
					ID:      reqID,
					Result: map[string]any{
						"stopReason": "end_turn",
						"usage":      usageMap,
					},
				})
			}(msg.ID, s, promptText, turnNum, cancelCh)
		}
	}
}
