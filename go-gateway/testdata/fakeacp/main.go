package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
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
	ID         string
	Cwd        string
	Model      string
	Tier       string
	McpURL     string
	McpAuth    string
	McpTools   []string
	SysAppend  string
	TurnCount  int
	CancelChan chan struct{}
}

var (
	outMu    sync.Mutex
	sessions = map[string]*sessionInfo{}
	sessMu   sync.Mutex
	seq      int
)

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
		_ = callMcp(s, "initialize", map[string]any{"protocolVersion": "2025-06-18"})
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
	prefix := "dsh_" + hostName + "_"
	for _, n := range s.McpTools {
		if strings.HasPrefix(n, prefix) {
			return n
		}
	}
	if len(s.McpTools) > 0 {
		return s.McpTools[0]
	}
	return "dsh_" + hostName + "_000000000000"
}

func main() {
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

		switch msg.Method {
		case "initialize":
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
			s := &sessionInfo{ID: sid, Model: "gemini-4-argon", Tier: "medium", CancelChan: make(chan struct{})}
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
				s = &sessionInfo{ID: sid, Model: "gemini-4-argon", Tier: "medium", CancelChan: make(chan struct{})}
				sessions[sid] = s
			} else {
				s.CancelChan = make(chan struct{})
			}
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
			sessMu.Lock()
			s := sessions[sid]
			if s != nil {
				if cid == "model" {
					s.Model = val
				} else if cid == "thought_level" {
					s.Tier = val
				}
			}
			sessMu.Unlock()
			if s != nil && s.Model == "gpt-oss-120b-medium" && cid == "thought_level" {
				send(rpcMsg{
					JSONRPC: "2.0",
					ID:      msg.ID,
					Error:   map[string]any{"code": -32602, "message": "Thinking level not offered for GPT-OSS"},
				})
				continue
			}
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
				if strings.Contains(txt, "[FAKE_RPC_ERROR]") {
					send(rpcMsg{
						JSONRPC: "2.0",
						ID:      reqID,
						Error:   map[string]any{"code": -32000, "message": "simulated upstream RPC failure"},
					})
					return
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
				if strings.Contains(txt, "[FAKE_THINK]") {
					sendUpdate(sess.ID, map[string]any{
						"sessionUpdate": "agent_thought_chunk",
						"content":       map[string]any{"type": "text", "text": "Thinking step 1..."},
					})
				}
				if strings.Contains(txt, "[FAKE_TOOL_CALL]") && tNum == 1 {
					mcpName := findMcpTool(sess, "Bash")
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
					if res != nil {
						if arr, ok := res["content"].([]any); ok && len(arr) > 0 {
							if cm, ok := arr[0].(map[string]any); ok {
								toolOut, _ = cm["text"].(string)
							}
						}
					}
					sendUpdate(sess.ID, map[string]any{
						"sessionUpdate": "agent_message_chunk",
						"content":       map[string]any{"type": "text", "text": "TOOL_DONE:" + toolOut},
					})
					send(rpcMsg{
						JSONRPC: "2.0",
						ID:      reqID,
						Result: map[string]any{
							"stopReason": "end_turn",
							"usage": map[string]any{
								"inputTokens":      320,
								"outputTokens":     24,
								"cachedReadTokens": 4096,
							},
						},
					})
					return
				}
				if strings.Contains(txt, "[FAKE_MAX_TOKENS]") && tNum == 1 {
					sendUpdate(sess.ID, map[string]any{
						"sessionUpdate": "agent_message_chunk",
						"content":       map[string]any{"type": "text", "text": "PART1_"},
					})
					send(rpcMsg{
						JSONRPC: "2.0",
						ID:      reqID,
						Result: map[string]any{
							"stopReason": "max_tokens",
							"usage":      map[string]any{"inputTokens": 200, "outputTokens": 10, "cachedReadTokens": 1000},
						},
					})
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

				reply := fmt.Sprintf("FAKE_OK model=%s tier=%s turn=%d", sess.Model, sess.Tier, tNum)
				if strings.Contains(txt, "[ECHO_SYS_APPEND]") && sess.SysAppend != "" {
					reply += " sys_has_rules=" + fmt.Sprintf("%v", strings.Contains(sess.SysAppend, "CRITICAL TOOL ROUTING RULES"))
				}
				sendUpdate(sess.ID, map[string]any{
					"sessionUpdate": "agent_message_chunk",
					"content":       map[string]any{"type": "text", "text": reply},
				})
				send(rpcMsg{
					JSONRPC: "2.0",
					ID:      reqID,
					Result: map[string]any{
						"stopReason": "end_turn",
						"usage": map[string]any{
							"inputTokens":      256,
							"outputTokens":     18,
							"cachedReadTokens": 2048,
						},
					},
				})
			}(msg.ID, s, promptText, turnNum, cancelCh)
		}
	}
}
