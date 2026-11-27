package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	gwcfg "agy-acp-gateway-go/internal/config"
)

const mcpSseWriteTimeout = 30 * time.Second

// P3-6: Binary verification of bin/agy-acp-900k (agy 1.5.16) confirmed that agy still enforces a
// 10,240-byte output truncation decorator on MCP tool results:
//   - Embedded config JSON @ 0xdb54fd8: `"tool-output-max-bytes": 10240`
//   - Flag string @ 0x68772f4 loaded in `config.getToolConfigOverrides` @ 0x8d50ba4 -> ToolOutputConfig.MaxOutputBytes
//   - Applied to MCP tools in `tooldecorators.newOutputTruncationDecorator.func1` (0x9f3dc60..0x9f3de97)
//   - Mid-truncates via `tooldecorators.truncateOutput` (0x9f3cb40..0x9f3ce40) pointing to a saved temp
//     file (0x6a34b3d) that ws-bridge disables read_file on.
//
// Pre-bounding text outputs cleanly at 70/30 head+tail (default 9600B, 0 disables) keeps structured
// outputs and pagination hints intact beneath agy's 10,240B cliff.
var McpToolResultMaxBytes = parseNonNegIntEnv("AGY_MCP_TOOL_RESULT_MAX_BYTES", 9600)

func parseNonNegIntEnv(name string, def int) int {
	raw := strings.TrimSpace(gwcfg.EnvStr(name, ""))
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < 0 {
		return def
	}
	return v
}

func effectiveMcpToolResultMaxBytes() int {
	return McpToolResultMaxBytes
}

func utf8SafePrefixBytes(s string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	if len(s) <= maxBytes {
		return s
	}
	cut := maxBytes
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}

func utf8SafeSuffixBytes(s string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	if len(s) <= maxBytes {
		return s
	}
	start := len(s) - maxBytes
	for start < len(s) && !utf8.RuneStart(s[start]) {
		start++
	}
	return s[start:]
}

// preboundMcpToolText applies UTF-8-safe 70/30 head+tail truncation when len(txt) > maxBytes,
// preserving both opening structure and trailing summary/pagination hints under agy's 10,240B limit.
func preboundMcpToolText(txt string, maxBytes int) (string, bool) {
	if maxBytes <= 0 || len(txt) <= maxBytes {
		return txt, false
	}
	estMarker := fmt.Sprintf("\n... [truncated %d bytes to preserve structure] ...\n", len(txt))
	if maxBytes <= len(estMarker)+16 {
		return utf8SafePrefixBytes(txt, maxBytes), true
	}
	avail := maxBytes - len(estMarker)
	headBudget := (avail * 70) / 100
	tailBudget := avail - headBudget
	head := utf8SafePrefixBytes(txt, headBudget)
	tail := utf8SafeSuffixBytes(txt, tailBudget)
	omitted := len(txt) - len(head) - len(tail)
	if omitted <= 0 {
		return txt, false
	}
	marker := fmt.Sprintf("\n... [truncated %d bytes to preserve structure] ...\n", omitted)
	out := head + marker + tail
	if len(out) > maxBytes {
		// Defensive clamp if marker length ever grew (impossible since omitted <= len(txt)).
		out = utf8SafePrefixBytes(out, maxBytes)
	}
	return out, true
}

func preboundMcpToolResult(res map[string]any, maxBytes int) (map[string]any, int, int) {
	if res == nil || maxBytes <= 0 {
		return res, 0, 0
	}
	rawContent, ok := res["content"]
	if !ok || rawContent == nil {
		return res, 0, 0
	}

	var blocks []map[string]any
	isMapSlice := false
	switch c := rawContent.(type) {
	case []map[string]any:
		blocks = c
		isMapSlice = true
	case []any:
		blocks = make([]map[string]any, len(c))
		for i, item := range c {
			if m, okM := item.(map[string]any); okM {
				blocks[i] = m
			}
		}
	default:
		return res, 0, 0
	}

	totalTextBytes := 0
	textBlockIndices := make([]int, 0, len(blocks))
	for i, blk := range blocks {
		if blk == nil {
			continue
		}
		if typ, _ := blk["type"].(string); typ == "text" {
			if txt, okT := blk["text"].(string); okT {
				totalTextBytes += len(txt)
				textBlockIndices = append(textBlockIndices, i)
			}
		}
	}
	if totalTextBytes <= maxBytes || len(textBlockIndices) == 0 {
		return res, totalTextBytes, totalTextBytes
	}

	// Copy outer map and content slice so caller-owned structures are never mutated in place.
	outRes := make(map[string]any, len(res))
	for k, v := range res {
		outRes[k] = v
	}
	clonedBlocks := make([]map[string]any, len(blocks))
	for i, blk := range blocks {
		if blk != nil {
			cp := make(map[string]any, len(blk))
			for k, v := range blk {
				cp[k] = v
			}
			clonedBlocks[i] = cp
		}
	}

	newTotalBytes := 0
	if len(textBlockIndices) == 1 {
		idx := textBlockIndices[0]
		txt, _ := clonedBlocks[idx]["text"].(string)
		bounded, _ := preboundMcpToolText(txt, maxBytes)
		clonedBlocks[idx]["text"] = bounded
		newTotalBytes = len(bounded)
	} else {
		// Split maxBytes proportionally across multiple text blocks.
		remainingBudget := maxBytes
		remainingOrig := totalTextBytes
		for k, idx := range textBlockIndices {
			txt, _ := clonedBlocks[idx]["text"].(string)
			origLen := len(txt)
			var share int
			if k == len(textBlockIndices)-1 {
				share = remainingBudget
			} else if remainingOrig > 0 {
				share = (remainingBudget * origLen) / remainingOrig
			}
			var bounded string
			if share <= 0 {
				bounded = ""
			} else {
				bounded, _ = preboundMcpToolText(txt, share)
			}
			clonedBlocks[idx]["text"] = bounded
			newTotalBytes += len(bounded)
			remainingBudget -= len(bounded)
			remainingOrig -= origLen
		}
	}

	if isMapSlice {
		outRes["content"] = clonedBlocks
	} else {
		anyBlocks := make([]any, len(clonedBlocks))
		for i, blk := range clonedBlocks {
			if blk != nil {
				anyBlocks[i] = blk
			} else if arr, okA := rawContent.([]any); okA && i < len(arr) {
				anyBlocks[i] = arr[i]
			}
		}
		outRes["content"] = anyBlocks
	}
	return outRes, totalTextBytes, newTotalBytes
}

func setMcpSseWriteDeadline(w http.ResponseWriter) {
	if w != nil {
		_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(mcpSseWriteTimeout))
	}
}

func isLoopbackRemoteAddr(remoteAddr string) bool {
	trimmed := strings.TrimSpace(remoteAddr)
	if trimmed == "" {
		return testing.Testing()
	}
	host := trimmed
	if h, _, err := net.SplitHostPort(trimmed); err == nil {
		host = h
	} else if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = host[1 : len(host)-1]
	}
	if strings.EqualFold(host, "localhost") || (host == "192.0.2.1" && testing.Testing()) {
		return true
	}
	addr, err := netip.ParseAddr(host)
	return err == nil && addr.Unmap().IsLoopback()
}

// isTrustedProxyRemoteAddr permits X-Forwarded-Host / X-Forwarded-Proto only from loopback
// or private RFC1918/ULA reverse-proxy hops (e.g. host OpenResty or Docker bridge Nginx/Caddy).
func isTrustedProxyRemoteAddr(remoteAddr string) bool {
	if isLoopbackRemoteAddr(remoteAddr) {
		return true
	}
	trimmed := strings.TrimSpace(remoteAddr)
	if trimmed == "" {
		return false
	}
	host := trimmed
	if h, _, err := net.SplitHostPort(trimmed); err == nil {
		host = h
	} else if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = host[1 : len(host)-1]
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}
	u := addr.Unmap()
	return u.IsLoopback() || u.IsPrivate()
}

func (s *AcpProjectSession) HandleMcpHTTP(w http.ResponseWriter, r *http.Request) {
	if !isLoopbackRemoteAddr(r.RemoteAddr) {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	// Same guards as Node DshMcpBridge: constant-time bearer token per tenant, no browser Origin
	expectedAuth := "Bearer " + s.TenantKey
	gotAuth := r.Header.Get("Authorization")
	if s.TenantKey == "" || subtle.ConstantTimeCompare([]byte(gotAuth), []byte(expectedAuth)) != 1 {
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
	dec := json.NewDecoder(r.Body)
	dec.UseNumber()
	if err := dec.Decode(&msg); err != nil {
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
		bridgeSrv := s.bridgeServerName()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      msg.ID,
			"result": map[string]any{
				"protocolVersion": protoVer,
				"capabilities":    map[string]any{"tools": map[string]any{"listChanged": false}},
				"serverInfo":      map[string]any{"name": bridgeSrv, "version": "2.0.0"},
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
		cat := s.InitialToolsCatalog
		if len(cat) == 0 {
			cat = s.ToolsCatalog
		}
		codec := s.Codec
		tools := make([]map[string]any, 0, len(cat))
		for _, t := range cat {
			desc := fmt.Sprintf("Workspace tool %s: %s", t.OutboundName(), t.Description)
			schema := t.Parameters
			if codec != nil {
				desc = codec.EncodeOutboundText(desc)
				schema = codec.EncodeMap(schema)
			}
			tools = append(tools, map[string]any{
				"name":        t.McpName,
				"description": desc,
				"inputSchema": schema,
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
		setMcpSseWriteDeadline(w)
		defer clearSseWriteDeadline(w)
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		if flusher != nil {
			flusher.Flush()
		}

		var sseMu sync.Mutex
		var sseWg sync.WaitGroup
		doneCh := make(chan struct{})
		var stopOnce sync.Once
		stopKeepalive := func() {
			stopOnce.Do(func() {
				close(doneCh)
				sseWg.Wait()
			})
		}
		sseWg.Add(1)
		defer stopKeepalive()
		go func() {
			defer sseWg.Done()
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
					select {
					case <-doneCh:
						sseMu.Unlock()
						return
					default:
					}
					setMcpSseWriteDeadline(w)
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
		hostName, allowed := resolveAllowedHostTool(s.McpToHostMap, rawName)
		sid := s.SessionID
		convKey := s.ConvKey
		s.mu.Unlock()

		var resObj map[string]any
		if !allowed {
			LogEvent("MCP_TOOL_REJECTED", "Rejected tools/call for a tool outside the current catalog", map[string]any{
				"convKey": convKey, "sessionId": sid, "toolName": rawName,
			})
			resObj = mcpToolError(fmt.Sprintf("Tool %s is not available in this session.", rawName))
		} else {
			resObj = s.executeMcpToolCall(r.Context(), hostName, rawArgs)
		}
		stopKeepalive()

		if maxBytes := effectiveMcpToolResultMaxBytes(); maxBytes > 0 {
			boundedObj, origLen, boundedLen := preboundMcpToolResult(resObj, maxBytes)
			if boundedLen < origLen {
				resObj = boundedObj
				LogEvent("MCP_TOOL_RESULT_PREBOUNDED", fmt.Sprintf("Pre-bounded MCP tool result %d -> %d bytes (limit %d)", origLen, boundedLen, maxBytes), map[string]any{
					"convKey":      convKey,
					"sessionId":    sid,
					"toolName":     hostName,
					"origBytes":    origLen,
					"boundedBytes": boundedLen,
					"maxBytes":     maxBytes,
				})
			}
		}

		replyBytes, _ := json.Marshal(map[string]any{
			"jsonrpc": "2.0",
			"id":      msg.ID,
			"result":  resObj,
		})
		sseMu.Lock()
		setMcpSseWriteDeadline(w)
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

func (s *AcpProjectSession) awaitParkedCall(ctx context.Context, callID string) map[string]any {
	s.mu.Lock()
	entry, ok := s.ParkedCalls[callID]
	if !ok && s.RelayedCalls != nil {
		entry, ok = s.RelayedCalls[callID]
	}
	s.mu.Unlock()
	if !ok {
		return map[string]any{
			"content": []map[string]any{{"type": "text", "text": fmt.Sprintf("Unknown or already completed call_id %s.", callID)}},
			"isError": true,
		}
	}
	return s.awaitParkedEntry(ctx, entry)
}

func (s *AcpProjectSession) awaitParkedEntry(ctx context.Context, entry *ParkedMcpCall) map[string]any {
	if ctx == nil {
		ctx = context.Background()
	}
	callID := entry.Call.ID
	awaitMcpName := s.awaitHelperFullMcpName()
	outToolName := NeutralizeToolIdentifier(entry.Call.Name)
	parkedAt := entry.ParkedAt
	if parkedAt.IsZero() {
		parkedAt = time.Now()
	}
	s.mu.Lock()
	ctxTokens := s.LastReportedContextUsed
	if ctxTokens <= 0 {
		ctxTokens = s.LastCachedReadTokens
	}
	s.mu.Unlock()
	interval := toolRelayIntervalForTool(time.Since(parkedAt), entry.Call.Name, ctxTokens)
	timer := time.NewTimer(interval)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return mcpToolError("MCP tool call context cancelled.")
	case res := <-entry.ResultChan:
		s.mu.Lock()
		delete(s.ParkedCalls, callID)
		if s.RelayedCalls != nil {
			delete(s.RelayedCalls, callID)
		}
		s.mu.Unlock()
		return res
	case <-timer.C:
		now := time.Now()
		mergeWin := interval / 2
		if mergeWin > 5*time.Second {
			mergeWin = 5 * time.Second
		}
		maxRelays := effectiveMaxToolRelays()
		parkedTtl := effectiveParkedTtl()
		s.mu.Lock()
		if s.SessionID == "" || (entry.SessionID != "" && s.SessionID != entry.SessionID) {
			s.mu.Unlock()
			return mcpToolError("Session closed.")
		}
		sid := s.SessionID
		convKey := s.ConvKey
		parkedDur := now.Sub(parkedAt)
		if !s.ParkedSince.IsZero() && now.Sub(s.ParkedSince) > parkedDur {
			parkedDur = now.Sub(s.ParkedSince)
		}
		if s.hasActiveCompactionPendingLocked(now) {
			s.mu.Unlock()
			if s.HibernateIfSession(sid, "compaction_pending_relay_pause") {
				LogEvent("MCP_TOOL_RELAY_PAUSED", fmt.Sprintf("Paused parked tool relay while compaction is in flight (%ds parked); hibernating with resume state", int(parkedDur.Seconds())), map[string]any{
					"convKey":    convKey,
					"sessionId":  sid,
					"toolCallId": callID,
					"toolName":   entry.Call.Name,
					"parkedSec":  int(parkedDur.Seconds()),
				})
				select {
				case res := <-entry.ResultChan:
					return res
				default:
					return mcpToolError("Session hibernated.")
				}
			}
			// A live turn claimed the session concurrently; wait normally without burning orphan relay cap.
			s.mu.Lock()
			if s.SessionID == "" || s.closing || (entry.SessionID != "" && s.SessionID != entry.SessionID) {
				s.mu.Unlock()
				select {
				case res := <-entry.ResultChan:
					return res
				default:
					return mcpToolError("Session hibernated.")
				}
			}
			if _, stillParked := s.ParkedCalls[callID]; stillParked || len(entry.ResultChan) > 0 {
				if s.RelayedCalls == nil {
					s.RelayedCalls = make(map[string]*ParkedMcpCall)
				}
				s.RelayedCalls[callID] = entry
			}
			s.mu.Unlock()
			waitLive := time.NewTimer(toolRelayInterval(parkedDur))
			defer waitLive.Stop()
			select {
			case <-ctx.Done():
				return mcpToolError("MCP tool call context cancelled.")
			case res := <-entry.ResultChan:
				s.mu.Lock()
				delete(s.ParkedCalls, callID)
				if s.RelayedCalls != nil {
					delete(s.RelayedCalls, callID)
				}
				s.mu.Unlock()
				return res
			case <-waitLive.C:
				return map[string]any{
					"content": []map[string]any{
						{
							"type": "text",
							"text": fmt.Sprintf("Tool call %s (%s) is still running in the host harness. Call %s with {\"call_id\":\"%s\"} immediately to continue waiting for its result.", callID, outToolName, awaitMcpName, callID),
						},
					},
				}
			}
		}
		if maxRelays > 0 && s.orphanCapStreak >= 1 {
			maxRelays = 0
		}
		entry.RelayCount++
		if s.lastOrphanRelayAt.IsZero() || now.Sub(s.lastOrphanRelayAt) >= mergeWin {
			s.orphanRelayHops++
			s.lastOrphanRelayAt = now
		}
		relayCount := entry.RelayCount
		orphanHops := s.orphanRelayHops
		capReached := (maxRelays >= 0 && (relayCount > maxRelays || orphanHops > maxRelays)) ||
			(parkedTtl > 0 && parkedDur >= parkedTtl)
		firstCap := false
		if capReached {
			if !s.orphanCapLogged {
				s.orphanCapLogged = true
				firstCap = true
			}
		} else {
			s.relaysThisTurn++
			if s.RelayedCalls == nil {
				s.RelayedCalls = make(map[string]*ParkedMcpCall)
			}
			s.RelayedCalls[callID] = entry
		}
		s.mu.Unlock()

		if capReached {
			if firstCap {
				orphanRelayCapsTotal.Add(1)
				LogEvent("MCP_TOOL_RELAY_CAP", fmt.Sprintf("Stopped orphan tool relay after %d hop(s) (%ds parked); hibernating session with resume state", orphanHops-1, int(parkedDur.Seconds())), map[string]any{
					"convKey":         convKey,
					"sessionId":       sid,
					"toolCallId":      callID,
					"toolName":        entry.Call.Name,
					"relayCount":      relayCount - 1,
					"orphanRelayHops": orphanHops - 1,
					"maxToolRelays":   maxRelays,
					"parkedSec":       int(parkedDur.Seconds()),
				})
			}
			if s.HibernateIfSession(sid, "reap_orphan_parked_relay_cap") {
				select {
				case res := <-entry.ResultChan:
					return res
				default:
					return mcpToolError("Session hibernated.")
				}
			}
			// HibernateIfSession yielded because a client turn arrived (s.IsBusy == true) or another
			// goroutine is closing the session: ensure RelayedCalls tracks entry so ws_await_result can
			// still drain ResultChan if waitLive expires while RunTurn removes callID from ParkedCalls.
			s.mu.Lock()
			if s.SessionID == "" || s.closing || (entry.SessionID != "" && s.SessionID != entry.SessionID) {
				s.mu.Unlock()
				select {
				case res := <-entry.ResultChan:
					return res
				default:
					return mcpToolError("Session hibernated.")
				}
			}
			if _, stillParked := s.ParkedCalls[callID]; stillParked || len(entry.ResultChan) > 0 {
				if s.RelayedCalls == nil {
					s.RelayedCalls = make(map[string]*ParkedMcpCall)
				}
				s.RelayedCalls[callID] = entry
			}
			s.mu.Unlock()
			waitLive := time.NewTimer(toolRelayInterval(parkedDur))
			defer waitLive.Stop()
			select {
			case <-ctx.Done():
				return mcpToolError("MCP tool call context cancelled.")
			case res := <-entry.ResultChan:
				s.mu.Lock()
				delete(s.ParkedCalls, callID)
				if s.RelayedCalls != nil {
					delete(s.RelayedCalls, callID)
				}
				s.mu.Unlock()
				return res
			case <-waitLive.C:
				return map[string]any{
					"content": []map[string]any{
						{
							"type": "text",
							"text": fmt.Sprintf("Tool call %s (%s) is still running in the host harness. Call %s with {\"call_id\":\"%s\"} immediately to continue waiting for its result.", callID, outToolName, awaitMcpName, callID),
						},
					},
				}
			}
		}

		LogEvent("MCP_TOOL_RELAY", fmt.Sprintf("Relaying long-running tool call via %s", AwaitToolName), map[string]any{
			"convKey":         convKey,
			"sessionId":       sid,
			"toolCallId":      callID,
			"toolName":        entry.Call.Name,
			"relayCount":      relayCount,
			"orphanRelayHops": orphanHops,
		})
		return map[string]any{
			"content": []map[string]any{
				{
					"type": "text",
					"text": fmt.Sprintf("Tool call %s (%s) is still running in the host harness. Call %s with {\"call_id\":\"%s\"} immediately to continue waiting for its result.", callID, outToolName, awaitMcpName, callID),
				},
			},
		}
	}
}

func cleanToolInputMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		if k == "toolSummary" {
			continue
		}
		out[k] = v
	}
	return out
}

func (s *AcpProjectSession) resolveEffectiveToolLocked(hostName string, parsedInput map[string]any) (effectiveName string, effectiveInput map[string]any, sig string, errMsg string) {
	if s.activeTurnToolChoice.Mode == "none" {
		return hostName, nil, "", "Tool calls are disabled on this turn (tool_choice=none). Respond directly in assistant text without calling tools."
	}
	effectiveName = hostName
	effectiveInput = cleanToolInputMap(parsedInput)
	if hostName == CallToolName || hostName == LegacyCallToolName {
		n, _ := parsedInput["name"].(string)
		if s.Codec != nil {
			n = s.Codec.RestoreText(n)
		}
		resolved, ok := allowedHostToolName(s.McpToHostMap, n, s.ToolsCatalog, s.InitialToolsCatalog)
		if !ok {
			return n, nil, "", fmt.Sprintf("Tool %q is not available in this session.", n)
		}
		effectiveName = resolved
		if a, ok := parsedInput["arguments"].(map[string]any); ok {
			effectiveInput = cleanToolInputMap(a)
		} else {
			effectiveInput = make(map[string]any)
		}
	}
	if len(s.activeTurnToolChoice.Allowed) > 0 && !s.activeTurnToolChoice.Allowed[effectiveName] {
		return effectiveName, nil, "", fmt.Sprintf("Tool %q is disallowed on this turn by tool_choice; allowed tool(s): %s.", effectiveName, strings.Join(s.activeTurnToolChoice.AllowedOrdered, ", "))
	}
	if s.Codec != nil {
		effectiveInput = s.Codec.RestoreMap(effectiveInput)
	}
	inBytes, _ := json.Marshal(effectiveInput)
	sig = effectiveName + ":" + string(inBytes)
	return effectiveName, effectiveInput, sig, ""
}

func (s *AcpProjectSession) wouldTripToolLoopLocked(sig string) bool {
	if len(s.RecentToolSigs) < 2 {
		return false
	}
	last2 := s.RecentToolSigs[len(s.RecentToolSigs)-2:]
	return last2[0] == sig && last2[1] == sig
}

func (s *AcpProjectSession) appendToolSigLocked(sig string) {
	s.RecentToolSigs = append(s.RecentToolSigs, sig)
	if len(s.RecentToolSigs) > 6 {
		s.RecentToolSigs = s.RecentToolSigs[len(s.RecentToolSigs)-6:]
	}
}

func (s *AcpProjectSession) readLatestPlannerToolCallsWithWait(sessionID string, afterIdx int) (plannerIdx int, calls []PlannerToolCall) {
	if sessionID == "" {
		return -1, nil
	}
	dbPath := filepath.Join(ConversationsDir(), sessionID+".db")
	if st, err := os.Stat(dbPath); err != nil || st.Size() < 100 {
		return -1, nil
	}
	backoffs := [...]time.Duration{5 * time.Millisecond, 10 * time.Millisecond, 20 * time.Millisecond}
	for attempt := 0; attempt <= len(backoffs); attempt++ {
		if idx, tcs := s.readLatestPlannerToolCalls(sessionID, afterIdx); idx > afterIdx {
			return idx, tcs
		}
		if attempt < len(backoffs) {
			time.Sleep(backoffs[attempt])
		}
	}
	return -1, nil
}

func (s *AcpProjectSession) executeMcpToolCall(ctx context.Context, hostName string, parsedInput map[string]any) map[string]any {
	if hostName == AwaitToolName || hostName == LegacyAwaitToolName {
		RecordMonitorToolDispatch(currentProcessAccountID(), 1)
		targetID, _ := parsedInput["call_id"].(string)
		return s.awaitParkedCall(ctx, strings.TrimSpace(targetID))
	}

	s.mu.Lock()
	effectiveName, effectiveInput, sig, errMsg := s.resolveEffectiveToolLocked(hostName, parsedInput)
	hasPendingSibs := len(s.PendingSiblingCalls) > 0
	hasAbortedSibs := len(s.abortedSiblingCalls) > 0
	sid := s.SessionID
	convKey := s.ConvKey
	s.mu.Unlock()

	if errMsg != "" {
		LogEvent("MCP_TOOL_REJECTED", "Rejected dsh_call_tool for a tool outside the current catalog", map[string]any{
			"convKey": convKey, "sessionId": sid, "toolName": effectiveName,
		})
		return mcpToolError(errMsg)
	}

	curMaxPlannerIdx := -1
	readPlannerIdx := false
	if hasPendingSibs {
		curMaxPlannerIdx = s.readMaxSessionStepIdx(sid)
		readPlannerIdx = true
		s.mu.Lock()
		kept := s.PendingSiblingCalls[:0]
		for _, item := range s.PendingSiblingCalls {
			if curMaxPlannerIdx > item.PlannerIdx && item.PlannerIdx >= 0 {
				continue
			}
			kept = append(kept, item)
		}
		s.PendingSiblingCalls = kept
		matchIdx := -1
		for i, item := range s.PendingSiblingCalls {
			if item.CanonicalSig == sig {
				matchIdx = i
				break
			}
		}
		if matchIdx < 0 && len(s.PendingSiblingCalls) > 0 && s.PendingSiblingCalls[0].Call.Name == effectiveName && curMaxPlannerIdx == s.PendingSiblingCalls[0].PlannerIdx {
			matchIdx = 0
		}
		var matchedSib *ParkedMcpCall
		if matchIdx >= 0 {
			matchedSib = s.PendingSiblingCalls[matchIdx]
			matchedSib.PreDispatched = false
			s.PendingSiblingCalls = append(s.PendingSiblingCalls[:matchIdx], s.PendingSiblingCalls[matchIdx+1:]...)
		}
		s.mu.Unlock()
		if matchedSib != nil {
			s.touchPromptActivity()
			RecordMonitorToolDispatch(currentProcessAccountID(), 1)
			LogEvent("MCP_TOOL_PARKED", "Matched pre-dispatched sibling MCP tool call", map[string]any{
				"convKey":              convKey,
				"sessionId":            sid,
				"toolCallId":           matchedSib.Call.ID,
				"toolName":             matchedSib.Call.Name,
				"siblingPreDispatched": true,
			})
			return s.awaitParkedEntry(ctx, matchedSib)
		}
	}

	if hasAbortedSibs {
		if !readPlannerIdx {
			curMaxPlannerIdx = s.readMaxSessionStepIdx(sid)
		}
		s.mu.Lock()
		kept := s.abortedSiblingCalls[:0]
		for _, item := range s.abortedSiblingCalls {
			if curMaxPlannerIdx > item.PlannerIdx && item.PlannerIdx >= 0 {
				continue
			}
			kept = append(kept, item)
		}
		s.abortedSiblingCalls = kept
		matchIdx := -1
		for i, item := range s.abortedSiblingCalls {
			if item.CanonicalSig == sig {
				matchIdx = i
				break
			}
		}
		if matchIdx < 0 && len(s.abortedSiblingCalls) > 0 && s.abortedSiblingCalls[0].Call.Name == effectiveName && curMaxPlannerIdx == s.abortedSiblingCalls[0].PlannerIdx {
			matchIdx = 0
		}
		var matchedAborted *ParkedMcpCall
		if matchIdx >= 0 {
			matchedAborted = s.abortedSiblingCalls[matchIdx]
			s.abortedSiblingCalls = append(s.abortedSiblingCalls[:matchIdx], s.abortedSiblingCalls[matchIdx+1:]...)
		}
		s.mu.Unlock()
		if matchedAborted != nil {
			s.touchPromptActivity()
			LogEvent("MCP_TOOL_ABORTED_SIBLING_DRAINED", "Drained remaining pre-dispatched sibling MCP tool call from interrupted batch without parking", map[string]any{
				"convKey":    convKey,
				"sessionId":  sid,
				"toolCallId": matchedAborted.Call.ID,
				"toolName":   matchedAborted.Call.Name,
				"plannerIdx": matchedAborted.PlannerIdx,
			})
			return map[string]any{
				"content": []map[string]any{{"type": "text", "text": "Host user interrupted the pending tool execution; follow the updated instruction delivered in this batch."}},
				"isError": true,
			}
		}
	}

	callID := "acp_" + randomHex(16)
	call := ToolCallItem{
		ID:    callID,
		Name:  effectiveName,
		Input: effectiveInput,
	}

	s.mu.Lock()
	if s.closing || s.SessionID == "" || (sid != "" && s.SessionID != sid) || !s.PromptInFlight || s.activeTurn == nil {
		s.mu.Unlock()
		return mcpToolError("MCP tool call aborted: active turn cancelled or session closed.")
	}
	myPromptGen := s.promptGen
	isLoop := s.wouldTripToolLoopLocked(sig)
	if isLoop {
		s.RecentToolSigs = nil
	} else {
		s.appendToolSigLocked(sig)
	}
	lastPlannerIdx := s.LastPlannerBatchIdx
	hasRemainingInStep := lastPlannerIdx >= 0 && s.LastPlannerBatchNextPos > 0 && s.LastPlannerBatchNextPos < s.LastPlannerBatchTotal
	s.mu.Unlock()

	if isLoop {
		LogEvent("TOOL_LOOP_BREAKER", fmt.Sprintf("Intercepted 3x identical consecutive tool call (%s)", effectiveName), map[string]any{
			"convKey":   convKey,
			"sessionId": sid,
		})
		return map[string]any{
			"content": []map[string]any{
				{
					"type": "text",
					"text": fmt.Sprintf("Loop circuit breaker: You have called %s with identical arguments 3 times consecutively. Do not repeat this identical call; proceed with a different approach.", effectiveName),
				},
			},
			"isError": true,
		}
	}

	inBytes, _ := json.Marshal(effectiveInput)
	preview := string(inBytes)
	if len(preview) > 220 {
		preview = preview[:220]
	}
	s.touchPromptActivity()
	RecordMonitorToolDispatch(currentProcessAccountID(), 1)
	LogEvent("MCP_TOOL_PARKED", "Intercepted & parked tool call from upstream ACP", map[string]any{
		"convKey":      convKey,
		"sessionId":    sid,
		"toolCallId":   call.ID,
		"toolName":     call.Name,
		"inputPreview": preview,
	})

	if call.Name == "StructuredOutput" {
		s.enqueueToolCallToTurn(call)
		select {
		case <-ctx.Done():
		case <-time.After(600 * time.Millisecond):
		}
		structSid := sid
		go func() {
			time.Sleep(400 * time.Millisecond)
			s.HibernateIfSession(structSid, "structured_output_terminal")
		}()
		return map[string]any{
			"content": []map[string]any{{"type": "text", "text": "StructuredOutput delivered to host; subagent finished."}},
		}
	}

	var plannerIdx int
	var plannerCalls []PlannerToolCall
	if hasRemainingInStep {
		plannerIdx, plannerCalls = s.readLatestPlannerToolCalls(sid, lastPlannerIdx-1)
		if plannerIdx < 0 {
			plannerIdx, plannerCalls = s.readLatestPlannerToolCallsWithWait(sid, lastPlannerIdx)
		}
	} else {
		plannerIdx, plannerCalls = s.readLatestPlannerToolCallsWithWait(sid, lastPlannerIdx)
	}
	now := time.Now()
	parked := &ParkedMcpCall{
		Call:         call,
		SessionID:    sid,
		ResultChan:   make(chan map[string]any, 1),
		PlannerIdx:   plannerIdx,
		CanonicalSig: sig,
		ParkedAt:     now,
	}

	batch := []ToolCallItem{call}
	fromPlanner := false
	isResumeStep := false
	curPos := -1
	breakIdx := -1
	breakTool := ""
	breakReason := ""
	totalResolved := 0
	s.mu.Lock()
	if s.closing || s.SessionID == "" || (sid != "" && s.SessionID != sid) || !s.PromptInFlight || s.activeTurn == nil || s.promptGen != myPromptGen {
		s.mu.Unlock()
		return mcpToolError("MCP tool call aborted: active turn cancelled or session closed.")
	}
	if len(s.ParkedCalls) == 0 {
		s.ParkedSince = now
	}
	s.ParkedCalls[call.ID] = parked
	isNewStep := plannerIdx > s.LastPlannerBatchIdx
	isResumeStep = !isNewStep && plannerIdx == s.LastPlannerBatchIdx && plannerIdx >= 0 && s.LastPlannerBatchNextPos > 0 && s.LastPlannerBatchNextPos < s.LastPlannerBatchTotal
	if isNewStep || isResumeStep {
		fromPlanner = true
		searchStart := 0
		if isNewStep {
			s.LastPlannerBatchIdx = plannerIdx
			s.LastPlannerBatchNextPos = 0
			s.LastPlannerBatchTotal = 0
			kept := s.PendingSiblingCalls[:0]
			for _, item := range s.PendingSiblingCalls {
				if item.PlannerIdx >= plannerIdx {
					kept = append(kept, item)
				}
			}
			s.PendingSiblingCalls = kept
		} else {
			searchStart = s.LastPlannerBatchNextPos
		}

		type resolvedProposal struct {
			rawName       string
			name          string
			input         map[string]any
			sig           string
			valid         bool
			invalidReason string
		}
		resolvedList := make([]resolvedProposal, 0, len(plannerCalls))
		for _, ptc := range plannerCalls {
			rawHost, allowed := resolveAllowedHostTool(s.McpToHostMap, ptc.RawToolName)
			if !allowed {
				// Skip built-in tools (search_web, read_url_content) executed internally by agy.
				continue
			}
			if rawHost == AwaitToolName || rawHost == LegacyAwaitToolName {
				resolvedList = append(resolvedList, resolvedProposal{
					rawName:       ptc.RawToolName,
					valid:         false,
					invalidReason: "await_helper",
				})
				continue
			}
			ptcArgs := make(map[string]any)
			validArgs := true
			if trimmed := strings.TrimSpace(ptc.ArgsJSON); trimmed != "" && trimmed != "null" {
				if err := unmarshalJSONPreserveNumbers([]byte(trimmed), &ptcArgs); err != nil {
					validArgs = false
				} else if ptcArgs == nil {
					ptcArgs = make(map[string]any)
				}
			}
			if !validArgs {
				resolvedList = append(resolvedList, resolvedProposal{
					rawName:       ptc.RawToolName,
					valid:         false,
					invalidReason: "invalid_args_json",
				})
				continue
			}
			pName, pInput, pSig, pErr := s.resolveEffectiveToolLocked(rawHost, ptcArgs)
			if pErr != "" {
				resolvedList = append(resolvedList, resolvedProposal{
					rawName:       ptc.RawToolName,
					valid:         false,
					invalidReason: "resolve_err",
				})
				continue
			}
			if pName == "StructuredOutput" {
				resolvedList = append(resolvedList, resolvedProposal{
					rawName:       ptc.RawToolName,
					valid:         false,
					invalidReason: "structured_output",
				})
				continue
			}
			if isInternalBridgeTool(pName) {
				resolvedList = append(resolvedList, resolvedProposal{
					rawName:       ptc.RawToolName,
					valid:         false,
					invalidReason: "internal_bridge_tool",
				})
				continue
			}
			resolvedList = append(resolvedList, resolvedProposal{
				rawName: ptc.RawToolName,
				name:    pName,
				input:   pInput,
				sig:     pSig,
				valid:   true,
			})
		}

		totalResolved = len(resolvedList)
		s.LastPlannerBatchTotal = totalResolved
		if searchStart < 0 {
			searchStart = 0
		}
		if searchStart > totalResolved {
			searchStart = totalResolved
		}

		fallbackPos := -1
		for i := searchStart; i < totalResolved; i++ {
			prop := resolvedList[i]
			if !prop.valid {
				continue
			}
			if curPos < 0 && prop.sig == sig {
				curPos = i
			}
			if fallbackPos < 0 && prop.name == effectiveName {
				fallbackPos = i
			}
		}
		if curPos < 0 {
			curPos = fallbackPos
		}

		if curPos >= 0 {
			nextPos := curPos + 1
			for i := curPos + 1; i < totalResolved; i++ {
				prop := resolvedList[i]
				if !prop.valid {
					breakIdx = i
					breakTool = prop.rawName
					breakReason = prop.invalidReason
					break
				}
				if s.wouldTripToolLoopLocked(prop.sig) {
					breakIdx = i
					breakTool = prop.rawName
					breakReason = "loop_breaker"
					nextPos = i + 1
					break
				}
				s.appendToolSigLocked(prop.sig)
				sibCall := ToolCallItem{
					ID:    "acp_" + randomHex(16),
					Name:  prop.name,
					Input: prop.input,
				}
				sibParked := &ParkedMcpCall{
					Call:          sibCall,
					SessionID:     sid,
					ResultChan:    make(chan map[string]any, 1),
					PreDispatched: true,
					PlannerIdx:    plannerIdx,
					CanonicalSig:  prop.sig,
					ParkedAt:      now,
				}
				s.ParkedCalls[sibCall.ID] = sibParked
				s.PendingSiblingCalls = append(s.PendingSiblingCalls, sibParked)
				batch = append(batch, sibCall)
				nextPos = i + 1
			}
			s.LastPlannerBatchNextPos = nextPos
		} else {
			s.LastPlannerBatchNextPos = totalResolved
			if totalResolved > 0 {
				breakTool = effectiveName
				if isResumeStep {
					breakReason = "resume_pos_not_found"
				} else {
					breakReason = "cur_pos_not_found"
				}
			}
		}
	}
	s.mu.Unlock()

	if breakReason != "" || (curPos < 0 && totalResolved > 0) {
		LogEvent("MCP_TOOL_SIBLING_BREAK", fmt.Sprintf("Stopped sibling pre-batching at PlannerResponse step %d (%s)", plannerIdx, breakReason), map[string]any{
			"convKey":       convKey,
			"sessionId":     sid,
			"plannerIdx":    plannerIdx,
			"curPos":        curPos,
			"breakIdx":      breakIdx,
			"totalResolved": totalResolved,
			"batchedCount":  len(batch),
			"rawToolName":   breakTool,
			"reason":        breakReason,
			"resumedStep":   isResumeStep,
		})
	}

	if len(batch) > 1 {
		names := make([]string, 0, len(batch))
		for _, tc := range batch {
			names = append(names, tc.Name)
		}
		LogEvent("MCP_TOOL_SIBLINGS_BATCHED", fmt.Sprintf("Batched %d tool calls from PlannerResponse step %d into single HTTP turn", len(batch), plannerIdx), map[string]any{
			"convKey":    convKey,
			"sessionId":  sid,
			"plannerIdx": plannerIdx,
			"count":      len(batch),
			"tools":      names,
		})
	}
	s.persistSessionAsync()

	s.enqueueToolCallsToTurn(batch, fromPlanner)
	return s.awaitParkedEntry(ctx, parked)
}

func (s *AcpProjectSession) enqueueToolCallToTurn(call ToolCallItem) {
	s.enqueueToolCallsToTurn([]ToolCallItem{call}, false)
}

func (s *AcpProjectSession) enqueueToolCallsToTurn(calls []ToolCallItem, fromPlanner bool) {
	if len(calls) == 0 {
		return
	}
	s.mu.Lock()
	turn := s.activeTurn
	s.mu.Unlock()
	if turn == nil {
		return
	}
	delay := 80 * time.Millisecond
	if fromPlanner {
		delay = 15 * time.Millisecond
	}
	turn.mu.Lock()
	if turn.firstChunk.IsZero() {
		turn.firstChunk = time.Now()
	}
	turn.toolCalls = append(turn.toolCalls, calls...)
	if turn.batchTimer != nil {
		turn.batchTimer.Stop()
	}
	turn.batchTimer = time.AfterFunc(delay, func() {
		s.flushToolBatch(turn)
	})
	turn.mu.Unlock()
}

func (s *AcpProjectSession) flushToolBatch(turn *activeTurnState) {
	if !turn.IsResolved() {
		s.mu.Lock()
		sid := s.SessionID
		s.mu.Unlock()
		s.pollTurnErrorStepsOnce(sid, turn, false)
	}
	turn.mu.Lock()
	if turn.resolved || len(turn.toolCalls) == 0 {
		turn.mu.Unlock()
		return
	}
	if held := turn.finishNoticeLocked(); held != "" {
		turn.textChunks = append(turn.textChunks, held)
		turn.pushDeltaLocked(DeltaChunk{Type: "text", Text: held})
	}
	turn.resolved = true
	turn.upstreamDone = true
	if turn.releaseSem != nil {
		turn.releaseOnce.Do(turn.releaseSem)
	}
	text := strings.Join(turn.textChunks, "")
	thinking := strings.Join(turn.thoughtChks, "")
	if s.Codec != nil {
		text = s.Codec.RestoreText(text)
		thinking = s.Codec.RestoreText(thinking)
	}
	calls := append([]ToolCallItem(nil), turn.toolCalls...)
	firstAt := turn.firstChunk
	if firstAt.IsZero() {
		firstAt = time.Now()
	}
	ttft := int(firstAt.Sub(turn.createdAt).Milliseconds())
	if ttft < 10 {
		ttft = 10
	}
	thinkingMs := turn.computeThinkingMsLocked()
	retryCnt := turn.retryCountLocked()
	turn.mu.Unlock()

	s.mu.Lock()
	s.LastDeliveredCallIDs = s.LastDeliveredCallIDs[:0]
	for _, tc := range calls {
		if p, ok := s.ParkedCalls[tc.ID]; ok {
			p.Delivered = true
			s.LastDeliveredCallIDs = append(s.LastDeliveredCallIDs, tc.ID)
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
	// Bound flushToolBatch usage probe to <=15ms so tool_use delivery is never stalled 255ms;
	// SyncUsageSteps reconciles any delayed SQLite generation rows asynchronously.
	usage := s.normalizeTurnUsageFast(nil, true, 0, outChars)
	select {
	case turn.resolveChan <- TurnResult{
		StopReason:    "tool_use",
		Text:          text,
		Thinking:      thinking,
		ToolCalls:     calls,
		Usage:         usage,
		TtftMs:        ttft,
		ThinkingMs:    thinkingMs,
		RetryCount:    retryCnt,
		EmittedDeltas: emitted,
		FirstChunkAt:  firstAt,
	}:
	default:
	}
}
