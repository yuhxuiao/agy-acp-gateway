package main

// Opt-in ACP prompt placement. The ACP child and its arguments are unchanged.
// Placement state belongs to an engine generation, not a process-global relay
// or a wall-clock worker deadline.
import (
	gwcfg "agy-acp-gateway-go/internal/config"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"time"
)

func firstPromptPlacement() bool { return gwcfg.Current().PromptPlacement == "first-prompt" }

// ToolsDigest is computed from the raw compact array, preserving producer key
// and array order, unlike re-marshalling through Go maps. Raw bytes are not logged.
func recordPlacementRequest(raw []byte) {
	if !firstPromptPlacement() {
		return
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return
	}
	var tools []json.RawMessage
	_ = json.Unmarshal(fields["tools"], &tools)
	var compact bytes.Buffer
	_ = json.Compact(&compact, fields["tools"])
	requestSum := sha256.Sum256(raw)
	toolsSum := sha256.Sum256(compact.Bytes())
	LogEvent("PLACEMENT_HTTP", "Raw request identity before transformations", map[string]any{"requestBytes": len(raw), "requestSHA256": hex.EncodeToString(requestSum[:]), "toolCount": len(tools), "orderedToolsBytes": compact.Len(), "orderedToolsSHA256": hex.EncodeToString(toolsSum[:])})
}

type placedSession struct {
	appendText string
	pending    bool
	generation uint64
}
type promptPlacement struct {
	mu         sync.Mutex
	sessions   map[string]placedSession
	generation uint64
	retired    bool
}

func (p *promptPlacement) activate(g uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if g > p.generation {
		p.sessions = nil
		p.generation = g
		p.retired = false
	}
	return g == p.generation && !p.retired
}
func (p *promptPlacement) retire(g uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.generation == g {
		p.sessions = nil
		p.retired = true
	}
}
func (p *promptPlacement) forget(sid string, g uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if state, ok := p.sessions[sid]; ok && state.generation == g {
		delete(p.sessions, sid)
	}
}
func (p *promptPlacement) known(sid string, g uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	state, ok := p.sessions[sid]
	return ok && state.generation == g && !p.retired
}

func clonePlacementObject(m map[string]any) map[string]any {
	n := make(map[string]any, len(m))
	for k, v := range m {
		n[k] = v
	}
	return n
}

// Remove only the append leaf; preserve opaque siblings, including empty maps.
// Strings are not normalized, pruned, encoded, trimmed, or split.
func takePlacementAppend(p map[string]any) (map[string]any, string, bool, error) {
	meta, ok := p["_meta"].(map[string]any)
	if !ok {
		return p, "", false, nil
	}
	agy, ok := meta["antigravity"].(map[string]any)
	if !ok {
		return p, "", false, nil
	}
	sys, ok := agy["systemPrompt"].(map[string]any)
	if !ok {
		return p, "", false, nil
	}
	value, exists := sys["append"]
	if !exists {
		return p, "", false, nil
	}
	text, ok := value.(string)
	if !ok {
		return nil, "", false, errors.New("placement append must be a string")
	}
	out := clonePlacementObject(p)
	meta = clonePlacementObject(meta)
	agy = clonePlacementObject(agy)
	sys = clonePlacementObject(sys)
	delete(sys, "append")
	agy["systemPrompt"] = sys
	meta["antigravity"] = agy
	out["_meta"] = meta
	return out, text, true, nil
}

func (p *promptPlacement) call(method string, params any, generation uint64, rpc func(string, any, time.Duration) (json.RawMessage, error), timeout time.Duration) (json.RawMessage, error) {
	obj, ok := params.(map[string]any)
	if !ok {
		return rpc(method, params, timeout)
	}
	sid, _ := obj["sessionId"].(string)
	if !p.activate(generation) {
		if method == "session/close" {
			return nil, nil
		}
		if method == "session/new" || method == "session/prompt" || method == "session/resume" || method == "session/load" {
			return nil, errors.New("placement generation retired; fresh session required")
		}
	}
	switch method {
	case "session/new":
		clean, text, present, err := takePlacementAppend(obj)
		if err != nil {
			return nil, err
		}
		result, err := rpc(method, clean, timeout)
		if err != nil {
			return result, err
		}
		var created struct {
			SessionID string `json:"sessionId"`
		}
		if json.Unmarshal(result, &created) != nil || created.SessionID == "" {
			return nil, errors.New("placement new missing session ID")
		}
		p.mu.Lock()
		if p.generation != generation || p.retired {
			p.mu.Unlock()
			return nil, errors.New("placement new completed after generation retirement")
		}
		if p.sessions == nil {
			p.sessions = make(map[string]placedSession)
		}
		p.sessions[created.SessionID] = placedSession{text, present, generation}
		p.mu.Unlock()
		sum := sha256.Sum256([]byte(text))
		LogEvent("PLACEMENT_NEW", "First-prompt placement prepared", map[string]any{"appendBytes": len(text), "appendSHA256": hex.EncodeToString(sum[:]), "generation": generation})
		return result, nil
	case "session/resume", "session/load":
		p.mu.Lock()
		state, known := p.sessions[sid]
		p.mu.Unlock()
		// No persisted provenance for pre-patch / old-generation sessions. Existing
		// gateway recovery falls back to a fresh session with full host history.
		if !known || state.generation != generation {
			return nil, errors.New("placement provenance unavailable; fresh session required")
		}
		clean, _, _, err := takePlacementAppend(obj)
		if err != nil {
			return nil, err
		}
		return rpc(method, clean, timeout)
	case "session/prompt":
		p.mu.Lock()
		state, known := p.sessions[sid]
		if !known || state.generation != generation {
			p.mu.Unlock()
			return nil, errors.New("placement prompt missing current-generation session")
		}
		out := obj
		injected := state.pending
		if state.pending {
			parts, valid := obj["prompt"].([]map[string]any)
			if !valid {
				p.mu.Unlock()
				return nil, errors.New("placement prompt blocks invalid")
			}
			out = clonePlacementObject(obj)
			blocks := make([]map[string]any, 0, len(parts)+2)
			blocks = append(blocks, map[string]any{"type": "text", "text": "# Pi session instructions\n\n" + state.appendText}, map[string]any{"type": "text", "text": "# Current Pi user request"})
			out["prompt"] = append(blocks, parts...)
			// Consume before dispatch: uncertain transport outcomes must never cause a
			// second injection. Failed prompts are not automatically replayed in mode.
			state.pending = false
			state.appendText = ""
			p.sessions[sid] = state
		}
		p.mu.Unlock()
		LogEvent("PLACEMENT_PROMPT", "Dispatching prompt without automatic resubmission", map[string]any{"injected": injected, "generation": generation})
		result, err := rpc(method, out, timeout)
		if injected && err != nil {
			p.forget(sid, generation)
		}
		return result, err
	case "session/close":
		result, err := rpc(method, params, timeout)
		if isBenignCloseSessionErr(err) {
			p.forget(sid, generation)
		}
		return result, err
	default:
		// initialize, model/thought ACKs, cancel and other state controls remain native.
		return rpc(method, params, timeout)
	}
}
