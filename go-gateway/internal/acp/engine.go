package acp

import (
	"encoding/json"
	"time"
)

// PermissionOption mirrors an ACP session/request_permission option.
type PermissionOption struct {
	OptionID string `json:"optionId"`
	Kind     string `json:"kind"`
}

// SessionListener receives streaming updates, tool permission prompts, and engine exit callbacks for a bound ACP session.
type SessionListener interface {
	SetEngineGen(gen uint64)
	HandleSessionUpdate(u map[string]any)
	HandlePermissionRequest(rpcID any, options []PermissionOption, tc map[string]any)
	OnEngineExited(sid string, exitedGen uint64)
}

// FastSessionUpdateListener is an optional fast path implemented by SessionListener to receive
// high-frequency text/thinking chunks and integer usage updates without allocating map[string]any.
type FastSessionUpdateListener interface {
	HandleFastTextChunk(updType, text string)
	HandleFastUsageUpdate(used, size int, hasUsed, hasSize bool)
}

// Engine abstracts the shared or mockable ACP child process JSON-RPC transport.
type Engine interface {
	EnsureStarted() error
	StopEngine()
	IsAlive() bool
	IsBooting() bool
	CurrentGen() uint64
	Capabilities() Capabilities
	RPC(method string, params any, timeout time.Duration) (json.RawMessage, error)
	RegisterListener(sid string, listener SessionListener)
	UnregisterSession(sid string)
	CloseSession(sid string) error
	CancelSession(sid string)
	EnqueueControlReply(payload map[string]any)
	BinaryInfo() map[string]any
}
