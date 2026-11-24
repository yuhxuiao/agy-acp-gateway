package main

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestPrivatePlacementGenerationRetirement(t *testing.T) {
	var p promptPlacement
	native := func(_ string, _ any, _ time.Duration) (json.RawMessage, error) {
		return json.RawMessage(`{"sessionId":"same"}`), nil
	}
	for g := uint64(1); g <= 4; g++ {
		if _, err := p.call("session/new", placementParams("full"), g, native, time.Second); err != nil {
			t.Fatal(err)
		}
		p.retire(g)
		if len(p.sessions) != 0 || p.known("same", g) {
			t.Fatal("generation not purged")
		}
		if _, err := p.call("session/new", placementParams("late"), g, native, time.Second); err == nil {
			t.Fatal("retired generation resurrected")
		}
	}
	p.call("session/new", placementParams("current"), 5, native, time.Second)
	p.retire(4)
	p.forget("same", 4)
	if !p.known("same", 5) {
		t.Fatal("late old cleanup removed current provenance")
	}
}
func TestPrivatePlacementLateFirstPromptFailure(t *testing.T) {
	var p promptPlacement
	native := func(_ string, _ any, _ time.Duration) (json.RawMessage, error) {
		return json.RawMessage(`{"sessionId":"same"}`), nil
	}
	p.call("session/new", placementParams("old"), 1, native, time.Second)
	delayed := func(_ string, _ any, _ time.Duration) (json.RawMessage, error) {
		p.call("session/new", placementParams("new"), 2, native, time.Second)
		return nil, errors.New("old transport failed")
	}
	p.call("session/prompt", map[string]any{"sessionId": "same", "prompt": []map[string]any{{"type": "text", "text": "user"}}}, 1, delayed, time.Second)
	if !p.known("same", 2) {
		t.Fatal("late prompt failure deleted replacement")
	}
}
func TestPrivatePlacementBenignCloseAndDeadEngine(t *testing.T) {
	var p promptPlacement
	native := func(_ string, _ any, _ time.Duration) (json.RawMessage, error) {
		return json.RawMessage(`{"sessionId":"same"}`), nil
	}
	p.call("session/new", placementParams("full"), 1, native, time.Second)
	missing := func(_ string, _ any, _ time.Duration) (json.RawMessage, error) {
		return nil, errors.New("unknown session")
	}
	p.call("session/close", map[string]any{"sessionId": "same"}, 1, missing, time.Second)
	if p.known("same", 1) {
		t.Fatal("benign close retained append")
	}
	e := newSharedAcpEngine()
	e.gen.Store(1)
	e.placement.call("session/new", placementParams("full"), 1, native, time.Second)
	if err := e.CloseSession("same"); err != nil || e.placement.known("same", 1) {
		t.Fatal("dead-engine close retained provenance")
	}
	e.placement.call("session/new", placementParams("full"), 1, native, time.Second)
	e.StopEngine()
	if len(e.placement.sessions) != 0 {
		t.Fatal("StopEngine retained generation")
	}
}
