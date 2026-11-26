package main

import (
	"testing"

	"agy-acp-gateway-go/internal/session"
)

func TestA2_SessionLifecycleFSMTransitionsAndPredicates(t *testing.T) {
	s := NewAcpProjectSession("sid:a2-fsm:u:1", "/tmp", "")
	if got := s.Phase(); got != PhaseHibernated {
		t.Fatalf("initial phase = %s, want Hibernated", got)
	}
	if !s.IsAvailableForNewTurn() {
		t.Fatal("initial Hibernated session should be available for new turn")
	}

	assertPhase := func(want SessionPhase, avail, evict1, evict2, occupied, working bool, label string) {
		t.Helper()
		s.mu.Lock()
		got := s.syncPhaseLocked()
		gotAvail := s.isAvailableForNewTurnLocked()
		gotEvict1 := s.canPass1EvictLocked()
		gotEvict2 := s.canPass2ParkedEvictLocked()
		gotOcc := s.isEngineSlotOccupiedLocked()
		gotWork := s.isActivelyWorkingLocked()
		gotLabel := s.sessionStateLabelLocked()
		s.mu.Unlock()
		if got != want {
			t.Fatalf("phase = %s, want %s", got, want)
		}
		if gotAvail != avail || gotEvict1 != evict1 || gotEvict2 != evict2 || gotOcc != occupied || gotWork != working {
			t.Fatalf("phase=%s predicates got (avail=%v ev1=%v ev2=%v occ=%v work=%v), want (%v %v %v %v %v)",
				got, gotAvail, gotEvict1, gotEvict2, gotOcc, gotWork, avail, evict1, evict2, occupied, working)
		}
		if gotLabel != label {
			t.Fatalf("phase=%s label=%q want=%q", got, gotLabel, label)
		}
	}

	// Hibernated -> Spawning
	s.mu.Lock()
	prev := s.syncPhaseLocked()
	s.beginSpawnLocked()
	next := s.syncPhaseLocked()
	s.mu.Unlock()
	if !session.CanTransition(prev, next) {
		t.Fatalf("illegal transition %s -> %s", prev, next)
	}
	assertPhase(PhaseSpawning, false, false, false, true, true, "Detached")

	// Spawning -> Hibernated (boot failure recovery)
	s.mu.Lock()
	s.endSpawnLocked(false)
	s.mu.Unlock()
	assertPhase(PhaseHibernated, true, false, false, false, false, "Detached")

	// Hibernated -> Spawning -> Idle
	s.mu.Lock()
	s.beginSpawnLocked()
	s.SessionID = "sess-a2-1"
	s.endSpawnLocked(false)
	s.mu.Unlock()
	assertPhase(PhaseIdle, true, true, false, true, false, "LiveIdle")

	// Idle -> Streaming (Turn starts)
	turn := newActiveTurnState(nil, func() {})
	s.mu.Lock()
	s.enterStreamingTurnLocked(TurnToolChoice{})
	s.PromptInFlight = true
	s.activeTurn = turn
	s.syncPhaseLocked()
	s.mu.Unlock()
	assertPhase(PhaseStreaming, false, false, false, true, true, "BusyPrompting")

	// Streaming -> HotParked (tool_use resolved activeTurn + ParkedCalls non-empty, IsBusy leaves)
	turn.mu.Lock()
	turn.resolved = true
	turn.mu.Unlock()
	s.mu.Lock()
	s.ParkedCalls["acp_call_1"] = &ParkedMcpCall{
		Call:       ToolCallItem{ID: "acp_call_1", Name: "Read"},
		SessionID:  "sess-a2-1",
		ResultChan: make(chan map[string]any, 1),
	}
	s.leaveStreamingTurnLocked()
	s.mu.Unlock()
	assertPhase(PhaseHotParked, false, false, true, true, false, "ParkedWaitingTool")

	// HotParked -> Streaming -> Idle (tool_result delivered and prompt finishes)
	s.mu.Lock()
	delete(s.ParkedCalls, "acp_call_1")
	s.enterStreamingTurnLocked(TurnToolChoice{})
	s.mu.Unlock()
	assertPhase(PhaseStreaming, false, false, false, true, true, "BusyPrompting")

	s.mu.Lock()
	s.PromptInFlight = false
	s.activeTurn = nil
	s.leaveStreamingTurnLocked()
	s.mu.Unlock()
	assertPhase(PhaseIdle, true, true, false, true, false, "LiveIdle")

	// Idle -> Streaming -> Detached -> Streaming -> Idle
	s.mu.Lock()
	s.enterStreamingTurnLocked(TurnToolChoice{})
	s.enterDetachedTurnLocked(&detachedTurnState{sid: "sess-a2-1", reqFp: "fp-a2"})
	s.mu.Unlock()
	assertPhase(PhaseDetached, false, false, false, true, true, "BusyPrompting")

	s.mu.Lock()
	s.clearDetachedTurnLocked(true)
	s.leaveStreamingTurnLocked()
	s.mu.Unlock()
	assertPhase(PhaseIdle, true, true, false, true, false, "LiveIdle")

	// Idle -> Closing -> Hibernated
	doneCh := make(chan struct{})
	s.mu.Lock()
	s.beginCloseLocked(doneCh)
	s.mu.Unlock()
	assertPhase(PhaseClosing, false, false, false, true, true, "Detached")

	s.mu.Lock()
	s.HibernatedSessionID = "sess-a2-1"
	s.finishCloseLocked("")
	close(doneCh)
	s.mu.Unlock()
	assertPhase(PhaseHibernated, true, false, false, false, false, "Detached")

	if tr := s.PhaseTransitions(); tr < 8 {
		t.Fatalf("expected >= 8 FSM phase transitions recorded, got %d", tr)
	}
}

func TestA2_WhiteBoxFieldSyncAndHotParkedEngineExitDemotion(t *testing.T) {
	// Verify white-box direct field mutations immediately reconcile with syncPhaseLocked().
	s := NewAcpProjectSession("sid:a2-wb:u:1", "/tmp", "")
	s.mu.Lock()
	s.SessionID = "00000000-0000-4000-8000-000000000a21"
	s.engineGen = 7
	s.MessageCount = 4
	s.PromptInFlight = true
	turn := newActiveTurnState(nil, func() {})
	turn.mu.Lock()
	turn.resolved = true
	turn.mu.Unlock()
	s.activeTurn = turn
	s.ParkedCalls["acp_a2_p1"] = &ParkedMcpCall{Call: ToolCallItem{ID: "acp_a2_p1", Name: "Bash"}, ResultChan: make(chan map[string]any, 1)}
	if got := s.syncPhaseLocked(); got != PhaseHotParked {
		s.mu.Unlock()
		t.Fatalf("white-box HotParked sync got %s, want HotParked", got)
	}
	s.mu.Unlock()

	// onEngineExited must demote HotParked -> Hibernated preserving HibernatedParkedCallIDs.
	s.onEngineExited("00000000-0000-4000-8000-000000000a21", 7)
	if got := s.Phase(); got != PhaseHibernated {
		t.Fatalf("expected HotParked -> Hibernated on engine exit, got %s", got)
	}
	s.mu.Lock()
	canResume := s.canResumeHibernatedParkedLocked()
	s.mu.Unlock()
	if !canResume {
		t.Fatal("expected HibernatedParkedCallIDs to be resumable in PhaseHibernated")
	}

	// White-box direct IsBusy=true even without SessionID must report PhaseStreaming and unavailable.
	s2 := NewAcpProjectSession("sid:a2-wb2:u:1", "/tmp", "")
	s2.IsBusy = true
	if got := s2.Phase(); got != PhaseStreaming {
		t.Fatalf("white-box IsBusy=true sync got %s, want Streaming", got)
	}
	if s2.IsAvailableForNewTurn() {
		t.Fatal("session with IsBusy=true must not be available for new turn")
	}
}
