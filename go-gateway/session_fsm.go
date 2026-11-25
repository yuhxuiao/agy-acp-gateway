package main

import (
	"time"

	"agy-acp-gateway-go/internal/session"
)

type SessionPhase = session.Phase

const (
	PhaseHibernated = session.PhaseHibernated
	PhaseSpawning   = session.PhaseSpawning
	PhaseIdle       = session.PhaseIdle
	PhaseStreaming  = session.PhaseStreaming
	PhaseHotParked  = session.PhaseHotParked
	PhaseDetached   = session.PhaseDetached
	PhaseClosing    = session.PhaseClosing
)

func (s *AcpProjectSession) phaseInputsLocked() session.PhaseInputs {
	return session.PhaseInputs{
		Closing:           s.closing,
		Spawning:          s.spawning,
		HasDetachedTurn:   s.detachedTurn != nil,
		IsBusy:            s.IsBusy,
		TurnClaimed:       s.turnClaimed,
		HasUnresolvedTurn: s.hasActiveUnresolvedTurnLocked(),
		HasSessionID:      s.SessionID != "",
		PromptInFlight:    s.PromptInFlight,
		HasActiveTurn:     s.activeTurn != nil,
		ParkedCalls:       len(s.ParkedCalls),
	}
}

func (s *AcpProjectSession) syncPhaseLocked() SessionPhase {
	next := session.DerivePhase(s.phaseInputsLocked())
	if next != s.phase {
		s.phase = next
		s.phaseTransitions++
	}
	return next
}

func (s *AcpProjectSession) Phase() SessionPhase {
	if s == nil {
		return PhaseHibernated
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.syncPhaseLocked()
}

func (s *AcpProjectSession) PhaseTransitions() uint64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.syncPhaseLocked()
	return s.phaseTransitions
}

func (s *AcpProjectSession) isAvailableForNewTurnLocked() bool {
	p := s.syncPhaseLocked()
	return (p == PhaseIdle || p == PhaseHibernated) && s.activeTurn == nil
}

func (s *AcpProjectSession) canPass1EvictLocked() bool {
	return s.syncPhaseLocked() == PhaseIdle && s.activeTurn == nil
}

func (s *AcpProjectSession) canPass2ParkedEvictLocked() bool {
	return s.syncPhaseLocked() == PhaseHotParked
}

func (s *AcpProjectSession) isEngineSlotOccupiedLocked() bool {
	p := s.syncPhaseLocked()
	return p == PhaseSpawning || p == PhaseClosing || s.SessionID != ""
}

func (s *AcpProjectSession) isActivelyWorkingLocked() bool {
	p := s.syncPhaseLocked()
	return p == PhaseSpawning || p == PhaseStreaming || p == PhaseDetached || p == PhaseClosing
}

func (s *AcpProjectSession) canResumeHibernatedParkedLocked() bool {
	return len(s.HibernatedParkedCallIDs) > 0 && s.syncPhaseLocked() == PhaseHibernated
}

func (s *AcpProjectSession) beginSpawnLocked() {
	s.spawning = true
	s.syncPhaseLocked()
}

func (s *AcpProjectSession) endSpawnLocked(restore bool) {
	s.spawning = restore
	s.syncPhaseLocked()
}

func (s *AcpProjectSession) beginCloseLocked(doneCh chan struct{}) {
	s.closing = true
	s.closingDone = doneCh
	s.SessionID = ""
	s.PromptInFlight = false
	s.PendingThoughtLevel = ""
	s.activeTurnToolChoice = TurnToolChoice{}
	s.reportedToolCalls = nil
	s.ParkedSince = time.Time{}
	s.syncPhaseLocked()
}

func (s *AcpProjectSession) finishCloseLocked(restoreSid string) {
	s.closing = false
	s.closingDone = nil
	if restoreSid != "" {
		s.SessionID = restoreSid
		s.HibernatedSessionID = ""
	}
	s.syncPhaseLocked()
}

func (s *AcpProjectSession) enterStreamingTurnLocked(tc TurnToolChoice) {
	s.IsBusy = true
	s.activeTurnToolChoice = tc
	s.syncPhaseLocked()
}

func (s *AcpProjectSession) leaveStreamingTurnLocked() {
	if !s.PromptInFlight && len(s.ParkedCalls) == 0 && len(s.RelayedCalls) == 0 {
		s.activeTurnToolChoice = TurnToolChoice{}
	}
	s.IsBusy = false
	s.syncPhaseLocked()
}

func (s *AcpProjectSession) enterDetachedTurnLocked(det *detachedTurnState) {
	s.detachedTurn = det
	s.syncPhaseLocked()
}

func (s *AcpProjectSession) clearDetachedTurnLocked(markBusy bool) {
	s.detachedTurn = nil
	if markBusy {
		s.IsBusy = true
	}
	s.syncPhaseLocked()
}

func (s *AcpProjectSession) demoteToHibernatedLocked(hibSid string) {
	s.SessionID = ""
	s.engineGen = 0
	s.PromptInFlight = false
	s.PendingThoughtLevel = ""
	s.reportedToolCalls = nil
	if hibSid != "" {
		s.HibernatedSessionID = hibSid
	}
	s.syncPhaseLocked()
}
