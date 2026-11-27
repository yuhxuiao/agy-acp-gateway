package session

// Phase represents the explicit finite-state-machine lifecycle stage of an ACP session.
type Phase uint8

const (
	PhaseHibernated Phase = iota
	PhaseSpawning
	PhaseIdle
	PhaseStreaming
	PhaseHotParked
	PhaseDetached
	PhaseClosing
)

func (p Phase) String() string {
	switch p {
	case PhaseHibernated:
		return "Hibernated"
	case PhaseSpawning:
		return "Spawning"
	case PhaseIdle:
		return "Idle"
	case PhaseStreaming:
		return "Streaming"
	case PhaseHotParked:
		return "HotParked"
	case PhaseDetached:
		return "Detached"
	case PhaseClosing:
		return "Closing"
	default:
		return "Unknown"
	}
}

// PhaseInputs captures the locked session fields from which the canonical Phase is derived.
type PhaseInputs struct {
	Closing           bool
	Spawning          bool
	HasDetachedTurn   bool
	IsBusy            bool
	TurnClaimed       bool
	HasUnresolvedTurn bool
	HasSessionID      bool
	PromptInFlight    bool
	HasActiveTurn     bool
	ParkedCalls       int
}

// DerivePhase deterministically maps session fields to a single canonical FSM Phase.
// PhaseHotParked precedes PromptInFlight/HasActiveTurn because a hot-parked tool session
// intentionally retains an in-flight prompt RPC and a resolved activeTurn while awaiting client tool output.
func DerivePhase(in PhaseInputs) Phase {
	if in.Closing {
		return PhaseClosing
	}
	if in.Spawning {
		return PhaseSpawning
	}
	if in.HasDetachedTurn {
		return PhaseDetached
	}
	if in.IsBusy || in.TurnClaimed || in.HasUnresolvedTurn {
		return PhaseStreaming
	}
	if in.HasSessionID && in.ParkedCalls > 0 {
		return PhaseHotParked
	}
	if in.HasSessionID && in.HasActiveTurn {
		return PhaseStreaming
	}
	if in.HasSessionID {
		return PhaseIdle
	}
	return PhaseHibernated
}

// CanTransition validates legal lifecycle transitions between explicit FSM phases.
func CanTransition(from, to Phase) bool {
	if from == to {
		return true
	}
	switch from {
	case PhaseHibernated:
		return to == PhaseSpawning || to == PhaseStreaming || to == PhaseClosing
	case PhaseSpawning:
		return to == PhaseIdle || to == PhaseStreaming || to == PhaseHibernated || to == PhaseClosing
	case PhaseIdle:
		return to == PhaseStreaming || to == PhaseSpawning || to == PhaseClosing || to == PhaseHibernated
	case PhaseStreaming:
		return to == PhaseIdle || to == PhaseHotParked || to == PhaseDetached || to == PhaseClosing || to == PhaseHibernated || to == PhaseSpawning
	case PhaseHotParked:
		return to == PhaseStreaming || to == PhaseIdle || to == PhaseClosing || to == PhaseHibernated || to == PhaseSpawning
	case PhaseDetached:
		return to == PhaseStreaming || to == PhaseIdle || to == PhaseClosing || to == PhaseHibernated
	case PhaseClosing:
		return to == PhaseHibernated || to == PhaseIdle || to == PhaseSpawning
	default:
		return false
	}
}
