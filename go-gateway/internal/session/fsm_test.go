package session

import "testing"

func TestDerivePhasePriorityAndHotParkedOrdering(t *testing.T) {
	cases := []struct {
		name string
		in   PhaseInputs
		want Phase
	}{
		{name: "empty hibernated", in: PhaseInputs{}, want: PhaseHibernated},
		{name: "closing overrides spawning and busy", in: PhaseInputs{Closing: true, Spawning: true, IsBusy: true, HasSessionID: true}, want: PhaseClosing},
		{name: "spawning overrides detached and session", in: PhaseInputs{Spawning: true, HasDetachedTurn: true, HasSessionID: true}, want: PhaseSpawning},
		{name: "detached overrides busy", in: PhaseInputs{HasDetachedTurn: true, IsBusy: true, HasSessionID: true}, want: PhaseDetached},
		{name: "busy without sid still streaming", in: PhaseInputs{IsBusy: true}, want: PhaseStreaming},
		{name: "turnClaimed without sid still streaming", in: PhaseInputs{TurnClaimed: true}, want: PhaseStreaming},
		{name: "unresolved turn still streaming even with parked calls", in: PhaseInputs{HasSessionID: true, HasUnresolvedTurn: true, PromptInFlight: true, ParkedCalls: 1}, want: PhaseStreaming},
		{name: "hot parked keeps PromptInFlight and resolved activeTurn", in: PhaseInputs{HasSessionID: true, PromptInFlight: true, HasActiveTurn: true, ParkedCalls: 1}, want: PhaseHotParked},
		{name: "active prompt without parked calls is streaming", in: PhaseInputs{HasSessionID: true, PromptInFlight: true, HasActiveTurn: true}, want: PhaseStreaming},
		{name: "unwinding cancelled prompt without active turn is idle", in: PhaseInputs{HasSessionID: true, PromptInFlight: true}, want: PhaseIdle},
		{name: "live idle", in: PhaseInputs{HasSessionID: true}, want: PhaseIdle},
	}
	for _, tc := range cases {
		if got := DerivePhase(tc.in); got != tc.want {
			t.Fatalf("%s: DerivePhase=%s want=%s", tc.name, got, tc.want)
		}
	}
}

func TestCanTransitionTable(t *testing.T) {
	valid := [][2]Phase{
		{PhaseHibernated, PhaseSpawning},
		{PhaseSpawning, PhaseIdle},
		{PhaseSpawning, PhaseHibernated},
		{PhaseIdle, PhaseStreaming},
		{PhaseStreaming, PhaseHotParked},
		{PhaseHotParked, PhaseStreaming},
		{PhaseStreaming, PhaseDetached},
		{PhaseDetached, PhaseIdle},
		{PhaseIdle, PhaseClosing},
		{PhaseClosing, PhaseHibernated},
		{PhaseHotParked, PhaseHibernated},
	}
	for _, pair := range valid {
		if !CanTransition(pair[0], pair[1]) {
			t.Fatalf("expected %s -> %s to be legal", pair[0], pair[1])
		}
	}
	invalid := [][2]Phase{
		{PhaseHibernated, PhaseHotParked},
		{PhaseHibernated, PhaseDetached},
		{PhaseClosing, PhaseHotParked},
		{PhaseClosing, PhaseDetached},
	}
	for _, pair := range invalid {
		if CanTransition(pair[0], pair[1]) {
			t.Fatalf("expected %s -> %s to be illegal", pair[0], pair[1])
		}
	}
}
