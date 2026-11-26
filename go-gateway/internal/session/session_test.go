package session

import (
	"testing"
	"time"
)

func TestClassifyCacheMissSpec(t *testing.T) {
	if got := ClassifyCacheMissSpec(1000, 100000, 500, 101000, 1000, false); got != "" {
		t.Fatalf("expected cache hit empty miss label, got %q", got)
	}
	if got := ClassifyCacheMissSpec(1000, 100000, 50000, 10000, 1000, true); got != "model_switch" {
		t.Fatalf("expected model_switch, got %q", got)
	}
	if got := ClassifyCacheMissSpec(1000, 100000, 50000, 0, 1000, false); got != "shrink" {
		t.Fatalf("expected shrink, got %q", got)
	}
	if got := ClassifyCacheMissSpec(1000, 10000, 50000, 0, 350000, false); got != "gap_ttl" {
		t.Fatalf("expected gap_ttl, got %q", got)
	}
	if got := ClassifyCacheMissSpec(1000, 10000, 50000, 0, 1000, false); got != "zero" {
		t.Fatalf("expected zero, got %q", got)
	}
	if got := ClassifyCacheMissSpec(1000, 50000, 50000, 2000, 1000, false); got != "prefix_only" {
		t.Fatalf("expected prefix_only, got %q", got)
	}
}

func TestCompareLruCandidates(t *testing.T) {
	now := time.Now()
	util := LruCandidate{IsUtil: true, LastUsed: now, ConvKey: "util:1"}
	disposable := LruCandidate{IsUtil: false, Disposable: true, LastUsed: now, ConvKey: "sid:disp"}
	cold := LruCandidate{IsUtil: false, CacheWarm: false, LastUsed: now.Add(-6 * time.Minute), ConvKey: "sid:cold"}
	warm := LruCandidate{IsUtil: false, ProtectedWarm: true, CacheWarm: true, CostScore: 1000, LastUsed: now.Add(-time.Minute), ConvKey: "sid:warm"}
	if !CompareLruCandidates(util, disposable) {
		t.Fatal("util candidate must evict before disposable candidate")
	}
	if !CompareLruCandidates(disposable, cold) {
		t.Fatal("fresh disposable candidate must evict before older cold interactive candidate")
	}
	if !CompareLruCandidates(cold, warm) {
		t.Fatal("cold unprotected candidate must evict before protected warm candidate")
	}
}
