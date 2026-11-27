package main

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestV2324_MonitorPresetsCustomCapsAndEnvOverrides(t *testing.T) {
	resetMonitorTripsForTest()
	t.Cleanup(func() {
		resetMonitorTripsForTest()
		initConfig()
	})
	t.Setenv("AGY_MONITOR_SOFT_CAP_ULTRA", "")
	t.Setenv("AGY_MONITOR_DRAIN_CAP_ULTRA", "")
	t.Setenv("AGY_MONITOR_HARD_CAP_ULTRA", "")
	t.Setenv("AGY_MONITOR_SOFT_CAP_PRO", "")
	t.Setenv("AGY_MONITOR_DRAIN_CAP_PRO", "")
	t.Setenv("AGY_MONITOR_HARD_CAP_PRO", "")
	initConfig()

	now := time.Now().UTC().Truncate(time.Second)
	uAcc := AccountProfile{ID: "u-preset", PlanTierHint: "ultra"}
	pAcc := AccountProfile{ID: "p-preset", PlanTierHint: "pro"}

	// 1. Default preset is balanced (3300/4250/4800 Ultra, 2100/2900/3400 Pro).
	if s := EvaluateMonitorBudget(uAcc, now); s.Preset != MonitorPresetBalanced || s.SoftCap != 3300 || s.DrainCap != 4250 || s.HardCap != 4800 || s.EffectiveSoftCap != 3300 || s.EffectiveDrainCap != 4250 {
		t.Fatalf("unexpected balanced Ultra snapshot: %+v", s)
	}
	if s := EvaluateMonitorBudget(pAcc, now); s.Preset != MonitorPresetBalanced || s.SoftCap != 2100 || s.DrainCap != 2900 || s.HardCap != 3400 {
		t.Fatalf("unexpected balanced Pro snapshot: %+v", s)
	}
	// Calibration ratios under balanced.
	if cs, cd, ch := calibratedCapsForHard(2000); cs != 1375 || cd != 1771 || ch != 2000 {
		t.Fatalf("unexpected balanced calibrated caps for 2000: %d/%d/%d", cs, cd, ch)
	}

	// 2. Conservative preset (2800/3800/4800 Ultra, 1800/2600/3400 Pro).
	SetRuntimeMonitorPolicy(AccountPoolConfig{MonitorPreset: MonitorPresetConservative})
	if s := EvaluateMonitorBudget(uAcc, now); s.Preset != MonitorPresetConservative || s.SoftCap != 2800 || s.DrainCap != 3800 || s.HardCap != 4800 {
		t.Fatalf("unexpected conservative Ultra snapshot: %+v", s)
	}
	if s := EvaluateMonitorBudget(pAcc, now); s.Preset != MonitorPresetConservative || s.SoftCap != 1800 || s.DrainCap != 2600 || s.HardCap != 3400 {
		t.Fatalf("unexpected conservative Pro snapshot: %+v", s)
	}
	if cs, cd, ch := calibratedCapsForHard(2000); cs != 1200 || cd != 1600 || ch != 2000 {
		t.Fatalf("unexpected conservative calibrated caps for 2000: %d/%d/%d", cs, cd, ch)
	}

	// 3. Edge preset (3600/4450/4800 Ultra, 2300/3100/3400 Pro).
	SetRuntimeMonitorPolicy(AccountPoolConfig{MonitorPreset: MonitorPresetEdge})
	if s := EvaluateMonitorBudget(uAcc, now); s.Preset != MonitorPresetEdge || s.SoftCap != 3600 || s.DrainCap != 4450 || s.HardCap != 4800 {
		t.Fatalf("unexpected edge Ultra snapshot: %+v", s)
	}
	if s := EvaluateMonitorBudget(pAcc, now); s.Preset != MonitorPresetEdge || s.SoftCap != 2300 || s.DrainCap != 3100 || s.HardCap != 3400 {
		t.Fatalf("unexpected edge Pro snapshot: %+v", s)
	}
	if cs, cd, ch := calibratedCapsForHard(2000); cs != 1500 || cd != 1854 || ch != 2000 {
		t.Fatalf("unexpected edge calibrated caps for 2000: %d/%d/%d", cs, cd, ch)
	}

	// 4. Custom preset with explicit overrides + safe normalization.
	SetRuntimeMonitorPolicy(AccountPoolConfig{
		MonitorPreset:        MonitorPresetCustom,
		MonitorSoftCapUltra:  3100,
		MonitorDrainCapUltra: 4100,
		MonitorHardCapUltra:  4700,
		MonitorSoftCapPro:    1950,
		MonitorDrainCapPro:   2750,
		MonitorHardCapPro:    3300,
	})
	if s := EvaluateMonitorBudget(uAcc, now); s.Preset != MonitorPresetCustom || s.SoftCap != 3100 || s.DrainCap != 4100 || s.HardCap != 4700 {
		t.Fatalf("unexpected custom Ultra snapshot: %+v", s)
	}
	if s := EvaluateMonitorBudget(pAcc, now); s.Preset != MonitorPresetCustom || s.SoftCap != 1950 || s.DrainCap != 2750 || s.HardCap != 3300 {
		t.Fatalf("unexpected custom Pro snapshot: %+v", s)
	}

	// 5. Explicit env overrides remain respected over runtime presets.
	t.Setenv("AGY_MONITOR_SOFT_CAP_ULTRA", "2700")
	t.Setenv("AGY_MONITOR_DRAIN_CAP_PRO", "2850")
	initConfig()
	SetRuntimeMonitorPolicy(AccountPoolConfig{MonitorPreset: MonitorPresetEdge})
	if s := EvaluateMonitorBudget(uAcc, now); s.SoftCap != 2700 || s.DrainCap != 4450 || s.HardCap != 4800 {
		t.Fatalf("expected AGY_MONITOR_SOFT_CAP_ULTRA=2700 env override on edge preset, got %+v", s)
	}
	if s := EvaluateMonitorBudget(pAcc, now); s.SoftCap != 2300 || s.DrainCap != 2850 || s.HardCap != 3400 {
		t.Fatalf("expected AGY_MONITOR_DRAIN_CAP_PRO=2850 env override on edge preset, got %+v", s)
	}
	t.Setenv("AGY_MONITOR_SOFT_CAP_ULTRA", "")
	t.Setenv("AGY_MONITOR_DRAIN_CAP_PRO", "")
	initConfig()

	// 6. AccountPoolConfig.ForAPI() ALWAYS emits concrete effective JSON values + round-trip via POST /api/accounts/routing & accounts.json reload.
	am := setupV2318FiveAccountPool(t, true)
	postReq := httptest.NewRequest(http.MethodPost, "/api/accounts/routing", strings.NewReader(`{
		"strategy": "round-robin",
		"monitorPreset": "edge",
		"monitorSmartPacing": false,
		"monitorSoftRedHop": true,
		"monitorSoftCapUltra": 2999
	}`))
	postReq.Header.Set("Content-Type", "application/json")
	postReq.Header.Set("X-Agy-Requested-With", "dashboard")
	rec := httptest.NewRecorder()
	HandleAccountsAPI(rec, postReq)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /api/accounts/routing status=%d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		OK      bool           `json:"ok"`
		Routing map[string]any `json:"routing"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil || !resp.OK {
		t.Fatalf("decode routing response: err=%v body=%s", err, rec.Body.String())
	}
	// Stale custom int 2999 MUST be ignored when monitorPreset == "edge", and ForAPI() MUST emit concrete edge caps!
	wantFields := map[string]any{
		"monitorPreset":        "edge",
		"monitorSmartPacing":   false,
		"monitorSoftRedHop":    true,
		"monitorSoftCapUltra":  float64(3600),
		"monitorDrainCapUltra": float64(4450),
		"monitorHardCapUltra":  float64(4800),
		"monitorSoftCapPro":    float64(2300),
		"monitorDrainCapPro":   float64(3100),
		"monitorHardCapPro":    float64(3400),
	}
	for k, want := range wantFields {
		if resp.Routing[k] != want {
			t.Fatalf("routing[%q]=%#v, want %#v (full=%+v)", k, resp.Routing[k], want, resp.Routing)
		}
	}

	// Reject unknown monitorPreset cleanly.
	badReq := httptest.NewRequest(http.MethodPost, "/api/accounts/routing", strings.NewReader(`{"strategy":"round-robin","monitorPreset":"turbo"}`))
	badReq.Header.Set("Content-Type", "application/json")
	badReq.Header.Set("X-Agy-Requested-With", "dashboard")
	badRec := httptest.NewRecorder()
	HandleAccountsAPI(badRec, badReq)
	if badRec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid monitorPreset, got %d body=%s", badRec.Code, badRec.Body.String())
	}

	// Reload from disk restores preset + booleans + updates runtime monitor policy.
	resetMonitorTripsForTest()
	reloaded := NewAccountManager()
	reloaded.filePath = am.filePath
	reloaded.mu.Lock()
	okLoad := reloaded.loadFromDiskLocked()
	reloaded.mu.Unlock()
	if !okLoad {
		t.Fatal("failed to reload accounts.json")
	}
	rCfg := reloaded.GetRoutingConfig()
	if rCfg.EffectiveMonitorPreset() != MonitorPresetEdge || rCfg.MonitorSmartPacingEnabled() || !rCfg.MonitorSoftRedHopEnabled() {
		t.Fatalf("reloaded routing config lost v3 fields: %+v", rCfg)
	}
	if s := EvaluateMonitorBudget(uAcc, now); s.Preset != MonitorPresetEdge || s.DrainCap != 4450 {
		t.Fatalf("loadFromDiskLocked did not activate runtime monitor policy: %+v", s)
	}
}

func TestV2324_SmartPacingDynamicCapsAndDriftBuffer(t *testing.T) {
	resetMonitorTripsForTest()
	t.Cleanup(resetMonitorTripsForTest)
	initConfig()

	now := time.Date(2026, time.November, 27, 12, 0, 0, 0, time.UTC)
	uAcc := AccountProfile{ID: "u-pace", PlanTierHint: "ultra"}

	// 1. Balanced preset (3300/4250/4800, safeCap=4464) with authoritative liteResetAt in 5m (NO +20m buffer):
	//    effRate = max(0, 600) = 600, projNeed = ceil(600 * (5/60) * 1.5) = 75
	//    effDrain = 4464 - 75 = 4389, effSoft = 4389 - (4250 - 3300) = 3439, smartPacingActive = true!
	RecordMonitorLiteResetAt(uAcc.ID, now.Add(5*time.Minute), now)
	SyncMonitorBudgetCalls(uAcc.ID, 4300, now.Add(5*time.Minute).Format(time.RFC3339), now)
	s5mLite := EvaluateMonitorBudget(uAcc, now)
	if !s5mLite.SmartPacingActive || s5mLite.EffectiveDrainCap != 4389 || s5mLite.EffectiveSoftCap != 3439 {
		t.Fatalf("expected Smart Pacing 5m liteResetAt -> effSoft=3439 effDrain=4389 active=true, got %+v", s5mLite)
	}
	if s5mLite.Zone != monitorZoneYellow {
		t.Fatalf("4300 calls with effDrain=4389 must evaluate Yellow (not Red), got %+v", s5mLite)
	}

	// AccountHealthView mirrors all 4 additive fields.
	hv := buildSingleHealthViewFromRuntimeWithMonitor(uAcc, true, 0, 0, nil, false, "", s5mLite, now)
	if !hv.MonitorSmartPacingActive || hv.MonitorEffectiveSoftCap != 3439 || hv.MonitorEffectiveDrainCap != 4389 || hv.MonitorPreset != MonitorPresetBalanced {
		t.Fatalf("AccountHealthView did not mirror Smart Pacing fields: %+v", hv)
	}

	// 2. +20m reverse-drift safety buffer when ONLY windowResetAt is known (no liteResetAt):
	//    At 5m remaining: remEff = 25m -> projNeed = ceil(600 * (25/60) * 1.5) = 375 -> 4464 - 375 = 4089.
	//    - Under balanced (baseDrain=4250), 4089 clamps to 4250 (SmartPacingActive=false -> protects against reverse drift!).
	//    - Under conservative (baseDrain=3800), 4089 > 3800 -> effDrain=4089, effSoft=3089, SmartPacingActive=true!
	uWin := AccountProfile{ID: "u-pace-win", PlanTierHint: "ultra"}
	SyncMonitorBudgetCalls(uWin.ID, 3950, now.Add(5*time.Minute).Format(time.RFC3339), now)
	s5mWinBal := EvaluateMonitorBudget(uWin, now)
	if s5mWinBal.SmartPacingActive || s5mWinBal.EffectiveDrainCap != 4250 {
		t.Fatalf("balanced with +20m drift buffer at 5m left should stay at baseDrain 4250, got %+v", s5mWinBal)
	}
	SetRuntimeMonitorPolicy(AccountPoolConfig{MonitorPreset: MonitorPresetConservative})
	s5mWinCons := EvaluateMonitorBudget(uWin, now)
	if !s5mWinCons.SmartPacingActive || s5mWinCons.EffectiveDrainCap != 4089 || s5mWinCons.EffectiveSoftCap != 3089 || s5mWinCons.Zone != monitorZoneYellow {
		t.Fatalf("conservative with +20m drift buffer at 5m left expected effSoft=3089 effDrain=4089 yellow, got %+v", s5mWinCons)
	}

	// 3. 20m remaining (windowResetAt only -> remEff=40m -> projNeed=600 -> effDrain=3864 on conservative).
	u20m := AccountProfile{ID: "u-pace-20m", PlanTierHint: "ultra"}
	SyncMonitorBudgetCalls(u20m.ID, 3820, now.Add(20*time.Minute).Format(time.RFC3339), now)
	s20m := EvaluateMonitorBudget(u20m, now)
	if !s20m.SmartPacingActive || s20m.EffectiveDrainCap != 3864 || s20m.EffectiveSoftCap != 2864 || s20m.Zone != monitorZoneYellow {
		t.Fatalf("conservative at 20m remaining (+20m buffer) expected effSoft=2864 effDrain=3864 yellow, got %+v", s20m)
	}

	// 4. 3h remaining: projNeed >> safeCap - baseDrain -> clamps cleanly to baseDrain (SmartPacingActive=false).
	u3h := AccountProfile{ID: "u-pace-3h", PlanTierHint: "ultra"}
	SyncMonitorBudgetCalls(u3h.ID, 3850, now.Add(3*time.Hour).Format(time.RFC3339), now)
	s3h := EvaluateMonitorBudget(u3h, now)
	if s3h.SmartPacingActive || s3h.EffectiveDrainCap != 3800 || s3h.Zone != monitorZoneRed {
		t.Fatalf("at 3h remaining Smart Pacing must not relax baseDrain, got %+v", s3h)
	}

	// 5. High burn rate clamps dynamic relaxation back down to baseDrain.
	uBurst := AccountProfile{ID: "u-pace-burst", PlanTierHint: "ultra"}
	RecordMonitorLiteResetAt(uBurst.ID, now.Add(15*time.Minute), now)
	// 600 calls in last 15m -> 2400/hr -> projNeed = ceil(2400 * 0.25 * 1.5) = 900 -> 4464 - 900 = 3564 < 3800
	RecordMonitorToolDispatchAt(uBurst.ID, 600, now.Add(-5*time.Minute))
	sBurst := EvaluateMonitorBudget(uBurst, now)
	if sBurst.SmartPacingActive || sBurst.EffectiveDrainCap != 3800 {
		t.Fatalf("high burn rate (2400/hr) must prevent premature Smart Pacing relaxation, got %+v", sBurst)
	}

	// 6. Disabling monitorSmartPacing turns off dynamic relaxation completely.
	paceFalse := false
	SetRuntimeMonitorPolicy(AccountPoolConfig{MonitorPreset: MonitorPresetConservative, MonitorSmartPacing: &paceFalse})
	sOff := EvaluateMonitorBudget(uWin, now)
	if sOff.SmartPacingActive || sOff.EffectiveDrainCap != 3800 || sOff.EffectiveSoftCap != 2800 || sOff.Zone != monitorZoneRed {
		t.Fatalf("monitorSmartPacing=false must evaluate strictly against base caps 2800/3800, got %+v", sOff)
	}

	// 7. Unknown reset time keeps SmartPacingActive=false.
	paceTrue := true
	SetRuntimeMonitorPolicy(AccountPoolConfig{MonitorPreset: MonitorPresetConservative, MonitorSmartPacing: &paceTrue})
	uUnknown := AccountProfile{ID: "u-pace-unk", PlanTierHint: "ultra"}
	RecordMonitorToolDispatchAt(uUnknown.ID, 3850, now)
	sUnk := EvaluateMonitorBudget(uUnknown, now)
	if sUnk.SmartPacingActive || sUnk.EffectiveDrainCap != 3800 || sUnk.Zone != monitorZoneRed {
		t.Fatalf("unknown reset time must keep SmartPacingActive=false, got %+v", sUnk)
	}
}

func TestV2324_SoftRedStickyPreservationVsReal429AndEmergencyHop(t *testing.T) {
	am := setupV2318FiveAccountPool(t, true) // u1/u2/u3 primary P10 Ultra, p1/p2 standby P20 Pro
	now := time.Now()
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	req.Header.Set("Authorization", "Bearer master-v2318-key")

	tierTrue := true
	spillFalse := false
	if _, err := am.UpdateRoutingConfig(AccountPoolConfig{
		Strategy:             RouteStrategyFillFirst,
		PriorityTiering:      &tierTrue,
		MonitorSpillOnYellow: &spillFalse,
	}); err != nil {
		t.Fatalf("UpdateRoutingConfig: %v", err)
	}

	// Seed sticky session on u1.
	seed := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:c1-sticky", HasTools: true})
	if seed.DenyErr != nil || seed.Account.ID != "u1" {
		t.Fatalf("expected sticky seed on u1, got %q err=%v", seed.Account.ID, seed.DenyErr)
	}

	var cancelAcc, cancelReason string
	am.setMonitorTripHopCancelHookForTest(func(accID, _ string, _ []string, reason string) {
		cancelAcc = accID
		cancelReason = reason
	})

	// 1. Push u1 into Soft-Red (4350 >= 4250, < 4550 emergency hop ceiling); u2 & u3 are Green.
	//    - NEW tool session MUST skip Soft-Red u1 and route to Green u2/u3.
	//    - Existing U8 sticky continuation on u1 MUST STAY PINNED (monitorSoftRedHop=false default)!
	RecordMonitorToolDispatchAt("u1", 4350, now)
	newPick := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:c1-new", HasTools: true})
	if newPick.DenyErr != nil || newPick.Account.ID == "u1" || newPick.Account.IsStandby() {
		t.Fatalf("new tool session must avoid Soft-Red u1 and pick healthy primary peer, got %q err=%v", newPick.Account.ID, newPick.DenyErr)
	}
	stickyKeep := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:c1-sticky", IsContinuation: true, HasTools: true})
	if stickyKeep.DenyErr != nil || stickyKeep.Account.ID != "u1" || !stickyKeep.IsStickyHit || cancelAcc != "" {
		t.Fatalf("C1: sticky continuation on Soft-Red u1 (4350 < 4550) must stay pinned, got acc=%q sticky=%v cancel=%q err=%v", stickyKeep.Account.ID, stickyKeep.IsStickyHit, cancelAcc, stickyKeep.DenyErr)
	}

	// 2. Push u2 & u3 into Soft-Red too so ONLY Standby Pros p1/p2 are Green, then push u1 to emergency ceiling 4560 (>= 4800-250=4550):
	//    Because NO same-tier (Primary) healthy peer exists, u1 sticky MUST NOT forcibly hop to Standby Pro!
	RecordMonitorToolDispatchAt("u2", 4350, now)
	RecordMonitorToolDispatchAt("u3", 4350, now)
	RecordMonitorToolDispatchAt("u1", 210, now) // u1 = 4560 >= 4550
	stickyNoPeer := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:c1-sticky", IsContinuation: true, HasTools: true})
	if stickyNoPeer.DenyErr != nil || stickyNoPeer.Account.ID != "u1" || !stickyNoPeer.IsStickyHit || cancelAcc != "" {
		t.Fatalf("C1: emergency cap without same-tier Primary healthy peer must NOT hop to Standby Pro, got acc=%q sticky=%v cancel=%q err=%v", stickyNoPeer.Account.ID, stickyNoPeer.IsStickyHit, cancelAcc, stickyNoPeer.DenyErr)
	}

	// 3. Recover u2 (Primary, same-tier) to Green -> NOW u1 at 4560 (>= 4550) MUST hop to u2 and cancel parked session!
	resetAccountMonitorBudget("u2", false)
	hopEmerg := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:c1-sticky", IsContinuation: true, HasTools: true})
	if hopEmerg.DenyErr != nil || hopEmerg.Account.ID != "u2" || hopEmerg.IsStickyHit {
		t.Fatalf("C1: emergency cap with healthy same-tier Primary u2 must hop to u2, got acc=%q sticky=%v err=%v", hopEmerg.Account.ID, hopEmerg.IsStickyHit, hopEmerg.DenyErr)
	}
	if cancelAcc != "u1" || cancelReason != "monitor_emergency_hop_cancel" {
		t.Fatalf("expected monitor_emergency_hop_cancel on u1, got acc=%q reason=%q", cancelAcc, cancelReason)
	}

	// 4. Real 429 Tripped on u2 (even when u1/u3 are also tripped so only Standby Pro is left):
	//    MUST forcibly hop off tripped u2 onto Standby Pro!
	cancelAcc, cancelReason = "", ""
	setMonitorTripForTest("u1", now.Add(time.Hour))
	setMonitorTripForTest("u2", now.Add(time.Hour))
	setMonitorTripForTest("u3", now.Add(time.Hour))
	hopTrip := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:c1-sticky", IsContinuation: true, HasTools: true})
	if hopTrip.DenyErr != nil || !hopTrip.Account.IsStandby() || hopTrip.IsStickyHit {
		t.Fatalf("real 429 Tripped must hop even to Standby Pro when no Primary peer exists, got acc=%q sticky=%v err=%v", hopTrip.Account.ID, hopTrip.IsStickyHit, hopTrip.DenyErr)
	}
	if cancelAcc != "u2" || cancelReason != "monitor_trip_hop_cancel" {
		t.Fatalf("expected monitor_trip_hop_cancel on u2, got acc=%q reason=%q", cancelAcc, cancelReason)
	}
}

func TestV2324_PrimaryOverStandbyBelowEmergencyCapAndHeadroomFallback(t *testing.T) {
	am := setupV2318FiveAccountPool(t, true) // u1/u2/u3 Primary Ultra P10, p1/p2 Standby Pro P20
	now := time.Now()
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	req.Header.Set("Authorization", "Bearer master-v2318-key")

	tierTrue := true
	spillFalse := false
	spreadFalse := false
	if _, err := am.UpdateRoutingConfig(AccountPoolConfig{
		Strategy:             RouteStrategyRoundRobin,
		PriorityTiering:      &tierTrue,
		MonitorSpillOnYellow: &spillFalse,
		MonitorBudgetSpread:  &spreadFalse,
	}); err != nil {
		t.Fatalf("UpdateRoutingConfig: %v", err)
	}

	// 1. C2 + C3: All 3 Primary Ultras are Soft-Red below emergencyCap (4500) with different headrooms:
	//    u1=4430 (headroom 370), u2=4280 (headroom 520, HIGHEST), u3=4470 (headroom 330).
	//    Standby Pros p1/p2 are Green (0 calls).
	//    - Must keep traffic on Primary Ultras (0 spills to Standby Pro!).
	//    - Must pick u2 (highest remaining monitor headroom) instead of blind round-robin!
	RecordMonitorToolDispatchAt("u1", 4430, now)
	RecordMonitorToolDispatchAt("u2", 4280, now)
	RecordMonitorToolDispatchAt("u3", 4470, now)
	for i := 0; i < 4; i++ {
		pick := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{
			SessionKey: "hdr:c2c3-sub-" + strconv.Itoa(i),
			HasTools:   true,
		})
		if pick.DenyErr != nil || pick.Account.ID != "u2" {
			t.Fatalf("C2+C3 #%d: expected Primary u2 (max headroom 520 < emergencyCap) over Standby Pro and lower-headroom peers, got %q err=%v", i, pick.Account.ID, pick.DenyErr)
		}
	}

	// Once u2 absorbs calls and drops below u1's headroom (u2=4460 headroom 340 < u1=4430 headroom 370), u1 takes over!
	RecordMonitorToolDispatchAt("u2", 180, now) // u2=4460
	pickNext := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:c2c3-shift", HasTools: true})
	if pickNext.DenyErr != nil || pickNext.Account.ID != "u1" {
		t.Fatalf("C3: after u2 headroom drops below u1, expected u1 (headroom 370), got %q err=%v", pickNext.Account.ID, pickNext.DenyErr)
	}

	// 2. Push all 3 Primary Ultras to >= emergencyCap (4500) -> NOW spills to Standby Pro!
	RecordMonitorToolDispatchAt("u1", 100, now) // 4530 >= 4500
	RecordMonitorToolDispatchAt("u2", 100, now) // 4560 >= 4500
	RecordMonitorToolDispatchAt("u3", 100, now) // 4570 >= 4500
	pickSpill := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:c2-emerg-spill", HasTools: true})
	if pickSpill.DenyErr != nil || !pickSpill.Account.IsStandby() {
		t.Fatalf("C2: once Primary Ultras >= emergencyCap(4500), must spill to Standby Pro, got %q err=%v", pickSpill.Account.ID, pickSpill.DenyErr)
	}

	// 3. C3 fallback when EVERY account (including Standby Pros) is Soft-Red:
	//    First, with p1=3200 (>= Pro emergencyCap 3100) and p2=3000 (< Pro emergencyCap 3100),
	//    p2 still has sub-emergency headroom while Ultras are >= 4500 -> p2 absorbs spillover safely.
	RecordMonitorToolDispatchAt("p1", 3200, now)
	RecordMonitorToolDispatchAt("p2", 3000, now)
	pickSubStandby := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{
		SessionKey: "hdr:c3-sub-standby",
		HasTools:   true,
	})
	if pickSubStandby.DenyErr != nil || pickSubStandby.Account.ID != "p2" {
		t.Fatalf("expected sub-emergency standby p2 (3000 < 3100) to absorb spillover when all Ultras >= 4500, got %q err=%v", pickSubStandby.Account.ID, pickSubStandby.DenyErr)
	}
	//    Now push p2 past its emergencyCap (3150 >= 3100) so ALL accounts enter monitorRedIdealScored fallback:
	//    Primary tier wins over Standby, and within Primary tier u1 (headroom 270) beats u2 (240) and u3 (230) deterministically under round-robin!
	RecordMonitorToolDispatchAt("p2", 150, now) // 3150 >= 3100
	for i := 0; i < 4; i++ {
		pickAllRed := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{
			SessionKey: "hdr:c3-allred-" + strconv.Itoa(i),
			HasTools:   true,
		})
		if pickAllRed.DenyErr != nil || pickAllRed.Account.ID != "u1" {
			t.Fatalf("C3 all-Soft-Red #%d: expected highest-headroom primary u1 (headroom 270), got %q err=%v", i, pickAllRed.Account.ID, pickAllRed.DenyErr)
		}
	}

	// 4. Verify MonitorSpillOnYellow=true STILL spills Soft-Red Primary (< emergencyCap) to Standby Pro!
	resetAccountMonitorBudget("p1", false)
	resetAccountMonitorBudget("u1", false)
	resetAccountMonitorBudget("u2", false)
	resetAccountMonitorBudget("u3", false)
	RecordMonitorToolDispatchAt("u1", 4350, now)
	RecordMonitorToolDispatchAt("u2", 4350, now)
	RecordMonitorToolDispatchAt("u3", 4350, now)
	spillTrue := true
	if _, err := am.UpdateRoutingConfig(AccountPoolConfig{
		Strategy:             RouteStrategyRoundRobin,
		PriorityTiering:      &tierTrue,
		MonitorSpillOnYellow: &spillTrue,
	}); err != nil {
		t.Fatalf("re-enable spillOnYellow: %v", err)
	}
	pickSpillTrue := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:c2-spill-true", HasTools: true})
	if pickSpillTrue.DenyErr != nil || !pickSpillTrue.Account.IsStandby() {
		t.Fatalf("MonitorSpillOnYellow=true must spill Soft-Red Primaries to Green Standby Pro, got %q err=%v", pickSpillTrue.Account.ID, pickSpillTrue.DenyErr)
	}
}

func TestV2324_RuntimePolicyPropagationToChildWorker(t *testing.T) {
	am := setupV2318FiveAccountPool(t, false)
	t.Setenv("AGY_TOOL_RELAY_MS", "")
	t.Setenv("AGY_TOOL_RELAY_SEC", "")
	initConfig()

	gs := NewGatewayServer()
	defer gs.Close()

	// 1. Direct handler loopback guard: non-loopback rejected with 403.
	reqNonLoop := httptest.NewRequest(http.MethodPost, "/api/internal/monitor-policy", strings.NewReader(`{"monitorPreset":"edge"}`))
	reqNonLoop.Host = "127.0.0.1"
	reqNonLoop.RemoteAddr = "203.0.113.9:54321"
	reqNonLoop.Header.Set("x-api-key", "master-v2318-key")
	rrNonLoop := httptest.NewRecorder()
	gs.DashboardOnlyHandler().ServeHTTP(rrNonLoop, reqNonLoop)
	if rrNonLoop.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for non-loopback /api/internal/monitor-policy, got %d body=%s", rrNonLoop.Code, rrNonLoop.Body.String())
	}

	// 2. Real loopback HTTP server representing child worker dashboard endpoint:
	//    Verify live policy update via POST /api/internal/monitor-policy immediately shifts toolRelayIntervalForTool!
	childSrv := httptest.NewServer(gs.DashboardOnlyHandler())
	defer childSrv.Close()
	u, _ := url.Parse(childSrv.URL)
	_, portStr, _ := net.SplitHostPort(u.Host)
	childPort, _ := strconv.Atoi(portStr)

	// Start with 3000 calls on current process: under balanced (soft=3300), 3000 is Green (>=50% -> 75s).
	RecordMonitorToolDispatchAt(currentProcessAccountID(), 3000, time.Now())
	if got := toolRelayIntervalForTool(0, "Read", 64000); got != 75*time.Second {
		t.Fatalf("expected 75s at 3000 calls under balanced (Green >=50%%), got %v", got)
	}

	// Wire childSrv + spySrv as running child workers and update routing to conservative!
	var gotPolicyMu sync.Mutex
	var gotPolicies []MonitorPolicy
	spySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/api/internal/monitor-policy" {
			var p MonitorPolicy
			if err := json.NewDecoder(r.Body).Decode(&p); err == nil {
				gotPolicyMu.Lock()
				gotPolicies = append(gotPolicies, p)
				gotPolicyMu.Unlock()
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"ok":true}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer spySrv.Close()
	su, _ := url.Parse(spySrv.URL)
	_, spyPortStr, _ := net.SplitHostPort(su.Host)
	spyPort, _ := strconv.Atoi(spyPortStr)

	am.mu.Lock()
	am.workers["u2"] = &accountWorkerProc{id: "u2", dashPort: childPort}
	am.workers["u3"] = &accountWorkerProc{id: "u3", dashPort: spyPort}
	am.mu.Unlock()
	t.Cleanup(func() {
		am.mu.Lock()
		delete(am.workers, "u2")
		delete(am.workers, "u3")
		am.mu.Unlock()
	})

	// Update routing to conservative (soft=2800) -> propagates to both childSrv and spySrv!
	paceFalse := false
	if _, err := am.UpdateRoutingConfig(AccountPoolConfig{
		Strategy:           RouteStrategyRoundRobin,
		MonitorPreset:      MonitorPresetConservative,
		MonitorSmartPacing: &paceFalse,
	}); err != nil {
		t.Fatalf("UpdateRoutingConfig(conservative): %v", err)
	}

	gotPolicyMu.Lock()
	nRec := len(gotPolicies)
	var lastRec MonitorPolicy
	if nRec > 0 {
		lastRec = gotPolicies[nRec-1]
	}
	gotPolicyMu.Unlock()
	if nRec == 0 || lastRec.Preset != MonitorPresetConservative || lastRec.SmartPacing == nil || *lastRec.SmartPacing {
		t.Fatalf("expected UpdateRoutingConfig to propagate conservative/smartPacing=false to child worker, got count=%d last=%+v", nRec, lastRec)
	}

	// Re-check toolRelayIntervalForTool: now 3000 >= 2800 (Yellow under conservative) -> immediately widens to 135s!
	if got := toolRelayIntervalForTool(0, "Read", 64000); got != 135*time.Second {
		t.Fatalf("expected child toolRelayIntervalForTool to immediately widen to 135s after conservative policy push, got %v", got)
	}
}

func TestV2324_IgnoreBurnAnomalySuppressionAndCRUD(t *testing.T) {
	am := setupV2318FiveAccountPool(t, false)
	t0 := time.Now().UTC().Truncate(time.Second)
	fPtr := func(v float64) *float64 { return &v }

	// 1. Update u1 via PUT /api/accounts/u1 with ignoreBurnAnomaly=true and verify GET /api/accounts + disk reload.
	u1, ok := am.GetAccount("u1")
	if !ok {
		t.Fatal("missing u1")
	}
	u1.IgnoreBurnAnomaly = true
	bodyBytes, _ := json.Marshal(u1)
	putReq := httptest.NewRequest(http.MethodPut, "/api/accounts/u1", strings.NewReader(string(bodyBytes)))
	putReq.Header.Set("Content-Type", "application/json")
	putReq.Header.Set("X-Agy-Requested-With", "dashboard")
	recPut := httptest.NewRecorder()
	HandleAccountsAPI(recPut, putReq)
	if recPut.Code != http.StatusOK {
		t.Fatalf("PUT /api/accounts/u1 status=%d body=%s", recPut.Code, recPut.Body.String())
	}

	recGet := httptest.NewRecorder()
	HandleAccountsAPI(recGet, httptest.NewRequest(http.MethodGet, "/api/accounts", nil))
	if recGet.Code != http.StatusOK {
		t.Fatalf("GET /api/accounts status=%d body=%s", recGet.Code, recGet.Body.String())
	}
	var getResp struct {
		OK       bool             `json:"ok"`
		Accounts []map[string]any `json:"accounts"`
	}
	if err := json.Unmarshal(recGet.Body.Bytes(), &getResp); err != nil || !getResp.OK {
		t.Fatalf("decode GET /api/accounts: err=%v", err)
	}
	seenU1Ign, seenU2NotIgn := false, false
	for _, a := range getResp.Accounts {
		if a["id"] == "u1" && a["ignoreBurnAnomaly"] == true {
			seenU1Ign = true
		}
		if a["id"] == "u2" && a["ignoreBurnAnomaly"] == false {
			seenU2NotIgn = true
		}
	}
	if !seenU1Ign || !seenU2NotIgn {
		t.Fatalf("expected GET /api/accounts u1.ignoreBurnAnomaly=true and u2.ignoreBurnAnomaly=false, got %+v", getResp.Accounts)
	}

	// Reload from disk preserves ignoreBurnAnomaly.
	reloaded := NewAccountManager()
	reloaded.filePath = am.filePath
	reloaded.mu.Lock()
	okLoad := reloaded.loadFromDiskLocked()
	reloaded.mu.Unlock()
	if !okLoad {
		t.Fatal("reload accounts.json failed")
	}
	if accRel, okR := reloaded.GetAccount("u1"); !okR || !accRel.IgnoreBurnAnomaly {
		t.Fatalf("reloaded u1 lost ignoreBurnAnomaly=true: %+v", accRel)
	}

	// 2. Verify attachQuotaBurnAnomalies skips both anomaly attachment AND QUOTA_BURN_ANOMALY log debounce for u1,
	//    while still emitting & logging for u2 (ignoreBurnAnomaly=false).
	makeAnomalousQuota := func() QuotaSnapshot {
		return QuotaSnapshot{
			Status:     "ok",
			UpdatedAt:  t0.Format(time.RFC3339),
			GeminiPool: &PoolQuotaState{State: QuotaStateKnown},
			Groups: []QuotaGroup{{
				Name: "Gemini Models",
				Buckets: []QuotaBucket{
					{Window: "5h", UsedFraction: 0.125, RemainingFraction: 0.875, ResetTime: t0.Add(3 * time.Hour).Format(time.RFC3339)},
				},
			}},
			Deduction: &QuotaDeduction{
				Cap5hUsd:        200.0,
				CapWeeklyUsd:    800.0,
				PoolUsed5hUsd:   fPtr(25.0),
				LocalTotal5hUsd: 4.0,
				MonitorEst5hUsd: 1.0,
				LocalSteps5h:    10,
			},
		}
	}

	burnAnomalyLogMu.Lock()
	burnAnomalyLogLast = map[string]time.Time{}
	burnAnomalyLogMu.Unlock()

	qIgnored := makeAnomalousQuota()
	attachQuotaBurnAnomalies(&qIgnored, "u1", "1.5.18", t0)
	if qIgnored.Deduction.BurnAnomaly5h != nil || qIgnored.Deduction.BurnAnomalyWeekly != nil {
		t.Fatalf("ignored account u1 must NOT attach BurnAnomaly, got 5h=%+v wk=%+v", qIgnored.Deduction.BurnAnomaly5h, qIgnored.Deduction.BurnAnomalyWeekly)
	}
	burnAnomalyLogMu.Lock()
	u1LogCount := len(burnAnomalyLogLast)
	burnAnomalyLogMu.Unlock()
	if u1LogCount != 0 {
		t.Fatalf("ignored account u1 must NOT log QUOTA_BURN_ANOMALY, got map=%+v", burnAnomalyLogLast)
	}

	qNormal := makeAnomalousQuota()
	attachQuotaBurnAnomalies(&qNormal, "u2", "1.5.18", t0)
	if qNormal.Deduction.BurnAnomaly5h == nil || qNormal.Deduction.BurnAnomaly5h.Severity != "critical" {
		t.Fatalf("non-ignored account u2 must still attach critical BurnAnomaly, got %+v", qNormal.Deduction.BurnAnomaly5h)
	}
	burnAnomalyLogMu.Lock()
	u2LogCount := len(burnAnomalyLogLast)
	burnAnomalyLogMu.Unlock()
	if u2LogCount == 0 {
		t.Fatal("non-ignored account u2 expected QUOTA_BURN_ANOMALY log debounce entry")
	}

	// 3. Federated dashboard strips peer burn anomalies for ignored accounts AND exposes ignoreBurnAnomaly on instances!
	u2, _ := am.GetAccount("u2")
	u2.IgnoreBurnAnomaly = true
	// Register a mock peer server for u2 returning an anomaly in both quota.deduction and quotaBurnAnomalies.
	var childIgnReceived bool
	peerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost && r.URL.Path == "/api/internal/monitor-policy" {
			var p MonitorPolicy
			if json.NewDecoder(r.Body).Decode(&p) == nil && p.IgnoreBurnAnomaly != nil && *p.IgnoreBurnAnomaly {
				childIgnReceived = true
			}
			_, _ = w.Write([]byte(`{"ok":true}`))
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/api/dashboard" {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"now": t0.Format(time.RFC3339),
				"gateway": map[string]any{
					"instanceId":    "u2",
					"instanceLabel": "Ultra 2",
				},
				"quota": qNormal,
				"quotaBurnAnomalies": []map[string]any{
					{"accountId": "u2", "accountLabel": "Ultra 2", "window": "5h", "kind": "multiplier_anomaly", "severity": "critical", "ratio": 5.0},
				},
				"windows":       map[string]any{},
				"liveSessions":  []any{},
				"gatewayTurns":  []any{},
				"recentDbSteps": []any{},
				"canaryRoutes":  []any{},
				"cacheWarnings": []any{},
			})
			return
		}
		http.NotFound(w, r)
	}))
	defer peerSrv.Close()
	pu, _ := url.Parse(peerSrv.URL)
	_, pPortStr, _ := net.SplitHostPort(pu.Host)
	pPort, _ := strconv.Atoi(pPortStr)

	am.mu.Lock()
	am.workers["u2"] = &accountWorkerProc{id: "u2", dashPort: pPort}
	am.mu.Unlock()
	t.Cleanup(func() {
		am.mu.Lock()
		delete(am.workers, "u2")
		am.mu.Unlock()
	})

	// Live toggle u2.IgnoreBurnAnomaly=true via UpsertAccount pushes ignoreBurnAnomaly to child without restart!
	if _, err := am.UpsertAccount(u2); err != nil {
		t.Fatalf("UpsertAccount u2 ignoreBurnAnomaly: %v", err)
	}
	if !childIgnReceived {
		t.Fatal("expected UpsertAccount changing ignoreBurnAnomaly to push update to child /api/internal/monitor-policy")
	}

	InvalidatePeerDashboardCache()
	gs := NewGatewayServer()
	defer gs.Close()
	// Ensure primary quota cache doesn't shell out during test.
	quotaMu.Lock()
	savedQ, savedAt := cachedQuota, cachedQuotaAt
	qPrimCopy := qNormal
	cachedQuota = &qPrimCopy
	cachedQuotaAt = time.Now()
	quotaMu.Unlock()
	t.Cleanup(func() {
		quotaMu.Lock()
		cachedQuota, cachedQuotaAt = savedQ, savedAt
		quotaMu.Unlock()
	})

	fed := BuildFederatedDashboardPayload(gs, false)
	anoms, _ := fed["quotaBurnAnomalies"].([]map[string]any)
	for _, a := range anoms {
		if a["accountId"] == "u1" || a["accountId"] == "u2" {
			t.Fatalf("ignored account %v leaked into federated quotaBurnAnomalies: %+v", a["accountId"], anoms)
		}
	}
	insts, _ := fed["instances"].([]map[string]any)
	foundU1, foundU2 := false, false
	for _, inst := range insts {
		id, _ := inst["id"].(string)
		if id == "u1" {
			foundU1 = true
			if inst["ignoreBurnAnomaly"] != true {
				t.Fatalf("expected u1 instance ignoreBurnAnomaly=true, got %+v", inst["ignoreBurnAnomaly"])
			}
			if q, okQ := inst["quota"].(QuotaSnapshot); okQ && q.Deduction != nil && (q.Deduction.BurnAnomaly5h != nil || q.Deduction.BurnAnomalyWeekly != nil) {
				t.Fatalf("expected u1 quota burn anomalies stripped, got %+v", q.Deduction)
			}
		}
		if id == "u2" {
			foundU2 = true
			if inst["ignoreBurnAnomaly"] != true {
				t.Fatalf("expected u2 instance ignoreBurnAnomaly=true, got %+v", inst["ignoreBurnAnomaly"])
			}
			if q, okQ := inst["quota"].(QuotaSnapshot); okQ && q.Deduction != nil && (q.Deduction.BurnAnomaly5h != nil || q.Deduction.BurnAnomalyWeekly != nil) {
				t.Fatalf("expected peer u2 quota burn anomalies stripped, got %+v", q.Deduction)
			}
		}
	}
	if !foundU1 || !foundU2 {
		t.Fatalf("expected both u1 and u2 in federated instances, foundU1=%v foundU2=%v", foundU1, foundU2)
	}

	// Ensure no stray files written outside temp dirs.
	if _, err := os.Stat(filepath.Dir(am.filePath)); err != nil {
		t.Fatalf("unexpected accounts dir error: %v", err)
	}
}

func TestV2324_EmergencyHopDoesNotSpillPrimaryToStandby(t *testing.T) {
	t.Setenv("AGY_MONITOR_TRIP_ROUTING", "1")
	t.Setenv("AGY_MONITOR_TRIP_HOP", "1")
	t.Setenv("AGY_MONITOR_SOFT_CAP_ULTRA", "")
	t.Setenv("AGY_MONITOR_DRAIN_CAP_ULTRA", "")
	t.Setenv("AGY_MONITOR_HARD_CAP_ULTRA", "")
	initConfig()
	t.Cleanup(func() {
		resetMonitorTripsForTest()
		initConfig()
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	req.Header.Set("Authorization", "Bearer master-v2318-key")
	bTrue, bFalse := true, false

	// Case A (Bypass A): u1 Primary at emergency cap (4600), u2 Primary Green at maxConcurrency (healthyAtCapScored),
	// u3 disabled, p1/p2 Standby Pro Green idle (idealScored), MonitorSpillOnYellow=false.
	// Must NOT hop Primary u1 -> Standby p1!
	t.Run("BypassA_AtCapPrimaryPeer_NoHopToIdleStandby", func(t *testing.T) {
		am := setupV2318FiveAccountPool(t, true) // u1/u2/u3 Primary Ultra P10, p1/p2 Standby Pro P20
		now := time.Now().UTC().Truncate(time.Second)
		am.mu.Lock()
		for i := range am.accounts {
			switch am.accounts[i].ID {
			case "u2":
				am.accounts[i].MaxConcurrency = 1
			case "u3":
				am.accounts[i].Enabled = false
				am.accounts[i].SchedState = SchedStateDisabled
			}
		}
		am.mu.Unlock()
		if _, err := am.UpdateRoutingConfig(AccountPoolConfig{
			Strategy:             RouteStrategyFillFirst,
			MonitorPreset:        MonitorPresetBalanced,
			MonitorSmartPacing:   &bFalse,
			MonitorSoftRedHop:    &bFalse,
			MonitorSpillOnYellow: &bFalse,
		}); err != nil {
			t.Fatalf("UpdateRoutingConfig: %v", err)
		}
		// Seed sticky on u1
		seed := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:bypass-a", HasTools: true})
		if seed.DenyErr != nil || seed.Account.ID != "u1" {
			t.Fatalf("expected sticky seed on u1, got %q err=%v", seed.Account.ID, seed.DenyErr)
		}

		var cancelAcc string
		am.setMonitorTripHopCancelHookForTest(func(accID, _ string, _ []string, _ string) {
			cancelAcc = accID
		})
		RecordMonitorToolDispatchAt("u1", 4600, now)
		RecordMonitorToolDispatchAt("u2", 100, now)
		relU2 := am.AcquireAccountInFlight("u2") // puts u2 into healthyAtCapScored (1 >= maxConc 1)
		defer relU2()

		res := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{
			SessionKey:     "hdr:bypass-a",
			IsContinuation: true,
			HasTools:       true,
		})
		if res.DenyErr != nil {
			t.Fatalf("unexpected deny: %v", res.DenyErr)
		}
		if res.Account.ID != "u1" || cancelAcc != "" {
			t.Fatalf("Bypass A: expected u1 to remain pinned instead of hopping to Standby, got %q cancel=%q (mode=%s)", res.Account.ID, cancelAcc, res.RouteMode)
		}
	})

	// Case B (Bypass B): u1 Primary at emergency cap (4600), u2 Primary Yellow (3600, idealScored),
	// u3 disabled, p1/p2 Standby Pro Green (0, idealScored), MonitorSpillOnYellow=true.
	// Must NOT hop Primary u1 -> Standby p1; once u2 becomes Green, MUST hop to u2!
	t.Run("BypassB_YellowPrimaryPeer_SpillOnYellow_NoHopToGreenStandby", func(t *testing.T) {
		am := setupV2318FiveAccountPool(t, true)
		now := time.Now().UTC().Truncate(time.Second)
		am.mu.Lock()
		for i := range am.accounts {
			if am.accounts[i].ID == "u3" {
				am.accounts[i].Enabled = false
				am.accounts[i].SchedState = SchedStateDisabled
			}
		}
		am.mu.Unlock()
		if _, err := am.UpdateRoutingConfig(AccountPoolConfig{
			Strategy:             RouteStrategyFillFirst,
			MonitorPreset:        MonitorPresetBalanced,
			MonitorSmartPacing:   &bFalse,
			MonitorSoftRedHop:    &bFalse,
			MonitorSpillOnYellow: &bTrue,
		}); err != nil {
			t.Fatalf("UpdateRoutingConfig: %v", err)
		}
		seed := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{SessionKey: "hdr:bypass-b", HasTools: true})
		if seed.DenyErr != nil || seed.Account.ID != "u1" {
			t.Fatalf("expected sticky seed on u1, got %q err=%v", seed.Account.ID, seed.DenyErr)
		}

		var cancelAcc, cancelReason string
		am.setMonitorTripHopCancelHookForTest(func(accID, _ string, _ []string, reason string) {
			cancelAcc = accID
			cancelReason = reason
		})
		RecordMonitorToolDispatchAt("u1", 4600, now)
		RecordMonitorToolDispatchAt("u2", 3600, now) // Yellow (>= 3300, < 4250)

		res := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{
			SessionKey:     "hdr:bypass-b",
			IsContinuation: true,
			HasTools:       true,
		})
		if res.DenyErr != nil {
			t.Fatalf("unexpected deny: %v", res.DenyErr)
		}
		if res.Account.ID != "u1" || cancelAcc != "" {
			t.Fatalf("Bypass B: expected u1 to stay pinned when u2 is Yellow and Standby is Green, got %q cancel=%q (mode=%s)", res.Account.ID, cancelAcc, res.RouteMode)
		}

		// Positive control: once u2 is Green, u1 MUST emergency-hop cleanly to same-tier Primary u2!
		resetAccountMonitorBudget("u2", false)
		RecordMonitorToolDispatchAt("u2", 100, now)
		res2 := am.ResolveTargetAccountEx(req, "gemini-4-neon", ResolveOptions{
			SessionKey:     "hdr:bypass-b",
			IsContinuation: true,
			HasTools:       true,
		})
		if res2.DenyErr != nil || res2.Account.ID != "u2" || cancelAcc != "u1" || cancelReason != "monitor_emergency_hop_cancel" {
			t.Fatalf("Positive control: expected emergency hop from u1 to Green Primary u2, got acc=%q cancel=%q/%q err=%v", res2.Account.ID, cancelAcc, cancelReason, res2.DenyErr)
		}
	})
}

func TestV2324_SmartPacingFarFutureGuardPartialInternalAndProCalibration(t *testing.T) {
	resetMonitorTripsForTest()
	t.Cleanup(func() {
		resetMonitorTripsForTest()
		initConfig()
	})
	t.Setenv("AGY_MONITOR_SOFT_CAP_ULTRA", "")
	t.Setenv("AGY_MONITOR_DRAIN_CAP_ULTRA", "")
	t.Setenv("AGY_MONITOR_HARD_CAP_ULTRA", "")
	t.Setenv("AGY_MONITOR_SOFT_CAP_PRO", "")
	t.Setenv("AGY_MONITOR_DRAIN_CAP_PRO", "")
	t.Setenv("AGY_MONITOR_HARD_CAP_PRO", "")
	initConfig()

	now := time.Now().UTC().Truncate(time.Second)
	bTrue, bFalse := true, false
	// 1. Far-future resetAt (> 6h, e.g. 500 years saturating Duration) must NOT wrap int64 or relax thresholds.
	SetRuntimeMonitorPolicy(AccountPoolConfig{MonitorPreset: MonitorPresetBalanced, MonitorSmartPacing: &bTrue})
	monitorBudgets.mu.Lock()
	a := ensureMonitorBudgetAccountLocked("u-far")
	a.windowResetAt = now.AddDate(500, 0, 0)
	a.logCalls5h = 4300
	monitorBudgets.mu.Unlock()
	snap := EvaluateMonitorBudget(AccountProfile{ID: "u-far", PlanTierHint: "ultra"}, now)
	if snap.SmartPacingActive || snap.EffectiveDrainCap != snap.DrainCap {
		t.Fatalf("far-future resetAt must NOT relax Smart Pacing, got active=%v effDrain=%d baseDrain=%d",
			snap.SmartPacingActive, snap.EffectiveDrainCap, snap.DrainCap)
	}

	// 2. Partial internal policy payload must preserve omitted fields (preset=edge & softRedHop=true).
	SetRuntimeMonitorPolicy(AccountPoolConfig{
		MonitorPreset:      MonitorPresetEdge,
		MonitorSmartPacing: &bTrue,
		MonitorSoftRedHop:  &bTrue,
	})
	SetRuntimeMonitorPolicyFromInternal(MonitorPolicy{SmartPacing: &bFalse})
	gotPol := currentMonitorPolicySnapshot()
	if gotPol.Preset != MonitorPresetEdge || gotPol.SmartPacing == nil || *gotPol.SmartPacing != false || gotPol.SoftRedHop == nil || *gotPol.SoftRedHop != true {
		t.Fatalf("partial internal policy must preserve preset=edge and softRedHop=true, got %+v", gotPol)
	}

	// 3. calibratedCapsForHardTier must honor Pro custom ratios (not Ultra ratios).
	SetRuntimeMonitorPolicy(AccountPoolConfig{
		MonitorPreset:        MonitorPresetCustom,
		MonitorSoftCapUltra:  3600,
		MonitorDrainCapUltra: 4400,
		MonitorHardCapUltra:  4800,
		MonitorSoftCapPro:    1200, // 40%
		MonitorDrainCapPro:   1800, // 60%
		MonitorHardCapPro:    3000,
	})
	sp, dp, hp := calibratedCapsForHardTier(2000, "pro")
	if hp != 2000 || sp != 800 || dp != 1200 {
		t.Fatalf("expected Pro custom calibration ratios (40%%/60%% of 2000 = 800/1200/2000), got %d/%d/%d", sp, dp, hp)
	}
	// Ultra 5x also uses conservative Pro ratios.
	s5, d5, h5 := calibratedCapsForHardTier(2000, "ultra_5x")
	if h5 != 2000 || s5 != 800 || d5 != 1200 {
		t.Fatalf("expected ultra_5x calibration to match conservative Pro ratios (800/1200/2000), got %d/%d/%d", s5, d5, h5)
	}
}
