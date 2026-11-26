package main

// Production update flow, gateway side.
//
// A root-owned host agent (deploy/prod-upgrader) checks for agy releases, builds and tests a
// candidate and writes <upgrade dir>/status.json. The gateway never runs docker or touches the
// host: it only shows that status on the dashboard and, when an administrator approves, writes
// an approval file the host agent acts on.
//
// agy updates are applied without restarting the gateway: the host switches the symlink that
// AGY_ACP_BIN points to, and the gateway restarts its ACP engine once no turn is in flight
// (sessions resume afterwards; resume keeps the upstream cache, review C4).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	gwcfg "agy-acp-gateway-go/internal/config"
	gwdash "agy-acp-gateway-go/internal/dashboard"
)

// UpgradeDir holds status.json, approvals/ and engine.json. Defaults next to the state DB
// (/data/upgrade in the container, runtime/upgrade locally).
func UpgradeDir() string {
	if v := gwcfg.Current().UpgradeDirEnv; v != "" {
		return v
	}
	return filepath.Join(filepath.Dir(GatewayStateDB), "upgrade")
}

type upgradeCandidate struct {
	ID        string `json:"id"`
	Kind      string `json:"kind"` // "agy" (hot switch) or "gateway" (container restart)
	Version   string `json:"version"`
	SHA256    string `json:"sha256"`
	State     string `json:"state"` // testing, ready, failed, approved, applying, applied, rolled_back, waiting_idle
	Message   string `json:"message"`
	Installed string `json:"installed,omitempty"`
}

type upgradeStatus struct {
	Candidate *upgradeCandidate `json:"candidate"`
}

var knownUpgradeMetricStates = gwdash.KnownUpgradeMetricStates

func readUpgradeStatusRaw() (map[string]any, *upgradeStatus, error) {
	b, err := os.ReadFile(filepath.Join(UpgradeDir(), "status.json"))
	if err != nil {
		return nil, nil, err
	}
	var raw map[string]any
	var st upgradeStatus
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, nil, err
	}
	if err := json.Unmarshal(b, &st); err != nil {
		return nil, nil, err
	}
	return raw, &st, nil
}

const (
	upgradeApprovalMaxAge        = 24 * time.Hour
	upgradeApprovalMaxFutureSkew = 5 * time.Minute
)

func readValidPendingApproval(cand *upgradeCandidate, now time.Time) (map[string]any, bool) {
	if cand == nil || cand.State != "ready" || !upgradeIDRe.MatchString(cand.ID) || !sha256Re.MatchString(cand.SHA256) {
		return nil, false
	}
	rawBytes, err := os.ReadFile(filepath.Join(UpgradeDir(), "approvals", cand.ID+".json"))
	if err != nil {
		return nil, false
	}
	var existing map[string]any
	if json.Unmarshal(rawBytes, &existing) != nil || existing == nil {
		return nil, false
	}
	cid, _ := existing["candidateId"].(string)
	sha, _ := existing["sha256"].(string)
	if cid != cand.ID || sha != cand.SHA256 {
		return nil, false
	}
	atStr, _ := existing["approvedAt"].(string)
	atStr = strings.TrimSpace(atStr)
	if atStr == "" {
		return nil, false
	}
	approvedAt, err := time.Parse(time.RFC3339Nano, atStr)
	if err != nil {
		approvedAt, err = time.Parse(time.RFC3339, atStr)
		if err != nil {
			return nil, false
		}
	}
	age := now.Sub(approvedAt)
	if age < -upgradeApprovalMaxFutureSkew || age >= upgradeApprovalMaxAge {
		return nil, false
	}
	return existing, true
}

func hasPendingUpgradeApproval(cand *upgradeCandidate) bool {
	_, ok := readValidPendingApproval(cand, time.Now().UTC())
	return ok
}

func extractUpgradeLastCheckError(raw map[string]any, st *upgradeStatus, readErr error) string {
	if readErr != nil {
		if errors.Is(readErr, os.ErrNotExist) {
			return ""
		}
		return "status.json unreadable"
	}
	if raw != nil {
		for _, k := range []string{"lastCheckError", "error"} {
			if s, ok := raw[k].(string); ok && strings.TrimSpace(s) != "" {
				return strings.TrimSpace(s)
			}
		}
	}
	if st != nil && st.Candidate != nil && (st.Candidate.State == "failed" || st.Candidate.State == "rolled_back") {
		if msg := strings.TrimSpace(st.Candidate.Message); msg != "" {
			return msg
		}
		if raw != nil {
			if gates, ok := raw["gates"].([]any); ok {
				for _, item := range gates {
					gm, okMap := item.(map[string]any)
					if !okMap {
						continue
					}
					if okFlag, hasOK := gm["ok"].(bool); hasOK && !okFlag {
						name, _ := gm["name"].(string)
						detail, _ := gm["detail"].(string)
						if name != "" && detail != "" {
							return name + ": " + detail
						}
						if detail != "" {
							return detail
						}
						if name != "" {
							return name + " failed"
						}
					}
				}
			}
		}
		return "candidate " + st.Candidate.State
	}
	return ""
}

// CurrentUpgradeMetricState returns the bounded state label for Prometheus agy_gateway_upgrade_state.
func CurrentUpgradeMetricState() string {
	_, st, err := readUpgradeStatusRaw()
	hasFile := !errors.Is(err, os.ErrNotExist)
	hasCand := st != nil && st.Candidate != nil
	rawState := ""
	approved := false
	if hasCand {
		rawState = st.Candidate.State
		approved = hasPendingUpgradeApproval(st.Candidate)
	}
	return gwdash.NormalizeUpgradeMetricState(hasFile, err != nil && hasFile, rawState, hasCand, approved)
}

type peerUpgradeEngineReport struct {
	ID            string
	Label         string
	Alive         bool
	Running       string
	StartedAt     string
	PendingSwitch bool
}

func fetchPeerUpgradeEngine(peer gwcfg.DashboardPeerConfig, fallbackKey string) (peerUpgradeEngineReport, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(peer.BaseURL, "/")+"/api/upgrade", nil)
	if err != nil {
		return peerUpgradeEngineReport{}, false
	}
	key := strings.TrimSpace(peer.APIKey)
	if key == "" {
		key = fallbackKey
	}
	if key != "" {
		req.Header.Set("x-api-key", key)
	}
	resp, err := peerHTTPClient.Do(req)
	if err != nil {
		return peerUpgradeEngineReport{}, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return peerUpgradeEngineReport{}, false
	}
	var body struct {
		Engine        map[string]any `json:"engine"`
		PendingSwitch bool           `json:"pendingSwitch"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&body); err != nil {
		return peerUpgradeEngineReport{}, false
	}
	alive, _ := body.Engine["alive"].(bool)
	running, _ := body.Engine["running"].(string)
	startedAt, _ := body.Engine["startedAt"].(string)
	return peerUpgradeEngineReport{
		ID:            peer.ID,
		Label:         peer.Label,
		Alive:         alive,
		Running:       strings.TrimSpace(running),
		StartedAt:     strings.TrimSpace(startedAt),
		PendingSwitch: body.PendingSwitch,
	}, true
}

func aggregateUpgradeEngineInfo(self map[string]any, selfPending bool, primaryEnabled bool, peers []peerUpgradeEngineReport) (map[string]any, bool) {
	out := make(map[string]any, len(self)+2)
	for k, v := range self {
		out[k] = v
	}
	current, _ := out["current"].(string)
	selfAlive, _ := out["alive"].(bool)
	selfRunning, _ := out["running"].(string)
	selfStartedAt, _ := out["startedAt"].(string)

	anyPending := selfPending
	workers := make([]map[string]any, 0, len(peers))
	var oldAlive *peerUpgradeEngineReport
	var curAlive *peerUpgradeEngineReport
	var latestIdle *peerUpgradeEngineReport

	if (primaryEnabled || selfAlive) && selfAlive && selfRunning != "" {
		prim := peerUpgradeEngineReport{
			ID:            "primary",
			Alive:         true,
			Running:       selfRunning,
			StartedAt:     selfStartedAt,
			PendingSwitch: selfPending,
		}
		if current != "" && selfRunning != current {
			oldAlive = &prim
			anyPending = true
		} else {
			curAlive = &prim
		}
	}

	for i := range peers {
		p := peers[i]
		w := map[string]any{
			"id":            p.ID,
			"label":         p.Label,
			"alive":         p.Alive,
			"pendingSwitch": p.PendingSwitch,
		}
		if p.Running != "" {
			w["running"] = p.Running
		}
		if p.StartedAt != "" {
			w["startedAt"] = p.StartedAt
		}
		workers = append(workers, w)
		if p.PendingSwitch {
			anyPending = true
		}
		if p.Alive && p.Running != "" {
			if current != "" && p.Running != current {
				if oldAlive == nil {
					cp := p
					oldAlive = &cp
				}
				anyPending = true
			} else if curAlive == nil || p.StartedAt > curAlive.StartedAt {
				cp := p
				curAlive = &cp
			}
		} else if p.Running != "" {
			if latestIdle == nil || p.StartedAt > latestIdle.StartedAt {
				cp := p
				latestIdle = &cp
			}
		}
	}

	if len(workers) > 0 {
		out["workers"] = workers
	}
	switch {
	case oldAlive != nil:
		out["alive"] = true
		out["running"] = oldAlive.Running
		if oldAlive.StartedAt != "" {
			out["startedAt"] = oldAlive.StartedAt
		}
	case curAlive != nil:
		out["alive"] = true
		out["running"] = curAlive.Running
		if curAlive.StartedAt != "" {
			out["startedAt"] = curAlive.StartedAt
		}
	default:
		out["alive"] = false
		if selfRunning == "" && latestIdle != nil {
			out["running"] = latestIdle.Running
			if latestIdle.StartedAt != "" {
				out["startedAt"] = latestIdle.StartedAt
			}
		}
	}
	return out, anyPending
}

func federatedUpgradeEngineInfo() (map[string]any, bool) {
	self := GlobalEngine.binaryInfo()
	selfPending := acpBinarySwitchPending()
	if isChildAccountWorker() || GlobalAccountManager == nil || !GlobalAccountManager.HasConfiguredAccounts() {
		return self, selfPending
	}
	peers, _ := GlobalAccountManager.ManagedPeersForDashboard()
	if len(peers) == 0 {
		return self, selfPending
	}
	primaryEnabled := true
	if prim, ok := GlobalAccountManager.GetPrimaryAccount(); ok {
		primaryEnabled = prim.Enabled
	}
	fallbackKey := defaultPeerAPIKey(gwcfg.Current())
	results := make([]peerUpgradeEngineReport, len(peers))
	okFlags := make([]bool, len(peers))
	var wg sync.WaitGroup
	for i, peer := range peers {
		wg.Add(1)
		go func(idx int, p gwcfg.DashboardPeerConfig) {
			defer wg.Done()
			results[idx], okFlags[idx] = fetchPeerUpgradeEngine(p, fallbackKey)
		}(i, peer)
	}
	wg.Wait()
	reached := make([]peerUpgradeEngineReport, 0, len(peers))
	for i := range peers {
		if okFlags[i] {
			reached = append(reached, results[i])
		}
	}
	return aggregateUpgradeEngineInfo(self, selfPending, primaryEnabled, reached)
}

// BuildUpgradePayload is what GET /api/upgrade returns.
func BuildUpgradePayload() map[string]any {
	engInfo, pendingSwitch := federatedUpgradeEngineInfo()
	out := map[string]any{
		"enabled":         authEnabled(),
		"engine":          engInfo,
		"pendingSwitch":   pendingSwitch,
		"approvalPending": false,
		"lastCheckError":  "",
	}
	raw, st, err := readUpgradeStatusRaw()
	switch {
	case err == nil:
		out["status"] = raw
		if st != nil && hasPendingUpgradeApproval(st.Candidate) {
			out["approvalPending"] = true
		}
	case errors.Is(err, os.ErrNotExist):
		out["status"] = nil
	default:
		out["statusError"] = "status.json unreadable"
	}
	out["lastCheckError"] = extractUpgradeLastCheckError(raw, st, err)
	return out
}

var upgradeIDRe = regexp.MustCompile(`^[A-Za-z0-9._-]{4,80}$`)
var sha256Re = regexp.MustCompile(`^[0-9a-f]{64}$`)

// sameOrigin requires a browser Origin that matches the host the request was sent to, so a
// third-party page cannot ride on the browser's cached Basic credentials.
func sameOrigin(r *http.Request) bool {
	if r == nil || len(r.Header.Values("Origin")) > 1 {
		return false
	}
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin == "" {
		return false
	}
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return false
	}
	if !strings.EqualFold(u.Scheme, "http") && !strings.EqualFold(u.Scheme, "https") {
		return false
	}
	if r.TLS != nil && !strings.EqualFold(u.Scheme, "https") {
		return false
	}
	trustedProxy := isTrustedProxyRemoteAddr(r.RemoteAddr)
	if trustedProxy {
		fwdProto := strings.ToLower(strings.TrimSpace(strings.Split(r.Header.Get("X-Forwarded-Proto"), ",")[0]))
		if fwdProto == "https" && !strings.EqualFold(u.Scheme, "https") {
			return false
		}
		if fwdHost := strings.TrimSpace(strings.Split(r.Header.Get("X-Forwarded-Host"), ",")[0]); fwdHost != "" && strings.EqualFold(fwdHost, u.Host) {
			return true
		}
	}
	reqHost := strings.TrimSpace(strings.Split(r.Host, ",")[0])
	return reqHost != "" && strings.EqualFold(reqHost, u.Host)
}

var approvalMu sync.Mutex

// HandleUpgradeApprove implements POST /api/upgrade/approve. The caller already passed the
// dashboard (API key) check; this adds a custom header and a same-origin Origin so a third-party
// page cannot ride on the browser's cached credentials. Approval is only possible with API-key
// auth on (an unauthenticated loopback gateway has no upgrade agent).
func HandleUpgradeApprove(w http.ResponseWriter, r *http.Request) {
	fail := func(code int, msg string) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": msg})
	}
	if !authEnabled() {
		fail(http.StatusForbidden, "approval disabled: API-key auth (AGY_API_KEYS) is not configured")
		return
	}
	if r.Header.Get("X-Agy-Requested-With") != "dashboard" || !sameOrigin(r) {
		LogEvent("UPGRADE_APPROVE_REJECTED", "Approval rejected: missing custom header or cross-origin request", map[string]any{"remote": r.RemoteAddr})
		fail(http.StatusForbidden, "cross-origin or non-dashboard request")
		return
	}
	var body struct {
		CandidateID string `json:"candidateId"`
		SHA256      string `json:"sha256"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&body); err != nil {
		fail(http.StatusBadRequest, "malformed body")
		return
	}
	if !upgradeIDRe.MatchString(body.CandidateID) || !sha256Re.MatchString(body.SHA256) {
		fail(http.StatusBadRequest, "invalid candidate id or sha256")
		return
	}
	approvalMu.Lock()
	defer approvalMu.Unlock()
	_, st, err := readUpgradeStatusRaw()
	if err != nil || st.Candidate == nil {
		fail(http.StatusConflict, "no candidate")
		return
	}
	c := st.Candidate
	if c.ID != body.CandidateID || c.SHA256 != body.SHA256 {
		fail(http.StatusConflict, "candidate changed; reload the dashboard")
		return
	}
	if c.State != "ready" {
		fail(http.StatusConflict, "candidate is not ready (state "+c.State+")")
		return
	}
	dir := filepath.Join(UpgradeDir(), "approvals")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fail(http.StatusInternalServerError, "cannot create approvals directory")
		return
	}
	final := filepath.Join(dir, c.ID+".json")
	now := time.Now().UTC()
	if existing, ok := readValidPendingApproval(c, now); ok {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":              true,
			"alreadyApproved": true,
			"approval":        existing,
		})
		return
	}
	approval := map[string]any{
		"candidateId": c.ID, "sha256": c.SHA256, "kind": c.Kind, "version": c.Version,
		"approvedAt": now.Format(time.RFC3339), "remote": r.RemoteAddr,
	}
	b, _ := json.MarshalIndent(approval, "", "  ")
	tmp := final + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil || os.Rename(tmp, final) != nil {
		fail(http.StatusInternalServerError, "cannot write approval")
		return
	}
	LogEvent("UPGRADE_APPROVED", fmt.Sprintf("Update %s %s approved from the dashboard", c.Kind, c.Version), map[string]any{
		"candidateId": c.ID, "sha256": c.SHA256, "remote": r.RemoteAddr,
	})
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "alreadyApproved": false, "approval": approval})
}

// ---- ACP binary hot switch ----

type acpBinaryState struct {
	mu        sync.Mutex
	path      string // AGY_ACP_BIN as configured
	resolved  string // symlink target the running engine was started from
	startedAt time.Time
}

var acpBinary acpBinaryState

// recordEngineBinary resolves AGY_ACP_BIN right before the engine is exec'd. If the symlink
// moves in between, the switch is simply detected once more and the engine restarts again.
func recordEngineBinary() {
	resolved, err := filepath.EvalSymlinks(PatchedAgyBin)
	if err != nil {
		resolved = PatchedAgyBin
	}
	acpBinary.mu.Lock()
	acpBinary.path, acpBinary.resolved, acpBinary.startedAt = PatchedAgyBin, resolved, time.Now().UTC()
	acpBinary.mu.Unlock()
}

func writeEngineInfoFile(info map[string]any) {
	b, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		return
	}
	dir := UpgradeDir()
	if os.MkdirAll(dir, 0o755) != nil {
		return
	}
	tmp := filepath.Join(dir, "engine.json.tmp")
	if os.WriteFile(tmp, b, 0o644) == nil {
		_ = os.Rename(tmp, filepath.Join(dir, "engine.json"))
	}
}

// publishEngineInfo writes engine.json after a successful initialize (or binary switch stop) so
// the host agent can confirm which agy binary is serving and auto-finalize waiting_idle upgrades.
func publishEngineInfo() {
	writeEngineInfoFile(GlobalEngine.binaryInfo())
}

var (
	waitingIdleSignalMu     sync.Mutex
	lastWaitingIdleSigKey   string
	lastWaitingIdleSigAt    time.Time
	waitingIdleSignalWindow = 60 * time.Second
)

func resetWaitingIdleSignalForTest() {
	waitingIdleSignalMu.Lock()
	lastWaitingIdleSigKey = ""
	lastWaitingIdleSigAt = time.Time{}
	waitingIdleSignalMu.Unlock()
}

// syncWaitingIdleEngineSignal checks whether the host upgrader left an agy upgrade in
// waiting_idle; once all active engines (including federated child workers) have either exited
// or switched onto the new binary, it updates engine.json (throttled) so systemd PathModified
// immediately wakes agy-upgrader-apply.service to run the final probe instead of waiting up to 24h.
func syncWaitingIdleEngineSignal() {
	if isChildAccountWorker() {
		return
	}
	_, st, err := readUpgradeStatusRaw()
	if err != nil || st == nil || st.Candidate == nil || st.Candidate.Kind != "agy" || st.Candidate.State != "waiting_idle" {
		waitingIdleSignalMu.Lock()
		lastWaitingIdleSigKey = ""
		lastWaitingIdleSigAt = time.Time{}
		waitingIdleSignalMu.Unlock()
		return
	}
	engInfo, pendingSwitch := federatedUpgradeEngineInfo()
	if pendingSwitch {
		return
	}
	alive, _ := engInfo["alive"].(bool)
	running, _ := engInfo["running"].(string)
	installed := strings.TrimSpace(st.Candidate.Installed)
	readyToFinalize := !alive || (installed != "" && strings.Contains(running, installed))
	if !readyToFinalize {
		return
	}
	sigKey := fmt.Sprintf("%s|%s|%t", st.Candidate.ID, running, alive)
	now := time.Now()
	waitingIdleSignalMu.Lock()
	if lastWaitingIdleSigKey == sigKey && now.Sub(lastWaitingIdleSigAt) < waitingIdleSignalWindow {
		waitingIdleSignalMu.Unlock()
		return
	}
	lastWaitingIdleSigKey = sigKey
	lastWaitingIdleSigAt = now
	waitingIdleSignalMu.Unlock()
	writeEngineInfoFile(engInfo)
}

func (e *SharedAcpEngine) BinaryInfo() map[string]any {
	return e.binaryInfo()
}

func (e *SharedAcpEngine) binaryInfo() map[string]any {
	acpBinary.mu.Lock()
	defer acpBinary.mu.Unlock()
	out := map[string]any{"configured": PatchedAgyBin, "alive": e.IsAlive()}
	if acpBinary.resolved != "" {
		out["running"] = acpBinary.resolved
		out["startedAt"] = acpBinary.startedAt.Format(time.RFC3339)
	}
	if cur, err := filepath.EvalSymlinks(PatchedAgyBin); err == nil {
		out["current"] = cur
	}
	return out
}

// acpBinarySwitchPending reports whether AGY_ACP_BIN now resolves to a different file than the
// one the live engine was started from.
func acpBinarySwitchPending() bool {
	return acpBinarySwitchPendingOn(GlobalEngine)
}

func acpBinarySwitchPendingOn(eng interface{ IsAlive() bool }) bool {
	if eng == nil || !eng.IsAlive() {
		return false
	}
	cur, err := filepath.EvalSymlinks(PatchedAgyBin)
	if err != nil {
		return false
	}
	acpBinary.mu.Lock()
	defer acpBinary.mu.Unlock()
	return acpBinary.resolved != "" && cur != acpBinary.resolved
}

// prepareBinarySwitch hibernates every idle worker (keeping resume state) when a new agy binary
// is waiting; the reaper then stops the engine once no worker is live, and the next request
// starts the new binary. Busy, prompting or parked workers are left alone until they finish.
func (gs *GatewayServer) prepareBinarySwitch() {
	if !acpBinarySwitchPendingOn(gs.engine()) {
		return
	}
	gs.mu.RLock()
	var idle []*AcpProjectSession
	for _, s := range gs.sessions {
		s.mu.Lock()
		live := s.SessionID != ""
		s.mu.Unlock()
		if live && s.IsAvailableForNewTurn() {
			idle = append(idle, s)
		}
	}
	gs.mu.RUnlock()
	for _, s := range idle {
		s.HibernateWorker("acp_binary_switch", false)
	}
	if len(idle) > 0 {
		LogEvent("ACP_BINARY_SWITCH_PENDING", "New agy binary detected; idle workers hibernated, engine restarts when all turns finish", map[string]any{
			"hibernated": len(idle),
		})
	}
}
