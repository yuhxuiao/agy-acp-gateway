package main

import (
	"context"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"agy-acp-gateway-go/internal/acp"
	gwstore "agy-acp-gateway-go/internal/store"
)

func TestA1_ZeroHotPathGetenvOutsideConfig(t *testing.T) {
	fset := token.NewFileSet()
	var violations []string
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			if (strings.HasPrefix(d.Name(), ".") && path != ".") || d.Name() == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		norm := filepath.ToSlash(path)
		if strings.HasPrefix(norm, "internal/config/") {
			return nil
		}
		fileAST, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			return parseErr
		}
		osAliases := make(map[string]bool)
		for _, imp := range fileAST.Imports {
			impPath, uErr := strconv.Unquote(imp.Path.Value)
			if uErr != nil || impPath != "os" {
				continue
			}
			alias := "os"
			if imp.Name != nil && imp.Name.Name != "" {
				alias = imp.Name.Name
			}
			if alias != "." && alias != "_" {
				osAliases[alias] = true
			}
		}
		if len(osAliases) == 0 {
			return nil
		}
		ast.Inspect(fileAST, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			ident, ok := sel.X.(*ast.Ident)
			if !ok || !osAliases[ident.Name] {
				return true
			}
			if sel.Sel.Name == "Getenv" || sel.Sel.Name == "LookupEnv" {
				pos := fset.Position(sel.Pos())
				violations = append(violations, norm+":"+strconv.Itoa(pos.Line)+" calls "+ident.Name+"."+sel.Sel.Name)
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("WalkDir failed: %v", err)
	}
	if len(violations) > 0 {
		t.Fatalf("unexpected hot-path os.Getenv/os.LookupEnv outside internal/config:\n%s", strings.Join(violations, "\n"))
	}
}

func TestA1_InternalPackagesStrictAcyclicDAG(t *testing.T) {
	const modPrefix = "agy-acp-gateway-go/"
	const internalPrefix = "agy-acp-gateway-go/internal/"

	fset := token.NewFileSet()
	nodes := make(map[string]bool)
	edges := make(map[string]map[string]bool)
	var rootViolations []string

	err := filepath.WalkDir("internal", func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			if strings.HasPrefix(d.Name(), ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		norm := filepath.ToSlash(path)
		pkgRel := filepath.ToSlash(filepath.Dir(norm))
		pkgPath := modPrefix + pkgRel
		nodes[pkgPath] = true
		if edges[pkgPath] == nil {
			edges[pkgPath] = make(map[string]bool)
		}
		fileAST, parseErr := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if parseErr != nil {
			return parseErr
		}
		for _, imp := range fileAST.Imports {
			impPath, uErr := strconv.Unquote(imp.Path.Value)
			if uErr != nil {
				return uErr
			}
			if impPath == "agy-acp-gateway-go" {
				rootViolations = append(rootViolations, norm+" imports root package "+impPath)
				continue
			}
			if strings.HasPrefix(impPath, internalPrefix) {
				if impPath == pkgPath {
					rootViolations = append(rootViolations, norm+" self-imports "+impPath)
					continue
				}
				edges[pkgPath][impPath] = true
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WalkDir internal failed: %v", err)
	}
	if len(rootViolations) > 0 {
		t.Fatalf("forbidden internal package imports:\n%s", strings.Join(rootViolations, "\n"))
	}

	expectedPkgs := []string{"acp", "cleanroom", "config", "dashboard", "protocol", "session", "store"}
	for _, short := range expectedPkgs {
		full := internalPrefix + short
		if !nodes[full] {
			t.Fatalf("missing expected internal package %s", full)
		}
	}

	// Verify pure leaf packages have zero intra-internal dependencies.
	for _, leaf := range []string{"config", "dashboard", "session"} {
		full := internalPrefix + leaf
		if len(edges[full]) > 0 {
			var deps []string
			for dep := range edges[full] {
				deps = append(deps, dep)
			}
			sort.Strings(deps)
			t.Fatalf("leaf package %s must not depend on internal subpackages, got %v", full, deps)
		}
	}

	// Verify strict acyclic DAG via Kahn's topological sort.
	inDegree := make(map[string]int, len(nodes))
	revAdj := make(map[string][]string, len(nodes))
	for n := range nodes {
		inDegree[n] = 0
	}
	for from, tos := range edges {
		for to := range tos {
			if !nodes[to] {
				t.Fatalf("internal package %s imports unknown internal package %s", from, to)
			}
			inDegree[to]++
			revAdj[from] = append(revAdj[from], to)
		}
	}

	var queue []string
	for n, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, n)
		}
	}
	visited := 0
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		visited++
		for _, dep := range revAdj[cur] {
			inDegree[dep]--
			if inDegree[dep] == 0 {
				queue = append(queue, dep)
			}
		}
	}
	if visited != len(nodes) {
		t.Fatalf("cycle detected in internal packages: visited %d of %d packages", visited, len(nodes))
	}
}

type mockAcpEngine struct {
	mu        sync.Mutex
	alive     bool
	gen       uint64
	listeners map[string]acp.SessionListener
	rpcCalls  []string
	nextSid   string
}

func newMockAcpEngine() *mockAcpEngine {
	return &mockAcpEngine{
		gen:       7,
		listeners: make(map[string]acp.SessionListener),
		nextSid:   "00000000-0000-0000-0000-00000000a1d1",
	}
}

func (m *mockAcpEngine) EnsureStarted() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.alive = true
	return nil
}

func (m *mockAcpEngine) StopEngine() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.alive = false
}

func (m *mockAcpEngine) IsAlive() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.alive
}

func (m *mockAcpEngine) IsBooting() bool { return false }

func (m *mockAcpEngine) CurrentGen() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.gen
}

func (m *mockAcpEngine) Capabilities() acp.Capabilities {
	return acp.Capabilities{Resume: true, Load: true, McpHTTP: true}
}

func (m *mockAcpEngine) RegisterListener(sid string, listener acp.SessionListener) {
	m.mu.Lock()
	gen := m.gen
	m.listeners[sid] = listener
	m.mu.Unlock()
	if listener != nil {
		listener.SetEngineGen(gen)
	}
}

func (m *mockAcpEngine) UnregisterSession(sid string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.listeners, sid)
}

func (m *mockAcpEngine) CloseSession(sid string) error {
	m.UnregisterSession(sid)
	return nil
}

func (m *mockAcpEngine) CancelSession(string)               {}
func (m *mockAcpEngine) EnqueueControlReply(map[string]any) {}
func (m *mockAcpEngine) BinaryInfo() map[string]any         { return map[string]any{"mock": true} }
func (m *mockAcpEngine) recordedRPCs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.rpcCalls...)
}

func (m *mockAcpEngine) RPC(method string, params any, _ time.Duration) (json.RawMessage, error) {
	m.mu.Lock()
	m.rpcCalls = append(m.rpcCalls, method)
	sid := m.nextSid
	m.mu.Unlock()

	p, _ := params.(map[string]any)
	switch method {
	case "session/new":
		return json.Marshal(map[string]any{"sessionId": sid})
	case "session/set_config_option":
		cfgID, _ := p["configId"].(string)
		return json.Marshal(map[string]any{
			"configOptions": []map[string]any{
				{"id": cfgID, "currentValue": p["value"]},
			},
		})
	case "session/prompt":
		reqSid, _ := p["sessionId"].(string)
		m.mu.Lock()
		listener := m.listeners[reqSid]
		m.mu.Unlock()
		if listener != nil {
			listener.HandleSessionUpdate(map[string]any{
				"sessionUpdate": "agent_message_chunk",
				"content":       map[string]any{"type": "text", "text": "DI_OK"},
			})
		}
		return json.Marshal(map[string]any{"stopReason": "end_turn"})
	default:
		return json.Marshal(map[string]any{})
	}
}

type mockSessionStore struct {
	mu                      sync.Mutex
	upsertCalls             int
	asyncUpsertCalls        int
	insertTurnWithSnapCalls int
	lastSnap                gwstore.SessionSnapshot
	lastEntry               gwstore.TurnHistoryEntry
}

func (m *mockSessionStore) IsReady() bool                        { return true }
func (m *mockSessionStore) Version() uint64                      { return 1 }
func (m *mockSessionStore) DBPath() string                       { return ":memory:" }
func (m *mockSessionStore) Flush()                               {}
func (m *mockSessionStore) LoadKnownGatewaySids(map[string]bool) {}
func (m *mockSessionStore) IsCursorUpToDate(string, int64, int64, int64, int64) bool {
	return true
}
func (m *mockSessionStore) ClearCleanedConversationSid(string)                {}
func (m *mockSessionStore) SyncUsageStepsFromDir(string, map[string]bool) int { return 0 }
func (m *mockSessionStore) SyncUsageStepsFromSourceDir(string, string, string, string, map[string]bool) int {
	return 0
}
func (m *mockSessionStore) QueryTurnCacheMissIndex() (map[gwstore.TurnMissKey]string, map[string]string) {
	return nil, nil
}
func (m *mockSessionStore) QueryActiveUsageSteps(map[string]bool, int64) ([]gwstore.ParsedModelStep, bool) {
	return nil, true
}
func (m *mockSessionStore) QueryUsageAllTimeRollup(map[string]bool) (gwstore.UsageAllTimeRollup, bool) {
	return gwstore.UsageAllTimeRollup{}, true
}
func (m *mockSessionStore) QueryAllUsageSteps(map[string]bool) ([]gwstore.ParsedModelStep, bool) {
	return nil, true
}
func (m *mockSessionStore) LookupSessionClientMap() map[string]gwstore.SessionClientMeta {
	return nil
}

func (m *mockSessionStore) UpsertSessionSnapshot(snap gwstore.SessionSnapshot) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.upsertCalls++
	m.lastSnap = snap
}

func (m *mockSessionStore) UpsertSessionSnapshotAsync(snap gwstore.SessionSnapshot) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.asyncUpsertCalls++
	m.lastSnap = snap
}

func (m *mockSessionStore) InsertTurn(string, gwstore.TurnHistoryEntry) {}

func (m *mockSessionStore) InsertTurnWithSnapshot(snap gwstore.SessionSnapshot, hasSnap bool, _ string, entry gwstore.TurnHistoryEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.insertTurnWithSnapCalls++
	if hasSnap {
		m.lastSnap = snap
	}
	m.lastEntry = entry
}

func TestA1_SessionRunTurnDependencyInjection(t *testing.T) {
	GlobalEngine.StopEngine()
	if GlobalEngine.IsAlive() {
		t.Fatal("expected GlobalEngine to be stopped before DI test")
	}
	origStoreVer := GlobalStore.Version()

	mockEng := newMockAcpEngine()
	mockStore := &mockSessionStore{}

	gs := NewGatewayServerWithDeps(mockEng, mockStore)
	defer gs.Close()

	// Point PureApiHome into a clean temp dir so advanceUsageWatermarks never touches host files.
	tmpRoot := t.TempDir()
	origAgyHome := PureApiHome
	PureApiHome = filepath.Join(tmpRoot, "pure-api-home")
	defer func() { PureApiHome = origAgyHome }()
	_ = os.MkdirAll(filepath.Join(PureApiHome, "antigravity-cli", "conversations"), 0o755)

	sess := gs.getOrCreateAvailableSession("conv:a1-di-test", t.TempDir(), "You are concise.")
	if sess.Engine != mockEng || sess.Store != mockStore {
		t.Fatalf("expected injected Engine/Store on created session")
	}

	sel := ResolveRequestedModel("gemini-3.8-flash-low", nil)
	var streamed strings.Builder
	res, err := sess.RunTurn(context.Background(), []map[string]any{
		{"role": "user", "content": "ping"},
	}, nil, sel.Tier, func(d DeltaChunk) {
		if d.Type == "text" {
			streamed.WriteString(d.Text)
		}
	}, &sel)
	if err != nil {
		t.Fatalf("RunTurn with injected mocks failed: %v", err)
	}
	if res.StopReason != "end_turn" {
		t.Fatalf("unexpected stopReason %q (want end_turn)", res.StopReason)
	}
	if res.Text != "DI_OK" || streamed.String() != "DI_OK" {
		t.Fatalf("unexpected text output res=%q streamed=%q", res.Text, streamed.String())
	}

	rpcs := mockEng.recordedRPCs()
	wantSeen := map[string]bool{"session/new": false, "session/set_config_option": false, "session/prompt": false}
	for _, m := range rpcs {
		if _, ok := wantSeen[m]; ok {
			wantSeen[m] = true
		}
	}
	for method, seen := range wantSeen {
		if !seen {
			t.Fatalf("mockEngine missed expected RPC %q; got %v", method, rpcs)
		}
	}

	mockStore.mu.Lock()
	upserts := mockStore.upsertCalls + mockStore.asyncUpsertCalls
	inserts := mockStore.insertTurnWithSnapCalls
	lastStop := mockStore.lastEntry.StopReason
	mockStore.mu.Unlock()
	if upserts < 1 {
		t.Fatalf("expected >= 1 UpsertSessionSnapshot/Async call on mockStore, got %d", upserts)
	}
	if inserts != 1 || lastStop != "end_turn" {
		t.Fatalf("expected 1 InsertTurnWithSnapshot with end_turn, got count=%d stop=%q", inserts, lastStop)
	}
	if GlobalEngine.IsAlive() {
		t.Fatal("GlobalEngine must remain stopped when session uses injected mockEngine")
	}
	if GlobalStore.Version() != origStoreVer {
		t.Fatalf("GlobalStore version changed (%d -> %d) during mockStore DI test", origStoreVer, GlobalStore.Version())
	}
}
