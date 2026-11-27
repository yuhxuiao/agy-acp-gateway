package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
	"unicode"
)

func TestKnownGatewaySidsVersionCacheAndDBIsolation(t *testing.T) {
	prevDB := GatewayStateDB
	t.Cleanup(func() {
		initConfig()
		InitGlobalStore(prevDB)
	})
	tmp1 := t.TempDir()
	tmp2 := t.TempDir()
	db1 := filepath.Join(tmp1, "gw1.db")
	db2 := filepath.Join(tmp2, "gw2.db")
	logFile := filepath.Join(tmp1, "gateway.log")

	t.Setenv("AGY_STATE_DB", db1)
	t.Setenv("AGY_PURE_API_HOME", filepath.Join(tmp1, "pure"))
	t.Setenv("AGY_GW_LOG", logFile)
	initConfig()
	InitGlobalStore(db1)

	sid1 := "11111111-1111-4000-8000-000000000001"
	sid2 := "22222222-2222-4000-8000-000000000002"
	sid3 := "33333333-3333-4000-8000-000000000003"

	s1 := NewAcpProjectSession("sid:k1", tmp1, "")
	s1.SessionID = sid1
	s1.MessageCount = 2
	GlobalStore.UpsertSession(s1)

	got1 := getKnownGatewaySids()
	if !got1[sid1] {
		t.Fatalf("expected sid1 in knownGwSids, got %v", got1)
	}
	gwSidMu.RLock()
	cachedVer1 := knownGwSidsVer
	cachedDB1 := knownGwSidsDBPath
	gwSidMu.RUnlock()
	if cachedDB1 != db1 || cachedVer1 != GlobalStore.Version() {
		t.Fatalf("expected version cache synced to (%s, %d), got (%s, %d)", db1, GlobalStore.Version(), cachedDB1, cachedVer1)
	}

	// Insert a raw row WITHOUT bumping GlobalStore.Version() to prove the fast path skips SQLite UNION scans on idle polls.
	GlobalStore.mu.Lock()
	_, err := GlobalStore.db.Exec(`INSERT OR IGNORE INTO known_gateway_sids (session_id) VALUES (?)`, sid2)
	GlobalStore.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	gotIdle := getKnownGatewaySids()
	if gotIdle[sid2] {
		t.Fatalf("expected idle poll with unchanged Store.Version() to hit fast memory cache without scanning SQLite")
	}

	// Once a real store write bumps Version(), getKnownGatewaySids() refreshes and picks up sid2.
	GlobalStore.InsertTurn("sid:k1", TurnHistoryEntry{
		TurnIndex:     1,
		CompletedAtMs: time.Now().UnixMilli(),
		SessionID:     sid1,
		Mode:          "cold_start",
		Model:         "gemini-4-carbon",
		StopReason:    "end_turn",
	})
	gotAfterBump := getKnownGatewaySids()
	if !gotAfterBump[sid1] || !gotAfterBump[sid2] {
		t.Fatalf("expected sid1 & sid2 after Store.Version() bump, got %v", gotAfterBump)
	}

	// Switching GlobalStore to a fresh DB resets storeGwSids (preventing unbounded cross-DB growth).
	InitGlobalStore(db2)
	s3 := NewAcpProjectSession("sid:k3", tmp2, "")
	s3.SessionID = sid3
	s3.MessageCount = 2
	GlobalStore.UpsertSession(s3)

	gotDB2 := getKnownGatewaySids()
	if !gotDB2[sid3] || gotDB2[sid2] {
		t.Fatalf("expected DB2 storeGwSids to contain sid3 and drop DB1-only sid2, got %v", gotDB2)
	}
}

func TestPrometheusWafAutobisectAndUpgradeStateSet(t *testing.T) {
	prevDB := GatewayStateDB
	t.Cleanup(func() {
		initConfig()
		InitGlobalStore(prevDB)
	})
	tmp := t.TempDir()
	upDir := filepath.Join(tmp, "upgrade")
	t.Setenv("AGY_UPGRADE_DIR", upDir)
	t.Setenv("AGY_STATE_DB", filepath.Join(tmp, "state.db"))
	t.Setenv("AGY_PURE_API_HOME", filepath.Join(tmp, "pure"))
	t.Setenv("AGY_GW_LOG", filepath.Join(tmp, "gateway.log"))
	initConfig()
	InitGlobalStore(filepath.Join(tmp, "state.db"))

	beforeWaf := WafBisectHealedTotal.Load()
	WafBisectHealedTotal.Add(2)
	defer WafBisectHealedTotal.Add(-2)

	gs := NewGatewayServer()
	defer gs.Close()

	prom := RenderPrometheusMetrics(gs)
	wantWaf := fmt.Sprintf("agy_gateway_waf_autobisect_healed_total %d", beforeWaf+2)
	if !strings.Contains(prom, wantWaf) {
		t.Fatalf("expected %q in Prometheus metrics, got:\n%s", wantWaf, prom)
	}

	// Verify state-set invariant: sum(agy_gateway_upgrade_state) == 1
	re := regexp.MustCompile(`(?m)^agy_gateway_upgrade_state\{state="([^"]+)"\} (\d+)$`)
	matches := re.FindAllStringSubmatch(prom, -1)
	if len(matches) != len(knownUpgradeMetricStates) {
		t.Fatalf("expected %d upgrade state gauge lines, got %d", len(knownUpgradeMetricStates), len(matches))
	}
	sum := 0
	activeState := ""
	for _, m := range matches {
		if m[2] == "1" {
			sum++
			activeState = m[1]
		}
	}
	if sum != 1 || activeState != "none" {
		t.Fatalf("expected exactly 1 active state='none', got sum=%d active=%q", sum, activeState)
	}
}

func isEmojiRune(r rune) bool {
	return (r >= 0x1F300 && r <= 0x1FAFF) ||
		(r >= 0x2600 && r <= 0x27BF) ||
		(r >= 0xFE00 && r <= 0xFE0F) ||
		r == 0x200D
}

func TestDashboardHTMLRound4RolloverCompletenessStyleAndVMBehavior(t *testing.T) {
	html := RenderDashboardHTML()

	// 1. Strict style rules: zero em-dash, zero en-dash, zero Emoji, Decision U8 respected
	for i, r := range html {
		if r == '\u2014' || r == '\u2013' {
			t.Fatalf("dashboard.html contains forbidden dash U+%04X at byte %d", r, i)
		}
		if isEmojiRune(r) && !unicode.IsSpace(r) {
			t.Fatalf("dashboard.html contains forbidden emoji rune U+%04X (%q) at byte %d", r, string(r), i)
		}
	}
	if strings.Contains(html, "建议切换 Hydrogen") {
		t.Fatal("dashboard.html violates Decision U8 strict model pinning")
	}

	// 2. Every "rollover_..." code across non-test go-gateway/*.go must appear in MISS and MISS_SHORT
	goFiles, err := filepath.Glob("*.go")
	if err != nil || len(goFiles) == 0 {
		t.Fatalf("glob *.go failed: %v", err)
	}
	rollRe := regexp.MustCompile(`"(rollover_[a-z0-9_]+)"`)
	rollCodes := map[string]bool{}
	for _, f := range goFiles {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range rollRe.FindAllStringSubmatch(string(b), -1) {
			rollCodes[m[1]] = true
		}
	}
	if len(rollCodes) < 8 {
		t.Fatalf("expected >=8 rollover_* codes discovered in go-gateway/*.go, got %v", rollCodes)
	}
	missBlockRe := regexp.MustCompile(`(?s)const MISS = \{(.*?)\};\s*const MISS_SHORT = \{(.*?)\};`)
	mb := missBlockRe.FindStringSubmatch(html)
	if len(mb) != 3 {
		t.Fatal("could not locate MISS and MISS_SHORT blocks in RenderDashboardHTML")
	}
	for code := range rollCodes {
		if !strings.Contains(mb[1], code+":") {
			t.Fatalf("MISS dictionary in dashboard.html is missing %q", code)
		}
		if !strings.Contains(mb[2], code+":") {
			t.Fatalf("MISS_SHORT dictionary in dashboard.html is missing %q", code)
		}
	}

	// 3. Extract <script> and run syntax + behavioral assertions in Node VM when node is available
	startTag := "<script>"
	endTag := "</script>"
	si := strings.Index(html, startTag)
	ei := strings.LastIndex(html, endTag)
	if si < 0 || ei <= si {
		t.Fatal("missing <script>...</script> in RenderDashboardHTML")
	}
	scriptSrc := html[si+len(startTag) : ei]

	nodeBin, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not installed; skipping JS VM behavioral verification")
	}
	tmp := t.TempDir()
	jsFile := filepath.Join(tmp, "dashboard_script.js")
	if err := os.WriteFile(jsFile, []byte(scriptSrc), 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command(nodeBin, "--check", jsFile).CombinedOutput(); err != nil {
		t.Fatalf("dashboard.html <script> failed node --check: %v\n%s", err, string(out))
	}

	harness := `
const fs = require('fs');
const vm = require('vm');
const assert = require('assert');

const code = fs.readFileSync(process.argv[2], 'utf8');

class Elem {
  constructor(id = '', tag = 'DIV') {
    this.id = id;
    this.tagName = tag;
    this.textContent = '';
    this._innerHTML = '';
    this.hidden = false;
    this.value = 'ALL';
    this.dataset = {};
    this.style = {};
    this.attrs = {};
    this.listeners = {};
    this.children = [];
    this.focused = false;
    this.className = '';
    const classes = new Set();
    this.classList = {
      add: (...cs) => cs.forEach(c => classes.add(c)),
      remove: (...cs) => cs.forEach(c => classes.delete(c)),
      toggle: (c, on) => {
        const next = on === undefined ? !classes.has(c) : !!on;
        next ? classes.add(c) : classes.delete(c);
        return next;
      },
      contains: (c) => classes.has(c),
    };
  }
  set innerHTML(v) {
    this._innerHTML = String(v);
    if (this.id === 'river-bar') {
      const cnt = (this._innerHTML.match(/<i /g) || []).length;
      this.children = Array.from({ length: cnt }, () => new Elem('', 'I'));
    }
  }
  get innerHTML() { return this._innerHTML; }
  getAttribute(k) { return this.attrs[k] || ''; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  insertAdjacentHTML() {}
  addEventListener(ev, fn) { (this.listeners[ev] = this.listeners[ev] || []).push(fn); }
  dispatch(ev, obj) { for (const fn of (this.listeners[ev] || [])) fn(obj); }
  querySelectorAll() { return []; }
  querySelector() { return null; }
  contains() { return false; }
  closest() { return null; }
  scrollIntoView() { this.scrolled = true; }
  focus() { this.focused = true; }
}

const els = new Map();
function getEl(id) {
  if (!els.has(id)) els.set(id, new Elem(id));
  return els.get(id);
}

let fetchReply = async () => ({ ok: true, status: 200, json: async () => ({}) });
let rafId = 1;
const rafMap = new Map();
const stTtftHistory = [];

const ttftEl = getEl('st-ttft');
Object.defineProperty(ttftEl, 'textContent', {
  get() { return this._txt || ''; },
  set(v) { this._txt = String(v); stTtftHistory.push(this._txt); },
});

const famBtns = ['ALL', 'gemini', 'claude', 'gpt'].map(f => {
  const b = new Elem('', 'BUTTON');
  b.dataset.f = f;
  return b;
});
const fltBtns = ['all', 'canary', 'issue'].map(k => {
  const b = new Elem('', 'BUTTON');
  b.dataset.k = k;
  return b;
});
const winBtns = ['rolling5h', 'last24h', 'rolling7d', 'allTime'].map(w => {
  const b = new Elem('', 'BUTTON');
  b.dataset.w = w;
  return b;
});

const ctx = {
  console,
  Math, Date, Number, String, Boolean, Array, Object, Set, Map, RegExp, JSON, Promise, Error,
  parseFloat, isNaN,
  location: { protocol: 'http:', href: 'http://127.0.0.1:18891/', search: '', reload() {}, replace() {} },
  history: { replaceState() {} },
  localStorage: { getItem: () => 'rolling5h', setItem() {} },
  sessionStorage: { getItem: () => null, setItem() {} },
  navigator: { clipboard: { writeText: async () => {} } },
  window: { getSelection: () => null },
  performance: { now: () => 100 },
  matchMedia: () => ({ matches: false }),
  requestAnimationFrame: (fn) => { const id = ++rafId; rafMap.set(id, fn); return id; },
  cancelAnimationFrame: (id) => { rafMap.delete(id); },
  setInterval: () => 0,
  setTimeout: () => 0,
  fetch: (...args) => fetchReply(...args),
  document: {
    hidden: false,
    activeElement: null,
    getElementById: (id) => getEl(id),
    addEventListener: () => {},
    querySelectorAll: (sel) => {
      if (sel === '#fam-seg button') return famBtns;
      if (sel === '#flt-seg button') return fltBtns;
      if (sel === '#win-seg button') return winBtns;
      if (sel === '#log-body tr.row') return ctx.__logRows || [];
      return [];
    },
  },
};

vm.runInNewContext(code, ctx);

(async () => {
  // Seed valid dashboard state
  const samplePayload = {
    now: '2026-11-18T12:00:00Z',
    gateway: { uiBuildId: ctx.UI_BUILD_ID, defaultModel: 'gemini-4-carbon', models: [{ id: 'gemini-4-carbon', label: 'Gemini 4 Carbon', family: 'gemini' }], activeSessionsCount: 1, maxLiveWorkers: 8 },
    quota: {
      status: 'ok', updatedAt: '2026-11-18T11:59:00Z',
      geminiPool: { state: 'known', updatedAt: '2026-11-18T11:59:00Z' },
      groups: [{ name: 'Gemini models', buckets: [{ window: '5h', remainingFraction: 0.8, resetTime: '2026-11-18T15:00:00Z' }, { window: 'weekly', remainingFraction: 0.9, resetTime: '2026-11-22T15:00:00Z' }] }],
      deduction: { cap5hUsd: 200, capWeeklyUsd: 800, poolUsed5hUsd: 40, poolUsedWeeklyUsd: 80, localTotal5hUsd: 12.5, localSteps5h: 7, localTotalWeeklyUsd: 35.0, localStepsWeekly: 19, remEffectiveTokensAt99PctCache: 1000000, effectiveRemUsd: 160, bottleneck: '5h' },
      thirdPartyPool: { state: 'not_enabled', available: false, window5h: {}, windowWeekly: {} },
    },
    windows: {
      rolling5h: { stepsCount: 2, alignedCr: 50000, alignedIn: 2000, outTok: 400, thTok: 0, alignedHitRate: 96.2, hits99: 1, avgTtftMs: 850, avgDecodeTps: 120, costUsd: 0.12, savedUsd: 0.09, models: [{ modelId: 'gemini-4-carbon', modelLabel: 'Gemini 4 Carbon', family: 'gemini', steps: 2, rawIn: 2000, rawCr: 50000, outTok: 400, hitRate: 96.2, steadySteps: 1, steadyHitRate: 100, tiers: { '<img src=x onerror=1>': 1 } }] },
      last24h: { stepsCount: 2, avgTtftMs: 1500, models: [] },
      rolling7d: { stepsCount: 2, avgTtftMs: 1800, models: [] },
      allTime: { stepsCount: 2, avgTtftMs: 1800, models: [] },
    },
    canaryRoutes: [{ modelId: 'gemini-4-carbon', modelLabel: 'Gemini 4 Carbon', servedModel: 'gemini-4-carbon-exp', steps: 1, live: true }],
    cacheWarnings: [],
    liveSessions: [{ sessionId: 'sid-1', convKey: 'sid:1', model: 'gemini-4-carbon', modelLabel: 'Gemini 4 Carbon', thoughtLevel: '<svg onload=1>', status: 'IDLE', lastHitRate: '96.2%' }],
    gatewayTurns: [],
    recentDbSteps: [
      { ts: '2026-11-18T11:58:00Z', sessionId: 'sid-1', stepIdx: 1, modelId: 'gemini-4-carbon', modelLabel: 'Gemini 4 Carbon', modelTier: '<b>bad</b>', servedModel: 'gemini-4-carbon-exp', servedMismatch: true, cacheMiss: 'rollover_cancelled_edit', alignedHitRate: '0.0%', totalCtx: 40000, alignedCr: 0, alignedIn: 40000, outTok: 100, ttftMs: 850, streamMs: 250, elapsedMs: 1100, costUsd: 0.08, savedUsd: 0 },
    ],
  };

  fetchReply = async () => ({ ok: true, status: 200, json: async () => samplePayload });
  await ctx.load(false, false);

  // Verify BUG-TEL-05 Gemini pool step counts rendered
  assert(getEl('f-gem').innerHTML.includes('7 步'), 'Gemini 5h step count missing: ' + getEl('f-gem').innerHTML);
  assert(getEl('f-gem').innerHTML.includes('19 步'), 'Gemini weekly step count missing: ' + getEl('f-gem').innerHTML);

  // Verify BUG-UI-04 new rollover_cancelled_edit translated
  assert(getEl('log-body').innerHTML.includes('改词重建'), 'rollover_cancelled_edit short label missing');

  // Verify BUG-UI-08 XSS escaping on tier across lanes, sessions, and log rows
  for (const id of ['lanes', 'sessions', 'log-body']) {
    const h = getEl(id).innerHTML;
    assert(!h.includes('<img src=x') && !h.includes('<svg onload=1>') && !h.includes('<b>bad</b>'), 'Unescaped tier HTML in ' + id + ': ' + h);
  }

  // Verify BUG-UI-05 cross-unit tween (850 ms -> 1.5 s must NOT flash 850.0) & RAF cancellation
  ctx.render(samplePayload, false);
  stTtftHistory.length = 0;
  winBtns[1].closest = () => winBtns[1]; // 850ms ('毫秒') -> 1.5 ('秒')
  getEl('win-seg').dispatch('click', { target: winBtns[1] });
  for (const frameFn of [...rafMap.values()]) frameFn(250);
  assert.deepStrictEqual(stTtftHistory, ['1.5'], 'Cross-unit switch must jump directly to 1.5 without tweening ms->s: ' + JSON.stringify(stTtftHistory));

  // Same-unit switch (1.5 '秒' -> 1.8 '秒') tweens smoothly & rapid re-switch cancels prior RAFs
  rafMap.clear();
  winBtns[2].closest = () => winBtns[2];
  getEl('win-seg').dispatch('click', { target: winBtns[2] });
  assert(rafMap.size >= 1 && rafMap.size <= 2, 'Expected 1-2 active RAFs (st-hit & st-ttft), got ' + rafMap.size);
  const priorRafIds = [...rafMap.keys()];
  winBtns[1].closest = () => winBtns[1];
  getEl('win-seg').dispatch('click', { target: winBtns[1] });
  assert(priorRafIds.every(id => !rafMap.has(id)) && rafMap.size <= 2, 'Expected all prior RAFs canceled on rapid re-switch');

  // Verify BUG-UI-03 HTTP 500 error JSON does not poison state.data
  fetchReply = async () => ({ ok: false, status: 500, json: async () => ({ error: 'boom' }) });
  await ctx.load(false, true);
  assert.strictEqual(getEl('live-txt').textContent, '连接断开');
  assert(getEl('sys-banners').innerHTML.includes('灰度路由观察'), 'Banners wiped by 500 response');

  // Verify BUG-UI-07 focusCanaryLogs clears conflicting filters & updates canaryCnt
  famBtns[2].closest = () => famBtns[2]; // select claude
  getEl('fam-seg').dispatch('click', { target: famBtns[2] });
  assert.strictEqual(getEl('flt-canary-btn').textContent, '仅灰度'); // 0 claude canary rows
  ctx.focusCanaryLogs();
  assert.strictEqual(getEl('flt-canary-btn').textContent, '仅灰度 (1)');
  assert(getEl('log-body').innerHTML.includes('gemini-4-carbon-exp'), 'focusCanaryLogs failed to clear family filter');

  // Verify BUG-UI-06 #log-body Enter/Space keydown expands & collapses row
  const rowEl = new Elem('', 'TR');
  rowEl.dataset.k = 'db:sid-1:1';
  rowEl.closest = (sel) => sel === 'tr.row' ? rowEl : null;
  ctx.__logRows = [rowEl];
  let prevented = false;
  getEl('log-body').dispatch('keydown', { key: 'Enter', target: rowEl, preventDefault() { prevented = true; } });
  assert(prevented && getEl('log-body').innerHTML.includes('换会话 (中断后修改重发)'), 'Enter keydown did not expand detail row');
  assert(rowEl.focused, 'Row focus not restored after expand');

  // Verify BUG-UI-02 renderUpgrade with approvalPending disables #up-ok and shows 已确认，等待执行
  fetchReply = async () => ({
    ok: true, status: 200,
    json: async () => ({
      enabled: true,
      approvalPending: true,
      pendingSwitch: false,
      engine: { alive: true, running: '/data/agy/1.5.9-aaaa/agy-acp-900k' },
      status: { updatedAt: '2026-11-18T11:55:00Z', current: { agy: '1.5.9', gateway: 'abcdef1234567890' }, candidate: { id: 'agy-1.5.10', kind: 'agy', version: '1.5.10', sha256: 'a'.repeat(64), state: 'ready' }, gates: [] },
    }),
  });
  await ctx.loadUpgrade();
  const upHTML = getEl('up-body').innerHTML;
  assert(upHTML.includes('已确认，等待执行'), 'approvalPending missing 已确认，等待执行: ' + upHTML);
  assert(/id="up-ok"[^>]*disabled/.test(upHTML), '#up-ok must be disabled when approvalPending=true: ' + upHTML);

  // Verify OPT-OBS-01 & BUG-UI-08: lastCheckError surfaced when !st, when c.state=failed without c.message, and when !c;
  // plus Object.hasOwn guards on MISS / MISS_SHORT / UP_KIND / UP_STATE against Object.prototype keys.
  fetchReply = async () => ({
    ok: true, status: 200,
    json: async () => ({ enabled: true, approvalPending: false, lastCheckError: 'status.json unreadable', status: null }),
  });
  await ctx.loadUpgrade();
  assert.strictEqual(getEl('up').hidden, false, '#up must unhide when !status && lastCheckError');
  assert(getEl('up-body').innerHTML.includes('status.json unreadable'), 'Missing status.json unreadable in #up-body: ' + getEl('up-body').innerHTML);

  fetchReply = async () => ({
    ok: true, status: 200,
    json: async () => ({
      enabled: true,
      approvalPending: false,
      lastCheckError: 'G3 ACP 契约: failed',
      status: { current: { agy: '1.5.9', gateway: 'abcdef123456' }, candidate: { id: 'agy-1.5.11', kind: 'constructor', version: '1.5.11', state: 'failed' } },
    }),
  });
  await ctx.loadUpgrade();
  const upFailHTML = getEl('up-body').innerHTML;
  assert(upFailHTML.includes('G3 ACP 契约: failed'), 'Missing fallback lastCheckError on failed candidate: ' + upFailHTML);
  assert(!upFailHTML.includes('function '), 'Prototype key in UP_KIND leaked function source: ' + upFailHTML);

  fetchReply = async () => ({
    ok: true, status: 200,
    json: async () => ({
      enabled: true,
      approvalPending: false,
      lastCheckError: '',
      status: { current: { agy: '1.5.9', gateway: 'abcdef123456' }, candidate: { id: 'agy-1.5.12', kind: 'agy', version: '1.5.12', state: 'toString' } },
    }),
  });
  await ctx.loadUpgrade();
  assert(!getEl('up-body').innerHTML.includes('function '), 'Prototype key in UP_STATE leaked function source: ' + getEl('up-body').innerHTML);

  fetchReply = async () => ({
    ok: true, status: 200,
    json: async () => ({
      enabled: true,
      approvalPending: false,
      lastCheckError: 'manifest fetch timeout',
      status: { current: { agy: '1.5.9', gateway: 'abcdef123456' }, candidate: null },
    }),
  });
  await ctx.loadUpgrade();
  assert(getEl('up-body').innerHTML.includes('manifest fetch timeout'), 'Missing lastCheckError when !candidate: ' + getEl('up-body').innerHTML);

  const protoPayload = JSON.parse(JSON.stringify(samplePayload));
  protoPayload.recentDbSteps[0].cacheMiss = 'toString';
  ctx.render(protoPayload, false);
  const protoLogHTML = getEl('log-body').innerHTML;
  assert(!protoLogHTML.includes('function '), 'Prototype key in MISS / MISS_SHORT leaked function source: ' + protoLogHTML);
})().catch(err => {
  console.error(err);
  process.exit(1);
});
`
	harnessFile := filepath.Join(tmp, "run_vm.js")
	if err := os.WriteFile(harnessFile, []byte(harness), 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command(nodeBin, harnessFile, jsFile).CombinedOutput(); err != nil {
		t.Fatalf("dashboard.html VM behavioral harness failed: %v\n%s", err, string(out))
	}
}
