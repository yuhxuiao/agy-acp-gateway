#!/usr/bin/env python3
"""Real-upstream prompt-cache probes (merged from probe_cache / probe_cache_resume / probe_cache_sticky).

All subcommands talk ACP directly to agy (unless basic --via-gateway) with an isolated --gemini-dir under
runtime/upgrades/probes/ holding only the agent plugin and a symlink to the existing login; the token is
never read or printed. Without --live only the plan and a worst-case quota cost are printed. Every live run
holds the shared upgrade lock (same lock as agy_upgrade.py / quota_calibrate.py / probe_config_ack.py), so
probes never overlap candidate builds or quota experiments. The 18890/18892 instances are not touched.

Subcommands:
  basic   --run name=model,tier,tokens,gaps (repeatable). gaps is a ';' list, one per follow-up turn:
            30 wait 30s | c30 wait, close+resume | m30 wait, switch model away and back | t30 toggle tier
          Runs concurrently in one agy process. After every direct-ACP turn the latest gen_metadata request
          snapshot (message hashes only) is compared with the previous one (prefix_diff).
          --via-gateway sends the same plan through an isolated gateway instance on --port.
  resume  sticky turns vs turns after session/close + session/resume (gateway-identical MCP answers).
  sticky  one big first message then --turns follow-ups adding --add-tokens each; per-turn cache hit and
          snapshot prefix comparison; aborts above --budget USD (measured quota weights).

Examples:
  scripts/probe_cache.py basic --run a=gemini-4-argon,low,30000,3;3;60;c5 --live
  scripts/probe_cache.py resume --model gemini-3.8-flash --ctx-tokens 40000 --live
  scripts/probe_cache.py sticky --model gemini-4-carbon --ctx-tokens 150000 --turns 8 --budget 2.5 --live
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import random
import shutil
import signal
import sqlite3
import string
import subprocess
import sys
import threading
import time
from http.server import ThreadingHTTPServer

sys.path.insert(0, str(Path(__file__).resolve().parent))
import agy_upgrade as U  # noqa: E402
import probe_config_ack as P  # noqa: E402
import quota_calibrate as Q  # noqa: E402

ROOT = U.ROOT
# (input, cached read, output) USD per 1M, measured quota weights (history/release-20261111-quota.md)
PRICE = {"gemini-4-argon": (2.0, 0.10, 10.0), "gemini-4-carbon": (2.0, 0.10, 10.0), "gemini-4-neon": (1.0, 0.05, 5.0), "gemini-4-hydrogen": (0.50, 0.025, 2.50),
         "gemini-3.8-flash": (0.25, 0.025, 1.25), "claude-sonnet-5-5": (2.0, 0.10, 10.0), "claude-opus-5-5": (4.0, 0.20, 20.0)}
OTHER = {"gemini-4-argon": "gemini-3.8-flash", "gemini-4-carbon": "gemini-3.8-flash", "gemini-4-neon": "gemini-3.8-flash", "gemini-4-hydrogen": "gemini-3.8-flash",
         "gemini-3.8-flash": "gemini-4-argon", "claude-sonnet-5-5": "claude-opus-5-5", "claude-opus-5-5": "claude-sonnet-5-5"}
WORDS = "alpha beta gamma delta kernel socket buffer packet router quota cache token stream session parser vector matrix".split()
INIT = {"protocolVersion": 1, "clientCapabilities": {"fs": {"readTextFile": False, "writeTextFile": False}, "terminal": False},
        "clientInfo": {"name": "agy-acp-gateway", "version": "probe"}}
LOCAL_MCP_AUTH = "Bearer probe-local"  # the local mimic MCP server ignores auth


# ------------------------------------------------------------------ shared plumbing

class Acp:
    def __init__(self, binary, home, env):
        self.p = subprocess.Popen([str(binary), "acp", "--gemini-dir", str(home), "--app-data-dir", "antigravity-cli",
                                   "--agent", "dsh-bridge", "--process-per-session", "--disable-telemetry"],
                                  stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env=env, text=True,
                                  cwd="/tmp", start_new_session=True)
        self.n, self.res, self.ev, self.text, self.lock = 0, {}, {}, {}, threading.Lock()
        threading.Thread(target=self.reader, daemon=True).start()

    def reader(self):
        for line in self.p.stdout:
            try:
                m = json.loads(line)
            except Exception:
                continue
            if "id" in m and "method" not in m:
                self.res[m["id"]] = m
                self.ev[m["id"]].set()
            elif m.get("method") == "session/update":
                p = m.get("params") or {}
                u = p.get("update") or {}
                if u.get("sessionUpdate") == "agent_message_chunk":
                    self.text.setdefault(p.get("sessionId"), []).append(((u.get("content") or {}).get("text")) or "")
            elif "id" in m:
                self.send({"jsonrpc": "2.0", "id": m["id"], "error": {"code": -32601, "message": "not supported by probe"}})

    def send(self, obj):
        with self.lock:
            self.p.stdin.write(json.dumps(obj) + "\n")
            self.p.stdin.flush()

    def rpc(self, method, params, timeout=600):
        with self.lock:
            self.n += 1
            i = self.n
            self.ev[i] = threading.Event()
        t = time.time()
        self.send({"jsonrpc": "2.0", "id": i, "method": method, "params": params})
        if not self.ev[i].wait(timeout):
            return {"error": {"message": "timeout"}, "ms": int((time.time() - t) * 1000)}
        r = self.res.pop(i)
        r["ms"] = int((time.time() - t) * 1000)
        return r

    def close(self):
        try:
            os.killpg(self.p.pid, signal.SIGTERM)
            self.p.wait(10)
        except Exception:
            pass


def isolated_home(stage):
    home = stage / "home"
    (home / "config").mkdir(parents=True, mode=0o700)
    shutil.copytree(ROOT / "runtime/pure-api-home/config/plugins", home / "config/plugins")
    U.prepare_auth_link(home)
    proxy = os.environ.get("AGY_UPSTREAM_PROXY", "http://127.0.0.1:7890")
    env = dict(os.environ, HOME=str(home), DBUS_SESSION_BUS_ADDRESS="unix:path=/dev/null", HTTPS_PROXY=proxy, HTTP_PROXY=proxy,
               NO_PROXY="127.0.0.1,localhost,::1", no_proxy="127.0.0.1,localhost,::1")
    return home, env


def mimic_mcp():
    """Local MCP server answering exactly like the gateway (GET 405, listChanged false, no session id)."""
    P.MIMIC = True
    srv = ThreadingHTTPServer(("127.0.0.1", 0), P.Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{srv.server_address[1]}/mcp"
    return srv, [{"name": "dsh-bridge", "type": "http", "url": url, "headers": [{"name": "Authorization", "value": LOCAL_MCP_AUTH}]}]


def pin(acp, sid, model, tier):
    for cid, v in (("model", model), ("thought_level", tier)):
        r = acp.rpc("session/set_config_option", {"sessionId": sid, "configId": cid, "value": v}, 60)
        if r.get("error"):
            raise RuntimeError(f"set {cid}={v}: {r['error']}")


def random_text(tokens, rnd, chars_per_token=3.9):
    nonce = "".join(rnd.choices(string.ascii_lowercase + string.digits, k=20))
    words = [f"[{nonce}]"]
    while len(words) < tokens / chars_per_token:
        words.append(rnd.choice(WORDS) + str(rnd.randint(0, 999)))
    return " ".join(words)


def usage_cost(model, u):
    pin_, pcr, pout = PRICE[model]
    return ((u.get("inputTokens") or 0) * pin_ + (u.get("cachedReadTokens") or 0) * pcr + (u.get("outputTokens") or 0) * pout) / 1e6


def stage_dir(prefix):
    stage = U.checked_path(U.STATE / "probes" / (prefix + "-" + time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())))
    stage.mkdir(parents=True, mode=0o700)
    return stage


def jsonl_writer(path):
    lock = threading.Lock()

    def out(rec):
        with lock:
            with path.open("a") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            print(json.dumps(rec, ensure_ascii=False), flush=True)
    return out


# ------------------------------------------------------------------ gen_metadata snapshot comparison

def _varint(b, i):
    r = s = 0
    while True:
        c = b[i]
        i += 1
        r |= (c & 0x7f) << s
        s += 7
        if c < 0x80:
            return r, i


def _fields(b, path="", out=None, depth=0):
    """Flatten protobuf to (path, value); strings/bytes are hashed, never kept."""
    out = [] if out is None else out
    i = 0
    try:
        while i < len(b):
            tag, i = _varint(b, i)
            fn, wt = tag >> 3, tag & 7
            if fn == 0:
                return None
            p = f"{path}.{fn}"
            if wt == 0:
                v, i = _varint(b, i)
                out.append((p, v))
            elif wt == 1:
                i += 8
            elif wt == 5:
                i += 4
            elif wt == 2:
                n, i = _varint(b, i)
                sub = b[i:i + n]
                i += n
                tmp = []
                if depth < 8 and n and _fields(sub, p, tmp, depth + 1) is not None and tmp:
                    out.extend(tmp)
                else:
                    out.append((p, "H" + hashlib.sha1(sub).hexdigest()[:12] + ":%d" % n))
            else:
                return None
    except Exception:
        return None
    return out


def latest_snapshot(db):
    """Request snapshot of the newest generation (agy keeps it only on the latest row)."""
    if not db.exists():
        return None
    c = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        rows = c.execute("select idx, data from gen_metadata order by idx desc limit 3").fetchall()
    except sqlite3.Error:
        return None
    finally:
        c.close()
    for idx, data in rows:
        f = _fields(data) or []
        msgs = [v for p, v in f if p == ".1.2.3"]
        if msgs:
            return {"idx": idx, "msgs": msgs, "system": [v for p, v in f if p in (".1.1", ".1.16.2")]}
    return None


def prefix_diff(prev, snap):
    if not (prev and snap):
        return None
    a, b = prev["msgs"], snap["msgs"]
    first = next((i for i in range(min(len(a), len(b))) if a[i] != b[i]), None)
    return {"prev_len": len(a), "len": len(b), "first_changed": first, "system_same": prev["system"] == snap["system"]}


# ------------------------------------------------------------------ basic

def parse_run(spec):
    name, rest = spec.split("=", 1)
    model, tier, tokens, gaps = rest.split(",", 3)
    ops = []
    for g in [x for x in gaps.split(";") if x]:
        kind = g[0] if g[0] in "cmt" else ""
        ops.append((kind, int(g[len(kind):])))
    return {"name": name, "model": model, "tier": tier, "tokens": int(tokens), "ops": ops}


def basic_session(acp, home, run, out, t0):
    fam = Q.family(run["model"])
    meta = {"antigravity": {"systemPrompt": {"append": "Reply exactly as instructed. Do not use any tools."}}}
    new = acp.rpc("session/new", {"cwd": "/tmp", "mcpServers": [], "_meta": meta}, 180)
    sid = ((new.get("result") or {}).get("sessionId"))
    if not sid:
        out({"name": run["name"], "error": "session/new failed", "detail": str(new.get("error"))[:300]})
        return
    pin(acp, sid, run["model"], run["tier"])
    db = home / "antigravity-cli/conversations" / f"{sid}.db"
    prev_total, last_end, prev_snap = 0, None, None
    for i, (kind, gap) in enumerate([("", 0)] + run["ops"]):
        if i == 0:
            text = "Reply with exactly OK. Context only, do not analyze:\n\n" + Q.context_text(run["tokens"], fam)
        else:
            time.sleep(gap)
            text = f"Reply with exactly OK{i + 1}."
        note = ""
        if kind == "c":
            acp.rpc("session/close", {"sessionId": sid}, 30)
            r = acp.rpc("session/resume", {"sessionId": sid, "cwd": "/tmp", "mcpServers": [], "_meta": meta}, 180)
            note = "resume" if not r.get("error") else "resume_failed:" + str(r["error"])[:120]
            pin(acp, sid, run["model"], run["tier"])
        elif kind == "m":
            pin(acp, sid, OTHER[run["model"]], run["tier"])
            pin(acp, sid, run["model"], run["tier"])
            note = "model_roundtrip"
        elif kind == "t":
            pin(acp, sid, run["model"], "high" if run["tier"] != "high" else "low")
            pin(acp, sid, run["model"], run["tier"])
            note = "tier_roundtrip"
        acp.text[sid] = []
        idle = round(time.time() - last_end, 1) if last_end else 0
        r = acp.rpc("session/prompt", {"sessionId": sid, "prompt": [{"type": "text", "text": text}]}, 900)
        last_end = time.time()
        u = (r.get("result") or {}).get("usage") or {}
        rin, rcr = u.get("inputTokens", 0), u.get("cachedReadTokens", 0)
        time.sleep(1.5)
        snap = latest_snapshot(db)
        out({"name": run["name"], "turn": i + 1, "t": round(time.time() - t0), "model": run["model"], "op": kind + str(gap), "note": note,
             "idle_s": idle, "in": rin, "cr": rcr, "cw": u.get("cachedWriteTokens", 0), "out": u.get("outputTokens", 0),
             "thought": u.get("thoughtTokens", 0), "ms": r["ms"], "hit_of_prev": round(rcr / prev_total, 3) if prev_total else None,
             "prefix_diff": prefix_diff(prev_snap, snap), "stop": (r.get("result") or {}).get("stopReason"),
             "error": str(r["error"])[:200] if r.get("error") else None, "text": "".join(acp.text.get(sid, []))[:120]})
        prev_snap = snap or prev_snap
        if rin + rcr:
            prev_total = rin + rcr
    acp.rpc("session/close", {"sessionId": sid}, 30)


def basic_gateway(run, port, log, out, t0, tools):
    """Same turn plan through the gateway /v1/messages (only plain gaps are meaningful here)."""
    fam = Q.family(run["model"])
    sid = f"cprobe-{run['name']}-{time.time_ns()}"
    msgs, prev_total, last_end = None, 0, None
    for i, (kind, gap) in enumerate([("", 0)] + run["ops"]):
        if i == 0:
            msgs = [{"role": "user", "content": "Reply with exactly OK. Context only, do not analyze:\n\n" + Q.context_text(run["tokens"], fam)}]
        else:
            time.sleep(gap)
            msgs.append({"role": "user", "content": f"Reply with exactly OK{i + 1}."})
        idle = round(time.time() - last_end, 1) if last_end else 0
        t = time.time()
        try:
            resp, msgs = Q.post(port, f"{run['model']}-{run['tier']}", msgs, sid, tools, 64)
        except Exception as e:  # noqa: BLE001
            out({"name": run["name"], "turn": i + 1, "error": str(e)[:200]})
            return
        last_end = time.time()
        turn = Q.last_turn(log)
        rin = turn.get("rawUncachedInputTokens", turn.get("inputTokens", 0))
        rcr = turn.get("rawTpuCachedTokens", turn.get("cachedReadTokens", 0))
        out({"name": run["name"], "turn": i + 1, "t": round(time.time() - t0), "model": run["model"], "op": str(gap), "note": "gateway:" + str(turn.get("mode")),
             "idle_s": idle, "in": rin, "cr": rcr, "out": turn.get("outputTokens", 0), "ms": int((last_end - t) * 1000),
             "hit_of_prev": round(rcr / prev_total, 3) if prev_total else None, "stop": resp.get("stop_reason"), "error": None,
             "text": "".join(c.get("text", "") for c in resp.get("content", []) if c.get("type") == "text")[:120]})
        if rin + rcr:
            prev_total = rin + rcr


def cmd_basic(args):
    runs = [parse_run(s) for s in args.run]
    worst = sum(r["tokens"] * (1 + len(r["ops"])) * PRICE[r["model"]][0] / 1e6 for r in runs)
    secs = max(sum(g for _, g in r["ops"]) for r in runs)
    print(json.dumps({"probe": "basic", "runs": len(runs), "turns": sum(1 + len(r["ops"]) for r in runs),
                      "worst_case_usd_if_never_cached": round(worst, 3), "min_duration_s": secs, "bin": args.bin}))
    if not args.live:
        print("dry run: add --live to send these requests")
        return 0
    with U.upgrade_lock():
        stage = stage_dir("cache")
        out, t0 = jsonl_writer(stage / "results.jsonl"), time.time()
        if args.via_gateway:
            with Q.instance(stage, args.port) as genv:
                for r in runs:
                    basic_gateway(r, args.port, genv["AGY_GW_LOG"], out, t0, not args.no_tools)
            print("RESULTS", stage / "results.jsonl")
            return 0
        home, env = isolated_home(stage)
        acp = Acp(args.bin, home, env)
        try:
            init = acp.rpc("initialize", INIT, 90)
            out({"agent": ((init.get("result") or {}).get("agentInfo"))})
            threads = []
            for r in runs:
                th = threading.Thread(target=basic_session, args=(acp, home, r, out, t0), daemon=True)
                th.start()
                threads.append(th)
                time.sleep(args.stagger)
            for th in threads:
                th.join()
        finally:
            acp.close()
    print("RESULTS", stage / "results.jsonl")
    return 0


# ------------------------------------------------------------------ resume

def cmd_resume(args):
    plan = ["t1_cold", "t2_sticky", "t3_sticky", "t4_after_resume", "t5_sticky_after_resume"]
    worst = args.ctx_tokens * len(plan) * PRICE[args.model][0] / 1e6
    print(json.dumps({"probe": "resume", "model": args.model, "tier": args.tier, "turns": len(plan),
                      "worst_case_usd_if_never_cached": round(worst, 3)}))
    if not args.live:
        print("dry run: add --live to send these requests")
        return 0
    rows = []
    with U.upgrade_lock():
        stage = stage_dir("cache-resume")
        home, env = isolated_home(stage)
        srv, mcp = mimic_mcp()
        acp = Acp(args.bin, home, env)
        meta = {"antigravity": {"systemPrompt": {"append": "Answer tersely."}}}
        db = None

        def turn(sid, label, text, prev):
            acp.text[sid] = []
            r = acp.rpc("session/prompt", {"sessionId": sid, "prompt": [{"type": "text", "text": text}]}, 180)
            u = (r.get("result") or {}).get("usage") or {}
            time.sleep(1.5)
            snap = latest_snapshot(db)
            row = {"label": label, "t": round(time.time() - P.T0, 1), "ms": r["ms"], "stop": (r.get("result") or {}).get("stopReason"),
                   "input": u.get("inputTokens"), "cachedRead": u.get("cachedReadTokens"), "output": u.get("outputTokens"),
                   "prefix_diff": prefix_diff(prev, snap), "error": r.get("error")}
            rows.append(row)
            print(json.dumps(row), flush=True)
            time.sleep(args.gap)
            return snap or prev

        try:
            acp.rpc("initialize", INIT, 90)
            new = acp.rpc("session/new", {"cwd": "/tmp", "mcpServers": mcp, "_meta": meta}, 120)
            sid = ((new.get("result") or {}).get("sessionId"))
            if not sid:
                raise RuntimeError(f"session/new failed: {new.get('error')}")
            db = home / "antigravity-cli/conversations" / f"{sid}.db"
            pin(acp, sid, args.model, args.tier)
            snap = turn(sid, plan[0], "Context follows; reply only OK.\n" + random_text(args.ctx_tokens, random.Random(), 1.33), None)
            snap = turn(sid, plan[1], "Reply only OK2.", snap)
            snap = turn(sid, plan[2], "Reply only OK3.", snap)
            acp.rpc("session/close", {"sessionId": sid}, 20)
            res = acp.rpc("session/resume", {"sessionId": sid, "cwd": "/tmp", "mcpServers": mcp, "_meta": meta}, 120)
            if res.get("error"):
                raise RuntimeError(f"resume failed: {res['error']}")
            pin(acp, sid, args.model, args.tier)
            snap = turn(sid, plan[3], "Reply only OK4.", snap)
            turn(sid, plan[4], "Reply only OK5.", snap)
            acp.rpc("session/close", {"sessionId": sid}, 20)
        finally:
            acp.close()
            srv.shutdown()
            (stage / "results.json").write_text(json.dumps({"args": vars(args), "rows": rows, "mcp_log": P.MCP.log}, indent=1, default=str))
            print("RESULTS", stage / "results.json")
    return 0


# ------------------------------------------------------------------ sticky

def cmd_sticky(args):
    worst = sum(args.ctx_tokens + n * args.add_tokens for n in range(args.turns)) * PRICE[args.model][0] / 1e6
    print(json.dumps({"probe": "sticky", "model": args.model, "turns": args.turns, "budget_usd": args.budget,
                      "worst_case_usd_if_never_cached": round(worst, 3)}))
    if not args.live:
        print("dry run: add --live to send these requests")
        return 0
    rows, spent = [], 0.0
    with U.upgrade_lock():
        stage = stage_dir("cache-sticky-" + args.model)
        home, env = isolated_home(stage)
        srv, mcp = mimic_mcp()
        acp = Acp(args.bin, home, env)
        rnd, prev_snap = random.Random(), None
        try:
            acp.rpc("initialize", INIT, 90)
            new = acp.rpc("session/new", {"cwd": "/tmp", "mcpServers": mcp, "_meta": {"antigravity": {"systemPrompt": {"append": "Answer tersely."}}}}, 120)
            sid = ((new.get("result") or {}).get("sessionId"))
            if not sid:
                raise RuntimeError(f"session/new failed: {new.get('error')}")
            pin(acp, sid, args.model, args.tier)
            db = home / "antigravity-cli/conversations" / f"{sid}.db"
            for n in range(args.turns):
                body = ("Context follows; reply only OK.\n" + random_text(args.ctx_tokens, rnd)) if n == 0 else \
                       (f"More data #{n}; reply only OK{n}.\n" + random_text(args.add_tokens, rnd))
                acp.text[sid] = []
                r = acp.rpc("session/prompt", {"sessionId": sid, "prompt": [{"type": "text", "text": body}]}, 300)
                u = (r.get("result") or {}).get("usage") or {}
                cost = usage_cost(args.model, u)
                spent += cost
                time.sleep(1.5)
                snap = latest_snapshot(db)
                row = {"turn": n, "t": round(time.time() - P.T0, 1), "ms": r["ms"], "stop": (r.get("result") or {}).get("stopReason"),
                       "input": u.get("inputTokens") or 0, "cachedRead": u.get("cachedReadTokens") or 0, "output": u.get("outputTokens") or 0,
                       "cost": round(cost, 4), "spent": round(spent, 3), "snapshot": bool(snap), "prefix_diff": prefix_diff(prev_snap, snap),
                       "error": r.get("error"), "text": "".join(acp.text.get(sid, []))[:80]}
                rows.append(row)
                print(json.dumps(row, ensure_ascii=False), flush=True)
                prev_snap = snap or prev_snap
                if spent > args.budget:
                    print("budget reached, stopping", flush=True)
                    break
                time.sleep(args.gap)
            acp.rpc("session/close", {"sessionId": sid}, 20)
        finally:
            acp.close()
            srv.shutdown()
            (stage / "results.json").write_text(json.dumps({"args": vars(args), "rows": rows, "spent": spent}, indent=1, default=str))
            print("RESULTS", stage / "results.json", "spent~$%.3f" % spent)
    return 0


def build_parser():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--live", action="store_true", help="send real requests (uses real quota)")
    common.add_argument("--bin", default=str(ROOT / "bin/agy-acp-900k"), help="agy binary (e.g. an older one under backups/)")
    b = sub.add_parser("basic", parents=[common], help="concurrent turn plans, optional gateway path")
    b.add_argument("--run", action="append", required=True, help="name=model,tier,tokens,gaps")
    b.add_argument("--via-gateway", action="store_true", help="send the plan through an isolated gateway instance on --port")
    b.add_argument("--port", type=int, default=18892)
    b.add_argument("--no-tools", action="store_true", help="gateway mode: send without tools / Claude Code session header")
    b.add_argument("--stagger", type=float, default=2.0, help="seconds between session starts")
    r = sub.add_parser("resume", parents=[common], help="sticky vs after close+resume")
    r.add_argument("--model", default="gemini-3.8-flash", choices=sorted(PRICE))
    r.add_argument("--tier", default="low")
    r.add_argument("--ctx-tokens", type=int, default=40000)
    r.add_argument("--gap", type=float, default=8.0, help="seconds between prompts")
    s = sub.add_parser("sticky", parents=[common], help="growing context, per-turn hit and prefix comparison")
    s.add_argument("--model", default="gemini-4-carbon", choices=sorted(PRICE))
    s.add_argument("--tier", default="low")
    s.add_argument("--ctx-tokens", type=int, default=150000)
    s.add_argument("--add-tokens", type=int, default=3000)
    s.add_argument("--turns", type=int, default=8)
    s.add_argument("--gap", type=float, default=8.0)
    s.add_argument("--budget", type=float, default=2.5)
    return ap


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0].startswith("--") and argv[0] not in ("--help", "-h"):
        argv.insert(0, "basic")  # legacy form: probe_cache.py --run ... [--live]
    args = build_parser().parse_args(argv)
    return {"basic": cmd_basic, "resume": cmd_resume, "sticky": cmd_sticky}[args.cmd](args)


if __name__ == "__main__":
    os.umask(0o077)
    raise SystemExit(main())
