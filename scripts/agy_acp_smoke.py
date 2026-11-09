#!/usr/bin/env python3
"""ACP contract smoke test for an agy binary. No prompt is sent (no quota).

Checks exactly what the gateway depends on, so an upstream agy release that
changes behaviour fails here before it reaches bin/agy-acp-900k:
  * initialize: loadSession, sessionCapabilities.resume/close, mcpCapabilities.http,
    promptCapabilities.image, _meta.antigravity.systemPrompt.append
  * session/new: configOptions "model" offers every gateway model id,
    "thought_level" offers low/medium/high
  * session/set_config_option model + thought_level accepted
  * session/resume restores without replaying history; session/close works

Usage: agy_acp_smoke.py <agy-binary> [--gemini-dir DIR] [--json]
Exit 0 = all checks passed, 1 = contract changed, 2 = could not run.
"""
import argparse, json, os, subprocess, sys, threading, time

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
GATEWAY_MODELS = ["gemini-4-argon", "gemini-3.8-flash", "claude-opus-5-5", "claude-sonnet-5-5", "gpt-oss-120b-medium"]
TIERS = ["low", "medium", "high"]

ap = argparse.ArgumentParser()
ap.add_argument("binary")
ap.add_argument("--gemini-dir", required=True, help="isolated candidate state directory")
ap.add_argument("--cwd", default=os.environ.get("AGY_DEFAULT_CWD", "/tmp"))
ap.add_argument("--json", action="store_true")
ap.add_argument("--report", help="write machine-readable contract evidence")
args = ap.parse_args()

proxy = os.environ.get("AGY_UPSTREAM_PROXY", "http://127.0.0.1:7890")
env = dict(os.environ, HTTPS_PROXY=proxy, HTTP_PROXY=proxy, NO_PROXY="127.0.0.1,localhost,::1", no_proxy="127.0.0.1,localhost,::1")
try:
    p = subprocess.Popen([args.binary, "acp", "--gemini-dir", args.gemini_dir, "--app-data-dir", "antigravity-cli",
                          "--process-per-session", "--disable-telemetry"],
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                         env=env, text=True, start_new_session=True, cwd=args.cwd if os.path.isdir(args.cwd) else ROOT)
except OSError as exc:
    print(f"error: cannot start {args.binary}: {exc}")
    sys.exit(2)

res, ev, updates = {}, {}, []
lock = threading.Lock()

def reader():
    for line in p.stdout:
        try:
            m = json.loads(line)
        except Exception:
            continue
        if "id" in m and ("result" in m or "error" in m) and "method" not in m:
            res[m["id"]] = m
            ev.setdefault(m["id"], threading.Event()).set()
        elif m.get("method") == "session/update":
            with lock:
                updates.append(((m.get("params") or {}).get("update") or {}).get("sessionUpdate"))
        elif m.get("method") == "session/request_permission" and "id" in m:
            p.stdin.write(json.dumps({"jsonrpc": "2.0", "id": m["id"], "result": {"outcome": {"outcome": "cancelled"}}}) + "\n")
            p.stdin.flush()

threading.Thread(target=reader, daemon=True).start()

def rpc(i, method, params, timeout=90):
    ev.setdefault(i, threading.Event())
    p.stdin.write(json.dumps({"jsonrpc": "2.0", "id": i, "method": method, "params": params}) + "\n")
    p.stdin.flush()
    ev[i].wait(timeout)
    return res.get(i)

checks, info = [], {}
def check(name, ok, detail=""):
    checks.append({"check": name, "ok": bool(ok), "detail": detail})

def opt_values(config_options, oid):
    for o in config_options or []:
        if o.get("id") == oid:
            return [x.get("value") for x in o.get("options") or []]
    return None

try:
    init = rpc(1, "initialize", {"protocolVersion": 1, "clientCapabilities": {"fs": {"readTextFile": False, "writeTextFile": False}, "terminal": False},
                                 "clientInfo": {"name": "agy-acp-gateway-smoke", "version": "1"}})
    r = (init or {}).get("result") or {}
    caps = r.get("agentCapabilities") or {}
    info["agentInfo"] = r.get("agentInfo"); info["protocolVersion"] = r.get("protocolVersion")
    sc = caps.get("sessionCapabilities") or {}
    check("initialize", bool(r), json.dumps((init or {}).get("error")) if not r else "")
    check("protocolVersion==1", r.get("protocolVersion") == 1, str(r.get("protocolVersion")))
    check("cap.loadSession", caps.get("loadSession") is True)
    check("cap.session.resume", "resume" in sc)
    check("cap.session.close", "close" in sc)
    check("cap.mcp.http", (caps.get("mcpCapabilities") or {}).get("http") is True)
    check("cap.prompt.image", (caps.get("promptCapabilities") or {}).get("image") is True)
    check("cap._meta.antigravity.systemPrompt.append",
          (((caps.get("_meta") or {}).get("antigravity") or {}).get("systemPrompt") or {}).get("append") is True)

    new = rpc(2, "session/new", {"cwd": args.cwd, "mcpServers": []})
    nr = (new or {}).get("result") or {}
    sid = nr.get("sessionId")
    check("session/new", bool(sid), json.dumps((new or {}).get("error")) if not sid else "")
    models = opt_values(nr.get("configOptions"), "model") or []
    tiers = opt_values(nr.get("configOptions"), "thought_level") or []
    info["models"] = models; info["thought_level"] = tiers
    missing = [m for m in GATEWAY_MODELS if m not in models]
    check("config.model offers gateway models", not missing, f"missing={missing}" if missing else "")
    check("config.thought_level low/medium/high", all(t in tiers for t in TIERS), f"got={tiers}")
    if sid:
        sm = rpc(3, "session/set_config_option", {"sessionId": sid, "configId": "model", "value": "gemini-3.8-flash"}, 30)
        check("set_config_option model", bool((sm or {}).get("result")), json.dumps((sm or {}).get("error")) if not (sm or {}).get("result") else "")
        st = rpc(4, "session/set_config_option", {"sessionId": sid, "configId": "thought_level", "value": "low"}, 30)
        check("set_config_option thought_level", bool((st or {}).get("result")), json.dumps((st or {}).get("error")) if not (st or {}).get("result") else "")
        rpc(5, "session/close", {"sessionId": sid}, 15)
        with lock:
            updates.clear()
        rs = rpc(6, "session/resume", {"sessionId": sid, "cwd": args.cwd, "mcpServers": []}, 60)
        time.sleep(1.5)
        with lock:
            replayed = [u for u in updates if u in ("user_message_chunk", "agent_message_chunk", "agent_thought_chunk")]
        check("session/resume", bool((rs or {}).get("result")), json.dumps((rs or {}).get("error")) if not (rs or {}).get("result") else "")
        check("resume does not replay history", not replayed, f"replayed={replayed}")
        cl = rpc(7, "session/close", {"sessionId": sid}, 15)
        check("session/close", cl is not None and "error" not in cl, json.dumps((cl or {}).get("error")) if cl else "timeout")
finally:
    from agy_upgrade import stop_owned_process
    stop_owned_process(p)

failed = [c for c in checks if not c["ok"]]
result = {"ok": not failed, "info": info, "checks": checks, "generation_requests": 0}
if args.report:
    from pathlib import Path
    Path(args.report).write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
if args.json:
    print(json.dumps(result, ensure_ascii=False))
else:
    print(f"[i] agent={info.get('agentInfo')} protocolVersion={info.get('protocolVersion')}")
    for c in checks:
        print(f"[{'ok' if c['ok'] else 'FAIL'}] {c['check']}" + (f"  ({c['detail']})" if c["detail"] else ""))
    print(f"[=] {'all ' + str(len(checks)) + ' checks passed' if not failed else str(len(failed)) + ' check(s) FAILED'}")
sys.exit(0 if not failed else 1)
