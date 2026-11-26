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
  * set_config_option acknowledges with configOptions[*].currentValue == requested value (P1),
    unknown values fail with -32602 (P2)
  * MCP handshake: with the gateway's HTTP MCP answers (POST only, GET 405, listChanged=false,
    server/discover -> -32601) agy initializes the server and pulls tools/list at session/new (P5)
  * every agy status text the gateway filters is still present verbatim (O10)

Usage: agy_acp_smoke.py <agy-binary> [--gemini-dir DIR] [--json]
Exit 0 = all checks passed, 1 = contract changed, 2 = could not run.
"""
import argparse, json, os, socket, subprocess, sys, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
GATEWAY_MODELS = ["gemini-4-argon", "gemini-4-carbon", "gemini-4-neon", "gemini-4-hydrogen", "gemini-3.8-flash", "claude-opus-5-5", "claude-sonnet-5-5"]
TIERS = ["low", "medium", "high"]

ap = argparse.ArgumentParser()
ap.add_argument("binary")
ap.add_argument("--gemini-dir", required=True, help="isolated candidate state directory")
ap.add_argument("--cwd", default=os.environ.get("AGY_DEFAULT_CWD", "/tmp"))
ap.add_argument("--json", action="store_true")
ap.add_argument("--report", help="write machine-readable contract evidence")
args = ap.parse_args()


def _resolve_proxy():
    if "AGY_UPSTREAM_PROXY" in os.environ:
        return os.environ["AGY_UPSTREAM_PROXY"].strip()
    try:
        with socket.create_connection(("127.0.0.1", 7890), timeout=0.15):
            return "http://127.0.0.1:7890"
    except OSError:
        return ""


proxy = _resolve_proxy()
env = dict(os.environ, NO_PROXY="127.0.0.1,localhost,::1", no_proxy="127.0.0.1,localhost,::1")
if proxy:
    env["HTTPS_PROXY"] = proxy
    env["HTTP_PROXY"] = proxy
else:
    env.pop("HTTPS_PROXY", None)
    env.pop("HTTP_PROXY", None)
try:
    p = subprocess.Popen([args.binary, "acp", "--gemini-dir", args.gemini_dir, "--app-data-dir", "antigravity-cli",
                          "--process-per-session", "--disable-telemetry"],
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                         env=env, text=True, start_new_session=True, cwd=args.cwd if os.path.isdir(args.cwd) else ROOT)
except OSError as exc:
    print(f"error: cannot start {args.binary}: {exc}")
    sys.exit(2)

MCP_TOKEN = "smoke-" + os.urandom(8).hex()
mcp_calls = []


class GatewayLikeMcp(BaseHTTPRequestHandler):
    """Answers exactly like go-gateway HandleMcpHTTP (the shape probe P5 proved agy accepts)."""
    def log_message(self, *a):
        pass

    def do_GET(self):
        mcp_calls.append("GET")
        self.send_response(405)
        self.send_header("Allow", "POST, DELETE")
        self.end_headers()

    def do_DELETE(self):
        self.send_response(200)
        self.end_headers()

    def do_POST(self):
        if self.headers.get("Authorization") != "Bearer " + MCP_TOKEN:
            self.send_response(401)
            self.end_headers()
            return
        n = int(self.headers.get("Content-Length") or 0)
        try:
            msg = json.loads(self.rfile.read(n) or b"{}")
        except Exception:
            msg = {}
        method = msg.get("method", "")
        mcp_calls.append(method)
        if msg.get("id") is None:
            self.send_response(202)
            self.end_headers()
            return
        if method == "initialize":
            pv = (msg.get("params") or {}).get("protocolVersion") or "2025-06-18"
            out = {"result": {"protocolVersion": pv, "capabilities": {"tools": {"listChanged": False}},
                              "serverInfo": {"name": "ws-bridge", "version": "2.0.0"}}}
        elif method == "tools/list":
            out = {"result": {"tools": [{"name": "ws_Probe_000000000000", "description": "contract probe tool",
                                         "inputSchema": {"type": "object", "properties": {}}}]}}
        elif method in ("ping",):
            out = {"result": {}}
        elif method in ("resources/list", "resources/templates/list", "prompts/list"):
            key = {"resources/list": "resources", "resources/templates/list": "resourceTemplates", "prompts/list": "prompts"}[method]
            out = {"result": {key: []}}
        else:
            out = {"error": {"code": -32601, "message": "Method not found"}}
        body = json.dumps({"jsonrpc": "2.0", "id": msg.get("id"), **out}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


mcp_srv = ThreadingHTTPServer(("127.0.0.1", 0), GatewayLikeMcp)
threading.Thread(target=mcp_srv.serve_forever, daemon=True).start()
MCP_URL = f"http://127.0.0.1:{mcp_srv.server_address[1]}/mcp/{MCP_TOKEN}"

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

def ack_value(resp, oid):
    for o in (((resp or {}).get("result") or {}).get("configOptions") or []):
        if o.get("id") == oid:
            return o.get("currentValue")
    return None

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
        check("model ack currentValue (P1)", ack_value(sm, "model") == "gemini-3.8-flash", f"got={ack_value(sm, 'model')!r}")
        st = rpc(4, "session/set_config_option", {"sessionId": sid, "configId": "thought_level", "value": "low"}, 30)
        check("set_config_option thought_level", bool((st or {}).get("result")), json.dumps((st or {}).get("error")) if not (st or {}).get("result") else "")
        check("thought_level ack currentValue (P1)", ack_value(st, "thought_level") == "low", f"got={ack_value(st, 'thought_level')!r}")
        bad = rpc(41, "session/set_config_option", {"sessionId": sid, "configId": "model", "value": "no-such-model"}, 30)
        check("unknown model -> -32602 (P2)", ((bad or {}).get("error") or {}).get("code") == -32602, json.dumps((bad or {}).get("error")))
        rpc(5, "session/close", {"sessionId": sid}, 15)
        with lock:
            updates.clear()
        rs = rpc(6, "session/resume", {"sessionId": sid, "cwd": args.cwd, "mcpServers": []}, 60)
        time.sleep(1.5)
        with lock:
            replayed = [u for u in updates if u in ("user_message_chunk", "agent_message_chunk", "agent_thought_chunk")]
        check("session/resume", bool((rs or {}).get("result")), json.dumps((rs or {}).get("error")) if not (rs or {}).get("result") else "")
        check("resume does not replay history", not replayed, f"replayed={replayed}")
        info["resume_model"] = ack_value(rs, "model")
        info["resume_thought_level"] = ack_value(rs, "thought_level")
        cl = rpc(7, "session/close", {"sessionId": sid}, 15)
        check("session/close", cl is not None and "error" not in cl, json.dumps((cl or {}).get("error")) if cl else "timeout")
    mcp_calls.clear()
    mnew = rpc(8, "session/new", {"cwd": args.cwd, "mcpServers": [{"name": "ws-bridge", "type": "http", "url": MCP_URL,
                                   "headers": [{"name": "Authorization", "value": "Bearer " + MCP_TOKEN}]}]}, 90)
    msid = ((mnew or {}).get("result") or {}).get("sessionId")
    deadline = time.time() + 10
    while time.time() < deadline and "tools/list" not in mcp_calls:
        time.sleep(0.2)
    info["mcp_calls"] = list(mcp_calls)
    check("session/new with gateway-style MCP", bool(msid), json.dumps((mnew or {}).get("error")) if not msid else "")
    check("MCP initialize at session/new (P5)", "initialize" in mcp_calls, f"calls={mcp_calls}")
    check("MCP tools/list at session/new (P5)", "tools/list" in mcp_calls, f"calls={mcp_calls}")
    if msid:
        rpc(9, "session/close", {"sessionId": msid}, 15)
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import agy_notice_templates as nt
    data = open(args.binary, "rb").read()
    missing = [t for t in nt.REQUIRED if t.encode() not in data]
    check("agy status texts present (O10)", not missing, f"missing={missing}" if missing else "")
    baseline = json.loads(nt.BASELINE.read_text()).get("templates", []) if nt.BASELINE.exists() else []
    info["new_status_templates"] = [t for t in nt.extract(data) if t not in baseline]
finally:
    from agy_upgrade import stop_owned_process
    stop_owned_process(p)
    mcp_srv.shutdown()

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
