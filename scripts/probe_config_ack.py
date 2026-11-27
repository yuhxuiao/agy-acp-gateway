"""Zero-generation probe of real agy ACP: session/set_config_option ack shapes and MCP tools/list_changed handling.

Never sends session/prompt unless --prompt is given (then one tiny gemini-3.8-flash/low prompt per phase).
Uses an isolated --gemini-dir under runtime/upgrades/probes/ holding only the agent plugin and a symlink to the
existing login; never reads or prints the token. Does not touch the 18890/18892 instances. Holds the shared upgrade lock.
Usage: python3 scripts/probe_config_ack.py [--prompt]
"""
import argparse, json, os, shutil, signal, subprocess, sys, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import agy_upgrade as U  # noqa: E402

MODELS = ["gemini-3.8-flash", "gemini-4-argon", "gemini-4-carbon", "gemini-4-neon", "gemini-4-hydrogen", "claude-sonnet-5-5", "claude-opus-5-5"]
TOOLS_V1 = [{"name": "Read", "description": "Read a file. v1", "inputSchema": {"type": "object", "properties": {"path": {"type": "string"}}}}]
TOOLS_V2 = [{"name": "Write", "description": "Write a file. v2", "inputSchema": {"type": "object", "properties": {"path": {"type": "string"}, "text": {"type": "string"}}}}]


class Mcp:
    def __init__(self):
        self.log, self.tools, self.streams, self.lock = [], TOOLS_V1, [], threading.Lock()

    def note(self, **kw):
        with self.lock:
            self.log.append(dict(t=round(time.time() - T0, 3), **kw))

    def push(self, obj):
        data = ("event: message\ndata: " + json.dumps(obj) + "\n\n").encode()
        sent = 0
        for w in list(self.streams):
            try:
                w.write(data); w.flush(); sent += 1
            except Exception:
                self.streams.remove(w)
        return sent


MCP = Mcp()
MIMIC = False
T0 = time.time()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def hdrs(self):
        return {k: self.headers.get(k) for k in ("Accept", "Mcp-Session-Id", "Mcp-Protocol-Version") if self.headers.get(k)}

    def do_GET(self):
        MCP.note(verb="GET", headers=self.hdrs())
        if MIMIC:
            self.send_response(405); self.send_header("Allow", "POST, DELETE"); self.send_header("Content-Length", "0"); self.end_headers(); return
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Mcp-Session-Id", "probe-mcp")
        self.end_headers()
        self.wfile.flush()
        MCP.streams.append(self.wfile)
        while self.wfile in MCP.streams:
            time.sleep(0.5)

    def do_DELETE(self):
        MCP.note(verb="DELETE")
        self.send_response(200); self.send_header("Content-Length", "0"); self.end_headers()

    def reply(self, obj, code=200):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        if not MIMIC:
            self.send_header("Mcp-Session-Id", "probe-mcp")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        msg = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)) or b"{}")
        m = msg.get("method")
        MCP.note(verb="POST", method=m, headers=self.hdrs())
        if "id" not in msg:
            self.send_response(202); self.send_header("Content-Length", "0"); self.end_headers(); return
        if m == "initialize":
            res = {"protocolVersion": (msg.get("params") or {}).get("protocolVersion", "2025-06-18"),
                   "capabilities": {"tools": {"listChanged": not MIMIC}}, "serverInfo": {"name": "dsh-bridge", "version": "2.0.0"},
                   "instructions": "Host harness tools bridge."}
        elif m == "tools/list":
            res = {"tools": [dict(t, _meta={"antigravity/alwaysLoad": True}) for t in MCP.tools]}
            MCP.log[-1]["served"] = [t["name"] for t in MCP.tools]
        elif m == "tools/call":
            res = {"content": [{"type": "text", "text": "probe: tool not executed"}], "isError": True}
        elif m == "server/discover":
            self.reply({"jsonrpc": "2.0", "id": msg["id"], "error": {"code": -32601, "message": "Method not found"}}); return
        elif m and m.endswith("/list"):
            res = {m.split("/")[0]: []}
        else:
            res = {}
        self.reply({"jsonrpc": "2.0", "id": msg["id"], "result": res})


class Acp:
    def __init__(self, home, env):
        self.p = subprocess.Popen([str(ROOT / "bin/agy-acp-900k"), "acp", "--gemini-dir", str(home), "--app-data-dir", "antigravity-cli",
                                   "--agent", "dsh-bridge", "--process-per-session", "--disable-telemetry"],
                                  stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env=env, text=True,
                                  cwd="/tmp", start_new_session=True)
        self.n, self.res, self.ev, self.updates, self.reverse, self.text = 0, {}, {}, [], [], []
        threading.Thread(target=self.reader, daemon=True).start()

    def reader(self):
        for line in self.p.stdout:
            try:
                m = json.loads(line)
            except Exception:
                continue
            if "id" in m and "method" not in m:
                self.res[m["id"]] = m; self.ev.setdefault(m["id"], threading.Event()).set(); continue
            meth = m.get("method")
            if meth == "session/update":
                u = (m.get("params") or {}).get("update") or {}
                k = u.get("sessionUpdate")
                if k == "agent_message_chunk":
                    self.text.append(((u.get("content") or {}).get("text")) or "")
                keep = u if k not in ("agent_message_chunk", "agent_thought_chunk") else {"sessionUpdate": k}
                self.updates.append({"t": round(time.time() - T0, 3), "update": json.dumps(keep, ensure_ascii=False)[:1500]})
            elif meth:
                self.reverse.append({"t": round(time.time() - T0, 3), "method": meth})
                if "id" in m:
                    self.send({"jsonrpc": "2.0", "id": m["id"], "error": {"code": -32601, "message": "not supported by probe"}})

    def send(self, obj):
        self.p.stdin.write(json.dumps(obj) + "\n"); self.p.stdin.flush()

    def rpc(self, method, params, timeout=60):
        self.text = []
        self.n += 1
        i = self.n
        ev = self.ev.setdefault(i, threading.Event())
        t = time.time()
        self.send({"jsonrpc": "2.0", "id": i, "method": method, "params": params})
        ok = ev.wait(timeout)
        r = self.res.get(i, {}) if ok else {"timeout": True}
        return {"method": method, "params": {k: v for k, v in params.items() if k not in ("mcpServers", "_meta")},
                "ms": int((time.time() - t) * 1000), "text": "".join(self.text)[:400], "result": r.get("result"), "error": r.get("error"), "timeout": r.get("timeout", False)}


def summarize_cfg(result):
    """Keep config option ids/currentValue/option values; drop long descriptions."""
    if not isinstance(result, dict):
        return result
    out = {k: v for k, v in result.items() if k not in ("configOptions", "models", "modes")}
    for key in ("configOptions",):
        if key in result:
            out[key] = [{"id": o.get("id"), "type": o.get("type"), "currentValue": o.get("currentValue"),
                         "values": [x.get("value") for x in (o.get("options") or []) if isinstance(x, dict)][:20]}
                        for o in result[key] or []]
    for key in ("models", "modes"):
        if key in result:
            out[key] = json.dumps(result[key], ensure_ascii=False)[:800]
    return out


def main():
    # Shared lock with agy_upgrade.py / probe_cache.py / quota_calibrate.py: never overlap real-upstream work.
    with U.upgrade_lock():
        _main()


def _main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mimic", action="store_true", help="answer MCP exactly like the gateway (GET 405, listChanged false, no session id)")
    ap.add_argument("--prompt", action="store_true", help="allow one tiny Flash/low prompt per list_changed phase")
    args = ap.parse_args()
    global MIMIC
    MIMIC = args.mimic
    stage = U.checked_path(U.STATE / "probes" / ("config-ack-" + time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())))
    home = stage / "home"
    (home / "config").mkdir(parents=True, mode=0o700)
    shutil.copytree(ROOT / "runtime/pure-api-home/config/plugins", home / "config/plugins")
    U.prepare_auth_link(home)

    srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{srv.server_address[1]}/mcp"
    proxy = os.environ.get("AGY_UPSTREAM_PROXY", "http://127.0.0.1:7890")
    env = dict(os.environ, HOME=str(home), DBUS_SESSION_BUS_ADDRESS="unix:path=/dev/null", HTTPS_PROXY=proxy, HTTP_PROXY=proxy,
               NO_PROXY="127.0.0.1,localhost,::1", no_proxy="127.0.0.1,localhost,::1")
    acp = Acp(home, env)
    out = {"stage": str(stage), "steps": []}
    step = lambda r: (out["steps"].append(r), print(json.dumps(r, ensure_ascii=False)[:600], flush=True))[1]
    try:
        init = acp.rpc("initialize", {"protocolVersion": 1, "clientCapabilities": {"fs": {"readTextFile": False, "writeTextFile": False}, "terminal": False},
                                      "clientInfo": {"name": "agy-acp-gateway", "version": "probe"}}, 90)
        out["initialize"] = init
        new = acp.rpc("session/new", {"cwd": "/tmp", "mcpServers": [{"name": "dsh-bridge", "type": "http", "url": url, "headers": [{"name": "Authorization", "value": "Bearer probe-local"}]}],
                                      "_meta": {"antigravity": {"systemPrompt": {"append": "PROBE_RULE_A"}}}}, 120)
        new["result"] = summarize_cfg(new["result"])
        step(new)
        sid = (new["result"] or {}).get("sessionId")
        if not sid:
            raise SystemExit("session/new failed")
        time.sleep(6)
        out["mcp_after_new"] = list(MCP.log)

        for m in MODELS + [MODELS[2], "no-such-model"]:
            r = acp.rpc("session/set_config_option", {"sessionId": sid, "configId": "model", "value": m}, 45)
            r["result"] = summarize_cfg(r["result"]); step(r)
        acp.rpc("session/set_config_option", {"sessionId": sid, "configId": "model", "value": "gemini-3.8-flash"}, 45)
        for v in ("low", "high", "no-such-level"):
            r = acp.rpc("session/set_config_option", {"sessionId": sid, "configId": "thought_level", "value": v}, 45)
            r["result"] = summarize_cfg(r["result"]); step(r)
        r = acp.rpc("session/set_config_option", {"sessionId": sid, "configId": "no_such_option", "value": "x"}, 45)
        step(r)
        acp.rpc("session/set_config_option", {"sessionId": sid, "configId": "thought_level", "value": "low"}, 45)

        # list_changed: swap catalog, notify over any GET stream, see whether agy re-lists
        n_before = sum(1 for e in MCP.log if e.get("method") == "tools/list")
        if args.prompt:
            step(acp.rpc("session/prompt", {"sessionId": sid, "prompt": [{"type": "text", "text": "Reply with exactly: OK. List the names of tools you have, comma separated."}]}, 120))
        MCP.tools = TOOLS_V2
        pushed = MCP.push({"jsonrpc": "2.0", "method": "notifications/tools/list_changed"})
        time.sleep(8)
        out["list_changed"] = {"get_streams": pushed, "tools_list_before": n_before,
                               "tools_list_after_notify": sum(1 for e in MCP.log if e.get("method") == "tools/list")}
        if args.prompt:
            step(acp.rpc("session/prompt", {"sessionId": sid, "prompt": [{"type": "text", "text": "Reply with exactly the names of tools you have now, comma separated."}]}, 120))
            out["list_changed"]["tools_list_after_prompt"] = sum(1 for e in MCP.log if e.get("method") == "tools/list")

        step(acp.rpc("session/close", {"sessionId": sid}, 20))
        res = acp.rpc("session/resume", {"sessionId": sid, "cwd": "/tmp", "mcpServers": []}, 120)
        res["result"] = summarize_cfg(res["result"]); step(res)
        if res.get("result") is not None:
            step(acp.rpc("session/close", {"sessionId": sid}, 20))
    finally:
        out["mcp_log"] = MCP.log
        out["updates"] = acp.updates
        out["reverse_requests"] = acp.reverse
        try:
            os.killpg(acp.p.pid, signal.SIGTERM)
            acp.p.wait(10)
        except Exception:
            pass
        srv.shutdown()
        (stage / "results.json").write_text(json.dumps(out, ensure_ascii=False, indent=1))
        print("RESULTS", stage / "results.json")


if __name__ == "__main__":
    main()
