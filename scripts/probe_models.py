"""Probe agy ACP: list model / thought_level config options exposed by session/new (no MCP, no prompt)."""
import json, os, subprocess, sys, threading

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
CWD = os.environ.get("AGY_DEFAULT_CWD", "/tmp")
env = dict(os.environ, HTTPS_PROXY="http://127.0.0.1:7890", HTTP_PROXY="http://127.0.0.1:7890", NO_PROXY="127.0.0.1,localhost")
p = subprocess.Popen([f"{ROOT}/bin/agy-acp-900k", "acp", "--gemini-dir", f"{ROOT}/runtime/pure-api-home",
                      "--app-data-dir", "antigravity-cli", "--disable-telemetry"],
                     stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env=env, text=True,
                     cwd=CWD)
res = {}
ev = {}


def reader():
    for line in p.stdout:
        try:
            m = json.loads(line)
        except Exception:
            continue
        if "id" in m and ("result" in m or "error" in m):
            res[m["id"]] = m
            ev.setdefault(m["id"], threading.Event()).set()


threading.Thread(target=reader, daemon=True).start()


def rpc(i, method, params, timeout=90):
    ev.setdefault(i, threading.Event())
    p.stdin.write(json.dumps({"jsonrpc": "2.0", "id": i, "method": method, "params": params}) + "\n")
    p.stdin.flush()
    ev[i].wait(timeout)
    return res.get(i)


rpc(1, "initialize", {"protocolVersion": 1, "clientCapabilities": {"fs": {"readTextFile": False, "writeTextFile": False}, "terminal": False}})
r = rpc(2, "session/new", {"cwd": CWD, "mcpServers": []})
out = r.get("result", {}) if r else {}
sid = out.get("sessionId")
print("availableModels:", json.dumps(out.get("models", {}), ensure_ascii=False, indent=1))
for opt in out.get("configOptions", []):
    print("CONFIG", opt.get("id"), "current=", opt.get("currentValue"))
    for o in opt.get("options", []):
        print("   ", o.get("value"), "|", o.get("name"), "|", (o.get("description") or "")[:80])
if sid:
    rpc(3, "session/close", {"sessionId": sid}, 10)
p.terminate()
