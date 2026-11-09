"""Compare session/load vs session/resume on an existing agy session: replayed update count, bytes, latency.

No prompts are sent (no quota use). Do not point it at a session that a running gateway is using.
Usage: python3 scripts/probe_resume_vs_load.py <sessionId>
"""
import json, os, subprocess, sys, threading, time, collections

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
CWD = os.environ.get("AGY_DEFAULT_CWD", "/tmp")
SID = sys.argv[1]
env = dict(os.environ, HTTPS_PROXY="http://127.0.0.1:7890", HTTP_PROXY="http://127.0.0.1:7890",
           NO_PROXY="127.0.0.1,localhost,::1", no_proxy="127.0.0.1,localhost,::1")
p = subprocess.Popen([f"{ROOT}/bin/agy-acp-900k", "acp", "--gemini-dir", f"{ROOT}/runtime/pure-api-home",
                      "--app-data-dir", "antigravity-cli", "--process-per-session", "--disable-telemetry"],
                     stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env=env, text=True, cwd=CWD)
res, ev = {}, {}
kinds = collections.Counter()
nbytes = [0]
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
            k = ((m.get("params") or {}).get("update") or {}).get("sessionUpdate", "?")
            with lock:
                kinds[k] += 1
                nbytes[0] += len(line)


threading.Thread(target=reader, daemon=True).start()


def rpc(i, method, params, timeout=120):
    ev.setdefault(i, threading.Event())
    p.stdin.write(json.dumps({"jsonrpc": "2.0", "id": i, "method": method, "params": params}) + "\n")
    p.stdin.flush()
    ev[i].wait(timeout)
    return res.get(i)


def measure(i, method):
    with lock:
        kinds.clear()
        nbytes[0] = 0
    t0 = time.time()
    r = rpc(i, method, {"sessionId": SID, "cwd": CWD, "mcpServers": []})
    time.sleep(1.5)
    with lock:
        print(f"{method}: {time.time()-t0:.2f}s ok={'result' in (r or {})} updates={dict(kinds)} bytes={nbytes[0]}")
    rpc(i + 100, "session/close", {"sessionId": SID}, 15)


rpc(1, "initialize", {"protocolVersion": 1, "clientCapabilities": {"fs": {"readTextFile": False, "writeTextFile": False}, "terminal": False}})
measure(2, "session/load")
measure(3, "session/resume")
p.terminate()
