"""Probe agy ACP capabilities: initialize caps, session/update kinds, PromptResponse.usage, session/cancel, session/resume.

Side effects: two tiny prompts on gemini-3.8-flash (low) consume a little quota and create one small
conversation DB under runtime/pure-api-home/antigravity-cli/conversations/.
Usage: python3 scripts/probe_acp_caps.py
"""
import json, os, subprocess, threading, time, collections

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
CWD = os.environ.get("AGY_DEFAULT_CWD", "/tmp")
env = dict(os.environ, HTTPS_PROXY="http://127.0.0.1:7890", HTTP_PROXY="http://127.0.0.1:7890",
           NO_PROXY="127.0.0.1,localhost,::1", no_proxy="127.0.0.1,localhost,::1")
p = subprocess.Popen([f"{ROOT}/bin/agy-acp-900k", "acp", "--gemini-dir", f"{ROOT}/runtime/pure-api-home",
                      "--app-data-dir", "antigravity-cli", "--process-per-session", "--disable-telemetry"],
                     stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env=env, text=True, cwd=CWD)
res, ev = {}, {}
kinds = collections.Counter()
samples = {}
other_methods = collections.Counter()
lock = threading.Lock()
chunk_seen = threading.Event()


def reader():
    for line in p.stdout:
        try:
            m = json.loads(line)
        except Exception:
            continue
        if "id" in m and ("result" in m or "error" in m) and "method" not in m:
            res[m["id"]] = m
            ev.setdefault(m["id"], threading.Event()).set()
            continue
        meth = m.get("method")
        if meth == "session/update":
            u = (m.get("params") or {}).get("update") or {}
            k = u.get("sessionUpdate", "?")
            with lock:
                kinds[k] += 1
                samples.setdefault(k, [])
                if len(samples[k]) < 3:
                    samples[k].append(json.dumps(u, ensure_ascii=False)[:500])
            if k == "agent_message_chunk":
                chunk_seen.set()
        elif meth:
            with lock:
                other_methods[meth] += 1
            if "id" in m and meth == "session/request_permission":
                p.stdin.write(json.dumps({"jsonrpc": "2.0", "id": m["id"], "result": {"outcome": {"outcome": "cancelled"}}}) + "\n")
                p.stdin.flush()


threading.Thread(target=reader, daemon=True).start()


def send(obj):
    p.stdin.write(json.dumps(obj) + "\n")
    p.stdin.flush()


def rpc(i, method, params, timeout=120):
    ev.setdefault(i, threading.Event())
    send({"jsonrpc": "2.0", "id": i, "method": method, "params": params})
    ev[i].wait(timeout)
    return res.get(i)


init = rpc(1, "initialize", {"protocolVersion": 1, "clientCapabilities": {"fs": {"readTextFile": False, "writeTextFile": False}, "terminal": False}})
print("INIT agentCapabilities:", json.dumps((init or {}).get("result", {}).get("agentCapabilities"), ensure_ascii=False))
r = rpc(2, "session/new", {"cwd": CWD, "mcpServers": []})
sid = ((r or {}).get("result") or {}).get("sessionId")
print("SESSION", sid)
rpc(3, "session/set_config_option", {"sessionId": sid, "configId": "model", "value": "gemini-3.8-flash"}, 30)
rpc(4, "session/set_config_option", {"sessionId": sid, "configId": "thought_level", "value": "low"}, 30)

# Prompt A: long output, cancel after the first text chunk
ev.setdefault(5, threading.Event())
send({"jsonrpc": "2.0", "id": 5, "method": "session/prompt", "params": {"sessionId": sid, "prompt": [{"type": "text", "text": "Write the numbers from 1 to 400, one per line, no other text."}]}})
print("A first chunk:", chunk_seen.wait(90))
tc = time.time()
send({"jsonrpc": "2.0", "method": "session/cancel", "params": {"sessionId": sid}})
ev[5].wait(60)
print("A result after cancel:", json.dumps(res.get(5), ensure_ascii=False)[:400], f"cancel->result {time.time()-tc:.1f}s")

# Prompt B: tiny, capture PromptResponse.usage
print("B result:", json.dumps(rpc(6, "session/prompt", {"sessionId": sid, "prompt": [{"type": "text", "text": "Reply with exactly: OK"}]}, 120), ensure_ascii=False)[:600])
print("resume:", json.dumps(rpc(7, "session/resume", {"sessionId": sid, "cwd": CWD, "mcpServers": []}, 30), ensure_ascii=False)[:200])
print("close:", json.dumps(rpc(8, "session/close", {"sessionId": sid}, 15))[:200])
time.sleep(1)
with lock:
    print("UPDATE KINDS:", dict(kinds))
    print("OTHER METHODS:", dict(other_methods))
    for k, v in samples.items():
        for s in v[:1]:
            print("SAMPLE", k, s)
p.terminate()
