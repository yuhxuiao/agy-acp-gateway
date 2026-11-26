#!/usr/bin/env python3
"""Probe agy ACP capabilities: initialize caps, session/update kinds, PromptResponse.usage, session/cancel, session/resume.

Without --live only prints the plan (zero quota). With --live runs inside an isolated probe home under the shared upgrade lock.
Usage: python3 scripts/probe_acp_caps.py [--live]
"""
import argparse
import collections
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
import agy_upgrade as U  # noqa: E402
from probe_models import build_probe_env, prepare_isolated_home  # noqa: E402

ROOT = U.ROOT


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--binary", type=Path, default=ROOT / "bin/agy-acp-900k")
    ap.add_argument("--cwd", default=os.environ.get("AGY_DEFAULT_CWD", "/tmp"))
    ap.add_argument("--live", action="store_true", help="send two tiny gemini-3.8-flash/low prompts (uses real quota)")
    args = ap.parse_args(argv)

    if not args.live:
        print(json.dumps({
            "mode": "dry-run",
            "model": "gemini-3.8-flash",
            "tier": "low",
            "generation_requests_if_live": 2,
            "note": "pass --live to execute prompts in an isolated probe directory",
        }))
        return 0

    with U.upgrade_lock():
        _stage, home = prepare_isolated_home("acp-caps")
        env = build_probe_env(home)
        p = subprocess.Popen(
            [str(args.binary), "acp", "--gemini-dir", str(home), "--app-data-dir", "antigravity-cli",
             "--process-per-session", "--disable-telemetry"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            env=env, text=True, cwd=args.cwd, start_new_session=True,
        )
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

        try:
            init = rpc(1, "initialize", {"protocolVersion": 1, "clientCapabilities": {"fs": {"readTextFile": False, "writeTextFile": False}, "terminal": False}})
            print("INIT agentCapabilities:", json.dumps((init or {}).get("result", {}).get("agentCapabilities"), ensure_ascii=False))
            r = rpc(2, "session/new", {"cwd": args.cwd, "mcpServers": []})
            sid = ((r or {}).get("result") or {}).get("sessionId")
            print("SESSION", sid)
            if sid:
                rpc(3, "session/set_config_option", {"sessionId": sid, "configId": "model", "value": "gemini-3.8-flash"}, 30)
                rpc(4, "session/set_config_option", {"sessionId": sid, "configId": "thought_level", "value": "low"}, 30)

                ev.setdefault(5, threading.Event())
                send({"jsonrpc": "2.0", "id": 5, "method": "session/prompt",
                      "params": {"sessionId": sid, "prompt": [{"type": "text", "text": "Write the numbers from 1 to 400, one per line, no other text."}]}})
                print("A first chunk:", chunk_seen.wait(90))
                tc = time.time()
                send({"jsonrpc": "2.0", "method": "session/cancel", "params": {"sessionId": sid}})
                ev[5].wait(60)
                print("A result after cancel:", json.dumps(res.get(5), ensure_ascii=False)[:400], f"cancel->result {time.time()-tc:.1f}s")

                print("B result:", json.dumps(rpc(6, "session/prompt", {"sessionId": sid, "prompt": [{"type": "text", "text": "Reply with exactly: OK"}]}, 120), ensure_ascii=False)[:600])
                print("resume:", json.dumps(rpc(7, "session/resume", {"sessionId": sid, "cwd": args.cwd, "mcpServers": []}, 30), ensure_ascii=False)[:200])
                print("close:", json.dumps(rpc(8, "session/close", {"sessionId": sid}, 15))[:200])
            time.sleep(1)
            with lock:
                print("UPDATE KINDS:", dict(kinds))
                print("OTHER METHODS:", dict(other_methods))
                for k, v in samples.items():
                    for s in v[:1]:
                        print("SAMPLE", k, s)
        finally:
            U.stop_owned_process(p)
    return 0


if __name__ == "__main__":
    os.umask(0o077)
    raise SystemExit(main())
