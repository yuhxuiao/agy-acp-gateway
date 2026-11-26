#!/usr/bin/env python3
"""Compare session/load vs session/resume on an existing agy session: replayed update count, bytes, latency.

No prompts are sent (no quota use). Copies only the target session DB into an isolated probe home under the
shared upgrade lock so live runtime/pure-api-home is never opened or mutated by the probe subprocess.
Usage: python3 scripts/probe_resume_vs_load.py <sessionId> [--source-conversations DIR]
"""
import argparse
import collections
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
import agy_upgrade as U  # noqa: E402
from probe_models import build_probe_env, prepare_isolated_home  # noqa: E402

ROOT = U.ROOT
SESSION_ID_RE = re.compile(r"^[A-Za-z0-9_-]{4,80}$")


def stage_session_copy(sid, src_conv_dir, isolated_home):
    if not SESSION_ID_RE.fullmatch(sid):
        raise ValueError(f"invalid sessionId: {sid!r}")
    src_db = Path(src_conv_dir) / f"{sid}.db"
    if not src_db.is_file():
        raise FileNotFoundError(f"session DB not found: {src_db}")
    dst_conv = isolated_home / "antigravity-cli/conversations"
    dst_conv.mkdir(parents=True, exist_ok=True, mode=0o700)
    for suffix in ("", "-wal", "-shm"):
        cand = Path(str(src_db) + suffix)
        if cand.is_file():
            shutil.copy2(cand, dst_conv / cand.name)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("session_id")
    ap.add_argument("--binary", type=Path, default=ROOT / "bin/agy-acp-900k")
    ap.add_argument("--cwd", default=os.environ.get("AGY_DEFAULT_CWD", "/tmp"))
    ap.add_argument("--source-conversations", type=Path,
                    default=ROOT / "runtime/pure-api-home/antigravity-cli/conversations")
    args = ap.parse_args(argv)

    with U.upgrade_lock():
        _stage, home = prepare_isolated_home("resume-vs-load")
        stage_session_copy(args.session_id, args.source_conversations, home)
        env = build_probe_env(home)
        p = subprocess.Popen(
            [str(args.binary), "acp", "--gemini-dir", str(home), "--app-data-dir", "antigravity-cli",
             "--process-per-session", "--disable-telemetry"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            env=env, text=True, cwd=args.cwd, start_new_session=True,
        )
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
            r = rpc(i, method, {"sessionId": args.session_id, "cwd": args.cwd, "mcpServers": []})
            time.sleep(1.5)
            with lock:
                print(f"{method}: {time.time()-t0:.2f}s ok={'result' in (r or {})} updates={dict(kinds)} bytes={nbytes[0]}")
            rpc(i + 100, "session/close", {"sessionId": args.session_id}, 15)

        try:
            rpc(1, "initialize", {"protocolVersion": 1, "clientCapabilities": {"fs": {"readTextFile": False, "writeTextFile": False}, "terminal": False}})
            measure(2, "session/load")
            measure(3, "session/resume")
        finally:
            U.stop_owned_process(p)
    return 0


if __name__ == "__main__":
    os.umask(0o077)
    raise SystemExit(main())
