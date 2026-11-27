#!/usr/bin/env python3
"""Probe agy ACP: list model / thought_level config options exposed by session/new (no MCP, no prompt)."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
import agy_upgrade as U  # noqa: E402

ROOT = U.ROOT


def build_probe_env(home):
    env = dict(os.environ, HOME=str(home), DBUS_SESSION_BUS_ADDRESS="unix:path=/dev/null",
               NO_PROXY="127.0.0.1,localhost,::1", no_proxy="127.0.0.1,localhost,::1")
    proxy = os.environ.get("AGY_UPSTREAM_PROXY", "http://127.0.0.1:7890")
    if proxy:
        env["HTTPS_PROXY"] = proxy
        env["HTTP_PROXY"] = proxy
    else:
        env.pop("HTTPS_PROXY", None)
        env.pop("HTTP_PROXY", None)
    return env


def prepare_isolated_home(prefix):
    stage = U.checked_path(U.STATE / "probes" / f"{prefix}-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}-{os.getpid()}")
    home = stage / "home"
    home.mkdir(parents=True, exist_ok=True, mode=0o700)
    U.prepare_auth_link(home)
    return stage, home


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--binary", type=Path, default=ROOT / "bin/agy-acp-900k")
    ap.add_argument("--cwd", default=os.environ.get("AGY_DEFAULT_CWD", "/tmp"))
    args = ap.parse_args(argv)

    with U.upgrade_lock():
        _stage, home = prepare_isolated_home("models")
        env = build_probe_env(home)
        p = subprocess.Popen(
            [str(args.binary), "acp", "--gemini-dir", str(home), "--app-data-dir", "antigravity-cli",
             "--process-per-session", "--disable-telemetry"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            env=env, text=True, cwd=args.cwd, start_new_session=True,
        )
        res, ev = {}, {}

        def reader():
            for line in p.stdout:
                try:
                    m = json.loads(line)
                except Exception:
                    continue
                if "id" in m and ("result" in m or "error" in m) and "method" not in m:
                    res[m["id"]] = m
                    ev.setdefault(m["id"], threading.Event()).set()

        threading.Thread(target=reader, daemon=True).start()

        def rpc(i, method, params, timeout=90):
            ev.setdefault(i, threading.Event())
            p.stdin.write(json.dumps({"jsonrpc": "2.0", "id": i, "method": method, "params": params}) + "\n")
            p.stdin.flush()
            ev[i].wait(timeout)
            return res.get(i)

        try:
            rpc(1, "initialize", {"protocolVersion": 1, "clientCapabilities": {"fs": {"readTextFile": False, "writeTextFile": False}, "terminal": False}})
            r = rpc(2, "session/new", {"cwd": args.cwd, "mcpServers": []})
            out = r.get("result", {}) if r else {}
            sid = out.get("sessionId")
            print("availableModels:", json.dumps(out.get("models", {}), ensure_ascii=False, indent=1))
            for opt in out.get("configOptions", []):
                print("CONFIG", opt.get("id"), "current=", opt.get("currentValue"))
                for o in opt.get("options", []):
                    print("   ", o.get("value"), "|", o.get("name"), "|", (o.get("description") or "")[:80])
            if sid:
                rpc(3, "session/close", {"sessionId": sid}, 10)
        finally:
            U.stop_owned_process(p)
    return 0


if __name__ == "__main__":
    os.umask(0o077)
    raise SystemExit(main())
