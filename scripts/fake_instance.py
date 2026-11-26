#!/usr/bin/env python3
"""Build and run an isolated gateway instance backed by the fake ACP (never a real model).

  python3 scripts/fake_instance.py build --dir /tmp/agy-b0-fake
  python3 scripts/fake_instance.py start --dir /tmp/agy-b0-fake --port 18894 [--acp scripts/review_fault_acp.py --env REVIEW_FAULT=late_notice]
  python3 scripts/fake_instance.py stop  --dir /tmp/agy-b0-fake --port 18894

start prints the receipt path (<dir>/fake-instance-<port>.json) that the e2e_*.py fake mode requires.
Ports 18888-18891 are refused (since dash_port = port + 1, 18888 would bind 18889).
Instance state lives under <dir>/instance-fake-<port>/.
"""
import argparse
import contextlib
import fcntl
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parent))
import agy_upgrade as U  # noqa: E402
import e2e_common as E  # noqa: E402

FAKEACP = "fakeacp"
FORBIDDEN_PORTS = {18888, 18889, 18890, 18891}
_OWNED_PROCS = {}


def _validate_port(port):
    if not isinstance(port, int) or port in FORBIDDEN_PORTS or (port + 1) in FORBIDDEN_PORTS or not (1024 <= port <= 65534):
        raise ValueError(f"candidate ports ({port}/{port + 1}) must differ from production 18889-18891 and fit 1024..65535")
    return port


def build(stage):
    stage = U.checked_path(stage)
    stage.mkdir(parents=True, exist_ok=True, mode=0o700)
    env = U.clean_env()
    env["CGO_ENABLED"] = "0"
    U.run(["go", "build", "-trimpath", "-o", stage / U.FILES["gateway"], "."], cwd=U.ROOT / "go-gateway", env=env, log=stage / "build.log", timeout=300)
    U.run(["go", "build", "-o", stage / FAKEACP, "./testdata/fakeacp"], cwd=U.ROOT / "go-gateway", env=env, log=stage / "build.log", timeout=300)
    return stage


def _env(stage, port, acp, extra):
    _validate_port(port)
    mode = f"fake-{port}"
    env = U.instance_env(stage, mode, acp, port)
    conv_dir = Path(env["AGY_PURE_API_HOME"]) / "antigravity-cli" / "conversations"
    env["AGY_MAX_LIVE_WORKERS"] = "8"
    env["REVIEW_FAKEACP_BIN"] = str(stage / FAKEACP)
    env["AGY_CONV_DIR"] = str(conv_dir)
    env["FAKE_ACP_WRITE_DB"] = "1"
    env["FAKE_ACP_EMIT_USAGE_UPDATE"] = "1"
    extra_clean = dict(extra or {})
    if extra_clean.get("FAKE_ACP_CONV_DIR"):
        checked_conv = U.checked_path(extra_clean["FAKE_ACP_CONV_DIR"])
        extra_clean["FAKE_ACP_CONV_DIR"] = str(checked_conv)
        env["AGY_CONV_DIR"] = str(checked_conv)
    env.update(extra_clean)
    return env


def receipt_path(stage, port):
    return U.checked_path(stage) / f"fake-instance-{port}.json"


@contextlib.contextmanager
def _control_lock(run_dir):
    run_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (run_dir / "control.lock").open("a") as lock_f:
        try:
            fcntl.flock(lock_f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError(f"instance operation already running in {run_dir}") from None
        yield


def _port_listening(port):
    try:
        with socket.create_connection(("127.0.0.1", int(port)), timeout=0.2):
            return True
    except OSError:
        return False


def _http_ready(port):
    req = urllib.request.Request(f"http://127.0.0.1:{port}/healthz")
    try:
        with urllib.request.urlopen(req, timeout=1.0) as resp:
            return resp.status == 200
    except (OSError, urllib.error.URLError, ValueError):
        return False


def _all_process_identities():
    if Path("/proc").is_dir():
        table = {}
        for entry in Path("/proc").iterdir():
            if entry.name.isdigit():
                pid = int(entry.name)
                ident = U.process_identity(pid)
                if ident:
                    table[pid] = ident
        return table
    return U._ps_process_table()


def _descendant_identities(root_pid):
    table = _all_process_identities()
    owned = {int(root_pid)}
    for _ in range(128):
        more = {pid for pid, (ppid, _) in table.items() if ppid in owned}
        if more <= owned:
            break
        owned |= more
    return {pid: table[pid][1] for pid in (owned - {int(root_pid)}) if pid in table}


def _verify_owned_gateway(pid, ident_meta, gw_bin):
    cur = U.process_identity(pid)
    if cur is None:
        return None
    expected_start = (ident_meta or {}).get("start") or ""
    if expected_start:
        if cur[1] != expected_start:
            raise RuntimeError(f"PID {pid} was reused; refusing signal")
        return cur[1]
    try:
        actual_exe = E.pid_executable(pid)
    except OSError as exc:
        raise RuntimeError(f"cannot verify executable for legacy PID {pid}: {exc}") from None
    if actual_exe != gw_bin.resolve():
        raise RuntimeError(f"unverified PID {pid} ({actual_exe}); refusing signal")
    return cur[1]


def _signal_Matching(pid, expected_start, sig, also_pgid=False):
    cur = U.process_identity(pid)
    if not cur or cur[1] != expected_start:
        return False
    if also_pgid:
        try:
            if os.getpgid(pid) == pid:
                os.killpg(pid, sig)
                return True
        except (ProcessLookupError, PermissionError, OSError):
            pass
    try:
        os.kill(pid, sig)
        return True
    except ProcessLookupError:
        return False


def _reap_in_process_child(proc, pid):
    if proc is not None and proc.pid == pid:
        try:
            proc.wait(timeout=0.2)
        except subprocess.TimeoutExpired:
            pass


def _stop_locked(run_dir, stage, port):
    proc = _OWNED_PROCS.pop((str(stage), int(port)), None)
    pid_file = run_dir / "gateway.pid"
    ident_file = run_dir / "gateway.ident.json"
    gw_bin = stage / U.FILES["gateway"]
    if pid_file.is_file():
        raw_pid = pid_file.read_text().strip()
        ident_meta = {}
        if ident_file.is_file():
            try:
                ident_meta = json.loads(ident_file.read_text())
            except ValueError:
                ident_meta = {}
        if raw_pid.isdigit():
            pid = int(raw_pid)
            verified_start = _verify_owned_gateway(pid, ident_meta, gw_bin)
            if verified_start is not None:
                descendants = _descendant_identities(pid)
                _signal_Matching(pid, verified_start, signal.SIGTERM)
                deadline = time.monotonic() + 10.0
                while time.monotonic() < deadline:
                    _reap_in_process_child(proc, pid)
                    if U.process_identity(pid) is None:
                        break
                    time.sleep(0.05)
                if U.process_identity(pid) is not None:
                    _signal_Matching(pid, verified_start, signal.SIGKILL)
                    kill_deadline = time.monotonic() + 2.0
                    while time.monotonic() < kill_deadline and U.process_identity(pid) is not None:
                        _reap_in_process_child(proc, pid)
                        time.sleep(0.05)
                _reap_in_process_child(proc, pid)

                for cpid, cstart in descendants.items():
                    _signal_Matching(cpid, cstart, signal.SIGTERM, also_pgid=True)
                child_deadline = time.monotonic() + 2.0
                while time.monotonic() < child_deadline:
                    if not any((cur := U.process_identity(cpid)) and cur[1] == cstart for cpid, cstart in descendants.items()):
                        break
                    time.sleep(0.05)
                for cpid, cstart in descendants.items():
                    _signal_Matching(cpid, cstart, signal.SIGKILL, also_pgid=True)
        os.replace(pid_file, run_dir / "gateway.last.pid")
    elif proc is not None:
        _reap_in_process_child(proc, proc.pid)
    ident_file.unlink(missing_ok=True)
    port_deadline = time.monotonic() + 3.0
    while time.monotonic() < port_deadline:
        if not _port_listening(port) and not _port_listening(port + 1):
            break
        time.sleep(0.05)
    receipt_path(stage, port).unlink(missing_ok=True)


def start(stage, port, acp=None, extra=None):
    _validate_port(port)
    stage = U.checked_path(stage)
    acp = U.checked_path(acp or stage / FAKEACP)
    if not (stage / U.FILES["gateway"]).is_file() or not (stage / FAKEACP).is_file():
        build(stage)
    env = _env(stage, port, acp, extra)
    run_dir = Path(env["AGY_RUN_DIR"])
    log_dir = Path(env["AGY_LOG_DIR"])
    gw_bin = stage / U.FILES["gateway"]

    with _control_lock(run_dir):
        if _port_listening(port) or _port_listening(port + 1):
            raise RuntimeError(f"instance port already occupied ({port}/{port + 1})")
        log_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        Path(env["AGY_DEFAULT_CWD"]).mkdir(parents=True, exist_ok=True, mode=0o700)
        Path(env["AGY_CONV_DIR"]).mkdir(parents=True, exist_ok=True, mode=0o700)
        out_path = log_dir / "go-gateway.out"
        with out_path.open("ab") as out_f:
            os.chmod(out_path, 0o600)
            proc = subprocess.Popen(
                [str(gw_bin)],
                cwd=U.ROOT,
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=out_f,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                close_fds=True,
            )
        ident = U.process_identity(proc.pid)
        _OWNED_PROCS[(str(stage), int(port))] = proc
        (run_dir / "gateway.pid").write_text(f"{proc.pid}\n")
        U.write_json(run_dir / "gateway.ident.json", {
            "pid": proc.pid,
            "start": ident[1] if ident else "",
            "gateway_bin": str(gw_bin),
            "gateway_sha256": U.sha(gw_bin),
        })
        (run_dir / "mode").write_text("go\n")

        try:
            deadline = time.monotonic() + 15.0
            ready = False
            while time.monotonic() < deadline:
                if proc.poll() is not None:
                    raise RuntimeError(f"gateway exited early with code {proc.returncode}; see {out_path}")
                if _http_ready(port) and _http_ready(port + 1):
                    ready = True
                    break
                time.sleep(0.1)
            if not ready:
                raise RuntimeError(f"gateway readiness timed out on {port}/{port + 1}; see {out_path}")
            receipt = receipt_path(stage, port)
            U.write_json(receipt, {
                "mode": "fake",
                "pid": proc.pid,
                "port": port,
                "dash_port": port + 1,
                "run_dir": str(run_dir),
                "conv_dir": env["AGY_CONV_DIR"],
                "state_db": env["AGY_STATE_DB"],
                "gateway_sha256": U.sha(gw_bin),
                "acp_path": str(acp),
                "acp_sha256": U.sha(acp),
                "gateway_log": env["AGY_GW_LOG"],
                "extra_env": sorted((extra or {}).keys()),
            })
            return receipt
        except Exception:
            _stop_locked(run_dir, stage, port)
            raise


def stop(stage, port):
    _validate_port(port)
    stage = U.checked_path(stage)
    env = _env(stage, port, stage / FAKEACP, {})
    run_dir = Path(env["AGY_RUN_DIR"])
    with _control_lock(run_dir):
        _stop_locked(run_dir, stage, port)


@contextlib.contextmanager
def instance(stage, port, acp=None, extra=None):
    receipt = start(stage, port, acp, extra)
    try:
        yield receipt
    finally:
        stop(stage, port)


def parse_extra_env(raw_items):
    extra = {}
    for kv in raw_items or []:
        k, sep, v = kv.partition("=")
        if not sep or not (k.startswith(("REVIEW_", "FAKE_ACP_")) or k == "AGY_API_KEYS"):
            raise ValueError("only REVIEW_* / FAKE_ACP_* / AGY_API_KEYS KEY=VALUE pairs may be injected")
        extra[k] = v
    return extra


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("action", choices=["build", "start", "stop"])
    ap.add_argument("--dir", type=Path, required=True)
    ap.add_argument("--port", type=int, default=18896)
    ap.add_argument("--acp", type=Path, help="ACP executable (default: the built fakeacp)")
    ap.add_argument("--env", action="append", default=[], help="KEY=VALUE passed to the instance (REVIEW_*/FAKE_ACP_* and AGY_API_KEYS only)")
    args = ap.parse_args(argv)
    try:
        extra = parse_extra_env(args.env)
    except ValueError as exc:
        raise SystemExit(str(exc)) from None
    if args.action == "build":
        print(build(args.dir))
    elif args.action == "start":
        receipt = start(args.dir, args.port, args.acp, extra)
        proc = _OWNED_PROCS.pop((str(U.checked_path(args.dir)), int(args.port)), None)
        if proc is not None and proc.returncode is None:
            proc.returncode = 0  # intentionally daemonized across CLI invocations
        print(receipt)
    else:
        stop(args.dir, args.port)
        print("stopped", args.port)
    return 0


if __name__ == "__main__":
    os.umask(0o077)
    try:
        raise SystemExit(main())
    except (ValueError, RuntimeError, OSError) as exc:
        print("fake instance:", exc, file=sys.stderr)
        raise SystemExit(1)
