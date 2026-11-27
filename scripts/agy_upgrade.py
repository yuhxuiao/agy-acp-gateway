#!/usr/bin/env python3
"""Isolated candidate validation and reversible local release. Default is fake only."""
import argparse
import contextlib
import datetime as dt
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import subprocess
import sys
import time
import uuid
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
STATE = ROOT / "runtime/upgrades"
SCHEMA = 2
FILES = {"upstream": "agy-acp-900k", "gateway": "agy-acp-gateway"}
# Must match agy_upgrade_e2e.FAKE_SCENARIOS; a fake gate without them is a plain-text smoke only.
FAKE_E2E_SCENARIOS = ("text", "tool_roundtrip", "model_switch", "protocols", "thinking_tiers")
DEFAULT_BLOCKED_AGY_VERSIONS = {
    "1.5.17": "agy#1890 (--agent 4x Gemini quota burn) & agy#1872",
    "1.5.18": "agy#1890 (--agent 4x Gemini quota burn) & agy#1872",
}


def normalize_agy_version(raw):
    s = str(raw or "").strip()
    if not s:
        return ""
    first = s.splitlines()[0].strip()
    m = re.search(r"(\d+\.\d+\.\d+)", first)
    return m.group(1) if m else first


def blocked_agy_versions():
    """Return {version: reason} for agy releases blocked from local download/build/install.

    - AGY_BLOCKED_VERSIONS unset: returns DEFAULT_BLOCKED_AGY_VERSIONS.
    - AGY_BLOCKED_VERSIONS set to "" or "none" (case-insensitive): returns {} (operator override).
    - Otherwise parses comma-separated "<ver>" or "<ver>:<reason>" entries.
    """
    raw = os.environ.get("AGY_BLOCKED_VERSIONS")
    if raw is None:
        return dict(DEFAULT_BLOCKED_AGY_VERSIONS)
    s = raw.strip()
    if not s or s.lower() == "none":
        return {}
    out = {}
    for item in s.split(","):
        part = item.strip()
        if not part:
            continue
        if ":" in part:
            ver_raw, reason = part.split(":", 1)
            ver = normalize_agy_version(ver_raw)
            reason = reason.strip()
        else:
            ver = normalize_agy_version(part)
            reason = ""
        if not ver:
            continue
        if not reason:
            reason = DEFAULT_BLOCKED_AGY_VERSIONS.get(ver, "blocked by AGY_BLOCKED_VERSIONS")
        out[ver] = reason
    return out


def blocked_agy_reason(ver, blocked=None):
    norm = normalize_agy_version(ver)
    if not norm:
        return None
    table = blocked_agy_versions() if blocked is None else blocked
    return table.get(norm)


def allow_blocked_env():
    return os.environ.get("AGY_ALLOW_BLOCKED_VERSION", "").strip() == "1"


def ensure_version_not_blocked(ver, *, allow_blocked=False, context="agy"):
    if allow_blocked or allow_blocked_env():
        return None
    norm = normalize_agy_version(ver)
    reason = blocked_agy_reason(norm)
    if reason:
        raise ValueError(
            f"{context} version {norm} is blocked: {reason} "
            "(override with --allow-blocked, AGY_ALLOW_BLOCKED_VERSION=1, or AGY_BLOCKED_VERSIONS=none)"
        )
    return None


def sha(path):
    with Path(path).open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


def checked_path(path):
    p = Path(path).expanduser().resolve()
    if p == Path("/mnt/c") or Path("/mnt/c") in p.parents:
        raise ValueError("C drive writes are prohibited")
    return p


def write_json(path, value):
    path = checked_path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    tmp = path.with_name(path.name + ".tmp-" + uuid.uuid4().hex)
    with tmp.open("x") as f:
        os.chmod(tmp, 0o600)
        json.dump(value, f, indent=2, ensure_ascii=False)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def source_digest(root=None):
    root = root or ROOT
    names = list((root / "go-gateway").rglob("*.go"))
    names += [p for p in (root / "go-gateway/webui").rglob("*") if p.is_file()]
    names += list((root / "go-gateway/testdata").rglob("*.json"))
    names += [root / "go-gateway/go.mod", root / "go-gateway/go.sum"]
    names += list((root / "scripts").glob("*.py")) + list((root / "scripts").glob("*.sh")) + list((root / "scripts").glob("*.json"))
    names += list((root / "tools").glob("*.py")) + [root / "third_party/agy-context-unlock/agy-patch.py"]
    h = hashlib.sha256()
    for p in sorted(names):
        h.update(str(p.relative_to(root)).encode()+b"\0"+sha(p).encode()+b"\n")
    return h.hexdigest()


def clean_env():
    env = {k: v for k, v in os.environ.items() if not k.startswith(("AGY_", "FAKE_"))}
    env["PATH"] = str(Path.home()/".local/go/bin") + os.pathsep + env.get("PATH", "")
    return env


def version(path):
    return subprocess.check_output([str(path), "--version"], env=clean_env(), timeout=20,
                                   stderr=subprocess.DEVNULL, text=True).strip().splitlines()[0]


def run(command, *, env=None, log=None, cwd=None, timeout=600):
    opts = dict(cwd=cwd or ROOT, env=env if env is not None else clean_env(), start_new_session=True)
    with contextlib.ExitStack() as stack:
        if log:
            f = stack.enter_context(Path(log).open("ab"))
            os.chmod(log, 0o600)
            opts.update(stdout=f, stderr=subprocess.STDOUT)
        p = subprocess.Popen([str(x) for x in command], **opts)
        try:
            p.wait(timeout)
        except (subprocess.TimeoutExpired, KeyboardInterrupt):
            stop_owned_process(p)
            raise
    if p.returncode:
        raise RuntimeError("command failed: " + Path(str(command[0])).name + ("; see " + str(log) if log else ""))


def _ps_process_table():
    try:
        out = subprocess.check_output(["ps", "-eo", "pid=,ppid=,stat=,lstart="], text=True, stderr=subprocess.DEVNULL, timeout=5)
    except (OSError, subprocess.SubprocessError):
        return {}
    table = {}
    for line in out.splitlines():
        parts = line.split(None, 3)
        if len(parts) < 4 or not parts[0].isdigit() or not parts[1].isdigit():
            continue
        if parts[2].startswith("Z"):
            continue
        table[int(parts[0])] = (int(parts[1]), parts[3].strip())
    return table


def process_identity(pid):
    proc_stat = Path(f"/proc/{pid}/stat")
    if Path("/proc").is_dir():
        try:
            fields = proc_stat.read_text().rsplit(")", 1)[1].split()
            if fields[0].startswith("Z"):
                return None
            return int(fields[1]), fields[19]
        except (OSError, ValueError, IndexError):
            return None
    try:
        out = subprocess.check_output(["ps", "-o", "ppid=,stat=,lstart=", "-p", str(int(pid))], text=True, stderr=subprocess.DEVNULL, timeout=5).strip()
        parts = out.split(None, 2)
        if len(parts) < 3 or parts[1].startswith("Z"):
            return None
        return int(parts[0]), parts[2].strip()
    except (OSError, ValueError, subprocess.SubprocessError):
        return None


def stop_owned_process(process):
    identities = {}
    if Path("/proc").is_dir():
        for entry in Path("/proc").iterdir():
            if entry.name.isdigit():
                item = process_identity(int(entry.name))
                if item:
                    identities[int(entry.name)] = item
    else:
        identities = _ps_process_table()
    owned = {process.pid}
    for _ in range(128):
        more = {pid for pid, (ppid, _) in identities.items() if ppid in owned}
        if more <= owned:
            break
        owned |= more
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(12)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(5)
    for pid in owned - {process.pid}:
        # Parent IDs change when reparented, but process start ticks cannot.
        current = process_identity(pid)
        if current and current[1] == identities[pid][1]:
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        current_children = {pid: process_identity(pid) for pid in owned - {process.pid}}
        if not any(item and item[1] == identities[pid][1] for pid, item in current_children.items()):
            break
        time.sleep(0.05)
    for pid in owned - {process.pid}:
        current = process_identity(pid)
        if current and current[1] == identities[pid][1]:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


def upgrade_lock(state=None):
    state = checked_path(state or STATE)
    state.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (state/"upgrade.lock").open("a") as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("another upgrade operation is running") from None
        yield


upgrade_lock = contextlib.contextmanager(upgrade_lock)


def instance_env(stage, mode, binary, port=18892):
    if port in (18889, 18890, 18891) or not 1024 <= port <= 65534:
        raise ValueError("candidate ports must differ from production")
    run_dir = checked_path(stage / ("instance-" + mode))
    if run_dir == ROOT/"runtime" or ROOT/"runtime/pure-api-home" in run_dir.parents:
        raise ValueError("unsafe candidate state directory")
    run_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    env = clean_env()
    env.update({"AGY_GW_HOME": str(ROOT), "AGY_GATEWAY_BIN": str(stage/FILES["gateway"]),
                "AGY_ACP_BIN": str(binary), "AGY_QUOTA_BIN": str(binary),
                "AGY_GW_PORT": str(port), "AGY_DASH_PORT": str(port+1),
                "AGY_LISTEN_HOST": "127.0.0.1", "AGY_RUN_DIR": str(run_dir),
                "AGY_LOG_DIR": str(run_dir/"logs"), "AGY_GW_LOG": str(run_dir/"logs/gateway.log"),
                "AGY_PURE_API_HOME": str(run_dir/"pure-api-home"),
                "AGY_STATE_DB": str(run_dir/"gateway.db"), "AGY_TOS_TRIPPED_FILE": str(run_dir/"tos-tripped"),
                "AGY_APPDATA_LINK": "", "AGY_GW_TMPLOG": "", "AGY_ACP_RECORD": "",
                "AGY_API_KEYS": "", "AGY_API_KEYS_FILE": "", "AGY_MAX_LIVE_WORKERS": "2",
                "AGY_DEFAULT_CWD": str(run_dir/"workspace"), "AGY_UPSTREAM_PROXY": ""})
    (run_dir/"workspace").mkdir(exist_ok=True)
    return env


def default_upstream_proxy():
    if "AGY_UPSTREAM_PROXY" in os.environ:
        return os.environ["AGY_UPSTREAM_PROXY"]
    try:
        with socket.create_connection(("127.0.0.1", 7890), timeout=0.15):
            return "http://127.0.0.1:7890"
    except OSError:
        return ""


def prepare_auth_link(pure_home):
    # The official CLI consumes existing auth; never copy its conversation tree.
    pure_home = checked_path(pure_home)
    app = pure_home / "antigravity-cli"
    app.mkdir(parents=True, exist_ok=True, mode=0o700)
    cli_home = Path.home() / ".gemini/antigravity-cli"
    target = cli_home / "antigravity-oauth-token"
    def link_meta():
        for rel in ("installation_id", "cache/onboarding.json"):
            src = cli_home / rel
            dst = app / rel
            if src.is_file() and not dst.exists():
                dst.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                dst.symlink_to(src)

    if target.is_file():
        link = app / target.name
        if not link.exists():
            link.symlink_to(target)
        link_meta()
        return
    if sys.platform == "darwin" and (cli_home / "installation_id").is_file():
        link_meta()
        return
    raise RuntimeError("existing official CLI login is unavailable")


def probe_metadata(port):
    from agy_upgrade_e2e import auth_headers
    for path in ("/healthz", "/v1/models"):
        request = urllib.request.Request(f"http://127.0.0.1:{port}{path}", headers=auth_headers())
        with urllib.request.urlopen(request, timeout=5) as r:
            data = json.load(r)
            if r.status != 200 or not isinstance(data, dict):
                raise RuntimeError("metadata probe failed")
            if path == "/v1/models" and not any(x.get("id", "").startswith("gemini-3.8-flash") for x in data.get("data", [])):
                raise RuntimeError("Flash model missing")


def gateway_instance(stage, mode, binary, port):
    env = instance_env(stage, mode, binary, port)
    if mode == "live":
        prepare_auth_link(Path(env["AGY_PURE_API_HOME"]))
        env["AGY_UPSTREAM_PROXY"] = default_upstream_proxy()
    log = stage/(mode+"-control.log")
    started = False
    try:
        run([ROOT/"scripts/gw.sh", "start", "go"], env=env, log=log, timeout=40)
        started = True
        pid = int((Path(env["AGY_RUN_DIR"])/"gateway.pid").read_text())
        receipt = stage/(mode+"-instance.json")
        write_json(receipt, {"mode": mode, "pid": pid, "port": port,
                              "gateway_sha256": sha(stage/FILES["gateway"]), "acp_sha256": sha(binary),
                              "acp_path": str(binary), "gateway_log": env["AGY_GW_LOG"]})
        probe_metadata(port)
        yield env, receipt
    finally:
        if started:
            run([ROOT/"scripts/gw.sh", "stop"], env=env, log=log, timeout=35)


gateway_instance = contextlib.contextmanager(gateway_instance)


def validate_manifest(stage, *, require_live=True, root=None, allow_blocked=False):
    stage = checked_path(stage)
    m = json.loads((stage/"manifest.json").read_text())
    if m.get("schema") != SCHEMA or m.get("source_digest") != source_digest(root):
        raise ValueError("candidate source/manifest mismatch; rebuild required")
    for ver_key in ("official_version", "patched_version"):
        if m.get(ver_key):
            ensure_version_not_blocked(m.get(ver_key), allow_blocked=allow_blocked, context="candidate agy")
    for key, name in FILES.items():
        if m.get("artifacts", {}).get(key) != sha(stage/name):
            raise ValueError("candidate artifact hash mismatch")
    required = ["offline", "media_strict", "fake_e2e", "contract"] + (["live_flash"] if require_live else [])
    for key in required:
        evidence = m.get("checks", {}).get(key, {})
        if evidence.get("passed") is not True:
            raise ValueError("missing successful acceptance: " + key)
        filename = evidence.get("file", "")
        if Path(filename).name != filename or not filename:
            raise ValueError("invalid evidence file")
        path = stage/filename
        if sha(path) != evidence.get("sha256"):
            raise ValueError("acceptance evidence hash mismatch")
        result = json.loads(path.read_text())
        if result.get("ok") is not True:
            raise ValueError("acceptance did not pass")
        if key == "live_flash" and (result.get("model") != "gemini-3.8-flash-low" or result.get("generation_requests") != 1):
            raise ValueError("live acceptance exceeds authorized model/count")
        if key == "fake_e2e":
            scenarios = result.get("scenarios") or {}
            missing = [n for n in FAKE_E2E_SCENARIOS if (scenarios.get(n) or {}).get("ok") is not True]
            if missing:
                raise ValueError("fake acceptance missing behavioral scenarios: " + ",".join(missing))
    return m


def skip_g5_media_check():
    return os.environ.get("AGY_SKIP_G5_MEDIA_CHECK", "").strip() == "1"


def check_media_strict_g5(binary, stage):
    report = stage / "media_strict.json"
    report.unlink(missing_ok=True)
    if skip_g5_media_check():
        write_json(report, {
            "ok": True,
            "skipped": True,
            "reason": "AGY_SKIP_G5_MEDIA_CHECK=1",
            "generation_requests": 0,
        })
        print("WARNING: G5 media strict-permission check skipped via AGY_SKIP_G5_MEDIA_CHECK=1", file=sys.stderr, flush=True)
        return
    run(
        [sys.executable, ROOT / "tools/agy_media_strict_check.py", "--binary", binary, "--report", report],
        log=stage / "patch.log",
        timeout=120,
    )
    if not report.is_file():
        raise RuntimeError("G5 media strict check did not produce media_strict.json")
    data = json.loads(report.read_text(encoding="utf-8"))
    if data.get("ok") is not True:
        errs = "; ".join(str(e) for e in (data.get("errors") or [])[:4])
        raise RuntimeError("G5 media strict check failed" + (": " + errs if errs else ""))


def patch_candidate(official, stage, dry=False):
    staged = stage/FILES["upstream"]
    shutil.copy2(official, staged)
    staged.chmod(0o755)
    flag = "--dry-run" if dry else "--in-place"
    run([sys.executable, ROOT/"third_party/agy-context-unlock/agy-patch.py", "--binary", staged, flag, "--acp-only", "--no-env-fix"] + ([] if dry else ["--no-backup"]), log=stage/"patch.log")
    run([sys.executable, ROOT/"tools/agy_extra_patch.py", "--binary", staged, flag], log=stage/"patch.log")
    if not dry:
        run([sys.executable, ROOT/"tools/agy_extra_patch.py", "--binary", staged, "--check"], log=stage/"patch.log")
    check_media_strict_g5(staged, stage)


def build_candidate(official, *, contract=False, live=False, port=18892, dry=False, allow_blocked=False):
    ensure_version_not_blocked(version(official), allow_blocked=allow_blocked, context="official agy")
    stage = checked_path(STATE)/"candidates"/(dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")+"-"+uuid.uuid4().hex[:8])
    stage.mkdir(parents=True, mode=0o700)
    print("candidate:", stage, flush=True)
    official_hash, initial_source = sha(official), source_digest()
    patch_candidate(official, stage, dry)
    if sha(official) != official_hash:
        raise RuntimeError("official source changed during build")
    if dry:
        return stage
    env = clean_env()
    run(["go", "vet", "./..."], cwd=ROOT/"go-gateway", env=env, log=stage/"offline.log")
    formatting = subprocess.check_output(["gofmt", "-l", "."], cwd=ROOT/"go-gateway", env=env, text=True)
    if formatting.strip():
        raise RuntimeError("gofmt gate failed")
    run(["go", "test", "-race", "-count=1", "./..."], cwd=ROOT/"go-gateway", env=env, log=stage/"offline.log")
    run([sys.executable, "-m", "unittest", "discover", "-s", "scripts", "-p", "test_*.py"], log=stage/"offline.log")
    write_json(stage/"offline.json", {"ok": True, "gofmt": True, "vet": True, "race": True, "python": True, "generation_requests": 0, "log_sha256": sha(stage/"offline.log")})
    env["CGO_ENABLED"] = "0"
    run(["go", "build", "-trimpath", "-ldflags=-s -w", "-o", stage/FILES["gateway"], "."], cwd=ROOT/"go-gateway", env=env, log=stage/"build.log")
    run(["go", "build", "-o", stage/"fakeacp", "./testdata/fakeacp"], cwd=ROOT/"go-gateway", env=env, log=stage/"build.log")
    with gateway_instance(stage, "fake", stage/"fakeacp", port) as (_, receipt):
        run([sys.executable, ROOT/"scripts/agy_upgrade_e2e.py", str(port), "--fake-receipt", receipt, "--report", stage/"fake_e2e.json"], log=stage/"fake-e2e.log", timeout=300)
    if contract or live:
        pure_home = stage/"contract-home"
        prepare_auth_link(pure_home)
        run([sys.executable, ROOT/"scripts/agy_acp_smoke.py", stage/FILES["upstream"], "--gemini-dir", pure_home, "--cwd", ROOT, "--report", stage/"contract.json"], log=stage/"contract.log", timeout=360)
    if live:
        with gateway_instance(stage, "live", stage/FILES["upstream"], port):
            run([sys.executable, ROOT/"scripts/agy_upgrade_e2e.py", str(port), "--live-flash", "--report", stage/"live_flash.json"], log=stage/"live-flash.log", timeout=150)
    if source_digest() != initial_source:
        raise RuntimeError("source changed while candidate was being tested; rebuild required")
    checks = {}
    for name in ("offline", "media_strict", "fake_e2e", "contract", "live_flash"):
        p = stage/(name+".json")
        if p.exists():
            checks[name] = {"passed": json.loads(p.read_text()).get("ok") is True, "file": p.name, "sha256": sha(p)}
    manifest = {"schema": SCHEMA, "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "git_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                "source_digest": initial_source, "official_version": version(official), "official_sha256": official_hash,
                "patched_version": version(stage/FILES["upstream"]), "checks": checks,
                "artifacts": {key: sha(stage/name) for key, name in FILES.items()}}
    write_json(stage/"manifest.json", manifest)
    write_json(STATE/"latest-candidate.json", {"path": str(stage), "release_ready": live})
    return stage


def atomic_copy(source, target):
    target = checked_path(target)
    tmp = target.with_name(target.name+".install-"+uuid.uuid4().hex)
    shutil.copy2(source, tmp)
    with tmp.open("rb") as f:
        os.fsync(f.fileno())
    os.replace(tmp, target)


def backup_release():
    backup = ROOT/"backups"/("upgrade-"+dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")+"-"+uuid.uuid4().hex[:6])
    backup.mkdir(parents=True, mode=0o700)
    present = []
    for name in [*FILES.values(), "agy-acp-900k.manifest.json"]:
        src = ROOT/"bin"/name
        if src.exists():
            shutil.copy2(src, backup/name)
            present.append(name)
    write_json(backup/"backup.json", {"files": {name: sha(backup/name) for name in present}})
    return backup


def restore_release(backup):
    saved = json.loads((backup/"backup.json").read_text())["files"]
    for name, digest in saved.items():
        if name not in [*FILES.values(), "agy-acp-900k.manifest.json"] or sha(backup/name) != digest:
            raise ValueError("rollback artifact verification failed")
    if not all(name in saved for name in FILES.values()):
        raise ValueError("incomplete rollback bundle")
    for name in [*FILES.values(), "agy-acp-900k.manifest.json"]:
        target = ROOT/"bin"/name
        if name in saved:
            atomic_copy(backup/name, target)
        elif target.exists():
            os.replace(target, backup/(name+".displaced-"+uuid.uuid4().hex[:6]))


def production_env():
    env = dict(os.environ)
    env.update({"AGY_GW_PORT": "18890", "AGY_DASH_PORT": "18891", "AGY_RUN_DIR": str(ROOT/"runtime"),
                "AGY_LOG_DIR": str(ROOT/"logs"), "AGY_GATEWAY_BIN": str(ROOT/"bin/agy-acp-gateway"),
                "AGY_ACP_BIN": str(ROOT/"bin/agy-acp-900k"), "AGY_STATE_DB": str(ROOT/"runtime/gateway.db"),
                "AGY_PURE_API_HOME": str(ROOT/"runtime/pure-api-home"),
                "AGY_TOS_TRIPPED_FILE": str(ROOT/"runtime/tos-tripped"), "AGY_GW_LOG": str(ROOT/"logs/gateway.log")})
    return env


def install(stage, *, allow_blocked=False):
    from agy_upgrade_e2e import run_probe
    # A fully qualified candidate is required; install never builds implicitly.
    manifest = validate_manifest(stage, allow_blocked=allow_blocked)
    backup = backup_release()
    write_json(STATE/"last-backup.json", {"path": str(backup)})
    env = production_env()
    run([ROOT/"scripts/gw.sh", "stop"], env=env, timeout=40)
    try:
        for name in FILES.values():
            atomic_copy(stage/name, ROOT/"bin"/name)
        atomic_copy(stage/"manifest.json", ROOT/"bin/agy-acp-900k.manifest.json")
        run([ROOT/"scripts/gw.sh", "start", "go"], env=env, timeout=40)
        probe_metadata(18890)
        result = run_probe(18890, live=True)
        write_json(backup/"post-install.json", result)
        if not result["ok"]:
            raise RuntimeError("post-install semantic probe failed")
    except Exception:
        run([ROOT/"scripts/gw.sh", "stop"], env=env, timeout=40)
        restore_release(backup)
        run([ROOT/"scripts/gw.sh", "start", "go"], env=env, timeout=40)
        raise RuntimeError("installation acceptance failed; previous bundle restored") from None
    write_json(STATE/"installed.json", {"candidate": str(stage), "backup": str(backup), "artifacts": manifest["artifacts"]})
    print("installed verified bundle; rollback:", backup)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    action = ap.add_mutually_exclusive_group()
    action.add_argument("--status", action="store_true")
    action.add_argument("--dry-run", action="store_true")
    action.add_argument("--install", action="store_true")
    action.add_argument("--rollback", nargs="?", const="latest")
    ap.add_argument("--candidate", type=Path)
    ap.add_argument("--official", type=Path, default=Path.home()/".local/bin/agy")
    ap.add_argument("--contract", action="store_true")
    ap.add_argument("--live-flash", action="store_true")
    ap.add_argument("--restart", action="store_true", help="compatibility flag; install always restarts safely")
    ap.add_argument("--allow-blocked", action="store_true", help="override DEFAULT_BLOCKED_AGY_VERSIONS guard")
    ap.add_argument("--port", type=int, default=18892)
    args = ap.parse_args()
    if args.status:
        for label, p in [("global", args.official), ("gateway-upstream", ROOT/"bin/agy-acp-900k")]:
            ver = version(p)
            reason = blocked_agy_reason(ver)
            info = {"component": label, "version": ver, "sha256": sha(p), "blocked": bool(reason)}
            if reason:
                info["blocked_reason"] = reason
            print(json.dumps(info))
        return 0
    with upgrade_lock():
        if args.rollback:
            backup = Path(json.loads((STATE/"last-backup.json").read_text())["path"]) if args.rollback == "latest" else checked_path(args.rollback)
            run([ROOT/"scripts/gw.sh", "stop"], env=production_env(), timeout=40)
            restore_release(backup)
            run([ROOT/"scripts/gw.sh", "start", "go"], env=production_env(), timeout=40)
            probe_metadata(18890)
        elif args.install:
            if not args.candidate:
                raise ValueError("--install requires --candidate; no implicit builds or model calls")
            install(args.candidate.resolve(), allow_blocked=args.allow_blocked)
        else:
            build_candidate(args.official.resolve(), contract=args.contract, live=args.live_flash, port=args.port, dry=args.dry_run, allow_blocked=args.allow_blocked)
    return 0


if __name__ == "__main__":
    os.umask(0o077)
    try:
        raise SystemExit(main())
    except (ValueError, RuntimeError, OSError, subprocess.SubprocessError) as exc:
        print("upgrade stopped:", str(exc), file=sys.stderr)
        raise SystemExit(1)
