#!/usr/bin/env python3
"""Production update agent for agy-acp-gateway.

Runs as root on the Docker host, next to docker-compose.yml. The gateway container never gets
docker access: it only displays <data>/upgrade/status.json and writes approval files.

  check   find a new official agy release (or a staged gateway release bundle), build and test a
          candidate (G1 integrity, G2 patches, G3 zero-generation ACP contract, G4 live Flash test
          in an isolated container) and publish its status. Never applies anything.
  apply   act on a dashboard approval that matches the tested candidate:
            agy     -> install under data/agy/<ver>-<sha8>/ and switch the data/agy/current symlink;
                       the gateway restarts its ACP engine when idle (no container restart);
                       verify with one Flash request, switch back on failure.
            gateway -> run the staged releases/<name>/release.sh (stop, swap, verify, auto-rollback).
  status  print the authoritative state.

The authoritative state lives in upgrader/state/ (root only). data/upgrade/status.json is a
published copy for the dashboard; approvals are only honoured when their candidate id and SHA-256
match the root copy and the candidate is "ready". .env and the login token are never read; the
token file is only bind-mounted read-only into the isolated test container.
"""
import argparse
import contextlib
import datetime as dt
import fcntl
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import subprocess
import sys
import tarfile
import time
import urllib.error
import urllib.request

HERE = Path(__file__).resolve().parent
ROOT = Path(os.environ.get("AGY_PROD_ROOT", HERE.parent))  # /root/agy-acp-gateway
DATA = ROOT / "data"
STATE = HERE / "state"
PUBLIC = DATA / "upgrade"
AGY_DIR = DATA / "agy"
TOKEN = DATA / ".gemini/antigravity-cli/antigravity-oauth-token"
OFFICIAL = "google-antigravity/antigravity-cli"
UPDATER_MANIFEST = "https://antigravity-cli-auto-updater-974169037036.us-central1.run.app/manifests/linux_amd64.json"
GCS_PREFIX = "https://storage.googleapis.com/antigravity-public/antigravity-cli/"
IMAGE = os.environ.get("AGY_PROD_IMAGE", "agy-acp-gateway:local")
CONTAINER = "agy-acp-gateway"
TEST_CONTAINER = "agy-upgrade-test"
GW_PORT = int(os.environ.get("AGY_GW_PORT") or os.environ.get("GW_PORT") or "18890")
DASH_PORT = int(os.environ.get("AGY_DASH_PORT") or os.environ.get("DASH_PORT") or "18891")
TEST_PORT = int(os.environ.get("AGY_UPGRADE_TEST_PORT", "18892"))
APPROVAL_MAX_AGE = dt.timedelta(hours=24)
APPLY_WAIT_SEC = 900  # how long apply waits for the gateway to go idle
LIVE_BUDGET_USD = 0.05
GATEWAY_UID = 10001
MAX_STAGE_KEEP = 2


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(1 << 20):
            h.update(chunk)
    return h.hexdigest()


def sha512(path):
    h = hashlib.sha512()
    with open(path, "rb") as f:
        while chunk := f.read(1 << 20):
            h.update(chunk)
    return h.hexdigest()


def log(msg):
    line = f"[{now()}] {msg}"
    print(line, flush=True)
    STATE.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (STATE / "upgrader.log").open("a") as f:
        f.write(line + "\n")


@contextlib.contextmanager
def lock():
    STATE.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (STATE / "upgrade.lock").open("a") as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit("another upgrader run is active")
        yield


def load_state():
    p = STATE / "status.json"
    if p.exists():
        return json.loads(p.read_text())
    return {"candidate": None, "gates": [], "history": []}


def save_state(st):
    st["updatedAt"] = now()
    st["current"] = current_versions()
    STATE.mkdir(parents=True, exist_ok=True, mode=0o700)
    tmp = STATE / "status.json.tmp"
    tmp.write_text(json.dumps(st, ensure_ascii=False, indent=2) + "\n")
    os.replace(tmp, STATE / "status.json")
    publish(st)


def publish(st):
    """Copy for the dashboard. The container owns data/upgrade, so it is never read back."""
    (PUBLIC / "approvals").mkdir(parents=True, exist_ok=True)
    if os.geteuid() == 0:  # the gateway (uid 10001) writes approvals and engine.json here
        os.chown(PUBLIC, GATEWAY_UID, GATEWAY_UID)
        os.chown(PUBLIC / "approvals", GATEWAY_UID, GATEWAY_UID)
    tmp = PUBLIC / ".status.json.tmp"
    tmp.write_text(json.dumps(st, ensure_ascii=False, indent=2) + "\n")
    tmp.chmod(0o644)
    os.replace(tmp, PUBLIC / "status.json")


def run(cmd, timeout=600, check=True, **kw):
    r = subprocess.run([str(c) for c in cmd], capture_output=True, text=True, timeout=timeout, **kw)
    if check and r.returncode != 0:
        raise RuntimeError(f"{Path(str(cmd[0])).name} failed ({r.returncode}): {(r.stderr or r.stdout)[-600:]}")
    return r


def agy_version(binary):
    return run([binary, "--version"], timeout=60).stdout.strip()


def current_agy_dir():
    cur = AGY_DIR / "current"
    return (AGY_DIR / os.readlink(cur)) if cur.is_symlink() else None


def current_versions():
    out = {}
    d = current_agy_dir()
    if d and (d / "agy-acp-900k").exists():
        out["agy"] = d.name.split("-")[0]
        out["agySha"] = sha256(d / "agy-acp-900k")
    gw = ROOT / "bin/agy-acp-gateway"
    if gw.exists():
        out["gateway"] = sha256(gw)
    return out


def semver(v):
    return tuple(int(x) for x in re.findall(r"\d+", v)[:3])


def gateway_key():
    r = run(["docker", "exec", CONTAINER, "sh", "-c", 'printf "%s" "${AGY_API_KEYS%%,*}"'], timeout=30)
    return r.stdout.strip()


def http_json(url, body, key, timeout=180, session=None):
    headers = {"Content-Type": "application/json", "x-api-key": key}
    if session:
        headers["x-claude-code-session-id"] = session
    req = urllib.request.Request(url, data=json.dumps(body).encode(), headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, json.loads(r.read())


def flash_probe(base, key, tag):
    """One Flash/low generation; returns (ok, detail) and never raises on HTTP/network errors."""
    body = {"model": "gemini-3.8-flash-low", "max_tokens": 16, "system": "Reply briefly. Do not use any tools.",
            "messages": [{"role": "user", "content": "Reply with exactly: OK"}]}
    t0 = time.monotonic()
    try:
        status, data = http_json(base + "/v1/messages", body, key, session=f"upgrader-{tag}-{time.time_ns()}")
    except urllib.error.HTTPError as exc:
        err_body = ""
        with contextlib.closing(exc), contextlib.suppress(Exception):
            err_body = exc.read(200).decode("utf-8", "replace")
        return False, f"HTTP {exc.code} {err_body[:80]!r} {time.monotonic() - t0:.1f}s"
    except Exception as exc:  # noqa: BLE001
        return False, f"{type(exc).__name__}: {str(exc)[:120]} ({time.monotonic() - t0:.1f}s)"
    if not isinstance(data, dict):
        return False, f"{status} invalid JSON payload {time.monotonic() - t0:.1f}s"
    text = "".join(x.get("text", "") for x in data.get("content", []) if isinstance(x, dict) and x.get("type") == "text")
    ok = status == 200 and data.get("stop_reason") == "end_turn" and bool(re.fullmatch(r"\s*OK[.!]?\s*", text, re.I))
    return ok, f"{status} {data.get('stop_reason')} {text[:40]!r} {time.monotonic() - t0:.1f}s"


# ---------------------------------------------------------------- candidate building

def fetch_github_release():
    req = urllib.request.Request(f"https://api.github.com/repos/{OFFICIAL}/releases/latest",
                                 headers={"User-Agent": "agy-prod-upgrader", "Accept": "application/vnd.github+json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        rel = json.loads(r.read(4 << 20))
    if rel.get("draft") or rel.get("prerelease") or not re.fullmatch(r"\d+\.\d+\.\d+", rel.get("tag_name", "")):
        raise RuntimeError("latest official release is not a stable version")
    return rel


def is_trusted_gcs_tarball(url):
    return bool(re.fullmatch(re.escape(GCS_PREFIX) + r"[A-Za-z0-9._/-]+\.tar\.gz", url)) and ".." not in url.split("/")


def fetch_manifest_release():
    req = urllib.request.Request(UPDATER_MANIFEST, headers={"User-Agent": "agy-prod-upgrader"})
    with urllib.request.urlopen(req, timeout=30) as r:
        manifest = json.loads(r.read(1 << 20))
    ver = str(manifest.get("version", "")).strip()
    url = str(manifest.get("url", "")).strip()
    digest = str(manifest.get("sha512", "")).strip().lower()
    if not re.fullmatch(r"\d+\.\d+\.\d+", ver):
        raise RuntimeError("updater manifest version is not a stable semver")
    if not is_trusted_gcs_tarball(url) or not re.fullmatch(r"[0-9a-f]{128}", digest):
        raise RuntimeError("updater manifest lacks a trusted GCS URL or SHA-512")
    return {"source": "manifest", "tag_name": ver, "url": url, "sha512": digest}


def latest_release():
    gh, mf, gh_err, mf_err = None, None, None, None
    try:
        gh = fetch_github_release()
    except Exception as exc:  # noqa: BLE001
        gh_err = exc
    try:
        mf = fetch_manifest_release()
    except Exception as exc:  # noqa: BLE001
        mf_err = exc
    if gh and mf:
        return mf if semver(mf["tag_name"]) > semver(gh["tag_name"]) else gh
    if gh:
        return gh
    if mf:
        return mf
    raise RuntimeError(f"release discovery failed: github={gh_err}; manifest={mf_err}")


def download_with_resume(url, dest, max_bytes=512 << 20, attempts=6):
    dest = Path(dest)
    dest.unlink(missing_ok=True)
    received, total = 0, None
    last_err = None
    try:
        for _ in range(attempts):
            headers = {"User-Agent": "agy-prod-upgrader"}
            if received:
                headers["Range"] = f"bytes={received}-"
            req = urllib.request.Request(url, headers=headers)
            try:
                with urllib.request.urlopen(req, timeout=120) as r:
                    status = getattr(r, "status", 200)
                    if received and status != 206:
                        received = 0
                    length = r.headers.get("Content-Length") if r.headers else None
                    total = (received + int(length)) if length is not None else None
                    if total is not None and total > max_bytes:
                        raise RuntimeError("download too large")
                    with dest.open("ab" if received else "wb") as out:
                        while chunk := r.read(1 << 20):
                            received += len(chunk)
                            if received > max_bytes:
                                raise RuntimeError("download too large")
                            out.write(chunk)
            except urllib.error.HTTPError as exc:
                if exc.code not in (408, 429) and 400 <= exc.code < 500:
                    raise
                last_err = exc
                received = dest.stat().st_size if dest.exists() else 0
                continue
            except (OSError, TimeoutError, EOFError, http.client.HTTPException) as exc:
                last_err = exc
                received = dest.stat().st_size if dest.exists() else 0
                continue
            if received > 0 and (total is None or received >= total):
                return received
        if last_err is not None and total is None:
            raise RuntimeError(f"download failed after {attempts} attempts: {last_err}")
        raise RuntimeError(f"download incomplete after {attempts} attempts ({received} of {total} bytes)")
    except BaseException:
        dest.unlink(missing_ok=True)
        raise


def unpack_official_tarball(archive, stage, expected_tag):
    try:
        with tarfile.open(archive, "r:gz") as tar:
            members = [m for m in tar.getmembers() if m.isfile() and Path(m.name).name in ("agy", "antigravity")]
            if len(members) != 1:
                raise RuntimeError("unexpected archive layout")
            with tar.extractfile(members[0]) as src, (stage / "agy-official").open("wb") as dst:
                shutil.copyfileobj(src, dst)
        (stage / "agy-official").chmod(0o755)
    finally:
        archive.unlink(missing_ok=True)
    ver = agy_version(stage / "agy-official")
    if ver != expected_tag:
        raise RuntimeError(f"binary reports {ver}, release is {expected_tag}")
    return ver


def download_g1(rel, stage):
    if rel.get("source") == "manifest":
        url = str(rel.get("url", ""))
        expected = str(rel.get("sha512", "")).lower()
        if not is_trusted_gcs_tarball(url) or not re.fullmatch(r"[0-9a-f]{128}", expected):
            raise RuntimeError("manifest lacks a trusted GCS URL or SHA-512")
        name = Path(url).name or "agy_cli_linux_x64.tar.gz"
        archive = stage / name
        total = download_with_resume(url, archive)
        if sha512(archive) != expected:
            archive.unlink(missing_ok=True)
            raise RuntimeError("manifest SHA-512 mismatch")
        ver = unpack_official_tarball(archive, stage, rel["tag_name"])
        return f"{name} sha512 {expected[:12]}..., {total} bytes, --version {ver} (manifest)"
    name = "agy_cli_linux_x64.tar.gz"
    assets = [a for a in rel.get("assets", []) if a.get("name") == name]
    if len(assets) != 1:
        raise RuntimeError("expected exactly one linux x64 asset")
    a = assets[0]
    digest = a.get("digest", "")
    prefix = f"https://github.com/{OFFICIAL}/releases/download/{rel['tag_name']}/"
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest) or not a.get("browser_download_url", "").startswith(prefix):
        raise RuntimeError("asset lacks a trusted SHA-256 or URL")
    archive = stage / name
    total = download_with_resume(a["browser_download_url"], archive)
    if total != a.get("size") or sha256(archive) != digest.split(":", 1)[1]:
        archive.unlink(missing_ok=True)
        raise RuntimeError("asset size or SHA-256 mismatch")
    ver = unpack_official_tarball(archive, stage, rel["tag_name"])
    return f"{name} sha256 {digest[7:19]}..., {total} bytes, --version {ver}"


def patch_g2(stage):
    target = stage / "agy-acp-900k"
    shutil.copy2(stage / "agy-official", target)
    run([sys.executable, HERE / "third_party/agy-context-unlock/agy-patch.py", "--binary", target, "--in-place", "--no-env-fix", "--no-backup"])
    run([sys.executable, HERE / "tools/agy_extra_patch.py", "--binary", target, "--in-place"])
    run([sys.executable, HERE / "tools/agy_extra_patch.py", "--binary", target, "--check"])
    return "context unlock and checkpoint patches applied and checked"


def _load_proxy_preset_map():
    out = {}
    cand_paths = []
    env_p = os.environ.get("AGY_PROXY_PRESETS_FILE", "").strip()
    if env_p:
        cand_paths.append(Path(env_p))
    cand_paths.append(DATA / "proxy-presets.json")
    for p in cand_paths:
        if not p.is_file():
            continue
        try:
            items = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(items, list):
            for it in items:
                if isinstance(it, dict) and it.get("id") and it.get("url"):
                    out[str(it["id"]).strip()] = str(it["url"]).strip()
        break
    return out


def _resolve_account_proxy(acc, default_proxy, presets):
    preset = str(acc.get("proxyPreset") or "").strip().lower()
    if preset == "direct":
        return ""
    if preset == "custom":
        return str(acc.get("customProxy") or "").strip()
    if preset and preset in presets:
        return presets[preset]
    return default_proxy


def resolve_probe_auth():
    """Choose an active authenticated account's token and upstream proxy for G3/G4.
    Prefers Primary when enabled & active; falls back to an active enabled non-primary
    account from data/accounts.json when Primary is disabled or draining (e.g. banned)."""
    default_proxy = os.environ.get("AGY_UPSTREAM_PROXY", "").strip()
    accounts_file = DATA / "accounts.json"
    if not accounts_file.is_file():
        return TOKEN, default_proxy
    try:
        raw = json.loads(accounts_file.read_text(encoding="utf-8"))
    except Exception:
        return TOKEN, default_proxy
    accs = raw.get("accounts") if isinstance(raw, dict) else None
    if not isinstance(accs, list) or not accs:
        return TOKEN, default_proxy

    presets = _load_proxy_preset_map()
    primary_fallback = None
    active_non_primary = []
    draining_non_primary = []
    for a in accs:
        if not isinstance(a, dict):
            continue
        acc_id = str(a.get("id") or "").strip()
        is_primary = bool(a.get("primary"))
        enabled = bool(a.get("enabled", True))
        sched = str(a.get("schedState") or ("active" if enabled else "disabled")).strip().lower()
        tok = TOKEN if is_primary else (DATA / "accounts" / acc_id / ".gemini/antigravity-cli/antigravity-oauth-token")
        if not tok.is_file():
            continue
        pxy = _resolve_account_proxy(a, default_proxy, presets)
        if is_primary:
            if enabled and sched == "active":
                return tok, pxy
            primary_fallback = (tok, pxy)
        elif enabled and sched == "active":
            active_non_primary.append((tok, pxy))
        elif enabled and sched == "draining":
            draining_non_primary.append((tok, pxy))

    if active_non_primary:
        return active_non_primary[0]
    if draining_non_primary:
        return draining_non_primary[0]
    if primary_fallback is not None:
        return primary_fallback
    return TOKEN, default_proxy


def isolated_home(stage, token_path=None):
    tok = token_path or resolve_probe_auth()[0]
    home = stage / "home"
    app = home / "antigravity-cli"
    app.mkdir(parents=True, exist_ok=True, mode=0o700)
    link = app / tok.name
    if link.is_symlink() or link.exists():
        link.unlink()
    link.symlink_to(tok)
    for rel in ("installation_id", "cache/onboarding.json"):
        src = tok.parent / rel
        dst = app / rel
        if src.is_file():
            dst.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            if dst.is_symlink() or dst.exists():
                dst.unlink()
            dst.symlink_to(src)
    return home


def contract_g3(stage):
    tok, upstream_proxy = resolve_probe_auth()
    env = dict(os.environ, AGY_UPSTREAM_PROXY=upstream_proxy)
    r = run([sys.executable, HERE / "scripts/agy_acp_smoke.py", stage / "agy-acp-900k", "--gemini-dir", isolated_home(stage, tok),
             "--cwd", "/tmp", "--report", stage / "contract.json"], timeout=420, check=False, env=env)
    rep = json.loads((stage / "contract.json").read_text()) if (stage / "contract.json").exists() else {}
    failed = [c["check"] for c in rep.get("checks", []) if not c.get("ok")]
    if r.returncode != 0 or not rep.get("ok"):
        raise RuntimeError("contract failed: " + (", ".join(failed) or r.stdout[-300:]))
    extra = rep.get("info", {}).get("new_status_templates") or []
    note = f"{len(rep['checks'])} checks, 0 generations"
    if extra:
        note += f"; {len(extra)} new status text(s) to review"
    return note


@contextlib.contextmanager
def test_container(agy_binary, stage):
    """Isolated copy of the production image with the candidate agy, its own /data and the
    login token mounted read-only. Loopback port only."""
    tok, upstream_proxy = resolve_probe_auth()
    data = stage / "testdata"
    (data / ".gemini/antigravity-cli/cache").mkdir(parents=True, exist_ok=True)
    for p in [data, *data.rglob("*")]:
        os.chown(p, GATEWAY_UID, GATEWAY_UID)
    key = "upgrade-test-" + secrets.token_hex(16)
    extra_docker_args = []
    if upstream_proxy:
        extra_docker_args += ["-e", f"AGY_UPSTREAM_PROXY={upstream_proxy}"]
    for rel in ("installation_id", "cache/onboarding.json"):
        src = tok.parent / rel
        if src.is_file():
            extra_docker_args += ["-v", f"{src}:/data/.gemini/antigravity-cli/{rel}:ro"]
    run(["docker", "rm", "-f", TEST_CONTAINER], check=False)
    run(["docker", "run", "-d", "--name", TEST_CONTAINER, "--memory", "1500m", "-p", f"127.0.0.1:{TEST_PORT}:18890",
         "-e", f"AGY_API_KEYS={key}", "-e", "AGY_ACP_BIN=/cand/agy-acp-900k", "-e", "AGY_QUOTA_BIN=/cand/agy-acp-900k",
         "-e", "AGY_MAX_LIVE_WORKERS=2", *extra_docker_args,
         "-v", f"{agy_binary}:/cand/agy-acp-900k:ro", "-v", f"{data}:/data",
         "-v", f"{tok}:/data/.gemini/antigravity-cli/antigravity-oauth-token:ro", IMAGE])
    try:
        base = f"http://127.0.0.1:{TEST_PORT}"
        for _ in range(60):
            try:
                urllib.request.urlopen(base + "/healthz", timeout=2)
                break
            except Exception:
                time.sleep(1)
        else:
            raise RuntimeError("test container not healthy")
        yield base, key
    finally:
        run(["docker", "rm", "-f", TEST_CONTAINER], check=False)


def live_g4(stage, agy_binary):
    """Flash/low only: plain reply, tool round trip (2 generations), no status text leaks."""
    with test_container(agy_binary, stage) as (base, key):
        ok, detail = flash_probe(base, key, "g4")
        if not ok:
            raise RuntimeError("text probe: " + detail)
        tools = [{"name": "Bash", "description": "Run a bash command",
                  "input_schema": {"type": "object", "properties": {"command": {"type": "string"}}, "required": ["command"]}}]
        sid = f"upgrader-tool-{time.time_ns()}"
        msgs = [{"role": "user", "content": "Use the Bash tool to run exactly: echo UPGRADE_OK . Then reply with the output only."}]
        body = {"model": "gemini-3.8-flash-low", "max_tokens": 512, "system": "Primary working directory: /tmp", "tools": tools}
        _, d1 = http_json(base + "/v1/messages", {**body, "messages": msgs}, key, session=sid)
        tu = [c for c in d1.get("content", []) if c.get("type") == "tool_use"]
        if d1.get("stop_reason") != "tool_use" or not tu:
            raise RuntimeError(f"tool step 1: stop={d1.get('stop_reason')}")
        msgs += [{"role": "assistant", "content": d1["content"]},
                 {"role": "user", "content": [{"type": "tool_result", "tool_use_id": tu[0]["id"], "content": "UPGRADE_OK"}]}]
        _, d2 = http_json(base + "/v1/messages", {**body, "messages": msgs}, key, session=sid)
        text = "".join(x.get("text", "") for x in d2.get("content", []) if x.get("type") == "text")
        if d2.get("stop_reason") != "end_turn" or "UPGRADE_OK" not in text or "⚠" in text:
            raise RuntimeError(f"tool step 2: stop={d2.get('stop_reason')} text={text[:60]!r}")
    return f"text {detail}; tool round trip ok; 3 Flash generations (budget ${LIVE_BUDGET_USD})"


def gateway_bundle():
    """A gateway release staged by the operator: releases/<name>/ with READY, release.sh,
    bin/agy-acp-gateway and manifest.json (built and tested locally, uploaded)."""
    cur = current_versions().get("gateway")
    for d in sorted((ROOT / "releases").glob("*/READY"), reverse=True):
        rel = d.parent
        if (rel / "result").exists():
            continue
        gw = rel / "bin/agy-acp-gateway"
        if not gw.exists() or not (rel / "release.sh").exists():
            continue
        sha = sha256(gw)
        if sha == cur:
            continue
        try:
            manifest = json.loads((rel / "manifest.json").read_text(encoding="utf-8"))
            if not isinstance(manifest, dict):
                raise ValueError("manifest root must be a JSON object")
        except (OSError, json.JSONDecodeError, TypeError, ValueError) as exc:
            return rel, sha, False, f"manifest.json unreadable or invalid: {str(exc)[:180]}"
        artifacts = manifest.get("artifacts")
        if not isinstance(artifacts, dict) or artifacts.get("gateway") != sha:
            return rel, sha, False, "manifest SHA-256 does not match bin/agy-acp-gateway"
        checks = manifest.get("checks")
        if not isinstance(checks, dict):
            return rel, sha, False, "manifest checks missing or invalid"
        missing = [
            k for k in ("offline", "fake_e2e", "contract")
            if not (isinstance(checks.get(k), dict) and checks[k].get("passed") is True)
        ]
        if missing:
            return rel, sha, False, "local gates missing: " + ", ".join(missing)
        return rel, sha, True, "local gates passed: " + ", ".join(sorted(checks))
    return None


def prune_candidates(st, keep=MAX_STAGE_KEEP):
    """Retain at most `keep` newest staged candidate directories plus any active stage."""
    cands_dir = STATE / "candidates"
    if not cands_dir.is_dir():
        return
    protected = set()
    cand = (st or {}).get("candidate")
    if isinstance(cand, dict) and cand.get("state") in ("testing", "ready", "approved", "applying", "waiting_idle"):
        raw_stage = cand.get("stage")
        if raw_stage:
            with contextlib.suppress(OSError):
                protected.add(Path(raw_stage).resolve())
    entries = []
    for d in cands_dir.iterdir():
        if d.is_symlink() or not d.is_dir():
            continue
        try:
            mtime = d.stat().st_mtime
        except OSError:
            continue
        entries.append((mtime, d.name, d))
    entries.sort(reverse=True)
    for idx, (_mtime, _name, d) in enumerate(entries):
        if idx < keep:
            continue
        try:
            if d.resolve() in protected:
                continue
        except OSError:
            continue
        shutil.rmtree(d, ignore_errors=True)


def _rollback_agy_candidate(cand, reason):
    prev = cand.get("previous")
    if prev and (AGY_DIR / prev / "agy-acp-900k").exists():
        switch_current(AGY_DIR / prev)
    cand["state"] = "rolled_back"
    cand["message"] = f"切换后验证失败, 已切回 {prev or '--'}: {reason}"
    log(cand["message"])


def cmd_check(args):
    st = load_state()
    cand = st.get("candidate")
    if cand and cand.get("state") == "waiting_idle":
        try:
            ok = finish_agy(st, cand, wait=0)
        except Exception as exc:  # noqa: BLE001
            _rollback_agy_candidate(cand, f"{type(exc).__name__}: {str(exc)[:180]}")
            ok = False
        record_history(st, cand)
        save_state(st)
        prune_candidates(st)
        return 0 if ok else 1
    if cand and cand.get("state") in ("ready", "approved", "applying") and not args.force:
        log(f"candidate {cand['id']} is {cand['state']}; nothing to check")
        save_state(st)
        prune_candidates(st)
        return 0
    bundle = gateway_bundle()
    if bundle:
        rel, sha, ok, detail = bundle
        cid = f"gateway-{rel.name}-{sha[:8]}"
        st["candidate"] = {"id": cid, "kind": "gateway", "version": rel.name, "sha256": sha, "release": rel.name,
                           "state": "ready" if ok else "failed", "createdAt": now(), "message": detail}
        st["gates"] = [{"name": "G5 网关发布包", "ok": ok, "detail": detail}]
        st["costUsd"] = 0
        log(f"gateway candidate {cid}: {detail}")
        save_state(st)
        prune_candidates(st)
        return 0
    rel = latest_release()
    tag = rel["tag_name"]
    cur = current_versions().get("agy", "0")
    if semver(tag) <= semver(cur) and not args.force:
        log(f"agy {cur} is current (latest {tag})")
        if cand and cand.get("state") == "failed" and cand.get("version") != tag:
            st["candidate"] = None
        save_state(st)
        prune_candidates(st)
        return 0
    stage = STATE / "candidates" / f"agy-{tag}-{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%SZ}"
    stage.mkdir(parents=True, mode=0o700)
    gates = []
    st["candidate"] = {"id": stage.name, "kind": "agy", "version": tag, "sha256": "", "state": "testing", "createdAt": now(), "stage": str(stage)}
    st["gates"] = gates
    st["costUsd"] = 0
    save_state(st)
    steps = [("G1 完整性", lambda: download_g1(rel, stage)), ("G2 补丁", lambda: patch_g2(stage)),
             ("G3 ACP 契约", lambda: contract_g3(stage)), ("G4 真实验收", lambda: live_g4(stage, stage / "agy-acp-900k"))]
    for name, fn in steps:
        try:
            detail = fn()
            gates.append({"name": name, "ok": True, "detail": detail})
            if name.startswith("G4"):
                st["costUsd"] = LIVE_BUDGET_USD
            log(f"{stage.name} {name}: ok, {detail}")
        except Exception as exc:  # noqa: BLE001 - every failure ends the candidate
            gates.append({"name": name, "ok": False, "detail": str(exc)[:300]})
            st["candidate"]["state"] = "failed"
            st["candidate"]["message"] = f"{name}: {str(exc)[:200]}"
            log(f"{stage.name} {name}: FAILED {exc}")
            save_state(st)
            prune_candidates(st)
            return 1
        save_state(st)
    st["candidate"]["sha256"] = sha256(stage / "agy-acp-900k")
    st["candidate"]["state"] = "ready"
    st["candidate"]["message"] = "等待面板确认"
    save_state(st)
    prune_candidates(st)
    log(f"{stage.name} ready ({st['candidate']['sha256'][:12]})")
    return 0


# ---------------------------------------------------------------- applying

def take_approval(cand):
    approvals_dir = PUBLIC / "approvals"
    if not approvals_dir.is_dir():
        return None
    matched = None
    matched_at = None
    now_utc = dt.datetime.now(dt.timezone.utc)
    used = STATE / "approvals"
    used.mkdir(parents=True, exist_ok=True, mode=0o700)
    for idx, p in enumerate(sorted(approvals_dir.glob("*.json"))):
        try:
            raw = p.read_text(encoding="utf-8", errors="replace")
        except OSError as exc:
            log(f"ignored unreadable approval {p.name}: {exc}")
            with contextlib.suppress(OSError):
                p.unlink()
            continue
        dest = used / f"{int(time.time() * 1000)}-{idx}-{p.name}"
        try:
            shutil.move(str(p), dest)
        except OSError as exc:
            log(f"failed to archive approval {p.name}: {exc}")
            with contextlib.suppress(OSError):
                p.unlink()
            continue
        try:
            a = json.loads(raw)
            if not isinstance(a, dict):
                raise ValueError("approval payload must be a JSON object")
            raw_at = a.get("approvedAt")
            if not isinstance(raw_at, str) or not raw_at.strip():
                raise ValueError("missing approvedAt")
            approved = dt.datetime.fromisoformat(raw_at.strip().replace("Z", "+00:00"))
            if approved.tzinfo is None:
                raise ValueError("approvedAt must include timezone")
            approved_utc = approved.astimezone(dt.timezone.utc)
            age = now_utc - approved_utc
            fresh = dt.timedelta(seconds=-300) <= age < APPROVAL_MAX_AGE
            if (
                isinstance(cand, dict)
                and cand.get("id") is not None
                and a.get("candidateId") == cand.get("id")
                and a.get("sha256") == cand.get("sha256")
                and fresh
            ):
                if matched_at is None or approved_utc >= matched_at:
                    matched = a
                    matched_at = approved_utc
                continue
            log(f"ignored approval {p.name}: does not match the ready candidate or expired")
        except Exception as exc:  # noqa: BLE001
            log(f"ignored malformed approval {p.name}: {exc}")
    return matched


def switch_current(target_dir):
    tmp = AGY_DIR / f".current-{os.getpid()}"
    with contextlib.suppress(FileNotFoundError):
        tmp.unlink()
    tmp.symlink_to(target_dir.name)
    os.replace(tmp, AGY_DIR / "current")


def engine_info(key):
    """Live engine state from the gateway (engine.json is only written when an engine starts)."""
    try:
        req = urllib.request.Request(f"http://127.0.0.1:{DASH_PORT}/api/upgrade", headers={"x-api-key": key})
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read()).get("engine") or {}
    except Exception:
        return {}


def apply_agy(st, cand):
    stage = Path(cand["stage"])
    src = stage / "agy-acp-900k"
    if sha256(src) != cand["sha256"]:
        raise RuntimeError("candidate binary changed after testing")
    prev = current_agy_dir()
    dest = AGY_DIR / f"{cand['version']}-{cand['sha256'][:8]}"
    dest.mkdir(parents=True, exist_ok=True)
    os.chmod(dest, 0o755)
    shutil.copy2(src, dest / "agy-acp-900k.tmp")
    os.chmod(dest / "agy-acp-900k.tmp", 0o755)
    os.replace(dest / "agy-acp-900k.tmp", dest / "agy-acp-900k")
    cand["installed"] = dest.name
    cand["previous"] = prev.name if prev and prev != dest else cand.get("previous")
    switch_current(dest)
    cand["state"] = "waiting_idle"
    cand["message"] = "软链已切换, 等待网关空闲后重启 ACP 引擎"
    save_state(st)
    log(f"switched data/agy/current -> {dest.name}")
    return finish_agy(st, cand, wait=APPLY_WAIT_SEC)


def finish_agy(st, cand, wait):
    """Wait for the engine to run the new binary, then verify it with one Flash request.
    Under constant traffic the engine may stay busy past `wait`: the symlink stays and the next
    check run finishes the job. A failed probe on the new binary switches back."""
    key = gateway_key()
    deadline = time.time() + wait
    eng = engine_info(key)
    while cand["installed"] not in eng.get("running", "") and eng.get("alive") is True and time.time() < deadline:
        time.sleep(10)
        eng = engine_info(key)
    if cand["installed"] not in eng.get("running", "") and eng.get("alive") is True:
        cand["message"] = "软链已切换, 网关仍有进行中的请求, 空闲后自动切换 (下次检查时验证)"
        log(cand["message"])
        return True
    try:
        ok, detail = flash_probe(f"http://127.0.0.1:{GW_PORT}", key, "apply")
    except urllib.error.HTTPError as exc:
        err_body = ""
        with contextlib.closing(exc), contextlib.suppress(Exception):
            err_body = exc.read(200).decode("utf-8", "replace")
        ok, detail = False, f"HTTP {exc.code} {err_body[:80]!r}"
    except Exception as exc:  # noqa: BLE001
        ok, detail = False, f"{type(exc).__name__}: {str(exc)[:120]}"
    eng = engine_info(key)
    if ok and cand["installed"] in eng.get("running", ""):
        cand["state"] = "applied"
        cand["message"] = f"agy {cand['version']} 已生效; 探针 {detail}"
        log(cand["message"])
        return True
    _rollback_agy_candidate(cand, f"{detail}; running={eng.get('running')}")
    return False


def apply_gateway(st, cand):
    rel = ROOT / "releases" / cand["release"]
    if sha256(rel / "bin/agy-acp-gateway") != cand["sha256"]:
        raise RuntimeError("gateway bundle changed after it was offered")
    cand["state"] = "applying"
    save_state(st)
    r = run(["sh", rel / "release.sh"], timeout=900, check=False, cwd=ROOT)
    (STATE / f"release-{rel.name}.log").write_text(r.stdout + r.stderr)
    result = (rel / "result").read_text().strip() if (rel / "result").exists() else ""
    if r.returncode == 0 and result == "DEPLOYED_AND_VERIFIED":
        cand["state"] = "applied"
        cand["message"] = "网关已更新并通过上线检查"
        log(f"gateway {rel.name} applied")
        return True
    cand["state"] = "rolled_back" if result == "ROLLED_BACK" else "failed"
    cand["message"] = f"发布脚本失败 ({r.returncode}, {result or 'no result'})" + (", 已自动回滚" if result == "ROLLED_BACK" else ", 未改动生产")
    log(f"gateway {rel.name}: {cand['message']}")
    return False


def cmd_apply(args):
    st = load_state()
    cand = st.get("candidate")
    if cand and cand.get("state") == "waiting_idle":
        take_approval({"id": None})  # discard stale approvals
        try:
            ok = finish_agy(st, cand, wait=0)
        except Exception as exc:  # noqa: BLE001
            _rollback_agy_candidate(cand, f"{type(exc).__name__}: {str(exc)[:180]}")
            ok = False
        record_history(st, cand)
        save_state(st)
        prune_candidates(st)
        return 0 if ok else 1
    if not cand or cand.get("state") != "ready":
        take_approval({"id": None})  # discard stale approvals
        return 0
    approval = take_approval(cand)
    if not approval:
        return 0
    log(f"approval for {cand['id']} accepted")
    cand["state"] = "approved"
    cand["approvedAt"] = approval.get("approvedAt")
    save_state(st)
    try:
        ok = apply_agy(st, cand) if cand["kind"] == "agy" else apply_gateway(st, cand)
    except Exception as exc:  # noqa: BLE001
        if cand.get("kind") == "agy" and cand.get("installed"):
            _rollback_agy_candidate(cand, f"{type(exc).__name__}: {str(exc)[:180]}")
        else:
            cand["state"] = "failed"
            cand["message"] = f"执行失败: {str(exc)[:200]}"
        ok = False
    record_history(st, cand)
    save_state(st)
    prune_candidates(st)
    return 0 if ok else 1


def record_history(st, cand):
    if cand["state"] in ("applied", "rolled_back", "failed"):
        st.setdefault("history", []).insert(0, {"id": cand["id"], "kind": cand["kind"], "version": cand["version"], "state": cand["state"], "at": now()})
        st["history"] = st["history"][:10]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("check")
    c.add_argument("--force", action="store_true", help="test even when the latest release is not newer")
    sub.add_parser("apply")
    sub.add_parser("status")
    args = ap.parse_args()
    if args.cmd == "status":
        print(json.dumps(load_state(), ensure_ascii=False, indent=2))
        return 0
    with lock():
        return {"check": cmd_check, "apply": cmd_apply}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
