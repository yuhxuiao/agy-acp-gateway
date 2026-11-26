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
match the root copy and the candidate is "ready". .env and the login tokens are never read; token
files are only bind-mounted read-only into the isolated test container. Multi-account: G3/G4 pick
the account with the most Gemini 5h quota that may serve Flash (named Resin presets resolved like the
gateway), route to the next account when one is quota-limited, and a post-switch probe that only
fails on pool quota defers verification instead of rolling back.
"""
import argparse
import contextlib
import contextvars
import datetime as dt
import fcntl
import hashlib
import http.client
import ipaddress
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import signal
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.error
import urllib.parse
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
DEFAULT_VERIFY_DEFER_BACKOFF_SECS = (900, 1800, 3600)
DEFAULT_BLOCKED_AGY_VERSIONS = {
    "1.5.17": "agy#1890 (--agent 4x Gemini quota burn) & agy#1872",
    "1.5.18": "agy#1890 (--agent 4x Gemini quota burn) & agy#1872",
}
VALID_ACCOUNT_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,31}$")
VALID_PROXY_PRESET_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
EXCLUDED_DOCKER_NETWORKS = {"bridge", "host", "none"}
_URL_CRED_RE = re.compile(r"([a-zA-Z][a-zA-Z0-9+.-]*://)([^/\s@\"'<>:]*):([^/\s@\"'<>]+)@")
_URL_USER_RE = re.compile(r"([a-zA-Z][a-zA-Z0-9+.-]*://)([^/\s@\"'<>:]+)@")
_RUNTIME_SECRETS = set()
_G4_ATTEMPT_IDX = contextvars.ContextVar("g4_attempt_idx", default=None)


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


def _register_secret(secret):
    s = str(secret or "").strip()
    if len(s) >= 6:
        _RUNTIME_SECRETS.add(s)
        quoted = urllib.parse.quote(s, safe="")
        if len(quoted) >= 6:
            _RUNTIME_SECRETS.add(quoted)


def _register_url_secret(raw_url):
    if not raw_url:
        return
    try:
        u = urllib.parse.urlsplit(str(raw_url))
    except ValueError:
        return
    if u.password:
        _register_secret(u.password)
        _register_secret(urllib.parse.unquote(u.password))


def redact_secrets(text, extra_secrets=None):
    s = str(text)
    s = _URL_CRED_RE.sub(r"\1\2:***@", s)
    s = _URL_USER_RE.sub(r"\1***@", s)
    secrets_set = set(_RUNTIME_SECRETS)
    if extra_secrets:
        for sec in extra_secrets:
            sv = str(sec or "").strip()
            if len(sv) >= 6:
                secrets_set.add(sv)
                q = urllib.parse.quote(sv, safe="")
                if len(q) >= 6:
                    secrets_set.add(q)
    for sec in sorted(secrets_set, key=len, reverse=True):
        if sec in s:
            s = s.replace(sec, "***")
    return s


def _redact_obj(obj):
    if isinstance(obj, str):
        return redact_secrets(obj)
    if isinstance(obj, dict):
        return {k: _redact_obj(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_redact_obj(v) for v in obj]
    if isinstance(obj, tuple):
        return tuple(_redact_obj(v) for v in obj)
    return obj


def log(msg):
    line = f"[{now()}] {redact_secrets(msg)}"
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
    clean = _redact_obj(st)
    STATE.mkdir(parents=True, exist_ok=True, mode=0o700)
    tmp = STATE / f".status.json.{os.getpid()}.{secrets.token_hex(6)}.tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            fd = -1
            f.write(json.dumps(clean, ensure_ascii=False, indent=2) + "\n")
        os.replace(tmp, STATE / "status.json")
    except BaseException:
        if fd >= 0:
            os.close(fd)
        with contextlib.suppress(OSError):
            tmp.unlink()
        raise
    publish(clean)


def publish(st):
    """Copy for the dashboard. The container owns data/upgrade, so it is never read back."""
    if DATA.is_symlink():
        raise RuntimeError("refusing symlink DATA dir")
    if PUBLIC.is_symlink():
        raise RuntimeError("refusing symlink upgrade dir")
    approvals_dir = PUBLIC / "approvals"
    if approvals_dir.is_symlink():
        raise RuntimeError("refusing symlink approvals dir")
    PUBLIC.mkdir(parents=True, exist_ok=True)
    if PUBLIC.is_symlink() or not PUBLIC.is_dir():
        raise RuntimeError("upgrade dir is not a real directory")
    if approvals_dir.is_symlink():
        raise RuntimeError("refusing symlink approvals dir")
    approvals_dir.mkdir(exist_ok=True)
    if approvals_dir.is_symlink() or not approvals_dir.is_dir():
        raise RuntimeError("approvals dir is not a real directory")
    target = PUBLIC / "status.json"
    if target.is_symlink():
        raise RuntimeError("refusing symlink status.json")
    if os.geteuid() == 0:  # the gateway (uid 10001) writes approvals and engine.json here
        os.lchown(PUBLIC, GATEWAY_UID, GATEWAY_UID)
        os.lchown(approvals_dir, GATEWAY_UID, GATEWAY_UID)
    payload = json.dumps(_redact_obj(st), ensure_ascii=False, indent=2) + "\n"
    tmp = PUBLIC / f".status.json.{os.getpid()}.{secrets.token_hex(8)}.tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o644)
    try:
        os.fchmod(fd, 0o644)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            fd = -1
            f.write(payload)
            f.flush()
        if tmp.is_symlink() or target.is_symlink():
            raise RuntimeError("refusing symlink during status publish")
        os.replace(tmp, target)
    except BaseException:
        if fd >= 0:
            os.close(fd)
        with contextlib.suppress(OSError):
            tmp.unlink()
        raise


def run(cmd, timeout=600, check=True, **kw):
    r = subprocess.run([str(c) for c in cmd], capture_output=True, text=True, timeout=timeout, **kw)
    if check and r.returncode != 0:
        raise RuntimeError(f"{Path(str(cmd[0])).name} failed ({r.returncode}): {(r.stderr or r.stdout)[-600:]}")
    return r


def agy_version(binary):
    return run([binary, "--version"], timeout=60).stdout.strip()


def _normalize_agy_version(ver):
    s = str(ver or "").strip()
    if len(s) >= 2 and s[0] in ("v", "V") and s[1].isdigit():
        s = s[1:]
    return s


def _extract_dir_agy_version(dirname):
    first = _normalize_agy_version(str(dirname or "").split("-")[0])
    if re.fullmatch(r"\d+\.\d+\.\d+", first):
        return first
    m = re.search(r"(?<![0-9])(\d+\.\d+\.\d+)(?![0-9])", str(dirname or ""))
    if m:
        return m.group(1)
    return first


def blocked_agy_versions() -> dict[str, str]:
    """Return {version: reason} for agy releases blocked from auto-staging or applying.

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
    out: dict[str, str] = {}
    for part in s.split(","):
        item = part.strip()
        if not item:
            continue
        if ":" in item:
            raw_ver, raw_reason = item.split(":", 1)
            ver = _normalize_agy_version(raw_ver)
            reason = raw_reason.strip()
        else:
            ver = _normalize_agy_version(item)
            reason = ""
        if not ver:
            continue
        if not reason:
            reason = DEFAULT_BLOCKED_AGY_VERSIONS.get(ver, "blocked by AGY_BLOCKED_VERSIONS")
        out[ver] = reason
    return out


def blocked_agy_reason(version, blocked=None):
    ver = _normalize_agy_version(version)
    if not ver:
        return None
    table = blocked_agy_versions() if blocked is None else blocked
    return table.get(ver)


def current_agy_dir():
    cur = AGY_DIR / "current"
    return (AGY_DIR / os.readlink(cur)) if cur.is_symlink() else None


def current_versions():
    out = {}
    d = current_agy_dir()
    if d and (d / "agy-acp-900k").exists():
        out["agy"] = _extract_dir_agy_version(d.name)
        out["agySha"] = sha256(d / "agy-acp-900k")
    gw = ROOT / "bin/agy-acp-gateway"
    if gw.exists():
        out["gateway"] = sha256(gw)
    return out


def _candidate_matches_current_agy(cand, cur_vers, cur_dir):
    """Return False when live data/agy/current exists and disagrees with cand (e.g. manual symlink rollback)."""
    if not cur_vers.get("agy") and cur_dir is None:
        return True
    cand_ver = _normalize_agy_version(cand.get("version"))
    cur_ver = _normalize_agy_version(cur_vers.get("agy"))
    if cand_ver and cur_ver and cand_ver != cur_ver:
        return False
    installed = str(cand.get("installed") or "").strip()
    if installed and cur_dir is not None and cur_dir.name != installed:
        return False
    return True


def reconcile_agy_candidate(st, blocked=None, log_changes=True):
    """Reconcile stored agy candidate against live symlink state and version blocklist.

    Returns True when st was modified.
    """
    cand = st.get("candidate")
    if not isinstance(cand, dict) or cand.get("kind") != "agy":
        return False
    state = cand.get("state")
    cur_vers = current_versions()
    cur_dir = current_agy_dir()
    cur_ver = _normalize_agy_version(cur_vers.get("agy"))
    cur_dir_name = cur_dir.name if cur_dir else ""
    cand_ver = _normalize_agy_version(cand.get("version"))

    if state in ("applied", "waiting_idle", "verify_deferred") and not _candidate_matches_current_agy(cand, cur_vers, cur_dir):
        if log_changes:
            log(
                f"clearing stale {state} candidate {cand.get('id')} (agy {cand_ver}); "
                f"current symlink is {cur_dir_name or cur_ver or 'unknown'}"
            )
        st["candidate"] = None
        st["gates"] = []
        return True

    if state in ("ready", "approved"):
        reason = blocked_agy_reason(cand_ver, blocked)
        if reason:
            note = f"skipping blocked agy {cand_ver}: {reason}"
            if log_changes:
                log(f"clearing blocked {state} candidate {cand.get('id')} (agy {cand_ver}): {reason}")
            st["candidate"] = None
            st["gates"] = []
            st["lastCheckNote"] = note
            return True

    return False


def current_state():
    """Return authoritative state refreshed with live symlink current_versions() and reconciled."""
    st = load_state()
    st["current"] = current_versions()
    reconcile_agy_candidate(st, blocked=blocked_agy_versions(), log_changes=False)
    return st


def semver(v):
    return tuple(int(x) for x in re.findall(r"\d+", v)[:3])


def _parse_api_keys_output(raw):
    for line in str(raw or "").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        for part in s.split(","):
            k = part.strip()
            if k and not k.startswith("#"):
                return k
    return ""


def gateway_key():
    cmd = (
        'if [ -n "${AGY_API_KEYS:-}" ]; then printf "%s\\n" "$AGY_API_KEYS" | tr "," "\\n"; fi; '
        'if [ -n "${AGY_API_KEYS_FILE:-}" ] && [ -r "$AGY_API_KEYS_FILE" ]; then cat "$AGY_API_KEYS_FILE"; fi'
    )
    r = run(["docker", "exec", CONTAINER, "sh", "-c", cmd], timeout=30)
    key = _parse_api_keys_output(r.stdout)
    if key:
        _register_secret(key)
    return key


def http_json(url, body, key, timeout=180, session=None):
    headers = {"Content-Type": "application/json"}
    if key:
        headers["x-api-key"] = key
    if session:
        headers["x-claude-code-session-id"] = session
    req = urllib.request.Request(url, data=json.dumps(body).encode(), headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, json.loads(r.read())


class ProbeError(RuntimeError):
    def __init__(self, detail, http_status=0, err_code=""):
        super().__init__(detail)
        self.http_status = int(http_status or 0)
        self.err_code = str(err_code or "").strip().lower()
        self.detail = str(detail or "")


def _parse_http_error_body(raw_bytes):
    if isinstance(raw_bytes, str):
        body_text = raw_bytes[:512]
    else:
        body_text = (raw_bytes or b"")[:512].decode("utf-8", "replace")
    err_code = ""
    try:
        obj = json.loads(body_text)
    except Exception:
        obj = None
    if isinstance(obj, dict):
        err = obj.get("error")
        if isinstance(err, dict):
            for k in ("code", "type", "status"):
                v = err.get(k)
                if isinstance(v, str) and v.strip():
                    err_code = v.strip().lower()
                    break
        elif isinstance(err, str) and err.strip():
            err_code = err.strip().lower()
        if not err_code:
            for k in ("code", "type", "status"):
                v = obj.get(k)
                if isinstance(v, str) and v.strip() and v.strip().lower() != "error":
                    err_code = v.strip().lower()
                    break
    if not err_code and body_text:
        for field in ("code", "type", "status"):
            for m in re.finditer(rf'"{field}"\s*:\s*"([^"]+)"', body_text):
                val = m.group(1).strip().lower()
                if val and not (field == "type" and val == "error"):
                    err_code = val
                    break
            if err_code:
                break
    return err_code, body_text


def _read_http_error(exc):
    raw = b""
    with contextlib.closing(exc), contextlib.suppress(Exception):
        raw = exc.read(512)
    return _parse_http_error_body(raw)


def flash_probe(base, key, tag):
    """One Flash/low generation; returns (ok, http_status, err_code, detail) and never raises on HTTP/network errors."""
    body = {"model": "gemini-3.8-flash-low", "max_tokens": 16, "system": "Reply briefly. Do not use any tools.",
            "messages": [{"role": "user", "content": "Reply with exactly: OK"}]}
    t0 = time.monotonic()
    try:
        status, data = http_json(base + "/v1/messages", body, key, session=f"upgrader-{tag}-{time.time_ns()}")
    except urllib.error.HTTPError as exc:
        err_code, err_body = _read_http_error(exc)
        code_part = f" [{err_code}]" if err_code else ""
        return False, exc.code, err_code, f"HTTP {exc.code}{code_part} {err_body[:240]!r} {time.monotonic() - t0:.1f}s"
    except Exception as exc:  # noqa: BLE001
        return False, 0, "", f"{type(exc).__name__}: {str(exc)[:180]} ({time.monotonic() - t0:.1f}s)"
    if not isinstance(data, dict):
        return False, status, "", f"{status} invalid JSON payload {time.monotonic() - t0:.1f}s"
    text = "".join(x.get("text", "") for x in data.get("content", []) if isinstance(x, dict) and x.get("type") == "text")
    ok = status == 200 and data.get("stop_reason") == "end_turn" and bool(re.fullmatch(r"\s*OK[.!]?\s*", text, re.I))
    return ok, status, "", f"{status} {data.get('stop_reason')} {text[:40]!r} {time.monotonic() - t0:.1f}s"


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


def _is_ip_address(host):
    if not host:
        return False
    try:
        ipaddress.ip_address(str(host).strip("[]"))
        return True
    except ValueError:
        return False


def _validate_proxy_url(raw):
    """Validate proxy URL matching go-gateway validateCustomProxyURL, strictly rejecting control/whitespace."""
    if not isinstance(raw, str):
        return None
    if any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in raw):
        return None
    s = raw.strip()
    if not s:
        return None
    if any(ch in s for ch in ("\r", "\n", "\t", " ", "\x00")):
        return None
    try:
        u = urllib.parse.urlsplit(s)
        _ = u.port  # raises ValueError on invalid port
    except ValueError:
        return None
    if u.scheme.lower() not in ("http", "https", "socks5", "socks5h"):
        return None
    if not u.hostname or not u.netloc:
        return None
    return s


def _is_safe_data_file(path, root_dir=None):
    """Verify path is a regular non-symlink file inside DATA with no symlink parent components inside DATA."""
    base = Path(root_dir) if root_dir is not None else DATA
    p = Path(path)
    if base.is_symlink() or not base.is_dir():
        return False
    try:
        base_abs = base.absolute()
        p_abs = p.absolute()
        rel = p_abs.relative_to(base_abs)
    except ValueError:
        return False
    cur = base_abs
    for part in rel.parts:
        cur = cur / part
        if cur.is_symlink():
            return False
    try:
        st = os.lstat(p_abs)
    except OSError:
        return False
    if not stat.S_ISREG(st.st_mode):
        return False
    try:
        return p_abs.resolve().is_relative_to(base.resolve())
    except OSError:
        return False


def _read_safe_json_file(path, max_bytes=1 << 20):
    p = Path(path)
    if p.is_symlink():
        return None
    try:
        fd = os.open(p, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError:
        return None
    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode):
            return None
        with os.fdopen(fd, "rb") as f:
            fd = -1
            raw = f.read(max_bytes + 1)
        if len(raw) > max_bytes:
            return None
        return json.loads(raw.decode("utf-8"))
    except Exception:  # noqa: BLE001
        return None
    finally:
        if fd >= 0:
            os.close(fd)


def _load_proxy_preset_map():
    out = {}
    cand_paths = []
    env_p = os.environ.get("AGY_PROXY_PRESETS_FILE", "").strip()
    if env_p:
        cand_paths.append(Path(env_p))
    cand_paths.append(DATA / "proxy-presets.json")
    for p in cand_paths:
        if p.is_symlink() or not p.is_file():
            continue
        items = _read_safe_json_file(p)
        if items is None:
            continue
        if isinstance(items, list):
            for it in items:
                if not isinstance(it, dict):
                    continue
                pid = str(it.get("id") or "").strip().lower()
                raw_url = it.get("url")
                if not pid or not VALID_PROXY_PRESET_RE.fullmatch(pid):
                    continue
                clean_url = _validate_proxy_url(raw_url)
                if clean_url:
                    out[pid] = clean_url
        break
    return out


FLASH_MODEL_FAMILY = "gemini-3.8-flash"
QUOTA_ERROR_CODES = {
    "rate_limit_error",
    "pool_cooldown",
    "pool-cooldown",
    "pool-monitor-tripped",
    "pool_monitor_tripped",
    "resource_exhausted",
}
EXPLICIT_UPSTREAM_QUOTA_PHRASES = (
    "resource_exhausted",
    "resource has been exhausted",
    "individual quota reached",
    "exhausted your capacity",
    "rate_limit_error",
    "pool_cooldown",
    "pool-cooldown",
    "pool-monitor-tripped",
    "pool_monitor_tripped",
    "too many requests",
)
MAX_VERIFY_DEFERRALS = 3


def verify_defer_backoff_secs(deferral_num):
    """Exponential backoff seconds for verify_deferred (default 15m / 30m / 60m; overridable via env)."""
    raw = os.environ.get("AGY_VERIFY_DEFER_BACKOFF_SECS", "").strip()
    schedule = None
    if raw:
        vals = []
        for part in raw.split(","):
            s = part.strip()
            if not s:
                continue
            try:
                v = int(s)
            except ValueError:
                vals = []
                break
            if v < 0:
                vals = []
                break
            vals.append(v)
        if vals:
            schedule = tuple(vals)
    if not schedule:
        schedule = DEFAULT_VERIFY_DEFER_BACKOFF_SECS
    idx = max(0, int(deferral_num) - 1)
    return schedule[min(idx, len(schedule) - 1)]


def _parse_iso_utc(raw):
    if not isinstance(raw, str) or not raw.strip():
        return None
    try:
        parsed = dt.datetime.fromisoformat(raw.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(dt.timezone.utc)


def _verify_deferred_ready(cand, now_dt=None):
    not_before = _parse_iso_utc(cand.get("verifyNotBefore"))
    if not_before is None:
        return True
    cur = now_dt if now_dt is not None else dt.datetime.now(dt.timezone.utc)
    return cur >= not_before


def is_quota_failure(detail=None, *, http_status=None, err_code=None):
    """True only when a probe failed due to quota/rate-limit/monitor-trip (not HTTP 500 or generic text)."""
    status = http_status
    code = err_code
    if isinstance(detail, ProbeError):
        if status is None and detail.http_status:
            status = detail.http_status
        if code is None and detail.err_code:
            code = detail.err_code
    text = str(detail or "")
    low = text.lower()
    if status is None:
        m = re.search(r"\bhttp\s+(\d{3})\b", low)
        if m:
            status = int(m.group(1))
    if status == 429:
        return True
    code_norm = str(code or "").strip().lower()
    if code_norm in QUOTA_ERROR_CODES:
        return True
    if status == 500:
        return False
    if not code_norm and "{" in text:
        parsed_code, _ = _parse_http_error_body(text[text.find("{"):])
        if parsed_code in QUOTA_ERROR_CODES:
            return True
    return any(p in low for p in EXPLICIT_UPSTREAM_QUOTA_PHRASES)


def gateway_env(name):
    """Read one variable from the running gateway container (same trust path as gateway_key);
    the value is only passed on, never logged."""
    if not re.fullmatch(r"[A-Z0-9_]+", name):
        raise ValueError("bad env name")
    try:
        r = run(["docker", "exec", CONTAINER, "sh", "-c", f'printf "%s" "${{{name}}}"'], timeout=30, check=False)
    except Exception:  # noqa: BLE001
        return ""
    return r.stdout.strip() if r.returncode == 0 else ""


def _is_docker_service_hostname(hostname):
    h = str(hostname or "").strip()
    return bool(h) and not _is_ip_address(h) and h.lower() != "localhost" and "." not in h


def _rewrite_proxy_for_host(proxy_url):
    """Rewrite container-only single-label Docker service hostnames (e.g. resin:2260) for host-side use.
    Preserves real IPs, localhost, and external FQDNs (with dots) unless AGY_RESIN_HOST_FROM_HOST is set."""
    if not proxy_url:
        return proxy_url
    try:
        u = urllib.parse.urlsplit(proxy_url)
        port = u.port
    except ValueError:
        return None
    hostname = u.hostname or ""
    if not hostname or _is_ip_address(hostname) or hostname.lower() == "localhost":
        return proxy_url
    override = os.environ.get("AGY_RESIN_HOST_FROM_HOST", "").strip()
    if override:
        if any(ord(ch) < 0x20 or ord(ch) == 0x7F or ch in ("\t", " ") for ch in override):
            return None
        new_hostport = override
    elif _is_docker_service_hostname(hostname):
        new_hostport = f"127.0.0.1:{port}" if port else "127.0.0.1"
    else:
        return proxy_url
    userinfo = u.netloc.rsplit("@", 1)[0] + "@" if "@" in u.netloc else ""
    rewritten = urllib.parse.urlunsplit((u.scheme, userinfo + new_hostport, u.path, u.query, u.fragment))
    return _validate_proxy_url(rewritten)


def upstream_proxy(from_host=False):
    """Container AGY_UPSTREAM_PROXY (or explicit host env override), matching go-gateway primary fallback.
    When from_host=True, container-sourced Docker service hostnames are rewritten for host-side access."""
    host_val = os.environ.get("AGY_UPSTREAM_PROXY", "").strip()
    if host_val:
        _register_url_secret(host_val)
        return host_val
    val = gateway_env("AGY_UPSTREAM_PROXY")
    if val:
        _register_url_secret(val)
        if from_host:
            rewritten = _rewrite_proxy_for_host(val)
            return rewritten if rewritten is not None else val
    return val


def _extract_resin_from_upstream_proxy(raw_proxy):
    """Mirror go-gateway ResolveAccountProxy fallback: extract token + host from http://user:pass@host:port."""
    if not raw_proxy:
        return {}
    try:
        u = urllib.parse.urlsplit(str(raw_proxy).strip())
    except ValueError:
        return {}
    if not u.password:
        return {}
    tok = urllib.parse.unquote(u.password)
    if not tok:
        return {}
    host = u.netloc.rsplit("@", 1)[-1] if "@" in u.netloc else ""
    if not host or any(ch in host for ch in ("\r", "\n", "\t", " ", "\x00")):
        host = "resin:2260"
    return {"token": tok, "host": host}


def resin_settings():
    """Resin proxy credentials used by the gateway for named proxy presets (fail-closed: empty when absent)."""
    tok = gateway_env("AGY_RESIN_PROXY_TOKEN")
    host = gateway_env("AGY_RESIN_HOST") or "resin:2260"
    if not tok:
        extracted = _extract_resin_from_upstream_proxy(upstream_proxy(from_host=False))
        if extracted:
            tok = extracted["token"]
            if extracted.get("host"):
                host = extracted["host"]
    if tok:
        _register_secret(tok)
        return {"token": tok, "host": host}
    return {}


def resin_proxy_url(preset, acc_id, resin, from_host):
    """Mirror go-gateway ResolveAccountProxy: http://<preset>.<account>:<token>@<resin host>.
    from_host=True rewrites single-label Docker service hostnames (never real IPs or external FQDNs
    unless AGY_RESIN_HOST_FROM_HOST is explicitly set) to loopback or AGY_RESIN_HOST_FROM_HOST."""
    preset_s = str(preset or "").strip().lower()
    acc_s = str(acc_id or "").strip()
    if not VALID_PROXY_PRESET_RE.fullmatch(preset_s) or not VALID_ACCOUNT_ID_RE.fullmatch(acc_s):
        return None
    tok = str((resin or {}).get("token") or "")
    host = str((resin or {}).get("host") or "").strip()
    if not tok or not host or any(ch in tok + host for ch in ("\r", "\n", "\t", " ", "\x00")):
        return None
    _register_secret(tok)
    try:
        parsed = urllib.parse.urlsplit(f"http://{host}")
        hostname = parsed.hostname or ""
        port = str(parsed.port) if parsed.port else "2260"
    except ValueError:
        return None
    if not hostname:
        return None
    if from_host and not _is_ip_address(hostname) and hostname.lower() != "localhost":
        override = os.environ.get("AGY_RESIN_HOST_FROM_HOST", "").strip()
        if override:
            host = override
        elif _is_docker_service_hostname(hostname):
            host = f"127.0.0.1:{port}"
        if any(ch in host for ch in ("\r", "\n", "\t", " ", "\x00")):
            return None
    user = urllib.request.quote(f"{preset_s}.{acc_s}", safe="")
    url = f"http://{user}:{urllib.request.quote(tok, safe='')}@{host}"
    return _validate_proxy_url(url)


def _resolve_account_proxy(acc, default_proxy, presets, resin=None, from_host=True):
    """Mirror go-gateway ResolveAccountProxyChecked (fail-closed for ALL accounts including primary)."""
    preset = str(acc.get("proxyPreset") or "").strip().lower()
    if preset in ("", "direct"):
        if acc.get("primary"):
            if not default_proxy:
                return ""
            clean = _validate_proxy_url(default_proxy)
            if clean:
                _register_url_secret(clean)
            return clean
        return ""
    if preset == "custom":
        clean = _validate_proxy_url(acc.get("customProxy") or "")
        if clean:
            _register_url_secret(clean)
        return clean
    if not VALID_PROXY_PRESET_RE.fullmatch(preset):
        return None
    if preset in presets:
        clean = _validate_proxy_url(presets[preset])
        if clean:
            _register_url_secret(clean)
        return clean
    if resin:
        return resin_proxy_url(preset, str(acc.get("id") or "").strip(), resin, from_host)
    # Mirror Go fallback: if Resin token is unset, fall back to a valid customProxy if configured.
    fallback_custom = _validate_proxy_url(acc.get("customProxy") or "")
    if fallback_custom:
        _register_url_secret(fallback_custom)
        return fallback_custom
    # Named preset we cannot resolve: fail closed for EVERY account (including primary).
    return None


def account_allows_flash(acc):
    allowed = acc.get("allowedModels")
    if allowed is None:
        return True
    if not isinstance(allowed, list):
        return False
    for m in allowed:
        ms = str(m).strip().lower()
        if ms == "*" or ms == FLASH_MODEL_FAMILY or ms.startswith(FLASH_MODEL_FAMILY + "-"):
            return True
    return False


def parse_pool_view(d, now_dt=None):
    """Parse /api/dashboard payload into per-account quota, cooldown, and monitor-trip view."""
    if not isinstance(d, dict):
        return {}
    cur = now_dt if now_dt is not None else dt.datetime.now(dt.timezone.utc)
    pool_stats = ((d.get("poolRouting") or {}).get("poolStats") or {})
    raw_trips = pool_stats.get("monitorTrips") or {}
    raw_budgets = pool_stats.get("monitorBudgets") or {}

    active_trips = set()
    if isinstance(raw_trips, dict):
        for k, until_raw in raw_trips.items():
            aid = str(k or "").strip().lower()
            if not aid:
                continue
            until_dt = _parse_iso_utc(until_raw) if isinstance(until_raw, str) else None
            if until_dt is None or until_dt > cur:
                active_trips.add(aid)

    budgets = {}
    if isinstance(raw_budgets, dict):
        for k, val in raw_budgets.items():
            aid = str(k or "").strip().lower()
            if aid and isinstance(val, dict):
                budgets[aid] = val

    view = {}
    for inst in d.get("instances") or []:
        if not isinstance(inst, dict) or not inst.get("id"):
            continue
        aid = str(inst["id"]).strip().lower()
        health_obj = inst.get("health") if isinstance(inst.get("health"), dict) else {}
        health_state = str(health_obj.get("state") or "").strip().lower()
        budget = budgets.get(aid, {})
        mon_tripped = (
            health_state == "monitor_tripped"
            or bool(health_obj.get("monitorTripped"))
            or aid in active_trips
            or bool(budget.get("tripped"))
        )
        mon_zone = str(budget.get("zone") or health_obj.get("monitorZone") or "").strip().lower()
        if mon_tripped:
            mon_zone = "red"
        fam_cd = health_obj.get("familyCooldowns") if isinstance(health_obj.get("familyCooldowns"), dict) else {}
        gemini_cd = fam_cd.get("gemini")
        in_cooldown = health_state == "cooldown" or (isinstance(gemini_cd, (int, float)) and gemini_cd > 0)

        min_rem = None
        exhausted = False
        for g in ((inst.get("quota") or {}).get("groups") or []):
            if not isinstance(g, dict):
                continue
            gname = str(g.get("name") or "").strip()
            glow = gname.lower()
            if not (glow == "gemini models" or ("gemini" in glow and "argon" not in glow)):
                continue
            for b in g.get("buckets") or []:
                if not isinstance(b, dict) or b.get("window") not in ("5h", "weekly"):
                    continue
                rf_raw = b.get("remainingFraction")
                if not isinstance(rf_raw, (int, float)):
                    continue
                reset_dt = _parse_iso_utc(b.get("resetTime"))
                if reset_dt is not None and reset_dt <= cur:
                    rf = 1.0
                else:
                    rf = max(0.0, float(rf_raw))
                    if rf <= 0.0005:
                        exhausted = True
                min_rem = rf if min_rem is None else min(min_rem, rf)

        view[aid] = {
            "rem5h": min_rem,
            "exhausted": exhausted,
            "health": health_state,
            "monitorTripped": mon_tripped,
            "monitorZone": mon_zone,
            "cooldown": in_cooldown,
        }

    for aid in active_trips | set(budgets.keys()):
        if aid not in view:
            b = budgets.get(aid, {})
            tripped = aid in active_trips or bool(b.get("tripped"))
            zone = "red" if tripped else str(b.get("zone") or "").strip().lower()
            view[aid] = {
                "rem5h": None,
                "exhausted": False,
                "health": "monitor_tripped" if tripped else "",
                "monitorTripped": tripped,
                "monitorZone": zone,
                "cooldown": False,
            }
    return view


def fetch_pool_view():
    """Per-account Gemini quota, cooldown, and monitor state from the live gateway dashboard.
    Returns {} when unavailable (then the static order is used)."""
    try:
        key = gateway_key()
    except Exception:  # noqa: BLE001
        key = ""
    try:
        headers = {"x-api-key": key} if key else {}
        req = urllib.request.Request(f"http://127.0.0.1:{DASH_PORT}/api/dashboard", headers=headers)
        with urllib.request.urlopen(req, timeout=60) as r:
            d = json.loads(r.read())
    except Exception:  # noqa: BLE001
        return {}
    return parse_pool_view(d)


def probe_auth_candidates(pool_view=None, from_host=True, allow_monitor_tripped=False):
    """Ordered (account_id, token_path, proxy) candidates for G3/G4 (strictly fail-closed).

    Multi-account aware: only enabled accounts with a verified non-symlink token inside DATA
    that may serve Flash and whose proxy resolves cleanly. Banned/unauthenticated/disabled
    accounts are skipped; monitor-tripped/red accounts are skipped by default (avoiding 80x
    monitor escalation during G4 tool calls); cooldown/yellow/exhausted accounts are deprioritized;
    unknown quota ranks below verified non-exhausted accounts. Never falls back to an excluded primary."""
    default_proxy = upstream_proxy(from_host=from_host)
    view = pool_view if pool_view is not None else {}
    accounts_file = DATA / "accounts.json"
    if not accounts_file.exists() and not accounts_file.is_symlink():
        if not _is_safe_data_file(TOKEN):
            return []
        if default_proxy:
            clean_def = _validate_proxy_url(default_proxy)
            if clean_def is None:
                return []
            default_proxy = clean_def
        info = view.get("primary", {})
        health = str(info.get("health") or "").strip().lower()
        if health in ("tos_tripped", "auth_error", "unauthenticated", "disabled"):
            return []
        mon_tripped = bool(info.get("monitorTripped")) or health == "monitor_tripped"
        mon_zone = str(info.get("monitorZone") or "").strip().lower()
        if (mon_tripped or mon_zone == "red") and not allow_monitor_tripped:
            return []
        return [("primary", TOKEN, default_proxy)]

    raw = _read_safe_json_file(accounts_file)
    accs = raw.get("accounts") if isinstance(raw, dict) else None
    if not isinstance(accs, list) or not accs:
        return []

    presets = _load_proxy_preset_map()
    needs_resin = any(
        isinstance(a, dict)
        and str(a.get("proxyPreset") or "").strip().lower() not in ("", "direct", "custom")
        and str(a.get("proxyPreset") or "").strip().lower() not in presets
        for a in accs
    )
    resin = resin_settings() if needs_resin else {}
    scored = []
    for order, a in enumerate(accs):
        if not isinstance(a, dict):
            continue
        acc_id = str(a.get("id") or "").strip()
        if not VALID_ACCOUNT_ID_RE.fullmatch(acc_id):
            continue
        is_primary = bool(a.get("primary"))
        enabled = bool(a.get("enabled", True))
        sched = str(a.get("schedState") or ("active" if enabled else "disabled")).strip().lower()
        if not enabled or sched == "disabled" or sched not in ("", "active", "draining"):
            continue
        tok = TOKEN if is_primary else (DATA / "accounts" / acc_id / ".gemini/antigravity-cli/antigravity-oauth-token")
        if not _is_safe_data_file(tok) or not account_allows_flash(a):
            continue
        pxy = _resolve_account_proxy(a, default_proxy, presets, resin, from_host)
        if pxy is None:
            continue
        info = view.get(acc_id.lower(), {})
        health = str(info.get("health") or "").strip().lower()
        if health in ("tos_tripped", "auth_error", "unauthenticated", "disabled"):
            continue
        mon_tripped = bool(info.get("monitorTripped")) or health == "monitor_tripped"
        mon_zone = str(info.get("monitorZone") or "").strip().lower()
        if (mon_tripped or mon_zone == "red") and not allow_monitor_tripped:
            continue
        in_cooldown = bool(info.get("cooldown")) or health == "cooldown"
        rem = info.get("rem5h")
        exhausted = bool(info.get("exhausted")) or (rem is not None and rem <= 0.0005)

        monitor_hard = 1 if (mon_tripped or mon_zone == "red") else 0
        avail_rank = 2 if exhausted else (1 if in_cooldown else 0)
        monitor_yellow = 1 if mon_zone == "yellow" else 0
        sched_rank = 1 if sched == "draining" else 0
        quota_known_rank = (1 if rem is None else 0) if (view and not exhausted) else 0
        rem_key = (-rem if rem is not None else 0.0) if view else 0.0
        scored.append((
            (monitor_hard, avail_rank, monitor_yellow, sched_rank, quota_known_rank, rem_key, 0 if is_primary else 1, order),
            (acc_id, tok, pxy),
        ))
    scored.sort(key=lambda x: x[0])
    return [c for _, c in scored]


def resolve_probe_auth(pool_view=None):
    """First probe candidate as (token_path, proxy) for host-side use (G3, isolated_home)."""
    cands = probe_auth_candidates(pool_view, from_host=True)
    if not cands:
        raise RuntimeError("no eligible Flash probe account available (fail-closed)")
    _, tok, pxy = cands[0]
    return tok, pxy


def docker_network_for(proxy_url):
    """Docker user-defined network shared by the gateway and the proxy host (e.g. resin or Compose alias),
    so the isolated test container can reach it by name. Excludes bridge/host/none and IP/loopback hosts."""
    if not proxy_url:
        return None
    try:
        host = urllib.parse.urlsplit(proxy_url).hostname or ""
    except ValueError:
        return None
    if not host or host.startswith("-") or host.lower() == "localhost" or _is_ip_address(host):
        return None

    def _inspect_networks(name):
        r = run(["docker", "inspect", "--format", "{{json .NetworkSettings.Networks}}", "--", name], timeout=30, check=False)
        if r.returncode != 0:
            return {}
        try:
            data = json.loads(r.stdout or "{}")
            return data if isinstance(data, dict) else {}
        except Exception:  # noqa: BLE001
            return {}

    gw_nets_map = _inspect_networks(CONTAINER)
    gw_nets = sorted(k for k in gw_nets_map.keys() if k not in EXCLUDED_DOCKER_NETWORKS)
    if not gw_nets:
        return None

    # 1. Direct container name match on any shared user-defined network.
    host_nets = {k for k in _inspect_networks(host).keys() if k not in EXCLUDED_DOCKER_NETWORKS}
    shared = sorted(set(gw_nets) & host_nets)
    if shared:
        return shared[0]

    # 2. Inspect each user-defined network attached to CONTAINER to match container names or Compose aliases.
    for net in gw_nets:
        r = run(["docker", "network", "inspect", "--format", "{{json .Containers}}", "--", net], timeout=30, check=False)
        if r.returncode != 0:
            continue
        try:
            containers = json.loads(r.stdout or "{}")
        except Exception:  # noqa: BLE001
            continue
        if not isinstance(containers, dict):
            continue
        cids_to_check = []
        for cid, info in containers.items():
            if not isinstance(info, dict):
                continue
            cname = str(info.get("Name") or "").lstrip("/")
            if cname == host:
                return net
            aliases = info.get("Aliases") or info.get("DNSNames") or []
            if isinstance(aliases, list) and host in aliases:
                return net
            if cname and cname != CONTAINER and not str(cid).startswith("-"):
                cids_to_check.append(str(cid))
        for cid in cids_to_check:
            c_nets = _inspect_networks(cid)
            net_cfg = c_nets.get(net) if isinstance(c_nets, dict) else None
            if isinstance(net_cfg, dict):
                aliases = (net_cfg.get("Aliases") or []) + (net_cfg.get("DNSNames") or [])
                if host in aliases:
                    return net

    # 3. Unambiguous fallback when CONTAINER is attached to exactly one user-defined network.
    if len(gw_nets) == 1:
        return gw_nets[0]
    return None


def isolated_home(stage, token_path=None, name="home"):
    tok = Path(token_path) if token_path is not None else resolve_probe_auth()[0]
    if not _is_safe_data_file(tok):
        raise RuntimeError(f"refusing unsafe token path: {tok}")
    home = stage / name
    app = home / "antigravity-cli"
    app.mkdir(parents=True, exist_ok=True, mode=0o700)
    link = app / tok.name
    if link.is_symlink() or link.exists():
        link.unlink()
    link.symlink_to(tok)
    for rel in ("installation_id", "cache/onboarding.json"):
        src = tok.parent / rel
        dst = app / rel
        if _is_safe_data_file(src):
            dst.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            if dst.is_symlink() or dst.exists():
                dst.unlink()
            dst.symlink_to(src)
    return home


_G3_PROXY_AUTH_HINTS = (
    "proxy", "407", "401", "403", "unauthenticated", "unauthorized", "auth",
    "oauth", "token", "invalid_grant", "connection refused", "econnrefused",
    "econnreset", "timed out", "timeout", "network", "dns", "lookup", "tls",
    "ssl", "eof", "unreachable", "502", "503", "504", "429",
    "resource_exhausted", "rate_limit",
)


def _is_g3_proxy_or_auth_failure(rep, r):
    checks = rep.get("checks") if isinstance(rep, dict) else None
    init_failed = False
    new_failed = False
    early_details = []
    if isinstance(checks, list) and checks:
        for c in checks:
            if not isinstance(c, dict) or c.get("ok"):
                continue
            cname = str(c.get("check") or "")
            if cname == "initialize":
                init_failed = True
                early_details.append(str(c.get("detail") or ""))
            elif cname == "session/new":
                new_failed = True
                early_details.append(str(c.get("detail") or ""))
        if not init_failed and not new_failed:
            return False
    combined = " ".join(early_details + [str(r.stdout or ""), str(r.stderr or "")]).lower()
    return any(h in combined for h in _G3_PROXY_AUTH_HINTS) or any(d.strip() in ("", "null") for d in early_details)


def contract_g3(stage, pool_view=None):
    cands = probe_auth_candidates(pool_view, from_host=True, allow_monitor_tripped=True)
    if not cands:
        raise RuntimeError("no eligible Flash probe account available (fail-closed)")
    tried = []
    report_path = stage / "contract.json"
    for idx, (acc_id, tok, upstream_proxy) in enumerate(cands):
        home_name = f"home-{idx}"
        home_dir = stage / home_name
        with contextlib.suppress(FileNotFoundError):
            report_path.unlink()
        env = dict(os.environ, AGY_UPSTREAM_PROXY=upstream_proxy)
        try:
            r = run(
                [sys.executable, HERE / "scripts/agy_acp_smoke.py", stage / "agy-acp-900k",
                 "--gemini-dir", isolated_home(stage, tok, name=home_name),
                 "--cwd", "/tmp", "--report", report_path],
                timeout=420, check=False, env=env,
            )
            rep = json.loads(report_path.read_text()) if report_path.exists() else {}
        finally:
            shutil.rmtree(home_dir, ignore_errors=True)
        failed = [c["check"] for c in rep.get("checks", []) if not c.get("ok")]
        if r.returncode == 0 and rep.get("ok"):
            extra = rep.get("info", {}).get("new_status_templates") or []
            note = f"{len(rep['checks'])} checks, 0 generations"
            if extra:
                note += f"; {len(extra)} new status text(s) to review"
            if tried:
                note += f" via {acc_id} (skipped {', '.join(tried)})"
            return note
        err_summary = redact_secrets(", ".join(failed) or (r.stderr or r.stdout)[-300:])
        if _is_g3_proxy_or_auth_failure(rep, r) and idx + 1 < len(cands):
            tried.append(f"{acc_id}: proxy/auth")
            log(f"G3 account {acc_id} hit proxy/auth error ({err_summary}), routing to the next account")
            continue
        raise RuntimeError(f"contract failed [{acc_id}]: {err_summary}")
    raise RuntimeError("contract failed on all candidates: " + ", ".join(tried))


@contextlib.contextmanager
def test_container(agy_binary, stage, auth=None, data_dir=None):
    """Isolated copy of the production image with the candidate agy, its own /data and the
    probing account's login token mounted read-only. Loopback port only. Proxy credentials are
    passed through a 0600 O_EXCL|O_NOFOLLOW env file, never on the command line."""
    if auth is None:
        cands = probe_auth_candidates(from_host=False)
        if not cands:
            raise RuntimeError("no eligible Flash probe account available (fail-closed)")
        _, tok, upstream_proxy = cands[0]
    else:
        tok, upstream_proxy = auth
    tok = Path(tok)
    if not _is_safe_data_file(tok):
        raise RuntimeError(f"refusing unsafe token path: {tok}")
    if upstream_proxy:
        clean_proxy = _validate_proxy_url(upstream_proxy)
        if clean_proxy is None:
            raise ValueError("invalid upstream proxy URL")
        upstream_proxy = clean_proxy
        _register_url_secret(upstream_proxy)

    if data_dir is not None:
        data = Path(data_dir)
        if data.is_symlink():
            raise RuntimeError("refusing symlink testdata dir")
        try:
            os.mkdir(data, 0o700)
        except FileExistsError:
            data = Path(tempfile.mkdtemp(prefix=f"{data.name}-", dir=stage))
    else:
        data = Path(tempfile.mkdtemp(prefix="testdata-", dir=stage))

    try:
        (data / ".gemini/antigravity-cli/cache").mkdir(parents=True, exist_ok=True)
        for p in [data, *data.rglob("*")]:
            if p.is_symlink():
                raise RuntimeError(f"refusing symlink in testdata: {p}")
            if os.geteuid() == 0:
                os.lchown(p, GATEWAY_UID, GATEWAY_UID)
        key = "upgrade-test-" + secrets.token_hex(16)
        _register_secret(key)
        extra_docker_args = []
        env_lines = [f"AGY_API_KEYS={key}"]
        if upstream_proxy:
            env_lines.append(f"AGY_UPSTREAM_PROXY={upstream_proxy}")
            net = docker_network_for(upstream_proxy)
            if net:
                extra_docker_args += ["--network", net]
        for rel in ("installation_id", "cache/onboarding.json"):
            src = tok.parent / rel
            if _is_safe_data_file(src):
                extra_docker_args += ["-v", f"{src}:/data/.gemini/antigravity-cli/{rel}:ro"]

        env_file = stage / "test.env"
        created_env = False
        started_container = False
        try:
            try:
                fd = os.open(env_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
                created_env = True
                try:
                    os.fchmod(fd, 0o600)
                    with os.fdopen(fd, "w", encoding="utf-8") as f:
                        fd = -1
                        f.write("\n".join(env_lines) + "\n")
                finally:
                    if fd >= 0:
                        os.close(fd)
                run(["docker", "rm", "-f", TEST_CONTAINER], check=False)
                started_container = True
                run(["docker", "run", "-d", "--name", TEST_CONTAINER, "--memory", "1500m", "-p", f"127.0.0.1:{TEST_PORT}:18890",
                     "--env-file", env_file, "-e", "AGY_ACP_BIN=/cand/agy-acp-900k", "-e", "AGY_QUOTA_BIN=/cand/agy-acp-900k",
                     "-e", "AGY_MAX_LIVE_WORKERS=2", *extra_docker_args,
                     "-v", f"{agy_binary}:/cand/agy-acp-900k:ro", "-v", f"{data}:/data",
                     "-v", f"{tok}:/data/.gemini/antigravity-cli/antigravity-oauth-token:ro", IMAGE])
            finally:
                if created_env:
                    with contextlib.suppress(FileNotFoundError):
                        env_file.unlink()
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
            if started_container:
                run(["docker", "rm", "-f", TEST_CONTAINER], check=False)
    finally:
        shutil.rmtree(data, ignore_errors=True)


def _live_g4_on(stage, agy_binary, auth, attempt_idx=None):
    """Flash/low only: plain reply, tool round trip (2 generations), no status text leaks.
    Returns structured (ok, http_status, err_code, detail)."""
    idx = attempt_idx if attempt_idx is not None else _G4_ATTEMPT_IDX.get()
    data_dir = (Path(stage) / f"testdata-{idx}") if idx is not None else None
    with test_container(agy_binary, stage, auth, data_dir=data_dir) as (base, key):
        probe_res = flash_probe(base, key, "g4")
        if len(probe_res) == 4:
            ok, http_status, err_code, detail = probe_res
        else:
            ok, detail = probe_res
            http_status, err_code = (200 if ok else 0), ""
        if not ok:
            return False, http_status, err_code, "text probe: " + detail
        tools = [{"name": "Bash", "description": "Run a bash command",
                  "input_schema": {"type": "object", "properties": {"command": {"type": "string"}}, "required": ["command"]}}]
        sid = f"upgrader-tool-{time.time_ns()}"
        msgs = [{"role": "user", "content": "Use the Bash tool to run exactly: echo UPGRADE_OK . Then reply with the output only."}]
        body = {"model": "gemini-3.8-flash-low", "max_tokens": 512, "system": "Primary working directory: /tmp", "tools": tools}
        try:
            s1, d1 = http_json(base + "/v1/messages", {**body, "messages": msgs}, key, session=sid)
        except urllib.error.HTTPError as exc:
            ec, eb = _read_http_error(exc)
            return False, exc.code, ec, f"tool step 1: HTTP {exc.code} {eb[:240]!r}"
        except Exception as exc:  # noqa: BLE001
            return False, 0, "", f"tool step 1: {type(exc).__name__}: {str(exc)[:180]}"
        tu = [c for c in (d1.get("content", []) if isinstance(d1, dict) else []) if isinstance(c, dict) and c.get("type") == "tool_use"]
        if not isinstance(d1, dict) or d1.get("stop_reason") != "tool_use" or not tu:
            return False, s1, "", f"tool step 1: stop={d1.get('stop_reason') if isinstance(d1, dict) else None}"
        msgs += [{"role": "assistant", "content": d1["content"]},
                 {"role": "user", "content": [{"type": "tool_result", "tool_use_id": tu[0]["id"], "content": "UPGRADE_OK"}]}]
        try:
            s2, d2 = http_json(base + "/v1/messages", {**body, "messages": msgs}, key, session=sid)
        except urllib.error.HTTPError as exc:
            ec, eb = _read_http_error(exc)
            return False, exc.code, ec, f"tool step 2: HTTP {exc.code} {eb[:240]!r}"
        except Exception as exc:  # noqa: BLE001
            return False, 0, "", f"tool step 2: {type(exc).__name__}: {str(exc)[:180]}"
        text = "".join(x.get("text", "") for x in (d2.get("content", []) if isinstance(d2, dict) else []) if isinstance(x, dict) and x.get("type") == "text")
        if not isinstance(d2, dict) or d2.get("stop_reason") != "end_turn" or "UPGRADE_OK" not in text or "⚠" in text:
            return False, s2, "", f"tool step 2: stop={d2.get('stop_reason') if isinstance(d2, dict) else None} text={text[:60]!r}"
    return True, 200, "", detail


def live_g4(stage, agy_binary, pool_view=None):
    """Run the live gate on the best account; when an account is out of quota, route to the next one.
    Only a quota-independent failure (or every account quota-limited) fails the gate."""
    view = fetch_pool_view() if pool_view is None else pool_view
    cands = probe_auth_candidates(view, from_host=False, allow_monitor_tripped=False)
    if not cands:
        raise RuntimeError("no eligible Flash probe account available (fail-closed)")
    tried = []
    for idx, (acc_id, tok, pxy) in enumerate(cands):
        tok_ctx = _G4_ATTEMPT_IDX.set(idx)
        try:
            res = _live_g4_on(stage, agy_binary, (tok, pxy))
        except Exception as exc:  # noqa: BLE001
            if is_quota_failure(exc):
                tried.append(f"{acc_id}: quota")
                log(f"G4 account {acc_id} is quota-limited, routing to the next account")
                continue
            raise RuntimeError(redact_secrets(f"[{acc_id}] {exc}")) from exc
        finally:
            _G4_ATTEMPT_IDX.reset(tok_ctx)
        if isinstance(res, tuple) and len(res) == 4:
            ok, http_status, err_code, detail = res
        else:
            ok, http_status, err_code, detail = True, 200, "", str(res)
        if not ok:
            if is_quota_failure(detail, http_status=http_status, err_code=err_code):
                tried.append(f"{acc_id}: quota")
                log(f"G4 account {acc_id} is quota-limited, routing to the next account")
                continue
            raise RuntimeError(redact_secrets(f"[{acc_id}] {detail}"))
        via = f" via {acc_id}" + (f" (skipped {', '.join(tried)})" if tried else "")
        return f"text {detail}; tool round trip ok; 3 Flash generations (budget ${LIVE_BUDGET_USD}){via}"
    raise RuntimeError("all accounts quota-limited: " + ", ".join(tried or ["no eligible account"]))


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
    if isinstance(cand, dict) and cand.get("state") in ("testing", "ready", "approved", "applying", "waiting_idle", "verify_deferred"):
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
    cand.pop("verifyNotBefore", None)
    prev = cand.get("previous")
    prev_dir = (AGY_DIR / prev) if prev else None
    if prev and prev_dir and not prev_dir.is_symlink() and (prev_dir / "agy-acp-900k").is_file():
        switch_current(prev_dir)
        cand["state"] = "rolled_back"
        cand["message"] = f"切换后验证失败, 已切回 {prev}: {reason}"
    else:
        cand["state"] = "failed"
        cand["message"] = f"切换后验证失败, 无法回滚 (无可用旧版本): {reason}"
    log(cand["message"])


def cmd_check(args):
    st = load_state()
    blocked = blocked_agy_versions()
    if reconcile_agy_candidate(st, blocked=blocked, log_changes=True):
        save_state(st)
        prune_candidates(st)
    cand = st.get("candidate")
    if cand and cand.get("state") in ("waiting_idle", "verify_deferred"):
        if cand.get("state") == "verify_deferred" and not _verify_deferred_ready(cand):
            log(f"candidate {cand['id']} verify_deferred until {cand.get('verifyNotBefore')}; skipping probe")
            save_state(st)
            prune_candidates(st)
            return 0
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
        st.pop("lastCheckNote", None)
        log(f"gateway candidate {cid}: {detail}")
        save_state(st)
        prune_candidates(st)
        return 0
    rel = latest_release()
    tag = _normalize_agy_version(rel["tag_name"])
    blocked_reason = blocked_agy_reason(tag, blocked)
    if blocked_reason:
        note = f"skipping blocked agy {tag}: {blocked_reason}"
        log(note)
        st["lastCheckNote"] = note
        if (
            cand
            and cand.get("kind") == "agy"
            and cand.get("state") != "applied"
            and blocked_agy_reason(cand.get("version"), blocked)
        ):
            st["candidate"] = None
            st["gates"] = []
        save_state(st)
        prune_candidates(st)
        return 0
    cur = current_versions().get("agy", "0")
    if semver(tag) <= semver(cur) and not args.force:
        log(f"agy {cur} is current (latest {tag})")
        st.pop("lastCheckNote", None)
        if cand and cand.get("state") == "failed" and cand.get("version") != tag:
            st["candidate"] = None
        save_state(st)
        prune_candidates(st)
        return 0
    st.pop("lastCheckNote", None)
    stage = STATE / "candidates" / f"agy-{tag}-{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%SZ}"
    stage.mkdir(parents=True, mode=0o700)
    gates = []
    st["candidate"] = {"id": stage.name, "kind": "agy", "version": tag, "sha256": "", "state": "testing", "createdAt": now(), "stage": str(stage)}
    st["gates"] = gates
    st["costUsd"] = 0
    save_state(st)
    pool_view = fetch_pool_view()
    steps = [("G1 完整性", lambda: download_g1(rel, stage)), ("G2 补丁", lambda: patch_g2(stage)),
             ("G3 ACP 契约", lambda: contract_g3(stage, pool_view)),
             ("G4 真实验收", lambda: live_g4(stage, stage / "agy-acp-900k", pool_view))]
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
    if PUBLIC.is_symlink():
        log("refusing symlink upgrade dir during approval scan")
        return None
    approvals_dir = PUBLIC / "approvals"
    if approvals_dir.is_symlink() or not approvals_dir.is_dir():
        return None
    matched = None
    matched_at = None
    now_utc = dt.datetime.now(dt.timezone.utc)
    used = STATE / "approvals"
    used.mkdir(parents=True, exist_ok=True, mode=0o700)
    for idx, p in enumerate(sorted(approvals_dir.glob("*.json"))):
        if p.is_symlink():
            log(f"ignored symlink approval {p.name}")
            with contextlib.suppress(OSError):
                p.unlink()
            continue
        raw = None
        fd = -1
        try:
            fd = os.open(p, os.O_RDONLY | os.O_NOFOLLOW)
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise OSError("approval is not a regular file")
            with os.fdopen(fd, "rb") as f:
                fd = -1
                raw = f.read(65536).decode("utf-8", "replace")
        except OSError as exc:
            if fd >= 0:
                os.close(fd)
            log(f"ignored unreadable approval {p.name}: {exc}")
            with contextlib.suppress(OSError):
                p.unlink()
            continue
        dest = used / f"{int(time.time() * 1000)}-{idx}-{p.name}"
        try:
            if p.is_symlink():
                p.unlink()
                continue
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
    if AGY_DIR.is_symlink():
        raise RuntimeError("refusing symlink data/agy dir")
    tmp = AGY_DIR / f".current-{os.getpid()}-{secrets.token_hex(4)}"
    with contextlib.suppress(FileNotFoundError):
        tmp.unlink()
    tmp.symlink_to(target_dir.name)
    os.replace(tmp, AGY_DIR / "current")


def engine_info(key):
    """Live engine state from the gateway (engine.json is only written when an engine starts)."""
    try:
        headers = {"x-api-key": key} if key else {}
        req = urllib.request.Request(f"http://127.0.0.1:{DASH_PORT}/api/upgrade", headers=headers)
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read()).get("engine") or {}
    except Exception:
        return {}


def apply_agy(st, cand):
    blocked_reason = blocked_agy_reason(cand.get("version"))
    if blocked_reason:
        ver = _normalize_agy_version(cand.get("version"))
        raise RuntimeError(
            f"refusing to apply blocked agy {ver}: {blocked_reason} "
            '(set AGY_BLOCKED_VERSIONS="" or "none" to override)'
        )
    stage = Path(cand["stage"])
    src = stage / "agy-acp-900k"
    if sha256(src) != cand["sha256"]:
        raise RuntimeError("candidate binary changed after testing")
    if AGY_DIR.is_symlink():
        raise RuntimeError("refusing symlink data/agy dir")
    prev = current_agy_dir()
    dest = AGY_DIR / f"{cand['version']}-{cand['sha256'][:8]}"
    if dest.is_symlink():
        raise RuntimeError("refusing symlink install target")
    dest.mkdir(parents=True, exist_ok=True)
    os.chmod(dest, 0o755)
    tmp_bin = dest / f".agy-acp-900k.{os.getpid()}.{secrets.token_hex(4)}.tmp"
    fd = os.open(tmp_bin, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o755)
    try:
        os.fchmod(fd, 0o755)
        with src.open("rb") as rf, os.fdopen(fd, "wb") as wf:
            fd = -1
            shutil.copyfileobj(rf, wf)
        os.replace(tmp_bin, dest / "agy-acp-900k")
    except BaseException:
        if fd >= 0:
            os.close(fd)
        with contextlib.suppress(OSError):
            tmp_bin.unlink()
        raise
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
    check run finishes the job. A quota-limited probe defers with exponential backoff; a genuine
    failure switches back."""
    key = gateway_key()
    deadline = time.time() + wait
    eng = engine_info(key)
    while cand["installed"] not in (eng.get("running") or "") and eng.get("alive") is True and time.time() < deadline:
        time.sleep(10)
        eng = engine_info(key)
    if cand["installed"] not in (eng.get("running") or "") and eng.get("alive") is True:
        cand["message"] = "软链已切换, 网关仍有进行中的请求, 空闲后自动切换 (下次检查时验证)"
        log(cand["message"])
        return True
    try:
        probe_res = flash_probe(f"http://127.0.0.1:{GW_PORT}", key, "apply")
        if isinstance(probe_res, tuple) and len(probe_res) == 4:
            ok, http_status, err_code, detail = probe_res
        else:
            ok, detail = probe_res
            http_status, err_code = (200 if ok else None), ""
    except urllib.error.HTTPError as exc:
        err_code, err_body = _read_http_error(exc)
        ok, http_status, detail = False, exc.code, f"HTTP {exc.code} {err_body[:240]!r}"
    except Exception as exc:  # noqa: BLE001
        ok, http_status, err_code, detail = False, 0, "", f"{type(exc).__name__}: {str(exc)[:180]}"
    eng = engine_info(key)
    running_new = cand["installed"] in (eng.get("running") or "")
    idle_on_new = (not eng.get("alive")) and (cand["installed"] in (eng.get("current") or ""))
    if ok and running_new:
        cand["state"] = "applied"
        cand["message"] = f"agy {cand['version']} 已生效; 探针 {detail}"
        cand.pop("verifyDeferrals", None)
        cand.pop("verifyNotBefore", None)
        log(cand["message"])
        return True
    if not ok and is_quota_failure(detail, http_status=http_status, err_code=err_code) and (running_new or idle_on_new):
        n = int(cand.get("verifyDeferrals") or 0) + 1
        if n <= MAX_VERIFY_DEFERRALS:
            # The pool is out of quota: the probe says nothing about the binary (it already passed G4).
            backoff_sec = verify_defer_backoff_secs(n)
            not_before = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=backoff_sec)).isoformat(timespec="seconds")
            cand["verifyDeferrals"] = n
            cand["verifyNotBefore"] = not_before
            cand["state"] = "verify_deferred"
            cand["message"] = (
                f"切换后验证因号池额度不足暂缓 ({n}/{MAX_VERIFY_DEFERRALS}), "
                f"将于 {not_before} 后重试: {detail}"
            )
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
    (STATE / f"release-{rel.name}.log").write_text(redact_secrets(r.stdout + r.stderr))
    result = (rel / "result").read_text().strip() if (rel / "result").exists() else ""
    if r.returncode == 0 and result == "DEPLOYED_AND_VERIFIED":
        cand["state"] = "applied"
        cand["message"] = "网关已更新并通过上线检查"
        log(f"gateway {rel.name} applied")
        return True
    if r.returncode == 0 and result == "DEPLOYED_QUOTA_DEFERRED":
        cand["state"] = "applied"
        cand["message"] = "网关已更新 (健康与接口检查通过; 实时 Flash 探针因号池额度耗尽暂缓)"
        log(f"gateway {rel.name} applied (live smoke deferred on pool 429)")
        return True
    cand["state"] = "rolled_back" if result == "ROLLED_BACK" else "failed"
    cand["message"] = f"发布脚本失败 ({r.returncode}, {result or 'no result'})" + (", 已自动回滚" if result == "ROLLED_BACK" else ", 未改动生产")
    log(f"gateway {rel.name}: {cand['message']}")
    return False


def cmd_apply(args):
    st = load_state()
    cand = st.get("candidate")
    if cand and cand.get("state") in ("waiting_idle", "verify_deferred"):
        take_approval({"id": None})  # discard stale approvals
        if cand.get("kind") == "agy" and not _candidate_matches_current_agy(cand, current_versions(), current_agy_dir()):
            cur_dir = current_agy_dir()
            log(
                f"clearing stale {cand['state']} candidate {cand['id']}: "
                f"current symlink changed externally to {cur_dir.name if cur_dir else 'unknown'}"
            )
            st["candidate"] = None
            st["gates"] = []
            save_state(st)
            prune_candidates(st)
            return 0
        if cand.get("state") == "verify_deferred" and not _verify_deferred_ready(cand):
            log(f"candidate {cand['id']} verify_deferred until {cand.get('verifyNotBefore')}; skipping probe")
            return 0
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
    if cand.get("kind") == "agy":
        blocked_reason = blocked_agy_reason(cand.get("version"))
        if blocked_reason:
            ver = _normalize_agy_version(cand.get("version"))
            msg = (
                f"refusing to apply blocked agy {ver}: {blocked_reason} "
                '(set AGY_BLOCKED_VERSIONS="" or "none" to override)'
            )
            log(msg)
            cand["state"] = "failed"
            cand["message"] = msg
            st["lastCheckNote"] = f"blocked agy {ver}: {blocked_reason}"
            record_history(st, cand)
            save_state(st)
            prune_candidates(st)
            return 1
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


def _handle_sigterm(signum, _frame):
    raise SystemExit(128 + signum)


def install_signal_handlers():
    signal.signal(signal.SIGTERM, _handle_sigterm)


def main():
    install_signal_handlers()
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("check")
    c.add_argument("--force", action="store_true", help="test even when the latest release is not newer")
    sub.add_parser("apply")
    sub.add_parser("status")
    args = ap.parse_args()
    if args.cmd == "status":
        print(json.dumps(current_state(), ensure_ascii=False, indent=2))
        return 0
    with lock():
        return {"check": cmd_check, "apply": cmd_apply}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
