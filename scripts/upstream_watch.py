#!/usr/bin/env python3
"""Track stable releases and optionally prepare a verified candidate, never deploy it."""
import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import tarfile
import urllib.request

from agy_upgrade import ROOT, STATE, build_candidate, checked_path, sha, source_digest, upgrade_lock, version, write_json

OFFICIAL = "google-antigravity/antigravity-cli"
UPDATER_MANIFEST_BASE = "https://antigravity-cli-auto-updater-974169037036.us-central1.run.app/manifests"
GCS_PREFIX = "https://storage.googleapis.com/antigravity-public/antigravity-cli/"
MAX_DOWNLOAD_BYTES = 512 * 1024 * 1024
PLATFORM_SPECS = {
    ("Linux", "x86_64"): ("linux_amd64", ("agy_cli_linux_x64.tar.gz", "cli_linux_x64.tar.gz")),
    ("Linux", "amd64"): ("linux_amd64", ("agy_cli_linux_x64.tar.gz", "cli_linux_x64.tar.gz")),
    ("Linux", "aarch64"): ("linux_arm64", ("agy_cli_linux_arm64.tar.gz", "cli_linux_arm64.tar.gz")),
    ("Linux", "arm64"): ("linux_arm64", ("agy_cli_linux_arm64.tar.gz", "cli_linux_arm64.tar.gz")),
    ("Darwin", "arm64"): ("darwin_arm64", ("agy_cli_darwin_arm64.tar.gz", "agy_cli_mac_arm64.tar.gz", "cli_mac_arm64.tar.gz")),
    ("Darwin", "aarch64"): ("darwin_arm64", ("agy_cli_darwin_arm64.tar.gz", "agy_cli_mac_arm64.tar.gz", "cli_mac_arm64.tar.gz")),
}
# Pruned dead upstream repositories (kept out of REFERENCES to avoid noisy 404 snapshots):
# - tochemey/atto (GitHub 404 since 2026-11-09)
REFERENCES = ["lili-luo/dsh-plugin-bundle", "dannndouglas/dsh-antigravity-acp", "chaojimct/cli-agent-gateway",
              "router-for-me/CLIProxyAPI", "agentclientprotocol/agent-client-protocol", "coder/acp-go-sdk",
              "modelcontextprotocol/go-sdk", "agentclientprotocol/claude-agent-acp", "NoeFabris/opencode-antigravity-auth"]


def semver(v):
    return tuple(int(x) for x in re.findall(r"\d+", str(v))[:3])


def sha512(path):
    with Path(path).open("rb") as f:
        return hashlib.file_digest(f, "sha512").hexdigest()


def is_trusted_gcs_tarball(url):
    return bool(re.fullmatch(re.escape(GCS_PREFIX) + r"[A-Za-z0-9._/-]+\.tar\.gz", str(url))) and ".." not in str(url).split("/")


def resolve_platform(system=None, machine=None):
    sys_name = system if system is not None else platform.system()
    arch_raw = (machine if machine is not None else platform.machine()).lower()
    spec = PLATFORM_SPECS.get((sys_name, arch_raw))
    if spec is None:
        raise ValueError(f"unsupported updater platform: {sys_name}/{arch_raw}")
    return spec


def get_json(url):
    request = urllib.request.Request(url, headers={"User-Agent": "agy-acp-gateway-upstream-watch", "Accept": "application/vnd.github+json"})
    with urllib.request.urlopen(request, timeout=30) as response:
        content = response.read(4*1024*1024+1)
    if len(content) > 4*1024*1024:
        raise ValueError("metadata response exceeds size limit")
    return json.loads(content)


def fetch_github_release():
    release = get_json(f"https://api.github.com/repos/{OFFICIAL}/releases/latest")
    if release.get("draft") or release.get("prerelease") or not re.fullmatch(r"\d+\.\d+\.\d+", release.get("tag_name", "")):
        raise ValueError("official release is not a stable semantic version")
    return release


def fetch_manifest_release(manifest_key=None):
    if manifest_key is None:
        try:
            manifest_key, _ = resolve_platform()
        except ValueError:
            manifest_key = "linux_amd64"
    manifest = get_json(f"{UPDATER_MANIFEST_BASE}/{manifest_key}.json")
    if not isinstance(manifest, dict):
        raise ValueError("updater manifest must be a JSON object")
    ver = str(manifest.get("version", "")).strip()
    url = str(manifest.get("url", "")).strip()
    digest = str(manifest.get("sha512", "")).strip().lower()
    if not re.fullmatch(r"\d+\.\d+\.\d+", ver):
        raise ValueError("updater manifest version is not a stable semantic version")
    if not is_trusted_gcs_tarball(url) or not re.fullmatch(r"[0-9a-f]{128}", digest):
        raise ValueError("updater manifest lacks a trusted GCS URL or SHA-512")
    return {
        "source": "manifest",
        "manifest_key": manifest_key,
        "tag_name": ver,
        "published_at": str(manifest.get("timestamp") or manifest.get("published_at") or ""),
        "html_url": url,
        "url": url,
        "sha512": digest,
    }


def latest_official():
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
    raise ValueError(f"official release discovery failed: github={gh_err}; manifest={mf_err}")


def select_asset(release):
    _, candidate_names = resolve_platform()
    assets = [x for x in release.get("assets", []) if x.get("name") in candidate_names]
    if len(assets) != 1:
        raise ValueError("expected one official platform asset")
    asset = assets[0]
    digest = asset.get("digest", "")
    prefix = f"https://github.com/{OFFICIAL}/releases/download/{release['tag_name']}/"
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest) or not asset.get("browser_download_url", "").startswith(prefix):
        raise ValueError("official asset lacks a trusted SHA256 or URL")
    return asset


def _stream_download(url, tmp_path):
    request = urllib.request.Request(url, headers={"User-Agent": "agy-acp-gateway-upstream-watch"})
    total = 0
    with urllib.request.urlopen(request, timeout=90) as response, tmp_path.open("wb") as output:
        while chunk := response.read(1024 * 1024):
            total += len(chunk)
            if total > MAX_DOWNLOAD_BYTES:
                raise ValueError("download exceeds size limit")
            output.write(chunk)
    return total


def _extract_verified_binary(archive, directory, expected_tag):
    binary = directory / "agy"
    tmp = directory / "agy.extracting"
    tmp.unlink(missing_ok=True)
    try:
        with tarfile.open(archive, "r:gz") as tar:
            matches = [x for x in tar.getmembers() if x.isfile() and Path(x.name).name in ("agy", "antigravity")]
            if len(matches) != 1 or matches[0].size > MAX_DOWNLOAD_BYTES:
                raise ValueError("unexpected official archive layout")
            with tar.extractfile(matches[0]) as source, tmp.open("wb") as dest:
                while chunk := source.read(1024 * 1024):
                    dest.write(chunk)
        tmp.chmod(0o755)
        if version(tmp) != expected_tag:
            raise ValueError("downloaded executable reports unexpected version")
        os.replace(tmp, binary)
    finally:
        tmp.unlink(missing_ok=True)
    return binary


def download_official(release):
    if release.get("source") != "manifest":
        try:
            asset = select_asset(release)
        except ValueError:
            manifest_key, _ = resolve_platform()
            mf = fetch_manifest_release(manifest_key)
            if mf["tag_name"] != release.get("tag_name"):
                raise
            release = mf
            asset = None
    else:
        asset = None

    directory = checked_path(STATE / "downloads" / release["tag_name"])
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)

    if release.get("source") == "manifest":
        url = str(release.get("url", ""))
        expected512 = str(release.get("sha512", "")).lower()
        if not is_trusted_gcs_tarball(url) or not re.fullmatch(r"[0-9a-f]{128}", expected512):
            raise ValueError("manifest release lacks a trusted GCS URL or SHA-512")
        asset_name = Path(url).name or f"agy_cli_{release.get('manifest_key', 'official')}.tar.gz"
        archive = directory / asset_name
        if not archive.exists() or sha512(archive) != expected512:
            tmp = archive.with_suffix(".download")
            tmp.unlink(missing_ok=True)
            try:
                _stream_download(url, tmp)
                if sha512(tmp) != expected512:
                    raise ValueError("manifest asset SHA-512 mismatch")
                os.replace(tmp, archive)
            finally:
                tmp.unlink(missing_ok=True)
        binary = _extract_verified_binary(archive, directory, release["tag_name"])
        write_json(directory / "source.json", {
            "source": "manifest",
            "version": release["tag_name"],
            "asset": asset_name,
            "archive_sha512": expected512,
            "archive_sha256": sha(archive),
            "binary_sha256": sha(binary),
            "release_url": release.get("html_url", url),
        })
        return binary

    archive = directory / asset["name"]
    expected = asset["digest"].split(":", 1)[1]
    if not archive.exists() or sha(archive) != expected:
        tmp = archive.with_suffix(".download")
        tmp.unlink(missing_ok=True)
        try:
            total = _stream_download(asset["browser_download_url"], tmp)
            if total != asset.get("size") or sha(tmp) != expected:
                raise ValueError("official asset digest or size mismatch")
            os.replace(tmp, archive)
        finally:
            tmp.unlink(missing_ok=True)
    binary = _extract_verified_binary(archive, directory, release["tag_name"])
    write_json(directory / "source.json", {
        "source": "github",
        "version": release["tag_name"],
        "asset": asset["name"],
        "archive_sha256": expected,
        "binary_sha256": sha(binary),
        "release_url": release["html_url"],
    })
    return binary


def _attempt_digest(release):
    if release.get("source") == "manifest":
        return "sha512:" + str(release["sha512"]).lower()
    return select_asset(release)["digest"]


def snapshot(release):
    data = {"checked_at": dt.datetime.now(dt.timezone.utc).isoformat(), "official": {
        "version": release["tag_name"], "published_at": release.get("published_at", ""), "url": release.get("html_url", "")}, "references": []}
    for repo in REFERENCES:
        item = {"repo": repo}
        for kind, suffix in [("release", "releases/latest"), ("commit", "commits?per_page=1")]:
            try:
                response = get_json(f"https://api.github.com/repos/{repo}/{suffix}")
                if kind == "release":
                    item[kind] = {k: response.get(k) for k in ("tag_name", "published_at", "html_url")}
                else:
                    item[kind] = {"sha": response[0]["sha"], "date": response[0]["commit"]["committer"]["date"]}
            except Exception as exc:
                item[kind] = {"error_type": type(exc).__name__, "status": getattr(exc, "code", None)}
        data["references"].append(item)
    return data


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--download", action="store_true", help="download and verify the official stable asset only")
    ap.add_argument("--prepare", action="store_true", help="prepare changed stable versions with fake/ACP checks, zero prompts")
    ap.add_argument("--official-only", action="store_true", help="skip reference repository metadata")
    args = ap.parse_args()
    with upgrade_lock():
        release = latest_official()
        data = {"official": {"version": release["tag_name"], "published_at": release.get("published_at", "")}} if args.official_only else snapshot(release)
        write_json(STATE/"upstream-latest.json", data)
        if args.download:
            binary = download_official(release)
            print(json.dumps({"version": release["tag_name"], "binary": str(binary), "sha256": sha(binary)}))
        elif args.prepare and version(ROOT/"bin/agy-acp-900k") != release["tag_name"]:
            digest = _attempt_digest(release)
            key = hashlib.sha256((digest+source_digest()).encode()).hexdigest()
            attempt = STATE/"attempts"/(key+".json")
            if attempt.exists():
                print(json.dumps({"status": "already_attempted", "report": str(attempt)}))
                return 0
            write_json(attempt, {"status": "started", "version": release["tag_name"]})
            try:
                binary = download_official(release)
                stage = build_candidate(binary, contract=True, live=False)
                write_json(attempt, {"status": "candidate_ready_for_live_acceptance", "candidate": str(stage), "generation_requests": 0})
            except Exception as exc:
                write_json(attempt, {"status": "failed", "error_type": type(exc).__name__, "automatic_retry": False})
                raise
        else:
            print(json.dumps({"status": "checked", "latest": release["tag_name"], "generation_requests": 0}))
    return 0


if __name__ == "__main__":
    os.umask(0o077)
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(json.dumps({"ok": False, "error_type": type(exc).__name__}))
        raise SystemExit(1)
