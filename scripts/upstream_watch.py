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
REFERENCES = ["lili-luo/dsh-plugin-bundle", "dannndouglas/dsh-antigravity-acp", "chaojimct/cli-agent-gateway",
              "router-for-me/CLIProxyAPI", "agentclientprotocol/agent-client-protocol", "coder/acp-go-sdk",
              "modelcontextprotocol/go-sdk", "agentclientprotocol/claude-agent-acp", "NoeFabris/opencode-antigravity-auth", "tochemey/atto"]


def get_json(url):
    request = urllib.request.Request(url, headers={"User-Agent": "agy-acp-gateway-upstream-watch", "Accept": "application/vnd.github+json"})
    with urllib.request.urlopen(request, timeout=30) as response:
        content = response.read(4*1024*1024+1)
    if len(content) > 4*1024*1024:
        raise ValueError("metadata response exceeds size limit")
    return json.loads(content)


def latest_official():
    release = get_json(f"https://api.github.com/repos/{OFFICIAL}/releases/latest")
    if release.get("draft") or release.get("prerelease") or not re.fullmatch(r"\d+\.\d+\.\d+", release.get("tag_name", "")):
        raise ValueError("official release is not a stable semantic version")
    return release


def select_asset(release):
    if platform.system() != "Linux":
        raise ValueError("this updater targets the Linux gateway")
    arch = {"x86_64": "x64", "aarch64": "arm64"}.get(platform.machine())
    name = f"agy_cli_linux_{arch}.tar.gz"
    assets = [x for x in release.get("assets", []) if x.get("name") == name]
    if len(assets) != 1:
        raise ValueError("expected one official platform asset")
    asset = assets[0]
    digest = asset.get("digest", "")
    prefix = f"https://github.com/{OFFICIAL}/releases/download/{release['tag_name']}/"
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest) or not asset.get("browser_download_url", "").startswith(prefix):
        raise ValueError("official asset lacks a trusted SHA256 or URL")
    return asset


def download_official(release):
    asset = select_asset(release)
    directory = checked_path(STATE/"downloads"/release["tag_name"])
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    archive = directory/asset["name"]
    expected = asset["digest"].split(":", 1)[1]
    if not archive.exists() or sha(archive) != expected:
        tmp = archive.with_suffix(".download")
        request = urllib.request.Request(asset["browser_download_url"], headers={"User-Agent": "agy-acp-gateway-upstream-watch"})
        total = 0
        with urllib.request.urlopen(request, timeout=60) as response, tmp.open("wb") as output:
            while chunk := response.read(1024*1024):
                total += len(chunk)
                if total > 512*1024*1024:
                    raise ValueError("download exceeds size limit")
                output.write(chunk)
        if total != asset.get("size") or sha(tmp) != expected:
            raise ValueError("official asset digest or size mismatch")
        os.replace(tmp, archive)
    binary = directory/"agy"
    with tarfile.open(archive, "r:gz") as tar:
        matches = [x for x in tar.getmembers() if x.isfile() and Path(x.name).name in ("agy", "antigravity")]
        if len(matches) != 1 or matches[0].size > 512*1024*1024:
            raise ValueError("unexpected official archive layout")
        tmp = directory/"agy.extracting"
        with tar.extractfile(matches[0]) as source, tmp.open("wb") as dest:
            while chunk := source.read(1024*1024):
                dest.write(chunk)
        tmp.chmod(0o755)
        if version(tmp) != release["tag_name"]:
            raise ValueError("downloaded executable reports unexpected version")
        os.replace(tmp, binary)
    write_json(directory/"source.json", {"version": release["tag_name"], "asset": asset["name"],
                                         "archive_sha256": expected, "binary_sha256": sha(binary),
                                         "release_url": release["html_url"]})
    return binary


def snapshot(release):
    data = {"checked_at": dt.datetime.now(dt.timezone.utc).isoformat(), "official": {
        "version": release["tag_name"], "published_at": release["published_at"], "url": release["html_url"]}, "references": []}
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
        data = {"official": {"version": release["tag_name"], "published_at": release["published_at"]}} if args.official_only else snapshot(release)
        write_json(STATE/"upstream-latest.json", data)
        if args.download:
            binary = download_official(release)
            print(json.dumps({"version": release["tag_name"], "binary": str(binary), "sha256": sha(binary)}))
        elif args.prepare and version(ROOT/"bin/agy-acp-900k") != release["tag_name"]:
            asset = select_asset(release)
            key = hashlib.sha256((asset["digest"]+source_digest()).encode()).hexdigest()
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
