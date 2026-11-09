#!/usr/bin/env python3
"""
Patch or verify Antigravity CLI binary (`agy`) into `bin/agy-acp-900k`.

Background:
Official `agy` triggers internal context checkpointing at 200,000 tokens (`0x00030d40`)
in `GetContextCheckpointThreshold`. For the ACP gateway (where the external client
manages context compaction), we raise that threshold to 900,000 tokens (`0x000dbba0`)
on a standalone copy `bin/agy-acp-900k` without modifying `~/.local/bin/agy`.
"""
import hashlib
import os
import shutil
import stat
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_SRC = os.path.expanduser("~/.local/bin/agy")
DEFAULT_DST = os.path.join(ROOT, "bin", "agy-acp-900k")

# x86_64 immediate 200000 (0x00030d40) -> 900000 (0x000dbba0) in GetContextCheckpointThreshold
ORIG_SEQ = bytes.fromhex("400d0300")
PATCH_SEQ = bytes.fromhex("a0bb0d00")


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> int:
    mode = sys.argv[1] if len(sys.argv) > 1 else "--verify"
    dst = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_DST
    if mode == "--verify":
        if not os.path.exists(dst):
            print(f"MISSING: {dst}")
            return 1
        data = open(dst, "rb").read()
        has_patch = PATCH_SEQ in data
        print(f"File: {dst}")
        print(f"SHA256: {sha256_file(dst)}")
        print(f"900K threshold sequence present: {has_patch}")
        return 0 if has_patch else 1

    src = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_SRC
    out = sys.argv[3] if len(sys.argv) > 3 else DEFAULT_DST
    if not os.path.exists(src):
        print(f"Source binary not found: {src}", file=sys.stderr)
        return 1
    shutil.copy2(src, out + ".tmp")
    with open(out + ".tmp", "rb") as f:
        buf = bytearray(f.read())
    cnt = buf.count(PATCH_SEQ)
    print(f"Copied {src} -> {out} (existing 900K sequences: {cnt})")
    os.chmod(out + ".tmp", os.stat(out + ".tmp").st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    os.replace(out + ".tmp", out)
    print(f"Done. SHA256={sha256_file(out)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
