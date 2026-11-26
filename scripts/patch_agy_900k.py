#!/usr/bin/env python3
"""
Verify Antigravity CLI binary (`bin/agy-acp-900k`) patch status.

Note: Legacy raw byte-sequence patching is retired; candidate binaries MUST be
built and verified through `scripts/agy-upgrade.sh` (which runs both
`third_party/agy-context-unlock/agy-patch.py` and `tools/agy_extra_patch.py`).
"""
import hashlib
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_DST = os.path.join(ROOT, "bin", "agy-acp-900k")
EXTRA_PATCHER = os.path.join(ROOT, "tools", "agy_extra_patch.py")

# Legacy 1.4.x/1.5.0 x86_64 immediate 900000 (0x000dbba0) fallback check
PATCH_SEQ = bytes.fromhex("a0bb0d00")


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else list(argv)
    mode = argv[0] if argv else "--verify"
    if mode != "--verify":
        print(
            "error: legacy direct copy/patch mode in patch_agy_900k.py is retired (would produce an unpatched copy); "
            "use scripts/agy-upgrade.sh (or third_party/agy-context-unlock/agy-patch.py + tools/agy_extra_patch.py).",
            file=sys.stderr,
        )
        return 2

    dst = argv[1] if len(argv) > 1 else DEFAULT_DST
    if not os.path.exists(dst):
        print(f"MISSING: {dst}")
        return 1
    check = subprocess.run([sys.executable, EXTRA_PATCHER, "--binary", dst, "--check"], capture_output=True, text=True)
    extra_ok = check.returncode == 0
    with open(dst, "rb") as f:
        has_legacy_seq = PATCH_SEQ in f.read()
    ok = extra_ok or has_legacy_seq
    print(f"File: {dst}")
    print(f"SHA256: {sha256_file(dst)}")
    print(f"Extra patches verified: {extra_ok}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
