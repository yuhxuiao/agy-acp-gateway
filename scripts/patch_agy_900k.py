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
CONTEXT_PATCHER = os.path.join(ROOT, "third_party", "agy-context-unlock", "agy-patch.py")
EXTRA_PATCHER = os.path.join(ROOT, "tools", "agy_extra_patch.py")
ALL_UNLOCKED_MARKER = "[=] all gates already unlocked; nothing to write"


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def verify_acp_context_patches(dst: str) -> tuple[bool, str]:
    """Require `agy-patch.py --acp-only --dry-run` to report all active gates patched/native."""
    res = subprocess.run(
        [sys.executable, CONTEXT_PATCHER, "--binary", dst, "--acp-only", "--dry-run", "--no-env-fix"],
        capture_output=True,
        text=True,
    )
    out = res.stdout or ""
    err = res.stderr or ""
    lines = [line.strip() for line in out.splitlines()]
    has_pending = any(line.startswith("[*] ") for line in lines)
    unlocked = ALL_UNLOCKED_MARKER in lines
    ok = (res.returncode == 0) and unlocked and (not has_pending)
    detail = (err or out).strip()
    return ok, detail


def verify_extra_patches(dst: str) -> tuple[bool, str]:
    """Require `agy_extra_patch.py --check` to exit 0 (all extra patches patched/native)."""
    res = subprocess.run(
        [sys.executable, EXTRA_PATCHER, "--binary", dst, "--check"],
        capture_output=True,
        text=True,
    )
    ok = res.returncode == 0
    detail = (res.stderr or res.stdout or "").strip()
    return ok, detail


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else list(argv)
    mode = argv[0] if argv else "--verify"
    if mode != "--verify":
        print(
            "error: legacy direct copy/patch mode in patch_agy_900k.py is retired (would produce an unpatched copy); "
            "use scripts/agy-upgrade.sh (or third_party/agy-context-unlock/agy-patch.py --acp-only + tools/agy_extra_patch.py).",
            file=sys.stderr,
        )
        return 2

    dst = argv[1] if len(argv) > 1 else DEFAULT_DST
    if not os.path.exists(dst):
        print(f"MISSING: {dst}")
        return 1
    context_ok, context_detail = verify_acp_context_patches(dst)
    extra_ok, extra_detail = verify_extra_patches(dst)
    ok = context_ok and extra_ok
    print(f"File: {dst}")
    print(f"SHA256: {sha256_file(dst)}")
    print(f"ACP context patches verified: {context_ok}")
    print(f"Extra patches verified: {extra_ok}")
    if not context_ok and context_detail:
        print(f"ACP context check detail:\n{context_detail}", file=sys.stderr)
    if not extra_ok and extra_detail:
        print(f"Extra patch check detail:\n{extra_detail}", file=sys.stderr)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
