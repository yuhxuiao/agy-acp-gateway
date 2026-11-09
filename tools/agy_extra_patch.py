#!/usr/bin/env python3
"""Gateway-specific extra patches for the agy binary, applied AFTER the upstream
context-unlock patcher (third_party/agy-context-unlock/agy-patch.py).

Only what upstream does not cover and the gateway still needs lives here.
Every patch is located by Go function name (via the upstream pclntab parser)
and guarded by byte signatures: if the function does not look exactly like a
known vanilla or patched layout, nothing is written (fail closed).

Patches
-------
disable-internal-checkpoint  checkpoint.shouldCheckpoint -> return (false, nil)
    agy's own auto-compaction would silently summarise/replace the session
    history behind the gateway's back. The gateway delegates compaction to the
    host client (Claude Code /compact), so agy must never checkpoint itself.
    Same behaviour as the hand-made 1.5.1 patch (verified by disassembly).

Dropped on purpose (verified on 1.5.3, see docs/research-sdk-upstream-banrisk-20261109.md):
    * mcp_config.json -> acp_nomcp_.json string patch: agy acp reads
      <gemini-dir>/config/mcp_config.json, not $HOME/.gemini/config, so the
      gateway's private gemini-dir is enough.
    * buildCascadeConfigInternal forced 1M: superseded by upstream Gate 3A,
      which uses each model's real budget (1M models -> 983,040).

Usage:
    agy_extra_patch.py --binary FILE --dry-run
    agy_extra_patch.py --binary FILE --in-place
    agy_extra_patch.py --binary FILE --check      # exit 0 = all applied, 1 = vanilla, 2 = error
"""

import argparse
import hashlib
import importlib.util
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
UPSTREAM = os.path.normpath(os.path.join(HERE, "..", "third_party", "agy-context-unlock", "agy-patch.py"))

SPEC_VERSION = 1

PATCHES = [
    {
        "id": "disable-internal-checkpoint",
        "func": "checkpoint.shouldCheckpoint",
        "x86-64": {
            # Go ABIInternal prologue: cmp 0x10(%r14),%rsp  (stack-growth check)
            "vanilla_prefix": bytes.fromhex("49 3b 66 10"),
            # a native "return false, nil" site proves the (bool, error) signature:
            # xor eax,eax ; xor ebx,ebx ; xor ecx,ecx ; leave ; ret
            "signature_in_body": bytes.fromhex("31 c0 31 db 31 c9 c9 c3"),
            # xor eax,eax ; xor ebx,ebx ; xor ecx,ecx ; ret   (no frame set up yet)
            "patched": bytes.fromhex("31 c0 31 db 31 c9 c3"),
        },
    },
]


class ExtraPatchError(Exception):
    pass


def load_upstream():
    if not os.path.exists(UPSTREAM):
        raise ExtraPatchError(f"upstream patcher not found: {UPSTREAM}")
    spec = importlib.util.spec_from_file_location("agy_context_unlock", UPSTREAM)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def inspect(data, up):
    text_va, text_off, arch = up.find_text_section(data)
    funcs = up.build_functions(bytes(data))
    results = []
    for p in PATCHES:
        layout = p.get(arch)
        if layout is None:
            raise ExtraPatchError(f"{p['id']}: no layout for arch {arch}")
        if p["func"] not in funcs:
            raise ExtraPatchError(f"{p['id']}: symbol not found: {p['func']}")
        entryoff, size = funcs[p["func"]]
        off = text_off + entryoff
        body = bytes(data[off:off + size])
        if body.startswith(layout["patched"]):
            state = "patched"
        elif body.startswith(layout["vanilla_prefix"]) and layout["signature_in_body"] in body:
            state = "vanilla"
        else:
            raise ExtraPatchError(
                f"{p['id']}: {p['func']} @ {text_va + entryoff:#x} has an unknown layout "
                f"(entry {body[:8].hex(' ')}); refusing to patch")
        results.append((p, layout, off, text_va + entryoff, state))
    return arch, results


def write_atomic(path, data):
    mode = os.stat(path).st_mode & 0o7777
    tmp = f"{path}.tmp.{os.getpid()}"
    try:
        with open(tmp, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.chmod(tmp, mode or 0o755)
        os.replace(tmp, path)
    except Exception:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def main():
    ap = argparse.ArgumentParser(description="agy gateway extra patches (after upstream context unlock)")
    ap.add_argument("--binary", required=True)
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--in-place", action="store_true")
    mode.add_argument("--check", action="store_true")
    args = ap.parse_args()

    try:
        up = load_upstream()
        with open(args.binary, "rb") as fh:
            data = bytearray(fh.read())
        before = hashlib.sha256(data).hexdigest()
        arch, results = inspect(data, up)
    except (ExtraPatchError, OSError) as exc:
        print(f"error: {exc}")
        return 2
    except Exception as exc:  # upstream PatchError and friends
        print(f"error: {type(exc).__name__}: {exc}")
        return 2

    print(f"[i] extra-patch spec v{SPEC_VERSION} arch={arch} binary={args.binary}")
    pending = [r for r in results if r[4] == "vanilla"]
    for p, layout, off, va, state in results:
        print(f"[i] {p['id']}: {p['func']} @ {va:#x} state={state}")

    if args.check:
        return 0 if not pending else 1

    if not pending:
        print("[=] extra patches already applied; nothing to write")
        return 0

    for p, layout, off, va, state in pending:
        data[off:off + len(layout["patched"])] = layout["patched"]
        print(f"[*] {p['id']}: entry {va:#x} -> {layout['patched'].hex(' ')}")

    # re-verify on the modified image before writing anything
    try:
        _, again = inspect(data, up)
    except ExtraPatchError as exc:
        print(f"error: post-patch verification failed: {exc}")
        return 2
    if any(r[4] != "patched" for r in again):
        print("error: post-patch verification failed")
        return 2

    after = hashlib.sha256(data).hexdigest()
    if args.dry_run:
        print(f"[*] dry run complete: sha256 {before[:12]} -> {after[:12]}")
        return 0
    write_atomic(args.binary, data)
    print(f"[*] written: sha256 {before[:12]} -> {after[:12]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
