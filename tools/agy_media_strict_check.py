#!/usr/bin/env python3
"""Offline G5a binary contract check for ws-media strict-permission enforcement.

Verifies (with ZERO network / Google API quota!) that a candidate `agy` binary
still enforces `"toolPermission": "strict"` synchronously BEFORE any hardcoded
or default allow grants for `read_file`, so future `agy` releases can never
silently break `ws-media`'s pre-open `session/request_permission` gate.

Supports x86-64 and arm64 binaries (ELF, Mach-O, PE) by reusing the upstream Go
pclntab / container parser in `third_party/agy-context-unlock/agy-patch.py`.

Verified invariants (fail-closed):
  1. `permission_grants_for`:
     - `backend.PermissionGrantsFor` exists and directly references both
       `"read_file(/)"` and `"write_file(/)"` string literals in `.rodata`.
     - `backend.(*ServerBackend).populatePermissionConfig` directly calls
       `backend.PermissionGrantsFor`.
  2. `apply_settings_strict`:
     - `server.(*Server).applySettings` exists and handles `"strict"`.
     - `server.(*settingsProvider).PermissionGrants` exists, handles `"strict"`,
       and directly calls `backend.PermissionGrantsFor`.
  3. `check_single_target_order`:
     - `permissions.(*permissionManager).checkSingleTargetOnce`,
       `permissions.(*permissionManager).lookupEffectiveGrant`, and
       `permissions.getHardcodedGrants` all exist.
     - Inside `checkSingleTargetOnce`, EVERY direct call to
       `lookupEffectiveGrant` occurs at a strictly lower address (BEFORE) any
       fallback grant lookup:
         * `agy <= 1.5.16` (`direct_hardcoded`): direct call to
           `permissions.getHardcodedGrants`.
         * `agy >= 1.5.18` (`grant_store_defaults`):
           `permissions.NewPermissionManagerWithConversationGrants` calls
           `permissions.getHardcodedGrants` before `permissions.NewPermissionGrantStore`,
           `lookupEffectiveGrant` calls `permissions.(*PermissionGrantStore).LookupEffective`,
           and `checkSingleTargetOnce` calls `lookupEffectiveGrant` strictly
           before `permissions.(*PermissionGrantStore).LookupDefault`.
       Additionally, if `permissions.(*permissionManager).isFilePathAllowed` is
       called inside `checkSingleTargetOnce`, `lookupEffectiveGrant` must also
       precede it.
  4. `read_file_required_permissions`:
     - `tools.(*ReadFileTool).RequiredPermissions` exists (non-stub), references
       `"read_file"`, and enforces symlink resolution via `fs.ResolveSymlink`
       (either directly or via `tools.fileRequiredPermissions`).
  5. `acp_permission_prompt_path`:
     - `permissions.(*permissionManager).EnsurePermissions` exists.
     - `server.(*Server).resolveToolPermission` exists and directly calls
       `server.(*Server).requestPermissionFromClient`.

Usage:
    agy_media_strict_check.py --binary FILE [--report FILE] [--version VER]
Exit codes:
    0 = all invariants passed (`ok: true`)
    1 = one or more invariants failed (`ok: false`)
    2 = binary could not be read or parsed (`ok: false`)
"""

import argparse
import importlib.util
import json
import os
from pathlib import Path
import struct
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
UPSTREAM = os.path.normpath(os.path.join(HERE, "..", "third_party", "agy-context-unlock", "agy-patch.py"))

SPEC_VERSION = 1

SYM_PERMISSION_GRANTS_FOR = "backend.PermissionGrantsFor"
SYM_POPULATE_PERMISSION_CONFIG = "backend.(*ServerBackend).populatePermissionConfig"
SYM_APPLY_SETTINGS = "server.(*Server).applySettings"
SYM_SETTINGS_PERMISSION_GRANTS = "server.(*settingsProvider).PermissionGrants"
SYM_CHECK_SINGLE_TARGET_ONCE = "permissions.(*permissionManager).checkSingleTargetOnce"
SYM_LOOKUP_EFFECTIVE_GRANT = "permissions.(*permissionManager).lookupEffectiveGrant"
SYM_GET_HARDCODED_GRANTS = "permissions.getHardcodedGrants"
SYM_NEW_PM_WITH_CONV_GRANTS = "permissions.NewPermissionManagerWithConversationGrants"
SYM_NEW_GRANT_STORE = "permissions.NewPermissionGrantStore"
SYM_STORE_LOOKUP_EFFECTIVE = "permissions.(*PermissionGrantStore).LookupEffective"
SYM_STORE_LOOKUP_DEFAULT = "permissions.(*PermissionGrantStore).LookupDefault"
SYM_IS_FILE_PATH_ALLOWED = "permissions.(*permissionManager).isFilePathAllowed"
SYM_READ_FILE_REQUIRED_PERMS = "tools.(*ReadFileTool).RequiredPermissions"
SYM_FILE_REQUIRED_PERMS = "tools.fileRequiredPermissions"
SYM_RESOLVE_SYMLINK = "fs.ResolveSymlink"
SYM_ENSURE_PERMISSIONS = "permissions.(*permissionManager).EnsurePermissions"
SYM_RESOLVE_TOOL_PERMISSION = "server.(*Server).resolveToolPermission"
SYM_REQUEST_PERMISSION_FROM_CLIENT = "server.(*Server).requestPermissionFromClient"

REQUIRED_STRICT_GRANTS = (b"read_file(/)", b"write_file(/)")


class MediaStrictCheckError(Exception):
    pass


def load_upstream():
    if not os.path.exists(UPSTREAM):
        raise MediaStrictCheckError(f"upstream patcher not found: {UPSTREAM}")
    spec = importlib.util.spec_from_file_location("agy_context_unlock_g5", UPSTREAM)
    if spec is None or spec.loader is None:
        raise MediaStrictCheckError(f"cannot load upstream patcher: {UPSTREAM}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _build_elf_va_segments(data):
    if len(data) < 64 or data[4] != 2 or data[5] != 1:
        raise MediaStrictCheckError("only 64-bit little-endian ELF is supported")
    phoff = struct.unpack_from("<Q", data, 32)[0]
    phentsize, phnum = struct.unpack_from("<HH", data, 54)
    segs = []
    for i in range(phnum):
        ph = phoff + i * phentsize
        if ph + 40 > len(data):
            break
        if struct.unpack_from("<I", data, ph)[0] != 1:  # PT_LOAD
            continue
        p_offset, p_vaddr, _p_paddr, p_filesz = struct.unpack_from("<QQQQ", data, ph + 8)
        if p_filesz > 0:
            segs.append((p_vaddr, p_vaddr + p_filesz, p_offset))
    return segs


def _build_macho_va_segments(data):
    if len(data) < 32:
        raise MediaStrictCheckError("truncated Mach-O header")
    ncmds = struct.unpack_from("<I", data, 16)[0]
    off = 32
    segs = []
    for _ in range(ncmds):
        if off + 8 > len(data):
            break
        cmd, size = struct.unpack_from("<II", data, off)
        if size < 8 or off + size > len(data):
            break
        if cmd == 0x19 and size >= 56:  # LC_SEGMENT_64
            vmaddr, _vmsize, fileoff, filesize = struct.unpack_from("<QQQQ", data, off + 24)
            if filesize > 0:
                segs.append((vmaddr, vmaddr + filesize, fileoff))
        off += size
    return segs


def _build_pe_va_segments(data):
    if len(data) < 0x40:
        raise MediaStrictCheckError("truncated PE header")
    pe = struct.unpack_from("<I", data, 0x3C)[0]
    if pe + 24 > len(data) or data[pe:pe + 4] != b"PE\0\0":
        raise MediaStrictCheckError("invalid PE header signature")
    nsec = struct.unpack_from("<H", data, pe + 6)[0]
    opt_size = struct.unpack_from("<H", data, pe + 20)[0]
    opt = pe + 24
    if opt + 32 > len(data):
        raise MediaStrictCheckError("truncated PE optional header")
    image_base = struct.unpack_from("<Q", data, opt + 24)[0]
    sect = opt + opt_size
    segs = []
    for _ in range(nsec):
        if sect + 40 > len(data):
            break
        _vsize, vaddr, rawsize, rawptr = struct.unpack_from("<IIII", data, sect + 8)
        if rawsize > 0:
            segs.append((image_base + vaddr, image_base + vaddr + rawsize, rawptr))
        sect += 40
    return segs


def build_va_to_off(data, text_va, text_off):
    """Return callable mapping virtual address -> file offset (or None if unmapped)."""
    if data[:4] == b"\x7fELF":
        segs = _build_elf_va_segments(data)
    elif data[:4] == b"\xcf\xfa\xed\xfe":
        segs = _build_macho_va_segments(data)
    elif data[:2] == b"MZ":
        segs = _build_pe_va_segments(data)
    else:
        # Synthetic flat buffer fallback (reachable only when find_text_section is mocked in unit tests).
        n = len(data)

        def flat_map(va):
            off = text_off + (va - text_va)
            return off if 0 <= off < n else None

        return flat_map

    n = len(data)

    def seg_map(va):
        for v_start, v_end, f_off in segs:
            if v_start <= va < v_end:
                off = f_off + (va - v_start)
                return off if 0 <= off < n else None
        return None

    return seg_map


def find_direct_call_offsets(data, text_va, text_off, arch, caller_entryoff, caller_size, target_va):
    """Return sorted byte offsets within `caller` that directly call/tail-jump to `target_va`."""
    caller_off = text_off + caller_entryoff
    caller_va = text_va + caller_entryoff
    if caller_off < 0 or caller_size <= 0 or caller_off + caller_size > len(data):
        return []
    body = data[caller_off:caller_off + caller_size]
    hits = []
    if arch == "x86-64":
        limit = len(body) - 5
        for opcode in (b"\xe8", b"\xe9"):
            pos = 0
            while pos <= limit:
                idx = body.find(opcode, pos)
                if idx == -1 or idx > limit:
                    break
                rel32 = struct.unpack_from("<i", body, idx + 1)[0]
                if caller_va + idx + 5 + rel32 == target_va:
                    hits.append(idx)
                pos = idx + 1
        return sorted(set(hits))
    if arch == "arm64":
        for i in range(0, len(body) - 3, 4):
            w = struct.unpack_from("<I", body, i)[0]
            if (w & 0xFC000000) not in (0x94000000, 0x14000000):
                continue
            imm26 = w & 0x3FFFFFF
            if imm26 & 0x2000000:
                imm26 -= 0x4000000
            if caller_va + i + imm26 * 4 == target_va:
                hits.append(i)
        return hits
    return []


def find_referenced_strings(data, va_to_off, text_va, text_off, arch, fn_entryoff, fn_size, needles):
    """Find instructions inside `fn` that compute the VA of any byte literal in `needles`."""
    found = {needle: [] for needle in needles}
    fn_off = text_off + fn_entryoff
    fn_va = text_va + fn_entryoff
    if fn_off < 0 or fn_size <= 0 or fn_off + fn_size > len(data):
        return found
    body = data[fn_off:fn_off + fn_size]
    max_len = max((len(n) for n in needles), default=0)

    def _record_if_match(instr_va, target_va):
        toff = va_to_off(target_va)
        if toff is None or toff < 0 or toff >= len(data):
            return
        chunk = data[toff:toff + max_len]
        for needle in needles:
            if chunk.startswith(needle):
                found[needle].append(instr_va)

    if arch == "x86-64":
        # Match 64-bit RIP-relative LEA: REX(0x48|0x4C) 0x8D ModRM(mod=00,rm=101) disp32
        limit = len(body) - 7
        i = 0
        while i <= limit:
            b0 = body[i]
            if (b0 == 0x48 or b0 == 0x4C) and body[i + 1] == 0x8D and (body[i + 2] & 0xC7) == 0x05:
                disp32 = struct.unpack_from("<i", body, i + 3)[0]
                target_va = fn_va + i + 7 + disp32
                _record_if_match(fn_va + i, target_va)
            i += 1
        return found

    if arch == "arm64":
        # Track ADRP page bases per register and pair with ADD Xd, Xn, #imm12 (within 8 insns).
        adrp_pages = {}
        for i in range(0, len(body) - 3, 4):
            w = struct.unpack_from("<I", body, i)[0]
            pc = fn_va + i
            # ADRP Xd, imm21 (op=1, bits[28:24]=10000)
            if (w & 0x9F000000) == 0x90000000:
                rd = w & 0x1F
                immlo = (w >> 29) & 0x3
                immhi = (w >> 5) & 0x7FFFF
                imm21 = (immhi << 2) | immlo
                if imm21 & (1 << 20):
                    imm21 -= 1 << 21
                page = (pc & ~0xFFF) + (imm21 << 12)
                adrp_pages[rd] = (i, page)
                # Page-aligned string literal (imm12 == 0, compiler may omit ADD)
                _record_if_match(pc, page)
                continue
            # ADD Xd, Xn, #imm12 (64-bit, sh=0)
            if (w & 0xFFC00000) == 0x91000000:
                rd = w & 0x1F
                rn = (w >> 5) & 0x1F
                imm12 = (w >> 10) & 0xFFF
                base = adrp_pages.get(rn)
                if base is not None and (i - base[0]) <= 32:
                    _record_if_match(pc, base[1] + imm12)
                if rd != rn:
                    adrp_pages.pop(rd, None)
                continue
            # BL clears caller-saved adrp tracking
            if (w & 0xFC000000) == 0x94000000:
                adrp_pages.clear()
        return found

    return found


def _has_inline_strict_compare(body, arch):
    """Detect compiler-inlined comparison against 6-byte ASCII literal 'strict' ('stri' + 'ct')."""
    if arch == "x86-64":
        # Go compiles `s == "strict"` as length==6 + cmpl $0x69727473 ("stri") + cmpw $0x7463 ("ct").
        pos = 0
        while True:
            idx = body.find(b"stri", pos)
            if idx < 0:
                return False
            window = body[max(0, idx - 24):min(len(body), idx + 32)]
            if b"ct" in window:
                return True
            pos = idx + 1
    if arch == "arm64":
        # Match movz/movk materializing 0x69727473 ("stri") and movz 0x7463 ("ct") nearby.
        stri_offs = []
        ct_offs = []
        n = len(body)
        for i in range(0, n - 3, 4):
            w0 = struct.unpack_from("<I", body, i)[0]
            # movz wR/xR, #imm16 (hw=0)
            if (w0 & 0x7FE00000) == 0x52800000:
                imm16 = (w0 >> 5) & 0xFFFF
                rd = w0 & 0x1F
                if imm16 == 0x7463:  # "ct"
                    ct_offs.append(i)
                elif imm16 == 0x7473 and i + 4 < n:  # "st" followed by movk #0x6972, lsl #16 ("ri")
                    for j in range(i + 4, min(n - 3, i + 16), 4):
                        w1 = struct.unpack_from("<I", body, j)[0]
                        if (w1 & 0x7FE00000) == 0x72A00000 and (w1 & 0x1F) == rd and ((w1 >> 5) & 0xFFFF) == 0x6972:
                            stri_offs.append(i)
                            break
        return any(abs(s - c) <= 48 for s in stri_offs for c in ct_offs)
    return False


def _func_handles_strict(data, va_to_off, text_va, text_off, arch, fn_entryoff, fn_size):
    evidence = []
    refs = find_referenced_strings(data, va_to_off, text_va, text_off, arch, fn_entryoff, fn_size, (b"strict",))
    if refs[b"strict"]:
        evidence.append(f"rodata_ref@{refs[b'strict'][0]:#x}")
    fn_off = text_off + fn_entryoff
    if 0 <= fn_off and fn_off + fn_size <= len(data):
        if _has_inline_strict_compare(data[fn_off:fn_off + fn_size], arch):
            evidence.append("inline_cmp:strict")
    return bool(evidence), evidence


def inspect_binary_data(data, up=None):
    """Run all G5a strict-permission invariants against binary bytes.

    Returns dict `{ok, spec_version, arch, checks, errors}` (strictly fail-closed).
    """
    if up is None:
        up = load_upstream()
    raw = bytes(data) if not isinstance(data, (bytes, bytearray)) else data
    text_va, text_off, arch = up.find_text_section(raw)
    if arch not in ("x86-64", "arm64"):
        raise MediaStrictCheckError(f"unsupported architecture: {arch}")
    funcs = up.build_functions(bytes(raw))
    va_to_off = build_va_to_off(raw, text_va, text_off)

    checks = {}
    errors = []

    def fail(name, detail, extra=None):
        entry = {"ok": False, "detail": detail}
        if extra:
            entry.update(extra)
        checks[name] = entry
        errors.append(f"{name}: {detail}")

    def ok(name, detail, extra=None):
        entry = {"ok": True, "detail": detail}
        if extra:
            entry.update(extra)
        checks[name] = entry

    def calls(caller_sym, callee_sym):
        if caller_sym not in funcs or callee_sym not in funcs:
            return []
        c_eo, c_sz = funcs[caller_sym]
        t_eo, _ = funcs[callee_sym]
        return find_direct_call_offsets(raw, text_va, text_off, arch, c_eo, c_sz, text_va + t_eo)

    # 1. backend.PermissionGrantsFor constructs "read_file(/)" and "write_file(/)" and is wired into populatePermissionConfig.
    if SYM_PERMISSION_GRANTS_FOR not in funcs:
        fail("permission_grants_for", f"missing symbol: {SYM_PERMISSION_GRANTS_FOR}")
    else:
        pg_eo, pg_sz = funcs[SYM_PERMISSION_GRANTS_FOR]
        refs = find_referenced_strings(
            raw, va_to_off, text_va, text_off, arch, pg_eo, pg_sz, REQUIRED_STRICT_GRANTS
        )
        missing_lits = [lit.decode("ascii") for lit in REQUIRED_STRICT_GRANTS if not refs[lit]]
        pop_calls = calls(SYM_POPULATE_PERMISSION_CONFIG, SYM_PERMISSION_GRANTS_FOR)
        if missing_lits:
            fail(
                "permission_grants_for",
                f"{SYM_PERMISSION_GRANTS_FOR} missing string literal reference(s): {', '.join(missing_lits)}",
                {"symbol": SYM_PERMISSION_GRANTS_FOR, "va": hex(text_va + pg_eo)},
            )
        elif SYM_POPULATE_PERMISSION_CONFIG not in funcs:
            fail(
                "permission_grants_for",
                f"missing symbol: {SYM_POPULATE_PERMISSION_CONFIG}",
                {"symbol": SYM_PERMISSION_GRANTS_FOR, "va": hex(text_va + pg_eo)},
            )
        elif not pop_calls:
            fail(
                "permission_grants_for",
                f"{SYM_POPULATE_PERMISSION_CONFIG} does not call {SYM_PERMISSION_GRANTS_FOR}",
                {"symbol": SYM_PERMISSION_GRANTS_FOR, "va": hex(text_va + pg_eo)},
            )
        else:
            ok(
                "permission_grants_for",
                f"{SYM_PERMISSION_GRANTS_FOR} references read_file(/) & write_file(/) and is called by populatePermissionConfig",
                {
                    "symbol": SYM_PERMISSION_GRANTS_FOR,
                    "va": hex(text_va + pg_eo),
                    "literals": {lit.decode("ascii"): [hex(a) for a in refs[lit]] for lit in REQUIRED_STRICT_GRANTS},
                    "populate_call_offsets": [hex(o) for o in pop_calls],
                },
            )

    # 2. server.(*Server).applySettings and server.(*settingsProvider).PermissionGrants handle "strict".
    missing_apply_syms = [s for s in (SYM_APPLY_SETTINGS, SYM_SETTINGS_PERMISSION_GRANTS) if s not in funcs]
    if missing_apply_syms:
        fail("apply_settings_strict", f"missing symbol(s): {', '.join(missing_apply_syms)}")
    else:
        as_eo, as_sz = funcs[SYM_APPLY_SETTINGS]
        sp_eo, sp_sz = funcs[SYM_SETTINGS_PERMISSION_GRANTS]
        as_ok, as_ev = _func_handles_strict(raw, va_to_off, text_va, text_off, arch, as_eo, as_sz)
        sp_ok, sp_ev = _func_handles_strict(raw, va_to_off, text_va, text_off, arch, sp_eo, sp_sz)
        sp_calls_pg = calls(SYM_SETTINGS_PERMISSION_GRANTS, SYM_PERMISSION_GRANTS_FOR)
        if not as_ok:
            fail(
                "apply_settings_strict",
                f"{SYM_APPLY_SETTINGS} does not handle 'strict' literal",
                {"va": hex(text_va + as_eo)},
            )
        elif not sp_ok:
            fail(
                "apply_settings_strict",
                f"{SYM_SETTINGS_PERMISSION_GRANTS} does not handle 'strict' literal",
                {"va": hex(text_va + sp_eo)},
            )
        elif not sp_calls_pg:
            fail(
                "apply_settings_strict",
                f"{SYM_SETTINGS_PERMISSION_GRANTS} does not call {SYM_PERMISSION_GRANTS_FOR}",
                {"va": hex(text_va + sp_eo)},
            )
        else:
            ok(
                "apply_settings_strict",
                f"{SYM_APPLY_SETTINGS} and {SYM_SETTINGS_PERMISSION_GRANTS} handle 'strict' and call {SYM_PERMISSION_GRANTS_FOR}",
                {
                    "apply_settings_va": hex(text_va + as_eo),
                    "apply_settings_evidence": as_ev,
                    "settings_provider_va": hex(text_va + sp_eo),
                    "settings_provider_evidence": sp_ev,
                    "grants_call_offsets": [hex(o) for o in sp_calls_pg],
                },
            )

    # 3. checkSingleTargetOnce evaluates lookupEffectiveGrant strictly BEFORE hardcoded/default allow grants.
    missing_order_syms = [
        s for s in (SYM_CHECK_SINGLE_TARGET_ONCE, SYM_LOOKUP_EFFECTIVE_GRANT, SYM_GET_HARDCODED_GRANTS)
        if s not in funcs
    ]
    if missing_order_syms:
        fail("check_single_target_order", f"missing symbol(s): {', '.join(missing_order_syms)}")
    else:
        chk_eo, _ = funcs[SYM_CHECK_SINGLE_TARGET_ONCE]
        chk_va = text_va + chk_eo
        eff_offs = calls(SYM_CHECK_SINGLE_TARGET_ONCE, SYM_LOOKUP_EFFECTIVE_GRANT)
        hc_offs = calls(SYM_CHECK_SINGLE_TARGET_ONCE, SYM_GET_HARDCODED_GRANTS)
        def_offs = calls(SYM_CHECK_SINGLE_TARGET_ONCE, SYM_STORE_LOOKUP_DEFAULT)
        fpa_offs = calls(SYM_CHECK_SINGLE_TARGET_ONCE, SYM_IS_FILE_PATH_ALLOWED)

        if not eff_offs:
            fail(
                "check_single_target_order",
                f"{SYM_CHECK_SINGLE_TARGET_ONCE} does not call {SYM_LOOKUP_EFFECTIVE_GRANT}",
                {"va": hex(chk_va)},
            )
        elif hc_offs:
            # Architecture A (agy <= 1.5.16): direct lookupEffectiveGrant < getHardcodedGrants inside checkSingleTargetOnce
            if max(eff_offs) >= min(hc_offs):
                fail(
                    "check_single_target_order",
                    f"call order regression: lookupEffectiveGrant ({hex(chk_va + max(eff_offs))}) "
                    f"is NOT before getHardcodedGrants ({hex(chk_va + min(hc_offs))})",
                    {
                        "mode": "direct_hardcoded",
                        "lookup_effective_vas": [hex(chk_va + o) for o in eff_offs],
                        "hardcoded_grants_vas": [hex(chk_va + o) for o in hc_offs],
                    },
                )
            elif def_offs and max(eff_offs) >= min(def_offs):
                fail(
                    "check_single_target_order",
                    f"call order regression: lookupEffectiveGrant ({hex(chk_va + max(eff_offs))}) "
                    f"is NOT before LookupDefault ({hex(chk_va + min(def_offs))})",
                    {"mode": "direct_hardcoded"},
                )
            elif fpa_offs and max(eff_offs) >= min(fpa_offs):
                fail(
                    "check_single_target_order",
                    f"call order regression: lookupEffectiveGrant ({hex(chk_va + max(eff_offs))}) "
                    f"is NOT before isFilePathAllowed ({hex(chk_va + min(fpa_offs))})",
                    {"mode": "direct_hardcoded"},
                )
            else:
                ok(
                    "check_single_target_order",
                    f"lookupEffectiveGrant ({hex(chk_va + eff_offs[0])}) precedes getHardcodedGrants ({hex(chk_va + hc_offs[0])})",
                    {
                        "mode": "direct_hardcoded",
                        "check_va": hex(chk_va),
                        "lookup_effective_vas": [hex(chk_va + o) for o in eff_offs],
                        "hardcoded_grants_vas": [hex(chk_va + o) for o in hc_offs],
                        "is_file_path_allowed_vas": [hex(chk_va + o) for o in fpa_offs],
                    },
                )
        elif def_offs:
            # Architecture B (agy >= 1.5.18): getHardcodedGrants loaded into PermissionGrantStore defaults;
            # checkSingleTargetOnce calls lookupEffectiveGrant (-> LookupEffective) BEFORE LookupDefault.
            store_eff_calls = calls(SYM_LOOKUP_EFFECTIVE_GRANT, SYM_STORE_LOOKUP_EFFECTIVE)
            init_hc_calls = calls(SYM_NEW_PM_WITH_CONV_GRANTS, SYM_GET_HARDCODED_GRANTS)
            init_store_calls = calls(SYM_NEW_PM_WITH_CONV_GRANTS, SYM_NEW_GRANT_STORE)
            if not store_eff_calls:
                fail(
                    "check_single_target_order",
                    f"{SYM_LOOKUP_EFFECTIVE_GRANT} does not call {SYM_STORE_LOOKUP_EFFECTIVE}",
                    {"mode": "grant_store_defaults"},
                )
            elif not init_hc_calls or not init_store_calls or max(init_hc_calls) >= min(init_store_calls):
                fail(
                    "check_single_target_order",
                    f"{SYM_NEW_PM_WITH_CONV_GRANTS} does not pass {SYM_GET_HARDCODED_GRANTS} into {SYM_NEW_GRANT_STORE}",
                    {"mode": "grant_store_defaults"},
                )
            elif max(eff_offs) >= min(def_offs):
                fail(
                    "check_single_target_order",
                    f"call order regression: lookupEffectiveGrant ({hex(chk_va + max(eff_offs))}) "
                    f"is NOT before LookupDefault ({hex(chk_va + min(def_offs))})",
                    {
                        "mode": "grant_store_defaults",
                        "lookup_effective_vas": [hex(chk_va + o) for o in eff_offs],
                        "lookup_default_vas": [hex(chk_va + o) for o in def_offs],
                    },
                )
            elif fpa_offs and max(eff_offs) >= min(fpa_offs):
                fail(
                    "check_single_target_order",
                    f"call order regression: lookupEffectiveGrant ({hex(chk_va + max(eff_offs))}) "
                    f"is NOT before isFilePathAllowed ({hex(chk_va + min(fpa_offs))})",
                    {
                        "mode": "grant_store_defaults",
                        "lookup_effective_vas": [hex(chk_va + o) for o in eff_offs],
                        "is_file_path_allowed_vas": [hex(chk_va + o) for o in fpa_offs],
                    },
                )
            else:
                ok(
                    "check_single_target_order",
                    f"lookupEffectiveGrant ({hex(chk_va + eff_offs[0])}) precedes LookupDefault ({hex(chk_va + def_offs[0])})"
                    + (f" and isFilePathAllowed ({hex(chk_va + fpa_offs[0])})" if fpa_offs else ""),
                    {
                        "mode": "grant_store_defaults",
                        "check_va": hex(chk_va),
                        "lookup_effective_vas": [hex(chk_va + o) for o in eff_offs],
                        "lookup_default_vas": [hex(chk_va + o) for o in def_offs],
                        "is_file_path_allowed_vas": [hex(chk_va + o) for o in fpa_offs],
                    },
                )
        else:
            fail(
                "check_single_target_order",
                f"{SYM_CHECK_SINGLE_TARGET_ONCE} calls neither {SYM_GET_HARDCODED_GRANTS} nor {SYM_STORE_LOOKUP_DEFAULT}; "
                f"unrecognized permission precedence layout",
                {"va": hex(chk_va)},
            )

    # 4. ReadFileTool implements RequiredPermissions referencing "read_file" with symlink resolution.
    if SYM_READ_FILE_REQUIRED_PERMS not in funcs:
        fail("read_file_required_permissions", f"missing symbol: {SYM_READ_FILE_REQUIRED_PERMS}")
    else:
        rf_eo, rf_sz = funcs[SYM_READ_FILE_REQUIRED_PERMS]
        rf_va = text_va + rf_eo
        rf_refs = find_referenced_strings(raw, va_to_off, text_va, text_off, arch, rf_eo, rf_sz, (b"read_file",))
        direct_symlink = calls(SYM_READ_FILE_REQUIRED_PERMS, SYM_RESOLVE_SYMLINK)
        helper_calls = calls(SYM_READ_FILE_REQUIRED_PERMS, SYM_FILE_REQUIRED_PERMS)
        helper_symlink = calls(SYM_FILE_REQUIRED_PERMS, SYM_RESOLVE_SYMLINK) if helper_calls else []
        if rf_sz < 16:
            fail(
                "read_file_required_permissions",
                f"{SYM_READ_FILE_REQUIRED_PERMS} body too small ({rf_sz} bytes; possible no-op stub)",
                {"va": hex(rf_va), "size": rf_sz},
            )
        elif not rf_refs[b"read_file"]:
            fail(
                "read_file_required_permissions",
                f"{SYM_READ_FILE_REQUIRED_PERMS} does not reference 'read_file' action string",
                {"va": hex(rf_va), "size": rf_sz},
            )
        elif not direct_symlink and not (helper_calls and helper_symlink):
            fail(
                "read_file_required_permissions",
                f"{SYM_READ_FILE_REQUIRED_PERMS} does not resolve symlinks via {SYM_RESOLVE_SYMLINK}",
                {"va": hex(rf_va), "size": rf_sz},
            )
        else:
            ok(
                "read_file_required_permissions",
                f"{SYM_READ_FILE_REQUIRED_PERMS} references 'read_file' and resolves symlinks "
                + ("directly" if direct_symlink else f"via {SYM_FILE_REQUIRED_PERMS}"),
                {
                    "va": hex(rf_va),
                    "size": rf_sz,
                    "read_file_refs": [hex(a) for a in rf_refs[b"read_file"]],
                    "symlink_resolution": "direct" if direct_symlink else SYM_FILE_REQUIRED_PERMS,
                },
            )

    # 5. Synchronous ACP permission prompt bridge is intact.
    missing_acp_syms = [
        s for s in (SYM_ENSURE_PERMISSIONS, SYM_RESOLVE_TOOL_PERMISSION, SYM_REQUEST_PERMISSION_FROM_CLIENT)
        if s not in funcs
    ]
    if missing_acp_syms:
        fail("acp_permission_prompt_path", f"missing symbol(s): {', '.join(missing_acp_syms)}")
    else:
        prompt_calls = calls(SYM_RESOLVE_TOOL_PERMISSION, SYM_REQUEST_PERMISSION_FROM_CLIENT)
        if not prompt_calls:
            fail(
                "acp_permission_prompt_path",
                f"{SYM_RESOLVE_TOOL_PERMISSION} does not call {SYM_REQUEST_PERMISSION_FROM_CLIENT}",
            )
        else:
            rt_eo, _ = funcs[SYM_RESOLVE_TOOL_PERMISSION]
            ok(
                "acp_permission_prompt_path",
                f"{SYM_RESOLVE_TOOL_PERMISSION} calls {SYM_REQUEST_PERMISSION_FROM_CLIENT} and EnsurePermissions is present",
                {
                    "resolve_va": hex(text_va + rt_eo),
                    "prompt_call_offsets": [hex(o) for o in prompt_calls],
                },
            )

    return {
        "ok": len(errors) == 0,
        "spec_version": SPEC_VERSION,
        "arch": arch,
        "checks": checks,
        "errors": errors,
    }


def _probe_binary_version(binary_path):
    """Best-effort offline `--version` query (0 API quota; returns None on cross-arch/non-executable)."""
    p = Path(binary_path)
    if not p.is_file() or not os.access(p, os.X_OK):
        return None
    try:
        with p.open("rb") as fh:
            head = fh.read(4)
    except OSError:
        return None
    if head not in (b"\x7fELF", b"\xcf\xfa\xed\xfe") and head[:2] != b"MZ":
        return None
    env = {k: v for k, v in os.environ.items() if not k.startswith(("AGY_", "FAKE_"))}
    try:
        out = subprocess.check_output(
            [str(p), "--version"],
            env=env,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    lines = [line.strip() for line in out.splitlines() if line.strip()]
    return lines[0] if lines else None


def check_binary(binary_path, *, up=None, version_override=None):
    """Inspect a candidate `agy` binary file on disk and return structured JSON report."""
    p = Path(binary_path)
    raw = p.read_bytes()
    result = inspect_binary_data(raw, up=up)
    ver = version_override if version_override is not None else _probe_binary_version(p)
    return {
        "ok": result["ok"],
        "version": ver,
        "spec_version": result["spec_version"],
        "arch": result["arch"],
        "binary": str(p),
        "generation_requests": 0,
        "checks": result["checks"],
        "errors": result["errors"],
    }


def main(argv=None):
    ap = argparse.ArgumentParser(description="Offline G5a strict-permission binary contract check for agy")
    ap.add_argument("--binary", required=True, help="path to candidate agy binary")
    ap.add_argument("--report", help="optional path to write structured JSON report")
    ap.add_argument("--version", dest="version_override", default=None, help="optional version label override")
    args = ap.parse_args(argv)

    exit_code = 0
    try:
        report = check_binary(args.binary, version_override=args.version_override)
        exit_code = 0 if report["ok"] else 1
    except (MediaStrictCheckError, OSError) as exc:
        report = {
            "ok": False,
            "version": args.version_override,
            "spec_version": SPEC_VERSION,
            "arch": None,
            "binary": str(args.binary),
            "generation_requests": 0,
            "checks": {},
            "errors": [str(exc)],
        }
        exit_code = 2
    except Exception as exc:  # noqa: BLE001 - upstream PatchError / container errors
        report = {
            "ok": False,
            "version": args.version_override,
            "spec_version": SPEC_VERSION,
            "arch": None,
            "binary": str(args.binary),
            "generation_requests": 0,
            "checks": {},
            "errors": [f"{type(exc).__name__}: {exc}"],
        }
        exit_code = 2

    payload = json.dumps(report, ensure_ascii=False, indent=2)
    if args.report:
        rp = Path(args.report)
        rp.parent.mkdir(parents=True, exist_ok=True)
        rp.write_text(payload + "\n", encoding="utf-8")
    print(payload)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
