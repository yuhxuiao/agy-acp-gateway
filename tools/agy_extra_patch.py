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
    Supported on both x86-64 and arm64 (ELF / Mach-O).

codeassist-per-trajectory-session-id  getStreamingTextCompletion -> SessionID = traj.TrajectoryId
    On agy <= 1.5.13, propagates the per-conversation trajectory UUID into the
    upstream CCPA streaming completion request instead of the caller's empty
    zero-init string (x86-64 with post-site %rsi/%esi liveness verification and
    arm64 ELF / Mach-O). On agy >= 1.5.14, verifies that the refactored native
    streaming pipeline (`modelapiccpa` -> `GenerateStream` -> `ccpa.BuildRequest`
    -> `ccpa.HashSessionID` -> `strconv.FormatInt`, plus version-specific
    `1.5.14` / `1.5.15+` execution and CCPA HTTP transport profiles) is intact
    and reports `state=native` without patching (fail-closed if any symbol or
    call edge breaks).

Usage:
    agy_extra_patch.py --binary FILE --dry-run
    agy_extra_patch.py --binary FILE --in-place
    agy_extra_patch.py --binary FILE --check      # exit 0 = all applied, 1 = vanilla, 2 = error
"""

import argparse
import hashlib
import importlib.util
import os
import struct
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
UPSTREAM = os.path.normpath(os.path.join(HERE, "..", "third_party", "agy-context-unlock", "agy-patch.py"))

SPEC_VERSION = 4

TRAJ_CALLER_FUNC = "codeassistclient.(*CodeAssistClient).GetChatMessage.func3"
TRAJ_CALLEE_FUNC = "codeassistclient.(*CodeAssistClient).getStreamingTextCompletion"

NATIVE_SESSION_ID_ANCHOR_FUNC = "ccpa.BuildRequest"
NATIVE_SESSION_ID_COMMON_FUNCS = (
    "core.NewAgent",
    "integration.(*modelAPIAdapter).Generate",
    "integration.(*modelAPIAdapter).ClassifyError",
    "modelapiccpa.(*Client).Generate",
    "modelapiccpa.(*Client).buildGenerateStreamRequest",
    "codeassistclient.(*CodeAssistClient).GenerateStream",
    "codeassistclient.(*CodeAssistClient).openStreamGenerateContent",
    "codeassistclient.(*CodeAssistClient).openCCPAContinuationStream",
    "ccpa.BuildRequest",
    "ccpa.HashSessionID",
    "ccpahttp.MarshalRequest",
    "strconv.FormatInt",
)
NATIVE_SESSION_ID_COMMON_CALL_EDGES = (
    ("modelapiccpa.(*Client).Generate", "modelapiccpa.(*Client).buildGenerateStreamRequest"),
    ("modelapiccpa.(*Client).Generate", "codeassistclient.(*CodeAssistClient).GenerateStream"),
    ("codeassistclient.(*CodeAssistClient).GenerateStream", "ccpa.BuildRequest"),
    ("codeassistclient.(*CodeAssistClient).GenerateStream", "codeassistclient.(*CodeAssistClient).openStreamGenerateContent"),
    ("codeassistclient.(*CodeAssistClient).openStreamGenerateContent", "codeassistclient.(*CodeAssistClient).openCCPAContinuationStream"),
    ("ccpa.BuildRequest", "ccpa.HashSessionID"),
    ("ccpa.HashSessionID", "strconv.FormatInt"),
)
NATIVE_SESSION_ID_PROFILES = {
    "1.5.14": {
        "funcs": (
            "agentexecutor.(*AgentExecutor).constructExecutor",
            "integration.NewFromAgent",
            "codeassistclient.(*CodeAssistClient).openCCPASSEStreamLeg",
        ),
        "call_edges": (
            ("agentexecutor.(*AgentExecutor).constructExecutor", "core.NewAgent"),
            ("agentexecutor.(*AgentExecutor).constructExecutor", "integration.NewFromAgent"),
            ("codeassistclient.(*CodeAssistClient).openCCPASSEStreamLeg", "ccpahttp.MarshalRequest"),
        ),
    },
    "1.5.15+": {
        "funcs": (
            "agentexecutor.(*AgentExecutor).constructCoreExecution",
            "agentexecutor.(*AgentExecutor).newCoreExecution",
            "ccpahttp.(*Transport).Stream",
            "ccpahttp.(*Transport).prepareEnvelope",
            "ccpahttp.shallowCloneAndStamp",
        ),
        "call_edges": (
            ("agentexecutor.(*AgentExecutor).constructCoreExecution", "agentexecutor.(*AgentExecutor).newCoreExecution"),
            ("agentexecutor.(*AgentExecutor).newCoreExecution", "core.NewAgent"),
            ("ccpahttp.(*Transport).Stream", "ccpahttp.(*Transport).prepareEnvelope"),
            ("ccpahttp.(*Transport).prepareEnvelope", "ccpahttp.shallowCloneAndStamp"),
            ("ccpahttp.(*Transport).Stream", "ccpahttp.MarshalRequest"),
        ),
    },
}
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
        "arm64": {
            # Go ABIInternal arm64 prologue: ldr x16, [x28, #0x10]  (stack-growth check)
            "vanilla_prefix": bytes.fromhex("90 0b 40 f9"),
            # a native "return false, nil" site proves the (bool, error) -> (x0, x1, x2) signature:
            # mov x0, xzr ; mov x1, xzr ; mov x2, xzr
            "signature_in_body": bytes.fromhex("e0 03 1f aa e1 03 1f aa e2 03 1f aa"),
            # mov x0, xzr ; mov x1, xzr ; mov x2, xzr ; ret   (no frame set up yet)
            "patched": bytes.fromhex("e0 03 1f aa e1 03 1f aa e2 03 1f aa c0 03 5f d6"),
        },
    },
    {
        "id": "codeassist-per-trajectory-session-id",
        "func": TRAJ_CALLEE_FUNC,
        "locator": "trajectory_session_id",
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


def _build_traj_session_id_patch(traj_disp):
    return (
        b"\x48\x8b\x8c\x24" + struct.pack("<I", traj_disp) +
        b"\x48\x85\xc9" +
        b"\x74\x10" +
        b"\x48\x8b\x71\x08" +
        b"\x48\x89\x70\x58" +
        b"\x48\x8b\x71\x10" +
        b"\x48\x89\x70\x60" +
        b"\x0f\x1f\x00"
    )


def _build_arm64_patched_anchor(traj_disp, params_disp):
    """6-word (24B) arm64 anchor keeping `traj` in scratch x16 and `&params` in x0."""
    return struct.pack(
        "<6I",
        0xF94003F0 | ((traj_disp // 8) << 10),  # ldr   x16, [sp, #traj_disp]
        0xB4000070,                             # cbz   x16, +0xc
        0xB9801A01,                             # ldrsw x1, [x16, #0x18]
        0x14000002,                             # b     +0x8
        0xAA1F03E1,                             # mov   x1, xzr
        0x910003E0 | (params_disp << 10),       # add   x0, sp, #params_disp
    )


def _build_arm64_patched_tail(ldr_x2_src50, tail_words):
    """Tail words writing params+0x50 (if coalesced) and params.SessionID (+0x58/+0x60) from x16."""
    core = [
        0xB4000070,  # cbz x16, +0xc (if traj == nil, keep zero-filled SessionID)
        0xA9408E02,  # ldp x2, x3, [x16, #0x8]
        0xA9058C02,  # stp x2, x3, [x0, #0x58]
    ]
    if ldr_x2_src50 is not None:
        words = [ldr_x2_src50, 0xF9002802, *core]  # ldr x2, [sp, #src50] ; str x2, [x0, #0x50] ; ...
    else:
        words = list(core)
    if len(words) < tail_words:
        words.extend([0xD503201F] * (tail_words - len(words)))
    return struct.pack(f"<{len(words)}I", *words)


def _arm64_branch_targets_interior(fn_body, region_start, region_end):
    """Return True if any branch outside [region_start, region_end) jumps into (region_start, region_end)."""
    for i in range(0, len(fn_body) - 3, 4):
        if region_start <= i < region_end:
            continue
        w = struct.unpack_from("<I", fn_body, i)[0]
        target = None
        # Unconditional B imm26 (0x14000000) — exclude BL (0x94000000)
        if (w & 0xFC000000) == 0x14000000:
            imm26 = w & 0x3FFFFFF
            if imm26 & 0x2000000:
                imm26 -= 0x4000000
            target = i + imm26 * 4
        # B.cond (0x54000000) or CBZ/CBNZ (0x34/35/B4/B5) -> imm19 at bits [23:5]
        elif (w & 0xFF000010) == 0x54000000 or (w & 0x7E000000) == 0x34000000:
            imm19 = (w >> 5) & 0x7FFFF
            if imm19 & 0x40000:
                imm19 -= 0x80000
            target = i + imm19 * 4
        # TBZ/TBNZ (0x36/37/B6/B7) -> imm14 at bits [18:5]
        elif (w & 0x7E000000) == 0x36000000:
            imm14 = (w >> 5) & 0x3FFF
            if imm14 & 0x2000:
                imm14 -= 0x4000
            target = i + imm14 * 4
        if target is not None and region_start < target < region_end:
            return True
    return False


def _x86_decode_modrm(buf, idx, rex_r, rex_x, rex_b):
    """Decode x86-64 ModRM (+ optional SIB + disp).

    Returns (next_idx, mod, reg_id, rm_id, addr_regs) or None if truncated.
    """
    if idx >= len(buf):
        return None
    modrm = buf[idx]
    idx += 1
    mod = (modrm >> 6) & 3
    reg_id = ((modrm >> 3) & 7) | (rex_r << 3)
    rm_low = modrm & 7
    rm_id = rm_low | (rex_b << 3)
    if mod == 3:
        return idx, mod, reg_id, rm_id, set()

    addr_regs = set()
    if rm_low == 4:
        if idx >= len(buf):
            return None
        sib = buf[idx]
        idx += 1
        index_low = (sib >> 3) & 7
        base_low = sib & 7
        if not (index_low == 4 and rex_x == 0):
            addr_regs.add(index_low | (rex_x << 3))
        if mod == 0 and base_low == 5:
            if idx + 4 > len(buf):
                return None
            idx += 4
        else:
            addr_regs.add(base_low | (rex_b << 3))
    elif mod == 0 and rm_low == 5:
        if idx + 4 > len(buf):
            return None
        idx += 4
    else:
        addr_regs.add(rm_id)

    if mod == 1:
        if idx + 1 > len(buf):
            return None
        idx += 1
    elif mod == 2:
        if idx + 4 > len(buf):
            return None
        idx += 4
    return idx, mod, reg_id, rm_id, addr_regs


def _x86_reg_is_rsi(reg_id, is_8bit, has_rex):
    # Without a REX prefix, 8-bit register encoding 6 is %dh, not %sil.
    if reg_id != 6:
        return False
    return (not is_8bit) or has_rex


def _x86_decode_rsi_step(buf, pos):
    """Decode one x86-64 instruction at `pos` for %rsi/%esi/%si/%sil liveness.

    Returns (next_pos, reads_rsi, writes_rsi, kind, rel32) or None if unrecognised.
    """
    n = len(buf)
    cur = pos
    op_66 = False
    rep_pfx = False
    while cur < n and buf[cur] in (0x66, 0xF2, 0xF3):
        if buf[cur] == 0x66:
            op_66 = True
        else:
            rep_pfx = True
        cur += 1
    has_rex = False
    rex_w = rex_r = rex_x = rex_b = 0
    if cur < n and 0x40 <= buf[cur] <= 0x4F:
        rex = buf[cur]
        has_rex = True
        rex_w = (rex >> 3) & 1
        rex_r = (rex >> 2) & 1
        rex_x = (rex >> 1) & 1
        rex_b = rex & 1
        cur += 1
    if cur >= n:
        return None
    op = buf[cur]
    cur += 1

    # Single-byte NOP / XCHG %eax,%eax
    if op == 0x90 and not rep_pfx and rex_b == 0:
        return cur, False, False, "nop", 0
    # Near RET
    if op == 0xC3 and not has_rex and not op_66 and not rep_pfx:
        return cur, False, True, "ret", 0
    # Relative CALL rel32
    if op == 0xE8 and not has_rex and not op_66 and not rep_pfx:
        if cur + 4 > n:
            return None
        rel32 = struct.unpack_from("<i", buf, cur)[0]
        return cur + 4, False, False, "call", rel32
    # MOV $imm, reg (0xB8..0xBF)
    if 0xB8 <= op <= 0xBF and not rep_pfx:
        reg_id = (op - 0xB8) | (rex_b << 3)
        imm_len = 8 if rex_w else (2 if op_66 else 4)
        if cur + imm_len > n:
            return None
        return cur + imm_len, False, reg_id == 6, "op", 0
    # MOV $imm8, reg8 (0xB0..0xB7)
    if 0xB0 <= op <= 0xB7 and not rep_pfx:
        reg_id = (op - 0xB0) | (rex_b << 3)
        if cur + 1 > n:
            return None
        return cur + 1, False, _x86_reg_is_rsi(reg_id, True, has_rex), "op", 0

    # Two-byte 0x0F opcodes
    if op == 0x0F:
        if cur >= n:
            return None
        op2 = buf[cur]
        cur += 1
        # Multi-byte NOP: 0F 1F /0
        if op2 == 0x1F:
            dec = _x86_decode_modrm(buf, cur, rex_r, rex_x, rex_b)
            return (dec[0], False, False, "nop", 0) if dec else None
        # SSE moves: 0F 10 / 0F 11 / 0F 28 / 0F 29 (movups/movaps/movss/movsd)
        if op2 in (0x10, 0x11, 0x28, 0x29):
            dec = _x86_decode_modrm(buf, cur, rex_r, rex_x, rex_b)
            if not dec:
                return None
            end_i, _mod, _reg, _rm, addr_regs = dec
            return end_i, (6 in addr_regs), False, "op", 0
        # Zero/sign-extend loads: 0F B6 (movzbl), 0F B7 (movzwl), 0F BE (movsbl), 0F BF (movswl)
        if op2 in (0xB6, 0xB7, 0xBE, 0xBF) and not rep_pfx:
            dec = _x86_decode_modrm(buf, cur, rex_r, rex_x, rex_b)
            if not dec:
                return None
            end_i, mod, reg_id, rm_id, addr_regs = dec
            is_8bit_src = op2 in (0xB6, 0xBE)
            reads = (6 in addr_regs) or (mod == 3 and _x86_reg_is_rsi(rm_id, is_8bit_src, has_rex))
            writes = reg_id == 6
            return end_i, reads, writes, "op", 0
        return None

    if rep_pfx:
        return None

    # Stores r -> r/m (0x88 8-bit, 0x89 16/32/64-bit)
    if op in (0x88, 0x89):
        dec = _x86_decode_modrm(buf, cur, rex_r, rex_x, rex_b)
        if not dec:
            return None
        end_i, mod, reg_id, rm_id, addr_regs = dec
        is_8bit = op == 0x88
        reads = (6 in addr_regs) or _x86_reg_is_rsi(reg_id, is_8bit, has_rex)
        writes = (mod == 3) and _x86_reg_is_rsi(rm_id, is_8bit, has_rex) and not (op == 0x88 or op_66)
        return end_i, reads, writes, "op", 0

    # Loads r/m -> r (0x8A 8-bit, 0x8B 16/32/64-bit, 0x63 movsxd) & LEA (0x8D)
    if op in (0x8A, 0x8B, 0x8D, 0x63):
        dec = _x86_decode_modrm(buf, cur, rex_r, rex_x, rex_b)
        if not dec:
            return None
        end_i, mod, reg_id, rm_id, addr_regs = dec
        if op == 0x8D and mod == 3:
            return None
        is_8bit = op == 0x8A
        reads = (6 in addr_regs) or (mod == 3 and _x86_reg_is_rsi(rm_id, is_8bit, has_rex))
        writes = _x86_reg_is_rsi(reg_id, is_8bit, has_rex) and not (is_8bit or op_66)
        return end_i, reads, writes, "op", 0

    # XOR r, r/m (0x31) or XOR r/m, r (0x33) — handle self-zeroing xor %esi,%esi / xor %rsi,%rsi
    if op in (0x31, 0x33) and not op_66:
        dec = _x86_decode_modrm(buf, cur, rex_r, rex_x, rex_b)
        if not dec:
            return None
        end_i, mod, reg_id, rm_id, addr_regs = dec
        if mod == 3 and reg_id == rm_id:
            return end_i, False, (reg_id == 6), "op", 0
        reads = (6 in addr_regs) or (reg_id == 6) or (mod == 3 and rm_id == 6)
        return end_i, reads, False, "op", 0

    # MOV $imm, r/m (0xC6 /0 imm8, 0xC7 /0 imm16/32)
    if op in (0xC6, 0xC7):
        dec = _x86_decode_modrm(buf, cur, rex_r, rex_x, rex_b)
        if not dec or (dec[2] & 7) != 0:
            return None
        end_i, mod, _reg_id, rm_id, addr_regs = dec
        imm_len = 1 if op == 0xC6 else (2 if op_66 else 4)
        if end_i + imm_len > n:
            return None
        is_8bit = op == 0xC6
        writes = (mod == 3) and _x86_reg_is_rsi(rm_id, is_8bit, has_rex) and not (is_8bit or op_66)
        return end_i + imm_len, (6 in addr_regs), writes, "op", 0

    # ALU $imm, r/m (0x80 imm8, 0x81 imm16/32, 0x83 imm8) & TEST (0x84, 0x85) & CMP (0x38..0x3B)
    if op in (0x80, 0x81, 0x83, 0x84, 0x85, 0x38, 0x39, 0x3A, 0x3B):
        dec = _x86_decode_modrm(buf, cur, rex_r, rex_x, rex_b)
        if not dec:
            return None
        end_i, mod, reg_id, rm_id, addr_regs = dec
        imm_len = 0
        if op in (0x80, 0x83):
            imm_len = 1
        elif op == 0x81:
            imm_len = 2 if op_66 else 4
        if end_i + imm_len > n:
            return None
        is_8bit = op in (0x80, 0x84, 0x38, 0x3A)
        reads = (6 in addr_regs) or (mod == 3 and _x86_reg_is_rsi(rm_id, is_8bit, has_rex))
        if op in (0x84, 0x85, 0x38, 0x39, 0x3A, 0x3B):
            reads = reads or _x86_reg_is_rsi(reg_id, is_8bit, has_rex)
        return end_i + imm_len, reads, False, "op", 0

    return None


def _x86_callee_reads_rsi_arg(data, text_va, text_off, target_va):
    """Check whether a called Go x86-64 function consumes %rsi as an input argument."""
    rel_text = target_va - text_va
    if rel_text < 0 or text_off + rel_text >= len(data):
        return False
    t_off = text_off + rel_text
    head = bytes(data[t_off:min(len(data), t_off + 128)])
    if len(head) < 4:
        return False

    # 1. If the callee has a standard Go ABIInternal stack-guard prologue (`cmp 0x10(%r14), ...`
    #    followed by `jbe morestack`), Go's `morestack` block spills EVERY live input register
    #    argument before `call runtime.morestack_noctxt`. Check that spill block first.
    cur = 0
    for marker in (b"\x49\x3b\x66\x10", b"\x4d\x3b\x66\x10"):
        idx = head[:24].find(marker)
        if idx >= 0:
            j_pos = idx + 4
            ms_rel = None
            if j_pos + 2 <= len(head) and head[j_pos] == 0x76:
                ms_rel = j_pos + 2 + struct.unpack_from("<b", head, j_pos + 1)[0]
                cur = j_pos + 2
            elif j_pos + 6 <= len(head) and head[j_pos:j_pos + 2] == b"\x0f\x86":
                ms_rel = j_pos + 6 + struct.unpack_from("<i", head, j_pos + 2)[0]
                cur = j_pos + 6
            if ms_rel is not None and 0 < ms_rel and t_off + ms_rel < len(data):
                ms_buf = bytes(data[t_off + ms_rel:min(len(data), t_off + ms_rel + 64)])
                m_pos = 0
                while m_pos < len(ms_buf):
                    step = _x86_decode_rsi_step(ms_buf, m_pos)
                    if step is None:
                        break
                    next_m, reads_rsi, writes_rsi, kind, _ = step
                    if reads_rsi:
                        return True
                    if writes_rsi or kind in ("call", "ret"):
                        break
                    m_pos = next_m
            break

    # 2. Also scan the callee entry instructions (skipping standard frame allocation `sub $imm, %rsp`).
    while cur < min(len(head), 64):
        if head[cur:cur + 3] == b"\x48\x83\xec" and cur + 4 <= len(head):
            cur += 4
            continue
        if head[cur:cur + 3] == b"\x48\x81\xec" and cur + 7 <= len(head):
            cur += 7
            continue
        step = _x86_decode_rsi_step(head, cur)
        if step is None:
            break
        next_c, reads_rsi, writes_rsi, kind, _ = step
        if reads_rsi:
            return True
        if writes_rsi or kind in ("call", "ret"):
            return False
        cur = next_c
    return False


def _verify_x86_64_rsi_dead(fn_body, start_pos, fn_va, data, text_va, text_off, patch_id):
    """Verify %rsi/%esi is overwritten or killed by CALL/RET without any prior read."""
    limit = min(len(fn_body), start_pos + 192)
    cur = start_pos
    while cur < limit:
        step = _x86_decode_rsi_step(fn_body, cur)
        if step is None:
            raise ExtraPatchError(
                f"{patch_id}: {TRAJ_CALLEE_FUNC} @ {fn_va + cur:#x} has unrecognised x86-64 "
                f"instruction ({fn_body[cur:cur + 8].hex(' ')}) before %rsi is killed; refusing to patch"
            )
        next_pos, reads_rsi, writes_rsi, kind, rel32 = step
        if reads_rsi:
            raise ExtraPatchError(
                f"{patch_id}: {TRAJ_CALLEE_FUNC} @ {fn_va + cur:#x} reads %rsi/%esi after patch site; "
                f"refusing to clobber %rsi"
            )
        if writes_rsi or kind == "ret":
            return
        if kind == "call":
            target_va = fn_va + next_pos + rel32
            if _x86_callee_reads_rsi_arg(data, text_va, text_off, target_va):
                raise ExtraPatchError(
                    f"{patch_id}: {TRAJ_CALLEE_FUNC} call target {target_va:#x} reads %rsi argument; "
                    f"refusing to clobber %rsi"
                )
            return
        cur = next_pos
    raise ExtraPatchError(
        f"{patch_id}: {TRAJ_CALLEE_FUNC} @ {fn_va + start_pos:#x} did not overwrite or kill %rsi "
        f"within 192 bytes; refusing to patch"
    )


def _arm64_decode_scratch_use(word):
    """Decode arm64 post-site instructions for x16, x17, x27 read/write tracking.

    Returns (reads: set[int], writes: set[int], is_call_or_ret: bool) or None if unknown.
    """
    if word == 0xD503201F:  # nop
        return set(), set(), False
    if word == 0xD65F03C0:  # ret
        return set(), {16, 17, 27}, True
    if (word & 0xFC000000) == 0x94000000:  # bl imm26
        return set(), {16, 17, 27}, True
    # add Rd, Rn, #imm12 (64-bit: 0x91000000)
    if (word & 0xFF800000) == 0x91000000:
        rd = word & 0x1F
        rn = (word >> 5) & 0x1F
        reads = {rn} if rn != 31 else set()
        writes = {rd} if rd != 31 else set()
        return reads, writes, False
    # Unsigned-offset loads: ldr x/w, ldrb, ldrh, ldrsw
    if (word & 0xFFC00000) in (0xF9400000, 0xB9400000, 0x39400000, 0x79400000, 0xB9800000):
        rt = word & 0x1F
        rn = (word >> 5) & 0x1F
        reads = {rn} if rn != 31 else set()
        writes = {rt} if rt != 31 else set()
        return reads, writes, False
    # Unsigned-offset stores: str x/w, strb, strh
    if (word & 0xFFC00000) in (0xF9000000, 0xB9000000, 0x39000000, 0x79000000):
        rt = word & 0x1F
        rn = (word >> 5) & 0x1F
        reads = set()
        if rn != 31:
            reads.add(rn)
        if rt != 31:
            reads.add(rt)
        return reads, set(), False
    # Signed-offset STP (32-bit 0x29000000 / 64-bit 0xA9000000)
    if (word & 0xFFC00000) in (0x29000000, 0xA9000000):
        rt = word & 0x1F
        rn = (word >> 5) & 0x1F
        rt2 = (word >> 10) & 0x1F
        reads = {r for r in (rn, rt, rt2) if r != 31}
        return reads, set(), False
    # Signed-offset LDP (32-bit 0x29400000 / 64-bit 0xA9400000)
    if (word & 0xFFC00000) in (0x29400000, 0xA9400000):
        rt = word & 0x1F
        rn = (word >> 5) & 0x1F
        rt2 = (word >> 10) & 0x1F
        reads = {rn} if rn != 31 else set()
        writes = {r for r in (rt, rt2) if r != 31}
        return reads, writes, False
    # Register MOV / ORR Rd, XZR, ... (0xAA000000 / 0x2A000000 / 0xB2000000 / 0xD2800000)
    if (word & 0xFFE00000) in (0xAA000000, 0x2A000000):
        rd = word & 0x1F
        rn = (word >> 5) & 0x1F
        rm = (word >> 16) & 0x1F
        reads = {r for r in (rn, rm) if r != 31}
        writes = {rd} if rd != 31 else set()
        return reads, writes, False
    if (word & 0xFF800000) in (0xB2000000, 0xD2800000):
        rd = word & 0x1F
        rn = (word >> 5) & 0x1F if (word & 0xFF800000) == 0xB2000000 else 31
        reads = {rn} if rn != 31 else set()
        writes = {rd} if rd != 31 else set()
        return reads, writes, False
    return None


def _verify_arm64_x16_dead(fn_body, start_pos, fn_va, patch_id):
    """Verify x16 is not read before overwrite/BL/RET after the arm64 patch site."""
    limit = min(len(fn_body), start_pos + 128)
    for cur in range(start_pos, limit - 3, 4):
        word = struct.unpack_from("<I", fn_body, cur)[0]
        dec = _arm64_decode_scratch_use(word)
        if dec is None:
            raise ExtraPatchError(
                f"{patch_id}: {TRAJ_CALLEE_FUNC} @ {fn_va + cur:#x} has unexpected arm64 instruction "
                f"{word:#010x} before BL/RET; refusing to patch"
            )
        reads, writes, done = dec
        if 16 in reads:
            raise ExtraPatchError(
                f"{patch_id}: {TRAJ_CALLEE_FUNC} @ {fn_va + cur:#x} reads scratch x16 before overwrite"
            )
        if done or (16 in writes):
            return
    raise ExtraPatchError(
        f"{patch_id}: {TRAJ_CALLEE_FUNC} @ {fn_va + start_pos:#x} did not reach BL/RET within 128 bytes"
    )


def _inspect_trajectory_session_id_x86_64(p, data, funcs, text_va, text_off):
    patch_id = p["id"]
    fn_entryoff, fn_size = funcs[TRAJ_CALLEE_FUNC]
    fn_off = text_off + fn_entryoff
    fn_va = text_va + fn_entryoff
    fn_body = bytes(data[fn_off:fn_off + fn_size])

    caller_entryoff, caller_size = funcs[TRAJ_CALLER_FUNC]
    caller_off = text_off + caller_entryoff
    caller_va = text_va + caller_entryoff
    caller_body = bytes(data[caller_off:caller_off + caller_size])

    call_offs = []
    for i in range(len(caller_body) - 4):
        if caller_body[i] != 0xE8:
            continue
        rel = struct.unpack_from("<i", caller_body, i + 1)[0]
        if caller_va + i + 5 + rel == fn_va:
            call_offs.append(i)
    if len(call_offs) != 1:
        raise ExtraPatchError(
            f"{patch_id}: expected 1 call from {TRAJ_CALLER_FUNC} to {TRAJ_CALLEE_FUNC}, found {len(call_offs)}"
        )
    call_off = call_offs[0]
    if b"\x44\x0f\x11\x7c\x24" not in caller_body[max(0, call_off - 192):call_off]:
        raise ExtraPatchError(
            f"{patch_id}: caller {TRAJ_CALLER_FUNC} missing empty sessionID zero-init pattern before call"
        )

    mid = b"\x48\x85\xc9\x74\x05\x8b\x51\x18\xeb\x02\x31\xd2\x48\x8d\x84\x24"
    anchors = []
    search_from = 0
    while True:
        idx = fn_body.find(mid, search_from)
        if idx < 0:
            break
        if idx >= 8 and fn_body[idx - 8:idx - 4] == b"\x48\x8b\x8c\x24" and idx + len(mid) + 4 <= len(fn_body):
            traj_disp = struct.unpack_from("<I", fn_body, idx - 4)[0]
            params_disp = struct.unpack_from("<I", fn_body, idx + len(mid))[0]
            anchors.append((idx - 8, idx + len(mid) + 4, traj_disp, params_disp))
        search_from = idx + 1
    if len(anchors) != 1:
        raise ExtraPatchError(
            f"{patch_id}: {TRAJ_CALLEE_FUNC} @ {fn_va:#x} expected 1 traj/params anchor, found {len(anchors)}"
        )
    _, pos, traj_disp, params_disp = anchors[0]

    while pos < len(fn_body):
        if fn_body[pos:pos + 4] == b"\x44\x0f\x11\x38":
            pos += 4
        elif pos + 5 <= len(fn_body) and fn_body[pos:pos + 4] == b"\x44\x0f\x11\x78":
            pos += 5
        elif pos + 8 <= len(fn_body) and fn_body[pos:pos + 4] == b"\x44\x0f\x11\xb8":
            pos += 8
        else:
            break

    patched_blob = _build_traj_session_id_patch(traj_disp)
    p60 = params_disp + 0x60
    p58 = params_disp + 0x58
    site_off_rel = None
    state = None

    while pos < len(fn_body):
        if pos + 32 <= len(fn_body) and fn_body[pos:pos + 32] == patched_blob:
            site_off_rel = pos
            state = "patched"
            break
        if (
            pos + 16 <= len(fn_body)
            and fn_body[pos:pos + 4] == b"\x48\x8b\x8c\x24"
            and fn_body[pos + 8:pos + 12] == b"\x48\x89\x8c\x24"
        ):
            if (
                pos + 32 <= len(fn_body)
                and fn_body[pos + 16:pos + 20] == b"\x48\x8b\x8c\x24"
                and fn_body[pos + 24:pos + 28] == b"\x48\x89\x8c\x24"
            ):
                src1 = struct.unpack_from("<I", fn_body, pos + 4)[0]
                dst1 = struct.unpack_from("<I", fn_body, pos + 12)[0]
                src2 = struct.unpack_from("<I", fn_body, pos + 20)[0]
                dst2 = struct.unpack_from("<I", fn_body, pos + 28)[0]
                if dst1 == p60 and dst2 == p58 and src1 == src2 + 8:
                    site_off_rel = pos
                    state = "vanilla"
                    break
            pos += 16
            continue
        break

    if site_off_rel is None or state is None:
        raise ExtraPatchError(
            f"{patch_id}: {TRAJ_CALLEE_FUNC} @ {fn_va:#x} did not reach valid SessionID slot "
            f"(params_disp={params_disp:#x}, traj_disp={traj_disp:#x})"
        )

    _verify_x86_64_rsi_dead(fn_body, site_off_rel + 32, fn_va, data, text_va, text_off, patch_id)
    return {"patched": patched_blob}, fn_off + site_off_rel, fn_va + site_off_rel, state


def _inspect_trajectory_session_id_arm64(p, data, funcs, text_va, text_off, up):
    patch_id = p["id"]
    fn_entryoff, fn_size = funcs[TRAJ_CALLEE_FUNC]
    fn_off = text_off + fn_entryoff
    fn_va = text_va + fn_entryoff
    fn_body = bytes(data[fn_off:fn_off + fn_size])

    caller_entryoff, caller_size = funcs[TRAJ_CALLER_FUNC]
    caller_off = text_off + caller_entryoff
    caller_va = text_va + caller_entryoff
    caller_body = bytes(data[caller_off:caller_off + caller_size])

    call_offs = []
    for i in range(0, len(caller_body) - 3, 4):
        w = struct.unpack_from("<I", caller_body, i)[0]
        if up.decode_bl_target(caller_va + i, w) == fn_va:
            call_offs.append(i)
    if len(call_offs) != 1:
        raise ExtraPatchError(
            f"{patch_id}: expected 1 BL from {TRAJ_CALLER_FUNC} to {TRAJ_CALLEE_FUNC}, found {len(call_offs)}"
        )
    call_off = call_offs[0]
    has_zero_sid = False
    for i in range(max(0, call_off - 192), call_off, 4):
        w = struct.unpack_from("<I", caller_body, i)[0]
        # stp xzr, xzr, [sp, #imm]
        if (w & 0xFFC07FFF) == 0xA9007FFF:
            has_zero_sid = True
            break
    if not has_zero_sid:
        raise ExtraPatchError(
            f"{patch_id}: caller {TRAJ_CALLER_FUNC} missing arm64 empty sessionID zero-init before BL"
        )

    anchors = []
    for i in range(0, len(fn_body) - 23, 4):
        w0, w1, w2, w3, w4, w5 = struct.unpack_from("<6I", fn_body, i)
        is_vanilla_anchor = (
            (w0 & 0xFFC003FF) == 0xF94003E0
            and w1 == 0xB4000060
            and w2 == 0xB9801801
        )
        is_patched_anchor = (
            (w0 & 0xFFC003FF) == 0xF94003F0
            and w1 == 0xB4000070
            and w2 == 0xB9801A01
        )
        if (
            (is_vanilla_anchor or is_patched_anchor)
            and w3 == 0x14000002
            and w4 == 0xAA1F03E1
            and (w5 & 0xFFC003FF) == 0x910003E0
        ):
            traj_disp = ((w0 >> 10) & 0xFFF) * 8
            params_disp = (w5 >> 10) & 0xFFF
            anchors.append((i, i + 24, traj_disp, params_disp, is_patched_anchor))
    if len(anchors) != 1:
        raise ExtraPatchError(
            f"{patch_id}: {TRAJ_CALLEE_FUNC} @ {fn_va:#x} expected 1 arm64 traj/params anchor, found {len(anchors)}"
        )
    anchor_rel, pos, traj_disp, params_disp, anchor_is_patched = anchors[0]
    if (
        traj_disp % 8 != 0
        or (params_disp + 0x50) % 8 != 0
        or (traj_disp // 8) > 0xFFF
        or ((params_disp + 0x60) // 8) > 0xFFF
    ):
        raise ExtraPatchError(
            f"{patch_id}: arm64 displacements out of range (params_disp={params_disp:#x}, traj_disp={traj_disp:#x})"
        )

    zeroed_offsets = set()
    while pos + 4 <= len(fn_body):
        w = struct.unpack_from("<I", fn_body, pos)[0]
        # stp xzr, xzr, [x0, #imm] (unsigned/non-negative imm7 in bits [20:15], scaled by 8)
        if (w & 0xFFE07FFF) == 0xA9007C1F:
            zeroed_offsets.add(((w >> 15) & 0x3F) * 8)
            pos += 4
        else:
            break
    if len(zeroed_offsets) < 6 or not {0x50, 0x60}.issubset(zeroed_offsets):
        raise ExtraPatchError(
            f"{patch_id}: {TRAJ_CALLEE_FUNC} @ {fn_va:#x} zero-fill STP offsets {sorted(zeroed_offsets)} "
            f"do not cover SessionID slots 0x50/0x60"
        )

    tail_rel = None
    tail_len = 0
    patched_tail = None
    tail_state = None
    p50_disp = params_disp + 0x50
    p58_disp = params_disp + 0x58
    p60_disp = params_disp + 0x60
    want_str_p60 = 0xF90003E2 | ((p60_disp // 8) << 10)

    while pos < len(fn_body):
        # 1a. Check 6-word (24B) coalesced params+0x50 & SessionID (+0x58/+0x60) via `add x27, sp, #p50 ; stp`
        if pos + 24 <= len(fn_body):
            w0, w1, w2, w3, w4, w5 = struct.unpack_from("<6I", fn_body, pos)
            if (w0 & 0xFFC003FF) == 0xF94003E2:
                cand6 = _build_arm64_patched_tail(w0, 6)
                if fn_body[pos:pos + 24] == cand6:
                    tail_rel, tail_len, patched_tail, tail_state = pos, 24, cand6, "patched"
                    break
                want_add_p50 = 0x910003FB | (p50_disp << 10)
                if (
                    (w1 & 0xFFC003FF) == 0xF94003E3
                    and w2 == want_add_p50
                    and w3 == 0xA9000F62
                    and (w4 & 0xFFC003FF) == 0xF94003E2
                    and w5 == want_str_p60
                ):
                    sid_ptr_disp = ((w1 >> 10) & 0xFFF) * 8
                    sid_len_disp = ((w4 >> 10) & 0xFFF) * 8
                    if sid_len_disp == sid_ptr_disp + 8:
                        tail_rel, tail_len, patched_tail, tail_state = pos, 24, cand6, "vanilla"
                        break

        # 1b. Check 5-word (20B) coalesced params+0x50 & SessionID (+0x58/+0x60) via `stp x2, x3, [sp, #p50]`
        if pos + 20 <= len(fn_body):
            w0, w1, w2, w3, w4 = struct.unpack_from("<5I", fn_body, pos)
            if (w0 & 0xFFC003FF) == 0xF94003E2:
                cand5 = _build_arm64_patched_tail(w0, 5)
                if fn_body[pos:pos + 20] == cand5:
                    tail_rel, tail_len, patched_tail, tail_state = pos, 20, cand5, "patched"
                    break
                if (
                    p50_disp <= 0x1F8
                    and (w1 & 0xFFC003FF) == 0xF94003E3
                    and w2 == (0xA9000FE2 | ((p50_disp // 8) << 15))
                    and (w3 & 0xFFC003FF) == 0xF94003E2
                    and w4 == want_str_p60
                ):
                    sid_ptr_disp = ((w1 >> 10) & 0xFFF) * 8
                    sid_len_disp = ((w3 >> 10) & 0xFFF) * 8
                    if sid_len_disp == sid_ptr_disp + 8:
                        tail_rel, tail_len, patched_tail, tail_state = pos, 20, cand5, "vanilla"
                        break

        # 2a. Check 4-word (16B) paired SessionID (+0x58/+0x60) via `add x27, sp, #p58 ; stp`
        if pos + 16 <= len(fn_body):
            w0, w1, w2, w3 = struct.unpack_from("<4I", fn_body, pos)
            cand4 = _build_arm64_patched_tail(None, 4)
            if fn_body[pos:pos + 16] == cand4:
                tail_rel, tail_len, patched_tail, tail_state = pos, 16, cand4, "patched"
                break
            want_add_p58 = 0x910003FB | (p58_disp << 10)
            if (
                (w0 & 0xFFC003FF) == 0xF94003E2
                and (w1 & 0xFFC003FF) == 0xF94003E3
                and w2 == want_add_p58
                and w3 == 0xA9000F62
            ):
                sid_ptr_disp = ((w0 >> 10) & 0xFFF) * 8
                sid_len_disp = ((w1 >> 10) & 0xFFF) * 8
                if sid_len_disp == sid_ptr_disp + 8:
                    tail_rel, tail_len, patched_tail, tail_state = pos, 16, cand4, "vanilla"
                    break

        # 2b. Check 3-word (12B) paired SessionID (+0x58/+0x60) via `stp x2, x3, [sp, #p58]`
        if pos + 12 <= len(fn_body):
            w0, w1, w2 = struct.unpack_from("<3I", fn_body, pos)
            cand3 = _build_arm64_patched_tail(None, 3)
            if fn_body[pos:pos + 12] == cand3:
                tail_rel, tail_len, patched_tail, tail_state = pos, 12, cand3, "patched"
                break
            if (
                p58_disp <= 0x1F8
                and (w0 & 0xFFC003FF) == 0xF94003E2
                and (w1 & 0xFFC003FF) == 0xF94003E3
                and w2 == (0xA9000FE2 | ((p58_disp // 8) << 15))
            ):
                sid_ptr_disp = ((w0 >> 10) & 0xFFF) * 8
                sid_len_disp = ((w1 >> 10) & 0xFFF) * 8
                if sid_len_disp == sid_ptr_disp + 8:
                    tail_rel, tail_len, patched_tail, tail_state = pos, 12, cand3, "vanilla"
                    break

        # Advance past preceding 4-word field-pair copies (`ldr x2; ldr x3; add x27, sp, #dst; stp x2, x3, [x27]`)
        if pos + 16 <= len(fn_body):
            w0, w1, w2, w3 = struct.unpack_from("<4I", fn_body, pos)
            if (
                (w0 & 0xFFC003FF) == 0xF94003E2
                and (w1 & 0xFFC003FF) == 0xF94003E3
                and (w2 & 0xFFC003FF) == 0x910003FB
                and w3 == 0xA9000F62
            ):
                dst_disp = (w2 >> 10) & 0xFFF
                if params_disp <= dst_disp < p50_disp:
                    pos += 16
                    continue

        # Advance past preceding 3-word field-pair copies (`ldr x2; ldr x3; stp x2, x3, [sp, #dst]`)
        if pos + 12 <= len(fn_body):
            w0, w1, w2 = struct.unpack_from("<3I", fn_body, pos)
            if (
                (w0 & 0xFFC003FF) == 0xF94003E2
                and (w1 & 0xFFC003FF) == 0xF94003E3
                and (w2 & 0xFFE07FFF) == 0xA9000FE2
            ):
                dst_disp = ((w2 >> 15) & 0x3F) * 8
                if params_disp <= dst_disp < p50_disp:
                    pos += 12
                    continue

        # Advance past preceding 2-word single-field copies (`ldr x2, [sp,...]; str x2, [sp, #dst]`)
        if pos + 8 <= len(fn_body):
            w0, w1 = struct.unpack_from("<2I", fn_body, pos)
            if (w0 & 0xFFC003FF) == 0xF94003E2 and (w1 & 0xFFC003FF) == 0xF90003E2:
                dst_disp = ((w1 >> 10) & 0xFFF) * 8
                if params_disp <= dst_disp <= p50_disp:
                    pos += 8
                    continue
        break

    if tail_rel is None or tail_state is None or patched_tail is None:
        raise ExtraPatchError(
            f"{patch_id}: {TRAJ_CALLEE_FUNC} @ {fn_va:#x} did not reach valid arm64 SessionID slot "
            f"(params_disp={params_disp:#x}, traj_disp={traj_disp:#x})"
        )

    if (anchor_is_patched and tail_state != "patched") or (not anchor_is_patched and tail_state != "vanilla"):
        raise ExtraPatchError(
            f"{patch_id}: {TRAJ_CALLEE_FUNC} @ {fn_va:#x} has mismatched arm64 anchor/tail patch states"
        )

    region_end = tail_rel + tail_len
    if _arm64_branch_targets_interior(fn_body, anchor_rel, region_end):
        raise ExtraPatchError(
            f"{patch_id}: {TRAJ_CALLEE_FUNC} @ {fn_va:#x} has an external branch into patch region"
        )

    _verify_arm64_x16_dead(fn_body, region_end, fn_va, patch_id)
    patched_region = (
        _build_arm64_patched_anchor(traj_disp, params_disp)
        + fn_body[anchor_rel + 24:tail_rel]
        + patched_tail
    )
    return {"patched": patched_region}, fn_off + anchor_rel, fn_va + anchor_rel, tail_state


def _func_calls_target(data, text_va, text_off, arch, caller_entryoff, caller_size, target_va, up=None):
    """Return True if `caller` contains a direct call instruction targeting `target_va`."""
    caller_off = text_off + caller_entryoff
    caller_va = text_va + caller_entryoff
    body = data[caller_off:caller_off + caller_size]
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
                    return True
                pos = idx + 1
        return False
    if arch == "arm64":
        for i in range(0, len(body) - 3, 4):
            w = struct.unpack_from("<I", body, i)[0]
            if (w & 0xFC000000) not in (0x94000000, 0x14000000):
                continue
            imm26 = w & 0x3FFFFFF
            if imm26 & 0x2000000:
                imm26 -= 0x4000000
            if caller_va + i + imm26 * 4 == target_va:
                return True
        return False
    return False


def _inspect_native_trajectory_session_id(patch_id, data, funcs, text_va, text_off, arch, up=None):
    """Verify agy >= 1.5.14 native per-conversation CCPA SessionId propagation (fail-closed)."""
    if arch not in ("x86-64", "arm64"):
        raise ExtraPatchError(f"{patch_id}: no layout for arch {arch}")
    missing_common = [name for name in NATIVE_SESSION_ID_COMMON_FUNCS if name not in funcs]
    if missing_common:
        raise ExtraPatchError(
            f"{patch_id}: symbol not found: {TRAJ_CALLEE_FUNC} "
            f"(and 1.5.14+ native pipeline incomplete, missing common symbols: {', '.join(missing_common)})"
        )
    for caller, callee in NATIVE_SESSION_ID_COMMON_CALL_EDGES:
        caller_entryoff, caller_size = funcs[caller]
        callee_entryoff, _ = funcs[callee]
        callee_va = text_va + callee_entryoff
        if not _func_calls_target(data, text_va, text_off, arch, caller_entryoff, caller_size, callee_va, up=up):
            raise ExtraPatchError(
                f"{patch_id}: 1.5.14+ native SessionID common call-edge verification failed: "
                f"{caller} does not directly call {callee}"
            )

    matched_profiles = []
    missing_by_profile = []
    for prof_name, prof in NATIVE_SESSION_ID_PROFILES.items():
        missing_prof = [name for name in prof["funcs"] if name not in funcs]
        if not missing_prof:
            matched_profiles.append((prof_name, prof))
        else:
            missing_by_profile.append(f"{prof_name}: [{', '.join(missing_prof)}]")
    if not matched_profiles:
        raise ExtraPatchError(
            f"{patch_id}: 1.5.14+ native pipeline matched no version profile "
            f"(missing profile symbols — {'; '.join(missing_by_profile)})"
        )

    for prof_name, prof in matched_profiles:
        for caller, callee in prof["call_edges"]:
            if caller not in funcs or callee not in funcs:
                missing_ep = caller if caller not in funcs else callee
                raise ExtraPatchError(
                    f"{patch_id}: 1.5.14+ native SessionID [{prof_name}] call-edge endpoint missing: {missing_ep}"
                )
            caller_entryoff, caller_size = funcs[caller]
            callee_entryoff, _ = funcs[callee]
            callee_va = text_va + callee_entryoff
            if not _func_calls_target(data, text_va, text_off, arch, caller_entryoff, caller_size, callee_va, up=up):
                raise ExtraPatchError(
                    f"{patch_id}: 1.5.14+ native SessionID [{prof_name}] profile call-edge verification failed: "
                    f"{caller} does not directly call {callee}"
                )

    profile_label = ", ".join(prof_name for prof_name, _ in matched_profiles)
    anchor_entryoff, _ = funcs[NATIVE_SESSION_ID_ANCHOR_FUNC]
    anchor_off = text_off + anchor_entryoff
    anchor_va = text_va + anchor_entryoff
    return (
        {
            "patched": b"",
            "func_label": NATIVE_SESSION_ID_ANCHOR_FUNC,
            "note": f"native upstream ccpa.HashSessionID [{profile_label}]",
        },
        anchor_off,
        anchor_va,
        "native",
    )


def inspect_trajectory_session_id(p, data, funcs, text_va, text_off, arch, up=None):
    patch_id = p["id"]
    if TRAJ_CALLEE_FUNC not in funcs and TRAJ_CALLER_FUNC not in funcs:
        return _inspect_native_trajectory_session_id(patch_id, data, funcs, text_va, text_off, arch, up=up)
    if TRAJ_CALLEE_FUNC not in funcs:
        raise ExtraPatchError(f"{patch_id}: symbol not found: {TRAJ_CALLEE_FUNC}")
    if TRAJ_CALLER_FUNC not in funcs:
        raise ExtraPatchError(f"{patch_id}: caller symbol not found: {TRAJ_CALLER_FUNC}")
    if arch == "x86-64":
        return _inspect_trajectory_session_id_x86_64(p, data, funcs, text_va, text_off)
    if arch == "arm64":
        if up is None:
            up = load_upstream()
        return _inspect_trajectory_session_id_arm64(p, data, funcs, text_va, text_off, up)
    raise ExtraPatchError(f"{patch_id}: no layout for arch {arch}")

def inspect(data, up):
    text_va, text_off, arch = up.find_text_section(data)
    funcs = up.build_functions(bytes(data))
    results = []
    for p in PATCHES:
        locator = p.get("locator")
        if locator == "trajectory_session_id":
            layout, off, va, state = inspect_trajectory_session_id(p, data, funcs, text_va, text_off, arch, up=up)
            results.append((p, layout, off, va, state))
            continue
        layout = p.get(arch)
        if layout is None:
            raise ExtraPatchError(f"{p['id']}: no layout for arch {arch}")
        if p["func"] not in funcs:
            raise ExtraPatchError(f"{p['id']}: symbol not found: {p['func']}")
        entryoff, size = funcs[p["func"]]
        if size < len(layout["patched"]):
            raise ExtraPatchError(
                f"{p['id']}: {p['func']} size {size:#x} smaller than patch ({len(layout['patched'])} bytes)"
            )
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
        func_label = layout.get("func_label", p["func"])
        note = f" ({layout['note']})" if layout.get("note") else ""
        print(f"[i] {p['id']}: {func_label} @ {va:#x} state={state}{note}")

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
    if any(r[4] not in ("patched", "native") for r in again):
        print("error: post-patch verification failed")
        return 2

    after = hashlib.sha256(data).hexdigest()
    if args.dry_run:
        print(f"[*] dry run complete: sha256 {before[:12]} -> {after[:12]}")
        return 0
    try:
        up.write_atomic_signed(
            args.binary,
            data,
            src_for_entitlements=args.binary,
            make_backup=False,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"error: failed to write signed binary: {exc}")
        return 2
    with open(args.binary, "rb") as fh:
        final_sha = hashlib.sha256(fh.read()).hexdigest()
    print(f"[*] written: sha256 {before[:12]} -> {final_sha[:12]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
