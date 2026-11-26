#!/usr/bin/env python3
"""Opaque-box cache resilience, P0/P1 & 4-tier E2E gate (fake-only, 0 live quota).

Part A runs against an isolated fake-ACP gateway on <port>/<dash-port> via --fake-receipt
(zero restarts, zero ToS poisoning):
  1. p0b_large_tool_hit_and_burn_guard
  2. p0c_hot_parked_interrupt_in_place
  3. p1a_carbon_high_fallback_and_parked_tier
  4. p1a_carbon_4x_lru_protection
  5. multimodal_happy_and_boundaries
  6. cleanroom_notices_and_mcp_guards
  7. protocol_tool_roundtrips_and_soft_detach

Part B (enabled by default; skip with --no-subinstances) spawns ephemeral-port isolated
sub-instances via scripts/gw.sh + scripts/cache_resilience_acp.py inside TemporaryDirectory:
  8. p0c_orphan_relay_cap_and_streak_breaker
  9. p1a_subinstance_medium_fallback_and_transient_32603
 10. p0a_config_and_security_subinstance

Usage:
  python3 scripts/e2e_cache_resilience.py 18892 --fake-receipt /path/to/fake-instance-18892.json
"""
import argparse
import base64
import http.client
import json
import os
from pathlib import Path
import re
import select
import socket
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
import agy_upgrade as U  # noqa: E402
import e2e_common as E  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
GW_SH = REPO_ROOT / "scripts" / "gw.sh"
WRAPPER_ACP = REPO_ROOT / "scripts" / "cache_resilience_acp.py"

FORBIDDEN_SUB_PORTS = set(E.MAIN_PORTS) | {18888, 18892, 18893, 18894, 18895, 18896}
PNG_1X1_B64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="

LOG_LINE_RE = re.compile(r"^\[[^\]]+\]\s+\[([A-Z0-9_]+)\]\s+(.*)$")
PROM_LINE_RE = re.compile(
    r"^[a-zA-Z_:][a-zA-Z0-9_:]*(\{[^{}]*\})?\s+[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?$"
)


# ------------------------------------------------------------------ CLI & guardrails


def resolve_dash_port(gw_port, receipt, explicit_dash=None):
    dp = int(explicit_dash if explicit_dash is not None else ((receipt or {}).get("dash_port") or (gw_port + 1)))
    if dp in E.MAIN_PORTS or not (1024 <= dp <= 65535) or dp == gw_port:
        raise E.GateError(f"invalid or forbidden dashboard port {dp} for gateway port {gw_port}")
    return dp


def resolve_log_path(explicit_log, receipt):
    cand = explicit_log or (Path(receipt["gateway_log"]) if receipt and receipt.get("gateway_log") else None)
    if not cand or not Path(cand).is_file():
        raise E.GateError(f"gateway log not found: {cand}")
    return Path(cand)


# ------------------------------------------------------------------ HTTP / SSE / Disconnect helpers


def _parse_body_bytes(raw_bytes):
    text = raw_bytes.decode("utf-8", "replace")
    try:
        return json.loads(text)
    except ValueError:
        return text


def request_full(port, method, path, body=None, sid=None, extra_headers=None, timeout=180, include_auth=True, raw_body=None):
    """Issue HTTP request via http.client with deterministic cleanup; return (status, lower_headers, parsed)."""
    headers = E.auth_headers() if include_auth else {"Content-Type": "application/json", "anthropic-version": "2023-06-01"}
    if body is None and raw_body is None:
        headers.pop("Content-Type", None)
    if sid:
        headers["x-claude-code-session-id"] = sid
    for k, v in (extra_headers or {}).items():
        if v is None:
            headers.pop(k, None)
        else:
            headers[k] = v

    payload = None
    if raw_body is not None:
        payload = raw_body.encode("utf-8") if isinstance(raw_body, str) else raw_body
    elif body is not None:
        payload = body if isinstance(body, bytes) else json.dumps(body).encode("utf-8")

    conn = http.client.HTTPConnection("127.0.0.1", int(port), timeout=timeout)
    try:
        conn.request(method, path, body=payload, headers=headers)
        resp = conn.getresponse()
        try:
            raw = resp.read()
            hdrs = {k.lower(): v for k, v in resp.getheaders()}
            return resp.status, hdrs, _parse_body_bytes(raw)
        finally:
            resp.close()
    finally:
        conn.close()


def post_full(port, path, body, sid=None, extra_headers=None, timeout=180, include_auth=True):
    return request_full(port, "POST", path, body=body, sid=sid, extra_headers=extra_headers, timeout=timeout, include_auth=include_auth)


def get_dash(dash_port, path, extra_headers=None, timeout=30, include_auth=True):
    st, _, data = request_full(dash_port, "GET", path, body=None, extra_headers=extra_headers, timeout=timeout, include_auth=include_auth)
    return st, data


def disconnect_request(port, path, body, sid=None, wait_sec=0.25, read_first_sse=False, timeout=10, extra_headers=None, include_auth=True):
    """Send a request and cleanly disconnect its TCP socket before completion (zero ResourceWarnings)."""
    headers = E.auth_headers() if include_auth else {"Content-Type": "application/json", "anthropic-version": "2023-06-01"}
    if sid:
        headers["x-claude-code-session-id"] = sid
    if extra_headers:
        for k, v in extra_headers.items():
            if v is None:
                headers.pop(k, None)
            else:
                headers[k] = v
    payload = body if isinstance(body, bytes) else json.dumps(body).encode("utf-8")
    saw_bytes = 0
    conn = http.client.HTTPConnection("127.0.0.1", int(port), timeout=timeout)
    try:
        conn.connect()
        conn.request("POST", path, body=payload, headers=headers)
        if not read_first_sse:
            if conn.sock is not None:
                readable, _, _ = select.select([conn.sock], [], [], max(0.0, float(wait_sec)))
                if readable:
                    try:
                        chunk = conn.sock.recv(4096)
                        saw_bytes = len(chunk)
                    except OSError:
                        pass
            return {"disconnected": True, "saw_bytes": saw_bytes}

        try:
            resp = conn.getresponse()
        except TimeoutError:
            return {"disconnected": True, "saw_bytes": 0, "timed_out": True}
        try:
            deadline = time.monotonic() + max(0.0, float(wait_sec))
            while True:
                buffered = False
                fp = getattr(resp, "fp", None)
                if fp is not None and hasattr(fp, "peek"):
                    try:
                        buffered = bool(fp.peek(1))
                    except OSError:
                        buffered = False
                if not buffered:
                    rem = deadline - time.monotonic()
                    if rem <= 0:
                        break
                    if conn.sock is not None:
                        readable, _, _ = select.select([conn.sock], [], [], rem)
                        if not readable:
                            break
                try:
                    line = resp.readline()
                except TimeoutError:
                    return {"disconnected": True, "saw_bytes": saw_bytes, "status": resp.status, "timed_out": True}
                if not line:
                    break
                saw_bytes += len(line)
                if line.strip():
                    break
            return {"disconnected": True, "saw_bytes": saw_bytes, "status": resp.status}
        finally:
            resp.close()
    finally:
        conn.close()


# ------------------------------------------------------------------ Log & telemetry parsers


def log_offset(log_path):
    try:
        return Path(log_path).stat().st_size
    except OSError:
        return 0


def read_log_slice(log_path, offset=0):
    p = Path(log_path)
    if not p.is_file():
        return []
    with p.open("rb") as f:
        f.seek(max(0, int(offset)))
        raw = f.read()
    return raw.decode("utf-8", "replace").splitlines()


def wait_log_lines(log_path, offset, needle, timeout=3.0):
    deadline = time.monotonic() + float(timeout)
    while True:
        lines = [ln for ln in read_log_slice(log_path, offset) if needle in ln]
        if lines or time.monotonic() >= deadline:
            return lines
        time.sleep(0.1)


def parse_log_line(line):
    m = LOG_LINE_RE.match((line or "").strip())
    if not m:
        return "", None
    cat, rest = m.group(1), m.group(2)
    idx = rest.find("{")
    if idx >= 0:
        cand = rest[idx:]
        try:
            parsed = json.loads(cand)
            if isinstance(parsed, dict):
                return cat, parsed
        except ValueError:
            pass
    return cat, None


def filter_log_for_sid(lines, sid):
    conv_needle = f'"convKey":"sid:{sid}:'
    sid_needle = f"sid:{sid}:"
    return [ln for ln in lines if conv_needle in ln or sid_needle in ln]


def parse_prom_metrics(raw):
    bad_lines = []
    metrics = {}
    for line in (raw or "").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if not PROM_LINE_RE.match(s):
            bad_lines.append(s[:160])
            continue
        head, val_str = s.rsplit(None, 1)
        base_name = head.split("{", 1)[0]
        metrics.setdefault(base_name, []).append((head, float(val_str)))
    return bad_lines, metrics


def metric_sum(metrics, name, label_substr=None):
    entries = metrics.get(name, [])
    if not label_substr:
        return sum(val for _, val in entries)
    return sum(val for head, val in entries if label_substr in head)


def turns_for_sid(dash, sid):
    prefix = f"sid:{sid}:"
    turns = [t for t in (dash.get("gatewayTurns") or []) if str(t.get("convKey", "")).startswith(prefix)]
    return sorted(turns, key=lambda t: int(t.get("turnIndex") or 0))


def live_for_sid(dash, sid):
    prefix = f"sid:{sid}:"
    for s in dash.get("liveSessions") or []:
        if str(s.get("convKey", "")).startswith(prefix):
            return s
    return None


def steps_for_acp_sid(dash, acp_sid):
    want = str(acp_sid or "").lower()
    if not want:
        return []
    steps = [s for s in (dash.get("recentDbSteps") or []) if str(s.get("sessionId", "")).lower() == want]
    return sorted(steps, key=lambda s: int(s.get("stepIdx") or 0))


# ------------------------------------------------------------------ Fail-closed verdict evaluators


def eval_p0b_verdict(
    *,
    turns,
    db_steps,
    prefix_warn_headers,
    zero3_warn_header,
    stream_warn_header,
    recovery_warn_header,
    low50k_warn_headers,
    burn_warn_delta_after_prefix,
    burn_warn_delta_after_zero,
    cache_warnings_with_sid_after_zero,
    cache_warnings_with_sid_after_recovery,
    sid_log_lines,
):
    turn_misses = [t.get("cacheMiss") for t in turns]
    step_misses = [s.get("cacheMiss") for s in db_steps]
    has_burn_log = any("[CACHE_BURN_WARNING]" in ln for ln in sid_log_lines)
    ok = (
        len(turns) == 11
        and turn_misses[0] == "cold_start"
        and turn_misses[1] == ""
        and turn_misses[2:6] == ["prefix_only", "prefix_only", "prefix_only", "prefix_only"]
        and turn_misses[6:10] == ["zero", "zero", "zero", "zero"]
        and turn_misses[10] == ""
        and len(step_misses) == len(turn_misses)
        and step_misses == turn_misses
        and all(not h for h in prefix_warn_headers)
        and burn_warn_delta_after_prefix == 0
        and "consecutive-zero-cache" in (zero3_warn_header or "")
        and "streak=3" in (zero3_warn_header or "")
        and "consecutive-zero-cache" in (stream_warn_header or "")
        and not recovery_warn_header
        and len(low50k_warn_headers) == 4
        and all(not h for h in low50k_warn_headers)
        and burn_warn_delta_after_zero >= 1
        and cache_warnings_with_sid_after_zero
        and not cache_warnings_with_sid_after_recovery
        and has_burn_log
    )
    return ok, {
        "turn_misses": turn_misses,
        "step_misses": step_misses,
        "burn_warn_delta_after_prefix": burn_warn_delta_after_prefix,
        "burn_warn_delta_after_zero": burn_warn_delta_after_zero,
        "zero3_warn_header": zero3_warn_header,
        "stream_warn_header": stream_warn_header,
        "recovery_warn_header": recovery_warn_header,
        "has_burn_log": has_burn_log,
    }


def eval_p0c_interrupt_verdict(*, orig_sid, turns, live, turn2_text, turn3_text, sid_log_lines):
    has_parked_resume = any("[PARKED_MCP_INTERRUPT_RESUME]" in ln or "[PARKED_MCP_RESUME]" in ln for ln in sid_log_lines)
    has_rollover = any("[COMPACTION_ROLLOVER]" in ln or "rollover_aborted_tool" in ln for ln in sid_log_lines)
    ok = (
        bool(orig_sid)
        and live is not None
        and live.get("sessionId") == orig_sid
        and int(live.get("compactionRollovers") or 0) == 0
        and len(turns) == 3
        and turns[1].get("mode") == "parked_mcp_resume"
        and turns[2].get("mode") == "sticky_delta"
        and all(t.get("sessionId") == orig_sid for t in turns)
        and "INTERRUPTED_NEW_QUESTION" in (turn2_text or "")
        and "TOOL_DONE:" in (turn2_text or "")
        and "FOLLOWUP_AFTER_INTERRUPT" in (turn3_text or "")
        and "resumes=0" in (turn3_text or "")
        and has_parked_resume
        and not has_rollover
    )
    return ok, {
        "orig_sid": orig_sid,
        "turn_modes": [t.get("mode") for t in turns],
        "turn_sids": [t.get("sessionId") for t in turns],
        "compaction_rollovers": (live or {}).get("compactionRollovers"),
        "has_parked_resume": has_parked_resume,
        "has_rollover": has_rollover,
    }


def eval_p1a_fallback_verdict(
    *,
    cap_sid,
    cap_turn2_text,
    cap_turns,
    cap_live,
    cap_log_lines,
    park_sid,
    park_turns,
    park_turn2_text,
    park_turn3_text,
    park_live,
    rpc_sid,
    rpc_529_status,
    rpc_recovery_turns,
    rpc_live,
    rpc_log_lines,
    rpc_400_status,
):
    has_cap_log = any("[HIGH_TIER_CAPACITY_FALLBACK]" in ln and '"toTier":"medium"' in ln for ln in cap_log_lines)
    has_rpc_hib = any("[ACP_HIBERNATE]" in ln and '"reason":"prompt_rpc_error"' in ln for ln in rpc_log_lines)
    ok = (
        bool(cap_sid)
        and "FALLBACK_MEDIUM_OK" in (cap_turn2_text or "")
        and len(cap_turns) >= 2
        and cap_turns[-1].get("sessionId") == cap_sid
        and int((cap_live or {}).get("compactionRollovers") or 0) == 0
        and has_cap_log
        and bool(park_sid)
        and len(park_turns) >= 3
        and park_turns[1].get("mode") == "parked_mcp_resume"
        and park_turns[1].get("thoughtLevel") == "high"
        and park_turns[1].get("sessionId") == park_sid
        and "TOOL_DONE:" in (park_turn2_text or "")
        and "FAKE_OK model=gemini-4-carbon tier=low" in (park_turn3_text or "")
        and park_turns[2].get("thoughtLevel") == "low"
        and park_turns[2].get("sessionId") == park_sid
        and (park_live or {}).get("thoughtLevel") == "low"
        and int((park_live or {}).get("compactionRollovers") or 0) == 0
        and bool(rpc_sid)
        and rpc_529_status in (503, 529)
        and not has_rpc_hib
        and len(rpc_recovery_turns) >= 2
        and rpc_recovery_turns[-1].get("sessionId") == rpc_sid
        and rpc_recovery_turns[-1].get("mode") == "sticky_delta"
        and int((rpc_live or {}).get("compactionRollovers") or 0) == 0
        and rpc_400_status == 400
    )
    return ok, {
        "cap_sid": cap_sid,
        "cap_last_sid": cap_turns[-1].get("sessionId") if cap_turns else None,
        "has_cap_log": has_cap_log,
        "park_sid": park_sid,
        "park_turn2_mode": park_turns[1].get("mode") if len(park_turns) > 1 else None,
        "park_turn2_tier": park_turns[1].get("thoughtLevel") if len(park_turns) > 1 else None,
        "rpc_529_status": rpc_529_status,
        "has_rpc_hib": has_rpc_hib,
        "rpc_recovery_mode": rpc_recovery_turns[-1].get("mode") if rpc_recovery_turns else None,
        "rpc_400_status": rpc_400_status,
    }


def eval_lru_verdict(*, carbon_sid, carbon_live_after_evict, carbon_followup_turns, lru_log_lines):
    has_lru_evict = any("[ACP_HIBERNATE]" in ln and '"reason":"lru_cap_' in ln for ln in lru_log_lines)
    ok = (
        bool(carbon_sid)
        and carbon_live_after_evict is not None
        and carbon_live_after_evict.get("sessionId") == carbon_sid
        and len(carbon_followup_turns) >= 2
        and carbon_followup_turns[-1].get("sessionId") == carbon_sid
        and carbon_followup_turns[-1].get("mode") == "sticky_delta"
        and has_lru_evict
    )
    return ok, {
        "carbon_sid": carbon_sid,
        "survived_in_live": carbon_live_after_evict is not None,
        "survived_sid": (carbon_live_after_evict or {}).get("sessionId"),
        "followup_mode": carbon_followup_turns[-1].get("mode") if carbon_followup_turns else None,
        "has_lru_evict": has_lru_evict,
    }


def eval_tier5_double_stripped_verdict(protocol_records):
    """Verify each protocol's 4-turn double-stripped trace stayed on Turn-3 session on Turn 4 without rotation."""
    details = []
    for rec in protocol_records or []:
        turns = rec.get("turns") or []
        live = rec.get("live") or {}
        st4 = rec.get("turn4_status")
        t3_sid = turns[-2].get("sessionId") if len(turns) >= 2 else ""
        t4_sid = turns[-1].get("sessionId") if len(turns) >= 1 else ""
        t4_mode = turns[-1].get("mode") if len(turns) >= 1 else ""
        rolls = int(live.get("compactionRollovers") or 0)
        rec_ok = (
            st4 == 200
            and len(turns) == 3
            and turns[0].get("mode") == "cold_start"
            and turns[1].get("mode") == "compaction_rollover"
            and t4_mode == "sticky_delta"
            and bool(t3_sid)
            and t4_sid == t3_sid
            and rolls == 1
        )
        details.append({
            "protocol": rec.get("protocol"),
            "ok": rec_ok,
            "turn4_status": st4,
            "turns_len": len(turns),
            "rolls": rolls,
            "t3_sid": t3_sid,
            "t4_sid": t4_sid,
            "t4_mode": t4_mode,
        })
    ok = bool(details) and all(d["ok"] for d in details)
    return ok, {"protocols": details}


# ------------------------------------------------------------------ Part A scenarios (18892/18893)


def _msg_post(port, sid, model, msgs, *, tools=True, stream=False, extra=None, max_tokens=1024, system=E.SYS):
    body = {"model": model, "max_tokens": max_tokens, "system": system, "messages": msgs, "stream": stream}
    if tools:
        body["tools"] = [E.BASH_TOOL]
    if extra:
        body.update(extra)
    return post_full(port, "/v1/messages", body, sid=sid)


def scenario_p0b_large_tool_hit_and_burn_guard(port, dash_port, log_path):
    """P0-B: cr+2500>=prevTotal hit guard, prefix_only excluded from ZeroCacheStreak, zero x3 warns."""
    off = log_offset(log_path)
    _, met0_raw = get_dash(dash_port, "/metrics")
    _, met0 = parse_prom_metrics(met0_raw if isinstance(met0_raw, str) else "")
    warn0 = metric_sum(met0, "agy_gateway_cache_burn_warnings_total")

    sid = f"e2e-p0b-{time.time_ns()}"
    msgs = []
    specs = [
        ("[FAKE_INPUT_TOKENS:200000] [FAKE_CACHE_READ:0] [FAKE_OUTPUT_TOKENS:20]", False),      # 1 cold_start (200K)
        ("[FAKE_INPUT_TOKENS:30000] [FAKE_CACHE_READ:197500] [FAKE_OUTPUT_TOKENS:20]", False), # 2 exact cr+2500==prevTotal hit "" (227.5K)
        ("[FAKE_INPUT_TOKENS:30000] [FAKE_CACHE_READ:224999] [FAKE_OUTPUT_TOKENS:20]", False), # 3 exact cr+2500==prevTotal-1 -> prefix_only (255K-1)
        ("[FAKE_PREFIX_250K]", False),  # 4 prefix_only (260K)
        ("[FAKE_PREFIX_250K]", False),  # 5 prefix_only (265K)
        ("[FAKE_PREFIX_250K]", False),  # 6 prefix_only (270K, must NOT warn)
        ("[FAKE_ZERO_250K]", False),    # 7 zero #1
        ("[FAKE_ZERO_250K]", False),    # 8 zero #2
        ("[FAKE_ZERO_250K]", False),    # 9 zero #3 -> warn!
        ("[FAKE_ZERO_250K]", True),     # 10 zero #4 (stream=True pre-set header!)
        ("[FAKE_HIT_250K]", False),     # 11 hit -> clears warning!
    ]
    prefix_warn_headers = []
    zero3_warn_header = ""
    stream_warn_header = ""
    recovery_warn_header = ""
    burn_warn_after_prefix = 0.0
    cache_warnings_after_zero = []

    for idx, (marker, stream) in enumerate(specs, 1):
        msgs.append({"role": "user", "content": f"Turn {idx} {marker}"})
        st, hdrs, data = _msg_post(port, sid, "gemini-4-carbon", msgs, stream=stream)
        if st != 200:
            return {"ok": False, "turn": idx, "status": st, "reason": "HTTP turn failed"}
        warn_hdr = hdrs.get("x-agy-cache-warning", "")
        if idx in (2, 3, 4, 5, 6):
            prefix_warn_headers.append(warn_hdr)
        if idx == 6:
            _, met_p_raw = get_dash(dash_port, "/metrics")
            _, met_p = parse_prom_metrics(met_p_raw if isinstance(met_p_raw, str) else "")
            burn_warn_after_prefix = metric_sum(met_p, "agy_gateway_cache_burn_warnings_total") - warn0
        if idx == 9:
            zero3_warn_header = warn_hdr
        if idx == 10:
            stream_warn_header = warn_hdr
            _, dash_z = get_dash(dash_port, "/api/dashboard?refresh=1")
            if isinstance(dash_z, dict):
                cache_warnings_after_zero = [
                    w for w in (dash_z.get("cacheWarnings") or [])
                    if str(w.get("convKey", "")).startswith(f"sid:{sid}:")
                ]
        if idx == 11:
            recovery_warn_header = warn_hdr

        if stream:
            asst_text = "".join(
                ((d.get("delta") or {}).get("text") or "")
                for _, d in E.sse_events(data if isinstance(data, str) else "")
                if isinstance(d, dict) and d.get("type") == "content_block_delta"
            )
            msgs.append({"role": "assistant", "content": [{"type": "text", "text": asst_text or "OK"}]})
        else:
            msgs.append({"role": "assistant", "content": data["content"]})

    # Verify < 80K default threshold suppression across 4 turns (1 cold_start + 3 zero = streak 3 if broken)
    sid_50k = f"e2e-p0b50k-{time.time_ns()}"
    msgs_50k = []
    low50k_warn_headers = []
    for idx in range(1, 5):
        msgs_50k.append({"role": "user", "content": f"Low50k {idx} [FAKE_ZERO_50K]"})
        st50, hdrs50, d50 = _msg_post(port, sid_50k, "gemini-4-carbon", msgs_50k)
        if st50 != 200 or not isinstance(d50, dict):
            return {"ok": False, "reason": f"50k turn {idx} failed: {st50}"}
        low50k_warn_headers.append(hdrs50.get("x-agy-cache-warning", ""))
        msgs_50k.append({"role": "assistant", "content": d50["content"]})

    _, dash = get_dash(dash_port, "/api/dashboard?refresh=1")
    _, met_end_raw = get_dash(dash_port, "/metrics")
    _, met_end = parse_prom_metrics(met_end_raw if isinstance(met_end_raw, str) else "")
    burn_warn_after_zero = metric_sum(met_end, "agy_gateway_cache_burn_warnings_total") - warn0
    if not isinstance(dash, dict):
        return {"ok": False, "reason": "dashboard unavailable"}

    turns = turns_for_sid(dash, sid)
    acp_sid = (live_for_sid(dash, sid) or {}).get("sessionId") or (turns[-1].get("sessionId") if turns else "")
    db_steps = steps_for_acp_sid(dash, acp_sid)
    cache_warnings_after_rec = [
        w for w in (dash.get("cacheWarnings") or [])
        if str(w.get("convKey", "")).startswith(f"sid:{sid}:")
    ]
    sid_logs = filter_log_for_sid(wait_log_lines(log_path, off, "[CACHE_BURN_WARNING]", timeout=1.5), sid)
    if not sid_logs:
        sid_logs = filter_log_for_sid(read_log_slice(log_path, off), sid)

    ok, detail = eval_p0b_verdict(
        turns=turns,
        db_steps=db_steps,
        prefix_warn_headers=prefix_warn_headers,
        zero3_warn_header=zero3_warn_header,
        stream_warn_header=stream_warn_header,
        recovery_warn_header=recovery_warn_header,
        low50k_warn_headers=low50k_warn_headers,
        burn_warn_delta_after_prefix=burn_warn_after_prefix,
        burn_warn_delta_after_zero=burn_warn_after_zero,
        cache_warnings_with_sid_after_zero=cache_warnings_after_zero,
        cache_warnings_with_sid_after_recovery=cache_warnings_after_rec,
        sid_log_lines=sid_logs,
    )
    return {"ok": ok, **detail, "generation_requests": 0}


def scenario_p0c_hot_parked_interrupt_in_place(port, dash_port, log_path):
    """P0-C: hot-parked tool interrupt without tool_result resumes in-place with 0 rollover."""
    off = log_offset(log_path)
    sid = f"e2e-p0c-int-{time.time_ns()}"
    u1 = {"role": "user", "content": "[FAKE_TOOL_CALL] inspect repository files"}
    st1, _, d1 = _msg_post(port, sid, "gemini-4-carbon", [u1])
    if st1 != 200 or not isinstance(d1, dict) or d1.get("stop_reason") != "tool_use":
        return {"ok": False, "status": st1, "reason": "turn 1 did not park tool_use"}

    _, dash1 = get_dash(dash_port, "/api/dashboard?refresh=1")
    orig_sid = (live_for_sid(dash1, sid) or {}).get("sessionId") if isinstance(dash1, dict) else ""

    # Turn 2: drop tool_use and send new user prompt without substring "cancelled"
    u2 = {"role": "user", "content": "INTERRUPTED_NEW_QUESTION [ECHO_PROMPT]"}
    st2, _, d2 = _msg_post(port, sid, "gemini-4-carbon", [u1, u2])
    t2_txt = E.anthropic_text(d2)
    if st2 != 200 or not isinstance(d2, dict) or d2.get("stop_reason") != "end_turn":
        return {"ok": False, "status": st2, "stop": (d2 or {}).get("stop_reason") if isinstance(d2, dict) else None, "reason": "turn 2 interrupt failed"}

    u3 = {"role": "user", "content": "FOLLOWUP_AFTER_INTERRUPT [ECHO_PROMPT] [ECHO_RELAY_META]"}
    st3, _, d3 = _msg_post(port, sid, "gemini-4-carbon", [u1, u2, {"role": "assistant", "content": d2["content"]}, u3])
    t3_txt = E.anthropic_text(d3)
    if st3 != 200 or not isinstance(d3, dict) or d3.get("stop_reason") != "end_turn":
        return {"ok": False, "status": st3, "reason": "turn 3 followup failed"}

    _, dash = get_dash(dash_port, "/api/dashboard?refresh=1")
    if not isinstance(dash, dict):
        return {"ok": False, "reason": "dashboard unavailable"}
    turns = turns_for_sid(dash, sid)
    live = live_for_sid(dash, sid)
    sid_logs = filter_log_for_sid(read_log_slice(log_path, off), sid)
    ok, detail = eval_p0c_interrupt_verdict(
        orig_sid=orig_sid,
        turns=turns,
        live=live,
        turn2_text=t2_txt,
        turn3_text=t3_txt,
        sid_log_lines=sid_logs,
    )
    return {"ok": ok, **detail, "generation_requests": 0}


def scenario_p1a_carbon_high_fallback_and_parked_tier(port, dash_port, log_path):
    """P1-A: high->medium capacity fallback, parked_mcp_resume tier switch, and RPC 529 warm preservation."""
    off = log_offset(log_path)

    # Part A: Carbon implicit high (no -high suffix -> ExplicitTier == false) -> medium fallback
    sid_cap = f"e2e-p1a-cap-{time.time_ns()}"
    high_cfg = {"thinking": {"type": "enabled", "budget_tokens": 32000}}
    m_cap = [{"role": "user", "content": "warmup carbon high"}]
    st_c1, _, d_c1 = _msg_post(port, sid_cap, "gemini-4-carbon", m_cap, max_tokens=40000, extra=high_cfg)
    if st_c1 != 200 or "tier=high" not in E.anthropic_text(d_c1):
        return {"ok": False, "status": st_c1, "text": E.anthropic_text(d_c1), "reason": "cap warmup failed"}
    _, dash_c1 = get_dash(dash_port, "/api/dashboard?refresh=1")
    cap_sid = (live_for_sid(dash_c1, sid_cap) or {}).get("sessionId") if isinstance(dash_c1, dict) else ""

    m_cap.append({"role": "assistant", "content": d_c1["content"]})
    m_cap.append({"role": "user", "content": "[FAKE_HIGH_429_MEDIUM_OK] capacity fallback"})
    st_c2, _, d_c2 = _msg_post(port, sid_cap, "gemini-4-carbon", m_cap, max_tokens=40000, extra=high_cfg)
    cap_t2_txt = E.anthropic_text(d_c2)

    # Part B: parked_mcp_resume tier switch (high -> low on tool_result, same sessionId)
    sid_park = f"e2e-p1a-park-{time.time_ns()}"
    mp1 = {"role": "user", "content": "[FAKE_TOOL_CALL] park at high"}
    st_p1, _, d_p1 = _msg_post(port, sid_park, "gemini-4-carbon", [mp1], max_tokens=40000, extra=high_cfg)
    tus = [c for c in (d_p1.get("content", []) if isinstance(d_p1, dict) else []) if c.get("type") == "tool_use"]
    if st_p1 != 200 or not tus:
        return {"ok": False, "status": st_p1, "reason": "parked tier turn 1 did not yield tool_use"}
    _, dash_p1 = get_dash(dash_port, "/api/dashboard?refresh=1")
    park_sid = (live_for_sid(dash_p1, sid_park) or {}).get("sessionId") if isinstance(dash_p1, dict) else ""

    msgs_p = [
        mp1,
        {"role": "assistant", "content": d_p1["content"]},
        {"role": "user", "content": [{"type": "tool_result", "tool_use_id": tus[0]["id"], "content": "FAKE_TOOL_OK"}]},
    ]
    low_cfg = {"output_config": {"effort": "low"}}
    st_p2, _, d_p2 = _msg_post(port, sid_park, "gemini-4-carbon", msgs_p, extra=low_cfg)
    park_t2_txt = E.anthropic_text(d_p2)
    msgs_p.append({"role": "assistant", "content": d_p2["content"] if isinstance(d_p2, dict) else []})
    msgs_p.append({"role": "user", "content": "confirm tier persisted"})
    st_p3, _, d_p3 = _msg_post(port, sid_park, "gemini-4-carbon", msgs_p, extra=low_cfg)
    park_t3_txt = E.anthropic_text(d_p3)

    # Part C: [FAKE_RPC_529] preserves warm session vs [FAKE_RPC_400] fast-fails
    sid_rpc = f"e2e-p1a-rpc-{time.time_ns()}"
    mr1 = {"role": "user", "content": "warmup rpc session"}
    st_r1, _, d_r1 = _msg_post(port, sid_rpc, "gemini-4-carbon-high", [mr1])
    if st_r1 != 200:
        return {"ok": False, "status": st_r1, "reason": "rpc warmup failed"}
    _, dash_r1 = get_dash(dash_port, "/api/dashboard?refresh=1")
    rpc_sid = (live_for_sid(dash_r1, sid_rpc) or {}).get("sessionId") if isinstance(dash_r1, dict) else ""

    mr_hist = [mr1, {"role": "assistant", "content": d_r1["content"]}]
    st_r529, _, _ = _msg_post(port, sid_rpc, "gemini-4-carbon-high", mr_hist + [{"role": "user", "content": "[FAKE_RPC_529] transient overload"}])
    st_r_rec, _, _ = _msg_post(port, sid_rpc, "gemini-4-carbon-high", mr_hist + [{"role": "user", "content": "recover after 529"}])
    if st_r_rec != 200:
        return {"ok": False, "status": st_r_rec, "reason": "rpc recovery turn failed"}

    st_r400, _, _ = _msg_post(port, f"e2e-p1a-400-{time.time_ns()}", "gemini-4-carbon-high", [{"role": "user", "content": "[FAKE_RPC_400] bad budget"}])

    _, dash = get_dash(dash_port, "/api/dashboard?refresh=1")
    if not isinstance(dash, dict):
        return {"ok": False, "reason": "dashboard unavailable"}
    all_logs = read_log_slice(log_path, off)
    ok, detail = eval_p1a_fallback_verdict(
        cap_sid=cap_sid,
        cap_turn2_text=cap_t2_txt,
        cap_turns=turns_for_sid(dash, sid_cap),
        cap_live=live_for_sid(dash, sid_cap),
        cap_log_lines=filter_log_for_sid(all_logs, sid_cap),
        park_sid=park_sid,
        park_turns=turns_for_sid(dash, sid_park),
        park_turn2_text=park_t2_txt,
        park_turn3_text=park_t3_txt,
        park_live=live_for_sid(dash, sid_park),
        rpc_sid=rpc_sid,
        rpc_529_status=st_r529,
        rpc_recovery_turns=turns_for_sid(dash, sid_rpc),
        rpc_live=live_for_sid(dash, sid_rpc),
        rpc_log_lines=filter_log_for_sid(all_logs, sid_rpc),
        rpc_400_status=st_r400,
    )
    return {"ok": ok, **detail, "generation_requests": 0}


def scenario_p1a_carbon_4x_lru_protection(port, dash_port, log_path):
    """P1-A: Carbon 4x cost-weighted LRU protects warm Carbon ahead of newer warm Flash workers."""
    off = log_offset(log_path)
    _, dash0 = get_dash(dash_port, "/api/dashboard?refresh=1")
    if not isinstance(dash0, dict):
        return {"ok": False, "reason": "dashboard unavailable"}
    max_workers = int((dash0.get("gateway") or {}).get("maxLiveWorkers") or 8)

    sid_c = f"e2e-lru-carb-{time.time_ns()}"
    c_msg1 = {"role": "user", "content": "[FAKE_INPUT_TOKENS:4000] [FAKE_CACHE_READ:80000] seed carbon"}
    st_c, _, d_c = _msg_post(port, sid_c, "gemini-4-carbon", [c_msg1])
    if st_c != 200 or not isinstance(d_c, dict):
        return {"ok": False, "status": st_c, "reason": "carbon seed failed"}
    _, dash_c = get_dash(dash_port, "/api/dashboard?refresh=1")
    carbon_sid = (live_for_sid(dash_c, sid_c) or {}).get("sessionId") if isinstance(dash_c, dict) else ""

    for i in range(max_workers - 1):
        sid_f = f"e2e-lru-fl-{i}-{time.time_ns()}"
        st_f, _, _ = _msg_post(port, sid_f, "gemini-3.8-flash-low", [{"role": "user", "content": "[FAKE_INPUT_TOKENS:4000] [FAKE_CACHE_READ:80000] flash warm"}])
        if st_f != 200:
            return {"ok": False, "status": st_f, "reason": f"flash warm {i} failed"}

    sid_trig = f"e2e-lru-trig-{time.time_ns()}"
    st_t, _, _ = _msg_post(port, sid_trig, "gemini-3.8-flash-low", [{"role": "user", "content": "trigger lru cap"}])
    if st_t != 200:
        return {"ok": False, "status": st_t, "reason": "trigger worker failed"}

    _, dash_mid = get_dash(dash_port, "/api/dashboard?refresh=1")
    carbon_live_after = live_for_sid(dash_mid, sid_c) if isinstance(dash_mid, dict) else None

    c_msg2 = {"role": "user", "content": "[FAKE_INPUT_TOKENS:1000] [FAKE_CACHE_READ:84000] followup carbon"}
    st_c2, _, _ = _msg_post(port, sid_c, "gemini-4-carbon", [c_msg1, {"role": "assistant", "content": d_c["content"]}, c_msg2])
    if st_c2 != 200:
        return {"ok": False, "status": st_c2, "reason": "carbon followup failed"}

    _, dash_end = get_dash(dash_port, "/api/dashboard?refresh=1")
    lru_logs = read_log_slice(log_path, off)
    ok, detail = eval_lru_verdict(
        carbon_sid=carbon_sid,
        carbon_live_after_evict=carbon_live_after,
        carbon_followup_turns=turns_for_sid(dash_end, sid_c) if isinstance(dash_end, dict) else [],
        lru_log_lines=lru_logs,
    )
    return {"ok": ok, "max_workers": max_workers, **detail, "generation_requests": 0}


def scenario_multimodal_happy_and_boundaries(port):
    """Feature #3 Tier 1 & 2: valid 1x1 PNG across 4 protocols, count_tokens image bump, and 400 guards."""
    cases = []

    st_a, _, d_a = post_full(port, "/v1/messages", {
        "model": "gemini-3.8-flash-low",
        "max_tokens": 128,
        "system": E.SYS,
        "messages": [{"role": "user", "content": [
            {"type": "text", "text": "describe image"},
            {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": PNG_1X1_B64}},
        ]}],
    }, sid=f"e2e-mm-a-{time.time_ns()}")
    cases.append(("anthropic_png", st_a == 200 and "FAKE_OK" in E.anthropic_text(d_a), st_a))

    st_c, _, d_c = post_full(port, "/v1/chat/completions", {
        "model": "gemini-3.8-flash-low",
        "messages": [
            {"role": "system", "content": E.SYS},
            {"role": "user", "content": [
                {"type": "text", "text": "describe image"},
                {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{PNG_1X1_B64}"}},
            ]},
        ],
    }, sid=f"e2e-mm-c-{time.time_ns()}")
    cases.append(("chat_png", st_c == 200 and "FAKE_OK" in E.chat_text(d_c), st_c))

    st_r, _, d_r = post_full(port, "/v1/responses", {
        "model": "gemini-3.8-flash-low",
        "instructions": E.SYS,
        "input": [{"type": "message", "role": "user", "content": [
            {"type": "input_text", "text": "describe image"},
            {"type": "input_image", "image_url": f"data:image/png;base64,{PNG_1X1_B64}"},
        ]}],
    }, sid=f"e2e-mm-r-{time.time_ns()}")
    cases.append(("responses_png", st_r == 200 and "FAKE_OK" in E.responses_text(d_r), st_r))

    st_g, _, d_g = post_full(port, "/v1beta/models/gemini-3.8-flash:generateContent", {
        "systemInstruction": {"parts": [{"text": E.SYS}]},
        "contents": [{"role": "user", "parts": [
            {"text": "describe image"},
            {"inlineData": {"mimeType": "image/png", "data": PNG_1X1_B64}},
        ]}],
    }, sid=f"e2e-mm-g-{time.time_ns()}")
    cases.append(("gemini_png", st_g == 200 and "FAKE_OK" in E.gemini_text(d_g), st_g))

    st_ct0, _, d_ct0 = post_full(port, "/v1/messages/count_tokens", {
        "model": "gemini-3.8-flash-low",
        "messages": [{"role": "user", "content": [{"type": "text", "text": "describe image"}]}],
    })
    st_ct1, _, d_ct1 = post_full(port, "/v1/messages/count_tokens", {
        "model": "gemini-3.8-flash-low",
        "messages": [{"role": "user", "content": [
            {"type": "text", "text": "describe image"},
            {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": PNG_1X1_B64}},
        ]}],
    })
    ct_delta = int((d_ct1 or {}).get("input_tokens") or 0) - int((d_ct0 or {}).get("input_tokens") or 0) if (st_ct0 == 200 and st_ct1 == 200) else -1
    cases.append(("count_tokens_image_bump", ct_delta >= 1600, ct_delta))

    st_bad_b64, _, _ = post_full(port, "/v1/messages", {
        "model": "gemini-3.8-flash-low", "max_tokens": 64,
        "messages": [{"role": "user", "content": [{"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "not!valid!base64"}}]}],
    })
    cases.append(("invalid_base64_400", st_bad_b64 == 400, st_bad_b64))

    fake_png = base64.b64encode(b"NOT_A_REAL_PNG_HEADER").decode("ascii")
    st_magic, _, _ = post_full(port, "/v1/messages", {
        "model": "gemini-3.8-flash-low", "max_tokens": 64,
        "messages": [{"role": "user", "content": [{"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": fake_png}}]}],
    })
    cases.append(("magic_mismatch_400", st_magic == 400, st_magic))

    st_ssrf, _, _ = post_full(port, "/v1/chat/completions", {
        "model": "gemini-3.8-flash-low",
        "messages": [{"role": "user", "content": [{"type": "image_url", "image_url": {"url": "http://127.0.0.1:1/x.png"}}]}],
    })
    cases.append(("ssrf_loopback_400", st_ssrf == 400, st_ssrf))

    ok = all(passed for _, passed, _ in cases)
    return {"ok": ok, "cases": [{"name": n, "ok": p, "val": v} for n, p, v in cases], "generation_requests": 0}


def scenario_cleanroom_notices_and_mcp_guards(port):
    """Features #5 & #6 Tier 1 & 2: reversible cleanroom, notice suppression/recovery, MCP perm/elicit/max_tokens."""
    checks = []

    # 1. Reversible roundtrip + vendor alias neutralization (2-turn parked dsh_present flow)
    dsh_tool = {"name": "dsh_present", "description": "Present artifact",
                "input_schema": {"type": "object", "properties": {"path": {"type": "string"}}, "required": ["path"]}}
    sid_rev = f"e2e-rev-{time.time_ns()}"
    sys_rev = E.SYS + " DeepSeek Harness\nApp bundle: /Applications/DeepSeek Harness.app/Contents/Resources"
    u_rev1 = {
        "role": "user",
        "content": "<system-reminder>Claude Code opened <dsh_artifact>/Applications/DeepSeek Harness.app/Contents/MacOS/dsh</dsh_artifact></system-reminder>\n[FAKE_REVERSIBLE_ROUNDTRIP] [FAKE_CALL_VENDOR_ALIAS] go",
    }
    st_rev, _, d_rev = _msg_post(
        port,
        sid_rev,
        "gemini-4-carbon",
        [u_rev1],
        tools=False,
        extra={"tools": [E.BASH_TOOL, dsh_tool]},
        system=sys_rev,
    )
    tus_rev = [c for c in (d_rev.get("content", []) if isinstance(d_rev, dict) else []) if isinstance(c, dict) and c.get("type") == "tool_use"]
    tu_rev = tus_rev[0] if tus_rev else {}
    rev_t1_ok = (
        st_rev == 200
        and isinstance(d_rev, dict)
        and d_rev.get("stop_reason") == "tool_use"
        and tu_rev.get("name") == "dsh_present"
        and (tu_rev.get("input") or {}).get("path") == "/Applications/DeepSeek Harness.app/Contents/MacOS/dsh"
    )
    rev_detail = f"t1={st_rev}:{(d_rev or {}).get('stop_reason') if isinstance(d_rev, dict) else None}:{tu_rev.get('name')}"
    rev_ok = False
    if rev_t1_ok:
        st_rev2, _, d_rev2 = _msg_post(
            port,
            sid_rev,
            "gemini-4-carbon",
            [
                u_rev1,
                {"role": "assistant", "content": d_rev["content"]},
                {"role": "user", "content": [{
                    "type": "tool_result",
                    "tool_use_id": tu_rev["id"],
                    "content": "<system-reminder>Claude Code read /Applications/DeepSeek Harness.app/Contents/MacOS/dsh</system-reminder>\n<dsh_artifact>hi</dsh_artifact>",
                }]},
            ],
            tools=False,
            extra={"tools": [E.BASH_TOOL, dsh_tool]},
            system=sys_rev,
        )
        txt_rev2 = E.anthropic_text(d_rev2)
        rev_ok = (
            st_rev2 == 200
            and isinstance(d_rev2, dict)
            and d_rev2.get("stop_reason") == "end_turn"
            and "UPSTREAM_CLEAN=true" in txt_rev2
            and "TOOL_SEG=true" in txt_rev2
            and "/Applications/DeepSeek Harness.app" in txt_rev2
            and "<dsh_artifact>" in txt_rev2
        )
        rev_detail = txt_rev2[:160]
    checks.append(("reversible_roundtrip", rev_ok, rev_detail))

    # 2. Notice suppression ([FAKE_NOTICE_NET] -> 503) & same-chunk retry recovery -> 200
    st_net, _, _ = _msg_post(port, f"e2e-net-{time.time_ns()}", "gemini-3.8-flash-low", [{"role": "user", "content": "[FAKE_NOTICE_NET]"}])
    checks.append(("notice_net_503", st_net == 503, st_net))

    st_rec, _, d_rec = _msg_post(port, f"e2e-rec-{time.time_ns()}", "gemini-3.8-flash-low", [{"role": "user", "content": "[FAKE_RETRY_SAME_CHUNK_RECOVER]"}])
    txt_rec = E.anthropic_text(d_rec)
    checks.append(("retry_same_chunk_recover", st_rec == 200 and "Recovered after retry" in txt_rec and "API error (attempt 1)" not in txt_rec, txt_rec[:120]))

    st_lic, _, _ = _msg_post(port, f"e2e-lic-{time.time_ns()}", "gemini-3.8-flash-low", [{"role": "user", "content": "[FAKE_LICENSE_3501]"}])
    checks.append(("license_3501_403", st_lic == 403, st_lic))

    # 3. MCP zero-trust permission gate + elicit + max_tokens continuation
    st_pn, _, d_pn = _msg_post(port, f"e2e-pn-{time.time_ns()}", "gemini-3.8-flash-low", [{"role": "user", "content": "[FAKE_PERM_NAMEONLY]"}])
    txt_pn = E.anthropic_text(d_pn)
    checks.append(("perm_nameonly_allowed", st_pn == 200 and "PERM_NAMEONLY=selected:" in txt_pn, txt_pn[:100]))

    st_ps, _, d_ps = _msg_post(port, f"e2e-ps-{time.time_ns()}", "gemini-3.8-flash-low", [{"role": "user", "content": "[FAKE_PERM_SPOOF]"}])
    txt_ps = E.anthropic_text(d_ps)
    checks.append(("perm_spoof_cancelled", st_ps == 200 and "PERM_SPOOF=selected:opt_reject_once" in txt_ps, txt_ps[:100]))

    st_el, _, d_el = _msg_post(port, f"e2e-el-{time.time_ns()}", "gemini-3.8-flash-low", [{"role": "user", "content": "[FAKE_ELICIT]"}])
    txt_el = E.anthropic_text(d_el)
    checks.append(("elicit_rejected_gracefully", st_el == 200 and "ELICIT_REJECTED" in txt_el, txt_el[:100]))

    st_mt, _, d_mt = _msg_post(port, f"e2e-mt-{time.time_ns()}", "gemini-3.8-flash-low", [{"role": "user", "content": "[FAKE_MAX_TOKENS]"}])
    txt_mt = E.anthropic_text(d_mt)
    checks.append(("max_tokens_continuation", st_mt == 200 and isinstance(d_mt, dict) and d_mt.get("stop_reason") == "end_turn" and "PART1_" in txt_mt and "FAKE_OK" in txt_mt, txt_mt[:120]))

    # 4. Unauthenticated loopback Host/Origin 403 guards on 18892
    st_host, _, _ = request_full(port, "POST", "/v1/messages", body={"model": "gemini-3.8-flash-low", "messages": [{"role": "user", "content": "hi"}]}, extra_headers={"Host": f"evil.local:{port}"})
    checks.append(("non_loopback_host_403", st_host == 403, st_host))

    st_orig, _, _ = request_full(port, "POST", "/v1/messages", body={"model": "gemini-3.8-flash-low", "messages": [{"role": "user", "content": "hi"}]}, extra_headers={"Origin": "http://evil.local"})
    checks.append(("disallowed_origin_403", st_orig == 403, st_orig))

    ok = all(passed for _, passed, _ in checks)
    return {"ok": ok, "checks": [{"name": n, "ok": p, "detail": v} for n, p, v in checks], "generation_requests": 0}


def scenario_protocol_tool_roundtrips_and_soft_detach(port, dash_port):
    """Features #1 & #6 Tier 1 & 3: Chat/Responses/Gemini tool roundtrips, 415/400 guards, disconnect recovery."""
    results = []

    # 1. /v1/chat/completions tool roundtrip
    chat_tool = {"type": "function", "function": {"name": "Bash", "description": "Run bash", "parameters": {"type": "object", "properties": {"command": {"type": "string"}}, "required": ["command"]}}}
    sid_c = f"e2e-tr-chat-{time.time_ns()}"
    c_msgs = [{"role": "system", "content": E.SYS}, {"role": "user", "content": "[FAKE_TOOL_CALL] run echo FAKE_TOOL_OK"}]
    st_c1, _, d_c1 = post_full(port, "/v1/chat/completions", {"model": "gemini-4-carbon", "tools": [chat_tool], "messages": c_msgs}, sid=sid_c)
    tcs = ((d_c1.get("choices") or [{}])[0].get("message") or {}).get("tool_calls") or [] if isinstance(d_c1, dict) else []
    ok_c = False
    if st_c1 == 200 and tcs:
        c_msgs.append({"role": "assistant", "content": None, "tool_calls": tcs})
        c_msgs.append({"role": "tool", "tool_call_id": tcs[0]["id"], "content": "FAKE_TOOL_OK"})
        st_c2, _, d_c2 = post_full(port, "/v1/chat/completions", {"model": "gemini-4-carbon", "tools": [chat_tool], "messages": c_msgs}, sid=sid_c)
        ok_c = st_c2 == 200 and "TOOL_DONE:FAKE_TOOL_OK" in E.chat_text(d_c2)
    results.append(("chat_tool_roundtrip", ok_c, st_c1))

    # 2. /v1/responses tool roundtrip
    resp_tool = {"type": "function", "name": "Bash", "description": "Run bash", "parameters": {"type": "object", "properties": {"command": {"type": "string"}}, "required": ["command"]}}
    sid_r = f"e2e-tr-resp-{time.time_ns()}"
    r_in = [{"type": "message", "role": "user", "content": "[FAKE_TOOL_CALL] run echo FAKE_TOOL_OK"}]
    st_r1, _, d_r1 = post_full(port, "/v1/responses", {"model": "gemini-4-carbon", "instructions": E.SYS, "tools": [resp_tool], "input": r_in}, sid=sid_r)
    fcs = [it for it in (d_r1.get("output") or []) if isinstance(it, dict) and it.get("type") == "function_call"] if isinstance(d_r1, dict) else []
    ok_r = False
    if st_r1 == 200 and fcs:
        r_in.append(fcs[0])
        r_in.append({"type": "function_call_output", "call_id": fcs[0]["call_id"], "output": "FAKE_TOOL_OK"})
        st_r2, _, d_r2 = post_full(port, "/v1/responses", {"model": "gemini-4-carbon", "instructions": E.SYS, "tools": [resp_tool], "input": r_in}, sid=sid_r)
        ok_r = st_r2 == 200 and "TOOL_DONE:FAKE_TOOL_OK" in E.responses_text(d_r2)
    results.append(("responses_tool_roundtrip", ok_r, st_r1))

    # 3. /v1beta tool roundtrip
    gem_tools = [{"functionDeclarations": [{"name": "Bash", "description": "Run bash", "parameters": {"type": "OBJECT", "properties": {"command": {"type": "STRING"}}, "required": ["command"]}}]}]
    sid_g = f"e2e-tr-gem-{time.time_ns()}"
    g_contents = [{"role": "user", "parts": [{"text": "[FAKE_TOOL_CALL] run echo FAKE_TOOL_OK"}]}]
    st_g1, _, d_g1 = post_full(port, "/v1beta/models/gemini-4-carbon:generateContent", {"systemInstruction": {"parts": [{"text": E.SYS}]}, "tools": gem_tools, "contents": g_contents}, sid=sid_g)
    g_parts = (((d_g1.get("candidates") or [{}])[0].get("content") or {}).get("parts") or []) if isinstance(d_g1, dict) else []
    g_fcs = [p["functionCall"] for p in g_parts if isinstance(p, dict) and "functionCall" in p]
    ok_g = False
    if st_g1 == 200 and g_fcs:
        fc = g_fcs[0]
        g_contents.append({"role": "model", "parts": [{"functionCall": fc}]})
        fr = {"name": fc["name"], "response": {"output": "FAKE_TOOL_OK"}}
        if fc.get("id"):
            fr["id"] = fc["id"]
        g_contents.append({"role": "user", "parts": [{"functionResponse": fr}]})
        st_g2, _, d_g2 = post_full(port, "/v1beta/models/gemini-4-carbon:generateContent", {"systemInstruction": {"parts": [{"text": E.SYS}]}, "tools": gem_tools, "contents": g_contents}, sid=sid_g)
        ok_g = st_g2 == 200 and "TOOL_DONE:FAKE_TOOL_OK" in E.gemini_text(d_g2)
    results.append(("gemini_tool_roundtrip", ok_g, st_g1))

    # 4. 415 wrong Content-Type & 400 malformed JSON
    st_415, _, _ = request_full(port, "POST", "/v1/messages", raw_body="{}", extra_headers={"Content-Type": "text/plain"})
    results.append(("wrong_content_type_415", st_415 == 415, st_415))
    st_400, _, _ = request_full(port, "POST", "/v1/messages", raw_body="{bad json", extra_headers={"Content-Type": "application/json"})
    results.append(("malformed_json_400", st_400 == 400, st_400))

    # 5. [FAKE_SLOW_PREFILL_300MS] disconnect + warm retry
    sid_sd = f"e2e-disc-{time.time_ns()}"
    u1 = {"role": "user", "content": [{"type": "text", "text": "<system-reminder>currentDate: 2026-11-18T08:00:00Z</system-reminder>\nAnchor.", "cache_control": {"type": "ephemeral"}}]}
    st_sd1, _, d_sd1 = _msg_post(port, sid_sd, "gemini-3.8-flash-low", [u1])
    _, dash_sd1 = get_dash(dash_port, "/api/dashboard?refresh=1")
    sd_sid = (live_for_sid(dash_sd1, sid_sd) or {}).get("sessionId") if isinstance(dash_sd1, dict) else ""
    disc_res = disconnect_request(
        port,
        "/v1/messages",
        {"model": "gemini-3.8-flash-low", "max_tokens": 256, "system": E.SYS, "stream": True, "tools": [E.BASH_TOOL],
         "messages": [u1, {"role": "assistant", "content": d_sd1["content"]}, {"role": "user", "content": "[FAKE_SLOW_PREFILL_300MS] slow prefill"}]},
        sid=sid_sd,
        wait_sec=0.12,
        read_first_sse=False,
    )
    time.sleep(0.45)
    u1_mut = {"role": "user", "content": [{"type": "text", "text": "<system-reminder>currentDate: 2026-11-18T08:15:00Z</system-reminder>\nAnchor."}]}
    st_sd2, _, _ = _msg_post(port, sid_sd, "gemini-3.8-flash-low", [u1_mut, {"role": "assistant", "content": d_sd1["content"]}, {"role": "user", "content": "[FAKE_SLOW_PREFILL_300MS] slow prefill"}])
    _, dash_sd2 = get_dash(dash_port, "/api/dashboard?refresh=1")
    sd_turns = turns_for_sid(dash_sd2, sid_sd) if isinstance(dash_sd2, dict) else []
    sd_live = live_for_sid(dash_sd2, sid_sd) if isinstance(dash_sd2, dict) else None
    ok_sd = (
        st_sd1 == 200
        and disc_res.get("disconnected") is True
        and st_sd2 == 200
        and bool(sd_sid)
        and len(sd_turns) >= 2
        and sd_turns[-1].get("sessionId") == sd_sid
        and sd_turns[-1].get("mode") == "sticky_delta"
        and int((sd_live or {}).get("compactionRollovers") or 0) == 0
    )
    results.append(("slow_prefill_disconnect_recovery", ok_sd, sd_turns[-1].get("mode") if sd_turns else None))

    ok = all(p for _, p, _ in results)
    return {"ok": ok, "results": [{"name": n, "ok": p, "detail": v} for n, p, v in results], "generation_requests": 0}


def scenario_tier5_double_stripped_continuity(port, dash_port):
    """Tier 5 Bug 2 E2E: 4-turn double-stripped assistant continuity across Anthropic, Chat & Responses."""
    u0 = {"role": "user", "content": "tier5 stripped u0"}
    u1 = {"role": "user", "content": "tier5 stripped u1"}
    u2 = {"role": "user", "content": "tier5 stripped u2"}
    a3 = {"role": "assistant", "content": "tier5 assistant 3"}
    u3 = {"role": "user", "content": "tier5 stripped u3"}
    specs = [
        ("Messages", "/v1/messages", lambda ms: {"model": "gemini-4-carbon", "max_tokens": 128, "system": E.SYS, "messages": ms}),
        ("Chat", "/v1/chat/completions", lambda ms: {"model": "gemini-4-carbon", "messages": [{"role": "system", "content": E.SYS}] + ms}),
        ("Responses", "/v1/responses", lambda ms: {"model": "gemini-4-carbon", "instructions": E.SYS, "input": ms}),
    ]
    traces = [
        [u0],
        [u0, u1],
        [u0, u1, u2],
        [u0, u1, u2, a3, u3],
    ]
    records = []
    for proto, path, mk_body in specs:
        sid = f"e2e-t5-dbl-{proto.lower()}-{time.time_ns()}"
        st4 = 0
        for idx, tr in enumerate(traces):
            st, _, _ = post_full(port, path, mk_body(tr), sid=sid)
            if idx < 3 and st != 200:
                return {"ok": False, "protocol": proto, "turn": idx + 1, "status": st}
            st4 = st
        _, dash = get_dash(dash_port, "/api/dashboard?refresh=1")
        all_turns = turns_for_sid(dash, sid) if isinstance(dash, dict) else []
        # Turn 1 ([u0]) runs on u:fp1; Turns 2, 3, 4 all share the Turn-2 u:fp2 convKey (3 turns total on u:fp2).
        conv2 = all_turns[-1].get("convKey") if all_turns else ""
        turns2 = [t for t in all_turns if t.get("convKey") == conv2]
        live2 = None
        for ls in (dash.get("liveSessions") or []) if isinstance(dash, dict) else []:
            if ls.get("convKey") == conv2:
                live2 = ls
                break
        records.append({"protocol": proto, "turn4_status": st4, "turns": turns2, "live": live2})
    ok, detail = eval_tier5_double_stripped_verdict(records)
    return {"ok": ok, **detail, "generation_requests": 0}


# ------------------------------------------------------------------ Part B isolated sub-instance harness & scenarios


def _pick_free_port_pair(forbidden):
    for base in range(29500, 31500, 2):
        if base in forbidden or (base + 1) in forbidden:
            continue
        ok = True
        for p in (base, base + 1):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.bind(("127.0.0.1", p))
            except OSError:
                ok = False
                break
        if ok:
            return base
    raise RuntimeError("no free port pair available for isolated sub-instance")


class IsolatedSubInstance:
    """Context manager launching scripts/gw.sh + scripts/cache_resilience_acp.py in TemporaryDirectory."""

    def __init__(self, receipt, extra_env=None, tag="sub"):
        self.receipt = receipt or {}
        self.extra_env = dict(extra_env or {})
        self.tag = tag
        self._tmp = None
        self.port = None
        self.dash_port = None
        self.env = None
        self.log_path = None
        self.run_dir = None

    def __enter__(self):
        gw_bin = E.pid_executable(self.receipt["pid"])
        fakeacp_bin = self.receipt.get("acp_path")
        if not fakeacp_bin or not Path(fakeacp_bin).is_file():
            raise E.GateError(f"receipt missing valid acp_path: {fakeacp_bin}")
        self._tmp = tempfile.TemporaryDirectory(prefix=f"agy-resil-{self.tag}-")
        try:
            root = Path(self._tmp.name).resolve()
            self.port = _pick_free_port_pair(FORBIDDEN_SUB_PORTS)
            self.dash_port = self.port + 1
            env = U.instance_env(root, f"{self.tag}-{self.port}", WRAPPER_ACP, self.port)
            conv_dir = Path(env["AGY_PURE_API_HOME"]) / "antigravity-cli" / "conversations"
            conv_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
            env["AGY_GATEWAY_BIN"] = str(gw_bin)
            env["REVIEW_FAKEACP_BIN"] = str(fakeacp_bin)
            env["AGY_CONV_DIR"] = str(conv_dir)
            env["FAKE_ACP_WRITE_DB"] = "1"
            env["FAKE_ACP_EMIT_USAGE_UPDATE"] = "1"
            env["AGY_ENV_FILE"] = ""
            env.update(self.extra_env)
            self.env = env
            self.log_path = Path(env["AGY_GW_LOG"])
            self.run_dir = Path(env["AGY_RUN_DIR"])

            res = subprocess.run(["bash", str(GW_SH), "start", "go"], env=env, capture_output=True, text=True, timeout=30)
            if res.returncode != 0:
                raise RuntimeError(f"gw.sh start failed ({res.returncode}): {res.stderr or res.stdout}")
            deadline = time.monotonic() + 10.0
            while time.monotonic() < deadline:
                try:
                    st, _, _ = request_full(self.port, "GET", "/healthz", timeout=1, include_auth=False)
                    if st == 200:
                        return self
                except (OSError, http.client.HTTPException):
                    pass
                time.sleep(0.1)
            raise RuntimeError(f"sub-instance {self.port} did not become ready in time")
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def __exit__(self, exc_type, exc, tb):
        try:
            if self.env is not None:
                subprocess.run(["bash", str(GW_SH), "stop"], env=self.env, capture_output=True, text=True, timeout=30)
        finally:
            if self._tmp is not None:
                self._tmp.cleanup()


def scenario_p0c_orphan_relay_cap_and_streak_breaker(receipt):
    """Part B: orphan relay cap (2 relays -> cap), orphanCapStreak=0 relays on 2nd orphan, tool_result reset, & hibernated equal-MessageCount safety."""
    extra = {
        "AGY_TOOL_RELAY_MS": "700",
        "AGY_MAX_TOOL_RELAYS": "2",
        "AGY_PARKED_TTL_SEC": "300",
        "AGY_MAX_LIVE_WORKERS": "4",
    }
    with IsolatedSubInstance(receipt, extra_env=extra, tag="p0c") as sub:
        # Session A: orphan #1 -> cap(2) -> orphan #2 -> cap(0) -> valid tool_result resets -> orphan #3 -> cap(2)
        sid_s = f"e2e-streak-{time.time_ns()}"
        off1 = log_offset(sub.log_path)
        u1 = {"role": "user", "content": "[WRAPPER_TOOL_CALL] [WRAPPER_FOLLOW_RELAYS] orphan 1"}
        st1, _, d1 = _msg_post(sub.port, sid_s, "gemini-4-carbon", [u1])
        if st1 != 200 or not isinstance(d1, dict) or d1.get("stop_reason") != "tool_use":
            return {"ok": False, "status": st1, "reason": "orphan 1 did not return tool_use"}
        cap1_lines = wait_log_lines(sub.log_path, off1, "[MCP_TOOL_RELAY_CAP]", timeout=4.0)
        cap1_metas = [parse_log_line(ln)[1] for ln in filter_log_for_sid(cap1_lines, sid_s)]
        cap1_ok = any((m or {}).get("maxToolRelays") == 2 and (m or {}).get("relayCount") == 2 for m in cap1_metas)
        wait_log_lines(sub.log_path, off1, "reap_orphan_parked_relay_cap", timeout=2.0)

        # Turn 2: 2nd orphan tool call on same sid_s (must clamp maxToolRelays=0 immediately!)
        off2 = log_offset(sub.log_path)
        u2 = {"role": "user", "content": "[WRAPPER_TOOL_CALL] [WRAPPER_FOLLOW_RELAYS] orphan 2"}
        st2, _, d2 = _msg_post(sub.port, sid_s, "gemini-4-carbon", [u1, {"role": "assistant", "content": d1["content"]}, u2])
        if st2 != 200 or not isinstance(d2, dict) or d2.get("stop_reason") != "tool_use":
            return {"ok": False, "status": st2, "reason": "orphan 2 did not return tool_use"}
        cap2_lines = wait_log_lines(sub.log_path, off2, "[MCP_TOOL_RELAY_CAP]", timeout=3.0)
        cap2_metas = [parse_log_line(ln)[1] for ln in filter_log_for_sid(cap2_lines, sid_s)]
        cap2_ok = any((m or {}).get("maxToolRelays") == 0 and (m or {}).get("relayCount") == 0 for m in cap2_metas)
        wait_log_lines(sub.log_path, off2, "reap_orphan_parked_relay_cap", timeout=2.0)

        # Turn 3: valid tool_result roundtrip (before 250ms timer!) resets orphanCapStreak
        u3 = {"role": "user", "content": "[WRAPPER_TOOL_CALL] valid tool roundtrip"}
        st3a, _, d3a = _msg_post(sub.port, sid_s, "gemini-4-carbon", [u1, {"role": "assistant", "content": d1["content"]}, u2, {"role": "assistant", "content": d2["content"]}, u3])
        tus3 = [c for c in (d3a.get("content", []) if isinstance(d3a, dict) else []) if c.get("type") == "tool_use"]
        if st3a != 200 or not tus3:
            return {"ok": False, "status": st3a, "reason": "valid tool turn 3 did not return tool_use"}
        msgs3b = [
            u1, {"role": "assistant", "content": d1["content"]},
            u2, {"role": "assistant", "content": d2["content"]},
            u3, {"role": "assistant", "content": d3a["content"]},
            {"role": "user", "content": [{"type": "tool_result", "tool_use_id": tus3[0]["id"], "content": "RESET_OK"}]},
        ]
        st3b, _, d3b = _msg_post(sub.port, sid_s, "gemini-4-carbon", msgs3b)
        reset_ok = st3b == 200 and "WRAPPER_TOOL_DONE:RESET_OK" in E.anthropic_text(d3b)

        # Turn 4: after valid tool_result reset, orphan call gets maxToolRelays=2 again
        off4 = log_offset(sub.log_path)
        u4 = {"role": "user", "content": "[WRAPPER_TOOL_CALL] [WRAPPER_FOLLOW_RELAYS] orphan after reset"}
        st4, _, d4 = _msg_post(sub.port, sid_s, "gemini-4-carbon", msgs3b + [{"role": "assistant", "content": d3b["content"]}, u4])
        cap4_ok = False
        if st4 == 200 and isinstance(d4, dict) and d4.get("stop_reason") == "tool_use":
            cap4_lines = wait_log_lines(sub.log_path, off4, "[MCP_TOOL_RELAY_CAP]", timeout=4.0)
            cap4_metas = [parse_log_line(ln)[1] for ln in filter_log_for_sid(cap4_lines, sid_s)]
            cap4_ok = any((m or {}).get("maxToolRelays") == 2 and (m or {}).get("relayCount") == 2 for m in cap4_metas)
            wait_log_lines(sub.log_path, off4, "reap_orphan_parked_relay_cap", timeout=3.0)

        # Session B: hibernated-parked equal-MessageCount interrupt safety
        sid_hib = f"e2e-hibeq-{time.time_ns()}"
        off_h = log_offset(sub.log_path)
        uh1 = {"role": "user", "content": "[FAKE_TOOL_CALL] [FAKE_FOLLOW_RELAYS] hibernate me"}
        sth1, _, dh1 = _msg_post(sub.port, sid_hib, "gemini-4-carbon", [uh1])
        hib_ok = False
        if sth1 == 200 and isinstance(dh1, dict) and dh1.get("stop_reason") == "tool_use":
            hib_deadline = time.monotonic() + 4.0
            while time.monotonic() < hib_deadline:
                if any("reap_orphan_parked_relay_cap" in ln for ln in filter_log_for_sid(read_log_slice(sub.log_path, off_h), sid_hib)):
                    break
                time.sleep(0.1)
            uh2 = {"role": "user", "content": "HIBERNATED_INTERRUPT_OK [ECHO_PROMPT]"}
            sth2, _, dh2 = _msg_post(sub.port, sid_hib, "gemini-4-carbon", [uh1, uh2])
            hib_logs = filter_log_for_sid(read_log_slice(sub.log_path, off_h), sid_hib)
            hib_ok = (
                sth2 == 200
                and isinstance(dh2, dict)
                and dh2.get("stop_reason") == "end_turn"
                and "HIBERNATED_INTERRUPT_OK" in E.anthropic_text(dh2)
                and not any("rollover_compaction" in ln for ln in hib_logs)
            )

        _, met_raw = get_dash(sub.dash_port, "/metrics")
        _, metrics = parse_prom_metrics(met_raw if isinstance(met_raw, str) else "")
        cap_total = metric_sum(metrics, "agy_gateway_orphan_relay_caps_total")

        ok = cap1_ok and cap2_ok and reset_ok and cap4_ok and hib_ok and cap_total >= 3
        return {
            "ok": ok,
            "cap1_ok": cap1_ok,
            "cap2_zero_relays_ok": cap2_ok,
            "reset_ok": reset_ok,
            "cap4_restored_ok": cap4_ok,
            "hibernated_equal_msgcount_ok": hib_ok,
            "orphan_relay_caps_total": cap_total,
            "generation_requests": 0,
        }


def scenario_p1a_subinstance_medium_fallback_and_transient_32603(receipt):
    """Part B: Carbon implicit medium->low fallback and 1-shot -32603 retry recovery within 1 HTTP 200."""
    extra = {"AGY_MAX_LIVE_WORKERS": "4"}
    with IsolatedSubInstance(receipt, extra_env=extra, tag="p1a") as sub:
        # 1. Carbon implicit medium (no suffix, no thinking config) -> 429 -> in-place low fallback
        sid_med = f"e2e-medlow-{time.time_ns()}"
        m1 = {"role": "user", "content": "warmup carbon medium"}
        st1, _, d1 = _msg_post(sub.port, sid_med, "gemini-4-carbon", [m1])
        if st1 != 200 or "tier=medium" not in E.anthropic_text(d1):
            return {"ok": False, "status": st1, "text": E.anthropic_text(d1), "reason": "medium warmup failed"}
        _, dash1 = get_dash(sub.dash_port, "/api/dashboard?refresh=1")
        med_sid = (live_for_sid(dash1, sid_med) or {}).get("sessionId") if isinstance(dash1, dict) else ""

        m2 = {"role": "user", "content": "[WRAPPER_MEDIUM_429_LOW_OK] probe"}
        st2, _, d2 = _msg_post(sub.port, sid_med, "gemini-4-carbon", [m1, {"role": "assistant", "content": d1["content"]}, m2])
        t2_txt = E.anthropic_text(d2)

        # 2. 1-shot transient -32603 retry inside single HTTP 200 request (no HibernateWorker)
        sid_326 = f"e2e-32603-{time.time_ns()}"
        off_r = log_offset(sub.log_path)
        r1 = {"role": "user", "content": "warmup 32603"}
        str1, _, dr1 = _msg_post(sub.port, sid_326, "gemini-4-carbon-high", [r1])
        if str1 != 200:
            return {"ok": False, "status": str1, "reason": "32603 warmup failed"}
        _, dash_r1 = get_dash(sub.dash_port, "/api/dashboard?refresh=1")
        r_sid = (live_for_sid(dash_r1, sid_326) or {}).get("sessionId") if isinstance(dash_r1, dict) else ""

        r2 = {"role": "user", "content": f"[WRAPPER_TRANSIENT_32603_ONCE:{sid_326}] transient test"}
        str2, _, dr2 = _msg_post(sub.port, sid_326, "gemini-4-carbon-high", [r1, {"role": "assistant", "content": dr1["content"]}, r2])
        r2_txt = E.anthropic_text(dr2)

        r3 = {"role": "user", "content": "[ECHO_RELAY_META] after 32603"}
        str3, _, dr3 = _msg_post(
            sub.port,
            sid_326,
            "gemini-4-carbon-high",
            [r1, {"role": "assistant", "content": dr1["content"]}, r2, {"role": "assistant", "content": dr2["content"] if isinstance(dr2, dict) else []}, r3],
        )
        r3_txt = E.anthropic_text(dr3)

        _, dash_end = get_dash(sub.dash_port, "/api/dashboard?refresh=1")
        if not isinstance(dash_end, dict):
            return {"ok": False, "reason": "dashboard unavailable"}

        med_turns = turns_for_sid(dash_end, sid_med)
        med_live = live_for_sid(dash_end, sid_med)
        med_ok = (
            st2 == 200
            and "FALLBACK_LOW_OK" in t2_txt
            and bool(med_sid)
            and len(med_turns) >= 2
            and med_turns[-1].get("sessionId") == med_sid
            and med_turns[-1].get("thoughtLevel") == "low"
            and int((med_live or {}).get("compactionRollovers") or 0) == 0
        )

        rpc_turns = turns_for_sid(dash_end, sid_326)
        rpc_live = live_for_sid(dash_end, sid_326)
        rpc_logs = filter_log_for_sid(read_log_slice(sub.log_path, off_r), sid_326)
        rpc_ok = (
            str2 == 200
            and "RETRY_32603_OK" in r2_txt
            and str3 == 200
            and "resumes=0" in r3_txt
            and bool(r_sid)
            and len(rpc_turns) >= 3
            and all(t.get("sessionId") == r_sid for t in rpc_turns)
            and int((rpc_live or {}).get("compactionRollovers") or 0) == 0
            and not any('"reason":"prompt_rpc_error"' in ln for ln in rpc_logs)
        )
        return {
            "ok": med_ok and rpc_ok,
            "medium_to_low_ok": med_ok,
            "transient_32603_ok": rpc_ok,
            "med_sid": med_sid,
            "med_last_tier": med_turns[-1].get("thoughtLevel") if med_turns else None,
            "r_sid": r_sid,
            "generation_requests": 0,
        }


def scenario_p0a_config_and_security_subinstance(receipt):
    """Part B: API keys, /health, /api/quota, CORS preflight, 40K warn, WAF auto-bisect, PrefillSoftDetach join & ToS breaker."""
    test_key = "e2e-test-key-1"
    extra = {
        "AGY_API_KEYS": test_key,
        "AGY_CORS_ORIGINS": "http://allowed.local",
        "AGY_CACHE_WARN_MIN_TOKENS": "40000",
        "AGY_IDLE_WORKER_TTL_SEC": "600",
        "AGY_PREFILL_CANCEL_MIN_ELAPSED_SEC": "1",
        "AGY_PREFILL_CANCEL_GRACE_SEC": "90",
        "AGY_MAX_LIVE_WORKERS": "4",
    }
    with IsolatedSubInstance(receipt, extra_env=extra, tag="p0a") as sub:
        checks = []
        # 1. Auth 401 vs 200 across x-api-key / Bearer / x-goog-api-key / dashboard + /health + /api/quota
        st_unauth, _, _ = post_full(sub.port, "/v1/messages", {"model": "gemini-3.8-flash-low", "messages": [{"role": "user", "content": "hi"}]}, include_auth=False)
        st_dash_unauth, _ = get_dash(sub.dash_port, "/api/dashboard", include_auth=False)
        st_xapi, _, _ = post_full(sub.port, "/v1/messages", {"model": "gemini-3.8-flash-low", "max_tokens": 64, "messages": [{"role": "user", "content": "ok"}]}, extra_headers={"x-api-key": test_key}, include_auth=False)
        st_bearer, _, _ = post_full(sub.port, "/v1/chat/completions", {"model": "gemini-3.8-flash-low", "messages": [{"role": "user", "content": "ok"}]}, extra_headers={"Authorization": f"Bearer {test_key}"}, include_auth=False)
        st_goog, _, _ = request_full(sub.port, "GET", "/v1beta/models", extra_headers={"x-goog-api-key": test_key}, include_auth=False)
        checks.append(("api_key_auth", st_unauth == 401 and st_dash_unauth == 401 and st_xapi == 200 and st_bearer == 200 and st_goog == 200,
                       (st_unauth, st_dash_unauth, st_xapi, st_bearer, st_goog)))

        st_h, _, d_h = request_full(sub.dash_port, "GET", "/health", extra_headers={"x-api-key": test_key}, include_auth=False)
        health_ok = (
            st_h == 200
            and isinstance(d_h, dict)
            and d_h.get("idleWorkerTtlMs") == 600000
            and d_h.get("maxToolRelays") == 4
        )
        checks.append(("health_config_defaults", health_ok, (st_h, (d_h or {}).get("idleWorkerTtlMs") if isinstance(d_h, dict) else None, (d_h or {}).get("maxToolRelays") if isinstance(d_h, dict) else None)))

        st_q, _, d_q = request_full(sub.dash_port, "GET", "/api/quota", extra_headers={"x-api-key": test_key}, include_auth=False)
        quota_ok = (
            st_q == 200
            and isinstance(d_q, dict)
            and isinstance(d_q.get("quota"), dict)
            and bool((d_q.get("quota") or {}).get("status"))
        )
        checks.append(("api_quota_200", quota_ok, (st_q, (d_q.get("quota") or {}).get("status") if isinstance(d_q, dict) else None)))

        # 2. CORS OPTIONS preflight & Access-Control-Expose-Headers
        st_opt, hdrs_opt, _ = request_full(sub.port, "OPTIONS", "/v1/messages", extra_headers={"Origin": "http://allowed.local"}, include_auth=False)
        checks.append(("cors_preflight", st_opt == 204 and hdrs_opt.get("access-control-allow-origin") == "http://allowed.local",
                       (st_opt, hdrs_opt.get("access-control-allow-origin"))))

        # 3. AGY_CACHE_WARN_MIN_TOKENS=40000 -> [FAKE_ZERO_50K] x4 (cold_start + 3 zero = streak=3) warns
        sid_40k = f"e2e-40k-{time.time_ns()}"
        m40 = []
        hdr40 = ""
        for idx in range(1, 5):
            m40.append({"role": "user", "content": f"Turn {idx} [FAKE_ZERO_50K]"})
            st40, h40, d40 = post_full(
                sub.port,
                "/v1/messages",
                {"model": "gemini-4-carbon", "max_tokens": 128, "system": E.SYS, "messages": m40},
                sid=sid_40k,
                extra_headers={"x-api-key": test_key, "Origin": "http://allowed.local"},
                include_auth=False,
            )
            if st40 != 200 or not isinstance(d40, dict):
                return {"ok": False, "status": st40, "reason": "40k threshold turn failed"}
            hdr40 = h40.get("x-agy-cache-warning", "")
            m40.append({"role": "assistant", "content": d40["content"]})
        checks.append(("cache_warn_40k_threshold", "streak=3" in hdr40, hdr40))

        # 4. TRIGGER_UNKNOWN_HARNESS_WAF_ auto-bisect recovery
        waf_sys = f"{E.SYS}\nYou are a helpful assistant.\nTRIGGER_UNKNOWN_HARNESS_WAF_ALPHA.\nFollow instructions."
        st_waf, _, d_waf = post_full(
            sub.port,
            "/v1/messages",
            {"model": "gemini-3.8-flash-low", "max_tokens": 128, "system": waf_sys, "messages": [{"role": "user", "content": "hello after bisect"}]},
            sid=f"e2e-waf-{time.time_ns()}",
            extra_headers={"x-api-key": test_key},
            include_auth=False,
        )
        checks.append(("waf_autobisect_healed", st_waf == 200 and "FAKE_OK" in E.anthropic_text(d_waf), st_waf))

        # 5. PrefillSoftDetach (minElapsed=1s, graceSec=90s) + JoinDetachedTurn + /metrics counter
        _, m0_raw = get_dash(sub.dash_port, "/metrics", extra_headers={"x-api-key": test_key}, include_auth=False)
        _, m0 = parse_prom_metrics(m0_raw if isinstance(m0_raw, str) else "")
        joined0 = metric_sum(m0, "agy_gateway_soft_detach_total", 'outcome="joined"')
        off_sd = log_offset(sub.log_path)
        sid_sd = f"e2e-p0a-sd-{time.time_ns()}"
        sd_body = {"model": "gemini-3.8-flash-low", "max_tokens": 128, "system": E.SYS, "messages": [{"role": "user", "content": "[FAKE_SLOW] soft detach join"}]}
        disc_sd = disconnect_request(
            sub.port,
            "/v1/messages",
            sd_body,
            sid=sid_sd,
            wait_sec=1.35,
            read_first_sse=False,
            extra_headers={"x-api-key": test_key},
            include_auth=False,
        )
        sd_lines = filter_log_for_sid(wait_log_lines(sub.log_path, off_sd, "[PREFILL_SOFT_DETACH]", timeout=3.0), sid_sd)
        sd_grace_ok = any(((parse_log_line(ln)[1] or {}).get("graceSec") or 0) >= 90 for ln in sd_lines)
        st_sd_join, _, d_sd_join = post_full(
            sub.port,
            "/v1/messages",
            sd_body,
            sid=sid_sd,
            extra_headers={"x-api-key": test_key},
            include_auth=False,
            timeout=30,
        )
        _, m1_raw = get_dash(sub.dash_port, "/metrics", extra_headers={"x-api-key": test_key}, include_auth=False)
        _, m1 = parse_prom_metrics(m1_raw if isinstance(m1_raw, str) else "")
        joined_delta = metric_sum(m1, "agy_gateway_soft_detach_total", 'outcome="joined"') - joined0
        sd_join_ok = (
            disc_sd.get("disconnected") is True
            and sd_grace_ok
            and st_sd_join == 200
            and "FAKE_OK" in E.anthropic_text(d_sd_join)
            and joined_delta >= 1
        )
        checks.append(("prefill_soft_detach_join", sd_join_ok, (sd_grace_ok, st_sd_join, joined_delta)))

        # 6. [FAKE_TOS_RPC_ERROR] trips tos-tripped marker and fails subsequent request closed (503)
        st_tos1, _, d_tos1 = post_full(
            sub.port,
            "/v1/messages",
            {"model": "gemini-3.8-flash-low", "max_tokens": 64, "messages": [{"role": "user", "content": "[FAKE_TOS_RPC_ERROR]"}]},
            sid=f"e2e-tos-{time.time_ns()}",
            extra_headers={"x-api-key": test_key},
            include_auth=False,
        )
        st_tos2, _, d_tos2 = post_full(
            sub.port,
            "/v1/messages",
            {"model": "gemini-3.8-flash-low", "max_tokens": 64, "messages": [{"role": "user", "content": "should fail closed"}]},
            sid=f"e2e-tos2-{time.time_ns()}",
            extra_headers={"x-api-key": test_key},
            include_auth=False,
        )
        tos_file = Path(sub.env["AGY_TOS_TRIPPED_FILE"])
        tos_ok = (
            st_tos1 == 503
            and st_tos2 == 503
            and tos_file.is_file()
            and isinstance(d_tos1, dict)
            and (d_tos2.get("error") or {}).get("type") == "tos_circuit_breaker"
        )
        checks.append(("tos_fail_closed", tos_ok, (st_tos1, st_tos2, tos_file.is_file())))

        ok = all(p for _, p, _ in checks)
        return {"ok": ok, "checks": [{"name": n, "ok": p, "detail": v} for n, p, v in checks], "generation_requests": 0}


def scenario_tier5_eviction_cancel_and_hibernated_protocols(receipt):
    """Tier 5 Part B: Pass-2 hot-parked eviction, slow-cancel race recovery, & hibernated Turn 2+ multi-protocol resumes."""
    checks = []

    # 1. Pass-2 hot-parked LRU eviction when MaxLiveWorkers=2, ParkedTtl=2s, Relay=5s (Bug 5b)
    extra_evict = {
        "AGY_MAX_LIVE_WORKERS": "2",
        "AGY_PARKED_TTL_SEC": "2",
        "AGY_TOOL_RELAY_MS": "5000",
        "AGY_MAX_TOOL_RELAYS": "4",
    }
    with IsolatedSubInstance(receipt, extra_env=extra_evict, tag="t5ev") as sub:
        off_ev = log_offset(sub.log_path)
        for i in (1, 2):
            st_p, _, d_p = _msg_post(sub.port, f"e2e-t5-ev-{i}-{time.time_ns()}", "gemini-4-carbon", [{"role": "user", "content": "[WRAPPER_TOOL_CALL] park"}])
            if st_p != 200 or not isinstance(d_p, dict) or d_p.get("stop_reason") != "tool_use":
                return {"ok": False, "reason": f"hot-parked worker {i} failed to park ({st_p})"}
        time.sleep(1.25)  # >= ParkedTtl/2 (1s) and < tool relay (5s)
        st_w3, _, d_w3 = _msg_post(sub.port, f"e2e-t5-ev-3-{time.time_ns()}", "gemini-4-carbon", [{"role": "user", "content": "[ECHO_PROMPT] PASS2_EVICT_OK"}])
        ev_logs = read_log_slice(sub.log_path, off_ev)
        ev_ok = (
            st_w3 == 200
            and "PASS2_EVICT_OK" in E.anthropic_text(d_w3)
            and any("reap_orphan_parked_lru" in ln for ln in ev_logs)
        )
        checks.append(("hot_parked_pass2_lru_eviction", ev_ok, (st_w3, any("reap_orphan_parked_lru" in ln for ln in ev_logs))))

    # 2. Slow-cancel + immediate retry race (Bug 7) & hibernated Turn 2+ Chat/Responses/Gemini parked resumes
    extra_proto = {
        "AGY_MAX_LIVE_WORKERS": "6",
        "AGY_TOOL_RELAY_MS": "450",
        "AGY_MAX_TOOL_RELAYS": "1",
        "AGY_PARKED_TTL_SEC": "300",
    }
    with IsolatedSubInstance(receipt, extra_env=extra_proto, tag="t5pr") as sub:
        # 2a. Slow-cancel race: REQ1 warmup -> REQ2 slow cancel disconnect -> immediate REQ3 succeeds 200
        sid_sc = f"e2e-t5-sc-{time.time_ns()}"
        u0_sc = {"role": "user", "content": "[ECHO_PROMPT] warmup slow cancel"}
        st_sc1, _, d_sc1 = _msg_post(sub.port, sid_sc, "gemini-4-carbon", [u0_sc])
        if st_sc1 != 200 or not isinstance(d_sc1, dict):
            return {"ok": False, "reason": f"slow cancel warmup failed ({st_sc1})"}
        _, dash_sc1 = get_dash(sub.dash_port, "/api/dashboard?refresh=1")
        orig_sc_sid = (live_for_sid(dash_sc1, sid_sc) or {}).get("sessionId") if isinstance(dash_sc1, dict) else ""

        u_slow = {"role": "user", "content": f"[WRAPPER_SLOW_CANCEL_ONCE:{sid_sc}] slow cancel"}
        disc_sc = disconnect_request(
            sub.port,
            "/v1/messages",
            {
                "model": "gemini-4-carbon",
                "max_tokens": 128,
                "system": E.SYS,
                "stream": True,
                "messages": [u0_sc, {"role": "assistant", "content": d_sc1["content"]}, u_slow],
            },
            sid=sid_sc,
            wait_sec=0.08,
            read_first_sse=False,
        )
        time.sleep(0.02)
        st_sc3, _, d_sc3 = _msg_post(
            sub.port,
            sid_sc,
            "gemini-4-carbon",
            [u0_sc, {"role": "assistant", "content": d_sc1["content"]}, u_slow, {"role": "user", "content": "[FAKE_TTFT_300MS] [ECHO_PROMPT] REQ4_AFTER_SLOW_CANCEL"}],
        )
        sc3_txt = E.anthropic_text(d_sc3)
        _, dash_sc2 = get_dash(sub.dash_port, "/api/dashboard?refresh=1")
        sc_turns = turns_for_sid(dash_sc2, sid_sc) if isinstance(dash_sc2, dict) else []
        sc_ok = (
            disc_sc.get("disconnected") is True
            and st_sc3 == 200
            and "REQ4_AFTER_SLOW_CANCEL" in sc3_txt
            and bool(orig_sc_sid)
            and len(sc_turns) >= 2
            and sc_turns[-1].get("sessionId") == orig_sc_sid
        )
        checks.append(("slow_cancel_immediate_retry_not_hijacked", sc_ok, (st_sc3, "REQ4_AFTER_SLOW_CANCEL" in sc3_txt, sc_turns[-1].get("sessionId") if sc_turns else None, orig_sc_sid)))

        def _wait_hibernated(sid_key, off_pos):
            dl = time.monotonic() + 4.0
            while time.monotonic() < dl:
                if any("reap_orphan_parked_relay_cap" in ln for ln in filter_log_for_sid(read_log_slice(sub.log_path, off_pos), sid_key)):
                    return True
                time.sleep(0.08)
            return False

        # 2b. Hibernated Turn 2+ tool_result resume across Chat, Responses, and Gemini v1beta
        chat_tool = {"type": "function", "function": {"name": "Bash", "description": "Run bash", "parameters": {"type": "object", "properties": {"command": {"type": "string"}}, "required": ["command"]}}}
        sid_hc = f"e2e-t5-hc-{time.time_ns()}"
        off_hc = log_offset(sub.log_path)
        hc_msgs = [{"role": "system", "content": E.SYS}, {"role": "user", "content": "warmup chat"}]
        st_hc1, _, d_hc1 = post_full(sub.port, "/v1/chat/completions", {"model": "gemini-4-carbon", "tools": [chat_tool], "messages": hc_msgs}, sid=sid_hc)
        hc_msgs.append({"role": "assistant", "content": E.chat_text(d_hc1)})
        hc_msgs.append({"role": "user", "content": "[WRAPPER_TOOL_CALL] [WRAPPER_FOLLOW_RELAYS] turn2"})
        st_hc2, _, d_hc2 = post_full(sub.port, "/v1/chat/completions", {"model": "gemini-4-carbon", "tools": [chat_tool], "messages": hc_msgs}, sid=sid_hc)
        tcs_hc = ((d_hc2.get("choices") or [{}])[0].get("message") or {}).get("tool_calls") or [] if isinstance(d_hc2, dict) else []
        hc_ok = False
        if st_hc1 == 200 and st_hc2 == 200 and tcs_hc and _wait_hibernated(sid_hc, off_hc):
            hc_msgs.append({"role": "assistant", "content": None, "tool_calls": tcs_hc})
            hc_msgs.append({"role": "tool", "tool_call_id": tcs_hc[0]["id"], "content": "HIB_CHAT_OK"})
            st_hc3, _, d_hc3 = post_full(sub.port, "/v1/chat/completions", {"model": "gemini-4-carbon", "tools": [chat_tool], "messages": hc_msgs}, sid=sid_hc)
            hc_logs = filter_log_for_sid(read_log_slice(sub.log_path, off_hc), sid_hc)
            hc_ok = st_hc3 == 200 and "FAKE_OK" in E.chat_text(d_hc3) and not any("rollover_compaction" in ln for ln in hc_logs)
        checks.append(("hibernated_turn2_chat_tool_resume", hc_ok, st_hc2))

        resp_tool = {"type": "function", "name": "Bash", "description": "Run bash", "parameters": {"type": "object", "properties": {"command": {"type": "string"}}, "required": ["command"]}}
        sid_hr = f"e2e-t5-hr-{time.time_ns()}"
        off_hr = log_offset(sub.log_path)
        hr_in = [{"type": "message", "role": "user", "content": "warmup resp"}]
        st_hr1, _, d_hr1 = post_full(sub.port, "/v1/responses", {"model": "gemini-4-carbon", "instructions": E.SYS, "tools": [resp_tool], "input": hr_in}, sid=sid_hr)
        hr_in.append({"type": "message", "role": "assistant", "content": E.responses_text(d_hr1)})
        hr_in.append({"type": "message", "role": "user", "content": "[WRAPPER_TOOL_CALL] [WRAPPER_FOLLOW_RELAYS] turn2"})
        st_hr2, _, d_hr2 = post_full(sub.port, "/v1/responses", {"model": "gemini-4-carbon", "instructions": E.SYS, "tools": [resp_tool], "input": hr_in}, sid=sid_hr)
        fcs_hr = [it for it in (d_hr2.get("output") or []) if isinstance(it, dict) and it.get("type") == "function_call"] if isinstance(d_hr2, dict) else []
        hr_ok = False
        if st_hr1 == 200 and st_hr2 == 200 and fcs_hr and _wait_hibernated(sid_hr, off_hr):
            hr_in.append(fcs_hr[0])
            hr_in.append({"type": "function_call_output", "call_id": fcs_hr[0]["call_id"], "output": "HIB_RESP_OK"})
            st_hr3, _, d_hr3 = post_full(sub.port, "/v1/responses", {"model": "gemini-4-carbon", "instructions": E.SYS, "tools": [resp_tool], "input": hr_in}, sid=sid_hr)
            hr_logs = filter_log_for_sid(read_log_slice(sub.log_path, off_hr), sid_hr)
            hr_ok = st_hr3 == 200 and "FAKE_OK" in E.responses_text(d_hr3) and not any("rollover_compaction" in ln for ln in hr_logs)
        checks.append(("hibernated_turn2_responses_tool_resume", hr_ok, st_hr2))

        gem_tools = [{"functionDeclarations": [{"name": "Bash", "description": "Run bash", "parameters": {"type": "OBJECT", "properties": {"command": {"type": "STRING"}}, "required": ["command"]}}]}]
        sid_hg = f"e2e-t5-hg-{time.time_ns()}"
        off_hg = log_offset(sub.log_path)
        hg_contents = [{"role": "user", "parts": [{"text": "warmup gem"}]}]
        st_hg1, _, d_hg1 = post_full(sub.port, "/v1beta/models/gemini-4-carbon:generateContent", {"systemInstruction": {"parts": [{"text": E.SYS}]}, "tools": gem_tools, "contents": hg_contents}, sid=sid_hg)
        hg_contents.append({"role": "model", "parts": [{"text": E.gemini_text(d_hg1)}]})
        hg_contents.append({"role": "user", "parts": [{"text": "[WRAPPER_TOOL_CALL] [WRAPPER_FOLLOW_RELAYS] turn2"}]})
        st_hg2, _, d_hg2 = post_full(sub.port, "/v1beta/models/gemini-4-carbon:generateContent", {"systemInstruction": {"parts": [{"text": E.SYS}]}, "tools": gem_tools, "contents": hg_contents}, sid=sid_hg)
        hg_parts = (((d_hg2.get("candidates") or [{}])[0].get("content") or {}).get("parts") or []) if isinstance(d_hg2, dict) else []
        hg_fcs = [p["functionCall"] for p in hg_parts if isinstance(p, dict) and "functionCall" in p]
        hg_ok = False
        if st_hg1 == 200 and st_hg2 == 200 and hg_fcs and _wait_hibernated(sid_hg, off_hg):
            fc = hg_fcs[0]
            hg_contents.append({"role": "model", "parts": [{"functionCall": fc}]})
            fr = {"name": fc["name"], "response": {"output": "HIB_GEM_OK"}}
            if fc.get("id"):
                fr["id"] = fc["id"]
            hg_contents.append({"role": "user", "parts": [{"functionResponse": fr}]})
            st_hg3, _, d_hg3 = post_full(sub.port, "/v1beta/models/gemini-4-carbon:generateContent", {"systemInstruction": {"parts": [{"text": E.SYS}]}, "tools": gem_tools, "contents": hg_contents}, sid=sid_hg)
            hg_logs = filter_log_for_sid(read_log_slice(sub.log_path, off_hg), sid_hg)
            hg_ok = st_hg3 == 200 and "FAKE_OK" in E.gemini_text(d_hg3) and not any("rollover_compaction" in ln for ln in hg_logs)
        checks.append(("hibernated_turn2_gemini_tool_resume", hg_ok, st_hg2))

        # 2c. Hibernated Turn 2 equal-MessageCount interrupt safety on Chat & Responses
        sid_eq_c = f"e2e-t5-eqc-{time.time_ns()}"
        off_eq_c = log_offset(sub.log_path)
        eq_c_base = [{"role": "system", "content": E.SYS}, {"role": "user", "content": "warmup eqc"}]
        st_ec1, _, d_ec1 = post_full(sub.port, "/v1/chat/completions", {"model": "gemini-4-carbon", "tools": [chat_tool], "messages": eq_c_base}, sid=sid_eq_c)
        eq_c_t2 = eq_c_base + [{"role": "assistant", "content": E.chat_text(d_ec1)}, {"role": "user", "content": "[WRAPPER_TOOL_CALL] [WRAPPER_FOLLOW_RELAYS] eqc"}]
        st_ec2, _, _ = post_full(sub.port, "/v1/chat/completions", {"model": "gemini-4-carbon", "tools": [chat_tool], "messages": eq_c_t2}, sid=sid_eq_c)
        ec_ok = False
        if st_ec1 == 200 and st_ec2 == 200 and _wait_hibernated(sid_eq_c, off_eq_c):
            eq_c_t3 = eq_c_t2 + [{"role": "user", "content": "[ECHO_PROMPT] HIB_EQ_CHAT_OK"}]
            st_ec3, _, d_ec3 = post_full(sub.port, "/v1/chat/completions", {"model": "gemini-4-carbon", "tools": [chat_tool], "messages": eq_c_t3}, sid=sid_eq_c)
            ec_logs = filter_log_for_sid(read_log_slice(sub.log_path, off_eq_c), sid_eq_c)
            ec_ok = st_ec3 == 200 and "HIB_EQ_CHAT_OK" in E.chat_text(d_ec3) and not any("rollover_compaction" in ln for ln in ec_logs)
        checks.append(("hibernated_turn2_equal_msgcount_chat", ec_ok, st_ec2))

        sid_eq_r = f"e2e-t5-eqr-{time.time_ns()}"
        off_eq_r = log_offset(sub.log_path)
        eq_r_base = [{"type": "message", "role": "user", "content": "warmup eqr"}]
        st_er1, _, d_er1 = post_full(sub.port, "/v1/responses", {"model": "gemini-4-carbon", "instructions": E.SYS, "tools": [resp_tool], "input": eq_r_base}, sid=sid_eq_r)
        eq_r_t2 = eq_r_base + [{"type": "message", "role": "assistant", "content": E.responses_text(d_er1)}, {"type": "message", "role": "user", "content": "[WRAPPER_TOOL_CALL] [WRAPPER_FOLLOW_RELAYS] eqr"}]
        st_er2, _, _ = post_full(sub.port, "/v1/responses", {"model": "gemini-4-carbon", "instructions": E.SYS, "tools": [resp_tool], "input": eq_r_t2}, sid=sid_eq_r)
        er_ok = False
        if st_er1 == 200 and st_er2 == 200 and _wait_hibernated(sid_eq_r, off_eq_r):
            eq_r_t3 = eq_r_t2 + [{"type": "message", "role": "user", "content": "[ECHO_PROMPT] HIB_EQ_RESP_OK"}]
            st_er3, _, d_er3 = post_full(sub.port, "/v1/responses", {"model": "gemini-4-carbon", "instructions": E.SYS, "tools": [resp_tool], "input": eq_r_t3}, sid=sid_eq_r)
            er_logs = filter_log_for_sid(read_log_slice(sub.log_path, off_eq_r), sid_eq_r)
            er_ok = st_er3 == 200 and "HIB_EQ_RESP_OK" in E.responses_text(d_er3) and not any("rollover_compaction" in ln for ln in er_logs)
        checks.append(("hibernated_turn2_equal_msgcount_responses", er_ok, st_er2))

    ok = all(p for _, p, _ in checks)
    return {"ok": ok, "checks": [{"name": n, "ok": p, "detail": v} for n, p, v in checks], "generation_requests": 0}


# ------------------------------------------------------------------ Suite wiring


def run_all(port, mode, receipt, explicit_dash=None, explicit_log=None, include_subinstances=True):
    if mode != "fake":
        raise E.GateError("e2e_cache_resilience only runs in fake mode (zero live quota cost)")
    dp = resolve_dash_port(port, receipt, explicit_dash)
    log_path = resolve_log_path(explicit_log, receipt)
    scenarios = [
        ("p0b_large_tool_hit_and_burn_guard", lambda _m, _r: scenario_p0b_large_tool_hit_and_burn_guard(port, dp, log_path)),
        ("p0c_hot_parked_interrupt_in_place", lambda _m, _r: scenario_p0c_hot_parked_interrupt_in_place(port, dp, log_path)),
        ("p1a_carbon_high_fallback_and_parked_tier", lambda _m, _r: scenario_p1a_carbon_high_fallback_and_parked_tier(port, dp, log_path)),
        ("p1a_carbon_4x_lru_protection", lambda _m, _r: scenario_p1a_carbon_4x_lru_protection(port, dp, log_path)),
        ("multimodal_happy_and_boundaries", lambda _m, _r: scenario_multimodal_happy_and_boundaries(port)),
        ("cleanroom_notices_and_mcp_guards", lambda _m, _r: scenario_cleanroom_notices_and_mcp_guards(port)),
        ("protocol_tool_roundtrips_and_soft_detach", lambda _m, _r: scenario_protocol_tool_roundtrips_and_soft_detach(port, dp)),
        ("tier5_double_stripped_continuity", lambda _m, _r: scenario_tier5_double_stripped_continuity(port, dp)),
    ]
    if include_subinstances:
        scenarios.extend([
            ("p0c_orphan_relay_cap_and_streak_breaker", lambda _m, r: scenario_p0c_orphan_relay_cap_and_streak_breaker(r)),
            ("p1a_subinstance_medium_fallback_and_transient_32603", lambda _m, r: scenario_p1a_subinstance_medium_fallback_and_transient_32603(r)),
            ("p0a_config_and_security_subinstance", lambda _m, r: scenario_p0a_config_and_security_subinstance(r)),
            ("tier5_eviction_cancel_and_hibernated_protocols", lambda _m, r: scenario_tier5_eviction_cancel_and_hibernated_protocols(r)),
        ])
    return scenarios


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("port", type=int, help="isolated fake-ACP gateway API port")
    ap.add_argument("log", nargs="?", type=Path, help="optional gateway.log path (defaults to receipt gateway_log)")
    ap.add_argument("--dash-port", type=int, default=None, help="optional dashboard port override")
    ap.add_argument("--no-subinstances", action="store_true", help="skip Part B ephemeral sub-instance scenarios")
    E.add_mode_args(ap)
    args = ap.parse_args(argv)
    if args.live:
        print(json.dumps({"ok": False, "error": "GateError: e2e_cache_resilience refuses --live mode"}))
        return 2
    try:
        mode, receipt = E.resolve_mode(args, args.port)
        scenarios = run_all(
            args.port,
            mode,
            receipt,
            explicit_dash=args.dash_port,
            explicit_log=args.log,
            include_subinstances=not args.no_subinstances,
        )
    except (OSError, ValueError, KeyError) as exc:
        print(json.dumps({"ok": False, "error": f"{type(exc).__name__}: {exc}"}))
        return 2
    return E.run_main(scenarios, args, args.port)


if __name__ == "__main__":
    raise SystemExit(main())
