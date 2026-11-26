#!/usr/bin/env python3
"""DB telemetry, step timing, cache hit/miss, usage_update & servedModel canary gate (fake-only, 0 live quota).

Exercises the primary SQLite <sid>.db accounting pipeline (gen_metadata + steps B-tree/SQL),
ACP sessionUpdate="usage_update", step timing / thinking metrics, pre-MCP PlannerResponse tool
billing, Carbon canary UI detection (Decision U8: pinned model, no cross-model rollover), and
Argon shadow-pool transparent self-healing end-to-end against an isolated fake-ACP instance:
  python3 scripts/e2e_db_telemetry.py 18894 --fake-receipt <receipt.json>
"""
import argparse
import json
from pathlib import Path
import sys
import time
import urllib.error
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parent))
import e2e_common as E  # noqa: E402


def _dash_port(port, receipt):
    dp = int((receipt or {}).get("dash_port") or (port + 1))
    if dp in E.MAIN_PORTS:
        raise E.GateError(f"refusing main dashboard port {dp}")
    return dp


def _get_dash(dash_port, path):
    req = urllib.request.Request(f"http://127.0.0.1:{dash_port}{path}", headers=E.auth_headers())
    try:
        resp = urllib.request.urlopen(req, timeout=30)
    except urllib.error.HTTPError as err:
        resp = err
    with resp:
        raw = resp.read().decode("utf-8", "replace")
        try:
            return resp.status, json.loads(raw)
        except ValueError:
            return resp.status, raw


def _turns_for_sid(dash, sid):
    prefix = f"sid:{sid}:"
    turns = [t for t in (dash.get("gatewayTurns") or []) if str(t.get("convKey", "")).startswith(prefix)]
    return sorted(turns, key=lambda t: int(t.get("turnIndex") or 0))


def _live_for_sid(dash, sid):
    prefix = f"sid:{sid}:"
    for s in dash.get("liveSessions") or []:
        if str(s.get("convKey", "")).startswith(prefix):
            return s
    return None


def _steps_for_acp_sid(dash, acp_sid):
    want = str(acp_sid or "").lower()
    steps = [s for s in (dash.get("recentDbSteps") or []) if str(s.get("sessionId", "")).lower() == want]
    return sorted(steps, key=lambda s: int(s.get("stepIdx") or 0))


def scenario_db_billing_and_cache_lifecycle(port, dash_port, receipt):
    """Cold start -> steady-state cache hit -> unexpected zero-cache miss via primary SQLite 'db' ledger."""
    sid = f"e2e-db-cache-{time.time_ns()}"
    msgs = []
    specs = [
        (40000, 0, 50, 180, 500),
        (2000, 40000, 60, 120, 600),
        (42000, 0, 40, 220, 400),
    ]
    http_usages = []
    for idx, (in_tok, cr_tok, out_tok, ttft_ms, stream_ms) in enumerate(specs, 1):
        prompt = (
            f"Turn {idx} [FAKE_INPUT_TOKENS:{in_tok}] [FAKE_CACHE_READ:{cr_tok}] "
            f"[FAKE_OUTPUT_TOKENS:{out_tok}] [FAKE_TTFT_MS:{ttft_ms}] [FAKE_STREAM_MS:{stream_ms}]"
        )
        msgs.append({"role": "user", "content": prompt})
        st, _, data = E.post(port, "/v1/messages", {
            "model": "gemini-4-carbon",
            "max_tokens": 256,
            "system": E.SYS,
            "tools": [E.BASH_TOOL],
            "messages": msgs,
        }, sid)
        if st != 200 or not isinstance(data, dict) or data.get("stop_reason") != "end_turn":
            return {"ok": False, "turn": idx, "status": st, "reason": "HTTP turn failed"}
        u = data.get("usage") or {}
        http_usages.append({
            "in": u.get("input_tokens"),
            "cr": u.get("cache_read_input_tokens"),
            "out": u.get("output_tokens"),
        })
        msgs.append({"role": "assistant", "content": data["content"]})

    d_status, dash = _get_dash(dash_port, "/api/dashboard?refresh=1")
    if d_status != 200 or not isinstance(dash, dict):
        return {"ok": False, "reason": f"dashboard returned {d_status}"}

    live = _live_for_sid(dash, sid)
    turns = _turns_for_sid(dash, sid)
    acp_sid = (live or {}).get("sessionId") or (turns[-1].get("sessionId") if turns else "")
    db_steps = _steps_for_acp_sid(dash, acp_sid)

    conv_dir = Path(receipt["conv_dir"]) if receipt and receipt.get("conv_dir") else None
    db_file = (conv_dir / f"{acp_sid}.db") if (conv_dir and acp_sid) else None
    db_size = db_file.stat().st_size if (db_file and db_file.is_file()) else 0

    quota_ok = (dash.get("quota") or {}).get("status") == "ok"
    rolling5h = (dash.get("windows") or {}).get("rolling5h") or {}

    ok = (
        http_usages == [
            {"in": 40000, "cr": 0, "out": 50},
            {"in": 2000, "cr": 40000, "out": 60},
            {"in": 42000, "cr": 0, "out": 40},
        ]
        and db_size >= 4096
        and live is not None
        and live.get("reportedContextUsed") == 42000
        and live.get("reportedContextSize") == 983040
        and len(turns) == 3
        and all(t.get("usageSource") == "db" for t in turns)
        and [t.get("cacheMiss") for t in turns] == ["cold_start", "", "zero"]
        and turns[1].get("cacheHitRate") == "95.24%"
        and len(db_steps) == 3
        and [s.get("cacheMiss") for s in db_steps] == ["cold_start", "", "zero"]
        and db_steps[1].get("savedUsd", 0) > 0
        and quota_ok
        and rolling5h.get("stepsCount", 0) >= 3
        and rolling5h.get("costUsd", 0) > 0
    )
    return {
        "ok": ok,
        "acp_sid": acp_sid,
        "db_size": db_size,
        "http_usages": http_usages,
        "turn_sources": [t.get("usageSource") for t in turns],
        "turn_misses": [t.get("cacheMiss") for t in turns],
        "step_misses": [s.get("cacheMiss") for s in db_steps],
        "reported_ctx": [live.get("reportedContextUsed"), live.get("reportedContextSize")] if live else None,
        "quota_status": (dash.get("quota") or {}).get("status"),
        "generation_requests": 0,
    }


def scenario_db_step_timing_and_thinking(port, dash_port):
    """Authoritative TTFT, streamMs, decodeTps, and thinking telemetry from gen_metadata + steps."""
    sid = f"e2e-db-timing-{time.time_ns()}"
    prompt = (
        "[FAKE_THINK] [FAKE_THINKING_MS:90] [FAKE_INPUT_TOKENS:8000] [FAKE_CACHE_READ:32000] "
        "[FAKE_OUTPUT_TOKENS:80] [FAKE_THOUGHT_TOKENS:32] [FAKE_TTFT_MS:150] [FAKE_STREAM_MS:500] [FAKE_REAL_TIMING]"
    )
    st, _, data = E.post(port, "/v1/messages", {
        "model": "gemini-4-hydrogen",
        "max_tokens": 1024,
        "output_config": {"effort": "max"},
        "thinking": {"type": "enabled", "budget_tokens": 16000},
        "system": E.SYS,
        "tools": [E.BASH_TOOL],
        "messages": [{"role": "user", "content": prompt}],
    }, sid)
    if st != 200 or not isinstance(data, dict):
        return {"ok": False, "status": st, "reason": "Hydrogen thinking turn failed"}

    has_think_block = any(c.get("type") == "thinking" and c.get("thinking") for c in data.get("content", []))
    d_status, dash = _get_dash(dash_port, "/api/dashboard?refresh=1")
    if d_status != 200 or not isinstance(dash, dict):
        return {"ok": False, "reason": f"dashboard returned {d_status}"}

    turns = _turns_for_sid(dash, sid)
    turn = turns[-1] if turns else {}
    acp_sid = turn.get("sessionId", "")
    steps = _steps_for_acp_sid(dash, acp_sid)
    step = steps[-1] if steps else {}

    ok = (
        has_think_block
        and turn.get("usageSource") == "db"
        and turn.get("thoughtLevel") == "xhigh"
        and turn.get("hasThinking") is True
        and turn.get("thinkingTokens") == 32
        and turn.get("thinkingMs", 0) >= 40
        and turn.get("upstreamTtftMs") == 150
        and turn.get("streamMs") == 500
        and turn.get("decodeTps") == 160.0
        and step.get("modelId") == "gemini-4-hydrogen"
        and step.get("modelTier") == "xhigh"
        and step.get("ttftMs") == 150
        and step.get("streamMs") == 500
        and step.get("elapsedMs") == 650
        and step.get("outTok") == 80
        and step.get("thTok") == 32
        and step.get("visOutTok") == 48
        and step.get("thinkingMs") == 200
        and step.get("visibleStreamMs") == 300
        and step.get("decodeTps") == 160.0
    )
    return {
        "ok": ok,
        "turn": {k: turn.get(k) for k in ("usageSource", "thoughtLevel", "upstreamTtftMs", "streamMs", "thinkingMs", "thinkingTokens", "decodeTps")},
        "step": {k: step.get(k) for k in ("modelId", "modelTier", "ttftMs", "streamMs", "elapsedMs", "thinkingMs", "visibleStreamMs", "thTok", "visOutTok", "decodeTps")},
        "generation_requests": 0,
    }


def scenario_db_tool_roundtrip_billing(port, dash_port):
    """Both pre-MCP PlannerResponse (tool_use) and post-MCP completion (end_turn) bill via 'db'."""
    sid = f"e2e-db-tool-{time.time_ns()}"
    base = {"model": "claude-sonnet-5-5", "max_tokens": 1024, "system": E.SYS, "tools": [E.BASH_TOOL]}
    msgs = [{"role": "user", "content": "[FAKE_TOOL_CALL] Use the Bash tool to run exactly: echo FAKE_TOOL_OK"}]
    s1, _, d1 = E.post(port, "/v1/messages", {**base, "messages": msgs}, sid)
    tus = [c for c in (d1.get("content", []) if isinstance(d1, dict) else []) if c.get("type") == "tool_use"]
    if s1 != 200 or not tus:
        return {"ok": False, "status": s1, "reason": "missing tool_use on step 1"}
    msgs += [
        {"role": "assistant", "content": d1["content"]},
        {"role": "user", "content": [{"type": "tool_result", "tool_use_id": tus[0]["id"], "content": "FAKE_TOOL_OK"}]},
    ]
    s2, _, d2 = E.post(port, "/v1/messages", {**base, "messages": msgs}, sid)
    if s2 != 200 or "TOOL_DONE:FAKE_TOOL_OK" not in E.anthropic_text(d2):
        return {"ok": False, "status": s2, "reason": "missing TOOL_DONE on step 2"}

    d_status, dash = _get_dash(dash_port, "/api/dashboard?refresh=1")
    if d_status != 200 or not isinstance(dash, dict):
        return {"ok": False, "reason": f"dashboard returned {d_status}"}

    turns = _turns_for_sid(dash, sid)
    acp_sid = turns[-1].get("sessionId") if turns else ""
    steps = _steps_for_acp_sid(dash, acp_sid)
    rolling5h = (dash.get("windows") or {}).get("rolling5h") or {}

    ok = (
        len(turns) == 2
        and [t.get("stopReason") for t in turns] == ["tool_use", "end_turn"]
        and [t.get("usageSource") for t in turns] == ["db", "db"]
        and len(steps) == 2
        and all(s.get("modelId") == "claude-sonnet-5-5" and s.get("quotaGroup") == "3p" and s.get("sourceCategory") == "CLAUDE_GW" for s in steps)
        and rolling5h.get("thirdPartyPoolCostUsd", 0) > 0
    )
    return {
        "ok": ok,
        "turn_stops": [t.get("stopReason") for t in turns],
        "turn_sources": [t.get("usageSource") for t in turns],
        "db_steps_count": len(steps),
        "third_party_cost_usd": rolling5h.get("thirdPartyPoolCostUsd"),
        "generation_requests": 0,
    }


def scenario_served_model_canary_and_argon_guard(port, dash_port):
    """Carbon canary surfaces in canaryRoutes without rollover (U8), while Argon '-b' shadow self-heals."""
    # Part A: Carbon canary (UI observation only; strict model pinning U8)
    sid_canary = f"e2e-db-canary-{time.time_ns()}"
    st_c, _, data_c = E.post(port, "/v1/messages", {
        "model": "gemini-4-carbon-high",
        "max_tokens": 256,
        "system": E.SYS,
        "tools": [E.BASH_TOOL],
        "messages": [{
            "role": "user",
            "content": "[FAKE_SERVED_MODEL:gemini-4-carbon-exp] [FAKE_INPUT_TOKENS:4000] [FAKE_CACHE_READ:60000] [FAKE_OUTPUT_TOKENS:100]",
        }],
    }, sid_canary)
    if st_c != 200 or not isinstance(data_c, dict) or data_c.get("model") != "gemini-4-carbon":
        return {"ok": False, "status": st_c, "reason": "Carbon canary turn failed or broke model pinning"}

    # Part B: Argon shadow pool ('gemini-4-argon-b') transparent in-turn recovery onto primary 'gemini-4-argon'
    sid_argon = f"e2e-db-argon-{time.time_ns()}"
    st_a, _, data_a = E.post(port, "/v1/messages", {
        "model": "gemini-4-argon-high",
        "max_tokens": 256,
        "system": E.SYS,
        "tools": [E.BASH_TOOL],
        "messages": [{
            "role": "user",
            "content": f"[FAKE_SERVED_SHADOW_ONCE] nonce={sid_argon} [FAKE_INPUT_TOKENS:6000] [FAKE_CACHE_READ:50000] [FAKE_OUTPUT_TOKENS:90]",
        }],
    }, sid_argon)
    if st_a != 200 or not isinstance(data_a, dict) or data_a.get("model") != "gemini-4-argon":
        return {"ok": False, "status": st_a, "reason": "Argon shadow recovery turn failed"}

    d_status, dash = _get_dash(dash_port, "/api/dashboard?refresh=1")
    m_status, metrics_raw = _get_dash(dash_port, "/metrics")
    if d_status != 200 or not isinstance(dash, dict) or m_status != 200 or not isinstance(metrics_raw, str):
        return {"ok": False, "reason": f"dashboard/metrics failed: {d_status}/{m_status}"}

    live_c = _live_for_sid(dash, sid_canary) or {}
    turns_c = _turns_for_sid(dash, sid_canary)
    steps_c = _steps_for_acp_sid(dash, live_c.get("sessionId"))
    canary_routes = dash.get("canaryRoutes") or []
    matched_route = next(
        (r for r in canary_routes if r.get("modelId") == "gemini-4-carbon" and r.get("servedModel") == "gemini-4-carbon-exp"),
        None,
    )

    turns_a = _turns_for_sid(dash, sid_argon)
    turn_a = turns_a[-1] if turns_a else {}
    gw_meta = dash.get("gateway") or {}

    ok = (
        live_c.get("servedModel") == "gemini-4-carbon-exp"
        and live_c.get("servedMismatch") is True
        and len(turns_c) == 1
        and turns_c[0].get("mode") == "cold_start"
        and len(steps_c) == 1
        and steps_c[0].get("servedModel") == "gemini-4-carbon-exp"
        and steps_c[0].get("servedMismatch") is True
        and matched_route is not None
        and matched_route.get("live") is True
        and matched_route.get("steps", 0) >= 1
        and turn_a.get("mode") == "argon_degraded_recovery"
        and turn_a.get("usageSource") == "db"
        and turn_a.get("servedModel") == "gemini-4-argon"
        and gw_meta.get("argonDegradationDetected", 0) >= 1
        and gw_meta.get("argonDegradationRecovered", 0) >= 1
        and "agy_gateway_argon_degradation_detected_total" in metrics_raw
        and "agy_gateway_argon_degradation_recovered_total" in metrics_raw
    )
    return {
        "ok": ok,
        "carbon_canary": {
            "live_served": live_c.get("servedModel"),
            "live_mismatch": live_c.get("servedMismatch"),
            "canary_route": matched_route,
        },
        "argon_recovery": {
            "turn_mode": turn_a.get("mode"),
            "served_model": turn_a.get("servedModel"),
            "detected_total": gw_meta.get("argonDegradationDetected"),
            "recovered_total": gw_meta.get("argonDegradationRecovered"),
        },
        "generation_requests": 0,
    }


def run_all(port, mode, receipt):
    if mode != "fake":
        raise E.GateError("e2e_db_telemetry only runs in fake mode (zero live quota cost)")
    dp = _dash_port(port, receipt)
    return [
        ("db_billing_and_cache_lifecycle", lambda _m, r: scenario_db_billing_and_cache_lifecycle(port, dp, r)),
        ("db_step_timing_and_thinking", lambda _m, _r: scenario_db_step_timing_and_thinking(port, dp)),
        ("db_tool_roundtrip_billing", lambda _m, _r: scenario_db_tool_roundtrip_billing(port, dp)),
        ("served_model_canary_and_argon_guard", lambda _m, _r: scenario_served_model_canary_and_argon_guard(port, dp)),
    ]


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("port", type=int)
    E.add_mode_args(ap)
    args = ap.parse_args(argv)
    if args.live:
        print(json.dumps({"ok": False, "error": "GateError: e2e_db_telemetry refuses --live mode"}))
        return 2
    try:
        mode, receipt = E.resolve_mode(args, args.port)
        scenarios = run_all(args.port, mode, receipt)
    except (OSError, ValueError, KeyError) as exc:
        print(json.dumps({"ok": False, "error": f"{type(exc).__name__}: {exc}"}))
        return 2
    return E.run_main(scenarios, args, args.port)


if __name__ == "__main__":
    raise SystemExit(main())
