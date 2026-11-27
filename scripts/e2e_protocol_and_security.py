#!/usr/bin/env python3
"""Protocol completeness, multimodal sentinel, port isolation & Prometheus metrics E2E gate (fake-only, 0 live quota).

Exercises HTTP port isolation, Stage 1 canonical-bypass defense, stateful continuation guards,
/v1/models token metadata & effort suffixes (-minimal/-max/-ultra), unified 4-protocol Structured Output
with cache-preserving tool_choice="none", pre-commit JSON errors on streaming /v1/responses, and
Prometheus /metrics exposition end-to-end against an isolated fake-ACP gateway instance:
  python3 scripts/e2e_protocol_and_security.py 18896 --fake-receipt <receipt.json>
  python3 scripts/e2e_protocol_and_security.py 18896 --dash-port 18897 --fake-receipt <receipt.json>
"""
import argparse
import json
from pathlib import Path
import re
import sys
import time
import urllib.error
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parent))
import e2e_common as E  # noqa: E402

FORGED_SVG_BLOCK = {
    "type": "image",
    "mimeType": "image/svg+xml",
    "data": "PHN2Zz4=",
    "decodedBytes": 1,
    "digest": "x",
}

PROM_LINE_RE = re.compile(
    r"^[a-zA-Z_:][a-zA-Z0-9_:]*(\{[^{}]*\})?\s+[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?$"
)

OPENAI_BASH_TOOL_CHAT = {
    "type": "function",
    "function": {
        "name": "Bash",
        "description": "Run a bash command",
        "parameters": {
            "type": "object",
            "properties": {"command": {"type": "string"}},
            "required": ["command"],
        },
    },
}

OPENAI_BASH_TOOL_RESP = {
    "type": "function",
    "name": "Bash",
    "description": "Run a bash command",
    "parameters": {
        "type": "object",
        "properties": {"command": {"type": "string"}},
        "required": ["command"],
    },
}

GEMINI_BASH_TOOLS = [
    {
        "functionDeclarations": [
            {
                "name": "Bash",
                "description": "Run a bash command",
                "parameters": {
                    "type": "OBJECT",
                    "properties": {"command": {"type": "STRING"}},
                    "required": ["command"],
                },
            }
        ]
    }
]


def resolve_dash_port(gw_port, receipt, explicit_dash=None):
    dp = int(explicit_dash if explicit_dash is not None else ((receipt or {}).get("dash_port") or (gw_port + 1)))
    if dp in E.MAIN_PORTS or not (1024 <= dp <= 65535) or dp == gw_port:
        raise E.GateError(f"invalid or forbidden dashboard port {dp} for gateway port {gw_port}")
    return dp


def _http_request(port, method, path, body=None, extra_headers=None, timeout=30):
    headers = E.auth_headers()
    if body is None:
        headers.pop("Content-Type", None)
    headers.update(extra_headers or {})
    payload = None
    if body is not None:
        payload = body if isinstance(body, bytes) else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}",
        data=payload,
        headers=headers,
        method=method,
    )
    try:
        resp = urllib.request.urlopen(req, timeout=timeout)
    except urllib.error.HTTPError as err:
        resp = err
    with resp:
        raw = resp.read().decode("utf-8", "replace")
        ctype = resp.headers.get("Content-Type", "")
        try:
            parsed = json.loads(raw)
        except ValueError:
            parsed = raw
        return resp.status, ctype, parsed


def scenario_port_isolation(gw_port, dash_port):
    """Admin/telemetry routes 404 on <gw_port>; inference routes 404 on <dash_port>; /api/upgrade 200 on <dash_port>."""
    gw_up_st, gw_up_ct, gw_up_data = _http_request(gw_port, "GET", "/api/upgrade")
    gw_app_st, gw_app_ct, gw_app_data = _http_request(gw_port, "POST", "/api/upgrade/approve", {})
    gw_dash_st, _, _ = _http_request(gw_port, "GET", "/api/dashboard")
    gw_met_st, _, _ = _http_request(gw_port, "GET", "/metrics")
    gw_root_st, _, _ = _http_request(gw_port, "GET", "/")
    gw_hlz_st, _, gw_hlz_data = _http_request(gw_port, "GET", "/healthz")

    dash_up_st, dash_up_ct, dash_up_data = _http_request(dash_port, "GET", "/api/upgrade")
    dash_mod_st, _, _ = _http_request(dash_port, "GET", "/v1/models")
    dash_msg_st, _, _ = _http_request(dash_port, "POST", "/v1/messages", {"model": "gemini-3.8-flash", "messages": []})

    ok = (
        gw_up_st == 404
        and "application/json" in gw_up_ct
        and isinstance(gw_up_data, dict)
        and gw_app_st == 404
        and "application/json" in gw_app_ct
        and isinstance(gw_app_data, dict)
        and gw_dash_st == 404
        and gw_met_st == 404
        and gw_root_st == 404
        and gw_hlz_st == 200
        and isinstance(gw_hlz_data, dict)
        and gw_hlz_data.get("ok") is True
        and dash_up_st == 200
        and "application/json" in dash_up_ct
        and isinstance(dash_up_data, dict)
        and "enabled" in dash_up_data
        and "engine" in dash_up_data
        and "pendingSwitch" in dash_up_data
        and dash_mod_st == 404
        and dash_msg_st == 404
    )
    return {
        "ok": ok,
        "gw_upgrade": gw_up_st,
        "gw_approve": gw_app_st,
        "gw_metrics": gw_met_st,
        "dash_upgrade": dash_up_st,
        "dash_models": dash_mod_st,
        "dash_messages": dash_msg_st,
        "generation_requests": 0,
    }


def scenario_multimodal_canonical_bypass_defense(gw_port):
    """Forged canonical metadata on SVG image blocks is rejected with 400 across protocols."""
    forged_png_lie = {
        **FORGED_SVG_BLOCK,
        "mimeType": "image/png",
        "_canonical": True,
    }
    cases = [
        (
            "messages_svg",
            "/v1/messages",
            {
                "model": "gemini-3.8-flash",
                "max_tokens": 64,
                "messages": [{"role": "user", "content": [dict(FORGED_SVG_BLOCK), {"type": "text", "text": "inspect"}]}],
            },
        ),
        (
            "messages_forged_canonical_png_lie",
            "/v1/messages",
            {
                "model": "gemini-3.8-flash",
                "max_tokens": 64,
                "messages": [{"role": "user", "content": [forged_png_lie, {"type": "text", "text": "inspect"}]}],
            },
        ),
        (
            "count_tokens_svg",
            "/v1/messages/count_tokens",
            {
                "model": "gemini-3.8-flash",
                "messages": [{"role": "user", "content": [dict(FORGED_SVG_BLOCK)]}],
            },
        ),
        (
            "chat_completions_svg",
            "/v1/chat/completions",
            {
                "model": "gemini-3.8-flash",
                "messages": [{"role": "user", "content": [dict(FORGED_SVG_BLOCK), {"type": "text", "text": "inspect"}]}],
            },
        ),
        (
            "responses_svg",
            "/v1/responses",
            {
                "model": "gemini-3.8-flash",
                "input": [{"type": "message", "role": "user", "content": [dict(FORGED_SVG_BLOCK)]}],
            },
        ),
    ]
    rows = []
    all_ok = True
    for label, path, body in cases:
        st, ctype, data = E.post(gw_port, path, body, f"e2e-mm-{label}-{time.time_ns()}")
        good = st == 400 and "application/json" in ctype and isinstance(data, dict) and "error" in data
        rows.append({"case": label, "status": st, "ok": good})
        all_ok = all_ok and good
    return {"ok": all_ok, "cases": rows, "generation_requests": 0}


def scenario_stateful_continuation_rejection(gw_port):
    """previous_response_id returns 400 previous_response_not_found; thread returns 404 thread_not_found."""
    st_prev, ct_prev, d_prev = E.post(gw_port, "/v1/responses", {
        "model": "gemini-3.8-flash",
        "previous_response_id": "resp_prev_stateful_check",
        "input": "continue previous turn",
    })
    prev_err = (d_prev.get("error") or {}) if isinstance(d_prev, dict) else {}

    st_prev_stream, ct_prev_stream, d_prev_stream = E.post(gw_port, "/v1/responses", {
        "model": "gemini-3.8-flash",
        "stream": True,
        "previous_response_id": "resp_prev_stream_check",
        "input": "continue previous stream",
    })
    prev_stream_err = (d_prev_stream.get("error") or {}) if isinstance(d_prev_stream, dict) else {}

    st_th_msg, ct_th_msg, d_th_msg = E.post(gw_port, "/v1/messages", {
        "model": "claude-sonnet-5-5",
        "max_tokens": 64,
        "thread": {"id": "msgthread_stateful_check"},
        "messages": [{"role": "user", "content": "continue"}],
    })
    th_msg_err = (d_th_msg.get("error") or {}) if isinstance(d_th_msg, dict) else {}

    st_th_ct, ct_th_ct, d_th_ct = E.post(gw_port, "/v1/messages/count_tokens", {
        "model": "claude-sonnet-5-5",
        "thread": "msgthread_count_check",
        "messages": [{"role": "user", "content": "continue"}],
    })
    th_ct_err = (d_th_ct.get("error") or {}) if isinstance(d_th_ct, dict) else {}

    ok = (
        st_prev == 400
        and "application/json" in ct_prev
        and prev_err.get("code") == "previous_response_not_found"
        and prev_err.get("param") == "previous_response_id"
        and st_prev_stream == 400
        and "application/json" in ct_prev_stream
        and prev_stream_err.get("code") == "previous_response_not_found"
        and st_th_msg == 404
        and "application/json" in ct_th_msg
        and th_msg_err.get("type") == "thread_not_found"
        and st_th_ct == 404
        and "application/json" in ct_th_ct
        and th_ct_err.get("type") == "thread_not_found"
    )
    return {
        "ok": ok,
        "responses_prev": {"status": st_prev, "code": prev_err.get("code")},
        "responses_prev_stream": {"status": st_prev_stream, "ctype": ct_prev_stream, "code": prev_stream_err.get("code")},
        "messages_thread": {"status": st_th_msg, "type": th_msg_err.get("type")},
        "count_tokens_thread": {"status": st_th_ct, "type": th_ct_err.get("type")},
        "generation_requests": 0,
    }


def scenario_models_metadata_and_effort_suffixes(gw_port):
    """/v1/models exposes max_input_tokens & max_tokens; -max/-minimal/-ultra suffixes resolve cleanly."""
    m_st, m_ct, m_data = _http_request(gw_port, "GET", "/v1/models")
    models = (m_data.get("data") or []) if isinstance(m_data, dict) else []
    models_ok = (
        m_st == 200
        and "application/json" in m_ct
        and len(models) >= 6
        and all(
            isinstance(item, dict)
            and isinstance(item.get("context_window"), int)
            and item.get("context_window", 0) >= 900000
            and item.get("max_input_tokens") == item.get("context_window")
            and item.get("max_tokens") == 65536
            for item in models
        )
    )

    gm_st, gm_ct, gm_data = _http_request(gw_port, "GET", "/v1beta/models")
    gmodels = (gm_data.get("models") or []) if isinstance(gm_data, dict) else []
    gmodels_ok = (
        gm_st == 200
        and "application/json" in gm_ct
        and len(gmodels) >= 6
        and all(
            isinstance(item, dict)
            and int(item.get("inputTokenLimit") or 0) >= 900000
            and item.get("outputTokenLimit") == 65536
            for item in gmodels
        )
    )

    suffix_cases = [
        ("claude-opus-5-5-max", "/v1/messages", ("claude-opus-5-5", "high")),
        ("gemini-4-hydrogen-max", "/v1/chat/completions", ("gemini-4-hydrogen", "xhigh")),
        ("gemini-4-hydrogen-ultra", "/v1/responses", ("gemini-4-hydrogen", "xhigh")),
        ("gemini-3.8-flash-minimal", "/v1/messages", ("gemini-3.8-flash", "low")),
    ]
    suffix_rows = []
    suffixes_ok = True
    for req_model, path, (want_model, want_tier) in suffix_cases:
        sid = f"e2e-sfx-{req_model}-{time.time_ns()}"
        if path == "/v1/messages":
            body = {"model": req_model, "max_tokens": 128, "system": E.SYS, "messages": [{"role": "user", "content": "Reply OK"}]}
        elif path == "/v1/chat/completions":
            body = {"model": req_model, "messages": [{"role": "system", "content": E.SYS}, {"role": "user", "content": "Reply OK"}]}
        else:
            body = {"model": req_model, "instructions": E.SYS, "input": "Reply OK"}
        st, _, data = E.post(gw_port, path, body, sid)
        txt = (
            E.anthropic_text(data) if path == "/v1/messages"
            else E.chat_text(data) if path == "/v1/chat/completions"
            else E.responses_text(data)
        )
        ident = E.fake_identity(txt)
        good = st == 200 and ident is not None and ident[:2] == (want_model, want_tier)
        suffix_rows.append({"requested": req_model, "status": st, "executed": ident, "ok": good})
        suffixes_ok = suffixes_ok and good

    return {
        "ok": models_ok and gmodels_ok and suffixes_ok,
        "v1_models_count": len(models),
        "v1beta_models_count": len(gmodels),
        "suffixes": suffix_rows,
        "generation_requests": 0,
    }


def scenario_structured_output_and_tool_choice_none(gw_port):
    """Unified Structured Output + tool_choice='none' succeed (200 OK) across all 4 protocols & keep warm cache."""
    schema = {
        "type": "object",
        "properties": {"ok": {"type": "boolean"}, "summary": {"type": "string"}},
        "required": ["ok", "summary"],
    }

    # 1. /v1/messages/count_tokens accounts for output_config.format schema
    _, _, ct_base = E.post(gw_port, "/v1/messages/count_tokens", {
        "model": "claude-sonnet-5-5",
        "messages": [{"role": "user", "content": "Summarize status"}],
    })
    _, _, ct_so = E.post(gw_port, "/v1/messages/count_tokens", {
        "model": "claude-sonnet-5-5",
        "output_config": {"format": {"type": "json_schema", "schema": schema}},
        "messages": [{"role": "user", "content": "Summarize status"}],
    })
    ct_ok = (
        isinstance(ct_base, dict)
        and isinstance(ct_so, dict)
        and int(ct_so.get("input_tokens") or 0) > int(ct_base.get("input_tokens") or 0)
    )

    # 2. Anthropic /v1/messages: output_config.format + tool_choice="none" -> warm follow-up tool_choice="auto" (turn=2)
    sid_anth = f"e2e-so-anth-{time.time_ns()}"
    anth_t1_body = {
        "model": "gemini-3.8-flash",
        "max_tokens": 128,
        "system": E.SYS,
        "tools": [E.BASH_TOOL],
        "tool_choice": "none",
        "output_config": {"format": {"type": "json_schema", "schema": schema}},
        "messages": [{"role": "user", "content": "[FAKE_TOOL_CALL] summarize without calling tools"}],
    }
    st_a1, _, d_a1 = E.post(gw_port, "/v1/messages", anth_t1_body, sid_anth)
    txt_a1 = E.anthropic_text(d_a1)
    anth_t1_ok = (
        st_a1 == 200
        and isinstance(d_a1, dict)
        and d_a1.get("stop_reason") == "end_turn"
        and "tool_choice=none" in txt_a1
    )
    st_a2, _, d_a2 = (0, "", {})
    txt_a2 = ""
    if anth_t1_ok:
        anth_t2_body = {
            "model": "gemini-3.8-flash",
            "max_tokens": 128,
            "system": E.SYS,
            "tools": [E.BASH_TOOL],
            "tool_choice": "auto",
            "output_config": {"format": {"type": "json_schema", "schema": schema}},
            "messages": [
                {"role": "user", "content": "[FAKE_TOOL_CALL] summarize without calling tools"},
                {"role": "assistant", "content": d_a1["content"]},
                {"role": "user", "content": "Follow up on warm session"},
            ],
        }
        st_a2, _, d_a2 = E.post(gw_port, "/v1/messages", anth_t2_body, sid_anth)
        txt_a2 = E.anthropic_text(d_a2)
    anth_warm_ok = anth_t1_ok and st_a2 == 200 and E.fake_identity(txt_a2) == ("gemini-3.8-flash", "medium", 2)

    # 3. OpenAI /v1/chat/completions: response_format + tool_choice="none"
    sid_chat = f"e2e-so-chat-{time.time_ns()}"
    st_chat, _, d_chat = E.post(gw_port, "/v1/chat/completions", {
        "model": "gemini-3.8-flash",
        "tools": [OPENAI_BASH_TOOL_CHAT],
        "tool_choice": "none",
        "response_format": {"type": "json_schema", "json_schema": {"name": "status_report", "schema": schema}},
        "messages": [
            {"role": "system", "content": E.SYS},
            {"role": "user", "content": "[FAKE_TOOL_CALL] summarize in JSON"},
        ],
    }, sid_chat)
    txt_chat = E.chat_text(d_chat)
    chat_msg = (((d_chat.get("choices") or [{}])[0]).get("message") or {}) if isinstance(d_chat, dict) else {}
    chat_ok = (
        st_chat == 200
        and not chat_msg.get("tool_calls")
        and "tool_choice=none" in txt_chat
    )

    # 4. OpenAI /v1/responses: text.format + tool_choice="none"
    sid_resp = f"e2e-so-resp-{time.time_ns()}"
    st_resp, _, d_resp = E.post(gw_port, "/v1/responses", {
        "model": "gemini-3.8-flash",
        "instructions": E.SYS,
        "tools": [OPENAI_BASH_TOOL_RESP],
        "tool_choice": "none",
        "text": {"format": {"type": "json_schema", "name": "status_report", "schema": schema}},
        "input": "[FAKE_TOOL_CALL] summarize in JSON",
    }, sid_resp)
    txt_resp = E.responses_text(d_resp)
    resp_items = (d_resp.get("output") or []) if isinstance(d_resp, dict) else []
    resp_ok = (
        st_resp == 200
        and isinstance(d_resp, dict)
        and d_resp.get("status") == "completed"
        and d_resp.get("store") is False
        and d_resp.get("tool_choice") == "none"
        and not any(isinstance(it, dict) and it.get("type") in ("function_call", "custom_tool_call") for it in resp_items)
        and "tool_choice=none" in txt_resp
    )

    # 5. Gemini /v1beta: generationConfig.responseSchema + toolConfig.functionCallingConfig.mode="NONE"
    sid_gem = f"e2e-so-gem-{time.time_ns()}"
    st_gem, _, d_gem = E.post(gw_port, "/v1beta/models/gemini-3.8-flash:generateContent", {
        "systemInstruction": {"parts": [{"text": E.SYS}]},
        "tools": GEMINI_BASH_TOOLS,
        "toolConfig": {"functionCallingConfig": {"mode": "NONE"}},
        "generationConfig": {
            "responseMimeType": "application/json",
            "responseSchema": {
                "type": "OBJECT",
                "properties": {"ok": {"type": "BOOLEAN"}, "summary": {"type": "STRING"}},
                "required": ["ok", "summary"],
            },
        },
        "contents": [{"role": "user", "parts": [{"text": "[FAKE_TOOL_CALL] summarize in JSON"}]}],
    }, sid_gem)
    txt_gem = E.gemini_text(d_gem)
    gem_parts = ((((d_gem.get("candidates") or [{}])[0]).get("content") or {}).get("parts") or []) if isinstance(d_gem, dict) else []
    gem_ok = (
        st_gem == 200
        and not any(isinstance(p, dict) and "functionCall" in p for p in gem_parts)
        and "tool_choice=none" in txt_gem
    )

    return {
        "ok": ct_ok and anth_warm_ok and chat_ok and resp_ok and gem_ok,
        "count_tokens_schema": ct_ok,
        "anthropic_warm_reuse": anth_warm_ok,
        "chat_completions": chat_ok,
        "responses": resp_ok,
        "gemini_v1beta": gem_ok,
        "generation_requests": 0,
    }


def scenario_responses_stream_precommit_error(gw_port):
    """/v1/responses stream=true pre-commit errors return application/json; post-commit errors emit SSE response.failed."""
    # 1. Stage 1 validation error before SSE commit
    st_val, ct_val, d_val = E.post(gw_port, "/v1/responses", {
        "model": "gemini-3.8-flash",
        "stream": True,
        "input": [{"type": "message", "role": "user", "content": [dict(FORGED_SVG_BLOCK)]}],
    }, f"e2e-resp-pre-val-{time.time_ns()}")

    # 2. Upstream pre-commit RPC 400 error before any SSE chunk is emitted
    st_rpc, ct_rpc, d_rpc = E.post(gw_port, "/v1/responses", {
        "model": "gemini-3.8-flash",
        "stream": True,
        "instructions": E.SYS,
        "input": "[FAKE_RPC_400]",
    }, f"e2e-resp-pre-rpc-{time.time_ns()}")

    # 3. Post-commit late notice after SSE stream already committed
    st_late, ct_late, d_late = E.post(gw_port, "/v1/responses", {
        "model": "gemini-3.8-flash",
        "stream": True,
        "instructions": E.SYS,
        "input": "[FAKE_LATE_NOTICE]",
    }, f"e2e-resp-post-late-{time.time_ns()}")
    events_late = E.sse_events(d_late) if isinstance(d_late, str) else []
    has_resp_failed = any(
        ev == "response.failed" or (isinstance(d, dict) and d.get("type") == "response.failed")
        for ev, d in events_late
    )

    ok = (
        st_val == 400
        and "application/json" in ct_val
        and "text/event-stream" not in ct_val
        and isinstance(d_val, dict)
        and "error" in d_val
        and st_rpc == 400
        and "application/json" in ct_rpc
        and "text/event-stream" not in ct_rpc
        and isinstance(d_rpc, dict)
        and "error" in d_rpc
        and st_late == 200
        and ct_late.startswith("text/event-stream")
        and has_resp_failed
    )
    return {
        "ok": ok,
        "precommit_validation": {"status": st_val, "content_type": ct_val},
        "precommit_rpc_400": {"status": st_rpc, "content_type": ct_rpc},
        "postcommit_late_notice": {"status": st_late, "content_type": ct_late, "response_failed": has_resp_failed},
        "generation_requests": 0,
    }


def parse_prometheus_text(raw):
    """Validate Prometheus exposition text and return (bad_lines, metric_values_by_name)."""
    bad_lines = []
    metrics = {}
    for line in (raw or "").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        m = PROM_LINE_RE.match(s)
        if not m:
            bad_lines.append(s[:160])
            continue
        head, val_str = s.rsplit(None, 1)
        base_name = head.split("{", 1)[0]
        metrics.setdefault(base_name, []).append((head, float(val_str)))
    return bad_lines, metrics


def scenario_prometheus_metrics(dash_port):
    """<dash_port>/metrics exposes valid Prometheus counters and gauges reflecting gateway traffic."""
    st, ctype, raw = _http_request(dash_port, "GET", "/metrics")
    if st != 200 or not isinstance(raw, str):
        return {"ok": False, "status": st, "content_type": ctype, "generation_requests": 0}
    bad_lines, metrics = parse_prometheus_text(raw)
    required = [
        "agy_gateway_store_ready",
        "agy_gateway_live_workers",
        "agy_gateway_hibernated_sessions",
        "agy_gateway_max_live_workers",
        "agy_gateway_prompts_inflight",
        "agy_gateway_max_concurrent_prompts",
        "agy_gateway_parked_calls",
        "agy_gateway_requests_total",
        "agy_gateway_engine_exits_total",
        "agy_gateway_worker_cap_rejections_total",
        "agy_gateway_soft_detach_total",
        "agy_gateway_compaction_rollovers_total",
        "agy_gateway_session_reuses_total",
        "agy_gateway_argon_degradation_detected_total",
        "agy_gateway_argon_degradation_recovered_total",
        "agy_gateway_cache_burn_warnings_total",
        "agy_gateway_orphan_relay_caps_total",
        "agy_gateway_turns_total",
        "agy_gateway_tokens_total",
    ]
    missing = [name for name in required if name not in metrics]
    store_ready = metrics.get("agy_gateway_store_ready", [(None, 0.0)])[0][1]
    req_total = metrics.get("agy_gateway_requests_total", [(None, 0.0)])[0][1]
    reuses_total = metrics.get("agy_gateway_session_reuses_total", [(None, 0.0)])[0][1]

    ok = (
        ctype.startswith("text/plain")
        and not bad_lines
        and not missing
        and store_ready == 1.0
        and req_total >= 10.0
        and reuses_total >= 1.0
    )
    return {
        "ok": ok,
        "content_type": ctype,
        "bad_lines": bad_lines[:5],
        "missing": missing,
        "store_ready": store_ready,
        "requests_total": req_total,
        "session_reuses_total": reuses_total,
        "metric_families": len(metrics),
        "generation_requests": 0,
    }


def run_all(gw_port, mode, receipt, explicit_dash=None):
    if mode != "fake":
        raise E.GateError("e2e_protocol_and_security only runs in fake mode (zero live quota cost)")
    dp = resolve_dash_port(gw_port, receipt, explicit_dash)
    return [
        ("port_isolation", lambda _m, _r: scenario_port_isolation(gw_port, dp)),
        ("multimodal_canonical_bypass_defense", lambda _m, _r: scenario_multimodal_canonical_bypass_defense(gw_port)),
        ("stateful_continuation_rejection", lambda _m, _r: scenario_stateful_continuation_rejection(gw_port)),
        ("models_metadata_and_effort_suffixes", lambda _m, _r: scenario_models_metadata_and_effort_suffixes(gw_port)),
        ("structured_output_and_tool_choice_none", lambda _m, _r: scenario_structured_output_and_tool_choice_none(gw_port)),
        ("responses_stream_precommit_error", lambda _m, _r: scenario_responses_stream_precommit_error(gw_port)),
        ("prometheus_metrics", lambda _m, _r: scenario_prometheus_metrics(dp)),
    ]


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("port", type=int, help="isolated fake-ACP gateway API port")
    ap.add_argument("dash_port_pos", nargs="?", type=int, help="optional dashboard port (defaults to receipt dash_port or port+1)")
    ap.add_argument("--dash-port", dest="dash_port_opt", type=int, help="optional dashboard port override")
    E.add_mode_args(ap)
    args = ap.parse_args(argv)
    if args.live:
        print(json.dumps({"ok": False, "error": "GateError: e2e_protocol_and_security refuses --live mode"}))
        return 2
    explicit_dash = args.dash_port_opt if args.dash_port_opt is not None else args.dash_port_pos
    try:
        mode, receipt = E.resolve_mode(args, args.port)
        scenarios = run_all(args.port, mode, receipt, explicit_dash)
    except (OSError, ValueError, KeyError) as exc:
        print(json.dumps({"ok": False, "error": f"{type(exc).__name__}: {exc}"}))
        return 2
    return E.run_main(scenarios, args, args.port)


if __name__ == "__main__":
    raise SystemExit(main())
