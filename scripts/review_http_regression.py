#!/usr/bin/env python3
"""HTTP regressions from the 2026-11-11 review (R1, R3, R5, R6, R7) against fake ACP instances only.

Each scenario starts (or reuses) an isolated gateway on --port with scripts/review_fault_acp.py as the ACP
binary in the scenario's fault mode, sends real HTTP requests to the normal entrypoints, and prints PASS/FAIL.
Exit code: 0 all selected scenarios pass, 1 any fails, 2 bad arguments.

  python3 scripts/review_http_regression.py --list
  python3 scripts/review_http_regression.py [--port 18894] [--dir /tmp/agy-review-regression] [--rebuild] [scenario ...]
"""
import argparse
import itertools
import json
import os
from pathlib import Path
import sys
import time

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import e2e_common as E  # noqa: E402
import fake_instance as F  # noqa: E402

WRAPPER = HERE/"review_fault_acp.py"
SYS = "Primary working directory: /tmp"
CC_SYS = "You are Claude Code, Anthropic's official CLI for Claude.\n" + SYS
NOTICE_FRAGMENT = "network issue connecting to the server"


def tool(name):
    return {"name": name, "description": "Review fixture " + name, "input_schema": {"type": "object", "properties": {}}}


def body(messages, model="gemini-3.8-flash", **kw):
    return dict(model=model, max_tokens=128, system=SYS, messages=messages, **kw)


def sid(name):
    return f"rv-{name}-{time.time_ns()}"


# ------------------------------------------------------------------ R6: utility probe fast path

def quoted(prompt):
    def run(port):
        status, _, data = E.post(port, "/v1/messages", body([{"role": "user", "content": prompt}]), sid("quoted"))
        text = E.anthropic_text(data)
        return {"ok": status == 200 and "FAKE_OK" in text, "status": status, "text": text[:160],
                "expect": "quoted marker must reach the model"}
    return run


def fastpath_openai(kind, stream):
    def run(port):
        if kind == "chat":
            path, b = "/v1/chat/completions", {"model": "gemini-3.8-flash", "stream": stream, "max_tokens": 1,
                                               "messages": [{"role": "system", "content": SYS}, {"role": "user", "content": "Return the digit 7"}]}
        else:
            path, b = "/v1/responses", {"model": "gemini-3.8-flash", "stream": stream, "max_tokens": 1,
                                        "instructions": SYS, "input": "Return the digit 7"}
        status, ctype, data = E.post(port, path, b, sid("fast-" + kind))
        if stream:
            raw = data if isinstance(data, str) else json.dumps(data)
            events = E.sse_events(raw) if isinstance(data, str) else []
            if kind == "chat":
                text = "".join((c.get("delta") or {}).get("content") or "" for _, d in events if isinstance(d, dict) for c in d.get("choices", []))
                shaped = any(isinstance(d, dict) and d.get("object") == "chat.completion.chunk" for _, d in events) and \
                    any(d == "[DONE]" for _, d in events)
            else:
                done = [d for ev, d in events if isinstance(d, dict) and d.get("type") == "response.completed"]
                text = E.responses_text(done[0]["response"]) if done else ""
                shaped = bool(done)
            ok = status == 200 and ctype.startswith("text/event-stream") and shaped and "FAKE_OK" in text
        else:
            want = "chat.completion" if kind == "chat" else "response"
            text = E.chat_text(data) if kind == "chat" else E.responses_text(data)
            ok = status == 200 and isinstance(data, dict) and data.get("object") == want and "FAKE_OK" in text
        return {"ok": ok, "status": status, "content_type": ctype, "text": text[:120],
                "expect": f"{kind} max_tokens=1 uses its own protocol and reaches the model"}
    return run


def fastpath_messages_plain(port):
    status, _, data = E.post(port, "/v1/messages", {**body([{"role": "user", "content": "Return the digit 7"}]), "max_tokens": 1}, sid("fast-plain"))
    text = E.anthropic_text(data)
    return {"ok": status == 200 and "FAKE_OK" in text, "status": status, "text": text[:120],
            "expect": "legal max_tokens=1 without Claude Code identity reaches the model"}


def fastpath_messages_probe(stream):
    def run(port):
        prompt = "[SUGGESTION MODE: Suggest what the user might naturally type next into Claude Code.]"
        b = dict(model="claude-sonnet-5-5", max_tokens=64, system=CC_SYS, stream=stream,
                 messages=[{"role": "user", "content": "Fix the bug."}, {"role": "assistant", "content": "Done."},
                           {"role": "user", "content": prompt}])
        status, ctype, data = E.post(port, "/v1/messages", b, sid("fast-probe"))
        if stream:
            events = E.sse_events(data) if isinstance(data, str) else []
            text = "".join(d.get("delta", {}).get("text", "") for _, d in events if isinstance(d, dict) and d.get("type") == "content_block_delta")
            shaped = any(isinstance(d, dict) and d.get("type") == "message_stop" for _, d in events)
        else:
            text = E.anthropic_text(data)
            shaped = isinstance(data, dict) and data.get("type") == "message" and data.get("stop_reason") == "end_turn"
        return {"ok": status == 200 and shaped and "FAKE_OK" not in text, "status": status, "content_type": ctype,
                "text": text[:120], "expect": "real Claude Code suggestion probe answered locally in Anthropic shape"}
    return run


# ------------------------------------------------------------------ R5: late agy notice

def late_notice(stream):
    def run(port):
        status, ctype, data = E.post(port, "/v1/messages", body([{"role": "user", "content": "Reply OK."}], stream=stream), sid("late"))
        if stream:
            events = E.sse_events(data) if isinstance(data, str) else []
            deltas = "".join(d.get("delta", {}).get("text", "") for _, d in events if isinstance(d, dict) and d.get("type") == "content_block_delta")
            has_error = any(ev == "error" or (isinstance(d, dict) and d.get("type") == "error") for ev, d in events)
            end_turn = any(isinstance(d, dict) and d.get("type") == "message_delta" and (d.get("delta") or {}).get("stop_reason") == "end_turn" for _, d in events)
            ok = status == 200 and NOTICE_FRAGMENT not in deltas and has_error and not end_turn
            return {"ok": ok, "notice_streamed": NOTICE_FRAGMENT in deltas, "error_event": has_error, "end_turn": end_turn,
                    "expect": "notice withheld, stream ends with a protocol error event, never end_turn"}
        text = E.anthropic_text(data)
        ok = status >= 400 and NOTICE_FRAGMENT not in text
        return {"ok": ok, "status": status, "text": text[:160], "expect": "non-stream late notice becomes an error status"}
    return run


# ------------------------------------------------------------------ R1: model confirmation

def switch_rejected(port):
    s = sid("switch")
    msgs = [{"role": "user", "content": "Remember BLUE-42. Reply OK."}]
    s1, _, first = E.post(port, "/v1/messages", body(msgs), s)
    if s1 != 200:
        return {"ok": False, "status_first": s1, "reason": "first turn failed"}
    msgs += [{"role": "assistant", "content": first["content"]}, {"role": "user", "content": "Now answer using Carbon."}]
    status, _, data = E.post(port, "/v1/messages", body(msgs, "gemini-4-carbon"), s)
    text = E.anthropic_text(data)
    ident = E.fake_identity(text)
    ok = status >= 400 or (status == 200 and ident is not None and ident[0] == "gemini-4-carbon")
    return {"ok": ok, "status": status, "executed": ident, "text": text[:160],
            "expect": "rejected switch must not answer 200 from another model"}


def empty_model_ack(port):
    status, _, data = E.post(port, "/v1/messages", body([{"role": "user", "content": "Reply OK."}], "gemini-4-argon"), sid("emptyack"))
    text = E.anthropic_text(data)
    ident = E.fake_identity(text)
    ok = status >= 400 or (status == 200 and ident is not None and ident[0] == "gemini-4-argon")
    return {"ok": ok, "status": status, "executed": ident, "text": text[:160],
            "expect": "empty set_config_option ack is not a confirmation"}


# ------------------------------------------------------------------ R3: tool inventory and system rules

def inventory_reply(text):
    out = {}
    for part in text.replace("INVENTORY ", "").split(" "):
        k, _, v = part.partition("=")
        out[k] = [x for x in v.split(",") if x]
    return out


def _inventory_turn(port, s, msgs, tools, rule):
    b = body(msgs, tools=[tool(t) for t in tools])
    b["system"] += "\n" + rule
    status, _, data = E.post(port, "/v1/messages", b, s)
    return status, data, inventory_reply(E.anthropic_text(data))


def inventory_first(port):
    s = sid("inv")
    status, _, inv = _inventory_turn(port, s, [{"role": "user", "content": "List tools and rules."}], ["Read"], "REVIEW_RULE_A")
    ok = status == 200 and inv.get("tools") == ["Read"] and "REVIEW_RULE_A" in inv.get("rules", [])
    return {"ok": ok, "status": status, "seen": inv}


def inventory_changed(port):
    s = sid("inv")
    msgs = [{"role": "user", "content": "List tools and rules."}]
    s1, first, inv1 = _inventory_turn(port, s, msgs, ["Read"], "REVIEW_RULE_A")
    if s1 != 200:
        return {"ok": False, "reason": "first turn failed", "status": s1}
    msgs += [{"role": "assistant", "content": first["content"]}, {"role": "user", "content": "Tools and rules changed. Inspect again."}]
    status, _, inv = _inventory_turn(port, s, msgs, ["Write"], "REVIEW_RULE_B")
    ok = status == 200 and inv.get("tools") == ["Write"] and "REVIEW_RULE_B" in inv.get("rules", [])
    return {"ok": ok, "status": status, "first": inv1, "seen": inv,
            "expect": "agy sees the new tool catalog (Write only) and the new system rule B"}


def inventory_removed_call(port):
    s = sid("inv-rm")
    msgs = [{"role": "user", "content": "List tools and rules."}]
    s1, first, _ = _inventory_turn(port, s, msgs, ["Read", "Write"], "REVIEW_RULE_A")
    if s1 != 200:
        return {"ok": False, "reason": "first turn failed", "status": s1}
    msgs += [{"role": "assistant", "content": first["content"]}, {"role": "user", "content": "[CALL_REMOVED:Write] try the removed tool."}]
    b = body(msgs, tools=[tool("Read")])
    b["system"] += "\nREVIEW_RULE_A"
    status, _, data = E.post(port, "/v1/messages", b, s, timeout=60)
    text = E.anthropic_text(data)
    tool_uses = [c.get("name") for c in (data.get("content", []) if isinstance(data, dict) else []) if c.get("type") == "tool_use"]
    ok = status == 200 and data.get("stop_reason") == "end_turn" and "Write" not in tool_uses and "isError=True" in text
    return {"ok": ok, "status": status, "stop": data.get("stop_reason") if isinstance(data, dict) else None,
            "tool_uses": tool_uses, "text": text[:200], "expect": "removed tool is rejected by MCP, never forwarded to the host"}


# ------------------------------------------------------------------ R7: gates must fail when behavior breaks

def gate_detects(fn):
    def run(port):
        res = fn(port)
        return {"ok": res.get("ok") is False, "gate_result_ok": res.get("ok"), "expect": "e2e gate reports failure"}
    return run


SCENARIOS = {
    # name: (fault mode, extra env, function)
    "quoted_suggestion": ("baseline", {}, quoted('Explain the literal string "[SUGGESTION MODE:" in this Go code.')),
    "quoted_spinner": ("baseline", {}, quoted('Translate the phrase "Describe your most recent action in 3-5 words" into Chinese.')),
    "fastpath_chat_json": ("baseline", {}, fastpath_openai("chat", False)),
    "fastpath_chat_stream": ("baseline", {}, fastpath_openai("chat", True)),
    "fastpath_responses_json": ("baseline", {}, fastpath_openai("responses", False)),
    "fastpath_responses_stream": ("baseline", {}, fastpath_openai("responses", True)),
    "fastpath_messages_max1_plain": ("baseline", {}, fastpath_messages_plain),
    "fastpath_messages_probe_json": ("baseline", {}, fastpath_messages_probe(False)),
    "fastpath_messages_probe_stream": ("baseline", {}, fastpath_messages_probe(True)),
    "late_notice_json": ("late_notice", {}, late_notice(False)),
    "late_notice_stream": ("late_notice", {}, late_notice(True)),
    "switch_rejected": ("reject", {"FAKE_ACP_REJECT_MODEL": "gemini-4-carbon"}, switch_rejected),
    "empty_model_ack": ("empty_config", {}, empty_model_ack),
    "inventory_first": ("inventory", {}, inventory_first),
    "inventory_changed": ("inventory", {}, inventory_changed),
    "inventory_removed_call": ("inventory", {}, inventory_removed_call),
    "gate_detects_broken_tools": ("break_tools", {}, gate_detects(lambda p: E.scenario_tool_roundtrip(p, "fake", "gemini-3.8-flash-low"))),
    "gate_detects_broken_model": ("break_model", {}, gate_detects(lambda p: E.scenario_model_switch(p, "fake", None))),
    "gate_detects_broken_tier": ("break_tier", {}, gate_detects(lambda p: E.scenario_model_switch(p, "fake", None))),
}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("scenarios", nargs="*")
    ap.add_argument("--port", type=int, default=18894)
    ap.add_argument("--dir", type=Path, default=Path("/tmp/agy-review-regression"))
    ap.add_argument("--rebuild", action="store_true", help="rebuild gateway and fakeacp from the current source")
    ap.add_argument("--list", action="store_true")
    args = ap.parse_args()
    if args.list:
        for name, (fault, _, _) in SCENARIOS.items():
            print(f"{name:34s} fault={fault}")
        return 0
    names = args.scenarios or list(SCENARIOS)
    unknown = [n for n in names if n not in SCENARIOS]
    if unknown or args.port in E.MAIN_PORTS:
        print("unknown scenarios: %s" % unknown if unknown else "refusing main instance port", file=sys.stderr)
        return 2
    if args.rebuild or not (args.dir/"fakeacp").is_file():
        F.build(args.dir)
    failed = 0
    keyfn = lambda n: (SCENARIOS[n][0], json.dumps(SCENARIOS[n][1], sort_keys=True))  # noqa: E731
    for (fault, env_key), group in itertools.groupby(sorted(names, key=keyfn), key=keyfn):
        extra = {"REVIEW_FAULT": fault, **json.loads(env_key)}
        with F.instance(args.dir, args.port, WRAPPER, extra) as receipt:
            E.verify_fake_receipt(args.port, receipt)
            for name in group:
                try:
                    res = SCENARIOS[name][2](args.port)
                except Exception as exc:  # noqa: BLE001 - a crash is a failure, keep running the rest
                    res = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
                failed += 0 if res.get("ok") is True else 1
                print(("PASS " if res.get("ok") else "FAIL ") + f"{name} [{fault}] " + json.dumps(res, ensure_ascii=False)[:900], flush=True)
    print(f"SUMMARY {len(names) - failed}/{len(names)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    os.umask(0o077)
    raise SystemExit(main())
