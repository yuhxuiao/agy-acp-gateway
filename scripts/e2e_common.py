"""Shared gateway E2E scenarios with explicit fake/live modes.

Fake mode (default) only talks to an instance proven by a receipt to run the fake ACP binary
(see scripts/fake_instance.py and agy_upgrade.gateway_instance). Live mode is limited to one
model, gemini-3.8-flash-low, must be requested explicitly, and refuses the main 18890 instance
unless --allow-main-instance is given. Every scenario returns {"ok": bool, ...}; callers exit
non-zero when any scenario fails.
"""
import ctypes
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import time
import urllib.error
import urllib.request

LIVE_MODEL = "gemini-3.8-flash-low"
MAIN_PORTS = {18889, 18890, 18891}
FAKE_OK_RE = re.compile(r"FAKE_OK model=(\S+) tier=(\S+) turn=(\d+)")
SYS = "Primary working directory: /tmp"
BASH_TOOL = {"name": "Bash", "description": "Run a bash command",
             "input_schema": {"type": "object", "properties": {"command": {"type": "string"}}, "required": ["command"]}}


class GateError(ValueError):
    pass


def add_mode_args(ap):
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--fake", action="store_true", help="fake ACP instance (default); requires --fake-receipt")
    mode.add_argument("--live", action="store_true", help="real upstream; only with --model %s" % LIVE_MODEL)
    ap.add_argument("--fake-receipt", type=Path, default=os.environ.get("AGY_E2E_FAKE_RECEIPT") or None,
                    help="instance receipt JSON (env AGY_E2E_FAKE_RECEIPT)")
    ap.add_argument("--model", help="live mode: must be %s; fake mode: optional override" % LIVE_MODEL)
    ap.add_argument("--allow-main-instance", action="store_true", help="live mode: allow ports 18889-18891")


def sha256(path):
    with Path(path).open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


def pid_executable(pid):
    pid = int(pid)
    if Path("/proc").is_dir():
        return Path(f"/proc/{pid}/exe").resolve(strict=True)
    if sys.platform == "darwin":
        libproc = ctypes.CDLL("/usr/lib/libproc.dylib")
        buf = ctypes.create_string_buffer(4096)
        ret = libproc.proc_pidpath(ctypes.c_int(pid), buf, ctypes.c_uint32(len(buf)))
        if ret <= 0:
            raise FileNotFoundError(f"executable path unavailable for pid {pid}")
        return Path(os.fsdecode(buf.value)).resolve(strict=True)
    raise FileNotFoundError(f"unsupported OS for pid executable lookup: {sys.platform}")


def verify_fake_receipt(port, receipt_path):
    """Raise unless the receipt proves a fake-ACP gateway owns this port."""
    if port in MAIN_PORTS:
        raise GateError("fake mode never targets the main instance ports 18889-18891")
    if not receipt_path:
        raise GateError("fake mode requires an owned instance receipt (--fake-receipt or AGY_E2E_FAKE_RECEIPT)")
    receipt = json.loads(Path(receipt_path).read_text())
    if receipt.get("mode") != "fake" or receipt.get("port") != port:
        raise GateError("fake instance receipt mismatch")
    exe = pid_executable(receipt["pid"])
    if sha256(exe) != receipt.get("gateway_sha256"):
        raise GateError("fake gateway ownership changed")
    acp_path = receipt.get("acp_path")
    if acp_path and sha256(acp_path) != receipt.get("acp_sha256"):
        raise GateError("fake ACP binary changed since the instance started")
    return receipt


def resolve_mode(args, port):
    """Return ("fake", receipt) or ("live", None); raise GateError on unsafe combinations."""
    if args.live:
        if args.model != LIVE_MODEL:
            raise GateError("live mode requires explicit --model %s (single Flash model only)" % LIVE_MODEL)
        if port in MAIN_PORTS and not args.allow_main_instance:
            raise GateError("live mode refuses the main instance; use an isolated 18892 instance")
        return "live", None
    return "fake", verify_fake_receipt(port, args.fake_receipt)


def auth_headers():
    headers = {"Content-Type": "application/json", "anthropic-version": "2023-06-01"}
    key = os.environ.get("AGY_KEY") or os.environ.get("AGY_API_KEYS", "").split(",")[0].strip()
    if key:
        headers["x-api-key"] = key
    return headers


def post(port, path, body, sid=None, extra_headers=None, timeout=180):
    """POST and return (status, content_type, parsed JSON or raw text)."""
    headers = auth_headers()
    if sid:
        headers["x-claude-code-session-id"] = sid
    headers.update(extra_headers or {})
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=json.dumps(body).encode(), headers=headers)
    try:
        resp = urllib.request.urlopen(req, timeout=timeout)
    except urllib.error.HTTPError as err:
        resp = err
    with resp:
        raw = resp.read().decode("utf-8", "replace")
        ctype = resp.headers.get("Content-Type", "")
        try:
            data = json.loads(raw)
        except ValueError:
            data = raw
        return resp.status, ctype, data


def sse_events(raw):
    """Parse an SSE body into a list of (event, data) with JSON data when possible."""
    out = []
    for block in raw.split("\n\n"):
        ev, datas = "", []
        for line in block.splitlines():
            if line.startswith("event:"):
                ev = line[6:].strip()
            elif line.startswith("data:"):
                datas.append(line[5:].strip())
        if not datas:
            continue
        data = "\n".join(datas)
        try:
            data = json.loads(data)
        except ValueError:
            pass
        out.append((ev, data))
    return out


def anthropic_text(data):
    if not isinstance(data, dict):
        return ""
    return "".join(c.get("text", "") for c in data.get("content", []) if c.get("type") == "text")


def chat_text(data):
    try:
        return data["choices"][0]["message"].get("content") or ""
    except (KeyError, IndexError, TypeError, AttributeError):
        return ""


def responses_text(data):
    if not isinstance(data, dict):
        return ""
    return "".join(c.get("text", "") for it in data.get("output", []) for c in (it.get("content") or [])
                   if c.get("type") == "output_text")


def gemini_text(data):
    if not isinstance(data, dict):
        return ""
    out = []
    for cand in data.get("candidates") or []:
        for p in ((cand.get("content") or {}).get("parts") or []):
            if not p.get("thought") and isinstance(p.get("text"), str):
                out.append(p["text"])
    return "".join(out)


def fake_identity(text):
    m = FAKE_OK_RE.search(text or "")
    return (m.group(1), m.group(2), int(m.group(3))) if m else None


def model_tier(name):
    """Expected (model id, tier) for a requested model name, mirroring go-gateway ResolveRequestedModel."""
    m = re.match(r"^(.*?)-(low|medium|high|xhigh)$", name)
    return (m.group(1), m.group(2)) if m else (name, "medium")


def check(results, name, ok, **detail):
    results.append(dict(scenario=name, ok=bool(ok), **detail))
    return ok


# ---------------------------------------------------------------- scenarios

def scenario_tool_roundtrip(port, mode, model):
    """Tool call must surface as tool_use, and the tool result must reach the final end_turn answer."""
    sid = f"e2e-tool-{time.time_ns()}"
    if mode == "fake":
        prompt, command, result_text, want = "[FAKE_TOOL_CALL] Use the Bash tool to run exactly: echo FAKE_TOOL_OK", \
            "echo FAKE_TOOL_OK", "FAKE_TOOL_OK", "TOOL_DONE:FAKE_TOOL_OK"
    else:
        prompt, command, result_text, want = "Use the Bash tool to run exactly: echo GO_GATEWAY_OK . Then reply with the output only.", \
            None, "GO_GATEWAY_OK", "GO_GATEWAY_OK"
    base = {"model": model, "max_tokens": 2048, "system": SYS, "tools": [BASH_TOOL]}
    msgs = [{"role": "user", "content": prompt}]
    s1, _, d1 = post(port, "/v1/messages", {**base, "messages": msgs}, sid, timeout=600)
    tus = [c for c in (d1.get("content", []) if isinstance(d1, dict) else []) if c.get("type") == "tool_use"]
    step1 = {"status": s1, "stop": d1.get("stop_reason") if isinstance(d1, dict) else None,
             "tool_uses": [t.get("name") for t in tus]}
    ok1 = s1 == 200 and step1["stop"] == "tool_use" and len(tus) >= 1 and tus[0].get("name") == "Bash"
    if ok1 and command is not None:
        ok1 = (tus[0].get("input") or {}).get("command") == command
    if not ok1:
        return {"ok": False, "step1": step1, "reason": "no Bash tool_use in step 1", "generation_requests": 1 if mode == "live" else 0}
    msgs += [{"role": "assistant", "content": d1["content"]},
             {"role": "user", "content": [{"type": "tool_result", "tool_use_id": tus[0]["id"], "content": result_text}]}]
    s2, _, d2 = post(port, "/v1/messages", {**base, "messages": msgs}, sid, timeout=600)
    text = anthropic_text(d2)
    step2 = {"status": s2, "stop": d2.get("stop_reason") if isinstance(d2, dict) else None, "text": text[:200]}
    ok = s2 == 200 and step2["stop"] == "end_turn" and want in text
    return {"ok": ok, "step1": step1, "step2": step2, "generation_requests": 2 if mode == "live" else 0,
            **({} if ok else {"reason": "tool result missing from final end_turn"})}


def _anthropic_turn(port, sid, model, msgs, tools=True):
    body = {"model": model, "max_tokens": 256, "system": SYS, "messages": msgs}
    if tools:
        body["tools"] = [BASH_TOOL]
    return post(port, "/v1/messages", body, sid)


def scenario_model_switch(port, mode, model):
    """Fake: one conversation switches Flash -> Carbon -> Argon(high) -> Sonnet and must keep its session.
    Live: a single Flash turn (no switching under the single-model authorization)."""
    sid = f"e2e-switch-{time.time_ns()}"
    plan = ["gemini-3.8-flash-low", "gemini-4-carbon", "gemini-4-neon-xhigh", "gemini-4-hydrogen-xhigh", "gemini-4-argon-high", "claude-sonnet-5-5"] if mode == "fake" else [model]
    msgs, turns, ok = [], [], True
    for i, name in enumerate(plan, 1):
        msgs.append({"role": "user", "content": f"Turn {i}: reply only OK{i}."})
        status, _, data = _anthropic_turn(port, sid, name, msgs)
        text = anthropic_text(data)
        want_model, want_tier = model_tier(name)
        row = {"requested": name, "status": status, "model": data.get("model") if isinstance(data, dict) else None,
               "stop": data.get("stop_reason") if isinstance(data, dict) else None, "text": text[:120]}
        good = status == 200 and row["stop"] == "end_turn" and row["model"] == want_model
        if mode == "fake":
            ident = fake_identity(text)
            row["executed"] = ident
            good = good and ident == (want_model, want_tier, i)
        else:
            good = good and bool(text.strip())
        row["ok"] = good
        turns.append(row)
        ok = ok and good
        if not good:
            break
        msgs.append({"role": "assistant", "content": data["content"]})
    return {"ok": ok, "turns": turns, "generation_requests": len(turns) if mode == "live" else 0}


def scenario_protocols(port, mode, model):
    """OpenAI chat/Responses and Gemini v1beta, non-stream and stream, must report and execute the requested model."""
    fake = mode == "fake"
    cases = [("chat", False, "gemini-4-carbon"), ("responses", False, "gemini-3.8-flash"), ("gemini", False, "gemini-4-carbon"),
             ("chat", True, "gemini-4-carbon"), ("responses", True, "gemini-3.8-flash-low"), ("gemini", True, "gemini-4-hydrogen-xhigh")] if fake else \
            [("chat", False, model), ("responses", False, model)]
    rows, ok = [], True
    for kind, stream, name in cases:
        sid = f"e2e-proto-{kind}-{time.time_ns()}"
        q = f"Reply with exactly: PONG {sid}"
        if kind == "chat":
            body = {"model": name, "stream": stream, "messages": [{"role": "system", "content": SYS}, {"role": "user", "content": q}]}
            path = "/v1/chat/completions"
        elif kind == "responses":
            body = {"model": name, "stream": stream, "instructions": SYS, "input": q}
            path = "/v1/responses"
        else:
            body = {"systemInstruction": {"parts": [{"text": SYS}]}, "contents": [{"role": "user", "parts": [{"text": q}]}]}
            path = f"/v1beta/models/{name}:streamGenerateContent?alt=sse" if stream else f"/v1beta/models/{name}:generateContent"
        status, ctype, data = post(port, path, body, sid)
        want_model, want_tier = model_tier(name)
        got_model, text, finished = None, "", False
        if stream:
            if not ctype.startswith("text/event-stream") or not isinstance(data, str):
                rows.append({"case": f"{kind}-stream", "ok": False, "status": status, "content_type": ctype})
                ok = False
                continue
            for ev, d in sse_events(data):
                if not isinstance(d, dict):
                    finished = finished or (d == "[DONE]" and kind == "chat")
                    continue
                if kind == "chat":
                    got_model = d.get("model", got_model)
                    for ch in d.get("choices", []):
                        text += (ch.get("delta") or {}).get("content") or ""
                elif kind == "responses" and d.get("type") == "response.completed":
                    got_model = d["response"].get("model")
                    text = responses_text(d["response"])
                    finished = True
                elif kind == "gemini":
                    got_model = d.get("modelVersion", got_model)
                    text += gemini_text(d)
                    for cand in d.get("candidates") or []:
                        if cand.get("finishReason") == "STOP":
                            finished = True
        else:
            if kind == "gemini":
                got_model = data.get("modelVersion") if isinstance(data, dict) else None
                text = gemini_text(data)
                finished = isinstance(data, dict) and bool(data.get("candidates"))
            else:
                got_model = data.get("model") if isinstance(data, dict) else None
                text = chat_text(data) if kind == "chat" else responses_text(data)
                finished = isinstance(data, dict) and data.get("object") == ("chat.completion" if kind == "chat" else "response")
        good = status == 200 and finished and got_model == want_model
        executed = None
        if fake:
            executed = fake_identity(text)
            good = good and executed is not None and executed[:2] == (want_model, want_tier)
        else:
            good = good and bool(text.strip())
        rows.append({"case": f"{kind}-{'stream' if stream else 'json'}", "requested": name, "status": status,
                     "model": got_model, "executed": executed, "text": text[:120], "ok": good})
        ok = ok and good
    return {"ok": ok, "cases": rows, "generation_requests": len(rows) if not fake else 0}


TIER_CASES = [
    ("anthropic budget 32000", "/v1/messages", {"model": "claude-sonnet-5-5", "max_tokens": 40000, "thinking": {"type": "enabled", "budget_tokens": 32000}}, "high"),
    ("anthropic budget 2048", "/v1/messages", {"model": "claude-sonnet-5-5", "max_tokens": 4000, "thinking": {"type": "enabled", "budget_tokens": 2048}}, "low"),
    ("anthropic no thinking", "/v1/messages", {"model": "claude-sonnet-5-5", "max_tokens": 1000}, "medium"),
    ("openai reasoning_effort=xhigh", "/v1/chat/completions", {"model": "gemini-3.8-flash", "reasoning_effort": "xhigh"}, "high"),
    ("hydrogen reasoning_effort=xhigh", "/v1/chat/completions", {"model": "gemini-4-hydrogen", "reasoning_effort": "xhigh"}, "xhigh"),
    ("neon reasoning_effort=xhigh", "/v1/chat/completions", {"model": "gemini-4-neon", "reasoning_effort": "xhigh"}, "xhigh"),
    ("hydrogen output_config.effort=max", "/v1/messages", {"model": "gemini-4-hydrogen", "max_tokens": 1000, "output_config": {"effort": "max"}}, "xhigh"),
    ("neon output_config.effort=max", "/v1/messages", {"model": "gemini-4-neon", "max_tokens": 1000, "output_config": {"effort": "max"}}, "xhigh"),
    ("responses effort=minimal", "/v1/responses", {"model": "gemini-3.8-flash", "reasoning": {"effort": "minimal"}}, "low"),
    ("suffix -high, effort=low wins", "/v1/chat/completions", {"model": "gemini-3.8-flash-high", "reasoning_effort": "low"}, "low"),
    ("gemini v1beta thinkingLevel=XHIGH", "/v1beta/models/gemini-4-hydrogen:generateContent",
     {"generationConfig": {"thinkingConfig": {"thinkingLevel": "THINKING_LEVEL_XHIGH"}}}, "xhigh"),
    ("gemini v1beta thinkingBudget=0", "/v1beta/models/gemini-3.8-flash-high:generateContent",
     {"generationConfig": {"thinkingConfig": {"thinkingBudget": 0}}}, "low"),
]
LIVE_TIER_CASES = [("live flash low", "/v1/responses", {"model": LIVE_MODEL}, "low")]


def session_new_tier(log, sid, wait=3.0):
    """thoughtLevel of the SESSION_NEW line whose convKey belongs to this request's session id."""
    needle = f'"convKey":"sid:{sid}:'
    deadline = time.time() + wait
    while True:
        try:
            lines = Path(log).read_text(encoding="utf-8", errors="ignore").splitlines()[-2000:]
        except OSError:
            lines = []
        for line in reversed(lines):
            if "[SESSION_NEW]" in line and needle in line:
                m = re.search(r'"thoughtLevel":"(\w+)"', line)
                if m:
                    return m.group(1)
        if time.time() >= deadline:
            return None
        time.sleep(0.2)


def scenario_thinking_tiers(port, mode, log):
    if not log or not Path(log).is_file():
        return {"ok": False, "reason": "gateway log not found: %s" % log}
    rows, ok = [], True
    for name, path, extra, want in (TIER_CASES if mode == "fake" else LIVE_TIER_CASES):
        sid = f"tier-{time.time_ns()}"
        q = f"Reply with exactly: OK {sid}"
        body = dict(extra)
        if path == "/v1/messages":
            body.update({"system": SYS, "tools": [BASH_TOOL], "messages": [{"role": "user", "content": q}]})
        elif path == "/v1/chat/completions":
            body.update({"messages": [{"role": "system", "content": SYS}, {"role": "user", "content": q}]})
        elif path.startswith("/v1beta/"):
            body.update({"systemInstruction": {"parts": [{"text": SYS}]}, "contents": [{"role": "user", "parts": [{"text": q}]}]})
        else:
            body.update({"instructions": SYS, "input": q})
        status, _, data = post(port, path, body, sid)
        text = (anthropic_text(data) if path == "/v1/messages"
                else chat_text(data) if path.endswith("completions")
                else gemini_text(data) if path.startswith("/v1beta/")
                else responses_text(data))
        logged = session_new_tier(log, sid)
        executed = fake_identity(text) if mode == "fake" else None
        good = status == 200 and logged == want and (mode != "fake" or (executed is not None and executed[1] == want))
        rows.append({"case": name, "want": want, "logged": logged, "executed": executed[1] if executed else None,
                     "status": status, "ok": good})
        ok = ok and good
    return {"ok": ok, "cases": rows, "generation_requests": len(rows) if mode == "live" else 0}


def run_main(scenarios, args, port):
    """Resolve mode, run (name, fn) scenarios, print one PASS/FAIL line each, return exit code."""
    try:
        mode, receipt = resolve_mode(args, port)
    except (OSError, ValueError, KeyError) as exc:
        print(json.dumps({"ok": False, "error": f"{type(exc).__name__}: {exc}"}))
        return 2
    failed = 0
    for name, fn in scenarios:
        try:
            res = fn(mode, receipt)
        except Exception as exc:  # noqa: BLE001 - any crash is a gate failure
            res = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
        print(("PASS " if res.get("ok") else "FAIL ") + name + " " + json.dumps(res, ensure_ascii=False)[:1500], flush=True)
        failed += 0 if res.get("ok") is True else 1
    return 1 if failed else 0
