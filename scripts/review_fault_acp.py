#!/usr/bin/env python3
"""Fault-injecting ACP wrapper around the repository fake ACP (never a real model).

The gateway starts this file as AGY_ACP_BIN. It runs the fake from REVIEW_FAKEACP_BIN and rewrites
traffic according to REVIEW_FAULT:
  baseline       pass-through
  reject         pass-through (combine with FAKE_ACP_REJECT_MODEL=<model> for the child)
  empty_config   session/set_config_option(model) answers {} without applying the model
  late_notice    the normal reply becomes a partial answer followed by an agy network notice chunk
  inventory      like real agy 1.5.5, tools/list is fetched only when a session is opened (new/resume);
                 every prompt is answered "INVENTORY tools=<host names> rules=<REVIEW_RULE_x seen>";
                 a prompt containing [CALL_REMOVED:<Tool>] calls that tool by its first-seen MCP name
  break_tools    strips [FAKE_TOOL_CALL] so no tool is ever called (gate self-test)
  break_model    acknowledges model switches without applying them (gate self-test)
  break_tier     acknowledges thought_level without applying it (gate self-test)
"""
import json
import os
import re
import subprocess
import sys
import threading
import urllib.request

MODE = os.environ.get("REVIEW_FAULT", "baseline")
CHILD = os.environ.get("REVIEW_FAKEACP_BIN") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "fakeacp")
NOTICE = "\n⚠ There was a network issue connecting to the server, please try again.\n"
RULE_RE = re.compile(r"REVIEW_RULE_[A-Z]")
CALL_RE = re.compile(r"\[CALL_REMOVED:(\w+)\]")
REMOVED_TOOLS_RE = re.compile(r"no longer available in this session and calls to them will fail:\s*([^\n.]+)")
ADDED_TOOL_LINE_RE = re.compile(r"^-\s+([^\s:(]+)(?:\s*\([^)]*\))?:\s+", re.MULTILINE)

out_lock = threading.Lock()
pending = {}          # gateway request id -> (method, params)
sessions = {}         # sid -> {"tools": {mcp_name: host}, "sys": str, "url": str, "auth": str}
ever_seen = {}        # host tool name -> first MCP name (agy keeps calling names it already listed)
child = subprocess.Popen([CHILD, *sys.argv[1:]], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, bufsize=1)


def emit(msg):
    with out_lock:
        sys.stdout.write(json.dumps(msg) + "\n")
        sys.stdout.flush()


def to_child(line):
    child.stdin.write(line if line.endswith("\n") else line + "\n")
    child.stdin.flush()


def mcp_call(info, method, params):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    req = urllib.request.Request(info["url"], data=body, headers={"Content-Type": "application/json",
                                                                  "Accept": "application/json, text/event-stream",
                                                                  "Authorization": info["auth"]})
    with urllib.request.urlopen(req, timeout=3600) as r:
        raw = r.read().decode()
    for line in reversed(raw.splitlines()):
        line = line.strip()
        if line.startswith("data:"):
            line = line[5:].strip()
        if line.startswith("{"):
            return json.loads(line).get("result") or {}
    return {}


def open_session(sid, params):
    info = {"tools": {}, "active_tools": set(), "rules": set(), "sys": "", "url": "", "auth": ""}
    meta = ((params.get("_meta") or {}).get("antigravity") or {}).get("systemPrompt") or {}
    info["sys"] = meta.get("append", "")
    info["rules"] = set(RULE_RE.findall(info["sys"]))
    for srv in params.get("mcpServers") or []:
        info["url"] = srv.get("url", "")
        for h in srv.get("headers") or []:
            if h.get("name") == "Authorization":
                info["auth"] = h.get("value", "")
    if info["url"]:
        mcp_call(info, "initialize", {"protocolVersion": "2025-11-25"})
        for t in mcp_call(info, "tools/list", {}).get("tools", []):
            name = t.get("name", "")
            m = re.match(r"^(?:ws|dsh)_(.+)_[0-9a-f]{6,}$", name)
            if not m or m.group(1).startswith(("ws_", "dsh_")):
                continue  # gateway-internal helpers (ws_await_result / ws_call_tool / legacy dsh_*) are not host tools
            host = m.group(1)
            info["tools"][name] = host
            info["active_tools"].add(host)
            ever_seen.setdefault(host, name)
    sessions[sid] = info


def answer_prompt(rid, sid, text):
    try:
        _answer_prompt(rid, sid, text)
    except Exception as exc:  # noqa: BLE001 - reply instead of leaving the gateway waiting
        emit({"jsonrpc": "2.0", "id": rid, "error": {"code": -32000, "message": "review wrapper: %r" % (exc,)}})


def _apply_prompt_deltas(info, text):
    if "[Host tool update]" in text:
        for rm in REMOVED_TOOLS_RE.findall(text):
            for tname in rm.split(","):
                info["active_tools"].discard(tname.strip())
        for sec in re.split(r"(?=Newly available tools|Re-enabled tools|Updated tool definitions)", text):
            if sec.startswith(("Newly available tools", "Re-enabled tools")):
                for m in ADDED_TOOL_LINE_RE.finditer(sec):
                    info["active_tools"].add(m.group(1).strip())
    info["rules"].update(RULE_RE.findall(text))


def _answer_prompt(rid, sid, text):
    info = sessions.get(sid) or {"tools": {}, "active_tools": set(), "rules": set(), "sys": ""}
    _apply_prompt_deltas(info, text)
    reply = "INVENTORY tools=%s rules=%s" % (",".join(sorted(info["active_tools"])),
                                              ",".join(sorted(info["rules"])))
    m = CALL_RE.search(text)
    if m:
        name = ever_seen.get(m.group(1), "ws_%s_000000000000" % m.group(1))
        res = mcp_call(info, "tools/call", {"name": name, "arguments": {}})
        first = ((res.get("content") or [{}])[0] or {}).get("text", "")
        reply += " call=%s isError=%s result=%s" % (m.group(1), bool(res.get("isError")), first[:120].replace("\n", " "))
    emit({"jsonrpc": "2.0", "method": "session/update", "params": {"sessionId": sid, "update": {
        "sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": reply}}}})
    emit({"jsonrpc": "2.0", "id": rid, "result": {"stopReason": "end_turn",
                                                  "usage": {"inputTokens": 100, "outputTokens": 10, "cachedReadTokens": 0}}})


def from_gateway():
    for line in sys.stdin:
        if not line.strip():
            continue
        try:
            msg = json.loads(line)
        except ValueError:
            continue
        method, params, rid = msg.get("method"), msg.get("params") or {}, msg.get("id")
        if method and rid is not None:
            pending[str(rid)] = (method, params)
        if method == "session/set_config_option":
            cid, val = params.get("configId"), params.get("value")
            if MODE == "empty_config" and cid == "model":
                emit({"jsonrpc": "2.0", "id": rid, "result": {}})
                continue
            if (MODE == "break_model" and cid == "model") or (MODE == "break_tier" and cid == "thought_level"):
                emit({"jsonrpc": "2.0", "id": rid, "result": {"configOptions": [{"id": cid, "currentValue": val}]}})
                continue
        if method == "session/prompt":
            text = "\n".join(b.get("text", "") for b in params.get("prompt") or [] if isinstance(b, dict))
            text += "\n".join((b.get("resource") or {}).get("text", "") for b in params.get("prompt") or [] if isinstance(b, dict))
            if MODE == "inventory":
                threading.Thread(target=answer_prompt, args=(rid, params.get("sessionId"), text), daemon=True).start()
                continue
            if MODE == "break_tools" and "[FAKE_TOOL_CALL]" in line:
                line = line.replace("[FAKE_TOOL_CALL]", "")
        to_child(line)


def from_child():
    for line in child.stdout:
        try:
            msg = json.loads(line)
        except ValueError:
            continue
        req = pending.pop(str(msg.get("id")), None) if "method" not in msg else None
        if req and req[0] in ("session/new", "session/resume", "session/load") and "result" in msg and MODE == "inventory":
            sid = (msg["result"] or {}).get("sessionId") or req[1].get("sessionId")
            try:
                open_session(sid, req[1])
            except Exception as exc:  # noqa: BLE001 - keep the ACP stream alive; the scenario will fail visibly
                sys.stderr.write("inventory listing failed: %r\n" % (exc,))
        if MODE == "late_notice" and msg.get("method") == "session/update":
            upd = (msg.get("params") or {}).get("update") or {}
            content = upd.get("content") or {}
            if upd.get("sessionUpdate") == "agent_message_chunk" and content.get("text", "").startswith("FAKE_OK"):
                content["text"] = "Partial answer before an upstream failure.\n"
                emit(msg)
                content["text"] = NOTICE
        emit(msg)
    os._exit(0)


threading.Thread(target=from_child, daemon=True).start()
try:
    from_gateway()
finally:
    try:
        child.stdin.close()
    except OSError:
        pass
    child.wait(5)
