#!/usr/bin/env python3
"""Thin stdio JSON-RPC ACP wrapper around compiled fakeacp for isolated resilience tests.

Activated only in isolated ephemeral-port sub-instances (never against production or shared 18892).
Supports CLI passthrough (--version, -v, -p /usage) and prompt-marker fault hooks:
  [WRAPPER_TOOL_CALL]                Fires a Bash MCP tool call on ANY turn (tNum >= 1).
  [WRAPPER_FOLLOW_RELAYS]            Follows ws_await_result relays up to 20 hops.
  [WRAPPER_MEDIUM_429_LOW_OK]        Emits 429 soft error on medium/high/xhigh tier; "FALLBACK_LOW_OK" on low.
  [WRAPPER_TRANSIENT_32603_ONCE(:k)] Returns JSON-RPC -32603 503 on 1st call for key k; "RETRY_32603_OK" on retry.
  [WRAPPER_SLOW_CANCEL_ONCE(:k)]     On 1st call for key k, waits for session/cancel then sleeps 0.25s before emitting cancelled.
"""
import http.client
import json
import os
import re
import subprocess
import sys
import threading
import time
from urllib.parse import urlparse

MCP_TOOL_NAME_RE = re.compile(r"^(?:ws|dsh)_(.+)_[0-9a-f]{6,}$")
CALL_ID_RE = re.compile(r'"call_id":"([^"]+)"')
TRANSIENT_32603_RE = re.compile(r"\[WRAPPER_TRANSIENT_32603_ONCE(?::([A-Za-z0-9_.-]+))?\]")
SLOW_CANCEL_RE = re.compile(r"\[WRAPPER_SLOW_CANCEL_ONCE(?::([A-Za-z0-9_.-]+))?\]")


def _extract_prompt_text(params):
    parts = []
    for blk in (params or {}).get("prompt") or []:
        if not isinstance(blk, dict):
            continue
        if isinstance(blk.get("text"), str):
            parts.append(blk["text"])
        res = blk.get("resource")
        if isinstance(res, dict) and isinstance(res.get("text"), str):
            parts.append(res["text"])
    return "\n".join(parts)


def mcp_http_rpc(url, auth, method, params, timeout=3600):
    """Perform a single JSON-RPC POST over HTTP/SSE to the gateway MCP bridge with clean socket teardown."""
    parsed = urlparse(url)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    path = parsed.path or "/"
    if parsed.query:
        path = f"{path}?{parsed.query}"
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}}).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }
    if auth:
        headers["Authorization"] = auth

    conn = http.client.HTTPConnection(host, port, timeout=timeout)
    try:
        conn.request("POST", path, body=payload, headers=headers)
        resp = conn.getresponse()
        try:
            raw = resp.read().decode("utf-8", "replace")
        finally:
            resp.close()
    finally:
        conn.close()

    for line in reversed(raw.splitlines()):
        s = line.strip()
        if s.startswith("data:"):
            s = s[5:].strip()
        if s.startswith("{"):
            try:
                return json.loads(s).get("result") or {}
            except ValueError:
                return {}
    return {}


class WrapperState:
    """Pure, thread-safe JSON-RPC state machine for fault injection and tier tracking."""

    def __init__(self, mcp_caller=None):
        self._lock = threading.Lock()
        self.pending = {}
        self.sessions = {}
        self.seen_32603 = set()
        self.seen_slow_cancel = set()
        self.mcp_caller = mcp_caller or mcp_http_rpc

    def record_gateway_request(self, msg):
        method = msg.get("method")
        params = msg.get("params") or {}
        rid = msg.get("id")
        if method and rid is not None:
            with self._lock:
                self.pending[str(rid)] = (method, params)

        if method == "session/set_config_option":
            sid = params.get("sessionId")
            cid = params.get("configId")
            val = params.get("value")
            if sid:
                with self._lock:
                    sess = self.sessions.setdefault(sid, self._new_session_dict())
                    if cid == "thought_level" and isinstance(val, str) and val:
                        sess["tier"] = val
                    elif cid == "model" and isinstance(val, str) and val:
                        sess["model"] = val
            return None

        if method in ("session/cancel", "session/close"):
            sid = params.get("sessionId")
            consumed = False
            if sid:
                with self._lock:
                    sess = self.sessions.get(sid)
                    if sess:
                        sess["cancel_ev"].set()
                        sess["cancel_count"] += 1
                        if method == "session/cancel" and sess["intercept_active"] > 0:
                            consumed = True
            return consumed

        return False

    @staticmethod
    def _new_session_dict():
        return {
            "model": "gemini-4-carbon",
            "tier": "medium",
            "url": "",
            "auth": "",
            "mcp_tools": {},
            "turns": 0,
            "cancel_ev": threading.Event(),
            "cancel_count": 0,
            "intercept_active": 0,
        }

    def begin_intercept(self, msg):
        sid = ((msg.get("params") or {}).get("sessionId")) or ""
        if not sid:
            return
        with self._lock:
            sess = self.sessions.setdefault(sid, self._new_session_dict())
            sess["intercept_active"] += 1

    def end_intercept(self, msg):
        sid = ((msg.get("params") or {}).get("sessionId")) or ""
        if not sid:
            return
        with self._lock:
            sess = self.sessions.get(sid)
            if sess and sess["intercept_active"] > 0:
                sess["intercept_active"] -= 1

    def handle_child_response(self, msg):
        if "method" in msg or msg.get("id") is None:
            return
        with self._lock:
            req = self.pending.pop(str(msg.get("id")), None)
        if not req:
            return
        method, params = req
        if method not in ("session/new", "session/resume", "session/load") or "result" not in msg:
            return
        res = msg.get("result") or {}
        sid = res.get("sessionId") or params.get("sessionId")
        if not sid:
            return
        explicit_mcp = isinstance(params.get("mcpServers"), list)
        url, auth = "", ""
        for srv in params.get("mcpServers") or []:
            if not isinstance(srv, dict):
                continue
            url = srv.get("url", "") or url
            for h in srv.get("headers") or []:
                if isinstance(h, dict) and h.get("name") == "Authorization":
                    auth = h.get("value", "") or auth
        with self._lock:
            sess = self.sessions.setdefault(sid, self._new_session_dict())
            sess["model"] = "gemini-4-carbon"
            sess["tier"] = "medium"
            if explicit_mcp:
                sess["url"] = url
                sess["auth"] = auth
                sess["mcp_tools"] = {}
            else:
                if url:
                    sess["url"] = url
                if auth:
                    sess["auth"] = auth
            sess["cancel_ev"] = threading.Event()

    def should_intercept_prompt(self, msg):
        if msg.get("method") != "session/prompt":
            return False
        text = _extract_prompt_text(msg.get("params") or {})
        if (
            "[WRAPPER_TOOL_CALL]" in text
            or "[WRAPPER_MEDIUM_429_LOW_OK]" in text
            or "[WRAPPER_TRANSIENT_32603_ONCE" in text
        ):
            return True
        if "[WRAPPER_SLOW_CANCEL_ONCE" in text:
            m = SLOW_CANCEL_RE.search(text)
            key = (m.group(1) if m and m.group(1) else "default")
            with self._lock:
                return key not in self.seen_slow_cancel
        return False

    def _ensure_mcp_tools(self, sid):
        with self._lock:
            sess = self.sessions.get(sid)
            if not sess or not sess["url"]:
                return {}, "", ""
            if sess["mcp_tools"]:
                return dict(sess["mcp_tools"]), sess["url"], sess["auth"]
            url, auth = sess["url"], sess["auth"]

        self.mcp_caller(url, auth, "initialize", {"protocolVersion": "2025-11-25"})
        listed = self.mcp_caller(url, auth, "tools/list", {})
        tools_map = {}
        for t in (listed or {}).get("tools") or []:
            name = t.get("name", "")
            m = MCP_TOOL_NAME_RE.match(name)
            if m:
                tools_map[m.group(1)] = name
        with self._lock:
            sess = self.sessions.get(sid)
            if sess:
                sess["mcp_tools"] = dict(tools_map)
        return tools_map, url, auth

    def execute_intercepted_prompt(self, msg, emit_fn):
        rid = msg.get("id")
        params = msg.get("params") or {}
        sid = params.get("sessionId") or ""
        text = _extract_prompt_text(params)

        with self._lock:
            sess = self.sessions.setdefault(sid, self._new_session_dict())
            sess["turns"] += 1
            before_cancels = sess["cancel_count"]
            sess["cancel_ev"].clear()
            cur_tier = sess["tier"]
            cancel_ev = sess["cancel_ev"]

        if "[WRAPPER_SLOW_CANCEL_ONCE" in text:
            m = SLOW_CANCEL_RE.search(text)
            key = (m.group(1) if m and m.group(1) else "default")
            first_hit = False
            with self._lock:
                if key not in self.seen_slow_cancel:
                    self.seen_slow_cancel.add(key)
                    first_hit = True
            if first_hit:
                deadline = time.monotonic() + 2.0
                while time.monotonic() < deadline:
                    with self._lock:
                        if sess["cancel_count"] > before_cancels:
                            break
                    cancel_ev.wait(timeout=0.05)
                time.sleep(0.25)
                emit_fn({"jsonrpc": "2.0", "id": rid, "result": {"stopReason": "cancelled"}})
                return
            emit_fn({
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": sid,
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "content": {"type": "text", "text": "SLOW_CANCEL_RECOVERY_OK"},
                    },
                },
            })
            emit_fn({
                "jsonrpc": "2.0",
                "id": rid,
                "result": {
                    "stopReason": "end_turn",
                    "usage": {"inputTokens": 120, "outputTokens": 10, "cachedReadTokens": 2048},
                },
            })
            return

        if "[WRAPPER_TRANSIENT_32603_ONCE" in text:
            m = TRANSIENT_32603_RE.search(text)
            key = (m.group(1) if m and m.group(1) else "default")
            first_hit = False
            with self._lock:
                if key not in self.seen_32603:
                    self.seen_32603.add(key)
                    first_hit = True
            if first_hit:
                emit_fn({
                    "jsonrpc": "2.0",
                    "id": rid,
                    "error": {
                        "code": -32603,
                        "message": "503 MODEL_CAPACITY_EXHAUSTED: transient upstream rpc failure",
                    },
                })
                return
            emit_fn({
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": sid,
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "content": {"type": "text", "text": "RETRY_32603_OK"},
                    },
                },
            })
            emit_fn({
                "jsonrpc": "2.0",
                "id": rid,
                "result": {
                    "stopReason": "end_turn",
                    "usage": {"inputTokens": 120, "outputTokens": 10, "cachedReadTokens": 2048},
                },
            })
            return

        if "[WRAPPER_MEDIUM_429_LOW_OK]" in text:
            if cur_tier in ("medium", "high", "xhigh"):
                chunks = [
                    "\n⚠ API error (attempt 1): RESOURCE_EXHAUSTED (code 429): Resource has been exhausted (e.g. check quota).\n",
                    "\n⚠ RESOURCE_EXHAUSTED (code 429): Resource has been exhausted (e.g. check quota).\n",
                ]
            else:
                chunks = ["FALLBACK_LOW_OK"]
            for c in chunks:
                emit_fn({
                    "jsonrpc": "2.0",
                    "method": "session/update",
                    "params": {
                        "sessionId": sid,
                        "update": {
                            "sessionUpdate": "agent_message_chunk",
                            "content": {"type": "text", "text": c},
                        },
                    },
                })
            emit_fn({
                "jsonrpc": "2.0",
                "id": rid,
                "result": {
                    "stopReason": "end_turn",
                    "usage": {"inputTokens": 120, "outputTokens": 12, "cachedReadTokens": 4096},
                },
            })
            return

        if "[WRAPPER_TOOL_CALL]" in text:
            tools_map, url, auth = self._ensure_mcp_tools(sid)
            bash_name = tools_map.get("Bash")
            await_name = tools_map.get("ws_await_result") or tools_map.get("dsh_await_result")
            if not bash_name or not url:
                emit_fn({
                    "jsonrpc": "2.0",
                    "id": rid,
                    "error": {"code": -32000, "message": "wrapper missing MCP Bash tool"},
                })
                return
            res = self.mcp_caller(
                url,
                auth,
                "tools/call",
                {"name": bash_name, "arguments": {"command": "echo WRAPPER_TOOL_OK"}},
            )
            tool_out, is_err = self._extract_mcp_text(res)
            if "[WRAPPER_FOLLOW_RELAYS]" in text and await_name:
                for _ in range(20):
                    if is_err:
                        break
                    m = CALL_ID_RE.search(tool_out)
                    if not m:
                        break
                    if cancel_ev.is_set():
                        emit_fn({"jsonrpc": "2.0", "id": rid, "result": {"stopReason": "cancelled"}})
                        return
                    res = self.mcp_caller(
                        url,
                        auth,
                        "tools/call",
                        {"name": await_name, "arguments": {"call_id": m.group(1)}},
                    )
                    tool_out, is_err = self._extract_mcp_text(res)
                if is_err and "hibernated" in tool_out:
                    emit_fn({"jsonrpc": "2.0", "id": rid, "result": {"stopReason": "cancelled"}})
                    return
            if is_err and "cancelled" in tool_out.lower():
                emit_fn({"jsonrpc": "2.0", "id": rid, "result": {"stopReason": "cancelled"}})
                return
            emit_fn({
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": sid,
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "content": {"type": "text", "text": "WRAPPER_TOOL_DONE:" + tool_out},
                    },
                },
            })
            emit_fn({
                "jsonrpc": "2.0",
                "id": rid,
                "result": {
                    "stopReason": "end_turn",
                    "usage": {"inputTokens": 320, "outputTokens": 24, "cachedReadTokens": 4096},
                },
            })
            return

    @staticmethod
    def _extract_mcp_text(res):
        if not isinstance(res, dict):
            return "", False
        is_err = bool(res.get("isError"))
        content = res.get("content") or []
        if content and isinstance(content[0], dict):
            return str(content[0].get("text") or ""), is_err
        return "", is_err


def _default_child_bin():
    return os.environ.get("REVIEW_FAKEACP_BIN") or os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "fakeacp"
    )


def main():
    child_bin = _default_child_bin()
    args = sys.argv[1:]
    if any(a in ("--version", "-v", "-p", "--prompt") for a in args):
        proc = subprocess.run([child_bin, *args], check=False)
        return proc.returncode

    out_lock = threading.Lock()
    state = WrapperState()
    child = subprocess.Popen(
        [child_bin, *args],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
        bufsize=1,
    )

    def emit(msg):
        with out_lock:
            sys.stdout.write(json.dumps(msg) + "\n")
            sys.stdout.flush()

    def to_child(line):
        child.stdin.write(line if line.endswith("\n") else line + "\n")
        child.stdin.flush()

    def run_intercepted(msg):
        try:
            state.execute_intercepted_prompt(msg, emit)
        except Exception as exc:  # noqa: BLE001 - always answer gateway RPC
            emit({
                "jsonrpc": "2.0",
                "id": msg.get("id"),
                "error": {"code": -32000, "message": f"cache_resilience_acp error: {exc!r}"},
            })
        finally:
            state.end_intercept(msg)

    def from_child():
        try:
            for line in child.stdout:
                try:
                    msg = json.loads(line)
                except ValueError:
                    continue
                state.handle_child_response(msg)
                emit(msg)
        finally:
            os._exit(0)

    threading.Thread(target=from_child, daemon=True).start()
    try:
        for line in sys.stdin:
            if not line.strip():
                continue
            try:
                msg = json.loads(line)
            except ValueError:
                continue
            consumed = state.record_gateway_request(msg)
            if consumed:
                if msg.get("id") is not None:
                    emit({"jsonrpc": "2.0", "id": msg.get("id"), "result": {}})
                continue
            if state.should_intercept_prompt(msg):
                state.begin_intercept(msg)
                threading.Thread(target=run_intercepted, args=(msg,), daemon=True).start()
                continue
            to_child(line)
    finally:
        try:
            if child.stdin:
                child.stdin.close()
        except OSError:
            pass
        try:
            if child.stdout:
                child.stdout.close()
        except OSError:
            pass
        child.wait(5)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
