import json, os, sys, time, urllib.request

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 18892
SID = sys.argv[2] if len(sys.argv) > 2 else f"go-e2e-{int(time.time())}"
URL = f"http://127.0.0.1:{PORT}/v1/messages"
TOOLS = [{
    "name": "Bash",
    "description": "Run a bash command",
    "input_schema": {"type": "object", "properties": {"command": {"type": "string"}}, "required": ["command"]},
}]


def post(body, timeout=600):
    req = urllib.request.Request(URL, data=json.dumps(body).encode(), headers={
        "content-type": "application/json", "x-claude-code-session-id": SID, "anthropic-version": "2023-06-01", **({"x-api-key": os.environ["AGY_KEY"]} if os.environ.get("AGY_KEY") else {})})
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=timeout) as r:
        data = json.loads(r.read())
    return data, time.time() - t0


msgs = [{"role": "user", "content": "Use the Bash tool to run exactly: echo GO_GATEWAY_OK . Then reply with the output only."}]
MODEL = sys.argv[3] if len(sys.argv) > 3 else "claude-sonnet-4-5"
base = {"model": MODEL, "max_tokens": 2048, "system": "You are a helpful agent.", "tools": TOOLS}
d1, t1 = post({**base, "messages": msgs})
print("STEP1", round(t1, 1), d1.get("stop_reason"), json.dumps(d1.get("content"))[:400], d1.get("usage"))
tu = [c for c in d1.get("content", []) if c.get("type") == "tool_use"]
if not tu:
    sys.exit(0)
msgs.append({"role": "assistant", "content": d1["content"]})
msgs.append({"role": "user", "content": [{"type": "tool_result", "tool_use_id": tu[0]["id"], "content": "GO_GATEWAY_OK"}]})
d2, t2 = post({**base, "messages": msgs})
print("STEP2", round(t2, 1), d2.get("stop_reason"), json.dumps(d2.get("content"))[:400], d2.get("usage"))
