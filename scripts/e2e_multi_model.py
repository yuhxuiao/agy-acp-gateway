"""Multi-model checks: mid-conversation model switch, OpenAI chat (gpt-oss), Responses (flash)."""
import json, os, sys, time, urllib.request

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 18890
BASE = f"http://127.0.0.1:{PORT}"


def post(path, body, headers=None, timeout=300):
    h = {"content-type": "application/json", "anthropic-version": "2023-06-01"}
    h.update(headers or {})
    if os.environ.get("AGY_KEY"):
        h["Authorization"] = "Bearer " + os.environ["AGY_KEY"]
    req = urllib.request.Request(BASE + path, data=json.dumps(body).encode(), headers=h)
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read()), round(time.time() - t0, 1)


sid = f"switch-{int(time.time())}"
hdr = {"x-claude-code-session-id": sid}
sysmsg = "Primary working directory: /tmp"
tools = [{"name": "Bash", "description": "Run bash", "input_schema": {"type": "object", "properties": {"command": {"type": "string"}}}}]
msgs = [{"role": "user", "content": "Remember the code word BLUE-42. Reply only: OK"}]
d1, t1 = post("/v1/messages", {"model": "claude-sonnet-5-5", "max_tokens": 256, "system": sysmsg, "tools": tools, "messages": msgs}, hdr)
print("SWITCH-1 sonnet", t1, d1.get("model"), [c.get("text") for c in d1["content"] if c["type"] == "text"])
msgs += [{"role": "assistant", "content": d1["content"]}, {"role": "user", "content": "What was the code word? Reply only the word."}]
d2, t2 = post("/v1/messages", {"model": "claude-opus-5-5", "max_tokens": 256, "system": sysmsg, "tools": tools, "messages": msgs}, hdr)
print("SWITCH-2 opus", t2, d2.get("model"), [c.get("text") for c in d2["content"] if c["type"] == "text"])

o, t3 = post("/v1/chat/completions", {"model": "gpt-oss-120b", "messages": [{"role": "user", "content": "Reply with exactly: PONG-OSS " + str(time.time())}]})
print("OPENAI gpt-oss", t3, o.get("model"), o["choices"][0]["message"].get("content"))

r, t4 = post("/v1/responses", {"model": "gemini-3.8-flash", "input": "Reply with exactly: PONG-FLASH " + str(time.time())})
txt = [c.get("text") for it in r.get("output", []) for c in (it.get("content") or []) if c.get("type") == "output_text"]
print("RESPONSES flash", t4, r.get("model"), txt)
