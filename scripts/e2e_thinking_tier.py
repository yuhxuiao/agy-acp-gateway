"""Check client thinking controls -> agy tier mapping via real requests; prints the tier from gateway log."""
import json, os, re, sys, time, urllib.request

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 18890
LOG = sys.argv[2] if len(sys.argv) > 2 else os.path.join(ROOT, "logs", "gateway.log")
KEY = os.environ.get("AGY_KEY", "")
SYS = "Primary working directory: /tmp"
TOOLS = [{"name": "Bash", "description": "Run bash", "input_schema": {"type": "object", "properties": {"command": {"type": "string"}}}}]


def post(path, body, sid):
    h = {"content-type": "application/json", "anthropic-version": "2023-06-01", "x-claude-code-session-id": sid}
    if KEY:
        h["Authorization"] = "Bearer " + KEY
    req = urllib.request.Request(f"http://127.0.0.1:{PORT}{path}", data=json.dumps(body).encode(), headers=h)
    with urllib.request.urlopen(req, timeout=180) as r:
        return json.loads(r.read())


cases = [
    ("anthropic budget 32000", "/v1/messages", {"model": "claude-sonnet-5-5", "max_tokens": 40000, "thinking": {"type": "enabled", "budget_tokens": 32000}}, "high"),
    ("anthropic budget 2048", "/v1/messages", {"model": "claude-sonnet-5-5", "max_tokens": 4000, "thinking": {"type": "enabled", "budget_tokens": 2048}}, "low"),
    ("anthropic no thinking", "/v1/messages", {"model": "claude-sonnet-5-5", "max_tokens": 1000}, "medium"),
    ("openai reasoning_effort=xhigh", "/v1/chat/completions", {"model": "gemini-3.8-flash", "reasoning_effort": "xhigh"}, "high"),
    ("responses effort=minimal", "/v1/responses", {"model": "gemini-3.8-flash", "reasoning": {"effort": "minimal"}}, "low"),
    ("suffix -high, effort=low wins", "/v1/chat/completions", {"model": "gemini-3.8-flash-high", "reasoning_effort": "low"}, "low"),
]
for name, path, extra, want in cases:
    sid = f"tier-{int(time.time()*1000)}"
    q = f"Reply with exactly: OK {sid}"
    body = dict(extra)
    if path == "/v1/messages":
        body.update({"system": SYS, "tools": TOOLS, "messages": [{"role": "user", "content": q}]})
    elif path == "/v1/chat/completions":
        body.update({"messages": [{"role": "system", "content": SYS}, {"role": "user", "content": q}]})
    else:
        body.update({"instructions": SYS, "input": q})
    post(path, body, sid)
    time.sleep(0.3)
    tier = "?"
    for line in reversed(open(LOG, encoding="utf-8", errors="ignore").read().splitlines()[-400:]):
        if "SESSION_NEW" in line and ("tier-" in line or True):
            m = re.search(r'"thoughtLevel":"(\w+)"', line)
            if m:
                tier = m.group(1)
                break
    print(f"{'PASS' if tier == want else 'FAIL'}  {name:34s} want={want:6s} got={tier}")
