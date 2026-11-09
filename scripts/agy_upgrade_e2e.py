#!/usr/bin/env python3
"""Explicit fake acceptance or one authorized Flash request. No retries."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import time
import urllib.request

MODEL = "gemini-3.8-flash-low"


def auth_headers():
    headers = {"Content-Type": "application/json"}
    key = os.environ.get("AGY_KEY") or os.environ.get("AGY_API_KEYS", "").split(",")[0].strip()
    if key:
        headers["x-api-key"] = key
    return headers


def run_probe(port, *, live=False, fake_receipt=None):
    if not live:
        if not fake_receipt:
            raise ValueError("fake mode requires an owned instance receipt")
        receipt = json.loads(Path(fake_receipt).read_text())
        if receipt.get("mode") != "fake" or receipt.get("port") != port:
            raise ValueError("fake instance receipt mismatch")
        exe = Path(f"/proc/{int(receipt['pid'])}/exe").resolve(strict=True)
        if hashlib.sha256(exe.read_bytes()).hexdigest() != receipt["gateway_sha256"]:
            raise ValueError("fake gateway ownership changed")
    body = {"model": MODEL, "max_tokens": 12, "system": "Reply briefly. Do not use any tools.",
            "messages": [{"role": "user", "content": "Reply with exactly: OK"}]}
    headers = auth_headers()
    headers["x-session-id"] = f"upgrade-smoke-{time.time_ns()}"
    request = urllib.request.Request(f"http://127.0.0.1:{port}/v1/messages", data=json.dumps(body).encode(), headers=headers)
    started = time.monotonic()
    with urllib.request.urlopen(request, timeout=120) as response:
        data = json.load(response)
        status = response.status
    text = "".join(x.get("text", "") for x in data.get("content", []) if x.get("type") == "text")
    semantic_ok = bool(re.fullmatch(r"\s*OK[.!。！]?\s*", text, re.I)) if live else "FAKE_OK" in text
    return {"ok": status == 200 and data.get("stop_reason") == "end_turn" and semantic_ok,
            "mode": "live-flash" if live else "fake", "model": MODEL,
            "generation_requests": 1 if live else 0, "requests": 1,
            "http_status": status, "stop_reason": data.get("stop_reason"),
            "semantic_ok": semantic_ok, "elapsed_seconds": round(time.monotonic()-started, 2)}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("port", type=int)
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--live-flash", action="store_true")
    mode.add_argument("--fake-receipt", type=Path)
    ap.add_argument("--report", type=Path)
    args = ap.parse_args()
    try:
        result = run_probe(args.port, live=args.live_flash, fake_receipt=args.fake_receipt)
    except Exception as exc:
        result = {"ok": False, "error_type": type(exc).__name__, "retry": False}
    if args.report:
        args.report.write_text(json.dumps(result, indent=2)+"\n")
    print(json.dumps(result))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
