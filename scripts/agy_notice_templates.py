#!/usr/bin/env python3
"""Audit agy's user-facing status texts in a binary (review 2026-11-11, O10).

The gateway filters agy status lines (go-gateway/agy_notice.go). If a new agy release changes
one of those texts, or adds a new "please try again" style status, the filter silently stops
working. This script:
  * checks that every text the gateway depends on is still present verbatim;
  * extracts status-like sentences and compares them with the reviewed baseline
    (scripts/agy_notice_templates.json); new sentences are flagged for review.

No process is started and no request is sent.

Usage: agy_notice_templates.py <binary> [--report FILE] [--write-baseline]
Exit 0 = ok (new templates are reported as review_needed), 1 = a required text is missing,
2 = cannot read. Template extraction is heuristic (Go packs string constants back to back), so a
new template is an alert for review, not a hard failure.
"""
import argparse
import json
import re
import sys
from pathlib import Path

BASELINE = Path(__file__).with_name("agy_notice_templates.json")

# Texts handled by go-gateway/agy_notice.go and acp_engine.go retryNoticeRe.
REQUIRED = [
    "There was a network issue connecting to the server, please try again.",
    "Our servers are experiencing high traffic right now, please try again in a minute.",
    "Agent execution terminated due to error.",
    "The stream was interrupted. Please continue the task you were working on.",
    "API error (attempt %d): %v",
    "\n⚠ %s\n",
]

KEYWORDS = [b"please try again", b"Please try again", b"try again in", b"Please continue", b"was interrupted",
            b"terminated due to", b"API error (attempt"]
# Go packs string constants back to back; a lower-case letter or sentence end directly followed by
# an upper-case letter marks the start of the next constant.
BOUNDARY = re.compile(rb"(?<=[a-z0-9.)!?%:\]`'\"])(?=[A-Z])|[\x00-\x1f|\\]")
SENTENCE_END = re.compile(rb"[.!?]|[\x00-\x1f]")


def extract(data: bytes) -> list:
    """Status-like sentences around KEYWORDS, normalised so neighbouring string constants
    (which move between builds) do not leak into the template text."""
    found = set()
    for kw in KEYWORDS:
        start = 0
        while True:
            i = data.find(kw, start)
            if i < 0:
                break
            start = i + 1
            lo = max(0, i - 160)
            head = data[lo:i + 1]  # include the keyword's first byte so a boundary right before it counts
            cut = [m.end() if m.group(0)[:1] in b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\x0c\r\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f|\\" else m.start()
                   for m in BOUNDARY.finditer(head)]
            begin = lo + (max(cut) if cut else 0)
            tail = data[i:i + 200]
            m = SENTENCE_END.search(tail)
            end = i + (m.end() if m and m.group(0) in (b".", b"!", b"?") else (m.start() if m else len(tail)))
            nb = BOUNDARY.search(data, i + len(kw), end)
            if nb:
                end = nb.start()
            text = data[begin:end].decode("utf-8", "replace").strip()
            if 12 <= len(text) <= 240 and "\ufffd" not in text:
                found.add(text)
    return sorted(found)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("binary")
    ap.add_argument("--report")
    ap.add_argument("--write-baseline", action="store_true", help="record the current templates as reviewed")
    args = ap.parse_args()
    try:
        data = Path(args.binary).read_bytes()
    except OSError as exc:
        print(f"error: {exc}")
        return 2
    templates = extract(data)
    missing = [t for t in REQUIRED if t.encode() not in data]
    if args.write_baseline:
        BASELINE.write_text(json.dumps({"templates": templates}, ensure_ascii=False, indent=1) + "\n")
        print(f"baseline written: {len(templates)} templates")
    baseline = json.loads(BASELINE.read_text()).get("templates", []) if BASELINE.exists() else []
    new = [t for t in templates if t not in baseline]
    gone = [t for t in baseline if t not in templates]
    result = {"ok": not missing, "review_needed": bool(new), "missing_required": missing, "new_templates": new,
              "removed_templates": gone, "templates": len(templates), "generation_requests": 0}
    if args.report:
        Path(args.report).write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    for t in missing:
        print(f"[FAIL] required status text missing: {t}")
    for t in new:
        print(f"[WARN] new status template (review, then --write-baseline): {t}")
    for t in gone:
        print(f"[i] template no longer present: {t}")
    print(f"[=] {'ok' if result['ok'] else 'FAILED'}{' (review needed)' if new else ''} ({len(templates)} templates)")
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
