#!/usr/bin/env python3
"""Measure how Antigravity quota is charged per token type, against the gateway price table.

Each scenario sends real requests to an isolated gateway instance (default port 18892) and reads the
official /usage pool fractions before and after every request. The measured USD of a request is
(pool fraction used) x (pool size); the predicted USD comes from the gateway's own price table.

Without --live the script only prints the plan and an estimate of the quota it would use.

Scenarios:
  cold    one uncached request (random text, no tools)          -> input weight
  output  one short prompt asking for a long answer              -> output weight (incl. thinking)
  cache   N turns in one session (tools + session header)        -> cache write / cache read weights
  long    cold request of --long-tokens, then (with --long-cache) cache turns at that size
          -> long-context surcharge on input, cache write and cache read
  longcache  only the cache turns at --long-tokens

Examples:
  scripts/quota_calibrate.py --models claude-sonnet-5-5-low --scenarios cache,long --long-tokens 230000
  scripts/quota_calibrate.py --models gemini-4-argon-low,gemini-4-carbon-low --scenarios cold,output,cache --live
"""
import argparse
import contextlib
import json
import os
from pathlib import Path
import random
import re
import string
import subprocess
import sys
import time
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parent))
import agy_upgrade as U  # noqa: E402

ROOT = U.ROOT
POOL_OF = {"gemini": ("gemini-5h", "gemini-weekly"), "claude": ("3p-5h", "3p-weekly")}
# Pool sizes in list-price USD, measured 2026-11-11 (history/release-20261111-quota.md).
POOL_5H_USD = {"gemini": 200.0, "claude": 24.83}
TOOLS = [{"name": "noop", "description": "Never call this tool.", "input_schema": {"type": "object", "properties": {}}}]
CHARS_PER_TOKEN = {"gemini": 2.8, "claude": 1.9}  # measured on go-gateway source: 100K chars = 36K Gemini / 53K Claude tokens


def family(model):
    return "claude" if model.startswith("claude") else "gemini"


def base_model(model):
    return re.sub(r"-(low|medium|high|xhigh)$", "", model)


def corpus():
    files = sorted((ROOT / "go-gateway").glob("*.go"))
    text = "".join(f.read_text() for f in files if not f.name.endswith("_test.go"))
    # the gateway answers some Claude Code utility prompts locally; keep their markers out of the context
    for marker in ("[SUGGESTION MODE:", "Describe your most recent action in 3-5 words"):
        text = text.replace(marker, "")
    return text


def context_text(tokens, fam):
    """Natural text of about `tokens` tokens, unique per call so earlier runs never hit cache."""
    src = corpus()
    need = int(tokens * CHARS_PER_TOKEN[fam])
    nonce = "run-" + "".join(random.choices(string.ascii_lowercase + string.digits, k=16))
    out, i = [nonce + "\n"], 0
    while sum(len(x) for x in out) < need:
        out.append(f"\n// copy {i} {nonce}\n" + src)
        i += 1
    return "".join(out)[:need]


def read_quota(home):
    env = dict(os.environ, HOME=str(home), DBUS_SESSION_BUS_ADDRESS="unix:path=/dev/null")
    proxy = os.environ.get("AGY_UPSTREAM_PROXY", "http://127.0.0.1:7890")
    if proxy:
        env.update(HTTPS_PROXY=proxy, HTTP_PROXY=proxy)
    out = subprocess.run([str(ROOT / "bin/agy-acp-900k"), "-p", "/usage", "--output-format", "json"], env=env,
                         capture_output=True, text=True, timeout=90, cwd="/tmp").stdout
    data = json.loads(out)
    used = {"t": time.time()}
    for group in data["command"]["data"]["groups"]:
        for b in group["buckets"]:
            used[b["id"]] = 1 - b.get("remaining_fraction", 1)
    return used


def stable_quota(home, settle):
    """Official /usage refreshes in ~20s; wait, then re-read until two reads agree."""
    time.sleep(settle)
    a = read_quota(home)
    for _ in range(8):
        time.sleep(10)
        b = read_quota(home)
        if all(abs(a[k] - b[k]) < 1e-9 for k in a if k != "t"):
            return b
        a = b
    return a


def post(port, model, messages, session, tools, max_tokens=64):
    body = {"model": model, "max_tokens": max_tokens, "messages": messages,
            "system": "Primary working directory: /tmp\nDo not use any tools. Follow the user's instruction exactly."}
    headers = {"Content-Type": "application/json"}
    if tools:
        body["tools"] = TOOLS
        headers["x-claude-code-session-id"] = session
    else:
        headers["x-session-id"] = session
    req = urllib.request.Request(f"http://127.0.0.1:{port}/v1/messages", data=json.dumps(body).encode(), headers=headers)
    with urllib.request.urlopen(req, timeout=900) as r:
        d = json.load(r)
    text = "".join(c.get("text", "") for c in d.get("content", []) if c.get("type") == "text")
    return d, messages + [{"role": "assistant", "content": text or "OK"}]


def last_turn(log):
    lines = [x for x in Path(log).read_text(errors="replace").splitlines() if "[TURN_DONE]" in x]
    m = re.search(r"\{.*\}$", lines[-1]) if lines else None
    return json.loads(m.group(0)) if m else {}


def price_table(port):
    d = json.load(urllib.request.urlopen(f"http://127.0.0.1:{port + 1}/api/dashboard", timeout=30))
    return {m["id"]: m["pricing"] for m in d["gateway"]["models"]}


def predict_usd(p, raw_in, raw_cr, out):
    """Mirror of go-gateway PriceUsage: cache write premium only on warm turns."""
    in_price = p.get("uncachedWritePerM") if (p.get("uncachedWritePerM") and raw_cr > 0) else p["inPerM"]
    return (raw_in * in_price + raw_cr * p["cachedPerM"] + out * p["outPerM"]) / 1e6


def implied(p, raw_in, raw_cr, out, measured):
    """Unit prices implied by one measurement, solving for the dominant token type."""
    res = {}
    cr_cost = raw_cr * p["cachedPerM"] / 1e6
    out_cost = out * p["outPerM"] / 1e6
    if raw_in > 0 and raw_in >= raw_cr:
        res["inPerM"] = round((measured - cr_cost - out_cost) / raw_in * 1e6, 4)
    if raw_cr > 0 and raw_cr > raw_in:
        in_price = p.get("uncachedWritePerM") or p["inPerM"]
        res["cachedPerM"] = round((measured - raw_in * in_price / 1e6 - out_cost) / raw_cr * 1e6, 4)
    if out > 10 * max(raw_in, 1) / 20 and out > 1000:
        res["outPerM"] = round((measured - raw_in * p["inPerM"] / 1e6 - cr_cost) / out * 1e6, 4)
    return res


def plan(models, scenarios, args):
    steps = []
    for model in models:
        fam = family(model)
        for sc in scenarios:
            if sc == "cold":
                steps.append((model, "cold", args.tokens))
            elif sc == "output":
                steps.append((model, "output", 0))
            elif sc == "cache":
                steps.append((model, "cache", args.tokens))
            elif sc == "long":
                steps.append((model, "long", args.long_tokens))
                if args.long_cache:
                    steps.append((model, "long-cache", args.long_tokens))
            elif sc == "longcache":
                steps.append((model, "long-cache", args.long_tokens))
            else:
                raise SystemExit(f"unknown scenario {sc}")
        if fam not in POOL_OF:
            raise SystemExit(f"unknown model family for {model}")
    return steps


def estimate(steps, prices, turns):
    total = {"gemini": 0.0, "claude": 0.0}
    for model, sc, tok in steps:
        p = prices.get(base_model(model))
        if not p:
            continue
        fam = family(model)
        if sc == "output":
            usd = 6000 * p["outPerM"] / 1e6
        elif sc in ("cache", "long-cache"):
            write = p.get("uncachedWritePerM") or p["inPerM"]
            usd = tok * (p["inPerM"] + write + (turns - 2) * p["cachedPerM"]) / 1e6
        else:
            usd = tok * p["inPerM"] / 1e6
        total[fam] += usd
    return {f: {"usd": round(v, 3), "pct_of_5h_pool": round(v / POOL_5H_USD[f] * 100, 2)} for f, v in total.items()}


@contextlib.contextmanager
def instance(stage, port):
    env = U.instance_env(stage, "live", ROOT / "bin/agy-acp-900k", port)
    U.prepare_auth_link(Path(env["AGY_PURE_API_HOME"]))
    env["AGY_UPSTREAM_PROXY"] = os.environ.get("AGY_UPSTREAM_PROXY", "http://127.0.0.1:7890")
    gw = stage / U.FILES["gateway"]
    if not gw.exists():
        subprocess.run(["cp", "-p", str(ROOT / "bin/agy-acp-gateway"), str(gw)], check=True)
    U.run([ROOT / "scripts/gw.sh", "start", "go"], env=env, log=stage / "control.log", timeout=40)
    try:
        yield env
    finally:
        U.run([ROOT / "scripts/gw.sh", "stop"], env=env, log=stage / "control.log", timeout=35)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--models", required=True, help="comma list, e.g. gemini-4-argon-low,claude-sonnet-5-5-low")
    ap.add_argument("--scenarios", default="cold,output,cache", help="comma list of cold,output,cache,long,longcache")
    ap.add_argument("--tokens", type=int, default=30000, help="context size for cold and cache scenarios")
    ap.add_argument("--long-tokens", type=int, default=230000, help="context size for the long scenario")
    ap.add_argument("--long-cache", action="store_true", help="also run cache turns at --long-tokens")
    ap.add_argument("--turns", type=int, default=3, help="turns per cache scenario (first is cold)")
    ap.add_argument("--spacing", type=int, default=90, help="seconds between cache turns (Gemini implicit cache warm-up)")
    ap.add_argument("--settle", type=int, default=25, help="seconds to wait before reading /usage after a request")
    ap.add_argument("--port", type=int, default=18892)
    ap.add_argument("--live", action="store_true", help="send real requests (uses real quota)")
    args = ap.parse_args()

    models = [m.strip() for m in args.models.split(",") if m.strip()]
    steps = plan(models, [s.strip() for s in args.scenarios.split(",") if s.strip()], args)
    stage = U.checked_path(U.STATE / "probes" / ("quota-" + time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())))
    stage.mkdir(parents=True, mode=0o700)
    qhome = stage / "quota-home"
    (qhome / ".gemini/antigravity-cli").mkdir(parents=True, mode=0o700)
    token = Path.home() / ".gemini/antigravity-cli/antigravity-oauth-token"
    (qhome / ".gemini/antigravity-cli" / token.name).symlink_to(token)
    out_file = stage / "results.jsonl"

    with U.upgrade_lock(), instance(stage, args.port) as env:
        prices = price_table(args.port)
        est = estimate(steps, prices, args.turns)
        print(json.dumps({"stage": str(stage), "steps": steps, "estimate": est}, ensure_ascii=False))
        if not args.live:
            print("dry run: add --live to send these requests")
            return 0
        log = env["AGY_GW_LOG"]
        q = read_quota(qhome)
        for model, sc, tok in steps:
            fam, p = family(model), prices[base_model(model)]
            pool5h, poolw = POOL_OF[fam]
            other = [k for k in q if k not in ("t", pool5h, poolw)]
            sid = f"qcal-{sc}-{model}-{time.time_ns()}"
            if sc == "output":
                turns = [[{"role": "user", "content": "Write a detailed essay of about 1800 words on the history of bridges. Plain prose, no headings."}]]
                tools, max_tokens = False, 8192
            else:
                ctx = context_text(tok, fam)
                first = [{"role": "user", "content": "Reply with exactly OK. Context only, do not analyze:\n\n" + ctx}]
                n = args.turns if sc in ("cache", "long-cache") else 1
                turns, tools, max_tokens = [first] + [None] * (n - 1), sc in ("cache", "long-cache"), 64
            msgs = None
            for i, t in enumerate(turns):
                if i > 0:
                    time.sleep(args.spacing)
                    msgs.append({"role": "user", "content": f"Reply with exactly OK{i + 1}."})
                else:
                    msgs = t
                resp, msgs = post(args.port, model, msgs, sid, tools, max_tokens)
                q1 = stable_quota(qhome, args.settle)
                turn = last_turn(log)
                raw_in = turn.get("rawUncachedInputTokens", turn.get("inputTokens", 0))
                raw_cr = turn.get("rawTpuCachedTokens", turn.get("cachedReadTokens", 0))
                out = turn.get("outputTokens", 0)
                measured = (q1[pool5h] - q[pool5h]) * POOL_5H_USD[fam]
                pred = predict_usd(p, raw_in, raw_cr, out)
                rec = {"scenario": sc, "turn": i + 1, "model": model, "stop": resp.get("stop_reason"),
                       "raw_in": raw_in, "raw_cr": raw_cr, "out": out, "thinking": turn.get("thinkingTokens"),
                       "d5h": q1[pool5h] - q[pool5h], "dweekly": q1[poolw] - q[poolw],
                       "other_pool_moved": any(abs(q1[k] - q[k]) > 1e-9 for k in other),
                       "measured_usd": round(measured, 6), "predicted_usd": round(pred, 6),
                       "ratio": round(measured / pred, 4) if pred else None,
                       "implied": implied(p, raw_in, raw_cr, out, measured)}
                with out_file.open("a") as f:
                    f.write(json.dumps(rec) + "\n")
                print(json.dumps(rec)); sys.stdout.flush()
                q = q1
    print(json.dumps({"results": str(out_file)}))
    return 0


if __name__ == "__main__":
    os.umask(0o077)
    raise SystemExit(main())
