#!/usr/bin/env python3
"""Thinking-tier gate: client thinking controls must map to the agy tier.

Each case uses its own session id; the tier is read from that session's SESSION_NEW log line (filtered by
convKey) and, in fake mode, also from the fake reply ("tier=<tier>"), so the tier really applied upstream
is checked. Any mismatch exits 1.
Fake (default): python3 scripts/e2e_thinking_tier.py <port> [<gateway.log>] --fake-receipt <receipt.json>
  (log defaults to the receipt's gateway_log)
Live (1 generation): python3 scripts/e2e_thinking_tier.py 18892 <gateway.log> --live --model gemini-3.8-flash-low
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import e2e_common as E  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("port", type=int)
    ap.add_argument("log", nargs="?", type=Path)
    E.add_mode_args(ap)
    args = ap.parse_args()

    def run(mode, receipt):
        log = args.log or (Path(receipt["gateway_log"]) if receipt and receipt.get("gateway_log") else None)
        return E.scenario_thinking_tiers(args.port, mode, log)
    return E.run_main([("thinking_tiers", run)], args, args.port)


if __name__ == "__main__":
    raise SystemExit(main())
