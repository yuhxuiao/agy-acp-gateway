#!/usr/bin/env python3
"""Tool round-trip gate: step 1 must return a Bash tool_use, step 2 must end_turn with the tool result.

Fake (default): python3 scripts/e2e_tool_call.py <port> --fake-receipt <receipt.json>
Live (1 model): python3 scripts/e2e_tool_call.py 18892 --live --model gemini-3.8-flash-low   (2 generations)
Exit 0 only when the round trip is observed; 1 on failure; 2 on unsafe/unknown target.
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import e2e_common as E  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("port", type=int)
    E.add_mode_args(ap)
    args = ap.parse_args()
    model = args.model or E.LIVE_MODEL
    return E.run_main([("tool_roundtrip", lambda mode, _r: E.scenario_tool_roundtrip(args.port, mode, model))], args, args.port)


if __name__ == "__main__":
    raise SystemExit(main())
