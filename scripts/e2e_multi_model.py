#!/usr/bin/env python3
"""Model gate: in-conversation model switching plus OpenAI chat / Responses (JSON and stream).

Fake (default): Flash -> Carbon -> Argon(high) -> Sonnet in one session, each turn must report and execute
the requested model/tier (fake reply "FAKE_OK model=<id> tier=<tier> turn=<n>") without losing the session;
chat/Responses must return the requested model in protocol shape.
  python3 scripts/e2e_multi_model.py <port> --fake-receipt <receipt.json>
Live (single model, 3 generations): python3 scripts/e2e_multi_model.py 18892 --live --model gemini-3.8-flash-low
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
    return E.run_main([
        ("model_switch", lambda mode, _r: E.scenario_model_switch(args.port, mode, model)),
        ("protocols", lambda mode, _r: E.scenario_protocols(args.port, mode, model)),
    ], args, args.port)


if __name__ == "__main__":
    raise SystemExit(main())
