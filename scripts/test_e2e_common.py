import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import e2e_common as E  # noqa: E402


def parse(*argv):
    ap = argparse.ArgumentParser()
    E.add_mode_args(ap)
    return ap.parse_args(list(argv))


class ModeGuardTests(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict(os.environ, {"AGY_E2E_FAKE_RECEIPT": ""})
        self.env.start()
        self.addCleanup(self.env.stop)

    def test_unsafe_targets_are_refused_without_network(self):
        with patch("urllib.request.urlopen") as request:
            for argv, port in [((), 18896), ((), 18890), (("--live",), 18892),
                               (("--live", "--model", "gemini-4-argon"), 18892),
                               (("--live", "--model", E.LIVE_MODEL), 18890)]:
                with self.subTest(argv=argv, port=port):
                    with self.assertRaises(ValueError):
                        E.resolve_mode(parse(*argv), port)
            request.assert_not_called()

    def test_live_single_flash_on_isolated_port_is_allowed(self):
        self.assertEqual(E.resolve_mode(parse("--live", "--model", E.LIVE_MODEL), 18892), ("live", None))

    def test_fake_receipt_must_match_running_gateway(self):
        with tempfile.TemporaryDirectory() as tmp:
            exe = E.pid_executable(os.getpid())
            digest = hashlib.sha256(exe.read_bytes()).hexdigest()
            receipt = Path(tmp)/"r.json"
            receipt.write_text(json.dumps({"mode": "fake", "pid": os.getpid(), "port": 18896, "gateway_sha256": digest}))
            self.assertEqual(E.resolve_mode(parse("--fake-receipt", str(receipt)), 18896)[0], "fake")
            for change in ({"port": 18897}, {"mode": "live"}, {"gateway_sha256": "0" * 64}):
                with self.subTest(change=change):
                    receipt.write_text(json.dumps({"mode": "fake", "pid": os.getpid(), "port": 18896, "gateway_sha256": digest, **change}))
                    with self.assertRaises(ValueError):
                        E.resolve_mode(parse("--fake-receipt", str(receipt)), 18896)

    def test_fake_identity_and_model_tier(self):
        self.assertEqual(E.fake_identity("x FAKE_OK model=gemini-4-carbon tier=medium turn=2"), ("gemini-4-carbon", "medium", 2))
        self.assertIsNone(E.fake_identity("OK"))
        self.assertEqual(E.model_tier("gemini-3.8-flash-low"), ("gemini-3.8-flash", "low"))
        self.assertEqual(E.model_tier("gemini-4-hydrogen-xhigh"), ("gemini-4-hydrogen", "xhigh"))
        self.assertEqual(E.model_tier("gemini-4-carbon"), ("gemini-4-carbon", "medium"))
        self.assertEqual(E.gemini_text({"candidates": [{"content": {"parts": [{"thought": True, "text": "skip"}, {"text": "PONG"}]}}]}), "PONG")

    def test_failed_scenario_gives_nonzero_exit(self):
        with tempfile.TemporaryDirectory() as tmp:
            exe = E.pid_executable(os.getpid())
            receipt = Path(tmp)/"r.json"
            receipt.write_text(json.dumps({"mode": "fake", "pid": os.getpid(), "port": 18896,
                                           "gateway_sha256": hashlib.sha256(exe.read_bytes()).hexdigest()}))
            args = parse("--fake-receipt", str(receipt))
            with patch("builtins.print"):
                self.assertEqual(E.run_main([("a", lambda m, r: {"ok": True})], args, 18896), 0)
                self.assertEqual(E.run_main([("a", lambda m, r: {"ok": True}), ("b", lambda m, r: {"ok": False})], args, 18896), 1)
                self.assertEqual(E.run_main([("c", lambda m, r: 1 / 0)], args, 18896), 1)


if __name__ == "__main__":
    unittest.main()
