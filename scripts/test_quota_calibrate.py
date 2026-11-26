#!/usr/bin/env python3
"""Offline tests for quota_calibrate pricing math (no network, no quota)."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import quota_calibrate as Q  # noqa: E402

SONNET = {"inPerM": 2.0, "uncachedWritePerM": 2.5, "cachedPerM": 0.10, "outPerM": 10.0}
ARGON = {"inPerM": 2.0, "cachedPerM": 0.10, "outPerM": 10.0}


class QuotaCalibrateTest(unittest.TestCase):
    def test_predict_matches_measured_2026_11_11(self):
        # (raw_in, raw_cr, out, 3p 5h fraction) measured on Sonnet 5.5, pool 24.83 USD
        for raw_in, raw_cr, out, frac in [(34525, 0, 4, 0.00278121), (33562, 2212, 5, 0.0033651), (193, 35678, 5, 0.00016207)]:
            measured = frac * Q.POOL_5H_USD["claude"]
            self.assertAlmostEqual(Q.predict_usd(SONNET, raw_in, raw_cr, out) / measured, 1.0, delta=0.02)
        self.assertAlmostEqual(Q.predict_usd(ARGON, 2129, 34056, 6) / (3.862e-05 * 200), 1.0, delta=0.02)

    def test_implied_unit_prices(self):
        usd = Q.predict_usd(ARGON, 300000, 0, 5)
        self.assertAlmostEqual(Q.implied(ARGON, 300000, 0, 5, usd)["inPerM"], 2.0, places=3)
        usd = Q.predict_usd(SONNET, 200, 230000, 5)
        self.assertAlmostEqual(Q.implied(SONNET, 200, 230000, 5, usd)["cachedPerM"], 0.10, places=3)
        usd = Q.predict_usd(ARGON, 600, 0, 6000)
        self.assertAlmostEqual(Q.implied(ARGON, 600, 0, 6000, usd)["outPerM"], 10.0, places=3)

    def test_context_is_unique_and_sized(self):
        a, b = Q.context_text(20000, "gemini"), Q.context_text(20000, "gemini")
        self.assertNotEqual(a[:30], b[:30])
        self.assertEqual(len(a), int(20000 * Q.CHARS_PER_TOKEN["gemini"]))

    def test_estimate_and_plan(self):
        class A:
            tokens, long_tokens, long_cache = 30000, 230000, True
        steps = Q.plan(["claude-sonnet-5-5-low"], ["cache", "long"], A)
        self.assertEqual([s[1] for s in steps], ["cache", "long", "long-cache"])
        est = Q.estimate(steps, {"claude-sonnet-5-5": SONNET}, 3)
        self.assertGreater(est["claude"]["usd"], 1.0)


if __name__ == "__main__":
    unittest.main()
