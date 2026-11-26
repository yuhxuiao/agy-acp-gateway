import json
from pathlib import Path
import socket
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import agy_upgrade as U  # noqa: E402
import e2e_common as E  # noqa: E402
import e2e_db_telemetry as DBT  # noqa: E402
import fake_instance as F  # noqa: E402


def _free_port_pair():
    for base in range(28910, 29500, 2):
        if base in F.FORBIDDEN_PORTS or (base + 1) in F.FORBIDDEN_PORTS:
            continue
        ok = True
        for p in (base, base + 1):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.bind(("127.0.0.1", p))
            except OSError:
                ok = False
                break
        if ok:
            return base
    raise RuntimeError("no free local port pair available for test")


class FakeInstanceAndTelemetryTests(unittest.TestCase):
    def test_refuses_production_and_adjacent_ports(self):
        with tempfile.TemporaryDirectory(prefix="fake-inst-unit-") as tmp:
            for bad in (18888, 18889, 18890, 18891, 80, 65535):
                with self.subTest(port=bad):
                    with self.assertRaises(ValueError):
                        F._validate_port(bad)
                    with self.assertRaises(ValueError):
                        F._env(Path(tmp), bad, Path(tmp) / "fakeacp", {})

    def test_env_allowlist_and_conv_dir_sync(self):
        for bad_kv in (["PATH=/evil"], ["AGY_ACP_BIN=/bin/sh"], ["LD_PRELOAD=/x.so"], ["NO_EQUALS"]):
            with self.subTest(bad=bad_kv):
                with self.assertRaises(ValueError):
                    F.parse_extra_env(bad_kv)

        good = F.parse_extra_env([
            "FAKE_ACP_SERVED_MODEL=gemini-4-carbon-exp",
            "FAKE_ACP_CACHE_READ_TOKENS=32768",
            "REVIEW_FAULT=baseline",
            "AGY_API_KEYS=test-key-1",
        ])
        self.assertEqual(good["FAKE_ACP_SERVED_MODEL"], "gemini-4-carbon-exp")
        self.assertEqual(good["AGY_API_KEYS"], "test-key-1")

        with tempfile.TemporaryDirectory(prefix="fake-inst-conv-") as tmp:
            stage = Path(tmp).resolve()
            custom_conv = stage / "custom-convs"
            env = F._env(stage, 18896, stage / "fakeacp", {"FAKE_ACP_CONV_DIR": str(custom_conv)})
            self.assertEqual(env["AGY_CONV_DIR"], str(custom_conv))
            self.assertEqual(env["FAKE_ACP_CONV_DIR"], str(custom_conv))
            self.assertEqual(env["FAKE_ACP_WRITE_DB"], "1")
            self.assertEqual(env["FAKE_ACP_EMIT_USAGE_UPDATE"], "1")

            with self.assertRaises(ValueError):
                F._env(stage, 18896, stage / "fakeacp", {"FAKE_ACP_CONV_DIR": "/mnt/c/Users/x"})

    def test_pid_reuse_refuses_signal(self):
        with tempfile.TemporaryDirectory(prefix="fake-inst-pid-") as tmp:
            stage = Path(tmp).resolve()
            run_dir = stage / "instance-fake-18896"
            run_dir.mkdir(parents=True)
            # Current Python process is alive, with a deliberately mismatched start identity
            (run_dir / "gateway.pid").write_text(f"{socket_pid()}\n")
            U.write_json(run_dir / "gateway.ident.json", {
                "pid": socket_pid(),
                "start": "STALE_START_TICKS_NEVER_MATCH",
                "gateway_bin": str(stage / U.FILES["gateway"]),
            })
            with patch("os.kill") as kill_mock:
                with self.assertRaisesRegex(RuntimeError, "PID .* was reused"):
                    F.stop(stage, 18896)
                kill_mock.assert_not_called()

    def test_e2e_db_telemetry_refuses_live_and_main_ports(self):
        with patch("urllib.request.urlopen") as urlopen_mock:
            self.assertEqual(DBT.main(["18894", "--live", "--model", E.LIVE_MODEL]), 2)
            self.assertEqual(DBT.main(["18890"]), 2)
            urlopen_mock.assert_not_called()
        with self.assertRaises(E.GateError):
            DBT._dash_port(18890, {"dash_port": 18891})

    def test_isolated_env_overrides_and_clean_shutdown(self):
        port = _free_port_pair()
        with tempfile.TemporaryDirectory(prefix="fake-inst-e2e-") as tmp:
            stage = Path(tmp).resolve()
            conv_dir = stage / "override-convs"
            extra = {
                "FAKE_ACP_CONV_DIR": str(conv_dir),
                "FAKE_ACP_SERVED_MODEL": "gemini-4-carbon-canary",
                "FAKE_ACP_CACHE_READ_TOKENS": "32768",
                "FAKE_ACP_THINKING_MS": "60",
                "FAKE_ACP_EMIT_USAGE_UPDATE": "1",
            }
            gw_pid = None
            with F.instance(stage, port, extra=extra) as receipt_file:
                receipt = E.verify_fake_receipt(port, receipt_file)
                gw_pid = receipt["pid"]
                self.assertEqual(receipt["dash_port"], port + 1)
                self.assertEqual(U.version(stage / F.FAKEACP), "1.5.10-fakeacp")

                sid = "env-override-check"
                st, _, data = E.post(port, "/v1/messages", {
                    "model": "gemini-4-carbon",
                    "max_tokens": 128,
                    "system": E.SYS,
                    "messages": [{"role": "user", "content": "Plain prompt with zero markers"}],
                }, sid)
                self.assertEqual(st, 200)
                self.assertEqual((data.get("usage") or {}).get("cache_read_input_tokens"), 32768)

                d_st, dash = DBT._get_dash(port + 1, "/api/dashboard?refresh=1")
                self.assertEqual(d_st, 200)
                turns = DBT._turns_for_sid(dash, sid)
                self.assertEqual(len(turns), 1)
                self.assertEqual(turns[0]["usageSource"], "db")
                self.assertEqual(turns[0]["cachedReadTokens"], 32768)
                self.assertEqual(turns[0]["servedModel"], "gemini-4-carbon-canary")
                self.assertTrue(turns[0]["servedMismatch"])
                self.assertGreater(turns[0]["thinkingMs"], 0)

                live = DBT._live_for_sid(dash, sid)
                self.assertIsNotNone(live)
                self.assertEqual(live["reportedContextSize"], 983040)
                acp_sid = live["sessionId"]
                self.assertTrue((conv_dir / f"{acp_sid}.db").is_file())
                self.assertGreaterEqual((conv_dir / f"{acp_sid}.db").stat().st_size, 4096)

            self.assertIsNotNone(gw_pid)
            self.assertIsNone(U.process_identity(gw_pid))
            self.assertFalse(F._port_listening(port))
            self.assertFalse(F._port_listening(port + 1))
            self.assertFalse(receipt_file.exists())


def socket_pid():
    import os
    return os.getpid()


if __name__ == "__main__":
    unittest.main()
