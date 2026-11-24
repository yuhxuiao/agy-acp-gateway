import contextlib
import gc
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import cache_resilience_acp as CRA  # noqa: E402
import e2e_cache_resilience as ECR  # noqa: E402
import e2e_common as E  # noqa: E402


class _QuietThreadingHTTPServer(ThreadingHTTPServer):
    def handle_error(self, request, client_address):
        return


class ModeAndSafetyGuardTests(unittest.TestCase):
    def setUp(self):
        env = patch.dict(os.environ, {"AGY_E2E_FAKE_RECEIPT": ""})
        env.start()
        self.addCleanup(env.stop)

    def test_refuses_live_main_ports_and_missing_receipt_with_zero_network_calls(self):
        with patch("urllib.request.urlopen") as urlopen_mock, \
                patch("http.client.HTTPConnection") as conn_mock, \
                contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(ECR.main(["18892", "--live", "--model", E.LIVE_MODEL]), 2)
            for mp in ("18889", "18890", "18891"):
                self.assertEqual(ECR.main([mp]), 2)
            self.assertEqual(ECR.main(["18892"]), 2)
            urlopen_mock.assert_not_called()
            conn_mock.assert_not_called()

    def test_rejects_invalid_dash_port_mismatched_receipt_and_missing_log(self):
        for bad_dash in (18889, 18890, 18891, 18892, 80, 70000):
            with self.subTest(bad_dash=bad_dash):
                with self.assertRaises(E.GateError):
                    ECR.resolve_dash_port(18892, {"dash_port": bad_dash})

        self.assertEqual(ECR.resolve_dash_port(18892, {"dash_port": 18893}), 18893)
        self.assertEqual(ECR.resolve_dash_port(18892, None, explicit_dash=18899), 18899)

        with tempfile.TemporaryDirectory(prefix="ecr-guard-") as tmp:
            p = Path(tmp)
            rec_bad_mode = p / "bad_mode.json"
            rec_bad_mode.write_text(json.dumps({"mode": "live", "port": 18892}), encoding="utf-8")
            with patch("urllib.request.urlopen") as urlopen_mock, \
                    patch("http.client.HTTPConnection") as conn_mock, \
                    contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(ECR.main(["18892", "--fake-receipt", str(rec_bad_mode)]), 2)
                urlopen_mock.assert_not_called()
                conn_mock.assert_not_called()

            rec_bad_port = p / "bad_port.json"
            rec_bad_port.write_text(json.dumps({"mode": "fake", "port": 18894}), encoding="utf-8")
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(ECR.main(["18892", "--fake-receipt", str(rec_bad_port)]), 2)

            with self.assertRaises(E.GateError):
                ECR.resolve_log_path(p / "nonexistent.log", {"gateway_log": str(p / "also-missing.log")})


class LogSlicingAndTelemetryHelperTests(unittest.TestCase):
    def test_byte_offset_log_slicing_and_convkey_filtering(self):
        with tempfile.TemporaryDirectory(prefix="ecr-log-") as tmp:
            log_path = Path(tmp) / "gateway.log"
            old_line = '[2026-11-18T00:00:00.000Z] [CACHE_BURN_WARNING] old {"convKey":"sid:alpha:x","streak":3}\n'
            log_path.write_text(old_line, encoding="utf-8")
            off = ECR.log_offset(log_path)
            self.assertEqual(off, len(old_line.encode("utf-8")))

            new_lines = (
                '[2026-11-18T00:00:01.000Z] [PARKED_MCP_RESUME] resumed {"convKey":"sid:alpha:x","sessionId":"s1"}\n'
                '[2026-11-18T00:00:02.000Z] [COMPACTION_ROLLOVER] other {"convKey":"sid:beta:x","reason":"rollover"}\n'
            )
            with log_path.open("a", encoding="utf-8") as f:
                f.write(new_lines)

            sliced = ECR.read_log_slice(log_path, off)
            self.assertEqual(len(sliced), 2)
            self.assertNotIn("CACHE_BURN_WARNING", "\n".join(sliced))

            alpha_only = ECR.filter_log_for_sid(sliced, "alpha")
            self.assertEqual(len(alpha_only), 1)
            cat, meta = ECR.parse_log_line(alpha_only[0])
            self.assertEqual(cat, "PARKED_MCP_RESUME")
            self.assertEqual(meta["sessionId"], "s1")

            waited = ECR.wait_log_lines(log_path, off, "PARKED_MCP_RESUME", timeout=0.2)
            self.assertEqual(len(waited), 1)

    def test_prometheus_parser_and_dashboard_selectors(self):
        raw = (
            "# HELP agy_gateway_cache_burn_warnings_total Total warnings.\n"
            "# TYPE agy_gateway_cache_burn_warnings_total counter\n"
            "agy_gateway_cache_burn_warnings_total 2\n"
            'agy_gateway_soft_detach_total{outcome="joined"} 3\n'
            'agy_gateway_soft_detach_total{outcome="expired"} 1\n'
            "invalid prom line\n"
        )
        bad, metrics = ECR.parse_prom_metrics(raw)
        self.assertEqual(bad, ["invalid prom line"])
        self.assertEqual(ECR.metric_sum(metrics, "agy_gateway_cache_burn_warnings_total"), 2.0)
        self.assertEqual(ECR.metric_sum(metrics, "agy_gateway_soft_detach_total"), 4.0)

        dash = {
            "gatewayTurns": [
                {"convKey": "sid:s1:a", "turnIndex": 3, "cacheMiss": "zero"},
                {"convKey": "sid:s2:a", "turnIndex": 1, "cacheMiss": ""},
                {"convKey": "sid:s1:a", "turnIndex": 1, "cacheMiss": "cold_start"},
            ],
            "liveSessions": [
                {"convKey": "sid:s2:a", "sessionId": "acp-other"},
                {"convKey": "sid:s1:a", "sessionId": "ACP-Target"},
            ],
            "recentDbSteps": [
                {"sessionId": "acp-target", "stepIdx": 2, "cacheMiss": "zero"},
                {"sessionId": "acp-target", "stepIdx": 0, "cacheMiss": "cold_start"},
                {"sessionId": "acp-other", "stepIdx": 0, "cacheMiss": ""},
            ],
        }
        self.assertEqual([t["turnIndex"] for t in ECR.turns_for_sid(dash, "s1")], [1, 3])
        self.assertEqual(ECR.live_for_sid(dash, "s1")["sessionId"], "ACP-Target")
        self.assertEqual([s["stepIdx"] for s in ECR.steps_for_acp_sid(dash, "ACP-Target")], [0, 2])
        self.assertEqual(ECR.metric_sum(metrics, "agy_gateway_soft_detach_total", 'outcome="joined"'), 3.0)

    def test_filter_log_for_sid_captures_acp_hibernate_and_fails_p1a(self):
        lines = [
            '[2026-11-18T00:00:01.000Z] [ACP_HIBERNATE] Closed idle session in shared engine for sid:alpha:1122 {"closedSessionId":"s-rpc","reason":"prompt_rpc_error"}',
            '[2026-11-18T00:00:02.000Z] [ACP_HIBERNATE] Closed idle session in shared engine for sid:alpha-2:1122 {"closedSessionId":"s-other","reason":"prompt_rpc_error"}',
        ]
        filtered = ECR.filter_log_for_sid(lines, "alpha")
        self.assertEqual(len(filtered), 1)
        self.assertIn("s-rpc", filtered[0])

        p1a = {
            "cap_sid": "s-cap",
            "cap_turn2_text": "FALLBACK_MEDIUM_OK",
            "cap_turns": [{"sessionId": "s-cap"}, {"sessionId": "s-cap"}],
            "cap_live": {"compactionRollovers": 0},
            "cap_log_lines": ['[HIGH_TIER_CAPACITY_FALLBACK] ok {"toTier":"medium"}'],
            "park_sid": "s-park",
            "park_turns": [
                {"sessionId": "s-park", "mode": "cold_start", "thoughtLevel": "high"},
                {"sessionId": "s-park", "mode": "parked_mcp_resume", "thoughtLevel": "high"},
                {"sessionId": "s-park", "mode": "sticky_delta", "thoughtLevel": "low"},
            ],
            "park_turn2_text": "TOOL_DONE:FAKE_TOOL_OK",
            "park_turn3_text": "FAKE_OK model=gemini-4-carbon tier=low turn=2",
            "park_live": {"compactionRollovers": 0, "thoughtLevel": "low"},
            "rpc_sid": "s-rpc",
            "rpc_529_status": 529,
            "rpc_recovery_turns": [{"sessionId": "s-rpc", "mode": "cold_start"}, {"sessionId": "s-rpc", "mode": "sticky_delta"}],
            "rpc_live": {"compactionRollovers": 0},
            "rpc_log_lines": filtered,
            "rpc_400_status": 400,
        }
        self.assertFalse(ECR.eval_p1a_fallback_verdict(**p1a)[0])


class HttpAndDisconnectHelperTests(unittest.TestCase):
    def test_post_full_and_disconnect_zero_resource_warnings(self):
        sse_sent = threading.Event()

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, fmt, *args):
                return

            def do_POST(self):
                length = int(self.headers.get("Content-Length") or 0)
                if length:
                    self.rfile.read(length)
                if self.path == "/json":
                    body = json.dumps({"ok": True, "echo_hdr": self.headers.get("x-claude-code-session-id")}).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("X-Agy-Cache-Warning", "consecutive-zero-cache; streak=3")
                    self.send_header("X-Agy-Served-Model", "gemini-4-carbon-exp")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                elif self.path == "/slow-prefill":
                    time.sleep(0.35)
                    try:
                        self.send_response(200)
                        self.end_headers()
                    except OSError:
                        pass
                elif self.path == "/sse-disconnect":
                    self.send_response(200)
                    self.send_header("Content-Type", "text/event-stream")
                    self.end_headers()
                    try:
                        self.wfile.write(b"event: message_start\ndata: {}\n\n")
                        self.wfile.flush()
                        sse_sent.set()
                        time.sleep(0.35)
                    except OSError:
                        pass

        server = _QuietThreadingHTTPServer(("127.0.0.1", 0), Handler)
        port = server.server_address[1]
        th = threading.Thread(target=server.serve_forever, daemon=True)
        th.start()
        try:
            st, hdrs, data = ECR.post_full(port, "/json", {"hello": "world"}, sid="sid-test-1")
            self.assertEqual(st, 200)
            self.assertEqual(hdrs.get("x-agy-cache-warning"), "consecutive-zero-cache; streak=3")
            self.assertEqual(hdrs.get("x-agy-served-model"), "gemini-4-carbon-exp")
            self.assertEqual(data, {"ok": True, "echo_hdr": "sid-test-1"})

            d1 = ECR.disconnect_request(port, "/slow-prefill", {"slow": True}, sid="sid-d1", wait_sec=0.05, read_first_sse=False)
            self.assertTrue(d1["disconnected"])

            d2 = ECR.disconnect_request(port, "/sse-disconnect", {"stream": True}, sid="sid-d2", wait_sec=1.0, read_first_sse=True)
            self.assertTrue(d2["disconnected"])
            self.assertGreater(d2["saw_bytes"], 0)
            self.assertTrue(sse_sent.is_set())
        finally:
            server.shutdown()
            server.server_close()
            th.join(timeout=5)
        gc.collect()

    def test_disconnect_request_coalesced_sse_and_header_timeout(self):
        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, fmt, *args):
                return

            def do_POST(self):
                length = int(self.headers.get("Content-Length") or 0)
                if length:
                    self.rfile.read(length)
                if self.path == "/sse-coalesced":
                    payload = (
                        b"HTTP/1.1 200 OK\r\n"
                        b"Content-Type: text/event-stream; charset=utf-8\r\n"
                        b"Transfer-Encoding: chunked\r\n\r\n"
                        b"24\r\nevent: message_start\ndata: {\"ok\":1}\n\n\r\n"
                    )
                    self.connection.sendall(payload)
                    time.sleep(0.35)
                elif self.path == "/hang-headers":
                    time.sleep(0.5)
                    try:
                        self.send_response(200)
                        self.send_header("Content-Length", "0")
                        self.end_headers()
                    except OSError:
                        pass

        server = _QuietThreadingHTTPServer(("127.0.0.1", 0), Handler)
        port = server.server_address[1]
        th = threading.Thread(target=server.serve_forever, daemon=True)
        th.start()
        try:
            t0 = time.monotonic()
            d_coal = ECR.disconnect_request(port, "/sse-coalesced", {"stream": True}, wait_sec=0.6, read_first_sse=True, timeout=2.0)
            elapsed = time.monotonic() - t0
            self.assertTrue(d_coal["disconnected"])
            self.assertEqual(d_coal.get("status"), 200)
            self.assertGreater(d_coal["saw_bytes"], 0)
            self.assertLess(elapsed, 0.3)

            d_to = ECR.disconnect_request(port, "/hang-headers", {"slow": True}, wait_sec=0.5, read_first_sse=True, timeout=0.12)
            self.assertTrue(d_to["disconnected"])
            self.assertEqual(d_to["saw_bytes"], 0)
            self.assertTrue(d_to.get("timed_out"))
        finally:
            server.shutdown()
            server.server_close()
            th.join(timeout=5)
        gc.collect()

    def test_isolated_subinstance_enter_cleans_up_on_healthz_error_and_timeout(self):
        with tempfile.TemporaryDirectory(prefix="ecr-sub-") as bin_dir:
            bin_path = Path(bin_dir)
            fake_gw = bin_path / "gw"
            fake_acp = bin_path / "acp"
            fake_gw.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            fake_acp.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            receipt = {"pid": 99999, "acp_path": str(fake_acp)}

            class BadHealthHandler(BaseHTTPRequestHandler):
                def log_message(self, fmt, *args):
                    return

                def do_GET(self):
                    if self.path == "/healthz":
                        self.send_response(500)
                        self.send_header("Content-Length", "0")
                        self.end_headers()

            server = ThreadingHTTPServer(("127.0.0.1", 0), BadHealthHandler)
            bad_port = server.server_address[1]
            th = threading.Thread(target=server.serve_forever, daemon=True)
            th.start()

            gw_actions = []
            created_roots = []

            def fake_instance_env(root, mode, binary, port=18892):
                created_roots.append(Path(root))
                run_dir = Path(root) / f"instance-{mode}"
                run_dir.mkdir(parents=True, exist_ok=True)
                return {
                    "AGY_GW_PORT": str(port),
                    "AGY_DASH_PORT": str(port + 1),
                    "AGY_RUN_DIR": str(run_dir),
                    "AGY_LOG_DIR": str(run_dir / "logs"),
                    "AGY_GW_LOG": str(run_dir / "logs" / "gateway.log"),
                    "AGY_PURE_API_HOME": str(run_dir / "pure-api-home"),
                    "AGY_STATE_DB": str(run_dir / "gateway.db"),
                    "AGY_TOS_TRIPPED_FILE": str(run_dir / "tos-tripped"),
                }

            def fake_subproc_run(cmd, **_kwargs):
                gw_actions.append(cmd[2:])
                return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

            def fake_request_full(port, method, path, **_kwargs):
                if path == "/healthz":
                    raise http.client.RemoteDisconnected("simulated transient healthz error")
                return 200, {}, {"ok": True}

            try:
                # 1. Timeout path when /healthz raises HTTPException repeatedly
                sub1 = ECR.IsolatedSubInstance(receipt, tag="tmo")
                with patch.object(E, "pid_executable", return_value=fake_gw), \
                        patch.object(ECR, "_pick_free_port_pair", return_value=bad_port), \
                        patch.object(ECR.U, "instance_env", side_effect=fake_instance_env), \
                        patch.object(ECR.subprocess, "run", side_effect=fake_subproc_run), \
                        patch.object(ECR, "request_full", side_effect=fake_request_full), \
                        patch.object(ECR.time, "sleep", return_value=None):
                    # Speed up deadline loop using monotonic side_effect
                    ticks = iter([0.0, 0.1, 11.0])
                    with patch.object(ECR.time, "monotonic", side_effect=lambda: next(ticks, 11.0)):
                        with self.assertRaises(RuntimeError):
                            sub1.__enter__()
                self.assertEqual(gw_actions, [["start", "go"], ["stop"]])
                self.assertFalse(created_roots[0].exists())

                # 2. HTTP 500 path against real ThreadingHTTPServer /healthz
                gw_actions.clear()
                sub2 = ECR.IsolatedSubInstance(receipt, tag="h500")
                with patch.object(E, "pid_executable", return_value=fake_gw), \
                        patch.object(ECR, "_pick_free_port_pair", return_value=bad_port), \
                        patch.object(ECR.U, "instance_env", side_effect=fake_instance_env), \
                        patch.object(ECR.subprocess, "run", side_effect=fake_subproc_run), \
                        patch.object(ECR.time, "sleep", return_value=None):
                    ticks2 = iter([0.0, 0.1, 11.0])
                    with patch.object(ECR.time, "monotonic", side_effect=lambda: next(ticks2, 11.0)):
                        with self.assertRaises(RuntimeError):
                            sub2.__enter__()
                self.assertEqual(gw_actions, [["start", "go"], ["stop"]])
                self.assertFalse(created_roots[1].exists())
            finally:
                server.shutdown()
                server.server_close()
                th.join(timeout=5)
        gc.collect()


class VerdictEvaluatorTests(unittest.TestCase):
    def test_p0b_verdict_pass_and_fail_closed(self):
        misses = ["cold_start", "", "prefix_only", "prefix_only", "prefix_only", "prefix_only", "zero", "zero", "zero", "zero", ""]
        base = {
            "turns": [{"cacheMiss": m} for m in misses],
            "db_steps": [{"cacheMiss": m} for m in misses],
            "prefix_warn_headers": ["", "", "", "", ""],
            "zero3_warn_header": "consecutive-zero-cache; streak=3; context_tokens=250000; uncached_tokens=735000; model=gemini-4-carbon",
            "stream_warn_header": "consecutive-zero-cache; streak=3; context_tokens=250000; uncached_tokens=735000; model=gemini-4-carbon",
            "recovery_warn_header": "",
            "low50k_warn_headers": ["", "", "", ""],
            "burn_warn_delta_after_prefix": 0.0,
            "burn_warn_delta_after_zero": 1.0,
            "cache_warnings_with_sid_after_zero": [{"streak": 4}],
            "cache_warnings_with_sid_after_recovery": [],
            "sid_log_lines": ['[2026-11-18T00:00:00.000Z] [CACHE_BURN_WARNING] burn {"streak":3}'],
        }
        ok, _ = ECR.eval_p0b_verdict(**base)
        self.assertTrue(ok)

        # False-positive prefix_only warning must fail
        bad1 = dict(base, prefix_warn_headers=["", "consecutive-zero-cache; streak=3", "", "", ""])
        self.assertFalse(ECR.eval_p0b_verdict(**bad1)[0])

        # 50K suppression check with < 4 turns or any warning must fail
        self.assertFalse(ECR.eval_p0b_verdict(**dict(base, low50k_warn_headers=["", "", ""]))[0])
        self.assertFalse(ECR.eval_p0b_verdict(**dict(base, low50k_warn_headers=["", "", "", "consecutive-zero-cache; streak=3"]))[0])

        # Turn 2 large tool delta misclassified as prefix_only must fail
        bad_misses = list(misses)
        bad_misses[1] = "prefix_only"
        bad2 = dict(base, turns=[{"cacheMiss": m} for m in bad_misses], db_steps=[{"cacheMiss": m} for m in bad_misses])
        self.assertFalse(ECR.eval_p0b_verdict(**bad2)[0])

        # Dashboard/DB parity mismatch must fail
        bad3 = dict(base, db_steps=[{"cacheMiss": "cold_start"}] * 11)
        self.assertFalse(ECR.eval_p0b_verdict(**bad3)[0])

    def test_p0c_interrupt_verdict_pass_and_fail_closed(self):
        base = {
            "orig_sid": "sid-orig",
            "turns": [
                {"sessionId": "sid-orig", "mode": "cold_start"},
                {"sessionId": "sid-orig", "mode": "parked_mcp_resume"},
                {"sessionId": "sid-orig", "mode": "sticky_delta"},
            ],
            "live": {"sessionId": "sid-orig", "compactionRollovers": 0},
            "turn2_text": "TOOL_DONE:ok INTERRUPTED_NEW_QUESTION",
            "turn3_text": "FOLLOWUP_AFTER_INTERRUPT sid=sid-orig resumes=0 relay_follows=0",
            "sid_log_lines": ['[2026-11-18T00:00:00.000Z] [PARKED_MCP_INTERRUPT_RESUME] ok {"sessionId":"sid-orig"}'],
        }
        self.assertTrue(ECR.eval_p0c_interrupt_verdict(**base)[0])
        self.assertTrue(
            ECR.eval_p0c_interrupt_verdict(
                **dict(base, sid_log_lines=['[2026-11-18T00:00:00.000Z] [PARKED_MCP_RESUME] ok {"sessionId":"sid-orig"}'])
            )[0]
        )
        self.assertFalse(ECR.eval_p0c_interrupt_verdict(**dict(base, sid_log_lines=[]))[0])

        # Missing or non-zero resumes on Turn 3 must fail
        self.assertFalse(ECR.eval_p0c_interrupt_verdict(**dict(base, turn3_text="FOLLOWUP_AFTER_INTERRUPT sid=sid-orig resumes=1"))[0])
        self.assertFalse(ECR.eval_p0c_interrupt_verdict(**dict(base, turn3_text="FOLLOWUP_AFTER_INTERRUPT"))[0])

        # Rollover or changed sessionId must fail
        self.assertFalse(ECR.eval_p0c_interrupt_verdict(**dict(base, live={"sessionId": "sid-orig", "compactionRollovers": 1}))[0])
        self.assertFalse(ECR.eval_p0c_interrupt_verdict(**dict(base, sid_log_lines=['[COMPACTION_ROLLOVER] rollover_aborted_tool']))[0])
        bad_turns = [dict(t) for t in base["turns"]]
        bad_turns[1]["sessionId"] = "sid-new"
        self.assertFalse(ECR.eval_p0c_interrupt_verdict(**dict(base, turns=bad_turns))[0])

    def test_p1a_and_lru_verdicts_pass_and_fail_closed(self):
        p1a = {
            "cap_sid": "s-cap",
            "cap_turn2_text": "FALLBACK_MEDIUM_OK",
            "cap_turns": [{"sessionId": "s-cap"}, {"sessionId": "s-cap"}],
            "cap_live": {"compactionRollovers": 0},
            "cap_log_lines": ['[HIGH_TIER_CAPACITY_FALLBACK] ok {"toTier":"medium"}'],
            "park_sid": "s-park",
            "park_turns": [
                {"sessionId": "s-park", "mode": "cold_start", "thoughtLevel": "high"},
                {"sessionId": "s-park", "mode": "parked_mcp_resume", "thoughtLevel": "high"},
                {"sessionId": "s-park", "mode": "sticky_delta", "thoughtLevel": "low"},
            ],
            "park_turn2_text": "TOOL_DONE:FAKE_TOOL_OK",
            "park_turn3_text": "FAKE_OK model=gemini-4-carbon tier=low turn=2",
            "park_live": {"compactionRollovers": 0, "thoughtLevel": "low"},
            "rpc_sid": "s-rpc",
            "rpc_529_status": 529,
            "rpc_recovery_turns": [{"sessionId": "s-rpc", "mode": "cold_start"}, {"sessionId": "s-rpc", "mode": "sticky_delta"}],
            "rpc_live": {"compactionRollovers": 0},
            "rpc_log_lines": [],
            "rpc_400_status": 400,
        }
        self.assertTrue(ECR.eval_p1a_fallback_verdict(**p1a)[0])
        self.assertFalse(ECR.eval_p1a_fallback_verdict(**dict(p1a, rpc_log_lines=['[ACP_HIBERNATE] {"reason":"prompt_rpc_error"}']))[0])
        self.assertFalse(ECR.eval_p1a_fallback_verdict(**dict(p1a, park_turn3_text="FAKE_OK model=gemini-4-carbon tier=high turn=2"))[0])

        lru = {
            "carbon_sid": "s-carbon",
            "carbon_live_after_evict": {"sessionId": "s-carbon"},
            "carbon_followup_turns": [{"sessionId": "s-carbon", "mode": "cold_start"}, {"sessionId": "s-carbon", "mode": "sticky_delta"}],
            "lru_log_lines": ['[ACP_HIBERNATE] evicted {"reason":"lru_cap_8"}'],
        }
        self.assertTrue(ECR.eval_lru_verdict(**lru)[0])
        self.assertFalse(ECR.eval_lru_verdict(**dict(lru, carbon_live_after_evict=None))[0])
        self.assertFalse(ECR.eval_lru_verdict(**dict(lru, carbon_followup_turns=[{"sessionId": "s-carbon", "mode": "cold_start"}, {"sessionId": "s-carbon", "mode": "resumed_delta"}]))[0])

    def test_tier5_double_stripped_verdict_pass_and_fail_closed(self):
        good = [
            {
                "protocol": "Messages",
                "turn4_status": 200,
                "turns": [
                    {"sessionId": "s1", "mode": "cold_start"},
                    {"sessionId": "s2", "mode": "compaction_rollover"},
                    {"sessionId": "s2", "mode": "sticky_delta"},
                ],
                "live": {"compactionRollovers": 1},
            },
            {
                "protocol": "Chat",
                "turn4_status": 200,
                "turns": [
                    {"sessionId": "c1", "mode": "cold_start"},
                    {"sessionId": "c2", "mode": "compaction_rollover"},
                    {"sessionId": "c2", "mode": "sticky_delta"},
                ],
                "live": {"compactionRollovers": 1},
            },
        ]
        self.assertTrue(ECR.eval_tier5_double_stripped_verdict(good)[0])
        self.assertFalse(ECR.eval_tier5_double_stripped_verdict([])[0])
        bad_rot = [dict(good[0]), dict(good[1], turns=[dict(t) for t in good[1]["turns"]])]
        bad_rot[1]["turns"][2]["sessionId"] = "c3"
        self.assertFalse(ECR.eval_tier5_double_stripped_verdict(bad_rot)[0])
        bad_len = [dict(good[0], turns=good[0]["turns"][:2])]
        self.assertFalse(ECR.eval_tier5_double_stripped_verdict(bad_len)[0])


class CacheResilienceAcpWrapperTests(unittest.TestCase):
    def test_medium_to_low_and_session_resume_reset(self):
        state = CRA.WrapperState(mcp_caller=lambda *_a, **_kw: {})
        state.record_gateway_request({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "session/new",
            "params": {"mcpServers": [{"url": "http://127.0.0.1:9999/mcp/t1", "headers": [{"name": "Authorization", "value": "Bearer t1"}]}]},
        })
        state.handle_child_response({"jsonrpc": "2.0", "id": 1, "result": {"sessionId": "acp-s1"}})
        self.assertEqual(state.sessions["acp-s1"]["tier"], "medium")

        # Prompt at medium -> 429 soft error
        emitted1 = []
        state.execute_intercepted_prompt({
            "jsonrpc": "2.0", "id": 2, "method": "session/prompt",
            "params": {"sessionId": "acp-s1", "prompt": [{"type": "text", "text": "[WRAPPER_MEDIUM_429_LOW_OK]"}]},
        }, emitted1.append)
        texts1 = "".join(
            ((m.get("params") or {}).get("update") or {}).get("content", {}).get("text", "")
            for m in emitted1 if m.get("method") == "session/update"
        )
        self.assertIn("RESOURCE_EXHAUSTED (code 429)", texts1)

        # Gateway switches tier -> low
        state.record_gateway_request({
            "jsonrpc": "2.0", "id": 3, "method": "session/set_config_option",
            "params": {"sessionId": "acp-s1", "configId": "thought_level", "value": "low"},
        })
        emitted2 = []
        state.execute_intercepted_prompt({
            "jsonrpc": "2.0", "id": 4, "method": "session/prompt",
            "params": {"sessionId": "acp-s1", "prompt": [{"type": "text", "text": "[WRAPPER_MEDIUM_429_LOW_OK]"}]},
        }, emitted2.append)
        texts2 = "".join(
            ((m.get("params") or {}).get("update") or {}).get("content", {}).get("text", "")
            for m in emitted2 if m.get("method") == "session/update"
        )
        self.assertEqual(texts2, "FALLBACK_LOW_OK")

        # session/resume resets tier back to medium
        state.record_gateway_request({"jsonrpc": "2.0", "id": 5, "method": "session/resume", "params": {"sessionId": "acp-s1"}})
        state.handle_child_response({"jsonrpc": "2.0", "id": 5, "result": {"sessionId": "acp-s1"}})
        self.assertEqual(state.sessions["acp-s1"]["tier"], "medium")

    def test_one_shot_32603_and_tool_relay_framing(self):
        calls = []

        def fake_mcp(url, auth, method, params):
            calls.append((method, params))
            if method == "tools/list":
                return {"tools": [{"name": "ws_Bash_abcdef123456"}, {"name": "ws_ws_await_result_abcdef123456"}]}
            if method == "tools/call":
                name = params["name"]
                if "Bash" in name:
                    return {"content": [{"type": "text", "text": '{"call_id":"call_99"}'}], "isError": False}
                return {"content": [{"type": "text", "text": "RELAY_FINAL_OK"}], "isError": False}
            return {}

        state = CRA.WrapperState(mcp_caller=fake_mcp)
        state.record_gateway_request({
            "jsonrpc": "2.0", "id": 10, "method": "session/new",
            "params": {"mcpServers": [{"url": "http://127.0.0.1:9999/mcp/tok", "headers": [{"name": "Authorization", "value": "Bearer tok"}]}]},
        })
        state.handle_child_response({"jsonrpc": "2.0", "id": 10, "result": {"sessionId": "acp-s2"}})

        # 1st -32603 fails, 2nd succeeds, different key fails once independently
        e_a1, e_a2, e_b1 = [], [], []
        msg_a = {"jsonrpc": "2.0", "id": 11, "method": "session/prompt", "params": {"sessionId": "acp-s2", "prompt": [{"type": "text", "text": "[WRAPPER_TRANSIENT_32603_ONCE:kA]"}]}}
        msg_b = {"jsonrpc": "2.0", "id": 12, "method": "session/prompt", "params": {"sessionId": "acp-s2", "prompt": [{"type": "text", "text": "[WRAPPER_TRANSIENT_32603_ONCE:kB]"}]}}
        state.execute_intercepted_prompt(msg_a, e_a1.append)
        state.execute_intercepted_prompt(msg_a, e_a2.append)
        state.execute_intercepted_prompt(msg_b, e_b1.append)
        self.assertEqual(e_a1[0]["error"]["code"], -32603)
        self.assertEqual(e_a2[-1]["result"]["stopReason"], "end_turn")
        self.assertEqual(e_b1[0]["error"]["code"], -32603)

        # [WRAPPER_TOOL_CALL] + [WRAPPER_FOLLOW_RELAYS]
        e_tool = []
        state.execute_intercepted_prompt({
            "jsonrpc": "2.0", "id": 13, "method": "session/prompt",
            "params": {"sessionId": "acp-s2", "prompt": [{"type": "text", "text": "[WRAPPER_TOOL_CALL] [WRAPPER_FOLLOW_RELAYS]"}]},
        }, e_tool.append)
        self.assertEqual(e_tool[-1]["result"]["stopReason"], "end_turn")
        self.assertEqual(e_tool[0]["params"]["update"]["content"]["text"], "WRAPPER_TOOL_DONE:RELAY_FINAL_OK")
        self.assertEqual([c[0] for c in calls], ["initialize", "tools/list", "tools/call", "tools/call"])

    def test_mcp_http_rpc_real_server_and_cancel_and_resume_mcp_reset(self):
        seen_auth = []

        class McpHandler(BaseHTTPRequestHandler):
            def log_message(self, fmt, *args):
                return

            def do_POST(self):
                length = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(length).decode("utf-8"))
                seen_auth.append(self.headers.get("Authorization"))
                method = body.get("method")
                if method == "tools/list":
                    raw = json.dumps({"jsonrpc": "2.0", "id": 1, "result": {"tools": [{"name": "ws_Bash_abcdef123456"}]}}).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(raw)))
                    self.end_headers()
                    self.wfile.write(raw)
                else:
                    sse = (
                        "event: message\n"
                        f"data: {json.dumps({'jsonrpc': '2.0', 'id': 1, 'result': {'content': [{'type': 'text', 'text': 'SSE_OK'}], 'isError': False}})}\n\n"
                    ).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "text/event-stream")
                    self.send_header("Content-Length", str(len(sse)))
                    self.end_headers()
                    self.wfile.write(sse)

        server = ThreadingHTTPServer(("127.0.0.1", 0), McpHandler)
        port = server.server_address[1]
        th = threading.Thread(target=server.serve_forever, daemon=True)
        th.start()
        try:
            url1 = f"http://127.0.0.1:{port}/mcp/t1"
            res_json = CRA.mcp_http_rpc(url1, "Bearer tok1", "tools/list", {}, timeout=5)
            self.assertEqual(res_json["tools"][0]["name"], "ws_Bash_abcdef123456")
            res_sse = CRA.mcp_http_rpc(url1, "Bearer tok1", "tools/call", {"name": "ws_Bash_abcdef123456"}, timeout=5)
            self.assertEqual(res_sse["content"][0]["text"], "SSE_OK")
            self.assertEqual(seen_auth, ["Bearer tok1", "Bearer tok1"])
        finally:
            server.shutdown()
            server.server_close()
            th.join(timeout=5)
        gc.collect()

        # session/resume with explicit mcpServers updates url/auth and clears stale mcp_tools;
        # session/cancel aborts [WRAPPER_FOLLOW_RELAYS] loop cleanly.
        entered_relay = threading.Event()
        cancel_done = threading.Event()

        def relay_mcp(url, auth, method, params):
            if method == "initialize":
                return {}
            if method == "tools/list":
                return {"tools": [{"name": "ws_Bash_abcdef123456"}, {"name": "ws_ws_await_result_abcdef123456"}]}
            if method == "tools/call" and "Bash" in params["name"]:
                entered_relay.set()
                cancel_done.wait(timeout=2.0)
                return {"content": [{"type": "text", "text": '{"call_id":"call_abort"}'}], "isError": False}
            raise AssertionError(f"unexpected mcp call after cancel: {method} {params}")

        st = CRA.WrapperState(mcp_caller=relay_mcp)
        st.record_gateway_request({"jsonrpc": "2.0", "id": 1, "method": "session/new", "params": {"mcpServers": [{"url": "http://127.0.0.1:1111/mcp/a", "headers": [{"name": "Authorization", "value": "Bearer a"}]}]}})
        st.handle_child_response({"jsonrpc": "2.0", "id": 1, "result": {"sessionId": "acp-c1"}})
        st.sessions["acp-c1"]["mcp_tools"] = {"Bash": "stale_name"}

        st.record_gateway_request({"jsonrpc": "2.0", "id": 2, "method": "session/resume", "params": {"sessionId": "acp-c1", "mcpServers": [{"url": "http://127.0.0.1:2222/mcp/b", "headers": [{"name": "Authorization", "value": "Bearer b"}]}]}})
        st.handle_child_response({"jsonrpc": "2.0", "id": 2, "result": {"sessionId": "acp-c1"}})
        self.assertEqual(st.sessions["acp-c1"]["url"], "http://127.0.0.1:2222/mcp/b")
        self.assertEqual(st.sessions["acp-c1"]["auth"], "Bearer b")
        self.assertEqual(st.sessions["acp-c1"]["mcp_tools"], {})

        emitted = []

        def cancel_once_ready():
            if entered_relay.wait(timeout=2.0):
                st.record_gateway_request({"jsonrpc": "2.0", "method": "session/cancel", "params": {"sessionId": "acp-c1"}})
                cancel_done.set()

        c_th = threading.Thread(target=cancel_once_ready, daemon=True)
        c_th.start()
        st.execute_intercepted_prompt({
            "jsonrpc": "2.0", "id": 3, "method": "session/prompt",
            "params": {"sessionId": "acp-c1", "prompt": [{"type": "text", "text": "[WRAPPER_TOOL_CALL] [WRAPPER_FOLLOW_RELAYS]"}]},
        }, emitted.append)
        c_th.join(timeout=2.0)
        self.assertTrue(cancel_done.is_set())
        self.assertEqual(emitted[-1]["result"]["stopReason"], "cancelled")

    def test_slow_cancel_once_waits_for_cancel_then_recovers(self):
        st = CRA.WrapperState(mcp_caller=lambda *_a, **_kw: {})
        st.record_gateway_request({"jsonrpc": "2.0", "id": 1, "method": "session/new", "params": {}})
        st.handle_child_response({"jsonrpc": "2.0", "id": 1, "result": {"sessionId": "acp-sc1"}})

        emitted1 = []
        prompt1 = {
            "jsonrpc": "2.0", "id": 2, "method": "session/prompt",
            "params": {"sessionId": "acp-sc1", "prompt": [{"type": "text", "text": "[WRAPPER_SLOW_CANCEL_ONCE:k1]"}]},
        }
        self.assertTrue(st.should_intercept_prompt(prompt1))

        def send_cancel():
            time.sleep(0.05)
            st.record_gateway_request({"jsonrpc": "2.0", "method": "session/cancel", "params": {"sessionId": "acp-sc1"}})

        c_th = threading.Thread(target=send_cancel, daemon=True)
        c_th.start()
        t0 = time.monotonic()
        st.execute_intercepted_prompt(prompt1, emitted1.append)
        elapsed = time.monotonic() - t0
        c_th.join(timeout=2.0)
        self.assertGreaterEqual(elapsed, 0.25)
        self.assertEqual(emitted1[-1]["result"]["stopReason"], "cancelled")
        self.assertFalse(st.should_intercept_prompt(prompt1))

        emitted2 = []
        st.execute_intercepted_prompt(prompt1, emitted2.append)
        self.assertEqual(emitted2[-1]["result"]["stopReason"], "end_turn")
        self.assertEqual(emitted2[0]["params"]["update"]["content"]["text"], "SLOW_CANCEL_RECOVERY_OK")

        # session/cancel is consumed when intercept_active > 0 and forwarded when 0
        self.assertFalse(st.record_gateway_request({"jsonrpc": "2.0", "method": "session/cancel", "params": {"sessionId": "acp-sc1"}}))
        st.begin_intercept(prompt1)
        self.assertTrue(st.record_gateway_request({"jsonrpc": "2.0", "method": "session/cancel", "params": {"sessionId": "acp-sc1"}}))
        st.end_intercept(prompt1)
        self.assertFalse(st.record_gateway_request({"jsonrpc": "2.0", "method": "session/cancel", "params": {"sessionId": "acp-sc1"}}))


if __name__ == "__main__":
    unittest.main()
