import fcntl
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parent))
import agy_upgrade as U  # noqa: E402
import e2e_common as E  # noqa: E402
import fake_instance as F  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
GW_SH = REPO_ROOT / "scripts" / "gw.sh"


def _free_port_pair(preferred=18894):
    candidates = [preferred, *range(28960, 29400, 2)]
    for base in candidates:
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
    raise RuntimeError("no free local port pair available for gw.sh test")


def _http_get(port, path, headers=None):
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", headers=headers or {})
    try:
        resp = urllib.request.urlopen(req, timeout=5)
    except urllib.error.HTTPError as err:
        resp = err
    with resp:
        return resp.status, resp.read().decode("utf-8", "replace")


class GwShCrossPlatformTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory(prefix="gw-sh-suite-")
        cls.stage = Path(cls._tmp.name).resolve() / "stage with space"
        F.build(cls.stage)
        cls.gw_bin = cls.stage / U.FILES["gateway"]
        cls.acp_bin = cls.stage / F.FAKEACP

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def _isolated_env(self, root_dir, port, extra=None):
        env = U.instance_env(root_dir, f"gwsh-{port}", self.acp_bin, port)
        env["AGY_GATEWAY_BIN"] = str(self.gw_bin)
        env["FAKE_ACP_WRITE_DB"] = "1"
        env["FAKE_ACP_EMIT_USAGE_UPDATE"] = "1"
        env["AGY_CONV_DIR"] = str(Path(env["AGY_PURE_API_HOME"]) / "antigravity-cli" / "conversations")
        Path(env["AGY_CONV_DIR"]).mkdir(parents=True, exist_ok=True, mode=0o700)
        if extra:
            env.update(extra)
        return env

    def _run_gw(self, *args, env, script=GW_SH, timeout=30):
        return subprocess.run(
            ["bash", str(script), *args],
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout,
        )

    def test_bash_syntax_valid(self):
        res = subprocess.run(["bash", "-n", str(GW_SH)], capture_output=True, text=True, timeout=10)
        self.assertEqual(res.returncode, 0, res.stderr)

    def test_invalid_ports_and_unsupported_mode_fail_fast(self):
        with tempfile.TemporaryDirectory(prefix="gw-sh-invalid-") as tmp:
            base_dir = Path(tmp).resolve()
            env = self._isolated_env(base_dir, 18896)
            for bad_env in (
                {"AGY_GW_PORT": "18896", "AGY_DASH_PORT": "18896"},
                {"AGY_GW_PORT": "0", "AGY_DASH_PORT": "18897"},
                {"AGY_GW_PORT": "70000", "AGY_DASH_PORT": "18897"},
                {"AGY_GW_PORT": "abc", "AGY_DASH_PORT": "18897"},
            ):
                with self.subTest(bad_env=bad_env):
                    res = self._run_gw("status", env={**env, **bad_env})
                    self.assertEqual(res.returncode, 1)
                    self.assertIn("Invalid gateway/dashboard ports", res.stderr)

            bad_mode = self._run_gw("start", "node", env=env)
            self.assertEqual(bad_mode.returncode, 1)
            self.assertIn("Unsupported mode", bad_mode.stderr)

    def test_full_lifecycle_start_status_restart_stop_and_fd9_release(self):
        port = _free_port_pair(18894)
        with tempfile.TemporaryDirectory(prefix="gw-sh-life-") as tmp:
            root_dir = Path(tmp).resolve() / "instance dir"
            env = self._isolated_env(root_dir, port)
            run_dir = Path(env["AGY_RUN_DIR"])

            pre = self._run_gw("status", env=env)
            self.assertNotEqual(pre.returncode, 0)
            self.assertIn(f"Port {port}: stopped", pre.stdout)
            self.assertIn(f"Port {port + 1}: stopped", pre.stdout)

            started = False
            try:
                st = self._run_gw("start", "go", env=env)
                self.assertEqual(st.returncode, 0, st.stderr)
                started = True
                self.assertIn(f"Port {port}: listening", st.stdout)
                self.assertIn(f"Port {port + 1}: listening", st.stdout)

                pid1 = int((run_dir / "gateway.pid").read_text().strip())
                start1 = (run_dir / "gateway.start").read_text().strip()
                self.assertTrue(start1)
                self.assertEqual((run_dir / "mode").read_text().strip(), "go")
                self.assertEqual(os.getsid(pid1), pid1)

                # Prove FD 9 (control.lock) was NOT leaked into the backgrounded gateway daemon.
                with (run_dir / "control.lock").open("a") as lock_f:
                    fcntl.flock(lock_f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    fcntl.flock(lock_f.fileno(), fcntl.LOCK_UN)

                # Spawn a live fakeacp child worker so restart/stop also exercises descendant cleanup.
                code, _, body = E.post(port, "/v1/messages", {
                    "model": "gemini-4-carbon",
                    "max_tokens": 64,
                    "system": E.SYS,
                    "messages": [{"role": "user", "content": "Reply OK"}],
                }, "gw-sh-descendant-check")
                self.assertEqual(code, 200, body)

                dash_code, dash_raw = _http_get(port + 1, "/api/dashboard?refresh=1")
                self.assertEqual(dash_code, 200)
                dash = json.loads(dash_raw)
                self.assertGreaterEqual(len(dash.get("liveSessions") or []), 1)

                status_res = self._run_gw("status", env=env)
                self.assertEqual(status_res.returncode, 0, status_res.stderr)
                self.assertIn('{"ok":true}', status_res.stdout)

                # Occupied port check if another start is attempted while running
                dup_start = self._run_gw("start", "go", env=env)
                self.assertEqual(dup_start.returncode, 1)
                self.assertIn("Instance port already occupied", dup_start.stderr)

                # Restart must cleanly terminate pid1 + fakeacp descendants and start a fresh pid2
                rs = self._run_gw("restart", env=env)
                self.assertEqual(rs.returncode, 0, rs.stderr)
                pid2 = int((run_dir / "gateway.pid").read_text().strip())
                self.assertNotEqual(pid1, pid2)
                self.assertIsNone(U.process_identity(pid1))
                self.assertEqual(int((run_dir / "gateway.last.pid").read_text().strip()), pid1)
                self.assertEqual((run_dir / "gateway.last.start").read_text().strip(), start1)

                hz_code, _ = _http_get(port, "/healthz")
                ui_code, ui_html = _http_get(port + 1, "/")
                self.assertEqual(hz_code, 200)
                self.assertEqual(ui_code, 200)
                self.assertIn("<html", ui_html.lower())

                sp = self._run_gw("stop", env=env)
                self.assertEqual(sp.returncode, 0, sp.stderr)
                started = False
                self.assertIn(f"Stopped instance {port}", sp.stdout)
                self.assertIsNone(U.process_identity(pid2))
                self.assertFalse(F._port_listening(port))
                self.assertFalse(F._port_listening(port + 1))

                # Idempotent second stop
                sp2 = self._run_gw("stop", env=env)
                self.assertEqual(sp2.returncode, 0, sp2.stderr)
            finally:
                if started:
                    self._run_gw("stop", env=env)

    def test_concurrent_control_lock_contention_and_no_linux_cli_fallback(self):
        port = _free_port_pair(18896)
        with tempfile.TemporaryDirectory(prefix="gw-sh-lock-") as tmp:
            base_dir = Path(tmp).resolve()
            env = self._isolated_env(base_dir, port)
            run_dir = Path(env["AGY_RUN_DIR"])

            with (run_dir / "control.lock").open("a") as lock_f:
                fcntl.flock(lock_f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                for action in ("start", "stop", "restart"):
                    with self.subTest(action=action):
                        res = self._run_gw(action, env=env)
                        self.assertEqual(res.returncode, 1)
                        self.assertIn("Instance operation already running", res.stderr)

            # Exercise the no-ss / no-setsid / no-flock fallback path even on hosts that have them.
            shim_bin = base_dir / "shim-bin"
            shim_bin.mkdir(mode=0o700)
            needed = ("awk", "bash", "cat", "curl", "dirname", "kill", "lsof", "mkdir", "mv", "nohup", "printf", "ps", "python3", "readlink", "rm", "sed", "sleep", "tail", "uname")
            for tool in needed:
                resolved = shutil.which(tool)
                if resolved:
                    (shim_bin / tool).symlink_to(resolved)

            fallback_env = dict(env, PATH=str(shim_bin))
            started = False
            try:
                st = self._run_gw("start", "go", env=fallback_env)
                self.assertEqual(st.returncode, 0, st.stderr)
                started = True
                pid = int((run_dir / "gateway.pid").read_text().strip())
                self.assertEqual(os.getsid(pid), pid)

                # Confirm _AGY_GW_FLOCKED was scrubbed before spawning the daemon; nested stop/restart still respects lock
                with (run_dir / "control.lock").open("a") as lock_f:
                    fcntl.flock(lock_f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    blocked = self._run_gw("stop", env=fallback_env)
                    self.assertEqual(blocked.returncode, 1)
                    self.assertIn("Instance operation already running", blocked.stderr)

                sp = self._run_gw("stop", env=fallback_env)
                self.assertEqual(sp.returncode, 0, sp.stderr)
                started = False
                self.assertIsNone(U.process_identity(pid))
            finally:
                if started:
                    self._run_gw("stop", env=fallback_env)

    def test_pid_ownership_reuse_refusal_and_legacy_pid_fallback(self):
        port = _free_port_pair(18898)
        with tempfile.TemporaryDirectory(prefix="gw-sh-pid-") as tmp:
            base_dir = Path(tmp).resolve()
            symlink_dir = base_dir / "bin-symlink"
            symlink_dir.symlink_to(self.stage)
            symlink_gw = symlink_dir / U.FILES["gateway"]

            env = self._isolated_env(base_dir, port, {"AGY_GATEWAY_BIN": str(symlink_gw)})
            run_dir = Path(env["AGY_RUN_DIR"])

            sleeper = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
            try:
                # 1. Mismatched .start timestamp -> refuses signal (exit 2)
                (run_dir / "gateway.pid").write_text(f"{sleeper.pid}\n")
                (run_dir / "gateway.start").write_text("STALE_MISMATCHED_START_STAMP\n")
                res_reused = self._run_gw("stop", env=env)
                self.assertEqual(res_reused.returncode, 2)
                self.assertIn("PID was reused; refusing signal", res_reused.stderr)
                self.assertIsNone(sleeper.poll())

                # 2. Legacy .pid without .start pointing to foreign executable -> refuses signal (exit 2)
                (run_dir / "gateway.start").unlink()
                res_foreign = self._run_gw("stop", env=env)
                self.assertEqual(res_foreign.returncode, 2)
                self.assertIn("Unverified legacy PID; refusing signal", res_foreign.stderr)
                self.assertIsNone(sleeper.poll())
            finally:
                sleeper.terminate()
                sleeper.wait(timeout=5)

            (run_dir / "gateway.pid").unlink(missing_ok=True)

            # 3. Legacy .pid without .start pointing to actual AGY_GATEWAY_BIN (via symlink) -> verified & stopped cleanly
            started = False
            try:
                st = self._run_gw("start", "go", env=env)
                self.assertEqual(st.returncode, 0, st.stderr)
                started = True
                gw_pid = int((run_dir / "gateway.pid").read_text().strip())
                (run_dir / "gateway.start").unlink()

                st_legacy = self._run_gw("status", env=env)
                self.assertEqual(st_legacy.returncode, 0, st_legacy.stderr)

                sp_legacy = self._run_gw("stop", env=env)
                self.assertEqual(sp_legacy.returncode, 0, sp_legacy.stderr)
                started = False
                self.assertIsNone(U.process_identity(gw_pid))
            finally:
                if started:
                    self._run_gw("stop", env=env)

    def test_dotenv_loading_precedence_and_isolated_instance_protection(self):
        port = _free_port_pair(18902)
        with tempfile.TemporaryDirectory(prefix="gw-sh-dotenv-") as tmp:
            base_dir = Path(tmp).resolve()
            fake_root = base_dir / "fake-root"
            (fake_root / "scripts").mkdir(parents=True, mode=0o700)
            copied_gw_sh = fake_root / "scripts" / "gw.sh"
            shutil.copy2(GW_SH, copied_gw_sh)
            copied_gw_sh.chmod(0o755)

            # Synthetic root .env that must NOT leak into isolated non-default port runs
            (fake_root / ".env").write_text("AGY_API_KEYS=root-env-should-not-leak\n")
            iso_env = self._isolated_env(base_dir, port)
            iso_env.pop("AGY_API_KEYS", None)

            started = False
            try:
                st = self._run_gw("start", "go", env=iso_env, script=copied_gw_sh)
                self.assertEqual(st.returncode, 0, st.stderr)
                started = True
                # Isolated port ignored fake_root/.env, so /v1/models is unauthenticated (200)
                models_code, _ = _http_get(port, "/v1/models")
                self.assertEqual(models_code, 200)
                self._run_gw("stop", env=iso_env, script=copied_gw_sh)
                started = False

                # Explicit AGY_ENV_FILE loads key=value pairs without overriding caller-supplied env vars
                custom_env_file = base_dir / "custom.env"
                custom_env_file.write_text(
                    "# synthetic test dotenv\n"
                    "export AGY_API_KEYS='dotenv-key-alpha'\n"
                    'AGY_GW_PORT="19999"\n'
                    "AGY_UPSTREAM_PROXY=\n"
                )
                dotenv_env = dict(iso_env, AGY_ENV_FILE=str(custom_env_file))
                dotenv_env.pop("AGY_UPSTREAM_PROXY", None)
                st2 = self._run_gw("start", "go", env=dotenv_env, script=copied_gw_sh)
                self.assertEqual(st2.returncode, 0, st2.stderr)
                started = True

                # Caller's AGY_GW_PORT won over custom.env's 19999, while AGY_API_KEYS loaded from custom.env
                hz_code, _ = _http_get(port, "/healthz")
                unauth_code, _ = _http_get(port, "/v1/models")
                auth_code, _ = _http_get(port, "/v1/models", headers={"x-api-key": "dotenv-key-alpha"})
                self.assertEqual(hz_code, 200)
                self.assertEqual(unauth_code, 401)
                self.assertEqual(auth_code, 200)

                sp2 = self._run_gw("stop", env=dotenv_env, script=copied_gw_sh)
                self.assertEqual(sp2.returncode, 0, sp2.stderr)
                started = False
            finally:
                if started:
                    self._run_gw("stop", env=iso_env, script=copied_gw_sh)


if __name__ == "__main__":
    unittest.main()
