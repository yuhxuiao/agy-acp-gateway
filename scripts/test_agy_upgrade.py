import contextlib
import copy
import json
import os
import http.server
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

import agy_upgrade as upgrade
import agy_upgrade_e2e as probe
import upstream_watch as watch
SOURCE_DIGEST = upgrade.source_digest


class UpgradeGateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="agy-upgrade-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.stage = self.root/"candidate"
        self.stage.mkdir()
        self.manifest = {"schema": 2, "source_digest": "source-1", "artifacts": {}, "checks": {}}
        for key, name in upgrade.FILES.items():
            p = self.stage/name
            p.write_bytes(("candidate-"+key).encode())
            self.manifest["artifacts"][key] = upgrade.sha(p)
        for name in ("offline", "fake_e2e", "contract", "live_flash"):
            result = {"ok": True, "model": probe.MODEL, "generation_requests": 1 if name == "live_flash" else 0}
            p = self.stage/(name+".json")
            upgrade.write_json(p, result)
            self.manifest["checks"][name] = {"passed": True, "file": p.name, "sha256": upgrade.sha(p)}
        self.save()
        self.digest = patch.object(upgrade, "source_digest", return_value="source-1")
        self.digest.start()
        self.addCleanup(self.digest.stop)

    def save(self):
        upgrade.write_json(self.stage/"manifest.json", self.manifest)

    def test_accepts_bound_successful_bundle(self):
        self.assertEqual(upgrade.validate_manifest(self.stage)["schema"], 2)

    def test_rejects_missing_skipped_or_string_success(self):
        for name in self.manifest["checks"]:
            for value in (False, "true", "skipped", None):
                with self.subTest(check=name, value=value):
                    self.manifest["checks"][name]["passed"] = value
                    self.save()
                    with self.assertRaises(ValueError):
                        upgrade.validate_manifest(self.stage)
            self.manifest["checks"][name]["passed"] = True

    def test_rejects_changed_source_artifact_or_evidence(self):
        for kind in ("source", "artifact", "evidence"):
            with self.subTest(kind=kind):
                saved = copy.deepcopy(self.manifest)
                if kind == "source":
                    self.manifest["source_digest"] = "new-source"
                elif kind == "artifact":
                    self.manifest["artifacts"]["gateway"] = "0"*64
                else:
                    self.manifest["checks"]["contract"]["sha256"] = "0"*64
                self.save()
                with self.assertRaises(ValueError):
                    upgrade.validate_manifest(self.stage)
                self.manifest = saved

    def test_bad_model_or_failed_result_cannot_be_rehashed_into_pass(self):
        for result in ({"ok": True, "model": "gemini-4-argon", "generation_requests": 1},
                       {"ok": False, "model": probe.MODEL, "generation_requests": 1},
                       {"ok": True, "model": probe.MODEL, "generation_requests": 2}):
            p = self.stage/"live_flash.json"
            upgrade.write_json(p, result)
            self.manifest["checks"]["live_flash"]["sha256"] = upgrade.sha(p)
            self.save()
            with self.assertRaises(ValueError):
                upgrade.validate_manifest(self.stage)

    def test_isolation_overrides_inherited_production_paths(self):
        with patch.dict(os.environ, {"AGY_STATE_DB": "/production.db", "AGY_ACP_RECORD": "/production-log", "AGY_API_KEYS": "synthetic-secret", "AGY_APPDATA_LINK": "/production-home", "FAKE_ACP_CAPABILITIES": "{}"}):
            env = upgrade.instance_env(self.stage, "fake", self.stage/"fakeacp")
        self.assertTrue(env["AGY_STATE_DB"].startswith(str(self.stage)))
        self.assertTrue(env["AGY_TOS_TRIPPED_FILE"].startswith(str(self.stage)))
        self.assertTrue(env["AGY_PURE_API_HOME"].startswith(str(self.stage)))
        self.assertEqual(env["AGY_API_KEYS"], "")
        self.assertEqual(env["AGY_APPDATA_LINK"], "")
        self.assertEqual(env["AGY_ACP_RECORD"], "")
        self.assertNotIn("FAKE_ACP_CAPABILITIES", env)
        for port in (18889, 18890, 18891):
            with self.assertRaises(ValueError):
                upgrade.instance_env(self.stage, "fake", self.stage/"fakeacp", port)

    def test_concurrent_mutation_is_rejected(self):
        with upgrade.upgrade_lock(self.root/"locks"):
            with self.assertRaises(RuntimeError):
                with upgrade.upgrade_lock(self.root/"locks"):
                    self.fail("second lock acquired")
        with upgrade.upgrade_lock(self.root/"locks"):
            pass

    def test_c_drive_symlinks_are_rejected(self):
        link = self.root/"linked"
        link.symlink_to("/mnt/c")
        with self.assertRaises(ValueError):
            upgrade.checked_path(link/"candidate")

    def test_failed_install_restores_both_binaries_and_manifest(self):
        (self.root/"bin").mkdir()
        expected = {}
        for name in [*upgrade.FILES.values(), "agy-acp-900k.manifest.json"]:
            expected[name] = ("old-"+name).encode()
            (self.root/"bin"/name).write_bytes(expected[name])
        with patch.object(upgrade, "ROOT", self.root), patch.object(upgrade, "STATE", self.root/"state"), \
             patch.object(upgrade, "run"), patch.object(upgrade, "probe_metadata"), \
             patch.object(probe, "run_probe", return_value={"ok": False}):
            with self.assertRaisesRegex(RuntimeError, "previous bundle restored"):
                upgrade.install(self.stage)
        for name, original in expected.items():
            self.assertEqual((self.root/"bin"/name).read_bytes(), original)
        self.assertTrue((self.root/"state/last-backup.json").exists())

    def test_unverified_install_does_not_stop_running_service(self):
        self.manifest["checks"]["contract"]["passed"] = False
        self.save()
        with patch.object(upgrade, "run") as execute:
            with self.assertRaises(ValueError):
                upgrade.install(self.stage)
            execute.assert_not_called()

    def test_fake_probe_cannot_silently_call_live_model(self):
        with patch("urllib.request.urlopen") as request:
            with self.assertRaises(ValueError):
                probe.run_probe(18890)
            request.assert_not_called()

    def test_asset_requires_digest_and_official_source(self):
        release = {"tag_name": "1.5.3", "assets": [{"name": "agy_cli_linux_x64.tar.gz", "digest": "", "browser_download_url": "https://example.com/agy"}]}
        with patch("platform.machine", return_value="x86_64"), patch("platform.system", return_value="Linux"):
            with self.assertRaises(ValueError):
                watch.select_asset(release)

    def test_embedded_assets_invalidate_acceptance(self):
        for name in ("go-gateway/go.mod", "go-gateway/go.sum", "third_party/agy-context-unlock/agy-patch.py", "go-gateway/webui/dashboard.html"):
            p = self.root/name
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text("original")
        before = SOURCE_DIGEST(self.root)
        (self.root/"go-gateway/webui/dashboard.html").write_text("changed")
        self.assertNotEqual(before, SOURCE_DIGEST(self.root))

    def test_metadata_probe_uses_existing_environment_auth(self):
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                ok = self.headers.get("x-api-key") == "synthetic-key"
                self.send_response(200 if ok else 401)
                self.end_headers()
                self.wfile.write(json.dumps({"data": [{"id": "gemini-3.8-flash-low"}]}).encode())

            def log_message(self, *_):
                pass
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with patch.dict(os.environ, {"AGY_API_KEYS": "synthetic-key,synthetic-second", "AGY_KEY": ""}):
                upgrade.probe_metadata(server.server_address[1])
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def test_owned_child_ignoring_term_is_killed(self):
        child_script = "import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); print('ready',flush=True); time.sleep(60)"
        parent_script = "import subprocess,sys,time; p=subprocess.Popen([sys.executable,'-c',sys.argv[1]],stdout=subprocess.PIPE,text=True); p.stdout.readline(); print(p.pid,flush=True); time.sleep(60)"
        parent = subprocess.Popen([sys.executable, "-c", parent_script, child_script], stdout=subprocess.PIPE, text=True)
        child = int(parent.stdout.readline())
        try:
            upgrade.stop_owned_process(parent)
            for _ in range(20):
                path = Path(f"/proc/{child}/stat")
                if not path.exists() or path.read_text().rsplit(")", 1)[1].split()[0] == "Z":
                    break
                time.sleep(0.05)
            else:
                self.fail("owned descendant survived cleanup")
        finally:
            parent.stdout.close()
            try:
                os.kill(child, signal.SIGKILL)
            except ProcessLookupError:
                pass


if __name__ == "__main__":
    unittest.main()
