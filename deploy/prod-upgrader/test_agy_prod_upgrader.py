"""Offline tests for the production update agent (no docker, no network, no generation)."""
import datetime as dt
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import tarfile
import tempfile
import types
import unittest
from unittest import mock

HERE = Path(__file__).resolve().parent


def load(root):
    os.environ["AGY_PROD_ROOT"] = str(root)
    spec = importlib.util.spec_from_file_location("agy_prod_upgrader_t", HERE / "agy_prod_upgrader.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    m.STATE = root / "upgrader-state"
    return m


class UpgraderTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        (self.root / "data").mkdir()
        self.m = load(self.root)

    def tearDown(self):
        self.tmp.cleanup()

    def approve(self, cid, sha, age=dt.timedelta(0)):
        d = self.m.PUBLIC / "approvals"
        d.mkdir(parents=True, exist_ok=True)
        at = (dt.datetime.now(dt.timezone.utc) - age).isoformat()
        (d / f"{cid}.json").write_text(json.dumps({"candidateId": cid, "sha256": sha, "approvedAt": at}))

    def test_approval_must_match_and_be_fresh(self):
        cand = {"id": "agy-1.5.6-x", "sha256": "a" * 64}
        self.approve("agy-1.5.6-x", "b" * 64)
        self.assertIsNone(self.m.take_approval(cand))
        self.approve("agy-1.5.6-x", "a" * 64, age=dt.timedelta(hours=30))
        self.assertIsNone(self.m.take_approval(cand))
        self.approve("agy-1.5.6-x", "a" * 64)
        self.assertEqual(self.m.take_approval(cand)["candidateId"], "agy-1.5.6-x")
        self.assertEqual(list((self.m.PUBLIC / "approvals").glob("*.json")), [])  # consumed

    def test_apply_ignores_approval_for_unready_candidate(self):
        st = {"candidate": {"id": "c1", "kind": "agy", "version": "1.5.6", "sha256": "a" * 64, "state": "failed"}, "history": []}
        self.m.save_state(st)
        self.approve("c1", "a" * 64)
        with mock.patch.object(self.m, "apply_agy") as ap:
            self.m.cmd_apply(types.SimpleNamespace())
        ap.assert_not_called()
        self.assertEqual(list((self.m.PUBLIC / "approvals").glob("*.json")), [])

    def test_published_status_is_not_trusted(self):
        st = {"candidate": {"id": "c1", "kind": "agy", "version": "1.5.6", "sha256": "a" * 64, "state": "failed"}, "history": []}
        self.m.save_state(st)
        forged = json.loads((self.m.PUBLIC / "status.json").read_text())
        forged["candidate"]["state"] = "ready"
        (self.m.PUBLIC / "status.json").write_text(json.dumps(forged))
        self.approve("c1", "a" * 64)
        with mock.patch.object(self.m, "apply_agy") as ap:
            self.m.cmd_apply(types.SimpleNamespace())
        ap.assert_not_called()

    def test_symlink_switch_and_rollback(self):
        agy = self.m.AGY_DIR
        for v in ("1.5.5-aaaa", "1.5.6-bbbb"):
            (agy / v).mkdir(parents=True)
            (agy / v / "agy-acp-900k").write_text(v)
        self.m.switch_current(agy / "1.5.5-aaaa")
        self.assertEqual(os.readlink(agy / "current"), "1.5.5-aaaa")  # relative: valid inside the container
        stage = self.root / "stage"
        stage.mkdir()
        (stage / "agy-acp-900k").write_text("new")
        sha = self.m.sha256(stage / "agy-acp-900k")
        cand = {"id": "c2", "kind": "agy", "version": "1.5.6", "sha256": sha, "state": "approved", "stage": str(stage)}
        st = {"candidate": cand, "history": []}
        engine = {"alive": True, "running": "/data/agy/1.5.5-aaaa/agy-acp-900k"}
        with mock.patch.object(self.m, "gateway_key", return_value="k"), \
                mock.patch.object(self.m, "engine_info", side_effect=lambda k: engine), \
                mock.patch.object(self.m, "flash_probe", return_value=(False, "500")), \
                mock.patch.object(self.m.time, "sleep"):
            # engine busy past the wait: keep the new link, finish on the next check
            self.assertTrue(self.m.finish_agy(st, dict(cand, installed="1.5.6-" + sha[:8], previous="1.5.5-aaaa"), wait=0))
            self.m.APPLY_WAIT_SEC = 0
            ok = self.m.apply_agy(st, cand)
            self.assertTrue(ok)
            self.assertEqual(cand["state"], "waiting_idle")
            self.assertEqual(os.readlink(agy / "current"), "1.5.6-" + sha[:8])
            engine.update(running=f"/data/agy/1.5.6-{sha[:8]}/agy-acp-900k")
            self.assertFalse(self.m.finish_agy(st, cand, wait=0))  # probe fails on the new binary
        self.assertEqual(cand["state"], "rolled_back")
        self.assertEqual(os.readlink(agy / "current"), "1.5.5-aaaa")

    def test_successful_switch(self):
        agy = self.m.AGY_DIR
        (agy / "1.5.5-aaaa").mkdir(parents=True)
        (agy / "1.5.5-aaaa" / "agy-acp-900k").write_text("old")
        self.m.switch_current(agy / "1.5.5-aaaa")
        stage = self.root / "stage"
        stage.mkdir()
        (stage / "agy-acp-900k").write_text("new")
        sha = self.m.sha256(stage / "agy-acp-900k")
        cand = {"id": "c3", "kind": "agy", "version": "1.5.6", "sha256": sha, "state": "approved", "stage": str(stage)}
        calls = []

        def eng(_):
            calls.append(1)
            return {"alive": len(calls) < 2, "running": "/data/agy/1.5.5-aaaa/agy-acp-900k"} if len(calls) < 3 else \
                {"alive": True, "running": f"/data/agy/1.5.6-{sha[:8]}/agy-acp-900k"}
        with mock.patch.object(self.m, "gateway_key", return_value="k"), \
                mock.patch.object(self.m, "engine_info", side_effect=eng), \
                mock.patch.object(self.m, "flash_probe", return_value=(True, "200 end_turn")), \
                mock.patch.object(self.m.time, "sleep"):
            self.assertTrue(self.m.apply_agy({"candidate": cand}, cand))
        self.assertEqual(cand["state"], "applied")

    def test_gateway_bundle_requires_matching_manifest_and_gates(self):
        rel = self.root / "releases/20261112-x"
        (rel / "bin").mkdir(parents=True)
        (rel / "bin/agy-acp-gateway").write_text("gw")
        (rel / "release.sh").write_text("exit 0")
        (rel / "READY").write_text("")
        sha = self.m.sha256(rel / "bin/agy-acp-gateway")
        (rel / "manifest.json").write_text(json.dumps({"artifacts": {"gateway": "0" * 64}, "checks": {}}))
        self.assertFalse(self.m.gateway_bundle()[2])
        (rel / "manifest.json").write_text(json.dumps({"artifacts": {"gateway": sha}, "checks": {"offline": {"passed": True}}}))
        self.assertFalse(self.m.gateway_bundle()[2])
        checks = {k: {"passed": True} for k in ("offline", "fake_e2e", "contract", "live_flash")}
        (rel / "manifest.json").write_text(json.dumps({"artifacts": {"gateway": sha}, "checks": checks}))
        self.assertTrue(self.m.gateway_bundle()[2])
        (rel / "result").write_text("DEPLOYED_AND_VERIFIED")
        self.assertIsNone(self.m.gateway_bundle())

    def _build_tar_bytes(self, ver="1.5.8"):
        buf = io.BytesIO()
        script = f"#!/bin/sh\necho {ver}\n".encode()
        with tarfile.open(fileobj=buf, mode="w:gz") as tar:
            info = tarfile.TarInfo("agy")
            info.size = len(script)
            info.mode = 0o755
            tar.addfile(info, io.BytesIO(script))
        return buf.getvalue()

    def test_latest_release_manifest_fallback_and_newer_selection(self):
        mf = {
            "source": "manifest",
            "tag_name": "1.5.8",
            "url": self.m.GCS_PREFIX + "1.5.8/linux_amd64/agy.tar.gz",
            "sha512": "a" * 128,
        }
        with mock.patch.object(self.m, "fetch_github_release", side_effect=RuntimeError("403 rate limit")), \
                mock.patch.object(self.m, "fetch_manifest_release", return_value=mf):
            rel = self.m.latest_release()
        self.assertEqual(rel["source"], "manifest")
        self.assertEqual(rel["tag_name"], "1.5.8")

        gh_old = {"tag_name": "1.5.6", "assets": []}
        with mock.patch.object(self.m, "fetch_github_release", return_value=gh_old), \
                mock.patch.object(self.m, "fetch_manifest_release", return_value=mf):
            rel2 = self.m.latest_release()
        self.assertEqual(rel2["tag_name"], "1.5.8")
        self.assertEqual(rel2.get("source"), "manifest")

    def test_download_with_resume_handles_206_and_200_reset(self):
        payload = b"0123456789abcdef" * 64
        seen_ranges = []

        class FakeResp:
            def __init__(self, data, status, content_len):
                self._buf = io.BytesIO(data)
                self.status = status
                self.headers = {"Content-Length": str(content_len)}

            def read(self, n=-1):
                return self._buf.read(n)

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

        calls = [
            FakeResp(payload[:300], 200, len(payload)),           # cut short at 300 bytes
            FakeResp(payload[300:], 206, len(payload) - 300),     # resumed via 206
        ]

        def fake_urlopen(req, timeout=120):
            seen_ranges.append(req.get_header("Range"))
            return calls.pop(0)

        dest = self.root / "dl.bin"
        with mock.patch.object(self.m.urllib.request, "urlopen", side_effect=fake_urlopen):
            n = self.m.download_with_resume("https://example.invalid/a.bin", dest)
        self.assertEqual(n, len(payload))
        self.assertEqual(dest.read_bytes(), payload)
        self.assertEqual(seen_ranges, [None, "bytes=300-"])

        # Server ignores Range and replies 200 with full body -> must truncate instead of duplicating prefix.
        calls2 = [
            FakeResp(payload[:250], 200, len(payload)),
            FakeResp(payload, 200, len(payload)),
        ]
        with mock.patch.object(self.m.urllib.request, "urlopen", side_effect=lambda req, timeout=120: calls2.pop(0)):
            n2 = self.m.download_with_resume("https://example.invalid/b.bin", dest)
        self.assertEqual(n2, len(payload))
        self.assertEqual(dest.read_bytes(), payload)

    def test_download_g1_verifies_manifest_sha512(self):
        tar_bytes = self._build_tar_bytes("1.5.8")
        good_sha512 = hashlib.sha512(tar_bytes).hexdigest()
        stage = self.root / "stage-mf"
        stage.mkdir()

        def fake_dl(url, dest, **kw):
            Path(dest).write_bytes(tar_bytes)
            return len(tar_bytes)

        bad_rel = {
            "source": "manifest",
            "tag_name": "1.5.8",
            "url": self.m.GCS_PREFIX + "1.5.8/linux_amd64/agy.tar.gz",
            "sha512": "b" * 128,
        }
        with mock.patch.object(self.m, "download_with_resume", side_effect=fake_dl):
            with self.assertRaisesRegex(RuntimeError, "SHA-512 mismatch"):
                self.m.download_g1(bad_rel, stage)

        good_rel = dict(bad_rel, sha512=good_sha512)
        with mock.patch.object(self.m, "download_with_resume", side_effect=fake_dl):
            detail = self.m.download_g1(good_rel, stage)
        self.assertIn("sha512", detail)
        self.assertIn("1.5.8", detail)
        self.assertTrue((stage / "agy-official").exists())

    def test_gcs_url_rejects_traversal_and_incomplete_read_resumes(self):
        self.assertTrue(self.m.is_trusted_gcs_tarball(self.m.GCS_PREFIX + "1.5.8/linux_amd64/agy.tar.gz"))
        self.assertFalse(self.m.is_trusted_gcs_tarball(self.m.GCS_PREFIX + "../other-bucket/agy.tar.gz"))
        self.assertFalse(self.m.is_trusted_gcs_tarball(self.m.GCS_PREFIX + "1.5.8/agy.tar.gz?alt=media"))

        payload = b"resume-after-incomplete-read-" * 32

        class IncompleteFirstResp:
            def __init__(self):
                self.status = 200
                self.headers = {"Content-Length": str(len(payload))}
                self._sent = False

            def read(self, n=-1):
                if not self._sent:
                    self._sent = True
                    return payload[:200]
                raise self.outer_http.IncompleteRead(b"")

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

        IncompleteFirstResp.outer_http = self.m.http.client

        class TailResp:
            def __init__(self):
                self.status = 206
                self.headers = {"Content-Length": str(len(payload) - 200)}
                self._buf = io.BytesIO(payload[200:])

            def read(self, n=-1):
                return self._buf.read(n)

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

        seq = [IncompleteFirstResp(), TailResp()]
        dest = self.root / "inc.bin"
        with mock.patch.object(self.m.urllib.request, "urlopen", side_effect=lambda req, timeout=120: seq.pop(0)):
            got = self.m.download_with_resume("https://example.invalid/inc.bin", dest)
        self.assertEqual(got, len(payload))
        self.assertEqual(dest.read_bytes(), payload)

    def test_http_500_and_urlerror_trigger_auto_rollback_on_apply_and_check(self):
        agy = self.m.AGY_DIR
        (agy / "1.5.5-aaaa").mkdir(parents=True)
        (agy / "1.5.5-aaaa" / "agy-acp-900k").write_text("old-stable")
        self.m.switch_current(agy / "1.5.5-aaaa")

        stage = self.root / "stage-broken"
        stage.mkdir()
        (stage / "agy-acp-900k").write_text("broken-new")
        sha = self.m.sha256(stage / "agy-acp-900k")
        installed_name = f"1.5.10-{sha[:8]}"

        # 1. Real urllib.error.HTTPError(500) raised inside urlopen during cmd_apply -> MUST auto-rollback!
        st = {
            "candidate": {
                "id": "agy-1.5.10-broken",
                "kind": "agy",
                "version": "1.5.10",
                "sha256": sha,
                "state": "ready",
                "stage": str(stage),
            },
            "history": [],
        }
        self.m.save_state(st)
        self.approve("agy-1.5.10-broken", sha)

        def raise_http_500(req, timeout=180):
            raise self.m.urllib.error.HTTPError(
                req.full_url, 500, "Internal Server Error", {}, io.BytesIO(b'{"error":"acp crashed"}')
            )

        eng_new = {"alive": True, "running": f"/data/agy/{installed_name}/agy-acp-900k"}
        with mock.patch.object(self.m, "gateway_key", return_value="k"), \
                mock.patch.object(self.m, "engine_info", return_value=eng_new), \
                mock.patch.object(self.m.urllib.request, "urlopen", side_effect=raise_http_500):
            rc = self.m.cmd_apply(types.SimpleNamespace())
        self.assertEqual(rc, 1)
        self.assertEqual(os.readlink(agy / "current"), "1.5.5-aaaa")
        saved = self.m.load_state()
        self.assertEqual(saved["candidate"]["state"], "rolled_back")
        self.assertIn("HTTP 500", saved["candidate"]["message"])
        self.assertEqual(saved["history"][0]["state"], "rolled_back")

        # 2. URLError / TimeoutError during cmd_check finishing a waiting_idle candidate -> MUST auto-rollback!
        self.m.switch_current(agy / installed_name)
        st_wait = {
            "candidate": {
                "id": "agy-1.5.10-wait",
                "kind": "agy",
                "version": "1.5.10",
                "sha256": sha,
                "state": "waiting_idle",
                "installed": installed_name,
                "previous": "1.5.5-aaaa",
                "stage": str(stage),
            },
            "history": [],
        }
        self.m.save_state(st_wait)
        with mock.patch.object(self.m, "gateway_key", return_value="k"), \
                mock.patch.object(self.m, "engine_info", return_value=eng_new), \
                mock.patch.object(
                    self.m.urllib.request,
                    "urlopen",
                    side_effect=self.m.urllib.error.URLError("connection reset"),
                ):
            rc2 = self.m.cmd_check(types.SimpleNamespace(force=False))
        self.assertEqual(rc2, 1)
        self.assertEqual(os.readlink(agy / "current"), "1.5.5-aaaa")
        saved2 = self.m.load_state()
        self.assertEqual(saved2["candidate"]["state"], "rolled_back")
        self.assertIn("URLError", saved2["candidate"]["message"])

    def test_empty_engine_info_does_not_wedge_waiting_idle_and_honors_port_env(self):
        with mock.patch.dict(os.environ, {"AGY_GW_PORT": "19890", "AGY_DASH_PORT": "19891"}, clear=False):
            m = load(self.root)
        self.assertEqual(m.GW_PORT, 19890)
        self.assertEqual(m.DASH_PORT, 19891)

        with mock.patch.dict(os.environ, {"AGY_GW_PORT": "", "AGY_DASH_PORT": "", "GW_PORT": "20890", "DASH_PORT": "20891"}, clear=False):
            m_short = load(self.root)
        self.assertEqual(m_short.GW_PORT, 20890)
        self.assertEqual(m_short.DASH_PORT, 20891)

        agy = m.AGY_DIR
        (agy / "1.5.5-aaaa").mkdir(parents=True, exist_ok=True)
        (agy / "1.5.5-aaaa" / "agy-acp-900k").write_text("old")
        (agy / "1.5.10-bbbb").mkdir(parents=True, exist_ok=True)
        (agy / "1.5.10-bbbb" / "agy-acp-900k").write_text("new")
        m.switch_current(agy / "1.5.10-bbbb")

        cand = {
            "id": "c-empty-eng",
            "kind": "agy",
            "version": "1.5.10",
            "state": "waiting_idle",
            "installed": "1.5.10-bbbb",
            "previous": "1.5.5-aaaa",
        }
        st = {"candidate": cand, "history": []}
        seen_bases = []
        eng_seq = [{}, {"alive": True, "running": "/data/agy/1.5.10-bbbb/agy-acp-900k"}]
        with mock.patch.object(m, "gateway_key", return_value="k"), \
                mock.patch.object(m, "engine_info", side_effect=lambda _k: eng_seq.pop(0)), \
                mock.patch.object(m, "flash_probe", side_effect=lambda base, _k, _t: (seen_bases.append(base) or True, "200 ok")):
            ok = m.finish_agy(st, cand, wait=30)
        self.assertTrue(ok)
        self.assertEqual(cand["state"], "applied")
        self.assertEqual(seen_bases, ["http://127.0.0.1:19890"])

        # Pre-switch rollback metadata: even if an OSError occurs right after switch_current(dest) inside apply_agy,
        # cmd_apply must still roll data/agy/current back to previous!
        m.switch_current(agy / "1.5.5-aaaa")
        stage_err = self.root / "stage-save-err"
        stage_err.mkdir()
        (stage_err / "agy-acp-900k").write_text("new-err")
        sha_err = m.sha256(stage_err / "agy-acp-900k")
        m.save_state({
            "candidate": {"id": "c-save-err", "kind": "agy", "version": "1.5.10", "sha256": sha_err, "state": "ready", "stage": str(stage_err)},
            "history": [],
        })
        self.approve("c-save-err", sha_err)
        orig_save = m.save_state
        save_calls = []

        def fail_second_save(s):
            save_calls.append(1)
            if len(save_calls) == 2:  # inside apply_agy right after switch_current(dest)
                raise OSError("simulated disk error after symlink switch")
            return orig_save(s)

        with mock.patch.object(m, "save_state", side_effect=fail_second_save):
            rc_err = m.cmd_apply(types.SimpleNamespace())
        self.assertEqual(rc_err, 1)
        self.assertEqual(os.readlink(agy / "current"), "1.5.5-aaaa")
        self.assertEqual(m.load_state()["candidate"]["state"], "rolled_back")

    def test_take_approval_drains_all_files_and_handles_malformed_or_naive_json(self):
        d = self.m.PUBLIC / "approvals"
        d.mkdir(parents=True, exist_ok=True)
        cid = "agy-1.5.10-good"
        sha = "c" * 64
        t1 = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=5)).isoformat()
        t2 = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=1)).isoformat()

        (d / "01-list.json").write_text("[1, 2, 3]")
        (d / "02-corrupt.json").write_text("{not-json")
        (d / "03-naive.json").write_text(json.dumps({"candidateId": cid, "sha256": sha, "approvedAt": "2026-11-17T12:00:00"}))
        (d / "04-bad-iso.json").write_text(json.dumps({"candidateId": cid, "sha256": sha, "approvedAt": "yesterday"}))
        (d / "05-valid-older.json").write_text(json.dumps({"candidateId": cid, "sha256": sha, "approvedAt": t1, "seq": 1}))
        (d / "06-valid-newer.json").write_text(json.dumps({"candidateId": cid, "sha256": sha, "approvedAt": t2, "seq": 2}))

        got = self.m.take_approval({"id": cid, "sha256": sha})
        self.assertIsNotNone(got)
        self.assertEqual(got["seq"], 2)
        # Every file must be drained in one pass so systemd PathExistsGlob cannot re-trigger in a loop
        self.assertEqual(list(d.glob("*.json")), [])
        self.assertEqual(len(list((self.m.STATE / "approvals").glob("*.json"))), 6)

    def test_gateway_bundle_handles_missing_and_corrupt_manifest_gracefully(self):
        rel = self.root / "releases/20261118-broken"
        (rel / "bin").mkdir(parents=True)
        (rel / "bin/agy-acp-gateway").write_text("gw-bin")
        (rel / "release.sh").write_text("exit 0")
        (rel / "READY").write_text("")

        # 1. Missing manifest.json -> clean failed gate instead of uncaught FileNotFoundError
        b1 = self.m.gateway_bundle()
        self.assertIsNotNone(b1)
        self.assertFalse(b1[2])
        self.assertIn("manifest.json", b1[3])

        rc = self.m.cmd_check(types.SimpleNamespace(force=False))
        self.assertEqual(rc, 0)
        st = self.m.load_state()
        self.assertEqual(st["candidate"]["state"], "failed")
        self.assertFalse(st["gates"][0]["ok"])

        # 2. Corrupt JSON / non-object manifest.json
        for bad in ("{corrupt", "[]", json.dumps({"artifacts": "bad", "checks": "bad"})):
            (rel / "manifest.json").write_text(bad)
            res = self.m.gateway_bundle()
            self.assertIsNotNone(res)
            self.assertFalse(res[2])

    def test_prune_candidates_retains_latest_two_and_protects_active_stage(self):
        cands_dir = self.m.STATE / "candidates"
        cands_dir.mkdir(parents=True, exist_ok=True)
        stages = []
        for idx in range(5):
            s = cands_dir / f"agy-1.5.{6 + idx}-20261117T0{idx}0000Z"
            s.mkdir()
            (s / "agy-acp-900k").write_text(f"bin-{idx}")
            ts = 1_700_000_000 + idx * 100
            os.utime(s, (ts, ts))
            stages.append(s)

        # Oldest stage is still `ready`: keep 2 newest + oldest active stage = 3 surviving directories
        st = {"candidate": {"id": stages[0].name, "state": "ready", "stage": str(stages[0])}}
        self.m.prune_candidates(st, keep=2)
        remaining = sorted(p.name for p in cands_dir.iterdir())
        self.assertEqual(remaining, sorted([stages[0].name, stages[3].name, stages[4].name]))

        # Once `applied`, active stage protection ends -> strictly 2 newest survive
        st["candidate"]["state"] = "applied"
        self.m.prune_candidates(st, keep=2)
        remaining2 = sorted(p.name for p in cands_dir.iterdir())
        self.assertEqual(remaining2, sorted([stages[3].name, stages[4].name]))

    def test_gateway_release_and_openresty_config_hardening(self):
        rel_sh = (HERE / "gateway_release.sh").read_text()
        self.assertIn("data/agy/current/agy-acp-900k", rel_sh)
        self.assertNotIn("cd /root/agy-acp-gateway", rel_sh)
        nginx_conf = (HERE.parent / "openresty-acp.conf").read_text()
        self.assertIn("proxy_set_header Host $http_host;", nginx_conf)
        self.assertNotIn("proxy_set_header Host $host;", nginx_conf)

    def test_cmd_apply_finishes_waiting_idle_and_rolls_back_on_failed_probe(self):
        agy = self.m.AGY_DIR
        (agy / "1.5.5-aaaa").mkdir(parents=True, exist_ok=True)
        (agy / "1.5.5-aaaa" / "agy-acp-900k").write_text("old-stable")
        (agy / "1.5.10-bbbb").mkdir(parents=True, exist_ok=True)
        (agy / "1.5.10-bbbb" / "agy-acp-900k").write_text("new-cand")
        self.m.switch_current(agy / "1.5.10-bbbb")

        cand = {
            "id": "agy-1.5.10-waiting",
            "kind": "agy",
            "version": "1.5.10",
            "sha256": "b" * 64,
            "state": "waiting_idle",
            "installed": "1.5.10-bbbb",
            "previous": "1.5.5-aaaa",
        }
        self.m.save_state({"candidate": cand, "history": []})
        self.approve("stale-cand", "f" * 64)

        # 1. While old engine is still alive and running the old binary, cmd_apply keeps waiting_idle with 0 Flash probes.
        eng_old = {"alive": True, "running": "/data/agy/1.5.5-aaaa/agy-acp-900k"}
        with mock.patch.object(self.m, "gateway_key", return_value="k"), \
                mock.patch.object(self.m, "engine_info", return_value=eng_old), \
                mock.patch.object(self.m, "flash_probe") as probe_mock:
            rc_busy = self.m.cmd_apply(types.SimpleNamespace())
        self.assertEqual(rc_busy, 0)
        probe_mock.assert_not_called()
        self.assertEqual(list((self.m.PUBLIC / "approvals").glob("*.json")), [])
        st_busy = self.m.load_state()
        self.assertEqual(st_busy["candidate"]["state"], "waiting_idle")
        self.assertEqual(st_busy["history"], [])

        # 2. Once engine switches to the new binary, cmd_apply verifies with 1 Flash probe and marks applied.
        eng_new = {"alive": True, "running": "/data/agy/1.5.10-bbbb/agy-acp-900k"}
        with mock.patch.object(self.m, "gateway_key", return_value="k"), \
                mock.patch.object(self.m, "engine_info", return_value=eng_new), \
                mock.patch.object(self.m, "flash_probe", return_value=(True, "200 ok")) as probe_ok:
            rc_done = self.m.cmd_apply(types.SimpleNamespace())
        self.assertEqual(rc_done, 0)
        probe_ok.assert_called_once()
        st_done = self.m.load_state()
        self.assertEqual(st_done["candidate"]["state"], "applied")
        self.assertEqual(st_done["history"][0]["state"], "applied")

        # 3. If the post-idle Flash probe fails on cmd_apply, it immediately auto-rolls back to previous.
        self.m.save_state({"candidate": dict(cand), "history": []})
        self.m.switch_current(agy / "1.5.10-bbbb")
        with mock.patch.object(self.m, "gateway_key", return_value="k"), \
                mock.patch.object(self.m, "engine_info", return_value=eng_new), \
                mock.patch.object(self.m, "flash_probe", return_value=(False, "HTTP 500")):
            rc_fail = self.m.cmd_apply(types.SimpleNamespace())
        self.assertEqual(rc_fail, 1)
        self.assertEqual(os.readlink(agy / "current"), "1.5.5-aaaa")
        st_fail = self.m.load_state()
        self.assertEqual(st_fail["candidate"]["state"], "rolled_back")
        self.assertEqual(st_fail["history"][0]["state"], "rolled_back")

    def test_gateway_release_sh_honors_agy_prod_root_and_ports_and_guards_release_dir(self):
        import subprocess

        opt_root = self.root / "opt" / "agy-acp-gateway"
        opt_root.mkdir(parents=True)

        # 1. Direct invocation from deploy/prod-upgrader/gateway_release.sh MUST exit 2 even when AGY_PROD_ROOT is set.
        env_direct = dict(os.environ, AGY_PROD_ROOT=str(opt_root))
        r_direct = subprocess.run(
            ["sh", str(HERE / "gateway_release.sh")],
            cwd=str(HERE.parent.parent),
            env=env_direct,
            capture_output=True,
            text=True,
        )
        self.assertEqual(r_direct.returncode, 2)
        self.assertIn("unexpected release dir", r_direct.stderr)

        # 2. Staged release under custom AGY_PROD_ROOT (/opt/agy-acp-gateway style) with custom ports runs end-to-end offline via shims.
        (opt_root / "Dockerfile").write_text("FROM scratch\n")
        (opt_root / "bin").mkdir()
        (opt_root / "bin" / "agy-acp-gateway").write_text("old-gw")
        (opt_root / "data" / "agy" / "1.5.10-aaaa").mkdir(parents=True)
        (opt_root / "data" / "agy" / "1.5.10-aaaa" / "agy-acp-900k").write_text("agy-bin")
        (opt_root / "data" / "agy" / "current").symlink_to("1.5.10-aaaa")

        rel_dir = opt_root / "releases" / "20261118-r5"
        (rel_dir / "bin").mkdir(parents=True)
        (rel_dir / "bin" / "agy-acp-gateway").write_text("new-gw-binary")
        gw_sha = self.m.sha256(rel_dir / "bin" / "agy-acp-gateway")
        (rel_dir / "manifest.json").write_text(json.dumps({"artifacts": {"gateway": gw_sha}}))
        rel_sh_path = rel_dir / "release.sh"
        rel_sh_path.write_text((HERE / "gateway_release.sh").read_text())
        rel_sh_path.chmod(0o755)

        shim_dir = self.root / "shims"
        shim_dir.mkdir()
        curl_log = self.root / "curl.log"

        (shim_dir / "sha256sum").write_text(
            "#!/usr/bin/env python3\n"
            "import hashlib, pathlib, sys\n"
            "p = pathlib.Path(sys.argv[1])\n"
            "print(hashlib.sha256(p.read_bytes()).hexdigest() + '  ' + sys.argv[1])\n"
        )
        (shim_dir / "sha256sum").chmod(0o755)

        (shim_dir / "curl").write_text(
            "#!/usr/bin/env python3\n"
            "import os, pathlib, sys\n"
            "args = sys.argv[1:]\n"
            "url = [a for a in args if a.startswith('http://')][-1]\n"
            f"with open({str(curl_log)!r}, 'a') as f:\n"
            "    f.write(url + '\\n')\n"
            "if '%{http_code}' in args:\n"
            "    sys.stdout.write('401')\n"
            "elif url.endswith('/v1/models'):\n"
            "    sys.stdout.write('{\"data\":[{\"id\":\"gemini-4-carbon\"},{\"id\":\"gemini-4-neon\"},{\"id\":\"gemini-4-hydrogen\"}]}')\n"
            "elif url.endswith('/v1beta/models'):\n"
            "    sys.stdout.write('{\"models\":[{\"name\":\"models/gemini-4-carbon\"},{\"name\":\"models/gemini-4-neon\"}]}')\n"
            "elif url.endswith('/api/dashboard'):\n"
            "    sys.stdout.write('{\"geminiPool\":{}}')\n"
            "elif url.endswith('/api/upgrade'):\n"
            "    sys.stdout.write('{\"enabled\":true}')\n"
        )
        (shim_dir / "curl").chmod(0o755)

        (shim_dir / "docker").write_text(
            "#!/usr/bin/env python3\n"
            "import os, subprocess, sys\n"
            "args = sys.argv[1:]\n"
            "if args[:2] == ['run', '--rm'] and 'sha256sum' in args:\n"
            f"    print({gw_sha!r} + '  /app/bin/agy-acp-gateway')\n"
            "elif args and args[0] == 'exec':\n"
            "    env = dict(os.environ, AGY_API_KEYS='dummy-test-key')\n"
            "    i = 1\n"
            "    while i < len(args) and args[i] == '-e':\n"
            "        k, v = args[i + 1].split('=', 1)\n"
            "        env[k] = v\n"
            "        i += 2\n"
            "    i += 1  # container name\n"
            "    cmd = args[i:]\n"
            "    if cmd[:2] == ['sha256sum', '/app/bin/agy-acp-gateway']:\n"
            f"        print({gw_sha!r} + '  /app/bin/agy-acp-gateway')\n"
            "    else:\n"
            "        r = subprocess.run(cmd, env=env)\n"
            "        sys.exit(r.returncode)\n"
            "elif args and args[0] == 'inspect':\n"
            "    print('startedAt=now image=local health=healthy restarts=0')\n"
        )
        (shim_dir / "docker").chmod(0o755)

        # Serve a tiny ephemeral loopback HTTP server on a random ephemeral port ONLY for the inline python smoke call.
        import http.server
        import threading

        seen_smoke_paths = []

        class SmokeHandler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                seen_smoke_paths.append(self.path)
                body = json.dumps({
                    "stop_reason": "end_turn",
                    "content": [{"type": "text", "text": "OK"}],
                }).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        srv = http.server.HTTPServer(("127.0.0.1", 0), SmokeHandler)
        smoke_gw_port = srv.server_port
        smoke_dash_port = smoke_gw_port + 1
        th = threading.Thread(target=srv.serve_forever, daemon=True)
        th.start()
        try:
            env_staged = dict(
                os.environ,
                PATH=f"{shim_dir}:{os.environ.get('PATH', '')}",
                AGY_PROD_ROOT=str(opt_root) + "/",
                AGY_GW_PORT=str(smoke_gw_port),
                AGY_DASH_PORT=str(smoke_dash_port),
            )
            r_staged = subprocess.run(
                ["sh", str(rel_sh_path)],
                cwd=str(self.root),
                env=env_staged,
                capture_output=True,
                text=True,
                timeout=30,
            )
        finally:
            srv.shutdown()
            srv.server_close()

        self.assertEqual(r_staged.returncode, 0, msg=f"stdout={r_staged.stdout}\nstderr={r_staged.stderr}")
        self.assertEqual((rel_dir / "result").read_text().strip(), "DEPLOYED_AND_VERIFIED")
        self.assertEqual((opt_root / "bin" / "agy-acp-gateway").read_text(), "new-gw-binary")
        self.assertEqual(seen_smoke_paths, ["/v1/messages"])
        logged_urls = curl_log.read_text().splitlines()
        self.assertTrue(any(f"127.0.0.1:{smoke_gw_port}/healthz" in u for u in logged_urls))
        self.assertTrue(any(f"127.0.0.1:{smoke_dash_port}/api/dashboard" in u for u in logged_urls))
        self.assertFalse(any(":18890/" in u or ":18891/" in u for u in logged_urls))


    def test_isolated_home_and_test_container_preserve_installation_id_and_proxy(self):
        app_src = self.m.TOKEN.parent
        (app_src / "cache").mkdir(parents=True, exist_ok=True)
        self.m.TOKEN.write_text('{"token":{"access_token":"tok"}}')
        (app_src / "installation_id").write_text("inst-uuid-1234")
        (app_src / "cache/onboarding.json").write_text('{"onboardingComplete":true}')

        stage = self.root / "stage-id"
        stage.mkdir()
        home = self.m.isolated_home(stage)
        self.assertEqual((home / "antigravity-cli/antigravity-oauth-token").resolve(), self.m.TOKEN.resolve())
        self.assertEqual((home / "antigravity-cli/installation_id").resolve(), (app_src / "installation_id").resolve())
        self.assertEqual((home / "antigravity-cli/cache/onboarding.json").resolve(), (app_src / "cache/onboarding.json").resolve())

        seen_cmds = []

        def fake_run(cmd, **kw):
            seen_cmds.append([str(x) for x in cmd])
            return types.SimpleNamespace(returncode=0, stdout="", stderr="")

        class OkResp:
            status = 200
            def __enter__(self):
                return self
            def __exit__(self, *args):
                return False

        cand_bin = stage / "agy-acp-900k"
        cand_bin.write_text("bin")
        with mock.patch.object(self.m, "run", side_effect=fake_run), \
                mock.patch.object(self.m.os, "chown"), \
                mock.patch.object(self.m.urllib.request, "urlopen", return_value=OkResp()), \
                mock.patch.dict(os.environ, {"AGY_UPSTREAM_PROXY": "http://127.0.0.1:7890"}):
            with self.m.test_container(cand_bin, stage):
                pass

        docker_runs = [c for c in seen_cmds if len(c) >= 2 and c[0] == "docker" and c[1] == "run"]
        self.assertEqual(len(docker_runs), 1)
        dcmd = docker_runs[0]
        self.assertIn("AGY_UPSTREAM_PROXY=http://127.0.0.1:7890", dcmd)
        self.assertTrue(any(x.endswith(":/data/.gemini/antigravity-cli/installation_id:ro") for x in dcmd))
        self.assertTrue(any(x.endswith(":/data/.gemini/antigravity-cli/cache/onboarding.json:ro") for x in dcmd))

    def test_resolve_probe_auth_falls_back_to_active_child_account_when_primary_draining_or_disabled(self):
        acc4_tok = self.m.DATA / "accounts/acc4/.gemini/antigravity-cli/antigravity-oauth-token"
        acc4_tok.parent.mkdir(parents=True, exist_ok=True)
        acc4_tok.write_text("child-token")
        (self.m.DATA / "accounts.json").write_text(json.dumps({
            "accounts": [
                {"id": "tr20x", "primary": True, "enabled": True, "schedState": "draining", "proxyPreset": "direct"},
                {"id": "acc4", "primary": False, "enabled": True, "schedState": "active", "proxyPreset": "custom", "customProxy": "http://127.0.0.1:7899"},
            ]
        }))
        tok, proxy = self.m.resolve_probe_auth()
        self.assertEqual(tok, acc4_tok)
        self.assertEqual(proxy, "http://127.0.0.1:7899")

    def test_apply_path_unit_watches_engine_json_for_waiting_idle(self):
        unit = (Path(__file__).resolve().parent / "systemd/agy-upgrader-apply.path").read_text(encoding="utf-8")
        self.assertIn("PathExistsGlob=/root/agy-acp-gateway/data/upgrade/approvals/*.json", unit)
        self.assertIn("PathModified=/root/agy-acp-gateway/data/upgrade/engine.json", unit)

if __name__ == "__main__":
    sys.exit(unittest.main())
