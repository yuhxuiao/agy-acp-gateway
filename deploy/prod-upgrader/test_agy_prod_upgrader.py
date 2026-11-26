"""Offline tests for the production update agent (no docker, no network, no generation)."""
import contextlib
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
        # proxy credentials travel in a 0600 env file (removed after start), never on the command line
        self.assertFalse(any("AGY_UPSTREAM_PROXY" in x for x in dcmd))
        self.assertIn("--env-file", dcmd)
        self.assertFalse((stage / "test.env").exists())
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

    def _multi_accounts(self):
        for acc in ("uspro", "acc3", "acc4"):
            t = self.m.DATA / f"accounts/{acc}/.gemini/antigravity-cli/antigravity-oauth-token"
            t.parent.mkdir(parents=True, exist_ok=True)
            t.write_text("tok-" + acc)
        self.m.TOKEN.parent.mkdir(parents=True, exist_ok=True)
        self.m.TOKEN.write_text("tok-primary")
        (self.m.DATA / "accounts.json").write_text(json.dumps({"accounts": [
            {"id": "tr20x", "primary": True, "enabled": True, "schedState": "active", "proxyPreset": "direct",
             "allowedModels": ["gemini-4-neon", "gemini-3.8-flash"]},
            {"id": "uspro", "enabled": True, "schedState": "active", "proxyPreset": "agy-uspro-jp",
             "allowedModels": ["gemini-4-neon", "gemini-3.8-flash"]},
            {"id": "acc3", "enabled": True, "schedState": "active", "proxyPreset": "agy-acc3-sg",
             "allowedModels": ["gemini-3.8-flash"]},
            {"id": "acc4", "enabled": True, "schedState": "active", "proxyPreset": "agy-acc4-kr",
             "allowedModels": ["claude-sonnet-5-5"]},
        ]}))

    def test_probe_candidates_route_by_quota_flash_binding_and_resin_preset(self):
        self._multi_accounts()
        view = {"tr20x": {"rem5h": 0.0, "health": "healthy"}, "uspro": {"rem5h": 0.095, "health": "healthy"},
                "acc3": {"rem5h": 0.20, "health": "healthy"}, "acc4": {"rem5h": 1.0, "health": "healthy"}}
        with mock.patch.object(self.m, "resin_settings", return_value={"token": "rtok", "host": "resin:2260"}):
            cands = self.m.probe_auth_candidates(view, from_host=False)
            ids = [c[0] for c in cands]
            # acc4 cannot serve Flash; exhausted primary goes last; most remaining quota first
            self.assertEqual(ids, ["acc3", "uspro", "tr20x"])
            self.assertEqual(cands[0][2], "http://agy-acc3-sg.acc3:rtok@resin:2260")
            self.assertEqual(cands[-1][2], "")
            host_side = self.m.probe_auth_candidates(view, from_host=True)
            self.assertEqual(host_side[0][2], "http://agy-acc3-sg.acc3:rtok@127.0.0.1:2260")
        # banned accounts are skipped entirely
        view["acc3"]["health"] = "tos_tripped"
        with mock.patch.object(self.m, "resin_settings", return_value={"token": "rtok", "host": "resin:2260"}):
            self.assertEqual([c[0] for c in self.m.probe_auth_candidates(view)], ["uspro", "tr20x"])

    def test_unresolvable_named_preset_never_falls_back_to_direct_for_child(self):
        self._multi_accounts()
        with mock.patch.object(self.m, "resin_settings", return_value={}):
            ids = [c[0] for c in self.m.probe_auth_candidates({})]
        self.assertEqual(ids, ["tr20x"])  # child accounts with Resin presets are dropped, not sent direct

    def test_live_g4_routes_past_quota_limited_accounts(self):
        self._multi_accounts()
        view = {"tr20x": {"rem5h": 0.5}, "uspro": {"rem5h": 0.4}, "acc3": {"rem5h": 0.3}}
        calls = []

        def fake_on(stage, binary, auth):
            calls.append(auth[0].parent.parent.parent.name if "accounts" in str(auth[0]) else "tr20x")
            if len(calls) == 1:
                raise RuntimeError("text probe: HTTP 429 'RESOURCE_EXHAUSTED quota'")
            return "200 end_turn 'OK' 2.0s"

        with mock.patch.object(self.m, "resin_settings", return_value={"token": "r", "host": "resin:2260"}), \
                mock.patch.object(self.m, "_live_g4_on", side_effect=fake_on), mock.patch.object(self.m, "log"):
            detail = self.m.live_g4(self.root, self.root / "bin", view)
        self.assertEqual(calls, ["tr20x", "uspro"])
        self.assertIn("via uspro", detail)
        self.assertIn("skipped tr20x: quota", detail)

        def broken(stage, binary, auth):
            raise RuntimeError("tool step 1: stop=end_turn")

        with mock.patch.object(self.m, "resin_settings", return_value={"token": "r", "host": "resin:2260"}), \
                mock.patch.object(self.m, "_live_g4_on", side_effect=broken):
            with self.assertRaisesRegex(RuntimeError, "tool step 1"):
                self.m.live_g4(self.root, self.root / "bin", view)

        def all_quota(stage, binary, auth):
            raise RuntimeError("HTTP 429 pool_cooldown")

        with mock.patch.object(self.m, "resin_settings", return_value={"token": "r", "host": "resin:2260"}), \
                mock.patch.object(self.m, "_live_g4_on", side_effect=all_quota), mock.patch.object(self.m, "log"):
            with self.assertRaisesRegex(RuntimeError, "all accounts quota-limited"):
                self.m.live_g4(self.root, self.root / "bin", view)

    def test_post_switch_quota_failure_defers_instead_of_rolling_back(self):
        agy = self.m.AGY_DIR
        for v in ("1.5.5-aaaa", "1.5.6-bbbbbbbb"):
            (agy / v).mkdir(parents=True)
            (agy / v / "agy-acp-900k").write_text(v)
        self.m.switch_current(agy / "1.5.6-bbbbbbbb")
        cand = {"id": "c9", "kind": "agy", "version": "1.5.6", "sha256": "b" * 64, "state": "waiting_idle",
                "installed": "1.5.6-bbbbbbbb", "previous": "1.5.5-aaaa"}
        st = {"candidate": cand, "history": []}
        engine = {"alive": True, "running": "/data/agy/1.5.6-bbbbbbbb/agy-acp-900k"}
        probe = (False, 429, "pool_cooldown", "HTTP 429 [pool_cooldown] '{\"error\":{\"code\":\"pool_cooldown\"}}'")
        with mock.patch.object(self.m, "gateway_key", return_value="k"), \
                mock.patch.object(self.m, "engine_info", side_effect=lambda k: engine), \
                mock.patch.object(self.m, "flash_probe", return_value=probe), mock.patch.object(self.m, "log"), \
                mock.patch.dict(os.environ, {"AGY_VERIFY_DEFER_BACKOFF_SECS": "60,120,240"}, clear=False):
            prev_not_before = None
            for i in range(1, self.m.MAX_VERIFY_DEFERRALS + 1):
                self.assertTrue(self.m.finish_agy(st, cand, wait=0))
                self.assertEqual(cand["state"], "verify_deferred")
                self.assertEqual(cand["verifyDeferrals"], i)
                self.assertIn("verifyNotBefore", cand)
                nb = self.m._parse_iso_utc(cand["verifyNotBefore"])
                self.assertIsNotNone(nb)
                if prev_not_before is not None:
                    self.assertGreater(nb, prev_not_before)
                prev_not_before = nb
                self.assertEqual(os.readlink(agy / "current"), "1.5.6-bbbbbbbb")
            self.assertFalse(self.m.finish_agy(st, cand, wait=0))  # gave up after the deferral budget
        self.assertEqual(cand["state"], "rolled_back")
        self.assertNotIn("verifyNotBefore", cand)
        self.assertEqual(os.readlink(agy / "current"), "1.5.5-aaaa")

    def test_is_quota_failure(self):
        for d in ("HTTP 429 'x'", "RESOURCE_EXHAUSTED", "Individual quota reached", "pool_cooldown",
                  "pool-monitor-tripped", "rate_limit_error"):
            self.assertTrue(self.m.is_quota_failure(d), d)
        self.assertTrue(self.m.is_quota_failure("opaque", http_status=429))
        self.assertTrue(self.m.is_quota_failure("opaque", http_status=503, err_code="rate_limit_error"))
        self.assertTrue(self.m.is_quota_failure("opaque", http_status=503, err_code="pool-monitor-tripped"))
        # Must NOT false-positive on HTTP 500 or bare substrings '429'/'quota'/'capacity'
        for d in (
            "HTTP 500 'boom'",
            "HTTP 500 'pid 14290 crashed'",
            "HTTP 500 'worker pool at capacity'",
            "HTTP 500 'quota parse error'",
            "HTTP 500 'buffer capacity exceeded'",
            "pid 14290 crashed",
            "worker pool at capacity",
            "quota parse error",
            "tool step 1: stop=end_turn",
            "URLError: refused",
        ):
            self.assertFalse(self.m.is_quota_failure(d), d)
        self.assertFalse(self.m.is_quota_failure("quota exceeded in parser", http_status=500))

    def test_verify_deferred_backoff_skip_in_apply_and_check_and_idle_engine_deferral(self):
        agy = self.m.AGY_DIR
        for v in ("1.5.5-aaaa", "1.5.6-bbbbbbbb"):
            (agy / v).mkdir(parents=True, exist_ok=True)
            (agy / v / "agy-acp-900k").write_text(v)
        self.m.switch_current(agy / "1.5.6-bbbbbbbb")

        # Default backoff schedule: +15m, +30m, +60m
        with mock.patch.dict(os.environ, {"AGY_VERIFY_DEFER_BACKOFF_SECS": ""}, clear=False):
            self.assertEqual(self.m.verify_defer_backoff_secs(1), 900)
            self.assertEqual(self.m.verify_defer_backoff_secs(2), 1800)
            self.assertEqual(self.m.verify_defer_backoff_secs(3), 3600)

        # Idle/restarted engine (alive=False, running="", current points to installed) MUST defer on 429
        cand = {
            "id": "agy-1.5.6-idle",
            "kind": "agy",
            "version": "1.5.6",
            "sha256": "b" * 64,
            "state": "waiting_idle",
            "installed": "1.5.6-bbbbbbbb",
            "previous": "1.5.5-aaaa",
        }
        st = {"candidate": cand, "history": []}
        idle_eng = {"alive": False, "running": "", "current": "/data/agy/1.5.6-bbbbbbbb/agy-acp-900k"}
        probe_429 = (False, 429, "pool-monitor-tripped", "HTTP 429 [pool-monitor-tripped]")
        with mock.patch.object(self.m, "gateway_key", return_value="k"), \
                mock.patch.object(self.m, "engine_info", return_value=idle_eng), \
                mock.patch.object(self.m, "flash_probe", return_value=probe_429), \
                mock.patch.object(self.m, "log"):
            self.assertTrue(self.m.finish_agy(st, cand, wait=0))
        self.assertEqual(cand["state"], "verify_deferred")
        self.assertEqual(cand["verifyDeferrals"], 1)
        self.assertEqual(os.readlink(agy / "current"), "1.5.6-bbbbbbbb")
        self.m.save_state(st)

        # While now < verifyNotBefore: BOTH cmd_apply and cmd_check skip cleanly WITHOUT incrementing verifyDeferrals
        with mock.patch.object(self.m, "finish_agy") as fin_mock, mock.patch.object(self.m, "log"):
            self.assertEqual(self.m.cmd_apply(types.SimpleNamespace()), 0)
            self.assertEqual(self.m.cmd_check(types.SimpleNamespace(force=False)), 0)
        fin_mock.assert_not_called()
        saved = self.m.load_state()
        self.assertEqual(saved["candidate"]["state"], "verify_deferred")
        self.assertEqual(saved["candidate"]["verifyDeferrals"], 1)

        # Once now >= verifyNotBefore: cmd_apply runs finish_agy and marks applied (clearing verifyDeferrals/verifyNotBefore)
        saved["candidate"]["verifyNotBefore"] = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=5)).isoformat(timespec="seconds")
        self.m.save_state(saved)
        running_eng = {"alive": True, "running": "/data/agy/1.5.6-bbbbbbbb/agy-acp-900k"}
        with mock.patch.object(self.m, "gateway_key", return_value="k"), \
                mock.patch.object(self.m, "engine_info", return_value=running_eng), \
                mock.patch.object(self.m, "flash_probe", return_value=(True, 200, "", "200 end_turn 'OK' 1.0s")), \
                mock.patch.object(self.m, "log"):
            self.assertEqual(self.m.cmd_apply(types.SimpleNamespace()), 0)
        done = self.m.load_state()
        self.assertEqual(done["candidate"]["state"], "applied")
        self.assertNotIn("verifyDeferrals", done["candidate"])
        self.assertNotIn("verifyNotBefore", done["candidate"])

        # Non-quota HTTP 500 during post-backoff cmd_check immediately rolls back
        cand_500 = dict(cand, state="verify_deferred", verifyDeferrals=1,
                        verifyNotBefore=(dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=5)).isoformat(timespec="seconds"))
        self.m.save_state({"candidate": cand_500, "history": []})
        with mock.patch.object(self.m, "gateway_key", return_value="k"), \
                mock.patch.object(self.m, "engine_info", return_value=running_eng), \
                mock.patch.object(self.m, "flash_probe", return_value=(False, 500, "", "HTTP 500 'acp died'")), \
                mock.patch.object(self.m, "log"):
            self.assertEqual(self.m.cmd_check(types.SimpleNamespace(force=False)), 1)
        rb = self.m.load_state()
        self.assertEqual(rb["candidate"]["state"], "rolled_back")
        self.assertEqual(os.readlink(agy / "current"), "1.5.5-aaaa")

        # prune_candidates protects verify_deferred stage
        cands_dir = self.m.STATE / "candidates"
        cands_dir.mkdir(parents=True, exist_ok=True)
        stages = []
        for idx in range(4):
            s = cands_dir / f"agy-1.5.{idx}-s"
            s.mkdir()
            ts = 1_700_000_000 + idx * 100
            os.utime(s, (ts, ts))
            stages.append(s)
        self.m.prune_candidates({"candidate": {"state": "verify_deferred", "stage": str(stages[0])}}, keep=2)
        self.assertTrue(stages[0].exists())

    def test_privilege_boundary_symlinks_env_injection_and_path_traversal(self):
        # 1. publish() rejects symlinked PUBLIC, approvals, status.json and ignores pre-planted .status.json.tmp symlink
        outside = self.root / "outside-secret.txt"
        outside.write_text("do-not-touch")
        self.m.PUBLIC.mkdir(parents=True, exist_ok=True)
        (self.m.PUBLIC / ".status.json.tmp").symlink_to(outside)
        with mock.patch.object(self.m.os, "geteuid", return_value=0), \
                mock.patch.object(self.m.os, "lchown") as lchown_mock, \
                mock.patch.object(self.m.os, "chown", side_effect=AssertionError("must use lchown")):
            self.m.publish({"candidate": None})
        self.assertEqual(outside.read_text(), "do-not-touch")
        self.assertGreaterEqual(lchown_mock.call_count, 2)

        # symlinked status.json is rejected
        (self.m.PUBLIC / "status.json").unlink()
        (self.m.PUBLIC / "status.json").symlink_to(outside)
        with self.assertRaisesRegex(RuntimeError, "symlink"):
            self.m.publish({"candidate": None})
        (self.m.PUBLIC / "status.json").unlink()

        # symlinked approvals dir is rejected
        (self.m.PUBLIC / "approvals").rmdir()
        (self.m.PUBLIC / "approvals").symlink_to(self.root)
        with self.assertRaisesRegex(RuntimeError, "symlink"):
            self.m.publish({"candidate": None})
        (self.m.PUBLIC / "approvals").unlink()

        # 2. take_approval() rejects & unlinks symlinked approval without reading target
        (self.m.PUBLIC / "approvals").mkdir(parents=True, exist_ok=True)
        outside_approval = self.root / "real-approval.json"
        outside_approval.write_text(json.dumps({
            "candidateId": "c1", "sha256": "a" * 64,
            "approvedAt": dt.datetime.now(dt.timezone.utc).isoformat(),
        }))
        (self.m.PUBLIC / "approvals/c1.json").symlink_to(outside_approval)
        with mock.patch.object(self.m, "log"):
            self.assertIsNone(self.m.take_approval({"id": "c1", "sha256": "a" * 64}))
        self.assertFalse((self.m.PUBLIC / "approvals/c1.json").is_symlink())
        self.assertTrue(outside_approval.exists())

        # 3. Path traversal acc_id, symlinked token, and newline-injected customProxy are rejected
        evil_tok = self.root / "host-secret-token"
        evil_tok.write_text("secret")
        sym_dir = self.m.DATA / "accounts/symacc/.gemini/antigravity-cli"
        sym_dir.mkdir(parents=True, exist_ok=True)
        (sym_dir / "antigravity-oauth-token").symlink_to(evil_tok)
        good_dir = self.m.DATA / "accounts/inj/.gemini/antigravity-cli"
        good_dir.mkdir(parents=True, exist_ok=True)
        (good_dir / "antigravity-oauth-token").write_text("ok")
        (self.m.DATA / "accounts.json").write_text(json.dumps({"accounts": [
            {"id": "../escape", "enabled": True, "schedState": "active", "proxyPreset": "direct"},
            {"id": "symacc", "enabled": True, "schedState": "active", "proxyPreset": "direct"},
            {"id": "inj", "enabled": True, "schedState": "active", "proxyPreset": "custom",
             "customProxy": "http://127.0.0.1:7890\nLD_PRELOAD=/tmp/evil.so"},
        ]}))
        with mock.patch.object(self.m, "gateway_env", return_value=""):
            self.assertEqual(self.m.probe_auth_candidates({}), [])

        # 4. test_container() rejects newline proxy, rejects symlink token, and cleans test.env + testdata on failure
        stage = self.root / "stage-sec"
        stage.mkdir()
        cand_bin = stage / "agy-acp-900k"
        cand_bin.write_text("bin")
        with self.assertRaises(ValueError):
            with self.m.test_container(cand_bin, stage, (good_dir / "antigravity-oauth-token", "http://127.0.0.1:7890\nFOO=bar")):
                pass
        with self.assertRaisesRegex(RuntimeError, "unsafe token"):
            with self.m.test_container(cand_bin, stage, (sym_dir / "antigravity-oauth-token", "")):
                pass

        # Pre-planted symlink at stage/test.env must fail closed with O_EXCL|O_NOFOLLOW without touching target
        (stage / "test.env").symlink_to(outside)
        with self.assertRaises(OSError):
            with self.m.test_container(cand_bin, stage, (good_dir / "antigravity-oauth-token", "http://127.0.0.1:7890")):
                pass
        self.assertEqual(outside.read_text(), "do-not-touch")
        (stage / "test.env").unlink()

        # Failure during docker run still unlinks test.env AND removes testdata-*
        def fail_docker(cmd, **kw):
            if len(cmd) >= 2 and cmd[0] == "docker" and cmd[1] == "run":
                raise RuntimeError("docker run failed")
            return types.SimpleNamespace(returncode=0, stdout="", stderr="")

        with mock.patch.object(self.m, "run", side_effect=fail_docker):
            with self.assertRaisesRegex(RuntimeError, "docker run failed"):
                with self.m.test_container(cand_bin, stage, (good_dir / "antigravity-oauth-token", "http://127.0.0.1:7890"),
                                           data_dir=stage / "testdata-0"):
                    pass
        self.assertFalse((stage / "test.env").exists())
        self.assertFalse((stage / "testdata-0").exists())

    def test_fail_closed_candidates_and_go_proxy_allowlist_parity(self):
        # 1. account_allows_flash parity: None -> True, [] -> False, "*" -> True, exact flash -> True
        self.assertTrue(self.m.account_allows_flash({"id": "a"}))
        self.assertTrue(self.m.account_allows_flash({"id": "a", "allowedModels": None}))
        self.assertFalse(self.m.account_allows_flash({"id": "a", "allowedModels": []}))
        self.assertTrue(self.m.account_allows_flash({"id": "a", "allowedModels": ["*"]}))
        self.assertTrue(self.m.account_allows_flash({"id": "a", "allowedModels": ["GEMINI-3.8-FLASH"]}))
        self.assertFalse(self.m.account_allows_flash({"id": "a", "allowedModels": ["claude-sonnet-5-5"]}))

        # 2. Enabled=False primary is dropped even when pool_view is empty; no unsafe primary fallback!
        self.m.TOKEN.parent.mkdir(parents=True, exist_ok=True)
        self.m.TOKEN.write_text("tok-p")
        (self.m.DATA / "accounts.json").write_text(json.dumps({"accounts": [
            {"id": "tr20x", "primary": True, "enabled": False, "schedState": "disabled", "proxyPreset": "direct"},
        ]}))
        with mock.patch.object(self.m, "gateway_env", return_value=""):
            self.assertEqual(self.m.probe_auth_candidates({}), [])
            with self.assertRaisesRegex(RuntimeError, "fail-closed"):
                self.m.resolve_probe_auth({})
            with self.assertRaisesRegex(RuntimeError, "fail-closed"):
                self.m.contract_g3(self.root, {})
            with self.assertRaisesRegex(RuntimeError, "fail-closed"):
                self.m.live_g4(self.root, self.root / "bin", {})

        # 3. Primary with unresolvable named proxyPreset MUST fail closed (return None), matching Go ResolveAccountProxyChecked
        (self.m.DATA / "accounts.json").write_text(json.dumps({"accounts": [
            {"id": "tr20x", "primary": True, "enabled": True, "schedState": "active", "proxyPreset": "agy-jp-res"},
        ]}))
        with mock.patch.object(self.m, "gateway_env", return_value=""):
            self.assertEqual(self.m.probe_auth_candidates({}), [])

        # 4. Container AGY_UPSTREAM_PROXY read for primary direct AND Resin token+host extraction when AGY_RESIN_PROXY_TOKEN unset
        def fake_gw_env(name):
            if name == "AGY_UPSTREAM_PROXY":
                return "http://boot.user:extractedTok99@10.0.0.5:2260"
            return ""

        (self.m.DATA / "accounts.json").write_text(json.dumps({"accounts": [
            {"id": "tr20x", "primary": True, "enabled": True, "schedState": "active", "proxyPreset": "direct"},
            {"id": "uspro", "enabled": True, "schedState": "active", "proxyPreset": "agy-us-east"},
        ]}))
        uspro_tok = self.m.DATA / "accounts/uspro/.gemini/antigravity-cli/antigravity-oauth-token"
        uspro_tok.parent.mkdir(parents=True, exist_ok=True)
        uspro_tok.write_text("tok-uspro")
        with mock.patch.object(self.m, "gateway_env", side_effect=fake_gw_env), \
                mock.patch.dict(os.environ, {"AGY_UPSTREAM_PROXY": "", "AGY_RESIN_HOST_FROM_HOST": ""}, clear=False):
            cands_host = self.m.probe_auth_candidates({}, from_host=True)
        self.assertEqual([c[0] for c in cands_host], ["tr20x", "uspro"])
        self.assertEqual(cands_host[0][2], "http://boot.user:extractedTok99@10.0.0.5:2260")
        # Real IP 10.0.0.5 MUST NOT be rewritten to 127.0.0.1 on from_host=True!
        self.assertEqual(cands_host[1][2], "http://agy-us-east.uspro:extractedTok99@10.0.0.5:2260")

        # 5. resin_proxy_url IPv6 kept verbatim; empty AGY_RESIN_HOST_FROM_HOST falls back cleanly to 127.0.0.1:<port>
        with mock.patch.dict(os.environ, {"AGY_RESIN_HOST_FROM_HOST": ""}, clear=False):
            self.assertEqual(
                self.m.resin_proxy_url("agy-jp", "acc1", {"token": "tok123456", "host": "[2001:db8::1]:2260"}, from_host=True),
                "http://agy-jp.acc1:tok123456@[2001:db8::1]:2260",
            )
            self.assertEqual(
                self.m.resin_proxy_url("agy-jp", "acc1", {"token": "tok123456", "host": "resin:2260"}, from_host=True),
                "http://agy-jp.acc1:tok123456@127.0.0.1:2260",
            )

        # 6. Case-insensitive proxy-presets.json lookup + customProxy fallback when Resin absent
        (self.m.DATA / "proxy-presets.json").write_text(json.dumps([
            {"id": "AGY-US-EAST", "url": "http://preset-proxy:8080"},
        ]))
        acc2_tok = self.m.DATA / "accounts/acc2/.gemini/antigravity-cli/antigravity-oauth-token"
        acc2_tok.parent.mkdir(parents=True, exist_ok=True)
        acc2_tok.write_text("tok-acc2")
        (self.m.DATA / "accounts.json").write_text(json.dumps({"accounts": [
            {"id": "uspro", "enabled": True, "schedState": "active", "proxyPreset": "agy-us-east"},
            {"id": "acc2", "enabled": True, "schedState": "active", "proxyPreset": "agy-fallback",
             "customProxy": "socks5://127.0.0.1:1080"},
        ]}))
        with mock.patch.object(self.m, "gateway_env", return_value=""):
            cands = self.m.probe_auth_candidates({}, from_host=True)
        self.assertEqual([(c[0], c[2]) for c in cands], [
            ("uspro", "http://preset-proxy:8080"),
            ("acc2", "socks5://127.0.0.1:1080"),
        ])

    def test_secret_redaction_testdata_isolation_and_sigterm_cleanup(self):
        # 1. Redact user:pass@ URLs and registered resin token across log(), save_state(), publish()
        self.m.resin_proxy_url("agy-jp", "acc1", {"token": "SuperSecretResinTok42", "host": "resin:2260"}, from_host=False)
        self.m.log("proxy fail http://agy-jp.acc1:SuperSecretResinTok42@resin:2260 token=SuperSecretResinTok42")
        log_txt = (self.m.STATE / "upgrader.log").read_text()
        self.assertNotIn("SuperSecretResinTok42", log_txt)
        self.assertIn("http://agy-jp.acc1:***@resin:2260", log_txt)

        st = {
            "candidate": {"id": "c-red", "state": "failed",
                          "message": "err http://u:MyPass99@proxy:8080 raw=SuperSecretResinTok42"},
            "gates": [{"name": "G3", "ok": False, "detail": "leak SuperSecretResinTok42"}],
            "history": [],
        }
        self.m.save_state(st)
        for p in (self.m.STATE / "status.json", self.m.PUBLIC / "status.json"):
            raw = p.read_text()
            self.assertNotIn("MyPass99", raw)
            self.assertNotIn("SuperSecretResinTok42", raw)
            self.assertIn("http://u:***@proxy:8080", raw)

        # 2. SIGTERM handler raises SystemExit so test_container cleans up container, test.env, and testdata
        with self.assertRaises(SystemExit):
            self.m._handle_sigterm(self.m.signal.SIGTERM, None)

        self.m.TOKEN.parent.mkdir(parents=True, exist_ok=True)
        self.m.TOKEN.write_text("tok")
        stage = self.root / "stage-term"
        stage.mkdir()
        cand_bin = stage / "agy-acp-900k"
        cand_bin.write_text("bin")
        seen_cmds = []

        def fake_run(cmd, **kw):
            seen_cmds.append([str(x) for x in cmd])
            return types.SimpleNamespace(returncode=0, stdout="", stderr="")

        class OkResp:
            status = 200
            def __enter__(self):
                return self
            def __exit__(self, *a):
                return False

        with mock.patch.object(self.m, "run", side_effect=fake_run), \
                mock.patch.object(self.m.urllib.request, "urlopen", return_value=OkResp()):
            with self.assertRaises(SystemExit):
                with self.m.test_container(cand_bin, stage, (self.m.TOKEN, ""), data_dir=stage / "testdata-9"):
                    self.assertTrue((stage / "testdata-9").is_dir())
                    self.m._handle_sigterm(self.m.signal.SIGTERM, None)
        self.assertFalse((stage / "testdata-9").exists())
        self.assertFalse((stage / "test.env").exists())
        self.assertGreaterEqual(sum(1 for c in seen_cmds if c[:3] == ["docker", "rm", "-f"]), 2)

        # 3. live_g4 isolates testdata-0 / testdata-1 across failover attempts and cleans both up
        self._multi_accounts()
        view = {"tr20x": {"rem5h": 0.9}, "uspro": {"rem5h": 0.8}}
        seen_data_dirs = []

        @contextlib.contextmanager
        def spy_container(agy_binary, stg, auth=None, data_dir=None):
            seen_data_dirs.append(Path(data_dir).name if data_dir else None)
            yield "http://127.0.0.1:18892", "k"

        probe_seq = [
            (False, 429, "rate_limit_error", "HTTP 429 [rate_limit_error]"),
            (True, 200, "", "200 end_turn 'OK' 1.0s"),
        ]
        http_seq = [
            (200, {"stop_reason": "tool_use", "content": [{"type": "tool_use", "id": "t1"}]}),
            (200, {"stop_reason": "end_turn", "content": [{"type": "text", "text": "UPGRADE_OK"}]}),
        ]
        with mock.patch.object(self.m, "gateway_env", return_value=""), \
                mock.patch.object(self.m, "resin_settings", return_value={"token": "tok999999", "host": "resin:2260"}), \
                mock.patch.object(self.m, "test_container", side_effect=spy_container), \
                mock.patch.object(self.m, "flash_probe", side_effect=lambda *a: probe_seq.pop(0)), \
                mock.patch.object(self.m, "http_json", side_effect=lambda *a, **kw: http_seq.pop(0)), \
                mock.patch.object(self.m, "log"):
            detail = self.m.live_g4(stage, cand_bin, view)
        self.assertEqual(seen_data_dirs, ["testdata-0", "testdata-1"])
        self.assertIn("via uspro", detail)

    def test_docker_network_for_aliases_ipv6_bridge_exclusion_and_dash_guard(self):
        # IPv4, IPv6, localhost, and leading-dash host all return None without calling docker
        with mock.patch.object(self.m, "run") as run_mock:
            for url in (
                "", "http://127.0.0.1:7890", "http://10.0.0.5:2260",
                "http://[fe80::1]:2260", "http://[2001:db8::1]:2260",
                "http://localhost:2260", "http://--format:2260",
            ):
                self.assertIsNone(self.m.docker_network_for(url), url)
        run_mock.assert_not_called()

        # Excludes bridge/host/none and passes '--' before container names
        seen = []

        def fake_run_shared(cmd, **kw):
            seen.append(cmd)
            if cmd[:2] == ["docker", "inspect"]:
                self.assertIn("--", cmd)
                target = cmd[cmd.index("--") + 1]
                if target == self.m.CONTAINER:
                    return types.SimpleNamespace(returncode=0, stdout=json.dumps({"bridge": {}, "host": {}, "resin_default": {}}))
                if target == "resin":
                    return types.SimpleNamespace(returncode=0, stdout=json.dumps({"bridge": {}, "resin_default": {}}))
            return types.SimpleNamespace(returncode=1, stdout="")

        with mock.patch.object(self.m, "run", side_effect=fake_run_shared):
            self.assertEqual(self.m.docker_network_for("http://u:p@resin:2260"), "resin_default")

        # Matches Compose container alias via docker network inspect + container inspect
        def fake_run_alias(cmd, **kw):
            if cmd[:2] == ["docker", "inspect"]:
                target = cmd[cmd.index("--") + 1]
                if target == self.m.CONTAINER:
                    return types.SimpleNamespace(returncode=0, stdout=json.dumps({"net_a": {}, "net_b": {}}))
                if target == "cid-resin":
                    return types.SimpleNamespace(returncode=0, stdout=json.dumps({"net_b": {"Aliases": ["resin", "cid-resin"]}}))
                return types.SimpleNamespace(returncode=1, stdout="")
            if cmd[:3] == ["docker", "network", "inspect"]:
                net = cmd[cmd.index("--") + 1]
                if net == "net_a":
                    return types.SimpleNamespace(returncode=0, stdout=json.dumps({"cid-gw": {"Name": self.m.CONTAINER}}))
                if net == "net_b":
                    return types.SimpleNamespace(returncode=0, stdout=json.dumps({"cid-resin": {"Name": "stack-resin-1"}}))
            return types.SimpleNamespace(returncode=1, stdout="")

        with mock.patch.object(self.m, "run", side_effect=fake_run_alias):
            self.assertEqual(self.m.docker_network_for("http://u:p@resin:2260"), "net_b")

        # Sole user-defined network fallback when unambiguous
        def fake_run_sole(cmd, **kw):
            if cmd[:2] == ["docker", "inspect"] and cmd[cmd.index("--") + 1] == self.m.CONTAINER:
                return types.SimpleNamespace(returncode=0, stdout=json.dumps({"bridge": {}, "only_custom_net": {}}))
            return types.SimpleNamespace(returncode=1, stdout="")

        with mock.patch.object(self.m, "run", side_effect=fake_run_sole):
            self.assertEqual(self.m.docker_network_for("http://u:p@resin:2260"), "only_custom_net")

    def test_pool_view_monitor_trips_argon_weekly_expired_reset_unknown_rank_and_api_keys_file(self):
        now_dt = dt.datetime(2026, 11, 26, 12, 0, 0, tzinfo=dt.timezone.utc)
        future_iso = "2026-11-26T14:00:00Z"
        past_iso = "2026-11-26T10:00:00Z"

        dashboard_payload = {
            "poolRouting": {
                "poolStats": {
                    "monitorTrips": {"tr20x": future_iso},
                    "monitorBudgets": {
                        "tr20x": {"calls5h": 4900, "hardCap": 4800, "zone": "red", "tripped": True},
                        "uspro": {"calls5h": 2100, "hardCap": 3400, "zone": "yellow", "tripped": False},
                        "acc3": {"calls5h": 100, "hardCap": 3400, "zone": "green", "tripped": False},
                    },
                }
            },
            "instances": [
                {
                    "id": "tr20x",
                    "health": {"state": "monitor_tripped", "monitorTripped": True, "monitorZone": "red"},
                    "quota": {"groups": [{"name": "Gemini Models", "buckets": [{"window": "5h", "remainingFraction": 0.99, "resetTime": future_iso}]}]},
                },
                {
                    "id": "uspro",
                    "health": {"state": "healthy"},
                    "quota": {"groups": [
                        {"name": "Gemini Models", "buckets": [
                            {"window": "5h", "remainingFraction": 0.80, "resetTime": future_iso},
                            {"window": "weekly", "remainingFraction": 0.40, "resetTime": future_iso},
                        ]},
                        # Argon appears after Gemini Models -> MUST NOT overwrite rem5h!
                        {"name": "Gemini Argon", "buckets": [{"window": "5h", "remainingFraction": 0.0, "resetTime": future_iso}]},
                    ]},
                },
                {
                    "id": "acc3",
                    "health": {"state": "healthy"},
                    "quota": {"groups": [
                        # Expired resetTime <= now treated as refreshed (1.0, not exhausted)
                        {"name": "Gemini Models", "buckets": [{"window": "5h", "remainingFraction": 0.0, "resetTime": past_iso}]},
                    ]},
                },
                {
                    "id": "acc_cool",
                    "health": {"state": "cooldown", "familyCooldowns": {"gemini": 120}},
                    "quota": {"groups": [{"name": "Gemini Models", "buckets": [{"window": "5h", "remainingFraction": 0.95, "resetTime": future_iso}]}]},
                },
                {
                    "id": "acc_unknown",
                    "health": {"state": "healthy"},
                    "quota": {"groups": []},
                },
                {
                    "id": "acc_low",
                    "health": {"state": "healthy"},
                    "quota": {"groups": [{"name": "Gemini Models", "buckets": [{"window": "5h", "remainingFraction": 0.05, "resetTime": future_iso}]}]},
                },
                {
                    "id": "acc_weekly_zero",
                    "health": {"state": "healthy"},
                    "quota": {"groups": [{"name": "Gemini Models", "buckets": [
                        {"window": "5h", "remainingFraction": 0.90, "resetTime": future_iso},
                        {"window": "weekly", "remainingFraction": 0.0, "resetTime": future_iso},
                    ]}]},
                },
            ],
        }
        view = self.m.parse_pool_view(dashboard_payload, now_dt=now_dt)
        self.assertTrue(view["tr20x"]["monitorTripped"])
        self.assertEqual(view["uspro"]["rem5h"], 0.40)  # min(5h=0.80, weekly=0.40), Argon ignored
        self.assertFalse(view["uspro"]["exhausted"])
        self.assertEqual(view["acc3"]["rem5h"], 1.0)    # expired resetTime treated as fresh
        self.assertFalse(view["acc3"]["exhausted"])
        self.assertTrue(view["acc_cool"]["cooldown"])
        self.assertIsNone(view["acc_unknown"]["rem5h"])
        self.assertTrue(view["acc_weekly_zero"]["exhausted"])

        # Build accounts matching the dashboard
        self.m.TOKEN.parent.mkdir(parents=True, exist_ok=True)
        self.m.TOKEN.write_text("tok-p")
        acc_list = [{"id": "tr20x", "primary": True, "enabled": True, "schedState": "active", "proxyPreset": "direct"}]
        for aid in ("uspro", "acc3", "acc_cool", "acc_unknown", "acc_low", "acc_weekly_zero"):
            t = self.m.DATA / f"accounts/{aid}/.gemini/antigravity-cli/antigravity-oauth-token"
            t.parent.mkdir(parents=True, exist_ok=True)
            t.write_text("tok-" + aid)
            acc_list.append({"id": aid, "enabled": True, "schedState": "active", "proxyPreset": "direct"})
        (self.m.DATA / "accounts.json").write_text(json.dumps({"accounts": acc_list}))

        with mock.patch.object(self.m, "gateway_env", return_value=""):
            # Default (live_g4): tr20x (monitor_tripped/red) is SKIPPED!
            # Order: green verified (acc3 1.0 > acc_low 0.05) > green unknown (acc_unknown) > yellow (uspro) > cooldown (acc_cool) > exhausted (acc_weekly_zero)
            ids = [c[0] for c in self.m.probe_auth_candidates(view, from_host=False)]
            self.assertNotIn("tr20x", ids)
            self.assertEqual(ids, ["acc3", "acc_low", "acc_unknown", "uspro", "acc_cool", "acc_weekly_zero"])
            # With allow_monitor_tripped=True (G3 zero-gen), tr20x is strictly last
            ids_g3 = [c[0] for c in self.m.probe_auth_candidates(view, from_host=True, allow_monitor_tripped=True)]
            self.assertEqual(ids_g3[-1], "tr20x")

        # gateway_key() handles leading comma, comments, whitespace, AGY_API_KEYS_FILE, and auth-disabled
        self.assertEqual(self.m._parse_api_keys_output("\n  # comment\n , \n  valid-key-1  \nvalid-key-2\n"), "valid-key-1")
        with mock.patch.object(self.m, "run", return_value=types.SimpleNamespace(returncode=0, stdout="  # hdr\n\n  file-key-99 \n")):
            self.assertEqual(self.m.gateway_key(), "file-key-99")
        # Auth-disabled gateway: fetch_pool_view() still queries /api/dashboard without x-api-key header
        seen_headers = []

        class DashResp:
            def read(self):
                return json.dumps({"instances": [{"id": "primary", "health": {"state": "healthy"}}]}).encode()
            def __enter__(self):
                return self
            def __exit__(self, *a):
                return False

        def fake_urlopen(req, timeout=60):
            seen_headers.append(dict(req.header_items()))
            return DashResp()

        with mock.patch.object(self.m, "gateway_key", return_value=""), \
                mock.patch.object(self.m.urllib.request, "urlopen", side_effect=fake_urlopen):
            pv = self.m.fetch_pool_view()
        self.assertIn("primary", pv)
        self.assertFalse(any(k.lower() == "x-api-key" for k in seen_headers[0]))

    def test_structured_flash_probe_and_g4_512b_error_body(self):
        long_envelope = json.dumps({
            "type": "error",
            "error": {
                "type": "rate_limit_error",
                "code": "pool-monitor-tripped",
                "padding": "x" * 100,
                "message": "all accounts monitor tripped",
            },
        }).encode()
        closed = []

        class TrackedBody(io.BytesIO):
            def close(self):
                closed.append(True)
                super().close()

        def raise_429(req, timeout=180):
            raise self.m.urllib.error.HTTPError(req.full_url, 429, "Too Many Requests", {}, TrackedBody(long_envelope))

        with mock.patch.object(self.m.urllib.request, "urlopen", side_effect=raise_429):
            ok, status, err_code, detail = self.m.flash_probe("http://127.0.0.1:18890", "k", "t")
        self.assertFalse(ok)
        self.assertEqual(status, 429)
        self.assertEqual(err_code, "pool-monitor-tripped")
        self.assertIn("pool-monitor-tripped", detail)
        self.assertTrue(self.m.is_quota_failure(detail, http_status=status, err_code=err_code))
        self.assertTrue(closed)

        # _live_g4_on returns structured 4-tuple on tool step 1 HTTPError and closes error response
        closed2 = []

        class TrackedBody2(io.BytesIO):
            def close(self):
                closed2.append(True)
                super().close()

        @contextlib.contextmanager
        def fake_ctr(*a, **kw):
            yield "http://127.0.0.1:18892", "k"

        def raise_tool_429(url, body, key, **kw):
            raise self.m.urllib.error.HTTPError(url, 429, "Too Many Requests", {}, TrackedBody2(long_envelope))

        with mock.patch.object(self.m, "test_container", side_effect=fake_ctr), \
                mock.patch.object(self.m, "flash_probe", return_value=(True, 200, "", "200 end_turn 'OK' 0.5s")), \
                mock.patch.object(self.m, "http_json", side_effect=raise_tool_429):
            ok2, st2, ec2, det2 = self.m._live_g4_on(self.root, self.root / "bin", (self.m.TOKEN, ""))
        self.assertFalse(ok2)
        self.assertEqual(st2, 429)
        self.assertEqual(ec2, "pool-monitor-tripped")
        self.assertIn("tool step 1: HTTP 429", det2)
        self.assertTrue(closed2)

    def test_contract_g3_failover_on_proxy_auth_and_fail_fast_on_binary_bug(self):
        self._multi_accounts()
        view = {"tr20x": {"rem5h": 0.9}, "uspro": {"rem5h": 0.8}}
        stage = self.root / "stage-g3"
        stage.mkdir()
        (stage / "agy-acp-900k").write_text("bin")
        report = stage / "contract.json"
        seen_proxies = []

        def fake_smoke_failover(cmd, **kw):
            seen_proxies.append(kw["env"]["AGY_UPSTREAM_PROXY"])
            if len(seen_proxies) == 1:
                report.write_text(json.dumps({
                    "ok": False,
                    "checks": [
                        {"check": "initialize", "ok": True, "detail": ""},
                        {"check": "session/new", "ok": False, "detail": "407 Proxy Authentication Required"},
                    ],
                }))
                return types.SimpleNamespace(returncode=1, stdout="proxy error", stderr="")
            report.write_text(json.dumps({"ok": True, "checks": [{"check": "initialize", "ok": True}], "info": {}}))
            return types.SimpleNamespace(returncode=0, stdout="ok", stderr="")

        with mock.patch.object(self.m, "gateway_env", return_value=""), \
                mock.patch.object(self.m, "resin_settings", return_value={"token": "tok888888", "host": "resin:2260"}), \
                mock.patch.object(self.m, "run", side_effect=fake_smoke_failover), \
                mock.patch.object(self.m, "log"):
            note = self.m.contract_g3(stage, view)
        self.assertEqual(len(seen_proxies), 2)
        self.assertIn("via uspro", note)
        self.assertIn("skipped tr20x: proxy/auth", note)

        # Pure binary contract bug (initialize + session/new OK, O10 status texts failed) MUST fail fast without failover
        calls_bin = []

        def fake_smoke_binary_bug(cmd, **kw):
            calls_bin.append(1)
            report.write_text(json.dumps({
                "ok": False,
                "checks": [
                    {"check": "initialize", "ok": True, "detail": ""},
                    {"check": "session/new", "ok": True, "detail": ""},
                    {"check": "agy status texts present (O10)", "ok": False, "detail": "missing=['foo']"},
                ],
            }))
            return types.SimpleNamespace(returncode=1, stdout="contract mismatch", stderr="")

        with mock.patch.object(self.m, "gateway_env", return_value=""), \
                mock.patch.object(self.m, "resin_settings", return_value={"token": "tok888888", "host": "resin:2260"}), \
                mock.patch.object(self.m, "run", side_effect=fake_smoke_binary_bug):
            with self.assertRaisesRegex(RuntimeError, "O10"):
                self.m.contract_g3(stage, view)
        self.assertEqual(len(calls_bin), 1)

    def test_missing_rollback_target_sets_failed_instead_of_rolled_back(self):
        cand_none = {"id": "c-noprev", "kind": "agy", "version": "1.5.9", "previous": None, "state": "waiting_idle"}
        with mock.patch.object(self.m, "log"):
            self.m._rollback_agy_candidate(cand_none, "HTTP 500")
        self.assertEqual(cand_none["state"], "failed")
        self.assertIn("无法回滚 (无可用旧版本)", cand_none["message"])

        cand_missing = {"id": "c-miss", "kind": "agy", "version": "1.5.9", "previous": "1.5.4-gone", "state": "waiting_idle"}
        with mock.patch.object(self.m, "log"):
            self.m._rollback_agy_candidate(cand_missing, "HTTP 500")
        self.assertEqual(cand_missing["state"], "failed")
        self.assertIn("无法回滚 (无可用旧版本)", cand_missing["message"])

    def test_install_sh_parameterized_root_flock_atomic_swap_and_systemd_backup(self):
        import subprocess

        prod_root = self.root / "opt" / "custom-agy"
        (prod_root / "data").mkdir(parents=True)
        (prod_root / "upgrader" / "state").mkdir(parents=True)
        (prod_root / "upgrader" / "state" / "status.json").write_text('{"keep":true}\n')
        (prod_root / "upgrader" / "agy_prod_upgrader.py").write_text("# old-version\n")
        (prod_root / "upgrader" / "scripts").mkdir()
        (prod_root / "upgrader" / "scripts" / "old.py").write_text("old")

        sysd_dir = self.root / "etc-systemd"
        sysd_dir.mkdir()
        (sysd_dir / "agy-upgrader-check.service").write_text("# old-service\n")
        (sysd_dir / "agy-upgrader-probe.timer").write_text("# retired-probe\n")

        # Build a miniature valid bundle matching bundle.sh layout
        bundle = self.root / "bundle"
        (bundle / "upgrader" / "scripts").mkdir(parents=True)
        (bundle / "systemd").mkdir(parents=True)
        (bundle / "upgrader" / "agy_prod_upgrader.py").write_text("# new-version\n")
        (bundle / "upgrader" / "gateway_release.sh").write_text("#!/bin/sh\nexit 0\n")
        (bundle / "upgrader" / "scripts" / "new.py").write_text("new")
        for u in (HERE / "systemd").iterdir():
            (bundle / "systemd" / u.name).write_text(u.read_text(encoding="utf-8"), encoding="utf-8")
        (bundle / "install.sh").write_text((HERE / "install.sh").read_text(encoding="utf-8"), encoding="utf-8")
        (bundle / "install.sh").chmod(0o755)

        # Generate SHA256SUMS
        lines = []
        for f in sorted(bundle.rglob("*")):
            if f.is_file() and f.name != "SHA256SUMS":
                rel = "./" + f.relative_to(bundle).as_posix()
                lines.append(f"{self.m.sha256(f)}  {rel}")
        (bundle / "SHA256SUMS").write_text("\n".join(lines) + "\n")

        env = dict(
            os.environ,
            AGY_PROD_ROOT=str(prod_root),
            AGY_SYSTEMD_DIR=str(sysd_dir),
            AGY_SKIP_SYSTEMCTL="1",
        )
        r = subprocess.run(["sh", str(bundle / "install.sh")], env=env, capture_output=True, text=True, timeout=30)
        self.assertEqual(r.returncode, 0, msg=f"stdout={r.stdout}\nstderr={r.stderr}")

        # Verify atomic swap preserved state and updated scripts
        self.assertEqual((prod_root / "upgrader" / "state" / "status.json").read_text(), '{"keep":true}\n')
        self.assertTrue((prod_root / "upgrader" / "state" / "upgrade.lock").exists())
        self.assertEqual((prod_root / "upgrader" / "agy_prod_upgrader.py").read_text(), "# new-version\n")
        self.assertTrue((prod_root / "upgrader" / "scripts" / "new.py").exists())
        self.assertFalse((prod_root / "upgrader" / "scripts" / "old.py").exists())

        # Verify systemd units templated with custom AGY_PROD_ROOT
        apply_path = (sysd_dir / "agy-upgrader-apply.path").read_text()
        self.assertIn(f"PathExistsGlob={prod_root}/data/upgrade/approvals/*.json", apply_path)
        self.assertNotIn("/root/agy-acp-gateway", apply_path)
        apply_svc = (sysd_dir / "agy-upgrader-apply.service").read_text()
        self.assertIn(f"WorkingDirectory={prod_root}", apply_svc)
        self.assertIn(f"Environment=AGY_PROD_ROOT={prod_root}", apply_svc)
        self.assertFalse((sysd_dir / "agy-upgrader-probe.timer").exists())

        # Verify backup directory includes pid, has 0700 perms, and backs up old scripts + systemd units
        backups = list((prod_root / "backups").glob("upgrader-*-*"))
        self.assertEqual(len(backups), 1)
        bdir = backups[0]
        self.assertEqual(bdir.stat().st_mode & 0o777, 0o700)
        self.assertEqual((bdir / "agy_prod_upgrader.py").read_text(), "# old-version\n")
        self.assertEqual((bdir / "systemd" / "agy-upgrader-check.service").read_text(), "# old-service\n")
        self.assertTrue((bdir / "systemd" / "agy-upgrader-probe.timer").exists())

    def test_gateway_release_sh_tos_gate_api_keys_file_and_429_no_rollback(self):
        import http.server
        import subprocess
        import threading

        opt_root = self.root / "opt" / "gw-rel-multi"
        (opt_root / "bin").mkdir(parents=True)
        (opt_root / "Dockerfile").write_text("FROM scratch\n")
        (opt_root / "bin" / "agy-acp-gateway").write_text("old-gw")
        (opt_root / "data" / "agy" / "1.5.10-aaaa").mkdir(parents=True)
        (opt_root / "data" / "agy" / "1.5.10-aaaa" / "agy-acp-900k").write_text("agy-bin")
        (opt_root / "data" / "agy" / "current").symlink_to("1.5.10-aaaa")
        (opt_root / "data" / "tos-tripped").write_text("tripped")

        keys_file = self.root / "api.keys"
        keys_file.write_text("# comment\n  file-secret-key-123  \n")

        def make_release(name):
            rel_dir = opt_root / "releases" / name
            (rel_dir / "bin").mkdir(parents=True)
            (rel_dir / "bin" / "agy-acp-gateway").write_text("new-gw-" + name)
            gw_sha = self.m.sha256(rel_dir / "bin" / "agy-acp-gateway")
            (rel_dir / "manifest.json").write_text(json.dumps({"artifacts": {"gateway": gw_sha}}))
            rel_sh = rel_dir / "release.sh"
            rel_sh.write_text((HERE / "gateway_release.sh").read_text(encoding="utf-8"), encoding="utf-8")
            rel_sh.chmod(0o755)
            return rel_dir, gw_sha

        # 1. With data/tos-tripped and enabled primary -> MUST block release (NOT_STARTED)
        (opt_root / "data" / "accounts.json").write_text(json.dumps({"accounts": [
            {"id": "tr20x", "primary": True, "enabled": True, "schedState": "active"},
            {"id": "acc3", "enabled": True, "schedState": "active"},
        ]}))
        rel_blocked, _ = make_release("20261126-blocked")
        r_blk = subprocess.run(["sh", str(rel_blocked / "release.sh")],
                               env=dict(os.environ, AGY_PROD_ROOT=str(opt_root)),
                               capture_output=True, text=True, timeout=30)
        self.assertNotEqual(r_blk.returncode, 0)
        self.assertEqual((rel_blocked / "result").read_text().strip(), "NOT_STARTED")

        # 2. Primary disabled + child enabled + AGY_API_KEYS_FILE + live smoke HTTP 429 -> DEPLOYED_QUOTA_DEFERRED (no rollback!)
        (opt_root / "data" / "accounts.json").write_text(json.dumps({"accounts": [
            {"id": "tr20x", "primary": True, "enabled": False, "schedState": "disabled"},
            {"id": "acc3", "enabled": True, "schedState": "active"},
        ]}))
        rel_429, sha_429 = make_release("20261126-pool429")
        shim_dir = self.root / "shims-429"
        shim_dir.mkdir()
        (shim_dir / "sha256sum").write_text(
            "#!/usr/bin/env python3\nimport hashlib, pathlib, sys\n"
            "print(hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest() + '  ' + sys.argv[1])\n"
        )
        (shim_dir / "sha256sum").chmod(0o755)
        (shim_dir / "curl").write_text(
            "#!/usr/bin/env python3\nimport sys\nargs = sys.argv[1:]\n"
            "url = [a for a in args if a.startswith('http://')][-1]\n"
            "if '%{http_code}' in args: sys.stdout.write('401')\n"
            "elif url.endswith('/v1/models'): sys.stdout.write('{\"data\":[{\"id\":\"gemini-3.8-flash\"}]}')\n"
            "elif url.endswith('/v1beta/models'): sys.stdout.write('{\"models\":[{\"name\":\"models/gemini-3.8-flash\"}]}')\n"
            "elif url.endswith('/api/dashboard'): sys.stdout.write('{\"geminiPool\":{}}')\n"
            "elif url.endswith('/api/upgrade'): sys.stdout.write('{\"enabled\":true}')\n"
        )
        (shim_dir / "curl").chmod(0o755)
        (shim_dir / "docker").write_text(
            "#!/usr/bin/env python3\nimport os, subprocess, sys\nargs = sys.argv[1:]\n"
            "if args[:2] == ['run', '--rm'] and 'sha256sum' in args:\n"
            f"    print({sha_429!r} + '  /app/bin/agy-acp-gateway')\n"
            "elif args and args[0] == 'exec':\n"
            f"    env = dict(os.environ, AGY_API_KEYS='', AGY_API_KEYS_FILE={str(keys_file)!r})\n"
            "    i = 1\n"
            "    while i < len(args) and args[i] == '-e':\n"
            "        k, v = args[i + 1].split('=', 1); env[k] = v; i += 2\n"
            "    i += 1; cmd = args[i:]\n"
            "    if cmd[:2] == ['sha256sum', '/app/bin/agy-acp-gateway']:\n"
            f"        print({sha_429!r} + '  /app/bin/agy-acp-gateway')\n"
            "    else:\n"
            "        sys.exit(subprocess.run(cmd, env=env).returncode)\n"
            "elif args and args[0] == 'inspect':\n"
            "    print('startedAt=now image=local health=healthy restarts=0')\n"
        )
        (shim_dir / "docker").chmod(0o755)

        seen_api_keys = []
        resp_mode = {"status": 429, "body": {"type": "error", "error": {"type": "rate_limit_error", "code": "pool_cooldown"}}}

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                seen_api_keys.append(self.headers.get("x-api-key"))
                b = json.dumps(resp_mode["body"]).encode()
                self.send_response(resp_mode["status"])
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                self.wfile.write(b)
            def log_message(self, *a):
                pass

        srv = http.server.HTTPServer(("127.0.0.1", 0), Handler)
        port = srv.server_port
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        try:
            env_run = dict(
                os.environ,
                PATH=f"{shim_dir}:{os.environ.get('PATH', '')}",
                AGY_PROD_ROOT=str(opt_root),
                AGY_GW_PORT=str(port),
                AGY_DASH_PORT=str(port + 1),
            )
            r429 = subprocess.run(["sh", str(rel_429 / "release.sh")], env=env_run, capture_output=True, text=True, timeout=30)
            self.assertEqual(r429.returncode, 0, msg=f"stdout={r429.stdout}\nstderr={r429.stderr}")
            self.assertEqual((rel_429 / "result").read_text().strip(), "DEPLOYED_QUOTA_DEFERRED")
            self.assertEqual((opt_root / "bin" / "agy-acp-gateway").read_text(), "new-gw-20261126-pool429")
            self.assertEqual(seen_api_keys, ["file-secret-key-123"])
            smoke_doc = json.loads((rel_429 / "production-smoke.json").read_text())
            self.assertTrue(smoke_doc["quota_deferred"])
            self.assertEqual(smoke_doc["http_status"], 429)

            # apply_gateway() treats DEPLOYED_QUOTA_DEFERRED as applied
            m_opt = load(opt_root)
            cand_gw = {"id": "gw-1", "kind": "gateway", "version": "20261126-pool429",
                       "release": "20261126-pool429", "sha256": sha_429, "state": "approved"}
            with mock.patch.object(m_opt, "run", return_value=types.SimpleNamespace(returncode=0, stdout="ok", stderr="")), \
                    mock.patch.object(m_opt, "log"):
                self.assertTrue(m_opt.apply_gateway({"candidate": cand_gw, "history": []}, cand_gw))
            self.assertEqual(cand_gw["state"], "applied")

            # 3. HTTP 500 during live smoke STILL rolls back!
            rel_500, sha_500 = make_release("20261126-err500")
            (shim_dir / "docker").write_text(
                (shim_dir / "docker").read_text().replace(sha_429, sha_500)
            )
            resp_mode["status"] = 500
            resp_mode["body"] = {"error": "broken binary"}
            r500 = subprocess.run(["sh", str(rel_500 / "release.sh")], env=env_run, capture_output=True, text=True, timeout=30)
            self.assertNotEqual(r500.returncode, 0)
            self.assertEqual((rel_500 / "result").read_text().strip(), "ROLLED_BACK")
            self.assertEqual((opt_root / "bin" / "agy-acp-gateway").read_text(), "new-gw-20261126-pool429")
        finally:
            srv.shutdown()
            srv.server_close()

    def test_host_proxy_rewrite_fqdn_empty_user_redaction_null_engine_and_mid_run_sigterm(self):
        # 1. Container-sourced AGY_UPSTREAM_PROXY with single-label Docker service host (resin:2260)
        #    MUST rewrite to 127.0.0.1:2260 for from_host=True, and stay resin:2260 for from_host=False,
        #    in BOTH single-primary mode and multi-account primary direct mode!
        self.m.TOKEN.parent.mkdir(parents=True, exist_ok=True)
        self.m.TOKEN.write_text("tok-primary")

        def fake_gw_resin(name):
            return "http://agy-us.primary:tok123456@resin:2260" if name == "AGY_UPSTREAM_PROXY" else ""

        with mock.patch.object(self.m, "gateway_env", side_effect=fake_gw_resin), \
                mock.patch.dict(os.environ, {"AGY_UPSTREAM_PROXY": "", "AGY_RESIN_HOST_FROM_HOST": ""}, clear=False):
            # Single-primary mode (no accounts.json)
            sp_host = self.m.probe_auth_candidates({}, from_host=True)
            sp_ctr = self.m.probe_auth_candidates({}, from_host=False)
            self.assertEqual(sp_host[0][2], "http://agy-us.primary:tok123456@127.0.0.1:2260")
            self.assertEqual(sp_ctr[0][2], "http://agy-us.primary:tok123456@resin:2260")

            # Multi-account primary with proxyPreset="direct"
            (self.m.DATA / "accounts.json").write_text(json.dumps({"accounts": [
                {"id": "tr20x", "primary": True, "enabled": True, "schedState": "active", "proxyPreset": "direct"},
            ]}))
            mp_host = self.m.probe_auth_candidates({}, from_host=True)
            mp_ctr = self.m.probe_auth_candidates({}, from_host=False)
            self.assertEqual(mp_host[0][2], "http://agy-us.primary:tok123456@127.0.0.1:2260")
            self.assertEqual(mp_ctr[0][2], "http://agy-us.primary:tok123456@resin:2260")

        # Explicit host AGY_UPSTREAM_PROXY is NOT rewritten
        with mock.patch.object(self.m, "gateway_env", return_value=""), \
                mock.patch.dict(os.environ, {"AGY_UPSTREAM_PROXY": "http://hostproxy:8080", "AGY_RESIN_HOST_FROM_HOST": ""}, clear=False):
            hp = self.m.probe_auth_candidates({}, from_host=True)
            self.assertEqual(hp[0][2], "http://hostproxy:8080")

        # External FQDN with dot (resin.example.com:2260) is preserved when AGY_RESIN_HOST_FROM_HOST is unset,
        # and still honors explicit AGY_RESIN_HOST_FROM_HOST override!
        with mock.patch.dict(os.environ, {"AGY_UPSTREAM_PROXY": "", "AGY_RESIN_HOST_FROM_HOST": ""}, clear=False):
            self.assertEqual(
                self.m.resin_proxy_url("agy-jp", "acc1", {"token": "tok123456", "host": "resin.example.com:2260"}, from_host=True),
                "http://agy-jp.acc1:tok123456@resin.example.com:2260",
            )
            with mock.patch.object(self.m, "gateway_env", return_value="http://u:p@resin.example.com:2260"):
                self.assertEqual(self.m.upstream_proxy(from_host=True), "http://u:p@resin.example.com:2260")
        with mock.patch.dict(os.environ, {"AGY_UPSTREAM_PROXY": "", "AGY_RESIN_HOST_FROM_HOST": "127.0.0.1:12260"}, clear=False):
            self.assertEqual(
                self.m.resin_proxy_url("agy-jp", "acc1", {"token": "tok123456", "host": "resin.example.com:2260"}, from_host=True),
                "http://agy-jp.acc1:tok123456@127.0.0.1:12260",
            )
            with mock.patch.object(self.m, "gateway_env", return_value="http://u:p@resin.example.com:2260"):
                self.assertEqual(self.m.upstream_proxy(from_host=True), "http://u:p@127.0.0.1:12260")

        # 2. Empty-username proxy URL redaction: http://:secret@host:2260 -> http://:***@host:2260
        red = self.m.redact_secrets("err http://:secretpass@host:2260 ok")
        self.assertNotIn("secretpass", red)
        self.assertIn("http://:***@host:2260", red)

        # 3. JSON null values for running/current in finish_agy() do not raise TypeError
        agy = self.m.AGY_DIR
        for v in ("1.5.5-aaaa", "1.5.6-bbbbbbbb"):
            (agy / v).mkdir(parents=True, exist_ok=True)
            (agy / v / "agy-acp-900k").write_text(v)
        self.m.switch_current(agy / "1.5.6-bbbbbbbb")
        cand = {"id": "c-null", "kind": "agy", "version": "1.5.6", "sha256": "b" * 64,
                "state": "waiting_idle", "installed": "1.5.6-bbbbbbbb", "previous": "1.5.5-aaaa"}
        null_eng = {"alive": False, "running": None, "current": "/data/agy/1.5.6-bbbbbbbb/agy-acp-900k"}
        with mock.patch.object(self.m, "gateway_key", return_value="k"), \
                mock.patch.object(self.m, "engine_info", return_value=null_eng), \
                mock.patch.object(self.m, "flash_probe", return_value=(False, 429, "pool_cooldown", "HTTP 429")), \
                mock.patch.object(self.m, "log"):
            self.assertTrue(self.m.finish_agy({"candidate": cand}, cand, wait=0))
        self.assertEqual(cand["state"], "verify_deferred")

        # 4. Mid-'docker run' SIGTERM still removes TEST_CONTAINER, test.env, and testdata
        stage = self.root / "stage-mid-term"
        stage.mkdir()
        cand_bin = stage / "agy-acp-900k"
        cand_bin.write_text("bin")
        seen_cmds = []

        def sigterm_during_docker_run(cmd, **kw):
            seen_cmds.append([str(x) for x in cmd])
            if len(cmd) >= 2 and cmd[0] == "docker" and cmd[1] == "run":
                self.m._handle_sigterm(self.m.signal.SIGTERM, None)
            return types.SimpleNamespace(returncode=0, stdout="", stderr="")

        with mock.patch.object(self.m, "run", side_effect=sigterm_during_docker_run):
            with self.assertRaises(SystemExit):
                with self.m.test_container(cand_bin, stage, (self.m.TOKEN, ""), data_dir=stage / "testdata-mid"):
                    pass
        self.assertFalse((stage / "test.env").exists())
        self.assertFalse((stage / "testdata-mid").exists())
        self.assertEqual(sum(1 for c in seen_cmds if c[:3] == ["docker", "rm", "-f"]), 2)

    @contextlib.contextmanager
    def _env_without(self, *names):
        saved = {k: os.environ.pop(k) for k in names if k in os.environ}
        try:
            yield
        finally:
            for k in names:
                os.environ.pop(k, None)
            os.environ.update(saved)

    def _setup_agy_symlink(self, dir_name, content="bin"):
        d = self.m.AGY_DIR / dir_name
        d.mkdir(parents=True, exist_ok=True)
        (d / "agy-acp-900k").write_text(content)
        self.m.switch_current(d)
        return d

    def test_blocked_agy_versions_default_override_and_custom_parsing(self):
        with self._env_without("AGY_BLOCKED_VERSIONS"):
            default_map = self.m.blocked_agy_versions()
            self.assertEqual(default_map, self.m.DEFAULT_BLOCKED_AGY_VERSIONS)
            self.assertIn("1.5.17", default_map)
            self.assertIn("1.5.18", default_map)
            self.assertIn("agy#1890", default_map["1.5.17"])
            self.assertIn("agy#1872", default_map["1.5.18"])
            self.assertNotIn("1.5.16", default_map)
            self.assertNotIn("1.5.19", default_map)
            # Mutating returned dict must not mutate module constant
            default_map["9.9.9"] = "mutated"
            self.assertNotIn("9.9.9", self.m.DEFAULT_BLOCKED_AGY_VERSIONS)

        for disable_val in ("", "   ", "none", "NONE", " None "):
            with mock.patch.dict(os.environ, {"AGY_BLOCKED_VERSIONS": disable_val}, clear=False):
                self.assertEqual(self.m.blocked_agy_versions(), {}, repr(disable_val))

        with mock.patch.dict(os.environ, {"AGY_BLOCKED_VERSIONS": "1.5.17, 1.5.18"}, clear=False):
            parsed = self.m.blocked_agy_versions()
            self.assertEqual(set(parsed.keys()), {"1.5.17", "1.5.18"})
            self.assertIn("agy#1890", parsed["1.5.17"])
            self.assertIn("agy#1890", parsed["1.5.18"])

        with mock.patch.dict(
            os.environ,
            {"AGY_BLOCKED_VERSIONS": "1.5.19:test, v1.5.20:custom:colon , 1.5.21, 1.5.22:, :orphan, ,"},
            clear=False,
        ):
            custom = self.m.blocked_agy_versions()
            self.assertEqual(
                custom,
                {
                    "1.5.19": "test",
                    "1.5.20": "custom:colon",
                    "1.5.21": "blocked by AGY_BLOCKED_VERSIONS",
                    "1.5.22": "blocked by AGY_BLOCKED_VERSIONS",
                },
            )

    def test_cmd_check_skips_blocked_1517_and_1518_without_download_or_gates_and_clears_stale_ready(self):
        self._setup_agy_symlink("1.5.16-aaaa1111", "agy-1516")

        for blocked_ver in ("1.5.17", "1.5.18"):
            # Pre-seed an old ready candidate for the blocked version to verify cmd_check clears it!
            self.m.save_state({
                "candidate": {
                    "id": f"agy-{blocked_ver}-stale",
                    "kind": "agy",
                    "version": blocked_ver,
                    "sha256": "a" * 64,
                    "state": "ready",
                    "stage": str(self.root / f"stage-{blocked_ver}"),
                },
                "gates": [{"name": "G4 真实验收", "ok": True, "detail": "old"}],
                "history": [],
            })
            for force_flag in (False, True):
                with self._env_without("AGY_BLOCKED_VERSIONS"), \
                        mock.patch.object(self.m, "latest_release", return_value={"tag_name": blocked_ver}), \
                        mock.patch.object(self.m, "download_g1") as dl_mock, \
                        mock.patch.object(self.m, "patch_g2") as p2_mock, \
                        mock.patch.object(self.m, "contract_g3") as g3_mock, \
                        mock.patch.object(self.m, "live_g4") as g4_mock, \
                        mock.patch.object(self.m, "fetch_pool_view") as pv_mock:
                    rc = self.m.cmd_check(types.SimpleNamespace(force=force_flag))
                self.assertEqual(rc, 0)
                dl_mock.assert_not_called()
                p2_mock.assert_not_called()
                g3_mock.assert_not_called()
                g4_mock.assert_not_called()
                pv_mock.assert_not_called()

                for status_path in (self.m.STATE / "status.json", self.m.PUBLIC / "status.json"):
                    doc = json.loads(status_path.read_text())
                    self.assertEqual(doc["current"]["agy"], "1.5.16")
                    self.assertIsNone(doc["candidate"])
                    self.assertEqual(doc["gates"], [])
                    self.assertIn(f"skipping blocked agy {blocked_ver}", doc.get("lastCheckNote", ""))
                    self.assertIn("agy#1890", doc.get("lastCheckNote", ""))

    def test_cmd_check_allows_unblocked_1519_and_honors_env_override_and_custom_block(self):
        self._setup_agy_symlink("1.5.16-aaaa1111", "agy-1516")

        def fake_patch(stage):
            (stage / "agy-acp-900k").write_text("patched-bin")
            return "patched ok"

        # 1. Unblocked 1.5.19 proceeds normally even if an old blocked 1.5.18 ready candidate was present.
        self.m.save_state({
            "candidate": {
                "id": "agy-1.5.18-oldready",
                "kind": "agy",
                "version": "1.5.18",
                "sha256": "e" * 64,
                "state": "ready",
            },
            "gates": [],
            "lastCheckNote": "skipping blocked agy 1.5.18: old",
            "history": [],
        })
        with self._env_without("AGY_BLOCKED_VERSIONS"), \
                mock.patch.object(self.m, "latest_release", return_value={"tag_name": "1.5.19"}), \
                mock.patch.object(self.m, "fetch_pool_view", return_value={}), \
                mock.patch.object(self.m, "download_g1", return_value="g1 ok") as dl_mock, \
                mock.patch.object(self.m, "patch_g2", side_effect=fake_patch) as p2_mock, \
                mock.patch.object(self.m, "contract_g3", return_value="g3 ok") as g3_mock, \
                mock.patch.object(self.m, "live_g4", return_value="g4 ok") as g4_mock:
            rc = self.m.cmd_check(types.SimpleNamespace(force=False))
        self.assertEqual(rc, 0)
        dl_mock.assert_called_once()
        p2_mock.assert_called_once()
        g3_mock.assert_called_once()
        g4_mock.assert_called_once()
        st = self.m.load_state()
        self.assertEqual(st["candidate"]["version"], "1.5.19")
        self.assertEqual(st["candidate"]["state"], "ready")
        self.assertNotIn("lastCheckNote", st)

        # 2. AGY_BLOCKED_VERSIONS="" disables the default blocklist -> 1.5.18 proceeds normally.
        self.m.save_state({"candidate": None, "gates": [], "history": []})
        with mock.patch.dict(os.environ, {"AGY_BLOCKED_VERSIONS": ""}, clear=False), \
                mock.patch.object(self.m, "latest_release", return_value={"tag_name": "1.5.18"}), \
                mock.patch.object(self.m, "fetch_pool_view", return_value={}), \
                mock.patch.object(self.m, "download_g1", return_value="g1 ok") as dl_ov, \
                mock.patch.object(self.m, "patch_g2", side_effect=fake_patch), \
                mock.patch.object(self.m, "contract_g3", return_value="g3 ok"), \
                mock.patch.object(self.m, "live_g4", return_value="g4 ok"):
            rc_ov = self.m.cmd_check(types.SimpleNamespace(force=False))
        self.assertEqual(rc_ov, 0)
        dl_ov.assert_called_once()
        self.assertEqual(self.m.load_state()["candidate"]["version"], "1.5.18")
        self.assertEqual(self.m.load_state()["candidate"]["state"], "ready")

        # 3. Custom AGY_BLOCKED_VERSIONS="1.5.19:test" blocks 1.5.19 without running download/gates.
        self.m.save_state({"candidate": None, "gates": [], "history": []})
        with mock.patch.dict(os.environ, {"AGY_BLOCKED_VERSIONS": "1.5.19:test"}, clear=False), \
                mock.patch.object(self.m, "latest_release", return_value={"tag_name": "1.5.19"}), \
                mock.patch.object(self.m, "download_g1") as dl_custom, \
                mock.patch.object(self.m, "live_g4") as g4_custom:
            rc_custom = self.m.cmd_check(types.SimpleNamespace(force=False))
        self.assertEqual(rc_custom, 0)
        dl_custom.assert_not_called()
        g4_custom.assert_not_called()
        st_custom = self.m.load_state()
        self.assertIsNone(st_custom["candidate"])
        self.assertEqual(st_custom["lastCheckNote"], "skipping blocked agy 1.5.19: test")

    def test_external_symlink_rollback_reconciles_applied_and_waiting_idle_state(self):
        self._setup_agy_symlink("1.5.18-bbbb2222", "agy-1518")
        self.m.save_state({
            "candidate": {
                "id": "agy-1.5.18-20261126T060000Z",
                "kind": "agy",
                "version": "1.5.18",
                "sha256": self.m.sha256(self.m.AGY_DIR / "1.5.18-bbbb2222/agy-acp-900k"),
                "state": "applied",
                "installed": "1.5.18-bbbb2222",
                "previous": "1.5.16-aaaa1111",
                "message": "agy 1.5.18 已生效",
            },
            "gates": [{"name": "G4 真实验收", "ok": True, "detail": "ok"}],
            "history": [],
        })
        self.assertEqual(self.m.load_state()["current"]["agy"], "1.5.18")

        # Operator manually rolls data/agy/current symlink back from 1.5.18 -> 1.5.16
        d16 = self._setup_agy_symlink("1.5.16-aaaa1111", "agy-1516")
        sha16 = self.m.sha256(d16 / "agy-acp-900k")

        # 1. current_state() immediately reflects live 1.5.16 symlink and reconciles stale applied 1.5.18
        with self._env_without("AGY_BLOCKED_VERSIONS"):
            live = self.m.current_state()
        self.assertEqual(live["current"]["agy"], "1.5.16")
        self.assertEqual(live["current"]["agySha"], sha16)
        self.assertIsNone(live["candidate"])

        # 2. cmd_check() reflects actual 1.5.16, clears stale applied 1.5.18, and skips blocked 1.5.18
        with self._env_without("AGY_BLOCKED_VERSIONS"), \
                mock.patch.object(self.m, "latest_release", return_value={"tag_name": "1.5.18"}), \
                mock.patch.object(self.m, "download_g1") as dl_mock, \
                mock.patch.object(self.m, "live_g4") as g4_mock:
            rc = self.m.cmd_check(types.SimpleNamespace(force=False))
        self.assertEqual(rc, 0)
        dl_mock.assert_not_called()
        g4_mock.assert_not_called()
        saved = self.m.load_state()
        self.assertEqual(saved["current"]["agy"], "1.5.16")
        self.assertEqual(saved["current"]["agySha"], sha16)
        self.assertIsNone(saved["candidate"])
        self.assertIn("skipping blocked agy 1.5.18", saved["lastCheckNote"])

        # 3. External rollback while candidate was waiting_idle must NOT probe or alter symlink in cmd_apply/cmd_check
        self.m.save_state({
            "candidate": {
                "id": "agy-1.5.18-wait",
                "kind": "agy",
                "version": "1.5.18",
                "sha256": "b" * 64,
                "state": "waiting_idle",
                "installed": "1.5.18-bbbb2222",
                "previous": "1.5.15-0000",
            },
            "gates": [],
            "history": [],
        })
        with self._env_without("AGY_BLOCKED_VERSIONS"), \
                mock.patch.object(self.m, "finish_agy") as fin_mock:
            rc_apply = self.m.cmd_apply(types.SimpleNamespace())
        self.assertEqual(rc_apply, 0)
        fin_mock.assert_not_called()
        self.assertEqual(os.readlink(self.m.AGY_DIR / "current"), "1.5.16-aaaa1111")
        self.assertIsNone(self.m.load_state()["candidate"])

    def test_cmd_apply_refuses_blocked_candidate_and_allows_when_override_unblocks(self):
        self._setup_agy_symlink("1.5.16-aaaa1111", "agy-1516")
        stage = self.root / "stage-1518"
        stage.mkdir()
        (stage / "agy-acp-900k").write_text("agy-1518-bin")
        sha = self.m.sha256(stage / "agy-acp-900k")

        def seed_ready(ver="1.5.18", cid="agy-1.5.18-cand"):
            self.m.save_state({
                "candidate": {
                    "id": cid,
                    "kind": "agy",
                    "version": ver,
                    "sha256": sha,
                    "state": "ready",
                    "stage": str(stage),
                },
                "gates": [],
                "history": [],
            })
            self.approve(cid, sha)

        # 1. Default blocklist refuses applying 1.5.18, drains approval, keeps symlink on 1.5.16
        seed_ready("1.5.18", "agy-1.5.18-blocked")
        with self._env_without("AGY_BLOCKED_VERSIONS"), \
                mock.patch.object(self.m, "apply_agy") as apply_mock:
            rc = self.m.cmd_apply(types.SimpleNamespace())
        self.assertEqual(rc, 1)
        apply_mock.assert_not_called()
        self.assertEqual(os.readlink(self.m.AGY_DIR / "current"), "1.5.16-aaaa1111")
        self.assertEqual(list((self.m.PUBLIC / "approvals").glob("*.json")), [])
        st = self.m.load_state()
        self.assertEqual(st["candidate"]["state"], "failed")
        self.assertIn("refusing to apply blocked agy 1.5.18", st["candidate"]["message"])
        self.assertIn("agy#1890", st["candidate"]["message"])
        self.assertEqual(st["history"][0]["state"], "failed")

        # 2. Custom AGY_BLOCKED_VERSIONS="1.5.19:test" also refuses applying 1.5.19
        seed_ready("1.5.19", "agy-1.5.19-blocked")
        with mock.patch.dict(os.environ, {"AGY_BLOCKED_VERSIONS": "1.5.19:test"}, clear=False), \
                mock.patch.object(self.m, "apply_agy") as apply_mock2:
            rc2 = self.m.cmd_apply(types.SimpleNamespace())
        self.assertEqual(rc2, 1)
        apply_mock2.assert_not_called()
        self.assertIn("refusing to apply blocked agy 1.5.19: test", self.m.load_state()["candidate"]["message"])

        # 3. Direct apply_agy() call is also fail-closed for blocked versions
        with self._env_without("AGY_BLOCKED_VERSIONS"):
            with self.assertRaisesRegex(RuntimeError, "refusing to apply blocked agy 1.5.18"):
                self.m.apply_agy(
                    {"candidate": None},
                    {"id": "direct-1518", "kind": "agy", "version": "1.5.18", "sha256": sha, "stage": str(stage)},
                )
        self.assertEqual(os.readlink(self.m.AGY_DIR / "current"), "1.5.16-aaaa1111")

        # 4. When AGY_BLOCKED_VERSIONS="" (or "none") unblocks 1.5.18, cmd_apply proceeds normally
        seed_ready("1.5.18", "agy-1.5.18-override")
        with mock.patch.dict(os.environ, {"AGY_BLOCKED_VERSIONS": "none"}, clear=False), \
                mock.patch.object(self.m, "apply_agy", return_value=True) as apply_ok:
            rc_ok = self.m.cmd_apply(types.SimpleNamespace())
        self.assertEqual(rc_ok, 0)
        apply_ok.assert_called_once()

    def test_patch_g2_invokes_acp_only_and_checks_extra_patches(self):
        stage = self.root / "stage-g2"
        stage.mkdir()
        (stage / "agy-official").write_bytes(b"official-bin")
        seen_cmds = []

        def fake_run(cmd, **kw):
            seen_cmds.append([str(c) for c in cmd])
            return types.SimpleNamespace(returncode=0, stdout="", stderr="")

        with mock.patch.object(self.m, "run", side_effect=fake_run):
            detail = self.m.patch_g2(stage)
        self.assertIn("acp-only", detail)
        self.assertEqual(len(seen_cmds), 3)
        self.assertTrue(seen_cmds[0][1].endswith("third_party/agy-context-unlock/agy-patch.py"))
        self.assertIn("--acp-only", seen_cmds[0])
        self.assertIn("--in-place", seen_cmds[0])
        self.assertIn("--no-env-fix", seen_cmds[0])
        self.assertIn("--no-backup", seen_cmds[0])
        self.assertTrue(seen_cmds[1][1].endswith("tools/agy_extra_patch.py"))
        self.assertIn("--in-place", seen_cmds[1])
        self.assertTrue(seen_cmds[2][1].endswith("tools/agy_extra_patch.py"))
        self.assertIn("--check", seen_cmds[2])

    def test_prune_installed_agy_protects_all_required_dirs_and_prunes_older_and_blocked(self):
        agy = self.m.AGY_DIR
        agy.mkdir(parents=True, exist_ok=True)
        self.m.PUBLIC.mkdir(parents=True, exist_ok=True)

        def mk_agy(name, mtime, *, with_bin=True):
            d = agy / name
            d.mkdir(parents=True, exist_ok=True)
            if with_bin:
                (d / "agy-acp-900k").write_text(name)
            os.utime(d, (mtime, mtime))
            return d

        # Older valid dirs, protected dirs, newer valid dirs, incomplete dir, and blocked dirs
        d_oldest_prune = mk_agy("1.5.10-10101010", 1_700_000_100)
        d_prev_protect = mk_agy("1.5.11-11111111", 1_700_000_200)
        d_running_prot = mk_agy("1.5.12-12121212", 1_700_000_300)
        d_current_prot = mk_agy("1.5.13-13131313", 1_700_000_400)
        d_mid_prune = mk_agy("1.5.14-14141414", 1_700_000_500)
        d_keep_2 = mk_agy("1.5.15-15151515", 1_700_000_600)
        d_keep_1 = mk_agy("1.5.16-16161616", 1_700_000_700)
        d_installed_pr = mk_agy("1.5.19-19191919", 1_700_000_250)
        d_incomplete = mk_agy("1.5.16-deadbeef", 1_700_000_950, with_bin=False)
        d_blocked_17 = mk_agy("1.5.17-17171717", 1_700_000_800)
        d_blocked_18 = mk_agy("1.5.18-18181818", 1_700_000_900)

        # Unrelated dir + symlinked dir pointing outside AGY_DIR must NEVER be touched
        non_matching = agy / "manual-backup"
        non_matching.mkdir()
        (non_matching / "keep.txt").write_text("safe")
        external_dir = self.root / "external-sensitive"
        external_dir.mkdir()
        (external_dir / "secret.txt").write_text("do-not-delete")
        symlink_escape = agy / "1.5.9-99999999"
        symlink_escape.symlink_to(external_dir)

        self.m.switch_current(d_current_prot)
        (self.m.PUBLIC / "engine.json").write_text(json.dumps({
            "alive": True,
            "running": f"/data/agy/{d_running_prot.name}/agy-acp-900k",
            "current": f"/data/agy/{d_current_prot.name}/agy-acp-900k",
        }))
        st = {
            "candidate": {
                "id": "agy-1.5.19-cand",
                "kind": "agy",
                "version": "1.5.19",
                "state": "applied",
                "previous": d_prev_protect.name,
                "installed": d_installed_pr.name,
            },
            "history": [],
        }

        with self._env_without("AGY_BLOCKED_VERSIONS"):
            removed = self.m.prune_installed_agy(st, keep=2)

        self.assertEqual(
            sorted(removed),
            sorted([d_oldest_prune.name, d_mid_prune.name, d_incomplete.name, d_blocked_17.name, d_blocked_18.name]),
        )
        # Protected dirs MUST survive
        for kept in (d_prev_protect, d_running_prot, d_current_prot, d_installed_pr, d_keep_1, d_keep_2, non_matching):
            self.assertTrue(kept.is_dir(), f"expected {kept.name} to be protected")
        # current symlink & symlink escape target MUST remain intact
        self.assertTrue((agy / "current").is_symlink())
        self.assertEqual(os.readlink(agy / "current"), d_current_prot.name)
        self.assertTrue(symlink_escape.is_symlink())
        self.assertTrue((external_dir / "secret.txt").is_file())

        # Blocked version that IS current or running must STILL be protected (never delete active binary!)
        d_blk_active = mk_agy("1.5.18-aaaa1111", 1_700_001_000)
        d_blk_running = mk_agy("1.5.17-bbbb2222", 1_700_001_100)
        self.m.switch_current(d_blk_active)
        (self.m.PUBLIC / "engine.json").write_text(json.dumps({
            "alive": True,
            "running": f"/data/agy/{d_blk_running.name}/agy-acp-900k",
        }))
        with self._env_without("AGY_BLOCKED_VERSIONS"):
            self.m.prune_installed_agy({"candidate": None}, keep=2)
        self.assertTrue(d_blk_active.is_dir())
        self.assertTrue(d_blk_running.is_dir())
        self.assertTrue(d_keep_1.is_dir())
        self.assertTrue(d_keep_2.is_dir())
        # Old previously-protected dirs are now pruned since they are no longer current/running/cand
        self.assertFalse(d_prev_protect.exists())
        self.assertFalse(d_running_prot.exists())

    def test_prune_installed_agy_fails_safe_on_corrupt_engine_json_and_runs_in_check_and_finish(self):
        agy = self.m.AGY_DIR
        agy.mkdir(parents=True, exist_ok=True)
        self.m.PUBLIC.mkdir(parents=True, exist_ok=True)

        dirs = []
        for idx, name in enumerate(("1.5.13-13131313", "1.5.14-14141414", "1.5.15-15151515", "1.5.16-16161616")):
            d = agy / name
            d.mkdir(parents=True, exist_ok=True)
            (d / "agy-acp-900k").write_text(name)
            ts = 1_700_000_000 + idx * 100
            os.utime(d, (ts, ts))
            dirs.append(d)
        self.m.switch_current(dirs[-1])

        # 1. Corrupt engine.json -> fail safe, prune nothing
        (self.m.PUBLIC / "engine.json").write_text("{corrupt-json")
        self.assertEqual(self.m.prune_installed_agy({"candidate": None}, keep=2), [])
        for d in dirs:
            self.assertTrue(d.is_dir())

        # 2. Valid engine.json + cmd_check() -> prunes older 1.5.13 & 1.5.14 while keeping 1.5.15 & 1.5.16
        (self.m.PUBLIC / "engine.json").write_text(json.dumps({
            "alive": True,
            "running": f"/data/agy/{dirs[-1].name}/agy-acp-900k",
        }))
        with self._env_without("AGY_BLOCKED_VERSIONS"), \
                mock.patch.object(self.m, "latest_release", return_value={"tag_name": "1.5.16"}):
            rc = self.m.cmd_check(types.SimpleNamespace(force=False))
        self.assertEqual(rc, 0)
        self.assertFalse(dirs[0].exists())
        self.assertFalse(dirs[1].exists())
        self.assertTrue(dirs[2].is_dir())
        self.assertTrue(dirs[3].is_dir())

        # 3. finish_agy() reaching applied triggers prune_installed_agy (keeping previous + current)
        d_old_extra = agy / "1.5.12-12121212"
        d_old_extra.mkdir()
        (d_old_extra / "agy-acp-900k").write_text("old")
        os.utime(d_old_extra, (1_699_000_000, 1_699_000_000))
        d_new = agy / "1.5.19-19191919"
        d_new.mkdir()
        (d_new / "agy-acp-900k").write_text("new")
        os.utime(d_new, (1_700_005_000, 1_700_005_000))
        self.m.switch_current(d_new)
        (self.m.PUBLIC / "engine.json").write_text(json.dumps({
            "alive": True,
            "running": f"/data/agy/{d_new.name}/agy-acp-900k",
        }))
        cand = {
            "id": "agy-1.5.19-fin",
            "kind": "agy",
            "version": "1.5.19",
            "state": "waiting_idle",
            "installed": d_new.name,
            "previous": dirs[3].name,
        }
        st = {"candidate": cand, "history": []}
        with self._env_without("AGY_BLOCKED_VERSIONS"), \
                mock.patch.object(self.m, "gateway_key", return_value="k"), \
                mock.patch.object(self.m, "engine_info", return_value={"alive": True, "running": f"/data/agy/{d_new.name}/agy-acp-900k"}), \
                mock.patch.object(self.m, "flash_probe", return_value=(True, 200, "", "200 end_turn OK")):
            self.assertTrue(self.m.finish_agy(st, cand, wait=0))
        self.assertEqual(cand["state"], "applied")
        self.assertFalse(d_old_extra.exists())
        self.assertTrue(d_new.is_dir())
        self.assertTrue(dirs[3].is_dir())

    def test_prune_installed_agy_protects_all_multi_account_workers_and_fails_safe_on_symlink_public(self):
        agy = self.m.AGY_DIR
        agy.mkdir(parents=True, exist_ok=True)
        self.m.PUBLIC.mkdir(parents=True, exist_ok=True)

        def mk_agy(name, mtime):
            d = agy / name
            d.mkdir(parents=True, exist_ok=True)
            (d / "agy-acp-900k").write_text(name)
            os.utime(d, (mtime, mtime))
            return d

        # Multi-account rolling rollback scenario:
        # current symlink is 1.5.16, worker acc1 still runs blocked 1.5.18 (top-level running),
        # worker acc2 still runs blocked 1.5.17, worker acc3 still runs older valid 1.5.12,
        # while unused 1.5.11 and unused blocked 1.5.18-deadbeef should be pruned.
        d_unused_old = mk_agy("1.5.11-11111111", 1_700_000_100)
        d_worker_12 = mk_agy("1.5.12-12121212", 1_700_000_200)
        d_keep_15 = mk_agy("1.5.15-15151515", 1_700_000_300)
        d_current_16 = mk_agy("1.5.16-16161616", 1_700_000_400)
        d_worker_17 = mk_agy("1.5.17-17171717", 1_700_000_500)
        d_worker_18 = mk_agy("1.5.18-18181818", 1_700_000_600)
        d_unused_blk = mk_agy("1.5.18-deadbeef", 1_700_000_700)

        self.m.switch_current(d_current_16)
        (self.m.PUBLIC / "engine.json").write_text(json.dumps({
            "alive": True,
            "running": f"/data/agy/{d_worker_18.name}/agy-acp-900k",
            "current": f"/data/agy/{d_current_16.name}/agy-acp-900k",
            "workers": [
                {"id": "primary", "alive": True, "running": f"/data/agy/{d_current_16.name}/agy-acp-900k"},
                {"id": "acc1", "alive": True, "running": f"/data/agy/{d_worker_18.name}/agy-acp-900k"},
                {"id": "acc2", "alive": True, "running": f"/data/agy/{d_worker_17.name}/agy-acp-900k"},
                {"id": "acc3", "alive": True, "running": f"/data/agy/{d_worker_12.name}/agy-acp-900k"},
            ],
        }))

        with self._env_without("AGY_BLOCKED_VERSIONS"):
            removed = self.m.prune_installed_agy({"candidate": None}, keep=2)

        self.assertEqual(sorted(removed), sorted([d_unused_old.name, d_unused_blk.name]))
        for kept in (d_worker_12, d_keep_15, d_current_16, d_worker_17, d_worker_18):
            self.assertTrue(kept.is_dir(), f"expected multi-account worker dir {kept.name} to survive")

        # Symlinked PUBLIC (data/upgrade) MUST fail safe and prune nothing
        d_extra = mk_agy("1.5.10-10101010", 1_699_000_000)
        real_pub = self.root / "real-upgrade"
        self.m.PUBLIC.rename(real_pub)
        self.m.PUBLIC.symlink_to(real_pub)
        try:
            self.assertEqual(self.m.prune_installed_agy({"candidate": None}, keep=2), [])
            self.assertTrue(d_extra.is_dir())
        finally:
            self.m.PUBLIC.unlink()
            real_pub.rename(self.m.PUBLIC)


if __name__ == "__main__":
    sys.exit(unittest.main())
