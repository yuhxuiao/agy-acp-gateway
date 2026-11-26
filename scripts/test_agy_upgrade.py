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
        self.root = Path(self.temp.name).resolve()
        self.stage = self.root/"candidate"
        self.stage.mkdir()
        self.manifest = {"schema": 2, "source_digest": "source-1", "artifacts": {}, "checks": {}}
        for key, name in upgrade.FILES.items():
            p = self.stage/name
            p.write_bytes(("candidate-"+key).encode())
            self.manifest["artifacts"][key] = upgrade.sha(p)
        for name in ("offline", "fake_e2e", "contract", "live_flash"):
            result = {"ok": True, "model": probe.MODEL, "generation_requests": 1 if name == "live_flash" else 0}
            if name == "fake_e2e":
                result["scenarios"] = {n: {"ok": True} for n in probe.FAKE_SCENARIOS}
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

    def test_fake_gate_requires_every_behavioral_scenario(self):
        self.assertEqual(tuple(upgrade.FAKE_E2E_SCENARIOS), tuple(probe.FAKE_SCENARIOS))
        for drop in probe.FAKE_SCENARIOS:
            with self.subTest(missing=drop):
                result = {"ok": True, "model": probe.MODEL, "generation_requests": 0,
                          "scenarios": {n: {"ok": n != drop} for n in probe.FAKE_SCENARIOS}}
                p = self.stage/"fake_e2e.json"
                upgrade.write_json(p, result)
                self.manifest["checks"]["fake_e2e"]["sha256"] = upgrade.sha(p)
                self.save()
                with self.assertRaises(ValueError):
                    upgrade.validate_manifest(self.stage)

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
            for _ in range(40):
                if upgrade.process_identity(child) is None:
                    break
                time.sleep(0.05)
            else:
                self.fail("owned descendant survived cleanup")
        finally:
            parent.stdout.close()
            if parent.poll() is None:
                parent.kill()
                parent.wait(5)
            try:
                os.kill(child, signal.SIGKILL)
            except ProcessLookupError:
                pass

    def test_extra_patch_trajectory_session_id_spec_v2(self) -> None:
        import importlib.util
        import pathlib
        import struct
        import types

        repo_root = pathlib.Path(__file__).resolve().parent.parent
        extra_path = repo_root / "tools" / "agy_extra_patch.py"
        spec = importlib.util.spec_from_file_location("agy_extra_patch_under_test", extra_path)
        self.assertIsNotNone(spec)
        self.assertIsNotNone(spec.loader)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        self.assertEqual(mod.SPEC_VERSION, 5)
        up = mod.load_upstream()

        # --- 1. Synthetic x86-64 tests: vanilla -> patch -> idempotent + %rsi liveness fail-closed ---
        text_va = 0x400000
        text_off = 0x100
        traj_disp = 0x350
        params_disp = 0x238
        callee_rel = 0x200
        callee_va = text_va + callee_rel
        helper_rel = 0x500
        helper_va = text_va + helper_rel

        anchor = (
            b"\x48\x8b\x8c\x24" + struct.pack("<I", traj_disp)
            + b"\x48\x85\xc9\x74\x05\x8b\x51\x18\xeb\x02\x31\xd2"
            + b"\x48\x8d\x84\x24" + struct.pack("<I", params_disp)
        )
        zero_fill = b"\x44\x0f\x11\x38\x44\x0f\x11\x78\x10"
        other_copy = (
            b"\x48\x8b\x8c\x24" + struct.pack("<I", 0x2e0)
            + b"\x48\x89\x8c\x24" + struct.pack("<I", params_disp + 0x20)
        )
        vanilla_sid_copy = (
            b"\x48\x8b\x8c\x24" + struct.pack("<I", 0x330)
            + b"\x48\x89\x8c\x24" + struct.pack("<I", params_disp + 0x60)
            + b"\x48\x8b\x8c\x24" + struct.pack("<I", 0x328)
            + b"\x48\x89\x8c\x24" + struct.pack("<I", params_disp + 0x58)
        )
        # Post-copy stack store (reads %rcx/%rsp, not %rsi) followed by CALL to 1-arg helper(&params in %rax)
        post_prefix = b"\x90" * 8 + anchor + zero_fill + other_copy + vanilla_sid_copy
        post_store = b"\x0f\xb6\x8c\x24\x40\x03\x00\x00\x88\x8c\x24\xf4\x02\x00\x00"
        call_site_rel = len(post_prefix) + len(post_store)
        call_helper = b"\xe8" + struct.pack("<i", helper_va - (callee_va + call_site_rel + 5))
        callee_body = post_prefix + post_store + call_helper + b"\xc3"

        # 1-arg Go helper: cmp 0x10(%r14),%rsp ; jbe +8 ; mov (%rax),%rcx ; ret ; morestack: mov %rax,8(%rsp) ; ret
        helper_body = b"\x49\x3b\x66\x10\x76\x04\x48\x8b\x08\xc3\x48\x89\x44\x24\x08\xc3"

        caller_rel = 0x600
        caller_va = text_va + caller_rel
        zero_sid_caller = b"\x44\x0f\x11\x7c\x24\x50"
        call_instr_rel = 32
        rel32 = callee_va - (caller_va + call_instr_rel + 5)
        caller_body = (
            b"\x90" * (call_instr_rel - len(zero_sid_caller))
            + zero_sid_caller
            + b"\xe8" + struct.pack("<i", rel32)
            + b"\xc3"
        )

        ckpt_rel = 0x800
        ckpt_body = bytes.fromhex("49 3b 66 10 90 90 31 c0 31 db 31 c9 c9 c3")

        def make_x86_buf(c_body=callee_body, h_body=helper_body):
            b = bytearray(0x1000)
            b[text_off + callee_rel:text_off + callee_rel + len(c_body)] = c_body
            b[text_off + helper_rel:text_off + helper_rel + len(h_body)] = h_body
            b[text_off + caller_rel:text_off + caller_rel + len(caller_body)] = caller_body
            b[text_off + ckpt_rel:text_off + ckpt_rel + len(ckpt_body)] = ckpt_body
            return b, types.SimpleNamespace(
                find_text_section=lambda _d: (text_va, text_off, "x86-64"),
                build_functions=lambda _d: {
                    "checkpoint.shouldCheckpoint": (ckpt_rel, len(ckpt_body)),
                    mod.TRAJ_CALLEE_FUNC: (callee_rel, len(c_body)),
                    mod.TRAJ_CALLER_FUNC: (caller_rel, len(caller_body)),
                },
            )

        buf, fake_up = make_x86_buf()
        arch, res = mod.inspect(buf, fake_up)
        self.assertEqual(arch, "x86-64")
        by_id = {p["id"]: (layout, off, va, st) for p, layout, off, va, st in res}
        self.assertEqual(by_id["codeassist-per-trajectory-session-id"][3], "vanilla")
        layout, off, va, _ = by_id["codeassist-per-trajectory-session-id"]
        buf[off:off + len(layout["patched"])] = layout["patched"]

        _, res2 = mod.inspect(buf, fake_up)
        by_id2 = {p["id"]: st for p, _l, _o, _v, st in res2}
        self.assertEqual(by_id2["codeassist-per-trajectory-session-id"], "patched")

        # Fail-closed when caller sessionID zeroing disappears
        mutated = bytearray(buf)
        bad_idx = bytes(mutated).index(zero_sid_caller)
        mutated[bad_idx:bad_idx + 2] = b"\x90\x90"
        with self.assertRaises(mod.ExtraPatchError):
            mod.inspect(mutated, fake_up)

        # Fail-closed when post-site instruction reads %rsi (`mov %rsi, 0x70(%rax)`) before overwrite/call
        rsi_read_body = post_prefix + b"\x48\x89\x70\x70" + b"\xc3"
        buf_bad_rsi, up_bad_rsi = make_x86_buf(c_body=rsi_read_body)
        with self.assertRaisesRegex(mod.ExtraPatchError, "reads %rsi"):
            mod.inspect(buf_bad_rsi, up_bad_rsi)

        # Succeeds when %esi is explicitly killed (`xor %esi, %esi`) before reading %rsi
        rsi_killed_body = post_prefix + b"\x31\xf6\x48\x89\x70\x70\xc3"
        buf_ok_rsi, up_ok_rsi = make_x86_buf(c_body=rsi_killed_body)
        self.assertEqual(mod.inspect(buf_ok_rsi, up_ok_rsi)[0], "x86-64")

        # Fail-closed when CALL target spills %rsi in its Go morestack prologue (5+ arg callee)
        helper_5arg = b"\x49\x3b\x66\x10\x76\x04\x48\x8b\x08\xc3\x48\x89\x74\x24\x28\xc3"
        buf_bad_callee, up_bad_callee = make_x86_buf(h_body=helper_5arg)
        with self.assertRaisesRegex(mod.ExtraPatchError, "reads %rsi argument"):
            mod.inspect(buf_bad_callee, up_bad_callee)

        # --- 2. Synthetic arm64 tests: 5-word coalesced (1.5.10), 6-word coalesced (1.5.9), + fail-closed ---
        for p_disp, use_sp_stp in ((0x148, True), (0x248, False)):
            with self.subTest(arm64_params_disp=hex(p_disp)):
                t_disp = 0x2F8
                a_words = [
                    0xF94003E0 | ((t_disp // 8) << 10),
                    0xB4000060,
                    0xB9801801,
                    0x14000002,
                    0xAA1F03E1,
                    0x910003E0 | (p_disp << 10),
                ]
                zeros = [0xA9007C1F | (((i * 0x10) // 8) << 15) for i in range(8)]
                if use_sp_stp:
                    pair0 = [0xF941A3E2, 0xF941A7E3, 0xA9000FE2 | ((p_disp // 8) << 15)]
                    sid_tail = [
                        0xF941CFE2,
                        0xF9416BE3,
                        0xA9000FE2 | (((p_disp + 0x50) // 8) << 15),
                        0xF9416FE2,
                        0xF90003E2 | (((p_disp + 0x60) // 8) << 10),
                    ]
                else:
                    pair0 = [0xF941BBE2, 0xF941BFE3, 0x910003FB | (p_disp << 10), 0xA9000F62]
                    sid_tail = [
                        0xF941E7E2,
                        0xF94183E3,
                        0x910003FB | ((p_disp + 0x50) << 10),
                        0xA9000F62,
                        0xF94187E2,
                        0xF90003E2 | (((p_disp + 0x60) // 8) << 10),
                    ]
                arm_callee_words = [0xD503201F] + a_words + zeros + pair0 + sid_tail + [0x394BC3E2, 0x94000010, 0xD65F03C0]
                arm_callee = struct.pack(f"<{len(arm_callee_words)}I", *arm_callee_words)

                bl_word = up.encode_bl(caller_va + 8, callee_va)
                arm_caller = struct.pack("<4I", 0xD503201F, 0xA9027FFF, bl_word, 0xD65F03C0)
                arm_ckpt = bytes.fromhex("90 0b 40 f9 1f 20 03 d5 e0 03 1f aa e1 03 1f aa e2 03 1f aa c0 03 5f d6")

                abuf = bytearray(0x1200)
                abuf[text_off + callee_rel:text_off + callee_rel + len(arm_callee)] = arm_callee
                abuf[text_off + caller_rel:text_off + caller_rel + len(arm_caller)] = caller_caller_bytes = arm_caller
                abuf[text_off + ckpt_rel:text_off + ckpt_rel + len(arm_ckpt)] = arm_ckpt
                arm_up = types.SimpleNamespace(
                    find_text_section=lambda _d: (text_va, text_off, "arm64"),
                    build_functions=lambda _d, c_len=len(arm_callee), k_len=len(arm_ckpt): {
                        "checkpoint.shouldCheckpoint": (ckpt_rel, k_len),
                        mod.TRAJ_CALLEE_FUNC: (callee_rel, c_len),
                        mod.TRAJ_CALLER_FUNC: (caller_rel, len(caller_caller_bytes)),
                    },
                    decode_bl_target=up.decode_bl_target,
                )

                a_arch, a_res = mod.inspect(abuf, arm_up)
                self.assertEqual(a_arch, "arm64")
                for p_item, l_item, o_item, _v, st_item in a_res:
                    self.assertEqual(st_item, "vanilla")
                    abuf[o_item:o_item + len(l_item["patched"])] = l_item["patched"]
                _, a_res2 = mod.inspect(abuf, arm_up)
                for _p, _l, _o, _v, st_item in a_res2:
                    self.assertEqual(st_item, "patched")

                # Fail-closed when zero-fill omits 0x60 (`SessionID.len`)
                zeros_no_60 = [0xA9007C1F | (((i * 0x10) // 8) << 15) for i in range(6)]
                no60_words = [0xD503201F] + a_words + zeros_no_60 + pair0 + sid_tail + [0x394BC3E2, 0x94000010, 0xD65F03C0]
                no60_callee = struct.pack(f"<{len(no60_words)}I", *no60_words)
                abuf_no60 = bytearray(abuf)
                abuf_no60[text_off + callee_rel:text_off + callee_rel + len(no60_callee)] = no60_callee
                arm_up_no60 = types.SimpleNamespace(
                    find_text_section=lambda _d: (text_va, text_off, "arm64"),
                    build_functions=lambda _d, c_len=len(no60_callee), k_len=len(arm_ckpt): {
                        "checkpoint.shouldCheckpoint": (ckpt_rel, k_len),
                        mod.TRAJ_CALLEE_FUNC: (callee_rel, c_len),
                        mod.TRAJ_CALLER_FUNC: (caller_rel, len(caller_caller_bytes)),
                    },
                    decode_bl_target=up.decode_bl_target,
                )
                with self.assertRaisesRegex(mod.ExtraPatchError, "0x50/0x60"):
                    mod.inspect(abuf_no60, arm_up_no60)

                # Fail-closed on post-site x16 read (`str x16, [sp, #0x10]`)
                bad_words = [0xD503201F] + a_words + zeros + pair0 + sid_tail + [0xF9000BF0, 0x94000010]
                bad_callee = struct.pack(f"<{len(bad_words)}I", *bad_words)
                abuf[text_off + callee_rel:text_off + callee_rel + len(bad_callee)] = bad_callee
                with self.assertRaisesRegex(mod.ExtraPatchError, "reads scratch x16"):
                    mod.inspect(abuf, arm_up)

        # --- 3. Verify main(--in-place) delegates signing/writing to up.write_atomic_signed ---
        bin_file = self.root / "synthetic-agy.bin"
        fresh_buf, fresh_up = make_x86_buf()
        bin_file.write_bytes(fresh_buf)
        signed_calls = []

        def fake_write_signed(dst, data, src_for_entitlements=None, make_backup=False):
            signed_calls.append((dst, src_for_entitlements, make_backup))
            Path(dst).write_bytes(data)

        fresh_up.write_atomic_signed = fake_write_signed
        with patch.object(mod, "load_upstream", return_value=fresh_up), \
                patch.object(sys, "argv", ["agy_extra_patch.py", "--binary", str(bin_file), "--in-place"]):
            self.assertEqual(mod.main(), 0)
        self.assertEqual(signed_calls, [(str(bin_file), str(bin_file), False)])

        # --- 3b. Synthetic agy >= 1.5.14 native SessionID pipeline (1.5.14, 1.5.15-1.5.16 & 1.5.17+, x86-64 & arm64 + fail-closed) ---
        def make_native_buf(arch_name, *, profiles=("1.5.17+",), drop_sym=None, break_edge=None):
            nb = bytearray(0x4000)
            nfuncs = {}
            if arch_name == "x86-64":
                n_ckpt = bytes.fromhex("49 3b 66 10 90 90 31 c0 31 db 31 c9 c9 c3")
            else:
                n_ckpt = struct.pack("<5I", 0xF9400B90, 0xAA1F03E0, 0xAA1F03E1, 0xAA1F03E2, 0xD65F03C0)
            nfuncs["checkpoint.shouldCheckpoint"] = (0x100, len(n_ckpt))
            nb[text_off + 0x100:text_off + 0x100 + len(n_ckpt)] = n_ckpt

            ordered_syms = list(mod.NATIVE_SESSION_ID_COMMON_FUNCS)
            active_edges = list(mod.NATIVE_SESSION_ID_COMMON_CALL_EDGES)
            for pname in profiles:
                prof = mod.NATIVE_SESSION_ID_PROFILES[pname]
                for sym in prof["funcs"]:
                    if sym not in ordered_syms:
                        ordered_syms.append(sym)
                for edge in prof["call_edges"]:
                    if edge not in active_edges:
                        active_edges.append(edge)

            layout_offsets = {}
            next_rel = 0x200
            fn_span = 0x80
            for sym in ordered_syms:
                layout_offsets[sym] = next_rel
                next_rel += fn_span
                if sym == drop_sym:
                    continue
                nfuncs[sym] = (layout_offsets[sym], fn_span)
                rel = layout_offsets[sym]
                nb[text_off + rel:text_off + rel + fn_span] = (
                    (b"\x90" * fn_span)
                    if arch_name == "x86-64"
                    else (struct.pack("<I", 0xD503201F) * (fn_span // 4))
                )

            edge_slot = {}
            for caller, callee in active_edges:
                if (caller, callee) == break_edge or caller not in nfuncs or callee not in nfuncs:
                    continue
                c_rel = layout_offsets[caller]
                t_rel = layout_offsets[callee]
                slot = edge_slot.get(caller, 0)
                edge_slot[caller] = slot + 1
                at = c_rel + 4 + slot * 8
                if arch_name == "x86-64":
                    rel32 = (text_va + t_rel) - (text_va + at + 5)
                    nb[text_off + at:text_off + at + 5] = b"\xe8" + struct.pack("<i", rel32)
                else:
                    imm26 = ((t_rel - at) // 4) & 0x3FFFFFF
                    struct.pack_into("<I", nb, text_off + at, 0x94000000 | imm26)

            return nb, types.SimpleNamespace(
                find_text_section=lambda _d: (text_va, text_off, arch_name),
                build_functions=lambda _d: dict(nfuncs),
                write_atomic_signed=fake_write_signed,
            )

        for pname, prof in mod.NATIVE_SESSION_ID_PROFILES.items():
            overlap = set(prof["funcs"]) & set(mod.NATIVE_SESSION_ID_COMMON_FUNCS)
            self.assertEqual(overlap, set(), f"profile {pname} duplicates common symbols: {overlap}")

        all_profiles = ("1.5.14", "1.5.15-1.5.16", "1.5.17+")
        for native_arch in ("x86-64", "arm64"):
            # Future 1.5.19+ renamed internal helpers (no known profile matches, only core invariants) -> native!
            no_prof_buf, no_prof_up = make_native_buf(native_arch, profiles=())
            _, core_res = mod.inspect(no_prof_buf, no_prof_up)
            core_map = {p["id"]: (layout, st) for p, layout, _o, _v, st in core_res}
            self.assertEqual(core_map["disable-internal-checkpoint"][1], "vanilla")
            self.assertEqual(core_map["codeassist-per-trajectory-session-id"][1], "native")
            self.assertEqual(
                core_map["codeassist-per-trajectory-session-id"][0]["note"],
                "native upstream ccpa.HashSessionID [core-invariants]",
            )

            # Future 1.5.19+ explicit renamed helper (openStreamGenerateContent -> openStreamGenerateContentV2)
            # passes as native while still failing closed if any core symbol/edge or checkpoint layout breaks.
            ren_buf, ren_up = make_native_buf(native_arch, profiles=("1.5.17+",))
            ren_funcs_orig = ren_up.build_functions(ren_buf)
            ren_funcs_orig["codeassistclient.(*CodeAssistClient).openStreamGenerateContentV2"] = ren_funcs_orig.pop(
                "codeassistclient.(*CodeAssistClient).openStreamGenerateContentWithOptions"
            )
            ren_funcs_orig.pop("codeassistclient.(*CodeAssistClient).openCCPAContinuationStream", None)
            ren_up_v19 = types.SimpleNamespace(
                find_text_section=ren_up.find_text_section,
                build_functions=lambda _d, rf=ren_funcs_orig: dict(rf),
                write_atomic_signed=fake_write_signed,
            )
            _, ren_res = mod.inspect(ren_buf, ren_up_v19)
            ren_map = {p["id"]: (layout, st) for p, layout, _o, _v, st in ren_res}
            self.assertEqual(ren_map["codeassist-per-trajectory-session-id"][1], "native")
            self.assertEqual(
                ren_map["codeassist-per-trajectory-session-id"][0]["note"],
                "native upstream ccpa.HashSessionID [core-invariants]",
            )

            # disable-internal-checkpoint MUST remain strictly fail-closed even on native 1.5.19+
            bad_ckpt_buf = bytearray(ren_buf)
            bad_ckpt_buf[text_off + 0x100:text_off + 0x104] = b"\x00\x00\x00\x00"
            with self.assertRaisesRegex(mod.ExtraPatchError, "disable-internal-checkpoint"):
                mod.inspect(bad_ckpt_buf, ren_up_v19)

            for prof_name in all_profiles:
                with self.subTest(arch=native_arch, profile=prof_name):
                    nbuf, nup = make_native_buf(native_arch, profiles=(prof_name,))
                    _, nres = mod.inspect(nbuf, nup)
                    nmap = {p["id"]: (layout, st) for p, layout, _o, _v, st in nres}
                    self.assertEqual(nmap["disable-internal-checkpoint"][1], "vanilla")
                    self.assertEqual(nmap["codeassist-per-trajectory-session-id"][1], "native")
                    self.assertEqual(
                        nmap["codeassist-per-trajectory-session-id"][0]["note"],
                        f"native upstream ccpa.HashSessionID [{prof_name}]",
                    )

                    safe_tag = prof_name.replace(".", "").replace("+", "plus")
                    nfile = self.root / f"synthetic-{safe_tag}-{native_arch}.bin"
                    nfile.write_bytes(nbuf)
                    with patch.object(mod, "load_upstream", return_value=nup):
                        with patch.object(sys, "argv", ["agy_extra_patch.py", "--binary", str(nfile), "--check"]):
                            self.assertEqual(mod.main(), 1)
                        with patch.object(sys, "argv", ["agy_extra_patch.py", "--binary", str(nfile), "--dry-run"]):
                            self.assertEqual(mod.main(), 0)
                        self.assertEqual(nfile.read_bytes(), bytes(nbuf))
                        with patch.object(sys, "argv", ["agy_extra_patch.py", "--binary", str(nfile), "--in-place"]):
                            self.assertEqual(mod.main(), 0)
                        with patch.object(sys, "argv", ["agy_extra_patch.py", "--binary", str(nfile), "--check"]):
                            self.assertEqual(mod.main(), 0)

                    # Fail-closed when ANY core invariant symbol is missing
                    for common_sym in mod.NATIVE_SESSION_ID_COMMON_FUNCS:
                        bad_sym_buf, bad_sym_up = make_native_buf(
                            native_arch,
                            profiles=(prof_name,),
                            drop_sym=common_sym,
                        )
                        with self.assertRaisesRegex(mod.ExtraPatchError, "native pipeline incomplete"):
                            mod.inspect(bad_sym_buf, bad_sym_up)

                    # Fail-closed when ANY core invariant call edge is broken
                    for core_edge in mod.NATIVE_SESSION_ID_COMMON_CALL_EDGES:
                        bad_edge_buf, bad_edge_up = make_native_buf(
                            native_arch,
                            profiles=(prof_name,),
                            break_edge=core_edge,
                        )
                        with self.assertRaisesRegex(mod.ExtraPatchError, "common call-edge verification failed"):
                            mod.inspect(bad_edge_buf, bad_edge_up)

                    # Dropping a non-core profile-only helper degrades gracefully to [core-invariants]
                    prof_only_sym = mod.NATIVE_SESSION_ID_PROFILES[prof_name]["funcs"][0]
                    deg_buf, deg_up = make_native_buf(
                        native_arch,
                        profiles=(prof_name,),
                        drop_sym=prof_only_sym,
                    )
                    _, deg_res = mod.inspect(deg_buf, deg_up)
                    deg_map = {p["id"]: (layout, st) for p, layout, _o, _v, st in deg_res}
                    self.assertEqual(deg_map["codeassist-per-trajectory-session-id"][1], "native")
                    self.assertEqual(
                        deg_map["codeassist-per-trajectory-session-id"][0]["note"],
                        "native upstream ccpa.HashSessionID [core-invariants]",
                    )

        # --- 4. Real binary verification (strictly read-only in memory) if present ---
        for real_bin in (repo_root / "bin" / "agy-acp-900k", pathlib.Path.home() / ".local" / "bin" / "agy"):
            if real_bin.is_file():
                raw = bytearray(real_bin.read_bytes())
                _, real_res = mod.inspect(raw, up)
                real_map = {p["id"]: (layout, off, va, st) for p, layout, off, va, st in real_res}
                self.assertIn(real_map["disable-internal-checkpoint"][3], ("vanilla", "patched"))
                self.assertIn(real_map["codeassist-per-trajectory-session-id"][3], ("vanilla", "patched", "native"))
                for p, l, o, _v, st in real_res:
                    if st == "vanilla":
                        raw[o:o + len(l["patched"])] = l["patched"]
                _, patched_res = mod.inspect(raw, up)
                for p, _l, _o, _v, st in patched_res:
                    self.assertIn(st, ("patched", "native"), f"{p['id']} expected patched or native")

    def test_gw_sh_start_ticks_handles_comm_spaces_parens_and_missing_proc(self):
        # Verify the exact sed+awk pipeline used by scripts/gw.sh start_ticks()
        stat_line = "4242 (agy worker (v2)) S 1 4242 4242 0 -1 4194304 10 0 0 0 5 2 0 0 20 0 4 0 987654321 12345 100\n"
        out = subprocess.check_output(
            ["bash", "-euo", "pipefail", "-c", "printf '%s' \"$1\" | sed 's/^.*)[[:space:]]*//' | awk '{print $20}'", "_", stat_line],
            text=True,
        ).strip()
        self.assertEqual(out, "987654321")
        gw_src = (Path(__file__).resolve().parent / "gw.sh").read_text()
        self.assertIn("sed 's/^.*)[[:space:]]*//'", gw_src)

    def test_upstream_watch_manifest_fallback_multiarch_and_temp_cleanup(self):
        import hashlib
        import io
        import tarfile

        self.assertEqual(watch.resolve_platform("Linux", "x86_64")[0], "linux_amd64")
        self.assertEqual(watch.resolve_platform("Linux", "aarch64")[0], "linux_arm64")
        self.assertEqual(watch.resolve_platform("Darwin", "arm64")[0], "darwin_arm64")
        with self.assertRaises(ValueError):
            watch.resolve_platform("Windows", "x86_64")

        mf = {
            "source": "manifest",
            "manifest_key": "darwin_arm64",
            "tag_name": "1.5.10",
            "published_at": "1794894071",
            "html_url": watch.GCS_PREFIX + "1.5.10/darwin_arm64/cli_mac_arm64.tar.gz",
            "url": watch.GCS_PREFIX + "1.5.10/darwin_arm64/cli_mac_arm64.tar.gz",
            "sha512": "a" * 128,
        }
        with patch.object(watch, "fetch_github_release", side_effect=RuntimeError("rate limit")), \
                patch.object(watch, "fetch_manifest_release", return_value=mf):
            self.assertEqual(watch.latest_official()["tag_name"], "1.5.10")

        with patch.object(watch, "fetch_github_release", return_value={"tag_name": "1.5.6", "assets": []}), \
                patch.object(watch, "fetch_manifest_release", return_value=mf):
            self.assertEqual(watch.latest_official()["source"], "manifest")

        # Build valid & mismatched archives to verify SHA-512 check and temp-file cleanup on failure
        tar_buf = io.BytesIO()
        script = b"#!/bin/sh\necho 1.5.10\n"
        with tarfile.open(fileobj=tar_buf, mode="w:gz") as tar:
            ti = tarfile.TarInfo("agy")
            ti.size = len(script)
            ti.mode = 0o755
            tar.addfile(ti, io.BytesIO(script))
        tar_bytes = tar_buf.getvalue()
        good_512 = hashlib.sha512(tar_bytes).hexdigest()

        state_dir = self.root / "watch-state"
        with patch.object(watch, "STATE", state_dir), \
                patch.object(watch, "_stream_download", side_effect=lambda _u, tmp: tmp.write_bytes(tar_bytes)):
            with self.assertRaisesRegex(ValueError, "SHA-512 mismatch"):
                watch.download_official(dict(mf, sha512="b" * 128))
            dl_dir = state_dir / "downloads/1.5.10"
            self.assertEqual(list(dl_dir.glob("*.download")), [])

            with patch.object(watch, "version", return_value="9.9.9"):
                with self.assertRaisesRegex(ValueError, "unexpected version"):
                    watch.download_official(dict(mf, sha512=good_512))
            self.assertFalse((dl_dir / "agy.extracting").exists())

            with patch.object(watch, "version", return_value="1.5.10"):
                out_bin = watch.download_official(dict(mf, sha512=good_512))
            self.assertTrue(out_bin.is_file())

    def test_patch_agy_900k_and_probe_scripts_hardening(self):
        import io
        import patch_agy_900k
        import probe_models
        import probe_acp_caps
        import probe_resume_vs_load

        # 1. patch_agy_900k non-verify mode fails closed without writing target
        src_dummy = self.root / "dummy-src"
        dst_dummy = self.root / "dummy-dst"
        src_dummy.write_bytes(b"vanilla")
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(patch_agy_900k.main(["--patch", str(src_dummy), str(dst_dummy)]), 2)
        self.assertFalse(dst_dummy.exists())

        # 2. probe_models.build_probe_env honors empty vs custom AGY_UPSTREAM_PROXY
        with patch.dict(os.environ, {"AGY_UPSTREAM_PROXY": "", "HTTPS_PROXY": "http://stale:1", "HTTP_PROXY": "http://stale:1"}):
            env_direct = probe_models.build_probe_env(self.root)
        self.assertNotIn("HTTPS_PROXY", env_direct)
        self.assertNotIn("HTTP_PROXY", env_direct)

        with patch.dict(os.environ, {"AGY_UPSTREAM_PROXY": "http://127.0.0.1:8899"}):
            env_proxied = probe_models.build_probe_env(self.root)
        self.assertEqual(env_proxied["HTTPS_PROXY"], "http://127.0.0.1:8899")

        # 3. probe_acp_caps dry-runs with zero subprocesses unless --live is given
        buf = io.StringIO()
        with patch("subprocess.Popen") as popen, contextlib.redirect_stdout(buf):
            self.assertEqual(probe_acp_caps.main([]), 0)
            popen.assert_not_called()
        self.assertEqual(json.loads(buf.getvalue())["mode"], "dry-run")

        # 4. probe_resume_vs_load stages an isolated copy and rejects path traversal sessionIds
        src_conv = self.root / "src-convs"
        src_conv.mkdir()
        (src_conv / "sess-abc-1.db").write_bytes(b"sqlite-bytes")
        iso_home = self.root / "iso-home"
        probe_resume_vs_load.stage_session_copy("sess-abc-1", src_conv, iso_home)
        self.assertEqual((iso_home / "antigravity-cli/conversations/sess-abc-1.db").read_bytes(), b"sqlite-bytes")
        with self.assertRaises(ValueError):
            probe_resume_vs_load.stage_session_copy("../escape", src_conv, iso_home)

    def test_prepare_auth_link_and_proxy_detection(self):
        fake_home = self.root / "fake-home"
        cli_home = fake_home / ".gemini/antigravity-cli"
        cli_home.mkdir(parents=True)
        (cli_home / "conversations").mkdir()

        # Case 1: missing token & installation_id -> RuntimeError
        with patch("agy_upgrade.Path.home", return_value=fake_home):
            with self.assertRaisesRegex(RuntimeError, "existing official CLI login is unavailable"):
                upgrade.prepare_auth_link(self.root / "pure-missing")

        # Case 2: macOS Keychain profile without antigravity-oauth-token links installation_id + onboarding only
        (cli_home / "installation_id").write_text("inst-id")
        (cli_home / "cache").mkdir()
        (cli_home / "cache/onboarding.json").write_text("{}")
        darwin_pure = self.root / "pure-darwin"
        with patch("agy_upgrade.Path.home", return_value=fake_home), patch("agy_upgrade.sys.platform", "darwin"):
            upgrade.prepare_auth_link(darwin_pure)
        self.assertEqual((darwin_pure / "antigravity-cli/installation_id").resolve(), (cli_home / "installation_id").resolve())
        self.assertEqual((darwin_pure / "antigravity-cli/cache/onboarding.json").resolve(), (cli_home / "cache/onboarding.json").resolve())
        self.assertFalse((darwin_pure / "antigravity-cli/conversations").exists())

        # Case 3: token file links directly
        tok = cli_home / "antigravity-oauth-token"
        tok.write_text("tok")
        tok_pure = self.root / "pure-token"
        with patch("agy_upgrade.Path.home", return_value=fake_home), patch("agy_upgrade.sys.platform", "linux"):
            upgrade.prepare_auth_link(tok_pure)
        self.assertEqual((tok_pure / "antigravity-cli/antigravity-oauth-token").resolve(), tok.resolve())
        self.assertEqual((tok_pure / "antigravity-cli/installation_id").resolve(), (cli_home / "installation_id").resolve())
        self.assertEqual((tok_pure / "antigravity-cli/cache/onboarding.json").resolve(), (cli_home / "cache/onboarding.json").resolve())

        # Case 4: AGY_UPSTREAM_PROXY="" overrides default proxy probe
        with patch.dict(os.environ, {"AGY_UPSTREAM_PROXY": ""}):
            self.assertEqual(upgrade.default_upstream_proxy(), "")


    def test_agy_extra_patch_tail_calls_and_source_digest_json_templates(self):
        import importlib.util
        import struct

        repo_root = Path(__file__).resolve().parents[1]
        extra_path = repo_root / "tools" / "agy_extra_patch.py"
        spec = importlib.util.spec_from_file_location("agy_extra_patch_tail_test", extra_path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        # 1. x86-64 JMP rel32 (0xE9) tail-call edge (forward and backward)
        buf_x86 = bytearray(b"\x90" * 256)
        rel_fwd = 0x1080 - (0x1010 + 5)
        buf_x86[0x10:0x15] = b"\xe9" + struct.pack("<i", rel_fwd)
        self.assertTrue(mod._func_calls_target(buf_x86, 0x1000, 0, "x86-64", 0x0, 0x40, 0x1080))
        self.assertFalse(mod._func_calls_target(buf_x86, 0x1000, 0, "x86-64", 0x0, 0x40, 0x1084))

        rel_back = 0x1004 - (0x1090 + 5)
        buf_x86[0x90:0x95] = b"\xe9" + struct.pack("<i", rel_back)
        self.assertTrue(mod._func_calls_target(buf_x86, 0x1000, 0, "x86-64", 0x80, 0x40, 0x1004))

        # 2. arm64 unconditional B imm26 (0x14000000) tail-call edge (forward and backward)
        buf_arm = bytearray(b"\x1f\x20\x03\xd5" * 64)
        imm_fwd = ((0x1080 - 0x1010) // 4) & 0x3FFFFFF
        struct.pack_into("<I", buf_arm, 0x10, 0x14000000 | imm_fwd)
        self.assertTrue(mod._func_calls_target(buf_arm, 0x1000, 0, "arm64", 0x0, 0x40, 0x1080))
        self.assertFalse(mod._func_calls_target(buf_arm, 0x1000, 0, "arm64", 0x0, 0x40, 0x1084))

        imm_back = ((0x1008 - 0x1094) // 4) & 0x3FFFFFF
        struct.pack_into("<I", buf_arm, 0x94, 0x14000000 | imm_back)
        self.assertTrue(mod._func_calls_target(buf_arm, 0x1000, 0, "arm64", 0x80, 0x40, 0x1008))

        # 3. source_digest tracks scripts/*.json (e.g. agy_notice_templates.json)
        fake_repo = self.root / "digest-repo"
        for sub in ("go-gateway/webui", "go-gateway/testdata", "scripts", "tools", "third_party/agy-context-unlock"):
            (fake_repo / sub).mkdir(parents=True)
        (fake_repo / "go-gateway/go.mod").write_text("module test\n")
        (fake_repo / "go-gateway/go.sum").write_text("")
        (fake_repo / "third_party/agy-context-unlock/agy-patch.py").write_text("# patch\n")
        tpl_json = fake_repo / "scripts/agy_notice_templates.json"
        tpl_json.write_text('{"version":"1.5.13"}\n')
        d1 = SOURCE_DIGEST(fake_repo)
        tpl_json.write_text('{"version":"1.5.14"}\n')
        d2 = SOURCE_DIGEST(fake_repo)
        self.assertNotEqual(d1, d2)

    def test_agy_patch_acp_only_and_native_self_skip_and_fail_closed(self):
        import importlib.util
        import io
        import struct

        repo_root = Path(__file__).resolve().parents[1]
        patch_path = repo_root / "third_party" / "agy-context-unlock" / "agy-patch.py"
        spec = importlib.util.spec_from_file_location("agy_patch_under_test", patch_path)
        up = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(up)

        text_va = 0x400000
        text_off = 0x100

        # --- 1. Gate 3A native self-skip vs unexpected smaller cap (x86-64 & arm64) ---
        # x86-64 native: has 40000 (0x9c40) & 16384 (0x4000) budget markers, NO 256K/sub-900K clamp
        g3a_x64_native = (
            b"\x90" * 16
            + b"\x48\x81\xff" + struct.pack("<I", 40000)
            + b"\x90" * 16
            + b"\x81\xfa" + struct.pack("<I", 16384)
            + b"\x90" * 32
        )
        buf = bytearray(0x800)
        buf[text_off + 0x100:text_off + 0x100 + len(g3a_x64_native)] = g3a_x64_native
        funcs_3a = {up.FUNC_CHECKPOINT: (0x100, len(g3a_x64_native))}
        changed, msg = up.patch_gate3a_checkpoint(buf, funcs_3a, text_va, text_off, "x86-64")
        self.assertFalse(changed)
        self.assertIn("native", msg)

        # x86-64 unexpected smaller cap (512000 = 0x7d000) -> MUST fail closed!
        g3a_x64_cap512 = (
            b"\x90" * 16
            + b"\x48\x81\xff" + struct.pack("<I", 40000)
            + b"\x81\xfa" + struct.pack("<I", 512000)
            + b"\xbe" + struct.pack("<I", 512000) + b"\x0f\x42\xf2"
            + b"\x90" * 32
        )
        buf2 = bytearray(0x800)
        buf2[text_off + 0x100:text_off + 0x100 + len(g3a_x64_cap512)] = g3a_x64_cap512
        with self.assertRaisesRegex(up.PatchError, "unexpected smaller cap 512000"):
            up.patch_gate3a_checkpoint(buf2, {up.FUNC_CHECKPOINT: (0x100, len(g3a_x64_cap512))}, text_va, text_off, "x86-64")

        # x86-64 unrecognizable empty body -> MUST fail closed!
        buf_empty = bytearray(0x800)
        with self.assertRaises(up.PatchError):
            up.patch_gate3a_checkpoint(buf_empty, {up.FUNC_CHECKPOINT: (0x100, 0x80)}, text_va, text_off, "x86-64")

        # arm64 native: movz w4,#0x9c40 + movz w5,#0x4000, no sub-900K clamp (non-NOP body + ret)
        arm_3a_native = struct.pack(
            "<20I",
            0x52800000 | (0x9C40 << 5) | 4,
            0x52800000 | (0x4000 << 5) | 5,
            *([0xAA0103E0] * 17),  # mov x0, x1
            0xD65F03C0,            # ret
        )
        abuf = bytearray(0x800)
        abuf[text_off + 0x100:text_off + 0x100 + len(arm_3a_native)] = arm_3a_native
        changed_a, msg_a = up.patch_gate3a_checkpoint(abuf, {up.FUNC_CHECKPOINT: (0x100, len(arm_3a_native))}, text_va, text_off, "arm64")
        self.assertFalse(changed_a)
        self.assertIn("native", msg_a)

        # arm64 unexpected smaller cap (512000 = 0x7d000 -> lo=0xd000, hi=0x7) + cmp -> MUST fail closed!
        arm_3a_cap512 = struct.pack(
            "<20I",
            0x52800000 | (0x9C40 << 5) | 4,
            0xD2800000 | (0xD000 << 5) | 5,
            0xF2A00000 | (0x0007 << 5) | 5,
            0x6B05007F,  # cmp w3, w5
            *([0xAA0103E0] * 15),
            0xD65F03C0,
        )
        abuf2 = bytearray(0x800)
        abuf2[text_off + 0x100:text_off + 0x100 + len(arm_3a_cap512)] = arm_3a_cap512
        with self.assertRaisesRegex(up.PatchError, "unexpected smaller cap 512000"):
            up.patch_gate3a_checkpoint(abuf2, {up.FUNC_CHECKPOINT: (0x100, len(arm_3a_cap512))}, text_va, text_off, "arm64")

        # --- 2. Gate 3B native self-skip when all built-in model_profiles >= 900000 vs fail-closed ---
        profiles_900k = json.dumps({
            "model_profiles": {
                "MODEL_A": {"CASCADE_USE_EXPERIMENT_CHECKPOINTER": json.dumps({"max_token_limit": "900000"})},
                "MODEL_B": {"CASCADE_USE_EXPERIMENT_CHECKPOINTER": json.dumps({"max_token_limit": "1000000"})},
            }
        }).encode("utf-8")
        buf_3b_native = bytearray(0x1000) + bytearray(profiles_900k)
        c3b, m3b = up.patch_gate3b_exp_checkpoint(buf_3b_native, {up.FUNC_EXP_CHECKPOINT: (0x100, 0x80)}, text_va, text_off, "x86-64")
        self.assertFalse(c3b)
        self.assertIn("native", m3b)
        self.assertIn("min=900000", m3b)

        # Even ONE model profile with max_token_limit < 900000 (e.g. 512000) MUST NOT self-skip -> fails closed!
        profiles_mixed = json.dumps({
            "model_profiles": {
                "MODEL_A": {"CASCADE_USE_EXPERIMENT_CHECKPOINTER": json.dumps({"max_token_limit": "900000"})},
                "MODEL_B": {"CASCADE_USE_EXPERIMENT_CHECKPOINTER": json.dumps({"max_token_limit": "512000"})},
            }
        }).encode("utf-8")
        buf_3b_bad = bytearray(0x1000) + bytearray(profiles_mixed)
        with self.assertRaises(up.PatchError):
            up.patch_gate3b_exp_checkpoint(
                buf_3b_bad,
                {up.FUNC_EXP_CHECKPOINT: (0x100, 0x80), up.FUNC_SELECT_EXP_KEY: (0x200, 0x40)},
                text_va, text_off, "x86-64",
            )

        # --- 3. Gate 4B native self-skip vs unexpected smaller user-step cap (x86-64 & arm64) ---
        # x86-64: stepToMessage has no 0xe clamp AND defaultConversationalCascadeConfig has 16384 (0x4000) with NO 64K / sub-900K cap
        cfg_x64_native = b"\x90" * 16 + b"\xc7\x00\x00\x40\x00\x00" + b"\x90" * 32 + b"\xc7\x00" + struct.pack("<I", 1_000_000) + b"\x90" * 32
        buf_4b = bytearray(0x1000)
        buf_4b[text_off + 0x100:text_off + 0x180] = b"\x90" * 0x80
        buf_4b[text_off + 0x200:text_off + 0x200 + len(cfg_x64_native)] = cfg_x64_native
        funcs_4b = {
            up.FUNC_ONEHARNESS_STEP: (0x100, 0x80),
            up.FUNC_DEFAULT_CASCADE_CONFIG: (0x200, len(cfg_x64_native)),
        }
        c4b, m4b = up.patch_gate4b_oneharness_user_input(buf_4b, funcs_4b, text_va, text_off, "x86-64")
        self.assertFalse(c4b)
        self.assertIn("native", m4b)

        # x86-64: 0xfa00 (64000) still in defaultConversationalCascadeConfig -> MUST fail closed!
        cfg_x64_64k = b"\x90" * 16 + b"\xc7\x00\x00\x40\x00\x00" + b"\x90" * 16 + b"\xc7\x00\x00\xfa\x00\x00" + b"\x90" * 32
        buf_4b_64k = bytearray(0x1000)
        buf_4b_64k[text_off + 0x100:text_off + 0x180] = b"\x90" * 0x80
        buf_4b_64k[text_off + 0x200:text_off + 0x200 + len(cfg_x64_64k)] = cfg_x64_64k
        with self.assertRaisesRegex(up.PatchError, "unexpected smaller user-step cap 64000"):
            up.patch_gate4b_oneharness_user_input(
                buf_4b_64k,
                {up.FUNC_ONEHARNESS_STEP: (0x100, 0x80), up.FUNC_DEFAULT_CASCADE_CONFIG: (0x200, len(cfg_x64_64k))},
                text_va, text_off, "x86-64",
            )

        # x86-64: unexpected smaller cap 128000 (0x1f400) right after 16384 -> MUST fail closed!
        cfg_x64_128k = b"\x90" * 16 + b"\xc7\x00\x00\x40\x00\x00" + b"\x90" * 16 + b"\xc7\x00" + struct.pack("<I", 128000) + b"\x90" * 32
        buf_4b_128k = bytearray(0x1000)
        buf_4b_128k[text_off + 0x100:text_off + 0x180] = b"\x90" * 0x80
        buf_4b_128k[text_off + 0x200:text_off + 0x200 + len(cfg_x64_128k)] = cfg_x64_128k
        with self.assertRaisesRegex(up.PatchError, "unexpected smaller user-step cap 128000"):
            up.patch_gate4b_oneharness_user_input(
                buf_4b_128k,
                {up.FUNC_ONEHARNESS_STEP: (0x100, 0x80), up.FUNC_DEFAULT_CASCADE_CONFIG: (0x200, len(cfg_x64_128k))},
                text_va, text_off, "x86-64",
            )

        # arm64 Gate 4B native vs 0xfa00 fail-closed
        cfg_arm_native = struct.pack("<16I", 0x321203E0 | 2, *([0xD503201F] * 15))  # orr w2, wzr, #0x4000
        buf_4b_arm = bytearray(0x1000)
        buf_4b_arm[text_off + 0x100:text_off + 0x180] = struct.pack("<32I", *([0xD503201F] * 32))
        buf_4b_arm[text_off + 0x200:text_off + 0x200 + len(cfg_arm_native)] = cfg_arm_native
        c4ba, m4ba = up.patch_gate4b_oneharness_user_input(
            buf_4b_arm,
            {up.FUNC_ONEHARNESS_STEP: (0x100, 0x80), up.FUNC_DEFAULT_CASCADE_CONFIG: (0x200, len(cfg_arm_native))},
            text_va, text_off, "arm64",
        )
        self.assertFalse(c4ba)
        self.assertIn("native", m4ba)

        cfg_arm_64k = struct.pack("<16I", 0x52880002, 0x529F4003, *([0xD503201F] * 14))  # 0x4000 + 0xfa00
        buf_4b_arm_bad = bytearray(0x1000)
        buf_4b_arm_bad[text_off + 0x100:text_off + 0x180] = struct.pack("<32I", *([0xD503201F] * 32))
        buf_4b_arm_bad[text_off + 0x200:text_off + 0x200 + len(cfg_arm_64k)] = cfg_arm_64k
        with self.assertRaisesRegex(up.PatchError, "unexpected smaller user-step cap 64000"):
            up.patch_gate4b_oneharness_user_input(
                buf_4b_arm_bad,
                {up.FUNC_ONEHARNESS_STEP: (0x100, 0x80), up.FUNC_DEFAULT_CASCADE_CONFIG: (0x200, len(cfg_arm_64k))},
                text_va, text_off, "arm64",
            )

        # --- 4. CLI --acp-only mode skips Gate 1 & Gate 2 (even if missing or vanilla) while default preserves them ---
        # Build a tiny synthetic x86-64 binary with Gate 1, Gate 2, Gate 3A (vanilla), Gate 3B (vanilla), Gate 4A (vanilla), Gate 4B (vanilla)
        synth = bytearray(0x2000)
        g1_off, g2_off, g2c_off = 0x100, 0x180, 0x200
        g3a_off, g3b_off, sel_off = 0x300, 0x400, 0x480
        g4a_off, g4b_off = 0x500, 0x600

        synth[text_off + g1_off:text_off + g1_off + 16] = b"\x31\xc0\xc3" + b"\x90" * 13
        synth[text_off + g2_off:text_off + g2_off + 16] = b"\x48\x89\xc0\xc3" + b"\x90" * 12
        synth[text_off + g2c_off:text_off + g2c_off + 16] = b"\xc3" + b"\x90" * 15
        synth[text_off + g3a_off:text_off + g3a_off + len(up.CLAMP_256K_X64)] = up.CLAMP_256K_X64
        # Gate 3B: call sel_va ; test %al,%al ; jne +4 ; ret
        call_sel_rel = (text_va + sel_off) - (text_va + g3b_off + 5)
        g3b_bytes = b"\xe8" + struct.pack("<i", call_sel_rel) + b"\x84\xc0\x75\x04\x90\x90\x90\x90\xc3"
        synth[text_off + g3b_off:text_off + g3b_off + len(g3b_bytes)] = g3b_bytes
        # Gate 4A: cmp $8,%eax ; je +3 ; cmp $0x13,%eax ; jne +12 ; <12B user body> ; pop %rbp ; ret
        g4a_bytes = b"\x83\xf8\x08\x74\x05\x83\xf8\x13\x75\x0c" + (b"\x90" * 12) + b"\x48\x83\xc4\x08\x5d\xc3"
        synth[text_off + g4a_off:text_off + g4a_off + len(g4a_bytes)] = g4a_bytes
        # Gate 4B: cmp $0xe,%edx ; je +10 ; cmp $0x17,%edx ; jne +6 ; xor %edi,%edi ; ret
        g4b_bytes = b"\x83\xfa\x0e\x74\x0a\x83\xfa\x17\x75\x04\x31\xff\x90\x90\xc3"
        synth[text_off + g4b_off:text_off + g4b_off + len(g4b_bytes)] = g4b_bytes

        full_funcs = {
            up.FUNC_OFFERS: (g1_off, 0x40),
            up.FUNC_STORE_CW: (g2_off, 0x40),
            up.FUNC_STORE_CHOICE: (g2c_off, 0x40),
            up.FUNC_CHECKPOINT: (g3a_off, 0x80),
            up.FUNC_EXP_CHECKPOINT: (g3b_off, 0x40),
            up.FUNC_SELECT_EXP_KEY: (sel_off, 0x20),
            up.FUNC_MAX_STEP_TOKENS: (g4a_off, 0x40),
            up.FUNC_ONEHARNESS_STEP: (g4b_off, 0x40),
        }
        acp_bin = self.root / "agy-acp-only-test"
        acp_bin.write_bytes(synth)
        out_acp = io.StringIO()
        with patch.object(up, "find_text_section", return_value=(text_va, text_off, "x86-64")), \
                patch.object(up, "build_functions", return_value=dict(full_funcs)), \
                patch.object(up, "_running_pids_for_binary", return_value=[]), \
                patch.object(sys, "argv", ["agy-patch.py", "--binary", str(acp_bin), "--acp-only", "--in-place", "--no-backup"]), \
                contextlib.redirect_stdout(out_acp):
            up.main()
        patched_acp = acp_bin.read_bytes()
        self.assertIn("mode   : acp-only", out_acp.getvalue())
        self.assertNotIn("[Gate 1]", out_acp.getvalue())
        self.assertNotIn("[Gate 2]", out_acp.getvalue())
        # Gate 1 and Gate 2 bytes MUST remain untouched!
        self.assertEqual(patched_acp[text_off + g1_off:text_off + g1_off + 6], synth[text_off + g1_off:text_off + g1_off + 6])
        self.assertEqual(patched_acp[text_off + g2_off:text_off + g2_off + 5], synth[text_off + g2_off:text_off + g2_off + 5])
        # Second --acp-only --dry-run reports all active gates already unlocked
        out_acp2 = io.StringIO()
        with patch.object(up, "find_text_section", return_value=(text_va, text_off, "x86-64")), \
                patch.object(up, "build_functions", return_value=dict(full_funcs)), \
                patch.object(up, "_running_pids_for_binary", return_value=[]), \
                patch.object(sys, "argv", ["agy-patch.py", "--binary", str(acp_bin), "--acp-only", "--dry-run"]), \
                contextlib.redirect_stdout(out_acp2):
            up.main()
        self.assertIn("[=] all gates already unlocked; nothing to write", out_acp2.getvalue())

        # --acp-only also succeeds on a binary named "agy" that removed TUI Gate 1 & Gate 2 symbols completely!
        acp_only_funcs = {k: v for k, v in full_funcs.items() if k not in (up.FUNC_OFFERS, up.FUNC_STORE_CW, up.FUNC_STORE_CHOICE)}
        agy_named = self.root / "agy"
        agy_named.write_bytes(synth)
        with patch.object(up, "find_text_section", return_value=(text_va, text_off, "x86-64")), \
                patch.object(up, "build_functions", return_value=acp_only_funcs), \
                patch.object(up, "_running_pids_for_binary", return_value=[]), \
                patch.object(sys, "argv", ["agy-patch.py", "--binary", str(agy_named), "--acp-only", "--dry-run"]), \
                contextlib.redirect_stdout(io.StringIO()):
            up.main()

        # Backward compatibility: without --acp-only, Gate 1 & Gate 2 ARE still patched!
        full_bin = self.root / "agy-full-test"
        full_bin.write_bytes(synth)
        out_full = io.StringIO()
        with patch.object(up, "find_text_section", return_value=(text_va, text_off, "x86-64")), \
                patch.object(up, "build_functions", return_value=dict(full_funcs)), \
                patch.object(up, "_running_pids_for_binary", return_value=[]), \
                patch.object(sys, "argv", ["agy-patch.py", "--binary", str(full_bin), "--in-place", "--no-env-fix", "--no-backup"]), \
                contextlib.redirect_stdout(out_full):
            up.main()
        self.assertIn("[Gate 1]", out_full.getvalue())
        self.assertIn("[Gate 2]", out_full.getvalue())
        self.assertEqual(full_bin.read_bytes()[text_off + g1_off:text_off + g1_off + len(up.RET_TRUE_X64)], up.RET_TRUE_X64)

        # --- 5. scripts/agy_upgrade.py patch_candidate wires --acp-only ---
        official_dummy = self.root / "official-agy"
        official_dummy.write_bytes(b"dummy")
        stage_dir = self.root / "stage-acp-wire"
        stage_dir.mkdir()
        seen_cmds = []
        with patch.object(upgrade, "run", side_effect=lambda cmd, **kw: seen_cmds.append([str(c) for c in cmd])):
            upgrade.patch_candidate(official_dummy, stage_dir, dry=False)
        self.assertIn("--acp-only", seen_cmds[0])
        self.assertTrue(seen_cmds[0][1].endswith("third_party/agy-context-unlock/agy-patch.py"))

    def test_patch_agy_900k_requires_both_acp_context_and_extra_checks_no_legacy_magic_false_positive(self):
        import io
        import patch_agy_900k

        # Binary containing legacy 0x000dbba0 bytes (a0 bb 0d 00) MUST NOT falsely pass verification!
        fake_vanilla = self.root / "vanilla-with-900k-bytes"
        fake_vanilla.write_bytes(b"\x00" * 32 + bytes.fromhex("a0bb0d00") + b"\x00" * 32)
        self.assertFalse(hasattr(patch_agy_900k, "PATCH_SEQ"))

        def mk_cp(rc, stdout="", stderr=""):
            return subprocess.CompletedProcess(args=[], returncode=rc, stdout=stdout, stderr=stderr)

        # 1. Both fail (vanilla binary with legacy a0bb0d00 bytes) -> exit 1
        with patch("subprocess.run", side_effect=[
            mk_cp(0, "[*] [Gate 3A] ...\n[*] dry run complete: sha256 aaaa -> bbbb\n"),
            mk_cp(1, "[i] disable-internal-checkpoint: state=vanilla\n"),
        ]), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(patch_agy_900k.main(["--verify", str(fake_vanilla)]), 1)

        # 2. Only extra_patch passes while ACP context gates are still vanilla -> exit 1
        with patch("subprocess.run", side_effect=[
            mk_cp(0, "[*] [Gate 3A] ...\n[*] dry run complete: sha256 aaaa -> bbbb\n"),
            mk_cp(0, "[i] disable-internal-checkpoint: state=patched\n"),
        ]), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(patch_agy_900k.main(["--verify", str(fake_vanilla)]), 1)

        # 3. Only ACP context gates pass while extra_patch fails -> exit 1
        with patch("subprocess.run", side_effect=[
            mk_cp(0, "[=] [Gate 3A] already patched\n[=] all gates already unlocked; nothing to write\n"),
            mk_cp(1, "[i] disable-internal-checkpoint: state=vanilla\n"),
        ]), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(patch_agy_900k.main(["--verify", str(fake_vanilla)]), 1)

        # 4. Both ACP context gates (--acp-only --dry-run reports all unlocked) AND extra_patch --check pass -> exit 0
        seen_run_cmds = []
        def ok_run(cmd, **kw):
            seen_run_cmds.append([str(c) for c in cmd])
            if str(cmd[1]).endswith("agy-patch.py"):
                return mk_cp(0, "[=] [Gate 3A] native\n[=] all gates already unlocked; nothing to write\n")
            return mk_cp(0, "[i] disable-internal-checkpoint: state=patched\n")

        out_buf = io.StringIO()
        with patch("subprocess.run", side_effect=ok_run), contextlib.redirect_stdout(out_buf):
            self.assertEqual(patch_agy_900k.main(["--verify", str(fake_vanilla)]), 0)
        self.assertIn("--acp-only", seen_run_cmds[0])
        self.assertIn("--dry-run", seen_run_cmds[0])
        self.assertIn("--check", seen_run_cmds[1])
        self.assertIn("ACP context patches verified: True", out_buf.getvalue())
        self.assertIn("Extra patches verified: True", out_buf.getvalue())

if __name__ == "__main__":
    unittest.main()
