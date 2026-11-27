import contextlib
import io
import pathlib
import re
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import e2e_common as E  # noqa: E402
import e2e_protocol_and_security as EPS  # noqa: E402

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent


class DocsConfigAndProtocolGateTests(unittest.TestCase):
    def test_e2e_protocol_and_security_refuses_live_and_main_ports(self):
        with patch("urllib.request.urlopen") as urlopen_mock, contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(EPS.main(["18896", "--live", "--model", E.LIVE_MODEL]), 2)
            self.assertEqual(EPS.main(["18890"]), 2)
            urlopen_mock.assert_not_called()
        for bad_dash in (18889, 18890, 18891, 18896, 80):
            with self.subTest(bad_dash=bad_dash):
                with self.assertRaises(E.GateError):
                    EPS.resolve_dash_port(18896, {"dash_port": bad_dash})
        self.assertEqual(EPS.resolve_dash_port(18896, {"dash_port": 18897}), 18897)
        self.assertEqual(EPS.resolve_dash_port(18896, None, explicit_dash=18899), 18899)

    def test_parse_prometheus_text_validates_exposition_lines(self):
        sample = (
            "# HELP agy_gateway_store_ready Whether store is healthy.\n"
            "# TYPE agy_gateway_store_ready gauge\n"
            "agy_gateway_store_ready 1\n"
            'agy_gateway_soft_detach_total{outcome="joined"} 3\n'
            'agy_gateway_upgrade_state{state="idle"} 1\n'
        )
        bad, metrics = EPS.parse_prometheus_text(sample)
        self.assertEqual(bad, [])
        self.assertEqual(metrics["agy_gateway_store_ready"][0][1], 1.0)
        self.assertEqual(metrics["agy_gateway_soft_detach_total"][0][1], 3.0)
        bad2, _ = EPS.parse_prometheus_text("not a valid prometheus line\n")
        self.assertEqual(len(bad2), 1)

    def test_env_example_parity_with_gateway_config(self):
        env_example = (REPO_ROOT / ".env.example").read_text(encoding="utf-8")
        kv = {}
        for line in env_example.splitlines():
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            key, sep, val = s.partition("=")
            self.assertEqual(sep, "=", f"malformed line in .env.example: {line}")
            kv[key.strip()] = val.strip()

        self.assertEqual(kv.get("AGY_IDLE_WORKER_TTL_SEC"), "180")
        self.assertNotIn("AGY_DASH_TOKEN", kv)
        self.assertNotIn("AGY_DASH_PASS", kv)
        self.assertNotIn("AGY_DASH_TOKEN", env_example)
        self.assertNotIn("AGY_DASH_PASS", env_example)

        go_sources = "\n".join(
            p.read_text(encoding="utf-8")
            for p in sorted((REPO_ROOT / "go-gateway").rglob("*.go"))
            if not p.name.endswith("_test.go") and "testdata" not in p.parts
        )
        for key in kv:
            self.assertIn(f'"{key}"', go_sources, f"{key} in .env.example is not read by go-gateway")

    def test_public_docs_assets_anchors_and_privacy_desensitization(self):
        asset_attr_re = re.compile(r'(?:src|srcset|href)="([^"]+)"')
        anchor_def_re = re.compile(r'<a\s+id="([^"]+)"')
        local_user_path_re = re.compile(r"(?<!/mnt/c)/(?:Users|home)/[A-Za-z0-9._-]{2,}")
        internal_id_re = re.compile(r"\b(?:[A-Z]{2,5}-(?:[A-Z]{2,5}-)?\d{2}|\d{2}-\d{2}-[a-z0-9]{4,})\b")

        for doc_name in ("README.md", "README_ZH.md"):
            raw = (REPO_ROOT / doc_name).read_text(encoding="utf-8")
            defined_ids = set(anchor_def_re.findall(raw))
            for target in asset_attr_re.findall(raw):
                if target.startswith(("http://", "https://", "mailto:")):
                    continue
                if target.startswith("#"):
                    self.assertIn(target[1:], defined_ids, f"{doc_name} broken anchor {target}")
                    continue
                rel = target.removeprefix("./")
                self.assertTrue((REPO_ROOT / rel).is_file(), f"{doc_name} missing asset {target}")

            self.assertIn("AGY_IDLE_WORKER_TTL_SEC", raw)
            self.assertIn("180", raw)
            self.assertIn("darwin_arm64", raw)
            self.assertIn("linux_arm64", raw)
            self.assertIn("output_config.format", raw)
            self.assertIn("max_input_tokens", raw)
            self.assertIn("previous_response_not_found", raw)
            self.assertIn("thread_not_found", raw)
            self.assertIn("GEMINI_ENTERPRISE_LICENSE_NOT_VALID", raw)
            self.assertNotIn("AGY_DASH_TOKEN", raw)
            self.assertNotIn("AGY_DASH_PASS", raw)

        tracked_public = [
            REPO_ROOT / ".env.example",
            REPO_ROOT / "README.md",
            REPO_ROOT / "README_ZH.md",
            *sorted((REPO_ROOT / "scripts").glob("*.py")),
            *sorted((REPO_ROOT / "scripts").glob("*.sh")),
            *sorted((REPO_ROOT / "scripts").glob("*.json")),
        ]
        allowed_public_tokens = {"SHA-256", "SHA-512", "UTF-8", "JSON-RPC", "HMAC-SHA256", "GPT-OSS"}
        for path in tracked_public:
            content = path.read_text(encoding="utf-8")
            self.assertIsNone(local_user_path_re.search(content), f"local user path leaked in {path.name}")
            for m in internal_id_re.finditer(content):
                tok = m.group(0)
                if tok in allowed_public_tokens or tok.startswith("BLUE-"):
                    continue
                self.fail(f"internal task identifier {tok!r} leaked in {path.name}")


if __name__ == "__main__":
    unittest.main()
