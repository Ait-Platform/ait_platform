"""Parity comparisons and real local PostgreSQL read-only capture; no attestation."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from dotenv import dotenv_values
from sqlalchemy import create_engine, event
from sqlalchemy.engine import make_url
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
spec = importlib.util.spec_from_file_location("home_parity", ROOT / "scripts/check_home_production_parity.py")
p = importlib.util.module_from_spec(spec)
spec.loader.exec_module(p)


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


class Parity(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        (self.root / "seed.py").write_text("approved seed", encoding="utf-8")
        self.source = {"chapters": [{"id": 1, "title": "Observation"}],
            "questions": [{"id": 1, "question": "Approved question"}], "options": [],
            "files": {"chapter.html": {"text": "approved text", "sha256": "filehash"}},
            "assets": {"image.jpg": "assethash"}}
        digest = hashlib.sha256(canonical(self.source).encode()).hexdigest()
        self.snapshot = {"builder_version": "home-source-snapshot-v1", "source": self.source,
            "source_sha256": digest, "gaps": []}
        self.manifest = {**copy.deepcopy(self.snapshot), "kind": "participant_manual",
            "source_approval": {"production_parity_checked": False},
            "current_provenance": {"current_additional_inputs": {
                "seed.py": hashlib.sha256((self.root / "seed.py").read_bytes()).hexdigest()}}}

    def compare(self):
        self.snapshot["source_sha256"] = hashlib.sha256(canonical(self.snapshot["source"]).encode()).hexdigest()
        return p.compare_manifest(self.manifest, self.snapshot, self.root, canonical)

    def test_match_without_attestation_or_manifest_mutation(self):
        before = canonical(self.manifest)
        self.assertTrue(self.compare()["matches"])
        self.assertEqual(canonical(self.manifest), before)
        self.assertFalse(self.manifest["source_approval"]["production_parity_checked"])

    def test_row_mismatch(self):
        self.snapshot["source"]["chapters"][0]["title"] = "Changed"
        report = self.compare()
        self.assertFalse(report["matches"])
        self.assertEqual(report["differences"][0]["path"], "source.chapters[0].title")

    def test_text_mismatch(self):
        self.snapshot["source"]["files"]["chapter.html"]["text"] = "Changed"
        self.assertEqual(self.compare()["differences"][0]["path"], "source.files.chapter.html.text")

    def test_asset_mismatch(self):
        self.snapshot["source"]["assets"]["image.jpg"] = "Changed"
        self.assertEqual(self.compare()["differences"][0]["path"], "source.assets.image.jpg")

    def test_additional_input_mismatch_and_missing(self):
        (self.root / "seed.py").write_text("changed")
        report = self.compare()
        self.assertFalse(report["matches"])
        self.assertFalse(report["additional_inputs"][0]["matches"])
        (self.root / "seed.py").unlink()
        self.assertEqual(self.compare()["additional_inputs"][0]["error"], "Input unavailable.")

    def test_gaps_and_invalid_manifest_digest(self):
        self.snapshot["gaps"] = ["Missing chapter 2"]
        self.assertFalse(self.compare()["matches"])
        self.snapshot["gaps"] = []
        self.manifest["source_sha256"] = "0" * 64
        self.assertIn("Manifest source does not match its canonical SHA-256.", self.compare()["errors"])

    def test_missing_rows_and_invalid_additional_paths(self):
        self.snapshot["source"]["questions"] = []
        report = self.compare()
        self.assertEqual(report["differences"][0]["path"], "source.questions[0]")
        self.manifest["current_provenance"]["current_additional_inputs"] = {"../seed.py": "bad"}
        self.assertEqual(self.compare()["additional_inputs"][0]["error"], "Invalid input path.")

    def test_real_local_readonly_snapshot_capture(self):
        url = make_url(dotenv_values(ROOT / ".env")["DATABASE_URL"])
        if (url.get_backend_name() != "postgresql" or url.host not in
                ("localhost", "127.0.0.1", "::1") or url.database != "ait_local_db" or url.query):
            raise RuntimeError("Requires verified localhost ait_local_db without URL overrides.")
        engine = create_engine(url, connect_args={"options": "-c default_transaction_read_only=on"})
        self.addCleanup(engine.dispose)
        statements = []
        def guard(conn, cursor, statement, parameters, context, executemany):
            self.assertIn(statement.strip().split()[0].upper(), {"SELECT", "SHOW", "SET"})
            statements.append(statement)
        event.listen(engine, "before_cursor_execute", guard)
        module = p.load_snapshot_module(ROOT)
        snapshot, enforcement = p.capture(engine, ROOT, module)
        self.assertEqual(enforcement["transaction_read_only"], "on")
        self.assertEqual(enforcement["transaction_isolation"], "repeatable read")
        self.assertGreaterEqual(len(statements), 6)
        self.assertEqual(snapshot["source_sha256"],
            hashlib.sha256(module.canonical(snapshot["source"]).encode()).hexdigest())
        self.assertEqual(len(snapshot["source"]["chapters"]), 30)


if __name__ == "__main__":
    unittest.main(verbosity=2)
