"""Mocked R2 behind real HOME HTTP authority; only local temporary DB tables."""
import hashlib
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch
ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("home_private_support", ROOT / "tests/support/home_sace_postgres_runner.py")
f = importlib.util.module_from_spec(spec)
spec.loader.exec_module(f)
from app.extensions import db
from app.models.sace_home import HomeController, HomeAssignment, HomeDocumentVersion, HomeEvidence
from app.program_sace_home import examination as ex, document_storage as storage
from app.utils import cloudflare_r2 as r2


class HomePrivateDocuments(f.HomeFoundation):
    def setUp(self):
        super().setUp()
        self.app.config["SACE_HOME_REQUIREMENTS_VERSION"] = ex.REQUIREMENTS
        self.app.config["SACE_HOME_R2_BUCKET"] = "private-home-test"
        self.addCleanup(self.app.config.pop, "SACE_HOME_R2_BUCKET", None)
        self.provision_home()
        self.auditor, self.aid = self.join_home(self.code_home())
        self.foreign, self.fid = self.join_home(self.code_home(), email="private-other@example.test")
        self.objects = {}
        def upload(key, content, **kwargs):
            if key in self.objects and self.objects[key] != content:
                raise r2.R2KeyCollision("Fixture collision")
            self.objects[key] = content
            return key
        def read(key, **kwargs):
            if key not in self.objects:
                raise OSError("Fixture offline")
            return self.objects[key]
        self.uploader = patch.object(r2, "upload_bytes_to_r2", side_effect=upload)
        self.reader = patch.object(r2, "read_file_from_r2", side_effect=read)
        self.upload_mock = self.uploader.start()
        self.read_mock = self.reader.start()
        self.addCleanup(self.uploader.stop)
        self.addCleanup(self.reader.stop)

    def fixture(self, label="private-test-v1", data=b"%PDF-1.4 private HTTP fixture"):
        digest = hashlib.sha256(data).hexdigest()
        key = "endorsement/" + digest + ".pdf"
        manifest = {"subject": f.s.SUBJECT, "kind": "application_form_1", "pdf_sha256": digest,
            "home_approval": {"approved_by": "test fixture", "reference": "not production"}}
        with self.app.app_context():
            version = f.s.publish_document(HomeController.query.one(), "application_form_1",
                label, key, manifest, content=data)
            db.session.commit()
            return version.id, key, data

    def target(self, vid, assignment=None):
        return f"/sace/home/documents/{vid}/content?assignment_id={self.aid if assignment is None else assignment}"

    def test_primary_fallback_headers_and_authorization(self):
        vid, key, data = self.fixture()
        target = self.target(vid)
        with self.app.app_context():
            disk = storage.disk_path(key)
            self.assertEqual(disk.read_bytes(), data)
            disk.unlink()
        for suffix in ("", "&download=1", "&download=attachment&as_attachment=1"):
            self.read_mock.reset_mock()
            with patch.object(storage, 'disk_path', side_effect=ValueError('invalid disk root')):
                response = self.auditor.get(target + suffix)
            self.read_mock.assert_called_once_with(key, bucket='private-home-test')
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.data, data)
            self.assertEqual(response.headers["Cache-Control"], "private, no-store")
            self.assertEqual(response.headers["X-Content-Type-Options"], "nosniff")
            self.assertTrue(response.headers['Content-Disposition'].startswith('inline;'))
            self.assertNotIn("Location", response.headers)
            self.assertNotIn(b"r2.dev", response.data)
            self.assertNotIn(b"https://", response.data)
            response.close()
        board = self.auditor.get(f"/sace/home/assignments/{self.aid}/board")
        self.assertEqual(board.status_code, 200)
        self.assertNotIn(b"private-home-test", board.data)
        self.assertNotIn(b"r2.dev", board.data)
        material = self.auditor.get(f"/sace/home/assignments/{self.aid}/materials/application_form_1")
        self.assertIn(target.encode(), material.data)
        self.assertIn(b'pdfjsLib.getDocument(', material.data)
        self.assertNotIn(key.encode(), material.data)
        self.assertNotIn(b'private-home-test', material.data)
        self.assertNotIn(b'r2.dev', material.data)
        self.read_mock.reset_mock()
        for client, url in ((self.foreign, target), (self.auditor, self.target(vid, self.fid)),
                            (self.auditor, f"/sace/home/documents/{vid}/content")):
            self.assertEqual(client.get(url).status_code, 403)
        self.read_mock.assert_not_called()
        self.assertNotEqual(self.app.test_client().get(target).status_code, 200)
        # R2 outage works only with the verified disk copy retained by publication.
        disk.parent.mkdir(parents=True, exist_ok=True)
        disk.write_bytes(data)
        self.objects.clear()
        self.assertEqual(self.auditor.get(target).data, data)
        disk.unlink()
        self.assertNotEqual(self.auditor.get(target).status_code, 200)
        with self.app.app_context():
            db.session.get(HomeAssignment, self.aid).status = "revoked"
            db.session.commit()
        self.read_mock.reset_mock()
        self.assertEqual(self.auditor.get(target).status_code, 403)
        self.read_mock.assert_not_called()

    def test_corrupt_primary_and_immutable_binding(self):
        vid, key, data = self.fixture()
        base = f"/sace/home/assignments/{self.aid}"
        self.assertEqual(self.auditor.get(self.target(vid)).status_code, 200)
        self.assertEqual(self.auditor.get(base + "/materials/application_form_1").status_code, 200)
        self.assertEqual(self.auditor.post(base + "/materials/application_form_1",
            data={"version_id": vid}).status_code, 302)
        newer, newer_key, newer_data = self.fixture("private-test-v2", b"%PDF-1.4 newer fixture")
        self.assertEqual(self.auditor.get(self.target(newer)).status_code, 409)
        self.assertEqual(self.auditor.get(self.target(vid)).data, data)
        self.objects[key] = b"%PDF-1.4 corrupt fixture"
        self.assertEqual(self.auditor.get(self.target(vid)).status_code, 409)
        with self.app.app_context():
            self.assertEqual(storage.disk_path(key).read_bytes(), data)
            self.assertEqual(HomeEvidence.query.filter_by(assignment_id=self.aid,
                item="application_form_1", event="examined", document_version_id=vid).count(), 1)
            self.assertEqual(HomeDocumentVersion.query.count(), 2)

    def test_publication_rejects_hash_and_collision_before_version_acceptance(self):
        vid, key, data = self.fixture()
        with self.app.app_context():
            owner = HomeController.query.one()
            manifest = {"subject": f.s.SUBJECT, "kind": "application_form_1", "pdf_sha256": "0" * 64,
                "home_approval": {"approved_by": "fixture", "reference": "test-only"}}
            with self.assertRaises(storage.StorageIntegrityError):
                f.s.publish_document(owner, "application_form_1", "invalid", key, manifest, content=data)
            db.session.rollback()
            self.objects[key] = b"%PDF-different fixture"
            manifest["pdf_sha256"] = hashlib.sha256(data).hexdigest()
            with self.assertRaises(storage.StorageIntegrityError):
                f.s.publish_document(owner, "application_form_1", "collision", key, manifest, content=data)
            db.session.rollback()
            self.assertEqual(HomeDocumentVersion.query.count(), 1)


if __name__ == "__main__":
    unittest.main(defaultTest=(
        "HomePrivateDocuments.test_primary_fallback_headers_and_authorization",
        "HomePrivateDocuments.test_corrupt_primary_and_immutable_binding",
        "HomePrivateDocuments.test_publication_rejects_hash_and_collision_before_version_acceptance"), verbosity=2)
