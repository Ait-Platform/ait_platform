"""Offline R2/private HOME storage checks. No real storage calls or publication."""
import hashlib
import io
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
from flask import Flask
from botocore.exceptions import ClientError
from werkzeug.datastructures import FileStorage

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from app.utils import cloudflare_r2 as r2
from app.program_sace_home import document_storage as storage

ORIGINAL_CLIENT = r2.boto3.client

PDF = b"%PDF-1.4\nprivate-test-fixture"
SHA = hashlib.sha256(PDF).hexdigest()


class FakeR2:
    def __init__(self):
        self.objects = {}
        self.calls = []
        self.last_body = None

    def put_object(self, **kwargs):
        self.calls.append(kwargs)
        key = (kwargs["Bucket"], kwargs["Key"])
        if kwargs.get("IfNoneMatch") == "*" and key in self.objects:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        self.objects[key] = kwargs["Body"]
        return {}

    def get_object(self, **kwargs):
        key = (kwargs["Bucket"], kwargs["Key"])
        if key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        self.last_body = io.BytesIO(self.objects[key])
        return {"Body": self.last_body}

    def head_object(self, **kwargs):
        key = (kwargs["Bucket"], kwargs["Key"])
        if key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {"ContentLength": len(self.objects[key]), "ContentType": "application/pdf"}


class PrivateStorage(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.app = Flask("home-private-tests", static_folder=str(ROOT / "app/static"))
        self.app.config.update(SACE_HOME_DOCUMENT_ROOT=self.directory.name,
                               SACE_HOME_R2_BUCKET="private-home-test")
        self.context = self.app.app_context()
        self.context.push()
        self.addCleanup(self.context.pop)
        self.fake = FakeR2()
        self.env = patch.dict(os.environ, {"R2_ENDPOINT_URL": "https://r2.invalid",
            "R2_ACCESS_KEY": "test", "R2_SECRET_KEY": "test", "R2_BUCKET_NAME": "public-test"}, clear=True)
        self.env.start()
        self.addCleanup(self.env.stop)
        self.client = patch.object(r2.boto3, "client", return_value=self.fake)
        self.client.start()
        self.addCleanup(self.client.stop)
        self.key = "manuals/" + SHA + ".pdf"

    def test_private_exact_key_head_read_and_close_without_public_domain(self):
        self.assertNotIn("R2_PUBLIC_DOMAIN", os.environ)
        self.assertEqual(r2.upload_bytes_to_r2(self.key, PDF, bucket="private-home-test"), self.key)
        self.assertEqual(self.fake.calls[0]["IfNoneMatch"], "*")
        self.assertEqual(r2.head_file_from_r2(self.key, bucket="private-home-test")["ContentLength"], len(PDF))
        self.assertIsNone(r2.head_file_from_r2("absent.pdf", bucket="private-home-test"))
        self.assertEqual(r2.read_file_from_r2(self.key, bucket="private-home-test"), PDF)
        self.assertTrue(self.fake.last_body.closed)
        with patch.object(self.fake, "head_object", side_effect=ClientError(
                {"Error": {"Code": "AccessDenied"}}, "HeadObject")):
            with self.assertRaises(ClientError):
                r2.head_file_from_r2(self.key, bucket="private-home-test")

    def test_installed_sdk_conditional_put_without_network(self):
        from botocore.awsrequest import AWSResponse
        client = ORIGINAL_CLIENT("s3", endpoint_url="https://r2.invalid",
            aws_access_key_id="test", aws_secret_access_key="test", region_name="auto")
        class Raw(io.BytesIO):
            def stream(self, amt=1024, decode_content=False):
                yield self.read()
        requests = []
        def offline_send(request):
            requests.append(request)
            if request.method == "PUT":
                self.assertEqual(request.headers["If-None-Match"], b"*")
                self.assertIn(b"if-none-match", request.headers["Authorization"])
                return AWSResponse(request.url, 200, {"Content-Length": "0"}, Raw(b""))
            self.assertEqual(request.method, "GET")
            return AWSResponse(request.url, 200, {"Content-Length": str(len(PDF))}, Raw(PDF))
        with patch.object(client._endpoint.http_session, "send", side_effect=offline_send):
            with patch.object(r2, "r2_client", return_value=(client, "private-home-test")):
                self.assertEqual(r2.upload_bytes_to_r2(self.key, PDF), self.key)
        self.assertEqual(len(requests), 2)
        client.close()

    def test_existing_public_upload_contract_unchanged(self):
        os.environ["R2_PUBLIC_DOMAIN"] = "https://public.invalid"
        file = FileStorage(stream=io.BytesIO(PDF), filename="test.pdf", content_type="application/pdf")
        url = r2.upload_file_to_r2(file)
        self.assertTrue(url.startswith("https://public.invalid/organogram/"))
        self.assertEqual(file.tell(), 0)
        self.assertTrue(r2.upload_file_to_r2(file, prefix="uip/rp_queries", return_key=True).startswith("uip/rp_queries/"))

    def test_immutable_r2_collision_and_identical_reuse(self):
        r2.upload_bytes_to_r2(self.key, PDF, bucket="private-home-test")
        r2.upload_bytes_to_r2(self.key, PDF, bucket="private-home-test")
        with self.assertRaises(r2.R2KeyCollision):
            r2.upload_bytes_to_r2(self.key, b"%PDF-other", bucket="private-home-test")
        self.assertEqual(self.fake.objects[("private-home-test", self.key)], PDF)

    def test_primary_and_verified_disk_retention(self):
        self.assertEqual(storage.store(self.key, PDF, SHA), self.key)
        self.assertEqual(storage.disk_path(self.key).read_bytes(), PDF)
        storage.disk_path(self.key).unlink()
        self.assertEqual(storage.read(self.key, SHA), PDF)

    def test_r2_failure_uses_verified_disk_and_bad_disk_fails(self):
        with patch.object(r2, "upload_bytes_to_r2", side_effect=OSError("offline")):
            storage.store(self.key, PDF, SHA)
        with patch.object(r2, "read_file_from_r2", side_effect=OSError("offline")):
            self.assertEqual(storage.read(self.key, SHA), PDF)
            storage.disk_path(self.key).write_bytes(b"%PDF-corrupt")
            with self.assertRaises(storage.StorageIntegrityError):
                storage.read(self.key, SHA)

    def test_corrupt_r2_fails_closed_even_with_good_disk(self):
        storage.store(self.key, PDF, SHA)
        self.fake.objects[("private-home-test", self.key)] = b"%PDF-corrupt"
        with self.assertRaises(storage.StorageIntegrityError):
            storage.read(self.key, SHA)
        self.assertEqual(storage.disk_path(self.key).read_bytes(), PDF)
        with self.assertRaises(storage.StorageIntegrityError):
            storage.store(self.key, PDF, SHA)

    def test_both_backends_unavailable(self):
        with self.assertRaises(storage.StorageUnavailable):
            storage.read(self.key, SHA)

    def test_hash_and_pdf_validation_and_disk_collision(self):
        for digest in ("bad", "0" * 64, SHA.upper(), None):
            with self.assertRaises(storage.StorageIntegrityError):
                storage.store(self.key, PDF, digest)
        bad = b"not a PDF"
        with self.assertRaises(storage.StorageIntegrityError):
            storage.store(self.key, bad, hashlib.sha256(bad).hexdigest())
        path = storage.disk_path(self.key)
        path.parent.mkdir(parents=True)
        path.write_bytes(b"%PDF-different")
        with self.assertRaises(storage.StorageIntegrityError):
            storage.store(self.key, PDF, SHA)
        self.assertEqual(path.read_bytes(), b"%PDF-different")
        self.assertEqual(self.fake.calls, [])

    def test_traversal_urls_and_public_roots_rejected(self):
        for key in ("../file.pdf", "/file.pdf", "a/../file.pdf", "a//file.pdf",
                    "https://r2.invalid/file.pdf", "C:/file.pdf", "a\\file.pdf",
                    "file.pdf?token=x", "%2e%2e/file.pdf", "a/./file.pdf"):
            with self.subTest(key=key), self.assertRaises(ValueError):
                storage.read(key, SHA)
        for directory in (ROOT / "app/static", ROOT / "app/static/uploads/home",
                          Path("/app/static/uploads/home")):
            self.app.config["SACE_HOME_DOCUMENT_ROOT"] = str(directory)
            with self.assertRaises(ValueError):
                storage.store(self.key, PDF, SHA)
        self.assertEqual(self.fake.calls, [])

    def test_private_root_cannot_resolve_key_into_public_storage(self):
        self.app.config["SACE_HOME_DOCUMENT_ROOT"] = str(ROOT / "app")
        with self.assertRaises(ValueError):
            storage.read("static/uploads/home/file.pdf", SHA)
        self.assertEqual(self.fake.calls, [])

    def test_explicit_legacy_public_bucket_rejected(self):
        self.app.config["SACE_HOME_R2_BUCKET"] = "public-test"
        with self.assertRaises(storage.StorageIntegrityError):
            storage.read(self.key, SHA)
        self.assertEqual(self.fake.calls, [])

    def test_configured_public_upload_root_rejected(self):
        self.app.config["UPLOAD_FOLDER"] = self.directory.name
        with self.assertRaises(ValueError):
            storage.store(self.key, PDF, SHA)
        self.assertEqual(self.fake.calls, [])

    def test_no_shared_public_bucket_default(self):
        self.app.config.pop("SACE_HOME_R2_BUCKET")
        storage.store(self.key, PDF, SHA)
        self.assertEqual(storage.read(self.key, SHA), PDF)
        self.assertEqual(self.fake.calls, [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
