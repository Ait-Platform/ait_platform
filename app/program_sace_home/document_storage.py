"""HOME-only verified private storage. No storage URLs leave this module.

SACE_HOME_R2_BUCKET must designate an operationally verified non-public bucket.
It never defaults to the existing public Reading bucket. Without it disk-only
operation remains available. This module does not configure buckets or publish.
"""
import hashlib
import os
import re
from pathlib import Path, PurePosixPath
from flask import current_app
from app.utils import cloudflare_r2 as r2


class StorageUnavailable(OSError):
    pass


class StorageIntegrityError(ValueError):
    pass


def validate_key(key):
    if (not isinstance(key, str) or not key or key != key.strip()
            or any(c in key for c in "\\:%?#") or key.startswith("/")
            or any(ord(c) < 32 for c in key)
            or any(p in {"", ".", ".."} for p in key.split("/"))
            or PurePosixPath(key).suffix.lower() != ".pdf"):
        raise ValueError("A relative HOME PDF storage key is required.")
    return key


def public_roots():
    roots = [Path(current_app.root_path) / "static", Path("/app/static")]
    if current_app.static_folder:
        roots.append(Path(current_app.static_folder))
    for name in ("UPLOAD_FOLDER", "UPLOAD_ROOT"):
        if current_app.config.get(name):
            roots.append(Path(current_app.config[name]))
    return [path.resolve() for path in roots]


def disk_root():
    root = Path(current_app.config.get("SACE_HOME_DOCUMENT_ROOT",
        Path(current_app.instance_path) / "sace_home_documents")).resolve()
    if any(root.is_relative_to(public) for public in public_roots()):
        raise ValueError("HOME fallback storage must be outside public static storage.")
    return root


def disk_path(key):
    key = validate_key(key)
    root = disk_root()
    path = (root / key).resolve()
    if (path == root or not path.is_relative_to(root)
            or any(path.is_relative_to(public) for public in public_roots())):
        raise ValueError("HOME storage path escapes the private root.")
    return path


def private_bucket():
    bucket = current_app.config.get("SACE_HOME_R2_BUCKET") or os.getenv("SACE_HOME_R2_BUCKET")
    if bucket and bucket == os.getenv("R2_BUCKET_NAME"):
        raise StorageIntegrityError("HOME must not use the legacy public storage bucket.")
    return bucket


def verify(content, expected_sha256):
    if (not isinstance(expected_sha256, str)
            or not re.fullmatch(r"[0-9a-f]{64}", expected_sha256)
            or hashlib.sha256(content).hexdigest() != expected_sha256
            or not content.startswith(b"%PDF-")):
        raise StorageIntegrityError("HOME PDF does not match its approved SHA-256.")
    return content


def read(key, expected_sha256):
    validate_key(key)
    bucket = private_bucket()
    if bucket:
        try:
            content = r2.read_file_from_r2(key, bucket=bucket)
        except Exception:
            current_app.logger.warning("HOME R2 read unavailable; trying private disk.")
        else:
            # Corruption fails closed, rather than hiding it behind fallback.
            return verify(content, expected_sha256)
    path = disk_path(key)
    try:
        content = path.read_bytes()
    except OSError as exc:
        raise StorageUnavailable("No verified HOME document backend is available.") from exc
    return verify(content, expected_sha256)


def _store_disk(key, content, expected_sha256):
    path = disk_path(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError:
        verify(path.read_bytes(), expected_sha256)
    verify(path.read_bytes(), expected_sha256)


def store(key, content, expected_sha256, *, require_r2=False):
    """Admin: verified R2 first, optional disk. Controllers retain required disk."""
    validate_key(key)
    verify(content, expected_sha256)
    bucket = private_bucket()
    if require_r2:
        if not bucket:
            raise StorageUnavailable("Configure the private HOME R2 bucket before publishing.")
        try:
            r2.upload_bytes_to_r2(key, content, bucket=bucket)
            stored = r2.read_file_from_r2(key, bucket=bucket)
        except r2.R2KeyCollision as exc:
            raise StorageIntegrityError(str(exc)) from exc
        except Exception as exc:
            raise StorageUnavailable("HOME private R2 publication could not be verified; document was not published.") from exc
        verify(stored, expected_sha256)
        if stored != content:
            raise StorageIntegrityError("HOME R2 content does not match the uploaded bytes.")
        try:
            _store_disk(key, content, expected_sha256)
        except Exception:
            current_app.logger.warning("HOME optional private disk backup unavailable; verified R2 retained.")
        return key
    # Existing controller contract: a verified private disk copy is required.
    _store_disk(key, content, expected_sha256)
    if bucket:
        try:
            r2.upload_bytes_to_r2(key, content, bucket=bucket)
        except r2.R2KeyCollision as exc:
            raise StorageIntegrityError(str(exc)) from exc
        except Exception:
            current_app.logger.warning("HOME R2 upload unavailable; verified private disk retained.")
    return key


def staged(key, expected_sha256=None):
    """Read privately staged publication bytes; no inferred public location."""
    content = disk_path(key).read_bytes()
    expected = expected_sha256 or hashlib.sha256(content).hexdigest()
    return verify(content, expected)
