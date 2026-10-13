"""Fixed R01 review pair in private storage; no conversion or public URLs."""
import hashlib
from pathlib import Path

from flask import current_app

VERSION = "R01"
WORD_NAME = "Lease_Review_Draft_R01.docx"
PDF_NAME = "Lease_Review_Draft_R01.pdf"
# PDF exported by Microsoft Word from this exact DOCX and visually verified.
EXPECTED_SHA256 = {
    WORD_NAME: "d3485ad0d72564a6aa01835afa110756867b574c35e16e4449a15173f9974b57",
    PDF_NAME: "9f1bf5d6f4762098e923a65cd7e23503dc9239298e50561a846350a445538601",
}


class ReviewDocumentUnavailable(OSError):
    pass


def private_root():
    configured = current_app.config.get("PM_LEASE_DOCUMENT_ROOT")
    root = Path(configured or Path(current_app.instance_path) / "pm_lease_documents").resolve()
    public = [Path(current_app.root_path) / "static", Path(current_app.root_path).parent / "static"]
    if current_app.static_folder:
        public.append(Path(current_app.static_folder))
    for name in ("UPLOAD_FOLDER", "UPLOAD_ROOT"):
        if current_app.config.get(name):
            public.append(Path(current_app.config[name]))
    if any(root.is_relative_to(path.resolve()) for path in public):
        raise ReviewDocumentUnavailable("Lease storage must be private.")
    directory = (root / VERSION).resolve()
    if not directory.is_relative_to(root) or any(directory.is_relative_to(path.resolve()) for path in public):
        raise ReviewDocumentUnavailable("Lease storage escapes its private root.")
    return directory


def read_review_pair():
    """Read and verify both immutable files so Word and preview cannot drift."""
    try:
        directory = private_root()
        content = {}
        for filename, digest in EXPECTED_SHA256.items():
            path = (directory / filename).resolve()
            if path.parent != directory:
                raise ReviewDocumentUnavailable("Lease file escapes its private root.")
            payload = path.read_bytes()
            if hashlib.sha256(payload).hexdigest() != digest:
                raise ReviewDocumentUnavailable("Lease review integrity check failed.")
            content[filename] = payload
        return content
    except (OSError, ValueError) as exc:
        raise ReviewDocumentUnavailable("The current lease review is unavailable.") from exc
