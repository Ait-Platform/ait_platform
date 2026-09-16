"""Private, tenant-scoped immutable file versions. HTTP layer owns the transaction."""
import hashlib
import uuid
from datetime import date
from pathlib import Path
from flask import abort, current_app
from werkzeug.utils import secure_filename
from werkzeug.exceptions import Forbidden
from app.extensions import db
from app.models.uip import UipDocument
from app.models.uip_operations import UipDocumentFolder, UipDocumentVersion
from . import audit
from .reception import text, identifier

READERS = ("manager", "receptionist", "committee_member", "owner", "resident")
WRITERS = ("manager", "committee_member")
VISIBILITY = {"PRIVATE": ("manager",), "STAFF": ("manager", "receptionist"),
              "COMMITTEE_ONLY": ("manager", "committee_member", "owner"), "MEMBERS": READERS}
TYPES = {".pdf": "application/pdf", ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".txt": "text/plain"}


def accessible(org, actor, document):
    try:
        audit.authorize(org, actor, VISIBILITY.get(document.access_classification, ("manager",)))
        return True
    except Forbidden:
        return False


def listing(org, actor):
    audit.authorize(org, actor, READERS)
    return [row for row in UipDocument.query.filter_by(organization_id=org).order_by(UipDocument.id.desc()).all()
            if accessible(org, actor, row)]


def folder(org, actor, name):
    audit.authorize(org, actor, WRITERS)
    name = text(name, 100, True)
    if UipDocumentFolder.query.filter_by(organization_id=org, name=name).first():
        abort(409, description="Folder already exists.")
    row = UipDocumentFolder(organization_id=org, name=name)
    db.session.add(row)
    audit.record(org, actor, "document.folder_created", row)
    return row


def root(org):
    # Private instance storage, never a static/public upload directory.
    path = Path(current_app.instance_path) / "uip_documents" / str(int(org))
    return path.resolve()


def metadata(org, actor, document_id, values):
    audit.authorize(org, actor, WRITERS)
    row = UipDocument.query.filter_by(organization_id=org, id=document_id).with_for_update().first_or_404()
    if not accessible(org, actor, row):
        abort(404)
    classification = values.get("access_classification")
    if classification not in VISIBILITY:
        abort(400, description="Select a visibility classification.")
    audit.authorize(org, actor, VISIBILITY[classification])
    folder_id = values.get("folder_id")
    folder_id = UipDocumentFolder.query.filter_by(organization_id=org, id=identifier(folder_id)).first_or_404().id if folder_id else None
    row.title = text(values.get("title"), 255, True)
    row.category = text(values.get("category"), 100, True)
    row.folder_id, row.access_classification = folder_id, classification
    audit.record(org, actor, "document.metadata_updated", row)
    return row


def upload(org, actor, file, values, document_id=None):
    audit.authorize(org, actor, WRITERS)
    if not file:
        abort(400, description="Select a file.")
    filename = secure_filename(file.filename or "")
    extension = Path(filename).suffix.lower()
    if not filename or len(filename) > 255 or extension not in TYPES:
        abort(400, description="Supported files: PDF, PNG, JPEG and UTF-8 text.")
    limit = int(current_app.config.get("UIP_DOCUMENT_MAX_BYTES", 10 * 1024 * 1024))
    content = file.stream.read(limit + 1)
    if not content or len(content) > limit:
        abort(400, description="The document is empty or exceeds the upload limit.")
    valid = {".pdf": content.startswith(b"%PDF-"), ".png": content.startswith(b"\x89PNG\r\n\x1a\n"),
             ".jpg": content.startswith(b"\xff\xd8\xff"), ".jpeg": content.startswith(b"\xff\xd8\xff")}
    if extension == ".txt":
        try:
            content.decode("utf-8")
        except UnicodeDecodeError:
            abort(400, description="Text documents must be UTF-8.")
    elif not valid[extension]:
        abort(400, description="File content does not match its type.")
    try:
        effective = date.fromisoformat(values.get("effective_date", ""))
    except (ValueError, TypeError):
        abort(400, description="An effective date is required.")
    if document_id:
        row = UipDocument.query.filter_by(organization_id=org, id=document_id).populate_existing().with_for_update().first_or_404()
        if not accessible(org, actor, row):
            abort(404)
        try:
            expected = int(values.get("expected_version"))
        except (ValueError, TypeError):
            abort(400)
        if expected != row.current_version:
            abort(409, description="The document has been replaced; reload.")
    else:
        classification = values.get("access_classification")
        if classification not in VISIBILITY:
            abort(400, description="Select an access level.")
        audit.authorize(org, actor, VISIBILITY[classification])
        folder_id = values.get("folder_id")
        if folder_id:
            folder_id = UipDocumentFolder.query.filter_by(organization_id=org, id=identifier(folder_id)).first_or_404().id
        row = UipDocument(organization_id=org, uploader_id=actor, filename=filename,
            file_type=extension[1:], title=text(values.get("title"), 255, True),
            category=text(values.get("category"), 100, True), folder_id=folder_id or None,
            access_classification=classification, current_version=0)
        db.session.add(row)
    reason = text(values.get("replacement_reason"), required=bool(document_id)) or "Initial version"
    row.current_version += 1
    db.session.flush()
    key = uuid.uuid4().hex
    version = UipDocumentVersion(organization_id=org, document_id=row.id, version=row.current_version,
        effective_date=effective, actor_user_id=actor, filename=filename, storage_key=key,
        content_type=TYPES[extension], size_bytes=len(content), sha256=hashlib.sha256(content).hexdigest(),
        replacement_reason=reason)
    db.session.add(version)
    audit.record(org, actor, "document.replaced" if document_id else "document.created", row)
    directory = root(org)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / key
    created = False
    try:
        with path.open("xb") as stream:
            created = True
            stream.write(content)
    except Exception:
        if created and path.exists():
            path.unlink()
        raise
    return row, path


def download(org, actor, document_id, version_number):
    audit.authorize(org, actor, READERS)
    row = UipDocument.query.filter_by(organization_id=org, id=document_id).first_or_404()
    if not accessible(org, actor, row):
        abort(404)
    version = UipDocumentVersion.query.filter_by(organization_id=org, document_id=row.id,
                                                version=version_number).first_or_404()
    if len(version.storage_key) != 32 or any(c not in "0123456789abcdef" for c in version.storage_key):
        abort(404)
    path = root(org) / version.storage_key
    if not path.is_file():
        abort(404)
    return path, version
