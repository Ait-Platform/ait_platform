"""Platform-admin uploaded PDF publication using HOME private versions and delivery."""
import hashlib
import io
import uuid
from pathlib import PurePath
from flask import abort
from flask_login import current_user
from pypdf import PdfReader
from app.extensions import db
from app.models.auth import User, ApprovedAdmin
from app.models.sace_home import HomeDocument, HomeDocumentVersion, now
from . import document_storage as storage, examination as ex, service as s

KINDS = {kind: ex.ITEMS.get(kind, s.ITEMS.get(kind)) for kind in (
    'application_form_1', 'application_form_2', 'timetable',
    'participant_manual', 'facilitator_manual')}
MAX_BYTES = 20 * 1024 * 1024


def require_platform_admin():
    if not current_user.is_authenticated:
        abort(403)
    user = db.session.query(User).filter_by(id=current_user.id).populate_existing().with_for_update().first()
    if user is None or not user.is_active:
        abort(403)
    grant = (ApprovedAdmin.query.filter(db.func.lower(ApprovedAdmin.email) == user.email.lower(),
        db.cast(ApprovedAdmin.active, db.String).in_(['true', '1'])).populate_existing().with_for_update().first())
    if grant is None:
        abort(403)
    return user


def publish_uploaded_pdf(kind, upload):
    actor = require_platform_admin()
    if kind not in KINDS or upload is None or not upload.filename:
        raise ValueError('Select a HOME endorsement document and a PDF file.')
    content = upload.stream.read(MAX_BYTES + 1)
    if not content or len(content) > MAX_BYTES or not content.startswith(b'%PDF-'):
        raise ValueError('Upload a PDF of at most 20 MB.')
    try:
        pdf = PdfReader(io.BytesIO(content), strict=True)
        if pdf.is_encrypted or not len(pdf.pages):
            raise ValueError('Encrypted or empty PDF')
    except Exception as exc:
        raise ValueError('Upload a readable, unencrypted PDF with at least one page.') from exc
    digest = hashlib.sha256(content).hexdigest()
    if digest in ex.reading_artifact_hashes():
        raise ValueError('Frozen Reading documents cannot be published as HOME evidence.')
    label = 'admin-' + uuid.uuid4().hex
    key = 'endorsement/admin/' + label + '.pdf'
    approved_at = now().isoformat()
    manifest = {'subject': s.SUBJECT, 'kind': kind, 'pdf_sha256': digest,
        'source_filename': PurePath(upload.filename.replace('\\', '/')).name[:255],
        'home_approval': {'approved_by': actor.id, 'approved_at': approved_at,
            'reference': 'admin-approved uploaded PDF'},
        'publication_method': 'admin_uploaded_pdf'}
    # Same lock as controller publication; no current version or binding is mutated.
    db.session.execute(db.text('SELECT pg_advisory_xact_lock(74831029)'))
    document = HomeDocument.query.filter_by(kind=kind).first()
    if document is None:
        document = HomeDocument(kind=kind, title=KINDS[kind])
        db.session.add(document)
        db.session.flush()
    storage.store(key, content, digest, require_r2=True)
    row = HomeDocumentVersion(document_id=document.id, version=label, storage_key=key,
        sha256=digest, source_manifest=manifest, approved_by_admin_user_id=actor.id)
    db.session.add(row)
    db.session.flush()
    return row
