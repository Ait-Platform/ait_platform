"""Publish approved endorsement PDFs through the existing HOME version service."""
import hashlib
from pathlib import Path
from flask import current_app
from app.models.sace_home import HomeDocument, HomeDocumentVersion
from . import service as s, examination as ex

SOURCES = {
    'application_form_1': 'Signed Professional Development Activity Application form of duration from 6 days upwards (1).pdf',
    'application_form_2': 'Signed Professional Development Activity Form for  2 Hours TO 5 Days Programs (2).pdf',
    'timetable': 'TimeTable.pdf',
}


def publish(owner, source_directory, approved_by, reference):
    if not approved_by.strip() or not reference.strip():
        raise ValueError('Provide the HOME source approver and approval reference.')
    source_directory = Path(source_directory).resolve()
    root = Path(current_app.config.get('SACE_HOME_DOCUMENT_ROOT',
        Path(current_app.instance_path) / 'sace_home_documents')).resolve()
    prepared = []
    for kind, filename in SOURCES.items():
        content = (source_directory / filename).read_bytes()
        digest = hashlib.sha256(content).hexdigest()
        if not content.startswith(b'%PDF-') or digest in ex.reading_artifact_hashes():
            raise ValueError('An approved HOME PDF is required: ' + filename)
        prepared.append((kind, filename, content, digest))
    versions = {}
    for kind, filename, content, digest in prepared:
        existing = (HomeDocumentVersion.query.join(HomeDocument)
            .filter(HomeDocument.kind == kind, HomeDocumentVersion.sha256 == digest)
            .order_by(HomeDocumentVersion.published_at.desc(), HomeDocumentVersion.id.desc()).all())
        matching = next((row for row in existing if ex.valid_document(row)), None)
        if matching is not None:
            versions[kind] = matching
            continue
        storage_key = 'endorsement/' + digest + '.pdf'
        path = root / storage_key
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            if path.read_bytes() != content:
                raise ValueError('Controlled storage content mismatch.')
        else:
            with path.open('xb') as stream:
                stream.write(content)
        manifest = {'subject': s.SUBJECT, 'kind': kind, 'source_filename': filename,
            'source_sha256': digest, 'home_approval': {
                'approved_by': approved_by, 'reference': reference}}
        versions[kind] = s.publish_document(owner, kind, 'approved-' + digest[:48], storage_key, manifest)
        versions[kind].storage_key = storage_key
    return versions
