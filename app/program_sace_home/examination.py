"""Version-pinned HOME Auditor evidence, separate from frozen authority/lifecycle."""
import json
import re
from pathlib import Path
from flask import abort, current_app, url_for
from app.extensions import db
from app.models.sace_home import HomeEvidence, HomeDocumentVersion, HomeDocument
from . import content_snapshot as content

REQUIREMENTS = 'home-auditor-v2'
ITEMS = {
    'summary': 'Activity Summary', 'application_form_1': 'Application Form 1',
    'application_form_2': 'Application Form 2', 'timetable': 'HOME Programme / Timetable',
    'participant_manual': 'HOME Participant Manual', 'facilitator_manual': 'HOME Facilitator Manual',
    'experience': 'HOME Learning Journey', 'assessment': 'Assessment / Evaluation',
    'monitoring': 'Workshop Evaluation / Monitoring', 'certificate': 'Certificate / Diagnostic Report Evidence',
}
DOCUMENTS = {'application_form_1', 'application_form_2', 'timetable',
             'participant_manual', 'facilitator_manual', 'monitoring'}
STAGES = {'practical': 'Practical', 'application': 'Application / MCQ',
          'theory': 'Theory', 'review': 'Review / Retention'}


def root():
    directory = Path(current_app.config.get('SACE_HOME_EXAMINATION_ROOT',
        Path(current_app.instance_path) / 'sace_home_examination')).resolve()
    if 'static' in directory.parts:
        abort(503, description='HOME examination content must be private.')
    return directory


def selected():
    value = current_app.config.get('SACE_HOME_EXAMINATION_VERSION')
    if value is None:
        try:
            value = (root() / 'CURRENT').read_text(encoding='ascii').strip()
        except OSError:
            return None
    if not re.fullmatch('[a-f0-9]{64}', value):
        abort(503, description='Invalid HOME examination version configuration.')
    return value


def load(version):
    if not re.fullmatch('[a-f0-9]{64}', version or ''):
        abort(409, description='Invalid HOME examination version.')
    try:
        bundle = json.loads((root() / (version + '.json')).read_text(encoding='utf-8'))
        if bundle['version'] != version or content.digest(bundle['manifest']) != version:
            raise ValueError('Version mismatch')
        content.validate(bundle['manifest'])
        return bundle['manifest']
    except (OSError, ValueError, KeyError, TypeError):
        abort(409, description='The immutable HOME examination content is unavailable or invalid.')


def pinned(row, bind=False):
    from . import service as s
    if row.requirements_version != REQUIREMENTS:
        abort(409, description='This historical HOME assignment retains its original examination requirements.')
    events = HomeEvidence.query.filter_by(assignment_id=row.id, item='content_manifest', event='bound').all()
    if len(events) > 1:
        abort(409, description='Conflicting HOME examination manifests require review.')
    if events:
        return events[0].details['version'], load(events[0].details['version'])
    version = selected()
    if version is None:
        if bind:
            abort(409, description='Approved HOME educational examination content is not available.')
        return None, None
    manifest = load(version)
    if bind:
        s.record(row, 'content_manifest', 'bound', {'version': version, 'requirements': REQUIREMENTS})
    return version, manifest


def evidence(row, item, event, version):
    return next((e for e in HomeEvidence.query.filter_by(assignment_id=row.id, item=item, event=event).all()
                 if e.details.get('version') == version), None)


def open_item(row, item, version, details=None):
    from . import service as s
    if evidence(row, item, 'opened', version) is None:
        s.record(row, item, 'opened', dict(details or {}, version=version))


def confirm(row, item, posted_version, version, details=None):
    from . import service as s
    if posted_version != version or evidence(row, item, 'opened', version) is None:
        abort(409, description='Open this exact HOME examination version before confirming it.')
    if evidence(row, item, 'examined', version) is None:
        s.record(row, item, 'examined', dict(details or {}, version=version))


def chapter_details(manifest, number, stage):
    c = manifest['chapters'][number - 1]
    return {'stage': stage, 'chapter_number': number, 'chapter_id': c['id'],
            'content_hash': content.digest(c), 'question_ids': [q['id'] for q in c['questions']],
            'option_ids': [[o['id'] for o in q['options']] for q in c['questions']]}


def journey_complete(row, version):
    return bool(version) and all(evidence(row, f'{stage}:{number}', 'examined', version)
        for stage, numbers in content.STAGES.items() for number in numbers)


def render_html(row, version, html, manifest):
    for key in manifest['assets']:
        html = html.replace('home-asset:' + key, url_for('home_sace_bp.examination_asset',
            assignment_id=row.id, version=version, asset=key))
    return html


def valid_document(version):
    from . import service as s
    if version is None:
        return False
    m = version.source_manifest
    approval = m.get('home_approval', {})
    if m.get('subject') != s.SUBJECT or m.get('kind') != db.session.get(HomeDocument, version.document_id).kind:
        return False
    if not approval.get('approved_by') or not approval.get('reference'):
        return False
    # Reject the known frozen Reading artifacts even if relabelled in a manifest.
    if version.sha256 in reading_artifact_hashes():
        return False
    try:
        s.document_path(version)
    except Exception as exc:
        from werkzeug.exceptions import HTTPException
        if not isinstance(exc, HTTPException):
            raise
        return False
    return True


def reading_artifact_hashes():
    # Identities of the inspected frozen Reading PDFs, not files/routes imported at runtime.
    return {
        'c61debeaf305d8834eebcddd25240c9ca93e0e65dcc27cdf88daae8d90c21a63',
        '42dd21af0a3949d996654a978ea0613c6d3190648a5c2ef3a44b093858e23e85',
        'dbead72b84e6463896ed7cb7de228b1c9bc39fb60fbdd4a0c7e8240d1bcb5f10',
        '34e945d1cb49f64beac360d1d09bf4c2de523cd3dffdcebd88892c0628017ee4',
        '400f72f34e6997d386369765200e0e5e1210138eabfcdb4145e048455c5f46bb',
    }


def document(row, kind, bind=False):
    from . import service as s
    events = HomeEvidence.query.filter_by(assignment_id=row.id, item=kind, event='document_bound').all()
    if len(events) > 1:
        abort(409, description='Conflicting HOME document versions require review.')
    version = db.session.get(HomeDocumentVersion, events[0].document_version_id) if events else s.latest_version(kind)
    if not valid_document(version):
        return None
    if events and db.session.get(HomeDocument, version.document_id).kind != kind:
        abort(409)
    identity = {'id': version.id, 'document_id': version.document_id, 'version': version.version,
                'sha256': version.sha256, 'storage_key': version.storage_key,
                'source_manifest': version.source_manifest}
    manifest_hash = content.digest(identity)
    if events and events[0].details.get('manifest_hash') != manifest_hash:
        abort(409, description='The bound HOME document manifest has changed.')
    if bind and not events:
        s.record(row, kind, 'document_bound', {'sha256': version.sha256,
            'manifest_hash': manifest_hash, 'requirements': REQUIREMENTS}, version.id)
    return version


def board_items(row):
    from . import service as s
    version, manifest = pinned(row)
    values = []
    for kind, title in ITEMS.items():
        doc = document(row, kind) if kind in DOCUMENTS else None
        available = (kind == 'summary' or bool(doc)) if kind in DOCUMENTS or kind == 'summary' else bool(manifest)
        if kind in DOCUMENTS:
            done = bool(doc) and s.examined(row, kind, doc.id)
        elif kind == 'experience':
            done = journey_complete(row, version)
        elif kind in {'assessment', 'certificate'}:
            done = bool(version) and evidence(row, kind, 'examined', version) is not None
        else:
            done = s.examined(row, kind)
        values.append(dict(kind=kind, title=title, version=doc, available=available, examined=available and done))
    return values
