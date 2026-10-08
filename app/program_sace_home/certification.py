"""Static HOME endorsement certification over existing assignment evidence."""
import hashlib
from pathlib import Path
from flask import abort, render_template
from app.extensions import db
from app.models.auth import User
from app.models.sace_home import HomeEvidence, HomeInvitation, HomeController, HomeEngagement, now
from . import content_snapshot as content, examination as ex, lifecycle as lc, service as s

SCHEMA = 'home-endorsement-certification-v1'
STATEMENT = 'This records that the auditor has examined all ten items of HOME material submitted by the provider for SACE endorsement.'


def saved(row):
    records = [e for e in HomeEvidence.query.filter_by(assignment_id=row.id,
        actor_id=row.auditor_id, item='endorsement_certification', event='certified').all()
        if e.details.get('schema') == SCHEMA]
    if len(records) > 1:
        abort(409, description='Conflicting endorsement certifications require review.')
    if not records:
        return None
    evidence = records[0]
    details = evidence.details
    snapshot = details.get('snapshot', {})
    if (details.get('snapshot_sha256') != content.digest(snapshot)
            or details.get('html_sha256') != hashlib.sha256(details.get('html', '').encode('utf-8')).hexdigest()
            or snapshot.get('assignment_id') != row.id or snapshot.get('auditor', {}).get('id') != row.auditor_id):
        abort(409, description='The endorsement certification snapshot does not match its audit record.')
    return evidence


def create(row):
    items = ex.board_items(row)[:10]
    if len(items) != 10 or not all(item['examined'] for item in items):
        abort(409)
    invitation = db.session.get(HomeInvitation, row.invitation_id)
    appointment = db.session.get(lc.Appointment, invitation.appointment_id)
    owner = db.session.get(HomeController, invitation.controller_id)
    provider = db.session.get(User, owner.user_id)
    auditor = db.session.get(User, row.auditor_id)
    engagement = db.session.get(HomeEngagement, appointment.engagement_id)
    examined = []
    for item in items:
        kind, version = item['kind'], item['version']
        events = HomeEvidence.query.filter_by(assignment_id=row.id, actor_id=row.auditor_id, item=kind).order_by(HomeEvidence.id).all()
        events = [e for e in events if (e.event == 'examined'
            and e.document_version_id == (version.id if version else None))
            or (kind in ex.INSTRUMENTS and e.event == 'submitted')]
        if kind in ex.FUNCTIONAL_ITEMS:
            identity = content.digest(ex.evidence_status(row, kind))
            confirmations = [e for e in events if e.event == 'examined'
                and e.details.get('evidence_sha256') == identity]
            if confirmations:
                events = confirmations
            else:
                events = [e for e in events if e.event == 'submitted']
        if kind == 'experience' and not events:
            events = HomeEvidence.query.filter_by(assignment_id=row.id, actor_id=row.auditor_id,
                event='examined').filter(HomeEvidence.item.like('participant_chapter:%')).order_by(HomeEvidence.id).all()
        if not events:
            abort(409, description='Auditor examination evidence is missing.')
        latest = events[-1]
        material = (dict(document_version_id=version.id, version=version.version, sha256=version.sha256,
                    source_manifest=version.source_manifest) if version else latest.details.get('snapshot'))
        if material is None and kind in ex.FUNCTIONAL_ITEMS:
            material = ex.evidence_status(row, kind)
        if material is None:
            source = Path(__file__).resolve().parents[2] / 'templates/program_sace_home/summary.html'
            material = {'version': latest.details.get('version'), 'source_sha256': hashlib.sha256(source.read_bytes()).hexdigest()}
        examined.append({'kind': kind, 'title': item['title'], 'examined_at': latest.created_at.isoformat(),
            'evidence_ids': [e.id for e in events], 'material': material})
    snapshot = {'schema': SCHEMA, 'statement': STATEMENT, 'programme': 'HOME',
        'assignment_id': row.id, 'requirements': row.requirements_version,
        'engagement_id': engagement.id, 'engagement_reference': engagement.reference,
        'issuing_appointment_id': appointment.id,
        'auditor': {'id': auditor.id, 'name': auditor.name, 'email': auditor.email},
        'provider': {'id': provider.id, 'name': provider.name, 'email': provider.email},
        'certified_at': now().isoformat(), 'items': examined}
    digest = content.digest(snapshot)
    html = render_template('program_sace_home/certification_snapshot.html', snapshot=snapshot, digest=digest)
    return s.record(row, 'endorsement_certification', 'certified', {'schema': SCHEMA,
        'snapshot': snapshot, 'snapshot_sha256': digest, 'html': html,
        'html_sha256': hashlib.sha256(html.encode('utf-8')).hexdigest()})


def email_text(evidence):
    snapshot = evidence.details['snapshot']
    return '\n'.join([snapshot['statement'],
        'Auditor: ' + snapshot['auditor']['name'], 'Provider: ' + snapshot['provider']['name'],
        'Engagement: ' + snapshot['engagement_reference'], 'Certified: ' + snapshot['certified_at'],
        *[str(n) + '. ' + item['title'] + ' - Examined' for n, item in enumerate(snapshot['items'], 1)],
        'Snapshot SHA-256: ' + evidence.details['snapshot_sha256']])
