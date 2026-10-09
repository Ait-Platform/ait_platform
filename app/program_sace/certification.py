"""Immutable certification of the existing Reading Auditor Board statuses."""
import hashlib
import json
from datetime import datetime, timezone
from flask import abort, render_template
from app.extensions import db
from app.models.sace import SaceWorkshopInteraction as Interaction
from . import endorsement as flow

SCHEMA = 'reading-endorsement-certification-v1'
SLUG = 'reading_certification'
STATEMENT = ('This records that the auditor has completed examination of the Reading '
    'endorsement material recorded on the Auditor Board, including the workshop '
    'component and the 18-video Reading course.')
BOARD_ITEMS = (
    ('map_reviewed', 'Activity Summary', 'auditor_map', None),
    ('app_form', 'Application Form 1', 'secure_view', 'app_form'),
    ('app_form_2', 'Application Form 2', 'secure_view', 'app_form_2'),
    ('f_guide', 'Facilitator Manual', 'secure_view', 'f_guide'),
    ('p_guide', 'Workshop Manual', 'secure_view', 'p_guide'),
    ('ip_pledge', 'AIT IP Pledge (reference)', 'secure_view', 'ip_pledge'),
    ('timetable', 'Reading Timetable (T/T)', 'secure_view', 'timetable'),
    ('demo_complete', 'Workshop Online Interaction & Assessment — Steps 32–35', 'simulator', None),
    ('reading_complete', '18-video Reading Course', 'reading_course', None),
)


def digest(snapshot):
    return hashlib.sha256(json.dumps(snapshot, sort_keys=True, separators=(',', ':'),
        ensure_ascii=False).encode('utf-8')).hexdigest()


def available(row):
    return all(flow.latest(row, slug) is not None for slug, *_ in BOARD_ITEMS)


def saved(row):
    auditor_id = flow.payload(row).get('claimed_by_user_id')
    records = Interaction.query.filter_by(workshop_session_id=flow.room(row),
        user_id=auditor_id, activity_slug=SLUG).all()
    if len(records) > 1:
        abort(409, description='Conflicting Reading certifications require review.')
    if not records:
        return None
    evidence = records[0]
    details = flow.payload(evidence)
    snapshot = details.get('snapshot')
    html = details.get('html')
    if (details.get('schema') != SCHEMA or not isinstance(snapshot, dict)
            or not isinstance(html, str) or details.get('snapshot_sha256') != digest(snapshot)
            or details.get('html_sha256') != hashlib.sha256(html.encode('utf-8')).hexdigest()
            or snapshot.get('assignment_id') != row.id
            or snapshot.get('auditor', {}).get('id') != auditor_id):
        abort(409, description='The Reading certification snapshot does not match its audit record.')
    return evidence


def create(row, auditor):
    from .lifecycle import engagement_for_assignment
    from app.utils.branding import get_logo_data_uri, get_seal_data_uri
    if auditor.id != flow.payload(row).get('claimed_by_user_id') or not available(row):
        abort(409, description='Complete all nine Reading Board items before opening Certification.')
    items = []
    for slug, title, _, kind in BOARD_ITEMS:
        event = flow.latest(row, slug)
        items.append(dict(kind=slug, title=title,
            status='Examined' if kind or slug == 'map_reviewed' else 'Recorded',
            examined_at=event.timestamp.isoformat(), evidence_ids=[event.id],
            evidence_actor_id=event.user_id, recorded_facts=flow.payload(event)))
    engagement = engagement_for_assignment(row)
    snapshot = dict(schema=SCHEMA, programme='Reading', statement=STATEMENT,
        assignment_id=row.id, engagement_id=engagement.id, engagement_reference=engagement.reference,
        auditor=dict(id=auditor.id, name=auditor.name, email=auditor.email), provider='SACE',
        certified_at=datetime.now(timezone.utc).isoformat(), items=items)
    snapshot_digest = digest(snapshot)
    html = render_template('program_sace/certification_snapshot.html', snapshot=snapshot,
        digest=snapshot_digest, logo_path=get_logo_data_uri(), seal_path=get_seal_data_uri())
    evidence = Interaction(user_id=auditor.id, workshop_session_id=flow.room(row),
        activity_slug=SLUG, response_data=json.dumps(dict(schema=SCHEMA, snapshot=snapshot,
            snapshot_sha256=snapshot_digest, html=html,
            html_sha256=hashlib.sha256(html.encode('utf-8')).hexdigest())))
    # Append certification only; do not refresh or modify the existing journey state.
    db.session.add(evidence)
    db.session.flush()
    return evidence
