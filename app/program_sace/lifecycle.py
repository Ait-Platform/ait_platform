"""Reading-only lifecycle. Callers commit once; no operation touches HOME or user accounts.

Lock order for mutations: SACE subject -> engagement -> appointment -> invitation.
The common subject lock serializes grant issue/retirement and assignment closure.
"""
import hashlib
import json
import secrets
from datetime import datetime, timezone, timedelta
from flask import abort
from flask_login import current_user
from app.extensions import db
from app.models.auth import AuthSubject, AuthSubjectAdmin, User
from app.models.sace import SaceWorkshopInteraction as Interaction
from app.models.sace_reading_engagement import (
    ReadingEngagement as Engagement, ReadingControllerAppointment as Appointment,
    ReadingAssignmentContext as AssignmentContext, utcnow)

SUBJECT = 'sace_endorsement'


def payload(row):
    try:
        value = json.loads(row.response_data)
        return value if isinstance(value, dict) else {}
    except (ValueError, TypeError, AttributeError):
        return {}


def subject_lock():
    row = AuthSubject.query.filter_by(slug=SUBJECT, is_active=1).with_for_update().first()
    if row is None:
        abort(503, description='SACE Reading is not configured.')
    return row


def operational_engagement():
    # Deadline enforcement is independent of housekeeping persistence.
    return db.or_(Engagement.status == 'active', db.and_(
        Engagement.status == 'completion_pending', Engagement.completion_deadline > utcnow()))


def controller(user=None, lock=False):
    user = user or current_user
    if not user.is_authenticated:
        return None
    if lock:
        subject_lock()
    return (Appointment.query.join(Engagement, Engagement.id == Appointment.engagement_id)
        .join(AuthSubjectAdmin, AuthSubjectAdmin.id == Appointment.operational_grant_id)
        .join(AuthSubject, AuthSubject.id == AuthSubjectAdmin.subject_id)
        .filter(Appointment.user_id == user.id, Appointment.status == 'active',
            operational_engagement(), AuthSubject.slug == SUBJECT, AuthSubject.is_active == 1,
            Appointment.grant_subject_id == AuthSubject.id,
            Appointment.grant_id_at_issue == AuthSubjectAdmin.id,
            db.func.lower(AuthSubjectAdmin.email) == user.email.strip().lower())
        .populate_existing().first())


def require_controller(lock=True):
    row = controller(lock=lock)
    if row is None:
        abort(403, description='An active Reading controller appointment and its operational grant are required.')
    return row


def event(user_id, action, details, engagement_id=None, timestamp=None):
    row = Interaction(user_id=user_id, activity_slug=action,
        workshop_session_id=f'reading-engagement-{engagement_id}' if engagement_id else 'reading-provisioning',
        response_data=json.dumps(details), **({'timestamp': timestamp} if timestamp else {}))
    db.session.add(row)
    db.session.flush()
    return row


def engagement_for_assignment(row, lock=False):
    if row is None:
        return None
    if lock:
        subject_lock()
    return (Engagement.query.join(AssignmentContext, AssignmentContext.engagement_id == Engagement.id)
        .filter(AssignmentContext.invitation_event_id == row.id, operational_engagement())
        .populate_existing().first())


def link_assignment(row, appointment, provenance_event_id):
    if row.activity_slug != 'auditor_provisioned' or row.user_id != appointment.user_id:
        abort(409, description='Invitation issuer does not match the controller appointment.')
    link = AssignmentContext(invitation_event_id=row.id, engagement_id=appointment.engagement_id,
        issuing_appointment_id=appointment.id, provenance_event_id=provenance_event_id)
    db.session.add(link)
    db.session.flush()
    return link


def issue_handover(target_email):
    appointment = require_controller()
    email = (target_email or '').strip().lower()
    if not email or '@' not in email or email == current_user.email.strip().lower():
        abort(400, description='Specify the successor official email address.')
    token = secrets.token_urlsafe(32)
    row = event(current_user.id, 'controller_handover_invited', dict(
        token_hash=hashlib.sha256(token.encode()).hexdigest(), email=email,
        issuing_appointment_id=appointment.id, engagement_id=appointment.engagement_id,
        expires_at=(utcnow() + timedelta(hours=24)).isoformat(), status='pending'), appointment.engagement_id)
    return row, token


def handover_invitation(token=None, event_id=None):
    if event_id is not None:
        row = db.session.get(Interaction, event_id)
    else:
        digest = hashlib.sha256((token or '').encode()).hexdigest()
        row = next((r for r in Interaction.query.filter_by(activity_slug='controller_handover_invited').all()
                    if secrets.compare_digest(str(payload(r).get('token_hash', '')), digest)), None)
    if not row or row.activity_slug != 'controller_handover_invited':
        abort(400, description='Invalid handover invitation.')
    data = payload(row)
    issuer = db.session.get(Appointment, data.get('issuing_appointment_id'))
    engagement = db.session.get(Engagement, data.get('engagement_id'))
    issuer_user = db.session.get(User, issuer.user_id) if issuer else None
    if (data.get('status') != 'pending' or datetime.fromisoformat(data['expires_at']) <= utcnow()
            or not issuer or not engagement or not Engagement.query.filter(Engagement.id == engagement.id, operational_engagement()).first()
            or not issuer_user or not controller(issuer_user) or issuer.status != 'active'):
        abort(409, description='Handover invitation is no longer active.')
    return row


def provision(ctx, pledge):
    """Called from secured complete_provisioning, inside its subject-locked transaction."""
    subject = subject_lock()
    uid = current_user.id
    email = current_user.email.strip().lower()
    if Appointment.query.filter_by(user_id=uid, status='active').first():
        abort(409, description='An active controller appointment already exists; review it before provisioning.')
    if AuthSubjectAdmin.query.filter(AuthSubjectAdmin.subject_id == subject.id,
            db.func.lower(AuthSubjectAdmin.email) == email).first():
        abort(409, description='An existing operational grant requires reviewed cutover; it cannot be adopted automatically.')
    invitation = None
    engagement = None
    if ctx.get('handover_event_id'):
        invitation = handover_invitation(event_id=ctx['handover_event_id'])
        data = payload(invitation)
        if data['email'] != email:
            abort(403, description='Sign in as the official named in the handover invitation.')
        engagement = db.session.get(Engagement, data['engagement_id'])
    grant = AuthSubjectAdmin(subject_id=subject.id, email=email)
    db.session.add(grant)
    db.session.flush()
    provenance = event(uid, 'controller_provisioned', dict(subject=SUBJECT,
        authority='administrator_provisioning_journey', journey=ctx['nonce']))
    if engagement is None:
        engagement = Engagement(reference='LITRE-' + secrets.token_hex(8).upper(),
            created_by_user_id=uid, provenance_event_id=provenance.id)
        db.session.add(engagement)
        db.session.flush()
    appointment = Appointment(engagement_id=engagement.id, user_id=uid,
        operational_grant_id=grant.id, grant_id_at_issue=grant.id,
        grant_subject_id=subject.id, grant_email_at_issue=email,
        pledge_event_id=pledge.id, provisioning_event_id=provenance.id)
    db.session.add(appointment)
    db.session.flush()
    provenance.workshop_session_id = f'reading-engagement-{engagement.id}'
    provenance.response_data = json.dumps(dict(payload(provenance), engagement_id=engagement.id,
        appointment_id=appointment.id, grant_id=grant.id))
    if invitation:
        invitation.response_data = json.dumps(dict(payload(invitation), status='consumed',
            accepted_by_user_id=uid, appointment_id=appointment.id, consumed_at=utcnow().isoformat()))
    return appointment


def revoke_invitations(engagement_id, actor_id, reason, issuer_id=None):
    query = AssignmentContext.query.filter_by(engagement_id=engagement_id)
    if issuer_id is not None:
        query = query.filter_by(issuing_appointment_id=issuer_id)
    for link in query.all():
        row = db.session.get(Interaction, link.invitation_event_id)
        data = payload(row)
        affected = ('Unclaimed',) if issuer_id is not None else ('Unclaimed', 'Claimed')
        if data.get('status') in affected:
            row.response_data = json.dumps(dict(data, status='Revoked', revoked_at=utcnow().isoformat(), reason=reason))
            event(actor_id, 'assignment_revoked', dict(invitation_event_id=row.id, reason=reason), engagement_id)
    for row in Interaction.query.filter_by(activity_slug='controller_handover_invited',
            workshop_session_id=f'reading-engagement-{engagement_id}').all():
        data = payload(row)
        if data.get('status') == 'pending' and (issuer_id is None or data.get('issuing_appointment_id') == issuer_id):
            row.response_data = json.dumps(dict(data, status='revoked', reason=reason))


def end_appointment(appointment_id, status, reason, actor=None):
    actor = actor or require_controller()
    subject_lock()
    row = db.session.get(Appointment, appointment_id, populate_existing=True)
    if not row or row.engagement_id != actor.engagement_id:
        abort(404)
    if status not in ('completed', 'revoked') or not (reason or '').strip():
        abort(400, description='Choose completion or revocation and provide a reason.')
    if row.status != 'active':
        abort(409, description='Appointment has already ended.')
    return _retire_appointment(row, status, reason, current_user.id)


def _retire_appointment(row, status, reason, actor_id):
    row.status = status
    setattr(row, 'completed_at' if status == 'completed' else 'revoked_at', utcnow())
    row.ended_by_user_id = actor_id
    row.end_reason = reason.strip()
    grant = db.session.get(AuthSubjectAdmin, row.operational_grant_id) if row.operational_grant_id else None
    if grant and (grant.id != row.grant_id_at_issue or grant.subject_id != row.grant_subject_id):
        abort(409, description='Operational grant mismatch requires review.')
    row.operational_grant_id = None
    event(actor_id, 'controller_appointment_ended', dict(appointment_id=row.id,
        status=status, reason=row.end_reason, retired_grant_id=grant.id if grant else None), row.engagement_id)
    if grant:
        db.session.delete(grant)
    revoke_invitations(row.engagement_id, actor_id, reason, issuer_id=row.id)
    db.session.flush()
    return row


def _close_engagement(engagement, status, reason, actor_id):
    for row in Appointment.query.filter_by(engagement_id=engagement.id, status='active').order_by(Appointment.id).all():
        _retire_appointment(row, status, reason, actor_id)
    revoke_invitations(engagement.id, actor_id, reason)
    engagement.status = status
    setattr(engagement, 'completed_at' if status == 'completed' else 'revoked_at', utcnow())
    engagement.ended_by_user_id = actor_id
    engagement.end_reason = reason.strip()
    event(actor_id, 'reading_engagement_ended', dict(status=status, reason=reason), engagement.id)
    db.session.flush()
    return engagement


def end_engagement(status, reason):
    actor = require_controller()
    if status != 'revoked' or not (reason or '').strip():
        abort(400, description='Completion requires confirmation and the 48-hour safeguard.')
    engagement = db.session.get(Engagement, actor.engagement_id, populate_existing=True)
    return _close_engagement(engagement, status, reason, current_user.id)


def request_completion():
    actor = require_controller()  # Same subject lock as cancellation/finalization.
    engagement = db.session.get(Engagement, actor.engagement_id, populate_existing=True)
    if engagement.status != 'active':
        abort(409, description='Completion is already pending or the engagement has ended.')
    requested = utcnow()
    engagement.status = 'completion_pending'
    engagement.completion_requested_by_user_id = current_user.id
    engagement.completion_requested_at = requested
    engagement.completion_deadline = requested + timedelta(hours=48)
    event(current_user.id, 'reading_completion_requested', dict(
        requested_at=requested.isoformat(), deadline=engagement.completion_deadline.isoformat()),
        engagement.id, timestamp=requested.replace(tzinfo=None))
    db.session.flush()
    return engagement


def cancel_completion():
    actor = require_controller()
    engagement = db.session.get(Engagement, actor.engagement_id, populate_existing=True)
    if engagement.status != 'completion_pending' or utcnow() >= engagement.completion_deadline:
        abort(409, description='There is no cancellable pending completion.')
    event(current_user.id, 'reading_completion_cancelled', dict(
        requested_by_user_id=engagement.completion_requested_by_user_id,
        requested_at=engagement.completion_requested_at.isoformat(),
        deadline=engagement.completion_deadline.isoformat()), engagement.id)
    engagement.status = 'active'
    engagement.completion_requested_by_user_id = None
    engagement.completion_requested_at = None
    engagement.completion_deadline = None
    db.session.flush()
    return engagement


def finalize_due_completions():
    """Operator housekeeping; caller commits. No login/HTTP authority bypass."""
    subject_lock()
    rows = Engagement.query.filter(Engagement.status == 'completion_pending',
        Engagement.completion_deadline <= utcnow()).order_by(Engagement.id).populate_existing().all()
    for engagement in rows:
        _close_engagement(engagement, 'completed', '48-hour completion period elapsed',
            engagement.completion_requested_by_user_id)
    return [row.id for row in rows]


def reviewed_cutover(manifest, reviewed_by_user_id):
    """Explicit operator-only service, no HTTP route or startup invocation.

    Caller must review IDs and commit/rollback. This function never guesses which
    production grants are legitimate. It supports only the approved 622/623/630 case.
    """
    subject = subject_lock()
    required = {'reference', 'r_provisioning_event_id', 'r_pledge_event_id', 'auditor_assignments', 'reason'}
    if set(manifest) != required or not manifest['reason'].strip() or not manifest['auditor_assignments']:
        abort(400, description='An exact reviewed cutover manifest is required.')
    reviewed = manifest['auditor_assignments']
    if not isinstance(reviewed, list) or any(
        not isinstance(item, dict) or set(item) != {'invitation_id', 'auditor_user_id', 'status'}
        or type(item['invitation_id']) is not int or type(item['auditor_user_id']) is not int
        or item['status'] not in ('Claimed', 'Completed', 'Revoked') for item in reviewed
    ) or len({item['invitation_id'] for item in reviewed}) != len(reviewed):
        abort(400, description='Unique, explicit reviewed Auditor assignment mappings are required.')
    digest = hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest()
    if not db.session.get(User, reviewed_by_user_id):
        abort(400, description='The reviewing actor must be an existing identified user.')
    existing = Engagement.query.filter_by(reference=manifest['reference']).first()
    if existing:
        evidence = db.session.get(Interaction, existing.provenance_event_id)
        if evidence.activity_slug != 'reading_cutover_reviewed' or payload(evidence).get('manifest_sha256') != digest:
            abort(409, description='Cutover reference already exists with different provenance.')
        return existing
    r = db.session.get(User, 622)
    test_user = db.session.get(User, 630)
    grant = db.session.get(AuthSubjectAdmin, 2)
    test_grant = db.session.get(AuthSubjectAdmin, 3)
    def verified_grant(g, u):
        return g and u and g.subject_id == subject.id and g.email.lower() == u.email.strip().lower()
    if not verified_grant(grant, r) or not verified_grant(test_grant, test_user):
        abort(409, description='Reviewed user/grant mapping does not match the database.')
    if Appointment.query.filter(Appointment.user_id.in_([622, 630])).first():
        abort(409, description='Existing controller history requires separate review.')
    def verified_event(event_id, uid, slug):
        row = db.session.get(Interaction, event_id)
        if not row or row.user_id != uid or row.activity_slug != slug:
            abort(409, description='Reviewed event identity does not match the database.')
        return row
    provision = verified_event(manifest['r_provisioning_event_id'], 622, 'controller_provisioned')
    pledge = verified_event(manifest['r_pledge_event_id'], 622, 'admin_patent_pledge')
    verified_event(304, 630, 'controller_provisioned')
    verified_event(305, 630, 'admin_patent_pledge')
    invitations = []
    for item in reviewed:
        row = verified_event(item['invitation_id'], 622, 'auditor_provisioned')
        if (db.session.get(User, item['auditor_user_id']) is None
                or payload(row).get('claimed_by_user_id') != item['auditor_user_id']
                or payload(row).get('status') != item['status']):
            abort(409, description='Auditor assignment does not match the reviewed user and state.')
        if db.session.get(AssignmentContext, row.id):
            abort(409, description='Assignment already belongs to an engagement.')
        invitations.append(row)
    provenance = event(reviewed_by_user_id, 'reading_cutover_reviewed', dict(
        manifest=manifest, manifest_sha256=digest, legitimate_user_id=622,
        retained_grant_id=2, test_user_id=630, retired_grant_id=3, preserved_test_events=[304, 305]))
    # Legacy event timestamps are naive UTC; preserve them, do not claim a new acceptance.
    started = provision.timestamp.replace(tzinfo=timezone.utc)
    engagement = Engagement(reference=manifest['reference'], created_by_user_id=622,
        started_at=started, provenance_event_id=provenance.id)
    db.session.add(engagement)
    db.session.flush()
    appointment = Appointment(engagement_id=engagement.id, user_id=622,
        operational_grant_id=2, grant_id_at_issue=2, grant_subject_id=subject.id,
        grant_email_at_issue=grant.email, started_at=started,
        pledge_event_id=pledge.id, provisioning_event_id=provision.id)
    db.session.add(appointment)
    db.session.flush()
    for row in invitations:
        link_assignment(row, appointment, provenance.id)
    provenance.workshop_session_id = f'reading-engagement-{engagement.id}'
    event(reviewed_by_user_id, 'controller_test_grant_retired', dict(user_id=630,
        retired_grant_id=3, preserved_events=[304, 305], reason=manifest['reason'],
        manifest_sha256=digest), engagement.id)
    db.session.delete(test_grant)
    db.session.flush()
    return engagement
