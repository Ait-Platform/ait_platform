"""HOME-only operational authority and engagement lifecycle. Callers commit once."""
import secrets
from datetime import timedelta
from flask import abort, g, has_request_context
from flask_login import current_user
from app.extensions import db
from app.models.auth import AuthSubject, AuthSubjectAdmin, User
from app.models.sace_home import (HomeController, HomeProvisioning, HomePledge,
    HomeInvitation, HomeAssignment, HomeEngagement as Engagement,
    HomeControllerAppointment as Appointment, HomeAuditEvent, now)

SUBJECT = 'sace_home_endorsement'


def subject_lock():
    subject = AuthSubject.query.filter_by(slug=SUBJECT, is_active=1).populate_existing().with_for_update().first()
    if subject is None:
        abort(503, description='The dedicated HOME endorsement subject must be configured and active.')
    return subject


def operational():
    return db.or_(Engagement.status == 'active', db.and_(
        Engagement.status == 'completion_pending', Engagement.completion_deadline > now()))


def appointment(user=None, lock=False):
    user = user or current_user
    if not user.is_authenticated:
        return None
    if lock:
        subject_lock()
    return (Appointment.query.join(HomeController, HomeController.id == Appointment.controller_id)
        .join(Engagement, Engagement.id == Appointment.engagement_id)
        .join(AuthSubjectAdmin, AuthSubjectAdmin.id == Appointment.operational_grant_id)
        .join(AuthSubject, AuthSubject.id == AuthSubjectAdmin.subject_id)
        .filter(HomeController.user_id == user.id, HomeController.active.is_(True),
            Appointment.status == 'active', operational(), AuthSubject.slug == SUBJECT,
            AuthSubject.is_active == 1, Appointment.grant_subject_id == AuthSubject.id,
            Appointment.grant_id_at_issue == AuthSubjectAdmin.id,
            db.func.lower(AuthSubjectAdmin.email) == db.func.lower(Appointment.grant_email_at_issue),
            db.func.lower(AuthSubjectAdmin.email) == user.email.strip().lower())
        .populate_existing().first())


def require_appointment(lock=True):
    row = appointment(lock=lock)
    if row is None:
        abort(403, description='An active HOME appointment, engagement and exact operational grant are required.')
    if has_request_context():
        g.home_access = ('controller', row.engagement_id, None)
    return row


def assignments(user=None):
    user = user or current_user
    if not user.is_authenticated:
        return []
    return (HomeAssignment.query.join(HomeInvitation, HomeInvitation.id == HomeAssignment.invitation_id)
        .join(Appointment, Appointment.id == HomeInvitation.appointment_id)
        .join(Engagement, Engagement.id == Appointment.engagement_id)
        .join(AuthSubject, AuthSubject.id == Appointment.grant_subject_id)
        .join(AuthSubjectAdmin, AuthSubjectAdmin.id == Appointment.operational_grant_id)
        .join(HomeController, HomeController.id == Appointment.controller_id)
        .join(User, User.id == HomeController.user_id)
        .filter(HomeAssignment.auditor_id == user.id, HomeAssignment.status == 'active',
            HomeInvitation.controller_id == Appointment.controller_id, HomeInvitation.status == "claimed",
            Appointment.status == 'active', HomeController.active.is_(True),
            AuthSubjectAdmin.subject_id == AuthSubject.id,
            AuthSubjectAdmin.id == Appointment.grant_id_at_issue,
            db.func.lower(AuthSubjectAdmin.email) == db.func.lower(Appointment.grant_email_at_issue),
            db.func.lower(AuthSubjectAdmin.email) == db.func.lower(User.email),
            AuthSubject.slug == SUBJECT, AuthSubject.is_active == 1, operational())
        .order_by(HomeAssignment.id.desc()).populate_existing().all())


def finalize_assignment(assignment_id):
    actor = require_appointment()
    row = (HomeAssignment.query.join(HomeInvitation, HomeInvitation.id == HomeAssignment.invitation_id)
        .filter(HomeAssignment.id == assignment_id, HomeInvitation.appointment_id == actor.id)
        .populate_existing().with_for_update().first_or_404())
    if row.status == 'completed':
        return row  # Historical closures and retries remain unchanged.
    from . import service
    if row.status != 'active' or service.submission(row) is None:
        abort(409, description='The Auditor must submit the examination before finalization.')
    row.status, row.completed_at = 'completed', now()
    audit(current_user.id, 'controller', 'assignment_finalized', actor.engagement_id, row.id)
    return row


def invitation_appointment(invitation):
    row = db.session.get(Appointment, invitation.appointment_id) if invitation.appointment_id else None
    if row is None or row.controller_id != invitation.controller_id:
        abort(403, description='HOME invitation has no current engagement linkage.')
    owner = db.session.get(HomeController, row.controller_id)
    user = db.session.get(User, owner.user_id) if owner else None
    active = appointment(user) if user else None
    if active is None or active.id != row.id:
        abort(403, description='HOME invitation authority has ended.')
    return row


def audit(user_id, role, event, engagement_id=None, assignment_id=None, details=None):
    row = HomeAuditEvent(user_id=user_id, role=role, event=event,
        engagement_id=engagement_id, assignment_id=assignment_id, details=details or {})
    db.session.add(row)
    db.session.flush()
    return row


def provision(provisioning, pledge):
    subject = subject_lock()
    User.query.filter_by(id=current_user.id).with_for_update().one()
    owner = HomeController.query.filter_by(user_id=current_user.id).first()
    if owner and not owner.active:
        abort(403, description='HOME controller identity is revoked.')
    if owner and Appointment.query.filter_by(controller_id=owner.id, status='active').first():
        abort(409, description='An existing HOME appointment must be resolved before a new engagement.')
    if AuthSubjectAdmin.query.filter(AuthSubjectAdmin.subject_id == subject.id,
            db.func.lower(AuthSubjectAdmin.email) == current_user.email.strip().lower()).first():
        abort(409, description='An unlinked HOME grant cannot be adopted automatically.')
    if owner is None:
        owner = HomeController(user_id=current_user.id)
        db.session.add(owner)
    db.session.flush()
    engagement = Engagement(reference='HOME-' + secrets.token_hex(8).upper(), created_by_user_id=current_user.id)
    grant = AuthSubjectAdmin(subject_id=subject.id, email=current_user.email.strip().lower())
    db.session.add_all([engagement, grant]); db.session.flush()
    row = Appointment(controller_id=owner.id, engagement_id=engagement.id,
        operational_grant_id=grant.id, grant_id_at_issue=grant.id, grant_subject_id=subject.id,
        grant_email_at_issue=grant.email, provisioning_id=provisioning.id, pledge_id=pledge.id)
    db.session.add(row); db.session.flush()
    audit(current_user.id, 'controller', 'controller_provisioned', engagement.id,
        details={'appointment_id': row.id, 'grant_id': grant.id, 'pledge_id': pledge.id,
                 'provisioning_id': provisioning.id})
    return owner


def request_completion():
    actor = require_appointment()
    row = db.session.get(Engagement, actor.engagement_id, populate_existing=True)
    if row.status != 'active':
        abort(409, description='HOME completion is already pending or closed.')
    requested = now()
    row.status = 'completion_pending'
    row.completion_requested_by_user_id = current_user.id
    row.completion_requested_at = requested
    row.completion_deadline = requested + timedelta(hours=48)
    audit(current_user.id, 'controller', 'completion_requested', row.id,
        details={'requested_at': requested.isoformat(), 'deadline': row.completion_deadline.isoformat()})
    return row


def cancel_completion():
    actor = require_appointment()
    row = db.session.get(Engagement, actor.engagement_id, populate_existing=True)
    if row.status != 'completion_pending' or now() >= row.completion_deadline:
        abort(409, description='HOME completion cannot be cancelled.')
    audit(current_user.id, 'controller', 'completion_cancelled', row.id, details={
        'requester': row.completion_requested_by_user_id,
        'requested_at': row.completion_requested_at.isoformat(), 'deadline': row.completion_deadline.isoformat()})
    row.status = 'active'
    row.completion_requested_by_user_id = None
    row.completion_requested_at = None
    row.completion_deadline = None
    return row


def finalize_due_completions():
    subject = subject_lock()
    rows = Engagement.query.filter(Engagement.status == 'completion_pending',
        Engagement.completion_deadline <= now()).order_by(Engagement.id).populate_existing().all()
    for engagement in rows:
        for row in Appointment.query.filter_by(engagement_id=engagement.id, status='active').all():
            grant = db.session.get(AuthSubjectAdmin, row.operational_grant_id)
            if (grant is None or grant.id != row.grant_id_at_issue or grant.subject_id != subject.id
                    or row.grant_subject_id != subject.id or grant.email != row.grant_email_at_issue):
                abort(409, description='HOME exact-grant mismatch requires review.')
            row.operational_grant_id = None
            row.status, row.ended_at = 'completed', now()
            audit(engagement.completion_requested_by_user_id, 'controller', 'grant_retired', engagement.id,
                details={'grant_id': grant.id, 'appointment_id': row.id})
            db.session.delete(grant)
        # Leave original claims/evidence intact; parent engagement now denies access.
        engagement.status, engagement.completed_at = 'completed', now()
        audit(engagement.completion_requested_by_user_id, 'controller', 'engagement_completed', engagement.id,
            details={'effective_at': engagement.completion_deadline.isoformat()})
    db.session.flush()
    return [row.id for row in rows]
