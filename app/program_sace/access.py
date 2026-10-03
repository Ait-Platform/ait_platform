"""Administrator provisioning continuation and persistent SACE-only controller access."""
from flask import session, abort, request
from flask_login import current_user
from app.extensions import db
from app.models.auth import AuthSubject, UserEnrollment
from app.models.sace import SaceWorkshopInteraction
import secrets
import time
from datetime import datetime, timezone
from urllib.parse import urlsplit, parse_qs

PROVISIONING_KEY = "sace_r_current_journey"
PROVISIONING_TTL = 900

def clear_provisioning():
    for key in (PROVISIONING_KEY, "sace_admin_provisioning", "sace_admin_pledged"):
        session.pop(key, None)

def provisioning_context(token=None):
    ctx = session.get(PROVISIONING_KEY)
    if not isinstance(ctx, dict):
        return None
    age = time.time() - ctx.get("started_at", 0)
    if not 0 <= age < PROVISIONING_TTL:
        clear_provisioning()
        return None
    if token is not None and not secrets.compare_digest(str(token), ctx["nonce"]):
        return None
    uid = ctx.get("user_id")
    if uid is not None and (not current_user.is_authenticated or uid != current_user.id):
        return None
    return ctx

def start_provisioning():
    clear_provisioning()
    ctx = dict(nonce=secrets.token_urlsafe(32), started_at=time.time(),
               user_id=current_user.id if current_user.is_authenticated else None)
    session[PROVISIONING_KEY] = ctx
    return ctx

def provisioning_auth_context(next_url):
    target = urlsplit(next_url or "")
    if target.scheme or target.netloc or target.path != "/sace/provisioning":
        return None
    token = parse_qs(target.query).get("journey", [""])[0]
    ctx = provisioning_context(token)
    return ctx if ctx and ctx.get("accepted_at") else None

def prepare_provisioning_auth(next_url):
    # Ordinary authentication abandons R continuation, never inherits its flags.
    if not provisioning_auth_context(next_url):
        clear_provisioning()

def authenticate_provisioning(next_url):
    ctx = provisioning_auth_context(next_url)
    if not ctx or not current_user.is_authenticated:
        clear_provisioning()
        return False
    ctx = dict(ctx, user_id=current_user.id)
    session[PROVISIONING_KEY] = ctx
    return True

def provisioning_destination():
    from flask import url_for
    ctx = provisioning_context()
    if ctx and ctx.get("accepted_at") and ctx.get("user_id") == current_user.id:
        return url_for("sace_bp.provisioning_map", journey=ctx["nonce"])
    return None


SUBJECT = "sace_endorsement"
def is_controller():
    if not current_user.is_authenticated:
        return False
    from .lifecycle import controller
    return controller() is not None



def complete_provisioning():
    """Called only after login and pledge; one transaction, no platform role."""
    ctx = provisioning_context()
    if (not current_user.is_authenticated or not ctx or not ctx.get("accepted_at")
            or ctx.get("user_id") != current_user.id):
        abort(400, description="Start a current administrator provisioning journey and accept its pledge.")
    # Serialize nonce consumption, grant creation and appointment creation.
    subject = AuthSubject.query.filter_by(slug=SUBJECT, is_active=1).with_for_update().first()
    if subject is None:
        abort(503, description="SACE Endorsement is not configured.")
    # The subject lock serializes consumption, including replayed signed cookies.
    if SaceWorkshopInteraction.query.filter(
        SaceWorkshopInteraction.activity_slug == "controller_provisioned",
        SaceWorkshopInteraction.response_data.contains(ctx["nonce"], autoescape=True),
    ).first():
        abort(400, description="This provisioning journey has already been used.")
    from . import lifecycle
    # Pledge history is evidence, not a substitute for this appointment's pledge.
    pledge = lifecycle.event(current_user.id, 'admin_patent_pledge',
        {'journey': ctx['nonce'], 'acceptance': 'Admin accepted IP pledge'},
        timestamp=datetime.fromtimestamp(ctx['accepted_at'], timezone.utc).replace(tzinfo=None))
    from app.models.sace_reading_audit import ReadingAuditEvent
    audit = ReadingAuditEvent(user_id=current_user.id, action='PLEDGE_ACCEPTED',
        entity_type='SACE_PLEDGE', details='Admin accepted IP pledge',
        created_at=pledge.timestamp, ip_address=request.headers.get('X-Forwarded-For', request.remote_addr),
        entity_id=pledge.id, metadata_json={'source_table': 'sace_workshop_interactions', 'pledge_event_id': pledge.id})
    appointment = lifecycle.provision(ctx, pledge)
    audit.engagement_id = appointment.engagement_id
    db.session.add(audit)
    enrollment = UserEnrollment.query.filter_by(user_id=current_user.id, subject_id=subject.id).first()
    if enrollment is None:
        db.session.add(UserEnrollment(user_id=current_user.id, subject_id=subject.id, status='active',
            country_code='ZA', local_currency='ZAR', local_amount_cents=0, zar_amount_cents=0))
    elif enrollment.status == 'pending':
        enrollment.status = 'active'
    db.session.commit()
    clear_provisioning()
    session.pop("pending_sace_code", None)
    session.pop("sace_evaluator_pledged", None)
    # Operational SACE authority is not a platform-admin session role.
    session["admin_subjects"] = [s for s in session.get("admin_subjects", []) if s != SUBJECT]


def authentication_destination(subject=None):
    """Resume a SACE journey after authentication, never grant authority here."""
    if not current_user.is_authenticated:
        return None
    if is_controller():
        session.pop("pending_sace_code", None)
        session.pop("sace_evaluator_pledged", None)
        return "sace_bp.provisioning_map"
    if session.get("pending_sace_code") and session.get("sace_evaluator_pledged"):
        return "sace_bp.claim_code"
    from . import endorsement
    if endorsement.assignments(active_only=True):
        return "sace_bp.reading_hub"
    if subject == SUBJECT:
        return "sace_bp.dashboard"
    return None


def ensure_endorsement_enrollment(user_id):
    subject = AuthSubject.query.filter_by(slug=SUBJECT, is_active=1).with_for_update().first()
    if subject is None:
        abort(503, description="SACE Endorsement is not configured.")
    enrollment = UserEnrollment.query.filter_by(user_id=user_id, subject_id=subject.id).first()
    if enrollment is None:
        enrollment = UserEnrollment(user_id=user_id, subject_id=subject.id, status="active",
                                    country_code="ZA", local_currency="ZAR",
                                    local_amount_cents=0, zar_amount_cents=0)
        db.session.add(enrollment)
    elif enrollment.status == "pending":
        enrollment.status = "active"
    db.session.commit()
    return enrollment
