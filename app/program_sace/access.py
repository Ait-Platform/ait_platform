"""Named provisioning authorization and persistent SACE-only controller access."""
from flask import current_app, session, abort
from flask_login import current_user
from itsdangerous import URLSafeTimedSerializer, BadData
from app.extensions import db
from app.models.auth import AuthSubject, AuthSubjectAdmin, UserEnrollment
from app.models.sace import SaceWorkshopInteraction
import json

SUBJECT = "sace_endorsement"
PROVISIONING_MAX_AGE = 7 * 24 * 60 * 60


def signer():
    return URLSafeTimedSerializer(current_app.secret_key, salt="sace-controller-provisioning-v1")


def make_provisioning_token(email):
    email = (email or "").strip().lower()
    if not email or "@" not in email:
        raise ValueError("A valid administrator email is required.")
    return signer().dumps({"subject": SUBJECT, "email": email})


def provisioning_invitation(token=None):
    token = token or session.get("sace_provisioning_token")
    if not token:
        return None
    try:
        data = signer().loads(token, max_age=PROVISIONING_MAX_AGE)
    except BadData:
        return None
    if (not isinstance(data, dict) or data.get("subject") != SUBJECT
            or not isinstance(data.get("email"), str) or not data["email"]):
        return None
    return data


def is_controller():
    if not current_user.is_authenticated:
        return False
    return AuthSubjectAdmin.query.join(AuthSubject).filter(
        db.func.lower(AuthSubjectAdmin.email) == current_user.email.strip().lower(),
        AuthSubject.slug == SUBJECT, AuthSubject.is_active == 1,
    ).first() is not None


def complete_provisioning():
    """Called only after login and pledge; one transaction, no platform role."""
    if not current_user.is_authenticated or not session.get("sace_admin_pledged"):
        abort(400, description="Sign in and accept the SACE administrator pledge first.")
    invite = provisioning_invitation()
    email = current_user.email.strip().lower()
    if not invite or invite["email"] != email:
        abort(400, description="Use the current provisioning link issued for your account.")
    # Serialize repeated completions without adding a new role/membership table.
    subject = AuthSubject.query.filter_by(slug=SUBJECT, is_active=1).with_for_update().first()
    if subject is None:
        abort(503, description="SACE Endorsement is not configured.")
    grant = AuthSubjectAdmin.query.filter(
        AuthSubjectAdmin.subject_id == subject.id,
        db.func.lower(AuthSubjectAdmin.email) == email,
    ).first()
    if grant is None:
        db.session.add(AuthSubjectAdmin(subject_id=subject.id, email=email))
        db.session.add(SaceWorkshopInteraction(
            user_id=current_user.id, activity_slug="controller_provisioned",
            response_data=json.dumps({"subject": SUBJECT, "authority": "named_provisioning_link"}),
        ))
    enrollment = UserEnrollment.query.filter_by(user_id=current_user.id, subject_id=subject.id).first()
    if enrollment is None:
        db.session.add(UserEnrollment(user_id=current_user.id, subject_id=subject.id, status="active", country_code="ZA", local_currency="ZAR", local_amount_cents=0, zar_amount_cents=0))
    elif enrollment.status == "pending":
        enrollment.status = "active"
    pledge = SaceWorkshopInteraction.query.filter_by(
        user_id=current_user.id, activity_slug="admin_patent_pledge").first()
    if pledge is None:
        db.session.add(SaceWorkshopInteraction(
            user_id=current_user.id, activity_slug="admin_patent_pledge",
            response_data="Admin accepted IP pledge",
        ))
    db.session.commit()
    session.pop("sace_provisioning_token", None)
    session.pop("sace_admin_pledged", None)
    session.pop("pending_sace_code", None)
    session.pop("sace_evaluator_pledged", None)
    # UI snapshot only: authorization always reads the persistent grant.
    session["admin_subjects"] = sorted(set(session.get("admin_subjects", [])) | {SUBJECT})


def authentication_destination(subject=None):
    """Resume a SACE journey after authentication, never grant authority here."""
    if not current_user.is_authenticated:
        return None
    if is_controller():
        session.pop("pending_sace_code", None)
        session.pop("sace_evaluator_pledged", None)
        return "sace_bp.provisioning_map"
    if session.get("sace_provisioning_token"):
        return "sace_bp.provisioning_map"
    if session.get("pending_sace_code") and session.get("sace_evaluator_pledged"):
        return "sace_bp.claim_code"
    from . import endorsement
    if endorsement.assignments():
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
