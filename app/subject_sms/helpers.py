# ---Helpers.py ----
from flask import abort, request
from flask_login import current_user
from sqlalchemy import text
from app.models.sms import SmsAccessLog, SmsApprovedUser, SmsSchool
from app.extensions import db


def _sms_access_log(*, school_id, role_effective, target, allowed, deny_reason=None):
    try:
        db.session.add(SmsAccessLog(
            school_id=school_id,
            user_id=current_user.id,
            role_effective=role_effective,
            target=target,
            allowed=bool(allowed),
            deny_reason=deny_reason,
            ip=(request.headers.get("X-Forwarded-For") or request.remote_addr),
            user_agent=(request.headers.get("User-Agent") or "")[:255],
        ))
        db.session.commit()
    except Exception:
        db.session.rollback()

def _audit_has_access(school, user):
    if not school or not user or not getattr(user, "email", None):
        return False

    # School owner always has access
    if school.user_id == user.id:
        return True

    email = (user.email or "").strip().lower()
    return (
        SmsApprovedUser.query
        .filter_by(school_id=school.id, email=email, active=True)
        .filter(SmsApprovedUser.role.in_(["auditor", "sgb"]))
        .first()
        is not None
    )

def _sms_norm_email(u):
    return (getattr(u, "email", None) or "").strip().lower()

def require_sms_audit_access():
    """
    Returns (school, role) if the logged-in user may access Finance Audit.
    - Owner always allowed.
    - Otherwise must be active in sms_approved_user with role in ('auditor','sgb').
    """
    if not current_user.is_authenticated:
        abort(403)

    # Owner path
    school = _current_sms_school()
    if school:
        return school, "owner"

    # Approved-user path (pick the most recently created active approval)
    email = _sms_norm_email(current_user)
    row = (
        SmsApprovedUser.query
        .filter_by(email=email, active=True)
        .filter(SmsApprovedUser.role.in_(["auditor", "sgb"]))
        .order_by(SmsApprovedUser.created_at.desc(), SmsApprovedUser.id.desc())
        .first()
    )
    if not row:
        abort(403)

    school = SmsSchool.query.get(row.school_id)
    if not school:
        abort(403)

    return school, row.role

def _sms_owner_school():
    return SmsSchool.query.filter_by(user_id=current_user.id).first()

def has_sms_role(role: str) -> bool:
    email = (getattr(current_user, "email", "") or "").strip().lower()
    if not email:
        return False

    school = _sms_owner_school()
    if school:
        # owner only gets what you explicitly allow
        row = SmsApprovedUser.query.filter_by(
            school_id=school.id,
            email=email,
            role=role,
            active=True,
        ).first()
        return bool(row)

    row = SmsApprovedUser.query.filter_by(
        email=email,
        role=role,
        active=True,
    ).first()
    return bool(row)

def _current_sms_school():
    if not getattr(current_user, "is_authenticated", False):
        return None

    # 1) Principal/owner path
    school = SmsSchool.query.filter_by(user_id=current_user.id).first()
    if school:
        return school

    # 2) Approved-role path
    email = (getattr(current_user, "email", "") or "").strip().lower()
    if not email:
        return None

    approved = (
        SmsApprovedUser.query
        .filter_by(email=email, active=True)
        .order_by(SmsApprovedUser.id.desc())
        .first()
    )
    if not approved:
        return None

    return db.session.get(SmsSchool, approved.school_id)

def has_sms_finance_access() -> bool:
    return has_sms_role("treasurer") or has_sms_role("sgb")

def has_sms_audit_notice_access() -> bool:
    return has_sms_role("sgb")

def has_sms_audit_access() -> bool:
    if not current_user.is_authenticated:
        return False
    email = (getattr(current_user, "email", "") or "").strip().lower()
    if not email:
        return False
    return bool(
        SmsApprovedUser.query
        .filter_by(email=email, active=True, role="auditor")
        .first()
    )

def _require_sms_auditor_school():
    """
    Auditor-only.
    - Must exist as active SmsApprovedUser(role='auditor') by email.
    - Owner/principal is explicitly blocked (even if they try to add themselves).
    Returns: SmsSchool
    """
    if not getattr(current_user, "is_authenticated", False):
        abort(403)

    email = (getattr(current_user, "email", "") or "").strip().lower()
    if not email:
        abort(403)

    approval = (
        SmsApprovedUser.query
        .filter_by(email=email, active=True, role="auditor")
        .order_by(SmsApprovedUser.id.desc())
        .first()
    )
    if not approval:
        abort(403)

    school = db.session.get(SmsSchool, approval.school_id)
    if not school:
        abort(403)

    # Hard rule: principal/owner never sees audit
    if school.user_id == current_user.id:
        abort(403)

    return school
