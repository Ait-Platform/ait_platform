from datetime import datetime, timedelta, timezone
from decimal import Decimal
from flask import session, url_for
from app.extensions import db
from app.models.auth import AuthPaymentLog, AuthSubject, UserEnrollment,  User
from sqlalchemy import text as sa_text
from datetime import datetime, timezone

#from app.payments.pricing import enrollment_locked_price
from app.time_utils import app_now


def _is_global_admin(email: str) -> bool:
    email = (email or "").strip().lower()
    if not email:
        return False

    try:
        ok = (
            db.session.execute(sa_text("SELECT to_regclass('auth_approved_admin') IS NOT NULL AS ok"))
            .mappings()
            .first()
        )
        if not (ok and ok.get("ok")):
            return False

        hit = db.session.execute(
            sa_text("SELECT 1 FROM auth_approved_admin WHERE lower(email)=:e LIMIT 1"),
            {"e": email},
        ).first()
        return hit is not None
    except Exception:
        try:
            db.session.rollback()
        except Exception:
            pass
        return False

# -------------------------------------------------
# helpers
# -------------------------------------------------



def _utcnow():
    return app_now()



def _as_utc(dt):
    if not dt:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _tile_next_url(tile: dict) -> str | None:
    """
    Entry destination only.
    """
    slug = (tile.get("slug") or "").strip().lower()
    if not slug:
        return None
    return url_for("program_entry", subject_slug=slug)


def fetch_user_enrollment(user_id: int):
    """
    Return all enrollment rows for this user, joined with subject info.
    """
    rows = (
        db.session.query(UserEnrollment)
        .join(AuthSubject, AuthSubject.id == UserEnrollment.subject_id)
        .filter(UserEnrollment.user_id == user_id)
        .all()
    )
    return rows


def bridge_decision(enr, now):
    """
    Decide bridge action based on redesigned anchors:
    trial_count, subscription_id, expires_at.
    """
    # Run 1: Trial active
    if enr.trial_count == 0 and enr.expires_at and enr.expires_at > now:
        return "enroll_now"

    # Run 2: Trial expired
    if enr.trial_count == 0 and enr.expires_at and enr.expires_at <= now:
        return "go_quote"

    # Run 2 consumed: trial_count == 1, no subscription
    if enr.trial_count == 1 and not enr.subscription_id:
        return "go_quote"

    # Subscription active
    if enr.subscription_id and enr.expires_at and enr.expires_at > now:
        return "enroll_now"

    # Subscription expired
    if enr.subscription_id and enr.expires_at and enr.expires_at <= now:
        return "go_pay"

    return "deny"


def get_bridge_tiles_for_email(email, user_id, now=None):
    now = now or datetime.utcnow()

    enrollments = (
        db.session.query(UserEnrollment)
        .filter(UserEnrollment.user_id == user_id)
        .all()
    )

    subjects = []
    for enr in enrollments:
        action = bridge_decision(enr, now)

        # Map action to access_level
        if action == "enroll_now":
            if enr.trial_count == 0:
                access_level = "trial"
            elif enr.subscription_id:
                access_level = "subscription"
            else:
                access_level = "enrolled"
        elif action == "go_quote":
            access_level = "expired_trial"
        elif action == "go_pay":
            access_level = "expired_subscription"
        else:
            access_level = "error"

        subjects.append({
            "slug": enr.subject.slug,
            "name": enr.subject.name,
            "action": action,
            "access_level": access_level,
            "subject": enr.subject,   # ✅ template can call tile_primary_href
            "price_id": enr.price_id,
        })
    return subjects


def get_user_enrollment(user_id):
    return UserEnrollment.query.filter_by(user_id=user_id).all()


def _tile_primary_href(tile):
    """
    Decide the primary href for a Bridge tile based on access level and action.
    """
    subj = tile.get("subject")
    action = (tile.get("action") or "").lower()
    access_level = (tile.get("access_level") or "").lower()

    # Access-level routing
    if access_level in ("trial", "subscription", "enrolled"):
        href = url_for(subj.start_endpoint, subject_id=subj.id)
    elif access_level == "admin":
        href = url_for(subj.admin_start_endpoint, subject_id=subj.id)
    elif access_level in ("expired_trial", "expired_subscription"):
        href = url_for("quote_bp.quote", subject=subj.slug)
    else:
        href = url_for("payfast_bp.checkout_review", subject=subj.slug)

    # Action overrides
    if action == "go_pay":
        href = url_for(subj.pay_endpoint, subject=subj.slug, price_id=tile.get("price_id"))
    elif action == "go_quote":
        href = url_for("quote_bp.quote", subject=subj.slug)
    elif action == "about":
        href = url_for(subj.about_endpoint, subject=subj.slug)
    elif action in ("enroll_now", "start_course"):
        if access_level not in ("expired_trial", "expired_subscription", "error"):
            href = url_for(subj.start_endpoint, subject_id=subj.id)

    print(f"Tile href for {subj.slug}: access={access_level}, action={action}, href={href}")
    return href

