from datetime import datetime, timezone
from sqlalchemy import text as sa_text
from app.models.auth import AuthSubject, UserEnrollment
from app.extensions import db
from app.time_utils import app_now



def has_access(user_id: int, product_slug: str) -> bool:
    """
    Check if a user has active access to a subject.
    Enrollment is the single source of truth.
    """
    now = datetime.utcnow()

    enr = (
        UserEnrollment.query
        .join(AuthSubject, UserEnrollment.subject_id == AuthSubject.id)
        .filter(
            UserEnrollment.user_id == int(user_id),
            db.func.lower(AuthSubject.slug) == (product_slug or "").strip().lower(),
        )
        .first()
    )

    if not enr:
        return False

    # Active enrollment with valid trial or paid window
    if enr.status == "active":
        if enr.trial_end and enr.trial_end > now:
            return True
        if enr.expires_at and enr.expires_at > now:
            return True

    return False

def is_trial_expired(enrollment_row) -> bool:
    """
    Determine if a user's trial has expired.
    Expects a mapping row with trial_count and trial_end.
    """
    if not enrollment_row:
        return False

    tc = int(enrollment_row.get("trial_count") or 0)
    te = enrollment_row.get("trial_end")

    if tc < 1 or te is None:
        return False

    # Normalize to UTC if naive
    if te.tzinfo is None:
        te = te.replace(tzinfo=timezone.utc)

    now = datetime.now(timezone.utc)

    return now >= te


def _utcnow():
    return app_now()


def ensure_utc(dt):
    if not dt:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def get_entitlement_state(user_id: int, product_slug: str) -> str:
    """
    Determine entitlement state for a given user and product.
    Gatekeeper (AuthSubject) defines commercial mode and trial support.
    Enrollment (UserEnrollment) defines user-specific status.
    """

    now = _utcnow()

    subj = (
        AuthSubject.query
        .filter(db.func.lower(AuthSubject.slug) == (product_slug or "").strip().lower())
        .first()
    )
    if not subj or not subj.is_active:
        return "no_course"  # Subject not available

    enr = (
        UserEnrollment.query
        .join(AuthSubject, UserEnrollment.subject_id == AuthSubject.id)
        .filter(
            UserEnrollment.user_id == int(user_id),
            db.func.lower(AuthSubject.slug) == (product_slug or "").strip().lower(),
        )
        .first()
    )

    # --- Free courses ---
    if subj.commercial_mode == "free":
        return "start_course"

    # --- Paid courses ---
    if subj.commercial_mode == "paid":
        trial_days = subj.trial_days or 0

        # No enrollment yet → must quote
        if not enr:
            return "go_quote"

        trial_end = ensure_utc(enr.trial_end)
        expires_at = ensure_utc(enr.expires_at)

        # Trial logic only if subject supports trial_days > 0
        if trial_days > 0 and trial_end:
            if trial_end > now:
                return "enroll_now"  # Trial in progress
            else:
                return "go_pay"      # Trial expired → must pay

        # Enrollment status checks
        if enr.status == "pending":
            return "go_pay"

        if enr.status == "active":
            # Subscription awareness
            if subj.requires_price and _is_subscription_course(enr.price_id):
                return "start_course"

            # Normal paid course
            if expires_at and expires_at > now:
                return "start_course"
            return "start_course"  # Active always means start

        if enr.status == "completed":
            if enr.report_pdf_url:
                return "show_certificate"
            return "no_course"

    return "go_quote"  # Fallback

def _is_subscription_course(price_id: int) -> bool:
    """
    Identify subscription-based courses by price_id.
    Extend this to check pricing metadata.
    """
    subscription_price_ids = {40, 60}  # Example IDs for monthly courses like 'budget'
    return price_id in subscription_price_ids