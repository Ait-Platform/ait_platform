from app.bridge.helpers import _tile_primary_href
from app.models.auth import AuthSubject
from datetime import datetime, timedelta, timezone

def modes_checkpoint(user, subjects, enrollments, is_admin_global):
    """
    Build entitlement tiles for the checkpoint screen.

    Args:
        user: current_user object
        subjects: list of AuthSubject rows
        enrollments: dict keyed by subject_id -> enrollment row
        is_admin_global: bool indicating global admin status

    Returns:
        tiles: list of dicts with subject, access_level, href, expires_at
    """
    tiles = []

    for subj in subjects:
        enrollment = enrollments.get(subj.id)  # safe dict lookup
        access_level = determine_access(subj, enrollment, is_admin_global)

        href = _tile_primary_href({
            "subject": subj,
            "access_level": access_level,
            "action": None
        })

        tile = {
            "subject": subj,
            "access_level": access_level,
            "href": href
        }

        # Add expiry if enrollment exists
        if enrollment and enrollment.expires_at:
            tile["expires_at"] = enrollment.expires_at.strftime("%Y-%m-%d %H:%M:%S")

        tiles.append(tile)

    return tiles

def calculate_expiry(subject: AuthSubject, mode: str):
    """
    Calculate expiry for a given subject.
    mode: 'trial' or 'paid'
    subject: an AuthSubject instance with trial_days and paid_days configured
    """

    now = datetime.now(timezone.utc)

    if mode == "trial":
        days = float(subject.trial_days)
    elif mode == "paid":
        days = float(subject.paid_days)  # e.g. 0.04 → ~57 minutes
    else:
        raise ValueError("Invalid expiry mode")

    if days <= 0:
        raise ValueError(f"{mode}_days not configured for subject {subject.slug}")

    return now + timedelta(days=days)

def determine_access(subject, enrollment, is_admin=False):
    """
    Decide the access level for a subject tile based on anchors:
    trial_count, subscription_id, expires_at.
    """
    if enrollment is None:
        return "locked"

    now = datetime.utcnow()

    # Admin override
    if is_admin and subject.slug == "admin":
        return "admin"

    # Free mode
    if subject.commercial_mode == "free":
        return "enrolled"

    # Trial active
    if enrollment.trial_count == 0 and enrollment.expires_at and enrollment.expires_at > now:
        return "trial"

    # Trial expired (consumed, no subscription)
    if enrollment.trial_count >= 1 and not enrollment.subscription_id:
        return "expired_trial"

    # Subscription active
    if enrollment.subscription_id and enrollment.expires_at and enrollment.expires_at > now:
        return "subscription"

    # Subscription expired
    if enrollment.subscription_id and enrollment.expires_at and enrollment.expires_at <= now:
        return "expired_subscription"

    # Fallback
    return "error"
