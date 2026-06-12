# app/auth/sync.py
from sqlalchemy import text
from app.extensions import db
from datetime import datetime, timezone

def _utc_aware(dt):
    """
    Normalize datetime to timezone-aware UTC.
    - naive -> assume UTC
    - aware -> convert to UTC
    """
    if not dt or not isinstance(dt, datetime):
        return None
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def _is_active_until(dt, now):
    dt = _utc_aware(dt)
    now = _utc_aware(now)
    return bool(dt and now and dt > now)

def sync_enrollment(user_id: int) -> None:
    """
    Keep user_enrollment.status in sync with its own trial/paid windows.

    Rule:
      - paid / trial subjects REQUIRE an active window in user_enrollment
      - if window expired -> enrollment becomes 'pending'
      - free / deny subjects are never touched here
    """
    uid = int(user_id)
    if not uid:
        return

    # Use DB time for consistent comparisons
    db_now = db.session.execute(text("SELECT NOW()")).scalar()
    now = _utc_aware(db_now)

    rows = db.session.execute(
        text("""
            SELECT
                ue.id AS ue_id,
                lower(coalesce(s.slug, s.name))            AS slug,
                lower(coalesce(s.commercial_mode,'free')) AS commercial_mode,
                lower(coalesce(ue.status,''))             AS status,
                ue.trial_end,
                ue.expires_at
            FROM user_enrollment ue
            JOIN auth_subject s
              ON s.id = ue.subject_id
            WHERE ue.user_id = :uid
        """),
        {"uid": uid},
    ).mappings().all()

    expire_ids: list[int] = []

    for r in rows:
        # Only ACTIVE paid/trial enrollments are governed by their own windows
        if r["status"] != "active":
            continue

        cm = r["commercial_mode"]
        if cm not in ("paid", "trial"):
            continue  # free / deny are window-agnostic

        paid_ok  = _is_active_until(r["expires_at"], now)
        trial_ok = (cm == "trial") and _is_active_until(r["trial_end"], now)

        if not (paid_ok or trial_ok):
            expire_ids.append(int(r["ue_id"]))

    if not expire_ids:
        return

    db.session.execute(
        text("""
            UPDATE user_enrollment
               SET status = 'pending'
             WHERE id = ANY(:ids)
        """),
        {"ids": expire_ids},
    )
    db.session.commit()