# app/utils/enrollment.py
from app.extensions import db
from flask import current_app
from app.auth.helpers import subject_id_from_slug
from sqlalchemy import text, bindparam

def _slug_from_id(subject_id: int) -> str | None:
    row = db.session.execute(
        text("SELECT slug FROM auth_subject WHERE id = :sid LIMIT 1"),
        {"sid": subject_id},
    ).fetchone()
    return (row[0] if row else None)

# -----------------------------
# Canonical helpers (slug-based)
# -----------------------------
def create_pending_user_enrollment(*, user_id: int, subject_slug: str, program: str | None):
    """Idempotent: ensure a pending enrollment (payment_pending=1) in user_enrollment for the subject."""
    subject_slug = (subject_slug or "").lower().strip()
    sid = subject_id_from_slug(subject_slug)
    if not sid:
        current_app.logger.warning("create_pending_user_enrollment: subject not found '%s'", subject_slug)
        return
    sql = text("""
        INSERT INTO user_enrollment (
            user_id, program, current_chapter, payment_pending, completed,
            subject_id, status, created_at
        )
        VALUES (:uid, :program, NULL, 1, 0, :sid, 'active', datetime('now'))
        ON CONFLICT(user_id, subject_id) DO UPDATE SET
            program         = COALESCE(excluded.program, user_enrollment.program),
            status          = 'active',
            payment_pending = 1;
    """)
    db.session.execute(sql, {"uid": user_id, "sid": sid, "program": (program or subject_slug)})
    db.session.commit()


def settle_user_enrollment_paid(*, user_id: int, subject_slug: str, program: str | None):
    """Idempotent: mark enrollment as paid (payment_pending=0, status=active) in user_enrollment."""
    subject_slug = (subject_slug or "").lower().strip()
    sid = subject_id_from_slug(subject_slug)
    if not sid:
        current_app.logger.warning("settle_user_enrollment_paid: subject not found '%s'", subject_slug)
        return
    sql = text("""
        INSERT INTO user_enrollment (
            user_id, program, current_chapter, payment_pending, completed,
            subject_id, status, created_at
        )
        VALUES (:uid, :program, NULL, 0, 0, :sid, 'active', datetime('now'))
        ON CONFLICT(user_id, subject_id) DO UPDATE SET
            program         = COALESCE(excluded.program, user_enrollment.program),
            status          = 'active',
            payment_pending = 0;
    """)
    db.session.execute(sql, {"uid": user_id, "sid": sid, "program": (program or subject_slug)})
    db.session.commit()

# -----------------------------------
# Back-compat wrappers (id/legacy API)
# -----------------------------------
def ensure_pending_enrollment(user_id: int, subject_id: int, program: str | None = None):
    """
    Back-compat wrapper:
    Ensure ACTIVE + pending enrollment using subject_id.
    (Delegates to slug-based canonical.)
    """
    slug = _slug_from_id(subject_id)
    if not slug:
        current_app.logger.warning("ensure_pending_enrollment: subject_id not found '%s'", subject_id)
        return
    create_pending_user_enrollment(user_id=user_id, subject_slug=slug, program=program or slug)


def mark_payment_settled(user_id: int, subject_id: int):
    """
    Back-compat wrapper:
    Mark enrollment as paid using subject_id.
    (Delegates to slug-based canonical.)
    """
    slug = _slug_from_id(subject_id)
    if not slug:
        current_app.logger.warning("mark_payment_settled: subject_id not found '%s'", subject_id)
        return
    settle_user_enrollment_paid(user_id=user_id, subject_slug=slug, program=slug)


def ensure_enrollment(user_id: int, subject_slug: str, role: str):
    """
    Back-compat wrapper:
    Ensure ACTIVE + pending enrollment using subject_slug.
    (Ignores role; settlement happens via Stripe success/webhook.)
    """
    slug = (subject_slug or "").strip().lower()
    create_pending_user_enrollment(user_id=user_id, subject_slug=slug, program=slug)


ENROLLED_STATI = ("active", "paid")

def is_enrolled(user_id: int, subject_id: int) -> bool:
    row = db.session.execute(text("""
        SELECT 1
        FROM user_enrollment
        WHERE user_id   = :uid
          AND subject_id= :sid
          AND status IN ('active','paid')
        LIMIT 1
    """), {"uid": user_id, "sid": subject_id}).fetchone()
    return row is not None
