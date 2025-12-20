# app/payments/helpers.py
from __future__ import annotations

from sqlalchemy import text
from app.extensions import db

def mark_enrollment_paid(*, user_id: int, subject_id: int, program: str | None = None) -> None:
    """
    Ensure an ACTIVE enrollment for exactly this (user_id, subject_id).
    Uses only the columns we know exist: user_id, subject_id, status.
    NO COMMIT here; caller should commit once after all work.
    Requires UNIQUE(user_id, subject_id) on user_enrollment.
    """
    db.session.execute(text("""
        INSERT INTO user_enrollment (user_id, subject_id, status)
        VALUES (:uid, :sid, 'active')
        ON CONFLICT(user_id, subject_id) DO UPDATE SET
            status = 'active'
    """), {"uid": int(user_id), "sid": int(subject_id)})
