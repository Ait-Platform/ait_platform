# app/services/enrollment.py (or wherever _ensure_enrollment_row lives)
from types import SimpleNamespace
from sqlalchemy import text as sa_text
from app.extensions import db

def _ensure_enrollment_row(user_id: int, subject_slug: str) -> int:
    """
    Return user_enrollment.id for this (user, subject).
    Creates it if missing. Works on Postgres + SQLite.
    """
    slug = (subject_slug or "").strip().lower()

    # 1) Check if existing
    row = db.session.execute(
        sa_text("""
            SELECT ue.id
            FROM user_enrollment ue
            JOIN auth_subject s ON s.id = ue.subject_id
            WHERE ue.user_id = :uid AND lower(s.slug) = :slug
            LIMIT 1
        """),
        {"uid": user_id, "slug": slug},
    ).first()

    if row:
        return int(row.id)

    # 2) Get subject id
    srow = db.session.execute(
        sa_text("SELECT id FROM auth_subject WHERE lower(slug)=:slug"),
        {"slug": slug},
    ).first()
    if not srow:
        raise ValueError(f"Unknown subject slug {subject_slug}")

    sid = int(srow.id)

    # 3) Insert & return ID (Postgres compatible)
    res = db.session.execute(
        sa_text("""
            INSERT INTO user_enrollment (user_id, subject_id, status, started_at)
            VALUES (:uid, :sid, 'active', CURRENT_TIMESTAMP)
            RETURNING id
        """),
        {"uid": user_id, "sid": sid},
    )
    eid = res.scalar_one()

    db.session.commit()
    return int(eid)
