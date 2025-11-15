# app/services/enrollment.py (or wherever _ensure_enrollment_row lives)
from types import SimpleNamespace
from sqlalchemy import text as sa_text
from app.extensions import db


def _ensure_enrollment_row(*, user_id: int, subject_slug: str):
    """
    Ensure there is a user_enrollment row for (user_id, subject_slug),
    and return an object with .id, .user_id, .subject_id.
    Works on both SQLite and Postgres (no last_insert_rowid).
    """
    # 1) Resolve subject_id from slug
    row = db.session.execute(
        sa_text("SELECT id FROM auth_subject WHERE slug = :slug"),
        {"slug": subject_slug},
    ).first()
    if not row:
        raise ValueError(f"Unknown subject slug: {subject_slug!r}")
    subject_id = int(row.id)

    # 2) Insert if missing; keep existing otherwise
    db.session.execute(
        sa_text("""
            INSERT INTO user_enrollment (user_id, subject_id, status, started_at)
            VALUES (:uid, :sid, 'active', CURRENT_TIMESTAMP)
            ON CONFLICT (user_id, subject_id) DO NOTHING
        """),
        {"uid": user_id, "sid": subject_id},
    )
    db.session.commit()

    # 3) Read back the id in a DB-agnostic way
    row = db.session.execute(
        sa_text("""
            SELECT id
            FROM user_enrollment
            WHERE user_id = :uid AND subject_id = :sid
        """),
        {"uid": user_id, "sid": subject_id},
    ).first()
    if not row:
        raise RuntimeError("Failed to upsert user_enrollment row.")

    return SimpleNamespace(
        id=int(row.id),
        user_id=int(user_id),
        subject_id=int(subject_id),
    )
