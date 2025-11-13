# app/services/enrollment.py (or wherever _ensure_enrollment_row lives)

from datetime import datetime
from flask import session
from app import db
from sqlalchemy import text as sa_text

def _ensure_enrollment_row(user_id: int, subject_slug: str):
    # 1) find subject id
    sid = db.session.execute(sa_text(
        "SELECT id FROM auth_subject WHERE slug=:s LIMIT 1"
    ), {"s": subject_slug}).scalar()

    # 2) try to find existing enrollment
    row = db.session.execute(sa_text("""
        SELECT id FROM user_enrollment
        WHERE user_id=:uid AND subject_id=:sid
        LIMIT 1
    """), {"uid": user_id, "sid": sid}).first()

    if row:
        return type("Obj", (), {"id": row[0]})  # mimic simple object with .id

    # 3) create new enrollment (status 'active' or your default)
    db.session.execute(sa_text("""
        INSERT INTO user_enrollment (user_id, subject_id, status, started_at)
        VALUES (:uid, :sid, 'active', CURRENT_TIMESTAMP)
    """), {"uid": user_id, "sid": sid})
    eid = db.session.execute(sa_text("SELECT last_insert_rowid()")).scalar()

    # 4) COPY LOCKED QUOTE FROM SESSION (only on first creation)
    q = (session.get("reg_ctx") or {}).get("quote")
    if q:
        db.session.execute(sa_text("""
            UPDATE user_enrollment
            SET country_code        = :cc,
                quoted_currency     = :cur,
                quoted_amount_cents = :amt,
                price_version       = :ver,
                price_locked_at     = CURRENT_TIMESTAMP
            WHERE id = :eid
        """), {
            "cc":  q.get("country_code"),
            "cur": q.get("currency"),
            "amt": q.get("amount_cents"),
            "ver": q.get("version") or "2025-11",
            "eid": eid
        })
    db.session.commit()

    return type("Obj", (), {"id": eid})
