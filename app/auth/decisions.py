# app/auth/decisions.py
from typing import Optional, Dict, Any, List

from flask import request, session
#from app.auth.routes import EMAIL_CANON_SQL
from app.extensions import db
from sqlalchemy import text as sa_text
try:
    from app.models import User
except Exception:
    from app.models import User  # type: ignore

try:
    from app.models.auth import AuthSubject, UserEnrollment
except Exception:
    from app.models import AuthSubject, UserEnrollment  # type: ignore

def get_canonical_user_id(email: str) -> int | None:
    if not email:
        return None
    return db.session.execute(sa_text(
        'SELECT MIN(id) FROM "user" WHERE lower(email)=lower(:e)'
    ), {"e": email}).scalar()

def has_active_enrollment(uid: int, sid: int) -> bool:
    row = db.session.execute(sa_text("""
        SELECT 1
        FROM user_enrollment
        WHERE user_id=:uid AND subject_id=:sid AND status='active'
        LIMIT 1
    """), {"uid": int(uid), "sid": int(sid)}).first()
    return bool(row)

def upsert_active_enrollment(uid: int, sid: int) -> None:
    # 1) set existing row(s) active (if any)
    upd = db.session.execute(sa_text("""
        UPDATE user_enrollment
        SET status='active'
        WHERE user_id=:uid AND subject_id=:sid
    """), {"uid": int(uid), "sid": int(sid)})

    # 2) if none updated, insert one
    if (getattr(upd, "rowcount", 0) or 0) == 0:
        db.session.execute(sa_text("""
            INSERT INTO user_enrollment (user_id, subject_id, status)
            VALUES (:uid, :sid, 'active')
        """), {"uid": int(uid), "sid": int(sid)})

    # 3) dedupe: keep the oldest active row, mark others inactive
    db.session.execute(sa_text("""
        WITH act AS (
          SELECT id,
                 ROW_NUMBER() OVER (ORDER BY id) AS rn
          FROM user_enrollment
          WHERE user_id=:uid AND subject_id=:sid AND status='active'
        )
        UPDATE user_enrollment
        SET status='inactive'
        WHERE id IN (SELECT id FROM act WHERE rn > 1)
    """), {"uid": int(uid), "sid": int(sid)})
