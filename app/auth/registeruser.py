# app/auth/registeruser.py
from typing import Optional, List, Dict, Any
from flask import request, url_for
from app.extensions import db
from sqlalchemy import func, text

# tolerant imports
try:
    from app.models import User
except Exception:
    from app.models import User  # type: ignore
try:
    from app.models.auth import UserEnrollment, AuthSubject
except Exception:
    # fallback if your models live elsewhere
    from app.models import UserEnrollment, AuthSubject  # type: ignore


def _norm_email(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    return (raw.strip().replace(" ", "").replace(",", ".")).lower()

def _resolve_subject_id() -> Optional[int]:
    raw_sid = request.values.get("subject_id") or request.values.get("sid")
    if raw_sid:
        try: return int(raw_sid)
        except: pass

    token = (request.values.get("subject") or "").strip().lower()
    if not token: return None

    subj = (db.session.query(AuthSubject)
            .filter(func.lower(func.coalesce(AuthSubject.slug, ""))==token)
            .first())
    if subj and getattr(subj, "id", None): return int(subj.id)

    subj = (db.session.query(AuthSubject)
            .filter(func.lower(func.coalesce(AuthSubject.name, ""))==token)
            .first())
    if subj and getattr(subj, "id", None): return int(subj.id)
    return None

def get_user_by_email(email):
    norm = _norm_email(email)
    if not norm: return None

    base = func.lower(func.replace(func.replace(func.trim(User.email), " ", ""), ",", ".")) == norm

    u = (db.session.query(User)
         .filter(base)
         .filter(text("is_active = 1"))  # column, not @property
         .order_by(User.id.asc())
         .first())
    if u: return u

    return (db.session.query(User)
            .filter(base)
            .order_by(User.id.asc())
            .first())
