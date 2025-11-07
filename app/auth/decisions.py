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
'''
def _norm_email(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    return (raw.strip().replace(" ", "").replace(",", ".")).lower()

def find_user_active_first_by_email(email: str) -> Optional[User]:
    """Active first (is_active=1) then earliest by id. No property clash."""
    norm = _norm_email(email)
    if not norm:
        return None
    f = func.lower(func.replace(func.replace(func.trim(User.email), " ", ""), ",", ".")) == norm
    u = (db.session.query(User).filter(f).filter(text("is_active = 1")).order_by(User.id.asc()).first())
    return u or db.session.query(User).filter(f).order_by(User.id.asc()).first()

def resolve_subject_id() -> Optional[int]:
    raw_sid = request.values.get("subject_id") or request.values.get("sid")
    if raw_sid:
        try: return int(raw_sid)
        except: pass
    token = (request.values.get("subject") or "").strip().lower()
    if not token:
        return None
    s = db.session.query(AuthSubject).filter(func.lower(func.coalesce(AuthSubject.slug, "")) == token).first()
    if s: return int(s.id)
    s = db.session.query(AuthSubject).filter(func.lower(func.coalesce(AuthSubject.name, "")) == token).first()
    return int(s.id) if s else None

def fetch_enrollments(user_id: int) -> List[Dict[str, Any]]:
    if not user_id:
        return []
    q = (db.session.query(UserEnrollment, AuthSubject)
         .outerjoin(AuthSubject, AuthSubject.id == UserEnrollment.subject_id)
         .filter(UserEnrollment.user_id == int(user_id))
         .order_by(UserEnrollment.id.asc()))
    out = []
    for enr, subj in q.all():
        out.append({
            "id": getattr(enr, "id", None),
            "subject": getattr(subj, "name", None) or getattr(subj, "slug", None) or "Subject",
            "status": (getattr(enr, "status", "") or "").lower() or None,
            "completed": bool(getattr(enr, "completed", False)),
            "payment_pending": bool(getattr(enr, "payment_pending", False)) if hasattr(enr, "payment_pending") else None,
            "created_at": getattr(enr, "created_at", None),
        })
    return out

def stash_reg_context(subject_id: int, role: str, form_data: Dict[str, Any]) -> None:
    """Lightweight session handoff between steps."""
    session["reg"] = {
        "subject_id": subject_id,
        "role": (role or "user").strip().lower(),
        "full_name": (form_data.get("full_name") or "").strip(),
        "email": (form_data.get("email") or "").strip(),
        "country": (form_data.get("country") or "").strip(),
        "next": form_data.get("next") or "",
    }

def get_reg_context() -> Dict[str, Any]:
    return session.get("reg", {})

import re
EMAIL_RX = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

def normalize_email(s: str) -> str:
    return (s or "").strip().lower()

def safe_program_for(subject, subject_slug: str) -> str:
    return (
        getattr(subject, "slug", None)
        or getattr(subject, "code", None)
        or getattr(subject, "name", None)
        or subject_slug
        or "generic"
    )

def fetch_enrollments_for_user(user_id: int):
    rows = db.session.execute(
        select(UserEnrollment, AuthSubject)
        .join(AuthSubject, AuthSubject.id == UserEnrollment.subject_id)
        .where(UserEnrollment.user_id == user_id)
        .order_by(AuthSubject.name)
    ).all()
    # shape to a light dict
    items = []
    for ae, subj in rows:
        items.append({
            "subject_id": subj.id,
            "subject": getattr(subj, "name", getattr(subj, "slug", "unknown")),
            "program": getattr(ae, "program", None),
            "status": getattr(ae, "status", None),
            "payment_pending": getattr(ae, "payment_pending", 0),
            "completed": getattr(ae, "completed", 0),
        })
    return items

def collect_account_candidates(email: str, full_name: str):
    """Find primary account by email and other accounts sharing the same name."""
    primary = db.session.scalar(select(User).where(User.email == email))
    others = []
    if full_name:
        # same name (case-insensitive), exclude primary if present
        q = select(User).where(User.name.ilike(full_name))
        rows = db.session.execute(q).scalars().all()
        for u in rows:
            if not primary or u.id != primary.id:
                others.append(u)
    return primary, others

def make_decision_context(email: str, full_name: str, subject_slug: str):
    ctx = {
        "email": email,
        "full_name": full_name,
        "subject_slug": subject_slug,
        "primary": None,
        "primary_enrollments": [],
        "others": [],                # list of {user, enrollments}
        "new_account": False,        # true if no accounts exist
    }

    primary, others = collect_account_candidates(email, full_name)

    if primary:
        ctx["primary"] = {
            "id": primary.id,
            "name": primary.name,
            "email": primary.email,
            "active": getattr(primary, "active", 1),
        }
        ctx["primary_enrollments"] = fetch_enrollments_for_user(primary.id)

    for u in others:
        ctx["others"].append({
            "id": u.id,
            "name": u.name,
            "email": u.email,
            "active": getattr(u, "active", 1),
            "enrollments": fetch_enrollments_for_user(u.id),
        })

    if not primary and not others:
        ctx["new_account"] = True

    return ctx

# archiving partfrom sqlalchemy import func, select, update

def norm_email_py(s: str) -> str:
    s = (s or '').strip().lower().replace(' ', '').replace(',', '')
    # drop +tag
    if '+' in s:
        local, at, domain = s.partition('@')
        if at:
            local = local.split('+', 1)[0]
            s = f"{local}@{domain}"
    # fix common typo
    s = s.replace('telkmsa', 'telkomsa')
    return s

def norm_email_sql(col):
    # lower -> strip spaces/commas -> drop +tag (approx: remove '+')
    return func.lower(
        func.replace(
            func.replace(
                func.replace(col, ' ', ''), ',', ''
            ), '+', ''
        )
    )

def canonical(e: str) -> str:
    e = (e or '').strip().lower()
    loc, _, dom = e.partition('@')
    return f"{loc.split('+',1)[0]}@{dom}"

def like_pattern_for_variants(email: str) -> str:
    """
    Match base+tag@domain variants via SQL LIKE.
    'archoney@telkomsa.net' -> 'archoney%@telkomsa.net'
    """
    e = canonical_email(email)            # strips spaces, commas, and +tag; lowercases
    local, _, domain = e.partition("@")
    if not domain:                        # safety fallback
        return f"{local}%"
    return f"{local}%@{domain}"

def canonical_email(email: str) -> str:
    """
    Lowercase, trim, remove spaces/commas, drop +tag, and fix common typo.
    """
    s = (email or "").strip().lower().replace(" ", "").replace(",", "")
    if "+" in s:
        local, at, domain = s.partition("@")
        if at:
            local = local.split("+", 1)[0]
            s = f"{local}@{domain}"
    # project-specific typo you mentioned
    s = s.replace("telkmsa", "telkomsa")
    return s

def like_pattern_for_variants(email: str) -> str:
    """
    Build a LIKE pattern matching base+tag@domain variants.
    'archoney@telkomsa.net' -> 'archoney%@telkomsa.net'
    """
    e = canonical_email(email)
    local, _, domain = e.partition("@")
    return f"{local}%@{domain}" if domain else f"{local}%"

from sqlalchemy import select, func, text
from app import db
#from app.models import User, UserEnrollment, Subject  # adjust paths

# --- helpers ---

def _work_key(email: str, subject_id: int, role: str) -> str:
    return f"{_canon_email_py(email)}|{subject_id}|{(role or '').strip().lower()}"



def _canon_email_py(addr: str) -> str:
    a = (addr or "").strip().lower()
    if "@" not in a:
        return a
    local, domain = a.split("@", 1)
    p = local.find("+")
    if p != -1:
        local = local[:p]
    return f"{local}@{domain}"

def _subject_id_from_slug_fallback(slug: str) -> int | None:
    row = db.session.execute(
        text("SELECT id FROM auth_subject WHERE slug=:s OR name=:s LIMIT 1"),
        {"s": slug},
    ).first()
    return int(row[0]) if row else None

# --- put near the top of routes.py ---

'''

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
