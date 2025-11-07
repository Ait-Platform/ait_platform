# app/auth/registeruser.py
from typing import Optional, List, Dict, Any
from flask import request, url_for
from sqlalchemy import func
from app.extensions import db

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

# ---------- email normalization + active-first user lookup ----------
def _norm_email(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    return (raw.strip().replace(" ", "").replace(",", ".")).lower()
# at top of registeruser.py
from sqlalchemy import func, text


# ---------- previous enrollments ----------
def _fetch_enrollments_for_user(user_id: int) -> List[Dict[str, Any]]:
    """
    Return a lightweight list of the user's prior enrollments with subject labels.
    Safe even if some columns don't exist.
    """
    if not user_id:
        return []
    q = (
        db.session.query(UserEnrollment, AuthSubject)
        .outerjoin(AuthSubject, AuthSubject.id == UserEnrollment.subject_id)
        .filter(UserEnrollment.user_id == int(user_id))
        .order_by(UserEnrollment.id.asc())
    )
    items: List[Dict[str, Any]] = []
    for enr, subj in q.all():
        items.append({
            "id": getattr(enr, "id", None),
            "subject_id": getattr(enr, "subject_id", None),
            "subject": getattr(subj, "name", None) or getattr(subj, "slug", None) or "Subject",
            "role": getattr(enr, "role", None) or "learner",
            "status": (getattr(enr, "status", None) or "").lower() or None,
            "completed": bool(getattr(enr, "completed", False)),
            "payment_pending": bool(getattr(enr, "payment_pending", False)) if hasattr(enr, "payment_pending") else None,
            "created_at": getattr(enr, "created_at", None),
        })
    return items

# ---------- single entry point for /register ----------
def decide_registration_flow() -> dict:
    """
    Gatekeeper for /register.
    - If no role: SHOW decision box (and include prior enrollments if email resolves to a user)
    - If role is present: pass to enrollment decision to bridge / resume / start checkout
    """
    sid_raw = request.values.get("subject_id") or request.values.get("sid")
    try:
        sid = int(sid_raw) if sid_raw is not None else None
    except (TypeError, ValueError):
        sid = None

    role = (request.values.get("role") or "").strip()
    email = request.values.get("email")

    # Try to resolve the user (active-first) when an email is present,
    # even BEFORE role is chosen — so we can show their previous enrollments.
    user_obj = get_user_by_email(email) if email else None
    enrollments = _fetch_enrollments_for_user(getattr(user_obj, "id", 0)) if user_obj else []

    # ✅ Decision box FIRST if role not chosen yet
    if not role:
        return {
            "intent": "render",
            "template": "auth/decision.html",
            "data": {
                "subject_id": sid,
                "user": user_obj,
                "enrollments": enrollments,  # ← shown in the box
                "prefill": {"email": email} if email else {},
            },
        }

    # From here on, hand off to your existing decision logic
    from app.auth.helpers import resolve_enrollment_decision  # import here to avoid cycles
    decision = resolve_enrollment_decision(user_obj, sid) if user_obj else {"action": "redirect_welcome"}

    action = decision.get("action")
    if action == "redirect_welcome":
        return {"intent": "redirect", "url": url_for("public_bp.welcome")}
    if action == "redirect_bridge":
        return {"intent": "redirect", "url": decision.get("bridge_url") or url_for("auth_bp.bridge_dashboard", role=role or "learner")}
    if action == "resume_checkout":
        return {"intent": "redirect", "url": url_for("checkout_bp.start", subject_id=decision.get("subject_id") or sid, role=role or "learner")}

    # default: start checkout
    return {"intent": "redirect", "url": url_for("checkout_bp.start", subject_id=decision.get("subject_id") or sid, role=role or "learner")}

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

# registeruser.py
from sqlalchemy import func, text

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
