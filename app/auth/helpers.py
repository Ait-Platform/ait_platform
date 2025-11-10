# app/auth/helpers.py
from __future__ import annotations
from typing import TypedDict, Optional, Dict, Tuple, Any,    Union
from flask import request, session, url_for, current_app
from flask_login import current_user
from sqlalchemy import text, text as sa_text
from app.utils.strings import _lower
import time
from app.extensions import db
from sqlalchemy import func
try:
    from app.models.auth import User
except Exception:
    from app.models.auth import User  # fallback

def _dashboard_endpoint_for_role(role: str | None) -> str:
    r = (role or "").lower().strip()
    # minimal, explicit mapping (expand later if needed)
    if r in ("reading_learner", "learner"):
        return "reading_bp.learner_dashboard"
    if r in ("reading_tutor", "tutor"):
        return "reading_bp.tutor_dashboard"
    return "public_bp.welcome"

def _norm_role(r: str) -> str:
    r = _lower(r)
    # accept legacy like "loss_user"
    if "_" in r:
        r = r.split("_")[-1]
    aliases = {"student": "learner", "mgr": "manager", "pm": "manager"}
    return aliases.get(r, r)

class SubjectCtx(TypedDict, total=False):
    action: str  # "redirect_welcome" | "redirect_bridge" | "resume_checkout" | "show_register_form"
    message: str
    subject_slug: str
    subject_id: int
    role: str
    next_url: str
    amount: Optional[str]
    currency: Optional[str]
    purpose: Optional[str]
    name: Optional[str]  # product name
    bridge_url: Optional[str]

def _norm(s: Optional[str]) -> str:
    return (s or "").strip()

def subject_id_from_slug(slug: str | None) -> int | None:
    """Return subject.id for a given slug (case-insensitive), or None."""
    if not slug:
        return None
    row = db.session.execute(
        text("SELECT id FROM auth_subject WHERE lower(slug)=:s LIMIT 1"),
        {"s": slug.lower().strip()},
    ).fetchone()
    return int(row[0]) if row else None

def _enrollment_row(user_id: int, subject_id: int):
    return db.session.execute(
        text("""
            SELECT user_id, subject_id, status, COALESCE(payment_pending, 0) AS payment_pending
            FROM user_enrollment
            WHERE user_id = :uid AND subject_id = :sid
            LIMIT 1
        """),
        {"uid": user_id, "sid": subject_id},
    ).fetchone()

def _import_models():
    """
    Import models from whichever module layout you're using.
    Returns (User, AuthEnrollment, AuthSubject, AuthPaymentLog)
    """
    User = AuthEnrollment = AuthSubject = AuthPaymentLog = None

    # Try the most likely locations first
    try:
        from app.models.auth import AuthEnrollment as _AE, AuthSubject as _AS, AuthPaymentLog as _APL
        AuthEnrollment, AuthSubject, AuthPaymentLog = _AE, _AS, _APL
    except Exception:
        pass

    try:
        from app.models.auth import User as _U  # pragma: no cover
        User = _U
    except Exception:
        # Sometimes User also sits in app.models.auth or app.models
        try:
            from app.models.auth import User as _U2
            User = _U2
        except Exception:
            try:
                from app.models.auth import User as _U3
                User = _U3
            except Exception:
                User = None

    if AuthEnrollment is None or AuthSubject is None:
        # Last resort: try app.models.* root
        try:
            from app.models import AuthEnrollment as _AE2, AuthSubject as _AS2  # type: ignore
            AuthEnrollment, AuthSubject = _AE2, _AS2
        except Exception:
            pass

    if AuthPaymentLog is None:
        try:
            from app.models import AuthPaymentLog as _APL2  # type: ignore
            AuthPaymentLog = _APL2
        except Exception:
            pass

    return User, AuthEnrollment, AuthSubject, AuthPaymentLog

def _coerce_user_id(user_or_id: Union[int, Any]) -> int:
    """Accept a User model or an int and return a concrete user_id (int)."""
    uid = getattr(user_or_id, "id", user_or_id)
    try:
        return int(uid)
    except (TypeError, ValueError):  # pragma: no cover
        raise ValueError("resolve_enrollment_decision(): invalid user/user_id")

def _subject_lookup_by_slug_or_name(AuthSubject, token: str):
    """
    Best-effort lookup of a subject by slug or name.
    Safe to call even if those columns differ; falls back to first match.
    """
    # Try slug first if it exists
    try:
        subj = AuthSubject.query.filter(AuthSubject.slug == token).first()
        if subj:
            return subj
    except Exception:
        pass

    # Try name next
    try:
        subj = AuthSubject.query.filter(AuthSubject.name == token).first()
        if subj:
            return subj
    except Exception:
        pass

    # Try code next
    try:
        subj = AuthSubject.query.filter(AuthSubject.code == token).first()
        if subj:
            return subj
    except Exception:
        pass

    return None

def _extract_subject_id_from_request() -> Tuple[Optional[int], Optional[Any]]:
    """
    Pull subject from query/form as id or slug/name.
    Returns (subject_id, subject_obj_if_we_found_it)
    """
    User, AuthEnrollment, AuthSubject, _ = _import_models()
    if AuthSubject is None:
        return None, None

    # Accept several param names
    raw_sid = (
        request.values.get("subject_id")
        or request.values.get("sid")
        or request.values.get("subject_id[]")  # sometimes from selects
    )
    if raw_sid:
        try:
            sid = int(raw_sid)
            subj = AuthSubject.query.get(sid)
            return (sid if subj else None), subj
        except (TypeError, ValueError):
            pass

    # Maybe a slug / name token
    token = request.values.get("subject") or request.values.get("slug") or request.values.get("program")
    if token:
        subj = _subject_lookup_by_slug_or_name(AuthSubject, token)
        if subj and getattr(subj, "id", None):
            return int(subj.id), subj

    return None, None

def _has_attr(model_obj, attr_name: str) -> bool:
    try:
        getattr(model_obj, attr_name)
        return True
    except Exception:
        return False

def _row_has_column(table_name: str, col_name: str) -> bool:
    """
    Very defensive: checks sqlite pragma for column existence.
    Works only for SQLite, but is harmless elsewhere.
    """
    try:
        if not db.session.bind.dialect.name.startswith("sqlite"):
            return True  # assume yes for non-sqlite
        res = db.session.execute(text(f"PRAGMA table_info({table_name})")).mappings().all()
        cols = {r["name"] for r in res}
        return col_name in cols
    except Exception:
        return True  # don't block behavior if PRAGMA not available

def resolve_enrollment_decision(user_or_id: Union[int, Any], subject_id: Optional[int]) -> Dict[str, Any]:
    """
    Compute the *next action* for /register based on user's enrollment status.

    Returns a dict with one of the following actions (your route already branches on these):
      - redirect_welcome      -> Subject not chosen / invalid
      - redirect_bridge       -> Already enrolled -> send to bridge/dashboard
      - resume_checkout       -> Existing, payment pending
      - start_checkout        -> No enrollment yet -> start a checkout/enroll flow
    """
    User, AuthEnrollment, AuthSubject, AuthPaymentLog = _import_models()
    uid = _coerce_user_id(user_or_id)

    # 1) No/invalid subject -> bounce to welcome/chooser
    if not subject_id or not AuthSubject:
        return {
            "action": "redirect_welcome",
            "message": "Please choose a subject to continue.",
            "subject_id": None,
            "role": "learner",
        }

    subject = None
    try:
        subject = AuthSubject.query.get(subject_id)
    except Exception:
        subject = None

    if subject is None:
        return {
            "action": "redirect_welcome",
            "message": "That subject wasn’t found. Please choose again.",
            "subject_id": None,
            "role": "learner",
        }

    # 2) Look up existing enrollment
    enr = None
    if AuthEnrollment is not None:
        try:
            enr = AuthEnrollment.query.filter_by(user_id=uid, subject_id=int(subject_id)).first()
        except Exception:
            enr = None

    # Determine flags in a way that won’t explode if columns are missing
    status = (getattr(enr, "status", None) or "").lower() if enr else None
    completed = bool(getattr(enr, "completed", False)) if enr and _has_attr(enr, "completed") else False

    # payment_pending may not exist early in your migrations
    if enr and _row_has_column("auth_enrollment", "payment_pending") and _has_attr(enr, "payment_pending"):
        payment_pending = bool(getattr(enr, "payment_pending", False))
    else:
        # fallback heuristic via payment_log if available
        payment_pending = False
        if AuthPaymentLog is not None and enr is not None:
            try:
                last_pay = (
                    AuthPaymentLog.query.filter_by(user_id=uid)
                    .order_by(AuthPaymentLog.id.desc())
                    .first()
                )
                # treat "pending", "unpaid" as pending
                if last_pay:
                    pay_status = (getattr(last_pay, "status", "") or "").lower()
                    payment_pending = pay_status in {"pending", "requires_action", "unpaid", "incomplete"}
            except Exception:
                payment_pending = False

    # 3) Decide
    if enr:
        # Already active/completed -> bridge
        if status in {"active", "enrolled"} or completed:
            return {
                "action": "redirect_bridge",
                "message": "You’re already enrolled.",
                "subject_id": int(subject_id),
                "role": getattr(enr, "role", "learner") or "learner",
                "bridge_url": url_for("auth_bp.bridge_dashboard", role=getattr(enr, "role", "learner") or "learner"),
            }

        # Enrollment exists but waiting on payment -> resume
        if payment_pending or status in {"pending", "awaiting_payment"}:
            return {
                "action": "resume_checkout",
                "message": "You have a pending enrollment. Please complete payment.",
                "subject_id": int(subject_id),
                "role": getattr(enr, "role", "learner") or "learner",
            }

        # Enrollment record exists but not active -> start/refresh checkout
        return {
            "action": "start_checkout",
            "message": "Let’s finalize your enrollment.",
            "subject_id": int(subject_id),
            "role": getattr(enr, "role", "learner") or "learner",
        }

    # 4) No enrollment yet -> start checkout
    return {
        "action": "start_checkout",
        "message": "You’re almost there—let’s set up your enrollment.",
        "subject_id": int(subject_id),
        "role": "learner",
    }

def _norm_email(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    return (raw.strip().replace(" ", "").replace(",", ".")).lower()

def get_user_by_email(email: str) -> Optional[User]:
    """
    Look up a user by normalized email.
    Prefers active users (is_active=1) first, else earliest inactive match.
    """
    norm = _norm_email(email)
    if not norm:
        return None
    return (
        db.session.query(User)
        .filter(
            func.lower(
                func.replace(
                    func.replace(func.trim(User.email), " ", ""),
                    ",", ".",
                )
            ) == norm
        )
        .order_by(User.is_active.desc(), User.id.asc())  # active first
        .first()
    )

def require_subject_and_enrollment_context() -> dict:
    """
    High-level resolver for /register.
    - Finds subject_id (sid) from request
    - Resolves user:
        * if logged in -> use current_user
        * else if email provided -> use get_user_by_email(email) (active-first)
        * else -> redirect_welcome
    - Delegates to resolve_enrollment_decision()
    """
    sid, _subject_obj = _extract_subject_id_from_request()

    # Resolve user (no login here; purely lookup so routes stay clean)
    user_obj = None
    if current_user and getattr(current_user, "is_authenticated", False):
        user_obj = current_user
    else:
        # pull email from POST/GET if present
        email = request.form.get("email") or request.values.get("email")
        if email:
            user_obj = get_user_by_email(email)

    # If we still don't have a user, bounce to welcome
    if not user_obj:
        return {
            "action": "redirect_welcome",
            "message": "Please sign in or enter your email to continue.",
            "subject_id": sid,
            "bridge_url": None,
            "role": "learner",
        }

    # Decide next step using the tolerant function (accepts user object)
    ctx = resolve_enrollment_decision(user_obj, sid)

    # Ensure expected keys
    ctx.setdefault("subject_id", sid)
    ctx.setdefault("bridge_url", None)
    ctx.setdefault("role", getattr(user_obj, "role", "learner") or "learner")

    return ctx

def _get_or_create_user_by_email(email: str):
    email = (email or "").strip().lower()
    uid = db.session.execute(sa_text('SELECT MIN(id) FROM "user" WHERE lower(email)=:e'), {"e": email}).scalar()
    if uid:
        return int(uid)

    # create a minimal row (adjust columns to match your model)
    db.session.execute(sa_text('''
        INSERT INTO "user" (email, role, is_active, name)
        VALUES (:e, 'user', 1, COALESCE(NULLIF(:name,''), :e))
    '''), {"e": email, "name": email.split("@")[0] if "@" in email else email})
    db.session.commit()
    return int(db.session.execute(sa_text('SELECT MIN(id) FROM "user" WHERE lower(email)=:e'), {"e": email}).scalar())

def _subject_id_from_slug_or_name(subject: str|None):
    s = (subject or "").strip().lower()
    if not s:
        return None
    return db.session.execute(sa_text("""
        SELECT id FROM auth_subject
        WHERE lower(slug)=:s OR lower(name)=:s
        LIMIT 1
    """), {"s": s}).scalar()

def _ensure_enrollment_status(user_id: int, subject_id: int, status: str):
    # keep only one row per (user,subject); if exists, update status
    db.session.execute(sa_text("""
        INSERT INTO user_enrollment (user_id, subject_id, status)
        VALUES (:uid, :sid, :st)
        ON CONFLICT(user_id, subject_id) DO UPDATE SET status=excluded.status
    """), {"uid": user_id, "sid": subject_id, "st": status})

def _table_has_columns(table: str, *cols: str) -> set:
    rows = db.session.execute(sa_text(f"PRAGMA table_info({table})")).all()
    have = {r[1] for r in rows}  # r[1] = column name
    return {c for c in cols if c in have}

def _insert_payment_log(payload: dict):
    """
    Schema-aware insert into auth_payment_log.
    Only inserts columns that exist in your DB.
    """
    cols_in_db = _table_has_columns(
        "auth_payment_log",
        "user_id", "subject", "subject_id",
        "amount", "currency",
        "status", "provider",
        "external_ref", "purpose",
        "created_at", "meta_json"
    )
    if not cols_in_db:
        return  # table missing; silently skip to avoid 500s

    cols = []
    vals = []
    params = {}
    for k in cols_in_db:
        cols.append(k)
        vals.append(f":{k}")
        params[k] = payload.get(k)

    sql = f"INSERT INTO auth_payment_log ({', '.join(cols)}) VALUES ({', '.join(vals)})"
    db.session.execute(sa_text(sql), params)

def _update_payment_log_by_extref(external_ref: str, status: str, extra: dict|None=None):
    if not external_ref:
        return
    cols_in_db = _table_has_columns("auth_payment_log", "status", "meta_json", "external_ref")
    sets = []
    params = {"external_ref": external_ref}
    if "status" in cols_in_db:
        sets.append("status=:status")
        params["status"] = status
    if extra and "meta_json" in cols_in_db:
        sets.append("meta_json=:meta_json")
        params["meta_json"] = extra.get("meta_json")
    if sets:
        db.session.execute(
            sa_text(f"UPDATE auth_payment_log SET {', '.join(sets)} WHERE external_ref=:external_ref"),
            params
        )
