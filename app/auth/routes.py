# app/auth/routes.py
from datetime import datetime, timezone
import datetime as dt
from flask import session as flask_session
import re
import json
import uuid
from flask import (
    Blueprint, current_app, make_response, render_template,
    redirect, url_for, request, flash,
    session, abort
    )
import stripe
#from app.checkout.stripe_client import fetch_subject_price, record_stripe_payment
from app.extensions import db
from werkzeug.security import check_password_hash
from flask_login import login_user, logout_user, login_required, current_user
#from app.auth.decisions import (
#    _canon_email_py, _subject_id_from_slug_fallback, 
#    _work_key, find_user_active_first_by_email, get_reg_context)
# app.checkout.routes import _create_checkout_session, _get_stripe_api_key
from app.models import subject
from app.utils.country_list import COUNTRIES, resolve_country, search_countries  # adjust path if needed
from app.utils.mailer import send_email
from app.utils.nav import resolve_next
from app.utils.queries import BRIDGE_QUERY
from app.utils.role_utils import get_dashboard_route
from sqlalchemy import text as sa_text, bindparam
from app.auth.forms import LoginForm, RegisterForm
from sqlalchemy.exc import IntegrityError
from urllib.parse import urlparse, urljoin
from app.models.auth import ApprovedAdmin, AuthSubject, User, UserEnrollment  # <-- make sure you added this model
from sqlalchemy import text, inspect, update
from sqlalchemy.exc import OperationalError, ProgrammingError
from .session_utils import set_identity
from flask_wtf.csrf import generate_csrf
from app.auth.helpers import (
    _ensure_enrollment_status,
    _get_or_create_user_by_email,
    _insert_payment_log,
    _subject_id_from_slug_or_name,
    require_subject_and_enrollment_context,
    subject_id_from_slug,
)
from app.utils.enrollment import ensure_pending_enrollment
from sqlalchemy import func
from app.utils.pricing import get_subject_price, get_subject_plan, format_currency
from app.utils.enrollment import is_enrolled
from werkzeug.routing import BuildError
from app.utils.password_reset import make_reset_token, load_reset_token
from flask_wtf.csrf import generate_csrf
from app.auth.registeruser import decide_registration_flow
from sqlalchemy import func as SA_FUNC, text as SA_TEXT
from sqlalchemy.exc import IntegrityError
from flask import render_template, render_template_string
from jinja2 import TemplateNotFound

from sqlalchemy.exc import IntegrityError
from sqlalchemy import select, func, update as sa_update

from sqlalchemy.exc import IntegrityError

auth_bp = Blueprint('auth_bp', __name__, url_prefix='/', template_folder='templates')

@auth_bp.get("/start_registration", endpoint="start_registration")
def start_registration():
    # Preserve old links: /start_registration?subject=...&role=...&next=...
    # 1) stash into session for downstream reads (optional but nice)
    role = (request.args.get("role") or "").strip()
    subject = (request.args.get("subject") or "").strip().lower()
    if role:
        session["role"] = role
    if subject:
        session["subject"] = subject

    # 2) hand off to the real register endpoint with ALL original query params
    return redirect(url_for("auth_bp.register", **request.args))



@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    form = LoginForm()

    # GET: prefill email for convenience (kept same behavior)
    if request.method == "GET":
        form.email.data = current_app.config.get("DEFAULT_LOGIN_EMAIL", "")
        return render_template("auth/login.html", form=form)

    # POST validation
    if not form.validate_on_submit():
        flash("Please correct the errors below.", "warning")
        return render_template("auth/login.html", form=form), 200

    # Normalize inputs
    email = (form.email.data or "").strip().lower()
    password_attempt = form.password.data or ""

    user = User.query.filter_by(email=email).first()
    if not user:
        flash("Invalid email or password.", "danger")
        return render_template("auth/login.html", form=form), 200

    # --- Password verification + legacy upgrade ---
    from werkzeug.security import check_password_hash, generate_password_hash

    def _check_hash(pw_hash: str, attempt: str) -> bool:
        # Prefer model's method if present, else raw werkzeug
        if hasattr(user, "check_password"):
            try:
                return bool(user.check_password(attempt))
            except Exception:
                # fall back to direct hash check
                pass
        return check_password_hash(pw_hash, attempt)

    is_ok = False
    pw_hash = getattr(user, "password_hash", None)

    if pw_hash:
        # normal modern path
        is_ok = _check_hash(pw_hash, password_attempt)

    else:
        # legacy plaintext path (user.password stored in DB)
        legacy_plain = getattr(user, "password", None)
        if legacy_plain:
            # upgrade: create password_hash from legacy plaintext once
            new_hash = generate_password_hash(legacy_plain)

            if hasattr(user, "set_password"):
                # if model implements set_password, let it manage fields
                try:
                    user.set_password(legacy_plain)
                except Exception:
                    # fallback to manual if their set_password explodes
                    user.password_hash = new_hash
            else:
                user.password_hash = new_hash

            # try wipe legacy cleartext
            try:
                setattr(user, "password", None)
            except Exception:
                pass

            db.session.commit()

            # now verify against what they just typed
            is_ok = _check_hash(user.password_hash, password_attempt)

    if not is_ok:
        flash("Invalid email or password.", "danger")
        return render_template("auth/login.html", form=form), 200

    # ── Auth success ─────────────────────────────────────────
    # purge old session keys to avoid stale state bleed
    for k in (
        "is_authenticated", "email", "is_admin",
        "admin_subjects", "enrolled_subjects",
        "subjects_access", "user_id", "user_name",
        "just_paid_subject_id", "payment_banner",
        "pending_email", "pending_subject", "pending_session_ref",
        "role",
    ):
        session.pop(k, None)

    # remember_me checkbox name fix:
    # template uses form.remember → so use that safely
    remember_flag = False
    if hasattr(form, "remember"):
        remember_flag = bool(getattr(form.remember, "data", False))
    elif hasattr(form, "remember_me"):
        remember_flag = bool(getattr(form.remember_me, "data", False))

    login_user(user, remember=remember_flag, fresh=True)

    # ── Identity scaffold (we'll set admin AFTER we compute it) ──
    session["is_authenticated"] = True
    session["email"] = (user.email or "").lower()
    session["user_id"] = int(user.id)
    session["user_name"] = (
        user.name
        or (user.email.split("@", 1)[0] if user.email else "")
    )

    # ---------- permissions + enrollment snapshot ----------
    from sqlalchemy import inspect, text
    from sqlalchemy.exc import OperationalError, ProgrammingError
    insp = inspect(db.engine)
    tables = set(insp.get_table_names())

    def _exists(*names: str) -> bool:
        return all(n in tables for n in names)

    # 1) global admin?
    is_admin_global = False
    if _exists("auth_approved_admin"):
        try:
            is_admin_global = db.session.execute(
                text("SELECT 1 FROM auth_approved_admin WHERE lower(email)=lower(:e) LIMIT 1"),
                {"e": email}
            ).fetchone() is not None
        except (OperationalError, ProgrammingError):
            is_admin_global = False

    # stash it
    session["is_admin"] = bool(is_admin_global)

    # 2) subject admin slugs
    admin_subjects = []
    if _exists("auth_subject_admin", "auth_subject"):
        try:
            rows = db.session.execute(
                text("""
                    SELECT s.slug
                    FROM auth_subject_admin sa
                    JOIN auth_subject s ON s.id = sa.subject_id
                    WHERE lower(sa.email) = lower(:e)
                """),
                {"e": email}
            ).fetchall()
            admin_subjects = [r.slug for r in rows]
        except (OperationalError, ProgrammingError):
            pass
    session["admin_subjects"] = admin_subjects

    # 3) enrolled subject slugs (from user_enrollment)
    enrolled_subjects = []
    if _exists("user_enrollment", "auth_subject"):
        try:
            rows = db.session.execute(
                text("""
                    SELECT s.slug
                    FROM user_enrollment ue
                    JOIN auth_subject s ON s.id = ue.subject_id
                    WHERE ue.user_id = :uid
                      AND ue.status  = 'active'
                """),
                {"uid": user.id}
            ).fetchall()
            enrolled_subjects = [r.slug for r in rows]
        except (OperationalError, ProgrammingError) as ex:
            current_app.logger.warning(f"[login] user_enrollment join not usable: {ex!r}")
    else:
        current_app.logger.info("[login] tables user_enrollment/auth_subject missing; no enrolled slugs")
    session["enrolled_subjects"] = enrolled_subjects

    # 4) build access map purely from admin/enrolled sets
    subjects_access = {}
    if _exists("auth_subject"):
        try:
            rows = db.session.execute(
                text("""
                    SELECT s.slug
                    FROM auth_subject s
                    WHERE s.is_active = 1
                    ORDER BY s.sort_order, s.name
                """)
            ).fetchall()
            slugs = [r.slug for r in rows]
            admin_set = set(admin_subjects)
            enrolled_set = set(enrolled_subjects)

            for slug in slugs:
                if is_admin_global or slug in admin_set:
                    level = "admin"
                elif slug in enrolled_set:
                    level = "enrolled"
                else:
                    level = "locked"
                subjects_access[slug] = level

        except (OperationalError, ProgrammingError) as ex:
            current_app.logger.warning(f"[login] auth_subject not usable: {ex!r}")
    else:
        current_app.logger.info("[login] table auth_subject missing; no subjects_access")
    session["subjects_access"] = subjects_access

    # NOW that we know is_admin, assign role
    session["role"] = "admin" if session.get("is_admin") else "user"

    # ---------- redirect ----------
    from urllib.parse import urljoin, urlparse
    def _is_safe_url(target: str) -> bool:
        if not target:
            return False
        ref = urlparse(request.host_url)
        test = urlparse(urljoin(request.host_url, target))
        return (
            (test.scheme in ("http", "https"))
            and (ref.netloc == test.netloc)
        )

    next_url = request.args.get("next")
    if not _is_safe_url(next_url):
        next_url = url_for("auth_bp.bridge_dashboard")
    return redirect(next_url)

@auth_bp.route("/logout", methods=["GET", "POST"])
def logout():
    logout_user()      # clears Flask-Login session + remember data
    session.clear()    # wipe our own keys

    # Extra-safe: explicitly delete remember cookie if set
    resp = redirect(url_for("public_bp.welcome"))
    try:
        resp.delete_cookie("remember_token")
    except Exception:
        pass

    flash("Logged out.", "info")
    return resp


@auth_bp.route("/admin/select_subject", methods=["GET", "POST"])
@login_required
def select_subject():
    if current_user.role != "admin":
        flash("Access denied", "danger")
        return redirect(url_for("public_bp.welcome"))

    available_subjects = [
        ("billing", "Billing"),
        ("reading", "Reading"),
        ("home", "Math (HOME)"),
        ("loss", "Loss Counselling")
    ]

    if request.method == "POST":
        chosen_subject = request.form.get("subject")
        if chosen_subject:
            session["subject"] = chosen_subject

            # ✅ Call the helper to get the dashboard endpoint
            endpoint = get_dashboard_route("admin", chosen_subject)
            print(f"[DEBUG] Admin selected → subject='{chosen_subject}', endpoint='{endpoint}'")

            if endpoint:
                return redirect(url_for(endpoint))
            else:
                flash("No dashboard available for this selection.", "warning")
                return redirect(url_for("public_bp.welcome"))

        flash("Please select a subject.", "warning")

    return render_template("auth/select_subject.html", subjects=available_subjects)

# add this near the top of the file after auth_bp = Blueprint(...)
@auth_bp.app_context_processor
def inject_has_endpoint():
    from flask import current_app
    def has_endpoint(ep: str) -> bool:
        return ep in current_app.view_functions
    return {"has_endpoint": has_endpoint}

@auth_bp.route("/dev-login/reading/admin")
def dev_login_reading_admin():
    if not current_app.debug: abort(404)
    session.update({"email": "san@gmail.com", "role": "admin", "subject": "reading", "is_admin": True})
    ep, p = get_dashboard_route("admin", subject="reading", with_params=True)
    return redirect(url_for(ep, **(p or {})))

@auth_bp.route("/admin/login")
def admin_login_shortcut():
    admin_email = current_app.config.get("DEFAULT_LOGIN_EMAIL", "san@gmail.com")
    return redirect(url_for("auth_bp.login", email=admin_email))

@auth_bp.route("/dashboard")
def bridge_dashboard():
    current_app.logger.info(
        "BRIDGE entry: host=%s auth=%s uid=%s",
        request.host,
        getattr(current_user, "is_authenticated", False),
        getattr(current_user, "id", None),
    )

    # Resolve email once
    email = (session.get("email") or "").lower()
    if not email and getattr(current_user, "is_authenticated", False):
        try:
            u = User.query.get(int(getattr(current_user, "id", 0)))
            if u and u.email:
                email = u.email.lower()
                session["email"] = email
        except Exception:
            pass
    if not email:
        qemail = (request.args.get("email") or "").strip().lower()
        if qemail:
            email = qemail
            session["email"] = email
    if not email:
        return redirect(url_for("auth_bp.login"))

    # One-time focus from Stripe success
    open_sid = session.pop("just_paid_subject_id", None)

    base_sql = BRIDGE_QUERY.strip()
    # drop a trailing semicolon so we can reuse / modify the text safely
    if base_sql.endswith(";"):
        base_sql = base_sql[:-1]

    params = {"email": email}

    if open_sid is not None:
        # check if base_sql already has a WHERE clause
        if "WHERE" in base_sql.upper():
            filtered_sql = base_sql.replace(
                "ORDER BY s.name",
                "AND s.id = :open_sid ORDER BY s.name"
            )
        else:
            filtered_sql = base_sql.replace(
                "ORDER BY s.name",
                "WHERE s.id = :open_sid ORDER BY s.name"
            )

        sql = sa_text(filtered_sql)
        params["open_sid"] = int(open_sid)
        rows = db.session.execute(sql, params).fetchall()



        # Fallback to full list if the filter returns nothing
        if not rows:
            rows = db.session.execute(sa_text(base_sql), {"email": email}).fetchall()
    else:
        rows = db.session.execute(sa_text(base_sql), params).fetchall()

    banner = session.pop("payment_banner", None)
    return render_template("auth/bridge_dashboard.html", subjects=rows, banner=banner)

@login_required
@auth_bp.route("/dashboard/learn/<subject>", methods=["GET"])
def learner_subject_dashboard(subject):
    subj_key = (subject or "").strip().lower()

    row = db.session.execute(text("""
        SELECT id,
               COALESCE(slug, name) AS slug,
               COALESCE(name, slug) AS name
        FROM auth_subject
        WHERE LOWER(COALESCE(slug, name)) = :s
        LIMIT 1
    """), {"s": subj_key}).mappings().first()
    if not row:
        return render_template("errors/not_found.html"), 404

    slug = row["slug"].lower()

    # 🔧 Minimal fix: loss → /loss/subject/home
    if slug == "loss":
        start_url = url_for("loss_bp.subject_home")
    else:
        # leave other subjects as they were
        try:
            start_url = url_for(f"{slug}_bp.subject_home")
        except BuildError:
            start_url = url_for("auth_bp.bridge_dashboard")

    just_paid = bool(session.pop("just_paid", False))

    return render_template(
        "auth/learner_subject_dashboard.html",
        subject=row,
        start_url=start_url,
        just_paid=just_paid,
        color_bar="bg-indigo-600",
    )


@auth_bp.route("/dashboard/info/<subject>", methods=["GET"])
def dashboard_info(subject: str):
    email = session.get("email")
    if not email:
        return redirect(url_for("auth_bp.login"))

    row = db.session.execute(
        text("SELECT id, slug, name FROM auth_subject WHERE slug = :slug AND is_active = 1"),
        {"slug": subject.strip().lower()}
    ).fetchone()
    if not row:
        abort(404)

    flash(f"You are not registered for {row.name}. Please contact an administrator if you need access.", "info")
    return redirect(url_for("auth_bp.bridge_dashboard"))

@auth_bp.route("/dev-login")
def dev_login():
    if not current_app.debug:
        abort(404)

    email   = (request.args.get("email") or "dev@example.com").strip().lower()
    name    = (request.args.get("name")  or email.split("@")[0].title())
    role_in = (request.args.get("role")  or "admin").strip().lower()  # default to admin for dev
    subject = (request.args.get("subject") or "").strip().lower()
    try:
        uid = int(request.args.get("user_id") or 1)
    except ValueError:
        uid = 1

    # derive subject like "reading_admin" -> "reading"
    if not subject and "_" in role_in:
        subject = role_in.split("_", 1)[0]

    # compute is_admin: from role OR from DB allowlist
    is_admin = role_in == "admin" or role_in.endswith("_admin")
    if not is_admin and email:
        try:
            with db.engine.begin() as conn:
                ok = conn.execute(
                    text("SELECT 1 FROM auth_approved_admin WHERE lower(email)=lower(:e) LIMIT 1"),
                    {"e": email}
                ).scalar()
                is_admin = bool(ok)
        except Exception as ex:
            current_app.logger.warning("DEV-LOGIN allowlist check failed: %r", ex)

    role = "admin" if is_admin else ("learner" if role_in in {"learner","reading_learner","loss_learner"} else role_in)

    set_identity(session,
        uid=uid, name=name, email=email, role=role, subject=subject, is_admin=is_admin
    )

    # Land somewhere sensible
    return redirect(url_for("admin_bp.bridge_dashboard" if is_admin else "auth_bp.bridge_dashboard"))

@auth_bp.route("/whoami")
def whoami():
    keys = ["user_id","user_name","user_email","email","role","user_role","subject","is_admin"]
    return {k: session.get(k) for k in keys}

@auth_bp.route("/dev/elevate")
def dev_elevate():
    if not current_app.debug:
        abort(404)
    session["role"] = "admin"
    session["user_role"] = "admin"
    session["is_admin"] = True
    session.setdefault("user_email", session.get("email","dev@example.com"))
    return redirect(url_for("admin_bp.bridge_dashboard"))

def _is_safe_url(target: str) -> bool:
    if not target:
        return False
    ref = urlparse(request.host_url)
    test = urlparse(urljoin(request.host_url, target))
    return (test.scheme in ("http", "https")) and (ref.netloc == test.netloc)

def is_global_admin(email: str) -> bool:
    if not email:
        return False
    row = db.session.execute(text("""
        # NEW (use view that always has 'active')
        SELECT 1
        FROM approved_admins
        WHERE lower(email)=lower(:e)
        AND active=1
        LIMIT 1

    """), {"e": email}).fetchone()
    return row is not None

def _resolve_dashboard(is_admin: bool, subject: str | None):
    """
    Decide where to send the user after dev-login.
    Tries endpoints in order; falls back to welcome if missing.
    """
    subject = (subject or "").lower()

    # Admin first
    if is_admin:
        for ep in ("admin_bp.dashboard", "admin_bp.bridge", "admin_bp.index"):
            try:
                url_for(ep)
                return ep, {}
            except Exception:
                pass

    # Subject-specific (learner/editor etc.)
    subject_targets = {
        "loss":    [("loss_bp.result_dashboard", {}),
                    ("loss_bp.course_start", {})],
        "reading": [("reading_bp.dashboard", {}),
                    ("reading_bp.admin_dashboard", {}),
                    ("reading_bp.index", {})],
        "billing": [("admin_bp.billing_dashboard", {}),
                    ("admin_bp.dashboard", {})],
        "home":    [("public_bp.dashboard", {}),
                    ("public_bp.welcome", {})],
    }
    for ep, params in subject_targets.get(subject, []):
        # Admin first (unchanged)
        # ...
        # Subject-agnostic bridge next
        try:
            url_for("auth_bp.bridge_dashboard", role="learner")
            return "auth_bp.bridge_dashboard", {"role": "learner"}
        except Exception:
            pass
        return "public_bp.welcome", {}

def redirect_to_first(*endpoints):
    """Redirect to the first endpoint that exists. Logs what it picked."""
    for ep in endpoints:
        try:
            return redirect(url_for(ep))
        except Exception:
            continue
    current_app.logger.warning("No admin dashboard endpoint found; falling back to public welcome")
    return redirect(url_for("public_bp.welcome"))

def _norm(s): return s.strip() if s else ""

def _is_valid_password(pw: str) -> bool:
    return bool(pw and len(pw) >= 3)

def _safe_next():
    return request.args.get("next") or request.referrer or "/"


@auth_bp.route("/forgot", methods=["GET", "POST"], endpoint="forgot")
def forgot():
    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        user = User.query.filter_by(email=email).first()

        if user:
            token = make_reset_token(user.id)
            reset_url = url_for("auth_bp.reset", token=token, _external=True)
            html = render_template("auth/email_reset.html", reset_url=reset_url, user=user)

            ok = send_email(
                "Reset your password",
                [email],
                body=f"Reset link: {reset_url}",
                html=html,
            )
            current_app.logger.info("forgot: send_email ok=%s to=%s", ok, email)
        else:
            current_app.logger.info("forgot: no account for %s", email)

        # IMPORTANT: redirect after POST so the page doesn't 'hang'
        flash("If that email exists, a reset link has been sent.", "info")
        return redirect(url_for("auth_bp.login"))

    # GET
    return render_template("auth/forgot.html", csrf_token=generate_csrf())

@auth_bp.route("/reset/<token>", methods=["GET", "POST"], endpoint="reset")
def reset(token):
    user_id = load_reset_token(token)
    if not user_id:
        flash("Reset link is invalid or expired.", "danger")
        return redirect(url_for("auth_bp.forgot"))

    user = User.query.get(user_id)
    if not user:
        flash("Account not found.", "danger")
        return redirect(url_for("auth_bp.forgot"))

    if request.method == "POST":
        pw = request.form.get("password") or ""
        if len(pw) < 8:
            flash("Password must be at least 8 characters.", "warning")
            return render_template("auth/reset.html")
        user.set_password(pw)
        db.session.commit()
        flash("Password updated. Please sign in.", "success")
        return redirect(url_for("auth_bp.login"))

    return render_template("auth/reset.html", csrf_token=generate_csrf())

@auth_bp.get("/debug/sendmail")
def _debug_sendmail():
    #from app.utils.emailer import send_email
    ok = send_email(
        "SMTP test (AIT)",
        ["sanjith.nanhoo@gmail.com"],  # your external inbox
        body="This is a test from AIT via Zoho SMTP."
    )
    return ("OK" if ok else "FAIL"), (200 if ok else 500)

@auth_bp.get("/debug/mailconfig")
def debug_mailconfig():
    from flask import current_app as app, jsonify
    cfg = {k: app.config.get(k) for k in (
        "MAIL_SERVER","MAIL_PORT","MAIL_USE_TLS","MAIL_USE_SSL",
        "MAIL_USERNAME","MAIL_DEFAULT_SENDER"
    )}
    cfg["MAIL_PASSWORD_SET"] = bool(app.config.get("MAIL_PASSWORD"))
    return jsonify(cfg), 200

def _build_enrol_decision(user, subject_slug, subject_name):
    """
    Returns a tiny object/dict with .case, .subject_slug, .subject_name, .completed_at (optional).
    Cases: 'already_active' | 'closed' | 'other_subjects' | 'payment_pending' | None
    """
    from types import SimpleNamespace
    D = SimpleNamespace(case=None, subject_slug=subject_slug, subject_name=subject_name, completed_at=None)

    # adapt these imports/attributes to your models
    from app.models import UserEnrollment

    if not user:
        return D  # no decision needed

    # enrollment for this subject?
    q = UserEnrollment.query.filter_by(user_id=user.id)
    # use whichever field identifies the subject
    if hasattr(UserEnrollment, "subject_slug"):
        q_subj = q.filter_by(subject_slug=subject_slug)
    elif hasattr(UserEnrollment, "program"):
        q_subj = q.filter_by(program=subject_slug)
    else:
        q_subj = q  # last resort

    row = q_subj.first()

    if row:
        status = getattr(row, "status", None)
        payment_pending = getattr(row, "payment_pending", False)
        completed = getattr(row, "completed", False)

        if payment_pending:
            D.case = "payment_pending"
            return D

        if status == "active" or (completed in (False, 0) and status not in ("closed", "completed")):
            D.case = "already_active"
            return D

        if status in ("closed", "completed") or completed in (True, 1):
            D.case = "closed"
            # optional: supply a date if you have it
            D.completed_at = getattr(row, "updated_at", None) or getattr(row, "created_at", None)
            return D

    # user exists, but no enrollment for this subject → offer enrol
    # (only if user has other enrollments)
    other = q.first()
    if other:
        D.case = "other_subjects"

    return D

@auth_bp.route("/reenrol", methods=["POST"])
def reenrol_existing_user():
    subject = (request.form.get("subject") or "").strip().lower()
    email   = (request.form.get("email") or "").strip().lower()
    role    = (request.form.get("role") or "user").strip().lower()
    if not subject or not email:
        abort(400, "subject and email required")
    user = User.query.filter_by(email=email).first()
    if not user:
        flash("Account not found for that email. Please register.", "warning")
        return redirect(url_for("auth_bp.register", subject=subject, role=role, email=email))
    
    # get the actual user instance for this email (oldest row)
    u = User.query.filter(
            db.func.lower(User.email) == email.lower()
        ).order_by(User.id.asc()).first()

    if u:
        login_user(u, remember=True)          # <-- pass the instance
        session["user_id"]   = int(u.id)
        session["email"]     = (u.email or "").lower()
        session["role"]      = (u.role or "user").lower()
        session["user_name"] = u.name or session["email"].split("@", 1)[0]

    sid = subject_id_from_slug(subject)
    if sid:
        ensure_pending_enrollment(user_id=user.id, subject_id=sid, program=subject)
    if subject == "loss":
        return redirect(url_for("loss_bp.about"))
    if subject == "reading":
        return redirect(url_for("reading_bp.about"))
    return redirect(url_for("public_bp.welcome"))

@auth_bp.route("/enrollment/restart/<int:subject_id>", methods=["POST"])
@login_required
def restart_enrollment(subject_id):
    from app.auth.helpers import archive_enrollment, subject_slug_from_id
    archive_enrollment(current_user.id, subject_id)
    flash("Previous enrollment archived. You can start fresh now.", "info")
    slug = subject_slug_from_id(subject_id) or "loss"
    # Send them back to register (or subject’s about) to begin fresh
    return redirect(url_for("auth_bp.register", subject=slug, role="user"))

# routes.py







EMAIL_RX = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

def normalize_email(s: str) -> str:
    return (s or "").strip().lower()

def htmx_redirect(to_url: str):
    """Return an HTMX-friendly redirect (HX-Redirect) and also works in plain browser."""
    resp = make_response("", 204)
    resp.headers["HX-Redirect"] = to_url
    return resp

def decide(email: str, subject_slug: str):
    """Return a dict describing what we should do for this email+subject."""
    ctx = {"status": "ok", "message": "", "action": "create"}

    if not EMAIL_RX.match(email):
        return {"status": "error", "message": "Please enter a valid email address.", "action": None}

    user = db.session.scalar(select(User).where(User.email == email))
    subject = db.session.scalar(select(AuthSubject).where((AuthSubject.slug == subject_slug) | (AuthSubject.name == subject_slug)))

    if not subject:
        return {"status": "error", "message": "Subject not found.", "action": None}

    if user:
        if getattr(user, "active", 1) == 0:
            return {"status": "warn", "message": "This account is inactive. Please contact support.", "action": None}

        # Check enrollment state
        ae = db.session.scalar(
            select(UserEnrollment)
            .where(UserEnrollment.user_id == user.id, UserEnrollment.subject_id == subject.id)
            .limit(1)
        )
        if ae:
            # already has an enrollment… choose where to send them
            return {"status": "ok", "message": "You’re already enrolled. We’ll take you to your dashboard.", "action": "resume"}

        # no enrollment yet -> ask for password to link to existing account
        return {"status": "ok", "message": "Account exists. Enter password to continue.", "action": "login"}

    # New user path
    return {"status": "ok", "message": "We’ll create your account and enroll you.", "action": "create"}

@auth_bp.post("/register/decision-preview")
def register_decision_preview():
    email = normalize_email(request.form.get("email"))
    subject = (request.form.get("subject") or "loss").strip().lower()
    role = (request.form.get("role") or "user").strip().lower()

    ctx = decide(email, subject)
    # Important: return HTML (200), not 204
    return render_template("auth/_decision_preview.html", ctx=ctx, subject=subject, role=role), 200

from collections import defaultdict

@auth_bp.post("/register/confirm")
def register_confirm():
    action = (request.form.get("action") or "").strip()
    role   = (request.form.get("role") or "").strip().lower()   # <-- per-role from the form
    ctx    = session.get("reg_ctx") or {}
    cemail = ctx.get("keep_email") or _canon_email_py(ctx.get("email",""))

    if action != "archive_only" or not role:
        flash("No action taken.", "info")
        return redirect(url_for("auth_bp.register_decision"), code=303)

    # Keep the lowest-id row active for this (email, role); others inactive.
    sql = f"""
    WITH dup AS (
      SELECT u.id, ROW_NUMBER() OVER (ORDER BY u.id) AS rn
      FROM "user" u
      WHERE ({EMAIL_CANON_SQL}) = :cemail
        AND lower(u.role) = :role
    )
    UPDATE "user"
    SET is_active = CASE WHEN id = (SELECT id FROM dup WHERE rn=1) THEN 1 ELSE 0 END
    WHERE id IN (SELECT id FROM dup);
    """
    db.session.execute(text(sql), {"cemail": cemail, "role": role})
    db.session.commit()

    flash(f"Consolidated duplicates for role '{role}'.", "success")
    return redirect(url_for("auth_bp.register_decision", just_archived=1), code=303)


# app/auth/routes.py — Silent duplicate cleanup + Decision page (no archive UI)


# ---------- Helpers -----------------------------------------------------------

# in app/auth/routes.py (or your API blueprint)
from flask import jsonify, request
#from utils.country_list import search_countries, resolve_country

@auth_bp.get("/api/countries")
def api_countries():
    q = (request.args.get("q") or "").strip()
    return jsonify(search_countries(q, limit=20))

from sqlalchemy import text

def consolidate_duplicates_silently(cemail: str):
    """Keep lowest-id per role for this canonical email. Move UE rows, deactivate losers."""
    # 0) fresh temps
    db.session.execute(text("DROP TABLE IF EXISTS tmp_cand"))
    db.session.execute(text("DROP TABLE IF EXISTS tmp_keepers"))
    db.session.execute(text("DROP TABLE IF EXISTS tmp_losers"))

    # 1) candidates for this canonical email
    db.session.execute(text(f"""
        CREATE TEMP TABLE tmp_cand AS
        SELECT u.id AS id, lower(u.role) AS role
        FROM "user" u
        WHERE ({EMAIL_CANON_SQL}) = :cemail
    """), {"cemail": cemail})

    # 2) survivor (lowest id) per role
    db.session.execute(text("""
        CREATE TEMP TABLE tmp_keepers AS
        SELECT role, MIN(id) AS survivor_id
        FROM tmp_cand
        GROUP BY role
    """))

    # 3) losers (others in that role)
    db.session.execute(text("""
        CREATE TEMP TABLE tmp_losers AS
        SELECT c.id AS loser_id, c.role, k.survivor_id
        FROM tmp_cand c
        JOIN tmp_keepers k ON k.role = c.role
        WHERE c.id <> k.survivor_id
    """))

    # 4) move non-colliding enrollments loser -> survivor
    db.session.execute(text("""
        UPDATE user_enrollment
        SET user_id = (
          SELECT survivor_id FROM tmp_losers
          WHERE loser_id = user_enrollment.user_id
        )
        WHERE user_id IN (SELECT loser_id FROM tmp_losers)
          AND NOT EXISTS (
            SELECT 1 FROM user_enrollment ue2
            WHERE ue2.user_id = (
                SELECT survivor_id FROM tmp_losers
                WHERE loser_id = user_enrollment.user_id
            )
            AND ue2.subject_id = user_enrollment.subject_id
          )
    """))

    # 5) delete duplicate enrollments remaining on losers
    db.session.execute(text("""
        DELETE FROM user_enrollment
        WHERE user_id IN (SELECT loser_id FROM tmp_losers)
          AND EXISTS (
            SELECT 1 FROM user_enrollment ue2
            WHERE ue2.user_id = (
                SELECT survivor_id FROM tmp_losers
                WHERE loser_id = user_enrollment.user_id
            )
            AND ue2.subject_id = user_enrollment.subject_id
          )
    """))

    # 6) deactivate losers; ensure survivors active
    db.session.execute(text("""UPDATE "user" SET is_active = 0
                               WHERE id IN (SELECT loser_id FROM tmp_losers)"""))
    db.session.execute(text("""UPDATE "user" SET is_active = 1
                               WHERE id IN (SELECT survivor_id FROM tmp_keepers)"""))

    # 7) (optional) clean up
    db.session.execute(text("DROP TABLE IF EXISTS tmp_losers"))
    db.session.execute(text("DROP TABLE IF EXISTS tmp_keepers"))
    db.session.execute(text("DROP TABLE IF EXISTS tmp_cand"))

    db.session.commit()

# ---- imports (top of file) ----
from collections import defaultdict
from sqlalchemy import text as sa_text

# Canonical email SQL (keep yours as-is)

# --------------- REGISTER (GET/POST) ---------------
from werkzeug.security import check_password_hash, generate_password_hash

def hash_password(password):
    return generate_password_hash(password)

def _canon_email_py(e: str) -> str:
    e = (e or "").strip().lower()
    if "@" not in e:
        return e
    local, domain = e.split("@", 1)
    local = local.split("+", 1)[0]
    return f"{local}@{domain}"

EMAIL_CANON_SQL = """
CASE
  WHEN instr(lower(trim(u.email)),'@') > 0 THEN
    CASE
      WHEN instr(substr(lower(trim(u.email)),1,instr(lower(trim(u.email)),'@')-1), '+') > 0
        THEN substr(lower(trim(u.email)),1, instr(lower(trim(u.email)),'+')-1)
      ELSE substr(lower(trim(u.email)),1, instr(lower(trim(u.email)),'@')-1)
    END || '@' || substr(lower(trim(u.email)), instr(lower(trim(u.email)),'@')+1)
  ELSE lower(trim(u.email))
END
"""

# routes.py (snippet) — polished /register only

from flask import render_template, request, redirect, url_for, session
from sqlalchemy import text as sa_text
from werkzeug.security import generate_password_hash

@auth_bp.route("/start-payment", methods=["GET", "POST"])
def start_payment():
    """
    Create/ensure user + pending enrollment, resolve price (form or DB),
    then hand off to PayFast (via checkout_bp.payfast_handoff).
    """
    from flask import session as flask_session, request, redirect, url_for, flash, current_app
    from werkzeug.security import generate_password_hash
    from sqlalchemy import text as sa_text
   
    from decimal import Decimal, InvalidOperation
    import secrets

    v = request.values

    # ---------- Inputs ----------
    email_in = (v.get("email") or "").strip().lower()
    if email_in:
        email = email_in
        flask_session["email"] = email
    else:
        email = (flask_session.get("email") or "").strip().lower()

    subject_slug = (v.get("subject") or flask_session.get("subject") or "").strip().lower()
    role         = (v.get("role") or flask_session.get("role") or "user").strip().lower()
    next_url     = (v.get("next")  or flask_session.get("next_url") or "/").strip()

    current_app.logger.info("start_payment[PF]: email=%r subject=%r role=%r", email, subject_slug, role)

    if not email or not subject_slug:
        flash("Missing email or subject.", "warning")
        return redirect(url_for("checkout_bp.checkout", subject=subject_slug or "", role=role))

    # ---------- Payment data (form overrides) ----------
    price_id     = v.get("price_id")
    currency_cfg = (v.get("currency") or current_app.config.get("STRIPE_CURRENCY") or "ZAR").upper()
    try:
        qty = max(1, int(v.get("quantity", "1") or 1))
    except Exception:
        qty = 1

    amount_raw = (v.get("amount") or "").strip()
    amount_cents = int(amount_raw) if (amount_raw and amount_raw.isdigit()) else None

    # ---------- Get or create user ----------
    uid = db.session.execute(
        sa_text('SELECT id FROM "user" WHERE lower(email)=:e LIMIT 1'),
        {"e": email},
    ).scalar()

    if not uid:
        fallback_name = email.split("@")[0] if "@" in email else email
        # Try staged password hash from registration flow; fall back to reset-required sentinel
        reg_ctx = flask_session.get("reg_ctx", {}) or session.get("reg_ctx", {}) or {}
        staged_hash = reg_ctx.get("password_hash") or generate_password_hash("PLEASE_RESET_PASSWORD")

        db.session.execute(
            sa_text("""
                INSERT INTO "user" (name, email, password_hash, is_active)
                SELECT :n, :e, :p, 1
                WHERE NOT EXISTS (
                    SELECT 1 FROM "user" u
                    WHERE lower(u.email) = lower(:e)
                )
            """),
            {"n": fallback_name, "e": email, "p": staged_hash},
        )
        db.session.commit()

        uid = db.session.execute(
            sa_text('SELECT id FROM "user" WHERE lower(email)=:e LIMIT 1'),
            {"e": email},
        ).scalar()

    uid = int(uid)

    # ---------- Resolve subject ----------
    sid = db.session.execute(
        sa_text("""
            SELECT id FROM auth_subject
            WHERE lower(slug)=:s OR lower(name)=:s
            LIMIT 1
        """),
        {"s": subject_slug},
    ).scalar()
    if not sid:
        flash("Unknown subject.", "warning")
        return redirect(url_for("checkout_bp.checkout", subject=subject_slug, role=role))
    sid = int(sid)

    # ---------- Ensure pending enrollment ----------
    db.session.execute(
        sa_text("""
            INSERT INTO user_enrollment (user_id, subject_id, status)
            VALUES (:uid, :sid, 'pending')
            ON CONFLICT(user_id, subject_id)
            DO UPDATE SET status='pending'
        """),
        {"uid": uid, "sid": sid},
    )
    db.session.commit()

    # ---------- Resolve pricing (form wins; else DB) ----------
    if not price_id and (not isinstance(amount_cents, int) or amount_cents <= 0):
        # Import here to avoid circulars if needed
        #from app.auth.pricing import fetch_subject_price
        amt_cents, db_cur = fetch_subject_price(subject_slug, role)
        if amt_cents:
            amount_cents = int(amt_cents)
            currency_cfg = (db_cur or currency_cfg or "ZAR").upper()

    if not isinstance(amount_cents, int) or amount_cents <= 0:
        flash("Payment configuration missing.", "warning")
        return redirect(url_for("checkout_bp.checkout", subject=subject_slug, role=role))

    # Multiply by quantity; convert cents -> rands with 2 decimals
    try:
        total_cents = amount_cents * qty
        amount_rands = f"{(Decimal(total_cents) / Decimal(100)).quantize(Decimal('0.01'))}"
    except (InvalidOperation, Exception):
        flash("Invalid amount.", "warning")
        return redirect(url_for("checkout_bp.checkout", subject=subject_slug, role=role))

    # ---------- Build PayFast payload (handoff) ----------
    item_name    = f"{subject_slug.title()} enrollment"
    buyer_email  = email
    m_payment_id = f"AIT-{secrets.token_hex(6)}"  # your internal reference

    # Stash context if you need to use it after return_url
    flask_session["pending_email"]   = email
    flask_session["pending_subject"] = subject_slug
    flask_session["next_url"]        = next_url
    flask_session["pending_amount"]  = str(amount_cents)
    flask_session["pending_qty"]     = str(qty)

    # Hand off to PayFast handoff page (auto-posts to payfast_bp.create_payment)

    return redirect(url_for(
        "checkout_bp.payfast_handoff",   # <-- use THIS endpoint name
        amount=amount_rands,             # or your existing amount string
        item_name=f"{subject_slug.title()} enrollment",
        buyer_email=email,
        m_payment_id=f"AIT-{secrets.token_hex(6)}",
        currency=currency_cfg,
        quantity=str(qty),
    ), code=303)

@auth_bp.route("/register/decision", methods=["GET", "POST"])
def register_decision():
    """
    One-subject decision (single source of truth = user_enrollment):
      - active   -> Start course (POST)
      - pending  -> Proceed to payment (GET link)
      - none     -> Proceed to payment (GET link)
    """
    ctx = session.get("reg_ctx") or {}

    # -------- identity --------
    email = (
        (request.args.get("email") or request.form.get("email")
         or session.get("pending_email") or ctx.get("email") or "")
        .strip().lower()
    )
    if not email:
        flash("Please complete registration first.", "warning")
        return redirect(url_for("auth_bp.register"))

    # -------- chosen subject (slug or name) --------
    chosen_subject = (
        request.args.get("subject") or request.form.get("subject")
        or (ctx.get("subject") or "")
    ).strip().lower()
    if not chosen_subject:
        flash("Please choose a subject.", "warning")
        return redirect(url_for("auth_bp.register"))

    # -------- resolve subject (must be active) --------
    subj = db.session.execute(sa_text("""
        SELECT id, slug, name
        FROM auth_subject
        WHERE is_active = 1
          AND (lower(slug)=:s OR lower(name)=:s)
        LIMIT 1
    """), {"s": chosen_subject}).mappings().first()
    if not subj:
        flash("Unknown or inactive subject.", "warning")
        return redirect(url_for("auth_bp.register"))

    sid           = int(subj["id"])
    subject_slug  = (subj["slug"] or "").lower()
    subject_name  = subj["name"]

    # -------- current enrollment state for THIS subject --------
    row = db.session.execute(sa_text("""
        SELECT ue.status
        FROM user_enrollment ue
        JOIN "user" u       ON u.id = ue.user_id
        JOIN auth_subject s ON s.id = ue.subject_id
        WHERE lower(u.email)=lower(:e)
          AND lower(s.slug)=:slug
        LIMIT 1
    """), {"e": email, "slug": subject_slug}).first()

    status      = (row[0] if row else None)  # 'active' | 'pending' | None
    is_active   = (status == "active")
    is_pending  = (status == "pending")

    # -------- CTA wiring --------
    # If ACTIVE: we render a POST button (no href). If not, we render a link to /start-payment.
    cta_post   = bool(is_active)  # template: render <form method=POST> when True
    cta_label  = "Start course" if is_active else "Proceed to payment"
    cta_url    = None if is_active else url_for(
        "auth_bp.start_payment",
        email=email,
        subject=subject_slug,
        role=(ctx.get("role") or "user")
    )

    # Card only for this subject
    subjects = [{
        "id": sid,
        "name": subject_name,
        "enrolled": 1 if is_active else 0,
        "pending":  1 if is_pending else 0,
    }]

    # -------- POST: Start course only when ACTIVE --------
    if request.method == "POST":
        if not is_active:
            # Safety: if someone posts when not active, send them to payment.
            return redirect(cta_url or url_for("auth_bp.register"))

        # Login + bounce to Bridge
        uid = db.session.execute(sa_text(
            'SELECT MIN(id) FROM "user" WHERE lower(email)=lower(:e)'
        ), {"e": email}).scalar()

        if uid:
            try:
                from flask_login import login_user
                u = db.session.get(User, int(uid))
                if u:
                    login_user(u, remember=True, fresh=True)
                    session.update({
                        "user_id": int(u.id),
                        "user_name": u.name or (u.email.split("@", 1)[0] if u.email else ""),
                        "role": "user",
                        "subject": subject_slug,
                        "email": (u.email or "").lower(),
                    })
                    session.permanent = True
            except Exception:
                session.update({
                    "user_id": int(uid),
                    "user_name": email.split("@", 1)[0],
                    "role": "user",
                    "subject": subject_slug,
                    "email": email,
                })
                session.permanent = True

        return redirect(url_for("auth_bp.bridge_dashboard", role="user"))

    # -------- GET: render --------
    return render_template(
        "auth/decision.html",
        email=email,
        subjects=subjects,
        subject_slug=subject_slug,
        subject_display=subject_name,
        cta_url=cta_url,        # used only when not active
        cta_label=cta_label,
        cta_post=cta_post,      # used when active
    )



def _save_reg_ctx(role, subject, email, full_name, next_url):
    """
    Store in-progress registration data in session so later
    steps (register_decision, payment, finalization) can read it.
    We're going to extend this to include password_hash safely.
    """
    session["reg_ctx"] = {
        "role": role,
        "subject": subject,
        "email": email,
        "full_name": full_name,
        "next_url": next_url,
    }


@auth_bp.route("/register", methods=["GET", "POST"])
def register():
    # ---------- GET ----------
    if request.method == "GET":
        role     = (request.args.get("role") or "user").strip().lower()
        subject  = (request.args.get("subject") or "loss").strip().lower()
        next_url = (request.args.get("next") or "/").strip()

        _save_reg_ctx(role, subject, "", "", next_url)

        return render_template(
            "auth/register.html",
            role=role,
            subject=subject,
            next_url=next_url,
            values={}
        )

    # ---------- POST ----------
    role       = (request.form.get("role") or "user").strip().lower()
    subject    = (request.form.get("subject") or "loss").strip().lower()
    next_url   = (request.form.get("next")
                  or session.get("reg_ctx", {}).get("next_url")
                  or "/").strip()
    email_in   = (request.form.get("email") or "").strip()
    full_name  = (request.form.get("full_name") or "").strip()
    password   = (request.form.get("password") or "").strip()

    values = {
        "email": email_in,
        "full_name": full_name,
    }

    # basic validation
    if not email_in or not password:
        flash("Please provide an email and password.", "danger")
        return render_template(
            "auth/register.html",
            role=role,
            subject=subject,
            next_url=next_url,
            values=values
        )

    if len(password) < 8:
        flash("Password must be at least 8 characters long.", "danger")
        return render_template(
            "auth/register.html",
            role=role,
            subject=subject,
            next_url=next_url,
            values=values
        )

    # confirm subject exists and get subject_id
    sid = db.session.execute(
        sa_text("SELECT id FROM auth_subject WHERE slug=:s OR name=:s LIMIT 1"),
        {"s": subject}
    ).scalar()
    if not sid:
        flash("Unknown subject.", "danger")
        return render_template(
            "auth/register.html",
            role=role,
            subject=subject,
            next_url=next_url,
            values=values
        )

    # normalize email
    email_norm = email_in.lower()

    # check if this email is already a real user in DB
    # we are *not* creating a user yet, but we must block duplicates now
    existing = db.session.execute(
        sa_text("SELECT id FROM \"user\" WHERE lower(email)=lower(:e) LIMIT 1"),
        {"e": email_norm}
    ).scalar()
    if existing:
        flash("That email is already registered. Please sign in.", "danger")
        return render_template(
            "auth/register.html",
            role=role,
            subject=subject,
            next_url=next_url,
            values=values
        )

    # --- CRITICAL PART ---
    # Instead of creating + logging in the user now,
    # we stage their info (including a secure hash of their chosen password)
    # into session["reg_ctx"]. Payment step will finish the account.
    #
    # We store password_hash, NOT the raw password.
    staged_password_hash = generate_password_hash(password)

    _save_reg_ctx(
        role=role,
        subject=subject,
        email=email_norm,
        full_name=full_name,
        next_url=next_url,
    )

    # extend reg_ctx with security fields we need for finalization
    # (this survives redirect to decision -> payment -> final create)
    session["reg_ctx"]["email_lower"] = email_norm
    session["reg_ctx"]["password_hash"] = staged_password_hash

    # you were already doing this:
    session.pop("just_paid_subject_id", None)

    # now go to decision screen (pay / choose plan / etc.)
    return redirect(url_for(
        "auth_bp.register_decision",
        email=email_in,
        subject=subject
    ))


def _finalize_user_after_payment():
    """
    Call this right after payment success, *before* redirecting to dashboard.
    Returns the created-or-existing user object.
    """

    reg_ctx = session.get("reg_ctx", {}) or {}

    email_norm   = (reg_ctx.get("email") or reg_ctx.get("email_lower") or "").strip().lower()
    full_name    = (reg_ctx.get("full_name") or "").strip()
    subject_slug = (reg_ctx.get("subject") or "loss").strip().lower()
    staged_hash  = reg_ctx.get("password_hash")  # <-- we set this in /register POST
    next_url     = reg_ctx.get("next_url") or "/"

    if not email_norm or not staged_hash:
        # This is catastrophic: we lost reg_ctx or never staged the password_hash.
        # We should log loudly and bail.
        current_app.logger.error("[finalize_user_after_payment] Missing email or password_hash in reg_ctx")
        raise RuntimeError("registration context incomplete")

    # Make sure subject exists and get ID
    sid = db.session.execute(
        sa_text("SELECT id FROM auth_subject WHERE slug=:s OR name=:s LIMIT 1"),
        {"s": subject_slug}
    ).scalar()
    if not sid:
        current_app.logger.error(f"[finalize_user_after_payment] Unknown subject {subject_slug!r}")
        raise RuntimeError("unknown subject")

    # Check if user already exists (rare but possible if they double-paid)
    user = User.query.filter_by(email=email_norm).first()

    if not user:
        # create new user using the staged password_hash from register()
        user = User(
            email=email_norm,
            name=full_name if full_name else email_norm.split("@", 1)[0],
        )

        user.password_hash = staged_hash

        # make sure any legacy .password column is blank
        if hasattr(user, "password"):
            try:
                setattr(user, "password", None)
            except Exception:
                pass

        db.session.add(user)
        db.session.flush()  # user.id now available

    # Enroll user in subject if not already
    try:
        db.session.execute(
            sa_text("""
                INSERT INTO user_enrollment (user_id, subject_id, status, started_at)
                VALUES (:uid, :sid, 'active', CURRENT_TIMESTAMP)
                ON CONFLICT (user_id, subject_id) DO NOTHING
            """),
            {"uid": user.id, "sid": sid}
        )
    except Exception as ex:
        current_app.logger.warning(f"[finalize_user_after_payment] couldn't insert enrollment: {ex!r}")

    db.session.commit()

    # Log them in and hydrate session, like login() does
    login_user(user, remember=True, fresh=True)

    # purge then repopulate session keys exactly the same shape as login() builds
    for k in (
        "is_authenticated", "email", "is_admin",
        "admin_subjects", "enrolled_subjects",
        "subjects_access", "user_id", "user_name",
        "just_paid_subject_id", "payment_banner",
        "pending_email", "pending_subject", "pending_session_ref",
        "role",
    ):
        session.pop(k, None)

    session["is_authenticated"] = True
    session["email"] = email_norm
    session["user_id"] = int(user.id)
    session["user_name"] = full_name if full_name else email_norm.split("@", 1)[0]

    # at signup they're just a learner
    session["is_admin"] = False
    session["admin_subjects"] = []

    # enrolled_subjects snapshot: just this subject for now
    session["enrolled_subjects"] = [subject_slug]
    session["subjects_access"] = {subject_slug: "enrolled"}

    session["role"] = "user"

    return user, next_url



def _compute_subject_start_url(slug: str) -> str:
    """
    Given a subject slug like "loss" or "reading",
    return the URL the first Start button should go to.

    This lets each subject own its own onboarding flow without
    hardcoding if/elif inside the view.
    """

    slug = (slug or "").strip().lower()

    # per-subject start endpoint priority:
    # - list of endpoint names we try in order
    # - first one that exists wins
    SUBJECT_START_ENDPOINTS = {
        # Loss: first go to the subject_home ("Getting ready for the assessment")
        "loss": [
            "loss_bp.subject_home",
        ],

        # Reading: try custom preflight first, fall back to subject_home
        "reading": [
            "reading_bp.preflight_reading",
            "reading_bp.subject_home",
        ],

        # you can add more subjects here later, eg:
        # "math": [
        #     "math_bp.subject_welcome",
        #     "math_bp.subject_home",
        # ],
    }

    # default generic guess if not in SUBJECT_START_ENDPOINTS:
    # we assume each subject has its own blueprint named "<slug>_bp"
    # and that blueprint exposes .subject_home
    if slug not in SUBJECT_START_ENDPOINTS:
        SUBJECT_START_ENDPOINTS[slug] = [f"{slug}_bp.subject_home"]

    # try each candidate endpoint until one resolves
    for endpoint_name in SUBJECT_START_ENDPOINTS[slug]:
        try:
            return url_for(endpoint_name)
        except BuildError as ex:
            current_app.logger.debug(
                f"_compute_subject_start_url: endpoint {endpoint_name} not available for {slug}: {ex}"
            )
            continue

    # final safe fallback
    return url_for("auth_bp.bridge_dashboard")

def fetch_subject_price(subject_slug: str, role: str = "user"):
    """
    Returns (amount_cents:int|None, currency:str|None) for the subject+role
    from auth_pricing. Role can be NULL (means any role).
    Picks the best row: exact role first, else NULL role; newest active_from wins.
    """
    slug = (subject_slug or "").strip().lower()
    r    = (role or "user").strip().lower()

    row = db.session.execute(sa_text("""
        SELECT p.amount_cents, p.currency
        FROM auth_pricing p
        JOIN auth_subject s ON s.id = p.subject_id
        WHERE lower(s.slug) = :slug
          AND p.plan = 'enrollment'
          AND COALESCE(p.is_active, 1) = 1
          AND (p.role IS NULL OR lower(p.role) = :role)
          AND (p.active_to IS NULL OR p.active_to > CURRENT_TIMESTAMP)
        ORDER BY
          CASE WHEN p.role IS NULL THEN 1 ELSE 0 END,   -- exact role first
          p.active_from DESC
        LIMIT 1
    """), {"slug": slug, "role": r}).first()

    if not row:
        return None, None

    amt = int(row[0]) if row[0] is not None else None
    cur = (row[1] or "ZAR").upper()
    return amt, cur