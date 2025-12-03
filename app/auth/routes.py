# app/auth/routes.py
from datetime import datetime, timezone
import datetime as dt
import email
from flask import g, session as flask_session
import re
import json
import uuid
from flask import (
    Blueprint, current_app, make_response, render_template,
    redirect, url_for, request, flash,
    session, jsonify, abort
    )
import stripe
from app.extensions import db, csrf
from werkzeug.security import check_password_hash
from flask_login import login_user, logout_user, login_required, current_user
from app.models import subject
from app.payments.pricing import price_cents_for, price_for_country, subject_id_for
from app.services import enrollment
from app.services.enrollment import _ensure_enrollment_row
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
from email import message
from sqlalchemy import func as SA_FUNC, text as SA_TEXT
from sqlalchemy.exc import IntegrityError
from flask import render_template, render_template_string
from jinja2 import TemplateNotFound
from sqlalchemy.exc import IntegrityError
from sqlalchemy import select, func, update as sa_update
from app.services.users import _ensure_or_create_user_from_session
from sqlalchemy.exc import IntegrityError
from werkzeug.security import generate_password_hash
import os

auth_bp = Blueprint('auth_bp', __name__, url_prefix='/', template_folder='templates')

@auth_bp.app_context_processor
def inject_has_endpoint():
    from flask import current_app
    def has_endpoint(ep: str) -> bool:
        return ep in current_app.view_functions
    return {"has_endpoint": has_endpoint}



@auth_bp.get("/start_registration")
def start_registration():
    """
    Entry point from pricing/enrol button.

    Example:
      /start_registration?role=user&subject=loss&next=/payments/checkout/review?subject%3Dloss

    This must land on the registration form – not 400 – for normal flows.
    """

    # 1) Get subject slug from query (?subject=loss) or reg_ctx, fallback 'loss'
    reg_ctx = session.get("reg_ctx") or {}
    subject_slug = (
        (request.args.get("subject") or "").strip().lower()
        or (reg_ctx.get("subject") or "").strip().lower()
        or "loss"
    )

    # 2) Look up subject row in auth_subject
    subject = AuthSubject.query.filter_by(slug=subject_slug).first()
    if not subject:
        flash("Enrollment for this course is not available right now.", "warning")
        return redirect(url_for("public_bp.welcome"))

    subject_id = subject.id

    # 3) Store subject info in reg_ctx
    ctx = session.setdefault("reg_ctx", {})
    ctx["subject"] = subject_slug
    ctx["subject_id"] = subject_id
    session.modified = True

    # 4) Work out the 'next' URL, but keep it internal-only
    raw_next = request.args.get("next") or ""
    next_url = ""
    if raw_next:
        p = urlparse(raw_next)
        if not p.scheme and not p.netloc:  # only internal
            next_url = p.path
            if p.query:
                next_url = f"{next_url}?{p.query}"

    if not next_url:
        next_url = url_for("payfast_bp.checkout_review",
                           subject_id=subject_id,
                           subject=subject_slug)

    # 5) Role (default "user")
    role = (request.args.get("role") or "user").strip().lower()

    # 6) Redirect to the real registration form
    return redirect(
        url_for("auth_bp.register",
                role=role,
                subject=subject_slug,
                next=next_url)
    )

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
            values={},
        )

    # ---------- POST ----------
    role       = (request.form.get("role") or "user").strip().lower()
    subject    = (request.form.get("subject") or "loss").strip().lower()
    next_url   = (
        request.form.get("next")
        or session.get("reg_ctx", {}).get("next_url")
        or "/"
    ).strip()
    email_in   = (request.form.get("email") or "").strip()
    full_name  = (request.form.get("full_name") or "").strip()
    password   = (request.form.get("password") or "").strip()

    values = {"email": email_in, "full_name": full_name}

    # basic validation
    if not email_in or not password:
        flash("Please provide an email and password.", "danger")
        return render_template(
            "auth/register.html",
            role=role,
            subject=subject,
            next_url=next_url,
            values=values,
        )

    if len(password) < 8:
        flash("Password must be at least 8 characters long.", "danger")
        return render_template(
            "auth/register.html",
            role=role,
            subject=subject,
            next_url=next_url,
            values=values,
        )

    # confirm subject exists -> sid
    sid = db.session.execute(
        sa_text("SELECT id FROM auth_subject WHERE slug = :s OR name = :s LIMIT 1"),
        {"s": subject},
    ).scalar()
    if not sid:
        flash("Unknown subject.", "danger")
        return render_template(
            "auth/register.html",
            role=role,
            subject=subject,
            next_url=next_url,
            values=values,
        )

    # normalize email & block duplicates
    email_norm = email_in.lower()
    existing = db.session.execute(
        sa_text('SELECT id FROM "user" WHERE lower(email) = lower(:e) LIMIT 1'),
        {"e": email_norm},
    ).scalar()
    if existing:
        flash("That email is already registered. Please sign in.", "danger")
        return render_template(
            "auth/register.html",
            role=role,
            subject=subject,
            next_url=next_url,
            values=values,
        )

    # stage user in session (no user row yet)
    staged_password_hash = generate_password_hash(password)

    _save_reg_ctx(
        role=role,
        subject=subject,
        email=email_norm,
        full_name=full_name,
        next_url=next_url,
    )
    session["reg_ctx"]["email_lower"]   = email_norm
    session["reg_ctx"]["password_hash"] = staged_password_hash

    # ---------- lock a DB-driven parity + ZAR quote into session ----------
    cc      = (request.form.get("country") or "ZA").strip().upper()
    subj_id = subject_id_for(subject)

    local_cents    = 0
    est_zar_cents  = 0
    cur            = "ZAR"

    if subj_id:
        # price_for_country → (local_cents, zar_cents, currency)
        local_cents, est_zar_cents, cur = price_for_country(subj_id, cc)

    if not est_zar_cents or est_zar_cents <= 0:
        est_zar_cents = 5000  # 50.00 ZAR safety net

    reg_ctx = session.get("reg_ctx", {})
    reg_ctx["quote"] = {
        "country_code":  cc,
        "currency":      cur,
        "amount_cents":  int(local_cents or 0),     # local parity price
        "est_zar_cents": int(est_zar_cents or 0),   # ZAR to bill
        "version":       "2025-11",
    }
    reg_ctx["bill_cents_zar"] = int(est_zar_cents)
    session["reg_ctx"] = reg_ctx
    session.modified = True

    session.pop("just_paid_subject_id", None)

    # go to decision screen
    return redirect(
        url_for("auth_bp.register_decision", email=email_in, subject=subject)
    )

@auth_bp.route("/register/decision", methods=["GET", "POST"])
@csrf.exempt
def register_decision():
    # 0) Resolve subject (no hardcoding)
    ctx = session.setdefault("reg_ctx", {})
    subject = (
        (request.values.get("subject") or "").strip().lower()
        or (ctx.get("subject") or "").strip().lower()
        or "loss"
    )

    # 1) Ensure/create the user from staged session data
    try:
        user_id = _ensure_or_create_user_from_session(ctx)
    except ValueError:
        flash("Your session expired. Please re-enter your details.", "warning")
        return redirect(url_for("auth_bp.register", subject=subject))

    # NEW: make sure this user is actually logged in
    from flask_login import current_user, login_user
    from app.models import User  # adjust if your User model is elsewhere

    if not getattr(current_user, "is_authenticated", False):
        user = db.session.get(User, user_id)  # or User.query.get(user_id)
        if user:
            login_user(user)


    # 2) Ensure an enrollment row
    enrollment_id = _ensure_enrollment_row(user_id=user_id, subject_slug=subject)

    # 3) Build a quote
    q = ctx.get("quote")

    # Special case: SMS uses a fixed ZAR price from auth_pricing
    if subject == "sms":
        #from app.subjects.utils import subject_id_for  # adjust import if needed

        sid = subject_id_for("sms")
        row = db.session.execute(
            db.text(
                """
                SELECT amount_cents, currency
                  FROM auth_pricing
                 WHERE subject_id = :sid
                   AND role = 'user'
                   AND plan = 'enrollment'
                   AND is_active = 1
                 ORDER BY active_from DESC
                 LIMIT 1
                """
            ),
            {"sid": sid},
        ).first()

        if row:
            amount_cents = int(row[0] or 0)
            currency = row[1] or "ZAR"
            q = {
                "country_code": "ZA",
                "currency": currency,
                "amount_cents": amount_cents,
                "version": "2025-11",
            }

    # Fallback for other subjects if there is still no quote (Loss etc.)
    if not q and subject != "sms":
        country = (
            (ctx.get("country_code") or "").strip().upper()
            or getattr(g, "country_iso2", "ZA")
            or "ZA"
        )
        try:
            from app.payments.pricing import price_cents_for

            amount_cents = price_cents_for(subject, country)
        except Exception:
            current_app.logger.exception(
                "pricing failed for subject=%s country=%s", subject, country
            )
            amount_cents = None

        if amount_cents and amount_cents > 0:
            q = {
                "country_code": country,
                "currency": ctx.get("quoted_currency") or "ZAR",
                "amount_cents": int(amount_cents),
                "version": "2025-11",
            }

    # If we still have no quote or 0 amount, bail out gracefully
    if not q or not int(q.get("amount_cents") or 0):
        flash(
            "Pricing is not configured for this course yet. Please contact us.",
            "danger",
        )
        return redirect(url_for("public_bp.welcome"))

    # 3b) Write quote to user_enrollment
    db.session.execute(
        db.text(
            """
            UPDATE user_enrollment
               SET country_code        = :cc,
                   quoted_currency     = :cur,
                   quoted_amount_cents = :amt,
                   price_version       = :ver,
                   price_locked_at     = CURRENT_TIMESTAMP
             WHERE id = :eid
            """
        ),
        {
            "cc": q.get("country_code"),
            "cur": q.get("currency") or "ZAR",
            "amt": int(q.get("amount_cents") or 0),
            "ver": q.get("version") or "2025-11",
            "eid": enrollment_id,
        },
    )
    db.session.commit()

    # 4) Read back persisted quote
    row = db.session.execute(
        db.text(
            """
            SELECT quoted_currency, quoted_amount_cents
              FROM user_enrollment
             WHERE id = :eid
            """
        ),
        {"eid": enrollment_id},
    ).first()

    quoted_currency = row[0] if row and row[0] else "ZAR"
    quoted_amount_cents = int(row[1] or 0) if row else 0

    if quoted_amount_cents <= 0:
        flash("Pricing could not be determined. Please try again or contact us.", "danger")
        return redirect(url_for("public_bp.welcome"))

    # 5) Decide where to send the user
    user_email = (
        (request.values.get("email") or "").strip().lower()
        or (ctx.get("email") or "").strip().lower()
    )
    if not user_email:
        flash("We couldn't confirm your email address. Please register again.", "warning")
        return redirect(url_for("auth_bp.register", subject=subject))

    # SMS: always skip PayFast, go straight to dashboard
    if subject == "sms":
        return redirect(url_for("auth_bp.bridge_dashboard"))

    # LOSS_FREE flag: allow Loss to bypass PayFast if configured
    if current_app.config.get("LOSS_FREE"):
        return redirect(url_for("auth_bp.bridge_dashboard"))

    # Normal flow: hand off to PayFast
    return redirect(
        url_for(
            "payfast_bp.handoff",
            email=user_email,
            subject=subject,
            debug=0,
        )
    )

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
    user_obj = None

    if not email and getattr(current_user, "is_authenticated", False):
        try:
            u = User.query.get(int(getattr(current_user, "id", 0)))
            if u and u.email:
                email = u.email.lower()
                session["email"] = email
            user_obj = u
        except Exception:
            pass
    if not email:
        qemail = (request.args.get("email") or "").strip().lower()
        if qemail:
            email = qemail
            session["email"] = email
    if not email:
        return redirect(url_for("auth_bp.login"))

    # If we didn't get user_obj above, resolve it by email
    if user_obj is None:
        user_obj = User.query.filter_by(email=email).first()

    # 🔹 NEW: build enrollment map for this user from user_enrollment
    enroll_map = {}
    if user_obj:
        rows_en = db.session.execute(
            text("""
                SELECT subject_id, status
                  FROM user_enrollment
                 WHERE user_id = :uid
            """),
            {"uid": user_obj.id},
        ).mappings().all()
        enroll_map = {r["subject_id"]: r["status"] for r in rows_en}

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
    return render_template(
        "auth/bridge_dashboard.html",
        subjects=rows,
        banner=banner,
        enroll_map=enroll_map,   # 🔹 pass enrollment info into template
    )

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

@auth_bp.get("/api/countries")
def api_countries():
    q = (request.args.get("q") or "").strip()
    return jsonify(search_countries(q, limit=20))

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


def _finalize_user_after_payment():
    """
    Call this right after payment success, *before* redirecting to dashboard.
    Returns the created-or-existing user object.
    """

    reg_ctx = session.get("reg_ctx", {}) or {}

    # 1) Inputs: add fallbacks; never raise
    email_norm   = (reg_ctx.get("email") or reg_ctx.get("email_lower")
                    or session.get("pending_email")
                    or request.args.get("email", "")
                ).strip().lower()
    full_name    = (reg_ctx.get("full_name") or "").strip()
    subject_slug = (reg_ctx.get("subject") or session.get("pending_subject") or "loss").strip().lower()
    staged_hash  = reg_ctx.get("password_hash") or generate_password_hash("PLEASE_RESET_PASSWORD")
    next_url     = reg_ctx.get("next_url") or "/"
    if not email_norm:
        current_app.logger.error("[finalize_user_after_payment] no email available after return")
        return None, next_url  # caller will show a gentle error

    # 2) Create-or-get user; only set password_hash if creating
    user = User.query.filter_by(email=email_norm).first()
    if not user:
        user = User(email=email_norm, name=(full_name or email_norm.split("@", 1)[0]))
        user.password_hash = staged_hash
        if hasattr(user, "password"):
            try: setattr(user, "password", None)
            except Exception: pass
        db.session.add(user); db.session.flush()

    # 3) Promote enrollment to active WITHOUT started_at column
    sid = db.session.execute(sa_text(
        "SELECT id FROM auth_subject WHERE slug=:s OR name=:s LIMIT 1"
    ), {"s": subject_slug}).scalar()
    if sid:
        db.session.execute(sa_text("""
            INSERT INTO user_enrollment (user_id, subject_id, status)
            VALUES (:uid, :sid, 'active')
            ON CONFLICT(user_id, subject_id) DO UPDATE SET status='active'
        """), {"uid": user.id, "sid": int(sid)})

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


def _resolve_subject_from_request() -> tuple[int, str]:
    """
    Resolve (subject_id, subject_slug) from:
    - ?subject / form["subject"]
    - ?subject_id / form["subject_id"]
    - reg_ctx["subject"]
    """
    reg_ctx = session.get("reg_ctx") or {}

    slug = (
        (request.values.get("subject") or request.args.get("subject") or "") or
        (reg_ctx.get("subject") or "")
    ).strip().lower()

    sid = request.values.get("subject_id", type=int) or request.args.get("subject_id", type=int)

    # slug only → look up id
    if slug and not sid:
        row = db.session.execute(
            sa_text("SELECT id FROM auth_subject WHERE slug = :s"),
            {"s": slug},
        ).first()
        if row:
            sid = int(row.id)

    # id only → look up slug
    if sid and not slug:
        row = db.session.execute(
            sa_text("SELECT slug FROM auth_subject WHERE id = :sid"),
            {"sid": sid},
        ).first()
        if row:
            slug = row.slug

    if not sid or not slug:
        abort(400, "Missing subject")

    # keep it in reg_ctx for later steps
    reg_ctx["subject"] = slug
    session["reg_ctx"] = reg_ctx

    return sid, slug

