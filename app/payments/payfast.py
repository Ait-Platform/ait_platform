# app/payments/payfast.py
from uuid import uuid4
from flask import (
    Blueprint, current_app, flash, request, render_template_string, 
    abort,render_template, redirect, session, url_for, jsonify, g
    )
from urllib.parse import urlencode
from flask_login import current_user, login_user
import os, hmac, hashlib, ipaddress, requests
from urllib.parse import urlencode, quote_plus
import hashlib
import re
from app.auth.routes import _resolve_subject_from_request
from app.models.auth import AuthSubject
from decimal import Decimal, ROUND_HALF_UP
# add these
from app.models.auth import AuthSubject     # gives you the SQLAlchemy session
from sqlalchemy import text                # enables raw SQL text() queries
from app.extensions import db
#from flask_wtf.csrf import csrf_exempt
from decimal import Decimal
from sqlalchemy import text
from app.models.auth import User, AuthSubject, UserEnrollment # adjust to your names
from app.extensions import csrf
import hashlib, re, logging
from sqlalchemy import text as sa_text
from werkzeug.security import generate_password_hash
#from app.auth.routes import _finalize_user_after_payment
from app.payments.pricing import (
    apply_percentage_discount, countries_from_ref, 
    countries_from_ref_with_names, currency_for_country_code, 
    get_parity_anchor_cents, lock_country_and_price, price_cents_for, 
    price_for_country, subject_id_for
    )
from app.utils.country_list import COUNTRIES, _name_code_iter

payfast_bp = Blueprint("payfast_bp", __name__)

#PAYFAST_PROCESS_URL = "https://www.payfast.co.za/eng/process"
#PAYFAST_VALIDATE_URL = "https://www.payfast.co.za/eng/query/validate"  # server-to-server

# app/payments/payfast.py
PAYFAST_PROCESS_URL  = "https://sandbox.payfast.co.za/eng/process"
PAYFAST_VALIDATE_URL = "https://sandbox.payfast.co.za/eng/query/validate"


# PayFast publishes source IPs; keep this list current occasionally.
PAYFAST_SOURCE_NETS = [
    "154.66.197.0/24",    # payfast range
    "154.72.56.0/21",
    "196.7.0.0/16",
]

def cfg(key, default=""):
    return os.getenv(key, default)

def ip_in_trusted_range(ip: str) -> bool:
    try:
        ip_obj = ipaddress.ip_address(ip)
        for net in PAYFAST_SOURCE_NETS:
            if ip_obj in ipaddress.ip_network(net):
                return True
    except Exception:
        pass
    return False

def pf_md5(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()

@payfast_bp.post("/create")
def create_payment():
    """Create a PayFast payment and auto-submit to PayFast."""
    # In a real flow, you’ll POST amount & item details from your checkout form
    amount = request.form.get("amount", "50.00")
    item_name = request.form.get("item_name", "AIT Subject")
    buyer_email = request.form.get("email", "payer@example.com")
    m_payment_id = request.form.get("m_payment_id", "AIT-" + hashlib.md5(os.urandom(8)).hexdigest()[:8])

    params = {
        "merchant_id":  cfg("PAYFAST_MERCHANT_ID"),
        "merchant_key": cfg("PAYFAST_MERCHANT_KEY"),
        "return_url":   cfg("PAYFAST_RETURN_URL"),
        "cancel_url":   cfg("PAYFAST_CANCEL_URL"),
        "notify_url":   cfg("PAYFAST_NOTIFY_URL"),

        "m_payment_id": m_payment_id,        # your internal reference
        "amount":       amount,              # string with 2 decimals
        "item_name":    item_name,
        "email_address": buyer_email,
    }

    params["signature"] = pf_signature(params, cfg("PAYFAST_PASSPHRASE"))

    # Minimal auto-post page (keeps you on best-practice server-signed pattern)
    html = """
    <html><body onload="document.forms[0].submit()">
      <p>Redirecting to PayFast…</p>
      <form method="post" action="{{ process_url }}">
        {% for k, v in params.items() %}
          <input type="hidden" name="{{ k|e }}" value="{{ v|e }}">
        {% endfor %}
        <noscript><button type="submit">Continue</button></noscript>
      </form>
    </body></html>
    """
    return render_template_string(html, params=params, process_url=PAYFAST_PROCESS_URL)

@payfast_bp.get("/test-button")
def test_button():
    # Simple page with a single Pay button for quick manual tests
    html = """
    <!doctype html><html><body style="font-family:system-ui;padding:2rem">
      <h1>Pay R50.00</h1>
      <form method="post" action="{{ url_for('payfast_bp.create_payment') }}">
        <input type="hidden" name="amount" value="50.00">
        <input type="hidden" name="item_name" value="AIT Subject: Reading">
        <input type="email" name="email" value="payer@example.com" required>
        <button type="submit">Pay with PayFast</button>
      </form>
    </body></html>
    """
    return render_template_string(html)

@payfast_bp.get("/ping")
def ping():
    return "payfast ok", 200

def pf_signature(params: dict, passphrase: str) -> str:
    filtered = {k: v for k, v in params.items() if v not in (None, "") and k != "signature"}
    qs = urlencode(sorted(filtered.items()), doseq=True)
    if passphrase:
        qs = f"{qs}&passphrase={quote_plus(passphrase)}"  # <-- encode
    return hashlib.md5(qs.encode("utf-8")).hexdigest()

# temporary debug helper
@payfast_bp.get("/ipn")
def ipn_get_debug():
    return "ipn-get-ok", 200

# in your payments blueprint
# --- GET probes (PayFast / browser checks)
# GET probe (health)
@payfast_bp.get("/notify", endpoint="payfast_notify_probe")
def pf_notify_probe():
    return "OK", 200


# POST IPN (authoritative) — always 200, never raise
@payfast_bp.post("/notify", endpoint="payfast_notify")
@csrf.exempt
def pf_notify_ipn():
    try:
        cfg  = current_app.config
        mode = (cfg.get("PAYFAST_MODE") or "sandbox").lower()

        # 1) Signature (only if passphrase set; sandbox is usually empty)
        passphrase = "" if mode == "sandbox" else (cfg.get("PAYFAST_PASSPHRASE") or "")
        if passphrase:
            data = request.form.to_dict(flat=True)
            expected = _pf_sig(data, passphrase)  # same helper used for handoff
            got = data.get("signature", "")
            if got != expected:
                current_app.logger.warning("PF IPN: bad signature (got=%s expected=%s)", got, expected)
                return "OK", 200  # still 200 to stop retries

        # 2) Remote validation with PayFast
        validate_url = (
            "https://sandbox.payfast.co.za/eng/query/validate"
            if mode == "sandbox" else
            "https://www.payfast.co.za/eng/query/validate"
        )
        r = requests.post(validate_url, data=request.form, timeout=8)
        if r.text.strip().lower() != "valid":
            current_app.logger.warning("PF IPN: validate failed → %s", r.text)
            return "OK", 200

        # 3) Business logic (minimal + idempotent)
        status = (request.form.get("payment_status") or "").upper()
        mref   = (request.form.get("m_payment_id") or "").strip()
        email  = (request.form.get("email_address") or "").strip().lower()

        # log row (append-only)
        db.session.execute(
            text("""INSERT INTO auth_payment_log (user_id, program, amount, timestamp)
                    VALUES ((SELECT id FROM "user" WHERE lower(email)=:em LIMIT 1),
                            :prog, :amt, CURRENT_TIMESTAMP)"""),
            {"em": email, "prog": mref or "payfast", "amt": request.form.get("amount","")}
        )

        # activate access on COMPLETE (subject inferred from ref prefix)
        # activate access on COMPLETE (subject inferred from ref prefix)
        if status == "COMPLETE" and "-" in mref:
            slug = mref.split("-", 1)[0]
            subj = AuthSubject.query.filter_by(slug=slug).first()
            if subj:
                # 1) Ensure the user exists
                user_id = db.session.execute(
                    text('SELECT id FROM "user" WHERE lower(email) = :em'),
                    {"em": email.lower()}
                ).scalar()

                if not user_id:
                    current_app.logger.warning("PF IPN: no user found for email=%s", email)
                    return "OK"

                # 2) First try to update an existing enrollment
                db.session.execute(
                    text("""
                        UPDATE user_enrollment
                        SET status    = 'active',
                            started_at = COALESCE(started_at, CURRENT_TIMESTAMP)
                        WHERE user_id   = :uid
                          AND subject_id = :sid
                    """),
                    {"uid": user_id, "sid": subj.id}
                )

                # 3) If nothing was updated, insert a new row
                db.session.execute(
                    text("""
                        INSERT INTO user_enrollment (user_id, subject_id, status, started_at)
                        SELECT :uid, :sid, 'active', CURRENT_TIMESTAMP
                        WHERE NOT EXISTS (
                            SELECT 1 FROM user_enrollment ue
                            WHERE ue.user_id   = :uid
                              AND ue.subject_id = :sid
                        )
                    """),
                    {"uid": user_id, "sid": subj.id}
                )

        db.session.commit()
        current_app.logger.info("PF IPN ok: ref=%s status=%s email=%s", mref, status, email)
        return "OK", 200

    except Exception:
        current_app.logger.exception("PF IPN handler error")
        db.session.rollback()
        return "OK", 200


# Optional: success/cancel placeholders so they don’t 500

def _cfg(name: str) -> str:
    val = current_app.config.get(name)
    if not val:
        abort(500, f"PayFast misconfiguration: {name} not set")
    return val

log = logging.getLogger(__name__)

def _pf_host(cfg) -> str:
    mode = (cfg.get("PAYFAST_MODE") or "sandbox").lower()
    return "https://www.payfast.co.za/eng/process" if mode == "live" \
           else "https://sandbox.payfast.co.za/eng/process"

def _ref_part(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]", "-", s)[:60]

def _pf_sig(data: dict, passphrase: str | None) -> str:
    clean = {k: str(v).strip() for k, v in data.items() if v not in (None, "",) and k != "signature"}
    parts = [f"{k}={quote_plus(clean[k])}" for k in sorted(clean)]
    if passphrase:
        parts.append(f"passphrase={quote_plus(passphrase)}")
    sig_base = "&".join(parts)
    sig = hashlib.md5(sig_base.encode("utf-8")).hexdigest()
    current_app.logger.info("PF sigbase=%s md5=%s", sig_base, sig)
    return sig

def _pf_sig_verify(payload: dict, passphrase: str) -> bool:
    data = {k: v for k, v in payload.items() if k != "signature" and v not in (None, "")}
    parts = [f"{k}={quote_plus(str(data[k]))}" for k in sorted(data)]
    if passphrase:
        parts.append(f"passphrase={quote_plus(passphrase)}")
    base = "&".join(parts)
    return hashlib.md5(base.encode("utf-8")).hexdigest() == payload.get("signature", "")

@payfast_bp.get("/_pf-config-ok")
def _pf_config_ok():
    cfg = current_app.config
    keys = ["PAYFAST_MODE","PAYFAST_MERCHANT_ID","PAYFAST_MERCHANT_KEY",
            "PAYFAST_PASSPHRASE","PAYFAST_RETURN_URL","PAYFAST_CANCEL_URL","PAYFAST_NOTIFY_URL"]
    return {
        k: bool(cfg.get(k)) if k != "PAYFAST_MODE" else cfg.get(k)
        for k in keys
    }, 200

def _ref(s: str) -> str:
    # safe, short token for m_payment_id
    return re.sub(r"[^A-Za-z0-9._-]", "-", s)[:40]

@payfast_bp.route("/handoff", methods=["GET", "POST"])
def handoff():
    cfg = current_app.config

    # Inputs (POST first, then GET fallback)
    email = (request.form.get("email") or request.args.get("email") or "").strip().lower()
    slug  = (request.form.get("subject") or request.args.get("subject") or "").strip().lower()
    debug = str(request.values.get("debug") or "0") == "1"

    if not email or not slug:
        abort(400, "Missing email or subject")

    # Config sanity
    required = [
        "PAYFAST_MERCHANT_ID",
        "PAYFAST_MERCHANT_KEY",
        "PAYFAST_RETURN_URL",
        "PAYFAST_CANCEL_URL",
        "PAYFAST_NOTIFY_URL",
    ]
    missing = [k for k in required if not cfg.get(k)]
    if missing:
        return render_template("payments/payfast_misconfig.html", missing=missing), 500

    # Subject
    subject = AuthSubject.query.filter_by(slug=slug).first()
    if not subject:
        abort(400, "Subject not found")
    subject_id   = subject.id
    subject_name = getattr(subject, "name", slug)

    # --- Amount (ZAR) --------------------------------------------------------
    # Prefer session parity lock (pp_value). Fallback to auth_pricing ZAR.
    amt_str = None
    if session.get("pp_value"):
        try:
            amt_str = f"{float(session['pp_value']):.2f}"
        except Exception:
            amt_str = None

    if not amt_str:
        row = db.session.execute(
            db.text("""
                SELECT amount_cents
                FROM auth_pricing
                WHERE subject_id = :sid
                  AND (role = 'learner' OR role IS NULL)
                  AND plan = 'enrollment'
                  AND currency = 'ZAR'
                  AND (is_active = 1 OR is_active = TRUE)
                  AND (active_to IS NULL OR active_to > CURRENT_TIMESTAMP)
                ORDER BY COALESCE(updated_at, created_at) DESC,
                         COALESCE(active_from, created_at) DESC,
                         id DESC
                LIMIT 1
            """),
            {"sid": subject_id},
        ).first()
        if not row or row.amount_cents is None:
            abort(400, "Active price not configured for this subject")
        amt_str = f"{(Decimal(int(row.amount_cents)) / Decimal(100)).quantize(Decimal('0.01')):.2f}"
        # also hydrate session so downstream UI shows the same number
        try:
            session["pp_value"] = float(amt_str)
        except Exception:
            pass

    # Ensure display fields exist (currency/country) for logging/audit
    if not session.get("pp_currency") or not session.get("pp_country"):
        cc = (request.headers.get("CF-IPCountry") or "ZA").strip().upper()
        cur, cents, _ = price_for_country(subject_id, cc)
        session.setdefault("pp_currency", cur)
        session.setdefault("pp_country", cc)

    # URL policy checks (HTTPS outside localhost)
    for k in ("PAYFAST_RETURN_URL", "PAYFAST_CANCEL_URL", "PAYFAST_NOTIFY_URL"):
        val = str(cfg.get(k) or "").strip()
        if not val:
            abort(500, f"{k} missing")
        if val.startswith("http://") and ("localhost" not in val and "127.0.0.1" not in val):
            abort(500, f"{k} must be https outside localhost")

    # Mode / credentials
    mode = (cfg.get("PAYFAST_MODE") or "sandbox").lower()

    if mode == "sandbox":
        # Use your sandbox credentials from env/config,
        # fall back to the generic test merchant only if nothing is set.
        merchant_id  = cfg.get("PAYFAST_MERCHANT_ID")  or "10000100"
        merchant_key = cfg.get("PAYFAST_MERCHANT_KEY") or "46f0cd694581a"
        passphrase   = (cfg.get("PAYFAST_PASSPHRASE") or "")
        payfast_host = "https://sandbox.payfast.co.za/eng/process"

        current_app.logger.info(
            "PayFast sandbox mode: using merchant_id=%s, merchant_key=%s",
            merchant_id,
            merchant_key,
        )

    else:
        merchant_id  = cfg.get("PAYFAST_MERCHANT_ID")
        merchant_key = cfg.get("PAYFAST_MERCHANT_KEY")
        passphrase   = (cfg.get("PAYFAST_PASSPHRASE") or "")
        payfast_host = "https://www.payfast.co.za/eng/process"


    # Ref + return URL (include subject/email so /payments/success can finalize)
    mref = f"{_ref(slug)}-{uuid4().hex[:10]}"
    return_url = f"{cfg['PAYFAST_RETURN_URL']}?ref={mref}&email={email}&subject={slug}"

    # Ensure user exists + name + staged password hash if present
    u = User.query.filter_by(email=email).first()
    if not u:
        u = User(email=email, is_active=1)
    if not (u.name or "").strip():
        u.name = email.split("@", 1)[0].replace(".", " ").replace("_", " ").title()
    reg_ctx = session.get("reg_ctx") or {}
    staged = reg_ctx.get("password_hash")
    if staged and not getattr(u, "password_hash", None):
        u.password_hash = staged
    db.session.add(u)
    db.session.flush()  # ensure u.id

    # Ensure pending enrollment (upsert)
    # Ensure pending enrollment (manual upsert; PG table uses id PK, not (user_id, subject_id))
    existing = db.session.execute(
        db.text("""
            SELECT id, status
              FROM user_enrollment
             WHERE user_id = :uid
               AND subject_id = :sid
             LIMIT 1
        """),
        {"uid": u.id, "sid": subject.id},
    ).first()

    if existing:
        # Just mark as pending
        db.session.execute(
            db.text("""
                UPDATE user_enrollment
                   SET status = 'pending'
                 WHERE id = :eid
            """),
            {"eid": existing.id},
        )
    else:
        # Insert a fresh row
        db.session.execute(
            db.text("""
                INSERT INTO user_enrollment (user_id, subject_id, status)
                VALUES (:uid, :sid, 'pending')
            """),
            {"uid": u.id, "sid": subject.id},
        )

    db.session.commit()


    # PayFast payload (amount in ZAR, description carries parity UI context)
    pf_data = {
        "merchant_id":      merchant_id,
        "merchant_key":     merchant_key,
        "return_url":       return_url,
        "cancel_url":       cfg["PAYFAST_CANCEL_URL"],
        "notify_url":       cfg["PAYFAST_NOTIFY_URL"],
        "m_payment_id":     mref,
        "amount":           amt_str,
        "item_name":        (f"{subject_name} enrollment")[:100],
        "item_description": f"Parity UI {session.get('pp_value')} {session.get('pp_currency')} ({session.get('pp_country')})",
        "email_address":    email,
    }

    # Signature (not required in sandbox merchant_id 10000100)
    if merchant_id != "10000100":
        pf_data["signature"] = _pf_sig(pf_data, passphrase)
    else:
        pf_data.pop("signature", None)

    current_app.logger.info(
        "PF handoff host=%s data=%s",
        payfast_host,
        {k: v for k, v in pf_data.items() if k != "signature"},
    )

    # --- DEBUG MODE: show JSON instead of auto-posting to PayFast ---
    if debug:
        return render_template(
            "payments/payfast_handoff_debug.html",
            payfast_url=payfast_host,
            pf_data=pf_data,
        )

    # Normal mode: auto-post form → PayFast
    return render_template(
        "payments/payfast_handoff.html",
        payfast_url=payfast_host,
        pf_data=pf_data,
    )

@payfast_bp.get("/_sanity")
def pf_sanity():
    # Must be HTTPS and reachable publicly
    ret = current_app.config["PAYFAST_RETURN_URL"]
    can = current_app.config["PAYFAST_CANCEL_URL"]
    noti = current_app.config["PAYFAST_NOTIFY_URL"]

    # Minimal ASCII payload; NO signature in sandbox
    pf_data = {
        "merchant_id":   "10000100",
        "merchant_key":  "46f0cd694581a",
        "return_url":    ret.strip(),
        "cancel_url":    can.strip(),
        "notify_url":    noti.strip(),
        "m_payment_id":  "sanity-12345",
        "amount":        "50.00",
        "item_name":     "sanity enrollment",
        "email_address": "test@example.com",
    }
    return render_template("payments/payfast_handoff.html",
                           payfast_url="https://sandbox.payfast.co.za/eng/process",
                           pf_data=pf_data)

def _fmt_amount(value) -> str:
    return f'{Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP):.2f}'

def _subject_amount(subject, req_amount: str | None):
    """
    Return Decimal price from the subject model or request override.
    Supports major-unit fields and *_cents fields.
    """
    # Likely major-unit candidates
    for f in ("price", "amount", "fee", "price_zar"):
        v = getattr(subject, f, None)
        if v is not None:
            return Decimal(str(v))

    # Likely cents-based fields
    for f in ("price_cents", "amount_cents", "fee_cents"):
        v = getattr(subject, f, None)
        if v is not None:
            return (Decimal(int(v)) / Decimal(100))

    # Request override (e.g., /hand-off?...&amount=50.00)
    if req_amount:
        return Decimal(str(req_amount))

    return None

@payfast_bp.get("/cancel", endpoint="payfast_cancel")
def cancel():
    return render_template("payments/cancelled.html"), 200

@payfast_bp.post("/notify")
@csrf.exempt
def notify():
    cfg   = current_app.config
    mode  = (cfg.get("PAYFAST_MODE") or "sandbox").lower()
    form  = request.form.to_dict(flat=True)

    # --- validate with PayFast (authoritative) ---
    validate_url = (
        "https://sandbox.payfast.co.za/eng/query/validate"
        if mode == "sandbox" else
        "https://www.payfast.co.za/eng/query/validate"
    )
    try:
        r = requests.post(validate_url, data=form, timeout=8)
        if r.text.strip().lower() != "valid":
            current_app.logger.error("IPN validate failed: %s", r.text)
            return ("", 400)
    except Exception:
        current_app.logger.exception("IPN validate error")
        return ("", 400)

    # --- extract fields we care about ---
    email   = (form.get("email_address") or "").strip().lower()
    amount  = (form.get("amount") or "").strip()          # "50.00"
    mref    = (form.get("m_payment_id") or "").strip()    # "<slug>-<rand>"
    status  = (form.get("payment_status") or "").upper()  # COMPLETE / PENDING / FAILED

    # find/create user (minimal)
    user = User.query.filter_by(email=email).first()
    if not user:
        user = User(email=email)
        db.session.add(user)
        db.session.flush()   # get user.id

    # --- on COMPLETE, activate the enrollment for the subject slug ---
    if status == "COMPLETE":
        slug = mref.split("-")[0] if "-" in mref else None
        subj = AuthSubject.query.filter_by(slug=slug).first() if slug else None

        if subj:
            # Manual upsert into user_enrollment (no ON CONFLICT)
            existing = db.session.execute(
                text("""
                    SELECT id, status
                      FROM user_enrollment
                     WHERE user_id   = :uid
                       AND subject_id = :sid
                     LIMIT 1
                """),
                {"uid": user.id, "sid": subj.id},
            ).first()

            if existing:
                db.session.execute(
                    text("""
                        UPDATE user_enrollment
                           SET status = 'active'
                         WHERE id = :eid
                    """),
                    {"eid": existing.id},
                )
            else:
                db.session.execute(
                    text("""
                        INSERT INTO user_enrollment (user_id, subject_id, status, started_at)
                        VALUES (:uid, :sid, 'active', CURRENT_TIMESTAMP)
                    """),
                    {"uid": user.id, "sid": subj.id},
                )

    db.session.commit()
    current_app.logger.info(
        "PF IPN ok: email=%s amount=%s status=%s ref=%s",
        email, amount, status, mref
    )
    return ("", 200)

@payfast_bp.get("/success", endpoint="payfast_success")
def success():
    # 1) Read query params FIRST (then log)
    ref     = (request.args.get("ref")     or "").strip()
    email   = (request.args.get("email")   or session.get("pending_email") or "").strip().lower()
    subject = (request.args.get("subject") or session.get("pending_subject") or session.get("reg_ctx", {}).get("subject") or "loss").strip().lower()

    current_app.logger.info("PF SUCCESS hit: ref=%s email=%s subject=%s", ref, email, subject)

    # No email? show success page but ask to sign in
    if not email:
        flash("Payment completed. Please sign in to continue.", "info")
        return render_template("payments/success.html", subject=subject, ref=ref), 200

    # 2) Ensure user exists; apply staged password hash if we staged one at /register
    u = User.query.filter_by(email=email).first()
    if not u:
        staged = (session.get("reg_ctx", {}) or {}).get("password_hash")
        display = (session.get("reg_ctx", {}) or {}).get("full_name") or email.split("@", 1)[0].replace(".", " ").replace("_", " ").title()
        u = User(email=email, name=display, is_active=1)
        if staged:
            u.password_hash = staged
        db.session.add(u)
        db.session.flush()  # get u.id

    # 3) Resolve subject id (safe if missing)
    sid = db.session.execute(text("""
        SELECT id FROM auth_subject
        WHERE lower(slug)=:s OR lower(name)=:s
        LIMIT 1
    """), {"s": subject}).scalar()

    # 4) Flip enrollment to ACTIVE when we have a subject id
    # 4) Flip enrollment to ACTIVE when we have a subject id
    if sid:
        existing = db.session.execute(
            text("""
                SELECT id, status
                  FROM user_enrollment
                 WHERE user_id   = :uid
                   AND subject_id = :sid
                 LIMIT 1
            """),
            {"uid": int(u.id), "sid": int(sid)},
        ).first()

        if existing:
            db.session.execute(
                text("""
                    UPDATE user_enrollment
                       SET status = 'active'
                     WHERE id = :eid
                """),
                {"eid": existing.id},
            )
        else:
            db.session.execute(
                text("""
                    INSERT INTO user_enrollment (user_id, subject_id, status)
                    VALUES (:uid, :sid, 'active')
                """),
                {"uid": int(u.id), "sid": int(sid)},
            )

        session["just_paid_subject_id"] = int(sid)


    db.session.commit()

    # 5) Log in and show confirmation page (button → Bridge)
    try:
        login_user(u, remember=True, fresh=True)
    except Exception:
        pass

    session["payment_banner"] = f"Payment successful for {subject.title() if subject else 'your course'}. You're all set!"

    return render_template("payments/success.html", subject=subject, ref=ref), 200

@payfast_bp.post("/ipn")
def ipn():
    # ... your validation/signature checks ...

    eid = int(request.form.get("m_payment_id") or 0)
    amount_gross = request.form.get("amount_gross")  # e.g., "50.00"
    cents = int(round(float(amount_gross)*100)) if amount_gross else None

    if eid and cents is not None:
        db.session.execute(sa_text("""
            UPDATE user_enrollment
            SET charged_currency = 'ZAR',
                charged_amount_cents = :amt,
                gateway_country = NULL,
                country_mismatch = 0
            WHERE id = :eid
        """), {"amt": cents, "eid": eid})
        db.session.commit()

    return ("OK", 200)

@payfast_bp.get("/checkout/review")
def checkout_review():
    sid = request.args.get("subject_id", type=int) or subject_id_for("loss")
    if not session.get("pp_value"):
        # force user to lock country/price first
        return redirect(url_for("loss_bp.about_loss"))
    return render_template("payments/review.html", subject_id=sid)


@payfast_bp.route("/checkout/cancel", methods=["POST", "GET"])
def checkout_cancel():
    reason = (request.values.get("reason") or "").strip()

    if reason == "price_too_high":
        try:
            apply_percentage_discount(session, 10.0)
            if not session.get("pp_discount_flash_shown"):
                flash("We’ve applied a 10% discount for you.", "info")
                session["pp_discount_flash_shown"] = True
        except Exception:
            pass

    sid = request.values.get("subject_id", type=int)
    if not sid:
        try:
            sid, _ = _resolve_subject_from_request()
        except Exception:
            sid = None

    return redirect(url_for("payfast_bp.pricing_get", subject_id=sid)) if sid \
           else redirect(url_for("loss_bp.about_loss"))

@payfast_bp.get("/handoff")
def handoff_get():
    """
    Build the PayFast form from the session + subject and render a hand-off
    page that auto-posts to PayFast (sandbox or live depending on PAYFAST_MODE).
    """
    cfg   = current_app.config
    mode  = (cfg.get("PAYFAST_MODE") or "sandbox").lower()

    # Decide which PayFast URL to hit
    payfast_url = (
        "https://sandbox.payfast.co.za/eng/process"
        if mode == "sandbox" else
        "https://www.payfast.co.za/eng/process"
    )

    # Subject + user context
    subject_slug = (request.args.get("subject") or "loss").lower()
    email        = (session.get("pending_email") or "").strip().lower()

    subj = AuthSubject.query.filter_by(slug=subject_slug).first()
    if not subj:
        flash("Could not resolve subject for payment.", "error")
        return redirect(url_for("loss_bp.about_loss"))

    # Price / country from parity-pricing session
    country  = session.get("pp_country")  or "ZA"
    currency = session.get("pp_currency") or "ZAR"
    value    = session.get("pp_value")    or 0.00  # e.g. 150.00

    # Basic PayFast fields
    fields = {
        "merchant_id":   cfg["PAYFAST_MERCHANT_ID"],
        "merchant_key":  cfg["PAYFAST_MERCHANT_KEY"],
        "return_url":    url_for("payfast_bp.payfast_success", _external=True),
        "cancel_url":    url_for("payfast_bp.checkout_cancel", _external=True),
        "notify_url":    url_for("payfast_bp.notify", _external=True),
        "amount":        f"{value:.2f}",
        "item_name":     f"AIT – {subj.name} course",
        "m_payment_id":  str(session.get("just_enrolled_id") or ""),
        "email_address": email,
        # optional meta / description
        "custom_str1":   subject_slug,
        "custom_str2":   country,
        "custom_str3":   currency,
    }

    # Add signature (same rule PayFast docs use)
    passphrase = (cfg.get("PAYFAST_PASSPHRASE") or "").strip()
    pairs = [f"{k}={fields[k]}" for k in sorted(fields.keys()) if fields[k] not in (None, "")]
    query = "&".join(pairs)
    if passphrase:
        query = f"{query}&passphrase={passphrase}"
    sig = hashlib.md5(query.encode("utf-8")).hexdigest()
    fields["signature"] = sig

    return render_template(
        "payments/payfast_handoff.html",
        payfast_url=payfast_url,
        pf_data=fields,     # ← rename payfast_fields → pf_data
    )


@payfast_bp.post("/pricing/lock")
def pricing_lock():
    subject_id, subject_slug = _resolve_subject_from_request()
    code = (request.form.get("country") or "").upper()

    ccy = currency_for_country_code(code)
    if not ccy:
        flash("That country isn’t listed. Please choose any country that uses your currency.", "warning")
        return redirect(url_for("payfast_bp.pricing_get", subject_id=subject_id))

    cents = get_parity_anchor_cents(subject_id)
    session.update({
        "pp_country":  code,
        "pp_currency": ccy,
        "pp_value":    round((cents or 0) / 100.0, 2),
        "pp_discount": False,
        "pp_vat_note": "excl. VAT",
    })
    return redirect(url_for("payfast_bp.pricing_get", subject_id=subject_id))

@payfast_bp.get("/pricing")
def pricing_get():
    # First, try ?subject=loss from the About page
    subj_slug_arg = (request.args.get("subject") or "").strip().lower()
    if subj_slug_arg:
        subject_slug = subj_slug_arg
        # subject_id_for already used elsewhere in this file
        subject_id = subject_id_for(subject_slug)
    else:
        # Fallback to the old helper for other flows (subject_id / form etc.)
        subject_id, subject_slug = _resolve_subject_from_request()

    countries = countries_from_ref_with_names()

    # first-time setup of price in session
    if not session.get("pp_value") or not session.get("pp_currency"):
        cents = get_parity_anchor_cents(subject_id)  # ZAR anchor from auth_pricing

        if countries:
            session["pp_country"]  = countries[0]["code"]
            session["pp_currency"] = countries[0]["currency"]
        else:
            session.setdefault("pp_country", "ZA")
            session.setdefault("pp_currency", "ZAR")

        session["pp_value"]    = round((cents or 0) / 100.0, 2)
        session["pp_discount"] = False
        session["pp_vat_note"] = "excl. VAT"

    return render_template(
        "payments/pricing.html",
        subject_id=subject_id,
        subject_slug=subject_slug,
        countries=countries,
    )

