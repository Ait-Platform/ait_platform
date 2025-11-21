# app/payments/payfast.py
import email
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
from app.payments.quote import fx_for_country_code
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
        # Apply a once-off 10% discount to the current pp_value
        try:
            apply_percentage_discount(session, 10.0)
        except Exception:
            current_app.logger.exception("Failed to apply discount")


    sid = request.values.get("subject_id", type=int)
    if not sid:
        try:
            sid, _ = _resolve_subject_from_request()
        except Exception:
            sid = None

    # If we still can't resolve a subject, send them home
    if not sid:
        return redirect(url_for("public_bp.welcome"))

    # Go back to pricing with the discount page
    return redirect(url_for("payfast_bp.pricing_get", subject_id=sid, discount=1))


@payfast_bp.route("/handoff", methods=["GET", "POST"])
def handoff():
    """
    Final step before redirecting to PayFast.

    Called from /register/decision:

        /payments/handoff?email=...&subject=loss&debug=0
    """
    cfg = current_app.config

    # 1) Inputs (POST first, then GET fallback)
    email = (request.form.get("email")
             or request.args.get("email")
             or "").strip().lower()
    slug = (request.form.get("subject")
            or request.args.get("subject")
            or "").strip().lower()
    debug = str(request.values.get("debug") or "0") == "1"

    if not email or not slug:
        flash("Missing email or subject for payment.", "danger")
        return redirect(url_for("public_bp.welcome"))

    # 2) Config sanity – MUST return a response on failure
    required = [
        "PAYFAST_MERCHANT_ID",
        "PAYFAST_MERCHANT_KEY",
        "PAYFAST_RETURN_URL",
        "PAYFAST_CANCEL_URL",
        "PAYFAST_NOTIFY_URL",
    ]
    missing = [k for k in required if not cfg.get(k)]
    if missing:
        current_app.logger.error("PayFast misconfig: missing %s", missing)
        return render_template(
            "payments/payfast_misconfig.html",
            missing=missing,
        ), 500

    # 3) Subject lookup
    subject_row = AuthSubject.query.filter_by(slug=slug).first()
    if not subject_row:
        flash("Enrollment for this course is not available right now.", "warning")
        return redirect(url_for("public_bp.welcome"))

    subject_id = subject_row.id
    subject_name = getattr(subject_row, "name", slug)

    # 4) Determine the amount in ZAR
    #
    # Strategy:
    #   a) Use reg_ctx["quote"].country_code if present.
    #   b) Compute ZAR via price_for_country(subject_id, country).
    #   c) If that fails, fall back to auth_pricing ZAR price.
    # 4) Determine the amount in ZAR (what PayFast actually charges)
    #
    # Strategy:
    #   a) Use reg_ctx["quote"].est_zar_cents if present.
    #   b) If missing/zero, fall back to auth_pricing ZAR row.
    # 4) Determine the amount in ZAR (what PayFast actually charges)
    #
    # First, trust the amount locked in the pricing flow (pf_amount_zar).
    # If that is missing/bad, fall back to auth_pricing.
    # 4) Determine the amount in ZAR (what PayFast actually charges)
    #
    # First, trust the amount locked in the pricing flow (pf_amount_zar).
    # If that is missing/bad, fall back to auth_pricing.
    reg_ctx = session.get("reg_ctx") or {}
    quote   = reg_ctx.get("quote") or {}

    country_code = (quote.get("country_code") or "ZA").strip().upper()

    pf_amount_zar = (session.get("pf_amount_zar") or "").strip()
    zar_cents = None

    if pf_amount_zar:
        try:
            # pf_amount_zar is a string like "6190.00"
            amt_dec = Decimal(pf_amount_zar)
            zar_cents = int(amt_dec * 100)
        except Exception:
            current_app.logger.warning("Bad pf_amount_zar in session: %r", pf_amount_zar)
            zar_cents = None

    if zar_cents is None or zar_cents <= 0:
        # Fallback: direct ZAR pricing from auth_pricing
        row = db.session.execute(
            sa_text(
                """
                SELECT amount_cents
                FROM auth_pricing
                WHERE subject_id = :sid
                  AND plan = 'enrollment'
                  AND COALESCE(is_active, 1) = 1
                  AND (active_to IS NULL OR active_to > CURRENT_TIMESTAMP)
                ORDER BY
                  COALESCE(active_from, created_at) DESC,
                  COALESCE(updated_at, created_at) DESC,
                  id DESC
                LIMIT 1
                """
            ),
            {"sid": subject_id},
        ).first()

        if not row or row.amount_cents is None:
            current_app.logger.error("No active price for subject_id=%s", subject_id)
            return render_template(
                "payments/payfast_misconfig.html",
                missing=["auth_pricing row for this subject"],
            ), 500

        zar_cents = int(row.amount_cents)
        pf_amount_zar = f"{Decimal(zar_cents) / Decimal(100):.2f}"

    # final ZAR amount for PayFast (string like "6190.00" or discounted "5571.00")
    amt_str = pf_amount_zar

    # refresh quote in session so we always know what we charged in ZAR
    quote.update({
        "est_zar_cents": zar_cents,
        "country_code":  country_code,
    })
    reg_ctx["quote"] = quote
    session["reg_ctx"] = reg_ctx
    session.modified = True


    # 5) Basic URL policy checks
    for k in ("PAYFAST_RETURN_URL", "PAYFAST_CANCEL_URL", "PAYFAST_NOTIFY_URL"):
        val = str(cfg.get(k) or "").strip()
        if not val:
            current_app.logger.error("%s missing", k)
            return render_template(
                "payments/payfast_misconfig.html",
                missing=[k],
            ), 500
        if val.startswith("http://") and ("localhost" not in val and "127.0.0.1" not in val):
            current_app.logger.error("%s must be https (value=%r)", k, val)
            return render_template(
                "payments/payfast_misconfig.html",
                missing=[k + " must be https"],
            ), 500

    # 6) Mode / credentials
    mode = (cfg.get("PAYFAST_MODE") or "sandbox").lower()
    merchant_id = cfg.get("PAYFAST_MERCHANT_ID")
    merchant_key = cfg.get("PAYFAST_MERCHANT_KEY")
    passphrase = cfg.get("PAYFAST_PASSPHRASE") or ""

    if mode == "sandbox":
        payfast_url = "https://sandbox.payfast.co.za/eng/process"
    else:
        payfast_url = "https://www.payfast.co.za/eng/process"

    current_app.logger.info("PayFast %s mode: merchant_id=%s", mode, merchant_id)

    # 7) Ensure user exists / staged hash (from reg_ctx)
    u = User.query.filter_by(email=email).first()
    if not u:
        u = User(email=email, is_active=1)

    if not (u.name or "").strip():
        u.name = (
            email.split("@", 1)[0]
            .replace(".", " ")
            .replace("_", " ")
            .title()
        )

    staged = reg_ctx.get("password_hash")
    if staged and not getattr(u, "password_hash", None):
        u.password_hash = staged

    db.session.add(u)
    db.session.flush()

    # 8) Ensure a pending enrollment row
    existing = db.session.execute(
        sa_text(
            """
            SELECT id, status
            FROM user_enrollment
            WHERE user_id = :uid
              AND subject_id = :sid
            LIMIT 1
            """
        ),
        {"uid": u.id, "sid": subject_id},
    ).first()

    if existing:
        db.session.execute(
            sa_text(
                """
                UPDATE user_enrollment
                SET status = 'pending'
                WHERE id = :eid
                """
            ),
            {"eid": existing.id},
        )
    else:
        db.session.execute(
            sa_text(
                """
                INSERT INTO user_enrollment (user_id, subject_id, status)
                VALUES (:uid, :sid, 'pending')
                """
            ),
            {"uid": u.id, "sid": subject_id},
        )

    db.session.commit()

    # 9) Build PayFast payload
    mref = f"{_ref(slug)}-{uuid4().hex[:10]}"
    return_url = f"{cfg['PAYFAST_RETURN_URL']}?ref={mref}&email={email}&subject={slug}"

    quote = reg_ctx.get("quote") or {}
    display_amount_cents = int(quote.get("amount_cents") or zar_cents)
    display_currency = quote.get("currency") or "ZAR"
    parity_descr = (
        f"Locked parity price {display_amount_cents / 100:.2f} "
        f"{display_currency} ({quote.get('country_code') or 'ZA'})"
    )

    pf_data = {
        "merchant_id":      merchant_id,
        "merchant_key":     merchant_key,
        "return_url":       return_url,
        "cancel_url":       cfg["PAYFAST_CANCEL_URL"],
        "notify_url":       cfg["PAYFAST_NOTIFY_URL"],
        "m_payment_id":     mref,
        "amount":           amt_str,  # ZAR amount PayFast will charge
        "item_name":        (f"{subject_name} enrollment")[:100],
        "item_description": parity_descr[:255],
        "email_address":    email,
    }

    # Signature (skip for generic test merchant if needed)
    if merchant_id != "10043395":  # keep your sandbox/live IDs here
        pf_data["signature"] = _pf_sig(pf_data, passphrase)
    else:
        pf_data.pop("signature", None)

    current_app.logger.info(
        "PF handoff url=%s data=%s",
        payfast_url,
        {k: v for k, v in pf_data.items() if k != "signature"},
    )

    # 10) Debug mode → show debug template instead of auto-post
    if debug:
        return render_template(
            "payments/payfast_handoff_debug.html",
            payfast_url=payfast_url,
            pf_data=pf_data,
        )

    # 11) Normal auto-post template
    return render_template(
        "payments/payfast_handoff.html",
        payfast_url=payfast_url,
        pf_data=pf_data,
    )

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
        db.session.flush()   

    # currency less
    amount_gross_str = (request.form.get("amount_gross")
                        or request.form.get("amount")
                        or "0.00").strip()

    try:
        amt_dec = Decimal(amount_gross_str).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        amount_zar_cents = int(amt_dec * 100)
    except Exception:
        current_app.logger.warning("Bad amount_gross in IPN: %r", amount_gross_str)
        amount_zar_cents = 0
        
    
    
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

        # --- log payment in auth_payment_log (one clean row) ---
        try:
            db.session.execute(
                text("""
                    INSERT INTO auth_payment_log (user_id, program, amount, timestamp)
                    VALUES (:uid, :prog, :amt, CURRENT_TIMESTAMP)
                """),
                {
                    "uid":  user.id,
                    "prog": mref,               # e.g. "loss-a32a506794"
                    "amt":  amount_gross_str,   # PayFast ZAR amount like "75.00"
                },
            )
        except Exception:
            current_app.logger.exception("Failed to insert row into auth_payment_log")

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
'''
@payfast_bp.get("/pricing")
def pricing_get():
    # Are we on the discount version (after cancel)?
    discount_flag = request.args.get("discount", type=int) == 1

    # Resolve subject
    subj_slug_arg = (request.args.get("subject") or "").strip().lower()
    if subj_slug_arg:
        subject_slug = subj_slug_arg
        subject_id = subject_id_for(subject_slug)
    else:
        subject_id, subject_slug = _resolve_subject_from_request()

    # Fresh visit from About with ?subject=loss and NOT discount:
    # wipe any old quote so the form starts blank.
    if (not discount_flag) and ("subject" in request.args):
        for key in (
            "pp_country",
            "pp_currency",
            "pp_value",
            "pp_est_zar",
            "pp_est_zar_base",  # NEW: keep original (non-discount) ZAR
            "pp_fx_rate",
            "pp_discount",
        ):
            session.pop(key, None)
        # also clear any previous PayFast amount
        session.pop("pf_amount_zar", None)

    countries = countries_from_ref_with_names()

    has_quote = bool(
        session.get("pp_country")
        and session.get("pp_currency")
        and (session.get("pp_value") is not None)
    )

    # Base context from session (non-discounted view of what we have)
    price_ctx = {
        "local_amount":   session.get("pp_value"),
        "local_currency": session.get("pp_currency"),
        "estimated_zar":  session.get("pp_est_zar"),
        "fx_rate":        session.get("pp_fx_rate"),
        "country_code":   session.get("pp_country"),
        "has_quote":      has_quote,
        # we'll overwrite is_discount below after we apply logic
        "is_discount":    False,
    }

    # Decide if this visit should be treated as discount
    is_discount = bool(session.get("pp_discount") or discount_flag)

    # Always work from the base (non-discount) ZAR estimate so we don't double-discount
    base_est_zar = session.get("pp_est_zar_base")
    if base_est_zar is None:
        base_est_zar = price_ctx["estimated_zar"]

    final_est_zar = base_est_zar
    final_local = price_ctx["local_amount"]

    if base_est_zar is not None:
        # Apply 10% discount ON THE ZAR VALUE if this is the discount page
        if is_discount:
            final_est_zar = round(float(base_est_zar) * 0.90, 2)
        else:
            final_est_zar = float(base_est_zar)

        # If we have an FX rate, recompute local in a clean way from ZAR
        fx_rate = price_ctx["fx_rate"]
        if fx_rate is not None:
            final_local = round(final_est_zar * float(fx_rate), 2)
        else:
            # fall back to whatever we already had in local_amount
            final_local = price_ctx["local_amount"]

        # Update context with final values
        price_ctx["estimated_zar"] = final_est_zar
        price_ctx["local_amount"] = final_local

        # Persist back to session for consistency + PayFast
        session.update({
            "pp_value":       final_local,
            "pp_est_zar":     final_est_zar,
            "pp_est_zar_base": base_est_zar,
            "pp_discount":    is_discount,
            # This is what PayFast must charge in ZAR (string "123.45")
            "pf_amount_zar":  f"{final_est_zar:.2f}",
        })
    else:
        # No ZAR estimate; we can still record discount flag
        session["pp_discount"] = is_discount
        # and ensure there is no stale PayFast amount
        session.pop("pf_amount_zar", None)

    price_ctx["is_discount"] = is_discount

    # Choose template
    template = (
        "payments/pricing_discount.html"
        if price_ctx["is_discount"] else
        "payments/pricing.html"
    )

    return render_template(
        template,
        subject_id=subject_id,
        subject_slug=subject_slug,
        countries=countries,
        price=price_ctx,
    )


@payfast_bp.post("/pricing/lock")
def pricing_lock():
    subject_id, subject_slug = _resolve_subject_from_request()
    code = (request.form.get("country") or "").strip().upper()

    if not code:
        flash("Please choose your country first.", "warning")
        return redirect(url_for("payfast_bp.pricing_get", subject_id=subject_id))

    # Parity engine: local price + ZAR estimate
    # cur: currency code ("ZAR"/"USD"/...)
    # local_cents: parity price in local cents
    # est_zar_cents: underlying ZAR anchor in cents
    # fx: multiplier used (local per ZAR), may be None
    cur, local_cents, est_zar_cents, fx = price_for_country(subject_id, code)

    safe_local_cents = local_cents or 0
    local_value = round(safe_local_cents / 100.0, 2)

    if est_zar_cents is not None:
        est_zar_value = round(est_zar_cents / 100.0, 2)
    else:
        est_zar_value = None

    fx_value = float(fx) if fx is not None else None

    # Store both the *base* ZAR estimate and the current one (same at this point)
    session.update({
        "pp_country":      code,
        "pp_currency":     cur,
        "pp_value":        local_value,      # what user sees as "Price (your currency)"
        "pp_est_zar":      est_zar_value,    # current ZAR estimate
        "pp_est_zar_base": est_zar_value,    # base ZAR (used for discount math)
        "pp_fx_rate":      fx_value,         # for recomputing local from ZAR
        "pp_vat_note":     "excl. VAT",
        "pp_discount":     False,            # start with no discount
    })

    # Also set the non-discount PayFast ZAR amount now
    if est_zar_value is not None:
        session["pf_amount_zar"] = f"{est_zar_value:.2f}"
    else:
        session.pop("pf_amount_zar", None)

    return redirect(url_for("payfast_bp.pricing_get", subject_id=subject_id))
'''
@payfast_bp.post("/pricing/lock")
def pricing_lock():
    subject_id, subject_slug = _resolve_subject_from_request()
    code = (request.form.get("country") or "").strip().upper()

    if not code:
        flash("Please choose your country first.", "warning")
        return redirect(url_for("payfast_bp.pricing_get", subject_id=subject_id))

    # Parity engine: local parity price + ZAR via FX
    # local_cents = parity (currency-less = 75 → 75 local)
    # est_zar_cents = that 75 local converted to ZAR via fx (ONE FX use)
    cur, local_cents, est_zar_cents, fx = price_for_country(subject_id, code)

    safe_local_cents = local_cents or 0
    local_value = round(safe_local_cents / 100.0, 2)

    if est_zar_cents is not None:
        est_zar_value = round(est_zar_cents / 100.0, 2)
    else:
        est_zar_value = None

    # Store base (non-discount) values
    session.update({
        "pp_country":       code,
        "pp_currency":      cur,
        "pp_value":         local_value,      # current local shown to user (75 local)
        "pp_value_base":    local_value,      # base local for possible future use
        "pp_est_zar":       est_zar_value,    # current ZAR (from FX once)
        "pp_est_zar_base":  est_zar_value,    # base ZAR for discount math
        "pp_fx_rate":       float(fx) if fx is not None else None,
        "pp_vat_note":      "excl. VAT",
        "pp_discount":      False,            # no discount yet
    })

    # This is the amount PayFast will charge in ZAR (before discount)
    if est_zar_value is not None:
        session["pf_amount_zar"] = f"{est_zar_value:.2f}"
    else:
        session.pop("pf_amount_zar", None)

    return redirect(url_for("payfast_bp.pricing_get", subject_id=subject_id))

@payfast_bp.get("/pricing")
def pricing_get():
    # Are we on the discount version (after cancel)?
    discount_flag = request.args.get("discount", type=int) == 1

    # Resolve subject
    subj_slug_arg = (request.args.get("subject") or "").strip().lower()
    if subj_slug_arg:
        subject_slug = subj_slug_arg
        subject_id = subject_id_for(subject_slug)
    else:
        subject_id, subject_slug = _resolve_subject_from_request()

    # Fresh visit from About with ?subject=... and NOT discount:
    # wipe any old quote so the form starts blank.
    if (not discount_flag) and ("subject" in request.args):
        for key in (
            "pp_country",
            "pp_currency",
            "pp_value",
            "pp_value_base",
            "pp_est_zar",
            "pp_est_zar_base",
            "pp_fx_rate",
            "pp_discount",
        ):
            session.pop(key, None)
        session.pop("pf_amount_zar", None)

    countries = countries_from_ref_with_names()

    has_quote = bool(
        session.get("pp_country")
        and session.get("pp_currency")
        and (session.get("pp_value") is not None)
    )

    # Base values from session
    base_local = session.get("pp_value_base")
    base_zar   = session.get("pp_est_zar_base")

    # Decide discount state
    is_discount = bool(session.get("pp_discount") or discount_flag)

    final_local = base_local
    final_zar   = base_zar

    # Apply 10% discount ONLY on ZAR (and mirror on local by same factor)
    if is_discount and (base_zar is not None):
        final_zar = round(float(base_zar) * 0.9, 2)
        if base_local is not None:
            final_local = round(float(base_local) * 0.9, 2)

        # Persist updated (discounted) values
        session.update({
            "pp_value":    final_local,
            "pp_est_zar":  final_zar,
            "pp_discount": True,
        })

        # PayFast amount is now the discounted ZAR
        session["pf_amount_zar"] = f"{final_zar:.2f}"
    else:
        # Non-discount view (or no base yet)
        if base_local is not None:
            session["pp_value"] = base_local
        if base_zar is not None:
            session["pp_est_zar"] = base_zar
            session["pf_amount_zar"] = f"{base_zar:.2f}"
        else:
            session.pop("pf_amount_zar", None)

        session["pp_discount"] = False

    price_ctx = {
        "local_amount":   session.get("pp_value"),
        "local_currency": session.get("pp_currency"),
        "estimated_zar":  session.get("pp_est_zar"),
        "fx_rate":        session.get("pp_fx_rate"),
        "country_code":   session.get("pp_country"),
        "has_quote":      has_quote,
        "is_discount":    bool(session.get("pp_discount")),
    }

    # Decide which template to show
    template = (
        "payments/pricing_discount.html"
        if price_ctx["is_discount"] else
        "payments/pricing.html"
    )

    return render_template(
        template,
        subject_id=subject_id,
        subject_slug=subject_slug,
        countries=countries,
        price=price_ctx,
    )
