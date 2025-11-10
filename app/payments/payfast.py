# app/payments/payfast.py
from uuid import uuid4
from flask import Blueprint, current_app, request, render_template_string, abort,render_template, redirect, url_for
from urllib.parse import urlencode
from flask_login import current_user, login_user
import os, hmac, hashlib, ipaddress, requests
from urllib.parse import urlencode, quote_plus
import hashlib
import re
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
'''
@payfast_bp.post("/ipn")
def ipn():
    # 1) Read the raw POST exactly as PayFast sent it
    raw = request.get_data(as_text=True)  # e.g. "m_payment_id=...&pf_payment_id=...&...&signature=abcd"

    # 2) Drop the signature pair from the raw string (do NOT re-order anything)
    parts = [p for p in raw.split("&") if not p.startswith("signature=")]
    signed_str = "&" .join(parts)

    # 3) Append your passphrase
    passphrase = cfg("PAYFAST_PASSPHRASE")
    if passphrase:
        signed_str = f"{signed_str}&passphrase={passphrase}"

    # 4) Compute and compare
    expected = pf_md5(signed_str)
    got = request.form.get("signature", "")
    if got != expected:
        current_app.logger.warning(f"PayFast signature mismatch: got={got} expected={expected}")
        return "bad sig", 400

    # 5) (optional but recommended) server-to-server validate
    try:
        resp = requests.post(PAYFAST_VALIDATE_URL, data=request.form, timeout=10)
        if resp.text.strip() != "VALID":
            current_app.logger.warning(f"PayFast validate said INVALID: {resp.text[:200]}")
            return "invalid", 400
    except Exception:
        current_app.logger.exception("PayFast validate POST failed")
        return "validate error", 500

    # 6) Your business logic
    status = request.form.get("payment_status")
    m_payment_id = request.form.get("m_payment_id")
    amount_gross = request.form.get("amount_gross")

    # TODO: load your order/enrollment by m_payment_id, compare expected amount/currency, etc.
    # if Decimal(amount_gross) != order.amount: return "bad amount", 400
    # if status == "COMPLETE": order.mark_paid(...)

    current_app.logger.info(f"PayFast IPN OK: {m_payment_id} status={status} amount={amount_gross}")
    return "ok", 200
'''
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
        if status == "COMPLETE" and "-" in mref:
            slug = mref.split("-", 1)[0]
            subj = AuthSubject.query.filter_by(slug=slug).first()
            if subj:
                db.session.execute(
                    text("""INSERT INTO user_enrollment (user_id, subject_id, status, started_at)
                            SELECT id, :sid, 'active', CURRENT_TIMESTAMP FROM "user"
                            WHERE lower(email)=:em
                            ON CONFLICT(user_id, subject_id)
                            DO UPDATE SET status='active', updated_at=CURRENT_TIMESTAMP"""),
                    {"sid": subj.id, "em": email}
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

@payfast_bp.get("/hand-off")
def handoff():
    cfg = current_app.config

    email = (request.args.get("email") or "").strip().lower()
    slug  = (request.args.get("subject") or "").strip()
    debug = (request.args.get("debug") == "1")
    if not email or not slug:
        abort(400, "Missing email or subject")

    # required env
    required = ["PAYFAST_MERCHANT_ID","PAYFAST_MERCHANT_KEY",
                "PAYFAST_RETURN_URL","PAYFAST_CANCEL_URL","PAYFAST_NOTIFY_URL"]
    missing = [k for k in required if not cfg.get(k)]
    if missing:
        return render_template("payfast_misconfig.html", missing=missing), 500

    # subject (id + name)
    subject = AuthSubject.query.filter_by(slug=slug).first()
    if not subject:
        abort(400, "Subject not found")
    subject_id   = subject.id
    subject_name = getattr(subject, "name", slug)

    # price from auth_pricing.amount_cents (active, most recent)
    row = db.session.execute(text("""
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
    """), {"sid": subject_id}).first()
    if not row or row.amount_cents is None:
        abort(400, "Active price not configured for this subject")
    amount_str = f"{(Decimal(int(row.amount_cents))/Decimal(100)).quantize(Decimal('0.01')):.2f}"

    # enforce https URLs
    for k in ("PAYFAST_RETURN_URL","PAYFAST_CANCEL_URL","PAYFAST_NOTIFY_URL"):
        if not str(cfg.get(k)).startswith("https://"):
            abort(500, f"{k} must be https")

    # mode → creds/host
    mode = (cfg.get("PAYFAST_MODE") or "sandbox").lower()
    if mode == "sandbox":
        merchant_id  = "10000100"
        merchant_key = "46f0cd694581a"
        passphrase   = ""  # sandbox: no passphrase
        payfast_host = "https://sandbox.payfast.co.za/eng/process"
        if cfg.get("PAYFAST_MERCHANT_ID") not in (None, "", "10000100"):
            current_app.logger.warning("Ignoring live merchant in sandbox; forcing 10000100.")
    else:
        merchant_id  = cfg.get("PAYFAST_MERCHANT_ID")
        merchant_key = cfg.get("PAYFAST_MERCHANT_KEY")
        passphrase   = (cfg.get("PAYFAST_PASSPHRASE") or "")
        payfast_host = "https://www.payfast.co.za/eng/process"

    # unique ref, no DB writes
    mref = f"{_ref(slug)}-{uuid4().hex[:10]}"

    # ASCII-clean minimal payload
    def _ascii(s: str) -> str: return (s or "").encode("ascii","ignore").decode("ascii")
    # after you compute mref and amount_str
    return_url = f"{current_app.config['PAYFAST_RETURN_URL']}?ref={mref}&email={email}"

    item_name_clean = (f"{subject_name} enrollment")[:100]
    email_clean = email

    # ensure user exists
    # ensure user exists + has a display name
    u = User.query.filter_by(email=email).first()
    if not u:
        u = User(email=email, is_active=1)
        # sensible default name from email
        u.name = email.split("@", 1)[0].replace(".", " ").replace("_", " ").title()
        db.session.add(u)
    else:
        if not u.name or u.name.strip() == "":
            u.name = email.split("@", 1)[0].replace(".", " ").replace("_", " ").title()

    # ensure subject exists
    subj = AuthSubject.query.filter_by(slug=slug).first() or abort(400, "Subject not found")

    # upsert pending enrollment — only columns that exist
    db.session.execute(text("""
        INSERT INTO user_enrollment (user_id, subject_id, status)
        VALUES (:uid, :sid, 'pending')
        ON CONFLICT(user_id, subject_id)
        DO UPDATE SET status='pending'
    """), {"uid": u.id, "sid": subj.id})

    db.session.commit()


    pf_data = {
        "merchant_id":   merchant_id,
        "merchant_key":  merchant_key,
        "return_url":    return_url,   # <— use this instead of the plain config value
        "cancel_url":    current_app.config["PAYFAST_CANCEL_URL"],
        "notify_url":    current_app.config["PAYFAST_NOTIFY_URL"],
        "m_payment_id":  mref,
        "amount":        amount_str,
        "item_name":     item_name_clean,
        "email_address": email_clean,
    }


    # In sandbox, do NOT include signature at all
    if merchant_id == "10000100":
        pf_data.pop("signature", None)
    else:
        pf_data["signature"] = _pf_sig(pf_data, passphrase)

    # signature (sandbox → passphrase "", so no passphrase param appended)
    #pf_data["signature"] = _pf_sig(pf_data, passphrase)

    current_app.logger.info("PF handoff host=%s data=%s",
        payfast_host, {k: v for k, v in pf_data.items() if k != "signature"})

    if debug:
        return render_template("payfast_handoff_debug.html",
                               payfast_url=payfast_host, pf_data=pf_data)

    return render_template("payfast_handoff.html",
                           payfast_url=payfast_host, pf_data=pf_data)

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
    return render_template("payfast_handoff.html",
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

@payfast_bp.get("/cancel")
def cancel():
    return render_template("payment_cancelled.html")

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
    amount  = (form.get("amount") or "").strip()          # string like "50.00"
    mref    = (form.get("m_payment_id") or "").strip()    # we set as "<slug>-<rand>"
    status  = (form.get("payment_status") or "").upper()  # COMPLETE / PENDING / FAILED

    # find/create user (minimal)
    user = User.query.filter_by(email=email).first()
    if not user:
        user = User(email=email)
        db.session.add(user)
        db.session.flush()   # get user.id

    # --- log every IPN in auth_payment_log (simple, append-only) ---
    db.session.execute(text("""
        INSERT INTO user_enrollment (user_id, subject_id, status)
        VALUES (:uid, :sid, 'active')
        ON CONFLICT(user_id, subject_id)
        DO UPDATE SET status='active'
    """), {"uid": user.id, "sid": subj.id})


    # --- on COMPLETE, (optionally) activate the enrollment for the subject slug ---
    if status == "COMPLETE":
        slug = mref.split("-")[0] if "-" in mref else None
        subj = AuthSubject.query.filter_by(slug=slug).first() if slug else None
        if subj:
            # upsert into user_enrollment; assumes PK (user_id, subject_id)
            db.session.execute(
                text("""
                    INSERT INTO user_enrollment (user_id, subject_id, status, started_at)
                    VALUES (:uid, :sid, 'active', CURRENT_TIMESTAMP)
                    ON CONFLICT(user_id, subject_id)
                    DO UPDATE SET status='active', updated_at=CURRENT_TIMESTAMP
                """),
                {"uid": user.id, "sid": subj.id}
            )

    db.session.commit()
    current_app.logger.info("PF IPN ok: email=%s amount=%s status=%s ref=%s", email, amount, status, mref)
    return ("", 200)

@payfast_bp.get("/success", endpoint="payfast_success")
def success():
    if current_user.is_authenticated:
        return redirect(url_for("auth_bp.bridge_dashboard"))

    email = (request.args.get("email") or "").strip().lower()
    if email:
        u = User.query.filter_by(email=email).first()
        if not u:
            # extreme edge: create if missing, match the same defaults
            u = User(email=email, is_active=1)
            u.name = email.split("@", 1)[0].replace(".", " ").replace("_", " ").title()
            db.session.add(u); db.session.commit()
        login_user(u, remember=True)
        return redirect(url_for("auth_bp.bridge_dashboard"))

    return render_template("payment_success.html"), 200
