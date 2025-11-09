# app/payments/payfast.py
from uuid import uuid4
from flask import Blueprint, current_app, request, render_template_string, abort,render_template, redirect
from urllib.parse import urlencode
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

# app/payments/payfast.py


def pf_md5(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()

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
@payfast_bp.get("/notify")
def pf_notify_probe():
    return "OK", 200

# --- POST: keep your real IPN logic, but never 500; log instead
@payfast_bp.post("/notify")
def pf_notify_ipn():
    try:
        # TODO: your existing IPN validate & process
        return "OK", 200
    except Exception:
        current_app.logger.exception("PayFast IPN handler error")
        # PayFast expects 200 even if you reject later
        return "OK", 200

# Optional: success/cancel placeholders so they don’t 500
@payfast_bp.get("/success")
def pf_success():
    return "OK", 200

@payfast_bp.get("/cancel")
def pf_cancel():
    return "OK", 200



from decimal import Decimal, ROUND_HALF_UP
import re


def _cfg(name: str) -> str:
    val = current_app.config.get(name)
    if not val:
        abort(500, f"PayFast misconfiguration: {name} not set")
    return val







import hashlib, re, logging

log = logging.getLogger(__name__)


def _pf_host(cfg) -> str:
    mode = (cfg.get("PAYFAST_MODE") or "sandbox").lower()
    return "https://www.payfast.co.za/eng/process" if mode == "live" \
           else "https://sandbox.payfast.co.za/eng/process"


def _ref_part(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]", "-", s)[:60]

# at top
from urllib.parse import quote
import hashlib

def _pf_sig(data: dict, passphrase: str | None) -> str:
    clean = {k: str(v).strip() for k, v in data.items() if v not in (None, "",) and k != "signature"}
    parts = [f"{k}={quote_plus(clean[k])}" for k in sorted(clean)]
    if passphrase:
        parts.append(f"passphrase={quote_plus(passphrase)}")
    sig_base = "&".join(parts)
    sig = hashlib.md5(sig_base.encode("utf-8")).hexdigest()
    current_app.logger.info("PF sigbase=%s md5=%s", sig_base, sig)
    return sig



# payments/payfast_debug.py (optional)
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

# requires:
# from app.models import db, AuthSubject
# from sqlalchemy import text
# from decimal import Decimal, ROUND_HALF_UP
# from uuid import uuid4
# import re, hashlib
# (and your existing: _pf_host, _pf_sig, _ref helpers if not already defined)

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
        # allow emergency override: /hand-off?...&amount=50.00
        override = request.args.get("amount")
        if not override:
            abort(400, "Active price not configured for this subject")
        amount_str = _fmt_amount(override)
    else:
        amount_dec = (Decimal(int(row.amount_cents)) / Decimal(100)).quantize(Decimal("0.01"))
        if amount_dec <= 0:
            abort(400, "Amount must be > 0")
        amount_str = f"{amount_dec:.2f}"

    # https requirement for PayFast
    for k in ("PAYFAST_RETURN_URL","PAYFAST_CANCEL_URL","PAYFAST_NOTIFY_URL"):
        if not str(cfg.get(k)).startswith("https://"):
            abort(500, f"{k} must be https")

    # sandbox must NOT include passphrase
    mode = (cfg.get("PAYFAST_MODE") or "sandbox").lower()

    if mode == "sandbox":
        # Force official sandbox merchant + no passphrase
        merchant_id  = "10000100"
        merchant_key = "46f0cd694581a"
        passphrase   = ""
        payfast_host = "https://sandbox.payfast.co.za/eng/process"
    else:
        merchant_id  = cfg.get("PAYFAST_MERCHANT_ID")
        merchant_key = cfg.get("PAYFAST_MERCHANT_KEY")
        passphrase   = (cfg.get("PAYFAST_PASSPHRASE") or "")
        payfast_host = "https://www.payfast.co.za/eng/process"

    if mode == "sandbox" and cfg.get("PAYFAST_MERCHANT_ID") not in (None, "", "10000100"):
        current_app.logger.warning("Ignoring live merchant in sandbox; forcing 10000100.")

    # (optional) enforce https for PF return/cancel/notify
    for k in ("PAYFAST_RETURN_URL","PAYFAST_CANCEL_URL","PAYFAST_NOTIFY_URL"):
        if not str(cfg.get(k)).startswith("https://"):
            abort(500, f"{k} must be https")

    # unique ref, no DB writes
    mref = f"{_ref(slug)}-{uuid4().hex[:10]}"

    # Minimal, ASCII-only, sandbox-safe payload
    # helpers (once, near top of file)
    def _ascii(s: str) -> str:
        return (s or "").encode("ascii", "ignore").decode("ascii")

    # --- build minimal, sandbox-safe payload ---
    # force sandbox creds/host if mode == "sandbox" (you already added this)
    # merchant_id = "10000100"; merchant_key = "46f0cd694581a"; passphrase = ""; payfast_host = "https://sandbox.payfast.co.za/eng/process"

    # force sandbox creds/host if mode == "sandbox" (you already added this)
    # merchant_id = "10000100"; merchant_key = "46f0cd694581a"; passphrase = ""; payfast_host = "https://sandbox.payfast.co.za/eng/process"

    def _ascii(s: str) -> str:
        return (s or "").encode("ascii", "ignore").decode("ascii")

    item_name_clean = _ascii(f"{subject_name} enrollment")[:100]
    email_clean     = _ascii(email)
    mref_clean      = _ascii(mref)  # your stable/redirect-locked ref

    pf_data = {
        "merchant_id":   merchant_id,
        "merchant_key":  merchant_key,
        "return_url":    cfg["PAYFAST_RETURN_URL"].strip(),
        "cancel_url":    cfg["PAYFAST_CANCEL_URL"].strip(),
        "notify_url":    cfg["PAYFAST_NOTIFY_URL"].strip(),
        "m_payment_id":  mref_clean,
        "amount":        amount_str,           # "50.00"
        "item_name":     item_name_clean,      # ASCII only
        "email_address": email_clean,
        "payment_method":"cc",                 # optional, but harmless
    }
    pf_data["signature"] = _pf_sig(pf_data, passphrase)

    if debug:
        return render_template("payfast_handoff_debug.html",
                            payfast_url=payfast_host, pf_data=pf_data)

    return render_template("payfast_handoff.html",
                        payfast_url=payfast_host, pf_data=pf_data)




@payfast_bp.get("/_sanity")
def pf_sanity():
    payfast_host = "https://sandbox.payfast.co.za/eng/process"
    pf_data = {
        "merchant_id": "10000100",
        "merchant_key":"46f0cd694581a",
        "return_url":  current_app.config["PAYFAST_RETURN_URL"],
        "cancel_url":  current_app.config["PAYFAST_CANCEL_URL"],
        "notify_url":  current_app.config["PAYFAST_NOTIFY_URL"],
        "m_payment_id":"sanity-12345",
        "amount":      "50.00",
        "item_name":   "sanity enrollment",
        "email_address":"test@example.com",
    }
    pf_data["signature"] = _pf_sig(pf_data, "")  # sandbox: empty passphrase
    return render_template("payfast_handoff.html",
                           payfast_url=payfast_host, pf_data=pf_data)









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
