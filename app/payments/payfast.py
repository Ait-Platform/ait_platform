# app/payments/payfast.py
from flask import Blueprint, current_app, request, render_template_string, abort,render_template, redirect
from urllib.parse import urlencode
import os, hmac, hashlib, ipaddress, requests
from urllib.parse import urlencode, quote_plus
import hashlib

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

def _fmt_amount(value) -> str:
    return f'{Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP):.2f}'

def _ref_part(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]", "-", s)[:60]

def _pf_sig(data: dict, passphrase: str | None) -> str:
    clean = {k: v for k, v in data.items() if v not in (None, "",) and k != "signature"}
    qs = urlencode(sorted(clean.items()))
    if passphrase:
        qs = f"{qs}&passphrase={passphrase}"
    return hashlib.md5(qs.encode("utf-8")).hexdigest()

@payfast_bp.get("/hand-off")
def handoff():
    cfg = current_app.config

    # 1) Basic args
    email = (request.args.get("email") or "").strip().lower()
    subject = (request.args.get("subject") or "").strip()
    if not email or not subject:
        abort(400, "Missing email or subject")

    # 2) Show exactly what’s missing (instead of a generic 500)
    required = [
        "PAYFAST_MERCHANT_ID",
        "PAYFAST_MERCHANT_KEY",
        "PAYFAST_RETURN_URL",
        "PAYFAST_CANCEL_URL",
        "PAYFAST_NOTIFY_URL",
    ]
    missing = [k for k in required if not cfg.get(k)]
    if missing:
        log.error("PayFast misconfig: missing %s", missing)
        return render_template("payfast_misconfig.html", missing=missing), 500

    # 3) Amount (replace with DB lookup when you wire it)
    amount = _fmt_amount(request.args.get("amount") or "50.00")

    # 4) Build payload
    m_payment_id = f"{_ref_part(subject)}:{_ref_part(email)}"
    pf_data = {
        "merchant_id":   cfg.get("PAYFAST_MERCHANT_ID"),
        "merchant_key":  cfg.get("PAYFAST_MERCHANT_KEY"),
        "return_url":    cfg.get("PAYFAST_RETURN_URL"),
        "cancel_url":    cfg.get("PAYFAST_CANCEL_URL"),
        "notify_url":    cfg.get("PAYFAST_NOTIFY_URL"),
        "m_payment_id":  m_payment_id,
        "amount":        amount,
        "item_name":     f"{subject} enrollment",
        "item_description": f"AIT • {subject}",
        "email_address": email,
    }
    pf_data["signature"] = _pf_sig(pf_data, cfg.get("PAYFAST_PASSPHRASE"))

    # 5) Log once (safe fields only)
    log.info("PayFast handoff → %s  m_payment_id=%s amount=%s email=%s",
             _pf_host(cfg), m_payment_id, amount, email)

    return render_template(
        "payfast_handoff.html",
        payfast_url=_pf_host(cfg),
        pf_data=pf_data
    )

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
