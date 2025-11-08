# app/payments/payfast.py
from flask import Blueprint, current_app, request, render_template_string, abort
from flask import redirect
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

# --- POST handlers (real IPN)
@payfast_bp.post("/notify")
def pf_notify_ipn():
    # TODO: your existing IPN validation + processing
    return "OK", 200

# --- GET probe: MUST always return 200 for PayFast checks
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
