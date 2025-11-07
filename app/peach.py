# app_peach.py
import os, hmac, hashlib, json
from datetime import datetime
from flask import Flask, request, redirect, jsonify, abort
import requests

app = Flask(__name__)

PEACH_BASE = os.getenv("PEACH_BASE_URL", "https://sandbox-card.peachpayments.com/v1")
ENTITY_ID    = os.getenv("PEACH_ENTITY_ID")
SECRET_TOKEN = os.getenv("PEACH_SECRET_TOKEN")            # also used for webhook signature
CLIENT_ID    = os.getenv("PEACH_CLIENT_ID")
CLIENT_SECRET= os.getenv("PEACH_CLIENT_SECRET")

def peach_headers():
    # If you use OAuth for specific APIs (e.g., Payment Links), swap in bearer token here.
    return {
        "Content-Type": "application/json",
        "X-Entity-Id": ENTITY_ID,
        "Authorization": f"Bearer {SECRET_TOKEN}"
    }

@app.post("/checkout/create")
def create_checkout():
    """
    Create a payment with Peach (Hosted Checkout).
    Store a local pending record; redirect customer to Peach-hosted page.
    """
    data = request.get_json() or {}
    order_id = data.get("order_id")
    amount = data.get("amount")            # string cents or minor units as your pricing model dictates
    currency = data.get("currency")        # "ZAR" / "USD" etc.

    # TODO: persist (order_id, amount, currency, status='pending')

    payload = {
        # Example payload; use fields per Peach Checkout/Payments API you enable.
        "amount": amount,
        "currency": currency,
        "merchantTransactionId": order_id,
        "paymentType": "DB",                # DB=debit (sale). Use PA for preauth if you plan to capture later.
        "successUrl": "https://your.app/pay/success",
        "failUrl":    "https://your.app/pay/fail",
        "notifyUrl":  "https://your.app/webhooks/peach"
    }

    # Endpoint varies by product; for card Checkout v1 you’ll post to card API.
    # Replace '/payments' with the exact create endpoint your Peach account uses.
    resp = requests.post(f"{PEACH_BASE}/payments", headers=peach_headers(), json=payload, timeout=20)
    if resp.status_code >= 300:
        return jsonify({"error": "peach_error", "detail": resp.text}), 502

    rj = resp.json()
    # Expect a redirect/checkout URL or an id + redirect URL depending on flow
    checkout_url = rj.get("redirectUrl") or rj.get("checkoutUrl")
    if not checkout_url:
        return jsonify({"error": "no_checkout_url", "detail": rj}), 502

    return jsonify({"url": checkout_url})

@app.post("/webhooks/peach")
def webhooks_peach():
    # Verify webhook signature using your secret token (HMAC, per Peach docs)
    signature = request.headers.get("X-Peach-Signature") or request.headers.get("X-Signature")
    raw = request.get_data()
    if not signature:
        abort(400)

    expected = hmac.new(SECRET_TOKEN.encode("utf-8"), raw, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, signature):
        abort(401)

    event = request.get_json()
    # Typical events: state change of DB/PA/RF (debit, preauth, refund). Use txn id to look up local order.
    # Update your DB based on event["status"] / event["result"]["code"] etc.
    # e.g., mark paid when status = "SUCCESS" (exact fields per Checkout/Webhooks doc)
    # Finally acknowledge:
    return "", 200

@app.get("/pay/success")
def pay_success():
    # UI page – final confirmation after redirect. Actual truth comes from webhook.
    return "Thanks! We’re confirming your payment. You’ll get an email shortly.", 200

@app.get("/pay/fail")
def pay_fail():
    return "Payment failed or was cancelled. Please try again.", 200

if __name__ == "__main__":
    app.run(debug=True)
