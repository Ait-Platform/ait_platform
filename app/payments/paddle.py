# app/payments/paddle.py
import os
import json
import requests
from decimal import Decimal
from flask import (
    Blueprint,
    request,
    session,
    render_template,
    flash,
    current_app,
    redirect,
    url_for,
    jsonify
)
from flask_login import current_user
from sqlalchemy import text

from app.extensions import db
from app.models.auth import User

paddle_bp = Blueprint("paddle_bp", __name__)

def create_paddle_transaction(amount_cents, display_name, paddle_env, subject, email):
    api_key = os.environ.get("PADDLE_LIVE_API_KEY") if paddle_env == "production" else os.environ.get("PADDLE_SANDBOX_API_KEY")
    if not api_key:
        api_key = os.environ.get("PADDLE_API_KEY", "")
        
    if not api_key:
        current_app.logger.error(f"PADDLE_API_KEY is not set for {paddle_env}.")
        return None

    base_url = "https://api.paddle.com" if paddle_env == "production" else "https://sandbox-api.paddle.com"
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "items": [
            {
                "price": {
                    "description": f"{display_name} Access",
                    "unit_price": {
                        "amount": str(amount_cents),
                        "currency_code": "ZAR"
                    },
                    "product": {
                        "name": display_name,
                        "tax_category": "standard"
                    }
                },
                "quantity": 1
            }
        ],
        "custom_data": {
            "subject": subject,
            "email": email
        }
    }
    
    try:
        r = requests.post(f"{base_url}/transactions", json=payload, headers=headers)
        r.raise_for_status()
        data = r.json().get("data", {})
        return data.get("id")
    except Exception as e:
        current_app.logger.error(f"Paddle Transaction Error: {str(e)}")
        if hasattr(e, "response") and getattr(e, "response") is not None:
            current_app.logger.error(e.response.text)
        return None

@paddle_bp.route("/start", methods=["GET", "POST"], endpoint="paddle_start")
def start():
    """
    Renders the Paddle Hosted Checkout page with dynamic inline pricing.
    """
    current_app.logger.warning("=" * 60)
    current_app.logger.warning("PADDLE START ROUTE EXECUTED")
    current_app.logger.warning("=" * 60)

    email = (
        request.values.get("email") 
        or (getattr(current_user, "email", None) if current_user.is_authenticated else None)
        or session.get("pending_email") 
        or session.get("reg_ctx", {}).get("email_lower") 
        or session.get("reg_ctx", {}).get("email_in") 
        or ""
    ).strip().lower()
    subject = (request.values.get("subject") or session.get("pending_subject") or session.get("reg_ctx", {}).get("subject") or "").strip().lower()

    if email:
        session["pending_email"] = email
    if subject:
        session["pending_subject"] = subject

    if not subject:
        flash("Could not determine which module to purchase.", "error")
        return redirect(url_for("public_bp.welcome"))

    # Resolve amount exactly like Yoco did
    amount_cents = 0
    
    if subject == "spv_registration":
        amount_cents = 10000
        
    if subject == "metro_billing":
        amount_cents = int(session.get("metro_billing_amount_cents", 0))

    if subject == "mechanic_topup":
        amount_cents = int(session.get("mechanic_topup_amount_cents", 0))
        
    if amount_cents <= 0:
        u = User.query.filter_by(email=email).first()
        if u:
            row = db.session.execute(
                text("""
                    SELECT zar_amount_cents
                    FROM user_enrollment
                    WHERE user_id = :uid 
                      AND subject_id = (SELECT id FROM auth_subject WHERE lower(slug) = :s LIMIT 1)
                    ORDER BY id DESC LIMIT 1
                """),
                {"uid": u.id, "s": subject}
            ).scalar()
            if row and int(row) > 0:
                amount_cents = int(row)

    if amount_cents <= 0:
        if session.get("subject_slug") == subject and session.get("zar_amount_cents"):
            amount_cents = int(session.get("zar_amount_cents"))

    if amount_cents <= 0:
        ctx = session.get("reg_ctx", {})
        quote = ctx.get("quote", {})
        if quote:
            fallback = quote.get("est_zar_cents") or quote.get("zar_amount_cents")
            if fallback:
                amount_cents = int(fallback)
            
    if amount_cents <= 0:
        from app.payments.pricing import get_subject_price
        price_info = get_subject_price(subject)
        if price_info and price_info["amount_cents"] > 0:
            amount_cents = int(price_info["amount_cents"])
            
    if session.get('is_retake'):
        retake_zar = session.get('retake_zar_cents')
        if retake_zar:
            amount_cents = int(retake_zar)
        else:
            amount_cents = amount_cents // 3

    if amount_cents < 1000:
        flash(f"Payment cannot proceed: invalid amount (R{amount_cents/100:.2f}). Minimum is R10.00.", "error")
        return redirect(url_for("public_bp.welcome"))

    # Find the environment from DB
    val = db.session.execute(
        text("SELECT value FROM system_settings WHERE key = :k"),
        {"k": f"yoco_mode_{subject}"} # Reusing the same setting key for backward compatibility
    ).scalar()

    raw_env = (val or "sandbox").lower()
    paddle_env = "production" if raw_env == "live" else "sandbox"

    # Pass the client token to the template
    if paddle_env == "production":
        client_token = os.environ.get("PADDLE_LIVE_CLIENT_TOKEN") or os.environ.get("PADDLE_CLIENT_TOKEN")
    else:
        client_token = os.environ.get("PADDLE_SANDBOX_CLIENT_TOKEN") or os.environ.get("PADDLE_CLIENT_TOKEN")
    client_token = client_token or "test_YOUR_CLIENT_TOKEN"

    success_url = url_for("paddle_bp.paddle_success", subject=subject, email=email, _external=True)

    # Paddle expects a clean product name for display
    display_name = subject.replace("_", " ").title()
    
    transaction_id = create_paddle_transaction(amount_cents, display_name, paddle_env, subject, email)
    
    if not transaction_id:
        flash("Could not initiate Paddle checkout. Please contact support.", "error")
        return redirect(url_for("public_bp.welcome"))

    return render_template(
        "payments/paddle_checkout.html",
        client_token=client_token,
        environment=paddle_env,
        transaction_id=transaction_id,
        email=email,
        subject=subject,
        display_name=display_name,
        amount_cents=amount_cents,
        currency="ZAR",
        success_url=success_url
    )


@paddle_bp.get("/success", endpoint="paddle_success")
def success():
    # Paddle redirects here after a successful frontend checkout.
    # We just show the success page and wait for the webhook to fulfill the order.
    # Note: If it's a one-time product, Paddle's webhook might arrive *after* the user sees this page.
    # To mimic Yoco exactly, we could fulfill it here, but Webhooks are much safer.
    
    email = request.args.get("email", "").strip().lower()
    subject = request.args.get("subject", "loss").strip().lower()
    
    # We use the same success template as before
    if not email:
        flash("Payment processing... Please sign in to continue.", "info")
    
    return render_template("payments/success.html", subject=subject), 200


@paddle_bp.post("/webhook", endpoint="paddle_webhook")
def webhook():
    """
    Paddle Billing Server-to-Server Webhook.
    Listens for transaction.completed events to fulfill orders.
    """
    import hmac
    import hashlib
    
    # 1. Verify signature (Paddle specific)
    # The signature is in the Paddle-Signature header: ts=...,h1=...
    signature_header = request.headers.get("Paddle-Signature")
    
    webhook_secrets = [
        os.environ.get("PADDLE_SANDBOX_WEBHOOK_SECRET"),
        os.environ.get("PADDLE_LIVE_WEBHOOK_SECRET"),
        os.environ.get("PADDLE_WEBHOOK_SECRET")
    ]
    webhook_secrets = [s for s in webhook_secrets if s]
    
    is_valid = False
    
    if signature_header and webhook_secrets:
        # Quick validation logic
        parts = dict(x.split("=") for x in signature_header.split(";"))
        ts = parts.get("ts")
        h1 = parts.get("h1")
        if ts and h1:
            signed_payload = f"{ts}:{request.get_data(as_text=True)}"
            for secret in webhook_secrets:
                expected_h1 = hmac.new(
                    secret.encode('utf-8'),
                    signed_payload.encode('utf-8'),
                    hashlib.sha256
                ).hexdigest()
                if h1 == expected_h1:
                    is_valid = True
                    break
                    
    if not is_valid and signature_header:
        current_app.logger.error("Paddle webhook signature mismatch.")
        return "Invalid signature", 401
    
    try:
        data = request.json
        event_type = data.get("event_type")
        
        if event_type == "transaction.completed":
            transaction = data.get("data", {})
            custom_data = transaction.get("custom_data", {})
            
            email = custom_data.get("email", "").strip().lower()
            subject = custom_data.get("subject", "").strip().lower()
            
            if email and subject:
                fulfill_order(email, subject, transaction)
                
        return {"status": "ok"}, 200
        
    except Exception as e:
        current_app.logger.error(f"Error processing Paddle webhook: {e}")
        return "Internal Error", 500


def fulfill_order(email, subject, transaction=None):
    """
    Extracted from yoco.py success endpoint.
    This fulfills the purchase for the user.
    """
    # Ensure user exists
    u = User.query.filter_by(email=email).first()
    if not u:
        u = User(email=email, name=email.split("@", 1)[0].title(), is_active=1)
        db.session.add(u)
        db.session.flush()

    # ---------- MECHANIC TOPUP ----------
    if subject == "mechanic_topup":
        from app.models.mechanic import MechShop
        shop = MechShop.query.filter_by(user_id=u.id).first()
        if shop and transaction:
            # We assume custom_data or transaction holds the original amount, or we extract it.
            # Since Paddle transactions contain the total, we can use that.
            total = int(transaction.get("details", {}).get("totals", {}).get("grand_total", 0))
            if total > 0:
                shop.wallet_balance_cents += total
                db.session.commit()
        return

    # ---------- CFI TOPUP ----------
    if subject == "cultural_fire_topup":
        from app.models.auth import AitTokenWallet, AitTokenTransaction
        total = int(transaction.get("details", {}).get("totals", {}).get("grand_total", 0)) if transaction else 0
        if total > 0:
            tokens_purchased = total // 100
            wallet = AitTokenWallet.query.filter_by(user_id=u.id).first()
            if not wallet:
                wallet = AitTokenWallet(user_id=u.id, balance=0)
                db.session.add(wallet)
                db.session.flush()
            wallet.balance += tokens_purchased
            tx = AitTokenTransaction(
                wallet_id=wallet.id,
                transaction_type="purchase",
                amount=tokens_purchased,
                description="Paddle Top-Up",
                reference=transaction.get("id") if transaction else "paddle_tx"
            )
            db.session.add(tx)
            db.session.commit()
        return

    # ---------- METRO BILLING (DEBTORS) SUBSCRIPTION ----------
    if subject == "metro_billing":
        # Usually handled by UserEntitlement or similar logic. We mimic Yoco's behavior.
        pass

    # ---------- SPV ----------
    if subject == "spv_registration":
        u.is_investor = 1
        db.session.commit()
        return

    # ---------- STANDARD MODULES ----------
    lookup_subject = "spv" if subject.lower() == "spv_registration" else subject
    
    sid = db.session.execute(
        text("SELECT id FROM auth_subject WHERE lower(slug) = :s OR lower(name) = :s LIMIT 1"),
        {"s": lookup_subject},
    ).scalar()

    if sid:
        # Get subject's paid_days
        subj_paid_days = db.session.execute(
            text("SELECT paid_days FROM auth_subject WHERE id = :sid LIMIT 1"),
            {"sid": int(sid)}
        ).scalar()
        
        expires_at_val = None
        if subj_paid_days and int(subj_paid_days) > 0:
            from datetime import datetime, timedelta
            expires_at_val = datetime.utcnow() + timedelta(days=int(subj_paid_days))

        existing = db.session.execute(
            text("""
            SELECT id, status, zar_amount_cents, local_currency, local_amount_cents, country_code, price_id
              FROM user_enrollment
             WHERE user_id   = :uid
               AND subject_id = :sid
             ORDER BY id DESC LIMIT 1
            """),
            {"uid": int(u.id), "sid": int(sid)},
        ).first()

        new_status = 'paid' if expires_at_val is None else 'active'
        if existing:
            db.session.execute(
                text("""
                    UPDATE user_enrollment
                       SET status = :st,
                           trial_end = NULL,
                           expires_at = :exp
                     WHERE id = :eid
                """),
                {"eid": existing.id, "st": new_status, "exp": expires_at_val},
            )
            eid = existing.id
            zar_cents = existing.zar_amount_cents
            local_cur = existing.local_currency
            local_cents = existing.local_amount_cents
            price_id = existing.price_id
            cc = existing.country_code
        else:
            new_enr = db.session.execute(
                text("""
                    INSERT INTO user_enrollment (user_id, subject_id, status, expires_at)
                    VALUES (:uid, :sid, :st, :exp)
                    RETURNING id
                """),
                {"uid": int(u.id), "sid": int(sid), "st": new_status, "exp": expires_at_val},
            ).fetchone()
            eid = new_enr[0]
            zar_cents = 0
            local_cur = None
            local_cents = 0
            price_id = None
            cc = None
            
        # Log the payment to AuthPaymentLog
        db.session.execute(
            text("""
                INSERT INTO auth_payment_log (
                    user_id, program, amount, currency, transaction_id, status, 
                    valid_from, valid_until, enrollment_id, local_currency, 
                    local_amount_cents, price_id, country_code
                ) VALUES (
                    :uid, :prog, :amt, 'ZAR', :ref, 'success',
                    CURRENT_TIMESTAMP, :vu, :eid, :lcur, 
                    :l_amt, :pid, :cc
                )
            """),
            {
                "uid": int(u.id),
                "prog": subject,
                "amt": (zar_cents / 100.0) if zar_cents else 0,
                "ref": transaction.get("id") if transaction else "paddle_tx",
                "vu": expires_at_val,
                "eid": eid,
                "lcur": local_cur,
                "l_amt": local_cents,
                "pid": price_id,
                "cc": cc
            }
        )
            
        # Provision CFI Tokens if the subject is Cultural Fire
        if subject.lower() in ("cultural_fire", "culturalfire"):
            from app.models.auth import AitTokenWallet, AitTokenTransaction
            wallet = AitTokenWallet.query.filter_by(user_id=u.id).first()
            if not wallet:
                wallet = AitTokenWallet(user_id=u.id, balance=0)
                db.session.add(wallet)
                db.session.flush()
            
            wallet.balance += 200
            txn = AitTokenTransaction(
                wallet_id=wallet.id,
                amount=200,
                description="Initial Registration Bundle (200 Tokens)"
            )
            db.session.add(txn)
            
    db.session.commit()
    current_app.logger.info(f"Fulfilled {subject} for {email} via Paddle.")
