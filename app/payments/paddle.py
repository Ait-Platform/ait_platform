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

def create_paddle_transaction(amount_cents, display_name, paddle_env, subject, email, currency_code="ZAR"):
    api_key = os.environ.get("PADDLE_LIVE_API_KEY") if paddle_env == "production" else os.environ.get("PADDLE_SANDBOX_API_KEY")
    if not api_key:
        api_key = os.environ.get("PADDLE_API_KEY", "")
        
    if not api_key:
        current_app.logger.error(f"PADDLE_API_KEY is not set for {paddle_env}.")
        return None, f"PADDLE_API_KEY is not set for {paddle_env}."

    base_url = "https://api.paddle.com" if paddle_env == "production" else "https://sandbox-api.paddle.com"
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    currency_code = str(currency_code or "ZAR").strip().upper()
    valid_currencies = ["ARS", "AUD", "BRL", "CAD", "CHF", "CLP", "CNY", "COP", "CZK", "DKK", "EUR", "GBP", "HKD", "HUF", "ILS", "INR", "JPY", "KRW", "MXN", "NOK", "NZD", "PEN", "PLN", "RUB", "SEK", "SGD", "THB", "TRY", "TWD", "UAH", "USD", "VND", "ZAR"]
    if currency_code not in valid_currencies:
        currency_code = "ZAR"

    payload = {
        "items": [
            {
                "price": {
                    "description": f"{display_name} Access",
                    "unit_price": {
                        "amount": str(amount_cents),
                        "currency_code": currency_code
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
        return data.get("id"), None
    except Exception as e:
        err_msg = str(e)
        current_app.logger.error(f"Paddle Transaction Error: {err_msg}")
        if hasattr(e, "response") and getattr(e, "response") is not None:
            err_msg = e.response.text
            current_app.logger.error(err_msg)
        return None, err_msg

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
    currency = "ZAR"
    
    if subject == "metro_billing":
        amount_cents = int(session.get("metro_billing_amount_cents", 0))

    if subject == "mechanic_topup":
        amount_cents = int(session.get("mechanic_topup_amount_cents", 0))
        
    if subject == "practice_crm_topup":
        amount_cents = int(session.get("practice_crm_topup_amount_cents", 0))
        
    if amount_cents <= 0:
        u = User.query.filter_by(email=email).first()
        if u:
            row = db.session.execute(
                text("""
                    SELECT local_amount_cents, local_currency, zar_amount_cents
                    FROM user_enrollment
                    WHERE user_id = :uid 
                      AND subject_id = (SELECT id FROM auth_subject WHERE lower(slug) = :s LIMIT 1)
                    ORDER BY id DESC LIMIT 1
                """),
                {"uid": u.id, "s": subject}
            ).mappings().first()
            if row:
                if row.get("local_amount_cents") and row.get("local_currency"):
                    amount_cents = int(row["local_amount_cents"])
                    currency = row["local_currency"]
                elif row.get("zar_amount_cents") and int(row["zar_amount_cents"]) > 0:
                    amount_cents = int(row["zar_amount_cents"])

    if amount_cents <= 0:
        if session.get("subject_slug") == subject and session.get("zar_amount_cents"):
            if session.get("local_currency") and session.get("local_amount_cents"):
                amount_cents = int(session.get("local_amount_cents"))
                currency = session.get("local_currency")
            else:
                amount_cents = int(session.get("zar_amount_cents"))

    if amount_cents <= 0:
        ctx = session.get("reg_ctx", {})
        quote = ctx.get("quote", {})
        if quote:
            if quote.get("local_cents") and quote.get("local_currency"):
                amount_cents = int(quote["local_cents"])
                currency = quote["local_currency"]
            else:
                fallback = quote.get("est_zar_cents") or quote.get("zar_amount_cents")
                if fallback:
                    amount_cents = int(fallback)
            
    if amount_cents <= 0:
        from app.payments.pricing import get_subject_price
        price_info = get_subject_price(subject)
        if price_info and price_info["amount_cents"] > 0:
            if price_info.get("local_cents") and price_info.get("local_currency"):
                amount_cents = int(price_info["local_cents"])
                currency = price_info["local_currency"]
            else:
                amount_cents = int(price_info["amount_cents"])
            
    if session.get('is_retake'):
        retake_zar = session.get('retake_zar_cents')
        if retake_zar:
            amount_cents = int(retake_zar)
            currency = "ZAR"
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
    
    transaction_id, err_msg = create_paddle_transaction(amount_cents, display_name, paddle_env, subject, email, currency_code=currency)
    
    if not transaction_id:
        flash(f"Paddle API Error: {err_msg}", "error")
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
        currency=currency,
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


from app.extensions import csrf

@paddle_bp.post("/webhook", endpoint="paddle_webhook")
@csrf.exempt
def webhook():
    import hmac
    import hashlib
    import json
    import os
    from datetime import datetime
    
    log_entry = {"time": str(datetime.utcnow()), "event": "webhook_received"}
    log_file = os.path.join(current_app.instance_path, 'webhook_debug.json')
    os.makedirs(current_app.instance_path, exist_ok=True)
    
    def write_log():
        logs = []
        if os.path.exists(log_file):
            try:
                with open(log_file, 'r') as f:
                    logs = json.load(f)
            except: pass
        logs.insert(0, log_entry)
        with open(log_file, 'w') as f:
            json.dump(logs[:20], f, indent=2)

    try:
        raw_body = request.get_data(as_text=True)
        log_entry["raw_body"] = raw_body
        
        signature_header = request.headers.get("Paddle-Signature")
        log_entry["signature_header"] = signature_header
        
        webhook_secrets = [
            os.environ.get("PADDLE_SANDBOX_WEBHOOK_SECRET"),
            os.environ.get("PADDLE_LIVE_WEBHOOK_SECRET"),
            os.environ.get("PADDLE_WEBHOOK_SECRET")
        ]
        webhook_secrets = [s for s in webhook_secrets if s]
        
        is_valid = False
        if signature_header and webhook_secrets:
            parts = dict(x.split("=") for x in signature_header.split(";"))
            ts = parts.get("ts")
            h1 = parts.get("h1")
            if ts and h1:
                signed_payload = f"{ts}:{raw_body}"
                for secret in webhook_secrets:
                    expected_h1 = hmac.new(
                        secret.encode('utf-8'),
                        signed_payload.encode('utf-8'),
                        hashlib.sha256
                    ).hexdigest()
                    if h1 == expected_h1:
                        is_valid = True
                        log_entry["matched_secret"] = secret[:10] + "..."
                        break
                        
        if not is_valid and signature_header:
            log_entry["error"] = "Invalid signature"
            write_log()
            return "Invalid signature", 401
            
        data = request.json
        log_entry["json_data"] = data
        event_type = data.get("event_type")
        
        if event_type == "transaction.completed":
            transaction = data.get("data", {})
            custom_data = transaction.get("custom_data") or {}
            
            email = custom_data.get("email", "").strip().lower()
            subject = custom_data.get("subject", "").strip().lower()
            
            log_entry["parsed_email"] = email
            log_entry["parsed_subject"] = subject
            
            if email and subject:
                try:
                    fulfill_order(email, subject, transaction)
                    log_entry["fulfill_success"] = True
                except Exception as e:
                    log_entry["fulfill_error"] = str(e)
            else:
                log_entry["skip_reason"] = "Missing email or subject"
                
    except Exception as e:
        log_entry["fatal_error"] = str(e)
        
    write_log()
    return jsonify({"status": "ok"}), 200


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

    # ---------- PRACTICE CRM TOPUP ----------
    if subject == "practice_crm_topup":
        from app.models.practice_crm import CrmPractice
        practice = CrmPractice.query.filter_by(owner_id=u.id).first()
        if practice and transaction:
            total = int(transaction.get("details", {}).get("totals", {}).get("grand_total", 0))
            if total > 0:
                # Top up practice wallet
                practice.wallet_balance_cents += total
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

    # ---------- STANDARD MODULES ----------
    lookup_subject = subject
    
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


@paddle_bp.route('/debug')
def paddle_debug():
    from flask_login import current_user
    import os, json
    
    if not current_user.is_authenticated: return 'Not logged in'
    
    enr = db.session.execute(text('SELECT * FROM user_enrollment WHERE user_id = :uid ORDER BY id DESC LIMIT 5'), {'uid': current_user.id}).mappings().all()
    logs = db.session.execute(text('SELECT * FROM auth_payment_log ORDER BY id DESC LIMIT 5')).mappings().all()
    
    webhook_logs = []
    log_file = os.path.join(current_app.instance_path, 'webhook_debug.json')
    if os.path.exists(log_file):
        try:
            with open(log_file, 'r') as f:
                webhook_logs = json.load(f)
        except: pass
        
    return {
        'enrollments': [dict(x) for x in enr], 
        'latest_logs': [dict(x) for x in logs],
        'webhook_logs': webhook_logs
    }

