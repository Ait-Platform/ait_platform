# app/payments/paystack.py
import os
import requests
import hmac
import hashlib
import json
from datetime import datetime
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

from app.extensions import db, csrf
from app.models.auth import User
from app.models.payment import PaystackPayment

from app.payments.quote import build_amount_quote
from app.payments.pricing import get_subject_price, countries_from_ref_with_names, _resolve_subject_from_request

paystack_bp = Blueprint("paystack_bp", __name__)

@paystack_bp.route("/checkout/review", endpoint="checkout_review")
def checkout_review():
    subject_id, subject_slug = _resolve_subject_from_request()
    if not subject_slug:
        flash("Subject not specified.", "error")
        return redirect(url_for("public_bp.welcome"))

    countries = countries_from_ref_with_names()
    
    country = session.get("pp_country")
    discounted = session.get("pp_discount", False)
    
    price_obj = {
        "has_quote": False,
        "is_discount": discounted,
        "country_code": country,
        "local_amount": None,
        "local_currency": None,
        "estimated_zar": None,
        "fx_rate": 1.0,
    }

    if country:
        quote = build_amount_quote(subject_slug, country, discounted)
        if quote:
            price_obj["has_quote"] = True
            price_obj["local_amount"] = quote.get("local_cents", 0) / 100.0 if quote.get("local_cents") else None
            price_obj["local_currency"] = quote.get("local_currency")
            
            zar_cents = quote.get("final_zar_cents") or quote.get("anchor_zar_cents") or 0
            price_obj["estimated_zar"] = zar_cents / 100.0 if zar_cents else None

            # Calculate effective FX rate for display
            if price_obj["local_amount"] and price_obj["estimated_zar"] and price_obj["local_amount"] > 0:
                price_obj["fx_rate"] = price_obj["estimated_zar"] / price_obj["local_amount"]

    return render_template(
        "payments/pricing.html",
        subject_id=subject_id,
        subject_slug=subject_slug,
        price=price_obj,
        countries=countries,
        loss_free=False
    )

@paystack_bp.post("/pricing/lock", endpoint="pricing_lock")
def pricing_lock():
    subject_id = request.form.get("subject_id")
    country = request.form.get("country")
    
    if not subject_id or not country:
        flash("Please select a country to continue.", "error")
        return redirect(request.referrer or url_for("public_bp.welcome"))

    # Lock country in session
    session["pp_country"] = country.strip().upper()
    session.modified = True

    return redirect(url_for("paystack_bp.checkout_review", subject_id=subject_id))

@paystack_bp.post("/checkout/cancel", endpoint="checkout_cancel")
def checkout_cancel():
    # Clear the quote lock
    session.pop("pp_country", None)
    session.pop("pp_discount", None)
    
    subject_slug = request.form.get("subject_slug") or session.get("pending_subject") or "loss"
    return redirect(url_for("auth_bp.bridge_dashboard", subject=subject_slug))

def get_paystack_secret(subject=None):
    is_live = False
    if subject:
        base_subject = subject.replace("_topup", "")
        val = db.session.execute(
            text("SELECT value FROM system_settings WHERE LOWER(key) = LOWER(:k)"),
            {"k": f"paystack_mode_{base_subject}"} 
        ).scalar()
        if (val or "sandbox").lower() == "live":
            is_live = True
    else:
        # Webhook / fallback
        is_live = os.environ.get("FLASK_ENV") == "production"

    if is_live:
        return os.environ.get("PAYSTACK_LIVE_KEY", os.environ.get("PAYSTACK_SECRET_KEY"))
    return os.environ.get("PAYSTACK_TEST_KEY", os.environ.get("PAYSTACK_SECRET_KEY", "sk_test_2e6012a225612c95a187a8b706d401de89e75bbe"))

def create_paystack_transaction(amount_cents, display_name, subject, email, currency_code="ZAR", tokens_purchased=None):
    secret_key = get_paystack_secret(subject)
    
    url = "https://api.paystack.co/transaction/initialize"
    
    headers = {
        "Authorization": f"Bearer {secret_key}",
        "Content-Type": "application/json"
    }
    
    # Paystack expects amounts in cents/kobo
    amount_in_lowest_denom = int(amount_cents)
    
    payload = {
        "email": email,
        "amount": amount_in_lowest_denom,
        "currency": currency_code,
        "callback_url": url_for("paystack_bp.paystack_success", subject=subject, email=email, _external=True),
        "metadata": {
            "subject": subject,
            "email": email,
            "tokens_purchased": tokens_purchased,
            "custom_fields": [
                {
                    "display_name": "Product Name",
                    "variable_name": "product_name",
                    "value": display_name
                }
            ]
        }
    }
    
    try:
        r = requests.post(url, json=payload, headers=headers)
        r.raise_for_status()
        data = r.json()
        if data.get("status"):
            return data["data"]["authorization_url"], data["data"]["reference"], None
        else:
            return None, None, data.get("message", "Unknown Paystack Error")
    except Exception as e:
        err_msg = str(e)
        if hasattr(e, "response") and getattr(e, "response") is not None:
            err_msg = e.response.text
            current_app.logger.error(f"Paystack Init Error: {err_msg}")
        return None, None, err_msg


@paystack_bp.route("/start", methods=["GET", "POST"], endpoint="paystack_start")
def start():
    current_app.logger.warning("=" * 60)
    current_app.logger.warning("PAYSTACK START ROUTE EXECUTED")
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

    # Resolve amount
    amount_cents = 0
    currency = "ZAR"
    
    if subject == "metro_billing":
        amount_cents = int(session.get("metro_billing_amount_cents", 0))

    if subject == "debtors_topup" or subject.endswith("_topup"):
        amount_cents = int(session.get("topup_amount_cents", 0))
        currency = session.get("topup_currency", "ZAR")
        
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
                if row.get("zar_amount_cents") and int(row["zar_amount_cents"]) > 0:
                    amount_cents = int(row["zar_amount_cents"])

    if amount_cents <= 0:
        if session.get("subject_slug") == subject and session.get("zar_amount_cents"):
            amount_cents = int(session.get("zar_amount_cents"))

    if amount_cents <= 0:
        ctx = session.get("reg_ctx", {})
        quote = ctx.get("quote", {})
        if quote:
            fallback = quote.get("est_zar_cents") or quote.get("zar_amount_cents") or quote.get("anchor_zar_cents")
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
            currency = "ZAR"
        else:
            amount_cents = amount_cents // 3

    if amount_cents < 500:
        flash(f"Payment cannot proceed: invalid amount (R{amount_cents/100:.2f}). Minimum is R5.00.", "error")
        return redirect(url_for("public_bp.welcome"))

    display_name = subject.replace("_", " ").title()
    
    # Retrieve explicitly set tokens if this is a token top-up
    tokens_purchased = session.get("topup_tokens")
    
    auth_url, ref, err_msg = create_paystack_transaction(amount_cents, display_name, subject, email, currency_code=currency, tokens_purchased=tokens_purchased)
    
    if not auth_url:
        flash(f"Paystack API Error: {err_msg}", "error")
        return redirect(url_for("public_bp.welcome"))

    return redirect(auth_url)


@paystack_bp.get("/success", endpoint="paystack_success")
def success():
    # Callback from Paystack
    # Actual fulfillment should be done in the webhook, but we do a synchronous check here as a fallback
    email = request.args.get("email", "").strip().lower()
    subject = request.args.get("subject", "loss").strip().lower()
    reference = request.args.get("reference") or request.args.get("trxref")
    
    if reference:
        try:
            secret = get_paystack_secret(subject)
            headers = {"Authorization": f"Bearer {secret}"}
            r = requests.get(f"https://api.paystack.co/transaction/verify/{reference}", headers=headers, timeout=10)
            if r.status_code == 200:
                data = r.json()
                if data.get("status") and data.get("data", {}).get("status") == "success":
                    tx_data = data["data"]
                    # Check if already fulfilled
                    from app.models.payment import PaystackPayment
                    existing = PaystackPayment.query.filter_by(gateway_reference=reference).first()
                    if not existing:
                        meta_email = tx_data.get("metadata", {}).get("email") or tx_data.get("customer", {}).get("email", "")
                        meta_subject = tx_data.get("metadata", {}).get("subject", "")
                        if meta_email and meta_subject:
                            fulfill_order(meta_email.strip().lower(), meta_subject.strip().lower(), tx_data)
                            flash("Payment verified and applied successfully!", "success")
        except Exception as e:
            current_app.logger.error(f"Sync fallback error: {e}")
    
    if not email:
        flash("Payment processing... Please sign in to continue.", "info")
    
    return render_template("payments/success.html", subject=subject), 200


@paystack_bp.post("/webhook", endpoint="paystack_webhook")
@csrf.exempt
def webhook():
    log_entry = {"time": str(datetime.utcnow()), "event": "paystack_webhook"}
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
        raw_body = request.get_data()
        signature = request.headers.get("x-paystack-signature")
        
        # Extract subject to get correct secret (sandbox vs live)
        data = request.get_json(silent=True) or {}
        subject = None
        try:
            subject = data.get("data", {}).get("metadata", {}).get("subject")
        except Exception:
            pass
            
        secret = get_paystack_secret(subject)

        # Validate signature
        hash = hmac.new(secret.encode('utf-8'), raw_body, hashlib.sha512).hexdigest()
        if hash != signature:
            log_entry["error"] = "Invalid Paystack signature"
            write_log()
            return "Invalid signature", 401

        data = request.json
        event_type = data.get("event")
        
        if event_type == "charge.success":
            tx_data = data.get("data", {})
            metadata = tx_data.get("metadata", {})
            
            email = metadata.get("email") or tx_data.get("customer", {}).get("email", "")
            subject = metadata.get("subject", "")
            email = email.strip().lower()
            subject = subject.strip().lower()

            if email and subject:
                try:
                    fulfill_order(email, subject, tx_data)
                    log_entry["fulfill_success"] = True
                except Exception as e:
                    log_entry["fulfill_error"] = str(e)
            else:
                log_entry["skip_reason"] = "Missing email or subject in metadata"
                
    except Exception as e:
        log_entry["fatal_error"] = str(e)
        
    write_log()
    return jsonify({"status": "ok"}), 200


def fulfill_order(email, subject, transaction=None):
    u = User.query.filter_by(email=email).first()
    if not u:
        u = User(email=email, name=email.split("@", 1)[0].title(), is_active=1)
        db.session.add(u)
        db.session.flush()

    if transaction:
        payment = PaystackPayment(
            user_id=u.id,
            email=email,
            subject_slug=subject,
            amount_cents=int(transaction.get("amount", 0)),
            currency=transaction.get("currency", "ZAR"),
            status=transaction.get("status", "success"),
            gateway_reference=transaction.get("reference", ""),
            paid_at=datetime.utcnow()
        )
        db.session.add(payment)
        # Flush so the ledger gets recorded before returning from any module handlers below
        db.session.flush()

    # ---------- MECHANIC TOPUP ----------
    if subject == "mechanic_topup":
        from app.models.mechanic import MechShop
        shop = MechShop.query.filter_by(user_id=u.id).first()
        if shop and transaction:
            total = int(transaction.get("amount", 0))
            if total > 0:
                shop.wallet_balance_cents += total
                db.session.commit()
        return

    # ---------- UNIVERSAL TOKEN TOPUP ----------
    if subject.endswith("_topup"):
        from app.models.auth import AitTokenWallet, AitTokenTransaction
        total = int(transaction.get("amount", 0)) if transaction else 0
        if total > 0:
            tokens_purchased = transaction.get("metadata", {}).get("tokens_purchased")
            if tokens_purchased is None:
                tokens_purchased = total // 100 # Legacy fallback
            
            wallet = AitTokenWallet.query.filter_by(user_id=u.id).first()
            if not wallet:
                wallet = AitTokenWallet(user_id=u.id, balance=0)
                db.session.add(wallet)
                db.session.flush()
                
            wallet.balance += int(tokens_purchased)
            tx = AitTokenTransaction(
                wallet_id=wallet.id,
                transaction_type="purchase",
                amount=int(tokens_purchased),
                description=f"Paystack Top-Up ({subject.replace('_topup', '').replace('_', ' ').title()})",
                reference=transaction.get("reference") if transaction else "paystack_tx"
            )
            db.session.add(tx)
            db.session.commit()
        return

    # ---------- METRO BILLING (DEBTORS) SUBSCRIPTION ----------
    if subject == "metro_billing":
        pass

    # ---------- STANDARD MODULES ----------
    lookup_subject = subject
    
    sid = db.session.execute(
        text("SELECT id FROM auth_subject WHERE lower(slug) = :s OR lower(name) = :s LIMIT 1"),
        {"s": lookup_subject},
    ).scalar()

    if sid:
        subj_paid_days = db.session.execute(
            text("SELECT paid_days FROM auth_subject WHERE id = :sid LIMIT 1"),
            {"sid": int(sid)}
        ).scalar()
        
        expires_at_val = None
        if subj_paid_days and int(subj_paid_days) > 0:
            from datetime import timedelta
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
                "ref": transaction.get("reference") if transaction else "paystack_tx",
                "vu": expires_at_val,
                "eid": eid,
                "lcur": local_cur,
                "l_amt": local_cents,
                "pid": price_id,
                "cc": cc
            }
        )
            
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
    current_app.logger.info(f"Fulfilled {subject} for {email} via Paystack.")
