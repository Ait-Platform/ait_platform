# app/payments/payfast.py
import os
import hashlib
from urllib.parse import urlencode
from flask import (
    Blueprint, request, session, render_template,
    flash, current_app, redirect, url_for, jsonify
)
from flask_login import current_user
from sqlalchemy import text
from app.extensions import db
from app.models.auth import User
from app.payments.quote import build_amount_quote
from app.payments.pricing import get_subject_price, countries_from_ref_with_names, _resolve_subject_from_request

payfast_bp = Blueprint("payfast_bp", __name__)

@payfast_bp.route("/checkout/review", endpoint="checkout_review")
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

@payfast_bp.post("/pricing/lock", endpoint="pricing_lock")
def pricing_lock():
    subject_id = request.form.get("subject_id")
    country = request.form.get("country")
    
    if not subject_id or not country:
        flash("Please select a country to continue.", "error")
        return redirect(request.referrer or url_for("public_bp.welcome"))

    # Lock country in session
    session["pp_country"] = country.strip().upper()
    session.modified = True

    return redirect(url_for("payfast_bp.checkout_review", subject_id=subject_id))

@payfast_bp.post("/checkout/cancel", endpoint="checkout_cancel")
def checkout_cancel():
    # Clear the quote lock
    session.pop("pp_country", None)
    session.pop("pp_discount", None)
    
    subject_slug = request.form.get("subject_slug") or session.get("pending_subject") or "loss"
    return redirect(url_for("auth_bp.bridge_dashboard", subject=subject_slug))


# --- Payfast Integration ---

def generate_payfast_signature(data: dict, passphrase: str = None) -> str:
    """Generate MD5 signature for PayFast."""
    pf_string = ""
    for key in data:
        if data[key] != "" and data[key] is not None:
            pf_string += f"{key}={data[key]}&"
    
    pf_string = pf_string[:-1] # Remove last '&'
    
    if passphrase:
        pf_string += f"&passphrase={passphrase}"
        
    return hashlib.md5(pf_string.encode('utf-8')).hexdigest()

@payfast_bp.route("/start", methods=["GET", "POST"], endpoint="payfast_start")
def payfast_start():
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

    # Fallback to session
    if amount_cents <= 0:
        country = session.get("pp_country")
        discounted = session.get("pp_discount", False)
        if country:
            quote = build_amount_quote(subject, country, discounted)
            if quote:
                # PayFast ALWAYS charges in ZAR. 
                amount_cents = quote.get("final_zar_cents") or quote.get("anchor_zar_cents") or 0
                currency = "ZAR"
                
    if amount_cents <= 0:
        price_info = get_subject_price(subject)
        if price_info and price_info["amount_cents"] > 0:
            amount_cents = int(price_info["amount_cents"])
            currency = "ZAR"

    if session.get('is_retake'):
        retake_zar = session.get('retake_zar_cents')
        if retake_zar:
            amount_cents = int(retake_zar)
            currency = "ZAR"
        else:
            amount_cents = amount_cents // 3

    if amount_cents < 500: # R5.00 min for Payfast
        flash(f"Payment cannot proceed: invalid amount (R{amount_cents/100:.2f}). Minimum is R5.00.", "error")
        return redirect(url_for("public_bp.welcome"))

    # PayFast settings
    val = db.session.execute(
        text("SELECT value FROM system_settings WHERE key = :k"),
        {"k": f"payfast_mode_{subject}"} 
    ).scalar()

    raw_env = (val or "sandbox").lower()
    is_live = (raw_env == "live")
    
    merchant_id = os.environ.get("PAYFAST_LIVE_MERCHANT_ID") if is_live else os.environ.get("PAYFAST_SANDBOX_MERCHANT_ID")
    merchant_key = os.environ.get("PAYFAST_LIVE_MERCHANT_KEY") if is_live else os.environ.get("PAYFAST_SANDBOX_MERCHANT_KEY")
    passphrase = os.environ.get("PAYFAST_LIVE_PASSPHRASE") if is_live else os.environ.get("PAYFAST_SANDBOX_PASSPHRASE")

    if not merchant_id or not merchant_key:
        flash("PayFast credentials are not fully configured on this environment.", "error")
        return redirect(url_for("public_bp.welcome"))

    pf_host = "www.payfast.co.za" if is_live else "sandbox.payfast.co.za"
    
    amount_rands = amount_cents / 100.0

    return_url = url_for("payfast_bp.payfast_success", subject=subject, email=email, _external=True)
    cancel_url = url_for("payfast_bp.payfast_cancel", _external=True)
    notify_url = url_for("payfast_bp.payfast_itn", _external=True)

    data = {
        "merchant_id": merchant_id,
        "merchant_key": merchant_key,
        "return_url": return_url,
        "cancel_url": cancel_url,
        "notify_url": notify_url,
        "name_first": email.split("@")[0].title() if email else "Learner",
        "email_address": email,
        "m_payment_id": f"{subject}_{int(datetime.utcnow().timestamp())}",
        "amount": f"{amount_rands:.2f}",
        "item_name": subject.replace("_", " ").title() + " Access",
    }
    
    data["signature"] = generate_payfast_signature(data, passphrase)

    html_form = f"""
    <!DOCTYPE html>
    <html>
    <head><title>Redirecting to PayFast...</title></head>
    <body onload="document.pfForm.submit()">
        <p>Redirecting to PayFast secure checkout, please wait...</p>
        <form name="pfForm" action="https://{pf_host}/eng/process" method="post">
    """
    for k, v in data.items():
        html_form += f'<input type="hidden" name="{k}" value="{v}">'
    html_form += """
        </form>
    </body>
    </html>
    """
    
    return html_form

@payfast_bp.route("/success", endpoint="payfast_success")
def payfast_success():
    email = request.args.get("email", "").strip().lower()
    subject = request.args.get("subject", "loss").strip().lower()
    return render_template("payments/success.html", subject=subject), 200

@payfast_bp.route("/cancel", endpoint="payfast_cancel")
def payfast_cancel():
    return render_template("payments/cancelled.html"), 200


from app.extensions import csrf
from datetime import datetime

@payfast_bp.post("/itn", endpoint="payfast_itn")
@csrf.exempt
def payfast_itn():
    """
    PayFast Instant Transaction Notification (ITN) webhook.
    """
    import os, urllib.parse
    
    pf_data = request.form.to_dict()
    
    # Verify ITN with PayFast
    m_payment_id = pf_data.get('m_payment_id', '')
    subject = m_payment_id.split('_')[0] if m_payment_id else 'loss'
    email = pf_data.get('email_address', '')
    
    val = db.session.execute(
        text("SELECT value FROM system_settings WHERE key = :k"),
        {"k": f"payfast_mode_{subject}"} 
    ).scalar()
    is_live = ((val or "sandbox").lower() == "live")
    
    pf_host = "www.payfast.co.za" if is_live else "sandbox.payfast.co.za"
    
    # To validate the ITN, we must post it back to PayFast
    # Payfast recommends this validation step.
    query_string = urllib.parse.urlencode(pf_data)
    try:
        import requests
        resp = requests.post(
            f"https://{pf_host}/eng/query/validate",
            data=query_string,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=10
        )
        if resp.text != "VALID":
            current_app.logger.error(f"PayFast ITN Validation Failed: {resp.text}")
            return "Invalid ITN", 400
    except Exception as e:
        current_app.logger.error(f"PayFast ITN Validation Error: {str(e)}")
        return "Validation Error", 500

    payment_status = pf_data.get('payment_status')
    
    if payment_status == "COMPLETE":
        try:
            fulfill_order(email, subject, pf_data)
        except Exception as e:
            current_app.logger.error(f"PayFast Fulfillment Error: {str(e)}")
            
    return jsonify({"status": "ok"}), 200

def fulfill_order(email, subject, transaction):
    # Duplicated logic from paddle.py / yoco.py to fulfill the order
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
            total = int(float(transaction.get('amount_gross', 0)) * 100)
            if total > 0:
                shop.wallet_balance_cents += total
                db.session.commit()
        return

    # ---------- PRACTICE CRM TOPUP ----------
    if subject == "practice_crm_topup":
        from app.models.practice_crm import CrmPractice
        practice = CrmPractice.query.filter_by(owner_id=u.id).first()
        if practice and transaction:
            total = int(float(transaction.get('amount_gross', 0)) * 100)
            if total > 0:
                practice.wallet_balance_cents += total
                db.session.commit()
        return

    # ---------- CFI TOPUP ----------
    if subject == "cultural_fire_topup":
        from app.models.auth import AitTokenWallet, AitTokenTransaction
        total = int(float(transaction.get('amount_gross', 0)) * 100)
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
                description="PayFast Top-Up",
                reference=transaction.get('pf_payment_id') or "payfast_tx"
            )
            db.session.add(tx)
            db.session.commit()
        return

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
            zar_cents = int(float(transaction.get('amount_gross', 0)) * 100)
            local_cur = 'ZAR'
            local_cents = zar_cents
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
                "ref": transaction.get('pf_payment_id') or "payfast_tx",
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
    current_app.logger.info(f"Fulfilled {subject} for {email} via PayFast.")
