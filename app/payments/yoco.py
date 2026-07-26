# app/payments/yoco.py
from decimal import Decimal, ROUND_HALF_UP

from flask import (
    Blueprint,
    request,
    session,
    render_template,
    flash,
    current_app,
    redirect,
    url_for,
)
from flask_login import login_user
from sqlalchemy import text

from app.extensions import db
from app.models.auth import User
from app.models.payment import YocoPayment

yoco_bp = Blueprint("yoco_bp", __name__)


import requests

@yoco_bp.route("/start", methods=["GET", "POST"], endpoint="yoco_start")
def start():
    """
    Creates a Yoco Hosted Checkout session and redirects the user to the Yoco gateway.
    """


    from flask_login import current_user
    
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
    # 1) Try to get amount from user_enrollment (since parity pricing locks the user to their first trial country price)
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

    # 2) Try to get amount from session root (since quote route stores it there for new users)
    if amount_cents <= 0:
        if session.get("subject_slug") == subject and session.get("zar_amount_cents"):
            amount_cents = int(session.get("zar_amount_cents"))

    # 3) Fallback to session reg_ctx if user isn't fully created yet but session has quote
    if amount_cents <= 0:
        ctx = session.get("reg_ctx", {})
        quote = ctx.get("quote", {})
        if quote:
            fallback = quote.get("est_zar_cents") or quote.get("zar_amount_cents")
            if fallback:
                amount_cents = int(fallback)
            
    # 3) Final fallback to AuthPricing
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

    # Removed hardcoded test value so actual pricing flows through

    if amount_cents < 1000:
        flash(f"Payment cannot proceed: invalid amount (R{amount_cents/100:.2f}). Minimum is R10.00.", "error")
        return redirect(url_for("public_bp.welcome"))

    # --- LOCAL DEVELOPMENT BYPASS ---
    # Yoco's Hosted Sandbox actively crashes when attempting to webhook/redirect to private local IPs (localhost/127.0.0.1).
    if request.host.startswith("127.0.0.1") or request.host.startswith("localhost"):
        return f"""
        <html>
            <head><title>Simulated Payment</title></head>
            <body style="font-family: sans-serif; text-align: center; padding: 50px;">
                <h1>Local Development: Simulated Payment Flow</h1>
                <p>You are about to pay R{amount_cents/100:.2f} for {subject}.</p>
                <p>Since you are running on localhost, the real Yoco gateway is bypassed to prevent crashes.</p>
                <form action="{url_for('yoco_bp.yoco_success', email=email, subject=subject)}" method="GET">
                    <button type="submit" style="padding: 10px 20px; background: #9333ea; color: white; border: none; border-radius: 5px; cursor: pointer;">
                        Simulate Successful Payment
                    </button>
                </form>
            </body>
        </html>
        """

        
    import os
    val = db.session.execute(text(f"SELECT value FROM system_settings WHERE key = 'yoco_mode_{subject}'")).scalar()
    yoco_mode = val if val else 'sandbox'
    
    # HARDCODED TEST KEY TO AVOID 403 FORBIDDEN FROM BAD ENV VARS
    SECRET_KEY = "sk_test_960bfde0VBrLlpK098e4ffeb53e1"
    
    success_url = url_for("yoco_bp.yoco_success", subject=subject, email=email, _external=True)
    cancel_url = url_for("yoco_bp.yoco_cancel", subject=subject, email=email, _external=True)
    
    try:
        response = requests.post(
            "https://payments.yoco.com/api/checkouts",
            headers={
                "Authorization": f"Bearer {SECRET_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "amount": int(amount_cents),
                "currency": "ZAR",
                "metadata": {"subject": subject, "email": email},
                "successUrl": success_url,
                "cancelUrl": cancel_url
            },
            timeout=15
        )
        
        if response.status_code in (200, 201):
            data = response.json()
            redirect_url = data.get("redirectUrl")
            checkout_id = data.get("id")
            
            if redirect_url:
                yp = YocoPayment(
                    user_id=u.id if u else None,
                    email=email,
                    subject_slug=subject,
                    amount_cents=amount_cents,
                    currency="ZAR",
                    status="pending",
                    checkout_id=checkout_id
                )
                db.session.add(yp)
                db.session.commit()
                return redirect(redirect_url)
            else:
                flash("Payment failed: Invalid response from payment gateway.", "error")
                return redirect(url_for("public_bp.welcome"))
        else:
            try:
                err_data = response.json()
                err_msg = err_data.get("message") or err_data.get("description") or err_data.get("error")
                if not err_msg:
                    import json
                    err_msg = f"Unknown error occurred. Details: {json.dumps(err_data)}"
            except Exception:
                err_msg = response.text
            current_app.logger.error(f"Yoco API Error: {response.status_code} - {err_msg}")
            flash(f"Payment gateway error: {err_msg}", "error")
            return redirect(url_for("public_bp.welcome"))
            
    except Exception as e:
        current_app.logger.error(f"Exception calling Yoco API: {str(e)}")
        flash("Payment gateway is currently unreachable. Please try again later.", "error")
        return redirect(url_for("public_bp.welcome"))


@yoco_bp.post("/callback", endpoint="yoco_callback")
def callback():
    """
    Yoco webhook endpoint to receive server-to-server notifications.
    Yoco will send a POST request here when a payment completes.
    Must return 200 OK.
    """
    current_app.logger.info("Yoco webhook received!")
    return {"ok": True}, 200


@yoco_bp.get("/success", endpoint="yoco_success")
def success():
    # 1) Read query params FIRST (then log)
    ref = (request.args.get("ref") or "").strip()
    from flask_login import current_user
    
    email = (
        (request.args.get("email") or "")
        or (getattr(current_user, "email", None) if current_user.is_authenticated else None)
        or (session.get("pending_email") or "")
    ).strip().lower()
    subject = (
        (request.args.get("subject") or "")
        or (session.get("pending_subject") or "")
        or (session.get("reg_ctx", {}) or {}).get("subject")
        or "loss"
    ).strip().lower()

    current_app.logger.info(
        "YOCO SUCCESS hit: ref=%s email=%s subject=%s",
        ref,
        email,
        subject,
    )

    # No email? show success page but ask to sign in
    if not email:
        flash("Payment completed. Please sign in to continue.", "info")
        return render_template("payments/success.html", subject=subject, ref=ref), 200

    # 2) Ensure user exists; apply staged password hash if we staged one at /register
    u = User.query.filter_by(email=email).first()
    if not u:
        ctx = (session.get("reg_ctx", {}) or {})
        staged = ctx.get("password_hash")
        display = (
            ctx.get("full_name")
            or email.split("@", 1)[0].replace(".", " ").replace("_", " ").title()
        )
        u = User(email=email, name=display, is_active=1)
        if staged:
            u.password_hash = staged
        db.session.add(u)
        db.session.flush()  # get u.id

    # ---------- MECHANIC TOPUP SUCCESS ----------
    if subject == "mechanic_topup":
        amount_cents = int(session.get("mechanic_topup_amount_cents", 0))
        if amount_cents > 0:
            from app.models.mechanic import MechShop
            shop = MechShop.query.filter_by(user_id=u.id).first()
            if shop:
                shop.wallet_balance_cents += amount_cents
                db.session.commit()
            session.pop("mechanic_topup_amount_cents", None)
            flash(f"Successfully topped up wallet by R{amount_cents/100:.2f}!", "success")
        return redirect(url_for("mechanic_bp.mechanic_dashboard"))
    # --------------------------------------------

    # ---------- CFI TOPUP SUCCESS ----------
    if subject.lower() == "cultural_fire_topup":
        from app.models.auth import AitTokenWallet, AitTokenTransaction
        
        # Check if this is a sponsored topup for another participant
        target_participant_id = session.pop("topup_participant_id", None)
        target_user_id = u.id
        sponsor_desc = "Wallet Top-Up"
        
        if target_participant_id:
            # Look up the actual participant's user_id
            target_enrollment = db.session.execute(
                text("SELECT user_id FROM user_enrollment WHERE id = :eid"),
                {"eid": int(target_participant_id)}
            ).scalar()
            if target_enrollment:
                target_user_id = target_enrollment
                sponsor_desc = f"Sponsored Top-Up by {u.name or email}"

        wallet = AitTokenWallet.query.filter_by(user_id=target_user_id).first()
        if not wallet:
            wallet = AitTokenWallet(user_id=target_user_id, balance=0)
            db.session.add(wallet)
            db.session.flush()
            
        amount_tokens = int(session.pop("topup_tokens", 0) or (int(session.get("zar_amount_cents", 0)) // 100))
        if amount_tokens > 0:
            wallet.balance += amount_tokens
            txn = AitTokenTransaction(
                wallet_id=wallet.id,
                amount=amount_tokens,
                description=sponsor_desc
            )
            db.session.add(txn)
            db.session.commit()
            flash(f"Successfully topped up wallet by {amount_tokens} Tokens!", "success")
            return redirect(url_for("cultural_bp.wallet_dashboard"))
    # --------------------------------------------

    # 3) Resolve subject id (safe if missing)
    # Map spv_registration to the spv subject so the user gets correct program enrollment
    lookup_subject = "spv" if subject.lower() == "spv_registration" else subject
    
    sid = db.session.execute(
        text(
            """
        SELECT id
          FROM auth_subject
         WHERE lower(slug) = :s
            OR lower(name) = :s
         LIMIT 1
        """
        ),
        {"s": lookup_subject},
    ).scalar()

    # 4) Flip enrollment to ACTIVE when we have a subject id
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
            text(
                """
            SELECT id, status, zar_amount_cents, local_currency, local_amount_cents, country_code, price_id
              FROM user_enrollment
             WHERE user_id   = :uid
               AND subject_id = :sid
             ORDER BY id DESC LIMIT 1
            """
            ),
            {"uid": int(u.id), "sid": int(sid)},
        ).first()

        new_status = 'paid' if expires_at_val is None else 'active'
        if existing:
            db.session.execute(
                text(
                    """
                    UPDATE user_enrollment
                       SET status = :st,
                           trial_end = NULL,
                           expires_at = :exp
                     WHERE id = :eid
                    """
                ),
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
                text(
                    """
                    INSERT INTO user_enrollment (user_id, subject_id, status, expires_at)
                    VALUES (:uid, :sid, :st, :exp)
                    RETURNING id
                    """
                ),
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
            text(f"""
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
                "ref": ref or "yoco_sandbox",
                "vu": expires_at_val,
                "eid": eid,
                "lcur": local_cur,
                "l_amt": local_cents,
                "pid": price_id,
                "cc": cc
            }
        )
            
        # Provision CFI Tokens if the subject is Cultural Fire
        if subject.lower() == "cultural_fire" or subject.lower() == "culturalfire":
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
            

        session["just_paid_subject_id"] = sid if sid else 12 # Default to CFI

    if session.get('is_retake') and subject == 'home':
        retake_type = session.get('retake_type', 'exam')
        from app.models.home import HomeFinalAssessment, HomeProgress
        HomeFinalAssessment.query.filter_by(user_id=u.id).delete()
        if retake_type == 'course':
            HomeProgress.query.filter_by(user_id=u.id).delete()
            for k in list(session.keys()):
                if k.startswith('chapter_') and k.endswith('_done'):
                    session.pop(k, None)
        session.pop('is_retake', None)
        session.pop('retake_type', None)
        db.session.commit()

    # Process SPV Registration
    if subject.lower() == "spv_registration":
        from app.models.spv import SpvDeal, SpvParticipation
        deal = SpvDeal.query.filter_by(slug="dale-housing").first()
        if deal:
            part = SpvParticipation(
                user_id=u.id,
                deal_id=deal.id,
                amount=100.00,
                pseudonym=u.name or "Anonymous",
                status="confirmed"
            )
            db.session.add(part)

    # Process SPV Investment
    if subject.lower() == "spv_investment":
        from app.models.spv import SpvDeal, SpvParticipation
        spv_amount = float(session.pop("spv_amount", 0))
        spv_pseudonym = session.pop("spv_pseudonym", "Anonymous")
        spv_deal_slug = session.pop("spv_deal_slug", None)
        
        # The 5% fee was added to the charge amount, so the recorded investment is the full spv_amount
        effective_amount = spv_amount
        
        if spv_deal_slug and effective_amount > 0:
            deal = SpvDeal.query.filter_by(slug=spv_deal_slug).first()
            if deal:
                part = SpvParticipation(
                    user_id=u.id,
                    deal_id=deal.id,
                    amount=effective_amount,
                    pseudonym=spv_pseudonym,
                    status="confirmed"
                )
                db.session.add(part)

        db.session.commit()

    # Process Metro Billing Payment
    if subject.lower() == "metro_billing":
        from app.models.billing import BilStatementPayment
        metro_month = session.get("metro_billing_month")
        metro_meters = session.get("metro_billing_meters", 0)
        if metro_month:
            payment = BilStatementPayment.query.filter_by(manager_id=u.id, month=metro_month).first()
            if not payment:
                payment = BilStatementPayment(
                    manager_id=u.id, 
                    month=metro_month, 
                    meters_billed=int(metro_meters),
                    amount_paid_cents=int(amount_cents if 'amount_cents' in locals() else session.get("metro_billing_amount_cents", 0))
                )
                db.session.add(payment)
            else:
                payment.amount_paid_cents += int(amount_cents if 'amount_cents' in locals() else session.get("metro_billing_amount_cents", 0))
            db.session.commit()

    # 4.5) Update YocoPayment record
    pending = YocoPayment.query.filter_by(email=email, subject_slug=subject, status="pending").order_by(YocoPayment.id.desc()).first()
    if pending:
        from datetime import datetime
        pending.status = "completed"
        pending.paid_at = datetime.utcnow()

    db.session.commit()

    # 5) Log in and show confirmation page (button → Bridge)
    try:
        login_user(u, remember=True, fresh=True)
    except Exception:
        pass

    session["payment_banner"] = (
        f"Payment successful for {subject.title() if subject else 'your course'}. You're all set!"
    )
    session["email"] = u.email

    return render_template("payments/success.html", subject=subject, ref=ref), 200


@yoco_bp.get("/cancel", endpoint="yoco_cancel")
def cancel():
    session.pop('is_retake', None)
    session.pop('retake_type', None)
    return render_template("payments/cancelled.html"), 200
