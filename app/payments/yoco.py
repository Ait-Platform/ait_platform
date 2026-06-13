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


    email = (
        request.values.get("email") 
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

    # 1) Try to get amount from session root (since quote route stores it there)
    amount_cents = 0
    if session.get("subject_slug") == subject and session.get("zar_amount_cents"):
        amount_cents = int(session.get("zar_amount_cents"))

    # 2) Fallback to session reg_ctx if user isn't fully created yet but session has quote
    if amount_cents <= 0:
        ctx = session.get("reg_ctx", {})
        quote = ctx.get("quote", {})
        if quote:
            fallback = quote.get("est_zar_cents") or quote.get("zar_amount_cents")
            if fallback:
                amount_cents = int(fallback)

    # 3) Fallback to user_enrollment (for existing trials)
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
            
    # 3) Final fallback to AuthPricing
    if amount_cents <= 0:
        from app.payments.pricing import get_subject_price
        price_info = get_subject_price(subject)
        if price_info and price_info["amount_cents"] > 0:
            amount_cents = int(price_info["amount_cents"])
            
    if amount_cents < 200:
        flash(f"Payment cannot proceed: invalid amount (R{amount_cents/100:.2f}). Minimum is R2.00.", "error")
        return redirect(url_for("public_bp.welcome"))

    # --- LOCAL DEVELOPMENT BYPASS ---
    # Yoco's Hosted Sandbox actively crashes when attempting to webhook/redirect to private local IPs (localhost/127.0.0.1).
    if request.host.startswith("127.0.0.1") or request.host.startswith("localhost"):
        flash("Local Development: Bypassing Yoco Gateway to simulate successful payment.", "info")
        return redirect(url_for("yoco_bp.yoco_success", email=email, subject=subject))

        
    # Charge via Yoco Checkout API (Hosted Gateway)
    SECRET_KEY = "sk_test_960bfde0VBrLlpK098e4ffeb53e1"
    
    success_url = url_for("yoco_bp.yoco_success", _external=True)
    cancel_url = url_for("yoco_bp.yoco_cancel", _external=True)
    
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
                err_msg = response.json().get("message", "Unknown error occurred.")
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
    email = (
        (request.args.get("email") or "")
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

    # 3) Resolve subject id (safe if missing)
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
        {"s": subject},
    ).scalar()

    # 4) Flip enrollment to ACTIVE when we have a subject id
    if sid:
        existing = db.session.execute(
            text(
                """
            SELECT id, status
              FROM user_enrollment
             WHERE user_id   = :uid
               AND subject_id = :sid
             LIMIT 1
            """
            ),
            {"uid": int(u.id), "sid": int(sid)},
        ).first()

        if existing:
            db.session.execute(
                text(
                    """
                    UPDATE user_enrollment
                       SET status = 'active'
                     WHERE id = :eid
                    """
                ),
                {"eid": existing.id},
            )
        else:
            db.session.execute(
                text(
                    """
                    INSERT INTO user_enrollment (user_id, subject_id, status)
                    VALUES (:uid, :sid, 'active')
                    """
                ),
                {"uid": int(u.id), "sid": int(sid)},
            )
            
        # Provision CFI Tokens if the subject is Cultural Fire
        if subject.lower() == "cultural_fire" or subject.lower() == "culturalfire":
            from app.models.culturalfire import CfiWallet, CfiTokenTransaction
            wallet = CfiWallet.query.filter_by(user_id=u.id).first()
            if not wallet:
                wallet = CfiWallet(user_id=u.id, balance=0)
                db.session.add(wallet)
                db.session.flush()
            
            wallet.balance += 150
            txn = CfiTokenTransaction(
                wallet_id=wallet.id,
                amount=150,
                description="Initial Registration Bundle (150 Tokens)"
            )
            db.session.add(txn)
            
        # Provision CFI Tokens if it was a wallet topup
        if subject.lower() == "cultural_fire_topup":
            from app.models.culturalfire import CfiWallet, CfiTokenTransaction
            wallet = CfiWallet.query.filter_by(user_id=u.id).first()
            if not wallet:
                wallet = CfiWallet(user_id=u.id, balance=0)
                db.session.add(wallet)
                db.session.flush()
                
            amount_tokens = int(session.pop("topup_tokens", 0) or (int(session.get("zar_amount_cents", 0)) // 100))
            if amount_tokens > 0:
                wallet.balance += amount_tokens
                txn = CfiTokenTransaction(
                    wallet_id=wallet.id,
                    amount=amount_tokens,
                    description=f"Wallet Top-Up ({amount_tokens} Tokens)"
                )
                db.session.add(txn)

        session["just_paid_subject_id"] = sid if sid else 12 # Default to CFI

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
    return render_template("payments/cancelled.html"), 200
