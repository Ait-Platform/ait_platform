# app/checkout/routes.py
from __future__ import annotations

import os
import datetime
import stripe
from urllib.parse import urlencode, urlparse, urlunparse, parse_qsl
from werkzeug.security import generate_password_hash

from flask import Blueprint, request, redirect, session, url_for, current_app, jsonify, flash
from flask_login import login_required, current_user, login_user
from sqlalchemy import text as sa_text
from sqlalchemy import text
from app import db, csrf
from app.auth.decisions import get_canonical_user_id, upsert_active_enrollment
from app.auth.helpers import subject_id_from_slug
from app.checkout.stripe_client import fetch_subject_price
from app.models import subject
from app.models.auth import AuthSubject, User
from app.payments.helpers import mark_enrollment_paid, record_stripe_payment
from sqlalchemy import func as SA_FUNC
from . import checkout_bp 

from flask import (
    flash, current_app, request, render_template, redirect, render_template_string,
    url_for, session as flask_session, session as _sess,
    jsonify,
)
from urllib.parse import urljoin

from werkzeug.routing import BuildError
from datetime import datetime
import json
from app.utils.roles import normalize_role
    # app/checkout/routes.py  → inside start()
from decimal import Decimal
from app.utils.pricing import get_subject_plan

import secrets
from decimal import Decimal, InvalidOperation


   


@checkout_bp.route("/ping")
def ping():
    return "checkout ok", 200

@checkout_bp.route("/hello/<name>")
def hello(name: str):
    return f"hello {name}", 200

def _stripe():
    """
    Lazy import + key check.
    Returns a configured stripe module OR None (to enable 'dev clone' fallback).
    """
    key = current_app.config.get("STRIPE_SECRET_KEY")
    if not key or key.endswith("xxx"):
        return None  # trigger dev/clone mode
    import stripe  # type: ignore[import-not-found,unused-ignore]
    stripe.api_key = key
    return stripe


def _abs_url(u: str | None) -> str | None:
    if not u:
        return None
    if u.startswith("http://") or u.startswith("https://"):
        return u
    # assume local absolute path
    return request.host_url.rstrip("/") + "/" + u.lstrip("/")

def _get_stripe_api_key() -> str:
    return os.getenv("STRIPE_SECRET_KEY") or current_app.config.get("STRIPE_SECRET_KEY", "")

# ------------------------------
# START: create Checkout Session
# ------------------------------
# app/checkout/routes.py

    
@checkout_bp.get("/success", endpoint="success")
def success():

    stripe.api_key = _get_stripe_api_key()

    sess_id = request.args.get("session_id")
    if not sess_id:
        return redirect(url_for("auth_bp.bridge_dashboard"), code=303)

    # ---------- 1) Fetch Stripe session ----------
    s = None
    try:
        s = stripe.checkout.Session.retrieve(
            sess_id, expand=["payment_intent.latest_charge", "customer"]
        )
    except Exception as e:
        current_app.logger.exception("success: retrieve %s failed: %s", sess_id, e)

    # Defaults
    is_paid = False
    email = None
    amount_total = 0
    currency = "ZAR"
    purpose = "checkout"
    pi_id = None
    receipt_url = None
    paid_at_val = None
    customer_id = None
    sid = None            # numeric subject id we will activate
    final_subject = None  # slug/name for banner

    if s:
        md = s.get("metadata") or {}
        # Strictly pull identifiers from Stripe context (not request args)
        email = (md.get("email")
                 or (s.get("customer_details") or {}).get("email")
                 or s.get("customer_email"))
        if email:
            email = email.strip().lower()

        # Subject: prefer metadata subject_id, else resolve metadata subject slug/name
        sid_raw = md.get("subject_id")
        try:
            sid = int(sid_raw) if sid_raw is not None else None
        except Exception:
            sid = None

        subj_md = (md.get("subject") or "").strip().lower()
        final_subject = subj_md or None

        if not sid and subj_md:
            sid = db.session.execute(sa_text("""
                SELECT id FROM auth_subject
                WHERE is_active=1 AND (lower(slug)=:k OR lower(name)=:k)
                LIMIT 1
            """), {"k": subj_md}).scalar()

        # Amount/currency/purpose
        amount_total = s.get("amount_total") or 0
        currency = (s.get("currency") or "ZAR").upper()
        purpose = md.get("purpose") or "checkout"

        # PI + charge
        pi = s.get("payment_intent")
        pi_status = ((pi.get("status") if isinstance(pi, dict) else "") or "").lower()
        is_paid = ((s.get("payment_status") or "").lower() == "paid") or \
                  (pi_status in ("succeeded", "processing", "requires_capture"))

        if isinstance(pi, dict):
            pi_id = pi.get("id")
            ch = pi.get("latest_charge") or {}
            receipt_url = ch.get("receipt_url")
            try:
                ts = ch.get("created") or pi.get("created")
                paid_at_val = (
                    datetime.datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S")
                    if ts else None
                )
            except Exception:
                paid_at_val = None

            customer_id = s.get("customer") or pi.get("customer")
        else:
            # No PI dict expanded; best-effort
            customer_id = s.get("customer")

        # Normalize customer_id to a plain string
        if isinstance(customer_id, dict):
            customer_id = customer_id.get("id")
        if customer_id is not None:
            customer_id = str(customer_id)

        current_app.logger.info(
            "stripe success: paid=%s email=%s sid=%s subj=%s sess=%s",
            is_paid, email, sid, final_subject, sess_id
        )

    # ---------- 2) Persist payment (own commit) ----------
    try:
        record_stripe_payment(
            user_id=None,                   # we upsert by session; user can be resolved later
            session_id=sess_id,
            payment_intent_id=pi_id,
            customer_id=customer_id,
            email=email,
            amount_total=amount_total,
            currency=currency,
            status=("succeeded" if is_paid else "failed"),
            purpose=purpose,
            next_url="",
            receipt_url=receipt_url,
            paid_at=paid_at_val,
        )
        db.session.commit()
    except Exception as e:
        current_app.logger.exception("success: payment upsert failed (non-fatal): %s", e)
        db.session.rollback()

    # ---------- 3) Flip THIS subject to ACTIVE (own commit) ----------
    uid = None
    if is_paid and email and sid:
        try:
            # Canonical user id by email
            uid = db.session.execute(sa_text(
                'SELECT MIN(id) FROM "user" WHERE lower(email)=lower(:e)'
            ), {"e": email}).scalar()

            if not uid:
                # ===== FIX: create proper user with real password hash =====
                reg_ctx = session.get("reg_ctx", {}) or {}

                full_name   = (reg_ctx.get("full_name") or "").strip()
                staged_hash = reg_ctx.get("password_hash")

                if not staged_hash:
                    current_app.logger.error(
                        "[checkout.success] missing password_hash in reg_ctx for %s",
                        email
                    )
                    # emergency fallback: force a reset-required hash
                    staged_hash = generate_password_hash("PLEASE_RESET_PASSWORD")

                new_user = User(
                    email=email,
                    name=full_name if full_name else email.split("@", 1)[0],
                )
                new_user.password_hash = staged_hash

                # wipe any legacy plaintext `password` field if model still has it
                if hasattr(new_user, "password"):
                    try:
                        setattr(new_user, "password", None)
                    except Exception:
                        pass

                db.session.add(new_user)
                db.session.flush()  # get new_user.id
                uid = int(new_user.id)

            # Idempotent activate enrollment
            db.session.execute(sa_text("""
                INSERT INTO user_enrollment (user_id, subject_id, status)
                VALUES (:uid, :sid, 'active')
                ON CONFLICT(user_id, subject_id) DO UPDATE SET status='active'
            """), {"uid": int(uid), "sid": int(sid)})

            # Focus Bridge on the new course
            session["user_id"] = int(uid)
            session["email"] = email
            session["just_paid_subject_id"] = int(sid)

            db.session.commit()
        except Exception as e:
            current_app.logger.exception("success: enrollment activate failed: %s", e)
            db.session.rollback()

    # ---------- 4) Establish login + banner; land on Bridge ----------
    user = None
    try:
        if session.get("user_id"):
            user = User.query.get(int(session["user_id"]))
    except Exception:
        user = None

    if user:
        try:
            login_user(user, remember=True)
        except Exception:
            pass

        session["user_id"] = int(user.id)
        session["email"] = email or (user.email or "").lower()
        session["role"] = "user"
        session["user_name"] = user.name or (
            session["email"].split("@", 1)[0] if session.get("email") else None
        )

        if is_paid and sid:
            session["just_paid_subject_id"] = int(sid)
            session["just_paid_subject_slug"] = final_subject or ""

        session.modified = True
        session.permanent = True

        if is_paid:
            subj_title = (final_subject or "").title() if final_subject else None
            session["payment_banner"] = {
                "title": "Payment confirmed",
                "detail": (
                    f"You’re enrolled in {subj_title}."
                    if subj_title else
                    "Payment confirmed."
                ),
                "amount": amount_total,
                "currency": currency,
                "receipt": receipt_url,
            }

    return redirect(
        url_for("auth_bp.bridge_dashboard", role=session.get("role", "user")),
        code=303
    )


# ------------------------------
# WEBHOOK: primary source of truth
# ------------------------------
@checkout_bp.post("/webhook", endpoint="webhook")
@csrf.exempt
def webhook():
    import json

    payload = request.get_data(as_text=True)
    sig = request.headers.get("Stripe-Signature", "")
    secret = os.getenv("STRIPE_WEBHOOK_SECRET") or current_app.config.get("STRIPE_WEBHOOK_SECRET")

    try:
        event = stripe.Webhook.construct_event(payload, sig, secret)
    except stripe.error.SignatureVerificationError:
        return "bad sig", 400
    except Exception:
        return "bad payload", 400

    if event.get("type") == "checkout.session.completed":
        s = event["data"]["object"]  # stripe session dict

        md = s.get("metadata") or {}
        purpose = md.get("purpose") or "checkout"
        subject = (md.get("subject") or "").lower().strip()
        user_id = md.get("user_id")

        # map email → user_id if needed
        email = (s.get("customer_details") or {}).get("email") or s.get("customer_email")
        try:
            user_id = int(user_id) if user_id else None
        except Exception:
            user_id = None
        if not user_id and email:
            from app.models import User
            u = User.query.filter_by(email=email.lower()).first()
            user_id = getattr(u, "id", None)

        # write stripe_payment
        try:
            # session.completed doesn't always include PI id; attempt to read it if provided
            pi_id = None
            pi = s.get("payment_intent")
            if isinstance(pi, dict):
                pi_id = pi.get("id")
            elif isinstance(pi, str):
                pi_id = pi

            record_stripe_payment(
                user_id=user_id,
                session_id=s.get("id"),
                pi_id=pi_id,
                customer_id=s.get("customer"),
                email=email,
                amount_total=s.get("amount_total") or 0,
                currency=(s.get("currency") or "ZAR").upper(),
                status=(s.get("payment_status") or "paid"),
                purpose=purpose,
                next_url=None,
                receipt_url=s.get("receipt_url"),
                paid_at=None,
            )
        except Exception as e:
            current_app.logger.exception("webhook: record_stripe_payment failed: %s", e)

        # ensure enrollment (paid)
        sid = subject_id_from_slug(subject)
        if user_id and sid:
            try:
                mark_enrollment_paid(user_id=user_id, subject_id=sid, program=subject or None)
            except Exception as e:
                current_app.logger.exception("webhook: mark_enrollment_paid failed: %s", e)

    return "ok", 200

@checkout_bp.get("/cancel", endpoint="cancel")
@login_required
def cancel():
    """User canceled on Stripe checkout page."""
    # (optional) mark the pending payment as canceled if you stored session_id on start:
    sess_id = request.args.get("session_id")
    try:
        if sess_id:
            db.session.execute(
                text("UPDATE stripe_payment SET status='canceled', updated_at=CURRENT_TIMESTAMP WHERE session_id=:sid"),
                {"sid": sess_id},
            )
            db.session.commit()
    except Exception:
        db.session.rollback()
    # Land back on bridge
    return redirect(url_for("auth_bp.bridge_dashboard"), code=303)

# --- keep in auth/routes.py ---




# models/helpers you already have
# from app.models import db, AuthSubject
# from app.utils.pricing import get_subject_price

def _subject_by_slug(slug: str):
    if not slug:
        return None
    return (
        db.session.query(AuthSubject)
        .filter(SA_FUNC.lower(SA_FUNC.coalesce(AuthSubject.slug, AuthSubject.name)) == slug.strip().lower())
        .first()
    )

def _create_checkout_session(subject_slug: str, role: str, email: str, full_name: str, currency: str, price: float):
    """Return a Stripe Checkout Session (no writes). Reuse your old Stripe code here."""
    # --- YOUR OLD STRIPE SETUP (unchanged) ---
    secret = current_app.config.get("STRIPE_SECRET_KEY")
    if not secret:
        raise RuntimeError("Stripe not configured")
    stripe.api_key = secret

    subj = _subject_by_slug(subject_slug)
    if not subj:
        raise RuntimeError("Unknown subject")

    product_name = f"{(subj.name or subject_slug.title())} Enrollment"
    amount_cents = int(Decimal(str(price or 0)) * 100)

    success_url = url_for("auth_bp.bridge_dashboard", role=role, _external=True) + "?paid=1"
    cancel_url  = url_for("auth_bp.register_decision", _external=True) + "?canceled=1"

    # Inputs expected to be defined before this snippet:
    # email, success_url, cancel_url, currency, amount_cents, product_name, subject_slug, role, full_name
    # Optionally: price_id (string). If provided, it will be used instead of price_data.

    # --- Build line_items (Stripe requires this in payment mode) ---
    # REQUIRED inputs already defined above this snippet:
    # email, success_url, cancel_url, currency, amount_cents, product_name, subject_slug, role, full_name

    # Hard requirements (fail fast if misconfigured)
    if not isinstance(amount_cents, int) or amount_cents <= 0:
        raise ValueError("amount_cents must be a positive integer (in cents).")

    currency_code = (currency or "zar").lower()
    product_title = product_name or f"{(subject_slug or 'course').title()} enrollment"

    checkout = stripe.checkout.Session.create(
        mode="payment",
        customer_email=(email or None),
        allow_promotion_codes=True,
        success_url=success_url,
        cancel_url=cancel_url,
        line_items=[{
            "price_data": {
                "currency": currency_code,          # stripe requires lowercase
                "unit_amount": amount_cents,        # e.g., 15000 for ZAR 150.00
                "product_data": {
                    "name": product_title,
                    "metadata": {
                        "subject_slug": (subject_slug or ""),
                        "role": (role or "user"),
                    },
                },
            },
            "quantity": 1,
        }],
        metadata={
            "subject_slug": (subject_slug or ""),
            "role": (role or "user"),
            "email": (email or ""),
            "full_name": (full_name or ""),
        },
    )

    return checkout

@checkout_bp.route("/checkout", methods=["GET"], endpoint="checkout")
@login_required
def checkout():
    q    = request.args
    subj = (q.get("subject") or flask_session.get("subject") or "").strip().lower()
    role = (q.get("role") or flask_session.get("role") or "user").strip().lower()
    nxt  = (q.get("next") or flask_session.get("next_url") or "/").strip()

    for k in ("pending_email", "pending_subject", "pending_session_ref", "just_paid_subject_id"):
        flask_session.pop(k, None)

    if subj:
        flask_session["subject"] = subj
    flask_session["role"]     = role
    flask_session["next_url"] = nxt

    # Defaults from request; we’ll fill from DB if blank
    price_id = q.get("price_id") or ""
    amount   = q.get("amount") or ""
    currency = (current_app.config.get("STRIPE_CURRENCY") or "ZAR").upper()

    # Prefill from auth_pricing if UI didn’t supply values
    if not price_id and not amount and subj:
        amt_cents, db_cur = fetch_subject_price(subj, role)
        if amt_cents:
            amount   = str(amt_cents)
            currency = (db_cur or currency).upper()

    # Quantity (min 1)
    try:
        quantity = int(q.get("quantity", 1) or 1)
        if quantity < 1:
            quantity = 1
    except Exception:
        quantity = 1

    return render_template(
        "checkout/checkout.html",
        price_id=price_id,
        amount=amount,
        quantity=quantity,
        name=q.get("name", "Payment"),
        purpose=q.get("purpose", "checkout"),
        subtitle=q.get("subtitle", ""),
        next_url=nxt,
        ap=q.get("ap"),
        currency=currency,
    )









checkout_bp = Blueprint("checkout_bp", __name__)

def _to_two_decimals(value: Decimal | float | str) -> str:
    """
    Return a string with 2 decimals (e.g., '50.00').
    """
    try:
        return f"{Decimal(value).quantize(Decimal('0.01'))}"
    except Exception:
        return "0.00"

def _normalize_amount_to_rands(amount_str: str, quantity_str: str = "1") -> str:
    """
    Accepts:
      - '5000' (cents)  -> '50.00'
      - '50.00'         -> '50.00'
      - '50'            -> '50.00'
    Multiplies by quantity if provided.
    """
    qty = 1
    try:
        qty = max(1, int(quantity_str))
    except Exception:
        qty = 1

    if not amount_str:
        return "0.00"

    # If it's all digits, treat as cents
    if amount_str.isdigit():
        cents = int(amount_str)
        rands = Decimal(cents) / Decimal(100)
    else:
        try:
            rands = Decimal(amount_str)
        except InvalidOperation:
            rands = Decimal(0)

    total = rands * qty
    return _to_two_decimals(total)

@checkout_bp.post("/start", endpoint="start")
@login_required
def start():
    """
    Entry point from your checkout form.
    - Reads subject/role/next
    - Uses price_id/amount if provided, otherwise fetches from pricing
    - Normalizes amount to 'R.xx'
    - Redirects to a PayFast handoff page (auto-posts to payfast_bp.create_payment)
    """
    subj = (request.form.get("subject") or flask_session.get("subject") or "").strip().lower()
    role = (request.form.get("role") or flask_session.get("role") or "user").strip().lower()
    nxt  = (request.form.get("next") or flask_session.get("next_url") or "/").strip()

    price_id = (request.form.get("price_id") or "").strip()
    amount   = (request.form.get("amount") or "").strip()
    qty      = request.form.get("quantity") or "1"
    cur      = (request.form.get("currency") or current_app.config.get("STRIPE_CURRENCY") or "ZAR").upper()

    # If both empty, pull from your pricing table now
    if not price_id and not amount and subj:
        amt_cents, db_cur = fetch_subject_price(subj, role)
        if amt_cents:
            amount = str(amt_cents)        # cents as string
            cur    = (db_cur or cur).upper()

    if not price_id and not amount:
        flash("Payment configuration missing for this subject.", "warning")
        return redirect(url_for("checkout_bp.checkout", subject=subj, role=role))

    # Convert to 'R.xx' and multiply by quantity if needed
    amount_rands = _normalize_amount_to_rands(amount, qty)

    # Build item name and reference
    item_name = f"AIT {subj.title() or 'Course'}"
    buyer_email = (current_user.email or "test@example.com").lower()
    m_payment_id = f"AIT-{secrets.token_hex(6)}"

    # Hand off to our PayFast handoff page (auto-posts to payfast_bp.create_payment)

    import secrets

    item_name = f"AIT {subj.title() or 'Course'}"

    return redirect(url_for(
        "checkout_bp.payfast_handoff",
        amount=amount_rands,
        item_name=item_name,
        buyer_email=buyer_email,
        m_payment_id=m_payment_id,
    ), code=303)
'''
@checkout_bp.get("/payfast-handoff", endpoint="payfast_handoff")
@login_required
def payfast_handoff():
    """
    Renders a minimal auto-post form to your PayFast creator route.
    No hardcoding of amount or names — all values come from the querystring
    set by `start()`. Your PayFast server route reads RETURN/CANCEL/NOTIFY
    from env and signs the payload before redirecting to PayFast.
    """
    amount       = request.args.get("amount", "0.00")
    item_name    = request.args.get("item_name", "AIT Course")
    buyer_email  = request.args.get("buyer_email", (current_user.email or "test@example.com").lower())
    m_payment_id = request.args.get("m_payment_id") or f"AIT-{secrets.token_hex(6)}"

    # Minimal inline template keeps this self-contained
    html = """
    <!doctype html>
    <html>
      <head><meta charset="utf-8"><title>Redirecting…</title></head>
      <body>
        <form id="pf" method="post" action="{{ url_for('payfast_bp.create_payment') }}">
          <input type="hidden" name="amount" value="{{ amount }}">
          <input type="hidden" name="item_name" value="{{ item_name }}">
          <input type="hidden" name="email" value="{{ buyer_email }}">
          <input type="hidden" name="m_payment_id" value="{{ m_payment_id }}">
          <noscript>
            <p>Click continue to proceed to PayFast.</p>
            <button type="submit">Continue to PayFast</button>
          </noscript>
        </form>
        <script>try{document.getElementById('pf').submit()}catch(e){}</script>
      </body>
    </html>
    """
    return render_template_string(
        html,
        amount=amount,
        item_name=item_name,
        buyer_email=buyer_email,
        m_payment_id=m_payment_id
    )
'''
# app/checkout/routes.py
import secrets
from flask import render_template_string, request, url_for
from flask_login import login_required, current_user

# IMPORTANT: import the blueprint object created in app/checkout/__init__.py
from app.checkout import checkout_bp

@checkout_bp.get("/payfast-handoff", endpoint="payfast_handoff")
@login_required
def payfast_handoff():
    amount       = request.args.get("amount", "0.00")
    item_name    = request.args.get("item_name", "AIT Course")
    buyer_email  = request.args.get("buyer_email", (current_user.email or "test@example.com").lower())
    m_payment_id = request.args.get("m_payment_id") or f"AIT-{secrets.token_hex(6)}"

    html = """
    <!doctype html>
    <html><head><meta charset="utf-8"><title>Redirecting…</title></head>
    <body>
      <form id="pf" method="post" action="{{ url_for('payfast_bp.create_payment') }}">
        <input type="hidden" name="amount" value="{{ amount }}">
        <input type="hidden" name="item_name" value="{{ item_name }}">
        <input type="hidden" name="email" value="{{ buyer_email }}">
        <input type="hidden" name="m_payment_id" value="{{ m_payment_id }}">
        <noscript><button type="submit">Continue to PayFast</button></noscript>
      </form>
      <script>try{document.getElementById('pf').submit()}catch(e){}</script>
    </body></html>
    """
    return render_template_string(
        html,
        amount=amount,
        item_name=item_name,
        buyer_email=buyer_email,
        m_payment_id=m_payment_id,
    )
