from datetime import datetime, timedelta, timezone
from flask import Blueprint, render_template, request, redirect, session, url_for, flash
from flask_login import login_required, current_user
from sqlalchemy import text as sa_text
from app.extensions import db
from app.forms import DummyForm
from app.models.auth import AuthPaymentLog, AuthSubject, AuthSubscription, User, UserEnrollment, UserRole
from app.time_utils import SA_TIMEZONE, app_now, expiry_for
from app.utils.redirects import safe_next
from . import subscription_bp


def _utcnow() -> datetime:
    return app_now()


def check_and_update_expiry(enrollment):
    subscription = None
    if enrollment.subscription_id:
        subscription = AuthSubscription.query.get(enrollment.subscription_id)

    if subscription and subscription.valid_until:
        exp = subscription.valid_until
        if exp and exp.tzinfo is None:
            exp = exp.replace(tzinfo=SA_TIMEZONE)

        now = app_now()
        if exp < now:
            # expired → renew
            new_expiry = expiry_for(enrollment.subject, mode="subscription")
            enrollment.expires_at = new_expiry
            enrollment.updated_at = now
            enrollment.status = "active"

            subscription.valid_from = now
            subscription.valid_until = new_expiry
            subscription.status = "active"

            db.session.add(enrollment)
            db.session.add(subscription)
            db.session.commit()


def process_subscription_renewal(subscription, subject):
    # Fetch the enrollment linked to this subscription
    enrollment = UserEnrollment.query.filter_by(subscription_id=subscription.id).first_or_404()

    # Insert payment log
    payment_log = insert_payment_log(enrollment, subject)

    # --- Update subscription ---
    subscription.status = "active"
    subscription.valid_from = app_now()
    subscription.valid_until = app_now() + timedelta(days=float(subject.paid_days))
    subscription.last_payment_id = payment_log.id
    subscription.payment_confirmed_at = datetime.utcnow()

    # --- Update enrollment ---
    enrollment.status = "active"
    enrollment.expires_at = subscription.valid_until  # keep aligned with subscription

    # Commit subscription + enrollment changes first
    db.session.commit()

    # --- Grant user permissions ---
    grant_user_permissions(subscription.user_id, program=subject.slug)

    # --- Refresh session baton (prevents stale 'expired/locked' tiles) ---
    if "enrollment_dict" in session:
        session["enrollment_dict"]["status"] = enrollment.status
        session["enrollment_dict"]["expires_at"] = str(enrollment.expires_at)
    session["access_level"] = "active"

    # --- Flash single success message ---
    flash("Your subscription has been renewed successfully!", "success")

def insert_payment_log(enrollment, subject):
    payment_log = AuthPaymentLog(
        user_id=enrollment.user_id,
        program=subject.slug,
        amount=enrollment.local_amount_cents,      # ✅ from enrollment
        currency=enrollment.local_currency,        # ✅ from enrollment
        status="success",
        valid_from=app_now(),
        valid_until=app_now() + timedelta(days=float(subject.paid_days)),  # ✅ from subject
        enrollment_id=enrollment.id,
        local_currency=enrollment.local_currency,
        local_amount_cents=enrollment.local_amount_cents,
        price_id=enrollment.price_id,
        country_code=enrollment.country_code,
        created_at=app_now()
    )
    db.session.add(payment_log)
    db.session.commit()
    return payment_log

@subscription_bp.route("/expired/<int:subscription_id>")
def expired_page(subscription_id):
    subscription = AuthSubscription.query.get_or_404(subscription_id)
    enrollment = UserEnrollment.query.filter_by(subscription_id=subscription_id).first()
    subject = AuthSubject.query.get(enrollment.subject_id)
    form = DummyForm()  # ensures CSRF token is available

    return render_template(
        "subscription/expired.html",
        subscription=subscription,
        enrollment=enrollment,
        subject=subject,
        form=form,
        options={"renew": True, "cancel": True}
    )

@subscription_bp.route("/cancel/<int:subscription_id>", methods=["POST"])
def cancel_subscription(subscription_id):
    # Fetch the subscription
    subscription = AuthSubscription.query.get(subscription_id)
    if not subscription:
        flash("Subscription not found.", "warning")
        return redirect(url_for("welcome"))

    # Fetch the enrollment using subscription_id
    enrollment = UserEnrollment.query.filter_by(subscription_id=subscription.id).first()
    if not enrollment:
        flash("Enrollment not found.", "warning")
        return redirect(url_for("welcome"))

    # Fetch the subject linked to the enrollment
    subject = AuthSubject.query.get(enrollment.subject_id)
    if not subject:
        flash("Subject not found.", "warning")
        return redirect(url_for("welcome"))

    # Mark subscription as canceled
    subscription.status = "canceled"
    subscription.canceled_at = app_now()

    # Mark enrollment as expired
    enrollment.status = "expired"

    # Commit changes before revoking permissions
    db.session.commit()

    # Revoke user permissions for this program
    revoke_user_permissions(enrollment.user_id, program=subject.slug)

    # Flash confirmation
    flash("Your subscription has been canceled.", "info")
    return redirect(url_for("welcome"))

def grant_user_permissions(user_id, program):
    user = User.query.get_or_404(user_id)
    target_role = f"paid_{program}"

    user.add_role(target_role)
    db.session.commit()

def revoke_user_permissions(user_id, program):
    user = User.query.get_or_404(user_id)
    target_role = f"paid_{program}"

    user.remove_role(target_role)
    db.session.commit()

@subscription_bp.route("/renew/<int:subscription_id>", methods=["POST"])
def renew_subscription_route(subscription_id):
    subscription = AuthSubscription.query.get(subscription_id)
    if not subscription:
        flash("Subscription not found.", "warning")
        return redirect(url_for("bridge_bp.bridge"))

    enrollment = UserEnrollment.query.filter_by(subscription_id=subscription.id).first()
    if not enrollment:
        flash("Enrollment not found.", "warning")
        return redirect(url_for("bridge_bp.bridge"))

    subject = AuthSubject.query.get(enrollment.subject_id)
    if not subject:
        flash("Subject not found.", "warning")
        return redirect(url_for("bridge_bp.bridge"))

    # --- Build baton from enrollment (stale quote wins) ---
    baton = {
        "subscription_id": subscription.id,
        "enrollment_id": enrollment.id,
        "subject_slug": subject.slug,
        "price_id": enrollment.price_id,
        "price_version": enrollment.price_version,
        "price_locked_at": enrollment.price_locked_at,
        "local_currency": enrollment.local_currency,
        "local_amount_cents": enrollment.local_amount_cents,
        "zar_amount_cents": enrollment.zar_amount_cents,
        "country_code": enrollment.country_code,
    }
    session["baton"] = baton
    form = DummyForm()  # a FlaskForm with CSRF enabled

    # Redirect into payment flow (checkout review)
    return redirect(url_for("paystack_bp.checkout_review",
                            subject=subject.slug,form=form,
                            enrollment_id=enrollment.id))


