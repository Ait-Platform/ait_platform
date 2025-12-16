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

yoco_bp = Blueprint("yoco_bp", __name__)


@yoco_bp.route("/start", methods=["GET", "POST"], endpoint="yoco_start")
def start():
    """
    Temporary Yoco bypass:
    - accept email + subject sent by redirect()
    - store into session
    - immediately forward to success handler
    """
    email = (request.values.get("email") or "").strip().lower()
    subject = (request.values.get("subject") or "").strip().lower()

    if email:
        session["pending_email"] = email

    if subject:
        session["pending_subject"] = subject

    return redirect(
        url_for(
            "yoco_bp.yoco_success",
            email=email,
            subject=subject
        )
    )


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

        session["just_paid_subject_id"] = int(sid)

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
