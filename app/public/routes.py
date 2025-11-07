# app/public/routes.py
from flask import (
    Blueprint, request, session, redirect, url_for, 
    current_app, flash, render_template
)

from app.extensions import db
from sqlalchemy import select, text

try:
    from app.security import verify_provider_signature
except Exception:
    def verify_provider_signature(**kwargs):
        return None  # DEV: do nothing

from flask_mail import Message
from app.extensions import mail

public_bp = Blueprint("public_bp", __name__, template_folder="../../templates")

BRIDGE_EP = "auth_bp.bridge_dashboard"

@public_bp.route("/privacy-policy")
def privacy_policy():
    return render_template("public/privacy_policy.html")

@public_bp.route("/terms")
def terms():
    return render_template("public/terms.html")

@public_bp.route("/contact", methods=["GET", "POST"])
def contact():
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        email = request.form.get("email", "").strip()
        subject = request.form.get("subject", "").strip() or "Contact form"
        message = request.form.get("message", "").strip()

        to_addr = current_app.config.get("CONTACT_TO_EMAIL") or current_app.config.get("MAIL_USERNAME")

        # guardrail: ensure SMTP creds exist and won’t be None
        mu = current_app.config.get("MAIL_USERNAME")
        mp = current_app.config.get("MAIL_PASSWORD")
        if not mu or not mp:
            current_app.logger.error("Contact mail aborted: MAIL_USERNAME or MAIL_PASSWORD is missing/empty.")
            flash("Email temporarily unavailable. Please try again later.", "error")
            return redirect(url_for("public_bp.contact"))

        msg = Message(subject=f"[AIT Contact] {subject}", recipients=[to_addr])
        # Let Gmail auth sender be the default; set reply-to to the user
        msg.reply_to = email or None
        msg.body = (
            f"From: {name} <{email}>\n"
            f"Subject: {subject}\n\n"
            f"{message}\n"
        )
        mail.send(msg)
        flash("Thanks! Your message has been sent.", "success")
        return redirect(url_for("public_bp.contact"))

    return render_template("public/contact.html")

@public_bp.route("/_debug/routes")
def _debug_routes():
    from flask import current_app
    lines = [f"{r.endpoint:35s} -> {r.rule}" for r in current_app.url_map.iter_rules()]
    return "<pre>" + "\n".join(sorted(lines)) + "</pre>"

@public_bp.get("/")
def welcome():
    endpoints = []  # whatever you pass
    return render_template("public/welcome.html", endpoints=endpoints)

def refresh_bridge_session(user):
    """
    Mirrors your login() session-building so tiles on /dashboard are correct.
    """
    session["is_authenticated"] = True
    session["email"] = user.email

    is_admin_global = db.session.execute(
        text("SELECT 1 FROM auth_approved_admin WHERE lower(email)=lower(:e) LIMIT 1"),
        {"e": user.email},
    ).fetchone() is not None
    session["is_admin"] = bool(is_admin_global)

    admin_subject_rows = db.session.execute(text("""
        SELECT s.slug
        FROM auth_subject_admin sa
        JOIN auth_subject s ON s.id = sa.subject_id
        WHERE lower(sa.email) = lower(:e)
    """), {"e": user.email}).fetchall()
    session["admin_subjects"] = [r.slug for r in admin_subject_rows]

    # ✅ use user_enrollment (not auth_enrollment)
    enrolled_rows = db.session.execute(text("""
        SELECT s.slug
        FROM user_enrollment ue
        JOIN auth_subject s ON s.id = ue.subject_id
        WHERE ue.user_id = :uid AND ue.status = 'active'
    """), {"uid": user.id}).fetchall()
    session["enrolled_subjects"] = [r.slug for r in enrolled_rows]

    # ✅ use user_enrollment in the access check
    access_rows = db.session.execute(text("""
        SELECT
          s.slug,
          CASE
            WHEN :is_admin_global = 1 THEN 'admin'
            WHEN EXISTS (
              SELECT 1 FROM auth_subject_admin sa
              WHERE sa.subject_id = s.id AND lower(sa.email) = lower(:e)
            ) THEN 'admin'
            WHEN EXISTS (
              SELECT 1
              FROM user_enrollment ue
              WHERE ue.subject_id = s.id
                AND ue.user_id    = :uid
                AND ue.status     = 'active'
            ) THEN 'enrolled'
            ELSE 'locked'
          END AS access_level
        FROM auth_subject s
        WHERE s.is_active = 1
        ORDER BY s.sort_order, s.name
    """), {"e": user.email, "uid": user.id, "is_admin_global": 1 if is_admin_global else 0}).fetchall()
    session["subjects_access"] = {r.slug: r.access_level for r in access_rows}



