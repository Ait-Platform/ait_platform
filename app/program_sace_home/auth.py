"""Platform identity registration with HOME-only continuation; grants no authority."""
from flask import render_template, request, redirect, url_for, flash
from flask_login import current_user, login_user
from werkzeug.security import generate_password_hash, check_password_hash
from sqlalchemy.exc import IntegrityError
from app.extensions import db
from app.models.auth import User
from . import service as s
from . import continuation


def registration():
    # The provisioning/code and signed pledge must precede HOME registration.
    from flask import session
    if session.get("sace_home_pending_code"):
        row = s.invitation()
        s.consent("auditor", row.id)
    else:
        row = s.provisioning()
        s.consent("controller", row.id)
    if current_user.is_authenticated:
        continuation.clear()
        return redirect(url_for(s.auth_destination()))
    values = {}
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        name = request.form.get("full_name", "").strip()
        password = request.form.get("password", "")
        values = dict(email=email, full_name=name)
        if not name or len(name) > 255 or "@" not in email or len(email) > 255 or len(password) < 8:
            flash("Enter your full name, email and a password of at least eight characters.", "warning")
        else:
            user = User.query.filter(db.func.lower(User.email) == email).first()
            if user is not None:
                if not user.is_active or not user.password_hash or not check_password_hash(user.password_hash, password):
                    flash("This account cannot be signed in with those details. Use sign in or password recovery.", "warning")
                    user = None
            else:
                user = User(email=email, name=name, password_hash=generate_password_hash(password), is_active=1)
                db.session.add(user)
                try:
                    db.session.commit()
                except IntegrityError:
                    db.session.rollback()
                    user = None
                    flash("That email is already registered. Please sign in.", "warning")
            if user is not None:
                login_user(user, fresh=True)
                session["is_authenticated"] = True
                session["email"] = user.email
                session["user_id"] = user.id
                session["user_name"] = user.name
                record_signin()
                continuation.clear()
                return redirect(url_for(s.auth_destination()))
    return render_template("auth/register.html", subject=s.SUBJECT, role="user",
        next_url=url_for(s.auth_destination()), values=values)


def lifecycle_available():
    # Shared login must still work before HOME's additive migration is installed.
    return db.session.execute(db.text("SELECT to_regclass('sace_home_engagement')")).scalar() is not None


def record_signin():
    from . import lifecycle as lc
    if not lifecycle_available():
        return
    actor = lc.appointment()
    if actor:
        lc.audit(current_user.id, "controller", "authenticated", actor.engagement_id)
    else:
        for row in lc.assignments():
            invitation = db.session.get(lc.HomeInvitation, row.invitation_id)
            appointment = db.session.get(lc.Appointment, invitation.appointment_id)
            lc.audit(current_user.id, "auditor", "authenticated", appointment.engagement_id, row.id)
    db.session.commit()
