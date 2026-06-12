from flask import current_app, flash, redirect, render_template, session, url_for, request, abort
from flask_login import current_user
from sqlalchemy import text as sa_text

from app.enrollment.helpers import lock_quote_into_enrollment
from app.extensions import db
from app.models.auth import AuthSubject, User, UserEnrollment
from datetime import datetime, timedelta
from werkzeug.security import generate_password_hash



def handle_returning_user(subj, enr):
    now = datetime.utcnow()

    if not enr:
        return redirect(url_for(subj["about_endpoint"], subject=subj["slug"]))

    # Paid subject
    if int(subj.get("requires_price") or 0) == 1:
        if enr.status == "pending" and enr.quoted_amount_cents:
            return redirect(url_for(subj["pay_endpoint"], subject=subj["slug"]))
        elif enr.status == "active" and (not enr.expires_at or enr.expires_at > now):
            return redirect(url_for(subj["start_endpoint"], subject=subj["slug"]))
        elif enr.status == "completed":
            return redirect(url_for(subj.get("report_pdf_url") or subj["start_endpoint"], subject=subj["slug"]))
        else:
            return redirect(url_for("quote_bp.quote", subject=subj["slug"]))

    # Free subject
    if int(subj.get("requires_price") or 0) == 0:
        if enr.status != "active":
            enr.status = "active"
            enr.started_at = now
            db.session.commit()
        return redirect(url_for(subj["start_endpoint"], subject=subj["slug"]))

    return redirect(url_for(subj["about_endpoint"], subject=subj["slug"]))

def handle_enrollment(subj, enr):
    now = datetime.utcnow()

    if not enr:
        return redirect(url_for(subj["about_endpoint"], subject=subj["slug"]))

    # Free subject
    if int(subj.get("requires_price") or 0) == 0:
        if enr.status != "active":
            enr.status = "active"
            enr.started_at = now
            db.session.commit()
        return redirect(url_for(subj["start_endpoint"], subject=subj["slug"]))

    # Paid only (no trial)
    if int(subj.get("requires_price") or 0) == 1 and int(subj.get("trial_days") or 0) == 0:
        if enr.status == "active" and (not enr.expires_at or enr.expires_at > now):
            return redirect(url_for(subj["start_endpoint"], subject=subj["slug"]))
        elif enr.status == "pending" and enr.quoted_amount_cents:
            return redirect(url_for(subj["pay_endpoint"], subject=subj["slug"]))
        else:
            return redirect(url_for("quote_bp.quote", subject=subj["slug"]))

    # Paid + Trial
    if int(subj.get("requires_price") or 0) == 1 and int(subj.get("trial_days") or 0) > 0:
        if enr.trial_count == 0:
            trial_end = now + timedelta(days=subj["trial_days"])
            enr.trial_count = 1
            enr.trial_end = trial_end
            enr.expires_at = trial_end
            enr.status = "active"
            enr.started_at = now
            db.session.commit()
            return redirect(url_for(subj["start_endpoint"], subject=subj["slug"]))

        if enr.trial_end and enr.trial_end > now:
            return redirect(url_for(subj["start_endpoint"], subject=subj["slug"]))

        if enr.trial_end and enr.trial_end <= now:
            if subj.get("trial_expired_endpoint"):
                return redirect(url_for(subj["trial_expired_endpoint"], subject=subj["slug"]))
            else:
                return redirect(url_for(subj["pay_endpoint"], subject=subj["slug"]))

    # Completed
    if enr.status == "completed":
        return redirect(url_for(subj.get("report_pdf_url") or subj["start_endpoint"], subject=subj["slug"]))

    # Fallback
    return redirect(url_for("quote_bp.quote", subject=subj["slug"]))

def handle_completion(subj, enr):
    if enr.status == "completed":
        if subj.get("report_pdf_url"):
            return redirect(enr.report_pdf_url)
        else:
            return redirect(url_for("dashboard_bp.view", subject=subj["slug"]))
    return None

def register_user(subject_slug, form_data):
    subj = AuthSubject.query.filter_by(slug=subject_slug).first()
    user = User.query.filter_by(email=form_data["email"]).first()

    if user:
        # Existing user → reuse enrollment
        enr = UserEnrollment.query.filter_by(user_id=user.id, subject_id=subj.id).first()
        return handle_returning_user(subj, enr)

    # New user → create record
    user = User(email=form_data["email"])
    db.session.add(user)
    db.session.commit()

    # Create enrollment depending on subject type
    if int(subj.get("requires_price") or 0) == 1 and int(subj.get("trial_days") or 0) > 0:
        # Trial subject
        trial_end = datetime.utcnow() + timedelta(days=subj["trial_days"])
        enr = UserEnrollment(user_id=user.id, subject_id=subj.id,
                             status="active", trial_end=trial_end, trial_count=1,
                             started_at=datetime.utcnow())
    elif int(subj.get("requires_price") or 0) == 1:
        # Paid subject
        enr = UserEnrollment(user_id=user.id, subject_id=subj.id,
                             status="pending",
                             quoted_amount_cents=form_data.get("quoted_amount_cents"),
                             quoted_currency=form_data.get("quoted_currency"),
                             price_version=form_data.get("price_version"),
                             price_locked_at=datetime.utcnow())
    else:
        # Free subject
        enr = UserEnrollment(user_id=user.id, subject_id=subj.id,
                             status="active", started_at=datetime.utcnow())

    db.session.add(enr)
    db.session.commit()

    return handle_enrollment(subj, enr)

def frontdoor_gate_after_register(email, password, subject_slug):
    subj = AuthSubject.query.filter_by(slug=subject_slug).first()
    if not subj or not subj.is_active:
        flash("Program unavailable.", "warning")
        return redirect(url_for("public_bp.welcome"))

    user = User.query.filter_by(email=email).first()
    if user:
        enr = UserEnrollment.query.filter_by(user_id=user.id, subject_id=subj.id).first()
        return handle_returning_user(subj, enr)

    # New user
    user = User(email=email, password_hash=generate_password_hash(password))
    db.session.add(user)
    db.session.commit()

    # Create enrollment depending on subject type
    enr = UserEnrollment(user_id=user.id, subject_id=subj.id, status="pending")
    db.session.add(enr)
    db.session.commit()

    return handle_enrollment(subj, enr)

def handle_returning_user(subj, enr):
    now = datetime.utcnow()
    if not enr:
        return redirect(url_for(subj["about_endpoint"], subject=subj["slug"]))

    if int(subj.requires_price or 0) == 1:
        if enr.status == "pending" and enr.quoted_amount_cents:
            return redirect(url_for(subj["pay_endpoint"], subject=subj["slug"]))
        elif enr.status == "active" and (not enr.expires_at or enr.expires_at > now):
            return redirect(url_for(subj["start_endpoint"], subject=subj["slug"]))
        elif enr.status == "completed":
            return redirect(url_for(subj.get("report_pdf_url") or subj["start_endpoint"], subject=subj["slug"]))
        else:
            return redirect(url_for("quote_bp.quote", subject=subj["slug"]))

    if int(subj.requires_price or 0) == 0:
        if enr.status != "active":
            enr.status = "active"
            enr.started_at = now
            db.session.commit()
        return redirect(url_for(subj["start_endpoint"], subject=subj["slug"]))

    return redirect(url_for(subj["about_endpoint"], subject=subj["slug"]))

def handle_enrollment(subj, enr):
    now = datetime.utcnow()

    if not enr:
        return redirect(url_for(subj["about_endpoint"], subject=subj["slug"]))

    # Free subject
    if int(subj.requires_price or 0) == 0:
        if enr.status != "active":
            enr.status = "active"
            enr.started_at = now
            db.session.commit()
        return redirect(url_for(subj["start_endpoint"], subject=subj["slug"]))

    # Paid only (no trial)
    if int(subj.requires_price or 0) == 1 and int(subj.trial_days or 0) == 0:
        if enr.status == "active" and (not enr.expires_at or enr.expires_at > now):
            return redirect(url_for(subj["start_endpoint"], subject=subj["slug"]))
        elif enr.status == "pending" and enr.quoted_amount_cents:
            return redirect(url_for(subj["pay_endpoint"], subject=subj["slug"]))
        else:
            return redirect(url_for("quote_bp.quote", subject=subj["slug"]))

    # Paid + Trial
    if int(subj.requires_price or 0) == 1 and int(subj.trial_days or 0) > 0:
        if enr.trial_count == 0:
            trial_end = now + timedelta(days=subj.trial_days)
            enr.trial_count = 1
            enr.trial_end = trial_end
            enr.expires_at = trial_end
            enr.status = "active"
            enr.started_at = now
            db.session.commit()
            return redirect(url_for(subj["start_endpoint"], subject=subj["slug"]))

        if enr.trial_end and enr.trial_end > now:
            return redirect(url_for(subj["start_endpoint"], subject=subj["slug"]))

        if enr.trial_end and enr.trial_end <= now:
            if subj.trial_expired_endpoint:
                return redirect(url_for(subj.trial_expired_endpoint, subject=subj["slug"]))
            else:
                return redirect(url_for(subj["pay_endpoint"], subject=subj["slug"]))

    if enr.status == "completed":
        return redirect(url_for(subj.get("report_pdf_url") or subj["start_endpoint"], subject=subj["slug"]))

    return redirect(url_for("quote_bp.quote", subject=subj["slug"]))

def handle_completion(subj, enr):
    if enr.status == "completed":
        if subj.get("report_pdf_url"):
            return redirect(enr.report_pdf_url)
        else:
            return redirect(url_for("dashboard_bp.view", subject=subj["slug"]))
    return None

def create_or_update_enrollment(user_id, subject, price_row=None, status="pending"):
    # Lookup existing enrollment
    enrollment = UserEnrollment.query.filter_by(
        user_id=user_id, subject_id=subject.id
    ).first()

    # Create new if none exists
    if not enrollment:
        enrollment = UserEnrollment(
            user_id=user_id,
            subject_id=subject.id,
            status=status,
            started_at=datetime.utcnow()  # lifecycle start
        )
        db.session.add(enrollment)

    # Always update status
    enrollment.status = status

    # Attach price details if provided
    if price_row:
        # Use the locking helper to freeze quote details
        enrollment = lock_quote_into_enrollment(enrollment, price_row)

        # Also ensure country_code is set (schema requires it)
        enrollment.country_code = price_row.country_code

    # Update audit timestamp
    enrollment.updated_at = datetime.utcnow()

    db.session.commit()
    return enrollment

'''
def program_entry(subject_slug: str):
    """
    Entry point for a subject/program.
    - Identify subject by slug
    - Persist slug + subject_id into baton/session
    - Hand off to welcome
    """

    subj = AuthSubject.query.filter_by(slug=subject_slug).first()
    if not subj:
        return redirect(url_for("public_bp.welcome"))

    # ✅ Seed baton with both slug and subject_id
    baton = get_baton_context()
    baton["subject_slug"] = subj.slug
    baton["subject_id"] = subj.id
    session["baton"] = baton

    # ✅ Debug print
    print(f"[program_entry] Seeded baton: subject_slug={baton['subject_slug']} subject_id={baton['subject_id']}")


    # No mode branching here — that happens later in checkpoint
    return redirect(url_for("public_bp.welcome", subject=subj.slug))
'''