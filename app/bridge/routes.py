from datetime import timezone

from flask import Blueprint, flash, redirect, render_template, session, url_for
from flask_login import current_user, login_required
from app.auth.routes import check_admin, get_all_subjects
from app.bridge.helpers import get_user_enrollment
from app.models.auth import AuthSubject, User, UserEnrollment
from app.modes.helpers import determine_access, modes_checkpoint
from app.time_utils import SA_TIMEZONE, app_now
from app.extensions import db
from app.utils.enrollment import ensure_free_enrollment



bridge_bp = Blueprint("bridge_bp", __name__)


BRIDGE_EP = "bridge_bp.bridge"

@bridge_bp.route("/bridge", endpoint="bridge")
@login_required
def bridge_dashboard():
    user = current_user
    print(f"[DEBUG] Bridge route entered for user={getattr(user, 'id', None)}")

    # … continue with admin/learner path logic

    tiles = []
    # … continue with admin/learner path logic unchanged

    # --- Admin path ---
    if check_admin(user.email):
        for subj in get_all_subjects():
            if subj.slug == "admin_general":
                href = url_for("general_bp.index")
            elif subj.slug == "admin":
                href = url_for("admin_bp.index")
            else:
                href = url_for("admin_bp.subject_dashboard", subject=subj.slug)

            tiles.append({
                "subject": subj,
                "slug": subj.slug,
                "name": subj.name,
                "access_level": "admin",
                "href": href
            })
            
        # Append synthetic tile for SPV (since it's not in the AuthSubject table)
        tiles.append({
            "subject": {
                "name": "SPV Precinct Investments",
                "description": "Almond Dale redevelopment and shareholder participation platform.",
                "slug": "spv"
            },
            "slug": "spv",
            "name": "SPV Precinct Investments",
            "access_level": "admin",
            "href": url_for("spv_admin_bp.spv_dashboard")
        })

        session.update({
            "user_id": user.id,
            "access_level": "admin",
            "is_admin": True,
            "role": "admin"
        })
        print(f"[DEBUG] Bridge: admin branch fired for user={user.id}")

    # --- Learner path (includes Cultural Fire via program_type/commercial_mode) ---
    else:
        enrollment_rows = get_user_enrollment(user.id)
        enrollments = {e.subject_id: e for e in enrollment_rows}
        subjects = [s for s in get_all_subjects() if s.id in enrollments]

        # Track if we've added CRM tile so we don't duplicate
        crm_added = False

        for subj in subjects:
            # We don't want these subjects showing up as a separate tile on the bridge
            if subj.slug in ['home_premium'] or getattr(subj, 'is_hidden_on_bridge', False):
                continue

            enrollment = enrollments.get(subj.id)
            if not enrollment:
                continue
                
            if subj.slug == 'practice_crm':
                crm_added = True

            session.update({
                "user_id": enrollment.user_id,
                "enrollment_id": enrollment.id,
                "subject_id": subj.id
            })

            # Branch by program_type + commercial_mode

            match (subj.program_type, subj.commercial_mode):
                case ("lifelong", "paid"):
                    if subj.slug == "cfi_judge":
                        # User specifically requested that CFI Judge does NOT get its own tile on the bridge.
                        # It should only be accessed via the Showcase Dashboard.
                        continue
                    else:
                        tiles.append({
                            "subject": subj,
                            "slug": subj.slug,
                            "name": subj.name,
                            "access_level": "enrolled",
                            "enrollment_id": enrollment.id,
                            "message": "You are enrolled in Cultural Fire — lifelong program.",
                            "href": url_for("cultural_bp.cultural_fire_router", role="participant")
                        })
                    
                case ("course", "free") | ("free", "free"):
                    ensure_free_enrollment(session["user_id"], subj.slug)
                    tiles.append({
                        "subject": subj,
                        "slug": subj.slug,
                        "name": subj.name,
                        "access_level": "enrolled",
                        "message": "You are enrolled in the free program.",
                        "href": url_for("modes_bp.modes_checkpoint_route", subject_slug=subj.slug)
                    })

                case ("trial", "free"):
                    ensure_free_enrollment(session["user_id"], subj.slug)
                    tiles.append({
                        "subject": subj,
                        "slug": subj.slug,
                        "name": subj.name,
                        "access_level": "enrolled",
                        "message": "Your free trial program is active.",
                        "href": url_for("modes_bp.modes_checkpoint_route", subject_slug=subj.slug)
                    })

                case ("trial", "paid"):
                    tiles.append({
                        "subject": subj,
                        "slug": subj.slug,
                        "name": subj.name,
                        "access_level": "enrolled",
                        "message": "Your trial/subscription program is active.",
                        "href": url_for("modes_bp.modes_checkpoint_route", subject_slug=subj.slug)
                    })

                case ("course", "paid") | ("paid", "paid"):
                    tiles.append({
                        "subject": subj,
                        "slug": subj.slug,
                        "name": subj.name,
                        "access_level": "enrolled",
                        "message": "Your paid program is active.",
                        "href": url_for("modes_bp.modes_checkpoint_route", subject_slug=subj.slug)
                    })

                case _:
                    flash("Enrollment state could not be resolved.", "error")
                    tiles.append({
                        "subject": subj,
                        "slug": subj.slug,
                        "name": subj.name,
                        "href": url_for("payment_bp.pricing_base", subject=subj.slug)
                    })

        if not crm_added:
            from app.models.practice_crm import CrmPracticeUser
            pu = CrmPracticeUser.query.filter_by(user_id=user.id, status='active').first()
            if pu:
                crm_subj = AuthSubject.query.filter_by(slug='practice_crm').first()
                if crm_subj:
                    tiles.append({
                        "subject": crm_subj,
                        "slug": crm_subj.slug,
                        "name": crm_subj.name,
                        "access_level": "enrolled",
                        "message": "You have staff access to Practice CRM.",
                        "href": url_for("modes_bp.modes_checkpoint_route", subject_slug=crm_subj.slug)
                    })

    return render_template("auth/bridge_dashboard.html", user=user, subjects=tiles)

def handle_lifelong_paid_enrollment(user, subj):
    enrollment = UserEnrollment.query.filter_by(user_id=user.id, subject_id=subj.id).first()
    if not enrollment:
        enrollment = UserEnrollment()
        enrollment.user_id = user.id
        enrollment.subject_id = subj.id
        enrollment.status = "pending"
        db.session.add(enrollment)
        db.session.commit()
        print(f"[DEBUG] Created new lifelong paid enrollment for user={user.id}, subject_id={subj.id}")

    # Patch baton/session
    session.update({
        "user_id": enrollment.user_id,
        "enrollment_id": enrollment.id,
        "subject_id": subj.id,
        "subject_slug": subj.slug,
        "program_type": subj.program_type,
        "commercial_mode": subj.commercial_mode,
        "start_endpoint": subj.start_endpoint,
        "about_endpoint": subj.about_endpoint
    })
    print(f"[DEBUG] Bridge baton rebuilt from enrollment={enrollment.id}, subj={subj.slug}")
    return enrollment


def dispatch_enrollment(user, subj):
    if subj.program_type == "lifelong" and subj.commercial_mode == "paid":
        return handle_lifelong_paid_enrollment(user, subj)
    elif subj.program_type == "trial" and subj.commercial_mode == "free":
        return handle_trial_free_enrollment(user, subj)
    # add more branches for other combinations
    print(f"[DEBUG] No helper defined for program_type={subj.program_type}, commercial_mode={subj.commercial_mode}")
    return None

def handle_trial_free_enrollment(user, subj):
    enrollment = UserEnrollment.query.filter_by(user_id=user.id, subject_id=subj.id).first()
    if not enrollment:
        enrollment = UserEnrollment()
        enrollment.user_id = user.id
        enrollment.subject_id = subj.id
        enrollment.status = "trial"
        db.session.add(enrollment)
        db.session.commit()
        print(f"[DEBUG] Created new trial free enrollment for user={user.id}, subject_id={subj.id}")

    # Patch baton/session
    session.update({
        "user_id": enrollment.user_id,
        "enrollment_id": enrollment.id,
        "subject_id": subj.id,
        "subject_slug": subj.slug,
        "program_type": subj.program_type,
        "commercial_mode": subj.commercial_mode,
        "start_endpoint": subj.start_endpoint,
        "about_endpoint": subj.about_endpoint
    })
    print(f"[DEBUG] Bridge baton rebuilt from enrollment={enrollment.id}, subj={subj.slug}")
    return enrollment




