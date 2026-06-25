from datetime import timezone
from flask import Blueprint, flash, redirect, session, url_for, render_template
from flask_login import login_required
from app.auth.routes import check_admin, get_all_subjects
from app.bridge.helpers import get_user_enrollment
from app.models.auth import AuthSubject, User, UserEnrollment
from app.extensions import db
from app.modes.helpers import modes_checkpoint
from app.subscription.routes import revoke_user_permissions
from app.time_utils import expiry_for, app_now
from app.utils.enrollment import ensure_free_enrollment


modes_bp = Blueprint("modes_bp", __name__)

@modes_bp.route("/checkpoint", methods=["GET", "POST"])
@login_required
def modes_checkpoint_route():
    from flask_login import current_user
    from flask import request
    from app.quote.routes import get_baton_context

    baton = session.get("baton", {}) or {}
    # Merge any keys stored in the session root (like zar_amount_cents)
    # so that the newly created UserEnrollment gets the correct price.
    ctx = get_baton_context() or {}
    for k, v in ctx.items():
        if v is not None:
            baton[k] = v

    print(f"[DEBUG] checkpoint baton={baton}")

    subj_slug = request.args.get("subject_slug") or (baton.get("subject_slug") or "").strip().lower()
    if not subj_slug:
        flash("No subject provided.", "warning")
        return redirect(url_for("bridge_bp.bridge"))

    subj = AuthSubject.query.filter_by(slug=subj_slug, is_active=1).first_or_404()
    print(f"[DEBUG] modes_checkpoint_route subj={subj.slug} id={subj.id}")

    uid = baton.get("user_id") or getattr(current_user, "id", None)

    # ✅ use uid, not session["user_id"]
    enrollment = UserEnrollment.query.filter_by(
        user_id=uid,
        subject_id=subj.id
    ).first()
    print(f"[DEBUG] modes_checkpoint_route enrollment={enrollment}")


    match (subj.program_type, subj.commercial_mode):

        # --- Free mode ---
        # --- Free mode ---
        case ("free", "free"):
            if not enrollment:
                ensure_free_enrollment(uid, subj.slug)
                flash("You are now enrolled in the free program!", "success")
            else:
                # Restrict re-entry after completion
                if enrollment.status == "completed":
                    flash("You have already completed this free course. Viewing results.", "info")
                    from werkzeug.routing import BuildError
                    try:
                        return redirect(url_for(f"{subj.slug}_bp.result_dashboard"))
                    except BuildError:
                        return redirect(url_for("bridge_bp.bridge"))
                else:
                    flash("You are already enrolled in the free program.", "info")
            return redirect(url_for(subj.start_endpoint))

        # --- Trial + Paid mode ---
        # --- Trial + Paid mode ---
        case ("trial", "paid"):
            # Always use canonical app clock
            now = app_now().astimezone(timezone.utc)

            if enrollment:
                exp = enrollment.expires_at

                # Normalize expiry to UTC as well
                if exp is None:
                    flash("Enrollment has no expiry date.", "error")
                    return redirect(url_for("bridge_bp.bridge"))

                if exp.tzinfo is None:
                    exp = exp.replace(tzinfo=timezone.utc)
                else:
                    exp = exp.astimezone(timezone.utc)

                # --- Run 1: Trial active ---
                if enrollment.trial_count == 0:
                    if now < exp:
                        # Trial still active
                        return redirect(url_for(
                            "payment_bp.checkout_review",
                            subject=subj.slug,
                            enrollment_id=enrollment.id
                        ))
                    else:
                        # Trial expired → mark consumed
                        enrollment.trial_count = 1
                        db.session.commit()
                        return redirect(url_for(
                            "auth_bp.trial_expired_page",
                            enrollment_id=enrollment.id
                        ))

                # --- Run 2+: Subscription lifecycle ---
                elif enrollment.trial_count == 1:
                    if now < exp:
                        flash(
                            f"Your subscription is active until "
                            f"{exp.strftime('%Y-%m-%d %H:%M UTC')}",
                            "success"
                        )
                        return redirect(url_for("bridge_bp.bridge"))
                    else:
                        revoke_user_permissions(enrollment.user_id, program=subj.slug)
                        if enrollment.subscription_id:
                            return redirect(url_for(
                                "subscription_bp.expired_page",
                                subscription_id=enrollment.subscription_id
                            ))
                        else:
                            flash("Subscription expired but no subscription record found.", "warning")
                            return redirect(url_for("bridge_bp.bridge"))

                # Fallback
                flash("Enrollment state could not be resolved.", "error")
                return redirect(url_for("bridge_bp.bridge"))

            else:
                # First run: create trial enrollment
                enrollment = UserEnrollment()
                for k, v in {
                    "user_id": uid,
                    "subject_id": subj.id,
                    "started_at": app_now(),
                    "expires_at": expiry_for(subj, "trial"),
                    "trial_count": 0,
                    "price_id": baton.get("price_id"),
                    "price_version": baton.get("price_version"),
                    "price_locked_at": baton.get("price_locked_at"),
                    "local_currency": baton.get("local_currency"),
                    "local_amount_cents": baton.get("local_amount_cents"),
                    "zar_amount_cents": baton.get("zar_amount_cents"),
                    "country_code": baton.get("country_code"),
                }.items():
                    setattr(enrollment, k, v)
                db.session.add(enrollment)
                db.session.commit()

                flash("Your free trial has started!", "success")
                return redirect(url_for("bridge_bp.bridge"))

        # --- Paid mode ---
        

        # --- Paid mode ---
        case ("paid", "paid"):
            if not enrollment:
                enrollment = UserEnrollment()
                for k, v in {
                    "user_id": uid,
                    "subject_id": subj.id,
                    "started_at": app_now(),
                    "expires_at": expiry_for(subj, "paid"),
                    "price_id": baton.get("price_id"),
                    "price_version": baton.get("price_version"),
                    "price_locked_at": baton.get("price_locked_at"),
                    "local_currency": baton.get("local_currency"),
                    "local_amount_cents": baton.get("local_amount_cents"),
                    "zar_amount_cents": baton.get("zar_amount_cents"),
                    "country_code": baton.get("country_code"),
                }.items():
                    setattr(enrollment, k, v)
                db.session.add(enrollment)
                db.session.commit()

                flash("Proceed to checkout to complete your enrollment.", "info")
                return redirect(url_for(
                    "payment_bp.checkout_review",
                    subject=subj.slug,
                    enrollment_id=enrollment.id
                ))
            else:
                if enrollment.status in ("pending_payment", "pending _payment", "pending"):
                    flash("You have a pending enrollment. Please complete checkout.", "info")
                    return redirect(url_for(
                        "payment_bp.checkout_review",
                        subject=subj.slug,
                        enrollment_id=enrollment.id
                    ))

                # Normalize both now and expiry to UTC
                now = app_now().astimezone(timezone.utc).replace(microsecond=0)
                exp = enrollment.expires_at
                if exp and exp.tzinfo is None:
                    exp = exp.replace(tzinfo=timezone.utc)
                else:
                    exp = exp.astimezone(timezone.utc)

                if now < exp:
                    return redirect(url_for(subj.start_endpoint))
                else:
                    flash("Your enrollment has expired. Please renew.", "warning")
                    return redirect(url_for(
                        "payment_bp.checkout_review",
                        subject=subj.slug,
                        enrollment_id=enrollment.id
                    ))



        # --- Lifelong + Paid (Cultural Fire) ---
        # --- Lifelong + Paid (Cultural Fire) ---
        case ("lifelong", "paid"):
            if not enrollment:
                enrollment = UserEnrollment()
                for k, v in {
                    "user_id": uid,
                    "subject_id": subj.id,
                    "started_at": app_now(),
                    "expires_at": None,   # no expiry for lifelong programs
                    "trial_count": 0,
                    "price_id": baton.get("price_id"),
                    "price_version": baton.get("price_version"),
                    "price_locked_at": baton.get("price_locked_at"),
                    "local_currency": baton.get("local_currency"),
                    "local_amount_cents": baton.get("local_amount_cents"),
                    "zar_amount_cents": baton.get("zar_amount_cents"),
                    "country_code": baton.get("country_code"),
                }.items():
                    setattr(enrollment, k, v)
                db.session.add(enrollment)
                db.session.commit()
                # Flash for new enrollment is okay, but let's pass a specific session flag or just let the tile show it.
                # Actually, if we just remove it, the tile will always show the "enrolled" message.
                flash("You are now successfully enrolled in Cultural Fire.", "success")
            else:
                pass # Removed redundant welcome back flash

            #return redirect(url_for("cultural_bp.cultural_fire_home", subject_id=subj.id))
            #return render_template("auth/bridge_dashboard.html", subjects=tiles)
            return redirect(url_for("bridge_bp.bridge"))
        
        # --- Admin mode ---
        case ("admin", "admin"):
            return redirect(url_for(subj.start_endpoint))

        # --- Fallback ---
        case _:
            user_id = baton.get("user_id") or session.get("user_id")
            user = User.query.get(user_id)
            is_admin_global = check_admin(user.email)
            enrollment_rows = get_user_enrollment(user.id)
            enrollments = {e.subject_id: e for e in enrollment_rows}
            subjects = [s for s in get_all_subjects() if s.id in enrollments]
            tiles = modes_checkpoint(user, subjects, enrollments, is_admin_global)
            return render_template("auth/bridge_dashboard.html", subjects=tiles)


