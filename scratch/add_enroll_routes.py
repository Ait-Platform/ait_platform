import os

routes_addition = """
@sace_bp.route('/sace/enroll/<activity_slug>', methods=['GET', 'POST'])
def enroll(activity_slug):
    from flask import request, session, redirect, url_for, render_template
    from app.payments.pricing import price_for_country
    from app.utils.country_list import resolve_country
    
    subject = f"sace_{activity_slug}"
    country = resolve_country(request)
    price_info = price_for_country(country, subject)
    
    if request.method == 'POST':
        action = request.form.get("action") # 'pay' or 'voucher'
        voucher = request.form.get("voucher", "").strip()
        
        session["sace_enroll_intent"] = {
            "activity_slug": activity_slug,
            "action": action,
            "voucher": voucher
        }
        
        # Go to register to collect email
        return redirect(url_for("auth_bp.register", subject=subject, next=url_for("sace_bp.enroll_finalize")))
        
    return render_template('program_sace/enroll_public.html', activity_slug=activity_slug, price_info=price_info)

@sace_bp.route('/sace/enroll/finalize', methods=['GET'])
def enroll_finalize():
    from flask import session, redirect, url_for, flash
    from flask_login import current_user
    from app.extensions import db
    from app.models.payment import VoucherToken
    from app.services.enrollment import _ensure_enrollment_row
    from app.auth.pricing_helpers import mark_loss_enrollment_free
    from datetime import datetime
    
    intent = session.pop("sace_enroll_intent", None)
    if not intent or not getattr(current_user, 'is_authenticated', False):
        return redirect(url_for("sace_bp.catalog"))
        
    activity_slug = intent["activity_slug"]
    subject = f"sace_{activity_slug}"
    user_id = current_user.id
    
    # Ensure enrollment exists
    enrollment_id = _ensure_enrollment_row(user_id=user_id, subject_slug=subject)
    
    if intent["action"] == "voucher" and intent["voucher"]:
        v_obj = VoucherToken.query.filter_by(code=intent["voucher"], is_used=False).first()
        if v_obj:
            v_obj.is_used = True
            v_obj.used_by_user_id = user_id
            v_obj.used_at = datetime.utcnow()
            db.session.commit()
            
            # Activate enrollment
            mark_loss_enrollment_free(enrollment_id)
            flash("Access code applied successfully!", "success")
            return redirect(url_for("sace_bp.selection_hub", activity_slug=activity_slug))
        else:
            flash("Invalid or expired access code.", "danger")
            return redirect(url_for("sace_bp.enroll", activity_slug=activity_slug))
            
    # If they want to pay, redirect to the Paystack initiator
    return redirect(url_for("paystack_bp.paystack_start", email=current_user.email, subject=subject, debug=0))
"""

with open('app/program_sace/routes.py', 'a', encoding='utf-8') as f:
    f.write(routes_addition)
print("Added public enroll routes")
