from flask import current_app, flash, redirect, render_template, request, url_for, session
from flask_login import login_required
from sqlalchemy import text

from app.admin import admin_bp
from app.extensions import db


@admin_bp.route("/security", endpoint="security_dashboard")
@login_required
def security_dashboard():
    return render_template("admin/security/dashboard.html")

@admin_bp.route("/security/communication-logs", endpoint="security_communication_logs")
@login_required
def security_communication_logs():
    if not (session.get("is_admin") or session.get("role") == "admin"):
        return redirect(url_for("public_bp.welcome"))

    from app.models.auth import InviteLog
    logs = InviteLog.query.order_by(InviteLog.sent_at.desc()).all()
    return render_template("shared/invite_logs_page.html", logs=logs, is_admin_view=True, back_url=url_for("admin_bp.security_dashboard"))

@admin_bp.route("/security/paystack-logs", endpoint="paystack_payment_logs")
@login_required
def paystack_payment_logs():
    if not (session.get("is_admin") or session.get("role") == "admin"):
        return redirect(url_for("public_bp.welcome"))

    from app.models.payment import PaystackPayment
    payments = PaystackPayment.query.order_by(PaystackPayment.created_at.desc()).all()
    return render_template("admin/security/paystack_logs.html", payments=payments)

@admin_bp.route("/security/pricing", methods=["GET", "POST"], endpoint="manage_pricing")
@login_required
def manage_pricing():
    if not (session.get("is_admin") or session.get("role") == "admin"):
        return redirect(url_for("public_bp.welcome"))

    from app.models.auth import AuthSubject
    from app.models.payment import SubjectCountryPrice, RefCountryCurrency

    if request.method == "POST":
        subject_id = request.form.get("subject_id")
        action = request.form.get("action")
        
        if action == "bulk_set":
            amount = request.form.get("bulk_amount", type=int)
            if amount is not None and subject_id:
                base_amount_cents = amount * 100
                countries = RefCountryCurrency.query.filter_by(is_active=True).all()
                for c in countries:
                    # Upsert pricing
                    p = SubjectCountryPrice.query.filter_by(subject_id=subject_id, country_code=c.alpha2).first()
                    if not p:
                        p = SubjectCountryPrice(
                            subject_id=subject_id,
                            country_code=c.alpha2,
                            local_currency=c.currency,
                        )
                        db.session.add(p)
                    
                    local_cents = max(base_amount_cents, 15000)
                    fx = float(c.fx_to_zar) if c.fx_to_zar else 0.0
                    if fx > 0:
                        computed_zar = int(local_cents * fx)
                        if computed_zar < 3000:
                            local_cents = int(3000 / fx)
                            computed_zar = max(3000, int(local_cents * fx))
                        p.local_amount_cents = max(1, local_cents)
                        p.zar_amount_cents = max(1, computed_zar)
                    else:
                        p.local_amount_cents = max(1, local_cents)
                        p.zar_amount_cents = max(3000, computed_zar if 'computed_zar' in locals() else 3000)
                      
                db.session.commit()
                flash("Bulk pricing updated successfully. Prices below 30 ZAR equivalent were automatically adjusted.", "success")
                
        elif action == "single_set":
            amount = request.form.get("single_amount", type=int)
            country_code = request.form.get("country_code")
            if amount is not None and subject_id and country_code:
                local_cents = max(amount * 100, 15000)
                c = RefCountryCurrency.query.filter_by(alpha2=country_code).first()
                if c:
                    p = SubjectCountryPrice.query.filter_by(subject_id=subject_id, country_code=c.alpha2).first()
                    if not p:
                        p = SubjectCountryPrice(
                            subject_id=subject_id,
                            country_code=c.alpha2,
                            local_currency=c.currency,
                        )
                        db.session.add(p)
                        
                    fx = float(c.fx_to_zar) if c.fx_to_zar else 0.0
                    if fx > 0:
                        computed_zar = int(local_cents * fx)
                        if computed_zar < 3000:
                            local_cents = int(3000 / fx)
                            computed_zar = max(3000, int(local_cents * fx))
                        p.local_amount_cents = max(1, local_cents)
                        p.zar_amount_cents = max(1, computed_zar)
                    else:
                        p.local_amount_cents = max(1, local_cents)
                        p.zar_amount_cents = max(3000, computed_zar if 'computed_zar' in locals() else 3000)
                        
                    db.session.commit()
                    flash(f"Price updated for {c.name}.", "success")
        
        elif action == "multi_set":
            if subject_id:
                for key, val in request.form.items():
                    if key.startswith("amount_") and val.strip():
                        country_code = key.replace("amount_", "")
                        amount = int(val)
                        local_cents = max(amount * 100, 15000)
                        c = RefCountryCurrency.query.filter_by(alpha2=country_code).first()
                        if c:
                            p = SubjectCountryPrice.query.filter_by(subject_id=subject_id, country_code=c.alpha2).first()
                            if not p:
                                p = SubjectCountryPrice(
                                    subject_id=subject_id,
                                    country_code=c.alpha2,
                                    local_currency=c.currency,
                                )
                                db.session.add(p)
                                
                            fx = float(c.fx_to_zar) if c.fx_to_zar else 0.0
                            if fx > 0:
                                computed_zar = int(local_cents * fx)
                                if computed_zar < 3000:
                                    local_cents = int(3000 / fx)
                                    computed_zar = int(local_cents * fx)
                                p.local_amount_cents = local_cents
                                p.zar_amount_cents = computed_zar
                            else:
                                p.local_amount_cents = local_cents
                                p.zar_amount_cents = max(3000, computed_zar if 'computed_zar' in locals() else 3000)
                db.session.commit()
                flash("All global prices updated successfully.", "success")
        
        return redirect(url_for("admin_bp.manage_pricing", subject_id=subject_id))

    # GET request
    subjects = AuthSubject.query.filter_by(is_active=1).order_by(AuthSubject.name).all()
    selected_subject_id = request.args.get("subject_id", type=int)
    if not selected_subject_id and subjects:
        selected_subject_id = subjects[0].id

    prices = []
    if selected_subject_id:
        prices = db.session.execute(
            text("""
                SELECT c.alpha2 as country_code, c.name as country_name, c.currency, p.local_amount_cents, p.zar_amount_cents
                FROM ref_country_currency c
                LEFT JOIN subject_country_price p ON c.alpha2 = p.country_code AND p.subject_id = :sid
                WHERE c.is_active = true
                ORDER BY c.name
            """), {"sid": selected_subject_id}
        ).mappings().all()

    return render_template(
        "admin/security/pricing.html",
        subjects=subjects,
        selected_subject_id=selected_subject_id,
        prices=prices
    )

@admin_bp.route("/settings", methods=["GET", "POST"])
def global_settings():
    if request.method == "POST":
        quote_cents = request.form.get("mechanic_quote_cents")
        invoice_cents = request.form.get("mechanic_invoice_cents")
        enquiry_cents = request.form.get("practice_enquiry_cents")
        
        hds_cents = request.form.get("hds_subscription_cents")
        adv_reg_cents = request.form.get("adv_math_registration_cents")
        adv_sub_cents = request.form.get("adv_math_subtopic_cents")
        
        bil_base = request.form.get("bil_base_price")
        bil_inc = request.form.get("bil_included_meters")
        bil_extra = request.form.get("bil_extra_meter_price")

        updates = []
        if quote_cents: updates.append(('mechanic_quote_cents', quote_cents))
        if invoice_cents: updates.append(('mechanic_invoice_cents', invoice_cents))
        if enquiry_cents: updates.append(('practice_enquiry_cents', enquiry_cents))
        if hds_cents: updates.append(('hds_subscription_cents', hds_cents))
        if adv_reg_cents: updates.append(('adv_math_registration_cents', adv_reg_cents))
        if adv_sub_cents: updates.append(('adv_math_subtopic_cents', adv_sub_cents))
        
        for key, val in updates:
            db.session.execute(text("INSERT INTO system_settings (key, value) VALUES (:k, :v) ON CONFLICT(key) DO UPDATE SET value=excluded.value"), {"k": key, "v": val})
            
        from app.models.billing import BilPlatformSettings
        bil_settings = BilPlatformSettings.query.first()
        if not bil_settings:
            bil_settings = BilPlatformSettings(base_price_cents=10000, included_meters=2, extra_meter_price_cents=1500)
            db.session.add(bil_settings)
        if bil_base: bil_settings.base_price_cents = int(float(bil_base) * 100)
        if bil_inc: bil_settings.included_meters = int(bil_inc)
        if bil_extra: bil_settings.extra_meter_price_cents = int(float(bil_extra) * 100)
            
        db.session.commit()
        flash("Global settings updated successfully", "success")
        return redirect(url_for("admin_bp.global_settings"))
        
    settings = db.session.execute(text("SELECT key, value FROM system_settings")).fetchall()
    settings_dict = {s.key: s.value for s in settings}
    
    from app.models.billing import BilPlatformSettings
    bil_settings = BilPlatformSettings.query.first()
    if not bil_settings:
        bil_settings = BilPlatformSettings(base_price_cents=10000, included_meters=2, extra_meter_price_cents=1500)
        db.session.add(bil_settings)
        db.session.commit()
        
    return render_template("admin/settings.html", settings=settings_dict, bil_settings=bil_settings)


@admin_bp.route('/security/sace-management', methods=['GET', 'POST'], endpoint='sace_management')
@login_required
def sace_management():
    if not (session.get('is_admin') or session.get('role') == 'admin'):
        return redirect(url_for('public_bp.welcome'))
        
    from app.models.auth import User, UserEnrollment
    try:
        from app.models.subject import AuthSubject
    except ImportError:
        from app.models.auth import AuthSubject
        
    from app.models.sace import SaceDocument
    from werkzeug.security import generate_password_hash
    from werkzeug.utils import secure_filename
    import os
    
    sace_subject = AuthSubject.query.filter_by(slug='sace_hub').first()
    upload_folder = os.path.join(current_app.static_folder, 'uploads', 'sace')
    os.makedirs(upload_folder, exist_ok=True)
    
    if request.method == 'POST':
        action = request.form.get('action')
        
        if action == 'create_evaluator':
            name = request.form.get('name')
            email = request.form.get('email')
            password = request.form.get('password')
            
            if User.query.filter_by(email=email).first():
                flash('Email already exists.', 'error')
            else:
                user = User(
                    name=name,
                    email=email,
                    password_hash=generate_password_hash(password),
                    is_active=1
                )
                db.session.add(user)
                db.session.commit()
                
                # Grant robust AuthSubjectAdmin rights to ONLY the selected SACE subject
                from app.models.auth import AuthSubjectAdmin
                assigned_slug = request.form.get('assigned_subject_slug')
                
                target_subject = AuthSubject.query.filter_by(slug=assigned_slug).first()
                if not target_subject:
                    flash(f'The selected SACE activity ({assigned_slug}) was not found in the database.', 'error')
                else:
                    # Optional: still enroll them in sace_hub for legacy dashboard access if needed
                    if sace_subject:
                        enrollment = UserEnrollment(
                            user_id=user.id,
                            subject_id=sace_subject.id,
                            status='active'
                        )
                        db.session.add(enrollment)
                        
                    admin_grant = AuthSubjectAdmin(email=email, subject_id=target_subject.id)
                    db.session.add(admin_grant)
                        
                    db.session.commit()
                    flash(f'Created SACE personnel account for {email} and granted access strictly to {target_subject.name}.', 'success')
                    
        elif action == 'upload_document':
            slug = request.form.get('slug')
            doc_type = request.form.get('document_type')
            file = request.files.get('file')
            
            if not file or file.filename == '':
                flash('No file selected.', 'error')
            else:
                filename = secure_filename(file.filename)
                save_path = os.path.join(upload_folder, f"{slug}_{doc_type}_{filename}")
                file.save(save_path)
                
                # Update or create document record
                existing_doc = SaceDocument.query.filter_by(slug=slug, document_type=doc_type).first()
                if existing_doc:
                    existing_doc.file_name = filename
                    existing_doc.file_path = f"uploads/sace/{slug}_{doc_type}_{filename}"
                    from datetime import datetime
                    existing_doc.uploaded_at = datetime.utcnow()
                else:
                    new_doc = SaceDocument(
                        slug=slug,
                        document_type=doc_type,
                        file_name=filename,
                        file_path=f"uploads/sace/{slug}_{doc_type}_{filename}"
                    )
                    db.session.add(new_doc)
                db.session.commit()
                flash(f'Successfully uploaded {doc_type} for {slug}.', 'success')
                    
    # Get list of evaluators
    evaluators = []
    if sace_subject:
        enrollments = UserEnrollment.query.filter_by(subject_id=sace_subject.id, status='active').all()
        evaluators = [e.user for e in enrollments]
        
    documents = SaceDocument.query.all()
    
    return render_template('admin/security/sace_management.html', evaluators=evaluators, documents=documents)


