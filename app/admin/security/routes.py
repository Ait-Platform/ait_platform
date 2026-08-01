from flask import current_app, flash, redirect, render_template, request, url_for, session
from flask_login import login_required
from sqlalchemy import text

from app.admin import admin_bp
from app.extensions import db


@admin_bp.route("/security", endpoint="security_dashboard")
@login_required
def security_dashboard():
    return render_template("admin/security/dashboard.html")

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
                amount_cents = amount * 100
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
                    
                    p.local_amount_cents = amount_cents
                    # As per user: e.g. 10000 in local, 2000 in ZAR. 
                    # If we don't have fx rate, just set ZAR to 0 or leave it. 
                    # For now, just set local_amount_cents.
                    p.zar_amount_cents = 0  # We only care about local_amount_cents in checkout
                    
                db.session.commit()
                flash("Bulk pricing updated successfully.", "success")
                
        elif action == "single_set":
            amount = request.form.get("single_amount", type=int)
            country_code = request.form.get("country_code")
            if amount is not None and subject_id and country_code:
                amount_cents = amount * 100
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
                    p.local_amount_cents = amount_cents
                    p.zar_amount_cents = 0
                    db.session.commit()
                    flash(f"Price updated for {c.name}.", "success")
        
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
                SELECT c.alpha2 as country_code, c.name as country_name, c.currency, p.local_amount_cents
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


