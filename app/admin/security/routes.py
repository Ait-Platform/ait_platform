from flask import current_app, flash, redirect, render_template, request, url_for
from flask_login import login_required
from sqlalchemy import text

from app.admin import admin_bp
from app.extensions import db


@admin_bp.route("/security", endpoint="security_dashboard")
@login_required
def security_dashboard():
    return render_template("admin/security/dashboard.html")

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


