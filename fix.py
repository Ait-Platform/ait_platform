import re

content = open('app/program_healthcore/routes.py', 'r').read()
content = content.replace('from flask import Blueprint, render_template, redirect, url_for, flash, request, current_app', 'from flask import Blueprint, render_template, redirect, url_for, flash, request, current_app\nfrom functools import wraps')

onboarding_code = """
# ---------------------------------------------------------
# ONBOARDING DECORATOR & ROUTES
# ---------------------------------------------------------
def healthcore_onboarded_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        from app.models.healthcore import HcPatientProfile
        profile = HcPatientProfile.query.filter_by(user_id=current_user.id).first()
        if not profile:
            flash("Please complete your baseline profile and consent to continue.", "info")
            return redirect(url_for("healthcore_bp.healthcore_onboarding"))
        return f(*args, **kwargs)
    return decorated_function

@healthcore_bp.route("/program/healthcore/onboarding", methods=["GET", "POST"])
@login_required
def healthcore_onboarding():
    from app.models.healthcore import HcPatientProfile, HcConsentLog
    from app.program_healthcore.forms import HcOnboardingForm
    
    profile = HcPatientProfile.query.filter_by(user_id=current_user.id).first()
    if profile:
        return redirect(url_for("healthcore_bp.healthcore_dashboard"))
        
    form = HcOnboardingForm()
    if form.validate_on_submit():
        new_profile = HcPatientProfile(
            user_id=current_user.id,
            dob=form.dob.data,
            biological_sex=form.biological_sex.data,
            blood_type=form.blood_type.data,
            height_cm=form.height_cm.data,
            weight_kg=form.weight_kg.data,
            chronic_conditions=form.chronic_conditions.data
        )
        db.session.add(new_profile)
        
        consent = HcConsentLog(
            user_id=current_user.id,
            consent_type="ai_processing",
            ip_address=request.remote_addr,
            user_agent=request.headers.get("User-Agent")
        )
        db.session.add(consent)
        
        db.session.commit()
        flash("Welcome to Health IQ! Your profile is set up.", "success")
        return redirect(url_for("healthcore_bp.healthcore_dashboard"))
        
    return render_template("program_healthcore/onboarding.html", form=form)

"""

content = content.replace('# ---------------------------------------------------------\n# ENGINE DASHBOARDS\n# ---------------------------------------------------------', onboarding_code + '# ---------------------------------------------------------\n# ENGINE DASHBOARDS\n# ---------------------------------------------------------')

content = content.replace('@login_required\ndef healthcore_dashboard()', '@login_required\n@healthcore_onboarded_required\ndef healthcore_dashboard()')
content = content.replace('@login_required\ndef laboratory_dashboard()', '@login_required\n@healthcore_onboarded_required\ndef laboratory_dashboard()')
content = content.replace('@login_required\ndef medication_dashboard()', '@login_required\n@healthcore_onboarded_required\ndef medication_dashboard()')
content = content.replace('@login_required\ndef nutrition_dashboard()', '@login_required\n@healthcore_onboarded_required\ndef nutrition_dashboard()')
content = content.replace('@login_required\ndef imaging_dashboard()', '@login_required\n@healthcore_onboarded_required\ndef imaging_dashboard()')
content = content.replace('@login_required\ndef lifestyle_dashboard()', '@login_required\n@healthcore_onboarded_required\ndef lifestyle_dashboard()')
content = content.replace('@login_required\ndef timeline_dashboard()', '@login_required\n@healthcore_onboarded_required\ndef timeline_dashboard()')
content = content.replace('@login_required\ndef risk_dashboard()', '@login_required\n@healthcore_onboarded_required\ndef risk_dashboard()')
content = content.replace('@login_required\ndef report_generate()', '@login_required\n@healthcore_onboarded_required\ndef report_generate()')

content = content.replace('def healthcore_dashboard():\n    return render_template("program_healthcore/dashboard.html")', 'def healthcore_dashboard():\n    from app.models.healthcore import HcPatientProfile\n    profile = HcPatientProfile.query.filter_by(user_id=current_user.id).first()\n    return render_template("program_healthcore/dashboard.html", profile=profile)')

# Add nutrition logic
add_nutrition = """
@healthcore_bp.route("/program/healthcore/engine/nutrition/add", methods=["POST"])
@login_required
@healthcore_onboarded_required
def add_nutrition():
    from app.models.healthcore import HcNutrition
    from datetime import datetime
    
    log_date_str = request.form.get("log_date")
    entry_type = request.form.get("entry_type")
    description = request.form.get("description")
    calories = request.form.get("calories_kcal", type=float) or 0.0
    protein = request.form.get("protein_g", type=float) or 0.0
    carbs = request.form.get("carbs_g", type=float) or 0.0
    fats = request.form.get("fats_g", type=float) or 0.0
    water = request.form.get("water_ml", type=float) or 0.0
    
    if not log_date_str or not entry_type:
        flash("Date and Entry Type are required.", "danger")
        return redirect(url_for("healthcore_bp.nutrition_dashboard"))
        
    try:
        ld = datetime.strptime(log_date_str, "%Y-%m-%d").date()
    except ValueError:
        ld = datetime.utcnow().date()
        
    record = HcNutrition(
        user_id=current_user.id,
        log_date=ld,
        entry_type=entry_type,
        description=description,
        calories_kcal=calories,
        protein_g=protein,
        carbs_g=carbs,
        fats_g=fats,
        water_ml=water
    )
    db.session.add(record)
    db.session.commit()
    flash("Nutrition entry added successfully!", "success")
    return redirect(url_for("healthcore_bp.nutrition_dashboard"))
"""

add_lifestyle = """
@healthcore_bp.route("/program/healthcore/engine/lifestyle/add", methods=["POST"])
@login_required
@healthcore_onboarded_required
def add_lifestyle():
    from app.models.healthcore import HcLifestyle
    from datetime import datetime
    
    log_date_str = request.form.get("log_date")
    category = request.form.get("category")
    metric_name = request.form.get("metric_name")
    value_str = request.form.get("value_str")
    value_num = request.form.get("value_num", type=float)
    units = request.form.get("units")
    
    if not log_date_str or not category or not metric_name:
        flash("Date, Category, and Metric Name are required.", "danger")
        return redirect(url_for("healthcore_bp.lifestyle_dashboard"))
        
    try:
        ld = datetime.strptime(log_date_str, "%Y-%m-%d").date()
    except ValueError:
        ld = datetime.utcnow().date()
        
    record = HcLifestyle(
        user_id=current_user.id,
        log_date=ld,
        category=category,
        metric_name=metric_name,
        value_str=value_str,
        value_num=value_num,
        units=units
    )
    db.session.add(record)
    db.session.commit()
    flash("Lifestyle entry added successfully!", "success")
    return redirect(url_for("healthcore_bp.lifestyle_dashboard"))
"""

content = content.replace('def nutrition_dashboard():\n    from app.models.healthcore import HcNutrition\n    records = HcNutrition.query.filter_by(user_id=current_user.id).order_by(HcNutrition.log_date.desc(), HcNutrition.created_at.desc()).all()\n    return render_template("program_healthcore/nutrition.html", records=records)', 'def nutrition_dashboard():\n    from app.models.healthcore import HcNutrition\n    records = HcNutrition.query.filter_by(user_id=current_user.id).order_by(HcNutrition.log_date.desc(), HcNutrition.created_at.desc()).all()\n    return render_template("program_healthcore/nutrition.html", records=records)\n' + add_nutrition)

content = content.replace('def lifestyle_dashboard():\n    from app.models.healthcore import HcLifestyle\n    records = HcLifestyle.query.filter_by(user_id=current_user.id).order_by(HcLifestyle.log_date.desc(), HcLifestyle.created_at.desc()).all()\n    return render_template("program_healthcore/lifestyle.html", records=records)', 'def lifestyle_dashboard():\n    from app.models.healthcore import HcLifestyle\n    records = HcLifestyle.query.filter_by(user_id=current_user.id).order_by(HcLifestyle.log_date.desc(), HcLifestyle.created_at.desc()).all()\n    return render_template("program_healthcore/lifestyle.html", records=records)\n' + add_lifestyle)

open('app/program_healthcore/routes.py', 'w').write(content)
