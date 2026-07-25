from flask import Blueprint, render_template, redirect, url_for, flash, request
from flask_login import login_required, current_user
from app import db

healthcore_bp = Blueprint("healthcore_bp", __name__)

@healthcore_bp.route("/program/healthcore")
def healthcore_home():
    return render_template("program_healthcore/welcome.html")

@healthcore_bp.route("/program/healthcore/about")
def healthcore_about():
    return render_template("program_healthcore/about.html")

@healthcore_bp.route("/program/healthcore/pricing")
def healthcore_pricing():
    return render_template("program_healthcore/pricing.html")

@healthcore_bp.route("/program/healthcore/dashboard")
@login_required
def healthcore_dashboard():
    return render_template("program_healthcore/dashboard.html")

# ---------------------------------------------------------
# ENGINE DASHBOARDS
# ---------------------------------------------------------

@healthcore_bp.route("/program/healthcore/engine/laboratory", methods=["GET"])
@login_required
def laboratory_dashboard():
    from app.models.healthcore import HcLaboratory
    # Fetch all laboratory records for the current user, ordered by date descending
    records = HcLaboratory.query.filter_by(user_id=current_user.id).order_by(HcLaboratory.report_date.desc()).all()
    return render_template("program_healthcore/laboratory.html", records=records)

@healthcore_bp.route("/program/healthcore/engine/laboratory/add", methods=["POST"])
@login_required
def laboratory_add():
    from app.models.healthcore import HcLaboratory
    from datetime import datetime

    test_name = request.form.get("test_name")
    value = request.form.get("value", type=float)
    units = request.form.get("units")
    reference_range = request.form.get("reference_range")
    report_date_str = request.form.get("report_date")
    status = request.form.get("status")

    if test_name and value is not None and report_date_str:
        report_date = datetime.strptime(report_date_str, "%Y-%m-%d").date()
        new_record = HcLaboratory(
            user_id=current_user.id,
            report_date=report_date,
            test_name=test_name,
            value=value,
            units=units,
            reference_range=reference_range,
            status=status
        )
        db.session.add(new_record)
        db.session.commit()
        flash("Laboratory record added successfully!", "success")
    else:
        flash("Please provide all required fields.", "danger")

    return redirect(url_for("healthcore_bp.laboratory_dashboard"))

@healthcore_bp.route("/program/healthcore/engine/medication", methods=["GET"])
@login_required
def medication_dashboard():
    from app.models.healthcore import HcMedication
    records = HcMedication.query.filter_by(user_id=current_user.id).order_by(HcMedication.created_at.desc()).all()
    return render_template("program_healthcore/medication.html", records=records)

@healthcore_bp.route("/program/healthcore/engine/medication/add", methods=["POST"])
@login_required
def add_medication():
    from app.models.healthcore import HcMedication
    from datetime import datetime

    medication_name = request.form.get("medication_name")
    date_prescribed = request.form.get("date_prescribed")
    dosage = request.form.get("dosage")
    frequency = request.form.get("frequency")
    status = request.form.get("status", "Active")

    if not medication_name:
        flash("Medication name is required.", "danger")
        return redirect(url_for("healthcore_bp.medication_dashboard"))

    try:
        dp = datetime.strptime(date_prescribed, "%Y-%m-%d").date() if date_prescribed else None
    except ValueError:
        dp = None

    record = HcMedication(
        user_id=current_user.id,
        medication_name=medication_name,
        date_prescribed=dp,
        dosage=dosage,
        frequency=frequency,
        status=status,
        adherence_score=100.0
    )
    db.session.add(record)
    db.session.commit()
    flash(f"Medication '{medication_name}' added successfully.", "success")
    return redirect(url_for("healthcore_bp.medication_dashboard"))

@healthcore_bp.route("/program/healthcore/engine/nutrition", methods=["GET"])
@login_required
def nutrition_dashboard():
    from app.models.healthcore import HcNutrition
    records = HcNutrition.query.filter_by(user_id=current_user.id).order_by(HcNutrition.log_date.desc(), HcNutrition.created_at.desc()).all()
    return render_template("program_healthcore/nutrition.html", records=records)

@healthcore_bp.route("/program/healthcore/engine/nutrition/add", methods=["POST"])
@login_required
def add_nutrition():
    from app.models.healthcore import HcNutrition
    from datetime import datetime

    log_date_str = request.form.get("log_date")
    entry_type = request.form.get("entry_type", "Meal")
    description = request.form.get("description")
    
    try:
        log_date = datetime.strptime(log_date_str, "%Y-%m-%d").date() if log_date_str else datetime.utcnow().date()
    except ValueError:
        log_date = datetime.utcnow().date()
        
    calories_kcal = float(request.form.get("calories_kcal") or 0)
    protein_g = float(request.form.get("protein_g") or 0)
    carbs_g = float(request.form.get("carbs_g") or 0)
    fats_g = float(request.form.get("fats_g") or 0)
    water_ml = float(request.form.get("water_ml") or 0)

    if not description:
        flash("Description is required.", "danger")
        return redirect(url_for("healthcore_bp.nutrition_dashboard"))

    record = HcNutrition(
        user_id=current_user.id,
        log_date=log_date,
        entry_type=entry_type,
        description=description,
        calories_kcal=calories_kcal,
        protein_g=protein_g,
        carbs_g=carbs_g,
        fats_g=fats_g,
        water_ml=water_ml
    )
    db.session.add(record)
    db.session.commit()
    flash("Nutrition log added successfully.", "success")
    return redirect(url_for("healthcore_bp.nutrition_dashboard"))

@healthcore_bp.route("/program/healthcore/engine/imaging")
@login_required
def imaging_dashboard():
    return render_template("program_healthcore/imaging.html")

@healthcore_bp.route("/program/healthcore/engine/lifestyle")
@login_required
def lifestyle_dashboard():
    return render_template("program_healthcore/lifestyle.html")

@healthcore_bp.route("/program/healthcore/engine/timeline")
@login_required
def timeline_dashboard():
    return render_template("program_healthcore/timeline.html")

@healthcore_bp.route("/program/healthcore/engine/risk")
@login_required
def risk_dashboard():
    return render_template("program_healthcore/risk.html")

@healthcore_bp.route("/program/healthcore/engine/correlation")
@login_required
def correlation_dashboard():
    return render_template("program_healthcore/correlation.html")

@healthcore_bp.route("/program/healthcore/engine/reporting")
@login_required
def reporting_dashboard():
    return render_template("program_healthcore/reporting.html")
