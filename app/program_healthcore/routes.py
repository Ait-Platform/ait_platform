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
    from app.models.healthcore import HcImaging
    records = HcImaging.query.filter_by(user_id=current_user.id).order_by(HcImaging.scan_date.desc(), HcImaging.created_at.desc()).all()
    return render_template("program_healthcore/imaging.html", records=records)

@healthcore_bp.route("/program/healthcore/engine/imaging/add", methods=["POST"])
@login_required
def add_imaging():
    from app.models.healthcore import HcImaging
    from datetime import datetime

    scan_date_str = request.form.get("scan_date")
    modality = request.form.get("modality", "Other")
    body_part = request.form.get("body_part")
    findings = request.form.get("findings")
    impression = request.form.get("impression")
    
    try:
        scan_date = datetime.strptime(scan_date_str, "%Y-%m-%d").date() if scan_date_str else datetime.utcnow().date()
    except ValueError:
        scan_date = datetime.utcnow().date()

    if not body_part:
        flash("Body part/region is required.", "danger")
        return redirect(url_for("healthcore_bp.imaging_dashboard"))

    record = HcImaging(
        user_id=current_user.id,
        scan_date=scan_date,
        modality=modality,
        body_part=body_part,
        findings=findings,
        impression=impression
    )
    db.session.add(record)
    db.session.commit()
    flash("Imaging record logged successfully.", "success")
    return redirect(url_for("healthcore_bp.imaging_dashboard"))

@healthcore_bp.route("/program/healthcore/engine/lifestyle")
@login_required
def lifestyle_dashboard():
    from app.models.healthcore import HcLifestyle
    records = HcLifestyle.query.filter_by(user_id=current_user.id).order_by(HcLifestyle.log_date.desc(), HcLifestyle.created_at.desc()).all()
    return render_template("program_healthcore/lifestyle.html", records=records)

@healthcore_bp.route("/program/healthcore/engine/lifestyle/add", methods=["POST"])
@login_required
def add_lifestyle():
    from app.models.healthcore import HcLifestyle
    from datetime import datetime

    log_date_str = request.form.get("log_date")
    category = request.form.get("category", "Habit")
    metric_name = request.form.get("metric_name")
    value_str = request.form.get("value_str")
    
    value_num_raw = request.form.get("value_num")
    value_num = float(value_num_raw) if value_num_raw else None
    
    units = request.form.get("units")
    
    try:
        log_date = datetime.strptime(log_date_str, "%Y-%m-%d").date() if log_date_str else datetime.utcnow().date()
    except ValueError:
        log_date = datetime.utcnow().date()

    if not metric_name:
        flash("Metric name is required.", "danger")
        return redirect(url_for("healthcore_bp.lifestyle_dashboard"))

    record = HcLifestyle(
        user_id=current_user.id,
        log_date=log_date,
        category=category,
        metric_name=metric_name,
        value_str=value_str,
        value_num=value_num,
        units=units
    )
    db.session.add(record)
    db.session.commit()
    flash("Lifestyle metric logged successfully.", "success")
    return redirect(url_for("healthcore_bp.lifestyle_dashboard"))

@healthcore_bp.route("/program/healthcore/engine/timeline")
@login_required
def timeline_dashboard():
    from app.models.healthcore import HcTimelineEvent
    records = HcTimelineEvent.query.filter_by(user_id=current_user.id).order_by(HcTimelineEvent.start_date.desc(), HcTimelineEvent.created_at.desc()).all()
    return render_template("program_healthcore/timeline.html", records=records)

@healthcore_bp.route("/program/healthcore/engine/timeline/add", methods=["POST"])
@login_required
def add_timeline():
    from app.models.healthcore import HcTimelineEvent
    from datetime import datetime

    start_date_str = request.form.get("start_date")
    end_date_str = request.form.get("end_date")
    category = request.form.get("category", "Other")
    title = request.form.get("title")
    description = request.form.get("description")
    
    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date() if start_date_str else datetime.utcnow().date()
    except ValueError:
        start_date = datetime.utcnow().date()
        
    end_date = None
    if end_date_str:
        try:
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
        except ValueError:
            pass

    if not title:
        flash("Event title is required.", "danger")
        return redirect(url_for("healthcore_bp.timeline_dashboard"))

    record = HcTimelineEvent(
        user_id=current_user.id,
        start_date=start_date,
        end_date=end_date,
        category=category,
        title=title,
        description=description,
        source_engine='Manual'
    )
    db.session.add(record)
    db.session.commit()
    flash("Timeline event logged successfully.", "success")
    return redirect(url_for("healthcore_bp.timeline_dashboard"))

@healthcore_bp.route("/program/healthcore/engine/risk")
@login_required
def risk_dashboard():
    from app.models.healthcore import HcRiskAssessment
    records = HcRiskAssessment.query.filter_by(user_id=current_user.id).order_by(HcRiskAssessment.calculated_date.desc(), HcRiskAssessment.created_at.desc()).all()
    return render_template("program_healthcore/risk.html", records=records)

@healthcore_bp.route("/program/healthcore/engine/risk/add", methods=["POST"])
@login_required
def add_risk():
    from app.models.healthcore import HcRiskAssessment
    from datetime import datetime

    calculated_date_str = request.form.get("calculated_date")
    algorithm_name = request.form.get("algorithm_name")
    
    score_percentage_raw = request.form.get("score_percentage")
    score_percentage = float(score_percentage_raw) if score_percentage_raw else 0.0
    
    risk_stratification = request.form.get("risk_stratification")
    driving_factors = request.form.get("driving_factors")
    
    try:
        calculated_date = datetime.strptime(calculated_date_str, "%Y-%m-%d").date() if calculated_date_str else datetime.utcnow().date()
    except ValueError:
        calculated_date = datetime.utcnow().date()

    if not algorithm_name:
        flash("Algorithm Name is required.", "danger")
        return redirect(url_for("healthcore_bp.risk_dashboard"))

    record = HcRiskAssessment(
        user_id=current_user.id,
        calculated_date=calculated_date,
        algorithm_name=algorithm_name,
        score_percentage=score_percentage,
        risk_stratification=risk_stratification,
        driving_factors=driving_factors
    )
    db.session.add(record)
    db.session.commit()
    flash("Risk assessment logged successfully.", "success")
    return redirect(url_for("healthcore_bp.risk_dashboard"))

@healthcore_bp.route("/program/healthcore/engine/correlation")
@login_required
def correlation_dashboard():
    from app.models.healthcore import HcCorrelationInsight
    records = HcCorrelationInsight.query.filter_by(user_id=current_user.id).order_by(HcCorrelationInsight.generated_date.desc(), HcCorrelationInsight.created_at.desc()).all()
    return render_template("program_healthcore/correlation.html", records=records)

@healthcore_bp.route("/program/healthcore/engine/correlation/add", methods=["POST"])
@login_required
def add_correlation():
    from app.models.healthcore import HcCorrelationInsight
    from datetime import datetime

    generated_date_str = request.form.get("generated_date")
    primary_factor = request.form.get("primary_factor")
    secondary_factor = request.form.get("secondary_factor")
    confidence = request.form.get("confidence", "Medium")
    insight_text = request.form.get("insight_text")
    
    try:
        generated_date = datetime.strptime(generated_date_str, "%Y-%m-%d").date() if generated_date_str else datetime.utcnow().date()
    except ValueError:
        generated_date = datetime.utcnow().date()

    if not insight_text:
        flash("Insight description is required.", "danger")
        return redirect(url_for("healthcore_bp.correlation_dashboard"))

    record = HcCorrelationInsight(
        user_id=current_user.id,
        generated_date=generated_date,
        primary_factor=primary_factor,
        secondary_factor=secondary_factor,
        confidence=confidence,
        insight_text=insight_text
    )
    db.session.add(record)
    db.session.commit()
    flash("Correlation insight logged successfully.", "success")
    return redirect(url_for("healthcore_bp.correlation_dashboard"))

@healthcore_bp.route("/program/healthcore/engine/reporting")
@login_required
def reporting_dashboard():
    from app.models.healthcore import HcGeneratedReport
    records = HcGeneratedReport.query.filter_by(user_id=current_user.id).order_by(HcGeneratedReport.generated_date.desc()).all()
    return render_template("program_healthcore/reporting.html", records=records)

@healthcore_bp.route("/program/healthcore/engine/reporting/generate", methods=["POST"])
@login_required
def generate_report():
    from app.models.healthcore import HcGeneratedReport, HcDocument
    from datetime import datetime

    report_type = request.form.get("report_type", "General Summary")
    audience = request.form.get("audience", "Doctor (Clinical)")
    
    # Mock generation of PDF and Document
    doc = HcDocument(
        user_id=current_user.id,
        file_type="PDF",
        doc_category="Generated Report",
        file_url="/static/healthcore_placeholder_report.pdf",
        extracted_text="MOCK AI EXTRACTED TEXT - HealthCore Summary."
    )
    db.session.add(doc)
    db.session.commit()

    record = HcGeneratedReport(
        user_id=current_user.id,
        generated_date=datetime.utcnow(),
        report_type=report_type,
        audience=audience,
        document_id=doc.id
    )
    db.session.add(record)
    db.session.commit()
    
    flash(f"{report_type} generated successfully.", "success")
    return redirect(url_for("healthcore_bp.reporting_dashboard"))
