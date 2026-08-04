from flask import Blueprint, render_template, redirect, url_for, flash, request, current_app
from functools import wraps
from flask_login import login_required, current_user
from app import db

healthcore_bp = Blueprint("healthcore_bp", __name__)

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
@healthcore_onboarded_required
def healthcore_dashboard():
    from app.models.healthcore import HcPatientProfile
    profile = HcPatientProfile.query.filter_by(user_id=current_user.id).first()
    return render_template("program_healthcore/dashboard.html", profile=profile)


# ---------------------------------------------------------
# ONBOARDING DECORATOR & ROUTES
# ---------------------------------------------------------
# ---------------------------------------------------------
@healthcore_bp.route("/program/healthcore/onboarding", methods=["GET", "POST"])
@login_required
def healthcore_onboarding():
    from app.models.healthcore import HcPatientProfile, HcConsentLog
    from app.program_healthcore.forms import HcOnboardingForm
    
    edit_mode = request.args.get('edit', '0') == '1'
    profile = HcPatientProfile.query.filter_by(user_id=current_user.id).first()
    
    if profile and not edit_mode:
        return redirect(url_for("healthcore_bp.healthcore_dashboard"))
        
    form = HcOnboardingForm()
    
    if request.method == "GET" and profile:
        form.dob.data = profile.dob
        form.biological_sex.data = profile.biological_sex
        form.blood_type.data = profile.blood_type
        form.height_cm.data = profile.height_cm
        form.weight_kg.data = profile.weight_kg
        form.chronic_conditions.data = profile.chronic_conditions
        form.consent_ai.data = True

    if form.validate_on_submit():
        if profile:
            profile.dob = form.dob.data
            profile.biological_sex = form.biological_sex.data
            profile.blood_type = form.blood_type.data
            profile.height_cm = form.height_cm.data
            profile.weight_kg = form.weight_kg.data
            profile.chronic_conditions = form.chronic_conditions.data
            flash("Your profile has been updated.", "success")
        else:
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
            flash("Welcome to Health IQ! Your profile is set up.", "success")
            
        db.session.commit()
        return redirect(url_for("healthcore_bp.healthcore_dashboard"))
        
    return render_template("program_healthcore/onboarding.html", form=form, edit_mode=edit_mode)

# ---------------------------------------------------------
# ENGINE DASHBOARDS
# ---------------------------------------------------------

@healthcore_bp.route("/program/healthcore/engine/laboratory", methods=["GET"])
@login_required
@healthcore_onboarded_required
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
@healthcore_onboarded_required
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
@healthcore_onboarded_required
def nutrition_dashboard():
    from app.models.healthcore import HcNutrition
    records = HcNutrition.query.filter_by(user_id=current_user.id).order_by(HcNutrition.log_date.desc(), HcNutrition.created_at.desc()).all()
    return render_template("program_healthcore/nutrition.html", records=records)

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
@healthcore_onboarded_required
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
@healthcore_onboarded_required
def lifestyle_dashboard():
    from app.models.healthcore import HcLifestyle
    records = HcLifestyle.query.filter_by(user_id=current_user.id).order_by(HcLifestyle.log_date.desc(), HcLifestyle.created_at.desc()).all()
    return render_template("program_healthcore/lifestyle.html", records=records)

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
@healthcore_onboarded_required
def timeline_dashboard():
    from app.models.healthcore import HcTimelineEvent
    records = HcTimelineEvent.query.filter_by(user_id=current_user.id).order_by(HcTimelineEvent.start_date.desc(), HcTimelineEvent.created_at.desc()).all()
    return render_template("program_healthcore/timeline.html", records=records)

@healthcore_bp.route("/program/healthcore/engine/timeline/add", methods=["POST"])
@login_required
@healthcore_onboarded_required
def add_timeline():
    from app.models.healthcore import HcTimelineEvent
    from datetime import datetime
    
    start_date_str = request.form.get("start_date")
    end_date_str = request.form.get("end_date")
    category = request.form.get("category")
    title = request.form.get("title")
    description = request.form.get("description")
    
    if not start_date_str or not category or not title:
        flash("Start Date, Category, and Title are required.", "danger")
        return redirect(url_for("healthcore_bp.timeline_dashboard"))
        
    try:
        sd = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    except ValueError:
        sd = datetime.utcnow().date()
        
    ed = None
    if end_date_str:
        try:
            ed = datetime.strptime(end_date_str, "%Y-%m-%d").date()
        except ValueError:
            pass
            
    record = HcTimelineEvent(
        user_id=current_user.id,
        start_date=sd,
        end_date=ed,
        category=category,
        title=title,
        description=description,
        source_engine="Manual"
    )
    db.session.add(record)
    db.session.commit()
    flash("Timeline event logged successfully!", "success")
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
@healthcore_onboarded_required
def risk_dashboard():
    from app.models.healthcore import HcRiskAssessment
    records = HcRiskAssessment.query.filter_by(user_id=current_user.id).order_by(HcRiskAssessment.calculated_date.desc(), HcRiskAssessment.created_at.desc()).all()
    return render_template("program_healthcore/risk.html", records=records)

@healthcore_bp.route("/program/healthcore/engine/risk/generate", methods=["POST"])
@login_required
@healthcore_onboarded_required
def generate_risk():
    from app.program_healthcore.ai_extractor import generate_risk_assessment
    result = generate_risk_assessment(current_user.id)
    if "error" in result:
        flash(f"Error generating Risk Assessment: {result['error']}", "danger")
    else:
        flash("AI Risk Assessment generated successfully!", "success")
    return redirect(url_for("healthcore_bp.risk_dashboard"))


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

@healthcore_bp.route("/program/healthcore/engine/correlation", methods=["GET"])
@login_required
@healthcore_onboarded_required
def correlation_dashboard():
    from app.models.healthcore import HcCorrelationInsight
    records = HcCorrelationInsight.query.filter_by(user_id=current_user.id).order_by(HcCorrelationInsight.generated_date.desc(), HcCorrelationInsight.created_at.desc()).all()
    return render_template("program_healthcore/correlation.html", records=records)

@healthcore_bp.route("/program/healthcore/engine/correlation/generate", methods=["POST"])
@login_required
@healthcore_onboarded_required
def generate_correlation():
    from app.program_healthcore.ai_extractor import generate_correlation_insight
    result = generate_correlation_insight(current_user.id)
    if "error" in result:
        flash(f"Error generating Correlation Insight: {result['error']}", "danger")
    else:
        flash("AI Correlation Insight generated successfully!", "success")
    return redirect(url_for("healthcore_bp.correlation_dashboard"))

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
    from app.program_healthcore.ai_extractor import generate_report as ai_generate_report

    report_type = request.form.get("report_type", "General Summary")
    audience = request.form.get("audience", "Doctor (Clinical)")
    
    result = ai_generate_report(current_user.id, report_type, audience)
    
    if "error" in result:
        flash(f"Error generating Report: {result['error']}", "danger")
    else:
        flash(f"{report_type} generated successfully.", "success")
    return redirect(url_for("healthcore_bp.reporting_dashboard"))
# ---------------------------------------------------------
# DOCUMENT INTELLIGENCE LAYER
# ---------------------------------------------------------

@healthcore_bp.route("/program/healthcore/documents")
@login_required
def document_dashboard():
    from app.models.healthcore import HcDocument
    documents = HcDocument.query.filter_by(user_id=current_user.id).order_by(HcDocument.upload_date.desc()).all()
    return render_template("program_healthcore/document_upload.html", documents=documents)

@healthcore_bp.route("/program/healthcore/documents/upload", methods=["POST"])
@login_required
def document_upload():
    import os
    from werkzeug.utils import secure_filename
    from app.models.healthcore import HcDocument
    
    files = request.files.getlist("documents")
    if not files or not files[0].filename:
        flash("No files selected.", "danger")
        return redirect(url_for("healthcore_bp.document_dashboard"))
        
    upload_folder = os.path.join(current_app.root_path, "static", "uploads", "healthcore")
    os.makedirs(upload_folder, exist_ok=True)
    
    for file in files:
        if file.filename:
            filename = secure_filename(file.filename)
            filepath = os.path.join(upload_folder, filename)
            file.save(filepath)
            
            doc = HcDocument(
                user_id=current_user.id,
                file_type=filename.split('.')[-1].lower(),
                doc_category='Unknown',
                file_url=f"uploads/healthcore/{filename}",
                status="uploaded"
            )
            db.session.add(doc)
            
    db.session.commit()
    flash(f"Successfully uploaded {len(files)} documents.", "success")
    return redirect(url_for("healthcore_bp.document_dashboard"))

@healthcore_bp.route("/program/healthcore/documents/delete/<int:doc_id>", methods=["POST"])
@login_required
def document_delete(doc_id):
    from app.models.healthcore import HcDocument, HcGeneratedReport, HcLaboratory, HcImaging
    import os
    
    doc = HcDocument.query.filter_by(id=doc_id, user_id=current_user.id).first_or_404()
    
    # Manually delete dependent records to prevent ForeignKeyViolation
    HcGeneratedReport.query.filter_by(document_id=doc.id).delete()
    HcLaboratory.query.filter_by(document_id=doc.id).delete()
    HcImaging.query.filter_by(document_id=doc.id).delete()
    
    # Optionally remove the file from disk
    if doc.file_url:
        filepath = os.path.join(current_app.root_path, "static", doc.file_url)
        if os.path.exists(filepath):
            try:
                os.remove(filepath)
            except Exception:
                pass
                
    db.session.delete(doc)
    db.session.commit()
    
    flash("Document deleted successfully.", "success")
    return redirect(url_for("healthcore_bp.document_dashboard"))

@healthcore_bp.route("/program/healthcore/documents/analyze", methods=["POST"])
@login_required
def document_analyze():
    import threading
    from app.program_healthcore.document_processor import process_documents_async
    from flask import current_app
    
    # Run processor in background
    app = current_app._get_current_object()
    thread = threading.Thread(target=process_documents_async, args=(app, current_user.id))
    thread.start()
    
    return {"status": "started"}

@healthcore_bp.route("/program/healthcore/documents/status", methods=["GET"])
@login_required
def document_status():
    from app.models.healthcore import HcDocument
    docs = HcDocument.query.filter_by(user_id=current_user.id).all()
    status_dict = {
        doc.id: {
            "status": doc.status,
            "url": url_for('healthcore_bp.document_review', doc_id=doc.id) if doc.status == 'review_ready' else None
        } for doc in docs
    }
    return {"documents": status_dict}

@healthcore_bp.route("/program/healthcore/documents/review/<int:doc_id>", methods=["GET", "POST"])
@login_required
def document_review(doc_id):
    from app.models.healthcore import HcDocument, HcDocumentExtraction
    import json
    
    doc = HcDocument.query.filter_by(id=doc_id, user_id=current_user.id).first_or_404()
    extraction = HcDocumentExtraction.query.filter_by(document_id=doc.id).first_or_404()
    
    if request.method == "POST":
        # Handle manual edits
        edited_json = request.form.get("extracted_json")
        try:
            # Validate JSON
            parsed_json = json.loads(edited_json)
            extraction.extracted_json = edited_json
            extraction.reviewed = True
            db.session.commit()
            
            # Map to Permanent Engine Table!
            from app.models.healthcore import HcLaboratory, HcMedication, HcImaging
            from datetime import datetime
            
            doc_type = parsed_json.get("document_type", "").lower()
            
            if doc_type == "laboratory":
                tests = parsed_json.get("tests", [])
                report_date_str = parsed_json.get("report_date")
                r_date = datetime.strptime(report_date_str, "%Y-%m-%d").date() if report_date_str else datetime.utcnow().date()
                
                for test in tests:
                    lab = HcLaboratory(
                        user_id=current_user.id,
                        report_date=r_date,
                        test_name=test.get("name", "Unknown"),
                        value=float(test.get("value", 0)),
                        units=test.get("unit", ""),
                        reference_range=test.get("reference_range", ""),
                        status=test.get("status", ""),
                        document_id=doc.id
                    )
                    db.session.add(lab)
            
            elif doc_type == "medication":
                meds = parsed_json.get("medications", [])
                report_date_str = parsed_json.get("report_date")
                r_date = datetime.strptime(report_date_str, "%Y-%m-%d").date() if report_date_str else None
                
                for med in meds:
                    m = HcMedication(
                        user_id=current_user.id,
                        medication_name=med.get("name", "Unknown"),
                        date_prescribed=r_date,
                        dosage=med.get("dose", ""),
                        frequency=med.get("frequency", ""),
                        status=med.get("status", "Active")
                    )
                    db.session.add(m)
                    
            elif doc_type == "imaging":
                report_date_str = parsed_json.get("report_date")
                r_date = datetime.strptime(report_date_str, "%Y-%m-%d").date() if report_date_str else datetime.utcnow().date()
                
                img = HcImaging(
                    user_id=current_user.id,
                    scan_date=r_date,
                    modality=parsed_json.get("modality", ""),
                    body_part=parsed_json.get("body_part", ""),
                    findings=parsed_json.get("findings", ""),
                    impression=parsed_json.get("impression", ""),
                    document_id=doc.id
                )
                db.session.add(img)

            doc.status = "completed"
            db.session.commit()
            
            flash("Document data successfully verified and saved to Health IQ!", "success")
            return redirect(url_for("healthcore_bp.document_dashboard"))
            
        except json.JSONDecodeError:
            flash("Invalid JSON format submitted.", "danger")
        except Exception as e:
            db.session.rollback()
            flash(f"Error saving to permanent record: {str(e)}", "danger")
            
    parsed_data = json.loads(extraction.extracted_json) if extraction.extracted_json else {}
    return render_template("program_healthcore/document_review.html", doc=doc, extraction=extraction, parsed_data=parsed_data)

# ---------------------------------------------------------
# DOCTOR ACCESS
# ---------------------------------------------------------
@healthcore_bp.route("/program/healthcore/share", methods=["GET"])
@login_required
@healthcore_onboarded_required
def share_dashboard():
    from app.models.healthcore import HcDoctorAccess
    shares = HcDoctorAccess.query.filter_by(user_id=current_user.id).order_by(HcDoctorAccess.created_at.desc()).all()
    return render_template("program_healthcore/share.html", shares=shares)

@healthcore_bp.route("/program/healthcore/share/add", methods=["POST"])
@login_required
@healthcore_onboarded_required
def create_share():
    from app.models.healthcore import HcDoctorAccess
    from datetime import datetime, timedelta
    import secrets
    
    doctor_email = request.form.get("doctor_email")
    doctor_name = request.form.get("doctor_name")
    days = request.form.get("days", type=int) or 7
    
    if not doctor_email:
        flash("Doctor's email is required.", "danger")
        return redirect(url_for("healthcore_bp.share_dashboard"))
        
    access_token = secrets.token_urlsafe(32)
    expires_at = datetime.utcnow() + timedelta(days=days)
    
    share = HcDoctorAccess(
        user_id=current_user.id,
        doctor_email=doctor_email,
        doctor_name=doctor_name,
        access_token=access_token,
        expires_at=expires_at
    )
    db.session.add(share)
    db.session.commit()
    
    # Send email invitation to the doctor
    from app import send_mail
    invite_url = url_for("healthcore_bp.doctor_view", token=access_token, _external=True)
    email_html = f"""
    <div style="font-family: sans-serif; padding: 20px; color: #333;">
        <h2>Health IQ Access Invitation</h2>
        <p>Dear {doctor_name or 'Doctor'},</p>
        <p>Your patient <strong>{current_user.name or 'A patient'}</strong> has shared their Health IQ medical profile with you.</p>
        <p>Health IQ is a next-generation clinical intelligence platform. By joining, you can review your patient's AI-summarized health records, laboratory trends, and AI-generated correlation insights to provide better care.</p>
        <p>To view their records securely, please create an account on the AIT Platform by clicking the link below:</p>
        <p>
            <a href="{invite_url}" style="background-color: #4F46E5; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px; display: inline-block;">
                Register & View Patient Data
            </a>
        </p>
        <p>This secure access token will expire on <strong>{expires_at.strftime('%B %d, %Y')}</strong>.</p>
        <br>
        <p>Best regards,<br>The Health IQ Team</p>
    </div>
    """
    try:
        send_mail(doctor_email, "Invitation to view patient data on Health IQ", email_html)
        flash("Secure sharing link generated and invitation email sent to the doctor!", "success")
    except Exception as e:
        flash("Secure sharing link generated, but there was an error sending the invitation email.", "warning")
        
    return redirect(url_for("healthcore_bp.share_dashboard"))

@healthcore_bp.route("/program/healthcore/share/revoke/<int:share_id>", methods=["POST"])
@login_required
def revoke_share(share_id):
    from app.models.healthcore import HcDoctorAccess
    share = HcDoctorAccess.query.filter_by(id=share_id, user_id=current_user.id).first_or_404()
    share.is_active = False
    db.session.commit()
    flash("Access revoked.", "info")
    return redirect(url_for("healthcore_bp.share_dashboard"))

@healthcore_bp.route("/healthcore/doctor/view/<token>")
@login_required
def doctor_view(token):
    from app.models.healthcore import HcDoctorAccess, HcPatientProfile, HcRiskAssessment, HcLaboratory
    from datetime import datetime
    from app.models.auth import User
    
    share = HcDoctorAccess.query.filter_by(access_token=token, is_active=True).first_or_404()
    
    if datetime.utcnow() > share.expires_at:
        return "This secure link has expired.", 403
        
    patient = User.query.get(share.user_id)
    profile = HcPatientProfile.query.filter_by(user_id=share.user_id).first()
    risks = HcRiskAssessment.query.filter_by(user_id=share.user_id).order_by(HcRiskAssessment.calculated_date.desc()).limit(5).all()
    labs = HcLaboratory.query.filter_by(user_id=share.user_id).order_by(HcLaboratory.report_date.desc()).limit(15).all()
    
    return render_template("program_healthcore/doctor_view.html", share=share, patient=patient, profile=profile, risks=risks, labs=labs)
