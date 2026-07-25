from app.extensions import db
from datetime import datetime

# ---------------------------------------------------------
# ENGINE 1: LABORATORY (LAB)
# ---------------------------------------------------------
class HcLaboratory(db.Model):
    __tablename__ = 'hc_laboratory'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    report_date = db.Column(db.Date, nullable=False)
    test_name = db.Column(db.String(100), nullable=False)
    value = db.Column(db.Float, nullable=False)
    units = db.Column(db.String(50))
    reference_range = db.Column(db.String(100))
    status = db.Column(db.String(50))  # e.g., 'Normal', 'High', 'Low'
    document_id = db.Column(db.Integer, db.ForeignKey('hc_document.id'), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

# ---------------------------------------------------------
# ENGINE 2: MEDICATION (MED)
# ---------------------------------------------------------
class HcMedication(db.Model):
    __tablename__ = 'hc_medication'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    date_prescribed = db.Column(db.Date, nullable=True)
    medication_name = db.Column(db.String(150), nullable=False)
    dosage = db.Column(db.String(100))
    frequency = db.Column(db.String(100))
    status = db.Column(db.String(50), default='Active')  # 'Active', 'Discontinued'
    adherence_score = db.Column(db.Float, default=100.0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

# ---------------------------------------------------------
# ENGINE 3: NUTRITION (NUT)
# ---------------------------------------------------------
class HcNutrition(db.Model):
    __tablename__ = 'hc_nutrition'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    log_date = db.Column(db.Date, nullable=False)
    entry_type = db.Column(db.String(50))  # 'Meal', 'Snack', 'Hydration'
    description = db.Column(db.String(255))
    calories_kcal = db.Column(db.Float, default=0)
    protein_g = db.Column(db.Float, default=0)
    carbs_g = db.Column(db.Float, default=0)
    fats_g = db.Column(db.Float, default=0)
    water_ml = db.Column(db.Float, default=0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

# ---------------------------------------------------------
# ENGINE 4: IMAGING (IMG)
# ---------------------------------------------------------
class HcImaging(db.Model):
    __tablename__ = 'hc_imaging'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    scan_date = db.Column(db.Date, nullable=False)
    modality = db.Column(db.String(100))  # e.g., 'MRI', 'X-Ray'
    body_part = db.Column(db.String(100))
    findings = db.Column(db.Text)
    impression = db.Column(db.Text)
    document_id = db.Column(db.Integer, db.ForeignKey('hc_document.id'), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

# ---------------------------------------------------------
# ENGINE 5: LIFESTYLE (LIFE)
# ---------------------------------------------------------
class HcLifestyle(db.Model):
    __tablename__ = 'hc_lifestyle'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    log_date = db.Column(db.Date, nullable=False)
    category = db.Column(db.String(50))  # 'Exercise', 'Sleep', 'Vital', 'Habit'
    metric_name = db.Column(db.String(100))  # e.g., 'Blood Pressure', 'Sleep Duration'
    value_str = db.Column(db.String(100))  # Used for things like '120/80'
    value_num = db.Column(db.Float, nullable=True)
    units = db.Column(db.String(50))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

# ---------------------------------------------------------
# ENGINE 6: TIMELINE (TIME)
# ---------------------------------------------------------
class HcTimelineEvent(db.Model):
    __tablename__ = 'hc_timeline_event'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    start_date = db.Column(db.Date, nullable=False)
    end_date = db.Column(db.Date, nullable=True)
    category = db.Column(db.String(100))  # 'Surgery', 'Diagnosis'
    title = db.Column(db.String(255), nullable=False)
    description = db.Column(db.Text)
    source_engine = db.Column(db.String(50), default='Manual')
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

# ---------------------------------------------------------
# ENGINE 7: RISK (RISK)
# ---------------------------------------------------------
class HcRiskAssessment(db.Model):
    __tablename__ = 'hc_risk_assessment'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    calculated_date = db.Column(db.Date, nullable=False)
    algorithm_name = db.Column(db.String(150))
    score_percentage = db.Column(db.Float)
    risk_stratification = db.Column(db.String(50))  # 'Low', 'Moderate', 'High'
    driving_factors = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

# ---------------------------------------------------------
# ENGINE 8: CORRELATION (CORR)
# ---------------------------------------------------------
class HcCorrelationInsight(db.Model):
    __tablename__ = 'hc_correlation_insight'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    generated_date = db.Column(db.Date, nullable=False)
    primary_factor = db.Column(db.String(255))
    secondary_factor = db.Column(db.String(255))
    insight_text = db.Column(db.Text, nullable=False)
    confidence = db.Column(db.String(50))  # 'High', 'Medium', 'Low'
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

# ---------------------------------------------------------
# ENGINE 9: REPORTING (REP) & SHARED DOCS
# ---------------------------------------------------------
class HcDocument(db.Model):
    __tablename__ = 'hc_document'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    upload_date = db.Column(db.DateTime, default=datetime.utcnow)
    file_type = db.Column(db.String(50))  # 'PDF', 'Image'
    doc_category = db.Column(db.String(100))  # 'Lab Report', 'Generated Report'
    file_url = db.Column(db.String(255), nullable=False)
    extracted_text = db.Column(db.Text)
    status = db.Column(db.String(50), default='uploaded')  # uploaded, processing, review_ready, completed, error

class HcDocumentExtraction(db.Model):
    __tablename__ = 'hc_document_extraction'
    id = db.Column(db.Integer, primary_key=True)
    document_id = db.Column(db.Integer, db.ForeignKey('hc_document.id'), nullable=False)
    extracted_json = db.Column(db.Text)  # JSON string
    document_type = db.Column(db.String(50))  # e.g., 'laboratory', 'medication'
    reviewed = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    document = db.relationship('HcDocument', backref=db.backref('extractions', lazy=True, cascade='all, delete-orphan'))

class HcGeneratedReport(db.Model):
    __tablename__ = 'hc_generated_report'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    generated_date = db.Column(db.DateTime, default=datetime.utcnow)
    report_type = db.Column(db.String(100))
    audience = db.Column(db.String(100))
    document_id = db.Column(db.Integer, db.ForeignKey('hc_document.id'), nullable=False)
