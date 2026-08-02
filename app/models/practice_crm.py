from app.extensions import db
from datetime import datetime

class CrmPractice(db.Model):
    __tablename__ = "crm_practice"
    id = db.Column(db.Integer, primary_key=True)
    owner_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    name = db.Column(db.String(100), nullable=False)
    phone = db.Column(db.String(50), nullable=True)
    email = db.Column(db.String(120), nullable=True)
    address = db.Column(db.String(255), nullable=True)
    practice_type = db.Column(db.String(50), nullable=True)
    dentist_details = db.Column(db.Text, nullable=True)
    operating_hours = db.Column(db.Text, nullable=True)
    slot_settings = db.Column(db.Text, nullable=True)
    wallet_balance_cents = db.Column(db.Integer, default=0, nullable=False)
    trial_ends_at = db.Column(db.DateTime, nullable=True)
    shadow_spent_cents = db.Column(db.Integer, default=0, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class CrmPracticeUser(db.Model):
    __tablename__ = "crm_practice_user"
    id = db.Column(db.Integer, primary_key=True)
    practice_id = db.Column(db.Integer, db.ForeignKey('crm_practice.id'), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    role = db.Column(db.String(50), default='receptionist') # 'owner' or 'receptionist'
    phone = db.Column(db.String(50), nullable=True)
    status = db.Column(db.String(20), default='active') # 'active' or 'suspended'
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class CrmEnquiry(db.Model):
    __tablename__ = "crm_enquiry"
    id = db.Column(db.Integer, primary_key=True)
    practice_id = db.Column(db.Integer, db.ForeignKey('crm_practice.id'), nullable=False)
    
    # Step 1: Initial capture
    patient_name = db.Column(db.String(150), nullable=False)
    patient_id_no = db.Column(db.String(20), nullable=True)
    phone = db.Column(db.String(50), nullable=False)
    medical_aid = db.Column(db.String(150), nullable=True)
    medical_aid_plan = db.Column(db.String(150), nullable=True)
    medical_aid_no = db.Column(db.String(100), nullable=True)
    reason = db.Column(db.Text, nullable=True)
    status = db.Column(db.String(50), default='New') # New, Verification Pending, Verified, Appointment Offered, Booked, Not Booked'
    
    # Step 2: Verification
    verification_date = db.Column(db.DateTime, nullable=True)
    consultant_name = db.Column(db.String(100), nullable=True)
    funds_available = db.Column(db.Boolean, nullable=True)
    reference_no = db.Column(db.String(100), nullable=True)
    
    # Step 3 & 4: Appointment / Outcome
    appointment_time = db.Column(db.DateTime, nullable=True)
    not_booked_reason = db.Column(db.String(255), nullable=True)
    
    # Meta
    created_by_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class CrmAuditLog(db.Model):
    __tablename__ = "crm_audit_log"
    id = db.Column(db.Integer, primary_key=True)
    enquiry_id = db.Column(db.Integer, db.ForeignKey('crm_enquiry.id'), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    action = db.Column(db.String(255), nullable=False) 
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)
