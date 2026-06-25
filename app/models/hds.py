from datetime import datetime, timezone
from app.extensions import db

class HdsOrganization(db.Model):
    __tablename__ = 'hds_organization'
    
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    name = db.Column(db.String(255), nullable=False)
    registration_number = db.Column(db.String(100), nullable=True)
    contact_email = db.Column(db.String(255), nullable=True)
    contact_phone = db.Column(db.String(100), nullable=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

class HdsClaim(db.Model):
    __tablename__ = 'hds_claim'
    
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, db.ForeignKey('hds_organization.id'), nullable=False)
    patient_name = db.Column(db.String(255), nullable=False)
    patient_id_no = db.Column(db.String(50), nullable=True)
    medical_aid = db.Column(db.String(255), nullable=True)
    medical_aid_no = db.Column(db.String(100), nullable=True)
    treatment_date = db.Column(db.Date, nullable=True)
    icd10_code = db.Column(db.String(50), nullable=True)
    tariff_code = db.Column(db.String(50), nullable=True)
    amount_claimed = db.Column(db.Numeric(10, 2), nullable=True)
    status = db.Column(db.String(50), default='Submitted')
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    organization = db.relationship('HdsOrganization', backref=db.backref('claims', lazy=True))
