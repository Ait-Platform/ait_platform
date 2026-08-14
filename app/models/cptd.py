from app.extensions import db
from datetime import datetime

class CptdRegistration(db.Model):
    __tablename__ = 'cptd_registrations'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id', ondelete='CASCADE'), nullable=False)
    programme = db.Column(db.String(50), nullable=False) # e.g., 'reading', 'cultural_fire'
    workshop_date = db.Column(db.Date, nullable=True)
    status = db.Column(db.String(20), default='registered') # 'registered', 'in_progress', 'completed'
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    user = db.relationship("User", backref=db.backref("cptd_registrations", lazy="dynamic", cascade="all, delete-orphan"))

class CptdProgress(db.Model):
    __tablename__ = 'cptd_progress'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id', ondelete='CASCADE'), nullable=False)
    programme = db.Column(db.String(50), nullable=False) # e.g., 'reading'
    module_id = db.Column(db.String(50), nullable=False) # e.g., '1', '2', 'intro', 'evaluation'
    
    status = db.Column(db.String(20), default='locked') # 'locked', 'unlocked', 'completed'
    completed_at = db.Column(db.DateTime, nullable=True)
    
    # Text or JSON data to store reflection text, assessment answers, or simple boolean flags
    evidence_data = db.Column(db.Text, nullable=True)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    user = db.relationship("User", backref=db.backref("cptd_progress", lazy="dynamic", cascade="all, delete-orphan"))

class CptdEvaluation(db.Model):
    __tablename__ = 'cptd_evaluations'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id', ondelete='CASCADE'), nullable=False)
    programme = db.Column(db.String(50), nullable=False)
    
    rating_programme = db.Column(db.Integer, nullable=True)
    rating_facilitator = db.Column(db.Integer, nullable=True)
    rating_platform = db.Column(db.Integer, nullable=True)
    
    feedback_text = db.Column(db.Text, nullable=True)
    
    submitted_at = db.Column(db.DateTime, default=datetime.utcnow)

    user = db.relationship("User", backref=db.backref("cptd_evaluations", lazy="dynamic", cascade="all, delete-orphan"))
