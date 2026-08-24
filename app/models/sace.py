from app.extensions import db
from datetime import datetime

class SaceDocument(db.Model):
    __tablename__ = 'sace_documents'

    id = db.Column(db.Integer, primary_key=True)
    slug = db.Column(db.String(50), nullable=False) # e.g., 'reading', 'cultural_fire'
    document_type = db.Column(db.String(50), nullable=False) # e.g., 'application_form', 'annexure_a', etc.
    file_name = db.Column(db.String(255), nullable=False) # Original file name
    file_path = db.Column(db.String(255), nullable=False) # Path where stored
    
    uploaded_at = db.Column(db.DateTime, default=datetime.utcnow)
    uploaded_by = db.Column(db.Integer, db.ForeignKey('user.id', ondelete='SET NULL'), nullable=True)

class SaceWorkshopInteraction(db.Model):
    __tablename__ = 'sace_workshop_interactions'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id', ondelete='CASCADE'), nullable=False)
    workshop_session_id = db.Column(db.String(50), nullable=True, default='demo-session-1')
    activity_slug = db.Column(db.String(50), nullable=False)
    response_data = db.Column(db.Text, nullable=False)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)

