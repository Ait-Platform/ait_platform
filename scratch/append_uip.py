import re

with open('app/models/uip.py', 'r', encoding='utf-8') as f:
    text = f.read()

docs = """
# --- DOCUMENTS & COMMUNICATIONS (Steps 16 & 17) ---

class UipDocument(db.Model):
    __tablename__ = "uip_document"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=False)
    uploader_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    
    filename = db.Column(db.String(255), nullable=False)
    file_type = db.Column(db.String(50)) # e.g. PDF, IMG
    description = db.Column(db.String(255))
    
    access_classification = db.Column(db.String(50), default="PRIVATE") # PUBLIC, PRIVATE, COMMITTEE_ONLY
    
    # Optional links
    interaction_id = db.Column(db.Integer, db.ForeignKey("core_interaction.id"), nullable=True)
    meeting_id = db.Column(db.Integer, db.ForeignKey("uip_committee_meeting.id"), nullable=True)
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class UipBroadcast(db.Model):
    __tablename__ = "uip_broadcast"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=False)
    sender_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    
    subject = db.Column(db.String(255))
    body_text = db.Column(db.Text)
    channel = db.Column(db.String(50)) # EMAIL, SMS, WHATSAPP
    
    target_audience = db.Column(db.String(50)) # ALL, RESIDENTS, COMMITTEE
    status = db.Column(db.String(50), default="DRAFT") # DRAFT, SCHEDULED, SENT
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    sent_at = db.Column(db.DateTime, nullable=True)
"""

text = text + docs

with open('app/models/uip.py', 'w', encoding='utf-8') as f:
    f.write(text)
