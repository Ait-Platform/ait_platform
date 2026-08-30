from app.extensions import db
from datetime import datetime

# --- PROVIDERS (Step 13) ---

class UipProvider(db.Model):
    __tablename__ = "uip_provider"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=False)
    name = db.Column(db.String(255), nullable=False)
    service_type = db.Column(db.String(100)) # e.g. Security, Maintenance
    contact_email = db.Column(db.String(255))
    contact_phone = db.Column(db.String(50))
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class UipWorkOrder(db.Model):
    __tablename__ = "uip_work_order"
    id = db.Column(db.Integer, primary_key=True)
    interaction_id = db.Column(db.Integer, db.ForeignKey("core_interaction.id"), nullable=False)
    provider_id = db.Column(db.Integer, db.ForeignKey("uip_provider.id"), nullable=False)
    
    reference = db.Column(db.String(50), unique=True)
    description = db.Column(db.Text)
    status = db.Column(db.String(50), default="SENT") # SENT, ACCEPTED, IN_PROGRESS, COMPLETED, VERIFIED
    
    cost_cents = db.Column(db.Integer, nullable=True) # Linked to Ledger later
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    completed_at = db.Column(db.DateTime, nullable=True)
    verified_at = db.Column(db.DateTime, nullable=True)

    interaction = db.relationship("CoreInteraction", backref="work_orders")
    provider = db.relationship("UipProvider", backref="work_orders")


# --- MUNICIPALITY (Step 14) ---

class UipMunicipalReferral(db.Model):
    __tablename__ = "uip_municipal_referral"
    id = db.Column(db.Integer, primary_key=True)
    interaction_id = db.Column(db.Integer, db.ForeignKey("core_interaction.id"), nullable=False)
    
    department = db.Column(db.String(100)) # e.g., Water & Sanitation, Parks
    municipality_reference = db.Column(db.String(100)) # The reference number given by the city
    
    status = db.Column(db.String(50), default="ESCALATED") # ESCALATED, IN_PROGRESS, RESOLVED_BY_CITY
    sla_expected_date = db.Column(db.DateTime, nullable=True)
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    resolved_at = db.Column(db.DateTime, nullable=True)
    
    interaction = db.relationship("CoreInteraction", backref="municipal_referrals")


# --- GOVERNANCE (Step 15) ---

class UipCommitteeMeeting(db.Model):
    __tablename__ = "uip_committee_meeting"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=False)
    
    title = db.Column(db.String(255), nullable=False)
    meeting_type = db.Column(db.String(50)) # AGM, Monthly, Special
    scheduled_at = db.Column(db.DateTime, nullable=False)
    location = db.Column(db.String(255))
    
    status = db.Column(db.String(50), default="SCHEDULED") # SCHEDULED, IN_PROGRESS, CONCLUDED
    minutes_text = db.Column(db.Text)
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class UipResolution(db.Model):
    __tablename__ = "uip_resolution"
    id = db.Column(db.Integer, primary_key=True)
    meeting_id = db.Column(db.Integer, db.ForeignKey("uip_committee_meeting.id"), nullable=False)
    
    title = db.Column(db.String(255), nullable=False)
    description = db.Column(db.Text)
    status = db.Column(db.String(50), default="PROPOSED") # PROPOSED, APPROVED, REJECTED, EXECUTED
    
    # "An Approved Resolution automatically spawns a Task assigned to an operational user"
    linked_task_id = db.Column(db.Integer, db.ForeignKey("core_task.id"), nullable=True)
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    meeting = db.relationship("UipCommitteeMeeting", backref="resolutions")
    task = db.relationship("CoreTask", backref="governance_resolution")

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
