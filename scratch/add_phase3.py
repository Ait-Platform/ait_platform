from app.extensions import db
from datetime import datetime

# --- PHASE 3 MODELS ---

class CoreInteraction(db.Model):
    __tablename__ = "core_interaction"
    id = db.Column(db.Integer, primary_key=True)
    reference = db.Column(db.String(50), unique=True, index=True) # e.g. "MG-00482"
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=False)
    creator_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False) # e.g., the Resident
    
    interaction_type = db.Column(db.String(50), nullable=False) # enquiry, complaint, request, etc.
    title = db.Column(db.String(255), nullable=False)
    description = db.Column(db.Text)
    
    status = db.Column(db.String(50), default="open", index=True)
    priority = db.Column(db.String(50), default="normal")
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    organization = db.relationship("CoreOrganization", backref="interactions")
    tasks = db.relationship("CoreTask", back_populates="interaction", cascade="all, delete-orphan")

class CoreTask(db.Model):
    __tablename__ = "core_task"
    id = db.Column(db.Integer, primary_key=True)
    interaction_id = db.Column(db.Integer, db.ForeignKey("core_interaction.id"), nullable=False)
    assignee_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=True) # e.g., the Receptionist
    
    title = db.Column(db.String(255))
    description = db.Column(db.Text)
    status = db.Column(db.String(50), default="pending", index=True)
    
    due_date = db.Column(db.DateTime)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    completed_at = db.Column(db.DateTime)

    # Relationships
    interaction = db.relationship("CoreInteraction", back_populates="tasks")
