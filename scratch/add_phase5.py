from app.extensions import db
from datetime import datetime

# --- PHASE 5 MODELS ---

class CoreAuditEvent(db.Model):
    __tablename__ = "core_audit_event"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=True) # The actor
    
    action = db.Column(db.String(100), nullable=False, index=True) # e.g. INTERACTION_CREATED
    entity_type = db.Column(db.String(100)) # e.g. CoreInteraction
    entity_id = db.Column(db.Integer)
    
    # Store JSON or text details of what changed
    details = db.Column(db.Text)
    
    ip_address = db.Column(db.String(50))
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)

    # Relationships
    user = db.relationship("User", backref="audit_events")
