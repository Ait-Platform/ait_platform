from app.extensions import db
from datetime import datetime

# --- PHASE 4 MODELS ---

class CoreRemunerationRule(db.Model):
    __tablename__ = "core_remuneration_rule"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=False)
    
    interaction_type = db.Column(db.String(50), nullable=False)
    rate_cents = db.Column(db.Integer, nullable=False, default=0)
    
    # The role eligible for this remuneration (e.g., Receptionist)
    role_id = db.Column(db.Integer, db.ForeignKey("core_role.id"), nullable=True)
    requires_approval = db.Column(db.Boolean, default=True)

class CoreRemunerationEvent(db.Model):
    __tablename__ = "core_remuneration_event"
    id = db.Column(db.Integer, primary_key=True)
    interaction_id = db.Column(db.Integer, db.ForeignKey("core_interaction.id"), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False) # The person who earned it
    rule_id = db.Column(db.Integer, db.ForeignKey("core_remuneration_rule.id"), nullable=True)
    
    amount_cents = db.Column(db.Integer, nullable=False)
    status = db.Column(db.String(50), default="pending_approval", index=True) # pending_approval, approved, paid
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    approved_at = db.Column(db.DateTime)
    
    # Relationships
    interaction = db.relationship("CoreInteraction", backref="remuneration_events")
    user = db.relationship("User", backref="remuneration_events")
