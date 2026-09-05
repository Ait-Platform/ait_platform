from app.extensions import db
from datetime import datetime

# --- PHASE 6 & 7 MODELS ---

class CoreAiRequest(db.Model):
    __tablename__ = "core_ai_request"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=True)
    interaction_id = db.Column(db.Integer, db.ForeignKey("core_interaction.id"), nullable=True)
    
    # Model Router: 'luna' (simple), 'terra' (complex), 'sol' (exceptional)
    model_requested = db.Column(db.String(50), default="luna")
    provider_used = db.Column(db.String(50)) # e.g., 'gemini', 'openai'
    
    prompt_text = db.Column(db.Text)
    response_text = db.Column(db.Text)
    
    status = db.Column(db.String(50), default="pending") # pending, completed, failed
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    completed_at = db.Column(db.DateTime)

class CoreAiUsage(db.Model):
    __tablename__ = "core_ai_usage"
    id = db.Column(db.Integer, primary_key=True)
    request_id = db.Column(db.Integer, db.ForeignKey("core_ai_request.id"), nullable=False)
    
    tokens_in = db.Column(db.Integer, default=0)
    tokens_out = db.Column(db.Integer, default=0)
    cost_cents = db.Column(db.Integer, default=0)
    
    # Link to the legacy AitTokenTransaction when we debit the user's wallet
    ledger_transaction_id = db.Column(db.Integer, db.ForeignKey("ait_token_transaction.id"), nullable=True)
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    # Relationships
    request = db.relationship("CoreAiRequest", backref="usage_metrics")
