from app.extensions import db
from datetime import datetime

class CoreOrganizationWallet(db.Model):
    __tablename__ = "core_organization_wallet"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, db.ForeignKey('core_organization.id'), nullable=False, unique=True)
    balance = db.Column(db.Integer, default=1000, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    transactions = db.relationship("CoreOrganizationLedger", back_populates="wallet", cascade="all, delete-orphan")

class CoreOrganizationLedger(db.Model):
    __tablename__ = "core_organization_ledger"
    id = db.Column(db.Integer, primary_key=True)
    wallet_id = db.Column(db.Integer, db.ForeignKey('core_organization_wallet.id'), nullable=False)
    amount = db.Column(db.Integer, nullable=False)
    description = db.Column(db.String(255))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    wallet = db.relationship("CoreOrganizationWallet", back_populates="transactions")

class CoreOrganization(db.Model):
    __tablename__ = "core_organization"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    slug = db.Column(db.String(255), unique=True, nullable=False, index=True)
    
    # Extended Functional Specification Fields
    area = db.Column(db.String(255))
    municipality_ref = db.Column(db.String(255))
    contact_email = db.Column(db.String(255))
    contact_phone = db.Column(db.String(50))
    status = db.Column(db.String(50), default="active", index=True)
    config_json = db.Column(db.Text) # Storing JSON configurations as Text
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    # Relationships
    members = db.relationship("CoreOrganizationMember", back_populates="organization", cascade="all, delete-orphan")
    wallet = db.relationship("CoreOrganizationWallet", uselist=False, backref="organization")


class CoreOrganizationMember(db.Model):
    __tablename__ = "core_organization_member"

    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    
    is_active = db.Column(db.Boolean, default=True, nullable=False)
    joined_at = db.Column(db.DateTime, default=datetime.utcnow)
    left_at = db.Column(db.DateTime, nullable=True)

    # Relationships
    organization = db.relationship("CoreOrganization", back_populates="members")
    # user relationship can be defined dynamically or accessed via FK

from app.extensions import db
from datetime import datetime

# --- PHASE 2 MODELS ---

class CorePermission(db.Model):
    __tablename__ = "core_permission"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    slug = db.Column(db.String(255), unique=True, nullable=False, index=True) # e.g. "interaction:create"
    description = db.Column(db.Text)

class CoreRolePermission(db.Model):
    __tablename__ = "core_role_permission"
    id = db.Column(db.Integer, primary_key=True)
    role_id = db.Column(db.Integer, db.ForeignKey("core_role.id"), nullable=False)
    permission_id = db.Column(db.Integer, db.ForeignKey("core_permission.id"), nullable=False)

class CoreRole(db.Model):
    __tablename__ = "core_role"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False) # e.g. "Receptionist"
    slug = db.Column(db.String(255), nullable=False, index=True)
    # If organization_id is NULL, it's a global template role. Otherwise, an org-specific custom role.
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=True)
    
    permissions = db.relationship("CorePermission", secondary="core_role_permission", backref="roles")

class CoreRoleAssignment(db.Model):
    __tablename__ = "core_role_assignment"
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=False)
    role_id = db.Column(db.Integer, db.ForeignKey("core_role.id"), nullable=False)
    assigned_at = db.Column(db.DateTime, default=datetime.utcnow)

    # Relationships
    role = db.relationship("CoreRole", backref="assignments")
    organization = db.relationship("CoreOrganization", backref="role_assignments")
    # user relationship


from app.extensions import db
from datetime import datetime

# --- PHASE 3 MODELS ---

class CoreInteraction(db.Model):
    __tablename__ = "core_interaction"
    id = db.Column(db.Integer, primary_key=True)
    reference = db.Column(db.String(50), unique=True, index=True) # e.g. "MG-00482"
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=False)
    
    # Audit Actors
    creator_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    assigned_to = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=True)
    closed_by = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=True)
    
    # Core Data
    channel = db.Column(db.String(50)) # e.g., Telephone, Web, Walk-in
    category = db.Column(db.String(100)) # e.g., Security, Maintenance
    interaction_type = db.Column(db.String(50), nullable=False) 
    
    title = db.Column(db.String(255), nullable=False)
    description = db.Column(db.Text)
    
    status = db.Column(db.String(50), default="NEW", index=True)
    priority = db.Column(db.String(50), default="NORMAL")
    
    # Timestamps
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    closed_at = db.Column(db.DateTime, nullable=True)

    # Relationships
    organization = db.relationship("CoreOrganization", backref="interactions")
    tasks = db.relationship("CoreTask", back_populates="interaction", cascade="all, delete-orphan")
    
    # User Relationships (using foreign_keys to disambiguate)
    creator = db.relationship("User", foreign_keys=[creator_id], backref="created_interactions")
    assignee = db.relationship("User", foreign_keys=[assigned_to], backref="assigned_interactions")
    closer = db.relationship("User", foreign_keys=[closed_by], backref="closed_interactions")

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
    assignee = db.relationship("User", foreign_keys=[assignee_id])


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

