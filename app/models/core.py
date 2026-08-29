from app.extensions import db
from datetime import datetime

class CoreOrganization(db.Model):
    __tablename__ = "core_organization"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    slug = db.Column(db.String(255), unique=True, nullable=False, index=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    # Relationships
    members = db.relationship("CoreOrganizationMember", back_populates="organization", cascade="all, delete-orphan")


class CoreOrganizationMember(db.Model):
    __tablename__ = "core_organization_member"

    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    joined_at = db.Column(db.DateTime, default=datetime.utcnow)

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

