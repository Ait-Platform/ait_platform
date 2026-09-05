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
