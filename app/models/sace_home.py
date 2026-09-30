"""Independent HOME endorsement records. No LITRE or participant-state foreign keys."""
from datetime import datetime, timezone
from app.extensions import db


def now():
    return datetime.now(timezone.utc)


class HomeController(db.Model):
    __tablename__ = "sace_home_controller"
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False, unique=True)
    active = db.Column(db.Boolean, nullable=False, default=True)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=now)


class HomeProvisioning(db.Model):
    __tablename__ = "sace_home_provisioning"
    id = db.Column(db.Integer, primary_key=True)
    token_hash = db.Column(db.String(64), nullable=False, unique=True)
    email = db.Column(db.String(255), nullable=False)
    issued_by = db.Column(db.String(255), nullable=False)
    expires_at = db.Column(db.DateTime(timezone=True), nullable=False)
    claimed_by = db.Column(db.Integer, db.ForeignKey("user.id"))
    claimed_at = db.Column(db.DateTime(timezone=True))
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=now)


class HomeInvitation(db.Model):
    __tablename__ = "sace_home_invitation"
    id = db.Column(db.Integer, primary_key=True)
    controller_id = db.Column(db.Integer, db.ForeignKey("sace_home_controller.id"), nullable=False)
    code_hash = db.Column(db.String(64), nullable=False, unique=True)
    status = db.Column(db.String(16), nullable=False, default="unclaimed")
    expires_at = db.Column(db.DateTime(timezone=True), nullable=False)
    claimed_at = db.Column(db.DateTime(timezone=True))
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=now)
    __table_args__ = (db.CheckConstraint("status IN ('unclaimed','claimed','revoked')", name="ck_sace_home_invitation_status"),)


class HomePledge(db.Model):
    __tablename__ = "sace_home_pledge"
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    role = db.Column(db.String(16), nullable=False)
    signature = db.Column(db.String(255), nullable=False)
    version = db.Column(db.String(40), nullable=False)
    text_hash = db.Column(db.String(64), nullable=False)
    provisioning_id = db.Column(db.Integer, db.ForeignKey("sace_home_provisioning.id"), unique=True)
    invitation_id = db.Column(db.Integer, db.ForeignKey("sace_home_invitation.id"), unique=True)
    accepted_at = db.Column(db.DateTime(timezone=True), nullable=False)
    recorded_at = db.Column(db.DateTime(timezone=True), nullable=False, default=now)
    __table_args__ = (db.CheckConstraint(
        "(role = 'controller' AND provisioning_id IS NOT NULL AND invitation_id IS NULL) OR "
        "(role = 'auditor' AND invitation_id IS NOT NULL AND provisioning_id IS NULL)",
        name="ck_sace_home_pledge_context"),)


class HomeAssignment(db.Model):
    __tablename__ = "sace_home_assignment"
    id = db.Column(db.Integer, primary_key=True)
    invitation_id = db.Column(db.Integer, db.ForeignKey("sace_home_invitation.id"), nullable=False, unique=True)
    auditor_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False, index=True)
    status = db.Column(db.String(16), nullable=False, default="active")
    requirements_version = db.Column(db.String(40), nullable=False, default="home-foundation-v1")
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=now)
    completed_at = db.Column(db.DateTime(timezone=True))
    __table_args__ = (db.CheckConstraint("status IN ('active','completed','revoked')", name="ck_sace_home_assignment_status"),
        db.CheckConstraint("(status = 'completed') = (completed_at IS NOT NULL)", name="ck_sace_home_completion"))


class HomeDocument(db.Model):
    __tablename__ = "sace_home_document"
    id = db.Column(db.Integer, primary_key=True)
    kind = db.Column(db.String(60), nullable=False, unique=True)
    title = db.Column(db.String(255), nullable=False)


class HomeDocumentVersion(db.Model):
    __tablename__ = "sace_home_document_version"
    id = db.Column(db.Integer, primary_key=True)
    document_id = db.Column(db.Integer, db.ForeignKey("sace_home_document.id"), nullable=False)
    version = db.Column(db.String(60), nullable=False)
    storage_key = db.Column(db.String(255), nullable=False)
    sha256 = db.Column(db.String(64), nullable=False)
    source_manifest = db.Column(db.JSON, nullable=False)
    approved_by = db.Column(db.Integer, db.ForeignKey("sace_home_controller.id"), nullable=False)
    published_at = db.Column(db.DateTime(timezone=True), nullable=False, default=now)
    __table_args__ = (db.UniqueConstraint("document_id", "version", name="uq_sace_home_document_version"),)


class HomeEvidence(db.Model):
    __tablename__ = "sace_home_evidence"
    id = db.Column(db.Integer, primary_key=True)
    assignment_id = db.Column(db.Integer, db.ForeignKey("sace_home_assignment.id"), nullable=False, index=True)
    actor_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    item = db.Column(db.String(60), nullable=False)
    event = db.Column(db.String(40), nullable=False)
    document_version_id = db.Column(db.Integer, db.ForeignKey("sace_home_document_version.id"))
    details = db.Column(db.JSON, nullable=False, default=dict)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=now)
