"""Additional UIP operational records; no application startup side effects."""
from app.extensions import db


class UipDocumentFolder(db.Model):
    __tablename__ = "uip_document_folder"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=False)
    name = db.Column(db.String(100), nullable=False)
    __table_args__ = (
        db.UniqueConstraint("id", "organization_id", name="uq_uip_document_folder_org"),
        db.UniqueConstraint("organization_id", "name", name="uq_uip_document_folder_name"),
    )


class UipDocumentVersion(db.Model):
    __tablename__ = "uip_document_version"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, nullable=False)
    document_id = db.Column(db.Integer, nullable=False)
    version = db.Column(db.Integer, nullable=False)
    effective_date = db.Column(db.Date, nullable=False)
    actor_user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, server_default=db.func.now())
    filename = db.Column(db.String(255), nullable=False)
    storage_key = db.Column(db.String(64), nullable=False, unique=True)
    content_type = db.Column(db.String(100), nullable=False)
    size_bytes = db.Column(db.Integer, nullable=False)
    sha256 = db.Column(db.String(64), nullable=False)
    replacement_reason = db.Column(db.Text, nullable=False)
    __table_args__ = (
        db.ForeignKeyConstraint(["document_id", "organization_id"], ["uip_document.id", "uip_document.organization_id"], name="fk_uip_document_version_org"),
        db.UniqueConstraint("document_id", "version", name="uq_uip_document_version"),
        db.CheckConstraint("version > 0 AND size_bytes > 0", name="ck_uip_document_version_values"),
    )


class UipFollowUp(db.Model):
    __tablename__ = "uip_follow_up"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, nullable=False)
    interaction_id = db.Column(db.Integer, nullable=False)
    actor_user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    occurred_at = db.Column(db.DateTime(timezone=True), nullable=False)
    recorded_at = db.Column(db.DateTime(timezone=True), nullable=False, server_default=db.func.now())
    method = db.Column(db.String(30), nullable=False)
    outcome = db.Column(db.String(30), nullable=False)
    next_action = db.Column(db.String(30), nullable=False)
    next_action_at = db.Column(db.DateTime(timezone=True))
    note = db.Column(db.Text)
    completed_at = db.Column(db.DateTime(timezone=True))
    completed_by = db.Column(db.Integer, db.ForeignKey("user.id"))
    __table_args__ = (
        db.ForeignKeyConstraint(["interaction_id", "organization_id"], ["core_interaction.id", "core_interaction.organization_id"], name="fk_uip_followup_issue_org"),
        db.CheckConstraint("method IN ('TELEPHONE','EMAIL','WHATSAPP','IN_PERSON','INTERNAL','OTHER')", name="ck_uip_followup_method"),
        db.CheckConstraint("outcome IN ('CONTACTED','NO_ANSWER','UNREACHABLE','INFORMATION_RECEIVED','ESCALATED','NO_CONTACT_REQUIRED')", name="ck_uip_followup_outcome"),
        db.CheckConstraint("next_action IN ('NONE','CONTACT','REVIEW','ASSIGN','ESCALATE')", name="ck_uip_followup_next"),
        db.CheckConstraint("next_action = 'NONE' OR next_action_at IS NOT NULL", name="ck_uip_followup_due"),
        db.CheckConstraint("(completed_at IS NULL) = (completed_by IS NULL)", name="ck_uip_followup_completed"),
    )


class UipReferralEvent(db.Model):
    __tablename__ = "uip_referral_event"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, nullable=False)
    referral_id = db.Column(db.Integer, nullable=False)
    actor_user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    occurred_at = db.Column(db.DateTime(timezone=True), nullable=False)
    recorded_at = db.Column(db.DateTime(timezone=True), nullable=False, server_default=db.func.now())
    previous_state = db.Column(db.String(50))
    new_state = db.Column(db.String(50), nullable=False)
    municipality_reference = db.Column(db.String(100))
    note = db.Column(db.Text)
    resulting_version = db.Column(db.Integer, nullable=False)
    __table_args__ = (
        db.ForeignKeyConstraint(["referral_id", "organization_id"], ["uip_municipal_referral.id", "uip_municipal_referral.organization_id"], name="fk_uip_referral_event_org"),
        db.UniqueConstraint("referral_id", "resulting_version", name="uq_uip_referral_event_version"),
    )


class UipCommunicationLog(db.Model):
    __tablename__ = "uip_communication_log"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=False)
    interaction_id = db.Column(db.Integer)
    work_order_id = db.Column(db.Integer)
    referral_id = db.Column(db.Integer)
    actor_user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    occurred_at = db.Column(db.DateTime(timezone=True), nullable=False)
    recorded_at = db.Column(db.DateTime(timezone=True), nullable=False, server_default=db.func.now())
    channel = db.Column(db.String(30), nullable=False)
    direction = db.Column(db.String(20), nullable=False)
    party_classification = db.Column(db.String(30), nullable=False)
    purpose = db.Column(db.String(30), nullable=False)
    status = db.Column(db.String(30), nullable=False)
    summary = db.Column(db.Text)
    __table_args__ = (
        db.ForeignKeyConstraint(["interaction_id", "organization_id"], ["core_interaction.id", "core_interaction.organization_id"], name="fk_uip_comm_issue_org"),
        db.ForeignKeyConstraint(["work_order_id", "organization_id"], ["uip_work_order.id", "uip_work_order.organization_id"], name="fk_uip_comm_order_org"),
        db.ForeignKeyConstraint(["referral_id", "organization_id"], ["uip_municipal_referral.id", "uip_municipal_referral.organization_id"], name="fk_uip_comm_referral_org"),
        db.CheckConstraint("direction IN ('INBOUND','OUTBOUND')", name="ck_uip_comm_direction"),
        db.CheckConstraint("status IN ('RECORDED','RECEIVED','FAILED','DELIVERY_UNAVAILABLE')", name="ck_uip_comm_status"),
    )
