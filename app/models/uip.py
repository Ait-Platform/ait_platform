from app.extensions import db
from datetime import datetime


class UipSlaPolicy(db.Model):
    __tablename__ = "uip_sla_policy"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=False)
    category = db.Column(db.String(100), nullable=False)
    priority = db.Column(db.String(20), nullable=False)
    stage = db.Column(db.String(30), nullable=False)
    target_minutes = db.Column(db.Integer, nullable=False)
    warning_minutes = db.Column(db.Integer, nullable=False)
    created_by = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, server_default=db.func.now())
    is_active = db.Column(db.Boolean, nullable=False, default=True)
    __table_args__ = (
        db.UniqueConstraint("id", "organization_id", name="uq_uip_sla_policy_org"),
        db.CheckConstraint("target_minutes > 0 AND warning_minutes >= 0 AND warning_minutes <= target_minutes", name="ck_uip_sla_policy_minutes"),
        db.CheckConstraint("stage IN ('acknowledgement','dispatch','acceptance','commencement','completion','closure')", name="ck_uip_sla_policy_stage"),
        db.Index("uq_uip_sla_policy_active", "organization_id", "category", "priority", "stage", unique=True, postgresql_where=db.text("is_active")),
    )


class UipSlaClock(db.Model):
    __tablename__ = "uip_sla_clock"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, nullable=False)
    interaction_id = db.Column(db.Integer, nullable=False)
    work_order_id = db.Column(db.Integer)
    policy_id = db.Column(db.Integer, nullable=False)
    stage = db.Column(db.String(30), nullable=False)
    started_at = db.Column(db.DateTime(timezone=True), nullable=False)
    target_at = db.Column(db.DateTime(timezone=True), nullable=False)
    warning_at = db.Column(db.DateTime(timezone=True), nullable=False)
    finished_at = db.Column(db.DateTime(timezone=True))
    stopped_at = db.Column(db.DateTime(timezone=True))
    stop_reason = db.Column(db.String(30))
    __table_args__ = (
        db.ForeignKeyConstraint(["interaction_id", "organization_id"], ["core_interaction.id", "core_interaction.organization_id"], name="fk_uip_sla_issue_org"),
        db.ForeignKeyConstraint(["work_order_id", "organization_id"], ["uip_work_order.id", "uip_work_order.organization_id"], name="fk_uip_sla_order_org"),
        db.ForeignKeyConstraint(["policy_id", "organization_id"], ["uip_sla_policy.id", "uip_sla_policy.organization_id"], name="fk_uip_sla_policy_org"),
        db.CheckConstraint("target_at >= warning_at AND warning_at >= started_at", name="ck_uip_sla_clock_dates"),
        db.CheckConstraint("finished_at IS NULL OR finished_at >= started_at", name="ck_uip_sla_clock_finish"),
        db.Index("uq_uip_sla_issue_stage", "interaction_id", "stage", unique=True, postgresql_where=db.text("work_order_id IS NULL")),
        db.Index("uq_uip_sla_order_stage", "work_order_id", "stage", unique=True, postgresql_where=db.text("work_order_id IS NOT NULL")),
    )

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
    availability = db.Column(db.String(20), nullable=False, server_default="UNKNOWN")
    version = db.Column(db.Integer, nullable=False, server_default="1")
    updated_at = db.Column(db.DateTime(timezone=True), nullable=False, server_default=db.func.now())
    __table_args__ = (
        db.UniqueConstraint("id", "organization_id", name="uq_uip_provider_id_org"),
        db.CheckConstraint("availability IN ('UNKNOWN','AVAILABLE','UNAVAILABLE')", name="ck_uip_provider_availability"),
    )

class UipWorkOrder(db.Model):
    __tablename__ = "uip_work_order"
    id = db.Column(db.Integer, primary_key=True)
    interaction_id = db.Column(db.Integer, db.ForeignKey("core_interaction.id"), nullable=False)
    provider_id = db.Column(db.Integer, db.ForeignKey("uip_provider.id"), nullable=False)
    
    reference = db.Column(db.String(50), unique=True)
    description = db.Column(db.Text)
    status = db.Column(db.String(50), default="CREATED", nullable=False)
    
    cost_cents = db.Column(db.Integer, nullable=True) # Linked to Ledger later
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    completed_at = db.Column(db.DateTime, nullable=True)
    verified_at = db.Column(db.DateTime, nullable=True)

    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=False)
    created_by = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    version = db.Column(db.Integer, nullable=False, default=1)
    state_changed_at = db.Column(db.DateTime(timezone=True), nullable=False, server_default=db.func.now())
    service_location = db.Column(db.String(500), nullable=False)
    __table_args__ = (
        db.UniqueConstraint("id", "organization_id", name="uq_uip_order_id_org"),
        db.ForeignKeyConstraint(["interaction_id", "organization_id"], ["core_interaction.id", "core_interaction.organization_id"], name="fk_uip_order_issue_org"),
        db.ForeignKeyConstraint(["provider_id", "organization_id"], ["uip_provider.id", "uip_provider.organization_id"], name="fk_uip_order_provider_org"),
        db.CheckConstraint("status IN ('CREATED','DISPATCHED','ACCEPTED','IN_PROGRESS','COMPLETED','VERIFIED','CLOSED','CANCELLED','REJECTED','FAILED')", name="ck_uip_order_state"),
        db.CheckConstraint("version > 0", name="ck_uip_order_version"),
        db.Index("uq_uip_order_open_issue", "interaction_id", unique=True, postgresql_where=db.text("status NOT IN ('CLOSED','CANCELLED','REJECTED','FAILED')")),
        db.Index("ix_uip_order_org_provider_state", "organization_id", "provider_id", "status"),
    )
    interaction = db.relationship("CoreInteraction", backref="work_orders", foreign_keys=[interaction_id])
    provider = db.relationship("UipProvider", backref="work_orders", foreign_keys=[provider_id])


# --- MUNICIPALITY (Step 14) ---

class UipMunicipalReferral(db.Model):
    __tablename__ = "uip_municipal_referral"
    id = db.Column(db.Integer, primary_key=True)
    interaction_id = db.Column(db.Integer, db.ForeignKey("core_interaction.id"), nullable=False)
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=False)
    version = db.Column(db.Integer, nullable=False, server_default="1")
    __table_args__ = (
        db.UniqueConstraint("id", "organization_id", name="uq_uip_referral_org"),
        db.ForeignKeyConstraint(["interaction_id", "organization_id"], ["core_interaction.id", "core_interaction.organization_id"], name="fk_uip_referral_issue_org"),
    )
    
    department = db.Column(db.String(100)) # e.g., Water & Sanitation, Parks
    municipality_reference = db.Column(db.String(100)) # The reference number given by the city
    
    status = db.Column(db.String(50), default="ESCALATED") # ESCALATED, IN_PROGRESS, RESOLVED_BY_CITY
    sla_expected_date = db.Column(db.DateTime, nullable=True)
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    resolved_at = db.Column(db.DateTime, nullable=True)
    
    interaction = db.relationship("CoreInteraction", backref="municipal_referrals", foreign_keys=[interaction_id])


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
    agenda = db.Column(db.Text)
    eligibility_basis = db.Column(db.JSON)
    quorum_rule = db.Column(db.JSON)
    eligible_count = db.Column(db.Integer)
    attendance_count = db.Column(db.Integer)
    required_quorum = db.Column(db.Integer)
    quorum_achieved = db.Column(db.Boolean)
    quorum_recorded_at = db.Column(db.DateTime(timezone=True))
    quorum_recorded_by = db.Column(db.Integer, db.ForeignKey("user.id"))
    __table_args__ = (db.UniqueConstraint("id", "organization_id", name="uq_uip_meeting_org"),)
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class UipResolution(db.Model):
    __tablename__ = "uip_resolution"
    id = db.Column(db.Integer, primary_key=True)
    meeting_id = db.Column(db.Integer, db.ForeignKey("uip_committee_meeting.id"), nullable=True)
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=False)
    survey_id = db.Column(db.Integer)
    decision_date = db.Column(db.Date)
    recorded_by = db.Column(db.Integer, db.ForeignKey("user.id"))
    responsible_user_id = db.Column(db.Integer, db.ForeignKey("user.id"))
    result_basis = db.Column(db.JSON)
    supersedes_id = db.Column(db.Integer)
    __table_args__ = (
        db.UniqueConstraint("id", "organization_id", name="uq_uip_resolution_org"),
        db.ForeignKeyConstraint(["meeting_id", "organization_id"], ["uip_committee_meeting.id", "uip_committee_meeting.organization_id"], name="fk_uip_resolution_meeting_org"),
        db.ForeignKeyConstraint(["survey_id", "organization_id"], ["uip_survey.id", "uip_survey.organization_id"], name="fk_uip_resolution_survey_org"),
        db.ForeignKeyConstraint(["supersedes_id", "organization_id"], ["uip_resolution.id", "uip_resolution.organization_id"], name="fk_uip_resolution_supersedes_org"),
        db.CheckConstraint("(meeting_id IS NOT NULL) <> (survey_id IS NOT NULL)", name="ck_uip_resolution_source"),
    )
    
    title = db.Column(db.String(255), nullable=False)
    description = db.Column(db.Text)
    status = db.Column(db.String(50), default="PROPOSED") # PROPOSED, APPROVED, REJECTED, EXECUTED
    
    # "An Approved Resolution automatically spawns a Task assigned to an operational user"
    linked_task_id = db.Column(db.Integer, db.ForeignKey("core_task.id"), nullable=True)
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    meeting = db.relationship("UipCommitteeMeeting", backref="resolutions", foreign_keys=[meeting_id])
    task = db.relationship("CoreTask", backref="governance_resolution")

# --- DOCUMENTS & COMMUNICATIONS (Steps 16 & 17) ---

class UipDocument(db.Model):
    __tablename__ = "uip_document"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=False)
    uploader_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    title = db.Column(db.String(255))
    category = db.Column(db.String(100))
    folder_id = db.Column(db.Integer)
    current_version = db.Column(db.Integer, nullable=False, server_default="0")
    __table_args__ = (
        db.UniqueConstraint("id", "organization_id", name="uq_uip_document_org"),
        db.ForeignKeyConstraint(["folder_id", "organization_id"], ["uip_document_folder.id", "uip_document_folder.organization_id"], name="fk_uip_document_folder_org"),
    )
    
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


# Phase 2: organisation register. Application roles never confer ownership/eligibility.
class UipMemberProfile(db.Model):
    __tablename__ = "uip_member_profile"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=False)
    membership_id = db.Column(db.Integer, nullable=False, unique=True)
    reference = db.Column(db.String(50), nullable=False)
    name = db.Column(db.String(255), nullable=False)
    member_type = db.Column(db.String(20), nullable=False, default="person")
    email = db.Column(db.String(255))
    phone = db.Column(db.String(50))
    is_active = db.Column(db.Boolean, nullable=False, default=True)
    eligibility_status = db.Column(db.String(20), nullable=False, default="unverified")
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, server_default=db.func.now())
    updated_at = db.Column(db.DateTime(timezone=True), nullable=False, server_default=db.func.now(), onupdate=db.func.now())
    __table_args__ = (
        db.UniqueConstraint("id", "organization_id", name="uq_uip_member_id_org"),
        db.UniqueConstraint("organization_id", "reference", name="uq_uip_member_reference"),
        db.ForeignKeyConstraint(["membership_id", "organization_id"],
                                ["core_organization_member.id", "core_organization_member.organization_id"],
                                name="fk_uip_profile_membership_org"),
        db.CheckConstraint("member_type IN ('person','business')", name="ck_uip_member_type"),
        db.CheckConstraint("eligibility_status IN ('unverified','eligible','ineligible')", name="ck_uip_member_eligibility"),
    )
    membership = db.relationship("CoreOrganizationMember", foreign_keys=[membership_id, organization_id])


class UipProperty(db.Model):
    __tablename__ = "uip_property"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=False)
    reference = db.Column(db.String(50), nullable=False)
    address = db.Column(db.String(500), nullable=False)
    rates_reference = db.Column(db.String(100))
    classification = db.Column(db.String(20), nullable=False)
    is_active = db.Column(db.Boolean, nullable=False, default=True)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, server_default=db.func.now())
    updated_at = db.Column(db.DateTime(timezone=True), nullable=False, server_default=db.func.now(), onupdate=db.func.now())
    __table_args__ = (
        db.UniqueConstraint("id", "organization_id", name="uq_uip_property_id_org"),
        db.UniqueConstraint("organization_id", "reference", name="uq_uip_property_reference"),
        db.CheckConstraint("classification IN ('residential','business','mixed','other')", name="ck_uip_property_classification"),
    )


class UipPropertyMember(db.Model):
    __tablename__ = "uip_property_member"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=False)
    property_id = db.Column(db.Integer, nullable=False)
    member_id = db.Column(db.Integer, nullable=False)
    relationship = db.Column(db.String(20), nullable=False)
    valid_from = db.Column(db.Date, nullable=False)
    valid_to = db.Column(db.Date)
    is_verified = db.Column(db.Boolean, nullable=False, default=False)
    __table_args__ = (
        db.ForeignKeyConstraint(["property_id", "organization_id"], ["uip_property.id", "uip_property.organization_id"], name="fk_uip_property_member_property"),
        db.ForeignKeyConstraint(["member_id", "organization_id"], ["uip_member_profile.id", "uip_member_profile.organization_id"], name="fk_uip_property_member_member"),
        db.UniqueConstraint("organization_id", "property_id", "member_id", "relationship", "valid_from", name="uq_uip_property_member"),
        db.CheckConstraint("relationship IN ('owner','occupier','representative')", name="ck_uip_property_member_relationship"),
        db.CheckConstraint("valid_to IS NULL OR valid_to >= valid_from", name="ck_uip_property_member_dates"),
    )


class UipMemberRepresentative(db.Model):
    __tablename__ = "uip_member_representative"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=False)
    member_id = db.Column(db.Integer, nullable=False)
    representative_id = db.Column(db.Integer, nullable=False)
    valid_from = db.Column(db.Date, nullable=False)
    valid_to = db.Column(db.Date)
    is_verified = db.Column(db.Boolean, nullable=False, default=False)
    __table_args__ = (
        db.ForeignKeyConstraint(["member_id", "organization_id"], ["uip_member_profile.id", "uip_member_profile.organization_id"], name="fk_uip_represented_member"),
        db.ForeignKeyConstraint(["representative_id", "organization_id"], ["uip_member_profile.id", "uip_member_profile.organization_id"], name="fk_uip_representative_member"),
        db.UniqueConstraint("organization_id", "member_id", "representative_id", "valid_from", name="uq_uip_representative"),
        db.CheckConstraint("member_id <> representative_id", name="ck_uip_representative_distinct"),
        db.CheckConstraint("valid_to IS NULL OR valid_to >= valid_from", name="ck_uip_representative_dates"),
    )


class UipCommunicationPreference(db.Model):
    __tablename__ = "uip_communication_preference"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=False)
    member_id = db.Column(db.Integer, nullable=False)
    channel = db.Column(db.String(20), nullable=False)
    preference = db.Column(db.String(20), nullable=False, default="unspecified")
    updated_at = db.Column(db.DateTime(timezone=True), nullable=False, server_default=db.func.now(), onupdate=db.func.now())
    __table_args__ = (
        db.ForeignKeyConstraint(["member_id", "organization_id"], ["uip_member_profile.id", "uip_member_profile.organization_id"], name="fk_uip_preference_member"),
        db.UniqueConstraint("organization_id", "member_id", "channel", name="uq_uip_preference"),
        db.CheckConstraint("channel IN ('Telephone','Email','WhatsApp','Post')", name="ck_uip_preference_channel"),
        db.CheckConstraint("preference IN ('unspecified','allowed','declined')", name="ck_uip_preference_value"),
    )


class UipAuditEvent(db.Model):
    __tablename__ = "uip_audit_event"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=False)
    actor_user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    action = db.Column(db.String(80), nullable=False)
    entity_type = db.Column(db.String(50), nullable=False)
    entity_id = db.Column(db.Integer, nullable=False)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, server_default=db.func.now())
    metadata_json = db.Column(db.JSON, nullable=False, default=dict)
    __table_args__ = (
        db.UniqueConstraint("id", "organization_id", name="uq_uip_audit_id_org"),
        db.Index("ix_uip_audit_org_time", "organization_id", "created_at", "id"),
        db.Index("ix_uip_audit_org_entity", "organization_id", "entity_type", "entity_id"),
    )


class UipProviderCapability(db.Model):
    __tablename__ = "uip_provider_capability"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, nullable=False)
    provider_id = db.Column(db.Integer, nullable=False)
    category = db.Column(db.String(100), nullable=False)
    __table_args__ = (
        db.ForeignKeyConstraint(["provider_id", "organization_id"], ["uip_provider.id", "uip_provider.organization_id"], name="fk_uip_capability_provider_org"),
        db.UniqueConstraint("provider_id", "category", name="uq_uip_provider_category"),
    )


class UipProviderUser(db.Model):
    __tablename__ = "uip_provider_user"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, nullable=False)
    provider_id = db.Column(db.Integer, nullable=False)
    membership_id = db.Column(db.Integer, nullable=False)
    is_active = db.Column(db.Boolean, nullable=False, default=True)
    linked_by = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    linked_at = db.Column(db.DateTime(timezone=True), nullable=False, server_default=db.func.now())
    revoked_by = db.Column(db.Integer, db.ForeignKey("user.id"))
    revoked_at = db.Column(db.DateTime(timezone=True))
    __table_args__ = (
        db.ForeignKeyConstraint(["provider_id", "organization_id"], ["uip_provider.id", "uip_provider.organization_id"], name="fk_uip_provider_user_provider_org"),
        db.ForeignKeyConstraint(["membership_id", "organization_id"], ["core_organization_member.id", "core_organization_member.organization_id"], name="fk_uip_provider_user_member_org"),
        db.Index("uq_uip_provider_user_active", "provider_id", "membership_id", unique=True, postgresql_where=db.text("is_active")),
    )


class UipWorkOrderAction(db.Model):
    __tablename__ = "uip_work_order_action"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, nullable=False)
    work_order_id = db.Column(db.Integer, nullable=False)
    actor_user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    action = db.Column(db.String(40), nullable=False)
    previous_state = db.Column(db.String(50))
    new_state = db.Column(db.String(50), nullable=False)
    resulting_version = db.Column(db.Integer, nullable=False)
    occurred_at = db.Column(db.DateTime(timezone=True), nullable=False, server_default=db.func.now())
    request_key = db.Column(db.String(36), nullable=False)
    fingerprint = db.Column(db.String(64), nullable=False)
    reason_code = db.Column(db.String(40))
    note = db.Column(db.Text)
    dispatch_method = db.Column(db.String(30))
    audit_event_id = db.Column(db.Integer, nullable=False)
    __table_args__ = (
        db.ForeignKeyConstraint(["work_order_id", "organization_id"], ["uip_work_order.id", "uip_work_order.organization_id"], name="fk_uip_action_order_org"),
        db.ForeignKeyConstraint(["audit_event_id", "organization_id"], ["uip_audit_event.id", "uip_audit_event.organization_id"], name="fk_uip_action_audit_org"),
        db.UniqueConstraint("organization_id", "actor_user_id", "request_key", name="uq_uip_action_request"),
        db.UniqueConstraint("work_order_id", "resulting_version", name="uq_uip_action_version"),
        db.Index("ix_uip_action_order_time", "organization_id", "work_order_id", "occurred_at"),
    )


# Register the additive UIP models with both the application and isolated harness.
from .uip_operations import UipFollowUp, UipReferralEvent, UipCommunicationLog, UipDocumentFolder, UipDocumentVersion
from .uip_governance import UipQuorumRule, UipMeetingParticipant, UipSurvey, UipSurveyResponse, UipDecisionEvent

from .uip_finance import UipFinanceTransaction, UipFinanceCommitment, UipFinanceBudgetLine, UipFinanceBudgetRevision, UipFinanceCommitmentRevision
