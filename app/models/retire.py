"""Business entities owned exclusively by the Retirement program."""
from datetime import datetime, timezone

from app.extensions import db

ROLE_DEFINITIONS = (
    ("organisation_owner", "Organisation Admin / Owner"),
    ("facility_manager", "Facility Manager"),
    ("care_staff", "Care Staff"),
    ("administration_reception", "Administration / Reception"),
    ("finance", "Finance"),
    ("kitchen_catering", "Kitchen / Catering"),
)
STAFF_ROLE_CODES = tuple(code for code, _ in ROLE_DEFINITIONS if code != "organisation_owner")


def utcnow():
    return datetime.now(timezone.utc)


class RetirementOrganisation(db.Model):
    __tablename__ = "retirement_organisation"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(200), nullable=False)
    owner_user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)


class RetirementRole(db.Model):
    __tablename__ = "retirement_role"

    id = db.Column(db.Integer, primary_key=True)
    code = db.Column(db.String(64), nullable=False, unique=True)
    name = db.Column(db.String(100), nullable=False)


class RetirementMembership(db.Model):
    __tablename__ = "retirement_membership"
    __table_args__ = (
        db.UniqueConstraint("organisation_id", "user_id", name="uq_retirement_membership_org_user"),
        db.CheckConstraint("status IN ('pending', 'active', 'denied', 'disabled')", name="ck_retirement_membership_status"),
        db.CheckConstraint("status != 'active' OR approved_role_id IS NOT NULL", name="ck_retirement_membership_active_role"),
        db.CheckConstraint("status NOT IN ('pending', 'denied') OR approved_role_id IS NULL", name="ck_retirement_membership_unapproved_role"),
        db.CheckConstraint("(association_approved_at IS NULL) = (association_approved_by_user_id IS NULL)", name="ck_retirement_membership_association_pair"),
        db.CheckConstraint("status IS NOT NULL OR (association_approved_at IS NOT NULL AND requested_role_id IS NULL AND approved_role_id IS NULL)", name="ck_retirement_membership_association_only"),
        db.Index("ix_retirement_membership_org_status", "organisation_id", "status"),
        db.Index("ix_retirement_membership_user_id", "user_id"),
    )

    id = db.Column(db.Integer, primary_key=True)
    organisation_id = db.Column(db.Integer, db.ForeignKey("retirement_organisation.id"), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    requested_role_id = db.Column(db.Integer, db.ForeignKey("retirement_role.id"))
    approved_role_id = db.Column(db.Integer, db.ForeignKey("retirement_role.id"))
    status = db.Column(db.String(16))
    requested_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow, server_default=db.func.now())
    reviewed_at = db.Column(db.DateTime(timezone=True))
    reviewed_by_user_id = db.Column(db.Integer, db.ForeignKey("user.id"))
    review_reason = db.Column(db.Text)
    association_approved_at = db.Column(db.DateTime(timezone=True))
    association_approved_by_user_id = db.Column(db.Integer, db.ForeignKey("user.id"))

    organisation = db.relationship("RetirementOrganisation")
    requested_role = db.relationship("RetirementRole", foreign_keys=[requested_role_id])
    approved_role = db.relationship("RetirementRole", foreign_keys=[approved_role_id])


class RetirementWaitingUser(db.Model):
    __tablename__ = "retirement_waiting_user"
    __table_args__ = (db.CheckConstraint("length(trim(preferred_name)) > 0", name="ck_retirement_waiting_name"),)
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False, unique=True)
    preferred_name = db.Column(db.String(200), nullable=False)
    home_name_clue = db.Column(db.String(200))
    discovery_consent = db.Column(db.Boolean, nullable=False)
    consent_updated_at = db.Column(db.DateTime(timezone=True), nullable=False)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)


class RetirementAssociationReview(db.Model):
    __tablename__ = "retirement_association_review"
    __table_args__ = (
        db.UniqueConstraint("organisation_id", "waiting_user_id", "version", name="uq_retirement_association_review_version"),
        db.CheckConstraint("decision IN ('approve', 'not_ours')", name="ck_retirement_association_review_decision"),
        db.CheckConstraint("version > 0", name="ck_retirement_association_review_version"),
    )
    id = db.Column(db.Integer, primary_key=True)
    organisation_id = db.Column(db.Integer, db.ForeignKey("retirement_organisation.id"), nullable=False)
    waiting_user_id = db.Column(db.Integer, db.ForeignKey("retirement_waiting_user.id"), nullable=False)
    decision = db.Column(db.String(16), nullable=False)
    reviewed_by_user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    reviewed_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)
    version = db.Column(db.Integer, nullable=False)


class RetirementRelationship(db.Model):
    __tablename__ = 'retirement_relationship'
    __table_args__ = (
        db.CheckConstraint("kind IN ('staff','resident','family_representative')", name='ck_retirement_relationship_kind'),
        db.CheckConstraint("origin IN ('owner','legacy_import')", name='ck_retirement_relationship_origin'),
        db.CheckConstraint("(origin = 'legacy_import') = (legacy_source_membership_id IS NOT NULL)", name='ck_retirement_relationship_source'),
        db.CheckConstraint("legacy_source_membership_id IS NULL OR (legacy_source_membership_id = membership_id AND kind = 'staff')", name='ck_retirement_relationship_import'),
        db.CheckConstraint("(withdrawn_at IS NULL AND withdrawn_by_user_id IS NULL AND withdrawal_reason IS NULL) OR (withdrawn_at IS NOT NULL AND withdrawn_by_user_id IS NOT NULL AND withdrawal_reason IS NOT NULL AND length(trim(withdrawal_reason)) > 0 AND withdrawn_at >= granted_at)", name='ck_retirement_relationship_withdrawal'),
        db.Index('uq_retirement_relationship_current', 'membership_id', 'kind', unique=True, postgresql_where=db.text('withdrawn_at IS NULL')),
    )
    id = db.Column(db.Integer, primary_key=True)
    membership_id = db.Column(db.Integer, db.ForeignKey('retirement_membership.id'), nullable=False)
    kind = db.Column(db.String(32), nullable=False)
    granted_at = db.Column(db.DateTime(timezone=True), nullable=False)
    granted_by_user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    recorded_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)
    origin = db.Column(db.String(16), nullable=False)
    legacy_source_membership_id = db.Column(db.Integer, db.ForeignKey('retirement_membership.id'), unique=True)
    withdrawn_at = db.Column(db.DateTime(timezone=True))
    withdrawn_by_user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    withdrawal_reason = db.Column(db.Text)


class RetirementStaffRoleAssignment(db.Model):
    __tablename__ = 'retirement_staff_role_assignment'
    __table_args__ = (
        db.CheckConstraint("origin IN ('owner','legacy_import')", name='ck_retirement_staff_assignment_origin'),
        db.CheckConstraint("(origin = 'legacy_import') = (legacy_source_membership_id IS NOT NULL)", name='ck_retirement_staff_assignment_source'),
        db.CheckConstraint("(withdrawn_at IS NULL AND withdrawn_by_user_id IS NULL AND withdrawal_reason IS NULL) OR (withdrawn_at IS NOT NULL AND withdrawn_by_user_id IS NOT NULL AND withdrawal_reason IS NOT NULL AND length(trim(withdrawal_reason)) > 0 AND withdrawn_at >= granted_at)", name='ck_retirement_staff_assignment_withdrawal'),
        db.Index('uq_retirement_staff_assignment_current', 'relationship_id', 'role_id', unique=True, postgresql_where=db.text('withdrawn_at IS NULL')),
    )
    id = db.Column(db.Integer, primary_key=True)
    relationship_id = db.Column(db.Integer, db.ForeignKey('retirement_relationship.id'), nullable=False)
    role_id = db.Column(db.Integer, db.ForeignKey('retirement_role.id'), nullable=False)
    granted_at = db.Column(db.DateTime(timezone=True), nullable=False)
    granted_by_user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    recorded_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)
    origin = db.Column(db.String(16), nullable=False)
    legacy_source_membership_id = db.Column(db.Integer, db.ForeignKey('retirement_membership.id'), unique=True)
    withdrawn_at = db.Column(db.DateTime(timezone=True))
    withdrawn_by_user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    withdrawal_reason = db.Column(db.Text)


class RetirementAuthorityEvent(db.Model):
    __tablename__ = 'retirement_authority_event'
    __table_args__ = (
        db.UniqueConstraint('membership_id','version',name='uq_retirement_authority_event_version'),
        db.CheckConstraint('version > 0',name='ck_retirement_authority_event_version'),
        db.CheckConstraint("action IN ('relationship_granted','relationship_withdrawn','role_granted','role_withdrawn','relationship_imported','role_imported')",name='ck_retirement_authority_event_action'),
        db.CheckConstraint("(action IN ('relationship_imported','role_imported') AND actor_user_id IS NULL) OR (action NOT IN ('relationship_imported','role_imported') AND actor_user_id IS NOT NULL)",name='ck_retirement_authority_event_actor'),
        db.Index('ix_retirement_authority_event_relationship','relationship_id','id'),
    )
    id = db.Column(db.Integer, primary_key=True)
    membership_id = db.Column(db.Integer, db.ForeignKey('retirement_membership.id'), nullable=False)
    relationship_id = db.Column(db.Integer, db.ForeignKey('retirement_relationship.id'), nullable=False)
    assignment_id = db.Column(db.Integer, db.ForeignKey('retirement_staff_role_assignment.id'))
    action = db.Column(db.String(32), nullable=False)
    actor_user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    recorded_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)
    version = db.Column(db.Integer, nullable=False)
    reason = db.Column(db.Text, nullable=False)
