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
    owner_user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False, unique=True)


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
        db.Index("ix_retirement_membership_org_status", "organisation_id", "status"),
        db.Index("ix_retirement_membership_user_id", "user_id"),
    )

    id = db.Column(db.Integer, primary_key=True)
    organisation_id = db.Column(db.Integer, db.ForeignKey("retirement_organisation.id"), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    requested_role_id = db.Column(db.Integer, db.ForeignKey("retirement_role.id"))
    approved_role_id = db.Column(db.Integer, db.ForeignKey("retirement_role.id"))
    status = db.Column(db.String(16), nullable=False)
    requested_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow, server_default=db.func.now())
    reviewed_at = db.Column(db.DateTime(timezone=True))
    reviewed_by_user_id = db.Column(db.Integer, db.ForeignKey("user.id"))
    review_reason = db.Column(db.Text)

    organisation = db.relationship("RetirementOrganisation")
    requested_role = db.relationship("RetirementRole", foreign_keys=[requested_role_id])
    approved_role = db.relationship("RetirementRole", foreign_keys=[approved_role_id])
