"""Delivery entitlement for the existing UIP survey response, never a second ballot."""
from app.extensions import db

class UipVotingInvitation(db.Model):
    __tablename__ = "uip_voting_invitation"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, nullable=False)
    survey_id = db.Column(db.Integer, nullable=False)
    member_id = db.Column(db.Integer, nullable=False)
    token_digest = db.Column(db.String(64), nullable=False, unique=True)
    expires_at = db.Column(db.DateTime(timezone=True), nullable=False)
    status = db.Column(db.String(20), nullable=False)
    attempts = db.Column(db.Integer, nullable=False)
    attempted_at = db.Column(db.DateTime(timezone=True), nullable=False)
    accepted_at = db.Column(db.DateTime(timezone=True))
    error_category = db.Column(db.String(40))
    __table_args__ = (
        db.ForeignKeyConstraint(["survey_id", "organization_id"], ["uip_survey.id", "uip_survey.organization_id"], name="fk_uip_invite_survey_org"),
        db.ForeignKeyConstraint(["member_id", "organization_id"], ["uip_member_profile.id", "uip_member_profile.organization_id"], name="fk_uip_invite_member_org"),
        db.UniqueConstraint("survey_id", "member_id", name="uq_uip_invite_member"),
        db.CheckConstraint("status IN ('ATTEMPTED','ACCEPTED','FAILED','TEST_SUPPRESSED') AND attempts > 0", name="ck_uip_invite_status"),
    )
