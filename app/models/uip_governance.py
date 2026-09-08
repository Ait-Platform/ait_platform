"""UIP governance snapshots; account roles are not participation eligibility."""
from app.extensions import db


class UipQuorumRule(db.Model):
    __tablename__ = "uip_quorum_rule"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=False, unique=True)
    percentage = db.Column(db.Integer, nullable=False)
    minimum = db.Column(db.Integer, nullable=False)
    relationship = db.Column(db.String(20), nullable=False)
    updated_by = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    updated_at = db.Column(db.DateTime(timezone=True), nullable=False, server_default=db.func.now())
    __table_args__ = (
        db.CheckConstraint("percentage > 0 AND percentage <= 100 AND minimum >= 1", name="ck_uip_quorum_values"),
        db.CheckConstraint("relationship IN ('owner','occupier')", name="ck_uip_quorum_relationship"),
    )


class UipMeetingParticipant(db.Model):
    __tablename__ = "uip_meeting_participant"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, nullable=False)
    meeting_id = db.Column(db.Integer, nullable=False)
    member_id = db.Column(db.Integer, nullable=False)
    attended_by_member_id = db.Column(db.Integer)
    status = db.Column(db.String(20), nullable=False)
    recorded_by = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    recorded_at = db.Column(db.DateTime(timezone=True), nullable=False, server_default=db.func.now())
    __table_args__ = (
        db.ForeignKeyConstraint(["meeting_id", "organization_id"], ["uip_committee_meeting.id", "uip_committee_meeting.organization_id"], name="fk_uip_participant_meeting_org"),
        db.ForeignKeyConstraint(["member_id", "organization_id"], ["uip_member_profile.id", "uip_member_profile.organization_id"], name="fk_uip_participant_member_org"),
        db.ForeignKeyConstraint(["attended_by_member_id", "organization_id"], ["uip_member_profile.id", "uip_member_profile.organization_id"], name="fk_uip_participant_proxy_org"),
        db.UniqueConstraint("meeting_id", "member_id", name="uq_uip_meeting_member"),
        db.CheckConstraint("status IN ('INVITED','PRESENT','APOLOGY','ABSENT')", name="ck_uip_attendance_status"),
    )


class UipSurvey(db.Model):
    __tablename__ = "uip_survey"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, db.ForeignKey("core_organization.id"), nullable=False)
    title = db.Column(db.String(255), nullable=False)
    purpose = db.Column(db.Text, nullable=False)
    opens_at = db.Column(db.DateTime(timezone=True), nullable=False)
    closes_at = db.Column(db.DateTime(timezone=True), nullable=False)
    relationship = db.Column(db.String(20), nullable=False)
    identifiable = db.Column(db.Boolean, nullable=False)
    questions = db.Column(db.JSON, nullable=False)
    status = db.Column(db.String(20), nullable=False)
    results = db.Column(db.JSON)
    created_by = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, server_default=db.func.now())
    finalized_at = db.Column(db.DateTime(timezone=True))
    finalized_by = db.Column(db.Integer, db.ForeignKey("user.id"))
    __table_args__ = (
        db.UniqueConstraint("id", "organization_id", name="uq_uip_survey_org"),
        db.CheckConstraint("closes_at > opens_at", name="ck_uip_survey_dates"),
        db.CheckConstraint("relationship IN ('owner','occupier')", name="ck_uip_survey_relationship"),
        db.CheckConstraint("status IN ('OPEN','FINALIZED')", name="ck_uip_survey_state"),
    )


class UipSurveyResponse(db.Model):
    __tablename__ = "uip_survey_response"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, nullable=False)
    survey_id = db.Column(db.Integer, nullable=False)
    member_id = db.Column(db.Integer, nullable=False)
    actor_user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    eligibility_basis = db.Column(db.JSON, nullable=False)
    answers = db.Column(db.JSON, nullable=False)
    responded_at = db.Column(db.DateTime(timezone=True), nullable=False)
    __table_args__ = (
        db.ForeignKeyConstraint(["survey_id", "organization_id"], ["uip_survey.id", "uip_survey.organization_id"], name="fk_uip_response_survey_org"),
        db.ForeignKeyConstraint(["member_id", "organization_id"], ["uip_member_profile.id", "uip_member_profile.organization_id"], name="fk_uip_response_member_org"),
        db.UniqueConstraint("survey_id", "member_id", name="uq_uip_survey_response"),
    )


class UipDecisionEvent(db.Model):
    __tablename__ = "uip_decision_event"
    id = db.Column(db.Integer, primary_key=True)
    organization_id = db.Column(db.Integer, nullable=False)
    decision_id = db.Column(db.Integer, nullable=False)
    actor_user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    status = db.Column(db.String(30), nullable=False)
    note = db.Column(db.Text, nullable=False)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, server_default=db.func.now())
    __table_args__ = (
        db.ForeignKeyConstraint(["decision_id", "organization_id"], ["uip_resolution.id", "uip_resolution.organization_id"], name="fk_uip_decision_event_org"),
        db.CheckConstraint("status IN ('RECORDED','IN_PROGRESS','COMPLETED','SUPERSEDED')", name="ck_uip_decision_status"),
    )
