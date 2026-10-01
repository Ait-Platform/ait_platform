"""Reading-only endorsement processes and appointments; shared identity, no HOME state."""
from datetime import datetime, timezone
from app.extensions import db


def utcnow():
    return datetime.now(timezone.utc)


def lifecycle_constraints(prefix, pending=False):
    open_states = "('active','completion_pending')" if pending else "('active')"
    statuses = "('active','completion_pending','completed','revoked')" if pending else "('active','completed','revoked')"
    return (
        db.CheckConstraint("status IN " + statuses, name=prefix + '_status'),
        db.CheckConstraint("(status IN " + open_states + " AND completed_at IS NULL AND revoked_at IS NULL) OR "
            "(status = 'completed' AND completed_at IS NOT NULL AND revoked_at IS NULL) OR "
            "(status = 'revoked' AND revoked_at IS NOT NULL AND completed_at IS NULL)", name=prefix + '_times'),
        db.CheckConstraint("completed_at IS NULL OR completed_at >= started_at", name=prefix + '_completed'),
        db.CheckConstraint("revoked_at IS NULL OR revoked_at >= started_at", name=prefix + '_revoked'),
        db.CheckConstraint("status IN " + open_states + " OR ended_by_user_id IS NOT NULL", name=prefix + '_actor'),
        db.CheckConstraint("status != 'revoked' OR length(trim(end_reason)) > 0 AND end_reason IS NOT NULL", name=prefix + '_reason'),
    )


class ReadingEngagement(db.Model):
    __tablename__ = 'sace_reading_engagement'
    id = db.Column(db.BigInteger, primary_key=True)
    reference = db.Column(db.String(80), nullable=False, unique=True)
    status = db.Column(db.String(24), nullable=False, default='active', index=True)
    completion_requested_by_user_id = db.Column(db.Integer, db.ForeignKey('user.id', ondelete='RESTRICT'))
    completion_requested_at = db.Column(db.DateTime(timezone=True))
    completion_deadline = db.Column(db.DateTime(timezone=True))
    started_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)
    completed_at = db.Column(db.DateTime(timezone=True))
    revoked_at = db.Column(db.DateTime(timezone=True))
    created_by_user_id = db.Column(db.Integer, db.ForeignKey('user.id', ondelete='RESTRICT'), nullable=False)
    ended_by_user_id = db.Column(db.Integer, db.ForeignKey('user.id', ondelete='RESTRICT'))
    end_reason = db.Column(db.Text)
    provenance_event_id = db.Column(db.Integer, db.ForeignKey('sace_workshop_interactions.id', ondelete='RESTRICT'), nullable=False)
    recorded_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)
    __table_args__ = lifecycle_constraints('ck_reading_engagement', pending=True) + (
        db.CheckConstraint("(completion_requested_by_user_id IS NULL AND completion_requested_at IS NULL AND completion_deadline IS NULL AND status != 'completion_pending') OR (completion_requested_by_user_id IS NOT NULL AND completion_requested_at IS NOT NULL AND completion_deadline IS NOT NULL AND completion_deadline = completion_requested_at + interval '48 hours' AND status IN ('completion_pending','completed','revoked'))", name='ck_reading_engagement_pending'),
    )


class ReadingControllerAppointment(db.Model):
    __tablename__ = 'sace_reading_controller_appointment'
    id = db.Column(db.BigInteger, primary_key=True)
    engagement_id = db.Column(db.BigInteger, db.ForeignKey('sace_reading_engagement.id', ondelete='RESTRICT'), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id', ondelete='RESTRICT'), nullable=False)
    operational_grant_id = db.Column(db.Integer, db.ForeignKey('auth_subject_admin.id', ondelete='SET NULL'))
    grant_id_at_issue = db.Column(db.Integer, nullable=False)
    grant_subject_id = db.Column(db.Integer, db.ForeignKey('auth_subject.id', ondelete='RESTRICT'), nullable=False)
    grant_email_at_issue = db.Column(db.String(255), nullable=False)
    status = db.Column(db.String(16), nullable=False, default='active')
    started_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)
    completed_at = db.Column(db.DateTime(timezone=True))
    revoked_at = db.Column(db.DateTime(timezone=True))
    ended_by_user_id = db.Column(db.Integer, db.ForeignKey('user.id', ondelete='RESTRICT'))
    end_reason = db.Column(db.Text)
    pledge_event_id = db.Column(db.Integer, db.ForeignKey('sace_workshop_interactions.id', ondelete='RESTRICT'), nullable=False)
    provisioning_event_id = db.Column(db.Integer, db.ForeignKey('sace_workshop_interactions.id', ondelete='RESTRICT'), nullable=False, unique=True)
    recorded_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)
    __table_args__ = lifecycle_constraints('ck_reading_appointment') + (
        db.CheckConstraint("status != 'active' OR operational_grant_id IS NOT NULL", name='ck_reading_active_grant'),
        db.CheckConstraint("operational_grant_id IS NULL OR operational_grant_id = grant_id_at_issue", name='ck_reading_exact_grant'),
        db.UniqueConstraint('id', 'engagement_id', name='uq_reading_appointment_engagement'),
        db.Index('uq_reading_active_controller', 'user_id', unique=True, postgresql_where=db.text("status = 'active'")),
        db.Index('uq_reading_active_grant', 'operational_grant_id', unique=True, postgresql_where=db.text("status = 'active'")),
        db.Index('ix_reading_appointment_engagement_status', 'engagement_id', 'status'),
        db.Index('ix_reading_appointment_user_start', 'user_id', 'started_at'),
    )


class ReadingAssignmentContext(db.Model):
    __tablename__ = 'sace_reading_assignment_context'
    invitation_event_id = db.Column(db.Integer, db.ForeignKey('sace_workshop_interactions.id', ondelete='RESTRICT'), primary_key=True)
    engagement_id = db.Column(db.BigInteger, db.ForeignKey('sace_reading_engagement.id', ondelete='RESTRICT'), nullable=False, index=True)
    issuing_appointment_id = db.Column(db.BigInteger, nullable=False, index=True)
    linked_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)
    provenance_event_id = db.Column(db.Integer, db.ForeignKey('sace_workshop_interactions.id', ondelete='RESTRICT'), nullable=False)
    __table_args__ = (db.ForeignKeyConstraint(['issuing_appointment_id', 'engagement_id'],
        ['sace_reading_controller_appointment.id', 'sace_reading_controller_appointment.engagement_id'],
        name='fk_reading_assignment_issuer_engagement', ondelete='RESTRICT'),)
