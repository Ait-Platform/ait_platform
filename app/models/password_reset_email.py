"""Password-reset-only IP quotas and account resend reservations."""
from app.extensions import db


class PasswordResetRequest(db.Model):
    __tablename__ = 'password_reset_request'
    id = db.Column(db.String(32), primary_key=True)
    ip_hash = db.Column(db.String(64), nullable=False)
    attempted_at = db.Column(db.DateTime(timezone=True), nullable=False)
    __table_args__ = (db.Index('ix_reset_request_ip_time', 'ip_hash', 'attempted_at'),)


class PasswordResetSend(db.Model):
    __tablename__ = 'password_reset_send'
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), primary_key=True)
    attempt_id = db.Column(db.String(32), nullable=False, unique=True)
    attempted_at = db.Column(db.DateTime(timezone=True), nullable=False)
    status = db.Column(db.String(16), nullable=False)
    __table_args__ = (db.CheckConstraint("status IN ('attempted','accepted','uncertain')", name='ck_reset_send_status'),)
