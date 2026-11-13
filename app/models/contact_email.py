"""Contact form quota reservations, independent of other email workflows."""
from app.extensions import db


class ContactEmailAttempt(db.Model):
    __tablename__ = 'contact_email_attempt'
    id = db.Column(db.String(32), primary_key=True)
    ip_hash = db.Column(db.String(64), nullable=False)
    attempted_at = db.Column(db.DateTime(timezone=True), nullable=False)
    __table_args__ = (db.Index('ix_contact_email_ip_time', 'ip_hash', 'attempted_at'),)
