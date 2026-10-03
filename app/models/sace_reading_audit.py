"""Observational Reading audit history; never used as authority or evidence."""
from datetime import datetime
from app.extensions import db


class ReadingAuditEvent(db.Model):
    __tablename__ = "sace_reading_audit_event"
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id", ondelete="RESTRICT"), nullable=False)
    # Reading-qualified context only; no lifecycle FK or migration dependency.
    engagement_id = db.Column(db.BigInteger, nullable=True)
    action = db.Column(db.String(100), nullable=False, index=True)
    entity_type = db.Column(db.String(100))
    entity_id = db.Column(db.Integer)
    details = db.Column(db.Text)
    metadata_json = db.Column(db.JSON, nullable=False, default=dict)
    ip_address = db.Column(db.String(50))
    # Preserve the existing audit convention: naive UTC, including pledge acceptance.
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)
