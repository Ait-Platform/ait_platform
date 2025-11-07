# app/models/visit.py
from app.extensions import db
from sqlalchemy.sql import func

class VisitLog(db.Model):
    __tablename__ = "visit_log"
    id       = db.Column(db.Integer, primary_key=True)
    ts       = db.Column(db.DateTime, server_default=func.now(), index=True)
    path     = db.Column(db.String(255), index=True)
    user_id  = db.Column(db.Integer, index=True, nullable=True)
    ip_hash  = db.Column(db.String(64), index=True)   # hashed for privacy
    ua       = db.Column(db.String(255))
