# models_billing.py

from flask_login import UserMixin
from app.extensions import db
from werkzeug.security import generate_password_hash, check_password_hash

  
class RdpLesson(db.Model):
    __tablename__ = 'rdp_lesson'

    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(100), nullable=False)
    video_filename = db.Column(db.String(100), nullable=False)
    caption = db.Column(db.String(255))
    order = db.Column(db.Integer, nullable=False)
    video_filename = db.Column(db.String, nullable=False, default="", server_default="")

class RdpLearnerProgress(db.Model):
    __tablename__ = 'rdp_learner_progress'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"))
    lesson_id = db.Column(db.Integer, db.ForeignKey("rdp_lesson.id"))
    completed = db.Column(db.Boolean, default=False)

    user = db.relationship("User", backref="rdp_progress")
    lesson = db.relationship("RdpLesson", backref="progress_entries")

