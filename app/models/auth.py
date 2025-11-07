# app/models.py
from flask_login import UserMixin
from app.extensions import db
from werkzeug.security import generate_password_hash, check_password_hash
from sqlalchemy import Column, Integer, String, Float, Date, Boolean, ForeignKey, CheckConstraint
from sqlalchemy.orm import relationship
from datetime import date, datetime
from flask_wtf import FlaskForm
from wtforms import StringField, SelectField, SubmitField
from wtforms.validators import DataRequired
from dateutil.relativedelta import relativedelta
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import UniqueConstraint, Index, func

class ApprovedAdmin(db.Model):
    __tablename__ = "auth_approved_admin"
    id    = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(320), unique=True, nullable=False)

class PaymentLog(db.Model):
    __tablename__ = "auth_payment_log"
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)
    program = db.Column(db.String(100), nullable=False)
    amount = db.Column(db.String(20), nullable=False)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)


class User(db.Model, UserMixin):
    __tablename__ = "user"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String)
    email = db.Column(db.String, unique=True, index=True, nullable=False)
    password_hash = db.Column(db.String)
    is_active = db.Column(db.Integer, default=1, nullable=False)

    # keep this so User has enrollments
    enrollments = db.relationship(
        "UserEnrollment",
        backref=db.backref("user", lazy="joined"),
        cascade="all, delete-orphan",
        lazy="select",
    )

class UserEnrollment(db.Model):
    __tablename__ = "user_enrollment"
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    subject_id = db.Column(db.Integer, db.ForeignKey("auth_subject.id"), nullable=False)
    status = db.Column(db.String, nullable=False, default="pending")

    # 🔧 ADD THIS: satisfy AuthSubject.enrollments(back_populates="subject")
    subject = db.relationship(
        "AuthSubject",
        back_populates="enrollments",
        lazy="joined",
        primaryjoin="UserEnrollment.subject_id == AuthSubject.id",
    )

# If you have the AuthSubject model, ensure it matches this:
class AuthSubject(db.Model):
    __tablename__ = "auth_subject"
    id = db.Column(db.Integer, primary_key=True)
    slug = db.Column(db.String)
    name = db.Column(db.String)
    is_active = db.Column(db.Integer, default=1, nullable=False)
    sort_order = db.Column(db.Integer, default=0)

    # must mirror the relationship above
    enrollments = db.relationship(
        "UserEnrollment",
        back_populates="subject",
        lazy="select",
        cascade="all, delete-orphan",
    )

