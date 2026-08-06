# app/models.py
from flask_login import UserMixin
from app.extensions import db
from werkzeug.security import generate_password_hash, check_password_hash
from sqlalchemy.orm import relationship
from dateutil.relativedelta import relativedelta
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import UniqueConstraint, Index, func
from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, SubmitField
from wtforms.validators import DataRequired, Email, EqualTo, Length
from sqlalchemy import DateTime
from datetime import datetime

from app.models.culturalfire import CfiRole

class UserEntitlement(db.Model):
    __tablename__ = "user_entitlement"
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    product_slug = db.Column(db.String(255), nullable=False)
    trial_start = db.Column(db.DateTime)
    trial_end = db.Column(db.DateTime)
    paid_until = db.Column(db.DateTime)
    last_active = db.Column(db.DateTime)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class User(db.Model, UserMixin):
    __tablename__ = "user"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String)
    email = db.Column(db.String, unique=True, index=True, nullable=False)
    password_hash = db.Column(db.String)
    is_active = db.Column(db.Integer, default=1, nullable=False)

    # Relationship to UserRole
    user_roles = db.relationship(
        "UserRole", back_populates="user", 
        cascade="all, delete-orphan", lazy="select")

    # keep this so User has enrollments
    enrollments = db.relationship(
        "UserEnrollment",
        backref=db.backref("user", lazy="joined"),
        cascade="all, delete-orphan",
        lazy="select",
    )

    def set_password(self, password: str):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password: str) -> bool:
        return check_password_hash(self.password_hash, password)

    def add_role(self, role):
        # Handle string input by resolving to a CfiRole object
        if isinstance(role, str):
            role_obj = CfiRole.query.filter_by(slug=role).first()
            if not role_obj:
                raise ValueError(f"Role '{role}' not found in cfi_roles table")
            role = role_obj

        # At this point, role is guaranteed to be a CfiRole object
        if not self.has_role(role.slug):
            self.user_roles.append(UserRole(role=role))

    def has_role(self, slug: str) -> bool:
        return any(ur.role.slug == slug for ur in self.user_roles)

    def remove_role(self, role_name: str):
        self.user_roles = [r for r in self.user_roles if r.role.slug != role_name]

class AuthPricing(db.Model):
    __tablename__ = "auth_pricing"
    id = db.Column(db.Integer, primary_key=True)
    subject_id = db.Column(db.Integer, db.ForeignKey("auth_subject.id", ondelete="CASCADE"), nullable=False)
    role = db.Column(db.Text)                               # NULL = any role
    plan = db.Column(db.Text, nullable=False, server_default="enrollment")
    currency = db.Column(db.Text, nullable=False, server_default="ZAR")
    amount_cents = db.Column(db.Integer, nullable=False, server_default="0")  # cents
    is_active = db.Column(db.Integer, nullable=False, server_default="1")     # 1/0
    active_from = db.Column(db.DateTime, nullable=False, server_default=db.func.current_timestamp())
    active_to = db.Column(db.DateTime)                      # NULL = open-ended
    created_at = db.Column(db.DateTime, nullable=False, server_default=db.func.current_timestamp())
    updated_at = db.Column(db.DateTime, nullable=False, server_default=db.func.current_timestamp())
    # paystack_price_id = db.Column(db.Text)

class AuthSubject(db.Model):
    __tablename__ = "auth_subject"

    id = db.Column(db.Integer, primary_key=True)
    slug = db.Column(db.String(255), nullable=False, unique=True)
    name = db.Column(db.String(255))
    is_active = db.Column(db.Integer, default=1, nullable=False)

    sort_order = db.Column(db.Integer)
    trial_days = db.Column(db.Integer, nullable=False, default=0)

    commercial_mode = db.Column(db.String(16), nullable=False, default="free")
    enroll_policy = db.Column(db.String(16), nullable=False, default="auto_enroll")
    processor_default = db.Column(db.String(16), nullable=False, default="paystack")
    requires_price = db.Column(db.Integer, nullable=False, default=0)
    allow_country_pricing = db.Column(db.Integer, nullable=False, default=0)
    mor_mode = db.Column(db.Integer, nullable=False, default=0)

    updated_at = db.Column(db.DateTime, nullable=False, server_default=db.func.now())
    updated_by = db.Column(db.String(255))
    program_type = db.Column(db.String, nullable=False, default="paid") 

    # Relationships
    enrollments = db.relationship("UserEnrollment", back_populates="subject")

    # Commercial details
    paid_days = db.Column(db.Integer, nullable=True)

    # Dashboard display & routing overrides
    is_hidden_on_bridge = db.Column(db.Boolean, default=False, nullable=False)
    parent_subject_id = db.Column(db.Integer, db.ForeignKey('auth_subject.id'), nullable=True)
    bypass_dashboard_endpoint = db.Column(db.String(128), nullable=True)

    # Endpoints
    start_endpoint = db.Column(db.String(128), nullable=True)
    about_endpoint = db.Column(db.String(128), nullable=True)
    pay_endpoint = db.Column(db.String(128), nullable=True)
    trial_expired_endpoint = db.Column(db.String(128), nullable=True)
    admin_start_endpoint = db.Column(db.String(128), nullable=True)
    show_on_welcome = db.Column(db.Boolean, nullable=False, default=False)

    
    def __repr__(self):
        return f"<AuthSubject slug={self.slug} name={self.name}>"

    @property
    def mode(self):
        """
        Derive canonical mode for sieve:
        - free
        - paid
        - trial
        """
        if self.commercial_mode == "free" or not self.requires_price:
            return "free"
        if self.commercial_mode == "paid" and self.trial_days and self.trial_days > 0:
            return "trial"
        if self.commercial_mode == "paid" and (not self.trial_days or self.trial_days == 0):
            return "paid"
        return "unknown"

class UserEnrollment(db.Model):
    __tablename__ = "user_enrollment"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    subject_id = db.Column(db.Integer, db.ForeignKey("auth_subject.id"), nullable=False)

    # Existing fields...

    # 🔹 Back-populates talent submissions
    talent_submissions = db.relationship("CfiTalentSubmission", back_populates="user_enrollment", cascade="all, delete-orphan")

    # Link to billing properties
    billing_properties = db.relationship("BilProperty", backref="enrollment", cascade="all, delete-orphan")

    status = db.Column(db.String, nullable=False, default="pending _payment")

    country_code = db.Column(db.String(2), nullable=True)

    # Simplified pricing fields
    local_currency = db.Column(db.String(10), nullable=False)       # e.g. "EUR"
    local_amount_cents = db.Column(db.Integer, nullable=False)      # e.g. 7560
    zar_amount_cents = db.Column(db.Integer, nullable=False)        # e.g. 150000

    price_id = db.Column(db.Integer, nullable=True)
    price_version = db.Column(db.String(32), nullable=True)
    price_locked_at = db.Column(db.DateTime, nullable=True)

    trial_count = db.Column(db.Integer, nullable=False, default=0)

    expires_at = db.Column(db.DateTime(timezone=True), nullable=True)
    trial_end = db.Column(db.DateTime(timezone=True), nullable=True)
    started_at = db.Column(db.DateTime(timezone=True), nullable=True)

    updated_at = db.Column(db.DateTime(timezone=True), nullable=True)
    subscription_id = db.Column(db.Integer, nullable=True)

    # FK link to biodata
    biodata_id = db.Column(db.Integer, db.ForeignKey("cfi_biodata.id"), nullable=False)

    # Relationship
    biodata = db.relationship("CfiBiodata", back_populates="enrollment", uselist=False)

    # Relationships
    subject = db.relationship(
        "AuthSubject",
        back_populates="enrollments",
        lazy="joined",
        primaryjoin="UserEnrollment.subject_id == AuthSubject.id",
    )

    submission_links = db.relationship(
        "CfiSubmissionParticipant",
        back_populates="enrollment",
        cascade="all, delete-orphan"
    )

    # Add this relationship so back_populates works
    group_memberships = db.relationship("CfiGroupMember", back_populates="enrollment")

    # Canonical back-populates
    talent_submissions = db.relationship(
        "CfiTalentSubmission",
        back_populates="user_enrollment",
        cascade="all, delete-orphan"
    )

    group_members = db.relationship(
        "CfiGroupMember",
        back_populates="enrollment",
        cascade="all, delete-orphan",
        overlaps="group_memberships"
    )
    
class ApprovedAdmin(db.Model):
    __tablename__ = "auth_approved_admin"

    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String, unique=True, nullable=False)
    active = db.Column(db.Boolean, default=True)   # <-- add this

class LoginForm(FlaskForm):
    email = StringField("Email", validators=[DataRequired(), Email()])
    password = PasswordField("Password", validators=[DataRequired()])
    submit = SubmitField("Log In")

class RegisterForm(FlaskForm):
    email = StringField("Email", validators=[DataRequired(), Email()])
    password = PasswordField("Password", validators=[DataRequired(), Length(min=8)])
    confirm_password = PasswordField("Confirm Password", validators=[
        DataRequired(),
        EqualTo("password", message="Passwords must match")
    ])
    submit = SubmitField("Register")    

class AuthSubscription(db.Model):
    __tablename__ = "auth_subscriptions"

    id = db.Column(db.Integer, primary_key=True)

    # Link to user
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)

    # ✅ Link back to originating enrollment for audit clarity
    #enrollment_id = db.Column(db.Integer, db.ForeignKey("user_enrollment.id"), nullable=True)

    # Program and plan details
    program = db.Column(db.String(255), nullable=False)
    plan = db.Column(db.String(50))  # e.g., "trial", "monthly", "annual"

    # Lifecycle status
    status = db.Column(db.String(20), nullable=False)  # active, pending, expired, cancelled

    # Validity window
    valid_from = db.Column(db.DateTime, nullable=False)
    valid_until = db.Column(db.DateTime, nullable=False)

    # Payment tracking
    last_payment_id = db.Column(db.Integer, db.ForeignKey("auth_payment_log.id"))
    payment_confirmed_at = db.Column(db.DateTime)

    # Auto-renewal control
    auto_renew = db.Column(db.Boolean, default=True)

    # Cancellation timestamp
    canceled_at = db.Column(db.DateTime)

    # Audit timestamps
    created_at = db.Column(db.DateTime, default=db.func.now())
    updated_at = db.Column(db.DateTime, default=db.func.now(), onupdate=db.func.now())

class AuthPaymentLog(db.Model):
    __tablename__ = "auth_payment_log"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)
    program = db.Column(db.String(255), nullable=False)

    # Actual debit
    amount = db.Column(db.Numeric(10, 2), nullable=True)   # e.g. 500.00
    currency = db.Column(db.String(10))                    # always "ZAR"

    transaction_id = db.Column(db.String(255))
    status = db.Column(db.String(20))  # success, failed, refunded
    timestamp = db.Column(db.DateTime, default=db.func.now())
    valid_from = db.Column(db.DateTime)
    valid_until = db.Column(db.DateTime)

    enrollment_id = db.Column(
        db.Integer,
        db.ForeignKey("user_enrollment.id"),
        nullable=False
    )

    # ✅ Extra fields for audit clarity
    local_currency = db.Column(db.String(10), nullable=True)       # e.g. "RWF"
    local_amount_cents = db.Column(db.Integer, nullable=True)      # e.g. 42521
    price_id = db.Column(db.Integer, nullable=True)
    country_code = db.Column(db.String(2), nullable=True)
    created_at = db.Column(DateTime, default=datetime.utcnow, nullable=False)

class UserRole(db.Model):
    __tablename__ = "user_roles"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    role_id = db.Column(db.Integer, db.ForeignKey("cfi_roles.id"), nullable=False)

    user = db.relationship("User", back_populates="user_roles")
    role = db.relationship("CfiRole", back_populates="user_roles")

class AuthBaton(db.Model):
    __tablename__ = "auth_baton"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)  # <-- fixed here
    subject_slug = db.Column(db.String(255), nullable=False)
    price_id = db.Column(db.Integer)
    price_version = db.Column(db.Integer)
    price_locked_at = db.Column(db.DateTime)
    local_currency = db.Column(db.String(10))
    local_amount_cents = db.Column(db.Integer)
    zar_amount_cents = db.Column(db.Integer)
    status = db.Column(db.String(50), default="locked")
    created_at = db.Column(db.DateTime, default=db.func.now())

class DirectMessage(db.Model):
    __tablename__ = 'direct_message'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    subject = db.Column(db.String(255), nullable=False)
    message = db.Column(db.Text, nullable=False)
    reply = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=db.func.now())
    is_read = db.Column(db.Boolean, default=False)
    
    user = db.relationship('User')

class UserWalletTransaction(db.Model):
    __tablename__ = "user_wallet_transaction"
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    amount_cents = db.Column(db.Integer, nullable=False) # positive for top-up, negative for spend
    currency = db.Column(db.String(10), default="ZAR")
    description = db.Column(db.String(255))
    created_at = db.Column(db.DateTime, default=db.func.now())
    
class UserUnlockedTopic(db.Model):
    __tablename__ = "user_unlocked_topic"
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    subject_slug = db.Column(db.String(255), nullable=False) # e.g., grade_12_math
    topic_id = db.Column(db.String(255), nullable=False) # e.g., equations_linear
    unlocked_at = db.Column(db.DateTime, default=db.func.now())

class AitTokenWallet(db.Model):
    __tablename__ = "ait_token_wallet"
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False, unique=True)
    balance = db.Column(db.Integer, default=0, nullable=False)
    created_at = db.Column(db.DateTime, default=db.func.now())
    
    transactions = db.relationship("AitTokenTransaction", backref="wallet", lazy=True, cascade="all, delete-orphan")
    
    user = db.relationship("User", backref=db.backref("token_wallet", uselist=False))

class AitTokenTransaction(db.Model):
    __tablename__ = "ait_token_transaction"
    id = db.Column(db.Integer, primary_key=True)
    wallet_id = db.Column(db.Integer, db.ForeignKey('ait_token_wallet.id'), nullable=False)
    amount = db.Column(db.Integer, nullable=False) # positive for top-up, negative for spend
    description = db.Column(db.String(255))
    created_at = db.Column(db.DateTime, default=db.func.now())

class InviteLog(db.Model):
    __tablename__ = "invite_log"
    id = db.Column(db.Integer, primary_key=True)
    sender_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    recipient_phone = db.Column(db.String(50), nullable=False)
    program_slug = db.Column(db.String(50), nullable=False)
    invite_type = db.Column(db.String(50), nullable=False)
    status = db.Column(db.String(20), nullable=False, default="Sent")
    sent_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    
    sender = db.relationship("User", backref="invites_sent")
