from app.extensions import db
from datetime import datetime

class SmsSchool(db.Model):
    __tablename__ = "sms_school"

    id          = db.Column(db.Integer, primary_key=True)
    user_id     = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)

    name        = db.Column(db.String(200), nullable=False)
    emis_no     = db.Column(db.String(50))        # optional
    phase       = db.Column(db.String(50))        # e.g. Primary / Secondary / Combined
    quintile    = db.Column(db.String(10))        # Q1–Q5, or "No-fee"
    learners    = db.Column(db.Integer)           # approx learner count

    logo_path       = db.Column(db.String(255))   # /static/uploads/schools/xxx.png
    letterhead_path = db.Column(db.String(255))   # /static/uploads/schools/xxx.pdf or .png

    created_at  = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at  = db.Column(
        db.DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

class SmsSgbMember(db.Model):
    __tablename__ = "sms_sgb_member"

    id         = db.Column(db.Integer, primary_key=True)
    school_id  = db.Column(db.Integer, db.ForeignKey("sms_school.id"), nullable=False)

    full_name  = db.Column(db.String(120), nullable=False)
    role       = db.Column(db.String(80),  nullable=False)  # Chairperson, Treasurer, Parent, etc.

    start_date = db.Column(db.Date, nullable=True)
    end_date   = db.Column(db.Date, nullable=True)

    is_active  = db.Column(db.Boolean, nullable=False, default=True)

    created_at = db.Column(db.DateTime, server_default=db.func.now())
    updated_at = db.Column(
        db.DateTime,
        server_default=db.func.now(),
        onupdate=db.func.now()
    )

class SmsSgbMeeting(db.Model):
    __tablename__ = "sms_sgb_meeting"

    id          = db.Column(db.Integer, primary_key=True)
    school_id   = db.Column(db.Integer, db.ForeignKey("sms_school.id"), nullable=False)

    meeting_date = db.Column(db.Date, nullable=False)
    term         = db.Column(db.Integer, nullable=True)  # 1–4, optional

    agenda       = db.Column(db.Text, nullable=True)
    minutes      = db.Column(db.Text, nullable=True)

    approved     = db.Column(db.Boolean, nullable=False, default=False)

    created_at   = db.Column(db.DateTime, server_default=db.func.now())
    updated_at   = db.Column(
        db.DateTime,
        server_default=db.func.now(),
        onupdate=db.func.now()
    )

class SmsLearner(db.Model):
    __tablename__ = "sms_learner"

    id = db.Column(db.Integer, primary_key=True)
    school_id = db.Column(db.Integer, db.ForeignKey("sms_school.id"), nullable=False)
    admission_no = db.Column(db.String(50), nullable=True)

    first_name = db.Column(db.String(80), nullable=False)
    last_name  = db.Column(db.String(80), nullable=False)
    grade      = db.Column(db.String(10), nullable=False)   # "R", "1", "7", "12" etc.
    class_code = db.Column(db.String(20), nullable=True)    # "A", "7B", "12C" etc.

    admission_no   = db.Column(db.String(40), nullable=True)
    admission_date = db.Column(db.Date, nullable=True)

    is_active = db.Column(db.Boolean, nullable=False, default=True)

    created_at = db.Column(db.DateTime, nullable=False, server_default=db.func.now())
    updated_at = db.Column(
        db.DateTime,
        nullable=False,
        server_default=db.func.now(),
        onupdate=db.func.now(),
    )

    # no backref here – SmsSchool already has a .learners relationship
    school = db.relationship("SmsSchool")

    # NEW: link table to guardians
    guardian_links = db.relationship(
        "SmsLearnerGuardian",
        back_populates="learner",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        db.UniqueConstraint(
            "school_id", "admission_no",
            name="uq_sms_learner_school_adm",
        ),
    )

class SmsTeacher(db.Model):
    __tablename__ = "sms_teacher"

    id = db.Column(db.Integer, primary_key=True)
    school_id = db.Column(db.Integer, db.ForeignKey("sms_school.id"), nullable=False)

    first_name = db.Column(db.String(80), nullable=False)
    last_name  = db.Column(db.String(80), nullable=False)

    employee_no = db.Column(db.String(40), nullable=True)  # Persal / staff no
    email       = db.Column(db.String(120), nullable=True)
    phone       = db.Column(db.String(40), nullable=True)

    phase       = db.Column(db.String(40), nullable=True)  # Foundation / Intermediate / FET
    subjects    = db.Column(db.String(255), nullable=True) # "Maths, NS, Technology"

    is_active = db.Column(db.Boolean, nullable=False, default=True)

    created_at = db.Column(db.DateTime, nullable=False, server_default=db.func.now())
    updated_at = db.Column(
        db.DateTime,
        nullable=False,
        server_default=db.func.now(),
        onupdate=db.func.now(),
    )

    # no backref here – keep it simple
    school = db.relationship("SmsSchool")

class SmsMgmtTask(db.Model):
    __tablename__ = "sms_mgmt_task"

    id = db.Column(db.Integer, primary_key=True)
    school_id = db.Column(db.Integer, db.ForeignKey("sms_school.id"), nullable=False)

    title       = db.Column(db.String(160), nullable=False)
    owner_name  = db.Column(db.String(80), nullable=True)   # principal / HOD etc.
    due_date    = db.Column(db.Date, nullable=True)
    status      = db.Column(db.String(20), nullable=False, default="open")  # open / in_progress / done
    notes       = db.Column(db.Text, nullable=True)

    is_active = db.Column(db.Boolean, nullable=False, default=True)

    created_at = db.Column(db.DateTime, nullable=False, server_default=db.func.now())
    updated_at = db.Column(
        db.DateTime,
        nullable=False,
        server_default=db.func.now(),
        onupdate=db.func.now(),
    )

    school = db.relationship("SmsSchool")

class SmsGuardian(db.Model):
    __tablename__ = "sms_guardian"

    id           = db.Column(db.Integer, primary_key=True)
    school_id    = db.Column(
        db.Integer,
        db.ForeignKey("sms_school.id", ondelete="CASCADE"),
        nullable=False,
    )

    full_name    = db.Column(db.String(120), nullable=False)
    relationship = db.Column(db.String(40))        # mother / father / guardian
    email        = db.Column(db.String(255))
    phone        = db.Column(db.String(50))

    created_at   = db.Column(db.DateTime, nullable=False, server_default=db.func.now())
    updated_at   = db.Column(
        db.DateTime,
        nullable=False,
        server_default=db.func.now(),
        onupdate=db.func.now(),
    )

    # Relationships
    school        = db.relationship("SmsSchool", backref="guardians")
    learner_links = db.relationship(
        "SmsLearnerGuardian",
        back_populates="guardian",
        cascade="all, delete-orphan",
    )

    def __repr__(self):
        return f"<SmsGuardian id={self.id} name={self.full_name!r}>"

class SmsLearnerGuardian(db.Model):
    __tablename__ = "sms_learner_guardian"

    id          = db.Column(db.Integer, primary_key=True)
    learner_id  = db.Column(
        db.Integer,
        db.ForeignKey("sms_learner.id", ondelete="CASCADE"),
        nullable=False,
    )
    guardian_id = db.Column(
        db.Integer,
        db.ForeignKey("sms_guardian.id", ondelete="CASCADE"),
        nullable=False,
    )
    is_primary  = db.Column(db.Boolean, nullable=False, default=True)

    learner  = db.relationship("SmsLearner", back_populates="guardian_links")
    guardian = db.relationship("SmsGuardian", back_populates="learner_links")

    def __repr__(self):
        return (
            f"<SmsLearnerGuardian learner_id={self.learner_id} "
            f"guardian_id={self.guardian_id} primary={self.is_primary}>"
        )

#-----finance part----

class SmsFinCategory(db.Model):
    __tablename__ = "sms_fin_category"

    id = db.Column(db.Integer, primary_key=True)
    school_id = db.Column(db.Integer, db.ForeignKey("sms_school.id"), nullable=False)
    name = db.Column(db.String(100), nullable=False)  # "School fees", "Hall hire", etc.
    kind = db.Column(db.String(10), nullable=False)   # "income" or "expense"
    is_fee = db.Column(db.Boolean, default=False)

    __table_args__ = (
        db.UniqueConstraint("school_id", "name", name="uq_fin_cat_school_name"),
    )

class SmsFinTxn(db.Model):
    __tablename__ = "sms_fin_txn"

    id = db.Column(db.Integer, primary_key=True)
    school_id = db.Column(db.Integer, db.ForeignKey("sms_school.id"), nullable=False)

    date = db.Column(db.Date, nullable=False)
    amount_cents = db.Column(db.Integer, nullable=False)  # always positive
    direction = db.Column(db.String(10), nullable=False)  # "in" or "out"

    category_id = db.Column(db.Integer, db.ForeignKey("sms_fin_category.id"), nullable=False)
    learner_id = db.Column(db.Integer, db.ForeignKey("sms_learner.id"), nullable=True)

    description = db.Column(db.String(255))
    method = db.Column(db.String(50))
    bank_ref = db.Column(db.String(100))
    bank_matched = db.Column(db.Boolean, default=False)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class SmsFinBankLine(db.Model):
    __tablename__ = "sms_fin_bank_line"

    id = db.Column(db.Integer, primary_key=True)
    school_id = db.Column(db.Integer, db.ForeignKey("sms_school.id"), nullable=False)

    date = db.Column(db.Date, nullable=False)
    amount_cents = db.Column(db.Integer, nullable=False)  # deposit (+) or debit (–)
    description = db.Column(db.String(255))
    reference = db.Column(db.String(100))

    matched_txn_id = db.Column(db.Integer, db.ForeignKey("sms_fin_txn.id"))

class SmsRoleAssignment(db.Model):
    __tablename__ = "sms_role_assignment"
    id = db.Column(db.Integer, primary_key=True)
    school_id = db.Column(db.Integer, db.ForeignKey("sms_school.id"), nullable=False, index=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False, index=True)
    role = db.Column(db.String(32), nullable=False)  # auditor, treasurer, dp, hod, teacher
    is_active = db.Column(db.Boolean, nullable=False, default=True)

    created_at = db.Column(db.DateTime, nullable=False, server_default=db.func.now())

class SmsAccessLog(db.Model):
    __tablename__ = "sms_access_log"
    id = db.Column(db.Integer, primary_key=True)
    school_id = db.Column(db.Integer, nullable=True, index=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False, index=True)

    role_effective = db.Column(db.String(32), nullable=True)
    target = db.Column(db.String(64), nullable=False)
    allowed = db.Column(db.Boolean, nullable=False, default=False)
    deny_reason = db.Column(db.String(64), nullable=True)

    ip = db.Column(db.String(64), nullable=True)
    user_agent = db.Column(db.String(255), nullable=True)

    created_at = db.Column(db.DateTime, nullable=False, server_default=db.func.now())

class SmsApprovedUser(db.Model):
    __tablename__ = "sms_approved_user"

    id = db.Column(db.Integer, primary_key=True)

    school_id = db.Column(db.Integer, db.ForeignKey("sms_school.id"), nullable=False, index=True)
    email = db.Column(db.String(255), nullable=False, index=True)     # store lowercase
    role = db.Column(db.String(50), nullable=False)                   # validated by sms_role (FK)
    active = db.Column(db.Boolean, nullable=False, server_default="true")

    created_at = db.Column(db.DateTime, nullable=False, server_default=db.func.now())
    created_by_user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=True)

    __table_args__ = (
        db.UniqueConstraint("school_id", "email", "role", name="uq_sms_approved_user_school_email_role"),
    )

class SmsRole(db.Model):
    __tablename__ = "sms_role"
    code = db.Column(db.Text, primary_key=True)
    label = db.Column(db.Text, nullable=False)
    sort_order = db.Column(db.Integer, nullable=False, server_default="0")
    active = db.Column(db.Boolean, nullable=False, server_default="true")
