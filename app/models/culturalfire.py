# app/models/culturalfire.py
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
from flask_wtf import FlaskForm
from wtforms import DateField, StringField, SubmitField
from app.extensions import db
from flask_wtf import FlaskForm
from wtforms.validators import DataRequired, Email, Optional
from sqlalchemy.dialects.postgresql import ENUM

class CfiConfig:
    COST_UPLOAD_VIDEO = 20
    COST_APPLY_JUDGE = 10
    COST_VOTE = 50

class CfiWallet(db.Model):
    __tablename__ = "cfi_wallet"
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False, unique=True)
    balance = db.Column(db.Integer, default=0, nullable=False)
    
    transactions = db.relationship("CfiTokenTransaction", backref="wallet", lazy=True, cascade="all, delete-orphan")

class CfiTokenTransaction(db.Model):
    __tablename__ = "cfi_token_transaction"
    id = db.Column(db.Integer, primary_key=True)
    wallet_id = db.Column(db.Integer, db.ForeignKey("cfi_wallet.id"), nullable=False)
    amount = db.Column(db.Integer, nullable=False)  # Positive for top-ups, negative for actions
    description = db.Column(db.String(255), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class CfiAward(db.Model):
    __tablename__ = "cfi_award"
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    show_id = db.Column(db.Integer, db.ForeignKey("cfi_shows.id"), nullable=True)
    award_type = db.Column(db.String(50), nullable=False, default="Milestone") # 'Milestone' or 'Pageant'
    title = db.Column(db.String(100), nullable=False) # e.g. 'Silver Award', 'Beautiful Eyes'
    description = db.Column(db.String(255), nullable=True)
    earned_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    user = db.relationship("User", backref="cfi_awards")
    show = db.relationship("CfiShow", backref="cfi_show_awards")

class CfiPageantQuestion(db.Model):
    __tablename__ = "cfi_pageant_question"
    id = db.Column(db.Integer, primary_key=True)
    question_text = db.Column(db.Text, nullable=False)

class CfiQuestionAssignment(db.Model):
    __tablename__ = "cfi_question_assignment"
    id = db.Column(db.Integer, primary_key=True)
    show_id = db.Column(db.Integer, db.ForeignKey("cfi_shows.id"), nullable=False)
    segment_item_id = db.Column(db.Integer, db.ForeignKey("cfi_segment_items.id"), nullable=False)
    question_id = db.Column(db.Integer, db.ForeignKey("cfi_pageant_question.id"), nullable=False)
    
    show = db.relationship("CfiShow", backref="assigned_questions")
    segment_item = db.relationship("CfiSegmentItem", backref="assigned_question")
    question = db.relationship("CfiPageantQuestion")

class CfiShowAd(db.Model):
    __tablename__ = "cfi_show_ad"
    id = db.Column(db.Integer, primary_key=True)
    show_id = db.Column(db.Integer, db.ForeignKey("cfi_shows.id"), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    video_url = db.Column(db.String(255), nullable=False)
    position_index = db.Column(db.Integer, nullable=False, default=0) # index of act after which ad plays
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    show = db.relationship("CfiShow", backref="ads")
    user = db.relationship("User", backref="cfi_ads")


class CfiTalentCategory(db.Model):
    __tablename__ = "cfi_talent_categories"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False, unique=True)
    slug = db.Column(db.String(100), nullable=False, unique=True)
    active = db.Column(db.Boolean, default=True)

    items = db.relationship(
        "CfiTalentCategoryItem",
        back_populates="category",
        cascade="all, delete-orphan"
    )

class CfiTalentCategoryItem(db.Model):
    __tablename__ = "cfi_talent_category_items"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False, unique=True)

    category_id = db.Column(
        db.Integer,
        db.ForeignKey("cfi_talent_categories.id"),
        nullable=False
    )

    category = db.relationship(
        "CfiTalentCategory",
        back_populates="items"
    )
# Existing relationships…
    submissions = db.relationship("CfiTalentSubmission", back_populates="category_item")

    # 🔹 Add this to match back_populates in CfiTalentStyle
    styles = db.relationship("CfiTalentStyle", back_populates="category_item", cascade="all, delete-orphan")

class UserProgram(db.Model):
    __tablename__ = "user_program"

    id = db.Column(db.Integer, primary_key=True)

    user_id = db.Column(db.Integer, nullable=False)
    subject_id = db.Column(db.Integer, nullable=False)

    role = db.Column(db.String(50))  # participant, parent, judge

    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class CfiBiodata(db.Model):
    __tablename__ = "cfi_biodata"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)

    # Identity
    full_name = db.Column(db.String(255), nullable=False)
    id_number = db.Column(db.String(50), nullable=False)
    dob = db.Column(db.Date, nullable=False)
    gender = db.Column(db.String(20))
    city = db.Column(db.String(100))   # critical for grouping shows
    province = db.Column(db.String(100))

    # Contact
    phone = db.Column(db.String(20), nullable=False)

    address_line = db.Column(db.String(255))
    
    # Minor Consent Tracking
    parent_email = db.Column(db.String(150), nullable=True)
    parent_consent_status = db.Column(db.String(50), nullable=True) # pending, granted
    parent_consent_token = db.Column(db.String(100), nullable=True)

    # Added dynamically: age, grade, school_name, cell_number, nullable=True)
    grade = db.Column(db.String(50), nullable=True)
    school = db.Column(db.String(255), nullable=True)
    
    # Emergency / Next of Kin
    emergency_contact_name = db.Column(db.String(255))
    emergency_contact_phone = db.Column(db.String(20))
    next_of_kin_name = db.Column(db.String(255))
    next_of_kin_relationship = db.Column(db.String(100))

    # Employment / Education
    employer_name = db.Column(db.String(255))
    employer_address = db.Column(db.String(255))
    employer_details = db.Column(db.String(255), nullable=True)
    occupation = db.Column(db.String(100))
    highest_qualification = db.Column(db.String(100))

    # Program Commitment

    pledge_agreed = db.Column(db.Boolean, default=False, nullable=False)
    pledge_date = db.Column(db.DateTime)
    signature = db.Column(db.Text)
    
    role = db.Column(db.String(50), nullable=True)

    # Optional / Extras
    notes = db.Column(db.Text)

    # … other biodata fields …

    enrollment = db.relationship("UserEnrollment", back_populates="biodata", uselist=False)

class CfiSubmissionParticipant(db.Model):
    __tablename__ = "cfi_submission_participants"

    id = db.Column(db.Integer, primary_key=True)
    submission_id = db.Column(
        db.Integer,
        db.ForeignKey("cfi_talent_submission.id", ondelete="CASCADE"),
        nullable=False
    )
    enrollment_id = db.Column(
        db.Integer,
        db.ForeignKey("user_enrollment.id", ondelete="CASCADE"),
        nullable=False
    )
    role = db.Column(db.String(50), default="participant")

    # Relationships
    submission = db.relationship("CfiTalentSubmission", back_populates="participants")
    enrollment = db.relationship("UserEnrollment", back_populates="submission_links")

# 🔹 Group Model
class CfiGroup(db.Model):
    __tablename__ = "cfi_groups"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False)
    leader_id = db.Column(db.Integer, db.ForeignKey("user_enrollment.id"), nullable=False)

    submission_id = db.Column(db.Integer, db.ForeignKey("cfi_talent_submission.id"), nullable=True)

    created_at = db.Column(db.DateTime, default=db.func.now())

    # One-to-one: group anchored to a submission
    submission = db.relationship(
        "CfiTalentSubmission",
        back_populates="group",
        foreign_keys=[submission_id],
        uselist=False
    )

    leader = db.relationship("UserEnrollment", foreign_keys=[leader_id])
    group_members = db.relationship("CfiGroupMember", back_populates="group", cascade="all, delete-orphan")

    @property
    def members(self):
        return [gm.enrollment for gm in self.group_members]

# 🔹 Group Member Model
class CfiGroupMember(db.Model):
    __tablename__ = "cfi_group_members"

    id = db.Column(db.Integer, primary_key=True)
    group_id = db.Column(db.Integer, db.ForeignKey("cfi_groups.id"), nullable=False)
    enrollment_id = db.Column(db.Integer, db.ForeignKey("user_enrollment.id"), nullable=False)
    submission_id = db.Column(db.Integer, db.ForeignKey("cfi_talent_submission.id"), nullable=True)

    #group = db.relationship("CfiGroup", back_populates="members")
    enrollment = db.relationship("UserEnrollment", back_populates="group_members")
    submission = db.relationship("CfiTalentSubmission", back_populates="group_members")
    group = db.relationship("CfiGroup", back_populates="group_members")

class CfiGroupInvitation(db.Model):
    __tablename__ = "cfi_group_invitations"
    id = db.Column(db.Integer, primary_key=True)
    group_id = db.Column(db.Integer, db.ForeignKey("cfi_groups.id"), nullable=False)
    enrollment_id = db.Column(db.Integer, db.ForeignKey("user_enrollment.id"), nullable=False)
    status = db.Column(db.String(20), default="pending") # pending, rejected
    created_at = db.Column(db.DateTime, default=db.func.now())
    
    group = db.relationship("CfiGroup")
    enrollment = db.relationship("UserEnrollment")

class CfiTalentSubmission(db.Model):
    __tablename__ = "cfi_talent_submission"

    id = db.Column(db.Integer, primary_key=True)

    # Canonical FK to enrollment
    user_enrollment_id = db.Column(
        db.Integer,
        db.ForeignKey("user_enrollment.id"),
        nullable=False
    )

    user_enrollment = db.relationship(
        "UserEnrollment",
        back_populates="talent_submissions",
        foreign_keys=[user_enrollment_id]
    )

    # Existing DB fields
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"))
    subject_id = db.Column(db.Integer, db.ForeignKey("auth_subject.id"))

    media_url = db.Column(db.String)
    video_url = db.Column(db.String, nullable=True)
    #status = db.Column(db.String, default="submitted")
    status = db.Column(db.String(20), default="pending")

    created_at = db.Column(db.DateTime, default=db.func.now())
    participants = db.relationship("CfiSubmissionParticipant", back_populates="submission")

    group = db.relationship(
        "CfiGroup",
        back_populates="submission",
        uselist=False
    )


    talent_name = db.Column(db.String)
    custom_talent = db.Column(db.String)

    # FK to category item
    category_item_id = db.Column(
        db.Integer,
        db.ForeignKey("cfi_talent_category_items.id"),
        nullable=True
    )

    category_item = db.relationship(
        "CfiTalentCategoryItem",
        back_populates="submissions",
        foreign_keys=[category_item_id]
    )

    permission_granted = db.Column(db.Boolean, default=False)

    style_id = db.Column(
        db.Integer,
        db.ForeignKey("cfi_talent_styles.id")
    )

    style = db.relationship(
        "CfiTalentStyle",
        back_populates="submissions"
    )

    context_id = db.Column(
        db.Integer,
        db.ForeignKey("cfi_talent_context.id")
    )

    context = db.relationship(
        "CfiTalentContext",
        back_populates="submissions"
    )

    sponsor_id = db.Column(db.Integer)
    supporter_id = db.Column(db.Integer)

    # Relationships
    group_members = db.relationship("CfiGroupMember", back_populates="submission")

    files = db.relationship(
        "CfiTalentFile",
        back_populates="submission",
        cascade="all, delete-orphan"
    )

    show_id = db.Column(db.Integer, db.ForeignKey("cfi_shows.id"), nullable=False)

    show = db.relationship("CfiShow", back_populates="submissions")

    segment_id = db.Column(db.Integer, db.ForeignKey("cfi_pageant_segments.id"), nullable=False)

    segment = db.relationship("CfiPageantSegment", backref="submissions")

class CfiTalentFile(db.Model):
    __tablename__ = "cfi_talent_files"

    id = db.Column(db.Integer, primary_key=True)
    submission_id = db.Column(db.Integer, db.ForeignKey("cfi_talent_submission.id"), nullable=False)
    filename = db.Column(db.String(255), nullable=False)

    submission = db.relationship("CfiTalentSubmission", back_populates="files")

class CfiParent(db.Model):
    __tablename__ = "cfi_parent"

    id = db.Column(db.Integer, primary_key=True)
    parent_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    child_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    relationship = db.Column(db.String(50))
    consent = db.Column(db.Boolean, default=False)
    permission_granted = db.Column(db.Boolean, default=False)

    # Optional: link to shows if needed
    show_id = db.Column(db.Integer, db.ForeignKey("cfi_shows.id"), nullable=True)
    show = db.relationship("CfiShow", back_populates="parents")

class CfiSponsorship(db.Model):
    __tablename__ = "cfi_sponsorships"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    participant_id = db.Column(db.Integer, db.ForeignKey("user_enrollment.id"), nullable=True)
    sponsor_item_id = db.Column(db.Integer, db.ForeignKey("cfi_sponsor_item.id"), nullable=False)
    show_id = db.Column(db.Integer, db.ForeignKey("cfi_shows.id"), nullable=True)

    # New column
    talent_submission_id = db.Column(db.Integer, db.ForeignKey("cfi_talent_submission.id"), nullable=True)

    sponsor_item = db.relationship("CfiSponsorItem", backref="sponsorships")
    user = db.relationship("User", backref="sponsorships")
    participant = db.relationship("UserEnrollment", backref="sponsorships")
    #show = db.relationship("CfiShow", backref="sponsorships")
    talent_submission = db.relationship("CfiTalentSubmission", backref="sponsorships")

    amount = db.Column(db.Integer, nullable=False)
    created_at = db.Column(db.DateTime, default=db.func.now())

class CfiSponsorItem(db.Model):
    __tablename__ = "cfi_sponsor_item"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    amount = db.Column(db.Integer, nullable=False)
    description = db.Column(db.String(255), nullable=True)

class CfiSupporter(db.Model):
    __tablename__ = "cfi_supporters"

    id = db.Column(db.Integer, primary_key=True)

    # supporter account
    user_id = db.Column(
        db.Integer,
        db.ForeignKey('user.id'),   # must match __tablename__ of User
        nullable=False
    )

    # participant being supported
    participant_id = db.Column(db.Integer, db.ForeignKey("user_enrollment.id"), nullable=True)
    participant = db.relationship(
        "UserEnrollment",
        foreign_keys=[participant_id],
        backref="supporters"
    )


    show_id = db.Column(db.Integer, db.ForeignKey("cfi_shows.id"))  # <-- new link
    #amount = db.Column(db.Numeric(10, 2))
    #note = db.Column(db.String(255))
    start_date = db.Column(db.DateTime, default=datetime.utcnow)
    duration_months = db.Column(db.Integer, default=6)

    # relationships
    supporter = db.relationship(
        "User",
        foreign_keys=[user_id],
        backref="supporter_links"
    )

    referee_id = db.Column(db.Integer, db.ForeignKey("user_enrollment.id"), nullable=False)
    referee = db.relationship(
        "UserEnrollment",
        foreign_keys=[referee_id],
        backref="referee_links"
    )

        # Add this field
    supporter_type = db.Column(db.String(50), nullable=False)

class CfiRole(db.Model):
    __tablename__ = "cfi_roles"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False, unique=True)  # participant, sponsor, supporter, etc.
    slug = db.Column(db.String(100), nullable=False, unique=True)  # URL-safe identifier
    description = db.Column(db.Text)
    active = db.Column(db.Boolean, default=True)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    user_roles = db.relationship("UserRole", back_populates="role")

    def __repr__(self):
        return f"<CfiRole {self.name}>"

class CfiTalentStyle(db.Model):
    __tablename__ = "cfi_talent_styles"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)

    # Link each style to a category item (e.g. Singing, Dance, Poetry)
    category_item_id = db.Column(
        db.Integer,
        db.ForeignKey("cfi_talent_category_items.id"),
        nullable=False
    )

    category_item = db.relationship("CfiTalentCategoryItem", back_populates="styles")

    # Back-populate submissions that chose this style
    submissions = db.relationship("CfiTalentSubmission", back_populates="style")

class CfiTalentContext(db.Model):
    __tablename__ = "cfi_talent_context"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)

    submissions = db.relationship(
        "CfiTalentSubmission",
        back_populates="context"
    )

class CfiShow(db.Model):
    __tablename__ = "cfi_shows"

    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(120), nullable=False)
    description = db.Column(db.Text, nullable=True)
    start_date = db.Column(db.Date, nullable=False)
    end_date = db.Column(db.Date, nullable=True)
    location = db.Column(db.String(255), nullable=False)
    created_at = db.Column(db.DateTime, default=db.func.now())
    updated_at = db.Column(db.DateTime, default=db.func.now(), onupdate=db.func.now())
    status = db.Column(db.String(50), default="active")  # "active", "archived"

    category_item_id = db.Column(db.Integer, db.ForeignKey("cfi_talent_category_items.id"), nullable=False)    
    category_item = db.relationship("CfiTalentCategoryItem", lazy="joined")
    
    sponsorships = db.relationship("CfiSponsorship", backref="show", lazy=True)
    supporters = db.relationship("CfiSupporter", backref="show", lazy=True)
    submissions = db.relationship("CfiTalentSubmission", back_populates="show", lazy=True)
    parents = db.relationship("CfiParent", back_populates="show", lazy=True)
                              
    def __repr__(self):
        return f"<CfiShow {self.title} ({self.start_date} - {self.end_date})>"

class CfiJudgeAssignment(db.Model):
    __tablename__ = "cfi_judge_assignment"
    id = db.Column(db.Integer, primary_key=True)
    judge_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    show_id = db.Column(db.Integer, db.ForeignKey('cfi_shows.id'))
    role = db.Column(db.String(50))  # "parent", "sponsor", "supporter", "paid_judge"
    assigned_at = db.Column(db.DateTime, default=datetime.utcnow)
    enrollment_id = db.Column(db.Integer, db.ForeignKey('user_enrollment.id'), nullable=True)
    
    show = db.relationship("CfiShow", backref="judge_assignments")
    judge = db.relationship("User", backref="judge_assignments")

class CfiSegmentItem(db.Model):
    __tablename__ = "cfi_segment_items"

    id = db.Column(db.Integer, primary_key=True)

    # Link to enrollment and show
    enrollment_id = db.Column(db.Integer, db.ForeignKey("user_enrollment.id"), nullable=False)
    show_id = db.Column(db.Integer, db.ForeignKey("cfi_shows.id"), nullable=False)

    # Segment metadata
    segment_type = db.Column(db.String(50), nullable=False)   # e.g. ramp_walk, talent, qna
    title = db.Column(db.String(100), nullable=False)         # Display name like "Ramp Walk"
    status = db.Column(db.String(20), default="pending")      # pending, uploaded, completed

    # File reference
    video_url = db.Column(db.String(255))                     # path to uploaded video

    # Audit fields
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    enrollment = db.relationship("UserEnrollment", backref="segment_items")
    show = db.relationship("CfiShow", backref="segment_items")

    def __repr__(self):
        return f"<CfiSegmentItem {self.segment_type} status={self.status}>"

class CfiPageantSegment(db.Model):
    __tablename__ = "cfi_pageant_segments"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)   # e.g. "Ramp Walk", "Intro", "Q&A"

    def __repr__(self):
        return f"<CfiPageantSegment {self.name}>"

class CfiShowcaseVote(db.Model):
    __tablename__ = 'cfi_showcase_votes'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    submission_id = db.Column(db.Integer, db.ForeignKey('cfi_talent_submission.id'), nullable=True)
    segment_item_id = db.Column(db.Integer, db.ForeignKey('cfi_segment_items.id'), nullable=True)
    score = db.Column(db.Integer, default=0, nullable=False)
    created_at = db.Column(db.DateTime, default=db.func.now())
    
    submission = db.relationship('CfiTalentSubmission')
    segment_item = db.relationship('CfiSegmentItem')

class CfiJudgeScore(db.Model):
    __tablename__ = 'cfi_judge_scores'
    id = db.Column(db.Integer, primary_key=True)
    vote_id = db.Column(db.Integer, db.ForeignKey('cfi_showcase_votes.id'), nullable=False)
    criterion_name = db.Column(db.String(100), nullable=False)
    score = db.Column(db.Integer, default=0, nullable=False)
    
    vote = db.relationship('CfiShowcaseVote', backref=db.backref('criteria_scores', lazy=True, cascade='all, delete-orphan'))

class CfiMcVote(db.Model):
    __tablename__ = 'cfi_mc_votes'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    show_id = db.Column(db.Integer, db.ForeignKey('cfi_shows.id'), nullable=False)
    mc_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    score = db.Column(db.Integer, default=0, nullable=False)
    created_at = db.Column(db.DateTime, default=db.func.now())
    
    show = db.relationship('CfiShow')
    mc = db.relationship('User', foreign_keys=[mc_id])

class CfiMcScore(db.Model):
    __tablename__ = 'cfi_mc_scores'
    id = db.Column(db.Integer, primary_key=True)
    vote_id = db.Column(db.Integer, db.ForeignKey('cfi_mc_votes.id'), nullable=False)
    criterion_name = db.Column(db.String(100), nullable=False)
    score = db.Column(db.Integer, default=0, nullable=False)
    
    vote = db.relationship('CfiMcVote', backref=db.backref('criteria_scores', lazy=True, cascade='all, delete-orphan'))
class CfiMcAssignment(db.Model):
    __tablename__ = "cfi_mc_assignment"
    id = db.Column(db.Integer, primary_key=True)
    mc_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    show_id = db.Column(db.Integer, db.ForeignKey('cfi_shows.id'))
    assigned_at = db.Column(db.DateTime, default=datetime.utcnow)
    enrollment_id = db.Column(db.Integer, db.ForeignKey('user_enrollment.id'), nullable=True)
    pageant_segment_id = db.Column(db.Integer, db.ForeignKey('cfi_pageant_segments.id'), nullable=True)
    
    show = db.relationship("CfiShow", backref="mc_assignments")
    mc = db.relationship("User", backref="mc_assignments")
    pageant_segment = db.relationship("CfiPageantSegment")

class CfiMcRecording(db.Model):
    __tablename__ = 'cfi_mc_recordings'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    show_id = db.Column(db.Integer, db.ForeignKey('cfi_shows.id'), nullable=True)
    recording_type = db.Column(db.String(50), nullable=True) # 'show_intro', 'act_intro', 'show_outro'
    submission_id = db.Column(db.Integer, db.ForeignKey('cfi_talent_submission.id'), nullable=True)
    segment_item_id = db.Column(db.Integer, db.ForeignKey('cfi_segment_items.id'), nullable=True)
    media_url = db.Column(db.String(500), nullable=False)
    created_at = db.Column(db.DateTime, default=db.func.now())

class CfiShowAccess(db.Model):
    __tablename__ = "cfi_show_access"
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    show_id = db.Column(db.Integer, db.ForeignKey('cfi_shows.id'), nullable=False)
    tokens_paid = db.Column(db.Integer, nullable=False, default=10)
    created_at = db.Column(db.DateTime, default=db.func.now())
    
    user = db.relationship("User", backref="cfi_show_access")
    show = db.relationship("CfiShow", backref="access_records")

class CfiTokenTariff(db.Model):
    __tablename__ = "cfi_token_tariff"
    id = db.Column(db.Integer, primary_key=True)
    action_name = db.Column(db.String(100), nullable=False, unique=True)
    base_token_cost = db.Column(db.Integer, nullable=False, default=10)
    created_at = db.Column(db.DateTime, default=db.func.now())

class CfiPrivateShowGroup(db.Model):
    __tablename__ = 'cfi_private_show_groups'
    id = db.Column(db.Integer, primary_key=True)
    show_id = db.Column(db.Integer, db.ForeignKey('cfi_shows.id'), nullable=False, unique=True)
    group_id = db.Column(db.Integer, db.ForeignKey('cfi_groups.id'), nullable=False)
    created_at = db.Column(db.DateTime, default=db.func.now())
    updated_at = db.Column(db.DateTime, default=db.func.now(), onupdate=db.func.now())


class CfiVideoFlag(db.Model):
    __tablename__ = 'cfi_video_flags'
    id = db.Column(db.Integer, primary_key=True)
    video_id = db.Column(db.String(100), nullable=False)
    reporter_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    created_at = db.Column(db.DateTime, default=db.func.now())

