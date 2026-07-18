from datetime import datetime

from app.extensions import db


class TPXPassport(db.Model):
    __tablename__ = "tpx_passport"

    id = db.Column(db.Integer, primary_key=True)

    # Owner
    user_id = db.Column(
        db.Integer,
        db.ForeignKey("user.id"),
        nullable=False,
        index=True
    )

    # Passport
    passport_number = db.Column(db.String(30), unique=True, index=True)

    # Profile
    headline = db.Column(db.String(200))
    about_me = db.Column(db.Text)

    profession = db.Column(db.String(120))
    industry = db.Column(db.String(120))

    employment_status = db.Column(db.String(50))
    years_experience = db.Column(db.Integer)

    desired_salary = db.Column(db.Numeric(12, 2))
    salary_frequency = db.Column(db.String(20))

    availability = db.Column(db.String(50))

    # Visibility
    visibility = db.Column(
        db.String(20),
        default="public"
    )

    searchable = db.Column(
        db.Boolean,
        default=True
    )

    # Passport Score
    passport_strength = db.Column(
        db.Integer,
        default=0
    )

    # Statistics
    profile_views = db.Column(
        db.Integer,
        default=0
    )

    employer_views = db.Column(
        db.Integer,
        default=0
    )

    job_applications = db.Column(
        db.Integer,
        default=0
    )

    interviews = db.Column(
        db.Integer,
        default=0
    )

    offers = db.Column(
        db.Integer,
        default=0
    )

    # Status
    status = db.Column(
        db.String(20),
        default="active"
    )

    verified = db.Column(
        db.Boolean,
        default=False
    )

    # Audit
    created_at = db.Column(
        db.DateTime,
        default=datetime.utcnow
    )

    updated_at = db.Column(
        db.DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow
    )

    # Relationships

    qualifications = db.relationship(
        "TPXQualification",
        backref="passport",
        lazy=True,
        cascade="all, delete-orphan"
    )

    employment = db.relationship(
        "TPXEmployment",
        backref="passport",
        lazy=True,
        cascade="all, delete-orphan"
    )

    skills = db.relationship(
        "TPXSkill",
        backref="passport",
        lazy=True,
        cascade="all, delete-orphan"
    )

    references = db.relationship(
        "TPXReference",
        backref="passport",
        lazy=True,
        cascade="all, delete-orphan"
    )

    documents = db.relationship(
        "TPXDocument",
        backref="passport",
        lazy=True,
        cascade="all, delete-orphan"
    )

    verifications = db.relationship(
        "TPXVerification",
        backref="passport",
        lazy=True,
        cascade="all, delete-orphan"
    )

    applications = db.relationship(
        "TPXApplication",
        backref="passport",
        lazy=True,
        cascade="all, delete-orphan"
    )

    timeline = db.relationship(
        "TPXTimeline",
        backref="passport",
        lazy=True,
        cascade="all, delete-orphan"
    )

    def __repr__(self):
        return f"<TPXPassport {self.id}>"
    
class TPXEmployer(db.Model):
    __tablename__ = "tpx_employer"

    id = db.Column(db.Integer, primary_key=True)

    # Account Owner
    user_id = db.Column(
        db.Integer,
        db.ForeignKey("user.id"),
        nullable=False,
        index=True
    )

    # Company Information
    company_name = db.Column(
        db.String(200),
        nullable=False,
        index=True
    )

    registration_number = db.Column(db.String(100))
    vat_number = db.Column(db.String(50))

    industry = db.Column(db.String(120))
    company_type = db.Column(db.String(100))

    website = db.Column(db.String(255))

    # Contact Details
    contact_person = db.Column(db.String(150))
    contact_position = db.Column(db.String(120))

    email = db.Column(db.String(150))
    phone = db.Column(db.String(50))
    mobile = db.Column(db.String(50))

    # Address
    address1 = db.Column(db.String(255))
    address2 = db.Column(db.String(255))

    suburb = db.Column(db.String(120))
    city = db.Column(db.String(120))
    province = db.Column(db.String(120))

    postal_code = db.Column(db.String(20))
    country_code = db.Column(db.String(2))

    # Company Profile
    company_description = db.Column(db.Text)

    logo_filename = db.Column(db.String(255))

    number_of_employees = db.Column(db.Integer)

    established_year = db.Column(db.Integer)

    # Recruitment

    hiring = db.Column(
        db.Boolean,
        default=True
    )

    verified = db.Column(
        db.Boolean,
        default=False
    )

    verification_level = db.Column(
        db.String(30),
        default="pending"
    )

    # Statistics

    profile_views = db.Column(
        db.Integer,
        default=0
    )

    vacancies_posted = db.Column(
        db.Integer,
        default=0
    )

    successful_hires = db.Column(
        db.Integer,
        default=0
    )

    # Status

    status = db.Column(
        db.String(20),
        default="active"
    )

    # Audit

    created_at = db.Column(
        db.DateTime,
        default=datetime.utcnow
    )

    updated_at = db.Column(
        db.DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow
    )

    # Relationships

    jobs = db.relationship(
        "TPXJob",
        backref="employer",
        lazy=True,
        cascade="all, delete-orphan"
    )

    shortlists = db.relationship(
        "TPXShortlist",
        backref="employer",
        lazy=True,
        cascade="all, delete-orphan"
    )

    def __repr__(self):
        return f"<TPXEmployer {self.company_name}>"
    
class TPXJob(db.Model):
    __tablename__ = "tpx_job"

    id = db.Column(db.Integer, primary_key=True)

    employer_id = db.Column(
        db.Integer,
        db.ForeignKey("tpx_employer.id"),
        nullable=False,
        index=True
    )

    # Job Details

    job_title = db.Column(
        db.String(200),
        nullable=False,
        index=True
    )

    reference_number = db.Column(db.String(50), unique=True)

    department = db.Column(db.String(120))

    job_category = db.Column(db.String(120))

    employment_type = db.Column(db.String(50))
    # Permanent
    # Contract
    # Temporary
    # Casual
    # Internship
    # Learnership
    # Graduate Programme

    workplace_type = db.Column(db.String(50))
    # Onsite
    # Hybrid
    # Remote

    # Description

    summary = db.Column(db.String(500))

    description = db.Column(db.Text)

    responsibilities = db.Column(db.Text)

    requirements = db.Column(db.Text)

    benefits = db.Column(db.Text)

    # Salary

    salary_from = db.Column(db.Numeric(12,2))

    salary_to = db.Column(db.Numeric(12,2))

    salary_frequency = db.Column(db.String(20))

    salary_negotiable = db.Column(
        db.Boolean,
        default=False
    )

    # Location

    country_code = db.Column(db.String(2))

    province = db.Column(db.String(100))

    city = db.Column(db.String(120))

    suburb = db.Column(db.String(120))

    # Experience

    minimum_experience = db.Column(db.Integer)

    education_level = db.Column(db.String(120))

    # Dates

    posted_date = db.Column(
        db.DateTime,
        default=datetime.utcnow
    )

    closing_date = db.Column(db.DateTime)

    # Statistics

    views = db.Column(
        db.Integer,
        default=0
    )

    applications = db.Column(
        db.Integer,
        default=0
    )

    shortlisted = db.Column(
        db.Integer,
        default=0
    )

    # Status

    status = db.Column(
        db.String(20),
        default="Open"
    )
    # Open
    # Closed
    # Filled
    # Cancelled
    # Draft

    featured = db.Column(
        db.Boolean,
        default=False
    )

    active = db.Column(
        db.Boolean,
        default=True
    )

    # Audit

    created_at = db.Column(
        db.DateTime,
        default=datetime.utcnow
    )

    updated_at = db.Column(
        db.DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow
    )

    # Relationships

    job_applications = db.relationship(
        "TPXApplication",
        backref="job",
        lazy=True,
        cascade="all, delete-orphan"
    )

    def __repr__(self):
        return f"<TPXJob {self.job_title}>"
    
class TPXApplication(db.Model):
    __tablename__ = "tpx_application"

    id = db.Column(db.Integer, primary_key=True)

    job_id = db.Column(
        db.Integer,
        db.ForeignKey("tpx_job.id"),
        nullable=False,
        index=True
    )

    passport_id = db.Column(
        db.Integer,
        db.ForeignKey("tpx_passport.id"),
        nullable=False,
        index=True
    )

    # Match Score

    match_score = db.Column(
        db.Integer,
        default=0
    )

    # Application

    cover_letter = db.Column(db.Text)

    expected_salary = db.Column(db.Numeric(12,2))

    available_from = db.Column(db.Date)

    # Status

    status = db.Column(
        db.String(30),
        default="Applied"
    )

    # Applied
    # Viewed
    # Under Review
    # Shortlisted
    # Interview Scheduled
    # Interviewed
    # Assessment
    # Offer Made
    # Offer Accepted
    # Offer Declined
    # Rejected
    # Withdrawn

    employer_notes = db.Column(db.Text)

    candidate_notes = db.Column(db.Text)

    rejection_reason = db.Column(db.Text)

    # Dates

    applied_at = db.Column(
        db.DateTime,
        default=datetime.utcnow
    )

    viewed_at = db.Column(db.DateTime)

    shortlisted_at = db.Column(db.DateTime)

    interview_at = db.Column(db.DateTime)

    decision_at = db.Column(db.DateTime)

    # Flags

    shortlisted = db.Column(
        db.Boolean,
        default=False
    )

    interviewed = db.Column(
        db.Boolean,
        default=False
    )

    offered = db.Column(
        db.Boolean,
        default=False
    )

    hired = db.Column(
        db.Boolean,
        default=False
    )

    withdrawn = db.Column(
        db.Boolean,
        default=False
    )

    active = db.Column(
        db.Boolean,
        default=True
    )

    # Audit

    created_at = db.Column(
        db.DateTime,
        default=datetime.utcnow
    )

    updated_at = db.Column(
        db.DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow
    )

    def __repr__(self):
        return (
            f"<TPXApplication "
            f"Job:{self.job_id} "
            f"Passport:{self.passport_id}>"
        )

class TPXEmployment(db.Model):
    __tablename__ = "tpx_employment"

    id = db.Column(db.Integer, primary_key=True)

    passport_id = db.Column(
        db.Integer,
        db.ForeignKey("tpx_passport.id"),
        nullable=False,
        index=True
    )

    # Employer

    employer_name = db.Column(
        db.String(200),
        nullable=False
    )

    trading_name = db.Column(db.String(200))

    industry = db.Column(db.String(120))

    # Position

    job_title = db.Column(
        db.String(150),
        nullable=False
    )

    department = db.Column(db.String(120))

    employment_type = db.Column(db.String(50))

    employment_level = db.Column(db.String(50))
    # Junior
    # Intermediate
    # Senior
    # Supervisor
    # Manager
    # Executive

    # Employment Period

    start_date = db.Column(db.Date)

    end_date = db.Column(db.Date)

    currently_employed = db.Column(
        db.Boolean,
        default=False
    )

    # Responsibilities

    responsibilities = db.Column(db.Text)

    achievements = db.Column(db.Text)

    # Salary

    starting_salary = db.Column(db.Numeric(12,2))

    ending_salary = db.Column(db.Numeric(12,2))

    salary_frequency = db.Column(db.String(20))

    # Leaving

    reason_for_leaving = db.Column(db.Text)

    eligible_for_rehire = db.Column(db.Boolean)

    # Verification

    supervisor_name = db.Column(db.String(150))

    supervisor_position = db.Column(db.String(150))

    supervisor_email = db.Column(db.String(150))

    supervisor_phone = db.Column(db.String(50))

    employer_verified = db.Column(
        db.Boolean,
        default=False
    )

    verification_status = db.Column(
        db.String(30),
        default="Pending"
    )

    verification_date = db.Column(db.DateTime)

    # Audit

    created_at = db.Column(
        db.DateTime,
        default=datetime.utcnow
    )

    updated_at = db.Column(
        db.DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow
    )

    def __repr__(self):
        return (
            f"<TPXEmployment "
            f"{self.employer_name} - "
            f"{self.job_title}>"
        )

class TPXQualification(db.Model):
    __tablename__ = "tpx_qualification"

    id = db.Column(db.Integer, primary_key=True)

    passport_id = db.Column(
        db.Integer,
        db.ForeignKey("tpx_passport.id"),
        nullable=False,
        index=True
    )

    # Institution

    institution = db.Column(
        db.String(200),
        nullable=False
    )

    campus = db.Column(db.String(150))

    country_code = db.Column(db.String(2))

    # Qualification

    qualification_name = db.Column(
        db.String(200),
        nullable=False
    )

    qualification_type = db.Column(db.String(100))
    # Certificate
    # Diploma
    # Degree
    # Honours
    # Masters
    # Doctorate
    # Short Course
    # Professional Certificate

    nqf_level = db.Column(db.Integer)

    field_of_study = db.Column(db.String(150))

    major = db.Column(db.String(150))

    # Study

    study_mode = db.Column(db.String(50))
    # Full Time
    # Part Time
    # Distance

    start_date = db.Column(db.Date)

    completion_date = db.Column(db.Date)

    completed = db.Column(
        db.Boolean,
        default=True
    )

    # Results

    grade = db.Column(db.String(50))

    distinction = db.Column(
        db.Boolean,
        default=False
    )

    # Documents

    certificate_filename = db.Column(db.String(255))

    transcript_filename = db.Column(db.String(255))

    # Verification

    institution_verified = db.Column(
        db.Boolean,
        default=False
    )

    verification_status = db.Column(
        db.String(30),
        default="Pending"
    )

    verification_reference = db.Column(db.String(120))

    verification_date = db.Column(db.DateTime)

    # Professional Registration

    professional_body = db.Column(db.String(150))

    registration_number = db.Column(db.String(100))

    registration_expiry = db.Column(db.Date)

    # Audit

    created_at = db.Column(
        db.DateTime,
        default=datetime.utcnow
    )

    updated_at = db.Column(
        db.DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow
    )

    def __repr__(self):
        return (
            f"<TPXQualification "
            f"{self.qualification_name}>"
        )

class TPXSkill(db.Model):
    __tablename__ = "tpx_skill"

    id = db.Column(db.Integer, primary_key=True)

    passport_id = db.Column(
        db.Integer,
        db.ForeignKey("tpx_passport.id"),
        nullable=False,
        index=True
    )

    # Skill

    skill_name = db.Column(
        db.String(150),
        nullable=False,
        index=True
    )

    skill_category = db.Column(db.String(100))
    # Technical
    # Professional
    # Soft Skill
    # Leadership
    # Trade
    # IT
    # Language

    # Competency

    proficiency_level = db.Column(db.String(30))
    # Beginner
    # Intermediate
    # Advanced
    # Expert

    years_experience = db.Column(db.Integer)

    last_used_year = db.Column(db.Integer)

    # Evidence

    self_assessed = db.Column(
        db.Boolean,
        default=True
    )

    certificate_filename = db.Column(db.String(255))

    portfolio_url = db.Column(db.String(255))

    # Verification

    verified = db.Column(
        db.Boolean,
        default=False
    )

    verified_by = db.Column(db.String(150))

    verification_date = db.Column(db.DateTime)

    # Employer Rating

    endorsed = db.Column(
        db.Boolean,
        default=False
    )

    endorsements = db.Column(
        db.Integer,
        default=0
    )

    # Audit

    created_at = db.Column(
        db.DateTime,
        default=datetime.utcnow
    )

    updated_at = db.Column(
        db.DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow
    )

    def __repr__(self):
        return f"<TPXSkill {self.skill_name}>"

class TPXVerification(db.Model):
    __tablename__ = "tpx_verification"

    id = db.Column(db.Integer, primary_key=True)

    passport_id = db.Column(
        db.Integer,
        db.ForeignKey("tpx_passport.id"),
        nullable=False,
        index=True
    )

    # Verification

    verification_type = db.Column(
        db.String(100),
        nullable=False,
        index=True
    )
    # Identity
    # Qualification
    # Employment
    # Reference
    # Police Clearance
    # Driver Licence
    # Professional Registration
    # Trade Test
    # Credit Check
    # Criminal Check

    verification_name = db.Column(db.String(200))

    verification_reference = db.Column(db.String(120))

    issuing_authority = db.Column(db.String(200))

    # Status

    status = db.Column(
        db.String(30),
        default="Pending"
    )

    # Pending
    # Submitted
    # In Progress
    # Verified
    # Rejected
    # Expired
    # Revoked

    confidence_score = db.Column(
        db.Integer,
        default=0
    )

    # Verification Dates

    submitted_at = db.Column(db.DateTime)

    verified_at = db.Column(db.DateTime)

    expiry_date = db.Column(db.Date)

    # Verification Agent

    verified_by = db.Column(db.String(150))

    verifier_company = db.Column(db.String(150))

    verifier_notes = db.Column(db.Text)

    # Documents

    document_filename = db.Column(db.String(255))

    supporting_documents = db.Column(db.Text)

    # Public Display

    show_on_passport = db.Column(
        db.Boolean,
        default=True
    )

    badge_colour = db.Column(
        db.String(20),
        default="Grey"
    )

    # Audit

    created_at = db.Column(
        db.DateTime,
        default=datetime.utcnow
    )

    updated_at = db.Column(
        db.DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow
    )

    def __repr__(self):
        return (
            f"<TPXVerification "
            f"{self.verification_type}>"
        )

class TPXReference(db.Model):
    __tablename__ = "tpx_reference"

    id = db.Column(db.Integer, primary_key=True)

    passport_id = db.Column(
        db.Integer,
        db.ForeignKey("tpx_passport.id"),
        nullable=False,
        index=True
    )

    # Reference

    full_name = db.Column(
        db.String(150),
        nullable=False
    )

    company_name = db.Column(db.String(200))

    position = db.Column(db.String(150))

    relationship = db.Column(db.String(100))
    # Manager
    # Supervisor
    # Colleague
    # Lecturer
    # Client
    # Mentor

    # Contact

    email = db.Column(db.String(150))

    phone = db.Column(db.String(50))

    country_code = db.Column(db.String(2))

    # Employment

    worked_from = db.Column(db.Date)

    worked_to = db.Column(db.Date)

    # Permission

    candidate_permission = db.Column(
        db.Boolean,
        default=True
    )

    reference_available = db.Column(
        db.Boolean,
        default=True
    )

    # Verification

    verified = db.Column(
        db.Boolean,
        default=False
    )

    verified_date = db.Column(db.DateTime)

    verified_by = db.Column(db.String(150))

    # Rating

    overall_rating = db.Column(db.Integer)

    communication = db.Column(db.Integer)

    teamwork = db.Column(db.Integer)

    leadership = db.Column(db.Integer)

    reliability = db.Column(db.Integer)

    professionalism = db.Column(db.Integer)

    attendance = db.Column(db.Integer)

    # Recommendation

    would_rehire = db.Column(db.Boolean)

    recommendation = db.Column(db.Text)

    confidential_notes = db.Column(db.Text)

    # Audit

    created_at = db.Column(
        db.DateTime,
        default=datetime.utcnow
    )

    updated_at = db.Column(
        db.DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow
    )

    def __repr__(self):
        return f"<TPXReference {self.full_name}>"

class TPXDocument(db.Model):
    __tablename__ = "tpx_document"

    id = db.Column(db.Integer, primary_key=True)

    passport_id = db.Column(
        db.Integer,
        db.ForeignKey("tpx_passport.id"),
        nullable=False,
        index=True
    )

    # Document

    document_type = db.Column(
        db.String(100),
        nullable=False,
        index=True
    )
    # CV
    # ID
    # Passport
    # Qualification
    # Academic Record
    # Police Clearance
    # Driver Licence
    # Professional Licence
    # Trade Test
    # Portfolio
    # Medical Certificate
    # Employment Contract
    # Reference Letter
    # Other

    title = db.Column(
        db.String(200),
        nullable=False
    )

    description = db.Column(db.Text)

    filename = db.Column(
        db.String(255),
        nullable=False
    )

    original_filename = db.Column(db.String(255))

    file_extension = db.Column(db.String(20))

    mime_type = db.Column(db.String(100))

    file_size = db.Column(db.BigInteger)

    # Version Control

    version = db.Column(
        db.Integer,
        default=1
    )

    current_version = db.Column(
        db.Boolean,
        default=True
    )

    # Verification

    verified = db.Column(
        db.Boolean,
        default=False
    )

    verification_status = db.Column(
        db.String(30),
        default="Pending"
    )

    verified_by = db.Column(db.String(150))

    verified_date = db.Column(db.DateTime)

    # Expiry

    issue_date = db.Column(db.Date)

    expiry_date = db.Column(db.Date)

    expires = db.Column(
        db.Boolean,
        default=False
    )

    # Privacy

    visibility = db.Column(
        db.String(20),
        default="Employer"
    )
    # Private
    # Employer
    # Public

    downloadable = db.Column(
        db.Boolean,
        default=True
    )

    # Statistics

    downloads = db.Column(
        db.Integer,
        default=0
    )

    views = db.Column(
        db.Integer,
        default=0
    )

    # Audit

    uploaded_at = db.Column(
        db.DateTime,
        default=datetime.utcnow
    )

    updated_at = db.Column(
        db.DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow
    )

    def __repr__(self):
        return f"<TPXDocument {self.title}>"

class TPXTimeline(db.Model):
    __tablename__ = "tpx_timeline"

    id = db.Column(db.Integer, primary_key=True)

    passport_id = db.Column(
        db.Integer,
        db.ForeignKey("tpx_passport.id"),
        nullable=False,
        index=True
    )

    # Event

    event_type = db.Column(
        db.String(100),
        nullable=False,
        index=True
    )
    # Birth
    # School
    # Qualification
    # Employment
    # Promotion
    # Award
    # Licence
    # Certification
    # Volunteer
    # Project
    # Publication
    # Patent
    # Military
    # Achievement
    # Membership
    # Interview
    # Employment End
    # Retirement

    title = db.Column(
        db.String(250),
        nullable=False
    )

    organisation = db.Column(db.String(200))

    location = db.Column(db.String(150))

    description = db.Column(db.Text)

    # Timeline

    start_date = db.Column(db.Date)

    end_date = db.Column(db.Date)

    current = db.Column(
        db.Boolean,
        default=False
    )

    # AI

    ai_summary = db.Column(db.Text)

    ai_keywords = db.Column(db.Text)

    ai_score = db.Column(
        db.Integer,
        default=0
    )

    # Display

    icon = db.Column(db.String(50))

    colour = db.Column(db.String(30))

    importance = db.Column(
        db.Integer,
        default=1
    )

    public = db.Column(
        db.Boolean,
        default=True
    )

    # Verification

    verified = db.Column(
        db.Boolean,
        default=False
    )

    verification_source = db.Column(db.String(150))

    # Audit

    created_at = db.Column(
        db.DateTime,
        default=datetime.utcnow
    )

    updated_at = db.Column(
        db.DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow
    )

    def __repr__(self):
        return f"<TPXTimeline {self.title}>"

class TPXCareerDNA(db.Model):
    __tablename__ = "tpx_career_dna"

    id = db.Column(db.Integer, primary_key=True)

    passport_id = db.Column(
        db.Integer,
        db.ForeignKey("tpx_passport.id"),
        nullable=False,
        unique=True,
        index=True
    )

    # AI Analysis

    ai_version = db.Column(db.String(30))

    analysed_at = db.Column(db.DateTime)

    confidence_score = db.Column(
        db.Integer,
        default=0
    )

    # Career Profile

    professional_level = db.Column(db.String(100))

    career_stage = db.Column(db.String(100))
    # Student
    # Graduate
    # Junior
    # Mid Career
    # Senior
    # Executive
    # Entrepreneur
    # Retired

    # AI Scores

    leadership_score = db.Column(db.Integer)

    communication_score = db.Column(db.Integer)

    teamwork_score = db.Column(db.Integer)

    innovation_score = db.Column(db.Integer)

    adaptability_score = db.Column(db.Integer)

    reliability_score = db.Column(db.Integer)

    technical_score = db.Column(db.Integer)

    learning_score = db.Column(db.Integer)

    professionalism_score = db.Column(db.Integer)

    # Employment

    employability_score = db.Column(db.Integer)

    promotion_readiness = db.Column(db.Integer)

    interview_readiness = db.Column(db.Integer)

    # Salary

    salary_potential = db.Column(db.Numeric(12,2))

    salary_growth = db.Column(db.Integer)

    # AI Advice

    strengths = db.Column(db.Text)

    weaknesses = db.Column(db.Text)

    opportunities = db.Column(db.Text)

    risks = db.Column(db.Text)

    recommendations = db.Column(db.Text)

    next_best_action = db.Column(db.Text)

    # Career Direction

    suggested_roles = db.Column(db.Text)

    suggested_industries = db.Column(db.Text)

    suggested_courses = db.Column(db.Text)

    suggested_certifications = db.Column(db.Text)

    # Future

    career_summary = db.Column(db.Text)

    five_year_plan = db.Column(db.Text)

    created_at = db.Column(
        db.DateTime,
        default=datetime.utcnow
    )

    updated_at = db.Column(
        db.DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow
    )

    def __repr__(self):
        return f"<TPXCareerDNA {self.passport_id}>"

class TPXProject(db.Model):
    __tablename__ = "tpx_project"

    id = db.Column(db.Integer, primary_key=True)

    passport_id = db.Column(
        db.Integer,
        db.ForeignKey("tpx_passport.id"),
        nullable=False,
        index=True
    )

    # Project

    project_name = db.Column(
        db.String(200),
        nullable=False
    )

    project_type = db.Column(db.String(100))
    # Commercial
    # Residential
    # Research
    # Software
    # Government
    # NGO
    # Academic

    role = db.Column(db.String(150))

    employer_name = db.Column(db.String(200))

    client_name = db.Column(db.String(200))

    # Description

    summary = db.Column(db.Text)

    responsibilities = db.Column(db.Text)

    achievements = db.Column(db.Text)

    technologies = db.Column(db.Text)

    skills_used = db.Column(db.Text)

    # Dates

    start_date = db.Column(db.Date)

    end_date = db.Column(db.Date)

    current_project = db.Column(
        db.Boolean,
        default=False
    )

    # Results

    budget = db.Column(db.Numeric(14,2))

    team_size = db.Column(db.Integer)

    project_value = db.Column(db.Numeric(14,2))

    # Evidence

    portfolio_link = db.Column(db.String(255))

    github_link = db.Column(db.String(255))

    website_link = db.Column(db.String(255))

    document_link = db.Column(db.String(255))

    image_count = db.Column(
        db.Integer,
        default=0
    )

    video_count = db.Column(
        db.Integer,
        default=0
    )

    # Verification

    verified = db.Column(
        db.Boolean,
        default=False
    )

    verified_by = db.Column(db.String(150))

    verified_date = db.Column(db.DateTime)

    # AI

    ai_summary = db.Column(db.Text)

    ai_score = db.Column(
        db.Integer,
        default=0
    )

    created_at = db.Column(
        db.DateTime,
        default=datetime.utcnow
    )

    updated_at = db.Column(
        db.DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow
    )

    def __repr__(self):
        return f"<TPXProject {self.project_name}>"

class TPXAchievement(db.Model):
    __tablename__ = "tpx_achievement"

    id = db.Column(db.Integer, primary_key=True)

    passport_id = db.Column(
        db.Integer,
        db.ForeignKey("tpx_passport.id"),
        nullable=False,
        index=True
    )

    # Achievement

    achievement_name = db.Column(
        db.String(250),
        nullable=False
    )

    achievement_type = db.Column(
        db.String(100),
        nullable=False
    )
    # Award
    # Promotion
    # Recognition
    # Publication
    # Patent
    # Innovation
    # Competition
    # Scholarship
    # Fellowship
    # Community Service
    # Sports
    # Leadership
    # Sales
    # Safety
    # Performance
    # Other

    category = db.Column(db.String(100))

    # Awarding Body

    organisation = db.Column(db.String(200))

    issued_by = db.Column(db.String(200))

    # Details

    description = db.Column(db.Text)

    achievement_date = db.Column(db.Date)

    location = db.Column(db.String(150))

    # Evidence

    certificate_filename = db.Column(db.String(255))

    media_filename = db.Column(db.String(255))

    website_url = db.Column(db.String(255))

    # Verification

    verified = db.Column(
        db.Boolean,
        default=False
    )

    verified_by = db.Column(db.String(150))

    verification_date = db.Column(db.DateTime)

    # AI

    impact_score = db.Column(
        db.Integer,
        default=0
    )

    visibility = db.Column(
        db.String(20),
        default="Public"
    )

    # Audit

    created_at = db.Column(
        db.DateTime,
        default=datetime.utcnow
    )

    updated_at = db.Column(
        db.DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow
    )

    def __repr__(self):
        return f"<TPXAchievement {self.achievement_name}>"

class TPXOrganisation(db.Model):
    __tablename__ = "tpx_organisation"

    id = db.Column(db.Integer, primary_key=True)

    organisation_name = db.Column(
        db.String(250),
        nullable=False,
        index=True
    )

    trading_name = db.Column(db.String(250))

    organisation_type = db.Column(
        db.String(100),
        nullable=False
    )
    # Employer
    # University
    # College
    # School
    # Government
    # Municipality
    # Recruiter
    # Professional Body
    # Verification Agency
    # NGO
    # Hospital
    # Private Practice
    # Training Provider

    registration_number = db.Column(db.String(120))

    tax_number = db.Column(db.String(120))

    website = db.Column(db.String(255))

    email = db.Column(db.String(150))

    phone = db.Column(db.String(50))

    address = db.Column(db.String(255))

    city = db.Column(db.String(120))

    province = db.Column(db.String(120))

    country_code = db.Column(db.String(2))

    logo_filename = db.Column(db.String(255))

    description = db.Column(db.Text)

    verified = db.Column(
        db.Boolean,
        default=False
    )

    trust_level = db.Column(
        db.String(30),
        default="Pending"
    )

    active = db.Column(
        db.Boolean,
        default=True
    )

    created_at = db.Column(
        db.DateTime,
        default=datetime.utcnow
    )

    updated_at = db.Column(
        db.DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow
    )

    def __repr__(self):
        return f"<TPXOrganisation {self.organisation_name}>"

class TPXCareerPlan(db.Model):
    __tablename__ = "tpx_career_plan"

    id = db.Column(db.Integer, primary_key=True)

    passport_id = db.Column(
        db.Integer,
        db.ForeignKey("tpx_passport.id"),
        nullable=False,
        index=True
    )

    # Career Goal

    current_role = db.Column(db.String(150))

    desired_role = db.Column(db.String(150))

    target_industry = db.Column(db.String(150))

    target_salary = db.Column(db.Numeric(12,2))

    target_country = db.Column(db.String(2))

    target_city = db.Column(db.String(120))

    # Timeframe

    target_date = db.Column(db.Date)

    # AI Analysis

    career_gap = db.Column(db.Text)

    required_skills = db.Column(db.Text)

    recommended_courses = db.Column(db.Text)

    recommended_certifications = db.Column(db.Text)

    recommended_experience = db.Column(db.Text)

    ai_probability = db.Column(
        db.Integer,
        default=0
    )

    # Progress

    progress_percent = db.Column(
        db.Integer,
        default=0
    )

    completed = db.Column(
        db.Boolean,
        default=False
    )

    created_at = db.Column(
        db.DateTime,
        default=datetime.utcnow
    )

    updated_at = db.Column(
        db.DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow
    )

    def __repr__(self):
        return f"<TPXCareerPlan {self.id}>"

class TPXOpportunity(db.Model):
    __tablename__ = "tpx_opportunity"

    id = db.Column(db.Integer, primary_key=True)

    organisation_id = db.Column(
        db.Integer,
        db.ForeignKey("tpx_organisation.id"),
        nullable=False,
        index=True
    )

    # Opportunity

    title = db.Column(
        db.String(250),
        nullable=False
    )

    opportunity_type = db.Column(
        db.String(100),
        nullable=False
    )
    # Employment
    # Internship
    # Learnership
    # Apprenticeship
    # Contract
    # Tender
    # Scholarship
    # Bursary
    # Research
    # Volunteer
    # Consulting
    # Board Position
    # Franchise
    # Partnership

    category = db.Column(db.String(120))

    summary = db.Column(db.Text)

    description = db.Column(db.Text)

    # Location

    country_code = db.Column(db.String(2))

    province = db.Column(db.String(120))

    city = db.Column(db.String(120))

    remote = db.Column(
        db.Boolean,
        default=False
    )

    # Financial

    remuneration = db.Column(db.Numeric(12,2))

    remuneration_type = db.Column(db.String(50))
    # Salary
    # Hourly
    # Contract
    # Grant
    # Commission
    # Volunteer

    negotiable = db.Column(
        db.Boolean,
        default=False
    )

    # Requirements

    minimum_experience = db.Column(db.Integer)

    education_level = db.Column(db.String(120))

    passport_strength_required = db.Column(db.Integer)

    trust_score_required = db.Column(db.Integer)

    reputation_score_required = db.Column(db.Integer)

    # AI

    ai_match_threshold = db.Column(
        db.Integer,
        default=80
    )

    # Dates

    opens_on = db.Column(db.Date)

    closes_on = db.Column(db.Date)

    # Status

    status = db.Column(
        db.String(30),
        default="Open"
    )

    featured = db.Column(
        db.Boolean,
        default=False
    )

    created_at = db.Column(
        db.DateTime,
        default=datetime.utcnow
    )

    updated_at = db.Column(
        db.DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow
    )

    def __repr__(self):
        return f"<TPXOpportunity {self.title}>"

class TPXMarketplace(db.Model):
    __tablename__ = "tpx_marketplace"

    id = db.Column(db.Integer, primary_key=True)

    passport_id = db.Column(
        db.Integer,
        db.ForeignKey("tpx_passport.id"),
        nullable=False,
        index=True
    )

    # Service

    service_title = db.Column(
        db.String(200),
        nullable=False
    )

    category = db.Column(db.String(120))

    summary = db.Column(db.Text)

    description = db.Column(db.Text)

    # Availability

    available = db.Column(
        db.Boolean,
        default=True
    )

    remote_service = db.Column(
        db.Boolean,
        default=False
    )

    onsite_service = db.Column(
        db.Boolean,
        default=True
    )

    country_code = db.Column(db.String(2))

    province = db.Column(db.String(120))

    city = db.Column(db.String(120))

    # Pricing

    pricing_type = db.Column(db.String(30))
    # Hourly
    # Daily
    # Weekly
    # Fixed Price
    # Negotiable

    hourly_rate = db.Column(db.Numeric(12,2))

    minimum_fee = db.Column(db.Numeric(12,2))

    # AI

    ai_recommended_rate = db.Column(db.Numeric(12,2))

    market_demand = db.Column(db.Integer)

    competition_score = db.Column(db.Integer)

    # Ratings

    completed_projects = db.Column(
        db.Integer,
        default=0
    )

    average_rating = db.Column(
        db.Numeric(3,2),
        default=0
    )

    reviews = db.Column(
        db.Integer,
        default=0
    )

    # Visibility

    featured = db.Column(
        db.Boolean,
        default=False
    )

    active = db.Column(
        db.Boolean,
        default=True
    )

    created_at = db.Column(
        db.DateTime,
        default=datetime.utcnow
    )

    updated_at = db.Column(
        db.DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow
    )

    def __repr__(self):
        return f"<TPXMarketplace {self.service_title}>"

class TPXMentor(db.Model):
    __tablename__ = "tpx_mentor"

    id = db.Column(db.Integer, primary_key=True)

    mentor_passport_id = db.Column(
        db.Integer,
        db.ForeignKey("tpx_passport.id"),
        nullable=False,
        index=True
    )

    mentee_passport_id = db.Column(
        db.Integer,
        db.ForeignKey("tpx_passport.id"),
        nullable=False,
        index=True
    )

    mentorship_type = db.Column(db.String(100))
    # Career
    # Leadership
    # Technical
    # Entrepreneurship
    # Academic

    status = db.Column(
        db.String(30),
        default="Active"
    )

    goals = db.Column(db.Text)

    progress = db.Column(
        db.Integer,
        default=0
    )

    meetings = db.Column(
        db.Integer,
        default=0
    )

    mentor_rating = db.Column(db.Numeric(3,2))

    mentee_rating = db.Column(db.Numeric(3,2))

    ai_summary = db.Column(db.Text)

    started_at = db.Column(
        db.DateTime,
        default=datetime.utcnow
    )

    completed_at = db.Column(db.DateTime)

    created_at = db.Column(
        db.DateTime,
        default=datetime.utcnow
    )

    updated_at = db.Column(
        db.DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow
    )

    def __repr__(self):
        return f"<TPXMentor {self.id}>"

class TPXLearning(db.Model):
    __tablename__ = "tpx_learning"

    id = db.Column(db.Integer, primary_key=True)

    passport_id = db.Column(
        db.Integer,
        db.ForeignKey("tpx_passport.id"),
        nullable=False,
        index=True
    )

    # Learning

    course_name = db.Column(
        db.String(250),
        nullable=False
    )

    provider = db.Column(db.String(200))

    learning_type = db.Column(db.String(100))
    # Course
    # Webinar
    # Workshop
    # Conference
    # CPD
    # Certification
    # Degree
    # Diploma
    # Short Course
    # Micro Credential

    category = db.Column(db.String(120))

    # Progress

    status = db.Column(
        db.String(30),
        default="In Progress"
    )

    progress_percent = db.Column(
        db.Integer,
        default=0
    )

    # Dates

    enrolled_date = db.Column(db.Date)

    completed_date = db.Column(db.Date)

    # Results

    score = db.Column(db.Numeric(5,2))

    cpd_points = db.Column(db.Numeric(8,2))

    certificate_filename = db.Column(db.String(255))

    # AI

    ai_importance = db.Column(
        db.Integer,
        default=0
    )

    ai_salary_impact = db.Column(db.Numeric(12,2))

    ai_career_impact = db.Column(db.Integer)

    # Verification

    verified = db.Column(
        db.Boolean,
        default=False
    )

    created_at = db.Column(
        db.DateTime,
        default=datetime.utcnow
    )

    updated_at = db.Column(
        db.DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow
    )

    def __repr__(self):
        return f"<TPXLearning {self.course_name}>"
    
                                                                            






