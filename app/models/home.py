from datetime import datetime

from app.extensions import db


class HomeChapter(db.Model):

    __tablename__ = "home_chapters"

    id = db.Column(db.Integer, primary_key=True)

    chapter_number = db.Column(
        db.Integer,
        nullable=False,
        unique=True
    )

    title = db.Column(
        db.String(255),
        nullable=False
    )

    objective = db.Column(db.Text)

    image_filename = db.Column(
        db.String(255)
    )

    pass_mark = db.Column(
        db.Integer,
        default=100
    )

    questions = db.relationship(
        "HomeQuestion",
        backref="chapter",
        lazy=True
    )

class HomeQuestion(db.Model):

    __tablename__ = "home_questions"

    id = db.Column(
        db.Integer,
        primary_key=True
    )

    chapter_id = db.Column(
        db.Integer,
        db.ForeignKey("home_chapters.id"),
        nullable=False
    )

    question = db.Column(
        db.Text,
        nullable=False
    )

    question_type = db.Column(
        db.String(50),
        nullable=False
    )

    correct_answer = db.Column(
        db.String(255),
        nullable=False
    )

    options = db.relationship(
        "HomeQuestionOption",
        backref="question",
        lazy=True,
        order_by="HomeQuestionOption.sort_order"
    )

class HomeQuestionOption(db.Model):

    __tablename__ = "home_question_options"

    id = db.Column(
        db.Integer,
        primary_key=True
    )

    question_id = db.Column(
        db.Integer,
        db.ForeignKey("home_questions.id"),
        nullable=False
    )

    option_text = db.Column(
        db.String(255),
        nullable=False
    )

    sort_order = db.Column(
        db.Integer,
        nullable=False,
        default=1
    )

class HomeFinalAssessment(db.Model):
    __tablename__ = "home_final_assessments"

    id = db.Column(
        db.Integer,
        primary_key=True
    )

    user_id = db.Column(
        db.Integer,
        db.ForeignKey("user.id"),
        nullable=False
    )

    observation_score = db.Column(
        db.Integer,
        nullable=False,
        default=0
    )

    position_score = db.Column(
        db.Integer,
        nullable=False,
        default=0
    )

    comparison_score = db.Column(
        db.Integer,
        nullable=False,
        default=0
    )

    estimation_score = db.Column(
        db.Integer,
        nullable=False,
        default=0
    )

    measurement_score = db.Column(
        db.Integer,
        nullable=False,
        default=0
    )

    pattern_score = db.Column(
        db.Integer,
        nullable=False,
        default=0
    )

    spatial_score = db.Column(
        db.Integer,
        nullable=False,
        default=0
    )

    logic_score = db.Column(
        db.Integer,
        nullable=False,
        default=0
    )

    mathematics_score = db.Column(
        db.Integer,
        nullable=False,
        default=0
    )

    critical_thinking_score = db.Column(
        db.Integer,
        nullable=False,
        default=0
    )

    overall_score = db.Column(
        db.Integer,
        nullable=False
    )

    passed = db.Column(
        db.Boolean,
        nullable=False
    )

    created_at = db.Column(
        db.DateTime,
        default=datetime.utcnow
    )

class HomeProgress(db.Model):
    __tablename__ = "home_progress"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    chapter_number = db.Column(db.Integer, nullable=False)
    completed_at = db.Column(db.DateTime, default=datetime.utcnow)

    __table_args__ = (
        db.UniqueConstraint('user_id', 'chapter_number', name='_user_home_chapter_uc'),
    )






