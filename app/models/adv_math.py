from app.extensions import db
from datetime import datetime

class AdvMathProgress(db.Model):
    __tablename__ = 'adv_math_progress'
    
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    enrollment_id = db.Column(db.Integer, db.ForeignKey('user_enrollment.id'), nullable=False)
    
    # Mastery statuses: 'not_started', 'in_progress', 'mastered'
    # Legacy hardcoded columns (we keep these to avoid breaking legacy code until fully migrated)
    topic_numbers = db.Column(db.String(20), default='not_started')
    topic_algebra = db.Column(db.String(20), default='not_started')
    topic_functions = db.Column(db.String(20), default='not_started')
    topic_calculus = db.Column(db.String(20), default='not_started')
    topic_probability = db.Column(db.String(20), default='not_started')
    topic_geometry = db.Column(db.String(20), default='not_started')
    
    # Future-proof dynamic mastery tracking
    # Format: {"algebra_sequences": "mastered", "algebra_equations": "in_progress"}
    mastery_data = db.Column(db.JSON, default=dict)
    
    # 0 = not ready, 1 = almost ready, 2 = ready
    readiness_status = db.Column(db.Integer, default=0)
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class AdvMathAssessment(db.Model):
    __tablename__ = 'adv_math_assessment'
    
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    topic_name = db.Column(db.String(50), nullable=False) # e.g. "Numbers", "Algebra"
    
    # AI MCQ attempt data
    question_text = db.Column(db.Text)
    student_answer = db.Column(db.String(255))
    is_correct = db.Column(db.Boolean, default=False)
    ai_feedback = db.Column(db.Text)
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class AdvMathQuestion(db.Model):
    __tablename__ = 'adv_math_question'
    
    id = db.Column(db.Integer, primary_key=True)
    topic_name = db.Column(db.String(50), nullable=False) # e.g. "algebra", "calculus"
    sub_topic = db.Column(db.String(100), nullable=True) # e.g. "sequences_series"
    source_paper = db.Column(db.String(255)) # e.g. "Nov 2023 Paper 1"
    concepts_tested = db.Column(db.String(255)) # Store comma separated subtopics
    
    question_type = db.Column(db.String(50), default="long_form") # "mcq" or "long_form"
    question_text = db.Column(db.Text, nullable=False)
    marks = db.Column(db.Integer, default=0) # Extracted mark allocation for verification

    
    # Options (only populated if question_type == "mcq")
    option_a = db.Column(db.String(255))
    option_b = db.Column(db.String(255))
    option_c = db.Column(db.String(255))
    option_d = db.Column(db.String(255))
    
    correct_answer = db.Column(db.Text, nullable=False) # Stores correct option or final numeric answer
    explanation = db.Column(db.Text)
    marking_memo = db.Column(db.Text) # Stores full markdown/HTML of the solution if needed
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class AdvMathStep(db.Model):
    __tablename__ = 'adv_math_step'
    
    id = db.Column(db.Integer, primary_key=True)
    question_id = db.Column(db.Integer, db.ForeignKey('adv_math_question.id'), nullable=False)
    step_number = db.Column(db.Integer, nullable=False)
    instruction = db.Column(db.String(255)) # e.g. "Factorize the quadratic equation"
    math_content = db.Column(db.Text) # e.g. "\\( (x-5)(x+4) = 0 \\)"
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    question = db.relationship('AdvMathQuestion', backref=db.backref('steps', lazy=True, cascade="all, delete-orphan", order_by='AdvMathStep.step_number'))
