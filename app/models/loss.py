# app/models/loss.py
from sqlalchemy.orm import synonym, relationship
from sqlalchemy import text  # for SQLite server_default
from app.extensions import db
from werkzeug.security import generate_password_hash, check_password_hash
from sqlalchemy.ext.declarative import declarative_base

# ───────────────────────────────
# LCA RESULT — Stores answers per user per question
# ───────────────────────────────
class LcaQuestion(db.Model):
    __tablename__ = 'lca_question'
    id = db.Column(db.Integer, primary_key=True)
    number = db.Column(db.Integer, nullable=False)
    text = db.Column(db.Text, nullable=False)
    title = db.Column(db.Text, default='Question')
    caption = db.Column(db.Text, default='Press yes or no to continue.')
    buttons = db.Column(db.Text, default='yes;no')

class LcaQuestionPhaseMap(db.Model):
    __tablename__ = 'lca_question_phase_map'
    question_id = db.Column(db.Integer, db.ForeignKey('lca_question.id', ondelete='CASCADE'), primary_key=True)
    answer_type = db.Column(db.String(3), db.CheckConstraint("answer_type IN ('yes','no')"), primary_key=True)
    phase_1 = db.Column(db.Integer, nullable=False, default=0)
    phase_2 = db.Column(db.Integer, nullable=False, default=0)
    phase_3 = db.Column(db.Integer, nullable=False, default=0)
    phase_4 = db.Column(db.Integer, nullable=False, default=0)

# optional, to record what the user answered
class LcaResponse(db.Model):
    __tablename__ = 'lca_response'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)  # <-- add this
    question_id = db.Column(db.Integer, db.ForeignKey('lca_question.id', ondelete='CASCADE'), nullable=False)
    answer = db.Column(db.String(3), nullable=False)  # 'yes' or 'no'
    created_at = db.Column(db.DateTime, server_default=db.func.current_timestamp())


class LcaScoreDefinition(db.Model):
    __tablename__ = 'lca_score_definitions'

    id = db.Column(db.Integer, primary_key=True)
    question_id = db.Column(db.Integer, nullable=False)
    question_text = db.Column(db.Text, nullable=False)
    phase_1 = db.Column(db.Integer, default=0)
    phase_2 = db.Column(db.Integer, default=0)
    phase_3 = db.Column(db.Integer, default=0)
    phase_4 = db.Column(db.Integer, default=0)
    answer_type = db.Column(
        db.String,
        db.CheckConstraint("answer_type IN ('yes', 'no')"),
        nullable=False
    )

class LcaScorecard(db.Model):
    __tablename__ = 'lca_scorecard'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)
    question_id = db.Column(db.Integer, nullable=False)
    answer_type = db.Column(
        db.String,
        db.CheckConstraint("answer_type IN ('yes', 'no')"),
        nullable=False
    )
    phase_1 = db.Column(db.Integer, default=0)
    phase_2 = db.Column(db.Integer, default=0)
    phase_3 = db.Column(db.Integer, default=0)
    phase_4 = db.Column(db.Integer, default=0)

class LcaInstruction(db.Model):
    __tablename__ = 'lca_instruction'

    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String, nullable=False)
    caption = db.Column(db.String)
    content = db.Column(db.Text, nullable=False)

    def __repr__(self):
        return f"<{self.__class__.__name__} #{self.id}>"

class LcaExplain(db.Model):
    __tablename__ = 'lca_explain'

    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String, nullable=False)
    caption = db.Column(db.String)
    content = db.Column(db.Text, nullable=False)

    def __repr__(self):
        return f"<{self.__class__.__name__} #{self.id}>"

class LcaSequence(db.Model):
    __tablename__ = 'lca_sequence'

    id = db.Column(db.Integer, primary_key=True)
    seq_order = db.Column(db.Integer, nullable=False)
    content_type = db.Column(db.String, nullable=False)
    content_id = db.Column(db.Integer)
    optional_label = db.Column(db.String)

    def __repr__(self):
        return f"<{self.__class__.__name__} #{self.id}>"

class LcaPrompt(db.Model):
    __tablename__ = 'lca_prompt'

    prompt_id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.Text, default='Prompt')
    caption = db.Column(db.Text, default='Press Yes or No to continue.')
    text = db.Column(db.Text, nullable=False)
    buttons = db.Column(db.Text, default='yes;no')

class LcaPhase(db.Model):
    __tablename__ = "lca_phase"
    id = db.Column(db.Integer, primary_key=True)              # 1..4
    name = db.Column(db.String(80), nullable=False)           # Impact, Hopelessness, ...
    order_index = db.Column(db.Integer, nullable=False)       # display order
    max_points = db.Column(db.Integer, nullable=False)        # 9, 9, 16, 16
    points_per_item = db.Column(db.Integer, nullable=False)   # 1, 1, 2, 2
    high_is_positive = db.Column(db.Boolean, nullable=False, default=False)
    neutral_line = db.Column(db.Text, nullable=True, default="No notable markers in this phase.")
    active = db.Column(db.Boolean, nullable=False, default=True)

    items = db.relationship(
        "LcaPhaseItem",
        backref="phase",
        order_by="LcaPhaseItem.ordinal.asc()",
        lazy="dynamic",
        cascade="all, delete-orphan",
    )

class LcaPhaseItem(db.Model):
    __tablename__ = "lca_phase_item"
    id = db.Column(db.Integer, primary_key=True)
    phase_id = db.Column(db.Integer, db.ForeignKey("lca_phase.id"), index=True, nullable=False)
    ordinal = db.Column(db.Integer, nullable=False)           # 1..N severity ladder
    body = db.Column(db.Text, nullable=False)
    active = db.Column(db.Boolean, nullable=False, default=True)

class LcaScoringMap(db.Model):
    __tablename__ = "lca_scoring_map"

    # Composite PK: one row per (question_id, answer_type)
    question_id   = db.Column(db.Integer, primary_key=True)
    answer_type   = db.Column(db.String(3), primary_key=True)  # 'yes' or 'no'

    # Phase weights from your grid (0/1)
    phase_1 = db.Column(db.Integer, nullable=False, default=0)
    phase_2 = db.Column(db.Integer, nullable=False, default=0)
    phase_3 = db.Column(db.Integer, nullable=False, default=0)
    phase_4 = db.Column(db.Integer, nullable=False, default=0)

class LcaRun(db.Model):
    __tablename__ = "lca_run"

    id          = db.Column(db.Integer, primary_key=True)
    user_id     = db.Column(db.Integer, nullable=False)
    started_at  = db.Column(db.String,  nullable=False)  # TEXT in DB
    finished_at = db.Column(db.String)                   # TEXT in DB (nullable)
    status      = db.Column(db.String,  default="in_progress")
    subject     = db.Column(db.String)

    # Optional convenience: parseable accessors (safe if strings are ISO-like)
    @property
    def started_display(self): return self.started_at or "—"
    @property
    def finished_display(self): return self.finished_at or ""

class LcaResult(db.Model):
    __tablename__ = "lca_result"

    id         = db.Column(db.Integer, primary_key=True)
    user_id    = db.Column(db.Integer, nullable=False)
    phase_1    = db.Column(db.Integer, nullable=False, default=0)
    phase_2    = db.Column(db.Integer, nullable=False, default=0)
    phase_3    = db.Column(db.Integer, nullable=False, default=0)
    phase_4    = db.Column(db.Integer, nullable=False, default=0)
    total      = db.Column(db.Integer, nullable=False, default=0)
    created_at = db.Column(db.String)                    # DATETIME in SQLite -> TEXT storage
    run_id     = db.Column(db.Integer, db.ForeignKey("lca_run.id"), index=True)
    subject    = db.Column(db.String)

class LcaProgressItem(db.Model):
    __tablename__ = "lca_progress_item"
    id       = db.Column(db.Integer, primary_key=True)
    phase_id = db.Column(db.Integer, db.ForeignKey("lca_phase.id"), nullable=False)
    band     = db.Column(db.String, nullable=False)   # 'low'|'mid'|'high'
    tone     = db.Column(db.String, nullable=False)   # 'positive'|'slightly_positive'|'negative'
    body     = db.Column(db.Text, nullable=False)
    ordinal  = db.Column(db.Integer, nullable=False, default=1)
    active   = db.Column(db.Boolean, nullable=False, default=True)

class LcaPause(db.Model):
    __tablename__ = "lca_pause"
    id      = db.Column(db.Integer, primary_key=True)
    title   = db.Column(db.String, nullable=False)
    caption = db.Column(db.String)
    content = db.Column(db.Text, nullable=False)

class LcaOverallItem(db.Model):
    __tablename__ = "lca_overall_item"

    id       = db.Column(db.Integer, primary_key=True)
    band     = db.Column(db.String(10), nullable=False, index=True)       # 'low'|'mid'|'high'
    tone     = db.Column(db.String(20))                                   # optional
    label    = db.Column(db.String(255), nullable=False)                  # was "title"
    key_need = db.Column(db.Text)                                         # was "caption"
    body     = db.Column(db.Text)                                         # was "content" (HTML ok)
    ordinal  = db.Column(db.Integer, nullable=False, default=0, index=True)
    active   = db.Column(db.Boolean, nullable=False, default=True, index=True)
    type     = db.Column(db.String(20), nullable=False, default="summary", index=True)


