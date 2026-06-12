from sqlalchemy import select, func
from app.extensions import db
from app.models.loss import LcaRun, LcaResult

def build_report_ctx(run_id: int) -> dict:
    run = db.session.get(LcaRun, run_id)

    row = db.session.execute(
        select(
            func.sum(LcaResult.phase_1).label("p1"),
            func.sum(LcaResult.phase_2).label("p2"),
            func.sum(LcaResult.phase_3).label("p3"),
            func.sum(LcaResult.phase_4).label("p4"),
            func.sum(LcaResult.total).label("tot"),
            func.count(LcaResult.id).label("rows"),
            func.min(LcaResult.created_at).label("first_at"),
            func.max(LcaResult.created_at).label("last_at"),
        ).where(LcaResult.run_id == run_id)
    ).one()

    status_display   = (run.status if run and getattr(run, "status", None) else "legacy")
    started_display  = (run.started_at if run and getattr(run, "started_at", None) else (row.first_at or "—"))
    finished_display = (run.finished_at if run and getattr(run, "finished_at", None) else "")

    return {
        "run": run,
        "run_id": run_id,
        "res_summary": row,
        "status_display": status_display,
        "started_display": started_display,
        "finished_display": finished_display,
    }
# services/loss_result.py (or inside your loss blueprint module)
from sqlalchemy import text
from datetime import datetime
from flask import current_app as app
from app.extensions import db  # adjust import to your project

def compute_and_upsert_loss_result(run_id: int) -> dict | None:
    """
    Computes phase totals for a run from lca_scorecard_v (or fall back to lca_response join),
    then inserts/updates lca_result. Returns the row dict or None if nothing to compute.
    """

    # 1) Pull per-phase totals from your scoring view/table.
    # Prefer your *existing* scoring view if you have it (you mentioned lca_scorecard_v).
    # Expected columns in the query result: p1, p2, p3, p4, user_id
    row = db.session.execute(
        text("""
            SELECT
              COALESCE(SUM(CASE WHEN phase = 1 THEN score ELSE 0 END), 0) AS p1,
              COALESCE(SUM(CASE WHEN phase = 2 THEN score ELSE 0 END), 0) AS p2,
              COALESCE(SUM(CASE WHEN phase = 3 THEN score ELSE 0 END), 0) AS p3,
              COALESCE(SUM(CASE WHEN phase = 4 THEN score ELSE 0 END), 0) AS p4,
              MAX(user_id) AS user_id
            FROM lca_scorecard_v
            WHERE run_id = :rid
        """),
        {"rid": run_id}
    ).mappings().first()

    if not row:
        return None

    p1, p2, p3, p4 = int(row["p1"] or 0), int(row["p2"] or 0), int(row["p3"] or 0), int(row["p4"] or 0)
    total = p1 + p2 + p3 + p4
    user_id = row["user_id"] or 0

    # 2) Upsert into lca_result.
    # If you have a UNIQUE constraint on lca_result.run_id you can use ON CONFLICT in SQLite.
    # If not, do a manual UPDATE/INSERT path.
    try:
        db.session.execute(
            text("""
                INSERT INTO lca_result (user_id, run_id, phase_1, phase_2, phase_3, phase_4, total, subject, created_at)
                VALUES (:user_id, :rid, :p1, :p2, :p3, :p4, :total, 'LOSS', :now)
                ON CONFLICT(run_id) DO UPDATE SET
                    user_id = excluded.user_id,
                    phase_1 = excluded.phase_1,
                    phase_2 = excluded.phase_2,
                    phase_3 = excluded.phase_3,
                    phase_4 = excluded.phase_4,
                    total   = excluded.total
            """),
            {"user_id": user_id, "rid": run_id, "p1": p1, "p2": p2, "p3": p3, "p4": p4, "total": total, "now": datetime.utcnow().isoformat(sep=" ", timespec="seconds")}
        )
    except Exception:
        # Fallback if run_id isn’t unique. Update if exists, else insert.
        existing = db.session.execute(
            text("SELECT id FROM lca_result WHERE run_id = :rid"),
            {"rid": run_id}
        ).first()
        if existing:
            db.session.execute(
                text("""
                    UPDATE lca_result
                       SET user_id=:user_id, phase_1=:p1, phase_2=:p2, phase_3=:p3, phase_4=:p4, total=:total, subject='LOSS'
                     WHERE run_id=:rid
                """),
                {"user_id": user_id, "rid": run_id, "p1": p1, "p2": p2, "p3": p3, "p4": p4, "total": total}
            )
        else:
            db.session.execute(
                text("""
                    INSERT INTO lca_result (user_id, run_id, phase_1, phase_2, phase_3, phase_4, total, subject, created_at)
                    VALUES (:user_id, :rid, :p1, :p2, :p3, :p4, :total, 'LOSS', :now)
                """),
                {"user_id": user_id, "rid": run_id, "p1": p1, "p2": p2, "p3": p3, "p4": p4, "total": total, "now": datetime.utcnow().isoformat(sep=" ", timespec="seconds")}
            )

    db.session.commit()

    return {
        "user_id": user_id, "run_id": run_id,
        "phase_1": p1, "phase_2": p2, "phase_3": p3, "phase_4": p4, "total": total
    }
# app/jobs/loss_archive.py
from sqlalchemy import text
from app.extensions import db  # adjust if your app factory exposes db elsewhere

def ensure_archive_tables():
    with db.engine.begin() as conn:
        # create empty archive tables with same columns
        conn.exec_driver_sql("""
            CREATE TABLE IF NOT EXISTS lca_run_archive           AS SELECT * FROM lca_run WHERE 0;
            CREATE TABLE IF NOT EXISTS lca_result_archive        AS SELECT * FROM lca_result WHERE 0;
            CREATE TABLE IF NOT EXISTS lca_response_archive      AS SELECT * FROM lca_response WHERE 0;
        """)
        # useful hot indexes (no-op if they exist)
        conn.exec_driver_sql("""
            CREATE INDEX IF NOT EXISTS ix_lca_run_user_started ON lca_run (user_id, started_at DESC);
            CREATE INDEX IF NOT EXISTS ix_lca_response_run     ON lca_response (run_id);
            CREATE INDEX IF NOT EXISTS ix_lca_result_run       ON lca_result (run_id);
        """)

def archive_finished_runs(older_than_days=30, limit=500):
    ensure_archive_tables()
    with db.engine.begin() as conn:
        # pick candidates
        rows = conn.execute(text("""
            SELECT id FROM lca_run
             WHERE status='finished'
               AND started_at <= datetime('now', :offset)
            ORDER BY datetime(started_at) ASC
            LIMIT :lim
        """), {"offset": f"-{older_than_days} days", "lim": limit}).fetchall()

        run_ids = [r[0] for r in rows]
        if not run_ids:
            return 0

        id_list = ",".join(str(x) for x in run_ids)

        # move children first, then parent
        conn.exec_driver_sql(f"""
            INSERT INTO lca_response_archive SELECT * FROM lca_response WHERE run_id IN ({id_list});
            DELETE FROM lca_response WHERE run_id IN ({id_list});
        """)
        conn.exec_driver_sql(f"""
            INSERT INTO lca_result_archive SELECT * FROM lca_result WHERE run_id IN ({id_list});
            DELETE FROM lca_result WHERE run_id IN ({id_list});
        """)
        conn.exec_driver_sql(f"""
            INSERT INTO lca_run_archive SELECT * FROM lca_run WHERE id IN ({id_list});
            DELETE FROM lca_run WHERE id IN ({id_list});
        """)

        return len(run_ids)
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

# Example models: adjust to match your DB


class LcaResponse(db.Model):
    __tablename__ = "lca_response"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)
    question_id = db.Column(db.Integer, nullable=False)
    answer = db.Column(db.String(3), nullable=False)  # 'yes' / 'no'
    created_at = db.Column(db.DateTime, server_default=db.func.now())
    run_id = db.Column(db.Integer, db.ForeignKey("lca_run.id"), nullable=False)

class LcaRun(db.Model):
    __tablename__ = "lca_run"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False, index=True)
    status = db.Column(db.String(20), nullable=False, default="in_progress")
    current_pos = db.Column(db.Integer, nullable=False, default=1)

    # 🔁 match your Postgres table
    created_at = db.Column(db.DateTime, server_default=db.func.now(), nullable=False)
    completed_at = db.Column(db.DateTime)

    # if you had subject or started_at here, REMOVE those lines completely
    # no ForeignKey from this model, responses link back to it
# app/utils/loss_service.py
from sqlalchemy import text
from flask import session
from app.extensions import db

# ---------- Runs ----------
def create_run(user_id: int) -> int:
    db.session.execute(text("""
        INSERT INTO lca_run (user_id, started_at, status)
        VALUES (:uid, datetime('now'), 'in_progress')
    """), {"uid": user_id})
    rid = db.session.execute(text("SELECT last_insert_rowid()")).scalar()
    db.session.commit()
    session["loss_run_id"] = int(rid)
    return int(rid)

def finish_run(run_id: int) -> None:
    db.session.execute(text("""
        UPDATE lca_run SET finished_at = datetime('now'), status='finished'
        WHERE id = :rid
    """), {"rid": int(run_id)})
    db.session.commit()
    if session.get("loss_run_id") == run_id:
        session.pop("loss_run_id", None)

def latest_run_id(user_id: int) -> int | None:
    return db.session.execute(text("""
        SELECT id FROM lca_run
        WHERE user_id = :uid
        ORDER BY id DESC LIMIT 1
    """), {"uid": user_id}).scalar()

def list_runs(user_id: int, limit: int = 20):
    return db.session.execute(text("""
        SELECT id, status, started_at, finished_at
        FROM lca_run
        WHERE user_id = :uid
        ORDER BY id DESC
        LIMIT :lim
    """), {"uid": user_id, "lim": limit}).mappings().all()

# ---------- Responses ----------
def save_answer(user_id: int, run_id: int, question_id: int, answer: str) -> None:
    db.session.execute(text("""
        INSERT INTO lca_response (user_id, run_id, question_id, answer)
        VALUES (:uid, :rid, :qid, :ans)
        ON CONFLICT(run_id, question_id)
        DO UPDATE SET answer = excluded.answer
    """), {"uid": user_id, "rid": run_id, "qid": question_id, "ans": (answer or "").strip().lower()})
    db.session.commit()

def responses_for_run(run_id: int):
    return db.session.execute(text("""
        SELECT r.question_id AS qid, r.answer,
               CASE WHEN lower(r.answer)='yes' THEN sd.p1 ELSE 0 END AS p1,
               CASE WHEN lower(r.answer)='yes' THEN sd.p2 ELSE 0 END AS p2,
               CASE WHEN lower(r.answer)='yes' THEN sd.p3 ELSE 0 END AS p3,
               CASE WHEN lower(r.answer)='yes' THEN sd.p4 ELSE 0 END AS p4
        FROM lca_response r
        JOIN lca_score_definitions sd ON sd.question_id = r.question_id
        WHERE r.run_id = :rid
        ORDER BY r.question_id
    """), {"rid": run_id}).mappings().all()

# ---------- Totals / Scorecard ----------
def totals_for_run(run_id: int):
    return db.session.execute(text("""
        SELECT
          SUM(CASE WHEN lower(r.answer)='yes' THEN sd.p1 ELSE 0 END) AS p1_raw,
          SUM(CASE WHEN lower(r.answer)='yes' THEN sd.p2 ELSE 0 END) AS p2_raw,
          SUM(CASE WHEN lower(r.answer)='yes' THEN sd.p3 ELSE 0 END) AS p3_raw,
          SUM(CASE WHEN lower(r.answer)='yes' THEN sd.p4 ELSE 0 END) AS p4_raw
        FROM lca_response r
        JOIN lca_score_definitions sd ON sd.question_id = r.question_id
        WHERE r.run_id = :rid
    """), {"rid": run_id}).mappings().first()

def maxima():
    return db.session.execute(text("""
        SELECT SUM(p1) AS p1_max, SUM(p2) AS p2_max,
               SUM(p3) AS p3_max, SUM(p4) AS p4_max
        FROM lca_score_definitions
    """)).mappings().first()

# ---------- Persist materialized results (optional) ----------
def persist_results_row(run_id: int) -> None:
    t = totals_for_run(run_id)
    if not t:  # no answers yet
        t = {"p1_raw": 0, "p2_raw": 0, "p3_raw": 0, "p4_raw": 0}
    db.session.execute(text("""
        INSERT INTO lca_result (run_id, user_id, p1_raw, p2_raw, p3_raw, p4_raw, created_at)
        SELECT :rid, lr.user_id, :p1, :p2, :p3, :p4, datetime('now')
        FROM lca_run lr WHERE lr.id = :rid
        ON CONFLICT(run_id) DO UPDATE SET
          p1_raw = excluded.p1_raw,
          p2_raw = excluded.p2_raw,
          p3_raw = excluded.p3_raw,
          p4_raw = excluded.p4_raw,
          created_at = excluded.created_at
    """), {"rid": run_id,
           "p1": t["p1_raw"] or 0, "p2": t["p2_raw"] or 0,
           "p3": t["p3_raw"] or 0, "p4": t["p4_raw"] or 0})
    db.session.commit()
{# Header row #}
<div class="flex items-center justify-between mb-4">
  <h1 class="text-2xl font-semibold">LOSS • Admin Dashboard</h1>

  <form method="get" id="runForm" class="flex items-center gap-2">
    <label for="runSelect" class="text-sm text-slate-600">Run:</label>
    <select name="run_id" id="runSelect"
            class="rounded border px-2 py-1 text-sm"
            onchange="this.form.submit()">
      {% for r in runs %}
        <option value="{{ r.id }}" {{ 'selected' if selected and r.id == selected.id else '' }}>
          #{{ r.id }} • U{{ r.user_id }} • {{ (r.started_at or '')[:16] }} • {{ r.status }} • Σ={{ r.total }}
        </option>
      {% endfor %}
    </select>
  </form>
</div>

{# Action buttons (use the selected run_id) #}
{% set rid = selected.id if selected else None %}

<div class="flex flex-wrap items-center gap-2">
  <a href="{{ url_for('admin_bp.loss_responses', run_id=rid) }}"
     class="rounded border px-3 py-1 text-sm hover:bg-slate-50">Responses</a>

  <a href="{{ url_for('admin_bp.loss_result', run_id=rid) }}"
     class="rounded border px-3 py-1 text-sm hover:bg-slate-50">Result</a>

  <a href="{{ url_for('admin_bp.loss_report', run_id=rid) }}"
     class="rounded bg-blue-600 text-white px-3 py-1 text-sm hover:bg-blue-700">View Report</a>

  <a href="{{ url_for('admin_bp.loss_report', run_id=rid, send=1) }}"
     class="rounded bg-emerald-600 text-white px-3 py-1 text-sm hover:bg-emerald-700">Send Report (Email + PDF)</a>

  <div class="ml-auto flex items-center gap-3">
    <a href="{{ url_for('loss_bp.course_start') }}"
       class="rounded border px-3 py-1 text-sm hover:bg-slate-50">Start Test</a>
    <a href="{{ url_for('auth_bp.bridge_dashboard') }}"
       class="text-sm text-slate-500 hover:underline">Back to Bridge</a>
  </div>
</div>

{# Optional: a small info card about the selected run #}
{% if selected %}
  <div class="mt-4 rounded-2xl border bg-white p-4">
    <div class="text-sm text-slate-700">
      <div><b>Run:</b> #{{ selected.id }} • <b>User:</b> {{ selected.user_id }}</div>
      <div><b>Started:</b> {{ (selected.started_at or '')[:19] }} •
           <b>Finished:</b> {{ (selected.finished_at or '-')[:19] }} •
           <b>Status:</b> {{ selected.status }} •
           <b>Total:</b> {{ selected.total }}</div>
    </div>
  </div>
{% endif %}
render@srv-d47bhsjipnbc73coe6ag-5698c66fcd-dgsdz:~/project/src$ 