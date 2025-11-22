# school_loss_routes.py
from __future__ import annotations
import io
from flask import (
    Blueprint, ctx, current_app, render_template, redirect, url_for, 
    session, request, flash, abort, make_response, send_file)
from flask import request, render_template, send_file, url_for, redirect, current_app 
import pdfkit
from app.admin.loss.routes import _render_report_html
from app.models.auth import AuthSubject
from app.models.loss import (
    LcaRun,  LcaQuestion,
    )
from app.extensions import db
from sqlalchemy import select, text, inspect, func, and_
from flask import send_file
from app.utils.country_list import COUNTRIES, _name_code_iter
from app.utils.mailer import send_loss_report_email as _send_mail  # <— use the shared mailer
from app.school_loss.routes import (
    SUBJECT, _came_from_admin, _coerce_dt, _current_user_id, _extract_phase_scores_from_ctx,
    _finalize_and_send_pdf, _get_int_arg, _get_user_id_for_run, _infer_user_id_for_run, 
    _loss_result_percents, _render_loss_pdf_bytes, _scores_from_blocks, _send_pdf_email_smtp, 
    _sum_scorecard, compute_loss_results, create_loss_run_for_user, ensure_lca_result, finalize_run_totals,
    finish_loss_run, latest_run_for_user, viewer_is_admin
    )
from app.subject_loss.charts import phase_scores_bar
from app.subject_loss.report_context import render_report_html
from app.subject_loss.report_context_adapter import build_learner_report_ctx
from app.utils.mailer import send_pdf_email
from xhtml2pdf import pisa
from datetime import datetime
import matplotlib.dates as mdates
from flask import render_template_string
import base64
from reportlab.platypus import Table, TableStyle
from reportlab.lib import colors
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # ✅ prevents Tkinter errors
from matplotlib import pyplot as plt
from flask_login import login_required, current_user
from app.diagnostics import trace_route
import traceback
try:
    from app.utils.assessment_helpers import get_user_display_name
except Exception:
    get_user_display_name = None
from decimal import Decimal, ROUND_HALF_UP
from app.admin.loss.utils import get_run_id, with_run_id_in_ctx
from flask import session
from sqlalchemy import text
from math import ceil
import csv, os
try:
    from flask_login import current_user
except Exception:
    current_user = None
from flask import current_app as cap
try:
    from weasyprint import HTML
    WEASYPRINT_AVAILABLE = True
except Exception:
    WEASYPRINT_AVAILABLE = False
from weasyprint import HTML as WPHTML   # alias to avoid name confusion
from flask import (
    Blueprint, render_template, request, abort, current_app, send_file
)
from flask_login import current_user
from werkzeug.exceptions import BadRequest
from io import BytesIO
from pathlib import Path
from functools import lru_cache
import csv
from flask_wtf.csrf import generate_csrf
from flask_mail import Message
from app.extensions import db, mail  # adjust if your Mail instance is named differently
from flask_login import login_required, current_user, logout_user
from app.utils.post_assessment import handle_exit_actions  # where your helper lives
from threading import Thread
from sqlalchemy import func as SA_FUNC, text as SA_TEXT
from app.payments.pricing import price_for_country, subject_id_for  # table-driven helper

loss_bp = Blueprint("loss_bp", __name__, url_prefix="/loss")

@loss_bp.get("/about")
def about_loss():
    # Subject slug/id (no magic 0)
    slug = (request.args.get("subject") or "loss").strip().lower()
    sid = subject_id_for(slug)  # your existing helper; returns int or raises

    # Keep subject in reg_ctx so pricing/registration can see it
    reg_ctx = session.setdefault("reg_ctx", {})
    reg_ctx["subject"] = slug

    # 1) Prefer a locked quote from session (set by /payments/pricing/lock)
    q = None
    if isinstance(reg_ctx.get("quote"), dict):
        q = {
            "currency": reg_ctx["quote"].get("currency"),
            "amount_cents": reg_ctx["quote"].get("amount_cents"),
        }

    # 2) If logged in and no session quote, use last enrollment quote
    if not q and getattr(current_user, "is_authenticated", False):
        row = db.session.execute(
            db.text("""
                SELECT ue.quoted_currency, ue.quoted_amount_cents
                FROM user_enrollment ue
                WHERE ue.user_id = :uid AND ue.subject_id = :sid
                ORDER BY ue.started_at DESC
                LIMIT 1
            """),
            {"uid": current_user.id, "sid": sid},
        ).first()
        if row and row[1] is not None:
            q = {"currency": row[0], "amount_cents": int(row[1])}

    # 3) Still nothing? Provisional price based on country
    if not q:
        cc = (request.headers.get("CF-IPCountry") or "ZA").strip().upper()
        #cur, amt, _ = price_for_country(sid, cc)
        cur, amt, _status, _fx = price_for_country(sid, cc)

        if amt is not None:
            q = {"currency": cur, "amount_cents": int(amt)}

    # 4) Build price object (or None)
    price = {"currency": q["currency"], "amount_cents": q["amount_cents"]} if q else None

    # 5) Countries list ONLY if your template still needs it; otherwise drop it
    countries = [
        {"name": nm, "code": (cd or "").upper()}
        for (nm, cd) in _name_code_iter(COUNTRIES)
    ]

    return render_template(
        "subject/loss/about.html",
        price=price,
        subject_id=sid,
        subject_slug=slug,
        can_enroll=True,
    )

# ----- entry from Bridge (non-admins jump straight into course) -----Step 1
# ====================================================
@loss_bp.get("/subject/home")
def subject_home():
    # fetch landing card row (id=1) – adjust if you use another id
    sa = current_app.extensions.get("sqlalchemy")
    row = None
    if sa:
        r = sa.session.execute(
            text("SELECT id,title,caption,content FROM lca_start WHERE id=1")
        ).mappings().first()
        row = dict(r) if r else None

    item = row or {"title": "Loss", "caption": "Press Start to begin.", "content": "Welcome back."}
    next_url = url_for("loss_bp.course_start")  # kicks off the assessment flow
    return render_template("subject/loss/cards/start.html", item=item, next_url=next_url)

# app/subject_loss/routes.py
# ensure loss_bp has url_prefix="/loss"

@loss_bp.post("/enrol", endpoint="enrol_loss")
def enrol_loss():
    return redirect(url_for(
        "auth_bp.start_registration",
        subject="loss",
        role="user",  # LOSS has a single role
        next=url_for("loss_bp.about_loss")  # where to land after payment
    ))


# ====================================================
@loss_bp.route("/dashboard", methods=["GET"])
def dashboard():
    if session.get("is_admin"):
        return render_template("school_loss/admin_dashboard.html")
    return redirect(url_for("loss_bp.course_start"))

# Assessment starts from here step 2
# =====================
@loss_bp.get("/course/start")
def course_start():
    uid = session.get("user_id", 1)
    run_id = (request.args.get("run_id", type=int)
              or session.get("current_run_id")
              or session.get("loss_run_id"))

    if not run_id:
        current_app.logger.warning("course_start: creating lca_run for uid=%s (LOSS)", uid)
        run_id = create_loss_run_for_user(uid)

    if request.args.get("return_to") == "admin":
        session["loss_return_to"] = {
            "endpoint": "admin_bp.loss_home",
            "params": {"uid": request.args.get("admin_uid", type=int) or session.get("user_id")},
        }
        session["came_from_admin"] = True  # optional flag for UI

    session["current_run_id"] = run_id
    session["loss_run_id"] = run_id

    #pos = 1
    return redirect(url_for("loss_bp.sequence_step", pos=1, run_id=run_id))


# Assessment step 3
# ================
@loss_bp.route("/sequence/<int:pos>", methods=["GET", "POST"])
def sequence_step(pos: int):
    # 1) read -> keep
    run_id = (
        request.args.get("run_id", type=int)
        or session.get("current_run_id")
        or session.get("loss_run_id")
        or session.get("last_loss_run_id")
    )
    if not run_id:
        current_app.logger.warning("sequence_step: missing run_id; redirecting to start")
        return redirect(url_for("loss_bp.course_start"))

    session["current_run_id"] = run_id

    # 2) sequence (unchanged)
    seq = get_sequence()
    total = len(seq)
    if total == 0:
        current_app.logger.warning("sequence_step: empty sequence; redirecting to start.")
        return redirect(url_for("loss_bp.course_start"))

    # Clamp pos — ALWAYS include run_id
    if pos < 1:
        return redirect(url_for("loss_bp.sequence_step", pos=1, run_id=run_id))
    if pos > total:
        return redirect(url_for("loss_bp.sequence_step", pos=total, run_id=run_id))

    kind, ident = seq[pos - 1]
    current_app.logger.info(f"sequence_step pos={pos} kind={kind} ident={ident} run_id={run_id}")

    # Default next — ALWAYS include run_id
    next_pos = min(pos + 1, total)
    next_url = url_for("loss_bp.sequence_step", pos=next_pos, run_id=run_id)

    # Final step → your result dashboard (or admin report if you prefer)
    if pos == total:
        session["last_loss_run_id"] = run_id
        next_url = url_for("loss_bp.result_run", run_id=run_id)
        # or: next_url = url_for("admin_bp.loss_report", run_id=run_id)
        
    # POST → PRG
    if request.method == "POST":
        return redirect(next_url)

    # QUESTION BRANCH
    if kind == "question":
        q_range = None
        if isinstance(ident, str) and "-" in ident:
            left, right = ident.split("-", 1)
            try:
                q_range = (int(left.strip()), int(right.strip()))
            except ValueError:
                q_range = None
        elif isinstance(ident, int) or (isinstance(ident, str) and ident.isdigit()):
            n = int(ident)
            q_range = (n, n)

        session["q_range"] = q_range
        session["q_seq_pos"] = pos
        session["current_index"] = 0
        session["active_q_range"] = q_range

        # Carry run_id to the question flow (harmless if ignored)
        return redirect(url_for("loss_bp.assessment_question_flow", run_id=run_id))

    # Finalize totals at explain/after_questions (as you had)
    if kind == "explain" and str(ident) == "after_questions" and request.method == "GET":
        uid = int(session.get("user_id"))
        rid = int(session.get("loss_run_id") or run_id)
        try:
            finalize_run_totals(rid, uid)
        except Exception:
            current_app.logger.exception("finalize_run_totals failed: run_id=%s uid=%s", run_id, uid)

        # after last card is submitted
        run_id = session.get("loss_run_id")
        uid    = session.get("user_id")

        compute_loss_results(run_id, uid)   # your helper
        finish_loss_run(run_id)             # your helper

        # ✅ go to PUBLIC route, not admin
        return redirect(url_for("loss_bp.result_run", run_id=run_id))



    # NON-QUESTION CARDS
    # --- NON-QUESTION CARDS: load from tables, no helpers, no hardcoding ---
    # --- NON-QUESTION CARDS: DB-driven, no external imports/helpers ---

    # Choose template under subject/loss
    template = {
        "instruction": "subject/loss/cards/instruction.html",
        "pause":       "subject/loss/cards/pause.html",
        "explain":     "subject/loss/cards/explain.html",
    }.get(kind, "subject/loss/cards/instruction.html")

    # Map kind -> table (adjust when you add specific tables)
    table_by_kind = {
        "instruction": "lca_instruction",
        "pause":       "lca_pause",   # change to 'lca_pause' when that table exists
        "explain":     "lca_explain",   # change to 'lca_explain' when that table exists
    }
    table = table_by_kind.get(kind)

    row_dict = None
    if table:
        # Try SQLAlchemy first (if present)
        sa_ext = current_app.extensions.get("sqlalchemy")
        if sa_ext:
            # ✅ Flask-SQLAlchemy v3: use sa_ext.session (not sa_ext.db.session)
            from sqlalchemy import text
            stmt = text(f"SELECT id, title, caption, content FROM {table} WHERE id = :id")
            result = sa_ext.session.execute(stmt, {"id": int(ident)})
            row = result.mappings().first()
            row_dict = dict(row) if row else None
        else:
            # fallback to sqlite3
            import sqlite3, os
            # try DATABASE first, else derive from SQLALCHEMY_DATABASE_URI if it's sqlite
            db_path = current_app.config.get("DATABASE")
            if not db_path:
                uri = current_app.config.get("SQLALCHEMY_DATABASE_URI", "")
                if uri.startswith("sqlite:///"):
                    db_path = uri.replace("sqlite:///", "", 1)
                elif uri.startswith("sqlite:////"):
                    db_path = uri.replace("sqlite:////", "/", 1)
            if not db_path or not os.path.exists(db_path):
                current_app.logger.error("DB path not found; set DATABASE or use sqlite URI. table=%s", table)
                row_dict = None
            else:
                conn = sqlite3.connect(db_path)
                try:
                    conn.row_factory = sqlite3.Row
                    cur = conn.execute(
                        f"SELECT id, title, caption, content FROM {table} WHERE id = ?",
                        (int(ident),)
                    )
                    r = cur.fetchone()
                    row_dict = dict(r) if r else None
                finally:
                    conn.close()


    # Build item for template (fallback shows at least a title if no row found)
    item = row_dict or {"title": f"{kind.title()} {ident}", "caption": "", "content": ""}

    # Always pass a Next button so the footer renders
    buttons = [{"label": "Next", "href": next_url, "kind": "primary"}]

    return render_template(
        template,
        kind=kind,
        ident=ident,
        pos=pos,
        total=total,
        next_url=next_url,
        item=item,
        buttons=buttons,
        run_id=run_id,
    )

# ===== Step 4 =======
EXPLAIN_COUNT = 8  # or whatever you use

def get_sequence():
    seq = []

    # 1) Instructions (IDs 1..6 in lca_instruction)
    seq += [("instruction", i) for i in range(1, 7)]

    # 2) Questions 1..25
    seq += [("question", i) for i in range(1, 26)]

    # — Single pause between Q25 and Q26 —
    # Use a numeric ID that exists in your table (e.g., 6 = "Take a Break")
    seq += [("pause", 6)]

    # 3) Questions 26..50
    seq += [("question", i) for i in range(26, 51)]

    # 4) Explains (1..EXPLAIN_COUNT)
    seq += [("explain", i) for i in range(1, EXPLAIN_COUNT + 1)]

    return seq

# ===== Step 5 =======
@loss_bp.route("/result/finalize", methods=["POST"])
def result_finalize():
    run_id = request.form.get("run_id", type=int) or request.args.get("run_id", type=int)
    
    if not run_id:
        abort(400, description="run_id is required")
    session["last_loss_run_id"] = run_id
    #return redirect(url_for("loss_bp.result_dashboard", run_id=run_id))
    return redirect(url_for("loss_bp.result_run", run_id=run_id))


@loss_bp.route("/assessment_question_flow", methods=["GET", "POST"])
def assessment_question_flow():
    uid = session.get("user_id")
    if not uid:
        return redirect(url_for("auth_bp.login"))

    # Current run_id (prefer session, else latest for this user)
    rid = session.get("loss_run_id")
    if not rid:
        rid = db.session.execute(
            text("SELECT id FROM lca_run WHERE user_id=:uid ORDER BY id DESC LIMIT 1"),
            {"uid": uid},
        ).scalar()

    # Optional hard reset for THIS run only
    if request.method == "GET" and request.args.get("reset") == "1":
        if rid:
            db.session.execute(text("DELETE FROM lca_response   WHERE run_id=:rid"), {"rid": rid})
            db.session.execute(text("DELETE FROM lca_scorecard WHERE run_id=:rid"), {"rid": rid})
            db.session.execute(text("DELETE FROM lca_result    WHERE run_id=:rid"), {"rid": rid})
            db.session.commit()
        for k in ("current_index", "q_range", "q_seq_pos", "active_q_range"):
            session.pop(k, None)
        return redirect(url_for("loss_bp.assessment_question_flow"))

    # Active block (e.g. (1,25) or (26,50)) is set by sequence_step
    q_range = session.get("q_range")  # tuple like (start, end) or None

    # Reset index if block changed (prevents loops)
    if q_range != session.get("active_q_range"):
        session["active_q_range"] = q_range
        session["current_index"] = 0

    # Build questions for this block
    q = LcaQuestion.query
    if q_range and all(q_range):
        q = q.filter(LcaQuestion.number.between(int(q_range[0]), int(q_range[1])))
    questions = q.order_by(LcaQuestion.number.asc()).all()

    global_total = LcaQuestion.query.count()

    # If no questions in this block → advance sequence
    if not questions:
        pos = int(session.get("q_seq_pos", 0))
        for k in ("q_range", "q_seq_pos", "current_index", "active_q_range"):
            session.pop(k, None)
        return redirect(url_for("loss_bp.sequence_step", pos=pos + 1))

    idx = int(session.get("current_index", 0))

    # Finished the block already → advance sequence
    if idx >= len(questions):
        pos = int(session.get("q_seq_pos", 0))
        for k in ("q_range", "q_seq_pos", "current_index", "active_q_range"):
            session.pop(k, None)
        return redirect(url_for("loss_bp.sequence_step", pos=pos + 1))

    # ---------- POST: save answer ----------
    answer = None  # defensive: prevents NameError if refactored later
    if request.method == "POST":
        answer = (request.form.get("answer") or "").strip().lower()
        qid    = request.form.get("question_id", type=int)

        # Validate
        if answer not in {"yes", "no"} or not qid:
            qrow = questions[idx]
            offset = (int(q_range[0]) - 1) if (q_range and all(q_range)) else 0
            display_idx = offset + idx + 1
            pct = int(round(display_idx * 100.0 / max(1, global_total)))
            return render_template(
                "subject/loss/cards/question.html",
                question=qrow,
                display_idx=display_idx,
                display_total=global_total,
                progress_pct=pct,
                error="Please choose Yes or No.",
            )

        # Upsert into lca_response (per-run uniqueness)
        db.session.execute(text("""
            INSERT INTO lca_response (user_id, run_id, question_id, answer)
            VALUES (:uid, :rid, :qid, :ans)
            ON CONFLICT(run_id, question_id) DO UPDATE SET
              user_id = excluded.user_id,
              answer  = excluded.answer
        """), {"uid": uid, "rid": rid, "qid": qid, "ans": answer})

        # Upsert into lca_scorecard (per-run uniqueness)
        db.session.execute(text("""
            INSERT INTO lca_scorecard
              (user_id, run_id, question_id, answer_type, phase_1, phase_2, phase_3, phase_4)
            SELECT
              :uid, :rid, :qid, :ans,
              m.phase_1, m.phase_2, m.phase_3, m.phase_4
            FROM lca_question_phase_map m
            WHERE m.question_id = :qid AND m.answer_type = :ans
            ON CONFLICT(run_id, question_id) DO UPDATE SET
              user_id     = excluded.user_id,
              answer_type = excluded.answer_type,
              phase_1     = excluded.phase_1,
              phase_2     = excluded.phase_2,
              phase_3     = excluded.phase_3,
              phase_4     = excluded.phase_4
        """), {"uid": uid, "rid": rid, "qid": qid, "ans": answer})

        # Ensure a lca_result row exists for this run (unique on run_id)
        db.session.execute(text("""
            INSERT INTO lca_result (user_id, run_id, subject, phase_1, phase_2, phase_3, phase_4)
            SELECT :uid, :rid, 'LOSS', 0, 0, 0, 0
            WHERE NOT EXISTS (
                SELECT 1
                FROM lca_result
                WHERE user_id = :uid
                AND run_id  = :rid
                AND subject = 'LOSS'
            )
        """), {"uid": uid, "rid": rid})

        # Increment cumulative totals in lca_result from the map for this (qid, answer)
        db.session.execute(text("""
            UPDATE lca_result
               SET phase_1 = phase_1 + (
                         SELECT COALESCE(phase_1,0)
                         FROM lca_question_phase_map
                         WHERE question_id = :qid AND answer_type = :ans),
                   phase_2 = phase_2 + (
                         SELECT COALESCE(phase_2,0)
                         FROM lca_question_phase_map
                         WHERE question_id = :qid AND answer_type = :ans),
                   phase_3 = phase_3 + (
                         SELECT COALESCE(phase_3,0)
                         FROM lca_question_phase_map
                         WHERE question_id = :qid AND answer_type = :ans),
                   phase_4 = phase_4 + (
                         SELECT COALESCE(phase_4,0)
                         FROM lca_question_phase_map
                         WHERE question_id = :qid AND answer_type = :ans),
                   total   = total + (
                         SELECT COALESCE(phase_1,0)+COALESCE(phase_2,0)+COALESCE(phase_3,0)+COALESCE(phase_4,0)
                         FROM lca_question_phase_map
                         WHERE question_id = :qid AND answer_type = :ans)
             WHERE run_id = :rid
        """), {"rid": rid, "qid": qid, "ans": answer})

        # Commit all DB work for this answer
        db.session.commit()

        # Advance index and continue / or return to sequence
        idx += 1
        session["current_index"] = idx

        if idx >= len(questions):
            # End of block: update max_* columns once (based on questions answered so far in this run)
            db.session.execute(text("""
            WITH qset AS (
              SELECT DISTINCT question_id
              FROM lca_scorecard
              WHERE run_id = :rid
            ),
            mx AS (
              SELECT
                SUM(COALESCE(m.phase_1,0)) AS max_p1,
                SUM(COALESCE(m.phase_2,0)) AS max_p2,
                SUM(COALESCE(m.phase_3,0)) AS max_p3,
                SUM(COALESCE(m.phase_4,0)) AS max_p4
              FROM lca_question_phase_map m
              JOIN qset q ON q.question_id = m.question_id
            )
            UPDATE lca_result
               SET max_phase_1 = (SELECT max_p1 FROM mx),
                   max_phase_2 = (SELECT max_p2 FROM mx),
                   max_phase_3 = (SELECT max_p3 FROM mx),
                   max_phase_4 = (SELECT max_p4 FROM mx),
                   max_total   = (SELECT max_p1 + max_p2 + max_p3 + max_p4 FROM mx)
             WHERE run_id = :rid
            """), {"rid": rid})
            db.session.commit()

            pos = int(session.get("q_seq_pos", 0))
            for k in ("q_range", "q_seq_pos", "current_index", "active_q_range"):
                session.pop(k, None)
            return redirect(url_for("loss_bp.sequence_step", pos=pos + 1))

        return redirect(url_for("loss_bp.assessment_question_flow"))

    # ---------- GET: render current question ----------
    qrow = questions[idx]
    offset = (int(q_range[0]) - 1) if (q_range and all(q_range)) else 0
    display_idx = offset + idx + 1
    pct = int(round(display_idx * 100.0 / max(1, global_total)))
    return render_template(
        "subject/loss/cards/question.html",
        question=qrow,
        display_idx=display_idx,
        display_total=global_total,
        progress_pct=pct,
    )

TEMPLATE_DIR_LEARNER = "subject/loss"   # learner page templates (report.html / report.pdf)
TEMPLATE_PDF_LEARNER = "subject/loss/report.pdf"  # or "subject/loss/report_pdf.html" if that's your file
TEMPLATE_RESULTS_HUB = "subject/loss/results_hub.html"

@loss_bp.route("/result/<int:run_id>")
def result_run(run_id: int):
    
    uid = request.args.get("user_id", type=int)
    if uid is None:
        from sqlalchemy import text
        
        row = db.session.execute(
            text("SELECT user_id FROM lca_result WHERE run_id=:rid LIMIT 1"),
            {"rid": run_id},
        ).mappings().first() or db.session.execute(
            text("SELECT user_id FROM lca_run WHERE id=:rid LIMIT 1"),
            {"rid": run_id}
        ).mappings().first()
        uid = row["user_id"] if row else None

    # Build the same context you use elsewhere
    from app.admin.loss.routes import _build_context
    ctx, _, _row = _build_context(run_id, uid)
    if not ctx:
        return (f"No result for run_id={run_id}", 404)

    # Make sure the learner page logic is chosen
    ctx["viewer_is_admin"] = False
    ctx["run_id"] = run_id
    ctx["user_id"] = uid

    # 🔒 Pin the exact learner template
    return render_template(f"subject/loss/results_hub.html", **ctx)
    #return render_template(TEMPLATE_RESULTS_HUB, **ctx)

@loss_bp.post("/finish")
def finish_run():
    """Finalize current run: write lca_result snapshot and mark run completed.
       (We intentionally DO NOT delete lca_scorecard so you can verify multiple runs.)
    """
    uid = _current_user_id()
    rid = int(session.get("loss_run_id"))

    totals = _sum_scorecard(rid)
    p1 = totals["p1"] or 0
    p2 = totals["p2"] or 0
    p3 = totals["p3"] or 0
    p4 = totals["p4"] or 0
    total = totals["total"] or 0

    # upsert result snapshot
    db.session.execute(text("""
        INSERT INTO lca_result (run_id, user_id, subject, phase_1, phase_2, phase_3, phase_4, score_total, completed_at)
        VALUES (:rid, :uid, :subj, :p1, :p2, :p3, :p4, :total, datetime('now'))
        ON CONFLICT(run_id) DO UPDATE SET
          phase_1     = excluded.phase_1,
          phase_2     = excluded.phase_2,
          phase_3     = excluded.phase_3,
          phase_4     = excluded.phase_4,
          score_total = excluded.score_total,
          completed_at= excluded.completed_at
    """), {"rid": rid, "uid": uid, "subj": SUBJECT, "p1": p1, "p2": p2, "p3": p3, "p4": p4, "total": total})

    # mark run completed
    db.session.execute(text("""
        UPDATE lca_run SET status='completed', completed_at=datetime('now')
        WHERE id=:rid
    """), {"rid": rid})
    db.session.commit()

    # Go to admin dashboard for this run
    return redirect(url_for("admin_bp.loss_dashboard", uid=uid, run_id=rid))

@loss_bp.route("/result/pdf", methods=["GET", "POST"])
def result_pdf():
    user_id = session.get("user_id")
    return _finalize_and_send_pdf(user_id)

@loss_bp.route("/result/email", methods=["GET", "POST"])
def result_email():
    user_id = session.get("user_id")
    return _finalize_and_send_pdf(user_id)

@loss_bp.route("/result/dashboard", endpoint="result_dashboard")
def loss_result_dashboard():
    uid = session.get("user_id")
    if not uid:
        return redirect(url_for("public_bp.welcome"))

    # --- helper SQL (latest LOSS run for this user, joined to result)
    SQL_LATEST_RUN = text("""
        SELECT
            r.id            AS run_id,
            r.started_at    AS started_at,
            r.finished_at   AS finished_at,
            r.status        AS status,
            res.phase_1     AS phase_1,
            res.phase_2     AS phase_2,
            res.phase_3     AS phase_3,
            res.phase_4     AS phase_4,
            res.total       AS total,
            res.created_at  AS res_created_at
        FROM lca_run r
        LEFT JOIN lca_result res ON res.run_id = r.id
        WHERE r.user_id = :uid AND r.subject = 'LOSS'
        ORDER BY r.started_at DESC
        LIMIT 1
    """)

    # --- fetch latest run
    with db.engine.begin() as conn:
        run = conn.execute(SQL_LATEST_RUN, {"uid": uid}).mappings().first()

    if not run:
        # no runs yet → start the course
        return redirect(url_for("loss_bp.course_start"))

    # --- if result missing, compute it, then refetch so we have created_at
    if run.get("total") is None:
        # requires your existing helper:
        # compute_loss_results(run_id, user_id) -> creates lca_result row
        compute_loss_results(run["run_id"], uid)
        with db.engine.begin() as conn:
            run = conn.execute(SQL_LATEST_RUN, {"uid": uid}).mappings().first()

    # --- parse created_at into a real datetime so template's .strftime works
    res_created_at_dt = _coerce_dt(run.get("res_created_at")) or _coerce_dt(run.get("started_at"))

    # --- lightweight user info (use your users table if you have one)
    user = {
        "name":  session.get("user_name", "Learner"),
        "email": session.get("user_email", ""),
    }

    # (optional) debug
    current_app.logger.debug(
        "LOSS result_dashboard: run_id=%s total=%s res_created_at=%r",
        run.get("run_id"), run.get("total"), run.get("res_created_at")
    )
    return render_template(
        "school_loss/result_dashboard.html",
        run=run,
        user=user,
        # pass a real datetime here so your template line:
        #   (res_created_at or now()).strftime('%Y-%m-%d %H:%M')
        # works without crashing
        res_created_at=res_created_at_dt,
        now=datetime.now,
    )

@loss_bp.route("/report", methods=["GET"])
def report():
    rid = request.args.get("run_id", type=int)
    uid = request.args.get("user_id", type=int)
    if not rid:
        return ("Missing run_id", 400)

    ctx = build_learner_report_ctx(rid, uid)
    if not ctx:
        return (f"No result for run_id={rid}", 404)

    ctx["default_email"] = ctx.get("learner_email") or ""

    ctx["run_id"]        = rid
    ctx["learner_name"]  = ctx.get("learner_name") or ctx.get("user_name") or ""
    ctx["learner_email"] = ctx.get("learner_email") or ctx.get("user_email") or ""
    ctx["taken_at"]      = ctx.get("taken_at_str") or ctx.get("taken_at") or ""

    try:
        scores = _extract_phase_scores_from_ctx(ctx)
        ctx["phase_scores_pct"] = scores
        data_uri, _png = phase_scores_bar(scores)  # returns (data_uri, bytes)
        ctx["phase_scores_chart_src"] = data_uri
    except Exception as e:
        current_app.logger.exception("phase_scores_bar failed: %s", e)
        ctx["phase_scores_pct"] = None
        ctx["phase_scores_chart_src"] = None

    return render_template(f"{TEMPLATE_DIR_LEARNER}/report.html", **ctx)


@loss_bp.route("/loss/report/actions")
def report_actions():
    run_id = request.args.get("run_id", type=int)
    user_id = request.args.get("user_id", type=int)

    # Friendly notice so users know why they left the report
    flash("You’re exiting the report to Print / Save / Email.", "info")

    share_url = url_for("loss_bp.report", run_id=run_id, user_id=user_id, _external=True)

    # Wrapper page that just shows the existing actions partial
    return render_template(
        "subject/loss/report_actions.html",
        run_id=run_id,
        user_id=user_id,
        share_url=share_url,   # so the partial can use it for Email/Copy
    )


@loss_bp.route("/report.pdf")
def report_pdf():
    from sqlalchemy import text  # local import to avoid module-level surprises

    rid = _get_int_arg("run_id")
    uid = _get_int_arg("user_id", required=False)
    if not rid:
        return ("Missing run_id", 400)

    # Build base context
    ctx = build_learner_report_ctx(rid, uid) or {}

    # Ensure the template never falls back to "Learner":
    # Prefer an explicit user object/dict with at least email/full_name.
    if not ctx.get("user"):
        # Try DB lookup only if we have a uid
        if uid:
            row = db.session.execute(
                text('SELECT COALESCE(name, "") AS full_name, email FROM "user" WHERE id = :id LIMIT 1'),
                {"id": int(uid)},
            ).mappings().first()
            if row:
                ctx["user"] = {"full_name": row["full_name"], "email": row["email"]}
        # Final fallback: if build_learner_report_ctx populated an email elsewhere
        if not ctx.get("user") and ctx.get("email"):
            ctx["user"] = {"full_name": "", "email": ctx["email"]}

    ctx["pdf_mode"] = True

    # Logo as data-URI for PDF engines (with static fallback in template)
    try:
        from app.utils.branding import get_logo_data_uri
        ctx["logo_data_uri"] = get_logo_data_uri()
    except Exception:
        current_app.logger.exception("Failed to load logo_data_uri")
        ctx["logo_data_uri"] = None

    # Phase scores (P1..P4) for the PNG chart + summary
    L = _loss_result_percents(rid, uid) or {}
    scores = [
        int(L.get("P1") or L.get("phase_1") or 0),
        int(L.get("P2") or L.get("phase_2") or 0),
        int(L.get("P3") or L.get("phase_3") or 0),
        int(L.get("P4") or L.get("phase_4") or 0),
    ]
    ctx["loss_result_percents"] = {"P1": scores[0], "P2": scores[1], "P3": scores[2], "P4": scores[3]}

    # Build the embedded bar-chart image (data URI)
    try:
        data_uri, _png_bytes = phase_scores_bar(scores)
        ctx["phase_scores_chart_src"] = data_uri
    except Exception:
        current_app.logger.exception("phase_scores_bar failed")
        ctx["phase_scores_chart_src"] = None

    # Render HTML -> PDF
    html = render_template("subject/loss/report_pdf.html", **ctx)

    from io import BytesIO
    try:
        from weasyprint import HTML
        pdf_bytes = HTML(string=html, base_url=request.host_url).write_pdf()
    except Exception:
        from xhtml2pdf import pisa
        out = BytesIO()
        pisa.CreatePDF(html, dest=out, encoding="UTF-8")
        pdf_bytes = out.getvalue()

    return send_file(
        BytesIO(pdf_bytes),
        mimetype="application/pdf",
        as_attachment=True,
        download_name=f"loss-result-run-{rid}.pdf",
        max_age=0,
    )



@loss_bp.route("/subject/loss/results")
def results_hub():
    run_id = request.args.get("run_id", type=int)
    run = None

    if run_id:
        run = db.session.get(LcaRun, run_id)
    elif current_user.is_authenticated:
        run = latest_run_for_user(current_user.id)

    if not run:
        abort(404)

    # compute/ensure result row
    ensure_lca_result(run.id)

    # ✅ recognize admin either by actual role OR by admin referrer
    is_admin_view = viewer_is_admin() or _came_from_admin()
    
    # ✅ pass run_id (and user_id if you want it), not `run`
    return render_template(
        "subject/loss/results_hub.html",
        run_id=run.id,
        user_id=run.user_id,
        viewer_is_admin=viewer_is_admin(),
    )

@loss_bp.post("/loss/report.email/<int:run_id>")
def report_email_and_download(run_id: int):
    to  = request.form.get("to") or request.form.get("email")
    uid = request.form.get("user_id", type=int) or _get_user_id_for_run(run_id)

    html = _render_report_html(run_id, uid, pdf_mode=True)
    if html is None:
        return ("Report not available", 404)

    exe = os.getenv("WKHTMLTOPDF_EXE", r"C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe")
    cfg = pdfkit.configuration(wkhtmltopdf=exe)
    options = {"encoding": "UTF-8", "enable-local-file-access": None, "print-media-type": None, "quiet": None}
    pdf_bytes = pdfkit.from_string(html, False, configuration=cfg, options=options)

    # Email only if SMTP is enabled; never block the download
    try:
        if current_app.config.get("EMAIL_ENABLED", False) and to:
            from flask_mail import Message
            from app.extensions import mail
            msg = Message(subject=f"LOSS Assessment — Run #{run_id}", recipients=[to])
            msg.body = "Your LOSS assessment report is attached."
            msg.attach(f"loss-report-run-{run_id}.pdf", "application/pdf", pdf_bytes)
            mail.send(msg)
    except Exception as e:
        current_app.logger.exception("Email send failed: %s", e)

    # Always return the file
    return send_file(
        io.BytesIO(pdf_bytes),
        mimetype="application/pdf",
        as_attachment=True,
        download_name=f"loss-report-run-{run_id}.pdf",
    )

@loss_bp.route("/report.pdf/<int:run_id>")
def report_pdf_download(run_id: int):
    uid = request.args.get("user_id", type=int) or _infer_user_id_for_run(run_id)
    if uid is None:
        return ("No user for this run.", 404)

    html = render_report_html(run_id, uid, pdf_mode=True)
    if not html:
        return ("Report not available", 404)

    exe = os.getenv("WKHTMLTOPDF_EXE", r"C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe")
    cfg = pdfkit.configuration(wkhtmltopdf=exe)

    css_path = os.path.join(current_app.root_path, "static", "pdf", "pdf.css")
    options = {
        "encoding": "UTF-8",
        "enable-local-file-access": None,
        "print-media-type": None,
        "quiet": None,
        "page-size": "A4",
        "margin-top": "12mm",
        "margin-right": "12mm",
        "margin-bottom": "12mm",
        "margin-left": "12mm",
        "disable-smart-shrinking": None,
        "zoom": "1.0",
    }

    pdf_bytes = pdfkit.from_string(
        html, False, configuration=cfg, options=options,
        css=[css_path] if os.path.exists(css_path) else None
    )

    return send_file(
        io.BytesIO(pdf_bytes),
        mimetype="application/pdf",
        as_attachment=True,
        download_name=f"loss-report-run-{run_id}.pdf",
    )

@loss_bp.route("/phase-graph.pdf")
def phase_graph_pdf():
    rid = request.args.get("run_id", type=int)
    uid = request.args.get("user_id", type=int)
    if not rid:
        return ("Missing run_id", 400)

    # Build the same context as the on-page report (don’t change that code)
    ctx = build_learner_report_ctx(rid, uid) or {}
    blocks = ctx.get("phase_blocks") or ctx.get("blocks") or []

    # Derive P1..P4 for PDF without touching summary partials
    scores = _scores_from_blocks(blocks)
    data_uri, _png_bytes = phase_scores_bar(scores)  # returns (data_uri, bytes)

    # Minimal HTML just for the graph
    html = render_template(
        "subject/loss/pdf/graph_only.html",
        run_id=rid,
        scores=scores,
        chart_src=data_uri,
    )

    # Prefer pdfkit/wkhtmltopdf on Windows; fall back quietly if needed
    pdf_bytes = None
    try:
        import pdfkit
        exe = os.getenv("WKHTMLTOPDF_EXE")
        if not exe:
            for p in (
                r"C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe",
                r"C:\Program Files (x86)\wkhtmltopdf\bin\wkhtmltopdf.exe",
            ):
                if os.path.exists(p):
                    exe = p
                    break
        cfg = pdfkit.configuration(wkhtmltopdf=exe) if exe else None
        pdf_bytes = pdfkit.from_string(html, False, configuration=cfg, options={"quiet": ""})
    except Exception as e:
        current_app.logger.warning("pdfkit failed, will try WeasyPrint: %s", e)

    if not pdf_bytes:
        try:
            from weasyprint import HTML
            pdf_bytes = HTML(string=html, base_url=request.host_url).write_pdf()
        except Exception as e:
            current_app.logger.exception("WeasyPrint failed: %s", e)
            return ("PDF generation failed", 500)

    return send_file(
        BytesIO(pdf_bytes),
        mimetype="application/pdf",
        as_attachment=True,
        download_name=f"loss-phase-graph-run-{rid}.pdf",
        max_age=0,
    )

@loss_bp.route("/loss/result/<int:run_id>")
def legacy_results_redirect(run_id: int):
    return redirect(url_for("loss_bp.results_hub", run_id=run_id), code=301)

@loss_bp.route("/report/send", methods=["POST"])
def report_send_and_download():
    """
    Combined action: email the PDF to the provided address AND return a download.
    The learner stays in-flow (no separate PDF page view).
    """
    rid = request.args.get("run_id", type=int)
    uid = request.args.get("user_id", type=int)
    if not rid:
        raise BadRequest("Missing run_id")

    to_email = (request.form.get("email") or "").strip()
    if not to_email:
        raise BadRequest("Email is required.")

    from app.admin.loss.routes import _build_context
    ctx, _, _row = _build_context(rid, uid)
    if not ctx:
        abort(404)

    # Generate once, reuse for email + download
    pdf_bytes = _render_loss_pdf_bytes(rid, ctx)
    filename = f"loss-result-run-{rid}.pdf"

    # Best-effort email (don’t block the download on failures)
    try:
        learner_name = ctx.get("learner_name") or "Learner"
        subject = "Your LOSS Assessment Report"
        body = (
            f"Dear {learner_name},\n\n"
            f"Attached is your LOSS assessment report for run #{rid}.\n\n"
            f"Regards,\nAIT Platform"
        )
        # Swap to Flask-Mail if you prefer:
        # from flask_mail import Message
        # msg = Message(subject=subject, recipients=[to_email], body=body)
        # msg.attach(filename, "application/pdf", pdf_bytes)
        # mail.send(msg)
        _send_pdf_email_smtp(to_email, pdf_bytes, filename, subject, body)
        #

    except Exception as e:
        current_app.logger.exception("Email send failed: %s", e)

    return send_file(
        BytesIO(pdf_bytes),
        as_attachment=True,
        download_name=filename,
        mimetype="application/pdf",
        max_age=0,
    )




@loss_bp.post("/report/email")
@login_required
def email_report():  # endpoint: loss_bp.email_report
    # --- form inputs ---
    to = (request.form.get("to") or (current_user.email if current_user.is_authenticated else "")).strip()
    run_id = request.form.get("run_id", type=int)
    user_id = request.form.get("user_id", type=int)
    note = (request.form.get("note") or "").strip()
    include_summary = "include_summary" in request.form
    include_responses = "include_responses" in request.form

    if not to:
        flash("Please provide a recipient email.", "warning")
        return redirect(url_for("loss_bp.report_exit", run_id=run_id, user_id=user_id))

    # --- call your existing PDF view to get the bytes, without JS auto-print ---
    # This avoids duplicating PDF generation logic.
    pdf_resp = current_app.ensure_sync(
        current_app.view_functions["loss_bp.report_pdf"]
    )(run_id=run_id, user_id=user_id, auto_print=0)

    # Handle (response, status) or plain response
    if isinstance(pdf_resp, tuple):
        pdf_response = pdf_resp[0]
    else:
        pdf_response = pdf_resp

    pdf_bytes = pdf_response.get_data()

    # --- build email ---
    subject = f"LOSS Assessment Report (Run {run_id})"
    body_lines = []
    if note:
        body_lines.append(note)
        body_lines.append("")  # blank line
    body_lines.append(f"Run ID: {run_id}, User ID: {user_id}")
    if include_summary:
        body_lines.append("Summary: See the attached PDF report for your overall assessment and phase breakdown.")
    if include_responses:
        body_lines.append("Responses: The attached PDF includes itemised responses and scoring (if enabled).")

    msg = Message(subject=subject, recipients=[to])
    msg.body = "\n".join(body_lines) if body_lines else "Please find your LOSS assessment report attached."
    msg.attach(
        f"LOSS_Assessment_Run_{run_id}.pdf",
        "application/pdf",
        pdf_bytes,
    )

    # --- send ---
    mail.send(msg)

    flash("Report emailed successfully.", "success")

    # for security, mirror your “finish up” flow: sign out / redirect as you prefer
    # if you have a logout route, redirect there; else go back to exit page.
    return redirect(url_for("loss_bp.report_exit", run_id=run_id, user_id=user_id))


@loss_bp.post("/report/finish", endpoint="finish_report")
@login_required
def finish_report():
    run_id  = request.form.get("run_id")  or request.args.get("run_id")
    user_id = request.form.get("user_id") or request.args.get("user_id")
    email   = (request.form.get("email") or request.args.get("email") or "").strip().lower()

    try:
        run_id, user_id = int(run_id), int(user_id)
    except Exception:
        return render_template(
            "subject/loss/report_exit.html",
            run_id=run_id, user_id=user_id,
            default_email=email or (session.get("email") or ""),
            error="Missing run/user. Please go back and try again."
        ), 400

    # Single call: completes + builds + emails (email from form takes precedence)
    result = handle_exit_actions(user_id=user_id, subject_slug="loss", run_id=run_id, email=email)
    artifact_url = result.get("artifact_url") or url_for(
        "loss_bp.report_pdf", run_id=run_id, user_id=user_id, _external=True
    )

    # Logout + clear
    try: logout_user()
    except Exception: pass
    try: session.clear()
    except Exception: pass

    # Close tab; fallback link
    try:
        welcome_url = url_for("auth_bp.welcome", _external=True)
    except Exception:
        welcome_url = (request.url_root or "/").rstrip("/")

    return f"""<!doctype html>
<html>
  <head><meta charset="utf-8"><title>Finishing…</title></head>
  <body>
    <script>
      (function() {{
        try {{ window.open({artifact_url!r}, "_blank"); }} catch(e) {{}}
        setTimeout(function() {{
          try {{ window.close(); }} catch(e) {{}}
          setTimeout(function() {{
            try {{ window.location.replace({welcome_url!r}); }} catch(e) {{}}
          }}, 400);
        }}, 400);
      }})();
    </script>
    <noscript>
      <p>Report ready — <a href="{artifact_url}" target="_blank" rel="noopener">Open</a>. You may now close this tab.</p>
      <p><a href="{welcome_url}">Return to welcome</a></p>
    </noscript>
  </body>
</html>
""", 200, {"Content-Type": "text/html; charset=utf-8"}


@loss_bp.get("/report/exit", endpoint="report_exit")
@login_required
def report_exit():
    run_id  = request.args.get("run_id", type=int)
    user_id = request.args.get("user_id", type=int)
    default_email = (session.get("email") or "").strip().lower()

    return render_template(
        "subject/loss/report_exit.html",
        run_id=run_id,
        user_id=user_id,
        default_email=default_email,
        error=None,
    )



def _get_run_user_email(run_id: int | None, user_id: int | None) -> str | None:
    try:
        try:
            from app.models.loss import LcaRun
        except Exception:
            LcaRun = None
        try:
            from app.models.auth import User
        except Exception:
            try:
                from app.models.auth import User
            except Exception:
                User = None

        if LcaRun is not None and run_id:
            run = db.session.get(LcaRun, run_id)
            if run:
                uid = getattr(run, "user_id", None)
                if uid and User is not None:
                    u = db.session.get(User, uid)
                    if u:
                        return (getattr(u, "email", None) or getattr(u, "username", None))

        if User is not None and user_id:
            u = db.session.get(User, user_id)
            if u:
                return (getattr(u, "email", None) or getattr(u, "username", None))
    except Exception:
        pass
    return None


def _close_loss_enrollment(user_id: int | None) -> None:
    """
    Best-effort: mark the user's LOSS enrollment as closed/completed so
    future re-enrol won't get blocked by 'already enrolled' checks.
    Tries to locate auth_enrollment via subject slug/name.
    Safe no-op if tables/columns differ.
    """
    try:
        # Try common model locations
        try:
            from app.models.auth import UserEnrollment, AuthSubject  # if you have these
        except Exception:
            UserEnrollment = None
            AuthSubject = None

        if not user_id or UserEnrollment is None:
            return

        # Find LOSS subject id if a subject table exists; otherwise update by program/subject text
        subject_id = None
        if AuthSubject is not None:
            loss = (
                db.session.query(AuthSubject)
                .filter(
                    (getattr(AuthSubject, "slug", None) == "loss") |
                    (getattr(AuthSubject, "name", None) == "LOSS")
                ).first()
            )
            if loss:
                subject_id = getattr(loss, "id", None)

        q = db.session.query(UserEnrollment).filter(UserEnrollment.user_id == user_id)
        if subject_id is not None and hasattr(UserEnrollment, "subject_id"):
            q = q.filter(UserEnrollment.subject_id == subject_id)
        elif hasattr(UserEnrollment, "program"):
            q = q.filter(UserEnrollment.program.in_(["loss", "LOSS"]))

        row = q.first()
        if not row:
            return

        # Set a 'closed' state in the most compatible way
        if hasattr(row, "status"):
            row.status = "closed"
        if hasattr(row, "completed"):
            try:
                row.completed = 1
            except Exception:
                row.completed = True

        db.session.commit()
    except Exception:
        db.session.rollback()  # don't block the finish flow if this fails



def _build_pdf_bytes(run_id: int, user_id: int) -> bytes | None:
    try:
        pdf_url = url_for("loss_bp.report_pdf", run_id=run_id, user_id=user_id, auto_print=0)
        with current_app.test_request_context(pdf_url, method="GET"):
            pdf_resp = current_app.ensure_sync(current_app.view_functions["loss_bp.report_pdf"])()
        resp = pdf_resp[0] if isinstance(pdf_resp, tuple) else pdf_resp
        if getattr(resp, "direct_passthrough", False):
            resp.direct_passthrough = False
        return resp.get_data()
    except Exception:
        current_app.logger.error("PDF build failed for run_id=%s user_id=%s\n%s",
                                 run_id, user_id, traceback.format_exc())
        return None




# ---------------------------
# Utilities
# ---------------------------


def _endpoint_exists(name: str) -> bool:
    return name in current_app.view_functions

def _build_loss_pdf_and_get_url(run_id: int, user_id: int) -> str:
    """
    Return an ABSOLUTE URL to the report PDF for this run/user.
    We don't guess your internal builder; instead we detect a PDF route you likely already have.
    Supported (first match wins):
      - loss_bp.report_pdf
      - loss_bp.report_download
      - loss_bp.report_view (with ?fmt=pdf)
    """
    # 1) /loss/report/pdf
    if _endpoint_exists("loss_bp.report_pdf"):
        return url_for("loss_bp.report_pdf", run_id=run_id, user_id=user_id, _external=True)

    # 2) /loss/report/download
    if _endpoint_exists("loss_bp.report_download"):
        return url_for("loss_bp.report_download", run_id=run_id, user_id=user_id, _external=True)

    # 3) /loss/report/view?fmt=pdf
    if _endpoint_exists("loss_bp.report_view"):
        return url_for("loss_bp.report_view", run_id=run_id, user_id=user_id, fmt="pdf", _external=True)

    # If none of the above exist, raise a clear error so you can wire one quickly.
    raise RuntimeError(
        "No PDF endpoint found. Please expose one of: "
        "loss_bp.report_pdf, loss_bp.report_download, or loss_bp.report_view(fmt=pdf)."
    )

def _send_loss_report_email_async(to_email: str, run_id: int, user_id: int, pdf_url: str) -> None:
    """
    Fire-and-forget email send so the request never blocks.
    Tries a few common locations for your existing mailer; if none found, logs a warning.
    """
    def _task():
        try:
            sender = None
            # Try common import paths you might already have:
            try:
                from app.utils.mailer import send_loss_report_email as sender  # noqa
            except Exception:
                pass
            if sender is None:
                try:
                    from app.utils.mailer  import send_loss_report_email as sender  # noqa
                except Exception:
                    pass
            if sender is None:
                try:
                    from app.utils.mailer  import send_loss_report_email as sender  # noqa
                except Exception:
                    pass

            if sender is None:
                current_app.logger.warning(
                    "No send_loss_report_email() function found; skipping email for run_id=%s user_id=%s to=%s",
                    run_id, user_id, to_email
                )
                return

            # Use your existing signature: adjust if yours differs.
            sender(to=to_email, run_id=run_id, user_id=user_id, pdf_url=pdf_url)

        except Exception as e:
            current_app.logger.exception("loss email async send failed: %s", e)

    Thread(target=_task, daemon=True).start()

# ---------------------------
# Finish flow
# ---------------------------



def _send_loss_report_email_async(to_email: str, run_id: int, user_id: int, pdf_url: str) -> None:
    """Fire-and-forget email send, with a proper Flask app context inside the thread."""
    # Capture the real app object while we're still in request/app context
    app = current_app._get_current_object()

    def _task():
        # Push app context for Flask extensions (logger, Flask-Mail, etc.)
        with app.app_context():
            try:
                # Import your sender here (inside context is fine)
                try:
                    from app.utils.mailer import send_loss_report_email as sender  # <-- your shared mailer
                except Exception:
                    sender = None

                if sender is None:
                    app.logger.warning(
                        "No send_loss_report_email() found; skipping email for run_id=%s user_id=%s to=%s",
                        run_id, user_id, to_email
                    )
                    return

                # Call your existing mailer
                sender(to=to_email, run_id=run_id, user_id=user_id, pdf_url=pdf_url)
                app.logger.info("Loss report email queued/sent to %s (run=%s user=%s)", to_email, run_id, user_id)

            except Exception as e:
                app.logger.exception("loss email async send failed: %s", e)

    Thread(target=_task, daemon=True).start()





def _send_loss_report_email_async(to_email: str, run_id: int, user_id: int, pdf_url: str, learner_name: str | None = None) -> None:
    """Background email with proper Flask app context so nothing crashes."""
    app = current_app._get_current_object()

    def _task():
        with app.app_context():
            try:
                _send_mail(to=to_email, run_id=run_id, user_id=user_id, pdf_url=pdf_url, learner_name=learner_name)
            except Exception as e:
                app.logger.exception("loss email async send failed: %s", e)

    Thread(target=_task, daemon=True).start()



def _complete_loss_enrollment_sql(user_id: int) -> None:
    """Mark the user's LOSS enrollment as completed in user_enrollment (safe + idempotent)."""
    if not user_id:
        return
    try:
        sid = db.session.execute(
            text("""
                SELECT id FROM auth_subject
                 WHERE lower(slug)='loss' OR lower(name)='loss'
                 LIMIT 1
            """)
        ).scalar()
        if not sid:
            current_app.logger.warning("LOSS subject not found — cannot complete enrollment")
            return

        # direct update on user_enrollment
        result = db.session.execute(
            text("""
                UPDATE user_enrollment
                   SET status='completed',
                       completed_at = COALESCE(completed_at, CURRENT_TIMESTAMP)
                 WHERE user_id=:uid
                   AND subject_id=:sid
                   AND (status IS NULL OR lower(status)!='completed')
            """),
            {"uid": user_id, "sid": sid},
        )
        db.session.commit()
        current_app.logger.info(
            "LOSS enrollment update result=%s for user_id=%s sid=%s",
            result.rowcount, user_id, sid
        )
    except Exception as e:
        db.session.rollback()
        current_app.logger.exception("complete_loss_enrollment_sql failed: %s", e)


