from flask import (
    Response, abort, json, request, redirect, url_for, 
    render_template, session,send_file, current_app, flash)
from flask_login import current_user
from app.admin import admin_bp
from flask_login import logout_user
from werkzeug.routing import BuildError
from app.admin.loss.index import _safe_url


from app.admin.seed_utils import import_csv_stream
from app.extensions import db
from sqlalchemy import bindparam, text, inspect, func, and_
from app.admin.loss.utils import get_run_id, with_run_id_in_ctx

from app.mailer import send_mail
from app.subject_loss import scoring_import
from flask import current_app, make_response
from flask_mail import Message
from app.extensions import mail
from jinja2 import TemplateNotFound

from app.seed.seed_simple import SEEDS, save_from_form

#from app.subject.loss.charts import phase_scores_bar
from app.utils.loss_service import (
    create_run, persist_results_row, responses_for_run
)
from io import BytesIO
import smtplib, ssl
from email.message import EmailMessage

try:
    from weasyprint import HTML
    WEASYPRINT_AVAILABLE = True
except Exception:
    WEASYPRINT_AVAILABLE = False

import math
import os, sys
if sys.platform == "win32":
    for d in (
        r"C:\msys64\mingw64\bin",                 # if you have MSYS2
        r"C:\Program Files\GTK3-Runtime Win64\bin" # if you used the GTK3 runtime installer
    ):
        if os.path.isdir(d):
            try:
                os.add_dll_directory(d)  # Python 3.8+
            except Exception:
                # fallback: extend PATH for older loaders
                os.environ["PATH"] = d + os.pathsep + os.environ.get("PATH", "")
            break
import os, sys, io
import re
from app.admin.loss.utils import get_run_id, with_run_id_in_ctx,compute_adaptive_vector
from datetime import datetime, time
from sqlalchemy.exc import OperationalError
from app.models.loss import LcaResult,LcaRun
from sqlalchemy import select, func, desc
from types import SimpleNamespace

from types import SimpleNamespace
from flask import request, render_template, url_for, current_app as app
from decimal import Decimal, ROUND_FLOOR
from werkzeug.routing import BuildError
import math, shutil
#from weasyprint import HTML
from .phase_item import (
    build_phase_blocks,
    adaptive_vector_from_phases,
    overall_assessment_from_p1,
    DISCLAIMER_TEXT,
)
from app.utils.mailer import send_pdf_email  # ← use helper instead
from pathlib import Path

from sqlalchemy import text, inspect
from app.admin.seed_utils import (
    SEED_TABLES, canon_seed, preview_rows,
    import_csv_stream
)
from app.admin.seed_helper import (
    SEED_CFG,
    fetch_rows as fetch_rows_db,
    seed_csv_path,
)
from app.seed.seed_simple import SEEDS as SEED_CFG, fetch_rows  # existing helpers
from app.seed.seed_simple import SEEDS, fetch_rows   # make sure this import exists
from flask_wtf.csrf import generate_csrf  # import once at top

import io, csv

from app.admin.seed_utils import (
    SEED_TABLES,        # {"questions": {"model": ModelClass, "columns": [...]}, ...}
    canon_seed,         # normalizes / validates the seed key
    import_csv_stream,  # handles CSV -> DB import
    preview_rows,       # returns preview data (list[dict] or list[model])
    import_from_repo,   # optional: pull a CSV from repo
)
from app.models.loss import (
    LcaQuestion,        # questions
    LcaPhaseItem,       # phase items
    LcaProgressItem,    # progress items
    LcaOverallItem,     # overall items
    LcaInstruction,     # introduction cards       <-- add this model
    LcaExplain,         # explain cards            <-- add this model
    LcaPause,           # pause cards              <-- add this model
)

from app.admin.seed_helper import (
    SEED_CFG, SEED_DIR, best_loss_back_url, db_rows_fallback, fetch_rows, map_rows_from_db, mget, mset, normalize_cols, read_csv, resolve_seed_key, seed_csv_path, seed_import_csv_path, seed_preview_rows, seed_export_csv_response, seed_import_csv_stream,
    registry_meta, write_csv
)
import os, shutil, glob

@admin_bp.get("/loss/assessment", endpoint="loss_assessment")
def loss_assessment():
    """Placeholder page to preview the LOSS assessment (admin view)."""
    return render_template("admin/loss/assessment.html")

@admin_bp.get("/loss/results", endpoint="loss_results")
def loss_results():
    """Placeholder page for viewing submitted results (admin view)."""
    return render_template("admin/loss/results.html")

@admin_bp.route("/loss/tool/<slug>", methods=["GET"])
def loss_placeholder(slug: str):
    return render_template("admin/loss/placeholder.html", slug=slug)

#new starting point

def _current_user_id():
    return session.get("user_id")

def _phase_totals_for_user(user_id):
    # Raw points by phase for the user's latest run (no run scoping yet)
    sql = """
    SELECT
      COALESCE(SUM(m.phase_1),0) AS p1,
      COALESCE(SUM(m.phase_2),0) AS p2,
      COALESCE(SUM(m.phase_3),0) AS p3,
      COALESCE(SUM(m.phase_4),0) AS p4
    FROM lca_response r
    JOIN lca_scoring_map m
      ON m.question_id = r.question_id
     AND m.answer_type = r.answer
    WHERE r.user_id = :uid
    """
    row = db.session.execute(text(sql), {"uid": user_id}).mappings().first()
    return (row["p1"], row["p2"], row["p3"], row["p4"]) if row else (0,0,0,0)

def _phase_maxima_from_map():
    # Derive maxima from the map: for each question take MAX per phase across answers, then sum.
    sql = """
    SELECT
      SUM(max_p1) AS max1,
      SUM(max_p2) AS max2,
      SUM(max_p3) AS max3,
      SUM(max_p4) AS max4
    FROM (
      SELECT
        question_id,
        MAX(phase_1) AS max_p1,
        MAX(phase_2) AS max_p2,
        MAX(phase_3) AS max_p3,
        MAX(phase_4) AS max_p4
      FROM lca_scoring_map
      GROUP BY question_id
    ) t
    """
    row = db.session.execute(text(sql)).mappings().first()
    return (row["max1"] or 0, row["max2"] or 0, row["max3"] or 0, row["max4"] or 0)

def _percent(raw, mx):
    return int(round((raw * 100.0) / mx)) if mx else 0


# admin routes (or a shared helpers file)


def _pick_uid():
    # priority: ?uid= → session flag (set when finishing a run) → most recent user in lca_response
    uid = request.args.get("uid", type=int)
    if uid:
        return uid
    uid = session.get("loss_last_uid")
    if uid:
        return uid
    row = db.session.execute(text("""
        SELECT user_id FROM lca_response ORDER BY id DESC LIMIT 1
    """)).first()
    return row[0] if row else None



@admin_bp.get("/loss/dev/runs")
def admin_loss_dev_runs():
    rid = session.get("loss_run_id")
    rows = db.session.execute(text("""
        SELECT id, user_id, status, started_at, finished_at
        FROM lca_run
        ORDER BY id DESC
        LIMIT 10
    """)).mappings().all()
    lines = [f"session.run_id={rid}"]
    lines += [f"id={r['id']} user={r['user_id']} status={r['status']} "
              f"{r['started_at']} -> {r['finished_at']}" for r in rows]
    return "\n".join(lines), 200, {"Content-Type": "text/plain"}

def _pick_uid(): return request.args.get("uid", type=int) or _current_user_id()

def _pick_run_id(uid:int):
    rid = request.args.get("run_id", type=int)
    if rid: return rid
    return db.session.execute(text("""
        SELECT id FROM lca_run WHERE user_id=:uid ORDER BY id DESC LIMIT 1
    """), {"uid": uid}).scalar()

@admin_bp.get("/loss/scorecard")
def loss_scorecard_admin():
    uid = _pick_uid()
    rid = _pick_run_id(uid)
    row = db.session.execute(text("""
        SELECT p1_raw, p2_raw, p3_raw, p4_raw FROM lca_result WHERE run_id=:rid
    """), {"rid": rid}).mappings().first()
    if not row:
        # fallback compute (rare, e.g. older runs)
        row = db.session.execute(text("""
            SELECT
              SUM(CASE WHEN lower(r.answer)='yes' THEN sd.p1 ELSE 0 END) AS p1_raw,
              SUM(CASE WHEN lower(r.answer)='yes' THEN sd.p2 ELSE 0 END) AS p2_raw,
              SUM(CASE WHEN lower(r.answer)='yes' THEN sd.p3 ELSE 0 END) AS p3_raw,
              SUM(CASE WHEN lower(r.answer)='yes' THEN sd.p4 ELSE 0 END) AS p4_raw
            FROM lca_response r
            JOIN lca_score_definitions sd ON sd.question_id = r.question_id
            WHERE r.run_id = :rid
        """), {"rid": rid}).mappings().first()

    maxima = db.session.execute(text("""
        SELECT SUM(p1) AS p1_max, SUM(p2) AS p2_max,
               SUM(p3) AS p3_max, SUM(p4) AS p4_max
        FROM lca_score_definitions
    """)).mappings().first()

    pct = lambda raw, mx: int(round((raw or 0) * 100 / (mx or 1)))
    data = {
      "p1": {"raw": row["p1_raw"] or 0, "max": maxima["p1_max"] or 0},
      "p2": {"raw": row["p2_raw"] or 0, "max": maxima["p2_max"] or 0},
      "p3": {"raw": row["p3_raw"] or 0, "max": maxima["p3_max"] or 0},
      "p4": {"raw": row["p4_raw"] or 0, "max": maxima["p4_max"] or 0},
    }
    for v in data.values(): v["pct"] = pct(v["raw"], v["max"])
    return render_template("admin/loss/scorecard.html", data=data, uid=uid, run_id=rid)




# Start a run from Admin (then you can click into /loss/start to do the test)
@admin_bp.post("/loss/start")
def admin_loss_start():
    uid = request.form.get("uid", type=int) or session.get("user_id", 1)
    rid = create_run(uid)
    # Option A: take user into assessment flow
    return redirect(url_for("loss_bp.start_sequence"))
    # Option B: stay in Admin and view this run:
    # return redirect(url_for("admin_bp.loss_home", uid=uid, run_id=rid))


# Optional: materialize a results row after a run finishes
@admin_bp.post("/loss/persist")
def admin_loss_persist():
    rid = request.form.get("run_id", type=int)
    if rid: persist_results_row(rid)
    return redirect(url_for("admin_bp.loss_home", run_id=rid))

# --- ONE-TIME MIGRATION: fix lca_response uniqueness to (run_id, question_id) ---


@admin_bp.post("/loss/dev/migrate_response_unique")
def migrate_response_unique():
    try:
        # 0) Show where we are writing
        url = str(db.engine.url)
        current_app.logger.warning("MIGRATE on DB: %s", url)

        # 1) Drop broken view if present (it was blocking renames earlier)
        db.session.execute(text("DROP VIEW IF EXISTS approved_admins;"))

        # 2) Inspect current DDL
        ddl = db.session.execute(text("""
            SELECT sql
            FROM sqlite_master
            WHERE type='table' AND name='lca_response'
        """)).scalar() or ""

        # If the table already has no UNIQUE(user_id, question_id), skip
        if "unique" in ddl.lower() and "user_id" in ddl.lower() and "question_id" in ddl.lower():
            current_app.logger.warning("Table has user-based UNIQUE; migrating...")

            # PRAGMAs
            db.session.execute(text("PRAGMA foreign_keys=OFF;"))
            db.session.execute(text("PRAGMA legacy_alter_table=ON;"))

            # A) new table without the old UNIQUE
            db.session.execute(text("""
                CREATE TABLE lca_response_new (
                  id          INTEGER PRIMARY KEY,
                  user_id     INTEGER NOT NULL,
                  run_id      INTEGER NOT NULL,
                  question_id INTEGER NOT NULL,
                  answer      VARCHAR(3) NOT NULL,
                  created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
                  FOREIGN KEY(question_id) REFERENCES lca_question (id) ON DELETE CASCADE
                )
            """))

            # B) copy data
            db.session.execute(text("""
                INSERT INTO lca_response_new (id, user_id, run_id, question_id, answer, created_at)
                SELECT id, user_id, run_id, question_id, answer, created_at
                FROM lca_response
            """))

            # C) swap tables
            db.session.execute(text("DROP TABLE lca_response;"))
            db.session.execute(text("ALTER TABLE lca_response_new RENAME TO lca_response;"))

            # D) correct unique + helpful indexes
            db.session.execute(text("""
                CREATE UNIQUE INDEX IF NOT EXISTS ux_response_run_q
                ON lca_response(run_id, question_id)
            """))
            db.session.execute(text("CREATE INDEX IF NOT EXISTS ix_response_user ON lca_response(user_id)"))
            db.session.execute(text("CREATE INDEX IF NOT EXISTS ix_response_run  ON lca_response(run_id)"))

            # restore PRAGMAs
            db.session.execute(text("PRAGMA legacy_alter_table=OFF;"))
            db.session.execute(text("PRAGMA foreign_keys=ON;"))

            db.session.commit()
            return ("OK: migrated lca_response to UNIQUE(run_id,question_id). "
                    "Check Admin → Responses again."), 200

        else:
            # ensure the run-based unique exists anyway
            db.session.execute(text("""
                CREATE UNIQUE INDEX IF NOT EXISTS ux_response_run_q
                ON lca_response(run_id, question_id)
            """))
            db.session.commit()
            return "SKIP: table already lacks user-based UNIQUE. Ensured ux_response_run_q exists.", 200

    except Exception as e:
        db.session.rollback()
        current_app.logger.exception("Migration failed")
        return f"ERROR: {e}", 500

@admin_bp.get("/loss/dev/schema")
def loss_dev_schema():
    from io import StringIO
    out = StringIO()
    url = str(db.engine.url)
    out.write(f"DB={url}\n\n")

    ddl = db.session.execute(text("""
        SELECT sql FROM sqlite_master WHERE type='table' AND name='lca_response'
    """)).scalar()
    out.write("lca_response DDL:\n")
    out.write((ddl or "(none)") + "\n\n")

    idx_list = db.session.execute(text("PRAGMA index_list('lca_response')")).all()
    out.write("Indexes:\n")
    for row in idx_list:
        name = row[1]
        unique = row[2]
        cols = db.session.execute(text(f"PRAGMA index_info('{name}')")).all()
        cols_str = ", ".join([c[2] for c in cols])
        out.write(f" - {name} (unique={unique}) -> [{cols_str}]\n")

    return out.getvalue(), 200, {"Content-Type": "text/plain"}



def _resolve_uid_rid():
    uid = request.args.get("uid", type=int) or session.get("user_id")
    rid = request.args.get("run_id", type=int) or session.get("loss_run_id")
    if not rid and uid:
        rid = db.session.execute(text(
            "SELECT id FROM lca_run WHERE user_id=:uid ORDER BY id DESC LIMIT 1"
        ), {"uid": int(uid)}).scalar()
    return int(uid) if uid is not None else None, int(rid) if rid is not None else None

@admin_bp.get("/loss/scorecard/latest")
def loss_scorecard_latest():
    uid = request.args.get("uid", type=int) or session.get("user_id")
    rid = db.session.execute(text("""
        SELECT id FROM lca_run WHERE user_id=:uid
        ORDER BY id DESC LIMIT 1
    """), {"uid": int(uid)}).scalar()
    return redirect(url_for("admin_bp.loss_scorecard", uid=uid, run_id=rid))


# ---------- helpers ----------
def resolve_uid_rid():
    uid = request.args.get("uid", type=int) or session.get("user_id")
    rid = request.args.get("run_id", type=int) or session.get("loss_run_id")
    if not rid and uid:
        rid = db.session.execute(text("""
            SELECT id FROM lca_run WHERE user_id=:uid AND subject='loss'
            ORDER BY id DESC LIMIT 1
        """), {"uid": int(uid)}).scalar()
    return (int(uid) if uid is not None else None,
            int(rid) if rid is not None else None)

@admin_bp.get("/loss/scorecard")
def loss_scorecard():
    uid, rid = resolve_uid_rid()
    # prefer final snapshot if exists, else live compute from scorecard
    snap = db.session.execute(text("""
        SELECT phase_1, phase_2, phase_3, phase_4, score_total, completed_at
        FROM lca_result WHERE run_id=:rid
    """), {"rid": rid}).mappings().first()

    if snap:
        sc = snap
    else:
        sc = db.session.execute(text("""
            SELECT
              SUM(phase_1) AS phase_1,
              SUM(phase_2) AS phase_2,
              SUM(phase_3) AS phase_3,
              SUM(phase_4) AS phase_4,
              SUM(phase_1+phase_2+phase_3+phase_4) AS score_total,
              MAX(created_at) AS completed_at
            FROM lca_scorecard WHERE run_id=:rid
        """), {"rid": rid}).mappings().first()

    return render_template("admin/loss/scorecard.html", sc=sc, uid=uid, run_id=rid)

# Optional: convenience start from Admin
@admin_bp.get("/loss/start-test")
def loss_start_test():
    # just bounce to the runner's start; runner will set session['loss_run_id']
    
    return redirect(url_for("loss_bp.course_start"))

def _compute_result_from_responses(run_id: int) -> dict | None:
    agg = db.session.execute(text("""
        SELECT
          COALESCE(run.user_id, MIN(r.user_id))                      AS user_id,
          COALESCE(run.subject, 'loss')                              AS subject,
          SUM(COALESCE(m.phase_1,0))                                 AS p1,
          SUM(COALESCE(m.phase_2,0))                                 AS p2,
          SUM(COALESCE(m.phase_3,0))                                 AS p3,
          SUM(COALESCE(m.phase_4,0))                                 AS p4,
          SUM(COALESCE(m.phase_1,0)+COALESCE(m.phase_2,0)+
              COALESCE(m.phase_3,0)+COALESCE(m.phase_4,0))           AS total,
          MAX(r.created_at)                                          AS created_at
        FROM lca_response r
        LEFT JOIN lca_question_phase_map m
               ON m.question_id = r.question_id
              AND m.answer_type = LOWER(r.answer)
        LEFT JOIN lca_run run
               ON run.id = r.run_id
        WHERE r.run_id = :rid
    """), {"rid": run_id}).mappings().first()

    if not agg or agg["user_id"] is None:
        return None

    params = {
        "rid": run_id,
        "uid": int(agg["user_id"]),
        "subj": agg["subject"],
        "p1": int(agg["p1"] or 0),
        "p2": int(agg["p2"] or 0),
        "p3": int(agg["p3"] or 0),
        "p4": int(agg["p4"] or 0),
        "tot": int(agg["total"] or 0),
        "ts":  agg["created_at"],
    }

    # Try UPDATE first
    upd = db.session.execute(text("""
        UPDATE lca_result
           SET user_id = :uid,
               subject = :subj,
               phase_1 = :p1,
               phase_2 = :p2,
               phase_3 = :p3,
               phase_4 = :p4,
               total   = :tot,
               created_at = :ts
         WHERE run_id = :rid
    """), params)

    if upd.rowcount == 0:
        # No row → INSERT
        db.session.execute(text("""
            INSERT INTO lca_result
              (run_id, user_id, subject, phase_1, phase_2, phase_3, phase_4, total, created_at)
            VALUES
              (:rid, :uid, :subj, :p1, :p2, :p3, :p4, :tot, :ts)
        """), params)

    db.session.commit()
    return params

# Helper: get run_id from querystring, else latest run for this user in LOSS
def _get_run_id_or_latest():
    rid = request.args.get("run_id", type=int)
    if rid:
        return rid
    uid = session.get("user_id")
    if not uid:
        return None
    # fallback to latest run for this user (subject=loss if you store it)
    return db.session.execute(text("""
        SELECT id FROM lca_run
        WHERE user_id = :uid
        ORDER BY id DESC
        LIMIT 1
    """), {"uid": uid}).scalar()

'''
@admin_bp.route("/loss/result/recompute", methods=["POST"])
def loss_result_recompute():
    rid = request.args.get("run_id", type=int)
    if not rid:
        return "run_id required", 400

    # Build (or refresh) totals from lca_response only
    db.session.execute(text("""
        INSERT INTO lca_result
          (run_id, user_id, subject, phase_1, phase_2, phase_3, phase_4, total, created_at)
        SELECT
          r.run_id,
          COALESCE(MIN(run.user_id), 0),
          COALESCE(MIN(run.subject), 'loss'),
          SUM(COALESCE(m.phase_1,0)),
          SUM(COALESCE(m.phase_2,0)),
          SUM(COALESCE(m.phase_3,0)),
          SUM(COALESCE(m.phase_4,0)),
          SUM(COALESCE(m.phase_1,0)+COALESCE(m.phase_2,0)+COALESCE(m.phase_3,0)+COALESCE(m.phase_4,0)),
          MAX(r.created_at)
        FROM lca_response r
        LEFT JOIN lca_question_phase_map m
               ON m.question_id = r.question_id
              AND m.answer_type = LOWER(r.answer)
        LEFT JOIN lca_run run
               ON run.id = r.run_id
        WHERE r.run_id = :rid
          AND LOWER(r.answer) IN ('yes','no')
        GROUP BY r.run_id
        ON CONFLICT(run_id) DO UPDATE SET
          user_id    = excluded.user_id,
          subject    = excluded.subject,
          phase_1    = excluded.phase_1,
          phase_2    = excluded.phase_2,
          phase_3    = excluded.phase_3,
          phase_4    = excluded.phase_4,
          total      = excluded.total,
          created_at = excluded.created_at
    """), {"rid": rid})
    db.session.commit()
    return redirect(url_for("admin_bp.loss_result", run_id=rid))

'''


def _fetch_result_row(run_id: int):
    row = db.session.execute(text("""
        SELECT run_id, user_id, subject,
               phase_1, phase_2, phase_3, phase_4,
               total, created_at
        FROM lca_result
        WHERE run_id = :rid
    """), {"rid": run_id}).mappings().first()
    return row


def _render_report_html(row):
    # Reuse a clean report template below
    return render_template("admin/loss/report.html", row=row)


def _make_pdf(html_str: str) -> bytes | None:
    if not WEASYPRINT_AVAILABLE:
        return None
    pdf_bytes = HTML(string=html_str).write_pdf()
    return pdf_bytes


def _send_email_with_attachment(to_addr: str, subject: str, body_html: str,
                                filename: str, content_bytes: bytes | None,
                                mimetype: str = "application/pdf") -> tuple[bool, str]:
    """
    Sends an email using SMTP config in Flask app config:
      MAIL_SERVER, MAIL_PORT, MAIL_USERNAME, MAIL_PASSWORD, MAIL_USE_TLS/SSL, MAIL_DEFAULT_SENDER
    If PDF isn't available, sends HTML body without attachment.
    Returns (ok, message)
    """
    cfg = current_app.config
    server = cfg.get("MAIL_SERVER")
    if not server:
        return False, "MAIL_SERVER not configured; skipped email."

    port = int(cfg.get("MAIL_PORT", 587))
    username = cfg.get("MAIL_USERNAME")
    password = cfg.get("MAIL_PASSWORD")
    use_tls = bool(cfg.get("MAIL_USE_TLS", True))
    use_ssl = bool(cfg.get("MAIL_USE_SSL", False))
    sender = cfg.get("MAIL_DEFAULT_SENDER") or username

    if not sender:
        return False, "No sender address configured; skipped email."

    msg = EmailMessage()
    msg["From"] = sender
    msg["To"] = to_addr
    msg["Subject"] = subject
    msg.set_content("Your LOSS report is attached. If you can’t see it, view the HTML version.")
    msg.add_alternative(body_html, subtype="html")

    if content_bytes:
        msg.add_attachment(content_bytes, maintype=mimetype.split("/")[0],
                           subtype=mimetype.split("/")[1], filename=filename)

    try:
        if use_ssl:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(server, port, context=context) as s:
                if username and password:
                    s.login(username, password)
                s.send_message(msg)
        else:
            with smtplib.SMTP(server, port) as s:
                if use_tls:
                    s.starttls(context=ssl.create_default_context())
                if username and password:
                    s.login(username, password)
                s.send_message(msg)
        return True, "Email sent."
    except Exception as e:
        return False, f"Email failed: {e}"



def _coerce_dt(x):
    if not x:
        return None
    if isinstance(x, datetime):
        return x
    s = str(x).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

def _fmt_dt_label(x, pattern="%Y-%m-%d %H:%M"):
    dt = _coerce_dt(x)
    return dt.strftime(pattern) if dt else (str(x) if x else "")

def _fmt_dt_label(x, pattern="%Y-%m-%d %H:%M"):
    dt = _coerce_dt(x)
    return dt.strftime(pattern) if dt else (str(x) if x else "")


def _safe_commit(retries: int = 3, base_sleep: float = 0.2):
    """Commit with tiny backoff to avoid 'database is locked' on SQLite."""
    for i in range(retries):
        try:
            db.session.commit()
            return
        except OperationalError as e:
            db.session.rollback()
            if i == retries - 1:
                raise
            time.sleep(base_sleep * (i + 1))



def _compute_result_from_run(rid: int):
    """
    Compute phase totals from lca_response LEFT JOIN lca_scoring_map for a run.
    Normalizes answers to 'yes'/'no'. Returns dict or None if no responses.
    """
    sql = """
        WITH resp AS (
          SELECT
            r.user_id,
            r.question_id,
            r.created_at,
            CASE
              WHEN LOWER(TRIM(r.answer)) IN ('1','y','yes','true','t') THEN 'yes'
              WHEN LOWER(TRIM(r.answer)) IN ('0','n','no','false','f','') THEN 'no'
              ELSE LOWER(TRIM(r.answer))
            END AS norm_answer
          FROM lca_response r
          WHERE r.run_id = :rid
        )
        SELECT
          MAX(resp.user_id)                                       AS user_id,
          MAX(resp.created_at)                                    AS latest_ts,
          COUNT(*)                                                AS n_responses,
          COALESCE(SUM(COALESCE(m.phase_1,0)), 0)                 AS p1,
          COALESCE(SUM(COALESCE(m.phase_2,0)), 0)                 AS p2,
          COALESCE(SUM(COALESCE(m.phase_3,0)), 0)                 AS p3,
          COALESCE(SUM(COALESCE(m.phase_4,0)), 0)                 AS p4
        FROM resp
        LEFT JOIN lca_scoring_map m
               ON m.question_id = resp.question_id
              AND m.answer_type = resp.norm_answer
    """
    row = db.session.execute(text(sql), {"rid": rid}).mappings().first()

    if not row or int(row["n_responses"] or 0) == 0:
        return None

    p1 = int(row["p1"] or 0)
    p2 = int(row["p2"] or 0)
    p3 = int(row["p3"] or 0)
    p4 = int(row["p4"] or 0)
    total = p1 + p2 + p3 + p4

    return {
        "run_id": rid,
        "user_id": int(row["user_id"]) if row["user_id"] is not None else None,
        "subject": "LOSS",
        "phase_1": p1,
        "phase_2": p2,
        "phase_3": p3,
        "phase_4": p4,
        "total": total,
        "created_at": row["latest_ts"],
    }


def _upsert_lca_result(res: dict):
    """
    Upsert into lca_result by run_id (SQLite ON CONFLICT). Requires UNIQUE/PK on run_id.
    """
    db.session.execute(text("""
        INSERT INTO lca_result
            (run_id, user_id, subject, phase_1, phase_2, phase_3, phase_4, total, created_at)
        VALUES
            (:run_id, :user_id, :subject, :phase_1, :phase_2, :phase_3, :phase_4, :total, :created_at)
        ON CONFLICT(run_id) DO UPDATE SET
            user_id   = excluded.user_id,
            subject   = excluded.subject,
            phase_1   = excluded.phase_1,
            phase_2   = excluded.phase_2,
            phase_3   = excluded.phase_3,
            phase_4   = excluded.phase_4,
            total     = excluded.total,
            created_at= excluded.created_at
    """), res)
    _safe_commit()


def save_result_row(run_id: int, user_id: int, p1: int, p2: int, p3: int, p4: int, latest_ts, subject: str = "LOSS"):
    total = int(p1) + int(p2) + int(p3) + int(p4)
    params = {"run_id": run_id, "user_id": user_id, "p1": p1, "p2": p2, "p3": p3, "p4": p4,
              "total": total, "created_at": latest_ts, "subject": subject}

    res = db.session.execute(text("""
        UPDATE lca_result
           SET user_id=:user_id, phase_1=:p1, phase_2=:p2, phase_3=:p3, phase_4=:p4,
               total=:total, created_at=:created_at, subject=:subject
         WHERE run_id=:run_id
    """), params)

    if res.rowcount == 0:
        db.session.execute(text("""
            INSERT INTO lca_result (user_id, phase_1, phase_2, phase_3, phase_4, total, created_at, run_id, subject)
            VALUES (:user_id, :p1, :p2, :p3, :p4, :total, :created_at, :run_id, :subject)
        """), params)

    db.session.commit()

def recompute_and_save(run_id: int) -> bool:
    # Prefer the view that powers your Responses page
    vc = db.session.execute(text("""
        SELECT 1 FROM sqlite_master WHERE type IN ('view','table') AND name='lca_scorecard_v'
    """)).first()

    if vc:
        row = db.session.execute(text("""
            SELECT MAX(user_id) AS user_id, MAX(created_at) AS latest_ts, COUNT(*) AS n_rows,
                   COALESCE(SUM(phase_1),0) AS p1, COALESCE(SUM(phase_2),0) AS p2,
                   COALESCE(SUM(phase_3),0) AS p3, COALESCE(SUM(phase_4),0) AS p4
            FROM lca_scorecard_v
            WHERE run_id=:run_id
        """), {"run_id": run_id}).mappings().first()
        if row and int(row["n_rows"] or 0) > 0:
            save_result_row(run_id, int(row["user_id"]), int(row["p1"] or 0), int(row["p2"] or 0),
                            int(row["p3"] or 0), int(row["p4"] or 0), row["latest_ts"])
            return True

    # Fallback: join responses to scoring map (normalize yes/no)
    row2 = db.session.execute(text("""
        WITH resp AS (
          SELECT r.run_id, r.user_id, r.question_id, r.created_at,
                 CASE
                   WHEN LOWER(TRIM(r.answer)) IN ('1','y','yes','true','t') THEN 'yes'
                   WHEN LOWER(TRIM(r.answer)) IN ('0','n','no','false','f','') THEN 'no'
                   ELSE LOWER(TRIM(r.answer))
                 END AS norm_answer
          FROM lca_response r
          WHERE r.run_id=:run_id
        )
        SELECT MAX(resp.user_id) AS user_id, MAX(resp.created_at) AS latest_ts, COUNT(*) AS n_responses,
               COALESCE(SUM(COALESCE(m.phase_1,0)),0) AS p1,
               COALESCE(SUM(COALESCE(m.phase_2,0)),0) AS p2,
               COALESCE(SUM(COALESCE(m.phase_3,0)),0) AS p3,
               COALESCE(SUM(COALESCE(m.phase_4,0)),0) AS p4
        FROM resp
        LEFT JOIN lca_scoring_map m
               ON m.question_id = resp.question_id
              AND m.answer_type = resp.norm_answer
    """), {"run_id": run_id}).mappings().first()

    if not row2 or int(row2["n_responses"] or 0) == 0:
        return False

    save_result_row(run_id, int(row2["user_id"]), int(row2["p1"] or 0), int(row2["p2"] or 0),
                    int(row2["p3"] or 0), int(row2["p4"] or 0), row2["latest_ts"])
    return True

def recompute_and_save_from_responses(run_id: int) -> bool:
    row = db.session.execute(text("""
        WITH resp AS (
          SELECT
            r.run_id,
            r.user_id,
            r.question_id,
            r.created_at,
            CASE
              WHEN LOWER(TRIM(r.answer)) IN ('1','y','yes','true','t') THEN 'yes'
              WHEN LOWER(TRIM(r.answer)) IN ('0','n','no','false','f','') THEN 'no'
              ELSE LOWER(TRIM(r.answer))
            END AS norm_answer
          FROM lca_response r
          WHERE r.run_id = :run_id
        )
        SELECT
          MAX(resp.user_id)                                  AS user_id,
          MAX(resp.created_at)                               AS latest_ts,
          COUNT(*)                                           AS n_responses,
          COALESCE(SUM(COALESCE(m.phase_1,0)), 0)            AS p1,
          COALESCE(SUM(COALESCE(m.phase_2,0)), 0)            AS p2,
          COALESCE(SUM(COALESCE(m.phase_3,0)), 0)            AS p3,
          COALESCE(SUM(COALESCE(m.phase_4,0)), 0)            AS p4
        FROM resp
        LEFT JOIN lca_scoring_map m
               ON m.question_id = resp.question_id
              AND m.answer_type = resp.norm_answer
    """), {"run_id": run_id}).mappings().first()

    if not row or int(row["n_responses"] or 0) == 0:
        return False

    save_result_row(
        run_id=run_id,
        user_id=int)


def upsert_result_from_scorecard(run_id: int) -> bool:
    """
    Aggregate phase totals for a run from lca_scorecard_v and UPSERT into lca_result.
    Falls back to upsert_result_from_responses(run_id) if the view doesn't exist.
    """
    # Does the view exist?
    exists = db.session.execute(text("""
        SELECT name FROM sqlite_master
        WHERE type IN ('view','table') AND name = 'lca_scorecard_v'
    """)).first()
    if not exists:
        # Fallback to the join-based path if you kept it
        return recompute_and_save_from_responses(run_id)

    row = db.session.execute(text("""
        SELECT
          MAX(user_id)                        AS user_id,
          MAX(created_at)                     AS latest_ts,
          COUNT(*)                            AS n_rows,
          COALESCE(SUM(phase_1), 0)           AS p1,
          COALESCE(SUM(phase_2), 0)           AS p2,
          COALESCE(SUM(phase_3), 0)           AS p3,
          COALESCE(SUM(phase_4), 0)           AS p4
        FROM lca_scorecard_v
        WHERE run_id = :run_id
    """), {"run_id": run_id}).mappings().first()

    if not row or int(row["n_rows"] or 0) == 0:
        return False  # truly no lines in the view for this run

    p1 = int(row["p1"] or 0)
    p2 = int(row["p2"] or 0)
    p3 = int(row["p3"] or 0)
    p4 = int(row["p4"] or 0)
    total = p1 + p2 + p3 + p4

    # UPSERT into lca_result
    db.session.execute(text("""
        INSERT INTO lca_result
          (run_id, user_id, subject, phase_1, phase_2, phase_3, phase_4, total, created_at)
        VALUES
          (:run_id, :user_id, 'LOSS', :p1, :p2, :p3, :p4, :total, :created_at)
        ON CONFLICT(run_id) DO UPDATE SET
          user_id    = excluded.user_id,
          subject    = excluded.subject,
          phase_1    = excluded.phase_1,
          phase_2    = excluded.phase_2,
          phase_3    = excluded.phase_3,
          phase_4    = excluded.phase_4,
          total      = excluded.total,
          created_at = excluded.created_at
    """), {
        "run_id": run_id,
        "user_id": row["user_id"],
        "p1": p1, "p2": p2, "p3": p3, "p4": p4,
        "total": total,
        "created_at": row["latest_ts"],
    })
    db.session.commit()
    return True

@admin_bp.route("/loss/run/rebuild")
def loss_run_rebuild():
    run_id = request.args.get("run_id", type=int)
    if not run_id:
        flash("Missing run_id", "warning")
        return redirect(url_for("admin_bp.loss_admin_dashboard"))  # change to your dashboard
    rebuild_loss_run(run_id)
    flash(f"LOSS results recomputed for run {run_id}.", "success")
    return redirect(url_for("admin_bp.loss_result", run_id=run_id))  # your existing Result view

# admin/loss/routes.py

# adjust this import to your actual path



# ---------------------------
# Route: recompute then go to report
# URL: /loss/rebuild?run_id=42
# ---------------------------
# in admin_bp routes.py
@admin_bp.route("/loss/rebuild")
def loss_rebuild():
    run_id = request.args.get("run_id", type=int)
    if not run_id:
        abort(400)
    try:
        info = rebuild_loss_run(run_id)
        flash(f"Rebuilt run {run_id}: responses={info['responses']}, "
              f"scorecard_rows={info['scorecard_rows']}, result_rows={info['result_rows']}", "success")
    except Exception as e:
        flash(f"Error recomputing run {run_id}: {e}", "danger")
    return redirect(url_for("admin_bp.loss_report", run_id=run_id))

SQL_MATERIALIZE_SCORECARD_RUN = text("""
WITH last_resp AS (
  SELECT r.*
  FROM lca_response r
  JOIN (
    SELECT run_id, user_id, question_id, MAX(id) AS max_id
    FROM lca_response
    WHERE run_id = :run_id
    GROUP BY run_id, user_id, question_id
  ) mx ON r.id = mx.max_id
)
INSERT OR REPLACE INTO lca_scorecard_run
  (run_id, user_id, question_id, phase_1, phase_2, phase_3, phase_4, answer, score_total)
SELECT
  lr.run_id,
  lr.user_id,
  lr.question_id,
  COALESCE(v.phase_1, COALESCE(v.p1, 0)),
  COALESCE(v.phase_2, COALESCE(v.p2, 0)),
  COALESCE(v.phase_3, COALESCE(v.p3, 0)),
  COALESCE(v.phase_4, COALESCE(v.p4, 0)),
  lr.answer,
  COALESCE(v.phase_1, COALESCE(v.p1, 0))
+ COALESCE(v.phase_2, COALESCE(v.p2, 0))
+ COALESCE(v.phase_3, COALESCE(v.p3, 0))
+ COALESCE(v.phase_4, COALESCE(v.p4, 0))
FROM last_resp lr
LEFT JOIN lca_scorecard_v v
       ON v.question_id = lr.question_id
""")

SQL_DELETE_RESULT_FOR_RUN = text("DELETE FROM lca_result WHERE run_id = :run_id")

SQL_INSERT_RESULT_FROM_SCORECARD_RUN = text("""
INSERT INTO lca_result
  (user_id, phase_1, phase_2, phase_3, phase_4, total, run_id, subject)
SELECT
  s.user_id,
  SUM(s.phase_1) AS phase_1,
  SUM(s.phase_2) AS phase_2,
  SUM(s.phase_3) AS phase_3,
  SUM(s.phase_4) AS phase_4,
  (SUM(s.phase_1)+SUM(s.phase_2)+SUM(s.phase_3)+SUM(s.phase_4)) AS total,
  :run_id AS run_id,
  'LOSS'  AS subject
FROM lca_scorecard_run s
WHERE s.run_id = :run_id
GROUP BY s.user_id
""")

SQL_UPDATE_RESULT_MAX_COLUMNS = text("""
WITH qset AS (
  SELECT DISTINCT question_id
  FROM lca_scorecard_run
  WHERE run_id = :run_id
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
       max_total   = (SELECT max_p1 + max_p2 + max_p3 + max_p4 FROM mx),
       max_phase_4 = (SELECT max_p4 FROM mx)
 WHERE run_id = :run_id
""")

def rebuild_loss_run(run_id: int):
    """
    Rebuilds run:
      - materialize lca_scorecard_run from latest responses + scorecard_v
      - write totals into lca_result
      - update max_* columns
    Returns diagnostics dict.
    """
    with db.engine.begin() as conn:
        # counts before
        resp_cnt = conn.execute(
            text("SELECT COUNT(*) AS c FROM lca_response WHERE run_id = :run_id"),
            {"run_id": run_id}
        ).scalar_one()

        # materialize
        conn.execute(SQL_MATERIALIZE_SCORECARD_RUN, {"run_id": run_id})

        # count materialized rows
        sc_cnt = conn.execute(
            text("SELECT COUNT(*) AS c FROM lca_scorecard_run WHERE run_id = :run_id"),
            {"run_id": run_id}
        ).scalar_one()

        # result write
        conn.execute(SQL_DELETE_RESULT_FOR_RUN, {"run_id": run_id})
        ins_rows = conn.execute(SQL_INSERT_RESULT_FROM_SCORECARD_RUN, {"run_id": run_id}).rowcount or 0

        # max columns
        if sc_cnt > 0:
            conn.execute(SQL_UPDATE_RESULT_MAX_COLUMNS, {"run_id": run_id})

        return {
            "responses": resp_cnt,
            "scorecard_rows": sc_cnt,
            "result_rows": ins_rows,
        }



def _safe_pct(n, d):
    try:
        n = float(n or 0); d = float(d or 0)
        return round((n / d) * 100) if d else 0
    except Exception:
        return 0

def _get_loss_report_context(run_id: int):
    """
    Non-recursive: fetch result; if missing, build it once from lca_scorecard, update max_*,
    then fetch again. Returns context dict for the template.
    """
    # ---- 1) try fetch existing result
    result_sql = text("""
        SELECT id, user_id, phase_1, phase_2, phase_3, phase_4, total,
               max_phase_1, max_phase_2, max_phase_3, max_phase_4, max_total,
               run_id, subject, created_at
        FROM lca_result
        WHERE run_id = :rid
        LIMIT 1
    """)
    row = db.session.execute(result_sql, {"rid": run_id}).fetchone()

    # ---- 2) if missing, build once from lca_scorecard (delete->insert), then update max_*
    if not row:
        with db.engine.begin() as conn:
            # delete any partial/old row and insert fresh totals from scorecard
            conn.execute(text("DELETE FROM lca_result WHERE run_id = :rid"), {"rid": run_id})
            conn.execute(text("""
                INSERT INTO lca_result
                    (user_id, phase_1, phase_2, phase_3, phase_4, total, run_id, subject)
                SELECT
                    s.user_id,
                    SUM(s.phase_1) AS phase_1,
                    SUM(s.phase_2) AS phase_2,
                    SUM(s.phase_3) AS phase_3,
                    SUM(s.phase_4) AS phase_4,
                    (SUM(s.phase_1)+SUM(s.phase_2)+SUM(s.phase_3)+SUM(s.phase_4)) AS total,
                    :rid AS run_id,
                    'LOSS' AS subject
                FROM lca_scorecard s
                WHERE s.run_id = :rid
                GROUP BY s.user_id
            """), {"rid": run_id})

            # write max_* from the actual questions answered in this run
            conn.execute(text("""
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
            """), {"rid": run_id})

        # fetch once more (still no recursion)
        row = db.session.execute(result_sql, {"rid": run_id}).fetchone()

    # ---- 3) build template context (even if still None)
    result = dict(row._mapping) if row else None

    items = []
    if result:
        lines = db.session.execute(text("""
            SELECT question_id,
                   answer_type AS answer,
                   phase_1, phase_2, phase_3, phase_4,
                   score_total, created_at
            FROM lca_scorecard
            WHERE run_id = :rid
            ORDER BY question_id
        """), {"rid": run_id}).fetchall()
        items = [dict(r._mapping) for r in lines]

    pct = {}
    if result:
        pct = {
            "p1": _safe_pct(result["phase_1"], result["max_phase_1"]),
            "p2": _safe_pct(result["phase_2"], result["max_phase_2"]),
            "p3": _safe_pct(result["phase_3"], result["max_phase_3"]),
            "p4": _safe_pct(result["phase_4"], result["max_phase_4"]),
            "total": _safe_pct(result["total"], result["max_total"]),
        }

    user_obj = current_user if getattr(current_user, "is_authenticated", False) else {"name": ""}

    return {
        "run_id": run_id,
        "result": result,
        "items": items,
        "pct": pct,
        "user": user_obj,
    }

def _pct(n, d):
    try:
        n = float(n or 0); d = float(d or 0)
        return round((n / d) * 100, 1) if d else 0.0
    except Exception:
        return 0.0

def _fetch_report_data(run_id: int):
    """Reads result + per-question rows joined with question text."""
    # 1) result row
    res = db.session.execute(text("""
        SELECT id, user_id, phase_1, phase_2, phase_3, phase_4, total,
               max_phase_1, max_phase_2, max_phase_3, max_phase_4, max_total,
               subject, run_id, created_at
        FROM lca_result
        WHERE run_id = :rid
        LIMIT 1
    """), {"rid": run_id}).fetchone()
    result = dict(res._mapping) if res else None

    # 2) per-question items (with question text)
    rows = db.session.execute(text("""
        SELECT sc.question_id,
               sc.answer_type AS answer,
               sc.phase_1, sc.phase_2, sc.phase_3, sc.phase_4,
               sc.score_total,
               q.text AS question_text
        FROM lca_scorecard sc
        LEFT JOIN lca_question q ON q.id = sc.question_id
        WHERE sc.run_id = :rid
        ORDER BY sc.question_id
    """), {"rid": run_id}).fetchall()
    items = [dict(r._mapping) for r in rows]

    return result, items

def _created_at_label(dt_str):
    if not dt_str:
        return ""
    # SQLite default CURRENT_TIMESTAMP is "YYYY-MM-DD HH:MM:SS"
    try:
        dt = datetime.fromisoformat(str(dt_str).replace(" ", "T"))
    except Exception:
        try:
            dt = datetime.strptime(str(dt_str), "%Y-%m-%d %H:%M:%S")
        except Exception:
            return str(dt_str)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _adaptive_vector_from_blocks(phase_blocks) -> str:
    """
    Rule (P1–P3 only; P4 ignored):
      - all three <= 50%  -> "Coping"
      - all three >  50%  -> "Not Coping"
      - otherwise         -> "Slightly Coping"
    """
    def _pct(n: int) -> int:
        v = next((b.get("percent_label_i") for b in phase_blocks if b.get("number") == n), 0)
        try:
            return int(v)
        except Exception:
            return 0

    p1, p2, p3 = _pct(1), _pct(2), _pct(3)
    le50 = sum(v <= 50 for v in (p1, p2, p3))
    if le50 == 3:
        return "Coping"
    if le50 == 0:
        return "Not Coping"
    return "Slightly Coping"


#from weasyprint import HTML, CSS


# Keep CSS as a PLAIN STRING (do NOT call CSS() at import time)
PDF_CSS_STR = """
@page { size: A4; margin: 16mm; }
body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial; color:#0f172a; }
"""



def _phase_library(phase_number: int):
    """
    Pull comments for a phase from lca_phase_item using your schema.
    """
    rows = db.session.execute(text("""
        SELECT body
        FROM lca_phase_item
        WHERE active = 1 AND phase_id = :pid
        ORDER BY ordinal, id
    """), {"pid": int(phase_number)}).fetchall()
    return [str(r.body).strip() for r in rows if (getattr(r, "body", None) or "").strip()]

def _count_comments_for_phase(phase_number: int, percent_int: int) -> int:
    """
    Count = floor(displayed_percent / step).
    Phases 1–2: step = 11
    Phases 3–4: step = 12.5
    """
    if percent_int <= 0:
        return 0
    step = 11.0 if phase_number in (1, 2) else 12.5
    # small epsilon to avoid float edge cases like 37.5 becoming 37.499999
    return int(math.floor((float(percent_int) / step) + 1e-9))

def _phase_blocks(run_id, items, res):
    names = {1: "Phase 1", 2: "Phase 2", 3: "Phase 3", 4: "Phase 4"}
    blocks = []
    for n in (1, 2, 3, 4):
        scored = res.get(f"phase_{n}") or 0
        maxv   = res.get(f"max_phase_{n}") or 0

        pct_f = (float(scored) / float(maxv) * 100.0) if maxv else 0.0
        pct_i = int(round(pct_f))  # what you display and what we use for counting

        want = _count_comments_for_phase(n, pct_i)
        lib  = _phase_library(n)
        shown = lib[:max(0, min(want, len(lib)))]

        blocks.append({
            "number": n,
            "name": names[n],
            "percent_label_i": pct_i,
            "comments_shown": shown,
        })
    return blocks



def _band_for_percent(pct_int: int) -> str:
    """Map displayed integer percent to band: low=0–40, mid=41–69, high=70+."""
    pct = int(pct_int or 0)
    if pct <= 40:
        return "low"
    if pct <= 69:
        return "mid"
    return "high"

def _progress_lines_from_db(phase_percents: dict, limit_per_phase: int = 1):
    """
    Build narrative lines for the Progress section using lca_progress_item.
    Shows: Phase N (XX%): <body> <ToneLabel>
    """
    lines = []

    tone_labels = {
        "positive": "Positive",
        "slightly_positive": "Slightly Positive",
        "negative": "Negative",
    }

    for pid in (1, 2, 3, 4):
        pct = int(phase_percents.get(pid, 0))
        band = _band_for_percent(pct)

        rows = db.session.execute(text("""
            SELECT body, tone
            FROM lca_progress_item
            WHERE phase_id = :pid
              AND band     = :band
              AND active   = 1
            ORDER BY ordinal
            LIMIT :n
        """), {"pid": pid, "band": band, "n": limit_per_phase}).fetchall()

        prefix = f"Phase {pid} ({pct}%): "
        for r in rows:
            body = (getattr(r, "body", "") or "").strip()
            tone = (getattr(r, "tone", "") or "").strip().lower()
            tone_label = tone_labels.get(tone, "")
            # Compose: body + tone label as the “third part”
            full = body if not tone_label else f"{body} {tone_label}."
            lines.append(prefix + full)

    return lines

@admin_bp.route("/loss/sequence/<int:pos>")
def loss_sequence_step(pos:int):
    # your existing renderer; keep endpoint for “Back to Sequence”
    return render_template("admin/loss/sequence_step.html", pos=pos)

def _get_phase_percentages_for_run(run_id:int):
    # Replace with your real source (lca_scorecard_v). Demo lets you see UI:
    rows = db.session.execute(text("""
        SELECT phase,
               CAST(100.0 * SUM(CASE WHEN answer='yes' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0) AS INT) AS pct
        FROM lca_scorecard_v
        WHERE run_id = :run_id
        GROUP BY phase
        ORDER BY phase
    """), {"run_id": run_id}).mappings().all()
    if not rows:  # fallback demo if you’re still wiring the view
        return {1:33.0, 2:48.0, 3:72.0, 4:0.0, 5:88.0}
    return {int(r["phase"]): int(r["pct"]) for r in rows}

@admin_bp.route("/loss/progress/<int:run_id>")
def loss_progress(run_id: int):
    pcts = _get_phase_percentages_for_run(run_id)  # your existing function
    if not pcts:
        abort(404)

    # your existing phase % fetch (keep your own helper if different)
    pcts = _get_phase_percentages_for_run(run_id)
    if not pcts:
        abort(404)

    adaptive_vector = compute_adaptive_vector(pcts)   # <-- add this line
    print("pcts =", pcts)               # debug once
    #print("Adaptive Vector =", av)      # debug once

    # keep the rest exactly as-is; just include adaptive_vector in the context
    return render_template(
        "admin/loss/report.html",        # or your current template name
        run_id=run_id,
        adaptive_vector=adaptive_vector     # <-- add this kwarg
    )

_WEASY_DLL_PREPARED = False
def _prepare_weasyprint_dlls():
    global _WEASY_DLL_PREPARED
    if _WEASY_DLL_PREPARED:
        return
    if sys.platform == "win32":
        # Prefer env var; otherwise try common locations
        candidates = []
        env_dirs = os.environ.get("WEASYPRINT_DLL_DIRECTORIES")
        if env_dirs:
            candidates.extend(env_dirs.split(os.pathsep))
        candidates.extend([
            r"C:\msys64\mingw64\bin",                   # MSYS2 stack (recommended)
            r"C:\Program Files\GTK3-Runtime Win64\bin", # GTK runtime
        ])
        for d in candidates:
            if d and os.path.isdir(d):
                try:
                    os.add_dll_directory(d)  # Python 3.8+
                except Exception:
                    os.environ["PATH"] = d + os.pathsep + os.environ.get("PATH", "")
    _WEASY_DLL_PREPARED = True

def _send_report_email(to_email: str, pdf_bytes: bytes, run_id: int) -> bool:
    if not to_email:
        return False
    try:
        from flask_mail import Message
        from app import mail  # your Mail() instance
    except Exception:
        # Mail isn’t configured; just return False gracefully
        return False

    try:
        msg = Message(subject="Your LOSS Report",
                      recipients=[to_email],
                      body="Attached is your LOSS report.",
                      sender=("AIT Platform", "no-reply@ait.local"))
        msg.attach(filename=f"LOSS_Report_{run_id}.pdf",
                   content_type="application/pdf",
                   data=pdf_bytes)
        mail.send(msg)
        return True
    except Exception:
        return False

def _get_phase_percentages_for_run(run_id: int):
    rows = db.session.execute(text("""
        SELECT phase,
               CAST(100.0 * SUM(CASE WHEN answer='yes' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0) AS INT) AS pct
        FROM lca_scorecard_v
        WHERE run_id = :run_id
        GROUP BY phase
        ORDER BY phase
    """), {"run_id": run_id}).mappings().all()
    if not rows:
        return {1:33, 2:48, 3:72, 4:0, 5:88}
    return {int(r["phase"]): int(r["pct"]) for r in rows}


@admin_bp.route("/loss/exit")
def loss_exit():
    try:
        logout_user()
    except Exception:
        pass
    return redirect(url_for("auth_bp.login"))  # or a public goodbye page

@admin_bp.route("/loss/thanks")
def loss_thanks():
    return render_template("admin/loss/thanks.html")


def _build_loss_report_context(run_id: int):
    result, items = _fetch_report_data(run_id)

    if not result:
        with db.engine.begin() as conn:
            conn.execute(text("DELETE FROM lca_result WHERE run_id = :rid"), {"rid": run_id})
            conn.execute(text("""
                INSERT INTO lca_result
                    (user_id, phase_1, phase_2, phase_3, phase_4, total, run_id, subject)
                SELECT
                    sc.user_id,
                    SUM(sc.phase_1), SUM(sc.phase_2), SUM(sc.phase_3), SUM(sc.phase_4),
                    SUM(sc.phase_1)+SUM(sc.phase_2)+SUM(sc.phase_3)+SUM(sc.phase_4) AS total,
                    :rid, 'LOSS'
                FROM lca_scorecard sc
                WHERE sc.run_id = :rid
                GROUP BY sc.user_id
            """), {"rid": run_id})
            conn.execute(text("""
                WITH qset AS (
                  SELECT DISTINCT question_id FROM lca_scorecard WHERE run_id = :rid
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
            """), {"rid": run_id})
        result, items = _fetch_report_data(run_id)

    # Phase blocks (unchanged source)
    phase_blocks = _phase_blocks(run_id, items, result or {})

    # Adaptive Vector from phases 1–3 (<=50 all → Coping, >50 all → Not Coping, else Slightly)
    def _adaptive_vector_from_blocks(blocks) -> str:
        def _pct(n: int) -> int:
            v = next((b.get("percent_label_i") for b in blocks if b.get("number") == n), 0)
            try: return int(v)
            except Exception: return 0
        p1, p2, p3 = _pct(1), _pct(2), _pct(3)
        le50 = sum(v <= 50 for v in (p1, p2, p3))
        if le50 == 3: return "Coping"
        if le50 == 0: return "Not Coping"
        return "Slightly Coping"

    adaptive_vector = _adaptive_vector_from_blocks(phase_blocks)

    # Integer percents (same method you already use)
    phase_percents = {
        1: next((b["percent_label_i"] for b in phase_blocks if b["number"] == 1), 0),
        2: next((b["percent_label_i"] for b in phase_blocks if b["number"] == 2), 0),
        3: next((b["percent_label_i"] for b in phase_blocks if b["number"] == 3), 0),
        4: next((b["percent_label_i"] for b in phase_blocks if b["number"] == 4), 0),
    }

    # Progress one-liners from DB (one per phase)
    progress_lines = _progress_lines_from_db(phase_percents, limit_per_phase=1)

    # Robust parse: "Phase 1 (33%): some text" -> {1: "some text", ...}
    per_phase_comment: dict[int, str] = {}
    rx = re.compile(r"^\s*Phase\s+(\d+)\s*\([^)]+\)\s*:\s*(.+)\s*$", re.IGNORECASE)
    for ln in progress_lines or []:
        m = rx.match(ln or "")
        if m:
            ph = int(m.group(1))
            per_phase_comment[ph] = m.group(2).strip()

    # Attach comment_text to each phase block (fallback stays None)
    for b in phase_blocks:
        try:
            ph = int(b.get("number"))
        except Exception:
            ph = None
        b["comment_text"] = per_phase_comment.get(ph)

    # Attach normalized bullets for PDF to every phase block
    for b in phase_blocks:
        b["pdf_bullets"] = _extract_bullets_from_block(b)

    progress_block = {"lines": progress_lines}

    # Flatten user fields so templates don't have to guess attr vs dict
    if getattr(current_user, "is_authenticated", False):
        user_name  = getattr(current_user, "name", getattr(current_user, "full_name", "")) or ""
        user_email = getattr(current_user, "email", "") or ""
    else:
        user_name = user_email = ""

    created_at_label = _created_at_label(result.get("created_at") if result else "")
    page_number = 1
    page_count  = 1

    return dict(
        run_id=run_id,
        user_name=user_name,
        user_email=user_email,
        created_at_label=created_at_label,
        phase_blocks=phase_blocks,
        progress_block=progress_block,
        page_number=page_number,
        page_count=page_count,
        adaptive_vector=adaptive_vector,
    )

def _weasy():
    """Prepare DLL path on Windows, then import and return (HTML, CSS)."""
    if sys.platform == "win32":
        # Prefer env var if you set it; else try common locations
        paths = []
        env_dirs = os.environ.get("WEASYPRINT_DLL_DIRECTORIES")
        if env_dirs:
            paths.extend(env_dirs.split(os.pathsep))
        paths.extend([
            r"C:\msys64\mingw64\bin",                   # MSYS2 (recommended)
            r"C:\Program Files\GTK3-Runtime Win64\bin", # legacy GTK runtime
        ])
        for d in paths:
            if d and os.path.isdir(d):
                try:
                    os.add_dll_directory(d)  # Python 3.8+
                except Exception:
                    os.environ["PATH"] = d + os.pathsep + os.environ.get("PATH", "")
    from weasyprint import HTML, CSS
    return HTML, CSS


def _extract_bullets_from_block(b: dict) -> list[str]:
    """Return a list of bullet lines from any common field names or HTML."""
    # 1) Common list-typed fields
    for k in ("comments", "comment_lines", "lines", "bullets", "detail_lines", "phase_bullets"):
        v = b.get(k)
        if isinstance(v, (list, tuple)) and v:
            return [str(x).strip() for x in v if str(x).strip()]

    # 2) Single string fields: split by newlines or bullet marks
    for k in ("comment", "description", "text", "details"):
        v = b.get(k)
        if isinstance(v, str) and v.strip():
            parts = re.split(r"(?:\r?\n|\u2022|•)", v)
            parts = [p.strip(" -•\t") for p in parts if p and p.strip(" -•\t")]
            if parts:
                return parts

    # 3) Try HTML lists
    for k in ("comment_html", "html", "details_html"):
        v = b.get(k)
        if isinstance(v, str) and "<li" in v.lower():
            items = re.findall(r"<li[^>]*>(.*?)</li>", v, flags=re.I | re.S)
            cleaned = []
            for it in items:
                # strip tags
                t = re.sub(r"<[^>]+>", "", it).strip()
                if t:
                    cleaned.append(t)
            if cleaned:
                return cleaned

    return []

# --- POST-TEST DASHBOARD -----------------------------------------------
@admin_bp.route("/loss/after")
def loss_after():
    run_id = get_run_id(required=True)

    result, _ = _fetch_report_data(run_id)
    if not result:
        abort(404)

    is_admin = bool(getattr(current_user, "is_admin", False))
    if not (is_admin or getattr(current_user, "id", None) == result["user_id"]):
        abort(403)

    ctx = _build_loss_report_context(run_id)
    session["last_loss_run_id"] = run_id
    return render_template("admin/loss/after.html", **with_run_id_in_ctx(ctx, run_id))

# app/routes.py


ALLOWED_ADMIN_ROLES = {"admin", "superadmin"}

@admin_bp.before_request
def admin_gate():
    uid   = session.get("user_id")
    role  = session.get("role") or session.get("user_role")  # tolerate both
    email = (session.get("user_email") or session.get("email") or "").lower()

    if not uid:
        current_app.logger.warning("ADMIN GATE: no user -> welcome (%s)", request.path)
        return redirect(url_for("public_bp.welcome"))

    if role in ALLOWED_ADMIN_ROLES:
        return  # allow

    # Self-heal: if email is on allowlist, elevate
    if email:
        try:
            with db.engine.begin() as conn:
                ok = conn.execute(
                    text("SELECT 1 FROM auth_approved_admin WHERE lower(email)=lower(:e) LIMIT 1"),
                    {"e": email}
                ).scalar()
            if ok:
                session["role"] = "admin"
                session["user_role"] = "admin"
                session["is_admin"] = True
                current_app.logger.info("ADMIN GATE: elevated %s to admin (allowlist)", email)
                return
        except Exception as ex:
            current_app.logger.error("ADMIN GATE allowlist check failed: %r", ex)

    current_app.logger.warning("ADMIN GATE: blocked uid=%s role=%r email=%r -> welcome", uid, role, email)
    return redirect(url_for("public_bp.welcome"))




def list_recent_runs_for_user(uid: int, limit: int = 50):
    # Use id DESC (fast and format-agnostic) so weird timestamp formats don't hide rows
    sql = """
        SELECT r.id, r.user_id, r.status, r.started_at, r.finished_at,
               COALESCE(res.total, 0) AS total
        FROM lca_run r
        LEFT JOIN lca_result res ON res.run_id = r.id
        WHERE r.subject='LOSS' AND r.user_id=:uid
        ORDER BY r.id DESC
        LIMIT :lim
    """
    with db.engine.begin() as conn:
        return conn.execute(text(sql), {"uid": uid, "lim": limit}).mappings().all()

def list_recent_runs_all(limit: int = 50):
    sql = """
        SELECT r.id, r.user_id, r.status, r.started_at, r.finished_at,
               COALESCE(res.total, 0) AS total
        FROM lca_run r
        LEFT JOIN lca_result res ON res.run_id = r.id
        WHERE r.subject='LOSS'
        ORDER BY r.id DESC
        LIMIT :lim
    """
    with db.engine.begin() as conn:
        return conn.execute(text(sql), {"lim": limit}).mappings().all()

# app/loss/routes.py


def latest_run_id_any():
    with db.engine.begin() as conn:
        row = conn.execute(text("SELECT id FROM lca_run ORDER BY id DESC LIMIT 1")).first()
        return row[0] if row else None

def list_runs_for_user(uid: int, limit: int = 50):
    sql = """
      SELECT r.id, r.user_id, r.status, r.started_at, r.finished_at,
             COALESCE(res.total, 0) AS total
      FROM lca_run r
      LEFT JOIN lca_result res ON res.run_id = r.id
      WHERE r.subject='LOSS' AND r.user_id=:uid
      ORDER BY r.id DESC
      LIMIT :lim
    """
    with db.engine.begin() as conn:
        return conn.execute(text(sql), {"uid": uid, "lim": limit}).mappings().all()

def list_runs_all(limit: int = 50):
    sql = """
      SELECT r.id, r.user_id, r.status, r.started_at, r.finished_at,
             COALESCE(res.total, 0) AS total
      FROM lca_run r
      LEFT JOIN lca_result res ON res.run_id = r.id
      WHERE r.subject='LOSS'
      ORDER BY r.id DESC
      LIMIT :lim
    """
    with db.engine.begin() as conn:
        return conn.execute(text(sql), {"lim": limit}).mappings().all()

def get_run_summary(run_id: int | None):
    if not run_id:
        return None
    sql = """
      SELECT r.id, r.user_id, r.status, r.started_at, r.finished_at,
             COALESCE(res.phase_1,0) AS phase_1,
             COALESCE(res.phase_2,0) AS phase_2,
             COALESCE(res.phase_3,0) AS phase_3,
             COALESCE(res.phase_4,0) AS phase_4,
             COALESCE(res.total,0)   AS total,
             res.created_at          AS res_created_at
      FROM lca_run r
      LEFT JOIN lca_result res ON res.run_id = r.id
      WHERE r.id=:rid
      LIMIT 1
    """
    with db.engine.begin() as conn:
        return conn.execute(text(sql), {"rid": run_id}).mappings().first()

# ===================== Archive endpoint (optional, if not already added) =====================

@admin_bp.post("/loss/archive")
def loss_archive_now():
    # import here to avoid circulars if needed
    from app.jobs.loss_archive import archive_finished_runs
    days  = request.form.get("days", type=int)  or 30
    limit = request.form.get("limit", type=int) or 500
    moved = archive_finished_runs(older_than_days=days, limit=limit)
    flash(f"Archived {moved} finished run(s) older than {days} day(s).", "success")
    return redirect(url_for("admin_bp.loss_home"))

@admin_bp.get("/test-email")
def admin_test_email():
    # ensure your admin gate here
    send_mail("you@example.com", "Test from AIT", "<b>It works!</b>")
    return "OK"

@admin_bp.post("/loss/import-scoring-map")
def admin_import_scoring_map():
    # add your admin gate here
    n = scoring_import.load_scoring_map_from_csv()
    return f"Imported {n} rows", 200




def _result_runs_list():
    """Distinct runs from lca_result, newest first."""
    stmt = (
        select(
            LcaResult.run_id,
            func.max(LcaResult.created_at).label("last_at"),
            func.count(LcaResult.id).label("rows_count"),
        )
        .where(LcaResult.run_id.isnot(None))
        .group_by(LcaResult.run_id)
        .order_by(func.max(LcaResult.created_at).desc())
    )
    rows = db.session.execute(stmt).all()
    return [{"run_id": r[0], "last_at": r[1], "rows_count": r[2]} for r in rows]

def _result_meta(run_id: int):
    """Meta for one run from lca_result (first/last/rows)."""
    stmt = select(
        func.min(LcaResult.created_at),   # first_at
        func.max(LcaResult.created_at),   # last_at
        func.count(LcaResult.id),         # rows
    ).where(LcaResult.run_id == run_id)
    first_at, last_at, rows = db.session.execute(stmt).one()
    return {"first_at": first_at, "last_at": last_at, "rows": rows}
# app/loss/routes.py


def _reports_dir():
    # keep reports under /data/reports/loss
    base = current_app.config.get("DATA_DIR", "/data")
    path = os.path.join(base, "report", "loss")
    os.makedirs(path, exist_ok=True)
    return path

def _report_filename(run_id: int) -> str:
    # simple, safe name
    return f"loss_run_{run_id}.pdf"



@admin_bp.route("/loss/about", methods=["GET"], endpoint="loss_about")
def loss_about():
    # Open the real LOSS admin dashboard (run-aware)
    return redirect(url_for("admin_bp.loss_dashboard", **request.args), code=302)




def list_runs_from_lca_result(limit: int = 200):
    rows = db.session.execute(text("""
        SELECT
            run_id,
            COUNT(*)                      AS rows_count,
            MAX(COALESCE(created_at, id)) AS last_key
        FROM lca_result
        GROUP BY run_id
        ORDER BY last_key DESC
        LIMIT :limit
    """), {"limit": limit}).mappings().all()

    return [
        SimpleNamespace(
            run_id=r["run_id"],
            rows_count=r["rows_count"],
            last_at=r["last_key"],  # string or datetime — we just display it
        )
        for r in rows
    ]

@admin_bp.route("/loss/runs")
def loss_runs_selector():
    runs = list_runs_from_lca_result()
    return render_template("admin/loss/runs.html", runs=runs)


  
# OPTIONAL: if you have WeasyPrint installed, we’ll use it.
try:
    from weasyprint import HTML
    HAVE_WEASY = True
except Exception:
    HAVE_WEASY = False

# ---------- helpers ----------
def _get_result_for_run(run_id: int):
    row = db.session.execute(
        text("""
            SELECT id, user_id, run_id, phase_1, phase_2, phase_3, phase_4, total, created_at, subject
            FROM lca_result
            WHERE run_id = :rid
        """),
        {"rid": run_id}
    ).mappings().first()
    return row

def _get_user_email_for_result(result_row) -> str | None:
    """Best-effort: try a few places; fall back to config default."""
    # 1) direct users table?
    try:
        u = db.session.execute(
            text("SELECT email FROM auth_user WHERE id=:uid"),
            {"uid": result_row["user_id"]}
        ).first()
        if u and u[0]:
            return u[0]
    except Exception:
        pass
    # 2) Userenrollment table?
    try:
        u = db.session.execute(
            text("SELECT email FROM auth_userenrollment WHERE user_id=:uid ORDER BY id DESC LIMIT 1"),
            {"uid": result_row["user_id"]}
        ).first()
        if u and u[0]:
            return u[0]
    except Exception:
        pass
    # 3) config default
    return (getattr(db.get_app(), "config", {}) or {}).get("DEFAULT_REPORT_RECIPIENT")

# ---------- Email & PDF ----------
@admin_bp.route("/loss/email", endpoint="loss_email_pdf", methods=["POST", "GET"])
def loss_email_pdf():
    run_id = request.args.get("run_id", type=int)
    if not run_id:
        abort(400, "run_id required")
    result_row = _get_result_for_run(run_id)
    if not result_row:
        flash("No computed result found for this run.", "warning")
        return redirect(url_for("admin_bp.loss_result", run_id=run_id))

    # Render the same report template as HTML
    html = render_template(
        "admin/loss/report.html",
        run_id=run_id,
        result=result_row,
        generated_at=datetime.utcnow(),
        # A tiny flag so the template can hide nav/buttons in the PDF
        for_pdf=True,
    )

    # Make a PDF (WeasyPrint if available; fallback to HTML attachment)
    pdf_bytes = None
    if HAVE_WEASY:
        pdf_bytes = HTML(string=html).write_pdf()

    # Send via SMTP (simple, no dependency on Flask-Mail)
    to_addr = _get_user_email_for_result(result_row)
    if not to_addr:
        flash("No recipient email found; set DEFAULT_REPORT_RECIPIENT or add user email.", "warning")
        return redirect(url_for("admin_bp.loss_report", run_id=run_id))

    import smtplib, email.utils
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.application import MIMEApplication

    cfg = db.get_app().config
    smtp_host = cfg.get("SMTP_HOST", "localhost")
    smtp_port = int(cfg.get("SMTP_PORT", 25))
    smtp_user = cfg.get("SMTP_USER")
    smtp_pass = cfg.get("SMTP_PASSWORD")
    from_addr = cfg.get("SMTP_FROM", "no-reply@ait-platform")

    msg = MIMEMultipart()
    msg["To"] = to_addr
    msg["From"] = from_addr
    msg["Subject"] = f"LOSS Report – Run {run_id}"
    msg["Date"] = email.utils.formatdate(localtime=True)

    # Body (text + HTML alternative)
    msg.attach(MIMEText(f"Attached is the LOSS report for run {run_id}.", "plain"))
    msg.attach(MIMEText(html, "html"))

    # Attach PDF if we have it
    if pdf_bytes:
        part = MIMEApplication(pdf_bytes, _subtype="pdf")
        part.add_header("Content-Disposition", "attachment", filename=f"loss_report_run_{run_id}.pdf")
        msg.attach(part)

    with smtplib.SMTP(smtp_host, smtp_port) as s:
        s.ehlo()
        if cfg.get("SMTP_STARTTLS"):
            s.starttls()
        if smtp_user and smtp_pass:
            s.login(smtp_user, smtp_pass)
        s.sendmail(from_addr, [to_addr], msg.as_string())

    flash("Report emailed.", "success")
    return redirect(url_for("admin_bp.loss_report", run_id=run_id))

# ---------- Archive ----------
@admin_bp.route("/loss/archive", endpoint="loss_archive", methods=["POST", "GET"])
def loss_archive():
    run_id = request.args.get("run_id", type=int)
    if not run_id:
        abort(400, "run_id required")

    # Prefer archiving the run itself if table exists; else mark result archived.
    updated = 0
    try:
        updated = db.session.execute(
            text("UPDATE lca_run SET archived=1 WHERE id=:rid"),
            {"rid": run_id}
        ).rowcount
        db.session.commit()
    except Exception:
        db.session.rollback()

    if updated == 0:
        try:
            db.session.execute(
                text("""
                    ALTER TABLE lca_result ADD COLUMN archived INTEGER DEFAULT 0
                """)
            )
            db.session.commit()
        except Exception:
            db.session.rollback()
        # Set archived on the result row
        db.session.execute(
            text("UPDATE lca_result SET archived=1 WHERE run_id=:rid"),
            {"rid": run_id}
        )
        db.session.commit()

    flash(f"Run {run_id} archived.", "success")
    # Send back to the dashboard (or wherever you want)
    return redirect(url_for("admin_bp.loss_index"))




@admin_bp.get("/loss/")
def loss_home():
    uid = request.args.get("uid", type=int) or session.get("user_id")
    rid = request.args.get("run_id", type=int) or latest_run_id_any()

    # try this user's runs first
    runs = list_runs_for_user(uid, limit=50) if uid else []
    # fallback to all users if empty
    if not runs:
        runs = list_runs_all(limit=50)

    selected = get_run_summary(rid) if rid else (runs[0] if runs else None)

    # ensure selected shows in dropdown even if older than page
    if selected and not any(r["id"] == selected["id"] for r in runs):
        runs = [selected] + runs

    return render_template("admin/loss/dashboard.html",
                           uid=uid, 
                           run_id=(selected["id"] if selected else None),
                           runs=runs, 
                           selected=selected)

# routes.py

def _log(msg, *args):
    try:
        app.logger.warning(msg, *args)
    except Exception:
        print(msg % args if args else msg)

def _fetch_runs():
    """Return [{"id": int, "label": str}, ...] and log every step."""
    dialect = getattr(db.session.bind.dialect, "name", "unknown")
    _log("FETCH_RUNS: dialect=%s", dialect)

    # --- Inspect schema first (SQLite-friendly) ---
    try:
        cols = db.session.execute(text("PRAGMA table_info(lca_result)")).all()
        _log("PRAGMA table_info(lca_result): %s", [tuple(r) for r in cols])
    except Exception as e:
        _log("PRAGMA lca_result failed: %r", e)

    try:
        sm = db.session.execute(text("""
            SELECT type, name, tbl_name
            FROM sqlite_master
            WHERE name IN ('lca_result','lca_scorecard_v')
        """)).all()
        _log("sqlite_master rows: %s", [tuple(r) for r in sm])
    except Exception as e:
        _log("sqlite_master inspect failed: %r", e)

    # --- Try robust query with COALESCE(run_id, id) ---
    try:
        q = text("""
            SELECT
              COALESCE(r.run_id, r.id) AS rid,
              COALESCE(cnt.rows, 0)    AS rows_count,
              r.created_at             AS created_at
            FROM lca_result r
            LEFT JOIN (
              SELECT run_id, COUNT(*) AS rows
              FROM lca_scorecard_v
              GROUP BY run_id
            ) AS cnt
              ON cnt.run_id = COALESCE(r.run_id, r.id)
            ORDER BY (r.created_at IS NULL) ASC, r.created_at DESC, rid DESC
        """)
        rows = db.session.execute(q).mappings().all()
        _log("FETCH_RUNS main query: %d row(s)", len(rows))
    except OperationalError as e:
        _log("FETCH_RUNS main query OperationalError: %r", e)
        rows = []

    # --- Fallbacks if main query gave nothing or failed ---
    if not rows:
        try:
            q2 = text("""
                SELECT COALESCE(run_id, id) AS rid, NULL AS created_at, NULL AS rows_count
                FROM lca_result
                ORDER BY rid DESC
            """)
            rows = db.session.execute(q2).mappings().all()
            _log("FETCH_RUNS fallback lca_result: %d row(s)", len(rows))
        except Exception as e:
            _log("FETCH_RUNS fallback lca_result failed: %r", e)
            rows = []

    if not rows:
        try:
            q3 = text("""
                SELECT run_id AS rid, COUNT(*) AS rows_count
                FROM lca_scorecard_v
                GROUP BY run_id
                ORDER BY run_id DESC
            """)
            rows = db.session.execute(q3).mappings().all()
            _log("FETCH_RUNS fallback view: %d row(s)", len(rows))
        except Exception as e:
            _log("FETCH_RUNS fallback view failed: %r", e)
            rows = []

    # --- Build output + sample preview ---
    out, seen = [], set()
    for r in rows:
        rid = r.get("rid")
        try:
            rid = int(rid) if rid is not None else None
        except Exception:
            rid = None
        if rid is None or rid in seen:
            continue
        seen.add(rid)
        rc = r.get("rows_count")
        label = f"Run {rid}" + (f" • {rc} rows" if rc not in (None, "") else "")
        out.append({"id": rid, "label": label})

    _log("FETCH_RUNS final list: %d run(s) -> %s", len(out), out[:5])
    return out


def _safe_url(endpoint: str, **values) -> str:
    try:
        return url_for(endpoint, **values)
    except BuildError:
        return "#"

def _fetch_runs_debug():
    """Return (runs_list, diag_lines). runs_list = [{'id': int, 'label': str}, ...]."""
    diag = []
    def d(msg):
        diag.append(msg)
        # also print so you see it in console even if logging is muted
        try:
            print(msg, flush=True)
        except Exception:
            pass
        try:
            app.logger.warning(msg)
        except Exception:
            pass

    # 0) Dialect
    try:
        dialect = getattr(db.session.bind.dialect, "name", "unknown")
    except Exception:
        dialect = "unknown"
    d(f"[runs] dialect={dialect}")

    # 1) Try the view: lca_scorecard_v
    try:
        rows = db.session.execute(text("""
            SELECT run_id, COUNT(*) AS rows
            FROM lca_scorecard_v
            GROUP BY run_id
            ORDER BY run_id DESC
        """)).mappings().all()
        d(f"[runs] view lca_scorecard_v -> {len(rows)} row(s)")
        if rows:
            out = [{"id": int(r["run_id"]),
                    "label": f"Run {int(r['run_id'])} • {int(r['rows'])} rows"} for r in rows]
            return out, diag
    except Exception as e:
        d(f"[runs] view lca_scorecard_v failed: {repr(e)}")

    # 2) Try the table: lca_scorecard
    try:
        rows = db.session.execute(text("""
            SELECT run_id, COUNT(*) AS rows
            FROM lca_scorecard
            GROUP BY run_id
            ORDER BY run_id DESC
        """)).mappings().all()
        d(f"[runs] table lca_scorecard -> {len(rows)} row(s)")
        if rows:
            out = [{"id": int(r["run_id"]),
                    "label": f"Run {int(r['run_id'])} • {int(r['rows'])} rows"} for r in rows]
            return out, diag
    except Exception as e:
        d(f"[runs] table lca_scorecard failed: {repr(e)}")

    # 3) Try lca_result; find which column exists (run_id or id)
    col = None
    try:
        cols = db.session.execute(text("PRAGMA table_info(lca_result)")).all()
        names = [c[1] for c in cols]  # (cid, name, type, notnull, dflt, pk)
        d(f"[runs] PRAGMA lca_result cols: {names}")
        if "run_id" in names:
            col = "run_id"
        elif "id" in names:
            col = "id"
    except Exception as e:
        d(f"[runs] PRAGMA lca_result failed: {repr(e)}")

    if col:
        try:
            rows = db.session.execute(text(f"""
                SELECT {col} AS rid
                FROM lca_result
                ORDER BY {col} DESC
            """)).mappings().all()
            d(f"[runs] lca_result using {col} -> {len(rows)} row(s)")
            if rows:
                seen = set()
                out = []
                for r in rows:
                    rid = r.get("rid")
                    try:
                        rid = int(rid)
                    except Exception:
                        continue
                    if rid in seen: 
                        continue
                    seen.add(rid)
                    out.append({"id": rid, "label": f"Run {rid}"})
                return out, diag
        except Exception as e:
            d(f"[runs] lca_result query failed: {repr(e)}")
    else:
        d("[runs] lca_result has neither run_id nor id")

    # 4) Nothing found
    d("[runs] No runs discovered from any source")
    return [], diag


def _render_loss_dashboard():
    requested = request.args.get("run_id", type=int)
    runs, diag = _fetch_runs_debug()
    valid = {r["id"] for r in runs}
    run_id = requested if requested in valid else None
    return render_template("admin/loss/dashboard.html",
                           runs=runs, run_id=run_id,
                           diag=diag, debug=(request.args.get("debug") == "1"))

@admin_bp.route("/loss/", endpoint="loss_dashboard")
def loss_dashboard():
    return _render_loss_dashboard()

    
def _get_result_for_run(rid: int):
    from sqlalchemy import text
    return db.session.execute(text("""
        SELECT id, user_id, phase_1, phase_2, phase_3, phase_4, total,
               created_at, run_id, subject, max_phase_1, max_phase_2, max_phase_3, max_phase_4, max_total, archived
        FROM lca_result
        WHERE run_id = :rid
        ORDER BY id DESC
        LIMIT 1
    """), {"rid": rid}).mappings().first()


#from .report import build_report_context  # if you already have this helper

def _get_int(name):
    v = request.args.get(name, type=int)
    return v if v else None

def _get_int_arg(name, *, required=False):
    try:
        return request.args.get(name, type=int)
    except Exception:
        return None

def _coerce_int(val):
    """Accept 93, '93', 'Run 93', '{id:93,...}' → return 93 (or None)."""
    if isinstance(val, int):
        return val
    if val is None:
        return None
    m = re.search(r"\d+", str(val))
    return int(m.group(0)) if m else None

def _result_for_run(run_id: int, user_id: int | None = None):
    """Read one row from lca_result for this run (optionally for a specific user)."""
    if not run_id:
        return None
    sql = "SELECT * FROM lca_result WHERE run_id = :rid"
    params = {"rid": run_id}
    if user_id:
        sql += " AND user_id = :uid"
        params["uid"] = user_id
    sql += " ORDER BY id DESC LIMIT 1"
    return db.session.execute(text(sql), params).mappings().first()

@admin_bp.route("/loss/", methods=["GET"], endpoint="loss_index")
def loss_index():
    # Read filters from query
    uid = _coerce_int(request.args.get("user_id"))
    rid = _coerce_int(request.args.get("run_id"))

    # 1) All users that have results
    user_ids = [
        r["user_id"]
        for r in db.session.execute(
            text("SELECT DISTINCT user_id FROM lca_result ORDER BY user_id")
        ).mappings().all()
    ]

    # 2) Runs for the chosen user (latest first)
    runs = []
    if uid:
        runs = [
            r["run_id"]
            for r in db.session.execute(
                text("SELECT DISTINCT run_id FROM lca_result WHERE user_id=:uid ORDER BY run_id DESC"),
                {"uid": uid},
            ).mappings().all()
        ]
        # If user picked but no run chosen yet, preselect latest
        if runs and not rid:
            rid = runs[0]

    # 3) Pull the result row for the selected run/user
    row = _result_for_run(rid, uid)
    subject = (row or {}).get("subject")
    has_result = bool(row)

    # Helpful trace while we stabilize
    current_app.logger.warning(
        "[loss_index] args=%r -> user_id=%r run_id=%r has_result=%r subject=%r",
        dict(request.args), uid, rid, has_result, subject
    )

    return render_template(
        "admin/loss/dashboard.html",
        # selections
        user_ids=user_ids,
        runs=runs,
        user_id=uid,
        run_id=rid,
        subject=subject,
        has_result=has_result,
    )


@admin_bp.route("/loss/responses", methods=["GET"], endpoint="loss_responses")
def loss_responses():
    rid = request.args.get("run_id", type=int)
    uid = request.args.get("user_id", type=int)
    if not rid:
        return ("Missing run_id", 400)

    owner = db.session.execute(
        text("SELECT user_id, subject, created_at FROM lca_result WHERE run_id=:rid ORDER BY id DESC LIMIT 1"),
        {"rid": rid}
    ).mappings().first()
    if not owner:
        return ("No result/owner for this run yet", 404)

    user_id = uid or owner["user_id"]
    subject  = owner.get("subject") or "LOSS"

    # OPTIONAL but helpful: make sure the result row exists before linking
    try:
        from app.school_loss.routes import ensure_lca_result  # wherever you defined it
        ensure_lca_result(rid)
    except Exception:
        pass

    rows = db.session.execute(text("""
        SELECT
          q.number              AS qnum,
          q.text                AS question,
          LOWER(r.answer)       AS answer,
          COALESCE(m.phase_1,0) AS p1,
          COALESCE(m.phase_2,0) AS p2,
          COALESCE(m.phase_3,0) AS p3,
          COALESCE(m.phase_4,0) AS p4,
          r.created_at          AS when_ts
        FROM lca_response r
        JOIN lca_question q ON q.id = r.question_id
        LEFT JOIN lca_question_phase_map m
          ON m.question_id = r.question_id
         AND m.answer_type = LOWER(r.answer)
        WHERE r.run_id = :rid
        ORDER BY q.number
    """), {"rid": rid}).mappings().all()

    # Precompute the hub/report links for the template
    hub_url    = url_for("loss_bp.result_run", run_id=rid, user_id=user_id)
    report_url = url_for("loss_bp.report",      run_id=rid, user_id=user_id)

    return render_template(
        "admin/loss/responses.html",
        run_id=rid, user_id=user_id, subject=subject,
        rows=rows,
        hub_url=hub_url,
        report_url=report_url
    )


def maybe_archive_runs(*, user_id: int, keep_per_user: int = 10, keep_global: int = 200):
    # Per-user: keep latest K live, archive older
    live_user_runs = db.session.execute(text("""
        SELECT run_id FROM lca_result
        WHERE user_id = :uid AND COALESCE(archived,0) = 0
        ORDER BY COALESCE(created_at, '') DESC, run_id DESC
    """), {"uid": user_id}).scalars().all()

    if len(live_user_runs) > keep_per_user:
        to_archive = live_user_runs[keep_per_user:]
        db.session.execute(text("""
            UPDATE lca_result SET archived = 1 WHERE run_id IN :rids
        """).bindparams(bindparam("rids", expanding=True)), {"rids": to_archive})
        db.session.commit()

    # Global: keep latest M live overall
    live_all = db.session.execute(text("""
        SELECT run_id FROM lca_result
        WHERE COALESCE(archived,0) = 0
        ORDER BY COALESCE(created_at, '') DESC, run_id DESC
    """)).scalars().all()

    if len(live_all) > keep_global:
        to_archive = live_all[keep_global:]
        db.session.execute(text("""
            UPDATE lca_result SET archived = 1 WHERE run_id IN :rids
        """).bindparams(bindparam("rids", expanding=True)), {"rids": to_archive})
        db.session.commit()

# routes.py
@admin_bp.route("/loss/result", methods=["GET"], endpoint="loss_result")
def loss_result():
    rid = request.args.get("run_id", type=int)
    uid = request.args.get("user_id", type=int)

    # No run provided? Try to infer latest for this user, else go back to dashboard.
    if not rid:
        if uid:
            rid = db.session.execute(
                text("""
                    SELECT run_id
                    FROM lca_result
                    WHERE user_id = :uid
                    ORDER BY id DESC
                    LIMIT 1
                """),
                {"uid": uid}
            ).scalar()
            if rid:
                return redirect(url_for("admin_bp.loss_result", run_id=rid, user_id=uid))
        return redirect(url_for("admin_bp.loss_index"))

    row = db.session.execute(
        text("""
            SELECT *
            FROM lca_result
            WHERE run_id = :rid
            ORDER BY id DESC
            LIMIT 1
        """),
        {"rid": rid}
    ).mappings().first()

    return render_template("admin/loss/result.html", run_id=rid, user_id=uid, row=row)


# app/loss/routes.py
# app/loss/routes.py
@admin_bp.route("/loss/phase-scores")
def loss_phase_scores():
    rid = request.args.get("run_id", type=int)
    uid = request.args.get("user_id", type=int)  # optional

    if not rid:
        abort(404)

    # Reuse the same rows you show on the responses page.
    rows = responses_for_run(rid)  # <-- already returns qnum, ans, p1..p4, when_ts

    # Simple totals
    def _v(x):  # guard against None/strings
        try: return int(x or 0)
        except Exception: return 0

    totals = {"P1": 0, "P2": 0, "P3": 0, "P4": 0}
    for r in rows:
        # supports both dict-like and attribute-like rows
        p1 = _v(getattr(r, "p1", r.get("p1") if isinstance(r, dict) else 0))
        p2 = _v(getattr(r, "p2", r.get("p2") if isinstance(r, dict) else 0))
        p3 = _v(getattr(r, "p3", r.get("p3") if isinstance(r, dict) else 0))
        p4 = _v(getattr(r, "p4", r.get("p4") if isinstance(r, dict) else 0))
        totals["P1"] += p1
        totals["P2"] += p2
        totals["P3"] += p3
        totals["P4"] += p4

    return render_template(
        "admin/loss/phase_scores.html",
        run_id=rid,
        user_id=uid,
        rows=rows,
        totals=totals,
    )


from flask import request, abort, render_template, redirect, url_for
from sqlalchemy import text
from app.admin import admin_bp

def _max_or_default(row, key, default_val):
    v = (row.get(key) if row else None) or 0
    return v if v > 0 else default_val

def _pct(score, maxv):
    if not maxv or maxv <= 0:
        return 0.0
    pct = (score or 0) * 100.0 / float(maxv)
    if pct < 0:   pct = 0.0
    if pct > 100: pct = 100.0
    return round(pct, 1)

def _comment_units(pct, step):
    # Each 11.1% (P1/P2) or 12.5% (P3/P4) gives you one comment.
    # Ensure at least 1 when there’s any signal (>0%).
    return int(max(0 if pct <= 0 else 1, pct // step))

@admin_bp.route("/loss/result-raw", methods=["GET"], endpoint="loss_result_raw")
def loss_results_raw():
    rid = request.args.get("run_id", type=int)
    uid = request.args.get("user_id", type=int)
    if not rid:
        abort(400, "Missing run_id")

    # Totals row (plus when it was taken)
    result_row = db.session.execute(text("""
        SELECT
          r.user_id, r.subject,
          r.phase_1, r.phase_2, r.phase_3, r.phase_4, r.total,
          r.max_phase_1, r.max_phase_2, r.max_phase_3, r.max_phase_4, r.max_total,
          COALESCE(r.created_at, lr.finished_at, lr.started_at) AS taken_at
        FROM lca_result r
        LEFT JOIN lca_run lr ON lr.id = r.run_id
        WHERE r.run_id = :rid
        ORDER BY r.id DESC
        LIMIT 1
    """), {"rid": rid}).mappings().first()

    # Per-question raw rows
    rows = db.session.execute(text("""
        SELECT
          q.number              AS qnum,
          LOWER(r.answer)       AS ans,
          COALESCE(m.phase_1,0) AS p1,
          COALESCE(m.phase_2,0) AS p2,
          COALESCE(m.phase_3,0) AS p3,
          COALESCE(m.phase_4,0) AS p4,
          r.created_at          AS when_ts
        FROM lca_response r
        JOIN lca_question q ON q.id = r.question_id
        LEFT JOIN lca_question_phase_map m
          ON m.question_id = r.question_id
         AND m.answer_type = LOWER(r.answer)
        WHERE r.run_id = :rid
        ORDER BY q.number
    """), {"rid": rid}).mappings().all()

    # Simple percentages from maxima
    def pct(v, m):
        try:
            v = int(v or 0); m = int(m or 0)
            return int(round(100 * v / m)) if m > 0 else 0
        except Exception:
            return 0

    result_pct = None
    if result_row:
        result_pct = {
            "p1": pct(result_row.get("phase_1"), result_row.get("max_phase_1")),
            "p2": pct(result_row.get("phase_2"), result_row.get("max_phase_2")),
            "p3": pct(result_row.get("phase_3"), result_row.get("max_phase_3")),
            "p4": pct(result_row.get("phase_4"), result_row.get("max_phase_4")),
            "total": pct(result_row.get("total"), result_row.get("max_total")),
        }
        if uid is None:
            uid = result_row.get("user_id")

    return render_template(
        "admin/loss/results_raw.html",
        run_id=rid,
        user_id=uid,
        result=result_row,
        result_pct=result_pct,
        rows=rows,
    )


def _html_to_pdf_response(html: str, filename: str = "report.pdf") -> Response:
    """
    Renders HTML to PDF. Tries WeasyPrint first, then pdfkit.
    Returns a Flask Response with application/pdf. If no PDF engine
    is available, returns the HTML (so you at least see something).
    """
    # Try WeasyPrint
    try:
        # AFTER
        from app.utils.pdf_render import html_to_pdf_bytes
        pdf_bytes = html_to_pdf_bytes(html, base_url=request.host_url)

        return Response(
            pdf_bytes,
            mimetype="application/pdf",
            headers={"Content-Disposition": f'inline; filename="{filename}"'}
        )
    except Exception:
        pass

    # Try pdfkit
    try:
        import pdfkit
        pdf_bytes = pdfkit.from_string(html, False)  # returns bytes
        return Response(
            pdf_bytes,
            mimetype="application/pdf",
            headers={"Content-Disposition": f'inline; filename="{filename}"'}
        )
    except Exception:
        # Last resort: give back the HTML so debugging is easy
        return Response(html, mimetype="text/html")


def _build_report_ctx(run_id: int, user_id: int | None):
    # 1) Get the lca_result row for this run
    row = db.session.execute(text("""
        SELECT id, user_id, subject, created_at,
               phase_1, phase_2, phase_3, phase_4, total,
               max_phase_1, max_phase_2, max_phase_3, max_phase_4
        FROM lca_result
        WHERE run_id = :rid
        ORDER BY id DESC
        LIMIT 1
    """), {"rid": run_id}).mappings().first()

    if not row:
        # No precomputed row? derive zeros so the page still renders.
        row = {"user_id": user_id, "subject": "LOSS",
               "created_at": None, "phase_1": 0, "phase_2": 0, "phase_3": 0, "phase_4": 0, "total": 0,
               "max_phase_1": 0, "max_phase_2": 0, "max_phase_3": 0, "max_phase_4": 0}

    uid = user_id or row.get("user_id")
    user = None
    if uid:
        user = db.session.execute(text("SELECT id, name, email FROM user WHERE id=:uid"),
                                  {"uid": uid}).mappings().first()

    def pct(n, d):
        try:
            return int(round((float(n) / float(d)) * 100)) if d else 0
        except Exception:
            return 0

    names = {1: "Phase 1", 2: "Phase 2", 3: "Phase 3", 4: "Phase 4"}
    scores = {
        1: row.get("phase_1", 0) or 0,
        2: row.get("phase_2", 0) or 0,
        3: row.get("phase_3", 0) or 0,
        4: row.get("phase_4", 0) or 0,
    }
    maxima = {
        1: row.get("max_phase_1", 0) or 0,
        2: row.get("max_phase_2", 0) or 0,
        3: row.get("max_phase_3", 0) or 0,
        4: row.get("max_phase_4", 0) or 0,
    }

    phase_blocks = []
    for i in (1, 2, 3, 4):
        phase_blocks.append({
            "number": i,
            "name": names[i],
            "percent_label_i": pct(scores[i], maxima[i]),
            "comments_shown": [],  # (optional: add your per-phase comments list here)
        })

    # Simple adaptive vector from total (tweak thresholds to your rules)
    total = row.get("total", 0) or 0
    if total >= 75:
        adaptive_vector = "Coping"
    elif total >= 50:
        adaptive_vector = "Slightly Coping"
    else:
        adaptive_vector = "Not Coping"

    progress_block = {"lines": []}  # (fill with your progress narrative if desired)

    created_at_label = None
    if row.get("created_at"):
        created_at_label = str(row["created_at"])  # or format as you like

    return {
        "run_id": run_id,
        "user_id": uid,
        "user": user or {"name": "", "email": ""},
        "phase_blocks": phase_blocks,
        "adaptive_vector": adaptive_vector,
        "progress_block": progress_block,
        "created_at_label": created_at_label,
        "page_number": 1,
        "page_count": 1,
    }

PHASE_ITEM_STEP = {
    1: Decimal("11.1"),
    2: Decimal("11.1"),
    3: Decimal("12.5"),
    4: Decimal("12.5"),
}

PHASE_ITEM_MAX = {  # safety caps (if DB has fewer, query will just return fewer)
    1: 9,
    2: 9,
    3: 8,
    4: 8,
}

def phase_item_count(phase_no: int, pct: float) -> int:
    step = PHASE_ITEM_STEP.get(int(phase_no), Decimal("12.5"))
    d_pct = Decimal(str(pct)).max(Decimal("0")).min(Decimal("100"))
    # floor(pct / step), but never 0 if pct>0
    count = int((d_pct / step).quantize(Decimal("1"), rounding=ROUND_FLOOR))
    # If you want 1 item to appear already from the first step only, keep as-is.
    # If you want 0 items until the first full step is hit, keep as-is (this does that).
    # Cap by configured max
    return max(0, min(count, PHASE_ITEM_MAX.get(int(phase_no), 8)))

def get_phase_items(phase_no: int, n: int):
    if n <= 0:
        return []
    from app.models import LcaPhaseItem  # adjust import
    rows = (db.session.query(LcaPhaseItem)
            .filter(LcaPhaseItem.phase_no == phase_no)
            .order_by(LcaPhaseItem.sort_order.asc(), LcaPhaseItem.id.asc())
            .limit(n)
            .all())
    return [r.text for r in rows]

@admin_bp.route("/loss/report/email", methods=["POST", "GET"])
def email_loss_report():
    rid = _get_int_arg("run_id")
    uid = _get_int_arg("user_id", required=False)

    # Find user email (or accept ?to= param)
    to_email = request.args.get("to")
    if not to_email and uid:
        u = db.session.execute(text('SELECT email FROM "user" WHERE id=:uid'),
                               {"uid": uid}).mappings().first()
        to_email = u["email"] if u else None
    if not to_email:
        return ("Missing recipient (?to=)", 400)

    # Render PDF content exactly like in loss_report_pdf()
    # ... (copy the data fetch + render steps from above) ...
    # pdf_bytes = pdf_io.getvalue()

    # msg = Message(subject=f"Loss Report (run {rid})",
    #               recipients=[to_email])
    # msg.body = "Please find your Loss Assessment Report attached."
    # msg.attach(f"loss-report-run{rid}.pdf", "application/pdf", pdf_bytes)
    # mail.send(msg)
    return ("Email queued", 200)

ARCHIVE_DIR = os.environ.get("LOSS_REPORT_ARCHIVE_DIR", os.path.join(os.getcwd(), "var", "loss_reports"))
os.makedirs(ARCHIVE_DIR, exist_ok=True)

def _archive_pdf(pdf_bytes: bytes, rid: int, uid: int | None) -> str:
    ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    fname = f"loss-report-run{rid}{('-u'+str(uid)) if uid else ''}-{ts}.pdf"
    fpath = os.path.join(ARCHIVE_DIR, fname)
    with open(fpath, "wb") as fh:
        fh.write(pdf_bytes)
    return fpath

REPORT_DIR = os.environ.get("LOSS_REPORT_DIR", os.path.join(os.getcwd(), "var", "loss_reports"))
ARCHIVE_DIR = os.environ.get("LOSS_REPORT_ARCHIVE_DIR", os.path.join(os.getcwd(), "var", "loss_reports_archive"))
os.makedirs(REPORT_DIR, exist_ok=True)
os.makedirs(ARCHIVE_DIR, exist_ok=True)

ARCHIVE_THRESHOLD = 100   # when total files in REPORT_DIR reaches/exceeds this
ARCHIVE_BATCH     = 50    # move the oldest N to ARCHIVE_DIR

def _pct(score, mx):
    try:
        s = float(score or 0); m = float(mx or 0)
        return int(round(100.0 * s / m)) if m > 0 else 0
    except Exception:
        return 0

def _build_context(rid: int, uid: int | None):
    row = db.session.execute(text("""
        SELECT id, user_id, run_id, subject, created_at,
               phase_1, phase_2, phase_3, phase_4, total,
               COALESCE(max_phase_1, 18) AS max_p1,
               COALESCE(max_phase_2, 18) AS max_p2,
               COALESCE(max_phase_3, 32) AS max_p3,
               COALESCE(max_phase_4, 32) AS max_p4
        FROM lca_result
        WHERE run_id = :rid
        ORDER BY id DESC
        LIMIT 1
    """), {"rid": rid}).mappings().first()
    if not row:
        return None, None, None

    p1 = _pct(row["phase_1"], row["max_p1"])
    p2 = _pct(row["phase_2"], row["max_p2"])
    p3 = _pct(row["phase_3"], row["max_p3"])
    p4 = _pct(row["phase_4"], row["max_p4"])

    blocks = build_phase_blocks(p1, p2, p3, p4)
    av = adaptive_vector_from_phases(p1, p2, p3, p4)
    oa = overall_assessment_from_p1(p1)

    user = None
    if uid:
        user = db.session.execute(text('SELECT id, name, email FROM "user" WHERE id=:uid'),
                                  {"uid": uid}).mappings().first()

    ctx = {
        "run_id": rid,
        "user_id": uid,
        "user": user,
        "created_at_label": str(row.get("created_at") or ""),
        "phase_blocks": blocks,
        "progress_block": {"lines": [f"Phase 1: {p1}%", f"Phase 2: {p2}%", f"Phase 3: {p3}%", f"Phase 4: {p4}%"]},
        "adaptive_vector": av,
        "overall_assessment": oa,
        "disclaimer_text": DISCLAIMER_TEXT,
    }
    return ctx, (p1, p2, p3, p4), row

def _render_pdf_bytes(ctx: dict) -> bytes:
    html = render_template("admin/loss/report.html", **ctx, pdf_mode=True)
    pdf_io = BytesIO()
    HTML(string=html, base_url=request.host_url).write_pdf(pdf_io)
    return pdf_io.getvalue()

def _save_pdf(pdf_bytes: bytes, rid: int, uid: int | None) -> str:
    ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    fname = f"loss-report-run{rid}{('-u'+str(uid)) if uid else ''}-{ts}.pdf"
    fpath = os.path.join(REPORT_DIR, fname)
    with open(fpath, "wb") as fh:
        fh.write(pdf_bytes)
    return fpath

def _maybe_housekeep_archive():
    files = sorted(glob.glob(os.path.join(REPORT_DIR, "*.pdf")), key=os.path.getmtime)
    if len(files) >= ARCHIVE_THRESHOLD:
        to_move = files[:ARCHIVE_BATCH]
        for src in to_move:
            dst = os.path.join(ARCHIVE_DIR, os.path.basename(src))
            try:
                shutil.move(src, dst)
            except Exception:
                pass

def _phase_scores_from_blocks(ctx):
    # your template shows a `blocks` list; each item has: phase, pct (or width_pct)
    blocks = ctx.get("blocks") or ctx.get("phase_blocks") or []
    by_phase = {}
    for b in blocks:
        p = int(b.get("phase") or 0)
        raw = b.get("pct")
        if raw is None:
            raw = b.get("width_pct")
        try:
            v = int(round(float(str(raw).replace('%','').strip())))
        except Exception:
            v = 0
        if 1 <= p <= 4:
            by_phase[p] = max(0, min(100, v))
    # ensure 4 values in order 1..4
    return [by_phase.get(i, 0) for i in (1, 2, 3, 4)]

TEMPLATE_DIR = "subject/loss"      # ← change to "admin/loss" if that’s where the files are

def render_loss_template(name, **ctx):
    for path in (f"subject/loss/{name}", f"admin/loss/{name}", f"school_loss/{name}"):
        try:
            return render_template(path, **ctx)
        except TemplateNotFound:
            continue
    raise

SEED_REGISTRY = {
    "questions":       LcaQuestion,
    "phase_items":     LcaPhaseItem,
    "progress_items":  LcaProgressItem,
    "overall_items":   LcaOverallItem,
    "intro_cards":     LcaInstruction,
    "explain_cards":   LcaExplain,
    "pause_cards":     LcaPause,
}

def _get_seed_model(seed: str):
    model = SEED_REGISTRY.get(seed)
    if not model:
        abort(404, description=f"Unknown seed '{seed}'")
    return model


# If there’s another variant somewhere else, rename its endpoint:
@admin_bp.get("/loss/seeds/<seed>/export.csv", endpoint="seed_export_csv_alt")
def seed_export_csv_alt(seed):
    ...

@admin_bp.route("/loss/seeds/<seed>/import-from-repo", methods=["POST", "GET"])
def seed_import_from_repo(seed: str):
    if not session.get("is_admin"):
        abort(403)
    model = _get_seed_model(seed)

    repo_dir = current_app.config.get("SEED_REPO_DIR")
    if not repo_dir:
        # default to <project>/seeds or <app_root>/loss/seeds
        repo_dir = os.path.join(current_app.root_path, "..", "seeds")
        if not os.path.isdir(repo_dir):
            repo_dir = os.path.join(current_app.root_path, "admin", "loss", "seeds")
    path = os.path.abspath(os.path.join(repo_dir, f"{seed}.csv"))
    if not os.path.exists(path):
        flash(f"No CSV found at {path}", "warning")
        return redirect(url_for("admin_bp.seed_preview", seed=seed))

    try:
        added, updated, skipped = seed_import_csv_path(model, path)
        flash(f"Imported from repo: {added} new, {updated} updated.", "success")
    except Exception as ex:
        current_app.logger.exception("Repo import failed")
        db.session.rollback()
        flash(f"Import failed: {ex}", "danger")
    return redirect(url_for("admin_bp.seed_preview", seed=seed))

def seeds_dir(subject: str = "loss") -> Path:
    # Use instance/seeds/<subject>  (e.g., instance/seeds/loss)
    d = Path(current_app.instance_path) / "seeds" / subject
    d.mkdir(parents=True, exist_ok=True)
    return d

if not getattr(admin_bp, "_loss_seed_routes_registered", False):
    
    @admin_bp.get("/loss/seeds/<seed>")
    def seed_preview(seed):
        seed = canon_seed(seed)
        meta = SEED_TABLES.get(seed)
        if not meta:
            abort(404)
        rows = preview_rows(seed)
        cols = meta["columns"]
        title = meta.get("title", seed.title())
        return render_template(
            "subject/loss/seed/preview.html",
            seed=seed, title=title, rows=rows, cols=cols
        )
    
    @admin_bp.post("/seed/upload-json")
    def seed_upload(seed):
        seed = canon_seed(seed)
        meta = SEED_TABLES.get(seed)
        if not meta:
            abort(404)

        f = request.files.get("file")
        if not f or not f.filename.lower().endswith(".csv"):
            flash("Please upload a .csv file.", "warning")
            return redirect(url_for("admin_bp.seed_preview", seed=seed))

        try:
            count = import_csv_stream(seed, f.stream)
            flash(f"Imported {count} rows into {seed}.", "success")
        except Exception as e:
            db.session.rollback()
            flash(f"Import failed: {e}", "danger")

        return redirect(url_for("admin_bp.seed_preview", seed=seed))

    @admin_bp.route("/loss/seeds/<seed>/import-from-repo", methods=["GET", "POST"])
    def seed_import_from_repo_route(seed):
        seed = canon_seed(seed)
        if request.method == "GET":
            return render_template("subject/loss/seed/import_from_repo.html", seed=seed)

        # POST
        try:
            count = import_from_repo(seed)
            flash(f"Imported {count} rows for {seed} from repo.", "success")
        except FileNotFoundError:
            flash("No seed CSV found in repo for this item.", "warning")
        except Exception as e:
            db.session.rollback()
            flash(f"Import failed: {e}", "danger")

        return redirect(url_for("admin_bp.seed_preview", seed=seed))

    admin_bp._loss_seed_routes_registered = True
# ========================================================================


@admin_bp.get("/loss/seeds/<seed>/edit", endpoint="seed_edit_simple")
def seed_edit_simple(seed):
    if seed not in SEEDS:
        return ("Unknown seed", 404)
    rows = fetch_rows(seed)
    meta = SEEDS[seed]
    return render_template("subject/loss/seed/editor.html", seed=seed, meta=meta, rows=rows)

@admin_bp.post("/loss/seeds/<seed>/edit", endpoint="seed_save_simple")
def seed_save_simple(seed):
    if seed not in SEEDS:
        return ("Unknown seed", 404)
    stats = save_from_form(seed, request.form)
    flash(f"Saved: {stats['updated']} updated, {stats['created']} created, {stats['deleted']} deleted.", "success")
    return redirect(url_for("admin_bp.seed_edit_simple", seed=seed))

# ---- Canonical seed endpoints (one of each) ----

@admin_bp.post("/loss/seeds/<seed>/upload", endpoint="seed_upload")
def seed_upload(seed: str):
    # form file field name MUST match your template (e.g., 'file')
    f = request.files.get("file")
    if not f or not f.filename.lower().endswith(".csv"):
        flash("Please upload a .csv file.", "warning")
        return redirect(url_for("admin_bp.seed_hub", tab=seed))
    count = import_csv_stream(seed, f)      # from your helper
    flash(f"Imported {count} rows into {seed}.", "success")
    return redirect(url_for("admin_bp.seed_preview", seed=seed))

@admin_bp.post("/loss/seeds/<seed>/import", endpoint="seed_import_csv")
def seed_import_csv(seed: str):
    meta = SEED_CFG.get(seed) or abort(404)
    f = request.files.get("file")
    if not f or not f.filename.lower().endswith(".csv"):
        abort(400, "Please upload a .csv file.")
    path = seed_csv_path(seed)
    f.save(path)

    # Import: truncate then insert (simple and predictable)
    cols = meta["cols"]
    with db.session.begin():
        db.session.execute(text(f"DELETE FROM {meta['table']}"))
        with open(path, encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                # if id is AUTOINCREMENT and left blank, omit it
                insert_cols = [c for c in cols if not (c == "id" and (row.get("id") in (None, "", "NULL")))]
                placeholders = ", ".join([f":{c}" for c in insert_cols])
                sql = text(f"INSERT INTO {meta['table']} ({', '.join(insert_cols)}) VALUES ({placeholders})")
                db.session.execute(sql, {c: row.get(c, None) for c in insert_cols})

    flash(f"Imported {meta['title']} from {path.name}", "success")
    return redirect(url_for("admin_bp.seed_hub", tab=seed))

@admin_bp.get("/loss/seeds")
def seed_hub():
    tab = request.args.get("tab")
    if not tab or tab not in SEED_CFG:
        tab = "questions"
    seeds_meta = {
        key: {
            "key": key,
            "title": cfg["title"],
            "cols_text": ", ".join(cfg["cols"]),
            "note": f"seeds/{cfg.get('filename', key + '.csv')}",
        }
        for key, cfg in SEED_CFG.items()
    }
    return render_template(
        "subject/loss/seed/index.html",
        tab=tab,
        seeds_meta=seeds_meta,
        seed_keys=list(SEED_CFG.keys()),
    )

@admin_bp.get("/loss/seeds/<seed>", endpoint="seed_preview")
def seed_preview(seed):
    meta = SEEDS.get(seed) or abort(404)
    rows = fetch_rows(seed)

    if isinstance(meta.cols, str):
        cols = [c.strip() for c in meta.cols.split(",")]
    else:
        cols = list(meta.cols)

    return render_template(
        "subject/loss/seed/preview.html",
        seed=seed,
        title=meta.title,
        cols=cols,
        rows=rows,
    )

@admin_bp.get("/loss/seeds/<seed>/export.csv", endpoint="seed_export_csv")
def seed_export_csv(seed: str):
    meta = SEED_CFG.get(seed) or abort(404)
    cols = meta["cols"]
    rows = fetch_rows_db(meta)   # <— same here
    path = seed_csv_path(seed)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        import csv
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({k: (r.get(k, "") if r.get(k, "") is not None else "") for k in cols})
    from flask import send_file
    return send_file(str(path), mimetype="text/csv",
                     as_attachment=True, download_name=meta.get("filename", f"{seed}.csv"))

@admin_bp.route("/loss/tables/save", methods=["GET", "POST"], endpoint="tables_save")
def tables_save():
    raw_seed = request.args.get("seed", "questions")
    seed = resolve_seed_key(raw_seed)
    meta = SEEDS.get(seed) or abort(404)

    if request.method == "POST":
        model = getattr(meta, "model", None) or abort(400, "SeedMeta has no model binding")
        cols = normalize_cols(meta)  # [(attr,label)]

        _, headers, rows = read_csv(seed)
        if not headers:
            abort(404, f"No CSV for {seed}. Use the View page to export first.")

        # optional coercion (kept concise)
        type_map = {c.name: c.type for c in model.__table__.columns}
        from sqlalchemy import Integer, Float, Boolean
        from sqlalchemy.sql.sqltypes import Date, DateTime
        from datetime import datetime

        def coerce(attr, raw):
            if raw is None or (isinstance(raw, str) and raw.strip() == ""): return None
            t = type_map.get(attr)
            try:
                if isinstance(t, Integer):  return int(raw)
                if isinstance(t, Float):    return float(raw)
                if isinstance(t, Boolean):  return str(raw).strip().lower() in ("1","true","t","yes","y")
                if isinstance(t, DateTime):
                    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
                        try: return datetime.strptime(str(raw), fmt)
                        except ValueError: pass
                if isinstance(t, Date):
                    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
                        try: return datetime.strptime(str(raw), fmt).date()
                        except ValueError: pass
            except Exception:
                pass
            return raw

        db.session.query(model).delete()
        for r in rows:
            data = {attr: coerce(attr, r.get(label)) for attr, label in cols if label in r}
            db.session.add(model(**data))
        db.session.commit()

        flash(f"Imported {len(rows)} rows into {model.__name__}.", "success")
        return redirect(url_for("admin_bp.tables_view", seed=seed, back=request.args.get("back")))

    # GET → CSV preview
    csv_path, headers, rows = read_csv(seed)
    if not headers:
        flash(f"No CSV found at {csv_path}. Use the View page to export first.", "warning")

    return render_template(
        "subject/loss/seed/view_tables.html",
        mode="save",
        seed=seed,
        seeds=list(SEEDS.keys()),
        title=f"{getattr(meta, 'title', seed.title())} (CSV)",
        cols=headers,
        rows=rows,
        csv_path=str(csv_path),
        import_url=url_for("admin_bp.tables_save", seed=seed, back=request.args.get("back")),
        back_url=best_loss_back_url(),
        csrf_token_value=generate_csrf(),             # <-- provide token to template
    )

@admin_bp.get("/loss", endpoint="loss_admin_home")
def loss_admin_home():
    # send users to the main admin home (adjust endpoint if yours differs)
    return redirect(url_for("admin_bp.admin_home"))

@admin_bp.get("/loss/tables/download", endpoint="tables_download")
def tables_download():
    seed = request.args.get("seed", "questions")
    meta = SEEDS.get(seed) or abort(404)

    cols = normalize_cols(meta)
    headers = [lbl for _, lbl in cols]
    db_rows = fetch_rows(seed)
    dict_rows = map_rows_from_db(db_rows, cols)
    csv_path = write_csv(seed, headers, dict_rows)
    return send_file(csv_path, as_attachment=True, download_name=f"{seed}.csv", mimetype="text/csv", max_age=0)

@admin_bp.get("/loss/tables/view", endpoint="tables_view")
def tables_view():
    raw_seed = request.args.get("seed", "questions")
    seed = resolve_seed_key(raw_seed)
    meta = SEEDS.get(seed) or abort(404)

    cols = normalize_cols(meta)                       # [(attr,label)]
    headers = [lbl for _, lbl in cols]

    db_rows = db_rows_fallback(seed, meta)            # robust
    dict_rows = map_rows_from_db(db_rows, cols)       # [{label: value}...]

    # keep seed/<seed>.csv fresh so VS Code shows the right file
    write_csv(seed, headers, dict_rows)

    return render_template(
        "subject/loss/seed/view_tables.html",
        mode="view",
        seed=seed,
        seeds=list(SEEDS.keys()),
        title=getattr(meta, "title", seed.title()),
        cols=headers,
        rows=dict_rows,
        back_url=best_loss_back_url(),
        download_base=url_for("admin_bp.tables_download"),
        csrf_token_value=generate_csrf(),             # pass token in case you add forms later
    )
