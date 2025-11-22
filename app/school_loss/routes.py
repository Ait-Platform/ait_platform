# loss_helper.py


from app.models.loss import (
    LcaResult, LcaRun, LcaScoringMap,LcaSequence,  LcaQuestion, LcaResponse,
    )

from app.extensions import db
from sqlalchemy import text, func
from flask import flash, send_file

from app.subject_loss.charts import phase_scores_bar

from xhtml2pdf import pisa

from datetime import datetime

import matplotlib.dates as mdates
from flask import render_template_string

from reportlab.lib.pagesizes import letter
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # ✅ prevents Tkinter errors
from matplotlib import pyplot as plt
from flask_login import  current_user
import sqlite3
from flask import current_app
from flask import request, redirect, url_for, render_template
from app.models.loss import LcaInstruction, LcaExplain, LcaPause  # adjust import path
from jinja2 import TemplateNotFound
from app.models.auth import User  
try:
    from app.utils.assessment_helpers import get_user_display_name
except Exception:
    get_user_display_name = None
from typing import Any, Dict, List, Optional
from types import SimpleNamespace
import csv, click
from decimal import Decimal, ROUND_HALF_UP
from flask import session
from sqlalchemy import text
from math import ceil
import csv, os
from flask import session, g, abort
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

from werkzeug.exceptions import BadRequest
from io import BytesIO
from email.message import EmailMessage
import smtplib
from pathlib import Path
from functools import lru_cache
import csv, logging
from flask_wtf.csrf import generate_csrf
from app.models.loss import LcaInstruction, LcaExplain, LcaPause  # adjust path
from urllib.parse import urlparse


def _choose_tpl(ctype: str) -> str:
    # Prefer folder structure, then flat fallback
    candidates = [
        f"school_loss/cards/{ctype}/index.html",
        f"school_loss/cards/{ctype}/{ctype}.html",
        f"school_loss/cards/{ctype}.html",
        "shared/card.html",
    ]
    available = set(current_app.jinja_env.list_templates())
    for t in candidates:
        if t in available:
            return t
    return f"school_loss/cards/{ctype}.html"

# Database connection function
def get_db():
    conn = sqlite3.connect('your_database.db')  # Replace with your database path
    return conn

def _lower(s): return (s or "").strip().lower()

def _get_user_id() -> int | None:
    return session.get("user_id")

def _fetch_cards_where(sql: str, params: dict = None) -> list[dict]:
    try:
        rows = db.session.execute(text(sql), params or {}).mappings().all()
        return [dict(r) for r in rows if r.get("text")]
    except Exception:
        return []

def _build_loss_sequence() -> list[dict]:
    """
    Exact storyboard:
      6 x instruction (welcome first if present) → pause → q1-25 → pause → q26-50 → pause → 8 x explain
    Falls back to synthesizing pauses/explains if the DB doesn’t have them.
    Expected columns if present: id, kind/type, seq/number, text, phase
    """

    # 1) Pull everything we might need
    instr = _fetch_cards_where("""
        SELECT id,
               COALESCE(kind, CASE WHEN lower(type) IN ('instr','inst','instruction') THEN 'instruction' END) AS kind,
               COALESCE(seq, number, id) AS seq,
               COALESCE(text, prompt, body) AS text,
               COALESCE(phase, NULL) AS phase
        FROM lca_question
        WHERE lower(COALESCE(kind,type,'')) IN ('instruction','instr','inst')
        ORDER BY seq
    """)

    qs = _fetch_cards_where("""
        SELECT id,
               'question' AS kind,
               COALESCE(seq, number, id) AS seq,
               COALESCE(text, prompt, body) AS text,
               COALESCE(phase, NULL) AS phase
        FROM lca_question
        WHERE lower(COALESCE(kind,type,'')) IN ('question','q')
        ORDER BY seq
    """)

    pauses = _fetch_cards_where("""
        SELECT id,
               'pause' AS kind,
               COALESCE(seq, number, id) AS seq,
               COALESCE(text, prompt, body) AS text,
               NULL AS phase
        FROM lca_question
        WHERE lower(COALESCE(kind,type,'')) IN ('pause','break')
        ORDER BY seq
        LIMIT 3
    """)

    explains = _fetch_cards_where("""
        SELECT id,
               'explain' AS kind,
               COALESCE(seq, number, id) AS seq,
               COALESCE(text, prompt, body) AS text,
               NULL AS phase
        FROM lca_question
        WHERE lower(COALESCE(kind,type,'')) IN ('explain','explanation')
        ORDER BY seq
        LIMIT 8
    """)

    # 2) Normalize instruction list: pick 6, force Welcome first if we can find it
    #    We consider a card "welcome" if text contains 'welcome' (case-insensitive).
    instr_sorted = instr[:]
    if instr_sorted:
        def is_welcome(c): return 'welcome' in (c.get("text") or "").strip().lower()
        welcomes = [c for c in instr_sorted if is_welcome(c)]
        others   = [c for c in instr_sorted if not is_welcome(c)]
        instr_sorted = (welcomes[:1] + others)[:6]
    else:
        # synthesize 6 basic instruction cards if none exist
        instr_sorted = [{"id": -i, "kind": "instruction", "seq": i, "text": ("Welcome" if i==1 else f"Instruction {i}"), "phase": None} for i in range(1, 7)]

    # 3) Split questions into 1–25 and 26–50 by their seq ordering
    qs_sorted = qs[:]
    # If we have more than 50 or fewer, still slice safely
    first_25 = qs_sorted[:25]
    next_25  = qs_sorted[25:50]

    # If DB had fewer than needed, synthesize to reach counts
    def synth_q(start_seq, count, start_id=-1000):
        return [
            {"id": start_id - i, "kind": "question", "seq": start_seq + i, "text": f"Question {start_seq + i}", "phase": None}
            for i in range(count)
        ]

    if len(first_25) < 25:
        first_25 += synth_q(len(first_25) + 1, 25 - len(first_25))
    if len(next_25) < 25:
        base = 26
        next_25 += synth_q(base + len(next_25), 25 - len(next_25), start_id=-2000)

    # 4) Pauses: ensure we have exactly 3
    while len(pauses) < 3:
        idx = len(pauses) + 1
        pauses.append({"id": -3000 - idx, "kind": "pause", "seq": 9000 + idx, "text": f"Take a short break #{idx}", "phase": None})
    pauses = pauses[:3]

    # 5) Explains: ensure 8
    while len(explains) < 8:
        idx = len(explains) + 1
        explains.append({"id": -4000 - idx, "kind": "explain", "seq": 10000 + idx, "text": f"Explanation {idx}", "phase": None})
    explains = explains[:8]

    # 6) Assemble exact storyboard
    seq_out: list[dict] = []
    # Label questions 1..50 for UI clarity
    q_counter = 0

    def add_block(items):
        nonlocal q_counter
        for it in items:
            it = dict(it)  # copy
            if it["kind"] == "question":
                q_counter += 1
                it["q_number"] = q_counter  # for display: “Question 1…”
            seq_out.append(it)

    add_block(instr_sorted)        # 6 instructions (Welcome first)
    add_block([pauses[0]])         # Pause A
    add_block(first_25)            # Q1–Q25
    add_block([pauses[1]])         # Pause B
    add_block(next_25)             # Q26–Q50
    add_block([pauses[2]])         # Pause C
    add_block(explains)            # 8 explains

    return seq_out

def _total_items() -> int:
    return len(_fetch_sequence())

def _get_cursor(uid: int) -> int:
    """Return 0-based position into sequence (persist if lca_progress table exists, else session)."""
    try:
        row = db.session.execute(text("""
            SELECT cursor_index
            FROM lca_progress
            WHERE user_id = :uid
            ORDER BY updated_at DESC
            LIMIT 1
        """), {"uid": uid}).fetchone()
        if row and row[0] is not None:
            return int(row[0])
    except Exception:
        pass
    return int(session.get("loss_cursor", 0))

def _set_cursor(uid: int, idx: int) -> None:
    """Persist cursor when possible; else session."""
    now = datetime.utcnow().isoformat(timespec="seconds")
    try:
        db.session.execute(text("""
            INSERT INTO lca_progress (user_id, cursor_index, updated_at)
            VALUES (:uid, :idx, :ts)
        """), {"uid": uid, "idx": idx, "ts": now})
        db.session.commit()
        return
    except Exception:
        db.session.rollback()
    session["loss_cursor"] = idx

def _record_answer(uid: int, qid: int, answer: str) -> None:
    """
    Idempotently record an answer into lca_results (or similar). 
    Tries an UPDATE first; if 0 rows, INSERT.
    """
    now = datetime.utcnow().isoformat(timespec="seconds")
    answer = _lower(answer)
    try:
        updated = db.session.execute(text("""
            UPDATE lca_results
            SET answer = :ans, updated_at = :ts
            WHERE user_id = :uid AND question_id = :qid
        """), {"ans": answer, "ts": now, "uid": uid, "qid": qid}).rowcount
        if updated == 0:
            db.session.execute(text("""
                INSERT INTO lca_results (user_id, question_id, answer, created_at, updated_at)
                VALUES (:uid, :qid, :ans, :ts, :ts)
            """), {"uid": uid, "qid": qid, "ans": answer, "ts": now})
        db.session.commit()
    except Exception:
        db.session.rollback()
        current_app.logger.warning("LOSS: could not persist answer uid=%s q=%s", uid, qid)

def _fetch_sequence() -> list[dict]:
    return _build_loss_sequence()

def _lower(s: Optional[str]) -> str:
    return (s or "").strip().lower()

def _get_user_id() -> Optional[int]:
    return session.get("user_id")

def _execute_mappings(sql: str, params: Optional[dict] = None) -> List[Dict[str, Any]]:
    """
    Safe helper that executes a SELECT and returns a list of dict rows.
    Returns [] if the table/columns don't exist (dev-friendly).
    """
    try:
        rows = db.session.execute(text(sql), params or {}).mappings().all()
        return [dict(r) for r in rows]
    except Exception as e:
        current_app.logger.debug("LOSS SQL skipped (likely missing table/cols): %s", e)
        return []

def _fetch_cards_where(kind_sql_filter: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    lim = f" LIMIT {int(limit)}" if limit else ""
    sql = f"""
        SELECT
            id,
            COALESCE(kind,
                     CASE
                       WHEN lower(type) IN ('instr','inst','instruction') THEN 'instruction'
                       WHEN lower(type) IN ('pause','break')            THEN 'pause'
                       WHEN lower(type) IN ('explain','explanation')    THEN 'explain'
                       WHEN lower(type) IN ('question','q')            THEN 'question'
                       ELSE NULL
                     END
            ) AS kind,
            COALESCE(seq, number, id) AS seq,
            COALESCE(text, prompt, body) AS text,
            COALESCE(phase, NULL) AS phase
        FROM lca_question
        WHERE {kind_sql_filter}
        ORDER BY seq
        {lim}
    """
    return _execute_mappings(sql)

def _build_loss_sequence() -> List[Dict[str, Any]]:
    # 1) Pull relevant buckets
    instr   = _fetch_cards_where("lower(COALESCE(kind,type,'')) IN ('instruction','instr','inst')")
    qs      = _fetch_cards_where("lower(COALESCE(kind,type,'')) IN ('question','q')")
    pauses  = _fetch_cards_where("lower(COALESCE(kind,type,'')) IN ('pause','break')", limit=3)
    explains= _fetch_cards_where("lower(COALESCE(kind,type,'')) IN ('explain','explanation')", limit=8)

    # 2) Instructions: ensure 6 and Welcome first if present
    if instr:
        def is_welcome(c): return "welcome" in _lower(c.get("text"))
        welcomes = [c for c in instr if is_welcome(c)]
        others   = [c for c in instr if not is_welcome(c)]
        instr_sorted = (welcomes[:1] + others)[:6]
    else:
        instr_sorted = [
            {"id": -i, "kind": "instruction", "seq": i, "text": ("Welcome" if i == 1 else f"Instruction {i}"), "phase": None}
            for i in range(1, 7)
        ]

    # 3) Split questions into 1–25 and 26–50; synthesize if short
    first_25 = qs[:25]
    next_25  = qs[25:50]

    def synth_q(start_seq: int, count: int, base_id: int) -> List[Dict[str, Any]]:
        return [
            {
                "id": base_id - i,
                "kind": "question",
                "seq": start_seq + i,
                "text": f"Question {start_seq + i}",
                "phase": None,
            }
            for i in range(count)
        ]

    if len(first_25) < 25:
        first_25 += synth_q(len(first_25) + 1, 25 - len(first_25), base_id=-1000)
    if len(next_25) < 25:
        base_seq = 26
        next_25  += synth_q(base_seq + len(next_25), 25 - len(next_25), base_id=-2000)

    # 4) Pauses: ensure exactly 3
    while len(pauses) < 3:
        idx = len(pauses) + 1
        pauses.append({
            "id": -3000 - idx,
            "kind": "pause",
            "seq": 9000 + idx,
            "text": f"Take a short break #{idx}",
            "phase": None
        })
    pauses = pauses[:3]

    # 5) Explains: ensure exactly 8
    while len(explains) < 8:
        idx = len(explains) + 1
        explains.append({
            "id": -4000 - idx,
            "kind": "explain",
            "seq": 10000 + idx,
            "text": f"Explanation {idx}",
            "phase": None
        })
    explains = explains[:8]

    # 6) Assemble in exact order; label questions 1..50 for UI
    seq_out: List[Dict[str, Any]] = []
    q_counter = 0

    def add_block(items: List[Dict[str, Any]]):
        nonlocal q_counter
        for it in items:
            it = dict(it)  # copy
            if it["kind"] == "question":
                q_counter += 1
                it["q_number"] = q_counter
            seq_out.append(it)

    add_block(instr_sorted)       # 6 instructions
    add_block([pauses[0]])        # Pause A
    add_block(first_25)           # Q1-25
    add_block([pauses[1]])        # Pause B
    add_block(next_25)            # Q26-50
    add_block([pauses[2]])        # Pause C
    add_block(explains)           # 8 explains

    return seq_out

def _fetch_sequence() -> List[Dict[str, Any]]:
    return _build_loss_sequence()

def store_response(user_id: int, question_id: int, answer: str) -> None:
    """Upsert the user's Y/N answer and refresh the scorecard entry."""
    answer = "yes" if (answer or "").lower() == "yes" else "no"

    # 1) Update existing; if none updated, insert
    upd = db.session.execute(
        text("""
            UPDATE lca_response
               SET answer = :a
             WHERE user_id = :uid AND question_id = :qid
        """),
        {"a": answer, "uid": user_id, "qid": question_id},
    )
    if upd.rowcount == 0:
        db.session.execute(
            text("""
                INSERT INTO lca_response (user_id, question_id, answer)
                VALUES (:uid, :qid, :a)
            """),
            {"uid": user_id, "qid": question_id, "a": answer},
        )

    # 2) Refresh scorecard row for this user+question
    db.session.execute(
        text("DELETE FROM lca_scorecard WHERE user_id=:uid AND question_id=:qid"),
        {"uid": user_id, "qid": question_id},
    )
    db.session.execute(
        text("""
            INSERT INTO lca_scorecard (user_id, question_id, answer_type, phase_1, phase_2, phase_3, phase_4)
            SELECT :uid, :qid, :a, m.phase_1, m.phase_2, m.phase_3, m.phase_4
              FROM lca_question_phase_map m
             WHERE m.question_id=:qid AND m.answer_type=:a
        """),
        {"uid": user_id, "qid": question_id, "a": answer},
    )

    db.session.commit()

def _endpoint_exists(endpoint_name: str) -> bool:
    """Check if an endpoint (e.g. 'loss_bp.explain_flow') is registered."""
    try:
        return endpoint_name in current_app.view_functions
    except Exception:
        return False

def _finalize_and_redirect(user_id: int):
    """
    Called when all questions are done.
    If you have an 8-card explain sequence route, go there; else go straight to complete.
    This keeps your storyboard: Questions → (Explains) → Results.
    """
    # Prefer an explain sequence if you’ve registered it
    if _endpoint_exists("loss_bp.explain_flow"):
        return redirect(url_for("loss_bp.explain_flow"))

    # Otherwise finalize now
    return redirect(url_for("loss_bp.assessment_complete"))

def _lower(s): return (s or "").strip().lower()

def _fetch_cards(sql, params=None):
    try:
        rows = db.session.execute(text(sql), params or {}).mappings().all()
        return [dict(r) for r in rows if (r.get("text") or r.get("body") or r.get("prompt"))]
    except Exception:
        return []

def _instructions_6():
    # 6 instruction cards, try DB first
    items = _fetch_cards("""
        SELECT id,
               COALESCE(kind, CASE WHEN LOWER(type) IN ('inst','instr','instruction') THEN 'instruction' END) AS kind,
               COALESCE(seq, number, id) AS seq,
               COALESCE(text, prompt, body) AS text
        FROM lca_question
        WHERE LOWER(COALESCE(kind,type,'')) IN ('instruction','instr','inst')
        ORDER BY seq
        LIMIT 6
    """)
    if not items:
        items = [{"id": -i, "kind":"instruction","seq":i, "text": "Welcome" if i==1 else f"Instruction {i}"} for i in range(1,7)]
    # Make sure Welcome (if present) is first
    welcomes = [c for c in items if "welcome" in _lower(c["text"])]
    others   = [c for c in items if c not in welcomes]
    return (welcomes[:1] + others)[:6]

def _explains_8():
    items = _fetch_cards("""
        SELECT id,
               'explain' AS kind,
               COALESCE(seq, number, id) AS seq,
               COALESCE(text, prompt, body) AS text
        FROM lca_question
        WHERE LOWER(COALESCE(kind,type,'')) IN ('explain','explanation')
        ORDER BY seq
        LIMIT 8
    """)
    if not items:
        items = [{"id": -4000-i, "kind":"explain","seq":10000+i, "text": f"Explanation {i}"} for i in range(1,9)]
    return items

def _load_sequence():
    # order by seq_order, then id as tie-breaker just in case
    return (LcaSequence.query
            .order_by(LcaSequence.seq_order.asc(), LcaSequence.id.asc())
            .all())

def _progress_label(idx: int, q_range, total_block: int) -> str:
    """Return 'N of M' where N is absolute question number, M is global total."""
    # global total across all questions
    total_global = session.get("q_total_global") or LcaQuestion.query.count()

    # base = start-1 for ranged blocks, else 0
    if q_range and all(q_range):
        start = int(q_range[0])
        base = max(start - 1, 0)
    else:
        base = 0

    current_abs = base + idx + 1  # e.g., second block: 25 + (0+1) = 26
    return f"{current_abs} of {total_global}"

def _parse_range(label: str) -> tuple[int, int]:
    """Parse '1-25' into (1, 25). Fallback to full range if missing."""
    try:
        a, b = (label or "").replace(" ", "").split("-", 1)
        return int(a), int(b)
    except Exception:
        # default to whole set if bad label
        qmin, qmax = db.session.execute(text("SELECT MIN(number), MAX(number) FROM lca_question")).one()
        return int(qmin or 1), int(qmax or 50)

def _render_loss_card(name: str, **ctx):
    # tolerate both names: question.html and cards_question.html
    from jinja2 import TemplateNotFound
    candidates = [
        f"school_loss/cards/{name}.html",
        f"school_loss/cards/cards_{name}.html",
    ]
    last = None
    for c in candidates:
        try:
            return render_template(c, **ctx)
        except TemplateNotFound as e:
            last = e
    raise last

def _make_item(title: str, content: str, caption: str | None = None):
    return SimpleNamespace(title=title, content=content, caption=caption or "")

def _next_pos_after_questions():
    """
    Decide which sequence step to jump to after finishing a question block.
    Priority:
      1) q_seq_pos (set by sequence_step_view when entering questions)
      2) infer from q_range label in lca_sequence (e.g. '1-25' or '26-50')
      3) last_seq_pos (set on every sequence step render)
      4) fallback to 1
    """
    # 1) direct hand-off from sequence
    if session.get("q_seq_pos"):
        try:
            return int(session["q_seq_pos"]) + 1
        except Exception:
            pass

    # 2) infer from current question range
    q_range = session.get("q_range")
    if q_range and len(q_range) == 2:
        try:
            label = f"{int(q_range[0])}-{int(q_range[1])}"
            row = db.session.execute(
                text("""
                     SELECT seq_order
                     FROM lca_sequence
                     WHERE lower(content_type)='question'
                       AND optional_label = :lab
                """),
                {"lab": label},
            ).fetchone()
            if row:
                return int(row.seq_order) + 1
        except Exception:
            pass

    # 3) last step we rendered
    if session.get("last_seq_pos"):
        try:
            return int(session["last_seq_pos"]) + 1
        except Exception:
            pass

    # 4) absolute fallback
    return 1

def _set_q_range_from_label(label: str | None):
    """
    Accepts an optional_label like '1-25' or '26-50' and stores q_range=(start,end) in session.
    """
    if not label:
        session.pop("q_range", None)
        return
    try:
        start_s, end_s = label.split("-", 1)
        start, end = int(start_s.strip()), int(end_s.strip())
        session["q_range"] = (start, end)
    except Exception:
        session.pop("q_range", None)

def _parse_range(label: str | None):
    if not label:
        return None
    try:
        a, b = [int(x.strip()) for x in label.split("-", 1)]
        return (a, b)
    except Exception:
        return None

def round_half_up(x: float) -> int:
    return int(Decimal(str(x)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))

def clamp(n: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, n))

def generate_results_pdf(user_id: int) -> bytes:
    # TODO: replace with your real PDF generator
    return b"%PDF-1.4\n%...\n1 0 obj<<>>endobj\ntrailer<<>>\n%%EOF"

def email_results_report(user_id: int, pdf_bytes: bytes) -> None:
    # TODO: send email with pdf_bytes as attachment
    pass

def _finalize_and_send_pdf(user_id: int):
    pdf = generate_results_pdf(user_id)  # must return valid PDF bytes
    try:
        email_results_report(user_id, pdf)
    except Exception:
        current_app.logger.exception("Email send failed")
    # close program for this session
    for k in ("q_range","q_seq_pos","current_index","active_q_range",
              "loss_flow_done","loss_flow_in_progress"):
        session.pop(k, None)
    session["loss_flow_closed"] = True
    # return the file (no redirect)
    return send_file(BytesIO(pdf), mimetype="application/pdf",
                     as_attachment=True, download_name="loss_results.pdf")

def _default_phases():
    return [
        SimpleNamespace(id=1, name="Impact",        order_index=1, max_points=9,  points_per_item=1,
                        neutral_line="No notable markers in this phase."),
        SimpleNamespace(id=2, name="Hopelessness",  order_index=2, max_points=9,  points_per_item=1,
                        neutral_line="No notable markers in this phase."),
        SimpleNamespace(id=3, name="Helplessness",  order_index=3, max_points=16, points_per_item=2,
                        neutral_line="No notable markers in this phase."),
        SimpleNamespace(id=4, name="Re-Engagement", order_index=4, max_points=16, points_per_item=2,
                        neutral_line="No notable markers in this phase."),
    ]

def _clamp(n: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, n))

def _clamp(n: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, n))

def _load_phase_points_for_user(user_id: int):
    latest = (db.session.query(LcaResult)
              .filter_by(user_id=user_id)
              .order_by(LcaResult.created_at.desc())
              .first())
    if latest:
        pts = {
            1: float(latest.phase_1 or 0),
            2: float(latest.phase_2 or 0),
            3: float(latest.phase_3 or 0),
            4: float(latest.phase_4 or 0),
        }
        return pts, latest.created_at

    yes_counts = (db.session.query(LcaQuestion.phase, func.count())
                  .join(LcaResponse, LcaResponse.question_id == LcaQuestion.id)
                  .filter(LcaResponse.user_id == user_id,
                          func.lower(LcaResponse.answer) == "yes")
                  .group_by(LcaQuestion.phase)
                  .all())
    pts = {1: 0.0, 2: 0.0, 3: 0.0, 4: 0.0}
    for ph, cnt in yes_counts:
        ip = int(ph)
        if ip in pts:
            pts[ip] = float(cnt)
    return pts, None

def _round_half_up(x: float) -> int:
    return int(Decimal(str(x)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))

def _round_half_up(x: float) -> int:
    import decimal
    D = decimal.Decimal
    decimal.getcontext().rounding = decimal.ROUND_HALF_UP
    return int(D(str(x)).to_integral_value())

def _load_phase_points_for_user(user_id: int):
    """
    Compute phase totals strictly from lca_response ⨝ lca_scoring_map.
    Returns (points_dict, created_at_of_latest_response_or_None)
    """
    # Latest response timestamp (optional, for header)
    latest_ts = (db.session.query(func.max(LcaResponse.created_at))
                 .filter(LcaResponse.user_id == user_id)
                 .scalar())

    # Sum phase weights from the mapping for the user's actual answers
    sums = (db.session.query(
                func.coalesce(func.sum(LcaScoringMap.phase_1), 0),
                func.coalesce(func.sum(LcaScoringMap.phase_2), 0),
                func.coalesce(func.sum(LcaScoringMap.phase_3), 0),
                func.coalesce(func.sum(LcaScoringMap.phase_4), 0),
            )
            .join(LcaResponse, (LcaResponse.question_id == LcaScoringMap.question_id) &
                               (func.lower(LcaResponse.answer) == LcaScoringMap.answer_type))
            .filter(LcaResponse.user_id == user_id)
            .one())

    pts = {1: float(sums[0]), 2: float(sums[1]), 3: float(sums[2]), 4: float(sums[3])}
    return pts, latest_ts

def _to_bool_int(v):
    if v is None:
        return 0
    s = str(v).strip().lower()
    if s in ("1", "true", "t", "yes", "y"):
        return 1
    if s in ("0", "false", "f", "no", "n", ""):
        return 0
    try:
        return 1 if int(s) != 0 else 0
    except Exception:
        return 0  # be forgiving

def _current_user_id() -> int:
    if current_user and getattr(current_user, "is_authenticated", False):
        return int(current_user.id)
    uid = session.get("user_id") or getattr(g, "user_id", None)
    if uid is None:
        # Dev-friendly: either abort or default to 1
        # abort(401)
        return 1
    return int(uid)




    # 2) reset cursor and go
    session["q_seq_pos"] = 1
    return redirect(url_for("loss_bp.sequence_step", pos=1))

def _current_user_id() -> int:
    if current_user and getattr(current_user, "is_authenticated", False):
        return int(current_user.id)
    # fallback to session for your app
    return int(session.get("user_id", 1))

def _start_new_run_for(uid: int) -> int:
    db.session.execute(text("""
        INSERT INTO lca_run (user_id, started_at, status)
        VALUES (:uid, datetime('now'), 'in_progress')
    """), {"uid": uid})
    rid = db.session.execute(text("SELECT last_insert_rowid()")).scalar()
    db.session.commit()
    session["loss_run_id"] = rid
    return rid

def _current_run_id() -> int:
    rid = session.get("loss_run_id")
    if rid:
        return int(rid)
    # if missing (e.g. user hit a deep link), start a run now
    return _start_new_run_for(_current_user_id())

def _qid_from_ident(ident: int) -> int:
    """
    If your sequence 'ident' already equals lca_question.id, this returns ident.
    If your questions use a 'number' column (1..50), uncomment the lookup.
    """
    # Uncomment if you have lca_question.number:
    # qid = db.session.execute(text(
    #     "SELECT id FROM lca_question WHERE number = :n"
    # ), {"n": ident}).scalar()
    # return int(qid or ident)
    return int(ident)

def save_answer(uid: int, rid: int, qid: int, answer: str) -> None:
    answer = (answer or "").strip().lower()
    db.session.execute(text("""
        INSERT INTO lca_response (user_id, run_id, question_id, answer)
        VALUES (:uid, :rid, :qid, :answer)
        ON CONFLICT(run_id, question_id)
        DO UPDATE SET answer = excluded.answer
    """), {"uid": uid, "rid": rid, "qid": qid, "answer": answer})
    db.session.commit()

def _finish_run() -> None:
    rid = session.get("loss_run_id")
    if rid:
        db.session.execute(text("""
            UPDATE lca_run
            SET finished_at = datetime('now'), status = 'finished'
            WHERE id = :rid
        """), {"rid": int(rid)})
        db.session.commit()
        session.pop("loss_run_id", None)

def _persist_results_for_run(run_id: int):
    totals = db.session.execute(text("""
        SELECT
          SUM(CASE WHEN lower(r.answer)='yes' THEN sd.p1 ELSE 0 END) AS p1_raw,
          SUM(CASE WHEN lower(r.answer)='yes' THEN sd.p2 ELSE 0 END) AS p2_raw,
          SUM(CASE WHEN lower(r.answer)='yes' THEN sd.p3 ELSE 0 END) AS p3_raw,
          SUM(CASE WHEN lower(r.answer)='yes' THEN sd.p4 ELSE 0 END) AS p4_raw
        FROM lca_response r
        JOIN lca_score_definitions sd ON sd.question_id = r.question_id
        WHERE r.run_id = :rid
    """), {"rid": run_id}).mappings().first()

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
    """), {
        "rid": run_id,
        "p1": totals["p1_raw"] or 0,
        "p2": totals["p2_raw"] or 0,
        "p3": totals["p3_raw"] or 0,
        "p4": totals["p4_raw"] or 0,
    })
    db.session.commit()

def resolve_uid_rid():
    uid = request.args.get("uid", type=int) or session.get("user_id")
    rid = request.args.get("run_id", type=int) or session.get("loss_run_id")
    if not rid and uid:
        rid = db.session.execute(text("""
            SELECT id FROM lca_run WHERE user_id=:uid ORDER BY id DESC LIMIT 1
        """), {"uid": int(uid)}).scalar()
    return int(uid) if uid else None, int(rid) if rid else None

SUBJECT = "loss"         # <- subject tag used in lca_run / lca_result


def _current_user_id():
    return int(session["user_id"])

def _sequence():
    # Example: intro (1..2), questions block A, explain #8, questions block B, then finalize
    # Adjust to your real flow; important part is we know "explain #8" is the stop point.
    return [
        ("instruction", 1),
        ("instruction", 2),
        ("question_block", (1, 25)),
        ("explain", 8),
        ("question_block", (26, 50)),
        ("finish", 1),
    ]

def _start_run(uid: int) -> int:
    db.session.execute(text("""
        INSERT INTO lca_run (user_id, subject, status, started_at)
        VALUES (:uid, :subj, 'in_progress', datetime('now'))
    """), {"uid": uid, "subj": SUBJECT})
    rid = db.session.execute(text("SELECT last_insert_rowid()")).scalar()
    db.session.commit()
    return int(rid)

def _sum_scorecard(rid: int):
    return db.session.execute(text("""
        SELECT
          SUM(phase_1) AS p1,
          SUM(phase_2) AS p2,
          SUM(phase_3) AS p3,
          SUM(phase_4) AS p4,
          SUM(phase_1+phase_2+phase_3+phase_4) AS total
        FROM lca_scorecard
        WHERE run_id = :rid
    """), {"rid": rid}).mappings().first()

def _latest_run_id_for_user(uid: int) -> int | None:
    """Return latest lca_run.id for the user, or None."""
    return db.session.execute(
        text("SELECT id FROM lca_run WHERE user_id=:uid ORDER BY id DESC LIMIT 1"),
        {"uid": int(uid)}
    ).scalar()

def rebuild_scorecard_for_run(rid: int):
    # Delete existing rows for this run
    db.session.execute(text("DELETE FROM lca_scorecard WHERE run_id = :rid"), {"rid": rid})
    # Recreate from responses + phase map
    db.session.execute(text("""
        INSERT INTO lca_scorecard
            (run_id, question_id, answer_type, phase_1, phase_2, phase_3, phase_4)
        SELECT
            r.run_id,
            r.question_id,
            r.answer      AS answer_type,
            m.phase_1,
            m.phase_2,
            m.phase_3,
            m.phase_4
        FROM lca_response r
        JOIN lca_question_phase_map m
          ON m.question_id = r.question_id
         AND m.answer_type = r.answer
        WHERE r.run_id = :rid
    """), {"rid": rid})
    db.session.commit()

def phase_items_for_score(phase_id: int, score: int) -> list[str]:
    """
    Given a phase_id and a raw score, select the correct number of items
    from lca_phase_item based on 'points per item' logic:
      - points_per_item = max_score / item_count
      - items_to_show   = round_half_up(score / points_per_item)
    Returns the list of item bodies to display (ordered by ordinal).
    """
    # Phase max scores (fixed by your scoring design)
    phase_max = {1: 18, 2: 18, 3: 32, 4: 32}[phase_id]

    # Fetch active items for this phase
    rows = db.session.execute(text("""
        SELECT body
        FROM lca_phase_item
        WHERE phase_id = :p AND active = 1
        ORDER BY ordinal, id
    """), {"p": phase_id}).scalars().all()

    item_count = len(rows)
    if item_count == 0:
        return []

    # Calculate how many points each item "costs"
    points_per_item = phase_max / item_count  # e.g. 18/9=2, 32/8=4

    # How many items to show, using half-up rounding
    ratio = float(score) / float(points_per_item)
    k = _round_half_up(ratio)

    # Clamp to available items
    k = max(0, min(k, item_count))

    return rows[:k]

def create_loss_run_for_user(user_id: int) -> int:
    """Create or reuse an active LOSS run for this user. Keep it local to avoid circular imports."""
    with db.engine.begin() as conn:
        # Reuse latest active LOSS run if present
        rid = conn.scalar(text("""
            SELECT id FROM lca_run
            WHERE user_id=:uid AND subject='LOSS' AND status='active'
            ORDER BY id DESC LIMIT 1
        """), {"uid": user_id})
        if rid:
            return int(rid)

        # Create a new run
        conn.execute(text("""
            INSERT INTO lca_run (user_id, subject, status, created_at)
            VALUES (:uid, 'LOSS', 'active', :ts)
        """), {"uid": user_id, "ts": datetime.utcnow()})

        # Return the new id
        rid = conn.scalar(text("""
            SELECT id FROM lca_run
            WHERE user_id=:uid AND subject='LOSS'
            ORDER BY id DESC LIMIT 1
        """), {"uid": user_id})
        return int(rid)

def compute_loss_results(run_id: int, user_id: int):
    """
    Compute phase totals for a LOSS run and upsert a row in lca_result.
    Prefers a view `lca_scorecard_v` if you have it; otherwise falls back
    to joining responses + mappings + score definitions.
    Returns a dict with phase totals.
    """
    with db.engine.begin() as conn:
        # 1) Try the convenient view first
        try:
            sums = conn.execute(text("""
                SELECT
                  COALESCE(SUM(phase_1), 0) AS p1,
                  COALESCE(SUM(phase_2), 0) AS p2,
                  COALESCE(SUM(phase_3), 0) AS p3,
                  COALESCE(SUM(phase_4), 0) AS p4
                FROM lca_scorecard_v
                WHERE run_id = :rid
            """), {"rid": run_id}).mappings().first()
        except Exception:
            sums = None

        # 2) Fallback: compute from raw tables
        if not sums or sums is None:
            sums = conn.execute(text("""
                SELECT
                  COALESCE(SUM(CASE WHEN m.phase = 1 AND r.answer = 'yes' THEN s.phase_1 ELSE 0 END), 0) AS p1,
                  COALESCE(SUM(CASE WHEN m.phase = 2 AND r.answer = 'yes' THEN s.phase_2 ELSE 0 END), 0) AS p2,
                  COALESCE(SUM(CASE WHEN m.phase = 3 AND r.answer = 'yes' THEN s.phase_3 ELSE 0 END), 0) AS p3,
                  COALESCE(SUM(CASE WHEN m.phase = 4 AND r.answer = 'yes' THEN s.phase_4 ELSE 0 END), 0) AS p4
                FROM lca_response r
                JOIN lca_question_phase_map m ON m.question_id = r.question_id
                JOIN lca_score_definitions s ON s.question_id = r.question_id
               WHERE r.run_id = :rid
            """), {"rid": run_id}).mappings().first()

        p1 = int(sums.get("p1", 0) if sums else 0)
        p2 = int(sums.get("p2", 0) if sums else 0)
        p3 = int(sums.get("p3", 0) if sums else 0)
        p4 = int(sums.get("p4", 0) if sums else 0)
        total = p1 + p2 + p3 + p4

        # Upsert result row (DELETE + INSERT works on SQLite without constraints)
        conn.execute(text("DELETE FROM lca_result WHERE run_id = :rid"), {"rid": run_id})
        conn.execute(text("""
            INSERT INTO lca_result
                (user_id, phase_1, phase_2, phase_3, phase_4, total, run_id, subject, created_at)
            VALUES
                (:uid, :p1, :p2, :p3, :p4, :tot, :rid, 'LOSS', CURRENT_TIMESTAMP)
        """), {"uid": user_id, "rid": run_id, "p1": p1, "p2": p2, "p3": p3, "p4": p4, "tot": total})

        return {"phase_1": p1, "phase_2": p2, "phase_3": p3, "phase_4": p4, "total": total}


def finalize_run_totals(rid: int, uid: int):
    # Insert/Upsert totals
    db.session.execute(text("""
        INSERT INTO lca_result
          (run_id, user_id, subject, phase_1, phase_2, phase_3, phase_4, score_total, created_at)
        SELECT
          r.run_id,
          r.user_id,
          'loss' AS subject,
          SUM(sc.phase_1) AS p1,
          SUM(sc.phase_2) AS p2,
          SUM(sc.phase_3) AS p3,
          SUM(sc.phase_4) AS p4,
          SUM(sc.phase_1 + sc.phase_2 + sc.phase_3 + sc.phase_4) AS total,
          MAX(r.created_at) AS created_at
        FROM lca_scorecard sc
        JOIN lca_response r
          ON r.run_id = sc.run_id
         AND r.question_id = sc.question_id
        WHERE sc.run_id = :rid AND r.user_id = :uid
        GROUP BY r.run_id, r.user_id
        ON CONFLICT(run_id) DO UPDATE SET
          user_id     = excluded.user_id,
          subject     = excluded.subject,
          phase_1     = excluded.phase_1,
          phase_2     = excluded.phase_2,
          phase_3     = excluded.phase_3,
          phase_4     = excluded.phase_4,
          score_total = excluded.score_total,
          created_at  = excluded.created_at
    """), {"rid": rid, "uid": uid})

    db.session.execute(text("""
        UPDATE lca_run
           SET status='complete', finished_at = COALESCE(finished_at, datetime('now'))
         WHERE id = :rid
    """), {"rid": rid})

    db.session.commit()

def finish_loss_run(run_id):
    ts = datetime.utcnow().isoformat(timespec="seconds")
    with db.engine.begin() as conn:
        conn.execute(text("""
            UPDATE lca_run
               SET status='finished', finished_at=:ts
             WHERE id=:rid
        """), {"rid": run_id, "ts": ts})

def _coerce_dt(s: str | None):
    """Parse common SQLite/ISO datetime strings -> datetime | None."""
    if not s:
        return None
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None

def finish_and_redirect(run_id, user_id):
    compute_loss_results(run_id, user_id)
    finish_loss_run(run_id)

    ret = session.pop("loss_return_to", None)
    if ret:
        # deep-link admin to the selected run
        params = ret.get("params", {})
        params["run_id"] = run_id
        return redirect(url_for(ret["endpoint"], **params))

    # otherwise normal learner flow
    return redirect(url_for("loss_bp.result_run", run_id=run_id))

def _get_int_arg(name, required=False):
    v = request.args.get(name) or request.form.get(name)
    try:
        return int(v) if v not in (None, "") else None
    except Exception:
        return None

def _render_loss_template(base_name: str, **ctx):
    """
    base_name: 'report.html' or 'report_pdf.html'
    Tries 'loss/<base_name>' then 'admin/loss/<base_name>'
    Logs which one succeeded.
    """
    try:
        html = render_template(f"loss/{base_name}", **ctx)
        current_app.logger.info("Rendered learner template loss/%s", base_name)
        return html
    except TemplateNotFound:
        current_app.logger.info("Learner template missing, using admin/loss/%s", base_name)
        return render_template(f"admin/loss/{base_name}", **ctx)
def _make_pdf(html_str: str) -> bytes | None:
    if not WEASYPRINT_AVAILABLE:
        return None
    pdf_bytes = HTML(string=html_str).write_pdf()
    return pdf_bytes

def _render_loss_pdf_bytes(run_id: int, ctx: dict) -> bytes:
    """
    Render your existing PDF template with pdf_mode=True and return raw bytes.
    """
    html = render_template(
        "subject/loss/report_pdf.html",  # your existing PDF layout that includes the same partials
        pdf_mode=True,
        run_id=run_id,
        phase_blocks=ctx.get("phase_blocks") or [],
        overall_assessment=ctx.get("overall_assessment"),
        viewer_is_admin=False,
        taken_at=ctx.get("taken_at_str") or ctx.get("taken_at") or "",
    )
    # Use WeasyPrint or your existing renderer
    from weasyprint import HTML
    return HTML(string=html, base_url=request.host_url).write_pdf()

def _send_pdf_email_smtp(to_email: str, pdf_bytes: bytes, filename: str, subject: str, body: str):
    cfg = current_app.config
    host    = (cfg.get("MAIL_SERVER") or "").lower()
    port    = int(cfg.get("MAIL_PORT") or 587)
    use_tls = bool(cfg.get("MAIL_USE_TLS", True))
    username = cfg.get("MAIL_USERNAME")            # gmail mailbox
    password = cfg.get("MAIL_PASSWORD")
    default_sender = cfg.get("MAIL_DEFAULT_SENDER")

    # Resolve display name + desired sender address (if any)
    display_name, desired_from = "AIT Platform", None
    if isinstance(default_sender, (list, tuple)) and len(default_sender) == 2:
        display_name, desired_from = default_sender
    elif isinstance(default_sender, str) and "@" in default_sender:
        desired_from = default_sender.split("<")[-1].split(">")[0].strip()
        if "<" in default_sender:
            dn = default_sender.split("<")[0].strip().strip('"')
            if dn:
                display_name = dn

    # If using Gmail SMTP, the envelope FROM must be the authenticated mailbox.
    is_gmail = ("smtp.gmail.com" in host) or ("googlemail.com" in host)
    if is_gmail:
        sender_email = username  # force envelope/header From to the gmail account
    else:
        sender_email = desired_from or username

    if not (host and sender_email and username and password):
        current_app.logger.error(
            "Email misconfigured: host=%r sender=%r username=%r (need all set)",
            host, sender_email, username
        )
        return

    from email.message import EmailMessage
    import smtplib

    msg = EmailMessage()
    # For Gmail, keep header From matching the authenticated account
    msg["From"] = f"{display_name} <{sender_email}>"
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.set_content(body)
    msg.add_attachment(pdf_bytes, maintype="application", subtype="pdf", filename=filename)

    with smtplib.SMTP(host, port) as s:
        if use_tls:
            s.starttls()
        s.login(username, password)
        # Envelope sender must be the authenticated mailbox on Gmail
        s.send_message(msg, from_addr=sender_email)

    current_app.logger.info("Email sent to %s via %s as %s", to_email, host, sender_email)

def _extract_phase_scores_from_ctx(ctx: dict) -> list[int]:
    """
    Build a list of 4 ints (0..100) in phase order 1..4 from ctx['phase_blocks'] (or ctx['blocks']).
    Falls back gracefully if keys are missing.
    """
    blocks = ctx.get("phase_blocks") or ctx.get("blocks") or []
    scores = [0, 0, 0, 0]  # P1..P4 default to 0

    for blk in blocks:
        try:
            phase = int(blk.get("phase"))
        except Exception:
            continue
        if 1 <= phase <= 4:
            # Prefer blk.pct; fallback to blk.width_pct; default 0
            pct = blk.get("pct")
            if pct is None:
                pct = blk.get("width_pct")
            try:
                scores[phase - 1] = int(pct or 0)
            except Exception:
                scores[phase - 1] = 0

    return scores

def _phase_graph_data_uri_for_run(run_id: int) -> str | None:
    row = db.session.execute(text("""
        SELECT phase_1, phase_2, phase_3, phase_4,
               max_phase_1, max_phase_2, max_phase_3, max_phase_4
        FROM lca_result
        WHERE run_id = :rid
        ORDER BY id DESC
        LIMIT 1
    """), {"rid": run_id}).mappings().first()
    if not row:
        return None

    def pct(v, m):
        try:
            v = float(v or 0); m = float(m or 0)
            return max(0, min(100, int(round(100 * v / m)))) if m > 0 else 0
        except Exception:
            return 0

    scores = {
        "P1": pct(row["phase_1"], row["max_phase_1"]),
        "P2": pct(row["phase_2"], row["max_phase_2"]),
        "P3": pct(row["phase_3"], row["max_phase_3"]),
        "P4": pct(row["phase_4"], row["max_phase_4"]),
    }

    try:
        data_uri, _png_bytes = phase_scores_bar(scores)  # returns (data_uri, bytes)
        return data_uri
    except Exception:
        return None

def _extract_phase_scores_from_ctx(ctx):
    """Return [p1,p2,p3,p4] ints 0..100 from whatever the ctx has."""
    blocks = ctx.get("phase_blocks") or ctx.get("blocks") or []
    vals = []
    if blocks:
        def _get(d, k): return d.get(k) if isinstance(d, dict) else getattr(d, k, None)
        try:
            blocks = sorted(blocks, key=lambda b: int(_get(b, "phase") or 0))
        except Exception:
            pass
        for b in blocks[:4]:
            v = _get(b, "pct") or _get(b, "width_pct") or 0
            try: vals.append(max(0, min(100, int(v))))
            except Exception: vals.append(0)
    else:
        for k in ("p1_pct", "p2_pct", "p3_pct", "p4_pct"):
            try: vals.append(max(0, min(100, int(ctx.get(k) or 0))))
            except Exception: vals.append(0)
    while len(vals) < 4: vals.append(0)
    return vals[:4]

def _scores_from_blocks(blocks):
    scores = {"P1": 0, "P2": 0, "P3": 0, "P4": 0}
    for b in (blocks or []):
        # allow dicts or objects
        phase = getattr(b, "phase", None)
        if phase is None and isinstance(b, dict):
            phase = b.get("phase")

        pct = getattr(b, "pct", None)
        if pct is None:
            pct = getattr(b, "width_pct", None)
        if pct is None and isinstance(b, dict):
            pct = b.get("pct") or b.get("width_pct")

        if phase in (1, 2, 3, 4) and pct is not None:
            try:
                scores[f"P{phase}"] = int(float(pct))
            except Exception:
                pass
    return scores

def _phase_scores_pct_from_db(run_id: int) -> dict:
    row = db.session.execute(text("""
        SELECT phase_1, phase_2, phase_3, phase_4,
               max_phase_1, max_phase_2, max_phase_3, max_phase_4
        FROM lca_result
        WHERE run_id = :rid
        ORDER BY id DESC
        LIMIT 1
    """), {"rid": run_id}).mappings().first()

    if not row:
        return {"P1": 0, "P2": 0, "P3": 0, "P4": 0}

    def pct(v, m):
        try:
            v = float(v or 0)
            m = float(m or 0)
            return int(round(100 * v / m)) if m > 0 else 0
        except Exception:
            return 0

    return {
        "P1": pct(row["phase_1"], row["max_phase_1"]),
        "P2": pct(row["phase_2"], row["max_phase_2"]),
        "P3": pct(row["phase_3"], row["max_phase_3"]),
        "P4": pct(row["phase_4"], row["max_phase_4"]),
    }

def _loss_result_percents(run_id: int, user_id: int | None = None):
    """
    Read phase totals + maxima from lca_result for this run (and user if given),
    then return integer percentages { 'P1': int, ... }.
    """
    row = db.session.execute(text("""
        SELECT phase_1, phase_2, phase_3, phase_4,
               max_phase_1, max_phase_2, max_phase_3, max_phase_4
        FROM lca_result
        WHERE run_id = :rid
          AND (:uid IS NULL OR user_id = :uid)
        ORDER BY id DESC
        LIMIT 1
    """), {"rid": run_id, "uid": user_id}).mappings().first()

    if not row:
        return None

    def pct(v, m):
        try:
            v = Decimal(v or 0)
            m = Decimal(m or 0)
            if m <= 0:
                return 0
            return int((v * Decimal(100) / m).quantize(0, rounding=ROUND_HALF_UP))
        except Exception:
            return 0

    return {
        "P1": pct(row["phase_1"], row["max_phase_1"]),
        "P2": pct(row["phase_2"], row["max_phase_2"]),
        "P3": pct(row["phase_3"], row["max_phase_3"]),
        "P4": pct(row["phase_4"], row["max_phase_4"]),
    }

def viewer_is_admin() -> bool:
    role = getattr(current_user, "role", None)
    # extend as needed (e.g. 'superadmin', 'subject_admin', etc.)
    return bool(current_user.is_authenticated and role in {"admin", "loss_admin", "subject_admin", "superadmin"})

def latest_run_for_user(user_id: int):
    return (
        db.session.query(LcaRun)
        .filter(LcaRun.user_id == user_id)
        .order_by(LcaRun.id.desc())
        .first()
    )

def ensure_lca_result(run_id: int) -> LcaResult | None:
    """
    Ensures there is an LcaResult row for run_id. If you already
    have a proper score builder, call that here instead.
    """
    run = db.session.get(LcaRun, run_id)
    if not run:
        return None

    res = db.session.query(LcaResult).filter_by(run_id=run_id).first()
    if res:
        return res

    # Minimal safe placeholder; replace with your real computation
    res = LcaResult(
        user_id=run.user_id,
        run_id=run.id,
        subject="LOSS",
        phase_1=0, phase_2=0, phase_3=0, phase_4=0, total=0,
    )
    db.session.add(res)
    db.session.commit()
    return res

def _came_from_admin() -> bool:
    ref = request.referrer
    if not ref:
        return False
    try:
        path = urlparse(ref).path or ""
    except Exception:
        return False
    return path.startswith("/admin/")

def _get_user_id_for_run(run_id: int) -> int | None:
    row = db.session.execute(
        text("SELECT user_id FROM lca_result WHERE run_id=:rid LIMIT 1"),
        {"rid": run_id},
    ).mappings().first() or db.session.execute(
        text("SELECT user_id FROM lca_run WHERE id=:rid LIMIT 1"),
        {"rid": run_id},
    ).mappings().first()
    return (row or {}).get("user_id")

def _html_to_pdf(html: str) -> bytes:
    from weasyprint import HTML
    return HTML(string=html, base_url=request.host_url).write_pdf()

def _infer_user_id_for_run(run_id: int):
    row = db.session.execute(
        text("SELECT user_id FROM lca_result WHERE run_id=:rid LIMIT 1"),
        {"rid": run_id}
    ).mappings().first() or db.session.execute(
        text("SELECT user_id FROM lca_run WHERE id=:rid LIMIT 1"),
        {"rid": run_id}
    ).mappings().first()
    return row["user_id"] if row else None
