from flask import Blueprint, render_template, redirect, url_for, request, flash,session
import sqlite3
from app.models.loss import (
    LcaResult, db, 
    LcaSequence, LcaInstruction, LcaExplain, LcaQuestion,
    LcaQuestionPhaseMap, LcaResponse    )
from app.utils.db_utils import get_db_connection
from sqlalchemy import func, and_


# assessment_helper.py

def calculate_phase_scores(answer, phase_1, phase_2, phase_3, phase_4):
    """
    Helper function to calculate phase scores based on the user's answer.
    
    Args:
        answer (str): The user's answer ('yes' or 'no').
        phase_1 (int): The score for phase 1.
        phase_2 (int): The score for phase 2.
        phase_3 (int): The score for phase 3.
        phase_4 (int): The score for phase 4.
    
    Returns:
        tuple: A tuple containing the scores for each phase (phase_1_score, phase_2_score, phase_3_score, phase_4_score).
    """
    # If the answer is 'yes', use the score for that phase. Otherwise, score is 0.
    phase_1_score = phase_1 if answer == 'yes' else 0
    phase_2_score = phase_2 if answer == 'yes' else 0
    phase_3_score = phase_3 if answer == 'yes' else 0
    phase_4_score = phase_4 if answer == 'yes' else 0

    return phase_1_score, phase_2_score, phase_3_score, phase_4_score

def get_user_responses(form_data):
    """
    Helper function to extract user responses from form data.
    
    Args:
        form_data (MultiDict): The form data containing user responses.
    
    Returns:
        dict: A dictionary where keys are question IDs and values are answers ('yes' or 'no').
    """
    responses = {}
    for question_id, answer in form_data.items():
        if question_id.startswith("question_"):  # Ensure we only process questions
            responses[question_id] = answer
    return responses

def insert_response_to_scorecard(cursor, user_id, question_id, answer, phase_1_score, phase_2_score, phase_3_score, phase_4_score):
    """
    Helper function to insert a response into the scorecard table.
    
    Args:
        cursor (sqlite3.Cursor): The database cursor to execute the SQL query.
        user_id (int): The ID of the user answering the question.
        question_id (int): The ID of the question being answered.
        answer (str): The user's answer ('yes' or 'no').
        phase_1_score (int): The calculated score for phase 1.
        phase_2_score (int): The calculated score for phase 2.
        phase_3_score (int): The calculated score for phase 3.
        phase_4_score (int): The calculated score for phase 4.
    """
    cursor.execute("""
        INSERT INTO lca_scorecard (user_id, question_id, answer_type, phase_1, phase_2, phase_3, phase_4)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (user_id, question_id, answer, phase_1_score, phase_2_score, phase_3_score, phase_4_score))

from sqlalchemy import text

from sqlalchemy import text

def store_response(user_id: int, question_id: int, answer: str) -> None:
    db.session.execute(text("""
        INSERT INTO lca_response (user_id, question_id, answer)
        VALUES (:uid, :qid, :ans)
        ON CONFLICT(user_id, question_id) DO UPDATE SET
          answer = excluded.answer,
          created_at = CURRENT_TIMESTAMP
    """), {"uid": user_id, "qid": question_id, "ans": answer})
    db.session.commit()


def capture_and_store_response(user_id: int, question_id: int, answer: str) -> dict:
    """
    Read-only lookup from lca_question_phase_map for the given answer,
    write the user's response via store_response, and return the phase scores.
    """
    pm = (LcaQuestionPhaseMap.query
          .filter_by(question_id=question_id, answer_type=answer)
          .first())
    if not pm:
        raise ValueError(f"No phase map row for Q{question_id} / '{answer}'")

    store_response(user_id, question_id, answer)

    return {"p1": pm.phase_1, "p2": pm.phase_2, "p3": pm.phase_3, "p4": pm.phase_4}


def compute_phase_totals_for_user(user_id:int):
    q = db.session.query(
        func.coalesce(func.sum(LcaQuestionPhaseMap.phase_1), 0),
        func.coalesce(func.sum(LcaQuestionPhaseMap.phase_2), 0),
        func.coalesce(func.sum(LcaQuestionPhaseMap.phase_3), 0),
        func.coalesce(func.sum(LcaQuestionPhaseMap.phase_4), 0),
    ).join(
        LcaResponse,
        and_(
            LcaResponse.question_id == LcaQuestionPhaseMap.question_id,
            LcaResponse.answer == LcaQuestionPhaseMap.answer_type
        )
    ).filter(LcaResponse.user_id == user_id)

    p1, p2, p3, p4 = q.one()
    return int(p1), int(p2), int(p3), int(p4)

def compute_phase_max_possible():
    q = db.session.query(
        func.coalesce(func.sum(LcaQuestionPhaseMap.phase_1), 0),
        func.coalesce(func.sum(LcaQuestionPhaseMap.phase_2), 0),
        func.coalesce(func.sum(LcaQuestionPhaseMap.phase_3), 0),
        func.coalesce(func.sum(LcaQuestionPhaseMap.phase_4), 0),
    ).filter(LcaQuestionPhaseMap.answer_type == 'yes')  # assume “yes” rows hold max per question
    m1, m2, m3, m4 = q.one()
    return int(m1), int(m2), int(m3), int(m4)

def to_percent(x, m): 
    return round((x / m * 100), 1) if m else 0.0

def get_phase_column_for_question(question_id, cursor):
    """Returns the phase column name for a given question_id from lca_questions"""
    cursor.execute("""
        SELECT phase_1, phase_2, phase_3, phase_4
        FROM lca_questions
        WHERE question_id = ?
        LIMIT 1
    """, (question_id,))
    row = cursor.fetchone()

    if row:
        for i, val in enumerate(row):
            if val == 1:
                return f"phase_{i+1}"
    return None

from sqlalchemy import func, and_

def finalize_results_for_user(user_id: int):
    # Sum phases for answered questions
    p1, p2, p3, p4 = db.session.query(
        func.coalesce(func.sum(LcaQuestionPhaseMap.phase_1), 0),
        func.coalesce(func.sum(LcaQuestionPhaseMap.phase_2), 0),
        func.coalesce(func.sum(LcaQuestionPhaseMap.phase_3), 0),
        func.coalesce(func.sum(LcaQuestionPhaseMap.phase_4), 0),
    ).join(
        LcaResponse,
        and_(
            LcaResponse.question_id == LcaQuestionPhaseMap.question_id,
            LcaResponse.answer == LcaQuestionPhaseMap.answer_type
        )
    ).filter(LcaResponse.user_id == user_id).one()

    total = int(p1) + int(p2) + int(p3) + int(p4)

    # Save snapshot
    res = LcaResult(user_id=user_id, phase_1=p1, phase_2=p2, phase_3=p3, phase_4=p4, total=total)
    db.session.add(res)
    db.session.commit()
    return res.id


def record_user_response(user_id, question_id, answer_type):
    import sqlite3
    conn = get_db_connection()
    cursor = conn.cursor()



    # ✅ FIX: Query from lca_sequence, not lca_scorecard
    cursor.execute("""
        SELECT phase_1, phase_2, phase_3, phase_4
        FROM lca_sequence
        WHERE question_id = ? AND answer_type = ?
        LIMIT 1
    """, (question_id, answer_type))
    row = cursor.fetchone()

    if not row:
        raise ValueError(f"No matching question found for question_id={question_id} and answer_type='{answer_type}'")

    # ✅ Insert into lca_scorecard
    cursor.execute("""
        INSERT INTO lca_scorecard (user_id, question_id, answer_type, phase_1, phase_2, phase_3, phase_4)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (user_id, question_id, answer_type, *row))

    conn.commit()
    conn.close()

def process_questions(user_id, session):
    """Loop through all 50 questions and store responses"""
    # Loop through the 50 questions (or as many questions as you have)
    questions = LcaSequence.query.filter_by(content_type="question").all()  # Fetch all questions
    total_questions = len(questions)

    for i in range(total_questions):
        # Get the question for the current sequence
        question = questions[i]

        # Capture the response (the answer submitted by the user)
        answer = request.form.get("answer")  # Get the answer submitted
        question_id = question.id  # Get the current question ID

        # Store the response in the database using the helper
        capture_and_store_response(user_id, question_id, answer)

    # Once the loop finishes, increment the sequence and continue the flow
    session["current_seq_order"] += 1
    return redirect(url_for('loss_bp.assessment_flow'))  # Proceed to the next step

from sqlalchemy import text

# —— Finalize helper (UPSERT) and redirect appropriately ——
def _finalize_and_redirect(user_id: int):
    p1, p2, p3, p4 = db.session.query(
        func.coalesce(func.sum(LcaQuestionPhaseMap.phase_1), 0),
        func.coalesce(func.sum(LcaQuestionPhaseMap.phase_2), 0),
        func.coalesce(func.sum(LcaQuestionPhaseMap.phase_3), 0),
        func.coalesce(func.sum(LcaQuestionPhaseMap.phase_4), 0),
    ).join(
        LcaResponse,
        and_(
            LcaResponse.question_id == LcaQuestionPhaseMap.question_id,
            LcaResponse.answer == LcaQuestionPhaseMap.answer_type
        )
    ).filter(LcaResponse.user_id == user_id).one()

    total = int(p1) + int(p2) + int(p3) + int(p4)

    db.session.execute(text("""
        INSERT INTO lca_result (user_id, phase_1, phase_2, phase_3, phase_4, total)
        VALUES (:uid, :p1, :p2, :p3, :p4, :tot)
        ON CONFLICT(user_id) DO UPDATE SET
          phase_1 = excluded.phase_1,
          phase_2 = excluded.phase_2,
          phase_3 = excluded.phase_3,
          phase_4 = excluded.phase_4,
          total   = excluded.total,
          created_at = CURRENT_TIMESTAMP
    """), {"uid": user_id, "p1": int(p1), "p2": int(p2), "p3": int(p3), "p4": int(p4), "tot": total})
    db.session.commit()

    session.pop("current_index", None)

    # If questions were launched from sequence, resume at explain stage
    if session.pop("return_to_sequence", None):
        session["seq_stage"] = "explain"
        session["seq_idx"]   = 0
        return redirect(url_for("loss_bp.sequence_flow"))

    # Otherwise go to results page
    return redirect(url_for("loss_bp.assessment_results"))

# helpers (e.g., app/utils/assessment_helpers.py)
from flask import render_template, abort
'''
def get_instruction_card(content_id, error=None):
    c = LcaInstruction.query.get(content_id)
    return render_template("shared/card.html",
                           heading=c.title, body=c.content,
                           show_next_button=True, button_label="Next",
                           error=error)

def get_question_card(content_id, error=None):
    q = LcaQuestion.query.get(content_id)
    if not q:
        return "Question not found", 404
    return render_template("shared/card.html",
                           heading=f"Question {q.number}", body=q.text,
                           show_yes_no_buttons=True, question_id=q.id,
                           error=error)

def get_explain_card(content_id, error=None):
    c = LcaExplain.query.get(content_id)
    return render_template("shared/card.html",
                           heading=c.title, body=c.content,
                           show_next_button=True, button_label="Next",
                           error=error)

def get_pause_card(label, error=None):
    return render_template("shared/card.html",
                           heading="Pause",
                           body=f"Take a short break before continuing.<br><br><strong>{label}</strong>",
                           show_next_button=True, button_label="Continue",
                           error=error)
'''
# helpers/user_display.py (or top of routes.py)
def get_user_display_name(user_id):
    """
    Robustly derive a display name from your auth user record,
    with safe fallbacks to session fields.
    """
    # Primary: ORM
    try:
        from app.models.auth import User  # adjust if your path/class differs
        u = User.query.get(user_id)
    except Exception:
        u = None

    if u:
        for attr in ("full_name",):
            v = getattr(u, attr, None)
            if v: return v.strip()

        first = getattr(u, "first_name", None)
        last  = getattr(u, "last_name",  None)
        if first or last:
            return " ".join(filter(None, [first, last])).strip()

        for attr in ("display_name", "name", "username"):
            v = getattr(u, attr, None)
            if v: return str(v).strip()

        # last resort: email local-part
        email = getattr(u, "email", None)
        if email and "@" in email:
            return email.split("@", 1)[0]

    # Secondary: session fallbacks you may have set at login
    for key in ("user_full_name", "user_name", "username"):
        v = session.get(key)
        if v: return str(v).strip()

    return "User"

def get_instruction_card(content_id, error=None):
    c = LcaInstruction.query.get(content_id)
    return render_template(
        "shared/card.html",
        heading=c.title,
        subheading=None,               # optional
        body=c.content,
        progress=session.get("progress_text"),  # optional if you track it
        show_next_button=True,
        button_label="Next",
        error=error,
    )

def get_question_card(content_id, error=None):
    q = LcaQuestion.query.get(content_id)
    return render_template(
        "shared/card.html",
        heading=f"Question {q.number}" if q.number is not None else "Question",
        subheading="Please choose Yes or No to continue.",
        body=q.text,
        progress=session.get("progress_text"),  # e.g., "12 of 50"
        show_yes_no_buttons=True,
        yes_label="Yes",
        no_label="No",
        question_id=q.id,
        error=error,
    )

def get_explain_card(content_id, error=None):
    x = LcaExplain.query.get(content_id)
    return render_template(
        "shared/card.html",
        heading=x.title,
        subheading=None,
        body=x.content,
        show_next_button=True,
        button_label="Next",
        error=error,
    )

def get_pause_card(label=None, error=None):
    return render_template(
        "shared/card.html",
        heading="Pause",
        subheading=None,
        body=(label or "Take a short break before continuing."),
        show_next_button=True,
        button_label="Continue",
        error=error,
    )
