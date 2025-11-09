from flask import Blueprint, render_template, request, redirect, url_for, session
from sqlalchemy import text
from app.extensions import db
from app.models.reading import RdpLearnerProgress, RdpLesson  # assumes db is created in app/__init__.py

SEQ_SESSION_KEY = 'reading_seq_idx'

reading_bp = Blueprint('reading_bp', __name__, url_prefix='/reading')

# Helper: fetch sequence rows ordered
def _load_sequence():
    sql = text("""
        SELECT id, order_index, kind, title, body, lesson_id, video_filename, button_label, pause_seconds
        FROM rdp_sequence
        ORDER BY order_index ASC
    """)
    rows = db.session.execute(sql).mappings().all()
    return list(rows)

# Helper: fetch lesson by id when needed
def _get_lesson(lesson_id):
    if not lesson_id:
        return None
    sql = text("""
        SELECT id, title, caption, video_filename
        FROM rdp_lesson
        WHERE id = :lid
    """)
    row = db.session.execute(sql, {"lid": lesson_id}).mappings().first()
    return row

@reading_bp.route('/run/reset', methods=['POST'])
def run_reset():
    session.pop(SEQ_SESSION_KEY, None)
    return redirect(url_for('reading_bp.run_sequence'))

@reading_bp.route('/run', methods=['GET', 'POST'])
def run_sequence():
    sequence = _load_sequence()
    if not sequence:
        return render_template('school_reading/sequence_card.html', heading='No sequence defined', body='Please seed rdp_sequence.', button_label='Back to Dashboard', show_next=False)

    # Initialize index
    if SEQ_SESSION_KEY not in session:
        session[SEQ_SESSION_KEY] = 0

    # Navigation
    if request.method == 'POST':
        action = request.form.get('action')
        idx = session.get(SEQ_SESSION_KEY, 0)
        if action == 'next':
            idx = min(idx + 1, len(sequence) - 1)
        elif action == 'prev':
            idx = max(idx - 1, 0)
        session[SEQ_SESSION_KEY] = idx

    idx = session.get(SEQ_SESSION_KEY, 0)
    current = sequence[idx]

    # Derive render payload
    kind = current['kind']
    heading = current.get('title')
    body = current.get('body')
    button_label = current.get('button_label') or ('Finish' if idx == len(sequence) - 1 else 'Next')
    pause_seconds = current.get('pause_seconds') or 0

    media = None
    if kind in ('lesson','exercise'):
        lesson = _get_lesson(current.get('lesson_id'))
        if lesson:
            heading = heading or lesson['title']
            body = body or (lesson.get('caption') or '')  # caption may include HTML
            media = lesson.get('video_filename') or current.get('video_filename')
        else:
            media = current.get('video_filename')

    # Flags for template
    show_prev = idx > 0
    show_next = True
    show_done = (idx == len(sequence) - 1)

    return render_template(
        'school_reading/sequence_card.html',
        kind=kind,
        heading=heading,
        body=body,
        media=media,
        pause_seconds=pause_seconds,
        show_prev=show_prev,
        show_next=show_next,
        show_done=show_done,
        button_label=button_label
    )

# Helper: fetch sequence rows ordered
def _load_sequence():
    sql = text("""
        SELECT id, order_index, kind, title, body, lesson_id, video_filename, button_label, pause_seconds
        FROM rdp_sequence
        ORDER BY order_index ASC
    """)
    rows = db.session.execute(sql).mappings().all()
    return list(rows)

# Helper: fetch lesson by id when needed
def _get_lesson(lesson_id):
    if not lesson_id:
        return None
    sql = text("""
        SELECT id, title, caption, video_filename
        FROM rdp_lesson
        WHERE id = :lid
    """)
    row = db.session.execute(sql, {"lid": lesson_id}).mappings().first()
    return row

@reading_bp.route('/run/reset', methods=['POST'])
def run_reset():
    session.pop(SEQ_SESSION_KEY, None)
    return redirect(url_for('reading_bp.run_sequence'))

@reading_bp.route('/run', methods=['GET', 'POST'])
def run_sequence():
    sequence = _load_sequence()
    if not sequence:
        return render_template('reading/sequence_card.html', heading='No sequence defined', body='Please seed rdp_sequence.', button_label='Back to Dashboard', show_next=False)

    # Initialize index
    if SEQ_SESSION_KEY not in session:
        session[SEQ_SESSION_KEY] = 0

    # Navigation
    if request.method == 'POST':
        action = request.form.get('action')
        idx = session.get(SEQ_SESSION_KEY, 0)
        if action == 'next':
            idx = min(idx + 1, len(sequence) - 1)
        elif action == 'prev':
            idx = max(idx - 1, 0)
        session[SEQ_SESSION_KEY] = idx

    idx = session.get(SEQ_SESSION_KEY, 0)
    current = sequence[idx]

    # Derive render payload
    kind = current['kind']
    heading = current.get('title')
    body = current.get('body')
    button_label = current.get('button_label') or ('Finish' if idx == len(sequence) - 1 else 'Next')
    pause_seconds = current.get('pause_seconds') or 0

    media = None
    if kind in ('lesson','exercise'):
        lesson = _get_lesson(current.get('lesson_id'))
        if lesson:
            heading = heading or lesson['title']
            # Caption can contain HTML; the template uses |safe
            body = body or (lesson.get('caption') or '')
            media = lesson.get('video_filename') or current.get('video_filename')
        else:
            media = current.get('video_filename')

    # Flags for template
    show_prev = idx > 0
    show_next = True
    show_done = (idx == len(sequence) - 1)

    return render_template(
        'reading/sequence_card.html',
        kind=kind,
        heading=heading,
        body=body,
        media=media,
        pause_seconds=pause_seconds,
        show_prev=show_prev,
        show_next=show_next,
        show_done=show_done,
        button_label=button_label
    )

def _email():
    return session.get("email")

def _is_tutor():
    role = (session.get("role") or "").lower()
    return "tutor" in role  # e.g. "reading_tutor"

def _build_items(email: str, unlock_all: bool = False):
    lessons = db.session.query(RdpLesson).order_by(RdpLesson.order.asc()).all()

    prog_rows = (
        db.session.query(RdpLearnerProgress.lesson_id, RdpLearnerProgress.is_complete)
        .filter(RdpLearnerProgress.email == email)
        .all()
    )
    progress = {lid: bool(done) for (lid, done) in prog_rows}

    items, prev_complete = [], True
    for ls in lessons:
        done = progress.get(ls.id, False)
        can_start = True if unlock_all else (prev_complete or done)
        items.append({
            "id": ls.id,
            "title": ls.title,
            "caption": ls.caption,
            "is_complete": done,
            "can_start": can_start,
        })
        prev_complete = done
    return items

# app/utils/reading_utils.py

# --- helpers ---------------------------------------------------------------

def _apply_email_scope(q, email: str):
    """
    Apply a per-learner scope to the query based on whichever column your
    RdpLearnerProgress uses. Falls back to user_id lookups and finally to no scope.
    """
    M = RdpLearnerProgress

    # Direct email-like columns
    for col in ("email", "learner_email", "user_email", "student_email", "account_email"):
        if hasattr(M, col):
            return q.filter(getattr(M, col) == email)

    # ID-based columns (resolve from email)
    for id_col in ("user_id", "learner_id", "student_id"):
        if hasattr(M, id_col):
            uid = _get_user_id_by_email(email)
            if uid is not None:
                return q.filter(getattr(M, id_col) == uid)
            # If we can’t resolve the user, return an empty result set cleanly
            return q.filter(db.text("1=0"))

    # Last resort: no scoping (not ideal, but won’t crash)
    return q

# --- public utils used by routes ------------------------------------------

def _get_user_id_by_email(email: str):
    try:
        from app.models.auth import User
    except Exception:
        try:
            from app.models.auth import User
        except Exception:
            return None
    u = db.session.query(User).filter(User.email == email).first()
    return getattr(u, "id", None) if u else None

def _completed_ids(email: str) -> set[int]:
    """Return lesson_ids completed by this learner, regardless of schema."""
    M = RdpLearnerProgress
    base = db.session.query(M.lesson_id)

    # Scope by learner (prefer email-like cols; else resolve user_id)
    if hasattr(M, "email"):
        q = base.filter(M.email == email)
    elif hasattr(M, "learner_email"):
        q = base.filter(getattr(M, "learner_email") == email)
    elif hasattr(M, "user_email"):
        q = base.filter(getattr(M, "user_email") == email)
    elif hasattr(M, "student_email"):
        q = base.filter(getattr(M, "student_email") == email)
    elif hasattr(M, "account_email"):
        q = base.filter(getattr(M, "account_email") == email)
    elif hasattr(M, "user_id"):
        uid = _get_user_id_by_email(email)
        if uid is None:
            return set()
        q = base.filter(M.user_id == uid)
    elif hasattr(M, "learner_id"):
        uid = _get_user_id_by_email(email)
        if uid is None:
            return set()
        q = base.filter(getattr(M, "learner_id") == uid)
    elif hasattr(M, "student_id"):
        uid = _get_user_id_by_email(email)
        if uid is None:
            return set()
        q = base.filter(getattr(M, "student_id") == uid)
    else:
        # last resort: no scoping (not ideal, but won’t crash)
        q = base

    # Apply completion condition based on whichever field exists
    if hasattr(M, "is_complete"):
        q = q.filter(M.is_complete.is_(True))
    elif hasattr(M, "completed"):
        q = q.filter(getattr(M, "completed").is_(True))
    elif hasattr(M, "is_completed"):
        q = q.filter(getattr(M, "is_completed").is_(True))
    elif hasattr(M, "status"):
        q = q.filter(getattr(M, "status").in_(["complete", "completed", "done", 1, True]))
    elif hasattr(M, "completed_at"):
        q = q.filter(getattr(M, "completed_at").isnot(None))
    # else: treat presence of a row as completion

    return {lid for (lid,) in q.all()}

def _progress_map(email: str) -> dict[int, bool]:
    """Return {lesson_id: True} for completed lessons (existing callers expect bool)."""
    return {lid: True for lid in _completed_ids(email)}

# Canonical keys -> your actual column/attr names (edit these once to match your DB)
LESSON_FIELD_MAP = {
    "html": ["elesson_html", "html", "content_html"],
    "url":  ["elesson_url", "elink", "url", "content_url"],
    "slug": ["slug", "folder", "package"],
}

def _get_first_attr(obj, names):
    for n in names:
        val = getattr(obj, n, None)
        if val:  # not None/empty
            return val
    return None

def lesson_payload(lesson):
    """
    Normalize lesson -> {"html": str|None, "url": str|None, "slug": str|None}
    Works whether content is on RdpLesson or on a related 'elessons' row.
    """
    # If you actually have a relation like lesson.elessons (list), use that first item:
    if hasattr(lesson, "elessons") and lesson.elessons:
        source = lesson.elessons[0]
    else:
        source = lesson

    return {
        "html": _get_first_attr(source, LESSON_FIELD_MAP["html"]),
        "url":  _get_first_attr(source, LESSON_FIELD_MAP["url"]),
        "slug": _get_first_attr(source, LESSON_FIELD_MAP["slug"]),
    }

# app/utils/reading_utils.py


def _lessons():
    return db.session.query(RdpLesson).order_by(RdpLesson.order.asc(), RdpLesson.id.asc()).all()

# --- add near the other helpers ---
import os
from flask import current_app, url_for

def static_media_url(filename: str) -> str:
    """
    Return a url_for() to a file that may live under /static/videos, /static/images, or /static/pdfs.
    Falls back to /static/videos if not found.
    """
    if not filename:
        return ""
    subfolders = ("videos", "images", "pdfs")
    root = current_app.static_folder
    for sub in subfolders:
        full = os.path.join(root, sub, filename)
        if os.path.exists(full):
            return url_for("static", filename=f"{sub}/{filename}")
    # default/fallback
    return url_for("static", filename=f"videos/{filename}")

def dashboard_context(email: str):
    """Builds the dashboard view-model once for both learner/admin."""
    lessons = _lessons()                 # you already have this
    prog_map = _progress_map(email)      # you already have this

    items = []
    for ls in lessons:
        items.append({
            "id": ls.id,
            "title": (ls.title or f"Lesson {ls.order or ''}").strip(),
            "caption": (ls.caption or "").strip(),
            "complete": bool(prog_map.get(ls.id)),
        })

    learner_name = (email or "").split("@")[0] or "learner"
    return {"items": items, "learner_name": learner_name}
