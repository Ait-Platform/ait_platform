# routes
from __future__ import annotations
import base64
import os
from flask import (
    Blueprint, current_app, g, render_template,redirect, 
    request, send_file, url_for, flash, session, abort)
from flask_login import login_required, current_user
from app.models.reading import RdpLearnerProgress, RdpLesson
from app.utils import reading_utils
from datetime import datetime, timedelta
from app.utils.reading_utils import lesson_payload  # canonicalize lesson content
from app.extensions import db
from sqlalchemy import text as sa_text
import smtplib
from email.message import EmailMessage

reading_bp = Blueprint("reading_bp", __name__, url_prefix="/reading")


READING_SUBJECT_ID = 1  # <-- set this to the actual id for 'Reading' in subject table

@reading_bp.before_request
def _set_ui_lang():
    g.ui_lang = (session.get("ui_lang") or "en").strip().lower()

@reading_bp.route("/about", methods=["GET"], endpoint="about_reading")
def about_reading():
    return render_template("subject_reading/about.html")  # ← change this line

@reading_bp.post("/enrol", endpoint="enrol_reading")
def enrol_reading():
    role = (request.form.get("role") or "learner").strip().lower()  # 'learner' or 'tutor'
    return redirect(url_for(
        "auth_bp.start_registration",
        subject="reading",
        role=role,
        next=url_for("reading_bp.about_reading"),  # or reading dashboard after payment
    ))

@reading_bp.get("/dashboard", endpoint="learner_dashboard")
@login_required
def learner_dashboard():
    # 1) learner display name
    email = getattr(current_user, "email", None) or "learner@example.com"
    learner_name = (
        getattr(current_user, "display_name", None)
        or getattr(current_user, "first_name", None)
        or email.split("@", 1)[0]
        or "Learner"
    )

    uid = int(current_user.id)

    # 2) lessons list (ordered) - includes lesson key ("order")
    lessons = db.session.execute(
        sa_text("""
            SELECT id, title, caption, "order"
            FROM rdp_lesson
            ORDER BY "order" ASC
        """)
    ).mappings().all()

    # Safe defaults when no lessons exist
    if not lessons:
        ui_lang = session.get("ui_lang", "en")
        return render_template(
            "subject_reading/learner_dashboard.html",
            items=[],
            learner_name=learner_name,
            ui_lang=ui_lang,
            t=_t,
            show_cta=False,
            open_key=1,
            total=0,
            completed=0,
            progress_percent=0,
        )

    total = len(lessons)

    # 3) per-lesson progress map (status + tries)
    prog_rows = db.session.execute(
        sa_text("""
            SELECT lesson_key, status, tries_used
            FROM rdp_lesson_progress
            WHERE user_id = :uid
        """),
        {"uid": uid},
    ).mappings().all()

    prog_map = {int(r["lesson_key"]): dict(r) for r in prog_rows}

    # 4) compute completed + next open lesson (first not completed by lesson_key)
    completed = 0
    open_key = None

    ordered_keys = [int(r["order"]) for r in lessons]

    for k in ordered_keys:
        st = (prog_map.get(k, {}).get("status") or "not_started")
        if st == "completed":
            completed += 1
        elif open_key is None:
            open_key = k

    if open_key is None:
        open_key = ordered_keys[-1]  # everything completed

    progress_percent = int((completed * 100) / total) if total else 0

    # 5) annotate lessons for template: open / locked / completed + tries
    items = [dict(r) for r in lessons]

    for row in items:
        k = int(row["order"])
        p = prog_map.get(k) or {}
        status = (p.get("status") or "not_started")
        tries_used = int(p.get("tries_used") or 0)

        is_completed = (status == "completed")
        is_open = (k == open_key) and (not is_completed) and (tries_used < 3)
        is_locked = (not is_open) and (not is_completed)

        row["lesson_key"] = k
        row["status"] = status
        row["tries_used"] = tries_used
        row["is_completed"] = is_completed
        row["is_open"] = is_open
        row["is_locked"] = is_locked

    # 6) ui lang from session
    ui_lang = session.get("ui_lang", "en")

    # 7) CTA shows only if there is an open lesson available
    show_cta = any(r.get("is_open") for r in items)

    return render_template(
        "subject_reading/learner_dashboard.html",
        items=items,
        learner_name=learner_name,
        ui_lang=ui_lang,
        t=_t,
        show_cta=show_cta,
        open_key=int(open_key),
        total=int(total),
        completed=int(completed),
        progress_percent=int(progress_percent),
    )

@reading_bp.get("/", endpoint="subject_home")
@login_required
def subject_home():
    enr = _get_enrollment(current_user.id)
    if not enr:
        return redirect(url_for("reading_bp.learner_dashboard"))

    uid = int(current_user.id)

    total = int(db.session.execute(sa_text("SELECT COUNT(*) FROM rdp_lesson")).scalar() or 0)

    completed = int(db.session.execute(
        sa_text("""
            SELECT COUNT(*)
              FROM rdp_lesson_progress
             WHERE user_id = :uid
               AND status = 'completed'
        """),
        {"uid": uid},
    ).scalar() or 0)

    progress_percent = int(round((completed / total) * 100)) if total else 0

    enrollment_status = (enr.get("status") if isinstance(enr, dict) else getattr(enr, "status", None)) or "active"

    return render_template(
        "subject_reading/hub.html",
        progress_percent=progress_percent,
        enrollment_status=enrollment_status,
        completed=completed,
        total=total,
    )


# ─────────────────────────────────
# 2. language picker save
# ─────────────────────────────────
@reading_bp.post("/set-lang", endpoint="set_lang")
@login_required
def set_lang():
    lang = (request.form.get("lang") or "en").strip().lower()
    session["ui_lang"] = lang

    db.session.execute(
        sa_text("""
            UPDATE rdp_enrollment
               SET ui_lang = :lang
             WHERE user_id = :uid
        """),
        {"lang": lang, "uid": int(current_user.id)},
    )
    db.session.commit()

    return redirect(request.referrer or url_for("reading_bp.dashboard"))

# ─────────────────────────────────
# 3. dashboard
# ─────────────────────────────────

@reading_bp.get("/dashboard", endpoint="dashboard")
@login_required
def dashboard():
    enr = _ensure_enrollment_row()

    # learner name for greeting
    if getattr(current_user, "display_name", None):
        learner_name = current_user.display_name
    elif getattr(current_user, "first_name", None):
        learner_name = current_user.first_name
    else:
        learner_name = getattr(current_user, "username", "Learner")

    # fetch lessons list (still used elsewhere)
    lessons = db.session.execute(
        sa_text("""
            SELECT id, title, caption, "order"
            FROM rdp_lesson
            ORDER BY "order" ASC
            
        """)
    ).mappings().all()

    # CTA label
    if enr.completed_at:
        cta_label = "Review Reading Again"
    elif enr.started_at:
        cta_label = "Continue Reading"
    else:
        cta_label = "Start Reading"

    return render_template(
        "subject_reading/learner_dashboard.html",
        ui_lang=g.ui_lang,
        learner_name=learner_name,
        items=lessons,
        cta_label=cta_label,
        progress_percent=enr.progress_percent,
        certificate_id=enr.certificate_id,
        completed_at=enr.completed_at,   # <-- add this
        t=_t,
    )

# ─────────────────────────────────
# 4. begin (Start / Continue button)
# ─────────────────────────────────

@reading_bp.post("/begin", endpoint="begin_course")
@login_required
def begin_course():
    # make sure they have an rdp_enrollment row and stamp start window
    _ensure_enrollment_row()
    _ensure_started_window()

    # jump them straight to first lesson
    first_id = db.session.execute(
        sa_text("""
            SELECT id FROM rdp_lesson
            ORDER BY "order" ASC
            LIMIT 1
        """)
    ).scalar()

    if first_id is None:
        # no lessons? just go back to dashboard
        return redirect(url_for("reading_bp.dashboard"))

    return redirect(url_for("reading_bp.view_lesson", lesson_id=first_id))

# ─────────────────────────────────
# 5. lesson player
# ─────────────────────────────────

@reading_bp.get("/lesson/<int:lesson_id>", endpoint="view_lesson")
@login_required
def view_lesson(lesson_id: int):
    uid = int(current_user.id)

    # fetch the lesson (includes its sequence key = "order")
    lesson = db.session.execute(
        sa_text("""
            SELECT id, title, caption, video_filename, "order"
            FROM rdp_lesson
            WHERE id = :i
            LIMIT 1
        """),
        {"i": int(lesson_id)},
    ).mappings().first()
    if not lesson:
        abort(404)

    lesson_key = int(lesson["order"])

    # ordered lessons list (needed for locking logic)
    items = db.session.execute(
        sa_text("""
            SELECT id, "order"
            FROM rdp_lesson
            ORDER BY "order" ASC
        """)
    ).mappings().all()

    # progress map: lesson_key -> row dict
    rows = db.session.execute(
        sa_text("""
            SELECT lesson_key, status, tries_used
            FROM rdp_lesson_progress
            WHERE user_id = :uid
        """),
        {"uid": uid},
    ).mappings().all()
    prog = {int(r["lesson_key"]): r for r in rows}

    # determine the only open lesson: first not completed
    open_key = None
    for r in items:
        k = int(r["order"])
        st = (prog.get(k, {}).get("status") or "not_started")
        if st != "completed":
            open_key = k
            break

    if open_key is None:
        open_key = int(items[-1]["order"]) if items else 1

    # enforce: only open_key can be viewed
    cur = prog.get(lesson_key) or {}
    status = (cur.get("status") or "not_started")

    if lesson_key != open_key and status != "completed":
        return redirect(url_for("reading_bp.learner_dashboard"))


    # enforce: max 3 tries on the open lesson
    cur = prog.get(lesson_key) or {}
    tries_used = int(cur.get("tries_used") or 0)
    status = (cur.get("status") or "not_started")

    if status != "completed" and tries_used >= 3:
        return redirect(url_for("reading_bp.learner_dashboard"))

    # consume a try on open (only if not completed)
    if status != "completed":
        db.session.execute(
            sa_text("""
                INSERT INTO rdp_lesson_progress (user_id, lesson_key, status, tries_used, started_at)
                VALUES (:uid, :k, 'in_progress', 1, CURRENT_TIMESTAMP)
                ON CONFLICT (user_id, lesson_key) DO UPDATE SET
                    status = CASE
                        WHEN rdp_lesson_progress.status = 'completed' THEN 'completed'
                        ELSE 'in_progress'
                    END,
                    tries_used = CASE
                        WHEN rdp_lesson_progress.status = 'completed' THEN rdp_lesson_progress.tries_used
                        ELSE COALESCE(rdp_lesson_progress.tries_used, 0) + 1
                    END,
                    started_at = CASE
                        WHEN rdp_lesson_progress.status = 'completed' THEN rdp_lesson_progress.started_at
                        ELSE CURRENT_TIMESTAMP
                    END
            """),
            {"uid": uid, "k": int(lesson_key)},
        )
        db.session.commit()

    # prev/next by order
    prev_id = db.session.execute(
        sa_text("""
            SELECT id
            FROM rdp_lesson
            WHERE "order" < :ord
            ORDER BY "order" DESC
            LIMIT 1
        """),
        {"ord": lesson_key},
    ).scalar()

    next_id = db.session.execute(
        sa_text("""
            SELECT id
            FROM rdp_lesson
            WHERE "order" > :ord
            ORDER BY "order" ASC
            LIMIT 1
        """),
        {"ord": lesson_key},
    ).scalar()

    video_src = url_for("static", filename=f"reading_videos/{lesson['video_filename']}")
    ui_lang = session.get("ui_lang", "en")

    return render_template(
        "subject_reading/lesson.html",
        lesson=lesson,
        lesson_key=lesson_key,
        video_src=video_src,
        prev_id=prev_id,
        next_id=next_id,
        last_one=(next_id is None),
        ui_lang=ui_lang,
        t=_t,
    )

# ─────────────────────────────────
# 6. finish (after last lesson / Finish button)
# ─────────────────────────────────

@reading_bp.post("/finish", endpoint="finish_course")
@login_required
def finish_course():
    uid = int(current_user.id)

    total = int(db.session.execute(sa_text("SELECT COUNT(*) FROM rdp_lesson")).scalar() or 0)
    completed = int(db.session.execute(
        sa_text("""
            SELECT COUNT(*)
              FROM rdp_lesson_progress
             WHERE user_id = :uid
               AND status = 'completed'
        """),
        {"uid": uid},
    ).scalar() or 0)

    if total and completed < total:
        flash("Finish all lessons first to unlock your certificate.", "warning")
        return redirect(url_for("reading_bp.subject_home"))

    enr = _get_enrollment()
    if not enr:
        return redirect(url_for("reading_bp.dashboard"))

    cert_id = enr.certificate_id or _make_certificate_id(current_user.id)

    # mark THIS subject completed only
    db.session.execute(
        sa_text("""
            UPDATE user_enrollment
            SET status = 'completed'
            WHERE user_id = :uid
              AND subject_id = :sid
        """),
        {
            "uid": g.user_id,
            "sid": READING_SUBJECT_ID,
        },
    )
    db.session.commit()

    # learner name
    learner_name = (
        getattr(current_user, "name", None)
        or getattr(current_user, "display_name", None)
        or getattr(current_user, "first_name", None)
        or getattr(current_user, "username", None)
        or "Learner"
    )

    learner_email = getattr(current_user, "email", None)

    # make / refresh PDF
    fresh = _get_enrollment()
    completed_at = fresh.completed_at if fresh else datetime.utcnow()
    pdf_path = _generate_certificate_pdf(
        certificate_id=cert_id,
        learner_name=learner_name,
        completed_at=completed_at,
    )

    # email best-effort
    if learner_email:
        _email_certificate_pdf(
            to_email=learner_email,
            learner_name=learner_name,
            certificate_id=cert_id,
            pdf_path=pdf_path,
        )

    flash("You're done. Your certificate has been issued.", "success")

    # do NOT guess next step. Let exit_page offer choices.
    return redirect(url_for("reading_bp.exit_page"))

@reading_bp.get("/exit", endpoint="exit_page")
@login_required
def exit_page():
    return render_template("subject_reading/exit.html")

@reading_bp.get("/certificate", endpoint="get_certificate")
@login_required
def get_certificate():
    return _finalize_and_send_certificate(current_user.id)

@reading_bp.post("/lesson/complete", endpoint="complete_lesson")
@login_required
def complete_lesson():
    uid = int(current_user.id)
    lesson_key = int(request.form.get("lesson_key") or 0)
    if lesson_key <= 0:
        return redirect(url_for("reading_bp.learner_dashboard"))

    # Mark completed (do not touch tries_used)
    db.session.execute(
        sa_text("""
            INSERT INTO rdp_lesson_progress (user_id, lesson_key, status, tries_used, completed_at)
            VALUES (:uid, :k, 'completed', 0, CURRENT_TIMESTAMP)
            ON CONFLICT (user_id, lesson_key) DO UPDATE SET
                status = 'completed',
                completed_at = COALESCE(rdp_lesson_progress.completed_at, CURRENT_TIMESTAMP)
        """),
        {"uid": uid, "k": lesson_key},
    )
    db.session.commit()

    # Go to next open lesson (or dashboard if none)
    next_id = db.session.execute(
        sa_text("""
            WITH ordered AS (
              SELECT id, "order" AS k
              FROM rdp_lesson
              ORDER BY "order" ASC
            ),
            first_open AS (
              SELECT o.id
              FROM ordered o
              LEFT JOIN rdp_lesson_progress p
                ON p.user_id = :uid AND p.lesson_key = o.k
              WHERE COALESCE(p.status, 'not_started') != 'completed'
              ORDER BY o.k ASC
              LIMIT 1
            )
            SELECT id FROM first_open
        """),
        {"uid": uid},
    ).scalar()

    if next_id:
        return redirect(url_for("reading_bp.view_lesson", lesson_id=int(next_id)))
    return redirect(url_for("reading_bp.learner_dashboard"))

@reading_bp.post("/lesson/defer", endpoint="defer_lesson")
@login_required
def defer_lesson():
    uid = int(current_user.id)
    lesson_key = int(request.form.get("lesson_key") or 0)
    if lesson_key <= 0:
        return redirect(url_for("reading_bp.learner_dashboard"))

    # Mark deferred (intro-only choice enforced in template; server stays safe anyway)
    db.session.execute(
        sa_text("""
            INSERT INTO rdp_lesson_progress (user_id, lesson_key, status, tries_used, started_at)
            VALUES (:uid, :k, 'deferred', 0, CURRENT_TIMESTAMP)
            ON CONFLICT (user_id, lesson_key) DO UPDATE SET
                status = CASE
                  WHEN rdp_lesson_progress.status = 'completed' THEN 'completed'
                  ELSE 'deferred'
                END
        """),
        {"uid": uid, "k": lesson_key},
    )
    db.session.commit()
    return redirect(url_for("reading_bp.learner_dashboard"))


# ─────────────────────────────────
# 1. helpers
# ─────────────────────────────────

def _get_enrollment():
    row = db.session.execute(
        sa_text("""
            SELECT
                id,
                user_id,
                started_at,
                expires_at,
                progress_percent,
                completed_at,
                certificate_id
            FROM rdp_enrollment
            WHERE user_id = :uid
            LIMIT 1
        """),
        {"uid": g.user_id},
    ).fetchone()
    return row

def _redirect_after_reading():
    if _has_other_active_subjects(g.user_id):
        # learner has more to do → send to bridge hub
        return redirect(url_for("bridge_bp.bridge_home"))
    else:
        # reading was their only/last subject → send to welcome/landing
        return redirect(url_for("public_bp.welcome"))


def _has_other_active_subjects(user_id):
    # TODO later:
    #   check LOSS / billing / math / whatever tables
    #   return True if enrolled & not completed in any of those
    return False

def _ensure_enrollment_row():
    """
    Make sure rdp_enrollment row exists for this user.
    Return that row as a mapping.
    """
    uid = int(current_user.id)

    row = db.session.execute(
        sa_text("""
            SELECT
                user_id,
                started_at,
                expires_at,
                completed_at,
                progress_percent,
                certificate_id
            FROM rdp_enrollment
            WHERE user_id = :uid
            LIMIT 1
        """),
        {"uid": uid},
    ).mappings().first()

    if row:
        return row

    # create brand new row
    db.session.execute(
        sa_text("""
            INSERT INTO rdp_enrollment (
                user_id,
                started_at,
                expires_at,
                completed_at,
                progress_percent,
                certificate_id
            ) VALUES (
                :uid,
                NULL,
                NULL,
                NULL,
                0,
                NULL
            )
        """),
        {"uid": uid},
    )
    db.session.commit()

    # fetch again
    row = db.session.execute(
        sa_text("""
            SELECT
                user_id,
                started_at,
                expires_at,
                completed_at,
                progress_percent,
                certificate_id
            FROM rdp_enrollment
            WHERE user_id = :uid
            LIMIT 1
        """),
        {"uid": uid},
    ).mappings().first()

    return row

def _ensure_started_window():
    uid = int(current_user.id)
    now = datetime.utcnow()
    expires = now + timedelta(days=365)

    db.session.execute(sa_text("""
        UPDATE rdp_enrollment
           SET started_at = COALESCE(started_at, :now),
               expires_at = COALESCE(expires_at, :exp)
         WHERE user_id = :uid
    """), {"now": now, "exp": expires, "uid": uid})
    db.session.commit()

def _update_progress_after_lesson(lesson_order_number: int, total_lessons: int = 18):
    """
    Cheap %:
      - learner starts at 10%
      - then increases up to 100% across lessons watched
    We'll just map lesson_order_number -> percent, cap at 100.
    """
    uid = int(current_user.id)
    
    raw_pct = int((lesson_order_number / total_lessons) * 100)
    if raw_pct < 10:
        raw_pct = 10
    if raw_pct > 100:
        raw_pct = 100

    db.session.execute(
        sa_text("""
            UPDATE rdp_enrollment
            SET progress_percent = CASE
                WHEN :pct > COALESCE(progress_percent,0) THEN :pct
                ELSE progress_percent
            END
            WHERE user_id = :uid
        """),
        {"pct": raw_pct, "uid": int(current_user.id)},
    )
    db.session.commit()

def _make_certificate_id(user_id):
    today_str = datetime.utcnow().strftime("%Y%m%d")
    return f"READ-{user_id}-{today_str}"

def _email_certificate_pdf(to_email, learner_name, certificate_id, pdf_path):
    """
    Best-effort email.
    Does NOT block the redirect if it fails.
    """
    subject = "Your Reading Programme Certificate"
    body = (
        f"Hi {learner_name},\n\n"
        "Congratulations on completing the AIT Reading Programme.\n"
        f"Your certificate ID is {certificate_id}.\n\n"
        "If an attachment is not included, you can still request a copy from your facilitator.\n\n"
        "AIT Platform"
    )

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = "no-reply@your-domain.example"
    msg["To"] = to_email
    msg.set_content(body)

    if pdf_path:
        try:
            with open(pdf_path, "rb") as f:
                data = f.read()
            msg.add_attachment(
                data,
                maintype="application",
                subtype="pdf",
                filename=f"{certificate_id}.pdf"
            )
        except Exception as e:
            current_app.logger.error(f"Attach failed for {certificate_id}: {e}")

    try:
        with smtplib.SMTP_SSL("smtp.zoho.com", 465) as smtp:
            smtp.login("no-reply@your-domain.example", "YOUR_SMTP_PASSWORD")
            smtp.send_message(msg)
    except Exception as e:
        current_app.logger.error(f"Email send failed for {certificate_id}: {e}")

def _t(label_key: str, lang: str):
    """
    Tiny label dictionary for UI strings.
    """
    MESSAGES = {
        "replay": {
            "en": "Replay",
            "zu": "Phinda",
            "hi": "दोबारा",
            "af": "Speel weer",
            "xh": "Phinda",
        },
        "next": {
            "en": "Next →",
            "zu": "Okulandelayo →",
            "hi": "आगे →",
            "af": "Volgende →",
            "xh": "Okulandelayo →",
        },
        "previous": {
            "en": "← Previous",
            "zu": "← Emuva",
            "hi": "← पिछला",
            "af": "← Vorige",
            "xh": "← Emva",
        },
        "finish_course": {
            "en": "Finish Course",
            "zu": "Qeda Isifundo",
            "hi": "कोर्स पूरा करें",
            "af": "Voltooi Kursus",
            "xh": "Gqiba Isifundo",
        },
        "back_to_dashboard": {
            "en": "← Back to dashboard",
            "zu": "← Buyela ku-dashboard",
            "hi": "← डैशबोर्ड पर वापस",
            "af": "← Terug na paneelbord",
            "xh": "← Buyela kwideshibhodi",
        },
        "your_lessons": {
            "en": "Your Lessons",
            "zu": "Izifundo Zakho",
            "hi": "आपके पाठ",
            "af": "Jou Lesse",
            "xh": "Izifundo Zakho",
        },
        "title_col": {
            "en": "Title",
            "zu": "Isihloko",
            "hi": "शीर्षक",
            "af": "Titel",
            "xh": "Isihloko",
        },
        "caption_col": {
            "en": "Caption",
            "zu": "Incazelo",
            "hi": "विवरण",
            "af": "Beskrywing",
            "xh": "Inkcazo",
        },
    }

    return (
        MESSAGES.get(label_key, {}).get(lang)
        or MESSAGES.get(label_key, {}).get("en")
        or label_key
    )

def _get_enrollment(user_id: int | None = None):
    uid = int(user_id or getattr(current_user, "id", 0) or 0)
    if not uid:
        return None

    return db.session.execute(
        sa_text("""
            SELECT *
            FROM rdp_enrollment
            WHERE user_id = :uid
            LIMIT 1
        """),
        {"uid": uid},
    ).mappings().first()

def _finalize_and_send_certificate(user_id: int):
    enr = _get_enrollment()
    if not enr:
        abort(400)

    # make / reuse cert id
    cert_id = enr.certificate_id or _make_certificate_id(user_id)

    # make sure enrollment is marked completed and has cert_id + completed_at
    db.session.execute(
        sa_text("""
            UPDATE rdp_enrollment
            SET
                completed_at = COALESCE(completed_at, CURRENT_TIMESTAMP),
                progress_percent = 100,
                certificate_id = :cid
            WHERE user_id = :uid
        """),
        {"cid": cert_id, "uid": user_id},
    )
    db.session.execute(
        sa_text("""
            UPDATE user_enrollment
            SET status = 'completed'
            WHERE user_id = :uid
        """),
        {"uid": user_id},
    )
    db.session.commit()

    # pull learner display name for cert/email
    learner_name = (
        getattr(current_user, "name", None)
        or getattr(current_user, "display_name", None)
        or getattr(current_user, "first_name", None)
        or getattr(current_user, "username", None)
        or "Learner"
    )

    learner_email = getattr(current_user, "email", None)


    # generate PDF
    fresh = _get_enrollment()
    completed_at = fresh.completed_at if fresh else datetime.utcnow()
    pdf_path = _generate_certificate_pdf(
        certificate_id=cert_id,
        learner_name=learner_name,
        completed_at=completed_at,
    )

    # try email
    if learner_email:
        _email_certificate_pdf(
            to_email=learner_email,
            learner_name=learner_name,
            certificate_id=cert_id,
            pdf_path=pdf_path,
        )

    # stream file back
    if pdf_path and os.path.exists(pdf_path):
        return send_file(
            pdf_path,
            mimetype="application/pdf",
            as_attachment=True,
            download_name=f"{cert_id}.pdf",
        )

    # fallback: no file
    flash("Certificate emailed (PDF download unavailable on this device).", "success")
    return redirect(url_for("reading_bp.exit_page"))

def _generate_certificate_pdf(certificate_id, learner_name, completed_at):
    # 1. normalize completed_at into datetime
    if isinstance(completed_at, str):
        try:
            completed_at = datetime.fromisoformat(completed_at)
        except Exception:
            completed_at = datetime.utcnow()
    elif completed_at is None:
        completed_at = datetime.utcnow()

    completed_date = completed_at.strftime("%d %B %Y")

    # 2. read logo file and base64 it
    logo_file_path = os.path.join(
        current_app.root_path,
        "static",
        "branding",
        "ait_logo.png",   # make sure this file exists
    )

    logo_data_uri = None
    try:
        with open(logo_file_path, "rb") as f:
            b = f.read()
        b64 = base64.b64encode(b).decode("ascii")
        # tell browser/WeasyPrint: this is an inline PNG
        logo_data_uri = f"data:image/png;base64,{b64}"
    except Exception as e:
        current_app.logger.error(f"Logo load failed: {e}")
        logo_data_uri = None

    # 3. render html
    html_str = render_template(
        "subject_reading/certificate.html",
        learner_name=learner_name,
        completed_date=completed_date,
        certificate_id=certificate_id,
        logo_path=logo_data_uri,   # <- now this is inline img data, no file://
    )

    # 4. write pdf
    cert_dir = os.path.join(current_app.root_path, "static", "certificates")
    os.makedirs(cert_dir, exist_ok=True)

    pdf_path = os.path.join(cert_dir, f"{certificate_id}.pdf")

    try:
        from weasyprint import HTML
        HTML(string=html_str).write_pdf(pdf_path)
        return pdf_path
    except Exception as e:
        current_app.logger.error(f"PDF generation failed for {certificate_id}: {e}")
        return None

def _get_lesson_progress_map(user_id: int) -> dict[int, dict]:
    rows = db.session.execute(
        sa_text("""
            SELECT lesson_key, status, tries_used
            FROM rdp_lesson_progress
            WHERE user_id = :uid
        """),
        {"uid": int(user_id)},
    ).mappings().all()

    return {int(r["lesson_key"]): dict(r) for r in rows}

def _next_open_lesson_key(*, items_count: int, prog_map: dict[int, dict]) -> int:
    for k in range(1, items_count + 1):
        st = (prog_map.get(k, {}).get("status") or "not_started")
        if st != "completed":
            return k
    return items_count  # fallback (shouldn’t happen)

def _recalc_and_store_progress(*, user_id: int, items_count: int) -> int:
    done = db.session.execute(
        sa_text("""
            SELECT COUNT(*) AS n
            FROM rdp_lesson_progress
            WHERE user_id = :uid AND status = 'completed'
        """),
        {"uid": int(user_id)},
    ).mappings().first()["n"] or 0

    pct = int(round((int(done) / max(int(items_count), 1)) * 100))

    db.session.execute(
        sa_text("""
            UPDATE rdp_enrollment
            SET progress_percent = :pct
            WHERE user_id = :uid
        """),
        {"pct": int(pct), "uid": int(user_id)},
    )
    return pct

