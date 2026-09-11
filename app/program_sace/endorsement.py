"""Assignment-scoped endorsement state in the existing SACE evidence store.

The provisioned invitation row is the transaction lock. No polling events or
ordinary request bodies are copied into this evidence stream.
"""
import json
from datetime import datetime, timezone
from flask import abort, current_app
from flask_login import current_user
from sqlalchemy import text
from app.extensions import db
from app.models.sace import SaceWorkshopInteraction as Interaction

PASS_MARK = 70  # Existing workshop assessment threshold.
ENGAGEMENT = ("objective", "sequence", "demonstration", "reflection")


def payload(row):
    try:
        value = json.loads(row.response_data)
        return value if isinstance(value, dict) else {}
    except (ValueError, TypeError, AttributeError):
        return {}


def is_controller():
    if not current_user.is_authenticated:
        return False
    from app.models.auth import AuthSubject, AuthSubjectAdmin
    return AuthSubjectAdmin.query.join(AuthSubject).filter(
        db.func.lower(AuthSubjectAdmin.email) == current_user.email.lower(),
        AuthSubject.slug.in_(("sace", "sace_endorsement", "sace_reading"))).first() is not None


def assignments(user_id=None):
    uid = user_id or current_user.id
    return [r for r in Interaction.query.filter_by(activity_slug="auditor_provisioned").order_by(Interaction.id.desc()).all()
            if payload(r).get("claimed_by_user_id") == uid]


def assignment(lock=False, active=True):
    rows = assignments()
    row = next((r for r in rows if payload(r).get("status") == "Claimed"), rows[0] if rows else None)
    if row is None:
        abort(403, description="An assigned Auditor access code is required.")
    if lock:
        row = Interaction.query.filter_by(id=row.id).populate_existing().with_for_update().one()
    state = payload(row)
    if active and state.get("status") != "Claimed":
        abort(403, description="This endorsement assignment has ended. Its evidence remains available to R.")
    if state.get("expires_at"):
        try:
            expired = datetime.fromisoformat(state["expires_at"]).astimezone(timezone.utc) <= datetime.now(timezone.utc)
        except (ValueError, TypeError):
            expired = True
        if expired and active:
            abort(403, description="This endorsement assignment has expired.")
    return row


def room(row):
    return f"endorsement-{row.id}"


def events(row):
    return Interaction.query.filter_by(workshop_session_id=room(row)).order_by(Interaction.id).all()


def latest(row, slug):
    return Interaction.query.filter_by(workshop_session_id=room(row), activity_slug=slug).order_by(Interaction.id.desc()).first()


def record(row, slug, values=None, once=False):
    if once:
        old = latest(row, slug)
        if old:
            return old
    event = Interaction(user_id=current_user.id, workshop_session_id=room(row),
                        activity_slug=slug, response_data=json.dumps(values or {}))
    db.session.add(event)
    db.session.flush()
    # The controller reads this same durable event as a ping; no duplicate notification table.
    return event


def save(row, state):
    row.response_data = json.dumps(state)
    db.session.flush()


def workshop_passed(row):
    state = payload(row)
    result = latest(row, "step34")
    return (state.get("demo_step", 0) == 35 and latest(row, "step31") is not None
            and latest(row, "step33") is not None
            and set(payload(latest(row, "step32")).get("engagement", [])) == set(ENGAGEMENT)
            and result is not None and payload(result).get("passed") is True) if latest(row, "step32") else False


MAP_MATERIALS = ('app_form', 'f_guide', 'p_guide')
MAP_REQUIRED = ('pledge', 'map_reviewed') + MAP_MATERIALS
FINAL_MESSAGE = 'A has completed the AIT activity evaluation journey.'


def course_lessons():
    return db.session.execute(text('SELECT id, "order", title, caption, video_filename FROM rdp_lesson ORDER BY "order"')).mappings().all()


def course_complete(row):
    lessons = course_lessons()
    return len(lessons) == 18 and len({x['order'] for x in lessons}) == 18 and all(
        latest(row, f"reading_lesson_{x['id']}_complete") for x in lessons)


def completion_requirements(row):
    required = MAP_REQUIRED + ('map_complete', 'ppp_complete', 'demo_complete', 'step31', 'step32', 'step33',
        'workshop_certificate', 'reading_complete', 'reading_certificate', 'board_returned')
    missing = [slug for slug in required if not latest(row, slug)]
    if not workshop_passed(row):
        missing.append('step34_pass')
    if not course_complete(row):
        missing.append('18_reading_videos')
    if not step35_passed(row):
        missing.append('step35_pass')
    return missing


def step35_passed(row):
    result = payload(latest(row, 'step35'))
    content = current_app.config.get('AIT_READING_STEP35')
    return (isinstance(content, dict) and bool(content.get('version'))
            and result.get('version') == content['version'] and result.get('passed') is True)
