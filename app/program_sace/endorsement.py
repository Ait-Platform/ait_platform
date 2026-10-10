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



def assignment_expired(state):
    value = state.get("expires_at")
    if value is None or value == "":
        return False
    try:
        expiry = datetime.fromisoformat(value)
        # Legacy timestamps without an offset are interpreted consistently as UTC.
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=timezone.utc)
        return expiry.astimezone(timezone.utc) <= datetime.now(timezone.utc)
    except (TypeError, ValueError, OverflowError):
        return True


def invitation_error(row):
    if row is None:
        return "Invalid or unrecognized Access Code."
    from .lifecycle import engagement_for_assignment
    if not engagement_for_assignment(row):
        return "This endorsement engagement is not active."
    state = payload(row)
    if state.get("status") != "Unclaimed":
        return "This Access Code is no longer available."
    if assignment_expired(state):
        return "This Access Code has expired. Please ask the SACE administrator for a new code."
    return None


def is_controller():
    from .access import is_controller as controller_access
    return controller_access()


def assignments(user_id=None, active_only=False):
    uid = user_id or current_user.id
    rows = [r for r in Interaction.query.filter_by(activity_slug="auditor_provisioned").order_by(Interaction.id.desc()).all()
            if payload(r).get("claimed_by_user_id") == uid]
    if active_only:
        from .lifecycle import engagement_for_assignment
        rows = [r for r in rows if payload(r).get('status') == 'Claimed'
                and not assignment_expired(payload(r)) and engagement_for_assignment(r)]
    return rows


def assignment(lock=False, active=True):
    from .lifecycle import subject_lock, engagement_for_assignment
    if lock:
        subject_lock()
    rows = assignments(active_only=active)
    row = rows[0] if rows else None
    if row is None:
        abort(403, description="An active Auditor assignment is required.")
    if lock:
        row = Interaction.query.filter_by(id=row.id).populate_existing().with_for_update().one()
    state = payload(row)
    if not engagement_for_assignment(row):
        abort(403, description="This endorsement engagement has ended.")
    if active and (state.get('status') != 'Claimed' or assignment_expired(state)):
        abort(403, description="This Auditor assignment has ended.")
    return row


def room(row):
    return f"endorsement-{row.id}"


def events(row):
    return Interaction.query.filter_by(workshop_session_id=room(row)).order_by(Interaction.id).all()


def latest(row, slug):
    event = Interaction.query.filter_by(workshop_session_id=room(row), activity_slug=slug).order_by(Interaction.id.desc()).first()
    if event is None and slug == 'ip_pledge':
        # The assignment's original entry acceptance also examines this reference.
        auditor_id = payload(row).get('claimed_by_user_id')
        if auditor_id is not None:
            pledge = Interaction.query.filter_by(workshop_session_id=room(row),
                activity_slug='pledge', user_id=auditor_id).order_by(Interaction.id.desc()).first()
            if pledge is not None and payload(pledge).get('accepted') is True:
                return pledge
    return event


def record(row, slug, values=None, once=False):
    if once:
        old = latest(row, slug)
        if old:
            return old
    event = Interaction(user_id=current_user.id, workshop_session_id=room(row),
                        activity_slug=slug, response_data=json.dumps(values or {}))
    db.session.add(event)
    db.session.flush()
    if slug not in {'map_complete', 'ppp_complete', 'reading_complete', 'evaluation_ready'}:
        refresh_progress(row)
    # The controller reads this same durable event as a ping; no duplicate notification table.
    return event


def save(row, state):
    row.response_data = json.dumps(state)
    db.session.flush()


def workshop_passed(row):
    from .workshop_interactions import answers_valid, EXPERIENCE_QUESTIONS
    state = payload(row)
    experience = payload(latest(row, "step32"))
    experience_complete = (answers_valid(experience.get('answers'), EXPERIENCE_QUESTIONS)
        if experience.get('instrument') == 'participant-experience-v1'
        else set(experience.get('engagement', [])) == set(ENGAGEMENT))
    result = latest(row, "step34")
    return (state.get("demo_step", 0) == 35 and latest(row, "step31") is not None
            and latest(row, "step33") is not None
            and experience_complete
            and result is not None and payload(result).get("passed") is True) if latest(row, "step32") else False


MAP_MATERIALS = ('app_form', 'app_form_2', 'f_guide', 'p_guide', 'timetable', 'ip_pledge')
MAP_REQUIRED = ('pledge', 'map_reviewed') + MAP_MATERIALS
FINAL_MESSAGE = 'The Auditor has completed the AIT examination journey and has notified SACE Admin.'


def course_lessons():
    return db.session.execute(text('SELECT id, "order", title, caption, video_filename FROM rdp_lesson ORDER BY "order"')).mappings().all()


def course_complete(row):
    lessons = course_lessons()
    return len(lessons) == 18 and len({x['order'] for x in lessons}) == 18 and all(
        latest(row, f"reading_lesson_{x['id']}_complete") for x in lessons)


def certificate_delivered(row, slug):
    return payload(latest(row, slug)).get('outcome') == 'accepted_by_mail_sender'


def completion_requirements(row):
    from . import certification as cert
    missing = [slug for slug, *_ in cert.BOARD_ITEMS if latest(row, slug) is None]
    if cert.saved(row) is None:
        missing.append(cert.SLUG)
    return missing


def step35_passed(row):
    result = payload(latest(row, 'step35'))
    content = current_app.config.get('AIT_READING_STEP35')
    return (isinstance(content, dict) and bool(content.get('version'))
            and result.get('version') == content['version'] and result.get('passed') is True)


def refresh_progress(row):
    """Derive milestones from recorded evidence; final closure stays explicit."""
    if all(latest(row, slug) for slug in MAP_REQUIRED):
        record(row, 'map_complete', {'evidence': 'map and controlled materials examined'}, once=True)
    if all(latest(row, f'ppp_slide_{i}') for i in range(1, 32)):
        record(row, 'ppp_complete', {'slides': 31}, once=True)
    if course_complete(row):
        record(row, 'reading_complete', {'videos': 18}, once=True)
    missing = completion_requirements(row)
    state = payload(row)
    state['journey_missing'] = missing
    state['journey_ready'] = not missing
    save(row, state)
    if not missing:
        record(row, 'evaluation_ready', {'evidence': 'all required activity evidence recorded'}, once=True)
    return missing
