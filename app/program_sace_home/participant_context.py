"""Explicit HOME examination context over the original participant handlers."""
from flask import abort, g, request, url_for
from flask_login import current_user
from app.extensions import db
from app.models.sace_home import HomeEvidence
from app.models.home import HomeFinalAssessment
from . import service as s

VERSION = 'home-participant-examination-v1'
PARAM = 'home_assignment_id'


def resolve():
    values = request.args.getlist(PARAM) + request.form.getlist(PARAM)
    if not values:
        return None
    if not current_user.is_authenticated or len(set(values)) != 1:
        abort(403)
    try:
        assignment_id = int(values[0])
    except (ValueError, TypeError):
        abort(403)
    row = s.assignment(assignment_id, lock=True, writable=request.method == 'POST')
    if row.requirements_version != 'home-auditor-v2':
        abort(409, description='This historical assignment retains its original requirements.')
    g.home_auditor = row
    return row


def current():
    return getattr(g, 'home_auditor', None)


def latest(row, item, event):
    return next((e for e in HomeEvidence.query.filter_by(assignment_id=row.id,
        actor_id=row.auditor_id, item=item, event=event).order_by(HomeEvidence.id.desc()).all()
        if e.details.get('examination') == VERSION), None)


def record(row, item, event, details=None):
    return s.record(row, item, event, dict(details or {}, examination=VERSION))


def progress(row):
    return {f'chapter_{n}_done': bool(latest(row, f'participant_chapter:{n}', 'examined'))
            for n in range(1, 31)}


def check_order(row, number):
    if not 1 <= number <= 30:
        abort(404)
    if any(not latest(row, f'participant_chapter:{n}', 'examined') for n in range(1, number)):
        abort(409, description='Examine or skip the preceding HOME chapters first.')


def advance(row, number, action):
    check_order(row, number)
    if action not in {'skip', 'teacher_examined', 'examined'} or (action == 'teacher_examined' and number > 10):
        abort(400)
    if not latest(row, f'participant_chapter:{number}', 'opened'):
        abort(409, description='Open this HOME chapter before continuing.')
    if not latest(row, f'participant_chapter:{number}', 'examined'):
        record(row, f'participant_chapter:{number}', 'examined',
            {'chapter_number': number, 'action': action})
    db.session.commit()
    return url_for('home_bp.chapter_page', chapter_num=number + 1) if number < 30 else url_for('home_sace_bp.material', assignment_id=row.id, kind='final_assessment')


def assessment(row, assessment_id=None):
    if assessment_id is not None:
        events = HomeEvidence.query.filter_by(assignment_id=row.id, actor_id=row.auditor_id,
            item='participant_assessment', event='completed').order_by(HomeEvidence.id.desc()).all()
        event = next((e for e in events if e.details.get('examination') == VERSION
            and str(e.details.get('assessment_id')) == str(assessment_id)), None)
    else:
        event = latest(row, 'participant_assessment', 'completed')
    if not event:
        return None
    result = db.session.get(HomeFinalAssessment, event.details['assessment_id'])
    return result if result and result.user_id == row.auditor_id else None


def journey_complete(row):
    return all(progress(row).values()) and assessment(row) is not None


def certificate_delivered(row):
    events = HomeEvidence.query.filter_by(assignment_id=row.id, actor_id=row.auditor_id,
        item='participant_certificate', event='sent').all()
    return any(e.details.get('examination') == VERSION
        and e.details.get('outcome') == 'accepted_by_mail_sender'
        and (result := assessment(row, e.details.get('assessment_id'))) is not None
        and result.passed and e.details.get('pdf_sha256') for e in events)
