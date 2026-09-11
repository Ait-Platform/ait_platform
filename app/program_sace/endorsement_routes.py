"""Authoritative AIT endorsement journey; live-roadshow routes are not used here."""
import csv
import io
import json
import secrets
from pathlib import Path
from flask import abort, current_app, g, jsonify, redirect, render_template, request, session, url_for, make_response
from flask_login import current_user
from app.extensions import db
from app.models.sace import SaceDocument, SaceWorkshopInteraction as Interaction
from . import sace_bp
from . import endorsement as flow

R_ENDPOINTS = {"provisioning_map", "generate_auditor_code", "print_access_slip", "provider_documents",
               "document_action", "provisioning_logs", "audit_report", "controller_feed", "audit_export"}
PUBLIC = {"auditor_join", "auditor_pledge", "claim_code", "provisioning_pledge", "sace_about"}
OBSOLETE = {"interactive_workshop", "participant_join", "participant_onboarding", "facilitator_dashboard",
            "reading_workshop_docs", "reviewer_guide", "annexure_a", "annexure_b", "annexure_c",
            "annexure_d", "annexure_e", "compliance_evidence", "evaluator_report", "catalog", "selection_hub"}
DISABLED = {"reset_evaluator_progress", "reset_workshop", "start_workshop", "set_slide", "join_room",
            "submit_interaction", "submit_poll", "get_state", "get_slide", "live_stats", "download_document", "log_event", "log_ppp_view"}


@sace_bp.before_request
def protect_endorsement():
    endpoint = (request.endpoint or '').split('.')[-1]
    if endpoint in PUBLIC or (endpoint == 'provisioning_map' and not current_user.is_authenticated):
        return None
    if not current_user.is_authenticated:
        return redirect(url_for('auth_bp.login', next=request.path))
    if endpoint == 'dashboard':
        return redirect(url_for('sace_bp.provisioning_map' if flow.is_controller() else 'sace_bp.reading_hub'))
    if endpoint in R_ENDPOINTS:
        if not flow.is_controller():
            abort(403)
        if endpoint != 'provisioning_map' and not Interaction.query.filter_by(user_id=current_user.id, activity_slug='admin_patent_pledge').first():
            abort(403, description="Accept the controller pledge first.")
        if endpoint == 'print_access_slip' and not any(flow.payload(row).get('code') == request.view_args.get('code') for row in controller_rows()):
            abort(404)
        return None
    if endpoint in DISABLED and not flow.assignments():
        return None
    if endpoint in DISABLED:
        abort(410, description="This legacy workshop operation is not part of the endorsement journey.")
    if endpoint in {'participant_join', 'participant_onboarding', 'interactive_workshop', 'facilitator_dashboard'} and not flow.assignments():
        return None
    g.sace_assignment = flow.assignment(lock=request.method == 'POST', active=endpoint != 'finish_evaluation')
    if endpoint in OBSOLETE:
        return redirect(url_for('sace_bp.reading_hub'))


def board():
    row = flow.assignment(lock=True)
    flow.record(row, 'map_entered', once=True)
    if flow.latest(row, 'reading_certificate'):
        flow.record(row, 'board_returned', once=True)
    db.session.commit()
    ticks = {e.activity_slug for e in flow.events(row)}
    return render_template('program_sace/endorsement_board.html', ticks=ticks,
                           workshop_passed=flow.workshop_passed(row), state=flow.payload(row), materials=MATERIALS, missing=flow.completion_requirements(row))


def generate_code():
    raw = secrets.token_hex(4).upper()
    row = Interaction(user_id=current_user.id, activity_slug='auditor_provisioned',
        response_data=json.dumps(dict(code=raw[:4]+'-'+raw[4:], status='Unclaimed', first_name='', last_name='', email='')))
    db.session.add(row)
    db.session.flush()
    flow.record(row, 'assignment_provisioned', {'controller_id': current_user.id})
    db.session.commit()
    return redirect(url_for('sace_bp.provisioning_map'))


def claim():
    code = session.get('pending_sace_code')
    if not code or not session.get('sace_evaluator_pledged'):
        return redirect(url_for('sace_bp.auditor_join'))
    # Lock before checking status so a code cannot be claimed concurrently.
    rows = Interaction.query.filter_by(activity_slug='auditor_provisioned').order_by(Interaction.id).with_for_update().all()
    row = next((r for r in rows if secrets.compare_digest(str(flow.payload(r).get('code', '')), str(code))), None)
    if not row or row.user_id == current_user.id or flow.payload(row).get('status') != 'Unclaimed':
        abort(409, description="The access code cannot be claimed.")
    if any(flow.payload(r).get('status') == 'Claimed' for r in flow.assignments()):
        abort(409, description="Complete your existing Auditor assignment first.")
    state = flow.payload(row)
    state.update(status='Claimed', claimed_by_user_id=current_user.id, first_name=current_user.name or '',
                 email=current_user.email, demo_step=0)
    flow.save(row, state)
    flow.record(row, 'pledge', {'accepted': True}, once=True)
    flow.record(row, 'assignment_claimed', {'controller_id': row.user_id}, once=True)
    db.session.commit()
    session.pop('pending_sace_code', None)
    session.pop('sace_evaluator_pledged', None)
    return redirect(url_for('sace_bp.reading_hub'))


MATERIALS = {'app_form': ('Activity application form', 'pdf/App_Form_1.pdf'), 'f_guide': ('Facilitator Manual', 'pdf/F_Guide.pdf'),
             'p_guide': ('Participant / Workshop Manual', 'pdf/P_Guide.pdf')}


def material_path(kind):
    if kind not in MATERIALS:
        abort(404)
    title, fallback = MATERIALS[kind]
    doc = SaceDocument.query.filter_by(slug='reading', document_type=kind).first()
    relative = doc.file_path if doc else fallback
    relative = relative.replace('app/static/', '').removeprefix('static/')
    root = Path(current_app.static_folder).resolve()
    path = (root / relative).resolve()
    if not path.is_relative_to(root) or not path.is_file() or path.suffix.lower() != '.pdf':
        abort(404, description="This controlled material is not available yet; it has not been marked viewed.")
    with path.open('rb') as source:
        if source.read(5) != b'%PDF-':
            abort(404, description="The controlled material is not a usable PDF.")
    return title, path


def material(kind):
    if kind == 'patent':
        return render_template('program_sace/endorsement_map.html')
    title, path = material_path(kind)
    return render_template('program_sace/endorsement_material.html', doc_title=title,
        doc_url=url_for('sace_bp.material_content', kind=kind),
        viewed_url=url_for('sace_bp.material_viewed', kind=kind))


@sace_bp.get('/sace/material/<kind>/content')
def material_content(kind):
    from flask import send_file
    title, path = material_path(kind)
    row = flow.assignment(lock=True)
    flow.record(row, kind + '_opened', once=True)
    db.session.commit()
    response = send_file(path, mimetype='application/pdf', as_attachment=False)
    response.headers['Cache-Control'] = 'private, no-store'
    return response


@sace_bp.post('/sace/material/<kind>/viewed')
def material_viewed(kind):
    material_path(kind)  # Never tick a missing resource.
    row = flow.assignment(lock=True)
    if not flow.latest(row, kind + '_opened'):
        abort(409, description='Open and examine this material before confirming.')
    flow.record(row, kind, {'evidence': 'Auditor confirmation after page review'}, once=True)
    db.session.commit()
    return jsonify(success=True)


def ppp():
    row = flow.assignment(lock=True)
    flow.record(row, 'ppp_entered', once=True)
    db.session.commit()
    return render_template('program_sace/endorsement_ppp.html')


@sace_bp.post('/sace/reading/presentation/viewed/<int:slide>')
def ppp_viewed(slide):
    if not 1 <= slide <= 30 or not (Path(current_app.static_folder)/'sace_slides'/f'{slide}.png').is_file():
        abort(404)
    row = flow.assignment(lock=True)
    flow.record(row, f'ppp_slide_{slide}', once=True)
    flow.record(row, 'ppp', once=True)
    db.session.commit()
    return jsonify(success=True)


def ppp_complete():
    row = flow.assignment(lock=True)
    if not all(flow.latest(row, f'ppp_slide_{i}') for i in range(1,31)):
        abort(409, description="Review all 30 PPP slides first. Previous and Next remain available.")
    flow.record(row, 'ppp_complete', once=True)
    db.session.commit()
    return redirect(url_for('sace_bp.reading_hub'))


def demo():
    row = flow.assignment()
    require_map(row)
    return render_template('program_sace/endorsement_demo.html', step=flow.payload(row).get('demo_step',0),
                           result=flow.payload(flow.latest(row,'step34')) if flow.latest(row,'step34') else {})


@sace_bp.post('/sace/reading/demo/advance')
def demo_advance():
    row = flow.assignment(lock=True)
    require_map(row)
    state = flow.payload(row)
    data = request.get_json(silent=True) or {}
    step = state.get('demo_step', 0)
    if not isinstance(data, dict) or type(data.get('step')) is not int or data.get('step') != step or not 0 <= step <= 33:
        abort(409, description="This step is no longer current. Reload to resume your saved position.")
    if step == 0:
        flow.record(row, 'demo_entered', once=True)
    elif step <= 30:
        if not (Path(current_app.static_folder)/'sace_slides'/f'{step}.png').is_file():
            abort(409, description="Slide unavailable; progress was not saved.")
        flow.record(row, f'demo_slide_{step}', once=True)
    elif step == 31:
        ratings = data.get('ratings', {})
        if not isinstance(ratings, dict) or set(ratings) != {'vocalization','positioning','pacing'} or any(type(v) is not int or v not in range(4) for v in ratings.values()):
            abort(400, description="Complete all three critique ratings (0-3).")
        flow.record(row,'step31',ratings,once=True)
    elif step == 32:
        engagement = data.get('engagement', [])
        if not isinstance(engagement,list) or sorted(engagement) != sorted(flow.ENGAGEMENT):
            abort(400, description="Complete the four required engagement activities before confirming them.")
        responses = data.get('responses', {})
        if not isinstance(responses, dict) or set(responses) != set(flow.ENGAGEMENT) or any(not isinstance(v, str) or not v.strip() or len(v) > 4000 for v in responses.values()):
            abort(400, description='Record a response for each engagement activity.')
        flow.record(row,'step32',{'engagement':engagement, 'responses':responses},once=True)
    elif step == 33:
        answers = data.get('answers', {})
        if (not isinstance(answers, dict) or set(answers) != {'efficacy','utility','fidelity'} or answers.get('efficacy') not in ['1','2','3','4','5']
            or answers.get('utility') not in ['yes','somewhat','no'] or answers.get('fidelity') not in ['strict','modify','loose']):
            abort(400, description="Complete the existing LITRE study baseline questions.")
        flow.record(row,'step33',{'instrument':'existing-baseline-v1','purpose':'Auditor test of longitudinal study baseline; not longitudinal outcomes','answers':answers},once=True)
    state['demo_step'] = step + 1
    flow.save(row,state)
    db.session.commit()
    return jsonify(success=True, next=url_for('sace_bp.simulator'))


def post_test():
    if flow.payload(flow.assignment()).get('demo_step') != 34:
        return redirect(url_for('sace_bp.simulator'))
    return render_template('program_sace/endorsement_workshop_mcq.html')


def mark_workshop():
    row = flow.assignment(lock=True)
    state = flow.payload(row)
    if state.get('demo_step') != 34 or not all(flow.latest(row,f'step{i}') for i in (31,32,33)):
        abort(409, description="Complete the preceding Demo steps first.")
    answers = {key:request.form.get(key) for key in ('q1','q2','q3','q4')}
    if any(value not in ('A','B','C','D') for value in answers.values()):
        abort(400, description="Answer all four workshop questions.")
    score = sum(25 for key,value in dict(q1='B',q2='B',q3='C',q4='A').items() if answers[key] == value)
    answers.update(score=score,passed=score>=flow.PASS_MARK)
    flow.record(row,'step34',answers)
    if answers['passed']:
        state['demo_step']=35
        flow.save(row,state)
        flow.record(row,'demo_complete',once=True)
    db.session.commit()
    return redirect(url_for('sace_bp.post_test_results'))


def results():
    row=flow.assignment()
    event=flow.latest(row,'step34')
    return render_template('program_sace/endorsement_results.html', answers=flow.payload(event) if event else {},
                           eligible=flow.workshop_passed(row))


def send_workshop_certificate():
    from .routes import _generate_sace_certificate_pdf
    row=flow.assignment(lock=True)
    if not flow.workshop_passed(row):
        abort(409, description="Workshop certificate requires engagement completion and a passing post-test.")
    from datetime import datetime
    state=flow.payload(row)
    certificate_id=state.setdefault('workshop_certificate_id','AIT-WS-'+secrets.token_hex(6).upper())
    state.setdefault('workshop_completed_at',datetime.utcnow().isoformat())
    flow.save(row,state)
    try:
        pdf=_generate_sace_certificate_pdf(certificate_id,current_user.name,state['workshop_completed_at'],
            current_user.id,flow.payload(flow.latest(row,'step34')))
    except Exception:
        current_app.logger.exception('Workshop certificate generation failed')
        pdf=None
    return deliver_certificate(row,'workshop_certificate',certificate_id,pdf)


def deliver_certificate(row,slug,certificate_id,pdf):
    from app.subject_reading.routes import _email_certificate_pdf
    email=(request.form.get('email') or '').strip()
    if not email or '@' not in email or any(c.isspace() for c in email):
        abort(400,description="Enter a valid email address.")
    if not pdf:
        flow.record(row,slug+'_failed',{'certificate_id':certificate_id,'reason':'generation'})
        db.session.commit()
        abort(503,description="Certificate generation failed. No email was sent.")
    if current_app.config.get('MAIL_SUPPRESS_SEND',False):
        flow.record(row,slug+'_suppressed',{'certificate_id':certificate_id})
        db.session.commit()
        abort(503,description="Email delivery is suppressed. No email was sent.")
    flow.record(row,slug+'_requested',{'certificate_id':certificate_id})
    db.session.commit()
    try:
        sent=_email_certificate_pdf(email,current_user.name,certificate_id,pdf)
        if sent is not True:
            raise RuntimeError('Sender did not confirm acceptance')
    except Exception:
        row=flow.assignment(lock=True)
        flow.record(row,slug+'_failed',{'certificate_id':certificate_id,'reason':'delivery'})
        db.session.commit()
        abort(503,description="Email delivery failed. Your progress is saved; retry from the board.")
    row=flow.assignment(lock=True)
    flow.record(row,slug,{'certificate_id':certificate_id,'outcome':'accepted_by_mail_sender'})
    db.session.commit()
    return redirect(url_for('sace_bp.reading_hub'))


def require_map(row):
    if not flow.latest(row, 'map_complete') or not flow.latest(row, 'ppp_complete'):
        abort(409, description="Complete the Auditor Map, controlled materials and PPP first.")


@sace_bp.route('/sace/reading/auditor-map', methods=['GET', 'POST'])
def auditor_map():
    row = flow.assignment(lock=True)
    if request.method == 'POST':
        if request.form.get('reviewed') != 'yes':
            abort(400, description="Confirm that you have reviewed the Auditor Map.")
        flow.record(row, 'map_reviewed', once=True)
        if not all(flow.latest(row, slug) for slug in flow.MAP_REQUIRED):
            db.session.commit()
            abort(409, description="Examine all controlled materials listed on the Auditor Board first.")
        flow.record(row, 'map_complete', once=True)
        db.session.commit()
        return redirect(url_for('sace_bp.reading_hub'))
    return render_template('program_sace/endorsement_map.html')


@sace_bp.get('/sace/reading/course')
def reading_course():
    row = flow.assignment(lock=True)
    if not flow.workshop_passed(row):
        abort(409, description="Complete the Demo and pass Step 34 first.")
    flow.record(row, 'reading_entered', once=True)
    lessons = flow.course_lessons()
    completed = {x['id'] for x in lessons if flow.latest(row, f"reading_lesson_{x['id']}_complete")}
    next_lesson = next((x['id'] for x in lessons if x['id'] not in completed), None)
    db.session.commit()
    return render_template('program_sace/endorsement_course.html', lessons=lessons,
                           completed=completed, next_lesson=next_lesson)


def course_lesson(row, lesson_id):
    if not flow.workshop_passed(row):
        abort(409, description="Complete the workshop first.")
    lessons = flow.course_lessons()
    lesson = next((x for x in lessons if x['id'] == lesson_id), None)
    if not lesson:
        abort(404)
    if any(not flow.latest(row, f"reading_lesson_{x['id']}_complete") for x in lessons if x['order'] < lesson['order']):
        abort(409, description="Complete the preceding Reading videos first.")
    return lesson


@sace_bp.route('/sace/reading/course/<int:lesson_id>', methods=['GET', 'POST'])
def reading_lesson(lesson_id):
    row = flow.assignment(lock=True)
    lesson = course_lesson(row, lesson_id)
    if request.method == 'POST':
        if not flow.latest(row, f'reading_lesson_{lesson_id}_served') or request.form.get('completed') != 'yes':
            abort(409, description="Play and complete the video first.")
        flow.record(row, f'reading_lesson_{lesson_id}_complete', {'lesson_order': lesson['order'], 'evidence': 'video-ended confirmation'}, once=True)
        if flow.course_complete(row):
            flow.record(row, 'reading_complete', once=True)
        db.session.commit()
        return redirect(url_for('sace_bp.reading_course'))
    flow.record(row, f'reading_lesson_{lesson_id}_opened', {'lesson_order': lesson['order']}, once=True)
    db.session.commit()
    return render_template('program_sace/endorsement_lesson.html', lesson=lesson)


@sace_bp.get('/sace/reading/course/<int:lesson_id>/video')
def reading_video(lesson_id):
    from flask import send_file
    row = flow.assignment()
    lesson = course_lesson(row, lesson_id)
    root = (Path(current_app.static_folder) / 'uploads/reading_videos').resolve()
    path = (root / (lesson['video_filename'] or '')).resolve()
    if not path.is_relative_to(root) or not path.is_file():
        abort(404, description="This Reading video is unavailable.")
    row = flow.assignment(lock=True)
    flow.record(row, f'reading_lesson_{lesson_id}_served', once=True)
    db.session.commit()
    response = send_file(path, conditional=True)
    response.headers['Cache-Control'] = 'private, no-store'
    return response


def reading_mcq():
    # Provider course content only. Never SACE endorsement criteria.
    content = current_app.config.get('AIT_READING_STEP35')
    if not content:
        return None
    if not isinstance(content, dict):
        abort(503, description='Step 35 course configuration is invalid.')
    questions = content.get('questions', [])
    valid = (isinstance(content.get('version'), str) and bool(content['version'])
             and type(content.get('pass_percent')) is int and 0 < content['pass_percent'] <= 100
             and isinstance(questions, list) and bool(questions))
    if not valid or any(not isinstance(q, dict) or not isinstance(q.get('id'), str) or not q['id'] or q['id'] in ('csrf_token', 'version') or not isinstance(q.get('prompt'), str) or not q['prompt']
        or not isinstance(q.get('options'), dict) or len(q['options']) < 2
        or q.get('answer') not in q['options'] for q in questions):
        abort(503, description="Step 35 content configuration is incomplete.")
    if len({q['id'] for q in questions}) != len(questions):
        abort(503, description="Step 35 question identifiers must be unique.")
    return content


@sace_bp.route('/sace/reading/step35', methods=['GET', 'POST'])
def step35():
    row = flow.assignment(lock=True)
    if not flow.workshop_passed(row) or not flow.course_complete(row):
        abort(409, description="Complete the workshop and all 18 Reading videos before Step 35.")
    flow.record(row, 'reading_complete', once=True)
    content = reading_mcq()
    if request.method == 'POST':
        if content is None:
            abort(409, description="Step 35 questions have not been supplied. No pass has been recorded.")
        if request.form.get('version') != content['version']:
            abort(409, description="Step 35 content changed. Reload before submitting.")
        answers = {q['id']: request.form.get(q['id']) for q in content['questions']}
        if any(answers[q['id']] not in q['options'] for q in content['questions']):
            abort(400, description="Answer every Reading course question.")
        score = 100 * sum(answers[q['id']] == q['answer'] for q in content['questions']) / len(content['questions'])
        flow.record(row, 'step35', {'version': content['version'], 'answers': answers, 'score': score,
                                  'pass_percent': content['pass_percent'], 'passed': score >= content['pass_percent']})
    result = flow.payload(flow.latest(row, 'step35'))
    db.session.commit()
    public_content = None if content is None else dict(version=content['version'], questions=[
        {k: q[k] for k in ('id', 'prompt', 'options')} for q in content['questions']])
    return render_template('program_sace/step35.html', content=public_content, result=result)


@sace_bp.route('/sace/reading/course/certificate', methods=['GET', 'POST'])
def reading_certificate():
    row = flow.assignment(lock=True)
    eligible = flow.course_complete(row) and flow.step35_passed(row)
    if request.method == 'POST':
        if not eligible:
            flow.record(row, 'reading_certificate_blocked', {'reason': 'course_or_step35_incomplete'})
            db.session.commit()
            abort(409, description="Complete all 18 videos and pass Step 35 before requesting the Reading certificate.")
        from app.subject_reading.routes import _generate_certificate_pdf
        from datetime import datetime
        state = flow.payload(row)
        cid = state.setdefault('reading_certificate_id', 'AIT-RD-' + secrets.token_hex(6).upper())
        completed_at = state.setdefault('reading_completed_at', datetime.utcnow().isoformat())
        flow.save(row, state)
        try:
            pdf = _generate_certificate_pdf(cid, current_user.name, completed_at, current_user.id)
        except Exception:
            current_app.logger.exception('Reading certificate generation failed')
            pdf = None
        return deliver_certificate(row, 'reading_certificate', cid, pdf)
    return render_template('program_sace/endorsement_reading_certificate.html', eligible=eligible)


@sace_bp.route('/sace/reading/finish-evaluation', methods=['GET', 'POST'])
def finish_evaluation():
    row = flow.assignment(lock=True, active=False)
    if flow.payload(row).get('status') == 'Completed':
        return render_template('program_sace/evaluation_closed.html')
    row = flow.assignment(lock=True)
    missing = flow.completion_requirements(row)
    if request.method == 'POST':
        if missing:
            abort(409, description="Complete the outstanding AIT activities on the Auditor Board first.")
        state = flow.payload(row)
        event = flow.record(row, 'evaluation_complete', {'message': flow.FINAL_MESSAGE,
            'controller_id': row.user_id, 'sace_decision': 'external'}, once=True)
        flow.record(row, 'controller_notification', {'message': flow.FINAL_MESSAGE,
            'controller_id': row.user_id, 'evaluation_event_id': event.id, 'channel': 'R Control Centre'}, once=True)
        flow.record(row, 'assignment_closed', {'reason': 'AIT activity evaluation journey completed'}, once=True)
        state['status'] = 'Completed'
        flow.save(row, state)
        db.session.commit()  # Evidence, durable R notification, then closure: all commit or none do.
        return render_template('program_sace/evaluation_closed.html')
    return render_template('program_sace/evaluation_finish.html', ready=not missing, missing=missing)


def controller_rows():
    return Interaction.query.filter_by(user_id=current_user.id,activity_slug='auditor_provisioned').order_by(Interaction.id.desc()).all()


def controller_events():
    rooms=[flow.room(row) for row in controller_rows()]
    return Interaction.query.filter(Interaction.workshop_session_id.in_(rooms)).order_by(Interaction.id).all() if rooms else []


@sace_bp.get('/sace/provisioning/feed')
def controller_feed():
    after=request.args.get('after',0,type=int)
    rows=[e for e in controller_events() if e.id>after]
    return jsonify(events=[dict(id=e.id,assignment=e.workshop_session_id,user_id=e.user_id,
        action=e.activity_slug,time=e.timestamp.isoformat(),data=flow.payload(e)) for e in rows])


def controller_audit():
    return render_template('program_sace/endorsement_audit.html',events=controller_events())


@sace_bp.get('/sace/provisioning/audit.csv')
def audit_export():
    out=io.StringIO()
    writer=csv.writer(out)
    writer.writerow(['event_id','assignment','user_id','action','timestamp','evidence'])
    for event in controller_events():
        writer.writerow([csv_cell(value) for value in (event.id,event.workshop_session_id,event.user_id,event.activity_slug,
                         event.timestamp.isoformat(),event.response_data)])
    response=make_response(out.getvalue())
    response.headers['Content-Type']='text/csv; charset=utf-8'
    response.headers['Content-Disposition']='attachment; filename=ait-endorsement-audit.csv'
    response.headers['Cache-Control']='private, no-store'
    return response


def csv_cell(value):
    value = str(value)
    return "'" + value if value.lstrip().startswith(('=', '+', '-', '@', '\t', '\r', '\n')) else value


@sace_bp.after_request
def private_journey_response(response):
    if getattr(g, 'sace_assignment', None) is not None:
        response.headers['Cache-Control'] = 'private, no-store'
    return response


@sace_bp.get('/sace/reading/slide/<int:slide>')
def journey_slide(slide):
    from flask import send_file
    if not 1 <= slide <= 30:
        abort(404)
    path = Path(current_app.static_folder) / 'sace_slides' / f'{slide}.png'
    if not path.is_file():
        abort(404)
    return send_file(path)
