"""Authoritative AIT endorsement journey; live-roadshow routes are not used here."""
import csv
import io
import json
import secrets
from pathlib import Path
from flask import abort, current_app, flash, g, jsonify, redirect, render_template, request, session, url_for, make_response
from flask_login import current_user
from app.extensions import db
from app.models.sace import SaceDocument, SaceWorkshopInteraction as Interaction
from . import sace_bp
from . import endorsement as flow

R_ENDPOINTS = {"provisioning_map", "generate_auditor_code", "print_access_slip", "provider_documents",
               "document_action", "provisioning_logs", "audit_report", "controller_feed", "audit_export",
               "reading_lifecycle", "reading_handover", "reading_end_appointment", "reading_end_engagement",
               "reading_complete_engagement", "reading_cancel_completion"}
PUBLIC = {"auditor_join", "auditor_pledge", "claim_code", "provisioning_pledge", "sace_about"}
OBSOLETE = {"interactive_workshop", "participant_join", "participant_onboarding", "facilitator_dashboard",
            "reading_workshop_docs", "reviewer_guide", "annexure_a", "annexure_b", "annexure_c",
            "annexure_d", "annexure_e", "compliance_evidence", "evaluator_report", "catalog", "selection_hub"}
DISABLED = {"reset_evaluator_progress", "reset_workshop", "start_workshop", "set_slide", "join_room",
            "submit_interaction", "submit_poll", "get_state", "get_slide", "live_stats", "download_document", "log_event", "log_ppp_view"}


@sace_bp.before_request
def protect_endorsement():
    endpoint = (request.endpoint or '').split('.')[-1]
    if current_user.is_authenticated and flow.is_controller() and endpoint not in R_ENDPOINTS | {'provisioning_pledge', 'sace_about'}:
        return redirect(url_for('sace_bp.provisioning_map'))
    if endpoint in PUBLIC or endpoint == 'provisioning_map':
        return None
    if not current_user.is_authenticated:
        return redirect(url_for('auth_bp.login', next=request.path))
    if endpoint == 'dashboard':
        if flow.assignments(active_only=True):
            return redirect(url_for('sace_bp.reading_hub'))
        abort(403, description="An Auditor assignment or existing controller grant is required.")
    if endpoint in R_ENDPOINTS:
        if not flow.is_controller():
            abort(403)
        from .lifecycle import require_controller
        require_controller(lock=request.method == 'POST')
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
    g.sace_assignment = flow.assignment(lock=request.method == 'POST', active=True)
    if endpoint in OBSOLETE:
        return redirect(url_for('sace_bp.reading_hub'))


def board():
    row = flow.assignment(lock=True)
    flow.record(row, 'map_entered', once=True)
    if flow.certificate_delivered(row, 'reading_certificate'):
        flow.record(row, 'board_returned', once=True)
    db.session.commit()
    flow.refresh_progress(row)
    db.session.commit()
    ticks = {e.activity_slug for e in flow.events(row)}
    for slug in ('workshop_certificate', 'reading_certificate'):
        if not flow.certificate_delivered(row, slug):
            ticks.discard(slug)
    if flow.workshop_passed(row):
        ticks.add('workshop_evaluation_complete')
    if not all(flow.latest(row, f'ppp_slide_{i}') for i in range(1,32)):
        ticks.discard('ppp_complete')
    from . import certification as cert
    certification = cert.saved(row)
    return render_template('program_sace/endorsement_board.html', ticks=ticks,
                           evidence_items=cert.BOARD_ITEMS, certification_recorded=certification is not None,
                           certification_available=certification is not None or cert.available(row),
                           workshop_passed=flow.workshop_passed(row), state=flow.payload(row), materials=MATERIALS, missing=flow.completion_requirements(row))


@sace_bp.route('/sace/reading/certification', methods=['GET', 'POST'])
def reading_certification():
    from . import certification as cert
    from app.utils.branding import get_logo_data_uri, get_seal_data_uri
    row = flow.assignment(lock=True)
    evidence = cert.saved(row)
    if evidence is None:
        if request.method == 'POST':
            abort(409, description='Open your endorsement certification before emailing it.')
        evidence = cert.create(row, current_user)
        db.session.commit()
    details = flow.payload(evidence)
    from app.models.auth import User
    auditor = db.session.get(User, details['snapshot']['auditor']['id'])
    default_email = (auditor.email or '').strip() if auditor is not None else ''
    if request.method == 'POST':
        if request.form.get('action') != 'email':
            abort(400)
        auditor_id = details['snapshot']['auditor']['id']
        if auditor_id != flow.payload(row).get('claimed_by_user_id') or auditor_id != current_user.id:
            abort(403)
        from email_validator import validate_email, EmailNotValidError
        try:
            recipient = validate_email(request.form.get('email', '').strip(),
                check_deliverability=False).normalized
        except EmailNotValidError:
            abort(400, description='Enter a valid recipient email address.')
        if current_app.config.get('MAIL_SUPPRESS_SEND'):
            abort(503, description='Email sending is suppressed; certification email was not sent.')
    from .lifecycle import AssignmentContext, Appointment
    sace_admin = (db.session.query(User.name, User.email)
        .join(Appointment, Appointment.user_id == User.id)
        .join(AssignmentContext, AssignmentContext.issuing_appointment_id == Appointment.id)
        .filter(AssignmentContext.invitation_event_id == row.id,
            AssignmentContext.engagement_id == details['snapshot']['engagement_id'],
            Appointment.engagement_id == AssignmentContext.engagement_id).one())
    certificate_html = render_template('program_sace/certification_snapshot.html',
        snapshot=details['snapshot'], digest=details['snapshot_sha256'],
        certificate_view=True, sace_admin=sace_admin,
        logo_path=get_logo_data_uri(), seal_path=get_seal_data_uri())
    if request.method == 'POST':
        from app.utils.mailer import send_email
        snapshot = details['snapshot']
        body = '\n'.join([snapshot['statement'], 'Auditor: ' + snapshot['auditor']['name'],
            'Engagement: ' + snapshot['engagement_reference'], 'Certified: ' + snapshot['certified_at'],
            *[str(n) + '. ' + item['title'] for n, item in enumerate(snapshot['items'], 1)],
            'Snapshot SHA-256: ' + details['snapshot_sha256']])
        if not send_email('Reading endorsement examination certification', [recipient], body, html=certificate_html):
            abort(503, description='Certification email could not be sent. You can retry.')
        flash('Your endorsement examination certification has been emailed successfully.', 'success')
        return redirect(url_for('sace_bp.reading_certification'))
    return render_template('program_sace/endorsement_certification.html',
        certificate_html=certificate_html, default_email=default_email)


def generate_code():
    from . import lifecycle
    appointment = lifecycle.require_controller()
    raw = secrets.token_hex(4).upper()
    row = Interaction(user_id=current_user.id, activity_slug='auditor_provisioned',
        response_data=json.dumps(dict(code=raw[:4]+'-'+raw[4:], status='Unclaimed', first_name='', last_name='', email='')))
    db.session.add(row)
    db.session.flush()
    evidence = flow.record(row, 'assignment_provisioned',
        {'controller_id': current_user.id, 'appointment_id': appointment.id})
    lifecycle.link_assignment(row, appointment, evidence.id)
    db.session.commit()
    return redirect(url_for('sace_bp.provisioning_map'))


def claim():
    if flow.is_controller():
        return redirect(url_for('sace_bp.provisioning_map'))
    code = session.get('pending_sace_code')
    if not code or not session.get('sace_evaluator_pledged'):
        return redirect(url_for('sace_bp.auditor_join'))
    if not (current_user.name or '').strip():
        return redirect(url_for('sace_bp.auditor_pledge'))
    from .lifecycle import subject_lock
    subject_lock()
    # Lock before checking status so a code cannot be claimed concurrently.
    rows = Interaction.query.filter_by(activity_slug='auditor_provisioned').order_by(Interaction.id).with_for_update().all()
    row = next((r for r in rows if secrets.compare_digest(str(flow.payload(r).get('code', '')), str(code))), None)
    error = flow.invitation_error(row)
    if error:
        abort(409, description=error)
    if row.user_id == current_user.id:
        abort(409, description="The access code cannot be claimed.")
    if any(flow.payload(r).get('status') == 'Claimed' for r in flow.assignments(active_only=True)):
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


MATERIALS = {'app_form': ('Application Form 1', 'pdf/App_Form_1.pdf'),
             'app_form_2': ('Application Form 2', 'pdf/App_Form_2.pdf'),
             'timetable': ('Reading Timetable (T/T)', 'pdf/Reading Timetable.pdf'),
             'ip_pledge': ('AIT IP Pledge (reference)', None), 'f_guide': ('Facilitator Manual', 'pdf/Reading_Facilitator_Manual.pdf'),
             'p_guide': ('Workshop Manual', 'pdf/Reading_Workshop_Manual.pdf')}


def material_path(kind):
    if kind not in MATERIALS or kind == 'ip_pledge':
        abort(404)
    title, fallback = MATERIALS[kind]
    doc = SaceDocument.query.filter_by(slug='reading', document_type=kind).first()
    # Serve the canonical Reading timetable and regenerated manuals even when
    # their catalogue mappings still point to older or missing assets.
    relative = fallback if kind in ('timetable', 'f_guide', 'p_guide') else (doc.file_path if doc else fallback)
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
    if kind in ('patent', 'ip_pledge'):
        row = flow.assignment(lock=True)
        response = render_template('program_sace/endorsement_pledge_reference.html')
        flow.record(row, 'ip_pledge', {'evidence': 'existing pledge displayed for reference'}, once=True)
        db.session.commit()
        return response
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
    flow.record(row, kind, {'evidence': 'all PDF pages displayed by controlled viewer'}, once=True)
    db.session.commit()
    return jsonify(success=True)


def ppp():
    row = flow.assignment(lock=True)
    flow.record(row, 'ppp_entered', once=True)
    db.session.commit()
    return render_template('program_sace/endorsement_ppp.html',
                           examined=all(flow.latest(row, f'ppp_slide_{i}') for i in range(1, 32)))


@sace_bp.post('/sace/reading/presentation/viewed/<int:slide>')
def ppp_viewed(slide):
    if not 1 <= slide <= 31 or not (Path(current_app.static_folder)/'sace_slides'/f'{slide}.png').is_file():
        abort(404)
    row = flow.assignment(lock=True)
    flow.record(row, f'ppp_slide_{slide}', once=True)
    flow.record(row, 'ppp', once=True)
    flow.refresh_progress(row)
    db.session.commit()
    return jsonify(success=True, complete=all(flow.latest(row, f'ppp_slide_{i}') for i in range(1,32)))


def ppp_complete():
    row = flow.assignment(lock=True)
    if not all(flow.latest(row, f'ppp_slide_{i}') for i in range(1,32)):
        abort(409, description="Review all 31 PPP slides first. Previous and Next remain available.")
    flow.record(row, 'ppp_complete', once=True)
    db.session.commit()
    return redirect(url_for('sace_bp.reading_hub'))


# Stable historical competency fields from simulator.html (c0f89c03 / 8868a05b).
COMPETENCIES = ('comp_objective', 'comp_sequence', 'comp_demo', 'comp_participation',
                'comp_guidance', 'comp_reading', 'comp_assessment', 'comp_reflection')
DEMO_SEQUENCE = 'workshop-31-v2'


def demo_state(row):
    """Resume old positions without deleting evidence or replaying completed slides."""
    state = flow.payload(row)
    # The two examined manuals cover slides 1-31; preserve old slide events.
    if state.get('demo_step', 0) < 32:
        state['demo_step'] = 32
        flow.save(row, state)
    if state.get('demo_sequence') != DEMO_SEQUENCE:
        old_step = state.get('demo_step', 0)
        # Old 32/33 already had critique/application recorded one number earlier.
        if old_step == 32 and flow.latest(row, 'step31'):
            state['demo_step'] = 33
        elif old_step == 33 and flow.latest(row, 'step32'):
            state['demo_step'] = 34
        else:
            state['demo_step'] = old_step or 1
        state['demo_sequence'] = DEMO_SEQUENCE
        flow.save(row, state)
    if state.get('demo_step', 0) == 0:
        state['demo_step'] = 1
        flow.save(row, state)
    return state


def demo():
    row = flow.assignment(lock=True)
    state = demo_state(row)
    flow.record(row, 'demo_entered', once=True)
    db.session.commit()
    if state['demo_step'] == 35:
        return redirect(url_for('sace_bp.post_test_results' if flow.workshop_passed(row) else 'sace_bp.step35'))
    from . import workshop_interactions as instrument
    return render_template('program_sace/endorsement_demo.html', step=state['demo_step'],
        instrument=instrument)


@sace_bp.post('/sace/reading/demo/advance')
def demo_advance():
    row = flow.assignment(lock=True)
    state = demo_state(row)
    data = request.get_json(silent=True) or {}
    step = state['demo_step']
    if not isinstance(data, dict) or type(data.get('step')) is not int or data.get('step') != step or not 1 <= step <= 34:
        abort(409, description="This step is no longer current. Reload to resume your saved position.")
    if step <= 31:
        if not (Path(current_app.static_folder)/'sace_slides'/f'{step}.png').is_file():
            abort(409, description="Slide unavailable; progress was not saved.")
        flow.record(row, f'demo_slide_{step}', once=True)
    elif step in (32, 33):
        from . import workshop_interactions as instrument
        questions = instrument.FACILITATOR_QUESTIONS if step == 32 else instrument.EXPERIENCE_QUESTIONS
        answers = data.get('answers')
        if not instrument.answers_valid(answers, questions):
            abort(400, description="Answer every question using Yes, Unsure or No.")
        values = {'instrument': 'facilitator-evaluation-v1' if step == 32 else 'participant-experience-v1',
                  'answers': answers}
        if step == 32:
            comment = data.get('comment', '')
            if not isinstance(comment, str) or len(comment) > 2000:
                abort(400, description="The optional comment must be text of at most 2000 characters.")
            values['comment'] = comment.strip()
        # Retain established evidence keys; their historical numbers are not UI steps.
        flow.record(row, 'step31' if step == 32 else 'step32', values, once=True)
    elif step == 34:
        from datetime import datetime, timezone
        intention = data.get('intention_to_use')
        if not isinstance(intention, str) or intention not in ('Yes', 'No'):
            abort(400, description="Select Yes or No before continuing.")
        survey = {'instrument': 'reading-longitudinal-intention-v1',
                  'intention_to_use': intention,
                  'recorded_at': datetime.now(timezone.utc).isoformat(),
                  'responding_user_id': current_user.id,
                  'context': {'programme': 'reading', 'assignment_id': row.id,
                              'kind': 'auditor-workshop-examination'}}
        # Retain the established prerequisite events for the unchanged Step 35.
        flow.record(row, 'workshop_survey', survey, once=True)
        flow.record(row, 'step33', survey, once=True)
    state['demo_step'] = step + 1
    flow.save(row, state)
    flow.refresh_progress(row)
    db.session.commit()
    return jsonify(success=True, next=url_for('sace_bp.step35' if step == 34 else 'sace_bp.simulator'))


def post_test():
    return redirect(url_for('sace_bp.step35'))


@sace_bp.route('/sace/reading/step35', methods=['GET', 'POST'])
def step35():
    """Workshop Step 35 is the historically established four-question post-test."""
    row = flow.assignment(lock=True)
    state = demo_state(row)
    if state.get('demo_step') != 35:
        db.session.commit()
        return redirect(url_for('sace_bp.simulator'))
    if request.method == 'POST':
        return mark_workshop()
    db.session.commit()
    if flow.workshop_passed(row):
        return redirect(url_for('sace_bp.post_test_results'))
    return render_template('program_sace/endorsement_workshop_mcq.html',
        result=flow.payload(flow.latest(row, 'step34')))


def mark_workshop():
    row = flow.assignment(lock=True)
    state = demo_state(row)
    if state.get('demo_step') != 35 or not all(flow.latest(row, key) for key in ('step31','step32','workshop_survey')):
        abort(409, description="Complete Critique 32, Classroom Application 33 and Competency Survey 34 first.")
    if flow.workshop_passed(row):
        return redirect(url_for('sace_bp.post_test_results'))
    answers = {key: request.form.get(key) for key in ('q1','q2','q3','q4')}
    if any(value not in ('A','B','C','D') for value in answers.values()):
        abort(400, description="Answer all four workshop questions.")
    score = sum(25 for key,value in dict(q1='B',q2='B',q3='C',q4='A').items() if answers[key] == value)
    answers.update(score=score, passed=score >= flow.PASS_MARK)
    flow.record(row, 'step34', answers)  # Established workshop-result/certificate evidence key.
    if answers['passed']:
        flow.record(row, 'demo_complete', once=True)
    db.session.commit()
    return redirect(url_for('sace_bp.post_test_results' if answers['passed'] else 'sace_bp.step35'))


def results():
    row = flow.assignment(lock=True)
    event = flow.latest(row, 'step34')
    eligible = flow.workshop_passed(row)
    evidence = eligible or request.args.get('evidence') == '1'
    if evidence and not eligible:
        abort(409, description="Pass the Workshop Post-Test before examining certificate evidence.")
    if request.method == 'POST':
        if not evidence:
            abort(409, description="Use the Workshop Certificate evidence item on the Auditor Board.")
        return send_workshop_certificate()
    certificate_html = certificate_presentation(row, 'workshop') if evidence else None
    db.session.commit()
    return render_template('program_sace/endorsement_results.html',
        answers=flow.payload(event) if event else {}, eligible=eligible, evidence=evidence,
        certificate_html=certificate_html)


def certificate_presentation(row, kind):
    """Present the existing eligible certificate without invoking a PDF viewer."""
    from datetime import datetime, timezone
    from app.utils.branding import get_logo_data_uri, get_seal_data_uri
    if kind == 'workshop':
        if not flow.workshop_passed(row):
            abort(409, description="Complete Steps 32-34 and pass the Workshop Step 35 Post-Test first.")
        template, prefix = 'program_sace/post_test/certificate_pdf.html', 'AIT-WS-'
    elif kind == 'reading':
        if not flow.course_complete(row) or not flow.step35_passed(row):
            abort(409, description="Mark all 18 videos examined and pass the separate Reading-course assessment first.")
        template, prefix = 'subject_reading/certificate.html', 'AIT-RD-'
    else:
        abort(404)
    state = flow.payload(row)
    cid = state.setdefault(kind + '_certificate_id', prefix + secrets.token_hex(6).upper())
    completed_at = state.setdefault(kind + '_completed_at', datetime.now(timezone.utc).isoformat())
    flow.save(row, state)
    if isinstance(completed_at, str):
        try:
            completed_at = datetime.fromisoformat(completed_at)
        except Exception:
            completed_at = datetime.utcnow()
    elif completed_at is None:
        completed_at = datetime.utcnow()
    completed_date = completed_at.strftime("%d %B %Y")
    return render_template(template, learner_name=current_user.name,
        completed_date=completed_date, certificate_id=cid, user_id=current_user.id,
        answers=flow.payload(flow.latest(row, 'step34')) if kind == 'workshop' else None,
        logo_path=get_logo_data_uri(), seal_path=get_seal_data_uri())


def certificate_pdf(row, kind):
    """Generate the eligible Auditor's real certificate using existing AIT generators."""
    from datetime import datetime, timezone
    if kind == 'workshop':
        if not flow.workshop_passed(row):
            abort(409, description="Complete Steps 32-34 and pass the Workshop Step 35 Post-Test first.")
        from .routes import _generate_sace_certificate_pdf
        generate = _generate_sace_certificate_pdf
        prefix = 'AIT-WS-'
    elif kind == 'reading':
        if not flow.course_complete(row) or not flow.step35_passed(row):
            abort(409, description="Mark all 18 videos examined and pass the separate Reading-course assessment first.")
        from app.subject_reading.routes import _generate_certificate_pdf
        generate = _generate_certificate_pdf
        prefix = 'AIT-RD-'
    else:
        abort(404)
    state = flow.payload(row)
    cid = state.setdefault(kind + '_certificate_id', prefix + secrets.token_hex(6).upper())
    completed_at = state.setdefault(kind + '_completed_at', datetime.now(timezone.utc).isoformat())
    flow.save(row, state)
    try:
        args = (cid, current_user.name, completed_at, current_user.id)
        pdf = generate(*args, flow.payload(flow.latest(row, 'step34'))) if kind == 'workshop' else generate(*args)
    except Exception:
        current_app.logger.exception('%s certificate generation failed', kind)
        pdf = None
    return cid, pdf


def send_workshop_certificate():
    row = flow.assignment(lock=True)
    cid, pdf = certificate_pdf(row, 'workshop')
    return deliver_certificate(row, 'workshop_certificate', cid, pdf)


@sace_bp.get('/sace/reading/certificate-evidence/<kind>')
def certificate_document(kind):
    row = flow.assignment(lock=True)
    cid, pdf = certificate_pdf(row, kind)
    if not pdf:
        flow.record(row, kind + '_certificate_failed', {'certificate_id': cid, 'reason': 'generation'})
        db.session.commit()
        abort(503, description="Certificate generation failed. No email was sent.")
    db.session.commit()
    response = make_response(pdf)
    response.headers['Content-Type'] = 'application/pdf'
    response.headers['Content-Disposition'] = 'inline; filename="' + cid + '.pdf"'
    response.headers['Cache-Control'] = 'private, no-store'
    return response


def deliver_certificate(row,slug,certificate_id,pdf):
    from app.subject_reading.routes import _email_certificate_pdf
    email=(current_user.email or '').strip()
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
        current_app.logger.exception('Certificate email delivery failed')
        row=flow.assignment(lock=True)
        flow.record(row,slug+'_failed',{'certificate_id':certificate_id,'reason':'delivery'})
        db.session.commit()
        abort(503,description="Email delivery failed. Your progress is saved; retry from the board.")
    row=flow.assignment(lock=True)
    flow.record(row,slug,{'certificate_id':certificate_id,'outcome':'accepted_by_mail_sender'})
    db.session.commit()
    return redirect(url_for('sace_bp.reading_hub'))


def require_map(row):
    if not flow.latest(row, 'map_complete'):
        abort(409, description="Complete the Activity Summary and controlled materials first.")


@sace_bp.route('/sace/reading/auditor-map', methods=['GET', 'POST'])
def auditor_map():
    row = flow.assignment(lock=True)
    if request.method == 'POST':
        flow.record(row, 'map_reviewed', {'evidence': 'Activity Summary understood'}, once=True)
        db.session.commit()
        return redirect(url_for('sace_bp.reading_hub'))
    return render_template('program_sace/endorsement_map.html')


@sace_bp.get('/sace/reading/course')
def reading_course():
    row = flow.assignment(lock=True)
    flow.record(row, 'reading_entered', once=True)
    lessons = flow.course_lessons()
    completed = {x['id'] for x in lessons if flow.latest(row, f"reading_lesson_{x['id']}_complete")}
    next_lesson = next((x['id'] for x in lessons if x['id'] not in completed), None)
    db.session.commit()
    return render_template('program_sace/endorsement_course.html', lessons=lessons,
                           completed=completed, next_lesson=next_lesson)


def course_lesson(row, lesson_id):
    lessons = flow.course_lessons()
    lesson = next((x for x in lessons if x['id'] == lesson_id), None)
    if not lesson:
        abort(404)
    if request.method == 'POST' and any(not flow.latest(row, f"reading_lesson_{x['id']}_complete") for x in lessons if x['order'] < lesson['order']):
        abort(409, description="Complete the preceding Reading videos first.")
    return lesson


@sace_bp.route('/sace/reading/course/<int:lesson_id>', methods=['GET', 'POST'])
def reading_lesson(lesson_id):
    row = flow.assignment(lock=True)
    lesson = course_lesson(row, lesson_id)
    if request.method == 'POST':
        if not flow.latest(row, f'reading_lesson_{lesson_id}_served') or (request.get_json(silent=True) or {}).get('examined') is not True:
            abort(409, description="Open the video before marking it examined.")
        flow.record(row, f'reading_lesson_{lesson_id}_complete', {'lesson_order': lesson['order'], 'evidence': 'Auditor confirmed video examination'}, once=True)
        if flow.course_complete(row):
            flow.record(row, 'reading_complete', once=True)
        db.session.commit()
        return jsonify(success=True, next=url_for('sace_bp.reading_course'))
    flow.record(row, f'reading_lesson_{lesson_id}_opened', {'lesson_order': lesson['order']}, once=True)
    db.session.commit()
    return render_template('program_sace/endorsement_lesson.html', lesson=lesson)


@sace_bp.get('/sace/reading/course/<int:lesson_id>/video')
def reading_video(lesson_id):
    from app.utils.reading_media import (
        ReadingMediaUnavailable, reading_video_url, verify_reading_video, reading_video_disk_path,
    )
    row = flow.assignment()
    lesson = course_lesson(row, lesson_id)
    try:
        video_url = reading_video_url(lesson['video_filename'])
        verify_reading_video(video_url)
    except ReadingMediaUnavailable:
        try:
            disk_path = reading_video_disk_path(current_app.static_folder, lesson['video_filename'])
        except ReadingMediaUnavailable:
            abort(503, description="This Reading video is unavailable. Please contact AIT.")
        video_url = None
    row = flow.assignment(lock=True)
    flow.record(row, f'reading_lesson_{lesson_id}_served', once=True)
    db.session.commit()
    if video_url:
        response = redirect(video_url, code=302)
    else:
        from flask import send_file
        response = send_file(disk_path, conditional=True)
    response.headers['Cache-Control'] = 'private, no-store'
    return response



def reading_mcq():
    # Provider course content only. Never SACE endorsement criteria.
    content = current_app.config.get('AIT_READING_STEP35')
    if not content:
        return None
    if not isinstance(content, dict):
        abort(503, description='Reading-course assessment configuration is invalid.')
    questions = content.get('questions', [])
    valid = (isinstance(content.get('version'), str) and bool(content['version'])
             and type(content.get('pass_percent')) is int and 0 < content['pass_percent'] <= 100
             and isinstance(questions, list) and bool(questions))
    if not valid or any(not isinstance(q, dict) or not isinstance(q.get('id'), str) or not q['id'] or q['id'] in ('csrf_token', 'version') or not isinstance(q.get('prompt'), str) or not q['prompt']
        or not isinstance(q.get('options'), dict) or len(q['options']) < 2
        or q.get('answer') not in q['options'] for q in questions):
        abort(503, description="Reading-course assessment configuration is incomplete.")
    if len({q['id'] for q in questions}) != len(questions):
        abort(503, description="Reading-course question identifiers must be unique.")
    return content


@sace_bp.route('/sace/reading/course/assessment', methods=['GET', 'POST'])
def reading_assessment():
    # AIT_READING_STEP35 is a legacy course configuration name, not workshop step numbering.
    row = flow.assignment(lock=True)
    if not flow.workshop_passed(row) or not flow.course_complete(row):
        abort(409, description="Complete the workshop and all 18 Reading videos before the Reading-course assessment.")
    flow.record(row, 'reading_complete', once=True)
    content = reading_mcq()
    if request.method == 'POST':
        if content is None:
            abort(409, description="Reading-course questions have not been supplied. No pass has been recorded.")
        if request.form.get('version') != content['version']:
            abort(409, description="Reading-course content changed. Reload before submitting.")
        answers = {q['id']: request.form.get(q['id']) for q in content['questions']}
        if any(answers[q['id']] not in q['options'] for q in content['questions']):
            abort(400, description="Answer every Reading course question.")
        score = 100 * sum(answers[q['id']] == q['answer'] for q in content['questions']) / len(content['questions'])
        flow.record(row, 'step35', {'version': content['version'], 'answers': answers, 'score': score,
                                  'pass_percent': content['pass_percent'], 'passed': score >= content['pass_percent']})
    result = flow.payload(flow.latest(row, 'step35'))
    db.session.commit()
    if flow.step35_passed(row):
        return redirect(url_for('sace_bp.reading_certificate'))
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
            abort(409, description="Mark all 18 videos examined and pass the Reading-course assessment first.")
        cid, pdf = certificate_pdf(row, 'reading')
        return deliver_certificate(row, 'reading_certificate', cid, pdf)
    certificate_html = certificate_presentation(row, 'reading') if eligible else None
    db.session.commit()
    return render_template('program_sace/endorsement_reading_certificate.html',
        eligible=eligible, course_complete=flow.course_complete(row), certificate_html=certificate_html)


@sace_bp.route('/sace/reading/finish-evaluation', methods=['GET', 'POST'])
def finish_evaluation():
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
        from datetime import datetime, timezone
        state['status'] = 'Completed'
        state['completed_at'] = datetime.now(timezone.utc).isoformat()
        flow.save(row, state)
        db.session.commit()  # Evidence, durable R notification, then closure: all commit or none do.
        return render_template('program_sace/evaluation_closed.html')
    return render_template('program_sace/evaluation_finish.html', ready=not missing, missing=missing)


def controller_rows():
    from .lifecycle import require_controller, AssignmentContext
    appointment = require_controller(lock=False)
    return (Interaction.query.join(AssignmentContext, AssignmentContext.invitation_event_id == Interaction.id)
        .filter(AssignmentContext.engagement_id == appointment.engagement_id)
        .order_by(Interaction.id.desc()).all())


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
    if not 1 <= slide <= 31:
        abort(404)
    path = Path(current_app.static_folder) / 'sace_slides' / f'{slide}.png'
    if not path.is_file():
        abort(404)
    return send_file(path)


@sace_bp.get('/sace/provisioning/lifecycle')
def reading_lifecycle():
    from . import lifecycle
    appointment = lifecycle.require_controller(lock=False)
    engagement = db.session.get(lifecycle.Engagement, appointment.engagement_id)
    appointments = lifecycle.Appointment.query.filter_by(engagement_id=engagement.id).order_by(lifecycle.Appointment.id).all()
    return render_template('program_sace/reading_lifecycle.html', engagement=engagement,
        appointments=appointments, current_appointment=appointment)


@sace_bp.post('/sace/provisioning/handover')
def reading_handover():
    from . import lifecycle
    row, token = lifecycle.issue_handover(request.form.get('email'))
    db.session.commit()
    return render_template('program_sace/reading_handover.html',
        handover_url=url_for('sace_bp.provisioning_map', handover=token, _external=True))


@sace_bp.post('/sace/provisioning/appointments/<int:appointment_id>/end')
def reading_end_appointment(appointment_id):
    from . import lifecycle
    lifecycle.end_appointment(appointment_id, request.form.get('status'), request.form.get('reason'))
    db.session.commit()
    return redirect(url_for('sace_bp.provisioning_map') if flow.is_controller() else url_for('auth_bp.bridge_dashboard'))


@sace_bp.post('/sace/provisioning/engagement/end')
def reading_end_engagement():
    from . import lifecycle
    lifecycle.end_engagement(request.form.get('status'), request.form.get('reason'))
    db.session.commit()
    return redirect(url_for('auth_bp.bridge_dashboard'))


@sace_bp.route('/sace/provisioning/engagement/complete', methods=['GET', 'POST'])
def reading_complete_engagement():
    from . import lifecycle
    appointment = lifecycle.require_controller()
    if request.method == 'POST':
        if request.form.get('confirm') == 'no':
            return redirect(url_for('sace_bp.reading_lifecycle'))
        if request.form.get('confirm') != 'yes':
            abort(400, description='Choose Yes or No.')
        lifecycle.request_completion()
        db.session.commit()
        return redirect(url_for('sace_bp.reading_lifecycle'))
    return render_template('program_sace/reading_completion_confirm.html')


@sace_bp.post('/sace/provisioning/engagement/cancel-completion')
def reading_cancel_completion():
    from . import lifecycle
    lifecycle.cancel_completion()
    db.session.commit()
    return redirect(url_for('sace_bp.reading_lifecycle'))
