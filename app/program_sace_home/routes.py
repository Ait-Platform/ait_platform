import secrets
from flask_wtf.csrf import CSRFError
from werkzeug.exceptions import BadRequest
from flask import current_app, g, abort, flash, redirect, render_template, request, session, url_for, send_file
from flask_login import current_user, login_required
from app.extensions import db
from app.models.sace_home import (HomeAssignment, HomeInvitation, HomePledge, HomeDocumentVersion, HomeEvidence, now)
from . import home_sace_bp
from . import service as s
from . import continuation
from . import lifecycle as lc
from . import examination as ex
from . import content_snapshot as content


def page(template, title, **values):
    return render_template("program_sace_home/" + template, title=title,
        hide_navbar=True, **values)


@home_sace_bp.after_request
def private_response(response):
    response.headers["Cache-Control"] = "private, no-store"
    response.headers["Referrer-Policy"] = "same-origin"
    response.headers["X-Content-Type-Options"] = "nosniff"
    access = getattr(g, "home_access", None)
    if access and response.status_code < 400 and current_user.is_authenticated:
        role, engagement_id, assignment_id = access
        lc.audit(current_user.id, role, "access", engagement_id, assignment_id,
            {"endpoint": request.endpoint, "method": request.method})
        db.session.commit()
    return response


@home_sace_bp.get("/")
@login_required
def entry():
    if s.controller():
        continuation.clear()
        return redirect(url_for("home_sace_bp.control"))
    rows = lc.assignments()
    if len(rows) == 1:
        return redirect(url_for("home_sace_bp.board", assignment_id=rows[0].id))
    return page("assignments.html", "HOME endorsement assignments", rows=rows)


@home_sace_bp.errorhandler(BadRequest)
def provisioning_bad_request(error):
    # Preserve Flask/Werkzeug's existing response; never include request values.
    if request.endpoint == "home_sace_bp.provision" and request.method == "POST":
        if isinstance(error, CSRFError):
            reasons = {
                "The CSRF token is missing.": "token missing",
                "The CSRF session token is missing.": "session token missing",
                "The CSRF token has expired.": "token expired",
                "The CSRF token is invalid.": "token invalid",
                "The CSRF tokens do not match.": "token/session mismatch",
                "The referrer header is missing.": "referrer missing",
                "The referrer does not match the host.": "referrer/host mismatch",
            }
            current_app.logger.warning("HOME provisioning rejected: CSRF (%s)",
                reasons.get(error.description, "validation failed"))
        elif error.description == "Review and sign the HOME IP pledge first.":
            current_app.logger.warning("HOME provisioning rejected: pledge validation")
        else:
            current_app.logger.warning("HOME provisioning rejected: bad request (route_entered=%s)",
                bool(getattr(g, "home_provisioning_post_entered", False)))
    return error


@home_sace_bp.route("/provisioning", methods=["GET", "POST"])
def provision():
    if request.method == "POST":
        g.home_provisioning_post_entered = True
        current_app.logger.info("HOME provisioning POST entered")
    if request.args.get("token"):
        session.pop(s.PROVISIONING_CONTEXT, None)
        session.pop("sace_home_pending_code", None)
        session.pop("sace_home_pledge_auditor", None)
        session["sace_home_provisioning_token"] = request.args["token"]
        session.pop("sace_home_pledge_controller", None)
        s.provisioning()  # Validate the entry before establishing current HOME intent.
        continuation.begin("provisioning")
        return redirect(url_for("home_sace_bp.provision"))
    if s.controller():
        continuation.clear()
        return redirect(url_for("home_sace_bp.control"))
    if request.method == 'GET' and not session.get('sace_home_provisioning_token'):
        if request.args.get('journey'):
            abort(403, description="Begin HOME provisioning in this browser first.")
        nonce = s.start_provisioning()
        continuation.begin('provisioning')
        return redirect(url_for('home_sace_bp.provision', journey=nonce))
    row = s.provisioning()
    if getattr(row, 'session_bootstrap', False):
        nonce = request.form.get('journey') if request.method == 'POST' else request.args.get('journey')
        if request.method == 'POST' or nonce:
            if not nonce or not secrets.compare_digest(str(nonce), row.id):
                if request.method == "POST":
                    current_app.logger.warning("HOME provisioning rejected: journey/session mismatch")
                abort(403, description="HOME provisioning nonce is invalid. Reload your current journey.")
    if request.method == "POST":
        accept_pledge("controller", row.id)
        if current_user.is_authenticated:
            s.complete_provisioning()
            continuation.clear()
            return redirect(url_for("home_sace_bp.control"))
        current_app.logger.info("HOME provisioning POST accepted: authentication redirect")
        return redirect(url_for("home_sace_bp.authenticate"))
    if current_user.is_authenticated and session.get("sace_home_pledge_controller"):
        s.complete_provisioning()
        continuation.clear()
        return redirect(url_for("home_sace_bp.control"))
    return page("pledge.html", "HOME Controller IP Pledge", terms=s.PLEDGE_TEXT,
        controller_pledge=True, signed=False, journey=row.id if getattr(row, 'session_bootstrap', False) else None, back=url_for("home_sace_bp.entry"))


def accept_pledge(role, context_id):
    if role == "controller":
        # The validated, CSRF-protected pledge POST is affirmative consent.
        # Bind the durable record to the authenticated identity at claim time.
        session["sace_home_pledge_controller"] = dict(context_id=context_id,
            version=s.PLEDGE_VERSION, signature="Accepted by controller",
            acceptance_method="accept_and_continue", accepted_at=now().isoformat())
        return
    if request.form.get("accept") != "yes":
        abort(400, description="Accept the HOME pledge to continue.")
    session["sace_home_pledge_" + role] = dict(context_id=context_id,
        version=s.PLEDGE_VERSION, signature="Accepted by evaluator",
        acceptance_method="accept_and_continue", accepted_at=now().isoformat())


@home_sace_bp.get("/authenticate")
def authenticate():
    # Only display authentication continuation after a valid signed HOME entry.
    if session.get("sace_home_pending_code"):
        row = s.invitation()
        s.consent("auditor", row.id)
    else:
        row = s.provisioning()
        s.consent("controller", row.id)
    if current_user.is_authenticated:
        return redirect(url_for(s.auth_destination()))
    return page("authenticate.html", "Continue HOME endorsement", subject=s.SUBJECT,
        resume=url_for(s.auth_destination()), back=url_for("home_sace_bp.entry"))


@home_sace_bp.get("/control")
@login_required
def control():
    owner = s.require_controller()
    actor = lc.require_appointment()
    invitations = HomeInvitation.query.filter_by(appointment_id=actor.id).order_by(HomeInvitation.id.desc()).all()
    assignments = (HomeAssignment.query.join(HomeInvitation)
        .filter(HomeInvitation.appointment_id == actor.id).order_by(HomeAssignment.id.desc()).all())
    return page("control.html", "HOME Control Centre", invitations=invitations,
        assignments=assignments, engagement=db.session.get(lc.Engagement, actor.engagement_id), back=url_for("home_sace_bp.entry"))


@home_sace_bp.post("/control/codes")
@login_required
def generate_code():
    code = s.issue_invitation(s.require_controller())
    db.session.commit()
    return page("code.html", "HOME Auditor access code", code=code,
        back=url_for("home_sace_bp.control"))


PROVIDER_DOCUMENTS = (
    ('application_form_1', 'Application Form 1', 'The primary SACE application form.'),
    ('application_form_2', 'Application Form 2', 'The secondary SACE application form.'),
    ('facilitator_compliance', 'Facilitator CVs & Compliance',
     'This is the facilitator compliance portfolio/evidence supplied for the endorsement, '
     'including the applicable facilitator CVs and compliance documentation.'),
)


@home_sace_bp.get("/control/documents")
@login_required
def provider_documents():
    s.require_controller()
    materials = [dict(kind=kind, title=title, description=description,
        version=s.latest_version(kind)) for kind, title, description in PROVIDER_DOCUMENTS]
    from werkzeug.exceptions import NotFound, Conflict
    for document in materials:
        if document['version'] is not None:
            try:
                s.document_path(document['version'])
            except (NotFound, Conflict):
                document['version'] = None
    return page("provider_documents.html", "HOME provider evidence", materials=materials,
        back=url_for("home_sace_bp.control"))


@home_sace_bp.post('/control/documents/<int:version_id>/email')
@login_required
def email_provider_document(version_id):
    from app.models.sace_home import HomeDocument
    from app.utils.mailer import send_pdf_email
    from email_validator import validate_email, EmailNotValidError
    s.require_controller()
    version = db.session.get(HomeDocumentVersion, version_id)
    document = db.session.get(HomeDocument, version.document_id) if version else None
    if document is None or document.kind not in {item[0] for item in PROVIDER_DOCUMENTS}:
        abort(404)
    path = s.document_path(version)
    try:
        recipient = validate_email(request.form.get('recipient_email', ''),
            check_deliverability=False).normalized
    except EmailNotValidError:
        flash('Enter a valid recipient email address.', 'warning')
        return redirect(url_for('home_sace_bp.provider_documents'))
    title = next(item[1] for item in PROVIDER_DOCUMENTS if item[0] == document.kind)
    send_pdf_email(recipient, 'HOME Provider Document: ' + title,
        'Please find the requested HOME provider document attached.', path.read_bytes(),
        'HOME-' + document.kind + '.pdf')
    flash('HOME provider document emailed.', 'success')
    return redirect(url_for('home_sace_bp.provider_documents'))


@home_sace_bp.get("/control/assignments/<int:assignment_id>")
@login_required
def controller_evidence(assignment_id):
    owner = s.require_controller()
    actor = lc.require_appointment()
    row = (HomeAssignment.query.join(HomeInvitation)
        .filter(HomeAssignment.id == assignment_id, HomeInvitation.appointment_id == actor.id).first_or_404())
    events = HomeEvidence.query.filter_by(assignment_id=row.id).order_by(HomeEvidence.id).all()
    return page("audit.html", "HOME examination evidence", row=row, events=events,
        back=url_for("home_sace_bp.control"))


@home_sace_bp.route("/join", methods=["GET", "POST"])
def join():
    if request.method == "POST":
        code = request.form.get("code", "").strip().upper()
        s.invitation(code)
        session.pop("sace_home_provisioning_token", None)
        session.pop(s.PROVISIONING_CONTEXT, None)
        session.pop("sace_home_pledge_controller", None)
        session["sace_home_pending_code"] = code
        session.pop("sace_home_pledge_auditor", None)
        continuation.begin("join")
        return redirect(url_for("home_sace_bp.pledge"))
    return page("join.html", "HOME Auditor join", back=url_for("home_sace_bp.entry"))


@home_sace_bp.route("/pledge", methods=["GET", "POST"])
def pledge():
    row = s.invitation()
    if request.method == "POST":
        accept_pledge("auditor", row.id)
        endpoint = "home_sace_bp.claim_assignment" if current_user.is_authenticated else "home_sace_bp.authenticate"
        return redirect(url_for(endpoint))
    return page("pledge.html", "HOME Evaluator IP Pledge", terms=s.PLEDGE_TEXT,
        signed=False, back=url_for("home_sace_bp.join"))


@home_sace_bp.get("/claim")
@login_required
def claim_assignment():
    # The pledge POST establishes consent; this also resumes after platform authentication.
    row = s.claim()
    continuation.clear()
    return redirect(url_for("home_sace_bp.board", assignment_id=row.id))


@home_sace_bp.get("/ip-pledge")
@login_required
def signed_pledge():
    if s.controller():
        s.require_controller()
    else:
        assignments = lc.assignments()
        if not assignments:
            abort(403)
        s.assignment(assignments[0].id)
    rows = HomePledge.query.filter_by(user_id=current_user.id).order_by(HomePledge.id.desc()).all()
    if not rows:
        abort(403)
    return page("pledge.html", "HOME Intellectual Property Pledge", terms=s.PLEDGE_TEXT,
        signed=True, pledges=rows, back=url_for("home_sace_bp.entry"))


@home_sace_bp.get("/assignments/<int:assignment_id>/board")
@login_required
def board(assignment_id):
    row = s.assignment(assignment_id)
    return page("board.html", "HOME Auditor Board", row=row, items=s.board_items(row),
        back=url_for("home_sace_bp.entry"))


@home_sace_bp.route("/assignments/<int:assignment_id>/summary", methods=["GET", "POST"])
@login_required
def summary(assignment_id):
    row = s.assignment(assignment_id, lock=request.method == "POST", writable=request.method == "POST")
    if request.method == "POST":
        if not s.examined(row, "summary"):
            s.record(row, "summary", "examined", {"version": "home-summary-v1"})
        db.session.commit()
        return redirect(url_for("home_sace_bp.board", assignment_id=row.id))
    return page("summary.html", "HOME Activity Summary", row=row,
        back=url_for("home_sace_bp.board", assignment_id=row.id))


@home_sace_bp.get("/assignments/<int:assignment_id>/experience")
@login_required
def experience(assignment_id):
    row = s.assignment(assignment_id, lock=True)
    if row.requirements_version == ex.REQUIREMENTS:
        return redirect(url_for('home_bp.learner_dashboard', home_assignment_id=row.id))
    return page("experience.html", "HOME practical / learning experience", row=row,
        back=url_for("home_sace_bp.board", assignment_id=row.id))


@home_sace_bp.route("/assignments/<int:assignment_id>/materials/<kind>", methods=["GET", "POST"])
@login_required
def material(assignment_id, kind):
    row = s.assignment(assignment_id, lock=request.method == "POST", writable=request.method == "POST")
    if row.requirements_version == ex.REQUIREMENTS:
        if kind == 'certificate':
            from . import participant_context as context
            result = context.assessment(row)
            if result is None:
                return redirect(url_for('home_sace_bp.experience', assignment_id=row.id), code=303)
            if request.method == 'POST':
                abort(409, description='Email the genuine Item 7 certificate; specimen confirmation cannot complete evidence.')
            return redirect(url_for('home_bp.report_exit', home_assignment_id=row.id, assessment_id=result.id,
                type='certificate' if result.passed else 'report'))
        if kind in {'assessment', 'monitoring'}:
            abort(410, description='Use the separate HOME workshop evaluation items on the Auditor Board.')
        if kind in ex.INSTRUMENTS:
            return redirect(url_for('home_sace_bp.workshop_response', assignment_id=row.id, kind=kind), code=303)
        if kind not in ex.DOCUMENTS:
            abort(404)
        # Bind on first opening; later publication cannot change this examination.
        row = s.assignment(assignment_id, lock=True)
        version = ex.document(row, kind, bind=True)
        if request.method == 'POST':
            if version is None or request.form.get('version_id') != str(version.id):
                abort(409, description='Approved HOME document version is unavailable or mismatched.')
            opened = HomeEvidence.query.filter_by(assignment_id=row.id, item=kind,
                event='opened', document_version_id=version.id).first()
            if opened is None:
                abort(409, description='Open this exact HOME document first.')
            if not s.examined(row, kind, version.id):
                s.record(row, kind, 'examined', {'sha256': version.sha256}, version.id)
            db.session.commit()
            return redirect(url_for('home_sace_bp.board', assignment_id=row.id))
        db.session.commit()
        return page('material.html', ex.ITEMS[kind], row=row, version=version, kind=kind,
            back=url_for('home_sace_bp.board', assignment_id=row.id))
    if kind not in s.DOCUMENT_ITEMS:
        abort(404)
    version = s.latest_version(kind)
    if request.method == "POST":
        if version is None or request.form.get("version_id") != str(version.id):
            abort(409, description="Open the current HOME document version before confirming examination.")
        s.document_path(version)
        opened = HomeEvidence.query.filter_by(assignment_id=row.id, item=kind,
            event="opened", document_version_id=version.id).first()
        if opened is None:
            abort(409, description="Open the document before confirming examination.")
        if not s.examined(row, kind, version.id):
            s.record(row, kind, "examined", version=version.id)
        db.session.commit()
        return redirect(url_for("home_sace_bp.board", assignment_id=row.id))
    return page("material.html", s.ITEMS[kind], row=row, version=version, kind=kind,
        back=url_for("home_sace_bp.board", assignment_id=row.id))


@home_sace_bp.get("/documents/<int:version_id>/content")
@login_required
def document_content(version_id):
    version = db.session.get(HomeDocumentVersion, version_id)
    if version is None:
        abort(404)
    assignment_id = request.args.get("assignment_id", type=int)
    if assignment_id is not None:
        row = s.assignment(assignment_id, lock=True)
        from app.models.sace_home import HomeDocument
        doc = db.session.get(HomeDocument, version.document_id)
        if row.requirements_version == ex.REQUIREMENTS:
            bound = ex.document(row, doc.kind) if doc.kind in ex.DOCUMENTS else None
            if bound is None or bound.id != version.id:
                abort(409, description='This document is not the approved HOME examination version.')
        path = s.document_path(version)
        if row.status == "active":
            s.record(row, doc.kind, "opened", version=version.id)
            db.session.commit()
    else:
        s.require_controller()
        path = s.document_path(version)
    download = (version.source_manifest.get('kind') in {'application_form_1', 'application_form_2', 'timetable'}
        and request.args.get('download') == '1')
    return send_file(path, mimetype="application/pdf", as_attachment=download,
        download_name="HOME-" + str(version.id) + ".pdf")


@home_sace_bp.route('/assignments/<int:assignment_id>/experience/<stage>/<int:chapter_number>', methods=['GET', 'POST'])
@login_required
def examination_chapter(assignment_id, stage, chapter_number):
    row = s.assignment(assignment_id, lock=True, writable=request.method == 'POST')
    if stage not in content.STAGES or chapter_number not in content.STAGES[stage]:
        abort(404)
    abort(410, description='Use Item 7 to examine the original HOME participant journey.')


@home_sace_bp.get('/assignments/<int:assignment_id>/content-assets/<version>/<asset>')
@login_required
def examination_asset(assignment_id, version, asset):
    import base64
    import io
    row = s.assignment(assignment_id)
    pinned_version, manifest = ex.pinned(row)
    if manifest is None or version != pinned_version or asset not in manifest['assets']:
        abort(404)
    image = manifest['assets'][asset]
    return send_file(io.BytesIO(base64.b64decode(image['data'])), mimetype=image['mime'])


def specimen(row, kind):
    row = s.assignment(row.id, lock=True, writable=request.method == 'POST')
    version, manifest = ex.pinned(row, bind=True)
    details = {'content_hash': content.digest(manifest['assessment'] if kind == 'assessment' else manifest['certificate_specimens'])}
    if kind == 'assessment':
        details['question_ids'] = [q['id'] for c in manifest['assessment'] for q in c['questions']]
    if request.method == 'POST':
        ex.confirm(row, kind, request.form.get('version'), version, details)
        db.session.commit()
        return redirect(url_for('home_sace_bp.board', assignment_id=row.id))
    ex.open_item(row, kind, version, details)
    db.session.commit()
    return page('specimen.html', ex.ITEMS[kind], row=row, kind=kind, version=version,
        manifest=manifest, back=url_for('home_sace_bp.board', assignment_id=row.id))


@home_sace_bp.route("/assignments/<int:assignment_id>/completion", methods=["GET", "POST"])
@login_required
def completion(assignment_id):
    row = s.assignment(assignment_id, lock=request.method == "POST", writable=request.method == "POST")
    missing = s.missing(row)
    if request.method == "POST":
        if missing:
            abort(409, description="HOME examination is incomplete; unavailable evidence cannot be marked examined.")
        row.status, row.completed_at = "completed", now()
        details = {"requirements": row.requirements_version}
        if row.requirements_version == ex.REQUIREMENTS:
            from .participant_context import VERSION
            details['examination'] = VERSION
        s.record(row, "completion", "completed", details)
        lc.audit(current_user.id, "auditor", "examination_completed", g.home_access[1], row.id)
        db.session.commit()
        return page("completion.html", "HOME examination completed", row=row, missing=[],
            back=url_for("home_sace_bp.entry"))
    return page("completion.html", "HOME examination completion", row=row, missing=missing,
        back=url_for("home_sace_bp.board", assignment_id=row.id))


@home_sace_bp.route("/control/completion", methods=["GET", "POST"])
@login_required
def request_completion():
    lc.require_appointment()
    if request.method == "POST":
        decision = request.form.get("decision")
        if decision == "yes":
            lc.request_completion()
            db.session.commit()
        elif decision != "no":
            abort(400, description="Choose Yes or No.")
        return redirect(url_for("home_sace_bp.control"))
    return page("completion_confirm.html", "Complete Activity Endorsement",
        back=url_for("home_sace_bp.control"))


@home_sace_bp.post("/control/cancel-completion")
@login_required
def cancel_completion():
    lc.cancel_completion()
    db.session.commit()
    return redirect(url_for("home_sace_bp.control"))


@home_sace_bp.route('/assignments/<int:assignment_id>/responses/<kind>', methods=['GET', 'POST'])
@login_required
def workshop_response(assignment_id, kind):
    from . import participant_context as context, workshop_interactions as instrument
    row = s.assignment(assignment_id, lock=True, writable=request.method == 'POST')
    if row.requirements_version != ex.REQUIREMENTS or kind not in ex.INSTRUMENTS:
        abort(404)
    questions = (instrument.FACILITATOR_QUESTIONS if kind == 'facilitator_evaluation'
        else instrument.PARTICIPANT_QUESTIONS if kind == 'participant_evaluation' else ())
    saved = context.latest(row, kind, 'submitted')
    if request.method == 'POST':
        details = {'instrument': ex.INSTRUMENTS[kind], 'programme': 'home',
            'assignment_id': row.id, 'responding_user_id': current_user.id}
        if kind == 'longitudinal_survey':
            intention = request.form.get('intention_to_use')
            if intention not in ('Yes', 'No') or len(request.form.getlist('intention_to_use')) != 1:
                abort(400, description='Select Yes or No before continuing.')
            details['intention_to_use'] = intention
        else:
            answers = {key: request.form.get(key) for key, _ in questions}
            if not instrument.answers_valid(answers, questions) or any(len(request.form.getlist(key)) != 1 for key, _ in questions):
                abort(400, description='Answer every question using Yes, Unsure or No.')
            details['answers'] = answers
            if kind == 'facilitator_evaluation':
                comment = request.form.get('comment', '')
                if len(comment) > 2000:
                    abort(400, description='The optional comment must be at most 2000 characters.')
                details['comment'] = comment.strip()
        context.record(row, kind, 'submitted', details)
        db.session.commit()
        return redirect(url_for('home_sace_bp.board', assignment_id=row.id))
    return page('workshop_response.html', ex.FUNCTIONAL_ITEMS[kind], row=row, kind=kind,
        questions=questions, choices=instrument.CHOICES, survey_question=instrument.SURVEY_QUESTION,
        saved=saved.details if saved else {}, back=url_for('home_sace_bp.board', assignment_id=row.id))
