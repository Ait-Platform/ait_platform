from flask import g, abort, flash, redirect, render_template, request, session, url_for, send_file
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
    response.headers["Referrer-Policy"] = "no-referrer"
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


@home_sace_bp.route("/provisioning", methods=["GET", "POST"])
def provision():
    if request.args.get("token"):
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
    row = s.provisioning()
    if request.method == "POST":
        accept_pledge("controller", row.id)
        if current_user.is_authenticated:
            s.complete_provisioning()
            continuation.clear()
            return redirect(url_for("home_sace_bp.control"))
        return redirect(url_for("home_sace_bp.authenticate"))
    if current_user.is_authenticated and session.get("sace_home_pledge_controller"):
        s.complete_provisioning()
        continuation.clear()
        return redirect(url_for("home_sace_bp.control"))
    return page("pledge.html", "HOME Controller IP Pledge", terms=s.PLEDGE_TEXT,
        email=row.email, signed=False, back=url_for("home_sace_bp.entry"))


def accept_pledge(role, context_id):
    signature = request.form.get("signature", "").strip()
    if request.form.get("accept") != "yes" or not signature or len(signature) > 255:
        abort(400, description="Enter your full name and accept the HOME pledge.")
    session["sace_home_pledge_" + role] = dict(context_id=context_id,
        version=s.PLEDGE_VERSION, signature=signature, accepted_at=now().isoformat())


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


@home_sace_bp.get("/control/documents")
@login_required
def provider_documents():
    s.require_controller()
    materials = [(kind, s.ITEMS.get(kind, ex.ITEMS[kind]), s.latest_version(kind))
        for kind in ex.ITEMS if kind in s.DOCUMENT_ITEMS]
    return page("provider_documents.html", "HOME provider evidence", materials=materials,
        back=url_for("home_sace_bp.control"))


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
        version, manifest = ex.pinned(row, bind=True)
        db.session.commit()
        stages = [dict(kind=kind, title=title, chapters=[dict(number=n,
            title=manifest['chapters'][n - 1]['title'], examined=bool(ex.evidence(row, f'{kind}:{n}', 'examined', version)))
            for n in content.STAGES[kind]]) for kind, title in ex.STAGES.items()]
        return page('journey.html', 'HOME Learning Journey', row=row, version=version, stages=stages,
            back=url_for('home_sace_bp.board', assignment_id=row.id))
    return page("experience.html", "HOME practical / learning experience", row=row,
        back=url_for("home_sace_bp.board", assignment_id=row.id))


@home_sace_bp.route("/assignments/<int:assignment_id>/materials/<kind>", methods=["GET", "POST"])
@login_required
def material(assignment_id, kind):
    row = s.assignment(assignment_id, lock=request.method == "POST", writable=request.method == "POST")
    if row.requirements_version == ex.REQUIREMENTS:
        if kind in {'assessment', 'certificate'}:
            return specimen(row, kind)
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
    return send_file(path, mimetype="application/pdf", as_attachment=False,
        download_name="HOME-" + str(version.id) + ".pdf")


@home_sace_bp.route('/assignments/<int:assignment_id>/experience/<stage>/<int:chapter_number>', methods=['GET', 'POST'])
@login_required
def examination_chapter(assignment_id, stage, chapter_number):
    row = s.assignment(assignment_id, lock=True, writable=request.method == 'POST')
    if stage not in content.STAGES or chapter_number not in content.STAGES[stage]:
        abort(404)
    version, manifest = ex.pinned(row, bind=True)
    item = f'{stage}:{chapter_number}'
    details = ex.chapter_details(manifest, chapter_number, stage)
    if request.method == 'POST':
        ex.confirm(row, item, request.form.get('version'), version, details)
        if ex.journey_complete(row, version) and not ex.evidence(row, 'experience', 'examined', version):
            s.record(row, 'experience', 'examined', {'version': version, 'coverage': 'all 40 stage/chapter examinations'})
        db.session.commit()
        return redirect(url_for('home_sace_bp.experience', assignment_id=row.id))
    ex.open_item(row, item, version, details)
    db.session.commit()
    chapter = manifest['chapters'][chapter_number - 1]
    html = ex.render_html(row, version, chapter['html'], manifest) if chapter['html'] else ''
    return page('examination_chapter.html', ex.STAGES[stage] + ' — ' + chapter['title'],
        row=row, chapter=chapter, stage=stage, version=version, educational_html=html,
        back=url_for('home_sace_bp.experience', assignment_id=row.id))


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
        s.record(row, "completion", "completed", {"requirements": row.requirements_version})
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
