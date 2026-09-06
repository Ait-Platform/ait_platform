import os
from flask import render_template, request, redirect, url_for, flash, current_app, jsonify, send_from_directory, session, abort
from flask_login import login_required, current_user
from app.extensions import db
from . import sace_bp
from app.models.sace import SaceWorkshopInteraction

def get_workshop_state(key, default_val):
    interaction = SaceWorkshopInteraction.query.filter_by(workshop_session_id='demo-session-1', activity_slug=key).first()
    return interaction.response_data if interaction else default_val

def set_workshop_state(key, value):
    interaction = SaceWorkshopInteraction.query.filter_by(workshop_session_id='demo-session-1', activity_slug=key).first()
    if not interaction:
        interaction = SaceWorkshopInteraction(user_id=current_user.id, workshop_session_id='demo-session-1', activity_slug=key, response_data=str(value))
        db.session.add(interaction)
    else:
        interaction.response_data = str(value)
    db.session.commit()

@sace_bp.route('/sace/workshop/get_state')
@login_required
def get_state():
    roster_rows = SaceWorkshopInteraction.query.filter_by(workshop_session_id='demo-session-1', activity_slug='participant_joined').all()
    roster = [r.response_data for r in roster_rows]
    
    # Calculate poll aggregates
    poll_data = {
        'crisis': {'true': 0, 'false': 0},
        'root_cause': {'resources': 0, 'class_size': 0, 'language': 0, 'methods': 0},
    }
    
    # Pre-test (Slide 0)
    crisis_votes = SaceWorkshopInteraction.query.filter_by(workshop_session_id='demo-session-1', activity_slug='poll_crisis').all()
    for v in crisis_votes:
        if v.response_data == 'TRUE': poll_data['crisis']['true'] += 1
        elif v.response_data == 'FALSE': poll_data['crisis']['false'] += 1
        
    # Root Cause (Slide 3)
    cause_votes = SaceWorkshopInteraction.query.filter_by(workshop_session_id='demo-session-1', activity_slug='poll_root_cause').all()
    for v in cause_votes:
        if v.response_data == 'A': poll_data['root_cause']['resources'] += 1
        elif v.response_data == 'B': poll_data['root_cause']['class_size'] += 1
        elif v.response_data == 'C': poll_data['root_cause']['language'] += 1
        elif v.response_data == 'D': poll_data['root_cause']['methods'] += 1

    return jsonify({
        "status": get_workshop_state('session_state', 'lobby'),
        "slide": int(get_workshop_state('current_slide', '0')),
        "attendance": len(roster),
        "roster": roster,
        "poll_data": poll_data
    })

@sace_bp.route('/sace/workshop/join', methods=['POST'])
@login_required
def join_room():
    existing = SaceWorkshopInteraction.query.filter_by(workshop_session_id='demo-session-1', activity_slug='participant_joined', user_id=current_user.id).first()
    if not existing:
        name = getattr(current_user, 'name', None) or current_user.email
        new_join = SaceWorkshopInteraction(user_id=current_user.id, workshop_session_id='demo-session-1', activity_slug='participant_joined', response_data=name)
        db.session.add(new_join)
        db.session.commit()
    return jsonify({"success": True})




@sace_bp.route('/sace/workshop/start', methods=['POST'])
@login_required
def start_workshop():
    set_workshop_state('session_state', 'active')
    set_workshop_state('current_slide', '0')
    return jsonify({"success": True})



@sace_bp.route('/sace/workshop/get_slide')
@login_required
def get_slide():
    # Keep for backwards compatibility if needed, but get_state is better
    return jsonify({"slide": int(get_workshop_state('current_slide', '0'))})

@sace_bp.route('/sace/workshop/set_slide', methods=['POST'])
@login_required
def set_slide():
    data = request.get_json()
    if 'slide' in data:
        set_workshop_state('current_slide', str(data['slide']))
        return jsonify({"success": True, "slide": int(data['slide'])})
    return jsonify({"error": "No slide provided"}), 400

@sace_bp.route("/sace/about")
def sace_about():
    return render_template("program_sace/about.html")

@sace_bp.route("/sace/dashboard")
@login_required
def dashboard():
    return render_template("program_sace/compliance/index.html")

@sace_bp.route("/sace/compliance/annexure_a")
@login_required
def annexure_a():
    from app.models.sace import SaceDocument
    tt_doc = SaceDocument.query.filter_by(slug='reading', document_type='timetable').first()
    return render_template("program_sace/compliance/annexure_a.html", tt_doc=tt_doc)

@sace_bp.route("/sace/compliance/annexure_b")
@login_required
def annexure_b():
    return render_template("program_sace/compliance/annexure_b.html")

@sace_bp.route("/sace/compliance/annexure_c")
@login_required
def annexure_c():
    return render_template("program_sace/compliance/annexure_c.html")

@sace_bp.route("/sace/compliance/annexure_d")
@login_required
def annexure_d():
    return render_template("program_sace/compliance/annexure_d.html")

@sace_bp.route("/sace/compliance/annexure_e")
@login_required
def annexure_e():
    return render_template("program_sace/compliance/annexure_e.html")

@sace_bp.route("/sace/compliance/evidence")
@login_required
def compliance_evidence():
    import datetime
    today = datetime.datetime.utcnow().strftime('%Y-%m-%d')
    return render_template("program_sace/compliance/evidence.html", today=today)
from flask import send_from_directory
import os

@sace_bp.route('/sace/reset_progress', methods=['POST'])
@login_required
def reset_evaluator_progress():
    from app.models.sace import SaceWorkshopInteraction
    from app.extensions import db
    from sqlalchemy import text as sa_text
    
    # Delete SACE Hub Interactions
    SaceWorkshopInteraction.query.filter_by(user_id=current_user.id).delete()
    
    # Delete Reading Course Progress
    db.session.execute(sa_text("DELETE FROM rdp_lesson_progress WHERE user_id = :uid"), {"uid": current_user.id})
    db.session.execute(sa_text("DELETE FROM rdp_enrollment WHERE user_id = :uid"), {"uid": current_user.id})
    
    db.session.commit()
    flash("Testing Mode: Progress has been completely reset. You are starting from the beginning.", "success")
    return redirect(url_for('sace_bp.reading_hub'))

@sace_bp.route("/sace/reading")
@login_required
def reading_hub():
    from app.models.sace import SaceDocument, SaceWorkshopInteraction
    
    app_form = SaceDocument.query.filter_by(slug='reading', document_type='app_form').first()
    
    # Fetch user's interactions to build the progress map
    interactions = SaceWorkshopInteraction.query.filter_by(user_id=current_user.id).all()
    completed_slugs = [i.activity_slug for i in interactions]
    
    # Also check reading module progress via raw SQL (since it lacks an ORM model)
    from sqlalchemy import text as sa_text
    from app.extensions import db
    reading_enr = db.session.execute(
        sa_text("SELECT progress_percent, certificate_id FROM rdp_enrollment WHERE user_id = :uid LIMIT 1"),
        {"uid": current_user.id}
    ).fetchone()
    reading_completed = reading_enr is not None and reading_enr.progress_percent == 100 and reading_enr.certificate_id is not None
    
    progress = {
        'app_form': 'viewed_app_form' in completed_slugs,
                'patent': 'viewed_patent' in completed_slugs,
        'annexures': 'viewed_annexures' in completed_slugs,
        'ppp': 'viewed_ppp' in completed_slugs,
        'demo_cert': 'workshop_post_test' in completed_slugs,
        'reading_cert': reading_completed
    }
    
    return render_template(
        "program_sace/reading_hub.html", 
        app_form=app_form,
        progress=progress
    )

@sace_bp.route("/sace/reading/workshop")
@login_required
def reading_workshop_docs():
    from app.models.sace import SaceDocument
    docs = SaceDocument.query.filter_by(slug='reading').all()
    doc_dict = {d.document_type: d for d in docs}
    return render_template("program_sace/workshop_documents.html", doc_dict=doc_dict)

@sace_bp.route("/sace/reading/interactive_workshop")
@login_required
def interactive_workshop():
    return render_template("program_sace/interactive_workshop.html")

@sace_bp.route("/sace/download/<int:doc_id>")
@login_required
def download_document(doc_id):
    from app.models.sace import SaceDocument
    doc = SaceDocument.query.get_or_404(doc_id)
    # file_path is like 'uploads/sace/filename.pdf'
    # we need to send it from static folder
    directory = os.path.join(current_app.static_folder)
    return send_from_directory(directory, doc.file_path)

@sace_bp.route('/sace/workshop/submit_interaction', methods=['POST'])
@login_required
def submit_interaction():
    from app.models.sace import SaceWorkshopInteraction
    from app.extensions import db
    data = request.get_json()
    activity_slug = data.get('activity_slug')
    response_data = data.get('response_data')
    workshop_session_id = data.get('workshop_session_id', 'demo-session-1')
    if not activity_slug or response_data is None:
        return jsonify({'error': 'Missing data'}), 400
    interaction = SaceWorkshopInteraction(user_id=current_user.id, workshop_session_id=workshop_session_id, activity_slug=activity_slug, response_data=str(response_data))
    db.session.add(interaction)
    db.session.commit()
    return jsonify({'success': True, 'message': 'Interaction recorded successfully.'})

@sace_bp.route('/sace/workshop/facilitator/live_stats')
@login_required
def live_stats():
    from app.models.sace import SaceWorkshopInteraction
    from sqlalchemy import func
    workshop_session_id = request.args.get('session_id', 'demo-session-1')
    activity_slug = request.args.get('activity_slug', 'module-1-pretest')
    results = db.session.query(SaceWorkshopInteraction.response_data, func.count(SaceWorkshopInteraction.id)).filter_by(workshop_session_id=workshop_session_id, activity_slug=activity_slug).group_by(SaceWorkshopInteraction.response_data).all()
    data = {row[0]: row[1] for row in results}
    total = sum(data.values())
    return jsonify({'total': total, 'breakdown': data})

@sace_bp.route('/sace/workshop/facilitator')
@login_required
def facilitator_dashboard():
    return render_template('program_sace/facilitator_dashboard.html')







@sace_bp.route('/sace/evaluator/report')
@login_required
def evaluator_report():
    interactions = SaceWorkshopInteraction.query.filter_by(
        user_id=current_user.id,
        workshop_session_id='demo-session-1'
    ).order_by(SaceWorkshopInteraction.timestamp).all()
    
    return render_template('program_sace/evaluator_report.html', interactions=interactions)


@sace_bp.route('/sace/workshop/submit_poll', methods=['POST'])
@login_required
def submit_poll():
    data = request.json
    slug = data.get('poll_id')
    
    interaction = SaceWorkshopInteraction.query.filter_by(
        user_id=current_user.id,
        workshop_session_id='demo-session-1',
        activity_slug=slug
    ).first()
    
    if interaction:
        interaction.response_data = data.get('data')
    else:
        interaction = SaceWorkshopInteraction(
            user_id=current_user.id,
            workshop_session_id='demo-session-1',
            activity_slug=slug,
            response_data=data.get('data')
        )
        db.session.add(interaction)
        
    db.session.commit()
    return jsonify({"success": True})


from flask import render_template

@sace_bp.route('/sace/participant/join')
@login_required
def participant_join():
    return render_template('program_sace/participant_join.html')


@sace_bp.route('/sace/workshop/reset', methods=['POST'])
@login_required
def reset_workshop():
    SaceWorkshopInteraction.query.filter_by(workshop_session_id='demo-session-1').delete(synchronize_session=False)
    db.session.commit()
    # Re-initialize the basic state so it doesn't break
    set_workshop_state('session_state', 'lobby')
    set_workshop_state('current_slide', '0')
    return jsonify({"success": True})


@sace_bp.route('/sace/evaluator/guide')
@login_required
def reviewer_guide():
    return render_template('program_sace/reviewer_guide.html')


@sace_bp.route('/sace/participant/<activity_slug>/onboarding', methods=['GET', 'POST'])
@login_required
def participant_onboarding(activity_slug):
    from flask_login import current_user
    from app.extensions import db
    from app.models.sace import SaceWorkshopInteraction
    
    # Check if they already entered their SACE number
    interaction = SaceWorkshopInteraction.query.filter_by(
        user_id=current_user.id, 
        activity_slug='sace_number'
    ).first()
    
    if interaction and request.method == 'GET':
        return redirect(url_for('sace_bp.interactive_workshop'))
        
    if request.method == 'POST':
        sace_num = request.form.get('sace_number', '').strip()
        if sace_num:
            new_interaction = SaceWorkshopInteraction(
                user_id=current_user.id,
                activity_slug='sace_number',
                response_data=sace_num
            )
            db.session.add(new_interaction)
            db.session.commit()
            return redirect(url_for('sace_bp.interactive_workshop'))
        else:
            flash("Please enter a valid SACE Registration Number.", "danger")
            
    return render_template('program_sace/onboarding.html', activity_slug=activity_slug)

@sace_bp.route('/sace/catalog')
def catalog():
    from flask_login import current_user
    from flask import redirect, url_for
    if getattr(current_user, 'is_authenticated', False):
        from flask import session
        is_sace_admin = any(s.startswith('sace') for s in session.get("admin_subjects", []))
        if is_sace_admin:
            return redirect(url_for('sace_bp.dashboard'))

    activities = [
        {
            "slug": "reading",
            "name": "I Learn to Read English Using the LITRE Method",
            "desc": "Interactive reading methodology for early childhood development.",
            "icon": "fa-book-open"
        }
    ]
    return render_template('program_sace/sace_catalog.html', activities=activities)

@sace_bp.route('/sace/hub/<activity_slug>')
@login_required
def selection_hub(activity_slug):
    return render_template('program_sace/sace_selection_hub.html', activity_slug=activity_slug)

from flask import make_response

@sace_bp.route("/sace/acknowledge_patent", methods=["POST"])
@login_required
def acknowledge_patent():
    from app.models.sace import SaceWorkshopInteraction
    interaction = SaceWorkshopInteraction.query.filter_by(user_id=current_user.id, activity_slug="viewed_patent").first()
    if not interaction:
        interaction = SaceWorkshopInteraction(
            user_id=current_user.id,
            activity_slug="viewed_patent",
            response_data="User acknowledged IP/Patent on hub."
        )
        db.session.add(interaction)
        db.session.commit()
        
        from app.models.core import CoreAuditEvent
        ip_addr = request.headers.get('X-Forwarded-For', request.remote_addr)
        audit = CoreAuditEvent(
            user_id=current_user.id,
            action="PLEDGE_ACCEPTED",
            entity_type="SACE_PLEDGE",
            details="Evaluator accepted IP pledge on map",
            ip_address=ip_addr
        )
        db.session.add(audit)
        db.session.commit()
        
        flash("Intellectual Property acknowledged.", "success")
    return redirect(url_for('sace_bp.reading_hub'))


@sace_bp.route("/sace/reading/presentation")
@login_required
def presentation():
    return render_template("program_sace/presentation_ppp.html")

@sace_bp.route("/sace/reading/presentation/complete")
@login_required
def presentation_complete():
    from app.models.sace import SaceWorkshopInteraction
    
    # Log that the user viewed the PPP
    interaction = SaceWorkshopInteraction.query.filter_by(user_id=current_user.id, activity_slug="viewed_ppp").first()
    if not interaction:
        interaction = SaceWorkshopInteraction(
            user_id=current_user.id,
            activity_slug="viewed_ppp",
            response_data="Linear presentation completed"
        )
        db.session.add(interaction)
        db.session.commit()
        
    flash("Linear Presentation completed successfully.", "success")
    return redirect(url_for('sace_bp.reading_hub'))


@sace_bp.route("/sace/reading/simulator")
@login_required
def simulator():
    from app.models.sace import SaceDocument
    docs = SaceDocument.query.filter_by(slug='reading').all()
    doc_dict = {d.document_type: d for d in docs}
    
    response = make_response(render_template("program_sace/simulator.html", docs=doc_dict))
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, post-check=0, pre-check=0, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '-1'
    return response

from app.models.core import CoreAuditEvent

@sace_bp.route("/sace/log_event", methods=["POST"])
@login_required
def log_event():
    data = request.get_json()
    action = data.get("action", "UNKNOWN_ACTION")
    details = data.get("details", "")
    
    # Get IP Address (handling proxies)
    ip_addr = request.headers.get('X-Forwarded-For', request.remote_addr)
    
    event = CoreAuditEvent(
        user_id=current_user.id,
        action=action,
        entity_type="SACE_SIMULATOR",
        details=details,
        ip_address=ip_addr
    )
    db.session.add(event)
    db.session.commit()
    
    return jsonify({"success": True})


@sace_bp.route("/sace/provisioning")
def provisioning_map():
    from app.models.sace import SaceWorkshopInteraction
    import json
    
    # If they are logged in and have a session pledge, save it to DB now
    if current_user.is_authenticated and session.get('sace_admin_pledged'):
        existing = SaceWorkshopInteraction.query.filter_by(user_id=current_user.id, activity_slug="admin_patent_pledge").first()
        if not existing:
            interaction = SaceWorkshopInteraction(
                user_id=current_user.id,
                activity_slug="admin_patent_pledge",
                response_data="Admin accepted IP pledge"
            )
            db.session.add(interaction)
            
            from app.models.core import CoreAuditEvent
            ip_addr = request.headers.get('X-Forwarded-For', request.remote_addr)
            audit = CoreAuditEvent(
                user_id=current_user.id,
                action="PLEDGE_ACCEPTED",
                entity_type="SACE_PLEDGE",
                details="Admin accepted IP pledge",
                ip_address=ip_addr
            )
            db.session.add(audit)
            db.session.commit()
        session.pop('sace_admin_pledged', None)

    if current_user.is_authenticated:
        pledge = SaceWorkshopInteraction.query.filter_by(user_id=current_user.id, activity_slug="admin_patent_pledge").first()
        has_pledged = pledge is not None or session.get('sace_admin_pledged', False)
        # Load provisioned auditors
        invites = SaceWorkshopInteraction.query.filter_by(user_id=current_user.id, activity_slug="auditor_provisioned").order_by(SaceWorkshopInteraction.timestamp.desc()).all()
    else:
        has_pledged = session.get('sace_admin_pledged', False)
        invites = []
    
    auditors = []
    for inv in invites:
        try:
            data = json.loads(inv.response_data)
            # Hide old legacy test accounts that don't have access codes
            if not data.get("code"):
                continue
                
            data['date'] = inv.timestamp.strftime("%Y-%m-%d")
            data['id'] = inv.id
            auditors.append(data)
        except Exception:
            pass
            
    return render_template("program_sace/provisioning_map.html", has_pledged=has_pledged, auditors=auditors)

@sace_bp.route("/sace/provisioning/pledge", methods=["POST"])
def provisioning_pledge():
    from app.models.sace import SaceWorkshopInteraction
    
    # Save to session so it survives registration
    session['sace_admin_pledged'] = True
    
    if current_user.is_authenticated:
        pledge = SaceWorkshopInteraction.query.filter_by(user_id=current_user.id, activity_slug="admin_patent_pledge").first()
        if not pledge:
            interaction = SaceWorkshopInteraction(
                user_id=current_user.id,
                activity_slug="admin_patent_pledge",
                response_data="Admin accepted IP pledge"
            )
            db.session.add(interaction)
            from app.models.core import CoreAuditEvent
            ip_addr = request.headers.get('X-Forwarded-For', request.remote_addr)
            audit = CoreAuditEvent(
                user_id=current_user.id,
                action="PLEDGE_ACCEPTED",
                entity_type="SACE_PLEDGE",
                details="Admin accepted IP pledge",
                ip_address=ip_addr
            )
            db.session.add(audit)
            db.session.commit()
            flash("Intellectual Property pledge accepted. Provisioning unlocked.", "success")
    else:
        # Just use the session, don't pollute the DB with user_id=1
        flash("Intellectual Property pledge accepted. Provisioning unlocked.", "success")
        
    return redirect(url_for('sace_bp.provisioning_map'))

@sace_bp.route("/sace/provisioning/generate_code", methods=["POST"])
def generate_auditor_code():
    from app.models.sace import SaceWorkshopInteraction
    import json
    import random
    import string
    
    sace_user_id = current_user.id if current_user.is_authenticated else 1
    
    # Generate an 8-char code, split with hyphen for readability
    chars = string.ascii_uppercase + string.digits
    raw_code = ''.join(random.choice(chars) for _ in range(8))
    code = f"{raw_code[:4]}-{raw_code[4:]}"
    
    data = {
        "code": code,
        "status": "Unclaimed",
        "first_name": "",
        "last_name": "",
        "email": ""
    }
    
    interaction = SaceWorkshopInteraction(
        user_id=sace_user_id,
        activity_slug="auditor_provisioned",
        response_data=json.dumps(data)
    )
    db.session.add(interaction)
    db.session.commit()
    
    from app.models.core import CoreAuditEvent
    ip_addr = request.headers.get('X-Forwarded-For', request.remote_addr)
    audit = CoreAuditEvent(
        user_id=sace_user_id,
        action="CODE_GENERATED",
        entity_type="SACE_EVALUATOR_CODE",
        details=f"Generated new SACE Evaluator Access Code: {code}",
        ip_address=ip_addr
    )
    db.session.add(audit)
    db.session.commit()
    
    flash(f"New Auditor Access Code generated: {code}", "success")
    return redirect(url_for('sace_bp.provisioning_map'))





@sace_bp.route("/sace/audit_report")
@login_required
def audit_report():
    # Only show events related to SACE and login
    events = CoreAuditEvent.query.order_by(CoreAuditEvent.created_at.desc()).limit(100).all()
    return render_template("program_sace/compliance/audit_report.html", events=events)


@sace_bp.route("/sace/reading/post_test", methods=["GET"])
@login_required
def post_test():
    return render_template("program_sace/post_test/test.html")

@sace_bp.route("/sace/reading/post_test", methods=["POST"])
@login_required
def submit_post_test():
    import json
    from app.models.sace import SaceWorkshopInteraction
    
    q1 = request.form.get('q1')
    q2 = request.form.get('q2')
    q3 = request.form.get('q3')
    q4 = request.form.get('q4')
    
    score = 0
    if q1 == 'B': score += 25
    if q2 == 'B': score += 25
    if q3 == 'C': score += 25
    if q4 == 'A': score += 25
    
    competencies = []
    for key in ['comp_objective', 'comp_sequence', 'comp_demo', 'comp_participation', 'comp_guidance', 'comp_reading', 'comp_assessment', 'comp_reflection']:
        val = request.form.get(key)
        if val:
            competencies.append(val)
            
    answers = {
        'q1': q1,
        'q2': q2,
        'q3': q3,
        'q4': q4,
        'score': score,
        'competencies': competencies
    }
    
    interaction = SaceWorkshopInteraction(
        user_id=current_user.id,
        activity_slug='workshop_post_test',
        response_data=json.dumps(answers)
    )
    db.session.add(interaction)
    db.session.commit()
    
    return redirect(url_for('sace_bp.post_test_results'))

@sace_bp.route("/sace/reading/post_test/results")
@login_required
def post_test_results():
    import json
    from app.models.sace import SaceWorkshopInteraction
    
    interaction = SaceWorkshopInteraction.query.filter_by(
        user_id=current_user.id,
        activity_slug='workshop_post_test'
    ).order_by(SaceWorkshopInteraction.timestamp.desc()).first()
    
    answers = {}
    if interaction:
        answers = json.loads(interaction.response_data)
        
    return render_template("program_sace/post_test/results.html", answers=answers)

@sace_bp.route("/sace/reading/certificate/email", methods=["POST"])
@login_required
def email_certificate():
    from datetime import datetime
    import uuid
    from app.subject_reading.routes import _email_certificate_pdf
    
    target_email = request.form.get("email")
    if not target_email:
        flash("Email address is required.", "error")
        return redirect(url_for("sace_bp.reading_hub"))
        
    cert_id = "AIT-WS-" + str(uuid.uuid4())[:8].upper()
    completed_at = datetime.utcnow()
    
    from app.models.sace import SaceWorkshopInteraction
    import json
    interaction = SaceWorkshopInteraction.query.filter_by(
        user_id=current_user.id,
        activity_slug='workshop_post_test'
    ).order_by(SaceWorkshopInteraction.timestamp.desc()).first()
    
    answers = {}
    if interaction:
        answers = json.loads(interaction.response_data)

    try:
        # Generate the standard PDF
        pdf_bytes = _generate_sace_certificate_pdf(
            certificate_id=cert_id,
            learner_name=current_user.name,
            completed_at=completed_at,
            user_id=current_user.id,
            answers=answers
        )
        
        # Email it
        _email_certificate_pdf(
            to_email=target_email,
            learner_name=current_user.name,
            certificate_id=cert_id,
            pdf_bytes=pdf_bytes
        )
        
        flash(f"Certificate successfully emailed to {target_email}", "success")
    except Exception as e:
        current_app.logger.error(f"Failed to email SACE workshop certificate: {e}")
        flash("Failed to email certificate. Please try again.", "error")
        return redirect(url_for("sace_bp.post_test_results"))
        
    return redirect(url_for("sace_bp.reading_hub"))


def _generate_sace_certificate_pdf(certificate_id, learner_name, completed_at, user_id=None, answers=None):
    from flask import current_app, render_template
    from datetime import datetime
    from app.utils.pdf_render import html_to_pdf_bytes
    
    if isinstance(completed_at, str):
        try:
            completed_at = datetime.fromisoformat(completed_at)
        except Exception:
            completed_at = datetime.utcnow()
    elif completed_at is None:
        completed_at = datetime.utcnow()

    completed_date = completed_at.strftime("%d %B %Y")

    from app.utils.branding import get_logo_data_uri, get_seal_data_uri
    logo_data_uri = get_logo_data_uri()
    seal_data_uri = get_seal_data_uri()

    try:
        html_out = render_template(
            "program_sace/post_test/certificate_pdf.html",
            learner_name=learner_name,
            completed_date=completed_date,
            certificate_id=certificate_id,
            logo_path=logo_data_uri,
            seal_path=seal_data_uri,
            answers=answers,
        )
        pdf_bytes = html_to_pdf_bytes(html_out)
        return pdf_bytes
    except Exception as e:
        current_app.logger.error(f"SACE PDF generation failed for {certificate_id}: {e}")
        return b""

@sace_bp.route("/sace/secure_view/<doc_type>")
@login_required
def secure_view(doc_type):
    """Secure on-site document viewer that logs the interaction and blocks downloads."""
    from app.models.sace import SaceWorkshopInteraction, SaceDocument
    
    # Retrieve the document URL first
    doc = SaceDocument.query.filter_by(document_type=doc_type).first()
    # TESTING FIX: If doc is missing, log interaction anyway and use a fallback title
    doc_title = doc.title if doc else doc_type.replace('_', ' ').title()
    doc_url = doc.document_url if doc else ""
    
    # HARDCODE P_GUIDE FOR TESTING
    if doc_type == 'p_guide':
        doc_title = "LITRE Participant Manual (P Guide)"
        doc_url = url_for('static', filename='pdf/P_Guide.pdf')

    # Log that the user viewed this document
    interaction = SaceWorkshopInteraction.query.filter_by(user_id=current_user.id, activity_slug=f"viewed_{doc_type}").first()
    if not interaction:
        interaction = SaceWorkshopInteraction(
            user_id=current_user.id,
            activity_slug=f"viewed_{doc_type}",
            response_data="Document opened in secure viewer"
        )
        db.session.add(interaction)
        db.session.commit()
        
    if doc and doc.file_path:
        doc_url = url_for('static', filename=doc.file_path.replace('app/static/', '').replace('static/', ''))
    elif not doc_url:
        doc_url = "about:blank"
    
    # Map document types to readable titles
    titles = {
        'reviewer_guide': 'Reviewer Guide',
        'app_form': 'Application Form',
        'patent': 'Patent Documentation',
        'annexures': 'Annexures A-E'
    }
    
    return render_template("program_sace/secure_viewer.html", doc_url=doc_url, doc_title=titles.get(doc_type, 'Secure Document'))


@sace_bp.route("/sace/log_ppp_view", methods=["POST"])
@login_required
def log_ppp_view():
    from app.models.sace import SaceWorkshopInteraction
    interaction = SaceWorkshopInteraction.query.filter_by(
        user_id=current_user.id, activity_slug="viewed_ppp"
    ).first()
    if not interaction:
        interaction = SaceWorkshopInteraction(
            user_id=current_user.id,
            activity_slug="viewed_ppp",
            response_data="Completed PPP slide review"
        )
        db.session.add(interaction)
        db.session.commit()
    return {"status": "ok"}

# ==========================================
# SACE CONTROL CENTRE: DOCUMENTS & LOGS
# ==========================================

@sace_bp.route("/sace/provisioning/documents")
@login_required
def provider_documents():
    from app.models.sace import SaceWorkshopInteraction
    sace_user_id = current_user.id if current_user.is_authenticated else 1
    
    # Check interaction tracking
    interactions = SaceWorkshopInteraction.query.filter_by(
        user_id=sace_user_id
    ).filter(SaceWorkshopInteraction.activity_slug.like("doc_tracked_%")).all()
    
    tracked_ids = [inv.activity_slug.replace("doc_tracked_", "") for inv in interactions]
    
    documents = [
        {
            "id": "app1",
            "title": "Application Form 1",
            "description": "The primary SACE application form.",
            "is_tracked": "app1" in tracked_ids
        },
        {
            "id": "app2",
            "title": "Application Form 2",
            "description": "The secondary SACE application form.",
            "is_tracked": "app2" in tracked_ids
        },
        {
            "id": "tt",
            "title": "Reading Timetable (T/T)",
            "description": "Workshop schedule and session breakdown.",
            "is_tracked": "tt" in tracked_ids
        },
        {
            "id": "f_cv",
            "title": "Facilitator CVs & Compliance",
            "description": "Details of Presenters, Certified copies of ID, and comprehensive CV.",
            "is_tracked": "f_cv" in tracked_ids
        },
        {
            "id": "f_guide",
            "title": "Facilitator Manual",
            "description": "Educator slide notes and methodology guide.",
            "is_tracked": "f_guide" in tracked_ids
        },
        {
            "id": "ip_pledge",
            "title": "AIT IP Pledge",
            "description": "Blank Intellectual Property Pledge for manual signing.",
            "is_tracked": "ip_pledge" in tracked_ids
        }
    ]
    
    return render_template("program_sace/provider_documents_map.html", documents=documents)


@sace_bp.route("/sace/provisioning/document/action/<doc_id>", methods=["GET", "POST"])
@login_required
def document_action(doc_id):
    from app.models.sace import SaceWorkshopInteraction
    from app.utils.mailer import send_email
    import json
    
    sace_user_id = current_user.id if current_user.is_authenticated else 1
    action = request.args.get('action') or request.form.get('action')
    
    # 1. Log the tracking interaction if not already logged
    slug = f"doc_tracked_{doc_id}"
    existing = SaceWorkshopInteraction.query.filter_by(user_id=sace_user_id, activity_slug=slug).first()
    if not existing:
        interaction = SaceWorkshopInteraction(
            user_id=sace_user_id,
            activity_slug=slug,
            response_data=json.dumps({"action": action, "doc_id": doc_id})
        )
        db.session.add(interaction)
        db.session.commit()
        
    
    # Also log to Platform Audit Report
    from app.models.core import CoreAuditEvent
    ip_addr = request.headers.get('X-Forwarded-For', request.remote_addr)
    doc_titles = {
        "app1": "SACE Application Form 1",
        "app2": "SACE Application Form 2",
        "tt": "Reading Timetable (T/T)",
        "f_cv": "Facilitator CVs & Compliance",
        "f_guide": "Facilitator Manual",
        "ip_pledge": "AIT IP Pledge"
    }
    title = doc_titles.get(str(doc_id), f"Document {doc_id}")
    audit = CoreAuditEvent(
        user_id=sace_user_id,
        action="DOCUMENT_ACCESSED" if action == "view" else "DOCUMENT_EMAILED",
        entity_type="SACE_DOCUMENT",
        details=f"Admin {action}ed '{title}'",
        ip_address=ip_addr
    )
    db.session.add(audit)
    db.session.commit()
    
    doc_file_map = {
        "app1": "pdf/App_Form_1.pdf",
        "app2": "pdf/App_Form_2.pdf",
        "tt": "pdf/Reading Timetable.pdf",
        "f_cv": "pdf/Facilitator_CVs.pdf",
        "f_guide": "pdf/F_Guide.pdf",
        "ip_pledge": "pdf/IP_Pledge.pdf"
    }
    
    filename = doc_file_map.get(doc_id, "pdf/App_Form_1.pdf")
    
    if action == "view":
        return redirect(url_for('static', filename=filename))
        
    elif action == "email" and request.method == "POST":
        recipient = request.form.get("recipient_email")
        if recipient:
            doc_names = {
                "app1": "SACE Application Form 1",
                "app2": "SACE Application Form 2",
                "tt": "Reading Timetable (T/T)",
                "f_cv": "Facilitator CVs & Compliance",
                "f_guide": "Facilitator Manual",
                "ip_pledge": "AIT IP Pledge"
            }
            doc_name = doc_names.get(doc_id, "Document")
            
            html_body = f"""
            <div style="font-family: sans-serif; padding: 20px;">
                <h2 style="color: #4f46e5;">AIT Provider Document</h2>
                <p>Hello,</p>
                <p>Please find the requested AIT SACE Document: <strong>{doc_name}</strong>.</p>
                <p>You can securely access and download this document using the link below:</p>
                <div style="margin: 20px 0;">
                    <a href="{url_for('static', filename=filename, _external=True)}" style="background-color: #4f46e5; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px; font-weight: bold;">View Document</a>
                </div>
                <p>Regards,<br>AIT Administrator</p>
            </div>
            """
            send_email(
                subject=f"AIT Document: {doc_name}",
                recipients=[recipient],
                body=f"Please view the document here: {url_for('static', filename=filename, _external=True)}",
                html=html_body
            )
            flash(f"Document securely emailed to {recipient}.", "success")
            
        return redirect(url_for('sace_bp.provider_documents'))
        
    return redirect(url_for('sace_bp.provider_documents'))


@sace_bp.route("/sace/provisioning/logs")
@login_required
def provisioning_logs():
    from app.models.core import CoreAuditEvent
    from app.models.sace import SaceWorkshopInteraction
    from app.models.auth import User
    import json
    
    sace_user_id = current_user.id if current_user.is_authenticated else 1
    
    # Find auditor emails provisioned by this user
    invites = SaceWorkshopInteraction.query.filter_by(user_id=sace_user_id, activity_slug="auditor_provisioned").all()
    emails = []
    for inv in invites:
        try:
            data = json.loads(inv.response_data)
            if 'email' in data:
                emails.append(data['email'].lower())
        except:
            pass
            
    auditor_ids = []
    if emails:
        auditor_users = User.query.filter(db.func.lower(User.email).in_(emails)).all()
        auditor_ids = [u.id for u in auditor_users]
        
    target_ids = [sace_user_id] + auditor_ids
    
    # Query logs for these users
    events = CoreAuditEvent.query.filter(CoreAuditEvent.user_id.in_(target_ids)).order_by(CoreAuditEvent.created_at.desc()).limit(100).all()
    
    # We will reuse the audit_report template but with a back button context if we want, 
    # but the simplest is just to render it natively with these events.
    # The existing template might need a slight tweak to show a back button to Control Centre.
    return render_template("program_sace/compliance/audit_report.html", events=events, is_control_centre=True)



# ==========================================
# AUDITOR JOIN FLOW (CODE REDEMPTION)
# ==========================================

@sace_bp.route("/sace/join", methods=["GET", "POST"])
def auditor_join():
    from app.models.sace import SaceWorkshopInteraction
    import json
    
    if request.method == "POST":
        code = (request.form.get("code") or "").strip().upper()
        if not code:
            flash("Please enter an access code.", "error")
            return redirect(url_for('sace_bp.auditor_join'))
            
        # Strip SACE- if they included it
        if code.startswith("SACE-"):
            code = code[5:].strip()
        if code.startswith("SACE "):
            code = code[5:].strip()
            
        # Format if user forgot hyphen (assuming 8 chars)
        code = code.replace(" ", "")
        if len(code) == 8 and '-' not in code:
            code = f"{code[:4]}-{code[4:]}"
            
        # Find the code in the DB (needs a scan since it's JSON, but it's a small table for SACE)
        interactions = SaceWorkshopInteraction.query.filter_by(activity_slug="auditor_provisioned").all()
        found_inv = None
        for inv in interactions:
            try:
                data = json.loads(inv.response_data)
                if data.get('code') == code:
                    found_inv = inv
                    break
            except:
                pass
                
        if not found_inv:
            flash("Invalid or unrecognized Access Code.", "error")
            return redirect(url_for('sace_bp.auditor_join'))
            
        data = json.loads(found_inv.response_data)
        if data.get('status') != "Unclaimed":
            flash("This Access Code has already been claimed.", "error")
            return redirect(url_for('sace_bp.auditor_join'))
            
        # If valid, put it in session and redirect to pledge
        session['pending_sace_code'] = code
        return redirect(url_for('sace_bp.auditor_pledge'))
        
    return render_template("program_sace/auditor_join.html")



@sace_bp.route("/sace/auditor_pledge", methods=["GET", "POST"])
def auditor_pledge():
    if 'pending_sace_code' not in session:
        return redirect(url_for('sace_bp.auditor_join'))
        
    if request.method == "POST":
        session['sace_evaluator_pledged'] = True
        
        if getattr(current_user, 'is_authenticated', False):
            return redirect(url_for('sace_bp.claim_code'))
            
        return redirect(url_for('auth_bp.register', subject='sace_endorsement', next=url_for('sace_bp.claim_code')))
        
    return render_template("program_sace/auditor_pledge.html")

@sace_bp.route("/sace/claim_code")
@login_required
def claim_code():
    from app.models.sace import SaceWorkshopInteraction
    import json
    
    code = session.get('pending_sace_code')
    if not code:
        flash("No pending access code to claim.", "warning")
        return redirect(url_for('sace_bp.auditor_join'))
        
    interactions = SaceWorkshopInteraction.query.filter_by(activity_slug="auditor_provisioned").all()
    found_inv = None
    for inv in interactions:
        try:
            data = json.loads(inv.response_data)
            if data.get('code') == code:
                found_inv = inv
                break
        except:
            pass
            
    if found_inv:
        if current_user.id == found_inv.user_id:
            flash("You cannot claim an access code that you generated. Access codes must be claimed by the Auditor.", "warning")
            return redirect(url_for('sace_bp.dashboard'))
            
        data = json.loads(found_inv.response_data)
        if data.get('status') == "Unclaimed":
            data['status'] = f"Claimed"
            data['first_name'] = current_user.name or current_user.email.split('@')[0]
            data['last_name'] = ""
            data['email'] = current_user.email
            data['claimed_by_user_id'] = current_user.id
            found_inv.response_data = json.dumps(data)
            db.session.commit()
            
            if session.get('sace_evaluator_pledged'):
                pledge_interaction = SaceWorkshopInteraction(
                    user_id=current_user.id,
                    activity_slug="viewed_patent",
                    response_data="Evaluator accepted IP pledge during onboarding"
                )
                db.session.add(pledge_interaction)
                
                from app.models.core import CoreAuditEvent
                ip_addr = request.headers.get('X-Forwarded-For', request.remote_addr)
                audit = CoreAuditEvent(
                    user_id=current_user.id,
                    action="PLEDGE_ACCEPTED",
                    entity_type="SACE_PLEDGE",
                    details="Evaluator accepted IP pledge during onboarding",
                    ip_address=ip_addr
                )
                db.session.add(audit)
                db.session.commit()
                session.pop('sace_evaluator_pledged', None)
            
            # Ensure they are enrolled in sace_reading (or just clear session so they can go to the hub)
            session.pop('pending_sace_code', None)
            flash("Access Code successfully claimed. Welcome to the SACE Evaluation Hub.", "success")
            
            # Direct them instantly to the reading activity!
            return redirect(url_for('sace_bp.reading_hub'))
            
    flash("Failed to claim code or code already used.", "error")
    return redirect(url_for('sace_bp.auditor_join'))

@sace_bp.route("/sace/provisioning/print_slip/<code>")
@login_required
def print_access_slip(code):
    return render_template("program_sace/print_slip.html", code=code)
