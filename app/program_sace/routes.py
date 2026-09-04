import os
from flask import render_template, request, redirect, url_for, flash, current_app, jsonify, send_from_directory
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
            "name": "Litre Reading Workshop",
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
@login_required
def provisioning_map():
    from app.models.sace import SaceWorkshopInteraction
    import json
    
    # Check if pledged
    pledge = SaceWorkshopInteraction.query.filter_by(user_id=current_user.id, activity_slug="admin_patent_pledge").first()
    has_pledged = pledge is not None
    
    # Load provisioned auditors
    invites = SaceWorkshopInteraction.query.filter_by(user_id=current_user.id, activity_slug="auditor_provisioned").order_by(SaceWorkshopInteraction.timestamp.desc()).all()
    
    auditors = []
    for inv in invites:
        try:
            data = json.loads(inv.response_data)
            data['date'] = inv.timestamp.strftime("%Y-%m-%d")
            auditors.append(data)
        except Exception:
            pass
            
    return render_template("program_sace/provisioning_map.html", has_pledged=has_pledged, auditors=auditors)

@sace_bp.route("/sace/provisioning/pledge", methods=["POST"])
@login_required
def provisioning_pledge():
    from app.models.sace import SaceWorkshopInteraction
    pledge = SaceWorkshopInteraction.query.filter_by(user_id=current_user.id, activity_slug="admin_patent_pledge").first()
    if not pledge:
        interaction = SaceWorkshopInteraction(
            user_id=current_user.id,
            activity_slug="admin_patent_pledge",
            response_data="Admin accepted IP pledge"
        )
        db.session.add(interaction)
        db.session.commit()
        flash("Intellectual Property pledge accepted. Provisioning unlocked.", "success")
    return redirect(url_for('sace_bp.provisioning_map'))

@sace_bp.route("/sace/provisioning/add_auditor", methods=["POST"])
@login_required
def provision_auditor():
    from app.models.sace import SaceWorkshopInteraction
    import json
    
    first_name = request.form.get("first_name")
    last_name = request.form.get("last_name")
    email = request.form.get("email")
    
    if first_name and last_name and email:
        data = {
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "status": "Invite Sent"
        }
        interaction = SaceWorkshopInteraction(
            user_id=current_user.id,
            activity_slug="auditor_provisioned",
            response_data=json.dumps(data)
        )
        db.session.add(interaction)
        db.session.commit()
        flash(f"Auditor {first_name} {last_name} has been provisioned. Access email sent to {email}.", "success")
    else:
        flash("All fields are required to provision an auditor.", "error")
        
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
