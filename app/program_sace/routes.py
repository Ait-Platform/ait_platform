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
    return render_template("program_sace/compliance/annexure_a.html")

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

@sace_bp.route("/sace/reading")
@login_required
def reading_hub():
    from app.models.sace import SaceDocument
    app_form = SaceDocument.query.filter_by(slug='reading', document_type='application_form').first()
    return render_template("program_sace/reading_hub.html", app_form=app_form)

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
    ).order_by(SaceWorkshopInteraction.created_at).all()
    
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

