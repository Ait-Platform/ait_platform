import re

with open('app/program_sace/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

new_routes = '''
@sace_bp.route("/sace/reading/post_test", methods=["GET"])
@login_required
def post_test():
    return render_template("program_sace/post_test/test.html")

@sace_bp.route("/sace/reading/post_test", methods=["POST"])
@login_required
def submit_post_test():
    import json
    from app.models.sace import SaceWorkshopInteraction
    
    answers = {
        'q1': request.form.get('q1'),
        'q2': request.form.get('q2'),
        'q3': request.form.get('q3'),
        'q4': request.form.get('q4')
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

@sace_bp.route("/sace/reading/certificate")
@login_required
def certificate():
    from datetime import datetime
    import uuid
    date_str = datetime.utcnow().strftime("%d %B %Y")
    session_id = str(uuid.uuid4())[:8].upper()
    return render_template("program_sace/post_test/certificate.html", date=date_str, session_id=session_id)
'''

text += "\n" + new_routes

with open('app/program_sace/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)

