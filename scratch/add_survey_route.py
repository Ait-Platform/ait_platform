import re

file_path = 'app/program_sace/routes.py'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

new_routes = '''
@sace_bp.route("/sace/reading/survey", methods=["GET"])
@login_required
def survey():
    return render_template("program_sace/post_test/survey.html")

@sace_bp.route("/sace/reading/survey", methods=["POST"])
@login_required
def submit_survey():
    from app.models.sace import SaceWorkshopInteraction
    import json
    
    competencies = []
    for key in ['comp_objective', 'comp_sequence', 'comp_demo', 'comp_participation', 'comp_guidance', 'comp_reading', 'comp_assessment', 'comp_reflection']:
        val = request.form.get(key)
        if val:
            competencies.append(val)
            
    # We can save it, but for now we just redirect to the post-test
    interaction = SaceWorkshopInteraction(
        user_id=current_user.id,
        activity_slug='workshop_survey',
        response_data=json.dumps({"competencies": competencies})
    )
    db.session.add(interaction)
    db.session.commit()
    
    return redirect(url_for('sace_bp.post_test'))
'''

if 'def survey():' not in text:
    text = text.replace('def post_test():', new_routes + '\n\n@sace_bp.route("/sace/reading/post_test", methods=["GET"])\n@login_required\ndef post_test():')

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
