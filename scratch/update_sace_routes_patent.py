import re

file_path = 'app/program_sace/routes.py'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

# Remove reviewer_guide from progress dict
text = text.replace("'reviewer_guide': 'viewed_reviewer_guide' in completed_slugs,\n", "")

# Add acknowledge_patent route
ack_route = '''
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
'''
if 'def acknowledge_patent():' not in text:
    text = text.replace('def presentation():', ack_route + '\n\n@sace_bp.route("/sace/reading/presentation")\n@login_required\ndef presentation():')

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
