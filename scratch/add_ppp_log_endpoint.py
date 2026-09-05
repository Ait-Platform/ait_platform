import re

file_path = 'app/program_sace/routes.py'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

new_endpoint = '''
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
'''

if 'def log_ppp_view' not in text:
    text += new_endpoint

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
