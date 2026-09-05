routes_file = 'app/program_sace/routes.py'
with open(routes_file, 'r', encoding='utf-8') as f: text = f.read()

reset_route = '''@sace_bp.route('/sace/reset_progress', methods=['POST'])
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

@sace_bp.route("/sace/reading")'''

text = text.replace('@sace_bp.route("/sace/reading")', reset_route)

with open(routes_file, 'w', encoding='utf-8') as f: f.write(text)
