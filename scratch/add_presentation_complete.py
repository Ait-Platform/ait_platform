import re

file_path = 'app/program_sace/routes.py'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

presentation_complete_route = '''
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
'''

if 'def presentation_complete():' not in text:
    text = text.replace('def presentation():\n    return render_template("program_sace/presentation_ppp.html")', 
                        'def presentation():\n    return render_template("program_sace/presentation_ppp.html")\n' + presentation_complete_route)

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
