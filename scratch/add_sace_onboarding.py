import re

with open('app/program_sace/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

new_route = """
@sace_bp.route('/sace/participant/onboarding', methods=['GET', 'POST'])
@login_required
def participant_onboarding():
    from flask_login import current_user
    from app.extensions import db
    from app.models.sace import SaceWorkshopInteraction
    
    # 1. If Evaluator/Admin, route them to the main Hub
    if current_user.is_admin_global():  # using is_admin_global() or similar? Let's just check roles
        pass # we will do a safe check
    
    # Safe admin check:
    is_eval = False
    for r in current_user.user_roles:
        if r.role and r.role.slug == 'admin':
            is_eval = True
            break
            
    if is_eval:
        return redirect(url_for('sace_bp.reading_hub'))

    # 2. Check if they already entered their SACE number
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
            
    return render_template('program_sace/onboarding.html')
"""

if "def participant_onboarding" not in content:
    content += new_route

with open('app/program_sace/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
