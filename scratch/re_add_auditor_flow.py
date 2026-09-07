import re

routes_path = 'app/program_sace/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Update auditor_join redirect
old_join = '''        # If valid, put it in session and redirect to registration
        session['pending_sace_code'] = code
        
        # If they are magically already logged in (e.g. testing)
        if current_user.is_authenticated:
            return redirect(url_for('sace_bp.claim_code'))
            
        # Redirect to generic registration page, but pass subject=sace so it's free
        # and next=/sace/claim_code so they return to claim the code.
        return redirect(url_for('auth_bp.register', subject='sace', next=url_for('sace_bp.claim_code')))'''

new_join = '''        # If valid, put it in session and redirect to pledge
        session['pending_sace_code'] = code
        return redirect(url_for('sace_bp.auditor_pledge'))'''

text = text.replace(old_join, new_join)

# 2. Re-add auditor_pledge route
old_claim = '''@sace_bp.route("/sace/claim_code")'''
new_pledge_route = '''@sace_bp.route("/sace/auditor_pledge", methods=["GET", "POST"])
def auditor_pledge():
    if 'pending_sace_code' not in session:
        return redirect(url_for('sace_bp.auditor_join'))
        
    if request.method == "POST":
        session['sace_evaluator_pledged'] = True
        
        if getattr(current_user, 'is_authenticated', False):
            return redirect(url_for('sace_bp.claim_code'))
            
        return redirect(url_for('auth_bp.register', subject='sace', next=url_for('sace_bp.claim_code')))
        
    return render_template("program_sace/auditor_pledge.html")

@sace_bp.route("/sace/claim_code")'''

text = text.replace(old_claim, new_pledge_route)

# 3. Restore claim_code logic
old_claim_commit = '''            found_inv.response_data = json.dumps(data)
            db.session.commit()
            
            # Ensure they are enrolled in sace_reading (or just clear session so they can go to the hub)'''

new_claim_commit = '''            found_inv.response_data = json.dumps(data)
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
            
            # Ensure they are enrolled in sace_reading (or just clear session so they can go to the hub)'''

text = text.replace(old_claim_commit, new_claim_commit)

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(text)
