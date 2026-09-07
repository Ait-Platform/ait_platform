import re

routes_path = 'app/program_sace/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Revert auditor_join redirect
old_join = '''        # If valid, put it in session and redirect to pledge
        session['pending_sace_code'] = code
        return redirect(url_for('sace_bp.auditor_pledge'))'''

new_join = '''        # If valid, put it in session and redirect to registration
        session['pending_sace_code'] = code
        
        # If they are magically already logged in (e.g. testing)
        if current_user.is_authenticated:
            return redirect(url_for('sace_bp.claim_code'))
            
        # Redirect to generic registration page, but pass subject=sace so it's free
        # and next=/sace/claim_code so they return to claim the code.
        return redirect(url_for('auth_bp.register', subject='sace', next=url_for('sace_bp.claim_code')))'''

text = text.replace(old_join, new_join)

# 2. Revert claim_code logic
old_claim_commit = '''            found_inv.response_data = json.dumps(data)
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

new_claim_commit = '''            found_inv.response_data = json.dumps(data)
            db.session.commit()
            
            # Ensure they are enrolled in sace_reading (or just clear session so they can go to the hub)'''

text = text.replace(old_claim_commit, new_claim_commit)

# 3. Remove auditor_pledge route completely
import re
pattern = r'@sace_bp\.route\("/sace/auditor_pledge".*?def auditor_pledge\(\):.*?return render_template\("program_sace/auditor_pledge\.html"\)'
text = re.sub(pattern, '', text, flags=re.DOTALL)

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(text)
