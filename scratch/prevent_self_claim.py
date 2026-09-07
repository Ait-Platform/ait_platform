import re

routes_path = 'app/program_sace/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    text = f.read()

old_logic = '''        data = json.loads(found_inv.response_data)
        if data.get('status') == "Unclaimed":
            data['status'] = f"Claimed"
            data['first_name'] = current_user.name or current_user.email.split('@')[0]'''

new_logic = '''        if current_user.id == found_inv.user_id:
            flash("You cannot claim an access code that you generated. Access codes must be claimed by the Auditor.", "warning")
            return redirect(url_for('sace_bp.dashboard'))
            
        data = json.loads(found_inv.response_data)
        if data.get('status') == "Unclaimed":
            data['status'] = f"Claimed"
            data['first_name'] = current_user.name or current_user.email.split('@')[0]'''

text = text.replace(old_logic, new_logic)

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(text)
