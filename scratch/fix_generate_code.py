import re

routes_path = 'app/program_sace/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    text = f.read()

# Replace from @sace_bp.route("/sace/provisioning/add_auditor"... to return redirect(url_for('sace_bp.provisioning_map'))
pattern = r'@sace_bp\.route\("/sace/provisioning/add_auditor".*?return redirect\(url_for\(\'sace_bp\.provisioning_map\'\)\)'

new_code = '''@sace_bp.route("/sace/provisioning/generate_code", methods=["POST"])
def generate_auditor_code():
    from app.models.sace import SaceWorkshopInteraction
    import json
    import random
    import string
    
    sace_user_id = current_user.id if current_user.is_authenticated else 1
    
    # Generate an 8-char code, split with hyphen for readability
    chars = string.ascii_uppercase + string.digits
    raw_code = ''.join(random.choice(chars) for _ in range(8))
    code = f"{raw_code[:4]}-{raw_code[4:]}"
    
    data = {
        "code": code,
        "status": "Unclaimed",
        "first_name": "",
        "last_name": "",
        "email": ""
    }
    
    interaction = SaceWorkshopInteraction(
        user_id=sace_user_id,
        activity_slug="auditor_provisioned",
        response_data=json.dumps(data)
    )
    db.session.add(interaction)
    db.session.commit()
    
    flash(f"New Auditor Access Code generated: {code}", "success")
    return redirect(url_for('sace_bp.provisioning_map'))'''

new_text = re.sub(pattern, new_code, text, flags=re.DOTALL)

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(new_text)

print("Replacement done.")
