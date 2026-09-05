import re

routes_path = 'app/program_sace/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    text = f.read()

# Replace provision_auditor logic
old_provision = '''@sace_bp.route("/sace/provisioning/add_auditor", methods=["POST"])
def provision_auditor():
    from app.models.sace import SaceWorkshopInteraction
    from app.utils.mailer import send_email
    import json
    import json
    
    first_name = request.form.get("first_name")
    last_name = request.form.get("last_name")
    email = request.form.get("email")
    
    if first_name and last_name and email:
        data = {
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "status": "Invite Sent"
        }
        sace_user_id = current_user.id if current_user.is_authenticated else 1
        interaction = SaceWorkshopInteraction(
            user_id=sace_user_id,
            activity_slug="auditor_provisioned",
            response_data=json.dumps(data)
        )
        # Send Email
        dashboard_url = url_for('sace_bp.dashboard', _external=True)
        html_body = f"""
        <p>Dear {first_name},</p>
        <p>You have been provisioned by SACE to audit the AIT Provider Activity.</p>
        <p>Please access your secure evaluation dashboard here:</p>
        <p><a href="{dashboard_url}">{dashboard_url}</a></p>
        <br>
        <p>Best regards,<br>AIT Platform System</p>
        """
        success = send_email(
            subject="Secure Access: SACE Auditor Dashboard",
            recipients=[email],
            body=f"Dear {first_name}, Please access your secure evaluation dashboard at: {dashboard_url}",
            html=html_body
        )
        if success:
            flash(f"Auditor provisioned and invite sent to {email}.", "success")
        else:
            flash(f"Auditor provisioned, but email failed to send to {email}.", "warning")
            
    else:
        flash("All fields are required to provision an auditor.", "error")
        
    return redirect(url_for('sace_bp.provisioning_map'))'''

new_provision = '''@sace_bp.route("/sace/provisioning/generate_code", methods=["POST"])
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

text = text.replace(old_provision, new_provision)

# Add the /sace/join route at the very end
join_routes = '''

# ==========================================
# AUDITOR JOIN FLOW (CODE REDEMPTION)
# ==========================================

@sace_bp.route("/sace/join", methods=["GET", "POST"])
def auditor_join():
    from app.models.sace import SaceWorkshopInteraction
    import json
    
    if request.method == "POST":
        code = (request.form.get("code") or "").strip().upper()
        if not code:
            flash("Please enter an access code.", "error")
            return redirect(url_for('sace_bp.auditor_join'))
            
        # Format if user forgot hyphen (assuming 8 chars)
        if len(code) == 8 and '-' not in code:
            code = f"{code[:4]}-{code[4:]}"
            
        # Find the code in the DB (needs a scan since it's JSON, but it's a small table for SACE)
        interactions = SaceWorkshopInteraction.query.filter_by(activity_slug="auditor_provisioned").all()
        found_inv = None
        for inv in interactions:
            try:
                data = json.loads(inv.response_data)
                if data.get('code') == code:
                    found_inv = inv
                    break
            except:
                pass
                
        if not found_inv:
            flash("Invalid or unrecognized Access Code.", "error")
            return redirect(url_for('sace_bp.auditor_join'))
            
        data = json.loads(found_inv.response_data)
        if data.get('status') != "Unclaimed":
            flash("This Access Code has already been claimed.", "error")
            return redirect(url_for('sace_bp.auditor_join'))
            
        # If valid, put it in session and redirect to registration
        session['pending_sace_code'] = code
        
        # If they are magically already logged in (e.g. testing)
        if current_user.is_authenticated:
            return redirect(url_for('sace_bp.claim_code'))
            
        # Redirect to generic registration page, but pass subject=sace so it's free
        # and next=/sace/claim_code so they return to claim the code.
        return redirect(url_for('auth_bp.register', subject='sace', next=url_for('sace_bp.claim_code')))
        
    return render_template("program_sace/auditor_join.html")

@sace_bp.route("/sace/claim_code")
@login_required
def claim_code():
    from app.models.sace import SaceWorkshopInteraction
    import json
    
    code = session.get('pending_sace_code')
    if not code:
        flash("No pending access code to claim.", "warning")
        return redirect(url_for('sace_bp.auditor_join'))
        
    interactions = SaceWorkshopInteraction.query.filter_by(activity_slug="auditor_provisioned").all()
    found_inv = None
    for inv in interactions:
        try:
            data = json.loads(inv.response_data)
            if data.get('code') == code:
                found_inv = inv
                break
        except:
            pass
            
    if found_inv:
        data = json.loads(found_inv.response_data)
        if data.get('status') == "Unclaimed":
            data['status'] = f"Claimed"
            data['first_name'] = current_user.name or current_user.email.split('@')[0]
            data['last_name'] = ""
            data['email'] = current_user.email
            data['claimed_by_user_id'] = current_user.id
            found_inv.response_data = json.dumps(data)
            db.session.commit()
            
            # Ensure they are enrolled in sace_reading (or just clear session so they can go to the hub)
            session.pop('pending_sace_code', None)
            flash("Access Code successfully claimed. Welcome to the SACE Evaluation Hub.", "success")
            
            # Direct them instantly to the reading activity!
            return redirect(url_for('sace_bp.selection_hub', activity_slug='reading'))
            
    flash("Failed to claim code or code already used.", "error")
    return redirect(url_for('sace_bp.auditor_join'))
'''

text += join_routes

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(text)

