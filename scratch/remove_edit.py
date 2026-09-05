import re

routes_path = 'app/program_sace/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    text = f.read()

# Remove the edit_auditor route
old_edit = '''@sace_bp.route("/sace/provisioning/edit_auditor/<int:auditor_id>", methods=["POST"])
def edit_auditor(auditor_id):
    from app.models.sace import SaceWorkshopInteraction
    import json
    
    interaction = SaceWorkshopInteraction.query.get_or_404(auditor_id)
    
    first_name = request.form.get("first_name")
    last_name = request.form.get("last_name")
    email = request.form.get("email")
    
    if first_name and last_name and email:
        data = {
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "status": "Invite Sent (Updated)"
        }
        interaction.response_data = json.dumps(data)
        db.session.commit()
        
        # Send Updated Email
        from app.utils.mailer import send_email
    import json
        dashboard_url = url_for('sace_bp.dashboard', _external=True)
        send_email(
            subject="Updated Secure Access: SACE Auditor Dashboard",
            recipients=[email],
            body=f"Dear {first_name}, Your access details have been updated. Access dashboard at: {dashboard_url}",
            html=f"<p>Dear {first_name},</p><p>Your SACE auditor access details have been updated.</p><p><a href='{dashboard_url}'>Click here to access</a></p>"
        )
        flash(f"Auditor details updated for {email}.", "success")
        
    return redirect(url_for('sace_bp.provisioning_map'))'''

text = text.replace(old_edit, '')

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(text)
