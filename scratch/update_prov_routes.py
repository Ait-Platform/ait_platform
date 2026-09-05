import re

routes_path = 'app/program_sace/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    routes = f.read()

# Add edit auditor route and update provision_auditor email logic
auditor_route = '''@sace_bp.route("/sace/provisioning/add_auditor", methods=["POST"])
@login_required
def provision_auditor():
    from app.models.sace import SaceWorkshopInteraction
    from app.utils.mailer import send_email
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
        interaction = SaceWorkshopInteraction(
            user_id=current_user.id,
            activity_slug="auditor_provisioned",
            response_data=json.dumps(data)
        )
        db.session.add(interaction)
        db.session.flush()
        from app.models.core import CoreAuditEvent
        db.session.add(CoreAuditEvent(
            user_id=current_user.id,
            action="SACE_AUDITOR_PROVISIONED",
            entity_type="SaceWorkshopInteraction",
            entity_id=interaction.id,
            details=f"SACE Admin provisioned auditor: {first_name} {last_name} ({email})"
        ))
        db.session.commit()
        
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
        
    return redirect(url_for('sace_bp.provisioning_map'))

@sace_bp.route("/sace/provisioning/edit_auditor/<int:auditor_id>", methods=["POST"])
@login_required
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
        dashboard_url = url_for('sace_bp.dashboard', _external=True)
        send_email(
            subject="Updated Secure Access: SACE Auditor Dashboard",
            recipients=[email],
            body=f"Dear {first_name}, Your access details have been updated. Access dashboard at: {dashboard_url}",
            html=f"<p>Dear {first_name},</p><p>Your SACE auditor access details have been updated.</p><p><a href='{dashboard_url}'>Click here to access</a></p>"
        )
        flash(f"Auditor details updated for {email}.", "success")
        
    return redirect(url_for('sace_bp.provisioning_map'))
'''

# We need to replace the old provision_auditor with the new one
old_provision_auditor_regex = re.compile(r'@sace_bp\.route\("/sace/provisioning/add_auditor", methods=\["POST"\]\).*?return redirect\(url_for\(\'sace_bp\.provisioning_map\'\)\)', re.DOTALL)
routes = old_provision_auditor_regex.sub(auditor_route, routes)

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(routes)
