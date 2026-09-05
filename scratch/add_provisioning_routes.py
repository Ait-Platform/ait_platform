import re

file_path = 'app/program_sace/routes.py'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

provisioning_routes = '''
@sace_bp.route("/sace/provisioning")
@login_required
def provisioning_map():
    from app.models.sace import SaceWorkshopInteraction
    import json
    
    # Check if pledged
    pledge = SaceWorkshopInteraction.query.filter_by(user_id=current_user.id, activity_slug="admin_patent_pledge").first()
    has_pledged = pledge is not None
    
    # Load provisioned auditors
    invites = SaceWorkshopInteraction.query.filter_by(user_id=current_user.id, activity_slug="auditor_provisioned").order_by(SaceWorkshopInteraction.timestamp.desc()).all()
    
    auditors = []
    for inv in invites:
        try:
            data = json.loads(inv.response_data)
            data['date'] = inv.timestamp.strftime("%Y-%m-%d")
            auditors.append(data)
        except Exception:
            pass
            
    return render_template("program_sace/provisioning_map.html", has_pledged=has_pledged, auditors=auditors)

@sace_bp.route("/sace/provisioning/pledge", methods=["POST"])
@login_required
def provisioning_pledge():
    from app.models.sace import SaceWorkshopInteraction
    pledge = SaceWorkshopInteraction.query.filter_by(user_id=current_user.id, activity_slug="admin_patent_pledge").first()
    if not pledge:
        interaction = SaceWorkshopInteraction(
            user_id=current_user.id,
            activity_slug="admin_patent_pledge",
            response_data="Admin accepted IP pledge"
        )
        db.session.add(interaction)
        db.session.commit()
        flash("Intellectual Property pledge accepted. Provisioning unlocked.", "success")
    return redirect(url_for('sace_bp.provisioning_map'))

@sace_bp.route("/sace/provisioning/add_auditor", methods=["POST"])
@login_required
def provision_auditor():
    from app.models.sace import SaceWorkshopInteraction
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
        db.session.commit()
        flash(f"Auditor {first_name} {last_name} has been provisioned. Access email sent to {email}.", "success")
    else:
        flash("All fields are required to provision an auditor.", "error")
        
    return redirect(url_for('sace_bp.provisioning_map'))
'''

if 'def provisioning_map():' not in text:
    # Add it before audit_report
    text = text.replace('@sace_bp.route("/sace/audit_report")', provisioning_routes + '\n\n@sace_bp.route("/sace/audit_report")')

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
