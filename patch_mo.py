import re

with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

start_idx = text.find('@uip_bp.route("/<org_slug>/mo-resolve/<int:referral_id>", methods=["POST"])')
end_idx = text.find('return redirect(url_for("uip_bp.mo_dashboard", org_slug=org.slug))', start_idx) + len('return redirect(url_for("uip_bp.mo_dashboard", org_slug=org.slug))')

if start_idx != -1:
    old_block = text[start_idx:end_idx]
    
    new_block = """@uip_bp.route("/<org_slug>/mo-action/<int:referral_id>", methods=["POST"])
@login_required
def mo_ticket_action(org_slug, referral_id):
    org = g.organization
    _require_role("municipal_officer")
    
    from app.models.uip import UipMunicipalReferral
    from app.models.core import CoreInteraction
    from app import db
    import datetime
    from flask import request, flash, redirect, url_for
    
    action = request.form.get("action")
    referral = UipMunicipalReferral.query.filter_by(id=referral_id, organization_id=org.id).first_or_404()
    master = referral.interaction
    
    if action == "acknowledge":
        referral.status = "ACKNOWLEDGED"
        master.status = "MO_ACKNOWLEDGED"
        flash(f"Master Ticket #{master.id} has been acknowledged.", "info")
    elif action == "dispatch":
        referral.status = "DISPATCHED"
        master.status = "MO_DISPATCHED"
        flash(f"City Team dispatched for Master Ticket #{master.id}.", "info")
    elif action == "resolve":
        referral.status = "RESOLVED"
        master.status = "RESOLVED"
        master.closed_at = datetime.datetime.utcnow()
        master.closed_by = current_user.id
        
        # Auto-cascade to all collated children
        if hasattr(master, "children"):
            for child in master.children:
                child.status = "RESOLVED"
                child.closed_at = datetime.datetime.utcnow()
                child.closed_by = current_user.id
                
        flash(f"Master Ticket #{master.id} and all collated public reports have been marked as resolved.", "success")
            
    db.session.commit()
    
    return redirect(url_for("uip_bp.mo_dashboard", org_slug=org.slug))"""
    
    text = text.replace(old_block, new_block)
    with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
        f.write(text)
    print("Updated routes successfully!")
else:
    print("Could not find start idx")
