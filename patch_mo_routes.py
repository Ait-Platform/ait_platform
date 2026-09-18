import re

with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_func = """@uip_bp.route("/<org_slug>/mo-resolve/<int:referral_id>", methods=["POST"])
@login_required
def mo_resolve_ticket(org_slug, referral_id):
    org = g.organization
    _require_role("municipal_officer")
    
    from app.models.uip import UipMunicipalReferral
    from app.models.core import CoreInteraction
    from app import db
    import datetime
    
    referral = UipMunicipalReferral.query.filter_by(id=referral_id, organization_id=org.id).first_or_404()
    referral.status = "RESOLVED"
    
    # Cascade to master ticket
    master = referral.interaction
    master.status = "RESOLVED"
    master.closed_at = datetime.datetime.utcnow()
    master.closed_by = current_user.id
    
    # Auto-cascade to all collated children
    if hasattr(master, "children"):
        for child in master.children:
            child.status = "RESOLVED"
            child.closed_at = datetime.datetime.utcnow()
            child.closed_by = current_user.id
            
    db.session.commit()
    flash(f"Master Ticket #{master.id} and all collated public reports have been marked as resolved.", "success")
    
    return redirect(url_for("uip_bp.mo_dashboard", org_slug=org.slug))"""

new_func = """@uip_bp.route("/<org_slug>/mo-action/<int:referral_id>", methods=["POST"])
@login_required
def mo_ticket_action(org_slug, referral_id):
    org = g.organization
    _require_role("municipal_officer")
    
    from app.models.uip import UipMunicipalReferral
    from app.models.core import CoreInteraction
    from app import db
    import datetime
    
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

if old_func in text:
    text = text.replace(old_func, new_func)
    with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
        f.write(text)
    print("Updated MO routing logic successfully")
else:
    print("Could not find old MO routing block")
