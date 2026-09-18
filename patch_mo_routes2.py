import re

with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_mo = """@uip_bp.route("/<org_slug>/mo-dashboard")
@login_required
def mo_dashboard(org_slug):
    _require_role("municipal_officer")
    return render_template("program_uip/dashboards/municipal_officer.html", org=g.organization)"""

new_mo = """@uip_bp.route("/<org_slug>/mo-dashboard")
@login_required
def mo_dashboard(org_slug):
    org = g.organization
    _require_role("municipal_officer")
    
    from app.models.uip import UipMunicipalReferral
    from app.models.core import CoreInteraction
    escalations = UipMunicipalReferral.query.filter_by(
        organization_id=org.id,
        status="ESCALATED_TO_MO"
    ).all()
    
    return render_template("program_uip/dashboards/municipal_officer.html", org=org, escalations=escalations)

@uip_bp.route("/<org_slug>/mo-resolve/<int:referral_id>", methods=["POST"])
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
    from flask import flash, redirect, url_for
    flash(f"Ticket {master.id} resolved. Auto-cascaded response to all affected ratepayers.", "success")
    return redirect(url_for("uip_bp.mo_dashboard", org_slug=org.slug))"""

text = text.replace(old_mo, new_mo)

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated mo routes")
