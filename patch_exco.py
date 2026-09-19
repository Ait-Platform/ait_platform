with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_render = """@uip_bp.route("/<org_slug>/executive-workspace")
@login_required
def exco_workspace(org_slug):
    org = g.organization
    from app.models.uip import UipResolution
    pending_resolutions = UipResolution.query.filter_by(organization_id=org.id, status="PROPOSED").count()
    return render_template("program_uip/dashboards/exco_workspace.html", org=org, pending_resolutions=pending_resolutions)"""

new_render = """@uip_bp.route("/<org_slug>/executive-workspace")
@login_required
def exco_workspace(org_slug):
    org = g.organization
    from app.models.uip import UipResolution
    from app.models.core import CoreInteraction
    
    pending_resolutions = UipResolution.query.filter_by(organization_id=org.id, status="PROPOSED").count()
    tabled_res = UipResolution.query.filter_by(organization_id=org.id, status="TABLED").count()
    
    open_claims_count = CoreInteraction.query.filter(
        CoreInteraction.organization_id == org.id,
        CoreInteraction.status == "OPEN",
        CoreInteraction.interaction_type.in_([
            "committee_claim", "ratepayer_claim", "subcommittee_claim", "mo_claim", "staff_claim", "unknown_claim"
        ])
    ).count()
    
    switch_gate = 'red' if open_claims_count > 0 else 'clear'
    if tabled_res > 0:
        switch_res = 'red'
    elif pending_resolutions > 0:
        switch_res = 'amber'
    else:
        switch_res = 'clear'
        
    return render_template(
        "program_uip/dashboards/exco_workspace.html", 
        org=org, 
        pending_resolutions=pending_resolutions,
        switch_gate=switch_gate,
        switch_res=switch_res,
        tabled_res=tabled_res,
        proposed_res=pending_resolutions,
        open_claims={"length": open_claims_count} # mock array length for the template
    )"""

if old_render in text:
    text = text.replace(old_render, new_render)
    with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
        f.write(text)
    print("Patched committee_routes.py")
else:
    print("Could not find old render in committee_routes.py")
