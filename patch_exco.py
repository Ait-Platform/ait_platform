with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

old_exco = """@uip_bp.route("/<org_slug>/executive-workspace")
@login_required
def exco_workspace(org_slug):
    org = g.organization
    return render_template("program_uip/dashboards/exco_workspace.html", org=org)"""

new_exco = """@uip_bp.route("/<org_slug>/executive-workspace")
@login_required
def exco_workspace(org_slug):
    org = g.organization
    from app.models.uip import UipResolution
    pending_resolutions = UipResolution.query.filter_by(organization_id=org.id, status="PROPOSED").count()
    return render_template("program_uip/dashboards/exco_workspace.html", org=org, pending_resolutions=pending_resolutions)"""

if old_exco in text:
    text = text.replace(old_exco, new_exco)
    with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
        f.write(text)
    print("Patched exco_workspace route")
else:
    print("Could not find old_exco")
