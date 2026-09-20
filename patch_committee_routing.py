with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

import re

old_routing = """    if current_appointment and current_appointment.position.lower() == "secretary":
        # Allow them to view the register if they explicitly clicked the tile
        if request.args.get("view") != "register":
            return redirect(url_for("uip_bp.secretary_workspace", org_slug=org.slug))"""

new_routing = """    if current_appointment:
        pos = current_appointment.position.lower()
        if request.args.get("view") != "register":
            if pos == "secretary":
                return redirect(url_for("uip_bp.secretary_workspace", org_slug=org.slug))
            elif pos in ["chairperson", "chairman"]:
                return redirect(url_for("uip_bp.chairman_workspace", org_slug=org.slug))
            elif pos in ["vice-chairperson", "vice chairman"]:
                return redirect(url_for("uip_bp.vice_chair_workspace", org_slug=org.slug))
            elif pos == "treasurer":
                return redirect(url_for("uip_bp.treasurer_workspace", org_slug=org.slug))"""

text = text.replace(old_routing, new_routing)

# Add the 3 new route functions at the end of the file
new_routes = """
@uip_bp.route("/<org_slug>/chairman-workspace")
@login_required
def chairman_workspace(org_slug):
    org = g.organization
    return render_template("program_uip/dashboards/placeholder_workspace.html", org=org, role_title="Chairman", role_desc="Oversee the entire precinct.")

@uip_bp.route("/<org_slug>/vice-chair-workspace")
@login_required
def vice_chair_workspace(org_slug):
    org = g.organization
    return render_template("program_uip/dashboards/placeholder_workspace.html", org=org, role_title="Vice-Chairman", role_desc="Support the Chairman in precinct oversight.")

@uip_bp.route("/<org_slug>/treasurer-workspace")
@login_required
def treasurer_workspace(org_slug):
    org = g.organization
    return render_template("program_uip/dashboards/placeholder_workspace.html", org=org, role_title="Treasurer", role_desc="Manage financial health, budgets, and procurement.")
"""

text += new_routes

with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
    f.write(text)

print("Updated committee routing and added 3 new dashboard endpoints")
