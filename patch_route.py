with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

# Add route
route_str = """
@uip_bp.route("/<org_slug>/executive-workspace")
@login_required
def exco_workspace(org_slug):
    org = g.organization
    return render_template("program_uip/dashboards/exco_workspace.html", org=org)
"""

if "def exco_workspace" not in text:
    text = text + "\n" + route_str
    with open("app/program_uip/committee_routes.py", "w", encoding="utf-8") as f:
        f.write(text)
    print("Added exco_workspace route")
