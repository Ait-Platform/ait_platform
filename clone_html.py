with open("templates/program_uip/dashboards/secretary_workspace.html", "r", encoding="utf-8") as f:
    html = f.read()

# 1. Update the headers
html = html.replace("Secretary Dashboard", "Chairman Dashboard")
html = html.replace("Secretary Dashboard - {{ org.name }}", "Chairman Dashboard - {{ org.name }}")

# 2. Update the Resolutions tile link to point to chairman_voting_room
html = html.replace("url_for('uip_bp.committee_dashboard', org_slug=org.slug, view='register')", "url_for('uip_bp.chairman_voting_room', org_slug=org.slug)")

# Note: The Intake desk and Organogram links currently point to secretary views still (secretary_intake, secretary_organogram). 
# This is fine for now as a "clone" because we are just providing the same grid. We can clone those downstream later if the user requests.

with open("templates/program_uip/dashboards/chairman_workspace.html", "w", encoding="utf-8") as f:
    f.write(html)

print("Created chairman_workspace.html clone")
