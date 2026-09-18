with open("templates/program_uip/dashboards/secretary_workspace.html", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace("url_for('uip_bp.committee_dashboard', org_slug=org.slug)", "url_for('uip_bp.committee_dashboard', org_slug=org.slug, view='register')")

with open("templates/program_uip/dashboards/secretary_workspace.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated links in switchboard")
