with open("templates/program_uip/dashboards/secretary_intake.html", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace('<form method="POST" action="{{ url_for(\'uip_bp.draft_access_resolution\', org_slug=org.slug) }}">', '<form id="draftResForm" method="POST" action="{{ url_for(\'uip_bp.draft_access_resolution\', org_slug=org.slug) }}">')

with open("templates/program_uip/dashboards/secretary_intake.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Added form ID")
