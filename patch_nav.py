import re
with open("templates/program_uip/navigation.html", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace('<a href="{{ url_for(\'uip_bp.secretary_workspace\', org_slug=org.slug) }}"><i class="fas fa-inbox w-5"></i> Intake Desk</a>', '<a href="{{ url_for(\'uip_bp.secretary_workspace\', org_slug=org.slug) }}"><i class="fas fa-toggle-on w-5"></i> Switchboard</a>\n    <a href="{{ url_for(\'uip_bp.secretary_intake\', org_slug=org.slug) }}"><i class="fas fa-inbox w-5"></i> Intake Desk</a>')

with open("templates/program_uip/navigation.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated navigation.html")
