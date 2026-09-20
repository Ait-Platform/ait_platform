with open("templates/program_uip/dashboards/secretary_organogram.html", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace(r"url_for(\'uip_bp.secretary_organogram\'", r"url_for('uip_bp.secretary_organogram'")

with open("templates/program_uip/dashboards/secretary_organogram.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Fixed template syntax error")
