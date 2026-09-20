with open("templates/program_uip/dashboards/placeholder_workspace.html", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace('{% extends "program_uip/layout.html" %}', '{% extends "program_uip/base.html" %}')

with open("templates/program_uip/dashboards/placeholder_workspace.html", "w", encoding="utf-8") as f:
    f.write(text)

print("Fixed extends in placeholder_workspace.html")
