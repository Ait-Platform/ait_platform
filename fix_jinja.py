import re
with open("templates/program_uip/dashboards/resolution_view.html", "r", encoding="utf-8") as f:
    text = f.read()

# Fix the syntax error
text = text.replace("{% else %}\n            {% elif resolution.status == 'DRAFT' %}", "{% elif resolution.status == 'DRAFT' %}")

with open("templates/program_uip/dashboards/resolution_view.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Fixed Jinja syntax error")
