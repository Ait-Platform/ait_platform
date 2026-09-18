import re
with open("templates/program_uip/dashboards/resolution_view.html", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace("{{ resolution.created_at.strftime('%Y-%m-%d') }}", "{% if resolution.created_at %}{{ resolution.created_at.strftime('%Y-%m-%d') }}{% else %}Pending Date{% endif %}")

with open("templates/program_uip/dashboards/resolution_view.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated resolution_view.html")
