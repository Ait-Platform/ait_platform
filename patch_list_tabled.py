import re
with open("templates/program_uip/dashboards/committee.html", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace("'ESCALATED'", "'TABLED'")
text = text.replace("Escalated", "Tabled")

with open("templates/program_uip/dashboards/committee.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated committee.html for TABLED flow")
