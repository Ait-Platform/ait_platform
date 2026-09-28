import re

filepath = 'templates/program_uip/dashboards/secretary_workspace.html'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace("Processed / Historical Claims", "Queries Resolved Register (Access Claims)")
content = content.replace("Date Processed", "Resolved Timestamp")

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)

print("Updated workspace HTML")
