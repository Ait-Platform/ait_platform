import re

filepath = 'templates/program_uip/dashboards/resolution_view.html'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# Change Live Ratification Desk to Mandate Recording Desk
content = content.replace('Live Ratification Desk', 'Mandate Recording Desk')

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("resolution_view updated")
