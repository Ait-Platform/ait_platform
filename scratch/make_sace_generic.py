import re

with open('templates/program_sace/about.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace("SACE Reading Activity | AIT Platform", "SACE Activity Hub | AIT Platform")
content = content.replace("AIT SACE Reading Activity", "AIT SACE Professional Development")
content = content.replace("central portal for the SACE Reading Activity", "central portal for AIT's SACE-approved professional development activities")

with open('templates/program_sace/about.html', 'w', encoding='utf-8') as f:
    f.write(content)

with open('templates/program_sace/sace_selection_hub.html', 'r', encoding='utf-8') as f:
    hub_content = f.read()

hub_content = hub_content.replace("SACE Reading Activity Hub", "SACE Activity Hub")

with open('templates/program_sace/sace_selection_hub.html', 'w', encoding='utf-8') as f:
    f.write(hub_content)

print("Removed 'Reading' to make SACE Hub generic")
