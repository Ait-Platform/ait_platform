import re

filepath = 'app/program_uip/services/audit.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

search = '''    if import_id and action in {"member.created", "member.updated", "property.created", "property.updated", "ownership.created", "ownership.updated"}:'''

replace = '''    if import_id and action in {"member.created", "member.updated", "property.created", "property.updated", "ownership.created", "ownership.updated", "preference.updated"}:'''

content = content.replace(search, replace)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated audit to allow MOs to update preferences")
