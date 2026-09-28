import re

filepath = 'app/program_uip/services/register.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# Add last_import_id to member
content = content.replace('member.is_active = True', 'member.is_active = True\n            member.last_import_id = batch.id')

# Add last_import_id to prop
content = content.replace('prop.is_active = True', 'prop.is_active = True\n            prop.last_import_id = batch.id')

# Add last_import_id to link
content = content.replace('link.is_verified = True', 'link.is_verified = True\n                link.last_import_id = batch.id')

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Added last_import_id")
