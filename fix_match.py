import re

filepath = 'app/program_uip/services/register.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('reference = row.get("reference")', 'reference = str(row.get("reference") or "").strip()')
content = content.replace('member_ref = row.get("member_reference")', 'member_ref = str(row.get("member_reference") or "").strip()')
content = content.replace('prop_ref = row.get("property_reference")', 'prop_ref = str(row.get("property_reference") or "").strip()')

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated match logic to strip whitespace")
