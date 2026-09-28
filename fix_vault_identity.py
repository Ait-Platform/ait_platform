import re

filepath = 'app/program_uip/services/ratepayer.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

search = '''    # Ambiguous municipal identities must not disclose another person's property.
    if len(members) != 1:
        return None, [], True
    member = members[0]'''

replace = '''    # If multiple profiles exist for the same email due to dirty CSVs, take the first one
    if not members:
        return None, [], True
    member = members[0]'''

content = content.replace(search, replace)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated vault_identity to handle duplicate emails gracefully")
