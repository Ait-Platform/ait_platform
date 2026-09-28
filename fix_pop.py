import re

filepath = 'app/program_uip/services/register.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

search = '''    if is_import and member_id is not None:
        # Protect operational fields from being overwritten by absent municipal fields
        values.pop("email", None)
        values.pop("phone", None)
        values.pop("eligibility_status", None)'''

replace = '''    if is_import and member_id is not None and not is_authoritative:
        # Protect operational fields from being overwritten by absent municipal fields
        values.pop("email", None)
        values.pop("phone", None)
        values.pop("eligibility_status", None)'''

content = content.replace(search, replace)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated save_member to allow Vault to overwrite emails")
