import re

filepath = 'app/program_uip/services/register.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# Fix choice function to have a default
search_choice = '''def choice(data, key, values):
    value = data.get(key)
    if value not in values:
        raise InvalidRegisterOption(key, value, values)
    return value'''

replace_choice = '''def choice(data, key, values, default=None):
    value = data.get(key)
    if not value and default is not None:
        return default
    if value and isinstance(value, str):
        value = value.strip().lower()
    if value not in values:
        if default is not None:
            return default
        raise InvalidRegisterOption(key, value, values)
    return value'''

content = content.replace(search_choice, replace_choice)

# Fix boolean function to be forgiving
search_bool = '''def boolean(data, key):
    return choice(data, key, ("true", "false")) == "true"'''

replace_bool = '''def boolean(data, key, default=True):
    val = data.get(key)
    if not val:
        return default
    val = str(val).strip().lower()
    if val in ('true', 't', 'yes', 'y', '1'):
        return True
    if val in ('false', 'f', 'no', 'n', '0'):
        return False
    return default'''

content = content.replace(search_bool, replace_bool)

# Update save_member to use defaults
search_save_member = '''    values = {
        "reference": text(data, "reference", 50, True), "name": text(data, "name", 255, True),
        "member_type": choice(data, "member_type", ("person", "business")),
        "email": text(data, "email", 255), "phone": text(data, "phone", 50),
        "is_active": boolean(data, "is_active"),
        "eligibility_status": choice(data, "eligibility_status", ("unverified", "eligible", "ineligible")),
    }'''

replace_save_member = '''    values = {
        "reference": text(data, "reference", 50, True), "name": text(data, "name", 255, True),
        "member_type": choice(data, "member_type", ("person", "business"), default="person"),
        "email": text(data, "email", 255), "phone": text(data, "phone", 50),
        "is_active": boolean(data, "is_active", default=True),
        "eligibility_status": choice(data, "eligibility_status", ("unverified", "eligible", "ineligible"), default="eligible"),
    }'''

content = content.replace(search_save_member, replace_save_member)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated register parsers to be much more forgiving")
