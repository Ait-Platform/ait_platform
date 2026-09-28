import re

filepath = 'app/program_uip/services/register.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

search_prop = '''    values = {
        "reference": text(data, "reference", 50, True), "address": text(data, "address", 500, True),
        "rates_reference": text(data, "rates_reference", 100),
        "classification": choice(data, "classification", ("residential", "business", "mixed", "other")),
        "is_active": boolean(data, "is_active"),
    }'''

replace_prop = '''    values = {
        "reference": text(data, "reference", 50, True), "address": text(data, "address", 500, True),
        "rates_reference": text(data, "rates_reference", 100),
        "classification": choice(data, "classification", ("residential", "business", "mixed", "other"), default="residential"),
        "is_active": boolean(data, "is_active", default=True),
    }'''
content = content.replace(search_prop, replace_prop)

search_rel = '''        "relationship": choice(data, "relationship", ("owner", "tenant", "managing_agent", "occupant")),
        "is_verified": boolean(data, "is_verified"),'''
replace_rel = '''        "relationship": choice(data, "relationship", ("owner", "tenant", "managing_agent", "occupant"), default="owner"),
        "is_verified": boolean(data, "is_verified", default=True),'''
content = content.replace(search_rel, replace_rel)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated property and relationship parsers")
