import re

init_path = 'app/uip/__init__.py'
with open(init_path, 'r', encoding='utf-8') as f:
    text = f.read()

old_patches = '''        ("core_interaction", "assigned_to INTEGER"),
        ("core_interaction", "closed_by INTEGER"),
        ("core_interaction", "channel VARCHAR(50)"),
        ("core_interaction", "category VARCHAR(100)")
    ]'''

new_patches = '''        ("core_interaction", "assigned_to INTEGER"),
        ("core_interaction", "closed_by INTEGER"),
        ("core_interaction", "channel VARCHAR(50)"),
        ("core_interaction", "category VARCHAR(100)"),
        ("core_interaction", "interaction_type VARCHAR(50)"),
        ("core_interaction", "priority VARCHAR(50)"),
        ("core_interaction", "reference VARCHAR(50)")
    ]'''

text = text.replace(old_patches, new_patches)

with open(init_path, 'w', encoding='utf-8') as f:
    f.write(text)
