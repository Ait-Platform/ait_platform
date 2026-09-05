import re

file_path = 'templates/public/welcome.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

# Add SACE slugs to the hidden list
old_hide = '''    {# Hide admin systems #}
    {% if slug not in [
        "admin",
        "admin_general",
        "sms",
        "staff"
    ] %}'''

new_hide = '''    {# Hide admin systems #}
    {% if slug not in [
        "admin",
        "admin_general",
        "sms",
        "staff",
        "sace_cptd",
        "sace_evaluator",
        "sace_facilitator",
        "sace_participant"
    ] %}'''

text = text.replace(old_hide, new_hide)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)
