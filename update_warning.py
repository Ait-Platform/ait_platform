import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Update the warning message
content = content.replace(
    'flash("Quote Confirmed! WARNING: Client has no email address. Please print and hand them the Tax Invoice manually.", "warning")',
    'flash("Quote Confirmed! WARNING: Client has no email address. Please WhatsApp or Print the Tax Invoice manually to remain legally compliant.", "warning")'
)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
