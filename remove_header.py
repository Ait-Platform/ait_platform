import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    html = f.read()

pattern = r'<div class="col-span-6 bulk-col-adj">\s*Account Number\s*</div>\s*<div class="col-span-6 bulk-col-adj">\s*Owner Name\s*</div>'
replacement = '<div class="col-span-12 bulk-col-adj">Account Number</div>'

html, count = re.subn(pattern, replacement, html)
print('Replaced header', count, 'times')

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(html)
