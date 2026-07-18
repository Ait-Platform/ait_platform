import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    html = f.read()

# 1. Account Input
html = re.sub(
    r'(<input type=\"text\" class=\"account-input.*?)(border-slate-400)',
    r'\1border-2 border-blue-500',
    html
)

# 2. Meter Input
html = re.sub(
    r'(<input type=\"text\" class=\"meter-input.*?)(border-slate-400)',
    r'\1border-2 border-blue-500',
    html
)

# 3. Capitalize Owner Name and Address
html = re.sub(
    r'(placeholder=\"Owner Name\".*?value=\"\$\{own\.name\}\")',
    r'\1 style="text-transform: capitalize;"',
    html
)

html = re.sub(
    r'(placeholder=\"Billing Address\".*?value=\"\$\{addr\.address\}\")',
    r'\1 style="text-transform: capitalize;"',
    html
)

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(html)
print('UI tweaks applied via regex!')
