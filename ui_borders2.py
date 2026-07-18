import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    html = f.read()

# 1. Add borders
html = html.replace(
    'class="acc-num w-full rounded border-slate-300', 
    'class="acc-num w-full rounded border-2 border-blue-500'
)
html = html.replace(
    'class="meter-num w-full rounded border-slate-300', 
    'class="meter-num w-full rounded border-2 border-blue-500'
)

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(html)
print('UI tweaks applied successfully!')
