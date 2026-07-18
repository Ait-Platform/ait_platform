import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    html = f.read()

# Add borders to account and meter inputs
html = html.replace('class="account-input w-full rounded border-slate-300', 'class="account-input w-full rounded border-2 border-blue-500')
html = html.replace('class="meter-input w-full rounded border-slate-300', 'class="meter-input w-full rounded border-2 border-blue-500')

# Add text-transform: capitalize to Step 12 inputs
html = html.replace('placeholder="Owner Name" value="${own.name}"', 'placeholder="Owner Name" value="${own.name}" style="text-transform: capitalize;"')
html = html.replace('placeholder="Billing Address" value="${addr.address}"', 'placeholder="Billing Address" value="${addr.address}" style="text-transform: capitalize;"')

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(html)
print('UI tweaks applied!')
