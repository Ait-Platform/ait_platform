import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    html = f.read()

# Debug prints to see exactly what we're replacing
acc_match = re.search(r'<input type="text" class="account-input[^"]*"', html)
if acc_match:
    print('Found account input:', acc_match.group(0))
    # Replace border-slate-300 or border-slate-400 with border-2 border-blue-500
    new_class = re.sub(r'border-slate-[34]00', 'border-2 border-blue-500', acc_match.group(0))
    html = html.replace(acc_match.group(0), new_class)

meter_match = re.search(r'<input type="text" class="meter-input[^"]*"', html)
if meter_match:
    print('Found meter input:', meter_match.group(0))
    new_class = re.sub(r'border-slate-[34]00', 'border-2 border-blue-500', meter_match.group(0))
    html = html.replace(meter_match.group(0), new_class)

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(html)
print('UI borders applied!')
