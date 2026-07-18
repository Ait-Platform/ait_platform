import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    text = f.read()

m = re.search(r'<input[^>]*rate-market-value[^>]*>', text)
if m: print('Market Value:', m.group(0))

m2 = re.search(r'<input[^>]*rate-gen-randage[^>]*>', text)
if m2: print('Gen Randage:', m2.group(0))
