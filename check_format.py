with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    text = f.read()

import re
m = re.search(r'class="rate-market-value(.*?)\>', text, re.DOTALL)
if m: print(m.group(0))

m2 = re.search(r'class="arr-amount(.*?)\>', text, re.DOTALL)
if m2: print(m2.group(0))
