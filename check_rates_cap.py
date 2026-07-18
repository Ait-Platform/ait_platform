with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    text = f.read()

import re
m = re.search(r'function buildRatesDashboard.*?\} *\n *\}', text, re.DOTALL)
if m:
    for line in m.group(0).split('\n'):
        if '<input type="text"' in line and 'rate-' in line:
            print(line.strip())
