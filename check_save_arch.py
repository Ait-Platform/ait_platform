with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    text = f.read()

import re
m = re.search(r'async function saveArchitecture\(\) \{.*?\n  \}', text, re.DOTALL)
if m: print(m.group(0))
