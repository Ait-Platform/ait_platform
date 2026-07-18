with open('templates/program_billing/architecture_summary.html', 'r', encoding='utf-8') as f:
    text = f.read()

import re
m = re.search(r'<a href="\{\{ url_for\(\'billing_bp\.email_architecture_summary(.*?)\</a\>', text, re.DOTALL)
if m: print(m.group(0))
