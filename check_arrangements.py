with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    text = f.read()

import re
m = re.search(r'class="arg-agreement(.*?)\>', text, re.DOTALL)
if m: print("agreement:", m.group(0))

m2 = re.search(r'class="arg-installment(.*?)\>', text, re.DOTALL)
if m2: print("installment:", m2.group(0))

m3 = re.search(r'class="arg-owing(.*?)\>', text, re.DOTALL)
if m3: print("owing:", m3.group(0))
