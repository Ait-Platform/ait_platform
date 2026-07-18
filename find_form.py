import re
with open('templates/program_billing/checkout_summary.html', 'r', encoding='utf-8') as f:
    text = f.read()

m = re.search(r'<form action="\{\{ url_for.*?billing_unlock.*?</form>', text, re.DOTALL)
if m:
    print(m.group(0))
