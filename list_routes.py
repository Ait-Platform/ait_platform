import re
with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

matches = re.findall(r'@billing_bp\.route\([^)]+\)', text)
for m in matches:
    print(m)
