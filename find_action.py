import re
with open('templates/program_billing/checkout_summary.html', 'r', encoding='utf-8') as f:
    text = f.read()

m = re.search(r'action="\{\{ url_for\((.*?)\) \}\}"', text)
if m:
    print(m.group(1))
