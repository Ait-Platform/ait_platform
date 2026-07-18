import re
with open('templates/program_billing/architecture_summary.html', 'r', encoding='utf-8') as f:
    text = f.read()

headings = re.findall(r'<h2[^>]*>(.*?)</h2>', text)
for h in headings:
    print(h)
