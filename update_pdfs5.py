import re

with open('templates/program_debtors/soa_template.html', 'r', encoding='utf-8') as f:
    content2 = f.read()

content2 = content2.replace(r"\'n/a\'", "'n/a'")

with open('templates/program_debtors/soa_template.html', 'w', encoding='utf-8') as f:
    f.write(content2)
