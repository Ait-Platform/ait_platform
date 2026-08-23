import re

with open('templates/program_debtors/soa_template.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('class="w-full object-cover"', 'class="w-full object-contain" style="max-height: 120px;"')

with open('templates/program_debtors/soa_template.html', 'w', encoding='utf-8') as f:
    f.write(content)
