import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('class="exc-rep-id', 'class="exc-replacement-id')

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(content)

print("Fixed exception dropdown class mismatch.")
