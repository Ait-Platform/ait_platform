import re

with open('templates/program_mechanic/client_ledger.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('name="amount" step="0.01"', 'name="amount" id="amount_input" step="0.01"')

with open('templates/program_mechanic/client_ledger.html', 'w', encoding='utf-8') as f:
    f.write(content)
