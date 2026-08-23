import re

with open('templates/program_mechanic/dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace(
    '''<div class="font-bold text-slate-900 group-hover:text-sky-700 text-lg">Client Accounts (SOA)</div>''',
    '''<div class="font-bold text-slate-900 group-hover:text-sky-700 text-lg">Debtors Control / Schedule</div>'''
)
with open('templates/program_mechanic/dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)

with open('templates/program_mechanic/client_ledger.html', 'r', encoding='utf-8') as f:
    content2 = f.read()
content2 = content2.replace(
    '''<span>All Accounts</span>''',
    '''<span>Debtors Schedule</span>'''
)
with open('templates/program_mechanic/client_ledger.html', 'w', encoding='utf-8') as f:
    f.write(content2)
