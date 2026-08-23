import re

with open('templates/program_mechanic/dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace(
    '''<div class="font-bold text-slate-900 group-hover:text-sky-700 text-lg">Debtors Schedule</div>''',
    '''<div class="font-bold text-slate-900 group-hover:text-sky-700 text-lg">Debtors Accounts</div>'''
)

with open('templates/program_mechanic/dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)
