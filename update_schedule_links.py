import re

with open('templates/program_mechanic/dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace(
    '''<div class="font-bold text-slate-900 group-hover:text-sky-700 text-lg">Debtors Control / Schedule</div>''',
    '''<div class="font-bold text-slate-900 group-hover:text-sky-700 text-lg">Client Accounts / Ledgers</div>'''
)

with open('templates/program_mechanic/dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)

with open('templates/program_mechanic/debtors_schedule.html', 'r', encoding='utf-8') as f:
    content2 = f.read()

content2 = content2.replace(
    '''<a href="{{ url_for('mechanic_bp.mechanic_dashboard') }}" class="inline-flex items-center gap-2 rounded-lg border border-slate-200 px-4 py-2 text-sm font-semibold text-slate-700 hover:bg-slate-50 transition shadow-sm">
          <span>&larr;</span><span>Dashboard</span>
        </a>''',
    '''<a href="{{ url_for('mechanic_bp.client_accounts') }}" class="inline-flex items-center gap-2 rounded-lg border border-slate-200 px-4 py-2 text-sm font-semibold text-slate-700 hover:bg-slate-50 transition shadow-sm">
          <span>&larr;</span><span>Client Accounts</span>
        </a>'''
)

# And make the route point to debtors_schedule
content2 = content2.replace(
    '''action="{{ url_for('mechanic_bp.client_accounts') }}"''',
    '''action="{{ url_for('mechanic_bp.debtors_schedule') }}"'''
)
content2 = content2.replace(
    '''href="{{ url_for('mechanic_bp.client_accounts') }}" class="px-4 py-2 bg-white''',
    '''href="{{ url_for('mechanic_bp.debtors_schedule') }}" class="px-4 py-2 bg-white'''
)

with open('templates/program_mechanic/debtors_schedule.html', 'w', encoding='utf-8') as f:
    f.write(content2)

