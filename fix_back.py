import sys

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

target = '''<a href="{{ url_for('mechanic_bp.mechanic_dashboard') }}" class="inline-flex items-center gap-2 rounded-lg border border-slate-200 px-4 py-2 text-sm font-semibold text-slate-700 hover:bg-slate-50 transition shadow-sm">
        <span>&larr;</span><span>Back</span>
      </a>'''

new_target = '''<a href="{{ url_for('mechanic_bp.job_cards_list') }}" class="inline-flex items-center gap-2 rounded-lg border border-slate-200 px-4 py-2 text-sm font-semibold text-slate-700 hover:bg-slate-50 transition shadow-sm">
        <span>&larr;</span><span>Back</span>
      </a>'''

content = content.replace(target, new_target)

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
