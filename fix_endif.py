import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

replacement = '''          <div class="flex items-center justify-end gap-2 flex-wrap">
            {% if job_card.status == 'Quote' %}
                <a href="{{ url_for('mechanic_bp.edit_quote', id=job_card.id) }}" class="px-4 py-2 bg-slate-800 text-white font-bold rounded-lg hover:bg-slate-900 shadow-sm transition text-sm mr-2">Edit Quote</a>
            {% endif %}'''

content = re.sub(
    r"<div class=\"flex items-center justify-end gap-2 flex-wrap\">\s*\{% if job_card\.status == 'Quote' %\}\s*<a href=\"\{\{ url_for\('mechanic_bp\.edit_quote', id=job_card\.id\) \}\}\" class=\"px-4 py-2 bg-slate-800 text-white font-bold rounded-lg hover:bg-slate-900 shadow-sm transition text-sm mr-2\">Edit Quote</a>\s*",
    replacement,
    content
)

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
