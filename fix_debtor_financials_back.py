import re

with open('templates/program_debtors/debtor_financials.html', 'r', encoding='utf-8') as f:
    content = f.read()

old_back = '''<a href="{{ url_for('debtors_bp.dashboard') }}" class="px-4 py-2 bg-gray-200 text-gray-700 rounded hover:bg-gray-300 font-semibold text-sm shadow-sm transition">Back</a>'''
new_back = '''{% if current_user.has_role('mechanic') %}
                    <a href="{{ url_for('mechanic_bp.job_cards_list') }}" class="px-4 py-2 bg-gray-200 text-gray-700 rounded hover:bg-gray-300 font-semibold text-sm shadow-sm transition">Back</a>
                    {% else %}
                    <a href="{{ url_for('debtors_bp.dashboard') }}" class="px-4 py-2 bg-gray-200 text-gray-700 rounded hover:bg-gray-300 font-semibold text-sm shadow-sm transition">Back</a>
                    {% endif %}'''

content = content.replace(old_back, new_back)

with open('templates/program_debtors/debtor_financials.html', 'w', encoding='utf-8') as f:
    f.write(content)
