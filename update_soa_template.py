import re

with open('templates/program_debtors/soa_template.html', 'r', encoding='utf-8') as f:
    content = f.read()

back_original = '''<a href="{{ url_for('debtors_bp.debtor_view', debtor_id=debtor.id) }}" class="px-4 py-2 bg-gray-200 text-gray-700 rounded hover:bg-gray-300 font-semibold text-sm shadow-sm transition">Back</a>'''

back_new = '''{% if return_url %}
          <a href="{{ return_url }}" class="px-4 py-2 bg-gray-200 text-gray-700 rounded hover:bg-gray-300 font-semibold text-sm shadow-sm transition">Back</a>
        {% else %}
          <a href="{{ url_for('debtors_bp.debtor_view', debtor_id=debtor.id) }}" class="px-4 py-2 bg-gray-200 text-gray-700 rounded hover:bg-gray-300 font-semibold text-sm shadow-sm transition">Back</a>
        {% endif %}'''

content = content.replace(back_original, back_new)

with open('templates/program_debtors/soa_template.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated soa_template.html")
