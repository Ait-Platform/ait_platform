import re

with open('templates/program_debtors/dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

old_back = '''        <div class="flex justify-between items-center mb-4">
          <h1 class="text-2xl font-bold text-slate-800">Debtors & Statements</h1>
          {% if request.args.get('source') == 'mechanic' %}
          <a href="{{ url_for('mechanic_bp.mechanic_dashboard') }}" class="px-4 py-2 bg-gray-200 text-gray-700 rounded hover:bg-gray-300 font-semibold text-sm shadow-sm transition">Back</a>
          {% else %}
          <a href="{{ url_for('auth_bp.bridge_dashboard', force=1) }}" class="px-4 py-2 bg-gray-200 text-gray-700 rounded hover:bg-gray-300 font-semibold text-sm shadow-sm transition">Back</a>
          {% endif %}
        </div>'''

new_back = '''        <div class="flex justify-between items-center mb-4">
          <h1 class="text-2xl font-bold text-slate-800">Debtors & Statements</h1>
          {% if current_user.has_role('mechanic') or request.args.get('source') == 'mechanic' %}
          <a href="{{ url_for('mechanic_bp.mechanic_dashboard') }}" class="px-4 py-2 bg-gray-200 text-gray-700 rounded hover:bg-gray-300 font-semibold text-sm shadow-sm transition">Back</a>
          {% else %}
          <a href="{{ url_for('auth_bp.bridge_dashboard', force=1) }}" class="px-4 py-2 bg-gray-200 text-gray-700 rounded hover:bg-gray-300 font-semibold text-sm shadow-sm transition">Back</a>
          {% endif %}
        </div>'''

content = content.replace(old_back, new_back)

with open('templates/program_debtors/dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)
