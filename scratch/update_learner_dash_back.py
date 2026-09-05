import re

file_path = 'templates/subject_reading/learner_dashboard.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

old_back = r'<a href="\{\{ url_for\(\'auth_bp\.bridge_dashboard\', force=1\) \}\}" class="inline-flex items-center rounded-lg px-4 py-2 text-sm font-semibold border border-slate-200 bg-white text-slate-600 hover:bg-slate-50 transition">\s*&larr; Back\s*</a>'

new_back = '''{% set is_sace = namespace(value=false) %}
            {% for s in session.get('admin_subjects', []) %}
              {% if s.startswith('sace') %}{% set is_sace.value = true %}{% endif %}
            {% endfor %}
            {% if is_sace.value %}
            <a href="{{ url_for('sace_bp.reading_hub') }}" class="inline-flex items-center rounded-lg px-4 py-2 text-sm font-semibold border border-indigo-200 bg-indigo-50 text-indigo-700 hover:bg-indigo-100 transition">
              &larr; Back to SACE Hub
            </a>
            {% else %}
            <a href="{{ url_for('auth_bp.bridge_dashboard', force=1) }}" class="inline-flex items-center rounded-lg px-4 py-2 text-sm font-semibold border border-slate-200 bg-white text-slate-600 hover:bg-slate-50 transition">
              &larr; Back
            </a>
            {% endif %}'''

text = re.sub(old_back, new_back, text, flags=re.DOTALL)
with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
