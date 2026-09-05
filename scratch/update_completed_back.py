import re

file_path = 'templates/subject_reading/completed_return.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

old_back = r'<a href="\{\{ url_for\(\'auth_bp\.bridge_dashboard\'\) \}\}" class="text-indigo-600 hover:text-indigo-700 text-sm font-medium">\s*&larr; No, take me back to dashboard\s*</a>'

new_back = '''{% set is_sace = namespace(value=false) %}
      {% for s in session.get('admin_subjects', []) %}
        {% if s.startswith('sace') %}{% set is_sace.value = true %}{% endif %}
      {% endfor %}
      {% if is_sace.value %}
      <a href="{{ url_for('sace_bp.reading_hub') }}" class="text-indigo-600 hover:text-indigo-700 text-sm font-medium">
        &larr; No, take me back to SACE Hub
      </a>
      {% else %}
      <a href="{{ url_for('auth_bp.bridge_dashboard') }}" class="text-indigo-600 hover:text-indigo-700 text-sm font-medium">
        &larr; No, take me back to dashboard
      </a>
      {% endif %}'''

text = re.sub(old_back, new_back, text, flags=re.DOTALL)
with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
