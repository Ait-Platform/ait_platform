import re

file_path = 'templates/program_sace/reading_hub.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

# Add empty flashes block
if "{% block flashes %}{% endblock %}" not in text:
    text = text.replace('{% block title %}Sace Authorised User(s) Map{% endblock %}', '{% block title %}Sace Authorised User(s) Map{% endblock %}\n{% block flashes %}{% endblock %}')

# Replace custom flash code with partial
custom_flash = '''                {% with messages = get_flashed_messages(with_categories=true) %}
                  {% if messages %}
                    <div class="mt-4">
                    {% for category, message in messages %}
                      <div class="p-3 rounded bg-red-100 text-red-700 border border-red-200 text-sm font-bold">
                        {{ message }}
                      </div>
                    {% endfor %}
                    </div>
                  {% endif %}
                {% endwith %}'''

partial_flash = '''                <div class="mt-4">
                  {% include "partials/flash_messages.html" %}
                </div>'''

text = text.replace(custom_flash, partial_flash)

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
