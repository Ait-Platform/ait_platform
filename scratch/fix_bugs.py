import re

# Fix 1: Update username to name
routes_path = 'app/program_sace/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace("current_user.username", "current_user.name")

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(text)

# Fix 2: Update results.html for flash messages
html_path = 'templates/program_sace/post_test/results.html'
with open(html_path, 'r', encoding='utf-8') as f:
    html = f.read()

# Add block flashes to disable global flash rendering
if "{% block flashes %}{% endblock %}" not in html:
    html = html.replace("{% block content %}", "{% block flashes %}{% endblock %}\n{% block content %}")

# Replace the manual flash loop with the partial include
old_flash = '''<!-- Flash Messages Block -->
        <div class="px-6 pt-4">
            {% with messages = get_flashed_messages(with_categories=true) %}
                {% if messages %}
                    {% for category, message in messages %}
                        <div class="mb-4 p-4 rounded-lg {% if category == 'success' %}bg-green-100 text-green-800 border border-green-200{% else %}bg-rose-100 text-rose-800 border border-rose-200{% endif %}">
                            {{ message }}
                        </div>
                    {% endfor %}
                {% endif %}
            {% endwith %}
        </div>'''

new_flash = '''<!-- Flash Messages Block -->
        <div class="px-6 pt-4">
            {% include "partials/flash_messages.html" %}
        </div>'''

html = html.replace(old_flash, new_flash)

with open(html_path, 'w', encoding='utf-8') as f:
    f.write(html)
