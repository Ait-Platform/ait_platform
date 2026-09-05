import re

with open('templates/admin/security/pricing.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace the option tag in the select dropdown
old_option = """<option value="{{ s.id }}" {% if s.id == selected_subject_id %}selected{% endif %}>
                        {{ s.name }}
                    </option>"""

new_option = """<option value="{{ s.id }}" {% if s.id == selected_subject_id %}selected{% endif %}>
                        {{ s.name }} (ID: {{ s.id }} | Slug: {{ s.slug }})
                    </option>"""

content = content.replace(old_option, new_option)

with open('templates/admin/security/pricing.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated admin pricing dropdown to show ID and slug")
