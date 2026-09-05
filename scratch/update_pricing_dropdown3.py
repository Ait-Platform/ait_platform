import re

with open('templates/admin/security/pricing.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace(
    '<option value="{{ s.id }}" {% if s.id == selected_subject_id %}selected{% endif %}>{{ s.name }}</option>',
    '<option value="{{ s.id }}" {% if s.id == selected_subject_id %}selected{% endif %}>{{ s.name }} (ID: {{ s.id }} | Slug: {{ s.slug }})</option>'
)

with open('templates/admin/security/pricing.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated admin pricing dropdown")
