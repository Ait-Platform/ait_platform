import re

with open('templates/program_sace/sace_catalog.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace(
    "url_for('auth_bp.register', subject='sace_' ~ activity.slug)",
    "url_for('quote_bp.quote', subject='sace_' ~ activity.slug)"
)

with open('templates/program_sace/sace_catalog.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated catalog links to point to quote")
