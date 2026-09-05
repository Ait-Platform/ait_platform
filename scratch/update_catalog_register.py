import re

with open('templates/program_sace/sace_catalog.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace("url_for('auth_bp.register_decision', subject=", "url_for('auth_bp.register', subject=")

with open('templates/program_sace/sace_catalog.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated catalog links to point to register instead of register_decision")
