import re

with open('app/auth/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('url_for("sace_bp.selection_hub")', 'url_for("sace_bp.catalog")')
content = content.replace("url_for('sace_bp.selection_hub')", "url_for('sace_bp.catalog')")

with open('app/auth/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated auth routing to point to catalog")
