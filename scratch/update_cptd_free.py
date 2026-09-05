import re

with open('app/auth/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('url_for("cptd_bp.hub")', 'url_for("sace_bp.selection_hub")')

with open('app/auth/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
