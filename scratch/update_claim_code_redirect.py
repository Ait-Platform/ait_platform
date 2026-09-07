import re

routes_path = 'app/program_sace/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    text = f.read()

old_redirect = "return redirect(url_for('sace_bp.selection_hub', activity_slug='reading'))"
new_redirect = "return redirect(url_for('sace_bp.reading_hub'))"

text = text.replace(old_redirect, new_redirect)

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(text)
