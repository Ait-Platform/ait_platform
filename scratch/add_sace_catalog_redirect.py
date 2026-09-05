import re

with open('app/program_sace/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

old_code = """@sace_bp.route('/sace/catalog')
def catalog():"""

new_code = """@sace_bp.route('/sace/catalog')
def catalog():
    from flask_login import current_user
    from flask import redirect, url_for
    if getattr(current_user, 'is_authenticated', False) and current_user.email == 'nan@gmail.com':
        return redirect(url_for('sace_bp.dashboard'))
"""

text = text.replace(old_code, new_code)

with open('app/program_sace/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)
print("Added SACE pre-registered catalog redirect")
