import re

# Fix auth/routes.py
with open('app/auth/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

old_auth = """    # SACE Pre-Registered Personnel / Evaluator Override
    if email == 'nan@gmail.com' or 'sace' in admin_subjects:
        return redirect(url_for("sace_bp.dashboard"))"""

new_auth = """    # SACE Pre-Registered Personnel / Evaluator Override
    # Dynamically check if the user is an admin for any SACE subject
    is_sace_admin = any(s.startswith('sace') for s in session.get("admin_subjects", []))
    if is_sace_admin:
        return redirect(url_for("sace_bp.dashboard"))"""

text = text.replace(old_auth, new_auth)
with open('app/auth/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)

# Fix program_sace/routes.py
with open('app/program_sace/routes.py', 'r', encoding='utf-8') as f:
    text2 = f.read()

old_sace = """    if getattr(current_user, 'is_authenticated', False) and current_user.email == 'nan@gmail.com':"""

new_sace = """    if getattr(current_user, 'is_authenticated', False):
        from flask import session
        is_sace_admin = any(s.startswith('sace') for s in session.get("admin_subjects", []))
        if is_sace_admin:"""

text2 = text2.replace(old_sace, new_sace)
with open('app/program_sace/routes.py', 'w', encoding='utf-8') as f:
    f.write(text2)

print("Removed hardcoded email and implemented dynamic DB checks")
