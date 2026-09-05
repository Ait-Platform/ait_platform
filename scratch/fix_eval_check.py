import re

with open('app/program_sace/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

old_code = """    # 1. If Evaluator/Admin, route them to the main Hub
    # Safe admin check:
    is_eval = False
    for r in current_user.user_roles:
        if r.role and r.role.slug == 'admin':
            is_eval = True
            break
            
    if is_eval:
        return redirect(url_for('sace_bp.reading_hub'))"""

new_code = """    # 1. If Evaluator/Admin, route them to the main Hub
    from app.models.auth import ApprovedAdmin
    is_eval = ApprovedAdmin.query.filter(db.func.lower(ApprovedAdmin.email) == current_user.email.lower()).first() is not None
            
    if is_eval:
        return redirect(url_for('sace_bp.reading_hub'))"""

content = content.replace(old_code, new_code)

with open('app/program_sace/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Fixed evaluator check to use ApprovedAdmin")
