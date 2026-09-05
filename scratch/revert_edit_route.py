import re

with open('app/program_practice_crm/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

old_edit = """        new_pw = request.form.get("password", "")
        if new_pw:
            if len(new_pw) < 8:
                flash("Password must be at least 8 characters.", "error")
                return redirect(url_for('practice_crm_bp.staff'))
            user.set_password(new_pw)"""

if old_edit in content:
    content = content.replace(old_edit, "")
    with open('app/program_practice_crm/routes.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Removed from routes")
else:
    print("Not found in routes")
