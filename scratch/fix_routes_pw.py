import re

with open('app/program_practice_crm/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Fix staff creation
old_create = """        if not user:
            if not password:
                flash(f"A password is required to create a new account for {email}.", "error")
                return redirect(url_for('practice_crm_bp.staff'))"""

new_create = """        if not user:
            if not password or len(password) < 8:
                flash(f"A temporary password of at least 8 characters is required to create a new account for {email}.", "error")
                return redirect(url_for('practice_crm_bp.staff'))"""

content = content.replace(old_create, new_create)

# Fix staff edit
old_edit = """    user = User.query.get(pu.user_id)
    if user:
        user.name = request.form.get("name", "").strip()
        pu.phone = request.form.get("phone", "").strip()
        db.session.commit()
        flash("Receptionist details updated.", "success")"""

new_edit = """    user = User.query.get(pu.user_id)
    if user:
        user.name = request.form.get("name", "").strip()
        pu.phone = request.form.get("phone", "").strip()
        
        new_pw = request.form.get("password", "")
        if new_pw:
            if len(new_pw) < 8:
                flash("Password must be at least 8 characters.", "error")
                return redirect(url_for('practice_crm_bp.staff'))
            user.set_password(new_pw)
            
        db.session.commit()
        flash("Receptionist details updated.", "success")"""

content = content.replace(old_edit, new_edit)

with open('app/program_practice_crm/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated routes.py")
