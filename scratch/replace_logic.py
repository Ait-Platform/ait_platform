import re

with open('app/program_practice_crm/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

old_logic = """        if not user:
            if not password or len(password) < 8:
                flash(f"A temporary password of at least 8 characters is required to create a new account for {email}.", "error")
                return redirect(url_for('practice_crm_bp.staff'))
            
            user = User(email=email, name=name, is_active=1)
            user.set_password(password)
            db.session.add(user)
            db.session.flush() # flush to get user.id
            flash(f"Successfully created a new account for {name}.", "success")"""

new_logic = """        if not user:
            flash(f"No account found for {email}. Please use the WhatsApp Invite tool below to invite them to register first.", "error")
            return redirect(url_for('practice_crm_bp.staff'))"""

if old_logic in content:
    content = content.replace(old_logic, new_logic)
    with open('app/program_practice_crm/routes.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Replaced logic in routes.py")
else:
    print("Failed to find logic in routes.py")
