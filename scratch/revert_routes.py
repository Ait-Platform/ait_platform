import re

with open('app/program_practice_crm/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

old_logic = """        if not user:
            flash(f"No account found for {email}. Please use the WhatsApp Invite tool below to invite them to register first.", "error")
            return redirect(url_for('practice_crm_bp.staff'))"""

new_logic = """        if not user:
            # Auto-create with default password
            user = User(email=email, name=name, is_active=1)
            user.set_password('12345678')
            db.session.add(user)
            db.session.flush() # flush to get user.id
            flash(f"Successfully created a new account for {name} with default password 12345678.", "success")"""

if old_logic in content:
    content = content.replace(old_logic, new_logic)
    with open('app/program_practice_crm/routes.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Reverted routes logic")
else:
    print("Failed to find routes logic")
