import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

bad_block = '''@mechanic_bp.route("/mechanic/fix_quotes", methods=["GET"])
@login_required
def fix_quotes():
    from app.models.mechanic import MechClient
    clients = MechClient.query.all()
    count = 0
    for c in clients:
        c.user_id = current_user.id
        count += 1
    db.session.commit()
    flash(f"Successfully recovered and linked {count} clients/quotes to your account!", "success")
    return redirect(url_for('mechanic_bp.mechanic_dashboard'))'''

# Remove from top
content = content.replace(bad_block + '\n', '')

# Insert it after rom . import mechanic_bp
content = content.replace('from . import mechanic_bp', 'from . import mechanic_bp\n\n' + bad_block)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
