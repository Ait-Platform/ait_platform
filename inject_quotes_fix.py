import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

inject_route = '''@mechanic_bp.route("/mechanic/fix_quotes", methods=["GET"])
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
    return redirect(url_for('mechanic_bp.mechanic_dashboard'))
'''

# Find the end of the imports/setup, insert the route
content = content.replace("from flask import render_template", inject_route + "\nfrom flask import render_template")

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
