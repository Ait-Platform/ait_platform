import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

bad_decorators = '''@mechanic_bp.route("/mechanic/quote/new", methods=["GET", "POST"])
@login_required
@mechanic_bp.route("/mechanic/quote/<int:id>/edit", methods=["GET", "POST"])
@login_required
def edit_quote(id):'''

good_decorators = '''@mechanic_bp.route("/mechanic/quote/<int:id>/edit", methods=["GET", "POST"])
@login_required
def edit_quote(id):'''

content = content.replace(bad_decorators, good_decorators)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
