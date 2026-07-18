with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

new_route = """@billing_bp.route('/property/<int:property_id>/architecture_summary')
@login_required
def architecture_summary(property_id):"""

dummy_route = """@billing_bp.route('/property/<int:property_id>/email_architecture_summary', methods=['GET', 'POST'])
@login_required
def email_architecture_summary(property_id):
    flash("Email feature coming soon!", "info")
    return redirect(url_for('billing_bp.architecture_summary', property_id=property_id))

@billing_bp.route('/property/<int:property_id>/architecture_summary')
@login_required
def architecture_summary(property_id):"""

text = text.replace(new_route, dummy_route)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)
print("Created dummy email route!")
