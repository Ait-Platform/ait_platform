import re

# 1. Add route to app/program_billing/routes.py
with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

summary_route = """
@billing_bp.route('/property/<int:property_id>/architecture_summary')
@login_required
def architecture_summary(property_id):
    prop = BilProperty.query.get_or_404(property_id)
    if prop.manager_id != current_user.id:
        flash("Unauthorized access.", "error")
        return redirect(url_for('billing_bp.learner_dashboard'))
    
    # Gather data for summary
    accounts = BilMuniAccount.query.filter_by(property_id=prop.id).all()
    
    # We also need bulk meters and sub meters mapped to accounts
    # This is a basic rendering for the user's records.
    return render_template('program_billing/architecture_summary.html', property=prop, accounts=accounts)
"""

if "def architecture_summary" not in content:
    content += summary_route
    with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Added architecture_summary route.")

# 2. Update the redirect in setup_wizard.html
with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    content = f.read()

old_redirect = "window.location.href = \"{{ url_for('billing_bp.learner_dashboard') }}\";"
new_redirect = "window.location.href = \"{{ url_for('billing_bp.architecture_summary', property_id=property.id) }}\";"

if old_redirect in content:
    content = content.replace(old_redirect, new_redirect)
    with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Updated redirect in setup_wizard.html")

