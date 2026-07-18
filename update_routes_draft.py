with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

import re

new_routes = """
@billing_bp.route("/billing/onboarding/edit_draft/<int:property_id>", methods=["POST"])
@login_required
def edit_draft(property_id):
    prop = BilProperty.query.filter_by(id=property_id, manager_id=current_user.id).first()
    if prop and prop.onboarding_status.startswith('draft_'):
        prop.name = request.form.get("property_name", prop.name)
        prop.expected_bills = int(request.form.get("expected_bills") or prop.expected_bills)
        prop.expected_tenants = int(request.form.get("expected_tenants") or prop.expected_tenants)
        prop.is_bulk_metered = int(request.form.get("is_bulk_metered") or prop.is_bulk_metered)
        prop.expected_sub_meters = int(request.form.get("expected_sub_meters") or prop.expected_sub_meters)
        db.session.commit()
        flash("Draft property setup updated successfully.", "success")
    return redirect(url_for('billing_bp.learner_dashboard'))

@billing_bp.route("/billing/onboarding/delete_draft/<int:property_id>", methods=["POST"])
@login_required
def delete_draft(property_id):
    prop = BilProperty.query.filter_by(id=property_id, manager_id=current_user.id).first()
    if prop and prop.onboarding_status.startswith('draft_'):
        db.session.delete(prop)
        db.session.commit()
        flash("Draft property setup deleted.", "info")
    return redirect(url_for('billing_bp.learner_dashboard'))
"""

if "def edit_draft" not in content:
    content += new_routes
    with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Added edit_draft and delete_draft routes.")
else:
    print("Routes already exist.")
