with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

old_finalize = """@billing_bp.route("/billing/onboarding/finalize_setup/<int:property_id>", methods=["POST"])
@login_required
def finalize_setup(property_id):
    prop = BilProperty.query.filter_by(id=property_id, manager_id=current_user.id).first()
    if prop and prop.onboarding_status == 'draft_manual':
        prop.onboarding_status = "draft_collating"
        db.session.commit()
        flash("Manual Capture Complete. Proceeding to Collation.", "success")
    return redirect(url_for('billing_bp.learner_dashboard'))"""

new_finalize = """@billing_bp.route("/billing/onboarding/finalize_setup/<int:property_id>", methods=["POST"])
@login_required
def finalize_setup(property_id):
    prop = BilProperty.query.filter_by(id=property_id, manager_id=current_user.id).first()
    if prop and prop.onboarding_status == 'draft_manual':
        prop.onboarding_status = "draft_readings"
        db.session.commit()
        flash("Architecture Map Finalized. Please proceed to Initial Readings Capture.", "success")
    return redirect(url_for('billing_bp.learner_dashboard'))"""

content = content.replace(old_finalize, new_finalize)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated finalize_setup.")
