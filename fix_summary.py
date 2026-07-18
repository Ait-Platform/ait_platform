with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

old_func = """def architecture_summary(property_id):
    prop = BilProperty.query.get_or_404(property_id)
    if prop.manager_id != current_user.id:
        flash("Unauthorized access.", "error")
        return redirect(url_for('billing_bp.learner_dashboard'))
    
    # Gather data for summary
    accounts = BilMuniAccount.query.filter_by(property_id=prop.id).all()"""

new_func = """def architecture_summary(property_id):
    prop = BilProperty.query.get_or_404(property_id)
    if prop.manager_id != current_user.id:
        flash("Unauthorized access.", "error")
        return redirect(url_for('billing_bp.learner_dashboard'))
    
    # Gather data for summary
    from app.models.billing import BilMuniAccount
    accounts = BilMuniAccount.query.filter_by(property_id=prop.id).all()"""

text = text.replace(old_func, new_func)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)
print("Updated architecture_summary!")
