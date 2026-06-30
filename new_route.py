with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

new_route = '''
@billing_bp.route("/billing/onboarding/start_setup", methods=["POST"])
@login_required
def onboarding_start_setup():
    # Only allow if no drafts exist
    existing = BilProperty.query.filter(
        BilProperty.manager_id == current_user.id,
        BilProperty.onboarding_status.like('draft_%')
    ).first()
    
    if existing:
        flash("You already have an onboarding in progress. Please finish it first.", "error")
        return redirect(url_for('billing_bp.learner_dashboard'))
        
    prop_name = request.form.get("property_name", "Draft Property")
    bills = int(request.form.get("bills", 1))
    tenants = int(request.form.get("tenants", 1))
    is_bulk = request.form.get("is_bulk", "no")
    sub_meters = int(request.form.get("sub_meters", 0))
    
    prop = BilProperty(
        name=prop_name,
        manager_id=current_user.id,
        enrollment_id=_get_billing_enrollment_id(current_user.id),
        onboarding_status='draft_extracting',
        expected_bills=bills,
        expected_tenants=tenants,
        is_bulk_metered=(1 if is_bulk == 'yes' else 0),
        expected_sub_meters=sub_meters
    )
    
    from app.extensions import db
    db.session.add(prop)
    db.session.commit()
    
    flash("Setup initialized! You can now proceed to View Extraction.", "success")
    return redirect(url_for('billing_bp.learner_dashboard'))
'''

content = content + new_route

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
