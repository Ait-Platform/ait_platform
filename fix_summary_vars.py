with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

old_func = """@billing_bp.route('/property/<int:property_id>/architecture_summary')
@login_required
def architecture_summary(property_id):
    prop = BilProperty.query.get_or_404(property_id)
    if prop.manager_id != current_user.id:
        flash("Unauthorized access.", "error")
        return redirect(url_for('billing_bp.learner_dashboard'))
    
    # Gather data for summary
    from app.models.billing import BilMuniAccount
    accounts = BilMuniAccount.query.filter_by(property_id=prop.id).all()
    return render_template('program_billing/architecture_summary.html', property=prop, accounts=accounts)"""

new_func = """@billing_bp.route('/property/<int:property_id>/architecture_summary')
@login_required
def architecture_summary(property_id):
    prop = BilProperty.query.get_or_404(property_id)
    if prop.manager_id != current_user.id:
        flash("Unauthorized access.", "error")
        return redirect(url_for('billing_bp.learner_dashboard'))
    
    # Gather data for summary
    from app.models.billing import BilMuniAccount, BilMeter, RefMuniOwner
    accounts = BilMuniAccount.query.filter_by(property_id=prop.id).all()
    
    acc_nums = [a.account_number for a in accounts if a.account_number]
    if acc_nums:
        meters = BilMeter.query.filter(BilMeter.municipal_bill_number.in_(acc_nums)).all()
    else:
        meters = []

    bulk_water = []
    bulk_elec = []
    sub_water = []
    sub_elec = []
    exceptions = [] # Exceptions not fully implemented in db yet
    
    owner_ids = list(set([a.owner_id for a in accounts if a.owner_id]))
    if owner_ids:
        owners = RefMuniOwner.query.filter(RefMuniOwner.id.in_(owner_ids)).all()
    else:
        owners = []
    
    for m in meters:
        # Determine bulk vs sub
        acc = next((a for a in accounts if a.account_number == m.municipal_bill_number), None)
        is_bulk = acc.is_bulk_account if acc else False
        
        if 'water' in (m.utility_type or '').lower():
            if is_bulk:
                bulk_water.append(m)
            else:
                sub_water.append(m)
        else:
            if is_bulk:
                bulk_elec.append(m)
            else:
                sub_elec.append(m)

    return render_template('program_billing/architecture_summary.html', 
                           property=prop, 
                           accounts=accounts,
                           bulk_water=bulk_water,
                           bulk_elec=bulk_elec,
                           sub_water=sub_water,
                           sub_elec=sub_elec,
                           exceptions=exceptions,
                           owners=owners)"""

text = text.replace(old_func, new_func)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)
print("Updated architecture_summary variables!")
