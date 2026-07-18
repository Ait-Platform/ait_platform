import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    routes_content = f.read()

# 1. Update setup_wizard to fetch and pass draft_json
old_setup_wizard = """def setup_wizard():
    property_id = request.args.get('property_id')
    if not property_id:
        flash("Missing property ID", "error")
        return redirect(url_for('billing_bp.learner_dashboard'))
    from app.models import BilProperty
    property = BilProperty.query.get_or_404(property_id)
    if property.manager_id != current_user.id:
        from flask import abort
        abort(403)
    return render_template("program_billing/setup_wizard.html", property=property)"""

new_setup_wizard = """def setup_wizard():
    property_id = request.args.get('property_id')
    if not property_id:
        flash("Missing property ID", "error")
        return redirect(url_for('billing_bp.learner_dashboard'))
    from app.models import BilProperty
    property = BilProperty.query.get_or_404(property_id)
    if property.manager_id != current_user.id:
        from flask import abort
        abort(403)
        
    from app.models.billing import BilArchitectureDraft
    import json
    
    draft_record = BilArchitectureDraft.query.filter_by(property_id=property.id).first()
    draft_json = draft_record.draft_json if draft_record else None
    
    return render_template("program_billing/setup_wizard.html", 
        property=property,
        draft_json=json.dumps(draft_json) if draft_json else 'null')"""

if "draft_json=json.dumps" not in routes_content:
    routes_content = routes_content.replace(old_setup_wizard, new_setup_wizard)

# 2. Make save_global_architecture idempotent and keep the draft
old_save_global_middle = """        # Process global payload (owners, accounts, meters)
        owners_data = data.get('owners', [])
        accounts_data = data.get('accounts', [])
        meters_data = data.get('meters', [])

        # Process Owners"""

new_save_global_middle = """        # Process global payload (owners, accounts, meters)
        owners_data = data.get('owners', [])
        accounts_data = data.get('accounts', [])
        meters_data = data.get('meters', [])

        # Idempotent Cleanup: Wipe existing structure for this property
        old_accounts = BilMuniAccount.query.filter_by(property_id=prop.id).all()
        old_acc_nums = [a.account_number for a in old_accounts if a.account_number]
        
        if old_acc_nums:
            old_meters = BilMeter.query.filter(BilMeter.municipal_bill_number.in_(old_acc_nums)).all()
            old_meter_ids = [m.id for m in old_meters]
            if old_meter_ids:
                from app.models.billing import BilMeterReading, BilConsumption
                BilMeterReading.query.filter(BilMeterReading.meter_id.in_(old_meter_ids)).delete(synchronize_session=False)
                BilConsumption.query.filter(BilConsumption.meter_id.in_(old_meter_ids)).delete(synchronize_session=False)
                BilMeter.query.filter(BilMeter.id.in_(old_meter_ids)).delete(synchronize_session=False)
                
        BilMuniAccount.query.filter_by(property_id=prop.id).delete(synchronize_session=False)

        # Process Owners"""

if "Idempotent Cleanup" not in routes_content:
    routes_content = routes_content.replace(old_save_global_middle, new_save_global_middle)

old_save_global_end = """        prop.onboarding_status = 'draft_manual'
        
        # Clear the draft!
        BilArchitectureDraft.query.filter_by(property_id=prop.id).delete()
        
        db.session.commit()"""

new_save_global_end = """        prop.onboarding_status = 'draft_manual'
        
        # Keep the draft! We don't delete BilArchitectureDraft so the wizard remains perfectly editable
        
        db.session.commit()"""

routes_content = routes_content.replace(old_save_global_end, new_save_global_end)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(routes_content)

print("routes.py updated with editability logic.")

# 3. Update dashboard Edit button
with open('templates/program_billing/manager_dashboard.html', 'r', encoding='utf-8') as f:
    dash = f.read()

old_edit_btn = """<a href="{{ url_for('billing_bp.edit_property', property_id=row.property_id) }}\""""
new_edit_btn = """<a href="{{ url_for('billing_bp.setup_wizard', property_id=row.property_id) }}\""""

dash = dash.replace(old_edit_btn, new_edit_btn)

with open('templates/program_billing/manager_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(dash)
    
print("Dashboard Edit button updated to point to setup_wizard.")
