import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Update manual_capture
old_manual_capture = """@billing_bp.route("/billing/onboarding/manual_capture", methods=["GET"])
@login_required
def manual_capture():
    from app.models.billing import BilArchitectureDraft
    draft_property = BilProperty.query.filter(
        BilProperty.manager_id == current_user.id,
        BilProperty.onboarding_status == 'draft_manual'
    ).first()

    if not draft_property:
        flash("No property in manual capture stage.", "warning")
        return redirect(url_for('billing_bp.learner_dashboard'))"""

new_manual_capture = """@billing_bp.route("/billing/onboarding/manual_capture", methods=["GET"])
@login_required
def manual_capture():
    from app.models.billing import BilArchitectureDraft
    property_id = request.args.get('property_id')
    
    if property_id:
        draft_property = BilProperty.query.filter_by(id=property_id, manager_id=current_user.id).first()
    else:
        # Fallback to any draft if no property_id specified (for backwards compatibility)
        draft_property = BilProperty.query.filter(
            BilProperty.manager_id == current_user.id,
            BilProperty.onboarding_status.like('draft_%')
        ).first()

    if not draft_property:
        flash("No property found to edit.", "warning")
        return redirect(url_for('billing_bp.learner_dashboard'))"""

content = content.replace(old_manual_capture, new_manual_capture)

# 2. Update save_global_architecture
# I will find the part where it deletes BilMuniAccount and inject meter cleanup.
old_save_global = """        data = request.json
        if not data:
            return jsonify({"error": "No data"}), 400

        BilMuniAccount.query.filter_by(property_id=prop.id).delete()
        
        accounts = data.get('accounts', [])"""

new_save_global = """        data = request.json
        if not data:
            return jsonify({"error": "No data"}), 400

        # Idempotent Cleanup: Wipe existing structure for this property
        old_accounts = BilMuniAccount.query.filter_by(property_id=prop.id).all()
        old_acc_nums = [a.account_number for a in old_accounts if a.account_number]
        
        if old_acc_nums:
            # Find meters tied to these accounts
            old_meters = BilMeter.query.filter(BilMeter.municipal_bill_number.in_(old_acc_nums)).all()
            old_meter_ids = [m.id for m in old_meters]
            
            if old_meter_ids:
                from app.models.billing import BilMeterReading, BilConsumption
                # Delete readings and consumptions tied to these meters
                BilMeterReading.query.filter(BilMeterReading.meter_id.in_(old_meter_ids)).delete(synchronize_session=False)
                BilConsumption.query.filter(BilConsumption.meter_id.in_(old_meter_ids)).delete(synchronize_session=False)
                # Now delete the meters
                BilMeter.query.filter(BilMeter.id.in_(old_meter_ids)).delete(synchronize_session=False)
                
        BilMuniAccount.query.filter_by(property_id=prop.id).delete(synchronize_session=False)
        
        accounts = data.get('accounts', [])"""

content = content.replace(old_save_global, new_save_global)

# 3. Prevent deleting BilArchitectureDraft
old_draft_delete = """        # Clear the draft!
        BilArchitectureDraft.query.filter_by(property_id=prop.id).delete()
        
        db.session.commit()"""

new_draft_delete = """        # Keep the draft! We don't delete BilArchitectureDraft so the wizard remains perfectly editable
        
        db.session.commit()"""

content = content.replace(old_draft_delete, new_draft_delete)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("routes.py updated for editability and idempotency.")
