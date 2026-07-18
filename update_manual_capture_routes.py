with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

import re

# Update manual_capture GET route
old_manual_capture = """def manual_capture():
    draft_property = BilProperty.query.filter(
        BilProperty.manager_id == current_user.id,
        BilProperty.onboarding_status == 'draft_manual'
    ).first()

    if not draft_property:
        flash("No property in manual capture stage.", "warning")
        return redirect(url_for('billing_bp.learner_dashboard'))

    accounts = BilMuniAccount.query.filter_by(property_id=draft_property.id).all()
    return render_template("program_billing/manual_capture.html", property=draft_property, accounts=accounts)"""

new_manual_capture = """def manual_capture():
    draft_property = BilProperty.query.filter(
        BilProperty.manager_id == current_user.id,
        BilProperty.onboarding_status == 'draft_manual'
    ).first()

    if not draft_property:
        flash("No property in manual capture stage.", "warning")
        return redirect(url_for('billing_bp.learner_dashboard'))

    accounts = BilMuniAccount.query.filter_by(property_id=draft_property.id).all()
    
    # Get all bulk meters for the dropdown
    bulk_accounts = [acc.account_number for acc in accounts if acc.is_bulk_account and acc.account_number]
    bulk_meters = []
    if bulk_accounts:
        bulk_meters = BilMeter.query.filter(BilMeter.municipal_bill_number.in_(bulk_accounts)).all()
        
    return render_template("program_billing/manual_capture.html", property=draft_property, accounts=accounts, bulk_meters=bulk_meters)"""

content = content.replace(old_manual_capture, new_manual_capture)

# Update save_account POST route
old_save_account_start = """    muni_acc.account_number = acc_number
    if owner:
        muni_acc.owner_id = owner.id
    # Note: Phase 1 does not save financials anymore!
    
    db.session.flush()

    # 3. Meters (Structural Only)
    meters_payload = request.form.get("meters_payload")"""

new_save_account_start = """    muni_acc.account_number = acc_number
    muni_acc.is_bulk_account = request.form.get("is_bulk_account") == 'true'
    if owner:
        muni_acc.owner_id = owner.id
    # Note: Phase 1 does not save financials anymore!
    
    db.session.flush()

    # 3. Meters (Structural Only)
    meters_payload = request.form.get("meters_payload")"""
content = content.replace(old_save_account_start, new_save_account_start)

# Update the parsing of meters to use the parent_meter_id directly if 'linked_bulk'
old_meters_parse = """        # Process Bulk first
        for m_data in meters_data:
            if m_data.get("assignment") == "bulk":
                m_number = m_data.get("meter_number", "").strip()
                if m_number:
                    meter = BilMeter.query.filter_by(meter_number=m_number).first()
                    if not meter:
                        meter = BilMeter(
                            meter_number=m_number, 
                            utility_type=m_data.get("utility_type"), 
                            pointing_to="Bulk Supply", 
                            municipal_bill_number=acc_number,
                            status=m_data.get("status")
                        )
                        db.session.add(meter)
                        db.session.flush()
                    else:
                        meter.status = m_data.get("status")
                        meter.municipal_bill_number = acc_number
                        
                    bulk_map[m_data.get("utility_type")] = meter.id
                    # No readings generated here anymore!

        # Process Linked
        for m_data in meters_data:
            if m_data.get("assignment") == "linked":
                m_number = m_data.get("meter_number", "").strip()
                if m_number:
                    meter = BilMeter.query.filter_by(meter_number=m_number).first()
                    if not meter:
                        meter = BilMeter(
                            meter_number=m_number, 
                            utility_type=m_data.get("utility_type"), 
                            pointing_to="Sub-Unit", 
                            municipal_bill_number=acc_number,
                            status=m_data.get("status"),
                            parent_meter_id=bulk_map.get(m_data.get("utility_type"))
                        )
                        db.session.add(meter)
                        db.session.flush()
                    else:
                        meter.status = m_data.get("status")
                        meter.municipal_bill_number = acc_number
                        meter.parent_meter_id = bulk_map.get(m_data.get("utility_type"))
                    # No readings generated here anymore!"""

new_meters_parse = """        for m_data in meters_data:
            m_number = m_data.get("meter_number", "").strip()
            if m_number:
                meter = BilMeter.query.filter_by(meter_number=m_number).first()
                parent_id = None
                assignment = m_data.get("assignment", "independent")
                pointing_to = "Sub-Unit"
                
                if assignment == "bulk_supply":
                    pointing_to = "Bulk Supply"
                elif assignment.startswith("linked_bulk_"):
                    # Extract the ID of the bulk meter it is linked to
                    try:
                        parent_id = int(assignment.replace("linked_bulk_", ""))
                    except:
                        pass
                elif assignment == "stolen_exception":
                    pointing_to = "Stolen Exception"

                if not meter:
                    meter = BilMeter(
                        meter_number=m_number, 
                        utility_type=m_data.get("utility_type"), 
                        pointing_to=pointing_to, 
                        municipal_bill_number=acc_number,
                        status=m_data.get("status"),
                        parent_meter_id=parent_id
                    )
                    db.session.add(meter)
                    db.session.flush()
                else:
                    meter.status = m_data.get("status")
                    meter.municipal_bill_number = acc_number
                    meter.parent_meter_id = parent_id
                    meter.pointing_to = pointing_to
                
                # Handle exceptional replacement linking
                replacement_for = m_data.get("replacement_for")
                if replacement_for:
                    # In the UI, the replacement meter specifies the stolen meter's ID or number it replaces
                    # For simplicity, if we get the stolen meter number, we query it and link.
                    stolen = BilMeter.query.filter_by(meter_number=replacement_for).first()
                    if stolen:
                        meter.replacement_for_meter_id = stolen.id
                        stolen.replacement_for_meter_id = meter.id # mutual link for easy querying
"""
content = content.replace(old_meters_parse, new_meters_parse)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated manual_capture GET and POST routes.")
