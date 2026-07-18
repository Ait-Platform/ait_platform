with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Find start of save_account
start_idx = -1
for i, line in enumerate(lines):
    if 'def save_account' in line:
        start_idx = i - 2
        break

# Find end of save_account (before finalize_setup)
end_idx = -1
for i in range(start_idx+1, len(lines)):
    if 'def finalize_setup' in lines[i]:
        end_idx = i - 2
        break

new_code = """@billing_bp.route("/billing/onboarding/save_account", methods=["POST"])
@login_required
def save_account():
    import json
    
    property_id = request.form.get("property_id")
    account_id = request.form.get("account_id")
    
    prop = BilProperty.query.filter_by(id=property_id, manager_id=current_user.id).first()
    if not prop:
        return "Unauthorized", 403

    owner_name = request.form.get("owner_name", "").strip()
    acc_number = request.form.get("account_number", "").strip()
    
    # 1. Owner
    owner = None
    if owner_name:
        owner = RefMuniOwner.query.filter_by(name=owner_name).first()
        if not owner:
            owner = RefMuniOwner(name=owner_name)
            db.session.add(owner)
            db.session.flush()

    # 2. Account
    if account_id:
        muni_acc = BilMuniAccount.query.get(account_id)
    else:
        muni_acc = BilMuniAccount.query.filter_by(account_number=acc_number, property_id=prop.id).first()
        if not muni_acc:
            muni_acc = BilMuniAccount(property_id=prop.id)
            db.session.add(muni_acc)
            
    muni_acc.account_number = acc_number
    if owner:
        muni_acc.owner_id = owner.id
    # Note: Phase 1 does not save financials anymore!
    
    db.session.flush()

    # 3. Meters (Structural Only)
    meters_payload = request.form.get("meters_payload")
    if meters_payload:
        meters_data = json.loads(meters_payload)
        bulk_map = {}
        
        # Process Bulk first
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
                    # No readings generated here anymore!

    db.session.commit()
    return "OK", 200

"""

if start_idx != -1 and end_idx != -1:
    lines[start_idx:end_idx] = [new_code]
    with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
        f.writelines(lines)
    print("Replaced save_account for Phase 1.")
else:
    print("Could not find save_account bounds.")
