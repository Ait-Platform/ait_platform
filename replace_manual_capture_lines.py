with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Find start of manual_capture
start_idx = -1
for i, line in enumerate(lines):
    if 'def manual_capture' in line:
        start_idx = i - 2 # Include the decorators @billing_bp.route and @login_required
        break

# Find end of manual_capture (before edit_draft)
end_idx = -1
for i in range(start_idx+1, len(lines)):
    if 'def edit_draft' in lines[i]:
        end_idx = i - 2 # Include the decorators
        break

if start_idx != -1 and end_idx != -1:
    new_code = """@billing_bp.route("/billing/onboarding/manual_capture", methods=["GET"])
@login_required
def manual_capture():
    draft_property = BilProperty.query.filter(
        BilProperty.manager_id == current_user.id,
        BilProperty.onboarding_status == 'draft_manual'
    ).first()

    if not draft_property:
        flash("No property in manual capture stage.", "warning")
        return redirect(url_for('billing_bp.learner_dashboard'))

    accounts = BilMuniAccount.query.filter_by(property_id=draft_property.id).all()
    return render_template("program_billing/manual_capture.html", property=draft_property, accounts=accounts)

@billing_bp.route("/billing/onboarding/save_account", methods=["POST"])
@login_required
def save_account():
    import json
    from datetime import datetime
    from dateutil.relativedelta import relativedelta
    
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
    muni_acc.valuation = float(request.form.get("valuation") or 0.0)
    muni_acc.rates_amount = float(request.form.get("rates_amount") or 0.0)
    muni_acc.arrangement_amount = float(request.form.get("arr_amount") or 0.0)
    muni_acc.arrangement_duration = int(request.form.get("arr_dur") or 0)
    
    db.session.flush()

    # 3. Meters
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
                    
                    try:
                        prev_date = datetime.strptime(m_data.get('prev_date'), "%Y-%m-%d").date()
                        curr_date = datetime.strptime(m_data.get('curr_date'), "%Y-%m-%d").date()
                    except:
                        prev_date = datetime.utcnow().date() - relativedelta(days=30)
                        curr_date = datetime.utcnow().date()
                        
                    prev_read = float(m_data.get('prev_read') or 0.0)
                    curr_read = float(m_data.get('curr_read') or 0.0)
                    usage = curr_read - prev_read if curr_read > prev_read else 0
                    
                    cons = BilConsumption(
                        meter_id=meter.id,
                        meter_number=meter.meter_number,
                        month=curr_date.strftime("%Y-%m"),
                        last_date=prev_date,
                        new_date=curr_date,
                        last_read=prev_read,
                        new_read=curr_read,
                        days=(curr_date - prev_date).days or 30,
                        consumption=usage
                    )
                    db.session.add(cons)

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
                        
                    try:
                        prev_date = datetime.strptime(m_data.get('prev_date'), "%Y-%m-%d").date()
                        curr_date = datetime.strptime(m_data.get('curr_date'), "%Y-%m-%d").date()
                    except:
                        prev_date = datetime.utcnow().date() - relativedelta(days=30)
                        curr_date = datetime.utcnow().date()
                        
                    prev_read = float(m_data.get('prev_read') or 0.0)
                    curr_read = float(m_data.get('curr_read') or 0.0)
                    usage = curr_read - prev_read if curr_read > prev_read else 0
                    
                    cons = BilConsumption(
                        meter_id=meter.id,
                        meter_number=meter.meter_number,
                        month=curr_date.strftime("%Y-%m"),
                        last_date=prev_date,
                        new_date=curr_date,
                        last_read=prev_read,
                        new_read=curr_read,
                        days=(curr_date - prev_date).days or 30,
                        consumption=usage
                    )
                    db.session.add(cons)

    db.session.commit()
    return "OK", 200

@billing_bp.route("/billing/onboarding/finalize_setup/<int:property_id>", methods=["POST"])
@login_required
def finalize_setup(property_id):
    prop = BilProperty.query.filter_by(id=property_id, manager_id=current_user.id).first()
    if prop and prop.onboarding_status == 'draft_manual':
        prop.onboarding_status = "draft_collating"
        db.session.commit()
        flash("Manual Capture Complete. Proceeding to Collation.", "success")
    return redirect(url_for('billing_bp.learner_dashboard'))

"""
    lines[start_idx:end_idx] = [new_code]
    with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
        f.writelines(lines)
    print("Successfully replaced manual_capture and added endpoints.")
else:
    print(f"Could not find boundaries. start_idx: {start_idx}, end_idx: {end_idx}")
