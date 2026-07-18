import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace save_global_architecture
old_func = """@billing_bp.route('/save_global_architecture/<int:property_id>', methods=['POST'])
@login_required
def save_global_architecture(property_id):
    try:
        prop = BilProperty.query.get_or_404(property_id)
        if prop.manager_id != current_user.id:
            return jsonify({"error": "Unauthorized"}), 403

        data = request.json
        if not data:
            return jsonify({"error": "No data"}), 400

        # 1. Clear existing accounts and meters for this draft property
        BilMuniAccount.query.filter_by(property_id=prop.id).delete()
        
        accounts = data.get('accounts', [])
        water_meters = data.get('water_meters', [])
        elec_meters = data.get('elec_meters', [])
        mapping = data.get('mapping', [])

        db_meters = {}
        
        for m in water_meters + elec_meters:
            meter = BilMeter(
                meter_number=m['number'],
                utility_type=m['type'],
                status='active',
                pointing_to='Unassigned'
            )
            db.session.add(meter)
            db.session.flush()
            db_meters[m['id']] = meter

        for acc_data in accounts:
            acc_map = next((x for x in mapping if x['account_id'] == acc_data['id']), None)
            if not acc_map: continue
            
            owner_name = acc_data['owner']
            owner = None
            if owner_name:
                owner = RefMuniOwner.query.filter_by(name=owner_name).first()
                if not owner:
                    owner = RefMuniOwner(name=owner_name)
                    db.session.add(owner)
                    db.session.flush()

            muni_acc = BilMuniAccount(
                property_id=prop.id,
                account_number=acc_data['number'],
                is_bulk_account=acc_data['isBulk'],
                owner_id=owner.id if owner else None
            )
            db.session.add(muni_acc)
            db.session.flush()

            if acc_map['isBulk']:
                for w_id in acc_map.get('water', []):
                    if w_id in db_meters:
                        db_meters[w_id].municipal_bill_number = muni_acc.account_number
                        db_meters[w_id].pointing_to = 'Bulk Supply'
                for e_id in acc_map.get('elec', []):
                    if e_id in db_meters:
                        db_meters[e_id].municipal_bill_number = muni_acc.account_number
                        db_meters[e_id].pointing_to = 'Bulk Supply'
            else:
                for w_id in acc_map.get('water', []):
                    if w_id in db_meters:
                        db_meters[w_id].municipal_bill_number = muni_acc.account_number
                        db_meters[w_id].pointing_to = 'Sub-Unit'
                for e_id in acc_map.get('elec', []):
                    if e_id in db_meters:
                        db_meters[e_id].municipal_bill_number = muni_acc.account_number
                        db_meters[e_id].pointing_to = 'Sub-Unit'

                for exc in acc_map.get('exceptions', []):
                    stolen_num = exc['stolen_num']
                    rep_id = exc['replacement_id']
                    if rep_id in db_meters:
                        stolen_meter = BilMeter(
                            meter_number=stolen_num,
                            utility_type=db_meters[rep_id].utility_type,
                            status='stolen',
                            municipal_bill_number=muni_acc.account_number,
                            replacement_for_meter_id=db_meters[rep_id].id,
                            pointing_to='Stolen Exception'
                        )
                        db.session.add(stolen_meter)
                        db.session.flush()
                        db_meters[rep_id].replacement_for_meter_id = stolen_meter.id

        prop.onboarding_status = 'draft_manual'
        db.session.commit()
        
        return jsonify({"message": "Architecture saved successfully!"}), 200
    except Exception as e:
        db.session.rollback()
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500"""

new_func = """@billing_bp.route('/save_global_architecture/<int:property_id>', methods=['POST'])
@login_required
def save_global_architecture(property_id):
    try:
        prop = BilProperty.query.get_or_404(property_id)
        if prop.manager_id != current_user.id:
            return jsonify({"error": "Unauthorized"}), 403

        data = request.json
        if not data:
            return jsonify({"error": "No data"}), 400

        BilMuniAccount.query.filter_by(property_id=prop.id).delete()
        
        accounts = data.get('accounts', [])
        bulkWater = data.get('bulkWater', [])
        bulkElec = data.get('bulkElec', [])
        subWater = data.get('subWater', [])
        subElec = data.get('subElec', [])
        exceptions = data.get('exceptions', [])
        mapping = data.get('mapping', [])

        db_meters = {}
        
        # 1. Create Bulk Meters
        for m in bulkWater:
            meter = BilMeter(meter_number=m['number'], utility_type='water', status='active', pointing_to='Bulk Supply')
            db.session.add(meter)
            db.session.flush()
            db_meters[m['id']] = meter
            
        for m in bulkElec:
            meter = BilMeter(meter_number=m['number'], utility_type='electricity', status='active', pointing_to='Bulk Supply')
            db.session.add(meter)
            db.session.flush()
            db_meters[m['id']] = meter

        # 2. Create Sub Meters
        for m in subWater:
            meter = BilMeter(meter_number=m['number'], utility_type='water', status='active', pointing_to='Sub-Unit')
            db.session.add(meter)
            db.session.flush()
            db_meters[m['id']] = meter
            
        for m in subElec:
            meter = BilMeter(meter_number=m['number'], utility_type='electricity', status='active', pointing_to='Sub-Unit')
            db.session.add(meter)
            db.session.flush()
            db_meters[m['id']] = meter

        # 3. Create Accounts & Map
        for acc_data in accounts:
            owner_name = acc_data['owner']
            owner = None
            if owner_name:
                owner = RefMuniOwner.query.filter_by(name=owner_name).first()
                if not owner:
                    owner = RefMuniOwner(name=owner_name)
                    db.session.add(owner)
                    db.session.flush()

            muni_acc = BilMuniAccount(
                property_id=prop.id,
                account_number=acc_data['number'],
                is_bulk_account=acc_data['isBulk'],
                owner_id=owner.id if owner else None
            )
            db.session.add(muni_acc)
            db.session.flush()

            if acc_data['isBulk']:
                # Attach all bulk meters to this account
                for m_id, meter in db_meters.items():
                    if m_id.startswith('bulk-'):
                        meter.municipal_bill_number = muni_acc.account_number
            else:
                # Sub Account - use mapping
                acc_map = next((x for x in mapping if x['account_id'] == acc_data['id']), None)
                if acc_map:
                    w_id = acc_map.get('water')
                    if w_id and w_id in db_meters:
                        db_meters[w_id].municipal_bill_number = muni_acc.account_number
                    e_id = acc_map.get('elec')
                    if e_id and e_id in db_meters:
                        db_meters[e_id].municipal_bill_number = muni_acc.account_number
                        
        # 4. Handle Exceptions (Stolen Meters)
        for exc in exceptions:
            stolen_num = exc['stolen_num']
            rep_id = exc['replacement_id']
            if rep_id in db_meters:
                rep_meter = db_meters[rep_id]
                # Inherit the municipal account from the replacement meter
                stolen_meter = BilMeter(
                    meter_number=stolen_num,
                    utility_type=rep_meter.utility_type,
                    status='stolen',
                    municipal_bill_number=rep_meter.municipal_bill_number,
                    replacement_for_meter_id=rep_meter.id,
                    pointing_to='Stolen Exception'
                )
                db.session.add(stolen_meter)
                db.session.flush()
                rep_meter.replacement_for_meter_id = stolen_meter.id

        prop.onboarding_status = 'draft_manual'
        db.session.commit()
        
        return jsonify({"message": "Architecture saved successfully!"}), 200
    except Exception as e:
        db.session.rollback()
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500"""

content = content.replace(old_func, new_func)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("Updated save route for new JSON payload")
