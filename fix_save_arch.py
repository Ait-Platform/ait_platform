import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

# I will replace the entire save_global_architecture function.
old_func_pattern = r'@billing_bp\.route\("/property/<int:property_id>/wizard/save_global_architecture", methods=\["POST"\]\)\n@login_required\ndef save_global_architecture\(property_id\):.*?(?=@billing_bp\.route|\Z)'

m = re.search(old_func_pattern, text, re.DOTALL)
if not m:
    print("Could not find save_global_architecture!")
else:
    old_func = m.group(0)
    
    new_func = """@billing_bp.route("/property/<int:property_id>/wizard/save_global_architecture", methods=["POST"])
@login_required
def save_global_architecture(property_id):
    try:
        from app.models.billing import BilArchitectureDraft
        prop = BilProperty.query.get_or_404(property_id)
        if prop.manager_id != current_user.id:
            return jsonify({"error": "Unauthorized"}), 403

        data = request.json
        if not data:
            return jsonify({"error": "No data provided"}), 400

        from app.extensions import db
        from app.models import BilMuniAccount, RefMuniOwner, BilMeter
        from datetime import datetime
        
        # 1. Clean up old architecture
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

        # 2. Process Owners
        owner_map = {} # acc.id (e.g. 'acc_0') -> owner_obj.id
        for o_data in data.get('owners', []):
            name = o_data.get('name', '').strip()
            acc_id = o_data.get('account_id')
            if name and acc_id:
                owner = RefMuniOwner.query.filter_by(name=name).first()
                if not owner:
                    owner = RefMuniOwner(name=name)
                    db.session.add(owner)
                    db.session.flush()
                owner_map[acc_id] = owner.id

        # 3. Process Accounts & attach rates/arrears/arrangements
        acc_obj_map = {} # acc.id -> BilMuniAccount
        for a_data in data.get('accounts', []):
            acc_num = a_data.get('number', '').strip()
            acc_id = a_data.get('id')
            if acc_num and acc_id:
                acc = BilMuniAccount(
                    property_id=prop.id,
                    account_number=acc_num,
                    is_bulk_account=True if a_data.get('isBulk') else False
                )
                if acc_id in owner_map:
                    acc.owner_id = owner_map[acc_id]
                db.session.add(acc)
                acc_obj_map[acc_id] = acc
        
        # Attach Rates
        for r in data.get('rates', []):
            acc = acc_obj_map.get(r.get('account_id'))
            if acc:
                acc.rates_amount = float(r.get('amount') or 0.0)
                if r.get('date'):
                    acc.rates_date = datetime.strptime(r.get('date'), '%Y-%m-%d').date()
                acc.rates_charge_to = r.get('charge_to', 'owner')
                acc.rates_reference = r.get('reference', '')
                acc.rates_erf_details = r.get('erf_details', '')
                acc.rates_property_category = r.get('property_category', '')
                acc.rates_market_value = float(r.get('market_value') or 0.0)
                acc.rates_rateable_value = float(r.get('rateable_value') or 0.0)
                acc.rates_general_randage = float(r.get('general_randage') or 0.0)
                acc.rates_sra_randage = float(r.get('sra_randage') or 0.0)
                acc.rates_deferred = float(r.get('deferred') or 0.0)
                acc.rates_sra_monthly = float(r.get('sra_monthly') or 0.0)
                acc.rates_general_monthly = float(r.get('general_monthly') or 0.0)

        # Attach Arrears
        for a in data.get('arrears', []):
            acc = acc_obj_map.get(a.get('account_id'))
            if acc:
                acc.arrears_amount = float(a.get('amount') or 0.0)
                if a.get('date'):
                    acc.arrears_date = datetime.strptime(a.get('date'), '%Y-%m-%d').date()
                acc.arrears_charge_to = a.get('charge_to', 'owner')
                
        # Attach Arrangements
        for arg in data.get('arrangements', []):
            acc = acc_obj_map.get(arg.get('account_id'))
            if acc:
                acc.arrangement_contract_number = arg.get('contract_number', '')
                acc.arrangement_charge_to = arg.get('charge_to', 'owner')
                acc.arrangement_agreement_amount = float(arg.get('agreement_amount') or 0.0)
                acc.arrangement_installments_raised = float(arg.get('installments_raised') or 0.0)
                acc.arrangement_installment_amount = float(arg.get('installment_amount') or 0.0)
                acc.arrangement_amount_owing = float(arg.get('amount_owing') or 0.0)
                acc.arrangement_remaining_periods = int(arg.get('remaining_periods') or 0)
                if arg.get('date'):
                    acc.arrangement_date = datetime.strptime(arg.get('date'), '%Y-%m-%d').date()

        # 4. Process Meters
        for m_data in data.get('meters', []):
            m_num = m_data.get('number', '').strip()
            acc_num = m_data.get('account', '').strip()
            u_type = 'Water' if 'water' in m_data.get('type', '').lower() else 'Electrical'
            
            if m_num:
                meter = BilMeter(
                    meter_number=m_num,
                    utility_type=u_type,
                    municipal_bill_number=acc_num
                )
                db.session.add(meter)

        prop.onboarding_status = 'draft_manual'
        
        db.session.commit()
        return jsonify({"message": "Architecture saved successfully!"}), 200
        
    except Exception as e:
        from app.extensions import db
        import traceback
        try:
            db.session.rollback()
        except:
            pass
        print("SAVE GLOBAL ARCHITECTURE ERROR:", str(e))
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

"""
    text = text.replace(old_func, new_func)
    with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
        f.write(text)
    print("Updated save_global_architecture!")
