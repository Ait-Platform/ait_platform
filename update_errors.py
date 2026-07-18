import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Update the catch block to alert the error message
old_catch = """      if (response.ok) {
        window.location.href = "{{ url_for('billing_bp.learner_dashboard') }}";
      } else {
        alert("Failed to save architecture. Please ensure all data is valid.");
      }
    } catch (e) {
      console.error(e);
      alert("Network error.");
    }"""

new_catch = """      if (response.ok) {
        window.location.href = "{{ url_for('billing_bp.learner_dashboard') }}";
      } else {
        const errData = await response.json();
        alert("Server Error: " + (errData.error || "Please ensure all data is valid."));
      }
    } catch (e) {
      console.error(e);
      alert("Network error: " + e.message);
    }"""

content = content.replace(old_catch, new_catch)

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(content)

# Now, let's also update the backend route to return the exact exception!
with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    routes_content = f.read()

import textwrap

# We will just replace the entire save_global_architecture function.
old_func = """@billing_bp.route('/save_global_architecture/<int:property_id>', methods=['POST'])
@login_required
def save_global_architecture(property_id):
    prop = BilProperty.query.get_or_404(property_id)
    if prop.manager_id != current_user.id:
        return jsonify({"error": "Unauthorized"}), 403

    data = request.json
    if not data:
        return jsonify({"error": "No data"}), 400

    # 1. Clear existing accounts and meters for this draft property
    BilMuniAccount.query.filter_by(property_id=prop.id).delete()
    # Meters are attached to Municipal Bills implicitly right now via `municipal_bill_number`
    # Let's clean up orphan meters that belong to this property. 
    # For a robust implementation, BilMeter should have a property_id. 
    # Assuming draft mode means we can just rely on the UI to recreate them.
    # Actually, let's look up existing meters by account numbers and delete.
    
    accounts = data.get('accounts', [])
    water_meters = data.get('water_meters', [])
    elec_meters = data.get('elec_meters', [])
    mapping = data.get('mapping', [])

    # Create Meters (We will save them first so we can link them)
    # Map frontend IDs (e.g. "water_1") to database IDs
    db_meters = {}
    
    # Pre-generate meter records
    for m in water_meters + elec_meters:
        # In this phase, they just belong to the property.
        meter = BilMeter(
            meter_number=m['number'],
            utility_type=m['type'],
            status='active',
            pointing_to='Unassigned'
        )
        db.session.add(meter)
        db.session.flush()
        db_meters[m['id']] = meter

    # Handle mapping and accounts
    # The mapping array tells us which meters go to which account
    for acc_data in accounts:
        # Find the map for this account
        acc_map = next((x for x in mapping if x['account_id'] == acc_data['id']), None)
        if not acc_map: continue
        
        # Create Owner if doesn't exist
        owner_name = acc_data['owner']
        owner = RefMuniOwner.query.filter_by(name=owner_name).first()
        if not owner and owner_name:
            owner = RefMuniOwner(name=owner_name)
            db.session.add(owner)
            db.session.flush()

        # Create Account
        muni_acc = BilMuniAccount(
            property_id=prop.id,
            account_number=acc_data['number'],
            is_bulk_account=acc_data['isBulk'],
            owner_id=owner.id if owner else None
        )
        db.session.add(muni_acc)
        db.session.flush()

        # Update Meters linked to this account
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
            # Sub-Account
            # They selected 1 water and 1 elec
            for w_id in acc_map.get('water', []):
                if w_id in db_meters:
                    db_meters[w_id].municipal_bill_number = muni_acc.account_number
                    db_meters[w_id].pointing_to = 'Sub-Unit'
                    # Link to the Bulk water meter? 
                    # The UI didn't specify which bulk meter specifically for the normal selection! 
                    # Wait, in the diagram, the Sub-Meter links to the Bulk meter.
                    # We will need the user to tell us which bulk meter! 
                    # If there's only 1 bulk water meter, we auto-link. If >1, we need to ask. 
                    # Let's refine this in a future iteration, or assume they can edit later.
            for e_id in acc_map.get('elec', []):
                if e_id in db_meters:
                    db_meters[e_id].municipal_bill_number = muni_acc.account_number
                    db_meters[e_id].pointing_to = 'Sub-Unit'

            # Handle Stolen Exceptional Links
            for exc in acc_map.get('exceptions', []):
                stolen_num = exc['stolen_num']
                rep_id = exc['replacement_id']
                if rep_id in db_meters:
                    # Create the Stolen Meter municipal record
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
                    # Link back
                    db_meters[rep_id].replacement_for_meter_id = stolen_meter.id

    prop.onboarding_status = 'draft_manual' # Keep it here for now until final finalize
    db.session.commit()
    
    return jsonify({"message": "Architecture saved successfully!"}), 200"""

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

routes_content = routes_content.replace(old_func, new_func)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(routes_content)

print("Updated try/catch in frontend and backend")
