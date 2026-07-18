import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Add route for save_architecture_draft
draft_route = """
@billing_bp.route('/save_architecture_draft/<int:property_id>', methods=['POST'])
@login_required
def save_architecture_draft(property_id):
    prop = BilProperty.query.get_or_404(property_id)
    if prop.manager_id != current_user.id:
        return jsonify({"error": "Unauthorized"}), 403

    data = request.json
    draft = BilArchitectureDraft.query.filter_by(property_id=prop.id).first()
    if not draft:
        draft = BilArchitectureDraft(property_id=prop.id)
        db.session.add(draft)
    
    draft.draft_json = data
    db.session.commit()
    
    return jsonify({"status": "success"})
"""

if 'def save_architecture_draft' not in content:
    content += "\n" + draft_route

# 2. Update manual_capture route to fetch draft and pass it
# First find the import at the top if we need it (db is already imported, we need BilArchitectureDraft)
# Wait, we can just query it in manual_capture
old_mc_route = """    # Fetch all meters associated with these accounts
    account_numbers = [a.account_number for a in accounts if a.account_number]
    all_meters = BilMeter.query.filter(BilMeter.municipal_bill_number.in_(account_numbers)).all() if account_numbers else []
    
    return render_template("program_billing/setup_wizard.html", 
        property=draft_property, 
        accounts=accounts, 
        all_meters=all_meters)"""

new_mc_route = """    # Fetch all meters associated with these accounts
    account_numbers = [a.account_number for a in accounts if a.account_number]
    all_meters = BilMeter.query.filter(BilMeter.municipal_bill_number.in_(account_numbers)).all() if account_numbers else []
    
    draft_record = BilArchitectureDraft.query.filter_by(property_id=draft_property.id).first()
    draft_json = draft_record.draft_json if draft_record else None
    
    import json
    return render_template("program_billing/setup_wizard.html", 
        property=draft_property, 
        accounts=accounts, 
        all_meters=all_meters,
        draft_json=json.dumps(draft_json) if draft_json else 'null')"""

content = content.replace(old_mc_route, new_mc_route)

# 3. Update save_global_architecture to delete draft upon success
old_save_end = """        prop.onboarding_status = 'draft_manual'
        db.session.commit()
        
        return jsonify({"message": "Architecture saved successfully!"}), 200"""

new_save_end = """        prop.onboarding_status = 'draft_manual'
        
        # Clear the draft!
        BilArchitectureDraft.query.filter_by(property_id=prop.id).delete()
        
        db.session.commit()
        
        return jsonify({"message": "Architecture saved successfully!"}), 200"""

content = content.replace(old_save_end, new_save_end)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("Updated routes for server-side draft")
