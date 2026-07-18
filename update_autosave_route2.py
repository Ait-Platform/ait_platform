import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

old_route = """    data = request.json
    draft = BilArchitectureDraft.query.filter_by(property_id=prop.id).first()
    if not draft:
        draft = BilArchitectureDraft(property_id=prop.id)
        db.session.add(draft)
    
    draft.draft_json = data
    db.session.commit()
    
    return jsonify({"status": "success"})"""

new_route = """    data = request.json
    draft = BilArchitectureDraft.query.filter_by(property_id=prop.id).first()
    if not draft:
        draft = BilArchitectureDraft(property_id=prop.id)
        db.session.add(draft)
    
    draft.draft_json = data
    
    # Sync property map if it exists in the draft JSON
    property_map = data.get('propertyMap')
    if property_map:
        prop.expected_bills = int(property_map.get('accounts', prop.expected_bills))
        prop.expected_water_meters = int(property_map.get('water', prop.expected_water_meters))
        prop.expected_elec_meters = int(property_map.get('elec', prop.expected_elec_meters))
        prop.is_bulk_water = bool(property_map.get('bulkWater', prop.is_bulk_water))
        prop.is_bulk_elec = bool(property_map.get('bulkElec', prop.is_bulk_elec))
        
    db.session.commit()
    
    return jsonify({"status": "success"})"""

content = content.replace(old_route, new_route)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("save_architecture_draft updated.")
