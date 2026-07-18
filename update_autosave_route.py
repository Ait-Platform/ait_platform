import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# We need to find auto_save_draft and inject the property map update.
old_autosave_logic = """    draft = BilArchitectureDraft.query.filter_by(property_id=prop_id).first()
    if not draft:
        draft = BilArchitectureDraft(property_id=prop_id)
        db.session.add(draft)
    
    draft.draft_json = payload.get('draft_json', {})
    db.session.commit()"""

new_autosave_logic = """    draft = BilArchitectureDraft.query.filter_by(property_id=prop_id).first()
    if not draft:
        draft = BilArchitectureDraft(property_id=prop_id)
        db.session.add(draft)
    
    draft_json = payload.get('draft_json', {})
    draft.draft_json = draft_json
    
    # Sync property map if it exists in the draft JSON
    property_map = draft_json.get('propertyMap')
    if property_map:
        prop.expected_bills = int(property_map.get('accounts', prop.expected_bills))
        prop.expected_water_meters = int(property_map.get('water', prop.expected_water_meters))
        prop.expected_elec_meters = int(property_map.get('elec', prop.expected_elec_meters))
        prop.is_bulk_water = bool(property_map.get('bulkWater', prop.is_bulk_water))
        prop.is_bulk_elec = bool(property_map.get('bulkElec', prop.is_bulk_elec))
        
    db.session.commit()"""

if old_autosave_logic in content:
    content = content.replace(old_autosave_logic, new_autosave_logic)
    with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print("auto_save_draft successfully updated to sync property map.")
else:
    print("Could not find auto_save_draft logic.")
