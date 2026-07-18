import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Let's wrap the save_global_architecture body in a try/except
old_block = """    accounts = data.get('accounts', [])
    water_meters = data.get('water_meters', [])
    elec_meters = data.get('elec_meters', [])
    mapping = data.get('mapping', [])

    # Create Meters (We will save them first so we can link them)
    # Map frontend IDs (e.g. "water_1") to database IDs"""

new_block = """    try:
        accounts = data.get('accounts', [])
        water_meters = data.get('water_meters', [])
        elec_meters = data.get('elec_meters', [])
        mapping = data.get('mapping', [])

        # Create Meters (We will save them first so we can link them)
        # Map frontend IDs (e.g. "water_1") to database IDs"""
        
old_end = """    prop.onboarding_status = 'draft_manual' # Keep it here for now until final finalize
    db.session.commit()
    
    return jsonify({"message": "Architecture saved successfully!"}), 200"""

new_end = """        prop.onboarding_status = 'draft_manual' # Keep it here for now until final finalize
        db.session.commit()
        
        return jsonify({"message": "Architecture saved successfully!"}), 200
    except Exception as e:
        db.session.rollback()
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500"""

# wait, replacing with indentation using simple replace will be tricky due to python whitespace
# Let's just use `re.sub` or rewrite it cleanly
