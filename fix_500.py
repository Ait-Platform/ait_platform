import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

# I will replace the start of save_global_architecture to wrap EVERYTHING in try-except
old_func_start = """def save_global_architecture(property_id):
    from app.models.billing import BilArchitectureDraft
    prop = BilProperty.query.get_or_404(property_id)
    if prop.manager_id != current_user.id:
        return jsonify({"error": "Unauthorized"}), 403

    data = request.json
    if not data:
        return jsonify({"error": "No data provided"}), 400

    from app.extensions import db
    from app.models import BilMuniAccount, RefMuniOwner, BilMeter
    
    try:"""

new_func_start = """def save_global_architecture(property_id):
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
        
"""

old_func_end = """        db.session.commit()
        return jsonify({"message": "Architecture saved successfully!"}), 200
        
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500"""

new_func_end = """        db.session.commit()
        return jsonify({"message": "Architecture saved successfully!"}), 200
        
    except Exception as e:
        from app.extensions import db
        import traceback
        traceback.print_exc()
        try:
            db.session.rollback()
        except:
            pass
        return jsonify({"error": str(e) + " - " + traceback.format_exc()}), 500"""

# Note: this simple replacement would break indentation if not careful.
