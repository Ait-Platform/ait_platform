import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Add route for meter exceptions
exceptions_route = '''@billing_bp.route("/meter_exceptions/<int:property_id>", methods=["GET"])
@login_required
def meter_exceptions(property_id):
    from app.models.billing import BilProperty, BilMeter
    prop = BilProperty.query.get_or_404(property_id)
    if prop.manager_id != current_user.id and not current_user.has_role('admin'):
        from flask import abort
        abort(403)
        
    all_meters = get_all_property_meters(property_id)
    # Filter for non-active meters (like stolen)
    exception_meters = [m for m in all_meters if (m.status or 'active').lower() != 'active']
    
    # We want to find the replacement meter for each exception meter
    # Replacement meter points to the stolen meter via replacement_for_meter_id
    replacement_map = {}
    for ex in exception_meters:
        rep = BilMeter.query.filter_by(replacement_for_meter_id=ex.id).first()
        replacement_map[ex.id] = rep
        
    return render_template("program_billing/meter_exceptions.html", 
                           property=prop, 
                           exception_meters=exception_meters,
                           replacement_map=replacement_map)

'''

text = text.replace('@billing_bp.route("/billing/utilities", methods=["GET", "POST"])', exceptions_route + '\n@billing_bp.route("/billing/utilities", methods=["GET", "POST"])')

# 2. Add action handling in utilities_hub
hub_old = '''        elif action == "metsoa":
            return redirect(url_for("billing_bp.metsoa", property_id=property_id, month=month))'''

hub_new = '''        elif action == "metsoa":
            return redirect(url_for("billing_bp.metsoa", property_id=property_id, month=month))
        elif action == "exceptions":
            return redirect(url_for("billing_bp.meter_exceptions", property_id=property_id))'''

text = text.replace(hub_old, hub_new)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)

print('Updated routes.py with meter_exceptions logic')
