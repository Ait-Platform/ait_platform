import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Delete the property_hub route and function
new_text = re.sub(r'@billing_bp\.route\("/billing/property/<int:property_id>/hub", methods=\["GET"\]\)\n@login_required\ndef property_hub\(property_id\):.*?(?=@billing_bp)', '', text, flags=re.DOTALL)

# 2. Rename input_readings route to property_hub route
new_text = new_text.replace(
    '@billing_bp.route("/billing/input_readings/<int:property_id>", methods=["GET", "POST"])',
    '@billing_bp.route("/billing/property/<int:property_id>/hub", methods=["GET", "POST"])'
)
# 3. Rename def input_readings to def property_hub
new_text = new_text.replace('def input_readings(property_id):', 'def property_hub(property_id):')

# 4. In property_hub, calculate tenant_id so it can be passed to the template
tenant_logic = '''
    tenant_id = None
    if units and units[0].tenants:
        tenant_id = units[0].tenants[0].id
'''
new_text = new_text.replace('    if request.method == "POST":', tenant_logic + '\n    if request.method == "POST":')

# 5. Add tenant_id to render_template
new_text = new_text.replace(
    'return render_template("program_billing/input_readings.html",',
    'return render_template("program_billing/property_hub.html",\n                           tenant_id=tenant_id,'
)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(new_text)
print('Done modifying routes.py')
