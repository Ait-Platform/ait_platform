import re

# 1. Fix manager_dashboard.html (properties -> data)
with open('templates/program_billing/manager_dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('{% for row in properties %}', '{% for row in data %}')
with open('templates/program_billing/manager_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)

# 2. Fix duplicate name check in routes.py
with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    routes = f.read()

old_prop_check = """    prop_name = request.form.get("property_name", "").strip().title()
    if not prop_name:
        flash("Property Name is required", "error")
        return redirect(url_for('billing_bp.learner_dashboard'))

    bills = 1"""

new_prop_check = """    prop_name = request.form.get("property_name", "").strip().title()
    if not prop_name:
        flash("Property Name is required", "error")
        return redirect(url_for('billing_bp.learner_dashboard'))
        
    from app.models import BilProperty
    existing = BilProperty.query.filter(BilProperty.name.ilike(prop_name), BilProperty.manager_id == current_user.id).first()
    if existing:
        flash(f"Property '{prop_name}' already exists. Please use a unique name.", "error")
        return redirect(url_for('billing_bp.learner_dashboard'))

    bills = 1"""

if "existing = BilProperty.query.filter(BilProperty.name.ilike" not in routes:
    routes = routes.replace(old_prop_check, new_prop_check)
    with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
        f.write(routes)

# 3. Move flash messages in setup_wizard.html
with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    wizard = f.read()

if "{% block flashes %}{% endblock %}" not in wizard:
    # Disable global flashes
    wizard = wizard.replace('{% block title %}Global Architecture Setup{% endblock %}', '{% block title %}Global Architecture Setup{% endblock %}\n{% block flashes %}{% endblock %}')
    
    # Insert inside the colored tile
    insert_target = """    <!-- Header -->
    <div class="bg-white px-8 pt-6 pb-2 flex flex-col gap-2">"""
    
    new_insert = """    <!-- Header -->
    <div class="bg-white px-8 pt-6 pb-2 flex flex-col gap-2">
      {% include "partials/flash_messages.html" %}"""
      
    wizard = wizard.replace(insert_target, new_insert)
    
    with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
        f.write(wizard)

print("Fixed blank dashboard, duplicate name check, and flash message placement.")
