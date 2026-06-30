import re

with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

target = '''    draft_property = BilProperty.query.filter(
        BilProperty.manager_id == current_user.id,
        BilProperty.onboarding_status.like('draft_%')
    ).first()
    
    return render_template("program_billing/manager_dashboard.html",
                           properties=properties,
                           draft_property=draft_property)'''

injection = '''    draft_property = BilProperty.query.filter(
        BilProperty.manager_id == current_user.id,
        BilProperty.onboarding_status.like('draft_%')
    ).first()
    
    uploaded_bills = 0
    if draft_property:
        from app.models.billing import BilExtractionLog
        uploaded_bills = BilExtractionLog.query.filter_by(property_name=draft_property.name).count()
    
    return render_template("program_billing/manager_dashboard.html",
                           properties=properties,
                           draft_property=draft_property,
                           uploaded_bills=uploaded_bills)'''

content = content.replace(target, injection)

with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
