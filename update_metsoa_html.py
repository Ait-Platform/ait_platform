import re

with open('templates/program_billing/metsoa.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Replace PDF generation route parameter
text = text.replace("tenant_id=tenant.id", "property_id=property.id")
text = text.replace("tenant_id=tenant_id", "property_id=property.id")

# Replace tenant name with property name context
text = text.replace("{{ tenant.name }}", "{{ property.name }} (Property Total)")
text = text.replace("{{ tenant.email or '' }}", "{{ property.manager.email or '' }}")

with open('templates/program_billing/metsoa.html', 'w', encoding='utf-8') as f:
    f.write(text)

print('Updated metsoa.html')
