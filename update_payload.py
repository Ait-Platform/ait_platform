import re

with open('templates/program_billing/ai_onboarding.html', 'r', encoding='utf-8') as f:
    content = f.read()

target = '''      const payload = {
        property_name: this.propertyName,
        address: this.address,
        rates_amount: this.ratesAmount,
        tenants: this.statementProfiles.map(s => ({'''

injection = '''      const payload = {
        property_id: {% if draft_property %}{{ draft_property.id }}{% else %}null{% endif %},
        property_name: this.propertyName,
        address: this.address,
        rates_amount: this.ratesAmount,
        tenants: this.statementProfiles.map(s => ({'''

content = content.replace(target, injection)

with open('templates/program_billing/ai_onboarding.html', 'w', encoding='utf-8') as f:
    f.write(content)
