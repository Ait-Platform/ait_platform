import re

with open('templates/program_billing/manager_dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Edit Draft Modal: Tenants/Units -> Statement(s)
content = content.replace(
    '<label class="block text-sm font-semibold text-slate-700 mb-1">Tenants/Units</label>',
    '<label class="block text-sm font-semibold text-slate-700 mb-1">Statement(s)</label>'
)

# 2. Add Property Modal: the "tenants" input label currently says "How many Bill Account Number(s) are required?" because of my previous messy regex replacement.
# Wait, the first input is `name="bills"` which is "distinct physical bills".
# The second is `name="tenants"`. 
# Let's fix the labels for the Add Property Modal to be exactly what they should be.

# For name="bills" -> "How many Bill Account Number(s) are required?"
old_bills_label = '<label class="block text-sm font-bold text-slate-700 mb-2">How many distinct physical bills do you expect to upload?</label>'
new_bills_label = '<label class="block text-sm font-bold text-slate-700 mb-2">How many Bill Account Number(s) are required?</label>'
content = content.replace(old_bills_label, new_bills_label)

# For name="tenants" -> "How many Statement(s) are required?"
# Note: due to previous script, it might currently say "How many Bill Account Number(s) are required?" right above name="tenants".
# Let's use regex to replace the label right above name="tenants"
tenants_regex = r'<label class="block text-sm font-bold text-slate-700 mb-2">.*?</label>\s*(<input type="number" name="tenants")'
content = re.sub(tenants_regex, r'<label class="block text-sm font-bold text-slate-700 mb-2">How many Statement(s) are required?</label>\n\1', content)

with open('templates/program_billing/manager_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)

print("Updated tenants/unit wording to statement(s)")
