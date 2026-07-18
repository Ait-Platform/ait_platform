import re

with open('templates/program_billing/architecture_summary.html', 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Remove Address and Erf Number from Property Details
old_prop_details = """<div class="grid grid-cols-2 gap-4">
                    <div><span class="text-slate-500 text-sm">Property Name:</span><div class="font-medium">{{ property.name }}</div></div>
                    <div><span class="text-slate-500 text-sm">Erf Number:</span><div class="font-medium">{{ property.erf_number }}</div></div>
                    <div class="col-span-2"><span class="text-slate-500 text-sm">Address:</span><div class="font-medium">{{ property.address or 'No Address Provided' }}</div></div>
                </div>"""

new_prop_details = """<div class="grid grid-cols-1 gap-4">
                    <div><span class="text-slate-500 text-sm">Property Name:</span><div class="font-medium">{{ property.name }}</div></div>
                </div>"""

text = text.replace(old_prop_details, new_prop_details)

# 2. Remove numbers from headings
# Matches <h2 ...>1. Property Details</h2>, etc.
text = re.sub(r'(<h2[^>]*>)\s*\d+\.\s*(.*?)(</h2>)', r'\1\2\3', text)

with open('templates/program_billing/architecture_summary.html', 'w', encoding='utf-8') as f:
    f.write(text)

print("Fixed property details and headings")
