import re

with open('templates/auth/checkout_decision.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace the question text
old_text = '''{% if 'sace' in subject %}
          <p class="text-lg font-bold text-slate-800 mb-4">Do you have a Pre-Paid Voucher or School Code?</p>
          {% else %}
          <p class="text-lg font-bold text-slate-800 mb-4">Do you have a Voucher or Private Code?</p>
          {% endif %}'''

new_text = '''{% if 'sace' in subject %}
          <p class="text-lg font-bold text-slate-800 mb-4">Are you attending a pre-paid SACE CPTD Approved Activity?</p>
          {% else %}
          <p class="text-lg font-bold text-slate-800 mb-4">Do you have an Access Code?</p>
          {% endif %}'''
content = content.replace(old_text, new_text)

# Replace the input label from "Voucher Code" to "Access Code"
content = content.replace('<label class="block text-sm font-medium text-slate-700 mb-2">Voucher Code</label>', '<label class="block text-sm font-medium text-slate-700 mb-2">Access Code (Provided by your School/Facilitator)</label>')
content = content.replace('placeholder="Enter voucher code"', 'placeholder="Enter access code"')

with open('templates/auth/checkout_decision.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated text to avoid 'voucher' terminology")
