import re

with open('templates/auth/checkout_decision.html', 'r', encoding='utf-8') as f:
    content = f.read()

old_text = '''{% if 'sace' in subject %}
          <p class="text-lg font-bold text-slate-800 mb-4">Do you have a SACE Voucher or Private Code?</p>
          {% else %}'''

new_text = '''{% if 'sace' in subject %}
          <p class="text-lg font-bold text-slate-800 mb-4">Do you have a Pre-Paid Voucher or School Code?</p>
          {% else %}'''

content = content.replace(old_text, new_text)

with open('templates/auth/checkout_decision.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated voucher text")
