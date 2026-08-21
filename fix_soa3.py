import sys

with open('templates/program_debtors/soa_template.html', 'r', encoding='utf-8') as f:
    content = f.read()

target = '''  </div>
<div class="max-w-4xl mx-auto mb-4 no-print">'''

new_code = '''  </div>
{% endif %}
{% if not is_pdf %}
<div class="max-w-4xl mx-auto mb-4 no-print">'''

content = content.replace(target, new_code)

target2 = '''  </div>
  <div class="max-w-4xl mx-auto bg-white p-10 shadow-lg print-container min-h-[1056px] border border-gray-200 relative">'''

new_code2 = '''  </div>
{% endif %}
  <div class="max-w-4xl mx-auto bg-white p-10 shadow-lg print-container min-h-[1056px] border border-gray-200 relative">'''

content = content.replace(target2, new_code2)

with open('templates/program_debtors/soa_template.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("soa_template.html fixed 3!")
