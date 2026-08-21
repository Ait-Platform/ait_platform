import sys

with open('templates/program_debtors/soa_template.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace(
    '''<div class="max-w-4xl mx-auto mb-8 mt-6 no-print">''',
    '''{% if not is_pdf %}<div class="max-w-4xl mx-auto mb-8 mt-6 no-print">'''
)

content = content.replace(
    '''    </div>
  </div>
</div>
{% if not is_pdf %}
<div class="max-w-4xl mx-auto mb-4 no-print">''',
    '''    </div>
  </div>
</div>
{% endif %}
{% if not is_pdf %}
<div class="max-w-4xl mx-auto mb-4 no-print">'''
)

with open('templates/program_debtors/soa_template.html', 'w', encoding='utf-8') as f:
    f.write(content)
