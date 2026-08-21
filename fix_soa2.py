import sys

with open('templates/program_debtors/soa_template.html', 'r', encoding='utf-8') as f:
    content = f.read()

target = '''    </div>
  </div>
<div class="max-w-4xl mx-auto mb-4 no-print">
    {% include "partials/flash_messages.html" %}
</div>
<div class="max-w-4xl mx-auto bg-white p-10 shadow-lg print-container min-h-[1056px] border border-gray-200 relative">'''

new = '''    </div>
  </div>
{% endif %}
{% if not is_pdf %}
<div class="max-w-4xl mx-auto mb-4 no-print">
    {% include "partials/flash_messages.html" %}
</div>
{% endif %}
<div class="max-w-4xl mx-auto bg-white p-10 shadow-lg print-container min-h-[1056px] border border-gray-200 relative">'''

# Let's use a regex instead since the whitespace might be different
import re
content = re.sub(r'    </div>\s*</div>\s*<div class="max-w-4xl mx-auto mb-4 no-print">\s*{% include "partials/flash_messages.html" %}\s*</div>\s*<div class="max-w-4xl mx-auto bg-white p-10 shadow-lg print-container min-h-\[1056px\] border border-gray-200 relative">', new, content)

with open('templates/program_debtors/soa_template.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("soa_template.html fixed!")
