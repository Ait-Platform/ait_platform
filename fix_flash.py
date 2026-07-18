import re

with open('templates/program_billing/utilities_hub.html', 'r', encoding='utf-8') as f:
    text = f.read()

text = re.sub(r'\{\%\s*include\s*"partials/flash_messages\.html"\s*\%\}\s*', '', text)

text = re.sub(
    r'(<div class="bg-white rounded-xl shadow overflow-hidden border border-slate-200">)',
    r'\1\n      {% include "partials/flash_messages.html" %}',
    text
)

with open('templates/program_billing/utilities_hub.html', 'w', encoding='utf-8') as f:
    f.write(text)

print('Moved flash messages in utilities_hub.html')
