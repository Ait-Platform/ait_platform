with open('templates/admin/modules_control.html', 'r', encoding='utf-8') as f:
    content = f.read()

import re
# Remove the entire div containing the global save button
content = re.sub(r'<div class="mt-6 flex justify-end">[\s\S]*?</button>\s*</div>', '', content)

with open('templates/admin/modules_control.html', 'w', encoding='utf-8') as f:
    f.write(content)
