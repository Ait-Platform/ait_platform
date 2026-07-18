with open('templates/admin/index.html', 'r', encoding='utf-8') as f:
    content = f.read()

import re
# Remove the Manage Programs & Visibility tile
content = re.sub(r'<a href="{{ url_for\(\'admin_bp\.manage_programs\'\).*?</a>', '', content, flags=re.DOTALL)

with open('templates/admin/index.html', 'w', encoding='utf-8') as f:
    f.write(content)
