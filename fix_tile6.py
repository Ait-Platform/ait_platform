import re

# 1. Update providers.py
filepath = 'app/program_uip/services/providers.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('STAFF = ("manager", "receptionist")', 'STAFF = ("manager", "receptionist", "secretary")')

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)

# 2. Update secretary_workspace.html
filepath_html = 'templates/program_uip/dashboards/secretary_workspace.html'
with open(filepath_html, 'r', encoding='utf-8') as f:
    html_content = f.read()

# Find Tile 6 block and replace href="#"
html_content = html_content.replace('<!-- Tile 6: Ratepayer Queries -->\n        <a href="#" class="group', '<!-- Tile 6: Ratepayer Queries -->\n        <a href="{{ url_for(\'uip_bp.reception_page\', org_slug=org.slug) }}" class="group')

with open(filepath_html, 'w', encoding='utf-8') as f:
    f.write(html_content)

print("Linked Tile 6 and updated STAFF permissions")
