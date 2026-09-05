import re
html_path = 'templates/program_sace/provisioning_map.html'
with open(html_path, 'r', encoding='utf-8') as f:
    html = f.read()

# Replace the rogue } with the full if block
html = html.replace('<!-- Post-Pledge Registration Modal -->\n}', '<!-- Post-Pledge Registration Modal -->\n{% if has_pledged and not current_user.is_authenticated %}')

with open(html_path, 'w', encoding='utf-8') as f:
    f.write(html)
