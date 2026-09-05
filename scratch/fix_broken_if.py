import re
html_path = 'templates/program_sace/provisioning_map.html'
with open(html_path, 'r', encoding='utf-8') as f:
    html = f.read()

# Fix the broken if statement string
html = html.replace('{% if has_pledged and not current_use\n\n\n        {% if not has_pledged %}', '{% if not has_pledged %}')
with open(html_path, 'w', encoding='utf-8') as f:
    f.write(html)
