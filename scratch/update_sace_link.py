import re
html_path = 'templates/program_sace/provisioning_map.html'
with open(html_path, 'r', encoding='utf-8') as f:
    html = f.read()

# Update the register link to explicitly set subject='sace'
old_link = "{{ url_for('auth_bp.register', next=request.path) }}"
new_link = "{{ url_for('auth_bp.register', subject='sace', next=request.path) }}"
html = html.replace(old_link, new_link)

with open(html_path, 'w', encoding='utf-8') as f:
    f.write(html)
