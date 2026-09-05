import re

html_path = 'templates/program_sace/compliance/audit_report.html'
with open(html_path, 'r', encoding='utf-8') as f:
    html = f.read()

html = html.replace('url_for(\'sace_bp.dashboard\')', 'url_for(\'sace_bp.provisioning_map\')')
html = html.replace('Back to Dashboard', 'Back to Control Centre')

with open(html_path, 'w', encoding='utf-8') as f:
    f.write(html)
