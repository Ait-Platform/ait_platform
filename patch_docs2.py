with open("app/program_uip/services/documents.py", "r", encoding="utf-8") as f:
    text = f.read()

import re

new_download = """def download(org, actor, document_id, version_number):
    try:
        audit.authorize(org, actor, READERS)
    except Forbidden:
        # Allow MO to proceed to accessible() check for their narrow permissions
        pass
        
    row = UipDocument.query.filter_by(organization_id=org, id=document_id).first_or_404()
    if not accessible(org, actor, row):
        abort(404)"""

pattern = re.compile(r'def download\(org, actor, document_id, version_number\):\s+audit\.authorize\(org, actor, READERS\)\s+row = UipDocument\.query\.filter_by\(organization_id=org, id=document_id\)\.first_or_404\(\)\s+if not accessible\(org, actor, row\):\s+abort\(404\)')
text = pattern.sub(new_download, text)

with open("app/program_uip/services/documents.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated download")
