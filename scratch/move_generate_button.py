import re

html_path = 'templates/program_sace/provisioning_map.html'
with open(html_path, 'r', encoding='utf-8') as f:
    text = f.read()

# Extract the form
form_pattern = r'<form action="{{ url_for\(\'sace_bp\.generate_auditor_code\'\) }}" method="POST" class="inline">.*?</form>'
form_match = re.search(form_pattern, text, flags=re.DOTALL)
if form_match:
    form_html = form_match.group(0)
    # Remove from top row
    text = text.replace(form_html, '')
    
    # Locate Provisioned Auditors header
    prov_pattern = r'(<div class="mb-4 flex justify-between items-end border-b border-slate-200 pb-2">\s*<div>\s*<h3.*?</h3>\s*<p.*?</p>\s*</div>)\s*</div>'
    prov_match = re.search(prov_pattern, text, flags=re.DOTALL)
    
    if prov_match:
        original_prov = prov_match.group(0)
        # We want to put the form right after the closing </div> of the text block
        # so it's a flex item inside the flex-between container.
        new_prov = prov_match.group(1) + '\n            ' + form_html + '\n        </div>'
        text = text.replace(original_prov, new_prov)

with open(html_path, 'w', encoding='utf-8') as f:
    f.write(text)
