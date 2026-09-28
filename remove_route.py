filepath = 'app/program_uip/routes.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# Try to remove the block we added
import re
pattern = re.compile(r'@uip_bp\.route\("/<org_slug>/verify/ratepayer/claim".*?return redirect\(url_for\("uip_bp\.my_access", org_slug=org_slug, claim="ratepayer"\)\)', re.DOTALL)
content = pattern.sub('', content)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Route removed")
