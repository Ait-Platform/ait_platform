with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

import re
m = re.search(r'owners = \[\]\s*for m in meters:', text, re.DOTALL)
if m:
    new_code = """owners = []
    
    # Extract unique owners
    owner_map = {}
    for acc in accounts:
        if acc.owner:
            owner_map[acc.owner.name] = {
                'name': acc.owner.name,
                'email_address': acc.muni_email or '-',
                'address': acc.owner_address or '-'
            }
    owners = list(owner_map.values())
    
    for m in meters:"""
    text = text.replace(m.group(0), new_code)
    with open('app/program_billing/routes.py', 'w', encoding='utf-8') as f:
        f.write(text)
    print("Fixed summary owners list!")
else:
    print("Regex failed")
