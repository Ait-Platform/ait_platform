with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

import re
matches = re.search(r'def .*?claim.*?:.*', text, re.IGNORECASE)
if matches:
    print("Found claim route")
    
# Let's just grep for "vault"
if "vault" in text.lower():
    print("Vault logic found in routes.py")
else:
    print("NO vault logic found in routes.py!")
