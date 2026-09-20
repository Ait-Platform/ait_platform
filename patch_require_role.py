with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

import re

# Find the _require_role definition
pattern = r'def _require_role.*?return roles\[0\]'
match = re.search(pattern, text, flags=re.DOTALL)
if match:
    func_text = match.group(0)
    # Replace all abort(403) with return None
    func_text = func_text.replace("abort(403)", "return None")
    
    text = text[:match.start()] + func_text + text[match.end():]
    
    with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
        f.write(text)
    print("Patched _require_role to never throw 403")
else:
    print("Could not find _require_role")
