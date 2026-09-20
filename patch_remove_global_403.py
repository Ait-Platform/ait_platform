with open("app/program_uip/__init__.py", "r", encoding="utf-8") as f:
    text = f.read()

import re

# Remove the entire block checking membership and aborting 403
pattern = r'if current_user\.is_authenticated:.*?abort\(403.*?\n'
text = re.sub(pattern, '', text, flags=re.DOTALL)

with open("app/program_uip/__init__.py", "w", encoding="utf-8") as f:
    f.write(text)

print("Removed global 403 abort from __init__.py")
