with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

import re
text = re.sub(r'except Exception as e:\s+import logging\s+logging\.error\(f"AUTO PATCH FAILED: \{e\}"\)', 'except Exception:', text)

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Reverted all bad except blocks")
