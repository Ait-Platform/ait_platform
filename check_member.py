with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

import re
matches = re.search(r'def .*?member.*?:', text)
if matches:
    print(matches.group(0))
else:
    print("No member list route found")
