with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

import re
text = re.sub(r'\s+elif action == "edit_seat":', '\n        elif action == "edit_seat":', text)

with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Fixed indentation")
