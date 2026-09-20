import re

# 1. Update completion_routes.py
with open("app/program_uip/completion_routes.py", "r", encoding="utf-8") as f:
    comp = f.read()
comp = comp.replace('["secretary", "chairperson", "chairman", "vice-chairperson", "vice chairman"]', '["secretary", "chairperson", "chairman", "vice-chairperson", "vice chairman", "treasurer"]')
with open("app/program_uip/completion_routes.py", "w", encoding="utf-8") as f:
    f.write(comp)

# 2. Update secretary_routes.py _require_secretary
with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    sec = f.read()
sec = sec.replace('["secretary", "chairperson", "chairman", "vice-chairperson", "vice chairman", "manager"]', '["secretary", "chairperson", "chairman", "vice-chairperson", "vice chairman", "treasurer", "manager"]')
with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
    f.write(sec)

print("Added treasurer to Secretary Tools access")
