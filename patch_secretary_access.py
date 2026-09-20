# 1. Update backend require_secretary
with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    text = f.read()

import re

old_require = """    if not current_appointment or not current_appointment.position or current_appointment.position.strip().lower() != "secretary":
        abort(403, description="Access restricted to the active Secretary.")"""

new_require = """    if not current_appointment or not current_appointment.position:
        abort(403, description="Access restricted.")
        
    pos = current_appointment.position.strip().lower()
    allowed = ["secretary", "chairperson", "chairman", "vice-chairperson", "vice chairman", "manager"]
    
    if pos not in allowed:
        abort(403, description="Access restricted to the active Secretary and Chairperson.")"""

text = text.replace(old_require, new_require)
with open("app/program_uip/secretary_routes.py", "w", encoding="utf-8") as f:
    f.write(text)


# 2. Update context processor
with open("app/program_uip/completion_routes.py", "r", encoding="utf-8") as f:
    comp = f.read()

old_is_sec = """            if mem.position == "Secretary":
                is_secretary = True"""

new_is_sec = """            if mem.position and mem.position.lower() in ["secretary", "chairperson", "chairman", "vice-chairperson", "vice chairman"]:
                is_secretary = True"""

comp = comp.replace(old_is_sec, new_is_sec)
with open("app/program_uip/completion_routes.py", "w", encoding="utf-8") as f:
    f.write(comp)

print("Broadened Secretary access to include Chairperson")
