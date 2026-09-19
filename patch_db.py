with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    lines = f.readlines()

# Add import before the commit
lines[162] = "                    current_appointment.position = new_pos.strip()\n"
lines[163] = "                    from app.extensions import db\n                    db.session.commit()\n"

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.writelines(lines)
print("Patched db import for local auto-fix")
