with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    lines = f.readlines()

lines[160] = "                if claim:\n"
lines[161] = "                    new_pos = claim.title.split(\": \")[-1] if \":\" in claim.title else (claim.title.split(\" - \")[-1] if \" - \" in claim.title else claim.title)\n"
lines[162] = "                    current_appointment.position = new_pos.strip()\n"
lines[163] = "                    db.session.commit()\n"
lines[164] = "                    pos = new_pos.strip().lower()\n"

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.writelines(lines)
print("Patched indentation error in routes.py")
