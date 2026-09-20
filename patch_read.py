with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    lines = f.readlines()
for i, line in enumerate(lines):
    if "role_slug = " in line and "committee_member" in line:
        print("".join(lines[i-5:i+10]))
        break
