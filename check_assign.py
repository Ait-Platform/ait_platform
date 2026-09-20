with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    lines = f.readlines()
for i, line in enumerate(lines):
    if 'elif action == "assign_member":' in line:
        print("".join(lines[i:i+15]))
        break
