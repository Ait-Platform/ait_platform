with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    lines = f.readlines()
for i in range(165, 205):
    if i < len(lines):
        print(f"{i}: {lines[i].strip()}")
