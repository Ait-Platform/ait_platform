with open("app/program_uip/secretary_routes.py", "r", encoding="utf-8") as f:
    lines = f.readlines()
for i, line in enumerate(lines):
    if "target == " in line:
        for j in range(i, i+40):
            if j < len(lines):
                print(f"{j}: {lines[j].strip()}")
