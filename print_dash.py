with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    lines = f.readlines()
for i, line in enumerate(lines):
    if "def dashboard" in line:
        for j in range(i, i+50):
            if j < len(lines):
                print(f"{j}: {lines[j].strip()}")
