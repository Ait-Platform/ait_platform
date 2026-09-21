with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    lines = f.readlines()
for i, line in enumerate(lines):
    if "AGM" in line:
        for j in range(max(0, i-5), min(len(lines), i+10)):
            print(f"  {j}: {lines[j].strip()}")
        break
