with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    lines = f.readlines()

for i, line in enumerate(lines):
    if "def submit_claim" in line:
        for j in range(i, i+60):
            print(f"  {j}: {lines[j].strip()}")
        break
