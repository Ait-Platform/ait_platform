with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    lines = f.readlines()

for i, line in enumerate(lines):
    if "vault_match = " in line:
        for j in range(i+25, i+50):
            print(f"  {j}: {lines[j].strip()}")
        break
