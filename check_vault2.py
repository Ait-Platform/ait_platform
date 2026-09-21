with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    lines = f.readlines()

for i, line in enumerate(lines):
    if "vault" in line.lower():
        print(f"Line {i}: {line.strip()}")
        # print some context
        for j in range(max(0, i-5), min(len(lines), i+15)):
            print(f"  {j}: {lines[j].strip()}")
        break
