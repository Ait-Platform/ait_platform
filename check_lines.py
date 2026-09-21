with open("app/program_uip/committee_routes.py", "r", encoding="utf-8") as f:
    lines = f.readlines()

for i, line in enumerate(lines):
    if "def edit_resolution" in line:
        print(f"Line {i}: {line.strip()}")
