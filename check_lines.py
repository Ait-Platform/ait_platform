with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    lines = f.readlines()

for i, line in enumerate(lines):
    if "if claim:" in line and "interaction_type=" not in line: # Avoid matching `if claim:` that are correctly indented in global fix
        print(f"Line {i}: {line.rstrip()}")
