with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

for i, line in enumerate(lines[:100]):
    if "MechClient" in line or "MechJobCard" in line:
        print(f"{i+1}: {line}", end='')
