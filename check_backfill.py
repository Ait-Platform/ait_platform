with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

for i, line in enumerate(lines[1295:1320]):
    print(f"{i + 1295 + 1}: {line}", end='')
