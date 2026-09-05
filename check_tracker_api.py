with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

for i, line in enumerate(lines[1230:1240]):
    print(f"{i + 1230 + 1}: {line}", end='')
