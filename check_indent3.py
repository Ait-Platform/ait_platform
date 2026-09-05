with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

for i, line in enumerate(lines[1685:1695]):
    print(f"{i + 1685 + 1}: {line}", end='')
