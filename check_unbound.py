with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

start = max(0, 1281 - 20)
end = min(len(lines), 1281 + 20)
for i, line in enumerate(lines[start:end]):
    print(f"{i + start + 1}: {line}", end='')
