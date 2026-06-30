with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()
for i in range(120, 160):
    if i < len(lines):
        print(f'{i}: {lines[i].strip()}')
