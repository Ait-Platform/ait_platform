with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()
for i in range(350, 400):
    if i < len(lines) and 'flash(' in lines[i]:
        print(f'{i}: {lines[i].strip()}')
