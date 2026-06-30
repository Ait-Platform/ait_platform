with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()
for i, line in enumerate(lines):
    if 'def ai_onboarding():' in line:
        for j in range(max(0, i-2), i+30):
            if j < len(lines):
                print(f'{j}: {lines[j].encode("ascii", "ignore").decode("ascii")}', end='')
        break
