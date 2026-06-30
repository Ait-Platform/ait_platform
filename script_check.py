with open('templates/program_billing/ai_onboarding.html', 'r', encoding='utf-8') as f:
    lines = f.readlines()
for j in range(350, 450):
    if j < len(lines):
        print(f'{j}: {lines[j]}', end='')
