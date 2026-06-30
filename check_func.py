with open('templates/program_billing/ai_onboarding.html', 'r', encoding='utf-8') as f:
    lines = f.readlines()
for i, line in enumerate(lines):
    if 'aiOnboarding' in line:
        print(f'{i}: {line.strip()}')
