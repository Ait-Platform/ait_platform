with open('templates/program_billing/ai_onboarding.html', 'r', encoding='utf-8') as f:
    lines = f.readlines()
for i, line in enumerate(lines):
    if 'function aiOnboarding()' in line:
        for j in range(i, i+50):
            if j < len(lines):
                print(f'{j}: {lines[j].strip()}')
        break
