with open('templates/program_billing/ai_onboarding.html', 'r', encoding='utf-8') as f:
    lines = f.readlines()
for i, line in enumerate(lines):
    if 'fetch(' in line or 'property_id' in line:
        print(f'{i}: {line.strip()}')
