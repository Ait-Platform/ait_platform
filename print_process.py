with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()
for i, line in enumerate(lines):
    if 'def ai_onboarding_process():' in line:
        for j in range(i, i+30):
            if j < len(lines):
                print(str(j) + ': ' + line.strip().encode("ascii", "ignore").decode("ascii"))
        break
