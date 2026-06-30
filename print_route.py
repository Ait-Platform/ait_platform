with open('app/program_billing/routes.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()
for i, line in enumerate(lines):
    if 'def learner_dashboard():' in line:
        for j in range(i, i+50):
            if j < len(lines):
                # safely print ascii
                print(f'{j}: {lines[j].encode("ascii", "ignore").decode("ascii")}', end='')
        break
