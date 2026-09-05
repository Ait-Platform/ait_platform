with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

for i, line in enumerate(lines):
    if "def job_cards_list():" in line:
        for j in range(15):
            print(lines[i+j], end='')
        break
