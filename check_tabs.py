with open('templates/program_mechanic/job_cards_list.html', 'r', encoding='utf-8') as f:
    lines = f.readlines()

for i, line in enumerate(lines):
    if "<!-- Tabs Navigation (Styled as Buttons) -->" in line:
        for j in range(25):
            print(lines[i+j], end='')
        break
