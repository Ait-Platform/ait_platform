
with open('app/program_culturalfire/routes.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

new_lines = []
for i, line in enumerate(lines):
    if line.strip() == 'import os' and i > 50:
        continue
    new_lines.append(line)

with open('app/program_culturalfire/routes.py', 'w', encoding='utf-8') as f:
    f.writelines(new_lines)

print('Fixed import os')

