with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

new_lines = []
for i, line in enumerate(lines):
    if i == 1301: # Line 1302 (0-indexed 1301)
        if "from app.models.mechanic import MechClient" in line:
            continue
    new_lines.append(line)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.writelines(new_lines)
