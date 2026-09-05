with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

new_lines = []
for i, line in enumerate(lines):
    if 1566 <= i <= 1583:
        # Increase indentation by 4 spaces
        new_lines.append("    " + line if not line.isspace() else line)
    else:
        new_lines.append(line)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.writelines(new_lines)
