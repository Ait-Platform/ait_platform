with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

end_idx = 0
for i, line in enumerate(lines):
    if "def accept_quote(id):" in line:
        end_idx = i

for i, line in enumerate(lines[end_idx+70:end_idx+95]):
    print(f"{i + end_idx+70 + 1}: {line}", end='')
