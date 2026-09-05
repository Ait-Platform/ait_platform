with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

for i, line in enumerate(lines):
    if "updated_at" in line and "def repair_tracker_api" in ''.join(lines[max(0, i-50):i+10]):
        print(f"{i + 1}: {line}", end='')
