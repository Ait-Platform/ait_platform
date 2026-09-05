with open('app/auth/routes.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

for i, line in enumerate(lines):
    if "re.split(r'[,;" in line:
        print(f"Line {i}: {line}")
        print(f"Line {i+1}: {lines[i+1]}")
