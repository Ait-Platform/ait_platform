with open('app/auth/routes.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()
    for i, line in enumerate(lines):
        if "def register_decision():" in line:
            print("".join(lines[i:i+80]))
            break
