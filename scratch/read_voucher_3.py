with open('app/auth/routes.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()
    for i, line in enumerate(lines):
        if "if subject in (\"billing\", \"metro_billing\"):" in line:
            print("".join(lines[i:i+40]))
            break
