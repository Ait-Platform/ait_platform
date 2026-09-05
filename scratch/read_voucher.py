with open('app/auth/routes.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()
    for i, line in enumerate(lines):
        if "voucher = request.values.get(" in line:
            print("".join(lines[i:i+40]))
            break
