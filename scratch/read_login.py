with open('app/auth/routes.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()
    for i, line in enumerate(lines):
        if "login_user(user, remember=remember_flag, fresh=True)" in line:
            print("".join(lines[i:i+40]))
            break
