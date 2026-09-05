with open('app/auth/routes.py', 'r', encoding='utf8') as f:
    text = f.read()
    start = text.find("def login()")
    if start != -1:
        end = text.find("def ", start + 10)
        print(text[start:end])
