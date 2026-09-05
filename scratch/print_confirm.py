import re
with open('app/auth/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

start = text.find('def register_confirm():')
end = text.find('def finalize_user_after_payment', start)
print(text[start:end])
