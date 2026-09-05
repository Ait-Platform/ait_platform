import re
with open('app/auth/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

start = text.find('def register_confirm():')
end = text.find('def finalize_user_after_payment', start)
with open('scratch/confirm_output.txt', 'w', encoding='utf-8') as out:
    out.write(text[start:end])
