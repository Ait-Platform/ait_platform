import re
with open('app/auth/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

start = text.find('def register_confirm():')
end = text.find('        # We assume the 100 token trial bonus', start)
with open('scratch/confirm_output2.txt', 'w', encoding='utf-8') as out:
    out.write(text[start:end])
