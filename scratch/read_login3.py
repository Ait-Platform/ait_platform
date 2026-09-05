import re

with open('app/auth/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

start = text.find('def login():')
end = text.find('@auth_bp.route', start)
clean_text = text[start:end].encode('ascii', 'ignore').decode('ascii')
print(clean_text)
