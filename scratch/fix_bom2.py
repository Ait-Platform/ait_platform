with open('app/uip/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace('\ufeff', '')

with open('app/uip/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)
