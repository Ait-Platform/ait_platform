import re

with open('app/uip/__init__.py', 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace('\\"\\"\\"', '\"\"\"')

with open('app/uip/__init__.py', 'w', encoding='utf-8') as f:
    f.write(text)
