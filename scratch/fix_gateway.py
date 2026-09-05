import os

with open('app/uip/gateway.py', 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace('\\"\\"\\"', '\"\"\"')

with open('app/uip/gateway.py', 'w', encoding='utf-8') as f:
    f.write(text)
