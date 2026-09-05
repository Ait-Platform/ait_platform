import re

with open('templates/public/welcome.html', 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace('"healthcore":{"color":"teal","desc":"Personalized Health IQ diagnostics."}', '"healthcore":{"color":"teal","desc":"Personalized Health IQ diagnostics."},\n"uip":{"color":"blue","desc":"Urban Improvement Precinct Management."}')

with open('templates/public/welcome.html', 'w', encoding='utf-8') as f:
    f.write(text)
