import re

file_path = 'templates/uip/reception/new_interaction.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace('name="category" required class="w-full rounded', 'name="category" required class="w-full py-3 rounded')

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)
