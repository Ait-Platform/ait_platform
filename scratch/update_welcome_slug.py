import re

with open('templates/public/welcome.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('"sace_cptd"', '"sace_participant"')

with open('templates/public/welcome.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated welcome page slug")
