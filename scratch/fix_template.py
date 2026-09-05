import re
with open('templates/uip/interactions/view.html', 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace('task.interaction.assignee.first_name', 'task.assignee.first_name')

with open('templates/uip/interactions/view.html', 'w', encoding='utf-8') as f:
    f.write(text)
