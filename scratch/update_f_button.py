import re

with open('templates/program_sace/facilitator_dashboard.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Change the text on the button
text = text.replace('Start Workshop</button>', 'Start Show</button>')
text = text.replace('Click Start to begin', 'Click Start Show to begin')

with open('templates/program_sace/facilitator_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(text)
