import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Replace image sizing
text = text.replace('max-h-[60vh] max-w-full', 'w-full h-full p-2')
# Also, if there are any max-h-[80vh] or max-h-[70vh]
text = re.sub(r'max-h-\[\d+vh\] max-w-full', 'w-full h-full p-2', text)

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
