import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Make sure all flex-grow elements have min-h-0 to prevent flex overflow issues
text = re.sub(r'class="flex-grow([^"]*)"', lambda m: 'class="flex-grow' + m.group(1) + '"' if 'min-h-0' in m.group(1) else 'class="flex-grow' + m.group(1) + ' min-h-0"', text)

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
