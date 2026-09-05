import re

with open('templates/program_sace/interactive_workshop.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace the duplicate let sessionState
content = content.replace("    let sessionState = 'unknown'; // don't default to lobby or it redirects instantly\n", "")

with open('templates/program_sace/interactive_workshop.html', 'w', encoding='utf-8') as f:
    f.write(content)
