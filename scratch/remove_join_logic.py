import re

with open('templates/program_sace/interactive_workshop.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Completely remove the hasJoinedLocally redirect logic
content = re.sub(r'if \(!hasJoinedLocally\).*?return;\n\s*\}', '', content, flags=re.DOTALL)
content = re.sub(r'let hasJoinedLocally.*?;', '', content)

with open('templates/program_sace/interactive_workshop.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Removed join logic")
