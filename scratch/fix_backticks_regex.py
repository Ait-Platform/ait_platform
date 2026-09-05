import re

with open('templates/program_sace/interactive_workshop.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = re.sub(
    r'container\.innerHTML =\s*(<div class="h-20 w-20 bg-green-100.*?)</p>\s*;',
    r'container.innerHTML = \n\1</p>\n;',
    content,
    flags=re.DOTALL
)

content = re.sub(
    r'container\.innerHTML =\s*(<div class="h-20 w-20 bg-red-100.*?)</p>\s*;',
    r'container.innerHTML = \n\1</p>\n;',
    content,
    flags=re.DOTALL
)

with open('templates/program_sace/interactive_workshop.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Regex replace run")
