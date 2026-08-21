import re

with open('templates/program_mechanic/dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace the first instance of the modal
content = re.sub(r'<!-- Manual Setup Modal -->.*?</div>\s*</div>', '', content, count=1, flags=re.DOTALL)

with open('templates/program_mechanic/dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Removed first modal")
