import re

with open('templates/program_sace/interactive_workshop.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Pattern to comment out the button and the modal
pattern = r'(<!-- Floating SACE Guide Button -->.*?</button>)'
replacement = r'<!-- \1 -->'
content = re.sub(pattern, replacement, content, flags=re.DOTALL)

with open('templates/program_sace/interactive_workshop.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Commented out SACE Guide floating button")
