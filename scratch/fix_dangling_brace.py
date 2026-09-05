import re

with open('templates/program_sace/interactive_workshop.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Fix dangling brace
content = content.replace("        if(activeView) activeView.classList.remove('hidden');\n    }\n\n        }, 1000);", "        if(activeView) activeView.classList.remove('hidden');\n    }")

with open('templates/program_sace/interactive_workshop.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Fixed dangling brace")
