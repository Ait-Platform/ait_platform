import re

with open('templates/program_sace/facilitator_dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Fix startWorkshop
content = re.sub(
    r'function startWorkshop\(\)\s*\{.*?\}',
    "function startWorkshop() {\n        sessionState = 'active'; \n        currentSlide = 1; \n        updateView();\n    }",
    content,
    flags=re.DOTALL
)

# Fix resetWorkshop
content = re.sub(
    r'function resetWorkshop\(\)\s*\{.*?\}',
    "function resetWorkshop() {\n        if(confirm('End workshop?')) {\n            sessionState = 'lobby'; \n            currentSlide = 0; \n            updateView();\n        }\n    }",
    content,
    flags=re.DOTALL
)

# Remove the dangling fetchState setInterval again just in case
content = re.sub(r'setInterval\(fetchState, \d+\);', '', content)
content = re.sub(r'fetchState\(\);', '', content)

with open('templates/program_sace/facilitator_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Fixed facilitator dashboard start button")
