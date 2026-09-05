import re

file_path = 'AGENT.md'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

target = "- **Render Deployment Constraint:** Render takes 5-9 minutes to deploy. **Code Freezes** are required before live SACE evaluation demos."
replacement = target + "\n  - **Communication Note:** The user uses the acronym **\"wew\"** (While We Wait). This means we are waiting 4-9 minutes for Render to deploy. During a 'wew' phase, we look forward and plan what to do next without writing any code."

text = text.replace(target, replacement)

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
