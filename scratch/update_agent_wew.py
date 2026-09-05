import re

file_path = 'AGENT.md'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

old_render_rule = "- Render takes 5-9 minutes to deploy. **Code Freezes** are required before live SACE evaluation demos."
new_render_rule = "- Render takes 5-9 minutes to deploy. **Code Freezes** are required before live SACE evaluation demos.\n  - **Communication Note:** The user often uses the acronym **\"wew\"** (While We Wait) at the start of prompts. This signals that Render is currently deploying, and we should use this 4-9 minute window to plan the next steps. Do not execute code when planning during a \"wew\" phase unless explicitly instructed."

if "While We Wait" not in text:
    text = text.replace(old_render_rule, new_render_rule)

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
