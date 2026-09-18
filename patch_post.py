import re
with open("app/program_uip/routes.py", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace('if request.method == "POST" and not existing_claim:', 'if request.method == "POST":')

with open("app/program_uip/routes.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Removed not existing_claim from POST check")
