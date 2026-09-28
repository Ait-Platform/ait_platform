import re

with open("app/program_uip/services/register.py", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace(
    'CoreRole.slug == "manager"',
    'CoreRole.slug.in_(["manager", "municipal_officer"])'
)

with open("app/program_uip/services/register.py", "w", encoding="utf-8") as f:
    f.write(text)
print("Patched register.py")
